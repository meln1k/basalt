//! Tests of the harness itself: a client pings a pong process on another machine through timers
//! while attrition kills and reboots that machine. [`PING_PONG_PLANTED`] is the same test with
//! a panic planted in seed [`PLANTED_SEED`], so a sweep has exactly one seed to find.
//!
//! The processes share a mailbox in memory instead of a network, so the test needs nothing but
//! machines, processes, kills, and timers.
//!
//! The pong process has a `buggify!` site that ignores a ping, and the pinger a probe for
//! pings that time out. Each also has a site and a probe behind a check that never passes,
//! [`UNREACHABLE_PROBE`] among them, so a sweep's coverage report always has some to name.
//!
//! [`FAULTS`] is the fault workloads' own test: see [`mesh`].

use std::cell::Cell;
use std::rc::Rc;

use basalt_env::{Ctx, Duration, buggify, probe, trace};
use basalt_sim::{KillType, MachineId, SimControl};

use crate::history::{History, Outcome};
use crate::workload::{TestSpec, Workload, WorkloadResult, Workloads};

pub mod mesh;

pub use mesh::FAULTS;

/// Ping-pong with no planted failure: every seed passes.
pub const PING_PONG: TestSpec = TestSpec {
    name: "ping-pong",
    about: "a client pings a process whose machine keeps crashing (harness self-test)",
    chaos: Duration::from_secs(10),
    quiet: Duration::from_secs(5),
    build: |sim| build(sim, false),
};

/// The seed in which [`PING_PONG_PLANTED`] panics.
pub const PLANTED_SEED: u64 = 7;

/// Ping-pong whose pong process panics in seed [`PLANTED_SEED`]; every other seed passes.
pub const PING_PONG_PLANTED: TestSpec = TestSpec {
    name: "ping-pong-planted",
    about: "ping-pong with a panic planted in seed 7, for checking that sweeps find it",
    chaos: Duration::from_secs(10),
    quiet: Duration::from_secs(5),
    build: |sim| build(sim, sim.seed() == PLANTED_SEED),
};

/// The pong process panics on this pong, counting across its incarnations.
const PLANTED_PONG: u64 = 5;

/// The name of a probe in the self-test that no seed can reach.
pub const UNREACHABLE_PROBE: &str = "selftest.ping_number_wrapped";

/// How long the client waits for each pong.
const PING_TIMEOUT: Duration = Duration::from_millis(100);

#[derive(Default)]
struct Mailbox {
    ping: Cell<Option<u64>>,
    pong: Cell<Option<u64>>,
    /// Pongs sent, by every incarnation of the pong process.
    pongs: Cell<u64>,
}

fn build(sim: &SimControl, planted: bool) -> Workloads {
    let mailbox = Rc::new(Mailbox::default());
    let machine = sim.add_machine("pong", 0);
    let mb = mailbox.clone();
    sim.add_process(machine, "pong", move |ctx| {
        Box::pin(pong(ctx, mb.clone(), planted))
    });
    let mut workloads = Workloads::new();
    workloads
        .add(Pinger {
            mailbox,
            history: History::new(),
            next: 0,
        })
        .add(Attrition { machine });
    workloads
}

/// The pong process: answers each ping a few milliseconds later.
async fn pong(ctx: Ctx, mailbox: Rc<Mailbox>, planted: bool) {
    trace!(Info, "PongStarted");
    loop {
        let wait = ctx
            .rng()
            .range(Duration::from_millis(1)..=Duration::from_millis(10));
        ctx.sleep(wait).await;
        if let Some(n) = mailbox.ping.take() {
            // A lost ping, which the pinger times out on.
            if buggify!() {
                continue;
            }
            let pongs = mailbox.pongs.get() + 1;
            mailbox.pongs.set(pongs);
            assert!(
                !(planted && pongs == PLANTED_PONG),
                "planted panic in pong {pongs}"
            );
            trace!(Info, "Pong", n = n);
            mailbox.pong.set(Some(n));
        }
    }
}

/// Sends numbered pings and records each as an operation whose output is the number echoed.
struct Pinger {
    mailbox: Rc<Mailbox>,
    history: History<u64, u64>,
    next: u64,
}

impl Pinger {
    /// Sends one ping and waits for its pong. Returns whether it came back in time; if not,
    /// the operation stays indeterminate.
    async fn ping(&mut self, ctx: &Ctx) -> bool {
        let n = self.next;
        self.next += 1;
        // Unreachable: pings are numbered from 0 and no run sends 2^64 of them.
        if n == u64::MAX {
            probe!("selftest.ping_number_wrapped");
            if buggify!() {
                self.next = 0;
            }
        }
        let call = self.history.invoke(ctx, 0, n);
        self.mailbox.ping.set(Some(n));
        let wait = async {
            loop {
                ctx.sleep(Duration::from_millis(1)).await;
                // Drops a late pong to an earlier ping.
                if self.mailbox.pong.take() == Some(n) {
                    return;
                }
            }
        };
        let answered = ctx.timeout(PING_TIMEOUT, wait).await.is_ok();
        if answered {
            call.ok(ctx, n);
        } else {
            probe!("selftest.ping_timed_out");
        }
        answered
    }
}

impl Workload for Pinger {
    fn name(&self) -> &'static str {
        "pinger"
    }

    async fn start(&mut self, ctx: &Ctx, _: &SimControl) -> WorkloadResult {
        loop {
            self.ping(ctx).await;
            let pause = ctx.rng().range(Duration::ZERO..Duration::from_millis(5));
            ctx.sleep(pause).await;
        }
    }

    async fn liveness(&mut self, ctx: &Ctx) -> WorkloadResult {
        while !self.ping(ctx).await {}
        Ok(())
    }

    async fn check(&mut self, _: &Ctx) -> WorkloadResult {
        let entries = self.history.entries();
        let mut answered = 0;
        for entry in &entries {
            match &entry.outcome {
                Outcome::Ok { output, at } => {
                    if *output != entry.input || at.position <= entry.call.position {
                        return Err(format!("bad ping: {entry:?}").into());
                    }
                    answered += 1;
                }
                Outcome::Failed { .. } => return Err(format!("failed ping: {entry:?}").into()),
                Outcome::Indeterminate => {}
            }
        }
        trace!(
            Info,
            "PingsChecked",
            pings = entries.len(),
            answered = answered
        );
        Ok(())
    }
}

/// Crashes the pong machine and reboots it, again and again.
struct Attrition {
    machine: MachineId,
}

impl Workload for Attrition {
    fn name(&self) -> &'static str {
        "attrition"
    }

    async fn start(&mut self, ctx: &Ctx, sim: &SimControl) -> WorkloadResult {
        loop {
            let up = ctx
                .rng()
                .range(Duration::from_millis(100)..Duration::from_millis(500));
            ctx.sleep(up).await;
            sim.kill(self.machine, KillType::Crash);
            let down = ctx
                .rng()
                .range(Duration::from_millis(10)..Duration::from_millis(200));
            ctx.sleep(down).await;
            sim.reboot(self.machine);
        }
    }
}

/// The tests in this module.
pub const TESTS: &[TestSpec] = &[PING_PONG, PING_PONG_PLANTED, FAULTS];
