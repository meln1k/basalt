//! The runner: one test, one seed, through setup, chaos, quiet, and check.

use std::fmt;
use std::future::{Future, poll_fn};
use std::task::Poll;

use basalt_env::backend::LocalBoxFuture;
use basalt_env::{Ctx, Duration, Priority, trace};
use basalt_sim::{Sim, SimControl, SimError, TraceRecord};

use crate::panic_hook;
use crate::workload::{DynWorkload, TestSpec, WorkloadResult};

/// The label and failure zone of the machine the harness runs its workloads on. It's protected
/// from faults, so fault workloads never see it.
pub const HARNESS: &str = "harness";

/// How long, in virtual time, the setup and the check phases may each take. Every phase has a
/// deadline, because a system with a timer always pending never trips the hang detector.
pub const PHASE_DEADLINE: Duration = Duration::from_secs(60);

/// How one run ended.
#[derive(Debug)]
pub struct RunReport {
    pub test: &'static str,
    pub seed: u64,
    pub result: Result<(), Failure>,
    /// A hash of every random stream's state and of the trace. Running the same seed on the
    /// same build must give the same unseed.
    pub unseed: u64,
}

/// Why a run failed. A panic doesn't get here: the panic hook prints the seed and aborts.
#[derive(Debug)]
pub enum Failure {
    /// The simulator stopped the run: a hang, or an `Error` trace event.
    Sim(SimError),
    /// A workload returned an error.
    Workload {
        phase: Phase,
        workload: &'static str,
        error: String,
    },
    /// A phase didn't finish within its deadline, in virtual time.
    Deadline { phase: Phase, after: Duration },
}

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Failure::Sim(error) => error.fmt(f),
            Failure::Workload {
                phase,
                workload,
                error,
            } => write!(f, "{workload} failed in the {phase:?} phase: {error}"),
            Failure::Deadline { phase, after } => write!(
                f,
                "the {phase:?} phase didn't finish within {after:?} of virtual time"
            ),
        }
    }
}

/// A phase of a run, in order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    Setup,
    Chaos,
    Quiet,
    Check,
}

/// Runs `spec` with `seed`, calling `on_trace` with each trace event as it's recorded. Installs
/// the panic hook with `seed` first, so a panic anywhere in the run prints the seed and commit
/// and aborts the process.
pub fn run(spec: &TestSpec, seed: u64, on_trace: impl FnMut(&TraceRecord) + 'static) -> RunReport {
    panic_hook::install(seed);
    let sim = Sim::new(seed);
    sim.on_trace(on_trace);
    let control = sim.control();
    // Before the test adds anything, so `machines()` never lists it, in `build` or later.
    let machine = control.add_machine(HARNESS, 0);
    control.protect(machine);
    let ctx = control.add_process(machine, HARNESS, |_| Box::pin(async {}));
    let workloads = (spec.build)(&control).0;
    let task = ctx.spawn(
        "harness",
        Priority::Default,
        phases(*spec, ctx.clone(), control, workloads),
    );
    let result = sim.block_on(task).unwrap_or_else(|e| Err(Failure::Sim(e)));
    RunReport {
        test: spec.name,
        seed,
        result,
        unseed: sim.unseed(),
    }
}

async fn phases(
    spec: TestSpec,
    ctx: Ctx,
    sim: SimControl,
    mut workloads: Vec<Box<dyn DynWorkload>>,
) -> Result<(), Failure> {
    trace!(Info, "SetupPhase");
    let setup = all(
        Phase::Setup,
        workloads.iter_mut().map(|w| (w.name(), w.setup(&ctx))),
    );
    within(&ctx, Phase::Setup, PHASE_DEADLINE, setup).await?;

    trace!(Info, "ChaosPhase", duration = spec.chaos);
    let chaos = all(
        Phase::Chaos,
        workloads
            .iter_mut()
            .map(|w| (w.name(), w.start(&ctx, &sim))),
    );
    // The chaos phase ends when its time is up; only an error before then fails the run.
    if let Ok(result) = ctx.timeout(spec.chaos, chaos).await {
        result?;
    }

    trace!(Info, "QuietPhase", deadline = spec.quiet);
    sim.lift_faults();
    let liveness = all(
        Phase::Quiet,
        workloads.iter_mut().map(|w| (w.name(), w.liveness(&ctx))),
    );
    within(&ctx, Phase::Quiet, spec.quiet, liveness).await?;

    trace!(Info, "CheckPhase");
    let check = all(
        Phase::Check,
        workloads.iter_mut().map(|w| (w.name(), w.check(&ctx))),
    );
    within(&ctx, Phase::Check, PHASE_DEADLINE, check).await?;
    trace!(Info, "TestPassed");
    Ok(())
}

/// Runs a phase, failing it if it takes longer than `after`.
async fn within(
    ctx: &Ctx,
    phase: Phase,
    after: Duration,
    phase_future: impl Future<Output = Result<(), Failure>>,
) -> Result<(), Failure> {
    ctx.timeout(after, phase_future)
        .await
        .unwrap_or(Err(Failure::Deadline { phase, after }))
}

/// Runs every workload's future of `phase` together. Returns once all have returned, or at the
/// first error, dropping the rest.
fn all<'a>(
    phase: Phase,
    futures: impl Iterator<Item = (&'static str, LocalBoxFuture<'a, WorkloadResult>)>,
) -> impl Future<Output = Result<(), Failure>> + 'a {
    let mut pending: Vec<_> = futures.collect();
    poll_fn(move |cx| {
        let mut i = 0;
        while i < pending.len() {
            let (name, future) = &mut pending[i];
            match future.as_mut().poll(cx) {
                Poll::Pending => i += 1,
                Poll::Ready(Ok(())) => drop(pending.remove(i)),
                Poll::Ready(Err(error)) => {
                    return Poll::Ready(Err(Failure::Workload {
                        phase,
                        workload: name,
                        error: error.to_string(),
                    }));
                }
            }
        }
        if pending.is_empty() {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    })
}

#[cfg(test)]
mod tests {
    use std::future::pending;

    use basalt_sim::KillType;

    use super::*;
    use crate::workload::{Workload, Workloads};

    /// Kills a machine picked from `sim.machines()` every 100 ms, as attrition would.
    struct KillAnything;

    impl Workload for KillAnything {
        async fn start(&mut self, ctx: &Ctx, sim: &SimControl) -> WorkloadResult {
            loop {
                ctx.sleep(Duration::from_millis(100)).await;
                let machines = sim.machines();
                let machine = machines[ctx.rng().range(0..machines.len())];
                sim.kill(machine, KillType::Crash);
            }
        }
    }

    /// Never finishes its setup, while keeping a timer pending so no hang is detected.
    struct EndlessSetup;

    impl Workload for EndlessSetup {
        async fn setup(&mut self, ctx: &Ctx) -> WorkloadResult {
            loop {
                ctx.sleep(Duration::from_secs(1)).await;
            }
        }

        async fn start(&mut self, _: &Ctx, _: &SimControl) -> WorkloadResult {
            pending().await
        }
    }

    fn spec(build: fn(&SimControl) -> Workloads) -> TestSpec {
        TestSpec {
            name: "test",
            about: "",
            chaos: Duration::from_secs(5),
            quiet: Duration::from_secs(1),
            build,
        }
    }

    #[test]
    fn fault_workloads_cant_reach_the_harness_machine() {
        let spec = spec(|sim| {
            for zone in ["a", "b"] {
                let m = sim.add_machine(zone, 0);
                sim.add_process(m, zone, |_| Box::pin(async {}));
            }
            let mut w = Workloads::new();
            w.add(KillAnything);
            w
        });
        for seed in 0..5 {
            let report = run(&spec, seed, |_| ());
            assert!(report.result.is_ok(), "seed {seed}: {:?}", report.result);
        }
    }

    #[test]
    fn a_phase_past_its_deadline_fails_the_run() {
        let spec = spec(|_| {
            let mut w = Workloads::new();
            w.add(EndlessSetup);
            w
        });
        let report = run(&spec, 0, |_| ());
        assert!(
            matches!(
                report.result,
                Err(Failure::Deadline { phase: Phase::Setup, after }) if after == PHASE_DEADLINE
            ),
            "{:?}",
            report.result
        );
    }
}
