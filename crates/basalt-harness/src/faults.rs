//! Fault workloads: attrition, power cuts, clogging, partitions, clock skew and jumps, and full
//! disks and I/O errors. [`add`] adds one workload for each fault type the seed's
//! [`Swarm`](basalt_sim::Swarm) config enables. They run in the chaos phase; the runner's quiet
//! phase lifts whatever they leave behind ([`SimControl::lift_faults`]).
//!
//! Each workload picks its targets from [`SimControl::machines`], so it never touches the
//! harness's machine, and draws its targets and timing from its own fork of the `faults` stream
//! ([`SimControl::fault_rng`]). So a seed's faults don't move when the system under test changes,
//! such as when a planted bug is switched on, and one fault type's draws don't depend on another
//! type's being enabled. They react only to what other faults did, such as which machines are
//! down. One thing can still shift them: the run loop sometimes fires the next timer up to a
//! millisecond late, drawn from the scheduler's stream.
//!
//! Each injection hits a `fault.*` probe, so the sweep's coverage report names every fault type,
//! and every variant of one, that no seed injected.

use std::ops::Range;

use basalt_env::{Ctx, DiskId, Duration, Instant, Rng, probe, trace};
use basalt_sim::{Fault, KillType, MachineId, SimControl};

use crate::workload::{Workload, WorkloadResult, Workloads};

/// Whether attrition may wipe machines, or only crash them.
///
/// A wiped machine comes back with empty disks, as a replacement node does. A system that
/// can't yet rebuild a replacement's state, such as the toy register, loses acknowledged data
/// to wipes through no bug of its own, so its tests turn them off.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Wipes {
    On,
    /// Attrition crashes the machines it would have wiped. Every other fault, and every draw,
    /// stays the same as with wipes on.
    Off,
}

/// Adds a workload for each fault type the run's swarm config enables. Call it once per test,
/// from the test's `build`, after adding the machines the faults may hit.
pub fn add(workloads: &mut Workloads, sim: &SimControl, wipes: Wipes) {
    for fault in Fault::ALL {
        // Every type gets its fork, enabled or not, so each type's stream depends only on the
        // seed.
        let rng = sim.fault_rng().fork();
        if !sim.swarm().enabled(fault) {
            continue;
        }
        let workload = FaultWorkload { fault, rng, wipes };
        workloads.add(workload);
    }
}

/// How often attrition wipes a machine instead of crashing it.
const WIPE_PROBABILITY: f64 = 0.2;
/// The largest drift, in parts per million, that clock skew sets.
const MAX_DRIFT_PPM: i32 = 100_000;
/// The largest wall-clock jump, in seconds, either way.
const MAX_JUMP_SECS: u64 = 3600;

/// What one fault type does: when it strikes, and how.
struct FaultWorkload {
    fault: Fault,
    rng: Rng,
    wipes: Wipes,
}

/// Undoes an injected fault, such as by rebooting what it killed.
type Undo<'a> = Box<dyn FnOnce() + 'a>;

impl Workload for FaultWorkload {
    fn name(&self) -> &'static str {
        match self.fault {
            Fault::Attrition => "attrition",
            Fault::PowerCut => "power-cuts",
            Fault::Clog => "clogging",
            Fault::Partition => "partitions",
            Fault::ClockSkew => "clock-skew",
            Fault::DiskFull => "full-disks",
            Fault::DiskErrors => "disk-errors",
            _ => "unknown-fault",
        }
    }

    async fn start(&mut self, ctx: &Ctx, sim: &SimControl) -> WorkloadResult {
        let (rng, wipes) = (&self.rng, self.wipes);
        let machines = sim.machines();
        if machines.is_empty() {
            return Ok(());
        }
        let ms = Duration::from_millis;
        let secs = Duration::from_secs;
        match self.fault {
            Fault::Attrition => {
                strike(ctx, rng, ms(500)..secs(5), || {
                    attrition(sim, rng, &machines, wipes, ms(100)..secs(3))
                })
                .await
            }
            Fault::PowerCut => {
                strike(ctx, rng, secs(3)..secs(15), || {
                    power_cut(sim, rng, &machines, ms(100)..secs(3))
                })
                .await
            }
            Fault::Clog => {
                strike(ctx, rng, ms(100)..secs(2), || {
                    clog(sim, rng, &machines, ms(10)..secs(2));
                    None
                })
                .await
            }
            Fault::Partition => {
                strike(ctx, rng, secs(2)..secs(10), || {
                    partition(sim, rng, &machines);
                    let undo: Undo<'_> = Box::new(|| sim.heal_network());
                    Some((rng.range(ms(500)..secs(10)), undo))
                })
                .await
            }
            Fault::ClockSkew => {
                strike(ctx, rng, secs(1)..secs(10), || {
                    clock_skew(sim, rng, &machines);
                    None
                })
                .await
            }
            Fault::DiskFull | Fault::DiskErrors => {
                let full = self.fault == Fault::DiskFull;
                strike(ctx, rng, secs(2)..secs(10), || {
                    disk_fault(sim, rng, &machines, full)?;
                    let undo: Undo<'_> = Box::new(|| sim.restore_disks());
                    Some((rng.range(ms(500)..secs(5)), undo))
                })
                .await
            }
            fault => Err(format!("no fault workload injects {fault:?}").into()),
        }
    }
}

/// Calls `inject` after each gap drawn from `gap`, forever. `inject` returns how long its fault
/// lasts and how to undo it, if it needs undoing; each undo runs when its time comes, so
/// faults of one type may overlap.
async fn strike<'a>(
    ctx: &Ctx,
    rng: &Rng,
    gap: Range<Duration>,
    mut inject: impl FnMut() -> Option<(Duration, Undo<'a>)>,
) -> WorkloadResult {
    let mut undos: Vec<(Instant, Undo<'a>)> = Vec::new();
    let mut next = ctx.now() + rng.range(gap.clone());
    loop {
        let due = undos.iter().map(|&(at, _)| at).fold(next, Instant::min);
        ctx.sleep(due.checked_duration_since(ctx.now()).unwrap_or_default())
            .await;
        let now = ctx.now();
        let (ready, later) = undos.into_iter().partition(|&(at, _)| at <= now);
        undos = later;
        for (_, undo) in ready {
            undo();
        }
        if next <= now {
            if let Some((lasts, undo)) = inject() {
                undos.push((now + lasts, undo));
            }
            next = now + rng.range(gap.clone());
        }
    }
}

/// The machines of `machines` that are up, in order.
fn up(sim: &SimControl, machines: &[MachineId]) -> Vec<MachineId> {
    machines.iter().copied().filter(|&m| sim.is_up(m)).collect()
}

/// Crashes or, if `wipes` allows, wipes one machine that's up, unless that would leave a
/// majority of `machines` down, counting machines down for any reason, such as a power cut.
/// Reboots it after a downtime drawn from `down`.
fn attrition<'a>(
    sim: &'a SimControl,
    rng: &Rng,
    machines: &[MachineId],
    wipes: Wipes,
    down: Range<Duration>,
) -> Option<(Duration, Undo<'a>)> {
    let up = up(sim, machines);
    let minority = (machines.len() - 1) / 2;
    if machines.len() - up.len() >= minority {
        return None;
    }
    let machine = up[rng.range(0..up.len())];
    // Drawn even with wipes off, so the draws after it don't move.
    let wipe = rng.bool(WIPE_PROBABILITY);
    let kind = if wipe && wipes == Wipes::On {
        probe!("fault.attrition.wipe");
        KillType::Wipe
    } else {
        probe!("fault.attrition.crash");
        KillType::Crash
    };
    sim.kill(machine, kind);
    Some((rng.range(down), Box::new(move || sim.reboot(machine))))
}

/// Cuts the power of a random number of the machines that are up, all at once, sometimes a
/// majority of `machines` or all of them. Reboots them together after a downtime drawn from
/// `down`.
fn power_cut<'a>(
    sim: &'a SimControl,
    rng: &Rng,
    machines: &[MachineId],
    down: Range<Duration>,
) -> Option<(Duration, Undo<'a>)> {
    let mut cut = up(sim, machines);
    if cut.is_empty() {
        return None;
    }
    rng.shuffle(&mut cut);
    cut.truncate(rng.range(1..=cut.len()));
    probe!("fault.power_cut");
    if cut.len() > machines.len() / 2 {
        probe!("fault.power_cut.majority");
    }
    for &machine in &cut {
        sim.kill(machine, KillType::PowerCut);
    }
    let undo = move || {
        for machine in cut {
            sim.reboot(machine);
        }
    };
    Some((rng.range(down), Box::new(undo)))
}

/// Two different machines of `machines`, which must have at least two.
fn pair(rng: &Rng, machines: &[MachineId]) -> (MachineId, MachineId) {
    let n = machines.len();
    let a = rng.range(0..n);
    let b = (a + rng.range(1..n)) % n;
    (machines[a], machines[b])
}

/// Clogs one pair of machines, or all that one machine sends or receives, for a time drawn from
/// `lasts`. A clog ends by itself.
fn clog(sim: &SimControl, rng: &Rng, machines: &[MachineId], lasts: Range<Duration>) {
    let d = rng.range(lasts);
    match rng.range(0..3) {
        0 if machines.len() >= 2 => {
            let (from, to) = pair(rng, machines);
            probe!("fault.clog.pair");
            sim.clog_pair(from, to, d);
        }
        1 => {
            probe!("fault.clog.sends");
            sim.clog_sends(machines[rng.range(0..machines.len())], d);
        }
        _ => {
            probe!("fault.clog.receives");
            sim.clog_receives(machines[rng.range(0..machines.len())], d);
        }
    }
}

/// Partitions one way between two machines, both ways, or one machine from all the others, or
/// starts resetting connections at random. The caller heals the whole network later, which
/// also ends any clog still on.
fn partition(sim: &SimControl, rng: &Rng, machines: &[MachineId]) {
    match rng.range(0..4) {
        0 if machines.len() >= 2 => {
            let (from, to) = pair(rng, machines);
            probe!("fault.partition.one_way");
            sim.partition(from, to);
        }
        1 if machines.len() >= 2 => {
            let (a, b) = pair(rng, machines);
            probe!("fault.partition.two_way");
            sim.partition_two_way(a, b);
        }
        2 if machines.len() >= 2 => {
            let isolated = machines[rng.range(0..machines.len())];
            probe!("fault.partition.isolate");
            for &other in machines.iter().filter(|&&m| m != isolated) {
                sim.partition_two_way(isolated, other);
            }
        }
        _ => {
            probe!("fault.partition.random_resets");
            sim.set_reset_probability(0.001 * rng.range(1..=20) as f64);
        }
    }
}

/// Sets one machine's clock drift, or jumps its wall clock forward or back.
fn clock_skew(sim: &SimControl, rng: &Rng, machines: &[MachineId]) {
    let machine = machines[rng.range(0..machines.len())];
    if rng.bool(0.5) {
        let ppm = rng.range(-MAX_DRIFT_PPM..=MAX_DRIFT_PPM);
        probe!("fault.clock.drift");
        trace!(Info, "ClockDrift", machine = machine.get(), ppm = ppm);
        sim.set_clock_drift(machine, ppm);
    } else {
        let by = Duration::from_secs(rng.range(1..=MAX_JUMP_SECS));
        let forward = rng.bool(0.5);
        let now = sim.wall_clock(machine);
        let to = if forward {
            now.checked_add(by)
        } else {
            now.checked_sub(by)
        };
        let to = to.unwrap_or(now);
        probe!("fault.clock.jump");
        trace!(
            Info,
            "WallClockJump",
            machine = machine.get(),
            by = by,
            forward = forward
        );
        sim.set_wall_clock(machine, to);
    }
}

/// Fills one disk of `machines` if `full`, or else makes one fail reads and writes or flip bits
/// on read. Returns `None` if no machine has a disk. The caller restores every disk later.
fn disk_fault(sim: &SimControl, rng: &Rng, machines: &[MachineId], full: bool) -> Option<()> {
    let disks: Vec<DiskId> = machines
        .iter()
        .flat_map(|&m| sim.disks(m).to_vec())
        .collect();
    if disks.is_empty() {
        return None;
    }
    let disk = disks[rng.range(0..disks.len())];
    if full {
        probe!("fault.disk.full");
        trace!(Info, "DiskFilled", disk = disk.get());
        sim.fill_disk(disk);
    } else if rng.bool(0.5) {
        let p = 0.002 * rng.range(1..=25) as f64;
        probe!("fault.disk.io_errors");
        trace!(Info, "DiskErrors", disk = disk.get(), p = p);
        sim.set_disk_errors(disk, p);
    } else {
        let p = 0.0005 * rng.range(1..=20) as f64;
        probe!("fault.disk.bit_flips");
        trace!(Info, "DiskBitFlips", disk = disk.get(), p = p);
        sim.set_bit_flips(disk, p);
    }
    Some(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use basalt_sim::TraceRecord;

    use super::*;
    use crate::runner::run;
    use crate::workload::TestSpec;

    // Even, so a majority and half differ.
    const MACHINES: usize = 4;

    /// Idle machines and every fault workload, with nothing else to end the chaos phase.
    const IDLE: TestSpec = TestSpec {
        name: "idle",
        about: "",
        chaos: Duration::from_secs(60),
        quiet: Duration::from_secs(1),
        build: |sim| idle(sim, Wipes::On),
    };

    /// [`IDLE`] with wipes off.
    const IDLE_WITHOUT_WIPES: TestSpec = TestSpec {
        build: |sim| idle(sim, Wipes::Off),
        ..IDLE
    };

    fn idle(sim: &SimControl, wipes: Wipes) -> Workloads {
        for i in 0..MACHINES {
            let m = sim.add_machine(&format!("z{i}"), 1);
            sim.add_process(m, "idle", |_| Box::pin(std::future::pending()));
        }
        let mut w = Workloads::new();
        add(&mut w, sim, wipes);
        w.add(Idle);
        w
    }

    /// Keeps the chaos phase going for its whole duration.
    struct Idle;

    impl Workload for Idle {
        async fn start(&mut self, _: &Ctx, _: &SimControl) -> WorkloadResult {
            std::future::pending().await
        }
    }

    fn field<'a>(record: &'a TraceRecord, key: &str) -> &'a str {
        &record.fields.iter().find(|(k, _)| *k == key).unwrap().1
    }

    /// Replays a run's kills and reboots. Returns the most machines down at once right after a
    /// crash or wipe, and right after a power cut, during the chaos phase, and checks every
    /// machine is up once the faults are lifted.
    fn most_down(trace: &[TraceRecord]) -> (usize, usize) {
        let mut down = BTreeSet::new();
        let (mut attrition, mut power) = (0, 0);
        for record in trace {
            match record.name {
                "MachineKilled" => {
                    down.insert(field(record, "machine").to_owned());
                    match field(record, "kind") {
                        "PowerCut" => power = power.max(down.len()),
                        _ => attrition = attrition.max(down.len()),
                    }
                }
                "MachineRebooted" => {
                    down.remove(field(record, "machine"));
                }
                "FaultsLifted" => assert!(down.is_empty(), "{down:?} still down"),
                _ => {}
            }
        }
        (attrition, power)
    }

    /// [`IDLE`], but each machine's process draws from `ctx.rng()` and reaches a `buggify!`
    /// site, as a system under test does, differently than when idle.
    const BUSY: TestSpec = TestSpec {
        name: "busy",
        build: |sim| {
            for i in 0..MACHINES {
                let m = sim.add_machine(&format!("z{i}"), 1);
                sim.add_process(m, "busy", |ctx| {
                    Box::pin(async move {
                        loop {
                            let d = ctx.rng().range(1..20u64);
                            ctx.sleep(Duration::from_millis(d)).await;
                            if basalt_env::buggify!() {
                                ctx.rng().u64();
                            }
                        }
                    })
                });
            }
            let mut w = Workloads::new();
            add(&mut w, sim, Wipes::On);
            w.add(Idle);
            w
        },
        ..IDLE
    };

    /// The faults a run injected, without their times, sorted.
    fn faults(spec: &TestSpec, seed: u64) -> Vec<(&'static str, Vec<(&'static str, String)>)> {
        const FAULTS: &[&str] = &[
            "MachineKilled",
            "MachineRebooted",
            "ClogPair",
            "ClogSends",
            "ClogReceives",
            "Partition",
            "ResetProbability",
            "ClockDrift",
            "WallClockJump",
            "DiskFilled",
            "DiskErrors",
            "DiskBitFlips",
        ];
        let trace = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
        let t = trace.clone();
        let report = run(spec, seed, move |r| {
            if FAULTS.contains(&r.name) {
                t.borrow_mut().push((r.name, r.fields.clone()));
            }
        });
        assert!(report.result.is_ok(), "seed {seed}: {:?}", report.result);
        let mut faults = trace.take();
        // Only their order may differ: a timer may fire up to a millisecond late, by a draw
        // from the scheduler's stream, which the system under test shifts.
        faults.sort();
        faults
    }

    #[test]
    fn the_system_under_test_doesnt_move_the_faults() {
        for seed in 0..20 {
            let idle = faults(&IDLE, seed);
            assert!(!idle.is_empty(), "seed {seed}");
            assert_eq!(idle, faults(&BUSY, seed), "seed {seed}");
        }
    }

    /// Turning wipes off turns each wipe into a crash and changes no other fault.
    #[test]
    fn wipes_off_crashes_instead_and_moves_nothing_else() {
        // A wipe gives a machine new disk ids, so a later disk fault names another id.
        let without_disk_ids = |faults: Vec<(&'static str, Vec<(&'static str, String)>)>| {
            faults
                .into_iter()
                .map(|(name, mut fields)| {
                    fields.retain(|&(key, _)| key != "disk");
                    (name, fields)
                })
                .collect::<Vec<_>>()
        };
        let wiped = |faults: &[(&str, Vec<(&str, String)>)]| {
            faults
                .iter()
                .any(|(_, fields)| fields.contains(&("kind", "Wipe".to_owned())))
        };
        let mut wipes = 0;
        for seed in 0..40 {
            let on = faults(&IDLE, seed);
            let off = faults(&IDLE_WITHOUT_WIPES, seed);
            assert!(!wiped(&off), "seed {seed}");
            wipes += u32::from(wiped(&on));
            let mut on_as_crashes: Vec<_> = without_disk_ids(on)
                .into_iter()
                .map(|(name, fields)| {
                    let fields = fields
                        .into_iter()
                        .map(|(k, v)| match (k, v.as_str()) {
                            ("kind", "Wipe") => (k, "Crash".to_owned()),
                            _ => (k, v),
                        })
                        .collect();
                    (name, fields)
                })
                .collect();
            on_as_crashes.sort();
            let mut off = without_disk_ids(off);
            off.sort();
            assert_eq!(on_as_crashes, off, "seed {seed}");
        }
        assert!(wipes > 0);
    }

    #[test]
    fn attrition_takes_down_a_minority_while_power_cuts_may_take_a_majority() {
        let (mut attrition, mut power) = (0, 0);
        for seed in 0..40 {
            let trace = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
            let t = trace.clone();
            let report = run(&IDLE, seed, move |r| t.borrow_mut().push(r.clone()));
            assert!(report.result.is_ok(), "seed {seed}: {:?}", report.result);
            let (a, p) = most_down(&trace.borrow());
            assert!(
                a <= (MACHINES - 1) / 2,
                "seed {seed}: {a} down after a crash"
            );
            attrition = attrition.max(a);
            power = power.max(p);
        }
        // Attrition reaches its limit, and some power cut takes down a majority.
        assert_eq!(attrition, (MACHINES - 1) / 2);
        assert!(power > MACHINES / 2, "{power}");
    }
}
