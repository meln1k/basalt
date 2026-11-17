//! The simulated world: every process, the network, disks, and clocks in one thread on virtual
//! time, driven by one seed. It implements `basalt_env::backend::Backend`.
//!
//! [`Sim`] owns a world and its run loop, which follows FoundationDB's `Sim2::runLoop`: run the
//! ready task of the highest priority, and when none is ready, jump virtual time to the next
//! timer. [`SimControl`] is the handle tests and fault workloads use to shape the world.
//!
//! Processes run on machines, each with a failure zone, disks, and its own drifting clocks. A
//! process is an entry point that reruns as a new incarnation, with a fresh `ProcessId`, each
//! time its machine reboots. Killing a machine drops every future of its processes at once, by
//! the kill and dead-process rules in `basalt_env::backend`.
//!
//! Processes talk over a simulated network of TCP-like connections, which [`SimControl`] can
//! clog, partition, and reset. Machine `n` has address `10.0.0.1 + n`
//! ([`SimControl::ip`]), and `127.0.0.1` reaches a process's own machine.
//!
//! Disks keep only what's synced through a power cut (see [`KillType`]), and take virtual time
//! for each operation by an IOPS and bandwidth model. Every disk fault, power-cut outcome, and
//! latency draws from the `disk` stream.
//!
//! A run fails when no task can make progress ([`SimError::Hang`]) or when any code records an
//! `Error` trace event ([`SimError::ErrorTrace`]). A panic in a task propagates out of the run.

mod disk;
mod executor;
mod machine;
mod net;
mod time;
mod trace;
mod world;

use std::cell::Ref;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use basalt_env::backend::LocalBoxFuture;
use basalt_env::world::WorldGuard;
use basalt_env::{
    Ctx, DiskId, Duration, Instant, IpAddr, Priority, SystemTime, Task, TaskId, trace,
};

pub use crate::machine::{KillType, MachineId};
pub use crate::trace::TraceRecord;
use crate::world::SimWorld;

/// A simulated world and its run loop, for one seed.
///
/// Creating a `Sim` installs its ambient world on the current thread, so `trace!` and the other
/// diagnostics reach it, until the `Sim` is dropped. Dropping it also drops every task left.
pub struct Sim {
    world: Rc<SimWorld>,
    _guard: WorldGuard,
}

impl Sim {
    /// A world with no processes, at virtual time zero. Panics if another world is installed on
    /// this thread.
    pub fn new(seed: u64) -> Sim {
        let world = Rc::new(SimWorld::new(seed));
        let guard = basalt_env::world::install(world.clone());
        Sim {
            world,
            _guard: guard,
        }
    }

    /// The handle for adding processes and, later, injecting faults.
    pub fn control(&self) -> SimControl {
        SimControl {
            world: self.world.clone(),
        }
    }

    /// Runs until every task has ended.
    pub fn run(&self) -> Result<(), SimError> {
        self.world
            .run_while(|| self.world.executor.has_live_tasks())
    }

    /// Runs until `task` completes and returns its output. Other tasks may still be live.
    pub fn block_on<T>(&self, mut task: Task<T>) -> Result<T, SimError> {
        let mut output = None;
        self.world.run_while(|| {
            if let Poll::Ready(out) =
                Pin::new(&mut task).poll(&mut Context::from_waker(Waker::noop()))
            {
                output = Some(out);
            }
            output.is_none()
        })?;
        Ok(output.expect("run_while stops only once the task completed"))
    }

    /// The current virtual time.
    pub fn now(&self) -> Instant {
        self.world.clock.now()
    }

    /// Every trace event recorded so far, in order.
    pub fn trace(&self) -> Ref<'_, [TraceRecord]> {
        self.world.trace.records()
    }

    /// Calls `sink` with each trace event as it's recorded, for example to print a run's trace
    /// as it happens, so a panic doesn't lose it. Replaces any earlier sink.
    pub fn on_trace(&self, sink: impl FnMut(&TraceRecord) + 'static) {
        self.world.trace.set_sink(Box::new(sink));
    }

    /// A hash of every random stream's state and of the trace so far. Running a seed twice on
    /// the same build must give the same unseed, or the run is nondeterministic.
    pub fn unseed(&self) -> u64 {
        self.world.unseed()
    }
}

impl Drop for Sim {
    fn drop(&mut self) {
        // While the world is still installed, in case some `Drop` code traces.
        self.world.executor.drop_tasks(|_| true, drop);
    }
}

impl fmt::Debug for Sim {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sim")
            .field("now", &self.now())
            .finish_non_exhaustive()
    }
}

/// Shapes a running simulation: adds machines and processes, kills and reboots machines, skews
/// their clocks, and injects network and disk faults. It
/// lives in `basalt-sim`, so production code can't reach it.
///
/// Every method may be called from inside a task, including one on the machine it kills.
#[derive(Clone)]
pub struct SimControl {
    world: Rc<SimWorld>,
}

impl SimControl {
    /// Adds a running machine in failure zone `zone` with `disks` disks. Its clocks start with
    /// no drift, and its wall clock at 2026-01-01T00:00:00Z.
    pub fn add_machine(&self, zone: &str, disks: usize) -> MachineId {
        self.world.machines.add_machine(zone, disks)
    }

    /// Adds a process to a running machine and starts it: `entry` runs as the process's `main`
    /// task, and runs again with a new `Ctx` each time the machine reboots. Returns this first
    /// incarnation's `Ctx`. `label` names the process in traces and hang reports.
    pub fn add_process(
        &self,
        machine: MachineId,
        label: &str,
        entry: impl Fn(Ctx) -> LocalBoxFuture<'static, ()> + 'static,
    ) -> Ctx {
        self.world.add_process(machine, label, Rc::new(entry))
    }

    /// Kills every process on `machine` and leaves the machine down until
    /// [`reboot`](SimControl::reboot). Every future of those processes is dropped before this
    /// returns, and their `Ctx`s stay dead. The one exception is a task killing its own
    /// machine: it is dropped as soon as its current poll ends, before any other task runs.
    /// Killing a machine that's down applies only what `kind` does to its disks.
    pub fn kill(&self, machine: MachineId, kind: KillType) {
        self.refuse_if_protected(machine, "kill");
        self.world.kill(machine, kind);
    }

    /// Starts a new incarnation of each process on `machine`, with a fresh `ProcessId`, by
    /// rerunning its entry point. A machine that's up is first killed with
    /// [`KillType::Crash`].
    pub fn reboot(&self, machine: MachineId) {
        self.refuse_if_protected(machine, "reboot");
        self.world.reboot(machine);
    }

    /// Whether `machine` is running, rather than killed and not yet rebooted.
    pub fn is_up(&self, machine: MachineId) -> bool {
        self.world.machines.is_up(machine)
    }

    /// Every machine added so far that isn't [protected](SimControl::protect), up or down, in
    /// the order they were added. Fault workloads pick their targets from these.
    pub fn machines(&self) -> Vec<MachineId> {
        let machines = &self.world.machines;
        let mut ids = machines.ids();
        ids.retain(|&m| !machines.is_protected(m));
        ids
    }

    /// Protects `machine` from faults: it's left out of [`machines`](SimControl::machines),
    /// and killing, rebooting, or skewing the clocks of it panics. The test harness protects
    /// the machine its workloads run on, since a run can't end once that machine is gone.
    pub fn protect(&self, machine: MachineId) {
        self.world.machines.protect(machine);
    }

    /// Lifts every fault, for a test's quiet phase: heals the network as
    /// [`heal_network`](SimControl::heal_network) does, clears every disk fault as
    /// [`restore_disks`](SimControl::restore_disks) does, reboots every machine that's down, and
    /// stops every clock's drift. Wall-clock jumps stay, since undoing one is another jump, and
    /// so does whatever a power cut, wipe, or disk failure lost.
    /// Each kind of fault the simulator models is lifted here, next to the state it owns.
    pub fn lift_faults(&self) {
        self.world.lift_faults();
    }

    /// The run's seed, to name a run, such as a self-test planting a failure at a known seed.
    /// Draw randomness from `ctx.rng()`, never from the seed.
    pub fn seed(&self) -> u64 {
        self.world.seed
    }

    fn refuse_if_protected(&self, machine: MachineId, fault: &str) {
        assert!(
            !self.world.machines.is_protected(machine),
            "{fault} on {machine:?}, which is protected from faults"
        );
    }

    /// The disks of `machine` right now. A [`KillType::Wipe`] replaces them with new ones, and a
    /// [`KillType::DiskFail`] leaves none.
    pub fn disks(&self, machine: MachineId) -> Rc<[DiskId]> {
        self.world.machines.disks(machine)
    }

    /// Fills `disk`: until [`restore_disks`](SimControl::restore_disks), creating a file and
    /// anything that makes a file longer fail with `StorageFull` (`ENOSPC`), and
    /// [`Disk::space`](basalt_env::Disk::space) reports nothing available. Panics if no machine
    /// has `disk`.
    pub fn fill_disk(&self, disk: DiskId) {
        self.world.disks.fill(disk);
    }

    /// Makes each read and write on `disk` fail with `EIO` with probability `p`, until
    /// [`restore_disks`](SimControl::restore_disks). A failed write changes nothing. Panics if
    /// no machine has `disk`, or unless `0 <= p <= 1`.
    pub fn set_disk_errors(&self, disk: DiskId, p: f64) {
        self.world.disks.set_io_errors(disk, p);
    }

    /// Makes each read on `disk` return one flipped bit with probability `p`, until
    /// [`restore_disks`](SimControl::restore_disks). The data on the disk stays intact. Panics
    /// if no machine has `disk`, or unless `0 <= p <= 1`.
    pub fn set_bit_flips(&self, disk: DiskId, p: f64) {
        self.world.disks.set_bit_flips(disk, p);
    }

    /// Lifts every disk fault on every disk, for the quiet phase: full disks get their space
    /// back, and reads and writes stop failing or flipping bits. What a power cut lost stays
    /// lost.
    pub fn restore_disks(&self) {
        self.world.disks.restore();
    }

    /// The failure zone `machine` was added in.
    pub fn zone(&self, machine: MachineId) -> Rc<str> {
        self.world.machines.zone(machine)
    }

    /// Skews `machine`'s clocks: from now on its monotonic clock, and the wall clock that
    /// follows it, run `ppm` parts per million faster than virtual time, or slower if
    /// negative. Its readings stay continuous. Panics unless `ppm` is within ±999,999.
    ///
    /// Timers already set on the machine move to the new rate.
    pub fn set_clock_drift(&self, machine: MachineId, ppm: i32) {
        self.refuse_if_protected(machine, "set_clock_drift");
        self.world.machines.clock(machine).set_drift(ppm);
    }

    /// Jumps `machine`'s wall clock, forward or back, so that it reads `to` now. Its monotonic
    /// clock is unchanged.
    pub fn set_wall_clock(&self, machine: MachineId, to: SystemTime) {
        self.refuse_if_protected(machine, "set_wall_clock");
        self.world.machines.clock(machine).set_wall(to);
    }

    /// What `machine`'s wall clock reads now.
    pub fn wall_clock(&self, machine: MachineId) -> SystemTime {
        self.world.machines.clock(machine).wall()
    }

    /// The current virtual time.
    pub fn now(&self) -> Instant {
        self.world.clock.now()
    }

    // Network

    /// `machine`'s address on the simulated network. Its processes listen on it, on every port.
    pub fn ip(&self, machine: MachineId) -> IpAddr {
        net::ip(machine)
    }

    /// Delays everything `from` sends to `to`, from now for `d`: bytes and connects that would
    /// arrive meanwhile arrive when the clog ends instead. Lengthens a clog that's still on.
    pub fn clog_pair(&self, from: MachineId, to: MachineId, d: Duration) {
        trace!(
            Info,
            "ClogPair",
            from = from.get(),
            to = to.get(),
            duration = d
        );
        self.world.net.clog_pair(from, to, d);
    }

    /// Like [`clog_pair`](SimControl::clog_pair), for everything `machine` sends.
    pub fn clog_sends(&self, machine: MachineId, d: Duration) {
        trace!(Info, "ClogSends", machine = machine.get(), duration = d);
        self.world.net.clog_sends(machine, d);
    }

    /// Like [`clog_pair`](SimControl::clog_pair), for everything `machine` receives.
    pub fn clog_receives(&self, machine: MachineId, d: Duration) {
        trace!(Info, "ClogReceives", machine = machine.get(), duration = d);
        self.world.net.clog_receives(machine, d);
    }

    /// Stops everything `from` sends to `to` from arriving, until
    /// [`heal_network`](SimControl::heal_network). Connects time out, and a connection that has
    /// something held back resets once it has waited 5 to 30 seconds, drawn per connection, as
    /// TCP gives up retransmitting. Bytes from `to` to `from` still arrive.
    pub fn partition(&self, from: MachineId, to: MachineId) {
        trace!(Info, "Partition", from = from.get(), to = to.get());
        self.world.net.partition(from, to);
    }

    /// [`partition`](SimControl::partition) in both directions.
    pub fn partition_two_way(&self, a: MachineId, b: MachineId) {
        self.partition(a, b);
        self.partition(b, a);
    }

    /// Resets connections at random: from now on, each write resets its connection with
    /// probability `p`, until [`heal_network`](SimControl::heal_network). Panics unless
    /// `0 <= p <= 1`.
    pub fn set_reset_probability(&self, p: f64) {
        trace!(Info, "ResetProbability", p = p);
        self.world.net.set_reset_probability(p);
    }

    /// Lifts every network fault, for the quiet phase: ends every clog and partition and stops
    /// random resets. Bytes held back arrive at once. A connection whose stall timeout has
    /// already passed stays reset.
    pub fn heal_network(&self) {
        trace!(Info, "NetworkHealed");
        self.world.net.heal();
    }
}

impl fmt::Debug for SimControl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SimControl").finish_non_exhaustive()
    }
}

/// Why a simulated run failed.
#[derive(Clone, Debug, PartialEq)]
pub enum SimError {
    /// Some task is still live, but none is ready and no timer is pending.
    Hang(Hang),
    /// Code recorded an `Error` trace event, which must never happen.
    ErrorTrace(TraceRecord),
}

impl fmt::Display for SimError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SimError::Hang(hang) => hang.fmt(f),
            SimError::ErrorTrace(record) => write!(f, "error trace: {record}"),
        }
    }
}

impl std::error::Error for SimError {}

/// A run in which no task can make progress.
#[derive(Clone, Debug, PartialEq)]
pub struct Hang {
    pub time: Instant,
    /// Every live task, by id.
    pub blocked: Vec<BlockedTask>,
}

impl fmt::Display for Hang {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "hang at {}: no task is ready and no timer is pending. Blocked tasks:",
            Seconds(self.time)
        )?;
        for task in &self.blocked {
            write!(f, "\n  {task}")?;
        }
        Ok(())
    }
}

/// A task that was live when a run hung.
#[derive(Clone, Debug, PartialEq)]
pub struct BlockedTask {
    pub id: TaskId,
    pub name: &'static str,
    pub priority: Priority,
    /// The label of the task's process.
    pub process: Rc<str>,
}

impl fmt::Display for BlockedTask {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:?} on {} ({:?}, task {})",
            self.name,
            self.process,
            self.priority,
            self.id.get()
        )
    }
}

/// Formats an instant as seconds with nanosecond precision.
struct Seconds(Instant);

impl fmt::Display for Seconds {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let nanos = self.0.as_nanos();
        write!(f, "{}.{:09}s", nanos / 1_000_000_000, nanos % 1_000_000_000)
    }
}
