//! The simulated world: the run loop, and the `Backend` and ambient `World` it implements by
//! delegating to the executor, the clock, and the trace.

use std::cell::Cell;
use std::future::pending;
use std::hash::{Hash, Hasher};
use std::io;
use std::rc::Rc;

use basalt_env::backend::{
    Backend, ComputeJob, ComputeOutput, DetachedTask, DiskIo, ListenerIo, LocalBoxFuture, Runnable,
    Scheduler, StreamIo,
};
use basalt_env::world::{self, BuggifySite, Event, ProbeSite, World};
use basalt_env::{
    Cost, Ctx, DiskId, Duration, Instant, Knobs, Priority, ProcessId, Rng, SocketAddr, SystemTime,
    TaskId, trace,
};

use crate::disk::Disks;
use crate::executor::Executor;
use crate::machine::{EntryPoint, KillType, MachineId, Machines};
use crate::net::Net;
use crate::time::Clock;
use crate::trace::TraceLog;
use crate::{BlockedTask, Hang, SimError};

/// How often a jump to the next timer overshoots it, and by at most how much. Timers may fire
/// late, never early, so code must not depend on exactly when they fire.
const LATE_TIMER_PROBABILITY: f64 = 0.01;
const MAX_TIMER_LATENESS: Duration = Duration::from_millis(1);

/// The run's seed split into one stream per concern (design doc, "Randomness"). Each draw
/// belongs to exactly one stream, so drawing more from one never shifts the others.
pub(crate) struct Streams {
    /// Picks among ready tasks of equal priority, and how late timers fire.
    pub(crate) scheduler: Rng,
    /// Latencies, short reads and writes, stall timeouts, and random resets, shared with the
    /// network.
    pub(crate) network: Rc<Rng>,
    /// Corruption modes, power-cut outcomes, bit flips, I/O errors, and latency. Shared with
    /// the disks.
    pub(crate) disk: Rc<Rng>,
    /// The swarm config and the fault workloads' targets and timing (T10, T12).
    pub(crate) faults: Rng,
    /// BUGGIFY site switches and firing (T10).
    pub(crate) buggify: Rng,
    /// What `ctx.rng()` returns.
    pub(crate) ctx: Rng,
}

impl Streams {
    fn new(seed: u64) -> Streams {
        Streams {
            scheduler: Rng::derive(seed, "scheduler"),
            network: Rc::new(Rng::derive(seed, "network")),
            disk: Rc::new(Rng::derive(seed, "disk")),
            faults: Rng::derive(seed, "faults"),
            buggify: Rng::derive(seed, "buggify"),
            ctx: Rng::derive(seed, "ctx"),
        }
    }

    fn hash_states(&self, hasher: &mut impl Hasher) {
        let Streams {
            scheduler,
            network,
            disk,
            faults,
            buggify,
            ctx,
        } = self;
        for rng in [scheduler, &**network, &**disk, faults, buggify, ctx] {
            rng.state().hash(hasher);
        }
    }
}

pub(crate) struct SimWorld {
    pub(crate) seed: u64,
    streams: Streams,
    pub(crate) clock: Rc<Clock>,
    pub(crate) executor: Executor,
    pub(crate) trace: TraceLog,
    pub(crate) machines: Rc<Machines>,
    pub(crate) net: Rc<Net>,
    pub(crate) disks: Rc<Disks>,
    /// The process whose task is being polled or dropped, for trace labels.
    polling: Cell<Option<ProcessId>>,
    knobs: Rc<Knobs>,
}

impl SimWorld {
    pub(crate) fn new(seed: u64) -> SimWorld {
        let streams = Streams::new(seed);
        let shuffle = streams.scheduler.bool(0.5);
        let clock = Rc::<Clock>::default();
        let machines = Rc::new(Machines::new(clock.clone()));
        let net = Rc::new(Net::new(
            clock.clone(),
            machines.clone(),
            streams.network.clone(),
        ));
        let disks = Rc::new(Disks::new(
            machines.clone(),
            clock.clone(),
            streams.disk.clone(),
        ));
        SimWorld {
            seed,
            streams,
            machines,
            net,
            disks,
            clock,
            executor: Executor::new(shuffle),
            trace: TraceLog::new(),
            polling: Cell::new(None),
            knobs: Rc::default(),
        }
    }

    pub(crate) fn add_process(
        self: &Rc<Self>,
        machine: MachineId,
        label: &str,
        entry: EntryPoint,
    ) -> Ctx {
        let id = self.machines.add_process(machine, label, entry.clone());
        self.start(id, &entry)
    }

    /// Runs the entry point as the process's `main` task.
    fn start(self: &Rc<Self>, process: ProcessId, entry: &EntryPoint) -> Ctx {
        let ctx = Ctx::new(self.clone(), process);
        ctx.spawn("main", Priority::Default, entry(ctx.clone()))
            .detach();
        ctx
    }

    /// Kills every process on `machine` and leaves it down, following the kill rules in
    /// `basalt_env::backend`. Every future of those processes is dropped before this returns,
    /// except that of a task killing its own machine, which [`poll`](Self::poll) drops as soon
    /// as its current poll ends.
    pub(crate) fn kill(&self, machine: MachineId, kind: KillType) {
        let disks = self.machines.kill(machine, kind);
        trace!(Info, "MachineKilled", machine = machine.get(), kind = kind);
        self.net.processes_died();
        match kind {
            // The page cache outlives the processes.
            KillType::Crash => {}
            KillType::PowerCut => self.disks.power_cut(&disks),
            KillType::DiskFail | KillType::Wipe => self.disks.remove(&disks),
        }
        self.reap();
    }

    /// Brings `machine` back up, after a `Crash` kill if it's up, and starts a new incarnation
    /// of each of its processes.
    pub(crate) fn reboot(self: &Rc<Self>, machine: MachineId) {
        if self.machines.is_up(machine) {
            self.kill(machine, KillType::Crash);
        }
        let starts = self.machines.reboot(machine);
        trace!(Info, "MachineRebooted", machine = machine.get());
        for (id, entry) in starts {
            self.start(id, &entry);
        }
    }

    /// Lifts every fault: heals the network, clears every disk fault, stops every clock's
    /// drift, and reboots every machine that's down. The network and disks recover first, so
    /// rebooted processes start on them healed.
    pub(crate) fn lift_faults(self: &Rc<Self>) {
        self.net.heal();
        self.disks.restore();
        for machine in self.machines.ids() {
            let clock = self.machines.clock(machine);
            // Only when drifting: a drift change wakes every sleeper on the machine.
            if clock.drift() != 0 {
                clock.set_drift(0);
            }
            if !self.machines.is_up(machine) {
                self.reboot(machine);
            }
        }
        trace!(Info, "FaultsLifted");
    }

    /// Drops every task of every dead process, each under its own process's trace label, since
    /// its `Drop` code acts for that process.
    fn reap(&self) {
        self.executor.drop_tasks(
            |task| !self.machines.alive(task.process),
            |runnable| {
                let _label = self.label_as(runnable.metadata().process);
                drop(runnable);
            },
        );
    }

    /// Labels trace events with `process` until the guard drops, even if a task panics.
    fn label_as(&self, process: ProcessId) -> impl Drop + '_ {
        struct Restore<'a>(&'a Cell<Option<ProcessId>>, Option<ProcessId>);
        impl Drop for Restore<'_> {
            fn drop(&mut self) {
                self.0.set(self.1);
            }
        }
        Restore(&self.polling, self.polling.replace(Some(process)))
    }

    pub(crate) fn unseed(&self) -> u64 {
        let mut hasher = self.trace.hasher();
        self.streams.hash_states(&mut hasher);
        hasher.finish()
    }

    /// The run loop, after FoundationDB's `Sim2::runLoop`: while `more` holds, polls the next
    /// ready task, or if none is ready, moves time to the next timer. Stops at the first `Error`
    /// trace, or when no task can make progress.
    pub(crate) fn run_while(&self, mut more: impl FnMut() -> bool) -> Result<(), SimError> {
        loop {
            if let Some(record) = self.trace.first_error() {
                return Err(SimError::ErrorTrace(record));
            }
            if !more() {
                return Ok(());
            }
            if let Some(runnable) = self.executor.pop_ready(&self.streams.scheduler) {
                self.poll(runnable);
                continue;
            }
            let Some(next) = self.clock.next_deadline() else {
                return Err(SimError::Hang(self.hang()));
            };
            let rng = &self.streams.scheduler;
            let mut to = next;
            if rng.bool(LATE_TIMER_PROBABILITY) {
                to = to
                    .checked_add(rng.range(Duration::ZERO..=MAX_TIMER_LATENESS))
                    .unwrap_or(next);
            }
            self.clock.advance_to(to);
        }
    }

    fn poll(&self, runnable: Runnable) {
        let process = runnable.metadata().process;
        // Defensive, as the design's run loop says: a kill already drops every queued runnable
        // of its processes, and `async-task` never queues a dropped task again.
        if !self.machines.alive(process) {
            drop(runnable);
            return;
        }
        {
            let _label = self.label_as(process);
            runnable.run();
        }
        // A task that killed its own process was still running during the kill, so it may have
        // been queued again just now. Drop it before any other task runs.
        if !self.machines.alive(process) {
            self.reap();
        }
    }

    fn hang(&self) -> Hang {
        let blocked = self
            .executor
            .live_tasks()
            .into_iter()
            .map(|meta| BlockedTask {
                id: meta.id,
                name: meta.name,
                priority: meta.priority,
                process: self.machines.label(meta.process),
            })
            .collect();
        Hang {
            time: self.clock.now(),
            blocked,
        }
    }
}

impl Backend for SimWorld {
    fn register_task(&self, _: ProcessId, _: &'static str, _: Priority) -> TaskId {
        self.executor.next_id()
    }

    fn start_task(&self, first: Runnable) {
        let process = first.metadata().process;
        self.executor.start(first);
        if !self.machines.alive(process) {
            self.reap();
        }
    }

    fn scheduler(&self) -> Scheduler {
        self.executor.scheduler()
    }

    fn task_ended(&self, task: TaskId) {
        self.executor.ended(task);
    }

    fn detach(&self, task: DetachedTask) {
        self.executor.detach(task);
    }

    fn now(&self, process: ProcessId) -> Instant {
        self.machines.clock_of(process).now()
    }

    fn wall_clock(&self, process: ProcessId) -> SystemTime {
        self.machines.clock_of(process).wall()
    }

    fn sleep_until(&self, process: ProcessId, deadline: Instant) -> LocalBoxFuture<'static, ()> {
        if !self.machines.alive(process) {
            return Box::pin(pending());
        }
        Box::pin(self.machines.clock_of(process).sleep_until(deadline))
    }

    fn rng(&self) -> &Rng {
        &self.streams.ctx
    }

    fn listen(
        &self,
        process: ProcessId,
        port: u16,
    ) -> LocalBoxFuture<'_, io::Result<Box<dyn ListenerIo>>> {
        let listener = self.net.listen(process, port);
        Box::pin(async { listener })
    }

    fn connect(
        &self,
        process: ProcessId,
        addr: SocketAddr,
    ) -> LocalBoxFuture<'_, io::Result<Box<dyn StreamIo>>> {
        Box::pin(self.net.clone().connect(process, addr))
    }

    fn disks(&self, process: ProcessId) -> Rc<[DiskId]> {
        self.machines.disks_of(process)
    }

    fn disk(&self, process: ProcessId, disk: DiskId) -> Rc<dyn DiskIo> {
        // A dead process may still hold the id of a disk its machine has since lost to a wipe.
        if self.machines.alive(process) {
            let disks = self.disks(process);
            assert!(
                disks.contains(&disk),
                "{disk:?} is not a disk of {process:?}, which has {disks:?}"
            );
        }
        Rc::new(self.disks.disk(process, disk))
    }

    fn compute(
        &self,
        process: ProcessId,
        cost: Cost,
        job: ComputeJob,
    ) -> LocalBoxFuture<'_, ComputeOutput> {
        if !self.machines.alive(process) {
            return Box::pin(pending());
        }
        let output = world::without(job);
        let done = self
            .now(process)
            .checked_add(cost.duration())
            .unwrap_or(Instant::MAX);
        let sleep = self.sleep_until(process, done);
        Box::pin(async move {
            sleep.await;
            output
        })
    }
}

impl World for SimWorld {
    fn is_simulated(&self) -> bool {
        true
    }

    fn buggify(&self, _: &'static BuggifySite, _: f64) -> bool {
        // Site switches and firing come with T10.
        false
    }

    fn probe(&self, _: &'static ProbeSite) {}

    fn trace(&self, event: &Event<'_>) {
        let process = self.polling.get().map(|p| self.machines.label(p));
        self.trace.record(event, self.clock.now(), process);
    }

    fn knobs(&self) -> Rc<Knobs> {
        self.knobs.clone()
    }
}
