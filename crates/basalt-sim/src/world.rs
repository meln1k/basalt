//! The simulated world: the run loop, and the `Backend` and ambient `World` it implements by
//! delegating to the executor, the clock, and the trace.

use std::cell::{Cell, RefCell};
use std::collections::BTreeSet;
use std::future::pending;
use std::hash::{Hash, Hasher};
use std::io;
use std::rc::Rc;

use basalt_env::backend::{
    Backend, ComputeJob, ComputeOutput, DetachedTask, DiskIo, ListenerIo, LocalBoxFuture, Runnable,
    Scheduler, StreamIo, TaskMeta,
};
use basalt_env::world::{self, BuggifySite, Event, ProbeSite, World};
use basalt_env::{
    Cost, Ctx, DiskId, Duration, Instant, Knobs, Priority, ProcessId, Rng, SocketAddr, SystemTime,
    TaskId, buggify, trace,
};

use crate::buggify::{Buggify, Coverage};
use crate::disk::Disks;
use crate::executor::Executor;
use crate::machine::{EntryPoint, KillType, MachineId, Machines};
use crate::net::Net;
use crate::swarm::Swarm;
use crate::time::Clock;
use crate::trace::TraceLog;
use crate::{BlockedTask, Hang, SimError};

/// How often a jump to the next timer overshoots it, and by at most how much. Timers may fire
/// late, never early, so code must not depend on exactly when they fire.
const LATE_TIMER_PROBABILITY: f64 = 0.01;
const MAX_TIMER_LATENESS: Duration = Duration::from_millis(1);

/// How often the random-cancel `buggify!` site fires, when switched on, each time the run loop
/// is about to move time forward. Rare, since every cancel is a fault the code must survive.
const CANCEL_PROBABILITY: f64 = 0.001;

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
    buggify: Buggify,
    /// The live task of each process that runs its entry point. The random-cancel site
    /// spares them.
    main_tasks: RefCell<BTreeSet<TaskId>>,
    pub(crate) swarm: Swarm,
    knobs: Rc<Knobs>,
}

impl SimWorld {
    pub(crate) fn new(seed: u64) -> SimWorld {
        let streams = Streams::new(seed);
        let mut knobs = Knobs::default();
        let swarm = Swarm::draw(&streams.faults, &mut knobs);
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
            buggify: Buggify::default(),
            main_tasks: RefCell::default(),
            swarm,
            knobs: Rc::new(knobs),
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
        let main = ctx.spawn("main", Priority::Default, entry(ctx.clone()));
        self.main_tasks.borrow_mut().insert(main.id());
        main.detach();
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

    /// Lifts every fault: turns BUGGIFY off for the rest of the run, heals the network, clears
    /// every disk fault, stops every clock's drift, and reboots every machine that's down. The
    /// network and disks recover first, so rebooted processes start on them healed.
    pub(crate) fn lift_faults(self: &Rc<Self>) {
        self.buggify.lift();
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
        self.drop_tasks(|task| !self.machines.alive(task.process));
    }

    /// Drops the future of every task `doomed` picks, each under its own process's trace label,
    /// since its `Drop` code acts for that process.
    fn drop_tasks(&self, doomed: impl Fn(&TaskMeta) -> bool) {
        self.executor.drop_tasks(doomed, |runnable| {
            let _label = self.label_as(runnable.metadata().process);
            drop(runnable);
        });
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

    /// The random-cancel site (design doc, "Risks"): drops the future of one detached task,
    /// picked from the `buggify` stream, as dropping its handle would have. It catches code
    /// that assumes work after an `.await` always completes. Only detached tasks are picked,
    /// because a task whose handle someone awaits can only vanish when its process dies, and
    /// a kill already covers that. A process's main task and every task on a protected
    /// machine are spared, since without them a process or the run itself stops doing
    /// anything at all. Returns whether it cancelled a task.
    fn cancel_random_task(&self) -> bool {
        let mut tasks = self.executor.detached_tasks();
        let main_tasks = self.main_tasks.borrow();
        tasks.retain(|t| {
            !main_tasks.contains(&t.id)
                && !self
                    .machines
                    .is_protected(self.machines.machine_of(t.process))
        });
        drop(main_tasks);
        if tasks.is_empty() {
            return false;
        }
        let victim = tasks[self.streams.buggify.range(0..tasks.len())];
        {
            let _label = self.label_as(victim.process);
            trace!(
                Info,
                "TaskCancelled",
                task = victim.id.get(),
                name = victim.name
            );
        }
        self.drop_tasks(|task| task.id == victim.id);
        true
    }

    pub(crate) fn fault_rng(&self) -> &Rng {
        &self.streams.faults
    }

    pub(crate) fn coverage(&self) -> Coverage {
        self.buggify.coverage()
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
            // Between bursts of work, now and then, rather than on every poll.
            if buggify!(CANCEL_PROBABILITY) && self.cancel_random_task() {
                continue;
            }
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
        self.main_tasks.borrow_mut().remove(&task);
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
        let mut deadline = deadline;
        // Timers may fire late, never early, as in FoundationDB's `delay`. Not on a protected
        // machine, the harness's: its fault workloads time faults from the `faults` stream, and
        // a delay drawn from the `buggify` stream, which the system under test also draws
        // from, would shift them whenever that system changes.
        let protected = self
            .machines
            .is_protected(self.machines.machine_of(process));
        if deadline != Instant::MAX && !protected && buggify!() {
            let extra = self
                .streams
                .buggify
                .range(Duration::ZERO..=self.knobs.max_buggified_delay);
            deadline = deadline.checked_add(extra).unwrap_or(Instant::MAX);
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

    fn buggify(&self, site: &'static BuggifySite, fire: f64) -> bool {
        self.buggify.fires(site, fire, &self.streams.buggify)
    }

    fn probe(&self, site: &'static ProbeSite) {
        self.buggify.probe(site);
    }

    fn trace(&self, event: &Event<'_>) {
        let process = self.polling.get().map(|p| self.machines.label(p));
        self.trace.record(event, self.clock.now(), process);
    }

    fn knobs(&self) -> Rc<Knobs> {
        self.knobs.clone()
    }
}
