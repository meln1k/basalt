//! The simulated world: the run loop, and the `Backend` and ambient `World` it implements by
//! delegating to the executor, the clock, and the trace.

use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
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
    TaskId, UNIX_EPOCH,
};

use crate::executor::Executor;
use crate::time::Clock;
use crate::trace::TraceLog;
use crate::{BlockedTask, Hang, SimError};

/// How often a jump to the next timer overshoots it, and by at most how much. Timers may fire
/// late, never early, so code must not depend on exactly when they fire.
const LATE_TIMER_PROBABILITY: f64 = 0.01;
const MAX_TIMER_LATENESS: Duration = Duration::from_millis(1);

/// What the wall clock reads at virtual time zero: 2026-01-01T00:00:00Z.
const WALL_CLOCK_START: Duration = Duration::from_secs(1_767_225_600);

/// The run's seed split into one stream per concern (design doc, "Randomness"). Each draw
/// belongs to exactly one stream, so drawing more from one never shifts the others.
pub(crate) struct Streams {
    /// Picks among ready tasks of equal priority, and how late timers fire.
    pub(crate) scheduler: Rng,
    /// Latency, bandwidth, and random resets (T8).
    pub(crate) network: Rng,
    /// Corruption modes, power-cut outcomes, bit flips, I/O errors, and latency (T9).
    pub(crate) disk: Rng,
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
            network: Rng::derive(seed, "network"),
            disk: Rng::derive(seed, "disk"),
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
        for rng in [scheduler, network, disk, faults, buggify, ctx] {
            rng.state().hash(hasher);
        }
    }
}

pub(crate) struct SimWorld {
    streams: Streams,
    pub(crate) clock: Rc<Clock>,
    pub(crate) executor: Executor,
    pub(crate) trace: TraceLog,
    next_process: Cell<u32>,
    /// Each process's label, for traces and hang reports.
    processes: RefCell<BTreeMap<ProcessId, Rc<str>>>,
    /// The process whose task is being polled.
    polling: Cell<Option<ProcessId>>,
    knobs: Rc<Knobs>,
}

impl SimWorld {
    pub(crate) fn new(seed: u64) -> SimWorld {
        let streams = Streams::new(seed);
        let shuffle = streams.scheduler.bool(0.5);
        SimWorld {
            streams,
            clock: Rc::default(),
            executor: Executor::new(shuffle),
            trace: TraceLog::new(),
            next_process: Cell::new(0),
            processes: RefCell::default(),
            polling: Cell::new(None),
            knobs: Rc::default(),
        }
    }

    pub(crate) fn add_process(self: &Rc<Self>, label: &str) -> Ctx {
        let id = ProcessId::new(self.next_process.get());
        self.next_process.set(id.get() + 1);
        self.processes.borrow_mut().insert(id, label.into());
        Ctx::new(self.clone(), id)
    }

    fn label(&self, process: ProcessId) -> Rc<str> {
        self.processes
            .borrow()
            .get(&process)
            .cloned()
            .expect("every task belongs to a process this world added")
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
        /// Clears the polled process even if the task panics.
        struct Polling<'a>(&'a Cell<Option<ProcessId>>);
        impl Drop for Polling<'_> {
            fn drop(&mut self) {
                self.0.set(None);
            }
        }
        self.polling.set(Some(runnable.metadata().process));
        let _polling = Polling(&self.polling);
        runnable.run();
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
                process: self.label(meta.process),
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
        self.executor.start(first);
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

    fn now(&self, _: ProcessId) -> Instant {
        self.clock.now()
    }

    fn wall_clock(&self, process: ProcessId) -> SystemTime {
        UNIX_EPOCH + WALL_CLOCK_START + Duration::from_nanos(self.now(process).as_nanos())
    }

    fn sleep_until(&self, _: ProcessId, deadline: Instant) -> LocalBoxFuture<'static, ()> {
        Box::pin(self.clock.sleep_until(deadline))
    }

    fn rng(&self) -> &Rng {
        &self.streams.ctx
    }

    fn listen(&self, _: ProcessId, _: u16) -> LocalBoxFuture<'_, io::Result<Box<dyn ListenerIo>>> {
        Box::pin(async { Err(no_network()) })
    }

    fn connect(
        &self,
        _: ProcessId,
        _: SocketAddr,
    ) -> LocalBoxFuture<'_, io::Result<Box<dyn StreamIo>>> {
        Box::pin(async { Err(no_network()) })
    }

    fn disks(&self, _: ProcessId) -> Rc<[DiskId]> {
        Rc::new([])
    }

    fn disk(&self, process: ProcessId, disk: DiskId) -> Rc<dyn DiskIo> {
        panic!("{disk:?} is not a disk of {process:?}, which has none")
    }

    fn compute(
        &self,
        process: ProcessId,
        cost: Cost,
        job: ComputeJob,
    ) -> LocalBoxFuture<'_, ComputeOutput> {
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

fn no_network() -> io::Error {
    io::Error::new(
        io::ErrorKind::Unsupported,
        "the simulated network isn't built yet",
    )
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
        let process = self.polling.get().map(|p| self.label(p));
        self.trace.record(event, self.clock.now(), process);
    }

    fn knobs(&self) -> Rc<Knobs> {
        self.knobs.clone()
    }
}
