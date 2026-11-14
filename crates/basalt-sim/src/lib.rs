//! The simulated world: every process, the network, disks, and clocks in one thread on virtual
//! time, driven by one seed. It implements `basalt_env::backend::Backend`.
//!
//! [`Sim`] owns a world and its run loop, which follows FoundationDB's `Sim2::runLoop`: run the
//! ready task of the highest priority, and when none is ready, jump virtual time to the next
//! timer. [`SimControl`] is the handle tests and fault workloads use to shape the world.
//!
//! A run fails when no task can make progress ([`SimError::Hang`]) or when any code records an
//! `Error` trace event ([`SimError::ErrorTrace`]). A panic in a task propagates out of the run.

mod executor;
mod time;
mod trace;
mod world;

use std::cell::Ref;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use basalt_env::world::WorldGuard;
use basalt_env::{Ctx, Instant, Priority, Task, TaskId};

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

    /// A hash of every random stream's state and of the trace so far. Running a seed twice on
    /// the same build must give the same unseed, or the run is nondeterministic.
    pub fn unseed(&self) -> u64 {
        self.world.unseed()
    }
}

impl Drop for Sim {
    fn drop(&mut self) {
        // While the world is still installed, in case some `Drop` code traces.
        self.world.executor.drop_tasks(|_| true);
    }
}

impl fmt::Debug for Sim {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sim")
            .field("now", &self.now())
            .finish_non_exhaustive()
    }
}

/// Shapes a running simulation: adds processes now, and kills, clock skew, network and disk
/// faults as later tasks add them. It lives in `basalt-sim`, so production code can't reach it.
#[derive(Clone)]
pub struct SimControl {
    world: Rc<SimWorld>,
}

impl SimControl {
    /// Adds a process and returns its `Ctx`. `label` names it in traces and hang reports.
    pub fn add_process(&self, label: &str) -> Ctx {
        self.world.add_process(label)
    }

    /// The current virtual time.
    pub fn now(&self) -> Instant {
        self.world.clock.now()
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
