//! Workloads and the test specs that put them together.

use std::cell::RefCell;
use std::error::Error;
use std::rc::Rc;

use basalt_env::backend::LocalBoxFuture;
use basalt_env::{Ctx, Duration};
use basalt_sim::{MachineId, SimControl};

/// What a workload phase returns. A string converts into the error with `.into()` or `?`.
pub type WorkloadResult = Result<(), Box<dyn Error>>;

/// One part of a test: a client that records a [`History`](crate::History), a fault injector,
/// or anything else that acts during a run. The runner drives every workload of a test through
/// each phase together, from outside every process ([`Sim::block_on`](basalt_sim::Sim::block_on)),
/// so no kill or fault can stop a workload itself. A workload waits on the world's clock with
/// [`SimControl::sleep`], and acts as a process through a [`Process`] it added in the test's
/// `build`, which faults hit like any other:
///
/// 1. [`setup`](Workload::setup), before any fault, within
///    [`PHASE_DEADLINE`](crate::runner::PHASE_DEADLINE).
/// 2. [`start`](Workload::start), the chaos phase. It ends after the test's chaos duration,
///    dropping every `start` future still running, or as soon as every one has returned.
/// 3. The quiet phase: the runner lifts every fault ([`SimControl::lift_faults`]), then
///    [`liveness`](Workload::liveness) must return within the test's quiet deadline.
/// 4. [`check`](Workload::check), which runs the checkers over what the workload recorded,
///    within [`PHASE_DEADLINE`](crate::runner::PHASE_DEADLINE).
///
/// An error from any phase, or a phase past its deadline, fails the run. A workload keeps its
/// own `History`, typed by its operations, and reads it back in `check`.
#[allow(
    async_fn_in_trait,
    reason = "simulated workloads run on one thread, so their futures never need to be Send"
)]
pub trait Workload {
    /// Names the workload in failure reports.
    fn name(&self) -> &'static str {
        std::any::type_name::<Self>()
    }

    async fn setup(&mut self, _sim: &SimControl) -> WorkloadResult {
        Ok(())
    }

    async fn start(&mut self, sim: &SimControl) -> WorkloadResult;

    /// Shows the system makes progress again once every fault is lifted, such as by a fresh
    /// write and read.
    async fn liveness(&mut self, _sim: &SimControl) -> WorkloadResult {
        Ok(())
    }

    async fn check(&mut self, _sim: &SimControl) -> WorkloadResult {
        Ok(())
    }
}

/// A process a workload acts through, such as a client of the system under test. It does
/// nothing on its own. It runs on a machine like any other, so faults hit it: a kill drops
/// every task it was running for the workload, and its old `Ctx` stays dead. [`ctx`](Process::ctx)
/// always returns the current incarnation's.
#[derive(Clone)]
pub struct Process {
    current: Rc<RefCell<(u64, Option<Ctx>)>>,
}

impl Process {
    /// Adds the process to `machine` as `label`.
    pub fn add(sim: &SimControl, machine: MachineId, label: &str) -> Process {
        let current: Rc<RefCell<(u64, Option<Ctx>)>> = Rc::default();
        // Weak, since the world keeps the entry point and a `Ctx` keeps the world: a strong
        // reference would make a cycle that leaks the whole world when the run ends.
        let c = Rc::downgrade(&current);
        sim.add_process(machine, label, move |ctx| {
            if let Some(current) = c.upgrade() {
                let mut current = current.borrow_mut();
                current.0 += 1;
                current.1 = Some(ctx);
            }
            Box::pin(async {})
        });
        Process { current }
    }

    /// The current incarnation's `Ctx`. While the machine is down it's the last one, which is
    /// dead: its sleeps never end and its I/O fails, so bound waits on it with
    /// [`SimControl::timeout`].
    pub fn ctx(&self) -> Ctx {
        self.current
            .borrow()
            .1
            .clone()
            .expect("set when the process was added")
    }

    /// Which incarnation [`ctx`](Process::ctx) belongs to, counting from 1. It changes on
    /// every reboot.
    pub fn incarnation(&self) -> u64 {
        self.current.borrow().0
    }
}

/// [`Workload`] with boxed futures, so a test can hold different workloads in one list.
pub(crate) trait DynWorkload {
    fn name(&self) -> &'static str;
    fn setup<'a>(&'a mut self, sim: &'a SimControl) -> LocalBoxFuture<'a, WorkloadResult>;
    fn start<'a>(&'a mut self, sim: &'a SimControl) -> LocalBoxFuture<'a, WorkloadResult>;
    fn liveness<'a>(&'a mut self, sim: &'a SimControl) -> LocalBoxFuture<'a, WorkloadResult>;
    fn check<'a>(&'a mut self, sim: &'a SimControl) -> LocalBoxFuture<'a, WorkloadResult>;
}

impl<W: Workload + 'static> DynWorkload for W {
    fn name(&self) -> &'static str {
        Workload::name(self)
    }

    fn setup<'a>(&'a mut self, sim: &'a SimControl) -> LocalBoxFuture<'a, WorkloadResult> {
        Box::pin(Workload::setup(self, sim))
    }

    fn start<'a>(&'a mut self, sim: &'a SimControl) -> LocalBoxFuture<'a, WorkloadResult> {
        Box::pin(Workload::start(self, sim))
    }

    fn liveness<'a>(&'a mut self, sim: &'a SimControl) -> LocalBoxFuture<'a, WorkloadResult> {
        Box::pin(Workload::liveness(self, sim))
    }

    fn check<'a>(&'a mut self, sim: &'a SimControl) -> LocalBoxFuture<'a, WorkloadResult> {
        Box::pin(Workload::check(self, sim))
    }
}

/// The workloads of one run of a test.
#[derive(Default)]
pub struct Workloads(pub(crate) Vec<Box<dyn DynWorkload>>);

impl Workloads {
    pub fn new() -> Workloads {
        Workloads::default()
    }

    pub fn add(&mut self, workload: impl Workload + 'static) -> &mut Workloads {
        self.0.push(Box::new(workload));
        self
    }
}

/// A test built into the binary, which `basalt sim list` shows and `basalt sim run --test`
/// names. Specs are plain constants; the `basalt` binary lists every crate's specs.
#[derive(Clone, Copy, Debug)]
pub struct TestSpec {
    /// The name `--test` takes.
    pub name: &'static str,
    /// One line for `basalt sim list`.
    pub about: &'static str,
    /// How long the chaos phase lasts, in virtual time.
    pub chaos: Duration,
    /// How long, in virtual time, the workloads' liveness checks may take after every fault is
    /// lifted.
    pub quiet: Duration,
    /// Builds the system under test with `sim` (its machines and processes) and returns the
    /// workloads that drive it. Take every random choice from `ctx.rng()` in the workloads.
    pub build: fn(sim: &SimControl) -> Workloads,
}
