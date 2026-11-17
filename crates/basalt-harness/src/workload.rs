//! Workloads and the test specs that put them together.

use std::error::Error;

use basalt_env::backend::LocalBoxFuture;
use basalt_env::{Ctx, Duration};
use basalt_sim::SimControl;

/// What a workload phase returns. A string converts into the error with `.into()` or `?`.
pub type WorkloadResult = Result<(), Box<dyn Error>>;

/// One part of a test: a client that records a [`History`](crate::History), a fault injector,
/// or anything else that acts during a run. The runner calls every workload of a test through
/// each phase together, on the harness's own process, which runs on a machine of its own that no
/// fault can touch ([`SimControl::protect`]):
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
/// An error from any phase, or a phase past its deadline, fails the run. A workload keeps its own `History`, typed by its
/// operations, and reads it back in `check`.
#[allow(
    async_fn_in_trait,
    reason = "simulated workloads run on one thread, so their futures never need to be Send"
)]
pub trait Workload {
    /// Names the workload in failure reports.
    fn name(&self) -> &'static str {
        std::any::type_name::<Self>()
    }

    async fn setup(&mut self, _ctx: &Ctx) -> WorkloadResult {
        Ok(())
    }

    async fn start(&mut self, ctx: &Ctx, sim: &SimControl) -> WorkloadResult;

    /// Shows the system makes progress again once every fault is lifted, such as by a fresh
    /// write and read.
    async fn liveness(&mut self, _ctx: &Ctx) -> WorkloadResult {
        Ok(())
    }

    async fn check(&mut self, _ctx: &Ctx) -> WorkloadResult {
        Ok(())
    }
}

/// [`Workload`] with boxed futures, so a test can hold different workloads in one list.
pub(crate) trait DynWorkload {
    fn name(&self) -> &'static str;
    fn setup<'a>(&'a mut self, ctx: &'a Ctx) -> LocalBoxFuture<'a, WorkloadResult>;
    fn start<'a>(
        &'a mut self,
        ctx: &'a Ctx,
        sim: &'a SimControl,
    ) -> LocalBoxFuture<'a, WorkloadResult>;
    fn liveness<'a>(&'a mut self, ctx: &'a Ctx) -> LocalBoxFuture<'a, WorkloadResult>;
    fn check<'a>(&'a mut self, ctx: &'a Ctx) -> LocalBoxFuture<'a, WorkloadResult>;
}

impl<W: Workload + 'static> DynWorkload for W {
    fn name(&self) -> &'static str {
        Workload::name(self)
    }

    fn setup<'a>(&'a mut self, ctx: &'a Ctx) -> LocalBoxFuture<'a, WorkloadResult> {
        Box::pin(Workload::setup(self, ctx))
    }

    fn start<'a>(
        &'a mut self,
        ctx: &'a Ctx,
        sim: &'a SimControl,
    ) -> LocalBoxFuture<'a, WorkloadResult> {
        Box::pin(Workload::start(self, ctx, sim))
    }

    fn liveness<'a>(&'a mut self, ctx: &'a Ctx) -> LocalBoxFuture<'a, WorkloadResult> {
        Box::pin(Workload::liveness(self, ctx))
    }

    fn check<'a>(&'a mut self, ctx: &'a Ctx) -> LocalBoxFuture<'a, WorkloadResult> {
        Box::pin(Workload::check(self, ctx))
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
