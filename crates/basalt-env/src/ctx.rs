use std::fmt;
use std::future::{Future, poll_fn};
use std::io;
use std::pin::{Pin, pin};
use std::rc::Rc;
use std::task::{Context, Poll};

use crate::backend::{Backend, ComputeOutput};
use crate::disk::{Disk, DiskId};
use crate::net::{Listener, SocketAddr, Stream};
use crate::task::{TaskEnd, TaskMeta};
use crate::{Duration, Elapsed, Instant, Priority, ProcessId, Rng, Sleep, SystemTime, Task};

/// A process's handle on the world: tasks, clocks, randomness, the network, disks, and compute.
///
/// A task spawned, a connection opened, or a file written through a process's `Ctx` belongs to
/// that process, wherever the call runs. Once the process is killed its `Ctx` stays dead, even
/// after a reboot, which gets a new `Ctx`. `Ctx` is a cheap clone of an `Rc`, so it isn't
/// `Send`: closures passed to [`compute`](Ctx::compute) can't capture it, and pool threads can't
/// do I/O or read the clock.
///
/// ```compile_fail,E0277
/// fn assert_send<T: Send>() {}
/// assert_send::<basalt_env::Ctx>();
/// ```
#[derive(Clone)]
pub struct Ctx {
    backend: Rc<dyn Backend>,
    process: ProcessId,
}

impl Ctx {
    /// For backends: the context of `process` in `backend`'s world.
    pub fn new(backend: Rc<dyn Backend>, process: ProcessId) -> Ctx {
        Ctx { backend, process }
    }

    // Tasks

    /// Starts running `future` as a task of this process. `name` appears in hang reports.
    pub fn spawn<F>(&self, name: &'static str, priority: Priority, future: F) -> Task<F::Output>
    where
        F: Future + 'static,
        F::Output: 'static,
    {
        let id = self.backend.register_task(self.process, name, priority);
        let meta = TaskMeta {
            id,
            process: self.process,
            name,
            priority,
        };
        let end = TaskEnd {
            id,
            backend: self.backend.clone(),
        };
        // Tuple fields drop in order, so even a task that never ran drops its future before
        // `end` tells the backend it ended.
        let state = (future, end);
        let schedule = self.backend.scheduler();
        let (runnable, task) = async_task::Builder::new().metadata(meta).spawn_local(
            move |_| async move {
                let (future, _end) = state;
                future.await
            },
            move |runnable| schedule(runnable),
        );
        self.backend.start_task(runnable);
        Task::new(meta, task, self.backend.clone())
    }

    /// Lets other ready tasks run before this one continues.
    pub async fn yield_now(&self) {
        YieldNow { yielded: false }.await
    }

    // Time

    /// The process's monotonic clock: virtual time in simulation.
    pub fn now(&self) -> Instant {
        self.backend.now(self.process)
    }

    /// The machine's wall clock. In simulation each machine's clock may be skewed or jump.
    pub fn wall_clock(&self) -> SystemTime {
        self.backend.wall_clock(self.process)
    }

    /// A future that completes after `d` on the process's clock. A `d` too long to represent,
    /// such as `Duration::MAX`, never completes.
    pub fn sleep(&self, d: Duration) -> Sleep {
        let deadline = self.now().checked_add(d).unwrap_or(Instant::MAX);
        Sleep::new(deadline, self.backend.sleep_until(self.process, deadline))
    }

    /// Runs `future` until it completes or `d` passes, whichever comes first. The deadline is
    /// fixed when `timeout` is called. If it passes first, `future` is dropped with the returned
    /// future.
    pub fn timeout<F: Future>(
        &self,
        d: Duration,
        future: F,
    ) -> impl Future<Output = Result<F::Output, Elapsed>> + use<F> {
        let mut sleep = self.sleep(d);
        async move {
            let mut future = pin!(future);
            poll_fn(move |cx| {
                if let Poll::Ready(output) = future.as_mut().poll(cx) {
                    return Poll::Ready(Ok(output));
                }
                Pin::new(&mut sleep).poll(cx).map(|()| Err(Elapsed))
            })
            .await
        }
    }

    // Randomness

    /// The world's random number generator, shared by every process. In simulation it is one of
    /// several streams drawn from the run's seed, so drawing from it never changes the network,
    /// faults, or scheduling.
    pub fn rng(&self) -> &Rng {
        self.backend.rng()
    }

    // Network

    /// Listens for connections on `port` of this process's machine.
    pub async fn listen(&self, port: u16) -> io::Result<Listener> {
        let io = self.backend.listen(self.process, port).await?;
        Ok(Listener::new(io))
    }

    /// Opens a connection to `addr`.
    pub async fn connect(&self, addr: SocketAddr) -> io::Result<Stream> {
        let io = self.backend.connect(self.process, addr).await?;
        Ok(Stream::new(io))
    }

    // Disks

    /// The disks of this process's machine right now.
    pub fn disks(&self) -> Rc<[DiskId]> {
        self.backend.disks(self.process)
    }

    /// One of this process's disks. Panics if `id` isn't in [`disks`](Ctx::disks).
    pub fn disk(&self, id: DiskId) -> Disk {
        Disk::new(id, self.backend.disk(self.process, id))
    }

    // CPU-heavy work

    /// Runs `f`, which may be expensive: inline in simulation, charging `cost` in virtual time,
    /// and on a thread pool in production.
    ///
    /// `f` runs with no ambient world in either backend: `buggify!()` is `false`, `trace!` and
    /// `probe!` do nothing, and `knobs()` returns the defaults. Read any knobs it needs before
    /// calling `compute`.
    pub async fn compute<T: Send + 'static>(
        &self,
        cost: crate::Cost,
        f: impl FnOnce() -> T + Send + 'static,
    ) -> T {
        let job = Box::new(move || Box::new(f()) as ComputeOutput);
        let output = self.backend.compute(self.process, cost, job).await;
        *output
            .downcast::<T>()
            .expect("compute returned another job's output")
    }
}

impl fmt::Debug for Ctx {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Ctx")
            .field("process", &self.process)
            .finish_non_exhaustive()
    }
}

/// An estimate of how much CPU time a [`Ctx::compute`] job takes. The simulator advances virtual
/// time by it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Cost(Duration);

impl Cost {
    pub const ZERO: Cost = Cost(Duration::ZERO);

    pub const fn from_duration(d: Duration) -> Cost {
        Cost(d)
    }

    pub const fn duration(self) -> Duration {
        self.0
    }
}

struct YieldNow {
    yielded: bool,
}

impl Future for YieldNow {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.yielded {
            return Poll::Ready(());
        }
        self.yielded = true;
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}
