//! The interface a world implements: `basalt-sim` in tests, `basalt-rt` in production.
//!
//! Core code never uses this module; it holds a [`Ctx`](crate::Ctx). A backend creates a `Ctx`
//! for each process with [`Ctx::new`](crate::Ctx::new) and installs its ambient
//! [`World`](crate::world::World) on its thread with [`install`](crate::world::install).
//!
//! # Tasks
//!
//! [`Ctx::spawn`](crate::Ctx::spawn) calls [`Backend::register_task`] for an id, builds the task
//! with `async-task` using [`Backend::scheduler`], and hands its first [`Runnable`] to
//! [`Backend::start_task`]. The backend owns every runnable from then on: it runs ready ones in
//! priority order and drops any whose process has died. When a task's future is dropped for any
//! reason (it completed, it was cancelled, or its process was killed), the backend hears about it
//! through [`Backend::task_ended`], after the future and everything it owned are gone.
//! [`Task::detach`](crate::Task::detach) hands the task's handle to [`Backend::detach`], and the
//! backend keeps it until the task ends.
//!
//! Cancelling a task never polls it again, but `async-task` drops its future only when the
//! backend runs or drops the task's runnable, which it schedules one last time for this.
//!
//! # Kills
//!
//! A backend that kills processes keeps each live task's waker, from `start_task` until
//! `task_ended`. To kill a process, it marks the process dead, wakes every live task of the
//! process, and then drops every queued runnable of the process, repeating until none is left,
//! all before it polls any other task. That drops every future of the process right away,
//! including tasks parked on something that will never wake them and tasks whose
//! [`Task`](crate::Task) handle someone else holds. Awaiting such a handle then never completes,
//! like waiting for a reply from a dead node.
//!
//! # Dead processes
//!
//! A [`ProcessId`] names one incarnation of a process and is never reused: a rebooted process
//! gets a fresh id, so a `Ctx` kept from before a kill stays dead. Any `Backend` method may still
//! be called for a dead process, from `Drop` code or from objects another process holds:
//!
//! - `start_task` drops the runnable, so the task's future is dropped without being polled.
//! - `detach` drops the handle.
//! - `sleep_until` and `compute` return futures that never complete.
//! - `listen`, `connect`, and every disk, file, stream, and listener operation return an error.
//! - `now`, `wall_clock`, `rng`, and `disks` keep working.
//!
//! # Reentrancy
//!
//! Dropping tasks runs arbitrary `Drop` code, which may spawn tasks, end other tasks, or call
//! any other `Backend` method. Backends must not hold a borrow of their own state, or a lock on
//! their run queue, while they drop a task, a runnable, or a [`DetachedTask`].
//!
//! # I/O errors
//!
//! I/O errors are `std::io` errors in both worlds, and core code matches on their
//! [`ErrorKind`](std::io::ErrorKind), so the simulator reports each fault with the kind a real
//! system reports for it:
//!
//! | Fault | Kind |
//! | --- | --- |
//! | Disk full (`ENOSPC`) | `StorageFull` |
//! | Read or write failed (`EIO`) | Built with `Error::from_raw_os_error(5)`, as on Linux |
//! | Connection reset, by a fault or a killed peer | `ConnectionReset` |
//! | Nothing listening at the address | `ConnectionRefused` |
//! | Port already in use | `AddrInUse` |
//! | File not found | `NotFound` |
//! | File exists (`create_new`) | `AlreadyExists` |
//! | A [`Ctx::timeout`](crate::Ctx::timeout) elapsed (via `From<Elapsed>`) | `TimedOut` |

use std::any::Any;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures_io::{AsyncRead, AsyncWrite};

use crate::disk::{DiskId, OpenOptions, Space};
use crate::net::SocketAddr;
use crate::{Cost, Instant, Priority, ProcessId, Rng, SystemTime, TaskId};

pub use crate::task::{DetachedTask, TaskMeta};

/// A boxed future that stays on one thread.
pub type LocalBoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

/// A scheduled run of a task. Running it polls the task's future once.
pub type Runnable = async_task::Runnable<TaskMeta>;

/// Queues a runnable to be run. `async-task` requires it to be `Send + Sync` because wakers are,
/// but backends must only accept runnables on their own thread: a wake from another thread
/// would make the simulator nondeterministic.
pub type Scheduler = Arc<dyn Fn(Runnable) + Send + Sync>;

/// Work for [`Backend::compute`], with its output boxed so the trait stays object-safe.
pub type ComputeJob = Box<dyn FnOnce() -> ComputeOutput + Send>;
pub type ComputeOutput = Box<dyn Any + Send>;

/// Everything a [`Ctx`](crate::Ctx) can do, for one world. See the [module docs](self) for the
/// task, kill, and dead-process rules every backend follows.
///
/// Methods that act for a process take its [`ProcessId`]: the backend uses it to find the
/// process's machine (its clock, disks, and network address) and to tie what it creates, such
/// as connections and tasks, to the process, so killing the process tears them down.
pub trait Backend {
    // Tasks

    /// Assigns an id to a task `process` is about to spawn. Its first run follows through
    /// [`start_task`](Backend::start_task).
    fn register_task(&self, process: ProcessId, name: &'static str, priority: Priority) -> TaskId;

    /// Takes a task's first run and queues it like any other runnable. A backend that kills
    /// processes keeps `first.waker()` until the task ends.
    fn start_task(&self, first: Runnable);

    /// The function every task of this world uses to schedule its later runnables.
    fn scheduler(&self) -> Scheduler;

    /// The task's future, and everything it owned, has been dropped. The task won't run again.
    fn task_ended(&self, task: TaskId);

    /// Keeps a detached task until it ends. Dropping the [`DetachedTask`] cancels the task.
    fn detach(&self, task: DetachedTask);

    // Time

    /// The process's monotonic clock.
    fn now(&self, process: ProcessId) -> Instant;

    /// The process's machine's wall clock.
    fn wall_clock(&self, process: ProcessId) -> SystemTime;

    /// A timer that completes once the process's clock reaches `deadline`, or immediately if it
    /// has already. The simulator may complete it later, never earlier. A deadline of
    /// [`Instant::MAX`] never completes.
    fn sleep_until(&self, process: ProcessId, deadline: Instant) -> LocalBoxFuture<'static, ()>;

    // Randomness

    /// The generator [`Ctx::rng`](crate::Ctx::rng) returns, shared by every process. In
    /// simulation it is the `ctx` stream, `Rng::derive(seed, "ctx")`. The backend's own draws
    /// (scheduling, latencies, faults, BUGGIFY) come from other streams, so core code drawing
    /// more or fewer numbers never shifts them.
    fn rng(&self) -> &Rng;

    // Network

    /// Listens on `port` of the process's machine.
    fn listen(
        &self,
        process: ProcessId,
        port: u16,
    ) -> LocalBoxFuture<'_, io::Result<Box<dyn ListenerIo>>>;

    /// Opens a connection from the process to `addr`.
    fn connect(
        &self,
        process: ProcessId,
        addr: SocketAddr,
    ) -> LocalBoxFuture<'_, io::Result<Box<dyn StreamIo>>>;

    // Disks

    /// The disks of the process's machine right now.
    fn disks(&self, process: ProcessId) -> Rc<[DiskId]>;

    /// One of the process's disks. Panics if `disk` isn't one of them.
    fn disk(&self, process: ProcessId, disk: DiskId) -> Rc<dyn DiskIo>;

    // CPU-heavy work

    /// Runs `job` with no ambient world installed and returns its output: inline under
    /// [`world::without`](crate::world::without) in simulation, charging `cost` in virtual time,
    /// and on a thread pool in production.
    fn compute(
        &self,
        process: ProcessId,
        cost: Cost,
        job: ComputeJob,
    ) -> LocalBoxFuture<'_, ComputeOutput>;
}

/// One side of a connection: an ordered, reliable byte stream, like TCP.
pub trait StreamIo: AsyncRead + AsyncWrite + Unpin {
    fn local_addr(&self) -> SocketAddr;
    fn peer_addr(&self) -> SocketAddr;
}

/// A listening socket.
pub trait ListenerIo {
    /// Polls for the next incoming connection.
    fn poll_accept(&self, cx: &mut Context<'_>) -> Poll<io::Result<Box<dyn StreamIo>>>;
    fn local_addr(&self) -> SocketAddr;
}

/// A disk. Nothing written to it is durable until it is synced: file contents by
/// [`FileIo::sync`], and creating, renaming, and removing files by [`DiskIo::sync_dir`].
pub trait DiskIo {
    fn open<'a>(
        &'a self,
        path: &'a str,
        opts: OpenOptions,
    ) -> LocalBoxFuture<'a, io::Result<Box<dyn FileIo>>>;
    fn rename<'a>(&'a self, from: &'a str, to: &'a str) -> LocalBoxFuture<'a, io::Result<()>>;
    fn remove<'a>(&'a self, path: &'a str) -> LocalBoxFuture<'a, io::Result<()>>;
    fn list<'a>(&'a self, dir: &'a str) -> LocalBoxFuture<'a, io::Result<Vec<String>>>;
    fn sync_dir<'a>(&'a self, dir: &'a str) -> LocalBoxFuture<'a, io::Result<()>>;
    fn space(&self) -> Space;
}

/// An open file. I/O takes and returns owned buffers, so an io_uring backend can hold them while
/// the kernel works.
#[allow(clippy::len_without_is_empty)]
pub trait FileIo {
    /// Reads into `buf[..buf.len()]` from `offset` and returns the number of bytes read, which is
    /// less than `buf.len()` only at the end of the file.
    fn read_at(
        &self,
        buf: Vec<u8>,
        offset: u64,
    ) -> LocalBoxFuture<'_, (io::Result<usize>, Vec<u8>)>;
    /// Writes all of `buf` at `offset`, extending the file if needed.
    fn write_at(&self, buf: Vec<u8>, offset: u64) -> LocalBoxFuture<'_, (io::Result<()>, Vec<u8>)>;
    fn sync(&self) -> LocalBoxFuture<'_, io::Result<()>>;
    fn set_len(&self, len: u64) -> LocalBoxFuture<'_, io::Result<()>>;
    /// The file's length, including writes not yet synced.
    fn len(&self) -> u64;
}
