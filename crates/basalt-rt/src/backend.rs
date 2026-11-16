//! `basalt-rt`'s implementation of `Backend`, for its one process.
//!
//! Production runs one process per runtime and never kills it, so the backend keeps no wakers.
//! Its only death is shutdown, when [`Runtime`](crate::Runtime) is dropped: queued and detached
//! tasks are dropped, and tasks spawned or detached afterwards are dropped at once. No task runs
//! after shutdown, so the futures other methods return are never polled again.

use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::io;
use std::mem;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant as StdInstant, SystemTime};

use async_io::Timer;
use basalt_env::backend::{
    Backend, ComputeJob, ComputeOutput, DetachedTask, DiskIo, ListenerIo, LocalBoxFuture, Runnable,
    Scheduler, StreamIo,
};
use basalt_env::{Cost, DiskId, Instant, Priority, ProcessId, Rng, SocketAddr, TaskId};

use crate::disk::Disk;
use crate::net;
use crate::pool::Pool;
use crate::queue::Queue;

pub(crate) struct RtBackend {
    pub(crate) queue: Arc<Queue>,
    /// The start of the monotonic clock.
    start: StdInstant,
    rng: Rng,
    next_task: Cell<u64>,
    detached: RefCell<BTreeMap<TaskId, DetachedTask>>,
    shut_down: Cell<bool>,
    disk_ids: Rc<[DiskId]>,
    disks: Vec<Rc<Disk>>,
    io_pool: Pool,
    compute_pool: Pool,
}

impl RtBackend {
    pub(crate) fn new(disks: Vec<Disk>, io_pool: Pool, compute_pool: Pool, rng: Rng) -> RtBackend {
        RtBackend {
            queue: Arc::new(Queue::new()),
            start: StdInstant::now(),
            rng,
            next_task: Cell::new(0),
            detached: RefCell::default(),
            shut_down: Cell::new(false),
            disk_ids: (0..disks.len() as u32).map(DiskId::new).collect(),
            disks: disks.into_iter().map(Rc::new).collect(),
            io_pool,
            compute_pool,
        }
    }

    /// Stops the pools, then drops every queued and detached task.
    pub(crate) fn shutdown(&self) {
        self.shut_down.set(true);
        // Pools first: a job still running wakes its task when it ends. Once the pools are
        // joined, every such wake is in the queue, which drops those tasks on this thread.
        self.io_pool.close();
        self.compute_pool.close();
        self.queue.close();
        // Out of the map first: dropping a task runs its `Drop` code, which may call back in.
        let detached = mem::take(&mut *self.detached.borrow_mut());
        drop(detached);
    }
}

impl Backend for RtBackend {
    fn register_task(&self, _: ProcessId, _: &'static str, _: Priority) -> TaskId {
        let id = TaskId::new(self.next_task.get());
        self.next_task.set(id.get() + 1);
        id
    }

    fn start_task(&self, first: Runnable) {
        // A closed queue drops it.
        self.queue.push(first);
    }

    fn scheduler(&self) -> Scheduler {
        let queue = self.queue.clone();
        Arc::new(move |runnable| queue.push(runnable))
    }

    fn task_ended(&self, task: TaskId) {
        let handle = self.detached.borrow_mut().remove(&task);
        drop(handle);
    }

    fn detach(&self, task: DetachedTask) {
        if self.shut_down.get() {
            drop(task);
            return;
        }
        self.detached.borrow_mut().insert(task.id(), task);
    }

    fn now(&self, _: ProcessId) -> Instant {
        let nanos = self.start.elapsed().as_nanos();
        Instant::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX))
    }

    fn wall_clock(&self, _: ProcessId) -> SystemTime {
        SystemTime::now()
    }

    fn sleep_until(&self, _: ProcessId, deadline: Instant) -> LocalBoxFuture<'static, ()> {
        let at = match deadline {
            Instant::MAX => None,
            deadline => self
                .start
                .checked_add(Duration::from_nanos(deadline.as_nanos())),
        };
        let timer = at.map_or_else(Timer::never, Timer::at);
        Box::pin(async move {
            timer.await;
        })
    }

    fn rng(&self) -> &Rng {
        &self.rng
    }

    fn listen(
        &self,
        _: ProcessId,
        port: u16,
    ) -> LocalBoxFuture<'_, io::Result<Box<dyn ListenerIo>>> {
        Box::pin(std::future::ready(net::listen(port)))
    }

    fn connect(
        &self,
        _: ProcessId,
        addr: SocketAddr,
    ) -> LocalBoxFuture<'_, io::Result<Box<dyn StreamIo>>> {
        Box::pin(net::connect(addr))
    }

    fn disks(&self, _: ProcessId) -> Rc<[DiskId]> {
        self.disk_ids.clone()
    }

    fn disk(&self, _: ProcessId, disk: DiskId) -> Rc<dyn DiskIo> {
        match self.disks.get(disk.get() as usize) {
            Some(io) => io.clone(),
            None => panic!("{disk:?} isn't one of this process's disks"),
        }
    }

    fn compute(&self, _: ProcessId, _: Cost, job: ComputeJob) -> LocalBoxFuture<'_, ComputeOutput> {
        Box::pin(self.compute_pool.run(job))
    }
}
