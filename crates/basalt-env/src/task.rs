use std::any::Any;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

use crate::backend::Backend;

/// Which ready task runs first. Backends always run a ready task of a higher priority before one
/// of a lower priority; among equal priorities the order is unspecified (the simulator picks
/// FIFO or a seeded shuffle per seed).
///
/// Variants are listed from highest to lowest priority, so a smaller value runs first.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Priority {
    /// Leader election, leases, and failure detection, which must not starve behind data.
    Coordination,
    /// Moving data between replicas.
    Replication,
    Default,
    /// Work that can wait, such as compaction and scrubbing.
    Background,
}

impl Priority {
    /// Every priority, highest first.
    pub const ALL: [Priority; 4] = [
        Priority::Coordination,
        Priority::Replication,
        Priority::Default,
        Priority::Background,
    ];
}

/// Identifies one incarnation of a process within a world. Backends assign them and never reuse
/// one: a rebooted process gets a fresh id, so anything still holding the old id stays dead.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProcessId(u32);

impl ProcessId {
    pub const fn new(raw: u32) -> ProcessId {
        ProcessId(raw)
    }

    pub const fn get(self) -> u32 {
        self.0
    }
}

/// Identifies a task within a world. Backends assign them, deterministically in simulation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TaskId(u64);

impl TaskId {
    pub const fn new(raw: u64) -> TaskId {
        TaskId(raw)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

/// What a backend knows about a task, carried by each of its [`Runnable`](crate::backend::Runnable)s.
#[derive(Clone, Copy, Debug)]
pub struct TaskMeta {
    pub id: TaskId,
    pub process: ProcessId,
    pub name: &'static str,
    pub priority: Priority,
}

/// A spawned task, from [`Ctx::spawn`](crate::Ctx::spawn). Awaiting it returns the task's output.
///
/// Dropping a `Task` cancels it: its future is never polled again, and the backend drops it the
/// next time it gets to the task. To let a task run on without keeping the handle, call
/// [`detach`](Task::detach), which hands it to its process, so killing the process still drops
/// it.
///
/// If the task's process is killed, the task is dropped with it, and awaiting it from another
/// process never completes, the same as waiting for a reply from a dead node.
#[must_use = "dropping a Task cancels it; call .detach() to let it run"]
pub struct Task<T> {
    id: TaskId,
    process: ProcessId,
    inner: async_task::FallibleTask<T, TaskMeta>,
    backend: Rc<dyn Backend>,
}

impl<T: 'static> Task<T> {
    pub(crate) fn new(
        meta: TaskMeta,
        inner: async_task::Task<T, TaskMeta>,
        backend: Rc<dyn Backend>,
    ) -> Task<T> {
        Task {
            id: meta.id,
            process: meta.process,
            inner: inner.fallible(),
            backend,
        }
    }

    /// Lets the task run to completion without a handle. Its process owns it from now on, so
    /// killing the process still drops it.
    pub fn detach(self) {
        let Task {
            id,
            process,
            inner,
            backend,
        } = self;
        // A finished task has already told the backend it ended; its output is dropped here.
        if !inner.is_finished() {
            backend.detach(DetachedTask {
                id,
                process,
                _handle: Box::new(inner),
            });
        }
    }

    /// The task's id, as its backend reports it in traces and hang reports.
    pub fn id(&self) -> TaskId {
        self.id
    }

    /// Whether the task's future has completed.
    pub fn is_finished(&self) -> bool {
        self.inner.is_finished()
    }
}

impl<T> Future for Task<T> {
    type Output = T;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        match Pin::new(&mut self.inner).poll(cx) {
            Poll::Ready(Some(output)) => Poll::Ready(output),
            // The backend dropped the task's runnable because its process died. It will never
            // complete, so nothing will wake us again either.
            Poll::Ready(None) | Poll::Pending => Poll::Pending,
        }
    }
}

impl<T> fmt::Debug for Task<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Task")
            .field("id", &self.id)
            .field("process", &self.process)
            .finish_non_exhaustive()
    }
}

/// A task handed to its process by [`Task::detach`]. Dropping it cancels the task.
pub struct DetachedTask {
    id: TaskId,
    process: ProcessId,
    _handle: Box<dyn Any>,
}

impl DetachedTask {
    pub fn id(&self) -> TaskId {
        self.id
    }

    pub fn process(&self) -> ProcessId {
        self.process
    }
}

impl fmt::Debug for DetachedTask {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DetachedTask")
            .field("id", &self.id)
            .field("process", &self.process)
            .finish_non_exhaustive()
    }
}

/// Tells the backend a task's future is gone, whether it completed, was cancelled, or was
/// dropped with its process. [`Ctx::spawn`](crate::Ctx::spawn) stores it beside the future, and
/// it drops right after the future does, even if the future was never polled.
pub(crate) struct TaskEnd {
    pub(crate) id: TaskId,
    pub(crate) backend: Rc<dyn Backend>,
}

impl Drop for TaskEnd {
    fn drop(&mut self) {
        self.backend.task_ended(self.id);
    }
}
