//! The executor: live tasks, their ready queues, and the order ready tasks run in.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::task::Waker;
use std::thread::{self, ThreadId};

use basalt_env::backend::{DetachedTask, Runnable, Scheduler, TaskMeta};
use basalt_env::{Priority, Rng, TaskId};

/// Runs tasks in priority order and keeps what it needs to drop them, following the task and
/// kill rules in `basalt_env::backend`.
pub(crate) struct Executor {
    thread: ThreadId,
    /// Whether ready tasks of equal priority run in a seeded random order rather than FIFO.
    shuffle: bool,
    ready: Arc<Mutex<Ready>>,
    next_task: Cell<u64>,
    /// Every task from `start_task` until `task_ended`.
    tasks: RefCell<BTreeMap<TaskId, LiveTask>>,
}

struct LiveTask {
    meta: TaskMeta,
    /// Kept so the task can be woken to drop it.
    waker: Waker,
    /// The handle, once the task is detached.
    detached: Option<DetachedTask>,
}

impl Executor {
    pub(crate) fn new(shuffle: bool) -> Executor {
        Executor {
            thread: thread::current().id(),
            shuffle,
            ready: Arc::default(),
            next_task: Cell::new(0),
            tasks: RefCell::default(),
        }
    }

    pub(crate) fn next_id(&self) -> TaskId {
        let id = TaskId::new(self.next_task.get());
        self.next_task.set(id.get() + 1);
        id
    }

    pub(crate) fn start(&self, first: Runnable) {
        let task = LiveTask {
            meta: *first.metadata(),
            waker: first.waker(),
            detached: None,
        };
        let old = self.tasks.borrow_mut().insert(task.meta.id, task);
        assert!(old.is_none(), "{:?} started twice", first.metadata().id);
        self.ready.lock().unwrap().push(first);
    }

    pub(crate) fn scheduler(&self) -> Scheduler {
        let (ready, thread) = (self.ready.clone(), self.thread);
        Arc::new(move |runnable| {
            if thread::current().id() != thread {
                // Dropping the runnable here would drop the task's future on this thread too,
                // which `async-task` turns into a second panic. Leak it instead.
                std::mem::forget(runnable);
                panic!(
                    "a simulated task was woken from another thread, which makes the simulation \
                     nondeterministic"
                );
            }
            ready.lock().unwrap().push(runnable);
        })
    }

    pub(crate) fn ended(&self, task: TaskId) {
        let live = self.tasks.borrow_mut().remove(&task);
        assert!(live.is_some(), "{task:?} ended twice");
        // Outside the borrow: dropping the handle may drop the task's output.
        drop(live);
    }

    pub(crate) fn detach(&self, task: DetachedTask) {
        let unkept = match self.tasks.borrow_mut().get_mut(&task.id()) {
            Some(live) => live.detached.replace(task),
            // Its future is already gone; dropping the handle is all that's left.
            None => Some(task),
        };
        drop(unkept);
    }

    pub(crate) fn has_live_tasks(&self) -> bool {
        !self.tasks.borrow().is_empty()
    }

    /// Every live task, by id.
    pub(crate) fn live_tasks(&self) -> Vec<TaskMeta> {
        self.tasks.borrow().values().map(|t| t.meta).collect()
    }

    /// Every live task that its process owns, because it was detached, by id. Nobody awaits
    /// these, so dropping one looks to the rest of the world like dropping its handle.
    pub(crate) fn detached_tasks(&self) -> Vec<TaskMeta> {
        self.tasks
            .borrow()
            .values()
            .filter(|t| t.detached.is_some())
            .map(|t| t.meta)
            .collect()
    }

    /// Takes the next runnable: highest priority first, then FIFO or a pick drawn from `rng`.
    pub(crate) fn pop_ready(&self, rng: &Rng) -> Option<Runnable> {
        let mut ready = self.ready.lock().unwrap();
        let queue = ready.queues.iter_mut().find(|q| !q.is_empty())?;
        if self.shuffle && queue.len() > 1 {
            queue.swap_remove_back(rng.range(0..queue.len()))
        } else {
            queue.pop_front()
        }
    }

    /// Drops the future of every task `doomed` picks without polling it again, as a kill does
    /// (`basalt_env::backend`): wakes each, then hands their queued runnables to `drop_one`,
    /// which drops them, until none is left. Dropping runs arbitrary `Drop` code, which may spawn
    /// or cancel tasks, so no borrow or lock is held meanwhile.
    pub(crate) fn drop_tasks(
        &self,
        doomed: impl Fn(&TaskMeta) -> bool,
        drop_one: impl Fn(Runnable),
    ) {
        let wakers: Vec<Waker> = self
            .tasks
            .borrow()
            .values()
            .filter(|t| doomed(&t.meta))
            .map(|t| t.waker.clone())
            .collect();
        for waker in wakers {
            waker.wake();
        }
        loop {
            let runnables = self.ready.lock().unwrap().take(&doomed);
            if runnables.is_empty() {
                return;
            }
            runnables.into_iter().for_each(&drop_one);
        }
    }
}

/// Ready runnables, one FIFO queue per priority. It's behind a mutex only because `async-task`
/// requires the scheduler to be `Send + Sync`; only the simulator thread ever locks it.
#[derive(Default)]
struct Ready {
    /// Indexed by `Priority as usize`: its variants are declared highest first.
    queues: [VecDeque<Runnable>; Priority::ALL.len()],
}

impl Ready {
    fn push(&mut self, runnable: Runnable) {
        self.queues[runnable.metadata().priority as usize].push_back(runnable);
    }

    fn take(&mut self, pick: impl Fn(&TaskMeta) -> bool) -> Vec<Runnable> {
        let mut taken = Vec::new();
        for queue in &mut self.queues {
            let (picked, kept): (Vec<_>, Vec<_>) =
                queue.drain(..).partition(|r| pick(r.metadata()));
            queue.extend(kept);
            taken.extend(picked);
        }
        taken
    }
}
