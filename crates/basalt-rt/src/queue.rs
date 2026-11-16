//! The executor's run queue: one FIFO per priority.
//!
//! Tasks run only on the runtime's thread, but they may be woken from any thread: file and
//! compute jobs finish on pool threads, and async-io's reactor can run on its own thread. So the
//! queue is shared and locked, and every push wakes the runtime's `block_on`.

use std::collections::VecDeque;
use std::mem;
use std::sync::Mutex;
use std::task::Waker;
use std::thread::{self, ThreadId};

use basalt_env::Priority;
use basalt_env::backend::Runnable;

pub(crate) struct Queue {
    state: Mutex<State>,
    /// The runtime's thread, the only one that may run or drop a runnable.
    thread: ThreadId,
}

struct State {
    ready: [VecDeque<Runnable>; Priority::ALL.len()],
    /// Wakes the runtime's `block_on` while it runs.
    waker: Option<Waker>,
    closed: bool,
}

impl Queue {
    pub(crate) fn new() -> Queue {
        Queue {
            state: Mutex::new(State {
                ready: Default::default(),
                waker: None,
                closed: false,
            }),
            thread: thread::current().id(),
        }
    }

    /// Queues `runnable` behind others of its priority. Called from any thread.
    pub(crate) fn push(&self, runnable: Runnable) {
        let mut state = self.state.lock().unwrap();
        if state.closed {
            drop(state);
            self.discard(runnable);
            return;
        }
        state.ready[runnable.metadata().priority as usize].push_back(runnable);
        if let Some(waker) = &state.waker {
            waker.wake_by_ref();
        }
    }

    /// The next runnable: the oldest of the highest priority that has one.
    pub(crate) fn pop(&self) -> Option<Runnable> {
        let mut state = self.state.lock().unwrap();
        state.ready.iter_mut().find_map(VecDeque::pop_front)
    }

    pub(crate) fn set_waker(&self, waker: Option<&Waker>) {
        let mut state = self.state.lock().unwrap();
        match (&mut state.waker, waker) {
            (Some(old), Some(new)) => old.clone_from(new),
            (slot, new) => *slot = new.cloned(),
        }
    }

    /// Stops accepting runnables and drops the queued ones. Runnables pushed later are dropped
    /// if they arrive on the runtime's thread.
    pub(crate) fn close(&self) {
        let ready = {
            let mut state = self.state.lock().unwrap();
            state.closed = true;
            state.waker = None;
            mem::take(&mut state.ready)
        };
        // Outside the lock: dropping a task runs its `Drop` code, which may wake other tasks.
        drop(ready);
    }

    fn discard(&self, runnable: Runnable) {
        if thread::current().id() == self.thread {
            drop(runnable);
        } else {
            // Pools are joined before the queue closes, so this is a wake from async-io's
            // reactor thread racing shutdown. `async-task` panics if a local task is dropped on
            // another thread, so leak it instead.
            mem::forget(runnable);
        }
    }
}
