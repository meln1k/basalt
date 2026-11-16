//! Thread pools for blocking file I/O and for `compute` jobs.
//!
//! A job runs on a pool thread, which has no ambient world, and hands its result back through a
//! [`Pending`] future. The pool thread wakes the waiting task, so runnables reach the executor
//! from other threads; the executor's queue accepts them.

use std::collections::VecDeque;
use std::future::Future;
use std::io;
use std::mem;
use std::panic::{self, AssertUnwindSafe};
use std::pin::Pin;
use std::sync::{Arc, Condvar, Mutex};
use std::task::{Context, Poll, Waker};
use std::thread::{self, JoinHandle};

type Job = Box<dyn FnOnce() + Send>;

/// A fixed set of threads that run jobs in the order they were submitted.
#[derive(Clone)]
pub(crate) struct Pool {
    shared: Arc<PoolShared>,
}

struct PoolShared {
    state: Mutex<PoolState>,
    ready: Condvar,
    threads: Mutex<Vec<JoinHandle<()>>>,
}

struct PoolState {
    jobs: VecDeque<Job>,
    closed: bool,
}

impl Pool {
    /// Starts `threads` threads, which must be at least 1.
    pub(crate) fn new(name: &str, threads: usize) -> io::Result<Pool> {
        if threads == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("the {name} pool needs at least one thread"),
            ));
        }
        let pool = Pool {
            shared: Arc::new(PoolShared {
                state: Mutex::new(PoolState {
                    jobs: VecDeque::new(),
                    closed: false,
                }),
                ready: Condvar::new(),
                threads: Mutex::default(),
            }),
        };
        for i in 0..threads {
            let shared = pool.shared.clone();
            match thread::Builder::new()
                .name(format!("{name}-{i}"))
                .spawn(move || shared.work())
            {
                Ok(handle) => pool.shared.threads.lock().unwrap().push(handle),
                Err(e) => {
                    pool.close();
                    return Err(e);
                }
            }
        }
        Ok(pool)
    }

    /// Runs `f` on a pool thread. A panic in `f` resumes in the task awaiting the result.
    pub(crate) fn run<T: Send + 'static>(
        &self,
        f: impl FnOnce() -> T + Send + 'static,
    ) -> Pending<T> {
        let slot = Arc::new(Mutex::new(Slot {
            result: None,
            waker: None,
        }));
        let job_slot = slot.clone();
        let job: Job = Box::new(move || {
            let result = panic::catch_unwind(AssertUnwindSafe(f));
            let waker = {
                let mut slot = job_slot.lock().unwrap();
                slot.result = Some(result);
                slot.waker.take()
            };
            if let Some(waker) = waker {
                waker.wake();
            }
        });
        // A closed pool drops the job, so the result never arrives; only a runtime that has
        // shut down closes its pools.
        let mut state = self.shared.state.lock().unwrap();
        if state.closed {
            drop(state);
            drop(job);
        } else {
            state.jobs.push_back(job);
            self.shared.ready.notify_one();
        }
        Pending { slot }
    }

    /// Drops the queued jobs, then waits for the threads to finish their current jobs and exit.
    /// Every wake from a pool thread has happened by the time it returns.
    pub(crate) fn close(&self) {
        let jobs = {
            let mut state = self.shared.state.lock().unwrap();
            state.closed = true;
            mem::take(&mut state.jobs)
        };
        self.shared.ready.notify_all();
        drop(jobs);
        let threads = mem::take(&mut *self.shared.threads.lock().unwrap());
        for thread in threads {
            // Jobs catch their own panics, so a pool thread never panics.
            let _ = thread.join();
        }
    }
}

impl PoolShared {
    fn work(&self) {
        loop {
            let job = {
                let mut state = self.state.lock().unwrap();
                loop {
                    if state.closed {
                        return;
                    }
                    if let Some(job) = state.jobs.pop_front() {
                        break job;
                    }
                    state = self.ready.wait(state).unwrap();
                }
            };
            job();
        }
    }
}

struct Slot<T> {
    result: Option<thread::Result<T>>,
    waker: Option<Waker>,
}

/// The result of a job on a [`Pool`]. Dropping it doesn't stop the job; its result is dropped.
pub(crate) struct Pending<T> {
    slot: Arc<Mutex<Slot<T>>>,
}

impl<T> Future for Pending<T> {
    type Output = T;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        let mut slot = self.slot.lock().unwrap();
        match slot.result.take() {
            Some(Ok(value)) => Poll::Ready(value),
            Some(Err(payload)) => {
                drop(slot);
                panic::resume_unwind(payload)
            }
            None => {
                match &mut slot.waker {
                    Some(waker) => waker.clone_from(cx.waker()),
                    None => slot.waker = Some(cx.waker().clone()),
                }
                Poll::Pending
            }
        }
    }
}
