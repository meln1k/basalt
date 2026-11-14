//! Virtual time: one clock for the whole world, and the timers waiting on it.

use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use basalt_env::Instant;

/// Virtual time and its pending timers. Time moves only when the run loop calls
/// [`advance_to`](Clock::advance_to), between tasks.
#[derive(Default)]
pub(crate) struct Clock {
    now: Cell<Instant>,
    next_timer: Cell<u64>,
    /// Each pending timer's waker, by deadline and then creation order, which is the order
    /// they fire in.
    timers: RefCell<BTreeMap<TimerKey, Waker>>,
}

type TimerKey = (Instant, u64);

impl Clock {
    pub(crate) fn now(&self) -> Instant {
        self.now.get()
    }

    /// The deadline of the earliest pending timer, if any.
    pub(crate) fn next_deadline(&self) -> Option<Instant> {
        self.timers
            .borrow()
            .first_key_value()
            .map(|(&(at, _), _)| at)
    }

    /// Moves time to `to` and wakes every timer due by then, earliest first.
    pub(crate) fn advance_to(&self, to: Instant) {
        debug_assert!(to >= self.now.get(), "time never moves backward");
        self.now.set(to);
        let mut due = Vec::new();
        {
            let mut timers = self.timers.borrow_mut();
            while let Some(entry) = timers.first_entry() {
                if entry.key().0 > to {
                    break;
                }
                due.push(entry.remove());
            }
        }
        // Outside the borrow: waking queues a runnable, and a waker may be anything.
        for waker in due {
            waker.wake();
        }
    }

    /// A future that completes once the clock reaches `deadline`. One at [`Instant::MAX`]
    /// never completes.
    pub(crate) fn sleep_until(self: &Rc<Self>, deadline: Instant) -> Sleep {
        Sleep {
            clock: self.clone(),
            deadline,
            timer: None,
        }
    }

    /// Removes a timer, if it hasn't fired.
    fn cancel(&self, key: TimerKey) {
        let waker = self.timers.borrow_mut().remove(&key);
        drop(waker);
    }
}

pub(crate) struct Sleep {
    clock: Rc<Clock>,
    deadline: Instant,
    /// The timer's key once registered.
    timer: Option<TimerKey>,
}

impl Future for Sleep {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let Sleep {
            clock,
            deadline,
            timer,
        } = self.get_mut();
        if *deadline == Instant::MAX {
            return Poll::Pending;
        }
        if clock.now() >= *deadline {
            if let Some(key) = timer.take() {
                clock.cancel(key);
            }
            return Poll::Ready(());
        }
        let key = *timer.get_or_insert_with(|| {
            let id = clock.next_timer.get();
            clock.next_timer.set(id + 1);
            (*deadline, id)
        });
        let old = clock.timers.borrow_mut().insert(key, cx.waker().clone());
        // Outside the borrow, since a waker may be anything.
        drop(old);
        Poll::Pending
    }
}

impl Drop for Sleep {
    fn drop(&mut self) {
        if let Some(key) = self.timer {
            self.clock.cancel(key);
        }
    }
}
