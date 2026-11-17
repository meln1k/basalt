//! Virtual time: one clock for the whole world and the timers waiting on it, and each machine's
//! drifting monotonic clock and wall clock on top of it.

use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use basalt_env::{Duration, Instant, SystemTime, UNIX_EPOCH};

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

/// A virtual timer that can be moved: each poll sets it for a time, replacing the timer if the
/// time changed.
#[derive(Default)]
pub(crate) struct Timer(Option<(Instant, Sleep)>);

impl Timer {
    /// Polls a timer for `at`, setting one up if it isn't set for `at` already.
    pub(crate) fn poll_at(
        &mut self,
        clock: &Rc<Clock>,
        at: Instant,
        cx: &mut Context<'_>,
    ) -> Poll<()> {
        if self.0.as_ref().is_none_or(|(set, _)| *set != at) {
            self.0 = Some((at, clock.sleep_until(at)));
        }
        let (_, sleep) = self.0.as_mut().expect("just set");
        Pin::new(sleep).poll(cx)
    }

    pub(crate) fn clear(&mut self) {
        self.0 = None;
    }
}

/// What every machine's wall clock reads at virtual time zero: 2026-01-01T00:00:00Z, in
/// nanoseconds since the Unix epoch.
const WALL_CLOCK_START: i128 = 1_767_225_600 * 1_000_000_000;

const MILLION: i128 = 1_000_000;

/// One machine's clocks, on top of the world's virtual time. Its monotonic clock runs
/// `drift_ppm` parts per million faster than virtual time (slower if negative), and its wall
/// clock reads the monotonic clock plus an offset that jumps can move either way.
pub(crate) struct MachineClock {
    clock: Rc<Clock>,
    /// Virtual time, and the monotonic clock's reading then, when the drift last changed.
    anchor: Cell<(Instant, Instant)>,
    drift_ppm: Cell<i32>,
    /// The wall clock minus the monotonic clock, in nanoseconds since the Unix epoch.
    wall_offset: Cell<i128>,
    /// The waker of each pending sleep on this machine, by id. A drift change wakes them all so
    /// they move their virtual timers.
    sleepers: RefCell<BTreeMap<u64, Waker>>,
    next_sleeper: Cell<u64>,
}

impl MachineClock {
    /// Clocks with no drift, whose wall clock reads 2026-01-01T00:00:00Z at virtual time zero.
    pub(crate) fn new(clock: Rc<Clock>) -> MachineClock {
        MachineClock {
            clock,
            anchor: Cell::new((Instant::ZERO, Instant::ZERO)),
            drift_ppm: Cell::new(0),
            wall_offset: Cell::new(WALL_CLOCK_START),
            sleepers: RefCell::default(),
            next_sleeper: Cell::new(0),
        }
    }

    /// The monotonic clock now.
    pub(crate) fn now(&self) -> Instant {
        self.local(self.clock.now())
    }

    /// The wall clock now.
    pub(crate) fn wall(&self) -> SystemTime {
        system_time(i128::from(self.now().as_nanos()) + self.wall_offset.get())
    }

    /// From now on, runs the monotonic clock `ppm` parts per million faster than virtual time,
    /// keeping it continuous, and moves every pending sleep to the new rate.
    pub(crate) fn set_drift(&self, ppm: i32) {
        assert!(
            ppm.unsigned_abs() < 1_000_000,
            "a clock drift of {ppm} ppm would stop or reverse the clock"
        );
        let now = self.clock.now();
        self.anchor.set((now, self.local(now)));
        self.drift_ppm.set(ppm);
        // Outside the borrow, since a waker may be anything.
        let sleepers = std::mem::take(&mut *self.sleepers.borrow_mut());
        for waker in sleepers.into_values() {
            waker.wake();
        }
    }

    pub(crate) fn drift(&self) -> i32 {
        self.drift_ppm.get()
    }

    /// Sets the wall clock to read `to` now.
    pub(crate) fn set_wall(&self, to: SystemTime) {
        self.wall_offset
            .set(epoch_nanos(to) - i128::from(self.now().as_nanos()));
    }

    /// A future that completes once the monotonic clock reaches `deadline`. One at
    /// [`Instant::MAX`] never completes.
    pub(crate) fn sleep_until(self: &Rc<Self>, deadline: Instant) -> LocalSleep {
        LocalSleep {
            machine: self.clone(),
            deadline,
            timer: Timer::default(),
            sleeper: None,
        }
    }

    fn rate(&self) -> i128 {
        MILLION + i128::from(self.drift_ppm.get())
    }

    /// What the monotonic clock reads at virtual time `at`, which must not be before the last
    /// drift change.
    fn local(&self, at: Instant) -> Instant {
        let (anchor, local) = self.anchor.get();
        let elapsed = i128::from(at.as_nanos() - anchor.as_nanos());
        to_instant(i128::from(local.as_nanos()) + elapsed * self.rate() / MILLION)
    }

    /// The earliest virtual time at which the monotonic clock reads `local` or later, if the
    /// drift doesn't change before then.
    fn virtual_time(&self, local: Instant) -> Instant {
        let (anchor, anchor_local) = self.anchor.get();
        let ahead = i128::from(local.as_nanos().saturating_sub(anchor_local.as_nanos()));
        let rate = self.rate();
        to_instant(i128::from(anchor.as_nanos()) + (ahead * MILLION + rate - 1) / rate)
    }

    fn forget(&self, sleeper: u64) {
        let waker = self.sleepers.borrow_mut().remove(&sleeper);
        drop(waker);
    }
}

fn to_instant(nanos: i128) -> Instant {
    Instant::from_nanos(u64::try_from(nanos.max(0)).unwrap_or(u64::MAX))
}

/// Nanoseconds since the Unix epoch, either side of it.
fn system_time(nanos: i128) -> SystemTime {
    let n = nanos.unsigned_abs();
    let secs = u64::try_from(n / 1_000_000_000).unwrap_or(u64::MAX);
    let d = Duration::new(secs, (n % 1_000_000_000) as u32);
    if nanos >= 0 {
        UNIX_EPOCH + d
    } else {
        UNIX_EPOCH - d
    }
}

fn epoch_nanos(t: SystemTime) -> i128 {
    let nanos = |d: Duration| i128::try_from(d.as_nanos()).expect("in range");
    match t.duration_since(UNIX_EPOCH) {
        Ok(d) => nanos(d),
        Err(before) => -nanos(before.duration()),
    }
}

/// A timer on one machine's monotonic clock, set as a virtual timer for when that clock reaches
/// the deadline. A drift change wakes it to move the virtual timer.
pub(crate) struct LocalSleep {
    machine: Rc<MachineClock>,
    /// On the machine's clock.
    deadline: Instant,
    /// The virtual timer for when the machine's clock reaches the deadline.
    timer: Timer,
    /// Its id among the machine's sleepers, once pending.
    sleeper: Option<u64>,
}

impl Future for LocalSleep {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        if this.deadline == Instant::MAX {
            return Poll::Pending;
        }
        let machine = &this.machine;
        if machine.now() >= this.deadline {
            this.timer.clear();
            if let Some(id) = this.sleeper.take() {
                machine.forget(id);
            }
            return Poll::Ready(());
        }
        // Still ahead of now, since the machine's clock hasn't reached the deadline.
        let at = machine.virtual_time(this.deadline);
        let fired = this.timer.poll_at(&machine.clock, at, cx);
        debug_assert!(
            fired.is_pending(),
            "the timer is set for a time still ahead"
        );
        let id = *this.sleeper.get_or_insert_with(|| {
            let id = machine.next_sleeper.get();
            machine.next_sleeper.set(id + 1);
            id
        });
        let old = machine.sleepers.borrow_mut().insert(id, cx.waker().clone());
        drop(old);
        Poll::Pending
    }
}

impl Drop for LocalSleep {
    fn drop(&mut self) {
        if let Some(id) = self.sleeper {
            self.machine.forget(id);
        }
    }
}
