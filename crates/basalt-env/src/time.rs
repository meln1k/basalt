use std::fmt;
use std::future::Future;
use std::ops::{Add, AddAssign, Sub, SubAssign};
use std::pin::Pin;
use std::task::{Context, Poll};

pub use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::backend::LocalBoxFuture;

/// A point on a process's monotonic clock, from [`Ctx::now`](crate::Ctx::now).
///
/// In simulation it's virtual time, which moves only between tasks. In production it's the time
/// since the backend started. Instants from different processes are only comparable within one
/// world, and the simulator may run each machine's clock at a slightly different rate.
#[derive(Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Instant {
    nanos: u64,
}

impl Instant {
    /// The start of the clock.
    pub const ZERO: Instant = Instant { nanos: 0 };

    /// The end of time. A [`Sleep`] with this deadline never completes.
    pub const MAX: Instant = Instant { nanos: u64::MAX };

    /// For backends: the instant `nanos` nanoseconds after the start of the clock.
    pub const fn from_nanos(nanos: u64) -> Instant {
        Instant { nanos }
    }

    /// Nanoseconds since the start of the clock.
    pub const fn as_nanos(self) -> u64 {
        self.nanos
    }

    /// Time from `earlier` to `self`, or zero if `earlier` is later.
    pub fn duration_since(self, earlier: Instant) -> Duration {
        Duration::from_nanos(self.nanos.saturating_sub(earlier.nanos))
    }

    /// Time from `earlier` to `self`, or `None` if `earlier` is later.
    pub fn checked_duration_since(self, earlier: Instant) -> Option<Duration> {
        self.nanos
            .checked_sub(earlier.nanos)
            .map(Duration::from_nanos)
    }

    pub fn checked_add(self, d: Duration) -> Option<Instant> {
        let d = u64::try_from(d.as_nanos()).ok()?;
        self.nanos.checked_add(d).map(Instant::from_nanos)
    }

    pub fn checked_sub(self, d: Duration) -> Option<Instant> {
        let d = u64::try_from(d.as_nanos()).ok()?;
        self.nanos.checked_sub(d).map(Instant::from_nanos)
    }
}

impl fmt::Debug for Instant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Instant({:?})", Duration::from_nanos(self.nanos))
    }
}

impl Add<Duration> for Instant {
    type Output = Instant;
    fn add(self, d: Duration) -> Instant {
        self.checked_add(d)
            .expect("overflow when adding duration to instant")
    }
}

impl AddAssign<Duration> for Instant {
    fn add_assign(&mut self, d: Duration) {
        *self = *self + d;
    }
}

impl Sub<Duration> for Instant {
    type Output = Instant;
    fn sub(self, d: Duration) -> Instant {
        self.checked_sub(d)
            .expect("overflow when subtracting duration from instant")
    }
}

impl SubAssign<Duration> for Instant {
    fn sub_assign(&mut self, d: Duration) {
        *self = *self - d;
    }
}

impl Sub<Instant> for Instant {
    type Output = Duration;
    /// Saturates at zero, like `std::time::Instant`.
    fn sub(self, earlier: Instant) -> Duration {
        self.duration_since(earlier)
    }
}

/// A future that completes at a deadline on its process's clock, from
/// [`Ctx::sleep`](crate::Ctx::sleep). Dropping it cancels the timer.
#[must_use = "a Sleep does nothing unless awaited"]
pub struct Sleep {
    deadline: Instant,
    timer: LocalBoxFuture<'static, ()>,
}

impl Sleep {
    pub(crate) fn new(deadline: Instant, timer: LocalBoxFuture<'static, ()>) -> Sleep {
        Sleep { deadline, timer }
    }

    /// When the sleep was asked to end, or [`Instant::MAX`] if it never ends. The simulator may
    /// end it later, never earlier.
    pub fn deadline(&self) -> Instant {
        self.deadline
    }
}

impl Future for Sleep {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        self.timer.as_mut().poll(cx)
    }
}

impl fmt::Debug for Sleep {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sleep")
            .field("deadline", &self.deadline)
            .finish_non_exhaustive()
    }
}

/// The error from a [`Ctx::timeout`](crate::Ctx::timeout) whose deadline passed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Elapsed;

impl fmt::Display for Elapsed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("deadline elapsed")
    }
}

impl std::error::Error for Elapsed {}

impl From<Elapsed> for std::io::Error {
    fn from(e: Elapsed) -> std::io::Error {
        std::io::Error::new(std::io::ErrorKind::TimedOut, e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn instant_arithmetic() {
        let t = Instant::from_nanos(1_000);
        let later = t + Duration::from_nanos(500);
        assert_eq!(later.as_nanos(), 1_500);
        assert_eq!(later - t, Duration::from_nanos(500));
        assert_eq!(t - later, Duration::ZERO);
        assert_eq!(t.checked_duration_since(later), None);
        assert_eq!(later - Duration::from_nanos(1_500), Instant::ZERO);
        assert_eq!(Instant::ZERO.checked_sub(Duration::from_nanos(1)), None);
        assert_eq!(
            Instant::from_nanos(u64::MAX).checked_add(Duration::from_nanos(1)),
            None
        );
    }
}
