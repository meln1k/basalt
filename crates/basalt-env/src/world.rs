//! The ambient world behind [`buggify!`](crate::buggify!), [`probe!`](crate::probe!),
//! [`trace!`](crate::trace!), [`knobs()`], and [`is_simulated()`].
//!
//! Each thread has at most one world. A backend installs it with [`install`] for the backend's
//! whole lifetime, not per poll, so it can't go stale between tasks. With no world installed
//! (unit tests, [`compute`](crate::Ctx::compute) jobs), every call falls back to an inert default:
//!
//! | | No world |
//! | --- | --- |
//! | `buggify!()`, `buggify!(p)` | `false` |
//! | `probe!` | No-op |
//! | `trace!` | No-op; field values aren't evaluated |
//! | `knobs()` | Defaults |
//! | `is_simulated()` | `false` |
//!
//! **The rule:** an ambient call may draw randomness and record things, but must never create,
//! own, or address anything. Spawning, I/O, clocks, and `ctx.rng()` stay on
//! [`Ctx`](crate::Ctx). Adding anything process-scoped here is a design change.

use std::cell::RefCell;
use std::fmt;
use std::marker::PhantomData;
use std::rc::Rc;

use crate::Knobs;

/// What a backend provides to diagnostics on its thread.
pub trait World {
    fn is_simulated(&self) -> bool;
    /// Whether the `buggify!` at `site` fires this time. A simulated world switches each site on
    /// or off for the whole run, then fires a switched-on site with probability `fire`, which is
    /// [`BUGGIFY_FIRE_PROBABILITY`] unless the site passed its own.
    fn buggify(&self, site: &'static BuggifySite, fire: f64) -> bool;
    /// The `probe!` at `site` was reached.
    fn probe(&self, site: &'static ProbeSite);
    /// Records `event`. A simulated world fails the run on an [`Severity::Error`] event, as
    /// FoundationDB does, so every error trace is an assertion the sweep checks.
    fn trace(&self, event: &Event<'_>);
    fn knobs(&self) -> Rc<Knobs>;
}

/// How often a switched-on `buggify!()` site fires when reached, as in FoundationDB.
pub const BUGGIFY_FIRE_PROBABILITY: f64 = 0.25;

/// Where a `buggify!` is in the source. Identify a site by `(file, line, column)`, never by its
/// address: addresses change between runs, so a map keyed by them iterates in a different order
/// each run.
#[derive(Debug)]
pub struct BuggifySite {
    pub file: &'static str,
    pub line: u32,
    pub column: u32,
}

/// A `probe!`: its name and where it is in the source.
#[derive(Debug)]
pub struct ProbeSite {
    pub name: &'static str,
    pub file: &'static str,
    pub line: u32,
}

/// How serious a [`trace!`](crate::trace!) event is.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Severity {
    Debug,
    Info,
    Warn,
    /// Something that must never happen. In simulation it fails the run.
    Error,
}

/// A [`trace!`](crate::trace!) event. Fields keep the order they were written in.
pub struct Event<'a> {
    pub severity: Severity,
    pub name: &'static str,
    pub fields: &'a [(&'static str, &'a dyn fmt::Debug)],
}

impl fmt::Debug for Event<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?} {}", self.severity, self.name)?;
        for (key, value) in self.fields {
            write!(f, " {key}={value:?}")?;
        }
        Ok(())
    }
}

// The one piece of ambient state. Everything else that is per thread lives in a world.
mod state {
    use super::*;

    thread_local! {
        pub(super) static WORLD: RefCell<Option<Rc<dyn World>>> = const { RefCell::new(None) };
        pub(super) static DEFAULT_KNOBS: Rc<Knobs> = Rc::new(Knobs::default());
    }
}

/// Installs `world` on this thread until the returned guard is dropped. Panics if a world is
/// already installed.
pub fn install(world: Rc<dyn World>) -> WorldGuard {
    state::WORLD.with_borrow_mut(|slot| {
        assert!(
            slot.is_none(),
            "a world is already installed on this thread"
        );
        *slot = Some(world);
    });
    WorldGuard {
        _not_send: PhantomData,
    }
}

/// Uninstalls the world when dropped, on the thread that installed it.
#[must_use = "dropping the guard uninstalls the world"]
pub struct WorldGuard {
    _not_send: PhantomData<Rc<()>>,
}

impl Drop for WorldGuard {
    fn drop(&mut self) {
        // Drop the world outside the borrow, in case its `Drop` traces.
        let world = state::WORLD.with_borrow_mut(Option::take);
        drop(world);
    }
}

impl fmt::Debug for WorldGuard {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("WorldGuard")
    }
}

/// For backends: runs `f` with no world installed on this thread, then puts the world back.
/// [`Backend::compute`](crate::backend::Backend::compute) runs inline jobs this way, so they see
/// what a production pool thread sees.
pub fn without<R>(f: impl FnOnce() -> R) -> R {
    let _restore = Restore(state::WORLD.with_borrow_mut(Option::take));
    f()
}

struct Restore(Option<Rc<dyn World>>);

impl Drop for Restore {
    fn drop(&mut self) {
        let world = self.0.take();
        state::WORLD.with_borrow_mut(|slot| {
            assert!(
                slot.is_none(),
                "a world installed inside world::without is still installed"
            );
            *slot = world;
        });
    }
}

fn current() -> Option<Rc<dyn World>> {
    // `try_with`: diagnostics may run in destructors after the thread-local is gone.
    state::WORLD
        .try_with(|slot| slot.borrow().clone())
        .ok()
        .flatten()
}

/// The knobs: per-seed values in simulation, defaults plus config in production, and defaults
/// with no world.
pub fn knobs() -> Rc<Knobs> {
    match current() {
        Some(world) => world.knobs(),
        None => state::DEFAULT_KNOBS
            .try_with(Rc::clone)
            .unwrap_or_else(|_| Rc::new(Knobs::default())),
    }
}

/// Whether this thread runs a simulated world. Use it to guard checks too expensive for
/// production; never to change behavior.
pub fn is_simulated() -> bool {
    current().is_some_and(|world| world.is_simulated())
}

#[doc(hidden)]
pub fn __buggify(site: &'static BuggifySite, fire: f64) -> bool {
    // Checked in every world, so a bad probability fails unit tests too.
    assert!(
        (0.0..=1.0).contains(&fire),
        "buggify! probability {fire} is outside [0, 1]"
    );
    current().is_some_and(|world| world.buggify(site, fire))
}

#[doc(hidden)]
pub fn __probe(site: &'static ProbeSite) {
    if let Some(world) = current() {
        world.probe(site);
    }
}

#[doc(hidden)]
pub fn __with_world(f: impl FnOnce(&dyn World)) {
    if let Some(world) = current() {
        f(&*world);
    }
}

/// A fault-injection point: `true` when the simulator wants this site to misbehave now.
///
/// In simulation, each site is switched on for the whole run with some probability and then
/// fires each time it's reached with probability
/// [`BUGGIFY_FIRE_PROBABILITY`](crate::world::BUGGIFY_FIRE_PROBABILITY), or with `p` for
/// `buggify!(p)` (FoundationDB's `BUGGIFY_WITH_PROB`). Use a smaller `p` for faults that would
/// stall a run if they fired often. It is always `false` in production and with no world, and
/// panics if `p` is outside `[0, 1]`.
///
/// ```
/// # fn send(_: &[u8]) {}
/// # fn reset_connection() {}
/// # let msg = b"x";
/// send(msg);
/// if basalt_env::buggify!() {
///     send(msg); // A duplicate the receiver must tolerate.
/// }
/// if basalt_env::buggify!(0.01) {
///     reset_connection(); // Rarely, or nothing would ever get through.
/// }
/// ```
#[macro_export]
macro_rules! buggify {
    () => {
        $crate::buggify!($crate::world::BUGGIFY_FIRE_PROBABILITY)
    };
    ($fire:expr) => {{
        static SITE: $crate::world::BuggifySite = $crate::world::BuggifySite {
            file: ::core::file!(),
            line: ::core::line!(),
            column: ::core::column!(),
        };
        $crate::world::__buggify(&SITE, $fire)
    }};
}

/// Marks a rare path, so a sweep can report probes that no seed reached.
///
/// ```
/// basalt_env::probe!("raft.vote_after_lost_log");
/// ```
#[macro_export]
macro_rules! probe {
    ($name:literal) => {{
        static SITE: $crate::world::ProbeSite = $crate::world::ProbeSite {
            name: $name,
            file: ::core::file!(),
            line: ::core::line!(),
        };
        $crate::world::__probe(&SITE)
    }};
}

/// Records an event: in simulation with virtual time and the process label, as part of the
/// unseed; in production to the log. In simulation an `Error` event fails the run, so use it
/// only for things that must never happen, and `Warn` for faults the system is built to survive. Field values are formatted with `Debug`, and evaluated only
/// when a world is installed.
///
/// ```
/// # let (ts, len) = (3, 4096);
/// basalt_env::trace!(Info, "ReplicaWrite", ts = ts, bytes = len);
/// ```
#[macro_export]
macro_rules! trace {
    ($severity:ident, $name:expr $(, $key:ident = $value:expr)* $(,)?) => {
        $crate::world::__with_world(|world| {
            world.trace(&$crate::world::Event {
                severity: $crate::world::Severity::$severity,
                name: $name,
                fields: &[$((::core::stringify!($key), &$value as &dyn ::core::fmt::Debug)),*],
            })
        })
    };
}

#[cfg(test)]
mod tests {
    use std::cell::{Cell, RefCell};
    use std::rc::Rc;

    use super::*;
    use crate::Duration;

    fn no_world_defaults() {
        assert!(!crate::buggify!());
        crate::probe!("test.no_world");
        let evaluated = Cell::new(false);
        let field = || {
            evaluated.set(true);
            1
        };
        crate::trace!(Info, "NoWorld", value = field());
        assert!(
            !evaluated.get(),
            "trace! evaluated its fields with no world"
        );
        assert_eq!(*knobs(), Knobs::default());
        assert!(!is_simulated());
    }

    #[test]
    fn with_no_world_everything_is_inert() {
        no_world_defaults();
    }

    #[derive(Default)]
    struct Recorder {
        probes: RefCell<Vec<&'static str>>,
        traces: RefCell<Vec<String>>,
        buggify_calls: RefCell<Vec<(u32, f64)>>,
        knobs: Rc<Knobs>,
    }

    impl World for Recorder {
        fn is_simulated(&self) -> bool {
            true
        }
        fn buggify(&self, site: &'static BuggifySite, fire: f64) -> bool {
            self.buggify_calls.borrow_mut().push((site.line, fire));
            true
        }
        fn probe(&self, site: &'static ProbeSite) {
            self.probes.borrow_mut().push(site.name);
        }
        fn trace(&self, event: &Event<'_>) {
            self.traces.borrow_mut().push(format!("{event:?}"));
        }
        fn knobs(&self) -> Rc<Knobs> {
            self.knobs.clone()
        }
    }

    #[test]
    fn an_installed_world_receives_every_call_until_its_guard_drops() {
        let knobs = Knobs {
            max_buggified_delay: Duration::from_millis(7),
        };
        let world = Rc::new(Recorder {
            knobs: Rc::new(knobs.clone()),
            ..Recorder::default()
        });
        let guard = install(world.clone());

        let first = crate::buggify!();
        let second = crate::buggify!(0.01);
        crate::probe!("test.reached");
        crate::trace!(Warn, "Thing", a = 1, b = "x");
        assert!(first && second);
        assert!(is_simulated());
        assert_eq!(*super::knobs(), knobs);

        let calls = world.buggify_calls.borrow().clone();
        assert_eq!(calls.len(), 2);
        assert_ne!(calls[0].0, calls[1].0, "each buggify! is its own site");
        assert_eq!(calls[0].1, BUGGIFY_FIRE_PROBABILITY);
        assert_eq!(calls[1].1, 0.01, "buggify!(p) passes its own probability");
        assert_eq!(*world.probes.borrow(), ["test.reached"]);
        assert_eq!(*world.traces.borrow(), [r#"Warn Thing a=1 b="x""#]);

        drop(guard);
        no_world_defaults();
    }

    #[test]
    fn without_suspends_the_world_and_puts_it_back() {
        let world = Rc::new(Recorder::default());
        let _guard = install(world.clone());
        let out = without(|| {
            no_world_defaults();
            7
        });
        assert_eq!(out, 7);
        assert!(is_simulated());
        crate::probe!("test.after_without");
        assert_eq!(*world.probes.borrow(), ["test.after_without"]);
    }

    #[test]
    #[should_panic(expected = "outside [0, 1]")]
    fn a_buggify_probability_outside_0_1_panics_even_with_no_world() {
        crate::buggify!(1.5);
    }

    #[test]
    #[should_panic(expected = "already installed")]
    fn one_world_per_thread() {
        let _a = install(Rc::new(Recorder::default()));
        let _b = install(Rc::new(Recorder::default()));
    }

    #[test]
    // Compute pool threads see no world, even while their parent thread has one.
    #[allow(clippy::disallowed_methods)]
    fn other_threads_see_no_world() {
        let _guard = install(Rc::new(Recorder::default()));
        assert!(is_simulated());
        std::thread::spawn(no_world_defaults).join().unwrap();
    }
}
