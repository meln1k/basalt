//! The only way Basalt's core code reaches the outside world.
//!
//! Everything that acts as a process goes through an explicit [`Ctx`]: spawning tasks, clocks,
//! randomness, the network, disks, and CPU-heavy work. Behind `Ctx` sits an [`Rc<dyn Backend>`],
//! implemented by `basalt-sim` in tests and by `basalt-rt` in production, so the same compiled
//! code runs in either world.
//!
//! Diagnostics are ambient instead, because they never depend on which process is acting:
//! [`buggify!`], [`probe!`], [`trace!`], [`knobs()`], and [`is_simulated()`]. They read the world
//! the backend installed on the current thread, and fall back to inert defaults when there is
//! none (unit tests, compute pool threads). See [`world`].
//!
//! See `docs/design/milestone-1-simulator.md` for the design.
//!
//! [`Rc<dyn Backend>`]: backend::Backend

// basalt-env holds the ambient world, the one sanctioned `thread_local!` outside the backends.
// (Clippy honors this allow only at the crate root.)
#![allow(clippy::disallowed_macros)]
// `deny`, not `forbid`: registering each `buggify!` and `probe!` site with `linkme` takes a
// `link_section`, which the macros allow on the site alone.
#![deny(unsafe_code)]

pub mod backend;
mod collections;
mod ctx;
mod disk;
mod knobs;
mod net;
mod rng;
mod task;
mod time;
pub mod world;

pub use collections::{FixedState, FxHasher, HashMap, HashSet};
pub use ctx::{Cost, Ctx};
pub use disk::{Disk, DiskId, File, OpenOptions, Space};
pub use knobs::Knobs;
pub use net::{IpAddr, Listener, SocketAddr, Stream};
pub use rng::{Rng, SampleRange};
pub use task::{Priority, ProcessId, Task, TaskId};
pub use time::{Duration, Elapsed, Instant, Sleep, SystemTime, UNIX_EPOCH};
pub use world::{Severity, is_simulated, knobs};
