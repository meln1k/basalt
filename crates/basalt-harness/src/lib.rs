//! Workloads, test specs, the seed runner and sweep, checkers, and swarm configs.
//!
//! A test is a [`TestSpec`]: a function that builds the simulated system and returns its
//! [`Workload`]s. [`runner::run`] takes one test and one seed through setup, the chaos phase,
//! the quiet phase, and the check. [`cli`] is `basalt sim`, which lists, runs, sweeps, and
//! checks the determinism of the tests built into the binary.

pub mod checker;
pub mod cli;
mod history;
pub mod panic_hook;
pub mod runner;
pub mod selftest;
pub mod sweep;
mod workload;

pub use history::{Call, Entry, Event, History, Outcome};
pub use workload::{TestSpec, Workload, WorkloadResult, Workloads};

/// The commit this build came from, with `-dirty` if its sources differed from it, or `unknown`
/// outside a git checkout. A failing seed reproduces only on this commit.
pub const COMMIT: &str = env!("BASALT_COMMIT");

/// The version and commit, as `basalt --version` prints them.
pub const VERSION: &str = concat!(env!("CARGO_PKG_VERSION"), " (", env!("BASALT_COMMIT"), ")");
