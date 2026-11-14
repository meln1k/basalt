//! Workloads, test specs, the seed runner and sweep, checkers, and swarm configs.

pub mod panic_hook;

/// The commit this build came from, with `-dirty` if its sources differed from it, or `unknown`
/// outside a git checkout. A failing seed reproduces only on this commit.
pub const COMMIT: &str = env!("BASALT_COMMIT");

/// The version and commit, as `basalt --version` prints them.
pub const VERSION: &str = concat!(env!("CARGO_PKG_VERSION"), " (", env!("BASALT_COMMIT"), ")");
