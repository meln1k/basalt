//! The panic hook for simulation runs: after the usual panic message, it prints the run's seed and
//! the commit hash, then aborts.
//!
//! The runner installs it with each run's seed before the run starts, so a panic anywhere in the
//! run names the seed and [`COMMIT`] that reproduce it. `basalt server` never installs it; there
//! a panic aborts through `panic = "abort"` and the supervisor restarts the process.
//!
//! The hook aborts itself rather than relying on the release profile's `panic = "abort"`, so a
//! panicking run stops at the panic in every build, including debug builds and tests.
//!
//! The seed stays set between runs, so a panic after one run ends and before the next installs
//! names the last run's seed. A later [`std::panic::set_hook`] replaces this hook for good:
//! `install` then only updates the seed.

use std::io::Write;
use std::panic;
use std::process;
use std::sync::Once;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::COMMIT;

static SEED: AtomicU64 = AtomicU64::new(0);
static HOOK: Once = Once::new();

/// Installs the hook for a run with `seed`. Calling it again for the next run replaces the seed.
pub fn install(seed: u64) {
    SEED.store(seed, Ordering::Relaxed);
    HOOK.call_once(|| {
        let default = panic::take_hook();
        panic::set_hook(Box::new(move |info| {
            default(info);
            let seed = SEED.load(Ordering::Relaxed);
            // Ignore a failed write: the abort below must happen either way.
            let _ = writeln!(
                std::io::stderr(),
                "basalt sim: panic with seed {seed} at commit {COMMIT}"
            );
            process::abort();
        }));
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_SEED: u64 = 4711;

    /// Set only in the child process, so `cargo test -- --include-ignored` doesn't abort.
    const CHILD: &str = "BASALT_PANIC_HOOK_CHILD";

    // The hook aborts the process, so a child process runs the panicking test below.
    #[test]
    #[allow(
        clippy::disallowed_types,
        reason = "the test runs itself as a child process"
    )]
    fn panic_prints_seed_and_commit_then_aborts() {
        use std::os::unix::process::ExitStatusExt;

        let out = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--ignored", "--exact", "--nocapture"])
            .arg("panic_hook::tests::panic_under_hook")
            .env(CHILD, "1")
            .output()
            .unwrap();
        let stderr = String::from_utf8_lossy(&out.stderr);

        const SIGABRT: i32 = 6;
        assert_eq!(out.status.signal(), Some(SIGABRT), "stderr:\n{stderr}");
        assert!(stderr.contains("planted panic"), "stderr:\n{stderr}");
        let expected = format!("basalt sim: panic with seed {TEST_SEED} at commit {COMMIT}");
        assert!(stderr.contains(&expected), "stderr:\n{stderr}");
    }

    #[test]
    #[ignore = "aborts the process; panic_prints_seed_and_commit_then_aborts runs it in a child"]
    #[allow(
        clippy::disallowed_methods,
        reason = "tells the child process from the parent"
    )]
    fn panic_under_hook() {
        if std::env::var_os(CHILD).is_none() {
            return;
        }
        install(1);
        install(TEST_SEED);
        panic!("planted panic");
    }
}
