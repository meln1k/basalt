//! `basalt sim`'s four commands, run as the shipped binary on the harness self-test, and the
//! planted-bug demo on the register tests.

#![allow(
    clippy::disallowed_types,
    reason = "these tests run the basalt binary as a child process"
)]

use std::process::{Command, Output};

use basalt_harness::selftest::{PLANTED_SEED, UNREACHABLE_PROBE};
use basalt_harness::sweep::SIGABRT;

fn basalt(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_basalt"))
        .args(args)
        .output()
        .unwrap()
}

fn stdout(out: &Output) -> String {
    String::from_utf8_lossy(&out.stdout).into_owned()
}

fn stderr(out: &Output) -> String {
    String::from_utf8_lossy(&out.stderr).into_owned()
}

#[test]
fn list_shows_the_built_in_tests() {
    let out = basalt(&["sim", "list"]);
    assert!(out.status.success());
    let names: Vec<_> = stdout(&out)
        .lines()
        .map(|l| l.split_whitespace().next().unwrap().to_owned())
        .collect();
    assert_eq!(
        names,
        [
            "ping-pong",
            "ping-pong-planted",
            "faults",
            "register",
            "register-skip-writeback",
            "register-ack-before-sync",
        ]
    );
}

#[test]
fn run_prints_the_trace_seed_commit_and_unseed_and_replays_exactly() {
    let run = |seed: &str| basalt(&["sim", "run", "--test", "ping-pong", "--seed", seed]);
    let first = run("4711");
    assert!(first.status.success(), "{}", stderr(&first));
    let text = stdout(&first);
    for line in [
        "result: passed",
        "seed: 4711",
        &format!("commit: {}", basalt_harness::COMMIT),
    ] {
        assert!(text.lines().any(|l| l == line), "no {line:?} in:\n{text}");
    }
    assert!(text.contains(" pong Info Pong n=0"), "{text}");
    assert!(text.contains(" - Info MachineKilled "), "{text}");
    assert!(text.contains("unseed: 0x"), "{text}");

    // Another process, the same seed: the identical trace and unseed.
    assert_eq!(stdout(&run("4711")), text);
    let other = stdout(&run("4712"));
    let unseed = |t: &str| {
        t.lines()
            .find(|l| l.starts_with("unseed: "))
            .unwrap()
            .to_owned()
    };
    assert_ne!(unseed(&other), unseed(&text));
}

#[test]
fn run_of_the_planted_seed_prints_the_trace_up_to_the_panic_then_aborts() {
    use std::os::unix::process::ExitStatusExt;

    let seed = PLANTED_SEED.to_string();
    let out = basalt(&["sim", "run", "--test", "ping-pong-planted", "--seed", &seed]);
    assert_eq!(out.status.signal(), Some(SIGABRT));
    assert!(
        stdout(&out).contains(" pong Info Pong n="),
        "{}",
        stdout(&out)
    );
    let expected = format!(
        "basalt sim: panic with seed {PLANTED_SEED} at commit {}",
        basalt_harness::COMMIT
    );
    assert!(stderr(&out).contains(&expected), "{}", stderr(&out));
}

#[test]
fn sweep_reports_exactly_the_planted_seed_and_runs_the_rest() {
    let out = basalt(&[
        "sim",
        "sweep",
        "--test",
        "ping-pong-planted",
        "--seeds",
        "0..24",
        "-j",
        "3",
    ]);
    assert_eq!(out.status.code(), Some(1), "{}", stderr(&out));
    let text = stdout(&out);
    assert!(text.contains("passed: 23, failed: 1"), "{text}");
    let failed: Vec<_> = text
        .lines()
        .filter(|l| l.starts_with("failed seed "))
        .collect();
    assert_eq!(
        failed,
        [format!(
            "failed seed {PLANTED_SEED}; replay with: basalt sim run --test ping-pong-planted \
             --seed {PLANTED_SEED}"
        )]
    );
    assert!(stderr(&out).contains("planted panic"), "{}", stderr(&out));
}

#[test]
fn sweep_of_a_passing_range_succeeds() {
    let out = basalt(&[
        "sim",
        "sweep",
        "--test",
        "ping-pong",
        "--seeds",
        "0..12",
        "-j",
        "2",
    ]);
    assert!(out.status.success(), "{}", stderr(&out));
    let text = stdout(&out);
    assert!(text.contains("passed: 12, failed: 0"), "{text}");
    // The coverage report names the self-test's unreachable probe and buggify! site, and not
    // the probe every sweep hits.
    assert!(text.contains("coverage over 12 seeds: "), "{text}");
    assert!(
        text.contains(&format!("probe never hit: {UNREACHABLE_PROBE} at ")),
        "{text}"
    );
    assert!(!text.contains("selftest.ping_timed_out"), "{text}");
    let unreached: Vec<_> = text
        .lines()
        .filter(|l| {
            l.starts_with("buggify! site never fired: crates/basalt-harness/src/selftest.rs:")
        })
        .collect();
    assert!(
        unreached.iter().any(|l| l.ends_with("(never reached)")),
        "{text}"
    );
}

/// Every fault type, and every variant of one, strikes in some seed of the fault test, and every
/// seed passes, so its quiet phase always left every machine up, every link healed, and every
/// disk writable.
#[test]
fn sweep_of_the_fault_test_hits_every_fault_and_passes() {
    let sweep = |test, seeds| {
        let out = basalt(&["sim", "sweep", "--test", test, "--seeds", seeds, "-j", "4"]);
        assert!(out.status.success(), "{}", stderr(&out));
        stdout(&out)
    };
    let missed = |text: &str| -> Vec<String> {
        text.lines()
            .filter(|l| l.starts_with("probe never hit: fault."))
            .map(str::to_owned)
            .collect()
    };
    // Ping-pong runs no fault workload, so its report names every fault probe.
    let all = missed(&sweep("ping-pong", "0..1"));
    assert!(all.len() >= 7, "{all:?}");

    let text = sweep("faults", "0..40");
    assert!(text.contains("passed: 40, failed: 0"), "{text}");
    assert_eq!(missed(&text), Vec::<String>::new(), "{text}");
}

#[test]
fn check_determinism_compares_unseeds() {
    let out = basalt(&[
        "sim",
        "check-determinism",
        "--test",
        "ping-pong",
        "--seeds",
        "0..10",
    ]);
    assert!(out.status.success(), "{}", stderr(&out));
    assert!(
        stdout(&out).contains("deterministic: 10, nondeterministic: 0, unfinished both times: 0"),
        "{}",
        stdout(&out)
    );
}

/// Each run is in its own child, so a panicking seed fails only itself and the check still
/// covers the rest of the range.
#[test]
fn check_determinism_survives_a_panicking_seed() {
    let out = basalt(&[
        "sim",
        "check-determinism",
        "--test",
        "ping-pong-planted",
        "--seeds",
        "5..10",
        "-j",
        "2",
    ]);
    assert!(out.status.success(), "{}", stderr(&out));
    assert!(
        stdout(&out).contains("deterministic: 4, nondeterministic: 0, unfinished both times: 1"),
        "{}",
        stdout(&out)
    );
}

#[test]
fn zero_jobs_is_rejected() {
    let out = basalt(&[
        "sim",
        "sweep",
        "--test",
        "ping-pong",
        "--seeds",
        "0..2",
        "-j",
        "0",
    ]);
    assert_eq!(out.status.code(), Some(2), "{}", stderr(&out));
}

/// The demo for one planted bug: a sweep with the bug on fails `seed`, `run --seed` reports the
/// checker's violation, a line with every phrase of `violation`, and replays the identical trace,
/// and the same seed passes with the bug off.
fn planted_bug_demo(test: &str, seed: u64, violation: &[&str]) {
    let range = format!("{seed}..{}", seed + 1);
    let seed = seed.to_string();
    let out = basalt(&["sim", "sweep", "--test", test, "--seeds", &range, "-j", "1"]);
    assert_eq!(out.status.code(), Some(1), "{}", stderr(&out));
    assert!(
        stdout(&out).contains(&format!("failed seed {seed}; replay with: ")),
        "{}",
        stdout(&out)
    );

    let run = |test| basalt(&["sim", "run", "--test", test, "--seed", &seed]);
    let first = run(test);
    assert_eq!(first.status.code(), Some(1), "{}", stderr(&first));
    let text = stdout(&first);
    assert!(
        text.contains("result: FAILED: register-checks failed in the Check phase"),
        "{text}"
    );
    assert!(
        text.lines()
            .any(|l| violation.iter().all(|phrase| l.contains(phrase))),
        "{text}"
    );
    assert_eq!(stdout(&run(test)), text);

    let off = run("register");
    assert!(off.status.success(), "{}", stdout(&off));
    assert!(stdout(&off).lines().any(|l| l == "result: passed"));
}

// The seeds below are ones a release-build sweep found. A change to the register, the
// simulator, or the fault workloads can move the failures to other seeds; if so, find new ones
// with `basalt sim sweep --test <test> --seeds 0..1000` and update them. A sweep doesn't say
// which checker failed a seed, and most ack-before-sync seeds fail linearizability first, so
// check each candidate with `basalt sim run` for the violation the test expects.

/// A read that skips its write-back lets a later read return an older value.
#[test]
fn skip_writeback_is_caught_by_the_linearizability_checker() {
    planted_bug_demo(
        "register-skip-writeback",
        76,
        &[
            "linearizability violation on key",
            "no operation can follow these",
        ],
    );
}

/// A power cut on a majority loses a write the replicas acknowledged before syncing it.
#[test]
fn ack_before_sync_is_caught_by_the_durability_checker() {
    planted_bug_demo(
        "register-ack-before-sync",
        117,
        &["durability violation on key", "losing acknowledged writes"],
    );
}

#[test]
fn sweep_of_the_register_with_both_bugs_off_passes() {
    let out = basalt(&[
        "sim", "sweep", "--test", "register", "--seeds", "0..4", "-j", "2",
    ]);
    assert!(out.status.success(), "{}", stdout(&out));
    assert!(
        stdout(&out).contains("passed: 4, failed: 0"),
        "{}",
        stdout(&out)
    );
}
