//! `basalt sim`: list, run, sweep, and check the determinism of the tests built into the binary.
//!
//! The binary mounts [`SimCommand`] as its `sim` subcommand and calls [`main`] with every test it
//! has. A sweep's children are the same binary (see [`sweep`]).

use std::io::Write;
use std::num::NonZeroUsize;
use std::ops::Range;
use std::process::ExitCode;
use std::time::Duration;

use clap::{Args, Subcommand};

use crate::COMMIT;
use crate::runner;
use crate::sweep::{self, SweepReport};
use crate::workload::TestSpec;

#[derive(Debug, Subcommand)]
pub enum SimCommand {
    /// List the tests built into this binary.
    List,
    /// Run one test with one seed and print its full trace, the seed, the commit, and the
    /// unseed.
    Run {
        #[arg(long)]
        test: String,
        #[arg(long)]
        seed: u64,
    },
    /// Run a test over a range of seeds in parallel child processes, and report the seeds
    /// that fail.
    Sweep {
        #[arg(long)]
        test: String,
        #[command(flatten)]
        seeds: SeedRange,
    },
    /// Run each seed twice, in separate child processes, and check that both runs end with
    /// the same unseed.
    CheckDeterminism {
        #[arg(long)]
        test: String,
        #[command(flatten)]
        seeds: SeedRange,
    },
    /// A sweep's child: runs each seed read from stdin and writes its result to stdout.
    #[command(hide = true)]
    SweepWorker {
        #[arg(long)]
        test: String,
    },
}

/// Which seeds a sweep runs, and how.
#[derive(Debug, Args)]
pub struct SeedRange {
    /// Seeds to run, as `start..end` (end excluded).
    #[arg(long, value_parser = parse_seeds)]
    seeds: Range<u64>,
    /// How many child processes to run at once. Defaults to one per CPU.
    #[arg(short, long)]
    jobs: Option<NonZeroUsize>,
    /// How many seconds of wall time one seed may take before its child is stopped and the
    /// seed fails.
    #[arg(long, default_value_t = 300)]
    seed_timeout: u64,
}

fn parse_seeds(s: &str) -> Result<Range<u64>, String> {
    let (start, end) = s
        .split_once("..")
        .ok_or_else(|| format!("{s:?} isn't a range like 0..1000"))?;
    let parse = |n: &str| n.parse::<u64>().map_err(|e| format!("{n:?}: {e}"));
    let seeds = parse(start)?..parse(end)?;
    if seeds.is_empty() {
        return Err(format!("{s:?} has no seeds"));
    }
    Ok(seeds)
}

/// Runs `command` with the tests in `tests`.
pub fn main(command: SimCommand, tests: &[TestSpec]) -> ExitCode {
    let name = match &command {
        SimCommand::List => {
            for test in tests {
                println!("{:<24} {}", test.name, test.about);
            }
            return ExitCode::SUCCESS;
        }
        SimCommand::Run { test, .. }
        | SimCommand::Sweep { test, .. }
        | SimCommand::CheckDeterminism { test, .. }
        | SimCommand::SweepWorker { test } => test,
    };
    let Some(spec) = tests.iter().find(|t| t.name == name) else {
        eprintln!("basalt sim: no test named {name:?}; `basalt sim list` shows them");
        return ExitCode::from(2);
    };
    match command {
        SimCommand::List => unreachable!("handled above"),
        SimCommand::Run { seed, .. } => run(spec, seed),
        SimCommand::Sweep { seeds, .. } => sweep(spec, &seeds),
        SimCommand::CheckDeterminism { seeds, .. } => check_determinism(spec, &seeds),
        SimCommand::SweepWorker { .. } => match sweep::worker(spec) {
            Ok(()) => ExitCode::SUCCESS,
            Err(e) => {
                eprintln!("basalt sim sweep-worker: {e}");
                ExitCode::FAILURE
            }
        },
    }
}

fn run(spec: &TestSpec, seed: u64) -> ExitCode {
    println!(
        "basalt sim run: test {} seed {seed} commit {COMMIT}",
        spec.name
    );
    // Printed as recorded, so a panic keeps the trace up to it.
    let report = runner::run(spec, seed, |record| println!("{record}"));
    match &report.result {
        Ok(()) => println!("result: passed"),
        Err(failure) => println!("result: FAILED: {failure}"),
    }
    println!("test: {}", report.test);
    println!("seed: {}", report.seed);
    println!("commit: {COMMIT}");
    println!("unseed: {:#018x}", report.unseed);
    exit(report.result.is_ok())
}

/// Sweeps `spec` over `range`, printing each failure as it comes in.
fn run_sweep(command: &str, spec: &TestSpec, range: &SeedRange) -> Option<SweepReport> {
    #[allow(
        clippy::disallowed_methods,
        reason = "only sizes the sweep's pool of child processes"
    )]
    let jobs = range
        .jobs
        .unwrap_or_else(|| std::thread::available_parallelism().unwrap_or(NonZeroUsize::MIN));
    let seeds = &range.seeds;
    println!(
        "basalt sim {command}: test {} seeds {}..{} jobs {jobs} commit {COMMIT}",
        spec.name, seeds.start, seeds.end
    );
    let timeout = Duration::from_secs(range.seed_timeout);
    let report = sweep::sweep(spec.name, seeds.clone(), jobs, timeout, |seed, why| {
        let _ = writeln!(std::io::stderr().lock(), "seed {seed} FAILED: {why}");
    });
    match report {
        Ok(report) => {
            for error in &report.worker_errors {
                eprintln!("{error}");
            }
            Some(report)
        }
        Err(e) => {
            eprintln!("basalt sim {command}: can't run child processes: {e}");
            None
        }
    }
}

fn sweep(spec: &TestSpec, range: &SeedRange) -> ExitCode {
    let Some(report) = run_sweep("sweep", spec, range) else {
        return ExitCode::FAILURE;
    };
    let failed: Vec<u64> = report.failed().map(|(seed, _)| seed).collect();
    println!("passed: {}, failed: {}", report.passed(), failed.len());
    for seed in &failed {
        println!(
            "failed seed {seed}; replay with: basalt sim run --test {} --seed {seed}",
            spec.name
        );
    }
    exit(failed.is_empty() && report.worker_errors.is_empty())
}

/// Sweeps the range twice and compares each seed's unseeds. Every run is in a child process,
/// so state that differs between processes shows up, and a panicking seed fails only itself.
fn check_determinism(spec: &TestSpec, range: &SeedRange) -> ExitCode {
    let reports = [(); 2].map(|()| run_sweep("check-determinism", spec, range));
    let [Some(first), Some(second)] = reports else {
        return ExitCode::FAILURE;
    };
    let (mut same, mut differ, mut unfinished) = (0u64, 0u64, 0u64);
    for (seed, a) in &first.seeds {
        let b = &second.seeds[seed];
        match (a.unseed, b.unseed) {
            (Some(x), Some(y)) if x == y => same += 1,
            // Aborted or stopped both times, so there's no unseed to compare.
            (None, None) => unfinished += 1,
            (x, y) => {
                differ += 1;
                let show = |u: Option<u64>| u.map_or("none".to_owned(), |u| format!("{u:#018x}"));
                println!(
                    "seed {seed}: NONDETERMINISTIC: unseed {} then {}",
                    show(x),
                    show(y)
                );
            }
        }
    }
    println!(
        "deterministic: {same}, nondeterministic: {differ}, unfinished both times: {unfinished}"
    );
    let workers_ok = first.worker_errors.is_empty() && second.worker_errors.is_empty();
    exit(differ == 0 && workers_ok)
}

fn exit(ok: bool) -> ExitCode {
    if ok {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}
