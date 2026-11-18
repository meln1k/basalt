//! Sweeps: one test over a range of seeds, in child processes of the same binary.
//!
//! Each child is a worker ([`worker`]), started as `<this binary> sim sweep-worker --test
//! <name>`. It reads seeds from stdin, one per line, runs each, and writes a tagged result to
//! stdout. The parent hands each idle child the next seed. A panic aborts its child
//! (`panic = "abort"` and the panic hook), and a run that takes too long in wall time is
//! stopped, so the seed that child was running is the failing one: the parent records it with
//! what the child wrote to stderr for that seed, and starts a new child for the seeds still to
//! run.
//!
//! # Protocol
//!
//! Before each run, the worker writes `basalt-sweep seed <seed>` to stderr, so the parent keeps
//! only that seed's stderr. After it, the worker writes one of these to stdout, each starting
//! on a fresh line:
//!
//! ```text
//! basalt-sweep pass <seed> <unseed> <coverage>
//! basalt-sweep fail <seed> <unseed> <coverage> <bytes>
//! <the failure text, exactly <bytes> bytes long>
//! ```
//!
//! `<coverage>` is the `buggify!` sites and probes the run reached, as
//! the coverage module encodes it.
//!
//! The parent skips every other line, so a stray `println!` in a test can't desync it.

// The parent spawns the children and threads to talk to each. Only the sweep's own control
// flow depends on them; every simulated run stays on one thread of one child.
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "a sweep runs child processes of the same binary, which is outside any simulation"
)]

use std::collections::BTreeMap;
use std::io::{self, BufRead, BufReader, Write};
use std::num::NonZeroUsize;
use std::ops::Range;
use std::path::Path;
use std::process::{Child, ChildStdin, Command, ExitStatus, Stdio};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::coverage::{self, SweepCoverage};
use crate::runner;
use crate::workload::TestSpec;

/// Starts every line of the protocol.
const TAG: &str = "basalt-sweep";

/// The signal a panicking run aborts with.
pub const SIGABRT: i32 = 6;

/// How one seed of a sweep ended.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SeedResult {
    /// The run's unseed, or `None` if its child aborted or was stopped before the run ended.
    pub unseed: Option<u64>,
    /// Why the seed failed, or `None` if it passed.
    pub failure: Option<String>,
}

/// How a sweep ended.
#[derive(Debug, Default)]
pub struct SweepReport {
    /// Every seed swept.
    pub seeds: BTreeMap<u64, SeedResult>,
    /// Children that misbehaved outside any seed, such as by exiting with an error after their
    /// last one.
    pub worker_errors: Vec<String>,
    /// What every seed that finished its run reached, between them.
    pub coverage: SweepCoverage,
}

impl SweepReport {
    pub fn passed(&self) -> usize {
        self.seeds.values().filter(|r| r.failure.is_none()).count()
    }

    /// Each failing seed and why it failed, in seed order.
    pub fn failed(&self) -> impl Iterator<Item = (u64, &str)> {
        self.seeds
            .iter()
            .filter_map(|(&seed, r)| Some((seed, r.failure.as_deref()?)))
    }
}

/// Runs the test named `test` for every seed in `seeds`, in `jobs` children of this binary,
/// stopping any child whose run takes longer than `timeout` of wall time. Calls `on_failure` as
/// each failure comes in.
pub fn sweep(
    test: &str,
    seeds: Range<u64>,
    jobs: NonZeroUsize,
    timeout: Duration,
    on_failure: impl Fn(u64, &str) + Sync,
) -> io::Result<SweepReport> {
    let program = std::env::current_exe()?;
    let next = Mutex::new(seeds);
    let report = Mutex::new(SweepReport::default());
    thread::scope(|scope| {
        for _ in 0..jobs.get() {
            scope.spawn(|| {
                let mut worker: Option<Worker> = None;
                loop {
                    let Some(seed) = next.lock().unwrap().next() else {
                        break;
                    };
                    let result = match &mut worker {
                        Some(w) => w.run(seed, timeout),
                        None => match Worker::spawn(&program, test) {
                            Ok(w) => worker.insert(w).run(seed, timeout),
                            Err(e) => Err(format!("can't start a sweep worker: {e}")),
                        },
                    };
                    let (result, coverage) = result.unwrap_or_else(|why| {
                        let failure = match worker.take() {
                            Some(w) => w.stop(&why),
                            None => why,
                        };
                        let result = SeedResult {
                            unseed: None,
                            failure: Some(failure),
                        };
                        (result, SweepCoverage::default())
                    });
                    if let Some(why) = &result.failure {
                        on_failure(seed, why);
                    }
                    let mut report = report.lock().unwrap();
                    report.seeds.insert(seed, result);
                    report.coverage.merge(coverage);
                }
                if let Some(error) = worker.and_then(Worker::finish) {
                    report.lock().unwrap().worker_errors.push(error);
                }
            });
        }
    });
    Ok(report.into_inner().unwrap())
}

/// One child process, the pipe to its stdin, and the threads reading its stdout and stderr.
struct Worker {
    child: Child,
    stdin: ChildStdin,
    /// Each result the child writes to stdout.
    replies: Receiver<io::Result<Reply>>,
    /// What the child wrote to stderr since it started its current seed.
    stderr: Arc<Mutex<String>>,
    readers: [JoinHandle<()>; 2],
}

#[derive(Debug, PartialEq, Eq)]
struct Reply {
    seed: u64,
    unseed: u64,
    coverage: String,
    failure: Option<String>,
}

impl Worker {
    fn spawn(program: &Path, test: &str) -> io::Result<Worker> {
        let mut child = Command::new(program)
            .args(["sim", "sweep-worker", "--test", test])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        let stdin = child.stdin.take().expect("piped");
        let mut stdout = BufReader::new(child.stdout.take().expect("piped"));
        let mut stderr = BufReader::new(child.stderr.take().expect("piped"));

        let (send, replies) = mpsc::channel();
        let stdout = thread::spawn(move || {
            while let Some(reply) = next_reply(&mut stdout).transpose() {
                let garbled = reply.is_err();
                if send.send(reply).is_err() || garbled {
                    return;
                }
            }
        });
        let text = Arc::new(Mutex::new(String::new()));
        let seed_text = text.clone();
        let stderr = thread::spawn(move || {
            let mut line = Vec::new();
            while matches!(stderr.read_until(b'\n', &mut line), Ok(1..)) {
                let line_text = String::from_utf8_lossy(&line);
                let mut text = seed_text.lock().unwrap();
                if line_text.starts_with(&format!("{TAG} seed ")) {
                    text.clear();
                } else {
                    text.push_str(&line_text);
                }
                line.clear();
            }
        });
        Ok(Worker {
            child,
            stdin,
            replies,
            stderr: text,
            readers: [stdout, stderr],
        })
    }

    /// Runs `seed`, and returns how it ended and what it reached. Returns why not if the child
    /// died, garbled its answer, or took longer than `timeout`; the child is then of no further
    /// use.
    fn run(&mut self, seed: u64, timeout: Duration) -> Result<(SeedResult, SweepCoverage), String> {
        writeln!(self.stdin, "{seed}")
            .and_then(|()| self.stdin.flush())
            .map_err(|e| format!("couldn't send the seed to the child process: {e}"))?;
        match self.replies.recv_timeout(timeout) {
            Ok(Ok(reply)) if reply.seed == seed => {
                let coverage = coverage::decode(&reply.coverage).ok_or_else(|| {
                    format!(
                        "the child process garbled its coverage: {:?}",
                        reply.coverage
                    )
                })?;
                let result = SeedResult {
                    unseed: Some(reply.unseed),
                    failure: reply.failure,
                };
                Ok((result, coverage))
            }
            Ok(Ok(reply)) => Err(format!(
                "the child process answered for seed {} instead",
                reply.seed
            )),
            Ok(Err(e)) => Err(format!("the child process garbled its answer: {e}")),
            Err(RecvTimeoutError::Timeout) => Err(format!(
                "the run took over {}s of wall time and was stopped",
                timeout.as_secs_f64()
            )),
            Err(RecvTimeoutError::Disconnected) => Err("the run didn't finish".to_owned()),
        }
    }

    /// Stops the child and says why it failed its seed: `why`, how it ended, and what it wrote
    /// to stderr for that seed.
    fn stop(mut self, why: &str) -> String {
        // It may have exited already; killing a process that's waiting to be reaped is a no-op.
        let _ = self.child.kill();
        let (status, stderr) = self.wait();
        format!(
            "{why}; the child process {}:\n{}",
            describe(status),
            stderr.trim_end()
        )
    }

    /// Tells the child there are no more seeds and waits for it. Returns what went wrong if it
    /// didn't exit cleanly.
    fn finish(self) -> Option<String> {
        let (status, stderr) = self.wait();
        (!status.success()).then(|| {
            format!(
                "a sweep worker {} after its last seed:\n{}",
                describe(status),
                stderr.trim_end()
            )
        })
    }

    fn wait(self) -> (ExitStatus, String) {
        let Worker {
            mut child,
            stdin,
            replies,
            stderr,
            readers,
        } = self;
        // Closing stdin tells a live worker there are no more seeds.
        drop(stdin);
        drop(replies);
        let status = child.wait().expect("waiting for a sweep worker");
        for reader in readers {
            let _ = reader.join();
        }
        let stderr = std::mem::take(&mut *stderr.lock().unwrap());
        (status, stderr)
    }
}

fn describe(status: ExitStatus) -> String {
    use std::os::unix::process::ExitStatusExt;
    match status.signal() {
        Some(SIGABRT) => "aborted (SIGABRT)".to_owned(),
        Some(signal) => format!("was killed by signal {signal}"),
        None => format!("exited with {status}"),
    }
}

/// Reads up to the next result in a worker's stdout, skipping any other output. Returns `None`
/// at the end of the output.
fn next_reply(out: &mut impl BufRead) -> io::Result<Option<Reply>> {
    let mut line = String::new();
    loop {
        line.clear();
        if out.read_line(&mut line)? == 0 {
            return Ok(None);
        }
        let words: Vec<&str> = line.trim_end_matches('\n').split(' ').collect();
        if words[0] != TAG {
            continue;
        }
        let bad = || {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("bad result line {line:?}"),
            )
        };
        let num = |word: &str| word.parse::<u64>().map_err(|_| bad());
        let reply = match words[1..] {
            ["pass", seed, unseed, coverage] => Reply {
                seed: num(seed)?,
                unseed: num(unseed)?,
                coverage: coverage.to_owned(),
                failure: None,
            },
            ["fail", seed, unseed, coverage, bytes] => {
                let mut text = vec![0; usize::try_from(num(bytes)?).map_err(|_| bad())?];
                out.read_exact(&mut text)?;
                Reply {
                    seed: num(seed)?,
                    unseed: num(unseed)?,
                    coverage: coverage.to_owned(),
                    failure: Some(String::from_utf8_lossy(&text).into_owned()),
                }
            }
            _ => return Err(bad()),
        };
        return Ok(Some(reply));
    }
}

/// The child side of a sweep: runs `spec` for each seed read from stdin, writing a result for
/// each to stdout, until stdin closes. A panic aborts the process, which tells the parent which
/// seed failed.
pub fn worker(spec: &TestSpec) -> io::Result<()> {
    for line in io::stdin().lock().lines() {
        let line = line?;
        let seed: u64 = line
            .trim()
            .parse()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("{line:?}: {e}")))?;
        eprintln!("{TAG} seed {seed}");
        let report = runner::run(spec, seed, |_| ());
        let unseed = report.unseed;
        let coverage = coverage::encode(&report.coverage);
        let mut out = io::stdout().lock();
        // Each result starts on a fresh line, even after stray output without a newline.
        match report.result {
            Ok(()) => write!(out, "\n{TAG} pass {seed} {unseed} {coverage}\n")?,
            Err(failure) => {
                let text = failure.to_string();
                write!(
                    out,
                    "\n{TAG} fail {seed} {unseed} {coverage} {}\n{text}",
                    text.len()
                )?;
            }
        }
        out.flush()?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn results_are_read_past_stray_output_and_keep_their_text() {
        let failure = "line one\nbasalt-sweep pass 9 9 ;;\n";
        let out = format!(
            "stray\n\n{TAG} pass 3 30 0,1;1;\nno newline\n{TAG} fail 4 40 ;; {}\n{failure}\nmore stray\n",
            failure.len()
        );
        let mut out = out.as_bytes();
        let reply = |seed, unseed, coverage: &str, failure: Option<&str>| Reply {
            seed,
            unseed,
            coverage: coverage.to_owned(),
            failure: failure.map(str::to_owned),
        };
        assert_eq!(
            next_reply(&mut out).unwrap(),
            Some(reply(3, 30, "0,1;1;", None))
        );
        assert_eq!(
            next_reply(&mut out).unwrap(),
            Some(reply(4, 40, ";;", Some(failure)))
        );
        assert_eq!(next_reply(&mut out).unwrap(), None);
    }

    #[test]
    fn a_garbled_result_is_an_error() {
        let mut out = format!("{TAG} pass three 30 ;;\n").into_bytes();
        assert!(next_reply(&mut out.as_slice()).is_err());
        out = format!("{TAG} pass 3 30\n").into_bytes();
        assert!(next_reply(&mut out.as_slice()).is_err());
        out = format!("{TAG} fail 4 40 ;; 100\nshort").into_bytes();
        assert!(next_reply(&mut out.as_slice()).is_err());
    }
}
