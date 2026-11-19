//! The toy register's simulation tests: clients read and write keys through 3 replicas while
//! every fault workload the seed enables strikes, then the checkers run over what they saw.
//!
//! - [`REGISTER`]: no planted bug; every seed passes.
//! - [`REGISTER_SKIP_WRITEBACK`]: reads skip their write-back, which the linearizability checker
//!   catches as a new-old inversion.
//! - [`REGISTER_ACK_BEFORE_SYNC`]: replicas acknowledge before syncing, so a power cut on a
//!   majority loses an acknowledged write. The durability checker catches it when the lost
//!   write was its key's last; the linearizability checker, when a later read misses it.
//!
//! The three differ only in their [`Bugs`], and the fault workloads draw from their own stream,
//! so a seed sees the same faults in each. A seed that fails with a bug on can be rerun with
//! the bug off.
//!
//! Most operations go to a few hot keys, where clients often meet, which a new-old inversion
//! needs. The rest write fresh keys that are never written again, or read one: a lost write to a
//! hot key is soon overwritten, but one to a fresh key stays lost, for the durability check's
//! final reads to see.
//!
//! Each client writes values that name it and number its operations, so every write is unique,
//! which the checkers need. Attrition never wipes a replica ([`Wipes::Off`]): a wiped
//! replica comes back empty, and the register can't rebuild it, so it would lose acknowledged
//! writes through no planted bug.

use std::cell::Cell;
use std::collections::BTreeSet;
use std::fmt;
use std::rc::Rc;

use basalt_env::{Ctx, Duration, SocketAddr};
use basalt_sim::SimControl;
use toy_register::{Bugs, Client, DEFAULT_PORT, ReplicaConfig, run_replica};

use crate::checker::{
    DurabilityViolation, LinearizabilityViolation, RegisterOp, RegisterOutput, check_durability,
    check_register_linearizability,
};
use crate::faults::{self, Wipes};
use crate::{Entry, History, Outcome, TestSpec, Workload, WorkloadResult, Workloads};

/// The register with no planted bug.
pub const REGISTER: TestSpec = TestSpec {
    name: "register",
    about: "clients read and write a 3-replica ABD register under every fault; no planted bug",
    chaos: Duration::from_secs(60),
    quiet: Duration::from_secs(60),
    build: |sim| build(sim, Bugs::default()),
};

/// The register with reads that skip their write-back.
pub const REGISTER_SKIP_WRITEBACK: TestSpec = TestSpec {
    name: "register-skip-writeback",
    about: "the register with the skip-writeback bug planted; the linearizability checker \
            catches it",
    build: |sim| {
        let bugs = Bugs {
            skip_writeback: true,
            ..Bugs::default()
        };
        build(sim, bugs)
    },
    ..REGISTER
};

/// The register with replicas that acknowledge before syncing.
pub const REGISTER_ACK_BEFORE_SYNC: TestSpec = TestSpec {
    name: "register-ack-before-sync",
    about: "the register with the ack-before-sync bug planted; the durability checker catches it",
    build: |sim| {
        let bugs = Bugs {
            ack_before_sync: true,
            ..Bugs::default()
        };
        build(sim, bugs)
    },
    ..REGISTER
};

/// The tests in this module.
pub const TESTS: &[TestSpec] = &[REGISTER, REGISTER_SKIP_WRITEBACK, REGISTER_ACK_BEFORE_SYNC];

const REPLICAS: usize = 3;
/// Clients in the chaos phase. Their writer ids are `0..CLIENTS`.
const CLIENTS: u64 = 4;
/// The hot keys, `0..HOT_KEYS`.
const HOT_KEYS: u64 = 4;
/// How often an operation goes to a hot key rather than a fresh one.
const HOT_PROBABILITY: f64 = 0.95;
/// The key the liveness check writes, which no chaos-phase operation touches.
const LIVENESS_KEY: u64 = HOT_KEYS;
/// The first fresh key. Fresh keys are handed out in order from here.
const FIRST_FRESH: u64 = LIVENESS_KEY + 1;
/// The writer id of the clients the quiet and check phases use.
const CHECKER: u64 = CLIENTS;
/// How long a chaos-phase operation may take before the client gives up on it, leaving it
/// indeterminate. A few of the client's per-replica request timeouts.
const OP_TIMEOUT: Duration = Duration::from_secs(10);
/// The longest pause a client takes between operations.
const MAX_PAUSE: Duration = Duration::from_millis(20);
/// How many operations a failure report lists on each side of where the linearizability
/// search got stuck.
const REPORTED_OPS: usize = 5;

/// A value one client wrote: which client, and which of its operations. Unique per write.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct Value {
    client: u64,
    seq: u64,
}

impl Value {
    fn encode(self) -> Vec<u8> {
        [self.client.to_le_bytes(), self.seq.to_le_bytes()].concat()
    }

    fn decode(bytes: &[u8]) -> Result<Value, String> {
        let bytes: [u8; 16] = bytes
            .try_into()
            .map_err(|_| format!("a read returned {bytes:?}, which no client wrote"))?;
        let (client, seq) = bytes.split_at(8);
        let u64 = |b: &[u8]| u64::from_le_bytes(b.try_into().expect("8 bytes"));
        Ok(Value {
            client: u64(client),
            seq: u64(seq),
        })
    }
}

/// `client#seq`, as failure reports show it.
impl fmt::Debug for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}#{}", self.client, self.seq)
    }
}

type Op = RegisterOp<u64, Value>;
type Out = RegisterOutput<Value>;

fn build(sim: &SimControl, bugs: Bugs) -> Workloads {
    let replicas: Vec<SocketAddr> = (0..REPLICAS)
        .map(|i| {
            let machine = sim.add_machine(&format!("zone-{i}"), 1);
            let config = ReplicaConfig {
                bugs,
                ..ReplicaConfig::default()
            };
            sim.add_process(machine, &format!("replica-{i}"), move |ctx| {
                Box::pin(run_replica(ctx, config))
            });
            SocketAddr::new(sim.ip(machine), DEFAULT_PORT)
        })
        .collect();
    let history = History::new();
    let next_fresh = Rc::new(Cell::new(FIRST_FRESH));
    let mut workloads = Workloads::new();
    faults::add(&mut workloads, sim, Wipes::Off);
    for id in 0..CLIENTS {
        workloads.add(ChaosClient {
            id,
            replicas: replicas.clone(),
            bugs,
            history: history.clone(),
            next_fresh: next_fresh.clone(),
        });
    }
    workloads.add(Checks {
        replicas,
        bugs,
        history,
    });
    workloads
}

/// A register client that records each operation in a history.
struct RecordingClient {
    id: u64,
    client: Client,
    history: History<Op, Out>,
}

impl RecordingClient {
    fn new(
        ctx: &Ctx,
        id: u64,
        replicas: &[SocketAddr],
        bugs: Bugs,
        history: &History<Op, Out>,
    ) -> Self {
        RecordingClient {
            id,
            client: Client::new(ctx.clone(), replicas.to_vec(), id, bugs),
            history: history.clone(),
        }
    }

    /// Writes this client's value numbered `seq` to `key`. Dropped unfinished, the write stays
    /// indeterminate.
    async fn write(&mut self, ctx: &Ctx, key: u64, seq: u64) -> Value {
        let value = Value {
            client: self.id,
            seq,
        };
        let call = self
            .history
            .invoke(ctx, self.id, RegisterOp::Write(key, value));
        self.client.write(key, value.encode()).await;
        call.ok(ctx, RegisterOutput::Written);
        value
    }

    /// Reads `key`, or fails if it holds a value no client wrote. Dropped unfinished, the read
    /// stays indeterminate.
    async fn read(&mut self, ctx: &Ctx, key: u64) -> Result<Option<Value>, String> {
        let call = self.history.invoke(ctx, self.id, RegisterOp::Read(key));
        let read = self.client.read(key).await;
        let value = read.as_deref().map(Value::decode).transpose()?;
        call.ok(ctx, RegisterOutput::Read(value));
        Ok(value)
    }
}

/// One client in the chaos phase: reads and writes random keys, recording each operation.
struct ChaosClient {
    id: u64,
    replicas: Vec<SocketAddr>,
    bugs: Bugs,
    history: History<Op, Out>,
    /// The next fresh key, shared by every client.
    next_fresh: Rc<Cell<u64>>,
}

impl Workload for ChaosClient {
    fn name(&self) -> &'static str {
        "register-client"
    }

    async fn start(&mut self, ctx: &Ctx, _: &SimControl) -> WorkloadResult {
        let mut client =
            RecordingClient::new(ctx, self.id, &self.replicas, self.bugs, &self.history);
        for seq in 0.. {
            let is_write = ctx.rng().bool(0.5);
            let key = if ctx.rng().bool(HOT_PROBABILITY) {
                ctx.rng().range(0..HOT_KEYS)
            } else if is_write {
                let key = self.next_fresh.get();
                self.next_fresh.set(key + 1);
                key
            } else {
                // A fresh key, maybe the next one, which no write has taken yet.
                ctx.rng().range(FIRST_FRESH..=self.next_fresh.get())
            };
            // An operation that times out is dropped, which leaves it indeterminate.
            if is_write {
                let _ = ctx.timeout(OP_TIMEOUT, client.write(ctx, key, seq)).await;
            } else if let Ok(read) = ctx.timeout(OP_TIMEOUT, client.read(ctx, key)).await {
                read?;
            }
            ctx.sleep(ctx.rng().range(Duration::ZERO..=MAX_PAUSE)).await;
        }
        Ok(())
    }
}

/// The quiet and check phases: a fresh write and read, then the checkers.
struct Checks {
    replicas: Vec<SocketAddr>,
    bugs: Bugs,
    history: History<Op, Out>,
}

impl Workload for Checks {
    fn name(&self) -> &'static str {
        "register-checks"
    }

    async fn start(&mut self, _: &Ctx, _: &SimControl) -> WorkloadResult {
        Ok(())
    }

    /// Writes a value to a key no chaos-phase operation used and reads it back, within the
    /// quiet phase's deadline.
    async fn liveness(&mut self, ctx: &Ctx) -> WorkloadResult {
        let mut client =
            RecordingClient::new(ctx, CHECKER, &self.replicas, self.bugs, &self.history);
        let value = client.write(ctx, LIVENESS_KEY, 0).await;
        let read = client.read(ctx, LIVENESS_KEY).await?;
        if read != Some(value) {
            return Err(format!("the liveness write of {value:?} read back as {read:?}").into());
        }
        Ok(())
    }

    /// Reads every key written once more, then checks the history for linearizability and the
    /// final values for durability. The final reads stay out of the history, so a lost write
    /// is the durability checker's to report.
    async fn check(&mut self, ctx: &Ctx) -> WorkloadResult {
        let entries = self.history.entries();
        let (ops, indices) = self.history.to_checker();
        let written: BTreeSet<u64> = entries
            .iter()
            .filter_map(|e| match e.input {
                RegisterOp::Write(key, _) => Some(key),
                RegisterOp::Read(_) => None,
            })
            .collect();
        let mut client = Client::new(ctx.clone(), self.replicas.clone(), CHECKER, self.bugs);
        let mut finals = Vec::new();
        for key in written {
            let value = client.read(key).await;
            finals.push((key, value.as_deref().map(Value::decode).transpose()?));
        }

        // The checkers report indices into `ops`; these are the entries they name.
        let entry = |i: usize| &entries[indices[i]];
        let mut report = Vec::new();
        if let Err(violation) = check_register_linearizability(&ops) {
            report.extend(linearizability_report(&violation, &entries, entry));
        }
        if let Err(violations) = check_durability(&ops, &finals) {
            report.extend(durability_report(violations, entry));
        }
        if report.is_empty() {
            Ok(())
        } else {
            Err(report.join("\n").into())
        }
    }
}

/// The end of the longest prefix the search linearized on the violation's key, then the first
/// operations on that key that returned and aren't in it.
fn linearizability_report<'a>(
    violation: &LinearizabilityViolation<u64>,
    entries: &[Entry<Op, Out>],
    entry: impl Fn(usize) -> &'a Entry<Op, Out>,
) -> Vec<String> {
    let mut lines = vec![format!(
        "linearizability violation on key {}: no operation can follow these, the last of the \
         longest linearizable prefix:",
        violation.key
    )];
    let start = violation.longest.len().saturating_sub(REPORTED_OPS);
    let prefix: Vec<&Entry<Op, Out>> = violation.longest.iter().map(|&i| entry(i)).collect();
    lines.extend(prefix[start..].iter().map(|e| format!("  {e}")));
    lines.push("  and the first operations left that returned:".to_owned());
    // By call position, which is unique per entry.
    let linearized: BTreeSet<u64> = prefix.iter().map(|e| e.call.position).collect();
    let left = entries.iter().filter(|e| {
        *e.input.key() == violation.key
            && matches!(e.outcome, Outcome::Ok { .. })
            && !linearized.contains(&e.call.position)
    });
    lines.extend(left.take(REPORTED_OPS).map(|e| format!("  {e}")));
    lines
}

/// Each key whose final value lost acknowledged writes, with the write the value came from and
/// the writes it lost.
fn durability_report<'a>(
    violations: Vec<DurabilityViolation<u64>>,
    entry: impl Fn(usize) -> &'a Entry<Op, Out>,
) -> Vec<String> {
    let mut lines = Vec::new();
    for violation in violations {
        match violation {
            DurabilityViolation::Lost {
                key,
                writes,
                final_write,
            } => {
                let from = match final_write {
                    Some(i) => format!("the value of {}", entry(i)),
                    None => "never written".to_owned(),
                };
                lines.push(format!(
                    "durability violation on key {key}: after the quiet phase it reads as {from}, \
                     losing acknowledged writes:"
                ));
                lines.extend(writes.into_iter().map(|i| format!("  {}", entry(i))));
            }
            other => lines.push(format!("durability violation: {other:?}")),
        }
    }
    lines
}
