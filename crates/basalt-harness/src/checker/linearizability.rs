//! A linearizability checker in the style of Porcupine.
//!
//! The search is Wing and Gong's, with Lowe's memoization: walk the history's call and return
//! events in order, try to linearize each operation whose call comes before the first pending
//! return, and backtrack when a return arrives for an operation not linearized yet. A cache of
//! (linearized set, model state) pairs keeps the search from exploring the same configuration
//! twice. The history is split per key first, since operations on different keys don't
//! constrain each other, which keeps each search small.
//!
//! An indeterminate operation gets a return after every other event, so the search may
//! linearize it anywhere after its call, including after everything else, which is the same as
//! never taking effect. Each one multiplies the configurations a failing search must rule out,
//! so a model that knows more should narrow them down first, as
//! [`check_register_linearizability`](super::check_register_linearizability) does.

use std::hash::Hash;

use basalt_env::{HashMap, HashSet};

use super::{Operation, Outcome};

/// The sequential specification that a history is checked against.
///
/// Histories are split by [`Model::key`] and each key is checked on its own, starting from
/// [`Model::init`], so a model describes a single key.
pub trait Model {
    /// What splits a history into independent parts. Use `()` to check it as one.
    type Key: Clone + Eq + Hash;
    /// The state of one key. The search caches states, so keep them small.
    type State: Clone + Eq + Hash;
    type Input;
    type Output;

    /// The key `input` acts on.
    fn key(&self, input: &Self::Input) -> Self::Key;

    /// The state of a key before any operation.
    fn init(&self) -> Self::State;

    /// Applies `input` to `state`. Returns the next state if the operation, with `output`, is
    /// legal in `state`, or `None` if it isn't. `output` is `None` for an indeterminate
    /// operation, which is legal whenever some output would be.
    fn step(
        &self,
        state: &Self::State,
        input: &Self::Input,
        output: Option<&Self::Output>,
    ) -> Option<Self::State>;
}

/// The operations on one key have no linearization.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LinearizabilityViolation<K> {
    pub key: K,
    /// The longest sequence of operations on `key` the search could linearize, as indices into
    /// the checked history, in linearization order. No remaining operation on `key` can come
    /// next, so the problem is usually near its end.
    pub longest: Vec<usize>,
}

/// Checks that `history` is linearizable with respect to `model`.
///
/// Keys are checked in the order they first appear, and the first key without a linearization
/// is returned.
///
/// # Panics
///
/// If an operation returns before its call.
pub fn check_linearizability<M: Model>(
    model: &M,
    history: &[Operation<M::Input, M::Output>],
) -> Result<(), LinearizabilityViolation<M::Key>> {
    let mut keys: HashMap<M::Key, usize> = HashMap::default();
    let mut partitions: Vec<(M::Key, Vec<usize>)> = Vec::new();
    for (i, op) in history.iter().enumerate() {
        if let Outcome::Ok { at, .. } = op.outcome {
            assert!(
                at >= op.call,
                "operation {i} returns at {at}, before its call at {}",
                op.call
            );
        }
        let key = model.key(&op.input);
        let part = *keys.entry(key.clone()).or_insert_with(|| {
            partitions.push((key, Vec::new()));
            partitions.len() - 1
        });
        partitions[part].1.push(i);
    }

    for (key, operations) in partitions {
        let ops = operations
            .into_iter()
            .map(|i| match history[i].outcome {
                Outcome::Ok { at, .. } => (i, at),
                Outcome::Indeterminate => (i, u64::MAX),
            })
            .collect();
        if let Err(longest) = search(model, history, ops) {
            return Err(LinearizabilityViolation { key, longest });
        }
    }
    Ok(())
}

const NIL: usize = usize::MAX;

/// Searches for a linearization of `ops`, each a history index and the position it returns
/// at. On failure, returns the longest partial linearization found, as history indices.
fn search<M: Model>(
    model: &M,
    history: &[Operation<M::Input, M::Output>],
    mut ops: Vec<(usize, u64)>,
) -> Result<(), Vec<usize>> {
    // Number the operations in call order, so the ones linearized so far are nearly a prefix
    // and the cache keys stay small (see `Linearized`).
    ops.sort_by_key(|&(i, _)| (history[i].call, i));

    // Events as a doubly linked list over node indices: the call of operation `j` is node `2j`,
    // its return is `2j + 1`, and `head` is a sentinel before the first event.
    let n = ops.len();
    let head = 2 * n;
    let mut events: Vec<(u64, usize)> = Vec::with_capacity(2 * n);
    for (j, &(i, at)) in ops.iter().enumerate() {
        events.push((history[i].call, 2 * j));
        events.push((at, 2 * j + 1));
    }
    // At equal positions, calls (even nodes) sort before returns, which makes them concurrent.
    events.sort_unstable_by_key(|&(time, node)| (time, node % 2, node));
    let mut next = vec![NIL; 2 * n + 1];
    let mut prev = vec![NIL; 2 * n + 1];
    let mut last = head;
    for &(_, node) in &events {
        next[last] = node;
        prev[node] = last;
        last = node;
    }

    // Removes a call and its return from the list; `unlift` puts them back. Lifts are undone
    // in reverse order, so each node's `prev` and `next` are still its old neighbours.
    let lift = |next: &mut [usize], prev: &mut [usize], call: usize| {
        for node in [call, call + 1] {
            next[prev[node]] = next[node];
            if next[node] != NIL {
                prev[next[node]] = prev[node];
            }
        }
    };
    let unlift = |next: &mut [usize], prev: &mut [usize], call: usize| {
        for node in [call + 1, call] {
            next[prev[node]] = node;
            if next[node] != NIL {
                prev[next[node]] = node;
            }
        }
    };

    let mut state = model.init();
    let mut linearized = Linearized::default();
    let mut seen: HashSet<(Linearized, M::State)> = HashSet::default();
    // The calls linearized so far, each with what `Linearized::insert` returned and the state
    // before it.
    let mut stack: Vec<(usize, usize, M::State)> = Vec::new();
    let mut longest: Vec<usize> = Vec::new();
    let mut at_peak = false;

    let mut node = next[head];
    while next[head] != NIL {
        if node.is_multiple_of(2) {
            let op = &history[ops[node / 2].0];
            let output = match &op.outcome {
                Outcome::Ok { output, .. } => Some(output),
                Outcome::Indeterminate => None,
            };
            if let Some(after) = model.step(&state, &op.input, output) {
                let end = linearized.insert(node / 2);
                if seen.insert((linearized.clone(), after.clone())) {
                    stack.push((node, end, std::mem::replace(&mut state, after)));
                    lift(&mut next, &mut prev, node);
                    at_peak |= stack.len() > longest.len();
                    node = next[head];
                    continue;
                }
                linearized.remove(node / 2, end);
            }
            node = next[node];
        } else {
            // A return for an operation not linearized yet: undo the last choice.
            if at_peak {
                longest = stack.iter().map(|&(call, ..)| ops[call / 2].0).collect();
                at_peak = false;
            }
            let Some((call, end, before)) = stack.pop() else {
                return Err(longest);
            };
            linearized.remove(call / 2, end);
            state = before;
            unlift(&mut next, &mut prev, call);
            node = next[call];
        }
    }
    Ok(())
}

/// A set of operations, by number in call order, stored as the end of the range it covers and
/// the operations missing from that range. The search only linearizes operations called before
/// the first pending return, so the gaps are the operations still in flight, plus indeterminate
/// ones not linearized yet: a few, where a bitset would grow with the history.
#[derive(Clone, Default, PartialEq, Eq, Hash)]
struct Linearized {
    end: usize,
    gaps: Vec<usize>,
}

impl Linearized {
    /// Adds operation `j`, which isn't in the set. Returns the old end, for `remove`.
    fn insert(&mut self, j: usize) -> usize {
        let end = self.end;
        if j < end {
            let at = self.gaps.binary_search(&j).expect("already linearized");
            self.gaps.remove(at);
        } else {
            self.gaps.extend(end..j);
            self.end = j + 1;
        }
        end
    }

    /// Undoes the last `insert(j)`, which returned `end`.
    fn remove(&mut self, j: usize, end: usize) {
        if j < end {
            let at = self.gaps.binary_search(&j).unwrap_err();
            self.gaps.insert(at, j);
        } else {
            self.gaps.truncate(self.gaps.len() - (j - end));
            self.end = end;
        }
    }
}

#[cfg(test)]
mod tests {
    use basalt_env::Rng;

    use super::super::register::tests::{Op, read, write};
    use super::super::{Register, RegisterOp, RegisterOutput, check_register_linearizability};
    use super::*;

    fn check(history: &[Op]) -> Result<(), LinearizabilityViolation<&'static str>> {
        check_linearizability(&Register::new(), history)
    }

    #[test]
    fn concurrent_history_passes() {
        // Two overlapping writes; the reads overlapping both may see either, but must agree with
        // one order: x=2 is seen, then x=1, so 2 linearizes before 1.
        let history = [
            read("x", None, 0, Some(3)),
            write("x", 1, 1, Some(20)),
            write("x", 2, 2, Some(10)),
            read("x", Some(2), 4, Some(8)),
            read("x", Some(1), 9, Some(25)),
            read("x", Some(1), 21, Some(22)),
        ];
        assert_eq!(check(&history), Ok(()));
    }

    #[test]
    fn new_old_inversion_fails() {
        // A write of 2 is in flight. One read sees 2, and a later read sees 1 again.
        let history = [
            write("x", 1, 0, Some(1)),
            write("x", 2, 2, Some(100)),
            read("x", Some(2), 3, Some(4)),
            read("x", Some(1), 5, Some(6)),
        ];
        let violation = check(&history).unwrap_err();
        assert_eq!(violation.key, "x");
        assert_eq!(violation.longest, [0, 1, 2]);

        // The same reads overlapping each other are fine: 1 can be read first.
        let mut overlapping = history.clone();
        overlapping[2] = read("x", Some(2), 3, Some(6));
        overlapping[3] = read("x", Some(1), 5, Some(7));
        assert_eq!(check(&overlapping), Ok(()));
    }

    #[test]
    fn equal_positions_are_concurrent() {
        // The write returns at the same position the read is called, so the read may come
        // first and see nothing.
        assert_eq!(
            check(&[write("x", 1, 0, Some(5)), read("x", None, 5, Some(6))]),
            Ok(())
        );
        assert!(check(&[write("x", 1, 0, Some(5)), read("x", None, 6, Some(7))]).is_err());
    }

    #[test]
    fn keys_are_checked_separately() {
        // Each key on its own is linearizable, and the stale read of y is reported against y.
        let history = [
            write("x", 1, 0, Some(1)),
            write("y", 1, 2, Some(3)),
            read("x", Some(1), 4, Some(5)),
            read("y", None, 4, Some(5)),
        ];
        let violation = check(&history).unwrap_err();
        assert_eq!(violation.key, "y");
        assert_eq!(violation.longest, [1]);
    }

    /// A linearizable history of `len` operations by `clients` concurrent clients on one key,
    /// half writes and half reads, each write of a unique value. One operation in a hundred is
    /// indeterminate, and half of the indeterminate writes take effect.
    fn generate(rng: &Rng, len: usize, clients: usize) -> Vec<Op> {
        let mut free = vec![0u64; clients];
        // Each operation, with the position it takes effect at and whether it does.
        let mut generated: Vec<(Op, u64, bool)> = Vec::with_capacity(len);
        for i in 0..len {
            let client = (0..clients).min_by_key(|&c| (free[c], c)).unwrap();
            let call = free[client] + rng.range(0..3u64);
            let at = call + rng.range(1..20u64);
            free[client] = at + 1;
            let returned = (!rng.bool(0.01)).then_some(at);
            let op = if rng.bool(0.5) {
                write("x", i as u32, call, returned)
            } else {
                read("x", None, call, returned)
            };
            let effective = returned.is_some() || rng.bool(0.5);
            generated.push((op, rng.range(call..=at), effective));
        }

        // Apply the operations in linearization order to fill in what the reads saw.
        let mut order: Vec<usize> = (0..len).collect();
        order.sort_by_key(|&i| (generated[i].1, i));
        let mut value = None;
        for i in order {
            let (op, _, effective) = &mut generated[i];
            match (&op.input, &mut op.outcome) {
                (RegisterOp::Write(_, v), _) if *effective => value = Some(*v),
                (RegisterOp::Read(_), Outcome::Ok { output, .. }) => {
                    *output = RegisterOutput::Read(value);
                }
                _ => {}
            }
        }
        generated.into_iter().map(|(op, ..)| op).collect()
    }

    #[test]
    #[allow(clippy::disallowed_types, clippy::disallowed_methods)] // Measures real time.
    fn checks_ten_thousand_operations_within_budget() {
        let history = generate(&Rng::new(0), 10_000, 8);
        assert!(
            history
                .iter()
                .any(|op| matches!(op.outcome, Outcome::Indeterminate))
        );

        // A read near the end that sees a value overwritten long before fails, after a search
        // that rules out every other order.
        let mut stale = history.clone();
        let last_read = (0..stale.len())
            .rev()
            .find(|&i| {
                matches!(
                    stale[i].outcome,
                    Outcome::Ok {
                        output: RegisterOutput::Read(Some(_)),
                        ..
                    }
                )
            })
            .unwrap();
        let old = (0..last_read - 100)
            .rev()
            .find_map(|i| match (&stale[i].input, &stale[i].outcome) {
                (RegisterOp::Write(_, v), Outcome::Ok { .. }) => Some(*v),
                _ => None,
            })
            .unwrap();
        if let Outcome::Ok { output, .. } = &mut stale[last_read].outcome {
            *output = RegisterOutput::Read(Some(old));
        }

        let started = std::time::Instant::now();
        assert_eq!(check_register_linearizability(&history), Ok(()));
        let passing = started.elapsed();
        let started = std::time::Instant::now();
        assert!(check_register_linearizability(&stale).is_err());
        let failing = started.elapsed();

        // The budget for each check of unique values (see `check_register_linearizability`): 2 s
        // in a release build, and 10 s in the unoptimized test build CI runs. On a 2-core orb, the checks took 0.1 s and 0.55 s in release, and
        // 0.5 s and 2.3 s unoptimized.
        let budget = std::time::Duration::from_secs(if cfg!(debug_assertions) { 10 } else { 2 });
        eprintln!("10,000 operations: passing check {passing:?}, failing check {failing:?}");
        assert!(passing < budget, "passing check took {passing:?}");
        assert!(failing < budget, "failing check took {failing:?}");
    }
}
