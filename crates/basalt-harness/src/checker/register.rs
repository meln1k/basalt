//! Keyed read/write registers: their operations, their [`Model`], and a linearizability check
//! that narrows down indeterminate operations before the search.

use std::hash::Hash;
use std::marker::PhantomData;

use basalt_env::HashMap;

use super::{LinearizabilityViolation, Model, Operation, Outcome, check_linearizability};

/// A read or a write of one key of a register store.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum RegisterOp<K, V> {
    Read(K),
    Write(K, V),
}

impl<K, V> RegisterOp<K, V> {
    /// The key the operation reads or writes.
    pub fn key(&self) -> &K {
        match self {
            RegisterOp::Read(key) | RegisterOp::Write(key, _) => key,
        }
    }
}

/// What a [`RegisterOp`] returned.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum RegisterOutput<V> {
    /// The value a read saw, or `None` if the key was never written.
    Read(Option<V>),
    /// A write was acknowledged.
    Written,
}

/// One operation on a register store.
pub type RegisterOperation<K, V> = Operation<RegisterOp<K, V>, RegisterOutput<V>>;

/// The [`Model`] of a store of independent read/write registers, one per key, each starting
/// empty.
pub struct Register<K, V>(PhantomData<fn() -> (K, V)>);

impl<K, V> Register<K, V> {
    pub fn new() -> Self {
        Register(PhantomData)
    }
}

impl<K, V> Default for Register<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> Model for Register<K, V>
where
    K: Clone + Eq + Hash,
    V: Clone + Eq + Hash,
{
    type Key = K;
    type State = Option<V>;
    type Input = RegisterOp<K, V>;
    type Output = RegisterOutput<V>;

    fn key(&self, input: &RegisterOp<K, V>) -> K {
        input.key().clone()
    }

    fn init(&self) -> Option<V> {
        None
    }

    /// # Panics
    ///
    /// If `output` doesn't match `input`, such as a read that returned [`RegisterOutput::Written`].
    fn step(
        &self,
        state: &Option<V>,
        input: &RegisterOp<K, V>,
        output: Option<&RegisterOutput<V>>,
    ) -> Option<Option<V>> {
        match (input, output) {
            (RegisterOp::Read(_), None) => Some(state.clone()),
            (RegisterOp::Read(_), Some(RegisterOutput::Read(seen))) => {
                (seen == state).then(|| state.clone())
            }
            (RegisterOp::Write(_, value), None | Some(RegisterOutput::Written)) => {
                Some(Some(value.clone()))
            }
            (RegisterOp::Read(_), Some(RegisterOutput::Written))
            | (RegisterOp::Write(..), Some(RegisterOutput::Read(_))) => {
                panic!("a register output doesn't match its operation")
            }
        }
    }
}

/// Checks that a register `history` is linearizable: [`check_linearizability`] with
/// [`Register`], after narrowing down its indeterminate operations.
///
/// Each indeterminate operation left open multiplies the configurations a failing search must
/// rule out. Before the search:
///
/// - Indeterminate reads are left out, since they change nothing and may have seen anything.
/// - An indeterminate write is left out if no read saw its value, since it can always be
///   overwritten before any read.
/// - An indeterminate write whose value a read saw, and no other write to the key wrote, took
///   effect before the first read that saw it returned, so it's checked as if it returned there.
///
/// # Write unique values
///
/// The verdict is right either way, but an indeterminate write of a value another write to the
/// same key also writes stays open. On one key with 10,000 operations, 1% of them
/// indeterminate, a failing check of unique values took about 0.6 s in a release build; drawn
/// from a thousand values it ran for over 25 s, and from a hundred it ran out of memory. Have
/// each client write its own id and a sequence number, for example.
///
/// # Panics
///
/// If an operation returns before its call, or its output doesn't match it.
pub fn check_register_linearizability<K, V>(
    history: &[RegisterOperation<K, V>],
) -> Result<(), LinearizabilityViolation<K>>
where
    K: Clone + Eq + Hash,
    V: Clone + Eq + Hash,
{
    validate(history);
    let (narrowed, original) = narrow(history);
    check_linearizability(&Register::new(), &narrowed).map_err(|violation| {
        LinearizabilityViolation {
            key: violation.key,
            longest: violation.longest.iter().map(|&i| original[i]).collect(),
        }
    })
}

/// `history` with indeterminate operations narrowed down as [`check_register_linearizability`]
/// describes, and the index in `history` of each operation kept.
fn narrow<K, V>(history: &[RegisterOperation<K, V>]) -> (Vec<RegisterOperation<K, V>>, Vec<usize>)
where
    K: Clone + Eq + Hash,
    V: Clone + Eq + Hash,
{
    let mut writes: HashMap<(&K, &V), usize> = HashMap::default();
    let mut first_seen: HashMap<(&K, &V), u64> = HashMap::default();
    for op in history {
        match (&op.input, &op.outcome) {
            (RegisterOp::Write(key, value), _) => *writes.entry((key, value)).or_default() += 1,
            (
                RegisterOp::Read(key),
                Outcome::Ok {
                    output: RegisterOutput::Read(Some(value)),
                    at,
                },
            ) => {
                let first = first_seen.entry((key, value)).or_insert(*at);
                *first = (*first).min(*at);
            }
            _ => {}
        }
    }

    let mut narrowed = Vec::new();
    let mut original = Vec::new();
    for (i, op) in history.iter().enumerate() {
        let outcome = match (&op.input, &op.outcome) {
            (_, Outcome::Ok { .. }) => op.outcome.clone(),
            (RegisterOp::Read(_), Outcome::Indeterminate) => continue,
            (RegisterOp::Write(key, value), Outcome::Indeterminate) => {
                match first_seen.get(&(key, value)) {
                    None => continue,
                    // A read before the call can't have seen this write, and fails the check.
                    Some(&at) if writes[&(key, value)] == 1 => Outcome::Ok {
                        output: RegisterOutput::Written,
                        at: at.max(op.call),
                    },
                    Some(_) => Outcome::Indeterminate,
                }
            }
        };
        narrowed.push(Operation {
            input: op.input.clone(),
            call: op.call,
            outcome,
        });
        original.push(i);
    }
    (narrowed, original)
}

/// Panics unless every operation that returned did so at or after its call, with an output
/// that matches it.
pub(super) fn validate<K, V>(history: &[RegisterOperation<K, V>]) {
    for (i, op) in history.iter().enumerate() {
        if let Outcome::Ok { output, at } = &op.outcome {
            assert!(
                *at >= op.call,
                "operation {i} returns at {at}, before its call at {}",
                op.call
            );
            assert!(
                matches!(
                    (&op.input, output),
                    (RegisterOp::Read(_), RegisterOutput::Read(_))
                        | (RegisterOp::Write(..), RegisterOutput::Written)
                ),
                "operation {i} has an output that doesn't match it"
            );
        }
    }
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;

    pub type Op = RegisterOperation<&'static str, u32>;

    pub fn write(key: &'static str, value: u32, call: u64, at: Option<u64>) -> Op {
        Operation {
            input: RegisterOp::Write(key, value),
            call,
            outcome: match at {
                Some(at) => Outcome::Ok {
                    output: RegisterOutput::Written,
                    at,
                },
                None => Outcome::Indeterminate,
            },
        }
    }

    pub fn read(key: &'static str, seen: Option<u32>, call: u64, at: Option<u64>) -> Op {
        Operation {
            input: RegisterOp::Read(key),
            call,
            outcome: match at {
                Some(at) => Outcome::Ok {
                    output: RegisterOutput::Read(seen),
                    at,
                },
                None => Outcome::Indeterminate,
            },
        }
    }

    /// Checks `history` with indeterminate operations narrowed down and left open, which must
    /// agree.
    fn check_both_ways(history: &[Op]) -> Result<(), LinearizabilityViolation<&'static str>> {
        let result = check_register_linearizability(history);
        assert_eq!(
            result.is_ok(),
            check_linearizability(&Register::new(), history).is_ok()
        );
        result
    }

    #[test]
    fn indeterminate_write_may_or_may_not_take_effect() {
        let history = |seen| {
            vec![
                write("x", 1, 0, Some(1)),
                write("x", 2, 2, None),
                read("x", Some(seen), 10, Some(11)),
            ]
        };
        assert_eq!(check_both_ways(&history(2)), Ok(()));
        assert_eq!(check_both_ways(&history(1)), Ok(()));

        // It can take effect long after its call, but once seen it can't be undone.
        let mut late = history(1);
        late.push(read("x", Some(2), 20, Some(21)));
        assert_eq!(check_both_ways(&late), Ok(()));
        late.push(read("x", Some(1), 30, Some(31)));
        let violation = check_both_ways(&late).unwrap_err();
        // Indices refer to `late`, not to the narrowed history.
        assert_eq!(violation.longest, [0, 2, 1, 3]);

        // It can't take effect before its call.
        let early = [read("x", Some(2), 0, Some(1)), write("x", 2, 2, None)];
        assert!(check_both_ways(&early).is_err());

        // An indeterminate read constrains nothing.
        let mut unread = history(1);
        unread.push(read("x", Some(7), 12, None));
        assert_eq!(check_both_ways(&unread), Ok(()));
    }

    #[test]
    fn indeterminate_write_of_a_value_already_written_stays_open() {
        // The first read saw the first write of 2, so the indeterminate write of 2 may never
        // take effect, and the last read may see 3.
        let history = [
            write("x", 2, 0, Some(1)),
            read("x", Some(2), 2, Some(3)),
            write("x", 3, 4, Some(5)),
            write("x", 2, 6, None),
            read("x", Some(3), 10, Some(11)),
        ];
        assert_eq!(check_both_ways(&history), Ok(()));
    }

    #[test]
    fn values_are_narrowed_per_key() {
        // y=2 was seen, but x=2 wasn't: the indeterminate write of x=2 may never take effect.
        let history = [
            write("y", 2, 0, Some(1)),
            read("y", Some(2), 2, Some(3)),
            write("x", 2, 4, None),
            read("x", None, 10, Some(11)),
        ];
        assert_eq!(check_both_ways(&history), Ok(()));
    }

    #[test]
    #[should_panic(expected = "operation 1 has an output that doesn't match it")]
    fn mismatched_output_panics() {
        let mut wrong = read("x", None, 2, Some(3));
        wrong.input = RegisterOp::Write("x", 1);
        let _ = check_register_linearizability(&[write("x", 1, 0, Some(1)), wrong]);
    }
}
