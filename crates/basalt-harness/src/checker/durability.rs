//! A durability checker: no acknowledged write is lost.
//!
//! After the quiet phase, the harness reads every key once more. The value it reads must come
//! from a write that nothing acknowledged later superseded: if a write was acknowledged before
//! another acknowledged write was invoked, the later one must win. An indeterminate write may or
//! may not have taken effect, so it can be the final value but is never required to be.
//!
//! [`check_register_linearizability`](super::check_register_linearizability) over a history
//! that ends with these final reads rejects everything this checker does. This one exists for
//! its report: it names the lost writes, where a linearizability violation only shows where the
//! search got stuck.

use std::hash::Hash;

use basalt_env::{HashMap, HashSet};

use super::register::validate;
use super::{Outcome, RegisterOp, RegisterOperation};

/// A key whose final value loses or invents a write.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DurabilityViolation<K> {
    /// Acknowledged writes that the final value doesn't reflect.
    Lost {
        key: K,
        /// The acknowledged writes invoked after the final value's write returned, as indices
        /// into the history.
        writes: Vec<usize>,
        /// The write the final value came from, or `None` if the key reads as never written.
        final_write: Option<usize>,
    },
    /// The final value of `key` isn't one that any write in the history wrote.
    Unknown { key: K },
    /// `key` has acknowledged writes, but no final value was given for it.
    Unread { key: K },
}

/// Checks that the final value of each key reflects every acknowledged write to it.
///
/// `finals` holds each key's value as read after the quiet phase, with `None` for a key that
/// reads as never written. Every key with an acknowledged write in `history` must appear in it.
/// Reads in `history` are ignored. Writes to a key are assumed to write distinct values; if two
/// write the same value, the final value may come from either.
///
/// Returns every violation found, keys in `finals` first, in its order.
///
/// # Panics
///
/// If an operation returns before its call or its output doesn't match it, or a key appears in
/// `finals` twice.
pub fn check_durability<K, V>(
    history: &[RegisterOperation<K, V>],
    finals: &[(K, Option<V>)],
) -> Result<(), Vec<DurabilityViolation<K>>>
where
    K: Clone + Eq + Hash,
    V: Eq,
{
    validate(history);
    let mut read: HashSet<&K> = HashSet::default();
    for (key, _) in finals {
        assert!(read.insert(key), "a key appears in `finals` twice");
    }

    let mut writes: HashMap<&K, Vec<usize>> = HashMap::default();
    let mut keys: Vec<&K> = Vec::new();
    for (i, op) in history.iter().enumerate() {
        if let RegisterOp::Write(key, _) = &op.input {
            writes
                .entry(key)
                .or_insert_with(|| {
                    keys.push(key);
                    Vec::new()
                })
                .push(i);
        }
    }
    let acked_at = |i: usize| match history[i].outcome {
        Outcome::Ok { at, .. } => Some(at),
        Outcome::Indeterminate => None,
    };

    let mut violations = Vec::new();
    for (key, value) in finals {
        let to_key = writes.get(key).map(Vec::as_slice).unwrap_or_default();
        let acked: Vec<usize> = to_key
            .iter()
            .copied()
            .filter(|&i| acked_at(i).is_some())
            .collect();

        // Where the final value came from: the initial state, or a write of that value. An
        // indeterminate write returns after everything, so it's never superseded.
        let returned = match value {
            None => None,
            Some(value) => {
                let returns = to_key
                    .iter()
                    .copied()
                    .filter(|&i| matches!(&history[i].input, RegisterOp::Write(_, v) if v == value))
                    .map(|i| (acked_at(i).unwrap_or(u64::MAX), i))
                    .max();
                match returns {
                    Some(source) => Some(source),
                    None => {
                        violations.push(DurabilityViolation::Unknown { key: key.clone() });
                        continue;
                    }
                }
            }
        };

        // A write acknowledged before another acknowledged write's call must have been
        // overwritten. Equal positions are concurrent.
        let lost: Vec<usize> = acked
            .into_iter()
            .filter(|&i| returned.is_none_or(|(at, _)| history[i].call > at))
            .collect();
        if !lost.is_empty() {
            violations.push(DurabilityViolation::Lost {
                key: key.clone(),
                writes: lost,
                final_write: returned.map(|(_, i)| i),
            });
        }
    }

    for key in keys {
        if !read.contains(key) && writes[key].iter().any(|&i| acked_at(i).is_some()) {
            violations.push(DurabilityViolation::Unread { key: key.clone() });
        }
    }

    if violations.is_empty() {
        Ok(())
    } else {
        Err(violations)
    }
}

#[cfg(test)]
mod tests {
    use super::super::register::tests::{read, write};
    use super::*;

    #[test]
    fn lost_acknowledged_write_fails() {
        // 2 was acknowledged after 1, but the key reads 1 after the quiet phase.
        let history = [write("x", 1, 0, Some(1)), write("x", 2, 2, Some(3))];
        assert_eq!(check_durability(&history, &[("x", Some(2))]), Ok(()));
        assert_eq!(
            check_durability(&history, &[("x", Some(1))]),
            Err(vec![DurabilityViolation::Lost {
                key: "x",
                writes: vec![1],
                final_write: Some(0),
            }])
        );
        assert_eq!(
            check_durability(&history, &[("x", None)]),
            Err(vec![DurabilityViolation::Lost {
                key: "x",
                writes: vec![0, 1],
                final_write: None,
            }])
        );
    }

    #[test]
    fn concurrent_acknowledged_writes_may_end_either_way() {
        // Neither write was acknowledged before the other was invoked, and equal positions are
        // concurrent.
        let history = [write("x", 1, 0, Some(10)), write("x", 2, 5, Some(15))];
        assert_eq!(check_durability(&history, &[("x", Some(1))]), Ok(()));
        assert_eq!(check_durability(&history, &[("x", Some(2))]), Ok(()));
        let touching = [write("x", 1, 0, Some(5)), write("x", 2, 5, Some(15))];
        assert_eq!(check_durability(&touching, &[("x", Some(1))]), Ok(()));
    }

    #[test]
    fn indeterminate_write_may_or_may_not_take_effect() {
        let history = [write("x", 1, 0, Some(1)), write("x", 2, 2, None)];
        assert_eq!(check_durability(&history, &[("x", Some(1))]), Ok(()));
        assert_eq!(check_durability(&history, &[("x", Some(2))]), Ok(()));

        // It may also be the last write to take effect, after a later acknowledged one.
        let mut later = history.to_vec();
        later.push(write("x", 3, 4, Some(5)));
        assert_eq!(check_durability(&later, &[("x", Some(2))]), Ok(()));
        assert!(check_durability(&later, &[("x", Some(1))]).is_err());
    }

    #[test]
    fn unknown_and_unread_values_fail() {
        let history = [
            write("x", 1, 0, Some(1)),
            write("y", 1, 0, Some(1)),
            write("z", 1, 0, None),
        ];
        assert_eq!(
            check_durability(&history, &[("x", Some(7)), ("w", Some(1))]),
            Err(vec![
                DurabilityViolation::Unknown { key: "x" },
                DurabilityViolation::Unknown { key: "w" },
                DurabilityViolation::Unread { key: "y" },
            ])
        );
    }

    #[test]
    fn reads_are_ignored() {
        let history = [write("x", 1, 0, Some(1)), read("x", Some(9), 5, Some(6))];
        assert_eq!(check_durability(&history, &[("x", Some(1))]), Ok(()));
    }
}
