//! Checkers that run over a recorded history after a test.
//!
//! - [`check_linearizability`]: a Porcupine-style search for a linearization, split per key and
//!   generic over a [`Model`].
//! - [`check_register_linearizability`]: the same for a store of read/write registers
//!   ([`Register`]), with indeterminate operations narrowed down first so failing searches stay
//!   fast.
//! - [`check_durability`]: no acknowledged write to a register is lost by the end of the quiet
//!   phase.
//!
//! They take their own input types, [`Operation`]s, and depend on nothing in the simulator. The
//! harness converts its recorded history into these types.
//!
//! # Times are positions in the order events were recorded
//!
//! [`Operation::call`] and [`Outcome::Ok::at`] only need to be ordered the way the events
//! happened. In simulation, many events share one virtual instant, so pass each event's position
//! in the history instead of its virtual time; otherwise an operation that returned before
//! another was invoked looks concurrent with it, and the checkers miss violations between them.
//! Equal values are treated as concurrent.
//!
//! # Outcomes
//!
//! An operation either returned ([`Outcome::Ok`]) or never got a response
//! ([`Outcome::Indeterminate`]), so it may or may not have taken effect, at any time after its
//! call. Leave out operations known to have failed without taking effect.

mod durability;
mod linearizability;
mod register;

pub use durability::{DurabilityViolation, check_durability};
pub use linearizability::{LinearizabilityViolation, Model, check_linearizability};
pub use register::{
    Register, RegisterOp, RegisterOperation, RegisterOutput, check_register_linearizability,
};

/// One operation from a history.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Operation<I, O> {
    /// What the client asked for.
    pub input: I,
    /// When the client invoked it, as a position in the history (see the [module docs](self)).
    pub call: u64,
    /// How it ended.
    pub outcome: Outcome<O>,
}

/// How an [`Operation`] ended.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome<O> {
    /// The operation returned `output` at position `at`, which is not before its call.
    Ok { output: O, at: u64 },
    /// No response arrived: the operation may have taken effect at any time after its call, or
    /// never.
    Indeterminate,
}
