//! [`History`]: what clients asked for during a run, when, and how each operation ended.
//!
//! Every invocation and every response is an event with its own position, counted from zero in
//! the order the events were recorded. The checkers order operations by these positions, not by
//! virtual time, because many events share one virtual instant (see the
//! [checker docs](crate::checker)).

use std::cell::RefCell;
use std::fmt;
use std::rc::Rc;

use basalt_env::{Ctx, Instant};

use crate::checker;

/// When an event happened: its position among all events of its history, and the virtual time
/// on the recording process's clock.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Event {
    pub position: u64,
    pub time: Instant,
}

/// How an operation ended.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome<O> {
    /// The operation returned `output`.
    Ok { output: O, at: Event },
    /// The operation failed and is known to have had no effect.
    Failed { at: Event },
    /// No response arrived: the operation may have taken effect at any time after its call, or
    /// never. This is what an operation records when its [`Call`] is dropped unfinished, for
    /// example because the chaos phase ended or its client was killed.
    Indeterminate,
}

/// One operation in a [`History`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry<I, O> {
    /// Which client invoked it.
    pub client: u64,
    pub input: I,
    pub call: Event,
    pub outcome: Outcome<O>,
}

/// One line for a failure report: who invoked what, and when it was called and returned, by
/// event position and virtual time.
impl<I: fmt::Debug, O: fmt::Debug> fmt::Display for Entry<I, O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Entry {
            client,
            input,
            call,
            outcome,
        } = self;
        write!(
            f,
            "client {client}: {input:?}, called #{} at {:?}",
            call.position, call.time
        )?;
        match outcome {
            Outcome::Ok { output, at } => {
                write!(f, ", returned {output:?} #{} at {:?}", at.position, at.time)
            }
            Outcome::Failed { at } => write!(f, ", failed #{} at {:?}", at.position, at.time),
            Outcome::Indeterminate => write!(f, ", no response"),
        }
    }
}

/// A recorder of operations with inputs of type `I` and outputs of type `O`. Clones share one
/// history, so each client task can hold its own.
pub struct History<I, O> {
    inner: Rc<RefCell<Inner<I, O>>>,
}

struct Inner<I, O> {
    entries: Vec<Entry<I, O>>,
    next_position: u64,
}

impl<I, O> Inner<I, O> {
    fn event(&mut self, ctx: &Ctx) -> Event {
        let position = self.next_position;
        self.next_position += 1;
        Event {
            position,
            time: ctx.now(),
        }
    }
}

impl<I, O> History<I, O> {
    pub fn new() -> Self {
        History {
            inner: Rc::new(RefCell::new(Inner {
                entries: Vec::new(),
                next_position: 0,
            })),
        }
    }

    /// Records that `client` invoked `input` now, on `ctx`'s clock. Finish the returned
    /// [`Call`] when the response arrives; dropping it leaves the operation indeterminate.
    pub fn invoke(&self, ctx: &Ctx, client: u64, input: I) -> Call<I, O> {
        let mut inner = self.inner.borrow_mut();
        let call = inner.event(ctx);
        inner.entries.push(Entry {
            client,
            input,
            call,
            outcome: Outcome::Indeterminate,
        });
        Call {
            history: self.clone(),
            index: inner.entries.len() - 1,
        }
    }

    /// How many operations were invoked.
    pub fn len(&self) -> usize {
        self.inner.borrow().entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn finish(&self, index: usize, ctx: &Ctx, outcome: impl FnOnce(Event) -> Outcome<O>) {
        let mut inner = self.inner.borrow_mut();
        let at = inner.event(ctx);
        inner.entries[index].outcome = outcome(at);
    }
}

impl<I: Clone, O: Clone> History<I, O> {
    /// Every operation so far, in the order they were invoked.
    pub fn entries(&self) -> Vec<Entry<I, O>> {
        self.inner.borrow().entries.clone()
    }

    /// The operations for the [checkers](crate::checker), timed by event positions, and the
    /// index in [`entries`](History::entries) of each, to map a checker's report back to the
    /// entries. Failed operations are left out, since they had no effect.
    pub fn to_checker(&self) -> (Vec<checker::Operation<I, O>>, Vec<usize>) {
        let inner = self.inner.borrow();
        inner
            .entries
            .iter()
            .enumerate()
            .filter_map(|(index, entry)| {
                let outcome = match &entry.outcome {
                    Outcome::Ok { output, at } => checker::Outcome::Ok {
                        output: output.clone(),
                        at: at.position,
                    },
                    Outcome::Indeterminate => checker::Outcome::Indeterminate,
                    Outcome::Failed { .. } => return None,
                };
                let operation = checker::Operation {
                    input: entry.input.clone(),
                    call: entry.call.position,
                    outcome,
                };
                Some((operation, index))
            })
            .unzip()
    }
}

impl<I, O> Clone for History<I, O> {
    fn clone(&self) -> Self {
        History {
            inner: self.inner.clone(),
        }
    }
}

impl<I, O> Default for History<I, O> {
    fn default() -> Self {
        Self::new()
    }
}

impl<I, O> fmt::Debug for History<I, O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("History")
            .field("len", &self.len())
            .finish_non_exhaustive()
    }
}

/// An operation waiting for its response. Finish it with [`ok`](Call::ok) or
/// [`fail`](Call::fail); dropping it leaves the operation [indeterminate](Outcome::Indeterminate).
#[must_use = "dropping a call leaves its operation indeterminate"]
pub struct Call<I, O> {
    history: History<I, O>,
    index: usize,
}

impl<I, O> Call<I, O> {
    /// Records that the operation returned `output` now, on `ctx`'s clock.
    pub fn ok(self, ctx: &Ctx, output: O) {
        self.history
            .finish(self.index, ctx, |at| Outcome::Ok { output, at });
    }

    /// Records that the operation failed now, on `ctx`'s clock, and is known to have had no
    /// effect. If it might have taken effect, drop the call instead.
    pub fn fail(self, ctx: &Ctx) {
        self.history
            .finish(self.index, ctx, |at| Outcome::Failed { at });
    }
}

impl<I, O> fmt::Debug for Call<I, O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Call").field("index", &self.index).finish()
    }
}

#[cfg(test)]
mod tests {
    use basalt_sim::Sim;

    use super::*;
    use crate::checker::{RegisterOp, RegisterOutput, check_register_linearizability};

    type Op = RegisterOp<&'static str, u32>;
    type Out = RegisterOutput<u32>;

    fn ctx(sim: &Sim) -> Ctx {
        let ctrl = sim.control();
        let machine = ctrl.add_machine("client", 0);
        ctrl.add_process(machine, "client", |_| Box::pin(async {}))
    }

    #[test]
    fn events_get_positions_in_recording_order() {
        let sim = Sim::new(0);
        let ctx = ctx(&sim);
        let history: History<Op, Out> = History::new();
        let write = history.invoke(&ctx, 1, RegisterOp::Write("k", 1));
        let read = history.invoke(&ctx, 2, RegisterOp::Read("k"));
        let lost = history.invoke(&ctx, 3, RegisterOp::Write("k", 2));
        read.ok(&ctx, RegisterOutput::Read(None));
        write.fail(&ctx);
        drop(lost);

        let entries = history.entries();
        let calls: Vec<_> = entries.iter().map(|e| e.call.position).collect();
        assert_eq!(calls, [0, 1, 2]);
        assert!(matches!(entries[0].outcome, Outcome::Failed { at } if at.position == 4));
        assert!(matches!(&entries[1].outcome, Outcome::Ok { at, .. } if at.position == 3));
        assert_eq!(entries[2].outcome, Outcome::Indeterminate);
        assert_eq!(entries[2].client, 3);

        // The failed write had no effect, so the checkers don't see it.
        let (ops, indices) = history.to_checker();
        assert_eq!(indices, [1, 2]);
        let inputs: Vec<_> = ops.iter().map(|op| op.input.clone()).collect();
        assert_eq!(inputs, [RegisterOp::Read("k"), RegisterOp::Write("k", 2)]);
        assert_eq!(
            ops[0].outcome,
            checker::Outcome::Ok {
                output: RegisterOutput::Read(None),
                at: 3
            }
        );
    }

    /// Every event happens at virtual time zero, but the write returns before the read is
    /// invoked, so a read that misses it is a violation the checker must see.
    #[test]
    fn the_checker_orders_events_at_one_instant_by_position() {
        let sim = Sim::new(0);
        let ctx = ctx(&sim);
        let history: History<Op, Out> = History::new();
        history
            .invoke(&ctx, 1, RegisterOp::Write("k", 1))
            .ok(&ctx, RegisterOutput::Written);
        history
            .invoke(&ctx, 2, RegisterOp::Read("k"))
            .ok(&ctx, RegisterOutput::Read(None));
        assert!(
            history
                .entries()
                .iter()
                .all(|e| e.call.time == Instant::ZERO)
        );
        assert!(check_register_linearizability(&history.to_checker().0).is_err());
    }
}
