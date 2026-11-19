//! Network faults as a timeline. Each clog and partition is a span of virtual time that is only
//! ever added, and lifting faults ends the spans still running at the current time. What
//! happened up to now never changes, so working out late when something arrived gives the same
//! answer as working it out on time.

use std::collections::BTreeMap;
use std::ops::Range;

use basalt_env::{Duration, Instant};

use crate::machine::MachineId;

/// A span of virtual time, from `start` until just before `end`.
#[derive(Clone, Copy)]
struct Span {
    start: Instant,
    end: Instant,
}

impl Span {
    fn covers(&self, at: Instant) -> bool {
        self.start <= at && at < self.end
    }
}

#[derive(Default)]
pub(super) struct Faults {
    pair_clogs: BTreeMap<(MachineId, MachineId), Vec<Span>>,
    send_clogs: BTreeMap<MachineId, Vec<Span>>,
    receive_clogs: BTreeMap<MachineId, Vec<Span>>,
    /// One-way partitions. A running one ends at `Instant::MAX`.
    partitions: BTreeMap<(MachineId, MachineId), Vec<Span>>,
    /// How likely each write is to reset its connection.
    pub(super) reset_probability: f64,
}

fn add(spans: &mut Vec<Span>, start: Instant, d: Duration) {
    let end = start.checked_add(d).unwrap_or(Instant::MAX);
    spans.push(Span { start, end });
}

impl Faults {
    pub(super) fn clog_pair(&mut self, from: MachineId, to: MachineId, now: Instant, d: Duration) {
        add(self.pair_clogs.entry((from, to)).or_default(), now, d);
    }

    pub(super) fn clog_sends(&mut self, machine: MachineId, now: Instant, d: Duration) {
        add(self.send_clogs.entry(machine).or_default(), now, d);
    }

    pub(super) fn clog_receives(&mut self, machine: MachineId, now: Instant, d: Duration) {
        add(self.receive_clogs.entry(machine).or_default(), now, d);
    }

    pub(super) fn partition(&mut self, from: MachineId, to: MachineId, now: Instant) {
        let spans = self.partitions.entry((from, to)).or_default();
        if spans.last().is_none_or(|s| s.end <= now) {
            spans.push(Span {
                start: now,
                end: Instant::MAX,
            });
        }
    }

    fn all_spans(&mut self) -> impl Iterator<Item = &mut Vec<Span>> {
        let clogs = self
            .pair_clogs
            .values_mut()
            .chain(self.send_clogs.values_mut());
        let clogs = clogs.chain(self.receive_clogs.values_mut());
        clogs.chain(self.partitions.values_mut())
    }

    /// Ends every clog and partition at `now`, and stops random resets. Every span started by
    /// `now`, so this changes only what happens from now on.
    pub(super) fn heal(&mut self, now: Instant) {
        for span in self.all_spans().flatten() {
            span.end = span.end.min(now);
        }
        self.reset_probability = 0.0;
    }

    /// Forgets the spans that ended by `horizon`: nothing sent at or after it can meet them.
    pub(super) fn forget_before(&mut self, horizon: Instant) {
        for spans in self.all_spans() {
            spans.retain(|s| s.end > horizon);
        }
        self.pair_clogs.retain(|_, spans| !spans.is_empty());
        self.send_clogs.retain(|_, spans| !spans.is_empty());
        self.receive_clogs.retain(|_, spans| !spans.is_empty());
        self.partitions.retain(|_, spans| !spans.is_empty());
    }

    /// Whether no clog or partition is left, as in most of a run.
    fn is_clear(&self) -> bool {
        self.pair_clogs.is_empty()
            && self.send_clogs.is_empty()
            && self.receive_clogs.is_empty()
            && self.partitions.is_empty()
    }

    /// When something sent from `from` to `to`, due at `due` without faults, arrives: at the
    /// first time from `due` on that no clog or partition covers, or `Instant::MAX` behind a
    /// partition that never ends. Later dues never arrive earlier.
    pub(super) fn arrival(&self, from: MachineId, to: MachineId, due: Instant) -> Instant {
        if self.is_clear() {
            return due;
        }
        let spans = [
            self.pair_clogs.get(&(from, to)),
            self.send_clogs.get(&from),
            self.receive_clogs.get(&to),
            self.partitions.get(&(from, to)),
        ];
        let mut at = due;
        while let Some(span) = spans.into_iter().flatten().flatten().find(|s| s.covers(at)) {
            at = span.end;
        }
        at
    }

    /// When a sender gives up on something unacknowledged from `from` to `to` during `waiting`:
    /// the first time a partition has held it back for `stall` without a break. `None` if no
    /// partition does.
    pub(super) fn stall_reset(
        &self,
        from: MachineId,
        to: MachineId,
        waiting: Range<Instant>,
        stall: Duration,
    ) -> Option<Instant> {
        let partitions = self.partitions.get(&(from, to))?;
        partitions
            .iter()
            .filter_map(|p| {
                let held = p.start.max(waiting.start);
                let gives_up = held.checked_add(stall).unwrap_or(Instant::MAX);
                (gives_up <= p.end.min(waiting.end) && gives_up < Instant::MAX).then_some(gives_up)
            })
            .min()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn secs(s: u64) -> Instant {
        Instant::from_nanos(s * 1_000_000_000)
    }

    /// A clog and a partition that overlap: arrivals stay in order, and the stall clock runs
    /// while the partition covers the oldest thing in transit, wherever a clog also does.
    #[test]
    fn arrival_is_monotone_and_the_stall_counts_partition_time() {
        let (a, b) = (MachineId::for_tests(0), MachineId::for_tests(1));
        let mut f = Faults::default();
        f.clog_pair(a, b, secs(0), Duration::from_secs(25));
        f.partition(a, b, secs(20));
        f.heal(secs(52));
        assert_eq!(f.arrival(a, b, secs(0)), secs(52));
        assert_eq!(f.arrival(a, b, secs(21)), secs(52));
        let stall = Duration::from_secs(30);
        assert_eq!(
            f.stall_reset(a, b, secs(0)..secs(52), stall),
            Some(secs(50))
        );
        assert_eq!(f.stall_reset(a, b, secs(25)..secs(52), stall), None);
        // Spans that ended by the horizon no longer matter to anything sent from it on.
        f.forget_before(secs(52));
        assert_eq!(f.arrival(a, b, secs(52)), secs(52));
        assert!(f.partitions.is_empty() && f.pair_clogs.is_empty());
    }
}
