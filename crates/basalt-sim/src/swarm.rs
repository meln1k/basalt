//! Swarm testing: each seed enables a random subset of fault types and knob randomizations, so
//! rare combinations get explored.

use basalt_env::{Knobs, Rng};

/// A type of fault a fault workload injects. A run's [`Swarm`] enables some of them.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum Fault {
    /// Crashing and wiping machines, and rebooting them.
    Attrition,
    PowerCut,
    Clog,
    Partition,
    /// Clock drift and wall-clock jumps.
    ClockSkew,
    DiskFull,
    DiskErrors,
}

impl Fault {
    /// Every fault type, in the order a swarm draws them.
    pub const ALL: [Fault; 7] = [
        Fault::Attrition,
        Fault::PowerCut,
        Fault::Clog,
        Fault::Partition,
        Fault::ClockSkew,
        Fault::DiskFull,
        Fault::DiskErrors,
    ];
}

/// How likely each fault type, and each knob's randomization, is to be enabled in a run.
const ENABLE_PROBABILITY: f64 = 0.5;

/// A run's swarm config: which fault types its fault workloads may inject, and which knobs
/// were randomized. Drawn once per seed from the `faults` stream, before the test is built.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Swarm {
    faults: Vec<Fault>,
    knobs: Vec<&'static str>,
}

impl Swarm {
    /// Draws the faults to enable, then which knobs to randomize, setting each picked knob in
    /// `knobs` to a value within its declared range.
    pub(crate) fn draw(rng: &Rng, knobs: &mut Knobs) -> Swarm {
        let faults = Fault::ALL
            .into_iter()
            .filter(|_| rng.bool(ENABLE_PROBABILITY))
            .collect();
        let randomized = knobs.randomize(rng, |_| rng.bool(ENABLE_PROBABILITY));
        Swarm {
            faults,
            knobs: randomized,
        }
    }

    /// Whether fault workloads may inject `fault` in this run.
    pub fn enabled(&self, fault: Fault) -> bool {
        self.faults.contains(&fault)
    }

    /// The enabled fault types, in [`Fault::ALL`] order.
    pub fn faults(&self) -> &[Fault] {
        &self.faults
    }

    /// The knobs randomized in this run; the rest keep their defaults.
    pub fn randomized_knobs(&self) -> &[&'static str] {
        &self.knobs
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_lists_every_fault_once() {
        // Fails to compile when a variant is added, as a reminder to add it to `ALL`.
        let index = |fault| match fault {
            Fault::Attrition => 0,
            Fault::PowerCut => 1,
            Fault::Clog => 2,
            Fault::Partition => 3,
            Fault::ClockSkew => 4,
            Fault::DiskFull => 5,
            Fault::DiskErrors => 6,
        };
        let indices: Vec<usize> = Fault::ALL.into_iter().map(index).collect();
        assert_eq!(indices, (0..Fault::ALL.len()).collect::<Vec<_>>());
    }
}
