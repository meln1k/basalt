//! The sweep's coverage report: the `buggify!` sites that no seed fired and the probes that no
//! seed hit, out of every site linked into the binary ([`BUGGIFY_SITES`] and [`PROBE_SITES`]).
//! It shows when a test stops exercising what it claims to.
//!
//! A sweep's children are the same binary as the parent, so they send a run's coverage as
//! indices into the registry of sites, whose order is fixed for a build.

use std::collections::BTreeSet;
use std::fmt;
use std::sync::LazyLock;

use basalt_env::world::{BUGGIFY_SITES, Location, PROBE_SITES};
use basalt_sim::Coverage;

/// Every `buggify!` site and probe in the binary, each list sorted and without repeats. Sites
/// that share a location (see [`Location`]) are one entry.
struct Registry {
    buggify: Vec<Location>,
    probes: Vec<Location>,
    /// The name of each probe in `probes`, by index. Of two probes at one location, the first
    /// in link order names it.
    probe_names: Vec<&'static str>,
}

static REGISTRY: LazyLock<Registry> = LazyLock::new(|| {
    let mut buggify: Vec<Location> = BUGGIFY_SITES.iter().map(|s| s.location).collect();
    buggify.sort_unstable();
    buggify.dedup();
    let mut probes: Vec<(Location, &'static str)> =
        PROBE_SITES.iter().map(|s| (s.location, s.name)).collect();
    probes.sort_by_key(|&(location, _)| location);
    probes.dedup_by_key(|&mut (location, _)| location);
    let (probes, probe_names) = probes.into_iter().unzip();
    Registry {
        buggify,
        probes,
        probe_names,
    }
});

/// `coverage` as one word for the sweep protocol: the indices of the `buggify!` sites reached,
/// of those fired, and of the probes hit, as three comma-separated lists joined by `;`. Panics
/// on a site missing from the registry, which would mean `linkme` failed to register it.
pub(crate) fn encode(coverage: &Coverage) -> String {
    let registry = &*REGISTRY;
    let list = |all: &[Location], sites: &mut dyn Iterator<Item = &Location>| {
        sites
            .map(|site| {
                all.binary_search(site)
                    .unwrap_or_else(|_| panic!("{site} isn't registered"))
                    .to_string()
            })
            .collect::<Vec<_>>()
            .join(",")
    };
    let fired = coverage
        .buggify
        .iter()
        .filter(|(_, hits)| hits.fired > 0)
        .map(|(site, _)| site);
    [
        list(&registry.buggify, &mut coverage.buggify.keys()),
        list(&registry.buggify, &mut { fired }),
        list(&registry.probes, &mut coverage.probes.keys()),
    ]
    .join(";")
}

/// Reads a word from [`encode`]. Returns `None` if it's garbled.
pub(crate) fn decode(word: &str) -> Option<SweepCoverage> {
    let registry = &*REGISTRY;
    let lists: Vec<&str> = word.split(';').collect();
    let [reached, fired, probes] = lists[..] else {
        return None;
    };
    let sites = |list: &str, all: &[Location]| -> Option<BTreeSet<Location>> {
        list.split(',')
            .filter(|i| !i.is_empty())
            .map(|i| all.get(i.parse::<usize>().ok()?).copied())
            .collect()
    };
    let coverage = SweepCoverage {
        seeds: 1,
        reached: sites(reached, &registry.buggify)?,
        fired: sites(fired, &registry.buggify)?,
        probes: sites(probes, &registry.probes)?,
    };
    coverage
        .fired
        .is_subset(&coverage.reached)
        .then_some(coverage)
}

/// What the seeds of a sweep reached between them. Its `Display` is the report: how many sites
/// fired and probes were hit, then each one that wasn't.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SweepCoverage {
    /// How many seeds reported their coverage. A seed whose child aborted reports none.
    pub seeds: u64,
    /// `buggify!` sites some seed reached.
    pub reached: BTreeSet<Location>,
    /// `buggify!` sites some seed fired.
    pub fired: BTreeSet<Location>,
    /// Probes some seed hit.
    pub probes: BTreeSet<Location>,
}

impl SweepCoverage {
    pub(crate) fn merge(&mut self, other: SweepCoverage) {
        self.seeds += other.seeds;
        self.reached.extend(other.reached);
        self.fired.extend(other.fired);
        self.probes.extend(other.probes);
    }
}

impl fmt::Display for SweepCoverage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let registry = &*REGISTRY;
        let unfired: Vec<_> = registry
            .buggify
            .iter()
            .filter(|s| !self.fired.contains(s))
            .collect();
        let unhit: Vec<_> = registry
            .probes
            .iter()
            .zip(&registry.probe_names)
            .filter(|(s, _)| !self.probes.contains(s))
            .collect();
        write!(
            f,
            "coverage over {} seeds: {} of {} buggify! sites fired, {} of {} probes hit",
            self.seeds,
            registry.buggify.len() - unfired.len(),
            registry.buggify.len(),
            registry.probes.len() - unhit.len(),
            registry.probes.len(),
        )?;
        for site in unfired {
            let reached = if self.reached.contains(site) {
                "reached, never fired"
            } else {
                "never reached"
            };
            write!(f, "\nbuggify! site never fired: {site} ({reached})")?;
        }
        for (site, name) in unhit {
            write!(f, "\nprobe never hit: {name} at {site}")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use basalt_sim::SiteHits;

    use super::*;

    #[test]
    fn coverage_round_trips_through_the_protocol_and_the_report_lists_what_was_missed() {
        let registry = &*REGISTRY;
        // This crate's own sites, at least the self-test's.
        assert!(registry.buggify.len() >= 2 && registry.probes.len() >= 2);
        let (fired, reached) = (registry.buggify[0], registry.buggify[1]);
        let probe = registry.probes[0];
        let mut run = Coverage::default();
        let hits = |reached, fired| SiteHits { reached, fired };
        run.buggify.insert(fired, hits(3, 1));
        run.buggify.insert(reached, hits(2, 0));
        run.probes.insert(probe, 5);

        let word = encode(&run);
        assert!(!word.contains(' '), "{word:?}");
        let mut sweep = SweepCoverage::default();
        sweep.merge(decode(&word).unwrap());
        sweep.merge(decode(";;").unwrap());
        assert_eq!(sweep.seeds, 2);
        assert_eq!(sweep.fired, BTreeSet::from([fired]));
        assert_eq!(sweep.reached, BTreeSet::from([fired, reached]));
        assert_eq!(sweep.probes, BTreeSet::from([probe]));

        let report = sweep.to_string();
        assert!(
            report.contains(&format!(
                "buggify! site never fired: {reached} (reached, never fired)"
            )),
            "{report}"
        );
        assert!(!report.contains(&format!("{fired} (")), "{report}");
        let probe_line = format!("at {probe}");
        assert!(
            !report.lines().any(|l| l.ends_with(&probe_line)),
            "{report}"
        );
    }

    #[test]
    fn garbled_coverage_is_rejected() {
        assert!(decode("1;2").is_none());
        assert!(decode("99999;;").is_none());
        // Fired but never reached.
        assert!(decode(";0;").is_none());
    }
}
