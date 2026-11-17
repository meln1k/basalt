//! BUGGIFY in the simulated world: each site's switch for the run, the quiet-phase off switch,
//! and what the run reached, which is how often each `buggify!` site was reached and fired and
//! how often each `probe!` was hit.

use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;

use basalt_env::Rng;
use basalt_env::world::{BuggifySite, Location, ProbeSite};

/// How likely a `buggify!` site is to be switched on for a run, as in FoundationDB.
pub const BUGGIFY_SWITCH_PROBABILITY: f64 = 0.25;

/// How often one `buggify!` site was reached in a run, and how often it fired.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SiteHits {
    pub reached: u64,
    pub fired: u64,
}

/// What a run reached: every `buggify!` site and `probe!` reached at least once. Sites never
/// reached aren't here; the full lists are
/// [`BUGGIFY_SITES`](basalt_env::world::BUGGIFY_SITES) and
/// [`PROBE_SITES`](basalt_env::world::PROBE_SITES).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Coverage {
    pub buggify: BTreeMap<Location, SiteHits>,
    /// Hits per probe.
    pub probes: BTreeMap<Location, u64>,
}

/// BUGGIFY's state for one run.
#[derive(Default)]
pub(crate) struct Buggify {
    /// Each site's switch, drawn the first time the site is reached.
    switches: RefCell<BTreeMap<Location, bool>>,
    /// Set by [`lift`](Buggify::lift): from then on no `buggify!` fires.
    lifted: Cell<bool>,
    coverage: RefCell<Coverage>,
}

impl Buggify {
    /// Whether the `buggify!` at `site` fires now. The first time a site is reached, it's
    /// switched on for the rest of the run with probability [`BUGGIFY_SWITCH_PROBABILITY`];
    /// a switched-on site then fires with probability `fire`. Both draws come from `rng`, the
    /// `buggify` stream. Once lifted, it's always `false` and counts nothing.
    pub(crate) fn fires(&self, site: &BuggifySite, fire: f64, rng: &Rng) -> bool {
        if self.lifted.get() {
            return false;
        }
        let key = site.location;
        let on = *self
            .switches
            .borrow_mut()
            .entry(key)
            .or_insert_with(|| rng.bool(BUGGIFY_SWITCH_PROBABILITY));
        let fired = on && rng.bool(fire);
        let mut coverage = self.coverage.borrow_mut();
        let hits = coverage.buggify.entry(key).or_default();
        hits.reached += 1;
        hits.fired += u64::from(fired);
        fired
    }

    /// Turns BUGGIFY off for the rest of the run, for the quiet phase, as FoundationDB does
    /// once a test's chaos ends.
    pub(crate) fn lift(&self) {
        self.lifted.set(true);
    }

    pub(crate) fn probe(&self, site: &ProbeSite) {
        *self
            .coverage
            .borrow_mut()
            .probes
            .entry(site.location)
            .or_default() += 1;
    }

    pub(crate) fn coverage(&self) -> Coverage {
        self.coverage.borrow().clone()
    }
}
