//! The production ambient world: `buggify!` is off, probes are counters, traces go to a log, and
//! knobs are the defaults plus config.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::io::Write;
use std::rc::Rc;
use std::time::{SystemTime, UNIX_EPOCH};

use basalt_env::world::{BuggifySite, Event, ProbeSite, World};
use basalt_env::{Knobs, Severity};

pub(crate) struct ProductionWorld {
    knobs: Rc<Knobs>,
    log: RefCell<Box<dyn Write>>,
    min_severity: Severity,
    probes: RefCell<BTreeMap<&'static str, u64>>,
}

impl ProductionWorld {
    pub(crate) fn new(knobs: Knobs, log: Box<dyn Write>, min_severity: Severity) -> Self {
        ProductionWorld {
            knobs: Rc::new(knobs),
            log: RefCell::new(log),
            min_severity,
            probes: RefCell::default(),
        }
    }

    pub(crate) fn probes(&self) -> Vec<(&'static str, u64)> {
        self.probes
            .borrow()
            .iter()
            .map(|(&name, &hits)| (name, hits))
            .collect()
    }
}

impl World for ProductionWorld {
    fn is_simulated(&self) -> bool {
        false
    }

    fn buggify(&self, _: &'static BuggifySite, _: f64) -> bool {
        false
    }

    fn probe(&self, site: &'static ProbeSite) {
        *self.probes.borrow_mut().entry(site.name).or_default() += 1;
    }

    fn trace(&self, event: &Event<'_>) {
        if event.severity < self.min_severity {
            return;
        }
        let since_epoch = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        // Formatted before the log is borrowed, since a field's `Debug` impl may trace too.
        let line = format!(
            "{}.{:06} {event:?}\n",
            since_epoch.as_secs(),
            since_epoch.subsec_micros()
        );
        if let Ok(mut log) = self.log.try_borrow_mut() {
            // One write per line, and losing a line is better than failing the server.
            let _ = log.write_all(line.as_bytes());
        }
    }

    fn knobs(&self) -> Rc<Knobs> {
        self.knobs.clone()
    }
}
