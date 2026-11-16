//! The trace: every `trace!` event of a run, and its hash for the unseed.

use std::cell::{Ref, RefCell};
use std::fmt;
use std::hash::{DefaultHasher, Hash};
use std::rc::Rc;

use basalt_env::world::Event;
use basalt_env::{Instant, Severity};

use crate::Seconds;

/// One `trace!` event, with the virtual time and the process whose task was being polled.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TraceRecord {
    pub time: Instant,
    /// The label of the process whose task recorded the event, or `None` outside any task.
    pub process: Option<Rc<str>>,
    pub severity: Severity,
    pub name: &'static str,
    /// Each field's name and `Debug` value, in the order they were written.
    pub fields: Vec<(&'static str, String)>,
}

impl fmt::Display for TraceRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} {} {:?} {}",
            Seconds(self.time),
            self.process.as_deref().unwrap_or("-"),
            self.severity,
            self.name
        )?;
        for (key, value) in &self.fields {
            write!(f, " {key}={value}")?;
        }
        Ok(())
    }
}

pub(crate) struct TraceLog {
    records: RefCell<Vec<TraceRecord>>,
    /// Every record so far. `DefaultHasher::new()` has fixed keys, unlike `RandomState`. Its
    /// algorithm may change between Rust releases, which is fine: unseeds are only compared
    /// between runs of one build.
    hash: RefCell<DefaultHasher>,
    /// The first `Error` event, which fails the run.
    first_error: RefCell<Option<TraceRecord>>,
}

impl TraceLog {
    pub(crate) fn new() -> TraceLog {
        TraceLog {
            records: RefCell::default(),
            hash: RefCell::new(DefaultHasher::new()),
            first_error: RefCell::default(),
        }
    }

    pub(crate) fn record(&self, event: &Event<'_>, time: Instant, process: Option<Rc<str>>) {
        // Formatting runs arbitrary `Debug` code, so do it before borrowing anything.
        let fields = event
            .fields
            .iter()
            .map(|(key, value)| (*key, format!("{value:?}")))
            .collect();
        let record = TraceRecord {
            time,
            process,
            severity: event.severity,
            name: event.name,
            fields,
        };
        record.hash(&mut *self.hash.borrow_mut());
        if record.severity == Severity::Error {
            self.first_error
                .borrow_mut()
                .get_or_insert_with(|| record.clone());
        }
        self.records.borrow_mut().push(record);
    }

    pub(crate) fn records(&self) -> Ref<'_, [TraceRecord]> {
        Ref::map(self.records.borrow(), Vec::as_slice)
    }

    pub(crate) fn first_error(&self) -> Option<TraceRecord> {
        self.first_error.borrow().clone()
    }

    /// The hash of every record so far, to extend into the unseed.
    pub(crate) fn hasher(&self) -> DefaultHasher {
        self.hash.borrow().clone()
    }
}
