//! Which ports of each machine are in use, and by what.

use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::io;
use std::ops::RangeInclusive;
use std::rc::{Rc, Weak};

use basalt_env::ProcessId;

use super::listener::Backlog;
use crate::machine::MachineId;

/// The ports `listen(0)` and outgoing connections take, as on Linux.
const EPHEMERAL_PORTS: RangeInclusive<u16> = 32768..=60999;

/// What holds a port.
pub(super) struct Port {
    pub(super) process: ProcessId,
    /// The listener's or connection's id, so a socket frees the port only while it holds it.
    pub(super) owner: u64,
    pub(super) holder: Holder,
}

pub(super) enum Holder {
    Listener(Weak<Backlog>),
    /// The local end of an outgoing connection.
    Connection,
}

impl Port {
    fn listener(&self) -> Option<Rc<Backlog>> {
        match &self.holder {
            Holder::Listener(backlog) => backlog.upgrade(),
            Holder::Connection => None,
        }
    }
}

#[derive(Default)]
pub(super) struct Ports {
    used: BTreeMap<(MachineId, u16), Port>,
    /// Where each machine's search for a free ephemeral port starts next.
    next_ephemeral: BTreeMap<MachineId, u16>,
}

impl Ports {
    /// Takes `port` on `machine`, or a free ephemeral port if it's 0, and returns the port.
    pub(super) fn claim(&mut self, machine: MachineId, port: u16, taken: Port) -> io::Result<u16> {
        let port = if port == 0 {
            self.pick_ephemeral(machine)?
        } else {
            port
        };
        match self.used.entry((machine, port)) {
            Entry::Occupied(_) => Err(io::Error::new(
                io::ErrorKind::AddrInUse,
                format!("port {port} is already in use on {machine:?}"),
            )),
            Entry::Vacant(e) => {
                e.insert(taken);
                Ok(port)
            }
        }
    }

    fn pick_ephemeral(&mut self, machine: MachineId) -> io::Result<u16> {
        let first = *EPHEMERAL_PORTS.start();
        let count = EPHEMERAL_PORTS.len() as u16;
        let cursor = self.next_ephemeral.entry(machine).or_insert(first);
        for _ in 0..count {
            let candidate = *cursor;
            *cursor = first + (candidate - first + 1) % count;
            if !self.used.contains_key(&(machine, candidate)) {
                return Ok(candidate);
            }
        }
        Err(io::Error::new(
            io::ErrorKind::AddrInUse,
            "every ephemeral port is in use",
        ))
    }

    pub(super) fn release(&mut self, machine: MachineId, port: u16, owner: u64) {
        if self
            .used
            .get(&(machine, port))
            .is_some_and(|p| p.owner == owner)
        {
            self.used.remove(&(machine, port));
        }
    }

    /// Frees every port of a dead process, and returns the listeners that held one.
    pub(super) fn release_dead(&mut self, alive: impl Fn(ProcessId) -> bool) -> Vec<Rc<Backlog>> {
        let mut listeners = Vec::new();
        self.used.retain(|_, port| {
            let keep = alive(port.process);
            if !keep {
                listeners.extend(port.listener());
            }
            keep
        });
        listeners
    }

    pub(super) fn listener(&self, machine: MachineId, port: u16) -> Option<Rc<Backlog>> {
        self.used.get(&(machine, port))?.listener()
    }
}
