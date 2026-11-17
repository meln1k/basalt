//! The milestone 1 system under test: an ABD multi-writer register on 3 or 5 replicas.
//!
//! Each key is its own register. A [`Client`] writes by reading the highest [`Tag`] from a
//! majority of replicas, then storing the value under `(ts + 1, writer)` on a majority. It reads
//! by taking the value with the highest tag from a majority, writing it back to a majority, and
//! returning it. A replica ([`run_replica`]) persists each value to its disk and syncs before it
//! acknowledges, and recovers what it stored when it reboots.
//!
//! Clients talk to replicas over [`Stream`](basalt_env::Stream) with length-prefixed messages,
//! and reconnect and retry until a majority answers, so an operation either completes or never
//! returns; callers bound it with [`Ctx::timeout`](basalt_env::Ctx::timeout).
//!
//! [`Bugs`] plants the two bugs the simulator must find. Both are off by default.
//!
//! The library uses only `basalt-env`, so the same code runs in simulation and on `basalt-rt`.

mod client;
mod replica;
mod wire;

pub use client::Client;
pub use replica::{ReplicaConfig, run_replica};

/// The port replicas listen on unless configured otherwise.
pub const DEFAULT_PORT: u16 = 4500;

/// Orders the values written to a key: the highest tag wins. Writers have distinct ids, so two
/// writes never share a tag.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Tag {
    pub ts: u64,
    pub writer: u64,
}

/// Bugs planted for the simulator to find. Both are off by default.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Bugs {
    /// Clients return a read without writing its value back to a majority, so a later read can
    /// see an older value (a new-old inversion).
    pub skip_writeback: bool,
    /// Replicas acknowledge a write before syncing it, so a power cut on a majority can lose an
    /// acknowledged write.
    pub ack_before_sync: bool,
}
