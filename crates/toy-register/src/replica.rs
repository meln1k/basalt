//! A replica: serves gets and puts over the network and stores each key in its own file.
//!
//! A put writes the key's record to `register/<key>.tmp`, syncs it, renames it over
//! `register/<key>`, and syncs the directory, so a power cut leaves either the old record or the
//! new one, never a torn mix. Only then does the replica acknowledge. On start it removes
//! leftover temporary files and loads every record, before it listens.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::future::poll_fn;
use std::io::{self, ErrorKind};
use std::rc::Rc;
use std::task::{Poll, Waker};

use basalt_env::{Ctx, Disk, Duration, OpenOptions, Priority, Stream, trace};

use crate::wire::{self, Request, Response};
use crate::{Bugs, DEFAULT_PORT, Tag};

const DIR: &str = "register";

/// With [`Bugs::ack_before_sync`], how many times recovery reads a record that fails its
/// checksum before giving it up as lost.
const LOAD_ATTEMPTS: u32 = 10;

/// How a replica runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReplicaConfig {
    pub port: u16,
    pub bugs: Bugs,
}

impl Default for ReplicaConfig {
    fn default() -> ReplicaConfig {
        ReplicaConfig {
            port: DEFAULT_PORT,
            bugs: Bugs::default(),
        }
    }
}

/// Runs a replica on `ctx`'s process, storing its data on the first of the process's disks:
/// recovers what it stored, then serves clients on `config.port` until the process is killed.
/// Returns early, with a `Warn` trace, only if the process has no disk or can't listen.
pub async fn run_replica(ctx: Ctx, config: ReplicaConfig) {
    let Some(&disk) = ctx.disks().first() else {
        trace!(Warn, "ReplicaNoDisk");
        return;
    };
    let replica = Rc::new(Replica {
        disk: ctx.disk(disk),
        bugs: config.bugs,
        slots: RefCell::default(),
        locks: KeyLocks::default(),
    });
    replica.recover(&ctx).await;
    let listener = match ctx.listen(config.port).await {
        Ok(listener) => listener,
        Err(e) => {
            trace!(Warn, "ReplicaListenFailed", port = config.port, error = e);
            return;
        }
    };
    trace!(Info, "ReplicaListening", port = config.port);
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let replica = replica.clone();
                ctx.spawn("replica-conn", Priority::Default, async move {
                    if let Err(e) = replica.serve(stream).await {
                        trace!(Info, "ReplicaConnClosed", error = e);
                    }
                })
                .detach();
            }
            Err(e) => trace!(Warn, "ReplicaAcceptFailed", error = e),
        }
    }
}

struct Replica {
    disk: Disk,
    bugs: Bugs,
    slots: RefCell<BTreeMap<u64, Slot>>,
    locks: KeyLocks,
}

/// What a replica knows about one key.
#[derive(Clone, Default)]
struct Slot {
    /// The highest tag received, and its value. A get answers with these.
    tag: Tag,
    value: Vec<u8>,
    /// The highest tag on disk, synced. A put of a tag up to this one is acknowledged.
    durable: Tag,
}

impl Replica {
    async fn recover(&self, ctx: &Ctx) {
        let names = loop {
            match self.disk.list(DIR).await {
                Ok(names) => break names,
                Err(e) => {
                    trace!(Warn, "ReplicaListFailed", error = e);
                    ctx.sleep(Duration::from_millis(1)).await;
                }
            }
        };
        for name in names {
            let path = format!("{DIR}/{name}");
            if name.ends_with(".tmp") {
                // An unacknowledged put. The `sync_dir` below makes its removal durable.
                if let Err(e) = self.disk.remove(&path).await {
                    trace!(Warn, "ReplicaRemoveTmpFailed", file = path, error = e);
                }
                continue;
            }
            let Ok(key) = name.parse::<u64>() else {
                trace!(Warn, "ReplicaStrayFile", file = path);
                continue;
            };
            if let Some((tag, value)) = self.load(ctx, &path).await {
                self.slots.borrow_mut().insert(
                    key,
                    Slot {
                        tag,
                        value,
                        durable: tag,
                    },
                );
            }
        }
        // A crash between a put's rename and its `sync_dir` leaves the new name in the page
        // cache, where a later power cut can still lose it. Make every name loaded above durable
        // before claiming it is, or the replica would acknowledge puts of those tags unsynced.
        while let Err(e) = self.disk.sync_dir(DIR).await {
            trace!(Warn, "ReplicaSyncDirFailed", error = e);
            ctx.sleep(Duration::from_millis(1)).await;
        }
        trace!(Info, "ReplicaRecovered", keys = self.slots.borrow().len());
    }

    /// Reads and decodes the record at `path`, retrying until it succeeds, since a replica that
    /// forgets an acknowledged value can break the register. A record under its final name was
    /// synced before the rename, so a failed checksum is a bit flip on read, which a retry gets
    /// past. Only with [`Bugs::ack_before_sync`] can a power cut damage the record itself; then
    /// one that keeps failing its checksum is given up as lost.
    async fn load(&self, ctx: &Ctx, path: &str) -> Option<(Tag, Vec<u8>)> {
        let mut damaged = 0;
        loop {
            match self.read_record(path).await {
                Ok(record) => return Some(record),
                Err(e) if e.kind() == ErrorKind::InvalidData => {
                    damaged += 1;
                    if self.bugs.ack_before_sync && damaged == LOAD_ATTEMPTS {
                        trace!(Warn, "ReplicaRecordDamaged", file = path, error = e);
                        return None;
                    }
                }
                Err(e) => trace!(Warn, "ReplicaLoadFailed", file = path, error = e),
            }
            ctx.sleep(Duration::from_millis(1)).await;
        }
    }

    async fn read_record(&self, path: &str) -> io::Result<(Tag, Vec<u8>)> {
        let file = self.disk.open(path, OpenOptions::new().read(true)).await?;
        let len = usize::try_from(file.len()).map_err(|_| ErrorKind::InvalidData)?;
        let (n, buf) = file.read_at(vec![0; len], 0).await;
        if n? != len {
            return Err(ErrorKind::UnexpectedEof.into());
        }
        wire::decode_record(&buf)
    }

    /// Answers requests on one connection, one at a time, until it closes or fails.
    async fn serve(&self, mut stream: Stream) -> io::Result<()> {
        loop {
            let (id, request) = wire::decode_request(&wire::read_frame(&mut stream).await?)?;
            match request {
                Request::Get { key } => {
                    let slot = self.slots.borrow().get(&key).cloned().unwrap_or_default();
                    let response = Response::Value {
                        tag: slot.tag,
                        value: slot.value,
                    };
                    wire::send(&mut stream, &wire::encode_response(id, &response)).await?;
                }
                Request::Put { key, tag, value } => {
                    self.put(&mut stream, id, key, tag, value).await?;
                }
            }
        }
    }

    /// Takes `value` under `tag` if it is newer than what the replica has, makes the key durable
    /// at `tag` or higher, and acknowledges.
    async fn put(
        &self,
        stream: &mut Stream,
        id: u64,
        key: u64,
        tag: Tag,
        value: Vec<u8>,
    ) -> io::Result<()> {
        {
            let mut slots = self.slots.borrow_mut();
            let slot = slots.entry(key).or_default();
            if tag > slot.tag {
                slot.tag = tag;
                slot.value = value;
            }
        }
        let ack = wire::encode_response(id, &Response::Ack);
        // One write per key at a time, so renames land in tag order. A put that waited here
        // often finds its tag already made durable by the one before it.
        let _lock = self.locks.lock(key).await;
        let slot = self.slots.borrow().get(&key).cloned().unwrap_or_default();
        if slot.durable >= tag {
            return wire::send(stream, &ack).await;
        }

        let path = format!("{DIR}/{key}");
        let tmp = format!("{path}.tmp");
        let opts = OpenOptions::new().write(true).create(true).truncate(true);
        let file = self.disk.open(&tmp, opts).await?;
        file.write_at(wire::encode_record(slot.tag, &slot.value), 0)
            .await
            .0?;
        // The planted bug renames and acknowledges while the record is only in the page cache,
        // which survives a crash but not a power cut. Otherwise only where the ack goes differs.
        let early_ack = self.bugs.ack_before_sync;
        if early_ack {
            self.disk.rename(&tmp, &path).await?;
            wire::send(stream, &ack).await?;
        }
        file.sync().await?;
        if !early_ack {
            self.disk.rename(&tmp, &path).await?;
        }
        self.disk.sync_dir(DIR).await?;
        self.mark_durable(key, slot.tag);
        trace!(
            Debug,
            "ReplicaStored",
            key = key,
            ts = slot.tag.ts,
            writer = slot.tag.writer
        );
        if early_ack {
            return Ok(());
        }
        wire::send(stream, &ack).await
    }

    fn mark_durable(&self, key: u64, tag: Tag) {
        let mut slots = self.slots.borrow_mut();
        let slot = slots.entry(key).or_default();
        slot.durable = slot.durable.max(tag);
    }
}

/// One async lock per key. Dropping a guard, including when its task is cancelled, releases
/// the key.
#[derive(Default)]
struct KeyLocks {
    /// The locked keys, each with the tasks waiting for it.
    held: RefCell<BTreeMap<u64, Vec<Waker>>>,
}

impl KeyLocks {
    async fn lock(&self, key: u64) -> KeyGuard<'_> {
        poll_fn(|cx| {
            let mut held = self.held.borrow_mut();
            match held.get_mut(&key) {
                Some(waiters) => {
                    // One waker per waiting task, however often it is polled.
                    if !waiters.iter().any(|w| w.will_wake(cx.waker())) {
                        waiters.push(cx.waker().clone());
                    }
                    Poll::Pending
                }
                None => {
                    held.insert(key, Vec::new());
                    Poll::Ready(())
                }
            }
        })
        .await;
        KeyGuard { locks: self, key }
    }
}

struct KeyGuard<'a> {
    locks: &'a KeyLocks,
    key: u64,
}

impl Drop for KeyGuard<'_> {
    fn drop(&mut self) {
        let waiters = self.locks.held.borrow_mut().remove(&self.key);
        for waker in waiters.into_iter().flatten() {
            waker.wake();
        }
    }
}
