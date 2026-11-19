//! Simulated disks: nothing is durable until it's synced, and a power cut loses or corrupts
//! what isn't.
//!
//! Each disk holds two views of its names ([`tree`]): the live one every process sees, and the
//! durable one that survives a power cut. A file's contents ([`inode`]) likewise have a live
//! image and a durable image, plus the pages written since its last `sync`, each tagged with the
//! corruption mode of the handle that wrote it. A `Crash` changes nothing here, since the page
//! cache outlives the process. A `PowerCut` collapses the disk to its durable view, applying
//! each pending page's mode. The path and directory rules are in `basalt_env::Disk`'s docs,
//! which both backends follow.
//!
//! The model is deliberately weaker than a real filesystem (design doc, "Disk"): code that is
//! correct here is correct on ext4 and XFS, not the other way around.
//!
//! Every operation is submitted when it's called, as the `DiskIo` contract requires: it takes its
//! turn on one of the disk's queues then, and a change it makes lands even if its future is
//! dropped. Queues run side by side, so operations issued together may complete in any order.
//!
//! Every random draw comes from the `disk` stream.

mod inode;
mod tree;

use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, ready};

use basalt_env::backend::{DiskIo, FileIo, LocalBoxFuture, check_dir, check_path};
use basalt_env::{DiskId, Duration, Instant, OpenOptions, ProcessId, Rng, Space, probe};

use crate::machine::Machines;
use crate::time::{Clock, Sleep};

use self::inode::{CorruptionMode, Inode};
use self::tree::{Entry, InodeRef, State};

/// Every disk's size.
const CAPACITY: u64 = 16 << 30;

/// How often an open picks full corruption over drop-only.
const FULL_CORRUPTION_PROBABILITY: f64 = 0.5;

/// The latency model: each operation occupies one of the disk's queues for one I/O (an IOPS
/// limit) plus its bytes at the disk's bandwidth, and a sync also waits for a cache flush. An
/// operation goes to a queue drawn at random and waits behind what's already on it; each takes
/// 50% to 150% of its nominal time, with a rare long stall.
const QUEUES: usize = 4;
const IO_TIME: Duration = Duration::from_micros(50);
const BYTES_PER_SECOND: u64 = 1 << 30;
const FLUSH_TIME: Duration = Duration::from_micros(500);
const STALL_PROBABILITY: f64 = 0.001;
const MIN_STALL: Duration = Duration::from_millis(5);
const MAX_STALL: Duration = Duration::from_millis(50);

/// How often a failed `sync` also loses the file's pending pages, as Linux does when writeback
/// fails: it marks them clean, so a later `sync` succeeds without writing them.
const SYNC_LOSES_PAGES_PROBABILITY: f64 = 0.5;

/// How often the disk finishes an operation of a process that died while the operation was in
/// flight, before the process died.
const LANDED_PROBABILITY: f64 = 0.5;

/// Every disk's contents and faults, by id. A disk's contents appear when it's first used and
/// go when its machine is wiped or its disks fail.
pub(crate) struct Disks {
    machines: Rc<Machines>,
    clock: Rc<Clock>,
    rng: Rc<Rng>,
    stores: RefCell<BTreeMap<DiskId, Rc<Store>>>,
}

impl Disks {
    pub(crate) fn new(machines: Rc<Machines>, clock: Rc<Clock>, rng: Rc<Rng>) -> Disks {
        Disks {
            machines,
            clock,
            rng,
            stores: RefCell::default(),
        }
    }

    /// `process`'s handle on `disk`.
    pub(crate) fn disk(self: &Rc<Self>, process: ProcessId, disk: DiskId) -> SimDisk {
        SimDisk {
            handle: Handle {
                disks: self.clone(),
                process,
            },
            id: disk,
        }
    }

    /// A current disk's contents, created empty on first use. Panics if no machine has `disk`.
    fn store(&self, disk: DiskId) -> Rc<Store> {
        if let Some(store) = self.stores.borrow().get(&disk) {
            return store.clone();
        }
        assert!(
            self.machines.has_disk(disk),
            "{disk:?} is not a disk of any machine"
        );
        let store = Rc::new(Store::new(disk));
        self.stores.borrow_mut().insert(disk, store.clone());
        store
    }

    /// Drops the contents of disks that are gone, after a wipe or a disk failure. Open handles
    /// keep their files' contents, but every operation through them fails, since their process
    /// is dead.
    pub(crate) fn remove(&self, disks: &[DiskId]) {
        let mut stores = self.stores.borrow_mut();
        for disk in disks {
            stores.remove(disk);
        }
    }

    /// Cuts the power to `disks`: each loses whatever isn't durable, applying each file's
    /// corruption mode to its pending pages, and forgets the operations queued on it.
    pub(crate) fn power_cut(&self, disks: &[DiskId]) {
        for &disk in disks {
            let store = self.stores.borrow().get(&disk).cloned();
            if let Some(store) = store {
                store.state.borrow_mut().power_cut(disk, &self.rng);
                store.queues.set([Instant::ZERO; QUEUES]);
            }
        }
    }

    /// Fills `disk`: from now on, anything that needs more space fails with `StorageFull`.
    pub(crate) fn fill(&self, disk: DiskId) {
        self.store(disk).faults.borrow_mut().full = true;
    }

    /// Makes reads, writes, and syncs on `disk` fail with `EIO` with probability `p`.
    pub(crate) fn set_io_errors(&self, disk: DiskId, p: f64) {
        check_probability(p);
        self.store(disk).faults.borrow_mut().io_errors = p;
    }

    /// Makes reads on `disk` return one flipped bit with probability `p`.
    pub(crate) fn set_bit_flips(&self, disk: DiskId, p: f64) {
        check_probability(p);
        self.store(disk).faults.borrow_mut().bit_flips = p;
    }

    /// Lifts every disk fault on every disk.
    pub(crate) fn restore(&self) {
        for store in self.stores.borrow().values() {
            *store.faults.borrow_mut() = Faults::default();
        }
    }

    /// Queues an operation of `bytes` bytes on `store` and returns when it finishes. A crash
    /// doesn't cancel what's queued, since the OS finishes it, so the next incarnation's first
    /// operation may wait behind the last one's.
    fn schedule(&self, store: &Store, bytes: usize, flush: bool) -> Instant {
        let rng = &self.rng;
        let mut time =
            IO_TIME + Duration::from_nanos(bytes as u64 * 1_000_000_000 / BYTES_PER_SECOND);
        if flush {
            time += FLUSH_TIME;
        }
        time = time * rng.range(50..150u32) / 100;
        if rng.bool(STALL_PROBABILITY) {
            time += rng.range(MIN_STALL..=MAX_STALL);
        }
        let mut queues = store.queues.get();
        let queue = &mut queues[rng.range(0..QUEUES)];
        *queue = self
            .clock
            .now()
            .max(*queue)
            .checked_add(time)
            .unwrap_or(Instant::MAX);
        let done = *queue;
        store.queues.set(queues);
        done
    }

    /// Whether this operation hits an `EIO` fault on `store`.
    fn io_error(&self, store: &Store) -> io::Result<()> {
        let p = store.faults.borrow().io_errors;
        if p > 0.0 && self.rng.bool(p) {
            return Err(io::Error::from_raw_os_error(5));
        }
        Ok(())
    }
}

fn check_probability(p: f64) {
    assert!(
        (0.0..=1.0).contains(&p),
        "probability {p} is outside [0, 1]"
    );
}

/// One disk.
struct Store {
    id: DiskId,
    state: RefCell<State>,
    faults: RefCell<Faults>,
    /// The total length of every file on the disk, kept by the inodes themselves.
    usage: Rc<Cell<u64>>,
    /// When each of the disk's queues finishes the operations on it so far, in virtual time.
    queues: Cell<[Instant; QUEUES]>,
}

#[derive(Default)]
struct Faults {
    full: bool,
    /// The probability that a read, write, `sync`, or `sync_dir` fails with `EIO`.
    io_errors: f64,
    /// The probability that a read returns one flipped bit.
    bit_flips: f64,
}

impl Store {
    fn new(id: DiskId) -> Store {
        Store {
            id,
            state: RefCell::default(),
            faults: RefCell::default(),
            usage: Rc::default(),
            queues: Cell::new([Instant::ZERO; QUEUES]),
        }
    }

    fn space(&self) -> Space {
        let available = if self.faults.borrow().full {
            0
        } else {
            CAPACITY.saturating_sub(self.usage.get())
        };
        Space {
            total: CAPACITY,
            available,
        }
    }

    /// Fails with `StorageFull` unless `more` bytes fit.
    fn reserve(&self, more: u64) -> io::Result<()> {
        if more > 0 && more > self.space().available {
            return Err(self.full());
        }
        Ok(())
    }

    fn full(&self) -> io::Error {
        io::Error::new(io::ErrorKind::StorageFull, format!("{:?} is full", self.id))
    }
}

/// What every disk and file handle shares: the disks, and the process acting through it.
struct Handle {
    disks: Rc<Disks>,
    process: ProcessId,
}

impl Handle {
    /// Waits for a read's turn on `store` and its finish, failing if the process is dead before
    /// or after, since a kill may come while it waits. A read changes nothing, so it starts
    /// only when polled.
    async fn start(&self, store: &Store, bytes: usize, flush: bool) -> io::Result<()> {
        self.alive()?;
        let done = self.disks.schedule(store, bytes, flush);
        self.disks.clock.sleep_until(done).await;
        self.alive()
    }

    /// Submits an operation that changes `store`: it takes its turn now, and `apply` runs when
    /// it finishes, whether or not anyone awaits it.
    fn submit<'a, T: 'a>(
        &self,
        store: &Store,
        bytes: usize,
        flush: bool,
        apply: impl FnOnce() -> T + 'a,
    ) -> InFlight<'a, T> {
        let done = self.disks.schedule(store, bytes, flush);
        InFlight {
            sleep: self.disks.clock.sleep_until(done),
            apply: Some(Box::new(apply)),
        }
    }

    /// Runs `change`, the effect of a submitted operation that just finished, for the process.
    /// A live process gets its result. A dead one gets an error, but the disk may have finished
    /// the operation before the process died, so `change` may still happen.
    fn complete<R>(&self, change: impl FnOnce() -> io::Result<R>) -> io::Result<R> {
        match self.alive() {
            Ok(()) => change(),
            Err(dead) => {
                if self.disks.rng.bool(LANDED_PROBABILITY) {
                    let _ = change();
                }
                Err(dead)
            }
        }
    }

    fn alive(&self) -> io::Result<()> {
        self.disks.machines.check_alive(self.process)
    }
}

/// An operation that changes a disk, submitted when it was called. Awaiting it waits until the
/// disk finishes it and returns what it did. Dropping it first doesn't cancel it: the disk
/// finishes it regardless, so it's applied then, at once, and its result is dropped.
struct InFlight<'a, T> {
    sleep: Sleep,
    apply: Option<Box<dyn FnOnce() -> T + 'a>>,
}

impl<T> Future for InFlight<'_, T> {
    type Output = T;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        ready!(Pin::new(&mut self.sleep).poll(cx));
        let apply = self.apply.take().expect("polled after it finished");
        Poll::Ready(apply())
    }
}

impl<T> Drop for InFlight<'_, T> {
    fn drop(&mut self) {
        if let Some(apply) = self.apply.take() {
            drop(apply());
        }
    }
}

/// The future for an operation that failed before it was submitted.
fn failed<'a, T: 'a>(output: T) -> LocalBoxFuture<'a, T> {
    Box::pin(std::future::ready(output))
}

/// A process's handle on one of its machine's disks.
pub(crate) struct SimDisk {
    handle: Handle,
    id: DiskId,
}

impl SimDisk {
    /// The disk's contents, once the process is checked to be alive.
    fn store(&self) -> io::Result<Rc<Store>> {
        self.handle.alive()?;
        Ok(self.handle.disks.store(self.id))
    }

    /// Submits an operation that changes the disk. `change` runs on its contents when the
    /// operation finishes.
    fn submit<'a, T: 'a>(
        &'a self,
        flush: bool,
        change: impl FnOnce(Rc<Store>) -> io::Result<T> + 'a,
    ) -> LocalBoxFuture<'a, io::Result<T>> {
        let store = match self.store() {
            Ok(store) => store,
            Err(e) => return failed(Err(e)),
        };
        let handle = &self.handle;
        let disk = store.clone();
        let apply = move || handle.complete(|| change(disk));
        Box::pin(handle.submit(&store, 0, flush, apply))
    }
}

fn not_a_directory(path: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::NotADirectory,
        format!("{path} is a file, not a directory"),
    )
}

fn is_a_directory(path: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::IsADirectory,
        format!("{path} is a directory, not a file"),
    )
}

fn not_found(path: &str) -> io::Error {
    io::Error::new(io::ErrorKind::NotFound, format!("{path} not found"))
}

/// The errors `std::fs::OpenOptions` reports before opening anything.
fn check_options(opts: OpenOptions) -> io::Result<()> {
    let creates = opts.create || opts.create_new || opts.truncate;
    if !(opts.read || opts.write) || (creates && !opts.write) {
        return Err(io::Error::from_raw_os_error(22));
    }
    Ok(())
}

impl DiskIo for SimDisk {
    fn open<'a>(
        &'a self,
        path: &'a str,
        opts: OpenOptions,
    ) -> LocalBoxFuture<'a, io::Result<Box<dyn FileIo>>> {
        if let Err(e) = check_path(path).and_then(|()| check_options(opts)) {
            return failed(Err(e));
        }
        self.submit(false, move |store| {
            let existing = {
                let state = store.state.borrow();
                state.live().check_parents(path)?;
                match state.live().entry(path) {
                    Entry::Dir | Entry::File(_) if opts.create_new => {
                        return Err(io::Error::new(
                            io::ErrorKind::AlreadyExists,
                            format!("{path} exists"),
                        ));
                    }
                    Entry::Dir => return Err(is_a_directory(path)),
                    Entry::File(inode) => Some(inode),
                    Entry::Missing if opts.create || opts.create_new => None,
                    Entry::Missing => return Err(not_found(path)),
                }
            };
            let disks = &self.handle.disks;
            let mode = if disks.rng.bool(FULL_CORRUPTION_PROBABILITY) {
                CorruptionMode::Full
            } else {
                CorruptionMode::DropOnly
            };
            let inode = match existing {
                Some(inode) => {
                    if opts.truncate {
                        inode.borrow_mut().set_len(0, mode);
                    }
                    inode
                }
                None => {
                    // Creating a file on a full disk fails, like running out of inodes.
                    if store.faults.borrow().full {
                        return Err(store.full());
                    }
                    let inode = Rc::new(RefCell::new(Inode::new(store.usage.clone())));
                    let mut state = store.state.borrow_mut();
                    state.insert_file(path, inode.clone());
                    inode
                }
            };
            Ok(Box::new(SimFile {
                handle: Handle {
                    disks: disks.clone(),
                    process: self.handle.process,
                },
                store,
                inode,
                opts,
                mode,
            }) as Box<dyn FileIo>)
        })
    }

    fn rename<'a>(&'a self, from: &'a str, to: &'a str) -> LocalBoxFuture<'a, io::Result<()>> {
        if let Err(e) = check_path(from).and_then(|()| check_path(to)) {
            return failed(Err(e));
        }
        self.submit(false, move |store| {
            let mut state = store.state.borrow_mut();
            let live = state.live();
            live.check_parents(from)?;
            let inode = match live.entry(from) {
                Entry::Missing => return Err(not_found(from)),
                Entry::Dir => return Err(is_a_directory(from)),
                Entry::File(inode) => inode,
            };
            live.check_parents(to)?;
            if let Entry::Dir = live.entry(to) {
                return Err(is_a_directory(to));
            }
            if from != to {
                state.remove_file(from);
                state.insert_file(to, inode);
            }
            Ok(())
        })
    }

    fn remove<'a>(&'a self, path: &'a str) -> LocalBoxFuture<'a, io::Result<()>> {
        if let Err(e) = check_path(path) {
            return failed(Err(e));
        }
        self.submit(false, move |store| {
            let mut state = store.state.borrow_mut();
            state.live().check_parents(path)?;
            match state.live().entry(path) {
                Entry::Missing => Err(not_found(path)),
                Entry::Dir => Err(is_a_directory(path)),
                Entry::File(_) => {
                    state.remove_file(path);
                    Ok(())
                }
            }
        })
    }

    fn list<'a>(&'a self, dir: &'a str) -> LocalBoxFuture<'a, io::Result<Vec<String>>> {
        Box::pin(async move {
            check_dir(dir)?;
            let store = self.store()?;
            self.handle.start(&store, 0, false).await?;
            let state = store.state.borrow();
            let live = state.live();
            live.check_parents(dir)?;
            if let Entry::File(_) = live.entry(dir) {
                return Err(not_a_directory(dir));
            }
            // `files` is sorted by path, so the names come out sorted.
            Ok(live
                .files_in(dir)
                .map(|(name, _)| name.to_owned())
                .collect())
        })
    }

    fn sync_dir<'a>(&'a self, dir: &'a str) -> LocalBoxFuture<'a, io::Result<()>> {
        if let Err(e) = check_dir(dir) {
            return failed(Err(e));
        }
        self.submit(true, move |store| {
            // A failed sync leaves the entries pending, for a power cut to lose.
            if let Err(e) = self.handle.disks.io_error(&store) {
                probe!("disk.sync_dir_failed");
                return Err(e);
            }
            let mut state = store.state.borrow_mut();
            state.live().check_parents(dir)?;
            match state.live().entry(dir) {
                Entry::File(_) => Err(not_a_directory(dir)),
                // Nothing in a missing directory can be lost.
                Entry::Missing => Ok(()),
                Entry::Dir => {
                    state.sync_dir(dir);
                    Ok(())
                }
            }
        })
    }

    fn space(&self) -> Space {
        let disks = &self.handle.disks;
        match disks.stores.borrow().get(&self.id) {
            Some(store) => store.space(),
            None if disks.machines.has_disk(self.id) => Space {
                total: CAPACITY,
                available: CAPACITY,
            },
            // A dead process's disk that its machine has since lost.
            None => Space {
                total: 0,
                available: 0,
            },
        }
    }
}

/// An open file. It keeps its contents even after the name is removed, as on Linux.
struct SimFile {
    handle: Handle,
    store: Rc<Store>,
    inode: InodeRef,
    opts: OpenOptions,
    mode: CorruptionMode,
}

impl SimFile {
    /// Fails with `not_writable()` unless the handle was opened for writing, and fails if its
    /// process is dead, before an operation that writes is submitted.
    fn writable(&self, not_writable: fn() -> io::Error) -> io::Result<()> {
        if !self.opts.write {
            return Err(not_writable());
        }
        self.handle.alive()
    }

    /// Fails with `StorageFull` unless the file can grow to `len`.
    fn reserve(&self, len: u64) -> io::Result<()> {
        let old = self.inode.borrow().len();
        self.store.reserve(len.saturating_sub(old))
    }
}

/// The error for reading or writing through a handle not opened for it (`EBADF`).
fn bad_handle() -> io::Error {
    io::Error::from_raw_os_error(9)
}

impl FileIo for SimFile {
    fn read_at(
        &self,
        mut buf: Vec<u8>,
        offset: u64,
    ) -> LocalBoxFuture<'_, (io::Result<usize>, Vec<u8>)> {
        Box::pin(async move {
            let result = async {
                if !self.opts.read {
                    return Err(bad_handle());
                }
                let disks = &self.handle.disks;
                self.handle.start(&self.store, buf.len(), false).await?;
                disks.io_error(&self.store)?;
                let n = self.inode.borrow().read(&mut buf, offset);
                let p = self.store.faults.borrow().bit_flips;
                if n > 0 && p > 0.0 && disks.rng.bool(p) {
                    let bit = disks.rng.range(0..n * 8);
                    buf[bit / 8] ^= 1 << (bit % 8);
                }
                Ok(n)
            }
            .await;
            (result, buf)
        })
    }

    fn write_at(&self, buf: Vec<u8>, offset: u64) -> LocalBoxFuture<'_, (io::Result<()>, Vec<u8>)> {
        if let Err(e) = self.writable(bad_handle) {
            return failed((Err(e), buf));
        }
        let handle = &self.handle;
        let store = &self.store;
        let len = buf.len();
        Box::pin(handle.submit(store, len, false, move || {
            let result = handle.complete(|| {
                handle.disks.io_error(store)?;
                if buf.is_empty() {
                    return Ok(());
                }
                let end = offset
                    .checked_add(buf.len() as u64)
                    .ok_or_else(|| io::Error::from_raw_os_error(22))?;
                self.reserve(end)?;
                self.inode.borrow_mut().write(&buf, offset, self.mode);
                Ok(())
            });
            (result, buf)
        }))
    }

    fn sync(&self) -> LocalBoxFuture<'_, io::Result<()>> {
        if let Err(e) = self.handle.alive() {
            return failed(Err(e));
        }
        let handle = &self.handle;
        Box::pin(handle.submit(&self.store, 0, true, move || {
            handle.complete(|| {
                // A failed sync leaves the pages pending, for a power cut to lose or corrupt, or
                // loses them outright, as Linux may.
                if let Err(e) = handle.disks.io_error(&self.store) {
                    probe!("disk.sync_failed");
                    if handle.disks.rng.bool(SYNC_LOSES_PAGES_PROBABILITY) {
                        probe!("disk.sync_lost_pages");
                        self.inode.borrow_mut().lose_dirty();
                    }
                    return Err(e);
                }
                self.inode.borrow_mut().sync();
                Ok(())
            })
        }))
    }

    fn set_len(&self, len: u64) -> LocalBoxFuture<'_, io::Result<()>> {
        if let Err(e) = self.writable(|| io::Error::from_raw_os_error(22)) {
            return failed(Err(e));
        }
        let handle = &self.handle;
        Box::pin(handle.submit(&self.store, 0, false, move || {
            handle.complete(|| {
                self.reserve(len)?;
                self.inode.borrow_mut().set_len(len, self.mode);
                Ok(())
            })
        }))
    }

    fn len(&self) -> u64 {
        self.inode.borrow().len()
    }
}
