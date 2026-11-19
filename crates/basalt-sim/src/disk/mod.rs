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
//! Every random draw comes from the `disk` stream.

mod inode;
mod tree;

use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::io;
use std::rc::Rc;

use basalt_env::backend::{DiskIo, FileIo, LocalBoxFuture};
use basalt_env::{DiskId, Duration, Instant, OpenOptions, ProcessId, Rng, Space};

use crate::machine::Machines;
use crate::time::Clock;

use self::inode::{CorruptionMode, Inode};
use self::tree::{Entry, InodeRef, State};

/// Every disk's size.
const CAPACITY: u64 = 16 << 30;

/// How often an open picks full corruption over drop-only.
const FULL_CORRUPTION_PROBABILITY: f64 = 0.5;

/// The latency model: each operation occupies the disk for one I/O (an IOPS limit) plus its
/// bytes at the disk's bandwidth, and a sync also waits for a cache flush. Operations queue
/// behind each other, and each takes 50% to 150% of its nominal time, with a rare long stall.
const IO_TIME: Duration = Duration::from_micros(50);
const BYTES_PER_SECOND: u64 = 1 << 30;
const FLUSH_TIME: Duration = Duration::from_micros(500);
const STALL_PROBABILITY: f64 = 0.001;
const MIN_STALL: Duration = Duration::from_millis(5);
const MAX_STALL: Duration = Duration::from_millis(50);

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
                store.busy_until.set(Instant::ZERO);
            }
        }
    }

    /// Fills `disk`: from now on, anything that needs more space fails with `StorageFull`.
    pub(crate) fn fill(&self, disk: DiskId) {
        self.store(disk).faults.borrow_mut().full = true;
    }

    /// Makes reads and writes on `disk` fail with `EIO` with probability `p`.
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

    /// Waits for an operation of `bytes` bytes on `store` to get its turn and finish. A crash
    /// doesn't cancel what's queued, since the OS finishes it, so the next incarnation's first
    /// operation may wait behind the last one's.
    async fn io(&self, store: &Store, bytes: usize, flush: bool) {
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
        let start = self.clock.now().max(store.busy_until.get());
        let done = start.checked_add(time).unwrap_or(Instant::MAX);
        store.busy_until.set(done);
        self.clock.sleep_until(done).await;
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
    /// When the disk finishes the operations queued on it so far, in virtual time.
    busy_until: Cell<Instant>,
}

#[derive(Default)]
struct Faults {
    full: bool,
    /// The probability that a read or write fails with `EIO`.
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
            busy_until: Cell::new(Instant::ZERO),
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
    /// Waits for an operation's turn on `store`, failing if the process is dead before or
    /// after, since a kill may come while it waits.
    async fn start(&self, store: &Store, bytes: usize, flush: bool) -> io::Result<()> {
        self.alive()?;
        self.disks.io(store, bytes, flush).await;
        self.alive()
    }

    fn alive(&self) -> io::Result<()> {
        self.disks.machines.check_alive(self.process)
    }
}

/// A process's handle on one of its machine's disks.
pub(crate) struct SimDisk {
    handle: Handle,
    id: DiskId,
}

impl SimDisk {
    /// Waits for an operation's turn on the disk and returns the disk.
    async fn start(&self, flush: bool) -> io::Result<Rc<Store>> {
        self.handle.alive()?;
        let store = self.handle.disks.store(self.id);
        self.handle.start(&store, 0, flush).await?;
        Ok(store)
    }
}

/// Checks a file path: non-empty components joined by `/`, without `.` or `..`.
fn check_path(path: &str) -> io::Result<()> {
    let valid = !path.is_empty()
        && !path.contains('\0')
        && path
            .split('/')
            .all(|part| !part.is_empty() && part != "." && part != "..");
    if valid {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid disk path {path:?}: use relative `a/b` paths without `.` or `..`"),
        ))
    }
}

/// Checks a directory path: like a file's, or `""` for the root.
fn check_dir(dir: &str) -> io::Result<()> {
    if dir.is_empty() {
        Ok(())
    } else {
        check_path(dir)
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
        Box::pin(async move {
            check_path(path)?;
            check_options(opts)?;
            let store = self.start(false).await?;
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
        Box::pin(async move {
            check_path(from)?;
            check_path(to)?;
            let store = self.start(false).await?;
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
        Box::pin(async move {
            check_path(path)?;
            let store = self.start(false).await?;
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
            let store = self.start(false).await?;
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
        Box::pin(async move {
            check_dir(dir)?;
            let store = self.start(true).await?;
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
        Box::pin(async move {
            let result = async {
                if !self.opts.write {
                    return Err(bad_handle());
                }
                self.handle.start(&self.store, buf.len(), false).await?;
                self.handle.disks.io_error(&self.store)?;
                if buf.is_empty() {
                    return Ok(());
                }
                let end = offset
                    .checked_add(buf.len() as u64)
                    .ok_or_else(|| io::Error::from_raw_os_error(22))?;
                self.reserve(end)?;
                self.inode.borrow_mut().write(&buf, offset, self.mode);
                Ok(())
            }
            .await;
            (result, buf)
        })
    }

    fn sync(&self) -> LocalBoxFuture<'_, io::Result<()>> {
        Box::pin(async move {
            self.handle.start(&self.store, 0, true).await?;
            self.inode.borrow_mut().sync();
            Ok(())
        })
    }

    fn set_len(&self, len: u64) -> LocalBoxFuture<'_, io::Result<()>> {
        Box::pin(async move {
            if !self.opts.write {
                return Err(io::Error::from_raw_os_error(22));
            }
            self.handle.start(&self.store, 0, false).await?;
            self.reserve(len)?;
            self.inode.borrow_mut().set_len(len, self.mode);
            Ok(())
        })
    }

    fn len(&self) -> u64 {
        self.inode.borrow().len()
    }
}
