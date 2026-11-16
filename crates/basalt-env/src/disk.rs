use std::fmt;
use std::io;
use std::rc::Rc;

use crate::backend::{DiskIo, FileIo};

/// Identifies a disk within a world. Backends assign them.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DiskId(u32);

impl DiskId {
    pub const fn new(raw: u32) -> DiskId {
        DiskId(raw)
    }

    pub const fn get(self) -> u32 {
        self.0
    }
}

/// A disk's size and free space, in bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Space {
    pub total: u64,
    pub available: u64,
}

/// How to open a file, as in `std::fs::OpenOptions`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct OpenOptions {
    pub read: bool,
    pub write: bool,
    /// Create the file if it doesn't exist.
    pub create: bool,
    /// Create the file, failing if it exists.
    pub create_new: bool,
    /// Cut the file to zero length.
    pub truncate: bool,
}

impl OpenOptions {
    /// No access; set what you need.
    pub fn new() -> OpenOptions {
        OpenOptions::default()
    }

    pub fn read(mut self, yes: bool) -> OpenOptions {
        self.read = yes;
        self
    }

    pub fn write(mut self, yes: bool) -> OpenOptions {
        self.write = yes;
        self
    }

    pub fn create(mut self, yes: bool) -> OpenOptions {
        self.create = yes;
        self
    }

    pub fn create_new(mut self, yes: bool) -> OpenOptions {
        self.create_new = yes;
        self
    }

    pub fn truncate(mut self, yes: bool) -> OpenOptions {
        self.truncate = yes;
        self
    }
}

/// A disk of this process's machine, from [`Ctx::disk`](crate::Ctx::disk).
///
/// Nothing is durable until it's synced: file contents by [`File::sync`], and creating,
/// renaming, and removing files by [`Disk::sync_dir`] on the directory that holds them. In
/// simulation, a power cut loses or corrupts anything not yet synced.
///
/// # Paths and directories
///
/// There is no `create_dir`: directories are implicit. Every backend follows these rules:
///
/// - A path is relative to the disk's root: non-empty components joined by `/`, with no `.` or
///   `..`. Anything else fails with `InvalidInput`. A directory argument may also be `""`, the
///   root.
/// - Opening a file with `create` or `create_new`, or renaming a file, creates the missing
///   directories above it.
/// - `sync_dir(d)` makes durable the creating, renaming, and removing of files directly in `d`,
///   and every directory on the path to `d`, including ones created above. Nothing else about
///   directories is durable before that. A rename between two directories needs `sync_dir` of
///   both. `sync_dir` of a directory that doesn't exist does nothing, since nothing in it can be
///   lost.
/// - `list` returns the names of the files directly in a directory, sorted, without
///   subdirectories. A directory with no files lists as empty, whether or not it exists.
/// - `remove` removes files only, and leaves empty directories behind.
/// - A name is a file or a directory, never both. Opening, renaming, or removing a directory as
///   a file fails with `IsADirectory`, and a path through a file fails with `NotADirectory`.
#[derive(Clone)]
pub struct Disk {
    id: DiskId,
    io: Rc<dyn DiskIo>,
}

impl Disk {
    pub(crate) fn new(id: DiskId, io: Rc<dyn DiskIo>) -> Disk {
        Disk { id, io }
    }

    pub fn id(&self) -> DiskId {
        self.id
    }

    pub async fn open(&self, path: &str, opts: OpenOptions) -> io::Result<File> {
        let io = self.io.open(path, opts).await?;
        Ok(File { io })
    }

    pub async fn rename(&self, from: &str, to: &str) -> io::Result<()> {
        self.io.rename(from, to).await
    }

    pub async fn remove(&self, path: &str) -> io::Result<()> {
        self.io.remove(path).await
    }

    /// The names of the files directly in `dir`, sorted.
    pub async fn list(&self, dir: &str) -> io::Result<Vec<String>> {
        self.io.list(dir).await
    }

    /// Makes durable the creating, renaming, and removing of files directly in `dir`, and every
    /// directory on the path to it.
    pub async fn sync_dir(&self, dir: &str) -> io::Result<()> {
        self.io.sync_dir(dir).await
    }

    pub fn space(&self) -> Space {
        self.io.space()
    }
}

impl fmt::Debug for Disk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Disk")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

/// An open file on a [`Disk`]. I/O takes and returns owned buffers, so moving to io_uring later
/// doesn't change the API: the buffer comes back whether or not the call succeeded.
pub struct File {
    io: Box<dyn FileIo>,
}

#[allow(clippy::len_without_is_empty)]
impl File {
    /// Reads into `buf[..buf.len()]` from `offset` and returns the number of bytes read, which is
    /// less than `buf.len()` only at the end of the file. Size the buffer with `vec![0; n]`.
    pub async fn read_at(&self, buf: Vec<u8>, offset: u64) -> (io::Result<usize>, Vec<u8>) {
        self.io.read_at(buf, offset).await
    }

    /// Writes all of `buf` at `offset`, extending the file if needed.
    pub async fn write_at(&self, buf: Vec<u8>, offset: u64) -> (io::Result<()>, Vec<u8>) {
        self.io.write_at(buf, offset).await
    }

    /// Makes everything written to the file so far durable.
    pub async fn sync(&self) -> io::Result<()> {
        self.io.sync().await
    }

    pub async fn set_len(&self, len: u64) -> io::Result<()> {
        self.io.set_len(len).await
    }

    /// The file's length, including writes not yet synced.
    pub fn len(&self) -> u64 {
        self.io.len()
    }
}

impl fmt::Debug for File {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("File").field("len", &self.len()).finish()
    }
}
