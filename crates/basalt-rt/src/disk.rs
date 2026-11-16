//! Disks as directories on the host, with blocking file calls on the I/O pool.
//!
//! The path and directory rules are in `basalt_env::Disk`'s docs, which both backends follow.

use std::fs;
use std::io;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use basalt_env::backend::{DiskIo, FileIo, LocalBoxFuture};
use basalt_env::{OpenOptions, Space};

use crate::pool::Pool;

/// A disk rooted at a directory on the host.
pub(crate) struct Disk {
    root: Arc<Path>,
    pool: Pool,
}

impl Disk {
    pub(crate) fn new(root: PathBuf, pool: Pool) -> Disk {
        Disk {
            root: root.into(),
            pool,
        }
    }

    /// Runs `f` on the I/O pool with an already resolved path.
    fn run<T: Send + 'static>(
        &self,
        path: io::Result<PathBuf>,
        f: impl FnOnce(PathBuf) -> io::Result<T> + Send + 'static,
    ) -> LocalBoxFuture<'_, io::Result<T>> {
        Box::pin(async move {
            let path = path?;
            self.pool.run(move || f(path)).await
        })
    }

    /// A file's path: non-empty components without `.` or `..`.
    fn file(&self, path: &str) -> io::Result<PathBuf> {
        let valid = !path.is_empty()
            && !path.contains('\0')
            && path
                .split('/')
                .all(|part| !part.is_empty() && part != "." && part != "..");
        if !valid {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid disk path {path:?}: use relative `a/b` paths without `.` or `..`"),
            ));
        }
        Ok(self.root.join(path))
    }

    /// A directory's path: like a file's, or `""` for the root.
    fn dir(&self, dir: &str) -> io::Result<PathBuf> {
        if dir.is_empty() {
            Ok(self.root.to_path_buf())
        } else {
            self.file(dir)
        }
    }
}

impl DiskIo for Disk {
    fn open<'a>(
        &'a self,
        path: &'a str,
        opts: OpenOptions,
    ) -> LocalBoxFuture<'a, io::Result<Box<dyn FileIo>>> {
        let pool = self.pool.clone();
        let opened = self.run(self.file(path), move |path| {
            if opts.create || opts.create_new {
                create_parents(&path)?;
            }
            let file = fs::OpenOptions::new()
                .read(opts.read)
                .write(opts.write)
                .create(opts.create)
                .create_new(opts.create_new)
                .truncate(opts.truncate)
                .open(&path)?;
            // Linux opens a directory read-only without complaint, but a directory isn't a file.
            if file.metadata()?.is_dir() {
                return Err(is_a_directory(&path));
            }
            Ok(file)
        });
        Box::pin(async move {
            let file = opened.await?;
            Ok(Box::new(File {
                file: Arc::new(file),
                pool,
            }) as Box<dyn FileIo>)
        })
    }

    fn rename<'a>(&'a self, from: &'a str, to: &'a str) -> LocalBoxFuture<'a, io::Result<()>> {
        let to = self.file(to);
        self.run(self.file(from), move |from| {
            let to = to?;
            for path in [&from, &to] {
                if path.is_dir() {
                    return Err(is_a_directory(path));
                }
            }
            create_parents(&to)?;
            fs::rename(from, to)
        })
    }

    fn remove<'a>(&'a self, path: &'a str) -> LocalBoxFuture<'a, io::Result<()>> {
        self.run(self.file(path), fs::remove_file)
    }

    fn list<'a>(&'a self, dir: &'a str) -> LocalBoxFuture<'a, io::Result<Vec<String>>> {
        self.run(self.dir(dir), |dir| {
            let entries = match fs::read_dir(dir) {
                Ok(entries) => entries,
                Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
                Err(e) => return Err(e),
            };
            let mut names = Vec::new();
            for entry in entries {
                let entry = entry?;
                if !entry.file_type()?.is_file() {
                    continue;
                }
                let name = entry.file_name().into_string().map_err(|name| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("file name {name:?} isn't UTF-8"),
                    )
                })?;
                names.push(name);
            }
            names.sort();
            Ok(names)
        })
    }

    fn sync_dir<'a>(&'a self, dir: &'a str) -> LocalBoxFuture<'a, io::Result<()>> {
        let root = self.root.clone();
        self.run(self.dir(dir), move |dir| {
            // Nothing in a missing directory can be lost.
            match fsync_dir(&dir) {
                Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
                result => result?,
            }
            // Then every directory above it, up to the root, so the path to `dir` is durable too.
            for above in dir.ancestors().skip(1) {
                if !above.starts_with(&root) {
                    break;
                }
                fsync_dir(above)?;
            }
            Ok(())
        })
    }

    fn space(&self) -> Space {
        // One statvfs call, quick enough for the executor thread. If it fails, report the disk
        // as full rather than inventing free space.
        match rustix::fs::statvfs(&*self.root) {
            Ok(stat) => Space {
                total: stat.f_blocks.saturating_mul(stat.f_frsize),
                available: stat.f_bavail.saturating_mul(stat.f_frsize),
            },
            Err(_) => Space {
                total: 0,
                available: 0,
            },
        }
    }
}

struct File {
    file: Arc<fs::File>,
    pool: Pool,
}

impl FileIo for File {
    fn read_at(
        &self,
        mut buf: Vec<u8>,
        offset: u64,
    ) -> LocalBoxFuture<'_, (io::Result<usize>, Vec<u8>)> {
        let file = self.file.clone();
        Box::pin(self.pool.run(move || {
            // Short reads happen only at the end of the file, so keep reading until then.
            let mut done = 0;
            let result = loop {
                if done == buf.len() {
                    break Ok(done);
                }
                match file.read_at(&mut buf[done..], offset + done as u64) {
                    Ok(0) => break Ok(done),
                    Ok(n) => done += n,
                    Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                    Err(e) => break Err(e),
                }
            };
            (result, buf)
        }))
    }

    fn write_at(&self, buf: Vec<u8>, offset: u64) -> LocalBoxFuture<'_, (io::Result<()>, Vec<u8>)> {
        let file = self.file.clone();
        Box::pin(self.pool.run(move || {
            let result = file.write_all_at(&buf, offset);
            (result, buf)
        }))
    }

    fn sync(&self) -> LocalBoxFuture<'_, io::Result<()>> {
        let file = self.file.clone();
        // fdatasync also flushes the file's size, which reading the data back needs.
        Box::pin(self.pool.run(move || file.sync_data()))
    }

    fn set_len(&self, len: u64) -> LocalBoxFuture<'_, io::Result<()>> {
        let file = self.file.clone();
        Box::pin(self.pool.run(move || file.set_len(len)))
    }

    fn len(&self) -> u64 {
        // One fstat call, quick enough for the executor thread. It can only fail on a closed
        // descriptor, which an open `File` never has.
        self.file.metadata().map_or(0, |m| m.len())
    }
}

/// Creates the missing directories above `path`. They become durable with the next `sync_dir`
/// below them, like the file itself.
fn create_parents(path: &Path) -> io::Result<()> {
    let parent = path.parent().expect("a disk path is below the disk's root");
    match fs::create_dir_all(parent) {
        // `create_dir_all` reports a file in the way as `AlreadyExists`.
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => Err(io::Error::new(
            io::ErrorKind::NotADirectory,
            format!("{} is a file, not a directory", parent.display()),
        )),
        result => result,
    }
}

fn fsync_dir(dir: &Path) -> io::Result<()> {
    let file = fs::File::open(dir)?;
    if !file.metadata()?.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::NotADirectory,
            format!("{} is a file, not a directory", dir.display()),
        ));
    }
    file.sync_all()
}

fn is_a_directory(path: &Path) -> io::Error {
    io::Error::new(
        io::ErrorKind::IsADirectory,
        format!("{} is a directory, not a file", path.display()),
    )
}
