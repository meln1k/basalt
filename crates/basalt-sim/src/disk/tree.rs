//! A disk's names: the directories and files processes see, and those that survive a power cut.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::rc::Rc;

use basalt_env::{DiskId, Rng, trace};

use super::inode::Inode;
use super::not_a_directory;

/// How often a power cut finds a directory's pending entries already flushed, as if the OS
/// synced it on its own.
const DIR_FLUSHED_PROBABILITY: f64 = 0.5;

pub(super) type InodeRef = Rc<RefCell<Inode>>;

/// What processes see, and what survives a power cut.
#[derive(Default)]
pub(super) struct State {
    pub(super) live: Tree,
    durable: Tree,
}

/// A disk's names: its directories and the files in them. The root `""` is always a directory
/// and isn't in `dirs`.
#[derive(Clone, Default)]
pub(super) struct Tree {
    dirs: BTreeSet<String>,
    pub(super) files: BTreeMap<String, InodeRef>,
}

pub(super) enum Entry {
    Missing,
    Dir,
    File(InodeRef),
}

impl Tree {
    pub(super) fn entry(&self, path: &str) -> Entry {
        if path.is_empty() || self.dirs.contains(path) {
            Entry::Dir
        } else if let Some(inode) = self.files.get(path) {
            Entry::File(inode.clone())
        } else {
            Entry::Missing
        }
    }

    /// Fails with `NotADirectory` if a directory above `path` is a file.
    pub(super) fn check_parents(&self, path: &str) -> io::Result<()> {
        for parent in parents(path) {
            if let Entry::File(_) = self.entry(parent) {
                return Err(not_a_directory(parent));
            }
        }
        Ok(())
    }

    /// Creates the missing directories above `path`, once
    /// [`check_parents`](Tree::check_parents) passed.
    pub(super) fn create_parents(&mut self, path: &str) {
        for parent in parents(path) {
            if !self.dirs.contains(parent) {
                self.dirs.insert(parent.to_owned());
            }
        }
    }

    /// The files directly in `dir`, by name.
    pub(super) fn files_in<'a>(
        &'a self,
        dir: &'a str,
    ) -> impl Iterator<Item = (&'a str, &'a InodeRef)> + 'a {
        let prefix = if dir.is_empty() {
            String::new()
        } else {
            format!("{dir}/")
        };
        let skip = prefix.len();
        self.files
            .range(prefix.clone()..)
            .take_while(move |(path, _)| path.starts_with(&prefix))
            .filter_map(move |(path, inode)| {
                let name = &path[skip..];
                (!name.contains('/')).then_some((name, inode))
            })
    }

    /// Whether `other` has the same files directly in `dir`.
    fn same_files_in(&self, other: &Tree, dir: &str) -> bool {
        self.files_in(dir)
            .map(|(name, inode)| (name, Rc::as_ptr(inode)))
            .eq(other
                .files_in(dir)
                .map(|(name, inode)| (name, Rc::as_ptr(inode))))
    }

    /// Whether every directory above every name is in `dirs`.
    fn is_connected(&self) -> bool {
        self.dirs
            .iter()
            .chain(self.files.keys())
            .all(|path| parents(path).all(|p| self.dirs.contains(p)))
    }
}

/// The directories above `path`, outermost first: `a` and `a/b` for `a/b/c`.
fn parents(path: &str) -> impl Iterator<Item = &str> {
    path.match_indices('/').map(|(i, _)| &path[..i])
}

fn join(dir: &str, name: &str) -> String {
    if dir.is_empty() {
        name.to_owned()
    } else {
        format!("{dir}/{name}")
    }
}

impl State {
    /// Makes durable the files directly in `dir` and every directory on the path to it, as
    /// `sync_dir` does. `dir` must be a live directory.
    pub(super) fn sync_dir(&mut self, dir: &str) {
        let State { live, durable } = self;
        if !dir.is_empty() {
            for d in parents(dir).chain([dir]) {
                // Its name in its parent is now a directory, whatever it was before.
                durable.files.remove(d);
                durable.dirs.insert(d.to_owned());
            }
        }
        let stale: Vec<String> = durable
            .files_in(dir)
            .map(|(name, _)| join(dir, name))
            .collect();
        for path in stale {
            durable.files.remove(&path);
        }
        for (name, inode) in live.files_in(dir) {
            durable.files.insert(join(dir, name), inode.clone());
        }
    }

    /// Whether `dir`'s entries, or the directory itself, aren't durable yet.
    fn dir_pending(&self, dir: &str) -> bool {
        let dir_durable = dir.is_empty() || self.durable.dirs.contains(dir);
        !dir_durable || !self.live.same_files_in(&self.durable, dir)
    }

    /// Drops everything not durable: some pending directories may have been flushed first,
    /// then each surviving file loses or corrupts its pending pages.
    pub(super) fn power_cut(&mut self, disk: DiskId, rng: &Rng) {
        // The OS may have written some directories' pending entries before the power went.
        // Parents sort before their children.
        let dirs: Vec<String> = [String::new()]
            .into_iter()
            .chain(self.live.dirs.iter().cloned())
            .collect();
        for dir in dirs {
            if self.dir_pending(&dir) {
                let flushed = rng.bool(DIR_FLUSHED_PROBABILITY);
                trace!(
                    Info,
                    "DiskPowerCutDir",
                    disk = disk.get(),
                    dir = dir,
                    flushed = flushed
                );
                if flushed {
                    self.sync_dir(&dir);
                }
            }
        }
        // `sync_dir` makes every directory above what it syncs durable too.
        debug_assert!(
            self.durable.is_connected(),
            "a durable name is under a directory that isn't durable"
        );
        // A file left with no durable name is gone, along with its pages.
        let mut seen = BTreeSet::new();
        for (path, inode) in &self.durable.files {
            if !seen.insert(Rc::as_ptr(inode)) {
                continue;
            }
            if let Some(cut) = inode.borrow_mut().power_cut(rng) {
                trace!(
                    Info,
                    "DiskPowerCutFile",
                    disk = disk.get(),
                    file = path,
                    worst_mode = cut.worst_mode,
                    length_kept = cut.length_kept,
                    kept = cut.kept,
                    dropped = cut.dropped,
                    torn = cut.torn,
                    corrupted = cut.corrupted
                );
            }
        }
        // A rename between two directories that only one of them made durable can leave the
        // file under both names. Give each name its own copy rather than a hard link.
        let mut seen = BTreeSet::new();
        for inode in self.durable.files.values_mut() {
            if !seen.insert(Rc::as_ptr(inode)) {
                let copy = inode.borrow().clone();
                *inode = Rc::new(RefCell::new(copy));
            }
        }
        self.live = self.durable.clone();
    }
}
