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
    live: Tree,
    durable: Tree,
    /// Exactly the file paths whose live entry differs from their durable one: a different
    /// inode, or a file in one tree and none in the other.
    pending: BTreeSet<String>,
}

/// A disk's names: its directories and the files in them. The root `""` is always a directory
/// and isn't in `dirs`.
#[derive(Clone, Default)]
pub(super) struct Tree {
    dirs: BTreeSet<String>,
    files: BTreeMap<String, InodeRef>,
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
    fn create_parents(&mut self, path: &str) {
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
        let prefix = prefix(dir);
        let skip = prefix.len();
        self.files
            .range(prefix.clone()..)
            .take_while(move |(path, _)| path.starts_with(&prefix))
            .filter_map(move |(path, inode)| {
                let name = &path[skip..];
                (!name.contains('/')).then_some((name, inode))
            })
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

/// Whether `a` and `b` have the same file at `path`, or both have none.
fn same_file(a: &Tree, b: &Tree, path: &str) -> bool {
    a.files.get(path).map(Rc::as_ptr) == b.files.get(path).map(Rc::as_ptr)
}

/// What every path directly in `dir` starts with.
fn prefix(dir: &str) -> String {
    if dir.is_empty() {
        String::new()
    } else {
        format!("{dir}/")
    }
}

impl State {
    /// What processes see. Its names change only through [`State::insert_file`] and
    /// [`State::remove_file`], which keep `pending` exact.
    pub(super) fn live(&self) -> &Tree {
        &self.live
    }

    /// Gives the live file at `path` the contents `inode`, creating the directories above it.
    pub(super) fn insert_file(&mut self, path: &str, inode: InodeRef) {
        self.live.create_parents(path);
        self.live.files.insert(path.to_owned(), inode);
        self.update_pending(path);
    }

    /// Removes the live file at `path`.
    pub(super) fn remove_file(&mut self, path: &str) {
        self.live.files.remove(path);
        self.update_pending(path);
    }

    /// Records whether `path`'s live and durable entries now differ.
    fn update_pending(&mut self, path: &str) {
        if same_file(&self.live, &self.durable, path) {
            self.pending.remove(path);
        } else if !self.pending.contains(path) {
            self.pending.insert(path.to_owned());
        }
    }

    /// Makes durable the files directly in `dir` and every directory on the path to it, as
    /// `sync_dir` does. `dir` must be a live directory.
    pub(super) fn sync_dir(&mut self, dir: &str) {
        // Only pending names can differ, so this costs only what changed under `dir` since it
        // was last synced, however many files it has.
        let synced: Vec<String> = self.pending_in(dir).cloned().collect();
        let State {
            live,
            durable,
            pending,
        } = self;
        if !dir.is_empty() {
            for d in parents(dir).chain([dir]) {
                // Its name in its parent is now a directory, whatever it was before. A durable
                // file there was pending, since the live name is a directory, and no longer is.
                durable.files.remove(d);
                durable.dirs.insert(d.to_owned());
                pending.remove(d);
            }
        }
        for path in synced {
            match live.files.get(&path) {
                Some(inode) => durable.files.insert(path.clone(), inode.clone()),
                None => durable.files.remove(&path),
            };
            pending.remove(&path);
        }
    }

    /// The pending paths directly in `dir`, as `sync_dir` makes them durable.
    fn pending_in<'a>(&'a self, dir: &str) -> impl Iterator<Item = &'a String> + 'a {
        let prefix = prefix(dir);
        let skip = prefix.len();
        self.pending
            .range(prefix.clone()..)
            .take_while(move |path| path.starts_with(&prefix))
            .filter(move |path| !path[skip..].contains('/'))
    }

    /// Whether `dir`'s entries, or the directory itself, aren't durable yet.
    fn dir_pending(&self, dir: &str) -> bool {
        let dir_durable = dir.is_empty() || self.durable.dirs.contains(dir);
        !dir_durable || self.pending_in(dir).next().is_some()
    }

    /// Whether `pending` is exactly the paths whose live and durable entries differ. Costs
    /// time proportional to every file on the disk.
    fn pending_is_exact(&self) -> bool {
        let differ: BTreeSet<&String> = self
            .live
            .files
            .keys()
            .chain(self.durable.files.keys())
            .filter(|path| !same_file(&self.live, &self.durable, path))
            .collect();
        differ.into_iter().eq(&self.pending)
    }

    /// Drops everything not durable: some pending directories may have been flushed first,
    /// then each surviving file loses or corrupts its pending pages.
    pub(super) fn power_cut(&mut self, disk: DiskId, rng: &Rng) {
        debug_assert!(self.pending_is_exact(), "`pending` drifted from the trees");
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
        self.pending.clear();
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use super::*;

    /// The model `State` replaced: `sync_dir` copies every file directly in the directory, and
    /// a power cut finds pending directories by comparing whole listings.
    #[derive(Default)]
    struct Reference {
        live: Tree,
        durable: Tree,
    }

    impl Reference {
        fn sync_dir(&mut self, dir: &str) {
            let Reference { live, durable } = self;
            if !dir.is_empty() {
                for d in parents(dir).chain([dir]) {
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

        fn dir_pending(&self, dir: &str) -> bool {
            let listing = |tree: &Tree| -> Vec<(String, *const RefCell<Inode>)> {
                tree.files_in(dir)
                    .map(|(name, inode)| (name.to_owned(), Rc::as_ptr(inode)))
                    .collect()
            };
            let dir_durable = dir.is_empty() || self.durable.dirs.contains(dir);
            !dir_durable || listing(&self.live) != listing(&self.durable)
        }

        /// Cuts the power with the same draws as `real`'s cut, given an `Rng` in the same state.
        /// Where a name gets its own copy of a file, it takes `real`'s copy, since a copy has
        /// no identity to compare.
        fn power_cut(&mut self, rng: &Rng, real: &State) {
            let dirs: Vec<String> = [String::new()]
                .into_iter()
                .chain(self.live.dirs.iter().cloned())
                .collect();
            for dir in dirs {
                if self.dir_pending(&dir) && rng.bool(DIR_FLUSHED_PROBABILITY) {
                    self.sync_dir(&dir);
                }
            }
            let mut seen = BTreeSet::new();
            for (path, inode) in &mut self.durable.files {
                if !seen.insert(Rc::as_ptr(inode)) {
                    let copy = real.durable.files[path].clone();
                    assert!(!Rc::ptr_eq(&copy, inode), "{path} wasn't copied");
                    *inode = copy;
                }
            }
            self.live = self.durable.clone();
        }
    }

    fn join(dir: &str, name: &str) -> String {
        format!("{}{name}", prefix(dir))
    }

    fn assert_same(real: &Tree, reference: &Tree, what: &str) {
        assert_eq!(real.dirs, reference.dirs, "{what} directories");
        let files = |tree: &Tree| -> Vec<(String, *const RefCell<Inode>)> {
            tree.files
                .iter()
                .map(|(path, inode)| (path.clone(), Rc::as_ptr(inode)))
                .collect()
        };
        assert_eq!(files(real), files(reference), "{what} files");
    }

    /// Random renames, creates, removes, directory syncs, and power cuts leave `State` and the
    /// full-copy model with the same live and durable trees, and `pending` exact, after every
    /// step. The paths are few, so the same name is often a file, then a directory, then a
    /// file again, and renames often go back and forth.
    #[test]
    fn incremental_sync_dir_matches_full_copy() {
        const PATHS: [&str; 9] = ["x", "y", "a", "a/x", "a/y", "a/c/x", "b/x", "b/y", "b"];
        const DIRS: [&str; 5] = ["", "a", "b", "a/c", "x"];
        let usage = Rc::new(Cell::new(0));
        for seed in 0..300 {
            let rng = Rng::new(seed);
            let mut real = State::default();
            let mut reference = Reference::default();
            for step in 0..200 {
                let pick = |paths: &[&'static str]| paths[rng.range(0..paths.len())];
                let op;
                let live = real.live();
                match rng.range(0..100u32) {
                    0..30 => {
                        let path = pick(&PATHS);
                        op = format!("create {path}");
                        if live.check_parents(path).is_ok()
                            && let Entry::Missing = live.entry(path)
                        {
                            let inode = Rc::new(RefCell::new(Inode::new(usage.clone())));
                            real.insert_file(path, inode.clone());
                            reference.live.create_parents(path);
                            reference.live.files.insert(path.to_owned(), inode);
                        }
                    }
                    30..60 => {
                        let (from, to) = (pick(&PATHS), pick(&PATHS));
                        op = format!("rename {from} {to}");
                        if let Entry::File(inode) = live.entry(from)
                            && live.check_parents(to).is_ok()
                            && !matches!(live.entry(to), Entry::Dir)
                            && from != to
                        {
                            real.remove_file(from);
                            real.insert_file(to, inode.clone());
                            reference.live.files.remove(from);
                            reference.live.create_parents(to);
                            reference.live.files.insert(to.to_owned(), inode);
                        }
                    }
                    60..75 => {
                        let path = pick(&PATHS);
                        op = format!("remove {path}");
                        if let Entry::File(_) = live.entry(path) {
                            real.remove_file(path);
                            reference.live.files.remove(path);
                        }
                    }
                    75..95 => {
                        let dir = pick(&DIRS);
                        op = format!("sync_dir {dir:?}");
                        if let Entry::Dir = live.entry(dir) {
                            real.sync_dir(dir);
                            reference.sync_dir(dir);
                        }
                    }
                    _ => {
                        op = "power_cut".to_owned();
                        let twin = Rng::from_state(rng.state());
                        real.power_cut(DiskId::new(0), &rng);
                        reference.power_cut(&twin, &real);
                        assert_eq!(rng.state(), twin.state(), "seed {seed} step {step}: draws");
                    }
                }
                let at = format!("seed {seed} step {step} ({op}):");
                assert_same(&real.live, &reference.live, &format!("{at} live"));
                assert_same(&real.durable, &reference.durable, &format!("{at} durable"));
                assert!(real.pending_is_exact(), "{at} `pending` isn't exact");
            }
        }
    }
}
