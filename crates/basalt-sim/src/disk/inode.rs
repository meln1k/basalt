//! A file's contents: what reads see, what survives a power cut, and what a power cut does to
//! the pages written in between.

use std::cell::Cell;
use std::collections::BTreeMap;
use std::rc::Rc;

use basalt_env::Rng;

/// The unit contents are stored in, and that a power cut keeps, drops, tears, or corrupts.
const PAGE: u64 = 4096;
const PAGE_BYTES: usize = PAGE as usize;
/// The unit a torn page mixes old and new data in.
const SECTOR: usize = 512;
const ZERO_PAGE: [u8; PAGE_BYTES] = [0; PAGE_BYTES];

/// How often a power cut keeps a pending change to a file's length.
const LENGTH_KEPT_PROBABILITY: f64 = 0.5;
/// How often a power cut keeps a pending drop-only page.
const DROP_ONLY_KEPT_PROBABILITY: f64 = 0.5;
/// How often a power cut keeps, drops, or tears a pending full-corruption page. It corrupts
/// the rest.
const FULL_KEPT_PROBABILITY: f64 = 0.4;
const FULL_DROPPED_PROBABILITY: f64 = 0.4;
const FULL_TORN_PROBABILITY: f64 = 0.1;
/// How often each sector of a torn page has its new data.
const TORN_SECTOR_NEW_PROBABILITY: f64 = 0.5;

/// What a power cut may do to a pending page of a file, picked when the file is opened. A page
/// written through several handles gets the most damaging of their modes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum CorruptionMode {
    /// Each pending page is kept or dropped whole.
    DropOnly,
    /// Each pending page may also be torn (some sectors new, some old) or corrupted.
    Full,
}

/// What a power cut did to one pending page.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PageFate {
    Kept,
    Dropped,
    /// Each sector new or old.
    Torn,
    /// Garbage.
    Corrupted,
}

impl PageFate {
    fn draw(mode: CorruptionMode, rng: &Rng) -> PageFate {
        match mode {
            CorruptionMode::DropOnly if rng.bool(DROP_ONLY_KEPT_PROBABILITY) => PageFate::Kept,
            CorruptionMode::DropOnly => PageFate::Dropped,
            CorruptionMode::Full => {
                let x = rng.f64();
                if x < FULL_KEPT_PROBABILITY {
                    PageFate::Kept
                } else if x < FULL_KEPT_PROBABILITY + FULL_DROPPED_PROBABILITY {
                    PageFate::Dropped
                } else if x < FULL_KEPT_PROBABILITY
                    + FULL_DROPPED_PROBABILITY
                    + FULL_TORN_PROBABILITY
                {
                    PageFate::Torn
                } else {
                    PageFate::Corrupted
                }
            }
        }
    }
}

/// What a power cut did to one file.
#[derive(Debug)]
pub(super) struct Cut {
    /// The most damaging mode among its pending pages.
    pub(super) worst_mode: CorruptionMode,
    pub(super) length_kept: bool,
    pub(super) kept: u32,
    pub(super) dropped: u32,
    pub(super) torn: u32,
    pub(super) corrupted: u32,
}

impl Cut {
    fn count(&mut self, fate: PageFate) {
        *match fate {
            PageFate::Kept => &mut self.kept,
            PageFate::Dropped => &mut self.dropped,
            PageFate::Torn => &mut self.torn,
            PageFate::Corrupted => &mut self.corrupted,
        } += 1;
    }
}

type Page = Box<[u8; PAGE_BYTES]>;

/// Pages by index. A missing page reads as zeros, so a sparse file costs only what's written,
/// and bytes past the length are always zero.
type Pages = BTreeMap<u64, Page>;

fn zero_page() -> Page {
    Box::new(ZERO_PAGE)
}

/// A file's contents: a live image that reads see and a durable image that survives a power
/// cut. The two differ only in the pages listed in `dirty`, so `sync` copies just those, unless
/// a failed sync lost some (`lost`).
///
/// Its live length counts toward its disk's `usage` for as long as the inode exists: while it
/// has a live or durable name, or an open handle, as on Linux.
pub(super) struct Inode {
    usage: Rc<Cell<u64>>,
    len: u64,
    pages: Pages,
    durable_len: u64,
    durable: Pages,
    /// The pages changed since the last sync, each with the most damaging mode of the handles
    /// that changed it.
    dirty: BTreeMap<u64, CorruptionMode>,
    /// Whether a failed sync marked pages clean without making them durable, so the live image
    /// may differ from the durable one outside `dirty` until a power cut.
    lost: bool,
}

impl Clone for Inode {
    fn clone(&self) -> Inode {
        self.usage.set(self.usage.get() + self.len);
        Inode {
            usage: self.usage.clone(),
            len: self.len,
            pages: self.pages.clone(),
            durable_len: self.durable_len,
            durable: self.durable.clone(),
            dirty: self.dirty.clone(),
            lost: self.lost,
        }
    }
}

impl Drop for Inode {
    fn drop(&mut self) {
        self.usage.set(self.usage.get() - self.len);
    }
}

impl Inode {
    /// An empty file whose length counts toward `usage`.
    pub(super) fn new(usage: Rc<Cell<u64>>) -> Inode {
        Inode {
            usage,
            len: 0,
            pages: Pages::new(),
            durable_len: 0,
            durable: Pages::new(),
            dirty: BTreeMap::new(),
            lost: false,
        }
    }

    /// The live length.
    pub(super) fn len(&self) -> u64 {
        self.len
    }

    /// Reads into `buf` from `offset` and returns how many bytes were read: fewer than
    /// `buf.len()` only at the end of the file.
    pub(super) fn read(&self, buf: &mut [u8], offset: u64) -> usize {
        let left = usize::try_from(self.len.saturating_sub(offset)).unwrap_or(usize::MAX);
        let n = buf.len().min(left);
        for (page, at, from, len) in chunks(offset, n) {
            let out = &mut buf[from..from + len];
            match self.pages.get(&page) {
                Some(p) => out.copy_from_slice(&p[at..at + len]),
                None => out.fill(0),
            }
        }
        n
    }

    /// Writes `buf` at `offset`, whose end the caller has checked fits in a `u64`.
    pub(super) fn write(&mut self, buf: &[u8], offset: u64, mode: CorruptionMode) {
        for (page, at, from, len) in chunks(offset, buf.len()) {
            let p = self.pages.entry(page).or_insert_with(zero_page);
            p[at..at + len].copy_from_slice(&buf[from..from + len]);
            self.mark(page, mode);
        }
        self.resize(self.len.max(offset + buf.len() as u64));
    }

    pub(super) fn set_len(&mut self, len: u64, mode: CorruptionMode) {
        // Growing changes no page: bytes past the old length were already zero.
        for page in truncate(&mut self.pages, len) {
            self.mark(page, mode);
        }
        self.resize(len);
    }

    /// Makes the live image durable, copying only the pages changed since the last sync.
    pub(super) fn sync(&mut self) {
        for page in std::mem::take(&mut self.dirty).into_keys() {
            put(&mut self.durable, page, self.pages.get(&page).cloned());
        }
        self.durable_len = self.len;
    }

    /// What a failed sync does on Linux: marks the pages changed since the last sync clean
    /// without making them durable. Reads still see them, a later sync doesn't write them, and
    /// a power cut reverts them. The length stays pending.
    pub(super) fn lose_dirty(&mut self) {
        self.lost |= !self.dirty.is_empty();
        self.dirty.clear();
    }

    /// Replaces both images with what survives a power cut, or returns `None` if nothing was
    /// pending.
    pub(super) fn power_cut(&mut self, rng: &Rng) -> Option<Cut> {
        let resized = self.len != self.durable_len;
        let worst_mode = self.dirty.values().copied().max();
        if worst_mode.is_none() && !resized && !self.lost {
            return None;
        }
        let length_kept = !resized || rng.bool(LENGTH_KEPT_PROBABILITY);
        let len = if length_kept {
            self.len
        } else {
            self.durable_len
        };
        let mut cut = Cut {
            worst_mode: worst_mode.unwrap_or(CorruptionMode::DropOnly),
            length_kept,
            kept: 0,
            dropped: 0,
            torn: 0,
            corrupted: 0,
        };
        for (&page, &mode) in &self.dirty {
            if page * PAGE >= len {
                break;
            }
            let fate = PageFate::draw(mode, rng);
            cut.count(fate);
            let new = self.pages.get(&page);
            match fate {
                PageFate::Kept => put(&mut self.durable, page, new.cloned()),
                PageFate::Dropped => {}
                PageFate::Torn => {
                    let new = new.map_or(&ZERO_PAGE, |p| &**p);
                    let torn = self.durable.entry(page).or_insert_with(zero_page);
                    for s in (0..PAGE_BYTES).step_by(SECTOR) {
                        if rng.bool(TORN_SECTOR_NEW_PROBABILITY) {
                            torn[s..s + SECTOR].copy_from_slice(&new[s..s + SECTOR]);
                        }
                    }
                }
                PageFate::Corrupted => {
                    let mut garbage = zero_page();
                    for bytes in garbage.chunks_mut(8) {
                        bytes.copy_from_slice(&rng.u64().to_le_bytes());
                    }
                    self.durable.insert(page, garbage);
                }
            }
        }
        truncate(&mut self.durable, len);
        self.durable_len = len;
        self.dirty.clear();
        self.lost = false;
        self.pages.clone_from(&self.durable);
        self.resize(len);
        Some(cut)
    }

    /// Sets the live length, keeping the disk's usage in step.
    fn resize(&mut self, len: u64) {
        self.usage.set(self.usage.get() - self.len + len);
        self.len = len;
    }

    fn mark(&mut self, page: u64, mode: CorruptionMode) {
        let m = self.dirty.entry(page).or_insert(mode);
        *m = (*m).max(mode);
    }
}

/// Splits the `len` bytes from `offset` at page boundaries: for each piece, its page, its
/// offset in the page, its offset in the run, and its length.
fn chunks(offset: u64, len: usize) -> impl Iterator<Item = (u64, usize, usize, usize)> {
    let mut done = 0;
    std::iter::from_fn(move || {
        if done == len {
            return None;
        }
        let pos = offset + done as u64;
        let at = (pos % PAGE) as usize;
        let n = (PAGE_BYTES - at).min(len - done);
        let piece = (pos / PAGE, at, done, n);
        done += n;
        Some(piece)
    })
}

/// Cuts `pages` to `len` bytes and returns the pages that changed.
fn truncate(pages: &mut Pages, len: u64) -> Vec<u64> {
    let mut changed: Vec<u64> = pages.split_off(&len.div_ceil(PAGE)).into_keys().collect();
    if !len.is_multiple_of(PAGE)
        && let Some(page) = pages.get_mut(&(len / PAGE))
    {
        page[(len % PAGE) as usize..].fill(0);
        changed.push(len / PAGE);
    }
    changed
}

fn put(pages: &mut Pages, index: u64, page: Option<Page>) {
    match page {
        Some(page) => pages.insert(index, page),
        None => pages.remove(&index),
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A page written through handles of both modes gets the more damaging one, whichever
    /// wrote last.
    #[test]
    fn a_page_gets_the_worst_mode_of_its_writers() {
        for modes in [
            [CorruptionMode::Full, CorruptionMode::DropOnly],
            [CorruptionMode::DropOnly, CorruptionMode::Full],
        ] {
            let mut inode = Inode::new(Rc::default());
            for mode in modes {
                inode.write(&[1; 10], 0, mode);
            }
            let cut = inode.power_cut(&Rng::new(1)).unwrap();
            assert_eq!(cut.worst_mode, CorruptionMode::Full, "{modes:?}");
        }
    }
}
