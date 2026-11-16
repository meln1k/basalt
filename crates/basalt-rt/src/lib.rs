//! The production backend: real clocks, sockets, disks, and threads behind
//! `basalt_env::backend::Backend`.
//!
//! A [`Runtime`] runs one process on the thread that creates it. Its executor follows the
//! simulator's priority rules: a ready task of a higher [`Priority`](basalt_env::Priority) runs
//! before one of a lower priority, and equal priorities run in the order they were woken. Sockets
//! and timers use the `async-io` reactor. File I/O runs on a pool of threads, and
//! [`Ctx::compute`] jobs on another, both with no ambient world.
//!
//! # Disks
//!
//! Each disk is a directory on the host, from [`Config::disks`]. The `Disk` API has no
//! `create_dir`, so directories are implicit, and `basalt-sim` follows the same rules:
//!
//! - A path is relative to the disk's root: non-empty components joined by `/`, with no `.` or
//!   `..`. Anything else fails with `InvalidInput`. A directory argument may also be `""`, the
//!   root.
//! - Opening a file with `create` or `create_new`, or renaming a file, creates the missing
//!   directories above it.
//! - `sync_dir(d)` makes durable the creating, renaming, and removing of files directly in `d`,
//!   and every directory on the path to `d`, including ones created above. Nothing else about
//!   directories is durable before that. A rename between two directories needs `sync_dir` of
//!   both. `sync_dir` of a directory that doesn't exist does nothing, since nothing in it can be
//!   lost.
//! - `list` returns the names of the files directly in a directory, sorted, without
//!   subdirectories. A directory with no files lists as empty, whether or not it exists on the
//!   host.
//! - `remove` removes files only, and leaves empty directories behind.
//! - A name is a file or a directory, never both. Opening, renaming, or removing a directory as
//!   a file fails with `IsADirectory`, and a path through a file fails with `NotADirectory`.
//!
//! # Example
//!
//! ```no_run
//! use basalt_rt::{Config, Runtime};
//!
//! let runtime = Runtime::new(Config {
//!     disks: vec!["/var/lib/basalt".into()],
//!     ..Config::default()
//! })?;
//! let ctx = runtime.ctx();
//! runtime.block_on(async move {
//!     let listener = ctx.listen(7000).await?;
//!     // ...
//!     # drop(listener);
//!     std::io::Result::Ok(())
//! })?;
//! # std::io::Result::Ok(())
//! ```

// The one crate that reaches the real world directly, so the determinism lints don't apply.
#![allow(
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::disallowed_types
)]

mod backend;
mod disk;
mod net;
mod pool;
mod queue;
mod world;

use std::cell::Cell;
use std::fs;
use std::future::{Future, poll_fn};
use std::io::{self, Write};
use std::path::PathBuf;
use std::pin::pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Wake, Waker};
use std::thread;

use basalt_env::world::WorldGuard;
use basalt_env::{Ctx, Knobs, ProcessId, Rng, Severity};

use crate::backend::RtBackend;
use crate::pool::Pool;
use crate::world::ProductionWorld;

/// How many tasks the executor runs before it lets the reactor check for I/O and timers.
const BATCH: usize = 64;

/// What a [`Runtime`] starts with.
pub struct Config {
    /// Each disk's root directory on the host, created if missing. `DiskId::new(i)` is the
    /// `i`-th. See [Disks](crate#disks) for how paths map to files under it.
    pub disks: Vec<PathBuf>,
    /// The knobs: the defaults plus config.
    pub knobs: Knobs,
    /// Where `trace!` events go, one line each.
    pub log: Box<dyn Write>,
    /// `trace!` events below this severity are dropped.
    pub min_severity: Severity,
    /// Threads for blocking file I/O, at least 1.
    pub io_threads: usize,
    /// Threads for [`Ctx::compute`] jobs, at least 1.
    pub compute_threads: usize,
}

impl Default for Config {
    /// No disks, default knobs, `Info` and above to stderr, 4 I/O threads, and a compute thread
    /// per core.
    fn default() -> Config {
        Config {
            disks: Vec::new(),
            knobs: Knobs::default(),
            log: Box::new(io::stderr()),
            min_severity: Severity::Info,
            io_threads: 4,
            compute_threads: thread::available_parallelism().map_or(1, |n| n.get()),
        }
    }
}

/// A production world with one process, running on the thread that created it.
///
/// Creating it installs the production ambient world on this thread, so it panics if a world is
/// already installed. Dropping it drops every task it still holds and stops its threads.
pub struct Runtime {
    backend: Rc<RtBackend>,
    world: Rc<ProductionWorld>,
    running: Cell<bool>,
    _guard: WorldGuard,
}

impl Runtime {
    /// Fails if a disk's root can't be created, a pool has no threads, or a thread can't start.
    pub fn new(config: Config) -> io::Result<Runtime> {
        for root in &config.disks {
            fs::create_dir_all(root)?;
        }
        let rng = entropy_rng()?;
        let world = Rc::new(ProductionWorld::new(
            config.knobs,
            config.log,
            config.min_severity,
        ));
        let guard = basalt_env::world::install(world.clone());
        // Threads last, so nothing above that fails or panics leaves them running.
        let io_pool = Pool::new("basalt-io", config.io_threads)?;
        let compute_pool = match Pool::new("basalt-compute", config.compute_threads) {
            Ok(pool) => pool,
            Err(e) => {
                io_pool.close();
                return Err(e);
            }
        };
        let disks = config
            .disks
            .into_iter()
            .map(|root| disk::Disk::new(root, io_pool.clone()))
            .collect();
        let backend = Rc::new(RtBackend::new(disks, io_pool, compute_pool, rng));
        Ok(Runtime {
            backend,
            world,
            running: Cell::new(false),
            _guard: guard,
        })
    }

    /// The process's context.
    pub fn ctx(&self) -> Ctx {
        Ctx::new(self.backend.clone(), ProcessId::new(0))
    }

    /// Runs the executor until `future` completes, polling `future` itself on this thread.
    /// Tasks spawned through [`ctx`](Runtime::ctx) run meanwhile; those still alive when it
    /// returns resume on the next call. `future` is polled only when woken, ahead of the tasks.
    /// Panics if called from inside `future` or a task.
    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        assert!(
            !self.running.replace(true),
            "Runtime::block_on called from inside the runtime"
        );
        let _running = ResetOnDrop(&self.running);
        let queue = &self.backend.queue;
        let mut future = pin!(future);
        let mut main: Option<(Arc<MainWake>, Waker)> = None;
        let output = async_io::block_on(poll_fn(|cx| {
            queue.set_waker(Some(cx.waker()));
            // `future` gets its own waker, so it's polled only when something woke it.
            let (wake, waker) = match &main {
                Some((wake, waker)) if wake.outer.will_wake(cx.waker()) => (wake, waker),
                _ => {
                    let wake = Arc::new(MainWake {
                        woken: AtomicBool::new(true),
                        outer: cx.waker().clone(),
                    });
                    let waker = Waker::from(wake.clone());
                    let (wake, waker) = main.insert((wake, waker));
                    (&*wake, &*waker)
                }
            };
            if wake.woken.swap(false, Ordering::SeqCst)
                && let Poll::Ready(output) = future.as_mut().poll(&mut Context::from_waker(waker))
            {
                return Poll::Ready(output);
            }
            let mut ran = 0;
            while ran < BATCH {
                let Some(runnable) = queue.pop() else { break };
                runnable.run();
                ran += 1;
            }
            if ran > 0 {
                // Come back after the reactor has checked for I/O and timers. If nothing ran,
                // the reactor, a timer, or a pool thread wakes us.
                cx.waker().wake_by_ref();
            }
            Poll::Pending
        }));
        queue.set_waker(None);
        output
    }

    /// How often each `probe!` has been reached, by name.
    pub fn probes(&self) -> Vec<(&'static str, u64)> {
        self.world.probes()
    }
}

impl Drop for Runtime {
    fn drop(&mut self) {
        // While the world is still installed, so `Drop` code can still trace.
        self.backend.shutdown();
    }
}

/// The main future's waker: notes the wake, then wakes `block_on`.
struct MainWake {
    woken: AtomicBool,
    outer: Waker,
}

impl Wake for MainWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.woken.store(true, Ordering::SeqCst);
        self.outer.wake_by_ref();
    }
}

struct ResetOnDrop<'a>(&'a Cell<bool>);

impl Drop for ResetOnDrop<'_> {
    fn drop(&mut self) {
        self.0.set(false);
    }
}

/// A generator seeded with 256 bits from the OS.
fn entropy_rng() -> io::Result<Rng> {
    loop {
        let mut bytes = [0u8; 32];
        let mut filled = 0;
        while filled < bytes.len() {
            filled += rustix::rand::getrandom(
                &mut bytes[filled..],
                rustix::rand::GetRandomFlags::empty(),
            )?;
        }
        let word = |i: usize| u64::from_le_bytes(bytes[i * 8..i * 8 + 8].try_into().unwrap());
        let state = [word(0), word(1), word(2), word(3)];
        // The one state xoshiro can't use; drawing it is a 2^-256 event.
        if state != [0; 4] {
            return Ok(Rng::from_state(state));
        }
    }
}
