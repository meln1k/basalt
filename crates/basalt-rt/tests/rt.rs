//! `basalt-rt` against the real OS: sockets, files, timers, the compute pool, priorities, and
//! the production ambient world.

// Tests reach the real world to check what the runtime did.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::cell::{Cell, RefCell};
use std::future::{Future, poll_fn};
use std::io::{self, Write};
use std::path::PathBuf;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

use basalt_env::{
    Cost, DiskId, Duration, Elapsed, Knobs, OpenOptions, Priority, Severity, SocketAddr, Stream,
};
use basalt_rt::{Config, Runtime};
use futures_io::{AsyncRead, AsyncWrite};

fn runtime() -> Runtime {
    Runtime::new(Config::default()).unwrap()
}

/// Runs `future` on `rt`, failing the test instead of hanging if it takes 10 seconds.
fn run<F: Future>(rt: &Runtime, future: F) -> F::Output {
    let ctx = rt.ctx();
    rt.block_on(ctx.timeout(Duration::from_secs(10), future))
        .expect("the test timed out")
}

/// Sets its flag when dropped.
struct SetOnDrop(Rc<Cell<bool>>);

impl Drop for SetOnDrop {
    fn drop(&mut self) {
        self.0.set(true);
    }
}

/// A fresh directory for one test, removed when dropped.
struct TempDir(PathBuf);

impl TempDir {
    fn new(name: &str) -> TempDir {
        let dir = std::env::temp_dir().join(format!("basalt-rt-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        TempDir(dir)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

async fn write_all(stream: &mut Stream, mut buf: &[u8]) -> io::Result<()> {
    while !buf.is_empty() {
        let n = poll_fn(|cx| Pin::new(&mut *stream).poll_write(cx, buf)).await?;
        buf = &buf[n..];
    }
    Ok(())
}

async fn read_to_end(stream: &mut Stream) -> io::Result<Vec<u8>> {
    let mut out = Vec::new();
    let mut chunk = [0; 7];
    loop {
        let n = poll_fn(|cx| Pin::new(&mut *stream).poll_read(cx, &mut chunk)).await?;
        if n == 0 {
            return Ok(out);
        }
        out.extend_from_slice(&chunk[..n]);
    }
}

async fn close(stream: &mut Stream) -> io::Result<()> {
    poll_fn(|cx| Pin::new(&mut *stream).poll_close(cx)).await
}

#[test]
fn a_listener_and_a_connection_exchange_bytes_on_localhost() {
    let rt = runtime();
    let ctx = rt.ctx();
    let message: Vec<u8> = (0..100_000u32).map(|i| (i % 251) as u8).collect();
    let (echoed, server_saw) = run(&rt, async {
        let listener = ctx.listen(0).await.unwrap();
        let port = listener.local_addr().port();
        // The server echoes everything until the client closes its write half.
        let server = ctx.spawn("server", Priority::Default, async move {
            let (mut stream, peer) = listener.accept().await.unwrap();
            assert_eq!(peer, stream.peer_addr());
            let got = read_to_end(&mut stream).await.unwrap();
            write_all(&mut stream, &got).await.unwrap();
            close(&mut stream).await.unwrap();
            got.len()
        });
        let addr = SocketAddr::from(([127, 0, 0, 1], port));
        let mut client = ctx.connect(addr).await.unwrap();
        assert_eq!(client.peer_addr(), addr);
        write_all(&mut client, &message).await.unwrap();
        close(&mut client).await.unwrap();
        let echoed = read_to_end(&mut client).await.unwrap();
        (echoed, server.await)
    });
    assert_eq!(server_saw, message.len());
    assert!(echoed == message, "the echo differs from what was sent");
}

#[test]
fn accept_survives_a_peer_that_resets_before_it_is_accepted() {
    let rt = runtime();
    let ctx = rt.ctx();
    run(&rt, async {
        let listener = ctx.listen(0).await.unwrap();
        let addr = SocketAddr::from(([127, 0, 0, 1], listener.local_addr().port()));
        // Connect and reset before the server accepts.
        let bad = std::net::TcpStream::connect(addr).unwrap();
        rustix::net::sockopt::set_socket_linger(&bad, Some(std::time::Duration::ZERO)).unwrap();
        drop(bad);
        ctx.sleep(Duration::from_millis(50)).await;

        let good = ctx.connect(addr).await.unwrap();
        loop {
            let (_, peer) = listener.accept().await.unwrap();
            if peer == good.local_addr() {
                break;
            }
        }
    });
}

#[test]
fn writing_to_a_closed_peer_reports_connection_reset() {
    let rt = runtime();
    let ctx = rt.ctx();
    let err = run(&rt, async {
        let listener = ctx.listen(0).await.unwrap();
        let addr = SocketAddr::from(([127, 0, 0, 1], listener.local_addr().port()));
        let mut client = ctx.connect(addr).await.unwrap();
        drop(listener.accept().await.unwrap());
        loop {
            if let Err(e) = write_all(&mut client, b"hello").await {
                break e;
            }
            ctx.sleep(Duration::from_millis(10)).await;
        }
    });
    assert_eq!(err.kind(), io::ErrorKind::ConnectionReset, "{err}");
}

#[test]
fn connecting_where_nothing_listens_is_refused() {
    let rt = runtime();
    let ctx = rt.ctx();
    let err = run(&rt, async {
        // Bind a port, then free it, so nothing listens there.
        let port = ctx.listen(0).await.unwrap().local_addr().port();
        ctx.connect(SocketAddr::from(([127, 0, 0, 1], port)))
            .await
            .unwrap_err()
    });
    assert_eq!(err.kind(), io::ErrorKind::ConnectionRefused);
}

#[test]
fn a_file_write_sync_and_read_round_trip_and_survive_a_restart() {
    let dir = TempDir::new("files");
    let config = || Config {
        disks: vec![dir.0.clone()],
        ..Config::default()
    };
    let data: Vec<u8> = (0..10_000u32).map(|i| (i * 7) as u8).collect();

    let rt = Runtime::new(config()).unwrap();
    let ctx = rt.ctx();
    run(&rt, async {
        assert_eq!(&*ctx.disks(), [DiskId::new(0)]);
        let disk = ctx.disk(DiskId::new(0));
        let opts = OpenOptions::new().read(true).write(true).create_new(true);
        // Directories are implicit: creating the file creates `log/` too.
        let file = disk.open("log/tmp", opts).await.unwrap();
        let (result, _) = file.write_at(data.clone(), 0).await;
        result.unwrap();
        let (result, _) = file.write_at(b"tail".to_vec(), 20_000).await;
        result.unwrap();
        file.sync().await.unwrap();
        assert_eq!(file.len(), 20_004);

        let (n, buf) = file.read_at(vec![0; 10_000], 0).await;
        assert_eq!(n.unwrap(), 10_000);
        assert!(buf == data);
        // A short read only at the end of the file, and the gap reads as zeros.
        let (n, buf) = file.read_at(vec![9; 8], 19_998).await;
        assert_eq!(n.unwrap(), 6);
        assert_eq!(buf, [0, 0, b't', b'a', b'i', b'l', 9, 9]);

        let err = disk.open("log/tmp", opts).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);
        disk.rename("log/tmp", "log/current").await.unwrap();
        disk.sync_dir("log").await.unwrap();
        assert_eq!(disk.list("log").await.unwrap(), ["current"]);
        assert_eq!(disk.list("").await.unwrap(), Vec::<String>::new());
        assert_eq!(disk.list("missing").await.unwrap(), Vec::<String>::new());
        disk.sync_dir("missing").await.unwrap();
        assert!(disk.space().total > 0);
    });
    drop(rt);

    let rt = Runtime::new(config()).unwrap();
    let ctx = rt.ctx();
    run(&rt, async {
        let disk = ctx.disk(DiskId::new(0));
        let file = disk
            .open("log/current", OpenOptions::new().read(true))
            .await
            .unwrap();
        let (n, buf) = file.read_at(vec![0; 10_000], 0).await;
        assert_eq!(n.unwrap(), 10_000);
        assert!(buf == data);
        disk.remove("log/current").await.unwrap();
        let err = disk.remove("log/current").await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
        for bad in ["/etc/passwd", "../x", "a//b", "a/./b", "", "a/"] {
            let err = disk
                .open(bad, OpenOptions::new().read(true))
                .await
                .unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::InvalidInput, "{bad:?}");
        }
    });
}

/// A disk operation is submitted when called, so dropping its future before polling it
/// doesn't stop it: the I/O pool runs it anyway.
#[test]
fn a_dropped_disk_operation_still_runs() {
    let dir = TempDir::new("dropped");
    let rt = Runtime::new(Config {
        disks: vec![dir.0.clone()],
        ..Config::default()
    })
    .unwrap();
    let ctx = rt.ctx();
    run(&rt, async {
        let disk = ctx.disk(DiskId::new(0));
        let create = OpenOptions::new().read(true).write(true).create(true);
        let file = disk.open("a", create).await.unwrap();
        drop(file.write_at(b"one".to_vec(), 0));
        drop(disk.open("b", create));
        // The pool runs them in its own time; wait until both have landed.
        let landed = async {
            loop {
                let (n, buf) = file.read_at(vec![0; 3], 0).await;
                if n.unwrap() == 3 && buf == b"one" && disk.list("").await.unwrap() == ["a", "b"] {
                    return;
                }
                ctx.sleep(Duration::from_millis(1)).await;
            }
        };
        ctx.timeout(Duration::from_secs(5), landed).await.unwrap();
    });
}

fn kind<T>(result: io::Result<T>) -> io::ErrorKind {
    result.map(drop).unwrap_err().kind()
}

#[test]
fn directories_are_implicit() {
    let dir = TempDir::new("dirs");
    let rt = Runtime::new(Config {
        disks: vec![dir.0.clone()],
        ..Config::default()
    })
    .unwrap();
    let ctx = rt.ctx();
    run(&rt, async {
        let disk = ctx.disk(DiskId::new(0));
        let create = OpenOptions::new().write(true).create(true);
        for name in ["d/c", "d/a", "d/sub/x", "d/b"] {
            disk.open(name, create).await.unwrap();
        }
        // Renaming creates the directories above the new name.
        disk.rename("d/b", "e/f/b").await.unwrap();
        // Files only, sorted, without the subdirectory.
        assert_eq!(disk.list("d").await.unwrap(), ["a", "c"]);
        assert_eq!(disk.list("e/f").await.unwrap(), ["b"]);
        assert_eq!(disk.list("e").await.unwrap(), Vec::<String>::new());
        disk.sync_dir("e/f").await.unwrap();
        disk.sync_dir("d").await.unwrap();

        // A directory isn't a file, and a file isn't a directory.
        let read = OpenOptions::new().read(true);
        assert_eq!(
            kind(disk.open("d", read).await),
            io::ErrorKind::IsADirectory
        );
        assert_eq!(
            kind(disk.open("e", create).await),
            io::ErrorKind::IsADirectory
        );
        assert_eq!(
            kind(disk.rename("d/a", "e/f").await),
            io::ErrorKind::IsADirectory
        );
        assert_eq!(
            kind(disk.rename("e/f", "x").await),
            io::ErrorKind::IsADirectory
        );
        assert_eq!(
            kind(disk.remove("d/sub").await),
            io::ErrorKind::IsADirectory
        );
        assert_eq!(
            kind(disk.open("d/a/x", create).await),
            io::ErrorKind::NotADirectory
        );
        assert_eq!(
            kind(disk.open("d/a/x", read).await),
            io::ErrorKind::NotADirectory
        );
        assert_eq!(kind(disk.list("d/a").await), io::ErrorKind::NotADirectory);
        assert_eq!(
            kind(disk.sync_dir("d/a").await),
            io::ErrorKind::NotADirectory
        );
    });
}

#[test]
fn sleep_and_timeout_fire() {
    let rt = runtime();
    let ctx = rt.ctx();
    run(&rt, async {
        let start = ctx.now();
        ctx.sleep(Duration::from_millis(50)).await;
        assert!(ctx.now() - start >= Duration::from_millis(50));

        let start = ctx.now();
        let slow = ctx.timeout(
            Duration::from_millis(20),
            ctx.sleep(Duration::from_secs(60)),
        );
        assert_eq!(slow.await, Err(Elapsed));
        let waited = ctx.now() - start;
        assert!(waited >= Duration::from_millis(20), "{waited:?}");
        assert!(waited < Duration::from_secs(10), "{waited:?}");

        let quick = ctx.timeout(Duration::from_secs(60), async { 7 });
        assert_eq!(quick.await, Ok(7));
    });
}

#[test]
fn compute_runs_off_thread_with_no_ambient_world() {
    let knobs = Knobs {
        max_buggified_delay: Duration::from_millis(1),
    };
    let rt = Runtime::new(Config {
        knobs: knobs.clone(),
        ..Config::default()
    })
    .unwrap();
    assert_eq!(*basalt_env::knobs(), knobs);
    let ctx = rt.ctx();
    let (thread_id, job_knobs) = run(
        &rt,
        ctx.compute(Cost::ZERO, || {
            (thread::current().id(), (*basalt_env::knobs()).clone())
        }),
    );
    assert_ne!(thread_id, thread::current().id());
    assert_eq!(job_knobs, Knobs::default());
}

#[test]
fn higher_priorities_run_first_and_equal_ones_in_order() {
    let rt = runtime();
    let ctx = rt.ctx();
    let ran = Rc::new(RefCell::new(Vec::new()));
    let spawn = |name, priority| {
        let ran = ran.clone();
        ctx.spawn(name, priority, async move { ran.borrow_mut().push(name) })
    };
    let tasks = [
        spawn("background", Priority::Background),
        spawn("default 1", Priority::Default),
        spawn("coordination", Priority::Coordination),
        spawn("default 2", Priority::Default),
        spawn("replication", Priority::Replication),
    ];
    run(&rt, async {
        for task in tasks {
            task.await;
        }
    });
    assert_eq!(
        *ran.borrow(),
        [
            "coordination",
            "replication",
            "default 1",
            "default 2",
            "background"
        ]
    );
}

#[test]
fn dropping_the_runtime_drops_detached_tasks() {
    let rt = runtime();
    let ctx = rt.ctx();
    let dropped = Rc::new(Cell::new(false));
    let guard = SetOnDrop(dropped.clone());
    let parked = ctx.clone();
    ctx.spawn("parked", Priority::Default, async move {
        let _guard = guard;
        parked.sleep(Duration::MAX).await;
    })
    .detach();
    run(&rt, ctx.yield_now());
    assert!(!dropped.get());
    drop(rt);
    assert!(dropped.get());
}

#[test]
fn dropping_the_runtime_waits_for_running_jobs_and_drops_the_tasks_they_wake() {
    let rt = runtime();
    let ctx = rt.ctx();
    let dropped = Rc::new(Cell::new(false));
    let guard = SetOnDrop(dropped.clone());
    let started = Arc::new(AtomicBool::new(false));
    let finished = Arc::new(AtomicBool::new(false));
    let (job_started, job_finished) = (started.clone(), finished.clone());
    let worker = ctx.clone();
    // Someone else holds the handle, so only the runtime can drop the task.
    let task = ctx.spawn("waits on compute", Priority::Default, async move {
        let _guard = guard;
        worker
            .compute(Cost::ZERO, move || {
                job_started.store(true, Ordering::SeqCst);
                thread::sleep(std::time::Duration::from_millis(200));
                job_finished.store(true, Ordering::SeqCst);
            })
            .await;
    });
    run(&rt, async {
        while !started.load(Ordering::SeqCst) {
            ctx.sleep(Duration::from_millis(1)).await;
        }
    });
    drop(rt);
    assert!(
        finished.load(Ordering::SeqCst),
        "shutdown didn't wait for the job"
    );
    assert!(dropped.get(), "the task the job woke was leaked");
    drop(task);
}

#[test]
fn a_task_woken_from_a_pool_thread_runs() {
    let rt = runtime();
    let ctx = rt.ctx();
    let worker = ctx.clone();
    // The main future waits on the task, not on the pool, so only the pool thread's wake of
    // the task, through the run queue, can finish it.
    let task = ctx.spawn("waits on compute", Priority::Default, async move {
        worker
            .compute(Cost::ZERO, || {
                thread::sleep(std::time::Duration::from_millis(20));
                7
            })
            .await
    });
    assert_eq!(run(&rt, task), 7);
}

/// A log the test can read back.
#[derive(Clone, Default)]
struct SharedLog(Rc<RefCell<Vec<u8>>>);

impl Write for SharedLog {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.borrow_mut().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn the_production_world_logs_traces_counts_probes_and_never_buggifies() {
    let log = SharedLog::default();
    let rt = Runtime::new(Config {
        log: Box::new(log.clone()),
        min_severity: Severity::Info,
        ..Config::default()
    })
    .unwrap();
    assert!(!basalt_env::is_simulated());
    assert!((0..1_000).all(|_| !basalt_env::buggify!(1.0)));
    for _ in 0..3 {
        basalt_env::probe!("rt.test_probe");
    }
    basalt_env::trace!(Debug, "Hidden", x = 1);
    basalt_env::trace!(Warn, "Shown", ts = 3, name = "a");
    assert_eq!(rt.probes(), [("rt.test_probe", 3)]);

    let log = String::from_utf8(log.0.borrow().clone()).unwrap();
    assert_eq!(log.lines().count(), 1, "{log}");
    assert!(log.ends_with(" Warn Shown ts=3 name=\"a\"\n"), "{log}");
}
