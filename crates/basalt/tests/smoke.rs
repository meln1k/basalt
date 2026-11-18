//! The register as 3 real `basalt server` processes on localhost, driven by the same
//! `toy-register` client the simulator uses, here running on `basalt-rt` in the test process.

#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "this test starts real server processes and reads their logs on the host"
)]

use std::fs::{self, OpenOptions};
use std::net::{Ipv4Addr, SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::process::{Child, Command};

use basalt_env::{Duration, Severity};
use basalt_rt::{Config, Runtime};
use toy_register::{Bugs, Client};

/// Each step finishes in well under a second on localhost; this only keeps a broken build from
/// hanging CI.
const STEP_DEADLINE: Duration = Duration::from_secs(30);

/// One `basalt server` child process, killed when dropped.
struct Server {
    child: Child,
    log: PathBuf,
}

impl Server {
    /// Starts replica `i` on its data directory in `dir`, appending its trace to its log there,
    /// and returns once it is listening.
    fn start(dir: &Path, i: usize, port: u16) -> Server {
        let log = dir.join(format!("replica-{i}.log"));
        let stderr = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log)
            .unwrap();
        let child = Command::new(env!("CARGO_BIN_EXE_basalt"))
            .args(["server", "--port", &port.to_string(), "--data-dir"])
            .arg(dir.join(format!("replica-{i}")))
            .stderr(stderr)
            .spawn()
            .unwrap();
        let mut server = Server { child, log };
        server.wait_listening(i);
        server
    }

    /// Waits until this start of the server has traced `ReplicaListening`, failing if it exits
    /// or takes longer than the deadline.
    fn wait_listening(&mut self, i: usize) {
        let before = self.read_log().matches("ReplicaListening").count();
        let deadline = std::time::Instant::now() + STEP_DEADLINE;
        loop {
            let log = self.read_log();
            if log.matches("ReplicaListening").count() > before {
                return;
            }
            if let Some(status) = self.child.try_wait().unwrap() {
                panic!("replica {i} exited with {status} before listening:\n{log}");
            }
            assert!(
                std::time::Instant::now() < deadline,
                "replica {i} isn't listening:\n{log}"
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }

    fn read_log(&self) -> String {
        fs::read_to_string(&self.log).unwrap_or_default()
    }

    fn kill(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.kill();
    }
}

/// A fresh directory for one test, removed when dropped, unless the test failed: then it is
/// kept, with the servers' logs, and its path printed.
struct TempDir(PathBuf);

impl TempDir {
    fn new(name: &str) -> TempDir {
        let dir = std::env::temp_dir().join(format!("basalt-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        TempDir(dir)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        if std::thread::panicking() {
            eprintln!("kept the servers' data and logs in {}", self.0.display());
        } else {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
}

/// `n` distinct ports nothing is listening on now. Another process could take one before its
/// server binds it, which would fail the test rather than pass it wrongly.
fn free_ports(n: usize) -> Vec<u16> {
    // All held open at once, so the OS can't hand out the same port twice.
    let probes: Vec<TcpListener> = (0..n)
        .map(|_| TcpListener::bind((Ipv4Addr::UNSPECIFIED, 0)).unwrap())
        .collect();
    probes
        .iter()
        .map(|l| l.local_addr().unwrap().port())
        .collect()
}

#[test]
fn register_runs_as_3_real_processes() {
    // Declared before the servers, so they are killed before their logs are removed.
    let dir = TempDir::new("smoke");
    let ports = free_ports(3);
    let mut servers: Vec<Server> = ports
        .iter()
        .enumerate()
        .map(|(i, &port)| Server::start(&dir.0, i, port))
        .collect();

    let rt = Runtime::new(Config {
        log: Box::new(std::io::sink()),
        min_severity: Severity::Warn,
        ..Config::default()
    })
    .unwrap();
    let ctx = rt.ctx();
    let addrs = ports
        .iter()
        .map(|&port| SocketAddr::from((Ipv4Addr::LOCALHOST, port)))
        .collect();
    let mut client = Client::new(ctx.clone(), addrs, 1, Bugs::default());

    rt.block_on(async {
        ctx.timeout(STEP_DEADLINE, async {
            assert_eq!(client.read(1).await, None, "a key never written");
            client.write(1, b"one".to_vec()).await;
            assert_eq!(client.read(1).await.as_deref(), Some(&b"one"[..]));
        })
        .await
        .expect("writes and reads finish with every replica up");
    });

    // A majority is still up, so reads and writes still complete.
    servers[0].kill();
    rt.block_on(async {
        ctx.timeout(STEP_DEADLINE, async {
            assert_eq!(client.read(1).await.as_deref(), Some(&b"one"[..]));
            client.write(1, b"two".to_vec()).await;
            assert_eq!(client.read(1).await.as_deref(), Some(&b"two"[..]));
        })
        .await
        .expect("reads and writes finish with one replica down");
    });

    // Every replica restarts on its data directory, so the value can only come from a record a
    // replica reloaded from disk. Replica 0 is already down.
    for server in &mut servers[1..] {
        server.kill();
    }
    for (i, server) in servers.iter_mut().enumerate() {
        *server = Server::start(&dir.0, i, ports[i]);
    }
    rt.block_on(async {
        ctx.timeout(STEP_DEADLINE, async {
            assert_eq!(client.read(1).await.as_deref(), Some(&b"two"[..]));
        })
        .await
        .expect("a read finishes after every replica restarts");
    });
}
