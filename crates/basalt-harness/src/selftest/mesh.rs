//! The fault workloads' self-test: nodes on five machines ping each other and write their disks
//! while every fault workload the seed enables strikes. Once the faults are lifted, every
//! machine must be up, every node must reach every other, and every disk must take a synced
//! write and read it back.

use std::future::poll_fn;
use std::io;
use std::pin::Pin;
use std::rc::Rc;

use basalt_env::{Ctx, Duration, OpenOptions, Priority, SocketAddr, Stream};
use basalt_sim::SimControl;
use futures_io::{AsyncRead, AsyncWrite};

use crate::faults;
use crate::workload::{Process, TestSpec, Workload, WorkloadResult, Workloads};

/// Every fault workload against a mesh of nodes; every seed passes.
pub const FAULTS: TestSpec = TestSpec {
    name: "faults",
    about: "every fault workload against nodes that ping each other and write their disks \
            (harness self-test)",
    chaos: Duration::from_secs(30),
    quiet: Duration::from_secs(60),
    build,
};

const NODES: usize = 5;
const PORT: u16 = 4500;

/// Requests, each one byte. A node answers an echo with `ECHO`, and a check with a status.
const ECHO: u8 = b'e';
const CHECK: u8 = b'c';

/// A check's status: passed, the node's disk failed, or `PEER + j`: node `j` didn't answer.
const PASSED: u8 = 0;
const DISK_FAILED: u8 = 1;
const PEER: u8 = 2;

/// How many times the quiet phase asks each node for a check, since a node just rebooted may
/// not listen yet.
const CHECK_ATTEMPTS: usize = 3;

fn build(sim: &SimControl) -> Workloads {
    let machines: Vec<_> = (0..NODES)
        .map(|i| sim.add_machine(&format!("zone{i}"), 1))
        .collect();
    let peers: Rc<[SocketAddr]> = machines
        .iter()
        .map(|&m| SocketAddr::new(sim.ip(m), PORT))
        .collect();
    for (i, &machine) in machines.iter().enumerate() {
        let peers = peers.clone();
        sim.add_process(machine, &format!("node{i}"), move |ctx| {
            Box::pin(node(ctx, i, peers.clone()))
        });
    }
    // The quiet phase asks each node for its check from here. Faults hit it too.
    let prober = Process::add(sim, sim.add_machine("prober", 0), "prober");
    let mut workloads = Workloads::new();
    faults::add(&mut workloads, sim, faults::Wipes::On);
    workloads.add(Mesh { prober, peers });
    workloads
}

/// A node: serves echoes and checks, and pings a random peer and writes its disk now and then.
async fn node(ctx: Ctx, me: usize, peers: Rc<[SocketAddr]>) {
    let listener = ctx
        .listen(PORT)
        .await
        .expect("a node's port is free when it starts");
    let (c, p) = (ctx.clone(), peers.clone());
    ctx.spawn("gossip", Priority::Default, gossip(c, me, p))
        .detach();
    // Accepting fails only once this process is dead.
    while let Ok((stream, _)) = listener.accept().await {
        let (c, p) = (ctx.clone(), peers.clone());
        ctx.spawn("serve", Priority::Default, serve(c, me, p, stream))
            .detach();
    }
}

/// Keeps the network and the disk busy during the chaos phase, ignoring every error.
async fn gossip(ctx: Ctx, me: usize, peers: Rc<[SocketAddr]>) {
    loop {
        let pause = ctx
            .rng()
            .range(Duration::from_millis(20)..Duration::from_millis(200));
        ctx.sleep(pause).await;
        let peer = (me + ctx.rng().range(1..peers.len())) % peers.len();
        let _ = ctx
            .timeout(Duration::from_secs(1), ask(&ctx, peers[peer], ECHO))
            .await;
        let _ = write_disk(&ctx).await;
    }
}

async fn serve(ctx: Ctx, me: usize, peers: Rc<[SocketAddr]>, mut stream: Stream) {
    let Ok(request) = read_byte(&mut stream).await else {
        return;
    };
    let reply = match request {
        CHECK => check(&ctx, me, &peers).await,
        _ => ECHO,
    };
    if write_byte(&mut stream, reply).await.is_ok() {
        let _ = poll_fn(|cx| Pin::new(&mut stream).poll_close(cx)).await;
    }
}

/// Writes, syncs, and reads back the disk, then echoes every peer.
async fn check(ctx: &Ctx, me: usize, peers: &[SocketAddr]) -> u8 {
    if write_disk(ctx).await.is_err() {
        return DISK_FAILED;
    }
    for (j, &peer) in peers.iter().enumerate().filter(|&(j, _)| j != me) {
        let echo = ctx.timeout(Duration::from_secs(1), ask(ctx, peer, ECHO));
        if !matches!(echo.await, Ok(Ok(ECHO))) {
            return PEER + j as u8;
        }
    }
    PASSED
}

/// Sends `request` to the node at `addr` on a new connection and returns its one-byte answer.
async fn ask(ctx: &Ctx, addr: SocketAddr, request: u8) -> io::Result<u8> {
    let mut stream = ctx.connect(addr).await?;
    write_byte(&mut stream, request).await?;
    read_byte(&mut stream).await
}

async fn write_byte(stream: &mut Stream, byte: u8) -> io::Result<()> {
    let n = poll_fn(|cx| Pin::new(&mut *stream).poll_write(cx, &[byte])).await?;
    if n == 0 {
        return Err(io::ErrorKind::WriteZero.into());
    }
    poll_fn(|cx| Pin::new(&mut *stream).poll_flush(cx)).await
}

async fn read_byte(stream: &mut Stream) -> io::Result<u8> {
    let mut buf = [0];
    let n = poll_fn(|cx| Pin::new(&mut *stream).poll_read(cx, &mut buf)).await?;
    if n == 0 {
        return Err(io::ErrorKind::UnexpectedEof.into());
    }
    Ok(buf[0])
}

/// Writes 8 random bytes to the node's disk, syncs them, and reads them back.
async fn write_disk(ctx: &Ctx) -> io::Result<()> {
    let id = *ctx.disks().first().ok_or(io::ErrorKind::NotFound)?;
    let opts = OpenOptions::new().read(true).write(true).create(true);
    let file = ctx.disk(id).open("mesh", opts).await?;
    let data = ctx.rng().u64().to_le_bytes().to_vec();
    let (written, data) = file.write_at(data, 0).await;
    written?;
    file.sync().await?;
    let (read, back) = file.read_at(vec![0; data.len()], 0).await;
    if read? != data.len() || back != data {
        return Err(io::Error::other("read back something else"));
    }
    Ok(())
}

/// Lets the nodes run through the chaos phase, then checks that every fault was lifted.
struct Mesh {
    prober: Process,
    peers: Rc<[SocketAddr]>,
}

impl Workload for Mesh {
    fn name(&self) -> &'static str {
        "mesh"
    }

    async fn start(&mut self, _: &SimControl) -> WorkloadResult {
        std::future::pending().await
    }

    async fn liveness(&mut self, sim: &SimControl) -> WorkloadResult {
        for machine in sim.machines() {
            if !sim.is_up(machine) {
                return Err(format!("{machine:?} is still down").into());
            }
        }
        // Every machine is up, so this incarnation stays alive to the end of the run.
        let ctx = &self.prober.ctx();
        for (i, &addr) in self.peers.iter().enumerate() {
            let mut failure = None;
            for _ in 0..CHECK_ATTEMPTS {
                let status = ctx.timeout(Duration::from_secs(5), ask(ctx, addr, CHECK));
                failure = match status.await {
                    Ok(Ok(PASSED)) => None,
                    Ok(Ok(DISK_FAILED)) => Some("its disk failed".to_owned()),
                    Ok(Ok(code)) => Some(format!("node {} didn't answer it", code - PEER)),
                    Ok(Err(e)) => Some(e.to_string()),
                    Err(_) => Some("no answer within 5 s".to_owned()),
                };
                if failure.is_none() {
                    break;
                }
                ctx.sleep(Duration::from_millis(100)).await;
            }
            if let Some(failure) = failure {
                return Err(format!("node {i} failed its check: {failure}").into());
            }
        }
        Ok(())
    }
}
