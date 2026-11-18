//! The simulated network, through `Ctx`'s `listen` and `connect`, `futures-io` on `Stream`, and
//! `SimControl`'s network faults.

use std::cell::RefCell;
use std::io::{self, ErrorKind};
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

use basalt_env::{Ctx, Duration, Elapsed, Instant, Listener, Priority, SocketAddr, Stream};
use basalt_sim::{KillType, MachineId, Sim, SimControl};
use futures_io::{AsyncRead, AsyncWrite};
use futures_util::io::{AsyncReadExt, AsyncWriteExt};

const PORT: u16 = 4500;
/// Comfortably above a one-way latency, which is under 50 ms.
const FAST: Duration = Duration::from_millis(60);

/// A machine in its own zone with one process, whose entry point does nothing.
fn node(ctrl: &SimControl, label: &str) -> (MachineId, Ctx) {
    let m = ctrl.add_machine(label, 0);
    (m, ctrl.add_process(m, label, |_| Box::pin(async {})))
}

/// A server process on machine `a` and a client process on machine `b`.
struct Net {
    sim: Sim,
    ctrl: SimControl,
    a: MachineId,
    server: Ctx,
    b: MachineId,
    client: Ctx,
}

fn net(seed: u64) -> Net {
    let sim = Sim::new(seed);
    let ctrl = sim.control();
    let (a, server) = node(&ctrl, "server");
    let (b, client) = node(&ctrl, "client");
    Net {
        sim,
        ctrl,
        a,
        server,
        b,
        client,
    }
}

impl Net {
    fn addr(&self) -> SocketAddr {
        SocketAddr::new(self.ctrl.ip(self.a), PORT)
    }

    /// Runs `f` in a client task with the server's listener and a connected pair of streams,
    /// the server's first.
    fn run<T: 'static>(&self, f: impl AsyncFnOnce(Listener, Stream, Stream) -> T + 'static) -> T {
        let (server, client, addr) = (self.server.clone(), self.client.clone(), self.addr());
        let task = self.client.spawn("driver", Priority::Default, async move {
            let listener = server.listen(PORT).await.unwrap();
            let c = client.connect(addr).await.unwrap();
            let (s, _) = listener.accept().await.unwrap();
            f(listener, s, c).await
        });
        self.sim.block_on(task).unwrap()
    }
}

/// A stream one task reads while another writes. Each poll borrows it only for the poll.
#[derive(Clone)]
struct Shared(Rc<RefCell<Stream>>);

impl Shared {
    fn new(s: Stream) -> Shared {
        Shared(Rc::new(RefCell::new(s)))
    }
}

impl AsyncRead for Shared {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut *self.0.borrow_mut()).poll_read(cx, buf)
    }
}

impl AsyncWrite for Shared {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut *self.0.borrow_mut()).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut *self.0.borrow_mut()).poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut *self.0.borrow_mut()).poll_close(cx)
    }
}

/// `n` bytes that differ from seed to seed, so a lost, duplicated, or reordered chunk shows.
fn pattern(ctx: &Ctx, n: usize) -> Vec<u8> {
    (0..n).map(|_| ctx.rng().u64() as u8).collect()
}

/// How long one byte written now takes to be read, with whatever faults are on.
async fn one_byte_latency(
    ctx: &Ctx,
    from: &mut (impl AsyncWrite + Unpin),
    to: &mut (impl AsyncRead + Unpin),
) -> Duration {
    let start = ctx.now();
    from.write_all(b"x").await.unwrap();
    to.read_exact(&mut [0]).await.unwrap();
    ctx.now() - start
}

#[test]
fn bytes_arrive_in_order_with_no_loss_or_duplication() {
    /// Writes `data` in pieces of random sizes, then closes the stream.
    async fn send(ctx: Ctx, mut s: Shared, data: Vec<u8>) {
        let mut rest = &data[..];
        while !rest.is_empty() {
            let k = ctx.rng().range(1..=rest.len().min(50_000));
            s.write_all(&rest[..k]).await.unwrap();
            rest = &rest[k..];
        }
        s.close().await.unwrap();
    }

    /// Reads until the end of the stream with a buffer of `chunk` bytes.
    async fn receive(mut s: Shared, chunk: usize) -> Vec<u8> {
        let (mut out, mut buf) = (Vec::new(), vec![0; chunk]);
        loop {
            match s.read(&mut buf).await.unwrap() {
                0 => return out,
                n => out.extend_from_slice(&buf[..n]),
            }
        }
    }

    for seed in 0..20 {
        let n = net(seed);
        let (sc, cc) = (n.server.clone(), n.client.clone());
        n.run(async move |_, s, c| {
            let up = pattern(&cc, 300_000);
            let down = pattern(&cc, 200_000);
            let (s, c) = (Shared::new(s), Shared::new(c));
            // Both directions at once, each reading with a buffer of another size.
            let p = Priority::Default;
            let sends = [
                sc.spawn("server-send", p, send(sc.clone(), s.clone(), down.clone())),
                cc.spawn("client-send", p, send(cc.clone(), c.clone(), up.clone())),
            ];
            let got_up = sc.spawn("server-receive", p, receive(s, 7_000));
            let got_down = cc.spawn("client-receive", p, receive(c, 13_000));
            assert!(got_up.await == up, "seed {seed}: client to server");
            assert!(got_down.await == down, "seed {seed}: server to client");
            for t in sends {
                t.await;
            }
        });
        n.sim.run().unwrap();
    }
}

/// What a `futures-io` reader and writer can count on from TCP: addresses, partial writes,
/// backpressure, reads of what has arrived, end of stream, and half-close.
#[test]
fn futures_io_reads_and_writes_behave_like_tcp() {
    for seed in 0..10 {
        let n = net(seed);
        let (client, addr, ip_b) = (n.client.clone(), n.addr(), n.ctrl.ip(n.b));
        n.run(async move |listener, mut s, mut c| {
            assert_eq!(listener.local_addr(), addr);
            assert_eq!((c.peer_addr(), s.local_addr()), (addr, addr));
            assert_eq!(s.peer_addr(), c.local_addr());
            assert_eq!(c.local_addr().ip(), ip_b);

            // A write larger than the send buffer is partial.
            let big = pattern(&client, 4 << 20);
            let first = c.write(&big).await.unwrap();
            assert!(0 < first && first < big.len(), "seed {seed}: wrote {first}");

            // With nobody reading, the writer waits once the buffer is full.
            let mut sent = first;
            let blocked = client
                .timeout(Duration::from_secs(1), async {
                    loop {
                        sent += c.write(&big[sent..]).await.unwrap();
                    }
                })
                .await;
            assert_eq!(blocked, Err(Elapsed));
            assert!(sent < big.len());

            // A read returns what has arrived, which may be less than the buffer and less than
            // was written; reading frees the writer.
            let mut buf = vec![0; big.len()];
            let got = s.read(&mut buf).await.unwrap();
            assert!(0 < got && got <= sent, "seed {seed}: read {got} of {sent}");
            assert_eq!(buf[..got], big[..got]);
            let rest = big[sent..].to_vec();
            let writer = client.spawn("writer", Priority::Default, async move {
                c.write_all(&rest).await.unwrap();
                c.close().await.unwrap();
                // Closing shuts down only the write half: the peer's reply still arrives.
                let mut reply = Vec::new();
                c.read_to_end(&mut reply).await.unwrap();
                // Writing after closing fails like a reset, as `basalt-rt` reports `EPIPE`.
                (reply, c.write(b"late").await.unwrap_err().kind())
            });
            s.read_exact(&mut buf[got..]).await.unwrap();
            assert!(buf == big, "seed {seed}");
            // The end of the stream reads as 0 every time.
            assert_eq!(s.read(&mut buf).await.unwrap(), 0);
            s.write_all(b"ok").await.unwrap();
            drop(s);
            assert_eq!(writer.await, (b"ok".to_vec(), ErrorKind::ConnectionReset));
        });
        n.sim.run().unwrap();
    }
}

/// TCP may move fewer bytes than it could, and the simulator sometimes does, so code that
/// assumes otherwise breaks in simulation.
#[test]
fn reads_and_writes_are_sometimes_short() {
    let (mut short_writes, mut short_reads, mut rounds) = (0, 0, 0);
    for seed in 0..5 {
        let n = net(seed);
        let client = n.client.clone();
        let (w, r) = n.run(async move |_, mut s, mut c| {
            let (mut short_writes, mut short_reads) = (0, 0);
            let piece = vec![1; 64 * 1024];
            for _ in 0..40 {
                // The send buffer is empty, so only a short write can take less.
                let wrote = c.write(&piece).await.unwrap();
                short_writes += usize::from(wrote < piece.len());
                // Everything has arrived, so only a short read can return less.
                client.sleep(Duration::from_millis(200)).await;
                let mut buf = vec![0; piece.len()];
                let got = s.read(&mut buf).await.unwrap();
                short_reads += usize::from(got < wrote);
                s.read_exact(&mut buf[got..wrote]).await.unwrap();
            }
            // A one-byte buffer reads one byte at a time.
            c.write_all(b"hi").await.unwrap();
            let mut byte = [0];
            assert_eq!(s.read(&mut byte).await.unwrap(), 1);
            assert_eq!(s.read(&mut byte).await.unwrap(), 1);
            (short_writes, short_reads)
        });
        (short_writes, short_reads, rounds) = (short_writes + w, short_reads + r, rounds + 40);
    }
    assert!(
        0 < short_writes && short_writes < rounds / 2,
        "{short_writes} of {rounds}"
    );
    assert!(
        0 < short_reads && short_reads < rounds / 2,
        "{short_reads} of {rounds}"
    );
}

#[test]
fn dropping_a_stream_with_unread_data_resets_the_peer() {
    let n = net(2);
    let client = n.client.clone();
    n.run(async move |_, s, mut c| {
        c.write_all(b"never read").await.unwrap();
        client.sleep(Duration::from_millis(100)).await;
        drop(s);
        let err = client
            .timeout(Duration::from_secs(1), c.read(&mut [0; 16]))
            .await
            .expect("the reset arrives within a latency")
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::ConnectionReset);
        let err = c.write(b"x").await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::ConnectionReset);
    });
}

#[test]
fn listen_and_connect_report_errors_like_tcp() {
    let n = net(3);
    let (server, client, ctrl) = (n.server.clone(), n.client.clone(), n.ctrl.clone());
    let (a, addr) = (n.a, n.addr());
    let task = n.client.spawn("driver", Priority::Default, async move {
        // Nothing listens yet: the server's machine answers with a reset.
        let err = client.connect(addr).await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::ConnectionRefused);

        let listener = server.listen(PORT).await.unwrap();
        let err = server.listen(PORT).await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::AddrInUse);

        // Port 0 picks a free ephemeral port, as on Linux.
        let any = server.listen(0).await.unwrap();
        let port = any.local_addr().port();
        assert!((32768..=60999).contains(&port), "port {port}");
        assert_eq!(any.local_addr().ip(), ctrl.ip(a));
        let other = server.listen(0).await.unwrap();
        assert_ne!(other.local_addr().port(), port);
        let c = client.connect(any.local_addr()).await.unwrap();
        let (s, peer) = any.accept().await.unwrap();
        assert_eq!(peer, c.local_addr());
        drop((s, c));

        // Dropping a listener frees its port.
        drop(listener);
        drop(server.listen(PORT).await.unwrap());

        // Loopback reaches the process's own machine.
        let lo = server.listen(PORT).await.unwrap();
        let c = server
            .connect(SocketAddr::from(([127, 0, 0, 1], PORT)))
            .await
            .unwrap();
        let (s, _) = lo.accept().await.unwrap();
        assert_eq!(s.peer_addr(), c.local_addr());
        assert!(c.local_addr().ip().is_loopback());

        // An address no machine has never answers.
        let start = client.now();
        let err = client
            .connect(SocketAddr::from(([192, 0, 2, 1], PORT)))
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::TimedOut);
        let waited = client.now() - start;
        assert!(
            (Duration::from_secs(5)..=Duration::from_secs(30)).contains(&waited),
            "{waited:?}"
        );
    });
    n.sim.block_on(task).unwrap();
}

#[test]
fn a_clog_delays_delivery_by_its_duration() {
    const CLOG: Duration = Duration::from_secs(2);
    type Clog = fn(&SimControl, MachineId, MachineId);
    // Each kind of clog of server `a` and client `b`, and whether it delays client-to-server
    // and server-to-client bytes.
    let clogs: [(&str, Clog, [bool; 2]); 4] = [
        (
            "pair",
            |ctrl, a, b| ctrl.clog_pair(b, a, CLOG),
            [true, false],
        ),
        (
            "sends",
            |ctrl, _, b| ctrl.clog_sends(b, CLOG),
            [true, false],
        ),
        (
            "receives",
            |ctrl, _, b| ctrl.clog_receives(b, CLOG),
            [false, true],
        ),
        (
            "receives at server",
            |ctrl, a, _| ctrl.clog_receives(a, CLOG),
            [true, false],
        ),
    ];
    for seed in 0..10 {
        for (name, clog, delayed) in clogs {
            let n = net(seed);
            let (client, ctrl, a, b) = (n.client.clone(), n.ctrl.clone(), n.a, n.b);
            n.run(async move |_, s, c| {
                let (s, c) = (Shared::new(s), Shared::new(c));
                clog(&ctrl, a, b);
                // One byte each way at once, timed from the clog.
                let p = Priority::Default;
                let directions = [(c.clone(), s.clone()), (s.clone(), c.clone())];
                let directions = directions.map(|(mut from, mut to)| {
                    let ctx = client.clone();
                    client.spawn("one-byte", p, async move {
                        one_byte_latency(&ctx, &mut from, &mut to).await
                    })
                });
                for (task, delayed) in directions.into_iter().zip(delayed) {
                    let took = task.await;
                    let expected = if delayed {
                        // When the clog ends, give or take a late timer.
                        CLOG..CLOG + Duration::from_millis(2)
                    } else {
                        Duration::ZERO..FAST
                    };
                    assert!(expected.contains(&took), "seed {seed}, {name}: {took:?}");
                }
                // Once the clog is over, bytes flow at full speed again.
                client.sleep(CLOG).await;
                let (mut s, mut c) = (s, c);
                let again = one_byte_latency(&client, &mut c, &mut s).await;
                assert!(again < FAST, "seed {seed}, {name}: {again:?}");
            });
        }
    }
}

#[test]
fn a_partition_stalls_then_resets_the_connection() {
    for seed in 0..10 {
        for two_way in [false, true] {
            let n = net(seed);
            let (client, ctrl, a, b) = (n.client.clone(), n.ctrl.clone(), n.a, n.b);
            n.run(async move |_, mut s, mut c| {
                if two_way {
                    ctrl.partition_two_way(a, b);
                } else {
                    ctrl.partition(b, a);
                }
                let start = client.now();
                c.write_all(b"stalled").await.unwrap();
                if !two_way {
                    // Server-to-client bytes still arrive over a one-way partition.
                    let took = one_byte_latency(&client, &mut s, &mut c).await;
                    assert!(took < FAST, "seed {seed}: {took:?}");
                }
                let err = s.read(&mut [0; 16]).await.unwrap_err();
                assert_eq!(err.kind(), ErrorKind::ConnectionReset, "seed {seed}");
                let stalled = client.now() - start;
                assert!(
                    (Duration::from_secs(5)..=Duration::from_secs(31)).contains(&stalled),
                    "seed {seed}: reset after {stalled:?}"
                );
                // Both sides see the reset, and healing doesn't undo it.
                let err = c.read(&mut [0; 16]).await.unwrap_err();
                assert_eq!(err.kind(), ErrorKind::ConnectionReset);
                ctrl.heal_network();
                let err = c.write(b"x").await.unwrap_err();
                assert_eq!(err.kind(), ErrorKind::ConnectionReset);
            });
        }
    }
}

#[test]
fn healing_a_partition_in_time_delivers_what_it_held_back() {
    for seed in 0..10 {
        let n = net(seed);
        let (client, ctrl, a, b, addr) = (n.client.clone(), n.ctrl.clone(), n.a, n.b, n.addr());
        n.run(async move |listener, mut s, mut c| {
            ctrl.partition_two_way(a, b);
            c.write_all(b"held").await.unwrap();
            // A connect across the partition waits too.
            let cc = client.clone();
            let connect = client.spawn("connect", Priority::Default, async move {
                let stream = cc.connect(addr).await;
                (stream, cc.now())
            });
            client.sleep(Duration::from_secs(3)).await;
            let healed = client.now();
            ctrl.heal_network();
            let mut buf = [0; 4];
            s.read_exact(&mut buf).await.unwrap();
            assert_eq!(&buf, b"held");
            assert!(client.now() - healed < Duration::from_millis(2));
            let (late, connected) = connect.await;
            assert!(
                connected >= healed,
                "seed {seed}: connected before the heal"
            );
            let (mut accepted, _) = listener.accept().await.unwrap();
            let took = one_byte_latency(&client, &mut late.unwrap(), &mut accepted).await;
            assert!(took < FAST, "seed {seed}: {took:?}");
        });
    }
}

/// A connection whose stall timeout passed during a partition stays reset after healing, even
/// if nothing looked at it in between.
#[test]
fn healing_too_late_leaves_the_connection_reset() {
    for seed in 0..10 {
        let n = net(seed);
        let (client, ctrl, a, b) = (n.client.clone(), n.ctrl.clone(), n.a, n.b);
        n.run(async move |_, mut s, mut c| {
            ctrl.partition_two_way(a, b);
            c.write_all(b"held").await.unwrap();
            client.sleep(Duration::from_secs(40)).await;
            ctrl.heal_network();
            let err = s.read(&mut [0; 4]).await.unwrap_err();
            assert_eq!(err.kind(), ErrorKind::ConnectionReset, "seed {seed}");
        });
    }
}

/// A clog that ends before a partition starts, then a new clog, never frees bytes the partition
/// holds: faults only add to what happened, never rewrite it.
#[test]
fn clogging_again_never_frees_bytes_a_partition_holds() {
    for seed in 0..10 {
        let n = net(seed);
        let (client, ctrl, a, b) = (n.client.clone(), n.ctrl.clone(), n.a, n.b);
        n.run(async move |_, mut s, mut c| {
            let start = client.now();
            ctrl.clog_pair(b, a, Duration::from_secs(2));
            c.write_all(b"held").await.unwrap();
            client.sleep(Duration::from_secs(1)).await;
            ctrl.partition(b, a);
            client.sleep(Duration::from_secs(2)).await;
            ctrl.clog_pair(b, a, Duration::from_secs(1));
            let err = s.read(&mut [0; 4]).await.unwrap_err();
            assert_eq!(err.kind(), ErrorKind::ConnectionReset, "seed {seed}");
            // Held by the partition from 1 s on, for the stall timeout.
            let reset = client.now() - start;
            assert!(
                (Duration::from_secs(6)..=Duration::from_secs(32)).contains(&reset),
                "seed {seed}: {reset:?}"
            );
        });
    }
}

#[test]
fn a_kill_resets_the_peers_connection() {
    for seed in 0..10 {
        let sim = Sim::new(seed);
        let ctrl = sim.control();
        let a = ctrl.add_machine("a", 0);
        // A server that greets each connection and then holds it open.
        let first = ctrl.add_process(a, "server", |ctx| {
            Box::pin(async move {
                let listener = ctx.listen(PORT).await.unwrap();
                loop {
                    let (mut s, _) = listener.accept().await.unwrap();
                    let c = ctx.clone();
                    ctx.spawn("conn", Priority::Default, async move {
                        s.write_all(b"hi").await.unwrap();
                        c.sleep(Duration::MAX).await;
                    })
                    .detach();
                }
            })
        });
        let (_, client) = node(&ctrl, "client");
        let addr = SocketAddr::new(ctrl.ip(a), PORT);
        let ctx = client.clone();
        let task = client.spawn("driver", Priority::Default, async move {
            let mut c = ctx.connect(addr).await.unwrap();
            let mut buf = [0; 2];
            c.read_exact(&mut buf).await.unwrap();
            assert_eq!(&buf, b"hi");

            let killed = ctx.now();
            ctrl.kill(a, KillType::Crash);
            let err = c.read(&mut buf).await.unwrap_err();
            assert_eq!(err.kind(), ErrorKind::ConnectionReset, "seed {seed}");
            assert!(ctx.now() - killed < FAST);
            let err = c.write(b"x").await.unwrap_err();
            assert_eq!(err.kind(), ErrorKind::ConnectionReset);

            // The dead incarnation's `Ctx` can't use the network.
            assert!(first.listen(PORT + 1).await.is_err());
            assert!(first.connect(addr).await.is_err());
            // A machine that's down never answers.
            let err = ctx.connect(addr).await.unwrap_err();
            assert_eq!(err.kind(), ErrorKind::TimedOut);

            // The new incarnation gets the port back, and serves again.
            ctrl.reboot(a);
            ctx.sleep(Duration::from_millis(1)).await;
            let mut c = ctx.connect(addr).await.unwrap();
            c.read_exact(&mut buf).await.unwrap();
            assert_eq!(&buf, b"hi");
            ctrl.kill(a, KillType::Crash);
        });
        sim.block_on(task).unwrap();
        sim.run().unwrap();
    }
}

/// What a crashing process sent before it died still arrives, ahead of the reset, even when a
/// clog makes both arrive at once.
#[test]
fn bytes_sent_before_a_kill_arrive_before_the_reset() {
    for seed in 0..10 {
        for clogged in [false, true] {
            let n = net(seed);
            let (ctrl, a, b) = (n.ctrl.clone(), n.a, n.b);
            n.run(async move |_, mut s, mut c| {
                if clogged {
                    ctrl.clog_receives(b, Duration::from_secs(1));
                }
                s.write_all(b"last words").await.unwrap();
                ctrl.kill(a, KillType::Crash);
                let mut buf = [0; 10];
                c.read_exact(&mut buf).await.unwrap();
                assert_eq!(&buf, b"last words");
                let err = c.read(&mut buf).await.unwrap_err();
                assert_eq!(err.kind(), ErrorKind::ConnectionReset, "seed {seed}");
            });
        }
    }
}

/// A stream or listener held by another process outlives the process it belongs to, but fails
/// with the dead-process error rather than a reset.
#[test]
fn a_dead_processs_sockets_fail() {
    let n = net(5);
    let (ctrl, a, client) = (n.ctrl.clone(), n.a, n.client.clone());
    n.run(async move |listener, mut s, mut c| {
        ctrl.kill(a, KillType::Crash);
        let err = s.read(&mut [0]).await.unwrap_err();
        assert_ne!(err.kind(), ErrorKind::ConnectionReset);
        assert!(s.write(b"x").await.is_err());
        assert!(listener.accept().await.is_err());
        drop(s);
        let err = client
            .timeout(Duration::from_secs(1), c.read(&mut [0]))
            .await
            .unwrap()
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::ConnectionReset);
    });
}

/// A task of another process already waiting to accept fails when the listener's process dies,
/// instead of waiting forever.
#[test]
fn an_accept_waiting_when_its_process_dies_fails() {
    let n = net(8);
    let (ctrl, a, server, client) = (n.ctrl.clone(), n.a, n.server.clone(), n.client.clone());
    let c = client.clone();
    let task = client.spawn("driver", Priority::Default, async move {
        let listener = server.listen(PORT).await.unwrap();
        let accept = c.spawn("accept", Priority::Default, async move {
            listener.accept().await.map(drop)
        });
        c.sleep(Duration::from_secs(1)).await;
        ctrl.kill(a, KillType::Crash);
        accept.await
    });
    assert!(n.sim.block_on(task).unwrap().is_err());
}

/// Two tasks may wait to accept on one listener, as with TCP; each gets a connection.
#[test]
fn several_tasks_can_wait_to_accept_on_one_listener() {
    let n = net(9);
    let (server, client, addr) = (n.server.clone(), n.client.clone(), n.addr());
    let c = client.clone();
    let task = client.spawn("driver", Priority::Default, async move {
        let listener = Rc::new(server.listen(PORT).await.unwrap());
        let accepts = [0, 1].map(|_| {
            let l = listener.clone();
            c.spawn("accept", Priority::Default, async move {
                l.accept().await.unwrap().1
            })
        });
        c.sleep(Duration::from_millis(10)).await;
        let streams = [
            c.connect(addr).await.unwrap(),
            c.connect(addr).await.unwrap(),
        ];
        let mut peers = Vec::new();
        for accept in accepts {
            peers.push(accept.await);
        }
        peers.sort();
        let mut locals = streams.map(|s| s.local_addr()).to_vec();
        locals.sort();
        assert_eq!(peers, locals);
    });
    n.sim.block_on(task).unwrap();
}

/// A writer waiting for buffer space learns of a reset caused by bytes the other side sends
/// into a partition.
#[test]
fn a_writer_waiting_on_a_full_buffer_sees_a_stall_reset() {
    for seed in 0..10 {
        let n = net(seed);
        let (client, ctrl, a, b) = (n.client.clone(), n.ctrl.clone(), n.a, n.b);
        n.run(async move |_, mut s, mut c| {
            let cc = client.clone();
            let writer = client.spawn("writer", Priority::Default, async move {
                let chunk = vec![0; 64 * 1024];
                loop {
                    if let Err(e) = c.write(&chunk).await {
                        return (e.kind(), cc.now());
                    }
                }
            });
            client.sleep(Duration::from_secs(1)).await;
            ctrl.partition(a, b);
            client.sleep(Duration::from_secs(1)).await;
            let sent = client.now();
            s.write_all(b"x").await.unwrap();
            let (kind, failed) = writer.await;
            assert_eq!(kind, ErrorKind::ConnectionReset, "seed {seed}");
            let after = failed - sent;
            assert!(
                (Duration::from_secs(5)..=Duration::from_secs(31)).contains(&after),
                "seed {seed}: {after:?}"
            );
        });
    }
}

/// Data written to a closed socket gets a reset only once it gets there: a partition in its
/// way holds the reset back, and healing the partition lets it through.
#[test]
fn a_closed_peer_resets_once_the_data_reaches_it() {
    for seed in 0..10 {
        for heal_after in [None, Some(Duration::from_secs(1))] {
            let n = net(seed);
            let (client, ctrl, a, b) = (n.client.clone(), n.ctrl.clone(), n.a, n.b);
            n.run(async move |_, s, mut c| {
                drop(s);
                assert_eq!(c.read(&mut [0]).await.unwrap(), 0);
                ctrl.partition(b, a);
                c.write_all(b"x").await.unwrap();
                let start = client.now();
                let mut healed = start;
                if let Some(d) = heal_after {
                    client.sleep(d).await;
                    ctrl.heal_network();
                    healed = client.now();
                }
                let err = loop {
                    if let Err(e) = c.write(&[0; 4096]).await {
                        break e;
                    }
                    client.sleep(Duration::from_millis(10)).await;
                };
                assert_eq!(err.kind(), ErrorKind::ConnectionReset);
                let (after, expected) = match heal_after {
                    // Only the stall timeout resets the connection.
                    None => (
                        client.now() - start,
                        Duration::from_secs(5)..Duration::from_secs(31),
                    ),
                    // The data arrives at the heal, and the reset a latency later. BUGGIFY may
                    // delay the sleep that notices it.
                    Some(_) => (
                        client.now() - healed,
                        Duration::ZERO..FAST + basalt_env::knobs().max_buggified_delay,
                    ),
                };
                assert!(
                    expected.contains(&after),
                    "seed {seed}, {heal_after:?}: {after:?}"
                );
            });
        }
    }
}

/// The quiet phase's `lift_faults` heals the network too: bytes a partition and a clog held
/// back arrive at once, random resets stop, and new connections cross the healed link.
#[test]
fn lifting_faults_heals_the_network() {
    for seed in 0..10 {
        let n = net(seed);
        let (client, ctrl, a, b, addr) = (n.client.clone(), n.ctrl.clone(), n.a, n.b, n.addr());
        n.run(async move |listener, mut s, mut c| {
            ctrl.partition_two_way(a, b);
            ctrl.clog_sends(b, Duration::from_secs(3600));
            c.write_all(b"held").await.unwrap();
            ctrl.set_reset_probability(1.0);
            client.sleep(Duration::from_secs(1)).await;
            let lifted = client.now();
            ctrl.lift_faults();
            let mut buf = [0; 4];
            s.read_exact(&mut buf).await.unwrap();
            assert_eq!(&buf, b"held");
            assert!(
                client.now() - lifted < Duration::from_millis(2),
                "seed {seed}"
            );
            // No more random resets, and the server's answer crosses the healed link.
            let took = one_byte_latency(&client, &mut s, &mut c).await;
            assert!(took < FAST, "seed {seed}: {took:?}");
            let connecting = client.now();
            let mut c2 = client.connect(addr).await.unwrap();
            assert!(client.now() - connecting < FAST, "seed {seed}");
            let (mut s2, _) = listener.accept().await.unwrap();
            let took = one_byte_latency(&client, &mut c2, &mut s2).await;
            assert!(took < FAST, "seed {seed}: {took:?}");
        });
    }
}

#[test]
fn random_resets_fail_writes_until_healed() {
    let n = net(6);
    let (ctrl, client, addr) = (n.ctrl.clone(), n.client.clone(), n.addr());
    n.run(async move |listener, mut s, mut c| {
        ctrl.set_reset_probability(1.0);
        let err = c.write(b"x").await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::ConnectionReset);
        let err = s.read(&mut [0]).await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::ConnectionReset);
        ctrl.heal_network();
        let mut c = client.connect(addr).await.unwrap();
        let (mut s, _) = listener.accept().await.unwrap();
        assert!(one_byte_latency(&client, &mut c, &mut s).await < FAST);
    });
}

#[test]
fn a_link_carries_at_most_its_bandwidth() {
    // 10 Gbit/s: the last byte of 1 MiB can't arrive sooner than this after the write, while
    // latency alone is under 450 µs 99.9% of the time.
    let mib = 1 << 20;
    let floor = Duration::from_nanos(mib as u64 * 8 / 10);
    for seed in 0..10 {
        let n = net(seed);
        let client = n.client.clone();
        let took = n.run(async move |_, mut s, mut c| {
            let start = client.now();
            c.write_all(&vec![7; mib]).await.unwrap();
            c.close().await.unwrap();
            let mut all = Vec::new();
            s.read_to_end(&mut all).await.unwrap();
            assert_eq!(all.len(), mib);
            client.now() - start
        });
        assert!(
            (floor..floor + FAST).contains(&took),
            "seed {seed}: {took:?}"
        );
    }
}

/// When each of a stream of messages arrived, and the run's unseed.
fn traffic(seed: u64) -> (Vec<Instant>, u64) {
    let n = net(seed);
    let (client, ctrl, a, b) = (n.client.clone(), n.ctrl.clone(), n.a, n.b);
    let arrivals = n.run(async move |_, mut s, mut c| {
        let cc = client.clone();
        let writer = client.spawn("writer", Priority::Default, async move {
            for i in 0..50u32 {
                c.write_all(&i.to_le_bytes()).await.unwrap();
                cc.sleep(Duration::from_micros(300)).await;
                if i == 20 {
                    ctrl.clog_pair(b, a, Duration::from_millis(5));
                }
            }
            c.close().await.unwrap();
        });
        let mut arrivals = Vec::new();
        while s.read(&mut [0; 64]).await.unwrap() > 0 {
            arrivals.push(client.now());
        }
        writer.await;
        arrivals
    });
    (arrivals, n.sim.unseed())
}

#[test]
fn the_network_is_deterministic_per_seed() {
    assert_eq!(traffic(11), traffic(11));
    assert_ne!(traffic(11).0, traffic(12).0);
}
