//! Connections: two ordered byte pipes, and the socket at each end.

use std::cell::{RefCell, RefMut};
use std::collections::VecDeque;
use std::io;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use basalt_env::backend::StreamIo;
use basalt_env::{Duration, Instant, ProcessId, SocketAddr};
use futures_io::{AsyncRead, AsyncWrite};

use super::Net;
use crate::machine::MachineId;
use crate::time::Timer;

/// The most bytes one segment carries, so a large write arrives a piece at a time.
const SEGMENT: usize = 16 * 1024;
/// The most bytes a pipe holds that its reader hasn't read yet, about what Linux's buffer
/// autotuning allows, so a pipe can keep its link busy. A write beyond it is partial, and a
/// write into a full pipe waits for the reader.
const SEND_BUFFER: usize = 1 << 20;
/// How often a read or write that could move more than one byte moves fewer, as TCP may.
const SHORT_IO_PROBABILITY: f64 = 0.05;

/// One side of a connection: side 0 connected, side 1 accepted.
pub(super) struct Side {
    pub(super) process: ProcessId,
    pub(super) machine: MachineId,
    pub(super) addr: SocketAddr,
    /// The ephemeral port it holds, on the connecting side.
    pub(super) port: Option<u16>,
}

pub(super) struct Conn {
    pub(super) id: u64,
    pub(super) sides: [Side; 2],
    /// How long a partition may hold back what a side sent before the connection resets.
    stall_timeout: Duration,
    state: RefCell<ConnState>,
}

#[derive(Default)]
struct ConnState {
    /// `pipes[i]` carries what side `i` sends to the other side.
    pipes: [Pipe; 2],
    /// Reset at once at both ends, by a random reset or a stall. Every read and write fails.
    reset: bool,
    /// Side `i`'s socket is gone: dropped, or its process died.
    closed: [bool; 2],
    readers: [Option<Waker>; 2],
    writers: [Option<Waker>; 2],
}

#[derive(Default)]
struct Pipe {
    /// Bytes not yet read, in order.
    segments: VecDeque<Segment>,
    /// Bytes in `segments`.
    unread: usize,
    /// When the sender's end of stream is due, once it has shut down writing.
    fin: Option<Instant>,
    /// A reset the sender sent. The reader sees it after every byte before it.
    reset: Option<Reset>,
    /// When the last thing sent is due. Nothing sent later is due earlier.
    last_due: Instant,
    /// When the last segment read arrived.
    last_arrival: Instant,
}

struct Segment {
    bytes: Vec<u8>,
    /// How many of `bytes` have been read.
    read: usize,
    due: Instant,
}

/// A reset a side sends.
#[derive(Clone, Copy)]
enum Reset {
    /// Due at this time.
    Due(Instant),
    /// A closed socket's answer to data: sent once the data, due at `data` in the other pipe,
    /// arrives, and due `latency` later, but never before `after`. It follows the faults as
    /// they change, as the data's arrival does.
    Reply {
        data: Instant,
        latency: Duration,
        after: Instant,
    },
}

/// Something in a pipe.
enum Item {
    Bytes(usize),
    Fin,
    Reset,
}

impl Pipe {
    /// When something that would be due at `at` is due, never before what was sent before it.
    fn next_due(&mut self, at: Instant) -> Instant {
        self.last_due = self.last_due.max(at);
        self.last_due
    }

    /// The earliest due time anything in the pipe depends on.
    fn oldest_due(&self) -> Option<Instant> {
        let reset = self.reset.map(|r| match r {
            Reset::Due(due) => due,
            Reset::Reply { data, after, .. } => data.min(after),
        });
        let first = self.segments.front().map(|s| s.due).or(self.fin);
        first.into_iter().chain(reset).min()
    }
}

impl ConnState {
    /// Resets the connection at both ends at once.
    fn fail(&mut self) {
        self.reset = true;
        self.pipes = Default::default();
    }

    /// Sends a reset from `side`, due at `at` or behind everything `side` already sent.
    fn send_reset(&mut self, side: usize, at: Instant) {
        let pipe = &mut self.pipes[side];
        if pipe.reset.is_none() {
            pipe.reset = Some(Reset::Due(pipe.next_due(at)));
        }
    }

    /// Shuts down writing from `side`: the other side reads the end of the stream after the
    /// bytes before it.
    fn send_fin(&mut self, side: usize, at: Instant) {
        let pipe = &mut self.pipes[side];
        if pipe.fin.is_none() {
            pipe.fin = Some(pipe.next_due(at));
        }
    }

    /// Closes `side`'s socket as Linux does: a reset if it had data it never read, the end of
    /// the stream otherwise.
    fn close(&mut self, side: usize, at: Instant) {
        self.closed[side] = true;
        if self.reset {
            return;
        }
        if self.pipes[1 - side].unread > 0 {
            self.send_reset(side, at);
        } else {
            self.send_fin(side, at);
        }
    }

    /// Closes the socket of `side`, whose process died: the other side gets a reset.
    fn close_dead(&mut self, side: usize, at: Instant) {
        self.closed[side] = true;
        if !self.reset {
            self.send_reset(side, at);
        }
    }
}

/// What has arrived in a pipe by now, and what its reader waits on.
#[derive(Default)]
struct Front {
    /// Bytes that have arrived and not been read.
    bytes: usize,
    /// Whether the end of the stream, or a reset, has arrived after all the bytes.
    fin: bool,
    reset: bool,
    /// When the oldest thing that hasn't arrived yet arrives, if there is one.
    arrival: Option<Instant>,
    /// When a reset the sender sent arrives, if it sent one and it hasn't arrived yet.
    reset_at: Option<Instant>,
    /// When a partition holding back something still unread resets the connection, if one
    /// does. It may have already happened.
    stall: Option<Instant>,
}

impl Conn {
    pub(super) fn new(id: u64, sides: [Side; 2], stall_timeout: Duration) -> Conn {
        Conn {
            id,
            sides,
            stall_timeout,
            state: RefCell::default(),
        }
    }

    /// Changes the state with `f`, then wakes every task waiting on the connection, so each
    /// works out again what it waits for. Wakes happen outside the borrow.
    fn update<R>(&self, f: impl FnOnce(&mut ConnState) -> R) -> R {
        let (result, wakers) = {
            let mut st = self.state.borrow_mut();
            let result = f(&mut st);
            let ConnState {
                readers, writers, ..
            } = &mut *st;
            let [r0, r1] = readers;
            let [w0, w1] = writers;
            let wakers = [r0.take(), r1.take(), w0.take(), w1.take()];
            (result, wakers)
        };
        wakers.into_iter().flatten().for_each(Waker::wake);
        result
    }

    /// Wakes every task waiting on the connection.
    pub(super) fn wake(&self) {
        self.update(|_| ());
    }

    /// The oldest thing still in transit, when its due time this or later is still pending.
    pub(super) fn oldest_due(&self) -> Option<Instant> {
        let st = self.state.borrow();
        st.pipes.iter().filter_map(Pipe::oldest_due).min()
    }

    /// Closes the socket of every side whose process has died.
    pub(super) fn close_dead_sides(&self, net: &Net) {
        let dead = |side: usize| {
            !self.state.borrow().closed[side] && !net.machines.alive(self.sides[side].process)
        };
        let doomed: Vec<usize> = (0..2).filter(|&side| dead(side)).collect();
        if !doomed.is_empty() {
            let at = net.latency_from_now();
            self.update(|st| doomed.into_iter().for_each(|side| st.close_dead(side, at)));
        }
    }

    fn arrival(&self, net: &Net, from: usize, due: Instant) -> Instant {
        let (a, b) = (self.sides[from].machine, self.sides[1 - from].machine);
        net.faults.borrow().arrival(a, b, due)
    }

    /// When the reset `from` sent is due.
    fn reset_due(&self, net: &Net, from: usize, reset: Reset) -> Instant {
        match reset {
            Reset::Due(due) => due,
            Reset::Reply {
                data,
                latency,
                after,
            } => {
                let reaches = self.arrival(net, 1 - from, data);
                reaches
                    .checked_add(latency)
                    .unwrap_or(Instant::MAX)
                    .max(after)
            }
        }
    }

    /// Everything in the pipe from `from`, oldest first, with when it's due.
    fn items<'a>(
        &'a self,
        net: &'a Net,
        pipe: &'a Pipe,
        from: usize,
    ) -> impl Iterator<Item = (Item, Instant)> + 'a {
        let segments = pipe
            .segments
            .iter()
            .map(|s| (Item::Bytes(s.bytes.len() - s.read), s.due));
        let fin = pipe.fin.map(|due| (Item::Fin, due));
        let reset =
            (pipe.reset.into_iter()).map(move |r| (Item::Reset, self.reset_due(net, from, r)));
        segments.chain(fin).chain(reset)
    }

    /// Scans the pipe from `from` up to the oldest thing that hasn't arrived by `now`. The
    /// sender waits on each thing from when it was sent or the thing before it arrived,
    /// whichever is later, as TCP's retransmission timer does, and resets the connection if a
    /// partition held it back for the stall timeout meanwhile.
    fn front(&self, net: &Net, st: &ConnState, from: usize, now: Instant) -> Front {
        let (a, b) = (self.sides[from].machine, self.sides[1 - from].machine);
        let pipe = &st.pipes[from];
        let mut front = Front::default();
        let mut before = pipe.last_arrival;
        let faults = net.faults.borrow();
        for (item, due) in self.items(net, pipe, from) {
            let arrival = faults.arrival(a, b, due);
            let stall = faults.stall_reset(a, b, due.max(before)..arrival, self.stall_timeout);
            front.stall = front.stall.into_iter().chain(stall).min();
            if arrival > now {
                front.arrival = Some(arrival);
                break;
            }
            match item {
                Item::Bytes(n) => front.bytes += n,
                Item::Fin => front.fin = true,
                Item::Reset => front.reset = true,
            }
            before = arrival;
        }
        if !front.reset {
            front.reset_at = pipe
                .reset
                .map(|r| faults.arrival(a, b, self.reset_due(net, from, r)))
                .filter(|&t| t < Instant::MAX);
        }
        front
    }

    /// Scans both pipes, and fails if the connection has reset at both ends: by a random
    /// reset, or because a partition held back something either side sent for the stall
    /// timeout. Otherwise returns each pipe's front and when such a stall would reset it.
    fn check(&self, net: &Net, now: Instant) -> io::Result<([Front; 2], Option<Instant>)> {
        let fronts = {
            let st = self.state.borrow();
            if st.reset {
                return Err(reset_error());
            }
            [0, 1].map(|side| self.front(net, &st, side, now))
        };
        let stall = fronts.iter().filter_map(|f| f.stall).min();
        if stall.is_some_and(|t| t <= now) {
            self.update(ConnState::fail);
            return Err(reset_error());
        }
        Ok((fronts, stall))
    }

    /// Like [`check`](Conn::check), for writing from `side`: also fails once `side` has shut
    /// down writing, or a reset from the other side has arrived. Otherwise returns when the
    /// earliest of those could fail it.
    fn check_writable(&self, net: &Net, side: usize, now: Instant) -> io::Result<Option<Instant>> {
        let peer = 1 - side;
        let (fronts, stall) = self.check(net, now)?;
        if fronts[peer].reset {
            return Err(reset_error());
        }
        let st = self.state.borrow();
        if st.pipes[side].fin.is_some() {
            // Linux reports `EPIPE`, which the `Backend` contract reports as a reset.
            return Err(io::Error::new(
                io::ErrorKind::ConnectionReset,
                "writing after the stream was closed",
            ));
        }
        Ok(fronts[peer].reset_at.into_iter().chain(stall).min())
    }
}

fn reset_error() -> io::Error {
    io::Error::new(io::ErrorKind::ConnectionReset, "connection reset")
}

/// Sets `slot` to wake the current task, and drops the old waker outside the borrow.
fn park(
    mut st: RefMut<'_, ConnState>,
    slot: impl FnOnce(&mut ConnState) -> &mut Option<Waker>,
    cx: &Context<'_>,
) {
    let slot = slot(&mut st);
    if slot.as_ref().is_some_and(|w| w.will_wake(cx.waker())) {
        return;
    }
    let old = slot.replace(cx.waker().clone());
    drop(st);
    drop(old);
}

/// One side's socket of a connection.
pub(super) struct End {
    net: Rc<Net>,
    conn: Rc<Conn>,
    side: usize,
    read_timer: Timer,
    write_timer: Timer,
}

impl End {
    pub(super) fn new(net: Rc<Net>, conn: Rc<Conn>, side: usize) -> End {
        End {
            net,
            conn,
            side,
            read_timer: Timer::default(),
            write_timer: Timer::default(),
        }
    }

    fn process(&self) -> ProcessId {
        self.conn.sides[self.side].process
    }

    /// Up to `n` bytes, or with [`SHORT_IO_PROBABILITY`] fewer, as TCP may move.
    fn maybe_short(&self, n: usize) -> usize {
        if n > 1 && self.net.rng.bool(SHORT_IO_PROBABILITY) {
            self.net.rng.range(1..n)
        } else {
            n
        }
    }
}

/// Sets `timer` to wake the task at `at`, which is still ahead, or clears it.
fn wake_at(timer: &mut Timer, net: &Net, at: Option<Instant>, cx: &mut Context<'_>) {
    match at {
        Some(at) => {
            debug_assert!(at > net.clock.now(), "a timer for a time already past");
            let _ = timer.poll_at(&net.clock, at, cx);
        }
        None => timer.clear(),
    }
}

impl StreamIo for End {
    fn local_addr(&self) -> SocketAddr {
        self.conn.sides[self.side].addr
    }

    fn peer_addr(&self) -> SocketAddr {
        self.conn.sides[1 - self.side].addr
    }
}

impl AsyncRead for End {
    /// Reads what has arrived; then the end of the stream, or a reset the other side sent.
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let (net, conn, side) = (&*this.net, &*this.conn, this.side);
        net.machines.check_alive(this.process())?;
        let now = net.clock.now();
        let (fronts, stall) = conn.check(net, now)?;
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        let incoming = 1 - side;
        let front = &fronts[incoming];
        if front.bytes > 0 {
            let n = this.maybe_short(front.bytes.min(buf.len()));
            conn.update(|st| {
                let pipe = &mut st.pipes[incoming];
                let mut copied = 0;
                while copied < n {
                    let segment = pipe.segments.front_mut().expect("counted as arrived");
                    let take = (segment.bytes.len() - segment.read).min(n - copied);
                    buf[copied..copied + take]
                        .copy_from_slice(&segment.bytes[segment.read..segment.read + take]);
                    segment.read += take;
                    copied += take;
                    if segment.read == segment.bytes.len() {
                        pipe.last_arrival = conn.arrival(net, incoming, segment.due);
                        pipe.segments.pop_front();
                    }
                }
                pipe.unread -= n;
            });
            return Poll::Ready(Ok(n));
        }
        if front.reset {
            return Poll::Ready(Err(reset_error()));
        }
        if front.fin {
            return Poll::Ready(Ok(0));
        }
        let next = front.arrival.filter(|&t| t < Instant::MAX);
        let at = next.into_iter().chain(stall).min();
        park(conn.state.borrow_mut(), |s| &mut s.readers[side], cx);
        wake_at(&mut this.read_timer, &this.net, at, cx);
        Poll::Pending
    }
}

impl AsyncWrite for End {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let (net, conn, side) = (&*this.net, &*this.conn, this.side);
        net.machines.check_alive(this.process())?;
        let now = net.clock.now();
        let fails_at = conn.check_writable(net, side, now)?;
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        let p = net.faults.borrow().reset_probability;
        if p > 0.0 && net.rng.bool(p) {
            conn.update(ConnState::fail);
            return Poll::Ready(Err(reset_error()));
        }
        let st = conn.state.borrow_mut();
        let space = SEND_BUFFER - st.pipes[side].unread;
        if space == 0 {
            park(st, |s| &mut s.writers[side], cx);
            wake_at(&mut this.write_timer, &this.net, fails_at, cx);
            return Poll::Pending;
        }
        drop(st);
        let n = this.maybe_short(buf.len().min(space));
        let peer = 1 - side;
        let (from, to) = (conn.sides[side].machine, conn.sides[peer].machine);
        conn.update(|st| {
            let pipe = &mut st.pipes[side];
            for bytes in buf[..n].chunks(SEGMENT) {
                let sent = net.send_on_link(from, to, bytes.len());
                let due = pipe.next_due(net.latency_from(sent));
                pipe.segments.push_back(Segment {
                    bytes: bytes.to_vec(),
                    read: 0,
                    due,
                });
            }
            pipe.unread += n;
            let data = pipe.last_due;
            let answer = &mut st.pipes[peer];
            if st.closed[peer] && answer.reset.is_none() {
                // A closed socket answers data with a reset once the data reaches it. It sends
                // nothing else, so the reset stays last in its pipe, after `after`.
                answer.reset = Some(Reset::Reply {
                    data,
                    latency: net.half_latency(),
                    after: answer.last_due,
                });
            }
        });
        Poll::Ready(Ok(n))
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(self.net.machines.check_alive(self.process()))
    }

    /// Shuts down the write half: the peer reads the end of the stream after the bytes before
    /// it.
    fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        let (net, conn, side) = (&*self.net, &*self.conn, self.side);
        net.machines.check_alive(self.process())?;
        if conn.state.borrow().pipes[side].fin.is_none() {
            conn.check_writable(net, side, net.clock.now())?;
            let at = net.latency_from_now();
            conn.update(|st| st.send_fin(side, at));
        }
        Poll::Ready(Ok(()))
    }
}

impl Drop for End {
    /// Closes the socket. Data the peer sends later gets a reset. A dead process's sockets were
    /// closed when it died, so this only drops their wakers.
    fn drop(&mut self) {
        let (net, conn, side) = (&*self.net, &*self.conn, self.side);
        if !net.machines.alive(self.process()) {
            conn.wake();
            return;
        }
        let at = net.latency_from_now();
        conn.update(|st| st.close(side, at));
        if let Some(port) = conn.sides[side].port {
            let machine = conn.sides[side].machine;
            net.ports.borrow_mut().release(machine, port, conn.id);
        }
    }
}
