//! The simulated network: TCP-like connections between processes, a latency model, a bandwidth
//! limit per link, and the faults `SimControl` injects.
//!
//! A connection is two ordered byte pipes. Each write is cut into segments, and each segment
//! is due after the link has sent everything queued before it plus a one-way latency, never
//! before the segment ahead of it. Faults never reorder or drop bytes inside a connection: they
//! delay them (clogs), hold them back (partitions), or reset the whole connection.
//!
//! Nothing here runs on its own. Faults are a timeline of spans that only grows (see
//! [`faults`]), and each read, write, and connect works out from it when what it waits for
//! arrives, and sets a timer for the next thing that can change what it sees.
//!
//! Addresses: machine `n` is `10.0.0.1 + n`, and `127.0.0.1` reaches the caller's own machine.

mod conn;
mod faults;
mod listener;
mod ports;

use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::future::poll_fn;
use std::io;
use std::net::{IpAddr, Ipv4Addr};
use std::ops::RangeInclusive;
use std::rc::{Rc, Weak};
use std::task::{Context, Poll, Waker};

use basalt_env::backend::{ListenerIo, StreamIo};
use basalt_env::{Duration, Instant, ProcessId, Rng, SocketAddr};

use self::conn::{Conn, End, Side};
use self::faults::Faults;
use self::listener::{Backlog, SimListener};
use self::ports::{Holder, Port, Ports};
use crate::machine::{MachineId, Machines};
use crate::time::{Clock, Timer};

/// FoundationDB's `halfLatency`: a one-way latency between `MIN_LATENCY / 2` and
/// `FAST_LATENCY / 2` 99.9% of the time, and up to `SLOW_LATENCY / 2` otherwise.
const MIN_LATENCY: f64 = 100e-6;
const FAST_LATENCY: f64 = 800e-6;
const SLOW_LATENCY: f64 = 100e-3;
const FAST_PROBABILITY: f64 = 0.999;

/// Bytes per second each link (from one machine to another, or a machine to itself) carries:
/// 10 Gbit/s. Connections over the same link share it.
const LINK_BANDWIDTH: u64 = 1_250_000_000;
/// How long a connection, or a connect, waits on something a partition holds back, or on a
/// machine that never answers, before it gives up. Drawn for each connection and each connect.
const STALL_TIMEOUT: RangeInclusive<Duration> = Duration::from_secs(5)..=Duration::from_secs(30);

const FIRST_IP: u32 = u32::from_be_bytes([10, 0, 0, 1]);

/// The address of `machine` on the simulated network.
pub(crate) fn ip(machine: MachineId) -> IpAddr {
    IpAddr::V4(Ipv4Addr::from(FIRST_IP + machine.get()))
}

pub(crate) struct Net {
    clock: Rc<Clock>,
    machines: Rc<Machines>,
    /// The `network` stream: latencies, short reads and writes, stall timeouts, random resets.
    rng: Rc<Rng>,
    faults: RefCell<Faults>,
    ports: RefCell<Ports>,
    /// When each link finishes sending what's queued on it.
    links: RefCell<BTreeMap<(MachineId, MachineId), Instant>>,
    /// Every connection that may still have an open end, by id.
    conns: RefCell<BTreeMap<u64, Weak<Conn>>>,
    /// Each connect waiting on a handshake, by the id of its wait: when what it waits for was
    /// sent, and its waker. A wait removes its entry when it ends.
    waiters: RefCell<BTreeMap<u64, (Instant, Waker)>>,
    next_id: Cell<u64>,
}

impl Net {
    pub(crate) fn new(clock: Rc<Clock>, machines: Rc<Machines>, rng: Rc<Rng>) -> Net {
        Net {
            clock,
            machines,
            rng,
            faults: RefCell::default(),
            ports: RefCell::default(),
            links: RefCell::default(),
            conns: RefCell::default(),
            waiters: RefCell::default(),
            next_id: Cell::new(0),
        }
    }

    fn next_id(&self) -> u64 {
        let id = self.next_id.get();
        self.next_id.set(id + 1);
        id
    }

    /// One one-way latency, after FoundationDB's `halfLatency`.
    fn half_latency(&self) -> Duration {
        let a = self.rng.f64();
        let secs = if a <= FAST_PROBABILITY {
            let a = a / FAST_PROBABILITY;
            0.5 * (MIN_LATENCY * (1.0 - a) + FAST_LATENCY / FAST_PROBABILITY * a)
        } else {
            let a = (a - FAST_PROBABILITY) / (1.0 - FAST_PROBABILITY);
            0.5 * (MIN_LATENCY * (1.0 - a) + SLOW_LATENCY * a)
        };
        Duration::from_secs_f64(secs)
    }

    fn latency_from(&self, at: Instant) -> Instant {
        at.checked_add(self.half_latency()).unwrap_or(Instant::MAX)
    }

    fn latency_from_now(&self) -> Instant {
        self.latency_from(self.clock.now())
    }

    /// Queues `len` bytes on the link from `from` to `to` and returns when they've all left.
    fn send_on_link(&self, from: MachineId, to: MachineId, len: usize) -> Instant {
        let mut links = self.links.borrow_mut();
        let free = links.entry((from, to)).or_default();
        let nanos = len as u128 * 1_000_000_000 / u128::from(LINK_BANDWIDTH);
        let took = Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX));
        *free = (*free)
            .max(self.clock.now())
            .checked_add(took)
            .unwrap_or(Instant::MAX);
        *free
    }

    fn machine_at(&self, ip: IpAddr) -> Option<MachineId> {
        let IpAddr::V4(v4) = ip else { return None };
        self.machines.machine(u32::from(v4).checked_sub(FIRST_IP)?)
    }

    fn track(&self, conn: &Rc<Conn>) {
        let mut conns = self.conns.borrow_mut();
        conns.retain(|_, c| c.strong_count() > 0);
        conns.insert(conn.id, Rc::downgrade(conn));
    }

    fn conns(&self) -> Vec<Rc<Conn>> {
        self.conns
            .borrow()
            .values()
            .filter_map(Weak::upgrade)
            .collect()
    }

    // Sockets

    pub(crate) fn listen(
        self: &Rc<Self>,
        process: ProcessId,
        port: u16,
    ) -> io::Result<Box<dyn ListenerIo>> {
        self.machines.check_alive(process)?;
        let machine = self.machines.machine_of(process);
        let owner = self.next_id();
        let backlog = Rc::new(Backlog::new(process));
        let taken = Port {
            process,
            owner,
            holder: Holder::Listener(Rc::downgrade(&backlog)),
        };
        let port = self.ports.borrow_mut().claim(machine, port, taken)?;
        Ok(Box::new(SimListener {
            net: self.clone(),
            backlog,
            machine,
            addr: SocketAddr::new(ip(machine), port),
            owner,
        }))
    }

    /// Opens a connection like TCP's handshake: the request takes one latency to reach `addr`'s
    /// machine, where the listener's process takes it at once, and the answer takes one more to
    /// come back. A machine that's down or unknown never answers, and one with nothing listening
    /// answers with a reset.
    pub(crate) async fn connect(
        self: Rc<Self>,
        process: ProcessId,
        addr: SocketAddr,
    ) -> io::Result<Box<dyn StreamIo>> {
        self.machines.check_alive(process)?;
        let from = self.machines.machine_of(process);
        let (local_ip, remote_ip, to) = if addr.ip().is_loopback() {
            (addr.ip(), addr.ip(), Some(from))
        } else {
            (ip(from), addr.ip(), self.machine_at(addr.ip()))
        };
        let stall = self.rng.range(STALL_TIMEOUT);
        let Some(to) = to.filter(|&to| self.machines.is_up(to)) else {
            return Err(self.no_answer(process, addr, stall).await);
        };
        self.travel(process, (from, to), stall, addr).await?;
        if !self.machines.is_up(to) {
            return Err(self.no_answer(process, addr, stall).await);
        }
        let backlog = self.ports.borrow().listener(to, addr.port());
        let Some(backlog) = backlog else {
            self.travel(process, (to, from), stall, addr).await?;
            return Err(io::Error::new(
                io::ErrorKind::ConnectionRefused,
                format!("nothing listens on {addr}"),
            ));
        };

        let id = self.next_id();
        let taken = Port {
            process,
            owner: id,
            holder: Holder::Connection,
        };
        let local_port = self.ports.borrow_mut().claim(from, 0, taken)?;
        let sides = [
            Side {
                process,
                machine: from,
                addr: SocketAddr::new(local_ip, local_port),
                port: Some(local_port),
            },
            Side {
                process: backlog.process,
                machine: to,
                addr: SocketAddr::new(remote_ip, addr.port()),
                port: None,
            },
        ];
        let conn = Rc::new(Conn::new(id, sides, self.rng.range(STALL_TIMEOUT)));
        self.track(&conn);
        let client = End::new(self.clone(), conn.clone(), 0);
        // The listener's side has the connection now, as with TCP, where the server's kernel
        // completes the handshake. If the answer never gets back, dropping `client` closes it,
        // and the server reads the end of the stream.
        backlog.push(End::new(self.clone(), conn, 1));
        self.travel(process, (to, from), stall, addr).await?;
        Ok(Box::new(client))
    }

    /// Sends one handshake packet from one machine to another and waits until it arrives.
    /// Fails with `TimedOut` if a partition holds it back for `stall`.
    async fn travel(
        &self,
        process: ProcessId,
        (from, to): (MachineId, MachineId),
        stall: Duration,
        addr: SocketAddr,
    ) -> io::Result<()> {
        let due = self.latency_from_now();
        let delivered = self
            .wait(process, due, |now| {
                let faults = self.faults.borrow();
                let arrival = faults.arrival(from, to, due);
                let gives_up = faults.stall_reset(from, to, due..arrival, stall);
                match gives_up {
                    Some(t) if t <= now => Ok(false),
                    _ if arrival <= now => Ok(true),
                    _ => Err(gives_up.map_or(arrival, |t| t.min(arrival))),
                }
            })
            .await?;
        delivered.then_some(()).ok_or_else(|| timed_out(addr))
    }

    /// Waits out `stall` for an answer that never comes, and returns the error.
    async fn no_answer(&self, process: ProcessId, addr: SocketAddr, stall: Duration) -> io::Error {
        let until = self.clock.now().checked_add(stall).unwrap_or(Instant::MAX);
        let waited = self
            .wait(process, Instant::MAX, |now| {
                if now >= until { Ok(()) } else { Err(until) }
            })
            .await;
        waited.err().unwrap_or_else(|| timed_out(addr))
    }

    /// Waits on behalf of `process` until `step` returns `Ok`. `step` gets the current time and
    /// returns `Err` with the time to look again, which is still ahead, and it's looked at
    /// sooner if the faults change. `sent` is when what it waits for was sent, so the faults
    /// before then may be forgotten. Fails if `process` dies first.
    async fn wait<T>(
        &self,
        process: ProcessId,
        sent: Instant,
        mut step: impl FnMut(Instant) -> Result<T, Instant>,
    ) -> io::Result<T> {
        struct Registered<'a>(&'a Net, u64);
        impl Drop for Registered<'_> {
            fn drop(&mut self) {
                let waker = self.0.waiters.borrow_mut().remove(&self.1);
                drop(waker);
            }
        }
        let registered = Registered(self, self.next_id());
        let mut timer = Timer::default();
        poll_fn(|cx: &mut Context<'_>| {
            self.machines.check_alive(process)?;
            match step(self.clock.now()) {
                Ok(done) => Poll::Ready(Ok(done)),
                Err(at) => {
                    debug_assert!(at > self.clock.now(), "a timer for a time already past");
                    let entry = (sent, cx.waker().clone());
                    let old = self.waiters.borrow_mut().insert(registered.1, entry);
                    drop(old);
                    let _ = timer.poll_at(&self.clock, at, cx);
                    Poll::Pending
                }
            }
        })
        .await
    }

    /// Wakes every waiting connect to look again.
    fn wake_waiters(&self) {
        let wakers: Vec<Waker> = self
            .waiters
            .borrow()
            .values()
            .map(|(_, w)| w.clone())
            .collect();
        wakers.into_iter().for_each(Waker::wake);
    }

    // Kills and faults

    /// Closes every socket of a process that has died: its ports are freed, a task waiting to
    /// accept on its listeners or to connect for it fails, and each of its connections sends
    /// the other side a reset behind what it already sent.
    pub(crate) fn processes_died(&self) {
        let listeners = self
            .ports
            .borrow_mut()
            .release_dead(|p| self.machines.alive(p));
        for conn in self.conns() {
            conn.close_dead_sides(self);
        }
        listeners.iter().for_each(|l| l.wake());
        self.wake_waiters();
    }

    /// Changes the faults, forgets the spans nothing in transit can meet any more, then wakes
    /// everything waiting on the network to look again.
    fn change_faults(&self, change: impl FnOnce(&mut Faults, Instant)) {
        let now = self.clock.now();
        let conns = self.conns();
        let oldest = conns.iter().filter_map(|c| c.oldest_due());
        let waiting = self.waiters.borrow().values().map(|&(sent, _)| sent).min();
        let horizon = oldest.chain(waiting).fold(now, Instant::min);
        {
            let mut faults = self.faults.borrow_mut();
            change(&mut faults, now);
            faults.forget_before(horizon);
        }
        self.wake_waiters();
        conns.iter().for_each(|c| c.wake());
    }

    pub(crate) fn clog_pair(&self, from: MachineId, to: MachineId, d: Duration) {
        self.change_faults(|f, now| f.clog_pair(from, to, now, d));
    }

    pub(crate) fn clog_sends(&self, machine: MachineId, d: Duration) {
        self.change_faults(|f, now| f.clog_sends(machine, now, d));
    }

    pub(crate) fn clog_receives(&self, machine: MachineId, d: Duration) {
        self.change_faults(|f, now| f.clog_receives(machine, now, d));
    }

    pub(crate) fn partition(&self, from: MachineId, to: MachineId) {
        self.change_faults(|f, now| f.partition(from, to, now));
    }

    pub(crate) fn set_reset_probability(&self, p: f64) {
        assert!(
            (0.0..=1.0).contains(&p),
            "probability {p} is outside [0, 1]"
        );
        self.change_faults(|f, _| f.reset_probability = p);
    }

    pub(crate) fn heal(&self) {
        self.change_faults(Faults::heal);
    }
}

fn timed_out(addr: SocketAddr) -> io::Error {
    io::Error::new(
        io::ErrorKind::TimedOut,
        format!("connecting to {addr} timed out"),
    )
}

#[cfg(test)]
mod tests {
    use basalt_env::{Duration, Priority, SocketAddr};

    use crate::Sim;

    /// Every connect removes the waker it registered once it's done, whichever way it ends.
    #[test]
    fn finished_connects_leave_no_waiters() {
        let sim = Sim::new(0);
        let ctrl = sim.control();
        let a = ctrl.add_machine("a", 0);
        let server = ctrl.add_process(a, "server", |_| Box::pin(async {}));
        let addr = SocketAddr::new(ctrl.ip(a), 80);
        let task = server
            .clone()
            .spawn("driver", Priority::Default, async move {
                let listener = server.listen(80).await.unwrap();
                for _ in 0..20 {
                    let c = server.connect(addr).await.unwrap();
                    drop((c, listener.accept().await.unwrap()));
                }
                drop(listener);
                for _ in 0..20 {
                    server.connect(addr).await.unwrap_err();
                }
            });
        sim.block_on(task).unwrap();
        assert!(sim.world.net.waiters.borrow().is_empty());

        // A connect dropped while it waits removes its waker too.
        let b = ctrl.add_machine("b", 0);
        let client = ctrl.add_process(b, "client", |_| Box::pin(async {}));
        let nowhere = SocketAddr::from(([192, 0, 2, 1], 80));
        let task = client
            .clone()
            .spawn("driver", Priority::Default, async move {
                let connect = client.connect(nowhere);
                assert!(
                    client
                        .timeout(Duration::from_secs(1), connect)
                        .await
                        .is_err()
                );
            });
        sim.block_on(task).unwrap();
        assert!(sim.world.net.waiters.borrow().is_empty());
    }
}
