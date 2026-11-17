//! Listening sockets.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::io;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use basalt_env::backend::{ListenerIo, StreamIo};
use basalt_env::{ProcessId, SocketAddr};

use super::Net;
use super::conn::End;
use crate::machine::MachineId;

/// What a listener shares with the connects that reach it: the connections its process hasn't
/// accepted yet.
pub(super) struct Backlog {
    pub(super) process: ProcessId,
    queue: RefCell<VecDeque<End>>,
    /// Every task waiting to accept. `Listener::accept` takes `&self`, so there may be several.
    /// An accept that's cancelled leaves its waker until the next connection or the process's
    /// death wakes them all, which at worst polls a task once for nothing.
    wakers: RefCell<Vec<Waker>>,
}

impl Backlog {
    pub(super) fn new(process: ProcessId) -> Backlog {
        Backlog {
            process,
            queue: RefCell::default(),
            wakers: RefCell::default(),
        }
    }

    pub(super) fn push(&self, end: End) {
        self.queue.borrow_mut().push_back(end);
        self.wake();
    }

    /// Wakes every task waiting to accept.
    pub(super) fn wake(&self) {
        let wakers = std::mem::take(&mut *self.wakers.borrow_mut());
        wakers.into_iter().for_each(Waker::wake);
    }
}

pub(super) struct SimListener {
    pub(super) net: Rc<Net>,
    pub(super) backlog: Rc<Backlog>,
    pub(super) machine: MachineId,
    pub(super) addr: SocketAddr,
    /// The id it holds its port under.
    pub(super) owner: u64,
}

impl ListenerIo for SimListener {
    fn poll_accept(&self, cx: &mut Context<'_>) -> Poll<io::Result<Box<dyn StreamIo>>> {
        self.net.machines.check_alive(self.backlog.process)?;
        let next = self.backlog.queue.borrow_mut().pop_front();
        match next {
            Some(end) => Poll::Ready(Ok(Box::new(end))),
            None => {
                let mut wakers = self.backlog.wakers.borrow_mut();
                if !wakers.iter().any(|w| w.will_wake(cx.waker())) {
                    wakers.push(cx.waker().clone());
                }
                Poll::Pending
            }
        }
    }

    fn local_addr(&self) -> SocketAddr {
        self.addr
    }
}

impl Drop for SimListener {
    /// Frees the port and closes every connection not yet accepted.
    fn drop(&mut self) {
        self.net
            .ports
            .borrow_mut()
            .release(self.machine, self.addr.port(), self.owner);
        let queue = std::mem::take(&mut *self.backlog.queue.borrow_mut());
        drop(queue);
    }
}
