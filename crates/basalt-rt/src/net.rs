//! TCP on the async-io reactor.

use std::io::{self, IoSlice};
use std::net::{Ipv4Addr, Shutdown, SocketAddr, TcpListener, TcpStream};
use std::pin::Pin;
use std::task::{Context, Poll, ready};

use async_io::Async;
use basalt_env::backend::{ListenerIo, StreamIo};
use futures_io::{AsyncRead, AsyncWrite};
use rustix::io::Errno;

/// Listens on `port` of every local IPv4 address.
pub(crate) fn listen(port: u16) -> io::Result<Box<dyn ListenerIo>> {
    let io = Async::<TcpListener>::bind((Ipv4Addr::UNSPECIFIED, port))?;
    let local = io.get_ref().local_addr()?;
    Ok(Box::new(Listener { io, local }))
}

pub(crate) async fn connect(addr: SocketAddr) -> io::Result<Box<dyn StreamIo>> {
    let io = Async::<TcpStream>::connect(addr).await?;
    Ok(Box::new(Stream::new(io, addr)?))
}

struct Listener {
    io: Async<TcpListener>,
    local: SocketAddr,
}

impl ListenerIo for Listener {
    /// Skips connections that fail because of their peer, such as one reset before it was
    /// accepted, so one bad client can't end a server's accept loop.
    fn poll_accept(&self, cx: &mut Context<'_>) -> Poll<io::Result<Box<dyn StreamIo>>> {
        loop {
            match self.io.get_ref().accept() {
                Ok((stream, peer)) => {
                    if let Ok(stream) = Async::new(stream).and_then(|io| Stream::new(io, peer)) {
                        return Poll::Ready(Ok(Box::new(stream)));
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    ready!(self.io.poll_readable(cx))?;
                }
                Err(e) if is_peer_error(&e) => {}
                Err(e) => return Poll::Ready(Err(e)),
            }
        }
    }

    fn local_addr(&self) -> SocketAddr {
        self.local
    }
}

/// Errors Linux's `accept` reports for a connection that broke before it was accepted, which
/// `accept(2)` says to treat like `EAGAIN`.
fn is_peer_error(e: &io::Error) -> bool {
    matches!(
        Errno::from_io_error(e),
        Some(
            Errno::CONNABORTED
                | Errno::INTR
                | Errno::NETDOWN
                | Errno::PROTO
                | Errno::NOPROTOOPT
                | Errno::HOSTDOWN
                | Errno::NONET
                | Errno::HOSTUNREACH
                | Errno::OPNOTSUPP
                | Errno::NETUNREACH
        )
    )
}

struct Stream {
    io: Async<TcpStream>,
    // Kept from the start: the OS can't report the peer once the connection is reset.
    local: SocketAddr,
    peer: SocketAddr,
}

impl Stream {
    fn new(io: Async<TcpStream>, peer: SocketAddr) -> io::Result<Stream> {
        // Messages are small and latency matters more than packet count.
        io.get_ref().set_nodelay(true)?;
        Ok(Stream {
            local: io.get_ref().local_addr()?,
            peer,
            io,
        })
    }
}

/// Linux reports writing to a connection the peer reset or closed as `BrokenPipe` or
/// `NotConnected`. The `Backend` contract, like the simulator, reports it as `ConnectionReset`.
fn reset<T>(poll: Poll<io::Result<T>>) -> Poll<io::Result<T>> {
    poll.map_err(|e| match e.kind() {
        io::ErrorKind::BrokenPipe | io::ErrorKind::NotConnected => {
            io::Error::new(io::ErrorKind::ConnectionReset, e)
        }
        _ => e,
    })
}

impl StreamIo for Stream {
    fn local_addr(&self) -> SocketAddr {
        self.local
    }

    fn peer_addr(&self) -> SocketAddr {
        self.peer
    }
}

impl AsyncRead for Stream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        reset(Pin::new(&mut self.io).poll_read(cx, buf))
    }
}

impl AsyncWrite for Stream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        reset(Pin::new(&mut self.io).poll_write(cx, buf))
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        reset(Pin::new(&mut self.io).poll_write_vectored(cx, bufs))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        reset(Pin::new(&mut self.io).poll_flush(cx))
    }

    /// Shuts down the write half, as `Stream` documents. async-io's own `poll_close` only
    /// flushes, so the peer would never see the end of the stream.
    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        ready!(reset(Pin::new(&mut self.io).poll_flush(cx)))?;
        reset(Poll::Ready(self.io.get_ref().shutdown(Shutdown::Write)))
    }
}
