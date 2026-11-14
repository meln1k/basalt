use std::fmt;
use std::future::poll_fn;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_io::{AsyncRead, AsyncWrite};

pub use std::net::{IpAddr, SocketAddr};

use crate::backend::{ListenerIo, StreamIo};

/// One side of a connection: an ordered, reliable byte stream, like TCP. From
/// [`Ctx::connect`](crate::Ctx::connect) or [`Listener::accept`].
///
/// It implements `futures-io`'s `AsyncRead` and `AsyncWrite`. Closing it shuts down the write
/// half.
pub struct Stream {
    io: Box<dyn StreamIo>,
}

impl Stream {
    pub(crate) fn new(io: Box<dyn StreamIo>) -> Stream {
        Stream { io }
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.io.local_addr()
    }

    pub fn peer_addr(&self) -> SocketAddr {
        self.io.peer_addr()
    }
}

impl fmt::Debug for Stream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Stream")
            .field("local", &self.local_addr())
            .field("peer", &self.peer_addr())
            .finish()
    }
}

impl AsyncRead for Stream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut *self.io).poll_read(cx, buf)
    }
}

impl AsyncWrite for Stream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut *self.io).poll_write(cx, buf)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut *self.io).poll_write_vectored(cx, bufs)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut *self.io).poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut *self.io).poll_close(cx)
    }
}

/// A listening socket, from [`Ctx::listen`](crate::Ctx::listen).
pub struct Listener {
    io: Box<dyn ListenerIo>,
}

impl Listener {
    pub(crate) fn new(io: Box<dyn ListenerIo>) -> Listener {
        Listener { io }
    }

    /// Waits for the next incoming connection and returns it with the peer's address.
    pub async fn accept(&self) -> io::Result<(Stream, SocketAddr)> {
        let io = poll_fn(|cx| self.io.poll_accept(cx)).await?;
        let peer = io.peer_addr();
        Ok((Stream::new(io), peer))
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.io.local_addr()
    }
}

impl fmt::Debug for Listener {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Listener")
            .field("local", &self.local_addr())
            .finish()
    }
}
