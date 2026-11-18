//! The ABD client: reads and writes through a majority of replicas.

use std::cell::RefCell;
use std::future::poll_fn;
use std::io;
use std::pin::Pin;
use std::rc::Rc;
use std::task::Poll;

use basalt_env::{Ctx, Duration, Priority, SocketAddr, Stream, Task, trace};

use crate::wire::{self, Request, Response};
use crate::{Bugs, Tag};

/// How long one request to one replica may take before the client drops the connection and
/// tries again. Longer than the network's slow tail plus a disk sync, and much shorter than the
/// 5 to 30 seconds a partitioned connection takes to reset.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(2);

/// How long the client waits before retrying a replica after a failed request.
const MIN_BACKOFF: Duration = Duration::from_millis(10);
const MAX_BACKOFF: Duration = Duration::from_millis(100);

/// A register client. Each client needs its own `writer` id, distinct from every other
/// client's, so two writes never share a [`Tag`].
///
/// An operation retries each replica until a majority answers, so it completes once a majority
/// is reachable and otherwise never returns. Bound it with [`Ctx::timeout`]; dropping an
/// operation cancels it, and a cancelled write may or may not take effect.
pub struct Client {
    ctx: Ctx,
    replicas: Vec<Replica>,
    writer: u64,
    bugs: Bugs,
    /// The highest timestamp this client has written, so its next write uses a higher one
    /// even if the majority it reads from never saw the last one.
    last_ts: u64,
    next_request: u64,
}

struct Replica {
    addr: SocketAddr,
    /// The open connection, if any. A request takes it out while using it, so a cancelled
    /// request drops it rather than leaving half a frame for the next one.
    conn: Rc<RefCell<Option<Stream>>>,
}

impl Client {
    /// A client of the replicas at `replicas` (3 or 5 of them), acting as `ctx`'s process.
    /// Only [`Bugs::skip_writeback`] affects clients.
    pub fn new(ctx: Ctx, replicas: Vec<SocketAddr>, writer: u64, bugs: Bugs) -> Client {
        assert!(!replicas.is_empty(), "a register needs replicas");
        Client {
            ctx,
            replicas: replicas
                .into_iter()
                .map(|addr| Replica {
                    addr,
                    conn: Rc::default(),
                })
                .collect(),
            writer,
            bugs,
            last_ts: 0,
            next_request: 0,
        }
    }

    /// Writes `value` to `key`: reads the highest timestamp from a majority, then stores the
    /// value under the next one on a majority.
    pub async fn write(&mut self, key: u64, value: Vec<u8>) {
        let mut ts = self.last_ts;
        for response in self.quorum(Request::Get { key }).await {
            ts = ts.max(expect_value(response).0.ts);
        }
        let tag = Tag {
            ts: ts + 1,
            writer: self.writer,
        };
        self.last_ts = tag.ts;
        self.store(key, tag, value).await;
    }

    /// Reads `key`: takes the value with the highest tag from a majority and writes it back to
    /// a majority before returning it, so no later read returns an older value. `None` if the
    /// key was never written.
    pub async fn read(&mut self, key: u64) -> Option<Vec<u8>> {
        let (tag, value) = self
            .quorum(Request::Get { key })
            .await
            .into_iter()
            .map(expect_value)
            .max_by_key(|(tag, _)| *tag)
            .expect("a quorum has at least one response");
        if tag == Tag::default() {
            return None;
        }
        if !self.bugs.skip_writeback {
            self.store(key, tag, value.clone()).await;
        }
        Some(value)
    }

    async fn store(&mut self, key: u64, tag: Tag, value: Vec<u8>) {
        for response in self.quorum(Request::Put { key, tag, value }).await {
            assert_eq!(response, Response::Ack, "a replica answered a put wrongly");
        }
    }

    /// Sends `request` to every replica and returns the first answers from a majority. Requests
    /// to the other replicas are cancelled, which drops their connections, since one may hold
    /// half a frame; the next operation reconnects to them. Letting them run on instead would
    /// leave calls to a down replica retrying forever.
    async fn quorum(&mut self, request: Request) -> Vec<Response> {
        let id = self.next_request;
        self.next_request += 1;
        let frame: Rc<[u8]> = wire::encode_request(id, &request).into();
        let mut calls: Vec<Option<Task<Response>>> = self
            .replicas
            .iter()
            .map(|replica| {
                let call = replica.call(self.ctx.clone(), id, frame.clone());
                Some(self.ctx.spawn("register-call", Priority::Default, call))
            })
            .collect();
        let majority = self.replicas.len() / 2 + 1;
        let mut responses = Vec::with_capacity(majority);
        poll_fn(|cx| {
            for slot in &mut calls {
                if let Some(task) = slot
                    && let Poll::Ready(response) = Pin::new(task).poll(cx)
                {
                    responses.push(response);
                    *slot = None;
                }
            }
            if responses.len() >= majority {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
        responses
    }
}

fn expect_value(response: Response) -> (Tag, Vec<u8>) {
    match response {
        Response::Value { tag, value } => (tag, value),
        Response::Ack => panic!("a replica acknowledged a get"),
    }
}

impl Replica {
    /// Sends one request to this replica and returns its answer, reconnecting and retrying
    /// until it gets one. The future owns what it uses, so it can run as a task of its own.
    fn call(&self, ctx: Ctx, id: u64, frame: Rc<[u8]>) -> impl Future<Output = Response> + use<> {
        let (conn, addr) = (self.conn.clone(), self.addr);
        async move {
            loop {
                let taken = conn.borrow_mut().take();
                let attempt = async {
                    let mut stream = match taken {
                        Some(stream) => stream,
                        None => ctx.connect(addr).await?,
                    };
                    wire::send(&mut stream, &frame).await?;
                    loop {
                        let (answers, response) =
                            wire::decode_response(&wire::read_frame(&mut stream).await?)?;
                        // A stale answer, such as to a resent request or an earlier phase.
                        if answers == id {
                            return io::Result::Ok((stream, response));
                        }
                    }
                };
                match ctx.timeout(REQUEST_TIMEOUT, attempt).await {
                    Ok(Ok((stream, response))) => {
                        *conn.borrow_mut() = Some(stream);
                        return response;
                    }
                    Ok(Err(e)) => trace!(Info, "RegisterRequestFailed", replica = addr, error = e),
                    Err(_) => trace!(Info, "RegisterRequestTimedOut", replica = addr),
                }
                ctx.sleep(ctx.rng().range(MIN_BACKOFF..=MAX_BACKOFF)).await;
            }
        }
    }
}
