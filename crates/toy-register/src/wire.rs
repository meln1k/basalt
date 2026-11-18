//! Messages between clients and replicas, and the record a replica stores per key.
//!
//! A message is a frame: a little-endian `u32` body length, then the body. A request body is the
//! request id, a kind byte, and the key, followed for a put by the tag and the value. A response
//! body is the request id it answers and a kind byte, followed for a value by the tag and the
//! value. A request id is echoed back, so a client can skip a stale response, such as the answer
//! to a duplicate.

use std::future::poll_fn;
use std::io::{self, ErrorKind};
use std::pin::Pin;

use futures_io::{AsyncRead, AsyncWrite};

use crate::Tag;

/// Frames longer than this are rejected, so a corrupt length can't allocate gigabytes.
const MAX_FRAME: usize = 16 << 20;

const GET: u8 = 1;
const PUT: u8 = 2;
const VALUE: u8 = 3;
const ACK: u8 = 4;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Request {
    /// The replica's tag and value for `key`.
    Get { key: u64 },
    /// Store `value` under `tag` unless the replica has a higher tag, and acknowledge once the
    /// replica durably holds `tag` or higher.
    Put { key: u64, tag: Tag, value: Vec<u8> },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Response {
    /// A key's tag and value. A key never written has the zero tag and an empty value.
    Value {
        tag: Tag,
        value: Vec<u8>,
    },
    Ack,
}

pub(crate) fn encode_request(id: u64, request: &Request) -> Vec<u8> {
    let mut out = Writer::frame();
    out.u64(id);
    match request {
        Request::Get { key } => {
            out.u8(GET);
            out.u64(*key);
        }
        Request::Put { key, tag, value } => {
            out.u8(PUT);
            out.u64(*key);
            out.tag(*tag);
            out.bytes(value);
        }
    }
    out.finish_frame()
}

/// Decodes a request body, without its length prefix.
pub(crate) fn decode_request(body: &[u8]) -> io::Result<(u64, Request)> {
    let mut r = Reader(body);
    let id = r.u64()?;
    let request = match r.u8()? {
        GET => Request::Get { key: r.u64()? },
        PUT => Request::Put {
            key: r.u64()?,
            tag: r.tag()?,
            value: r.bytes()?,
        },
        kind => return Err(invalid(format!("unknown request kind {kind}"))),
    };
    r.end()?;
    Ok((id, request))
}

pub(crate) fn encode_response(id: u64, response: &Response) -> Vec<u8> {
    let mut out = Writer::frame();
    out.u64(id);
    match response {
        Response::Value { tag, value } => {
            out.u8(VALUE);
            out.tag(*tag);
            out.bytes(value);
        }
        Response::Ack => out.u8(ACK),
    }
    out.finish_frame()
}

/// Decodes a response body, without its length prefix.
pub(crate) fn decode_response(body: &[u8]) -> io::Result<(u64, Response)> {
    let mut r = Reader(body);
    let id = r.u64()?;
    let response = match r.u8()? {
        VALUE => Response::Value {
            tag: r.tag()?,
            value: r.bytes()?,
        },
        ACK => Response::Ack,
        kind => return Err(invalid(format!("unknown response kind {kind}"))),
    };
    r.end()?;
    Ok((id, response))
}

/// The file a replica stores a key in: the tag, the value, and a checksum of both.
pub(crate) fn encode_record(tag: Tag, value: &[u8]) -> Vec<u8> {
    let mut out = Writer(Vec::with_capacity(value.len() + 28));
    out.tag(tag);
    out.bytes(value);
    let sum = checksum(&out.0);
    out.u64(sum);
    out.0
}

/// Decodes a stored record. Fails with `InvalidData` if it is short, long, or its checksum
/// doesn't match, as after a torn write or a bit flip on read.
pub(crate) fn decode_record(data: &[u8]) -> io::Result<(Tag, Vec<u8>)> {
    let Some(split) = data.len().checked_sub(8) else {
        return Err(invalid("record too short".into()));
    };
    let (body, sum) = data.split_at(split);
    if checksum(body).to_le_bytes() != sum {
        return Err(invalid("record checksum mismatch".into()));
    }
    let mut r = Reader(body);
    let record = (r.tag()?, r.bytes()?);
    r.end()?;
    Ok(record)
}

/// Reads one frame and returns its body.
pub(crate) async fn read_frame(stream: &mut (impl AsyncRead + Unpin)) -> io::Result<Vec<u8>> {
    let mut len = [0; 4];
    read_exact(stream, &mut len).await?;
    let len = u32::from_le_bytes(len) as usize;
    if len > MAX_FRAME {
        return Err(invalid(format!("frame of {len} bytes")));
    }
    let mut body = vec![0; len];
    read_exact(stream, &mut body).await?;
    Ok(body)
}

async fn read_exact(stream: &mut (impl AsyncRead + Unpin), buf: &mut [u8]) -> io::Result<()> {
    let mut done = 0;
    while done < buf.len() {
        let n = poll_fn(|cx| Pin::new(&mut *stream).poll_read(cx, &mut buf[done..])).await?;
        if n == 0 {
            return Err(ErrorKind::UnexpectedEof.into());
        }
        done += n;
    }
    Ok(())
}

/// Sends one encoded frame. Sometimes sends it twice, as a retry over a slow link would, so
/// receivers must tolerate duplicates: replicas apply puts idempotently, and clients skip
/// responses whose request id they aren't waiting for.
pub(crate) async fn send(stream: &mut (impl AsyncWrite + Unpin), frame: &[u8]) -> io::Result<()> {
    write_all(stream, frame).await?;
    if basalt_env::buggify!() {
        basalt_env::trace!(Info, "MessageResent", bytes = frame.len());
        write_all(stream, frame).await?;
    }
    Ok(())
}

/// Writes all of `buf` and flushes it.
async fn write_all(stream: &mut (impl AsyncWrite + Unpin), buf: &[u8]) -> io::Result<()> {
    let mut done = 0;
    while done < buf.len() {
        let n = poll_fn(|cx| Pin::new(&mut *stream).poll_write(cx, &buf[done..])).await?;
        if n == 0 {
            return Err(ErrorKind::WriteZero.into());
        }
        done += n;
    }
    poll_fn(|cx| Pin::new(&mut *stream).poll_flush(cx)).await
}

/// 64-bit FNV-1a.
fn checksum(data: &[u8]) -> u64 {
    data.iter().fold(0xcbf2_9ce4_8422_2325, |h, &b| {
        (h ^ u64::from(b)).wrapping_mul(0x0000_0100_0000_01b3)
    })
}

fn invalid(msg: String) -> io::Error {
    io::Error::new(ErrorKind::InvalidData, msg)
}

struct Writer(Vec<u8>);

impl Writer {
    /// A buffer with room for the length prefix.
    fn frame() -> Writer {
        Writer(vec![0; 4])
    }

    fn finish_frame(mut self) -> Vec<u8> {
        let len = u32::try_from(self.0.len() - 4).expect("frame fits in u32");
        self.0[..4].copy_from_slice(&len.to_le_bytes());
        self.0
    }

    fn u8(&mut self, v: u8) {
        self.0.push(v);
    }

    fn u64(&mut self, v: u64) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }

    fn tag(&mut self, tag: Tag) {
        self.u64(tag.ts);
        self.u64(tag.writer);
    }

    fn bytes(&mut self, v: &[u8]) {
        let len = u32::try_from(v.len()).expect("value fits in u32");
        self.0.extend_from_slice(&len.to_le_bytes());
        self.0.extend_from_slice(v);
    }
}

struct Reader<'a>(&'a [u8]);

impl Reader<'_> {
    fn take(&mut self, n: usize) -> io::Result<&[u8]> {
        if self.0.len() < n {
            return Err(invalid("message too short".into()));
        }
        let (head, rest) = self.0.split_at(n);
        self.0 = rest;
        Ok(head)
    }

    fn u8(&mut self) -> io::Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> io::Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }

    fn u64(&mut self) -> io::Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }

    fn tag(&mut self) -> io::Result<Tag> {
        Ok(Tag {
            ts: self.u64()?,
            writer: self.u64()?,
        })
    }

    fn bytes(&mut self) -> io::Result<Vec<u8>> {
        let len = self.u32()? as usize;
        Ok(self.take(len)?.to_vec())
    }

    fn end(&self) -> io::Result<()> {
        if self.0.is_empty() {
            Ok(())
        } else {
            Err(invalid("trailing bytes".into()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn messages_round_trip() {
        let tag = Tag { ts: 7, writer: 3 };
        for request in [
            Request::Get { key: 9 },
            Request::Put {
                key: 9,
                tag,
                value: b"abc".to_vec(),
            },
        ] {
            let frame = encode_request(42, &request);
            assert_eq!(frame[..4], (frame.len() as u32 - 4).to_le_bytes());
            assert_eq!(decode_request(&frame[4..]).unwrap(), (42, request));
        }
        for response in [
            Response::Value {
                tag,
                value: vec![1, 2],
            },
            Response::Ack,
        ] {
            let frame = encode_response(5, &response);
            assert_eq!(decode_response(&frame[4..]).unwrap(), (5, response));
        }
    }

    #[test]
    fn a_damaged_record_is_rejected() {
        let tag = Tag { ts: 1, writer: 2 };
        let record = encode_record(tag, b"value");
        assert_eq!(decode_record(&record).unwrap(), (tag, b"value".to_vec()));
        for bit in 0..record.len() * 8 {
            let mut flipped = record.clone();
            flipped[bit / 8] ^= 1 << (bit % 8);
            assert!(decode_record(&flipped).is_err(), "bit {bit} flipped");
        }
        for len in 0..record.len() {
            assert!(decode_record(&record[..len]).is_err(), "cut to {len}");
        }
        assert!(decode_record(&[0; 64]).is_err(), "a zeroed page");
    }
}
