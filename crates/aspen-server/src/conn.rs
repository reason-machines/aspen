//! Git over a connection rather than smart HTTP: reading pkt-lines from an
//! async byte stream, and writing responses to one. The SSH transport
//! ([`crate::ssh`]) serves each command over such a stream; the protocol
//! code (`aspen-proto`) is sans-I/O and sees one packet at a time either
//! way.

use std::io;

use aspen_proto::pkt::{self, MAX_PKT_LEN, PREFIX_LEN};
use axum::body::Body;
use futures_util::StreamExt;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Reads whole pkt-lines from a byte stream, one at a time, never reading
/// past the end of the line it returns. That matters: once a negotiation
/// is over the rest of the input belongs to nobody (A1b's C3 obligation),
/// and a push's pack follows its commands directly.
#[derive(Debug)]
pub(crate) struct PktInput<R> {
    inner: R,
    frame: Vec<u8>,
}

impl<R: AsyncRead + Unpin> PktInput<R> {
    pub(crate) fn new(inner: R) -> Self {
        Self {
            inner,
            frame: Vec::with_capacity(PREFIX_LEN),
        }
    }

    /// The next pkt-line, prefix included (decode it with
    /// [`pkt::decode`]), or `None` when the stream ended cleanly between
    /// lines. A stream that ends inside a line, or a malformed length, is
    /// an error.
    pub(crate) async fn next(&mut self) -> io::Result<Option<&[u8]>> {
        self.frame.clear();
        self.frame.resize(PREFIX_LEN, 0);
        let mut got = 0;
        while got < PREFIX_LEN {
            match self.inner.read(&mut self.frame[got..]).await? {
                0 if got == 0 => return Ok(None),
                0 => return Err(truncated()),
                n => got += n,
            }
        }
        if !self.frame.iter().all(u8::is_ascii_hexdigit) {
            return Err(bad_length());
        }
        let len = self
            .frame
            .iter()
            .filter_map(|&d| (d as char).to_digit(16))
            .fold(0usize, |n, d| n * 16 + d as usize);
        // 0, 1 and 2 are the flush, delimiter and response-end packets.
        if len == 3 || len > MAX_PKT_LEN {
            return Err(bad_length());
        }
        if len > PREFIX_LEN {
            self.frame.resize(len, 0);
            self.inner
                .read_exact(&mut self.frame[PREFIX_LEN..])
                .await
                .map_err(|e| match e.kind() {
                    io::ErrorKind::UnexpectedEof => truncated(),
                    _ => e,
                })?;
        }
        Ok(Some(&self.frame))
    }

    /// Reads and discards everything left, until the stream ends or fails.
    pub(crate) async fn drain(mut self) {
        let mut sink = [0u8; 8192];
        while matches!(self.inner.read(&mut sink).await, Ok(n) if n > 0) {}
    }

    /// The stream, for a reader that takes over from here.
    #[cfg(test)]
    pub(crate) fn into_inner(self) -> R {
        self.inner
    }
}

fn bad_length() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "bad pkt-line length")
}

fn truncated() -> io::Error {
    io::Error::new(
        io::ErrorKind::UnexpectedEof,
        "the client hung up inside a pkt-line",
    )
}

/// Decodes a frame [`PktInput::next`] returned. It holds exactly one
/// packet, so this cannot fail on a frame `next` accepted, except for
/// what [`pkt::decode`] checks beyond the length.
pub(crate) fn decode(frame: &[u8]) -> io::Result<pkt::Packet<'_>> {
    match pkt::decode(frame) {
        Ok(Some((packet, used))) if used == frame.len() => Ok(packet),
        Ok(_) => Err(io::Error::new(io::ErrorKind::InvalidData, "bad pkt-line")),
        Err(e) => Err(io::Error::new(io::ErrorKind::InvalidData, e.to_string())),
    }
}

/// Writes `bytes` and flushes them, so the client sees them before the
/// server waits for its next line.
pub(crate) async fn send<W: AsyncWrite + Unpin>(out: &mut W, bytes: &[u8]) -> io::Result<()> {
    out.write_all(bytes).await?;
    out.flush().await
}

/// Writes a whole response body as it streams (a stitched pack), then
/// flushes. A body that fails part-way (a fetch the server had to abort)
/// is an error after the bytes it did produce: the caller ends the
/// connection, so the client sees a short response, as over HTTP.
pub(crate) async fn send_body<W: AsyncWrite + Unpin>(out: &mut W, body: Body) -> io::Result<()> {
    let mut stream = body.into_data_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(io::Error::other)?;
        out.write_all(&chunk).await?;
    }
    out.flush().await
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn frames(input: &[u8]) -> io::Result<Vec<Vec<u8>>> {
        let mut r = PktInput::new(input);
        let mut out = Vec::new();
        while let Some(f) = r.next().await? {
            out.push(f.to_vec());
        }
        Ok(out)
    }

    #[tokio::test]
    async fn reads_one_line_at_a_time_and_stops_at_the_end() {
        let mut input = Vec::new();
        pkt::write_text(&mut input, "want x").unwrap();
        pkt::write_flush(&mut input);
        pkt::write_delim(&mut input);
        pkt::write_response_end(&mut input);
        let got = frames(&input).await.unwrap();
        assert_eq!(
            got,
            vec![
                b"000bwant x\n".to_vec(),
                b"0000".to_vec(),
                b"0001".to_vec(),
                b"0002".to_vec()
            ]
        );
        for f in &got {
            decode(f).unwrap();
        }
    }

    /// Invariant: the reader never consumes a byte past the line it
    /// returns, so whatever follows (a pack, pipelined haves) is left for
    /// the next reader.
    #[tokio::test]
    async fn never_reads_past_the_line_it_returns() {
        let mut input = Vec::new();
        pkt::write_text(&mut input, "done").unwrap();
        input.extend_from_slice(b"PACK rest of the stream");
        let mut r = PktInput::new(&input[..]);
        assert_eq!(r.next().await.unwrap().unwrap(), b"0009done\n");
        let mut rest = Vec::new();
        r.into_inner().read_to_end(&mut rest).await.unwrap();
        assert_eq!(rest, b"PACK rest of the stream");
    }

    #[tokio::test]
    async fn truncated_and_malformed_lines_are_errors() {
        for bad in [&b"00"[..], b"000awant", b"0003", b"zzzz", b"+00a", b"fff1"] {
            assert!(frames(bad).await.is_err(), "{bad:?}");
        }
        assert!(frames(b"").await.unwrap().is_empty());
    }
}
