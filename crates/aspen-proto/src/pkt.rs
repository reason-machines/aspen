//! pkt-line framing (`gitprotocol-common(5)`).
//!
//! A pkt-line is a four-digit hexadecimal length, which counts itself,
//! followed by that many bytes minus four. Three lengths are special:
//! `0000` (flush), `0001` (delimiter, protocol v2) and `0002` (response end,
//! protocol v2). `0003` is never valid. The longest pkt-line is
//! [`MAX_PKT_LEN`] bytes, so one carries at most [`MAX_DATA_LEN`] bytes.
//!
//! The framing here is sans-I/O: [`decode`] reads one packet from the front
//! of a buffer, and the `write_*` functions append to a `Vec<u8>`. Parsers in
//! this crate consume [`Packet`]s, so they work the same behind an async HTTP
//! body, an SSH channel or a blocking pipe. [`PktReader`] is a small blocking
//! adapter over [`std::io::Read`] for pipes and tests.

use std::io::{self, Read};

/// Length of the hexadecimal length prefix.
pub const PREFIX_LEN: usize = 4;

/// Longest pkt-line, prefix included (Git's `LARGE_PACKET_MAX`).
pub const MAX_PKT_LEN: usize = 65520;

/// Most payload bytes one pkt-line can carry.
pub const MAX_DATA_LEN: usize = MAX_PKT_LEN - PREFIX_LEN;

/// The flush packet, `0000`: ends a list or a message.
pub const FLUSH: &[u8; 4] = b"0000";

/// The delimiter packet, `0001`: separates sections in protocol v2.
pub const DELIM: &[u8; 4] = b"0001";

/// The response-end packet, `0002`: ends a stateless v2 response.
pub const RESPONSE_END: &[u8; 4] = b"0002";

/// One packet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Packet<'a> {
    /// `0000`.
    Flush,
    /// `0001`.
    Delim,
    /// `0002`.
    ResponseEnd,
    /// A data packet's payload, possibly empty (`0004`).
    Data(&'a [u8]),
}

impl<'a> Packet<'a> {
    /// The payload of a data packet with one trailing `\n` removed, as Git
    /// reads text lines; `None` for the special packets.
    pub fn text(&self) -> Option<&'a [u8]> {
        match *self {
            Packet::Data(d) => Some(d.strip_suffix(b"\n").unwrap_or(d)),
            _ => None,
        }
    }

    /// How many bytes this packet occupies on the wire.
    pub fn wire_len(&self) -> usize {
        match self {
            Packet::Data(d) => PREFIX_LEN + d.len(),
            _ => PREFIX_LEN,
        }
    }
}

/// A framing error. Every one of these ends the exchange: the stream can't
/// be resynchronised.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PktError {
    /// The length prefix is not four hexadecimal digits.
    #[error("bad pkt-line length prefix {0:?}")]
    BadPrefix([u8; 4]),
    /// `0003`, or another length below four that is not a special packet.
    #[error("invalid pkt-line length {0}")]
    InvalidLength(usize),
    /// A length above [`MAX_PKT_LEN`].
    #[error("pkt-line length {0} is above the limit of {MAX_PKT_LEN}")]
    TooLong(usize),
    /// A payload too long to fit one pkt-line.
    #[error("payload of {0} bytes does not fit one pkt-line (at most {MAX_DATA_LEN})")]
    PayloadTooLong(usize),
    /// A text line that contains a line feed, a NUL or another control
    /// character where the protocol needs exactly one line.
    #[error("text line contains a control character")]
    NotOneLine,
}

#[derive(Clone, Copy)]
enum Kind {
    Flush,
    Delim,
    ResponseEnd,
    Data,
}

fn hex(b: u8) -> Option<usize> {
    // Git's `hexval` accepts both cases; so do we.
    match b {
        b'0'..=b'9' => Some(usize::from(b - b'0')),
        b'a'..=b'f' => Some(usize::from(b - b'a' + 10)),
        b'A'..=b'F' => Some(usize::from(b - b'A' + 10)),
        _ => None,
    }
}

/// Reads the header of the packet at the front of `buf`: its kind and total
/// length. `Ok(None)` means `buf` does not yet hold the whole packet.
fn frame(buf: &[u8]) -> Result<Option<(Kind, usize)>, PktError> {
    let Some(prefix) = buf.first_chunk::<PREFIX_LEN>() else {
        return Ok(None);
    };
    let mut len = 0;
    for &b in prefix {
        len = len * 16 + hex(b).ok_or(PktError::BadPrefix(*prefix))?;
    }
    let kind = match len {
        0 => return Ok(Some((Kind::Flush, PREFIX_LEN))),
        1 => return Ok(Some((Kind::Delim, PREFIX_LEN))),
        2 => return Ok(Some((Kind::ResponseEnd, PREFIX_LEN))),
        3 => return Err(PktError::InvalidLength(len)),
        n if n > MAX_PKT_LEN => return Err(PktError::TooLong(n)),
        _ => Kind::Data,
    };
    Ok((buf.len() >= len).then_some((kind, len)))
}

fn packet(buf: &[u8], kind: Kind, len: usize) -> Packet<'_> {
    match kind {
        Kind::Flush => Packet::Flush,
        Kind::Delim => Packet::Delim,
        Kind::ResponseEnd => Packet::ResponseEnd,
        Kind::Data => Packet::Data(&buf[PREFIX_LEN..len]),
    }
}

/// Decodes the packet at the front of `buf`.
///
/// Returns the packet and how many bytes it used, or `Ok(None)` when `buf`
/// holds only part of it (read more and call again). Bytes after the packet
/// are untouched, so a caller can hand them to the next stage, such as the
/// pack that follows a push's commands.
pub fn decode(buf: &[u8]) -> Result<Option<(Packet<'_>, usize)>, PktError> {
    Ok(frame(buf)?.map(|(kind, len)| (packet(buf, kind, len), len)))
}

fn write_prefix(out: &mut Vec<u8>, len: usize) {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    for shift in [12, 8, 4, 0] {
        out.push(DIGITS[(len >> shift) & 0xf]);
    }
}

/// Appends a data packet holding `data`.
pub fn write_data(out: &mut Vec<u8>, data: &[u8]) -> Result<(), PktError> {
    if data.len() > MAX_DATA_LEN {
        return Err(PktError::PayloadTooLong(data.len()));
    }
    write_prefix(out, data.len() + PREFIX_LEN);
    out.extend_from_slice(data);
    Ok(())
}

/// Appends a text packet: `line` followed by `\n`. `line` must be one line
/// with no control characters (tabs aside), so that a reader that splits on
/// `\n` or stops at NUL sees exactly what was meant.
pub fn write_text(out: &mut Vec<u8>, line: &str) -> Result<(), PktError> {
    if line.bytes().any(|b| b.is_ascii_control() && b != b'\t') {
        return Err(PktError::NotOneLine);
    }
    if line.len() + 1 > MAX_DATA_LEN {
        return Err(PktError::PayloadTooLong(line.len() + 1));
    }
    write_prefix(out, line.len() + 1 + PREFIX_LEN);
    out.extend_from_slice(line.as_bytes());
    out.push(b'\n');
    Ok(())
}

/// Appends `0000`.
pub fn write_flush(out: &mut Vec<u8>) {
    out.extend_from_slice(FLUSH);
}

/// Appends `0001`.
pub fn write_delim(out: &mut Vec<u8>) {
    out.extend_from_slice(DELIM);
}

/// Appends `0002`.
pub fn write_response_end(out: &mut Vec<u8>) {
    out.extend_from_slice(RESPONSE_END);
}

/// Most bytes of a message that [`one_line`] keeps, so that any message fits
/// one pkt-line together with a prefix such as `ERR `, `ng <ref> ` or a
/// side-band byte.
pub const MAX_MESSAGE_LEN: usize = 4096;

/// Makes `message` safe to send as (part of) one text line: every control
/// character becomes a space, and the text is cut to [`MAX_MESSAGE_LEN`]
/// bytes at a character boundary. Used for every server-authored message
/// (`ERR`, `ng` reasons, side-band notices), so a message can never break the
/// framing or forge an extra line.
pub fn one_line(message: &str) -> String {
    let mut out = String::with_capacity(message.len().min(MAX_MESSAGE_LEN));
    for c in message.chars() {
        let c = if c.is_control() { ' ' } else { c };
        if out.len() + c.len_utf8() > MAX_MESSAGE_LEN {
            break;
        }
        out.push(c);
    }
    out
}

/// Appends `ERR <message>`, the packet Git clients print as
/// `fatal: remote error: <message>` before they stop (S5). Valid in place of a v0/v1
/// advertisement, a v2 response, or a push's report.
pub fn write_err(out: &mut Vec<u8>, message: &str) {
    let line = format!("ERR {}", one_line(message));
    write_text(out, &line).expect("one_line output always fits one text pkt-line");
}

/// Why [`PktReader`] failed.
#[derive(Debug, thiserror::Error)]
pub enum ReadError {
    /// The underlying reader failed.
    #[error("read: {0}")]
    Io(#[from] io::Error),
    /// The stream is not valid pkt-line framing.
    #[error(transparent)]
    Pkt(#[from] PktError),
    /// The stream ended inside a packet.
    #[error("stream ended inside a pkt-line")]
    TruncatedPacket,
}

/// A blocking pkt-line reader over any [`Read`].
///
/// It reads ahead, so once the pkt-line part of an exchange is over, take the
/// rest of the stream (for example a pushed pack) with
/// [`PktReader::into_remainder`], which yields the read-ahead bytes first.
#[derive(Debug)]
pub struct PktReader<R> {
    inner: R,
    buf: Vec<u8>,
    start: usize,
}

impl<R: Read> PktReader<R> {
    /// Wraps `inner`.
    pub fn new(inner: R) -> Self {
        Self {
            inner,
            buf: Vec::with_capacity(8192),
            start: 0,
        }
    }

    /// Reads the next packet. `Ok(None)` is a clean end of stream between
    /// packets.
    pub fn read_packet(&mut self) -> Result<Option<Packet<'_>>, ReadError> {
        loop {
            if let Some((kind, len)) = frame(&self.buf[self.start..])? {
                let at = self.start;
                self.start += len;
                return Ok(Some(packet(&self.buf[at..], kind, len)));
            }
            if self.start > 0 {
                self.buf.drain(..self.start);
                self.start = 0;
            }
            let have = self.buf.len();
            self.buf.resize(have + 8192, 0);
            let n = loop {
                match self.inner.read(&mut self.buf[have..]) {
                    Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                    other => break other,
                }
            };
            let n = match n {
                Ok(n) => n,
                Err(e) => {
                    self.buf.truncate(have);
                    return Err(e.into());
                }
            };
            self.buf.truncate(have + n);
            if n == 0 {
                return if have == 0 {
                    Ok(None)
                } else {
                    Err(ReadError::TruncatedPacket)
                };
            }
        }
    }

    /// The rest of the stream: bytes read ahead but not yet consumed as
    /// packets, then the underlying reader.
    pub fn into_remainder(self) -> io::Chain<io::Cursor<Vec<u8>>, R> {
        let mut buf = self.buf;
        buf.drain(..self.start);
        io::Cursor::new(buf).chain(self.inner)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// Deterministic bytes for fuzz-style tests (SplitMix64).
    pub(crate) fn noise(seed: u64, len: usize) -> Vec<u8> {
        let mut s = seed;
        (0..len)
            .map(|_| {
                s = s.wrapping_add(0x9e37_79b9_7f4a_7c15);
                let mut z = s;
                z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
                z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
                (z ^ (z >> 31)) as u8
            })
            .collect()
    }

    #[test]
    fn special_packets_and_known_encodings() {
        // Byte-exact against Git's documented examples.
        let mut out = Vec::new();
        write_text(&mut out, "a").unwrap();
        assert_eq!(out, b"0006a\n");
        out.clear();
        write_data(&mut out, b"").unwrap();
        assert_eq!(out, b"0004");
        out.clear();
        write_flush(&mut out);
        write_delim(&mut out);
        write_response_end(&mut out);
        assert_eq!(out, b"000000010002");
        assert_eq!(decode(b"0000"), Ok(Some((Packet::Flush, 4))));
        assert_eq!(decode(b"0001"), Ok(Some((Packet::Delim, 4))));
        assert_eq!(decode(b"0002"), Ok(Some((Packet::ResponseEnd, 4))));
        assert_eq!(decode(b"0004"), Ok(Some((Packet::Data(b""), 4))));
        assert_eq!(decode(b"0003"), Err(PktError::InvalidLength(3)));
        assert_eq!(
            decode(b"0009x\nyzwEXTRA"),
            Ok(Some((Packet::Data(b"x\nyzw"), 9)))
        );
        assert!(matches!(decode(b"00g0"), Err(PktError::BadPrefix(_))));
        assert_eq!(decode(b"fff1"), Err(PktError::TooLong(0xfff1)));
        assert_eq!(
            decode(b"fff0"),
            Ok(None),
            "max length is valid, needs bytes"
        );
    }

    /// Invariant: whatever the writer emits, the decoder returns unchanged,
    /// at every payload size up to the maximum, and never reads past the
    /// packet's end.
    #[test]
    fn round_trip_at_every_boundary_size() {
        for len in [0, 1, 2, 3, 4, 100, 995, 996, 65514, 65515, MAX_DATA_LEN] {
            let payload = noise(len as u64, len);
            let mut out = Vec::new();
            write_data(&mut out, &payload).unwrap();
            out.extend_from_slice(b"TAIL");
            let (p, n) = decode(&out).unwrap().unwrap();
            assert_eq!(p, Packet::Data(&payload));
            assert_eq!(n, len + 4);
            assert_eq!(&out[n..], b"TAIL");
            // Every strict prefix is "need more", never an error or a packet.
            for cut in [0, 1, 3, 4, n - 1].into_iter().filter(|&c| c < n) {
                assert_eq!(decode(&out[..cut]), Ok(None), "len {len} cut {cut}");
            }
        }
        assert_eq!(
            write_data(&mut Vec::new(), &[0; MAX_DATA_LEN + 1]),
            Err(PktError::PayloadTooLong(MAX_DATA_LEN + 1))
        );
    }

    /// Invariant: the decoder never panics and never claims more bytes than
    /// it was given, on any input (a fuzz-style sweep; `cargo fuzz` targets
    /// come with node D3).
    #[test]
    fn decoder_is_total_on_arbitrary_input() {
        for seed in 0..20_000u64 {
            let mut buf = noise(seed, (seed % 40) as usize);
            // Bias toward plausible prefixes so lengths get exercised.
            if seed % 2 == 0 && buf.len() >= 4 {
                let l = format!("{:04x}", seed % 64);
                buf[..4].copy_from_slice(l.as_bytes());
            }
            let mut rest = &buf[..];
            while let Ok(Some((p, n))) = decode(rest) {
                assert!(n <= rest.len() && n == p.wire_len());
                rest = &rest[n..];
            }
        }
    }

    #[test]
    fn text_lines_are_exactly_one_line() {
        let mut out = Vec::new();
        assert_eq!(write_text(&mut out, "a\nb"), Err(PktError::NotOneLine));
        assert_eq!(write_text(&mut out, "a\0b"), Err(PktError::NotOneLine));
        assert!(out.is_empty(), "nothing written on error");
        assert_eq!(Packet::Data(b"x\n").text(), Some(&b"x"[..]));
        assert_eq!(Packet::Data(b"x\n\n").text(), Some(&b"x\n"[..]));
        assert_eq!(Packet::Data(b"x").text(), Some(&b"x"[..]));
        assert_eq!(Packet::Flush.text(), None);
    }

    /// Invariant: no server-authored message can add a line or overflow a
    /// packet, whatever it contains.
    #[test]
    fn err_messages_cannot_forge_lines_or_overflow() {
        let mut out = Vec::new();
        write_err(&mut out, "bad\nERR injected\0\r");
        assert_eq!(out, b"001bERR bad ERR injected  \n");
        let huge = "é".repeat(MAX_DATA_LEN);
        let mut out = Vec::new();
        write_err(&mut out, &huge);
        let (p, n) = decode(&out).unwrap().unwrap();
        assert_eq!(n, out.len());
        let text = std::str::from_utf8(p.text().unwrap()).unwrap();
        assert!(text.len() <= MAX_MESSAGE_LEN + 4);
    }

    /// A reader that returns one byte per call, to exercise reassembly.
    struct Trickle<'a>(&'a [u8]);
    impl Read for Trickle<'_> {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            let Some((&b, rest)) = self.0.split_first() else {
                return Ok(0);
            };
            buf[0] = b;
            self.0 = rest;
            Ok(1)
        }
    }

    #[test]
    fn reader_reassembles_and_hands_over_the_rest() {
        let mut wire = Vec::new();
        write_text(&mut wire, "one").unwrap();
        write_flush(&mut wire);
        wire.extend_from_slice(b"PACK-bytes-follow");
        let mut r = PktReader::new(Trickle(&wire));
        assert_eq!(r.read_packet().unwrap(), Some(Packet::Data(b"one\n")));
        assert_eq!(r.read_packet().unwrap(), Some(Packet::Flush));
        let mut rest = Vec::new();
        r.into_remainder().read_to_end(&mut rest).unwrap();
        assert_eq!(rest, b"PACK-bytes-follow");

        // Clean EOF between packets, error inside one.
        let mut r = PktReader::new(&b"0000"[..]);
        assert_eq!(r.read_packet().unwrap(), Some(Packet::Flush));
        assert!(r.read_packet().unwrap().is_none());
        let mut r = PktReader::new(&b"0009ab"[..]);
        assert!(matches!(r.read_packet(), Err(ReadError::TruncatedPacket)));
    }

    #[test]
    fn reader_keeps_read_ahead_bytes_after_a_large_packet() {
        let big = noise(7, MAX_DATA_LEN);
        let mut wire = Vec::new();
        write_data(&mut wire, &big).unwrap();
        write_text(&mut wire, "after").unwrap();
        wire.extend_from_slice(b"tail");
        let mut r = PktReader::new(&wire[..]);
        assert_eq!(r.read_packet().unwrap(), Some(Packet::Data(&big)));
        assert_eq!(r.read_packet().unwrap(), Some(Packet::Data(b"after\n")));
        let mut rest = Vec::new();
        r.into_remainder().read_to_end(&mut rest).unwrap();
        assert_eq!(rest, b"tail");
    }
}
