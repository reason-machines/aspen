//! Side-band multiplexing (`gitprotocol-pack(5)`, "side-band").
//!
//! With side-band, each data pkt-line starts with one band byte: 1 for pack
//! (or report) data, 2 for progress text the client prints as `remote: ...`,
//! 3 for a fatal error. Stock git prints band 3 as `remote: ...` too (not
//! `remote error:`, which is for `ERR` lines) and then fails the transfer,
//! so the message must say what went wrong on its own. `side-band` limits a packet to 1000 bytes and `side-band-64k` to
//! 65520; protocol v2's `packfile` section always uses the 64k form.

use crate::pkt::{self, MAX_PKT_LEN, PREFIX_LEN};

/// A side-band channel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Band {
    /// 1: pack data, or a push's report-status.
    Data = 1,
    /// 2: progress and notices, printed by the client as `remote: ...`.
    Progress = 2,
    /// 3: a fatal error; the client prints it as `remote: ...` and fails.
    Error = 3,
}

/// Which side-band variant was negotiated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SidebandMode {
    /// `side-band`: packets of at most 1000 bytes.
    Small,
    /// `side-band-64k` (and every v2 `packfile` section): packets of at most
    /// 65520 bytes.
    Large,
}

impl SidebandMode {
    /// Largest whole packet, prefix included.
    pub const fn max_packet(self) -> usize {
        match self {
            Self::Small => 1000,
            Self::Large => MAX_PKT_LEN,
        }
    }

    /// Most payload bytes per packet after the band byte.
    pub const fn max_payload(self) -> usize {
        self.max_packet() - PREFIX_LEN - 1
    }
}

/// Appends `data` on `band`, split into as many packets as `mode` needs.
/// Empty `data` writes nothing: an empty band packet would carry no
/// information and some clients treat a bare band byte as noise.
///
/// Crate-private, so raw bytes reach band 2 or 3 only from inside this
/// crate: outside it, text goes through [`write_message`], which
/// sanitises it, and pack data through `v2::FetchResponse::pack_data`.
pub(crate) fn write_band(out: &mut Vec<u8>, mode: SidebandMode, band: Band, data: &[u8]) {
    let mut payload = Vec::with_capacity(mode.max_payload().min(data.len()) + 1);
    for chunk in data.chunks(mode.max_payload()) {
        payload.clear();
        payload.push(band as u8);
        payload.extend_from_slice(chunk);
        pkt::write_data(out, &payload).expect("chunks fit one packet by construction");
    }
}

/// Appends a one-line message on `band`, sanitised with [`pkt::one_line`]
/// and ended with `\n` so the client prints it on a line of its own.
pub fn write_message(out: &mut Vec<u8>, mode: SidebandMode, band: Band, message: &str) {
    let mut line = pkt::one_line(message).into_bytes();
    line.push(b'\n');
    write_band(out, mode, band, &line);
}

/// Why a packet is not a valid side-band packet.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SidebandError {
    /// A data packet with no band byte.
    #[error("empty side-band packet")]
    Empty,
    /// A band byte other than 1, 2 or 3.
    #[error("unknown side-band channel {0}")]
    UnknownBand(u8),
}

/// Splits a side-band packet's payload into its band and data. The client
/// side of the demultiplexer, used by tests and by mirror mode.
pub fn demux(payload: &[u8]) -> Result<(Band, &[u8]), SidebandError> {
    let (&b, rest) = payload.split_first().ok_or(SidebandError::Empty)?;
    let band = match b {
        1 => Band::Data,
        2 => Band::Progress,
        3 => Band::Error,
        other => return Err(SidebandError::UnknownBand(other)),
    };
    Ok((band, rest))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pkt::{Packet, decode};

    fn unmux(mut wire: &[u8]) -> Vec<(Band, Vec<u8>, usize)> {
        let mut out = Vec::new();
        while let Some((p, n)) = decode(wire).unwrap() {
            let Packet::Data(d) = p else {
                panic!("special packet")
            };
            let (band, data) = demux(d).unwrap();
            out.push((band, data.to_vec(), n));
            wire = &wire[n..];
        }
        assert!(wire.is_empty());
        out
    }

    /// Invariant: band data is split so that no packet exceeds the mode's
    /// limit, and the pieces concatenate back to the input exactly.
    #[test]
    fn chunks_respect_the_mode_limit_and_reassemble() {
        for mode in [SidebandMode::Small, SidebandMode::Large] {
            for len in [
                1,
                mode.max_payload() - 1,
                mode.max_payload(),
                mode.max_payload() + 1,
                3 * mode.max_payload() + 7,
            ] {
                let data: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
                let mut out = Vec::new();
                write_band(&mut out, mode, Band::Data, &data);
                let pieces = unmux(&out);
                assert_eq!(pieces.len(), len.div_ceil(mode.max_payload()));
                assert!(
                    pieces
                        .iter()
                        .all(|(b, _, n)| *b == Band::Data && *n <= mode.max_packet())
                );
                let joined: Vec<u8> = pieces.into_iter().flat_map(|(_, d, _)| d).collect();
                assert_eq!(joined, data);
            }
        }
        // Git's limits: 1000 and 65520 bytes per packet, band byte included.
        assert_eq!(SidebandMode::Small.max_payload(), 995);
        assert_eq!(SidebandMode::Large.max_payload(), 65515);
        let mut out = Vec::new();
        write_band(&mut out, SidebandMode::Large, Band::Data, b"");
        assert!(out.is_empty());
    }

    #[test]
    fn messages_are_one_line_on_their_band() {
        let mut out = Vec::new();
        write_message(&mut out, SidebandMode::Large, Band::Progress, "two\nlines");
        assert_eq!(out, b"000f\x02two lines\n");
        assert_eq!(demux(b""), Err(SidebandError::Empty));
        assert_eq!(demux(b"\x04x"), Err(SidebandError::UnknownBand(4)));
        assert_eq!(demux(b"\x03boom"), Ok((Band::Error, &b"boom"[..])));
    }
}
