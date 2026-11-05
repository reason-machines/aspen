//! The client side of Git's smart-HTTP protocol, sans I/O: the request
//! bodies mirror mode sends to an upstream and parsers for its answers.
//!
//! | Exchange | Request | Answer |
//! |---|---|---|
//! | v2 capabilities | `GET info/refs?service=git-upload-pack`, `Git-Protocol: version=2` | [`parse_v2_advertisement`] |
//! | `ls-refs` | [`ls_refs_request`] | [`parse_ls_refs`] |
//! | `fetch` | [`fetch_request`] | [`FetchReader`] (streams the pack) |
//! | push advertisement | `GET info/refs?service=git-receive-pack` | [`parse_push_advertisement`] |
//! | push | [`push_request`], then the pack | [`parse_push_report`] |
//!
//! Fetches use protocol v2, which every current forge speaks; pushes use
//! the v0 receive-pack exchange, which is the only one Git has. The
//! [`crate::upstream`] module does the HTTP around these.

use std::collections::BTreeMap;
use std::io::Write;

use aspen_core::id::ObjectId;
use aspen_core::name::RefName;
use aspen_proto::pkt::{self, Packet};
use aspen_proto::sideband::{self, Band};

use crate::RefUpdate;

/// The `agent=` value mirror mode sends.
pub const AGENT: &str = concat!("aspen-mirror/", env!("CARGO_PKG_VERSION"));

/// Ref prefixes mirror mode lists and mirrors: branches and tags.
pub const MIRRORED_PREFIXES: [&str; 2] = ["refs/heads/", "refs/tags/"];

/// Something the upstream sent that does not follow the protocol, or an
/// error it reported.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum WireError {
    /// The bytes do not parse.
    #[error("protocol error from the upstream: {0}")]
    Protocol(String),
    /// The upstream answered with `ERR` or a side-band error.
    #[error("the upstream reported an error: {0}")]
    Remote(String),
    /// The upstream lacks something mirror mode needs.
    #[error("the upstream does not support {0}")]
    Unsupported(&'static str),
}

fn protocol(what: impl Into<String>) -> WireError {
    WireError::Protocol(what.into())
}

fn text(p: &Packet<'_>) -> Option<String> {
    p.text().map(|t| String::from_utf8_lossy(t).into_owned())
}

/// Every packet of a complete body, in order.
fn packets(mut body: &[u8]) -> Result<Vec<Packet<'_>>, WireError> {
    let mut out = Vec::new();
    while !body.is_empty() {
        let (p, used) = pkt::decode(body)
            .map_err(|e| protocol(e.to_string()))?
            .ok_or_else(|| protocol("the response ends inside a pkt-line"))?;
        out.push(p);
        body = &body[used..];
    }
    Ok(out)
}

/// Skips the `# service=...` line and its flush that smart HTTP puts in
/// front of an advertisement (servers may leave them out for v2).
fn skip_service_header<'a>(packets: &'a [Packet<'a>], service: &str) -> &'a [Packet<'a>] {
    match packets {
        [first, Packet::Flush, rest @ ..]
            if text(first).as_deref() == Some(&format!("# service={service}")) =>
        {
            rest
        }
        _ => packets,
    }
}

fn remote_err(line: &str) -> Option<WireError> {
    line.strip_prefix("ERR ")
        .map(|m| WireError::Remote(m.to_owned()))
}

/// What a protocol v2 upload-pack advertisement offers that matters here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct V2Caps {
    /// `object-format` was advertised (then requests name `sha1`).
    pub object_format: bool,
}

/// Parses the answer to `GET info/refs?service=git-upload-pack` sent with
/// `Git-Protocol: version=2`. A server that answers with a v0
/// advertisement does not speak v2.
pub fn parse_v2_advertisement(body: &[u8]) -> Result<V2Caps, WireError> {
    let all = packets(body)?;
    let rest = skip_service_header(&all, "git-upload-pack");
    let mut lines = rest.iter();
    match lines.next().and_then(text) {
        Some(v) if v == "version 2" => {}
        Some(v) => {
            if let Some(e) = remote_err(&v) {
                return Err(e);
            }
            return Err(WireError::Unsupported("Git protocol version 2"));
        }
        None => return Err(protocol("empty capability advertisement")),
    }
    let mut caps = V2Caps::default();
    let (mut ls_refs, mut fetch) = (false, false);
    for p in lines {
        let Some(line) = text(p) else { break };
        let key = line.split('=').next().unwrap_or("");
        match key {
            "ls-refs" => ls_refs = true,
            "fetch" => fetch = true,
            "object-format" => {
                if line != "object-format=sha1" {
                    return Err(WireError::Unsupported("SHA-1 repositories only"));
                }
                caps.object_format = true;
            }
            _ => {}
        }
    }
    if !ls_refs {
        return Err(WireError::Unsupported("the v2 ls-refs command"));
    }
    if !fetch {
        return Err(WireError::Unsupported("the v2 fetch command"));
    }
    Ok(caps)
}

fn command_header(out: &mut Vec<u8>, command: &str, caps: V2Caps) {
    pkt::write_text(out, &format!("command={command}")).expect("short line");
    pkt::write_text(out, &format!("agent={AGENT}")).expect("short line");
    if caps.object_format {
        pkt::write_text(out, "object-format=sha1").expect("short line");
    }
    pkt::write_delim(out);
}

/// The `ls-refs` request: `HEAD` with its symref target, branches and
/// tags. Peeled values are not needed: mirror mode copies tag objects.
pub fn ls_refs_request(caps: V2Caps) -> Vec<u8> {
    let mut out = Vec::new();
    command_header(&mut out, "ls-refs", caps);
    pkt::write_text(&mut out, "symrefs").expect("short line");
    pkt::write_text(&mut out, "ref-prefix HEAD").expect("short line");
    for prefix in MIRRORED_PREFIXES {
        pkt::write_text(&mut out, &format!("ref-prefix {prefix}")).expect("short line");
    }
    pkt::write_flush(&mut out);
    out
}

/// The upstream's refs, from `ls-refs`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ListedRefs {
    /// The branch `HEAD` points at, if `HEAD` is a symref to a branch.
    pub head: Option<RefName>,
    /// Branches and tags, by name.
    pub refs: BTreeMap<RefName, ObjectId>,
    /// Refs under the mirrored prefixes whose names Aspen cannot store
    /// (for example not UTF-8, or longer than 1,024 bytes).
    pub unstorable: Vec<String>,
}

/// Parses an `ls-refs` answer.
pub fn parse_ls_refs(body: &[u8]) -> Result<ListedRefs, WireError> {
    let mut out = ListedRefs::default();
    let all = packets(body)?;
    let mut ended = false;
    for p in &all {
        let line = match p {
            Packet::Flush => {
                ended = true;
                break;
            }
            Packet::Data(_) => text(p).unwrap_or_default(),
            _ => return Err(protocol("unexpected special packet in ls-refs")),
        };
        if let Some(e) = remote_err(&line) {
            return Err(e);
        }
        let mut words = line.split(' ');
        let (Some(oid), Some(name)) = (words.next(), words.next()) else {
            return Err(protocol(format!("bad ls-refs line {line:?}")));
        };
        let symref = words.find_map(|w| w.strip_prefix("symref-target:"));
        if name == "HEAD" {
            out.head = symref
                .and_then(|t| RefName::new(t).ok())
                .filter(RefName::is_branch);
            continue;
        }
        if !MIRRORED_PREFIXES.iter().any(|p| name.starts_with(p)) {
            continue;
        }
        let oid = ObjectId::parse_hex(oid)
            .map_err(|e| protocol(format!("bad object ID in ls-refs: {e}")))?;
        match RefName::new(name) {
            Ok(n) => {
                out.refs.insert(n, oid);
            }
            Err(_) => out.unstorable.push(name.to_owned()),
        }
    }
    if !ended {
        return Err(protocol("ls-refs answer ends without a flush"));
    }
    Ok(out)
}

/// The `fetch` request: `wants`, `haves` and `done` in one round, so the
/// server answers with the pack at once. `ofs-delta` keeps the pack small;
/// `thin-pack` is not sent, so the pack's deltas only use bases inside it.
pub fn fetch_request(caps: V2Caps, wants: &[ObjectId], haves: &[ObjectId]) -> Vec<u8> {
    let mut out = Vec::new();
    command_header(&mut out, "fetch", caps);
    pkt::write_text(&mut out, "ofs-delta").expect("short line");
    pkt::write_text(&mut out, "no-progress").expect("short line");
    for w in wants {
        pkt::write_text(&mut out, &format!("want {w}")).expect("short line");
    }
    for h in haves {
        pkt::write_text(&mut out, &format!("have {h}")).expect("short line");
    }
    pkt::write_text(&mut out, "done").expect("short line");
    pkt::write_flush(&mut out);
    out
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FetchState {
    /// Before a section header.
    Header,
    /// Inside a section that is not `packfile`, skipping its lines.
    OtherSection,
    /// Inside `packfile`.
    Pack,
    /// After the final flush.
    Done,
}

/// Reads a v2 `fetch` answer as it arrives and writes the pack (side-band
/// 1 of the `packfile` section) to a sink. Progress (band 2) is dropped;
/// band 3 and `ERR` end the fetch with [`WireError::Remote`].
#[derive(Debug)]
pub struct FetchReader {
    buf: Vec<u8>,
    state: FetchState,
    pack_bytes: u64,
    max_pack_bytes: u64,
}

impl FetchReader {
    /// A reader that refuses a pack larger than `max_pack_bytes`.
    pub fn new(max_pack_bytes: u64) -> Self {
        Self {
            buf: Vec::new(),
            state: FetchState::Header,
            pack_bytes: 0,
            max_pack_bytes,
        }
    }

    /// Pack bytes written so far.
    pub fn pack_bytes(&self) -> u64 {
        self.pack_bytes
    }

    /// Feeds the next bytes of the answer.
    pub fn feed(&mut self, bytes: &[u8], sink: &mut impl Write) -> Result<(), WireError> {
        self.buf.extend_from_slice(bytes);
        let mut at = 0;
        while let Some((p, used)) =
            pkt::decode(&self.buf[at..]).map_err(|e| protocol(e.to_string()))?
        {
            at += used;
            match (self.state, p) {
                (FetchState::Done, _) => return Err(protocol("bytes after the end of the pack")),
                (FetchState::Header, Packet::Data(_)) => {
                    let line = text(&p).unwrap_or_default();
                    if let Some(e) = remote_err(&line) {
                        return Err(e);
                    }
                    self.state = if line == "packfile" {
                        FetchState::Pack
                    } else {
                        FetchState::OtherSection
                    };
                }
                (FetchState::OtherSection, Packet::Data(_)) => {}
                (FetchState::OtherSection, Packet::Delim) => self.state = FetchState::Header,
                (FetchState::Pack, Packet::Data(d)) => {
                    let (band, data) = sideband::demux(d).map_err(|e| protocol(e.to_string()))?;
                    match band {
                        Band::Data => {
                            self.pack_bytes += data.len() as u64;
                            if self.pack_bytes > self.max_pack_bytes {
                                return Err(protocol(format!(
                                    "the pack is larger than the limit of {} bytes",
                                    self.max_pack_bytes
                                )));
                            }
                            sink.write_all(data)
                                .map_err(|e| protocol(format!("writing the pack: {e}")))?;
                        }
                        Band::Progress => {}
                        Band::Error => {
                            return Err(WireError::Remote(
                                String::from_utf8_lossy(data).trim().to_owned(),
                            ));
                        }
                    }
                }
                (FetchState::Pack, Packet::Flush) => self.state = FetchState::Done,
                (FetchState::Header | FetchState::OtherSection, Packet::Flush) => {
                    return Err(protocol("the fetch answer ended without a pack"));
                }
                (_, other) => {
                    return Err(protocol(format!("unexpected packet {other:?} in fetch")));
                }
            }
        }
        self.buf.drain(..at);
        Ok(())
    }

    /// Checks that the answer ended with the pack's final flush.
    pub fn finish(&self) -> Result<(), WireError> {
        if self.state == FetchState::Done && self.buf.is_empty() {
            Ok(())
        } else {
            Err(protocol("the fetch answer ended early"))
        }
    }
}

/// What a receive-pack advertisement offers that a push needs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PushCaps {
    /// `report-status`: required, or the outcome cannot be known.
    pub report_status: bool,
    /// `side-band-64k`.
    pub side_band_64k: bool,
    /// `atomic`.
    pub atomic: bool,
    /// `ofs-delta`: required, since Aspen's packs use OFS_DELTA.
    pub ofs_delta: bool,
    /// `delete-refs`.
    pub delete_refs: bool,
    /// `object-format=sha1` was advertised.
    pub object_format: bool,
}

/// The upstream's refs and capabilities, from a receive-pack advertisement.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PushAdvertisement {
    /// Branches and tags whose names Aspen can store.
    pub refs: BTreeMap<RefName, ObjectId>,
    /// What it supports.
    pub caps: PushCaps,
}

/// Parses the answer to `GET info/refs?service=git-receive-pack`.
pub fn parse_push_advertisement(body: &[u8]) -> Result<PushAdvertisement, WireError> {
    let all = packets(body)?;
    let rest = skip_service_header(&all, "git-receive-pack");
    let mut out = PushAdvertisement::default();
    let mut first = true;
    let mut ended = false;
    for p in rest {
        let line = match p {
            Packet::Flush => {
                ended = true;
                break;
            }
            Packet::Data(d) => {
                let d = d.strip_suffix(b"\n").unwrap_or(d);
                d.to_vec()
            }
            _ => {
                return Err(protocol(
                    "unexpected special packet in the push advertisement",
                ));
            }
        };
        let (refpart, capspart) = match line.iter().position(|b| *b == 0) {
            Some(nul) => (&line[..nul], Some(&line[nul + 1..])),
            None => (&line[..], None),
        };
        let refpart = String::from_utf8_lossy(refpart);
        if first && refpart == "version 1" {
            continue;
        }
        if let Some(e) = remote_err(&refpart) {
            return Err(e);
        }
        if first {
            let caps = capspart
                .ok_or_else(|| protocol("the first advertised ref carries no capabilities"))?;
            for cap in String::from_utf8_lossy(caps).split(' ') {
                match cap {
                    "report-status" => out.caps.report_status = true,
                    "side-band-64k" => out.caps.side_band_64k = true,
                    "atomic" => out.caps.atomic = true,
                    "ofs-delta" => out.caps.ofs_delta = true,
                    "delete-refs" => out.caps.delete_refs = true,
                    "object-format=sha1" => out.caps.object_format = true,
                    c if c.starts_with("object-format=") => {
                        return Err(WireError::Unsupported("SHA-1 repositories only"));
                    }
                    _ => {}
                }
            }
            first = false;
        }
        let Some((oid, name)) = refpart.split_once(' ') else {
            return Err(protocol(format!("bad advertised ref {refpart:?}")));
        };
        if name == "capabilities^{}" || name.ends_with("^{}") {
            continue;
        }
        if !MIRRORED_PREFIXES.iter().any(|p| name.starts_with(p)) {
            continue;
        }
        let oid = ObjectId::parse_hex(oid)
            .map_err(|e| protocol(format!("bad object ID in the push advertisement: {e}")))?;
        if let Ok(name) = RefName::new(name) {
            out.refs.insert(name, oid);
        }
    }
    if !ended {
        return Err(protocol("the push advertisement ends without a flush"));
    }
    if first {
        return Err(protocol("empty push advertisement"));
    }
    Ok(out)
}

/// The command part of a push request (the pack follows it): one line per
/// update, the first carrying the capabilities. Each command's old value is
/// the upstream's current value as Aspen expects it, so the upstream's own
/// compare-and-swap refuses the update if the ref moved.
pub fn push_request(
    caps: PushCaps,
    updates: &[RefUpdate],
    atomic: bool,
) -> Result<Vec<u8>, WireError> {
    if !caps.report_status {
        return Err(WireError::Unsupported("report-status"));
    }
    if !caps.ofs_delta {
        return Err(WireError::Unsupported("ofs-delta"));
    }
    if atomic && !caps.atomic {
        return Err(WireError::Unsupported("atomic pushes"));
    }
    if updates.iter().any(|u| u.new.is_none()) && !caps.delete_refs {
        return Err(WireError::Unsupported("deleting refs"));
    }
    let mut wanted = vec!["report-status", "ofs-delta"];
    if caps.side_band_64k {
        wanted.push("side-band-64k");
    }
    if atomic {
        wanted.push("atomic");
    }
    if caps.object_format {
        wanted.push("object-format=sha1");
    }
    let agent = format!("agent={AGENT}");
    wanted.push(&agent);
    let mut out = Vec::new();
    for (i, u) in updates.iter().enumerate() {
        let mut line = format!(
            "{} {} {}",
            u.old.unwrap_or(ObjectId::ZERO),
            u.new.unwrap_or(ObjectId::ZERO),
            u.name
        )
        .into_bytes();
        if i == 0 {
            line.push(0);
            line.extend_from_slice(wanted.join(" ").as_bytes());
        }
        line.push(b'\n');
        pkt::write_data(&mut out, &line).map_err(|e| protocol(e.to_string()))?;
    }
    pkt::write_flush(&mut out);
    Ok(out)
}

/// Whether a push of `updates` sends a pack: Git sends none when every
/// command is a delete.
pub fn push_sends_pack(updates: &[RefUpdate]) -> bool {
    updates.iter().any(|u| u.new.is_some())
}

/// The upstream's report on a push.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PushReport {
    /// `unpack ok`, or the unpack error.
    pub unpack: Result<(), String>,
    /// Per ref: `ok`, or the `ng` reason.
    pub refs: BTreeMap<String, Result<(), String>>,
}

impl PushReport {
    /// The outcome for one ref: its own status, or the unpack error if the
    /// report left it out.
    pub fn status(&self, name: &RefName) -> Result<(), String> {
        match (self.refs.get(name.as_str()), &self.unpack) {
            (Some(s), _) => s.clone(),
            (None, Err(e)) => Err(format!("unpack failed: {e}")),
            (None, Ok(())) => Err("the upstream did not report on this ref".to_owned()),
        }
    }
}

/// Parses a push answer: report-status lines, inside side-band 1 when
/// `sideband` was requested.
pub fn parse_push_report(body: &[u8], sideband: bool) -> Result<PushReport, WireError> {
    let inner = if sideband {
        let mut data = Vec::new();
        for p in packets(body)? {
            match p {
                Packet::Data(d) => {
                    let (band, d) = sideband::demux(d).map_err(|e| protocol(e.to_string()))?;
                    match band {
                        Band::Data => data.extend_from_slice(d),
                        Band::Progress => {}
                        Band::Error => {
                            return Err(WireError::Remote(
                                String::from_utf8_lossy(d).trim().to_owned(),
                            ));
                        }
                    }
                }
                Packet::Flush => break,
                _ => return Err(protocol("unexpected special packet in the push answer")),
            }
        }
        data
    } else {
        body.to_vec()
    };
    let mut report = PushReport {
        unpack: Err("no unpack status".to_owned()),
        refs: BTreeMap::new(),
    };
    let mut saw_unpack = false;
    let mut ended = false;
    for p in packets(&inner)? {
        let line = match p {
            Packet::Flush => {
                ended = true;
                break;
            }
            Packet::Data(_) => text(&p).unwrap_or_default(),
            _ => return Err(protocol("unexpected special packet in report-status")),
        };
        if let Some(e) = remote_err(&line) {
            return Err(e);
        }
        if let Some(status) = line.strip_prefix("unpack ") {
            saw_unpack = true;
            report.unpack = if status == "ok" {
                Ok(())
            } else {
                Err(status.to_owned())
            };
        } else if let Some(name) = line.strip_prefix("ok ") {
            report.refs.insert(name.to_owned(), Ok(()));
        } else if let Some(rest) = line.strip_prefix("ng ") {
            let (name, reason) = rest.split_once(' ').unwrap_or((rest, "rejected"));
            report.refs.insert(name.to_owned(), Err(reason.to_owned()));
        } else {
            return Err(protocol(format!("bad report-status line {line:?}")));
        }
    }
    if !saw_unpack || !ended {
        return Err(protocol("incomplete report-status"));
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn oid(n: u8) -> ObjectId {
        ObjectId::from_bytes([n; 20])
    }

    fn name(s: &str) -> RefName {
        RefName::new(s).unwrap()
    }

    fn lines(ls: &[&str]) -> Vec<u8> {
        let mut out = Vec::new();
        for l in ls {
            match *l {
                "0000" => pkt::write_flush(&mut out),
                "0001" => pkt::write_delim(&mut out),
                l => pkt::write_data(&mut out, format!("{l}\n").as_bytes()).unwrap(),
            }
        }
        out
    }

    #[test]
    fn a_v2_advertisement_needs_ls_refs_and_fetch() {
        let ok = lines(&[
            "# service=git-upload-pack",
            "0000",
            "version 2",
            "agent=git/2.39.5",
            "ls-refs=unborn",
            "fetch=shallow wait-for-done",
            "object-format=sha1",
            "0000",
        ]);
        assert_eq!(
            parse_v2_advertisement(&ok),
            Ok(V2Caps {
                object_format: true
            })
        );
        // Without the service header (allowed for v2), no object-format.
        let bare = lines(&["version 2", "ls-refs", "fetch", "0000"]);
        assert_eq!(parse_v2_advertisement(&bare), Ok(V2Caps::default()));
        let v0 = lines(&["# service=git-upload-pack", "0000", "0000"]);
        assert!(matches!(
            parse_v2_advertisement(&v0),
            Err(WireError::Protocol(_) | WireError::Unsupported(_))
        ));
        let no_fetch = lines(&["version 2", "ls-refs", "0000"]);
        assert_eq!(
            parse_v2_advertisement(&no_fetch),
            Err(WireError::Unsupported("the v2 fetch command"))
        );
        let sha256 = lines(&[
            "version 2",
            "ls-refs",
            "fetch",
            "object-format=sha256",
            "0000",
        ]);
        assert_eq!(
            parse_v2_advertisement(&sha256),
            Err(WireError::Unsupported("SHA-1 repositories only"))
        );
    }

    #[test]
    fn ls_refs_keeps_branches_and_tags_and_heads_symref() {
        let a = oid(1).to_hex();
        let b = oid(2).to_hex();
        let body = lines(&[
            &format!("{a} HEAD symref-target:refs/heads/main"),
            &format!("{a} refs/heads/main"),
            &format!("{b} refs/tags/v1 peeled:{a}"),
            &format!("{b} refs/pull/1/head"),
            &format!("{b} refs/heads/bad..name"),
            "0000",
        ]);
        let got = parse_ls_refs(&body).unwrap();
        assert_eq!(got.head, Some(name("refs/heads/main")));
        assert_eq!(
            got.refs.into_iter().collect::<Vec<_>>(),
            vec![
                (name("refs/heads/main"), oid(1)),
                (name("refs/tags/v1"), oid(2))
            ]
        );
        assert_eq!(got.unstorable, vec!["refs/heads/bad..name".to_owned()]);
        assert!(parse_ls_refs(&lines(&["ERR access denied", "0000"])).is_err());
        // A truncated answer is not an empty repository.
        assert!(parse_ls_refs(&lines(&[&format!("{a} refs/heads/main")])).is_err());
    }

    #[test]
    fn fetch_requests_end_in_done_without_thin_packs() {
        let req = fetch_request(V2Caps::default(), &[oid(1)], &[oid(2)]);
        let text: Vec<String> = packets(&req)
            .unwrap()
            .iter()
            .map(|p| text(p).unwrap_or_else(|| format!("{p:?}")))
            .collect();
        assert_eq!(
            text,
            vec![
                "command=fetch".to_owned(),
                format!("agent={AGENT}"),
                "Delim".to_owned(),
                "ofs-delta".to_owned(),
                "no-progress".to_owned(),
                format!("want {}", oid(1)),
                format!("have {}", oid(2)),
                "done".to_owned(),
                "Flush".to_owned(),
            ]
        );
    }

    fn band(n: u8, data: &[u8]) -> Vec<u8> {
        let mut payload = vec![n];
        payload.extend_from_slice(data);
        let mut out = Vec::new();
        pkt::write_data(&mut out, &payload).unwrap();
        out
    }

    #[test]
    fn the_fetch_reader_writes_band_one_of_the_packfile_section() {
        let mut body = lines(&["acknowledgments", "ready", "0001", "packfile"]);
        body.extend(band(2, b"counting\n"));
        body.extend(band(1, b"PACK"));
        body.extend(band(1, b"rest"));
        pkt::write_flush(&mut body);
        // Fed one byte at a time, as the network may.
        let mut r = FetchReader::new(1 << 20);
        let mut pack = Vec::new();
        for b in &body {
            r.feed(std::slice::from_ref(b), &mut pack).unwrap();
        }
        r.finish().unwrap();
        assert_eq!(pack, b"PACKrest");

        let mut cut = lines(&["packfile"]);
        cut.extend(band(1, b"PACK"));
        let mut r = FetchReader::new(1 << 20);
        r.feed(&cut, &mut Vec::new()).unwrap();
        assert!(r.finish().is_err(), "a pack without the final flush");

        let mut err = lines(&["packfile"]);
        err.extend(band(3, b"upload-pack: not our ref\n"));
        let mut r = FetchReader::new(1 << 20);
        assert_eq!(
            r.feed(&err, &mut Vec::new()),
            Err(WireError::Remote("upload-pack: not our ref".into()))
        );

        let mut big = lines(&["packfile"]);
        big.extend(band(1, &[0; 10]));
        let mut r = FetchReader::new(9);
        assert!(r.feed(&big, &mut Vec::new()).is_err(), "over the limit");

        let mut r = FetchReader::new(1 << 20);
        assert_eq!(
            r.feed(&lines(&["ERR want not valid"]), &mut Vec::new()),
            Err(WireError::Remote("want not valid".into()))
        );
    }

    fn adv(first: &str, caps: &str, more: &[&str]) -> Vec<u8> {
        let mut out = lines(&["# service=git-receive-pack", "0000"]);
        let mut line = first.as_bytes().to_vec();
        line.push(0);
        line.extend_from_slice(caps.as_bytes());
        line.push(b'\n');
        pkt::write_data(&mut out, &line).unwrap();
        out.extend(lines(more));
        out
    }

    #[test]
    fn push_advertisements_give_refs_and_capabilities() {
        let a = oid(1).to_hex();
        let b = oid(2).to_hex();
        let body = adv(
            &format!("{a} refs/heads/main"),
            "report-status delete-refs side-band-64k quiet atomic ofs-delta agent=git/2.39.5",
            &[
                &format!("{b} refs/tags/v1"),
                &format!("{a} refs/tags/v1^{{}}"),
                &format!("{b} refs/pull/7/head"),
                "0000",
            ],
        );
        let got = parse_push_advertisement(&body).unwrap();
        assert_eq!(
            got.refs.into_iter().collect::<Vec<_>>(),
            vec![
                (name("refs/heads/main"), oid(1)),
                (name("refs/tags/v1"), oid(2))
            ]
        );
        assert_eq!(
            got.caps,
            PushCaps {
                report_status: true,
                side_band_64k: true,
                atomic: true,
                ofs_delta: true,
                delete_refs: true,
                object_format: false,
            }
        );
        let empty = adv(
            &format!("{} capabilities^{{}}", ObjectId::ZERO),
            "report-status ofs-delta",
            &["0000"],
        );
        let got = parse_push_advertisement(&empty).unwrap();
        assert!(got.refs.is_empty());
        assert!(got.caps.report_status && !got.caps.atomic);
    }

    #[test]
    fn push_requests_carry_capabilities_on_the_first_command_only() {
        let caps = PushCaps {
            report_status: true,
            side_band_64k: true,
            atomic: true,
            ofs_delta: true,
            delete_refs: true,
            object_format: false,
        };
        let updates = [
            RefUpdate {
                name: name("refs/heads/main"),
                old: Some(oid(1)),
                new: Some(oid(2)),
            },
            RefUpdate {
                name: name("refs/heads/gone"),
                old: Some(oid(3)),
                new: None,
            },
        ];
        let req = push_request(caps, &updates, true).unwrap();
        let ps = packets(&req).unwrap();
        assert_eq!(ps.len(), 3);
        let Packet::Data(first) = ps[0] else { panic!() };
        let nul = first.iter().position(|b| *b == 0).unwrap();
        assert_eq!(
            String::from_utf8_lossy(&first[..nul]),
            format!("{} {} refs/heads/main", oid(1), oid(2))
        );
        assert_eq!(
            String::from_utf8_lossy(&first[nul + 1..]),
            format!("report-status ofs-delta side-band-64k atomic agent={AGENT}\n")
        );
        assert_eq!(
            text(&ps[1]).unwrap(),
            format!("{} {} refs/heads/gone", oid(3), ObjectId::ZERO)
        );
        assert_eq!(ps[2], Packet::Flush);
        assert!(push_sends_pack(&updates));
        assert!(!push_sends_pack(&updates[1..]));

        let no_atomic = PushCaps {
            atomic: false,
            ..caps
        };
        assert_eq!(
            push_request(no_atomic, &updates, true),
            Err(WireError::Unsupported("atomic pushes"))
        );
        let no_report = PushCaps {
            report_status: false,
            ..caps
        };
        assert!(push_request(no_report, &updates, false).is_err());
    }

    #[test]
    fn push_reports_parse_with_and_without_side_band() {
        let report = lines(&[
            "unpack ok",
            "ok refs/heads/main",
            "ng refs/heads/x protected branch hook declined",
            "0000",
        ]);
        let plain = parse_push_report(&report, false).unwrap();
        assert_eq!(plain.unpack, Ok(()));
        assert_eq!(plain.status(&name("refs/heads/main")), Ok(()));
        assert_eq!(
            plain.status(&name("refs/heads/x")),
            Err("protected branch hook declined".to_owned())
        );
        let mut banded = band(2, b"remote: hi\n");
        banded.extend(band(1, &report));
        pkt::write_flush(&mut banded);
        assert_eq!(parse_push_report(&banded, true).unwrap(), plain);

        let failed = lines(&[
            "unpack index-pack abnormal exit",
            "ng refs/heads/main unpacker error",
            "0000",
        ]);
        let r = parse_push_report(&failed, false).unwrap();
        assert_eq!(r.unpack, Err("index-pack abnormal exit".to_owned()));
        assert_eq!(
            r.status(&name("refs/heads/other")),
            Err("unpack failed: index-pack abnormal exit".to_owned())
        );
        let mut fatal = band(3, b"pre-receive hook declined\n");
        pkt::write_flush(&mut fatal);
        assert_eq!(
            parse_push_report(&fatal, true),
            Err(WireError::Remote("pre-receive hook declined".into()))
        );
        assert!(parse_push_report(&lines(&["ok refs/heads/main", "0000"]), false).is_err());
    }
}
