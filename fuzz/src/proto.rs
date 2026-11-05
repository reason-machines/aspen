//! Targets for the wire protocol (`aspen-proto`): pkt-line framing, the
//! push request, v2 commands and v0 upload-pack.
//!
//! Each target reads [`split_header`]'s 9-byte header (a configuration byte
//! and a seed for the decisions a server would make), then treats the rest
//! as exactly the bytes a client sent. The real transcripts in
//! `crates/aspen-proto/tests/transcripts` and hand-written requests seed
//! the corpus (`aspen-fuzz seeds`).

use std::io::{self, Read};

use aspen_core::error::{Surface, WireError};
use aspen_core::fetch::{FetchRequest, PackfileUri, ShallowInfo};
use aspen_core::id::{ObjectId, PackChecksum};
use aspen_core::refs::PushOptions;
use aspen_core::repo::ShallowPolicy;
use aspen_proto::caps::{self, Agent};
use aspen_proto::pkt::{self, Packet, PktError, PktReader, ReadError};
use aspen_proto::receive::{
    ReceiveAdvertisement, ReceiveCaps, ReceiveLimits, ReceiveRequest, RefStatus, ReportStatus,
    RequestParser, write_fatal, write_progress,
};
use aspen_proto::sideband::{self, Band};
use aspen_proto::upload_v0::{
    self, Answered, Header, Negotiating, Transport, UploadRequest, V0Limits,
};
use aspen_proto::v2::{
    Acknowledgments, Command, CommandParser, FetchArgs, FetchResponse, LsRefsEntry, LsRefsName,
    LsRefsRequest, V2Advertisement, V2Limits, write_ls_refs,
};

use crate::input::{Mix, split_header};

/// A packet that owns its payload, for comparing two readers.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Owned {
    Flush,
    Delim,
    ResponseEnd,
    Data(Vec<u8>),
}

impl From<Packet<'_>> for Owned {
    fn from(p: Packet<'_>) -> Self {
        match p {
            Packet::Flush => Self::Flush,
            Packet::Delim => Self::Delim,
            Packet::ResponseEnd => Self::ResponseEnd,
            Packet::Data(d) => Self::Data(d.to_vec()),
        }
    }
}

/// A reader that hands out `data` in chunks of random sizes (1 to 9000
/// bytes), so `PktReader`'s buffering meets every packet boundary.
#[derive(Debug)]
struct Chunks<'a> {
    data: &'a [u8],
    mix: Mix,
}

impl Read for Chunks<'_> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let want = match self.mix.below(4) {
            0 => 1,
            1 => 1 + self.mix.below(16),
            _ => 1 + self.mix.below(9000),
        };
        let n = want.min(buf.len()).min(self.data.len());
        buf[..n].copy_from_slice(&self.data[..n]);
        self.data = &self.data[n..];
        Ok(n)
    }
}

/// How a byte stream ends for [`pkt::decode`].
#[derive(Debug, PartialEq, Eq)]
enum End {
    Clean,
    Truncated,
    Error(PktError),
}

/// `pkt-line`: [`pkt::decode`] over the whole input against
/// [`PktReader`] over the same bytes in random chunks. Both must see the
/// same packets and end the same way; the reader's remainder after any
/// packet is exactly the bytes after it (an SSH push's pack follows its
/// request on one stream). Every data packet re-encodes to itself, and
/// every parser of a client-chosen string on the way in (side-band demux,
/// capability lists, `agent=`, `GIT_PROTOCOL`, service names) and the
/// server's text sanitizer run on its payload.
pub fn pkt_line(data: &[u8]) {
    let (_config, mut mix, wire) = split_header(data);
    let mut expected = Vec::new();
    let mut ends = vec![0usize];
    let mut pos = 0;
    let end = loop {
        match pkt::decode(&wire[pos..]) {
            Ok(Some((p, used))) => {
                assert_eq!(used, p.wire_len(), "decode consumed a different length");
                assert!(used >= 4 && used <= wire.len() - pos);
                check_payload(&p);
                expected.push(Owned::from(p));
                pos += used;
                ends.push(pos);
            }
            Ok(None) if pos == wire.len() => break End::Clean,
            Ok(None) => break End::Truncated,
            Err(e) => break End::Error(e),
        }
    };

    let stop_at = mix.below(expected.len() + 1);
    let mut reader = PktReader::new(Chunks {
        data: wire,
        mix: mix.clone(),
    });
    for (i, want) in expected.iter().enumerate() {
        if i == stop_at {
            let mut rest = Vec::new();
            reader
                .into_remainder()
                .read_to_end(&mut rest)
                .expect("in-memory read");
            assert_eq!(rest, &wire[ends[i]..], "remainder after {i} packets");
            return;
        }
        let got = reader.read_packet().map(|p| p.map(Owned::from));
        match got {
            Ok(Some(p)) => assert_eq!(&p, want, "packet {i}"),
            other => panic!("packet {i}: decode gave {want:?}, the reader {other:?}"),
        }
    }
    let last = reader.read_packet().map(|p| p.map(Owned::from));
    match (&end, last) {
        (End::Clean, Ok(None)) | (End::Truncated, Err(ReadError::TruncatedPacket)) => {}
        (End::Error(e), Err(ReadError::Pkt(r))) if *e == r => {}
        (end, last) => panic!("decode ended with {end:?}, the reader with {last:?}"),
    }
}

fn check_payload(p: &Packet<'_>) {
    let Packet::Data(d) = *p else { return };
    let mut again = Vec::new();
    pkt::write_data(&mut again, d).expect("a decoded payload fits one packet");
    assert_eq!(
        pkt::decode(&again).expect("re-encoded packet decodes"),
        Some((Packet::Data(d), again.len()))
    );
    if let Ok((_, rest)) = sideband::demux(d) {
        assert_eq!(rest, &d[1..]);
    }
    let text = String::from_utf8_lossy(p.text().expect("data packet"));
    let _ = caps::parse_list(&text);
    let _ = Agent::new(text.as_ref());
    let _ = aspen_core::fetch::git_agent_version(&text);
    let _ = aspen_core::fetch::allows_direct_bucket_uris(Some(&text));
    let _ = aspen_proto::service::ProtocolVersion::requested(Some(&text));
    let _ = aspen_proto::service::Service::parse(&text);
    check_one_line(&pkt::one_line(&text));
}

/// What [`pkt::one_line`] promises: no control characters, at most 4,096
/// bytes, so a server message can never forge a second protocol line.
fn check_one_line(s: &str) {
    assert!(s.len() <= 4096, "one_line gave {} bytes", s.len());
    assert!(
        !s.chars().any(char::is_control),
        "one_line left a control character in {s:?}"
    );
}

/// Every complete packet in `bytes` (which must end on a packet boundary).
fn packets(bytes: &[u8]) -> Vec<Owned> {
    let mut out = Vec::new();
    let mut pos = 0;
    while pos < bytes.len() {
        let (p, used) = pkt::decode(&bytes[pos..])
            .unwrap_or_else(|e| panic!("server output does not frame: {e}"))
            .expect("server output ends on a packet boundary");
        out.push(Owned::from(p));
        pos += used;
    }
    out
}

/// A text line of server output: one line, LF-terminated, no other
/// control characters.
fn check_text_line(line: &[u8]) {
    let body = line
        .strip_suffix(b"\n")
        .unwrap_or_else(|| panic!("text line without LF: {:?}", String::from_utf8_lossy(line)));
    let s = std::str::from_utf8(body).expect("server text is UTF-8");
    assert!(
        !s.chars().any(char::is_control),
        "control character in {s:?}"
    );
}

/// A short server-side string from fuzzer bytes: reasons and notes a hook
/// could produce, including newlines and control characters.
fn text_from(mix: &mut Mix) -> String {
    const PIECES: [&str; 8] = ["ok refs/heads/x", "\n", "\r", "\0", "ng ", "é", " ", "0000"];
    (0..mix.below(6)).map(|_| PIECES[mix.below(8)]).collect()
}

/// `receive-request`: the v0/v1 push request parser
/// ([`RequestParser`]) on client bytes, under default or tight limits.
/// When it returns a request, the push options are parsed as B1 parses
/// them, and a report with a random status per command (reasons and notes
/// with newlines and control characters) must write, frame, and say exactly
/// one `ok`/`ng` line per command, invalid names at their positions, so no
/// client-chosen name or server reason can forge a report line.
pub fn receive_request(data: &[u8]) {
    let (config, mut mix, wire) = split_header(data);
    let mut advert = ReceiveAdvertisement::new(Agent::aspen());
    advert.no_thin = config & 1 == 1;
    let limits = if config & 2 == 2 {
        ReceiveLimits {
            max_request_bytes: 2048,
            max_commands: 4,
            max_shallow: 4,
            max_push_options: 3,
            max_push_option_bytes: 64,
        }
    } else {
        ReceiveLimits::default()
    };
    let mut parser = RequestParser::new(advert, limits);
    let mut reader = PktReader::new(wire);
    let req = loop {
        match reader.read_packet() {
            Ok(Some(p)) => match parser.packet(p) {
                Ok(Some(req)) => break req,
                Ok(None) => {}
                Err(_) => return,
            },
            Ok(None) | Err(_) => return,
        }
    };
    let _ = PushOptions::parse(req.push_options.iter().map(String::as_str));
    let _ = req.expects_pack();
    check_report(&req, &mut mix);
    let mut out = Vec::new();
    write_progress(&mut out, &req.caps, &text_from(&mut mix));
    write_fatal(&mut out, Some(&req.caps), &text_from(&mut mix));
    for p in packets(&out) {
        if let Owned::Data(d) = p {
            match req.caps.sideband {
                Some(_) => check_text_line(sideband::demux(&d).expect("band").1),
                None => assert!(d.starts_with(b"ERR ")),
            }
        }
    }
    let mut out = Vec::new();
    write_fatal(&mut out, None, &text_from(&mut mix));
    match packets(&out).as_slice() {
        [Owned::Data(d)] => assert!(d.starts_with(b"ERR ") && !d[..d.len() - 1].contains(&b'\n')),
        other => panic!("a fatal error before the request is one ERR line, not {other:?}"),
    }
}

fn check_report(req: &ReceiveRequest, mix: &mut Mix) {
    let mut report = ReportStatus::new(req);
    let mut ok = Vec::new();
    for c in &req.commands {
        let status = if mix.flag() {
            RefStatus::Ok
        } else {
            RefStatus::Rejected(text_from(mix))
        };
        ok.push((c.name.as_str().to_owned(), status == RefStatus::Ok));
        report.set(&c.name, status).expect("a command of this push");
    }
    for _ in 0..mix.below(3) {
        report.note(&text_from(mix));
    }
    let unpack_failed = mix.below(8) == 0;
    if unpack_failed {
        let e = WireError {
            surface: Surface::PushUnpack,
            message: text_from(mix),
            retryable: false,
            alert: false,
        };
        report.unpack_failed(&e).expect("unpack surface");
    }
    let mut out = Vec::new();
    report
        .write(&mut out, &req.caps)
        .expect("every command has a status");
    let lines = report_lines(&out, &req.caps);
    let Some(lines) = lines else { return };
    let count = req.command_count();
    assert_eq!(
        lines.len(),
        count + 1,
        "one unpack line plus one per command"
    );
    assert!(lines[0].starts_with(if unpack_failed {
        "unpack "
    } else {
        "unpack ok"
    }));
    let names: Vec<String> = lines[1..]
        .iter()
        .map(|l| {
            let rest = l
                .strip_prefix("ok ")
                .or_else(|| l.strip_prefix("ng "))
                .unwrap_or_else(|| panic!("report line {l:?}"));
            rest.to_owned()
        })
        .collect();
    for c in &req.invalid {
        assert!(c.position < count);
        let line = &lines[1 + c.position];
        assert!(
            line.starts_with(&format!("ng {} ", c.name)),
            "invalid command at {} reported as {line:?}",
            c.position
        );
    }
    let mut valid = lines[1..]
        .iter()
        .enumerate()
        .filter(|(i, _)| !req.invalid.iter().any(|c| c.position == *i));
    for (name, is_ok) in &ok {
        let (_, line) = valid.next().expect("a line per valid command");
        let expect_ok = *is_ok && !unpack_failed;
        if expect_ok {
            assert_eq!(line, &format!("ok {name}"));
        } else {
            assert!(line.starts_with(&format!("ng {name} ")), "{line:?}");
        }
    }
    assert!(valid.next().is_none());
    drop(names);
}

/// The report lines of a written report, or `None` when the client did not
/// ask for `report-status`. Checks every band-2 notice on the way.
fn report_lines(out: &[u8], caps: &ReceiveCaps) -> Option<Vec<String>> {
    let report = match caps.sideband {
        Some(_) => {
            let mut report = Vec::new();
            let all = packets(out);
            assert_eq!(
                all.last(),
                Some(&Owned::Flush),
                "side-band report ends in a flush"
            );
            for p in &all[..all.len() - 1] {
                let Owned::Data(d) = p else {
                    panic!("unexpected {p:?} in a side-band report")
                };
                match sideband::demux(d).expect("a known band") {
                    (Band::Data, chunk) => report.extend_from_slice(chunk),
                    (Band::Progress, line) => check_text_line(line),
                    (Band::Error, _) => panic!("band 3 in a report"),
                }
            }
            report
        }
        None => out.to_vec(),
    };
    if !caps.report_status {
        assert!(report.is_empty(), "a report nobody asked for");
        return None;
    }
    let all = packets(&report);
    assert_eq!(all.last(), Some(&Owned::Flush));
    Some(
        all[..all.len() - 1]
            .iter()
            .map(|p| match p {
                Owned::Data(d) => {
                    check_text_line(d);
                    String::from_utf8(d[..d.len() - 1].to_vec()).expect("UTF-8")
                }
                other => panic!("unexpected {other:?} in report-status"),
            })
            .collect(),
    )
}

/// `v2-command`: the protocol v2 command parser ([`CommandParser`]) on
/// client bytes, with `packfile-uris` advertised or not and default or
/// tight limits. A parsed `fetch` is answered through [`FetchResponse`] in
/// the order the server uses (acknowledgments unless `done`, then
/// `shallow-info` for a shallow request, `packfile-uris` in a scheme the
/// client listed, the pack, progress), which must never be refused; a
/// parsed `ls-refs` lists a few refs. The output must frame.
pub fn v2_command(data: &[u8]) {
    let (config, mut mix, wire) = split_header(data);
    let mut advert = V2Advertisement::new(Agent::aspen());
    advert.packfile_uris = config & 1 == 1;
    advert.no_ref_delta = config & 4 == 4;
    let limits = if config & 2 == 2 {
        V2Limits {
            max_request_bytes: 4096,
            max_wants: 4,
            max_haves: 4,
            max_shallow: 4,
            max_ref_prefixes: 4,
            max_server_options: 2,
        }
    } else {
        V2Limits::default()
    };
    let mut parser = CommandParser::new(advert, limits);
    let mut reader = PktReader::new(wire);
    loop {
        let command = match reader.read_packet() {
            Ok(Some(p)) => match parser.packet(p) {
                Ok(Some(c)) => c,
                Ok(None) => continue,
                Err(_) => return,
            },
            Ok(None) | Err(_) => return,
        };
        let mut out = Vec::new();
        match command {
            Command::End => return,
            Command::LsRefs(req) => answer_ls_refs(&mut out, &req),
            Command::Fetch(args) => answer_fetch(&mut out, &args, &mut mix),
        }
        let _ = packets(&out);
    }
}

fn answer_ls_refs(out: &mut Vec<u8>, req: &LsRefsRequest) {
    let main = aspen_core::name::RefName::new("refs/heads/main").expect("valid");
    let tag = aspen_core::name::RefName::new("refs/tags/v1").expect("valid");
    let entries = [
        LsRefsEntry {
            name: LsRefsName::Head,
            tip: None,
            symref_target: Some(main.clone()),
            peeled: None,
        },
        LsRefsEntry {
            name: LsRefsName::Ref(main),
            tip: Some(oid(1)),
            symref_target: None,
            peeled: None,
        },
        LsRefsEntry {
            name: LsRefsName::Ref(tag),
            tip: Some(oid(2)),
            symref_target: None,
            peeled: Some(oid(3)),
        },
    ];
    write_ls_refs(out, req, &entries).expect("valid entries");
}

fn oid(n: u8) -> ObjectId {
    ObjectId::from_bytes([n; 20])
}

/// A subset of `ids`.
fn some_of(ids: &[ObjectId], mix: &mut Mix) -> Vec<ObjectId> {
    ids.iter().copied().filter(|_| mix.flag()).collect()
}

fn answer_fetch(out: &mut Vec<u8>, args: &FetchArgs, mix: &mut Mix) {
    let request: &FetchRequest = &args.request;
    let _ = request.plan_mode(ShallowPolicy::Serve);
    let _ = request.plan_mode(ShallowPolicy::Refuse);
    let mut response = FetchResponse::new(args);
    if !request.done {
        let acks = Acknowledgments {
            common: some_of(&request.haves, mix),
            ready: mix.flag(),
        };
        response
            .acknowledgments(out, &acks)
            .expect("acknowledgments first");
        if !acks.ready {
            assert!(
                response.is_finished(),
                "a round without ready ends the response"
            );
            return;
        }
    }
    if request.is_shallow() {
        let info = ShallowInfo {
            shallow: some_of(&request.wants, mix),
            unshallow: some_of(&request.client_shallow, mix),
        };
        response.shallow_info(out, &info).expect("shallow-info");
    }
    // A URI in a scheme the client asked for, with a plain path, must be
    // sent. A client may list schemes no URI can use (spaces, control
    // characters, `://`), and a path from `text_from` may hold characters
    // no pkt-line text can; such a URI may be refused, and then nothing is
    // written.
    let scheme = request.packfile_uri_protocols.iter().find(|s| {
        s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'+' | b'-' | b'.'))
    });
    if let Some(scheme) = scheme
        && mix.flag()
    {
        let plain = mix.flag();
        let path = if plain {
            format!("packs/{}.pack", mix.next_u64())
        } else {
            text_from(mix)
        };
        let uri = PackfileUri {
            hash: PackChecksum::from_bytes([7; 20]),
            uri: format!("{scheme}://cdn.example/{path}"),
        };
        let before = out.len();
        if let Err(e) = response.packfile_uris(out, std::slice::from_ref(&uri)) {
            assert!(!plain, "packfile-uris refused {:?}: {e}", uri.uri);
            assert_eq!(out.len(), before, "a refused packfile-uris wrote bytes");
        }
    }
    response.start_packfile(out).expect("packfile section");
    let pack: Vec<u8> = (0..mix.below(70_000)).map(|i| i as u8).collect();
    response.pack_data(out, &pack).expect("pack data");
    response.progress(out, &text_from(mix)).expect("progress");
    if mix.flag() {
        let e = WireError {
            surface: Surface::FetchErr,
            message: text_from(mix),
            retryable: true,
            alert: false,
        };
        response
            .error(out, &e)
            .expect("error inside the packfile section");
    } else {
        response.finish(out).expect("finish");
    }
}

/// `upload-v0`: the v0/v1 upload-pack chain ([`aspen_proto::upload_v0`])
/// on client bytes, stateless (smart HTTP) or stateful (SSH), with default
/// or tight limits. Every step is answered as B2 and C3 answer it (a shallow section
/// when the request deepens, random shares and `ready` per round, then the
/// pack with progress) and must not be refused; end of input is handed to
/// the negotiation as the server does.
pub fn upload_v0(data: &[u8]) {
    let (config, mut mix, wire) = split_header(data);
    let transport = if config & 1 == 1 {
        Transport::Stateful
    } else {
        Transport::Stateless
    };
    let limits = if config & 2 == 2 {
        V0Limits {
            max_rounds: 3,
            ..V0Limits::from(V2Limits {
                max_request_bytes: 4096,
                max_wants: 4,
                max_haves: 8,
                max_shallow: 4,
                max_ref_prefixes: 4,
                max_server_options: 2,
            })
        }
    } else {
        V0Limits::default()
    };
    let mut parser = upload_v0::RequestParser::new(transport, limits);
    let mut reader = PktReader::new(wire);
    let request: UploadRequest = loop {
        match reader.read_packet() {
            Ok(Some(p)) => match parser.packet(p) {
                Ok(Some(Header::Fetch(r))) => break r,
                Ok(Some(Header::End)) | Err(_) => return,
                Ok(None) => {}
            },
            Ok(None) | Err(_) => return,
        }
    };
    let mut out = Vec::new();
    let _ = request.caps();
    let args = request.args().clone();
    if mix.below(16) == 0 {
        let e = WireError {
            surface: Surface::FetchErr,
            message: text_from(&mut mix),
            retryable: false,
            alert: false,
        };
        request.error(&mut out, &e).expect("fetch error surface");
        let _ = packets(&out);
        return;
    }
    let shallow = request.needs_shallow_info().then(|| ShallowInfo {
        shallow: some_of(&args.request.wants, &mut mix),
        unshallow: some_of(&args.request.client_shallow, &mut mix),
    });
    let mut negotiation = request
        .start(&mut out, shallow.as_ref())
        .expect("shallow section exactly when asked");
    loop {
        let p = match reader.read_packet() {
            Ok(Some(p)) => p,
            Ok(None) => {
                let _ = negotiation.end_of_input();
                break;
            }
            Err(_) => break,
        };
        let round = match negotiation.packet(p) {
            Ok(Negotiating::More(n)) => {
                negotiation = n;
                continue;
            }
            Ok(Negotiating::Round(r)) => r,
            Err(_) => break,
        };
        let common = some_of(round.haves(), &mut mix);
        // B2's policy: `ready` once a have is shared (an empty `ready` is
        // refused by design, `ReadyWithoutCommon`).
        let ready = (!common.is_empty() || !round.commons().is_empty()) && mix.flag();
        match round.answer(&mut out, &common, ready).expect("answer") {
            Answered::More(n) => negotiation = n,
            Answered::ResponseEnd => break,
            Answered::Pack { mut response, .. } => {
                let before = out.len();
                let _ = packets(&out[..before]);
                let pack: Vec<u8> = (0..mix.below(70_000)).map(|i| i as u8).collect();
                response.pack_data(&mut out, &pack).expect("pack data");
                response
                    .progress(&mut out, &text_from(&mut mix))
                    .expect("progress");
                response.finish(&mut out).expect("finish");
                if response.has_sideband() {
                    let _ = packets(&out[before..]);
                }
                return;
            }
        }
    }
    let _ = packets(&out);
}
