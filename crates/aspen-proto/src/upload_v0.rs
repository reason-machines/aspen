//! Protocol v0/v1 `git-upload-pack`, for clients without v2 fetch
//! (libgit2, go-git, and git with `protocol.version=0`):
//! `gitprotocol-pack(5)`, "Packfile Negotiation", and
//! `gitprotocol-capabilities(5)`.
//!
//! One fetch is:
//!
//! 1. Aspen sends [`write_advertisement`]: an optional `version 1` line,
//!    `HEAD` and the refs the service chose to show, with the capability
//!    list on the first line ([`V0_CAPABILITIES`]), or
//!    `capabilities^{}` when there is nothing to show. Over smart HTTP this
//!    is the `GET info/refs` response.
//! 2. The client sends its **header**: `want` lines (the first carries its
//!    capabilities after a space), then `shallow`, `deepen`,
//!    `deepen-since`, `deepen-not` and `filter` lines, and a flush.
//!    [`RequestParser`] turns it into an [`UploadRequest`].
//! 3. If the request has a `deepen` line, Aspen answers with the
//!    `shallow`/`unshallow` section and a flush
//!    ([`UploadRequest::start`] requires one exactly then).
//! 4. **Negotiation** ([`Negotiation`]): the client sends rounds of `have`
//!    lines, each ended by a flush, and finally `done`. After each round
//!    Aspen acknowledges the haves it shares ([`PendingRound::answer`]):
//!    with `multi_ack_detailed`, `ACK <oid> common` per shared have,
//!    `ACK <oid> ready` when it is ready to send the pack, then `NAK`;
//!    without it, one `ACK <oid>` for the first shared have only. After
//!    `done` it sends a final `ACK <last common>` or `NAK`. With `no-done`,
//!    a round answered with `ready` is followed directly by the final ACK
//!    and the pack, and the client never sends `done`.
//! 5. The **pack** ([`PackResponse`]), on side-band band 1 with progress on
//!    band 2 and a fatal error on band 3, then a flush; or raw, with no
//!    side-band, when the client did not ask for it.
//!
//! **Stateless and stateful.** Over smart HTTP every `POST` is a fresh
//! request that carries the whole header again, the haves the server
//! acknowledged as common in earlier rounds (the client replays them), and
//! one new round ending in a flush or `done`; the response ends after that
//! round's acknowledgements unless the pack follows
//! ([`Transport::Stateless`], as `git upload-pack --stateless-rpc` does).
//! Over a connection (SSH, `git://`, `ext::`) one exchange carries every
//! round ([`Transport::Stateful`]); with `no-done`, git keeps sending
//! rounds after the one answered `ready`, and the caller leaves that input
//! unread once [`Answered::Pack`] arrives, as `git upload-pack` does. A
//! deepen request gets its shallow
//! section in every stateless response, because each one starts from the
//! header again; the client reads and discards the repeats.
//!
//! The types are a chain, each consumed by the next step, so a caller
//! cannot skip one: [`RequestParser`] gives an [`UploadRequest`], whose
//! [`start`](UploadRequest::start) gives a [`Negotiation`], whose rounds
//! ([`PendingRound`]) are answered into the next [`Negotiation`], the end
//! of a stateless response, or a [`PackResponse`] with the final
//! [`FetchArgs`]. The parsing side applies [`V0Limits`] (A1's v2 limits,
//! `V0Limits::from(v2_limits)`, plus a cap on rounds) on the packet that
//! crosses each one.
//!
//! **Policy stays with the service (B2).** Which refs to show, whether a
//! have is shared, when to say `ready`, and which packs to send are the
//! caller's decisions. Aspen plans with the haves it acknowledged as common
//! ([`FetchArgs::request`]'s `haves`), resolved through
//! [`aspen_core::fetch::HaveVersion`] like v2's; a shallow request plans
//! the full closure of the wants and ignores them, whatever was
//! acknowledged (S5). v0 has no `packfile-uris`, so every v0 fetch gets
//! the stitched pack.
//!
//! # Serving a stateless request
//!
//! What `aspen-server` does for one `POST .../git-upload-pack` when
//! [`crate::service::UploadVersion::answering`] said
//! [`V0`](crate::service::UploadVersion::V0). Refuse what Aspen does not
//! serve (`plan_mode`, wants it cannot resolve) at the header with an
//! `ERR`; once the pack has started, errors go on band 3.
//!
//! ```
//! use aspen_proto::pkt::{self, PktReader};
//! use aspen_proto::upload_v0::{
//!     Answered, Header, Negotiating, RequestParser, Transport, V0Limits,
//! };
//!
//! // A clone from git: one want, no haves, done.
//! let mut body = Vec::new();
//! let want = format!("want {} multi_ack_detailed side-band-64k", "ab".repeat(20));
//! pkt::write_text(&mut body, &want)?;
//! pkt::write_flush(&mut body);
//! pkt::write_text(&mut body, "done")?;
//!
//! let mut reader = PktReader::new(&body[..]);
//! let mut out = Vec::new();
//! let mut parser = RequestParser::new(Transport::Stateless, V0Limits::default());
//! let req = loop {
//!     let p = reader.read_packet()?.expect("truncated"); // B2: ERR instead
//!     match parser.packet(p) {
//!         Ok(None) => {}
//!         Ok(Some(Header::Fetch(req))) => break req,
//!         Ok(Some(Header::End)) => return Ok(()),
//!         Err(e) => return Ok(pkt::write_err(&mut out, &e.to_string())),
//!     }
//! };
//! // B2: plan_mode, resolve the wants, compute shallow-info if deepen.
//! let mut neg = req.start(&mut out, None)?;
//! let (args, mut pack) = loop {
//!     let Some(p) = reader.read_packet()? else {
//!         neg.end_of_input()?; // header-only deepen request: done
//!         return Ok(());
//!     };
//!     neg = match neg.packet(p)? {
//!         Negotiating::More(n) => n,
//!         Negotiating::Round(round) => {
//!             // B2: which of round.haves() are known commits, and ready?
//!             match round.answer(&mut out, &[], false)? {
//!                 Answered::Pack { args, response } => break (args, response),
//!                 Answered::ResponseEnd => return Ok(()),
//!                 Answered::More(_) => unreachable!("stateless"),
//!             }
//!         }
//!     };
//! };
//! assert!(args.request.done && args.request.haves.is_empty());
//! // B2: plan with args.request, stitch, and stream the pack.
//! pack.pack_data(&mut out, b"PACK...")?;
//! pack.finish(&mut out)?;
//! assert!(out.starts_with(b"0008NAK\n"));
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

use std::collections::HashSet;
use std::num::NonZeroU32;

use aspen_core::error::{Surface, WireError};
use aspen_core::fetch::{Deepen, ShallowInfo};
use aspen_core::id::ObjectId;
use aspen_core::name::RefName;

use crate::caps::{self, Agent, OBJECT_FORMAT};
use crate::pkt::{self, Packet};
use crate::service::V0Version;
use crate::sideband::{self, Band, SidebandMode};
use crate::v2::{FetchArgs, MAX_REF_ARG_LEN, MAX_VALUE_LEN};

/// Capabilities Aspen advertises for a v0 fetch, in order. `symref`,
/// `object-format` and `agent` follow ([`V0Advertisement::capabilities`]).
///
/// - `multi_ack_detailed` and `no-done`: the negotiation git and libgit2
///   use; go-git asks for neither, and gets the single-`ACK` mode.
/// - `side-band-64k`: pack, progress and errors multiplexed. Plain
///   `side-band` (1000-byte packets) and plain `multi_ack` are not
///   advertised; every tested client uses the forms above.
/// - `thin-pack` and `ofs-delta`: accepted. Aspen never sends a thin pack
///   (stored packs are self-contained), which a client that asked for one
///   accepts as well.
/// - `shallow`, `deepen-since`, `deepen-not`, `deepen-relative`: shallow
///   fetches are served as in v2 (S5: the full closure of the wants plus
///   an exact boundary).
/// - `no-progress` and `include-tag`: as in v2's `fetch`.
///
/// `filter` is never advertised (S5), and neither is
/// `allow-tip-sha1-in-want` or `allow-reachable-sha1-in-want`: a client
/// wants what the advertisement showed, or uses protocol v2.
pub const V0_CAPABILITIES: [&str; 11] = [
    "multi_ack_detailed",
    "no-done",
    "side-band-64k",
    "thin-pack",
    "ofs-delta",
    "shallow",
    "deepen-since",
    "deepen-not",
    "deepen-relative",
    "no-progress",
    "include-tag",
];

/// Client capabilities that change the shape of the exchange. A client may
/// request one only if it was advertised; any other unknown capability is
/// ignored, as `git upload-pack` ignores them.
const EXCHANGE_CAPABILITIES: [&str; 5] = [
    "multi_ack",
    "multi_ack_detailed",
    "no-done",
    "side-band",
    "side-band-64k",
];

/// What Aspen advertises to a fetching v0 client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V0Advertisement {
    /// The `agent=` value.
    pub agent: Agent,
}

impl V0Advertisement {
    /// Aspen's v0 advertisement with the given agent.
    pub fn new(agent: Agent) -> Self {
        Self { agent }
    }

    /// The space-separated capability list. `symref=HEAD:<target>` is
    /// included when `HEAD` is shown and points at `head_target`.
    pub fn capabilities(&self, head_target: Option<&RefName>) -> String {
        let mut s = V0_CAPABILITIES.join(" ");
        if let Some(t) = head_target {
            s.push_str(" symref=HEAD:");
            s.push_str(t.as_str());
        }
        s.push_str(" object-format=");
        s.push_str(OBJECT_FORMAT);
        s.push_str(" agent=");
        s.push_str(self.agent.as_str());
        s
    }
}

/// `HEAD` in a v0 advertisement: Aspen points it at the trunk branch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V0Head {
    /// The trunk tip.
    pub tip: ObjectId,
    /// The trunk branch, advertised as `symref=HEAD:<target>`.
    pub target: RefName,
}

/// A ref in a v0 advertisement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V0Ref {
    /// The ref.
    pub name: RefName,
    /// Its tip.
    pub tip: ObjectId,
    /// For an annotated tag, the object it peels to, advertised as
    /// `<peeled> <name>^{}` on the next line.
    pub peeled: Option<ObjectId>,
}

/// Why a v0 advertisement can't be written. Each is a bug in the caller;
/// nothing is written.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum V0AdvertisementError {
    /// A tip or peeled ID that is zero: an unborn or deleted ref is not
    /// advertised.
    #[error("{0} has the zero object ID")]
    ZeroId(String),
    /// The same ref twice.
    #[error("ref {0} advertised twice")]
    DuplicateRef(RefName),
    /// `HEAD`'s target is advertised with a different tip.
    #[error("HEAD points at {0}, which is advertised with a different tip")]
    HeadMismatch(RefName),
}

/// Appends the v0 upload-pack advertisement: a `version 1` line for
/// [`V0Version::V1`], then `HEAD` (if shown), then `refs` in the order
/// given (Git sorts them by name; clients do not depend on it), each
/// annotated tag followed by its peeled line, and a flush. The capability
/// list rides on the first line; with nothing to show, the first line is
/// `0000000000000000000000000000000000000000 capabilities^{}`, without
/// `symref` (v0 cannot express an unborn `HEAD`; clients of an empty
/// repository use their own default branch). Over smart HTTP, write
/// [`crate::service::write_info_refs_header`] first.
pub fn write_advertisement(
    out: &mut Vec<u8>,
    version: V0Version,
    advert: &V0Advertisement,
    head: Option<&V0Head>,
    refs: &[V0Ref],
) -> Result<(), V0AdvertisementError> {
    let mut seen = HashSet::new();
    for r in refs {
        if r.tip.is_zero() || r.peeled.is_some_and(|p| p.is_zero()) {
            return Err(V0AdvertisementError::ZeroId(r.name.to_string()));
        }
        if !seen.insert(&r.name) {
            return Err(V0AdvertisementError::DuplicateRef(r.name.clone()));
        }
    }
    if let Some(h) = head {
        if h.tip.is_zero() {
            return Err(V0AdvertisementError::ZeroId("HEAD".to_owned()));
        }
        if refs.iter().any(|r| r.name == h.target && r.tip != h.tip) {
            return Err(V0AdvertisementError::HeadMismatch(h.target.clone()));
        }
    }
    if version == V0Version::V1 {
        pkt::write_text(out, "version 1").expect("static text fits");
    }
    let caps = advert.capabilities(head.map(|h| &h.target));
    let mut lines: Vec<(ObjectId, String)> = Vec::with_capacity(refs.len() + 1);
    if let Some(h) = head {
        lines.push((h.tip, "HEAD".to_owned()));
    }
    for r in refs {
        lines.push((r.tip, r.name.as_str().to_owned()));
        if let Some(p) = r.peeled {
            lines.push((p, format!("{}^{{}}", r.name)));
        }
    }
    let mut line = Vec::new();
    if lines.is_empty() {
        line.extend_from_slice(
            format!("{} capabilities^{{}}\0{caps}\n", ObjectId::ZERO).as_bytes(),
        );
        pkt::write_data(out, &line).expect("advertisement line fits");
    }
    for (i, (oid, name)) in lines.iter().enumerate() {
        line.clear();
        line.extend_from_slice(format!("{oid} {name}").as_bytes());
        if i == 0 {
            line.push(0);
            line.extend_from_slice(caps.as_bytes());
        }
        line.push(b'\n');
        pkt::write_data(out, &line).expect("ref names are at most 1024 bytes, so the line fits");
    }
    pkt::write_flush(out);
    Ok(())
}

/// How the exchange is carried.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Transport {
    /// Smart HTTP: one round per request, as `--stateless-rpc`.
    Stateless,
    /// One connection for the whole exchange: SSH, `git://`, `ext::`.
    Stateful,
}

/// How haves are acknowledged, from the client's capabilities.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckMode {
    /// No `multi_ack*`: one `ACK <oid>` for the first shared have, `NAK`
    /// at a flush only while nothing is shared, and nothing more after
    /// `done` once something was (go-git).
    ///
    /// Aspen follows the single-ACK behaviour `gitprotocol-pack(5)`
    /// documents: exactly one `ACK` per exchange. Stock `git upload-pack`
    /// (seen with 2.39.5) can send a second `ACK` for a later have whose
    /// parent it had already marked common. That is a known, accepted
    /// divergence: the documented single `ACK` is what this mode's clients
    /// are written against, and the go-git transcripts in the interop tests
    /// are answered this way.
    Single,
    /// `multi_ack_detailed`: `ACK <oid> common` per shared have,
    /// `ACK <oid> ready` when the server is ready, `NAK` after every round,
    /// and a final `ACK <last common>` (git, libgit2).
    Detailed,
}

/// The capabilities a fetching client asked for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V0Caps {
    /// How haves are acknowledged.
    pub ack: AckMode,
    /// `no-done`: a `ready` round is followed by the pack without `done`.
    /// Only with [`AckMode::Detailed`].
    pub no_done: bool,
    /// `side-band-64k`, or `None` for a raw pack.
    pub sideband: Option<SidebandMode>,
}

/// Limits on one v0 request, the same as A1's [`crate::v2::V2Limits`] for
/// v2 `fetch`: they bound what the server buffers before planning. Each is
/// checked on the packet that crosses it. On a stateful connection they
/// apply to the whole exchange (every round's haves together), which is
/// what the server holds for the final plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct V0Limits {
    /// Most bytes over every pkt-line payload of the request.
    pub max_request_bytes: usize,
    /// Most `want` lines.
    pub max_wants: usize,
    /// Most `have` lines.
    pub max_haves: usize,
    /// Most `shallow` and `deepen-not` lines together.
    pub max_shallow: usize,
    /// Most flush-ended negotiation rounds, checked on the flush that ends
    /// a round. Haves and bytes bound everything else, but a bare flush
    /// costs nothing against them, and on a stateful connection every flush
    /// earns a `NAK`: without this a client could keep the server answering
    /// empty rounds forever. Git grows its round size, so a client sending
    /// 100,000 haves over one connection needs about 80 rounds; a stateless
    /// request is a single round.
    pub max_rounds: usize,
}

impl V0Limits {
    /// Default for [`Self::max_rounds`].
    pub const DEFAULT_MAX_ROUNDS: usize = 1024;
}

impl Default for V0Limits {
    /// 8 MiB in all; 100,000 wants, haves and shallow lines; 1,024 rounds.
    fn default() -> Self {
        crate::v2::V2Limits::default().into()
    }
}

impl From<crate::v2::V2Limits> for V0Limits {
    /// The same request limits as v2 `fetch`, so a server configured once
    /// bounds both versions alike. v2 has no rounds to bound (each `fetch`
    /// command is answered on its own), so `max_rounds` takes its default.
    fn from(v2: crate::v2::V2Limits) -> Self {
        Self {
            max_request_bytes: v2.max_request_bytes,
            max_wants: v2.max_wants,
            max_haves: v2.max_haves,
            max_shallow: v2.max_shallow,
            max_rounds: Self::DEFAULT_MAX_ROUNDS,
        }
    }
}

/// Why a v0 request is malformed or refused. Answer with an `ERR`
/// pkt-line ([`pkt::write_err`] with this error's text) and end the
/// exchange; before the pack an `ERR` line is valid at every point, and
/// clients print it as `remote error: <text>`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum V0Error {
    /// A delimiter or response-end packet, which v0 does not use.
    #[error("protocol error: unexpected {0} packet")]
    UnexpectedPacket(&'static str),
    /// A line that is not UTF-8.
    #[error("protocol error: line is not valid UTF-8")]
    NotUtf8,
    /// A line that does not belong where it was sent. Only its first word
    /// is echoed.
    #[error("protocol error: unexpected line {0:?}")]
    UnexpectedLine(String),
    /// A `want`, `have` or `shallow` with a malformed object ID.
    #[error("protocol error: malformed object id in a {0} line")]
    BadObjectId(&'static str),
    /// Capabilities on a `want` line other than the first. Stricter than
    /// git, which parses features on every `want` line; no known client
    /// sends capabilities on a later want.
    #[error("protocol error: capabilities after the first want")]
    LateCapabilities,
    /// The client asked for a capability that changes the exchange and
    /// was not advertised.
    #[error("capability {0} was not advertised")]
    NotAdvertised(String),
    /// An object format other than SHA-1.
    #[error("object format {0} is not supported; this server serves sha1 repositories")]
    UnsupportedObjectFormat(String),
    /// `shallow` or `deepen` lines without any `want`.
    #[error("protocol error: no want lines")]
    NoWants,
    /// A bad `deepen` or `deepen-since` value, or a repeated one.
    #[error("invalid {0:?}")]
    BadDeepen(String),
    /// `deepen` together with `deepen-since` or `deepen-not`, which Git
    /// forbids too.
    #[error("deepen and deepen-since (or deepen-not) cannot be used together")]
    DeepenConflict,
    /// A limit in [`V0Limits`] was exceeded.
    #[error("request exceeds the server's limit on {0}")]
    LimitExceeded(&'static str),
    /// The input ended inside negotiation. Only a stateless request may end
    /// right after its header ([`Negotiation::end_of_input`]).
    #[error("protocol error: the request ended before done or a flush")]
    Truncated,
    /// A value longer than its cap. The value is not echoed.
    #[error("{what} value is longer than the server's limit of {max} bytes")]
    ValueTooLong {
        /// The capability or line.
        what: &'static str,
        /// Its limit.
        max: usize,
    },
}

/// The first word of a line, cut short, for [`V0Error::UnexpectedLine`].
fn first_word(line: &str) -> String {
    let w = line.split(' ').next().unwrap_or_default();
    pkt::one_line(&w.chars().take(64).collect::<String>())
}

fn check_len(what: &'static str, value: &str, max: usize) -> Result<(), V0Error> {
    if value.len() > max {
        return Err(V0Error::ValueTooLong { what, max });
    }
    Ok(())
}

fn text<'a>(p: &Packet<'a>) -> Result<&'a str, V0Error> {
    let line = p.text().expect("data packet");
    std::str::from_utf8(line).map_err(|_| V0Error::NotUtf8)
}

/// Byte and count budget shared by the header and every negotiation round.
#[derive(Debug, Clone, Copy)]
struct Budget {
    limits: V0Limits,
    bytes: usize,
    haves: usize,
    rounds: usize,
}

impl Budget {
    fn charge(&mut self, p: &Packet<'_>) -> Result<(), V0Error> {
        if let Packet::Data(d) = p {
            self.bytes += d.len();
            if self.bytes > self.limits.max_request_bytes {
                return Err(V0Error::LimitExceeded("request size"));
            }
        }
        Ok(())
    }
}

/// What the header parser returns.
#[derive(Debug)]
#[allow(
    clippy::large_enum_variant,
    reason = "returned once per request and consumed at once, never stored in bulk"
)]
pub enum Header {
    /// A fetch: answer it with [`UploadRequest::start`] or
    /// [`UploadRequest::error`].
    Fetch(UploadRequest),
    /// A flush with nothing before it: the client is done without fetching
    /// (`git ls-remote` over a connection). Send nothing more.
    End,
}

/// Incremental parser for the header of a v0 fetch: `want`, `shallow` and
/// `deepen*` lines up to the first flush. Feed it packets with
/// [`RequestParser::packet`] until it returns a [`Header`].
#[derive(Debug)]
pub struct RequestParser {
    transport: Transport,
    budget: Budget,
    args: FetchArgs,
    caps: Option<V0Caps>,
    depth: Option<NonZeroU32>,
    since: Option<i64>,
    not: Vec<String>,
    lines: usize,
}

impl RequestParser {
    /// A parser for one request (stateless) or one connection (stateful).
    pub fn new(transport: Transport, limits: V0Limits) -> Self {
        Self {
            transport,
            budget: Budget {
                limits,
                bytes: 0,
                haves: 0,
                rounds: 0,
            },
            args: FetchArgs::default(),
            caps: None,
            depth: None,
            since: None,
            not: Vec::new(),
            lines: 0,
        }
    }

    /// Consumes one packet; returns the header after its flush. After an
    /// error the parser must not be used again.
    pub fn packet(&mut self, p: Packet<'_>) -> Result<Option<Header>, V0Error> {
        self.budget.charge(&p)?;
        match p {
            Packet::Delim => Err(V0Error::UnexpectedPacket("delimiter")),
            Packet::ResponseEnd => Err(V0Error::UnexpectedPacket("response-end")),
            Packet::Flush => self.finish().map(Some),
            Packet::Data(_) => {
                let line = text(&p)?;
                self.lines += 1;
                self.line(line).map(|()| None)
            }
        }
    }

    fn line(&mut self, line: &str) -> Result<(), V0Error> {
        let limits = self.budget.limits;
        let r = &mut self.args.request;
        let (key, value) = match line.split_once(' ') {
            Some((k, v)) => (k, Some(v)),
            None => (line, None),
        };
        match (key, value) {
            ("want", Some(v)) => {
                let (hex, caps) = match v.split_once(' ') {
                    Some((h, c)) => (h, Some(c)),
                    None => (v, None),
                };
                let oid = ObjectId::parse_hex(hex).map_err(|_| V0Error::BadObjectId("want"))?;
                if r.wants.len() >= limits.max_wants {
                    return Err(V0Error::LimitExceeded("wants"));
                }
                let first = r.wants.is_empty();
                r.wants.push(oid);
                match (first, caps) {
                    (true, caps) => self.client_caps(caps.unwrap_or_default())?,
                    (false, Some(_)) => return Err(V0Error::LateCapabilities),
                    (false, None) => {}
                }
            }
            ("shallow", Some(v)) => {
                let oid = ObjectId::parse_hex(v).map_err(|_| V0Error::BadObjectId("shallow"))?;
                if r.client_shallow.len() + self.not.len() >= limits.max_shallow {
                    return Err(V0Error::LimitExceeded("shallow lines"));
                }
                r.client_shallow.push(oid);
            }
            ("deepen", Some(v)) => {
                let d = v
                    .parse::<u32>()
                    .ok()
                    .and_then(NonZeroU32::new)
                    .ok_or_else(|| V0Error::BadDeepen(pkt::one_line(line)))?;
                if self.depth.replace(d).is_some() {
                    return Err(V0Error::BadDeepen(pkt::one_line(line)));
                }
            }
            ("deepen-since", Some(v)) => {
                let t = v
                    .parse::<i64>()
                    .map_err(|_| V0Error::BadDeepen(pkt::one_line(line)))?;
                if self.since.replace(t).is_some() {
                    return Err(V0Error::BadDeepen(pkt::one_line(line)));
                }
            }
            ("deepen-not", Some(v)) if !v.is_empty() => {
                check_len("deepen-not", v, MAX_REF_ARG_LEN)?;
                if r.client_shallow.len() + self.not.len() >= limits.max_shallow {
                    return Err(V0Error::LimitExceeded("shallow lines"));
                }
                self.not.push(v.to_owned());
            }
            ("filter", Some(v)) => {
                // Never advertised (S5), but parsed so that the service
                // answers it with core's clear `ERR`, as in v2.
                check_len("filter", v, MAX_VALUE_LEN)?;
                r.filter = Some(v.to_owned());
            }
            _ => return Err(V0Error::UnexpectedLine(first_word(line))),
        }
        Ok(())
    }

    fn client_caps(&mut self, list: &str) -> Result<(), V0Error> {
        let mut ack = AckMode::Single;
        let mut no_done = false;
        let mut sideband = None;
        let r = &mut self.args.request;
        for c in caps::parse_list(list) {
            if EXCHANGE_CAPABILITIES.contains(&c.name.as_str())
                && !V0_CAPABILITIES.contains(&c.name.as_str())
            {
                return Err(V0Error::NotAdvertised(c.name));
            }
            match c.name.as_str() {
                "multi_ack_detailed" => ack = AckMode::Detailed,
                "no-done" => no_done = true,
                "side-band-64k" => sideband = Some(SidebandMode::Large),
                "thin-pack" => r.thin_pack = true,
                "ofs-delta" => r.ofs_delta = true,
                "deepen-relative" => r.deepen_relative = true,
                "no-progress" => self.args.no_progress = true,
                "include-tag" => self.args.include_tag = true,
                "agent" => {
                    let v = c.value.unwrap_or_default();
                    check_len("agent", &v, MAX_VALUE_LEN)?;
                    r.agent = Some(v);
                }
                "object-format" => {
                    let v = c.value.unwrap_or_default();
                    if v != OBJECT_FORMAT {
                        return Err(V0Error::UnsupportedObjectFormat(pkt::one_line(&v)));
                    }
                }
                // `shallow`, `deepen-since`, `deepen-not`, `filter`,
                // `session-id` and anything else: markers or unknown
                // capabilities, ignored as Git ignores them. The lines they
                // announce are parsed on their own.
                _ => {}
            }
        }
        self.caps = Some(V0Caps {
            ack,
            no_done: no_done && ack == AckMode::Detailed,
            sideband,
        });
        Ok(())
    }

    fn finish(&mut self) -> Result<Header, V0Error> {
        if self.args.request.wants.is_empty() {
            return if self.lines == 0 {
                Ok(Header::End)
            } else {
                Err(V0Error::NoWants)
            };
        }
        let mut args = std::mem::take(&mut self.args);
        let not = std::mem::take(&mut self.not);
        args.request.deepen = match (self.depth, self.since, not.is_empty()) {
            (Some(_), Some(_), _) | (Some(_), _, false) => return Err(V0Error::DeepenConflict),
            (Some(d), None, true) => Some(Deepen::Depth(d)),
            (None, None, true) => None,
            (None, since, _) => Some(Deepen::Boundary { since, not }),
        };
        Ok(Header::Fetch(UploadRequest {
            args,
            caps: self
                .caps
                .take()
                .expect("the first want sets the capabilities"),
            transport: self.transport,
            budget: self.budget,
        }))
    }
}

/// A response written against the protocol or against what the client
/// asked for. Each is a bug in the caller; nothing is written.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum V0ResponseError {
    /// A request with `deepen` started without a shallow section.
    #[error("a deepen request needs a shallow section before negotiation")]
    ShallowInfoRequired,
    /// A shallow section for a request without `deepen`: the client would
    /// not read it (Git sends none for a shallow client that only sends
    /// `shallow` lines).
    #[error("shallow section for a request without deepen")]
    ShallowInfoNotRequested,
    /// An acknowledged object that was not a have of this round.
    #[error("{0} is not a have of this round")]
    NotAHave(ObjectId),
    /// `ready` before any have was acknowledged as common: the client
    /// would stop negotiating with nothing in common.
    #[error("ready without any common have")]
    ReadyWithoutCommon,
    /// A [`WireError`] meant for another surface.
    #[error("error for surface {0:?} used in a fetch response")]
    WrongSurface(Surface),
    /// Pack data, progress or an error after the response ended.
    #[error("the response has already ended")]
    Finished,
}

fn check_surface(e: &WireError) -> Result<(), V0ResponseError> {
    if e.surface != Surface::FetchErr {
        return Err(V0ResponseError::WrongSurface(e.surface));
    }
    Ok(())
}

fn line(out: &mut Vec<u8>, l: &str) {
    pkt::write_text(out, l).expect("short protocol line");
}

/// A parsed v0 fetch header, before any response is written.
#[derive(Debug)]
pub struct UploadRequest {
    args: FetchArgs,
    caps: V0Caps,
    transport: Transport,
    budget: Budget,
}

impl UploadRequest {
    /// Wants, shallow and deepen, filter, agent and flags. `haves` is
    /// empty until negotiation ends ([`Answered::Pack`]).
    pub fn args(&self) -> &FetchArgs {
        &self.args
    }

    /// The client's negotiation and side-band capabilities.
    pub fn caps(&self) -> &V0Caps {
        &self.caps
    }

    /// Whether the request has a `deepen`, `deepen-since` or `deepen-not`
    /// line, and so needs a shallow section. A client that is shallow but
    /// does not deepen sends only `shallow` lines: its request is shallow
    /// ([`aspen_core::fetch::FetchRequest::is_shallow`], planned as the
    /// full closure) but gets no shallow section.
    pub fn needs_shallow_info(&self) -> bool {
        self.args.request.deepen.is_some()
    }

    /// Starts the response. `shallow` is the boundary for a deepen request
    /// (required exactly when [`Self::needs_shallow_info`]), written as
    /// `shallow <oid>`/`unshallow <oid>` lines with no trailing LF, as Git
    /// writes them, and a flush. Check what Aspen refuses
    /// (`plan_mode`, unknown wants) before this, and answer with
    /// [`Self::error`] instead.
    pub fn start(
        self,
        out: &mut Vec<u8>,
        shallow: Option<&ShallowInfo>,
    ) -> Result<Negotiation, V0ResponseError> {
        match (self.needs_shallow_info(), shallow) {
            (true, None) => return Err(V0ResponseError::ShallowInfoRequired),
            (false, Some(_)) => return Err(V0ResponseError::ShallowInfoNotRequested),
            (true, Some(info)) => {
                // Without a trailing LF, byte for byte as `git upload-pack`
                // writes them: libgit2 refuses `shallow <oid>\n` with
                // "invalid packet line" (its parser checks the exact
                // length), and git and go-git accept both.
                let bare = |out: &mut Vec<u8>, l: String| {
                    pkt::write_data(out, l.as_bytes()).expect("short protocol line");
                };
                for s in &info.shallow {
                    bare(out, format!("shallow {s}"));
                }
                for u in &info.unshallow {
                    bare(out, format!("unshallow {u}"));
                }
                pkt::write_flush(out);
            }
            (false, None) => {}
        }
        Ok(Negotiation {
            args: self.args,
            caps: self.caps,
            transport: self.transport,
            budget: self.budget,
            round: Vec::new(),
            commons: Vec::new(),
            common_set: HashSet::new(),
            last_common: None,
            started: false,
        })
    }

    /// Refuses the request with an `ERR` line. `e` must be a
    /// [`Surface::FetchErr`] error.
    pub fn error(self, out: &mut Vec<u8>, e: &WireError) -> Result<(), V0ResponseError> {
        check_surface(e)?;
        pkt::write_err(out, &e.message);
        Ok(())
    }
}

/// The negotiation phase: `have` rounds until `done` (or `ready` with
/// `no-done`).
#[derive(Debug)]
pub struct Negotiation {
    args: FetchArgs,
    caps: V0Caps,
    transport: Transport,
    budget: Budget,
    round: Vec<ObjectId>,
    /// Haves acknowledged as common, in the order first acknowledged,
    /// once each.
    commons: Vec<ObjectId>,
    common_set: HashSet<ObjectId>,
    /// The most recent have acknowledged as common, repeats included: the
    /// object of `ACK <oid> ready` and the final `ACK <oid>`, as Git's
    /// `last_hex`.
    last_common: Option<ObjectId>,
    /// Whether any packet was read after the header.
    started: bool,
}

/// What [`Negotiation::packet`] returns.
#[derive(Debug)]
pub enum Negotiating {
    /// The round goes on: feed the next packet.
    More(Negotiation),
    /// A round ended (a flush, or `done`): answer it.
    Round(PendingRound),
}

impl Negotiation {
    /// Consumes one packet.
    pub fn packet(mut self, p: Packet<'_>) -> Result<Negotiating, V0Error> {
        self.budget.charge(&p)?;
        self.started = true;
        match p {
            Packet::Delim => Err(V0Error::UnexpectedPacket("delimiter")),
            Packet::ResponseEnd => Err(V0Error::UnexpectedPacket("response-end")),
            Packet::Flush => {
                if self.budget.rounds >= self.budget.limits.max_rounds {
                    return Err(V0Error::LimitExceeded("negotiation rounds"));
                }
                self.budget.rounds += 1;
                Ok(Negotiating::Round(PendingRound {
                    negotiation: self,
                    done: false,
                }))
            }
            Packet::Data(_) => {
                let l = text(&p)?;
                if l == "done" {
                    return Ok(Negotiating::Round(PendingRound {
                        negotiation: self,
                        done: true,
                    }));
                }
                let Some(hex) = l.strip_prefix("have ") else {
                    return Err(V0Error::UnexpectedLine(first_word(l)));
                };
                let oid = ObjectId::parse_hex(hex).map_err(|_| V0Error::BadObjectId("have"))?;
                if self.budget.haves >= self.budget.limits.max_haves {
                    return Err(V0Error::LimitExceeded("haves"));
                }
                self.budget.haves += 1;
                self.round.push(oid);
                Ok(Negotiating::More(self))
            }
        }
    }

    /// The input ended. A stateless request may end right after its header:
    /// git's first request of a deepen fetch is only the header, to learn
    /// the shallow boundary before it sends haves, and the response is then
    /// complete with the shallow section ([`UploadRequest::start`] wrote
    /// it), as `git upload-pack --stateless-rpc` answers it. Anywhere else
    /// the request is truncated: answer [`V0Error::Truncated`] with `ERR`
    /// (a stateful client that hangs up is gone, and nothing needs
    /// writing).
    pub fn end_of_input(self) -> Result<(), V0Error> {
        if self.transport == Transport::Stateless && !self.started {
            Ok(())
        } else {
            Err(V0Error::Truncated)
        }
    }

    /// Refuses the request with an `ERR` line, valid at any point before
    /// the pack. `e` must be a [`Surface::FetchErr`] error.
    pub fn error(self, out: &mut Vec<u8>, e: &WireError) -> Result<(), V0ResponseError> {
        check_surface(e)?;
        pkt::write_err(out, &e.message);
        Ok(())
    }

    /// The request being negotiated.
    pub fn args(&self) -> &FetchArgs {
        &self.args
    }

    /// The haves acknowledged as common so far, in order.
    pub fn commons(&self) -> &[ObjectId] {
        &self.commons
    }
}

/// A round of haves waiting for its answer.
#[derive(Debug)]
pub struct PendingRound {
    negotiation: Negotiation,
    done: bool,
}

/// What answering a round leads to.
#[derive(Debug)]
pub enum Answered {
    /// Stateful: read the next round from the same connection.
    More(Negotiation),
    /// Stateless: the response is complete; the client sends its next
    /// round in a new request.
    ResponseEnd,
    /// Negotiation is over: plan with `args` (whose `haves` are the
    /// acknowledged commons and `done` is set) and send the pack.
    Pack {
        /// The final request.
        args: FetchArgs,
        /// The writer for the pack.
        response: PackResponse,
    },
}

impl PendingRound {
    /// The haves of this round, in the order sent. In a stateless request
    /// this includes the commons of earlier rounds, which the client
    /// replays.
    pub fn haves(&self) -> &[ObjectId] {
        &self.negotiation.round
    }

    /// Whether the round ended with `done`.
    pub fn is_done(&self) -> bool {
        self.done
    }

    /// The request being negotiated.
    pub fn args(&self) -> &FetchArgs {
        &self.negotiation.args
    }

    /// The client's capabilities.
    pub fn caps(&self) -> &V0Caps {
        &self.negotiation.caps
    }

    /// The haves acknowledged as common in earlier rounds.
    pub fn commons(&self) -> &[ObjectId] {
        &self.negotiation.commons
    }

    /// Answers the round: `common` are the haves of this round the server
    /// shares (any order; each must be one of [`Self::haves`]), `ready`
    /// says the server would send the pack now. `ready` applies to a
    /// flush-ended round with `multi_ack_detailed` and needs a common have
    /// in this round or an earlier one; after `done`, or in the
    /// single-`ACK` mode, the client decides and `ready` is ignored.
    pub fn answer(
        self,
        out: &mut Vec<u8>,
        common: &[ObjectId],
        ready: bool,
    ) -> Result<Answered, V0ResponseError> {
        let wanted: HashSet<ObjectId> = common.iter().copied().collect();
        let mut n = self.negotiation;
        let in_round: HashSet<ObjectId> = n.round.iter().copied().collect();
        if let Some(c) = wanted.iter().find(|c| !in_round.contains(c)) {
            return Err(V0ResponseError::NotAHave(*c));
        }
        let detailed = n.caps.ack == AckMode::Detailed;
        let ready = ready && detailed && !self.done;
        let any_common = !n.commons.is_empty() || !wanted.is_empty();
        if ready && !any_common {
            return Err(V0ResponseError::ReadyWithoutCommon);
        }
        // Checks done: from here on the answer is written.
        for h in std::mem::take(&mut n.round) {
            if !wanted.contains(&h) {
                continue;
            }
            let first = n.commons.is_empty();
            if n.common_set.insert(h) {
                n.commons.push(h);
            }
            n.last_common = Some(h);
            match n.caps.ack {
                AckMode::Detailed => line(out, &format!("ACK {h} common")),
                AckMode::Single if first => line(out, &format!("ACK {h}")),
                AckMode::Single => {}
            }
        }
        let last = n.last_common;
        if self.done {
            match (n.caps.ack, last) {
                (AckMode::Detailed, Some(l)) => line(out, &format!("ACK {l}")),
                (_, None) => line(out, "NAK"),
                (AckMode::Single, Some(_)) => {}
            }
            return Ok(n.into_pack());
        }
        match n.caps.ack {
            AckMode::Detailed => {
                if ready {
                    let l = last.expect("checked above");
                    line(out, &format!("ACK {l} ready"));
                }
                line(out, "NAK");
                if ready && n.caps.no_done {
                    let l = last.expect("checked above");
                    line(out, &format!("ACK {l}"));
                    return Ok(n.into_pack());
                }
            }
            AckMode::Single => {
                if last.is_none() {
                    line(out, "NAK");
                }
            }
        }
        Ok(match n.transport {
            Transport::Stateless => Answered::ResponseEnd,
            Transport::Stateful => Answered::More(n),
        })
    }

    /// Refuses the request with an `ERR` line. `e` must be a
    /// [`Surface::FetchErr`] error.
    pub fn error(self, out: &mut Vec<u8>, e: &WireError) -> Result<(), V0ResponseError> {
        self.negotiation.error(out, e)
    }
}

impl Negotiation {
    fn into_pack(self) -> Answered {
        let mut args = self.args;
        args.request.haves = self.commons;
        args.request.done = true;
        Answered::Pack {
            response: PackResponse {
                sideband: self.caps.sideband,
                no_progress: args.no_progress,
                finished: false,
            },
            args,
        }
    }
}

/// Writes the pack part of a v0 fetch response. With side-band, pack data
/// goes on band 1, progress on band 2 (unless `no-progress`), a fatal
/// error on band 3, and a flush ends the response. Without side-band the
/// pack is written raw, progress is dropped, and an error cannot be shown:
/// the caller must drop the connection, so the client fails on a short
/// pack.
#[derive(Debug)]
pub struct PackResponse {
    sideband: Option<SidebandMode>,
    no_progress: bool,
    finished: bool,
}

impl PackResponse {
    fn live(&self) -> Result<(), V0ResponseError> {
        if self.finished {
            return Err(V0ResponseError::Finished);
        }
        Ok(())
    }

    /// Whether the client asked for side-band, so errors can be shown.
    pub fn has_sideband(&self) -> bool {
        self.sideband.is_some()
    }

    /// Appends pack bytes.
    pub fn pack_data(&mut self, out: &mut Vec<u8>, data: &[u8]) -> Result<(), V0ResponseError> {
        self.live()?;
        match self.sideband {
            Some(mode) => sideband::write_band(out, mode, Band::Data, data),
            None => out.extend_from_slice(data),
        }
        Ok(())
    }

    /// A progress line on band 2, unless the client sent `no-progress` or
    /// did not ask for side-band.
    pub fn progress(&mut self, out: &mut Vec<u8>, message: &str) -> Result<(), V0ResponseError> {
        self.live()?;
        if let (Some(mode), false) = (self.sideband, self.no_progress) {
            sideband::write_message(out, mode, Band::Progress, message);
        }
        Ok(())
    }

    /// Ends the response: a flush with side-band, nothing without.
    pub fn finish(&mut self, out: &mut Vec<u8>) -> Result<(), V0ResponseError> {
        self.live()?;
        if self.sideband.is_some() {
            pkt::write_flush(out);
        }
        self.finished = true;
        Ok(())
    }

    /// Ends the response with an error: band 3 and a flush (stock git
    /// prints `remote: <message>` and fails). Without side-band nothing is
    /// written and the caller must drop the connection. Returns whether
    /// the error was written. `e` must be a [`Surface::FetchErr`] error.
    pub fn error(&mut self, out: &mut Vec<u8>, e: &WireError) -> Result<bool, V0ResponseError> {
        check_surface(e)?;
        self.live()?;
        self.finished = true;
        let Some(mode) = self.sideband else {
            return Ok(false);
        };
        sideband::write_message(out, mode, Band::Error, &e.message);
        pkt::write_flush(out);
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pkt::decode;
    use crate::pkt::tests::noise;
    use aspen_core::error::{FetchError, RejectReason, ToWire};

    fn oid(n: u8) -> ObjectId {
        ObjectId::from_bytes([n; 20])
    }
    fn rname(s: &str) -> RefName {
        RefName::new(s).unwrap()
    }
    fn advert() -> V0Advertisement {
        V0Advertisement::new(Agent::new("aspen/test").unwrap())
    }
    /// Renders pkt-lines as text, `0000` for a flush, `\0` visible.
    fn lines(mut wire: &[u8]) -> Vec<String> {
        let mut out = Vec::new();
        while let Some((p, n)) = decode(wire).unwrap() {
            out.push(match p {
                Packet::Flush => "0000".into(),
                Packet::Delim => "0001".into(),
                Packet::ResponseEnd => "0002".into(),
                Packet::Data(d) => String::from_utf8_lossy(d)
                    .trim_end_matches('\n')
                    .replace('\0', "\\0"),
            });
            wire = &wire[n..];
        }
        assert!(wire.is_empty(), "trailing bytes");
        out
    }
    fn request(lines: &[&str]) -> Vec<u8> {
        let mut out = Vec::new();
        for l in lines {
            match *l {
                "0000" => pkt::write_flush(&mut out),
                _ => pkt::write_text(&mut out, l).unwrap(),
            }
        }
        out
    }
    fn packets(wire: &[u8]) -> Vec<Packet<'_>> {
        let mut out = Vec::new();
        let mut at = 0;
        while let Some((p, n)) = decode(&wire[at..]).unwrap() {
            out.push(p);
            at += n;
        }
        out
    }
    fn header_with(t: Transport, limits: V0Limits, ls: &[&str]) -> Result<Header, V0Error> {
        let wire = request(ls);
        let mut p = RequestParser::new(t, limits);
        for (i, pk) in packets(&wire).into_iter().enumerate() {
            if let Some(h) = p.packet(pk)? {
                assert_eq!(i + 1, ls.len(), "header ends at its flush");
                return Ok(h);
            }
        }
        panic!("header did not end");
    }
    fn header(ls: &[&str]) -> Result<UploadRequest, V0Error> {
        match header_with(Transport::Stateless, V0Limits::default(), ls)? {
            Header::Fetch(r) => Ok(r),
            Header::End => panic!("end"),
        }
    }
    const GIT_CAPS: &str = "multi_ack_detailed no-done side-band-64k thin-pack \
                            include-tag ofs-delta deepen-since deepen-not agent=git/2.39.5";

    #[test]
    fn advertisement_is_byte_exact() {
        let head = V0Head {
            tip: oid(1),
            target: rname("refs/heads/main"),
        };
        let refs = [
            V0Ref {
                name: rname("refs/heads/main"),
                tip: oid(1),
                peeled: None,
            },
            V0Ref {
                name: rname("refs/tags/v1"),
                tip: oid(2),
                peeled: Some(oid(1)),
            },
        ];
        let mut out = Vec::new();
        write_advertisement(&mut out, V0Version::V0, &advert(), Some(&head), &refs).unwrap();
        let caps = "multi_ack_detailed no-done side-band-64k thin-pack ofs-delta shallow \
                    deepen-since deepen-not deepen-relative no-progress include-tag \
                    symref=HEAD:refs/heads/main object-format=sha1 agent=aspen/test";
        assert_eq!(
            lines(&out),
            [
                format!("{} HEAD\\0{caps}", oid(1)),
                format!("{} refs/heads/main", oid(1)),
                format!("{} refs/tags/v1", oid(2)),
                format!("{} refs/tags/v1^{{}}", oid(1)),
                "0000".into(),
            ]
        );
        // The first line's exact framing: NUL before the list, LF after it.
        let (first, _) = decode(&out).unwrap().unwrap();
        let Packet::Data(d) = first else { panic!() };
        assert_eq!(d, format!("{} HEAD\0{caps}\n", oid(1)).as_bytes());

        // v1 puts a version line first; the rest is identical.
        let mut v1 = Vec::new();
        write_advertisement(&mut v1, V0Version::V1, &advert(), Some(&head), &refs).unwrap();
        assert_eq!(&v1[..15], b"000eversion 1\n0");
        assert_eq!(&v1[14..], &out[..]);
    }

    /// Invariant: an empty repository advertises `capabilities^{}` with
    /// the zero ID and no `symref`; an advertisement never offers what
    /// Aspen does not serve (`filter`, plain `multi_ack`, small
    /// `side-band`, sha256).
    #[test]
    fn empty_advertisement_and_what_is_never_offered() {
        let mut out = Vec::new();
        write_advertisement(&mut out, V0Version::V0, &advert(), None, &[]).unwrap();
        let l = lines(&out);
        assert_eq!(l.len(), 2);
        assert!(l[0].starts_with(&format!("{} capabilities^{{}}\\0", ObjectId::ZERO)));
        assert!(!l[0].contains("symref"));
        let caps: Vec<_> = advert()
            .capabilities(None)
            .split(' ')
            .map(str::to_owned)
            .collect();
        for never in ["filter", "multi_ack", "side-band", "object-format=sha256"] {
            assert!(!caps.iter().any(|c| c == never), "{never}");
        }
    }

    #[test]
    fn advertisement_refuses_inconsistent_input() {
        let r = |n: &str, t: u8| V0Ref {
            name: rname(n),
            tip: oid(t),
            peeled: None,
        };
        let head = |t: u8| V0Head {
            tip: oid(t),
            target: rname("refs/heads/main"),
        };
        let cases: [(Option<V0Head>, Vec<V0Ref>, V0AdvertisementError); 4] = [
            (
                None,
                vec![r("refs/heads/a", 0)],
                V0AdvertisementError::ZeroId("refs/heads/a".into()),
            ),
            (
                None,
                vec![r("refs/heads/a", 1), r("refs/heads/a", 2)],
                V0AdvertisementError::DuplicateRef(rname("refs/heads/a")),
            ),
            (
                Some(head(0)),
                vec![],
                V0AdvertisementError::ZeroId("HEAD".into()),
            ),
            (
                Some(head(1)),
                vec![r("refs/heads/main", 2)],
                V0AdvertisementError::HeadMismatch(rname("refs/heads/main")),
            ),
        ];
        for (h, refs, err) in cases {
            let mut out = Vec::new();
            assert_eq!(
                write_advertisement(&mut out, V0Version::V0, &advert(), h.as_ref(), &refs),
                Err(err)
            );
            assert!(out.is_empty(), "nothing written on error");
        }
        let peeled_zero = V0Ref {
            peeled: Some(ObjectId::ZERO),
            ..r("refs/tags/t", 1)
        };
        assert!(
            write_advertisement(
                &mut Vec::new(),
                V0Version::V0,
                &advert(),
                None,
                &[peeled_zero]
            )
            .is_err()
        );
    }

    #[test]
    fn parses_git_and_go_git_headers() {
        // git 2.39 (`protocol.version=0`), from a packet trace.
        let r = header(&[
            &format!("want {} {GIT_CAPS}", oid(1)),
            &format!("want {}", oid(2)),
            "0000",
        ])
        .unwrap();
        assert_eq!(r.args().request.wants, [oid(1), oid(2)]);
        assert_eq!(
            r.caps(),
            &V0Caps {
                ack: AckMode::Detailed,
                no_done: true,
                sideband: Some(SidebandMode::Large),
            }
        );
        let a = r.args();
        assert!(a.request.thin_pack && a.request.ofs_delta && a.include_tag && !a.no_progress);
        assert_eq!(a.request.agent.as_deref(), Some("git/2.39.5"));
        assert!(!a.request.is_shallow() && !r.needs_shallow_info());
        assert!(a.request.haves.is_empty() && !a.request.done);

        // go-git v5: no multi_ack at all, `shallow` as a capability.
        let r = header(&[
            &format!(
                "want {} ofs-delta side-band-64k shallow agent=go-git/5.x",
                oid(1)
            ),
            &format!("shallow {}", oid(3)),
            "deepen 1",
            "0000",
        ])
        .unwrap();
        assert_eq!(r.caps().ack, AckMode::Single);
        assert!(!r.caps().no_done);
        assert_eq!(r.args().request.client_shallow, [oid(3)]);
        assert_eq!(
            r.args().request.deepen,
            Some(Deepen::Depth(NonZeroU32::new(1).unwrap()))
        );
        assert!(r.needs_shallow_info());

        // A want with no capabilities at all: raw pack, single ACKs.
        let r = header(&[&format!("want {}", oid(1)), "0000"]).unwrap();
        assert_eq!(
            r.caps(),
            &V0Caps {
                ack: AckMode::Single,
                no_done: false,
                sideband: None
            }
        );
        // `no-done` means nothing without `multi_ack_detailed`.
        let r = header(&[&format!("want {} no-done", oid(1)), "0000"]).unwrap();
        assert!(!r.caps().no_done);
    }

    /// Invariant: a capability that changes the exchange is accepted only
    /// if Aspen advertised it; other unknown capabilities are ignored.
    #[test]
    fn unadvertised_exchange_capabilities_are_refused() {
        for cap in ["multi_ack", "side-band"] {
            assert_eq!(
                header(&[&format!("want {} {cap}", oid(1)), "0000"]).unwrap_err(),
                V0Error::NotAdvertised(cap.into())
            );
        }
        let r = header(&[
            &format!(
                "want {} session-id=abc frobnicate allow-tip-sha1-in-want",
                oid(1)
            ),
            "0000",
        ])
        .unwrap();
        assert_eq!(r.caps().ack, AckMode::Single);
        assert_eq!(
            header(&[&format!("want {} object-format=sha256", oid(1)), "0000"]).unwrap_err(),
            V0Error::UnsupportedObjectFormat("sha256".into())
        );
        assert!(header(&[&format!("want {} object-format=sha1", oid(1)), "0000"]).is_ok());
        assert_eq!(
            header(&[
                &format!("want {}", oid(1)),
                &format!("want {} ofs-delta", oid(2)),
                "0000"
            ])
            .unwrap_err(),
            V0Error::LateCapabilities
        );
    }

    #[test]
    fn deepen_lines_map_to_core_deepen() {
        let w = format!("want {} shallow deepen-relative", oid(1));
        let r = header(&[&w, "deepen 2147483647", "0000"]).unwrap();
        assert_eq!(
            r.args().request.deepen,
            Some(Deepen::Depth(NonZeroU32::new(0x7fff_ffff).unwrap())),
            "--unshallow sends Git's INFINITE_DEPTH"
        );
        assert!(r.args().request.deepen_relative);
        let r = header(&[
            &w,
            "deepen-since 1700000000",
            "deepen-not refs/heads/old",
            "0000",
        ])
        .unwrap();
        assert_eq!(
            r.args().request.deepen,
            Some(Deepen::Boundary {
                since: Some(1_700_000_000),
                not: vec!["refs/heads/old".into()]
            })
        );
        for (extra, err) in [
            (&["deepen 1", "deepen-since 5"][..], V0Error::DeepenConflict),
            (&["deepen 1", "deepen-not x"][..], V0Error::DeepenConflict),
            (&["deepen 0"][..], V0Error::BadDeepen("deepen 0".into())),
            (&["deepen -1"][..], V0Error::BadDeepen("deepen -1".into())),
            (
                &["deepen 1", "deepen 2"][..],
                V0Error::BadDeepen("deepen 2".into()),
            ),
            (
                &["deepen-since x"][..],
                V0Error::BadDeepen("deepen-since x".into()),
            ),
        ] {
            let mut ls = vec![w.as_str()];
            ls.extend_from_slice(extra);
            ls.push("0000");
            assert_eq!(header(&ls).unwrap_err(), err, "{extra:?}");
        }
        // A shallow client that does not deepen: shallow, but no section.
        let r = header(&[&w, &format!("shallow {}", oid(2)), "0000"]).unwrap();
        assert!(r.args().request.is_shallow() && !r.needs_shallow_info());
    }

    #[test]
    fn malformed_headers_are_refused() {
        let want = format!("want {}", oid(1));
        let cases: Vec<(Vec<String>, V0Error)> = vec![
            (
                vec!["have x".into(), "0000".into()],
                V0Error::UnexpectedLine("have".into()),
            ),
            (
                vec!["want 123".into(), "0000".into()],
                V0Error::BadObjectId("want"),
            ),
            (
                vec![format!("want {}", "A".repeat(40)), "0000".into()],
                V0Error::BadObjectId("want"),
            ),
            (
                vec![want.clone(), "shallow nope".into(), "0000".into()],
                V0Error::BadObjectId("shallow"),
            ),
            (
                vec![want.clone(), "done".into(), "0000".into()],
                V0Error::UnexpectedLine("done".into()),
            ),
            (
                vec![format!("shallow {}", oid(2)), "0000".into()],
                V0Error::NoWants,
            ),
        ];
        for (ls, err) in cases {
            let ls: Vec<&str> = ls.iter().map(String::as_str).collect();
            assert_eq!(header(&ls).unwrap_err(), err, "{ls:?}");
        }
        let mut p = RequestParser::new(Transport::Stateless, V0Limits::default());
        assert_eq!(
            p.packet(Packet::Delim).unwrap_err(),
            V0Error::UnexpectedPacket("delimiter")
        );
        let mut p = RequestParser::new(Transport::Stateless, V0Limits::default());
        assert_eq!(
            p.packet(Packet::Data(b"want \xff\n")).unwrap_err(),
            V0Error::NotUtf8
        );
        // A bare flush is a client that is done (`git ls-remote`).
        assert!(matches!(
            header_with(Transport::Stateful, V0Limits::default(), &["0000"]),
            Ok(Header::End)
        ));
        // A filter line is parsed so the service refuses it clearly (S5).
        let r = header(&[&want, "filter blob:none", "0000"]).unwrap();
        assert_eq!(
            r.args()
                .request
                .plan_mode(aspen_core::repo::ShallowPolicy::Serve),
            Err(FetchError::filter())
        );
    }

    /// Feeds a whole header plus negotiation, one packet at a time, and
    /// returns the index of the packet that failed and its error.
    fn first_error(limits: V0Limits, ls: &[&str]) -> Option<(usize, V0Error)> {
        let wire = request(ls);
        let pks = packets(&wire);
        let mut p = RequestParser::new(Transport::Stateful, limits);
        let mut i = 0;
        let mut neg = loop {
            match p.packet(pks[i]) {
                Err(e) => return Some((i, e)),
                Ok(Some(Header::Fetch(r))) => break r.start(&mut Vec::new(), None).unwrap(),
                Ok(Some(Header::End)) => return None,
                Ok(None) => {}
            }
            i += 1;
        };
        for (j, pk) in pks.iter().enumerate().skip(i + 1) {
            neg = match neg.packet(*pk) {
                Err(e) => return Some((j, e)),
                Ok(Negotiating::More(n)) => n,
                Ok(Negotiating::Round(r)) => {
                    let done = r.is_done();
                    match r.answer(&mut Vec::new(), &[], false).unwrap() {
                        Answered::More(n) if !done => n,
                        _ => return None,
                    }
                }
            };
        }
        None
    }

    /// Invariant: every limit fails on the packet that crosses it, so the
    /// parser never holds more than the limits allow; on a connection the
    /// have count and byte budget span every round.
    #[test]
    fn limits_bound_the_request() {
        let limits = V0Limits {
            max_request_bytes: usize::MAX,
            max_wants: 1,
            max_haves: 2,
            max_shallow: 1,
            max_rounds: 2,
        };
        let w = format!("want {}", oid(1));
        let h = format!("have {}", oid(2));
        let s = format!("shallow {}", oid(3));
        let cases: [(&[&str], usize, &str); 6] = [
            (&[&w, &w, "0000"], 1, "wants"),
            (&[&w, &s, &s, "0000"], 2, "shallow lines"),
            (&[&w, &s, "deepen-not x", "0000"], 2, "shallow lines"),
            // Two haves in round one, the third (round two) crosses.
            (&[&w, "0000", &h, &h, "0000", &h, "0000"], 5, "haves"),
            // Bare flushes cost no haves or bytes; the header's flush is
            // not a round, two rounds are allowed, the third flush crosses.
            (
                &[&w, "0000", "0000", "0000", "0000"],
                4,
                "negotiation rounds",
            ),
            (
                &[&w, "0000", &h, "0000", "0000", "0000"],
                5,
                "negotiation rounds",
            ),
        ];
        for (ls, at, what) in cases {
            assert_eq!(
                first_error(limits, ls),
                Some((at, V0Error::LimitExceeded(what))),
                "{ls:?}"
            );
        }
        assert_eq!(first_error(limits, &[&w, "0000", &h, &h, "done"]), None);
        // `done` ends negotiation rather than opening another round.
        assert_eq!(
            first_error(limits, &[&w, "0000", "0000", "0000", "done"]),
            None
        );

        let bytes = V0Limits {
            max_request_bytes: w.len() + 1 + h.len() + 1,
            ..V0Limits::default()
        };
        assert_eq!(first_error(bytes, &[&w, "0000", &h, "0000", "0000"]), None);
        assert_eq!(
            first_error(bytes, &[&w, "0000", &h, "done"]),
            Some((3, V0Error::LimitExceeded("request size"))),
            "the byte budget spans header and negotiation"
        );
        assert_eq!(V0Limits::default().max_request_bytes, 8 * 1024 * 1024);
        assert_eq!(V0Limits::default().max_rounds, 1024);

        // Limits configured for v2 apply to v0 alike.
        let v2 = crate::v2::V2Limits {
            max_request_bytes: 11,
            max_wants: 12,
            max_haves: 13,
            max_shallow: 14,
            ..Default::default()
        };
        assert_eq!(
            V0Limits::from(v2),
            V0Limits {
                max_request_bytes: 11,
                max_wants: 12,
                max_haves: 13,
                max_shallow: 14,
                max_rounds: V0Limits::DEFAULT_MAX_ROUNDS,
            }
        );

        let long = "x".repeat(MAX_VALUE_LEN + 1);
        assert_eq!(
            header(&[&format!("{w} agent={long}"), "0000"]).unwrap_err(),
            V0Error::ValueTooLong {
                what: "agent",
                max: MAX_VALUE_LEN
            }
        );
        assert_eq!(
            header(&[&w, &format!("filter {long}"), "0000"]).unwrap_err(),
            V0Error::ValueTooLong {
                what: "filter",
                max: MAX_VALUE_LEN
            }
        );
        let long_ref = "r".repeat(MAX_REF_ARG_LEN + 1);
        assert_eq!(
            header(&[&w, &format!("deepen-not {long_ref}"), "0000"]).unwrap_err(),
            V0Error::ValueTooLong {
                what: "deepen-not",
                max: MAX_REF_ARG_LEN
            }
        );
    }

    /// Drives one request to its end: `common(round_haves)` picks the
    /// shared haves and `ready` is passed as given. Returns the response
    /// bytes before the pack and how the request ended.
    fn run(
        t: Transport,
        ls: &[&str],
        shallow: Option<&ShallowInfo>,
        common: &dyn Fn(&[ObjectId]) -> Vec<ObjectId>,
        ready: bool,
    ) -> (Vec<String>, Ending) {
        let wire = request(ls);
        let pks = packets(&wire);
        let mut out = Vec::new();
        let mut p = RequestParser::new(t, V0Limits::default());
        let mut it = pks.into_iter();
        let req = loop {
            match p.packet(it.next().expect("header")).unwrap() {
                Some(Header::Fetch(r)) => break r,
                Some(Header::End) => return (lines(&out), Ending::End),
                None => {}
            }
        };
        let mut neg = req.start(&mut out, shallow).unwrap();
        let mut rounds = 0;
        for pk in it {
            neg = match neg.packet(pk).unwrap() {
                Negotiating::More(n) => n,
                Negotiating::Round(r) => {
                    rounds += 1;
                    let c = common(r.haves());
                    match r.answer(&mut out, &c, ready).unwrap() {
                        Answered::More(n) => n,
                        Answered::ResponseEnd => return (lines(&out), Ending::ResponseEnd),
                        Answered::Pack { args, .. } => {
                            return (lines(&out), Ending::Pack(args.request.haves, rounds));
                        }
                    }
                }
            };
        }
        panic!("request did not end")
    }

    #[derive(Debug, PartialEq, Eq)]
    enum Ending {
        End,
        ResponseEnd,
        /// The final haves (the commons) and how many rounds it took.
        Pack(Vec<ObjectId>, usize),
    }

    fn known(set: &[ObjectId]) -> impl Fn(&[ObjectId]) -> Vec<ObjectId> + '_ {
        move |haves: &[ObjectId]| haves.iter().copied().filter(|h| set.contains(h)).collect()
    }

    /// Invariant (`multi_ack_detailed`, stateless, as `git upload-pack
    /// --stateless-rpc`): each shared have is `ACK <oid> common`; a round
    /// without `ready` ends with `NAK` and the response ends; `ready` adds
    /// `ACK <last> ready` before the `NAK`; with `no-done` the final
    /// `ACK <last>` and the pack follow in the same response; without it
    /// the client's next request ends with `done`, answered by the final
    /// ACK.
    #[test]
    fn multi_ack_detailed_stateless_rounds() {
        let want = format!("want {} {GIT_CAPS}", oid(1));
        let want_no_done = format!("want {} multi_ack_detailed side-band-64k ofs-delta", oid(1));
        let (h2, h3, h4) = (
            format!("have {}", oid(2)),
            format!("have {}", oid(3)),
            format!("have {}", oid(4)),
        );
        let shared = [oid(3), oid(4)];

        // Round one: nothing shared.
        let (out, end) = run(
            Transport::Stateless,
            &[&want, "0000", &h2, "0000"],
            None,
            &known(&shared),
            false,
        );
        assert_eq!(out, ["NAK"]);
        assert_eq!(end, Ending::ResponseEnd);

        // Shared haves, not ready yet.
        let (out, end) = run(
            Transport::Stateless,
            &[&want, "0000", &h2, &h3, &h4, "0000"],
            None,
            &known(&shared),
            false,
        );
        assert_eq!(
            out,
            [
                format!("ACK {} common", oid(3)),
                format!("ACK {} common", oid(4)),
                "NAK".into()
            ]
        );
        assert_eq!(end, Ending::ResponseEnd);

        // Ready with no-done: the pack follows directly.
        let (out, end) = run(
            Transport::Stateless,
            &[&want, "0000", &h2, &h3, &h4, "0000"],
            None,
            &known(&shared),
            true,
        );
        assert_eq!(
            out,
            [
                format!("ACK {} common", oid(3)),
                format!("ACK {} common", oid(4)),
                format!("ACK {} ready", oid(4)),
                "NAK".into(),
                format!("ACK {}", oid(4)),
            ]
        );
        assert_eq!(end, Ending::Pack(vec![oid(3), oid(4)], 1));

        // Ready without no-done: the response ends, the client says done.
        let (out, end) = run(
            Transport::Stateless,
            &[&want_no_done, "0000", &h3, "0000"],
            None,
            &known(&shared),
            true,
        );
        assert_eq!(
            out,
            [
                format!("ACK {} common", oid(3)),
                format!("ACK {} ready", oid(3)),
                "NAK".into()
            ]
        );
        assert_eq!(end, Ending::ResponseEnd);
        // The next request replays the common have and ends with done.
        let (out, end) = run(
            Transport::Stateless,
            &[&want_no_done, "0000", &h3, "done"],
            None,
            &known(&shared),
            false,
        );
        assert_eq!(
            out,
            [format!("ACK {} common", oid(3)), format!("ACK {}", oid(3))]
        );
        assert_eq!(end, Ending::Pack(vec![oid(3)], 1));

        // A clone: no haves, just done. NAK, then the pack.
        let (out, end) = run(
            Transport::Stateless,
            &[&want, "0000", "done"],
            None,
            &known(&shared),
            false,
        );
        assert_eq!(out, ["NAK"]);
        assert_eq!(end, Ending::Pack(vec![], 1));
    }

    /// Invariant (no `multi_ack`, go-git): only the first shared have is
    /// acknowledged, with a bare `ACK <oid>`; a flush gets `NAK` only while
    /// nothing is shared; after `done` the server adds `NAK` only if
    /// nothing was shared, and `ready` changes nothing.
    #[test]
    fn single_ack_mode() {
        let want = format!("want {} ofs-delta side-band-64k agent=go-git/5", oid(1));
        let (h2, h3, h4) = (
            format!("have {}", oid(2)),
            format!("have {}", oid(3)),
            format!("have {}", oid(4)),
        );
        let shared = [oid(3), oid(4)];
        // go-git sends every have and done in one request.
        let (out, end) = run(
            Transport::Stateless,
            &[&want, "0000", &h2, &h3, &h4, "done"],
            None,
            &known(&shared),
            true,
        );
        assert_eq!(out, [format!("ACK {}", oid(3))]);
        assert_eq!(end, Ending::Pack(vec![oid(3), oid(4)], 1));
        let (out, _) = run(
            Transport::Stateless,
            &[&want, "0000", &h2, "done"],
            None,
            &known(&shared),
            false,
        );
        assert_eq!(out, ["NAK"]);
        // Rounds on a connection: NAK until something is shared, then
        // silence; the client decides when to send done.
        let (out, end) = run(
            Transport::Stateful,
            &[
                &want, "0000", &h2, "0000", &h3, &h4, "0000", &h2, "0000", "done",
            ],
            None,
            &known(&shared),
            true,
        );
        assert_eq!(out, ["NAK".to_owned(), format!("ACK {}", oid(3))]);
        assert_eq!(end, Ending::Pack(vec![oid(3), oid(4)], 4));
    }

    /// Invariant: on a connection, rounds continue on the same exchange,
    /// commons accumulate across rounds (each once, in the order first
    /// acknowledged), and the final plan gets exactly those.
    #[test]
    fn stateful_rounds_accumulate_commons() {
        let want = format!("want {} multi_ack_detailed side-band-64k", oid(1));
        let h = |n| format!("have {}", oid(n));
        let shared = [oid(3), oid(5)];
        let (out, end) = run(
            Transport::Stateful,
            &[
                &want,
                "0000",
                &h(2),
                "0000",
                &h(3),
                &h(4),
                "0000",
                &h(5),
                &h(3),
                "done",
            ],
            None,
            &known(&shared),
            false,
        );
        assert_eq!(
            out,
            [
                "NAK".to_owned(),
                format!("ACK {} common", oid(3)),
                "NAK".into(),
                format!("ACK {} common", oid(5)),
                format!("ACK {} common", oid(3)),
                // The last common is the most recent, repeats included.
                format!("ACK {}", oid(3)),
            ]
        );
        assert_eq!(end, Ending::Pack(vec![oid(3), oid(5)], 3));
    }

    /// Invariant: a deepen request gets its shallow section first, in every
    /// stateless response (each starts from the header again), and the
    /// first stateless request of a deepen fetch (header, flush, flush) is
    /// answered with the section and a `NAK`; a request without `deepen`
    /// never gets one, even from a shallow client.
    #[test]
    fn shallow_section_exactly_for_deepen() {
        let want = format!("want {} {GIT_CAPS}", oid(1));
        let info = ShallowInfo {
            shallow: vec![oid(1)],
            unshallow: vec![oid(9)],
        };
        let nothing = |_: &[ObjectId]| Vec::new();
        let (out, end) = run(
            Transport::Stateless,
            &[&want, "deepen 1", "0000", "0000"],
            Some(&info),
            &nothing,
            false,
        );
        assert_eq!(
            out,
            [
                format!("shallow {}", oid(1)),
                format!("unshallow {}", oid(9)),
                "0000".into(),
                "NAK".into()
            ]
        );
        assert_eq!(end, Ending::ResponseEnd);
        let (out, end) = run(
            Transport::Stateless,
            &[&want, "deepen 1", "0000", "done"],
            Some(&ShallowInfo::default()),
            &nothing,
            false,
        );
        assert_eq!(out, ["0000", "NAK"], "an empty section is still a flush");
        assert_eq!(end, Ending::Pack(vec![], 1));

        // git's first stateless request of a deepen fetch is the header
        // alone: the shallow section completes the response.
        let parse = |ls: &[&str]| header(ls).unwrap();
        let mut out = Vec::new();
        let n = parse(&[&want, "deepen 1", "0000"])
            .start(&mut out, Some(&info))
            .unwrap();
        assert_eq!(n.end_of_input(), Ok(()));
        assert_eq!(
            out,
            format!("0034shallow {}0036unshallow {}0000", oid(1), oid(9)).as_bytes(),
            "no LF after the ID, as git writes it (libgit2 needs the exact length)"
        );
        // Anywhere else, the end of input is a truncated request.
        let n = parse(&[&want, "0000"])
            .start(&mut Vec::new(), None)
            .unwrap();
        let have = format!("have {}\n", oid(1));
        let Negotiating::More(n) = n.packet(Packet::Data(have.as_bytes())).unwrap() else {
            panic!()
        };
        assert_eq!(n.end_of_input(), Err(V0Error::Truncated));
        let wire = request(&[&want, "0000"]);
        let mut p = RequestParser::new(Transport::Stateful, V0Limits::default());
        let pks = packets(&wire);
        p.packet(pks[0]).unwrap();
        let Some(Header::Fetch(r)) = p.packet(pks[1]).unwrap() else {
            panic!()
        };
        assert_eq!(
            r.start(&mut Vec::new(), None).unwrap().end_of_input(),
            Err(V0Error::Truncated),
            "a stateful connection must not end before done"
        );
        let deepen = parse(&[&want, "deepen 1", "0000"]);
        assert_eq!(
            deepen.start(&mut Vec::new(), None).unwrap_err(),
            V0ResponseError::ShallowInfoRequired
        );
        let shallow_only = parse(&[&want, &format!("shallow {}", oid(5)), "0000"]);
        let mut out = Vec::new();
        assert_eq!(
            shallow_only.start(&mut out, Some(&info)).unwrap_err(),
            V0ResponseError::ShallowInfoNotRequested
        );
        assert!(out.is_empty());
    }

    /// Invariant: an answer that would mislead the client is refused and
    /// writes nothing: acknowledging an object that was not a have of the
    /// round, or `ready` with nothing in common.
    #[test]
    fn bad_answers_are_refused() {
        let wire = request(&[
            &format!("want {} multi_ack_detailed no-done", oid(1)),
            "0000",
            &format!("have {}", oid(2)),
            "0000",
        ]);
        let round = || {
            let pks = packets(&wire);
            let mut p = RequestParser::new(Transport::Stateless, V0Limits::default());
            let (Ok(None), Ok(Some(Header::Fetch(r)))) = (p.packet(pks[0]), p.packet(pks[1]))
            else {
                panic!()
            };
            let n = r.start(&mut Vec::new(), None).unwrap();
            let Negotiating::More(n) = n.packet(pks[2]).unwrap() else {
                panic!()
            };
            let Negotiating::Round(r) = n.packet(pks[3]).unwrap() else {
                panic!()
            };
            r
        };
        let mut out = Vec::new();
        assert_eq!(
            round().answer(&mut out, &[oid(7)], false).unwrap_err(),
            V0ResponseError::NotAHave(oid(7))
        );
        assert_eq!(
            round().answer(&mut out, &[], true).unwrap_err(),
            V0ResponseError::ReadyWithoutCommon
        );
        assert!(out.is_empty());
        assert_eq!(round().haves(), [oid(2)]);
        assert!(!round().is_done());
        // An error mid-negotiation is an ERR line.
        round()
            .error(&mut out, &FetchError::filter().to_wire())
            .unwrap();
        assert!(lines(&out)[0].starts_with("ERR "));
        let push = RejectReason::NonFastForward.to_wire();
        assert!(matches!(
            round().error(&mut Vec::new(), &push),
            Err(V0ResponseError::WrongSurface(_))
        ));
    }

    fn pack_response(caps: &str) -> PackResponse {
        let wire = request(&[&format!("want {} {caps}", oid(1)), "0000", "done"]);
        let pks = packets(&wire);
        let mut p = RequestParser::new(Transport::Stateless, V0Limits::default());
        p.packet(pks[0]).unwrap();
        let Some(Header::Fetch(r)) = p.packet(pks[1]).unwrap() else {
            panic!()
        };
        let Negotiating::Round(r) = r
            .start(&mut Vec::new(), None)
            .unwrap()
            .packet(pks[2])
            .unwrap()
        else {
            panic!()
        };
        let Answered::Pack { response, .. } = r.answer(&mut Vec::new(), &[], false).unwrap() else {
            panic!()
        };
        response
    }

    /// Invariant: with side-band the pack is band 1 in packets of at most
    /// 65520 bytes, progress band 2 (dropped under `no-progress`), an
    /// error band 3, and a flush ends the response; without side-band the
    /// bytes are raw and an error is reported as not shown. Nothing can be
    /// written after the end.
    #[test]
    fn pack_response_framing() {
        let data = noise(3, 70_000);
        let mut r = pack_response("side-band-64k");
        assert!(r.has_sideband());
        let mut out = Vec::new();
        r.progress(&mut out, "counting\nobjects").unwrap();
        r.pack_data(&mut out, &data).unwrap();
        r.finish(&mut out).unwrap();
        let mut at = 0;
        let mut bands = Vec::new();
        let mut joined = Vec::new();
        while let Some((p, n)) = decode(&out[at..]).unwrap() {
            at += n;
            assert!(n <= pkt::MAX_PKT_LEN);
            if let Packet::Data(d) = p {
                let (band, rest) = sideband::demux(d).unwrap();
                bands.push(band);
                if band == Band::Data {
                    joined.extend_from_slice(rest);
                } else {
                    assert_eq!(rest, b"counting objects\n");
                }
            } else {
                assert_eq!(p, Packet::Flush);
                assert_eq!(at, out.len(), "the flush is last");
            }
        }
        assert_eq!(bands, [Band::Progress, Band::Data, Band::Data]);
        assert_eq!(joined, data);
        assert_eq!(
            r.pack_data(&mut out, b"x").unwrap_err(),
            V0ResponseError::Finished
        );

        let mut r = pack_response("side-band-64k no-progress");
        let mut out = Vec::new();
        r.progress(&mut out, "hidden").unwrap();
        assert!(out.is_empty());
        let e = FetchError::filter().to_wire();
        assert!(r.error(&mut out, &e).unwrap());
        let l = lines(&out);
        assert_eq!(l.len(), 2);
        assert!(l[0].starts_with('\u{3}') && l[1] == "0000");

        let mut r = pack_response("ofs-delta");
        assert!(!r.has_sideband());
        let mut out = Vec::new();
        r.progress(&mut out, "dropped").unwrap();
        r.pack_data(&mut out, b"PACK").unwrap();
        r.finish(&mut out).unwrap();
        assert_eq!(out, b"PACK", "raw pack, no flush");
        let mut r = pack_response("ofs-delta");
        assert!(!r.error(&mut Vec::new(), &e).unwrap());
        let push = RejectReason::NonFastForward.to_wire();
        assert!(matches!(
            pack_response("side-band-64k").error(&mut Vec::new(), &push),
            Err(V0ResponseError::WrongSurface(_))
        ));
    }

    /// Invariant: the header parser and the negotiation are total: no
    /// packet sequence panics, and every error is a value. The sequences
    /// start plausible (a want with capabilities) and drift into noise, so
    /// they reach negotiation as well as the header's error paths.
    #[test]
    fn parsers_are_total_on_arbitrary_packets() {
        let caps = [
            "",
            " multi_ack_detailed no-done side-band-64k",
            " side-band",
            " shallow agent=x",
        ];
        let (mut headers, mut rounds) = (0, 0);
        for seed in 0..3_000u64 {
            let bytes = noise(seed, 96);
            let mut lines: Vec<Option<Vec<u8>>> = Vec::new();
            for (i, c) in bytes.chunks(6).enumerate() {
                let id = oid(c[1] % 4).to_hex();
                let line = match (i, c[0] % 16) {
                    (0, 0..=12) => format!("want {id}{}", caps[usize::from(c[2]) % 4]),
                    (_, 0..=2) => {
                        lines.push(None);
                        continue;
                    }
                    (_, 3..=6) => format!("have {id}"),
                    (_, 7) => "done".into(),
                    (_, 8) => format!("shallow {id}"),
                    (_, 9) => format!("deepen {}", c[2]),
                    (_, 10) => format!("deepen-not {}", c[2]),
                    (_, 11) => format!("want {id}"),
                    (_, 12) => String::from_utf8_lossy(&c[1..]).into_owned(),
                    (_, 13) => format!("have {}", &id[..c[2] as usize % 41]),
                    _ => format!("deepen-since {}", c[2]),
                };
                lines.push(Some(format!("{line}\n").into_bytes()));
            }
            let mut p = RequestParser::new(Transport::Stateful, V0Limits::default());
            let mut neg: Option<Negotiation> = None;
            for l in &lines {
                let pk = l.as_deref().map_or(Packet::Flush, Packet::Data);
                if let Some(n) = neg.take() {
                    match n.packet(pk) {
                        Ok(Negotiating::More(n)) => neg = Some(n),
                        Ok(Negotiating::Round(r)) => {
                            rounds += 1;
                            let c: Vec<_> = r.haves().iter().copied().step_by(2).collect();
                            match r.answer(&mut Vec::new(), &c, true) {
                                Ok(Answered::More(n)) => neg = Some(n),
                                Ok(_) | Err(V0ResponseError::ReadyWithoutCommon) => break,
                                Err(e) => panic!("{e}"),
                            }
                        }
                        Err(_) => break,
                    }
                    continue;
                }
                match p.packet(pk) {
                    Ok(Some(Header::Fetch(r))) => {
                        headers += 1;
                        let info = r.needs_shallow_info().then(ShallowInfo::default);
                        neg = Some(r.start(&mut Vec::new(), info.as_ref()).unwrap());
                    }
                    Ok(Some(Header::End)) | Err(_) => break,
                    Ok(None) => {}
                }
            }
        }
        // The sweep must reach negotiation, not stop at the first line.
        assert!(
            headers > 500 && rounds > 200,
            "{headers} headers, {rounds} rounds"
        );
    }
}
