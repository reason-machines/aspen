//! Protocol v2 `git-upload-pack`: the capability advertisement, command
//! requests, `ls-refs` and `fetch` (`gitprotocol-v2(5)`).
//!
//! A v2 exchange is a capability advertisement ([`V2Advertisement`]),
//! then one or more command requests: `command=<name>`, capability lines,
//! a delimiter, arguments, a flush. Over smart HTTP each POST carries one
//! command; over SSH a connection carries several and ends with a bare
//! flush or end of stream. [`CommandParser`] turns the packets of one
//! request into a [`Command`].
//!
//! Responses:
//! - `ls-refs`: [`write_ls_refs`], one line per ref, then a flush.
//! - `fetch`: [`FetchResponse`], a writer that enforces the section order
//!   and the rules spikes S3 and S5 found (an `acknowledgments` section
//!   before anything else unless the client sent `done`, `shallow-info`
//!   exactly when the request is shallow, URIs only in schemes the client
//!   listed, a `packfile` section always present when URIs are sent).

use std::num::NonZeroU32;

use aspen_core::error::{Surface, WireError};
use aspen_core::fetch::{Deepen, FetchRequest, PackfileUri, ShallowInfo};
use aspen_core::id::ObjectId;
use aspen_core::name::{MAX_REF_NAME_LEN, RefName};

use crate::caps::{Agent, Capability, OBJECT_FORMAT};
use crate::pkt::{self, Packet};
use crate::sideband::{self, Band, SidebandMode};

/// What Aspen advertises in protocol v2.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V2Advertisement {
    /// The `agent=` value.
    pub agent: Agent,
    /// Advertise the `packfile-uris` fetch feature (S3). On when the
    /// deployment can hand out pack URLs.
    pub packfile_uris: bool,
    /// Advertise the `no-ref-delta` fetch feature. Only when every pack a
    /// fetch may be planned onto is REF_DELTA-free
    /// ([`aspen_core::fetch::no_ref_delta_safe`], S3).
    pub no_ref_delta: bool,
}

impl V2Advertisement {
    /// Aspen's default v2 advertisement: no packfile URIs.
    pub fn new(agent: Agent) -> Self {
        Self {
            agent,
            packfile_uris: false,
            no_ref_delta: false,
        }
    }

    /// The `fetch=` feature list. `shallow` is always advertised (S5:
    /// without it, `actions/checkout`'s depth-1 fetch fails); `filter` never
    /// is (S5: clients then fall back to a full fetch). `wait-for-done` is
    /// always advertised: `git push` with `push.negotiate` runs a
    /// negotiation-only fetch (`git fetch --negotiate-only`) that Git gives
    /// up on without it, and the push then resends the branch's history.
    pub fn fetch_features(&self) -> String {
        let mut f = String::from("shallow wait-for-done");
        if self.packfile_uris {
            f.push_str(" packfile-uris");
        }
        if self.no_ref_delta {
            f.push_str(" no-ref-delta");
        }
        f
    }

    /// Appends the advertisement: `version 2`, the capabilities, a flush.
    /// Over smart HTTP no `# service=` line precedes it
    /// ([`crate::service::write_info_refs_header`]).
    pub fn write(&self, out: &mut Vec<u8>) {
        let lines = [
            "version 2".to_owned(),
            format!("agent={}", self.agent.as_str()),
            "ls-refs=unborn".to_owned(),
            format!("fetch={}", self.fetch_features()),
            "server-option".to_owned(),
            format!("object-format={OBJECT_FORMAT}"),
        ];
        for l in lines {
            pkt::write_text(out, &l).expect("advertisement lines are one short line each");
        }
        pkt::write_flush(out);
    }
}

/// Longest `ref-prefix` or `deepen-not` value: the longest ref name Aspen
/// stores, since a longer prefix matches nothing and a longer rev names
/// nothing.
pub const MAX_REF_ARG_LEN: usize = MAX_REF_NAME_LEN;

/// Longest free-form value: `agent=`, `server-option=`, `filter` and
/// `packfile-uris`.
pub const MAX_VALUE_LEN: usize = 4096;

/// Limits on one command request. Each is checked on the packet that
/// crosses it, so the error arrives before the line is stored and nothing
/// is buffered for a later check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct V2Limits {
    /// Most bytes over every pkt-line payload of the request (capability
    /// and argument lines). This bounds what the parser holds; the count
    /// limits below bound the number of entries.
    pub max_request_bytes: usize,
    /// Most `want` lines.
    pub max_wants: usize,
    /// Most `have` lines.
    pub max_haves: usize,
    /// Most `shallow` and `deepen-not` lines together.
    pub max_shallow: usize,
    /// Most `ref-prefix` lines. Git clears the prefixes and lists every ref
    /// above 65,536; Aspen refuses instead, because the whole point of
    /// `ref-prefix` is that a large repository never sends its full list.
    /// The default, 1,024, is far below Git's: a client sends one line per
    /// refspec (six for a ref named without `refs/`, from Git's
    /// `ref_rev_parse_rules`), and every line is a range the store reads
    /// on each page of the listing (#119).
    pub max_ref_prefixes: usize,
    /// Most `server-option` lines.
    pub max_server_options: usize,
}

impl Default for V2Limits {
    /// 8 MiB in all (100,000 `have` lines are about 4.6 MB); 100,000
    /// wants, haves and shallow lines; 1,024 prefixes; 256 server options.
    fn default() -> Self {
        Self {
            max_request_bytes: 8 * 1024 * 1024,
            max_wants: 100_000,
            max_haves: 100_000,
            max_shallow: 100_000,
            max_ref_prefixes: 1024,
            max_server_options: 256,
        }
    }
}

/// An `ls-refs` request.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LsRefsRequest {
    /// `symrefs`: show `symref-target:` attributes.
    pub symrefs: bool,
    /// `peel`: show `peeled:` attributes for annotated tags.
    pub peel: bool,
    /// `unborn`: show an unborn `HEAD` (an empty repository's default
    /// branch).
    pub unborn: bool,
    /// `ref-prefix` values; empty means every ref. The store answers these
    /// with prefix scans, so a client never gets the whole list unless it
    /// asks for it.
    pub prefixes: Vec<String>,
    /// The client's `agent=` capability.
    pub agent: Option<String>,
    /// `server-option=` capability values, in order.
    pub server_options: Vec<String>,
}

impl LsRefsRequest {
    /// Whether `name` (a full ref name, or `HEAD`) matches the request's
    /// prefixes.
    pub fn matches(&self, name: &str) -> bool {
        self.prefixes.is_empty() || self.prefixes.iter().any(|p| name.starts_with(p.as_str()))
    }
}

/// A `fetch` request: the fields the planner reads (an
/// [`aspen_core::fetch::FetchRequest`]) plus those only the response needs.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct FetchArgs {
    /// Wants, haves, shallow and deepen, done, filter, packfile URIs and
    /// agent.
    pub request: FetchRequest,
    /// `no-progress`: send no band-2 progress.
    pub no_progress: bool,
    /// `include-tag`: also send annotated tags that point into the pack.
    pub include_tag: bool,
    /// `server-option=` capability values, in order.
    pub server_options: Vec<String>,
    /// `wait-for-done`: answer every round without `done` with
    /// acknowledgments only, never `ready`. `git fetch --negotiate-only`
    /// (run by `git push` with `push.negotiate`) sends it with haves and no
    /// wants, and reads the `ACK`s as the commits the push may leave out.
    pub wait_for_done: bool,
}

/// One v2 command request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    /// `command=ls-refs`.
    LsRefs(LsRefsRequest),
    /// `command=fetch`.
    Fetch(FetchArgs),
    /// A flush with no command: the client is done with the connection.
    End,
}

/// Why a v2 request is malformed. Answer with an `ERR` pkt-line
/// ([`pkt::write_err`]) and end the exchange.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum V2Error {
    /// A packet where none belongs.
    #[error("protocol error: unexpected {0} packet")]
    UnexpectedPacket(&'static str),
    /// A line that is not UTF-8.
    #[error("protocol error: line is not valid UTF-8")]
    NotUtf8,
    /// No `command=` line, or two of them.
    #[error("protocol error: expected exactly one command= line")]
    NoCommand,
    /// A command Aspen does not serve.
    #[error("unknown command {0:?}")]
    UnknownCommand(String),
    /// A capability line Aspen did not advertise.
    #[error("unknown capability {0:?}")]
    UnknownCapability(String),
    /// An object format other than SHA-1.
    #[error("object format {0} is not supported; this server serves sha1 repositories")]
    UnsupportedObjectFormat(String),
    /// An argument the command does not take, or one whose feature was not
    /// advertised.
    #[error("unexpected argument {0:?}")]
    UnexpectedArgument(String),
    /// A `want`, `have` or `shallow` with a malformed object ID.
    #[error("malformed object id in {0:?}")]
    BadObjectId(String),
    /// A bad `deepen` or `deepen-since` value.
    #[error("invalid {0:?}")]
    BadDeepen(String),
    /// `deepen` together with `deepen-since` or `deepen-not`, which Git
    /// forbids too.
    #[error("deepen and deepen-since (or deepen-not) cannot be used together")]
    DeepenConflict,
    /// A limit in [`V2Limits`] was exceeded.
    #[error("request exceeds the server's limit on {0}")]
    LimitExceeded(&'static str),
    /// A value longer than [`MAX_REF_ARG_LEN`] or [`MAX_VALUE_LEN`]. The
    /// value is not echoed.
    #[error("{what} value is longer than the server's limit of {max} bytes")]
    ValueTooLong {
        /// The capability or argument.
        what: &'static str,
        /// Its limit.
        max: usize,
    },
    /// A packet after the request was complete.
    #[error("protocol error: packet after the end of the request")]
    AfterEnd,
}

fn check_len(what: &'static str, value: &str, max: usize) -> Result<(), V2Error> {
    if value.len() > max {
        return Err(V2Error::ValueTooLong { what, max });
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Part {
    Capabilities,
    Arguments,
    Done,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Name {
    LsRefs,
    Fetch,
}

/// A `fetch` request being parsed: the arguments so far, plus the deepen
/// parts, which are checked against each other at the end.
#[derive(Debug, Default)]
struct FetchState {
    args: FetchArgs,
    depth: Option<NonZeroU32>,
    since: Option<i64>,
    not: Vec<String>,
}

/// Incremental parser for one v2 command request. Feed it packets with
/// [`CommandParser::packet`] until it returns the command; make a new
/// parser for the next request on the same connection. Every argument is
/// classified, checked and counted as it arrives, so no limit waits for
/// the final flush.
#[derive(Debug)]
pub struct CommandParser {
    advert: V2Advertisement,
    limits: V2Limits,
    part: Part,
    command: Option<Name>,
    agent: Option<String>,
    server_options: Vec<String>,
    ls_refs: LsRefsRequest,
    fetch: FetchState,
    started: bool,
    bytes: usize,
}

impl CommandParser {
    /// A parser for a client that saw `advert`.
    pub fn new(advert: V2Advertisement, limits: V2Limits) -> Self {
        Self {
            advert,
            limits,
            part: Part::Capabilities,
            command: None,
            agent: None,
            server_options: Vec::new(),
            ls_refs: LsRefsRequest::default(),
            fetch: FetchState::default(),
            started: false,
            bytes: 0,
        }
    }

    /// Consumes one packet; returns the command after its final flush.
    pub fn packet(&mut self, p: Packet<'_>) -> Result<Option<Command>, V2Error> {
        if let (Part::Capabilities | Part::Arguments, Packet::Data(d)) = (self.part, p) {
            self.bytes += d.len();
            if self.bytes > self.limits.max_request_bytes {
                return Err(V2Error::LimitExceeded("request size"));
            }
        }
        match (self.part, p) {
            (Part::Done, _) => Err(V2Error::AfterEnd),
            (_, Packet::ResponseEnd) => Err(V2Error::UnexpectedPacket("response-end")),
            (Part::Capabilities, Packet::Flush) if !self.started => {
                self.part = Part::Done;
                Ok(Some(Command::End))
            }
            (Part::Capabilities, Packet::Delim) => {
                self.command.ok_or(V2Error::NoCommand)?;
                self.part = Part::Arguments;
                Ok(None)
            }
            (Part::Arguments, Packet::Delim) => Err(V2Error::UnexpectedPacket("delimiter")),
            (_, Packet::Flush) => {
                self.part = Part::Done;
                self.finish().map(Some)
            }
            (part, Packet::Data(_)) => {
                self.started = true;
                let line = p.text().expect("data packet");
                let line = std::str::from_utf8(line).map_err(|_| V2Error::NotUtf8)?;
                match (part, self.command) {
                    (Part::Capabilities, _) => self.capability(line)?,
                    (_, Some(Name::LsRefs)) => self.ls_refs_argument(line)?,
                    (_, Some(Name::Fetch)) => self.fetch_argument(line)?,
                    (_, None) => unreachable!("arguments start only after a command"),
                }
                Ok(None)
            }
        }
    }

    fn capability(&mut self, line: &str) -> Result<(), V2Error> {
        let c = Capability::parse(line);
        match (c.name.as_str(), c.value) {
            ("command", Some(v)) => {
                if self.command.is_some() {
                    return Err(V2Error::NoCommand);
                }
                self.command = Some(match v.as_str() {
                    "ls-refs" => Name::LsRefs,
                    "fetch" => Name::Fetch,
                    _ => return Err(V2Error::UnknownCommand(v)),
                });
            }
            ("agent", v) => {
                check_len("agent", v.as_deref().unwrap_or_default(), MAX_VALUE_LEN)?;
                self.agent = v;
            }
            ("object-format", v) => {
                let v = v.unwrap_or_default();
                if v != OBJECT_FORMAT {
                    return Err(V2Error::UnsupportedObjectFormat(v));
                }
            }
            ("server-option", Some(v)) => {
                check_len("server-option", &v, MAX_VALUE_LEN)?;
                if self.server_options.len() >= self.limits.max_server_options {
                    return Err(V2Error::LimitExceeded("server options"));
                }
                self.server_options.push(v);
            }
            _ => return Err(V2Error::UnknownCapability(line.to_owned())),
        }
        Ok(())
    }

    fn ls_refs_argument(&mut self, a: &str) -> Result<(), V2Error> {
        let r = &mut self.ls_refs;
        match a {
            "symrefs" => r.symrefs = true,
            "peel" => r.peel = true,
            "unborn" => r.unborn = true,
            _ => match a.strip_prefix("ref-prefix ") {
                Some(p) => {
                    check_len("ref-prefix", p, MAX_REF_ARG_LEN)?;
                    if r.prefixes.len() >= self.limits.max_ref_prefixes {
                        return Err(V2Error::LimitExceeded("ref-prefix lines"));
                    }
                    r.prefixes.push(p.to_owned());
                }
                None => return Err(V2Error::UnexpectedArgument(a.to_owned())),
            },
        }
        Ok(())
    }

    fn fetch_argument(&mut self, a: &str) -> Result<(), V2Error> {
        let limits = &self.limits;
        let s = &mut self.fetch;
        let oid = |v: &str| ObjectId::parse_hex(v).map_err(|_| V2Error::BadObjectId(a.to_owned()));
        let (key, value) = match a.split_once(' ') {
            Some((k, v)) => (k, Some(v)),
            None => (a, None),
        };
        let r = &mut s.args.request;
        match (key, value) {
            ("want", Some(v)) => {
                if r.wants.len() >= limits.max_wants {
                    return Err(V2Error::LimitExceeded("wants"));
                }
                r.wants.push(oid(v)?);
            }
            ("have", Some(v)) => {
                if r.haves.len() >= limits.max_haves {
                    return Err(V2Error::LimitExceeded("haves"));
                }
                r.haves.push(oid(v)?);
            }
            ("shallow", Some(v)) => {
                if r.client_shallow.len() + s.not.len() >= limits.max_shallow {
                    return Err(V2Error::LimitExceeded("shallow lines"));
                }
                r.client_shallow.push(oid(v)?);
            }
            ("deepen", Some(v)) => {
                let d = v
                    .parse::<u32>()
                    .ok()
                    .and_then(NonZeroU32::new)
                    .ok_or_else(|| V2Error::BadDeepen(a.to_owned()))?;
                if s.depth.replace(d).is_some() {
                    return Err(V2Error::BadDeepen(a.to_owned()));
                }
            }
            ("deepen-since", Some(v)) => {
                let t = v
                    .parse::<i64>()
                    .map_err(|_| V2Error::BadDeepen(a.to_owned()))?;
                if s.since.replace(t).is_some() {
                    return Err(V2Error::BadDeepen(a.to_owned()));
                }
            }
            ("deepen-not", Some(v)) if !v.is_empty() => {
                check_len("deepen-not", v, MAX_REF_ARG_LEN)?;
                if r.client_shallow.len() + s.not.len() >= limits.max_shallow {
                    return Err(V2Error::LimitExceeded("shallow lines"));
                }
                s.not.push(v.to_owned());
            }
            ("deepen-relative", None) => r.deepen_relative = true,
            ("filter", Some(v)) => {
                check_len("filter", v, MAX_VALUE_LEN)?;
                r.filter = Some(v.to_owned());
            }
            ("done", None) => r.done = true,
            ("thin-pack", None) => r.thin_pack = true,
            ("ofs-delta", None) => r.ofs_delta = true,
            ("no-progress", None) => s.args.no_progress = true,
            ("wait-for-done", None) => s.args.wait_for_done = true,
            ("include-tag", None) => s.args.include_tag = true,
            ("packfile-uris", Some(v)) if self.advert.packfile_uris => {
                check_len("packfile-uris", v, MAX_VALUE_LEN)?;
                r.packfile_uri_protocols = v
                    .split(',')
                    .filter(|s| !s.is_empty())
                    .map(str::to_owned)
                    .collect();
            }
            _ => return Err(V2Error::UnexpectedArgument(a.to_owned())),
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<Command, V2Error> {
        let agent = self.agent.take();
        let server_options = std::mem::take(&mut self.server_options);
        match self.command.ok_or(V2Error::NoCommand)? {
            Name::LsRefs => Ok(Command::LsRefs(LsRefsRequest {
                agent,
                server_options,
                ..std::mem::take(&mut self.ls_refs)
            })),
            Name::Fetch => {
                let FetchState {
                    mut args,
                    depth,
                    since,
                    not,
                } = std::mem::take(&mut self.fetch);
                args.server_options = server_options;
                args.request.agent = agent;
                args.request.deepen = match (depth, since, not.is_empty()) {
                    (Some(_), Some(_), _) | (Some(_), _, false) => {
                        return Err(V2Error::DeepenConflict);
                    }
                    (Some(d), None, true) => Some(Deepen::Depth(d)),
                    (None, None, true) => None,
                    (None, since, _) => Some(Deepen::Boundary { since, not }),
                };
                Ok(Command::Fetch(args))
            }
        }
    }
}

/// The name in an `ls-refs` line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LsRefsName {
    /// `HEAD`, which Aspen points at the trunk branch.
    Head,
    /// A full ref name.
    Ref(RefName),
}

impl LsRefsName {
    /// The name as written on the wire.
    pub fn as_str(&self) -> &str {
        match self {
            Self::Head => "HEAD",
            Self::Ref(r) => r.as_str(),
        }
    }
}

/// One ref in an `ls-refs` response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LsRefsEntry {
    /// The name.
    pub name: LsRefsName,
    /// The tip, or `None` for an unborn `HEAD` (empty repository).
    pub tip: Option<ObjectId>,
    /// What a symbolic ref points at: for `HEAD`, the trunk branch.
    pub symref_target: Option<RefName>,
    /// For an annotated tag, the object it peels to.
    pub peeled: Option<ObjectId>,
}

/// Why an `ls-refs` response can't be written. Each is a bug in the caller.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LsRefsError {
    /// An unborn entry other than `HEAD`.
    #[error("only HEAD can be unborn, not {0}")]
    UnbornRef(String),
    /// A tip with the zero ID.
    #[error("{0} has the zero object ID")]
    ZeroTip(String),
}

/// Appends an `ls-refs` response: one line per entry that matches the
/// request, then a flush. Attributes appear only when requested
/// (`symrefs`, `peel`), and an unborn `HEAD` only when the client sent
/// `unborn`; entries outside the requested prefixes are skipped. Nothing
/// is written when an entry is invalid.
pub fn write_ls_refs(
    out: &mut Vec<u8>,
    req: &LsRefsRequest,
    entries: &[LsRefsEntry],
) -> Result<(), LsRefsError> {
    for e in entries {
        match (e.tip, &e.name) {
            (None, LsRefsName::Ref(r)) => return Err(LsRefsError::UnbornRef(r.to_string())),
            (Some(t), n) if t.is_zero() => return Err(LsRefsError::ZeroTip(n.as_str().to_owned())),
            _ => {}
        }
    }
    for e in entries {
        if !req.matches(e.name.as_str()) {
            continue;
        }
        let mut line = match e.tip {
            Some(tip) => format!("{tip} {}", e.name.as_str()),
            None if req.unborn => format!("unborn {}", e.name.as_str()),
            None => continue,
        };
        if let (true, Some(t)) = (req.symrefs, &e.symref_target) {
            line.push_str(" symref-target:");
            line.push_str(t.as_str());
        }
        if let (true, Some(p), Some(_)) = (req.peel, e.peeled, e.tip) {
            line.push_str(" peeled:");
            line.push_str(&p.to_hex());
        }
        pkt::write_text(out, &line).expect("ref names are valid and short, so the line fits");
    }
    pkt::write_flush(out);
    Ok(())
}

/// The `acknowledgments` section of a fetch response.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Acknowledgments {
    /// `ACK <oid>` for each have the server shares with the client; `NAK`
    /// when empty.
    pub common: Vec<ObjectId>,
    /// `ready`: the server will send the pack now. Without it the response
    /// ends here and the client sends another round of haves.
    pub ready: bool,
}

/// A fetch response written out of order, or against what the client
/// asked for. Each is a bug in the caller; nothing is written.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ResponseError {
    /// A section after a later one, or after the response ended.
    #[error("{0} section out of order")]
    OutOfOrder(&'static str),
    /// `acknowledgments` after the client sent `done`.
    #[error("the client sent done, so there is no acknowledgments section")]
    AcknowledgmentsAfterDone,
    /// Another section before `acknowledgments`, when the client did not
    /// send `done` (S5: git fails with
    /// "expected 'acknowledgments', received 'shallow-info'").
    #[error("acknowledgments must come first when the client did not send done")]
    AcknowledgmentsRequired,
    /// The pack without `shallow-info` for a shallow request.
    #[error("a shallow request needs a shallow-info section before the pack")]
    ShallowInfoRequired,
    /// `shallow-info` for a request that is not shallow.
    #[error("shallow-info for a request that is not shallow")]
    ShallowInfoNotRequested,
    /// `packfile-uris` to a client that did not ask for them.
    #[error("packfile-uris for a client that did not send packfile-uris")]
    UrisNotRequested,
    /// A URI whose scheme the client did not list. Clients don't filter
    /// (S3), so the server must.
    #[error("URI scheme of {0:?} is not one the client accepts")]
    SchemeNotAccepted(String),
    /// A [`WireError`] meant for another surface.
    #[error("error for surface {0:?} used in a fetch response")]
    WrongSurface(Surface),
    /// `ready` to a client that sent `wait-for-done`: it expects the
    /// response to end after the acknowledgments until it sends `done`.
    #[error("ready for a client that sent wait-for-done")]
    ReadyWhileWaitingForDone,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Stage {
    Start,
    Acked,
    Shallow,
    Uris,
    Packfile,
    Finished,
}

/// Writes one fetch response, section by section, refusing any order or
/// content the client would reject:
///
/// ```text
/// [acknowledgments (ACK|NAK)* [ready] (delim | flush: end)]
/// [shallow-info (shallow|unshallow)* delim]
/// [packfile-uris (<hash> <uri>)* delim]
/// packfile <side-band data> flush
/// ```
///
/// Errors go through [`FetchResponse::error`], which writes an `ERR`
/// pkt-line before the `packfile` section (printed as `remote error: ...`)
/// and a band-3 packet inside it (printed as `remote: ...`, then the fetch
/// fails), so the client shows the message in both cases.
#[derive(Debug)]
pub struct FetchResponse {
    done: bool,
    wait_for_done: bool,
    shallow: bool,
    uri_protocols: Vec<String>,
    no_progress: bool,
    stage: Stage,
}

impl FetchResponse {
    /// A writer for the response to `args`.
    pub fn new(args: &FetchArgs) -> Self {
        Self {
            done: args.request.done,
            wait_for_done: args.wait_for_done,
            shallow: args.request.is_shallow(),
            uri_protocols: args.request.packfile_uri_protocols.clone(),
            no_progress: args.no_progress,
            stage: Stage::Start,
        }
    }

    /// Whether the response is complete (after the final flush, or an
    /// error).
    pub fn is_finished(&self) -> bool {
        self.stage == Stage::Finished
    }

    fn enter(&mut self, stage: Stage, name: &'static str) -> Result<(), ResponseError> {
        if self.stage >= stage {
            return Err(ResponseError::OutOfOrder(name));
        }
        if !self.done && self.stage == Stage::Start && stage != Stage::Acked {
            return Err(ResponseError::AcknowledgmentsRequired);
        }
        Ok(())
    }

    /// The `acknowledgments` section. Without `ready` this ends the
    /// response (with a flush), and the client sends another round.
    pub fn acknowledgments(
        &mut self,
        out: &mut Vec<u8>,
        acks: &Acknowledgments,
    ) -> Result<(), ResponseError> {
        if self.done {
            return Err(ResponseError::AcknowledgmentsAfterDone);
        }
        if self.wait_for_done && acks.ready {
            return Err(ResponseError::ReadyWhileWaitingForDone);
        }
        self.enter(Stage::Acked, "acknowledgments")?;
        let w = |out: &mut Vec<u8>, l: &str| pkt::write_text(out, l).expect("short line");
        w(out, "acknowledgments");
        if acks.common.is_empty() {
            w(out, "NAK");
        }
        for c in &acks.common {
            w(out, &format!("ACK {c}"));
        }
        if acks.ready {
            w(out, "ready");
            pkt::write_delim(out);
            self.stage = Stage::Acked;
        } else {
            pkt::write_flush(out);
            self.stage = Stage::Finished;
        }
        Ok(())
    }

    /// The `shallow-info` section, required exactly when the request is
    /// shallow (S5). An empty section is valid.
    pub fn shallow_info(
        &mut self,
        out: &mut Vec<u8>,
        info: &ShallowInfo,
    ) -> Result<(), ResponseError> {
        if !self.shallow {
            return Err(ResponseError::ShallowInfoNotRequested);
        }
        self.enter(Stage::Shallow, "shallow-info")?;
        let w = |out: &mut Vec<u8>, l: &str| pkt::write_text(out, l).expect("short line");
        w(out, "shallow-info");
        for s in &info.shallow {
            w(out, &format!("shallow {s}"));
        }
        for u in &info.unshallow {
            w(out, &format!("unshallow {u}"));
        }
        pkt::write_delim(out);
        self.stage = Stage::Shallow;
        Ok(())
    }

    /// The `packfile-uris` section: `<pack checksum> <uri>` per pack. Only
    /// for a client that sent `packfile-uris`, only URIs in a scheme it
    /// listed, and never empty (an empty list writes nothing). A `packfile`
    /// section must follow (S3: clients fail without one).
    pub fn packfile_uris(
        &mut self,
        out: &mut Vec<u8>,
        uris: &[PackfileUri],
    ) -> Result<(), ResponseError> {
        if uris.is_empty() {
            return Ok(());
        }
        if self.uri_protocols.is_empty() {
            return Err(ResponseError::UrisNotRequested);
        }
        for u in uris {
            let scheme = u.uri.split_once("://").map(|(s, _)| s);
            let accepted = scheme.is_some_and(|s| self.uri_protocols.iter().any(|p| p == s));
            if !accepted || pkt::one_line(&u.uri) != u.uri || u.uri.contains(' ') {
                return Err(ResponseError::SchemeNotAccepted(u.uri.clone()));
            }
        }
        self.enter(Stage::Uris, "packfile-uris")?;
        self.check_shallow_before_pack()?;
        pkt::write_text(out, "packfile-uris").expect("short line");
        for u in uris {
            pkt::write_text(out, &format!("{} {}", u.hash, u.uri)).expect("checked above");
        }
        pkt::write_delim(out);
        self.stage = Stage::Uris;
        Ok(())
    }

    fn check_shallow_before_pack(&self) -> Result<(), ResponseError> {
        if self.shallow && self.stage < Stage::Shallow {
            return Err(ResponseError::ShallowInfoRequired);
        }
        Ok(())
    }

    /// Starts the `packfile` section. Follow with [`Self::pack_data`] and
    /// [`Self::progress`], then [`Self::finish`].
    pub fn start_packfile(&mut self, out: &mut Vec<u8>) -> Result<(), ResponseError> {
        self.enter(Stage::Packfile, "packfile")?;
        self.check_shallow_before_pack()?;
        pkt::write_text(out, "packfile").expect("short line");
        self.stage = Stage::Packfile;
        Ok(())
    }

    fn in_packfile(&self, what: &'static str) -> Result<(), ResponseError> {
        if self.stage != Stage::Packfile {
            return Err(ResponseError::OutOfOrder(what));
        }
        Ok(())
    }

    /// Pack bytes on band 1, in packets of at most 65520 bytes.
    pub fn pack_data(&mut self, out: &mut Vec<u8>, data: &[u8]) -> Result<(), ResponseError> {
        self.in_packfile("pack data")?;
        sideband::write_band(out, SidebandMode::Large, Band::Data, data);
        Ok(())
    }

    /// A progress line on band 2, unless the client sent `no-progress`.
    pub fn progress(&mut self, out: &mut Vec<u8>, message: &str) -> Result<(), ResponseError> {
        self.in_packfile("progress")?;
        if !self.no_progress {
            sideband::write_message(out, SidebandMode::Large, Band::Progress, message);
        }
        Ok(())
    }

    /// Ends the `packfile` section and the response with a flush.
    pub fn finish(&mut self, out: &mut Vec<u8>) -> Result<(), ResponseError> {
        self.in_packfile("final flush")?;
        pkt::write_flush(out);
        self.stage = Stage::Finished;
        Ok(())
    }

    /// Ends the response with an error the client shows: an `ERR` pkt-line
    /// before the `packfile` section, or a band-3 packet and a flush inside
    /// it. `e` must be a [`Surface::FetchErr`] error.
    pub fn error(&mut self, out: &mut Vec<u8>, e: &WireError) -> Result<(), ResponseError> {
        if e.surface != Surface::FetchErr {
            return Err(ResponseError::WrongSurface(e.surface));
        }
        match self.stage {
            Stage::Finished => return Err(ResponseError::OutOfOrder("error")),
            Stage::Packfile => {
                sideband::write_message(out, SidebandMode::Large, Band::Error, &e.message);
                pkt::write_flush(out);
            }
            _ => pkt::write_err(out, &e.message),
        }
        self.stage = Stage::Finished;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pkt::decode;
    use aspen_core::error::{FetchError, RejectReason, ToWire};
    use aspen_core::id::PackChecksum;

    fn oid(n: u8) -> ObjectId {
        ObjectId::from_bytes([n; 20])
    }
    fn advert() -> V2Advertisement {
        V2Advertisement::new(Agent::new("aspen/test").unwrap())
    }
    fn lines(mut wire: &[u8]) -> Vec<String> {
        let mut out = Vec::new();
        while let Some((p, n)) = decode(wire).unwrap() {
            out.push(match p {
                Packet::Flush => "0000".into(),
                Packet::Delim => "0001".into(),
                Packet::ResponseEnd => "0002".into(),
                Packet::Data(d) => String::from_utf8_lossy(d).trim_end_matches('\n').to_owned(),
            });
            wire = &wire[n..];
        }
        out
    }
    fn request(lines: &[&str]) -> Vec<u8> {
        let mut out = Vec::new();
        for l in lines {
            match *l {
                "0000" => pkt::write_flush(&mut out),
                "0001" => pkt::write_delim(&mut out),
                _ => pkt::write_text(&mut out, l).unwrap(),
            }
        }
        out
    }
    fn parse_with(a: V2Advertisement, wire: &[u8]) -> Result<Command, V2Error> {
        let mut p = CommandParser::new(a, V2Limits::default());
        let mut at = 0;
        loop {
            let (pkt, n) = decode(&wire[at..]).unwrap().expect("complete");
            at += n;
            if let Some(c) = p.packet(pkt)? {
                assert_eq!(at, wire.len(), "request ends at its flush");
                return Ok(c);
            }
        }
    }
    fn parse(lines: &[&str]) -> Result<Command, V2Error> {
        parse_with(advert(), &request(lines))
    }
    fn fetch(lines: &[&str]) -> Result<FetchArgs, V2Error> {
        let mut all = vec![
            "command=fetch",
            "agent=git/2.56.0",
            "object-format=sha1",
            "0001",
        ];
        all.extend_from_slice(lines);
        all.push("0000");
        match parse(&all)? {
            Command::Fetch(f) => Ok(f),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn advertisement_is_what_s3_and_s5_tested() {
        let mut out = Vec::new();
        advert().write(&mut out);
        assert_eq!(
            lines(&out),
            [
                "version 2",
                "agent=aspen/test",
                "ls-refs=unborn",
                "fetch=shallow wait-for-done",
                "server-option",
                "object-format=sha1",
                "0000"
            ]
        );
        let mut a = advert();
        a.packfile_uris = true;
        a.no_ref_delta = true;
        assert_eq!(
            a.fetch_features(),
            "shallow wait-for-done packfile-uris no-ref-delta"
        );
        assert!(
            !a.fetch_features().contains("filter"),
            "filter is never advertised (S5)"
        );
    }

    #[test]
    fn parses_ls_refs_as_git_sends_it() {
        // git 2.39's clone, from a packet trace.
        let c = parse(&[
            "command=ls-refs",
            "agent=git/2.39.5",
            "object-format=sha1",
            "0001",
            "peel",
            "symrefs",
            "unborn",
            "ref-prefix HEAD",
            "ref-prefix refs/heads/",
            "ref-prefix refs/tags/",
            "0000",
        ])
        .unwrap();
        let Command::LsRefs(r) = c else { panic!() };
        assert!(r.peel && r.symrefs && r.unborn);
        assert_eq!(r.prefixes, ["HEAD", "refs/heads/", "refs/tags/"]);
        assert_eq!(r.agent.as_deref(), Some("git/2.39.5"));
        assert!(r.matches("refs/heads/main") && r.matches("HEAD") && !r.matches("refs/notes/x"));
        assert!(LsRefsRequest::default().matches("refs/anything"));
        // A request with no arguments needs no delimiter.
        assert!(matches!(
            parse(&["command=ls-refs", "0000"]),
            Ok(Command::LsRefs(_))
        ));
        assert_eq!(parse(&["0000"]), Ok(Command::End));
    }

    #[test]
    fn parses_fetch_into_the_core_request() {
        let f = fetch(&[
            "thin-pack",
            "no-progress",
            "include-tag",
            "ofs-delta",
            &format!("want {}", oid(1)),
            &format!("want {}", oid(2)),
            &format!("have {}", oid(3)),
            "done",
        ])
        .unwrap();
        assert_eq!(f.request.wants, [oid(1), oid(2)]);
        assert_eq!(f.request.haves, [oid(3)]);
        assert!(
            f.request.done
                && f.request.thin_pack
                && f.request.ofs_delta
                && f.no_progress
                && f.include_tag
        );
        assert_eq!(f.request.agent.as_deref(), Some("git/2.56.0"));
        assert!(!f.request.is_shallow());
    }

    #[test]
    fn shallow_and_deepen_map_to_core_deepen() {
        let d = fetch(&[&format!("want {}", oid(1)), "deepen 1"]).unwrap();
        assert_eq!(
            d.request.deepen,
            Some(Deepen::Depth(NonZeroU32::new(1).unwrap()))
        );
        let b = fetch(&[
            &format!("want {}", oid(1)),
            &format!("shallow {}", oid(2)),
            "deepen-since 1700000000",
            "deepen-not refs/heads/old",
            "deepen-relative",
        ])
        .unwrap();
        assert_eq!(
            b.request.deepen,
            Some(Deepen::Boundary {
                since: Some(1_700_000_000),
                not: vec!["refs/heads/old".into()]
            })
        );
        assert_eq!(b.request.client_shallow, [oid(2)]);
        assert!(b.request.deepen_relative && b.request.is_shallow());
        assert_eq!(
            fetch(&["deepen 1", "deepen-since 5"]),
            Err(V2Error::DeepenConflict)
        );
        assert_eq!(
            fetch(&["deepen 1", "deepen-not x"]),
            Err(V2Error::DeepenConflict)
        );
        assert_eq!(
            fetch(&["deepen 0"]),
            Err(V2Error::BadDeepen("deepen 0".into()))
        );
        assert_eq!(
            fetch(&["deepen -1"]),
            Err(V2Error::BadDeepen("deepen -1".into()))
        );
        assert_eq!(
            fetch(&["deepen 1", "deepen 2"]),
            Err(V2Error::BadDeepen("deepen 2".into()))
        );
    }

    /// Invariant: `filter` is parsed (so core refuses it with a clear ERR,
    /// S5), but every feature Aspen did not advertise is refused at parse.
    #[test]
    fn unadvertised_features_are_refused_filter_is_parsed_for_a_clear_err() {
        let f = fetch(&[&format!("want {}", oid(1)), "filter blob:none"]).unwrap();
        assert_eq!(f.request.filter.as_deref(), Some("blob:none"));
        assert_eq!(
            f.request.plan_mode(aspen_core::repo::ShallowPolicy::Serve),
            Err(FetchError::filter())
        );
        for arg in [
            "packfile-uris https",
            "want-ref refs/heads/main",
            "sideband-all",
            "frobnicate",
        ] {
            assert_eq!(
                fetch(&[arg]),
                Err(V2Error::UnexpectedArgument(arg.into())),
                "{arg}"
            );
        }
        let mut a = advert();
        a.packfile_uris = true;
        let wire = request(&["command=fetch", "0001", "packfile-uris https,http", "0000"]);
        let Command::Fetch(f) = parse_with(a, &wire).unwrap() else {
            panic!()
        };
        assert_eq!(f.request.packfile_uri_protocols, ["https", "http"]);
        assert_eq!(
            fetch(&[&format!("want {}", "A".repeat(40))]),
            Err(V2Error::BadObjectId(format!("want {}", "A".repeat(40))))
        );
    }

    #[test]
    fn malformed_command_requests_are_refused() {
        assert_eq!(
            parse(&["command=push", "0000"]),
            Err(V2Error::UnknownCommand("push".into()))
        );
        assert_eq!(parse(&["agent=x", "0000"]), Err(V2Error::NoCommand));
        assert_eq!(parse(&["agent=x", "0001"]).unwrap_err(), V2Error::NoCommand);
        assert_eq!(
            parse(&["command=fetch", "command=fetch"]).unwrap_err(),
            V2Error::NoCommand
        );
        assert_eq!(
            parse(&["command=fetch", "session-id=1"]).unwrap_err(),
            V2Error::UnknownCapability("session-id=1".into())
        );
        assert_eq!(
            parse(&["command=fetch", "object-format=sha256"]).unwrap_err(),
            V2Error::UnsupportedObjectFormat("sha256".into())
        );
        assert_eq!(
            parse(&["command=ls-refs", "0001", "0001"]).unwrap_err(),
            V2Error::UnexpectedPacket("delimiter")
        );
        assert_eq!(
            parse(&["command=ls-refs", "0001", "peel extra", "0000"]),
            Err(V2Error::UnexpectedArgument("peel extra".into()))
        );
        let mut p = CommandParser::new(advert(), V2Limits::default());
        assert_eq!(p.packet(Packet::Flush), Ok(Some(Command::End)));
        assert_eq!(p.packet(Packet::Flush), Err(V2Error::AfterEnd));
        let c = parse(&[
            "command=fetch",
            "server-option=a",
            "server-option=b",
            "0001",
            "0000",
        ])
        .unwrap();
        let Command::Fetch(f) = c else { panic!() };
        assert_eq!(f.server_options, ["a", "b"]);
    }

    /// Feeds `lines` one packet at a time; returns the index of the line
    /// that failed and its error, or `None` if the request parsed.
    fn first_error(limits: V2Limits, lines: &[&str]) -> Option<(usize, V2Error)> {
        let mut p = CommandParser::new(advert(), limits);
        for (i, l) in lines.iter().enumerate() {
            let wire = request(&[l]);
            let (pkt, _) = decode(&wire).unwrap().unwrap();
            match p.packet(pkt) {
                Err(e) => return Some((i, e)),
                Ok(Some(_)) => return None,
                Ok(None) => {}
            }
        }
        panic!("request did not end");
    }

    /// Invariant: every limit, count or size, fails on the packet that
    /// crosses it, never later at the flush, so the parser never buffers
    /// past a limit.
    #[test]
    fn limits_bound_the_request() {
        let limits = V2Limits {
            max_request_bytes: usize::MAX,
            max_wants: 1,
            max_haves: 1,
            max_shallow: 1,
            max_ref_prefixes: 1,
            max_server_options: 1,
        };
        let w = format!("want {}", oid(1));
        let h = format!("have {}", oid(1));
        let s = format!("shallow {}", oid(1));
        let cases: [(&[&str], &str); 6] = [
            (&["command=fetch", "0001", &w, &w, "0000"], "wants"),
            (&["command=fetch", "0001", &h, &h, "0000"], "haves"),
            (
                &[
                    "command=fetch",
                    "0001",
                    "deepen-not a",
                    "deepen-not b",
                    "0000",
                ],
                "shallow lines",
            ),
            (
                &["command=fetch", "0001", &s, "deepen-not b", "0000"],
                "shallow lines",
            ),
            (
                &[
                    "command=ls-refs",
                    "0001",
                    "ref-prefix a",
                    "ref-prefix b",
                    "0000",
                ],
                "ref-prefix lines",
            ),
            (
                &[
                    "command=fetch",
                    "agent=x",
                    "server-option=a",
                    "server-option=b",
                    "0000",
                ],
                "server options",
            ),
        ];
        for (lines, what) in cases {
            assert_eq!(
                first_error(limits, lines),
                Some((3, V2Error::LimitExceeded(what))),
                "{lines:?}"
            );
        }
        assert_eq!(
            first_error(limits, &["command=fetch", "0001", &w, &h, "0000"]),
            None
        );

        // The byte limit counts capability and argument lines alike.
        let bytes = V2Limits {
            max_request_bytes: "command=fetch\n".len() + w.len() + 1,
            ..V2Limits::default()
        };
        // Capability lines count too: one as long as the want line fits,
        // one byte longer fails on that line.
        let agent_at = format!("agent={}", "x".repeat(w.len() - "agent=".len()));
        let agent_over = format!("{agent_at}x");
        assert_eq!(
            first_error(bytes, &["command=fetch", "0001", &w, "0000"]),
            None,
            "exactly at the limit is fine"
        );
        assert_eq!(
            first_error(bytes, &["command=fetch", "0001", &w, "done", "0000"]),
            Some((3, V2Error::LimitExceeded("request size")))
        );
        assert_eq!(
            first_error(bytes, &["command=fetch", &agent_at, "0001", "0000"]),
            None,
            "a capability line of the same size also fits"
        );
        assert_eq!(
            first_error(bytes, &["command=fetch", &agent_over, "0001", "0000"]),
            Some((1, V2Error::LimitExceeded("request size"))),
            "one byte over on a capability line fails on that line"
        );
        assert_eq!(V2Limits::default().max_request_bytes, 8 * 1024 * 1024);
    }

    /// Invariant: by default an `ls-refs` request carries at most 1,024
    /// `ref-prefix` lines; the 1,025th is refused on its own packet, before
    /// any of them reaches the store (#119).
    #[test]
    fn default_limits_take_1024_ref_prefixes() {
        let prefixes: Vec<String> = (0..1025).map(|i| format!("ref-prefix r{i}")).collect();
        let mut lines = vec!["command=ls-refs", "0001"];
        lines.extend(prefixes[..1024].iter().map(String::as_str));
        lines.push("0000");
        let Command::LsRefs(r) = parse(&lines).unwrap() else {
            panic!()
        };
        assert_eq!(r.prefixes.len(), 1024);

        let mut p = CommandParser::new(advert(), V2Limits::default());
        let mut lines = vec!["command=ls-refs", "0001"];
        lines.extend(prefixes.iter().map(String::as_str));
        let last = lines.len() - 1;
        for (i, l) in lines.iter().enumerate() {
            let wire = request(&[l]);
            let (pkt, _) = decode(&wire).unwrap().unwrap();
            let r = p.packet(pkt);
            if i == last {
                assert_eq!(r, Err(V2Error::LimitExceeded("ref-prefix lines")));
            } else {
                assert_eq!(r, Ok(None), "line {i}");
            }
        }
    }

    /// Invariant: free-form and ref-shaped values are capped per line, so
    /// 1,024 prefixes (or server options, filters, `deepen-not` revs)
    /// cannot each carry a whole 65 KB pkt-line; the check fires on the
    /// line itself, and values at the cap are kept.
    #[test]
    fn values_are_capped_per_line() {
        let long_ref = "r".repeat(MAX_REF_ARG_LEN + 1);
        let long_value = "v".repeat(MAX_VALUE_LEN + 1);
        let mut uris = advert();
        uris.packfile_uris = true;
        let cases: Vec<(V2Advertisement, Vec<String>, &str, usize)> = vec![
            (
                advert(),
                vec![
                    "command=ls-refs".into(),
                    "0001".into(),
                    format!("ref-prefix {long_ref}"),
                ],
                "ref-prefix",
                MAX_REF_ARG_LEN,
            ),
            (
                advert(),
                vec![
                    "command=fetch".into(),
                    "0001".into(),
                    format!("deepen-not {long_ref}"),
                ],
                "deepen-not",
                MAX_REF_ARG_LEN,
            ),
            (
                advert(),
                vec![
                    "command=fetch".into(),
                    "0001".into(),
                    format!("filter {long_value}"),
                ],
                "filter",
                MAX_VALUE_LEN,
            ),
            (
                uris,
                vec![
                    "command=fetch".into(),
                    "0001".into(),
                    format!("packfile-uris {long_value}"),
                ],
                "packfile-uris",
                MAX_VALUE_LEN,
            ),
            (
                advert(),
                vec![
                    "command=fetch".into(),
                    format!("server-option={long_value}"),
                ],
                "server-option",
                MAX_VALUE_LEN,
            ),
            (
                advert(),
                vec!["command=fetch".into(), format!("agent={long_value}")],
                "agent",
                MAX_VALUE_LEN,
            ),
        ];
        for (a, lines, what, max) in cases {
            let mut p = CommandParser::new(a, V2Limits::default());
            let last = lines.len() - 1;
            for (i, l) in lines.iter().enumerate() {
                let wire = request(&[l]);
                let (pkt, _) = decode(&wire).unwrap().unwrap();
                let r = p.packet(pkt);
                if i == last {
                    assert_eq!(r, Err(V2Error::ValueTooLong { what, max }), "{what}");
                } else {
                    assert_eq!(r, Ok(None));
                }
            }
        }
        let at_cap = "r".repeat(MAX_REF_ARG_LEN);
        let Command::LsRefs(r) = parse(&[
            "command=ls-refs",
            "0001",
            &format!("ref-prefix {at_cap}"),
            "0000",
        ])
        .unwrap() else {
            panic!()
        };
        assert_eq!(r.prefixes, [at_cap]);
    }

    fn rname(s: &str) -> RefName {
        RefName::new(s).unwrap()
    }

    /// Invariant: ls-refs shows only what the client asked for: matching
    /// prefixes, attributes it requested, an unborn HEAD only with
    /// `unborn`.
    #[test]
    fn ls_refs_shows_only_what_was_asked() {
        let head = LsRefsEntry {
            name: LsRefsName::Head,
            tip: Some(oid(1)),
            symref_target: Some(rname("refs/heads/main")),
            peeled: None,
        };
        let tag = LsRefsEntry {
            name: LsRefsName::Ref(rname("refs/tags/v1")),
            tip: Some(oid(2)),
            symref_target: None,
            peeled: Some(oid(1)),
        };
        let note = LsRefsEntry {
            name: LsRefsName::Ref(rname("refs/notes/x")),
            tip: Some(oid(3)),
            symref_target: None,
            peeled: None,
        };
        let all = [head.clone(), tag.clone(), note];
        let req = LsRefsRequest {
            symrefs: true,
            peel: true,
            prefixes: vec!["HEAD".into(), "refs/tags/".into()],
            ..Default::default()
        };
        let mut out = Vec::new();
        write_ls_refs(&mut out, &req, &all).unwrap();
        assert_eq!(
            lines(&out),
            [
                format!("{} HEAD symref-target:refs/heads/main", oid(1)),
                format!("{} refs/tags/v1 peeled:{}", oid(2), oid(1)),
                "0000".into(),
            ]
        );
        let mut out = Vec::new();
        write_ls_refs(&mut out, &LsRefsRequest::default(), &all).unwrap();
        assert_eq!(lines(&out).len(), 4);
        assert!(
            lines(&out).iter().all(|l| !l.contains(':')),
            "no attributes unless asked"
        );

        let unborn = LsRefsEntry {
            name: LsRefsName::Head,
            tip: None,
            symref_target: Some(rname("refs/heads/main")),
            peeled: None,
        };
        let mut out = Vec::new();
        write_ls_refs(
            &mut out,
            &LsRefsRequest {
                symrefs: true,
                ..Default::default()
            },
            std::slice::from_ref(&unborn),
        )
        .unwrap();
        assert_eq!(lines(&out), ["0000"], "unborn only when asked");
        let mut out = Vec::new();
        write_ls_refs(
            &mut out,
            &LsRefsRequest {
                symrefs: true,
                unborn: true,
                ..Default::default()
            },
            &[unborn],
        )
        .unwrap();
        assert_eq!(
            lines(&out),
            ["unborn HEAD symref-target:refs/heads/main", "0000"]
        );

        let bad = LsRefsEntry {
            name: LsRefsName::Ref(rname("refs/heads/x")),
            tip: None,
            symref_target: None,
            peeled: None,
        };
        let mut out = Vec::new();
        assert!(write_ls_refs(&mut out, &LsRefsRequest::default(), &[head, bad]).is_err());
        assert!(out.is_empty());
    }

    fn args(done: bool, shallow: bool, uris: &[&str]) -> FetchArgs {
        let mut f = FetchArgs::default();
        f.request.wants = vec![oid(1)];
        f.request.done = done;
        if shallow {
            f.request.deepen = Some(Deepen::Depth(NonZeroU32::new(1).unwrap()));
        }
        f.request.packfile_uri_protocols = uris.iter().map(|s| s.to_string()).collect();
        f
    }
    fn uri(u: &str) -> PackfileUri {
        PackfileUri {
            hash: PackChecksum::from_bytes([9; 20]),
            uri: u.to_owned(),
        }
    }

    /// The response shape S3's and S5's servers used, accepted by every
    /// tested client: acknowledgments with ready, shallow-info,
    /// packfile-uris, packfile.
    #[test]
    fn full_response_in_the_order_clients_accept() {
        let mut r = FetchResponse::new(&args(false, true, &["https"]));
        let mut out = Vec::new();
        r.acknowledgments(
            &mut out,
            &Acknowledgments {
                common: vec![oid(3)],
                ready: true,
            },
        )
        .unwrap();
        r.shallow_info(
            &mut out,
            &ShallowInfo {
                shallow: vec![oid(1)],
                unshallow: vec![],
            },
        )
        .unwrap();
        r.packfile_uris(&mut out, &[uri("https://cdn.example/p.pack")])
            .unwrap();
        r.start_packfile(&mut out).unwrap();
        r.progress(&mut out, "Counting objects").unwrap();
        r.pack_data(&mut out, b"PACK").unwrap();
        r.finish(&mut out).unwrap();
        assert!(r.is_finished());
        assert_eq!(
            lines(&out),
            [
                "acknowledgments".to_owned(),
                format!("ACK {}", oid(3)),
                "ready".into(),
                "0001".into(),
                "shallow-info".into(),
                format!("shallow {}", oid(1)),
                "0001".into(),
                "packfile-uris".into(),
                format!(
                    "{} https://cdn.example/p.pack",
                    PackChecksum::from_bytes([9; 20])
                ),
                "0001".into(),
                "packfile".into(),
                "\x02Counting objects".into(),
                "\x01PACK".into(),
                "0000".into(),
            ]
        );
    }

    /// Invariant: without `done`, acknowledgments come first (S5's
    /// "expected 'acknowledgments'" failure); with `done`, they never
    /// appear; without `ready`, the response ends after them.
    #[test]
    fn acknowledgments_rules() {
        let mut out = Vec::new();
        let mut r = FetchResponse::new(&args(false, false, &[]));
        assert_eq!(
            r.start_packfile(&mut out),
            Err(ResponseError::AcknowledgmentsRequired)
        );
        let mut r = FetchResponse::new(&args(false, true, &[]));
        assert_eq!(
            r.shallow_info(&mut out, &ShallowInfo::default()),
            Err(ResponseError::AcknowledgmentsRequired)
        );
        assert!(out.is_empty());
        r.acknowledgments(&mut out, &Acknowledgments::default())
            .unwrap();
        assert_eq!(lines(&out), ["acknowledgments", "NAK", "0000"]);
        assert!(r.is_finished());
        assert!(r.start_packfile(&mut out).is_err());

        let mut r = FetchResponse::new(&args(true, false, &[]));
        assert_eq!(
            r.acknowledgments(&mut out, &Acknowledgments::default()),
            Err(ResponseError::AcknowledgmentsAfterDone)
        );
        let mut out = Vec::new();
        r.start_packfile(&mut out).unwrap();
        r.finish(&mut out).unwrap();
        assert_eq!(lines(&out), ["packfile", "0000"]);
    }

    /// Invariant: `git fetch --negotiate-only` (what `git push` runs with
    /// `push.negotiate`) is parsed as Git 2.39 sends it: `wait-for-done`,
    /// haves and no wants. Its answer is acknowledgments that end the
    /// response, never `ready` (Git then expects a pack it never asked for).
    #[test]
    fn negotiation_only_fetch_gets_acknowledgments_and_never_ready() {
        let wire = request(&[
            "command=fetch",
            "agent=git/2.39.5",
            "object-format=sha1",
            "0001",
            "wait-for-done",
            &format!("have {}", oid(1)),
            &format!("have {}", oid(2)),
            "0000",
        ]);
        let Command::Fetch(f) = parse_with(advert(), &wire).unwrap() else {
            panic!("a fetch")
        };
        assert!(f.wait_for_done);
        assert!(f.request.wants.is_empty() && !f.request.done);
        assert_eq!(f.request.haves, [oid(1), oid(2)]);

        let mut out = Vec::new();
        let mut r = FetchResponse::new(&f);
        let ready = Acknowledgments {
            common: vec![oid(2)],
            ready: true,
        };
        assert_eq!(
            r.acknowledgments(&mut out, &ready),
            Err(ResponseError::ReadyWhileWaitingForDone)
        );
        assert!(out.is_empty());
        let acks = Acknowledgments {
            common: vec![oid(2)],
            ready: false,
        };
        r.acknowledgments(&mut out, &acks).unwrap();
        assert_eq!(
            lines(&out),
            ["acknowledgments", &format!("ACK {}", oid(2)), "0000"]
        );
        assert!(r.is_finished());
    }

    /// Invariant: a shallow request always gets shallow-info before the
    /// pack, and a non-shallow one never does (S5).
    #[test]
    fn shallow_info_exactly_when_shallow() {
        let mut out = Vec::new();
        let mut r = FetchResponse::new(&args(true, true, &["https"]));
        assert_eq!(
            r.start_packfile(&mut out),
            Err(ResponseError::ShallowInfoRequired)
        );
        assert_eq!(
            r.packfile_uris(&mut out, &[uri("https://x/y")]),
            Err(ResponseError::ShallowInfoRequired)
        );
        r.shallow_info(&mut out, &ShallowInfo::default()).unwrap();
        assert_eq!(
            r.shallow_info(&mut out, &ShallowInfo::default()),
            Err(ResponseError::OutOfOrder("shallow-info"))
        );
        r.start_packfile(&mut out).unwrap();
        let mut r = FetchResponse::new(&args(true, false, &[]));
        assert_eq!(
            r.shallow_info(&mut out, &ShallowInfo::default()),
            Err(ResponseError::ShallowInfoNotRequested)
        );
    }

    /// Invariant: URIs go only to clients that asked, only in schemes they
    /// listed (S3: clients don't filter), and a packfile section follows.
    #[test]
    fn uris_only_in_accepted_schemes() {
        let mut out = Vec::new();
        let mut r = FetchResponse::new(&args(true, false, &[]));
        assert_eq!(
            r.packfile_uris(&mut out, &[uri("https://x/y")]),
            Err(ResponseError::UrisNotRequested)
        );
        let mut r = FetchResponse::new(&args(true, false, &["https"]));
        for bad in ["http://x/y", "x/y", "https://x/y z", "https://x/\ny"] {
            assert_eq!(
                r.packfile_uris(&mut out, &[uri(bad)]),
                Err(ResponseError::SchemeNotAccepted(bad.into()))
            );
        }
        assert!(out.is_empty());
        r.packfile_uris(&mut out, &[]).unwrap();
        assert!(out.is_empty(), "an empty list writes no section");
        r.packfile_uris(&mut out, &[uri("https://x/y")]).unwrap();
        assert_eq!(
            r.finish(&mut out),
            Err(ResponseError::OutOfOrder("final flush")),
            "packfile section is mandatory"
        );
        r.start_packfile(&mut out).unwrap();
        r.finish(&mut out).unwrap();
    }

    /// Invariant: an error is always shown to the client: ERR before the
    /// packfile section, band 3 inside it.
    #[test]
    fn errors_use_err_before_the_pack_and_band_3_inside() {
        let e = FetchError::filter().to_wire();
        let mut out = Vec::new();
        let mut r = FetchResponse::new(&args(true, false, &[]));
        r.error(&mut out, &e).unwrap();
        assert_eq!(lines(&out), [format!("ERR {}", e.message)]);
        assert!(r.is_finished());
        assert_eq!(
            r.error(&mut out, &e),
            Err(ResponseError::OutOfOrder("error"))
        );

        let mut out = Vec::new();
        let mut r = FetchResponse::new(&args(true, false, &[]));
        r.start_packfile(&mut out).unwrap();
        r.pack_data(&mut out, b"PA").unwrap();
        let stored = FetchError::StoredPackNotBaseFirst {
            pack: aspen_core::id::PackId::new(1).unwrap(),
            offset: 12,
        }
        .to_wire();
        r.error(&mut out, &stored).unwrap();
        assert_eq!(
            lines(&out),
            [
                "packfile".to_owned(),
                "\x01PA".into(),
                format!("\x03{}", stored.message),
                "0000".into()
            ]
        );
        let mut r = FetchResponse::new(&args(true, false, &[]));
        assert_eq!(
            r.error(&mut out, &RejectReason::CasLost.to_wire()),
            Err(ResponseError::WrongSurface(Surface::PushRef))
        );
    }

    #[test]
    fn no_progress_suppresses_band_2_only() {
        let mut a = args(true, false, &[]);
        a.no_progress = true;
        let mut r = FetchResponse::new(&a);
        let mut out = Vec::new();
        r.start_packfile(&mut out).unwrap();
        r.progress(&mut out, "hidden").unwrap();
        r.pack_data(&mut out, b"P").unwrap();
        r.finish(&mut out).unwrap();
        assert_eq!(lines(&out), ["packfile", "\x01P", "0000"]);
    }

    /// Invariant: the parser is total on arbitrary packets.
    #[test]
    fn parser_is_total_on_arbitrary_packets() {
        for seed in 0..5_000u64 {
            let bytes = crate::pkt::tests::noise(seed, 200);
            let mut p = CommandParser::new(advert(), V2Limits::default());
            let mut first = true;
            for chunk in bytes.chunks(1 + (seed % 50) as usize) {
                let text;
                let pkt = match chunk[0] % 9 {
                    0 => Packet::Flush,
                    1 => Packet::Delim,
                    2 if first => {
                        text = b"command=fetch\n".to_vec();
                        Packet::Data(&text)
                    }
                    _ => Packet::Data(chunk),
                };
                first = false;
                if p.packet(pkt).is_err() {
                    break;
                }
            }
        }
    }
}
