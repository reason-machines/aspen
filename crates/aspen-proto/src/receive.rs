//! Protocol v0/v1 `git-receive-pack`: the advertisement, the push request,
//! and the report (`gitprotocol-pack(5)`, "Pushing Data To a Server").
//!
//! One push is:
//!
//! 1. Aspen sends [`write_advertisement`]: an optional `version 1` line, the
//!    advertised refs (trunk, plus the branch of a branch-scoped URL), up to
//!    a few earlier trunk tips as `.have` lines, and the capabilities on the
//!    first line, or `capabilities^{}` when there is nothing to show (S2).
//! 2. The client sends optional `shallow` lines, its commands (the first
//!    carrying its capabilities), a flush, its push options and a flush when
//!    it asked for `push-options`, and then the pack unless every command is
//!    a delete. [`RequestParser`] turns the pkt-line part into a
//!    [`ReceiveRequest`]; whatever follows in the stream is the pack
//!    ([`ReceiveRequest::expects_pack`]).
//! 3. Aspen answers with a [`ReportStatus`]: `unpack ok` or
//!    `unpack <error>`, then exactly one `ok <ref>` or `ng <ref> <reason>`
//!    per command. With side-band the report travels on band 1, and every
//!    rejection is repeated on band 2 as a `remote:` notice even under
//!    `quiet`, because clients ask for `quiet` whenever stderr is not a
//!    terminal (S2).

use std::collections::HashSet;

use aspen_core::error::{RejectReason, Surface, WireError};
use aspen_core::id::ObjectId;
use aspen_core::name::{MAX_REF_NAME_LEN, NameError, RefName};
use aspen_core::refs::RefCommand;

use crate::caps::{self, Agent, OBJECT_FORMAT};
use crate::pkt::{self, Packet, PktError};
use crate::service::ReceiveVersion;
use crate::sideband::{self, Band, SidebandMode};

/// What Aspen advertises to a pushing client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceiveAdvertisement {
    /// The `agent=` value.
    pub agent: Agent,
    /// Whether to advertise `no-thin`, which makes clients send whole
    /// objects instead of deltas against objects the server has. Off by
    /// default (S2): thin packs are completed at ingest.
    pub no_thin: bool,
}

/// Capabilities Aspen always advertises for push, in order (S2). `agent`
/// and the optional `no-thin` follow.
pub const RECEIVE_CAPABILITIES: [&str; 8] = [
    "report-status",
    "delete-refs",
    "side-band-64k",
    "quiet",
    "atomic",
    "ofs-delta",
    "push-options",
    "object-format=sha1",
];

/// Capabilities that change the shape of the exchange. A client may request
/// one only if it was advertised; any other unknown capability is ignored,
/// as Git does.
const EXCHANGE_CAPABILITIES: [&str; 8] = [
    "report-status",
    "report-status-v2",
    "side-band",
    "side-band-64k",
    "quiet",
    "atomic",
    "push-options",
    "push-cert",
];

impl ReceiveAdvertisement {
    /// Aspen's default advertisement with the given agent.
    pub fn new(agent: Agent) -> Self {
        Self {
            agent,
            no_thin: false,
        }
    }

    /// The space-separated capability list.
    pub fn capabilities(&self) -> String {
        let mut s = RECEIVE_CAPABILITIES.join(" ");
        s.push_str(" agent=");
        s.push_str(self.agent.as_str());
        if self.no_thin {
            s.push_str(" no-thin");
        }
        s
    }

    fn advertises(&self, name: &str) -> bool {
        RECEIVE_CAPABILITIES
            .iter()
            .any(|c| c.split('=').next() == Some(name))
            || (name == "no-thin" && self.no_thin)
            || name == "agent"
    }
}

/// A ref shown to a pushing client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdvertisedRef {
    /// The ref.
    pub name: RefName,
    /// Its current tip.
    pub tip: ObjectId,
}

/// Why an advertisement can't be written. Each is a bug in the caller.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AdvertisementError {
    /// A ref with the zero ID: a deleted ref is not advertised.
    #[error("advertised ref {0} has the zero object ID")]
    ZeroTip(RefName),
    /// The same ref twice.
    #[error("ref {0} advertised twice")]
    DuplicateRef(RefName),
    /// A `.have` with the zero ID.
    #[error(".have line with the zero object ID")]
    ZeroHave,
}

/// Appends the receive-pack advertisement.
///
/// `refs` are the named refs, `haves` extra objects the client may use as
/// delta bases and for its "what does the server have" decision (S2's
/// log-spaced earlier trunk tips). The capability list rides on the first
/// line: the first ref, else the first `.have`, else a
/// `0000000000000000000000000000000000000000 capabilities^{}` line. Git
/// accepts nothing after `capabilities^{}`, so `.have` lines are never
/// written after it. The advertisement ends with a flush. Nothing is
/// written when the input is invalid.
pub fn write_advertisement(
    out: &mut Vec<u8>,
    version: ReceiveVersion,
    advert: &ReceiveAdvertisement,
    refs: &[AdvertisedRef],
    haves: &[ObjectId],
) -> Result<(), AdvertisementError> {
    let mut seen = HashSet::new();
    for r in refs {
        if r.tip.is_zero() {
            return Err(AdvertisementError::ZeroTip(r.name.clone()));
        }
        if !seen.insert(&r.name) {
            return Err(AdvertisementError::DuplicateRef(r.name.clone()));
        }
    }
    if haves.iter().any(ObjectId::is_zero) {
        return Err(AdvertisementError::ZeroHave);
    }
    if version == ReceiveVersion::V1 {
        pkt::write_text(out, "version 1").expect("static text fits");
    }
    let caps = advert.capabilities();
    let mut lines = refs
        .iter()
        .map(|r| (r.tip, r.name.as_str()))
        .chain(haves.iter().map(|h| (*h, ".have")))
        .peekable();
    let mut line = Vec::new();
    if lines.peek().is_none() {
        line.extend_from_slice(
            format!("{} capabilities^{{}}\0{caps}\n", ObjectId::ZERO).as_bytes(),
        );
        pkt::write_data(out, &line).expect("advertisement line fits");
    }
    for (i, (oid, name)) in lines.enumerate() {
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

/// The capabilities a pushing client asked for.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ReceiveCaps {
    /// `report-status`: the client wants a report.
    pub report_status: bool,
    /// `side-band-64k`: the report and messages are multiplexed.
    pub sideband: Option<SidebandMode>,
    /// `quiet`: no progress output. Rejections are still shown (S2).
    pub quiet: bool,
    /// `atomic`: all commands succeed or none do.
    pub atomic: bool,
    /// `push-options`: option lines follow the commands.
    pub push_options: bool,
    /// `ofs-delta`: the pack may use OFS_DELTA.
    pub ofs_delta: bool,
    /// The client's `agent=` value.
    pub agent: Option<String>,
}

/// Limits on the pkt-line part of a push, so a client cannot make the
/// server buffer without bound before the pack starts. Each is checked on
/// the packet that crosses it, so the error arrives before the line is
/// stored.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReceiveLimits {
    /// Most bytes over every pkt-line payload of the request (commands,
    /// capabilities, shallow lines and push options). This bounds what the
    /// parser holds; the count limits below bound the number of entries.
    pub max_request_bytes: usize,
    /// Most commands in one push.
    pub max_commands: usize,
    /// Most `shallow` lines.
    pub max_shallow: usize,
    /// Most push options.
    pub max_push_options: usize,
    /// Most bytes over all push options.
    pub max_push_option_bytes: usize,
}

impl Default for ReceiveLimits {
    /// 16 MiB in all; 100,000 commands (a mirror push of 100,000 tags with
    /// names of about 30 bytes is about 12 MB, so it fits), 100,000 shallow
    /// lines, 256 push options and 64 KiB of them.
    fn default() -> Self {
        Self {
            max_request_bytes: 16 * 1024 * 1024,
            max_commands: 100_000,
            max_shallow: 100_000,
            max_push_options: 256,
            max_push_option_bytes: 64 * 1024,
        }
    }
}

/// A command whose ref name Aspen does not accept (see
/// [`aspen_core::name::RefName`]): printable UTF-8 of at most
/// [`MAX_REF_NAME_LEN`] bytes, but not a valid full ref name (such as
/// `refs/x`). It still gets its own `ng` line ([`ReportStatus::new`] fills
/// it in), so one bad name does not hide the status of the others. Under
/// `atomic`, the push service must reject the whole push. A name that is
/// not printable or is too long refuses the whole request instead
/// ([`RequestError::UnprintableRefName`], [`RequestError::RefNameTooLong`]),
/// because the report has to echo it on one line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvalidCommand {
    /// The name as sent.
    pub name: String,
    /// The old object ID as sent.
    pub old: ObjectId,
    /// The new object ID as sent. A non-zero value means the client sends
    /// a pack for this command too, so it counts for
    /// [`ReceiveRequest::expects_pack`].
    pub new: ObjectId,
    /// Where the command was in the request, counting from 0 over all
    /// commands, valid or not. The report keeps this order.
    pub position: usize,
    /// Why it is invalid.
    pub error: NameError,
}

/// The pkt-line part of a push.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ReceiveRequest {
    /// The commands with valid ref names, in the order sent.
    pub commands: Vec<RefCommand>,
    /// Commands with names Aspen refuses.
    pub invalid: Vec<InvalidCommand>,
    /// The client's capabilities (from the first command).
    pub caps: ReceiveCaps,
    /// Push options (`git push -o`), in order. Interpreted with
    /// [`aspen_core::refs::PushOptions::parse`].
    pub push_options: Vec<String>,
    /// `shallow` lines: the client's shallow boundary, sent by a pushing
    /// shallow clone.
    pub shallow: Vec<ObjectId>,
}

impl ReceiveRequest {
    /// Whether a pack follows: unless every command is a delete (or there
    /// are none), as `git receive-pack` reads it. Commands with names Aspen
    /// refuses count too: the client sends the pack for them all the same,
    /// and a server that does not read it breaks the connection before the
    /// client sees its `ng` lines.
    pub fn expects_pack(&self) -> bool {
        self.commands.iter().any(|c| !c.new.is_zero())
            || self.invalid.iter().any(|c| !c.new.is_zero())
    }

    /// How many commands the client sent, valid or not.
    pub fn command_count(&self) -> usize {
        self.commands.len() + self.invalid.len()
    }
}

/// Why a push request is malformed. The exchange stops; render the error
/// with [`write_fatal`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RequestError {
    /// A special packet where none belongs.
    #[error("protocol error: unexpected {0} packet")]
    UnexpectedPacket(&'static str),
    /// A command line that is not `<old> <new> <ref>`.
    #[error("protocol error: malformed command line")]
    MalformedCommand,
    /// A command, option or shallow line that is not UTF-8.
    #[error("protocol error: line is not valid UTF-8")]
    NotUtf8,
    /// A malformed `shallow` line.
    #[error("protocol error: malformed shallow line")]
    MalformedShallow,
    /// Capabilities on a command other than the first.
    #[error("protocol error: capabilities after the first command")]
    LateCapabilities,
    /// The client asked for a capability Aspen did not advertise.
    #[error("capability {0} was not advertised")]
    NotAdvertised(String),
    /// An object format other than SHA-1.
    #[error("object format {0} is not supported; this server serves sha1 repositories")]
    UnsupportedObjectFormat(String),
    /// A signed push (`push-cert`), which Aspen does not advertise.
    #[error("signed pushes are not supported by this server")]
    PushCertNotSupported,
    /// Two commands for one ref.
    #[error("ref {0} appears twice in the push")]
    DuplicateRef(String),
    /// A ref name with a control character (Git never sends one). The name
    /// is not echoed: it could not be written on one line.
    #[error("protocol error: a ref name contains a control character")]
    UnprintableRefName,
    /// A ref name longer than [`MAX_REF_NAME_LEN`] bytes. The name is not
    /// echoed.
    #[error("a ref name is {0} bytes, longer than the server's limit of {MAX_REF_NAME_LEN}")]
    RefNameTooLong(usize),
    /// A limit in [`ReceiveLimits`] was exceeded.
    #[error("push exceeds the server's limit on {0}")]
    LimitExceeded(&'static str),
    /// A packet after the request was complete.
    #[error("protocol error: packet after the end of the request")]
    AfterEnd,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    Commands,
    Options,
    Done,
}

/// Incremental parser for the pkt-line part of a push. Feed it packets with
/// [`RequestParser::packet`] until it returns the request.
#[derive(Debug)]
pub struct RequestParser {
    advert: ReceiveAdvertisement,
    limits: ReceiveLimits,
    state: State,
    req: ReceiveRequest,
    names: HashSet<String>,
    option_bytes: usize,
    request_bytes: usize,
}

fn parse_oid(s: &str) -> Option<ObjectId> {
    ObjectId::parse_hex(s).ok()
}

impl RequestParser {
    /// A parser for a client that saw `advert`.
    pub fn new(advert: ReceiveAdvertisement, limits: ReceiveLimits) -> Self {
        Self {
            advert,
            limits,
            state: State::Commands,
            req: ReceiveRequest::default(),
            names: HashSet::new(),
            option_bytes: 0,
            request_bytes: 0,
        }
    }

    /// Consumes one packet. Returns the request once its last flush has
    /// been read; the bytes after that packet are the pack, if
    /// [`ReceiveRequest::expects_pack`].
    pub fn packet(&mut self, p: Packet<'_>) -> Result<Option<ReceiveRequest>, RequestError> {
        if let (State::Commands | State::Options, Packet::Data(d)) = (self.state, p) {
            self.request_bytes += d.len();
            if self.request_bytes > self.limits.max_request_bytes {
                return Err(RequestError::LimitExceeded("request size"));
            }
        }
        match (self.state, p) {
            (State::Done, _) => Err(RequestError::AfterEnd),
            (_, Packet::Delim) => Err(RequestError::UnexpectedPacket("delimiter")),
            (_, Packet::ResponseEnd) => Err(RequestError::UnexpectedPacket("response-end")),
            (State::Commands, Packet::Flush) => {
                if self.req.command_count() > 0 && self.req.caps.push_options {
                    self.state = State::Options;
                    Ok(None)
                } else {
                    Ok(Some(self.finish()))
                }
            }
            (State::Options, Packet::Flush) => Ok(Some(self.finish())),
            (State::Commands, Packet::Data(_)) => {
                let line = p.text().expect("data packet");
                self.command_line(line).map(|()| None)
            }
            (State::Options, Packet::Data(_)) => {
                let line = p.text().expect("data packet");
                let option = std::str::from_utf8(line).map_err(|_| RequestError::NotUtf8)?;
                self.option_bytes += option.len();
                if self.req.push_options.len() >= self.limits.max_push_options
                    || self.option_bytes > self.limits.max_push_option_bytes
                {
                    return Err(RequestError::LimitExceeded("push options"));
                }
                self.req.push_options.push(option.to_owned());
                Ok(None)
            }
        }
    }

    fn finish(&mut self) -> ReceiveRequest {
        self.state = State::Done;
        std::mem::take(&mut self.req)
    }

    fn command_line(&mut self, line: &[u8]) -> Result<(), RequestError> {
        let first = self.req.command_count() == 0;
        if line.starts_with(b"push-cert") {
            return Err(RequestError::PushCertNotSupported);
        }
        if first && let Some(rest) = line.strip_prefix(b"shallow ") {
            let s = std::str::from_utf8(rest).map_err(|_| RequestError::MalformedShallow)?;
            let oid = parse_oid(s).ok_or(RequestError::MalformedShallow)?;
            if self.req.shallow.len() >= self.limits.max_shallow {
                return Err(RequestError::LimitExceeded("shallow lines"));
            }
            self.req.shallow.push(oid);
            return Ok(());
        }
        let (command, caps) = match line.iter().position(|&b| b == 0) {
            Some(nul) => (&line[..nul], Some(&line[nul + 1..])),
            None => (line, None),
        };
        if let Some(caps) = caps {
            if !first {
                return Err(RequestError::LateCapabilities);
            }
            let caps = std::str::from_utf8(caps).map_err(|_| RequestError::NotUtf8)?;
            self.req.caps = self.client_caps(caps)?;
        }
        let command = std::str::from_utf8(command).map_err(|_| RequestError::NotUtf8)?;
        let mut parts = command.splitn(3, ' ');
        let (Some(old), Some(new), Some(name)) = (parts.next(), parts.next(), parts.next()) else {
            return Err(RequestError::MalformedCommand);
        };
        let (Some(old), Some(new)) = (parse_oid(old), parse_oid(new)) else {
            return Err(RequestError::MalformedCommand);
        };
        if name.is_empty() {
            return Err(RequestError::MalformedCommand);
        }
        // The report echoes every name on one text line, so a name that
        // could not be written there refuses the request before it is kept.
        if name.chars().any(char::is_control) {
            return Err(RequestError::UnprintableRefName);
        }
        if name.len() > MAX_REF_NAME_LEN {
            return Err(RequestError::RefNameTooLong(name.len()));
        }
        let position = self.req.command_count();
        if position >= self.limits.max_commands {
            return Err(RequestError::LimitExceeded("commands"));
        }
        if !self.names.insert(name.to_owned()) {
            return Err(RequestError::DuplicateRef(name.to_owned()));
        }
        match RefName::new(name) {
            Ok(name) => self.req.commands.push(RefCommand { name, old, new }),
            Err(error) => self.req.invalid.push(InvalidCommand {
                name: name.to_owned(),
                old,
                new,
                position,
                error,
            }),
        }
        Ok(())
    }

    fn client_caps(&self, list: &str) -> Result<ReceiveCaps, RequestError> {
        let mut caps = ReceiveCaps::default();
        for c in caps::parse_list(list) {
            if EXCHANGE_CAPABILITIES.contains(&c.name.as_str()) && !self.advert.advertises(&c.name)
            {
                return Err(RequestError::NotAdvertised(c.name));
            }
            match c.name.as_str() {
                "report-status" => caps.report_status = true,
                "side-band-64k" => caps.sideband = Some(SidebandMode::Large),
                "quiet" => caps.quiet = true,
                "atomic" => caps.atomic = true,
                "push-options" => caps.push_options = true,
                "ofs-delta" => caps.ofs_delta = true,
                "agent" => caps.agent = c.value,
                "object-format" => {
                    let v = c.value.unwrap_or_default();
                    if v != OBJECT_FORMAT {
                        return Err(RequestError::UnsupportedObjectFormat(v));
                    }
                }
                _ => {}
            }
        }
        Ok(caps)
    }
}

/// The status of one command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RefStatus {
    /// `ok <ref>`.
    Ok,
    /// `ng <ref> <reason>`.
    Rejected(String),
}

impl RefStatus {
    /// The `ng` status for an error whose surface is
    /// [`Surface::PushRef`].
    pub fn from_wire(e: &WireError) -> Result<Self, ReportError> {
        match e.surface {
            Surface::PushRef => Ok(Self::Rejected(e.message.clone())),
            other => Err(ReportError::WrongSurface(other)),
        }
    }
}

impl From<RejectReason> for RefStatus {
    fn from(r: RejectReason) -> Self {
        Self::Rejected(r.message().to_owned())
    }
}

/// Why a report can't be built or written. Each is a bug in the caller.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ReportError {
    /// A status for a ref the push did not name.
    #[error("ref {0} is not a command of this push")]
    UnknownRef(RefName),
    /// A command without a status. Git clients report "remote failed to
    /// report status" for it, so every command must get one.
    #[error("no status for {0}")]
    Missing(String),
    /// A [`WireError`] meant for another surface.
    #[error("error for surface {0:?} used in a push report")]
    WrongSurface(Surface),
    /// A report line that cannot be written as one pkt-line: a hand-built
    /// [`InvalidCommand`] whose name has a control character or is too
    /// long. [`RequestParser`] never produces one.
    #[error("report line for ref {name:?} cannot be written: {error}")]
    Unwritable {
        /// The name, as given.
        name: String,
        /// Why the line does not fit.
        error: PktError,
    },
}

/// The report a push ends with: one unpack status and exactly one status
/// per command, in the order the client sent them (valid and invalid names
/// interleaved as sent).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReportStatus {
    unpack: Result<(), String>,
    refs: Vec<(String, Option<RefStatus>)>,
    warnings: Vec<String>,
    notes: Vec<String>,
}

impl ReportStatus {
    /// A report for `req` with `unpack ok`, every invalid command already
    /// rejected, and every other command still to be decided.
    pub fn new(req: &ReceiveRequest) -> Self {
        let mut valid = req.commands.iter();
        let mut invalid = req.invalid.iter().peekable();
        let mut refs = Vec::with_capacity(req.command_count());
        // A merge by position. Each step takes exactly one command, so a
        // hand-built request with odd positions still keeps every command.
        while refs.len() < req.command_count() {
            let at = refs.len();
            let slot = match invalid.next_if(|c| c.position <= at) {
                Some(c) => Self::invalid_slot(c),
                None => match valid.next() {
                    Some(c) => (c.name.as_str().to_owned(), None),
                    None => Self::invalid_slot(invalid.next().expect("counted")),
                },
            };
            refs.push(slot);
        }
        Self {
            unpack: Ok(()),
            refs,
            warnings: Vec::new(),
            notes: Vec::new(),
        }
    }

    /// Adds a line written on band 2 (`remote: aspen: <line>`) before the
    /// rejections, even under `quiet`: the details behind a rejection
    /// that do not fit its one `ng` line, such as every problem a
    /// repository's push rules found. Each line goes through
    /// [`pkt::one_line`]. Without side-band there is nowhere to write them,
    /// and they are dropped.
    pub fn note(&mut self, line: &str) {
        self.notes.push(line.to_owned());
    }

    /// Adds a warning written on band 2 as `warning: <line>` (git prints
    /// `remote: warning: <line>`), first and even under `quiet`, exactly as
    /// stock `git receive-pack` writes its own warnings, such as "deleting
    /// a non-existent ref". Each line goes through [`pkt::one_line`].
    /// Without side-band they are dropped.
    pub fn warn(&mut self, line: &str) {
        self.warnings.push(line.to_owned());
    }

    fn invalid_slot(c: &InvalidCommand) -> (String, Option<RefStatus>) {
        let reason = format!("invalid ref name: {}", c.error);
        (c.name.clone(), Some(RefStatus::Rejected(reason)))
    }

    /// Sets the status of one command.
    pub fn set(&mut self, name: &RefName, status: RefStatus) -> Result<(), ReportError> {
        let slot = self
            .refs
            .iter_mut()
            .find(|(n, _)| n == name.as_str())
            .ok_or_else(|| ReportError::UnknownRef(name.clone()))?;
        slot.1 = Some(status);
        Ok(())
    }

    /// The pack was refused: `unpack <message>`, and every command
    /// `ng <ref> unpacker error`, as the [`Surface::PushUnpack`] contract
    /// says.
    pub fn unpack_failed(&mut self, e: &WireError) -> Result<(), ReportError> {
        if e.surface != Surface::PushUnpack {
            return Err(ReportError::WrongSurface(e.surface));
        }
        self.unpack = Err(e.message.clone());
        for (_, status) in &mut self.refs {
            *status = Some(RejectReason::UnpackFailed.into());
        }
        Ok(())
    }

    /// Appends the report as `caps` asked for it, followed by the final
    /// flush. With side-band, each rejection (and an unpack failure) is
    /// first written on band 2 as `aspen: <ref>: <reason>`, even under
    /// `quiet`, then the report on band 1. Without `report-status`, only
    /// those notices are written. Nothing is written if a command has no
    /// status or a line cannot be written.
    pub fn write(&self, out: &mut Vec<u8>, caps: &ReceiveCaps) -> Result<(), ReportError> {
        let mut report = Vec::new();
        match &self.unpack {
            Ok(()) => pkt::write_text(&mut report, "unpack ok"),
            Err(m) => pkt::write_text(&mut report, &format!("unpack {}", pkt::one_line(m))),
        }
        .expect("one_line output always fits one text pkt-line");
        for (name, status) in &self.refs {
            let line = match status {
                None => return Err(ReportError::Missing(name.clone())),
                Some(RefStatus::Ok) => format!("ok {name}"),
                Some(RefStatus::Rejected(reason)) => {
                    format!("ng {name} {}", pkt::one_line(reason))
                }
            };
            pkt::write_text(&mut report, &line).map_err(|error| ReportError::Unwritable {
                name: name.clone(),
                error,
            })?;
        }
        pkt::write_flush(&mut report);

        match caps.sideband {
            Some(mode) => {
                if let Err(m) = &self.unpack {
                    sideband::write_message(
                        out,
                        mode,
                        Band::Progress,
                        &format!("aspen: unpack failed: {m}"),
                    );
                }
                for line in &self.warnings {
                    sideband::write_message(out, mode, Band::Progress, &format!("warning: {line}"));
                }
                for line in &self.notes {
                    sideband::write_message(out, mode, Band::Progress, &format!("aspen: {line}"));
                }
                for (name, status) in &self.refs {
                    if let Some(RefStatus::Rejected(reason)) = status {
                        sideband::write_message(
                            out,
                            mode,
                            Band::Progress,
                            &format!("aspen: {name}: {reason}"),
                        );
                    }
                }
                if caps.report_status {
                    sideband::write_band(out, mode, Band::Data, &report);
                }
                pkt::write_flush(out);
            }
            None if caps.report_status => out.extend_from_slice(&report),
            None => {}
        }
        Ok(())
    }
}

/// Appends a progress line on band 2, unless the client asked for `quiet`
/// or has no side-band (then there is nowhere to put it).
pub fn write_progress(out: &mut Vec<u8>, caps: &ReceiveCaps, message: &str) {
    if let (Some(mode), false) = (caps.sideband, caps.quiet) {
        sideband::write_message(out, mode, Band::Progress, message);
    }
}

/// Appends a fatal error that ends the push. Before the request is parsed
/// (`caps` is `None`) or without side-band, it is an `ERR` pkt-line; with
/// side-band, a band-3 packet followed by a flush. Git prints the first as
/// `remote error: <message>` and the second as `remote: <message>`, and the
/// push fails either way (checked against stock git in `tests/interop.rs`).
pub fn write_fatal(out: &mut Vec<u8>, caps: Option<&ReceiveCaps>, message: &str) {
    match caps.and_then(|c| c.sideband) {
        Some(mode) => {
            sideband::write_message(out, mode, Band::Error, message);
            pkt::write_flush(out);
        }
        None => pkt::write_err(out, message),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pkt::decode;
    use aspen_core::error::{ToWire, UnpackError};

    fn oid(n: u8) -> ObjectId {
        ObjectId::from_bytes([n; 20])
    }
    fn name(s: &str) -> RefName {
        RefName::new(s).unwrap()
    }
    fn advert() -> ReceiveAdvertisement {
        ReceiveAdvertisement::new(Agent::new("aspen/test").unwrap())
    }

    /// Splits a wire buffer into text lines, `0000` for flush.
    fn lines(mut wire: &[u8]) -> Vec<String> {
        let mut out = Vec::new();
        while let Some((p, n)) = decode(wire).unwrap() {
            out.push(match p {
                Packet::Flush => "0000".to_owned(),
                Packet::Delim => "0001".to_owned(),
                Packet::ResponseEnd => "0002".to_owned(),
                Packet::Data(d) => String::from_utf8_lossy(d).into_owned(),
            });
            wire = &wire[n..];
        }
        assert!(wire.is_empty(), "trailing bytes");
        out
    }

    fn parse(wire: &[u8]) -> Result<(ReceiveRequest, usize), RequestError> {
        let mut p = RequestParser::new(advert(), ReceiveLimits::default());
        let mut at = 0;
        loop {
            let (pkt, n) = decode(&wire[at..]).unwrap().expect("complete request");
            at += n;
            if let Some(req) = p.packet(pkt)? {
                return Ok((req, at));
            }
        }
    }

    fn request(lines: &[&str]) -> Vec<u8> {
        let mut out = Vec::new();
        for l in lines {
            if *l == "0000" {
                pkt::write_flush(&mut out);
            } else {
                pkt::write_data(&mut out, format!("{l}\n").as_bytes()).unwrap();
            }
        }
        out
    }

    #[test]
    fn advertisement_matches_s2() {
        let mut out = Vec::new();
        let refs = [AdvertisedRef {
            name: name("refs/heads/main"),
            tip: oid(1),
        }];
        write_advertisement(
            &mut out,
            ReceiveVersion::V0,
            &advert(),
            &refs,
            &[oid(2), oid(3)],
        )
        .unwrap();
        let l = lines(&out);
        assert_eq!(
            l[0],
            format!(
                "{} refs/heads/main\0report-status delete-refs side-band-64k quiet atomic ofs-delta push-options object-format=sha1 agent=aspen/test\n",
                oid(1)
            )
        );
        assert_eq!(l[1], format!("{} .have\n", oid(2)));
        assert_eq!(l[2], format!("{} .have\n", oid(3)));
        assert_eq!(l[3], "0000");
        assert_eq!(l.len(), 4);
    }

    /// Invariant: capabilities ride on exactly one line, the first, and
    /// `capabilities^{}` appears only when nothing else is shown (Git
    /// rejects any line after it).
    #[test]
    fn capabilities_ride_on_the_first_line_only() {
        let a = advert();
        let cases: [(&[AdvertisedRef], &[ObjectId]); 3] = [
            (&[], &[]),
            (&[], &[oid(9)]),
            (
                &[
                    AdvertisedRef {
                        name: name("refs/heads/main"),
                        tip: oid(1),
                    },
                    AdvertisedRef {
                        name: name("refs/heads/topic"),
                        tip: oid(2),
                    },
                ],
                &[oid(3)],
            ),
        ];
        for (refs, haves) in cases {
            for version in [ReceiveVersion::V0, ReceiveVersion::V1] {
                let mut out = Vec::new();
                write_advertisement(&mut out, version, &a, refs, haves).unwrap();
                let mut l = lines(&out);
                if version == ReceiveVersion::V1 {
                    assert_eq!(l.remove(0), "version 1\n");
                }
                assert_eq!(l.pop().unwrap(), "0000");
                assert_eq!(l.len(), (refs.len() + haves.len()).max(1));
                assert!(l[0].contains('\0'));
                assert!(l[1..].iter().all(|x| !x.contains('\0')));
                let dummy = l.iter().filter(|x| x.contains("capabilities^{}")).count();
                assert_eq!(dummy, usize::from(refs.is_empty() && haves.is_empty()));
            }
        }
        let mut out = Vec::new();
        assert_eq!(
            write_advertisement(
                &mut out,
                ReceiveVersion::V0,
                &a,
                &[AdvertisedRef {
                    name: name("refs/heads/x"),
                    tip: ObjectId::ZERO
                }],
                &[]
            ),
            Err(AdvertisementError::ZeroTip(name("refs/heads/x")))
        );
        let dup = AdvertisedRef {
            name: name("refs/heads/x"),
            tip: oid(1),
        };
        assert!(
            write_advertisement(&mut out, ReceiveVersion::V0, &a, &[dup.clone(), dup], &[])
                .is_err()
        );
        assert!(
            write_advertisement(&mut out, ReceiveVersion::V0, &a, &[], &[ObjectId::ZERO]).is_err()
        );
        assert!(out.is_empty(), "nothing written for invalid input");
        let mut no_thin = advert();
        no_thin.no_thin = true;
        assert!(no_thin.capabilities().ends_with(" no-thin"));
    }

    #[test]
    fn parses_the_commands_git_sends() {
        // From S2's wire excerpts (git 2.56): a zero old-oid update and a
        // zero/zero full-name delete, with push options.
        let caps = "report-status side-band-64k quiet atomic push-options object-format=sha1 agent=git/2.56.0-Linux";
        let mut wire = request(&[
            &format!("{} {} refs/heads/feature\0{caps}", ObjectId::ZERO, oid(7)),
            &format!("{} {} refs/heads/old", ObjectId::ZERO, ObjectId::ZERO),
            "0000",
            "force",
            "lease=refs/heads/feature:0707070707070707070707070707070707070707",
            "0000",
        ]);
        let end = wire.len();
        wire.extend_from_slice(b"PACK\0\0\0\x02");
        let (req, used) = parse(&wire).unwrap();
        assert_eq!(used, end, "stops exactly where the pack starts");
        assert_eq!(
            req.commands,
            vec![
                RefCommand {
                    name: name("refs/heads/feature"),
                    old: ObjectId::ZERO,
                    new: oid(7)
                },
                RefCommand {
                    name: name("refs/heads/old"),
                    old: ObjectId::ZERO,
                    new: ObjectId::ZERO
                },
            ]
        );
        assert_eq!(
            req.push_options,
            [
                "force",
                "lease=refs/heads/feature:0707070707070707070707070707070707070707"
            ]
        );
        assert!(
            req.caps.report_status && req.caps.quiet && req.caps.atomic && req.caps.push_options
        );
        assert_eq!(req.caps.sideband, Some(SidebandMode::Large));
        assert_eq!(req.caps.agent.as_deref(), Some("git/2.56.0-Linux"));
        assert!(req.expects_pack());
        let opts =
            aspen_core::refs::PushOptions::parse(req.push_options.iter().map(String::as_str))
                .unwrap();
        assert!(opts.force_requested(&name("refs/heads/old")));
    }

    /// Invariant: a pack follows unless every command is a delete; without
    /// the push-options capability, the first flush ends the request.
    #[test]
    fn delete_only_push_has_no_pack_and_options_need_the_capability() {
        let wire = request(&[
            &format!("{} {} refs/heads/a\0report-status", oid(1), ObjectId::ZERO),
            "0000",
        ]);
        let (req, used) = parse(&wire).unwrap();
        assert_eq!(used, wire.len());
        assert!(!req.expects_pack());
        assert!(req.push_options.is_empty());
        let (req, _) = parse(&request(&["0000"])).unwrap();
        assert_eq!(req, ReceiveRequest::default());
    }

    #[test]
    fn shallow_lines_come_before_commands() {
        let wire = request(&[
            &format!("shallow {}", oid(4)),
            &format!("{} {} refs/heads/a\0report-status", ObjectId::ZERO, oid(1)),
            "0000",
        ]);
        let (req, _) = parse(&wire).unwrap();
        assert_eq!(req.shallow, [oid(4)]);
        let late = request(&[
            &format!("{} {} refs/heads/a\0report-status", ObjectId::ZERO, oid(1)),
            &format!("shallow {}", oid(4)),
        ]);
        assert_eq!(parse(&late), Err(RequestError::MalformedCommand));
    }

    /// Invariant: a client can request only what changes the exchange if it
    /// was advertised; it can never get a report format or framing Aspen
    /// does not write.
    #[test]
    fn refuses_unadvertised_exchange_capabilities_and_sha256() {
        for cap in ["report-status-v2", "side-band", "push-cert=abc"] {
            let wire = request(&[
                &format!(
                    "{} {} refs/heads/a\0report-status {cap}",
                    ObjectId::ZERO,
                    oid(1)
                ),
                "0000",
            ]);
            let want = cap.split('=').next().unwrap().to_owned();
            assert_eq!(
                parse(&wire),
                Err(RequestError::NotAdvertised(want)),
                "{cap}"
            );
        }
        let wire = request(&[
            &format!(
                "{} {} refs/heads/a\0object-format=sha256",
                ObjectId::ZERO,
                oid(1)
            ),
            "0000",
        ]);
        assert_eq!(
            parse(&wire),
            Err(RequestError::UnsupportedObjectFormat("sha256".into()))
        );
        // Unknown, harmless capabilities are ignored, as Git does.
        let wire = request(&[
            &format!(
                "{} {} refs/heads/a\0report-status session-id=x future-cap",
                ObjectId::ZERO,
                oid(1)
            ),
            "0000",
        ]);
        assert!(parse(&wire).unwrap().0.caps.report_status);
    }

    #[test]
    fn malformed_requests_are_refused() {
        let z = ObjectId::ZERO;
        let cases: Vec<(Vec<u8>, RequestError)> = vec![
            (
                request(&[&format!("{z} {} refs/heads/a", "A".repeat(40))]),
                RequestError::MalformedCommand,
            ),
            (
                request(&[&format!("{z} {z}")]),
                RequestError::MalformedCommand,
            ),
            (
                request(&[&format!("{z} {z} ")]),
                RequestError::MalformedCommand,
            ),
            (
                request(&["push-cert\0push-cert=1"]),
                RequestError::PushCertNotSupported,
            ),
            (
                request(&[
                    &format!("{z} {} refs/heads/a\0report-status", oid(1)),
                    &format!("{z} {} refs/heads/b\0quiet", oid(1)),
                ]),
                RequestError::LateCapabilities,
            ),
            (
                request(&[
                    &format!("{z} {} refs/heads/a\0report-status", oid(1)),
                    &format!("{z} {} refs/heads/a", oid(2)),
                ]),
                RequestError::DuplicateRef("refs/heads/a".into()),
            ),
        ];
        for (wire, want) in cases {
            assert_eq!(parse(&wire), Err(want));
        }
        let mut p = RequestParser::new(advert(), ReceiveLimits::default());
        assert_eq!(
            p.packet(Packet::Delim),
            Err(RequestError::UnexpectedPacket("delimiter"))
        );
        let mut p = RequestParser::new(advert(), ReceiveLimits::default());
        assert!(p.packet(Packet::Flush).unwrap().is_some());
        assert_eq!(p.packet(Packet::Flush), Err(RequestError::AfterEnd));
    }

    /// Invariant: the parser buffers a bounded amount before the pack.
    #[test]
    fn limits_bound_the_request() {
        let limits = ReceiveLimits {
            max_request_bytes: usize::MAX,
            max_commands: 2,
            max_shallow: 1,
            max_push_options: 1,
            max_push_option_bytes: 8,
        };
        let run = |lines: &[&str]| {
            let wire = request(lines);
            let mut p = RequestParser::new(advert(), limits);
            let mut at = 0;
            loop {
                let (pkt, n) = decode(&wire[at..]).unwrap().unwrap();
                at += n;
                if let Some(r) = p.packet(pkt)? {
                    return Ok(r);
                }
            }
        };
        let z = ObjectId::ZERO;
        let c = |n: &str, caps: &str| format!("{z} {} refs/heads/{n}{caps}", oid(1));
        assert_eq!(
            run(&[&c("a", "\0push-options"), &c("b", ""), &c("c", "")]),
            Err(RequestError::LimitExceeded("commands"))
        );
        assert_eq!(
            run(&[&c("a", "\0push-options"), "0000", "a", "b"]),
            Err(RequestError::LimitExceeded("push options"))
        );
        assert_eq!(
            run(&[&c("a", "\0push-options"), "0000", "123456789"]),
            Err(RequestError::LimitExceeded("push options"))
        );
        let s = format!("shallow {}", oid(2));
        assert_eq!(
            run(&[&s, &s]),
            Err(RequestError::LimitExceeded("shallow lines"))
        );
        assert!(
            run(&[
                &c("a", "\0push-options"),
                &c("b", ""),
                "0000",
                "12345678",
                "0000"
            ])
            .is_ok()
        );
    }

    /// Invariant: the byte limit is checked on the packet that crosses it,
    /// before the line is stored, whatever kind of line it is.
    #[test]
    fn byte_limit_fails_on_the_crossing_packet() {
        let z = ObjectId::ZERO;
        let first = format!("{z} {} refs/heads/a\0push-options\n", oid(1));
        let second = format!("{z} {} refs/heads/b\n", oid(1));
        let limits = ReceiveLimits {
            max_request_bytes: first.len() + second.len(),
            ..ReceiveLimits::default()
        };
        for (extra, state) in [
            (format!("{z} {} refs/heads/c\n", oid(1)), "command"),
            ("x\n".to_owned(), "option"),
        ] {
            let mut p = RequestParser::new(advert(), limits);
            assert_eq!(p.packet(Packet::Data(first.as_bytes())), Ok(None));
            assert_eq!(p.packet(Packet::Data(second.as_bytes())), Ok(None));
            if state == "option" {
                assert_eq!(p.packet(Packet::Flush), Ok(None));
            }
            assert_eq!(
                p.packet(Packet::Data(extra.as_bytes())),
                Err(RequestError::LimitExceeded("request size")),
                "{state}"
            );
        }
        assert_eq!(ReceiveLimits::default().max_request_bytes, 16 * 1024 * 1024);
    }

    /// Invariant: a name Git allows but Aspen does not store gets its own
    /// `ng` line, in the position the client sent it, and the other
    /// commands keep theirs.
    #[test]
    fn invalid_ref_names_get_their_own_ng_line_in_order() {
        let wire = request(&[
            &format!("{} {} refs/heads/ok\0report-status", ObjectId::ZERO, oid(1)),
            &format!("{} {} refs/one-level", ObjectId::ZERO, oid(1)),
            &format!("{} {} refs/heads/after", ObjectId::ZERO, oid(1)),
            &format!("{} {} refs/x", ObjectId::ZERO, oid(1)),
            "0000",
        ]);
        let (req, _) = parse(&wire).unwrap();
        assert_eq!(req.commands.len(), 2);
        assert_eq!(req.invalid.len(), 2);
        assert_eq!(req.invalid[0].position, 1);
        assert_eq!(
            (req.invalid[0].old, req.invalid[0].new),
            (ObjectId::ZERO, oid(1))
        );
        let mut report = ReportStatus::new(&req);
        report.set(&name("refs/heads/ok"), RefStatus::Ok).unwrap();
        report
            .set(&name("refs/heads/after"), RefStatus::Ok)
            .unwrap();
        let mut out = Vec::new();
        report.write(&mut out, &req.caps).unwrap();
        let l = lines(&out);
        assert_eq!(l[0], "unpack ok\n");
        assert_eq!(l[1], "ok refs/heads/ok\n");
        assert!(
            l[2].starts_with("ng refs/one-level invalid ref name: "),
            "{}",
            l[2]
        );
        assert_eq!(l[3], "ok refs/heads/after\n");
        assert!(l[4].starts_with("ng refs/x invalid ref name: "), "{}", l[4]);
        assert_eq!(l[5], "0000");
    }

    /// Invariant: a pack follows whenever any command, valid name or not,
    /// has a non-zero new ID, as `git receive-pack` counts it; stock git
    /// sends the pack for `main:refs/x` even though Aspen refuses the name.
    #[test]
    fn invalid_names_count_for_expects_pack() {
        let z = ObjectId::ZERO;
        let only_invalid = request(&[&format!("{z} {} refs/x\0report-status", oid(1)), "0000"]);
        let (req, _) = parse(&only_invalid).unwrap();
        assert!(req.commands.is_empty());
        assert!(req.expects_pack());
        let with_delete = request(&[
            &format!("{} {z} refs/heads/old\0report-status", oid(2)),
            &format!("{z} {} refs/x", oid(1)),
            "0000",
        ]);
        assert!(parse(&with_delete).unwrap().0.expects_pack());
        let invalid_delete = request(&[
            &format!("{} {z} refs/heads/old\0report-status", oid(2)),
            &format!("{} {z} refs/x", oid(1)),
            "0000",
        ]);
        assert!(!parse(&invalid_delete).unwrap().0.expects_pack());
    }

    /// Invariant: a ref name the report could not echo on one pkt-line (a
    /// control character anywhere, or longer than `MAX_REF_NAME_LEN`)
    /// refuses the whole request; every name the parser accepts can be
    /// written in a report with the longest reason, so `write` never fails
    /// on parsed input.
    #[test]
    fn unwritable_ref_names_refuse_the_request() {
        let z = ObjectId::ZERO;
        let cmd = |n: &[u8]| {
            let mut line = format!("{z} {} ", oid(1)).into_bytes();
            line.extend_from_slice(n);
            line.extend_from_slice(b"\0report-status side-band-64k\n");
            line
        };
        let huge = format!("refs/heads/{}", "a".repeat(pkt::MAX_DATA_LEN - 200));
        let cases: Vec<(Vec<u8>, RequestError)> = vec![
            (cmd(b"refs/heads/a\x01b"), RequestError::UnprintableRefName),
            (
                cmd(b"refs/heads/\x1b[31mred"),
                RequestError::UnprintableRefName,
            ),
            (
                cmd(b"refs/heads/a\nok refs/heads/b"),
                RequestError::UnprintableRefName,
            ),
            (cmd(b"refs/heads/a\tb"), RequestError::UnprintableRefName),
            (cmd(b"refs/heads/a\x7f"), RequestError::UnprintableRefName),
            (
                cmd("refs/heads/a\u{85}b".as_bytes()),
                RequestError::UnprintableRefName,
            ),
            (
                cmd(huge.as_bytes()),
                RequestError::RefNameTooLong(huge.len()),
            ),
            (
                cmd("r".repeat(MAX_REF_NAME_LEN + 1).as_bytes()),
                RequestError::RefNameTooLong(MAX_REF_NAME_LEN + 1),
            ),
        ];
        for (line, want) in cases {
            let mut p = RequestParser::new(advert(), ReceiveLimits::default());
            assert_eq!(p.packet(Packet::Data(&line)), Err(want));
        }
        // At the limit the name is kept, and the report (with the longest
        // reason, on both the plain and the side-band path) is written.
        let longest_invalid = "r".repeat(MAX_REF_NAME_LEN);
        let longest_valid = format!("refs/heads/{}", "v".repeat(MAX_REF_NAME_LEN - 11));
        let mut wire = Vec::new();
        pkt::write_data(&mut wire, &cmd(longest_invalid.as_bytes())).unwrap();
        pkt::write_data(
            &mut wire,
            format!("{z} {} {longest_valid}\n", oid(1)).as_bytes(),
        )
        .unwrap();
        pkt::write_flush(&mut wire);
        let (req, _) = parse(&wire).unwrap();
        assert_eq!(req.invalid.len(), 1);
        let mut r = ReportStatus::new(&req);
        r.set(
            &name(&longest_valid),
            RefStatus::Rejected("\x01".repeat(3 * pkt::MAX_MESSAGE_LEN)),
        )
        .unwrap();
        for caps in [
            req.caps.clone(),
            ReceiveCaps {
                sideband: None,
                ..req.caps.clone()
            },
        ] {
            let mut out = Vec::new();
            r.write(&mut out, &caps).unwrap();
            assert!(!out.is_empty());
        }
    }

    /// Invariant: a hand-built request whose name cannot be written gives
    /// an error from `write`, never a panic, and writes nothing.
    #[test]
    fn write_refuses_unwritable_hand_built_names() {
        for bad in [
            "refs/x\nok refs/heads/a".to_owned(),
            "r".repeat(pkt::MAX_DATA_LEN),
        ] {
            let req = ReceiveRequest {
                invalid: vec![InvalidCommand {
                    name: bad.clone(),
                    old: ObjectId::ZERO,
                    new: oid(1),
                    position: 0,
                    error: NameError::NotUnderRefs,
                }],
                caps: ReceiveCaps {
                    report_status: true,
                    ..Default::default()
                },
                ..Default::default()
            };
            let mut out = Vec::new();
            let e = ReportStatus::new(&req).write(&mut out, &req.caps);
            assert!(matches!(e, Err(ReportError::Unwritable { name, .. }) if name == bad));
            assert!(out.is_empty());
        }
    }

    /// Invariant: every command gets exactly one status, or nothing is
    /// written.
    #[test]
    fn report_needs_a_status_for_every_command() {
        let req = ReceiveRequest {
            commands: vec![
                RefCommand {
                    name: name("refs/heads/a"),
                    old: ObjectId::ZERO,
                    new: oid(1),
                },
                RefCommand {
                    name: name("refs/heads/b"),
                    old: ObjectId::ZERO,
                    new: oid(1),
                },
            ],
            caps: ReceiveCaps {
                report_status: true,
                ..Default::default()
            },
            ..Default::default()
        };
        let mut r = ReportStatus::new(&req);
        r.set(&name("refs/heads/a"), RefStatus::Ok).unwrap();
        let mut out = Vec::new();
        assert_eq!(
            r.write(&mut out, &req.caps),
            Err(ReportError::Missing("refs/heads/b".into()))
        );
        assert!(out.is_empty());
        assert_eq!(
            r.set(&name("refs/heads/c"), RefStatus::Ok),
            Err(ReportError::UnknownRef(name("refs/heads/c")))
        );
        r.set(&name("refs/heads/b"), RejectReason::NonFastForward.into())
            .unwrap();
        r.write(&mut out, &req.caps).unwrap();
        assert_eq!(
            lines(&out),
            [
                "unpack ok\n".to_owned(),
                "ok refs/heads/a\n".to_owned(),
                format!(
                    "ng refs/heads/b {}\n",
                    RejectReason::NonFastForward.message()
                ),
                "0000".to_owned(),
            ]
        );
    }

    /// Invariant: with side-band the report is on band 1 and each rejection
    /// is also on band 2, even under `quiet` (S2); a refused pack rejects
    /// every ref with "unpacker error".
    #[test]
    fn sideband_report_repeats_rejections_on_band_2_even_when_quiet() {
        let req = ReceiveRequest {
            commands: vec![
                RefCommand {
                    name: name("refs/heads/a"),
                    old: ObjectId::ZERO,
                    new: oid(1),
                },
                RefCommand {
                    name: name("refs/heads/b"),
                    old: ObjectId::ZERO,
                    new: oid(1),
                },
            ],
            caps: ReceiveCaps {
                report_status: true,
                sideband: Some(SidebandMode::Large),
                quiet: true,
                ..Default::default()
            },
            ..Default::default()
        };
        let mut r = ReportStatus::new(&req);
        let unpack = UnpackError::MissingBase { id: oid(5) }.to_wire();
        r.unpack_failed(&unpack).unwrap();
        let mut out = Vec::new();
        r.write(&mut out, &req.caps).unwrap();
        let l = lines(&out);
        assert_eq!(
            l[0],
            format!("\x02aspen: unpack failed: missing delta base {}\n", oid(5))
        );
        assert_eq!(l[1], "\x02aspen: refs/heads/a: unpacker error\n");
        assert_eq!(l[2], "\x02aspen: refs/heads/b: unpacker error\n");
        let inner = l[3].strip_prefix('\x01').unwrap();
        assert_eq!(
            lines(inner.as_bytes()),
            [
                format!("unpack missing delta base {}\n", oid(5)),
                "ng refs/heads/a unpacker error\n".to_owned(),
                "ng refs/heads/b unpacker error\n".to_owned(),
                "0000".to_owned(),
            ]
        );
        assert_eq!(l[4], "0000");
        assert_eq!(l.len(), 5);
        // Surfaces are checked.
        assert_eq!(
            r.unpack_failed(&RejectReason::CasLost.to_wire()),
            Err(ReportError::WrongSurface(Surface::PushRef))
        );
        assert_eq!(
            RefStatus::from_wire(&unpack),
            Err(ReportError::WrongSurface(Surface::PushUnpack))
        );
        assert_eq!(
            RefStatus::from_wire(&RejectReason::CasLost.to_wire()),
            Ok(RefStatus::Rejected(RejectReason::CasLost.message().into()))
        );
    }

    /// Invariant: warnings, then notes, go on band 2 before the rejections,
    /// one pkt-line each and even under `quiet`, cannot forge report lines,
    /// and are dropped without side-band (the report itself is unchanged).
    #[test]
    fn notes_go_on_band_2_before_rejections_and_only_with_side_band() {
        let mut req = ReceiveRequest {
            commands: vec![RefCommand {
                name: name("refs/heads/a"),
                old: ObjectId::ZERO,
                new: oid(1),
            }],
            caps: ReceiveCaps {
                report_status: true,
                sideband: Some(SidebandMode::Large),
                quiet: true,
                ..Default::default()
            },
            ..Default::default()
        };
        let mut r = ReportStatus::new(&req);
        r.note("tiles/1_1.txt: bad\nok refs/heads/forged");
        r.note("second");
        r.warn("deleting a non-existent ref\nok refs/heads/forged");
        r.set(
            &name("refs/heads/a"),
            RefStatus::Rejected("2 problems".into()),
        )
        .unwrap();
        let mut out = Vec::new();
        r.write(&mut out, &req.caps).unwrap();
        let l = lines(&out);
        assert_eq!(
            l[0],
            "\x02warning: deleting a non-existent ref ok refs/heads/forged\n"
        );
        assert_eq!(l[1], "\x02aspen: tiles/1_1.txt: bad ok refs/heads/forged\n");
        assert_eq!(l[2], "\x02aspen: second\n");
        assert_eq!(l[3], "\x02aspen: refs/heads/a: 2 problems\n");
        assert!(l[4].starts_with('\x01'));
        req.caps.sideband = None;
        let mut plain = Vec::new();
        r.write(&mut plain, &req.caps).unwrap();
        assert_eq!(
            lines(&plain),
            ["unpack ok\n", "ng refs/heads/a 2 problems\n", "0000"]
        );
    }

    #[test]
    fn reasons_cannot_forge_report_lines() {
        let req = ReceiveRequest {
            commands: vec![RefCommand {
                name: name("refs/heads/a"),
                old: ObjectId::ZERO,
                new: oid(1),
            }],
            caps: ReceiveCaps {
                report_status: true,
                ..Default::default()
            },
            ..Default::default()
        };
        let mut r = ReportStatus::new(&req);
        r.set(
            &name("refs/heads/a"),
            RefStatus::Rejected("no\nok refs/heads/a".into()),
        )
        .unwrap();
        let mut out = Vec::new();
        r.write(&mut out, &req.caps).unwrap();
        assert_eq!(lines(&out)[1], "ng refs/heads/a no ok refs/heads/a\n");
    }

    #[test]
    fn progress_and_fatal_follow_the_negotiated_framing() {
        let loud = ReceiveCaps {
            sideband: Some(SidebandMode::Large),
            ..Default::default()
        };
        let quiet = ReceiveCaps {
            quiet: true,
            ..loud.clone()
        };
        let mut out = Vec::new();
        write_progress(&mut out, &quiet, "x");
        write_progress(&mut out, &ReceiveCaps::default(), "x");
        assert!(out.is_empty());
        write_progress(&mut out, &loud, "indexing");
        assert_eq!(lines(&out), ["\x02indexing\n"]);
        let mut out = Vec::new();
        write_fatal(&mut out, None, "repository not found");
        assert_eq!(lines(&out), ["ERR repository not found\n"]);
        let mut out = Vec::new();
        write_fatal(&mut out, Some(&loud), "disk full");
        assert_eq!(lines(&out), ["\x03disk full\n", "0000"]);
    }

    /// Every command of a parsed request gets a status and the report is
    /// written under every framing; this must never fail or panic.
    fn report_everything(req: &ReceiveRequest, seed: u64) {
        let mut r = ReportStatus::new(req);
        for (i, c) in req.commands.iter().enumerate() {
            let status = if (seed + i as u64).is_multiple_of(2) {
                RefStatus::Ok
            } else {
                RefStatus::Rejected("reason\nok forged".repeat(500))
            };
            r.set(&c.name, status).unwrap();
        }
        for sideband in [None, Some(SidebandMode::Small), Some(SidebandMode::Large)] {
            for report_status in [false, true] {
                let caps = ReceiveCaps {
                    sideband,
                    report_status,
                    ..req.caps.clone()
                };
                r.write(&mut Vec::new(), &caps).unwrap();
            }
        }
        if seed.is_multiple_of(3) {
            r.unpack_failed(&aspen_core::error::UnpackError::MissingBase { id: oid(1) }.to_wire())
                .unwrap();
            r.write(&mut Vec::new(), &req.caps).unwrap();
        }
    }

    /// Invariant: the parser is total: arbitrary packets give a request or
    /// an error, never a panic, and the report for every request it
    /// accepts can be written.
    #[test]
    fn parser_is_total_on_arbitrary_packets() {
        for seed in 0..5_000u64 {
            let bytes = crate::pkt::tests::noise(seed, 120);
            let mut p = RequestParser::new(advert(), ReceiveLimits::default());
            for chunk in bytes.chunks(1 + (seed % 60) as usize) {
                let pkt = match chunk[0] % 7 {
                    0 => Packet::Flush,
                    1 => Packet::Delim,
                    _ => Packet::Data(chunk),
                };
                match p.packet(pkt) {
                    Err(_) => break,
                    Ok(Some(req)) => {
                        report_everything(&req, seed);
                        break;
                    }
                    Ok(None) => {}
                }
            }
        }
    }

    /// Invariant: the same, with well-formed command lines around arbitrary
    /// name bytes (control characters, invalid UTF-8, any length up to a
    /// whole pkt-line), so the name checks themselves are exercised.
    #[test]
    fn arbitrary_ref_names_parse_or_refuse_and_always_report() {
        let z = ObjectId::ZERO;
        let mut accepted = 0;
        for seed in 0..3_000u64 {
            let len = match seed % 4 {
                0 => 1 + (seed % 40) as usize,
                1 => MAX_REF_NAME_LEN - 20 + (seed % 40) as usize,
                2 => pkt::MAX_DATA_LEN - 120,
                _ => 1 + (seed % 300) as usize,
            };
            let noise = crate::pkt::tests::noise(seed, len);
            let mut p = RequestParser::new(advert(), ReceiveLimits::default());
            let mut result = Ok(None);
            for i in 0..3u8 {
                let mut line = format!("{z} {} ", oid(i + 1)).into_bytes();
                if seed % 5 == 0 {
                    line.extend_from_slice(&noise);
                } else {
                    // Mostly printable, so many names get through.
                    line.extend_from_slice(b"refs/");
                    line.extend(
                        noise
                            .iter()
                            .map(|b| if seed % 7 == 0 { *b } else { b'a' + b % 26 }),
                    );
                }
                line.push(b'0' + i);
                if i == 0 {
                    line.extend_from_slice(b"\0report-status side-band-64k");
                }
                line.push(b'\n');
                result = p.packet(Packet::Data(&line));
                if result.is_err() {
                    break;
                }
            }
            if result.is_ok() {
                let req = p.packet(Packet::Flush).unwrap().unwrap();
                accepted += 1;
                report_everything(&req, seed);
            }
        }
        assert!(accepted > 1_000, "too few names got through: {accepted}");
    }
}
