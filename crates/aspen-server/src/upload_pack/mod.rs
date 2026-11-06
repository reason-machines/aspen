//! `git-upload-pack` over smart HTTP and SSH (`docs/PLAN.md`, fetch path).
//!
//! - `GET info/refs?service=git-upload-pack` answers a v2 client with the
//!   capability advertisement, and a v0 or v1 client with the v0 ref
//!   advertisement (`v0`: `HEAD`, trunk and the scoped ref).
//! - `POST git-upload-pack` carries one command: [`ls_refs`] or
//!   [`fetch`]. Each POST is parsed with `aspen_proto::v2::CommandParser`
//!   and answered in full; smart HTTP is stateless, so a negotiation round
//!   is one POST. A v0 or v1 POST is one negotiation round of
//!   [`aspen_proto::upload_v0`], served by `v0` with the same planner,
//!   have rules and stitched delivery.
//!
//! Over SSH ([`crate::ssh`], through `serve_connection`) one command carries the whole exchange:
//! in v2 the capability advertisement, then any number of commands on the
//! same stream, each parsed and answered exactly as one HTTP POST; in v0
//! and v1 the ref advertisement, then the stateful negotiation of
//! `v0::serve_stateful`.
//!
//! Errors before the pack are `ERR <message>` lines (Git prints
//! `remote error: <message>`); errors once the `packfile` section has
//! started are band-3 messages. Their text comes from `aspen_core::error`.

mod deliver;
pub mod exact;
pub mod fetch;
mod graph;
pub mod heat;
pub mod ls_refs;
mod resolve;
mod shallow;
pub(crate) mod to_file;
mod v0;

use aspen_core::blob::BlobStore;
use aspen_core::error::{FetchError, Surface, ToWire, WireError};
use aspen_core::meta::MetaStore;
use aspen_core::repo::Repo;
use aspen_proto::pkt::{self, Packet};
use aspen_proto::service::{ProtocolVersion, Service, UploadVersion, write_info_refs_header};
use aspen_proto::v2::{Command, CommandParser, V2Advertisement};
use axum::body::Body;
use axum::http::request::Parts;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::Response;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::access::{Authorized, GitRoute};
use crate::conn::{self, PktInput};
use crate::{Server, body, plain};

/// Why a fetch ended before its pack.
#[derive(Debug)]
pub(crate) enum Failure {
    /// A store failed, data broke an invariant, or a commit cannot be
    /// resolved yet: [`FetchError`]'s own wire form (and alerts).
    Fetch(FetchError),
    /// The request is refused for a reason that is neither a store failure
    /// nor corruption: Git's own refusal text (shallow edge cases), or a
    /// configured limit. Never an alert.
    Refused {
        /// The `ERR` text.
        message: String,
        /// Whether the same request can succeed later unchanged.
        retryable: bool,
    },
}

impl Failure {
    /// A refusal the same request will meet again.
    pub(crate) fn refused(message: impl Into<String>) -> Self {
        Self::Refused {
            message: message.into(),
            retryable: false,
        }
    }

    pub(crate) fn to_wire(&self) -> WireError {
        match self {
            Self::Fetch(e) => e.to_wire(),
            Self::Refused { message, retryable } => WireError {
                surface: Surface::FetchErr,
                message: message.clone(),
                retryable: *retryable,
                alert: false,
            },
        }
    }
}

impl From<FetchError> for Failure {
    fn from(e: FetchError) -> Self {
        Self::Fetch(e)
    }
}

/// The advertisement this server sends.
pub(crate) fn advertisement<M, B>(server: &Server<M, B>) -> V2Advertisement {
    let config = server.config();
    let mut advert = V2Advertisement::new(config.agent.clone());
    advert.packfile_uris = config.packfile_uris;
    // Never advertised in v1: over smart HTTP the advertisement precedes the
    // request, so it cannot depend on that fetch's plan (decisions log, B2).
    advert.no_ref_delta = false;
    advert
}

fn requested_version(headers: &HeaderMap) -> ProtocolVersion {
    ProtocolVersion::requested(headers.get("git-protocol").and_then(|v| v.to_str().ok()))
}

/// `GET info/refs?service=git-upload-pack`.
pub(crate) async fn advertise<M, B>(
    server: &Server<M, B>,
    repo: &Repo,
    route: &GitRoute,
    parts: &Parts,
) -> Response
where
    M: MetaStore + Clone + 'static,
    B: BlobStore + Clone + 'static,
{
    let mut out = Vec::new();
    match UploadVersion::answering(requested_version(&parts.headers)) {
        UploadVersion::V2 => {
            write_info_refs_header(&mut out, Service::UploadPack, ProtocolVersion::V2);
            advertisement(server).write(&mut out);
        }
        UploadVersion::V0(v) => {
            write_info_refs_header(&mut out, Service::UploadPack, v.into());
            let mut refs = Vec::new();
            match v0::advertise(server, repo, route, v, &mut refs).await {
                Ok(()) => out.extend_from_slice(&refs),
                Err(e) => pkt::write_err(&mut out, &e.to_wire().message),
            }
        }
    }
    git_response(
        Service::UploadPack.advertisement_content_type(),
        Body::from(out),
    )
}

/// `POST git-upload-pack` by `caller`.
pub(crate) async fn serve<M, B>(
    server: &Server<M, B>,
    repo: &Repo,
    caller: &Authorized,
    parts: &Parts,
    body: Body,
) -> Response
where
    M: MetaStore + Clone + 'static,
    B: BlobStore + Clone + 'static,
{
    let content_type = parts
        .headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok());
    if content_type != Some(Service::UploadPack.request_content_type()) {
        return plain(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "expected application/x-git-upload-pack-request",
        );
    }
    let result =
        |out: Vec<u8>| git_response(Service::UploadPack.result_content_type(), Body::from(out));
    let version = UploadVersion::answering(requested_version(&parts.headers));
    let request = match body::read(&parts.headers, body, server.config().max_request_body).await {
        Ok(b) => b,
        Err(e) => {
            let mut out = Vec::new();
            pkt::write_err(&mut out, &e.to_string());
            return result(out);
        }
    };
    if matches!(version, UploadVersion::V0(_)) {
        return git_response(
            Service::UploadPack.result_content_type(),
            v0::serve(server, repo, caller, &request).await,
        );
    }
    let command = match parse_command(server, &request) {
        Ok(c) => c,
        Err(message) => {
            let mut out = Vec::new();
            pkt::write_err(&mut out, &message);
            return result(out);
        }
    };
    match command {
        Command::LsRefs(req) => result(ls_refs::respond(server, repo, &req).await),
        Command::Fetch(args) => git_response(
            Service::UploadPack.result_content_type(),
            fetch::respond(server, repo, caller, args).await,
        ),
        Command::End => result(Vec::new()),
    }
}

/// One `git-upload-pack` over a connection (SSH) by `caller` (whose
/// repository and route it serves), for a client that requested
/// `requested` (its `GIT_PROTOCOL`), after access was decided.
///
/// - **v2:** the capability advertisement, then commands until the client
///   ends the session (a lone flush, or end of input). Each command is
///   parsed with the same limits and answered by the same code as one HTTP
///   POST. `next_command` runs before every command after the first and
///   decides it as HTTP decides each POST (the caller identifies the key
///   again, authorizes the repository and charges the rate limit); an
///   `Err` is sent as `ERR` and ends the session, so a long session can
///   neither outlive its key's revocation nor pay less than HTTP.
/// - **v0 and v1:** the ref advertisement (`HEAD`, trunk and the scoped
///   ref, as over HTTP), then one stateful fetch.
///
/// Returns [`SessionEnd::Refused`] when the session ended with an `ERR`
/// line (a refused command, a malformed request, or a ref advertisement
/// that could not be read), so the caller can exit with a failure status.
/// An I/O error means the client is gone or the response could not be
/// finished; the caller ends the connection.
pub(crate) async fn serve_connection<M, B, R, W, N, F>(
    server: &Server<M, B>,
    caller: &Authorized,
    requested: ProtocolVersion,
    mut input: PktInput<R>,
    out: &mut W,
    mut next_command: N,
) -> std::io::Result<SessionEnd>
where
    N: FnMut() -> F,
    F: Future<Output = Result<(), String>>,
    M: MetaStore + Clone + 'static,
    B: BlobStore + Clone + 'static,
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin,
{
    let (repo, route) = (&caller.repo, &caller.route);
    let mut buf = Vec::new();
    let version = match UploadVersion::answering(requested) {
        UploadVersion::V2 => None,
        UploadVersion::V0(v) => Some(v),
    };
    if let Some(v) = version {
        if let Err(e) = v0::advertise(server, repo, route, v, &mut buf).await {
            buf.clear();
            pkt::write_err(&mut buf, &e.to_wire().message);
            return refused(out, &buf).await;
        }
        conn::send(out, &buf).await?;
        v0::serve_stateful(server, repo, caller, input, out).await?;
        return Ok(SessionEnd::Done);
    }
    advertisement(server).write(&mut buf);
    conn::send(out, &buf).await?;
    let mut first = true;
    loop {
        buf.clear();
        let mut parser = CommandParser::new(advertisement(server), server.config().v2_limits);
        let command = loop {
            // The client ended the session without a final flush.
            let Some(frame) = input.next().await? else {
                return Ok(SessionEnd::Done);
            };
            match parser.packet(conn::decode(frame)?) {
                Ok(Some(command)) => break command,
                Ok(None) => {}
                Err(e) => {
                    pkt::write_err(&mut buf, &e.to_string());
                    return refused(out, &buf).await;
                }
            }
        };
        if matches!(command, Command::End) {
            return Ok(SessionEnd::Done);
        }
        if !std::mem::take(&mut first)
            && let Err(message) = next_command().await
        {
            pkt::write_err(&mut buf, &message);
            return refused(out, &buf).await;
        }
        match command {
            Command::LsRefs(req) => {
                conn::send(out, &ls_refs::respond(server, repo, &req).await).await?;
            }
            Command::Fetch(args) => {
                conn::send_body(out, fetch::respond(server, repo, caller, args).await).await?;
            }
            Command::End => unreachable!("handled above"),
        }
    }
}

/// How a [`serve_connection`] session ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SessionEnd {
    /// The client finished the exchange.
    Done,
    /// The server sent an `ERR` line and stopped.
    Refused,
}

async fn refused<W: AsyncWrite + Unpin>(out: &mut W, err: &[u8]) -> std::io::Result<SessionEnd> {
    conn::send(out, err).await?;
    Ok(SessionEnd::Refused)
}

/// Parses one command request from the whole POST body.
fn parse_command<M, B>(server: &Server<M, B>, mut buf: &[u8]) -> Result<Command, String> {
    let mut parser = CommandParser::new(advertisement(server), server.config().v2_limits);
    loop {
        let Some((packet, used)) = pkt::decode(buf).map_err(|e| format!("protocol error: {e}"))?
        else {
            return Err("protocol error: the request ended before its final flush".into());
        };
        buf = &buf[used..];
        if let Some(command) = parser.packet(packet).map_err(|e| e.to_string())? {
            if !buf.is_empty() {
                // One command per POST; a trailing bare flush is the
                // client ending the connection (v2 over HTTP never sends
                // two commands in one body, but tolerate the end marker).
                match pkt::decode(buf) {
                    Ok(Some((Packet::Flush, n))) if n == buf.len() => {}
                    _ => return Err("protocol error: data after the command".into()),
                }
            }
            return Ok(command);
        }
    }
}

fn git_response(content_type: &'static str, body: Body) -> Response {
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, content_type)
        .header(
            header::CACHE_CONTROL,
            "no-cache, max-age=0, must-revalidate",
        )
        .header(header::EXPIRES, "Fri, 01 Jan 1980 00:00:00 GMT")
        .header(header::PRAGMA, "no-cache")
        .body(body)
        .expect("valid response")
}
