//! One SSH `exec` request: the Git command it names, run over the
//! channel's data stream, then the exit status and the end of the channel.

use std::io;
use std::sync::Arc;
use std::time::Instant;

use aspen_auth::{Identity, Need, RequestClass, SshKeyStore, TokenStore};
use aspen_core::auth::Credential;
use aspen_core::blob::BlobStore;
use aspen_core::meta::MetaStore;
use aspen_proto::pkt;
use aspen_proto::service::{ProtocolVersion, Service};
use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use russh::ChannelMsg;
use russh::server::Msg;
use russh::{ChannelReadHalf, ChannelWriteHalf};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio_util::io::{StreamReader, SyncIoBridge};

use super::command::{self, CommandError};
use super::{Shared, identify_now};
use crate::access::{Endpoint, GitRoute, PushMeter, Refusal, subject_key};
use crate::admission::Slot;
use crate::conn::{self, PktInput};
use crate::upload_pack;

/// Exit status of a command that ran to the end.
const OK: u32 = 0;
/// Exit status of a refused or failed command.
const FAILED: u32 = 1;

/// What a client that runs something other than Git is told, on stderr.
const NOT_GIT: &str = "aspen: this server runs only git-upload-pack and git-receive-pack\n";

/// Runs `command` on the channel and ends the channel: exit status, EOF,
/// close. `identity` was resolved for this command (so a key revoked since
/// the connection authenticated is already refused), with the verified
/// key it was resolved from, which every later v2 command in the session
/// is identified with again, charged to `address`'s failed-authentication
/// bucket like every key lookup (issue #243).
pub(super) async fn run<M, B, T, K>(
    shared: Arc<Shared<M, B, T, K>>,
    identity: Result<(Arc<Identity>, Credential), Refusal>,
    address: &str,
    read: ChannelReadHalf,
    write: ChannelWriteHalf<Msg>,
    command: Vec<u8>,
    git_protocol: Option<String>,
) where
    M: MetaStore + Clone + 'static,
    B: BlobStore + Clone + 'static,
    T: TokenStore + 'static,
    K: SshKeyStore + 'static,
{
    let mut stdout = write.make_writer();
    let mut stderr = write.make_writer_ext(Some(1));
    let status = serve(
        &shared,
        identity,
        address,
        &command,
        git_protocol.as_deref(),
        read,
        &mut stdout,
        &mut stderr,
    )
    .await;
    // The client may be gone; there is nobody to report a failure to.
    let _ = stdout.flush().await;
    let _ = stderr.flush().await;
    let _ = write.exit_status(status).await;
    let _ = write.eof().await;
    let _ = write.close().await;
}

/// The channel's data, as a byte stream that ends at EOF.
fn data(read: ChannelReadHalf) -> impl Stream<Item = io::Result<Bytes>> + Send + Unpin + 'static {
    Box::pin(futures_util::stream::unfold(read, |mut read| async move {
        loop {
            match read.wait().await? {
                ChannelMsg::Data { data } => return Some((Ok(data), read)),
                ChannelMsg::Eof | ChannelMsg::Close => return None,
                // Extended data, window changes, late requests: not input.
                _ => {}
            }
        }
    }))
}

#[allow(clippy::too_many_arguments)]
async fn serve<M, B, T, K, W, E>(
    shared: &Arc<Shared<M, B, T, K>>,
    identity: Result<(Arc<Identity>, Credential), Refusal>,
    address: &str,
    command: &[u8],
    git_protocol: Option<&str>,
    read: ChannelReadHalf,
    stdout: &mut W,
    stderr: &mut E,
) -> u32
where
    M: MetaStore + Clone + 'static,
    B: BlobStore + Clone + 'static,
    T: TokenStore + 'static,
    K: SshKeyStore + 'static,
    W: AsyncWrite + Unpin,
    E: AsyncWrite + Unpin,
{
    // A command that is refused before `admit` (not Git, a bad path)
    // still cost a key lookup, so it costs a request from the subject's
    // bucket too (D4): a loop of garbage commands is rate limited like
    // real ones.
    let charge_refused = || {
        if let Ok((identity, _)) = &identity
            && let Some(key) = subject_key(identity)
        {
            let _ = shared
                .access
                .charge(RequestClass::Read, &key, Instant::now());
        }
    };
    let command = match command::parse(command) {
        Ok(c) => c,
        Err(e) => {
            charge_refused();
            match e {
                CommandError::NotGit => {
                    let _ = conn::send(stderr, NOT_GIT.as_bytes()).await;
                    return FAILED;
                }
                // A Git client reads `ERR` as "remote error: <text>".
                CommandError::BadPath => {
                    return refuse(stdout, &Refusal::Auth(not_found())).await;
                }
            }
        }
    };
    let endpoint = match command.service {
        Service::UploadPack => Endpoint::UploadPack,
        Service::ReceivePack => Endpoint::ReceivePack,
    };
    let Ok(route) = GitRoute::from_repo_path(&command.path, endpoint) else {
        charge_refused();
        return refuse(stdout, &Refusal::Auth(not_found())).await;
    };
    let (identity, credential) = match identity {
        Ok(identified) => identified,
        Err(refusal) => return refuse(stdout, &refusal).await,
    };
    // Every SSH caller is signed in: there is no anonymous SSH.
    let key = subject_key(&identity).unwrap_or_else(|| "ssh:anonymous".to_owned());
    let (authorized, meter) = match shared
        .access
        .admit(identity, route, &key, None, Instant::now())
        .await
    {
        Ok(admitted) => admitted,
        Err(refusal) => return refuse(stdout, &refusal).await,
    };
    let requested = ProtocolVersion::requested(git_protocol);
    let result = match command.service {
        Service::UploadPack => {
            let input = PktInput::new(StreamReader::new(data(read)));
            let repo = &authorized.route.repo;
            upload_pack::serve_connection(
                &shared.server,
                &authorized,
                requested,
                input,
                stdout,
                // Each later v2 command is decided as one HTTP POST is:
                // identify the key (a failed check drains the address's
                // failed-authentication bucket), authorize by name, charge
                // the bucket.
                || async {
                    let identity = identify_now(&shared.access, &credential, address)
                        .await
                        .map_err(|r| r.message())?;
                    identity
                        .authorize(repo, Need::Read)
                        .map_err(|e| Refusal::Auth(e).message())?;
                    shared
                        .access
                        .charge(RequestClass::Read, &key, Instant::now())
                        .map_err(|e| e.to_string())
                },
            )
            .await
            .map(|end| end == upload_pack::SessionEnd::Done)
        }
        Service::ReceivePack => {
            let Some(svc) = shared.server.receive_pack() else {
                return refuse_text(
                    stdout,
                    "push (git-receive-pack) is not configured on this server",
                )
                .await;
            };
            let scope = authorized.route.scope.as_ref();
            let advert = match svc.advertise_refs(&authorized.repo, scope, requested).await {
                Ok(a) => a,
                Err(_) => return refuse(stdout, &Refusal::Unavailable).await,
            };
            if conn::send(stdout, &advert).await.is_err() {
                return FAILED;
            }
            let body = SyncIoBridge::new(metered(read, meter, authorized.slot().clone()));
            match svc
                .push(&authorized.repo, scope, &authorized, authorized.push, body)
                .await
            {
                Ok(outcome) => conn::send(stdout, &outcome.response).await.map(|()| true),
                // A store failed before the report could be built: the
                // client sees the connection end without one.
                Err(_) => Err(io::Error::other("push failed before its report")),
            }
        }
    };
    match result {
        Ok(true) => OK,
        // Refused with an `ERR` line, or the client or a store failed.
        Ok(false) | Err(_) => FAILED,
    }
}

/// The push body: the channel's data, every byte charged to the push's
/// quota (if any) before receive-pack reads it. It holds the push's slot
/// until the blocking thread reading it drops it.
fn metered(
    read: ChannelReadHalf,
    meter: Option<PushMeter>,
    slot: Slot,
) -> impl AsyncRead + Send + Unpin + 'static {
    let mut meter = meter;
    StreamReader::new(data(read).map(move |chunk| {
        let _held = &slot;
        let chunk = chunk?;
        if let Some(m) = meter.as_mut() {
            m.charge(chunk.len() as u64).map_err(io::Error::other)?;
        }
        Ok::<_, io::Error>(chunk)
    }))
}

fn not_found() -> aspen_core::error::AuthError {
    aspen_core::error::AuthError::NotFound
}

async fn refuse<W: AsyncWrite + Unpin>(stdout: &mut W, refusal: &Refusal) -> u32 {
    refuse_text(stdout, &refusal.message()).await
}

/// Refuses the command before its advertisement with an `ERR` line, which
/// a Git client prints as `fatal: remote error: <text>`.
async fn refuse_text<W: AsyncWrite + Unpin>(stdout: &mut W, text: &str) -> u32 {
    let mut out = Vec::new();
    pkt::write_err(&mut out, text);
    let _ = conn::send(stdout, &out).await;
    FAILED
}
