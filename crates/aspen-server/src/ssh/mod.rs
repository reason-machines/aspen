//! Git over SSH (node C3): `git clone ssh://git@host/acme/mono.git` and
//! `git@host:acme/mono.git`, with the same repositories, branch scopes,
//! access decisions, limits and protocol code as smart HTTP.
//!
//! # A connection
//!
//! 0. **Admission.** A connection is accepted only while fewer than
//!    [`SshConfig::max_connections`] are open and its address has fewer
//!    than [`SshConfig::max_unauthenticated_per_address`] connections that
//!    have not authenticated yet; otherwise the socket is closed before
//!    the key exchange costs anything. A connection that has not
//!    authenticated within [`SshConfig::auth_timeout`] of being accepted
//!    is disconnected (OpenSSH's `LoginGraceTime`), whatever it sends.
//!    The address is the TCP peer, so a load balancer in front must
//!    preserve client addresses (decisions log, D4).
//! 1. **Key exchange** with the server's host keys ([`SshConfig::host_keys`]).
//! 2. **Authentication: public keys only.** A key is accepted when it is
//!    registered ([`aspen_auth::ssh`], [`keys::SqlSshKeys`]), live, and of an
//!    accepted type (Ed25519 or ECDSA); the SSH layer has checked the
//!    client's signature before Aspen sees the key. Unknown keys are
//!    refused at the offer, so the client moves on to its next key. Each
//!    fingerprint is looked up at most once per connection (russh does not
//!    count an accepted unsigned offer as an attempt, so a repeated offer
//!    of a registered key would otherwise be a store read each time), at
//!    most [`SshConfig::max_auth_attempts`] distinct keys may be offered,
//!    and every refused key is charged to the address's failed-
//!    authentication bucket ([`aspen_auth::LimitsConfig::auth_failures`]);
//!    while that is empty, offers are refused without a lookup.
//!    Passwords, keyboard-interactive, host-based and `none` are refused,
//!    and so are OpenSSH certificates. The user name is ignored (Git sends
//!    `git`), as the user name of HTTP Basic is. There is no anonymous SSH.
//! 3. **Sessions.** Up to [`SshConfig::max_sessions`] session channels per
//!    connection; no other channel type (port forwarding, X11, agent) and
//!    no pty or subsystem. A channel may set one environment variable,
//!    `GIT_PROTOCOL`, which selects the protocol version as the
//!    `Git-Protocol` header does over HTTP.
//! 4. **One command per session** (the `command` module): `git-upload-pack <path>`
//!    or `git-receive-pack <path>`. For each command the key is identified
//!    again, so a key revoked or a grant changed while a connection (an
//!    OpenSSH control master) stays open applies to its next command. A
//!    protocol v2 session runs several commands on one stream; before each
//!    one after the first the key is identified and the repository
//!    authorized again, as each HTTP POST is. Each of these re-checks is
//!    charged like an offer in step 2 (`identify_now`, through
//!    [`Access::identify`]): a failed one drains the address's
//!    failed-authentication bucket, and while that is empty the command is
//!    refused without a lookup, so a connection that keeps running
//!    commands after its key was revoked is bounded too (issue #243).
//!    Then the same [`Access::admit`] as an HTTP request runs (authorize by
//!    name, rate limit per subject, look the repository up, push quota), and
//!    a refusal is an `ERR` line, which Git prints as `fatal: remote error:
//!    <text>`, with exit status 1. A shell request gets a one-line greeting
//!    on stderr and exit status 1, so `ssh -T git@host` shows which subject
//!    a key acts as.
//! 5. **The exchange** runs over the channel's data stream:
//!    `git-upload-pack` with `upload_pack::serve_connection` (v2 commands,
//!    or the stateful v0/v1 negotiation), `git-receive-pack` with the
//!    advertisement, then the push and its report through B1's
//!    [`crate::receive::ReceivePack`]. Then the exit status (0 when the
//!    exchange ran to its end), EOF, and the channel closes.

pub(crate) mod command;
mod exec;
pub mod keys;

use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use aspen_auth::{Identity, SshKeyStore, SshPublicKey, TokenStore};
use aspen_core::auth::Credential;
use aspen_core::blob::BlobStore;
use aspen_core::error::AuthError;
use aspen_core::meta::MetaStore;
use russh::keys::PublicKeyBase64;
use russh::server::{Auth, ChannelOpenHandle, Msg, PublicKeyOffer, Session, VerifiedPublicKey};
use russh::{Channel, ChannelId, ChannelOpenFailure, MethodKind, MethodSet, Pty, SshId};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, oneshot};

pub use russh::keys::PrivateKey;

use crate::Server;
use crate::access::{Access, Refusal, ip_key};

/// SSH server settings.
#[derive(Clone)]
pub struct SshConfig {
    /// The server's host keys. Clients pin the first one they see
    /// (`known_hosts`), so a deployment keeps them stable across restarts
    /// and instances. Use Ed25519 or ECDSA keys: RSA needs the `rsa` crate,
    /// which Aspen does not link (decisions log, C3).
    pub host_keys: Vec<PrivateKey>,
    /// A connection that sends nothing for this long is closed. A clone
    /// that is still streaming keeps the connection alive (the client
    /// acknowledges data with window adjustments).
    pub inactivity_timeout: Duration,
    /// Every refused authentication attempt takes at least this long, so
    /// the answer's timing does not show whether a key is registered.
    pub auth_rejection_time: Duration,
    /// Authentication attempts per connection before it is closed. An
    /// agent offering many keys spends one attempt per key.
    pub max_auth_attempts: usize,
    /// Session channels open at once per connection. Git opens one; an
    /// OpenSSH control master may run a few commands at once.
    pub max_sessions: usize,
    /// Connections open at once, authenticated or not. Further
    /// connections are closed as soon as they are accepted.
    pub max_connections: usize,
    /// Connections from one address (an IPv4 address or an IPv6 /64) that
    /// may be open at once before they have authenticated. Authenticated
    /// connections do not count, so a fleet behind one NAT address is
    /// limited only by its subjects' request rates.
    pub max_unauthenticated_per_address: usize,
    /// How long a connection may take from being accepted to
    /// authenticating, key exchange included.
    pub auth_timeout: Duration,
}

impl SshConfig {
    /// Defaults with these host keys: 10 minutes of inactivity, 1 s per
    /// refused authentication, 20 attempts, 8 sessions per connection,
    /// 4,096 connections, 10 unauthenticated connections per address and
    /// 30 s to authenticate.
    pub fn new(host_keys: Vec<PrivateKey>) -> Self {
        Self {
            host_keys,
            inactivity_timeout: Duration::from_secs(600),
            auth_rejection_time: Duration::from_secs(1),
            max_auth_attempts: 20,
            max_sessions: 8,
            max_connections: 4096,
            max_unauthenticated_per_address: 10,
            auth_timeout: Duration::from_secs(30),
        }
    }
}

impl fmt::Debug for SshConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SshConfig")
            .field(
                "host_keys",
                &self
                    .host_keys
                    .iter()
                    .map(|k| k.algorithm().to_string())
                    .collect::<Vec<_>>(),
            )
            .field("inactivity_timeout", &self.inactivity_timeout)
            .field("auth_rejection_time", &self.auth_rejection_time)
            .field("max_auth_attempts", &self.max_auth_attempts)
            .field("max_sessions", &self.max_sessions)
            .field("max_connections", &self.max_connections)
            .field(
                "max_unauthenticated_per_address",
                &self.max_unauthenticated_per_address,
            )
            .field("auth_timeout", &self.auth_timeout)
            .finish()
    }
}

/// Why an SSH server could not be set up.
#[derive(Debug, thiserror::Error)]
pub enum SshSetupError {
    /// No host key was given.
    #[error("the SSH server needs at least one host key")]
    NoHostKey,
    /// A host key file could not be read or decoded.
    #[error("host key {path}: {reason}")]
    HostKey {
        /// The file.
        path: String,
        /// What went wrong.
        reason: String,
    },
}

/// Reads an unencrypted OpenSSH private key file (what `ssh-keygen -t
/// ed25519 -N '' -f <path>` writes) for [`SshConfig::host_keys`].
///
/// It does not check the file's permissions. `aspen serve` reads
/// `ASPEN_SSH_HOST_KEY_FILE` through its own secret-file check instead,
/// which refuses a key other users may read (issue #125); an embedder that
/// calls this should check the mode itself.
pub fn load_host_key(path: &Path) -> Result<PrivateKey, SshSetupError> {
    russh::keys::load_secret_key(path, None).map_err(|e| SshSetupError::HostKey {
        path: path.display().to_string(),
        reason: e.to_string(),
    })
}

/// What every connection shares: the Git server and access control.
pub(crate) struct Shared<M, B, T, K> {
    pub(crate) server: Server<M, B>,
    pub(crate) access: Arc<Access<M, T, K>>,
    max_sessions: usize,
    max_offers: usize,
}

/// The SSH transport in front of a [`Server`]. Cheap to clone.
pub struct SshServer<M, B, T, K> {
    shared: Arc<Shared<M, B, T, K>>,
    config: Arc<russh::server::Config>,
    admission: Arc<Admission>,
}

impl<M, B, T, K> Clone for SshServer<M, B, T, K> {
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
            config: Arc::clone(&self.config),
            admission: Arc::clone(&self.admission),
        }
    }
}

/// Which connections may open (module docs, step 0).
struct Admission {
    open: Arc<Semaphore>,
    per_address: usize,
    unauthenticated: Arc<Mutex<HashMap<String, usize>>>,
    auth_timeout: Duration,
}

/// One unauthenticated connection counted against its address, until it
/// authenticates or closes.
struct Pending {
    address: String,
    counts: Arc<Mutex<HashMap<String, usize>>>,
}

impl Drop for Pending {
    fn drop(&mut self) {
        let mut counts = self.counts.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(n) = counts.get_mut(&self.address) {
            *n -= 1;
            if *n == 0 {
                counts.remove(&self.address);
            }
        }
    }
}

impl Admission {
    /// A connection from `address` may open: the permit it holds while
    /// open and its unauthenticated slot. `None` when either limit is
    /// reached.
    fn admit(&self, address: &str) -> Option<(OwnedSemaphorePermit, Pending)> {
        let permit = Arc::clone(&self.open).try_acquire_owned().ok()?;
        let mut counts = self
            .unauthenticated
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let n = counts.entry(address.to_owned()).or_insert(0);
        if *n >= self.per_address {
            return None;
        }
        *n += 1;
        drop(counts);
        Some((
            permit,
            Pending {
                address: address.to_owned(),
                counts: Arc::clone(&self.unauthenticated),
            },
        ))
    }
}

impl<M, B, T, K> fmt::Debug for SshServer<M, B, T, K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SshServer").finish_non_exhaustive()
    }
}

impl<M, B, T, K> SshServer<M, B, T, K>
where
    M: MetaStore + Clone + 'static,
    B: BlobStore + Clone + 'static,
    T: TokenStore + 'static,
    K: SshKeyStore + 'static,
{
    /// Serves `server`'s repositories over SSH, deciding access with
    /// `access` (share the HTTP router's, so a subject's HTTP requests and
    /// SSH commands draw on the same rate-limit buckets). `access` needs an
    /// SSH key store ([`aspen_auth::AuthService::with_ssh_keys`]); with
    /// [`aspen_auth::NoSshKeys`] every key is refused. Pushes are served
    /// when `server` has a receive-pack configuration.
    pub fn new(
        server: Server<M, B>,
        access: Arc<Access<M, T, K>>,
        config: SshConfig,
    ) -> Result<Self, SshSetupError> {
        if config.host_keys.is_empty() {
            return Err(SshSetupError::NoHostKey);
        }
        let russh = russh::server::Config {
            server_id: SshId::Standard(Cow::Owned(format!(
                "SSH-2.0-Aspen_{}",
                aspen_core::VERSION
            ))),
            methods: MethodSet::from(&[MethodKind::PublicKey][..]),
            auth_rejection_time: config.auth_rejection_time,
            // OpenSSH clients start with a `none` attempt to learn the
            // methods; answering it at once leaks nothing.
            auth_rejection_time_initial: Some(Duration::ZERO),
            keys: config.host_keys,
            max_auth_attempts: config.max_auth_attempts,
            inactivity_timeout: Some(config.inactivity_timeout),
            nodelay: true,
            ..Default::default()
        };
        Ok(Self {
            shared: Arc::new(Shared {
                server,
                access,
                max_sessions: config.max_sessions.max(1),
                max_offers: config.max_auth_attempts.max(1),
            }),
            config: Arc::new(russh),
            admission: Arc::new(Admission {
                open: Arc::new(Semaphore::new(config.max_connections.max(1))),
                per_address: config.max_unauthenticated_per_address.max(1),
                unauthenticated: Arc::new(Mutex::new(HashMap::new())),
                auth_timeout: config.auth_timeout,
            }),
        })
    }

    /// Accepts connections on `listener` until it fails. Each connection
    /// runs on its own task. A connection over the limits of step 0 is
    /// closed at once.
    pub async fn serve(self, listener: TcpListener) -> std::io::Result<()> {
        loop {
            let (socket, peer) = match listener.accept().await {
                Ok(accepted) => accepted,
                // Out of file descriptors and the like: wait and go on,
                // as a listener must not stop over one failed accept.
                Err(e) if crate::listen::transient(&e) => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
                Err(e) => return Err(e),
            };
            let address = ip_key(peer.ip());
            let Some((permit, pending)) = self.admission.admit(&address) else {
                tracing::debug!(%address, "ssh connection refused: connection limit");
                drop(socket);
                continue;
            };
            let _ = socket.set_nodelay(true);
            let (authenticated, signal) = oneshot::channel();
            let handler = Connection {
                sessions: Arc::new(Semaphore::new(self.shared.max_sessions)),
                shared: Arc::clone(&self.shared),
                key: None,
                channels: HashMap::new(),
                address,
                offers: HashMap::new(),
                authenticated: Some(authenticated),
            };
            let config = Arc::clone(&self.config);
            let deadline = tokio::time::Instant::now() + self.admission.auth_timeout;
            tokio::spawn(async move {
                let _permit = permit;
                run_connection(config, socket, handler, pending, signal, deadline).await;
            });
        }
    }
}

/// One connection, from the version exchange to its end. It counts as
/// unauthenticated (`pending`) until the handler signals authentication,
/// and is disconnected if that has not happened by `deadline`.
async fn run_connection<H, S>(
    config: Arc<russh::server::Config>,
    socket: S,
    handler: H,
    pending: Pending,
    authenticated: oneshot::Receiver<()>,
    deadline: tokio::time::Instant,
) where
    H: russh::server::Handler + Send + 'static,
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    // The version exchange runs here, before the session starts: a client
    // that never sends its version is cut off at the deadline too.
    let mut session =
        match tokio::time::timeout_at(deadline, russh::server::run_stream(config, socket, handler))
            .await
        {
            Ok(Ok(session)) => session,
            _ => return,
        };
    let handle = session.handle();
    tokio::select! {
        _ = &mut session => return,
        _ = authenticated => {}
        () = tokio::time::sleep_until(deadline) => {
            let _ = handle
                .disconnect(
                    russh::Disconnect::ByApplication,
                    "authentication timed out".to_owned(),
                    String::new(),
                )
                .await;
        }
    }
    drop(pending);
    let _ = session.await;
}

/// A session channel that has not run its command yet.
struct Opened {
    channel: Channel<Msg>,
    git_protocol: Option<String>,
    _permit: OwnedSemaphorePermit,
}

/// One client connection.
struct Connection<M, B, T, K> {
    shared: Arc<Shared<M, B, T, K>>,
    /// The key the client authenticated with, as the credential each
    /// command is identified with again.
    key: Option<Credential>,
    /// Bounds the session channels open at once.
    sessions: Arc<Semaphore>,
    channels: HashMap<ChannelId, Opened>,
    /// The client's address key ([`ip_key`]), for its failed-
    /// authentication bucket.
    address: String,
    /// Each key offered on this connection, by fingerprint, with whether
    /// it may authenticate: looked up once per connection.
    offers: HashMap<String, bool>,
    /// Told once, when the client authenticates (module docs, step 0).
    authenticated: Option<oneshot::Sender<()>>,
}

/// Longest `GIT_PROTOCOL` value kept. Git sends `version=2`.
const MAX_GIT_PROTOCOL_LEN: usize = 256;

impl<M, B, T, K> Connection<M, B, T, K>
where
    M: MetaStore + Clone + 'static,
    B: BlobStore + Clone + 'static,
    T: TokenStore + 'static,
    K: SshKeyStore + 'static,
{
    /// The credential for a key the client offered or proved it holds:
    /// `None` for a key of a type Aspen does not accept.
    ///
    /// This is the only place Aspen builds a [`Credential::SshKey`], whose
    /// fingerprint is a public value. Only the one built from a
    /// [`VerifiedPublicKey`] (the client's signature checked by the SSH
    /// layer) is kept and used to run commands; the one for an offer only
    /// decides whether to ask for a signature. HTTP never yields one
    /// (`aspen_auth::credential::from_authorization`).
    fn credential(key: &russh::keys::PublicKey) -> Option<Credential> {
        let key = SshPublicKey::from_blob(&key.public_key_bytes()).ok()?;
        Some(Credential::SshKey {
            fingerprint: key.fingerprint().to_string(),
        })
    }

    /// The identity a command runs as, resolved now.
    async fn command_identity(&self) -> Result<Arc<Identity>, Refusal> {
        let key = self
            .key
            .as_ref()
            .ok_or(Refusal::Auth(AuthError::Unauthenticated))?;
        identify_now(&self.shared.access, key, &self.address).await
    }
}

/// The identity `credential` (a verified key) stands for now: refused if
/// the key has been revoked, has expired or is no longer registered.
///
/// A re-check is a credential check like any other, so it goes through
/// [`Access::identify`] with the client's `address` (an [`ip_key`]): it
/// takes a failed authentication from the address before the lookup and
/// gives it back when the key still authenticates, and it is refused
/// without a lookup while the address has none left (issue #243). Every
/// key lookup Aspen makes is then charged the same way, so the
/// failed-authentication bound in `docs/SECURITY.md` covers a connection
/// that keeps running commands after its key was revoked.
pub(crate) async fn identify_now<M, T, K>(
    access: &Access<M, T, K>,
    credential: &Credential,
    address: &str,
) -> Result<Arc<Identity>, Refusal>
where
    M: MetaStore + 'static,
    T: TokenStore + 'static,
    K: SshKeyStore + 'static,
{
    access.identify(credential, address).await.map(Arc::new)
}

impl<M, B, T, K> russh::server::Handler for Connection<M, B, T, K>
where
    M: MetaStore + Clone + 'static,
    B: BlobStore + Clone + 'static,
    T: TokenStore + 'static,
    K: SshKeyStore + 'static,
{
    type Error = russh::Error;

    async fn auth_publickey_offered(
        &mut self,
        _user: &str,
        offer: PublicKeyOffer<'_>,
    ) -> Result<Auth, Self::Error> {
        // Only keys that would authenticate are worth a signature; any
        // other is refused here, so the client tries its next key.
        let Some(credential) = Self::credential(offer.public_key) else {
            return Ok(Auth::reject());
        };
        let Credential::SshKey { fingerprint } = &credential else {
            return Ok(Auth::reject());
        };
        if let Some(&ok) = self.offers.get(fingerprint) {
            return Ok(if ok { Auth::Accept } else { Auth::reject() });
        }
        if self.offers.len() >= self.shared.max_offers {
            // More distinct keys than attempts allowed: end the connection
            // rather than look up one more.
            return Err(russh::Error::Disconnect);
        }
        // Through the access layer, which takes a failed authentication
        // from the address before the lookup and gives it back if the key
        // is registered (issue #198).
        let ok = match self
            .shared
            .access
            .identify(&credential, &self.address)
            .await
        {
            Ok(_) => true,
            // The address has no failed authentications left, or the store
            // is down (not the client's failure): refused without
            // remembering an answer.
            Err(Refusal::Limit(_) | Refusal::Unavailable | Refusal::Busy(_)) => {
                return Ok(Auth::reject());
            }
            Err(Refusal::Auth(_)) => false,
        };
        self.offers.insert(fingerprint.clone(), ok);
        Ok(if ok { Auth::Accept } else { Auth::reject() })
    }

    async fn auth_publickey(
        &mut self,
        _user: &str,
        key: VerifiedPublicKey<'_>,
    ) -> Result<Auth, Self::Error> {
        let Some(credential) = Self::credential(key.public_key) else {
            return Ok(Auth::reject());
        };
        if self
            .shared
            .access
            .identify(&credential, &self.address)
            .await
            .is_err()
        {
            return Ok(Auth::reject());
        }
        self.key = Some(credential);
        if let Some(signal) = self.authenticated.take() {
            let _ = signal.send(());
        }
        Ok(Auth::Accept)
    }

    async fn channel_open_session(
        &mut self,
        channel: Channel<Msg>,
        reply: ChannelOpenHandle,
        _session: &mut Session,
    ) -> Result<(), Self::Error> {
        let permit = match Arc::clone(&self.sessions).try_acquire_owned() {
            Ok(p) if self.key.is_some() => p,
            _ => {
                reply
                    .reject(ChannelOpenFailure::AdministrativelyProhibited)
                    .await;
                return Ok(());
            }
        };
        self.channels.insert(
            channel.id(),
            Opened {
                channel,
                git_protocol: None,
                _permit: permit,
            },
        );
        reply.accept().await;
        Ok(())
    }

    async fn env_request(
        &mut self,
        channel: ChannelId,
        name: &str,
        value: &str,
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        match self.channels.get_mut(&channel) {
            Some(opened) if name == "GIT_PROTOCOL" && value.len() <= MAX_GIT_PROTOCOL_LEN => {
                opened.git_protocol = Some(value.to_owned());
                session.channel_success(channel)
            }
            _ => session.channel_failure(channel),
        }
    }

    async fn exec_request(
        &mut self,
        channel: ChannelId,
        data: &[u8],
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        let Some(opened) = self.channels.remove(&channel) else {
            return session.channel_failure(channel);
        };
        session.channel_success(channel)?;
        let identity = self.command_identity().await;
        let shared = Arc::clone(&self.shared);
        let command = data.to_vec();
        // Present whenever `identity` is `Ok`: channels open only after
        // authentication.
        let credential = self.key.clone();
        let address = self.address.clone();
        tokio::spawn(async move {
            let Opened {
                channel,
                git_protocol,
                _permit,
            } = opened;
            let (read, write) = channel.split();
            let identity = identity.and_then(|i| {
                credential
                    .map(|c| (i, c))
                    .ok_or(Refusal::Auth(AuthError::Unauthenticated))
            });
            exec::run(
                shared,
                identity,
                &address,
                read,
                write,
                command,
                git_protocol,
            )
            .await;
        });
        Ok(())
    }

    async fn shell_request(
        &mut self,
        channel: ChannelId,
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        let Some(opened) = self.channels.remove(&channel) else {
            return session.channel_failure(channel);
        };
        session.channel_success(channel)?;
        let identity = self.command_identity().await.and_then(|identity| {
            // A greeting costs a key lookup, so it costs a request (D4).
            match crate::access::subject_key(&identity) {
                Some(key) => self
                    .shared
                    .access
                    .charge(
                        aspen_auth::RequestClass::Read,
                        &key,
                        std::time::Instant::now(),
                    )
                    .map(|()| identity)
                    .map_err(Refusal::Limit),
                None => Ok(identity),
            }
        });
        let greeting = match identity {
            Ok(identity) => match identity.principal() {
                aspen_core::auth::Principal::Subject { id } => format!(
                    "Hi {}! You are signed in to Aspen; it serves Git only, with no shell access.\n",
                    aspen_proto::pkt::one_line(id)
                ),
                aspen_core::auth::Principal::Anonymous => "Aspen serves Git only.\n".to_owned(),
            },
            Err(refusal) => format!("aspen: {}\n", refusal.message()),
        };
        tokio::spawn(async move {
            let Opened { channel, .. } = opened;
            let mut stderr = channel.make_writer_ext(Some(1));
            let _ = stderr.write_all(greeting.as_bytes()).await;
            let _ = stderr.flush().await;
            let _ = channel.exit_status(1).await;
            let _ = channel.eof().await;
            let _ = channel.close().await;
        });
        Ok(())
    }

    async fn subsystem_request(
        &mut self,
        channel: ChannelId,
        _name: &str,
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        session.channel_failure(channel)
    }

    #[allow(clippy::too_many_arguments)]
    async fn pty_request(
        &mut self,
        channel: ChannelId,
        _term: &str,
        _col_width: u32,
        _row_height: u32,
        _pix_width: u32,
        _pix_height: u32,
        _modes: &[(Pty, u32)],
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        session.channel_failure(channel)
    }

    async fn x11_request(
        &mut self,
        channel: ChannelId,
        _single_connection: bool,
        _x11_auth_protocol: &str,
        _x11_auth_cookie: &str,
        _x11_screen_number: u32,
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        session.channel_failure(channel)
    }

    async fn channel_close(
        &mut self,
        channel: ChannelId,
        _session: &mut Session,
    ) -> Result<(), Self::Error> {
        self.channels.remove(&channel);
        Ok(())
    }
}
