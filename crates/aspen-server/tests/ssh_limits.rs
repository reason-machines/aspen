//! The SSH listener's limits for clients that have not authenticated yet
//! (node D4): connection caps, the login deadline, at most one key lookup
//! per offered key per connection, and the per-address limit on failed
//! authentications. Each test drives a real TCP or russh client against
//! `SshServer` and counts the key store's reads, which is what the limits
//! exist to bound.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use aspen_auth::{
    AuthService, Grants, LimitsConfig, MemoryTokenStore, NewSshKey, PolicyConfig, RateLimit,
    SshFingerprint, SshKeyRecord, SshKeyStore, SshKeyStoreError, SshPublicKey,
};
use aspen_blob::LocalBlobStore;
use aspen_core::id::UnixMicros;
use aspen_core::meta::MetaStore;
use aspen_core::name::{RefName, RepoName};
use aspen_core::repo::RepoSettings;
use aspen_meta::SqlStore;
use aspen_pack::MaxObjectBytes;
use aspen_server::access::{Access, ClientAddr};
use aspen_server::ssh::keys::SqlSshKeys;
use aspen_server::ssh::{PrivateKey, SshConfig, SshServer, load_host_key};
use aspen_server::{Server, ServerConfig};
use tokio::io::AsyncReadExt;

/// The SQL key store, counting every lookup.
#[derive(Clone)]
struct Counting {
    inner: SqlSshKeys,
    reads: Arc<AtomicUsize>,
}

impl SshKeyStore for Counting {
    async fn ssh_key(
        &self,
        fingerprint: &SshFingerprint,
    ) -> Result<Option<SshKeyRecord>, SshKeyStoreError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.inner.ssh_key(fingerprint).await
    }
}

static KEYS: AtomicUsize = AtomicUsize::new(0);

/// A new unencrypted Ed25519 key file in `dir`.
fn keygen(dir: &std::path::Path, name: &str) -> std::path::PathBuf {
    let path = dir.join(name);
    let out = std::process::Command::new("ssh-keygen")
        .args(["-q", "-t", "ed25519", "-N", "", "-C", "aspen-test", "-f"])
        .arg(&path)
        .output()
        .expect("ssh-keygen");
    assert!(out.status.success(), "{out:?}");
    path
}

struct Fixture {
    _dir: tempfile::TempDir,
    keys: SqlSshKeys,
    reads: Arc<AtomicUsize>,
    port: u16,
}

impl Fixture {
    async fn start(limits: LimitsConfig, configure: impl FnOnce(&mut SshConfig)) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let url = format!("sqlite://{}?mode=rwc", dir.path().join("meta.db").display());
        let meta = SqlStore::connect(&url).await.unwrap();
        meta.migrate().await.unwrap();
        let blobs = LocalBlobStore::open(dir.path().join("blobs")).unwrap();
        let server = Server::new(
            meta.clone(),
            blobs,
            ServerConfig::new(MaxObjectBytes::new(1 << 20).unwrap()),
        );
        let settings = RepoSettings::new(RefName::new("refs/heads/main").unwrap()).unwrap();
        meta.create_repo(&RepoName::new("demo").unwrap(), &settings)
            .await
            .unwrap();
        let keys = SqlSshKeys(meta.clone());
        let reads = Arc::new(AtomicUsize::new(0));
        let auth = AuthService::new(PolicyConfig::default(), MemoryTokenStore::new())
            .unwrap()
            .with_ssh_keys(Counting {
                inner: keys.clone(),
                reads: Arc::clone(&reads),
            });
        let access = Arc::new(Access::new(
            auth,
            aspen_auth::Limits::new(limits),
            meta,
            ClientAddr::Peer,
        ));
        let host = load_host_key(&keygen(dir.path(), "host")).unwrap();
        let mut config = SshConfig::new(vec![host]);
        config.auth_rejection_time = Duration::from_millis(10);
        configure(&mut config);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let ssh = SshServer::new(server, access, config).unwrap();
        tokio::spawn(async move { ssh.serve(listener).await.unwrap() });
        Self {
            _dir: dir,
            keys,
            reads,
            port,
        }
    }

    /// A new Ed25519 key, registered with read access when `register`.
    async fn key(&self, register: bool) -> Arc<PrivateKey> {
        let n = KEYS.fetch_add(1, Ordering::SeqCst);
        let key = load_host_key(&keygen(self._dir.path(), &format!("id{n}"))).unwrap();
        if register {
            let public = key.public_key().to_openssh().unwrap();
            self.keys
                .register(
                    NewSshKey {
                        key: SshPublicKey::parse_openssh(&public).unwrap(),
                        subject: "user:alice".into(),
                        grants: Grants::new(["read:demo".parse().unwrap()]),
                        description: String::new(),
                        expires_at: None,
                    },
                    UnixMicros::now(),
                )
                .await
                .unwrap();
        }
        Arc::new(key)
    }

    fn reads(&self) -> usize {
        self.reads.load(Ordering::SeqCst)
    }

    async fn connect(&self) -> russh::client::Handle<Client> {
        let config = Arc::new(russh::client::Config::default());
        russh::client::connect(config, ("127.0.0.1", self.port), Client)
            .await
            .unwrap()
    }
}

struct Client;

impl russh::client::Handler for Client {
    type Error = russh::Error;

    async fn check_server_key(
        &mut self,
        _key: &russh::keys::PublicKeyOrCertificate,
    ) -> Result<bool, Self::Error> {
        Ok(true)
    }
}

/// A signer that never signs: each `authenticate_publickey_with` through
/// it is an unsigned offer (an SSH `publickey` probe) and nothing more.
struct Declining;

#[derive(Debug)]
enum Declined {
    Declined,
    Send,
}

impl From<russh::SendError> for Declined {
    fn from(_: russh::SendError) -> Self {
        Self::Send
    }
}

impl russh::Signer for Declining {
    type Error = Declined;

    async fn auth_sign(
        &mut self,
        _key: &russh::keys::agent::AgentIdentity,
        _hash_alg: Option<russh::keys::HashAlg>,
        _to_sign: Vec<u8>,
    ) -> Result<Vec<u8>, Self::Error> {
        Err(Declined::Declined)
    }
}

/// Offers `key` without signing. `Accepted` when the server answered that
/// the key would authenticate (it asked for a signature).
#[derive(Debug, PartialEq, Eq)]
enum Offer {
    Accepted,
    Refused,
    Gone,
}

async fn offer(h: &mut russh::client::Handle<Client>, key: &PrivateKey) -> Offer {
    match h
        .authenticate_publickey_with("git", key.public_key().clone(), None, &mut Declining)
        .await
    {
        Err(Declined::Declined) => Offer::Accepted,
        Ok(r) if !r.success() && !h.is_closed() => Offer::Refused,
        _ => Offer::Gone,
    }
}

async fn sign_in(h: &mut russh::client::Handle<Client>, key: Arc<PrivateKey>) -> bool {
    h.authenticate_publickey("git", russh::keys::PrivateKeyWithHashAlg::new(key, None))
        .await
        .is_ok_and(|r| r.success())
}

/// Whether the server closes a raw TCP connection within `within`.
async fn closed_within(socket: &mut tokio::net::TcpStream, within: Duration) -> bool {
    let mut buf = [0u8; 4096];
    let deadline = tokio::time::Instant::now() + within;
    loop {
        match tokio::time::timeout_at(deadline, socket.read(&mut buf)).await {
            Ok(Ok(0) | Err(_)) => return true,
            Ok(Ok(_)) => {}
            Err(_) => return false,
        }
    }
}

/// Invariant: an unsigned offer of a registered key, repeated on one
/// connection, is looked up once. russh does not count an accepted offer
/// as an authentication attempt, so without the per-connection answer
/// every repetition was a metadata read by a client that never proves it
/// holds the key (and public keys are often published).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repeated_offers_of_a_registered_key_cost_one_lookup() {
    let f = Fixture::start(LimitsConfig::default(), |_| {}).await;
    let key = f.key(true).await;
    let mut h = f.connect().await;
    for _ in 0..50 {
        assert_eq!(offer(&mut h, &key).await, Offer::Accepted);
    }
    assert_eq!(f.reads(), 1);
    // The key still signs in on that connection.
    assert!(sign_in(&mut h, key).await);
}

/// Invariant: at most `max_auth_attempts` distinct keys are looked up per
/// connection, accepted offers included; the next offer ends the
/// connection without a lookup.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn distinct_offers_per_connection_are_capped() {
    let f = Fixture::start(LimitsConfig::default(), |c| c.max_auth_attempts = 3).await;
    let mut keys = Vec::new();
    for _ in 0..5 {
        keys.push(f.key(true).await);
    }
    let mut h = f.connect().await;
    for key in &keys[..3] {
        assert_eq!(offer(&mut h, key).await, Offer::Accepted);
    }
    assert_eq!(offer(&mut h, &keys[3]).await, Offer::Gone);
    assert_eq!(f.reads(), 3);
}

/// Invariant: refused keys drain the address's failed-authentication
/// bucket; while it is empty, offers are refused without a lookup, even
/// of a registered key, and the bucket is per address, shared by every
/// connection from it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_authentications_are_limited_per_address() {
    let limits = LimitsConfig {
        auth_failures: Some(RateLimit {
            burst: 3,
            per_second: 0.001,
        }),
        ..LimitsConfig::default()
    };
    let f = Fixture::start(limits, |_| {}).await;
    let good = f.key(true).await;
    for _ in 0..3 {
        let mut h = f.connect().await;
        let stranger = f.key(false).await;
        assert_eq!(offer(&mut h, &stranger).await, Offer::Refused);
    }
    assert_eq!(f.reads(), 3);
    let mut h = f.connect().await;
    for _ in 0..5 {
        let stranger = f.key(false).await;
        assert_ne!(offer(&mut h, &stranger).await, Offer::Accepted);
    }
    let mut h = f.connect().await;
    assert!(!sign_in(&mut h, good).await, "signed in past the limit");
    assert_eq!(f.reads(), 3, "a lookup ran past the limit");
}

/// Invariant: a connection that has not authenticated within
/// `auth_timeout` is closed, whether it sent nothing at all or finished
/// the key exchange and then idled; an authenticated one outlives it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_connection_must_authenticate_within_the_deadline() {
    let f = Fixture::start(LimitsConfig::default(), |c| {
        c.auth_timeout = Duration::from_millis(500);
    })
    .await;
    let key = f.key(true).await;
    let mut silent = tokio::net::TcpStream::connect(("127.0.0.1", f.port))
        .await
        .unwrap();
    let idle = f.connect().await;
    let mut signed_in = f.connect().await;
    assert!(sign_in(&mut signed_in, key).await);
    assert!(closed_within(&mut silent, Duration::from_secs(5)).await);
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert!(
        idle.is_closed(),
        "an unauthenticated session outlived the deadline"
    );
    assert!(
        !signed_in.is_closed(),
        "an authenticated session was cut off"
    );
    signed_in.channel_open_session().await.unwrap();
}

/// Invariant: at most `max_unauthenticated_per_address` connections from
/// one address wait to authenticate at once; the next is closed before
/// the key exchange. Authenticating frees the place.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unauthenticated_connections_per_address_are_capped() {
    let f = Fixture::start(LimitsConfig::default(), |c| {
        c.max_unauthenticated_per_address = 2;
    })
    .await;
    let key = f.key(true).await;
    let mut first = f.connect().await;
    let _second = f.connect().await;
    let mut third = tokio::net::TcpStream::connect(("127.0.0.1", f.port))
        .await
        .unwrap();
    assert!(closed_within(&mut third, Duration::from_secs(5)).await);
    assert!(sign_in(&mut first, key).await);
    // The freed place is visible once the server has seen the sign-in.
    let mut admitted = false;
    for _ in 0..50 {
        let config = Arc::new(russh::client::Config::default());
        if russh::client::connect(config, ("127.0.0.1", f.port), Client)
            .await
            .is_ok()
        {
            admitted = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(admitted, "an authenticated connection still counted");
}

/// Invariant: at most `max_connections` connections are open at once,
/// authenticated or not; the next is closed at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn connections_are_capped_in_total() {
    let f = Fixture::start(LimitsConfig::default(), |c| c.max_connections = 1).await;
    let key = f.key(true).await;
    let mut h = f.connect().await;
    assert!(sign_in(&mut h, key).await);
    let mut second = tokio::net::TcpStream::connect(("127.0.0.1", f.port))
        .await
        .unwrap();
    assert!(closed_within(&mut second, Duration::from_secs(5)).await);
    assert!(!h.is_closed());
}

/// Runs `command` on a new session channel and returns what it wrote to
/// stdout and stderr once the channel ends.
async fn exec(h: &russh::client::Handle<Client>, command: Option<&str>) -> (String, String) {
    let mut ch = h.channel_open_session().await.unwrap();
    match command {
        Some(c) => ch.exec(true, c).await.unwrap(),
        None => ch.request_shell(true).await.unwrap(),
    }
    let (mut out, mut err) = (Vec::new(), Vec::new());
    loop {
        match tokio::time::timeout(Duration::from_secs(30), ch.wait())
            .await
            .expect("the command did not end")
        {
            Some(russh::ChannelMsg::Data { data }) => out.extend_from_slice(&data),
            Some(russh::ChannelMsg::ExtendedData { data, .. }) => err.extend_from_slice(&data),
            Some(russh::ChannelMsg::Eof | russh::ChannelMsg::Close) | None => break,
            Some(_) => {}
        }
    }
    (
        String::from_utf8_lossy(&out).into_owned(),
        String::from_utf8_lossy(&err).into_owned(),
    )
}

/// Invariant: every command a signed-in key runs costs a request from its
/// subject's read bucket, also one refused before it reaches a
/// repository (not Git, a bad path) and a shell greeting, since each
/// costs a key lookup; a loop of them is rate limited like real fetches.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn refused_commands_and_greetings_draw_on_the_request_bucket() {
    let limits = LimitsConfig {
        read: Some(RateLimit {
            burst: 3,
            per_second: 0.001,
        }),
        ..LimitsConfig::default()
    };
    let f = Fixture::start(limits, |_| {}).await;
    let key = f.key(true).await;
    let mut h = f.connect().await;
    assert!(sign_in(&mut h, key).await);
    let (_, err) = exec(&h, Some("rm -rf /")).await;
    assert!(err.contains("only git-upload-pack"), "{err}");
    let (out, _) = exec(&h, Some("git-upload-pack '../etc'")).await;
    assert!(out.contains("ERR"), "{out}");
    let (_, err) = exec(&h, None).await;
    assert!(err.contains("Hi user:alice"), "{err}");
    // The bucket is spent: a real fetch and another greeting are refused.
    let (out, _) = exec(&h, Some("git-upload-pack 'demo'")).await;
    assert!(out.contains("rate limit"), "{out}");
    let (_, err) = exec(&h, None).await;
    assert!(err.contains("rate limit"), "{err}");
}

/// Revokes `key` in the fixture's store.
async fn revoke(f: &Fixture, key: &PrivateKey) {
    let public = key.public_key().to_openssh().unwrap();
    let fingerprint = SshPublicKey::parse_openssh(&public).unwrap().fingerprint();
    assert!(
        f.keys
            .revoke(&fingerprint, UnixMicros::now())
            .await
            .unwrap()
    );
}

/// Invariant: the key of an authenticated connection is identified again
/// for each command, and a re-check that fails (the key was revoked since
/// the connection authenticated) drains the address's failed-
/// authentication bucket like a refused offer; once the bucket is empty,
/// commands are refused without a lookup, and so is every other credential
/// from that address (issue #243). Before the fix each re-check was an
/// uncharged lookup, so an open connection could make as many as it liked.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_rechecks_of_a_revoked_key_drain_the_failure_bucket() {
    let limits = LimitsConfig {
        auth_failures: Some(RateLimit {
            burst: 3,
            per_second: 0.001,
        }),
        ..LimitsConfig::default()
    };
    let f = Fixture::start(limits, |_| {}).await;
    let key = f.key(true).await;
    let other = f.key(true).await;
    let mut h = f.connect().await;
    assert!(sign_in(&mut h, Arc::clone(&key)).await);
    // A live key's re-checks cost the bucket nothing.
    for _ in 0..5 {
        let (_, err) = exec(&h, None).await;
        assert!(err.contains("Hi user:alice"), "{err}");
    }
    revoke(&f, &key).await;
    let before = f.reads();
    let mut answers = Vec::new();
    for i in 0..10 {
        // Shell greetings and Git commands both re-check the key.
        let (out, err) = if i % 2 == 0 {
            exec(&h, None).await
        } else {
            exec(&h, Some("git-upload-pack 'demo'")).await
        };
        answers.push(format!("{out}{err}"));
    }
    assert_eq!(
        f.reads() - before,
        3,
        "re-checks past the bucket: {answers:?}"
    );
    for answer in &answers[..3] {
        assert!(answer.contains("authentication required"), "{answer}");
    }
    for answer in &answers[3..] {
        assert!(
            answer.contains("too many failed authentications"),
            "{answer}"
        );
    }
    // The bucket is the address's: another registered key is refused too.
    let mut h = f.connect().await;
    assert!(!sign_in(&mut h, other).await, "signed in past the limit");
    assert_eq!(f.reads() - before, 3, "a lookup ran past the limit");
}

/// One pkt-line from `r`: `None` for a flush, the line's data otherwise.
async fn read_pkt<R: tokio::io::AsyncRead + Unpin>(r: &mut R) -> Option<String> {
    let mut len = [0u8; 4];
    r.read_exact(&mut len).await.expect("pkt-line length");
    let len = usize::from_str_radix(std::str::from_utf8(&len).unwrap(), 16).unwrap();
    if len == 0 {
        return None;
    }
    let mut data = vec![0u8; len - 4];
    r.read_exact(&mut data).await.expect("pkt-line data");
    Some(String::from_utf8_lossy(&data).into_owned())
}

/// Every data line up to the next flush.
async fn read_until_flush<R: tokio::io::AsyncRead + Unpin>(r: &mut R) -> Vec<String> {
    let mut lines = Vec::new();
    while let Some(line) = read_pkt(r).await {
        lines.push(line);
    }
    lines
}

/// A v2 `ls-refs` request, as git sends it.
fn ls_refs_request() -> Vec<u8> {
    let pkt = |line: &str| format!("{:04x}{line}", line.len() + 4);
    format!(
        "{}0001{}0000",
        pkt("command=ls-refs\n"),
        pkt("ref-prefix refs/heads/\n")
    )
    .into_bytes()
}

/// Invariant: within a protocol v2 session, the key is identified again
/// before every command after the first, and a re-check that fails drains
/// the address's failed-authentication bucket; once it is empty, the
/// re-check is refused without a lookup (issue #243). Several sessions
/// that started while the key was live each fail one re-check after it is
/// revoked; together they cost at most the bucket's lookups.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_v2_rechecks_drain_the_failure_bucket() {
    use tokio::io::AsyncWriteExt;
    let limits = LimitsConfig {
        auth_failures: Some(RateLimit {
            burst: 2,
            per_second: 0.001,
        }),
        ..LimitsConfig::default()
    };
    let f = Fixture::start(limits, |_| {}).await;
    let key = f.key(true).await;
    let mut h = f.connect().await;
    assert!(sign_in(&mut h, Arc::clone(&key)).await);
    let mut sessions = Vec::new();
    for _ in 0..6 {
        let ch = h.channel_open_session().await.unwrap();
        ch.set_env(false, "GIT_PROTOCOL", "version=2")
            .await
            .unwrap();
        ch.exec(false, "git-upload-pack 'demo'").await.unwrap();
        let mut stream = ch.into_stream();
        let caps = read_until_flush(&mut stream).await;
        assert_eq!(
            caps.first().map(String::as_str),
            Some("version 2\n"),
            "{caps:?}"
        );
        // The first command runs on the identity the session started with.
        stream.write_all(&ls_refs_request()).await.unwrap();
        stream.flush().await.unwrap();
        assert_eq!(read_until_flush(&mut stream).await, Vec::<String>::new());
        sessions.push(stream);
    }
    revoke(&f, &key).await;
    let before = f.reads();
    let mut answers = Vec::new();
    for stream in &mut sessions {
        stream.write_all(&ls_refs_request()).await.unwrap();
        stream.flush().await.unwrap();
        let line = tokio::time::timeout(Duration::from_secs(30), read_pkt(stream))
            .await
            .expect("no answer to the second command");
        answers.push(line.unwrap_or_default());
    }
    assert_eq!(
        f.reads() - before,
        2,
        "re-checks past the bucket: {answers:?}"
    );
    for answer in &answers {
        assert!(answer.starts_with("ERR "), "{answer}");
    }
    for answer in &answers[..2] {
        assert!(answer.contains("authentication required"), "{answer}");
    }
    for answer in &answers[2..] {
        assert!(
            answer.contains("too many failed authentications"),
            "{answer}"
        );
    }
}
