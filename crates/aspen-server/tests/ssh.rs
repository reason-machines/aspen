//! Git over SSH (node C3) end to end: stock `git` and OpenSSH's `ssh`
//! against Aspen's SSH server, with keys registered in the metadata
//! database (`SqlSshKeys`) exactly as a deployment registers them.
//!
//! Every test asserts what a client or the stored state shows: refs and
//! objects after clone, fetch and push (checked with `git fsck --strict`),
//! the text a refused client prints, and what a push left in the stores.

use std::path::{Path, PathBuf};
use std::process::Output;
use std::sync::Arc;
use std::time::Duration;

use aspen_auth::{
    AuthService, Grants, LimitsConfig, MemoryTokenStore, NewSshKey, PolicyConfig, PushQuota,
    RateLimit, SshPublicKey,
};
use aspen_blob::LocalBlobStore;
use aspen_core::id::UnixMicros;
use aspen_core::meta::MetaStore;
use aspen_core::name::{RefName, RepoName};
use aspen_core::repo::{Repo, RepoSettings};
use aspen_meta::SqlStore;
use aspen_pack::{MaxInflatedBytes, MaxObjectBytes};
use aspen_server::access::{Access, ClientAddr};
use aspen_server::receive::ReceiveConfig;
use aspen_server::receive::hook::{
    Caller, CommittedUpdate, HookFuture, HookObjects, HookUpdate, HookVerdict, PushHook,
};
use aspen_server::ssh::keys::SqlSshKeys;
use aspen_server::ssh::{SshConfig, SshServer, load_host_key};
use aspen_server::{Server, ServerConfig};
use aspen_testkit::git::isolated_git;

const MIB: u64 = 1 << 20;

/// How long one `git` or `ssh` command may take before the test fails. A
/// server that waits for input the client never sends shows up as this.
const COMMAND_TIMEOUT: Duration = Duration::from_secs(60);

/// A client: a key pair on disk and the subject it is registered as.
struct User {
    key: PathBuf,
    public: String,
}

struct Fixture {
    dir: tempfile::TempDir,
    meta: SqlStore,
    keys: SqlSshKeys,
    repo: Repo,
    port: u16,
    known_hosts: PathBuf,
    home: PathBuf,
}

/// Runs a command with [`COMMAND_TIMEOUT`].
async fn run(mut cmd: tokio::process::Command) -> Output {
    cmd.kill_on_drop(true);
    let shown = format!("{cmd:?}");
    match tokio::time::timeout(COMMAND_TIMEOUT, cmd.output()).await {
        Ok(out) => out.expect("spawn"),
        Err(_) => panic!("timed out after {COMMAND_TIMEOUT:?}: {shown}"),
    }
}

fn text(out: &Output) -> String {
    format!(
        "status {:?}\n--- stdout\n{}--- stderr\n{}",
        out.status.code(),
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}

fn keygen(path: &Path, kind: &[&str]) -> String {
    let out = std::process::Command::new("ssh-keygen")
        .args(["-q", "-N", "", "-C", "aspen-test", "-f"])
        .arg(path)
        .args(kind)
        .output()
        .expect("ssh-keygen");
    assert!(out.status.success(), "{}", text(&out));
    std::fs::read_to_string(path.with_extension("pub")).unwrap()
}

impl Fixture {
    async fn start() -> Self {
        Self::start_with(LimitsConfig::default()).await
    }

    async fn start_with(limits: LimitsConfig) -> Self {
        Self::start_configured(limits, |_| {}).await
    }

    async fn start_configured(
        limits: LimitsConfig,
        configure: impl FnOnce(&mut SshConfig),
    ) -> Self {
        Self::start_hooked(limits, configure, None).await
    }

    /// A server whose receive-pack runs `hook` on every update.
    async fn start_hooked(
        limits: LimitsConfig,
        configure: impl FnOnce(&mut SshConfig),
        hook: Option<Arc<dyn PushHook>>,
    ) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let url = format!("sqlite://{}?mode=rwc", dir.path().join("meta.db").display());
        let meta = SqlStore::connect(&url).await.unwrap();
        meta.migrate().await.unwrap();
        let blobs = LocalBlobStore::open(dir.path().join("blobs")).unwrap();
        let mut receive = ReceiveConfig::new(
            dir.path().join("spool"),
            MaxObjectBytes::new(64 * MIB).unwrap(),
            MaxInflatedBytes::new(1024 * MIB).unwrap(),
        );
        receive.thread_limit = Some(2);
        receive.hook = hook;
        let server = Server::new(
            meta.clone(),
            blobs,
            ServerConfig::new(MaxObjectBytes::new(64 * MIB).unwrap()),
        )
        .with_receive_pack(receive)
        .unwrap();
        let settings = RepoSettings::new(RefName::new("refs/heads/main").unwrap()).unwrap();
        let repo = meta
            .create_repo(&RepoName::new("demo").unwrap(), &settings)
            .await
            .unwrap();
        let keys = SqlSshKeys(meta.clone());
        // No anonymous grants: SSH has no anonymous callers anyway, and
        // every grant a test sees comes from the key's registration.
        let auth = AuthService::new(PolicyConfig::default(), MemoryTokenStore::new())
            .unwrap()
            .with_ssh_keys(keys.clone());
        let access = Arc::new(Access::new(
            auth,
            aspen_auth::Limits::new(limits),
            meta.clone(),
            ClientAddr::Peer,
        ));
        let host = dir.path().join("host_ed25519");
        let host_pub = keygen(&host, &["-t", "ed25519"]);
        let mut config = SshConfig::new(vec![load_host_key(&host).unwrap()]);
        // Refused keys are tested on purpose; keep each refusal quick.
        config.auth_rejection_time = Duration::from_millis(50);
        configure(&mut config);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let ssh = SshServer::new(server, access, config).unwrap();
        tokio::spawn(async move { ssh.serve(listener).await.unwrap() });
        let known_hosts = dir.path().join("known_hosts");
        let host_pub = host_pub
            .split_whitespace()
            .take(2)
            .collect::<Vec<_>>()
            .join(" ");
        std::fs::write(&known_hosts, format!("[127.0.0.1]:{port} {host_pub}\n")).unwrap();
        let home = dir.path().join("home");
        std::fs::create_dir_all(&home).unwrap();
        Self {
            dir,
            meta,
            keys,
            repo,
            port,
            known_hosts,
            home,
        }
    }

    /// A new key pair (`ssh-keygen` arguments `kind`), not registered.
    fn user(&self, name: &str, kind: &[&str]) -> User {
        let key = self.dir.path().join(format!("id_{name}"));
        let public = keygen(&key, kind);
        User { key, public }
    }

    /// An Ed25519 user registered with `grants`.
    async fn registered(&self, name: &str, grants: &[&str]) -> User {
        let u = self.user(name, &["-t", "ed25519"]);
        self.register(&u, name, grants).await;
        u
    }

    async fn register(&self, u: &User, subject: &str, grants: &[&str]) {
        self.keys
            .register(
                NewSshKey {
                    key: SshPublicKey::parse_openssh(&u.public).unwrap(),
                    subject: format!("user:{subject}"),
                    grants: Grants::new(grants.iter().map(|g| g.parse().unwrap())),
                    description: String::new(),
                    expires_at: None,
                },
                UnixMicros::now(),
            )
            .await
            .unwrap();
    }

    fn url(&self, path: &str) -> String {
        format!("ssh://git@127.0.0.1:{}/{path}", self.port)
    }

    /// The `ssh` command line Git runs, for `u`, with `extra` options.
    fn ssh_command(&self, u: &User, extra: &str) -> String {
        format!(
            "ssh -F /dev/null -i {} -o IdentitiesOnly=yes -o IdentityAgent=none \
             -o UserKnownHostsFile={} -o StrictHostKeyChecking=yes -o BatchMode=yes \
             -o LogLevel=ERROR {extra}",
            u.key.display(),
            self.known_hosts.display()
        )
    }

    /// `git` in `dir` as user `u`.
    fn git(&self, u: &User, dir: &Path, args: &[&str]) -> tokio::process::Command {
        self.git_via(u, "", dir, args)
    }

    fn git_via(
        &self,
        u: &User,
        ssh_extra: &str,
        dir: &Path,
        args: &[&str],
    ) -> tokio::process::Command {
        let mut cmd: tokio::process::Command = isolated_git(&self.home).into();
        cmd.env("GIT_SSH_COMMAND", self.ssh_command(u, ssh_extra))
            .current_dir(dir)
            .args(args);
        cmd
    }

    async fn ok(&self, u: &User, dir: &Path, args: &[&str]) -> String {
        let out = run(self.git(u, dir, args)).await;
        assert!(out.status.success(), "git {args:?}\n{}", text(&out));
        String::from_utf8(out.stdout).unwrap()
    }

    async fn fails(&self, u: &User, dir: &Path, args: &[&str]) -> String {
        let out = run(self.git(u, dir, args)).await;
        assert!(
            !out.status.success(),
            "git {args:?} succeeded\n{}",
            text(&out)
        );
        String::from_utf8_lossy(&out.stderr).into_owned()
    }

    /// A working repository with `main` checked out.
    async fn work(&self, name: &str) -> PathBuf {
        let path = self.dir.path().join(name);
        std::fs::create_dir_all(&path).unwrap();
        let out = run(git_local(
            &self.home,
            &path,
            &["init", "-q", "--initial-branch=main"],
        ))
        .await;
        assert!(out.status.success(), "{}", text(&out));
        path
    }

    async fn commit(&self, dir: &Path, file: &str, content: &str) -> String {
        std::fs::write(dir.join(file), content).unwrap();
        local_ok(&self.home, dir, &["add", file]).await;
        let message = format!("{file}: {} bytes", content.len());
        local_ok(&self.home, dir, &["commit", "-q", "-m", &message]).await;
        local_ok(&self.home, dir, &["rev-parse", "HEAD"])
            .await
            .trim()
            .to_owned()
    }

    /// The tip Aspen stores for `name`, if the ref is live.
    async fn tip(&self, name: &str) -> Option<String> {
        self.meta
            .current_version(self.repo.id, &RefName::new(name).unwrap())
            .await
            .unwrap()
            .and_then(|v| v.value.tip())
            .map(|t| t.to_string())
    }
}

fn git_local(home: &Path, dir: &Path, args: &[&str]) -> tokio::process::Command {
    let mut cmd: tokio::process::Command = isolated_git(home).into();
    cmd.current_dir(dir).args(args);
    cmd
}

async fn local_ok(home: &Path, dir: &Path, args: &[&str]) -> String {
    let out = run(git_local(home, dir, args)).await;
    assert!(out.status.success(), "git {args:?}\n{}", text(&out));
    String::from_utf8(out.stdout).unwrap()
}

/// `git fsck --strict` passes and `rev` resolves to `want`.
async fn assert_clone(home: &Path, dir: &Path, rev: &str, want: &str) {
    assert_eq!(local_ok(home, dir, &["rev-parse", rev]).await.trim(), want);
    local_ok(home, dir, &["fsck", "--strict", "--no-dangling"]).await;
}

fn protocol(v: &str) -> String {
    format!("protocol.version={v}")
}

/// Invariant: over SSH, in protocol v2, v1 and v0, a push lands what the
/// client sent (Aspen stores the client's tip), and a clone and an
/// incremental fetch give back exactly those commits with every object
/// (`fsck --strict`). v0 and v1 run the stateful negotiation: the
/// incremental fetch is acknowledged on the client's old tip.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn clone_fetch_and_push_in_every_protocol_version() {
    let f = Fixture::start().await;
    let alice = f.registered("alice", &["write:demo"]).await;
    let w = f.work("w").await;
    let url = f.url("demo.git");
    let mut tip = f.commit(&w, "a.txt", "one").await;
    f.ok(&alice, &w, &["push", "-q", &url, "main"]).await;
    assert_eq!(
        f.tip("refs/heads/main").await.as_deref(),
        Some(tip.as_str())
    );

    for v in ["2", "1", "0"] {
        let c = f.dir.path().join(format!("clone-v{v}"));
        f.ok(
            &alice,
            f.dir.path(),
            &["-c", &protocol(v), "clone", "-q", &url, c.to_str().unwrap()],
        )
        .await;
        assert_clone(&f.home, &c, "origin/main", &tip).await;

        // Push from the clone in this version, then fetch it back into
        // the first working repository in this version.
        let old = tip.clone();
        tip = f
            .commit(&c, &format!("v{v}.txt"), &format!("pushed over v{v}"))
            .await;
        f.ok(
            &alice,
            &c,
            &["-c", &protocol(v), "push", "-q", "origin", "main"],
        )
        .await;
        assert_eq!(
            f.tip("refs/heads/main").await.as_deref(),
            Some(tip.as_str())
        );
        let mut fetch = f.git(
            &alice,
            &w,
            &[
                "-c",
                &protocol(v),
                "fetch",
                "-q",
                &url,
                "main:refs/remotes/o/main",
            ],
        );
        fetch.env("GIT_TRACE_PACKET", "1");
        let out = run(fetch).await;
        assert!(out.status.success(), "{}", text(&out));
        assert_clone(&f.home, &w, "refs/remotes/o/main", &tip).await;
        if v != "2" {
            let trace = String::from_utf8_lossy(&out.stderr);
            assert!(
                trace.contains(&format!("fetch< ACK {old} common")),
                "v{v}: the old tip is acknowledged\n{trace}"
            );
        }
        local_ok(
            &f.home,
            &w,
            &["merge", "-q", "--ff-only", "refs/remotes/o/main"],
        )
        .await;
    }
}

/// A hook that records the caller of every update it is asked about and
/// of every committed group.
#[derive(Debug, Default)]
struct CallerRecorder {
    checked: std::sync::Mutex<Vec<Caller>>,
    committed: std::sync::Mutex<Vec<Caller>>,
}

impl PushHook for CallerRecorder {
    fn check<'a>(
        &'a self,
        update: HookUpdate<'a>,
        _objects: &'a mut dyn HookObjects,
    ) -> HookFuture<'a, Result<HookVerdict, aspen_core::error::StoreError>> {
        Box::pin(async move {
            self.checked.lock().unwrap().push(update.caller.clone());
            Ok(HookVerdict::Accept)
        })
    }

    fn committed<'a>(
        &'a self,
        _repo: &'a Repo,
        caller: &'a Caller,
        _updates: &'a [CommittedUpdate],
    ) -> HookFuture<'a, ()> {
        Box::pin(async move {
            self.committed.lock().unwrap().push(caller.clone());
        })
    }
}

/// Invariant (C7 on C3): a push hook sees an SSH pusher as the subject its
/// key is registered to, with `subject:<id>` as its rate-limit key, the
/// key the access layer charged the push to (so a hook that limits by
/// caller, such as the place demo's, shares the bucket across HTTP and
/// SSH). Both the check and the committed notice carry it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_push_hook_sees_the_ssh_subject_as_the_caller() {
    let hook = Arc::new(CallerRecorder::default());
    let f = Fixture::start_hooked(
        LimitsConfig::default(),
        |_| {},
        Some(hook.clone() as Arc<dyn PushHook>),
    )
    .await;
    let alice = f.registered("alice", &["write:demo"]).await;
    let w = f.work("w").await;
    let tip = f.commit(&w, "a.txt", "one").await;
    f.ok(&alice, &w, &["push", "-q", &f.url("demo.git"), "main"])
        .await;
    assert_eq!(
        f.tip("refs/heads/main").await.as_deref(),
        Some(tip.as_str())
    );
    let want = Caller {
        principal: "user:alice".to_owned(),
        key: "subject:user:alice".to_owned(),
    };
    assert_eq!(*hook.checked.lock().unwrap(), vec![want.clone()]);
    assert_eq!(*hook.committed.lock().unwrap(), vec![want]);
}

/// Invariant (A1b's C3 obligation): with `no-done`, git keeps sending
/// rounds of haves after the round the server answers `ready`, then reads
/// the pack. The server sends the pack at once and leaves those rounds
/// unread; the fetch completes. The trace proves the client did send a
/// round after the one holding the common commit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pipelined_haves_after_ready_are_left_unread() {
    let f = Fixture::start().await;
    let alice = f.registered("alice", &["write:demo"]).await;
    let url = f.url("demo.git");
    let w = f.work("w").await;
    // 60 commits of history ending at `base`, all pushed; then 20 local
    // commits on top that the server never sees, newer than `base`. The
    // negotiator sends the local ones first (16, then 32 per round), so
    // `base` lands in the second round, and git has already sent a third
    // round (base's ancestors) when it reads the answer `ready` to the
    // second.
    fast_import(
        &f.home,
        &w,
        &history("refs/heads/main", None, 60, 1_900_000_000),
    )
    .await;
    local_ok(&f.home, &w, &["checkout", "-q", "main"]).await;
    let base = local_ok(&f.home, &w, &["rev-parse", "main"])
        .await
        .trim()
        .to_owned();
    f.ok(&alice, &w, &["push", "-q", &url, "main"]).await;
    fast_import(
        &f.home,
        &w,
        &history("refs/heads/local", Some(&base), 20, 2_000_000_000),
    )
    .await;

    let other = f.work("other").await;
    local_ok(
        &f.home,
        &other,
        &["pull", "-q", w.to_str().unwrap(), "main"],
    )
    .await;
    let new = f.commit(&other, "b.txt", "new on the server").await;
    f.ok(&alice, &other, &["push", "-q", &url, "main"]).await;

    let mut fetch = f.git(
        &alice,
        &w,
        &[
            "-c",
            "protocol.version=0",
            "fetch",
            &url,
            "main:refs/remotes/o/main",
        ],
    );
    fetch.env("GIT_TRACE_PACKET", "1");
    let out = run(fetch).await;
    assert!(out.status.success(), "{}", text(&out));
    assert_clone(&f.home, &w, "refs/remotes/o/main", &new).await;
    if std::env::var_os("ASPEN_SSH_TEST_TRACE").is_some() {
        eprintln!("{}", String::from_utf8_lossy(&out.stderr));
    }

    let trace = String::from_utf8_lossy(&out.stderr);
    let sent: Vec<&str> = trace
        .lines()
        .filter_map(|l| l.split_once("fetch> ").map(|(_, p)| p))
        .collect();
    let base_at = sent
        .iter()
        .position(|p| *p == format!("have {base}"))
        .unwrap_or_else(|| panic!("base was never sent\n{trace}"));
    let flush_after = base_at
        + sent[base_at..]
            .iter()
            .position(|p| *p == "0000")
            .expect("the round with base ends");
    assert!(
        sent[flush_after + 1..]
            .iter()
            .any(|p| p.starts_with("have ")),
        "the client pipelined no round after the one holding base\n{trace}"
    );
    assert!(
        trace.contains(&format!("fetch< ACK {base} ready")),
        "{trace}"
    );
}

/// A fast-import script: `n` commits on `branch`, one second apart from
/// `time`, starting from `from` (or a root).
fn history(branch: &str, from: Option<&str>, n: u32, time: u32) -> String {
    let mut script = String::new();
    for i in 0..n {
        let message = format!("{branch} {i}");
        script.push_str(&format!(
            "commit {branch}\ncommitter A <a@x> {} +0000\ndata {}\n{message}\n",
            time + i,
            message.len()
        ));
        if i == 0
            && let Some(from) = from
        {
            script.push_str(&format!("from {from}\n"));
        }
        script.push_str(&format!(
            "M 644 inline f.txt\ndata {}\n{message}\n\n",
            message.len()
        ));
    }
    script
}

async fn fast_import(home: &Path, dir: &Path, script: &str) {
    use tokio::io::AsyncWriteExt;
    let mut cmd = git_local(home, dir, &["fast-import", "--quiet", "--force"]);
    cmd.stdin(std::process::Stdio::piped());
    let mut child = cmd.spawn().unwrap();
    let mut stdin = child.stdin.take().unwrap();
    stdin.write_all(script.as_bytes()).await.unwrap();
    drop(stdin);
    assert!(child.wait().await.unwrap().success());
}

/// Invariant: a shallow clone and a deepen over SSH give the boundary stock
/// git gives, in v0 (where the shallow section must reach the client before
/// it sends any have) and in v2.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shallow_clone_and_deepen_in_v0_and_v2() {
    let f = Fixture::start().await;
    let alice = f.registered("alice", &["write:demo"]).await;
    let url = f.url("demo.git");
    let w = f.work("w").await;
    let mut tips = Vec::new();
    for i in 0..4 {
        tips.push(f.commit(&w, "a.txt", &format!("commit {i}")).await);
    }
    f.ok(&alice, &w, &["push", "-q", &url, "main"]).await;
    for v in ["0", "2"] {
        let c = f.dir.path().join(format!("shallow-v{v}"));
        f.ok(
            &alice,
            f.dir.path(),
            &[
                "-c",
                &protocol(v),
                "clone",
                "-q",
                "--depth=1",
                &url,
                c.to_str().unwrap(),
            ],
        )
        .await;
        let shallow = std::fs::read_to_string(c.join(".git/shallow")).unwrap();
        assert_eq!(shallow.trim(), tips[3], "v{v}");
        f.ok(
            &alice,
            &c,
            &["-c", &protocol(v), "fetch", "-q", "--deepen=1"],
        )
        .await;
        let shallow = std::fs::read_to_string(c.join(".git/shallow")).unwrap();
        assert_eq!(shallow.trim(), tips[2], "v{v}");
        local_ok(&f.home, &c, &["fsck", "--strict", "--no-dangling"]).await;
    }
}

/// Invariant: a push through a branch-scoped URL over SSH sees that branch
/// (so stock `--delete` and `--force-with-lease` work), and a v0 listing
/// through it shows `HEAD`, trunk and the branch, as over HTTP.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn branch_scoped_urls_over_ssh() {
    let f = Fixture::start().await;
    let alice = f.registered("alice", &["write:demo"]).await;
    let url = f.url("demo.git");
    let scoped = f.url("demo.git/-/refs/heads/topic");
    let w = f.work("w").await;
    let main = f.commit(&w, "a.txt", "main").await;
    f.ok(&alice, &w, &["push", "-q", &url, "main"]).await;
    let topic = f.commit(&w, "b.txt", "topic").await;
    f.ok(&alice, &w, &["push", "-q", &url, "HEAD:refs/heads/topic"])
        .await;
    assert_eq!(
        f.tip("refs/heads/topic").await.as_deref(),
        Some(topic.as_str())
    );

    let listed = f
        .ok(
            &alice,
            &w,
            &["-c", "protocol.version=0", "ls-remote", &scoped],
        )
        .await;
    let mut refs: Vec<String> = listed.lines().map(str::to_owned).collect();
    refs.sort_unstable();
    let mut want = vec![
        format!("{main}\tHEAD"),
        format!("{main}\trefs/heads/main"),
        format!("{topic}\trefs/heads/topic"),
    ];
    want.sort_unstable();
    assert_eq!(refs, want);
    // In v2, `ls-refs` through a scoped URL lists exactly what the plain
    // URL lists (the B2 decision: v2 is bounded by `ref-prefix`, so the
    // scope narrows nothing), including a branch the v0 listing above hid.
    f.ok(
        &alice,
        &w,
        &["push", "-q", &url, &format!("{main}:refs/heads/other")],
    )
    .await;
    let scoped_v2 = sorted_v2_refs(&f, &alice, &w, &scoped).await;
    assert!(
        scoped_v2.contains(&format!("{main}\trefs/heads/other")),
        "{scoped_v2:?}"
    );
    assert!(
        scoped_v2.contains(&format!("{topic}\trefs/heads/topic")),
        "{scoped_v2:?}"
    );
    assert_eq!(scoped_v2, sorted_v2_refs(&f, &alice, &w, &url).await);
    f.ok(
        &alice,
        &w,
        &[
            "push",
            "-q",
            &format!("--force-with-lease=topic:{topic}"),
            &scoped,
            &format!("{main}:refs/heads/topic"),
        ],
    )
    .await;
    assert_eq!(
        f.tip("refs/heads/topic").await.as_deref(),
        Some(main.as_str())
    );
    f.ok(&alice, &w, &["push", "-q", "--delete", &scoped, "topic"])
        .await;
    assert_eq!(f.tip("refs/heads/topic").await, None);
}

/// `git ls-remote` in protocol v2, sorted.
async fn sorted_v2_refs(f: &Fixture, u: &User, dir: &Path, url: &str) -> Vec<String> {
    let out = f
        .ok(u, dir, &["-c", "protocol.version=2", "ls-remote", url])
        .await;
    let mut refs: Vec<String> = out.lines().map(str::to_owned).collect();
    refs.sort_unstable();
    refs
}

/// Invariant: a key reaches exactly what it was registered for. A
/// read-only key fetches but cannot push; a key whose write grant names
/// one ref namespace pushes there and is refused (`ng`) elsewhere; a key
/// with no grant on a repository sees the same answer as for a repository
/// that does not exist.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn each_key_reaches_what_it_was_registered_for() {
    let f = Fixture::start().await;
    let alice = f.registered("alice", &["write:demo"]).await;
    let bob = f.registered("bob", &["read:demo"]).await;
    let dave = f
        .registered("dave", &["write:demo:refs/heads/dave/*"])
        .await;
    let eve = f.registered("eve", &["read:other"]).await;
    let url = f.url("demo.git");
    let w = f.work("w").await;
    let tip = f.commit(&w, "a.txt", "one").await;
    f.ok(&alice, &w, &["push", "-q", &url, "main"]).await;

    let c = f.dir.path().join("bob");
    f.ok(
        &bob,
        f.dir.path(),
        &["clone", "-q", &url, c.to_str().unwrap()],
    )
    .await;
    assert_clone(&f.home, &c, "origin/main", &tip).await;
    f.commit(&w, "b.txt", "two").await;
    let err = f.fails(&bob, &w, &["push", &url, "main"]).await;
    assert!(err.contains("remote error: permission denied"), "{err}");
    assert_eq!(
        f.tip("refs/heads/main").await.as_deref(),
        Some(tip.as_str())
    );

    let out = f
        .ok(
            &dave,
            &w,
            &["push", "--porcelain", &url, "main:refs/heads/dave/x"],
        )
        .await;
    assert!(out.contains("refs/heads/dave/x"), "{out}");
    assert!(f.tip("refs/heads/dave/x").await.is_some());
    let err = f.fails(&dave, &w, &["push", &url, "main"]).await;
    assert!(
        err.contains("[remote rejected] main -> main (permission denied)"),
        "{err}"
    );
    assert_eq!(
        f.tip("refs/heads/main").await.as_deref(),
        Some(tip.as_str())
    );

    let hidden = f.fails(&eve, &w, &["ls-remote", &url]).await;
    let missing = f.fails(&eve, &w, &["ls-remote", &f.url("nope.git")]).await;
    assert!(
        hidden.contains("remote error: repository not found"),
        "{hidden}"
    );
    assert_eq!(hidden, missing);
    let bad = f
        .fails(&alice, &w, &["ls-remote", &f.url("../etc/passwd")])
        .await;
    assert!(bad.contains("remote error: repository not found"), "{bad}");
}

/// Invariant: only registered keys of accepted types authenticate. An
/// unregistered key, an RSA key (refused at registration too) and a
/// revoked key all fail at the SSH layer, before any command runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn only_registered_live_keys_authenticate() {
    let f = Fixture::start().await;
    let alice = f.registered("alice", &["write:demo"]).await;
    let w = f.work("w").await;
    f.commit(&w, "a.txt", "one").await;
    let url = f.url("demo.git");
    f.ok(&alice, &w, &["push", "-q", &url, "main"]).await;

    let mallory = f.user("mallory", &["-t", "ed25519"]);
    let err = f.fails(&mallory, &w, &["ls-remote", &url]).await;
    assert!(err.contains("Permission denied (publickey)"), "{err}");

    let carol = f.user("carol", &["-t", "rsa", "-b", "2048"]);
    let refused = SshPublicKey::parse_openssh(&carol.public).unwrap_err();
    assert!(refused.to_string().contains("ssh-rsa"), "{refused}");
    let err = f.fails(&carol, &w, &["ls-remote", &url]).await;
    assert!(err.contains("Permission denied (publickey)"), "{err}");

    let bob = f.user("bob", &["-t", "ecdsa", "-b", "256"]);
    f.register(&bob, "bob", &["read:demo"]).await;
    f.ok(&bob, &w, &["ls-remote", &url]).await;
    let fp = SshPublicKey::parse_openssh(&bob.public)
        .unwrap()
        .fingerprint();
    assert!(f.keys.revoke(&fp, UnixMicros::now()).await.unwrap());
    let err = f.fails(&bob, &w, &["ls-remote", &url]).await;
    assert!(err.contains("Permission denied (publickey)"), "{err}");
}

/// Invariant: a key is identified again for every command, so revoking it
/// stops the next command even on a connection that authenticated before
/// the revocation (an OpenSSH control master reusing one connection).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_revoked_key_is_refused_on_a_connection_it_opened_before() {
    let f = Fixture::start().await;
    let alice = f.registered("alice", &["write:demo"]).await;
    let w = f.work("w").await;
    let url = f.url("demo.git");
    f.commit(&w, "a.txt", "one").await;
    f.ok(&alice, &w, &["push", "-q", &url, "main"]).await;

    let bob = f.registered("bob", &["read:demo"]).await;
    // A short path: control sockets have a length limit.
    let control = tempfile::Builder::new()
        .prefix("cm")
        .tempdir_in("/tmp")
        .unwrap();
    let master = format!(
        "-o ControlMaster=auto -o ControlPersist=60 -o ControlPath={}/m",
        control.path().display()
    );
    let ls = |extra: &str| f.git_via(&bob, extra, &w, &["ls-remote", &url]);
    let out = run(ls(&master)).await;
    assert!(out.status.success(), "{}", text(&out));
    let mut check = tokio::process::Command::new("sh");
    check.arg("-c").arg(format!(
        "{} -O check -p {} git@127.0.0.1",
        f.ssh_command(&bob, &master),
        f.port
    ));
    let out = run(check).await;
    assert!(
        out.status.success(),
        "the control master is up\n{}",
        text(&out)
    );

    let fp = SshPublicKey::parse_openssh(&bob.public)
        .unwrap()
        .fingerprint();
    assert!(f.keys.revoke(&fp, UnixMicros::now()).await.unwrap());
    let out = run(ls(&master)).await;
    assert!(!out.status.success(), "{}", text(&out));
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(
        err.contains("remote error: authentication required"),
        "{err}"
    );

    let mut exit = tokio::process::Command::new("sh");
    exit.arg("-c").arg(format!(
        "{} -O exit -p {} git@127.0.0.1",
        f.ssh_command(&bob, &master),
        f.port
    ));
    run(exit).await;
}

/// A pkt-line: `0000` flush, `0001` delim, `0002` response end, or data.
#[derive(Debug, PartialEq, Eq)]
enum Pkt {
    Flush,
    Delim,
    End,
    Data(Vec<u8>),
}

async fn read_pkt<R: tokio::io::AsyncRead + Unpin>(r: &mut R) -> Pkt {
    use tokio::io::AsyncReadExt;
    let mut len = [0u8; 4];
    tokio::time::timeout(COMMAND_TIMEOUT, r.read_exact(&mut len))
        .await
        .expect("timed out reading a pkt-line")
        .expect("pkt-line length");
    let len = usize::from_str_radix(std::str::from_utf8(&len).unwrap(), 16).unwrap();
    match len {
        0 => Pkt::Flush,
        1 => Pkt::Delim,
        2 => Pkt::End,
        n => {
            let mut data = vec![0; n - 4];
            r.read_exact(&mut data).await.expect("pkt-line data");
            Pkt::Data(data)
        }
    }
}

/// Every data line up to the next flush.
async fn read_until_flush<R: tokio::io::AsyncRead + Unpin>(r: &mut R) -> Vec<String> {
    let mut lines = Vec::new();
    loop {
        match read_pkt(r).await {
            Pkt::Flush => return lines,
            Pkt::Data(d) => lines.push(String::from_utf8_lossy(&d).into_owned()),
            other => panic!("unexpected {other:?} after {lines:?}"),
        }
    }
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

/// Invariant: a protocol v2 session decides every command as HTTP decides
/// every POST. Revoking a key ends a v2 session that is already running:
/// the next command on the same stream is refused with `ERR`, and nothing
/// more is served.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_revoked_key_is_refused_by_the_next_command_of_a_running_v2_session() {
    use tokio::io::AsyncWriteExt;
    let f = Fixture::start().await;
    let alice = f.registered("alice", &["write:demo"]).await;
    let w = f.work("w").await;
    let url = f.url("demo.git");
    let tip = f.commit(&w, "a.txt", "one").await;
    f.ok(&alice, &w, &["push", "-q", &url, "main"]).await;

    let bob = f.registered("bob", &["read:demo"]).await;
    let mut ssh = tokio::process::Command::new("sh");
    ssh.arg("-c")
        .arg(format!(
            "{} -o SetEnv=GIT_PROTOCOL=version=2 -p {} git@127.0.0.1 \"git-upload-pack 'demo.git'\"",
            f.ssh_command(&bob, ""),
            f.port
        ))
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);
    let mut child = ssh.spawn().unwrap();
    let mut stdin = child.stdin.take().unwrap();
    let mut stdout = child.stdout.take().unwrap();

    let caps = read_until_flush(&mut stdout).await;
    assert_eq!(
        caps.first().map(String::as_str),
        Some("version 2\n"),
        "{caps:?}"
    );
    let listed = format!("{tip} refs/heads/main\n");
    // Two commands on one stream while the key is live: both are served.
    for _ in 0..2 {
        stdin.write_all(&ls_refs_request()).await.unwrap();
        stdin.flush().await.unwrap();
        assert_eq!(read_until_flush(&mut stdout).await, vec![listed.clone()]);
    }

    let fp = SshPublicKey::parse_openssh(&bob.public)
        .unwrap()
        .fingerprint();
    assert!(f.keys.revoke(&fp, UnixMicros::now()).await.unwrap());
    stdin.write_all(&ls_refs_request()).await.unwrap();
    stdin.flush().await.unwrap();
    match read_pkt(&mut stdout).await {
        Pkt::Data(d) => {
            let d = String::from_utf8_lossy(&d);
            assert!(d.starts_with("ERR "), "{d}");
            assert!(d.contains("authentication required"), "{d}");
        }
        other => panic!("expected ERR, got {other:?}"),
    }
    drop(stdin);
    let out = tokio::time::timeout(COMMAND_TIMEOUT, child.wait_with_output())
        .await
        .expect("the session ends after the refusal")
        .unwrap();
    assert!(
        out.stdout.is_empty(),
        "nothing served after ERR: {}",
        text(&out)
    );
    assert_eq!(out.status.code(), Some(1), "{}", text(&out));
}

/// Invariant: SSH gives no shell and runs nothing but Git's two services;
/// `ssh -T` names the subject a key acts as.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_shell_and_no_other_commands() {
    let f = Fixture::start().await;
    let alice = f.registered("alice", &["read:demo"]).await;
    let ssh = |args: &str| {
        let mut c = tokio::process::Command::new("sh");
        c.arg("-c").arg(format!(
            "{} -p {} git@127.0.0.1 {args}",
            f.ssh_command(&alice, ""),
            f.port
        ));
        c
    };
    let out = run(ssh("-T")).await;
    assert_eq!(out.status.code(), Some(1), "{}", text(&out));
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("Hi user:alice!"),
        "{}",
        text(&out)
    );
    for command in [
        "ls /",
        "'git-upload-archive /demo.git'",
        "'git-upload-pack /demo.git; id'",
    ] {
        let out = run(ssh(command)).await;
        assert_eq!(out.status.code(), Some(1), "{command}: {}", text(&out));
        assert!(out.stdout.is_empty(), "{command}: {}", text(&out));
        assert!(
            String::from_utf8_lossy(&out.stderr).contains("runs only git-upload-pack"),
            "{command}: {}",
            text(&out)
        );
    }
    // `-W` opens a direct-tcpip channel at once; it must be refused.
    let out = run(ssh("-W 127.0.0.1:1")).await;
    assert!(
        !out.status.success(),
        "port forwarding refused\n{}",
        text(&out)
    );
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(
        err.contains("forwarding failed") || err.contains("prohibited"),
        "{}",
        text(&out)
    );
}

/// Invariant: SSH commands draw on the same per-subject request bucket as
/// HTTP requests, and a v2 session pays for every command it runs, as HTTP
/// pays per POST.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rate_limits_count_every_v2_command() {
    let f = Fixture::start_with(LimitsConfig {
        read: Some(RateLimit {
            burst: 3,
            per_second: 0.001,
        }),
        ..LimitsConfig::default()
    })
    .await;
    let alice = f.registered("alice", &["write:demo"]).await;
    let w = f.work("w").await;
    let url = f.url("demo.git");
    f.commit(&w, "a.txt", "one").await;
    f.ok(&alice, &w, &["push", "-q", &url, "main"]).await;
    // A v2 clone runs `ls-refs` and `fetch`: two requests of three.
    let c = f.dir.path().join("c");
    f.ok(
        &alice,
        f.dir.path(),
        &[
            "-c",
            "protocol.version=2",
            "clone",
            "-q",
            &url,
            c.to_str().unwrap(),
        ],
    )
    .await;
    f.ok(&alice, &w, &["-c", "protocol.version=2", "ls-remote", &url])
        .await;
    let err = f
        .fails(&alice, &w, &["-c", "protocol.version=2", "ls-remote", &url])
        .await;
    assert!(err.contains("remote error: rate limit exceeded"), "{err}");
}

/// Invariant: the push quota applies over SSH as over HTTP. A pack over
/// the per-push cap is refused while it streams, and nothing is stored:
/// no ref moves and no pack is written.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_push_quota_applies_over_ssh() {
    let f = Fixture::start_with(LimitsConfig {
        quota: PushQuota {
            max_pack_bytes: Some(64 * 1024),
            ..PushQuota::default()
        },
        ..LimitsConfig::default()
    })
    .await;
    let alice = f.registered("alice", &["write:demo"]).await;
    let w = f.work("w").await;
    let url = f.url("demo.git");
    let small = f.commit(&w, "a.txt", "small").await;
    f.ok(&alice, &w, &["push", "-q", &url, "main"]).await;
    let packs = || {
        walk_files(&f.dir.path().join("blobs"))
            .into_iter()
            .filter(|p| p.ends_with(".pack"))
            .count()
    };
    let packs_before = packs();
    // The first push stored a pack, so the pattern matches real blob keys
    // and the "no pack stored" check below is not vacuous.
    assert!(
        packs_before > 0,
        "{:?}",
        walk_files(&f.dir.path().join("blobs"))
    );
    // Incompressible content, well over the cap.
    let mut x = 0x9e37_79b9_7f4a_7c15u64;
    let noise: String = (0..50_000)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            format!("{:08x}", x as u32)
        })
        .collect();
    f.commit(&w, "big.txt", &noise).await;
    let err = f.fails(&alice, &w, &["push", &url, "main"]).await;
    assert!(err.contains("push too large"), "{err}");
    assert_eq!(
        f.tip("refs/heads/main").await.as_deref(),
        Some(small.as_str())
    );
    assert_eq!(packs(), packs_before, "no pack stored");
}

/// A russh client, for what OpenSSH does not show: the answer to each
/// channel open and channel request.
struct RawClient;

impl russh::client::Handler for RawClient {
    type Error = russh::Error;

    async fn check_server_key(
        &mut self,
        _key: &russh::keys::PublicKeyOrCertificate,
    ) -> Result<bool, Self::Error> {
        // The test talks to its own server on loopback.
        Ok(true)
    }
}

async fn raw_connect(f: &Fixture) -> russh::client::Handle<RawClient> {
    let config = Arc::new(russh::client::Config::default());
    russh::client::connect(config, ("127.0.0.1", f.port), RawClient)
        .await
        .unwrap()
}

/// A raw connection authenticated as `u`.
async fn raw_signed_in(f: &Fixture, u: &User) -> russh::client::Handle<RawClient> {
    let mut h = raw_connect(f).await;
    let key = russh::keys::load_secret_key(&u.key, None).unwrap();
    let auth = h
        .authenticate_publickey(
            "git",
            russh::keys::PrivateKeyWithHashAlg::new(Arc::new(key), None),
        )
        .await
        .unwrap();
    assert!(auth.success(), "{auth:?}");
    h
}

/// The server's answer to a request sent with `want_reply`.
async fn reply(ch: &mut russh::Channel<russh::client::Msg>) -> bool {
    loop {
        match tokio::time::timeout(COMMAND_TIMEOUT, ch.wait())
            .await
            .expect("timed out waiting for a reply")
        {
            Some(russh::ChannelMsg::Success) => return true,
            Some(russh::ChannelMsg::Failure) => return false,
            Some(_) => {}
            None => panic!("channel closed before the reply"),
        }
    }
}

/// Invariant: a connection holds at most `SshConfig::max_sessions` session
/// channels at once; one more is refused (administratively prohibited),
/// and closing one frees its place.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sessions_per_connection_are_capped() {
    let f = Fixture::start_configured(LimitsConfig::default(), |c| c.max_sessions = 2).await;
    let alice = f.registered("alice", &["read:demo"]).await;
    let h = raw_signed_in(&f, &alice).await;
    let first = h.channel_open_session().await.unwrap();
    let _second = h.channel_open_session().await.unwrap();
    match h.channel_open_session().await {
        Err(russh::Error::ChannelOpenFailure(
            russh::ChannelOpenFailure::AdministrativelyProhibited,
        )) => {}
        other => panic!(
            "a third session was not refused: {:?}",
            other.map(|c| c.id())
        ),
    }
    first.close().await.unwrap();
    // The close reaches the server asynchronously; the freed place shows
    // up within a moment.
    let mut reopened = false;
    for _ in 0..50 {
        if h.channel_open_session().await.is_ok() {
            reopened = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(reopened, "closing a session frees its place");
}

/// Invariant: no channel opens without a verified key. A client that
/// skips authentication, or whose `none` attempt is refused, gets no
/// session channel.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_channel_opens_without_a_verified_key() {
    let f = Fixture::start().await;
    f.registered("alice", &["write:demo"]).await;
    let mut h = raw_connect(&f).await;
    // Whatever the answer to `none` (refused), the session must not open.
    let _ = h.authenticate_none("git").await;
    match tokio::time::timeout(Duration::from_secs(2), h.channel_open_session()).await {
        Err(_) | Ok(Err(_)) => {}
        Ok(Ok(ch)) => panic!("a channel opened without a key: {:?}", ch.id()),
    }
}

/// Invariant: `GIT_PROTOCOL` is the only environment variable a session
/// accepts; any other is refused (a channel failure), so nothing a
/// client sets reaches the server beyond the protocol version.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn only_git_protocol_is_accepted_from_the_environment() {
    let f = Fixture::start().await;
    let alice = f.registered("alice", &["read:demo"]).await;
    let h = raw_signed_in(&f, &alice).await;
    let mut ch = h.channel_open_session().await.unwrap();
    for name in ["LD_PRELOAD", "PATH", "GIT_DIR", "git_protocol"] {
        ch.set_env(true, name, "x").await.unwrap();
        assert!(!reply(&mut ch).await, "{name} was accepted");
    }
    let long = "x".repeat(257);
    ch.set_env(true, "GIT_PROTOCOL", long).await.unwrap();
    assert!(
        !reply(&mut ch).await,
        "an over-long GIT_PROTOCOL was accepted"
    );
    ch.set_env(true, "GIT_PROTOCOL", "version=2").await.unwrap();
    assert!(reply(&mut ch).await, "GIT_PROTOCOL was refused");
}

fn walk_files(dir: &Path) -> Vec<String> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(&d).into_iter().flatten().flatten() {
            let p = e.path();
            if p.is_dir() {
                stack.push(p);
            } else {
                out.push(p.display().to_string());
            }
        }
    }
    out
}
