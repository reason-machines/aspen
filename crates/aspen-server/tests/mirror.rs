//! Mirror mode end to end (node C4): Aspen in front of a stock
//! `git http-backend` upstream that requires a credential, driven by stock
//! `git` clients. Each test checks observable state on both sides: the
//! upstream's refs (`git for-each-ref` on its bare repository), Aspen's
//! refs, and clones from Aspen that pass `git fsck --strict`.

mod common;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use aspen_blob::LocalBlobStore;
use aspen_core::error::StoreError;
use aspen_core::id::{ObjectId, RepoId, UnixMicros};
use aspen_core::meta::{JobTag, MetaStore, RefListQuery};
use aspen_core::name::{RefName, RepoName};
use aspen_core::refs::Closure;
use aspen_core::repo::{Repo, RepoSettings};
use aspen_meta::SqlStore;
use aspen_mirror::credential::{CredentialKind, GitHubApp, Secret};
use aspen_mirror::{
    CredentialName, Credentials, JobWorker, LedgerEntry, MirrorConfig, MirrorError, MirrorMode,
    MirrorRecord, MirrorStore, RefOutcome, Side, UpstreamUrl,
};
use aspen_pack::{MaxInflatedBytes, MaxObjectBytes};
use aspen_server::mirror::{ServerMirror, SqlMirrors};
use aspen_server::receive::ReceiveConfig;
use aspen_testkit::git::{init_bare, isolated_git};
use aspen_testkit::http_backend::GitHttpBackend;

const MIB: u64 = 1 << 20;
const TOKEN: &str = "s3cret-upstream-token";

type TestMirror = ServerMirror<SqlStore, LocalBlobStore, FlakyMirrors>;

/// The real mirror store, whose ledger writes fail while `fail_ledger` is
/// set (a database that is briefly unavailable).
#[derive(Debug, Clone)]
struct FlakyMirrors {
    inner: SqlMirrors,
    fail_ledger: Arc<AtomicBool>,
}

impl MirrorStore for FlakyMirrors {
    async fn create_mirror(
        &self,
        name: &RepoName,
        settings: &RepoSettings,
        record: impl FnOnce(RepoId) -> MirrorRecord + Send,
    ) -> Result<Repo, StoreError> {
        self.inner.create_mirror(name, settings, record).await
    }
    async fn mirror(&self, repo: RepoId) -> Result<Option<MirrorRecord>, StoreError> {
        self.inner.mirror(repo).await
    }
    async fn mirrors(&self) -> Result<Vec<MirrorRecord>, StoreError> {
        self.inner.mirrors().await
    }
    async fn delete_mirror(&self, repo: RepoId) -> Result<bool, StoreError> {
        self.inner.delete_mirror(repo).await
    }
    async fn record_sync(
        &self,
        repo: RepoId,
        at: UnixMicros,
        error: Option<&str>,
    ) -> Result<(), StoreError> {
        self.inner.record_sync(repo, at, error).await
    }
    async fn ledger(&self, repo: RepoId) -> Result<Vec<(RefName, LedgerEntry)>, StoreError> {
        self.inner.ledger(repo).await
    }
    async fn ledger_entry(&self, repo: RepoId, name: &RefName) -> Result<LedgerEntry, StoreError> {
        self.inner.ledger_entry(repo, name).await
    }
    async fn set_ledger(
        &self,
        repo: RepoId,
        name: &RefName,
        expected: Option<ObjectId>,
        next: LedgerEntry,
        at: UnixMicros,
    ) -> Result<bool, StoreError> {
        if self.fail_ledger.load(Ordering::SeqCst) {
            return Err(StoreError::Unavailable {
                reason: "injected: the database is unavailable".into(),
            });
        }
        self.inner.set_ledger(repo, name, expected, next, at).await
    }
    async fn conflict_count(&self) -> Result<u64, StoreError> {
        self.inner.conflict_count().await
    }
}

/// Runs git in `dir`, on a blocking thread; returns success and output.
async fn git(home: &Path, dir: &Path, args: &[&str]) -> (bool, String) {
    let mut cmd = isolated_git(home);
    cmd.current_dir(dir).args(args);
    tokio::task::spawn_blocking(move || {
        let o = cmd.output().unwrap();
        let mut t = String::from_utf8_lossy(&o.stdout).into_owned();
        t.push_str(&String::from_utf8_lossy(&o.stderr));
        (o.status.success(), t)
    })
    .await
    .unwrap()
}

async fn git_ok(home: &Path, dir: &Path, args: &[&str]) -> String {
    let (ok, out) = git(home, dir, args).await;
    assert!(ok, "git {args:?} in {}:\n{out}", dir.display());
    out
}

/// Branches and tags of a repository, as `git for-each-ref` sees them.
async fn refs_of(home: &Path, repo: &Path) -> BTreeMap<String, String> {
    git_ok(
        home,
        repo,
        &[
            "for-each-ref",
            "--format=%(refname) %(objectname)",
            "refs/heads",
            "refs/tags",
        ],
    )
    .await
    .lines()
    .map(|l| {
        let (n, o) = l.split_once(' ').unwrap();
        (n.to_owned(), o.to_owned())
    })
    .collect()
}

/// An upstream (stock git behind a credential check) and an Aspen server
/// with mirror mode.
struct Env {
    dir: tempfile::TempDir,
    home: PathBuf,
    meta: SqlStore,
    mirror: Arc<TestMirror>,
    /// Makes the mirror store's ledger writes fail while set.
    fail_ledger: Arc<AtomicBool>,
    base: String,
    upstream: GitHttpBackend,
    /// The upstream's bare repository.
    bare: PathBuf,
    /// A working repository that writes to the upstream directly (as
    /// someone pushing to GitHub would).
    direct: PathBuf,
}

fn credentials(extra: Vec<(CredentialName, CredentialKind)>) -> Credentials {
    let mut list = vec![(
        CredentialName::new("up").unwrap(),
        CredentialKind::Token {
            username: None,
            token: Secret::new(TOKEN),
        },
    )];
    list.extend(extra);
    Credentials::new(list)
}

impl Env {
    async fn start() -> Self {
        Self::start_with(credentials(vec![]), &common::basic("git", TOKEN)).await
    }

    /// `authorization`: the header value the upstream requires.
    async fn start_with(creds: Credentials, authorization: &str) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path().join("home");
        std::fs::create_dir_all(&home).unwrap();
        let up_root = dir.path().join("upstream");
        std::fs::create_dir_all(&up_root).unwrap();
        let bare = init_bare(&up_root, "acme.git").unwrap();
        let upstream = GitHttpBackend::start_with_auth(&up_root, authorization).unwrap();

        let url = format!("sqlite://{}?mode=rwc", dir.path().join("meta.db").display());
        let meta = SqlStore::connect(&url).await.unwrap();
        meta.migrate().await.unwrap();
        let blobs = LocalBlobStore::open(dir.path().join("blobs")).unwrap();
        let mut rcfg = ReceiveConfig::new(
            dir.path().join("spool"),
            MaxObjectBytes::new(64 * MIB).unwrap(),
            MaxInflatedBytes::new(1024 * MIB).unwrap(),
        );
        rcfg.thread_limit = Some(2);
        let mut mcfg = MirrorConfig::new(dir.path().join("mirror-work"));
        mcfg.retry_backoff = std::time::Duration::ZERO;
        let fail_ledger = Arc::new(AtomicBool::new(false));
        let store = FlakyMirrors {
            inner: SqlMirrors(meta.clone()),
            fail_ledger: Arc::clone(&fail_ledger),
        };
        let mut scfg = aspen_server::ServerConfig::new(MaxObjectBytes::new(64 * MIB).unwrap());
        // The tests read `/-/metrics` from the one listener they start.
        scfg.metrics_on_git_listener = true;
        let (server, mirror) = aspen_server::Server::new(meta.clone(), blobs, scfg)
            .with_receive_pack(rcfg)
            .unwrap()
            .with_mirror(store, creds, mcfg)
            .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let access = common::access(
            serde_json::json!({ "anonymous": ["write:*"] }),
            aspen_auth::LimitsConfig::default(),
            meta.clone(),
        );
        let app = server.router(access);
        tokio::spawn(async move {
            axum::serve(
                listener,
                app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
            )
            .await
            .unwrap()
        });
        let direct = dir.path().join("direct");
        std::fs::create_dir_all(&direct).unwrap();
        git_ok(&home, &direct, &["init", "-q", "--initial-branch=main"]).await;
        Self {
            home,
            meta,
            mirror,
            fail_ledger,
            base: format!("http://{addr}"),
            upstream,
            bare,
            direct,
            dir,
        }
    }

    fn upstream_url(&self) -> UpstreamUrl {
        UpstreamUrl::parse(&format!("{}/acme.git", self.upstream.base_url())).unwrap()
    }

    fn aspen_url(&self) -> String {
        format!("{}/acme.git", self.base)
    }

    fn scoped(&self, branch: &str) -> String {
        format!("{}/acme.git/-/refs/heads/{branch}", self.base)
    }

    /// Commits in the direct working repository.
    async fn commit(&self, dir: &Path, file: &str, content: &str) -> String {
        let path = dir.join(file);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, content).unwrap();
        git_ok(&self.home, dir, &["add", "-A"]).await;
        git_ok(
            &self.home,
            dir,
            &["commit", "-q", "-m", &format!("{file}: {content}")],
        )
        .await;
        git_ok(&self.home, dir, &["rev-parse", "HEAD"])
            .await
            .trim()
            .to_owned()
    }

    /// Pushes from the direct repository straight into the upstream's bare
    /// repository (not through Aspen).
    async fn push_direct(&self, args: &[&str]) {
        let bare = self.bare.to_str().unwrap().to_owned();
        let mut all = vec!["push", "-q", bare.as_str()];
        all.extend_from_slice(args);
        git_ok(&self.home, &self.direct, &all).await;
    }

    /// History at the upstream: main (3 commits), `feature` off main, an
    /// annotated tag and a lightweight tag.
    async fn seed(&self) {
        let d = &self.direct;
        self.commit(d, "a.txt", "one\n").await;
        self.commit(d, "dir/b.txt", "two\n").await;
        git_ok(&self.home, d, &["tag", "v0"]).await;
        self.commit(d, "a.txt", "three\n").await;
        git_ok(&self.home, d, &["tag", "-a", "v1", "-m", "release 1"]).await;
        git_ok(&self.home, d, &["checkout", "-q", "-b", "feature"]).await;
        self.commit(d, "f.txt", "feature\n").await;
        self.commit(d, "f.txt", "feature 2\n").await;
        git_ok(&self.home, d, &["checkout", "-q", "main"]).await;
        self.push_direct(&["main", "feature", "v0", "v1"]).await;
    }

    async fn create(&self, mode: MirrorMode) -> Repo {
        let (repo, report) = self
            .mirror
            .create(
                &RepoName::new("acme").unwrap(),
                self.upstream_url(),
                mode,
                Some(CredentialName::new("up").unwrap()),
            )
            .await
            .unwrap();
        assert!(report.failures().is_empty(), "{report:?}");
        repo
    }

    async fn upstream_refs(&self) -> BTreeMap<String, String> {
        refs_of(&self.home, &self.bare).await
    }

    async fn aspen_refs(&self, repo: &Repo) -> BTreeMap<String, String> {
        self.meta
            .list_refs(
                repo.id,
                &RefListQuery {
                    prefixes: vec![],
                    after: None,
                    limit: 10_000,
                },
            )
            .await
            .unwrap()
            .into_iter()
            .filter_map(|v| v.value.tip().map(|t| (v.name.to_string(), t.to_string())))
            .collect()
    }

    /// Clones Aspen into a fresh directory, checks it with `fsck --strict`
    /// and returns its branches (as `refs/heads/*`) and tags.
    async fn clone_refs(&self, name: &str) -> BTreeMap<String, String> {
        let path = self.dir.path().join(name);
        git_ok(
            &self.home,
            self.dir.path(),
            &[
                "clone",
                "-q",
                "--mirror",
                &self.aspen_url(),
                path.to_str().unwrap(),
            ],
        )
        .await;
        git_ok(&self.home, &path, &["fsck", "--strict", "--no-dangling"]).await;
        refs_of(&self.home, &path).await
    }

    /// A clone of Aspen with `main` checked out, to push from.
    async fn work(&self, name: &str) -> PathBuf {
        let path = self.dir.path().join(name);
        git_ok(
            &self.home,
            self.dir.path(),
            &["clone", "-q", &self.aspen_url(), path.to_str().unwrap()],
        )
        .await;
        path
    }

    async fn rev(&self, dir: &Path, r: &str) -> String {
        git_ok(&self.home, dir, &["rev-parse", r])
            .await
            .trim()
            .to_owned()
    }

    /// Runs every due `mirror_push` job; returns how many ran.
    async fn run_jobs(&self) -> usize {
        let mut n = 0;
        let mut worker = JobWorker::new("test-worker");
        while self.mirror.run_one_job(&mut worker).await.unwrap() {
            n += 1;
        }
        n
    }

    async fn metrics(&self) -> String {
        reqwest_get(&format!("{}/-/metrics", self.base)).await
    }
}

async fn reqwest_get(url: &str) -> String {
    let url = url.to_owned();
    tokio::task::spawn_blocking(move || {
        use std::io::{Read, Write};
        let rest = url.strip_prefix("http://").unwrap();
        let (host, path) = rest.split_once('/').unwrap();
        let mut s = std::net::TcpStream::connect(host).unwrap();
        write!(
            s,
            "GET /{path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n"
        )
        .unwrap();
        let mut out = String::new();
        s.read_to_string(&mut out).unwrap();
        out
    })
    .await
    .unwrap()
}

fn name(s: &str) -> RefName {
    RefName::new(s).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_import_copies_every_branch_and_tag_through_the_push_path() {
    let e = Env::start().await;
    e.seed().await;
    let repo = e.create(MirrorMode::Proxy).await;
    assert_eq!(
        repo.settings.trunk(),
        &name("refs/heads/main"),
        "trunk follows HEAD"
    );
    let upstream = e.upstream_refs().await;
    assert_eq!(upstream.len(), 4);
    assert_eq!(e.aspen_refs(&repo).await, upstream);
    // What a client gets from Aspen is the upstream's history, complete.
    assert_eq!(e.clone_refs("clone").await, upstream);
    // Imported history is stored like pushed history: trunk and branches
    // exact, each branch depending on trunk rather than holding its history.
    for r in ["refs/heads/main", "refs/heads/feature"] {
        let v = e
            .meta
            .current_version(repo.id, &name(r))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(v.value.closure(), Some(Closure::Exact), "{r}");
    }
    let feature = e
        .meta
        .current_version(repo.id, &name("refs/heads/feature"))
        .await
        .unwrap()
        .unwrap();
    assert!(
        feature.deps.trunk.is_some(),
        "a branch import depends on trunk"
    );
    // The ledger records the agreement on every ref.
    let ledger = e.mirror.store().ledger(repo.id).await.unwrap();
    assert_eq!(ledger.len(), 4);
    for (n, entry) in ledger {
        assert_eq!(entry.synced.unwrap().to_string(), upstream[n.as_str()]);
        assert!(entry.conflict.is_none());
    }
    // A second pass finds nothing to do.
    let report = e.mirror.sync(&repo).await.unwrap();
    assert_eq!(report.count(|o| *o == RefOutcome::InSync), 4, "{report:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_wrong_credential_creates_nothing() {
    let e = Env::start_with(credentials(vec![]), &common::basic("git", "another-token")).await;
    e.seed().await;
    let err = e
        .mirror
        .create(
            &RepoName::new("acme").unwrap(),
            e.upstream_url(),
            MirrorMode::Proxy,
            Some(CredentialName::new("up").unwrap()),
        )
        .await
        .unwrap_err();
    assert!(err.to_string().contains("refused the credential"), "{err}");
    assert!(
        e.meta
            .repo(&RepoName::new("acme").unwrap())
            .await
            .unwrap()
            .is_none(),
        "no repository is created when the upstream cannot be read"
    );
    // An unconfigured credential is refused before anything is contacted.
    let err = e
        .mirror
        .create(
            &RepoName::new("acme").unwrap(),
            e.upstream_url(),
            MirrorMode::Proxy,
            Some(CredentialName::new("nope").unwrap()),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, MirrorError::Refused(_)), "{err}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_sync_follows_every_kind_of_change_made_at_the_upstream() {
    let e = Env::start().await;
    e.seed().await;
    let repo = e.create(MirrorMode::Proxy).await;
    let client = e.work("client").await;
    let d = &e.direct;
    // A fast-forward of trunk, a forced rewrite of a branch, a deleted
    // branch, a new tag and a moved tag.
    e.commit(d, "a.txt", "four\n").await;
    git_ok(&e.home, d, &["checkout", "-q", "--orphan", "rewrite"]).await;
    e.commit(d, "new.txt", "unrelated history\n").await;
    git_ok(&e.home, d, &["branch", "-f", "feature", "rewrite"]).await;
    git_ok(&e.home, d, &["checkout", "-q", "main"]).await;
    git_ok(&e.home, d, &["branch", "-q", "temp", "HEAD~1"]).await;
    e.push_direct(&["temp"]).await;
    e.mirror.sync(&repo).await.unwrap();
    assert!(e.aspen_refs(&repo).await.contains_key("refs/heads/temp"));
    git_ok(&e.home, d, &["tag", "-f", "v0", "main"]).await;
    git_ok(&e.home, d, &["tag", "-a", "v2", "-m", "release 2"]).await;
    e.push_direct(&["-f", "main", "feature", "v0", "v2", ":temp"])
        .await;

    let report = e.mirror.sync(&repo).await.unwrap();
    assert!(report.failures().is_empty(), "{report:?}");
    assert_eq!(
        report.count(|o| *o == RefOutcome::Imported),
        5,
        "{report:?}"
    );
    let upstream = e.upstream_refs().await;
    assert_eq!(e.aspen_refs(&repo).await, upstream);
    assert_eq!(e.clone_refs("after").await, upstream);
    // An existing clone fetches the changes incrementally.
    git_ok(
        &e.home,
        &client,
        &["fetch", "-q", "--prune", "--tags", "--force", "origin"],
    )
    .await;
    git_ok(&e.home, &client, &["fsck", "--strict", "--no-dangling"]).await;
    assert_eq!(
        e.rev(&client, "origin/main").await,
        upstream["refs/heads/main"]
    );
    assert_eq!(
        e.rev(&client, "origin/feature").await,
        upstream["refs/heads/feature"]
    );
    // Trunk stays exact across imports.
    let main = e
        .meta
        .current_version(repo.id, &name("refs/heads/main"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(main.value.closure(), Some(Closure::Exact));

    // A forced rewrite of trunk at the upstream is followed too, whatever
    // the repository's own force policy (the default refuses forced trunk
    // pushes from clients): the upstream already accepted it. Trunk stays
    // exact under the forced-trunk rule.
    git_ok(&e.home, d, &["reset", "-q", "--hard", "HEAD~1"]).await;
    let rewritten = e.commit(d, "a.txt", "rewritten\n").await;
    e.push_direct(&["-f", "main"]).await;
    let report = e.mirror.sync(&repo).await.unwrap();
    assert_eq!(
        report.refs[&name("refs/heads/main")],
        RefOutcome::Imported,
        "{report:?}"
    );
    assert_eq!(e.aspen_refs(&repo).await["refs/heads/main"], rewritten);
    let main = e
        .meta
        .current_version(repo.id, &name("refs/heads/main"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(main.value.closure(), Some(Closure::Exact));
    assert_eq!(e.clone_refs("rewritten").await, e.upstream_refs().await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn proxy_pushes_land_at_the_upstream_before_aspen_accepts_them() {
    let e = Env::start().await;
    e.seed().await;
    let repo = e.create(MirrorMode::Proxy).await;
    let w = e.work("w").await;
    let tip = e.commit(&w, "a.txt", "from a proxy client\n").await;
    git_ok(&e.home, &w, &["push", "-q", &e.aspen_url(), "main"]).await;
    git_ok(&e.home, &w, &["checkout", "-q", "-b", "topic"]).await;
    let topic = e.commit(&w, "t.txt", "topic\n").await;
    git_ok(&e.home, &w, &["push", "-q", &e.scoped("topic"), "topic"]).await;
    let upstream = e.upstream_refs().await;
    assert_eq!(upstream["refs/heads/main"], tip);
    assert_eq!(upstream["refs/heads/topic"], topic);
    assert_eq!(e.aspen_refs(&repo).await, upstream);
    // A branch at an existing commit brings no objects; it is forwarded
    // with an empty pack.
    let main_tip = e.rev(&w, "main").await;
    git_ok(
        &e.home,
        &w,
        &[
            "push",
            "-q",
            &e.scoped("at-main"),
            "main:refs/heads/at-main",
        ],
    )
    .await;
    let upstream = e.upstream_refs().await;
    assert_eq!(upstream["refs/heads/at-main"], main_tip);
    assert_eq!(e.aspen_refs(&repo).await, upstream);
    // The ledger already agrees, so the next pass has nothing to do.
    let report = e.mirror.sync(&repo).await.unwrap();
    assert_eq!(report.count(|o| *o != RefOutcome::InSync), 0, "{report:?}");
    // A delete through the scoped URL is forwarded too.
    git_ok(
        &e.home,
        &w,
        &["push", "-q", &e.scoped("topic"), "--delete", "topic"],
    )
    .await;
    assert!(!e.upstream_refs().await.contains_key("refs/heads/topic"));
    assert!(!e.aspen_refs(&repo).await.contains_key("refs/heads/topic"));
    let metrics = e.metrics().await;
    assert!(
        metrics.contains("aspen_mirror_forwarded_total{result=\"accepted\"} 4"),
        "{metrics}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_push_the_upstream_refuses_is_refused_with_its_reason_and_stored_nowhere() {
    let e = Env::start().await;
    e.seed().await;
    let repo = e.create(MirrorMode::Proxy).await;
    let hook = e.bare.join("hooks/pre-receive");
    std::fs::write(
        &hook,
        "#!/bin/sh\nwhile read old new ref; do\n  case \"$ref\" in refs/heads/protected*) echo \"protected by policy\" >&2; exit 1;; esac\ndone\n",
    )
    .unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&hook, std::fs::Permissions::from_mode(0o755)).unwrap();
    let w = e.work("w").await;
    git_ok(&e.home, &w, &["checkout", "-q", "-b", "protected"]).await;
    e.commit(&w, "p.txt", "p\n").await;
    let (ok, out) = git(&e.home, &w, &["push", &e.scoped("protected"), "protected"]).await;
    assert!(!ok, "{out}");
    assert!(
        out.contains("rejected by the upstream repository"),
        "the client sees the upstream's refusal:\n{out}"
    );
    assert!(!e.upstream_refs().await.contains_key("refs/heads/protected"));
    assert!(
        !e.aspen_refs(&repo)
            .await
            .contains_key("refs/heads/protected")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_proxy_push_while_the_upstream_is_ahead_is_refused_until_the_mirror_syncs() {
    let e = Env::start().await;
    e.seed().await;
    let repo = e.create(MirrorMode::Proxy).await;
    // Someone pushes to the upstream directly; Aspen has not synced yet.
    let direct_tip = e.commit(&e.direct, "a.txt", "direct\n").await;
    e.push_direct(&["main"]).await;
    let w = e.work("w").await;
    e.commit(&w, "c.txt", "through aspen\n").await;
    let (ok, out) = git(&e.home, &w, &["push", &e.aspen_url(), "main"]).await;
    assert!(!ok, "{out}");
    assert!(
        out.contains("upstream repository has moved past this mirror"),
        "{out}"
    );
    // Neither side took the push: nothing at the upstream was overwritten.
    assert_eq!(e.upstream_refs().await["refs/heads/main"], direct_tip);
    assert_ne!(e.aspen_refs(&repo).await["refs/heads/main"], direct_tip);
    // Once synced, the client's usual fetch-and-rebase works.
    e.mirror.sync(&repo).await.unwrap();
    assert_eq!(e.aspen_refs(&repo).await["refs/heads/main"], direct_tip);
    git_ok(&e.home, &w, &["pull", "-q", "--rebase", "origin", "main"]).await;
    git_ok(&e.home, &w, &["push", "-q", &e.aspen_url(), "main"]).await;
    let mine = e.rev(&w, "HEAD").await;
    assert_eq!(e.upstream_refs().await["refs/heads/main"], mine);
    assert_eq!(e.aspen_refs(&repo).await["refs/heads/main"], mine);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn async_pushes_reach_the_upstream_through_the_job() {
    let e = Env::start().await;
    e.seed().await;
    let repo = e.create(MirrorMode::Async).await;
    let before = e.upstream_refs().await;
    let w = e.work("w").await;
    let tip = e.commit(&w, "a.txt", "async\n").await;
    git_ok(&e.home, &w, &["push", "-q", &e.aspen_url(), "main"]).await;
    git_ok(&e.home, &w, &["checkout", "-q", "-b", "topic"]).await;
    let topic = e.commit(&w, "t.txt", "topic\n").await;
    git_ok(&e.home, &w, &["push", "-q", &e.scoped("topic"), "topic"]).await;
    git_ok(
        &e.home,
        &w,
        &[
            "push",
            "-q",
            "-o",
            "force",
            &e.aspen_url(),
            "--delete",
            "refs/heads/feature",
        ],
    )
    .await;
    // Aspen is the primary: the push is accepted before the upstream has it.
    assert_eq!(e.upstream_refs().await, before);
    assert_eq!(e.aspen_refs(&repo).await["refs/heads/main"], tip);
    assert_eq!(e.run_jobs().await, 3, "one mirror_push job per write");
    let upstream = e.upstream_refs().await;
    assert_eq!(upstream["refs/heads/main"], tip);
    assert_eq!(upstream["refs/heads/topic"], topic);
    assert!(
        !upstream.contains_key("refs/heads/feature"),
        "the delete reached the upstream"
    );
    assert_eq!(e.aspen_refs(&repo).await, upstream);
    // The upstream received a complete history it can serve.
    git_ok(&e.home, &e.bare, &["fsck", "--strict", "--no-dangling"]).await;
    // A change made at the upstream directly flows back while Aspen has not
    // moved the same ref.
    git_ok(
        &e.home,
        &e.direct,
        &["fetch", "-q", e.bare.to_str().unwrap(), "main"],
    )
    .await;
    git_ok(&e.home, &e.direct, &["reset", "-q", "--hard", "FETCH_HEAD"]).await;
    let merged = e.commit(&e.direct, "m.txt", "merged on the forge\n").await;
    e.push_direct(&["main"]).await;
    let report = e.mirror.sync(&repo).await.unwrap();
    assert_eq!(report.refs[&name("refs/heads/main")], RefOutcome::Imported);
    assert_eq!(e.aspen_refs(&repo).await["refs/heads/main"], merged);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn async_conflicts_are_reported_and_never_overwritten_until_resolved() {
    let e = Env::start().await;
    e.seed().await;
    let repo = e.create(MirrorMode::Async).await;
    // Both sides change `feature` differently.
    git_ok(&e.home, &e.direct, &["checkout", "-q", "feature"]).await;
    let theirs = e.commit(&e.direct, "f.txt", "upstream side\n").await;
    e.push_direct(&["feature"]).await;
    let w = e.work("w").await;
    git_ok(
        &e.home,
        &w,
        &["checkout", "-q", "-b", "feature", "origin/feature"],
    )
    .await;
    let ours = e.commit(&w, "f.txt", "aspen side\n").await;
    git_ok(
        &e.home,
        &w,
        &["push", "-q", &e.scoped("feature"), "feature"],
    )
    .await;

    // The job records the conflict and completes; nothing is overwritten.
    assert_eq!(e.run_jobs().await, 1);
    assert_eq!(e.upstream_refs().await["refs/heads/feature"], theirs);
    assert_eq!(e.aspen_refs(&repo).await["refs/heads/feature"], ours);
    let entry = e
        .mirror
        .store()
        .ledger_entry(repo.id, &name("refs/heads/feature"))
        .await
        .unwrap();
    let c = entry.conflict.expect("conflict recorded");
    assert_eq!(c.aspen.unwrap().to_string(), ours);
    assert_eq!(c.upstream.unwrap().to_string(), theirs);
    assert_eq!(e.mirror.metrics().open_conflicts(), 1);
    assert!(e.metrics().await.contains("aspen_alert_mirror_conflicts 1"));
    assert!(
        e.meta
            .claim_job(
                &[JobTag::MirrorPush],
                "probe",
                std::time::Duration::from_secs(1)
            )
            .await
            .unwrap()
            .is_none(),
        "a conflict completes the job rather than retrying it"
    );
    // Later passes keep reporting it, still without writing either side.
    let report = e.mirror.sync(&repo).await.unwrap();
    assert!(matches!(
        report.refs[&name("refs/heads/feature")],
        RefOutcome::Conflict(_)
    ));
    assert_eq!(e.upstream_refs().await["refs/heads/feature"], theirs);

    // Keep Aspen's side: the next pass pushes it over the upstream's.
    e.mirror
        .resolve(&repo, &name("refs/heads/feature"), Side::Aspen)
        .await
        .unwrap();
    let report = e.mirror.sync(&repo).await.unwrap();
    assert_eq!(report.refs[&name("refs/heads/feature")], RefOutcome::Pushed);
    assert_eq!(e.upstream_refs().await["refs/heads/feature"], ours);
    assert_eq!(e.mirror.metrics().open_conflicts(), 0);

    // A second conflict, resolved the other way: Aspen takes the upstream's.
    let theirs2 = e.commit(&e.direct, "f.txt", "upstream again\n").await;
    git_ok(
        &e.home,
        &e.direct,
        &["push", "-q", "-f", e.bare.to_str().unwrap(), "feature"],
    )
    .await;
    let ours2 = e.commit(&w, "f.txt", "aspen again\n").await;
    git_ok(
        &e.home,
        &w,
        &["push", "-q", &e.scoped("feature"), "feature"],
    )
    .await;
    e.run_jobs().await;
    let entry = e
        .mirror
        .store()
        .ledger_entry(repo.id, &name("refs/heads/feature"))
        .await
        .unwrap();
    assert!(entry.conflict.is_some());
    e.mirror
        .resolve(&repo, &name("refs/heads/feature"), Side::Upstream)
        .await
        .unwrap();
    let report = e.mirror.sync(&repo).await.unwrap();
    assert_eq!(
        report.refs[&name("refs/heads/feature")],
        RefOutcome::Imported
    );
    assert_eq!(e.aspen_refs(&repo).await["refs/heads/feature"], theirs2);
    assert_ne!(e.upstream_refs().await["refs/heads/feature"], ours2);
    // Resolving a ref with no conflict is refused.
    assert!(
        e.mirror
            .resolve(&repo, &name("refs/heads/feature"), Side::Aspen)
            .await
            .is_err()
    );
}

/// A GitHub App key, made with the system's `openssl` (PKCS#1, as GitHub
/// issues them).
fn app_key(dir: &Path) -> String {
    let path = dir.join("app.pem");
    let out = std::process::Command::new("openssl")
        .args(["genrsa", "-traditional", "-out"])
        .arg(&path)
        .arg("2048")
        .output()
        .expect("openssl is installed");
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    std::fs::read_to_string(path).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn github_app_installation_tokens_are_minted_once_and_sent() {
    let keys = tempfile::tempdir().unwrap();
    let pem = app_key(keys.path());
    // A stand-in for GitHub's REST API that checks the app's JWT.
    let verifier = GitHubApp::new("12345", 42, &pem, "http://127.0.0.1:1").unwrap();
    let public = verifier.public_key_der();
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = Arc::clone(&calls);
    let api = axum::Router::new().route(
        "/app/installations/42/access_tokens",
        axum::routing::post(move |headers: axum::http::HeaderMap| {
            let public = public.clone();
            let seen = Arc::clone(&seen);
            async move {
                seen.fetch_add(1, Ordering::SeqCst);
                let auth = headers["authorization"].to_str().unwrap();
                let jwt = auth.strip_prefix("Bearer ").unwrap();
                let (signed, sig) = jwt.rsplit_once('.').unwrap();
                use base64::Engine as _;
                let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD;
                let sig = b64.decode(sig).unwrap();
                ring::signature::UnparsedPublicKey::new(
                    &ring::signature::RSA_PKCS1_2048_8192_SHA256,
                    &public,
                )
                .verify(signed.as_bytes(), &sig)
                .expect("the JWT is signed with the app's key");
                let claims: serde_json::Value =
                    serde_json::from_slice(&b64.decode(signed.split('.').nth(1).unwrap()).unwrap())
                        .unwrap();
                assert_eq!(claims["iss"], "12345");
                (
                    [("content-type", "application/json")],
                    serde_json::json!({
                        "token": "ghs_minted",
                        "expires_at": "2099-01-01T00:00:00Z",
                    })
                    .to_string(),
                )
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let api_url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, api).await.unwrap() });
    let app = GitHubApp::new("12345", 42, &pem, &api_url).unwrap();
    let creds = credentials(vec![(
        CredentialName::new("app").unwrap(),
        CredentialKind::GitHubApp(Box::new(app)),
    )]);
    let e = Env::start_with(creds, &common::basic("x-access-token", "ghs_minted")).await;
    e.seed().await;
    let (repo, report) = e
        .mirror
        .create(
            &RepoName::new("acme").unwrap(),
            e.upstream_url(),
            MirrorMode::Proxy,
            Some(CredentialName::new("app").unwrap()),
        )
        .await
        .unwrap();
    assert!(report.failures().is_empty(), "{report:?}");
    assert_eq!(e.aspen_refs(&repo).await, e.upstream_refs().await);
    e.mirror.sync(&repo).await.unwrap();
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "the token is cached until near expiry"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_upstream_acceptance_stands_when_recording_it_in_the_ledger_fails() {
    let e = Env::start().await;
    e.seed().await;
    let repo = e.create(MirrorMode::Proxy).await;
    let before = e
        .mirror
        .store()
        .ledger_entry(repo.id, &name("refs/heads/main"))
        .await
        .unwrap();
    let w = e.work("w").await;
    let tip = e.commit(&w, "a.txt", "while the ledger is down\n").await;
    e.fail_ledger.store(true, Ordering::SeqCst);
    // The upstream took it, so the client is told `ok` and Aspen commits.
    git_ok(&e.home, &w, &["push", "-q", &e.aspen_url(), "main"]).await;
    e.fail_ledger.store(false, Ordering::SeqCst);
    assert_eq!(e.upstream_refs().await["refs/heads/main"], tip);
    assert_eq!(e.aspen_refs(&repo).await["refs/heads/main"], tip);
    let metrics = e.metrics().await;
    for line in [
        "aspen_mirror_forwarded_total{result=\"accepted\"} 1",
        "aspen_mirror_ledger_write_failures_total 1",
    ] {
        assert!(metrics.contains(line), "{line}\n{metrics}");
    }
    // The ledger lags; the next pass finds both sides equal and records it.
    let ledger = e.mirror.store();
    assert_eq!(
        ledger
            .ledger_entry(repo.id, &name("refs/heads/main"))
            .await
            .unwrap(),
        before
    );
    let report = e.mirror.sync(&repo).await.unwrap();
    assert_eq!(report.count(|o| *o != RefOutcome::InSync), 0, "{report:?}");
    assert_eq!(
        ledger
            .ledger_entry(repo.id, &name("refs/heads/main"))
            .await
            .unwrap()
            .synced
            .map(|o| o.to_string()),
        Some(tip)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_repository_that_stopped_being_a_mirror_forwards_nothing() {
    let e = Env::start().await;
    e.seed().await;
    let repo = e.create(MirrorMode::Proxy).await;
    assert!(e.mirror.store().delete_mirror(repo.id).await.unwrap());
    let update = aspen_mirror::RefUpdate {
        name: name("refs/heads/gone"),
        old: None,
        new: Some(e.rev(&e.direct, "main").await.parse().unwrap()),
    };
    let answers = e
        .mirror
        .forward(&repo, std::slice::from_ref(&update), None, false)
        .await;
    assert_eq!(answers, vec![aspen_mirror::Forwarded::NotMirror]);
    assert!(!e.upstream_refs().await.contains_key("refs/heads/gone"));
    let metrics = e.metrics().await;
    for line in [
        "aspen_mirror_forwarded_total{result=\"accepted\"} 0",
        "aspen_mirror_forwarded_total{result=\"not_mirror\"} 1",
    ] {
        assert!(metrics.contains(line), "{line}\n{metrics}");
    }
}

// ---------------------------------------------------------------------------
// The push path's side of proxy mode, against a stub hook: what Aspen does
// with the hook's answers, independent of any upstream.

type Rp = aspen_server::receive::ReceivePack<SqlStore, LocalBlobStore>;

/// What the stub saw and does.
#[derive(Debug)]
struct StubState {
    /// `mode()` answers proxy while set, else "not a mirror".
    proxy: AtomicBool,
    /// An update the stub imports through `ReceivePack::import` inside
    /// `forward()`, before answering: a sync that moved the ref while the
    /// client's push was at the upstream.
    steal: Mutex<Option<aspen_mirror::RefUpdate>>,
    /// What the steal's import returned.
    stolen: Mutex<Option<aspen_mirror::Applied>>,
    /// The answer to every forwarded update.
    answer: Mutex<aspen_mirror::Forwarded>,
    /// Every update forwarded.
    forwarded: Mutex<Vec<aspen_mirror::RefUpdate>>,
    /// Every `committed` call: mode and `lagging`.
    committed: Mutex<Vec<(MirrorMode, bool)>>,
}

#[derive(Debug)]
struct Stub {
    rp: std::sync::Weak<Rp>,
    empty_pack: PathBuf,
    state: Arc<StubState>,
}

impl aspen_server::receive::PushThrough for Stub {
    fn mode<'a>(
        &'a self,
        _repo: &'a Repo,
    ) -> aspen_server::receive::BoxFuture<'a, Result<Option<MirrorMode>, StoreError>> {
        let proxy = self.state.proxy.load(Ordering::SeqCst);
        Box::pin(async move { Ok(proxy.then_some(MirrorMode::Proxy)) })
    }

    fn forward<'a>(
        &'a self,
        repo: &'a Repo,
        updates: &'a [aspen_mirror::RefUpdate],
        _pack: Option<&'a Path>,
        _atomic: bool,
    ) -> aspen_server::receive::BoxFuture<'a, Vec<aspen_mirror::Forwarded>> {
        Box::pin(async move {
            self.state
                .forwarded
                .lock()
                .unwrap()
                .extend(updates.iter().cloned());
            let steal = self.state.steal.lock().unwrap().take();
            if let Some(u) = steal {
                let rp = self.rp.upgrade().unwrap();
                let applied = rp.import(repo, &u, Some(&self.empty_pack)).await.unwrap();
                *self.state.stolen.lock().unwrap() = Some(applied);
            }
            let answer = self.state.answer.lock().unwrap().clone();
            vec![answer; updates.len()]
        })
    }

    fn committed(&self, _repo: &Repo, mode: MirrorMode, lagging: bool) {
        self.state.committed.lock().unwrap().push((mode, lagging));
    }
}

/// An Aspen server (no upstream) whose push path has the stub hook, and a
/// working repository that pushed `main` (3 commits) and `feature` (2
/// commits off main) to it while the stub answered "not a mirror".
struct StubEnv {
    _dir: tempfile::TempDir,
    home: PathBuf,
    meta: SqlStore,
    repo: Repo,
    state: Arc<StubState>,
    base: String,
    work: PathBuf,
}

impl StubEnv {
    async fn start() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path().join("home");
        std::fs::create_dir_all(&home).unwrap();
        let url = format!("sqlite://{}?mode=rwc", dir.path().join("meta.db").display());
        let meta = SqlStore::connect(&url).await.unwrap();
        meta.migrate().await.unwrap();
        let blobs = LocalBlobStore::open(dir.path().join("blobs")).unwrap();
        let mut rcfg = ReceiveConfig::new(
            dir.path().join("spool"),
            MaxObjectBytes::new(64 * MIB).unwrap(),
            MaxInflatedBytes::new(1024 * MIB).unwrap(),
        );
        rcfg.thread_limit = Some(2);
        let server = aspen_server::Server::new(
            meta.clone(),
            blobs,
            aspen_server::ServerConfig::new(MaxObjectBytes::new(64 * MIB).unwrap()),
        )
        .with_receive_pack(rcfg)
        .unwrap();
        // The pack stock git sends for a push that brings no objects.
        let empty_pack = dir.path().join("empty.pack");
        let mut cmd = isolated_git(&home);
        cmd.args(["pack-objects", "--stdout"])
            .stdin(std::process::Stdio::null());
        let out = cmd.output().unwrap();
        assert!(out.status.success());
        std::fs::write(&empty_pack, out.stdout).unwrap();
        let state = Arc::new(StubState {
            proxy: AtomicBool::new(false),
            steal: Mutex::new(None),
            stolen: Mutex::new(None),
            answer: Mutex::new(aspen_mirror::Forwarded::Accepted),
            forwarded: Mutex::new(Vec::new()),
            committed: Mutex::new(Vec::new()),
        });
        let rp = Arc::clone(server.receive_pack().unwrap());
        rp.set_push_through(Arc::new(Stub {
            rp: Arc::downgrade(&rp),
            empty_pack,
            state: Arc::clone(&state),
        }))
        .unwrap();
        let repo = meta
            .create_repo(
                &RepoName::new("acme").unwrap(),
                &RepoSettings::new(name("refs/heads/main")).unwrap(),
            )
            .await
            .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let access = common::access(
            serde_json::json!({ "anonymous": ["write:*"] }),
            aspen_auth::LimitsConfig::default(),
            meta.clone(),
        );
        let app = server.router(access);
        tokio::spawn(async move {
            axum::serve(
                listener,
                app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
            )
            .await
            .unwrap()
        });
        let work = dir.path().join("work");
        std::fs::create_dir_all(&work).unwrap();
        let e = Self {
            _dir: dir,
            home,
            meta,
            repo,
            state,
            base: format!("http://{addr}"),
            work,
        };
        let w = &e.work;
        git_ok(&e.home, w, &["init", "-q", "--initial-branch=main"]).await;
        for n in ["one", "two", "three"] {
            e.commit("a.txt", n).await;
        }
        git_ok(&e.home, w, &["checkout", "-q", "-b", "feature"]).await;
        e.commit("f.txt", "f1").await;
        e.commit("f.txt", "f2").await;
        git_ok(&e.home, w, &["push", "-q", &e.url(), "main", "feature"]).await;
        e
    }

    fn url(&self) -> String {
        format!("{}/acme.git", self.base)
    }

    fn scoped(&self, branch: &str) -> String {
        format!("{}/acme.git/-/refs/heads/{branch}", self.base)
    }

    async fn commit(&self, file: &str, content: &str) -> String {
        std::fs::write(self.work.join(file), content).unwrap();
        git_ok(&self.home, &self.work, &["add", "-A"]).await;
        git_ok(&self.home, &self.work, &["commit", "-q", "-m", content]).await;
        self.rev("HEAD").await
    }

    async fn rev(&self, r: &str) -> String {
        git_ok(&self.home, &self.work, &["rev-parse", r])
            .await
            .trim()
            .to_owned()
    }

    async fn aspen(&self, r: &str) -> Option<String> {
        self.meta
            .current_version(self.repo.id, &name(r))
            .await
            .unwrap()
            .and_then(|v| v.value.tip())
            .map(|t| t.to_string())
    }

    fn forwarded(&self) -> Vec<String> {
        self.state
            .forwarded
            .lock()
            .unwrap()
            .iter()
            .map(|u| u.name.to_string())
            .collect()
    }
}

/// Decision 2 of the C4 review: the upstream accepts a forwarded update,
/// but before Aspen commits it a sync moves the ref (Aspen's compare-and-swap
/// is lost). The client is told `ok`, since the upstream, the source of
/// truth, has its update; Aspen keeps the value the sync wrote, and the
/// hook is told the mirror lags so it requests a sync.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_update_the_upstream_accepted_is_ok_even_when_aspen_loses_the_race() {
    let e = StubEnv::start().await;
    let main = e.rev("main").await;
    let f2 = e.rev("feature").await;
    let s = &e.state;
    s.proxy.store(true, Ordering::SeqCst);
    *s.steal.lock().unwrap() = Some(aspen_mirror::RefUpdate {
        name: name("refs/heads/feature"),
        old: Some(f2.parse().unwrap()),
        new: Some(main.parse().unwrap()),
    });
    let f3 = e.commit("f.txt", "f3").await;
    let (ok, out) = git(&e.home, &e.work, &["push", &e.scoped("feature"), "feature"]).await;
    assert!(ok, "the client sees the upstream's acceptance:\n{out}");
    assert_eq!(
        *s.stolen.lock().unwrap(),
        Some(aspen_mirror::Applied::Updated),
        "the competing import committed"
    );
    assert_eq!(e.forwarded(), vec!["refs/heads/feature"]);
    assert_eq!(
        e.aspen("refs/heads/feature").await,
        Some(main),
        "Aspen holds the competing value, not the client's {f3}"
    );
    assert_eq!(
        *s.committed.lock().unwrap(),
        vec![(MirrorMode::Proxy, true)],
        "the hook hears that the mirror lags"
    );

    // A repository that stopped being a mirror meanwhile: Aspen decides
    // alone and commits; nothing lags.
    s.committed.lock().unwrap().clear();
    *s.answer.lock().unwrap() = aspen_mirror::Forwarded::NotMirror;
    git_ok(&e.home, &e.work, &["checkout", "-q", "-b", "solo", "main"]).await;
    let solo = e.commit("s.txt", "solo").await;
    git_ok(&e.home, &e.work, &["push", "-q", &e.scoped("solo"), "solo"]).await;
    assert_eq!(e.aspen("refs/heads/solo").await, Some(solo));
    assert_eq!(
        *s.committed.lock().unwrap(),
        vec![(MirrorMode::Proxy, false)]
    );
}

/// A client's force or non-fast-forward push to a proxy mirror is refused by
/// Aspen's own checks and never reaches the upstream.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_client_push_aspen_refuses_is_never_forwarded() {
    let e = StubEnv::start().await;
    let main = e.rev("main").await;
    let f2 = e.rev("feature").await;
    e.state.proxy.store(true, Ordering::SeqCst);
    // `git push --force` rewinding trunk: force policy `branches` refuses it.
    let (ok, out) = git(
        &e.home,
        &e.work,
        &["push", "--force", &e.url(), "main~1:refs/heads/main"],
    )
    .await;
    assert!(!ok, "{out}");
    assert!(out.contains("force"), "{out}");
    // A non-fast-forward to a branch the client cannot see (zero old
    // value), without `-o force`.
    let (ok, out) = git(
        &e.home,
        &e.work,
        &["push", &e.url(), "main~2:refs/heads/feature"],
    )
    .await;
    assert!(!ok, "{out}");
    assert!(out.contains("non-fast-forward"), "{out}");
    assert_eq!(
        e.forwarded(),
        Vec::<String>::new(),
        "nothing reached the upstream"
    );
    assert!(e.state.committed.lock().unwrap().is_empty());
    assert_eq!(e.aspen("refs/heads/main").await, Some(main));
    assert_eq!(e.aspen("refs/heads/feature").await, Some(f2));
}
