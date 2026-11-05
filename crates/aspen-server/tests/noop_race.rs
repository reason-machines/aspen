//! Issue #49: under `--atomic`, a lost compare-and-swap recomputes every
//! verdict, and a no-op (a branch the client re-sends at the tip it already
//! has, because the branch was not advertised) can come back as a write
//! after the push's shared pack was stored. Its objects must already have a
//! place then, or placement opens a second shared pack (a `debug_assert` in
//! debug builds; extra stored objects beyond the 2N bound in release).
//!
//! The race is made deterministic with a metadata store that moves the
//! no-op's ref just before the push's first commit transaction.

mod receive_support;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use aspen_blob::LocalBlobStore;
use aspen_core::error::StoreError;
use aspen_core::id::{CoverId, JobId, PackChecksum, PackId, RefVersionId, RepoId, Seq};
use aspen_core::ingest::SortedIds;
use aspen_core::meta::{
    CommitLookup, CommitOutcome, CommitRow, Cover, Job, JobEvent, JobTag, MetaStore, NewCover,
    NewJob, PackTombstone, PushCommit, RefListQuery, RefWrite, ShallowDepth, ShallowRecorded,
};
use aspen_core::name::{RefName, RepoName};
use aspen_core::pack::{CoverLevel, NewPack, PackRecord};
use aspen_core::refs::{Closure, Deps, RefValue, RefVersion};
use aspen_core::repo::{ForcePolicy, Repo, RepoSettings};
use aspen_meta::SqlStore;
use aspen_pack::MaxObjectBytes;

use receive_support::*;

/// Refs to move, once, just before the next push commit: each of `names`
/// is set (or created) at the tip of `to`.
#[derive(Debug, Clone)]
struct Move {
    repo: RepoId,
    names: Vec<RefName>,
    to: RefName,
    /// Each moved ref's new version also depends on the version it
    /// replaces, as a fast-forward through a scoped URL does.
    on_parent: bool,
}

/// `SqlStore`, except that an armed [`Move`] runs first in the next
/// `commit_push`: the ref is set to the tip of `to` (with `to`'s version as
/// its dependency), as a concurrent push would.
#[derive(Debug, Clone)]
struct Racing {
    inner: SqlStore,
    armed: Arc<Mutex<Option<Move>>>,
}

impl MetaStore for Racing {
    async fn create_repo(
        &self,
        name: &RepoName,
        settings: &RepoSettings,
    ) -> Result<Repo, StoreError> {
        self.inner.create_repo(name, settings).await
    }
    async fn repo(&self, name: &RepoName) -> Result<Option<Repo>, StoreError> {
        self.inner.repo(name).await
    }
    async fn repo_by_id(&self, id: RepoId) -> Result<Option<Repo>, StoreError> {
        self.inner.repo_by_id(id).await
    }
    async fn current_version(
        &self,
        repo: RepoId,
        name: &RefName,
    ) -> Result<Option<RefVersion>, StoreError> {
        self.inner.current_version(repo, name).await
    }
    async fn ref_version(&self, id: RefVersionId) -> Result<Option<RefVersion>, StoreError> {
        self.inner.ref_version(id).await
    }
    async fn versions_by_seq(
        &self,
        repo: RepoId,
        name: &RefName,
        seqs: &[Seq],
    ) -> Result<Vec<RefVersion>, StoreError> {
        self.inner.versions_by_seq(repo, name, seqs).await
    }
    async fn list_refs(
        &self,
        repo: RepoId,
        query: &RefListQuery,
    ) -> Result<Vec<RefVersion>, StoreError> {
        self.inner.list_refs(repo, query).await
    }
    async fn register_pack(&self, pack: &NewPack) -> Result<PackRecord, StoreError> {
        self.inner.register_pack(pack).await
    }
    async fn packs(&self, ids: &[PackId]) -> Result<Vec<PackRecord>, StoreError> {
        self.inner.packs(ids).await
    }
    async fn commit_push(&self, push: &PushCommit) -> Result<CommitOutcome, StoreError> {
        let armed = self.armed.lock().unwrap().take();
        if let Some(m) = armed {
            let to = self.inner.current_version(m.repo, &m.to).await?.unwrap();
            let mut writes = Vec::new();
            for name in &m.names {
                let current = self.inner.current_version(m.repo, name).await?;
                writes.push(RefWrite {
                    name: name.clone(),
                    expected: current.as_ref().map(|v| v.id),
                    value: RefValue::Live {
                        tip: to.value.tip().unwrap(),
                        pack: None,
                        closure: Closure::Exact,
                    },
                    deps: Deps {
                        trunk: Some(to.id),
                        scoped_old: if m.on_parent {
                            current.as_ref().map(|v| v.id)
                        } else {
                            None
                        },
                    },
                    commits: Vec::new(),
                    jobs: Vec::new(),
                });
            }
            let moved = self
                .inner
                .commit_push(&PushCommit {
                    repo: m.repo,
                    writes,
                })
                .await?;
            assert!(matches!(moved, CommitOutcome::Committed(_)), "{moved:?}");
        }
        self.inner.commit_push(push).await
    }
    async fn lookup_commits(
        &self,
        repo: RepoId,
        ids: &SortedIds,
    ) -> Result<Vec<CommitLookup>, StoreError> {
        self.inner.lookup_commits(repo, ids).await
    }
    async fn index_commits(&self, repo: RepoId, rows: &[CommitRow]) -> Result<(), StoreError> {
        self.inner.index_commits(repo, rows).await
    }
    async fn covers(&self, repo: RepoId, name: &RefName) -> Result<Vec<Cover>, StoreError> {
        self.inner.covers(repo, name).await
    }
    async fn covers_at(
        &self,
        repo: RepoId,
        name: &RefName,
        level: CoverLevel,
        firsts: &[Seq],
    ) -> Result<Vec<Cover>, StoreError> {
        self.inner.covers_at(repo, name, level, firsts).await
    }
    async fn insert_cover(&self, cover: &NewCover) -> Result<Cover, StoreError> {
        self.inner.insert_cover(cover).await
    }
    async fn set_cover_pack(&self, cover: CoverId, pack: PackId) -> Result<Cover, StoreError> {
        self.inner.set_cover_pack(cover, pack).await
    }
    async fn pack_by_checksum(
        &self,
        repo: RepoId,
        checksum: &PackChecksum,
    ) -> Result<Option<PackRecord>, StoreError> {
        self.inner.pack_by_checksum(repo, checksum).await
    }
    async fn sweep_orphan_packs(
        &self,
        grace: Duration,
        limit: u32,
    ) -> Result<Vec<PackRecord>, StoreError> {
        self.inner.sweep_orphan_packs(grace, limit).await
    }
    async fn pending_pack_deletes(&self, limit: u32) -> Result<Vec<PackTombstone>, StoreError> {
        self.inner.pending_pack_deletes(limit).await
    }
    async fn finish_pack_deletes(&self, done: &[PackTombstone]) -> Result<(), StoreError> {
        self.inner.finish_pack_deletes(done).await
    }
    async fn shallow_pack(
        &self,
        repo: RepoId,
        version: RefVersionId,
        depth: ShallowDepth,
    ) -> Result<Option<PackRecord>, StoreError> {
        self.inner.shallow_pack(repo, version, depth).await
    }
    async fn set_shallow_pack(
        &self,
        repo: RepoId,
        version: RefVersionId,
        depth: ShallowDepth,
        pack: PackId,
        max_repo_bytes: u64,
    ) -> Result<ShallowRecorded, StoreError> {
        self.inner
            .set_shallow_pack(repo, version, depth, pack, max_repo_bytes)
            .await
    }
    async fn mark_shallow_pack_used(
        &self,
        repo: RepoId,
        version: RefVersionId,
        depth: ShallowDepth,
    ) -> Result<(), StoreError> {
        self.inner
            .mark_shallow_pack_used(repo, version, depth)
            .await
    }
    async fn evict_shallow_packs(
        &self,
        superseded_after: Duration,
        unused_after: Duration,
        limit: u32,
    ) -> Result<u64, StoreError> {
        self.inner
            .evict_shallow_packs(superseded_after, unused_after, limit)
            .await
    }
    async fn enqueue_jobs(&self, jobs: &[NewJob]) -> Result<Vec<JobId>, StoreError> {
        self.inner.enqueue_jobs(jobs).await
    }
    async fn claim_job(
        &self,
        tags: &[JobTag],
        owner: &str,
        lease: Duration,
    ) -> Result<Option<Job>, StoreError> {
        self.inner.claim_job(tags, owner, lease).await
    }
    async fn release_jobs(
        &self,
        tags: &[JobTag],
        owner: &str,
        reason: &str,
    ) -> Result<u64, StoreError> {
        self.inner.release_jobs(tags, owner, reason).await
    }
    async fn update_job(&self, id: JobId, event: JobEvent) -> Result<Job, StoreError> {
        self.inner.update_job(id, event).await
    }
}

/// Invariant (issue #49): an atomic push whose compare-and-swap is lost
/// because a branch it re-sent unchanged (a no-op) was moved meanwhile
/// recomputes that branch as a fast-forward and commits, with every write's
/// objects placed before the first pack was stored: the push succeeds and
/// stores no second shared pack. Without the no-op placement the
/// recomputed write opens a second shared pack after the first was stored,
/// which trips the push path's `debug_assert` and fails the push.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_noop_that_becomes_a_write_after_a_lost_race_has_its_place() {
    let dir = tempfile::tempdir().unwrap();
    let url = format!("sqlite://{}?mode=rwc", dir.path().join("meta.db").display());
    let inner = SqlStore::connect(&url).await.unwrap();
    inner.migrate().await.unwrap();
    let meta = Racing {
        inner,
        armed: Arc::new(Mutex::new(None)),
    };
    let blobs = LocalBlobStore::open(dir.path().join("blobs")).unwrap();
    let server = aspen_server::Server::new(
        meta.clone(),
        blobs.clone(),
        aspen_server::ServerConfig::new(MaxObjectBytes::new(64 * MIB).unwrap()),
    )
    .with_receive_pack(config(dir.path()))
    .unwrap();
    let main = RefName::new("refs/heads/main").unwrap();
    let settings = RepoSettings::builder(
        main.clone(),
        RepoSettings::new(main.clone()).unwrap().writable().to_vec(),
    )
    .allow_force(ForcePolicy::Branches)
    .build()
    .unwrap();
    let repo = meta
        .create_repo(&RepoName::new("demo").unwrap(), &settings)
        .await
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let policy: aspen_auth::PolicyConfig =
        serde_json::from_value(serde_json::json!({ "anonymous": ["write:demo"] })).unwrap();
    let access = Arc::new(aspen_server::access::Access::new(
        aspen_auth::AuthService::new(policy, aspen_auth::MemoryTokenStore::new()).unwrap(),
        aspen_auth::Limits::new(aspen_auth::LimitsConfig::default()),
        meta.clone(),
        aspen_server::access::ClientAddr::Peer,
    ));
    let app = server.router(access);
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let url = format!("http://{addr}/demo.git");
    let home = dir.path().join("home");
    std::fs::create_dir_all(&home).unwrap();
    let w = Work {
        path: dir.path().join("w"),
        home,
    };
    std::fs::create_dir_all(&w.path).unwrap();
    w.git(&["init", "-q", "--initial-branch=main"]);
    let push = |args: Vec<&str>| {
        let mut a = vec!["push", "--porcelain", url.as_str()];
        a.extend(args);
        let a: Vec<String> = a.into_iter().map(str::to_owned).collect();
        let cmd = w.command();
        async move {
            blocking(move || {
                let mut cmd = cmd;
                let out = cmd.args(&a).output().unwrap();
                let mut text = String::from_utf8_lossy(&out.stdout).into_owned();
                text.push_str(&String::from_utf8_lossy(&out.stderr));
                (out.status.success(), text)
            })
            .await
        }
    };

    // Trunk at A; feature at X (a child of A) on the server.
    w.commit("a.txt", "a\n", "A");
    assert!(push(vec!["main"]).await.0);
    w.git(&["checkout", "-q", "-b", "feature"]);
    w.commit("x.txt", "x\n", "X");
    assert!(push(vec!["feature"]).await.0);

    // A chain of branches off A, the first with many new objects, so the
    // later ones overflow the own-pack budget into the shared pack, and the
    // no-op `feature` (re-sent, since it is not advertised) lands there too.
    w.git(&["checkout", "-q", "main"]);
    w.git(&["checkout", "-q", "-b", "b1"]);
    for i in 0..20 {
        std::fs::write(w.path.join(format!("y{i}.txt")), format!("y {i}\n")).unwrap();
    }
    w.git(&["add", "-A"]);
    w.git(&["commit", "-q", "-m", "Y1"]);
    for b in ["b2", "b3", "b4"] {
        w.git(&["checkout", "-q", "-b", b]);
        w.commit(&format!("{b}.txt"), &format!("{b}\n"), b);
    }

    // Just before the atomic push commits, a concurrent push moves
    // `feature` back to A and creates `b4` at A. The no-op writes nothing,
    // so it is `b4`'s create that loses its compare-and-swap; the recompute
    // then finds `feature` (A to X) and `b4` (A to Y4) to be fast-forwards.
    *meta.armed.lock().unwrap() = Some(Move {
        repo: repo.id,
        names: vec![
            RefName::new("refs/heads/b4").unwrap(),
            RefName::new("refs/heads/feature").unwrap(),
        ],
        to: main.clone(),
        on_parent: false,
    });
    let (ok, out) = push(vec!["--atomic", "b1", "b2", "b3", "b4", "feature"]).await;
    assert!(meta.armed.lock().unwrap().is_none(), "the race ran");
    assert!(ok, "the push survives the lost race: {out}");
    let feature = meta
        .current_version(repo.id, &RefName::new("refs/heads/feature").unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        feature.value.tip().unwrap(),
        oid(&w.rev("feature")),
        "the recomputed fast-forward of feature committed"
    );
    // Every pack the push stored is named by a version: b1's own pack and
    // one shared pack, which also holds feature's objects. A second shared
    // pack (the bug) would be stored and left behind by the failed attempt.
    let mut named = std::collections::BTreeSet::new();
    for b in ["main", "b1", "b2", "b3", "b4", "feature"] {
        let mut v = meta
            .current_version(repo.id, &RefName::new(format!("refs/heads/{b}")).unwrap())
            .await
            .unwrap();
        while let Some(version) = v {
            named.extend(version.value.pack());
            v = match version.parent {
                Some(p) => meta.ref_version(p).await.unwrap(),
                None => None,
            };
        }
    }
    let mut registered = std::collections::BTreeSet::new();
    for n in 1..=32 {
        let id = PackId::new(n).unwrap();
        if meta.packs(&[id]).await.is_ok() {
            registered.insert(id);
        }
    }
    assert_eq!(registered, named, "no pack is left unnamed");
    assert_eq!(
        registered.len(),
        4,
        "A, X, b1's own pack and one shared pack"
    );
    // Every ref fetches complete.
    let c = Work {
        path: dir.path().join("c"),
        home: w.home.clone(),
    };
    git_ok(
        &w.home,
        dir.path(),
        &["clone", "-q", "--mirror", url.as_str(), "c"],
    );
    git_ok(&c.home, &c.path, &["fsck", "--strict", "--no-dangling"]);
    for b in ["b1", "b2", "b3", "b4", "feature"] {
        let got = git_ok(&c.home, &c.path, &["rev-parse", &format!("refs/heads/{b}")]);
        assert_eq!(got.trim(), w.rev(b), "{b}");
    }
}

/// Invariant (issue #203 review): a push of one branch through the plain
/// repository URL depends on the branch version it searched. When it loses
/// its compare-and-swap to a concurrent fast-forward and commits on top of
/// that, its dependency is not its parent, so the compactor records no
/// cover chain across it (`VerifiedRange::verify` chains a version only
/// when its parent is among its deps), and an incremental fetch with a
/// have inside the range is still complete (`fsck --strict`).
///
/// Every other link of the range is a dependency on the parent, so the
/// lost race is the only thing that keeps the cover from carrying a chain.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_plain_url_push_that_loses_its_race_breaks_the_cover_chain() {
    use aspen_compact::{CompactConfig, Compactor, JobOutcome, Worker, WorkerOptions};
    use aspen_core::meta::CoverLayout;
    use aspen_pack::MaxInflatedBytes;

    const FANOUT: u64 = 4;
    let dir = tempfile::tempdir().unwrap();
    let url = format!("sqlite://{}?mode=rwc", dir.path().join("meta.db").display());
    let inner = SqlStore::connect(&url).await.unwrap();
    inner.migrate().await.unwrap();
    let meta = Racing {
        inner: inner.clone(),
        armed: Arc::new(Mutex::new(None)),
    };
    let blobs = LocalBlobStore::open(dir.path().join("blobs")).unwrap();
    let mut receive = config(dir.path());
    receive.cover_layout = CoverLayout::new(FANOUT).unwrap();
    let server = aspen_server::Server::new(
        meta.clone(),
        blobs.clone(),
        aspen_server::ServerConfig::new(MaxObjectBytes::new(64 * MIB).unwrap()),
    )
    .with_receive_pack(receive)
    .unwrap();
    let main = RefName::new("refs/heads/main").unwrap();
    let topic = RefName::new("refs/heads/topic").unwrap();
    let repo = meta
        .create_repo(
            &RepoName::new("demo").unwrap(),
            &RepoSettings::new(main.clone()).unwrap(),
        )
        .await
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let policy: aspen_auth::PolicyConfig =
        serde_json::from_value(serde_json::json!({ "anonymous": ["write:demo"] })).unwrap();
    let access = Arc::new(aspen_server::access::Access::new(
        aspen_auth::AuthService::new(policy, aspen_auth::MemoryTokenStore::new()).unwrap(),
        aspen_auth::Limits::new(aspen_auth::LimitsConfig::default()),
        meta.clone(),
        aspen_server::access::ClientAddr::Peer,
    ));
    let app = server.router(access);
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let url = format!("http://{addr}/demo.git");
    let home = dir.path().join("home");
    std::fs::create_dir_all(&home).unwrap();
    let w = Work {
        path: dir.path().join("w"),
        home: home.clone(),
    };
    std::fs::create_dir_all(&w.path).unwrap();
    w.git(&["init", "-q", "--initial-branch=main"]);
    let push = |args: Vec<&str>| {
        let mut a = vec!["push", "--porcelain", url.as_str()];
        a.extend(args);
        let a: Vec<String> = a.into_iter().map(str::to_owned).collect();
        let cmd = w.command();
        async move {
            blocking(move || {
                let mut cmd = cmd;
                let out = cmd.args(&a).output().unwrap();
                let mut text = String::from_utf8_lossy(&out.stdout).into_owned();
                text.push_str(&String::from_utf8_lossy(&out.stderr));
                (out.status.success(), text)
            })
            .await
        }
    };

    // Trunk at A; topic versions 1 (t1) and 2 (t2), plain-URL pushes.
    w.commit("a.txt", "a\n", "A");
    assert!(push(vec!["main"]).await.0);
    w.git(&["checkout", "-q", "-b", "topic"]);
    w.commit("t.txt", "1\n", "t1");
    assert!(push(vec!["topic"]).await.0);
    w.commit("t.txt", "2\n", "t2");
    assert!(push(vec!["topic"]).await.0);
    // A client current on topic version 2.
    git_ok(
        &home,
        dir.path(),
        &["clone", "-q", "-b", "topic", url.as_str(), "c"],
    );
    // Trunk moves to M, a child of t2; topic gets t3, a child of M.
    w.git(&["checkout", "-q", "main"]);
    w.git(&["merge", "-q", "--ff-only", "topic"]);
    w.commit("m.txt", "m\n", "M");
    assert!(push(vec!["main"]).await.0);
    w.git(&["checkout", "-q", "topic"]);
    w.git(&["merge", "-q", "--ff-only", "main"]);
    w.commit("t.txt", "3\n", "t3");

    // The push searches topic version 2. Just before it commits, a
    // concurrent fast-forward moves topic to M as version 3, depending on
    // version 2. The push loses its compare-and-swap, recomputes (t3 is a
    // fast-forward of M) and commits version 4 on top of version 3, still
    // depending on the version 2 it searched.
    *meta.armed.lock().unwrap() = Some(Move {
        repo: repo.id,
        names: vec![topic.clone()],
        to: main.clone(),
        on_parent: true,
    });
    let (ok, out) = push(vec!["topic"]).await;
    assert!(meta.armed.lock().unwrap().is_none(), "the race ran");
    assert!(ok, "the push survives the lost race: {out}");
    let seqs: Vec<Seq> = (1..=4).map(|n| Seq::new(n).unwrap()).collect();
    let versions = meta.versions_by_seq(repo.id, &topic, &seqs).await.unwrap();
    assert_eq!(versions.len(), 4, "{versions:?}");
    let [v1, v2, v3, v4] = [0, 1, 2, 3].map(|i| &versions[i]);
    assert_eq!(v4.value.tip().unwrap(), oid(&w.rev("topic")));
    assert_eq!(v4.parent, Some(v3.id));
    assert_eq!(v4.deps.scoped_old, Some(v2.id), "the version it searched");
    for (child, parent) in [(v2, v1), (v3, v2)] {
        assert!(
            child.deps.iter().any(|d| d == parent.id),
            "version {} depends on its parent",
            child.seq
        );
    }
    assert!(!v4.deps.iter().any(|d| d == v3.id), "{v4:?}");

    // Compaction: the push of version 4 enqueued the level-1 job.
    let mut compact = CompactConfig::new(
        dir.path().join("compact"),
        MaxObjectBytes::new(64 * MIB).unwrap(),
        MaxInflatedBytes::new(1024 * MIB).unwrap(),
    );
    compact.layout = CoverLayout::new(FANOUT).unwrap();
    std::fs::create_dir_all(&compact.spool_dir).unwrap();
    let mut worker = Worker::new(
        Compactor::new(inner.clone(), blobs.clone(), compact),
        WorkerOptions::new("test"),
    );
    let mut done = 0;
    while let Some(outcome) = worker.run_one().await.unwrap() {
        match outcome {
            JobOutcome::Done { .. } => done += 1,
            JobOutcome::Failed { error, .. } => panic!("a job failed: {error}"),
        }
    }
    assert!(done >= 1);
    let covers = meta.covers(repo.id, &topic).await.unwrap();
    let cover = covers
        .iter()
        .find(|c| c.range.first() == Seq::FIRST)
        .unwrap_or_else(|| panic!("no cover of versions 1 to 4: {covers:?}"));
    assert_eq!(cover.range.last(), Seq::new(4).unwrap());
    assert!(
        cover.chain.is_none(),
        "version 4 does not depend on its parent version 3: {cover:?}"
    );

    // An incremental fetch with a have inside the range (t2, version 2).
    let c = Work {
        path: dir.path().join("c"),
        home: home.clone(),
    };
    git_ok(&home, &c.path, &["fetch", "-q", "origin", "topic"]);
    git_ok(&home, &c.path, &["fsck", "--strict", "--no-dangling"]);
    let got = git_ok(&home, &c.path, &["rev-parse", "FETCH_HEAD"]);
    assert_eq!(got.trim(), w.rev("topic"));
}
