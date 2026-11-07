//! Throughput item 6c: `git ls-remote <url> <ref>` polls answered from
//! each instance's in-memory ref listing (`upload_pack::ref_snapshot`).
//!
//! Two Aspen instances share one metadata database, as a deployment behind
//! a load balancer does. A metadata store wrapper counts the store calls
//! the ref listing makes, and the ref rows they return; each counted call
//! is one SQL statement (a `list_refs` page, a `current_version` lookup or
//! a `ref_changes` read).

mod receive_support;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use aspen_blob::LocalBlobStore;
use aspen_core::error::StoreError;
use aspen_core::id::{CoverId, JobId, PackChecksum, PackId, RefVersionId, RepoId, Seq};
use aspen_core::ingest::SortedIds;
use aspen_core::meta::{
    CommitLookup, CommitOutcome, CommitRow, CompactionHeat, Cover, Job, JobEvent, JobTag,
    MetaStore, NewCover, NewJob, PackTombstone, PushCommit, RefChanges, RefCursor, RefListQuery,
    RefWrite, ShallowDepth, ShallowRecorded,
};
use aspen_core::name::{RefName, RepoName};
use aspen_core::pack::{CoverLevel, NewPack, PackRecord};
use aspen_core::refs::{Closure, Deps, RefValue, RefVersion};
use aspen_core::repo::{Repo, RepoSettings};
use aspen_meta::SqlStore;
use aspen_pack::MaxObjectBytes;

use receive_support::*;

/// What the ref listing asked the store.
#[derive(Debug, Default)]
struct Counts {
    /// `list_refs` pages and `current_version` lookups: the uncached
    /// listing's statements.
    scans: AtomicU64,
    /// `ref_changes` reads: the cached listing's statements.
    changes: AtomicU64,
    /// Ref rows those statements returned.
    rows: AtomicU64,
}

impl Counts {
    fn take(&self) -> (u64, u64, u64) {
        (
            self.scans.swap(0, Ordering::SeqCst),
            self.changes.swap(0, Ordering::SeqCst),
            self.rows.swap(0, Ordering::SeqCst),
        )
    }
}

/// `SqlStore`, counting the calls a ref listing makes.
#[derive(Debug, Clone)]
struct Counting {
    inner: SqlStore,
    counts: Arc<Counts>,
}

impl MetaStore for Counting {
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
    async fn list_repos(&self, after: Option<RepoId>, limit: u32) -> Result<Vec<Repo>, StoreError> {
        self.inner.list_repos(after, limit).await
    }
    async fn current_version(
        &self,
        repo: RepoId,
        name: &RefName,
    ) -> Result<Option<RefVersion>, StoreError> {
        self.counts.scans.fetch_add(1, Ordering::SeqCst);
        self.counts.rows.fetch_add(1, Ordering::SeqCst);
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
        let out = self.inner.list_refs(repo, query).await?;
        self.counts.scans.fetch_add(1, Ordering::SeqCst);
        self.counts
            .rows
            .fetch_add(out.len() as u64, Ordering::SeqCst);
        Ok(out)
    }
    async fn ref_changes(
        &self,
        repo: RepoId,
        since: Option<&RefCursor>,
        limit: u32,
    ) -> Result<Option<RefChanges>, StoreError> {
        let out = self.inner.ref_changes(repo, since, limit).await?;
        self.counts.changes.fetch_add(1, Ordering::SeqCst);
        if let Some(c) = &out {
            self.counts
                .rows
                .fetch_add(c.refs.len() as u64, Ordering::SeqCst);
        }
        Ok(out)
    }
    async fn register_pack(&self, pack: &NewPack) -> Result<PackRecord, StoreError> {
        self.inner.register_pack(pack).await
    }
    async fn packs(&self, ids: &[PackId]) -> Result<Vec<PackRecord>, StoreError> {
        self.inner.packs(ids).await
    }
    async fn commit_push(&self, push: &PushCommit) -> Result<CommitOutcome, StoreError> {
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
    async fn unexamined_commits(
        &self,
        repo: RepoId,
        after: Option<(aspen_core::id::RefVersionId, aspen_core::id::ObjectId)>,
        limit: usize,
    ) -> Result<Vec<CommitRow>, StoreError> {
        self.inner.unexamined_commits(repo, after, limit).await
    }
    async fn commits_waiting_on(
        &self,
        repo: RepoId,
        ids: &aspen_core::ingest::SortedIds,
        limit: usize,
    ) -> Result<Vec<CommitRow>, StoreError> {
        self.inner.commits_waiting_on(repo, ids, limit).await
    }
    async fn set_generation_waits_on(
        &self,
        repo: RepoId,
        waits: &[(aspen_core::id::ObjectId, aspen_core::id::ObjectId)],
    ) -> Result<(), StoreError> {
        self.inner.set_generation_waits_on(repo, waits).await
    }
    async fn set_generations(
        &self,
        repo: RepoId,
        generations: &[(aspen_core::id::ObjectId, u64)],
    ) -> Result<(), StoreError> {
        self.inner.set_generations(repo, generations).await
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
    async fn acquire_lease(
        &self,
        name: &str,
        owner: &str,
        ttl: Duration,
    ) -> Result<Option<aspen_core::meta::Lease>, StoreError> {
        self.inner.acquire_lease(name, owner, ttl).await
    }
    async fn sweep_orphan_packs(
        &self,
        lease: &aspen_core::meta::Lease,
        grace: Duration,
        limit: u32,
    ) -> Result<aspen_core::meta::SweepBatch, StoreError> {
        self.inner.sweep_orphan_packs(lease, grace, limit).await
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
    async fn add_compaction_heat(&self, heat: &[CompactionHeat]) -> Result<u64, StoreError> {
        self.inner.add_compaction_heat(heat).await
    }
}

/// Refs in the measured repository (the load test's 768 rows): trunk plus
/// 767 branches.
const REFS: usize = 768;
/// `ls-remote` polls measured per configuration.
const POLLS: u64 = 20;

/// One Aspen instance over the shared database, serving pushes and
/// fetches on a free port. Returns the repository URL.
async fn instance(
    meta: &Counting,
    dir: &std::path::Path,
    name: &str,
    ls_refs_cache_refs: usize,
) -> String {
    let blobs = LocalBlobStore::open(dir.join("blobs")).unwrap();
    let mut config = aspen_server::ServerConfig::new(MaxObjectBytes::new(64 << 20).unwrap());
    config.ls_refs_cache_refs = ls_refs_cache_refs;
    let server = aspen_server::Server::new(meta.clone(), blobs, config)
        .with_receive_pack(receive_support::config(&dir.join(name)))
        .unwrap();
    let policy: aspen_auth::PolicyConfig =
        serde_json::from_value(serde_json::json!({ "anonymous": ["write:demo"] })).unwrap();
    let access = Arc::new(aspen_server::access::Access::new(
        aspen_auth::AuthService::new(policy, aspen_auth::MemoryTokenStore::new()).unwrap(),
        aspen_auth::Limits::new(aspen_auth::LimitsConfig::default()),
        meta.clone(),
        aspen_server::access::ClientAddr::Peer,
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = server.router(access);
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}/demo.git")
}

/// `git ls-remote <url> <ref>` (stock: protocol v2, no `ref-prefix`), as
/// `(ref name, tip)` lines.
async fn ls_remote(w: &Work, url: &str, pattern: &str) -> Vec<(String, String)> {
    let (url, pattern) = (url.to_owned(), pattern.to_owned());
    let mut cmd = w.command();
    let out = blocking(move || cmd.args(["ls-remote", &url, &pattern]).output().unwrap()).await;
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout)
        .unwrap()
        .lines()
        .map(|l| {
            let (tip, name) = l.split_once('\t').unwrap();
            (name.to_owned(), tip.to_owned())
        })
        .collect()
}

/// Invariant: repeated `ls-remote` polls of an unchanged repository cost
/// one `ref_changes` statement each and read no ref rows after the first;
/// without the cache each poll scans every ref. A push acknowledged by
/// either instance is in the very next poll of either instance (read
/// after write, exact on the same instance and across instances), and so
/// is a deletion.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ls_remote_polls_cost_one_statement_and_see_every_acknowledged_push() {
    let dir = tempfile::tempdir().unwrap();
    let url = format!("sqlite://{}?mode=rwc", dir.path().join("meta.db").display());
    let inner = SqlStore::connect(&url).await.unwrap();
    inner.migrate().await.unwrap();
    let meta = Counting {
        inner,
        counts: Arc::default(),
    };
    let main = RefName::new("refs/heads/main").unwrap();
    let settings = RepoSettings::new(main.clone()).unwrap();
    let repo = meta
        .create_repo(&RepoName::new("demo").unwrap(), &settings)
        .await
        .unwrap();
    let a = instance(&meta, dir.path(), "a", 250_000).await;
    let b = instance(&meta, dir.path(), "b", 250_000).await;
    let uncached = instance(&meta, dir.path(), "uncached", 0).await;

    let home = dir.path().join("home");
    std::fs::create_dir_all(&home).unwrap();
    let w = Work {
        path: dir.path().join("w"),
        home,
    };
    std::fs::create_dir_all(&w.path).unwrap();
    w.git(&["init", "-q", "--initial-branch=main"]);
    let c1 = w.commit("a.txt", "a\n", "A");
    w.git(&["push", "-q", &a, "main"]);

    // 767 more branches at trunk's tip, in one store commit.
    let trunk = meta.current_version(repo.id, &main).await.unwrap().unwrap();
    let writes: Vec<RefWrite> = (1..REFS)
        .map(|i| RefWrite {
            name: RefName::new(format!("refs/heads/agent-{i:04}")).unwrap(),
            expected: None,
            value: RefValue::Live {
                tip: trunk.value.tip().unwrap(),
                pack: None,
                closure: Closure::Exact,
            },
            deps: Deps {
                trunk: Some(trunk.id),
                scoped_old: None,
            },
            commits: Vec::new(),
            jobs: Vec::new(),
        })
        .collect();
    let outcome = meta
        .commit_push(&PushCommit {
            repo: repo.id,
            writes,
        })
        .await
        .unwrap();
    assert!(
        matches!(outcome, CommitOutcome::Committed(_)),
        "{outcome:?}"
    );

    let poll = "refs/heads/agent-0500";
    let want = vec![(poll.to_owned(), c1.clone())];

    // Before (cache off): every poll scans every ref.
    meta.counts.take();
    for _ in 0..POLLS {
        assert_eq!(ls_remote(&w, &uncached, poll).await, want);
    }
    let (scans, changes, rows) = meta.counts.take();
    eprintln!("uncached: {POLLS} polls, {scans} scan statements, {rows} ref rows");
    assert_eq!(changes, 0);
    assert_eq!(scans, 2 * POLLS, "a HEAD lookup and one page per poll");
    assert_eq!(rows, POLLS * (REFS as u64 + 1));

    // After: the first poll lists every ref once; every later one is one
    // `ref_changes` statement that returns no rows.
    assert_eq!(ls_remote(&w, &a, poll).await, want);
    let (scans, changes, rows) = meta.counts.take();
    assert_eq!((scans, changes, rows), (0, 1, REFS as u64));
    for _ in 0..POLLS {
        assert_eq!(ls_remote(&w, &a, poll).await, want);
    }
    let (scans, changes, rows) = meta.counts.take();
    eprintln!("cached: {POLLS} polls, {changes} ref_changes statements, {rows} ref rows");
    assert_eq!((scans, changes, rows), (0, POLLS, 0));
    // Instance b lists every ref once too.
    assert_eq!(ls_remote(&w, &b, poll).await, want);

    // Read after write on the same instance, then across instances.
    w.git(&["checkout", "-q", "-b", "agent-0500"]);
    let c2 = w.commit("b.txt", "b\n", "B");
    w.git(&["push", "-q", &a, "agent-0500"]);
    let moved = vec![(poll.to_owned(), c2.clone())];
    assert_eq!(ls_remote(&w, &a, poll).await, moved, "same instance");
    assert_eq!(ls_remote(&w, &b, poll).await, moved, "other instance");
    let c3 = w.commit("c.txt", "c\n", "C");
    w.git(&["push", "-q", &b, "agent-0500"]);
    let moved = vec![(poll.to_owned(), c3.clone())];
    assert_eq!(ls_remote(&w, &a, poll).await, moved, "pushed through b");
    assert_eq!(ls_remote(&w, &b, poll).await, moved, "same instance");
    meta.counts.take();
    assert_eq!(ls_remote(&w, &a, poll).await, moved);
    assert_eq!(
        meta.counts.take(),
        (0, 1, 0),
        "after a push, a poll reads the moved ref only once"
    );

    // A new ref and a deletion pushed through b, seen through a.
    w.git(&["push", "-q", &b, "agent-0500:refs/heads/fresh"]);
    w.git(&[
        "push",
        "-q",
        "-o",
        "force",
        &b,
        "--delete",
        "refs/heads/agent-0007",
    ]);
    let all = ls_remote(&w, &a, "refs/heads/*").await;
    assert_eq!(all.len(), REFS, "767 + fresh - agent-0007, plus main");
    assert!(all.contains(&("refs/heads/fresh".to_owned(), c3.clone())));
    assert!(!all.iter().any(|(n, _)| n == "refs/heads/agent-0007"));
    // HEAD follows trunk.
    assert_eq!(
        ls_remote(&w, &a, "HEAD").await,
        vec![("HEAD".to_owned(), c1.clone())]
    );
    // The cached and uncached listings agree on everything.
    assert_eq!(all, ls_remote(&w, &uncached, "refs/heads/*").await);
    let full = |u: &str| {
        let mut cmd = w.command();
        let u = u.to_owned();
        async move { blocking(move || cmd.args(["ls-remote", &u]).output().unwrap().stdout).await }
    };
    assert_eq!(full(&a).await, full(&uncached).await);
}
