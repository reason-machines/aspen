//! The [`MetaStore`] contract on real databases.
//!
//! Every case runs twice: on SQLite (a file in a temporary directory) and
//! on a throwaway Postgres cluster (see `common`), each time on a fresh
//! database with schema v1 applied. The store's clock is a counter the case
//! controls, so lease expiry needs no sleeping.
//!
//! Each case states the invariant it pins in its doc comment.

mod common;

use std::collections::BTreeSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use aspen_core::error::StoreError;
use aspen_core::id::{
    CoverId, JobId, ObjectId, PackChecksum, PackId, RefVersionId, RepoId, Seq, UnixMicros,
};
use aspen_core::ingest::SortedIds;
use aspen_core::meta::{
    CommitLookup, CommitOutcome, CommitRow, CompactionHeat, Cover, CoverChain, GC_LEASE, JobEvent,
    JobKind, JobState, JobTag, Lease, MAX_JOB_ATTEMPTS, MAX_JOB_HEAT, MetaStore, NewCommit,
    NewCover, NewJob, PackReferences, PackTombstone, PushCommit, RefChanges, RefCursor,
    RefListQuery, RefWrite, SeqRange, ShallowDepth, ShallowRecorded, SweepBatch, VerifiedRange,
    VersionJob, VersionJobKind,
};
use aspen_core::name::{RefName, RefPattern, RepoName};
use aspen_core::pack::{CoverLevel, NewPack, PackGeneration, PackOrigin, entry_table_len};
use aspen_core::refs::{Closure, Deps, RefValue, RefVersion};
use aspen_core::repo::{ForcePolicy, Repo, RepoSettings, ShallowPolicy};
use aspen_meta::{
    Clock, ConflictRow, Dialect, JobCount, MirrorRefState, MirrorRow, OpenJobState, SqlStore,
    SshKeyRow, StoreOptions, TokenRow, TrunkClosures,
};
use sqlx::{AssertSqlSafe, Row as _};

use common::PgCluster;

// ---------------------------------------------------------------------------
// Harness

struct Ctx {
    store: SqlStore,
    /// The database URL, for cases that connect a second store.
    url: String,
    /// The store's clock, in microseconds.
    time: Arc<AtomicI64>,
}

impl Ctx {
    fn now(&self) -> UnixMicros {
        UnixMicros(self.time.load(Ordering::SeqCst))
    }
    fn set_time(&self, t: i64) {
        self.time.store(t, Ordering::SeqCst);
    }
}

type CaseFn = fn(Ctx) -> Pin<Box<dyn Future<Output = ()> + Send>>;

macro_rules! cases {
    ($($f:ident),* $(,)?) => {
        &[$((stringify!($f), (|c| Box::pin($f(c))) as CaseFn)),*]
    };
}

const CASES: &[(&str, CaseFn)] = cases![
    repos_round_trip_and_names_are_unique,
    a_ref_history_is_one_chain_including_deletions,
    a_stale_expectation_loses_and_writes_nothing,
    a_lost_race_on_update_is_cas_lost_after_the_winner_commits,
    a_lost_race_on_create_is_cas_lost,
    concurrent_pushers_lose_exactly_once_per_round,
    free_running_pushers_build_one_gap_free_chain,
    an_atomic_push_is_all_or_nothing,
    a_group_commit_isolates_each_push_in_one_transaction,
    a_group_registration_isolates_each_pack_in_one_transaction,
    concurrent_group_commits_over_shared_refs_never_deadlock,
    a_group_commit_whose_transaction_fails_commits_no_push,
    the_database_refuses_a_version_that_is_not_the_expected_child,
    commit_rows_resolve_in_order_and_are_never_overwritten,
    commit_generations_round_trip_page_and_are_never_overwritten,
    packs_register_idempotently_and_read_back_exactly,
    list_refs_filters_by_prefix_bytewise_and_paginates,
    list_refs_takes_any_number_of_prefixes,
    ref_changes_since_a_cursor_are_exactly_the_refs_that_moved,
    ref_changes_find_a_write_that_commits_after_a_later_one,
    versions_by_seq_serves_have_tips,
    current_versions_reads_many_refs_in_one_statement,
    current_and_recent_reads_a_window_after_a_seq,
    reachable_versions_walks_dependencies_to_the_stops,
    a_branch_push_commits_in_few_statements,
    a_registration_that_waits_for_a_sweep_takes_the_next_generation,
    covers_are_ordered_by_level_then_first_seq,
    covers_at_reads_only_the_covers_it_names,
    tokens_round_trip_and_revoke_once,
    ssh_keys_round_trip_and_revoke_once,
    mirrors_round_trip_and_removal_drops_the_ledger,
    a_mirror_repository_is_created_whole_or_not_at_all,
    the_mirror_ledger_is_a_compare_and_swap,
    enqueue_is_idempotent_per_kind_and_target,
    claims_take_the_earliest_due_job_and_are_fenced,
    heat_raises_the_queued_compact_jobs_fetches_wait_on,
    the_store_follows_the_job_state_machine,
    dead_jobs_are_requeued_as_new_jobs,
    release_gives_back_only_the_owners_jobs_of_the_kinds_asked,
    job_counts_cover_open_jobs_per_kind_and_state,
    trunk_closures_count_each_repository_by_its_current_trunk,
    concurrent_workers_never_claim_one_job_twice,
    a_refused_request_releases_its_locks,
    concurrent_pushes_index_shared_commits_in_any_order,
    a_cancelled_push_is_all_or_nothing_and_releases_its_locks,
    a_lock_wait_past_the_lock_timeout_is_unavailable,
    a_burst_of_operations_queues_for_the_pool_instead_of_failing,
    a_pool_that_makes_no_progress_fails_its_waiters,
    insert_cover_is_idempotent_per_span,
    a_cover_chain_round_trips_and_starts_at_the_first_version,
    set_cover_pack_records_one_pack_at_the_cover_level,
    pack_by_checksum_looks_up_one_repository,
    registering_a_pack_again_refreshes_its_time,
    an_orphan_is_swept_after_grace_and_tombstoned,
    the_sweep_never_deletes_a_referenced_pack,
    a_legacy_orphan_survives_its_first_sweep,
    a_pack_of_a_lost_compare_and_swap_is_swept,
    a_push_naming_a_swept_pack_is_unavailable,
    a_cover_naming_a_swept_pack_is_unavailable,
    the_sweep_spares_a_pack_an_open_push_is_referencing,
    the_sweep_resumes_where_it_stopped_in_bounded_batches,
    a_pack_unnamed_behind_the_cursor_survives_its_grace_then_goes,
    the_walk_never_passes_a_pack_an_open_transaction_holds,
    the_audit_stamps_a_legacy_pack_behind_the_walk,
    a_lease_has_one_holder_and_fences_a_stale_one,
    a_shallow_pack_is_recorded_once_and_read_back,
    a_shallow_pack_naming_a_swept_pack_is_unavailable,
    a_shallow_pack_stays_inside_its_repository,
    the_sweep_spares_a_pack_a_shallow_recording_names,
    eviction_takes_old_recordings_of_versions_no_longer_current,
    an_evicted_shallow_pack_is_swept_a_grace_after_eviction,
    the_byte_cap_evicts_the_least_recently_used_packs_first,
    a_pack_over_the_cap_alone_is_not_recorded,
    unused_recordings_are_evicted_current_or_not,
    concurrent_recordings_never_overshoot_the_cap,
    a_recordings_repository_lock_never_blocks_pushes_or_compaction,
    replace_pack_moves_every_reference_in_one_transaction,
    replace_pack_refuses_a_mismatched_or_swept_replacement,
];

async fn run_case(name: &str, url: &str, case: CaseFn) {
    eprintln!("case {name}");
    let time = Arc::new(AtomicI64::new(1_000_000));
    let t = Arc::clone(&time);
    let store = SqlStore::connect(url)
        .await
        .unwrap_or_else(|e| panic!("{name}: connect: {e}"))
        .with_clock(Clock::from_fn(move || UnixMicros(t.load(Ordering::SeqCst))));
    store.migrate().await.expect("migrations apply");
    let pool = store.pool().clone();
    case(Ctx {
        store,
        url: url.to_owned(),
        time,
    })
    .await;
    pool.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_store() {
    let dir = tempfile::tempdir().unwrap();
    for (i, (name, case)) in CASES.iter().enumerate() {
        let url = format!(
            "sqlite://{}?mode=rwc",
            dir.path().join(format!("case{i}.db")).display()
        );
        run_case(name, &url, *case).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_store() {
    let Some(bin) = common::pg_or_skip() else {
        return;
    };
    let cluster = PgCluster::start(&bin);
    let admin = SqlStore::connect(&cluster.url()).await.unwrap();
    assert_eq!(admin.dialect(), Dialect::Postgres);
    for (i, (name, case)) in CASES.iter().enumerate() {
        let db = format!("case{i}");
        sqlx::query(AssertSqlSafe(format!("CREATE DATABASE {db}")))
            .execute(admin.pool())
            .await
            .unwrap();
        run_case(name, &cluster.url_for(&db), *case).await;
    }

    // The listing case again, on a database whose default collation is not
    // bytewise. The test clusters are created with `--no-locale`, where
    // every comparison is bytewise anyway; here only the schema's
    // `COLLATE "C"` on ref names keeps `ls-refs` order and prefix ranges
    // bytewise. ICU is used because it needs no OS locale on the runner.
    sqlx::query(
        "CREATE DATABASE icu_en LOCALE_PROVIDER = icu ICU_LOCALE = 'en-US' TEMPLATE = template0",
    )
    .execute(admin.pool())
    .await
    .expect("PostgreSQL with ICU support (Debian and Ubuntu packages have it)");
    let url = cluster.url_for("icu_en");
    let probe = SqlStore::connect(&url).await.unwrap();
    let bytewise: bool = sqlx::query("SELECT 'B' < 'a' AS b")
        .fetch_one(probe.pool())
        .await
        .unwrap()
        .get("b");
    assert!(!bytewise, "the icu_en database must not collate bytewise");
    probe.pool().close().await;
    run_case(
        "list_refs_filters_by_prefix_bytewise_and_paginates (ICU en-US database)",
        &url,
        |c| Box::pin(list_refs_filters_by_prefix_bytewise_and_paginates(c)),
    )
    .await;
    admin.pool().close().await;
}

// ---------------------------------------------------------------------------
// Fixtures

fn oid(n: u8) -> ObjectId {
    assert_ne!(n, 0);
    ObjectId::from_bytes([n; 20])
}

fn oid_n(n: u32) -> ObjectId {
    let mut b = [0xab; 20];
    b[..4].copy_from_slice(&n.to_be_bytes());
    ObjectId::from_bytes(b)
}

fn rn(s: &str) -> RefName {
    RefName::new(s).unwrap()
}

const MAIN: &str = "refs/heads/main";

async fn new_repo(s: &SqlStore, name: &str) -> Repo {
    s.create_repo(
        &RepoName::new(name).unwrap(),
        &RepoSettings::new(rn(MAIN)).unwrap(),
    )
    .await
    .unwrap()
}

fn push_pack(repo: RepoId, n: u8) -> NewPack {
    NewPack {
        repo,
        checksum: PackChecksum::from_bytes([n; 20]),
        size: 100,
        idx_size: 1072 + 28,
        entries_size: entry_table_len(1),
        object_count: 1,
        ref_delta_count: 0,
        origin: PackOrigin::Push { received_bytes: 80 },
    }
}

async fn pack(s: &SqlStore, repo: RepoId, n: u8) -> PackId {
    s.register_pack(&push_pack(repo, n)).await.unwrap().id
}

fn live(name: &str, expected: Option<RefVersionId>, tip: ObjectId, pack: PackId) -> RefWrite {
    RefWrite {
        name: rn(name),
        expected,
        value: RefValue::Live {
            tip,
            pack: Some(pack),
            closure: Closure::Exact,
        },
        deps: Deps::default(),
        commits: Vec::new(),
        jobs: Vec::new(),
    }
}

fn delete(name: &str, expected: RefVersionId) -> RefWrite {
    RefWrite {
        name: rn(name),
        expected: Some(expected),
        value: RefValue::Deleted,
        deps: Deps::default(),
        commits: Vec::new(),
        jobs: Vec::new(),
    }
}

async fn commit(s: &SqlStore, repo: RepoId, writes: Vec<RefWrite>) -> CommitOutcome {
    s.commit_push(&PushCommit { repo, writes })
        .await
        .unwrap_or_else(|e| panic!("commit_push: {e}"))
}

async fn committed(s: &SqlStore, repo: RepoId, writes: Vec<RefWrite>) -> Vec<RefVersion> {
    match commit(s, repo, writes).await {
        CommitOutcome::Committed(v) => v,
        other => panic!("expected Committed, got {other:?}"),
    }
}

/// One live version of `name`, committed against its current version.
async fn advance(s: &SqlStore, repo: RepoId, name: &str, tip: ObjectId, p: PackId) -> RefVersion {
    let current = s.current_version(repo, &rn(name)).await.unwrap();
    committed(s, repo, vec![live(name, current.map(|v| v.id), tip, p)])
        .await
        .remove(0)
}

async fn count(s: &SqlStore, table: &str) -> i64 {
    sqlx::query(AssertSqlSafe(format!("SELECT COUNT(*) AS n FROM {table}")))
        .fetch_one(s.pool())
        .await
        .unwrap()
        .get::<i64, _>("n")
}

async fn counts(s: &SqlStore) -> [i64; 4] {
    [
        count(s, "ref_versions").await,
        count(s, "refs").await,
        count(s, "commit_index").await,
        count(s, "jobs").await,
    ]
}

/// Begins a raw write transaction the way the store does.
async fn raw_write_tx(s: &SqlStore) -> sqlx::Transaction<'static, sqlx::Any> {
    match s.dialect() {
        Dialect::Postgres => s.pool().begin().await.unwrap(),
        Dialect::Sqlite => s.pool().begin_with("BEGIN IMMEDIATE").await.unwrap(),
    }
}

fn is_conflict(r: Result<impl std::fmt::Debug, StoreError>) -> bool {
    matches!(r, Err(StoreError::Conflict { .. }))
}

// ---------------------------------------------------------------------------
// Repositories

/// Settings round-trip exactly through both lookups, and a repository name
/// is taken at most once ([`StoreError::Conflict`], not a storage error).
async fn repos_round_trip_and_names_are_unique(c: Ctx) {
    let s = &c.store;
    let name = RepoName::new("acme/mono").unwrap();
    let settings = RepoSettings::builder(
        rn("refs/heads/trunk"),
        vec![
            RefPattern::parse("refs/heads/*").unwrap(),
            RefPattern::parse("refs/agents/*").unwrap(),
        ],
    )
    .allow_force(ForcePolicy::All)
    .no_thin(true)
    .have_tips(3)
    .shallow(ShallowPolicy::Refuse)
    .build()
    .unwrap();
    let repo = s.create_repo(&name, &settings).await.unwrap();
    assert_eq!(repo.settings, settings);
    assert_eq!(s.repo(&name).await.unwrap(), Some(repo.clone()));
    assert_eq!(s.repo_by_id(repo.id).await.unwrap(), Some(repo.clone()));
    assert_eq!(
        s.repo(&RepoName::new("acme/other").unwrap()).await.unwrap(),
        None
    );
    let defaults = RepoSettings::new(rn(MAIN)).unwrap();
    assert!(is_conflict(s.create_repo(&name, &defaults).await));
    let other = new_repo(s, "acme/other").await;
    assert_eq!(other.settings, defaults);
    assert_ne!(other.id, repo.id);
}

// ---------------------------------------------------------------------------
// Refs and the compare-and-swap

/// Versions of one ref form one chain: seq 1, 2, 3, ... with each parent
/// the previous version. A deletion is a version whose `refs` row stays,
/// and a later create is a compare-and-swap against the deletion.
async fn a_ref_history_is_one_chain_including_deletions(c: Ctx) {
    let s = &c.store;
    let repo = new_repo(s, "acme/r").await.id;
    let p = pack(s, repo, 1).await;
    assert_eq!(s.current_version(repo, &rn(MAIN)).await.unwrap(), None);

    let v1 = committed(s, repo, vec![live(MAIN, None, oid(1), p)])
        .await
        .remove(0);
    assert_eq!((v1.seq, v1.parent), (Seq::FIRST, None));
    assert_eq!(v1.value.tip(), Some(oid(1)));
    assert_eq!(v1.value.pack(), Some(p));
    assert_eq!(v1.value.closure(), Some(Closure::Exact));

    let mut w = live(MAIN, Some(v1.id), oid(2), p);
    w.value = RefValue::Live {
        tip: oid(2),
        pack: None,
        closure: Closure::Wider,
    };
    w.deps = Deps {
        trunk: Some(v1.id),
        scoped_old: None,
    };
    let v2 = committed(s, repo, vec![w]).await.remove(0);
    assert_eq!((v2.seq.get(), v2.parent), (2, Some(v1.id)));
    assert_eq!(v2.deps.trunk, Some(v1.id));
    assert_eq!(v2.value.closure(), Some(Closure::Wider));
    assert_eq!(v2.value.pack(), None);

    let v3 = committed(s, repo, vec![delete(MAIN, v2.id)])
        .await
        .remove(0);
    assert_eq!((v3.seq.get(), v3.value), (3, RefValue::Deleted));
    let current = s.current_version(repo, &rn(MAIN)).await.unwrap().unwrap();
    assert_eq!(current, v3, "a deleted ref keeps its row, at the deletion");
    assert!(
        s.list_refs(repo, &all_refs()).await.unwrap().is_empty(),
        "ls-refs leaves deleted refs out"
    );

    let v4 = committed(s, repo, vec![live(MAIN, Some(v3.id), oid(4), p)])
        .await
        .remove(0);
    assert_eq!((v4.seq.get(), v4.parent), (4, Some(v3.id)));
    for v in [&v1, &v2, &v3, &v4] {
        assert_eq!(s.ref_version(v.id).await.unwrap().as_ref(), Some(v));
        assert_eq!(v.repo, repo);
        assert_eq!(v.name, rn(MAIN));
    }
    assert_eq!(
        s.ref_version(RefVersionId::new(9999).unwrap())
            .await
            .unwrap(),
        None
    );
}

/// A push whose expectation is stale gets `CasLost` naming the ref's
/// current version, and writes nothing: no version, no `refs` change, no
/// commit row, no job.
async fn a_stale_expectation_loses_and_writes_nothing(c: Ctx) {
    let s = &c.store;
    let repo = new_repo(s, "acme/r").await.id;
    let p = pack(s, repo, 1).await;
    let v1 = advance(s, repo, MAIN, oid(1), p).await;
    let v2 = advance(s, repo, MAIN, oid(2), p).await;
    let before = counts(s).await;

    let mut stale = live(MAIN, Some(v1.id), oid(3), p);
    stale.commits.push(NewCommit {
        commit: oid(3),
        parents: vec![oid(1)],
        committer_time: 7,
        generation: None,
        root_tree: None,
    });
    stale.jobs.push(VersionJob {
        kind: VersionJobKind::IndexCommits,
        run_after: UnixMicros(0),
    });
    let lost = CommitOutcome::CasLost {
        name: rn(MAIN),
        current: Some(v2.id),
    };
    assert_eq!(commit(s, repo, vec![stale]).await, lost);
    assert_eq!(
        commit(s, repo, vec![live(MAIN, None, oid(3), p)]).await,
        lost,
        "expecting no row when the ref exists"
    );
    assert_eq!(counts(s).await, before);
    assert_eq!(
        s.current_version(repo, &rn(MAIN)).await.unwrap().unwrap(),
        v2
    );
}

/// Two pushes race on one ref: the winner holds its transaction open with
/// the new `seq` inserted; the loser's `commit_push` waits for it (Postgres
/// on the `ref_versions_seq_unique` index, SQLite on `BEGIN IMMEDIATE`)
/// and, once the winner commits, returns `CasLost` naming the winner's
/// version, never a storage error.
async fn a_lost_race_on_update_is_cas_lost_after_the_winner_commits(c: Ctx) {
    let s = &c.store;
    let repo = new_repo(s, "acme/r").await.id;
    let p = pack(s, repo, 1).await;
    let v1 = advance(s, repo, MAIN, oid(1), p).await;

    let mut winner = raw_write_tx(s).await;
    let winner_id: i64 = sqlx::query(
        "INSERT INTO ref_versions (repo_id, ref_name, seq, parent_version_id, tip_oid, \
         closure_exact, pack_id) VALUES ($1, $2, 2, $3, $4, 1, $5) RETURNING id",
    )
    .bind(repo.get())
    .bind(MAIN)
    .bind(v1.id.get())
    .bind(oid(2).as_bytes().to_vec())
    .bind(p.get())
    .fetch_one(&mut *winner)
    .await
    .unwrap()
    .get("id");
    sqlx::query("UPDATE refs SET version_id = $1, seq = 2 WHERE repo_id = $2 AND ref_name = $3")
        .bind(winner_id)
        .bind(repo.get())
        .bind(MAIN)
        .execute(&mut *winner)
        .await
        .unwrap();

    let loser = s.clone();
    let push = PushCommit {
        repo,
        writes: vec![live(MAIN, Some(v1.id), oid(3), p)],
    };
    let handle = tokio::spawn(async move { loser.commit_push(&push).await });
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!handle.is_finished(), "the loser waits for the winner");
    winner.commit().await.unwrap();
    let outcome = handle.await.unwrap().expect("a lost race is not an error");
    assert_eq!(
        outcome,
        CommitOutcome::CasLost {
            name: rn(MAIN),
            current: Some(RefVersionId::new(winner_id).unwrap()),
        }
    );
    assert_eq!(count(s, "ref_versions").await, 2);
}

/// Two creates of a new ref race: the second sees a version 1 that is not
/// its own and gets `CasLost`, never a storage error.
async fn a_lost_race_on_create_is_cas_lost(c: Ctx) {
    let s = &c.store;
    let repo = new_repo(s, "acme/r").await.id;
    let p = pack(s, repo, 1).await;
    let mut winner = raw_write_tx(s).await;
    let winner_id: i64 = sqlx::query(
        "INSERT INTO ref_versions (repo_id, ref_name, seq, tip_oid, closure_exact, pack_id) \
         VALUES ($1, $2, 1, $3, 1, $4) RETURNING id",
    )
    .bind(repo.get())
    .bind("refs/heads/new")
    .bind(oid(1).as_bytes().to_vec())
    .bind(p.get())
    .fetch_one(&mut *winner)
    .await
    .unwrap()
    .get("id");
    sqlx::query("INSERT INTO refs (repo_id, ref_name, version_id, seq) VALUES ($1, $2, $3, 1)")
        .bind(repo.get())
        .bind("refs/heads/new")
        .bind(winner_id)
        .execute(&mut *winner)
        .await
        .unwrap();
    let loser = s.clone();
    let push = PushCommit {
        repo,
        writes: vec![live("refs/heads/new", None, oid(2), p)],
    };
    let handle = tokio::spawn(async move { loser.commit_push(&push).await });
    tokio::time::sleep(Duration::from_millis(300)).await;
    winner.commit().await.unwrap();
    assert_eq!(
        handle.await.unwrap().expect("a lost race is not an error"),
        CommitOutcome::CasLost {
            name: rn("refs/heads/new"),
            current: Some(RefVersionId::new(winner_id).unwrap()),
        }
    );
}

/// Real concurrent races through the store alone: in each round, six
/// pushers read the same current version, wait at a barrier, and push
/// against it. Exactly one commits; the other five get `CasLost` naming
/// the winner's version.
async fn concurrent_pushers_lose_exactly_once_per_round(c: Ctx) {
    const PUSHERS: usize = 6;
    let s = &c.store;
    let repo = new_repo(s, "acme/r").await.id;
    let p = pack(s, repo, 1).await;
    advance(s, repo, MAIN, oid(1), p).await;
    for round in 0..8u32 {
        let current = s.current_version(repo, &rn(MAIN)).await.unwrap().unwrap();
        let barrier = Arc::new(tokio::sync::Barrier::new(PUSHERS));
        let mut handles = Vec::new();
        for i in 0..PUSHERS {
            let (s, barrier) = (s.clone(), Arc::clone(&barrier));
            let tip = oid_n(round * 100 + i as u32);
            handles.push(tokio::spawn(async move {
                barrier.wait().await;
                s.commit_push(&PushCommit {
                    repo,
                    writes: vec![live(MAIN, Some(current.id), tip, p)],
                })
                .await
            }));
        }
        let mut winners = Vec::new();
        let mut losers = Vec::new();
        for h in handles {
            match h.await.unwrap().expect("no storage error under contention") {
                CommitOutcome::Committed(v) => winners.push(v[0].clone()),
                CommitOutcome::CasLost { current, .. } => losers.push(current),
            }
        }
        assert_eq!(winners.len(), 1, "round {round}");
        let w = &winners[0];
        assert_eq!(
            (w.seq, w.parent),
            (current.seq.next().unwrap(), Some(current.id))
        );
        assert_eq!(losers, vec![Some(w.id); PUSHERS - 1], "round {round}");
    }
}

/// Six pushers each land eight versions on one ref, retrying against the
/// version a lost race names. No push fails with a storage error, and the
/// ref's history is one gap-free chain of all 49 versions.
async fn free_running_pushers_build_one_gap_free_chain(c: Ctx) {
    let s = &c.store;
    let repo = new_repo(s, "acme/r").await.id;
    let p = pack(s, repo, 1).await;
    advance(s, repo, MAIN, oid(1), p).await;
    let mut handles = Vec::new();
    for i in 0..6u32 {
        let s = s.clone();
        handles.push(tokio::spawn(async move {
            let mut expected = s
                .current_version(repo, &rn(MAIN))
                .await
                .unwrap()
                .map(|v| v.id);
            let mut losses = 0;
            for k in 0..8u32 {
                loop {
                    let push = PushCommit {
                        repo,
                        writes: vec![live(MAIN, expected, oid_n(i * 1000 + k), p)],
                    };
                    match s.commit_push(&push).await.expect("no storage error") {
                        CommitOutcome::Committed(v) => {
                            expected = Some(v[0].id);
                            break;
                        }
                        CommitOutcome::CasLost { current, .. } => {
                            losses += 1;
                            expected = current;
                        }
                    }
                }
            }
            losses
        }));
    }
    let mut losses = 0;
    for h in handles {
        losses += h.await.unwrap();
    }
    eprintln!("  {losses} lost races");
    let current = s.current_version(repo, &rn(MAIN)).await.unwrap().unwrap();
    assert_eq!(current.seq.get(), 49);
    let seqs: Vec<Seq> = (1..=49).map(|n| Seq::new(n).unwrap()).collect();
    let chain = s.versions_by_seq(repo, &rn(MAIN), &seqs).await.unwrap();
    assert_eq!(chain.len(), 49);
    for pair in chain.windows(2) {
        assert_eq!(pair[1].parent, Some(pair[0].id));
        assert_eq!(pair[1].seq, pair[0].seq.next().unwrap());
    }
    let tips: BTreeSet<_> = chain.iter().filter_map(|v| v.value.tip()).collect();
    assert_eq!(tips.len(), 49, "every push landed exactly once");
}

/// Group commit (`commit_pushes`): every push of a batch runs in one
/// transaction (exactly one more write transaction), and each push keeps
/// its own compare-and-swap. A push that loses (to another push of the same
/// batch, or to an older version), a malformed push and a push naming a
/// pack that no longer exists each write nothing and get their own answer;
/// an atomic push whose second write loses is undone as a whole; the rest
/// commit, with their commit rows and jobs.
async fn a_group_commit_isolates_each_push_in_one_transaction(c: Ctx) {
    let s = &c.store;
    let repo = new_repo(s, "acme/r").await.id;
    let p = pack(s, repo, 1).await;
    let main1 = advance(s, repo, MAIN, oid(1), p).await;
    let a1 = advance(s, repo, "refs/heads/a", oid(2), p).await;
    let a2 = advance(s, repo, "refs/heads/a", oid(3), p).await;
    let before = counts(s).await;
    let tx_before = s.write_transactions();

    let mut first = live(MAIN, Some(main1.id), oid(10), p);
    first.commits.push(NewCommit {
        commit: oid(10),
        parents: vec![oid(1)],
        committer_time: 7,
        generation: None,
        root_tree: None,
    });
    first.jobs.push(VersionJob {
        kind: VersionJobKind::IndexCommits,
        run_after: UnixMicros(0),
    });
    let batch = vec![
        // 0: wins main.
        PushCommit {
            repo,
            writes: vec![first],
        },
        // 1: expects the same version of main as 0, in the same batch.
        PushCommit {
            repo,
            writes: vec![live(MAIN, Some(main1.id), oid(11), p)],
        },
        // 2: a stale expectation of `a`.
        PushCommit {
            repo,
            writes: vec![live("refs/heads/a", Some(a1.id), oid(12), p)],
        },
        // 3: malformed (no writes).
        PushCommit {
            repo,
            writes: Vec::new(),
        },
        // 4: names a pack that does not exist (garbage-collected).
        PushCommit {
            repo,
            writes: vec![live(
                "refs/heads/b",
                None,
                oid(13),
                PackId::new(1_000_000).unwrap(),
            )],
        },
        // 5: atomic; `0c` (created first: it sorts before `a`) must be
        // undone when `a` loses.
        PushCommit {
            repo,
            writes: vec![
                live("refs/heads/0c", None, oid(14), p),
                live("refs/heads/a", Some(a1.id), oid(15), p),
            ],
        },
        // 6: creates a new ref.
        PushCommit {
            repo,
            writes: vec![live("refs/heads/d", None, oid(16), p)],
        },
    ];
    let out = s.commit_pushes(&batch).await.expect("the batch commits");
    assert_eq!(
        s.write_transactions() - tx_before,
        1,
        "one write transaction for the batch"
    );
    assert_eq!(out.len(), batch.len());
    let won = match &out[0] {
        Ok(CommitOutcome::Committed(v)) => v[0].clone(),
        other => panic!("push 0: {other:?}"),
    };
    assert_eq!(
        (won.seq, won.parent),
        (main1.seq.next().unwrap(), Some(main1.id))
    );
    assert_eq!(
        out[1],
        Ok(CommitOutcome::CasLost {
            name: rn(MAIN),
            current: Some(won.id),
        })
    );
    assert_eq!(
        out[2],
        Ok(CommitOutcome::CasLost {
            name: rn("refs/heads/a"),
            current: Some(a2.id),
        })
    );
    assert!(is_conflict(out[3].clone()), "push 3: {:?}", out[3]);
    assert!(
        matches!(&out[4], Err(e) if e.is_pack_swept()),
        "push 4: {:?}",
        out[4]
    );
    assert_eq!(
        out[5],
        Ok(CommitOutcome::CasLost {
            name: rn("refs/heads/a"),
            current: Some(a2.id),
        })
    );
    let made = match &out[6] {
        Ok(CommitOutcome::Committed(v)) => v[0].clone(),
        other => panic!("push 6: {other:?}"),
    };

    // Only pushes 0 and 6 wrote: two versions, one new ref row, push 0's
    // commit row and job.
    let after = counts(s).await;
    assert_eq!(
        [
            after[0] - before[0],
            after[1] - before[1],
            after[2] - before[2],
            after[3] - before[3]
        ],
        [2, 1, 1, 1]
    );
    assert_eq!(s.current_version(repo, &rn(MAIN)).await.unwrap(), Some(won));
    assert_eq!(
        s.current_version(repo, &rn("refs/heads/a")).await.unwrap(),
        Some(a2)
    );
    for gone in ["refs/heads/b", "refs/heads/0c"] {
        assert_eq!(
            s.current_version(repo, &rn(gone)).await.unwrap(),
            None,
            "{gone}"
        );
    }
    assert_eq!(
        s.current_version(repo, &rn("refs/heads/d")).await.unwrap(),
        Some(made)
    );
}

/// Group registration (`register_packs`): one write transaction; new packs
/// are registered, a pack already registered comes back as its row, the
/// same checksum twice in one batch is one row, and a registration the
/// store refuses (different sizes for a known checksum, or a malformed
/// pack) fails alone.
async fn a_group_registration_isolates_each_pack_in_one_transaction(c: Ctx) {
    let s = &c.store;
    let repo = new_repo(s, "acme/r").await.id;
    let known = s.register_pack(&push_pack(repo, 1)).await.unwrap();
    let mut resized = push_pack(repo, 1);
    resized.size += 1;
    let mut malformed = push_pack(repo, 9);
    malformed.object_count = 0;
    let tx_before = s.write_transactions();
    let packs_before = count(s, "packs").await;
    let out = s
        .register_packs(&[
            push_pack(repo, 3),
            resized,
            push_pack(repo, 1),
            malformed,
            push_pack(repo, 2),
            push_pack(repo, 3),
        ])
        .await
        .expect("the batch commits");
    assert_eq!(s.write_transactions() - tx_before, 1);
    let ok = |i: usize| out[i].clone().unwrap_or_else(|e| panic!("pack {i}: {e}"));
    assert!(is_conflict(out[1].clone()), "pack 1: {:?}", out[1]);
    assert!(is_conflict(out[3].clone()), "pack 3: {:?}", out[3]);
    assert_eq!(ok(2), known);
    assert_eq!(ok(0), ok(5), "one checksum, one row");
    assert_ne!(ok(0).id, ok(4).id);
    assert_eq!(count(s, "packs").await - packs_before, 2);
    for i in [0, 4] {
        assert_eq!(
            s.packs(&[ok(i).id]).await.unwrap(),
            vec![ok(i)],
            "pack {i} reads back"
        );
    }
}

/// Batches that run at once over the same refs, each listing its pushes
/// in a different order, finish without a deadlock (the store orders a
/// batch's pushes by ref name) or any storage error, and every ref's
/// history stays one gap-free chain holding every push exactly once.
async fn concurrent_group_commits_over_shared_refs_never_deadlock(c: Ctx) {
    const REFS: usize = 6;
    const TASKS: u32 = 6;
    const ROUNDS: u32 = 6;
    let s = &c.store;
    let repo = new_repo(s, "acme/r").await.id;
    let p = pack(s, repo, 1).await;
    let names: Vec<String> = (0..REFS).map(|i| format!("refs/heads/r{i}")).collect();
    let mut handles = Vec::new();
    for t in 0..TASKS {
        let (s, names) = (s.clone(), names.clone());
        handles.push(tokio::spawn(async move {
            let mut landed = 0u32;
            for round in 0..ROUNDS {
                // Every ref once, in an order of this task's own, retried
                // until each lands.
                let mut left: Vec<usize> = (0..REFS).collect();
                left.rotate_left((t as usize + round as usize) % REFS);
                if t % 2 == 1 {
                    left.reverse();
                }
                let mut attempt = 0u32;
                while !left.is_empty() {
                    let mut batch = Vec::new();
                    for &r in &left {
                        let current = s
                            .current_version(repo, &rn(&names[r]))
                            .await
                            .unwrap()
                            .map(|v| v.id);
                        let tip = oid_n(t * 1_000_000 + round * 10_000 + attempt * 100 + r as u32);
                        batch.push(PushCommit {
                            repo,
                            writes: vec![live(&names[r], current, tip, p)],
                        });
                    }
                    let out = s
                        .commit_pushes(&batch)
                        .await
                        .expect("no batch fails under contention");
                    let mut still = Vec::new();
                    for (&r, o) in left.iter().zip(out) {
                        match o.expect("no storage error under contention") {
                            CommitOutcome::Committed(_) => landed += 1,
                            CommitOutcome::CasLost { .. } => still.push(r),
                        }
                    }
                    left = still;
                    attempt += 1;
                }
            }
            landed
        }));
    }
    let deadline = Duration::from_secs(120);
    let mut landed = 0;
    for h in handles {
        landed += tokio::time::timeout(deadline, h)
            .await
            .expect("no batch hangs")
            .unwrap();
    }
    assert_eq!(landed, TASKS * ROUNDS * REFS as u32);
    for name in &names {
        let current = s.current_version(repo, &rn(name)).await.unwrap().unwrap();
        let n = u64::from(TASKS * ROUNDS);
        assert_eq!(current.seq.get(), n, "{name}");
        let seqs: Vec<Seq> = (1..=n).map(|k| Seq::new(k).unwrap()).collect();
        let chain = s.versions_by_seq(repo, &rn(name), &seqs).await.unwrap();
        assert_eq!(chain.len() as u64, n, "{name}");
        for pair in chain.windows(2) {
            assert_eq!(pair[1].parent, Some(pair[0].id), "{name}");
        }
    }
}

/// When a batch's transaction cannot commit, no push of it is reported
/// committed and none of its writes land. On Postgres a deferred
/// constraint trigger refuses one push's commit row at `COMMIT`, after
/// every savepoint was released: the whole batch must fail. SQLite has no
/// deferred triggers, so there the trigger refuses the row in its
/// statement, inside the push's own savepoint: that push alone fails and
/// the others commit.
async fn a_group_commit_whose_transaction_fails_commits_no_push(c: Ctx) {
    let s = &c.store;
    let repo = new_repo(s, "acme/r").await.id;
    let p = pack(s, repo, 1).await;
    let main1 = advance(s, repo, MAIN, oid(1), p).await;
    let poison = oid(0x66);
    match s.dialect() {
        Dialect::Postgres => {
            for stmt in [
                format!(
                    "CREATE FUNCTION refuse_poison() RETURNS trigger LANGUAGE plpgsql AS $$ \
                     BEGIN IF NEW.commit_oid = '\\x{}'::bytea THEN \
                     RAISE EXCEPTION 'refused at commit'; END IF; RETURN NULL; END $$",
                    poison
                ),
                "CREATE CONSTRAINT TRIGGER refuse_poison AFTER INSERT ON commit_index \
                 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION refuse_poison()"
                    .to_owned(),
            ] {
                sqlx::query(AssertSqlSafe(stmt))
                    .execute(s.pool())
                    .await
                    .unwrap();
            }
        }
        Dialect::Sqlite => {
            // SQLite has no deferred triggers: refuse in the statement, and
            // check below that the push's savepoint isolates it instead.
            sqlx::query(AssertSqlSafe(format!(
                "CREATE TRIGGER refuse_poison BEFORE INSERT ON commit_index \
                 WHEN NEW.commit_oid = X'{poison}' BEGIN SELECT RAISE(ABORT, 'refused'); END"
            )))
            .execute(s.pool())
            .await
            .unwrap();
        }
    }
    let before = counts(s).await;
    let mut poisoned = live("refs/heads/p", None, poison, p);
    poisoned.commits.push(NewCommit {
        commit: poison,
        parents: Vec::new(),
        committer_time: 1,
        generation: None,
        root_tree: None,
    });
    let batch = vec![
        PushCommit {
            repo,
            writes: vec![live(MAIN, Some(main1.id), oid(2), p)],
        },
        PushCommit {
            repo,
            writes: vec![poisoned],
        },
        PushCommit {
            repo,
            writes: vec![live("refs/heads/q", None, oid(3), p)],
        },
    ];
    let out = s.commit_pushes(&batch).await;
    match s.dialect() {
        Dialect::Postgres => {
            assert!(out.is_err(), "a failed COMMIT fails the batch: {out:?}");
            assert_eq!(counts(s).await, before, "nothing of the batch landed");
            assert_eq!(
                s.current_version(repo, &rn(MAIN)).await.unwrap(),
                Some(main1)
            );
        }
        Dialect::Sqlite => {
            // The refusal is a statement error inside the push's own
            // savepoint: that push alone fails, the others commit.
            let out = out.expect("the batch commits");
            assert!(matches!(out[0], Ok(CommitOutcome::Committed(_))), "{out:?}");
            assert!(out[1].is_err(), "{out:?}");
            assert!(matches!(out[2], Ok(CommitOutcome::Committed(_))), "{out:?}");
            assert_eq!(
                s.current_version(repo, &rn("refs/heads/p")).await.unwrap(),
                None
            );
        }
    }
}

/// An atomic push is one transaction: if any write loses, the writes that
/// already ran (here a create that sorts first) are rolled back too. When
/// all win, each write's commit rows and jobs name that write's own new
/// version, and versions come back in `writes` order, not name order.
async fn an_atomic_push_is_all_or_nothing(c: Ctx) {
    let s = &c.store;
    let repo = new_repo(s, "acme/r").await.id;
    let p = pack(s, repo, 1).await;
    let z1 = advance(s, repo, "refs/heads/z", oid(1), p).await;
    let z2 = advance(s, repo, "refs/heads/z", oid(2), p).await;
    let before = counts(s).await;
    let mut create_a = live("refs/heads/a", None, oid(3), p);
    create_a.commits.push(NewCommit {
        commit: oid(3),
        parents: vec![],
        committer_time: 1,
        generation: None,
        root_tree: None,
    });
    let outcome = commit(
        s,
        repo,
        vec![
            create_a.clone(),
            live("refs/heads/z", Some(z1.id), oid(4), p),
        ],
    )
    .await;
    assert_eq!(
        outcome,
        CommitOutcome::CasLost {
            name: rn("refs/heads/z"),
            current: Some(z2.id),
        }
    );
    assert_eq!(counts(s).await, before, "the create of a was rolled back");
    assert_eq!(
        s.current_version(repo, &rn("refs/heads/a")).await.unwrap(),
        None
    );

    let mut update_z = live("refs/heads/z", Some(z2.id), oid(5), p);
    update_z.commits.push(NewCommit {
        commit: oid(5),
        parents: vec![oid(2)],
        committer_time: 2,
        generation: None,
        root_tree: None,
    });
    update_z.jobs.push(VersionJob {
        kind: VersionJobKind::IndexCommits,
        run_after: UnixMicros(0),
    });
    create_a.jobs.push(VersionJob {
        kind: VersionJobKind::MirrorPush,
        run_after: UnixMicros(0),
    });
    let v = committed(s, repo, vec![update_z, create_a]).await;
    assert_eq!(
        v.iter().map(|v| v.name.clone()).collect::<Vec<_>>(),
        vec![rn("refs/heads/z"), rn("refs/heads/a")]
    );
    let looked = s
        .lookup_commits(repo, &SortedIds::from_unsorted(vec![oid(3), oid(5)]))
        .await
        .unwrap();
    let version_of = |l: &CommitLookup| match l {
        CommitLookup::Indexed(r) => r.ref_version,
        CommitLookup::NotIndexed => panic!("not indexed"),
    };
    assert_eq!(version_of(&looked[0]), v[1].id, "oid 3 is a's tip");
    assert_eq!(version_of(&looked[1]), v[0].id, "oid 5 is z's tip");
    let index = s
        .claim_job(&[JobTag::IndexCommits], "w", Duration::from_secs(1))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(index.kind, JobKind::IndexCommits { version: v[0].id });
    let mirror = s
        .claim_job(&[JobTag::MirrorPush], "w", Duration::from_secs(1))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(mirror.kind, JobKind::MirrorPush { version: v[1].id });
}

/// A push that names another ref's version as its expectation, or a
/// malformed push (here two writes for one ref), is refused as a
/// `Conflict` and writes nothing: the store never lets a ref's chain
/// branch or jump to another ref.
async fn the_database_refuses_a_version_that_is_not_the_expected_child(c: Ctx) {
    let s = &c.store;
    let repo = new_repo(s, "acme/r").await.id;
    let p = pack(s, repo, 1).await;
    let other = advance(s, repo, "refs/heads/other", oid(1), p).await;
    advance(s, repo, MAIN, oid(2), p).await;
    let before = counts(s).await;
    assert!(is_conflict(
        s.commit_push(&PushCommit {
            repo,
            writes: vec![live(MAIN, Some(other.id), oid(3), p)],
        })
        .await
    ));
    assert!(is_conflict(
        s.commit_push(&PushCommit {
            repo,
            writes: vec![live(MAIN, None, oid(3), p), live(MAIN, None, oid(4), p)],
        })
        .await
    ));
    // A version from another repository.
    let repo2 = new_repo(s, "acme/r2").await.id;
    let p2 = pack(s, repo2, 1).await;
    assert!(is_conflict(
        s.commit_push(&PushCommit {
            repo: repo2,
            writes: vec![live(MAIN, Some(other.id), oid(3), p2)],
        })
        .await
    ));
    assert_eq!(counts(s).await, before);
}

// ---------------------------------------------------------------------------
// Commit index

/// Lookups answer one entry per ID in the (sorted) order asked, `NotIndexed`
/// for a missing row; an existing row is never overwritten, and parents
/// round-trip in commit order. Large batches cross the store's chunking.
async fn commit_rows_resolve_in_order_and_are_never_overwritten(c: Ctx) {
    let s = &c.store;
    let repo = new_repo(s, "acme/r").await.id;
    let p = pack(s, repo, 1).await;
    let mut w = live(MAIN, None, oid(11), p);
    w.commits = vec![
        NewCommit {
            commit: oid(11),
            parents: vec![oid(10), oid(9)],
            committer_time: 1_700_000_000,
            generation: None,
            root_tree: None,
        },
        NewCommit {
            commit: oid(10),
            parents: vec![],
            committer_time: 1_600_000_000,
            generation: None,
            root_tree: None,
        },
    ];
    let v1 = committed(s, repo, vec![w]).await.remove(0);
    let v2 = advance(s, repo, MAIN, oid(12), p).await;
    s.index_commits(
        repo,
        &[
            CommitRow {
                commit: oid(11),
                ref_version: v2.id,
                parents: vec![],
                committer_time: 0,
                generation: None,
                root_tree: None,
            },
            CommitRow {
                commit: oid(12),
                ref_version: v2.id,
                parents: vec![oid(11)],
                committer_time: 5,
                generation: None,
                root_tree: None,
            },
        ],
    )
    .await
    .unwrap();
    let got = s
        .lookup_commits(
            repo,
            &SortedIds::from_unsorted(vec![oid(13), oid(12), oid(11), oid(10)]),
        )
        .await
        .unwrap();
    assert_eq!(
        got,
        vec![
            CommitLookup::Indexed(CommitRow {
                commit: oid(10),
                ref_version: v1.id,
                parents: vec![],
                committer_time: 1_600_000_000,
                generation: None,
                root_tree: None,
            }),
            CommitLookup::Indexed(CommitRow {
                commit: oid(11),
                ref_version: v1.id,
                parents: vec![oid(10), oid(9)],
                committer_time: 1_700_000_000,
                generation: None,
                root_tree: None,
            }),
            CommitLookup::Indexed(CommitRow {
                commit: oid(12),
                ref_version: v2.id,
                parents: vec![oid(11)],
                committer_time: 5,
                generation: None,
                root_tree: None,
            }),
            CommitLookup::NotIndexed,
        ]
    );
    // Another repository does not see these rows.
    let repo2 = new_repo(s, "acme/r2").await.id;
    assert_eq!(
        s.lookup_commits(repo2, &SortedIds::from_unsorted(vec![oid(10)]))
            .await
            .unwrap(),
        vec![CommitLookup::NotIndexed]
    );
    // A big batch, as the indexing job writes.
    let rows: Vec<CommitRow> = (0..1200)
        .map(|n| CommitRow {
            commit: oid_n(n),
            ref_version: v2.id,
            parents: vec![oid_n(n + 1)],
            committer_time: i64::from(n),
            generation: None,
            root_tree: None,
        })
        .collect();
    s.index_commits(repo, &rows).await.unwrap();
    let ids = SortedIds::from_unsorted((0..1300).map(oid_n).collect());
    let got = s.lookup_commits(repo, &ids).await.unwrap();
    assert_eq!(got.len(), 1300);
    for (id, l) in ids.as_slice().iter().zip(&got) {
        let n = u32::from_be_bytes(id.as_bytes()[..4].try_into().unwrap());
        match l {
            CommitLookup::Indexed(r) => {
                assert!(n < 1200);
                assert_eq!((r.commit, r.parents.clone()), (*id, vec![oid_n(n + 1)]));
            }
            CommitLookup::NotIndexed => assert!(n >= 1200),
        }
    }
}

/// Migration 0011 (decisions log, "12c: generation numbers"): a row's
/// generation and root tree round-trip through a push and through the
/// indexing job's insert; `unexamined_commits` pages through exactly the
/// rows without one that wait on nothing, in (version, ID) order, per
/// repository; `set_generation_waits_on` takes a row out of those pages
/// and into `commits_waiting_on`'s answer for what it waits on, and never
/// marks a row that has a generation; and `set_generations` fills only
/// rows that have none (clearing what they waited on), so a value once
/// written is never changed (generations are derived, so a second writer
/// computes the same value anyway).
async fn commit_generations_round_trip_page_and_are_never_overwritten(c: Ctx) {
    let s = &c.store;
    let repo = new_repo(s, "acme/gen").await.id;
    let other = new_repo(s, "acme/gen2").await.id;
    let p = pack(s, repo, 1).await;
    let mut w = live(MAIN, None, oid(21), p);
    w.commits = vec![
        NewCommit {
            commit: oid(21),
            parents: vec![oid(20)],
            committer_time: 7,
            generation: Some(2),
            root_tree: Some(oid(99)),
        },
        NewCommit {
            commit: oid(20),
            parents: vec![],
            committer_time: 6,
            generation: None,
            root_tree: None,
        },
    ];
    let v = committed(s, repo, vec![w]).await.remove(0);
    let rows: Vec<CommitRow> = (0..1500)
        .map(|n| CommitRow {
            commit: oid_n(1000 + n),
            ref_version: v.id,
            parents: vec![],
            committer_time: 1,
            generation: (n % 2 == 0).then_some(1),
            root_tree: Some(oid_n(9000 + n)),
        })
        .collect();
    s.index_commits(repo, &rows).await.unwrap();
    let got = s
        .lookup_commits(repo, &SortedIds::from_unsorted(vec![oid(21), oid_n(1001)]))
        .await
        .unwrap();
    let rows_of = |l: &[CommitLookup]| -> Vec<(Option<u64>, Option<ObjectId>)> {
        l.iter()
            .map(|l| match l {
                CommitLookup::Indexed(r) => (r.generation, r.root_tree),
                CommitLookup::NotIndexed => panic!("indexed"),
            })
            .collect()
    };
    let mut want = vec![(Some(2), Some(oid(99))), (None, Some(oid_n(9001)))];
    if oid(21) > oid_n(1001) {
        want.reverse();
    }
    assert_eq!(rows_of(&got), want);

    // Pages: oid(20) and the 750 odd rows, in (version, ID) order (all
    // name the push's version, so by ID). Nothing of the other
    // repository's.
    let pages = |limit: usize| async move {
        let mut seen = Vec::new();
        let mut after = None;
        loop {
            let page = s.unexamined_commits(repo, after, limit).await.unwrap();
            assert!(page.len() <= limit);
            let Some(last) = page.last() else { break };
            after = Some((last.ref_version, last.commit));
            assert!(page.iter().all(|r| r.generation.is_none()));
            seen.extend(page.iter().map(|r| (r.ref_version, r.commit)));
        }
        seen
    };
    let seen = pages(100).await;
    assert_eq!(seen.len(), 751);
    assert!(seen.windows(2).all(|w| w[0] < w[1]), "(version, ID) order");
    assert!(seen.contains(&(v.id, oid(20))));
    assert!(
        s.unexamined_commits(other, None, 10)
            .await
            .unwrap()
            .is_empty()
    );
    let seen: Vec<ObjectId> = seen.into_iter().map(|(_, c)| c).collect();

    // Waits: the first 600 unexamined rows wait on 21 or 20 by parity; a
    // wait recorded for 21, which has a generation, is ignored. They leave
    // the unexamined pages and are listed as waiting, up to a limit.
    let waits: Vec<(ObjectId, ObjectId)> = seen[..600]
        .iter()
        .enumerate()
        .map(|(i, id)| (*id, if i % 2 == 0 { oid(21) } else { oid(20) }))
        .chain([(oid(21), oid(20))])
        .collect();
    s.set_generation_waits_on(repo, &waits).await.unwrap();
    s.set_generation_waits_on(repo, &[]).await.unwrap();
    assert_eq!(pages(1000).await.len(), 151);
    let on = |ids: Vec<ObjectId>, limit: usize| async move {
        let mut got: Vec<ObjectId> = s
            .commits_waiting_on(repo, &SortedIds::from_unsorted(ids), limit)
            .await
            .unwrap()
            .into_iter()
            .map(|r| r.commit)
            .collect();
        got.sort();
        got
    };
    let mut want_20: Vec<ObjectId> = seen[..600].iter().skip(1).step_by(2).copied().collect();
    want_20.sort();
    assert_eq!(on(vec![oid(20)], 1000).await, want_20);
    assert_eq!(on(vec![oid(20), oid(21)], 1000).await.len(), 600);
    assert_eq!(on(vec![oid(20), oid(21)], 250).await.len(), 250);
    assert!(on(vec![oid(22)], 1000).await.is_empty());
    assert!(
        s.commits_waiting_on(other, &SortedIds::from_unsorted(vec![oid(20)]), 10)
            .await
            .unwrap()
            .is_empty()
    );

    // Fill: odd rows get 5, and an attempt to change 21's 2 (and an even
    // row's 1) is ignored. Filled rows wait on nothing any more.
    let mut set: Vec<(ObjectId, u64)> = seen.iter().map(|id| (*id, 5)).collect();
    set.push((oid(21), 9));
    set.push((oid_n(1000), 9));
    s.set_generations(repo, &set).await.unwrap();
    s.set_generations(repo, &[]).await.unwrap();
    assert!(pages(10).await.is_empty());
    assert!(on(vec![oid(20), oid(21)], 1000).await.is_empty());
    let got = s
        .lookup_commits(
            repo,
            &SortedIds::from_unsorted(vec![oid(21), oid(20), oid_n(1000), oid_n(1001)]),
        )
        .await
        .unwrap();
    for l in got {
        let CommitLookup::Indexed(r) = l else {
            panic!("indexed")
        };
        let want = if r.commit == oid(21) {
            2
        } else if r.commit == oid_n(1000) {
            1
        } else {
            5
        };
        assert_eq!(r.generation, Some(want), "{}", r.commit);
    }
}

// ---------------------------------------------------------------------------
// Packs

/// Registering the same `(repo, checksum)` again returns the same ID, even
/// with different `received_bytes`; different sizes or origin are a
/// `Conflict`. Rows read back exactly, in the order asked; a missing ID is
/// `NotFound`, and a row whose stored blob key is not the derived one is
/// `Corrupt`.
async fn packs_register_idempotently_and_read_back_exactly(c: Ctx) {
    let s = &c.store;
    let repo = new_repo(s, "acme/r").await.id;
    let a = push_pack(repo, 1);
    let id = s.register_pack(&a).await.unwrap().id;
    assert_eq!(s.register_pack(&a).await.unwrap().id, id);
    let resent = NewPack {
        origin: PackOrigin::Push {
            received_bytes: 999,
        },
        ..a.clone()
    };
    assert_eq!(s.register_pack(&resent).await.unwrap().id, id);
    assert!(is_conflict(
        s.register_pack(&NewPack {
            size: 101,
            ..a.clone()
        })
        .await
    ));
    let l1 = CoverLevel::new(1).unwrap();
    assert!(is_conflict(
        s.register_pack(&NewPack {
            origin: PackOrigin::Compaction { level: l1 },
            ..a.clone()
        })
        .await
    ));
    assert!(
        is_conflict(
            s.register_pack(&NewPack {
                object_count: 0,
                ..push_pack(repo, 9)
            })
            .await
        ),
        "a row NewPack::validate refuses is never written"
    );

    let b = NewPack {
        checksum: PackChecksum::from_bytes([2; 20]),
        object_count: 3,
        ref_delta_count: 1,
        size: 5000,
        idx_size: 1072 + 3 * 28 + 8,
        entries_size: entry_table_len(3),
        origin: PackOrigin::Compaction {
            level: CoverLevel::new(3).unwrap(),
        },
        ..a.clone()
    };
    let bid = s.register_pack(&b).await.unwrap().id;
    let got = s.packs(&[bid, id, bid]).await.unwrap();
    assert_eq!(
        got.iter().map(|r| r.id).collect::<Vec<_>>(),
        vec![bid, id, bid]
    );
    assert_eq!(got[0].pack, b);
    assert_eq!(got[0].pack.level().get(), 3);
    assert_eq!(
        got[1].pack, a,
        "the first registration's received_bytes stays"
    );
    assert!(matches!(
        s.packs(&[id, PackId::new(9999).unwrap()]).await,
        Err(StoreError::NotFound { .. })
    ));
    // The same bytes in another repository are another pack.
    let repo2 = new_repo(s, "acme/r2").await.id;
    let other = s.register_pack(&push_pack(repo2, 1)).await.unwrap().id;
    assert_ne!(other, id);
    assert_eq!(
        s.packs(&[other]).await.unwrap()[0].keys().pack.as_str(),
        format!(
            "repos/{repo2}/packs/{}.pack",
            PackChecksum::from_bytes([1; 20])
        )
    );

    sqlx::query("UPDATE packs SET pack_key = 'repos/1/packs/elsewhere.pack' WHERE id = $1")
        .bind(id.get())
        .execute(s.pool())
        .await
        .unwrap();
    assert!(matches!(
        s.packs(&[id]).await,
        Err(StoreError::Corrupt { .. })
    ));
}

// ---------------------------------------------------------------------------
// Listing and lookups

fn all_refs() -> RefListQuery {
    RefListQuery {
        prefixes: vec![],
        after: None,
        limit: 1000,
    }
}

/// `ls-refs`: live refs only, in bytewise order (`B` before `a`), filtered
/// by byte prefix (case-sensitive, also on SQLite whose `LIKE` is not),
/// paginated by `after` and `limit`.
async fn list_refs_filters_by_prefix_bytewise_and_paginates(c: Ctx) {
    let s = &c.store;
    let repo = new_repo(s, "acme/r").await.id;
    let p = pack(s, repo, 1).await;
    let names = [
        MAIN,
        "refs/heads/a",
        "refs/heads/B",
        "refs/heads/é",
        "refs/heads/main2",
        "refs/tags/v1",
        "refs/heads/gone",
    ];
    for (i, n) in names.iter().enumerate() {
        advance(s, repo, n, oid(i as u8 + 1), p).await;
    }
    let gone = s
        .current_version(repo, &rn("refs/heads/gone"))
        .await
        .unwrap()
        .unwrap();
    committed(s, repo, vec![delete("refs/heads/gone", gone.id)]).await;
    let other = new_repo(s, "acme/r2").await.id;
    let p2 = pack(s, other, 1).await;
    advance(s, other, "refs/heads/elsewhere", oid(1), p2).await;

    let list = |prefixes: &[&str], after: Option<&str>, limit: u32| {
        let q = RefListQuery {
            prefixes: prefixes.iter().map(|p| p.to_string()).collect(),
            after: after.map(rn),
            limit,
        };
        async move {
            s.list_refs(repo, &q)
                .await
                .unwrap()
                .into_iter()
                .map(|v| v.name.as_str().to_owned())
                .collect::<Vec<_>>()
        }
    };
    let everything = vec![
        "refs/heads/B",
        "refs/heads/a",
        "refs/heads/main",
        "refs/heads/main2",
        "refs/heads/é",
        "refs/tags/v1",
    ];
    assert_eq!(list(&[], None, 100).await, everything);
    assert_eq!(list(&[""], None, 100).await, everything);
    assert_eq!(list(&["refs/tags/"], None, 100).await, vec!["refs/tags/v1"]);
    assert_eq!(
        list(&["refs/heads/main", "refs/tags/"], None, 100).await,
        vec!["refs/heads/main", "refs/heads/main2", "refs/tags/v1"]
    );
    assert_eq!(
        list(&["refs/heads/b"], None, 100).await,
        Vec::<String>::new()
    );
    assert_eq!(
        list(&["refs/heads/é"], None, 100).await,
        vec!["refs/heads/é"]
    );
    assert_eq!(
        list(&["refs/heads/gone"], None, 100).await,
        Vec::<String>::new()
    );
    assert_eq!(list(&[], None, 2).await, everything[..2]);
    assert_eq!(list(&[], Some("refs/heads/a"), 2).await, everything[2..4]);
    assert_eq!(list(&[], Some("refs/heads/é"), 2).await, everything[5..]);
    assert_eq!(
        list(&["refs/heads/"], Some("refs/heads/main"), 100).await,
        vec!["refs/heads/main2", "refs/heads/é"]
    );
    assert_eq!(list(&[], None, 0).await, Vec::<String>::new());
}

/// A listing's cost does not grow with prefixes a client repeats or that a
/// shorter prefix already covers, and no number of prefixes exceeds a
/// bind-parameter limit (Postgres 65,535, SQLite 32,766): each prefix was
/// two bound values, so 40,000 prefixes failed as a storage error on both
/// dialects (#119). Pinned at `ls-refs`'s cap (1,024 prefixes) and far
/// past it, paged with a limit that ends pages mid-way through the
/// prefix list: every page equals a plain filter over the full listing.
/// `ref_changes` as a map from ref to tip (`None` for a deletion),
/// asserting the answer is complete.
async fn changes(
    s: &SqlStore,
    repo: RepoId,
    since: Option<&RefCursor>,
) -> (
    std::collections::BTreeMap<String, Option<ObjectId>>,
    RefCursor,
) {
    let RefChanges {
        cursor,
        refs,
        complete,
    } = s
        .ref_changes(repo, since, 1000)
        .await
        .unwrap()
        .expect("SqlStore answers ref_changes");
    assert!(complete);
    let map = refs
        .into_iter()
        .map(|r| (r.name.as_str().to_owned(), r.tip))
        .collect();
    (map, cursor)
}

/// Invariant: a read from no cursor is every live ref of the repository
/// and nothing else; a read from a cursor is exactly the refs whose
/// current version the earlier read did not see, each once at its current
/// tip (a deletion with none), however many times it moved, and nothing
/// of another repository. Nothing new is an empty answer. More than
/// `limit` refs is an incomplete answer of at most `limit`.
async fn ref_changes_since_a_cursor_are_exactly_the_refs_that_moved(c: Ctx) {
    let s = &c.store;
    let repo = new_repo(s, "acme/r").await.id;
    let other = new_repo(s, "acme/other").await.id;
    let p = pack(s, repo, 1).await;
    let q = pack(s, other, 2).await;
    advance(s, repo, "refs/heads/a", oid(1), p).await;
    advance(s, repo, "refs/heads/b", oid(2), p).await;
    let gone = advance(s, repo, "refs/heads/gone", oid(3), p).await;
    committed(s, repo, vec![delete("refs/heads/gone", gone.id)]).await;
    advance(s, other, "refs/heads/a", oid(9), q).await;

    let (all, c0) = changes(s, repo, None).await;
    assert_eq!(
        all,
        [
            ("refs/heads/a".to_owned(), Some(oid(1))),
            ("refs/heads/b".to_owned(), Some(oid(2))),
        ]
        .into()
    );
    let (none, c0b) = changes(s, repo, Some(&c0)).await;
    assert!(none.is_empty(), "{none:?}");

    advance(s, repo, "refs/heads/a", oid(4), p).await;
    advance(s, repo, "refs/heads/a", oid(5), p).await;
    let b = s
        .current_version(repo, &rn("refs/heads/b"))
        .await
        .unwrap()
        .unwrap();
    committed(s, repo, vec![delete("refs/heads/b", b.id)]).await;
    advance(s, repo, "refs/heads/d", oid(6), p).await;
    advance(s, other, "refs/heads/z", oid(7), q).await;
    let (moved, c1) = changes(s, repo, Some(&c0b)).await;
    assert_eq!(
        moved,
        [
            ("refs/heads/a".to_owned(), Some(oid(5))),
            ("refs/heads/b".to_owned(), None),
            ("refs/heads/d".to_owned(), Some(oid(6))),
        ]
        .into()
    );
    // The same cursor read again gives the same answer; the new one, none.
    assert_eq!(changes(s, repo, Some(&c0b)).await.0, moved);
    assert!(changes(s, repo, Some(&c1)).await.0.is_empty());

    for (i, name) in ["refs/heads/x", "refs/heads/y", "refs/heads/w"]
        .into_iter()
        .enumerate()
    {
        advance(s, repo, name, oid(20 + u8::try_from(i).unwrap()), p).await;
    }
    let capped = s.ref_changes(repo, Some(&c1), 2).await.unwrap().unwrap();
    assert!(!capped.complete);
    assert_eq!(capped.refs.len(), 2);
    let exact = s.ref_changes(repo, Some(&c1), 3).await.unwrap().unwrap();
    assert!(exact.complete);
    assert_eq!(exact.refs.len(), 3);
    let full = s.ref_changes(repo, None, 4).await.unwrap().unwrap();
    assert!(!full.complete, "a, d, x, y, w are live");
    assert_eq!(full.refs.len(), 4);
}

/// Invariant: a version written by a transaction still open when a reader
/// read is in the reader's next answer once it commits, even when a
/// version with a later ID committed first (Postgres; SQLite runs one
/// write at a time, so there the open write just commits later). A cursor
/// of "the largest version ID seen" would miss it for good.
async fn ref_changes_find_a_write_that_commits_after_a_later_one(c: Ctx) {
    let s = &c.store;
    let repo = new_repo(s, "acme/r").await.id;
    let p = pack(s, repo, 1).await;
    advance(s, repo, MAIN, oid(1), p).await;
    let (_, c0) = changes(s, repo, None).await;

    // An open push of a new ref: its version ID is taken now.
    let mut slow = raw_write_tx(s).await;
    let slow_id: i64 = sqlx::query(
        "INSERT INTO ref_versions (repo_id, ref_name, seq, tip_oid, closure_exact, pack_id) \
         VALUES ($1, $2, 1, $3, 1, $4) RETURNING id",
    )
    .bind(repo.get())
    .bind("refs/heads/slow")
    .bind(oid(2).as_bytes().to_vec())
    .bind(p.get())
    .fetch_one(&mut *slow)
    .await
    .unwrap()
    .get("id");
    sqlx::query("INSERT INTO refs (repo_id, ref_name, version_id, seq) VALUES ($1, $2, $3, 1)")
        .bind(repo.get())
        .bind("refs/heads/slow")
        .bind(slow_id)
        .execute(&mut *slow)
        .await
        .unwrap();

    let mut expect_first = std::collections::BTreeMap::new();
    if s.dialect() == Dialect::Postgres {
        // A later push commits first, with a larger version ID.
        let fast = advance(s, repo, "refs/heads/fast", oid(3), p).await;
        assert!(fast.id.get() > slow_id);
        expect_first.insert("refs/heads/fast".to_owned(), Some(oid(3)));
    }
    let (first, c1) = changes(s, repo, Some(&c0)).await;
    assert_eq!(first, expect_first, "the open write is not visible yet");

    slow.commit().await.unwrap();
    let (second, c2) = changes(s, repo, Some(&c1)).await;
    assert_eq!(
        second,
        [("refs/heads/slow".to_owned(), Some(oid(2)))].into(),
        "the late commit is found from the cursor read before it"
    );
    assert!(changes(s, repo, Some(&c2)).await.0.is_empty());
}

async fn list_refs_takes_any_number_of_prefixes(c: Ctx) {
    let s = &c.store;
    let repo = new_repo(s, "acme/r").await.id;
    let p = pack(s, repo, 1).await;
    let b = |j: usize| format!("refs/heads/b{j:05}");
    let mut names: BTreeSet<String> = (0..60).map(|i| b(i * 661)).collect();
    for j in [1, 2, 500, 1023, 1024, 1500, 39_999, 40_000] {
        names.insert(b(j));
    }
    names.insert(format!("{}/x", b(7)));
    names.insert("refs/heads/c".into());
    names.insert("refs/tags/v1".into());
    for (i, n) in names.iter().enumerate() {
        advance(s, repo, n, oid_n(i as u32 + 1), p).await;
    }

    // Every page of `prefixes`, `limit` refs at a time, as `ls-refs` reads.
    let paged = |prefixes: Vec<String>, limit: u32| async move {
        let mut out = Vec::new();
        let mut after = None;
        loop {
            let q = RefListQuery {
                prefixes: prefixes.clone(),
                after: after.take(),
                limit,
            };
            let page = s
                .list_refs(repo, &q)
                .await
                .unwrap_or_else(|e| panic!("{} prefixes: {e}", prefixes.len()));
            assert!(page.len() <= limit as usize);
            let full = page.len() == limit as usize;
            after = page.last().map(|v| v.name.clone());
            out.extend(page.into_iter().map(|v| v.name.as_str().to_owned()));
            if !full {
                return out;
            }
        }
    };
    let filtered = |prefixes: &[String]| -> Vec<String> {
        names
            .iter()
            .filter(|n| prefixes.iter().any(|p| n.starts_with(p.as_str())))
            .cloned()
            .collect()
    };

    // At the cap, in reverse order, with every name listed once and one
    // ref (`b00007/x`) matched by a prefix of its name.
    let at_cap: Vec<String> = (0..1024).rev().map(b).collect();
    let want = filtered(&at_cap);
    assert_eq!(want.len(), 7, "{want:?}");
    assert_eq!(paged(at_cap.clone(), 1000).await, want);
    assert_eq!(paged(at_cap, 4).await, want);

    // 40,000 distinct prefixes, more than one query's worth of ranges.
    let distinct: Vec<String> = (0..40_000).map(b).collect();
    let want = filtered(&distinct);
    assert_eq!(want.len(), names.len() - 3, "{want:?}");
    assert_eq!(paged(distinct.clone(), 1000).await, want);
    assert_eq!(paged(distinct.clone(), 7).await, want);

    // The same again, each twice, plus prefixes that cover them: the
    // listing is what the covering prefixes alone list.
    let mut covered = distinct.clone();
    covered.extend(distinct);
    covered.push("refs/heads/".into());
    covered.push("refs/tags/".into());
    covered.push("refs/tags/v".into());
    let everything: Vec<String> = names.iter().cloned().collect();
    assert_eq!(paged(covered.clone(), 1000).await, everything);
    assert_eq!(paged(covered.clone(), 7).await, everything);

    // An empty prefix still matches every ref, whatever else is sent.
    covered.push(String::new());
    assert_eq!(paged(covered, 1000).await, everything);
}

/// The `.have` tips of a push advertisement are one lookup: the versions
/// at the seqs asked (`have_tip_seqs`), in that order, with missing ones
/// left out.
async fn versions_by_seq_serves_have_tips(c: Ctx) {
    let s = &c.store;
    let repo = new_repo(s, "acme/r").await.id;
    let p = pack(s, repo, 1).await;
    let mut versions = Vec::new();
    for n in 1..=10 {
        versions.push(advance(s, repo, MAIN, oid(n), p).await);
    }
    advance(s, repo, "refs/heads/other", oid(50), p).await;
    let seq = |n| Seq::new(n).unwrap();
    let wanted = [seq(9), seq(8), seq(6), seq(2)];
    let got = s.versions_by_seq(repo, &rn(MAIN), &wanted).await.unwrap();
    assert_eq!(
        got.iter().map(|v| v.seq.get()).collect::<Vec<_>>(),
        vec![9, 8, 6, 2]
    );
    assert_eq!(got[0], versions[8]);
    let got = s
        .versions_by_seq(repo, &rn(MAIN), &[seq(3), seq(99), seq(1)])
        .await
        .unwrap();
    assert_eq!(got, vec![versions[2].clone(), versions[0].clone()]);
    assert!(
        s.versions_by_seq(repo, &rn(MAIN), &[])
            .await
            .unwrap()
            .is_empty()
    );
}

/// `current_versions` answers every name in the order asked, `None` for a
/// name with no `refs` row, deletions included, in one statement, with each
/// tip's generation from this repository's `commit_index` row (`None` for a
/// deletion, a tip without a row, or a row without a generation).
async fn current_versions_reads_many_refs_in_one_statement(c: Ctx) {
    let s = &c.store;
    let repo = new_repo(s, "acme/r").await.id;
    let other = new_repo(s, "acme/o").await.id;
    let p = pack(s, repo, 1).await;
    let q = pack(s, other, 1).await;
    let main = advance(s, repo, MAIN, oid(1), p).await;
    let main = advance(s, repo, MAIN, oid(2), p).await.id.max(main.id);
    let b = advance(s, repo, "refs/heads/b", oid(3), p).await;
    let gone = committed(s, repo, vec![delete("refs/heads/b", b.id)])
        .await
        .remove(0);
    let x = advance(s, other, "refs/heads/x", oid(4), q).await;
    let no_generation = advance(s, repo, "refs/heads/c", oid(5), p).await;
    let no_row = advance(s, repo, "refs/heads/d", oid(6), p).await;
    let row = |commit, ref_version, generation| CommitRow {
        commit,
        ref_version,
        parents: vec![],
        committer_time: 1,
        generation,
        root_tree: None,
    };
    s.index_commits(
        repo,
        &[
            row(oid(2), main, Some(7)),
            row(oid(5), no_generation.id, None),
        ],
    )
    .await
    .unwrap();
    // The same commit in another repository: not this one's row.
    s.index_commits(other, &[row(oid(2), x.id, Some(99))])
        .await
        .unwrap();
    let names = [
        rn("refs/heads/x"),
        rn(MAIN),
        rn("refs/heads/b"),
        rn(MAIN),
        rn("refs/heads/c"),
        rn("refs/heads/d"),
    ];
    let before = s.statements();
    let got = s.current_versions(repo, &names).await.unwrap();
    assert_eq!((s.statements() - before).queries, 1, "one statement");
    let ids: Vec<_> = got
        .iter()
        .map(|v| v.as_ref().map(|v| v.version.id))
        .collect();
    assert_eq!(
        ids,
        vec![
            None,
            Some(main),
            Some(gone.id),
            Some(main),
            Some(no_generation.id),
            Some(no_row.id)
        ]
    );
    let generations: Vec<_> = got
        .iter()
        .map(|v| v.as_ref().and_then(|v| v.tip_generation))
        .collect();
    assert_eq!(generations, vec![None, Some(7), None, Some(7), None, None]);
    for (n, v) in names.iter().zip(&got) {
        assert_eq!(
            v.as_ref().map(|v| v.version.clone()),
            s.current_version(repo, n).await.unwrap()
        );
    }
    assert!(s.current_versions(repo, &[]).await.unwrap().is_empty());
}

/// `current_and_recent` returns the current version then the versions
/// above `after` and at most `window` below it, newest first, in one
/// statement; `None` for a ref with no row.
async fn current_and_recent_reads_a_window_after_a_seq(c: Ctx) {
    let s = &c.store;
    let repo = new_repo(s, "acme/r").await.id;
    let p = pack(s, repo, 1).await;
    let mut versions = Vec::new();
    for n in 1..=10 {
        versions.push(advance(s, repo, MAIN, oid(n), p).await);
    }
    advance(s, repo, "refs/heads/other", oid(50), p).await;
    let seqs = |got: Option<Vec<RefVersion>>| -> Vec<u64> {
        got.unwrap().iter().map(|v| v.seq.get()).collect()
    };
    let before = s.statements();
    let all = s
        .current_and_recent(repo, &rn(MAIN), None, 4)
        .await
        .unwrap();
    assert_eq!((s.statements() - before).queries, 1, "one statement");
    assert_eq!(all.as_ref().unwrap()[0], versions[9]);
    assert_eq!(all.as_ref().unwrap()[1], versions[8]);
    assert_eq!(seqs(all), vec![10, 9, 8, 7, 6]);
    let seq = |n| Some(Seq::new(n).unwrap());
    let after = s
        .current_and_recent(repo, &rn(MAIN), seq(7), 4)
        .await
        .unwrap();
    assert_eq!(seqs(after), vec![10, 9, 8]);
    let caught_up = s
        .current_and_recent(repo, &rn(MAIN), seq(10), 128)
        .await
        .unwrap();
    assert_eq!(seqs(caught_up), vec![10]);
    let whole = s
        .current_and_recent(repo, &rn(MAIN), None, 128)
        .await
        .unwrap();
    assert_eq!(seqs(whole), (1..=10).rev().collect::<Vec<_>>());
    let none = s
        .current_and_recent(repo, &rn(MAIN), None, 0)
        .await
        .unwrap();
    assert_eq!(seqs(none), vec![10]);
    assert_eq!(
        s.current_and_recent(repo, &rn("refs/heads/nope"), None, 4)
            .await
            .unwrap(),
        None
    );
}

/// `reachable_versions` returns the versions asked for and those they
/// reach through trunk and scoped dependencies, not past a stop, and at
/// most `limit` besides the ones asked for, in one statement.
async fn reachable_versions_walks_dependencies_to_the_stops(c: Ctx) {
    let s = &c.store;
    let repo = new_repo(s, "acme/r").await.id;
    let p = pack(s, repo, 1).await;
    // Trunk t1..t6, each depending on the one before; a branch whose
    // versions depend on the one before (scoped) and on t3 (trunk).
    let mut trunk: Vec<RefVersion> = Vec::new();
    for n in 1..=6u8 {
        let mut w = live(MAIN, trunk.last().map(|v| v.id), oid(n), p);
        w.deps.trunk = trunk.last().map(|v| v.id);
        trunk.push(committed(s, repo, vec![w]).await.remove(0));
    }
    let mut branch: Vec<RefVersion> = Vec::new();
    for n in 20..=22u8 {
        let mut w = live("refs/heads/b", branch.last().map(|v| v.id), oid(n), p);
        w.deps.trunk = Some(trunk[2].id);
        w.deps.scoped_old = branch.last().map(|v| v.id);
        branch.push(committed(s, repo, vec![w]).await.remove(0));
    }
    let ids = |mut v: Vec<RefVersion>| {
        let mut ids: Vec<_> = v.drain(..).map(|v| v.id).collect();
        ids.sort();
        ids
    };
    let mut want: Vec<_> = trunk[..3].iter().chain(&branch).map(|v| v.id).collect();
    want.sort();
    let before = s.statements();
    let got = s
        .reachable_versions(&[branch[2].id], &[], 100)
        .await
        .unwrap();
    assert_eq!((s.statements() - before).queries, 1, "one statement");
    assert_eq!(ids(got.clone()), want);
    for v in &got {
        assert_eq!(Some(v.clone()), s.ref_version(v.id).await.unwrap());
    }
    // A stop is returned but not followed.
    let got = s
        .reachable_versions(&[branch[2].id], &[trunk[2].id, branch[0].id], 100)
        .await
        .unwrap();
    let mut want = vec![trunk[2].id, branch[0].id, branch[1].id, branch[2].id];
    want.sort();
    assert_eq!(ids(got), want);
    // The limit cuts what is reached, never what was asked for.
    let got = s
        .reachable_versions(&[trunk[5].id, branch[2].id], &[], 1)
        .await
        .unwrap();
    assert_eq!(got.len(), 3, "{got:?}");
    assert!(got.iter().any(|v| v.id == trunk[5].id));
    assert!(got.iter().any(|v| v.id == branch[2].id));
    let missing = RefVersionId::new(999_999).unwrap();
    assert!(
        s.reachable_versions(&[missing], &[], 10)
            .await
            .unwrap()
            .is_empty()
    );
}

/// What one branch push costs the store, counted by `statements`: on
/// Postgres registering its pack and committing its version (with a commit
/// row and a job) are one statement each and need no transaction; SQLite
/// keeps its transactions. The rows are what the transactional path writes
/// (`a_ref_history_is_one_chain_including_deletions` and friends check
/// them in detail).
async fn a_branch_push_commits_in_few_statements(c: Ctx) {
    let s = &c.store;
    let repo = new_repo(s, "acme/r").await.id;
    let base = pack(s, repo, 1).await;
    let trunk = advance(s, repo, MAIN, oid(1), base).await;
    let mut prev = None;
    for n in 2..=4u8 {
        let before = s.statements();
        let p = s.register_pack(&push_pack(repo, n)).await.unwrap();
        let registered = s.statements() - before;
        let mut w = live("refs/heads/agent", prev, oid(n), p.id);
        w.deps.trunk = Some(trunk.id);
        w.deps.scoped_old = prev;
        w.commits = vec![aspen_core::meta::NewCommit {
            commit: oid(n),
            parents: vec![oid(1)],
            committer_time: 10,
            generation: None,
            root_tree: None,
        }];
        w.jobs = vec![aspen_core::meta::VersionJob {
            kind: aspen_core::meta::VersionJobKind::Compact,
            run_after: UnixMicros(0),
        }];
        let before = s.statements();
        let v = committed(s, repo, vec![w]).await.remove(0);
        let wrote = s.statements() - before;
        match s.dialect() {
            Dialect::Postgres => {
                assert_eq!((registered.queries, registered.transaction_control), (1, 0));
                assert_eq!((wrote.queries, wrote.transaction_control), (1, 0));
            }
            Dialect::Sqlite => {
                assert_eq!(registered.transaction_control, 2, "BEGIN, COMMIT");
                assert_eq!(wrote.transaction_control, 2, "BEGIN, COMMIT");
            }
        }
        assert_eq!(v.deps.trunk, Some(trunk.id));
        assert_eq!(v.deps.scoped_old, prev);
        assert_eq!(v.value.pack(), Some(p.id));
        assert_eq!(
            s.current_version(repo, &rn("refs/heads/agent"))
                .await
                .unwrap(),
            Some(v.clone())
        );
        let rows = s
            .lookup_commits(
                repo,
                &aspen_core::ingest::SortedIds::from_unsorted(vec![oid(n)]),
            )
            .await
            .unwrap();
        assert!(
            matches!(&rows[0], aspen_core::meta::CommitLookup::Indexed(r) if r.ref_version == v.id),
            "{rows:?}"
        );
        prev = Some(v.id);
    }
    assert_eq!(count(s, "ref_versions").await, 4);
    assert_eq!(count(s, "jobs").await, 3);
    // Registering an existing pack again is one statement on Postgres too.
    let before = s.statements();
    s.register_pack(&push_pack(repo, 2)).await.unwrap();
    if s.dialect() == Dialect::Postgres {
        assert_eq!((s.statements() - before).total(), 1);
    }

    // Group commit: a batch of 4 branch pushes is one transaction, and on
    // Postgres each push in it is one statement, with no savepoint.
    let new: Vec<NewPack> = (10..14).map(|n| push_pack(repo, n)).collect();
    let before = s.statements();
    let packs: Vec<PackId> = s
        .register_packs(&new)
        .await
        .unwrap()
        .into_iter()
        .map(|r| r.unwrap().id)
        .collect();
    let registered = s.statements() - before;
    let pushes: Vec<PushCommit> = packs
        .iter()
        .enumerate()
        .map(|(i, p)| {
            let mut w = live(&format!("refs/heads/g{i}"), None, oid(30 + i as u8), *p);
            w.deps.trunk = Some(trunk.id);
            PushCommit {
                repo,
                writes: vec![w],
            }
        })
        .collect();
    let before = s.statements();
    let out = s.commit_pushes(&pushes).await.unwrap();
    let wrote = s.statements() - before;
    assert!(
        out.iter()
            .all(|o| matches!(o, Ok(CommitOutcome::Committed(_)))),
        "{out:?}"
    );
    if s.dialect() == Dialect::Postgres {
        assert_eq!((registered.queries, registered.transaction_control), (4, 2));
        assert_eq!((wrote.queries, wrote.transaction_control), (4, 2));
    }
}

/// On Postgres a registration whose insert waits for a sweep deleting the
/// same checksum's old row must take the next generation, although the
/// tombstone the sweep writes is not in the snapshot of the statement it
/// started. The sweep is played by hand: lock the row `FOR UPDATE`, delete
/// it and write its tombstone, and commit only after the registration has
/// started and is waiting. (SQLite runs one writer at a time.)
async fn a_registration_that_waits_for_a_sweep_takes_the_next_generation(c: Ctx) {
    let s = &c.store;
    if s.dialect() != Dialect::Postgres {
        return;
    }
    let repo = new_repo(s, "acme/r").await.id;
    let old = s.register_pack(&push_pack(repo, 1)).await.unwrap();
    assert_eq!(old.generation, PackGeneration::FIRST);
    let mut sweep = raw_write_tx(s).await;
    sqlx::query("SELECT id FROM packs WHERE id = $1 FOR UPDATE")
        .bind(old.id.get())
        .fetch_all(&mut *sweep)
        .await
        .unwrap();
    sqlx::query("DELETE FROM pack_touches WHERE pack_id = $1")
        .bind(old.id.get())
        .execute(&mut *sweep)
        .await
        .unwrap();
    sqlx::query("DELETE FROM packs WHERE id = $1")
        .bind(old.id.get())
        .execute(&mut *sweep)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO pack_tombstones (repo_id, checksum, generation, swept_at_us) \
         VALUES ($1, $2, 0, 1)",
    )
    .bind(repo.get())
    .bind(vec![1u8; 20])
    .execute(&mut *sweep)
    .await
    .unwrap();
    let s2 = s.clone();
    let register = tokio::spawn(async move { s2.register_pack(&push_pack(repo, 1)).await });
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert!(
        !register.is_finished(),
        "the registration waits for the sweep"
    );
    sweep.commit().await.unwrap();
    let again = register.await.unwrap().unwrap();
    assert_ne!(again.id, old.id);
    assert_eq!(again.generation, PackGeneration::new(1));
    assert_eq!(s.packs(&[again.id]).await.unwrap(), vec![again]);
}

/// `covers_at` returns exactly the covers of the ref at the level whose
/// range starts at one of the firsts asked, with their packs and chains:
/// not other levels, other refs or other repositories, and nothing for a
/// first with no cover (issue #149: a push reads only the covers it needs).
async fn covers_at_reads_only_the_covers_it_names(c: Ctx) {
    let s = &c.store;
    let repo = new_repo(s, "acme/r").await.id;
    let other = new_repo(s, "acme/other").await.id;
    let l1 = CoverLevel::new(1).unwrap();
    let compacted = s
        .register_pack(&NewPack {
            origin: PackOrigin::Compaction { level: l1 },
            ..push_pack(repo, 7)
        })
        .await
        .unwrap()
        .id;
    for (r, name, level, first, last, pack) in [
        (repo, MAIN, 1, 1, 2, Some(compacted.get())),
        (repo, MAIN, 1, 3, 4, None),
        (repo, MAIN, 1, 5, 6, None),
        (repo, MAIN, 2, 1, 4, None),
        (repo, "refs/heads/x", 1, 1, 2, None),
        (other, MAIN, 1, 1, 2, None),
    ] {
        sqlx::query(
            "INSERT INTO covers (repo_id, ref_name, level, first_seq, last_seq, pack_id) \
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(r.get())
        .bind(name)
        .bind(level as i64)
        .bind(first as i64)
        .bind(last as i64)
        .bind(pack)
        .execute(s.pool())
        .await
        .unwrap();
    }
    let seqs = |v: &[u64]| v.iter().map(|&n| Seq::new(n).unwrap()).collect::<Vec<_>>();
    let mut got = s
        .covers_at(repo, &rn(MAIN), l1, &seqs(&[1, 3, 9]))
        .await
        .unwrap();
    got.sort_by_key(|c| c.range.first());
    let all = s.covers(repo, &rn(MAIN)).await.unwrap();
    let want: Vec<Cover> = all
        .iter()
        .filter(|c| c.level == l1 && [1, 3].contains(&c.range.first().get()))
        .cloned()
        .collect();
    assert_eq!(got, want);
    assert_eq!(got[0].pack, Some(compacted));
    assert!(
        s.covers_at(repo, &rn(MAIN), l1, &[])
            .await
            .unwrap()
            .is_empty()
    );
    // More firsts than one statement binds.
    let many: Vec<u64> = (1..=2_000).collect();
    assert_eq!(
        s.covers_at(repo, &rn(MAIN), l1, &seqs(&many))
            .await
            .unwrap()
            .len(),
        3
    );
}

/// Covers of one ref come back ordered by level, then `first_seq`; other
/// refs' covers are left out.
async fn covers_are_ordered_by_level_then_first_seq(c: Ctx) {
    let s = &c.store;
    let repo = new_repo(s, "acme/r").await.id;
    let compacted = s
        .register_pack(&NewPack {
            origin: PackOrigin::Compaction {
                level: CoverLevel::new(1).unwrap(),
            },
            ..push_pack(repo, 7)
        })
        .await
        .unwrap()
        .id;
    for (name, level, first, last, pack) in [
        (MAIN, 2, 1, 4, None),
        (MAIN, 1, 5, 6, None),
        (MAIN, 1, 1, 2, Some(compacted.get())),
        ("refs/heads/x", 1, 1, 2, None),
    ] {
        sqlx::query(
            "INSERT INTO covers (repo_id, ref_name, level, first_seq, last_seq, pack_id) \
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(repo.get())
        .bind(name)
        .bind(level as i64)
        .bind(first as i64)
        .bind(last as i64)
        .bind(pack)
        .execute(s.pool())
        .await
        .unwrap();
    }
    let got = s.covers(repo, &rn(MAIN)).await.unwrap();
    assert_eq!(
        got.iter()
            .map(|c| (
                c.level.get(),
                c.range.first().get(),
                c.range.last().get(),
                c.pack
            ))
            .collect::<Vec<_>>(),
        vec![(1, 1, 2, Some(compacted)), (1, 5, 6, None), (2, 1, 4, None)]
    );
    assert!(got.iter().all(|c| c.name == rn(MAIN) && c.repo == repo));
}

// ---------------------------------------------------------------------------
// Jobs

async fn target_version(s: &SqlStore) -> (RepoId, RefVersion) {
    let repo = new_repo(s, "acme/jobs").await.id;
    let p = pack(s, repo, 1).await;
    (repo, advance(s, repo, MAIN, oid(1), p).await)
}

fn new_job(repo: RepoId, kind: JobKind, run_after: i64) -> NewJob {
    NewJob {
        repo,
        kind,
        run_after: UnixMicros(run_after),
    }
}

/// Compaction heat (`add_compaction_heat`) raises exactly the queued
/// `compact` jobs of the named ref at or after `from`, and a claim takes the
/// hottest due job first, then the oldest; jobs no fetch asked for keep
/// oldest-first order, and heat never overflows the column.
async fn heat_raises_the_queued_compact_jobs_fetches_wait_on(c: Ctx) {
    let s = &c.store;
    let repo = new_repo(s, "acme/heat").await.id;
    let other = new_repo(s, "acme/other").await.id;
    let p = pack(s, repo, 1).await;
    let q = pack(s, other, 2).await;
    let mut main = Vec::new();
    for i in 0..3u8 {
        main.push(advance(s, repo, MAIN, oid(10 + i), p).await);
    }
    let topic = advance(s, repo, "refs/heads/topic", oid(20), p).await;
    let elsewhere = advance(s, other, "refs/heads/topic", oid(30), q).await;
    // Oldest first: main v1 (run_after 100), main v2 (200), main v3 (300),
    // topic (400), the other repository's topic (500), and an index job on
    // topic (50) that heat never touches.
    let compact = |v: &RefVersion| JobKind::Compact { version: v.id };
    let ids = s
        .enqueue_jobs(&[
            new_job(repo, compact(&main[0]), 100),
            new_job(repo, compact(&main[1]), 200),
            new_job(repo, compact(&main[2]), 300),
            new_job(repo, compact(&topic), 400),
            new_job(other, compact(&elsewhere), 500),
            new_job(repo, JobKind::IndexCommits { version: topic.id }, 50),
        ])
        .await
        .unwrap();
    let heat = |repo, name: &str, from: u64, heat| CompactionHeat {
        repo,
        name: rn(name),
        from: Seq::new(from).unwrap(),
        heat,
    };
    // Fetches of topic in `repo` and of main from v2 on.
    let raised = s
        .add_compaction_heat(&[
            heat(repo, "refs/heads/topic", 1, 5),
            heat(repo, MAIN, 2, 3),
            heat(repo, "refs/heads/missing", 1, 9),
        ])
        .await
        .unwrap();
    assert_eq!(
        raised, 3,
        "topic, main v2 and main v3; not v1, not the other repository"
    );
    assert_eq!(s.add_compaction_heat(&[]).await.unwrap(), 0);
    c.set_time(1_000);
    let tags = [JobTag::Compact, JobTag::IndexCommits];
    let mut order = Vec::new();
    while let Some(job) = s
        .claim_job(&tags, "w", Duration::from_secs(60))
        .await
        .unwrap()
    {
        order.push(job.id);
    }
    assert_eq!(
        order,
        vec![ids[3], ids[1], ids[2], ids[5], ids[0], ids[4]],
        "hottest first (topic 5, main v2 and v3 3), then oldest due"
    );
    // A running job is not raised; the column saturates instead of
    // overflowing.
    assert_eq!(
        s.add_compaction_heat(&[heat(repo, "refs/heads/topic", 1, 1)])
            .await
            .unwrap(),
        0
    );
    let v4 = advance(s, repo, MAIN, oid(14), p).await;
    s.enqueue_jobs(&[new_job(repo, compact(&v4), 0)])
        .await
        .unwrap();
    for _ in 0..3 {
        s.add_compaction_heat(&[heat(repo, MAIN, 1, u64::MAX)])
            .await
            .unwrap();
    }
    let top: i64 = sqlx::query("SELECT MAX(priority) AS p FROM jobs")
        .fetch_one(s.pool())
        .await
        .unwrap()
        .get("p");
    assert_eq!(top, i64::try_from(MAX_JOB_HEAT).unwrap());
}

/// One job that is not done per kind and target: enqueueing again returns
/// the open (queued, running or dead) job's ID; once it is done, a new job
/// is created.
async fn enqueue_is_idempotent_per_kind_and_target(c: Ctx) {
    let s = &c.store;
    let (repo, v) = target_version(s).await;
    let p = pack(s, repo, 2).await;
    let index = JobKind::IndexCommits { version: v.id };
    let rewrite = JobKind::RewritePack { pack: p };
    let ids = s
        .enqueue_jobs(&[
            new_job(repo, index, 0),
            new_job(repo, index, 0),
            new_job(repo, JobKind::Compact { version: v.id }, 0),
            new_job(repo, rewrite, 0),
            new_job(repo, rewrite, 0),
        ])
        .await
        .unwrap();
    assert_eq!(ids[0], ids[1]);
    assert_eq!(ids[3], ids[4]);
    assert_eq!(ids.iter().collect::<BTreeSet<_>>().len(), 3);
    // Running keeps the slot.
    let j = s
        .claim_job(&[JobTag::IndexCommits], "w", Duration::from_secs(10))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(j.id, ids[0]);
    assert_eq!(
        s.enqueue_jobs(&[new_job(repo, index, 0)]).await.unwrap(),
        vec![ids[0]]
    );
    s.update_job(
        j.id,
        JobEvent::Complete {
            owner: "w".into(),
            attempt: 1,
        },
    )
    .await
    .unwrap();
    let again = s.enqueue_jobs(&[new_job(repo, index, 0)]).await.unwrap()[0];
    assert_ne!(again, ids[0], "a done job frees its slot");
    // Dead keeps the slot until an operator acts.
    sqlx::query(
        "UPDATE jobs SET state = 'dead', lease_owner = NULL, lease_expires_us = NULL \
         WHERE id = $1",
    )
    .bind(ids[3].get())
    .execute(s.pool())
    .await
    .unwrap();
    assert_eq!(
        s.enqueue_jobs(&[new_job(repo, rewrite, 0)]).await.unwrap(),
        vec![ids[3]]
    );
}

fn claim_ev(owner: &str, now: i64, lease: u64) -> JobEvent {
    JobEvent::Claim {
        owner: owner.into(),
        now: UnixMicros(now),
        lease: Duration::from_micros(lease),
    }
}

fn complete(owner: &str, attempt: u32) -> JobEvent {
    JobEvent::Complete {
        owner: owner.into(),
        attempt,
    }
}

/// Claims take the due job with the earliest due time (a queued job's
/// `run_after` or an expired lease's end, ties by ID) among the tags asked;
/// a live lease blocks other workers; renew, complete and fail are fenced
/// on `(worker, attempt)`, so neither another worker nor the same worker's
/// older attempt can finish a reclaimed job.
async fn claims_take_the_earliest_due_job_and_are_fenced(c: Ctx) {
    let s = &c.store;
    let (repo, v) = target_version(s).await;
    let lease = Duration::from_micros(100);
    c.set_time(1000);
    let ids = s
        .enqueue_jobs(&[
            new_job(repo, JobKind::Compact { version: v.id }, 2000),
            new_job(repo, JobKind::IndexCommits { version: v.id }, 500),
            new_job(repo, JobKind::MirrorPush { version: v.id }, 500),
        ])
        .await
        .unwrap();
    let all = JobTag::ALL;
    let j = s.claim_job(&all, "w1", lease).await.unwrap().unwrap();
    assert_eq!(j.id, ids[1], "earliest due, then lowest ID");
    assert_eq!(
        j.state,
        JobState::Running {
            owner: "w1".into(),
            lease_until: UnixMicros(1100)
        }
    );
    assert_eq!(j.attempts, 1);
    assert_eq!(
        s.claim_job(&[JobTag::Compact], "w2", lease).await.unwrap(),
        None,
        "not due yet"
    );
    assert_eq!(
        s.claim_job(&[JobTag::IndexCommits], "w2", lease)
            .await
            .unwrap(),
        None,
        "the lease is live"
    );
    assert_eq!(s.claim_job(&[], "w2", lease).await.unwrap(), None);

    // Fencing.
    assert!(is_conflict(s.update_job(j.id, complete("w2", 1)).await));
    assert!(is_conflict(s.update_job(j.id, complete("w1", 0)).await));
    assert!(is_conflict(
        s.update_job(j.id, claim_ev("w1", 1000, 100)).await
    ));
    assert!(matches!(
        s.update_job(JobId::new(9999).unwrap(), complete("w1", 1))
            .await,
        Err(StoreError::NotFound { .. })
    ));
    let renewed = s
        .update_job(
            j.id,
            JobEvent::Renew {
                owner: "w1".into(),
                attempt: 1,
                now: UnixMicros(1050),
                lease: Duration::from_micros(100),
            },
        )
        .await
        .unwrap();
    assert_eq!(
        renewed.state,
        JobState::Running {
            owner: "w1".into(),
            lease_until: UnixMicros(1150)
        }
    );
    c.set_time(1149);
    assert_eq!(
        s.claim_job(&[JobTag::IndexCommits], "w2", lease)
            .await
            .unwrap(),
        None
    );
    // The lease expires; w2 reclaims, and w1's late completion is refused.
    c.set_time(1150);
    let j2 = s
        .claim_job(&[JobTag::IndexCommits], "w2", lease)
        .await
        .unwrap()
        .unwrap();
    assert_eq!((j2.id, j2.attempts), (j.id, 2));
    assert!(is_conflict(s.update_job(j.id, complete("w1", 1)).await));
    // The same worker ID reclaiming does not let its older attempt finish.
    c.set_time(1250);
    let j3 = s
        .claim_job(&[JobTag::IndexCommits], "w2", lease)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(j3.attempts, 3);
    assert!(is_conflict(s.update_job(j.id, complete("w2", 2)).await));
    let done = s.update_job(j.id, complete("w2", 3)).await.unwrap();
    assert_eq!(done.state, JobState::Done);
    assert_eq!(s.job(j.id).await.unwrap(), Some(done));

    // Failure backs off.
    let m = s
        .claim_job(&[JobTag::MirrorPush], "w1", lease)
        .await
        .unwrap()
        .unwrap();
    let failed = s
        .update_job(
            m.id,
            JobEvent::Fail {
                owner: "w1".into(),
                attempt: 1,
                now: UnixMicros(1260),
                error: "upstream unreachable".into(),
                backoff: Duration::from_micros(40),
            },
        )
        .await
        .unwrap();
    assert_eq!(
        failed.state,
        JobState::Queued {
            run_after: UnixMicros(1300)
        }
    );
    assert_eq!(failed.last_error.as_deref(), Some("upstream unreachable"));
    assert_eq!(
        s.claim_job(&[JobTag::MirrorPush], "w1", lease)
            .await
            .unwrap(),
        None
    );

    // Expired running vs queued: the earlier due time wins.
    c.set_time(3000);
    let compact = s
        .claim_job(&[JobTag::Compact], "w1", lease)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(compact.id, ids[0]); // lease until 3100
    c.set_time(5000);
    // Mirror is due since 1300, compact's lease expired at 3100.
    let first = s.claim_job(&all, "w3", lease).await.unwrap().unwrap();
    assert_eq!(first.id, ids[2]);
    let second = s.claim_job(&all, "w3", lease).await.unwrap().unwrap();
    assert_eq!((second.id, second.attempts), (ids[0], 2));
}

/// Model-based check: a seeded random sequence of claims, renewals,
/// completions and failures (by two workers, with current and stale
/// attempts, and time jumps past leases) is applied to the store and to
/// `Job::apply`; after every step the stored job equals the oracle's, and
/// the store refuses exactly the events the oracle refuses. Finished jobs
/// are replaced (re-enqueued when done, requeued when dead).
async fn the_store_follows_the_job_state_machine(c: Ctx) {
    let s = &c.store;
    let (repo, v) = target_version(s).await;
    let kind = JobKind::IndexCommits { version: v.id };
    let tag = [JobTag::IndexCommits];
    let mut rng = SplitMix64(0x005e_eda3);
    let mut id = s
        .enqueue_jobs(&[new_job(repo, kind, c.now().0)])
        .await
        .unwrap()[0];
    let mut oracle = s.job(id).await.unwrap().unwrap();
    let (mut parked, mut done) = (0, 0);
    for step in 0..1000 {
        c.set_time(c.now().0 + (rng.next() % 80) as i64);
        let now = c.now();
        let owner = if rng.next().is_multiple_of(2) {
            "w1"
        } else {
            "w2"
        };
        let attempt = oracle
            .attempts
            .saturating_sub(u32::from(rng.next().is_multiple_of(3)));
        let event = match rng.next() % 10 {
            0..=3 => claim_ev(owner, now.0, 60),
            4 | 5 => JobEvent::Renew {
                owner: owner.into(),
                attempt,
                now,
                lease: Duration::from_micros(60),
            },
            6 => complete(owner, attempt),
            _ => JobEvent::Fail {
                owner: owner.into(),
                attempt,
                now,
                error: format!("step {step}"),
                backoff: Duration::from_micros(rng.next() % 50),
            },
        };
        let expected = oracle.apply(event.clone());
        if let JobEvent::Claim { owner, lease, .. } = &event {
            let got = s.claim_job(&tag, owner, *lease).await.unwrap();
            match &expected {
                Ok(j) if j.state != JobState::Dead => assert_eq!(got.as_ref(), Some(j)),
                _ => assert_eq!(got, None, "step {step}: {expected:?}"),
            }
        } else {
            let got = s.update_job(id, event.clone()).await;
            match &expected {
                Ok(j) => assert_eq!(got.as_ref().ok(), Some(j), "step {step}"),
                Err(_) => assert!(is_conflict(got), "step {step}: {event:?}"),
            }
        }
        if let Ok(next) = expected {
            oracle = next;
        }
        assert_eq!(
            s.job(id).await.unwrap().as_ref(),
            Some(&oracle),
            "step {step}"
        );
        match oracle.state {
            JobState::Dead => {
                parked += 1;
                oracle = s.requeue_dead_job(id).await.unwrap();
                assert_ne!(oracle.id, id);
                id = oracle.id;
            }
            JobState::Done => {
                done += 1;
                id = s.enqueue_jobs(&[new_job(repo, kind, now.0)]).await.unwrap()[0];
                oracle = s.job(id).await.unwrap().unwrap();
            }
            _ => {}
        }
    }
    eprintln!("  {done} jobs done, {parked} parked as dead");
    assert!(done > 0 && parked > 0, "the sequence reaches Done and Dead");
}

/// Issue #131: `release_jobs` gives back exactly the running jobs of the
/// kinds asked that are leased to the owner, under their live lease, as the
/// holder's failure with no backoff: due now, the attempt counted, the
/// reason recorded. Other owners' jobs, other kinds and jobs that are not
/// running are untouched, a job on its last attempt is parked as dead, and
/// a lease that has already run out is released too.
async fn release_gives_back_only_the_owners_jobs_of_the_kinds_asked(c: Ctx) {
    let s = &c.store;
    let (repo, v) = target_version(s).await;
    let p = s.register_pack(&push_pack(repo, 2)).await.unwrap().id;
    let lease = Duration::from_secs(600);
    c.set_time(1000);
    let ids = s
        .enqueue_jobs(&[
            new_job(repo, JobKind::Compact { version: v.id }, 0),
            new_job(repo, JobKind::IndexCommits { version: v.id }, 0),
            new_job(repo, JobKind::MirrorPush { version: v.id }, 0),
            new_job(repo, JobKind::RewritePack { pack: p }, 0),
        ])
        .await
        .unwrap();
    let claim =
        async |tag: JobTag, owner: &str| s.claim_job(&[tag], owner, lease).await.unwrap().unwrap();
    let compact = claim(JobTag::Compact, "w").await;
    let mirror = claim(JobTag::MirrorPush, "w").await;
    let rewrite = claim(JobTag::RewritePack, "other").await;
    assert_eq!(
        [compact.id, mirror.id, rewrite.id],
        [ids[0], ids[2], ids[3]]
    );
    let tags = [JobTag::Compact, JobTag::IndexCommits, JobTag::RewritePack];
    c.set_time(2000);
    assert_eq!(s.release_jobs(&tags, "w", "lost answer").await.unwrap(), 1);
    let released = s.job(ids[0]).await.unwrap().unwrap();
    assert_eq!(
        released.state,
        JobState::Queued {
            run_after: UnixMicros(2000)
        }
    );
    assert_eq!(released.attempts, 1, "the released attempt counts");
    assert_eq!(released.last_error.as_deref(), Some("lost answer"));
    // A queued job of an asked kind (index_commits), a job of a kind not
    // asked (mirror_push) and another owner's job are untouched.
    assert_eq!(
        s.job(ids[1]).await.unwrap().unwrap().state,
        JobState::Queued {
            run_after: UnixMicros(0)
        }
    );
    assert_eq!(s.job(ids[2]).await.unwrap().unwrap(), mirror);
    assert_eq!(s.job(ids[3]).await.unwrap().unwrap(), rewrite);
    assert_eq!(s.release_jobs(&tags, "w", "again").await.unwrap(), 0);
    assert_eq!(s.release_jobs(&[], "other", "none").await.unwrap(), 0);
    // The released job is due at once.
    let again = claim(JobTag::Compact, "w").await;
    assert_eq!((again.id, again.attempts), (ids[0], 2));
    // On its last attempt, a release parks the job as dead.
    sqlx::query("UPDATE jobs SET attempts = $1 WHERE id = $2")
        .bind(i64::from(MAX_JOB_ATTEMPTS))
        .bind(ids[0].get())
        .execute(s.pool())
        .await
        .unwrap();
    assert_eq!(s.release_jobs(&tags, "w", "last").await.unwrap(), 1);
    assert_eq!(s.job(ids[0]).await.unwrap().unwrap().state, JobState::Dead);
    // A lease that has already run out (nobody reclaimed it yet) is
    // released like a live one, so it can never wedge the release.
    let index = claim(JobTag::IndexCommits, "w").await;
    assert_eq!(index.id, ids[1]);
    c.set_time(2000 + 600_000_000 + 1);
    assert_eq!(s.release_jobs(&tags, "w", "expired").await.unwrap(), 1);
    let expired = s.job(ids[1]).await.unwrap().unwrap();
    assert_eq!(
        expired.state,
        JobState::Queued {
            run_after: UnixMicros(2000 + 600_000_000 + 1)
        }
    );
    assert_eq!(expired.attempts, 1);
}

/// After `MAX_JOB_ATTEMPTS` expired leases a claim parks the job as dead
/// and returns nothing. The operator's requeue is resolve-then-enqueue in
/// one transaction: the dead row becomes done and a new job (new ID,
/// attempt 0, due now) takes the slot, so no worker holding an attempt of
/// the dead row can touch it. Resolve marks a dead job done; both refuse a
/// job that is not dead.
async fn dead_jobs_are_requeued_as_new_jobs(c: Ctx) {
    let s = &c.store;
    let (repo, v) = target_version(s).await;
    let kind = JobKind::Compact { version: v.id };
    let lease = Duration::from_micros(10);
    let id = s.enqueue_jobs(&[new_job(repo, kind, 0)]).await.unwrap()[0];
    for attempt in 1..=MAX_JOB_ATTEMPTS {
        let j = s
            .claim_job(&[JobTag::Compact], "w", lease)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(j.attempts, attempt);
        c.set_time(c.now().0 + 10);
    }
    assert_eq!(
        s.claim_job(&[JobTag::Compact], "w", lease).await.unwrap(),
        None
    );
    let dead = s.job(id).await.unwrap().unwrap();
    assert_eq!(
        (dead.state.clone(), dead.attempts),
        (JobState::Dead, MAX_JOB_ATTEMPTS)
    );
    assert!(dead.last_error.unwrap().contains("expired"));
    assert_eq!(
        s.dead_jobs(10)
            .await
            .unwrap()
            .iter()
            .map(|j| j.id)
            .collect::<Vec<_>>(),
        vec![id]
    );
    assert_eq!(
        s.enqueue_jobs(&[new_job(repo, kind, 0)]).await.unwrap(),
        vec![id]
    );

    let fresh = s.requeue_dead_job(id).await.unwrap();
    assert_ne!(fresh.id, id);
    assert_eq!(
        (
            fresh.kind,
            fresh.repo,
            fresh.attempts,
            fresh.state.clone(),
            fresh.last_error.clone()
        ),
        (kind, repo, 0, JobState::Queued { run_after: c.now() }, None)
    );
    assert_eq!(s.job(id).await.unwrap().unwrap().state, JobState::Done);
    assert!(s.dead_jobs(10).await.unwrap().is_empty());
    assert_eq!(
        s.enqueue_jobs(&[new_job(repo, kind, 0)]).await.unwrap(),
        vec![fresh.id],
        "the new job holds the slot"
    );
    // The last worker of the dead row held attempt 10; it can touch neither row.
    assert!(is_conflict(
        s.update_job(id, complete("w", MAX_JOB_ATTEMPTS)).await
    ));
    let claimed = s
        .claim_job(&[JobTag::Compact], "w", lease)
        .await
        .unwrap()
        .unwrap();
    assert_eq!((claimed.id, claimed.attempts), (fresh.id, 1));
    assert!(is_conflict(
        s.update_job(fresh.id, complete("w", MAX_JOB_ATTEMPTS))
            .await
    ));

    assert!(
        is_conflict(s.requeue_dead_job(fresh.id).await),
        "running is not dead"
    );
    assert!(
        is_conflict(s.resolve_dead_job(id).await),
        "done is not dead"
    );
    assert!(matches!(
        s.requeue_dead_job(JobId::new(9999).unwrap()).await,
        Err(StoreError::NotFound { .. })
    ));

    // Resolve: the slot frees without new work.
    sqlx::query(
        "UPDATE jobs SET state = 'dead', lease_owner = NULL, lease_expires_us = NULL WHERE id = $1",
    )
    .bind(fresh.id.get())
    .execute(s.pool())
    .await
    .unwrap();
    s.resolve_dead_job(fresh.id).await.unwrap();
    assert_eq!(
        s.job(fresh.id).await.unwrap().unwrap().state,
        JobState::Done
    );
    assert_eq!(count_open(s).await, 0);
}

/// `job_counts` (the dead-job alert and queue-lag metrics, node C5) counts
/// exactly the jobs that are not done, per kind and state, and reports the
/// earliest `run_after` of queued jobs only. Done jobs never appear, and
/// requeueing a dead job moves it from `dead` to `queued`.
async fn job_counts_cover_open_jobs_per_kind_and_state(c: Ctx) {
    let s = &c.store;
    assert_eq!(s.job_counts().await.unwrap(), vec![]);
    let (repo, v) = target_version(s).await;
    let p = pack(s, repo, 9).await;
    let v2 = advance(s, repo, "refs/heads/b", oid(2), p).await;
    let v3 = advance(s, repo, "refs/heads/c", oid(3), p).await;
    let ids = s
        .enqueue_jobs(&[
            new_job(repo, JobKind::Compact { version: v.id }, 900),
            new_job(repo, JobKind::Compact { version: v2.id }, 300),
            new_job(repo, JobKind::Compact { version: v3.id }, 600),
            new_job(repo, JobKind::IndexCommits { version: v.id }, 50),
            new_job(repo, JobKind::RewritePack { pack: p }, 70),
            new_job(repo, JobKind::MirrorPush { version: v.id }, 80),
        ])
        .await
        .unwrap();
    // The earliest compact job (run_after 300) is claimed: running.
    c.set_time(1_000);
    let claimed = s
        .claim_job(&[JobTag::Compact], "w", Duration::from_secs(60))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claimed.id, ids[1]);
    // The rewrite job dies; the mirror job is done.
    let set_state = |id: JobId, state: &'static str| {
        sqlx::query(AssertSqlSafe(format!(
            "UPDATE jobs SET state = '{state}' WHERE id = $1"
        )))
        .bind(id.get())
        .execute(s.pool())
    };
    set_state(ids[4], "dead").await.unwrap();
    set_state(ids[5], "done").await.unwrap();
    let count = |kind, state, count, earliest: Option<i64>| JobCount {
        kind,
        state,
        count,
        earliest_run_after: earliest.map(UnixMicros),
    };
    assert_eq!(
        s.job_counts().await.unwrap(),
        vec![
            count(JobTag::IndexCommits, OpenJobState::Queued, 1, Some(50)),
            count(JobTag::Compact, OpenJobState::Queued, 2, Some(600)),
            count(JobTag::Compact, OpenJobState::Running, 1, None),
            count(JobTag::RewritePack, OpenJobState::Dead, 1, None),
        ]
    );
    let fresh = s.requeue_dead_job(ids[4]).await.unwrap();
    let counts = s.job_counts().await.unwrap();
    assert_eq!(
        counts.last(),
        Some(&count(
            JobTag::RewritePack,
            OpenJobState::Queued,
            1,
            Some(fresh_run_after(&fresh))
        )),
        "requeued: no longer dead"
    );
    assert!(counts.iter().all(|c| c.state != OpenJobState::Dead));
}

fn fresh_run_after(j: &aspen_core::meta::Job) -> i64 {
    match j.state {
        JobState::Queued { run_after } => run_after.0,
        ref other => panic!("expected queued, got {other:?}"),
    }
}

/// `trunk_closures` (the wider-trunk metric, node C5) classifies each
/// repository by its **current** trunk version only: an older wider trunk
/// version, a wider branch, a repository with no trunk yet and a deleted
/// trunk are not counted as wider.
async fn trunk_closures_count_each_repository_by_its_current_trunk(c: Ctx) {
    let s = &c.store;
    assert_eq!(s.trunk_closures().await.unwrap(), TrunkClosures::default());
    let write = |name: &str, expected: Option<RefVersionId>, tip, p, closure| RefWrite {
        value: RefValue::Live {
            tip,
            pack: Some(p),
            closure,
        },
        ..live(name, expected, tip, p)
    };
    // exact: trunk was wider once, then exact again; a wider branch.
    let exact = new_repo(s, "acme/exact").await.id;
    let p = pack(s, exact, 1).await;
    let v = committed(s, exact, vec![write(MAIN, None, oid(1), p, Closure::Wider)])
        .await
        .remove(0);
    committed(
        s,
        exact,
        vec![
            write(MAIN, Some(v.id), oid(2), p, Closure::Exact),
            write("refs/heads/topic", None, oid(3), p, Closure::Wider),
        ],
    )
    .await;
    // wider: the current trunk version is wider.
    let wider = new_repo(s, "acme/wider").await.id;
    let p = pack(s, wider, 2).await;
    committed(s, wider, vec![write(MAIN, None, oid(4), p, Closure::Wider)]).await;
    // A branch only, and a deleted trunk: neither counts.
    let branch_only = new_repo(s, "acme/branch-only").await.id;
    let p = pack(s, branch_only, 3).await;
    committed(
        s,
        branch_only,
        vec![write("refs/heads/x", None, oid(5), p, Closure::Wider)],
    )
    .await;
    let deleted = new_repo(s, "acme/deleted").await.id;
    let p = pack(s, deleted, 4).await;
    let v = committed(
        s,
        deleted,
        vec![write(MAIN, None, oid(6), p, Closure::Wider)],
    )
    .await
    .remove(0);
    committed(s, deleted, vec![delete(MAIN, v.id)]).await;
    assert_eq!(
        s.trunk_closures().await.unwrap(),
        TrunkClosures { exact: 1, wider: 1 }
    );
}

async fn count_open(s: &SqlStore) -> i64 {
    sqlx::query("SELECT COUNT(*) AS n FROM jobs WHERE state <> 'done'")
        .fetch_one(s.pool())
        .await
        .unwrap()
        .get("n")
}

/// Eight workers drain 40 jobs concurrently: every job is claimed exactly
/// once (Postgres `FOR UPDATE SKIP LOCKED`, SQLite `BEGIN IMMEDIATE`), and
/// no claim fails with a storage error.
async fn concurrent_workers_never_claim_one_job_twice(c: Ctx) {
    let s = &c.store;
    let repo = new_repo(s, "acme/r").await.id;
    let mut jobs = Vec::new();
    for n in 1..=40 {
        let p = pack(s, repo, n).await;
        jobs.push(new_job(repo, JobKind::RewritePack { pack: p }, 0));
    }
    let ids: BTreeSet<JobId> = s.enqueue_jobs(&jobs).await.unwrap().into_iter().collect();
    assert_eq!(ids.len(), 40);
    let mut handles = Vec::new();
    for w in 0..8 {
        let s = s.clone();
        handles.push(tokio::spawn(async move {
            let owner = format!("w{w}");
            let mut mine = Vec::new();
            while let Some(j) = s
                .claim_job(&[JobTag::RewritePack], &owner, Duration::from_secs(60))
                .await
                .expect("no storage error")
            {
                s.update_job(j.id, complete(&owner, j.attempts))
                    .await
                    .unwrap();
                mine.push(j.id);
            }
            mine
        }));
    }
    let mut claimed = Vec::new();
    for h in handles {
        claimed.extend(h.await.unwrap());
    }
    assert_eq!(claimed.len(), 40, "each job claimed once");
    assert_eq!(claimed.into_iter().collect::<BTreeSet<_>>(), ids);
}

/// A tiny deterministic PRNG, so a failing sequence replays from its seed.
struct SplitMix64(u64);

impl SplitMix64 {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }
}

/// A request refused partway through its transaction releases everything
/// it took before it returns: right after a job update refused by the
/// fence, another connection can lock the job's row (Postgres) or take the
/// write lock (SQLite) without waiting, and the next worker claims the job
/// (Postgres `SKIP LOCKED` would otherwise pass over the still-locked
/// row); a push refused after its first write leaves no half-inserted
/// version that blocks the next push to that ref.
async fn a_refused_request_releases_its_locks(c: Ctx) {
    let s = &c.store;
    let repo = new_repo(s, "acme/r").await.id;
    // A second store whose lock waits fail at once, to probe the locks.
    let probe = SqlStore::connect_with(
        &c.url,
        &StoreOptions {
            lock_timeout: Duration::from_millis(1),
            ..StoreOptions::default()
        },
    )
    .await
    .unwrap();
    // One pack: each round's job is done before the next is enqueued, so
    // the same target gets a fresh job every round.
    let p = pack(s, repo, 1).await;
    for n in 1..=300 {
        let id = s
            .enqueue_jobs(&[new_job(repo, JobKind::RewritePack { pack: p }, 0)])
            .await
            .unwrap()[0];
        assert!(is_conflict(s.update_job(id, complete("w", 1)).await));
        // On SQLite, taking the write lock (`BEGIN IMMEDIATE`, 1 ms busy
        // timeout) is the probe.
        let mut tx = raw_write_tx(&probe).await;
        let lock = match s.dialect() {
            Dialect::Postgres => " FOR UPDATE NOWAIT",
            Dialect::Sqlite => "",
        };
        sqlx::query(AssertSqlSafe(format!(
            "SELECT id FROM jobs WHERE id = $1{lock}"
        )))
        .bind(id.get())
        .fetch_one(&mut *tx)
        .await
        .unwrap_or_else(|e| panic!("round {n}: the refused update still holds its lock: {e}"));
        tx.rollback().await.unwrap();
        let claimed = s
            .claim_job(&[JobTag::RewritePack], "w", Duration::from_secs(60))
            .await
            .unwrap();
        assert_eq!(claimed.map(|j| j.id), Some(id), "round {n}");
        s.update_job(id, complete("w", 1)).await.unwrap();
    }
    probe.pool().close().await;
    let other = advance(s, repo, "refs/heads/other", oid(1), p).await;
    for n in 1..=5u8 {
        let name = format!("refs/heads/a{n}");
        // `a<n>` sorts before `z`, so its version is inserted before the
        // `z` write is refused.
        assert!(is_conflict(
            s.commit_push(&PushCommit {
                repo,
                writes: vec![
                    live(&name, None, oid(2), p),
                    live("refs/heads/z", Some(other.id), oid(3), p),
                ],
            })
            .await
        ));
        let retry = tokio::time::timeout(
            Duration::from_secs(5),
            commit(s, repo, vec![live(&name, None, oid(2), p)]),
        )
        .await
        .expect("the refused push left nothing behind that blocks this one");
        assert!(matches!(retry, CommitOutcome::Committed(_)));
    }
}

/// Two pushes to different refs write the same commits to the commit index
/// concurrently, one in the order `a, b, ...` and the other reversed: both
/// commit, with no storage error, and each commit gets one row. An index
/// key another open transaction inserted makes the insert wait for that
/// transaction, so in caller order each push would hold a key the other
/// waits for, and Postgres would abort one as a deadlock (`40P01`). The
/// store writes rows in commit ID order.
async fn concurrent_pushes_index_shared_commits_in_any_order(c: Ctx) {
    const COMMITS: u32 = 300;
    const ROUNDS: u32 = 6;
    let s = &c.store;
    let repo = new_repo(s, "acme/r").await.id;
    let p = pack(s, repo, 1).await;
    for round in 0..ROUNDS {
        let ids: Vec<ObjectId> = (0..COMMITS).map(|n| oid_n(round * COMMITS + n)).collect();
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let mut handles = Vec::new();
        for (side, reversed) in [("a", false), ("b", true)] {
            let mut order = ids.clone();
            if reversed {
                order.reverse();
            }
            let mut w = live(&format!("refs/heads/r{round}{side}"), None, order[0], p);
            w.commits = order
                .iter()
                .map(|&commit| NewCommit {
                    commit,
                    parents: vec![],
                    committer_time: 0,
                    generation: None,
                    root_tree: None,
                })
                .collect();
            let (s, barrier) = (s.clone(), Arc::clone(&barrier));
            handles.push(tokio::spawn(async move {
                barrier.wait().await;
                s.commit_push(&PushCommit {
                    repo,
                    writes: vec![w],
                })
                .await
            }));
        }
        for h in handles {
            let outcome = h.await.unwrap().expect("no storage error (deadlock)");
            assert!(
                matches!(outcome, CommitOutcome::Committed(_)),
                "round {round}"
            );
        }
    }
    assert_eq!(count(s, "commit_index").await, i64::from(ROUNDS * COMMITS));
}

/// A push cancelled at any point (its future dropped, as when a client
/// disconnects or a timeout fires) is all-or-nothing and leaves nothing
/// behind: the next push to one of its refs neither fails nor hangs, and
/// it finds the other ref of the atomic push present exactly when the
/// cancelled push committed. The cancellation points are random, from
/// before the transaction starts to after it commits.
async fn a_cancelled_push_is_all_or_nothing_and_releases_its_locks(c: Ctx) {
    let s = &c.store;
    let repo = new_repo(s, "acme/r").await.id;
    let p = pack(s, repo, 1).await;
    let mut rng = SplitMix64(0x5eed);
    let mut cancelled = 0;
    for round in 0..40u32 {
        let (x, y) = (
            format!("refs/heads/x{round}"),
            format!("refs/heads/y{round}"),
        );
        let mut wx = live(&x, None, oid(1), p);
        wx.commits = (0..50)
            .map(|n| NewCommit {
                commit: oid_n(round * 100 + n),
                parents: vec![],
                committer_time: 0,
                generation: None,
                root_tree: None,
            })
            .collect();
        let push = PushCommit {
            repo,
            writes: vec![wx, live(&y, None, oid(1), p)],
        };
        let budget = Duration::from_micros(rng.next() % 2000);
        if tokio::time::timeout(budget, s.commit_push(&push))
            .await
            .is_err()
        {
            cancelled += 1;
        }
        let next = tokio::time::timeout(
            Duration::from_secs(5),
            s.commit_push(&PushCommit {
                repo,
                writes: vec![live(&x, None, oid(2), p)],
            }),
        )
        .await
        .expect("the cancelled push left nothing behind that blocks this one")
        .expect("no storage error");
        let y_now = s.current_version(repo, &rn(&y)).await.unwrap();
        match next {
            CommitOutcome::Committed(_) => {
                assert!(y_now.is_none(), "round {round}: half of a push landed")
            }
            CommitOutcome::CasLost { .. } => {
                assert!(y_now.is_some(), "round {round}: half of a push landed")
            }
        }
    }
    // How many budgets ran out depends on the machine's speed, so it is
    // reported, not asserted.
    eprintln!("  {cancelled} of 40 pushes cancelled");
}

/// Invariant (issue #150): a burst of operations that queue for the pool
/// far longer than the acquire timeout all succeed, because the pool keeps
/// returning connections; the wait is a queue, not a failure. A store with
/// one connection and a 200 ms acquire timeout runs ever larger bursts of
/// concurrent reads until some read has waited more than three timeouts,
/// which proves the burst was big enough that a deadline counted from the
/// start of each wait (sqlx's own, which the store used before) would have
/// failed it. Every read of every burst must succeed.
async fn a_burst_of_operations_queues_for_the_pool_instead_of_failing(c: Ctx) {
    let s = &c.store;
    new_repo(s, "acme/r").await;
    let timeout = Duration::from_millis(200);
    let small = SqlStore::connect_with(
        &c.url,
        &StoreOptions {
            max_connections: 1,
            acquire_timeout: timeout,
            ..StoreOptions::default()
        },
    )
    .await
    .unwrap();
    let name: RepoName = "acme/r".parse().unwrap();
    let mut jobs = 256usize;
    loop {
        let mut set = tokio::task::JoinSet::new();
        for _ in 0..jobs {
            let (small, name) = (small.clone(), name.clone());
            set.spawn(async move {
                let started = std::time::Instant::now();
                let found = small.repo(&name).await;
                (found, started.elapsed())
            });
        }
        let mut longest = Duration::ZERO;
        while let Some(done) = set.join_next().await {
            let (found, waited) = done.unwrap();
            let repo = found.unwrap_or_else(|e| {
                panic!("a read queued behind {jobs} others failed after {waited:?}: {e}")
            });
            assert!(repo.is_some());
            longest = longest.max(waited);
        }
        eprintln!("  {jobs} concurrent reads on one connection: longest wait {longest:?}");
        if longest > timeout * 3 {
            break;
        }
        assert!(
            jobs < 1 << 20,
            "no burst made a read wait past three acquire timeouts"
        );
        jobs *= 2;
    }
    small.pool().close().await;
}

/// Invariant: the queue still fails a waiter when the pool makes no
/// progress. The only connection of a store is held by a push waiting for a
/// row lock (5 s lock timeout); a read queued behind it fails with
/// `Unavailable` once a whole acquire timeout (300 ms) passes without a
/// connection coming back, and the push, released, still lands.
async fn a_pool_that_makes_no_progress_fails_its_waiters(c: Ctx) {
    let s = &c.store;
    let repo = new_repo(s, "acme/r").await.id;
    let p = pack(s, repo, 1).await;
    let v1 = advance(s, repo, MAIN, oid(1), p).await;
    let stuck = SqlStore::connect_with(
        &c.url,
        &StoreOptions {
            max_connections: 1,
            acquire_timeout: Duration::from_millis(300),
            lock_timeout: Duration::from_secs(5),
            ..StoreOptions::default()
        },
    )
    .await
    .unwrap();
    // Postgres: hold the `refs` row; SQLite: `BEGIN IMMEDIATE` holds the
    // write lock.
    let mut holder = raw_write_tx(s).await;
    sqlx::query("UPDATE refs SET seq = seq WHERE repo_id = $1")
        .bind(repo.get())
        .execute(&mut *holder)
        .await
        .unwrap();
    let push = {
        let stuck = stuck.clone();
        let push = PushCommit {
            repo,
            writes: vec![live(MAIN, Some(v1.id), oid(2), p)],
        };
        tokio::spawn(async move { stuck.commit_push(&push).await })
    };
    // Wait until the push holds the store's only connection (it then
    // blocks on the lock until `holder` ends).
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !(stuck.pool().size() == 1 && stuck.pool().num_idle() == 0) {
        assert!(
            std::time::Instant::now() < deadline,
            "the push never took the connection"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let started = std::time::Instant::now();
    let read = stuck.repo_by_id(repo).await;
    let waited = started.elapsed();
    assert!(
        matches!(&read, Err(StoreError::Unavailable { reason }) if reason.contains("came back")),
        "{read:?}"
    );
    assert!(
        waited >= Duration::from_millis(250) && waited < Duration::from_secs(3),
        "waited {waited:?}"
    );
    holder.rollback().await.unwrap();
    assert!(matches!(
        push.await.unwrap().unwrap(),
        CommitOutcome::Committed(_)
    ));
    // The queue works again once connections come back.
    assert!(stuck.repo_by_id(repo).await.unwrap().is_some());
    stuck.pool().close().await;
}

/// A push that waits for a lock longer than the store's lock timeout fails
/// with `Unavailable` (retryable), not a hang and not `CasLost`: Postgres
/// `lock_timeout` (`55P03`), SQLite's busy timeout. Once the lock is
/// released the same push commits. On Postgres every pooled connection
/// runs with the configured lock and statement timeouts, and the
/// migration connection's unlimited settings never reach the pool.
async fn a_lock_wait_past_the_lock_timeout_is_unavailable(c: Ctx) {
    let s = &c.store;
    let repo = new_repo(s, "acme/r").await.id;
    let p = pack(s, repo, 1).await;
    let v1 = advance(s, repo, MAIN, oid(1), p).await;
    let short = SqlStore::connect_with(
        &c.url,
        &StoreOptions {
            lock_timeout: Duration::from_millis(200),
            statement_timeout: Duration::from_secs(2),
            ..StoreOptions::default()
        },
    )
    .await
    .unwrap();

    // Postgres: hold the `refs` row; SQLite: `BEGIN IMMEDIATE` holds the
    // write lock.
    let mut holder = raw_write_tx(s).await;
    sqlx::query("UPDATE refs SET seq = seq WHERE repo_id = $1")
        .bind(repo.get())
        .execute(&mut *holder)
        .await
        .unwrap();
    let push = PushCommit {
        repo,
        writes: vec![live(MAIN, Some(v1.id), oid(2), p)],
    };
    let started = std::time::Instant::now();
    let result = short.commit_push(&push).await;
    let waited = started.elapsed();
    assert!(
        matches!(result, Err(StoreError::Unavailable { .. })),
        "{result:?}"
    );
    assert!(
        waited >= Duration::from_millis(150) && waited < Duration::from_secs(5),
        "waited {waited:?}"
    );
    holder.rollback().await.unwrap();
    assert!(matches!(
        short.commit_push(&push).await.unwrap(),
        CommitOutcome::Committed(_)
    ));

    if s.dialect() == Dialect::Postgres {
        let show = |store: SqlStore, setting: &'static str| async move {
            sqlx::query(AssertSqlSafe(format!("SHOW {setting}")))
                .fetch_one(store.pool())
                .await
                .unwrap()
                .get::<String, _>(0)
        };
        assert_eq!(show(short.clone(), "lock_timeout").await, "200ms");
        assert_eq!(show(short.clone(), "statement_timeout").await, "2s");
        // The case's store was migrated through its own pool.
        assert_eq!(show(s.clone(), "lock_timeout").await, "10s");
        assert_eq!(show(s.clone(), "statement_timeout").await, "1min");
    }
    short.pool().close().await;
}

// ---------------------------------------------------------------------------
// SSH keys (migration 0004)

fn ssh_key_row(n: u8) -> SshKeyRow {
    SshKeyRow {
        fingerprint: [n; 32],
        public_key: vec![n.wrapping_add(1); 51],
        subject: format!("user:{n}"),
        grants: "write:acme/mono".into(),
        description: "laptop".into(),
        created_at: UnixMicros(100 + i64::from(n)),
        expires_at: Some(UnixMicros(10_000)),
        revoked_at: None,
    }
}

/// SSH key rows read back exactly as written; a fingerprint is registered
/// once (a second registration, even of a revoked key, is a conflict); a
/// revocation keeps its first time and only touches its own key.
async fn ssh_keys_round_trip_and_revoke_once(c: Ctx) {
    let s = &c.store;
    let (a, b) = (ssh_key_row(1), ssh_key_row(2));
    s.insert_ssh_key(&a).await.unwrap();
    s.insert_ssh_key(&b).await.unwrap();
    assert_eq!(s.ssh_key(&a.fingerprint).await.unwrap(), Some(a.clone()));
    assert!(s.ssh_key(&[9; 32]).await.unwrap().is_none());
    let mut dup = ssh_key_row(1);
    dup.subject = "someone else".into();
    assert!(is_conflict(s.insert_ssh_key(&dup).await));
    assert_eq!(s.ssh_keys(10).await.unwrap(), vec![a.clone(), b.clone()]);
    assert_eq!(s.ssh_keys(1).await.unwrap(), vec![a.clone()]);

    assert!(
        s.revoke_ssh_key(&a.fingerprint, UnixMicros(500))
            .await
            .unwrap()
    );
    assert!(
        s.revoke_ssh_key(&a.fingerprint, UnixMicros(900))
            .await
            .unwrap()
    );
    assert!(!s.revoke_ssh_key(&[9; 32], UnixMicros(500)).await.unwrap());
    assert_eq!(
        s.ssh_key(&a.fingerprint).await.unwrap().unwrap().revoked_at,
        Some(UnixMicros(500))
    );
    assert_eq!(
        s.ssh_key(&b.fingerprint).await.unwrap().unwrap().revoked_at,
        None
    );
    // Re-registering a revoked key does not bring it back.
    assert!(is_conflict(s.insert_ssh_key(&a).await));
    assert_eq!(
        s.ssh_key(&a.fingerprint).await.unwrap().unwrap().revoked_at,
        Some(UnixMicros(500))
    );
}

// ---------------------------------------------------------------------------
// Tokens (migration 0002)

fn token_row(n: u8) -> TokenRow {
    TokenRow {
        id: [n; 16],
        secret_sha256: [n.wrapping_add(1); 32],
        subject: format!("agent:{n}"),
        grants: "write:acme/mono read:demo/*".into(),
        description: "ci".into(),
        created_at: UnixMicros(100 + i64::from(n)),
        expires_at: Some(UnixMicros(10_000)),
        revoked_at: None,
    }
}

/// Every field of a token row, for comparing rows in tests (`TokenRow` has
/// no `PartialEq`, so the digest is never compared with `==` in product
/// code).
type TokenFields = (
    [u8; 16],
    [u8; 32],
    String,
    String,
    String,
    UnixMicros,
    Option<UnixMicros>,
    Option<UnixMicros>,
);

fn fields(t: &TokenRow) -> TokenFields {
    (
        t.id,
        t.secret_sha256,
        t.subject.clone(),
        t.grants.clone(),
        t.description.clone(),
        t.created_at,
        t.expires_at,
        t.revoked_at,
    )
}

fn all_fields(ts: &[TokenRow]) -> Vec<TokenFields> {
    ts.iter().map(fields).collect()
}

/// Token rows read back exactly as written; an ID is stored once; a
/// revocation keeps its first time and only touches its own token.
async fn tokens_round_trip_and_revoke_once(c: Ctx) {
    let s = &c.store;
    let (a, b) = (token_row(1), token_row(2));
    s.insert_token(&a).await.unwrap();
    s.insert_token(&b).await.unwrap();
    assert_eq!(
        s.token(&a.id).await.unwrap().as_ref().map(fields),
        Some(fields(&a))
    );
    assert!(s.token(&[9; 16]).await.unwrap().is_none());
    let mut dup = token_row(1);
    dup.subject = "someone else".into();
    assert!(is_conflict(s.insert_token(&dup).await));
    assert_eq!(s.token(&a.id).await.unwrap().unwrap().subject, "agent:1");
    assert_eq!(
        all_fields(&s.tokens(10).await.unwrap()),
        all_fields(&[a.clone(), b.clone()])
    );
    assert_eq!(
        all_fields(&s.tokens(1).await.unwrap()),
        all_fields(std::slice::from_ref(&a))
    );

    assert!(s.revoke_token(&a.id, UnixMicros(500)).await.unwrap());
    assert!(s.revoke_token(&a.id, UnixMicros(900)).await.unwrap());
    assert!(!s.revoke_token(&[9; 16], UnixMicros(500)).await.unwrap());
    assert_eq!(
        s.token(&a.id).await.unwrap().unwrap().revoked_at,
        Some(UnixMicros(500))
    );
    assert_eq!(s.token(&b.id).await.unwrap().unwrap().revoked_at, None);
    // The row holds the digest it was given and nothing longer.
    let raw: Vec<u8> = sqlx::query("SELECT secret_sha256 FROM tokens WHERE id = $1")
        .bind(a.id.to_vec())
        .fetch_one(s.pool())
        .await
        .unwrap()
        .get(0);
    assert_eq!(raw, a.secret_sha256.to_vec());
}

// ---------------------------------------------------------------------------
// Mirror mode (migration 0003)

fn mirror_row(repo: RepoId) -> MirrorRow {
    MirrorRow {
        repo,
        upstream_url: "https://github.com/acme/mono.git".into(),
        mode: "proxy".into(),
        credential: "gh".into(),
        created_at: UnixMicros(100),
        last_sync_at: None,
        last_error: None,
    }
}

fn agreed(n: u8) -> MirrorRefState {
    MirrorRefState {
        synced: Some(oid(n)),
        conflict: None,
    }
}

/// A repository is a mirror at most once; the last sync's time and error
/// round-trip (a success clears the error); removing the mirror drops its
/// ledger and keeps the repository.
async fn mirrors_round_trip_and_removal_drops_the_ledger(c: Ctx) {
    let s = &c.store;
    let a = new_repo(s, "acme/a").await;
    let b = new_repo(s, "acme/b").await;
    s.insert_mirror(&mirror_row(a.id)).await.unwrap();
    let mut rb = mirror_row(b.id);
    rb.mode = "async".into();
    rb.credential = String::new();
    s.insert_mirror(&rb).await.unwrap();
    assert!(is_conflict(s.insert_mirror(&mirror_row(a.id)).await));
    assert_eq!(s.mirror(a.id).await.unwrap(), Some(mirror_row(a.id)));
    assert_eq!(
        s.mirrors().await.unwrap(),
        vec![mirror_row(a.id), rb.clone()]
    );

    s.record_mirror_sync(a.id, UnixMicros(200), Some("upstream: HTTP 503"))
        .await
        .unwrap();
    let m = s.mirror(a.id).await.unwrap().unwrap();
    assert_eq!(
        (m.last_sync_at, m.last_error.as_deref()),
        (Some(UnixMicros(200)), Some("upstream: HTTP 503"))
    );
    s.record_mirror_sync(a.id, UnixMicros(300), None)
        .await
        .unwrap();
    let m = s.mirror(a.id).await.unwrap().unwrap();
    assert_eq!(
        (m.last_sync_at, m.last_error),
        (Some(UnixMicros(300)), None)
    );

    let main = rn("refs/heads/main");
    assert!(
        s.set_mirror_ref(a.id, &main, None, agreed(1), UnixMicros(10))
            .await
            .unwrap()
    );
    assert!(
        s.set_mirror_ref(b.id, &main, None, agreed(2), UnixMicros(10))
            .await
            .unwrap()
    );
    assert!(s.delete_mirror(a.id).await.unwrap());
    assert!(!s.delete_mirror(a.id).await.unwrap());
    assert_eq!(s.mirror(a.id).await.unwrap(), None);
    assert!(s.mirror_refs(a.id).await.unwrap().is_empty());
    assert_eq!(
        s.mirror_refs(b.id).await.unwrap().len(),
        1,
        "other mirrors keep theirs"
    );
    assert!(
        s.repo_by_id(a.id).await.unwrap().is_some(),
        "the repository stays"
    );
    // A ledger row needs its mirror.
    assert!(
        s.set_mirror_ref(a.id, &main, None, agreed(1), UnixMicros(10))
            .await
            .is_err()
    );
}

/// `create_mirror_repo` writes the repository and its mirror row in one
/// transaction: a mirror row the schema refuses leaves no repository
/// behind (a retry with the same name then works), and a taken name
/// leaves no mirror row.
async fn a_mirror_repository_is_created_whole_or_not_at_all(c: Ctx) {
    let s = &c.store;
    let name = RepoName::new("acme/mirror").unwrap();
    let settings = RepoSettings::new(rn("refs/heads/main")).unwrap();
    let bad = |repo| MirrorRow {
        mode: "sideways".into(),
        ..mirror_row(repo)
    };
    assert!(s.create_mirror_repo(&name, &settings, bad).await.is_err());
    assert_eq!(s.repo(&name).await.unwrap(), None, "no orphan repository");
    assert!(s.mirrors().await.unwrap().is_empty());

    let repo = s
        .create_mirror_repo(&name, &settings, mirror_row)
        .await
        .unwrap();
    assert_eq!(s.repo(&name).await.unwrap(), Some(repo.clone()));
    assert_eq!(s.mirror(repo.id).await.unwrap(), Some(mirror_row(repo.id)));

    let taken = new_repo(s, "acme/taken").await;
    assert!(is_conflict(
        s.create_mirror_repo(&taken.name, &settings, mirror_row)
            .await
    ));
    assert_eq!(s.mirror(taken.id).await.unwrap(), None);
    assert_eq!(s.mirrors().await.unwrap().len(), 1);
}

/// The ledger only moves from the value the writer read: a stale
/// expectation writes nothing, an empty state removes the row, conflicts
/// round-trip with either side absent, and two first writes of one ref
/// race to exactly one winner.
async fn the_mirror_ledger_is_a_compare_and_swap(c: Ctx) {
    let s = &c.store;
    let r = new_repo(s, "acme/mono").await;
    s.insert_mirror(&mirror_row(r.id)).await.unwrap();
    let (main, tag) = (rn("refs/heads/main"), rn("refs/tags/v1"));
    assert_eq!(
        s.mirror_ref(r.id, &main).await.unwrap(),
        MirrorRefState::default()
    );
    assert!(
        s.set_mirror_ref(r.id, &main, None, agreed(1), UnixMicros(10))
            .await
            .unwrap()
    );
    // Stale: the row holds 1, not none or 2.
    assert!(
        !s.set_mirror_ref(r.id, &main, None, agreed(3), UnixMicros(11))
            .await
            .unwrap()
    );
    assert!(
        !s.set_mirror_ref(r.id, &main, Some(oid(2)), agreed(3), UnixMicros(11))
            .await
            .unwrap()
    );
    assert_eq!(s.mirror_ref(r.id, &main).await.unwrap(), agreed(1));

    let conflict = MirrorRefState {
        synced: Some(oid(1)),
        conflict: Some(ConflictRow {
            at: UnixMicros(20),
            aspen: Some(oid(4)),
            upstream: None,
        }),
    };
    assert!(
        s.set_mirror_ref(r.id, &main, Some(oid(1)), conflict, UnixMicros(20))
            .await
            .unwrap()
    );
    let both_absent = MirrorRefState {
        synced: None,
        conflict: Some(ConflictRow {
            at: UnixMicros(21),
            aspen: None,
            upstream: None,
        }),
    };
    assert!(
        s.set_mirror_ref(r.id, &tag, None, both_absent, UnixMicros(21))
            .await
            .unwrap()
    );
    let rows = s.mirror_refs(r.id).await.unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(
        (rows[0].name.clone(), rows[0].state),
        (main.clone(), conflict)
    );
    assert_eq!(rows[0].updated_at, UnixMicros(20));
    assert_eq!(
        (rows[1].name.clone(), rows[1].state),
        (tag.clone(), both_absent)
    );
    assert_eq!(s.mirror_conflict_count().await.unwrap(), 2);

    // Agreeing on absence removes the row.
    assert!(
        s.set_mirror_ref(r.id, &tag, None, MirrorRefState::default(), UnixMicros(30))
            .await
            .unwrap()
    );
    assert_eq!(s.mirror_refs(r.id).await.unwrap().len(), 1);
    assert_eq!(s.mirror_conflict_count().await.unwrap(), 1);

    // Two first writes of a new ref: one wins, the other sees it moved.
    let topic = rn("refs/heads/topic");
    let (s1, s2) = (s.clone(), s.clone());
    let (t1, t2) = (topic.clone(), topic.clone());
    let id = r.id;
    let (x, y) = tokio::join!(
        tokio::spawn(async move {
            s1.set_mirror_ref(id, &t1, None, agreed(7), UnixMicros(40))
                .await
        }),
        tokio::spawn(async move {
            s2.set_mirror_ref(id, &t2, None, agreed(8), UnixMicros(40))
                .await
        }),
    );
    let (x, y) = (x.unwrap().unwrap(), y.unwrap().unwrap());
    assert!(x ^ y, "exactly one first write wins: {x} {y}");
    let won = s.mirror_ref(r.id, &topic).await.unwrap();
    assert_eq!(won, if x { agreed(7) } else { agreed(8) });
    // Two updates of the existing row from the same agreement: the row lock
    // serializes them, and the second sees the first's value.
    let from = won.synced;
    let (s1, s2) = (s.clone(), s.clone());
    let (t1, t2) = (topic.clone(), topic.clone());
    let (x, y) = tokio::join!(
        tokio::spawn(async move {
            s1.set_mirror_ref(id, &t1, from, agreed(9), UnixMicros(50))
                .await
        }),
        tokio::spawn(async move {
            s2.set_mirror_ref(id, &t2, from, agreed(10), UnixMicros(50))
                .await
        }),
    );
    let (x, y) = (x.unwrap().unwrap(), y.unwrap().unwrap());
    assert!(x ^ y, "exactly one update wins: {x} {y}");
    let won = s.mirror_ref(r.id, &topic).await.unwrap();
    assert_eq!(won, if x { agreed(9) } else { agreed(10) });
}

// Covers and chains (migration 0005)

fn span(first: u64, last: u64) -> SeqRange {
    SeqRange::new(Seq::new(first).unwrap(), Seq::new(last).unwrap()).unwrap()
}

fn level(n: u8) -> CoverLevel {
    CoverLevel::new(n).unwrap()
}

fn new_cover(repo: RepoId, l: u8, first: u64, last: u64, chain: Option<CoverChain>) -> NewCover {
    NewCover {
        repo,
        name: rn(MAIN),
        level: level(l),
        range: span(first, last),
        chain,
    }
}

fn compaction_pack(repo: RepoId, n: u8, l: u8) -> NewPack {
    NewPack {
        origin: PackOrigin::Compaction { level: level(l) },
        ..push_pack(repo, n)
    }
}

/// `n` versions of main, each after the first depending on its parent:
/// a dependency chain.
async fn chained_versions(s: &SqlStore, repo: RepoId, n: u8) -> Vec<RefVersion> {
    let p = pack(s, repo, 200).await;
    let mut out: Vec<RefVersion> = Vec::new();
    for i in 1..=n {
        let mut w = live(MAIN, out.last().map(|v| v.id), oid(i), p);
        w.deps.trunk = out.last().map(|v| v.id);
        out.push(committed(s, repo, vec![w]).await.remove(0));
    }
    out
}

/// A cover is created once per repository, ref, level and first seq:
/// inserting it again returns the stored cover unchanged (a chain given the
/// second time is not added), another last seq is a conflict, and another
/// level is another cover.
async fn insert_cover_is_idempotent_per_span(c: Ctx) {
    let s = &c.store;
    let repo = new_repo(s, "acme/r").await.id;
    let first = s
        .insert_cover(&new_cover(repo, 1, 1, 4, None))
        .await
        .unwrap();
    assert_eq!(
        (
            first.repo,
            &first.name,
            first.level,
            first.range,
            first.pack,
            first.chain
        ),
        (repo, &rn(MAIN), level(1), span(1, 4), None, None)
    );
    assert_eq!(
        s.insert_cover(&new_cover(repo, 1, 1, 4, None))
            .await
            .unwrap(),
        first
    );
    let versions = chained_versions(s, repo, 4).await;
    let chain = CoverChain::recorded(versions[0].id);
    assert_eq!(
        s.insert_cover(&new_cover(repo, 1, 1, 4, Some(chain)))
            .await
            .unwrap(),
        first,
        "an existing cover's chain is not changed"
    );
    assert!(is_conflict(
        s.insert_cover(&new_cover(repo, 1, 1, 5, None)).await
    ));
    let l2 = s
        .insert_cover(&new_cover(repo, 2, 1, 4, None))
        .await
        .unwrap();
    assert_ne!(l2.id, first.id);
    assert_eq!(s.covers(repo, &rn(MAIN)).await.unwrap(), vec![first, l2]);
    assert_eq!(count(s, "cover_chains").await, 0);
}

/// A chain built by `VerifiedRange::verify` is stored with its cover and
/// read back by `covers()`; a chain whose first version is not the ref's
/// version at the cover's first seq is refused, and the refused cover is
/// not created either.
async fn a_cover_chain_round_trips_and_starts_at_the_first_version(c: Ctx) {
    let s = &c.store;
    let repo = new_repo(s, "acme/r").await.id;
    let v = chained_versions(s, repo, 4).await;
    let verified = VerifiedRange::verify(&rn(MAIN), span(1, 4), &v).unwrap();
    let cover = s
        .insert_cover(&new_cover(repo, 1, 1, 4, Some(verified.chain())))
        .await
        .unwrap();
    assert_eq!(cover.chain, Some(verified.chain()));
    assert_eq!(cover.chain.unwrap().first_version(), v[0].id);
    let listed = s.covers(repo, &rn(MAIN)).await.unwrap();
    assert_eq!(listed, vec![cover.clone()]);
    assert_eq!(VerifiedRange::of_cover(&listed[0]), Some(verified));

    // A sub-range starting at seq 3 must start at version 3.
    let tail = VerifiedRange::verify(&rn(MAIN), span(3, 4), &v[2..]).unwrap();
    let ok = s
        .insert_cover(&new_cover(repo, 2, 3, 4, Some(tail.chain())))
        .await
        .unwrap();
    assert_eq!(ok.chain.unwrap().first_version(), v[2].id);
    let before = count(s, "covers").await;
    for wrong in [v[1].id, v[3].id] {
        assert!(is_conflict(
            s.insert_cover(&new_cover(repo, 3, 3, 4, Some(CoverChain::recorded(wrong))))
                .await
        ));
    }
    // Another ref's version at the same seq.
    let other = advance(s, repo, "refs/heads/x", oid(9), pack(s, repo, 201).await).await;
    assert_eq!(other.seq.get(), 1);
    assert!(is_conflict(
        s.insert_cover(&new_cover(
            repo,
            4,
            1,
            4,
            Some(CoverChain::recorded(other.id))
        ))
        .await
    ));
    assert_eq!(
        count(s, "covers").await,
        before,
        "a refused cover is not created"
    );
    assert_eq!(count(s, "cover_chains").await, 2);
}

/// A cover's pack is set once, to a pack of its repository at its level:
/// the same pack again is fine, another pack is a conflict, a push pack,
/// a pack at another level or of another repository is a conflict that
/// leaves the cover uncompacted, and an unknown cover is not found.
async fn set_cover_pack_records_one_pack_at_the_cover_level(c: Ctx) {
    let s = &c.store;
    let repo = new_repo(s, "acme/r").await.id;
    let other_repo = new_repo(s, "acme/o").await.id;
    let cover = s
        .insert_cover(&new_cover(repo, 1, 1, 4, None))
        .await
        .unwrap();
    let l1 = s
        .register_pack(&compaction_pack(repo, 1, 1))
        .await
        .unwrap()
        .id;
    let l1b = s
        .register_pack(&compaction_pack(repo, 2, 1))
        .await
        .unwrap()
        .id;
    let set = s.set_cover_pack(cover.id, l1).await.unwrap();
    assert_eq!(set.pack, Some(l1));
    assert_eq!(
        Cover {
            pack: None,
            ..set.clone()
        },
        cover
    );
    assert_eq!(s.set_cover_pack(cover.id, l1).await.unwrap(), set);
    assert!(is_conflict(s.set_cover_pack(cover.id, l1b).await));
    assert_eq!(s.covers(repo, &rn(MAIN)).await.unwrap(), vec![set]);

    let bare = s
        .insert_cover(&new_cover(repo, 1, 5, 8, None))
        .await
        .unwrap();
    let push = pack(s, repo, 3).await;
    let l2 = s
        .register_pack(&compaction_pack(repo, 4, 2))
        .await
        .unwrap()
        .id;
    let foreign = s
        .register_pack(&compaction_pack(other_repo, 5, 1))
        .await
        .unwrap()
        .id;
    for wrong in [push, l2, foreign] {
        assert!(is_conflict(s.set_cover_pack(bare.id, wrong).await));
    }
    assert_eq!(s.covers(repo, &rn(MAIN)).await.unwrap()[1], bare);
    assert!(matches!(
        s.set_cover_pack(CoverId::new(9999).unwrap(), l1).await,
        Err(StoreError::NotFound { .. })
    ));
}

/// `pack_by_checksum` finds the pack with that checksum in that repository
/// only.
async fn pack_by_checksum_looks_up_one_repository(c: Ctx) {
    let s = &c.store;
    let repo = new_repo(s, "acme/r").await.id;
    let other = new_repo(s, "acme/o").await.id;
    let id = pack(s, repo, 1).await;
    let sum = PackChecksum::from_bytes([1; 20]);
    let got = s.pack_by_checksum(repo, &sum).await.unwrap().unwrap();
    assert_eq!((got.id, &got.pack), (id, &push_pack(repo, 1)));
    assert_eq!(s.pack_by_checksum(other, &sum).await.unwrap(), None);
    assert_eq!(
        s.pack_by_checksum(repo, &PackChecksum::from_bytes([2; 20]))
            .await
            .unwrap(),
        None
    );
}

// ---------------------------------------------------------------------------
// Garbage collection of orphaned packs (migration 0005, issue #48)

const GRACE: Duration = Duration::from_secs(10);
const GRACE_US: i64 = 10_000_000;

/// Longer than any test's clock runs: the test's lease never expires
/// unless a test lets it.
const FOREVER: Duration = Duration::from_secs(1 << 40);

/// The garbage-collection lease, held by this test.
async fn gc_lease(s: &SqlStore) -> Lease {
    s.acquire_lease(GC_LEASE, "test", FOREVER)
        .await
        .unwrap()
        .expect("nobody else holds the lease")
}

/// One sweep batch of at most `limit` registrations, under the test's
/// lease.
async fn sweep_batch(s: &SqlStore, limit: u32) -> SweepBatch {
    let lease = gc_lease(s).await;
    s.sweep_orphan_packs(&lease, GRACE, limit).await.unwrap()
}

/// The rows one batch of at most `limit` deletes.
async fn sweep_rows(s: &SqlStore, limit: u32) -> Vec<aspen_core::pack::PackRecord> {
    sweep_batch(s, limit).await.deleted
}

async fn sweep(s: &SqlStore) -> Vec<PackId> {
    sweep_rows(s, 100).await.iter().map(|p| p.id).collect()
}

/// Registering records the store's time on the pack, and registering it
/// again moves that time forward (never back): the sweep measures the grace
/// period from the latest registration.
async fn registering_a_pack_again_refreshes_its_time(c: Ctx) {
    let s = &c.store;
    let repo = new_repo(s, "acme/r").await.id;
    let t0 = c.now().0;
    let p = pack(s, repo, 1).await;
    c.set_time(t0 + GRACE_US - 1);
    assert_eq!(pack(s, repo, 1).await, p);
    c.set_time(t0 + GRACE_US);
    assert_eq!(sweep(s).await, vec![], "registered again 1 µs ago");
    // A registration on a server whose clock is behind keeps the later time.
    c.set_time(t0);
    assert_eq!(pack(s, repo, 1).await, p);
    c.set_time(t0 + 2 * GRACE_US - 2);
    assert_eq!(sweep(s).await, vec![]);
    c.set_time(t0 + 2 * GRACE_US - 1);
    assert_eq!(sweep(s).await, vec![p]);
}

/// An unreferenced pack is deleted only once its last registration is at
/// least the grace period old; the sweep returns the deleted rows exactly.
/// Each deleted row leaves a pending tombstone naming its generation, which
/// stays pending (whatever the clock says) until its deletes are finished.
/// The checksum registers again at once, as a new row of the next
/// generation whose keys differ from every tombstoned generation's.
async fn an_orphan_is_swept_after_grace_and_tombstoned(c: Ctx) {
    let s = &c.store;
    let repo = new_repo(s, "acme/r").await.id;
    let t0 = c.now().0;
    let p = pack(s, repo, 1).await;
    let q = pack(s, repo, 2).await;
    let rows = s.packs(&[p, q]).await.unwrap();
    assert!(
        rows.iter().all(|r| r.generation == PackGeneration::FIRST),
        "{rows:?}"
    );
    assert_eq!(sweep(s).await, vec![]);
    c.set_time(t0 + GRACE_US - 1);
    assert_eq!(sweep(s).await, vec![]);
    c.set_time(t0 + GRACE_US);
    // A limit of one deletes the oldest first.
    assert_eq!(sweep_rows(s, 1).await, vec![rows[0].clone()]);
    assert_eq!(sweep_rows(s, 0).await, vec![]);
    c.set_time(t0 + GRACE_US + 1);
    assert_eq!(sweep_rows(s, 5).await, vec![rows[1].clone()]);
    assert_eq!(sweep(s).await, vec![]);
    let sum = PackChecksum::from_bytes([1; 20]);
    assert_eq!(s.pack_by_checksum(repo, &sum).await.unwrap(), None);
    assert!(matches!(
        s.packs(&[p]).await,
        Err(StoreError::NotFound { .. })
    ));
    assert_eq!(count(s, "pack_touches").await, 0);

    // Both deletions are pending, oldest sweep first, and stay pending
    // however much later it is.
    let stone = |n: u8, g: u32| PackTombstone {
        repo,
        checksum: PackChecksum::from_bytes([n; 20]),
        generation: PackGeneration::new(g),
    };
    c.set_time(t0 + 1000 * GRACE_US);
    assert_eq!(
        s.pending_pack_deletes(10).await.unwrap(),
        vec![stone(1, 0), stone(2, 0)]
    );
    assert_eq!(s.pending_pack_deletes(1).await.unwrap(), vec![stone(1, 0)]);

    // Registering the checksum again is not refused: it is a new row of
    // the next generation, with keys no tombstone names.
    let again = s.register_pack(&push_pack(repo, 1)).await.unwrap();
    assert_ne!(again.id, p);
    assert_eq!(again.generation, PackGeneration::new(1));
    let old = stone(1, 0).keys();
    for key in [&again.keys().pack, &again.keys().idx, &again.keys().entries] {
        assert!(![&old.pack, &old.idx, &old.entries].contains(&key), "{key}");
    }
    assert_eq!(s.packs(&[again.id]).await.unwrap(), vec![again.clone()]);
    // Registering it once more returns the same row.
    assert_eq!(s.register_pack(&push_pack(repo, 1)).await.unwrap(), again);
    // The same checksum in another repository starts at generation 0.
    let other = new_repo(s, "acme/o").await.id;
    let o = s.register_pack(&push_pack(other, 1)).await.unwrap();
    assert_eq!(o.generation, PackGeneration::FIRST);

    // Finishing is per tombstone and idempotent; finished rows stay, so the
    // generation keeps counting.
    s.finish_pack_deletes(&[stone(1, 0)]).await.unwrap();
    s.finish_pack_deletes(&[stone(1, 0), stone(9, 0)])
        .await
        .unwrap();
    assert_eq!(s.pending_pack_deletes(10).await.unwrap(), vec![stone(2, 0)]);
    assert_eq!(count(s, "pack_tombstones").await, 2);
    c.set_time(t0 + 2000 * GRACE_US);
    assert_eq!(sweep(s).await, vec![again.id, o.id]);
    assert_eq!(
        s.pending_pack_deletes(10).await.unwrap(),
        vec![
            stone(2, 0),
            stone(1, 1),
            PackTombstone {
                repo: other,
                ..stone(1, 0)
            }
        ]
    );
    let third = s.register_pack(&push_pack(repo, 1)).await.unwrap();
    assert_eq!(third.generation, PackGeneration::new(2));
}

/// The sweep never deletes a pack that a ref version, a cover or a job
/// names, however old; it deletes the orphan next to them.
async fn the_sweep_never_deletes_a_referenced_pack(c: Ctx) {
    let s = &c.store;
    let repo = new_repo(s, "acme/r").await.id;
    let t0 = c.now().0;
    let by_version = pack(s, repo, 1).await;
    advance(s, repo, MAIN, oid(1), by_version).await;
    let by_cover = s
        .register_pack(&compaction_pack(repo, 2, 1))
        .await
        .unwrap()
        .id;
    let cover = s
        .insert_cover(&new_cover(repo, 1, 1, 1, None))
        .await
        .unwrap();
    s.set_cover_pack(cover.id, by_cover).await.unwrap();
    let by_job = pack(s, repo, 3).await;
    s.enqueue_jobs(&[new_job(repo, JobKind::RewritePack { pack: by_job }, 0)])
        .await
        .unwrap();
    let orphan = pack(s, repo, 4).await;
    c.set_time(t0 + 100 * GRACE_US);
    assert_eq!(sweep(s).await, vec![orphan]);
    c.set_time(t0 + 1000 * GRACE_US);
    assert_eq!(sweep(s).await, vec![]);
    assert_eq!(
        s.packs(&[by_version, by_cover, by_job])
            .await
            .unwrap()
            .len(),
        3
    );
}

/// A pack registered before migration 0005 has no registration time: the
/// first sweep that finds it orphaned records one instead of deleting it,
/// and a sweep a grace period later deletes it.
async fn a_legacy_orphan_survives_its_first_sweep(c: Ctx) {
    let s = &c.store;
    let repo = new_repo(s, "acme/r").await.id;
    let t0 = c.now().0;
    let p = pack(s, repo, 1).await;
    sqlx::query("DELETE FROM pack_touches")
        .execute(s.pool())
        .await
        .unwrap();
    c.set_time(t0 + 100 * GRACE_US);
    assert_eq!(sweep(s).await, vec![]);
    assert_eq!(count(s, "pack_touches").await, 1);
    c.set_time(t0 + 101 * GRACE_US - 1);
    assert_eq!(sweep(s).await, vec![]);
    c.set_time(t0 + 101 * GRACE_US);
    assert_eq!(sweep(s).await, vec![p]);
}

/// Issue #48: two pushes race on one ref; the loser's pack, registered for
/// a write whose compare-and-swap was lost, is swept after the grace
/// period, and the winner's pack stays.
async fn a_pack_of_a_lost_compare_and_swap_is_swept(c: Ctx) {
    let s = &c.store;
    let repo = new_repo(s, "acme/r").await.id;
    let t0 = c.now().0;
    let base = pack(s, repo, 1).await;
    let v1 = advance(s, repo, MAIN, oid(1), base).await;
    let winner = pack(s, repo, 2).await;
    let loser = pack(s, repo, 3).await;
    committed(s, repo, vec![live(MAIN, Some(v1.id), oid(2), winner)]).await;
    assert!(matches!(
        commit(s, repo, vec![live(MAIN, Some(v1.id), oid(3), loser)]).await,
        CommitOutcome::CasLost { .. }
    ));
    c.set_time(t0 + GRACE_US);
    assert_eq!(sweep(s).await, vec![loser]);
    assert_eq!(s.packs(&[base, winner]).await.unwrap().len(), 2);
}

/// A push that commits after its pack was swept (it took longer than the
/// grace period after registering it) fails retryably and writes nothing.
async fn a_push_naming_a_swept_pack_is_unavailable(c: Ctx) {
    let s = &c.store;
    let repo = new_repo(s, "acme/r").await.id;
    let t0 = c.now().0;
    let p = pack(s, repo, 1).await;
    c.set_time(t0 + GRACE_US);
    assert_eq!(sweep(s).await, vec![p]);
    let r = s
        .commit_push(&PushCommit {
            repo,
            writes: vec![live(MAIN, None, oid(1), p)],
        })
        .await;
    assert!(matches!(r, Err(StoreError::Unavailable { .. })), "{r:?}");
    assert_eq!(counts(s).await, [0, 0, 0, 0]);

    // The checksum registered again, by another writer, is a new row of the
    // next generation; the first writer's commit still names the old row
    // and still fails, while the new row commits.
    let again = s.register_pack(&push_pack(repo, 1)).await.unwrap();
    assert_eq!(again.generation, PackGeneration::new(1));
    let r = s
        .commit_push(&PushCommit {
            repo,
            writes: vec![live(MAIN, None, oid(1), p)],
        })
        .await;
    assert!(matches!(r, Err(StoreError::Unavailable { .. })), "{r:?}");
    assert_eq!(counts(s).await, [0, 0, 0, 0]);
    let v = advance(s, repo, MAIN, oid(1), again.id).await;
    assert_eq!(v.value.pack(), Some(again.id));
}

/// A compactor whose cover pack was swept between registering it and
/// recording it on the cover gets a retryable failure, also after the
/// checksum was registered again at the next generation; the cover keeps
/// no pack.
async fn a_cover_naming_a_swept_pack_is_unavailable(c: Ctx) {
    let s = &c.store;
    let repo = new_repo(s, "acme/r").await.id;
    let t0 = c.now().0;
    let p = s.register_pack(&compaction_pack(repo, 2, 1)).await.unwrap();
    let cover = s
        .insert_cover(&new_cover(repo, 1, 1, 1, None))
        .await
        .unwrap();
    c.set_time(t0 + GRACE_US);
    assert_eq!(sweep(s).await, vec![p.id]);
    let r = s.set_cover_pack(cover.id, p.id).await;
    assert!(matches!(r, Err(StoreError::Unavailable { .. })), "{r:?}");
    let again = s.register_pack(&compaction_pack(repo, 2, 1)).await.unwrap();
    assert_eq!(again.generation, PackGeneration::new(1));
    let r = s.set_cover_pack(cover.id, p.id).await;
    assert!(matches!(r, Err(StoreError::Unavailable { .. })), "{r:?}");
    assert_eq!(s.covers(repo, &rn(MAIN)).await.unwrap()[0].pack, None);
    let set = s.set_cover_pack(cover.id, again.id).await.unwrap();
    assert_eq!(set.pack, Some(again.id));
}

/// A push that names an old unreferenced pack in a transaction that has not
/// committed yet keeps it: the sweep skips it (Postgres) or waits for the
/// push (SQLite), and either way the pack survives with its reference.
async fn the_sweep_spares_a_pack_an_open_push_is_referencing(c: Ctx) {
    let s = &c.store;
    let repo = new_repo(s, "acme/r").await.id;
    let t0 = c.now().0;
    let p = pack(s, repo, 1).await;
    c.set_time(t0 + 100 * GRACE_US);
    // Before the open transaction: on SQLite it holds the write lock.
    let lease = gc_lease(s).await;
    let mut push = raw_write_tx(s).await;
    sqlx::query(
        "INSERT INTO ref_versions (repo_id, ref_name, seq, tip_oid, closure_exact, pack_id) \
         VALUES ($1, $2, 1, $3, 1, $4)",
    )
    .bind(repo.get())
    .bind(MAIN)
    .bind(oid(1).as_bytes().to_vec())
    .bind(p.get())
    .execute(&mut *push)
    .await
    .unwrap();
    let sweeper = s.clone();
    let handle = tokio::spawn(async move { sweeper.sweep_orphan_packs(&lease, GRACE, 100).await });
    tokio::time::sleep(Duration::from_millis(300)).await;
    push.commit().await.unwrap();
    assert_eq!(handle.await.unwrap().unwrap().deleted, vec![]);
    assert_eq!(s.packs(&[p]).await.unwrap()[0].id, p);
    c.set_time(t0 + 1000 * GRACE_US);
    assert_eq!(sweep(s).await, vec![]);
}

/// The sweep is incremental (migration 0009): each batch reads at most
/// `limit` registrations after where the last one stopped, plus `limit`
/// packs of the audit, however many packs are stored, and a run of batches
/// deletes every orphan that is due, oldest registration first.
async fn the_sweep_resumes_where_it_stopped_in_bounded_batches(c: Ctx) {
    let s = &c.store;
    let repo = new_repo(s, "acme/r").await.id;
    let t0 = c.now().0;
    // 40 packs, every fourth an orphan, each registered 1 µs after the last.
    let mut orphans = Vec::new();
    let mut named = Vec::new();
    for n in 1..=40u8 {
        c.set_time(t0 + i64::from(n));
        let p = pack(s, repo, n).await;
        if n % 4 == 0 {
            orphans.push(p);
        } else {
            advance(s, repo, MAIN, oid(n), p).await;
            named.push(p);
        }
    }
    c.set_time(t0 + 100 * GRACE_US);
    let mut deleted = Vec::new();
    let mut batches = 0;
    loop {
        let b = sweep_batch(s, 6).await;
        batches += 1;
        assert!(
            b.examined <= 12,
            "a batch reads at most twice its limit: {b:?}"
        );
        // Oldest registration first: nothing is deleted out of order.
        deleted.extend(b.deleted.iter().map(|r| r.id));
        if b.caught_up {
            break;
        }
        assert!(batches < 20, "the walk does not advance");
    }
    assert_eq!(deleted, orphans);
    assert_eq!(batches, 7, "40 registrations in batches of 6");
    assert_eq!(s.packs(&named).await.unwrap().len(), named.len());
    // Caught up: a batch reads no registration again, only the audit's.
    let b = sweep_batch(s, 6).await;
    assert!(b.caught_up && b.deleted.is_empty(), "{b:?}");
    assert!(b.examined <= 6, "{b:?}");
}

/// A pack the walk passed while something named it, and that stops being
/// named during the pass, is not deleted by that pass, nor by any before
/// its new grace period ends; then it is. Eviction touches the pack, which
/// moves its registration ahead of the watermark.
async fn a_pack_unnamed_behind_the_cursor_survives_its_grace_then_goes(c: Ctx) {
    let s = &c.store;
    let repo = new_repo(s, "acme/r").await.id;
    let t0 = c.now().0;
    let base = pack(s, repo, 1).await;
    let v1 = advance(s, repo, MAIN, oid(1), base).await;
    let p = shallow_built(s, repo, 2).await.id;
    record(s, repo, v1.id, depth(1), p).await.unwrap();
    let orphan = pack(s, repo, 3).await;
    advance(s, repo, MAIN, oid(2), base).await;
    let t1 = t0 + 100 * GRACE_US;
    c.set_time(t1);
    // One registration per batch: base, then the recorded pack, both named.
    assert_eq!(sweep_rows(s, 1).await, vec![]);
    assert_eq!(sweep_rows(s, 1).await, vec![]);
    // The walk is past `p`; its recording is evicted (p is touched now).
    assert_eq!(
        s.evict_shallow_packs(Duration::ZERO, NEVER, 10)
            .await
            .unwrap(),
        1
    );
    // The rest of the pass deletes the orphan after it and spares `p`.
    let rest = sweep_rows(s, 1).await;
    assert_eq!(rest.iter().map(|r| r.id).collect::<Vec<_>>(), vec![orphan]);
    assert_eq!(sweep(s).await, vec![]);
    c.set_time(t1 + GRACE_US - 1);
    assert_eq!(sweep(s).await, vec![]);
    c.set_time(t1 + GRACE_US);
    assert_eq!(sweep(s).await, vec![p]);
}

/// On Postgres a transaction naming an orphan holds its row, so the sweep
/// skips it; the walk must not move past it, because that transaction may
/// roll back and leave the pack an orphan. Here it rolls back, and the
/// next batch deletes the pack, before the audit could come round to it.
/// (On SQLite the sweep waits for the transaction instead.)
async fn the_walk_never_passes_a_pack_an_open_transaction_holds(c: Ctx) {
    let s = &c.store;
    let repo = new_repo(s, "acme/r").await.id;
    let t0 = c.now().0;
    // Named packs with lower IDs and later registrations: the audit (one
    // pack per batch here) reads them first, the walk after the orphan.
    c.set_time(t0 + 1);
    let mut named = Vec::new();
    for n in 1..=4u8 {
        let p = pack(s, repo, n).await;
        advance(s, repo, MAIN, oid(n), p).await;
        named.push(p);
    }
    c.set_time(t0);
    let orphan = pack(s, repo, 9).await;
    c.set_time(t0 + 100 * GRACE_US);
    // Before the open transaction: on SQLite it holds the write lock.
    let lease = gc_lease(s).await;
    let mut push = raw_write_tx(s).await;
    sqlx::query(
        "INSERT INTO ref_versions (repo_id, ref_name, seq, tip_oid, closure_exact, pack_id) \
         VALUES ($1, $2, 1, $3, 1, $4)",
    )
    .bind(repo.get())
    .bind("refs/heads/topic")
    .bind(oid(9).as_bytes().to_vec())
    .bind(orphan.get())
    .execute(&mut *push)
    .await
    .unwrap();
    let sweeper = s.clone();
    let first = tokio::spawn(async move { sweeper.sweep_orphan_packs(&lease, GRACE, 1).await });
    tokio::time::sleep(Duration::from_millis(300)).await;
    push.rollback().await.unwrap();
    let first = first.await.unwrap().unwrap();
    let deleted: Vec<PackId> = match s.dialect() {
        // SQLite waited for the rollback, so the orphan was free.
        Dialect::Sqlite => first.deleted.iter().map(|r| r.id).collect(),
        Dialect::Postgres => {
            assert_eq!(first.deleted, vec![], "the row was held");
            assert!(first.caught_up, "a held first row ends the pass: {first:?}");
            sweep_rows(s, 1).await.iter().map(|r| r.id).collect()
        }
    };
    assert_eq!(deleted, vec![orphan]);
    assert_eq!(s.packs(&named).await.unwrap().len(), 4);
}

/// The audit reaches a pack with no registration time (one registered
/// before migration 0005) wherever the walk is, records the time, and the
/// walk deletes it a grace period later.
async fn the_audit_stamps_a_legacy_pack_behind_the_walk(c: Ctx) {
    let s = &c.store;
    let repo = new_repo(s, "acme/r").await.id;
    let t0 = c.now().0;
    let legacy = pack(s, repo, 1).await;
    let later = pack(s, repo, 2).await;
    advance(s, repo, MAIN, oid(2), later).await;
    sqlx::query("DELETE FROM pack_touches WHERE pack_id = $1")
        .bind(legacy.get())
        .execute(s.pool())
        .await
        .unwrap();
    let t1 = t0 + 100 * GRACE_US;
    c.set_time(t1);
    let b = sweep_batch(s, 10).await;
    assert_eq!(b.deleted, vec![]);
    assert_eq!(count(s, "pack_touches").await, 2);
    c.set_time(t1 + GRACE_US);
    assert_eq!(sweep(s).await, vec![legacy]);
}

/// One holder at a time: a live lease is refused to another owner and
/// extended for its own; it expires `ttl` after its last extension (its
/// holder died), and the next owner takes it with a new epoch. Work under
/// the old epoch is then refused and writes nothing, whatever its owner's
/// clock says.
async fn a_lease_has_one_holder_and_fences_a_stale_one(c: Ctx) {
    let s = &c.store;
    let repo = new_repo(s, "acme/r").await.id;
    let t0 = c.now().0;
    let ttl = Duration::from_secs(60);
    let a = s.acquire_lease(GC_LEASE, "a", ttl).await.unwrap().unwrap();
    assert_eq!((a.owner.as_str(), a.epoch), ("a", 1));
    assert_eq!(s.acquire_lease(GC_LEASE, "b", ttl).await.unwrap(), None);
    // Another lease name is independent.
    assert!(s.acquire_lease("other", "b", ttl).await.unwrap().is_some());
    // The holder extends it; it keeps its epoch.
    c.set_time(t0 + 50_000_000);
    assert_eq!(
        s.acquire_lease(GC_LEASE, "a", ttl).await.unwrap(),
        Some(a.clone())
    );
    c.set_time(t0 + 109_999_999);
    assert_eq!(s.acquire_lease(GC_LEASE, "b", ttl).await.unwrap(), None);

    // `a` stops (it died, or stalled): its lease runs out and `b` takes it.
    let orphan = pack(s, repo, 1).await;
    c.set_time(t0 + 110_000_000);
    let b = s.acquire_lease(GC_LEASE, "b", ttl).await.unwrap().unwrap();
    assert_eq!((b.owner.as_str(), b.epoch), ("b", 2));
    assert_eq!(s.acquire_lease(GC_LEASE, "a", ttl).await.unwrap(), None);

    // `a` resumes: its sweep is refused and deletes nothing.
    c.set_time(t0 + 110_000_000 + GRACE_US);
    let r = s.sweep_orphan_packs(&a, GRACE, 100).await;
    assert!(matches!(&r, Err(e) if e.is_lease_lost()), "{r:?}");
    assert_eq!(s.packs(&[orphan]).await.unwrap().len(), 1);
    // `b`'s sweep extends `b`'s lease and does the work.
    let swept = s.sweep_orphan_packs(&b, GRACE, 100).await.unwrap();
    assert_eq!(
        swept.deleted.iter().map(|r| r.id).collect::<Vec<_>>(),
        vec![orphan]
    );
    c.set_time(t0 + 110_000_000 + GRACE_US + 59_000_000);
    assert_eq!(s.acquire_lease(GC_LEASE, "a", ttl).await.unwrap(), None);
    // A lease that expired is refused even to its own holder's work, and
    // taking it again is a new epoch.
    c.set_time(t0 + 110_000_000 + GRACE_US + 60_000_000);
    let r = s.sweep_orphan_packs(&b, GRACE, 100).await;
    assert!(matches!(&r, Err(e) if e.is_lease_lost()), "{r:?}");
    let b2 = s.acquire_lease(GC_LEASE, "b", ttl).await.unwrap().unwrap();
    assert_eq!(b2.epoch, 3);
}

// ---------------------------------------------------------------------------
// Exact shallow packs (migration 0006, node C8)

fn depth(n: u32) -> ShallowDepth {
    ShallowDepth::new(n).unwrap()
}

/// An unused window no test reaches: recordings are evicted only once
/// superseded.
const NEVER: Duration = Duration::MAX;

/// `set_shallow_pack` with no byte cap, as the C8 cases were written: the
/// recorded row.
async fn record(
    s: &SqlStore,
    repo: RepoId,
    version: RefVersionId,
    depth: ShallowDepth,
    pack: PackId,
) -> Result<aspen_core::pack::PackRecord, StoreError> {
    match s
        .set_shallow_pack(repo, version, depth, pack, u64::MAX)
        .await?
    {
        ShallowRecorded::Recorded { pack, evicted } => {
            assert_eq!(evicted, 0, "no cap, nothing to evict");
            Ok(pack)
        }
        other => panic!("no cap, yet {other:?}"),
    }
}

/// A shallow pack as the builder registers it: a compaction pack at L1.
async fn shallow_built(s: &SqlStore, repo: RepoId, n: u8) -> aspen_core::pack::PackRecord {
    s.register_pack(&compaction_pack(repo, n, 1)).await.unwrap()
}

/// A recording reads back as the pack's row, for exactly its repository,
/// version and depth. The first recording of a version and depth wins: a
/// later one returns the first pack and changes nothing, while another
/// depth of the same version is a recording of its own.
async fn a_shallow_pack_is_recorded_once_and_read_back(c: Ctx) {
    let s = &c.store;
    let repo = new_repo(s, "acme/r").await.id;
    let other = new_repo(s, "acme/o").await.id;
    let v = advance(s, repo, MAIN, oid(1), pack(s, repo, 1).await).await;
    let p = shallow_built(s, repo, 2).await;
    let q = shallow_built(s, repo, 3).await;
    assert_eq!(s.shallow_pack(repo, v.id, depth(1)).await.unwrap(), None);

    assert_eq!(record(s, repo, v.id, depth(1), p.id).await.unwrap(), p);
    assert_eq!(
        s.shallow_pack(repo, v.id, depth(1)).await.unwrap(),
        Some(p.clone())
    );
    assert_eq!(s.shallow_pack(repo, v.id, depth(2)).await.unwrap(), None);
    assert_eq!(s.shallow_pack(other, v.id, depth(1)).await.unwrap(), None);

    // A second builder lost the race: it gets the first pack back.
    assert_eq!(record(s, repo, v.id, depth(1), q.id).await.unwrap(), p);
    assert_eq!(record(s, repo, v.id, depth(1), p.id).await.unwrap(), p);
    assert_eq!(
        s.shallow_pack(repo, v.id, depth(1)).await.unwrap(),
        Some(p.clone())
    );
    let deepest = depth(ShallowDepth::MAX);
    assert_eq!(record(s, repo, v.id, deepest, q.id).await.unwrap(), q);
    assert_eq!(s.shallow_pack(repo, v.id, deepest).await.unwrap(), Some(q));
    assert_eq!(count(s, "shallow_packs").await, 2);
}

/// A builder whose pack was swept between registering it and recording it
/// gets a retryable failure and records nothing, also after the checksum
/// was registered again at the next generation; the new row records.
async fn a_shallow_pack_naming_a_swept_pack_is_unavailable(c: Ctx) {
    let s = &c.store;
    let repo = new_repo(s, "acme/r").await.id;
    let t0 = c.now().0;
    let v = advance(s, repo, MAIN, oid(1), pack(s, repo, 1).await).await;
    let p = shallow_built(s, repo, 2).await;
    c.set_time(t0 + GRACE_US);
    assert_eq!(sweep(s).await, vec![p.id]);
    let r = record(s, repo, v.id, depth(1), p.id).await;
    assert!(matches!(r, Err(StoreError::Unavailable { .. })), "{r:?}");
    let again = shallow_built(s, repo, 2).await;
    assert_eq!(again.generation, PackGeneration::new(1));
    let r = record(s, repo, v.id, depth(1), p.id).await;
    assert!(matches!(r, Err(StoreError::Unavailable { .. })), "{r:?}");
    assert_eq!(count(s, "shallow_packs").await, 0);
    assert_eq!(
        record(s, repo, v.id, depth(1), again.id).await.unwrap(),
        again
    );
}

/// A recording ties a version and a pack of one repository: a pack or a
/// version of another repository is a conflict, also when the version and
/// depth are already recorded (where the insert itself checks nothing), and
/// an unknown version is not found. Nothing is recorded by a refusal.
async fn a_shallow_pack_stays_inside_its_repository(c: Ctx) {
    let s = &c.store;
    let a = new_repo(s, "acme/a").await.id;
    let b = new_repo(s, "acme/b").await.id;
    let va = advance(s, a, MAIN, oid(1), pack(s, a, 1).await).await;
    let vb = advance(s, b, MAIN, oid(1), pack(s, b, 1).await).await;
    let pa = shallow_built(s, a, 2).await;
    let pb = shallow_built(s, b, 2).await;
    assert!(is_conflict(record(s, a, va.id, depth(1), pb.id).await));
    assert!(is_conflict(record(s, a, vb.id, depth(1), pa.id).await));
    assert_eq!(count(s, "shallow_packs").await, 0);
    record(s, a, va.id, depth(1), pa.id).await.unwrap();
    assert!(is_conflict(record(s, b, va.id, depth(1), pb.id).await));
    assert!(is_conflict(record(s, a, va.id, depth(1), pb.id).await));
    assert!(matches!(
        record(s, a, RefVersionId::new(9999).unwrap(), depth(1), pa.id).await,
        Err(StoreError::NotFound { .. })
    ));
    assert_eq!(count(s, "shallow_packs").await, 1);
    assert_eq!(s.shallow_pack(b, va.id, depth(1)).await.unwrap(), None);
}

/// A pack that only a shallow pack recording names is never swept, however
/// old; the orphan next to it is.
async fn the_sweep_spares_a_pack_a_shallow_recording_names(c: Ctx) {
    let s = &c.store;
    let repo = new_repo(s, "acme/r").await.id;
    let t0 = c.now().0;
    let v = advance(s, repo, MAIN, oid(1), pack(s, repo, 1).await).await;
    let p = shallow_built(s, repo, 2).await;
    record(s, repo, v.id, depth(1), p.id).await.unwrap();
    let orphan = shallow_built(s, repo, 3).await;
    c.set_time(t0 + 100 * GRACE_US);
    assert_eq!(sweep(s).await, vec![orphan.id]);
    c.set_time(t0 + 1000 * GRACE_US);
    assert_eq!(sweep(s).await, vec![]);
    assert_eq!(s.shallow_pack(repo, v.id, depth(1)).await.unwrap(), Some(p));
}

/// Eviction deletes recordings made at least `older_than` ago (by the
/// store's clock) whose version is no longer its ref's current version,
/// oldest first, at most `limit`, and returns how many it deleted. A
/// recording of a current version stays at any age, and is evicted once
/// its ref moves on.
async fn eviction_takes_old_recordings_of_versions_no_longer_current(c: Ctx) {
    let s = &c.store;
    let repo = new_repo(s, "acme/r").await.id;
    let t0 = c.now().0;
    let base = pack(s, repo, 1).await;
    let v1 = advance(s, repo, MAIN, oid(1), base).await;
    let v2 = advance(s, repo, MAIN, oid(2), base).await;
    let p = shallow_built(s, repo, 2).await.id;
    record(s, repo, v1.id, depth(1), p).await.unwrap();
    record(s, repo, v2.id, depth(1), p).await.unwrap();
    c.set_time(t0 + 1);
    record(s, repo, v1.id, depth(2), p).await.unwrap();

    let evict = |limit| s.evict_shallow_packs(GRACE, NEVER, limit);
    c.set_time(t0 + GRACE_US - 1);
    assert_eq!(evict(10).await.unwrap(), 0, "1 µs too young");
    c.set_time(t0 + GRACE_US + 1);
    assert_eq!(evict(0).await.unwrap(), 0);
    // The oldest first.
    assert_eq!(evict(1).await.unwrap(), 1);
    assert_eq!(s.shallow_pack(repo, v1.id, depth(1)).await.unwrap(), None);
    assert!(
        s.shallow_pack(repo, v1.id, depth(2))
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(evict(10).await.unwrap(), 1);
    assert_eq!(s.shallow_pack(repo, v1.id, depth(2)).await.unwrap(), None);

    // main is at v2: its recording stays, however old.
    c.set_time(t0 + 1000 * GRACE_US);
    assert_eq!(evict(10).await.unwrap(), 0);
    assert_eq!(
        s.evict_shallow_packs(Duration::ZERO, NEVER, 10)
            .await
            .unwrap(),
        0
    );
    assert!(
        s.shallow_pack(repo, v2.id, depth(1))
            .await
            .unwrap()
            .is_some()
    );
    // A deletion moves main on, and v2 goes too.
    committed(s, repo, vec![delete(MAIN, v2.id)]).await;
    assert_eq!(evict(10).await.unwrap(), 1);
    assert_eq!(count(s, "shallow_packs").await, 0);
}

/// An evicted recording's pack is swept only a whole grace period after the
/// eviction, however old the pack's registration: a fetch that read the
/// recording just before may still be streaming the pack.
async fn an_evicted_shallow_pack_is_swept_a_grace_after_eviction(c: Ctx) {
    let s = &c.store;
    let repo = new_repo(s, "acme/r").await.id;
    let t0 = c.now().0;
    let base = pack(s, repo, 1).await;
    let v1 = advance(s, repo, MAIN, oid(1), base).await;
    let p = shallow_built(s, repo, 2).await.id;
    record(s, repo, v1.id, depth(1), p).await.unwrap();
    advance(s, repo, MAIN, oid(2), base).await;
    let t1 = t0 + 100 * GRACE_US;
    c.set_time(t1);
    assert_eq!(sweep(s).await, vec![]);
    assert_eq!(
        s.evict_shallow_packs(Duration::ZERO, NEVER, 10)
            .await
            .unwrap(),
        1
    );
    assert_eq!(sweep(s).await, vec![]);
    c.set_time(t1 + GRACE_US - 1);
    assert_eq!(sweep(s).await, vec![]);
    c.set_time(t1 + GRACE_US);
    assert_eq!(sweep(s).await, vec![p]);
}

// ---------------------------------------------------------------------------
// Bounds on exact shallow packs (migration 0007, issue #108)

/// Stored bytes of every test pack (`push_pack`): pack, `.idx` and entry
/// table.
const B: u64 = 100 + 1100 + 96;

/// `set_shallow_pack` under a byte cap: the recorded pack and how many
/// recordings it evicted.
async fn record_capped(
    s: &SqlStore,
    repo: RepoId,
    version: RefVersionId,
    d: u32,
    pack: PackId,
    cap: u64,
) -> (PackId, u64) {
    match s
        .set_shallow_pack(repo, version, depth(d), pack, cap)
        .await
        .unwrap()
    {
        ShallowRecorded::Recorded { pack, evicted } => (pack.id, evicted),
        other => panic!("{other:?}"),
    }
}

/// The distinct packs `repo`'s recordings name, and their stored bytes
/// counted once each.
async fn recorded_bytes(s: &SqlStore, repo: RepoId) -> (BTreeSet<i64>, u64) {
    let rows = sqlx::query(
        "SELECT DISTINCT s.pack_id AS pack_id, p.size + p.idx_size + p.entries_size AS bytes \
         FROM shallow_packs s JOIN packs p ON p.id = s.pack_id WHERE s.repo_id = $1",
    )
    .bind(repo.get())
    .fetch_all(s.pool())
    .await
    .unwrap();
    let mut packs = BTreeSet::new();
    let mut bytes = 0;
    for r in rows {
        packs.insert(r.get::<i64, _>("pack_id"));
        bytes += r.get::<i64, _>("bytes") as u64;
    }
    (packs, bytes)
}

async fn used_at(s: &SqlStore, version: RefVersionId, d: u32) -> Option<i64> {
    sqlx::query("SELECT used_at_us FROM shallow_packs WHERE version_id = $1 AND depth = $2")
        .bind(version.get())
        .bind(i64::from(d))
        .fetch_optional(s.pool())
        .await
        .unwrap()
        .map(|r| r.get::<i64, _>("used_at_us"))
}

/// A repository's recorded packs stay within its cap: a recording that
/// does not fit evicts the least recently used pack first (last use being
/// its latest use or recording), with every recording that names it, and
/// counts a pack named twice once. Another repository's recordings are
/// neither counted nor evicted. An evicted pack is swept only a whole grace
/// period after the eviction.
async fn the_byte_cap_evicts_the_least_recently_used_packs_first(c: Ctx) {
    let s = &c.store;
    let repo = new_repo(s, "acme/r").await.id;
    let other = new_repo(s, "acme/o").await.id;
    let t0 = c.now().0;
    let base = pack(s, repo, 1).await;
    let v1 = advance(s, repo, MAIN, oid(1), base).await;
    let v2 = advance(s, repo, "refs/heads/b", oid(2), base).await;
    let ov = advance(s, other, MAIN, oid(1), pack(s, other, 1).await).await;
    let o = shallow_built(s, other, 2).await.id;
    let [p1, p2, p3, p4, p5] = [
        shallow_built(s, repo, 2).await.id,
        shallow_built(s, repo, 3).await.id,
        shallow_built(s, repo, 4).await.id,
        shallow_built(s, repo, 5).await.id,
        shallow_built(s, repo, 6).await.id,
    ];
    let cap = 3 * B;
    record_capped(s, other, ov.id, 1, o, B).await;

    c.set_time(t0 + 1);
    assert_eq!(record_capped(s, repo, v1.id, 1, p1, cap).await, (p1, 0));
    // The same pack for another version: counted once.
    assert_eq!(record_capped(s, repo, v2.id, 1, p1, cap).await, (p1, 0));
    c.set_time(t0 + 2);
    assert_eq!(record_capped(s, repo, v1.id, 2, p2, cap).await, (p2, 0));
    c.set_time(t0 + 3);
    assert_eq!(record_capped(s, repo, v1.id, 3, p3, cap).await, (p3, 0));
    assert_eq!(recorded_bytes(s, repo).await.1, 3 * B, "p1 counted once");

    // p1 is used again, so p2 is now the least recently used.
    c.set_time(t0 + 4);
    s.mark_shallow_pack_used(repo, v2.id, depth(1))
        .await
        .unwrap();
    let t_evict = t0 + 5;
    c.set_time(t_evict);
    assert_eq!(record_capped(s, repo, v1.id, 4, p4, cap).await, (p4, 1));
    assert_eq!(s.shallow_pack(repo, v1.id, depth(2)).await.unwrap(), None);
    let (packs, bytes) = recorded_bytes(s, repo).await;
    assert_eq!(packs, BTreeSet::from([p1.get(), p3.get(), p4.get()]));
    assert_eq!(bytes, cap);

    // p3 is the least recently used now. Then, under a cap of 2 B, p1 goes
    // with both of its recordings, and p4 after it.
    c.set_time(t0 + 6);
    assert_eq!(record_capped(s, repo, v1.id, 5, p5, cap).await, (p5, 1));
    assert_eq!(s.shallow_pack(repo, v1.id, depth(3)).await.unwrap(), None);
    c.set_time(t0 + 7);
    assert_eq!(
        record_capped(s, repo, v1.id, 6, p2, 2 * B).await,
        (p2, 3),
        "p1's two recordings, then p4"
    );
    let (packs, bytes) = recorded_bytes(s, repo).await;
    assert_eq!(packs, BTreeSet::from([p2.get(), p5.get()]));
    assert_eq!(bytes, 2 * B);
    assert_eq!(s.shallow_pack(repo, v2.id, depth(1)).await.unwrap(), None);
    // The other repository was neither counted nor touched.
    assert!(
        s.shallow_pack(other, ov.id, depth(1))
            .await
            .unwrap()
            .is_some()
    );

    // Recording an already-recorded pair changes nothing, even past the cap.
    assert_eq!(record_capped(s, repo, v1.id, 5, p5, 1).await, (p5, 0));
    assert_eq!(recorded_bytes(s, repo).await.1, 2 * B);

    // Evicted packs keep their blobs a whole grace period after their
    // eviction, however old their registration.
    c.set_time(t_evict + GRACE_US - 1);
    let swept = sweep(s).await;
    assert!(!swept.contains(&p2), "p2 is recorded again");
    assert!(!swept.contains(&p3) && !swept.contains(&p1) && !swept.contains(&p4));
    c.set_time(t0 + 7 + GRACE_US);
    let mut swept = sweep(s).await;
    swept.sort();
    assert_eq!(swept, vec![p1, p3, p4]);
}

/// A pack larger than the cap on its own is not recorded and evicts
/// nothing; a pack exactly at the cap is recorded alone.
async fn a_pack_over_the_cap_alone_is_not_recorded(c: Ctx) {
    let s = &c.store;
    let repo = new_repo(s, "acme/r").await.id;
    let v = advance(s, repo, MAIN, oid(1), pack(s, repo, 1).await).await;
    let p = shallow_built(s, repo, 2).await.id;
    let q = shallow_built(s, repo, 3).await.id;
    record_capped(s, repo, v.id, 1, p, B).await;
    assert_eq!(
        s.set_shallow_pack(repo, v.id, depth(2), q, B - 1)
            .await
            .unwrap(),
        ShallowRecorded::OverLimit {
            bytes: B,
            limit: B - 1
        }
    );
    assert_eq!(s.shallow_pack(repo, v.id, depth(2)).await.unwrap(), None);
    assert!(
        s.shallow_pack(repo, v.id, depth(1))
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(count(s, "shallow_packs").await, 1);
    // Exactly the cap: p makes room.
    assert_eq!(record_capped(s, repo, v.id, 2, q, B).await, (q, 1));
    assert_eq!(recorded_bytes(s, repo).await.1, B);
}

/// Recordings no fetch has used for `unused_after` are evicted, of current
/// versions too; a use (or the recording itself) restarts the window, never
/// moves last use backwards, and a use of a missing recording is no error.
/// The evicted pack keeps its blobs a grace period after the eviction.
async fn unused_recordings_are_evicted_current_or_not(c: Ctx) {
    let s = &c.store;
    let repo = new_repo(s, "acme/r").await.id;
    let t0 = c.now().0;
    let unused = Duration::from_secs(100);
    let u = 100_000_000;
    let v = advance(s, repo, MAIN, oid(1), pack(s, repo, 1).await).await;
    let p = shallow_built(s, repo, 2).await.id;
    let q = shallow_built(s, repo, 3).await.id;
    record(s, repo, v.id, depth(1), p).await.unwrap();
    record(s, repo, v.id, depth(2), q).await.unwrap();
    assert_eq!(used_at(s, v.id, 1).await, Some(t0));
    let evict = || s.evict_shallow_packs(NEVER, unused, 10);

    c.set_time(t0 + u - 1);
    assert_eq!(evict().await.unwrap(), 0, "1 µs too young");
    s.mark_shallow_pack_used(repo, v.id, depth(1))
        .await
        .unwrap();
    assert_eq!(used_at(s, v.id, 1).await, Some(t0 + u - 1));
    // A clock behind the last use leaves it alone.
    c.set_time(t0 + 5);
    s.mark_shallow_pack_used(repo, v.id, depth(1))
        .await
        .unwrap();
    assert_eq!(used_at(s, v.id, 1).await, Some(t0 + u - 1));
    // No recording at depth 3: nothing happens.
    s.mark_shallow_pack_used(repo, v.id, depth(3))
        .await
        .unwrap();
    assert_eq!(count(s, "shallow_packs").await, 2);

    // q was not used: it goes although v is current; p stays.
    let t1 = t0 + u;
    c.set_time(t1);
    assert_eq!(evict().await.unwrap(), 1);
    assert_eq!(s.shallow_pack(repo, v.id, depth(2)).await.unwrap(), None);
    assert!(
        s.shallow_pack(repo, v.id, depth(1))
            .await
            .unwrap()
            .is_some()
    );
    c.set_time(t0 + 2 * u - 2);
    assert_eq!(evict().await.unwrap(), 0);
    c.set_time(t0 + 2 * u - 1);
    assert_eq!(evict().await.unwrap(), 1);
    assert_eq!(count(s, "shallow_packs").await, 0);

    // q's blobs stay a grace period after its eviction.
    c.set_time(t1 + GRACE_US - 1);
    assert!(!sweep(s).await.contains(&q));
    c.set_time(t1 + GRACE_US);
    assert!(sweep(s).await.contains(&q));
}

/// Concurrent recordings of one repository never together overshoot its
/// cap: each sees the bytes the others recorded (on Postgres they wait for
/// the repository's row lock; on SQLite for the write lock).
async fn concurrent_recordings_never_overshoot_the_cap(c: Ctx) {
    let s = &c.store;
    let repo = new_repo(s, "acme/r").await.id;
    let v = advance(s, repo, MAIN, oid(1), pack(s, repo, 1).await).await;
    let mut packs = Vec::new();
    for n in 0..16u8 {
        packs.push(shallow_built(s, repo, 10 + n).await.id);
    }
    let cap = 3 * B;
    for round in 0..4u32 {
        let mut tasks = Vec::new();
        for (i, p) in packs.iter().enumerate() {
            let (s, p) = (s.clone(), *p);
            let d = round * 16 + i as u32 + 1;
            tasks.push(tokio::spawn(async move {
                s.set_shallow_pack(repo, v.id, depth(d), p, cap).await
            }));
        }
        for t in tasks {
            assert!(matches!(
                t.await.unwrap().unwrap(),
                ShallowRecorded::Recorded { .. }
            ));
        }
        let (named, bytes) = recorded_bytes(s, repo).await;
        assert!(bytes <= cap, "round {round}: {bytes} > {cap} ({named:?})");
        assert_eq!(bytes, cap, "round {round}: the cap is filled");
    }
}

/// On Postgres, the lock a recording takes on its repository's row
/// (`FOR NO KEY UPDATE`) makes no push, ref update, job enqueue, pack
/// registration or compaction write wait: those only take the `FOR KEY
/// SHARE` lock of their foreign keys to `repos`, which does not conflict
/// with it. Another recording of the same repository does wait, which is
/// what keeps the byte cap. SQLite serializes every writer with
/// `BEGIN IMMEDIATE` anyway, so the case is about Postgres only.
async fn a_recordings_repository_lock_never_blocks_pushes_or_compaction(c: Ctx) {
    let s = &c.store;
    if s.dialect() != Dialect::Postgres {
        return;
    }
    let repo = new_repo(s, "acme/r").await.id;
    let v1 = advance(s, repo, MAIN, oid(1), pack(s, repo, 1).await).await;
    let shallow = shallow_built(s, repo, 20).await.id;
    // Every lock wait on this store fails almost at once.
    let probe = SqlStore::connect_with(
        &c.url,
        &StoreOptions {
            lock_timeout: Duration::from_millis(1),
            ..StoreOptions::default()
        },
    )
    .await
    .unwrap();

    // The lock exactly as `set_shallow_pack` takes it, held open.
    let mut holder = raw_write_tx(s).await;
    sqlx::query("SELECT id FROM repos WHERE id = $1 FOR NO KEY UPDATE")
        .bind(repo.get())
        .fetch_one(&mut *holder)
        .await
        .unwrap();

    // A push: its pack, then a new version of an existing ref, a new ref,
    // and the commit index rows and jobs it writes.
    let p2 = pack(&probe, repo, 2).await;
    let v2 = advance(&probe, repo, MAIN, oid(2), p2).await;
    assert_eq!(v2.parent, Some(v1.id));
    advance(&probe, repo, "refs/heads/topic", oid(3), p2).await;
    probe
        .enqueue_jobs(&[new_job(repo, JobKind::RewritePack { pack: p2 }, 0)])
        .await
        .unwrap();
    // Compaction: a compacted pack and a cover.
    probe
        .register_pack(&compaction_pack(repo, 30, 1))
        .await
        .unwrap();
    probe
        .insert_cover(&new_cover(repo, 1, 1, 2, None))
        .await
        .unwrap();
    // A served fetch marking a recording used.
    probe
        .mark_shallow_pack_used(repo, v1.id, depth(1))
        .await
        .unwrap();
    // Another recording of the same repository waits, and here gives up.
    let blocked = probe
        .set_shallow_pack(repo, v2.id, depth(1), shallow, u64::MAX)
        .await;
    assert!(
        matches!(blocked, Err(StoreError::Unavailable { .. })),
        "{blocked:?}"
    );

    holder.rollback().await.unwrap();
    assert!(matches!(
        probe
            .set_shallow_pack(repo, v2.id, depth(1), shallow, u64::MAX)
            .await
            .unwrap(),
        ShallowRecorded::Recorded { .. }
    ));
    probe.pool().close().await;
}

// Replacing a pack with its base-first rewrite (reason-machines/aspen#10)

/// `replace_pack` moves every ref version, cover and shallow pack
/// recording naming the old pack to the new one, and nothing else: other
/// packs' rows, and the `rewrite_pack` job on the old pack, keep their
/// pack. `pack_references` counts what names a pack. The old pack is
/// touched, so the sweep keeps it a whole grace period even once no job
/// names it, and a repeat of the swap moves nothing.
async fn replace_pack_moves_every_reference_in_one_transaction(c: Ctx) {
    let s = &c.store;
    let repo = new_repo(s, "acme/r").await.id;
    let t0 = c.now().0;

    // Level 0: two versions of main name `old`, a third names `other`.
    let old = pack(s, repo, 1).await;
    let other = pack(s, repo, 2).await;
    let v1 = advance(s, repo, MAIN, oid(1), old).await;
    let v2 = advance(s, repo, MAIN, oid(2), old).await;
    let v3 = advance(s, repo, MAIN, oid(3), other).await;
    let new = pack(s, repo, 3).await;
    let job = s
        .enqueue_jobs(&[new_job(repo, JobKind::RewritePack { pack: old }, 0)])
        .await
        .unwrap()[0];
    assert_eq!(
        s.pack_references(old).await.unwrap(),
        PackReferences {
            versions: 2,
            covers: 0,
            shallow_packs: 0
        }
    );
    assert!(s.pack_references(new).await.unwrap().is_empty());

    c.set_time(t0 + 5 * GRACE_US);
    let moved = s.replace_pack(old, new).await.unwrap();
    assert_eq!(
        moved,
        PackReferences {
            versions: 2,
            covers: 0,
            shallow_packs: 0
        }
    );
    for (v, want) in [(v1.id, new), (v2.id, new), (v3.id, other)] {
        let got = s.ref_version(v).await.unwrap().unwrap();
        assert_eq!(got.value.pack(), Some(want), "version {v}");
    }
    assert!(s.pack_references(old).await.unwrap().is_empty());
    assert_eq!(s.pack_references(new).await.unwrap().versions, 2);
    // The job keeps naming the pack it repaired.
    let j = s
        .claim_job(&[JobTag::RewritePack], "w", Duration::from_secs(60))
        .await
        .unwrap()
        .unwrap();
    assert_eq!((j.id, j.kind), (job, JobKind::RewritePack { pack: old }));
    // A repeat after a lost answer moves nothing.
    assert!(s.replace_pack(old, new).await.unwrap().is_empty());

    // Level 1: a cover and two shallow recordings name `l1_old`.
    let cover = s
        .insert_cover(&new_cover(repo, 1, 1, 16, None))
        .await
        .unwrap();
    let l1_old = s
        .register_pack(&compaction_pack(repo, 10, 1))
        .await
        .unwrap()
        .id;
    let l1_new = s
        .register_pack(&compaction_pack(repo, 11, 1))
        .await
        .unwrap()
        .id;
    s.set_cover_pack(cover.id, l1_old).await.unwrap();
    record(s, repo, v1.id, depth(1), l1_old).await.unwrap();
    record(s, repo, v3.id, depth(2), l1_old).await.unwrap();
    let moved = s.replace_pack(l1_old, l1_new).await.unwrap();
    assert_eq!(
        moved,
        PackReferences {
            versions: 0,
            covers: 1,
            shallow_packs: 2
        }
    );
    assert_eq!(
        s.covers(repo, &rn(MAIN)).await.unwrap()[0].pack,
        Some(l1_new)
    );
    for (v, d) in [(v1.id, 1), (v3.id, 2)] {
        let got = s.shallow_pack(repo, v, depth(d)).await.unwrap().unwrap();
        assert_eq!(got.id, l1_new);
    }

    // Nothing names `l1_old` and no job does; the swap touched it, so the
    // sweep keeps it a whole grace period after the swap, then deletes it.
    let swap = c.now().0;
    c.set_time(swap + GRACE_US - 1);
    let swept = sweep(s).await;
    assert!(!swept.contains(&l1_old), "{swept:?}");
    c.set_time(swap + GRACE_US + 1);
    let swept = sweep(s).await;
    assert!(swept.contains(&l1_old), "{swept:?}");
    // `old` is still named by its job, so it stays at any age.
    assert!(!swept.contains(&old), "{swept:?}");
    for p in [new, l1_new, other] {
        assert!(!swept.contains(&p), "{p} is named: {swept:?}");
    }
}

/// `replace_pack` refuses, writing nothing, a replacement of another
/// repository or level, the same pack, and a missing old pack; a
/// replacement the sweep took after its registration is retryable.
async fn replace_pack_refuses_a_mismatched_or_swept_replacement(c: Ctx) {
    let s = &c.store;
    let repo = new_repo(s, "acme/r").await.id;
    let elsewhere = new_repo(s, "acme/o").await.id;
    let t0 = c.now().0;
    let old = pack(s, repo, 1).await;
    let v = advance(s, repo, MAIN, oid(1), old).await;
    let foreign = pack(s, elsewhere, 2).await;
    let higher = s
        .register_pack(&compaction_pack(repo, 3, 1))
        .await
        .unwrap()
        .id;
    for bad in [foreign, higher, old] {
        assert!(is_conflict(s.replace_pack(old, bad).await), "{bad}");
    }
    let missing = PackId::new(1_000_000).unwrap();
    assert!(matches!(
        s.replace_pack(missing, old).await,
        Err(StoreError::NotFound { .. })
    ));
    let swept = pack(s, repo, 4).await;
    c.set_time(t0 + 2 * GRACE_US);
    assert!(sweep(s).await.contains(&swept));
    let r = s.replace_pack(old, swept).await;
    assert!(matches!(&r, Err(e) if e.is_pack_swept()), "{r:?}");
    assert_eq!(
        s.ref_version(v.id).await.unwrap().unwrap().value.pack(),
        Some(old)
    );
    assert_eq!(s.pack_references(old).await.unwrap().versions, 1);
}
