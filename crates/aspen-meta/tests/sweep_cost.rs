//! The cost of garbage collection's sweep at scale (throughput item 7).
//!
//! The sweep before migration 0009 picked its candidates with one query
//! over every pack (`packs` joined to `pack_touches`, four "nothing names
//! it" checks per row), so once orphans were rare each pass read every
//! pack: the maintainers' AWS load test measured 7.9 s per run on 1.5 million pack
//! rows, on every instance at once. The sweep now reads registrations in
//! the order their grace periods end, after where it stopped, in batches
//! of at most `limit` (plus `limit` packs of a slow audit), under a lease.
//!
//! Each test seeds `N` push packs, every one registered long ago and named
//! by a ref version of its own except every 1,000th, then:
//!
//! 1. times the old candidate query (the sweep's first statement on
//!    `main` before this change, verbatim) with orphans due, and again once
//!    none are left, which is the steady state that read every pack;
//! 2. runs the new sweep, batch after batch, until it has caught up, while
//!    concurrent writers register packs and commit ref versions, and
//!    checks every batch read at most twice its limit, every orphan is
//!    gone, no named pack is, and no write failed;
//! 3. times one caught-up batch, the steady state.
//!
//! Required CI runs `N` = 20,000 on SQLite and PostgreSQL and checks, on
//! both, that the two scans are index range scans (the plans). Nightly runs
//! `N` = 1,000,000 (the tests marked `#[ignore]`) and also checks that the
//! caught-up batch costs less than a tenth of the old query. Run one with
//! the numbers printed:
//!
//! ```text
//! cargo test --release -p aspen-meta --test sweep_cost -- --ignored --nocapture million_postgres
//! ```

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use aspen_core::id::{ObjectId, PackChecksum, RepoId};
use aspen_core::meta::{GC_LEASE, MetaStore, PushCommit, RefWrite};
use aspen_core::name::{RefName, RepoName};
use aspen_core::pack::{NewPack, PackOrigin, entry_table_len};
use aspen_core::refs::{Closure, Deps, RefValue};
use aspen_core::repo::RepoSettings;
use aspen_meta::{Dialect, SWEEP_AUDIT_SQL, SWEEP_WALK_SQL, SqlStore};
use sqlx::{AssertSqlSafe, Row as _};

use common::PgCluster;

/// Every this many seeded packs, one is an orphan.
const ORPHAN_EVERY: i64 = 1000;
/// Registrations per sweep batch: `CompactConfig`'s default.
const LIMIT: u32 = 1000;
const GRACE: Duration = Duration::from_secs(10);
/// When the first seeded pack was registered (store clock, microseconds):
/// long ago. Pack `i` was registered `i` microseconds later.
const SEEDED_AT: i64 = 1_000_000;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sweep_cost_is_bounded_sqlite() {
    let dir = tempfile::tempdir().unwrap();
    let url = sqlite_url(&dir, "small.db");
    measure(&url, 20_000, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sweep_cost_is_bounded_postgres() {
    let Some(bin) = common::pg_or_skip() else {
        return;
    };
    let cluster = PgCluster::start(&bin);
    measure(&cluster.url(), 20_000, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "nightly: seeds a million packs"]
async fn sweep_cost_is_bounded_at_a_million_packs_sqlite() {
    let dir = tempfile::tempdir().unwrap();
    let url = sqlite_url(&dir, "million.db");
    measure(&url, 1_000_000, true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "nightly: seeds a million packs"]
async fn sweep_cost_is_bounded_at_a_million_packs_postgres() {
    let Some(bin) = common::pg_or_skip() else {
        return;
    };
    let cluster = PgCluster::start(&bin);
    measure(&cluster.url(), 1_000_000, true).await;
}

fn sqlite_url(dir: &tempfile::TempDir, name: &str) -> String {
    format!("sqlite://{}?mode=rwc", dir.path().join(name).display())
}

async fn measure(url: &str, n: i64, compare_time: bool) {
    let store = SqlStore::connect(url).await.unwrap();
    store.migrate().await.unwrap();
    let dialect = store.dialect();
    let repo = store
        .create_repo(
            &RepoName::new("acme/big").unwrap(),
            &RepoSettings::new(RefName::new("refs/heads/main").unwrap()).unwrap(),
        )
        .await
        .unwrap()
        .id;
    let seeding = Instant::now();
    seed(&store, repo, n).await;
    eprintln!("{dialect:?}: seeded {n} packs in {:?}", seeding.elapsed());
    check_plans(&store).await;

    let old_due = old_query(&store).await;

    // The sweep on a store of its own, as `aspen serve` gives it a pool of
    // its own on Postgres; writers on the other.
    let gc = SqlStore::connect(url).await.unwrap();
    let lease = gc
        .acquire_lease(GC_LEASE, "sweep-cost", Duration::from_secs(3600))
        .await
        .unwrap()
        .unwrap();
    let done = Arc::new(AtomicBool::new(false));
    let pushed = Arc::new(AtomicU64::new(0));
    let writers: Vec<_> = (0..4u32)
        .map(|w| {
            let (store, done, pushed) = (store.clone(), done.clone(), pushed.clone());
            tokio::spawn(async move { write_until(store, repo, w, done, pushed).await })
        })
        .collect();

    let started = Instant::now();
    let (mut batches, mut deleted, mut slowest, mut most) = (0u64, 0u64, Duration::ZERO, 0u64);
    loop {
        let begun = Instant::now();
        let b = gc.sweep_orphan_packs(&lease, GRACE, LIMIT).await.unwrap();
        let took = begun.elapsed();
        batches += 1;
        deleted += b.deleted.len() as u64;
        slowest = slowest.max(took);
        most = most.max(b.examined);
        assert!(
            b.examined <= 2 * u64::from(LIMIT),
            "batch {batches} read {} rows",
            b.examined
        );
        if b.caught_up {
            break;
        }
        assert!(
            batches <= 2 * n as u64 / u64::from(LIMIT) + 10,
            "the sweep does not catch up"
        );
    }
    let catch_up = started.elapsed();
    done.store(true, Ordering::SeqCst);
    let mut write_errors = Vec::new();
    for w in writers {
        write_errors.extend(w.await.unwrap());
    }
    let pushes = pushed.load(Ordering::SeqCst);

    assert_eq!(
        deleted,
        (n / ORPHAN_EVERY) as u64,
        "every orphan, nothing else"
    );
    assert_eq!(
        count(&store, "packs").await,
        n - n / ORPHAN_EVERY + pushes as i64
    );
    assert!(pushes > 0, "the writers ran during the sweep");
    assert!(
        write_errors.is_empty(),
        "{} of {pushes} writes failed during the sweep: {:?}",
        write_errors.len(),
        &write_errors[..write_errors.len().min(5)]
    );

    // Caught up: the steady state of every later pass.
    let begun = Instant::now();
    let steady = gc.sweep_orphan_packs(&lease, GRACE, LIMIT).await.unwrap();
    let steady_time = begun.elapsed();
    assert!(steady.caught_up && steady.deleted.is_empty(), "{steady:?}");
    // The audit's window, plus any write made during a long catch-up that
    // has come due since.
    assert!(steady.examined <= 2 * u64::from(LIMIT), "{steady:?}");
    let old_steady = old_query(&store).await;

    eprintln!(
        "{dialect:?}, {n} packs: old candidate query {old_due:?} with orphans due, \
         {old_steady:?} with none (every pass); new sweep caught up in {batches} batches \
         over {catch_up:?} (slowest {slowest:?}, at most {most} rows), {pushes} writes \
         alongside, none failed; caught-up batch {steady_time:?} ({} rows)",
        steady.examined
    );
    if compare_time {
        assert!(
            steady_time * 10 < old_steady,
            "a caught-up batch ({steady_time:?}) is not a tenth of the old query ({old_steady:?})"
        );
    }
}

/// `n` push packs registered at [`SEEDED_AT`], each named by a ref version
/// of its own (one branch per pack) except every [`ORPHAN_EVERY`]th.
async fn seed(store: &SqlStore, repo: RepoId, n: i64) {
    let (series, checksum, hex) = match store.dialect() {
        Dialect::Postgres => (
            "SELECT i FROM generate_series(1, $2) AS i",
            "decode(lpad(to_hex(i), 40, '0'), 'hex')",
            "encode(checksum, 'hex')",
        ),
        Dialect::Sqlite => (
            "WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM s WHERE i < $2) \
             SELECT i FROM s",
            "randomblob(20)",
            "lower(hex(checksum))",
        ),
    };
    let sql = format!(
        "INSERT INTO packs (repo_id, checksum, pack_key, size, idx_key, idx_size, entries_key, \
         entries_size, object_count, ref_delta_count, level, origin, received_bytes) \
         SELECT $1, {checksum}, 'k', 32, 'k', 1100, 'k', 96, 1, 0, 0, 'push', 32 \
         FROM ({series}) AS seq"
    );
    let pool = store.pool();
    sqlx::query(AssertSqlSafe(sql))
        .bind(repo.get())
        .bind(n)
        .execute(pool)
        .await
        .unwrap();
    // Readers check that a row's keys are its checksum's.
    let keys = format!(
        "UPDATE packs SET \
         pack_key = 'repos/' || repo_id || '/packs/' || {hex} || '.pack', \
         idx_key = 'repos/' || repo_id || '/packs/' || {hex} || '.idx', \
         entries_key = 'repos/' || repo_id || '/packs/' || {hex} || '.entries'"
    );
    sqlx::query(AssertSqlSafe(keys))
        .execute(pool)
        .await
        .unwrap();
    // One registration per microsecond, as pushes register them.
    sqlx::query(
        "INSERT INTO pack_touches (pack_id, repo_id, touched_at_us) \
         SELECT id, repo_id, $1 + id FROM packs",
    )
    .bind(SEEDED_AT)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO ref_versions (repo_id, ref_name, seq, tip_oid, closure_exact, pack_id) \
         SELECT repo_id, 'refs/heads/b' || id, 1, checksum, 1, id FROM packs \
         WHERE id % $1 <> 0",
    )
    .bind(ORPHAN_EVERY)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query("ANALYZE").execute(pool).await.unwrap();
}

/// The sweep's candidate query as it was before migration 0009 (crates/
/// aspen-meta/src/gc.rs on `main` at 5c49ec0), timed in a transaction
/// that is rolled back. Its `LIMIT` stops it after 100 candidates; once no
/// orphan is due, it reads every pack.
async fn old_query(store: &SqlStore) -> Duration {
    let lock = match store.dialect() {
        Dialect::Postgres => " FOR UPDATE OF p SKIP LOCKED",
        Dialect::Sqlite => "",
    };
    let col = "p.id";
    let sql = format!(
        "SELECT p.id, p.repo_id, t.touched_at_us FROM packs p \
         LEFT JOIN pack_touches t ON t.pack_id = p.id \
         WHERE (t.pack_id IS NULL OR t.touched_at_us <= $1) AND \
         NOT EXISTS (SELECT 1 FROM ref_versions v WHERE v.pack_id = {col}) \
         AND NOT EXISTS (SELECT 1 FROM covers c WHERE c.pack_id = {col}) \
         AND NOT EXISTS (SELECT 1 FROM jobs j WHERE j.pack_id = {col}) \
         AND NOT EXISTS (SELECT 1 FROM shallow_packs s WHERE s.pack_id = {col}) \
         ORDER BY p.id LIMIT $2{lock}"
    );
    let mut tx = store.pool().begin().await.unwrap();
    let begun = Instant::now();
    sqlx::query(AssertSqlSafe(sql))
        .bind(i64::MAX / 2)
        .bind(100i64)
        .fetch_all(&mut *tx)
        .await
        .unwrap();
    let took = begun.elapsed();
    tx.rollback().await.unwrap();
    took
}

/// Both scans of a sweep batch read an index range, not a table: the walk
/// uses `pack_touches_due`, the audit the `packs` primary key.
async fn check_plans(store: &SqlStore) {
    let walk = SWEEP_WALK_SQL
        .replace("$1", "0")
        .replace("$2", "0")
        .replace("$3", "9000000000000000")
        .replace("$4", "1000");
    // Deep in the table, where a scan from the start would show.
    let audit = SWEEP_AUDIT_SQL.replace("$1", "15000").replace("$2", "1000");
    for (what, sql, index) in [
        ("walk", walk, "pack_touches_due"),
        ("audit", audit, "packs_pkey"),
    ] {
        let plan = explain(store, &sql).await;
        eprintln!("{:?} plan of the {what}:\n{plan}", store.dialect());
        match store.dialect() {
            Dialect::Postgres => {
                assert!(plan.contains(index), "{what}: no scan of {index}:\n{plan}");
                assert!(
                    !plan.contains("Seq Scan"),
                    "{what}: a sequential scan:\n{plan}"
                );
                assert!(
                    !plan.contains("Sort"),
                    "{what}: sorts instead of reading in order:\n{plan}"
                );
                // A merge or hash join reads the other table from its start.
                for join in ["Merge", "Hash"] {
                    assert!(!plan.contains(join), "{what}: a {join} join:\n{plan}");
                }
            }
            Dialect::Sqlite => {
                // SQLite names a table's rowid key "INTEGER PRIMARY KEY".
                let index = if what == "audit" {
                    "INTEGER PRIMARY KEY"
                } else {
                    index
                };
                assert!(
                    plan.contains(index),
                    "{what}: no search of {index}:\n{plan}"
                );
                assert!(!plan.contains("TEMP B-TREE"), "{what}: sorts:\n{plan}");
                assert!(!plan.contains("SCAN "), "{what}: a full scan:\n{plan}");
            }
        }
    }
}

async fn explain(store: &SqlStore, sql: &str) -> String {
    let (prefix, col) = match store.dialect() {
        Dialect::Postgres => ("EXPLAIN ", 0),
        Dialect::Sqlite => ("EXPLAIN QUERY PLAN ", 3),
    };
    sqlx::query(AssertSqlSafe(format!("{prefix}{sql}")))
        .fetch_all(store.pool())
        .await
        .unwrap()
        .iter()
        .map(|r| r.get::<String, _>(col))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Registers a new pack and commits a branch naming it, again and again,
/// until `done`, as concurrent pushes do; returns every error.
async fn write_until(
    store: SqlStore,
    repo: RepoId,
    writer: u32,
    done: Arc<AtomicBool>,
    pushed: Arc<AtomicU64>,
) -> Vec<String> {
    let mut errors = Vec::new();
    let mut i = 0u32;
    while !done.load(Ordering::SeqCst) {
        i += 1;
        let mut sum = [0xee; 20];
        sum[..4].copy_from_slice(&writer.to_be_bytes());
        sum[4..8].copy_from_slice(&i.to_be_bytes());
        let pack = NewPack {
            repo,
            checksum: PackChecksum::from_bytes(sum),
            size: 100,
            idx_size: 1072 + 28,
            entries_size: entry_table_len(1),
            object_count: 1,
            ref_delta_count: 0,
            origin: PackOrigin::Push { received_bytes: 80 },
        };
        let result = async {
            let row = store.register_pack(&pack).await?;
            store
                .commit_push(&PushCommit {
                    repo,
                    writes: vec![RefWrite {
                        name: RefName::new(format!("refs/heads/w{writer}-{i}")).unwrap(),
                        expected: None,
                        value: RefValue::Live {
                            tip: ObjectId::from_bytes(sum),
                            pack: Some(row.id),
                            closure: Closure::Exact,
                        },
                        deps: Deps::default(),
                        commits: Vec::new(),
                        jobs: Vec::new(),
                    }],
                })
                .await
        }
        .await;
        match result {
            Ok(aspen_core::meta::CommitOutcome::Committed(_)) => {
                pushed.fetch_add(1, Ordering::SeqCst);
            }
            Ok(other) => errors.push(format!("{other:?}")),
            Err(e) => errors.push(e.to_string()),
        }
    }
    errors
}

async fn count(store: &SqlStore, table: &str) -> i64 {
    sqlx::query(AssertSqlSafe(format!("SELECT COUNT(*) AS n FROM {table}")))
        .fetch_one(store.pool())
        .await
        .unwrap()
        .get::<i64, _>("n")
}
