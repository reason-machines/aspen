//! Exact shallow packs: [`MetaStore::shallow_pack`],
//! [`MetaStore::set_shallow_pack`] and [`MetaStore::evict_shallow_packs`]
//! (migration 0006, node C8).
//!
//! A `shallow_packs` row records the pack that answers a shallow fetch of
//! one ref version at one depth. The row names its pack, so the sweep
//! (`src/gc.rs`) leaves the pack alone while the row exists. Recording a
//! pack locks its row against the sweep before inserting, as
//! [`MetaStore::set_cover_pack`] does, so a pack the sweep has taken is
//! never recorded: the writer gets [`StoreError::Unavailable`] and builds
//! the pack again.
//!
//! Eviction deletes old rows of versions that are no longer current. A
//! fetch that read a row just before it was evicted may still be streaming
//! the pack's blobs, so eviction touches each evicted pack (sets its
//! `pack_touches` time to now) in the same transaction: the sweep then
//! keeps the pack for a whole grace period after the eviction, exactly as
//! it keeps a freshly registered pack.
//!
//! # Bounds (issue #108)
//!
//! Eviction also deletes rows, of current versions too, that no fetch has
//! used for a configured time (`used_at_us`, migration 0007, kept by
//! [`MetaStore::mark_shallow_pack_used`]). And each repository's recorded
//! packs stay under a byte cap: [`MetaStore::set_shallow_pack`] counts the
//! stored bytes of every distinct pack the repository's rows name (a pack
//! named by several rows counts once), and when the new pack does not fit
//! it deletes every row naming the least recently used pack, then the
//! next, until it does, touching each evicted pack as eviction does. A
//! pack larger than the cap on its own is not recorded.
//!
//! Recordings of one repository are serialized, so two builds cannot both
//! see room for their pack and together overshoot the cap: on Postgres the
//! transaction takes the repository's row `FOR NO KEY UPDATE` (which does
//! not conflict with the `FOR KEY SHARE` lock a push's foreign keys take on
//! it), and on SQLite every write transaction is `BEGIN IMMEDIATE` already.
//!
//! [`MetaStore::shallow_pack`]: aspen_core::meta::MetaStore::shallow_pack
//! [`MetaStore::set_shallow_pack`]: aspen_core::meta::MetaStore::set_shallow_pack
//! [`MetaStore::evict_shallow_packs`]: aspen_core::meta::MetaStore::evict_shallow_packs
//! [`MetaStore::mark_shallow_pack_used`]: aspen_core::meta::MetaStore::mark_shallow_pack_used
//! [`MetaStore::set_cover_pack`]: aspen_core::meta::MetaStore::set_cover_pack

use std::collections::BTreeSet;
use std::time::Duration;

use aspen_core::error::StoreError;
use aspen_core::id::{PackId, RefVersionId, RepoId, UnixMicros};
use aspen_core::meta::{ShallowDepth, ShallowRecorded};
use aspen_core::pack::PackRecord;
use sqlx::{Any, AssertSqlSafe, Executor};

use crate::error::{conflict, corrupt, store_error};
use crate::gc::{key_share, micros, touch};
use crate::rows::{self, PACK_COLS};
use crate::store::{Dialect, SqlStore};
use crate::tx::WriteTx;

/// The pack recorded for `(version, depth)` in `repo`, through any
/// executor.
async fn recorded<'e, E>(
    executor: E,
    repo: RepoId,
    version: RefVersionId,
    depth: ShallowDepth,
) -> Result<Option<PackRecord>, StoreError>
where
    E: Executor<'e, Database = Any>,
{
    let sql = format!(
        "SELECT {PACK_COLS} FROM packs WHERE id = (SELECT pack_id FROM shallow_packs \
         WHERE version_id = $1 AND depth = $2 AND repo_id = $3)"
    );
    sqlx::query(AssertSqlSafe(sql))
        .bind(version.get())
        .bind(i64::from(depth.get()))
        .bind(repo.get())
        .fetch_optional(executor)
        .await
        .map_err(store_error)?
        .as_ref()
        .map(rows::pack)
        .transpose()
}

pub(crate) async fn shallow_pack(
    store: &SqlStore,
    repo: RepoId,
    version: RefVersionId,
    depth: ShallowDepth,
) -> Result<Option<PackRecord>, StoreError> {
    recorded(&mut *store.admit().await?, repo, version, depth).await
}

pub(crate) async fn set_shallow_pack(
    store: &SqlStore,
    repo: RepoId,
    version: RefVersionId,
    depth: ShallowDepth,
    pack: PackId,
    max_repo_bytes: u64,
) -> Result<ShallowRecorded, StoreError> {
    let what = || format!("shallow pack of version {version} at depth {depth}");
    store
        .write(async |tx| {
            let now = store.now().0;
            // The pack row, locked against the sweep on Postgres as a
            // foreign key check would lock it. A pack the sweep deleted
            // after the builder registered it is a retryable failure: the
            // builder registers the pack anew.
            let sql = format!(
                "SELECT repo_id FROM packs WHERE id = $1{}",
                key_share(store)
            );
            let owner = sqlx::query(AssertSqlSafe(sql))
                .bind(pack.get())
                .fetch_optional(&mut **tx)
                .await
                .map_err(store_error)?;
            let owner = owner
                .as_ref()
                .map(|r| rows::get::<i64>(r, "repo_id"))
                .transpose()?;
            match owner {
                None => {
                    return Err(StoreError::pack_swept(format_args!(
                        "{}: pack {pack} was deleted before it was recorded",
                        what()
                    )));
                }
                Some(owner) if owner != repo.get() => {
                    return Err(conflict(format!(
                        "{}: pack {pack} is not a pack of repository {repo}",
                        what()
                    )));
                }
                Some(_) => {}
            }
            // Checked here, not only by shallow_packs_version_fk: when the
            // pair is already recorded, the insert below does nothing and
            // checks no foreign key.
            let owner = sqlx::query("SELECT repo_id FROM ref_versions WHERE id = $1")
                .bind(version.get())
                .fetch_optional(&mut **tx)
                .await
                .map_err(store_error)?;
            let owner = owner
                .as_ref()
                .map(|r| rows::get::<i64>(r, "repo_id"))
                .transpose()?;
            match owner {
                None => {
                    return Err(StoreError::NotFound {
                        what: format!("ref version {version}"),
                    });
                }
                Some(owner) if owner != repo.get() => {
                    return Err(conflict(format!(
                        "{}: version {version} is not a version of repository {repo}",
                        what()
                    )));
                }
                Some(_) => {}
            }
            // From here on, recordings of this repository run one at a
            // time (module docs), so the bytes counted below stay true
            // until this transaction commits.
            let sql = format!("SELECT id FROM repos WHERE id = $1{}", repo_lock(store));
            sqlx::query(AssertSqlSafe(sql))
                .bind(repo.get())
                .fetch_optional(&mut **tx)
                .await
                .map_err(store_error)?;
            // First recording wins: one made meanwhile committed before the
            // lock above was granted, and this statement sees it.
            if let Some(first) = recorded(&mut **tx, repo, version, depth).await? {
                return Ok(ShallowRecorded::Recorded {
                    pack: first,
                    evicted: 0,
                });
            }
            let sql = format!("SELECT {PACK_COLS} FROM packs WHERE id = $1");
            let row = sqlx::query(AssertSqlSafe(sql))
                .bind(pack.get())
                .fetch_one(&mut **tx)
                .await
                .map_err(store_error)?;
            let bytes = rows::pack(&row)?.pack.stored_bytes();
            if bytes > max_repo_bytes {
                return Ok(ShallowRecorded::OverLimit {
                    bytes,
                    limit: max_repo_bytes,
                });
            }
            let evicted = make_room(tx, repo, pack, bytes, max_repo_bytes, now).await?;
            sqlx::query(
                "INSERT INTO shallow_packs \
                 (repo_id, version_id, depth, pack_id, recorded_at_us, used_at_us) \
                 VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (version_id, depth) DO NOTHING",
            )
            .bind(repo.get())
            .bind(version.get())
            .bind(i64::from(depth.get()))
            .bind(pack.get())
            .bind(now)
            .bind(now)
            .execute(&mut **tx)
            .await
            .map_err(store_error)?;
            let pack = recorded(&mut **tx, repo, version, depth)
                .await?
                .ok_or_else(|| corrupt(format!("{} vanished in its insert", what())))?;
            Ok(ShallowRecorded::Recorded { pack, evicted })
        })
        .await
}

/// `FOR NO KEY UPDATE` on Postgres, nothing on SQLite: serializes the
/// recordings of one repository without blocking its pushes (module docs).
fn repo_lock(store: &SqlStore) -> &'static str {
    match store.dialect() {
        Dialect::Postgres => " FOR NO KEY UPDATE",
        Dialect::Sqlite => "",
    }
}

/// Evicts `repo`'s least recently used packs (every recording naming one)
/// until `adding` more bytes fit under `limit`, and touches each evicted
/// pack so the sweep keeps it a whole grace period. `new` is the pack about
/// to be recorded: when a recording already names it, it adds nothing and
/// is never evicted to make room for itself. Returns how many recordings it
/// deleted.
async fn make_room(
    tx: &mut WriteTx,
    repo: RepoId,
    new: PackId,
    adding: u64,
    limit: u64,
    now: i64,
) -> Result<u64, StoreError> {
    // One row per distinct pack, least recently used first.
    let named = sqlx::query(
        "SELECT s.pack_id AS pack_id, p.size + p.idx_size + p.entries_size AS bytes \
         FROM shallow_packs s JOIN packs p ON p.id = s.pack_id \
         WHERE s.repo_id = $1 \
         GROUP BY s.pack_id, p.size, p.idx_size, p.entries_size \
         ORDER BY MAX(s.used_at_us), s.pack_id",
    )
    .bind(repo.get())
    .fetch_all(&mut **tx)
    .await
    .map_err(store_error)?;
    let mut held = Vec::with_capacity(named.len());
    let mut total = 0u64;
    let mut adding = adding;
    for row in &named {
        let id: i64 = rows::get(row, "pack_id")?;
        let bytes: i64 = rows::get(row, "bytes")?;
        let bytes =
            u64::try_from(bytes).map_err(|_| corrupt(format!("pack {id} has a negative size")))?;
        total = total.saturating_add(bytes);
        if id == new.get() {
            adding = 0;
        } else {
            held.push((id, bytes));
        }
    }
    let mut evicted = 0u64;
    for (id, bytes) in held {
        if total.saturating_add(adding) <= limit {
            break;
        }
        evicted += sqlx::query("DELETE FROM shallow_packs WHERE repo_id = $1 AND pack_id = $2")
            .bind(repo.get())
            .bind(id)
            .execute(&mut **tx)
            .await
            .map_err(store_error)?
            .rows_affected();
        total -= bytes;
        let pack = PackId::new(id).map_err(|e| corrupt(format!("shallow_packs.pack_id: {e}")))?;
        // As eviction does (module docs): a fetch that read the recording
        // just before may still be streaming the pack.
        touch(tx, pack, repo, UnixMicros(now)).await?;
    }
    Ok(evicted)
}

pub(crate) async fn mark_shallow_pack_used(
    store: &SqlStore,
    repo: RepoId,
    version: RefVersionId,
    depth: ShallowDepth,
) -> Result<(), StoreError> {
    store
        .write(async |tx| {
            let now = store.now().0;
            sqlx::query(
                "UPDATE shallow_packs SET used_at_us = $1 \
                 WHERE used_at_us < $2 AND version_id = $3 AND depth = $4 AND repo_id = $5",
            )
            .bind(now)
            .bind(now)
            .bind(version.get())
            .bind(i64::from(depth.get()))
            .bind(repo.get())
            .execute(&mut **tx)
            .await
            .map_err(store_error)?;
            Ok(())
        })
        .await
}

pub(crate) async fn evict_shallow_packs(
    store: &SqlStore,
    superseded_after: Duration,
    unused_after: Duration,
    limit: u32,
) -> Result<u64, StoreError> {
    if limit == 0 {
        return Ok(0);
    }
    // Superseded long enough (no refs row points at the version: it is no
    // longer its ref's current version, and never will be again, as a ref
    // only moves to a new version), or unused long enough, current or not.
    // $1 is the superseded cutoff and $2 the unused one in both statements.
    let due = |t: &str| {
        format!(
            "(({t}.recorded_at_us <= $1 AND NOT EXISTS \
             (SELECT 1 FROM refs r WHERE r.version_id = {t}.version_id)) \
             OR {t}.used_at_us <= $2)"
        )
    };
    store
        .write(async |tx| {
            let now = store.now().0;
            let cutoff = now.saturating_sub(micros(superseded_after));
            let unused_cutoff = now.saturating_sub(micros(unused_after));
            // Oldest first, locked on Postgres so that two evictions do not
            // count one row twice. SQLite has no DELETE ... LIMIT by
            // default, so the keys are picked first on both dialects.
            let sql = format!(
                "SELECT s.version_id, s.depth FROM shallow_packs s WHERE {} \
                 ORDER BY s.recorded_at_us, s.version_id, s.depth LIMIT $3{}",
                due("s"),
                store.skip_locked()
            );
            let picked = sqlx::query(AssertSqlSafe(sql))
                .bind(cutoff)
                .bind(unused_cutoff)
                .bind(i64::from(limit))
                .fetch_all(&mut **tx)
                .await
                .map_err(store_error)?;

            let mut evicted = 0u64;
            let mut packs = BTreeSet::new();
            for row in &picked {
                let version: i64 = rows::get(row, "version_id")?;
                let depth: i64 = rows::get(row, "depth")?;
                let sql = format!(
                    "DELETE FROM shallow_packs WHERE {} AND version_id = $3 AND depth = $4 \
                     RETURNING pack_id, repo_id",
                    due("shallow_packs")
                );
                let deleted = sqlx::query(AssertSqlSafe(sql))
                    .bind(cutoff)
                    .bind(unused_cutoff)
                    .bind(version)
                    .bind(depth)
                    .fetch_optional(&mut **tx)
                    .await
                    .map_err(store_error)?;
                if let Some(row) = deleted {
                    evicted += 1;
                    let pack: i64 = rows::get(&row, "pack_id")?;
                    let repo: i64 = rows::get(&row, "repo_id")?;
                    packs.insert((pack, repo));
                }
            }

            // Restart each evicted pack's grace period (module docs). The
            // pack row still exists: the row just deleted named it, and the
            // sweep never deletes a named pack.
            for (pack, repo) in packs {
                let pack = PackId::new(pack)
                    .map_err(|e| corrupt(format!("shallow_packs.pack_id: {e}")))?;
                let repo = RepoId::new(repo)
                    .map_err(|e| corrupt(format!("shallow_packs.repo_id: {e}")))?;
                touch(tx, pack, repo, UnixMicros(now)).await?;
            }
            Ok(evicted)
        })
        .await
}
