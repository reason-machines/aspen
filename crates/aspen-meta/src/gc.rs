//! Pack registration and garbage collection of orphaned packs:
//! [`MetaStore::register_pack`] and [`MetaStore::sweep_orphan_packs`]
//! (migration 0005, issue #48).
//!
//! A pack is registered before the push that uploaded it commits a version
//! naming it, so between the two the pack is unreferenced on purpose. A push
//! that loses its compare-and-swap, fails, or dies leaves it unreferenced
//! for good. The sweep tells the two apart by time: `pack_touches` records
//! when each pack was last registered (by the store's clock), and a pack is
//! deleted only once nothing names it and its last registration is at least
//! the grace period old. A ref version, a cover, a job and a shallow pack
//! recording (migration 0006, `src/shallow.rs`) each name a pack. Evicting
//! a shallow pack recording touches its pack like a registration does, so a
//! fetch that read the recording just before has a whole grace period to
//! finish streaming the pack.
//!
//! # Races, and why none deletes a referenced pack
//!
//! On SQLite every write transaction is `BEGIN IMMEDIATE`, so the sweep,
//! registrations and pushes run one at a time and each sees the others'
//! committed state.
//!
//! On Postgres (read committed), anything that makes a row name a pack (a
//! version inserted by `commit_push`, a cover's pack, a job, a shallow pack
//! recording) checks its foreign key into `packs`, which takes a
//! `FOR KEY SHARE` lock on the pack's row until that transaction ends.
//! Registration takes the same lock on an existing row, and
//! `set_cover_pack` and `set_shallow_pack` take it before they write. The
//! sweep:
//!
//! 1. Picks candidates with `FOR UPDATE OF p SKIP LOCKED`. `FOR UPDATE`
//!    conflicts with `FOR KEY SHARE`, so a pack that an open transaction is
//!    referencing or re-registering is skipped, and once the sweep holds the
//!    lock, no transaction can add a reference to the pack (its foreign key
//!    check waits) until the sweep has ended.
//! 2. Re-checks every condition in the `DELETE` statements themselves.
//!    These run after the lock is held, with a new snapshot, so they see
//!    every reference and registration committed before the lock was taken,
//!    which the candidate query's older snapshot might not have.
//!
//! A transaction that waited on the sweep's lock then finds the row gone:
//! its foreign key check fails, or (in `commit_push`, `register_pack`,
//! `set_cover_pack` and `set_shallow_pack`) its own locking read of the row
//! returns nothing. `commit_push`, `set_cover_pack` and `set_shallow_pack`
//! report a pack that is gone as [`StoreError::Unavailable`] (retryable),
//! and `register_pack` inserts a new row, which the sweep's tombstone makes
//! the next generation.
//!
//! Eviction of a shallow pack recording deletes the recording and upserts
//! the pack's touch in one transaction. A sweep whose snapshot still sees
//! the recording does not pick the pack; one that picks it after the
//! eviction committed sees the new touch, and on Postgres a sweep `DELETE`
//! that meets the touch row while the eviction is writing it waits and then
//! re-checks its condition against the new time. Either way the pack stays
//! for a grace period after the eviction.
//!
//! # Generations, and why a late blob deletion is harmless
//!
//! The sweep deletes rows; their blobs are deleted afterwards, outside any
//! transaction, by whoever runs garbage collection, and possibly much later
//! (a paused VM, a retry storm). Each deleted row leaves a tombstone naming
//! its `(repo, checksum, generation)`, pending until the blobs are gone. A
//! registration of the same checksum reads the highest tombstoned
//! generation after its insert (on Postgres the insert waits for a sweep
//! deleting the old row, which holds its unique index entry, so only a
//! statement after it sees the tombstone) and takes the next one, whose
//! keys differ ([`aspen_core::pack::PackKeys`]). Writers upload only after
//! registering, so every blob a live row names was uploaded under keys no
//! tombstone names, and a deletion can never remove it, however late. Row
//! IDs are never reused (SQLite `AUTOINCREMENT`, a Postgres identity), so a
//! writer that registered a row and commits by its ID commits that
//! generation or nothing.
//!
//! [`MetaStore::register_pack`]: aspen_core::meta::MetaStore::register_pack
//! [`MetaStore::sweep_orphan_packs`]: aspen_core::meta::MetaStore::sweep_orphan_packs

use std::time::Duration;

use aspen_core::error::StoreError;
use aspen_core::id::{PackChecksum, PackId, RepoId, UnixMicros};
use aspen_core::meta::PackTombstone;
use aspen_core::pack::{NewPack, PackGeneration, PackKeys, PackRecord};
use sqlx::AssertSqlSafe;

use crate::error::{conflict, corrupt, store_error};
use crate::rows::{self, PACK_COLS};
use crate::sql::{CHUNK, placeholders};
use crate::store::{Dialect, SqlStore};
use crate::tx::WriteTx;

/// `FOR KEY SHARE` on Postgres; nothing on SQLite. Locks a `packs` row the
/// way a foreign key check does: it blocks the sweep's `FOR UPDATE` and its
/// `DELETE` for the rest of the transaction.
pub(crate) fn key_share(store: &SqlStore) -> &'static str {
    match store.dialect() {
        Dialect::Postgres => " FOR KEY SHARE",
        Dialect::Sqlite => "",
    }
}

/// The pack row with this checksum, locked against the sweep on Postgres.
async fn find(
    store: &SqlStore,
    tx: &mut WriteTx,
    repo: RepoId,
    checksum: &PackChecksum,
) -> Result<Option<PackRecord>, StoreError> {
    let sql = format!(
        "SELECT {PACK_COLS} FROM packs WHERE repo_id = $1 AND checksum = $2{}",
        key_share(store)
    );
    sqlx::query(AssertSqlSafe(sql))
        .bind(repo.get())
        .bind(checksum.as_bytes().to_vec())
        .fetch_optional(&mut **tx)
        .await
        .map_err(store_error)?
        .as_ref()
        .map(rows::pack)
        .transpose()
}

pub(crate) async fn register_pack(
    store: &SqlStore,
    pack: &NewPack,
) -> Result<PackRecord, StoreError> {
    pack.validate()
        .map_err(|e| conflict(format!("pack {}: {e}", pack.checksum)))?;
    store
        .write(async |tx| {
            let now = store.now();
            let record = match find(store, tx, pack.repo, &pack.checksum).await? {
                Some(existing) => same_or_conflict(existing, pack)?,
                None => match insert(tx, pack).await? {
                    Some(id) => {
                        // After the insert, not before: on Postgres a sweep
                        // deleting this checksum's old row holds its unique
                        // index entry, so the insert waits for the sweep to
                        // commit, and only a statement after it sees the
                        // sweep's tombstone.
                        let generation = next_generation(tx, pack).await?;
                        if generation != PackGeneration::FIRST {
                            set_generation(tx, id, pack, generation).await?;
                        }
                        find(store, tx, pack.repo, &pack.checksum)
                            .await?
                            .filter(|r| r.id == id)
                            .ok_or_else(|| corrupt(format!("pack {id} vanished in its insert")))?
                    }
                    // A concurrent registration inserted the row after our
                    // read and has committed (the insert waited for it).
                    None => {
                        let existing = find(store, tx, pack.repo, &pack.checksum)
                            .await?
                            .ok_or_else(|| StoreError::Unavailable {
                                reason: format!(
                                    "pack {} was registered and removed concurrently",
                                    pack.checksum
                                ),
                            })?;
                        same_or_conflict(existing, pack)?
                    }
                },
            };
            touch(tx, record.id, pack.repo, now).await?;
            Ok(record)
        })
        .await
}

/// Inserts the row; `None` if a row with the checksum exists.
async fn insert(tx: &mut WriteTx, pack: &NewPack) -> Result<Option<PackId>, StoreError> {
    let keys = PackKeys::new(pack.repo, &pack.checksum, PackGeneration::FIRST);
    let received = match pack.origin {
        aspen_core::pack::PackOrigin::Push { received_bytes } => {
            Some(i64::try_from(received_bytes).expect("validated"))
        }
        aspen_core::pack::PackOrigin::Compaction { .. } => None,
    };
    let int = |v: u64| i64::try_from(v).expect("validated against DB_MAX");
    let inserted = sqlx::query(
        "INSERT INTO packs (repo_id, checksum, pack_key, size, idx_key, idx_size, entries_key, \
         entries_size, object_count, ref_delta_count, level, origin, received_bytes) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13) \
         ON CONFLICT (repo_id, checksum) DO NOTHING RETURNING id",
    )
    .bind(pack.repo.get())
    .bind(pack.checksum.as_bytes().to_vec())
    .bind(keys.pack.as_str().to_owned())
    .bind(int(pack.size))
    .bind(keys.idx.as_str().to_owned())
    .bind(int(pack.idx_size))
    .bind(keys.entries.as_str().to_owned())
    .bind(int(pack.entries_size))
    .bind(i64::from(pack.object_count))
    .bind(i64::from(pack.ref_delta_count))
    .bind(i64::from(pack.level().get()))
    .bind(pack.origin.as_str())
    .bind(received)
    .fetch_optional(&mut **tx)
    .await
    .map_err(store_error)?;
    inserted
        .map(|row| {
            let id: i64 = rows::get(&row, "id")?;
            PackId::new(id).map_err(|e| corrupt(format!("packs.id: {e}")))
        })
        .transpose()
}

/// The generation a new row for `pack`'s checksum gets: one past the
/// highest the sweep has deleted, or the first.
async fn next_generation(tx: &mut WriteTx, pack: &NewPack) -> Result<PackGeneration, StoreError> {
    let row = sqlx::query(
        "SELECT MAX(generation) AS g FROM pack_tombstones WHERE repo_id = $1 AND checksum = $2",
    )
    .bind(pack.repo.get())
    .bind(pack.checksum.as_bytes().to_vec())
    .fetch_one(&mut **tx)
    .await
    .map_err(store_error)?;
    let Some(last) = rows::get::<Option<i64>>(&row, "g")? else {
        return Ok(PackGeneration::FIRST);
    };
    u32::try_from(last)
        .ok()
        .and_then(|g| PackGeneration::new(g).next())
        .ok_or_else(|| {
            conflict(format!(
                "pack {} has been garbage-collected {last} times; no generation is left",
                pack.checksum
            ))
        })
}

/// Moves a just-inserted row to `generation`, with its keys.
async fn set_generation(
    tx: &mut WriteTx,
    id: PackId,
    pack: &NewPack,
    generation: PackGeneration,
) -> Result<(), StoreError> {
    let keys = PackKeys::new(pack.repo, &pack.checksum, generation);
    sqlx::query(
        "UPDATE packs SET generation = $1, pack_key = $2, idx_key = $3, entries_key = $4 \
         WHERE id = $5",
    )
    .bind(i64::from(generation.get()))
    .bind(keys.pack.as_str().to_owned())
    .bind(keys.idx.as_str().to_owned())
    .bind(keys.entries.as_str().to_owned())
    .bind(id.get())
    .execute(&mut **tx)
    .await
    .map_err(store_error)?;
    Ok(())
}

/// Records `now` as the pack's last registration; never moves it back (two
/// servers' clocks may disagree a little).
pub(crate) async fn touch(
    tx: &mut WriteTx,
    id: PackId,
    repo: RepoId,
    now: UnixMicros,
) -> Result<(), StoreError> {
    sqlx::query(
        "INSERT INTO pack_touches (pack_id, repo_id, touched_at_us) VALUES ($1, $2, $3) \
         ON CONFLICT (pack_id) DO UPDATE SET touched_at_us = CASE \
         WHEN excluded.touched_at_us > pack_touches.touched_at_us \
         THEN excluded.touched_at_us ELSE pack_touches.touched_at_us END",
    )
    .bind(id.get())
    .bind(repo.get())
    .bind(now.0)
    .execute(&mut **tx)
    .await
    .map_err(store_error)?;
    Ok(())
}

/// The existing row if it describes the same pack.
fn same_or_conflict(existing: PackRecord, pack: &NewPack) -> Result<PackRecord, StoreError> {
    if same_pack(&existing.pack, pack) {
        Ok(existing)
    } else {
        Err(conflict(format!(
            "pack {} is registered with different sizes or origin",
            pack.checksum
        )))
    }
}

/// Whether two registrations describe the same stored pack. Everything
/// derived from the bytes must match; `received_bytes` may differ, because
/// two pushes can send different packs that complete to the same bytes.
fn same_pack(a: &NewPack, b: &NewPack) -> bool {
    a.repo == b.repo
        && a.checksum == b.checksum
        && a.size == b.size
        && a.idx_size == b.idx_size
        && a.entries_size == b.entries_size
        && a.object_count == b.object_count
        && a.ref_delta_count == b.ref_delta_count
        && a.origin.as_str() == b.origin.as_str()
        && a.level() == b.level()
}

/// `NOT EXISTS` conditions: no version, cover, job or shallow pack
/// recording names the pack whose ID is in column `col`. Any job counts,
/// done or not: its foreign key keeps the row anyway.
fn unreferenced(col: &str) -> String {
    format!(
        "NOT EXISTS (SELECT 1 FROM ref_versions v WHERE v.pack_id = {col}) \
         AND NOT EXISTS (SELECT 1 FROM covers c WHERE c.pack_id = {col}) \
         AND NOT EXISTS (SELECT 1 FROM jobs j WHERE j.pack_id = {col}) \
         AND NOT EXISTS (SELECT 1 FROM shallow_packs s WHERE s.pack_id = {col})"
    )
}

/// Microseconds in `d`, saturating.
pub(crate) fn micros(d: Duration) -> i64 {
    i64::try_from(d.as_micros()).unwrap_or(i64::MAX)
}

pub(crate) async fn sweep_orphan_packs(
    store: &SqlStore,
    grace: Duration,
    limit: u32,
) -> Result<Vec<PackRecord>, StoreError> {
    let lock = match store.dialect() {
        Dialect::Postgres => " FOR UPDATE OF p SKIP LOCKED",
        Dialect::Sqlite => "",
    };
    store
        .write(async |tx| {
            let now = store.now().0;
            let cutoff = now.saturating_sub(micros(grace));
            if limit == 0 {
                return Ok(Vec::new());
            }

            // Step 1: candidates, never seen or registered at least `grace`
            // ago, locked (module docs). Young touched orphans are left out,
            // so they cannot use up the limit while older ones wait.
            let sql = format!(
                "SELECT p.id, p.repo_id, t.touched_at_us FROM packs p \
                 LEFT JOIN pack_touches t ON t.pack_id = p.id \
                 WHERE (t.pack_id IS NULL OR t.touched_at_us <= $1) AND {} \
                 ORDER BY p.id LIMIT $2{lock}",
                unreferenced("p.id")
            );
            let rows = sqlx::query(AssertSqlSafe(sql))
                .bind(cutoff)
                .bind(i64::from(limit))
                .fetch_all(&mut **tx)
                .await
                .map_err(store_error)?;
            let mut first_seen = Vec::new();
            let mut aged = Vec::new();
            for row in &rows {
                let id: i64 = rows::get(row, "id")?;
                let repo: i64 = rows::get(row, "repo_id")?;
                let touched: Option<i64> = rows::get(row, "touched_at_us")?;
                match touched {
                    None => first_seen.push((id, repo)),
                    Some(_) => aged.push(id),
                }
            }

            // A pack with no registration time (registered before migration
            // 0005) gets one now and is deleted by a later sweep.
            for (id, repo) in first_seen {
                sqlx::query(
                    "INSERT INTO pack_touches (pack_id, repo_id, touched_at_us) \
                     VALUES ($1, $2, $3) ON CONFLICT (pack_id) DO NOTHING",
                )
                .bind(id)
                .bind(repo)
                .bind(now)
                .execute(&mut **tx)
                .await
                .map_err(store_error)?;
            }

            // Step 2: delete, re-checking every condition with a fresh
            // snapshot (module docs). The touch row goes first: its foreign
            // key names the pack.
            let mut deleted = Vec::new();
            for chunk in aged.chunks(CHUNK) {
                let sql = format!(
                    "DELETE FROM pack_touches WHERE touched_at_us <= $1 AND pack_id IN ({}) \
                     AND {} RETURNING pack_id",
                    placeholders(2, chunk.len()),
                    unreferenced("pack_touches.pack_id")
                );
                let mut q = sqlx::query(AssertSqlSafe(sql)).bind(cutoff);
                for id in chunk {
                    q = q.bind(*id);
                }
                let untouched: Vec<i64> = q
                    .fetch_all(&mut **tx)
                    .await
                    .map_err(store_error)?
                    .iter()
                    .map(|r| rows::get(r, "pack_id"))
                    .collect::<Result<_, _>>()?;
                if untouched.is_empty() {
                    continue;
                }
                let sql = format!(
                    "DELETE FROM packs WHERE id IN ({}) AND {} RETURNING {PACK_COLS}",
                    placeholders(1, untouched.len()),
                    unreferenced("packs.id")
                );
                let mut q = sqlx::query(AssertSqlSafe(sql));
                for id in &untouched {
                    q = q.bind(*id);
                }
                for row in q.fetch_all(&mut **tx).await.map_err(store_error)? {
                    deleted.push(rows::pack(&row)?);
                }
            }

            for p in &deleted {
                sqlx::query(
                    "INSERT INTO pack_tombstones (repo_id, checksum, generation, swept_at_us) \
                     VALUES ($1, $2, $3, $4) ON CONFLICT (repo_id, checksum, generation) \
                     DO NOTHING",
                )
                .bind(p.pack.repo.get())
                .bind(p.pack.checksum.as_bytes().to_vec())
                .bind(i64::from(p.generation.get()))
                .bind(now)
                .execute(&mut **tx)
                .await
                .map_err(store_error)?;
            }
            deleted.sort_by_key(|p| p.id);
            Ok(deleted)
        })
        .await
}

pub(crate) async fn pending_pack_deletes(
    store: &SqlStore,
    limit: u32,
) -> Result<Vec<PackTombstone>, StoreError> {
    let rows = sqlx::query(
        "SELECT repo_id, checksum, generation FROM pack_tombstones \
         WHERE deleted_at_us IS NULL \
         ORDER BY swept_at_us, repo_id, checksum, generation LIMIT $1",
    )
    .bind(i64::from(limit))
    .fetch_all(&mut *store.admit().await?)
    .await
    .map_err(store_error)?;
    rows.iter().map(tombstone).collect()
}

fn tombstone(row: &sqlx::any::AnyRow) -> Result<PackTombstone, StoreError> {
    let repo: i64 = rows::get(row, "repo_id")?;
    let checksum: Vec<u8> = rows::get(row, "checksum")?;
    let generation: i64 = rows::get(row, "generation")?;
    Ok(PackTombstone {
        repo: RepoId::new(repo).map_err(|e| corrupt(format!("pack_tombstones.repo_id: {e}")))?,
        checksum: PackChecksum::from_slice(&checksum)
            .map_err(|e| corrupt(format!("pack_tombstones.checksum: {e}")))?,
        generation: u32::try_from(generation)
            .map(PackGeneration::new)
            .map_err(|e| corrupt(format!("pack_tombstones.generation: {e}")))?,
    })
}

pub(crate) async fn finish_pack_deletes(
    store: &SqlStore,
    done: &[PackTombstone],
) -> Result<(), StoreError> {
    if done.is_empty() {
        return Ok(());
    }
    store
        .write(async |tx| {
            let now = store.now().0;
            for t in done {
                sqlx::query(
                    "UPDATE pack_tombstones SET deleted_at_us = $1 \
                     WHERE repo_id = $2 AND checksum = $3 AND generation = $4 \
                     AND deleted_at_us IS NULL",
                )
                .bind(now)
                .bind(t.repo.get())
                .bind(t.checksum.as_bytes().to_vec())
                .bind(i64::from(t.generation.get()))
                .execute(&mut **tx)
                .await
                .map_err(store_error)?;
            }
            Ok(())
        })
        .await
}
