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
//! 1. Locks its candidates (registrations its walk or audit read, nothing
//!    naming them) with `FOR UPDATE OF p SKIP LOCKED`. `FOR UPDATE`
//!    conflicts with `FOR KEY SHARE`, so a pack that an open transaction is
//!    referencing or re-registering is skipped, and once the sweep holds the
//!    lock, no transaction can add a reference to the pack (its foreign key
//!    check waits) until the sweep has ended.
//! 2. Re-checks every condition in the `DELETE` statements themselves.
//!    These run after the lock is held, with a new snapshot, so they see
//!    every reference and registration committed before the lock was taken,
//!    which the candidate query's older snapshot might not have.
//!
//! Which packs a batch looks at (the walk after a watermark, the audit, and
//! why a skipped row holds the watermark) only decides when an orphan is
//! found, never whether a named pack is safe: that rests on the two steps
//! above alone. The batch is described at [`sweep_orphan_packs`] and in
//! `crates/aspen-meta/SCHEMA.md`.
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

use std::collections::BTreeSet;
use std::time::Duration;

use aspen_core::error::StoreError;
use aspen_core::id::{PackChecksum, PackId, RepoId, UnixMicros};
use aspen_core::meta::{Lease, PackTombstone, SweepBatch};
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
    if store.dialect() == Dialect::Postgres {
        // One statement, its own transaction, in the common cases; the
        // transaction below otherwise.
        let mut conn = store.admit().await?;
        if let Some(record) = register_one(&mut conn, pack, store.now()).await? {
            return same_or_conflict(record, pack);
        }
    }
    store
        .write(async |tx| register_in(store, tx, pack).await)
        .await
}

/// Postgres: a registration as one statement, or `None` when it needs the
/// transaction ([`register_in`]).
///
/// It inserts the row at the first generation only when the statement's
/// snapshot holds neither a row nor a tombstone for the checksum, and
/// otherwise locks the existing row `FOR KEY SHARE` as [`find`] does; in
/// both cases it touches the row, unless an existing row describes another
/// pack (which [`same_or_conflict`] then refuses). `None` means it did
/// neither: the checksum has a tombstone (so the new row needs a later
/// generation), or the row the snapshot held was deleted meanwhile (the
/// sweep), or a concurrent registration inserted one after the snapshot.
///
/// Why inserting at the first generation without reading the tombstones
/// after the insert is safe here, when [`register_in`] must read them
/// after: the read after is needed because an insert can wait for a sweep
/// that is deleting this checksum's old row, and only a later statement
/// sees the tombstone that sweep writes. A sweep deletes only a row whose
/// last registration is a grace period old, so that row was committed
/// before this statement began and is in its snapshot, and then this
/// statement does not insert. That holds while servers' clocks agree to
/// within the grace period, which every push already needs (a pack whose
/// registration clock is further behind is swept before its push commits).
async fn register_one(
    conn: &mut crate::conn::Conn,
    pack: &NewPack,
    now: UnixMicros,
) -> Result<Option<PackRecord>, StoreError> {
    let keys = PackKeys::new(pack.repo, &pack.checksum, PackGeneration::FIRST);
    let received = match pack.origin {
        aspen_core::pack::PackOrigin::Push { received_bytes } => {
            Some(i64::try_from(received_bytes).expect("validated"))
        }
        aspen_core::pack::PackOrigin::Compaction { .. } => None,
    };
    let int = |v: u64| i64::try_from(v).expect("validated against DB_MAX");
    let sql = format!(
        "WITH seen AS (SELECT id FROM packs WHERE repo_id = $1 AND checksum = $2), \
         tomb AS (SELECT generation FROM pack_tombstones WHERE repo_id = $1 AND checksum = $2), \
         f AS (SELECT {PACK_COLS} FROM packs WHERE repo_id = $1 AND checksum = $2 FOR KEY SHARE), \
         ins AS (INSERT INTO packs (repo_id, checksum, pack_key, size, idx_key, idx_size, \
         entries_key, entries_size, object_count, ref_delta_count, level, origin, received_bytes) \
         SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13 \
         WHERE NOT EXISTS (SELECT 1 FROM seen) AND NOT EXISTS (SELECT 1 FROM tomb) \
         ON CONFLICT (repo_id, checksum) DO NOTHING RETURNING {PACK_COLS}), \
         got AS (SELECT {PACK_COLS} FROM ins UNION ALL SELECT {PACK_COLS} FROM f), \
         t AS (INSERT INTO pack_touches (pack_id, repo_id, touched_at_us) \
         SELECT id, repo_id, $14 FROM got WHERE size = $4 AND idx_size = $6 \
         AND entries_size = $8 AND object_count = $9 AND ref_delta_count = $10 AND level = $11 \
         AND origin = $12 {TOUCH_UPSERT}) \
         SELECT {PACK_COLS} FROM got"
    );
    sqlx::query(AssertSqlSafe(sql))
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
        .bind(now.0)
        .fetch_optional(&mut *conn)
        .await
        .map_err(store_error)?
        .as_ref()
        .map(rows::pack)
        .transpose()
}

/// [`MetaStore::register_packs`]: every pack in one transaction, each
/// behind its own savepoint, so a pack the store refuses (a size conflict)
/// fails alone. Packs are registered in `(repository, checksum)` order, so
/// two batches that register the same checksums take their unique-index
/// entries in one order and cannot deadlock. A failed savepoint statement
/// or `COMMIT` fails the batch as a whole.
///
/// [`MetaStore::register_packs`]: aspen_core::meta::MetaStore::register_packs
pub(crate) async fn register_packs(
    store: &SqlStore,
    packs: &[NewPack],
) -> Result<Vec<Result<PackRecord, StoreError>>, StoreError> {
    let mut out: Vec<Option<Result<PackRecord, StoreError>>> = vec![None; packs.len()];
    let mut valid = Vec::with_capacity(packs.len());
    for (i, pack) in packs.iter().enumerate() {
        match pack.validate() {
            Ok(()) => valid.push(i),
            Err(e) => out[i] = Some(Err(conflict(format!("pack {}: {e}", pack.checksum)))),
        }
    }
    if !valid.is_empty() {
        valid.sort_by(|&a, &b| {
            (packs[a].repo, packs[a].checksum.as_bytes())
                .cmp(&(packs[b].repo, packs[b].checksum.as_bytes()))
                .then(a.cmp(&b))
        });
        // On Postgres most registrations are one statement that writes
        // everything or nothing ([`register_one`]) and need no savepoint;
        // only the others do. If a statement fails, the transaction is
        // aborted and the batch runs again with a savepoint around every
        // registration, so a failure still fails only its own pack.
        let pg = store.dialect() == Dialect::Postgres;
        let batch = async |savepoints: bool| {
            store
                .write(async |tx| {
                    let mut parts = Vec::with_capacity(valid.len());
                    for &i in &valid {
                        if pg
                            && !savepoints
                            && let Some(record) = register_one(tx, &packs[i], store.now()).await?
                        {
                            parts.push((i, same_or_conflict(record, &packs[i])));
                            continue;
                        }
                        crate::tx::savepoint(tx).await?;
                        let result = register_in(store, tx, &packs[i]).await;
                        match &result {
                            Ok(_) => crate::tx::release(tx).await?,
                            Err(_) => crate::tx::rollback_to(tx).await?,
                        }
                        parts.push((i, result));
                    }
                    Ok::<_, StoreError>(parts)
                })
                .await
        };
        let parts = match batch(false).await {
            Ok(parts) => parts,
            Err(_) if pg => batch(true).await?,
            Err(e) => return Err(e),
        };
        for (i, result) in parts {
            out[i] = Some(result);
        }
    }
    Ok(out
        .into_iter()
        .map(|o| o.expect("every pack has an outcome"))
        .collect())
}

/// The statements of one registration, inside a write transaction.
///
/// The insert comes first and, when it inserts, is the only statement
/// before the generation read; an existing row is read and locked after.
/// On Postgres the touch rides in the same statement as the insert or the
/// read (a `WITH` clause), so a new pack costs two statements and an
/// existing one one; SQLite cannot write in a `WITH` clause and touches in
/// a statement of its own.
async fn register_in(
    store: &SqlStore,
    tx: &mut WriteTx,
    pack: &NewPack,
) -> Result<PackRecord, StoreError> {
    let now = store.now();
    let pg = store.dialect() == Dialect::Postgres;
    if pg && let Some(record) = register_one(tx, pack, now).await? {
        return same_or_conflict(record, pack);
    }
    if let Some(inserted) = insert(tx, pack, pg.then_some(now)).await? {
        if !pg {
            touch(tx, inserted.id, pack.repo, now).await?;
        }
        // After the insert, not before: on Postgres a sweep deleting this
        // checksum's old row holds its unique index entry, so the insert
        // waits for the sweep to commit, and only a statement after it
        // sees the sweep's tombstone.
        let generation = next_generation(tx, pack).await?;
        if generation == PackGeneration::FIRST {
            return Ok(inserted);
        }
        let id = inserted.id;
        set_generation(tx, id, pack, generation).await?;
        return find(store, tx, pack.repo, &pack.checksum)
            .await?
            .filter(|r| r.id == id)
            .ok_or_else(|| corrupt(format!("pack {id} vanished in its insert")));
    }
    // A row with the checksum exists: registered before, or by a
    // concurrent registration that has committed (the insert waited for
    // it).
    let existing = if pg {
        find_and_touch(tx, pack.repo, &pack.checksum, now).await?
    } else {
        let found = find(store, tx, pack.repo, &pack.checksum).await?;
        if let Some(r) = &found {
            touch(tx, r.id, pack.repo, now).await?;
        }
        found
    };
    let existing = existing.ok_or_else(|| StoreError::Unavailable {
        reason: format!(
            "pack {} was registered and removed concurrently",
            pack.checksum
        ),
    })?;
    same_or_conflict(existing, pack)
}

/// The upsert that records a registration time, never moving it back
/// ([`touch`]), after the `INSERT INTO pack_touches` that supplies the row.
const TOUCH_UPSERT: &str = "ON CONFLICT (pack_id) DO UPDATE SET touched_at_us = CASE \
     WHEN excluded.touched_at_us > pack_touches.touched_at_us \
     THEN excluded.touched_at_us ELSE pack_touches.touched_at_us END";

/// Postgres: the row with this checksum, locked `FOR KEY SHARE` as
/// [`find`] locks it, touched at `now` in the same statement.
async fn find_and_touch(
    tx: &mut WriteTx,
    repo: RepoId,
    checksum: &PackChecksum,
    now: UnixMicros,
) -> Result<Option<PackRecord>, StoreError> {
    let sql = format!(
        "WITH f AS (SELECT {PACK_COLS} FROM packs WHERE repo_id = $1 AND checksum = $2 \
         FOR KEY SHARE), t AS (INSERT INTO pack_touches (pack_id, repo_id, touched_at_us) \
         SELECT id, repo_id, $3 FROM f {TOUCH_UPSERT}) SELECT {PACK_COLS} FROM f"
    );
    sqlx::query(AssertSqlSafe(sql))
        .bind(repo.get())
        .bind(checksum.as_bytes().to_vec())
        .bind(now.0)
        .fetch_optional(&mut **tx)
        .await
        .map_err(store_error)?
        .as_ref()
        .map(rows::pack)
        .transpose()
}

/// Inserts the row at the first generation and returns it; `None` if a row
/// with the checksum exists. With `touch_at` (Postgres), the same statement
/// records the registration time.
async fn insert(
    tx: &mut WriteTx,
    pack: &NewPack,
    touch_at: Option<UnixMicros>,
) -> Result<Option<PackRecord>, StoreError> {
    let keys = PackKeys::new(pack.repo, &pack.checksum, PackGeneration::FIRST);
    let received = match pack.origin {
        aspen_core::pack::PackOrigin::Push { received_bytes } => {
            Some(i64::try_from(received_bytes).expect("validated"))
        }
        aspen_core::pack::PackOrigin::Compaction { .. } => None,
    };
    let int = |v: u64| i64::try_from(v).expect("validated against DB_MAX");
    let insert = "INSERT INTO packs (repo_id, checksum, pack_key, size, idx_key, idx_size, \
         entries_key, entries_size, object_count, ref_delta_count, level, origin, received_bytes) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13) \
         ON CONFLICT (repo_id, checksum) DO NOTHING";
    let sql = match touch_at {
        Some(_) => format!(
            "WITH ins AS ({insert} RETURNING {PACK_COLS}), \
             t AS (INSERT INTO pack_touches (pack_id, repo_id, touched_at_us) \
             SELECT id, repo_id, $14 FROM ins {TOUCH_UPSERT}) SELECT {PACK_COLS} FROM ins"
        ),
        None => format!("{insert} RETURNING {PACK_COLS}"),
    };
    let mut q = sqlx::query(AssertSqlSafe(sql))
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
        .bind(received);
    if let Some(now) = touch_at {
        q = q.bind(now.0);
    }
    q.fetch_optional(&mut **tx)
        .await
        .map_err(store_error)?
        .as_ref()
        .map(rows::pack)
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
    let sql = format!(
        "INSERT INTO pack_touches (pack_id, repo_id, touched_at_us) VALUES ($1, $2, $3) \
         {TOUCH_UPSERT}"
    );
    sqlx::query(AssertSqlSafe(sql))
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

/// The sweep's ordered walk: registrations after the watermark `($1, $2)`
/// whose grace period ended by `$3`, oldest first, at most `$4`. A range
/// scan of index `pack_touches_due` (migration 0009); public so tests can
/// check its plan.
pub const SWEEP_WALK_SQL: &str = "SELECT pack_id, touched_at_us FROM pack_touches \
     WHERE (touched_at_us, pack_id) > ($1, $2) AND touched_at_us <= $3 \
     ORDER BY touched_at_us, pack_id LIMIT $4";

/// The sweep's audit: the `$2` packs after ID `$1`, with their
/// registration times. A range scan of an index of `packs` led by `id` (the
/// primary key, or `packs_id_repo_unique` once the table is vacuumed) with
/// one keyed lookup per row; public so tests can check its plan. The lookup is
/// a subquery, not a join: Postgres plans a join as a merge that reads
/// `pack_touches` from its start, which grows with the table.
pub const SWEEP_AUDIT_SQL: &str = "SELECT p.id, p.repo_id, \
     (SELECT t.touched_at_us FROM pack_touches t WHERE t.pack_id = p.id) AS touched_at_us \
     FROM packs p WHERE p.id > $1 ORDER BY p.id LIMIT $2";

/// The `gc_cursors` row of the orphan sweep.
const SWEEP_CURSOR: &str = "orphan_packs";

/// Where the sweep resumes ([`SWEEP_CURSOR`]): the `(touched_at_us,
/// pack_id)` key of the last registration the ordered walk examined, and
/// the last pack ID the audit examined.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Cursor {
    watermark: (i64, i64),
    audit: i64,
}

impl Cursor {
    /// Before the first sweep: before every registration and every pack.
    const START: Self = Self {
        watermark: (i64::MIN, 0),
        audit: 0,
    };
}

async fn read_cursor(tx: &mut WriteTx) -> Result<Cursor, StoreError> {
    let row =
        sqlx::query("SELECT touched_at_us, pack_id, audit_pack_id FROM gc_cursors WHERE name = $1")
            .bind(SWEEP_CURSOR)
            .fetch_optional(&mut **tx)
            .await
            .map_err(store_error)?;
    let Some(row) = row else {
        return Ok(Cursor::START);
    };
    Ok(Cursor {
        watermark: (
            rows::get(&row, "touched_at_us")?,
            rows::get(&row, "pack_id")?,
        ),
        audit: rows::get(&row, "audit_pack_id")?,
    })
}

async fn write_cursor(tx: &mut WriteTx, c: Cursor) -> Result<(), StoreError> {
    sqlx::query(
        "INSERT INTO gc_cursors (name, touched_at_us, pack_id, audit_pack_id) \
         VALUES ($1, $2, $3, $4) ON CONFLICT (name) DO UPDATE SET \
         touched_at_us = excluded.touched_at_us, pack_id = excluded.pack_id, \
         audit_pack_id = excluded.audit_pack_id",
    )
    .bind(SWEEP_CURSOR)
    .bind(c.watermark.0)
    .bind(c.watermark.1)
    .bind(c.audit)
    .execute(&mut **tx)
    .await
    .map_err(store_error)?;
    Ok(())
}

/// One sweep batch (module docs, and `crates/aspen-meta/SCHEMA.md`,
/// "Garbage collection"). Every step is a range scan of an index bounded by
/// `limit`, so a batch costs the same with a thousand packs or a million:
///
/// 1. Extend the lease, or refuse ([`crate::lease::fence`]).
/// 2. The ordered walk: up to `limit` registrations (`pack_touches`) after
///    the watermark whose grace period is over, oldest first (index
///    `pack_touches_due`).
/// 3. The audit: the next `limit` packs by ID after the audit cursor,
///    wrapping to the start at the end. A pack with no registration time
///    (registered before migration 0005) gets one now; a pack whose grace
///    period is over and whose registration the watermark already passed
///    is a candidate again, so an orphan the walk missed is found within
///    one audit cycle.
/// 4. Lock the candidates no row names (`FOR UPDATE SKIP LOCKED` on
///    Postgres) and delete them, re-checking every condition (module docs).
/// 5. Advance the watermark past what was decided, but never past a
///    candidate a concurrent transaction held locked: that transaction may
///    roll back and leave it an orphan, so the next batch looks at it
///    again.
pub(crate) async fn sweep_orphan_packs(
    store: &SqlStore,
    lease: &Lease,
    grace: Duration,
    limit: u32,
) -> Result<SweepBatch, StoreError> {
    let lock = match store.dialect() {
        Dialect::Postgres => " FOR UPDATE OF p SKIP LOCKED",
        Dialect::Sqlite => "",
    };
    store
        .write(async |tx| {
            let now = store.now().0;
            let cutoff = now.saturating_sub(micros(grace));
            crate::lease::fence(tx, lease, now).await?;
            if limit == 0 {
                return Ok(SweepBatch::default());
            }
            let cursor = read_cursor(tx).await?;

            // Step 2: the ordered walk.
            let walk = sqlx::query(SWEEP_WALK_SQL)
                .bind(cursor.watermark.0)
                .bind(cursor.watermark.1)
                .bind(cutoff)
                .bind(i64::from(limit))
                .fetch_all(&mut **tx)
                .await
                .map_err(store_error)?;
            let mut walked = Vec::with_capacity(walk.len());
            for row in &walk {
                let id: i64 = rows::get(row, "pack_id")?;
                let touched: i64 = rows::get(row, "touched_at_us")?;
                walked.push((touched, id));
            }

            // Step 3: the audit.
            let audit = sqlx::query(SWEEP_AUDIT_SQL)
                .bind(cursor.audit)
                .bind(i64::from(limit))
                .fetch_all(&mut **tx)
                .await
                .map_err(store_error)?;
            let mut candidates: BTreeSet<i64> = walked.iter().map(|&(_, id)| id).collect();
            let mut audit_end = cursor.audit;
            for row in &audit {
                let id: i64 = rows::get(row, "id")?;
                let repo: i64 = rows::get(row, "repo_id")?;
                let touched: Option<i64> = rows::get(row, "touched_at_us")?;
                audit_end = id;
                match touched {
                    // A pack with no registration time (registered before
                    // migration 0005) gets one now and is deleted a grace
                    // period later: the walk reaches it then.
                    None => {
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
                    Some(t) if t <= cutoff && (t, id) <= cursor.watermark => {
                        candidates.insert(id);
                    }
                    // Not due yet, or the walk will reach it.
                    Some(_) => {}
                }
            }
            let audit_next = if audit.len() < limit as usize {
                0
            } else {
                audit_end
            };

            // Step 4: lock the candidates no row names (module docs).
            let candidates: Vec<i64> = candidates.into_iter().collect();
            let mut locked = BTreeSet::new();
            for chunk in candidates.chunks(CHUNK) {
                let sql = format!(
                    "SELECT p.id FROM packs p WHERE p.id IN ({}) AND {}{lock}",
                    placeholders(1, chunk.len()),
                    unreferenced("p.id")
                );
                let mut q = sqlx::query(AssertSqlSafe(sql));
                for id in chunk {
                    q = q.bind(*id);
                }
                for row in q.fetch_all(&mut **tx).await.map_err(store_error)? {
                    locked.insert(rows::get::<i64>(&row, "id")?);
                }
            }
            // Walked candidates neither locked nor named: a concurrent
            // transaction holds them (only Postgres skips rows).
            let mut held = BTreeSet::new();
            if store.dialect() == Dialect::Postgres {
                let unlocked: Vec<i64> = walked
                    .iter()
                    .map(|&(_, id)| id)
                    .filter(|id| !locked.contains(id))
                    .collect();
                for chunk in unlocked.chunks(CHUNK) {
                    let sql = format!(
                        "SELECT p.id FROM packs p WHERE p.id IN ({}) AND {}",
                        placeholders(1, chunk.len()),
                        unreferenced("p.id")
                    );
                    let mut q = sqlx::query(AssertSqlSafe(sql));
                    for id in chunk {
                        q = q.bind(*id);
                    }
                    for row in q.fetch_all(&mut **tx).await.map_err(store_error)? {
                        held.insert(rows::get::<i64>(&row, "id")?);
                    }
                }
            }

            // Delete, re-checking every condition with a fresh snapshot
            // (module docs). The touch row goes first: its foreign key
            // names the pack.
            let locked: Vec<i64> = locked.into_iter().collect();
            let mut deleted = Vec::new();
            for chunk in locked.chunks(CHUNK) {
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

            // Step 5: the watermark stops before the first held candidate.
            let decided = walked.iter().take_while(|(_, id)| !held.contains(id));
            let watermark = decided.last().copied().unwrap_or(cursor.watermark);
            write_cursor(
                tx,
                Cursor {
                    watermark,
                    audit: audit_next,
                },
            )
            .await?;

            deleted.sort_by_key(|p| p.id);
            Ok(SweepBatch {
                deleted,
                examined: (walked.len() + audit.len()) as u64,
                // A batch that could not move the watermark (its first
                // candidate is held) also stops the caller's repeats: the
                // next pass looks again, rather than spinning on the lock.
                caught_up: walked.len() < limit as usize || watermark == cursor.watermark,
            })
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
