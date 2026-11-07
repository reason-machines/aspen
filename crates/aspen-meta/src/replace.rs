//! Replacing a stored pack with its base-first rewrite:
//! [`MetaStore::pack_references`] and [`MetaStore::replace_pack`]
//! (reason-machines/aspen#10).
//!
//! A `rewrite_pack` job writes the base-first rewrite of a pack that broke
//! the base-first invariant as a new `packs` row, uploads its blobs, and
//! then calls [`MetaStore::replace_pack`], which moves every ref version,
//! cover and shallow pack recording naming the old pack to the new one in
//! one transaction and touches the old pack. Jobs are left alone: the
//! `rewrite_pack` job keeps naming the old pack, done or not, and the sweep
//! counts any job as naming its pack, so the old pack stays stored for as
//! long as that row exists. Receive-side caches may hold the old pack's ID
//! for the life of a process (`docs/PLAN.md`, decisions log,
//! reason-machines/aspen#10), and read objects from it, which its bytes
//! still serve.
//!
//! The new row is locked against the sweep first, as
//! [`MetaStore::set_cover_pack`] locks the pack it records (`src/gc.rs`,
//! "Races"): on Postgres `FOR KEY SHARE`, which conflicts with the sweep's
//! `FOR UPDATE`; on SQLite every write transaction is `BEGIN IMMEDIATE`.
//! The old row is named while the transaction runs, so the sweep does not
//! pick it; once the transaction commits, the touch keeps it.
//!
//! [`MetaStore::pack_references`]: aspen_core::meta::MetaStore::pack_references
//! [`MetaStore::replace_pack`]: aspen_core::meta::MetaStore::replace_pack
//! [`MetaStore::set_cover_pack`]: aspen_core::meta::MetaStore::set_cover_pack

use aspen_core::error::StoreError;
use aspen_core::id::PackId;
use aspen_core::meta::PackReferences;
use sqlx::AssertSqlSafe;

use crate::error::{conflict, store_error};
use crate::gc::{key_share, touch};
use crate::rows;
use crate::store::SqlStore;
use crate::tx::WriteTx;

/// The tables whose `pack_id` names a pack a fetch may serve.
const NAMING: [&str; 3] = ["ref_versions", "covers", "shallow_packs"];

fn references(counts: [u64; 3]) -> PackReferences {
    PackReferences {
        versions: counts[0],
        covers: counts[1],
        shallow_packs: counts[2],
    }
}

pub(crate) async fn pack_references(
    store: &SqlStore,
    pack: PackId,
) -> Result<PackReferences, StoreError> {
    let sql = format!(
        "SELECT {} ",
        NAMING
            .iter()
            .map(|t| format!("(SELECT COUNT(*) FROM {t} WHERE pack_id = $1) AS {t}"))
            .collect::<Vec<_>>()
            .join(", ")
    );
    let row = sqlx::query(AssertSqlSafe(sql))
        .bind(pack.get())
        .fetch_one(&mut *store.admit().await?)
        .await
        .map_err(store_error)?;
    let mut counts = [0u64; 3];
    for (c, t) in counts.iter_mut().zip(NAMING) {
        *c = u64::try_from(rows::get::<i64>(&row, t)?).unwrap_or(0);
    }
    Ok(references(counts))
}

/// The pack's repository and level, locked against the sweep on Postgres.
async fn locked(
    store: &SqlStore,
    tx: &mut WriteTx,
    pack: PackId,
) -> Result<Option<(i64, i64)>, StoreError> {
    let sql = format!(
        "SELECT repo_id, level FROM packs WHERE id = $1{}",
        key_share(store)
    );
    let row = sqlx::query(AssertSqlSafe(sql))
        .bind(pack.get())
        .fetch_optional(&mut **tx)
        .await
        .map_err(store_error)?;
    row.as_ref()
        .map(|r| {
            Ok((
                rows::get::<i64>(r, "repo_id")?,
                rows::get::<i64>(r, "level")?,
            ))
        })
        .transpose()
}

pub(crate) async fn replace_pack(
    store: &SqlStore,
    old: PackId,
    new: PackId,
) -> Result<PackReferences, StoreError> {
    if old == new {
        return Err(conflict(format!("pack {old} cannot replace itself")));
    }
    store
        .write(async |tx| {
            let now = store.now();
            let Some((repo, level)) = locked(store, tx, old).await? else {
                return Err(StoreError::NotFound {
                    what: format!("pack {old}"),
                });
            };
            let Some((new_repo, new_level)) = locked(store, tx, new).await? else {
                return Err(StoreError::pack_swept(format_args!(
                    "pack {new}, the replacement of pack {old}, was deleted before it was recorded"
                )));
            };
            if (repo, level) != (new_repo, new_level) {
                return Err(conflict(format!(
                    "pack {new} (repository {new_repo}, level {new_level}) cannot replace pack \
                     {old} (repository {repo}, level {level})"
                )));
            }
            let mut counts = [0u64; 3];
            for (c, t) in counts.iter_mut().zip(NAMING) {
                let sql = format!("UPDATE {t} SET pack_id = $1 WHERE pack_id = $2");
                *c = sqlx::query(AssertSqlSafe(sql))
                    .bind(new.get())
                    .bind(old.get())
                    .execute(&mut **tx)
                    .await
                    .map_err(store_error)?
                    .rows_affected();
            }
            let repo = aspen_core::id::RepoId::new(repo)
                .map_err(|e| crate::error::corrupt(format!("packs.repo_id of pack {old}: {e}")))?;
            touch(tx, old, repo, now).await?;
            Ok(references(counts))
        })
        .await
}
