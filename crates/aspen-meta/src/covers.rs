//! Covers and their chains: [`MetaStore::insert_cover`] and
//! [`MetaStore::set_cover_pack`] (migration 0005).
//!
//! [`MetaStore::insert_cover`]: aspen_core::meta::MetaStore::insert_cover
//! [`MetaStore::set_cover_pack`]: aspen_core::meta::MetaStore::set_cover_pack

use aspen_core::error::StoreError;
use aspen_core::id::{CoverId, PackId};
use aspen_core::meta::{Cover, NewCover};
use sqlx::AssertSqlSafe;

use crate::error::{conflict, corrupt, store_error};
use crate::rows::{self, COVER_COLS, COVER_FROM};
use crate::store::SqlStore;
use crate::tx::WriteTx;

/// The cover with its chain, read in the write transaction.
async fn read(tx: &mut WriteTx, id: CoverId) -> Result<Option<Cover>, StoreError> {
    let sql = format!("SELECT {COVER_COLS} FROM {COVER_FROM} WHERE c.id = $1");
    sqlx::query(AssertSqlSafe(sql))
        .bind(id.get())
        .fetch_optional(&mut **tx)
        .await
        .map_err(store_error)?
        .as_ref()
        .map(rows::cover)
        .transpose()
}

pub(crate) async fn insert_cover(store: &SqlStore, cover: &NewCover) -> Result<Cover, StoreError> {
    let what = || {
        format!(
            "{} cover L{} {}..={}",
            cover.name,
            cover.level.get(),
            cover.range.first(),
            cover.range.last()
        )
    };
    store
        .write(async |tx| {
            // On Postgres, a concurrent insert of the same cover makes this
            // wait for that transaction; after it commits, nothing is
            // inserted and the read below sees its row.
            let inserted = sqlx::query(
                "INSERT INTO covers (repo_id, ref_name, level, first_seq, last_seq) \
                 VALUES ($1, $2, $3, $4, $5) \
                 ON CONFLICT (repo_id, ref_name, level, first_seq) DO NOTHING RETURNING id",
            )
            .bind(cover.repo.get())
            .bind(cover.name.as_str().to_owned())
            .bind(i64::from(cover.level.get()))
            .bind(cover.range.first().to_db())
            .bind(cover.range.last().to_db())
            .fetch_optional(&mut **tx)
            .await
            .map_err(store_error)?;
            let id = match inserted {
                Some(row) => {
                    let id: i64 = rows::get(&row, "id")?;
                    let id = CoverId::new(id).map_err(|e| corrupt(format!("covers.id: {e}")))?;
                    if let Some(chain) = cover.chain {
                        // cover_chains_version_fk refuses a first version
                        // that is not the ref's version at first_seq.
                        sqlx::query(
                            "INSERT INTO cover_chains \
                             (cover_id, repo_id, ref_name, first_seq, first_version_id) \
                             VALUES ($1, $2, $3, $4, $5)",
                        )
                        .bind(id.get())
                        .bind(cover.repo.get())
                        .bind(cover.name.as_str().to_owned())
                        .bind(cover.range.first().to_db())
                        .bind(chain.first_version().get())
                        .execute(&mut **tx)
                        .await
                        .map_err(|e| match store_error(e) {
                            StoreError::Conflict { what: why } => conflict(format!(
                                "{}: chain from version {} is not the ref's version at its \
                                 first seq ({why})",
                                what(),
                                chain.first_version()
                            )),
                            other => other,
                        })?;
                    }
                    id
                }
                None => {
                    let row = sqlx::query(
                        "SELECT id FROM covers WHERE repo_id = $1 AND ref_name = $2 \
                         AND level = $3 AND first_seq = $4",
                    )
                    .bind(cover.repo.get())
                    .bind(cover.name.as_str().to_owned())
                    .bind(i64::from(cover.level.get()))
                    .bind(cover.range.first().to_db())
                    .fetch_one(&mut **tx)
                    .await
                    .map_err(store_error)?;
                    let id: i64 = rows::get(&row, "id")?;
                    CoverId::new(id).map_err(|e| corrupt(format!("covers.id: {e}")))?
                }
            };
            let stored = read(tx, id).await?.ok_or_else(|| StoreError::NotFound {
                what: format!("cover {id}"),
            })?;
            if stored.range != cover.range {
                return Err(conflict(format!(
                    "{}: a cover from that seq already ends at {}",
                    what(),
                    stored.range.last()
                )));
            }
            Ok(stored)
        })
        .await
}

pub(crate) async fn set_cover_pack(
    store: &SqlStore,
    cover: CoverId,
    pack: PackId,
) -> Result<Cover, StoreError> {
    store
        .write(async |tx| {
            // The pack row, locked against the sweep on Postgres as a
            // foreign key check would lock it. A pack the sweep deleted
            // after the compactor registered it is a retryable failure:
            // the job runs again and registers the pack anew.
            let sql = format!(
                "SELECT id FROM packs WHERE id = $1{}",
                crate::gc::key_share(store)
            );
            let exists = sqlx::query(sqlx::AssertSqlSafe(sql))
                .bind(pack.get())
                .fetch_optional(&mut **tx)
                .await
                .map_err(store_error)?
                .is_some();
            if !exists {
                return Err(StoreError::pack_swept(format_args!(
                    "cover {cover}: pack {pack} was deleted before it was recorded"
                )));
            }
            // covers_pack_fk refuses a pack of another repository or at
            // another level (a push pack is at level 0, a cover at 1 to 4).
            sqlx::query("UPDATE covers SET pack_id = $1 WHERE id = $2 AND pack_id IS NULL")
                .bind(pack.get())
                .bind(cover.get())
                .execute(&mut **tx)
                .await
                .map_err(|e| match store_error(e) {
                    StoreError::Conflict { what } => conflict(format!(
                        "cover {cover}: pack {pack} is not a pack of its repository at its \
                         level ({what})"
                    )),
                    other => other,
                })?;
            let stored = read(tx, cover).await?.ok_or_else(|| StoreError::NotFound {
                what: format!("cover {cover}"),
            })?;
            match stored.pack {
                Some(p) if p == pack => Ok(stored),
                Some(p) => Err(conflict(format!("cover {cover} already has pack {p}"))),
                None => Err(corrupt(format!(
                    "cover {cover}: pack {pack} was not recorded"
                ))),
            }
        })
        .await
}
