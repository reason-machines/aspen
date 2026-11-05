//! The push commit transaction: [`MetaStore::commit_push`].
//!
//! [`MetaStore::commit_push`]: aspen_core::meta::MetaStore::commit_push

use aspen_core::error::StoreError;
use aspen_core::id::RefVersionId;
use aspen_core::meta::{CommitOutcome, PushCommit, RefWrite};
use aspen_core::name::RefName;
use aspen_core::refs::{Closure, RefValue, RefVersion};
use sqlx::AssertSqlSafe;

use crate::error::{conflict, is_unique_violation, store_error};
use crate::rows::{self, VERSION_RETURNING};
use crate::sql::{CHUNK, placeholders};
use crate::store::{SqlStore, current_version, insert_commits};

/// What one write's statements ended in.
enum Step {
    Wrote(RefVersion),
    /// The ref was not at the expected version.
    Lost,
}

pub(crate) async fn commit_push(
    store: &SqlStore,
    push: &PushCommit,
) -> Result<CommitOutcome, StoreError> {
    push.validate()
        .map_err(|e| conflict(format!("malformed push commit: {e}")))?;

    // Take ref rows in name order, so two atomic pushes that touch the same
    // refs wait for each other instead of deadlocking.
    let mut order: Vec<usize> = (0..push.writes.len()).collect();
    order.sort_by(|&a, &b| push.writes[a].name.cmp(&push.writes[b].name));

    let key_share = crate::gc::key_share(store);
    match store
        .write(async |tx| {
            lock_packs(tx, push, key_share).await?;
            write_all(tx, push, &order).await
        })
        .await
    {
        Ok(created) => Ok(CommitOutcome::Committed(created)),
        Err(Abort::Lost(name)) => {
            // `write` rolled back before returning: on Postgres the
            // transaction was aborted by the violation anyway, and on
            // SQLite the writes before the lost one must not land. The
            // winner has committed (the loser waited for it), so a fresh
            // read sees its version.
            let current = current_version(&mut *store.admit().await?, push.repo, name)
                .await?
                .map(|v| v.id);
            Ok(CommitOutcome::CasLost {
                name: name.clone(),
                current,
            })
        }
        Err(Abort::Store(e)) => Err(e),
    }
}

/// Why a push transaction rolled back.
enum Abort<'p> {
    /// The named ref was not at the expected version.
    Lost(&'p RefName),
    /// A storage error, or a refused request.
    Store(StoreError),
}

impl From<StoreError> for Abort<'_> {
    fn from(e: StoreError) -> Self {
        Self::Store(e)
    }
}

/// Locks the packs the writes name (on Postgres, `FOR KEY SHARE`, as their
/// foreign key checks would), and fails with [`StoreError::Unavailable`] if
/// one no longer exists: garbage collection deleted it because the push
/// took longer than the grace period after registering it (`crate::gc`).
/// Retrying the push later registers it again (at the next generation)
/// and uploads it again. A pack of
/// another repository is left to `ref_versions_pack_fk`, a conflict.
///
/// Without the explicit check the swept pack would still be refused, by
/// `ref_versions_pack_fk` when the version row is inserted, but as
/// [`StoreError::Conflict`] ("the request broke a rule"), which callers
/// must not retry; the check makes it the retryable kind
/// (`a_push_naming_a_swept_pack_is_unavailable`, mutation G9 in
/// `crates/aspen-compact/scripts/check-mutations.py`). Pack IDs are never
/// reused, so a row that exists under this ID is the generation the push
/// registered.
async fn lock_packs(
    conn: &mut sqlx::AnyConnection,
    push: &PushCommit,
    key_share: &str,
) -> Result<(), StoreError> {
    let mut ids: Vec<i64> = push
        .writes
        .iter()
        .filter_map(|w| match w.value {
            RefValue::Live { pack, .. } => pack.map(|p| p.get()),
            RefValue::Deleted => None,
        })
        .collect();
    // Sorted, so concurrent pushes take the locks in one order.
    ids.sort_unstable();
    ids.dedup();
    for chunk in ids.chunks(CHUNK) {
        let sql = format!(
            "SELECT id FROM packs WHERE id IN ({}) ORDER BY id{key_share}",
            placeholders(1, chunk.len())
        );
        let mut q = sqlx::query(AssertSqlSafe(sql));
        for id in chunk {
            q = q.bind(*id);
        }
        let found: Vec<i64> = q
            .fetch_all(&mut *conn)
            .await
            .map_err(store_error)?
            .iter()
            .map(|r| rows::get(r, "id"))
            .collect::<Result<_, _>>()?;
        if let Some(gone) = chunk.iter().find(|id| !found.contains(id)) {
            return Err(StoreError::pack_swept(format_args!(
                "pack {gone} no longer exists; retry the push"
            )));
        }
    }
    Ok(())
}

/// Every statement of the transaction: the new versions in `writes`
/// order, or [`Abort::Lost`] naming the first ref (in name order) that
/// lost.
async fn write_all<'p>(
    conn: &mut sqlx::AnyConnection,
    push: &'p PushCommit,
    order: &[usize],
) -> Result<Vec<RefVersion>, Abort<'p>> {
    let mut created: Vec<Option<RefVersion>> = vec![None; push.writes.len()];
    for &i in order {
        let write = &push.writes[i];
        match write_ref(conn, push, write).await? {
            Step::Wrote(version) => created[i] = Some(version),
            Step::Lost => return Err(Abort::Lost(&write.name)),
        }
    }
    let created: Vec<RefVersion> = created
        .into_iter()
        .map(|v| v.expect("every write was processed"))
        .collect();
    let ids: Vec<RefVersionId> = created.iter().map(|v| v.id).collect();
    let rows = push
        .rows(&ids)
        .map_err(|e| conflict(format!("malformed push commit: {e}")))?;
    insert_commits(conn, push.repo, &rows.commits).await?;
    for job in &rows.jobs {
        crate::jobs::enqueue(conn, job).await?;
    }
    Ok(created)
}

/// Steps 1 and 2 of [`MetaStore::commit_push`] for one write: insert the
/// version as the expected version's child, then move the `refs` row with
/// compare-and-swap.
///
/// [`MetaStore::commit_push`]: aspen_core::meta::MetaStore::commit_push
async fn write_ref(
    conn: &mut sqlx::AnyConnection,
    push: &PushCommit,
    write: &RefWrite,
) -> Result<Step, StoreError> {
    let (tip, closure, pack) = match write.value {
        RefValue::Live { tip, pack, closure } => (
            Some(tip.as_bytes().to_vec()),
            Some(match closure {
                Closure::Exact => 1_i64,
                Closure::Wider => 0,
            }),
            pack.map(|p| p.get()),
        ),
        RefValue::Deleted => (None, None, None),
    };
    let trunk = write.deps.trunk.map(|d| d.get());
    let scoped = write.deps.scoped_old.map(|d| d.get());

    // Step 1. The new version's seq and parent come from the expected
    // version itself, read in the same statement. If another push already
    // inserted that seq, the insert violates ref_versions_seq_unique (on
    // Postgres after waiting for that push's transaction): a lost race.
    let insert = match write.expected {
        Some(expected) => {
            let sql = format!(
                "INSERT INTO ref_versions (repo_id, ref_name, seq, parent_version_id, tip_oid, \
                 closure_exact, pack_id, trunk_dep_id, scoped_dep_id) \
                 SELECT repo_id, ref_name, seq + 1, id, $4, $5, $6, $7, $8 FROM ref_versions \
                 WHERE id = $1 AND repo_id = $2 AND ref_name = $3 RETURNING {VERSION_RETURNING}"
            );
            sqlx::query(AssertSqlSafe(sql))
                .bind(expected.get())
                .bind(push.repo.get())
                .bind(write.name.as_str().to_owned())
        }
        None => {
            let sql = format!(
                "INSERT INTO ref_versions (repo_id, ref_name, seq, parent_version_id, tip_oid, \
                 closure_exact, pack_id, trunk_dep_id, scoped_dep_id) \
                 VALUES ($1, $2, 1, NULL, $3, $4, $5, $6, $7) RETURNING {VERSION_RETURNING}"
            );
            sqlx::query(AssertSqlSafe(sql))
                .bind(push.repo.get())
                .bind(write.name.as_str().to_owned())
        }
    }
    .bind(tip)
    .bind(closure)
    .bind(pack)
    .bind(trunk)
    .bind(scoped);
    let version = match insert.fetch_optional(&mut *conn).await {
        Ok(Some(row)) => rows::version(&row)?,
        Ok(None) => {
            return Err(conflict(format!(
                "{}: expected version {} is not a version of this ref",
                write.name,
                write.expected.map_or(0, |v| v.get())
            )));
        }
        Err(e) if is_unique_violation(&e, "ref_versions_seq_unique") => return Ok(Step::Lost),
        Err(e) => return Err(store_error(e)),
    };

    // Step 2. The compare-and-swap itself. Its foreign key on
    // (version_id, repo_id, ref_name, seq) makes the database refuse any
    // version that is not the expected version's direct child.
    let moved = match write.expected {
        Some(expected) => sqlx::query(
            "UPDATE refs SET version_id = $1, seq = seq + 1 \
             WHERE repo_id = $2 AND ref_name = $3 AND version_id = $4",
        )
        .bind(version.id.get())
        .bind(push.repo.get())
        .bind(write.name.as_str().to_owned())
        .bind(expected.get())
        .execute(&mut *conn)
        .await
        .map(|r| r.rows_affected() == 1),
        None => sqlx::query(
            "INSERT INTO refs (repo_id, ref_name, version_id, seq) VALUES ($1, $2, $3, 1)",
        )
        .bind(push.repo.get())
        .bind(write.name.as_str().to_owned())
        .bind(version.id.get())
        .execute(&mut *conn)
        .await
        .map(|_| true),
    };
    match moved {
        Ok(true) => Ok(Step::Wrote(version)),
        Ok(false) => Ok(Step::Lost),
        Err(e) if is_unique_violation(&e, "refs_pkey") => Ok(Step::Lost),
        Err(e) => Err(store_error(e)),
    }
}
