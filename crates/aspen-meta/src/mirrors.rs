//! Mirror mode rows (migration 0003, node C4).
//!
//! `mirrors` says which repositories front an upstream (GitHub, GitLab or
//! any smart-HTTP Git server), where it is and how pushes reach it.
//! `mirror_refs` is the sync ledger: for each ref, the value Aspen and the
//! upstream last agreed on, and the conflict found when both sides moved
//! since. `aspen-mirror` owns the meaning of these rows; this module only
//! stores them, so the mode is kept in its text form.
//!
//! Like the token and dead-job tools these are inherent [`SqlStore`]
//! methods rather than `MetaStore` methods: only composition roots use
//! them, through `aspen-server`'s adapter for `aspen_mirror::MirrorStore`.

use aspen_core::error::StoreError;
use aspen_core::id::{ObjectId, RepoId, UnixMicros};
use aspen_core::name::{RefName, RepoName};
use aspen_core::repo::{Repo, RepoSettings};
use sqlx::AssertSqlSafe;
use sqlx::any::AnyRow;

use crate::SqlStore;
use crate::error::{conflict, corrupt, is_unique_violation, store_error};
use crate::rows::get;
use crate::store::insert_repo;

/// One row of `mirrors`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MirrorRow {
    /// The mirrored repository.
    pub repo: RepoId,
    /// The upstream's smart-HTTP URL, without credentials.
    pub upstream_url: String,
    /// `proxy` or `async`.
    pub mode: String,
    /// The name of an operator-configured credential; empty for none.
    pub credential: String,
    /// When the mirror was set up.
    pub created_at: UnixMicros,
    /// When the last sync pass ended, if one has.
    pub last_sync_at: Option<UnixMicros>,
    /// The last pass's error; `None` when it succeeded.
    pub last_error: Option<String>,
}

/// A conflict in the ledger: both sides changed a ref since they last
/// agreed, to different values. `None` is a deletion on that side.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConflictRow {
    /// When it was found.
    pub at: UnixMicros,
    /// Aspen's value then.
    pub aspen: Option<ObjectId>,
    /// The upstream's value then.
    pub upstream: Option<ObjectId>,
}

/// The ledger state of one ref.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MirrorRefState {
    /// The value both sides last agreed on; `None` for "absent on both".
    pub synced: Option<ObjectId>,
    /// The open conflict, if any.
    pub conflict: Option<ConflictRow>,
}

impl MirrorRefState {
    /// Whether the state carries nothing a row is needed for.
    pub fn is_empty(&self) -> bool {
        self.synced.is_none() && self.conflict.is_none()
    }
}

/// One row of `mirror_refs`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MirrorRefRow {
    /// The ref.
    pub name: RefName,
    /// Its ledger state.
    pub state: MirrorRefState,
    /// When the row was last written.
    pub updated_at: UnixMicros,
}

const MIRROR_COLS: &str =
    "repo_id, upstream_url, mode, credential, created_at_us, last_sync_at_us, last_error";
const REF_COLS: &str = "ref_name, synced_oid, conflict_at_us, conflict_aspen_oid, \
                        conflict_upstream_oid, updated_at_us";

fn opt_oid(row: &AnyRow, col: &str) -> Result<Option<ObjectId>, StoreError> {
    get::<Option<Vec<u8>>>(row, col)?
        .map(|b| ObjectId::from_slice(&b).map_err(|e| corrupt(format!("mirror_refs.{col}: {e}"))))
        .transpose()
}

fn decode_mirror(row: &AnyRow) -> Result<MirrorRow, StoreError> {
    let repo: i64 = get(row, "repo_id")?;
    Ok(MirrorRow {
        repo: RepoId::new(repo).map_err(|e| corrupt(format!("mirrors.repo_id: {e}")))?,
        upstream_url: get(row, "upstream_url")?,
        mode: get(row, "mode")?,
        credential: get(row, "credential")?,
        created_at: UnixMicros(get(row, "created_at_us")?),
        last_sync_at: get::<Option<i64>>(row, "last_sync_at_us")?.map(UnixMicros),
        last_error: get(row, "last_error")?,
    })
}

fn decode_ref(row: &AnyRow) -> Result<MirrorRefRow, StoreError> {
    let name: String = get(row, "ref_name")?;
    let conflict = match get::<Option<i64>>(row, "conflict_at_us")? {
        None => None,
        Some(at) => Some(ConflictRow {
            at: UnixMicros(at),
            aspen: opt_oid(row, "conflict_aspen_oid")?,
            upstream: opt_oid(row, "conflict_upstream_oid")?,
        }),
    };
    Ok(MirrorRefRow {
        name: RefName::new(name).map_err(|e| corrupt(format!("mirror_refs.ref_name: {e}")))?,
        state: MirrorRefState {
            synced: opt_oid(row, "synced_oid")?,
            conflict,
        },
        updated_at: UnixMicros(get(row, "updated_at_us")?),
    })
}

fn oid_bytes(id: Option<ObjectId>) -> Option<Vec<u8>> {
    id.map(|i| i.as_bytes().to_vec())
}

/// Inserts one `mirrors` row; a repository that already is a mirror is
/// [`StoreError::Conflict`].
async fn insert_mirror_row(
    conn: &mut crate::conn::Conn,
    row: &MirrorRow,
) -> Result<(), StoreError> {
    let sql = format!("INSERT INTO mirrors ({MIRROR_COLS}) VALUES ($1, $2, $3, $4, $5, $6, $7)");
    sqlx::query(AssertSqlSafe(sql))
        .bind(row.repo.get())
        .bind(row.upstream_url.clone())
        .bind(row.mode.clone())
        .bind(row.credential.clone())
        .bind(row.created_at.0)
        .bind(row.last_sync_at.map(|t| t.0))
        .bind(row.last_error.clone())
        .execute(conn)
        .await
        .map_err(|e| {
            if is_unique_violation(&e, "mirrors_pkey") {
                conflict("the repository is already a mirror")
            } else {
                store_error(e)
            }
        })?;
    Ok(())
}

/// Why a ledger update did not happen.
enum SetError {
    Store(StoreError),
    /// The row's `synced` value was not the expected one.
    Moved,
}

impl From<StoreError> for SetError {
    fn from(e: StoreError) -> Self {
        Self::Store(e)
    }
}

impl SqlStore {
    /// Makes `row.repo` a mirror. A repository that already is one is
    /// [`StoreError::Conflict`].
    pub async fn insert_mirror(&self, row: &MirrorRow) -> Result<(), StoreError> {
        self.write(async |tx| insert_mirror_row(tx, row).await)
            .await
    }

    /// Creates repository `name` with `settings` and makes it a mirror with
    /// the row `row` builds from the new repository's ID, in one
    /// transaction: either both rows exist afterwards or neither does. A
    /// taken name is [`StoreError::Conflict`].
    pub async fn create_mirror_repo(
        &self,
        name: &RepoName,
        settings: &RepoSettings,
        row: impl FnOnce(RepoId) -> MirrorRow + Send,
    ) -> Result<Repo, StoreError> {
        self.write(async |tx| {
            let repo = insert_repo(&mut **tx, name, settings).await?;
            insert_mirror_row(tx, &row(repo.id)).await?;
            Ok(repo)
        })
        .await
    }

    /// The mirror row of a repository, if it is a mirror.
    pub async fn mirror(&self, repo: RepoId) -> Result<Option<MirrorRow>, StoreError> {
        let sql = format!("SELECT {MIRROR_COLS} FROM mirrors WHERE repo_id = $1");
        sqlx::query(AssertSqlSafe(sql))
            .bind(repo.get())
            .fetch_optional(&mut *self.admit().await?)
            .await
            .map_err(store_error)?
            .as_ref()
            .map(decode_mirror)
            .transpose()
    }

    /// Every mirror, by repository ID.
    pub async fn mirrors(&self) -> Result<Vec<MirrorRow>, StoreError> {
        let sql = format!("SELECT {MIRROR_COLS} FROM mirrors ORDER BY repo_id");
        sqlx::query(AssertSqlSafe(sql))
            .fetch_all(&mut *self.admit().await?)
            .await
            .map_err(store_error)?
            .iter()
            .map(decode_mirror)
            .collect()
    }

    /// Stops mirroring a repository: removes its mirror row and its whole
    /// ledger. The repository and its refs stay. Returns whether it was a
    /// mirror.
    pub async fn delete_mirror(&self, repo: RepoId) -> Result<bool, StoreError> {
        self.write(async |tx| {
            // The foreign key cascades too; deleting explicitly keeps the
            // order obvious and works with foreign keys off.
            sqlx::query("DELETE FROM mirror_refs WHERE repo_id = $1")
                .bind(repo.get())
                .execute(&mut **tx)
                .await
                .map_err(store_error)?;
            let done = sqlx::query("DELETE FROM mirrors WHERE repo_id = $1")
                .bind(repo.get())
                .execute(&mut **tx)
                .await
                .map_err(store_error)?;
            Ok::<_, StoreError>(done.rows_affected() == 1)
        })
        .await
    }

    /// Records the end of a sync pass: its time, and its error (`None` when
    /// it succeeded, which clears an earlier error).
    pub async fn record_mirror_sync(
        &self,
        repo: RepoId,
        at: UnixMicros,
        error: Option<&str>,
    ) -> Result<(), StoreError> {
        let error = error.map(str::to_owned);
        self.write(async |tx| {
            sqlx::query(
                "UPDATE mirrors SET last_sync_at_us = $2, last_error = $3 WHERE repo_id = $1",
            )
            .bind(repo.get())
            .bind(at.0)
            .bind(error)
            .execute(&mut **tx)
            .await
            .map_err(store_error)
            .map(|_| ())
        })
        .await
    }

    /// The ledger of a repository, in ref-name order.
    pub async fn mirror_refs(&self, repo: RepoId) -> Result<Vec<MirrorRefRow>, StoreError> {
        let sql =
            format!("SELECT {REF_COLS} FROM mirror_refs WHERE repo_id = $1 ORDER BY ref_name");
        sqlx::query(AssertSqlSafe(sql))
            .bind(repo.get())
            .fetch_all(&mut *self.admit().await?)
            .await
            .map_err(store_error)?
            .iter()
            .map(decode_ref)
            .collect()
    }

    /// The ledger state of one ref; a ref without a row is the empty state.
    pub async fn mirror_ref(
        &self,
        repo: RepoId,
        name: &RefName,
    ) -> Result<MirrorRefState, StoreError> {
        let sql =
            format!("SELECT {REF_COLS} FROM mirror_refs WHERE repo_id = $1 AND ref_name = $2");
        Ok(sqlx::query(AssertSqlSafe(sql))
            .bind(repo.get())
            .bind(name.as_str().to_owned())
            .fetch_optional(&mut *self.admit().await?)
            .await
            .map_err(store_error)?
            .as_ref()
            .map(decode_ref)
            .transpose()?
            .map(|r| r.state)
            .unwrap_or_default())
    }

    /// Sets one ref's ledger state, but only if its `synced` value is still
    /// `expected` (a ref without a row counts as `None`): a compare-and-swap,
    /// so two workers reconciling the same ref cannot write a stale
    /// agreement over a newer one. An empty `next` removes the row. Returns
    /// whether the state was written.
    pub async fn set_mirror_ref(
        &self,
        repo: RepoId,
        name: &RefName,
        expected: Option<ObjectId>,
        next: MirrorRefState,
        at: UnixMicros,
    ) -> Result<bool, StoreError> {
        let lock = self.for_update();
        let result = self
            .write(async |tx| {
                let sql = format!(
                    "SELECT synced_oid FROM mirror_refs WHERE repo_id = $1 AND ref_name = $2{lock}"
                );
                let current = sqlx::query(AssertSqlSafe(sql))
                    .bind(repo.get())
                    .bind(name.as_str().to_owned())
                    .fetch_optional(&mut **tx)
                    .await
                    .map_err(store_error)?;
                let exists = current.is_some();
                let current = match &current {
                    Some(row) => opt_oid(row, "synced_oid")?,
                    None => None,
                };
                if current != expected {
                    return Err(SetError::Moved);
                }
                let (repo_id, ref_name) = (repo.get(), name.as_str().to_owned());
                if next.is_empty() {
                    let deleted =
                        sqlx::query("DELETE FROM mirror_refs WHERE repo_id = $1 AND ref_name = $2")
                            .bind(repo_id)
                            .bind(ref_name)
                            .execute(&mut **tx)
                            .await
                            .map_err(store_error)?;
                    // The row read above is still there (locked, or under
                    // SQLite's write lock); anything else is a lost race.
                    if deleted.rows_affected() != u64::from(exists) {
                        return Err(SetError::Moved);
                    }
                    return Ok(());
                }
                let c = next.conflict;
                // An existing row is locked by the read above (Postgres;
                // SQLite holds the write lock), so it is updated in place. A
                // missing row is inserted without `ON CONFLICT`: of two
                // concurrent first writes, the second fails on the primary
                // key instead of overwriting the first.
                let sql = if exists {
                    "UPDATE mirror_refs SET synced_oid = $3, conflict_at_us = $4, \
                     conflict_aspen_oid = $5, conflict_upstream_oid = $6, updated_at_us = $7 \
                     WHERE repo_id = $1 AND ref_name = $2"
                        .to_owned()
                } else {
                    format!(
                        "INSERT INTO mirror_refs (repo_id, {REF_COLS}) \
                         VALUES ($1, $2, $3, $4, $5, $6, $7)"
                    )
                };
                let written = sqlx::query(AssertSqlSafe(sql))
                    .bind(repo_id)
                    .bind(ref_name)
                    .bind(oid_bytes(next.synced))
                    .bind(c.map(|c| c.at.0))
                    .bind(c.and_then(|c| oid_bytes(c.aspen)))
                    .bind(c.and_then(|c| oid_bytes(c.upstream)))
                    .bind(at.0)
                    .execute(&mut **tx)
                    .await
                    .map_err(|e| {
                        if is_unique_violation(&e, "mirror_refs_pkey") {
                            SetError::Moved
                        } else {
                            SetError::Store(store_error(e))
                        }
                    })?;
                if written.rows_affected() != 1 {
                    return Err(SetError::Moved);
                }
                Ok(())
            })
            .await;
        match result {
            Ok(()) => Ok(true),
            Err(SetError::Moved) => Ok(false),
            Err(SetError::Store(e)) => Err(e),
        }
    }

    /// How many refs, over every mirror, have an open conflict.
    pub async fn mirror_conflict_count(&self) -> Result<u64, StoreError> {
        let row =
            sqlx::query("SELECT count(*) AS n FROM mirror_refs WHERE conflict_at_us IS NOT NULL")
                .fetch_one(&mut *self.admit().await?)
                .await
                .map_err(store_error)?;
        let n: i64 = get(&row, "n")?;
        u64::try_from(n).map_err(|_| corrupt("negative count"))
    }
}
