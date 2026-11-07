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
use crate::rows::{self, VERSION_COLS, VERSION_RETURNING};
use crate::sql::{CHUNK, placeholders};
use crate::store::{Dialect, SqlStore, current_version, insert_commits};

/// What one write's statements ended in.
enum Step {
    Wrote(RefVersion),
    /// The ref was not at the expected version.
    Lost,
    /// The statement ran and wrote nothing because the store refuses the
    /// write (its pack was swept, or it expects a version of another ref);
    /// the transaction it ran in is intact.
    Refused(StoreError),
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

    let dialect = store.dialect();
    let result = if one_statement(dialect, push) {
        // A single statement is its own transaction: no `BEGIN` and
        // `COMMIT` round trips (`write_one`).
        let mut conn = store.admit().await?;
        match write_one(&mut conn, push, &push.writes[0], true).await? {
            Step::Wrote(v) => Ok(vec![v]),
            Step::Lost => Err(Abort::Lost(&push.writes[0].name)),
            Step::Refused(e) => Err(Abort::Store(e)),
        }
    } else {
        let key_share = crate::gc::key_share(store);
        store
            .write(async |tx| write_all(tx, dialect, push, &order, key_share).await)
            .await
    };
    match result {
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

/// What one push of a batch ended in, inside the batch's transaction.
enum Part {
    Wrote(Vec<RefVersion>),
    /// The named ref was not at its expected version; its writes were
    /// undone.
    Lost(RefName),
    /// Refused or failed; its writes were undone.
    Failed(StoreError),
}

/// [`MetaStore::commit_pushes`]: every push in one transaction, each behind
/// its own savepoint, so a push that loses its compare-and-swap (or fails)
/// is undone alone and the others still commit.
///
/// Pushes run in the order of their first ref name (repository, then
/// name), and each takes its own refs in name order, so two batches of
/// single-ref pushes running at once take ref rows in one global order and
/// cannot deadlock (a batch that waits on another waits for its commit).
/// Two pushes of one batch that expect the same version of a ref race as
/// two transactions would: the first in that order wins, and the second
/// violates `ref_versions_seq_unique` inside its savepoint, a lost
/// compare-and-swap.
///
/// If a savepoint statement or the `COMMIT` fails, the batch fails as a
/// whole (the outer `Err`), and no push in it is reported committed.
///
/// [`MetaStore::commit_pushes`]: aspen_core::meta::MetaStore::commit_pushes
pub(crate) async fn commit_pushes(
    store: &SqlStore,
    pushes: &[PushCommit],
) -> Result<Vec<Result<CommitOutcome, StoreError>>, StoreError> {
    let mut out: Vec<Option<Result<CommitOutcome, StoreError>>> = vec![None; pushes.len()];
    let mut valid = Vec::with_capacity(pushes.len());
    for (i, push) in pushes.iter().enumerate() {
        match push.validate() {
            Ok(()) => {
                let mut order: Vec<usize> = (0..push.writes.len()).collect();
                order.sort_by(|&a, &b| push.writes[a].name.cmp(&push.writes[b].name));
                valid.push((i, order));
            }
            Err(e) => out[i] = Some(Err(conflict(format!("malformed push commit: {e}")))),
        }
    }
    if valid.is_empty() {
        return Ok(out.into_iter().map(|o| o.expect("set")).collect());
    }
    valid.sort_by(|(a, oa), (b, ob)| {
        let first = |p: &PushCommit, o: &[usize]| (p.repo, p.writes[o[0]].name.clone());
        first(&pushes[*a], oa)
            .cmp(&first(&pushes[*b], ob))
            .then(a.cmp(b))
    });

    let key_share = crate::gc::key_share(store);
    let dialect = store.dialect();
    // A push that commits in one statement ([`write_one`]) needs no
    // savepoint: the statement writes everything or nothing, and a lost
    // compare-and-swap or a swept pack ends it without an error. Only a
    // statement that fails aborts the transaction, and then the batch runs
    // again with a savepoint around every push, as it always does on
    // SQLite, so a failure still undoes only the push it belongs to.
    let batch = async |savepoints: bool| {
        store
            .write(async |tx| {
                let mut parts = Vec::with_capacity(valid.len());
                for (i, order) in &valid {
                    let push = &pushes[*i];
                    if !savepoints && one_statement(dialect, push) {
                        let write = &push.writes[0];
                        let part = match write_one(tx, push, write, true).await? {
                            Step::Wrote(v) => Part::Wrote(vec![v]),
                            Step::Lost => Part::Lost(write.name.clone()),
                            Step::Refused(e) => Part::Failed(e),
                        };
                        parts.push((*i, part));
                        continue;
                    }
                    crate::tx::savepoint(tx).await?;
                    let result = write_all(tx, dialect, push, order, key_share).await;
                    let part = match result {
                        Ok(created) => {
                            crate::tx::release(tx).await?;
                            Part::Wrote(created)
                        }
                        Err(Abort::Lost(name)) => {
                            crate::tx::rollback_to(tx).await?;
                            Part::Lost(name.clone())
                        }
                        Err(Abort::Store(e)) => {
                            crate::tx::rollback_to(tx).await?;
                            Part::Failed(e)
                        }
                    };
                    parts.push((*i, part));
                }
                Ok::<_, StoreError>(parts)
            })
            .await
    };
    let any_without = valid
        .iter()
        .any(|(i, _)| one_statement(dialect, &pushes[*i]));
    let parts = match batch(false).await {
        Ok(parts) => parts,
        Err(_) if any_without => batch(true).await?,
        Err(e) => return Err(e),
    };

    // Committed. A loser's current version is read now, so it sees the
    // winner whether that was another push of this batch or of another
    // transaction (which the loser's insert waited for).
    for (i, part) in parts {
        out[i] = Some(match part {
            Part::Wrote(created) => Ok(CommitOutcome::Committed(created)),
            Part::Failed(e) => Err(e),
            Part::Lost(name) => {
                let repo = pushes[i].repo;
                match store.admit().await {
                    Ok(mut conn) => current_version(&mut *conn, repo, &name).await.map(|v| {
                        CommitOutcome::CasLost {
                            name,
                            current: v.map(|v| v.id),
                        }
                    }),
                    Err(e) => Err(e),
                }
            }
        });
    }
    Ok(out
        .into_iter()
        .map(|o| o.expect("every push has an outcome"))
        .collect())
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
    conn: &mut crate::conn::Conn,
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
    conn: &mut crate::conn::Conn,
    dialect: Dialect,
    push: &'p PushCommit,
    order: &[usize],
    key_share: &str,
) -> Result<Vec<RefVersion>, Abort<'p>> {
    if one_statement(dialect, push) {
        let write = &push.writes[0];
        return match write_one(conn, push, write, true).await? {
            Step::Wrote(v) => Ok(vec![v]),
            Step::Lost => Err(Abort::Lost(&write.name)),
            Step::Refused(e) => Err(Abort::Store(e)),
        };
    }
    lock_packs(conn, push, key_share).await?;
    let mut created: Vec<Option<RefVersion>> = vec![None; push.writes.len()];
    for &i in order {
        let write = &push.writes[i];
        let step = match dialect {
            Dialect::Postgres => write_one(conn, push, write, false).await?,
            Dialect::Sqlite => write_ref(conn, push, write).await?,
        };
        match step {
            Step::Wrote(version) => created[i] = Some(version),
            Step::Lost => return Err(Abort::Lost(&write.name)),
            Step::Refused(e) => return Err(Abort::Store(e)),
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
    conn: &mut crate::conn::Conn,
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

/// Most commit rows a write carries in its one statement
/// ([`one_statement`]); a write with more uses the transaction, where
/// [`insert_commits`] writes them in chunks.
const ONE_STATEMENT_COMMITS: usize = CHUNK / 5;

/// Whether `push` commits in one statement ([`write_one`]): a single write,
/// with at most [`ONE_STATEMENT_COMMITS`] commit rows, on Postgres. SQLite
/// cannot put an `INSERT` or `UPDATE` in a `WITH` clause, and an atomic
/// multi-ref push needs its writes' commit rows in one global order
/// ([`insert_commits`]), so both use the transaction.
fn one_statement(dialect: Dialect, push: &PushCommit) -> bool {
    dialect == Dialect::Postgres
        && push.writes.len() == 1
        && push.writes[0].commits.len() <= ONE_STATEMENT_COMMITS
}

/// A value bound to a statement that [`write_one`] builds.
enum Arg {
    Int(Option<i64>),
    Bytes(Option<Vec<u8>>),
    Text(String),
}

/// One write on Postgres as one statement: the compare-and-swap on the
/// `refs` row, the new version, and (with `rows`) its commit index rows and
/// jobs, all or nothing.
///
/// The `refs` row moves first, to an ID taken from the version table's
/// identity sequence, and the version is inserted only from the row the
/// move returned, with that ID and the `seq` the move set. So a lost
/// compare-and-swap writes nothing: the update finds no row at the
/// expected version (it waits for a concurrent winner and re-reads the
/// row, as an `UPDATE` does under read committed), or a create finds the
/// `refs` row taken. Every foreign key is checked at the end of the
/// statement, so `refs_version_fk` sees the version the same statement
/// inserted, and a version whose `seq` another push took still violates
/// `ref_versions_seq_unique`, also a lost race. The pack is locked
/// `FOR KEY SHARE` first and the move happens only if it exists, so a pack
/// garbage collection swept is refused as [`StoreError::pack_swept`], as
/// [`lock_packs`] refuses it in a transaction.
async fn write_one(
    conn: &mut crate::conn::Conn,
    push: &PushCommit,
    write: &RefWrite,
    rows: bool,
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
    let mut args: Vec<Arg> = Vec::new();
    let mut arg = |a: Arg| {
        args.push(a);
        format!("${}", args.len())
    };
    let repo = arg(Arg::Int(Some(push.repo.get())));
    let name = arg(Arg::Text(write.name.as_str().to_owned()));
    let mut sql = String::from(
        "WITH id AS (SELECT nextval(pg_get_serial_sequence('ref_versions', 'id')) AS id)",
    );
    let has_pack = match pack {
        Some(p) => {
            let p = arg(Arg::Int(Some(p)));
            sql.push_str(&format!(
                ", p AS (SELECT id FROM packs WHERE id = {p} FOR KEY SHARE)"
            ));
            " AND EXISTS (SELECT 1 FROM p)"
        }
        None => "",
    };
    let parent = match write.expected {
        Some(e) => {
            let e = arg(Arg::Int(Some(e.get())));
            sql.push_str(&format!(
                ", r AS (UPDATE refs SET version_id = (SELECT id FROM id), seq = seq + 1 \
                 WHERE repo_id = {repo} AND ref_name = {name} AND version_id = {e}{has_pack} \
                 RETURNING seq)"
            ));
            e
        }
        None => {
            sql.push_str(&format!(
                ", r AS (INSERT INTO refs (repo_id, ref_name, version_id, seq) \
                 SELECT {repo}, {name}, id.id, 1 FROM id WHERE TRUE{has_pack} \
                 ON CONFLICT (repo_id, ref_name) DO NOTHING RETURNING seq)"
            ));
            "NULL".to_owned()
        }
    };
    let tip = arg(Arg::Bytes(tip));
    let closure = arg(Arg::Int(closure));
    let pack_arg = arg(Arg::Int(pack));
    let trunk = arg(Arg::Int(write.deps.trunk.map(|d| d.get())));
    let scoped = arg(Arg::Int(write.deps.scoped_old.map(|d| d.get())));
    sql.push_str(&format!(
        ", v AS (INSERT INTO ref_versions (id, repo_id, ref_name, seq, parent_version_id, tip_oid, \
         closure_exact, pack_id, trunk_dep_id, scoped_dep_id) OVERRIDING SYSTEM VALUE \
         SELECT (SELECT id FROM id), {repo}, {name}, r.seq, {parent}, {tip}, {closure}, \
         {pack_arg}, {trunk}, {scoped} FROM r RETURNING {VERSION_RETURNING})"
    ));
    if rows {
        // Commit rows in commit ID order, once per commit, as
        // `insert_commits` writes them (its docs: the global order is what
        // keeps two pushes of the same commits from deadlocking).
        let mut commits: Vec<_> = write.commits.iter().collect();
        commits.sort_by_key(|c| c.commit);
        commits.dedup_by_key(|c| c.commit);
        if !commits.is_empty() {
            let values = commits
                .iter()
                .map(|c| {
                    let o = arg(Arg::Bytes(Some(c.commit.as_bytes().to_vec())));
                    let p = arg(Arg::Bytes(Some(rows::parents_column(&c.parents))));
                    let t = arg(Arg::Int(Some(c.committer_time)));
                    let g = arg(Arg::Int(rows::generation_column(c.generation)));
                    let r = arg(Arg::Bytes(c.root_tree.map(|r| r.as_bytes().to_vec())));
                    format!(
                        "(CAST({o} AS BYTEA), CAST({p} AS BYTEA), CAST({t} AS BIGINT), \
                         CAST({g} AS BIGINT), CAST({r} AS BYTEA))"
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            sql.push_str(&format!(
                ", c AS (INSERT INTO commit_index (repo_id, commit_oid, ref_version_id, parents, \
                 committer_time, generation, root_tree) SELECT {repo}, x.o, v.id, x.p, x.t, x.g, \
                 x.r FROM v CROSS JOIN (VALUES {values}) AS x (o, p, t, g, r) ORDER BY x.o \
                 ON CONFLICT (repo_id, commit_oid) DO NOTHING)"
            ));
        }
        if !write.jobs.is_empty() {
            let values = write
                .jobs
                .iter()
                .map(|j| {
                    let k = arg(Arg::Text(job_tag(j.kind).to_owned()));
                    let a = arg(Arg::Int(Some(j.run_after.0)));
                    format!("(CAST({k} AS TEXT), CAST({a} AS BIGINT))")
                })
                .collect::<Vec<_>>()
                .join(", ");
            // A new version has no jobs yet, so every job is new; `DO
            // NOTHING` only keeps the statement as idempotent as
            // `enqueue` is.
            sql.push_str(&format!(
                ", j AS (INSERT INTO jobs (repo_id, kind, ref_version_id, run_after_us) \
                 SELECT {repo}, x.k, v.id, x.a FROM v CROSS JOIN (VALUES {values}) AS x (k, a) \
                 ON CONFLICT DO NOTHING)"
            ));
        }
    }
    let packs_found = if pack.is_some() {
        "(SELECT count(*) FROM p)"
    } else {
        "1"
    };
    let expected_found = match write.expected {
        Some(_) => format!(
            "(SELECT count(*) FROM ref_versions e WHERE e.id = {parent} \
             AND e.repo_id = {repo} AND e.ref_name = {name})"
        ),
        None => "1".to_owned(),
    };
    sql.push_str(&format!(
        " SELECT {packs_found} AS packs_found, {expected_found} AS expected_found, {VERSION_COLS} \
         FROM (SELECT 1 AS one) AS one LEFT JOIN v ON TRUE"
    ));
    let mut q = sqlx::query(AssertSqlSafe(sql));
    for a in args {
        q = match a {
            Arg::Int(i) => q.bind(i),
            Arg::Bytes(b) => q.bind(b),
            Arg::Text(t) => q.bind(t),
        };
    }
    let row = match q.fetch_one(&mut *conn).await {
        Ok(row) => row,
        Err(e) if is_unique_violation(&e, "ref_versions_seq_unique") => return Ok(Step::Lost),
        Err(e) => return Err(store_error(e)),
    };
    if rows::get::<i64>(&row, "packs_found")? == 0 {
        return Ok(Step::Refused(StoreError::pack_swept(format_args!(
            "pack {} no longer exists; retry the push",
            pack.expect("counted only with a pack")
        ))));
    }
    if rows::get::<Option<i64>>(&row, "id")?.is_some() {
        return rows::version(&row).map(Step::Wrote);
    }
    if rows::get::<i64>(&row, "expected_found")? == 0 {
        return Ok(Step::Refused(conflict(format!(
            "{}: expected version {} is not a version of this ref",
            write.name,
            write.expected.map_or(0, |v| v.get())
        ))));
    }
    Ok(Step::Lost)
}

/// The `jobs.kind` of a version job.
fn job_tag(kind: aspen_core::meta::VersionJobKind) -> &'static str {
    // Any ID gives the tag; the version's is filled in by the statement.
    let any = RefVersionId::new(1).expect("1 is a valid row ID");
    kind.for_version(any).tag().as_str()
}
