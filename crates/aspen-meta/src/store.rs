//! [`SqlStore`]: connection handling and the [`MetaStore`] implementation.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use aspen_core::error::StoreError;
use aspen_core::id::{CoverId, JobId, PackChecksum, PackId, RefVersionId, RepoId, Seq, UnixMicros};
use aspen_core::ingest::SortedIds;
use aspen_core::meta::{
    CommitLookup, CommitOutcome, CommitRow, Cover, Job, JobEvent, JobTag, Lease, MetaStore,
    NewCover, NewJob, PackTombstone, PushCommit, RefChanges, RefCursor, RefListQuery, ShallowDepth,
    ShallowRecorded, Statements, SweepBatch,
};
use aspen_core::name::{RefName, RepoName};
use aspen_core::pack::{CoverLevel, NewPack, PackRecord};
use aspen_core::refs::RefVersion;
use aspen_core::repo::{Repo, RepoSettings};
use sqlx::any::{AnyPoolOptions, AnyRow};
use sqlx::{AnyPool, AssertSqlSafe, Connection as _, Executor as _};

use crate::admission::{Admission, Admitted};
use crate::conn::Counter;
use crate::error::{conflict, is_unique_violation, store_error};
use crate::rows::{
    self, COMMIT_COLS, COVER_COLS, COVER_FROM, PACK_COLS, REPO_COLS, VERSION_COLS,
    VERSION_RETURNING,
};
use crate::sql::{CHUNK, PrefixRange, RANGES_PER_QUERY, placeholders, prefix_ranges};

/// The SQL dialect behind a [`SqlStore`], chosen from the URL scheme.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dialect {
    /// `postgres://` or `postgresql://`.
    Postgres,
    /// `sqlite:`.
    Sqlite,
}

impl Dialect {
    /// The dialect of a database URL, or `None` for an unsupported scheme.
    pub fn of_url(url: &str) -> Option<Self> {
        if url.starts_with("postgres://") || url.starts_with("postgresql://") {
            Some(Self::Postgres)
        } else if url.starts_with("sqlite:") {
            Some(Self::Sqlite)
        } else {
            None
        }
    }

    /// The migrations for this dialect.
    pub fn migrations(self) -> &'static sqlx::migrate::Migrator {
        match self {
            Self::Postgres => &crate::POSTGRES_MIGRATIONS,
            Self::Sqlite => &crate::SQLITE_MIGRATIONS,
        }
    }
}

/// The time source for job leases and backoff.
#[derive(Clone)]
pub struct Clock(Arc<dyn Fn() -> UnixMicros + Send + Sync>);

impl Clock {
    /// The system clock.
    pub fn system() -> Self {
        Self(Arc::new(UnixMicros::now))
    }

    /// A custom clock, for tests.
    pub fn from_fn(f: impl Fn() -> UnixMicros + Send + Sync + 'static) -> Self {
        Self(Arc::new(f))
    }

    /// The current time.
    pub fn now(&self) -> UnixMicros {
        (self.0)()
    }
}

impl fmt::Debug for Clock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Clock")
    }
}

/// Connection settings for [`SqlStore::connect_with`].
///
/// A zero duration means no limit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreOptions {
    /// Most connections in the pool (at least 1). An in-memory SQLite
    /// database always gets exactly one.
    pub max_connections: u32,
    /// How long an operation waiting for a connection may go without the
    /// pool making progress before it fails with
    /// [`StoreError::Unavailable`]. Operations take connections in arrival
    /// order, and a wait fails only when no connection was returned to the
    /// pool for this long: the database is unreachable (this also bounds
    /// opening a connection) or every connection is stuck. A busy pool is
    /// never a failure, however long the queue (issue #150; see
    /// `src/admission.rs`).
    pub acquire_timeout: Duration,
    /// How long a statement waits for a lock before it fails with
    /// [`StoreError::Unavailable`]: Postgres `lock_timeout` (SQLSTATE
    /// `55P03`), SQLite `busy_timeout` (`SQLITE_BUSY`). A racing push waits
    /// for the winner's transaction to end this long at most, so it bounds
    /// how long a stuck writer can hold up the others.
    pub lock_timeout: Duration,
    /// How long one statement may run on Postgres before the server cancels
    /// it (`statement_timeout`, SQLSTATE `57014`, which is
    /// [`StoreError::Unavailable`]). SQLite has no equivalent. Migrations
    /// run without either limit.
    pub statement_timeout: Duration,
}

impl Default for StoreOptions {
    /// 10 connections, a 30 s acquire timeout, a 10 s lock timeout and a
    /// 60 s statement timeout. Every store statement is a keyed lookup or a
    /// bounded batch, and write transactions last milliseconds, so any of
    /// these limits firing means something is stuck.
    fn default() -> Self {
        Self {
            max_connections: 10,
            acquire_timeout: Duration::from_secs(30),
            lock_timeout: Duration::from_secs(10),
            statement_timeout: Duration::from_secs(60),
        }
    }
}

/// sqlx's acquire timeout when [`StoreOptions::acquire_timeout`] is zero,
/// and the most it may be: a year, which is for ever for a request.
pub const MAX_ACQUIRE_TIMEOUT: Duration = NO_LIMIT;

const NO_LIMIT: Duration = Duration::from_secs(365 * 24 * 3600);

/// A duration in whole milliseconds for a database setting, where zero
/// means no limit; at least 1 ms otherwise, so a tiny limit is not read as
/// none.
fn setting_ms(d: Duration) -> u128 {
    if d.is_zero() { 0 } else { d.as_millis().max(1) }
}

/// The metadata store on Postgres or SQLite.
///
/// Cheap to clone: clones share one connection pool.
#[derive(Debug, Clone)]
pub struct SqlStore {
    pool: AnyPool,
    admission: Admission,
    dialect: Dialect,
    clock: Clock,
    /// Write transactions begun through this store (shared by clones).
    write_transactions: Arc<std::sync::atomic::AtomicU64>,
    counter: Counter,
}

impl SqlStore {
    /// Connects to `url` with [`StoreOptions::default`]. Does not run
    /// migrations; call [`SqlStore::migrate`].
    ///
    /// Postgres URLs follow libpq; a password left out of the URL is taken
    /// from `PGPASSWORD` (the deploy configuration contract,
    /// `deploy/README.md`). SQLite URLs are `sqlite://<path>?mode=rwc`; an
    /// in-memory database (`sqlite::memory:`) gets a single connection,
    /// because each SQLite connection would otherwise open its own empty
    /// database.
    pub async fn connect(url: &str) -> Result<Self, StoreError> {
        Self::connect_with(url, &StoreOptions::default()).await
    }

    /// Like [`SqlStore::connect`], with explicit [`StoreOptions`].
    pub async fn connect_with(url: &str, opts: &StoreOptions) -> Result<Self, StoreError> {
        sqlx::any::install_default_drivers();
        let dialect = Dialect::of_url(url).ok_or_else(|| StoreError::Unavailable {
            reason: "unsupported database URL scheme (expected postgres:// or sqlite:)".into(),
        })?;
        let memory =
            dialect == Dialect::Sqlite && (url.contains(":memory:") || url.contains("mode=memory"));
        let connections = if memory {
            1
        } else {
            opts.max_connections.max(1)
        };
        // A longer limit is no limit, and keeps `now + timeout` from
        // overflowing inside sqlx.
        let acquire_timeout = opts.acquire_timeout.min(NO_LIMIT);
        let admission = Admission::new(connections, acquire_timeout);
        let returned = admission.clone();
        let mut options = AnyPoolOptions::new()
            .max_connections(connections)
            // Inside a turn from the admission queue the pool has a free
            // slot, so this bounds only opening a connection (and the
            // wait of tooling that uses `pool()` directly).
            .acquire_timeout(if acquire_timeout.is_zero() {
                NO_LIMIT
            } else {
                acquire_timeout
            })
            .after_release(move |_, _| {
                returned.connection_returned();
                Box::pin(async { Ok(true) })
            });
        let lock_ms = setting_ms(opts.lock_timeout);
        match dialect {
            Dialect::Postgres => {
                let statement_ms = setting_ms(opts.statement_timeout);
                options = options.after_connect(move |conn, _| {
                    Box::pin(async move {
                        conn.execute(AssertSqlSafe(format!("SET lock_timeout = {lock_ms}")))
                            .await?;
                        conn.execute(AssertSqlSafe(format!(
                            "SET statement_timeout = {statement_ms}"
                        )))
                        .await?;
                        Ok(())
                    })
                });
            }
            Dialect::Sqlite => {
                // SQLite's busy timeout is an `int` of milliseconds, and 0
                // means "never wait", so "no limit" is the largest value.
                let busy_ms = if lock_ms == 0 {
                    i32::MAX as u128
                } else {
                    lock_ms.min(i32::MAX as u128)
                };
                options = options.after_connect(move |conn, _| {
                    Box::pin(async move {
                        // Foreign keys are on by default in sqlx; say so
                        // anyway, the schema depends on them.
                        conn.execute("PRAGMA foreign_keys = ON").await?;
                        conn.execute(AssertSqlSafe(format!("PRAGMA busy_timeout = {busy_ms}")))
                            .await?;
                        if !memory {
                            // Readers do not block the writer, and vice versa.
                            conn.execute("PRAGMA journal_mode = WAL").await?;
                        }
                        Ok(())
                    })
                });
            }
        }
        let pool = options.connect(url).await.map_err(store_error)?;
        Ok(Self {
            pool,
            admission,
            dialect,
            clock: Clock::system(),
            write_transactions: Arc::default(),
            counter: Counter::default(),
        })
    }

    /// Applies schema migrations that have not been applied yet.
    ///
    /// On Postgres the migrations run on a connection of their own with no
    /// lock or statement timeout (a migration may rewrite a large table, and
    /// waits for another instance's migration lock), which is closed
    /// afterwards so its settings never reach the pool.
    pub async fn migrate(&self) -> Result<(), StoreError> {
        let migrator = self.dialect.migrations();
        let result = match self.dialect {
            Dialect::Postgres => {
                let mut conn = self.pool.acquire().await.map_err(store_error)?.detach();
                let result = async {
                    conn.execute("SET lock_timeout = 0; SET statement_timeout = 0")
                        .await
                        .map_err(sqlx::migrate::MigrateError::from)?;
                    migrator.run(&mut conn).await
                }
                .await;
                let _ = conn.close().await;
                result
            }
            Dialect::Sqlite => migrator.run(self.admit().await?.for_migrations()).await,
        };
        result.map_err(|e| StoreError::Unavailable {
            reason: format!("migrations: {e}"),
        })
    }

    /// Replaces the clock used for job leases and backoff.
    pub fn with_clock(mut self, clock: Clock) -> Self {
        self.clock = clock;
        self
    }

    /// How many write transactions this store (and its clones) has begun
    /// since it connected, of every kind: one per
    /// [`MetaStore::commit_push`] or [`MetaStore::register_pack`], one per
    /// batch of [`MetaStore::commit_pushes`] or
    /// [`MetaStore::register_packs`], and one per job, cover or garbage
    /// collection write. For tests that measure group commit.
    pub fn write_transactions(&self) -> u64 {
        self.write_transactions
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub(crate) fn count_write_transaction(&self) {
        self.write_transactions
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Statements this store (all its clones together) has sent to the
    /// database since it connected, migrations and connection setup left
    /// out (`src/conn.rs`).
    pub fn statements(&self) -> Statements {
        self.counter.totals()
    }

    pub(crate) fn counter(&self) -> &Counter {
        &self.counter
    }

    /// The dialect.
    pub fn dialect(&self) -> Dialect {
        self.dialect
    }

    /// The connection pool, for operator tooling and tests. Writes through
    /// it bypass the store's rules, and every use of it bypasses the
    /// store's admission queue (`src/admission.rs`); the store itself
    /// never uses it, only `SqlStore::admit`.
    pub fn pool(&self) -> &AnyPool {
        &self.pool
    }

    /// Waits for a turn at the pool (in arrival order; see
    /// [`StoreOptions::acquire_timeout`]) and returns it with the pool to
    /// run statements on. Every store operation runs inside one turn,
    /// which ends when the returned value is dropped.
    pub(crate) async fn admit(&self) -> Result<Admitted, StoreError> {
        let permit = self.admission.admit().await?;
        let conn = self.admission.connect(|| self.pool.acquire()).await?;
        Ok(Admitted::new(conn, permit, self.counter.clone()))
    }

    /// A turn without its connection, for a write transaction, which takes
    /// its connection through [`Admission::connect`] itself.
    pub(crate) async fn admit_turn(&self) -> Result<crate::admission::Permit, StoreError> {
        self.admission.admit().await
    }

    /// See [`Admission::connect`].
    pub(crate) async fn connect_in_turn<T, F>(&self, f: impl FnMut() -> F) -> Result<T, StoreError>
    where
        F: std::future::Future<Output = Result<T, sqlx::Error>>,
    {
        self.admission.connect(f).await
    }

    pub(crate) fn now(&self) -> UnixMicros {
        self.clock.now()
    }

    /// `FOR UPDATE SKIP LOCKED` on Postgres; nothing on SQLite, where the
    /// `BEGIN IMMEDIATE` transaction already excludes other writers.
    pub(crate) fn skip_locked(&self) -> &'static str {
        match self.dialect {
            Dialect::Postgres => " FOR UPDATE SKIP LOCKED",
            Dialect::Sqlite => "",
        }
    }

    /// `FOR UPDATE` on Postgres; nothing on SQLite.
    pub(crate) fn for_update(&self) -> &'static str {
        match self.dialect {
            Dialect::Postgres => " FOR UPDATE",
            Dialect::Sqlite => "",
        }
    }

    async fn fetch_repo(
        &self,
        column: &str,
        bind: RepoKey<'_>,
    ) -> Result<Option<Repo>, StoreError> {
        let sql = format!("SELECT {REPO_COLS} FROM repos WHERE {column} = $1");
        let q = sqlx::query(AssertSqlSafe(sql));
        let q = match bind {
            RepoKey::Name(n) => q.bind(n.as_str().to_owned()),
            RepoKey::Id(id) => q.bind(id.get()),
        };
        q.fetch_optional(&mut *self.admit().await?)
            .await
            .map_err(store_error)?
            .as_ref()
            .map(rows::repo)
            .transpose()
    }
}

enum RepoKey<'a> {
    Name(&'a RepoName),
    Id(RepoId),
}

/// Inserts a repository, through any executor; a taken name is
/// [`StoreError::Conflict`].
pub(crate) async fn insert_repo<'e, E>(
    exec: E,
    name: &RepoName,
    settings: &RepoSettings,
) -> Result<Repo, StoreError>
where
    E: sqlx::Executor<'e, Database = sqlx::Any>,
{
    let result = sqlx::query(
        "INSERT INTO repos (name, trunk_ref, allow_force, writable_refs, no_thin, have_tips, shallow) \
         VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id",
    )
    .bind(name.as_str().to_owned())
    .bind(settings.trunk().as_str().to_owned())
    .bind(settings.allow_force().as_str())
    .bind(settings.writable_column())
    .bind(i64::from(settings.no_thin()))
    .bind(i64::from(settings.have_tips()))
    .bind(settings.shallow().as_str())
    .fetch_one(exec)
    .await;
    match result {
        Ok(row) => Ok(Repo {
            id: rows::get::<i64>(&row, "id").and_then(|id| {
                RepoId::new(id).map_err(|e| crate::error::corrupt(format!("repos.id: {e}")))
            })?,
            name: name.clone(),
            settings: settings.clone(),
        }),
        Err(e) if is_unique_violation(&e, "repos_name_unique") => {
            Err(conflict(format!("repository {name} already exists")))
        }
        Err(e) => Err(store_error(e)),
    }
}

/// Reads the current version of a ref, through any executor.
pub(crate) async fn current_version<'e, E>(
    exec: E,
    repo: RepoId,
    name: &RefName,
) -> Result<Option<RefVersion>, StoreError>
where
    E: sqlx::Executor<'e, Database = sqlx::Any>,
{
    let sql = format!(
        "SELECT {VERSION_COLS} FROM refs r JOIN ref_versions v ON v.id = r.version_id \
         WHERE r.repo_id = $1 AND r.ref_name = $2"
    );
    sqlx::query(AssertSqlSafe(sql))
        .bind(repo.get())
        .bind(name.as_str().to_owned())
        .fetch_optional(exec)
        .await
        .map_err(store_error)?
        .as_ref()
        .map(rows::version)
        .transpose()
}

/// Writes commit index rows (a commit that has a row keeps it), in chunks.
///
/// Rows are written in commit ID order, once per commit (the first row
/// given for a commit wins, as it would have in caller order). Inserting a
/// key that another open transaction inserted waits for that transaction,
/// so two pushes writing the same commits in opposite orders would each
/// hold a key the other waits for: a deadlock (Postgres `40P01`), which
/// fails one of the pushes. One global order makes that cycle impossible.
pub(crate) async fn insert_commits(
    conn: &mut crate::conn::Conn,
    repo: RepoId,
    commits: &[CommitRow],
) -> Result<(), StoreError> {
    let mut commits: Vec<&CommitRow> = commits.iter().collect();
    // Stable, so of several rows for one commit the first stays first.
    commits.sort_by_key(|c| c.commit);
    commits.dedup_by_key(|c| c.commit);
    for chunk in commits.chunks(CHUNK / 5) {
        let values = (0..chunk.len())
            .map(|i| format!("({})", placeholders(5 * i + 1, 5)))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "INSERT INTO commit_index (repo_id, commit_oid, ref_version_id, parents, committer_time) \
             VALUES {values} ON CONFLICT (repo_id, commit_oid) DO NOTHING"
        );
        let mut q = sqlx::query(AssertSqlSafe(sql));
        for c in chunk {
            q = q
                .bind(repo.get())
                .bind(c.commit.as_bytes().to_vec())
                .bind(c.ref_version.get())
                .bind(rows::parents_column(&c.parents))
                .bind(c.committer_time);
        }
        q.execute(&mut *conn).await.map_err(store_error)?;
    }
    Ok(())
}

/// Runs `SELECT {cols} FROM {table} WHERE {key} IN (...)` in chunks, with
/// `prefix` binds (for example `repo_id = $1`) before the list.
async fn fetch_in(
    conn: &mut crate::conn::Conn,
    sql_head: &str,
    prefix: &[i64],
    keys: Vec<KeyValue>,
) -> Result<Vec<AnyRow>, StoreError> {
    let mut out = Vec::new();
    for chunk in keys.chunks(CHUNK) {
        let sql = format!(
            "{sql_head} ({})",
            placeholders(prefix.len() + 1, chunk.len())
        );
        let mut q = sqlx::query(AssertSqlSafe(sql));
        for p in prefix {
            q = q.bind(*p);
        }
        for k in chunk {
            q = match k {
                KeyValue::Int(i) => q.bind(*i),
                KeyValue::Bytes(b) => q.bind(b.clone()),
            };
        }
        out.extend(q.fetch_all(&mut *conn).await.map_err(store_error)?);
    }
    Ok(out)
}

enum KeyValue {
    Int(i64),
    Bytes(Vec<u8>),
}

/// One statement of a ref listing: live refs of `repo` after `after` that
/// lie in one of `ranges` (every ref when `None`), in name order, at most
/// `limit` of them.
async fn list_ref_ranges(
    store: &SqlStore,
    repo: RepoId,
    after: Option<&str>,
    ranges: Option<&[PrefixRange]>,
    limit: u32,
) -> Result<Vec<RefVersion>, StoreError> {
    let mut sql = format!(
        "SELECT {VERSION_COLS} FROM refs r JOIN ref_versions v ON v.id = r.version_id \
         WHERE r.repo_id = $1 AND v.tip_oid IS NOT NULL"
    );
    // Binds `value` after `$1` (the repository) and returns its placeholder.
    fn next<'a>(texts: &mut Vec<&'a str>, value: &'a str) -> String {
        texts.push(value);
        format!("${}", texts.len() + 1)
    }
    let mut texts: Vec<&str> = Vec::new();
    if let Some(after) = after {
        let p = next(&mut texts, after);
        sql.push_str(&format!(" AND r.ref_name > {p}"));
    }
    if let Some(ranges) = ranges {
        let mut terms = Vec::new();
        for range in ranges {
            let lo = next(&mut texts, &range.lo);
            match &range.hi {
                Some(hi) => {
                    let hi = next(&mut texts, hi);
                    terms.push(format!("(r.ref_name >= {lo} AND r.ref_name < {hi})"));
                }
                None => terms.push(format!("r.ref_name >= {lo}")),
            }
        }
        sql.push_str(&format!(" AND ({})", terms.join(" OR ")));
    }
    sql.push_str(&format!(" ORDER BY r.ref_name LIMIT ${}", texts.len() + 2));
    let mut q = sqlx::query(AssertSqlSafe(sql)).bind(repo.get());
    for t in texts {
        q = q.bind(t);
    }
    q = q.bind(i64::from(limit));
    q.fetch_all(&mut *store.admit().await?)
        .await
        .map_err(store_error)?
        .iter()
        .map(rows::version)
        .collect()
}

impl MetaStore for SqlStore {
    async fn create_repo(
        &self,
        name: &RepoName,
        settings: &RepoSettings,
    ) -> Result<Repo, StoreError> {
        insert_repo(&mut *self.admit().await?, name, settings).await
    }

    async fn repo(&self, name: &RepoName) -> Result<Option<Repo>, StoreError> {
        self.fetch_repo("name", RepoKey::Name(name)).await
    }

    async fn repo_by_id(&self, id: RepoId) -> Result<Option<Repo>, StoreError> {
        self.fetch_repo("id", RepoKey::Id(id)).await
    }

    async fn list_repos(&self, after: Option<RepoId>, limit: u32) -> Result<Vec<Repo>, StoreError> {
        let sql = format!("SELECT {REPO_COLS} FROM repos WHERE id > $1 ORDER BY id LIMIT $2");
        sqlx::query(AssertSqlSafe(sql))
            .bind(after.map_or(0, |a| a.get()))
            .bind(i64::from(limit))
            .fetch_all(&mut *self.admit().await?)
            .await
            .map_err(store_error)?
            .iter()
            .map(rows::repo)
            .collect()
    }

    async fn current_version(
        &self,
        repo: RepoId,
        name: &RefName,
    ) -> Result<Option<RefVersion>, StoreError> {
        current_version(&mut *self.admit().await?, repo, name).await
    }

    async fn current_versions(
        &self,
        repo: RepoId,
        names: &[RefName],
    ) -> Result<Vec<Option<RefVersion>>, StoreError> {
        let mut found: Vec<RefVersion> = Vec::new();
        let mut conn = self.admit().await?;
        for chunk in names.chunks(CHUNK) {
            let sql = format!(
                "SELECT {VERSION_COLS} FROM refs r JOIN ref_versions v ON v.id = r.version_id \
                 WHERE r.repo_id = $1 AND r.ref_name IN ({})",
                placeholders(2, chunk.len())
            );
            let mut q = sqlx::query(AssertSqlSafe(sql)).bind(repo.get());
            for n in chunk {
                q = q.bind(n.as_str().to_owned());
            }
            for row in q.fetch_all(&mut *conn).await.map_err(store_error)? {
                found.push(rows::version(&row)?);
            }
        }
        Ok(names
            .iter()
            .map(|n| found.iter().find(|v| &v.name == n).cloned())
            .collect())
    }

    async fn current_and_recent(
        &self,
        repo: RepoId,
        name: &RefName,
        after: Option<Seq>,
        window: u64,
    ) -> Result<Option<Vec<RefVersion>>, StoreError> {
        // `c` is the current version; the versions of the ref come by the
        // `ref_versions_seq_unique` index, from `seq` past both bounds up
        // to the current one.
        let sql = format!(
            "SELECT {VERSION_COLS} FROM refs r JOIN ref_versions c ON c.id = r.version_id \
             JOIN ref_versions v ON v.repo_id = c.repo_id AND v.ref_name = c.ref_name \
             AND v.seq <= c.seq AND (v.seq = c.seq OR (v.seq > $3 AND v.seq >= c.seq - $4)) \
             WHERE r.repo_id = $1 AND r.ref_name = $2 ORDER BY v.seq DESC"
        );
        let window = i64::try_from(window).unwrap_or(i64::MAX);
        let rows = sqlx::query(AssertSqlSafe(sql))
            .bind(repo.get())
            .bind(name.as_str().to_owned())
            .bind(after.map_or(0, Seq::to_db))
            .bind(window)
            .fetch_all(&mut *self.admit().await?)
            .await
            .map_err(store_error)?;
        if rows.is_empty() {
            return Ok(None);
        }
        rows.iter()
            .map(rows::version)
            .collect::<Result<_, _>>()
            .map(Some)
    }

    async fn reachable_versions(
        &self,
        from: &[RefVersionId],
        stops: &[RefVersionId],
        limit: u32,
    ) -> Result<Vec<RefVersion>, StoreError> {
        let mut out = Vec::new();
        let mut conn = self.admit().await?;
        let stops = &stops[..stops.len().min(CHUNK)];
        for chunk in from.chunks(CHUNK) {
            let seeds = placeholders(1, chunk.len());
            let stop = if stops.is_empty() {
                String::new()
            } else {
                format!(
                    " WHERE w.id NOT IN ({})",
                    placeholders(chunk.len() + 1, stops.len())
                )
            };
            // `UNION` (not `UNION ALL`) ends the walk at versions already
            // read, so it terminates on any dependency graph, and both
            // engines produce the seeds before anything they reach, so the
            // limit never cuts a seed.
            let sql = format!(
                "WITH RECURSIVE walk ({VERSION_RETURNING}) AS (\
                 SELECT {VERSION_COLS} FROM ref_versions v WHERE v.id IN ({seeds}) \
                 UNION \
                 SELECT {VERSION_COLS} FROM walk w \
                 JOIN ref_versions v ON v.id = w.trunk_dep_id OR v.id = w.scoped_dep_id{stop}) \
                 SELECT {VERSION_RETURNING} FROM walk LIMIT ${}",
                chunk.len() + stops.len() + 1
            );
            let mut q = sqlx::query(AssertSqlSafe(sql));
            for id in chunk.iter().chain(stops) {
                q = q.bind(id.get());
            }
            let n = i64::try_from(chunk.len()).expect("a chunk") + i64::from(limit);
            for row in q.bind(n).fetch_all(&mut *conn).await.map_err(store_error)? {
                out.push(rows::version(&row)?);
            }
        }
        Ok(out)
    }

    async fn count_statements<F>(&self, work: F) -> (F::Output, Option<Statements>)
    where
        F: std::future::Future + Send,
        F::Output: Send,
    {
        let (out, n) = crate::conn::count(work).await;
        (out, Some(n))
    }

    async fn ref_version(&self, id: RefVersionId) -> Result<Option<RefVersion>, StoreError> {
        let sql = format!("SELECT {VERSION_COLS} FROM ref_versions v WHERE v.id = $1");
        sqlx::query(AssertSqlSafe(sql))
            .bind(id.get())
            .fetch_optional(&mut *self.admit().await?)
            .await
            .map_err(store_error)?
            .as_ref()
            .map(rows::version)
            .transpose()
    }

    async fn versions_by_seq(
        &self,
        repo: RepoId,
        name: &RefName,
        seqs: &[Seq],
    ) -> Result<Vec<RefVersion>, StoreError> {
        let mut out = Vec::with_capacity(seqs.len());
        for chunk in seqs.chunks(CHUNK) {
            let sql = format!(
                "SELECT {VERSION_COLS} FROM ref_versions v \
                 WHERE v.repo_id = $1 AND v.ref_name = $2 AND v.seq IN ({}) ORDER BY v.seq DESC",
                placeholders(3, chunk.len())
            );
            let mut q = sqlx::query(AssertSqlSafe(sql))
                .bind(repo.get())
                .bind(name.as_str().to_owned());
            for s in chunk {
                q = q.bind(s.to_db());
            }
            let rows = q
                .fetch_all(&mut *self.admit().await?)
                .await
                .map_err(store_error)?;
            for row in rows {
                out.push(rows::version(&row)?);
            }
        }
        // In the order asked, missing ones left out.
        let mut ordered = Vec::with_capacity(out.len());
        for s in seqs {
            if let Some(v) = out.iter().find(|v| v.seq == *s)
                && !ordered.iter().any(|o: &RefVersion| o.id == v.id)
            {
                ordered.push(v.clone());
            }
        }
        Ok(ordered)
    }

    async fn list_refs(
        &self,
        repo: RepoId,
        query: &RefListQuery,
    ) -> Result<Vec<RefVersion>, StoreError> {
        if query.limit == 0 {
            return Ok(Vec::new());
        }
        let after = query.after.as_ref().map(RefName::as_str);
        // No prefixes, or an empty one, match everything.
        let Some(ranges) = prefix_ranges(&query.prefixes) else {
            return list_ref_ranges(self, repo, after, None, query.limit).await;
        };
        // A range that ends by the cursor has nothing this page can list,
        // so later pages bind only the ranges still ahead of them.
        let ranges: Vec<PrefixRange> = ranges
            .into_iter()
            .filter(|r| !after.is_some_and(|a| r.ends_by(a)))
            .collect();
        // The ranges are disjoint and in name order, so every name one
        // chunk lists sorts before every name of the next: reading chunks
        // in turn until the page is full lists the page in order. A chunk
        // bounds what one statement binds, whatever the caller sends.
        let mut out = Vec::new();
        let mut left = query.limit;
        for chunk in ranges.chunks(RANGES_PER_QUERY) {
            let rows = list_ref_ranges(self, repo, after, Some(chunk), left).await?;
            left = left.saturating_sub(u32::try_from(rows.len()).unwrap_or(u32::MAX));
            out.extend(rows);
            if left == 0 {
                break;
            }
        }
        Ok(out)
    }

    async fn ref_changes(
        &self,
        repo: RepoId,
        since: Option<&RefCursor>,
        limit: u32,
    ) -> Result<Option<RefChanges>, StoreError> {
        crate::ref_changes::ref_changes(self, repo, since, limit)
            .await
            .map(Some)
    }

    async fn register_pack(&self, pack: &NewPack) -> Result<PackRecord, StoreError> {
        crate::gc::register_pack(self, pack).await
    }

    async fn packs(&self, ids: &[PackId]) -> Result<Vec<PackRecord>, StoreError> {
        let head = format!("SELECT {PACK_COLS} FROM packs WHERE id IN");
        let keys = ids.iter().map(|i| KeyValue::Int(i.get())).collect();
        let found = fetch_in(&mut *self.admit().await?, &head, &[], keys)
            .await?
            .iter()
            .map(|row| rows::pack(row).map(|p| (p.id, p)))
            .collect::<Result<std::collections::BTreeMap<_, _>, _>>()?;
        ids.iter()
            .map(|id| {
                found.get(id).cloned().ok_or_else(|| StoreError::NotFound {
                    what: format!("pack {id}"),
                })
            })
            .collect()
    }

    async fn commit_push(&self, push: &PushCommit) -> Result<CommitOutcome, StoreError> {
        crate::push::commit_push(self, push).await
    }

    async fn commit_pushes(
        &self,
        pushes: &[PushCommit],
    ) -> Result<Vec<Result<CommitOutcome, StoreError>>, StoreError> {
        crate::push::commit_pushes(self, pushes).await
    }

    async fn register_packs(
        &self,
        packs: &[NewPack],
    ) -> Result<Vec<Result<PackRecord, StoreError>>, StoreError> {
        crate::gc::register_packs(self, packs).await
    }

    async fn lookup_commits(
        &self,
        repo: RepoId,
        ids: &SortedIds,
    ) -> Result<Vec<CommitLookup>, StoreError> {
        let head =
            format!("SELECT {COMMIT_COLS} FROM commit_index WHERE repo_id = $1 AND commit_oid IN");
        let keys = ids
            .as_slice()
            .iter()
            .map(|id| KeyValue::Bytes(id.as_bytes().to_vec()))
            .collect();
        let mut found = fetch_in(&mut *self.admit().await?, &head, &[repo.get()], keys)
            .await?
            .iter()
            .map(rows::commit)
            .collect::<Result<Vec<_>, _>>()?;
        found.sort_unstable_by_key(|c| c.commit);
        Ok(ids
            .as_slice()
            .iter()
            .map(|id| match found.binary_search_by_key(id, |c| c.commit) {
                Ok(i) => CommitLookup::Indexed(found[i].clone()),
                Err(_) => CommitLookup::NotIndexed,
            })
            .collect())
    }

    async fn index_commits(&self, repo: RepoId, rows: &[CommitRow]) -> Result<(), StoreError> {
        if rows.is_empty() {
            return Ok(());
        }
        self.write(async |tx| insert_commits(tx, repo, rows).await)
            .await
    }

    async fn covers(&self, repo: RepoId, name: &RefName) -> Result<Vec<Cover>, StoreError> {
        let sql = format!(
            "SELECT {COVER_COLS} FROM {COVER_FROM} WHERE c.repo_id = $1 AND c.ref_name = $2 \
             ORDER BY c.level, c.first_seq"
        );
        sqlx::query(AssertSqlSafe(sql))
            .bind(repo.get())
            .bind(name.as_str().to_owned())
            .fetch_all(&mut *self.admit().await?)
            .await
            .map_err(store_error)?
            .iter()
            .map(rows::cover)
            .collect()
    }

    async fn covers_at(
        &self,
        repo: RepoId,
        name: &RefName,
        level: CoverLevel,
        firsts: &[Seq],
    ) -> Result<Vec<Cover>, StoreError> {
        let mut out = Vec::new();
        for chunk in firsts.chunks(CHUNK) {
            let sql = format!(
                "SELECT {COVER_COLS} FROM {COVER_FROM} WHERE c.repo_id = $1 AND c.ref_name = $2 \
                 AND c.level = $3 AND c.first_seq IN ({})",
                placeholders(4, chunk.len())
            );
            let mut q = sqlx::query(AssertSqlSafe(sql))
                .bind(repo.get())
                .bind(name.as_str().to_owned())
                .bind(i64::from(level.get()));
            for s in chunk {
                q = q.bind(s.to_db());
            }
            let rows = q
                .fetch_all(&mut *self.admit().await?)
                .await
                .map_err(store_error)?;
            for row in rows {
                out.push(rows::cover(&row)?);
            }
        }
        Ok(out)
    }

    async fn insert_cover(&self, cover: &NewCover) -> Result<Cover, StoreError> {
        crate::covers::insert_cover(self, cover).await
    }

    async fn set_cover_pack(&self, cover: CoverId, pack: PackId) -> Result<Cover, StoreError> {
        crate::covers::set_cover_pack(self, cover, pack).await
    }

    async fn pack_by_checksum(
        &self,
        repo: RepoId,
        checksum: &PackChecksum,
    ) -> Result<Option<PackRecord>, StoreError> {
        let sql = format!("SELECT {PACK_COLS} FROM packs WHERE repo_id = $1 AND checksum = $2");
        sqlx::query(AssertSqlSafe(sql))
            .bind(repo.get())
            .bind(checksum.as_bytes().to_vec())
            .fetch_optional(&mut *self.admit().await?)
            .await
            .map_err(store_error)?
            .as_ref()
            .map(rows::pack)
            .transpose()
    }

    async fn acquire_lease(
        &self,
        name: &str,
        owner: &str,
        ttl: Duration,
    ) -> Result<Option<Lease>, StoreError> {
        crate::lease::acquire_lease(self, name, owner, ttl).await
    }

    async fn sweep_orphan_packs(
        &self,
        lease: &Lease,
        grace: Duration,
        limit: u32,
    ) -> Result<SweepBatch, StoreError> {
        crate::gc::sweep_orphan_packs(self, lease, grace, limit).await
    }

    async fn pending_pack_deletes(&self, limit: u32) -> Result<Vec<PackTombstone>, StoreError> {
        crate::gc::pending_pack_deletes(self, limit).await
    }

    async fn finish_pack_deletes(&self, done: &[PackTombstone]) -> Result<(), StoreError> {
        crate::gc::finish_pack_deletes(self, done).await
    }

    async fn shallow_pack(
        &self,
        repo: RepoId,
        version: RefVersionId,
        depth: ShallowDepth,
    ) -> Result<Option<PackRecord>, StoreError> {
        crate::shallow::shallow_pack(self, repo, version, depth).await
    }

    async fn set_shallow_pack(
        &self,
        repo: RepoId,
        version: RefVersionId,
        depth: ShallowDepth,
        pack: PackId,
        max_repo_bytes: u64,
    ) -> Result<ShallowRecorded, StoreError> {
        crate::shallow::set_shallow_pack(self, repo, version, depth, pack, max_repo_bytes).await
    }

    async fn mark_shallow_pack_used(
        &self,
        repo: RepoId,
        version: RefVersionId,
        depth: ShallowDepth,
    ) -> Result<(), StoreError> {
        crate::shallow::mark_shallow_pack_used(self, repo, version, depth).await
    }

    async fn evict_shallow_packs(
        &self,
        superseded_after: Duration,
        unused_after: Duration,
        limit: u32,
    ) -> Result<u64, StoreError> {
        crate::shallow::evict_shallow_packs(self, superseded_after, unused_after, limit).await
    }

    async fn enqueue_jobs(&self, jobs: &[NewJob]) -> Result<Vec<JobId>, StoreError> {
        if jobs.is_empty() {
            return Ok(Vec::new());
        }
        self.write(async |tx| {
            let mut ids = Vec::with_capacity(jobs.len());
            for job in jobs {
                ids.push(crate::jobs::enqueue(tx, job).await?);
            }
            Ok(ids)
        })
        .await
    }

    async fn claim_job(
        &self,
        tags: &[JobTag],
        owner: &str,
        lease: Duration,
    ) -> Result<Option<Job>, StoreError> {
        crate::jobs::claim(self, tags, owner, lease).await
    }

    async fn release_jobs(
        &self,
        tags: &[JobTag],
        owner: &str,
        reason: &str,
    ) -> Result<u64, StoreError> {
        crate::jobs::release(self, tags, owner, reason).await
    }

    async fn add_compaction_heat(
        &self,
        heat: &[aspen_core::meta::CompactionHeat],
    ) -> Result<u64, StoreError> {
        crate::jobs::add_heat(self, heat).await
    }

    async fn update_job(&self, id: JobId, event: JobEvent) -> Result<Job, StoreError> {
        crate::jobs::update(self, id, event).await
    }
}
