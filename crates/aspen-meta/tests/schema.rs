//! Schema v1 applied to real databases.
//!
//! SQLite runs in-process. Postgres runs as a throwaway cluster started from
//! the local PostgreSQL binaries (`initdb` + `pg_ctl`, listening only on a
//! Unix socket in a temporary directory). GitHub's `ubuntu-24.04` runners
//! ship those binaries under `/usr/lib/postgresql/<major>/bin`; elsewhere set
//! `ASPEN_TEST_PG_BIN` to a directory holding `initdb` and `pg_ctl`. Without
//! binaries the Postgres test is skipped locally and fails under `CI`.
//!
//! Every named constraint in the migrations has a case: a row that a valid
//! fixture row was changed into so that it breaks that constraint, and the
//! test asserts the database names that constraint in its error (Postgres
//! reports the name for every kind; SQLite names `CHECK` constraints in the
//! message and reports only the kind for unique and foreign-key ones, whose
//! names the Postgres run checks on the same rows). A coverage test fails
//! when a constraint in either file has no case. Each case runs in a
//! transaction that is rolled back, so cases do not affect each other.

mod common;

use std::collections::BTreeSet;

use common::{PgCluster, pg_bin_dir};

use aspen_core::meta::{JobState, JobTag};
use aspen_core::repo::{ForcePolicy, ShallowPolicy};
use sqlx::any::{AnyPoolOptions, install_default_drivers};
use sqlx::error::ErrorKind;
use sqlx::{AnyPool, AssertSqlSafe, Row as _};

/// Every migration of each dialect, in order.
const PG_SQL: &str = concat!(
    include_str!("../migrations/postgres/0001_schema_v1.sql"),
    include_str!("../migrations/postgres/0002_tokens.sql"),
    include_str!("../migrations/postgres/0003_mirrors.sql"),
    include_str!("../migrations/postgres/0004_ssh_keys.sql"),
    include_str!("../migrations/postgres/0005_compaction.sql"),
    include_str!("../migrations/postgres/0006_shallow_packs.sql"),
    include_str!("../migrations/postgres/0007_shallow_pack_use.sql"),
    include_str!("../migrations/postgres/0008_job_priority.sql"),
    include_str!("../migrations/postgres/0009_gc_cursor.sql"),
    include_str!("../migrations/postgres/0010_ref_version_txid.sql"),
);
const SQLITE_SQL: &str = concat!(
    include_str!("../migrations/sqlite/0001_schema_v1.sql"),
    include_str!("../migrations/sqlite/0002_tokens.sql"),
    include_str!("../migrations/sqlite/0003_mirrors.sql"),
    include_str!("../migrations/sqlite/0004_ssh_keys.sql"),
    include_str!("../migrations/sqlite/0005_compaction.sql"),
    include_str!("../migrations/sqlite/0006_shallow_packs.sql"),
    include_str!("../migrations/sqlite/0007_shallow_pack_use.sql"),
    include_str!("../migrations/sqlite/0008_job_priority.sql"),
    include_str!("../migrations/sqlite/0009_gc_cursor.sql"),
    include_str!("../migrations/sqlite/0010_ref_version_txid.sql"),
);

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Dialect {
    Postgres,
    Sqlite,
}

/// A bound value.
#[derive(Clone, Debug)]
enum V {
    I(i64),
    B(Vec<u8>),
    T(&'static str),
    /// NULL in an integer column.
    Null,
    /// NULL in a BYTEA/BLOB column (Postgres needs the type).
    NullB,
    /// NULL in a TEXT column.
    NullT,
}
use V::{B, I, Null, NullB, NullT, T};

fn oid(n: u8) -> V {
    B(vec![n; 20])
}

/// One row to insert.
#[derive(Clone, Debug)]
struct Row {
    table: &'static str,
    cols: Vec<(&'static str, V)>,
}

impl Row {
    fn new(table: &'static str, cols: &[(&'static str, V)]) -> Row {
        Row {
            table,
            cols: cols.to_vec(),
        }
    }

    /// The row with `col` set to `v` (added if absent).
    fn set(&self, col: &'static str, v: V) -> Row {
        let mut r = self.clone();
        match r.cols.iter_mut().find(|(c, _)| *c == col) {
            Some(slot) => slot.1 = v,
            None => r.cols.push((col, v)),
        }
        r
    }

    fn with(&self, changes: &[(&'static str, V)]) -> Row {
        changes
            .iter()
            .fold(self.clone(), |r, (c, v)| r.set(c, v.clone()))
    }

    fn sql(&self, dialect: Dialect, returning: bool) -> String {
        let cols: Vec<&str> = self.cols.iter().map(|(c, _)| *c).collect();
        let params: Vec<String> = (1..=cols.len()).map(|i| format!("${i}")).collect();
        // `tokens.id` is a plain key, not an identity column; `ssh_keys`
        // has no `id`.
        let identity = self.table != "tokens";
        let overriding = if dialect == Dialect::Postgres && identity && cols.contains(&"id") {
            " OVERRIDING SYSTEM VALUE"
        } else {
            ""
        };
        format!(
            "INSERT INTO {} ({}){overriding} VALUES ({}){}",
            self.table,
            cols.join(", "),
            params.join(", "),
            if returning { " RETURNING id" } else { "" }
        )
    }
}

fn bind<'q>(
    mut q: sqlx::query::Query<'q, sqlx::Any, sqlx::any::AnyArguments>,
    binds: &[V],
) -> sqlx::query::Query<'q, sqlx::Any, sqlx::any::AnyArguments> {
    for b in binds {
        q = match b {
            I(i) => q.bind(*i),
            B(b) => q.bind(b.clone()),
            T(t) => q.bind(*t),
            Null => q.bind(Option::<i64>::None),
            NullB => q.bind(Option::<Vec<u8>>::None),
            NullT => q.bind(Option::<String>::None),
        };
    }
    q
}

/// The error kind a constraint raises, from its name.
fn kind_of(constraint: &str) -> ErrorKind {
    if constraint.ends_with("_fk") {
        ErrorKind::ForeignKeyViolation
    } else if constraint.ends_with("_pkey")
        || constraint.ends_with("_unique")
        || constraint.starts_with("jobs_open_")
    {
        ErrorKind::UniqueViolation
    } else {
        ErrorKind::CheckViolation
    }
}

struct Db {
    pool: AnyPool,
    dialect: Dialect,
}

impl Db {
    async fn connect(url: &str, dialect: Dialect) -> Db {
        install_default_drivers();
        let pool = AnyPoolOptions::new()
            .max_connections(1)
            .connect(url)
            .await
            .unwrap_or_else(|e| panic!("connect {dialect:?}: {e}"));
        Db { pool, dialect }
    }

    async fn migrate(&self) {
        let migrator = match self.dialect {
            Dialect::Postgres => &aspen_meta::POSTGRES_MIGRATIONS,
            Dialect::Sqlite => &aspen_meta::SQLITE_MIGRATIONS,
        };
        migrator.run(&self.pool).await.expect("migrations apply");
    }

    async fn exec(&self, sql: &str, binds: &[V]) -> Result<u64, sqlx::Error> {
        bind(sqlx::query(AssertSqlSafe(sql.to_owned())), binds)
            .execute(&self.pool)
            .await
            .map(|r| r.rows_affected())
    }

    /// Inserts a fixture row for good and returns its `id`.
    async fn insert(&self, row: &Row) -> i64 {
        let binds: Vec<V> = row.cols.iter().map(|(_, v)| v.clone()).collect();
        bind(
            sqlx::query(AssertSqlSafe(row.sql(self.dialect, true))),
            &binds,
        )
        .fetch_one(&self.pool)
        .await
        .unwrap_or_else(|e| panic!("{:?}: fixture {row:?}: {e}", self.dialect))
        .get::<i64, _>(0)
    }

    /// Inserts a fixture row without an `id` column.
    async fn insert_keyless(&self, row: &Row) {
        let binds: Vec<V> = row.cols.iter().map(|(_, v)| v.clone()).collect();
        if let Err(e) = self.exec(&row.sql(self.dialect, false), &binds).await {
            panic!("{:?}: fixture {row:?}: {e}", self.dialect);
        }
    }

    /// Inserts `row` inside a transaction that is then rolled back.
    async fn try_row(&self, row: &Row) -> Result<(), sqlx::Error> {
        let binds: Vec<V> = row.cols.iter().map(|(_, v)| v.clone()).collect();
        let mut tx = self.pool.begin().await.expect("begin");
        let result = bind(
            sqlx::query(AssertSqlSafe(row.sql(self.dialect, false))),
            &binds,
        )
        .execute(&mut *tx)
        .await
        .map(|_| ());
        tx.rollback().await.expect("rollback");
        result
    }

    async fn accepts(&self, what: &str, row: &Row) {
        if let Err(e) = self.try_row(row).await {
            panic!(
                "{:?}: {what}: valid row refused: {e}\n{row:?}",
                self.dialect
            );
        }
    }

    /// Asserts `row` is refused, and that `constraint` is what refused it.
    async fn rejects(&self, constraint: &str, row: &Row) {
        let kind = kind_of(constraint);
        match self.try_row(row).await {
            Ok(()) => panic!("{:?}: {constraint}: row accepted\n{row:?}", self.dialect),
            Err(sqlx::Error::Database(e)) => {
                let msg = e.message().to_owned();
                assert_eq!(
                    e.kind(),
                    kind,
                    "{:?}: {constraint}: wrong kind: {msg}",
                    self.dialect
                );
                match self.dialect {
                    Dialect::Postgres => assert_eq!(
                        e.constraint(),
                        Some(constraint),
                        "Postgres: expected {constraint}: {msg}"
                    ),
                    Dialect::Sqlite => match kind {
                        ErrorKind::CheckViolation => assert!(
                            msg == format!("CHECK constraint failed: {constraint}"),
                            "SQLite: expected {constraint}: {msg}"
                        ),
                        ErrorKind::UniqueViolation => assert!(
                            msg.starts_with("UNIQUE constraint failed:")
                                && msg.contains(&format!("{}.", row.table)),
                            "SQLite: expected {constraint}: {msg}"
                        ),
                        _ => assert_eq!(msg, "FOREIGN KEY constraint failed"),
                    },
                }
            }
            Err(e) => panic!("{:?}: {constraint}: unexpected error {e}", self.dialect),
        }
    }

    /// `(table, column, type class, not null)` for every column.
    async fn columns(&self) -> BTreeSet<(String, String, String, bool)> {
        let sql = match self.dialect {
            Dialect::Postgres => {
                "SELECT table_name::text, column_name::text, data_type::text, \
                 (is_nullable = 'NO')::int4 FROM information_schema.columns \
                 WHERE table_schema = 'public' AND table_name NOT LIKE '\\_sqlx%'"
            }
            Dialect::Sqlite => {
                "SELECT m.name, c.name, lower(c.type), c.\"notnull\" \
                 FROM sqlite_master m, pragma_table_xinfo(m.name) c \
                 WHERE m.type = 'table' AND m.name NOT LIKE 'sqlite%' \
                 AND m.name NOT LIKE '\\_sqlx%' ESCAPE '\\'"
            }
        };
        let class = |t: &str| match t {
            "bigint" | "smallint" | "integer" => "integer".to_owned(),
            "bytea" | "blob" => "blob".to_owned(),
            other => other.to_owned(),
        };
        sqlx::query(sql)
            .fetch_all(&self.pool)
            .await
            .expect("list columns")
            .into_iter()
            .map(|r| {
                let notnull: i64 = match self.dialect {
                    Dialect::Postgres => i64::from(r.get::<i32, _>(3)),
                    Dialect::Sqlite => r.get::<i64, _>(3),
                };
                (
                    r.get::<String, _>(0),
                    r.get::<String, _>(1),
                    class(&r.get::<String, _>(2)),
                    notnull == 1,
                )
            })
            .collect()
    }

    /// Every index except primary keys, as
    /// `(table, name, unique, columns, predicate)`. `name` is empty for
    /// indexes that back a UNIQUE constraint, whose index SQLite names
    /// itself; predicates are normalized (no spaces, parentheses or casts).
    /// On Postgres an index backs a constraint when a primary key, unique
    /// or exclusion constraint owns it; a foreign key that references a
    /// unique index (such as `covers_id_span_unique`) also records it in
    /// `conindid` but does not make it the constraint's own.
    async fn indexes(&self) -> BTreeSet<(String, String, bool, String, String)> {
        let norm = |p: &str| {
            p.to_lowercase()
                .replace("::text", "")
                .chars()
                .filter(|c| !c.is_whitespace() && *c != '(' && *c != ')')
                .collect::<String>()
        };
        let mut out = BTreeSet::new();
        match self.dialect {
            Dialect::Postgres => {
                let sql = "SELECT t.relname::text, i.relname::text, ix.indisunique::int4, \
                    array_to_string(ARRAY(SELECT a.attname::text FROM unnest(ix.indkey::int2[]) \
                    WITH ORDINALITY k(n, ord) JOIN pg_attribute a ON a.attrelid = ix.indrelid \
                    AND a.attnum = k.n ORDER BY k.ord), ','), \
                    coalesce(pg_get_expr(ix.indpred, ix.indrelid), ''), \
                    EXISTS (SELECT 1 FROM pg_constraint c WHERE c.conindid = ix.indexrelid \
                    AND c.contype IN ('p', 'u', 'x'))::int4 \
                    FROM pg_index ix JOIN pg_class i ON i.oid = ix.indexrelid \
                    JOIN pg_class t ON t.oid = ix.indrelid \
                    JOIN pg_namespace n ON n.oid = t.relnamespace \
                    WHERE n.nspname = 'public' AND NOT ix.indisprimary \
                    AND t.relname NOT LIKE '\\_sqlx%'";
                for r in sqlx::query(sql).fetch_all(&self.pool).await.unwrap() {
                    let backs_constraint = r.get::<i32, _>(5) == 1;
                    out.insert((
                        r.get::<String, _>(0),
                        if backs_constraint {
                            String::new()
                        } else {
                            r.get::<String, _>(1)
                        },
                        r.get::<i32, _>(2) == 1,
                        r.get::<String, _>(3),
                        norm(&r.get::<String, _>(4)),
                    ));
                }
            }
            Dialect::Sqlite => {
                let sql = "SELECT m.name, il.name, il.\"unique\", il.origin, \
                    coalesce((SELECT s.sql FROM sqlite_master s WHERE s.type = 'index' \
                    AND s.name = il.name), '') \
                    FROM sqlite_master m, pragma_index_list(m.name) il \
                    WHERE m.type = 'table' AND m.name NOT LIKE 'sqlite%' \
                    AND m.name NOT LIKE '\\_sqlx%' ESCAPE '\\' AND il.origin <> 'pk'";
                for r in sqlx::query(sql).fetch_all(&self.pool).await.unwrap() {
                    let name: String = r.get(1);
                    let cols: Vec<String> =
                        sqlx::query("SELECT name FROM pragma_index_info($1) ORDER BY seqno")
                            .bind(name.clone())
                            .fetch_all(&self.pool)
                            .await
                            .unwrap()
                            .into_iter()
                            .map(|c| c.get::<String, _>(0))
                            .collect();
                    let create: String = r.get(4);
                    let pred = create
                        .split_once(" WHERE ")
                        .map_or(String::new(), |(_, p)| norm(p));
                    let origin: String = r.get(3);
                    out.insert((
                        r.get::<String, _>(0),
                        if origin == "c" { name } else { String::new() },
                        r.get::<i64, _>(2) == 1,
                        cols.join(","),
                        pred,
                    ));
                }
            }
        }
        out
    }
}

/// Constraints with no case of their own: they exist as the target of a
/// composite foreign key and are implied by the primary key, so no row can
/// violate them alone. Removing one makes the migration fail to apply,
/// because the foreign key needs it.
const FK_TARGETS: [&str; 5] = [
    "packs_id_repo_unique",
    "packs_id_repo_level_unique",
    "ref_versions_id_repo_unique",
    "ref_versions_id_ref_seq_unique",
    "covers_id_span_unique",
];

/// Every constraint and unique index named in a migration file.
fn named_constraints(sql: &str) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    for marker in ["CONSTRAINT ", "CREATE UNIQUE INDEX "] {
        for (i, _) in sql.match_indices(marker) {
            let rest = &sql[i + marker.len()..];
            let name: String = rest
                .chars()
                .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
                .collect();
            names.insert(name);
        }
    }
    names
}

/// The fixture: rows the cases are changed from and point at.
struct Fx {
    repo: i64,
    other_repo: i64,
    /// Push pack, repo, 4 objects.
    pack: i64,
    /// Push pack in the other repository.
    other_pack: i64,
    /// Compaction pack at L2.
    l2_pack: i64,
    /// main v1 (pack), v2 (trunk dep v1).
    t1: i64,
    t2: i64,
    /// topic v1 (trunk dep t2, wider), v2 deleted.
    b1: i64,
    /// main v1 in the other repository.
    o1: i64,
    /// `refs/heads/late` v1 with the explicit ID 1000.
    late: i64,
    /// main L1 cover of seq 1..=2, with a chain (migration 0003).
    cover: i64,
    /// main L4 cover of seq 1..=2, without a chain.
    bare_cover: i64,
}

fn repo_row(name: &'static str) -> Row {
    Row::new(
        "repos",
        &[("name", T(name)), ("trunk_ref", T("refs/heads/main"))],
    )
}

fn pack_row(repo: i64, checksum: u8) -> Row {
    Row::new(
        "packs",
        &[
            ("repo_id", I(repo)),
            ("checksum", oid(checksum)),
            ("pack_key", T("k.pack")),
            ("size", I(100)),
            ("idx_key", T("k.idx")),
            ("idx_size", I(1184)),
            ("entries_key", T("k.entries")),
            ("entries_size", I(252)),
            ("object_count", I(4)),
            ("ref_delta_count", I(1)),
            ("level", I(0)),
            ("origin", T("push")),
            ("received_bytes", I(80)),
        ],
    )
}

/// A token row created at time 100; `id` and digest are `n` repeated.
fn token_row(n: u8) -> Row {
    Row::new(
        "tokens",
        &[
            ("id", B(vec![n; 16])),
            ("secret_sha256", B(vec![n; 32])),
            ("subject", T("agent:ci-7")),
            ("grants", T("write:acme/mono read:demo/*")),
            ("created_at_us", I(100)),
        ],
    )
}

/// An SSH key row registered at time 100; fingerprint and key are `n`
/// repeated.
fn ssh_key_row(n: u8) -> Row {
    Row::new(
        "ssh_keys",
        &[
            ("fingerprint", B(vec![n; 32])),
            ("public_key", B(vec![n; 51])),
            ("subject", T("user:alice")),
            ("grants", T("write:acme/mono")),
            ("created_at_us", I(100)),
        ],
    )
}

/// A mirror row for `repo` (migration 0003).
fn mirror_row(repo: i64) -> Row {
    Row::new(
        "mirrors",
        &[
            ("repo_id", I(repo)),
            ("upstream_url", T("https://github.com/acme/mono.git")),
            ("mode", T("proxy")),
            ("created_at_us", I(100)),
        ],
    )
}

/// A ledger row of `repo`'s mirror for `name`, agreed at `oid(30)`.
fn mirror_ref_row(repo: i64, name: &'static str) -> Row {
    Row::new(
        "mirror_refs",
        &[
            ("repo_id", I(repo)),
            ("ref_name", T(name)),
            ("synced_oid", oid(30)),
            ("updated_at_us", I(100)),
        ],
    )
}

fn compaction(row: &Row, level: i64) -> Row {
    row.with(&[
        ("level", I(level)),
        ("origin", T("compaction")),
        ("received_bytes", Null),
    ])
}

fn version_row(repo: i64, name: &'static str, seq: i64, parent: Option<i64>) -> Row {
    Row::new(
        "ref_versions",
        &[
            ("repo_id", I(repo)),
            ("ref_name", T(name)),
            ("seq", I(seq)),
            ("parent_version_id", parent.map_or(Null, I)),
            ("tip_oid", oid(20)),
            ("closure_exact", I(1)),
            ("pack_id", Null),
            ("trunk_dep_id", Null),
            ("scoped_dep_id", Null),
        ],
    )
}

async fn fixture(db: &Db) -> Fx {
    let repo = db.insert(&repo_row("acme/mono")).await;
    let other_repo = db.insert(&repo_row("acme/other")).await;
    let pack = db.insert(&pack_row(repo, 1)).await;
    let other_pack = db.insert(&pack_row(other_repo, 1)).await;
    let l2_pack = db.insert(&compaction(&pack_row(repo, 2), 2)).await;
    let main = "refs/heads/main";
    let t1 = db
        .insert(
            &version_row(repo, main, 1, None).with(&[("tip_oid", oid(10)), ("pack_id", I(pack))]),
        )
        .await;
    let t2 = db
        .insert(
            &version_row(repo, main, 2, Some(t1))
                .with(&[("tip_oid", oid(11)), ("trunk_dep_id", I(t1))]),
        )
        .await;
    let b1 = db
        .insert(&version_row(repo, "refs/heads/topic", 1, None).with(&[
            ("tip_oid", oid(12)),
            ("trunk_dep_id", I(t2)),
            ("closure_exact", I(0)),
        ]))
        .await;
    // A deletion has no tip, closure, pack or dependencies.
    db.insert(&version_row(repo, "refs/heads/topic", 2, Some(b1)).with(&[
        ("tip_oid", NullB),
        ("closure_exact", Null),
        ("trunk_dep_id", Null),
        ("scoped_dep_id", Null),
    ]))
    .await;
    let o1 = db
        .insert(
            &version_row(other_repo, main, 1, None)
                .with(&[("tip_oid", oid(10)), ("pack_id", I(other_pack))]),
        )
        .await;
    let late = db
        .insert(
            &version_row(repo, "refs/heads/late", 1, None)
                .with(&[("id", I(1000)), ("pack_id", I(pack))]),
        )
        .await;
    assert_eq!(late, 1000);
    db.insert_keyless(&Row::new(
        "refs",
        &[
            ("repo_id", I(repo)),
            ("ref_name", T(main)),
            ("version_id", I(t1)),
            ("seq", I(1)),
        ],
    ))
    .await;
    db.insert_keyless(&commit_row(repo, 10, t1)).await;
    let cover = db.insert(&cover_row(repo, 1, 1, 2, Null)).await;
    let bare_cover = db.insert(&cover_row(repo, 4, 1, 2, Null)).await;
    db.insert_keyless(&chain_row(repo, cover, main, 1, t1))
        .await;
    db.insert_keyless(&touch_row(pack, repo, 1)).await;
    db.insert_keyless(&tombstone_row(repo, 1, 0)).await;
    db.insert_keyless(&shallow_row(repo, t1, 1, pack)).await;
    db.insert(&job_row(repo).set("ref_version_id", I(t1))).await;
    db.insert(&job_row(repo).with(&[
        ("kind", T("rewrite_pack")),
        ("ref_version_id", Null),
        ("pack_id", I(pack)),
    ]))
    .await;
    db.insert_keyless(&token_row(1)).await;
    db.insert_keyless(&ssh_key_row(1)).await;
    db.insert_keyless(&mirror_row(repo)).await;
    db.insert_keyless(&mirror_ref_row(repo, main)).await;
    db.insert_keyless(&cursor_row(pack)).await;
    db.insert_keyless(&lease_row("gc")).await;
    Fx {
        repo,
        other_repo,
        pack,
        other_pack,
        l2_pack,
        t1,
        t2,
        b1,
        o1,
        late,
        cover,
        bare_cover,
    }
}

fn chain_row(repo: i64, cover: i64, name: &'static str, first_seq: i64, version: i64) -> Row {
    Row::new(
        "cover_chains",
        &[
            ("cover_id", I(cover)),
            ("repo_id", I(repo)),
            ("ref_name", T(name)),
            ("first_seq", I(first_seq)),
            ("first_version_id", I(version)),
        ],
    )
}

/// The sweep's cursor (migration 0009).
fn cursor_row(pack: i64) -> Row {
    Row::new(
        "gc_cursors",
        &[
            ("name", T("orphan_packs")),
            ("touched_at_us", I(5)),
            ("pack_id", I(pack)),
            ("audit_pack_id", I(pack)),
        ],
    )
}

/// A lease (migration 0009).
fn lease_row(name: &'static str) -> Row {
    Row::new(
        "leases",
        &[
            ("name", T(name)),
            ("owner", T("aspen-a")),
            ("epoch", I(1)),
            ("expires_at_us", I(5)),
        ],
    )
}

fn touch_row(pack: i64, repo: i64, at: i64) -> Row {
    Row::new(
        "pack_touches",
        &[
            ("pack_id", I(pack)),
            ("repo_id", I(repo)),
            ("touched_at_us", I(at)),
        ],
    )
}

/// `pack` recorded as `version`'s exact shallow pack at `depth`
/// (migration 0006), last used when it was recorded (migration 0007).
fn shallow_row(repo: i64, version: i64, depth: i64, pack: i64) -> Row {
    Row::new(
        "shallow_packs",
        &[
            ("repo_id", I(repo)),
            ("version_id", I(version)),
            ("depth", I(depth)),
            ("pack_id", I(pack)),
            ("recorded_at_us", I(10)),
            ("used_at_us", I(10)),
        ],
    )
}

fn tombstone_row(repo: i64, checksum: u8, generation: i64) -> Row {
    Row::new(
        "pack_tombstones",
        &[
            ("repo_id", I(repo)),
            ("checksum", oid(checksum)),
            ("generation", I(generation)),
            ("swept_at_us", I(10)),
            ("deleted_at_us", Null),
        ],
    )
}

fn commit_row(repo: i64, commit: u8, version: i64) -> Row {
    Row::new(
        "commit_index",
        &[
            ("repo_id", I(repo)),
            ("commit_oid", oid(commit)),
            ("ref_version_id", I(version)),
            ("parents", B(vec![])),
            ("committer_time", I(0)),
        ],
    )
}

fn cover_row(repo: i64, level: i64, first: i64, last: i64, pack: V) -> Row {
    Row::new(
        "covers",
        &[
            ("repo_id", I(repo)),
            ("ref_name", T("refs/heads/main")),
            ("level", I(level)),
            ("first_seq", I(first)),
            ("last_seq", I(last)),
            ("pack_id", pack),
        ],
    )
}

fn job_row(repo: i64) -> Row {
    Row::new(
        "jobs",
        &[
            ("repo_id", I(repo)),
            ("kind", T("compact")),
            ("ref_version_id", Null),
            ("pack_id", Null),
            ("state", T("queued")),
            ("attempts", I(0)),
            ("run_after_us", I(0)),
            ("lease_owner", NullT),
            ("lease_expires_us", Null),
        ],
    )
}

/// `(constraint, row that violates only it)`, plus the valid rows. Some
/// rows cannot avoid a second violation that the database checks later (a
/// `CHECK` fails before a foreign key is looked at); the name assertion
/// still catches the constraint's removal, because the error then names the
/// other one.
/// Rows labelled with a constraint name or a description.
type Labelled = Vec<(&'static str, Row)>;

fn cases(f: &Fx) -> (Labelled, Labelled) {
    let mut bad: Labelled = Vec::new();
    let mut good: Labelled = Vec::new();

    // repos
    let repo = repo_row("acme/new");
    for p in ForcePolicy::ALL {
        good.push(("allow_force value", repo.set("allow_force", T(p.as_str()))));
    }
    for p in ShallowPolicy::ALL {
        good.push(("shallow value", repo.set("shallow", T(p.as_str()))));
    }
    for (what, v) in [("no_thin 0", 0), ("no_thin 1", 1)] {
        good.push((what, repo.set("no_thin", I(v))));
    }
    for (what, v) in [("have_tips 0", 0), ("have_tips 32", 32)] {
        good.push((what, repo.set("have_tips", I(v))));
    }
    bad.extend([
        ("repos_pkey", repo.set("id", I(f.repo))),
        ("repos_name_unique", repo.set("name", T("acme/mono"))),
        (
            "repos_trunk_is_branch",
            repo.set("trunk_ref", T("refs/tags/main")),
        ),
        (
            "repos_trunk_is_branch",
            repo.set("trunk_ref", T("REFS/HEADS/main")),
        ),
        (
            "repos_allow_force_values",
            repo.set("allow_force", T("sometimes")),
        ),
        ("repos_no_thin_bool", repo.set("no_thin", I(2))),
        ("repos_have_tips_range", repo.set("have_tips", I(33))),
        ("repos_have_tips_range", repo.set("have_tips", I(-1))),
        ("repos_shallow_values", repo.set("shallow", T("maybe"))),
    ]);

    // packs
    let pack = pack_row(f.repo, 9);
    good.push(("push pack", pack.clone()));
    for level in 1..=4 {
        good.push(("compaction pack", compaction(&pack, level)));
    }
    good.push(("no REF_DELTA", pack.set("ref_delta_count", I(0))));
    good.push(("all REF_DELTA", pack.set("ref_delta_count", I(4))));
    good.push(("large offsets", pack.set("idx_size", I(1184 + 4 * 8))));
    good.push((
        "a later generation",
        pack.set("generation", I(4_294_967_295)),
    ));
    good.push((
        "same checksum, other repo",
        pack.set("repo_id", I(f.other_repo)),
    ));
    let huge = 4_294_967_296_i64;
    bad.extend([
        ("packs_pkey", pack.set("id", I(f.pack))),
        ("packs_repo_fk", pack.set("repo_id", I(9999))),
        ("packs_checksum_len", pack.set("checksum", B(vec![9; 19]))),
        ("packs_checksum_unique", pack.set("checksum", oid(1))),
        ("packs_generation_range", pack.set("generation", I(-1))),
        ("packs_generation_range", pack.set("generation", I(huge))),
        ("packs_size_min", pack.set("size", I(31))),
        (
            "packs_object_count_range",
            pack.with(&[
                ("object_count", I(0)),
                ("ref_delta_count", I(0)),
                ("idx_size", I(1072)),
                ("entries_size", I(44)),
            ]),
        ),
        (
            "packs_object_count_range",
            pack.with(&[
                ("object_count", I(huge)),
                ("idx_size", I(1072 + 28 * huge)),
                ("entries_size", I(44 + 52 * huge)),
            ]),
        ),
        ("packs_idx_size_v2", pack.set("idx_size", I(1100))),
        ("packs_idx_size_v2", pack.set("idx_size", I(1185))),
        ("packs_idx_size_v2", pack.set("idx_size", I(1184 + 5 * 8))),
        ("packs_entries_size_exact", pack.set("entries_size", I(253))),
        (
            "packs_ref_delta_count_range",
            pack.set("ref_delta_count", I(-1)),
        ),
        (
            "packs_ref_delta_count_range",
            pack.set("ref_delta_count", I(5)),
        ),
        ("packs_level_range", compaction(&pack, 5)),
        ("packs_level_range", compaction(&pack, -1)),
        (
            "packs_origin_values",
            pack.with(&[
                ("origin", T("other")),
                ("level", I(1)),
                ("received_bytes", Null),
            ]),
        ),
        ("packs_push_iff_l0", pack.set("level", I(1))),
        ("packs_push_iff_l0", compaction(&pack, 0)),
        (
            "packs_received_bytes_min",
            pack.set("received_bytes", I(-1)),
        ),
        (
            "packs_received_bytes_iff_push",
            pack.set("received_bytes", Null),
        ),
        (
            "packs_received_bytes_iff_push",
            compaction(&pack, 2).set("received_bytes", I(5)),
        ),
    ]);

    // ref_versions
    let main = "refs/heads/main";
    let v = version_row(f.repo, "refs/heads/new", 1, None).set("trunk_dep_id", I(f.t2));
    good.push(("live, trunk dependency", v.clone()));
    good.push(("live, wider closure", v.set("closure_exact", I(0))));
    good.push((
        "live, pack only",
        v.with(&[("trunk_dep_id", Null), ("pack_id", I(f.pack))]),
    ));
    good.push((
        "live, scoped dependency only",
        v.with(&[("trunk_dep_id", Null), ("scoped_dep_id", I(f.b1))]),
    ));
    good.push((
        "deletion",
        v.with(&[
            ("tip_oid", NullB),
            ("closure_exact", Null),
            ("trunk_dep_id", Null),
        ]),
    ));
    good.push((
        "next version of main",
        version_row(f.repo, main, 3, Some(f.t2)).set("trunk_dep_id", I(f.t2)),
    ));
    let explicit = v.with(&[("id", I(999)), ("ref_name", T("refs/heads/early"))]);
    bad.extend([
        (
            "ref_versions_pkey",
            v.with(&[
                ("id", I(f.t1)),
                ("trunk_dep_id", Null),
                ("pack_id", I(f.pack)),
            ]),
        ),
        (
            "ref_versions_repo_fk",
            v.with(&[
                ("repo_id", I(9999)),
                ("tip_oid", NullB),
                ("closure_exact", Null),
                ("trunk_dep_id", Null),
            ]),
        ),
        (
            "ref_versions_seq_min",
            v.with(&[("seq", I(0)), ("parent_version_id", I(f.t1))]),
        ),
        (
            "ref_versions_seq_unique",
            version_row(f.repo, main, 2, Some(f.t1)).set("trunk_dep_id", I(f.t1)),
        ),
        (
            "ref_versions_parent_iff_not_first",
            version_row(f.repo, main, 3, None).set("trunk_dep_id", I(f.t2)),
        ),
        (
            "ref_versions_parent_fk",
            version_row(f.repo, main, 3, Some(f.t1)).set("trunk_dep_id", I(f.t2)),
        ),
        (
            "ref_versions_parent_fk",
            version_row(f.repo, "refs/heads/topic", 3, Some(f.t2)).set("trunk_dep_id", I(f.t2)),
        ),
        (
            "ref_versions_parent_older",
            version_row(f.repo, "refs/heads/late", 2, Some(f.late))
                .with(&[("id", I(999)), ("pack_id", I(f.pack))]),
        ),
        ("ref_versions_tip_len", v.set("tip_oid", B(vec![1; 21]))),
        ("ref_versions_tip_not_zero", v.set("tip_oid", oid(0))),
        (
            "ref_versions_closure_iff_live",
            v.set("closure_exact", Null),
        ),
        (
            "ref_versions_closure_iff_live",
            v.with(&[("tip_oid", NullB), ("trunk_dep_id", Null)]),
        ),
        (
            "ref_versions_closure_exact_bool",
            v.set("closure_exact", I(2)),
        ),
        (
            "ref_versions_deleted_no_pack",
            v.with(&[
                ("tip_oid", NullB),
                ("closure_exact", Null),
                ("trunk_dep_id", Null),
                ("pack_id", I(f.pack)),
            ]),
        ),
        (
            "ref_versions_deletion_no_deps",
            v.with(&[("tip_oid", NullB), ("closure_exact", Null)]),
        ),
        (
            "ref_versions_deletion_no_deps",
            v.with(&[
                ("tip_oid", NullB),
                ("closure_exact", Null),
                ("trunk_dep_id", Null),
                ("scoped_dep_id", I(f.b1)),
            ]),
        ),
        ("ref_versions_connected", v.set("trunk_dep_id", Null)),
        ("ref_versions_pack_fk", v.set("pack_id", I(f.other_pack))),
        ("ref_versions_trunk_dep_fk", v.set("trunk_dep_id", I(f.o1))),
        (
            "ref_versions_scoped_dep_fk",
            v.set("scoped_dep_id", I(f.o1)),
        ),
        (
            "ref_versions_trunk_dep_older",
            explicit.set("trunk_dep_id", I(f.late)),
        ),
        (
            "ref_versions_scoped_dep_older",
            explicit.set("scoped_dep_id", I(f.late)),
        ),
    ]);

    // refs
    let r = Row::new(
        "refs",
        &[
            ("repo_id", I(f.repo)),
            ("ref_name", T("refs/heads/topic")),
            ("version_id", I(f.b1)),
            ("seq", I(1)),
        ],
    );
    good.push(("ref row", r.clone()));
    bad.extend([
        (
            "refs_pkey",
            r.with(&[
                ("ref_name", T(main)),
                ("version_id", I(f.t2)),
                ("seq", I(2)),
            ]),
        ),
        (
            "refs_version_fk",
            r.with(&[("version_id", I(f.t2)), ("seq", I(2))]),
        ),
        ("refs_version_fk", r.set("seq", I(2))),
        ("refs_version_fk", r.set("repo_id", I(f.other_repo))),
    ]);

    // commit_index
    let c = commit_row(f.repo, 30, f.t1);
    good.push(("root commit", c.clone()));
    good.push((
        "merge commit",
        c.set("parents", B([vec![1; 20], vec![2; 20]].concat())),
    ));
    bad.extend([
        ("commit_index_pkey", c.set("commit_oid", oid(10))),
        (
            "commit_index_commit_len",
            c.set("commit_oid", B(vec![1; 19])),
        ),
        ("commit_index_parents_len", c.set("parents", B(vec![1; 30]))),
        ("commit_index_version_fk", c.set("ref_version_id", I(f.o1))),
    ]);

    // covers
    let cov = cover_row(f.repo, 2, 1, 2, I(f.l2_pack));
    good.push(("compacted cover", cov.clone()));
    for level in 1..=4 {
        good.push(("pending cover", cover_row(f.repo, level, 3, 4, Null)));
    }
    bad.extend([
        (
            "covers_pkey",
            cover_row(f.repo, 3, 1, 2, Null).set("id", I(1)),
        ),
        ("covers_repo_fk", cover_row(9999, 2, 1, 2, Null)),
        ("covers_level_range", cover_row(f.repo, 0, 1, 2, Null)),
        ("covers_level_range", cover_row(f.repo, 5, 1, 2, Null)),
        ("covers_first_seq_min", cover_row(f.repo, 2, 0, 2, Null)),
        ("covers_seq_order", cover_row(f.repo, 2, 3, 2, Null)),
        ("covers_span_unique", cover_row(f.repo, 1, 1, 5, Null)),
        ("covers_pack_fk", cover_row(f.repo, 3, 1, 2, I(f.l2_pack))),
        ("covers_pack_fk", cover_row(f.repo, 1, 3, 4, I(f.pack))),
        (
            "covers_pack_fk",
            cover_row(f.repo, 2, 3, 4, I(f.other_pack)),
        ),
    ]);

    // jobs
    let j = job_row(f.repo).set("ref_version_id", I(f.t2));
    for tag in JobTag::ALL {
        let row = if tag == JobTag::RewritePack {
            j.with(&[("ref_version_id", Null), ("pack_id", I(f.l2_pack))])
        } else {
            j.clone()
        };
        good.push(("job kind", row.set("kind", T(tag.as_str()))));
    }
    for state in JobState::STORED {
        let row = if state == "running" {
            j.with(&[
                ("lease_owner", T("worker-1")),
                ("lease_expires_us", I(5)),
                ("attempts", I(1)),
            ])
        } else {
            j.clone()
        };
        good.push(("job state", row.set("state", T(state))));
    }
    good.push((
        "a done job leaves the slot free",
        j.with(&[("ref_version_id", I(f.t1)), ("state", T("done"))]),
    ));
    good.push((
        "another kind for the same version",
        j.with(&[("ref_version_id", I(f.t1)), ("kind", T("index_commits"))]),
    ));
    let lease = |row: &Row, state| {
        row.with(&[
            ("state", T(state)),
            ("lease_owner", T("w")),
            ("lease_expires_us", I(5)),
        ])
    };
    bad.extend([
        ("jobs_pkey", j.set("id", I(1))),
        ("jobs_kind_values", j.set("kind", T("gc"))),
        ("jobs_state_values", j.set("state", T("paused"))),
        ("jobs_attempts_min", j.set("attempts", I(-1))),
        ("jobs_pack_iff_rewrite", j.set("pack_id", I(f.l2_pack))),
        (
            "jobs_pack_iff_rewrite",
            j.with(&[("kind", T("rewrite_pack")), ("ref_version_id", Null)]),
        ),
        (
            "jobs_version_iff_not_rewrite",
            j.with(&[("kind", T("rewrite_pack")), ("pack_id", I(f.l2_pack))]),
        ),
        ("jobs_lease_iff_running", j.set("state", T("running"))),
        ("jobs_lease_iff_running", lease(&j, "queued")),
        (
            "jobs_lease_fields_together",
            j.with(&[("state", T("running")), ("lease_owner", T("w"))]),
        ),
        ("jobs_version_fk", j.set("ref_version_id", I(f.o1))),
        (
            "jobs_pack_fk",
            j.with(&[
                ("kind", T("rewrite_pack")),
                ("ref_version_id", Null),
                ("pack_id", I(f.other_pack)),
            ]),
        ),
        ("jobs_open_version", j.set("ref_version_id", I(f.t1))),
        (
            "jobs_open_version",
            lease(&j.set("ref_version_id", I(f.t1)), "running"),
        ),
        (
            "jobs_open_version",
            j.with(&[("ref_version_id", I(f.t1)), ("state", T("dead"))]),
        ),
        (
            "jobs_open_pack",
            j.with(&[
                ("kind", T("rewrite_pack")),
                ("ref_version_id", Null),
                ("pack_id", I(f.pack)),
            ]),
        ),
    ]);

    // tokens (migration 0002)
    let t = token_row(2);
    good.extend([
        ("token without expiry or revocation", t.clone()),
        ("token with no grants", t.set("grants", T(""))),
        ("token with a description", t.set("description", T("ci"))),
        ("token expiring later", t.set("expires_at_us", I(101))),
        ("token revoked at creation", t.set("revoked_at_us", I(100))),
        (
            "token subject of 255 characters",
            t.set("subject", T(&LONG_SUBJECT[..255])),
        ),
    ]);
    bad.extend([
        ("tokens_pkey", t.set("id", B(vec![1; 16]))),
        ("tokens_id_length", t.set("id", B(vec![2; 15]))),
        ("tokens_id_length", t.set("id", B(vec![2; 17]))),
        (
            "tokens_digest_length",
            t.set("secret_sha256", B(vec![2; 31])),
        ),
        (
            "tokens_digest_length",
            t.set("secret_sha256", B(vec![2; 20])),
        ),
        ("tokens_subject_length", t.set("subject", T(""))),
        ("tokens_subject_length", t.set("subject", T(LONG_SUBJECT))),
        (
            "tokens_expiry_after_creation",
            t.set("expires_at_us", I(100)),
        ),
        (
            "tokens_revoked_after_creation",
            t.set("revoked_at_us", I(99)),
        ),
    ]);

    // ssh_keys (migration 0004)
    let k = ssh_key_row(2);
    good.extend([
        ("key without expiry or revocation", k.clone()),
        ("key with no grants", k.set("grants", T(""))),
        ("key with a description", k.set("description", T("laptop"))),
        ("key expiring later", k.set("expires_at_us", I(101))),
        (
            "key revoked at registration",
            k.set("revoked_at_us", I(100)),
        ),
        ("key of 1,024 bytes", k.set("public_key", B(vec![2; 1024]))),
        (
            "key subject of 255 characters",
            k.set("subject", T(&LONG_SUBJECT[..255])),
        ),
    ]);
    bad.extend([
        ("ssh_keys_pkey", k.set("fingerprint", B(vec![1; 32]))),
        (
            "ssh_keys_fingerprint_length",
            k.set("fingerprint", B(vec![2; 31])),
        ),
        (
            "ssh_keys_fingerprint_length",
            k.set("fingerprint", B(vec![2; 33])),
        ),
        ("ssh_keys_key_length", k.set("public_key", B(Vec::new()))),
        ("ssh_keys_key_length", k.set("public_key", B(vec![2; 1025]))),
        ("ssh_keys_subject_length", k.set("subject", T(""))),
        ("ssh_keys_subject_length", k.set("subject", T(LONG_SUBJECT))),
        (
            "ssh_keys_expiry_after_creation",
            k.set("expires_at_us", I(100)),
        ),
        (
            "ssh_keys_revoked_after_creation",
            k.set("revoked_at_us", I(99)),
        ),
    ]);
    // mirrors and mirror_refs (migration 0003)
    let m = mirror_row(f.other_repo);
    good.extend([
        ("proxy mirror", m.clone()),
        ("async mirror", m.set("mode", T("async"))),
        (
            "mirror with a credential",
            m.set("credential", T(&LONG_SUBJECT[..64])),
        ),
        (
            "mirror after a failed sync",
            m.with(&[
                ("last_sync_at_us", I(200)),
                ("last_error", T("upstream: 503")),
            ]),
        ),
    ]);
    bad.extend([
        ("mirrors_pkey", mirror_row(f.repo)),
        ("mirrors_repo_fk", mirror_row(f.other_repo + 1000)),
        ("mirrors_mode_values", m.set("mode", T("mirror"))),
        ("mirrors_url_length", m.set("upstream_url", T(""))),
        ("mirrors_url_length", m.set("upstream_url", T(LONG_URL))),
        (
            "mirrors_credential_length",
            m.set("credential", T(&LONG_SUBJECT[..65])),
        ),
    ]);
    let r = mirror_ref_row(f.repo, "refs/heads/topic");
    good.extend([
        ("ledger row", r.clone()),
        ("ledger row agreed absent", r.set("synced_oid", NullB)),
        (
            "conflict between two values",
            r.with(&[
                ("conflict_at_us", I(150)),
                ("conflict_aspen_oid", oid(31)),
                ("conflict_upstream_oid", oid(32)),
            ]),
        ),
        (
            "conflict with a deletion on each side",
            r.with(&[("synced_oid", NullB), ("conflict_at_us", I(150))]),
        ),
    ]);
    bad.extend([
        (
            "mirror_refs_pkey",
            mirror_ref_row(f.repo, "refs/heads/main"),
        ),
        (
            "mirror_refs_mirror_fk",
            mirror_ref_row(f.other_repo + 1000, "refs/heads/x"),
        ),
        (
            "mirror_refs_synced_len",
            r.set("synced_oid", B(vec![1; 19])),
        ),
        (
            "mirror_refs_conflict_aspen_len",
            r.with(&[
                ("conflict_at_us", I(150)),
                ("conflict_aspen_oid", B(vec![1; 21])),
            ]),
        ),
        (
            "mirror_refs_conflict_upstream_len",
            r.with(&[
                ("conflict_at_us", I(150)),
                ("conflict_upstream_oid", B(vec![1; 21])),
            ]),
        ),
        (
            "mirror_refs_conflict_fields",
            r.set("conflict_aspen_oid", oid(31)),
        ),
        (
            "mirror_refs_conflict_fields",
            r.set("conflict_upstream_oid", oid(32)),
        ),
    ]);
    // cover_chains, pack_touches, pack_tombstones (migration 0005)
    let ch = chain_row(f.repo, f.bare_cover, main, 1, f.t1);
    good.push(("chain of a cover from its first version", ch.clone()));
    bad.extend([
        ("cover_chains_pkey", ch.set("cover_id", I(f.cover))),
        // The version is main's at seq 2, but the cover starts at seq 1.
        (
            "cover_chains_cover_fk",
            ch.with(&[("first_seq", I(2)), ("first_version_id", I(f.t2))]),
        ),
        // The version is topic's at seq 1, but the cover is main's.
        (
            "cover_chains_cover_fk",
            ch.with(&[
                ("ref_name", T("refs/heads/topic")),
                ("first_version_id", I(f.b1)),
            ]),
        ),
        // The cover starts at seq 1, but the version is at seq 2.
        (
            "cover_chains_version_fk",
            ch.set("first_version_id", I(f.t2)),
        ),
        // Another ref's version at the same seq.
        (
            "cover_chains_version_fk",
            ch.set("first_version_id", I(f.b1)),
        ),
    ]);
    let touch = touch_row(f.l2_pack, f.repo, 5);
    good.push(("touch of a pack", touch.clone()));
    bad.extend([
        ("pack_touches_pkey", touch.set("pack_id", I(f.pack))),
        (
            "pack_touches_pack_fk",
            touch.set("repo_id", I(f.other_repo)),
        ),
        ("pack_touches_pack_fk", touch.set("pack_id", I(9999))),
    ]);
    // gc_cursors, leases (migration 0009)
    let cursor = cursor_row(f.pack);
    good.push(("another cursor", cursor.set("name", T("other"))));
    bad.push(("gc_cursors_pkey", cursor.set("audit_pack_id", I(0))));
    let lease = lease_row("gc");
    good.push(("another lease", lease_row("other")));
    bad.push(("leases_pkey", lease.set("owner", T("aspen-b"))));
    let stone = tombstone_row(f.repo, 50, 0);
    good.push(("tombstone", stone.clone()));
    good.push((
        "same checksum, other repo",
        tombstone_row(f.other_repo, 1, 0),
    ));
    good.push((
        "same checksum, next generation",
        tombstone_row(f.repo, 1, 1),
    ));
    good.push((
        "finished deletion",
        stone.with(&[("generation", I(4_294_967_295)), ("deleted_at_us", I(11))]),
    ));
    bad.extend([
        ("pack_tombstones_pkey", stone.set("checksum", oid(1))),
        (
            "pack_tombstones_generation_range",
            stone.set("generation", I(-1)),
        ),
        (
            "pack_tombstones_generation_range",
            stone.set("generation", I(4_294_967_296)),
        ),
        (
            "pack_tombstones_checksum_len",
            stone.set("checksum", B(vec![1; 19])),
        ),
        ("pack_tombstones_repo_fk", stone.set("repo_id", I(9999))),
    ]);
    // shallow_packs (migration 0006). The fixture records main v1 at
    // depth 1.
    let sh = shallow_row(f.repo, f.t2, 1, f.l2_pack);
    good.push(("shallow pack", sh.clone()));
    good.push((
        "same version, another depth",
        sh.with(&[("version_id", I(f.t1)), ("depth", I(2))]),
    ));
    good.push(("deepest depth", sh.set("depth", I(2_147_483_646))));
    good.push((
        "a pack recorded for two versions",
        shallow_row(f.repo, f.b1, 1, f.pack),
    ));
    bad.extend([
        ("shallow_packs_pkey", sh.set("version_id", I(f.t1))),
        ("shallow_packs_depth_range", sh.set("depth", I(0))),
        // Git's INFINITE_DEPTH is not a depth.
        (
            "shallow_packs_depth_range",
            sh.set("depth", I(2_147_483_647)),
        ),
        // A version of another repository.
        ("shallow_packs_version_fk", sh.set("version_id", I(f.o1))),
        ("shallow_packs_version_fk", sh.set("version_id", I(9999))),
        // A pack of another repository.
        ("shallow_packs_pack_fk", sh.set("pack_id", I(f.other_pack))),
        ("shallow_packs_pack_fk", sh.set("pack_id", I(9999))),
    ]);
    (bad, good)
}

/// 2,066 characters: longer than a mirror's upstream URL may be (2,048).
const LONG_URL: &str = concat!(
    "https://example.com/",
    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcd",
    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
);

/// 256 characters: one more than a token subject may have.
const LONG_SUBJECT: &str = concat!(
    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
);

/// The non-constraint indexes the migrations must create.
fn expected_indexes() -> BTreeSet<(String, String, bool, String, String)> {
    [
        (
            "jobs",
            "jobs_queued",
            false,
            "run_after_us,id",
            "state='queued'",
        ),
        (
            "jobs",
            "jobs_running",
            false,
            "lease_expires_us",
            "state='running'",
        ),
        (
            "jobs",
            "jobs_open_version",
            true,
            "kind,ref_version_id",
            "state<>'done'",
        ),
        (
            "jobs",
            "jobs_open_pack",
            true,
            "kind,pack_id",
            "state<>'done'",
        ),
        (
            "commit_index",
            "commit_index_version",
            false,
            "ref_version_id",
            "",
        ),
        // Migration 0005.
        (
            "covers",
            "covers_id_span_unique",
            true,
            "id,repo_id,ref_name,first_seq",
            "",
        ),
        ("ref_versions", "ref_versions_pack", false, "pack_id", ""),
        ("covers", "covers_pack", false, "pack_id", ""),
        ("jobs", "jobs_pack", false, "pack_id", ""),
        (
            "pack_tombstones",
            "pack_tombstones_pending",
            false,
            "swept_at_us",
            "deleted_at_usisnull",
        ),
        // Migration 0006.
        ("shallow_packs", "shallow_packs_pack", false, "pack_id", ""),
        (
            "shallow_packs",
            "shallow_packs_recorded",
            false,
            "recorded_at_us",
            "",
        ),
        ("refs", "refs_version", false, "version_id", ""),
        // Migration 0007.
        (
            "shallow_packs",
            "shallow_packs_used",
            false,
            "used_at_us",
            "",
        ),
        (
            "shallow_packs",
            "shallow_packs_repo",
            false,
            "repo_id,used_at_us",
            "",
        ),
        // Migration 0008.
        (
            "jobs",
            "jobs_queued_priority",
            false,
            "priority,run_after_us,id",
            "state='queued'",
        ),
        // Migration 0009.
        (
            "pack_touches",
            "pack_touches_due",
            false,
            "touched_at_us,pack_id",
            "",
        ),
        // Migration 0010.
        (
            "ref_versions",
            "ref_versions_repo_txid",
            false,
            "repo_id,txid",
            "",
        ),
    ]
    .into_iter()
    .map(|(t, n, u, c, p)| (t.into(), n.into(), u, c.into(), p.into()))
    .collect()
}

/// Every invariant schema v1 is responsible for, checked on one database.
async fn exercise(db: &Db) {
    db.migrate().await;
    // Applying again is a no-op.
    db.migrate().await;

    let f = fixture(db).await;
    let (bad, good) = cases(&f);
    for (what, row) in &good {
        db.accepts(what, row).await;
    }
    for (constraint, row) in &bad {
        db.rejects(constraint, row).await;
    }
    let tested: BTreeSet<String> = bad.iter().map(|(c, _)| (*c).to_owned()).collect();
    let required: BTreeSet<String> = named_constraints(PG_SQL)
        .into_iter()
        .filter(|n| !FK_TARGETS.contains(&n.as_str()))
        .collect();
    assert_eq!(
        tested, required,
        "every named constraint (except FK targets) has a case, and every case names a real constraint"
    );

    compare_and_swap(db, &f).await;

    // Defaults: timestamps in microseconds since the epoch, on every table
    // that has one, and the job queue's initial state.
    for table in ["repos", "packs", "ref_versions", "jobs"] {
        let sql = format!("SELECT min(created_at_us), max(created_at_us) FROM {table}");
        let row = sqlx::query(AssertSqlSafe(sql))
            .fetch_one(&db.pool)
            .await
            .unwrap();
        for i in 0..2 {
            let t: i64 = row.get(i);
            assert!(
                t > 1_700_000_000_000_000 && t < 10_000_000_000_000_000,
                "{:?}: {table}.created_at_us = {t}",
                db.dialect
            );
        }
    }
    let defaults = sqlx::query(
        "INSERT INTO jobs (repo_id, kind, ref_version_id, run_after_us) \
         VALUES ($1, 'mirror_push', $2, 0) RETURNING state, attempts",
    )
    .bind(f.repo)
    .bind(f.t2)
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!(defaults.get::<String, _>(0), "queued");
    let attempts: i64 = match db.dialect {
        Dialect::Postgres => i64::from(defaults.get::<i32, _>(1)),
        Dialect::Sqlite => defaults.get(1),
    };
    assert_eq!(attempts, 0);

    // Every version names the transaction that wrote it (migration 0010):
    // on Postgres its 64-bit transaction ID, on SQLite, where writes run
    // one at a time, its own ID.
    let txids = sqlx::query("SELECT id, txid FROM ref_versions")
        .fetch_all(&db.pool)
        .await
        .unwrap();
    assert!(!txids.is_empty());
    for row in txids {
        let (id, txid): (i64, i64) = (row.get(0), row.get(1));
        match db.dialect {
            Dialect::Postgres => assert!(txid > 0, "version {id}: txid {txid}"),
            Dialect::Sqlite => assert_eq!(txid, id, "version {id}"),
        }
    }

    let indexes = db.indexes().await;
    let named: BTreeSet<_> = indexes
        .iter()
        .filter(|i| !i.1.is_empty())
        .cloned()
        .collect();
    assert_eq!(named, expected_indexes(), "{:?}: indexes", db.dialect);
}

/// The compare-and-swap on `refs` can only land on the expected version's
/// direct child.
async fn compare_and_swap(db: &Db, f: &Fx) {
    let main = T("refs/heads/main");
    let cas = "UPDATE refs SET version_id = $1, seq = seq + 1 \
               WHERE repo_id = $2 AND ref_name = $3 AND version_id = $4";
    let t3 = db
        .insert(&version_row(f.repo, "refs/heads/main", 3, Some(f.t2)).set("trunk_dep_id", I(f.t2)))
        .await;
    let t4 = db
        .insert(&version_row(f.repo, "refs/heads/main", 4, Some(t3)).set("trunk_dep_id", I(t3)))
        .await;
    let fk = |r: Result<u64, sqlx::Error>, what: &str| match r {
        Err(sqlx::Error::Database(e)) => assert_eq!(
            e.kind(),
            ErrorKind::ForeignKeyViolation,
            "{:?}: {what}: {}",
            db.dialect,
            e.message()
        ),
        other => panic!(
            "{:?}: {what}: expected a foreign key error, got {other:?}",
            db.dialect
        ),
    };
    // main is at t1 (seq 1); t1 -> t2 is the child.
    assert_eq!(
        db.exec(cas, &[I(f.t2), I(f.repo), main.clone(), I(f.t1)])
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        db.exec(cas, &[I(f.t2), I(f.repo), main.clone(), I(f.t1)])
            .await
            .unwrap(),
        0,
        "a stale expectation loses"
    );
    fk(
        db.exec(cas, &[I(f.t1), I(f.repo), main.clone(), I(f.t2)])
            .await,
        "back to an older version",
    );
    fk(
        db.exec(cas, &[I(t4), I(f.repo), main.clone(), I(f.t2)])
            .await,
        "skipping a version",
    );
    fk(
        db.exec(cas, &[I(f.b1), I(f.repo), main.clone(), I(f.t2)])
            .await,
        "onto another ref's version",
    );
    assert_eq!(
        db.exec(cas, &[I(t3), I(f.repo), main.clone(), I(f.t2)])
            .await
            .unwrap(),
        1
    );
    let now: i64 = sqlx::query("SELECT seq FROM refs WHERE repo_id = $1 AND ref_name = $2")
        .bind(f.repo)
        .bind("refs/heads/main")
        .fetch_one(&db.pool)
        .await
        .unwrap()
        .get(0);
    assert_eq!(now, 3);
}

#[test]
fn both_dialects_name_the_same_constraints_and_indexes() {
    assert_eq!(named_constraints(PG_SQL), named_constraints(SQLITE_SQL));
    let index_names = |sql: &str| -> Vec<String> {
        sql.lines()
            .filter_map(|l| l.strip_prefix("CREATE "))
            .filter(|l| l.contains("INDEX"))
            .map(str::to_owned)
            .collect()
    };
    assert_eq!(index_names(PG_SQL), index_names(SQLITE_SQL));
}

/// The versions of a dialect's migrations (`<version>_<name>.sql`, as sqlx
/// reads them), with their file names, in file-name order.
fn migration_versions(dir: &std::path::Path) -> Result<Vec<(i64, String)>, String> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(dir).map_err(|e| format!("{}: {e}", dir.display()))? {
        let name = entry
            .map_err(|e| e.to_string())?
            .file_name()
            .to_string_lossy()
            .into_owned();
        if !name.ends_with(".sql") {
            continue;
        }
        let version = name
            .split_once('_')
            .and_then(|(v, _)| v.parse::<i64>().ok())
            .ok_or_else(|| format!("{name}: no <version>_ prefix"))?;
        out.push((version, name));
    }
    out.sort_by(|a, b| a.1.cmp(&b.1));
    Ok(out)
}

/// Every dialect's migration versions are unique, and the dialects have
/// the same versions, so two nodes that each add migration N cannot both
/// merge unnoticed.
fn check_migration_versions(root: &std::path::Path) -> Result<(), String> {
    let mut seen: Vec<(String, Vec<i64>)> = Vec::new();
    for dialect in ["postgres", "sqlite"] {
        let files = migration_versions(&root.join(dialect))?;
        let mut by_version: std::collections::BTreeMap<i64, Vec<&str>> = Default::default();
        for (v, name) in &files {
            by_version.entry(*v).or_default().push(name);
        }
        for (v, names) in &by_version {
            if names.len() > 1 {
                return Err(format!("{dialect}: version {v} is used by {names:?}"));
            }
        }
        seen.push((dialect.to_owned(), by_version.into_keys().collect()));
    }
    if seen[0].1 != seen[1].1 {
        return Err(format!("the dialects' versions differ: {seen:?}"));
    }
    Ok(())
}

#[test]
fn migration_versions_are_unique_and_the_same_in_both_dialects() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations");
    check_migration_versions(&root).unwrap();
    // Every migration is also in the SQL these tests apply.
    for (dialect, all) in [("postgres", PG_SQL), ("sqlite", SQLITE_SQL)] {
        for (_, name) in migration_versions(&root.join(dialect)).unwrap() {
            let sql = std::fs::read_to_string(root.join(dialect).join(&name)).unwrap();
            assert!(
                all.contains(&sql),
                "{dialect}/{name} is not in this test's SQL"
            );
        }
    }
}

#[test]
fn two_migrations_with_one_version_are_refused() {
    let dir = tempfile::tempdir().unwrap();
    for d in ["postgres", "sqlite"] {
        std::fs::create_dir_all(dir.path().join(d)).unwrap();
        for f in ["0001_a.sql", "0002_b.sql"] {
            std::fs::write(dir.path().join(d).join(f), "").unwrap();
        }
    }
    check_migration_versions(dir.path()).unwrap();
    // Two nodes each adding migration 0002, in one dialect.
    std::fs::write(dir.path().join("sqlite/0002_c.sql"), "").unwrap();
    let err = check_migration_versions(dir.path()).unwrap_err();
    assert!(err.contains("sqlite: version 2"), "{err}");
    // A version only one dialect has.
    std::fs::remove_file(dir.path().join("sqlite/0002_c.sql")).unwrap();
    std::fs::write(dir.path().join("postgres/0003_d.sql"), "").unwrap();
    let err = check_migration_versions(dir.path()).unwrap_err();
    assert!(err.contains("differ"), "{err}");
}

#[test]
fn sqlite_migration_avoids_functions_newer_than_3_37() {
    // unixepoch() is 3.38 and its 'subsec' modifier 3.42; on 3.40 (Debian
    // 12) the old default evaluated to NULL and every insert failed.
    let code: String = SQLITE_SQL
        .lines()
        .map(|l| l.split_once("--").map_or(l, |(code, _)| code))
        .collect::<Vec<_>>()
        .join("\n");
    for newer in ["unixepoch", "subsec", "->>", "iif("] {
        assert!(!code.contains(newer), "{newer}");
    }
}

#[tokio::test]
async fn sqlite_enforces_schema_v1() {
    let dir = tempfile::tempdir().unwrap();
    let url = format!(
        "sqlite://{}?mode=rwc",
        dir.path().join("aspen.db").display()
    );
    let db = Db::connect(&url, Dialect::Sqlite).await;
    exercise(&db).await;
}

#[tokio::test]
async fn postgres_enforces_schema_v1_and_matches_sqlite() {
    let Some(bin) = pg_bin_dir() else {
        assert!(
            std::env::var_os("CI").is_none(),
            "PostgreSQL binaries not found; set ASPEN_TEST_PG_BIN to a directory holding initdb and pg_ctl"
        );
        eprintln!("skipping: PostgreSQL binaries not found (set ASPEN_TEST_PG_BIN)");
        return;
    };
    let cluster = PgCluster::start(&bin);
    let pg = Db::connect(&cluster.url(), Dialect::Postgres).await;
    exercise(&pg).await;

    let dir = tempfile::tempdir().unwrap();
    let url = format!(
        "sqlite://{}?mode=rwc",
        dir.path().join("aspen.db").display()
    );
    let lite = Db::connect(&url, Dialect::Sqlite).await;
    lite.migrate().await;
    let (pg_cols, lite_cols) = (pg.columns().await, lite.columns().await);
    assert!(pg_cols.len() > 50, "{pg_cols:?}");
    assert_eq!(
        pg_cols, lite_cols,
        "the two dialects define the same columns, type classes and nullability"
    );
    let (pg_idx, lite_idx) = (pg.indexes().await, lite.indexes().await);
    assert_eq!(pg_idx, lite_idx, "the two dialects define the same indexes");
    pg.pool.close().await;
}
