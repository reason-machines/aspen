# Metadata schema

Schema v1 (migration 0001) plus later additive migrations: 0002 adds
`tokens`, 0003 adds `mirrors` and `mirror_refs`, 0004 adds `ssh_keys`,
0005 adds `cover_chains`, `pack_touches`, `pack_tombstones` and `packs.generation` (node C1) and
indexes for garbage collection, 0006 adds `shallow_packs` (node C8), and
0007 adds `shallow_packs.used_at_us` (issue #108).

Aspen keeps refs, ref history, pack metadata, the commit index, compaction
covers, exact shallow packs, the job queue, API tokens, mirror settings and SSH keys in one SQL database: PostgreSQL in production and
`docker compose`, SQLite for `aspen dev`. Pack bytes never go in the
database; they live in the blob store.

The migrations are in [`migrations/postgres`](migrations/postgres) and
[`migrations/sqlite`](migrations/sqlite). They define the same tables,
columns, indexes and constraints, and every constraint has the same name
(`<table>_<rule>`) in both. The Rust types that rows map to are in
`aspen-core` (`meta`, `refs`, `pack`, `repo`).

[`tests/schema.rs`](tests/schema.rs) applies each file to a real database.
For every named constraint it inserts a row that breaks only that constraint
and asserts the database names it in the error (Postgres names every kind;
SQLite names `CHECK` constraints, and for unique and foreign-key violations
only the kind, whose names the Postgres run checks on the same rows); it also inserts one valid
row per enum value and per optional column, and compares column types,
nullability and indexes across the two dialects. A coverage check fails if a
constraint has no case. [`scripts/drop-each-constraint.py`](scripts/drop-each-constraint.py)
deletes each constraint in turn and confirms the test then fails.

SQLite 3.37 or newer is required (`STRICT` tables). The SQLite file uses no
function newer than that: `aspen dev` may run on a system SQLite such as
Debian 12's 3.40, where `unixepoch('subsec')` returns NULL.

## Conventions

- Object IDs and pack checksums are 20-byte binary (`BYTEA` / `BLOB`),
  checked by length.
- Timestamps are `BIGINT` microseconds since the Unix epoch, UTC, in columns
  named `*_us`. Both dialects use the same representation, so one mapping
  serves both (`aspen_core::id::UnixMicros`).
- Enumerations are `TEXT` with a `CHECK` list. Each list matches an
  `aspen-core` type's `as_str` values, which the test checks.
- Ref names sort bytewise (`COLLATE "C"` in Postgres, `BINARY` in SQLite),
  which is how Git orders refs and what `ls-refs` prefix scans need.
- Row IDs increase and are never reused (identity columns,
  `AUTOINCREMENT`). Several invariants below rely on that.
- Every cross-table reference that must stay inside one repository is a
  composite foreign key that includes `repo_id`, so the database itself keeps
  tenants apart.
- SQLite tables are `STRICT`, so a column holds only its declared type.

## Tables

### `repos`

One row per repository.

| Column | Meaning |
|---|---|
| `name` | URL path without `.git`, unique (`aspen_core::name::RepoName`) |
| `trunk_ref` | The only named ref a push advertisement shows; must be under `refs/heads/` |
| `allow_force` | `none`, `branches` (default; every ref except trunk) or `all`. Gates whether a force is possible; the client must still ask with `-o force` or a lease |
| `writable_refs` | Writable namespaces, space-separated patterns (`refs/heads/* refs/tags/*` by default) |
| `no_thin` | 1 to advertise `no-thin` on push (default 0) |
| `have_tips` | Scales the `.have` lines of a push advertisement, 0 to 32 (default 8; 0 advertises none): every one of the newest `8 × have_tips` earlier trunk versions, then tips about a quarter apart back to the oldest (`aspen_core::meta::have_tip_seqs`) |
| `shallow` | `serve` (default; full closure plus exact `shallow-info`) or `refuse` |

### `packs`

One row per stored pack. Writers register, then upload, then reference:
`register_pack` writes the row first and returns it, the pack, its `.idx`
and its entry table are uploaded to that row's keys (see `generation`
below), and only then may a `ref_versions` or `covers` row name it. So
every row a version or cover names can be served; a row nothing names may
not have its blobs yet, and garbage collection removes it after the grace
period. Every stored pack is **self-contained** (no delta base outside the
pack), **base-first** (every base before its deltas) and repeats no object;
see `aspen_core::pack`.

| Column | Meaning |
|---|---|
| `checksum` | Trailing SHA-1 of the stored bytes; the `packfile-uris` hash. Unique per repository |
| `pack_key`, `size` | Blob key and size of the pack: `repos/<repo>/packs/<checksum>.pack` at generation 0, `<checksum>.g<n>.pack` at generation n |
| `idx_key`, `idx_size` | The Git `.idx` beside it (`.idx`). Its size must fit the version 2 layout: 1072 + 28 × objects, plus up to 8 × objects of large offsets |
| `entries_key`, `entries_size` | The per-entry table the stitcher reads (`.entries`, format in `aspen_core::pack::EntryTable`); exactly 44 + 52 × objects bytes. Readers decode it with `EntryTable::decode_for`, which checks it against this row |
| `object_count` | Objects in the pack, 1 to 2³²−1. A push that brings no objects stores no pack |
| `ref_delta_count` | REF_DELTA entries; `no-ref-delta` is safe only when every planned pack has 0 |
| `level` | 0 for push packs, 1 to 4 for compaction output: `origin = 'push'` exactly when `level = 0` |
| `origin` | `push` or `compaction` |
| `received_bytes` | Bytes the client sent, set exactly for push packs. Compared with `size`, it shows how much thin-pack completion grew the pack |

The blob keys are derived from `repo_id` and `checksum`
(`aspen_core::pack::PackKeys`) and stored so operators can find blobs with
SQL alone.

### `ref_versions`

The history of every ref: one row per accepted update.

| Column | Meaning |
|---|---|
| `ref_name`, `seq` | The ref, and the version's position in its history starting at 1; unique together |
| `parent_version_id` | The previous version of the same ref, `NULL` exactly when `seq = 1`. A composite foreign key through the generated `parent_seq` (`seq - 1`) makes it the same ref's version with the previous `seq` |
| `tip_oid` | The ref's value, never the zero ID; `NULL` means the ref was deleted. A deletion is a version too, so a later create is a compare-and-swap against it |
| `closure_exact` | For a live version, 1 when its closure is exactly the objects its tip reaches, 0 when it may hold more (`aspen_core::refs::Closure`); `NULL` exactly for a deletion. A fetch have may stand for the version only when this is 1 (`aspen_core::fetch::HaveVersion`). Computed per write (`Closure::of_new_version`): 1 only when every object in **this version's own pack** is reachable from **this version's own tip** and every dependency is exact with a tip that is an ancestor of it. Judging a pack shared by several writes against the union of their tips is unsafe (a have of one tip would subtract another write's objects). A 0 is inherited by every version that depends on it, so one 0 on trunk makes the whole repository's later history 0; the push path stores one pack per write so that ordinary multi-ref pushes never do that |
| `pack_id` | The pack this write stored, `NULL` if it stored none (always `NULL` for a deletion). A multi-ref push stores one pack per write |
| `trunk_dep_id` | The exact trunk version the push's base lookup and connectivity check searched (always `NULL` for a deletion) |
| `scoped_dep_id` | For a branch-scoped URL push, the scoped ref's old version; for a client's push of one branch through the plain repository URL (not forced or leased), that branch's version the push searched (issue #203). Always `NULL` for a deletion |
| `txid` | Migration 0010: the transaction that wrote the row, so a reader can ask which refs changed since it last read them (`MetaStore::ref_changes`, which `ls-refs` uses to keep listings in memory). On Postgres the writing top-level transaction's 64-bit ID (`pg_current_xact_id()`, the column default); a reader's cursor is its statement's snapshot, `xmax` plus the transactions it saw in progress, so a write that commits after a later-numbered one is still found. On SQLite, where writes run one at a time, the row's own `id`, set by the trigger `ref_versions_txid`. `0` for rows written before the migration, which every reader sees. Indexed with `repo_id` (`ref_versions_repo_txid`) |

Invariant (**connected**): every object reachable from `tip_oid` is in
`pack_id` or in the closure of the dependencies. The push path's
connectivity check enforces it before a version is committed; as a
backstop, the database refuses a live version with neither a pack nor a
dependency (`ref_versions_connected`). A deletion's closure is empty, so it
has neither a pack nor dependencies (`ref_versions_deleted_no_pack`,
`ref_versions_deletion_no_deps`). Parent and
dependencies must have smaller IDs (`CHECK`), so the dependency graph is
acyclic.

### `refs`

`(repo_id, ref_name) → version_id, seq`: the current version of each ref,
and the only compare-and-swap target in Aspen. The foreign key on
`(version_id, repo_id, ref_name, seq)` makes `version_id` a version of the
same ref and `seq` that version's `seq`. A push moves it with

```sql
UPDATE refs SET version_id = :new, seq = seq + 1
WHERE repo_id = :repo AND ref_name = :name AND version_id = :expected;
```

and treats zero affected rows as a lost compare-and-swap (or inserts the row
when the verdict expected none). A racing push usually loses one step
earlier: both pushes insert the new `ref_versions` row with the same `seq`,
and the loser gets a `ref_versions_seq_unique` violation (in Postgres, after
waiting for the winner's transaction). Stores map that violation, like a
conflicting `refs` insert, to `CommitOutcome::CasLost`, never to a storage
error, so the push recomputes its verdict and answers with the race-loss
text instead of "internal storage error".

Two SQLite details for the store (node A3):

- SQLite does not report a unique violation by constraint name. It names
  the table and columns, for example `UNIQUE constraint failed:
  ref_versions.repo_id, ref_versions.ref_name, ref_versions.seq` for
  `ref_versions_seq_unique`, and a foreign-key violation only as `FOREIGN
  KEY constraint failed`. The SQLite store therefore maps violations by
  table and columns, not by the name Postgres reports.
- Every transaction that writes (pushes, commit indexing, job claims and
  updates, enqueues, dead-job requeues) starts with `BEGIN IMMEDIATE`. A
  deferred transaction that reads and then writes can fail with `SQLITE_BUSY` at its first write
  instead of waiting for the other writer; `BEGIN IMMEDIATE` takes the write
  lock up front, so racing pushes serialize and the loser sees the unique
  violation above. (With deferred transactions, the store's concurrent
  claim test fails with `database is locked` on every run.)
- Before `BEGIN IMMEDIATE`, a write transaction waits its turn in the
  store's write queue (`src/tx.rs`, `WriteQueue`), first in, first out.
  SQLite's busy handler polls for the lock and serves writers in no order,
  so a sweep running batch after batch could keep a push waiting past the
  lock timeout. The queue gives up only once the lock timeout passes with no
  write transaction ending. It orders one store's writers (its clones
  included); other processes or stores on the same file still meet the busy
  timeout. So do this store's few autocommit writes outside a transaction
  (`insert_token`, `revoke_token`, `insert_ssh_key`, `revoke_ssh_key`),
  which skip the queue.

Because the new row must match the new version's `seq`, and the parent
foreign key makes the version with the next `seq` a child of the version
before it, the database guarantees the new version's parent is exactly
`:expected`: an update back to an older version, past a version, or onto
another ref's version fails. The verdict that justified the update was
computed against `:expected` (`aspen_core::refs::Verdict::against`).

The store is `aspen_meta::SqlStore`, one implementation for both dialects
through sqlx's `Any` driver; the constraint mapping is in `src/error.rs`
and `tests/store.rs` runs the same suite against both. A push writes its
`commit_index` rows in commit ID order, so two pushes that index the same
commits cannot deadlock on each other's keys.

### `commit_index`

`(repo_id, commit_oid) → ref_version_id, parents, committer_time,
generation, root_tree, generation_waits_on`, written
synchronously for each pushed tip and by an `index_commits` job for the rest.

| Column | Meaning |
|---|---|
| `ref_version_id` | A version whose tip reaches the commit, so its closure holds the commit's whole history; read the commit from its packs when needed. Safe for resolving a want, never a have |
| `parents` | Parent IDs concatenated in commit order, 20 bytes each (empty for a root commit) |
| `committer_time` | Seconds since the epoch, for `deepen-since` |
| `generation` | Migration 0011: the commit's generation number (Git's commit-graph topological level: 1 for a root, otherwise one more than its parents' largest), or `NULL` when not known yet. Every ancestor of a commit has a smaller one, so an ancestry walk stops at the other commit's generation. Derived from history, so every writer computes the same value: a push writes it when its parents' rows have one, the `index_commits` job backfills the rest (`MetaStore::unexamined_commits`, paged through the partial index `commit_index_unexamined`), and `MetaStore::set_generations` never changes a value already written. `NULL` only costs time: the walk then runs without generations |
| `generation_waits_on` | Migration 0011: for a row without a generation that a backfill examined and could not fill, the commit whose generation it waits on (a parent, or an ancestor through parents that wait on it too), 20 bytes; `NULL` otherwise. Such a row leaves `commit_index_unexamined` and is found again through the partial index `commit_index_waiting` only when a backfill gives that commit a generation (`MetaStore::commits_waiting_on`), so each row is examined once however many jobs run; `set_generations` clears it |
| `root_tree` | Migration 0011: the commit's root tree ID, 20 bytes; `NULL` for rows written before it or for a tip the push read back from a pack. Stored for later use; nothing reads it yet |

A missing row means "not indexed yet", never "no parents" or "not an
ancestor" (`aspen_core::meta::CommitLookup`).

Migration 0011 on a large `commit_index`: adding the nullable columns does
not rewrite the table, and on PostgreSQL the three `CHECK` constraints are
added `NOT VALID` (every existing row holds `NULL` in the new columns, which
satisfies them; rows written afterwards are checked), so no statement scans
the table under the migration's exclusive lock. Building the two partial
indexes does read the table and blocks writes to it while they build, since
a migration runs in a transaction and cannot use `CREATE INDEX
CONCURRENTLY`. To upgrade a large deployment without that pause, add the
columns and build the indexes first with `CREATE INDEX CONCURRENTLY`, under
the names and definitions in `migrations/postgres/0011_commit_generation.sql`
(`ADD COLUMN IF NOT EXISTS` and `CREATE INDEX IF NOT EXISTS` there then do
nothing); then run the migration.

### `covers`

Compaction covers: versions `first_seq..=last_seq` of one ref at level 1 to
4, repacked into `pack_id` once compacted (`NULL` until then). The foreign
key on `(pack_id, repo_id, level)` makes the pack a compaction pack at the
cover's level. The compactor (node C1, `aspen-compact`) creates them with
`MetaStore::insert_cover`, idempotently per `(repo, ref, level,
first_seq)`, and sets the pack once with `MetaStore::set_cover_pack`.

### `cover_chains`

One row per cover whose range is a verified **dependency chain**
(`aspen_core::meta::CoverChain`, migration 0005): every version in the range
after the first depends on its parent. A client holding the closure of a
version in the range holds the closure of every earlier version in it, so
the fetch planner may skip the range on the have side; a cover without a
row is never skipped. The row is written in the same transaction as its
cover, from an `aspen_core::meta::VerifiedRange`, and never changes.

| Column | Meaning |
|---|---|
| `cover_id` | The cover, primary key |
| `repo_id`, `ref_name`, `first_seq` | Copies of the cover's, tied to it by `cover_chains_cover_fk` (on `covers_id_span_unique`) |
| `first_version_id` | The ref's version at `first_seq`, where a skip lands. `cover_chains_version_fk` makes it exactly that version |

### `pack_touches`, `pack_tombstones` and `packs.generation`: garbage collection

A writer registers a pack (`MetaStore::register_pack`), then uploads its
blobs, then commits the version or cover that names it, so for a while
every new pack is unreferenced on purpose. A push that loses its
compare-and-swap, is refused or dies, or a compactor that dies, leaves its
pack unreferenced for good (issue #48).
`MetaStore::sweep_orphan_packs(lease, grace, limit)` deletes those rows; the
caller (`aspen-compact`'s GC) then deletes their blobs and finishes the
tombstones (`MetaStore::pending_pack_deletes`,
`MetaStore::finish_pack_deletes`).

| Table | Column | Meaning |
|---|---|---|
| `packs` | `generation` | Which registration of its checksum the row is (`aspen_core::pack::PackGeneration`): 0 for the first, one more than the highest tombstoned generation after that. Generation 0 keeps the schema v1 keys; generation `n` uses `repos/<repo>/packs/<checksum>.g<n>.{pack,idx,entries}`, and the key columns always match the row's generation |
| `pack_touches` | `pack_id` (primary key), `repo_id`, `touched_at_us` | When `register_pack` last registered the pack, by the store's clock; every registration, of a new or an existing row, sets it (never moving it back) |
| `pack_tombstones` | `repo_id`, `checksum`, `generation` (primary key), `swept_at_us`, `deleted_at_us` | A row the sweep deleted. Pending (blobs still to delete) while `deleted_at_us` is null. Rows are kept after they are finished, so the checksum's next row still gets a later generation; they never expire |
| `gc_cursors` (migration 0009) | `name` (primary key), `touched_at_us`, `pack_id`, `audit_pack_id` | Where the sweep (row `orphan_packs`) resumes: the `(touched_at_us, pack_id)` key of the last registration its ordered walk decided (the watermark), and the last pack ID its audit read (0 to start over) |
| `leases` (migration 0009) | `name` (primary key), `owner`, `epoch`, `expires_at_us` | Time-limited leases on work one process does at a time (`MetaStore::acquire_lease`; the sweep's is `gc`). `epoch` grows by one each time the lease changes hands or is taken again after it expired. Expiry is by the store's clock |

Index `pack_touches_due` (migration 0009) on `pack_touches
(touched_at_us, pack_id)` orders registrations by when their grace periods
end.

The sweep, in one write transaction of bounded size (throughput item 7;
before migration 0009 it picked candidates with one query over every pack,
whose cost grew with the number of packs: 7.9 s per pass at 1.5 million
packs in a load test):

1. Extend the `gc` lease (`UPDATE leases ... WHERE` owner, epoch and not
   expired). If that matches no row, refuse (`StoreError::lease_lost`) and
   write nothing. On Postgres the update holds the lease row until the
   transaction ends, so a takeover waits for it.
2. The walk: up to `limit` `pack_touches` rows after the watermark whose
   touch is at least `grace` old, in `(touched_at_us, pack_id)` order (a
   range scan of `pack_touches_due`).
3. The audit: the next `limit` `packs` rows by ID after the audit cursor
   (wrapping to the start once it reads fewer), each with its touch. A row
   with no touch (registered before migration 0005) gets one with the
   current time, so the walk deletes it a grace period later. A row whose
   touch is old and that the watermark already passed is a candidate
   again.
4. Lock the candidates that no `ref_versions.pack_id`, `covers.pack_id`,
   `jobs.pack_id` (a job of any state) or `shallow_packs.pack_id` names,
   `FOR UPDATE OF p SKIP LOCKED` on Postgres.
5. Delete the locked candidates' touch rows and then their `packs` rows,
   each `DELETE` re-checking that nothing names the pack and that the
   touch is still old, and leave a pending tombstone for each deleted row
   with its generation.
6. Move the watermark to the last walked registration, but never past a
   walked candidate that no row names and that the lock skipped: a
   transaction holds it (it is naming the pack, or registering it again)
   and may roll back, leaving an orphan the walk must see again. Return the
   deleted rows, the rows read, and whether the walk caught up.

Why every orphan is eventually found: a pack whose registration the walk
passed was named then (or was skipped, which holds the watermark). Rows
that name a pack are never deleted, except `shallow_packs` recordings, and
every statement that deletes a recording touches the pack in the same
transaction (eviction, and the byte cap's eviction in `set_shallow_pack`).
Rows stop naming a pack in one other place, `MetaStore::replace_pack` (a
`rewrite_pack` job swapping in a pack's base-first rewrite, below), which
touches the old pack in the same transaction too.
A touch moves the registration ahead of the watermark, so the walk reads it
again once its grace period ends. **Any new code that stops naming a pack
must touch it in the same transaction**, as eviction does; the audit is
the backstop (it finds an orphan the walk passed within one cycle through
every pack, `limit` packs per transaction), and it is also what stamps
rows with no touch. A touch whose time is already behind the watermark
(a server clock more than `grace` behind the sweeper's) is also left to the
audit.

Why only one process sweeps at a time: the compactor's pass first takes
the lease (`MetaStore::acquire_lease`: insert it, take it over once
expired, or extend it as its holder) and does nothing while another owner
holds it. Each sweep transaction extends it in step 1, so the holder keeps
it while it works, and a holder that dies loses it `ttl` after its last
transaction. A holder that stalls past its expiry and resumes is refused in
step 1 once anyone took the lease (the epoch differs), and until then
nobody else holds it. The lease limits cost, not safety: the argument below
holds for any number of concurrent sweeps.

Registration, in one write transaction: lock the existing row with the
checksum (`FOR KEY SHARE` on Postgres) and return it, touched; or insert a
new row, then read the highest tombstoned generation for the checksum and,
if there is one, move the new row to the next generation and its keys. The
read comes after the insert on purpose: on Postgres a sweep that is
deleting the checksum's old row holds its unique index entry, so the insert
waits for that sweep to commit, and only a later statement sees its
tombstone.

Why no referenced pack is deleted: on Postgres, every statement that makes
a row name a pack checks its foreign key, which locks the pack's row
`FOR KEY SHARE` until that transaction ends; `register_pack` locks an
existing row the same way, and `commit_push`, `set_cover_pack` and
`set_shallow_pack` lock the packs they name before writing. Those locks conflict with the sweep's
`FOR UPDATE`, so the sweep skips a pack an open transaction is using, and
once the sweep holds a pack's lock, nobody can start using it until the
sweep ends. The `DELETE` statements run after the lock is held with a fresh
snapshot, so they see every reference committed before. A writer that then
finds its pack gone gets `StoreError::Unavailable` from `commit_push`,
`set_cover_pack` or `set_shallow_pack` (retryable; nothing is written). Pack IDs are never
reused (SQLite `AUTOINCREMENT`, a Postgres identity), so a writer that
commits by the ID it registered commits that generation or nothing, even
when someone else has registered the checksum again since. On SQLite every
write transaction is `BEGIN IMMEDIATE`, so all of this runs one transaction
at a time. The full argument is in `src/gc.rs`.

Why no live pack's blobs are deleted, with no timing assumption: a
tombstone names the keys of a generation whose row is gone, and no later
row of that checksum gets that generation again. Writers upload only after
registering, to the keys of the row they registered. So every blob a live
row names was uploaded under keys no tombstone names, and a deletion can
remove nothing a row names, however late it runs (a paused VM, a retry
storm) and whichever worker runs it. That is also why any worker may
re-drive any pending tombstone: a crashed worker's deletions are finished
by the next pass. `grace` only bounds how long a writer may take from
registering to committing: a writer slower than that may find its row
swept and fails retryably, as above. Clocks only need to agree to well
within `grace`, since each server's store clock stamps the touches it
writes.

**Upgrading to migration 0005: stop every pre-0005 binary before any
0005 binary runs garbage collection.** A binary built before 0005 uploads
to the generation-0 keys and only then registers, and its insert takes the
column default, generation 0. If that checksum has a pending tombstone at
generation 0, the new GC deletes exactly the keys the old binary just
uploaded, after its version committed. There is no rolling upgrade across
0005: drain the old servers, then start the new ones (see
[`deploy/README.md`](../../deploy/README.md#upgrading)).

Two leaks remain, both blobs without a row, never a row without its
blobs: a writer that is still uploading when its row is swept (it was
slower than `grace` between registering and uploading) leaves the blobs it
uploads after the deletion ran, and blobs whose deletion fails stay until
a later pass succeeds. Nothing lists the bucket to find blobs no row or
tombstone names (follow-up issue).

A sweep transaction reads at most `2 × limit` rows (the walk and the
audit), whatever the number of packs, and probes four indexes only for its
candidates (`crates/aspen-meta/tests/sweep_cost.rs` checks the bound and
both scans' plans; at a million packs a caught-up batch takes milliseconds
where the earlier candidate query read every pack).

### `shallow_packs`

The exact pack of a shallow fetch (`git fetch --depth=<n>`) of one ref
version at one depth (node C8, migration 0006): the commits fewer than
`depth` parents below the version's tip, and every tree and blob their trees
reach. A builder registers the pack (a compaction pack at level 1), uploads
it, and records it with `MetaStore::set_shallow_pack`; later shallow fetches
of that version and depth read it with `MetaStore::shallow_pack` and serve
it instead of the full history.

| Column | Meaning |
|---|---|
| `repo_id` | The repository of both the version and the pack |
| `version_id`, `depth` | The ref version and the depth, primary key (`shallow_packs_pkey`). `depth` runs from 1 to 2147483646 (`shallow_packs_depth_range`, `aspen_core::meta::ShallowDepth`); Git's `deepen 2147483647` means "unshallow" and is never cached |
| `pack_id` | The pack. `shallow_packs_pack_fk` on `(pack_id, repo_id)` and `shallow_packs_version_fk` on `(version_id, repo_id)` keep both in the row's repository |
| `recorded_at_us` | When the row was recorded, by the store's clock, like `pack_touches.touched_at_us`: eviction measures age against the clock the sweep uses |
| `used_at_us` | When a fetch last used the row, by the store's clock; the recording itself counts as a use (migration 0007, `MetaStore::mark_shallow_pack_used`, which the fetch path calls at most once a minute per row and process and which never moves it backwards). Default 0 only so the migration could add it: a row left at 0 reads as long unused and is evicted, never kept |

Recording, in one write transaction: lock the pack's row (`FOR KEY SHARE`
on Postgres) and fail with `StoreError::Unavailable` if the sweep has
deleted it, or with `StoreError::Conflict` if the pack or the version
belongs to another repository; lock the repository's `repos` row (`FOR NO
KEY UPDATE` on Postgres, which a push's `FOR KEY SHARE` does not wait for;
SQLite's `BEGIN IMMEDIATE` already runs writers one at a time), so the
repository's recordings run one at a time; if the version and depth already
have a row, return its pack (the first recording wins: a builder that lost
the race gets the winner's pack, and its own pack is left unreferenced for
the sweep). Otherwise apply the repository's byte cap (issue #108): count
the stored bytes (`size + idx_size + entries_size`) of every distinct pack
the repository's rows name, each once; a new pack larger than the cap on
its own is refused (`ShallowRecorded::OverLimit`, nothing written); while
the new pack does not fit, delete every row naming the pack with the
oldest last use (the latest `used_at_us` of its rows) and touch that pack
(`pack_touches`); then insert the row, recorded and used now.

While a row names a pack, the sweep leaves the pack alone (step 1 above).
`MetaStore::evict_shallow_packs(superseded_after, unused_after, limit)`
deletes up to `limit` rows recorded at least `superseded_after` ago whose
version is no longer the current version of its ref (no `refs` row points
at it; a ref never moves back to an old version), and rows last used at
least `unused_after` ago, current version or not. In the
same transaction it touches each evicted pack (`pack_touches`, as a
registration does), so the sweep keeps the pack for a whole grace period
after the eviction: a fetch that read the row just before may still be
streaming the pack's blobs. The indexes `shallow_packs_pack` (the sweep's
check and the foreign key check of deleting a `packs` row) and
`shallow_packs_recorded` (eviction, oldest first) serve the two, and
`refs_version` (on `refs.version_id`, also migration 0006) makes the
"no `refs` row points at it" check one lookup per candidate.
`shallow_packs_used` (migration 0007) serves the unused check, and
`shallow_packs_repo` (`repo_id, used_at_us`) a repository's rows for its
byte cap.

### `jobs`

The background queue, written in the same transaction as the push that
creates the work.

| Column | Meaning |
|---|---|
| `kind` | `index_commits`, `compact` or `mirror_push` (all name `ref_version_id`), or `rewrite_pack` (names `pack_id`) |
| `state` | `queued`, `running`, `done` or `dead` |
| `attempts` | Claims so far; after 10, a failure or a reclaim of an expired lease parks the job as `dead`. Workers name their attempt in every update, so a worker whose lease was reclaimed cannot finish the new attempt |
| `run_after_us` | Not before this time (backoff after a failure) |
| `lease_owner`, `lease_expires_us` | Set exactly while `running`. An expired lease may be claimed by another worker, so job work is idempotent. A worker that did not get a claim's answer gives back the jobs leased under its name (`release_jobs`, issues #131 and #188), and only those of the kinds it runs, instead of waiting for the lease to expire |
| `last_error` | Error of the last failed attempt |
| `priority` | Migration 0008. Fetch heat: the fetch path adds, per ref, the versions it had to send as their own push packs because their covers were not built yet (`add_compaction_heat`, queued `compact` jobs of that ref at or after the earliest such version only; at most 2^40). A claim takes the due job with the highest `priority` first, then the earliest due, then the lowest ID. `0` for every job no fetch asked for, which keeps them oldest first |

`jobs_queued_priority` (`priority DESC, run_after_us, id` over queued jobs,
migration 0008) serves the claim; `jobs_queued` still serves the
oldest-due reads of the queue-lag gauge.

The state machine is `aspen_core::meta::Job::apply`; stores implement the
same rules in SQL. Enqueueing is idempotent: the partial unique indexes
`jobs_open_version` and `jobs_open_pack` allow one job that is not `done`
per kind and target, so stores insert with `ON CONFLICT DO NOTHING`. A
`dead` job keeps its slot until an operator resolves it.

**Dead jobs must alert.** A dead job means work the system relies on is
not happening: a dead `index_commits` leaves commits resolvable only
through pack reads, and a dead `rewrite_pack` leaves a stored pack with a
forward base, which pages on every fetch that stitches it. The server's
metrics (node C5) export the number of jobs per kind and state
(`aspen_jobs{state="dead"}`, read by `SqlStore::job_counts`), and the
shipped alert rule `AspenDeadJobs` pages on any value above zero
(`docs/OBSERVABILITY.md`). The operator fixes the cause (the job's `last_error` says what
failed), then either requeues the work by resolving the dead job and
enqueueing a fresh one, in one transaction:

```sql
BEGIN;
UPDATE jobs SET state = 'done' WHERE id = :job_id AND state = 'dead';
-- Continue only if the UPDATE changed one row; otherwise roll back.
INSERT INTO jobs (repo_id, kind, ref_version_id, pack_id, run_after_us)
SELECT repo_id, kind, ref_version_id, pack_id, :now_us
FROM jobs WHERE id = :job_id;
COMMIT;
```

Marking the dead row `done` frees its slot in the partial unique index, so
the insert gets it. The new job has a new ID and starts at `attempts = 0`.
Never reset `attempts` on the dead row itself: workers fence every update
on the job, worker and attempt, and reusing attempt numbers on the same job
would let a stale worker that still holds an old attempt renew or finish the
new one.

Or, when the work was done some other way, mark it done:

```sql
UPDATE jobs SET state = 'done' WHERE id = :job_id AND state = 'dead';
```

**`rewrite_pack` jobs** (reason-machines/aspen#10). The fetch path
enqueues one when a stored pack it stitches has a delta before its base;
the compaction worker (`aspen-compact`, which `aspen serve` runs under
`ASPEN_ROLE` `all` or `compact`) claims it. The job writes the pack's
base-first rewrite (same objects, entries moved, nothing recompressed),
verifies it, registers it as a new `packs` row of the same repository and
origin (so at the same level), uploads its blobs, and then
`MetaStore::replace_pack` moves, in one transaction, every
`ref_versions.pack_id`, `covers.pack_id` and `shallow_packs.pack_id` that
named the old pack to the new row. This is the only update of a committed
version's or cover's pack, and it changes no object set, so every closure
and plan stays correct. The job's own row keeps naming the old pack, so
the sweep keeps the old pack's row and blobs for as long as that row
exists: a server process may still hold the old ID in a push's search
packs. A job on a pack nothing names any more, or on one that is already
base-first, finishes without writing (a lease expiry or a requeue may run
it twice).

`aspen doctor jobs` lists dead jobs with their `last_error`, and
`aspen doctor jobs requeue <id>` (resolve, then enqueue a fresh job, in one
transaction) and `aspen doctor jobs resolve <id>` run these statements
through `SqlStore::requeue_dead_job` and `SqlStore::resolve_dead_job`
(`docs/OBSERVABILITY.md`, "Dead jobs"). The SQL above is the same
procedure by hand.

### `tokens`

API tokens (migration 0002, node B3). Additive: it references no schema v1
table and changes none. A token's text is `aspen_<id>_<secret>`
(`aspen_auth::token`); the secret is never stored, only its SHA-256 digest,
and a presented secret is checked by comparing digests in constant time.

| Column | Meaning |
|---|---|
| `id` | The token's public ID, 16 bytes, primary key |
| `secret_sha256` | SHA-256 digest of the 32-byte secret, 32 bytes |
| `subject` | Who the token acts as (`agent:ci-7`), 1 to 255 characters; used in logs and as the rate-limit key, never as a source of grants |
| `grants` | Space-separated grants in `aspen-auth`'s text form (`write:acme/mono read:demo/*`); `aspen-meta` does not parse them. Empty means the token holds only what anonymous callers hold |
| `description` | A note for operators (default empty) |
| `created_at_us` | Set by the caller (the token's issue time), not by the database |
| `expires_at_us` | When it stops working, if ever; after `created_at_us` |
| `revoked_at_us` | When it was revoked, if it was; never before `created_at_us`. A second revocation keeps the first time |

Inherent `SqlStore` methods (`insert_token`, `token`, `tokens`,
`revoke_token`) read and write it; `aspen-server`'s
`access::tokens::SqlTokens` adapts them to `aspen_auth::TokenStore`.

### `ssh_keys`

SSH public keys (migration 0004, node C3). Additive: it references no other
table and changes none. A key is found by its fingerprint once the SSH
transport has checked that the client holds the private key
(`aspen_auth::ssh`).

| Column | Meaning |
|---|---|
| `fingerprint` | SHA-256 of `public_key`, 32 bytes, primary key; its text form `SHA256:<base64>` is what `ssh-keygen -l` prints |
| `public_key` | The key's wire encoding (the base64 part of an `authorized_keys` line, decoded), 1 to 1,024 bytes |
| `subject` | Who the key acts as (`user:alice`), 1 to 255 characters; used in logs and as the rate-limit key, never as a source of grants |
| `grants` | Space-separated grants in `aspen-auth`'s text form; empty means the key holds only what anonymous callers hold |
| `description` | A note for operators (default empty) |
| `created_at_us` | Set by the caller (the registration time), not by the database |
| `expires_at_us` | When it stops working, if ever; after `created_at_us` |
| `revoked_at_us` | When it was revoked, if it was; never before `created_at_us`. A second revocation keeps the first time |

A fingerprint is registered once: inserting it again, even after a
revocation, is a conflict, so a revoked key cannot be brought back by
registering it again. Inherent `SqlStore` methods (`insert_ssh_key`,
`ssh_key`, `ssh_keys`, `revoke_ssh_key`) read and write it;
`aspen-server`'s `ssh::keys::SqlSshKeys` adapts them to
`aspen_auth::SshKeyStore` and checks, on every read, that the stored key
hashes to its fingerprint.
### `mirrors` and `mirror_refs`

Mirror mode (migration 0003, node C4; `docs/MIRROR.md`). Additive: they
reference `repos` and change nothing in schema v1. No secret is stored:
`credential` names one the operator configures in the environment.

`mirrors`, one row per mirrored repository:

| Column | Meaning |
|---|---|
| `repo_id` | The repository, primary key; references `repos` |
| `upstream_url` | The upstream's smart-HTTP URL, 1 to 2,048 bytes, without credentials |
| `mode` | `proxy` (pushes forwarded before they are accepted) or `async` (pushed by `mirror_push` jobs) |
| `credential` | The configured credential's name, at most 64 characters; empty for anonymous access |
| `created_at_us` | Set by the caller |
| `last_sync_at_us`, `last_error` | When the last sync pass ended, and its error (null when it succeeded) |

`mirror_refs`, the sync ledger, one row per ref that needs one:

| Column | Meaning |
|---|---|
| `repo_id`, `ref_name` | Primary key; `repo_id` references `mirrors` and the rows go with their mirror (`ON DELETE CASCADE`) |
| `synced_oid` | The value Aspen and the upstream last agreed on; null for "absent on both" |
| `conflict_at_us` | Set while both sides have changed the ref since the agreement, to different values; null otherwise |
| `conflict_aspen_oid`, `conflict_upstream_oid` | Each side's value when the conflict was found (null: deleted on that side); only with `conflict_at_us` (`mirror_refs_conflict_fields`) |
| `updated_at_us` | When the row was last written |

A ref with neither an agreement nor a conflict has no row. Writers change a
row only from the `synced_oid` they read (`SqlStore::set_mirror_ref`, a
compare-and-swap; a missing row counts as null), so two workers reconciling
the same ref cannot write a stale agreement over a newer one. Inherent
`SqlStore` methods read and write both tables; `aspen-server`'s
`mirror::SqlMirrors` adapts them to `aspen_mirror::MirrorStore`.

## Changing the schema

Add a new numbered migration to **both** dialects; never edit a migration
that has shipped (sqlx records a checksum of each applied file). Name every
constraint, update this file and the `aspen-core` types in the same change,
and add a case for the new constraint to `tests/schema.rs` (the coverage
check fails until you do).
