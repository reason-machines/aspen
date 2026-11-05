-- Aspen metadata schema v1, PostgreSQL dialect.
--
-- Keep this file and ../sqlite/0001_schema_v1.sql in step: same tables,
-- same columns, same constraints under the same names, same indexes.
-- tests/schema.rs compares the two dialects and drives every named
-- constraint with a row that violates only that constraint.
-- crates/aspen-meta/SCHEMA.md explains every table and invariant.
--
-- Conventions: object IDs and checksums are 20-byte BYTEA; timestamps are
-- BIGINT microseconds since the Unix epoch (UTC), named *_us; enums are TEXT
-- with a CHECK list that matches the aspen-core type; ref names sort
-- bytewise (COLLATE "C"), as Git sorts them. Every constraint is named
-- <table>_<rule>, so errors say which rule a row broke.

CREATE TABLE repos (
    id              BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL
                    CONSTRAINT repos_pkey PRIMARY KEY,
    name            TEXT NOT NULL,
    trunk_ref       TEXT COLLATE "C" NOT NULL,
    allow_force     TEXT NOT NULL DEFAULT 'branches',
    writable_refs   TEXT NOT NULL DEFAULT 'refs/heads/* refs/tags/*',
    no_thin         SMALLINT NOT NULL DEFAULT 0,
    have_tips       SMALLINT NOT NULL DEFAULT 8,
    shallow         TEXT NOT NULL DEFAULT 'serve',
    created_at_us   BIGINT NOT NULL
                    DEFAULT (EXTRACT(EPOCH FROM clock_timestamp()) * 1000000)::BIGINT,
    CONSTRAINT repos_name_unique UNIQUE (name),
    CONSTRAINT repos_trunk_is_branch CHECK (substr(trunk_ref, 1, 11) = 'refs/heads/'),
    CONSTRAINT repos_allow_force_values CHECK (allow_force IN ('none', 'branches', 'all')),
    CONSTRAINT repos_no_thin_bool CHECK (no_thin IN (0, 1)),
    CONSTRAINT repos_have_tips_range CHECK (have_tips BETWEEN 0 AND 32),
    CONSTRAINT repos_shallow_values CHECK (shallow IN ('serve', 'refuse'))
);

CREATE TABLE packs (
    id              BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL
                    CONSTRAINT packs_pkey PRIMARY KEY,
    repo_id         BIGINT NOT NULL,
    checksum        BYTEA NOT NULL,
    pack_key        TEXT NOT NULL,
    size            BIGINT NOT NULL,
    idx_key         TEXT NOT NULL,
    idx_size        BIGINT NOT NULL,
    entries_key     TEXT NOT NULL,
    entries_size    BIGINT NOT NULL,
    object_count    BIGINT NOT NULL,
    ref_delta_count BIGINT NOT NULL,
    level           SMALLINT NOT NULL,
    origin          TEXT NOT NULL,
    received_bytes  BIGINT,
    created_at_us   BIGINT NOT NULL
                    DEFAULT (EXTRACT(EPOCH FROM clock_timestamp()) * 1000000)::BIGINT,
    CONSTRAINT packs_repo_fk FOREIGN KEY (repo_id) REFERENCES repos (id),
    CONSTRAINT packs_checksum_len CHECK (length(checksum) = 20),
    CONSTRAINT packs_checksum_unique UNIQUE (repo_id, checksum),
    CONSTRAINT packs_id_repo_unique UNIQUE (id, repo_id),
    CONSTRAINT packs_id_repo_level_unique UNIQUE (id, repo_id, level),
    -- Header (12 bytes) plus trailing SHA-1 (20 bytes).
    CONSTRAINT packs_size_min CHECK (size >= 32),
    CONSTRAINT packs_object_count_range CHECK (object_count BETWEEN 1 AND 4294967295),
    -- A version 2 .idx: 8-byte header, 256-entry fanout, 28 bytes per
    -- object (ID, CRC, 4-byte offset), one 8-byte large offset for at most
    -- every object, two trailing SHA-1s.
    CONSTRAINT packs_idx_size_v2 CHECK (
        idx_size >= 1072 + 28 * object_count
        AND idx_size <= 1072 + 36 * object_count
        AND (idx_size - 1072 - 28 * object_count) % 8 = 0),
    -- aspen_core::pack::EntryTable: 44-byte header, 52 bytes per entry.
    CONSTRAINT packs_entries_size_exact CHECK (entries_size = 44 + 52 * object_count),
    CONSTRAINT packs_ref_delta_count_range CHECK (ref_delta_count BETWEEN 0 AND object_count),
    CONSTRAINT packs_level_range CHECK (level BETWEEN 0 AND 4),
    CONSTRAINT packs_origin_values CHECK (origin IN ('push', 'compaction')),
    -- Push packs are exactly the L0 packs.
    CONSTRAINT packs_push_iff_l0 CHECK ((origin = 'push') = (level = 0)),
    CONSTRAINT packs_received_bytes_min CHECK (received_bytes >= 0),
    CONSTRAINT packs_received_bytes_iff_push CHECK ((origin = 'push') = (received_bytes IS NOT NULL))
);

CREATE TABLE ref_versions (
    id                BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL
                      CONSTRAINT ref_versions_pkey PRIMARY KEY,
    repo_id           BIGINT NOT NULL,
    ref_name          TEXT COLLATE "C" NOT NULL,
    seq               BIGINT NOT NULL,
    parent_seq        BIGINT GENERATED ALWAYS AS (seq - 1) STORED,
    parent_version_id BIGINT,
    tip_oid           BYTEA,
    -- 1: the closure is exactly the objects reachable from tip_oid; 0: it
    -- may hold more (aspen_core::refs::Closure). NULL exactly for deletions.
    closure_exact     SMALLINT,
    pack_id           BIGINT,
    trunk_dep_id      BIGINT,
    scoped_dep_id     BIGINT,
    created_at_us     BIGINT NOT NULL
                      DEFAULT (EXTRACT(EPOCH FROM clock_timestamp()) * 1000000)::BIGINT,
    CONSTRAINT ref_versions_repo_fk FOREIGN KEY (repo_id) REFERENCES repos (id),
    CONSTRAINT ref_versions_seq_min CHECK (seq >= 1),
    CONSTRAINT ref_versions_seq_unique UNIQUE (repo_id, ref_name, seq),
    CONSTRAINT ref_versions_id_repo_unique UNIQUE (id, repo_id),
    CONSTRAINT ref_versions_id_ref_seq_unique UNIQUE (id, repo_id, ref_name, seq),
    -- The parent is the previous version of the same ref.
    CONSTRAINT ref_versions_parent_iff_not_first CHECK ((seq = 1) = (parent_version_id IS NULL)),
    CONSTRAINT ref_versions_parent_fk FOREIGN KEY (parent_version_id, repo_id, ref_name, parent_seq)
        REFERENCES ref_versions (id, repo_id, ref_name, seq),
    CONSTRAINT ref_versions_tip_len CHECK (length(tip_oid) = 20),
    CONSTRAINT ref_versions_tip_not_zero
        CHECK (tip_oid <> '\x0000000000000000000000000000000000000000'::BYTEA),
    CONSTRAINT ref_versions_closure_iff_live CHECK ((tip_oid IS NULL) = (closure_exact IS NULL)),
    CONSTRAINT ref_versions_closure_exact_bool CHECK (closure_exact IN (0, 1)),
    -- A deletion (NULL tip) has no pack and no dependencies: its closure
    -- is empty.
    CONSTRAINT ref_versions_deleted_no_pack CHECK (tip_oid IS NOT NULL OR pack_id IS NULL),
    CONSTRAINT ref_versions_deletion_no_deps
        CHECK (tip_oid IS NOT NULL OR (trunk_dep_id IS NULL AND scoped_dep_id IS NULL)),
    -- A live version's objects are somewhere: in its pack or its deps
    -- (backstop for the push path's connectivity check).
    CONSTRAINT ref_versions_connected CHECK (
        tip_oid IS NULL OR pack_id IS NOT NULL
        OR trunk_dep_id IS NOT NULL OR scoped_dep_id IS NOT NULL),
    CONSTRAINT ref_versions_pack_fk FOREIGN KEY (pack_id, repo_id) REFERENCES packs (id, repo_id),
    -- Dependencies are earlier versions of the same repository, so the
    -- dependency graph is acyclic.
    CONSTRAINT ref_versions_trunk_dep_fk FOREIGN KEY (trunk_dep_id, repo_id)
        REFERENCES ref_versions (id, repo_id),
    CONSTRAINT ref_versions_scoped_dep_fk FOREIGN KEY (scoped_dep_id, repo_id)
        REFERENCES ref_versions (id, repo_id),
    CONSTRAINT ref_versions_parent_older CHECK (parent_version_id < id),
    CONSTRAINT ref_versions_trunk_dep_older CHECK (trunk_dep_id < id),
    CONSTRAINT ref_versions_scoped_dep_older CHECK (scoped_dep_id < id)
);

-- The only compare-and-swap target: one row per ref, pointing at its
-- current version (a deletion version once deleted). `seq` mirrors the
-- version's seq, so a compare-and-swap that sets `seq = seq + 1` can only
-- land on the expected version's direct child (see SCHEMA.md).
CREATE TABLE refs (
    repo_id     BIGINT NOT NULL,
    ref_name    TEXT COLLATE "C" NOT NULL,
    version_id  BIGINT NOT NULL,
    seq         BIGINT NOT NULL,
    CONSTRAINT refs_pkey PRIMARY KEY (repo_id, ref_name),
    CONSTRAINT refs_version_fk FOREIGN KEY (version_id, repo_id, ref_name, seq)
        REFERENCES ref_versions (id, repo_id, ref_name, seq)
);

CREATE TABLE commit_index (
    repo_id         BIGINT NOT NULL,
    commit_oid      BYTEA NOT NULL,
    ref_version_id  BIGINT NOT NULL,
    -- Parent IDs concatenated in commit order, 20 bytes each.
    parents         BYTEA NOT NULL,
    committer_time  BIGINT NOT NULL,
    CONSTRAINT commit_index_pkey PRIMARY KEY (repo_id, commit_oid),
    CONSTRAINT commit_index_commit_len CHECK (length(commit_oid) = 20),
    CONSTRAINT commit_index_parents_len CHECK (length(parents) % 20 = 0),
    CONSTRAINT commit_index_version_fk FOREIGN KEY (ref_version_id, repo_id)
        REFERENCES ref_versions (id, repo_id)
);

CREATE TABLE covers (
    id          BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL
                CONSTRAINT covers_pkey PRIMARY KEY,
    repo_id     BIGINT NOT NULL,
    ref_name    TEXT COLLATE "C" NOT NULL,
    level       SMALLINT NOT NULL,
    first_seq   BIGINT NOT NULL,
    last_seq    BIGINT NOT NULL,
    pack_id     BIGINT,
    CONSTRAINT covers_repo_fk FOREIGN KEY (repo_id) REFERENCES repos (id),
    CONSTRAINT covers_level_range CHECK (level BETWEEN 1 AND 4),
    CONSTRAINT covers_first_seq_min CHECK (first_seq >= 1),
    CONSTRAINT covers_seq_order CHECK (first_seq <= last_seq),
    CONSTRAINT covers_span_unique UNIQUE (repo_id, ref_name, level, first_seq),
    -- The cover's pack is a compaction pack at the cover's level.
    CONSTRAINT covers_pack_fk FOREIGN KEY (pack_id, repo_id, level)
        REFERENCES packs (id, repo_id, level)
);

CREATE TABLE jobs (
    id                BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL
                      CONSTRAINT jobs_pkey PRIMARY KEY,
    repo_id           BIGINT NOT NULL,
    kind              TEXT NOT NULL,
    ref_version_id    BIGINT,
    pack_id           BIGINT,
    state             TEXT NOT NULL DEFAULT 'queued',
    attempts          INTEGER NOT NULL DEFAULT 0,
    run_after_us      BIGINT NOT NULL,
    lease_owner       TEXT,
    lease_expires_us  BIGINT,
    last_error        TEXT,
    created_at_us     BIGINT NOT NULL
                      DEFAULT (EXTRACT(EPOCH FROM clock_timestamp()) * 1000000)::BIGINT,
    CONSTRAINT jobs_kind_values
        CHECK (kind IN ('index_commits', 'compact', 'mirror_push', 'rewrite_pack')),
    CONSTRAINT jobs_state_values CHECK (state IN ('queued', 'running', 'done', 'dead')),
    CONSTRAINT jobs_attempts_min CHECK (attempts >= 0),
    -- rewrite_pack names a pack; every other kind names a ref version. One
    -- of the two is always set, so its foreign key keeps the job inside
    -- repo_id.
    CONSTRAINT jobs_pack_iff_rewrite CHECK ((kind = 'rewrite_pack') = (pack_id IS NOT NULL)),
    CONSTRAINT jobs_version_iff_not_rewrite
        CHECK ((kind = 'rewrite_pack') = (ref_version_id IS NULL)),
    -- Exactly running jobs hold a lease.
    CONSTRAINT jobs_lease_iff_running CHECK ((state = 'running') = (lease_owner IS NOT NULL)),
    CONSTRAINT jobs_lease_fields_together CHECK ((lease_owner IS NULL) = (lease_expires_us IS NULL)),
    CONSTRAINT jobs_version_fk FOREIGN KEY (ref_version_id, repo_id)
        REFERENCES ref_versions (id, repo_id),
    CONSTRAINT jobs_pack_fk FOREIGN KEY (pack_id, repo_id) REFERENCES packs (id, repo_id)
);

-- Claiming: the oldest due queued job, and running jobs whose lease expired.
CREATE INDEX jobs_queued ON jobs (run_after_us, id) WHERE state = 'queued';
CREATE INDEX jobs_running ON jobs (lease_expires_us) WHERE state = 'running';
-- Enqueueing is idempotent: at most one job that is not done per kind and
-- target (a dead job keeps its slot until an operator resolves it).
CREATE UNIQUE INDEX jobs_open_version ON jobs (kind, ref_version_id) WHERE state <> 'done';
CREATE UNIQUE INDEX jobs_open_pack ON jobs (kind, pack_id) WHERE state <> 'done';
-- Commit index rows by version, for cleanup and the indexing job.
CREATE INDEX commit_index_version ON commit_index (ref_version_id);
