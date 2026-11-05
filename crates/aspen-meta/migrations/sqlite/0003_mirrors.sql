-- Aspen metadata schema, migration 0003: mirror mode (node C4), SQLite
-- dialect. Additive: it adds two tables and changes nothing in schema v1.
--
-- Keep this file and ../postgres/0003_mirrors.sql in step, as for 0001.
-- No credential is stored here: `credential` names one the operator
-- configures (docs/MIRROR.md), and secrets stay in that configuration.

-- One row per mirrored repository: where its upstream is and how pushes
-- reach it.
CREATE TABLE mirrors (
    repo_id          INTEGER NOT NULL
                     CONSTRAINT mirrors_pkey PRIMARY KEY,
    upstream_url     TEXT NOT NULL,
    mode             TEXT NOT NULL,
    credential       TEXT NOT NULL DEFAULT '',
    created_at_us    INTEGER NOT NULL,
    last_sync_at_us  INTEGER,
    last_error       TEXT,
    CONSTRAINT mirrors_repo_fk FOREIGN KEY (repo_id) REFERENCES repos (id),
    CONSTRAINT mirrors_mode_values CHECK (mode IN ('proxy', 'async')),
    CONSTRAINT mirrors_url_length CHECK (length(upstream_url) BETWEEN 1 AND 2048),
    CONSTRAINT mirrors_credential_length CHECK (length(credential) <= 64)
) STRICT;

-- The sync ledger: per ref, the value Aspen and the upstream last agreed
-- on (`synced_oid`, null for "agreed absent"), and the conflict found when
-- both sides moved since. A ref with neither has no row.
CREATE TABLE mirror_refs (
    repo_id                INTEGER NOT NULL,
    ref_name               TEXT NOT NULL,
    synced_oid             BLOB,
    conflict_at_us         INTEGER,
    conflict_aspen_oid     BLOB,
    conflict_upstream_oid  BLOB,
    updated_at_us          INTEGER NOT NULL,
    CONSTRAINT mirror_refs_pkey PRIMARY KEY (repo_id, ref_name),
    CONSTRAINT mirror_refs_mirror_fk FOREIGN KEY (repo_id)
        REFERENCES mirrors (repo_id) ON DELETE CASCADE,
    -- Either side's value in a conflict may be absent (a deletion), but
    -- values without a conflict time are not a conflict.
    CONSTRAINT mirror_refs_conflict_fields
        CHECK (conflict_at_us IS NOT NULL
               OR (conflict_aspen_oid IS NULL AND conflict_upstream_oid IS NULL)),
    CONSTRAINT mirror_refs_synced_len
        CHECK (synced_oid IS NULL OR length(synced_oid) = 20),
    CONSTRAINT mirror_refs_conflict_aspen_len
        CHECK (conflict_aspen_oid IS NULL OR length(conflict_aspen_oid) = 20),
    CONSTRAINT mirror_refs_conflict_upstream_len
        CHECK (conflict_upstream_oid IS NULL OR length(conflict_upstream_oid) = 20)
) STRICT, WITHOUT ROWID;
