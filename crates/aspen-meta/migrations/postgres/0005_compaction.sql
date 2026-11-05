-- Aspen metadata schema, migration 0005: cover chains and pack garbage
-- collection (node C1), PostgreSQL dialect. Additive: it adds tables and
-- indexes and changes no schema v1 table definition.
--
-- Keep this file and ../sqlite/0005_compaction.sql in step, as for 0001.
-- crates/aspen-meta/SCHEMA.md explains the tables and the garbage
-- collection protocol.

-- The target of cover_chains_cover_fk: a chain names its cover together
-- with the cover's ref and first seq, so the chain's first version can be
-- tied to that same ref and seq.
CREATE UNIQUE INDEX covers_id_span_unique ON covers (id, repo_id, ref_name, first_seq);

-- A cover whose range is a verified dependency chain
-- (aspen_core::meta::CoverChain). A cover without a row here is never
-- skipped on the have side of a fetch.
CREATE TABLE cover_chains (
    cover_id          BIGINT NOT NULL
                      CONSTRAINT cover_chains_pkey PRIMARY KEY,
    repo_id           BIGINT NOT NULL,
    ref_name          TEXT COLLATE "C" NOT NULL,
    first_seq         BIGINT NOT NULL,
    first_version_id  BIGINT NOT NULL,
    CONSTRAINT cover_chains_cover_fk FOREIGN KEY (cover_id, repo_id, ref_name, first_seq)
        REFERENCES covers (id, repo_id, ref_name, first_seq),
    -- The chain starts at the ref's version at the cover's first seq.
    CONSTRAINT cover_chains_version_fk FOREIGN KEY (first_version_id, repo_id, ref_name, first_seq)
        REFERENCES ref_versions (id, repo_id, ref_name, seq)
);

-- When register_pack last registered each pack, by the store's clock. The
-- sweep deletes an unreferenced pack only when this is at least the grace
-- period old.
CREATE TABLE pack_touches (
    pack_id         BIGINT NOT NULL
                    CONSTRAINT pack_touches_pkey PRIMARY KEY,
    repo_id         BIGINT NOT NULL,
    touched_at_us   BIGINT NOT NULL,
    CONSTRAINT pack_touches_pack_fk FOREIGN KEY (pack_id, repo_id) REFERENCES packs (id, repo_id)
);

-- Which registration of its checksum a pack row is (aspen_core::pack::
-- PackGeneration). Generation 0 keeps the schema v1 keys; a checksum
-- registered again after the sweep deleted its row gets the next
-- generation, whose keys no pending deletion names.
ALTER TABLE packs ADD COLUMN generation BIGINT NOT NULL DEFAULT 0
    CONSTRAINT packs_generation_range CHECK (generation >= 0 AND generation <= 4294967295);

-- Rows the sweep deleted. deleted_at_us is NULL while the row's blobs are
-- still to be deleted (pending); the row stays afterwards, so that
-- register_pack gives the checksum's next row a later generation.
CREATE TABLE pack_tombstones (
    repo_id        BIGINT NOT NULL,
    checksum       BYTEA NOT NULL,
    generation     BIGINT NOT NULL,
    swept_at_us    BIGINT NOT NULL,
    deleted_at_us  BIGINT,
    CONSTRAINT pack_tombstones_pkey PRIMARY KEY (repo_id, checksum, generation),
    CONSTRAINT pack_tombstones_checksum_len CHECK (length(checksum) = 20),
    CONSTRAINT pack_tombstones_generation_range CHECK (generation >= 0 AND generation <= 4294967295),
    CONSTRAINT pack_tombstones_repo_fk FOREIGN KEY (repo_id) REFERENCES repos (id)
);

-- The sweep's "no reference names this pack" checks, and the foreign key
-- checks of deleting a packs row.
CREATE INDEX ref_versions_pack ON ref_versions (pack_id);
CREATE INDEX covers_pack ON covers (pack_id);
CREATE INDEX jobs_pack ON jobs (pack_id);
-- Pending deletions, oldest first.
CREATE INDEX pack_tombstones_pending ON pack_tombstones (swept_at_us) WHERE deleted_at_us IS NULL;
