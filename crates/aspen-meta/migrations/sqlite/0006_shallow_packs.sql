-- Aspen metadata schema, migration 0006: exact shallow packs (node C8),
-- SQLite dialect. Additive: it adds a table and indexes and changes no
-- earlier table definition.
--
-- Keep this file and ../postgres/0006_shallow_packs.sql in step, as for 0001.
-- crates/aspen-meta/SCHEMA.md explains the table and its place in garbage
-- collection.

-- The exact pack of a shallow fetch of one ref version at one depth
-- (aspen_core::meta::MetaStore::set_shallow_pack): the commits fewer than
-- depth parents below the version's tip, and every tree and blob their
-- trees reach. Recorded once per (version, depth); the first recording
-- wins. While a row names a pack, the sweep counts the pack as named.
CREATE TABLE shallow_packs (
    repo_id         INTEGER NOT NULL,
    version_id      INTEGER NOT NULL,
    depth           INTEGER NOT NULL,
    pack_id         INTEGER NOT NULL,
    -- When the row was recorded, by the store's clock: eviction measures
    -- its age against that clock, as the sweep does with pack_touches.
    recorded_at_us  INTEGER NOT NULL,
    CONSTRAINT shallow_packs_pkey PRIMARY KEY (version_id, depth),
    -- 1 to one below Git's INFINITE_DEPTH (aspen_core::meta::ShallowDepth).
    CONSTRAINT shallow_packs_depth_range CHECK (depth BETWEEN 1 AND 2147483646),
    CONSTRAINT shallow_packs_version_fk FOREIGN KEY (version_id, repo_id)
        REFERENCES ref_versions (id, repo_id),
    CONSTRAINT shallow_packs_pack_fk FOREIGN KEY (pack_id, repo_id) REFERENCES packs (id, repo_id)
) STRICT;

-- The sweep's "no recording names this pack" check, and the foreign key
-- check of deleting a packs row.
CREATE INDEX shallow_packs_pack ON shallow_packs (pack_id);
-- Eviction, oldest recording first.
CREATE INDEX shallow_packs_recorded ON shallow_packs (recorded_at_us);
-- Eviction's "no refs row points at this version" check, one lookup per
-- candidate instead of a scan of refs.
CREATE INDEX refs_version ON refs (version_id);
