-- Aspen metadata schema, migration 0010: the transaction that wrote each
-- ref version, so a reader can ask which refs changed since it last read
-- them (ls-refs snapshots, aspen_core::meta::MetaStore::ref_changes),
-- SQLite dialect. Additive: it adds a column, an index and a trigger.
--
-- Keep this file and ../postgres/0010_ref_version_txid.sql in step, as for 0001.
-- crates/aspen-meta/SCHEMA.md explains the column.

-- SQLite runs one write transaction at a time (BEGIN IMMEDIATE), so a
-- version's own ID already orders writes against readers: every version a
-- reader did not see has an ID above the largest it saw. The column holds
-- that ID, set by the trigger below (a default cannot name another column),
-- so both dialects answer "changed since" with the same index. Rows written
-- before this migration keep 0: they are committed, and every reader sees
-- them.
ALTER TABLE ref_versions ADD COLUMN txid INTEGER NOT NULL DEFAULT 0;

-- A repository's versions by writing transaction.
CREATE INDEX ref_versions_repo_txid ON ref_versions (repo_id, txid);

CREATE TRIGGER ref_versions_txid AFTER INSERT ON ref_versions
BEGIN
    UPDATE ref_versions SET txid = NEW.id WHERE id = NEW.id;
END;
