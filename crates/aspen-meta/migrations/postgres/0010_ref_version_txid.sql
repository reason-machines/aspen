-- Aspen metadata schema, migration 0010: the transaction that wrote each
-- ref version, so a reader can ask which refs changed since it last read
-- them (ls-refs snapshots, aspen_core::meta::MetaStore::ref_changes),
-- PostgreSQL dialect. Additive: it adds a column and an index.
--
-- Keep this file and ../sqlite/0010_ref_version_txid.sql in step, as for 0001.
-- crates/aspen-meta/SCHEMA.md explains the column.

-- The 64-bit ID of the top-level transaction that inserted the row
-- (pg_current_xact_id(), the same under a savepoint). A reader that notes
-- its statement's snapshot (pg_current_snapshot()) knows that every version
-- it did not see was written by a transaction that snapshot lists as in
-- progress or that started after it, so "changed since" is an index range
-- plus a few point lookups, whatever order transactions commit in. Rows
-- written before this migration keep 0: they are committed, and every
-- snapshot sees them. Added with a constant default first, so existing rows
-- are not rewritten.
ALTER TABLE ref_versions ADD COLUMN txid BIGINT NOT NULL DEFAULT 0;
ALTER TABLE ref_versions ALTER COLUMN txid SET DEFAULT (pg_current_xact_id()::text::BIGINT);

-- A repository's versions by writing transaction.
CREATE INDEX ref_versions_repo_txid ON ref_versions (repo_id, txid);
