-- Aspen metadata schema, migration 0007: last use of exact shallow packs
-- (issue #108), SQLite dialect. Additive: it adds a column and indexes.
--
-- Keep this file and ../postgres/0007_shallow_pack_use.sql in step, as for 0001.
-- crates/aspen-meta/SCHEMA.md explains the column.

-- When a fetch last used the recording, by the store's clock (its
-- recording counts as a use; aspen_core::meta::MetaStore::mark_shallow_pack_used).
-- Garbage collection evicts recordings unused for a configured time, and
-- set_shallow_pack evicts a repository's least recently used packs to stay
-- under its byte cap. The default only fills existing rows (just below) and
-- schema-level fixtures: a row left at 0 reads as long unused, so it is
-- evicted at the next pass rather than kept for good.
ALTER TABLE shallow_packs ADD COLUMN used_at_us INTEGER NOT NULL DEFAULT 0;
UPDATE shallow_packs SET used_at_us = recorded_at_us;

-- Eviction of unused recordings, least recently used first.
CREATE INDEX shallow_packs_used ON shallow_packs (used_at_us);
-- A repository's recordings, for its byte cap.
CREATE INDEX shallow_packs_repo ON shallow_packs (repo_id, used_at_us);
