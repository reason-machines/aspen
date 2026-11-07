-- Aspen metadata schema, migration 0011: generation numbers and root trees
-- in the commit index (decisions log, "12c: generation numbers"),
-- SQLite dialect. Additive: three nullable columns and two partial indexes.
--
-- Keep this file and ../postgres/0011_commit_generation.sql in step, as for 0001.
-- crates/aspen-meta/SCHEMA.md explains the columns. SQLite adds a column's
-- CHECK without reading existing rows.

-- 1 for a commit without parents, otherwise one more than the largest of
-- its parents' generations. NULL until known: rows written before this
-- migration, and rows whose parents had none yet when they were written.
-- The index_commits job fills them in.
ALTER TABLE commit_index ADD COLUMN generation INTEGER
    CONSTRAINT commit_index_generation_positive CHECK (generation IS NULL OR generation >= 1);
-- The commit's root tree, 20 bytes. NULL for rows written before this
-- migration.
ALTER TABLE commit_index ADD COLUMN root_tree BLOB
    CONSTRAINT commit_index_root_tree_len CHECK (root_tree IS NULL OR length(root_tree) = 20);
-- For a row without a generation that the backfill examined and could not
-- give one: the commit whose generation it waits on (a parent, or an
-- ancestor through parents that wait on it too). NULL for a row with a
-- generation, and for a row no backfill has examined yet.
ALTER TABLE commit_index ADD COLUMN generation_waits_on BLOB
    CONSTRAINT commit_index_generation_waits_on_len
    CHECK (generation_waits_on IS NULL OR length(generation_waits_on) = 20);

-- The rows the backfill has still to examine, in the order it pages them.
CREATE INDEX IF NOT EXISTS commit_index_unexamined
    ON commit_index (repo_id, ref_version_id, commit_oid)
    WHERE generation IS NULL AND generation_waits_on IS NULL;
-- The rows waiting on a commit, for when that commit gets its generation.
CREATE INDEX IF NOT EXISTS commit_index_waiting
    ON commit_index (repo_id, generation_waits_on)
    WHERE generation IS NULL AND generation_waits_on IS NOT NULL;
