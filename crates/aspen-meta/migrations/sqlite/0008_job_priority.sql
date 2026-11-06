-- Aspen metadata schema, migration 0008: job priority from fetch heat, so
-- compaction builds the covers fetches are waiting for first. SQLite
-- dialect. Additive: it adds a column and an index.
--
-- Keep this file and ../postgres/0008_job_priority.sql in step, as for 0001.
-- crates/aspen-meta/SCHEMA.md explains the column.

-- How much fetches have wanted this job done: the fetch path adds, per ref,
-- the number of versions it had to send as their own push packs because
-- the covers over them were not built yet
-- (aspen_core::meta::MetaStore::add_compaction_heat). A claim takes the due
-- job with the highest priority first, then the earliest due. Existing and
-- new jobs start at 0, which keeps oldest-first order among jobs no fetch
-- has asked for.
ALTER TABLE jobs ADD COLUMN priority INTEGER NOT NULL DEFAULT 0;

-- Claiming: the hottest due queued job, then the oldest (jobs_queued still
-- serves the oldest-due reads of the queue-lag gauge).
CREATE INDEX jobs_queued_priority ON jobs (priority DESC, run_after_us, id) WHERE state = 'queued';
