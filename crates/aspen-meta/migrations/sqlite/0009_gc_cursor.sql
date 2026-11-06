-- Aspen metadata schema, migration 0009: incremental garbage collection
-- (throughput item 7), SQLite dialect. Additive: it adds an index and two
-- tables and changes no earlier table.
--
-- Keep this file and ../postgres/0009_gc_cursor.sql in step, as for 0001.
-- crates/aspen-meta/SCHEMA.md ("Garbage collection") explains the cursor
-- and the lease.

-- The sweep reads packs in the order their grace periods end: by last
-- registration time, then ID. A pass resumes after the last pack it
-- examined, so it reads each registration once, not every pack every pass.
CREATE INDEX pack_touches_due ON pack_touches (touched_at_us, pack_id);

-- Where the sweep resumes. touched_at_us and pack_id are the key of the
-- last pack_touches row the sweep examined (the watermark); audit_pack_id
-- is the last packs row its slow audit pass examined (packs without a
-- registration time, and packs the watermark passed that are orphans now).
CREATE TABLE gc_cursors (
    name            TEXT NOT NULL
                    CONSTRAINT gc_cursors_pkey PRIMARY KEY,
    touched_at_us   INTEGER NOT NULL,
    pack_id         INTEGER NOT NULL,
    audit_pack_id   INTEGER NOT NULL
) STRICT;

-- Time-limited leases on work only one process may do at a time (the
-- sweep). epoch grows by one each time the lease changes hands or is taken
-- again after it expired; work done under a lease checks owner and epoch
-- in its own transaction, so a holder that stalled past its expiry cannot
-- act after another process took the lease.
CREATE TABLE leases (
    name            TEXT NOT NULL
                    CONSTRAINT leases_pkey PRIMARY KEY,
    owner           TEXT NOT NULL,
    epoch           INTEGER NOT NULL,
    expires_at_us   INTEGER NOT NULL
) STRICT;
