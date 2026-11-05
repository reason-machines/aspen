-- Aspen metadata schema, migration 0002: API tokens (node B3), PostgreSQL
-- dialect. Additive: it adds one table and changes nothing in schema v1.
--
-- Keep this file and ../sqlite/0002_tokens.sql in step, as for 0001.
-- A token's secret is never stored: only its SHA-256 digest
-- (aspen-auth's token module explains the format and the check).

CREATE TABLE tokens (
    id              BYTEA NOT NULL
                    CONSTRAINT tokens_pkey PRIMARY KEY,
    secret_sha256   BYTEA NOT NULL,
    subject         TEXT NOT NULL,
    grants          TEXT NOT NULL,
    description     TEXT NOT NULL DEFAULT '',
    created_at_us   BIGINT NOT NULL,
    expires_at_us   BIGINT,
    revoked_at_us   BIGINT,
    CONSTRAINT tokens_id_length CHECK (length(id) = 16),
    CONSTRAINT tokens_digest_length CHECK (length(secret_sha256) = 32),
    CONSTRAINT tokens_subject_length CHECK (length(subject) BETWEEN 1 AND 255),
    CONSTRAINT tokens_expiry_after_creation
        CHECK (expires_at_us IS NULL OR expires_at_us > created_at_us),
    CONSTRAINT tokens_revoked_after_creation
        CHECK (revoked_at_us IS NULL OR revoked_at_us >= created_at_us)
);
