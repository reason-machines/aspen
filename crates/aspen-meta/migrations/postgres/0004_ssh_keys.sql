-- Aspen metadata schema, migration 0004: SSH keys (node C3), PostgreSQL
-- dialect. Additive: it adds one table and changes nothing in schema v1 or
-- in earlier migrations.
--
-- Keep this file and ../sqlite/0004_ssh_keys.sql in step, as for 0001.
-- A key is stored by its fingerprint, the SHA-256 of its wire encoding,
-- which is how the SSH transport finds it once the client has proved it
-- holds the private key (aspen-auth's ssh module explains the format).

CREATE TABLE ssh_keys (
    fingerprint     BYTEA NOT NULL
                    CONSTRAINT ssh_keys_pkey PRIMARY KEY,
    public_key      BYTEA NOT NULL,
    subject         TEXT NOT NULL,
    grants          TEXT NOT NULL,
    description     TEXT NOT NULL DEFAULT '',
    created_at_us   BIGINT NOT NULL,
    expires_at_us   BIGINT,
    revoked_at_us   BIGINT,
    CONSTRAINT ssh_keys_fingerprint_length CHECK (length(fingerprint) = 32),
    CONSTRAINT ssh_keys_key_length CHECK (length(public_key) BETWEEN 1 AND 1024),
    CONSTRAINT ssh_keys_subject_length CHECK (length(subject) BETWEEN 1 AND 255),
    CONSTRAINT ssh_keys_expiry_after_creation
        CHECK (expires_at_us IS NULL OR expires_at_us > created_at_us),
    CONSTRAINT ssh_keys_revoked_after_creation
        CHECK (revoked_at_us IS NULL OR revoked_at_us >= created_at_us)
);
