//! How database errors become [`StoreError`]s.
//!
//! The store needs to know *which* constraint a statement broke: a
//! `ref_versions_seq_unique` or `refs_pkey` violation in a push is a lost
//! compare-and-swap, not an error, and `repos_name_unique` is a taken
//! name. Postgres names the constraint in every error. SQLite does not: it
//! reports a unique or primary-key violation by table and columns
//! (`UNIQUE constraint failed: ref_versions.repo_id, ref_versions.ref_name,
//! ref_versions.seq`), so [`constraint_name`] maps those column lists back
//! to schema v1's names. The store's tests drive each mapped constraint on
//! both dialects.

use std::borrow::Cow;

use aspen_core::error::StoreError;
use sqlx::error::{DatabaseError, ErrorKind};

/// SQLite's column lists for schema v1's unique constraints and indexes,
/// with the constraint name Postgres reports for the same violation.
/// Constraints on `(id, ...)` are left out: `id` is unique on its own, so
/// they cannot be violated without `<table>_pkey` being violated first.
const SQLITE_UNIQUE: &[(&str, &str)] = &[
    ("repos.name", "repos_name_unique"),
    ("tokens.id", "tokens_pkey"),
    ("ssh_keys.fingerprint", "ssh_keys_pkey"),
    ("mirrors.repo_id", "mirrors_pkey"),
    (
        "mirror_refs.repo_id, mirror_refs.ref_name",
        "mirror_refs_pkey",
    ),
    ("packs.repo_id, packs.checksum", "packs_checksum_unique"),
    (
        "ref_versions.repo_id, ref_versions.ref_name, ref_versions.seq",
        "ref_versions_seq_unique",
    ),
    ("refs.repo_id, refs.ref_name", "refs_pkey"),
    (
        "commit_index.repo_id, commit_index.commit_oid",
        "commit_index_pkey",
    ),
    (
        "covers.repo_id, covers.ref_name, covers.level, covers.first_seq",
        "covers_span_unique",
    ),
    ("jobs.kind, jobs.ref_version_id", "jobs_open_version"),
    ("jobs.kind, jobs.pack_id", "jobs_open_pack"),
    (
        "shallow_packs.version_id, shallow_packs.depth",
        "shallow_packs_pkey",
    ),
];

/// The schema v1 name of the constraint a database error violated, if it is
/// a constraint violation the store can attribute.
pub(crate) fn constraint_name(err: &dyn DatabaseError) -> Option<Cow<'_, str>> {
    if let Some(name) = err.constraint() {
        return Some(Cow::Borrowed(name));
    }
    // SQLite: CHECK constraints are named in the message
    // ("CHECK constraint failed: <name>"); unique and primary-key ones are
    // reported by column list.
    let message = err.message();
    if let Some(name) = message.strip_prefix("CHECK constraint failed: ") {
        return Some(Cow::Borrowed(name.trim()));
    }
    let columns = message.strip_prefix("UNIQUE constraint failed: ")?;
    SQLITE_UNIQUE
        .iter()
        .find(|(cols, _)| *cols == columns.trim())
        .map(|(_, name)| Cow::Borrowed(*name))
}

/// Whether `err` is a unique (or primary-key) violation of `constraint`.
pub(crate) fn is_unique_violation(err: &sqlx::Error, constraint: &str) -> bool {
    match err {
        sqlx::Error::Database(db) => {
            db.kind() == ErrorKind::UniqueViolation
                && constraint_name(db.as_ref()).as_deref() == Some(constraint)
        }
        _ => false,
    }
}

/// Whether a database error is transient: retrying the same statement or
/// transaction later can succeed.
fn is_transient(err: &dyn DatabaseError) -> bool {
    let Some(code) = err.code() else {
        return false;
    };
    if code.len() == 5 && code.chars().all(|c| c.is_ascii_alphanumeric()) {
        // A Postgres SQLSTATE. 40001 serialization failure, 40P01 deadlock,
        // 55P03 lock not available, 57P0x shutdown or crash, class 53
        // insufficient resources (too many connections), class 08
        // connection exceptions.
        return matches!(
            &*code,
            "40001" | "40P01" | "55P03" | "57014" | "57P01" | "57P02" | "57P03"
        ) || code.starts_with("53")
            || code.starts_with("08");
    }
    // A SQLite (extended) result code: the primary code is the low byte.
    // 5 SQLITE_BUSY, 6 SQLITE_LOCKED.
    code.parse::<u32>().is_ok_and(|c| matches!(c & 0xff, 5 | 6))
}

/// Maps a sqlx error to a [`StoreError`].
///
/// - A constraint violation is [`StoreError::Conflict`] naming the
///   constraint: the request broke a rule (a taken name, or a row the
///   caller should have refused before writing).
/// - Connection, pool and lock-contention failures are
///   [`StoreError::Unavailable`], the retryable kind.
/// - A value that does not decode is [`StoreError::Corrupt`]: the stored
///   row breaks schema v1.
pub(crate) fn store_error(err: sqlx::Error) -> StoreError {
    match &err {
        sqlx::Error::Database(db) => match db.kind() {
            ErrorKind::UniqueViolation
            | ErrorKind::ForeignKeyViolation
            | ErrorKind::NotNullViolation
            | ErrorKind::CheckViolation
            | ErrorKind::ExclusionViolation => StoreError::Conflict {
                what: match constraint_name(db.as_ref()) {
                    Some(name) => format!("violates {name}"),
                    None => db.message().to_owned(),
                },
            },
            _ if is_transient(db.as_ref()) => StoreError::Unavailable {
                reason: db.message().to_owned(),
            },
            _ => StoreError::Unavailable {
                reason: format!("database error: {}", db.message()),
            },
        },
        sqlx::Error::RowNotFound => StoreError::NotFound {
            what: "row".to_owned(),
        },
        sqlx::Error::ColumnDecode { .. }
        | sqlx::Error::Decode(_)
        | sqlx::Error::ColumnNotFound(_)
        | sqlx::Error::ColumnIndexOutOfBounds { .. }
        | sqlx::Error::TypeNotFound { .. } => StoreError::Corrupt {
            what: err.to_string(),
        },
        _ => StoreError::Unavailable {
            reason: err.to_string(),
        },
    }
}

/// A stored value that breaks schema v1's rules.
pub(crate) fn corrupt(what: impl Into<String>) -> StoreError {
    StoreError::Corrupt { what: what.into() }
}

/// A request the store refuses before writing (a bug in the caller).
pub(crate) fn conflict(what: impl Into<String>) -> StoreError {
    StoreError::Conflict { what: what.into() }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_sqlite_column_list_names_a_schema_constraint() {
        let pg = concat!(
            include_str!("../migrations/postgres/0001_schema_v1.sql"),
            include_str!("../migrations/postgres/0002_tokens.sql"),
            include_str!("../migrations/postgres/0003_mirrors.sql"),
            include_str!("../migrations/postgres/0004_ssh_keys.sql"),
            include_str!("../migrations/postgres/0005_compaction.sql"),
            include_str!("../migrations/postgres/0006_shallow_packs.sql"),
        );
        for (_, name) in SQLITE_UNIQUE {
            assert!(
                pg.contains(&format!("CONSTRAINT {name} "))
                    || pg.contains(&format!("INDEX {name} ")),
                "{name} is not in the schema"
            );
        }
    }
}
