//! SSH key rows (migration 0004, node C3).
//!
//! Like `tokens`, the store keeps what `aspen-auth` needs and interprets
//! nothing: the key's fingerprint (the SHA-256 of its wire encoding, the
//! primary key), the wire encoding itself, who the key acts as and its
//! grants in their text form. `aspen-server` turns rows into
//! `aspen_auth::SshKeyRecord`s, parsing the key and the grants there.
//!
//! These are inherent [`SqlStore`] methods rather than `MetaStore` methods:
//! only composition roots use them.

use aspen_core::error::StoreError;
use aspen_core::id::UnixMicros;
use sqlx::any::AnyRow;

use crate::SqlStore;
use crate::error::{conflict, corrupt, is_unique_violation, store_error};
use crate::rows::get;

/// One row of `ssh_keys`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SshKeyRow {
    /// SHA-256 of `public_key`, 32 bytes.
    pub fingerprint: [u8; 32],
    /// The key's wire encoding (1 to 1,024 bytes).
    pub public_key: Vec<u8>,
    /// Who the key acts as (1 to 255 characters).
    pub subject: String,
    /// Grants, space-separated, in `aspen-auth`'s text form.
    pub grants: String,
    /// A note for operators.
    pub description: String,
    /// When it was registered.
    pub created_at: UnixMicros,
    /// When it stops working, if ever; after `created_at`.
    pub expires_at: Option<UnixMicros>,
    /// When it was revoked, if it was.
    pub revoked_at: Option<UnixMicros>,
}

const COLS: &str = "fingerprint, public_key, subject, grants, description, created_at_us, \
                    expires_at_us, revoked_at_us";

fn decode(row: &AnyRow) -> Result<SshKeyRow, StoreError> {
    let fingerprint: Vec<u8> = get(row, "fingerprint")?;
    Ok(SshKeyRow {
        fingerprint: fingerprint
            .try_into()
            .map_err(|_| corrupt("ssh_keys.fingerprint: not 32 bytes"))?,
        public_key: get(row, "public_key")?,
        subject: get(row, "subject")?,
        grants: get(row, "grants")?,
        description: get(row, "description")?,
        created_at: UnixMicros(get(row, "created_at_us")?),
        expires_at: get::<Option<i64>>(row, "expires_at_us")?.map(UnixMicros),
        revoked_at: get::<Option<i64>>(row, "revoked_at_us")?.map(UnixMicros),
    })
}

impl SqlStore {
    /// Registers a key. A key that is already registered (revoked or not)
    /// is [`StoreError::Conflict`]: re-registering a revoked key would undo
    /// its revocation, so it needs a deliberate new key instead.
    pub async fn insert_ssh_key(&self, key: &SshKeyRow) -> Result<(), StoreError> {
        let sql = format!("INSERT INTO ssh_keys ({COLS}) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)");
        let result = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(key.fingerprint.to_vec())
            .bind(key.public_key.clone())
            .bind(key.subject.clone())
            .bind(key.grants.clone())
            .bind(key.description.clone())
            .bind(key.created_at.0)
            .bind(key.expires_at.map(|t| t.0))
            .bind(key.revoked_at.map(|t| t.0))
            .execute(&mut *self.admit().await?)
            .await;
        match result {
            Ok(_) => Ok(()),
            Err(e) if is_unique_violation(&e, "ssh_keys_pkey") => {
                Err(conflict("this SSH key is already registered"))
            }
            Err(e) => Err(store_error(e)),
        }
    }

    /// The key with this fingerprint, revoked and expired ones included.
    pub async fn ssh_key(&self, fingerprint: &[u8; 32]) -> Result<Option<SshKeyRow>, StoreError> {
        let sql = format!("SELECT {COLS} FROM ssh_keys WHERE fingerprint = $1");
        sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(fingerprint.to_vec())
            .fetch_optional(&mut *self.admit().await?)
            .await
            .map_err(store_error)?
            .as_ref()
            .map(decode)
            .transpose()
    }

    /// Every key, oldest first, at most `limit`.
    pub async fn ssh_keys(&self, limit: u32) -> Result<Vec<SshKeyRow>, StoreError> {
        let sql =
            format!("SELECT {COLS} FROM ssh_keys ORDER BY created_at_us, fingerprint LIMIT $1");
        sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(i64::from(limit))
            .fetch_all(&mut *self.admit().await?)
            .await
            .map_err(store_error)?
            .iter()
            .map(decode)
            .collect()
    }

    /// Revokes a key at `at`. A key that is already revoked keeps its first
    /// revocation time. Returns whether the key exists.
    pub async fn revoke_ssh_key(
        &self,
        fingerprint: &[u8; 32],
        at: UnixMicros,
    ) -> Result<bool, StoreError> {
        let updated = sqlx::query(
            "UPDATE ssh_keys SET revoked_at_us = COALESCE(revoked_at_us, $2) \
             WHERE fingerprint = $1",
        )
        .bind(fingerprint.to_vec())
        .bind(at.0)
        .execute(&mut *self.admit().await?)
        .await
        .map_err(store_error)?;
        Ok(updated.rows_affected() == 1)
    }
}
