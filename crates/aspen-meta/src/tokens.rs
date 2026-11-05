//! API token rows (migration 0002, node B3).
//!
//! The store keeps what `aspen-auth` needs to check a token and nothing
//! more: the token's public ID, the SHA-256 digest of its secret, who it
//! acts as and its grants in their text form. The secret itself is never
//! stored. `aspen-meta` does not interpret grants; `aspen-server` turns rows
//! into `aspen_auth::TokenRecord`s and parses the grants there.
//!
//! Like the dead-job tools, these are inherent [`SqlStore`] methods rather
//! than `MetaStore` methods: only composition roots use them.

use aspen_core::error::StoreError;
use aspen_core::id::UnixMicros;
use sqlx::any::AnyRow;

use crate::SqlStore;
use crate::error::{conflict, corrupt, is_unique_violation, store_error};
use crate::rows::get;

/// One row of `tokens`.
///
/// Deliberately not comparable: an `==` on rows would compare
/// `secret_sha256` with a short-circuiting comparison. `aspen-auth` compares
/// digests in constant time.
#[derive(Clone)]
pub struct TokenRow {
    /// Public ID, 16 bytes.
    pub id: [u8; 16],
    /// SHA-256 digest of the secret.
    pub secret_sha256: [u8; 32],
    /// Who the token acts as (1 to 255 characters).
    pub subject: String,
    /// Grants, space-separated, in `aspen-auth`'s text form.
    pub grants: String,
    /// A note for operators.
    pub description: String,
    /// When it was created.
    pub created_at: UnixMicros,
    /// When it stops working, if ever; after `created_at`.
    pub expires_at: Option<UnixMicros>,
    /// When it was revoked, if it was.
    pub revoked_at: Option<UnixMicros>,
}

impl std::fmt::Debug for TokenRow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TokenRow")
            .field("id", &self.id)
            .field("secret_sha256", &"<sha256>")
            .field("subject", &self.subject)
            .field("grants", &self.grants)
            .field("created_at", &self.created_at)
            .field("expires_at", &self.expires_at)
            .field("revoked_at", &self.revoked_at)
            .finish_non_exhaustive()
    }
}

const COLS: &str = "id, secret_sha256, subject, grants, description, created_at_us, \
                    expires_at_us, revoked_at_us";

fn decode(row: &AnyRow) -> Result<TokenRow, StoreError> {
    let id: Vec<u8> = get(row, "id")?;
    let digest: Vec<u8> = get(row, "secret_sha256")?;
    Ok(TokenRow {
        id: id
            .try_into()
            .map_err(|_| corrupt("tokens.id: not 16 bytes"))?,
        secret_sha256: digest
            .try_into()
            .map_err(|_| corrupt("tokens.secret_sha256: not 32 bytes"))?,
        subject: get(row, "subject")?,
        grants: get(row, "grants")?,
        description: get(row, "description")?,
        created_at: UnixMicros(get(row, "created_at_us")?),
        expires_at: get::<Option<i64>>(row, "expires_at_us")?.map(UnixMicros),
        revoked_at: get::<Option<i64>>(row, "revoked_at_us")?.map(UnixMicros),
    })
}

impl SqlStore {
    /// Stores a new token. An ID that is already taken is
    /// [`StoreError::Conflict`].
    pub async fn insert_token(&self, token: &TokenRow) -> Result<(), StoreError> {
        let sql = format!("INSERT INTO tokens ({COLS}) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)");
        let result = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(token.id.to_vec())
            .bind(token.secret_sha256.to_vec())
            .bind(token.subject.clone())
            .bind(token.grants.clone())
            .bind(token.description.clone())
            .bind(token.created_at.0)
            .bind(token.expires_at.map(|t| t.0))
            .bind(token.revoked_at.map(|t| t.0))
            .execute(&mut *self.admit().await?)
            .await;
        match result {
            Ok(_) => Ok(()),
            Err(e) if is_unique_violation(&e, "tokens_pkey") => {
                Err(conflict("a token with this ID already exists"))
            }
            Err(e) => Err(store_error(e)),
        }
    }

    /// The token with this ID, revoked and expired ones included.
    pub async fn token(&self, id: &[u8; 16]) -> Result<Option<TokenRow>, StoreError> {
        let sql = format!("SELECT {COLS} FROM tokens WHERE id = $1");
        sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(id.to_vec())
            .fetch_optional(&mut *self.admit().await?)
            .await
            .map_err(store_error)?
            .as_ref()
            .map(decode)
            .transpose()
    }

    /// Every token, oldest first, at most `limit`.
    pub async fn tokens(&self, limit: u32) -> Result<Vec<TokenRow>, StoreError> {
        let sql = format!("SELECT {COLS} FROM tokens ORDER BY created_at_us, id LIMIT $1");
        sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(i64::from(limit))
            .fetch_all(&mut *self.admit().await?)
            .await
            .map_err(store_error)?
            .iter()
            .map(decode)
            .collect()
    }

    /// Revokes a token at `at`. A token that is already revoked keeps its
    /// first revocation time. Returns whether the token exists.
    pub async fn revoke_token(&self, id: &[u8; 16], at: UnixMicros) -> Result<bool, StoreError> {
        let updated = sqlx::query(
            "UPDATE tokens SET revoked_at_us = COALESCE(revoked_at_us, $2) WHERE id = $1",
        )
        .bind(id.to_vec())
        .bind(at.0)
        .execute(&mut *self.admit().await?)
        .await
        .map_err(store_error)?;
        Ok(updated.rows_affected() == 1)
    }
}
