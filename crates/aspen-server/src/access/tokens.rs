//! API tokens in the metadata database (`tokens`, migration 0002).

use aspen_auth::token::{IssueError, issue};
use aspen_auth::{
    Grant, Grants, IssuedToken, NewToken, TokenDigest, TokenId, TokenRecord, TokenStore,
    TokenStoreError,
};
use aspen_core::error::StoreError;
use aspen_core::id::UnixMicros;
use aspen_meta::{SqlStore, TokenRow};

/// A [`TokenStore`] on the metadata database.
#[derive(Debug, Clone)]
pub struct SqlTokens(pub SqlStore);

/// Why a token could not be created.
#[derive(Debug, thiserror::Error)]
pub enum CreateTokenError {
    /// The token itself could not be made.
    #[error(transparent)]
    Issue(#[from] IssueError),
    /// The store refused or failed.
    #[error("token store: {0}")]
    Store(#[from] StoreError),
}

impl SqlTokens {
    /// Creates and stores a token, and returns it with its plaintext, which
    /// is shown once and not kept.
    pub async fn create(
        &self,
        new: NewToken,
        now: UnixMicros,
    ) -> Result<IssuedToken, CreateTokenError> {
        let issued = issue(new, now)?;
        self.0.insert_token(&to_row(&issued.record)).await?;
        Ok(issued)
    }

    /// Revokes a token; `false` if there is no such token.
    pub async fn revoke(&self, id: &TokenId, now: UnixMicros) -> Result<bool, StoreError> {
        self.0.revoke_token(id.as_bytes(), now).await
    }
}

impl TokenStore for SqlTokens {
    async fn token(&self, id: &TokenId) -> Result<Option<TokenRecord>, TokenStoreError> {
        let row = self
            .0
            .token(id.as_bytes())
            .await
            .map_err(|e| TokenStoreError(e.to_string()))?;
        row.map(from_row).transpose()
    }
}

/// The row for a record.
pub fn to_row(r: &TokenRecord) -> TokenRow {
    TokenRow {
        id: *r.id.as_bytes(),
        secret_sha256: *r.digest.as_bytes(),
        subject: r.subject.clone(),
        grants: r
            .grants
            .as_slice()
            .iter()
            .map(Grant::to_string)
            .collect::<Vec<_>>()
            .join(" "),
        description: r.description.clone(),
        created_at: r.created_at,
        expires_at: r.expires_at,
        revoked_at: r.revoked_at,
    }
}

/// The record for a row. A grant that no longer parses fails the lookup
/// (so the request is retried, then investigated) rather than being
/// skipped, which could silently change what the token may do.
pub fn from_row(row: TokenRow) -> Result<TokenRecord, TokenStoreError> {
    let grants = row
        .grants
        .split_whitespace()
        .map(str::parse::<Grant>)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| TokenStoreError(format!("tokens.grants: {e}")))?;
    Ok(TokenRecord {
        id: TokenId::from_bytes(row.id),
        digest: TokenDigest::from_bytes(row.secret_sha256),
        subject: row.subject,
        grants: Grants::new(grants),
        description: row.description,
        created_at: row.created_at,
        expires_at: row.expires_at,
        revoked_at: row.revoked_at,
    })
}
