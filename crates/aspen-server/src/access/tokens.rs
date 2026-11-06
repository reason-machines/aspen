//! API tokens in the metadata database (`tokens`, migration 0002), and
//! [`CachedTokens`], which keeps the rows requests read for a short TTL.

use std::sync::Arc;
use std::time::Duration;

use aspen_auth::token::{IssueError, issue};
use aspen_auth::{
    Grant, Grants, IssuedToken, NewToken, TokenDigest, TokenId, TokenRecord, TokenStore,
    TokenStoreError,
};
use aspen_core::error::StoreError;
use aspen_core::id::UnixMicros;
use aspen_meta::{SqlStore, TokenRow};

use super::cache::TtlCache;

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

/// A [`TokenStore`] that keeps each token row it reads for `ttl`
/// ([`super::cache`]), so a caller's requests within one TTL cost one
/// lookup between them. Clones share the cache.
///
/// What the cache may hold back is bounded by the TTL: a token revoked
/// elsewhere (another instance, `aspen token revoke`) is refused here
/// within `ttl` of its revocation; one revoked through
/// [`CachedTokens::revoke`] (or followed by [`CachedTokens::forget`]) on
/// this instance at once. Expiry needs no lookup: the cached record carries
/// `expires_at`, and [`aspen_auth::AuthService`] checks liveness against the
/// clock on every request. Unknown token IDs are not remembered, so a token
/// created elsewhere works on its first request.
#[derive(Debug, Clone)]
pub struct CachedTokens<T> {
    inner: T,
    rows: Arc<TtlCache<TokenId, TokenRecord>>,
}

impl<T> CachedTokens<T> {
    /// `inner`, with rows kept for `ttl` (zero keeps nothing).
    pub fn new(inner: T, ttl: Duration) -> Self {
        Self {
            inner,
            rows: Arc::new(TtlCache::new(ttl, super::cache::CAPACITY)),
        }
    }

    /// The store behind the cache.
    pub fn inner(&self) -> &T {
        &self.inner
    }

    /// Drops this instance's copy of token `id`: its next request reads
    /// the store. Call it after changing the token through any other path.
    pub fn forget(&self, id: &TokenId) {
        self.rows.remove(id);
    }
}

impl CachedTokens<SqlTokens> {
    /// Revokes a token ([`SqlTokens::revoke`]) and drops this instance's
    /// copy, so it is refused here from the next request.
    pub async fn revoke(&self, id: &TokenId, now: UnixMicros) -> Result<bool, StoreError> {
        let revoked = self.inner.revoke(id, now).await;
        // Even when the store reports a failure: the update may have
        // landed, and a lookup is cheaper than a stale grant.
        self.forget(id);
        revoked
    }
}

impl<T: TokenStore> TokenStore for CachedTokens<T> {
    async fn token(&self, id: &TokenId) -> Result<Option<TokenRecord>, TokenStoreError> {
        if let Some(record) = self.rows.get(id) {
            return Ok(Some(record));
        }
        let ticket = self.rows.ticket();
        let record = self.inner.token(id).await?;
        if let Some(record) = &record {
            self.rows.insert(*id, record.clone(), ticket);
        }
        Ok(record)
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
