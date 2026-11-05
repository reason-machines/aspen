//! SSH keys in the metadata database (`ssh_keys`, migration 0004): key
//! registration for the SSH transport.

use aspen_auth::ssh::{RegisterError, register};
use aspen_auth::{
    Grant, Grants, NewSshKey, SshFingerprint, SshKeyRecord, SshKeyStore, SshKeyStoreError,
    SshPublicKey,
};
use aspen_core::error::StoreError;
use aspen_core::id::UnixMicros;
use aspen_meta::{SqlStore, SshKeyRow};

/// An [`SshKeyStore`] on the metadata database.
#[derive(Debug, Clone)]
pub struct SqlSshKeys(pub SqlStore);

/// Why a key could not be registered.
#[derive(Debug, thiserror::Error)]
pub enum RegisterKeyError {
    /// The registration itself is invalid (subject, expiry).
    #[error(transparent)]
    Invalid(#[from] RegisterError),
    /// The store refused it (the key is already registered,
    /// [`StoreError::Conflict`]) or failed.
    #[error("SSH key store: {0}")]
    Store(#[from] StoreError),
}

impl SqlSshKeys {
    /// Registers a key at `now` and returns its record. A key that is
    /// already registered, even a revoked one, is a conflict: revocation
    /// is final for that key.
    pub async fn register(
        &self,
        new: NewSshKey,
        now: UnixMicros,
    ) -> Result<SshKeyRecord, RegisterKeyError> {
        let record = register(new, now)?;
        self.0.insert_ssh_key(&to_row(&record)).await?;
        Ok(record)
    }

    /// Revokes a key; `false` if there is no such key.
    pub async fn revoke(
        &self,
        fingerprint: &SshFingerprint,
        now: UnixMicros,
    ) -> Result<bool, StoreError> {
        self.0.revoke_ssh_key(fingerprint.as_bytes(), now).await
    }
}

impl SshKeyStore for SqlSshKeys {
    async fn ssh_key(
        &self,
        fingerprint: &SshFingerprint,
    ) -> Result<Option<SshKeyRecord>, SshKeyStoreError> {
        let row = self
            .0
            .ssh_key(fingerprint.as_bytes())
            .await
            .map_err(|e| SshKeyStoreError(e.to_string()))?;
        row.map(from_row).transpose()
    }
}

/// The row for a record.
pub fn to_row(r: &SshKeyRecord) -> SshKeyRow {
    SshKeyRow {
        fingerprint: *r.fingerprint().as_bytes(),
        public_key: r.key.blob().to_vec(),
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

/// The record for a row. A key that no longer parses, a key that does not
/// hash to the row's fingerprint, or a grant that no longer parses fails
/// the lookup (so the request is retried, then investigated) rather than
/// being skipped or trusted.
pub fn from_row(row: SshKeyRow) -> Result<SshKeyRecord, SshKeyStoreError> {
    let key = SshPublicKey::from_blob(&row.public_key)
        .map_err(|e| SshKeyStoreError(format!("ssh_keys.public_key: {e}")))?;
    if key.fingerprint().as_bytes() != &row.fingerprint {
        return Err(SshKeyStoreError(
            "ssh_keys.public_key does not hash to its fingerprint".into(),
        ));
    }
    let grants = row
        .grants
        .split_whitespace()
        .map(str::parse::<Grant>)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| SshKeyStoreError(format!("ssh_keys.grants: {e}")))?;
    Ok(SshKeyRecord {
        key,
        subject: row.subject,
        grants: Grants::new(grants),
        description: row.description,
        created_at: row.created_at,
        expires_at: row.expires_at,
        revoked_at: row.revoked_at,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY: &str =
        "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAINfpeAnPZKwL5SOdrJPD2fsT9+r+8GZFy5xfTsLiuMxp";

    fn record() -> SshKeyRecord {
        register(
            NewSshKey {
                key: SshPublicKey::parse_openssh(KEY).unwrap(),
                subject: "user:alice".into(),
                grants: Grants::new([
                    "write:acme/*".parse().unwrap(),
                    "read:demo".parse().unwrap(),
                ]),
                description: "laptop".into(),
                expires_at: Some(UnixMicros(99)),
            },
            UnixMicros(10),
        )
        .unwrap()
    }

    #[test]
    fn a_record_round_trips_through_its_row() {
        let r = record();
        let back = from_row(to_row(&r)).unwrap();
        assert_eq!(back.key, r.key);
        assert_eq!(back.subject, r.subject);
        assert_eq!(back.grants.as_slice(), r.grants.as_slice());
        assert_eq!(
            (back.created_at, back.expires_at, back.revoked_at),
            (r.created_at, r.expires_at, r.revoked_at)
        );
    }

    /// Invariant: a row is trusted only for the key it holds. A row whose
    /// key does not hash to its fingerprint (a corrupted or hand-edited
    /// row), or whose key or grants no longer parse, fails the lookup
    /// instead of granting anything.
    #[test]
    fn a_row_that_does_not_match_its_key_is_refused() {
        let mut row = to_row(&record());
        row.fingerprint[0] ^= 1;
        assert!(from_row(row).is_err());
        let mut row = to_row(&record());
        row.public_key.push(0);
        assert!(from_row(row).is_err());
        let mut row = to_row(&record());
        row.grants = "write:acme/* sudo:everything".into();
        assert!(from_row(row).is_err());
    }
}
