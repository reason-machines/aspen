//! Aspen's access control.
//!
//! - [`token`]: API tokens (`aspen_<id>_<secret>`). Only the SHA-256 digest
//!   of a secret is stored, and digests are compared in constant time.
//! - [`jwt`]: OIDC identity tokens (GitHub Actions, GitLab CI, an IdP),
//!   verified against the issuer's published keys.
//! - [`grant`]: what a caller may do: `read`, `write` (optionally limited to
//!   ref patterns) or `admin`, on repository patterns.
//! - [`policy`]: [`AuthService`] turns a credential into an [`Identity`]
//!   once per request, and [`Identity::authorize`] makes the access decision
//!   from it, before the server looks the repository up.
//! - [`limit`]: per-caller request rates and push quotas.
//! - [`ssh`]: SSH public keys registered with their own grants, found by
//!   fingerprint once the SSH transport has verified the client holds the
//!   key (node C3).
//! - [`credential`]: reading the HTTP `Authorization` header.
//!
//! The crate does no I/O of its own: tokens and SSH keys live behind the
//! [`TokenStore`] and [`SshKeyStore`] traits (in memory here, in the
//! metadata database through the server), and the HTTP and SSH
//! integrations are `aspen-server`'s `access` and `ssh` modules. It depends
//! on `aspen-core` only.

#![deny(missing_docs)]

pub mod credential;
pub mod grant;
pub mod jwt;
pub mod limit;
pub mod policy;
pub mod ssh;
pub mod token;

pub use grant::{Grant, Grants, Permission, RepoPattern};
pub use limit::{
    ConcurrentLimits, CredentialCharge, LimitError, Limits, LimitsConfig, PushQuota, RateLimit,
    RequestClass,
};
pub use policy::{AuthService, Identity, Need, PolicyConfig, Via};
pub use ssh::{
    MemorySshKeyStore, NewSshKey, NoSshKeys, SshFingerprint, SshKeyRecord, SshKeyStore,
    SshKeyStoreError, SshPublicKey,
};
pub use token::{
    IssuedToken, MemoryTokenStore, NewToken, TokenDigest, TokenId, TokenRecord, TokenStore,
    TokenStoreError,
};
