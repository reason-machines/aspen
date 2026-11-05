//! A server with the access middleware in front of a test handler.
#![allow(dead_code, unreachable_pub)]

use std::sync::{Arc, Mutex};

use aspen_auth::token::issue;
use aspen_auth::{
    AuthService, Grants, Limits, MemorySshKeyStore, MemoryTokenStore, NewToken, PolicyConfig,
};
use aspen_core::id::UnixMicros;
use aspen_core::meta::MetaStore;
use aspen_core::name::{RefName, RepoName};
use aspen_core::repo::RepoSettings;
use aspen_meta::SqlStore;
use aspen_server::access::{Access, ClientAddr, guard};
use axum::Router;

/// With an SSH key store, so tests can show that a registered key's
/// public fingerprint is never an HTTP credential.
pub type TestAccess = Access<SqlStore, MemoryTokenStore, MemorySshKeyStore>;

/// A metadata store on in-memory SQLite holding `repos`.
pub async fn meta_with(repos: &[&str]) -> SqlStore {
    let meta = SqlStore::connect("sqlite::memory:").await.unwrap();
    meta.migrate().await.unwrap();
    let settings = RepoSettings::new(RefName::new("refs/heads/main").unwrap()).unwrap();
    for r in repos {
        meta.create_repo(&RepoName::new(*r).unwrap(), &settings)
            .await
            .unwrap();
    }
    meta
}

pub fn access(
    policy: serde_json::Value,
    limits: aspen_auth::LimitsConfig,
    meta: SqlStore,
) -> Arc<TestAccess> {
    access_with(policy, limits, meta, ClientAddr::Peer)
}

/// [`access`], taking client addresses from `client_addr`.
pub fn access_with(
    policy: serde_json::Value,
    limits: aspen_auth::LimitsConfig,
    meta: SqlStore,
    client_addr: ClientAddr,
) -> Arc<TestAccess> {
    let policy: PolicyConfig = serde_json::from_value(policy).unwrap();
    let auth = AuthService::new(policy, MemoryTokenStore::new())
        .unwrap()
        .with_ssh_keys(MemorySshKeyStore::new());
    Arc::new(Access::new(auth, Limits::new(limits), meta, client_addr))
}

/// Stores a token with these grants and returns its text.
pub fn mint(access: &TestAccess, subject: &str, grants: &[&str]) -> String {
    let issued = issue(
        NewToken {
            subject: subject.into(),
            grants: Grants::new(grants.iter().map(|g| g.parse().unwrap())),
            description: String::new(),
            expires_at: None,
        },
        UnixMicros::now(),
    )
    .unwrap();
    access.auth().tokens().insert(issued.record);
    issued.token.expose().to_owned()
}

/// `inner` behind the access middleware, for every path.
pub fn app(access: Arc<TestAccess>, inner: Router) -> Router {
    inner.layer(axum::middleware::from_fn_with_state(
        access,
        guard::<SqlStore, MemoryTokenStore, _>,
    ))
}

/// What the test handler saw.
#[derive(Debug, Default)]
pub struct Seen {
    /// Requests that reached it: (repository, endpoint).
    pub calls: Vec<(String, String)>,
    /// Bodies it read to the end and "stored".
    pub stored: Vec<Vec<u8>>,
    /// Bodies whose read failed, with the error.
    pub failed: Vec<String>,
}

pub type Log = Arc<Mutex<Seen>>;

/// `Basic` credentials, encoded by hand to keep the test's
/// dependencies small.
pub fn basic(user: &str, password: &str) -> String {
    const T: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let input = format!("{user}:{password}");
    let mut out = String::new();
    for chunk in input.as_bytes().chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        for i in 0..4 {
            if i <= chunk.len() {
                out.push(T[((n >> (18 - 6 * i)) & 63) as usize] as char);
            } else {
                out.push('=');
            }
        }
    }
    format!("Basic {out}")
}
