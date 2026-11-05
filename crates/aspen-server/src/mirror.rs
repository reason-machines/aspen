//! Mirror mode's wiring (node C4): `aspen-mirror`'s service over this
//! server's stores, push path and fetch planner.
//!
//! - [`SqlMirrors`] keeps mirror settings and the sync ledger in the
//!   metadata database (`aspen-meta`, migration 0003).
//! - [`LocalAspen`] is what the service needs from Aspen itself: applying
//!   an upstream's ref update through the push path
//!   ([`crate::receive::ReceivePack::import`]) and building a pack for the
//!   upstream with the fetch planner and stitcher.
//! - [`Server::with_mirror`] builds the service and hooks it into the push
//!   path, so proxy-mode pushes are forwarded and async-mode pushes enqueue
//!   `mirror_push` jobs. Run [`aspen_mirror::Mirror::run`] beside the server
//!   for the jobs and the periodic sync passes.

use std::path::Path;
use std::sync::{Arc, Weak};

use aspen_core::blob::BlobStore;
use aspen_core::error::StoreError;
use aspen_core::id::{ObjectId, RepoId, UnixMicros};
use aspen_core::meta::MetaStore;
use aspen_core::name::{RefName, RepoName};
use aspen_core::repo::{Repo, RepoSettings};
use aspen_meta::{ConflictRow, MirrorRefState, MirrorRow, SqlStore};
use aspen_mirror::{
    Applied, Conflict, CredentialName, Credentials, Forwarded, LedgerEntry, LocalRepo, Metrics,
    Mirror, MirrorConfig, MirrorMode, MirrorRecord, MirrorStore, RefUpdate, UpstreamUrl,
};

use crate::receive::{BoxFuture, PushThrough, ServiceError};
use crate::{Inner, Server};

/// The mirror service as a server runs it.
pub type ServerMirror<M, B, S> = Mirror<M, S, LocalAspen<M, B>>;

/// A server with mirror mode, and the mirror service to run beside it.
pub type WithMirror<M, B, S> = (Server<M, B>, Arc<ServerMirror<M, B, S>>);

/// [`MirrorStore`] on the metadata database.
#[derive(Debug, Clone)]
pub struct SqlMirrors(pub SqlStore);

fn corrupt(what: String) -> StoreError {
    StoreError::Corrupt { what }
}

fn record_of(row: MirrorRow) -> Result<MirrorRecord, StoreError> {
    let credential = if row.credential.is_empty() {
        None
    } else {
        Some(
            CredentialName::new(&row.credential)
                .map_err(|e| corrupt(format!("mirrors.credential: {e}")))?,
        )
    };
    Ok(MirrorRecord {
        repo: row.repo,
        upstream: UpstreamUrl::parse(&row.upstream_url)
            .map_err(|e| corrupt(format!("mirrors.upstream_url: {e}")))?,
        mode: row
            .mode
            .parse()
            .map_err(|e| corrupt(format!("mirrors.mode: {e}")))?,
        credential,
        created_at: row.created_at,
        last_sync_at: row.last_sync_at,
        last_error: row.last_error,
    })
}

fn entry_of(state: MirrorRefState) -> LedgerEntry {
    LedgerEntry {
        synced: state.synced,
        conflict: state.conflict.map(|c| Conflict {
            at: c.at,
            aspen: c.aspen,
            upstream: c.upstream,
        }),
    }
}

impl MirrorStore for SqlMirrors {
    async fn create_mirror(
        &self,
        name: &RepoName,
        settings: &RepoSettings,
        record: impl FnOnce(RepoId) -> MirrorRecord + Send,
    ) -> Result<Repo, StoreError> {
        self.0
            .create_mirror_repo(name, settings, |repo| {
                let r = record(repo);
                MirrorRow {
                    repo: r.repo,
                    upstream_url: r.upstream.as_str().to_owned(),
                    mode: r.mode.as_str().to_owned(),
                    credential: r
                        .credential
                        .as_ref()
                        .map(|c| c.as_str().to_owned())
                        .unwrap_or_default(),
                    created_at: r.created_at,
                    last_sync_at: r.last_sync_at,
                    last_error: r.last_error,
                }
            })
            .await
    }

    async fn mirror(&self, repo: RepoId) -> Result<Option<MirrorRecord>, StoreError> {
        self.0.mirror(repo).await?.map(record_of).transpose()
    }

    async fn mirrors(&self) -> Result<Vec<MirrorRecord>, StoreError> {
        self.0.mirrors().await?.into_iter().map(record_of).collect()
    }

    async fn delete_mirror(&self, repo: RepoId) -> Result<bool, StoreError> {
        self.0.delete_mirror(repo).await
    }

    async fn record_sync(
        &self,
        repo: RepoId,
        at: UnixMicros,
        error: Option<&str>,
    ) -> Result<(), StoreError> {
        self.0.record_mirror_sync(repo, at, error).await
    }

    async fn ledger(&self, repo: RepoId) -> Result<Vec<(RefName, LedgerEntry)>, StoreError> {
        Ok(self
            .0
            .mirror_refs(repo)
            .await?
            .into_iter()
            .map(|r| (r.name, entry_of(r.state)))
            .collect())
    }

    async fn ledger_entry(&self, repo: RepoId, name: &RefName) -> Result<LedgerEntry, StoreError> {
        Ok(entry_of(self.0.mirror_ref(repo, name).await?))
    }

    async fn set_ledger(
        &self,
        repo: RepoId,
        name: &RefName,
        expected: Option<ObjectId>,
        next: LedgerEntry,
        at: UnixMicros,
    ) -> Result<bool, StoreError> {
        let state = MirrorRefState {
            synced: next.synced,
            conflict: next.conflict.map(|c| ConflictRow {
                at: c.at,
                aspen: c.aspen,
                upstream: c.upstream,
            }),
        };
        self.0.set_mirror_ref(repo, name, expected, state, at).await
    }

    async fn conflict_count(&self) -> Result<u64, StoreError> {
        self.0.mirror_conflict_count().await
    }
}

/// Aspen's side of mirror mode, over a server. Holds the server weakly:
/// the server's push path holds the mirror service, which holds this.
#[derive(Debug)]
pub struct LocalAspen<M, B> {
    server: Weak<Inner<M, B>>,
}

impl<M, B> LocalAspen<M, B> {
    fn server(&self) -> Result<Server<M, B>, StoreError> {
        self.server
            .upgrade()
            .map(|inner| Server { inner })
            .ok_or_else(|| StoreError::Unavailable {
                reason: "the server is shutting down".into(),
            })
    }
}

impl<M, B> LocalRepo for LocalAspen<M, B>
where
    M: MetaStore + Clone + 'static,
    B: BlobStore + Clone + 'static,
{
    async fn apply(
        &self,
        repo: &Repo,
        update: &RefUpdate,
        pack: Option<&Path>,
    ) -> Result<Applied, StoreError> {
        let server = self.server()?;
        let svc = server
            .receive_pack()
            .ok_or_else(|| StoreError::Unavailable {
                reason: "mirror mode needs the receive-pack service".into(),
            })?;
        svc.import(repo, update, pack)
            .await
            .map_err(|ServiceError::Store(e)| e)
    }

    async fn write_pack(
        &self,
        repo: &Repo,
        want: ObjectId,
        haves: &[ObjectId],
        out: &Path,
    ) -> Result<(), StoreError> {
        let server = self.server()?;
        crate::upload_pack::to_file::write_pack(&server, repo, &[want], haves, out)
            .await
            .map(|_| ())
    }
}

/// The push path's hook, over the mirror service.
struct Hook<M, B, S> {
    mirror: Arc<ServerMirror<M, B, S>>,
}

impl<M, B, S> std::fmt::Debug for Hook<M, B, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("mirror::Hook")
    }
}

impl<M, B, S> PushThrough for Hook<M, B, S>
where
    M: MetaStore + Clone + 'static,
    B: BlobStore + Clone + 'static,
    S: MirrorStore + 'static,
{
    fn mode<'a>(&'a self, repo: &'a Repo) -> BoxFuture<'a, Result<Option<MirrorMode>, StoreError>> {
        Box::pin(async move { Ok(self.mirror.store().mirror(repo.id).await?.map(|m| m.mode)) })
    }

    fn forward<'a>(
        &'a self,
        repo: &'a Repo,
        updates: &'a [RefUpdate],
        pack: Option<&'a Path>,
        atomic: bool,
    ) -> BoxFuture<'a, Vec<Forwarded>> {
        Box::pin(self.mirror.forward(repo, updates, pack, atomic))
    }

    fn committed(&self, repo: &Repo, mode: MirrorMode, lagging: bool) {
        match mode {
            MirrorMode::Async => self.mirror.wake(),
            MirrorMode::Proxy if lagging => self.mirror.request_sync(repo.id),
            MirrorMode::Proxy => {}
        }
    }
}

/// Why [`Server::with_mirror`] failed.
#[derive(Debug, thiserror::Error)]
pub enum MirrorSetupError {
    /// Mirror mode writes refs through the push path.
    #[error("mirror mode needs the receive-pack service (Server::with_receive_pack first)")]
    NoReceivePack,
    /// The service could not start.
    #[error("{0}")]
    Service(String),
}

impl<M, B> Server<M, B>
where
    M: MetaStore + Clone + 'static,
    B: BlobStore + Clone + 'static,
{
    /// Adds mirror mode over `store`, with `credentials` and `config`:
    /// hooks it into the push path and adds its counters to `/-/metrics`.
    /// Returns the service; spawn [`Mirror::run`] on it to run the
    /// `mirror_push` jobs and the sync passes. Call after
    /// [`Server::with_receive_pack`] and before the server is shared.
    pub fn with_mirror<S: MirrorStore + 'static>(
        mut self,
        store: S,
        credentials: Credentials,
        config: MirrorConfig,
    ) -> Result<WithMirror<M, B, S>, MirrorSetupError> {
        let metrics = Arc::new(Metrics::default());
        let inner = Arc::get_mut(&mut self.inner)
            .expect("with_mirror is called before the server is shared");
        inner.mirror_metrics = Some(Arc::clone(&metrics));
        let receive = inner
            .receive
            .clone()
            .ok_or(MirrorSetupError::NoReceivePack)?;
        let local = LocalAspen {
            server: Arc::downgrade(&self.inner),
        };
        let mirror = Arc::new(
            Mirror::new(
                self.inner.meta.clone(),
                store,
                local,
                credentials,
                config,
                metrics,
            )
            .map_err(MirrorSetupError::Service)?,
        );
        receive
            .set_push_through(Arc::new(Hook {
                mirror: Arc::clone(&mirror),
            }))
            .map_err(|_| MirrorSetupError::Service("mirror mode is already set up".into()))?;
        Ok((self, mirror))
    }
}
