//! The mirror service: imports, sync passes, proxy forwarding, the
//! background push job and conflict resolution.
//!
//! A **sync pass** over one mirror lists the upstream's branches and tags
//! (`ls-refs`), Aspen's, and the ledger, and takes one step per ref that
//! differs ([`crate::reconcile::decide`]): trunk first, then the other
//! refs a few at a time. An **import** fetches what the upstream's value
//! reaches beyond Aspen's trunk and the ref's current value (the fetch's
//! haves) and pushes it into Aspen through the ordinary push path
//! ([`LocalRepo::apply`]), so every invariant of stored packs and ref
//! versions holds for imported history exactly as for pushed history. A
//! **push** (async mode) asks Aspen for a pack of what the ref's value
//! reaches beyond the upstream's ref and trunk ([`LocalRepo::write_pack`]),
//! and sends it with the upstream's value as the old value.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use aspen_core::error::StoreError;
use aspen_core::id::{JobId, ObjectId, RefVersionId, RepoId, UnixMicros};
use aspen_core::meta::{JobEvent, JobKind, JobTag, MetaStore, RefListQuery};
use aspen_core::name::{RefName, RepoName};
use aspen_core::repo::{Repo, RepoSettings};
use futures_util::StreamExt as _;
use tokio::sync::{Notify, OnceCell};

use crate::credential::{CredentialName, Credentials};
use crate::metrics::{Forwarded, Metrics};
use crate::reconcile::{Action, Side, decide};
use crate::store::{Conflict, LedgerEntry, MirrorMode, MirrorRecord, MirrorStore};
use crate::upstream::{HttpOptions, Upstream, UpstreamError, UpstreamUrl, http_client};
use crate::wire::{ListedRefs, MIRRORED_PREFIXES, PushAdvertisement};
use crate::{Applied, LocalRepo, RefUpdate};

/// Settings of the mirror service. [`MirrorConfig::new`] gives defaults.
#[derive(Debug, Clone)]
pub struct MirrorConfig {
    /// Where packs fetched from upstreams, and packs pushed to them, are
    /// spooled. A real disk, sized like the push spool.
    pub work_dir: std::path::PathBuf,
    /// How often each mirror gets a sync pass. Proxy-mode mirrors serve
    /// fetches that are this stale at most for changes made at the
    /// upstream directly.
    pub sync_interval: Duration,
    /// How often the worker looks for `mirror_push` jobs when nothing woke
    /// it.
    pub job_poll: Duration,
    /// The lease of a claimed job. A step that outlives it may run twice;
    /// both runs are compare-and-swaps, so the second one fails harmlessly.
    pub job_lease: Duration,
    /// Delay before a failed job runs again.
    pub retry_backoff: Duration,
    /// Largest pack fetched from an upstream in one step.
    pub max_pack_bytes: u64,
    /// Refs synced at once after trunk.
    pub parallel_refs: usize,
    /// HTTP timeouts.
    pub http: HttpOptions,
}

impl MirrorConfig {
    /// Defaults: a 60-second sync interval, 5-second job polling, 15-minute
    /// leases, 30-second retry backoff, 8 GiB packs and 4 refs at once.
    pub fn new(work_dir: impl Into<std::path::PathBuf>) -> Self {
        Self {
            work_dir: work_dir.into(),
            sync_interval: Duration::from_secs(60),
            job_poll: Duration::from_secs(5),
            job_lease: Duration::from_secs(15 * 60),
            retry_backoff: Duration::from_secs(30),
            max_pack_bytes: 8 << 30,
            parallel_refs: 4,
            http: HttpOptions::default(),
        }
    }
}

/// Why a mirror operation failed.
#[derive(Debug, thiserror::Error)]
pub enum MirrorError {
    /// A store failed.
    #[error("store: {0}")]
    Store(#[from] StoreError),
    /// The upstream could not be used.
    #[error(transparent)]
    Upstream(#[from] UpstreamError),
    /// The repository is not a mirror.
    #[error("repository {0} is not a mirror")]
    NotAMirror(String),
    /// The request does not make sense in the current state.
    #[error("{0}")]
    Refused(String),
    /// A ref step failed; the reason.
    #[error("{name}: {reason}")]
    Step {
        /// The ref.
        name: RefName,
        /// Why.
        reason: String,
    },
}

/// What happened to one ref in a pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RefOutcome {
    /// Both sides already agree.
    InSync,
    /// The upstream's value was imported.
    Imported,
    /// Aspen's value was pushed to the upstream.
    Pushed,
    /// Both sides moved; the conflict is recorded.
    Conflict(Conflict),
    /// The step failed; the next pass tries again.
    Failed(String),
}

/// The result of one sync pass.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SyncReport {
    /// Every ref looked at, with what happened.
    pub refs: BTreeMap<RefName, RefOutcome>,
    /// Upstream refs Aspen cannot store (bad names) or that are outside
    /// the repository's writable namespaces; never mirrored.
    pub skipped: Vec<String>,
}

impl SyncReport {
    /// How many refs ended with `pred`.
    pub fn count(&self, pred: impl Fn(&RefOutcome) -> bool) -> usize {
        self.refs.values().filter(|o| pred(o)).count()
    }

    /// The refs whose step failed, with why.
    pub fn failures(&self) -> Vec<(RefName, String)> {
        self.refs
            .iter()
            .filter_map(|(n, o)| match o {
                RefOutcome::Failed(r) => Some((n.clone(), r.clone())),
                _ => None,
            })
            .collect()
    }
}

/// The mirror service over Aspen's metadata store `M`, the mirror store
/// `S` and the local repository operations `L`.
#[derive(Debug)]
pub struct Mirror<M, S, L> {
    meta: M,
    store: S,
    local: L,
    credentials: Credentials,
    http: reqwest::Client,
    config: MirrorConfig,
    metrics: Arc<Metrics>,
    wake: Notify,
    /// Repositories whose next pass should run now, not at its interval.
    requested: Mutex<HashSet<RepoId>>,
}

/// The upstream and the snapshots one pass works from.
struct Pass<'a> {
    repo: &'a Repo,
    record: &'a MirrorRecord,
    upstream: Upstream,
    listed: ListedRefs,
    advert: OnceCell<PushAdvertisement>,
}

fn now() -> UnixMicros {
    UnixMicros::now()
}

/// First wait before recording a job's outcome again after a transient
/// store error; it doubles up to [`OUTCOME_RETRY_MAX`].
const OUTCOME_RETRY_FIRST: Duration = Duration::from_millis(50);
/// Longest wait between attempts to record a job's outcome.
const OUTCOME_RETRY_MAX: Duration = Duration::from_secs(5);

/// A worker of `mirror_push` jobs ([`Mirror::run_one_job`]): its name in
/// job leases, and whether a claim it never got the answer to may still
/// hold a job under that name.
///
/// One worker runs one job at a time: [`Mirror::run_one_job`] takes it by
/// `&mut`, and it cannot be cloned, so giving back the jobs leased under
/// its name never takes back a job another loop of the same worker is
/// running.
///
/// That holds only while no other live worker shares the name. After a
/// claim that returned an error, the worker gives back every `mirror_push`
/// job leased under its name, so two workers with one name, in one process
/// or in two processes on the same metadata store, can take back each
/// other's running job. Nothing checks this: give every worker its own name
/// ([`JobWorker::new`]).
#[derive(Debug)]
pub struct JobWorker {
    name: String,
    /// Set when a claim failed: a `mirror_push` job leased under this name
    /// may exist that the worker never received. Cleared once given back.
    release_pending: bool,
}

impl JobWorker {
    /// A worker named `name` in job leases. The name must be unique among
    /// the live workers of `mirror_push` jobs on the same metadata store,
    /// within a process and across processes (`aspen serve` uses host name,
    /// process ID and a random suffix); see [`JobWorker`] for what goes wrong
    /// otherwise. A compaction worker may share it: only `mirror_push` jobs
    /// are given back under it.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            release_pending: false,
        }
    }
}

impl<M, S, L> Mirror<M, S, L>
where
    M: MetaStore + 'static,
    S: MirrorStore + 'static,
    L: LocalRepo + 'static,
{
    /// The service. Creates the work directory.
    pub fn new(
        meta: M,
        store: S,
        local: L,
        credentials: Credentials,
        config: MirrorConfig,
        metrics: Arc<Metrics>,
    ) -> Result<Self, String> {
        std::fs::create_dir_all(&config.work_dir)
            .map_err(|e| format!("mirror work directory {}: {e}", config.work_dir.display()))?;
        Ok(Self {
            http: http_client(config.http)?,
            meta,
            store,
            local,
            credentials,
            config,
            metrics,
            wake: Notify::new(),
            requested: Mutex::new(HashSet::new()),
        })
    }

    /// The counters.
    pub fn metrics(&self) -> &Arc<Metrics> {
        &self.metrics
    }

    /// The mirror store.
    pub fn store(&self) -> &S {
        &self.store
    }

    /// The configured credentials.
    pub fn credentials(&self) -> &Credentials {
        &self.credentials
    }

    /// Asks the worker to sync `repo` now.
    pub fn request_sync(&self, repo: RepoId) {
        self.requested.lock().expect("not poisoned").insert(repo);
        self.wake.notify_one();
    }

    /// Wakes the worker to look for jobs now.
    pub fn wake(&self) {
        self.wake.notify_one();
    }

    async fn upstream(
        &self,
        url: &UpstreamUrl,
        credential: Option<&CredentialName>,
    ) -> Result<Upstream, MirrorError> {
        let auth = self
            .credentials
            .authorization(&self.http, credential, url)
            .await
            .map_err(UpstreamError::from)?;
        Ok(Upstream::new(
            self.http.clone(),
            url.clone(),
            auth,
            self.config.http,
        )?)
    }

    /// Creates repository `name` as a mirror of `upstream` and runs the
    /// first sync pass, which imports every branch and tag. Trunk is the
    /// branch the upstream's `HEAD` points at (`refs/heads/main` for an
    /// empty upstream). The repository and its mirror row are written in
    /// one transaction before the pass ([`MirrorStore::create_mirror`]), so
    /// a failed create leaves no repository behind, and a pass that fails
    /// part-way is finished by the next one.
    pub async fn create(
        &self,
        name: &RepoName,
        upstream: UpstreamUrl,
        mode: MirrorMode,
        credential: Option<CredentialName>,
    ) -> Result<(Repo, SyncReport), MirrorError> {
        if let Some(c) = &credential
            && !self.credentials.contains(c)
        {
            return Err(MirrorError::Refused(format!(
                "credential {c:?} is not configured (docs/MIRROR.md)"
            )));
        }
        let up = self.upstream(&upstream, credential.as_ref()).await?;
        let listed = up.ls_refs().await?;
        let trunk = listed
            .head
            .clone()
            .unwrap_or_else(|| RefName::new("refs/heads/main").expect("valid"));
        let settings = RepoSettings::new(trunk).map_err(|e| MirrorError::Refused(e.to_string()))?;
        let repo = self
            .store
            .create_mirror(name, &settings, |repo| MirrorRecord {
                repo,
                upstream,
                mode,
                credential,
                created_at: now(),
                last_sync_at: None,
                last_error: None,
            })
            .await?;
        let report = self.sync(&repo).await?;
        Ok((repo, report))
    }

    /// Runs one sync pass over `repo`, and records its end. A pass whose
    /// refs failed individually still ends `Ok`, with the failures in the
    /// report (and as the mirror's last error).
    pub async fn sync(&self, repo: &Repo) -> Result<SyncReport, MirrorError> {
        let record = self
            .store
            .mirror(repo.id)
            .await?
            .ok_or_else(|| MirrorError::NotAMirror(repo.name.to_string()))?;
        let result = self.pass(repo, &record, None).await;
        let error = match &result {
            Ok(r) => r
                .failures()
                .first()
                .map(|(n, why)| format!("{} ref(s) failed; first {n}: {why}", r.failures().len())),
            Err(e) => Some(e.to_string()),
        };
        self.metrics.pass(error.is_none());
        self.store
            .record_sync(repo.id, now(), error.as_deref())
            .await?;
        self.refresh_conflicts().await;
        result
    }

    async fn refresh_conflicts(&self) {
        if let Ok(n) = self.store.conflict_count().await {
            self.metrics.set_open_conflicts(n);
        }
    }

    /// One pass over every mirrored ref, or over `only` alone.
    async fn pass(
        &self,
        repo: &Repo,
        record: &MirrorRecord,
        only: Option<&RefName>,
    ) -> Result<SyncReport, MirrorError> {
        let upstream = self
            .upstream(&record.upstream, record.credential.as_ref())
            .await?;
        let listed = upstream.ls_refs().await?;
        let trunk = repo.settings.trunk().clone();
        let mut report = SyncReport {
            skipped: listed.unstorable.clone(),
            ..SyncReport::default()
        };
        let aspen = self.aspen_refs(repo).await?;
        let ledger: BTreeMap<RefName, LedgerEntry> =
            self.store.ledger(repo.id).await?.into_iter().collect();
        let mut names: BTreeSet<RefName> = BTreeSet::new();
        names.extend(listed.refs.keys().cloned());
        names.extend(aspen.keys().cloned());
        names.extend(ledger.keys().cloned());
        names.retain(|n| {
            let keep = repo.settings.is_writable(n);
            if !keep && listed.refs.contains_key(n) {
                report.skipped.push(n.to_string());
            }
            keep && only.is_none_or(|o| o == n)
        });
        let pass = Pass {
            repo,
            record,
            upstream,
            listed,
            advert: OnceCell::new(),
        };
        // Trunk first: the other refs' imports and pushes use its value.
        if names.remove(&trunk) {
            let outcome = self
                .step(
                    &pass,
                    &trunk,
                    aspen.get(&trunk).copied(),
                    ledger.get(&trunk),
                )
                .await;
            report.refs.insert(trunk.clone(), outcome);
        }
        let rest: Vec<RefName> = names.into_iter().collect();
        let outcomes: Vec<(RefName, RefOutcome)> = futures_util::stream::iter(rest)
            .map(|name| {
                let a = aspen.get(&name).copied();
                let entry = ledger.get(&name);
                let pass = &pass;
                async move {
                    let o = self.step(pass, &name, a, entry).await;
                    (name, o)
                }
            })
            .buffer_unordered(self.config.parallel_refs.max(1))
            .collect()
            .await;
        report.refs.extend(outcomes);
        Ok(report)
    }

    /// Aspen's live branches and tags.
    async fn aspen_refs(&self, repo: &Repo) -> Result<BTreeMap<RefName, ObjectId>, StoreError> {
        let mut out = BTreeMap::new();
        let mut after = None;
        const PAGE: u32 = 1000;
        loop {
            let rows = self
                .meta
                .list_refs(
                    repo.id,
                    &RefListQuery {
                        prefixes: MIRRORED_PREFIXES.iter().map(|p| (*p).to_owned()).collect(),
                        after: after.take(),
                        limit: PAGE,
                    },
                )
                .await?;
            let full = rows.len() == PAGE as usize;
            for v in rows {
                if let Some(tip) = v.value.tip() {
                    out.insert(v.name.clone(), tip);
                }
                after = Some(v.name);
            }
            if !full || after.is_none() {
                return Ok(out);
            }
        }
    }

    /// One ref's step. Never fails the pass: a failure is the outcome.
    async fn step(
        &self,
        pass: &Pass<'_>,
        name: &RefName,
        aspen: Option<ObjectId>,
        entry: Option<&LedgerEntry>,
    ) -> RefOutcome {
        let entry = entry.copied().unwrap_or_default();
        let upstream = pass.listed.refs.get(name).copied();
        let outcome = match decide(pass.record.mode, aspen, upstream, entry.synced) {
            Action::InSync => if entry.synced != aspen || entry.conflict.is_some() {
                self.agree(pass.repo.id, name, entry.synced, aspen).await
            } else {
                Ok(())
            }
            .map(|()| RefOutcome::InSync),
            Action::Import => match self.import(pass, name, aspen, upstream).await {
                Ok(Applied::Updated) => {
                    self.metrics.imported();
                    self.agree(pass.repo.id, name, entry.synced, upstream)
                        .await
                        .map(|_| RefOutcome::Imported)
                }
                Ok(Applied::Rejected(why)) => Ok(RefOutcome::Failed(format!("import: {why}"))),
                Err(e) => Ok(RefOutcome::Failed(format!("import: {e}"))),
            },
            Action::Push => match self.push(pass, name, aspen, upstream).await {
                Ok(Ok(())) => {
                    self.metrics.pushed();
                    self.agree(pass.repo.id, name, entry.synced, aspen)
                        .await
                        .map(|_| RefOutcome::Pushed)
                }
                Ok(Err(why)) => Ok(RefOutcome::Failed(format!(
                    "push to the upstream refused: {why}"
                ))),
                Err(e) => Ok(RefOutcome::Failed(format!("push to the upstream: {e}"))),
            },
            Action::Conflict => {
                let conflict = Conflict {
                    at: now(),
                    aspen,
                    upstream,
                };
                let same = entry
                    .conflict
                    .is_some_and(|c| (c.aspen, c.upstream) == (aspen, upstream));
                if same {
                    Ok(RefOutcome::Conflict(entry.conflict.expect("checked")))
                } else {
                    let next = LedgerEntry {
                        synced: entry.synced,
                        conflict: Some(conflict),
                    };
                    match self
                        .store
                        .set_ledger(pass.repo.id, name, entry.synced, next, now())
                        .await
                    {
                        Ok(true) => {
                            self.metrics.conflict_found();
                            eprintln!(
                                "aspen mirror: conflict on {} {name}: Aspen {} and upstream {} both moved since {}",
                                pass.repo.name,
                                show(aspen),
                                show(upstream),
                                show(entry.synced)
                            );
                            Ok(RefOutcome::Conflict(conflict))
                        }
                        Ok(false) => Ok(RefOutcome::Failed(
                            "the ledger changed during the pass".into(),
                        )),
                        Err(e) => Err(e),
                    }
                }
            }
        };
        let outcome = outcome.unwrap_or_else(|e| RefOutcome::Failed(format!("ledger: {e}")));
        if matches!(outcome, RefOutcome::Failed(_)) {
            self.metrics.step_failed();
        }
        outcome
    }

    /// Records `value` as agreed for `name` (clearing any conflict), if the
    /// ledger still holds `expected`. A lost race leaves the ledger to the
    /// winner; the next pass reads it again.
    async fn agree(
        &self,
        repo: RepoId,
        name: &RefName,
        expected: Option<ObjectId>,
        value: Option<ObjectId>,
    ) -> Result<(), StoreError> {
        let next = LedgerEntry {
            synced: value,
            conflict: None,
        };
        self.store
            .set_ledger(repo, name, expected, next, now())
            .await
            .map(|_| ())
    }

    /// Makes Aspen's `name` (now `aspen`) equal the upstream's `upstream`.
    async fn import(
        &self,
        pass: &Pass<'_>,
        name: &RefName,
        aspen: Option<ObjectId>,
        upstream: Option<ObjectId>,
    ) -> Result<Applied, MirrorError> {
        let update = RefUpdate {
            name: name.clone(),
            old: aspen,
            new: upstream,
        };
        let Some(want) = upstream else {
            return Ok(self.local.apply(pass.repo, &update, None).await?);
        };
        // The pack may leave out what Aspen's trunk and the ref's current
        // value reach: the push path searches exactly those (trunk, and the
        // ref as the branch scope).
        let trunk = pass.repo.settings.trunk();
        let mut haves: Vec<ObjectId> = Vec::new();
        if let Some(v) = self.meta.current_version(pass.repo.id, trunk).await?
            && let Some(t) = v.value.tip()
        {
            haves.push(t);
        }
        haves.extend(aspen);
        haves.sort();
        haves.dedup();
        let spool = tempfile::Builder::new()
            .prefix("fetch-")
            .suffix(".pack")
            .tempfile_in(&self.config.work_dir)
            .map_err(|e| UpstreamError::Io(e.to_string()))?;
        pass.upstream
            .fetch(&[want], &haves, spool.path(), self.config.max_pack_bytes)
            .await?;
        Ok(self
            .local
            .apply(pass.repo, &update, Some(spool.path()))
            .await?)
    }

    /// Makes the upstream's `name` (now `upstream`) equal Aspen's `aspen`.
    /// The outer error is a failed exchange; the inner one the upstream's
    /// refusal.
    async fn push(
        &self,
        pass: &Pass<'_>,
        name: &RefName,
        aspen: Option<ObjectId>,
        upstream: Option<ObjectId>,
    ) -> Result<Result<(), String>, MirrorError> {
        let advert = pass
            .advert
            .get_or_try_init(|| pass.upstream.push_advertisement())
            .await?;
        let update = RefUpdate {
            name: name.clone(),
            old: upstream,
            new: aspen,
        };
        let spool = tempfile::Builder::new()
            .prefix("push-")
            .suffix(".pack")
            .tempfile_in(&self.config.work_dir)
            .map_err(|e| UpstreamError::Io(e.to_string()))?;
        if let Some(want) = aspen {
            let mut haves: Vec<ObjectId> = upstream.into_iter().collect();
            haves.extend(pass.listed.refs.get(pass.repo.settings.trunk()).copied());
            haves.sort();
            haves.dedup();
            self.local
                .write_pack(pass.repo, want, &haves, spool.path())
                .await?;
        }
        let report = pass
            .upstream
            .push(
                advert,
                std::slice::from_ref(&update),
                Some(spool.path()),
                false,
            )
            .await?;
        Ok(report.status(name))
    }

    /// Proxy push-through: forwards `updates` (each `old` is Aspen's
    /// current value) to the upstream with `pack`, before Aspen commits
    /// them. An update goes out only if the upstream's value is Aspen's;
    /// otherwise it is [`Forwarded::Ahead`] and a sync is requested. The
    /// upstream checks every old value again under its own lock. Accepted
    /// updates are recorded as agreed in the ledger, best effort: proxy
    /// mode never decides from the ledger, so a failed write is logged and
    /// counted and the update stays accepted. The upstream's reasons are
    /// made safe to show a Git client ([`client_text`]).
    pub async fn forward(
        &self,
        repo: &Repo,
        updates: &[RefUpdate],
        pack: Option<&Path>,
        atomic: bool,
    ) -> Vec<Forwarded> {
        let out = self.forward_inner(repo, updates, pack, atomic).await;
        let out = match out {
            Ok(v) => v,
            Err(e) => {
                let f = match &e {
                    MirrorError::Upstream(u) if u.retryable() => {
                        Forwarded::Unavailable(e.to_string())
                    }
                    MirrorError::Store(_) => Forwarded::Unavailable(e.to_string()),
                    _ => Forwarded::Rejected(e.to_string()),
                };
                vec![f; updates.len()]
            }
        };
        let out: Vec<Forwarded> = out
            .into_iter()
            .map(|f| match f {
                Forwarded::Rejected(why) => Forwarded::Rejected(client_text(&why)),
                Forwarded::Unavailable(why) => Forwarded::Unavailable(client_text(&why)),
                other => other,
            })
            .collect();
        if out.contains(&Forwarded::Ahead) {
            self.request_sync(repo.id);
        }
        for f in &out {
            self.metrics.forwarded(f);
        }
        out
    }

    async fn forward_inner(
        &self,
        repo: &Repo,
        updates: &[RefUpdate],
        pack: Option<&Path>,
        atomic: bool,
    ) -> Result<Vec<Forwarded>, MirrorError> {
        let Some(record) = self.store.mirror(repo.id).await? else {
            // Mirroring was removed since the push started: nothing to
            // forward to, and nothing the upstream accepted.
            return Ok(vec![Forwarded::NotMirror; updates.len()]);
        };
        let up = self
            .upstream(&record.upstream, record.credential.as_ref())
            .await?;
        let advert = up.push_advertisement().await?;
        let mut out: Vec<Option<Forwarded>> = updates
            .iter()
            .map(|u| (advert.refs.get(&u.name).copied() != u.old).then_some(Forwarded::Ahead))
            .collect();
        if atomic && out.iter().any(Option::is_some) {
            return Ok(vec![Forwarded::Ahead; updates.len()]);
        }
        let send: Vec<RefUpdate> = updates
            .iter()
            .zip(&out)
            .filter(|(_, o)| o.is_none())
            .map(|(u, _)| u.clone())
            .collect();
        if !send.is_empty() {
            let report = up
                .push(&advert, &send, pack, atomic && send.len() > 1)
                .await?;
            for u in &send {
                let status = report.status(&u.name);
                if status.is_ok() {
                    self.record_agreed(repo, &u.name, u.new).await;
                }
                let slot = updates
                    .iter()
                    .position(|x| x.name == u.name)
                    .expect("sent updates come from `updates`");
                out[slot] = Some(match status {
                    Ok(()) => Forwarded::Accepted,
                    Err(why) => Forwarded::Rejected(why),
                });
            }
        }
        Ok(out
            .into_iter()
            .map(|o| o.expect("every update has an answer"))
            .collect())
    }

    /// Records in the ledger that both sides hold `value` for `name` after
    /// the upstream accepted a forwarded update. Best effort: the update
    /// stands whatever happens here, and a ledger that lags only makes the
    /// next pass record the agreement itself.
    async fn record_agreed(&self, repo: &Repo, name: &RefName, value: Option<ObjectId>) {
        let result = match self.store.ledger_entry(repo.id, name).await {
            Ok(entry) => self.agree(repo.id, name, entry.synced, value).await,
            Err(e) => Err(e),
        };
        if let Err(e) = result {
            self.metrics.ledger_write_failed();
            eprintln!(
                "aspen mirror: {} {name}: the upstream accepted the update, but recording it in the ledger failed: {e}",
                repo.name
            );
        }
    }

    /// Resolves the conflict on `name` in favour of `keep` and requests a
    /// sync, which then pushes or imports. Fails if the ref has no
    /// conflict, or the ledger changed meanwhile.
    pub async fn resolve(
        &self,
        repo: &Repo,
        name: &RefName,
        keep: Side,
    ) -> Result<(), MirrorError> {
        let entry = self.store.ledger_entry(repo.id, name).await?;
        let Some(conflict) = entry.conflict else {
            return Err(MirrorError::Refused(format!("{name} has no conflict")));
        };
        let synced = crate::reconcile::resolve(&conflict, keep);
        let next = LedgerEntry {
            synced,
            conflict: None,
        };
        if !self
            .store
            .set_ledger(repo.id, name, entry.synced, next, now())
            .await?
        {
            return Err(MirrorError::Refused(format!(
                "the ledger of {name} changed meanwhile; look at it again"
            )));
        }
        self.refresh_conflicts().await;
        self.request_sync(repo.id);
        Ok(())
    }

    /// Claims and runs one `mirror_push` job as `worker`; `false` when
    /// none was due. The job brings the upstream in line with the ref it
    /// names (async mode): a conflict completes it (the conflict is
    /// recorded and pages instead), a failed step fails it so it is retried
    /// with backoff and ends dead after
    /// [`aspen_core::meta::MAX_JOB_ATTEMPTS`].
    ///
    /// A transient store error ([`StoreError::Unavailable`]: a failover, a
    /// reset connection) does not leave the job leased to `worker` while it
    /// runs nothing (issue #188, as #131 for the compaction worker):
    ///
    /// - recording the outcome is retried with backoff for as long as the
    ///   job's lease lasts ([`MirrorConfig::job_lease`], from the claim), so
    ///   during an outage this call waits up to one lease before it returns
    ///   the error. The update is fenced on worker and attempt, so a repeat
    ///   is safe. A repeat the store refuses after a transient error means the
    ///   job is no longer running under this attempt: the outcome was
    ///   written with its answer lost, or the lease ended and the job was
    ///   reclaimed. Either way it is no longer this worker's, and the job is
    ///   idempotent, so this returns as if the outcome were recorded;
    /// - after a claim that returned an error, the claim may still have
    ///   committed, so the next call first gives back the `mirror_push`
    ///   jobs leased under the worker's name
    ///   ([`MetaStore::release_jobs`]), and only those: other kinds leased
    ///   under the same name keep their leases.
    pub async fn run_one_job(&self, worker: &mut JobWorker) -> Result<bool, StoreError> {
        const TAGS: [JobTag; 1] = [JobTag::MirrorPush];
        if worker.release_pending {
            self.meta
                .release_jobs(
                    &TAGS,
                    &worker.name,
                    "given back after a claim whose answer was lost",
                )
                .await?;
            worker.release_pending = false;
        }
        // The lease runs from the claim; read the time before it, so this
        // end is never later than the store's.
        let claimed_at = tokio::time::Instant::now();
        let claimed = self
            .meta
            .claim_job(&TAGS, &worker.name, self.config.job_lease)
            .await;
        worker.release_pending = claimed.is_err();
        let Some(job) = claimed? else {
            return Ok(false);
        };
        // The lease is not renewed while the job runs; a job that outlives
        // it gets no retries.
        let lease_end = claimed_at + self.config.job_lease;
        let result = match job.kind {
            JobKind::MirrorPush { version } => self.job(job.repo, version).await,
            other => Err(MirrorError::Refused(format!(
                "not a mirror_push job: {other:?}"
            ))),
        };
        let error = result.err().map(|e| e.to_string());
        let owner = &worker.name;
        self.record_outcome(job.id, lease_end, || match &error {
            None => JobEvent::Complete {
                owner: owner.clone(),
                attempt: job.attempts,
            },
            Some(error) => JobEvent::Fail {
                owner: owner.clone(),
                attempt: job.attempts,
                now: now(),
                error: error.clone(),
                backoff: self.config.retry_backoff,
            },
        })
        .await?;
        Ok(true)
    }

    /// Records a job's outcome (`event()`, built anew for each attempt),
    /// retrying a transient store error with backoff until `lease_end`.
    /// After an attempt that failed with [`StoreError::Unavailable`], the
    /// update may or may not have committed; a repeat the store then
    /// refuses ([`StoreError::Conflict`]) means it did, or that the lease
    /// ended and the job was reclaimed. Either way the job is no longer
    /// this worker's, so this returns `Ok` without knowing which.
    ///
    /// The first attempt is sent even when the job ran past `lease_end`.
    /// `aspen-core`'s job fencing ([`aspen_core::meta::Job::apply`]) checks
    /// worker and attempt, not the time: an outcome sent after the lease
    /// expired is accepted while no other worker has claimed the job again,
    /// and refused once one has, and the two can race. Both are safe: the
    /// job reconciles the ref's current value, so it is idempotent. If the
    /// outcome is accepted, the job is finished (`Complete`) or requeued
    /// (`Fail`) as usual; if it is refused, the worker that claimed the job
    /// again runs it, and the refusal is returned (or, after a transient
    /// error, taken as recorded, as above).
    async fn record_outcome(
        &self,
        job: JobId,
        lease_end: tokio::time::Instant,
        event: impl Fn() -> JobEvent,
    ) -> Result<(), StoreError> {
        let mut wait = OUTCOME_RETRY_FIRST;
        let mut sent_before = false;
        loop {
            match self.meta.update_job(job, event()).await {
                Ok(_) => return Ok(()),
                Err(StoreError::Conflict { .. }) if sent_before => return Ok(()),
                Err(e @ StoreError::Unavailable { .. }) => {
                    if tokio::time::Instant::now() + wait >= lease_end {
                        return Err(e);
                    }
                    sent_before = true;
                    tokio::time::sleep(wait).await;
                    wait = (wait * 2).min(OUTCOME_RETRY_MAX);
                }
                Err(e) => return Err(e),
            }
        }
    }

    async fn job(&self, repo: RepoId, version: RefVersionId) -> Result<(), MirrorError> {
        let Some(repo) = self.meta.repo_by_id(repo).await? else {
            return Ok(());
        };
        let Some(record) = self.store.mirror(repo.id).await? else {
            return Ok(());
        };
        if record.mode != MirrorMode::Async {
            return Ok(());
        }
        let v = self
            .meta
            .ref_version(version)
            .await?
            .ok_or_else(|| StoreError::NotFound {
                what: format!("ref version {version}"),
            })?;
        let report = self.pass(&repo, &record, Some(&v.name)).await?;
        self.refresh_conflicts().await;
        match report.refs.get(&v.name) {
            Some(RefOutcome::Failed(reason)) => Err(MirrorError::Step {
                name: v.name.clone(),
                reason: reason.clone(),
            }),
            _ => Ok(()),
        }
    }

    /// Runs the worker until the process ends: `mirror_push` jobs as they
    /// come, and a sync pass of every mirror each `sync_interval` (or at
    /// once, when one is requested). Errors are logged and retried.
    /// `worker` names the job leases. It must be unique among the live
    /// workers of `mirror_push` jobs on the same metadata store, so never
    /// run two loops under one name; see [`JobWorker`].
    pub async fn run(self: Arc<Self>, worker: String) {
        let mut worker = JobWorker::new(worker);
        let mut last_pass: BTreeMap<RepoId, tokio::time::Instant> = BTreeMap::new();
        loop {
            loop {
                match self.run_one_job(&mut worker).await {
                    Ok(true) => continue,
                    Ok(false) => break,
                    Err(e) => {
                        eprintln!("aspen mirror: job queue: {e}");
                        break;
                    }
                }
            }
            match self.store.mirrors().await {
                Ok(mirrors) => {
                    let requested: HashSet<RepoId> =
                        std::mem::take(&mut *self.requested.lock().expect("not poisoned"));
                    for m in mirrors {
                        let due = requested.contains(&m.repo)
                            || last_pass
                                .get(&m.repo)
                                .is_none_or(|t| t.elapsed() >= self.config.sync_interval);
                        if !due {
                            continue;
                        }
                        last_pass.insert(m.repo, tokio::time::Instant::now());
                        match self.meta.repo_by_id(m.repo).await {
                            Ok(Some(repo)) => {
                                if let Err(e) = self.sync(&repo).await {
                                    eprintln!("aspen mirror: sync of {}: {e}", repo.name);
                                }
                            }
                            Ok(None) => {}
                            Err(e) => eprintln!("aspen mirror: repository {}: {e}", m.repo),
                        }
                    }
                }
                Err(e) => eprintln!("aspen mirror: listing mirrors: {e}"),
            }
            let _ = tokio::time::timeout(self.config.job_poll, self.wake.notified()).await;
        }
    }
}

/// Longest upstream text passed on to a Git client, in bytes.
pub const CLIENT_TEXT_MAX: usize = 512;

/// Makes text from an upstream (a refusal reason, an error) safe to pass to
/// a Git client in an `ng` line or on side-band 2: control characters
/// (newlines, escape sequences) become spaces, runs of spaces collapse, and
/// the result is cut at [`CLIENT_TEXT_MAX`] bytes on a character boundary,
/// ending in `...` when cut.
pub fn client_text(s: &str) -> String {
    let mut out = String::new();
    for c in s.trim().chars() {
        let c = if c.is_control() { ' ' } else { c };
        if c == ' ' && out.ends_with(' ') {
            continue;
        }
        out.push(c);
        if out.len() > CLIENT_TEXT_MAX {
            break;
        }
    }
    if out.len() > CLIENT_TEXT_MAX {
        let mut cut = CLIENT_TEXT_MAX - 3;
        while !out.is_char_boundary(cut) {
            cut -= 1;
        }
        out.truncate(cut);
        out.truncate(out.trim_end().len());
        out.push_str("...");
    }
    out
}

fn show(v: Option<ObjectId>) -> String {
    v.map_or_else(|| "(absent)".to_owned(), |o| o.to_string())
}

/// Picks the upstream's side or Aspen's, for the CLI.
impl std::str::FromStr for Side {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, String> {
        match s {
            "aspen" => Ok(Self::Aspen),
            "upstream" => Ok(Self::Upstream),
            other => Err(format!("unknown side {other:?} (aspen or upstream)")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upstream_text_reaches_clients_on_one_line_and_bounded() {
        assert_eq!(
            client_text("  refused:\n\x1b[31mprotected\x1b[0m\r\n\tby policy \n"),
            "refused: [31mprotected [0m by policy"
        );
        let fits = "x".repeat(CLIENT_TEXT_MAX);
        assert_eq!(client_text(&fits), fits);
        let long = "é".repeat(CLIENT_TEXT_MAX);
        let cut = client_text(&long);
        assert!(cut.len() <= CLIENT_TEXT_MAX, "{}", cut.len());
        assert!(cut.ends_with("é..."), "cut on a character boundary");
        assert!(client_text(&"a\n".repeat(100_000)).len() <= CLIENT_TEXT_MAX);
    }
}
