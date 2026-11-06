//! Group commit ([`GroupCommit`], `ASPEN_GROUP_COMMIT`): concurrent pushes
//! share their metadata transactions.
//!
//! Without it, every push runs two write transactions of its own: one
//! registers its pack ([`MetaStore::register_pack`]) and one commits its
//! ref updates ([`MetaStore::commit_push`]). With it, each kind of request
//! goes to a *lane*: the first request to arrive opens a batch, the batch
//! takes every request that arrives within [`GroupCommit::window`] of the
//! first (or until it holds [`GroupCommit::max_batch`]), and the lane runs
//! the whole batch as one transaction ([`MetaStore::register_packs`],
//! [`MetaStore::commit_pushes`]). Each lane runs at most
//! [`GroupCommit::max_in_flight`] batches at once; requests that arrive
//! while every slot is busy wait for the next batch, so batches grow with
//! load instead of transactions multiplying.
//!
//! What does not change:
//!
//! - **Durable before acknowledged.** A push submits its commit only after
//!   its pack, `.idx` and entry table are uploaded (register, then upload,
//!   then reference), and is answered only with its own entry of a batch
//!   whose transaction committed. If the batch fails as a whole (a
//!   savepoint statement, the `COMMIT`, a panic), every push in it gets
//!   the error and is refused, never acknowledged.
//! - **Compare-and-swap per push.** Inside the transaction each push runs
//!   behind its own savepoint: a push whose ref moved is undone alone and
//!   gets [`CommitOutcome::CasLost`], then recomputes its verdict and is
//!   submitted again (to a later batch), exactly as without group commit.
//! - **Pack objects.** Each push still stores its own pack under its own
//!   keys. Batching the packs of unrelated pushes into one object would
//!   make every version name a pack holding objects its tip does not
//!   reach, so no fetch could use it as a have (`closure_exact`), and
//!   compaction would copy every batch-mate into each ref's covers
//!   (decisions log, "Group commit").

use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use aspen_core::error::StoreError;
use aspen_core::meta::{CommitOutcome, MetaStore, PushCommit};
use aspen_core::pack::{NewPack, PackRecord};
use tokio::sync::{Semaphore, mpsc, oneshot};
use tokio::time::Instant;

/// Group commit settings (`ASPEN_GROUP_COMMIT*`), in
/// [`super::ReceiveConfig::group_commit`]. Off by default; the window and
/// batch size act only while it is on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GroupCommit {
    /// Whether pushes share transactions at all (`ASPEN_GROUP_COMMIT`,
    /// default off). Off, every push runs its own two transactions.
    pub enabled: bool,
    /// How long a batch stays open for more requests after its first one
    /// arrived (`ASPEN_GROUP_COMMIT_WINDOW_MS`, default 2 ms). Each push
    /// waits for it at most twice (registering its pack, committing its
    /// refs). Zero batches only what is already waiting.
    pub window: Duration,
    /// Most requests one transaction takes (`ASPEN_GROUP_COMMIT_MAX_BATCH`,
    /// default 64). A full batch starts at once.
    pub max_batch: usize,
    /// Batches of each kind (pack registrations, ref commits) running at
    /// once (default 4). Each holds one database connection while it runs.
    pub max_in_flight: usize,
}

impl GroupCommit {
    /// The default window, 2 ms.
    pub const DEFAULT_WINDOW: Duration = Duration::from_millis(2);
    /// The default most requests per transaction, 64.
    pub const DEFAULT_MAX_BATCH: usize = 64;
    /// The default batches of each kind running at once, 4.
    pub const DEFAULT_MAX_IN_FLIGHT: usize = 4;

    /// On, with the default window, batch size and batches in flight.
    pub fn on() -> Self {
        Self {
            enabled: true,
            ..Self::default()
        }
    }
}

impl Default for GroupCommit {
    /// Off, with the default window, batch size and batches in flight.
    fn default() -> Self {
        Self {
            enabled: false,
            window: Self::DEFAULT_WINDOW,
            max_batch: Self::DEFAULT_MAX_BATCH,
            max_in_flight: Self::DEFAULT_MAX_IN_FLIGHT,
        }
    }
}

/// Counters of one service's group commit, for metrics and tests.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GroupCommitStats {
    /// Commit transactions run ([`MetaStore::commit_pushes`] calls).
    pub commit_batches: u64,
    /// Push commits submitted to them.
    pub commits: u64,
    /// Registration transactions run ([`MetaStore::register_packs`] calls).
    pub register_batches: u64,
    /// Pack registrations submitted to them.
    pub registrations: u64,
    /// Batches that failed as a whole (every request in them refused).
    pub failed_batches: u64,
}

/// A request a lane batches.
trait Batched: Send + Sync + 'static {
    type Out: Send + 'static;

    fn run<M: MetaStore>(
        meta: &M,
        batch: &[Self],
    ) -> impl Future<Output = Result<Vec<Result<Self::Out, StoreError>>, StoreError>> + Send
    where
        Self: Sized;
}

impl Batched for PushCommit {
    type Out = CommitOutcome;

    fn run<M: MetaStore>(
        meta: &M,
        batch: &[Self],
    ) -> impl Future<Output = Result<Vec<Result<CommitOutcome, StoreError>>, StoreError>> + Send
    {
        meta.commit_pushes(batch)
    }
}

impl Batched for NewPack {
    type Out = PackRecord;

    fn run<M: MetaStore>(
        meta: &M,
        batch: &[Self],
    ) -> impl Future<Output = Result<Vec<Result<PackRecord, StoreError>>, StoreError>> + Send {
        meta.register_packs(batch)
    }
}

struct Waiting<R: Batched> {
    req: R,
    at: Instant,
    answer: oneshot::Sender<Result<R::Out, StoreError>>,
}

#[derive(Default)]
struct Counters {
    batches: AtomicU64,
    requests: AtomicU64,
    failed: AtomicU64,
}

/// One kind of request's queue and the task that batches it. The task
/// starts on the first request (inside the server's runtime) and ends when
/// the lane is dropped.
struct Lane<M, R: Batched> {
    send: mpsc::UnboundedSender<Waiting<R>>,
    start: Mutex<Option<(M, mpsc::UnboundedReceiver<Waiting<R>>)>>,
    config: GroupCommit,
    counters: Arc<Counters>,
}

impl<M, R> Lane<M, R>
where
    M: MetaStore + Clone + 'static,
    R: Batched,
{
    fn new(meta: M, config: GroupCommit) -> Self {
        let (send, recv) = mpsc::unbounded_channel();
        Self {
            send,
            start: Mutex::new(Some((meta, recv))),
            config,
            counters: Arc::default(),
        }
    }

    async fn submit(&self, req: R) -> Result<R::Out, StoreError> {
        if let Some((meta, recv)) = self.start.lock().expect("not poisoned").take() {
            tokio::spawn(collect(meta, recv, self.config, Arc::clone(&self.counters)));
        }
        let (answer, answered) = oneshot::channel();
        self.send
            .send(Waiting {
                req,
                at: Instant::now(),
                answer,
            })
            .map_err(|_| unavailable("the group commit lane has stopped"))?;
        answered
            .await
            .map_err(|_| unavailable("a group commit batch ended without an answer"))?
    }
}

fn unavailable(reason: &str) -> StoreError {
    StoreError::Unavailable {
        reason: reason.to_owned(),
    }
}

/// The lane's task: forms batches and runs each in a task of its own,
/// holding one of `max_in_flight` slots.
async fn collect<M, R>(
    meta: M,
    mut recv: mpsc::UnboundedReceiver<Waiting<R>>,
    config: GroupCommit,
    counters: Arc<Counters>,
) where
    M: MetaStore + Clone + 'static,
    R: Batched,
{
    let slots = Arc::new(Semaphore::new(config.max_in_flight.max(1)));
    let max = config.max_batch.max(1);
    loop {
        let slot = Arc::clone(&slots)
            .acquire_owned()
            .await
            .expect("the semaphore is never closed");
        let Some(first) = recv.recv().await else {
            return;
        };
        let deadline = first.at + config.window;
        let mut batch = vec![first];
        // `timeout_at` polls the queue before the timer, so requests that
        // are already waiting join even after the deadline.
        while batch.len() < max {
            match tokio::time::timeout_at(deadline, recv.recv()).await {
                Ok(Some(w)) => batch.push(w),
                Ok(None) | Err(_) => break,
            }
        }
        let meta = meta.clone();
        let counters = Arc::clone(&counters);
        tokio::spawn(async move {
            run(&meta, batch, &counters).await;
            drop(slot);
        });
    }
}

/// Runs one batch and answers each request with its own result. A batch
/// that fails as a whole answers every request with the error. If this
/// panics, the dropped answer channels refuse every request too.
async fn run<M: MetaStore, R: Batched>(meta: &M, batch: Vec<Waiting<R>>, counters: &Counters) {
    let (reqs, answers): (Vec<R>, Vec<_>) = batch.into_iter().map(|w| (w.req, w.answer)).unzip();
    counters.batches.fetch_add(1, Ordering::Relaxed);
    counters
        .requests
        .fetch_add(reqs.len() as u64, Ordering::Relaxed);
    let results = match R::run(meta, &reqs).await {
        Ok(results) if results.len() == answers.len() => results,
        Ok(results) => {
            counters.failed.fetch_add(1, Ordering::Relaxed);
            let e = StoreError::Corrupt {
                what: format!(
                    "a group commit batch of {} answered {} results",
                    answers.len(),
                    results.len()
                ),
            };
            answers.iter().map(|_| Err(e.clone())).collect()
        }
        Err(e) => {
            counters.failed.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(error = %e, requests = answers.len(), "group commit batch failed; every request in it is refused");
            answers.iter().map(|_| Err(e.clone())).collect()
        }
    };
    for (answer, result) in answers.into_iter().zip(results) {
        // A push whose client went away no longer listens; its result
        // stands in the store either way.
        let _ = answer.send(result);
    }
}

/// The two lanes of a receive-pack service.
pub(crate) struct Group<M> {
    commits: Lane<M, PushCommit>,
    packs: Lane<M, NewPack>,
}

impl<M> std::fmt::Debug for Group<M> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Group")
            .field("config", &self.commits.config)
            .finish_non_exhaustive()
    }
}

impl<M> Group<M>
where
    M: MetaStore + Clone + 'static,
{
    pub(crate) fn new(meta: M, config: GroupCommit) -> Self {
        Self {
            commits: Lane::new(meta.clone(), config),
            packs: Lane::new(meta, config),
        }
    }

    pub(crate) async fn commit_push(&self, push: PushCommit) -> Result<CommitOutcome, StoreError> {
        self.commits.submit(push).await
    }

    pub(crate) async fn register_pack(&self, pack: NewPack) -> Result<PackRecord, StoreError> {
        self.packs.submit(pack).await
    }

    pub(crate) fn stats(&self) -> GroupCommitStats {
        let c = &self.commits.counters;
        let p = &self.packs.counters;
        GroupCommitStats {
            commit_batches: c.batches.load(Ordering::Relaxed),
            commits: c.requests.load(Ordering::Relaxed),
            register_batches: p.batches.load(Ordering::Relaxed),
            registrations: p.requests.load(Ordering::Relaxed),
            failed_batches: c.failed.load(Ordering::Relaxed) + p.failed.load(Ordering::Relaxed),
        }
    }
}
