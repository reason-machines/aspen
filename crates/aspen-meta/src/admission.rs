//! Admission to the connection pool: every store operation waits its turn
//! here before it takes a connection.
//!
//! sqlx's pool gives each waiter a fixed deadline counted from the moment
//! it starts waiting (`acquire_timeout`). That deadline cannot tell a busy
//! pool from a stuck one. In a fetch storm (issue #150: 1,000 concurrent
//! `git fetch --depth=1` jobs) every fetch makes a handful of short reads,
//! each read queues behind all the others, and on a machine whose CPUs are
//! saturated each read holds its connection for two task wake-ups. Reads
//! then waited more than 30 s for a connection and failed with "internal
//! storage error; retry" while the database answered every query in
//! milliseconds. The compaction worker and the mirror queue, which share
//! the pool, failed the same way.
//!
//! So the store queues its operations itself. [`Admission`] is a
//! first-in, first-out queue with exactly as many permits as the pool has
//! connections. An operation holds a permit, and one connection, for as
//! long as it runs (a single statement, a chunked read, or a whole write
//! transaction), so the store's own operations never queue inside the
//! pool. A waiter gives up only when the pool has made **no progress** for
//! the acquire timeout: no connection was returned to the pool since it
//! started waiting, or since it last looked. A busy pool returns
//! connections all the time, so its waiters keep their place in the queue
//! however long it is; a database that is unreachable, or a pool whose
//! every connection is stuck, returns none, and its waiters fail with
//! [`StoreError::Unavailable`] as before.
//!
//! Taking the connection after the permit follows the same rule
//! ([`Admission::connect`]). sqlx puts a released connection back in the
//! pool from a task of its own, so the permit of the operation before can
//! be free while its connection is still on its way back; under load that
//! task can wait longer than sqlx's own deadline, which must then not fail
//! the operation either.
//!
//! The queue does not bound how many requests a server accepts or how long
//! they may wait in total; that is a per-instance concurrency limit, which
//! belongs in front of the work (issue #118), not in the store.
//!
//! No store operation takes a second turn while it holds one (the one read
//! after a transaction, in a lost compare-and-swap, runs after the
//! transaction has ended), so the queue cannot deadlock on itself. Two
//! checks keep it so: a write transaction's body runs inside
//! [`in_turn`], and in debug builds [`Admission::admit`] panics when called
//! from inside one; and a source check (`no_store_operation_bypasses_the_queue`)
//! refuses any use of the pool outside the queue. Single-statement reads
//! take their turn and run their statement in one expression, so nothing
//! can nest inside them. [`crate::SqlStore::pool`] bypasses the queue, for
//! operator tooling.
//!
//! The queue bounds neither how many operations wait nor how long: a storm
//! larger than the server can serve becomes a backlog, not a refusal. That
//! is accepted until issue #118 bounds concurrent fetches per instance in
//! front of the work (decisions log, #150).

use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use std::ops::{Deref, DerefMut};

use aspen_core::error::StoreError;
use sqlx::pool::PoolConnection;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::conn::{Conn, Counter};
use crate::error::store_error;

/// The store's queue for connections. Cheap to clone: clones share it.
#[derive(Clone)]
pub(crate) struct Admission {
    inner: Arc<Inner>,
}

struct Inner {
    /// One permit per pooled connection, handed out in arrival order
    /// (Tokio's semaphore is fair).
    permits: Arc<Semaphore>,
    /// Connections returned to the pool so far: the progress a waiter
    /// looks for.
    returned: AtomicU64,
    /// How long a waiter waits without progress; zero means for ever.
    timeout: Duration,
    connections: u32,
}

impl Admission {
    /// A queue for a pool of `connections` (at least 1) that gives up after
    /// `timeout` without progress (zero: never).
    pub(crate) fn new(connections: u32, timeout: Duration) -> Self {
        let connections = connections.max(1);
        Self {
            inner: Arc::new(Inner {
                permits: Arc::new(Semaphore::new(connections as usize)),
                returned: AtomicU64::new(0),
                timeout,
                connections,
            }),
        }
    }

    /// Records that a connection was given back to the pool. Called from
    /// the pool's `after_release` hook for every connection the store gives
    /// back, before the pool decides whether to keep it: one the pool then
    /// closes also counts, since it frees a slot the next waiter can open
    /// a connection in. If the database is unreachable, opening that
    /// connection fails and records nothing, so the waiter after it still
    /// fails once a timeout passes without progress.
    pub(crate) fn connection_returned(&self) {
        self.inner.returned.fetch_add(1, Ordering::Release);
    }

    /// Waits for a permit, in arrival order. Fails with
    /// [`StoreError::Unavailable`] only after the timeout passes with no
    /// connection returned to the pool.
    pub(crate) async fn admit(&self) -> Result<Permit, StoreError> {
        debug_assert!(
            !IN_TURN.try_with(|held| *held).unwrap_or(false),
            "a store operation asked for a second turn while it holds one; with a pool \
             this size busy, that waits for itself"
        );
        let inner = &self.inner;
        // One `Acquire` future for the whole wait: dropping it would give
        // up this waiter's place in the queue.
        let acquire = Arc::clone(&inner.permits).acquire_owned();
        tokio::pin!(acquire);
        if inner.timeout.is_zero() {
            return Ok(Permit(
                acquire.await.expect("the semaphore is never closed"),
            ));
        }
        let mut seen = inner.returned.load(Ordering::Acquire);
        loop {
            match tokio::time::timeout(inner.timeout, &mut acquire).await {
                Ok(permit) => return Ok(Permit(permit.expect("the semaphore is never closed"))),
                Err(_) => {
                    let now = inner.returned.load(Ordering::Acquire);
                    if now == seen {
                        return Err(StoreError::Unavailable {
                            reason: format!(
                                "no connection to the metadata database came back in {:?} \
                                 while waiting for one of {} (the database is unreachable \
                                 or every connection is stuck)",
                                inner.timeout, inner.connections
                            ),
                        });
                    }
                    seen = now;
                }
            }
        }
    }
}

impl Admission {
    /// Runs `attempt` (taking a connection from the pool, or beginning a
    /// transaction on one) until it succeeds, retrying an attempt that hit
    /// sqlx's acquire deadline when a connection came back to the pool
    /// meanwhile. The caller holds a permit, so the connection it waits for
    /// is one being returned (or opened); without progress, or on any other
    /// error, it fails.
    pub(crate) async fn connect<T, F>(
        &self,
        mut attempt: impl FnMut() -> F,
    ) -> Result<T, StoreError>
    where
        F: Future<Output = Result<T, sqlx::Error>>,
    {
        loop {
            let seen = self.inner.returned.load(Ordering::Acquire);
            match attempt().await {
                Ok(v) => return Ok(v),
                Err(sqlx::Error::PoolTimedOut)
                    if self.inner.returned.load(Ordering::Acquire) != seen => {}
                Err(e) => return Err(store_error(e)),
            }
        }
    }
}

impl fmt::Debug for Admission {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Admission")
            .field("connections", &self.inner.connections)
            .field("timeout", &self.inner.timeout)
            .field("running", &{
                self.inner.connections as usize - self.inner.permits.available_permits()
            })
            .finish()
    }
}

tokio::task_local! {
    /// Set while a write transaction's body runs, which holds a turn.
    static IN_TURN: bool;
}

/// Runs `fut`, the body of an operation that holds a turn, so that
/// [`Admission::admit`] can refuse a nested turn (in debug builds).
pub(crate) async fn in_turn<F: Future>(fut: F) -> F::Output {
    IN_TURN.scope(true, fut).await
}

/// A turn at the pool; the turn ends when it is dropped.
#[derive(Debug)]
pub(crate) struct Permit(#[allow(dead_code)] OwnedSemaphorePermit);

/// A turn at the pool and the connection it holds: what the store's
/// operations run statements on ([`crate::SqlStore::admit`]); it
/// dereferences to the connection. Dropping it returns the connection and
/// ends the turn.
#[derive(Debug)]
pub(crate) struct Admitted {
    // Declared first, so the connection is released before the permit.
    conn: Conn,
    _permit: Permit,
}

impl Admitted {
    pub(crate) fn new(conn: PoolConnection<sqlx::Any>, permit: Permit, counter: Counter) -> Self {
        Self {
            conn: Conn::pooled(conn, counter),
            _permit: permit,
        }
    }
}

impl Deref for Admitted {
    type Target = Conn;

    fn deref(&self) -> &Conn {
        &self.conn
    }
}

impl DerefMut for Admitted {
    fn deref_mut(&mut self) -> &mut Conn {
        &mut self.conn
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Invariant: every store operation takes its connection through the
    /// queue. Outside the places listed here, no source file of the crate
    /// may name the pool: a statement run on `&self.pool` or `pool()`
    /// skips the queue and fails under load with sqlx's fixed deadline
    /// again (issue #150; `covers_at` did, when #149 and this fix landed
    /// side by side). Use `&mut *self.admit().await?` instead.
    #[test]
    fn no_store_operation_bypasses_the_queue() {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut found = Vec::new();
        let mut used = vec![false; POOL_ALLOWED.len()];
        for entry in std::fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();
            let file = path.file_name().unwrap().to_str().unwrap().to_owned();
            if file == "admission.rs" {
                continue;
            }
            for (n, line) in std::fs::read_to_string(&path).unwrap().lines().enumerate() {
                match pool_use(&file, line) {
                    PoolUse::None => {}
                    PoolUse::Allowed(i) => used[i] = true,
                    PoolUse::Bypass => found.push(format!("{file}:{}: {}", n + 1, line.trim())),
                }
            }
        }
        assert!(
            found.is_empty(),
            "store code uses the pool without a turn from the queue; run it on \
             `&mut *self.admit().await?`:\n{}",
            found.join("\n")
        );
        // A stale entry would let a future line with the same text through.
        let stale: Vec<_> = POOL_ALLOWED
            .iter()
            .zip(&used)
            .filter(|(_, used)| !**used)
            .map(|((file, line), _)| format!("{file}: {line}"))
            .collect();
        assert!(
            stale.is_empty(),
            "allowlist entries that match no line; remove them:\n{}",
            stale.join("\n")
        );
    }

    /// The check above flags the bypass this fix removed, and an allowed
    /// line lets nothing else on its line, or in another file, through.
    #[test]
    fn the_bypass_check_flags_a_statement_on_the_pool() {
        for line in [
            "            .fetch_one(&self.pool)",
            "q.fetch_optional(&self.pool).await",
            "let p = &self.pool; q.fetch_one(p)",
            ".execute(self.pool())",
            ".fetch_all(store.pool())",
            "Dialect::Postgres => self.pool().begin().await, // and more",
        ] {
            assert!(
                matches!(pool_use("store.rs", line), PoolUse::Bypass),
                "not flagged: {line}"
            );
        }
        assert!(matches!(
            pool_use("store.rs", "        &self.pool"),
            PoolUse::Allowed(_)
        ));
        assert!(matches!(
            pool_use("reads.rs", "&self.pool"),
            PoolUse::Bypass
        ));
        assert!(matches!(
            pool_use("store.rs", "// see `self.pool()`"),
            PoolUse::None
        ));
    }

    /// (file, whole trimmed line) pairs allowed to use the pool directly.
    /// A line matches only when it is exactly the listed text, so nothing
    /// can be appended to an allowed line.
    const POOL_ALLOWED: &[(&str, &str)] = &[
        // The body of `SqlStore::pool`, the accessor for operator tooling.
        ("store.rs", "&self.pool"),
        // The turn itself.
        (
            "store.rs",
            "let conn = self.admission.connect(|| self.pool.acquire()).await?;",
        ),
        // Migrations run before serving, on their own connection.
        (
            "store.rs",
            "let mut conn = self.pool.acquire().await.map_err(store_error)?.detach();",
        ),
        // A write transaction begins inside its turn.
        ("tx.rs", "Dialect::Postgres => self.pool().begin().await,"),
        (
            "tx.rs",
            "Dialect::Sqlite => self.pool().begin_with(\"BEGIN IMMEDIATE\").await,",
        ),
    ];

    enum PoolUse {
        None,
        Allowed(usize),
        Bypass,
    }

    /// Whether `line` of the source file `file` names the pool, and if so
    /// whether it is an allowed use.
    fn pool_use(file: &str, line: &str) -> PoolUse {
        let code = line.trim();
        if code.starts_with("//") || !(code.contains("self.pool") || code.contains(".pool()")) {
            return PoolUse::None;
        }
        match POOL_ALLOWED
            .iter()
            .position(|(f, allowed)| *f == file && code == *allowed)
        {
            Some(i) => PoolUse::Allowed(i),
            None => PoolUse::Bypass,
        }
    }

    /// Invariant: a waiter keeps its place while connections keep coming
    /// back, however long the wait, and is served in arrival order.
    #[tokio::test(start_paused = true)]
    async fn a_busy_queue_waits_and_serves_in_order() {
        let admission = Admission::new(1, Duration::from_secs(1));
        let held = admission.admit().await.unwrap();
        let order = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut waiters = Vec::new();
        for i in 0..3 {
            let (admission, order) = (admission.clone(), Arc::clone(&order));
            waiters.push(tokio::spawn(async move {
                let permit = admission.admit().await;
                order.lock().unwrap().push(i);
                permit.map(drop)
            }));
            tokio::task::yield_now().await;
        }
        // Ten seconds of waiting, ten times the timeout, with progress
        // every half second.
        for _ in 0..20 {
            tokio::time::sleep(Duration::from_millis(500)).await;
            admission.connection_returned();
        }
        drop(held);
        for w in waiters {
            w.await.unwrap().unwrap();
        }
        assert_eq!(*order.lock().unwrap(), vec![0, 1, 2]);
    }

    /// Invariant: a waiter fails once a whole timeout passes with no
    /// connection returned, and not before.
    #[tokio::test(start_paused = true)]
    async fn a_waiter_fails_after_a_timeout_without_progress() {
        let admission = Admission::new(1, Duration::from_secs(1));
        let _held = admission.admit().await.unwrap();
        let started = tokio::time::Instant::now();
        let waiter = {
            let admission = admission.clone();
            tokio::spawn(async move { admission.admit().await.map(drop) })
        };
        tokio::time::sleep(Duration::from_millis(900)).await;
        admission.connection_returned();
        let e = waiter.await.unwrap().unwrap_err();
        assert!(
            matches!(&e, StoreError::Unavailable { reason } if reason.contains("came back")),
            "{e:?}"
        );
        // One timeout with progress, then one without.
        assert_eq!(started.elapsed(), Duration::from_secs(2));
    }

    /// Invariant: a waiter dropped mid-wait (its request was cancelled)
    /// gives up its place and loses no turn: the waiters behind it are
    /// served in order, and afterwards every permit is free again.
    #[tokio::test(start_paused = true)]
    async fn a_dropped_waiter_gives_up_its_place_and_no_turn_is_lost() {
        let admission = Admission::new(1, Duration::from_secs(1));
        let held = admission.admit().await.unwrap();
        let order = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut waiters = Vec::new();
        for i in 0..3 {
            let (admission, order) = (admission.clone(), Arc::clone(&order));
            waiters.push(tokio::spawn(async move {
                let permit = admission.admit().await.unwrap();
                order.lock().unwrap().push(i);
                tokio::time::sleep(Duration::from_millis(10)).await;
                drop(permit);
            }));
            tokio::task::yield_now().await;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
        waiters[1].abort();
        assert!(waiters.remove(1).await.unwrap_err().is_cancelled());
        drop(held);
        for w in waiters {
            w.await.unwrap();
        }
        assert_eq!(*order.lock().unwrap(), vec![0, 2]);
        assert_eq!(admission.inner.permits.available_permits(), 1);
        drop(admission.admit().await.unwrap());
    }

    /// In debug builds, asking for a turn inside a body that holds one
    /// panics instead of waiting for itself.
    #[cfg(debug_assertions)]
    #[tokio::test]
    #[should_panic(expected = "second turn")]
    async fn a_nested_turn_is_refused() {
        let admission = Admission::new(1, Duration::from_secs(1));
        let held = admission.admit().await.unwrap();
        in_turn(async {
            let _ = admission.admit().await;
        })
        .await;
        drop(held);
    }

    /// A zero timeout waits for ever.
    #[tokio::test(start_paused = true)]
    async fn a_zero_timeout_never_gives_up() {
        let admission = Admission::new(1, Duration::ZERO);
        let held = admission.admit().await.unwrap();
        let waiter = {
            let admission = admission.clone();
            tokio::spawn(async move { admission.admit().await.map(drop) })
        };
        tokio::time::sleep(Duration::from_secs(3600)).await;
        assert!(!waiter.is_finished());
        drop(held);
        waiter.await.unwrap().unwrap();
    }
}
