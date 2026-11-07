//! Write transactions: [`SqlStore::write`] and the [`WriteTx`] guard.
//!
//! Every statement that writes runs inside [`SqlStore::write`], which owns
//! the transaction from `BEGIN` to `COMMIT` or `ROLLBACK`. Nothing else in
//! the crate can begin a write transaction or end one: [`WriteTx`] has no
//! public constructor, and its commit and rollback are private to this
//! module.
//!
//! Why the ending matters: dropping a sqlx transaction only *queues* its
//! rollback, which runs when the pool next tests the connection, in a task
//! of its own. Until then, on Postgres, the transaction's row locks and
//! uncommitted index entries stay in place. A request that failed would
//! return before its locks were released: a refused job update would keep
//! the job locked, so a `SKIP LOCKED` claim right after it passes over a
//! claimable job; a push refused halfway would keep its half-inserted
//! version, so the next push to that ref waits for it. [`SqlStore::write`]
//! therefore awaits the rollback before it returns the error.
//!
//! A write future can also be dropped before it reaches either ending: the
//! caller is cancelled (a client disconnects, a timeout fires) or the body
//! panics. [`WriteTx`]'s `Drop` then spawns the rollback at once on the
//! current Tokio runtime instead of leaving it for the pool.
//!
//! A transaction holds its turn from the store's admission queue
//! (`crate::admission`) from before `BEGIN` until it has ended, the spawned
//! rollback included, so the queue never hands its connection's slot to
//! another operation while the transaction still holds the connection.
//! On SQLite it also holds its turn at the write lock ([`WriteQueue`]) for
//! as long.

use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use aspen_core::error::StoreError;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::admission::Permit;
use crate::conn::Conn;
use crate::error::store_error;
use crate::store::{Dialect, SqlStore};

/// An open write transaction. Statements run through it as on a [`Conn`]
/// (it dereferences to one).
///
/// Only [`SqlStore::write`] creates one, and only it ends one, by commit or
/// rollback; if the guard is dropped while still open, its `Drop` spawns
/// the rollback.
pub(crate) struct WriteTx {
    /// `Some` until the transaction is committed or rolled back.
    conn: Option<Conn>,
    /// This transaction's turn at the pool, and on SQLite at the write
    /// lock; released after the transaction ends (dropped with the guard,
    /// or moved into the spawned rollback).
    permit: Option<(Option<WriteTurn>, Permit)>,
}

/// SQLite's queue for its one write lock, which [`SqlStore::write`] joins
/// before `BEGIN IMMEDIATE`. Shared by a store's clones.
///
/// SQLite lets one transaction write at a time. A connection that finds
/// the lock taken polls for it: its busy handler sleeps between tries, up
/// to 100 ms at a time, until the lock timeout. So without this queue,
/// writers get the lock in no particular order, and one that has waited
/// for seconds loses to one that has just asked. A garbage-collection
/// sweep that runs batch after batch takes the lock again within
/// microseconds of each commit, and in the nightly `sweep_cost` test at a
/// million packs, pushes running beside it waited out the 10 s lock
/// timeout and failed with "database is locked". Here writers take the
/// lock first in, first out, so the busy handler only arbitrates between
/// this store and other processes, or other stores, on the same file.
///
/// A waiter gives up only once the lock timeout passes with no write
/// transaction of this store ending, the admission queue's rule
/// (`crate::admission`): a long line of writers that each hold the lock
/// briefly is not a failure, a writer stuck holding it is.
#[derive(Debug, Clone)]
pub(crate) struct WriteQueue {
    inner: Arc<WriteQueueInner>,
}

#[derive(Debug)]
struct WriteQueueInner {
    /// One permit, handed out in arrival order (Tokio's semaphore is fair).
    lock: Arc<Semaphore>,
    /// Write transactions that have ended: the progress a waiter looks for.
    ended: AtomicU64,
    /// How long a waiter waits without progress; zero means for ever.
    timeout: Duration,
}

/// A turn at the write lock; it ends when dropped.
#[derive(Debug)]
pub(crate) struct WriteTurn {
    _permit: OwnedSemaphorePermit,
    queue: Arc<WriteQueueInner>,
}

impl Drop for WriteTurn {
    fn drop(&mut self) {
        self.queue.ended.fetch_add(1, Ordering::Release);
    }
}

impl WriteQueue {
    /// A queue whose waiters give up after `timeout` without a write
    /// transaction ending (zero: never).
    pub(crate) fn new(timeout: Duration) -> Self {
        Self {
            inner: Arc::new(WriteQueueInner {
                lock: Arc::new(Semaphore::new(1)),
                ended: AtomicU64::new(0),
                timeout,
            }),
        }
    }

    /// Waits for the write lock's turn, in arrival order. Fails with
    /// [`StoreError::Unavailable`] only after the timeout passes with no
    /// write transaction ending.
    pub(crate) async fn turn(&self) -> Result<WriteTurn, StoreError> {
        let inner = &self.inner;
        let turn = |permit: Result<OwnedSemaphorePermit, _>| WriteTurn {
            _permit: permit.expect("the semaphore is never closed"),
            queue: Arc::clone(inner),
        };
        // One `Acquire` future for the whole wait: dropping it would give
        // up this waiter's place in the queue.
        let acquire = Arc::clone(&inner.lock).acquire_owned();
        tokio::pin!(acquire);
        if inner.timeout.is_zero() {
            return Ok(turn(acquire.await));
        }
        let mut seen = inner.ended.load(Ordering::Acquire);
        loop {
            match tokio::time::timeout(inner.timeout, &mut acquire).await {
                Ok(permit) => return Ok(turn(permit)),
                Err(_) => {
                    let now = inner.ended.load(Ordering::Acquire);
                    if now == seen {
                        return Err(StoreError::Unavailable {
                            reason: format!(
                                "no write transaction on the metadata database ended in {:?} \
                                 while this one waited for the write lock (a writer is stuck)",
                                inner.timeout
                            ),
                        });
                    }
                    seen = now;
                }
            }
        }
    }
}

impl WriteTx {
    fn take(&mut self) -> (sqlx::Transaction<'static, sqlx::Any>, crate::conn::Counter) {
        self.conn
            .take()
            .and_then(Conn::into_tx)
            .expect("a WriteTx is ended once, and holds a transaction")
    }

    async fn commit(mut self) -> Result<(), StoreError> {
        let (tx, counter) = self.take();
        counter.transaction_control();
        tx.commit().await.map_err(store_error)
    }

    /// Rolls back, ignoring a failure: the request has already failed. If
    /// the rollback itself fails, sqlx queues it again and the pool tests
    /// the connection when it is released, closing it if it is broken;
    /// closing a connection ends its transaction on the server.
    async fn rollback(mut self) {
        let (tx, counter) = self.take();
        counter.transaction_control();
        let _ = tx.rollback().await;
    }
}

impl Deref for WriteTx {
    type Target = Conn;

    fn deref(&self) -> &Conn {
        self.conn
            .as_ref()
            .expect("a WriteTx is open until it is ended")
    }
}

impl DerefMut for WriteTx {
    fn deref_mut(&mut self) -> &mut Conn {
        self.conn
            .as_mut()
            .expect("a WriteTx is open until it is ended")
    }
}

impl Drop for WriteTx {
    fn drop(&mut self) {
        let Some((tx, counter)) = self.conn.take().and_then(Conn::into_tx) else {
            return;
        };
        // Still open: the write future was cancelled or its body panicked.
        // Roll back now rather than when the pool next tests the
        // connection. Outside a runtime (it cannot happen inside the store,
        // which only runs on Tokio), dropping `tx` queues the rollback as
        // sqlx always does.
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            let permit = self.permit.take();
            counter.transaction_control();
            runtime.spawn(async move {
                let _ = tx.rollback().await;
                drop(permit);
            });
        }
    }
}

impl SqlStore {
    /// Runs `body` in a write transaction: commits if it returns `Ok`, and
    /// otherwise rolls back, waiting for the rollback, before returning its
    /// error. This is the only way the store writes.
    ///
    /// On SQLite the transaction is `BEGIN IMMEDIATE`: the write lock is
    /// taken (waiting up to the lock timeout) before the first statement,
    /// so the transaction can never fail with `SQLITE_BUSY` halfway through
    /// when another writer commits between its reads and its writes. Before
    /// that, it waits its turn at the write lock in [`WriteQueue`], so this
    /// store's writers get the lock in arrival order.
    ///
    /// `E` lets a body end in a rollback that is not a storage error, such
    /// as a lost compare-and-swap.
    pub(crate) async fn write<T, E>(
        &self,
        body: impl AsyncFnOnce(&mut WriteTx) -> Result<T, E>,
    ) -> Result<T, E>
    where
        E: From<StoreError>,
    {
        let permit = self.admit_turn().await?;
        let write_turn = match self.write_queue() {
            Some(queue) => Some(queue.turn().await?),
            None => None,
        };
        self.count_write_transaction();
        let tx = self
            .connect_in_turn(|| async {
                match self.dialect() {
                    Dialect::Postgres => self.pool().begin().await,
                    Dialect::Sqlite => self.pool().begin_with("BEGIN IMMEDIATE").await,
                }
            })
            .await?;
        self.counter().transaction_control();
        let mut tx = WriteTx {
            conn: Some(Conn::tx(tx, self.counter().clone())),
            permit: Some((write_turn, permit)),
        };
        match crate::admission::in_turn(body(&mut tx)).await {
            Ok(value) => {
                tx.commit().await?;
                Ok(value)
            }
            Err(e) => {
                tx.rollback().await;
                Err(e)
            }
        }
    }
}

/// A savepoint inside a write transaction (Postgres and SQLite alike),
/// so one part of a batched transaction (group commit) can be undone
/// without the rest: [`savepoint`] before the part, then [`release`] if
/// it succeeded or [`rollback_to`] if it did not. On Postgres an error
/// aborts the whole transaction until the rollback to the savepoint. If
/// either statement fails, the caller must fail the whole transaction.
pub(crate) async fn savepoint(conn: &mut Conn) -> Result<(), StoreError> {
    conn.control("SAVEPOINT aspen_part")
        .await
        .map_err(store_error)
}

/// Ends the savepoint taken by [`savepoint`], keeping its writes.
pub(crate) async fn release(conn: &mut Conn) -> Result<(), StoreError> {
    conn.control("RELEASE SAVEPOINT aspen_part")
        .await
        .map_err(store_error)
}

/// Undoes every write since [`savepoint`] (and frees the locks they
/// took), then ends the savepoint.
pub(crate) async fn rollback_to(conn: &mut Conn) -> Result<(), StoreError> {
    conn.control("ROLLBACK TO SAVEPOINT aspen_part")
        .await
        .map_err(store_error)?;
    release(conn).await
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;
    use std::time::Instant;

    use super::*;
    use crate::store::StoreOptions;

    /// Invariant: on SQLite, a writer that asks while another writer of
    /// the store runs transactions back to back gets the lock within a few
    /// of them. Before the write queue it polled for the lock, lost every
    /// time to the writer that retook it within microseconds of each
    /// commit, and failed with "database is locked" once the lock timeout
    /// passed (the nightly `sweep_cost` test at a million packs).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_writer_is_not_starved_by_back_to_back_transactions() {
        let dir = tempfile::tempdir().unwrap();
        let url = format!("sqlite://{}?mode=rwc", dir.path().join("w.db").display());
        let opts = StoreOptions {
            lock_timeout: Duration::from_secs(1),
            ..StoreOptions::default()
        };
        let store = SqlStore::connect_with(&url, &opts).await.unwrap();
        store.migrate().await.unwrap();

        // A sweep catching up: one write transaction after another, each
        // holding the lock 20 ms.
        let done = Arc::new(AtomicBool::new(false));
        let hog = {
            let (store, done) = (store.clone(), Arc::clone(&done));
            tokio::spawn(async move {
                while !done.load(Ordering::SeqCst) {
                    store
                        .write(async |_| {
                            tokio::time::sleep(Duration::from_millis(20)).await;
                            Ok::<_, StoreError>(())
                        })
                        .await
                        .unwrap();
                }
            })
        };
        tokio::time::sleep(Duration::from_millis(50)).await;

        let writers: Vec<_> = (0..3)
            .map(|_| {
                let store = store.clone();
                tokio::spawn(async move {
                    let mut slowest = Duration::ZERO;
                    for _ in 0..10 {
                        let begun = Instant::now();
                        store
                            .write(async |_| Ok::<_, StoreError>(()))
                            .await
                            .unwrap();
                        slowest = slowest.max(begun.elapsed());
                    }
                    slowest
                })
            })
            .collect();
        let mut slowest = Duration::ZERO;
        for w in writers {
            slowest = slowest.max(w.await.unwrap());
        }
        done.store(true, Ordering::SeqCst);
        hog.await.unwrap();
        // Behind at most the hog's turn and the two other writers'.
        assert!(
            slowest < Duration::from_millis(500),
            "a write waited {slowest:?} for the lock"
        );
    }

    /// A waiter keeps its place for as long as write transactions keep
    /// ending, however long the line; it gives up once none has ended for
    /// the timeout, a writer stuck holding the lock.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_waiter_gives_up_only_when_no_write_ends() {
        // Wide margins for loaded runners: a write ends every 60 ms, well
        // inside the 200 ms timeout, and the waiter's line lasts 300 ms,
        // half again the timeout.
        let timeout = Duration::from_millis(200);
        let hold = Duration::from_millis(60);
        let queue = WriteQueue::new(timeout);

        // Five turns of 60 ms ahead of the waiter: 300 ms in all, longer
        // than the timeout, with one ending every 60 ms.
        let first = queue.turn().await.unwrap();
        let mut ahead = Vec::new();
        for _ in 0..4 {
            let queue = queue.clone();
            ahead.push(tokio::spawn(async move {
                let turn = queue.turn().await.unwrap();
                tokio::time::sleep(hold).await;
                drop(turn);
            }));
            // Let it join the line before the next one.
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let waiter = {
            let queue = queue.clone();
            tokio::spawn(async move {
                let begun = Instant::now();
                queue.turn().await.map(|_| begun.elapsed())
            })
        };
        tokio::time::sleep(hold).await;
        drop(first);
        let waited = waiter.await.unwrap().unwrap();
        assert!(waited > timeout, "waited only {waited:?}");
        for a in ahead {
            a.await.unwrap();
        }

        // A turn held past the timeout with nothing ending: the waiter fails.
        let stuck = queue.turn().await.unwrap();
        let begun = Instant::now();
        let err = queue.turn().await.unwrap_err();
        assert!(matches!(err, StoreError::Unavailable { .. }), "{err:?}");
        assert!(begun.elapsed() >= timeout);
        drop(stuck);
        assert!(queue.turn().await.is_ok());
    }
}
