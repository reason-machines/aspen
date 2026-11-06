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

use std::ops::{Deref, DerefMut};

use aspen_core::error::StoreError;
use sqlx::{Any, AnyConnection, Transaction};

use crate::admission::Permit;
use crate::error::store_error;
use crate::store::{Dialect, SqlStore};

/// An open write transaction. Statements run through it as an
/// `&mut AnyConnection` (it dereferences to one).
///
/// Only [`SqlStore::write`] creates one, and only it ends one, by commit or
/// rollback; if the guard is dropped while still open, its `Drop` spawns
/// the rollback.
pub(crate) struct WriteTx {
    /// `Some` until the transaction is committed or rolled back.
    tx: Option<Transaction<'static, Any>>,
    /// This transaction's turn at the pool; released after the
    /// transaction ends (dropped with the guard, or moved into the spawned
    /// rollback).
    permit: Option<Permit>,
}

impl WriteTx {
    fn open(&mut self) -> &mut Transaction<'static, Any> {
        self.tx
            .as_mut()
            .expect("a WriteTx is open until it is ended")
    }

    async fn commit(mut self) -> Result<(), StoreError> {
        let tx = self.tx.take().expect("a WriteTx is ended once");
        tx.commit().await.map_err(store_error)
    }

    /// Rolls back, ignoring a failure: the request has already failed. If
    /// the rollback itself fails, sqlx queues it again and the pool tests
    /// the connection when it is released, closing it if it is broken;
    /// closing a connection ends its transaction on the server.
    async fn rollback(mut self) {
        let tx = self.tx.take().expect("a WriteTx is ended once");
        let _ = tx.rollback().await;
    }
}

impl Deref for WriteTx {
    type Target = AnyConnection;

    fn deref(&self) -> &AnyConnection {
        self.tx
            .as_ref()
            .expect("a WriteTx is open until it is ended")
    }
}

impl DerefMut for WriteTx {
    fn deref_mut(&mut self) -> &mut AnyConnection {
        self.open()
    }
}

impl Drop for WriteTx {
    fn drop(&mut self) {
        let Some(tx) = self.tx.take() else {
            return;
        };
        // Still open: the write future was cancelled or its body panicked.
        // Roll back now rather than when the pool next tests the
        // connection. Outside a runtime (it cannot happen inside the store,
        // which only runs on Tokio), dropping `tx` queues the rollback as
        // sqlx always does.
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            let permit = self.permit.take();
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
    /// when another writer commits between its reads and its writes.
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
        self.count_write_transaction();
        let tx = self
            .connect_in_turn(|| async {
                match self.dialect() {
                    Dialect::Postgres => self.pool().begin().await,
                    Dialect::Sqlite => self.pool().begin_with("BEGIN IMMEDIATE").await,
                }
            })
            .await?;
        let mut tx = WriteTx {
            tx: Some(tx),
            permit: Some(permit),
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
pub(crate) async fn savepoint(conn: &mut AnyConnection) -> Result<(), StoreError> {
    sqlx::query("SAVEPOINT aspen_part")
        .execute(conn)
        .await
        .map(drop)
        .map_err(store_error)
}

/// Ends the savepoint taken by [`savepoint`], keeping its writes.
pub(crate) async fn release(conn: &mut AnyConnection) -> Result<(), StoreError> {
    sqlx::query("RELEASE SAVEPOINT aspen_part")
        .execute(conn)
        .await
        .map(drop)
        .map_err(store_error)
}

/// Undoes every write since [`savepoint`] (and frees the locks they
/// took), then ends the savepoint.
pub(crate) async fn rollback_to(conn: &mut AnyConnection) -> Result<(), StoreError> {
    sqlx::query("ROLLBACK TO SAVEPOINT aspen_part")
        .execute(&mut *conn)
        .await
        .map_err(store_error)?;
    release(conn).await
}
