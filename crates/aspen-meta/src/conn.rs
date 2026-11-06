//! The connection every store statement runs on, and the statement counter
//! it feeds.
//!
//! [`Conn`] is what [`crate::admission::Admitted`] (a read's connection)
//! and [`crate::tx::WriteTx`] (a write transaction) dereference to. It is
//! an executor of its own and gives no access to the connection inside, so
//! a statement cannot reach the database without being counted; and no
//! store operation takes a connection any other way
//! (`no_store_operation_bypasses_the_queue` in `src/admission.rs`). `BEGIN`,
//! `COMMIT`, `ROLLBACK` and the savepoint statements of group commit are
//! counted by [`crate::tx`], apart from the statements, because they cost
//! the database almost nothing and a single statement on its own needs
//! none.
//!
//! Counts go to the store's totals ([`crate::SqlStore::statements`]) and to
//! the innermost [`count`] scope of the task that sent the statement, which
//! is how receive-pack counts what one push sent
//! ([`aspen_core::meta::MetaStore::count_statements`]). A statement sent
//! from another task (a rollback spawned for a dropped transaction) counts
//! only in the totals.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use aspen_core::meta::Statements;
use futures_core::stream::Stream;
use sqlx::any::{AnyQueryResult, AnyRow, AnyStatement, AnyTypeInfo};
use sqlx::pool::PoolConnection;
use sqlx::{Any, AnyConnection, Either, Error, Execute, Executor, SqlStr, Transaction};

/// Statements and transaction control sent, counted atomically.
#[derive(Debug, Default)]
pub(crate) struct Counts {
    queries: AtomicU64,
    transaction_control: AtomicU64,
}

impl Counts {
    pub(crate) fn get(&self) -> Statements {
        Statements {
            queries: self.queries.load(Ordering::Relaxed),
            transaction_control: self.transaction_control.load(Ordering::Relaxed),
        }
    }
}

tokio::task_local! {
    /// The counts of the innermost [`count`] scope of this task.
    static SCOPE: Arc<Counts>;
}

/// Runs `work` and returns what the statements it sent from its own task
/// added up to, with nested scopes counted in each enclosing one.
pub(crate) async fn count<F: Future>(work: F) -> (F::Output, Statements) {
    let outer = SCOPE.try_with(Arc::clone).ok();
    let mine = Arc::new(Counts::default());
    let out = SCOPE.scope(Arc::clone(&mine), work).await;
    let got = mine.get();
    if let Some(outer) = outer {
        outer.queries.fetch_add(got.queries, Ordering::Relaxed);
        outer
            .transaction_control
            .fetch_add(got.transaction_control, Ordering::Relaxed);
    }
    (out, got)
}

/// Where a store's statements are counted: its totals, shared by every
/// clone of the store.
#[derive(Debug, Clone, Default)]
pub(crate) struct Counter(Arc<Counts>);

impl Counter {
    pub(crate) fn totals(&self) -> Statements {
        self.0.get()
    }

    fn add(&self, pick: fn(&Counts) -> &AtomicU64) {
        pick(&self.0).fetch_add(1, Ordering::Relaxed);
        let _ = SCOPE.try_with(|s| pick(s).fetch_add(1, Ordering::Relaxed));
    }

    /// One statement that reads or writes rows.
    pub(crate) fn query(&self) {
        self.add(|c| &c.queries);
    }

    /// One `BEGIN`, `COMMIT` or `ROLLBACK`.
    pub(crate) fn transaction_control(&self) {
        self.add(|c| &c.transaction_control);
    }
}

enum Inner {
    Pooled(PoolConnection<Any>),
    Tx(Transaction<'static, Any>),
}

/// A pooled connection or an open transaction, counting every statement
/// run on it. Run statements on `&mut Conn` as on any sqlx executor.
pub(crate) struct Conn {
    inner: Inner,
    counter: Counter,
}

impl std::fmt::Debug for Conn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self.inner {
            Inner::Pooled(_) => "Conn(pooled)",
            Inner::Tx(_) => "Conn(transaction)",
        })
    }
}

impl Conn {
    pub(crate) fn pooled(conn: PoolConnection<Any>, counter: Counter) -> Self {
        Self {
            inner: Inner::Pooled(conn),
            counter,
        }
    }

    pub(crate) fn tx(tx: Transaction<'static, Any>, counter: Counter) -> Self {
        Self {
            inner: Inner::Tx(tx),
            counter,
        }
    }

    /// The open transaction, for [`crate::tx`] to end; `None` for a pooled
    /// connection.
    pub(crate) fn into_tx(self) -> Option<(Transaction<'static, Any>, Counter)> {
        match self.inner {
            Inner::Tx(tx) => Some((tx, self.counter)),
            Inner::Pooled(_) => None,
        }
    }

    /// Runs a savepoint statement, counted as transaction control.
    pub(crate) async fn control(&mut self, sql: &'static str) -> Result<(), Error> {
        self.counter.transaction_control();
        sqlx::query(sql).execute(self.raw()).await.map(drop)
    }

    /// The connection itself, uncounted: only for SQLite's migrations,
    /// which sqlx runs on an `AnyConnection`.
    pub(crate) fn for_migrations(&mut self) -> &mut AnyConnection {
        self.raw()
    }

    fn raw(&mut self) -> &mut AnyConnection {
        match &mut self.inner {
            Inner::Pooled(c) => c,
            Inner::Tx(t) => t,
        }
    }
}

type BoxFuture<'e, T> = Pin<Box<dyn Future<Output = T> + Send + 'e>>;
type BoxStream<'e, T> = Pin<Box<dyn Stream<Item = T> + Send + 'e>>;

impl<'c> Executor<'c> for &'c mut Conn {
    type Database = Any;

    fn fetch_many<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> BoxStream<'e, Result<Either<AnyQueryResult, AnyRow>, Error>>
    where
        'c: 'e,
        E: 'q + Execute<'q, Any>,
    {
        self.counter.query();
        self.raw().fetch_many(query)
    }

    fn fetch_optional<'e, 'q: 'e, E>(self, query: E) -> BoxFuture<'e, Result<Option<AnyRow>, Error>>
    where
        'c: 'e,
        E: 'q + Execute<'q, Any>,
    {
        self.counter.query();
        self.raw().fetch_optional(query)
    }

    fn prepare_with<'e>(
        self,
        sql: SqlStr,
        parameters: &'e [AnyTypeInfo],
    ) -> BoxFuture<'e, Result<AnyStatement, Error>>
    where
        'c: 'e,
    {
        self.raw().prepare_with(sql, parameters)
    }

    fn describe<'e>(self, sql: SqlStr) -> BoxFuture<'e, Result<sqlx::Describe<Any>, Error>>
    where
        'c: 'e,
    {
        self.raw().describe(sql)
    }
}
