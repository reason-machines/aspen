//! The record of every operation the workload ran.
//!
//! Each client thread records an operation when it starts and completes it
//! when the `git` command returns. Times come from one monotonic clock
//! shared by all clients, so the real-time order the linearizability
//! checker relies on is exact: if one operation completed before another
//! started, the first really happened first.

use std::fmt;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// A 40-hex object ID.
pub type Oid = String;

/// What an operation did to a ref.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Call {
    /// A push of `to`, a commit whose first parent is `from`, the tip the
    /// client read before building it. On a server that only fast-forwards
    /// it succeeds exactly when the ref is still at `from`, so it is a
    /// compare-and-swap of the ref from `from` to `to`.
    Push { from: Option<Oid>, to: Oid },
    /// A read of the ref's tip (`ls-remote`, or the advertisement of a
    /// fetch): `None` when the ref did not exist.
    Read { value: Option<Oid> },
}

/// How an operation ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// The server acknowledged it (a push's `ok` line, or a read's answer).
    Ok,
    /// The server definitely did not apply it because the ref was not at
    /// the push's base (a push rejected as stale or as a lost race).
    Fail,
    /// The server definitely did not apply it, for a reason that says
    /// nothing about the ref's value: it could not lock the ref, or could
    /// not decide ancestry yet. The checker ignores such a push.
    NoEffect,
    /// The client cannot tell: the connection broke, the command timed
    /// out, or the server answered with a retryable error that may have
    /// come after the write. The operation may take effect at any time
    /// after it started, or never.
    Unknown,
}

/// One recorded operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Op {
    /// Sequence number in the whole run.
    pub id: u64,
    /// Which client ran it (`usize::MAX` for the suite's own checks).
    pub client: usize,
    /// The ref, a full name such as `refs/heads/main`.
    pub key: String,
    pub call: Call,
    pub outcome: Outcome,
    /// Nanoseconds since the run started.
    pub invoked: u64,
    /// Nanoseconds since the run started; `None` for [`Outcome::Unknown`].
    pub completed: Option<u64>,
    /// The client's or server's words, for reports.
    pub note: String,
}

impl fmt::Display for Op {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let who = if self.client == usize::MAX {
            "check".to_owned()
        } else {
            format!("client {}", self.client)
        };
        let end = match self.completed {
            Some(t) => format!("{:.3}s", t as f64 / 1e9),
            None => "?".to_owned(),
        };
        let what = match &self.call {
            Call::Push { from, to } => {
                format!("push {} -> {}", short(from.as_deref()), short(Some(to)))
            }
            Call::Read { value } => format!("read = {}", short(value.as_deref())),
        };
        write!(
            f,
            "#{} [{:.3}s, {end}] {who} {} {what}: {:?}",
            self.id,
            self.invoked as f64 / 1e9,
            self.key,
            self.outcome
        )?;
        if !self.note.is_empty() {
            write!(f, " ({})", self.note)?;
        }
        Ok(())
    }
}

/// The first 12 hex digits of an object ID, or `absent`.
pub fn short(oid: Option<&str>) -> String {
    match oid {
        Some(o) => o.get(..12).unwrap_or(o).to_owned(),
        None => "absent".to_owned(),
    }
}

/// The shared, thread-safe record.
#[derive(Debug)]
pub struct History {
    start: Instant,
    ops: Mutex<Vec<Op>>,
    /// How many recorded pushes the server acknowledged, readable without
    /// the lock (an epoch's active phase asks often, from every client).
    acked_pushes: AtomicU64,
}

impl History {
    /// A history whose clock starts now.
    pub fn new() -> Self {
        Self::starting_at(Instant::now())
    }

    /// A history whose clock started at `start`.
    pub fn starting_at(start: Instant) -> Self {
        Self {
            start,
            ops: Mutex::new(Vec::new()),
            acked_pushes: AtomicU64::new(0),
        }
    }

    /// Nanoseconds since the history started.
    pub fn now(&self) -> u64 {
        self.start.elapsed().as_nanos() as u64
    }

    /// Records a finished operation that started at `invoked`.
    pub fn record(
        &self,
        client: usize,
        key: &str,
        call: Call,
        outcome: Outcome,
        invoked: u64,
        note: impl Into<String>,
    ) -> Op {
        let completed = (outcome != Outcome::Unknown).then(|| self.now());
        let acked = outcome == Outcome::Ok && matches!(call, Call::Push { .. });
        let mut ops = self.ops.lock().unwrap_or_else(|e| e.into_inner());
        let op = Op {
            id: ops.len() as u64,
            client,
            key: key.to_owned(),
            call,
            outcome,
            invoked,
            completed,
            note: note.into(),
        };
        ops.push(op.clone());
        if acked {
            self.acked_pushes.fetch_add(1, Ordering::Relaxed);
        }
        op
    }

    /// How many pushes recorded so far the server acknowledged.
    pub fn acked_pushes(&self) -> u64 {
        self.acked_pushes.load(Ordering::Relaxed)
    }

    /// Every operation recorded so far, in recording order.
    pub fn ops(&self) -> Vec<Op> {
        self.ops.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    /// Operations recorded from index `from` on.
    pub fn ops_since(&self, from: usize) -> Vec<Op> {
        let ops = self.ops.lock().unwrap_or_else(|e| e.into_inner());
        ops.get(from..).map(<[Op]>::to_vec).unwrap_or_default()
    }

    /// How many operations are recorded.
    pub fn len(&self) -> usize {
        self.ops.lock().unwrap_or_else(|e| e.into_inner()).len()
    }

    /// Whether nothing is recorded.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Default for History {
    fn default() -> Self {
        Self::new()
    }
}
