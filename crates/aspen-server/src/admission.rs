//! How much Git work runs at once (issue #118): fetches, pushes and
//! stitches, bounded per caller and per instance, over HTTP and SSH alike.
//!
//! Rate limits ([`aspen_auth::Limits`]) bound how often a caller starts
//! work; these bound how much of it is running. A request holds a [`Slot`]
//! for its kind of [`Work`] from the moment [`crate::access::Access::admit`]
//! lets it in until it is finished, before its body is read:
//!
//! | Work | Held by |
//! |---|---|
//! | [`Work::Fetch`] | every `git-upload-pack` HTTP request (advertisement, `ls-refs`, a `fetch` round up to its pack) and every SSH `git-upload-pack` session |
//! | [`Work::Push`] | every `git-receive-pack` HTTP request and SSH session, including the blocking thread that reads the pack |
//! | [`Work::Stitch`] | a fetch's inline pack, from its stitch plan until the last byte is handed to the connection |
//!
//! Each request first takes a place in its caller's count (the
//! rate-limit key: the principal, or the anonymous address), capped by
//! [`ConcurrentLimits`]; over the cap it is refused at once
//! ([`Busy::Caller`], HTTP `429`). It then takes one of the instance's
//! slots ([`InstanceLimits`]). When none is free it waits in a bounded
//! queue for at most [`InstanceLimits::queue_wait`]; with the queue full,
//! or after the wait, it is refused ([`Busy::Instance`], HTTP `503`).
//! Both refusals say when to retry ([`RETRY_AFTER`]); over SSH and inside
//! a fetch they are a retryable `ERR`.
//!
//! A slot is released when the last clone of it is dropped, so it is
//! released however the request ends: answered, failed, or abandoned by a
//! client that hung up (the server drops the request's future, its body
//! and its response stream). Callers are counted only while they hold or
//! wait for a slot, so the per-caller table never holds more keys than
//! the instance has slots and queue places.

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use aspen_auth::ConcurrentLimits;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// What a refused request is told to wait before it retries.
pub const RETRY_AFTER: Duration = Duration::from_secs(5);

/// A kind of work that is counted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Work {
    /// A `git-upload-pack` request or SSH session.
    Fetch,
    /// A `git-receive-pack` request or SSH session.
    Push,
    /// Building and streaming one fetch's inline pack.
    Stitch,
}

impl Work {
    /// The plural noun messages use.
    pub fn plural(self) -> &'static str {
        match self {
            Self::Fetch => "fetches",
            Self::Push => "pushes",
            Self::Stitch => "stitches",
        }
    }
}

/// The instance-wide limits: how many requests of each kind run at once
/// on this server process, whoever sends them, and how many may wait.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InstanceLimits {
    /// Fetch requests and SSH fetch sessions at once.
    pub fetches: usize,
    /// Push requests and SSH push sessions at once. Each push reads its
    /// pack on one of tokio's blocking threads (512 by default, shared with
    /// gzip decoding and local blob reads), so keep this well below that.
    pub pushes: usize,
    /// Inline packs being stitched at once.
    pub stitches: usize,
    /// Requests of each kind that may wait for a slot; `0` refuses as soon
    /// as every slot is taken.
    pub queue: usize,
    /// Longest a request waits in the queue before it is refused.
    pub queue_wait: Duration,
}

impl InstanceLimits {
    /// As many slots as a semaphore holds: in effect no limit (test
    /// harnesses and embeddings that bound work themselves).
    pub const UNLIMITED: Self = Self {
        fetches: Semaphore::MAX_PERMITS,
        pushes: Semaphore::MAX_PERMITS,
        stitches: Semaphore::MAX_PERMITS,
        queue: 0,
        queue_wait: Duration::ZERO,
    };
}

impl Default for InstanceLimits {
    /// 32 fetches, 16 pushes and 32 stitches at once, 64 requests of each
    /// kind waiting for at most 10 seconds.
    fn default() -> Self {
        Self {
            fetches: 32,
            pushes: 16,
            stitches: 32,
            queue: 64,
            queue_wait: Duration::from_secs(10),
        }
    }
}

/// Why a request was not given a slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum Busy {
    /// The caller already runs as many of this kind as it may (HTTP `429`).
    #[error(
        "too many concurrent {} for this caller (at most {max}); retry in {} s",
        work.plural(),
        RETRY_AFTER.as_secs()
    )]
    Caller {
        /// The kind of work.
        work: Work,
        /// The caller's cap.
        max: u32,
    },
    /// Every slot of this instance is taken and the queue is full or the
    /// wait ran out (HTTP `503`).
    #[error(
        "server busy: too many {} running; retry in {} s",
        work.plural(),
        RETRY_AFTER.as_secs()
    )]
    Instance {
        /// The kind of work.
        work: Work,
    },
}

/// Slots and counts for one server process. Shared by HTTP and SSH
/// through [`crate::access::Access`].
pub struct Admission {
    per_caller: ConcurrentLimits,
    callers: Arc<Mutex<HashMap<(Work, String), u32>>>,
    fetches: Pool,
    pushes: Pool,
    stitches: Pool,
}

impl fmt::Debug for Admission {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Admission")
            .field("per_caller", &self.per_caller)
            .field("fetches", &self.fetches)
            .field("pushes", &self.pushes)
            .field("stitches", &self.stitches)
            .finish_non_exhaustive()
    }
}

impl Admission {
    /// Admission with these per-caller and instance limits.
    pub fn new(per_caller: ConcurrentLimits, instance: InstanceLimits) -> Self {
        let pool = |slots: usize| Pool {
            slots: Arc::new(Semaphore::new(slots)),
            waiting: Arc::new(AtomicUsize::new(0)),
            queue: instance.queue,
            wait: instance.queue_wait,
        };
        Self {
            per_caller,
            callers: Arc::default(),
            fetches: pool(instance.fetches),
            pushes: pool(instance.pushes),
            stitches: pool(instance.stitches),
        }
    }

    /// A slot for `work` by the caller `key`: refused at once when the
    /// caller is at its cap, after waiting (bounded) when the instance is.
    /// Dropping the future gives back whatever it took.
    pub async fn enter(&self, work: Work, key: &str) -> Result<Slot, Busy> {
        let cap = match work {
            Work::Fetch => self.per_caller.fetches,
            Work::Push => self.per_caller.pushes,
            Work::Stitch => self.per_caller.stitches,
        };
        let caller = match cap {
            Some(max) => Some(self.count(work, key, max)?),
            None => None,
        };
        let pool = match work {
            Work::Fetch => &self.fetches,
            Work::Push => &self.pushes,
            Work::Stitch => &self.stitches,
        };
        let permit = pool.acquire().await.ok_or(Busy::Instance { work })?;
        Ok(Slot {
            _held: Arc::new(Held {
                _permit: permit,
                _caller: caller,
            }),
        })
    }

    fn count(&self, work: Work, key: &str, max: u32) -> Result<Counted, Busy> {
        let mut callers = lock(&self.callers);
        let entry = (work, key.to_owned());
        let n = callers.get(&entry).copied().unwrap_or(0);
        if n >= max {
            return Err(Busy::Caller { work, max });
        }
        callers.insert(entry.clone(), n + 1);
        Ok(Counted {
            callers: Arc::clone(&self.callers),
            entry,
        })
    }

    /// Requests of `work` this caller is running or waiting to run.
    pub fn running(&self, work: Work, key: &str) -> u32 {
        lock(&self.callers)
            .get(&(work, key.to_owned()))
            .copied()
            .unwrap_or(0)
    }
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// One kind's instance slots and its wait queue.
#[derive(Debug)]
struct Pool {
    slots: Arc<Semaphore>,
    waiting: Arc<AtomicUsize>,
    queue: usize,
    wait: Duration,
}

impl Pool {
    async fn acquire(&self) -> Option<OwnedSemaphorePermit> {
        if let Ok(permit) = Arc::clone(&self.slots).try_acquire_owned() {
            return Some(permit);
        }
        let queued = self.waiting.fetch_add(1, Ordering::AcqRel);
        let _place = Waiting(Arc::clone(&self.waiting));
        if queued >= self.queue {
            return None;
        }
        tokio::time::timeout(self.wait, Arc::clone(&self.slots).acquire_owned())
            .await
            .ok()?
            .ok()
    }
}

/// A place in a [`Pool`]'s queue, given back on drop (the wait ended, or
/// its request was dropped while waiting).
struct Waiting(Arc<AtomicUsize>);

impl Drop for Waiting {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

/// One request's place in its caller's count, given back on drop.
struct Counted {
    callers: Arc<Mutex<HashMap<(Work, String), u32>>>,
    entry: (Work, String),
}

impl Drop for Counted {
    fn drop(&mut self) {
        let mut callers = lock(&self.callers);
        if let Some(n) = callers.get_mut(&self.entry) {
            *n -= 1;
            if *n == 0 {
                callers.remove(&self.entry);
            }
        }
    }
}

struct Held {
    _permit: OwnedSemaphorePermit,
    _caller: Option<Counted>,
}

/// A running request's slot: its instance slot and its place in its
/// caller's count. Clones share it; it is released when the last clone is
/// dropped.
#[derive(Clone)]
pub struct Slot {
    _held: Arc<Held>,
}

impl fmt::Debug for Slot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Slot(..)")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits(fetches: Option<u32>) -> ConcurrentLimits {
        ConcurrentLimits {
            fetches,
            ..ConcurrentLimits::UNLIMITED
        }
    }

    fn instance(fetches: usize, queue: usize) -> InstanceLimits {
        InstanceLimits {
            fetches,
            queue,
            queue_wait: Duration::from_millis(200),
            ..InstanceLimits::default()
        }
    }

    /// Invariant: a caller runs at most its cap of a kind at once, other
    /// callers and other kinds are not affected, and a dropped slot (clones
    /// included) frees its place.
    #[tokio::test]
    async fn a_caller_runs_at_most_its_cap() {
        let a = Admission::new(limits(Some(2)), instance(100, 0));
        let one = a.enter(Work::Fetch, "ip:a").await.unwrap();
        let two = a.enter(Work::Fetch, "ip:a").await.unwrap();
        assert_eq!(
            a.enter(Work::Fetch, "ip:a").await.unwrap_err(),
            Busy::Caller {
                work: Work::Fetch,
                max: 2
            }
        );
        a.enter(Work::Fetch, "ip:b").await.unwrap();
        a.enter(Work::Push, "ip:a").await.unwrap();
        let clone = one.clone();
        drop(one);
        assert!(
            a.enter(Work::Fetch, "ip:a").await.is_err(),
            "a clone holds it"
        );
        drop(clone);
        let three = a.enter(Work::Fetch, "ip:a").await.unwrap();
        drop((two, three));
        assert_eq!(a.running(Work::Fetch, "ip:a"), 0);
        assert!(lock(&a.callers).is_empty(), "idle callers are forgotten");
    }

    /// Invariant: the instance runs at most its slots at once; with no
    /// queue the next request is refused at once, with a queue it waits
    /// for a slot and is refused when the wait runs out or the queue is
    /// full. A refused request gives its caller's place back.
    #[tokio::test]
    async fn the_instance_runs_at_most_its_slots_and_queues_a_few() {
        let a = Admission::new(limits(Some(10)), instance(1, 0));
        let held = a.enter(Work::Fetch, "ip:a").await.unwrap();
        let busy = Busy::Instance { work: Work::Fetch };
        assert_eq!(a.enter(Work::Fetch, "ip:b").await.unwrap_err(), busy);
        assert_eq!(a.running(Work::Fetch, "ip:b"), 0);
        drop(held);
        let held = a.enter(Work::Fetch, "ip:b").await.unwrap();

        let a = Arc::new(Admission::new(limits(None), instance(1, 1)));
        let first = a.enter(Work::Fetch, "k").await.unwrap();
        let waiter = {
            let a = Arc::clone(&a);
            tokio::spawn(async move { a.enter(Work::Fetch, "k").await.map(drop) })
        };
        tokio::time::sleep(Duration::from_millis(20)).await;
        // The queue's one place is taken.
        assert_eq!(a.enter(Work::Fetch, "k").await.unwrap_err(), busy);
        drop(first);
        waiter
            .await
            .unwrap()
            .expect("the waiter got the freed slot");
        let first = a.enter(Work::Fetch, "k").await.unwrap();
        // Nobody frees the slot: the wait runs out.
        assert_eq!(a.enter(Work::Fetch, "k").await.unwrap_err(), busy);
        drop((first, held));
    }

    /// Invariant: a request dropped while it waits (its client hung up)
    /// gives back its queue place and its caller's place.
    #[tokio::test]
    async fn a_dropped_waiter_gives_everything_back() {
        let mut i = instance(1, 1);
        i.queue_wait = Duration::from_secs(60);
        let a = Arc::new(Admission::new(limits(Some(5)), i));
        let held = a.enter(Work::Fetch, "k").await.unwrap();
        let waiter = {
            let a = Arc::clone(&a);
            tokio::spawn(async move { a.enter(Work::Fetch, "k").await.map(drop) })
        };
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(a.running(Work::Fetch, "k"), 2);
        waiter.abort();
        let _ = waiter.await;
        assert_eq!(a.running(Work::Fetch, "k"), 1);
        assert_eq!(a.fetches.waiting.load(Ordering::Acquire), 0);
        drop(held);
        a.enter(Work::Fetch, "k").await.unwrap();
    }

    #[test]
    fn refusals_say_what_and_when() {
        let caller = Busy::Caller {
            work: Work::Stitch,
            max: 1,
        };
        assert_eq!(
            caller.to_string(),
            "too many concurrent stitches for this caller (at most 1); retry in 5 s"
        );
        assert_eq!(
            Busy::Instance { work: Work::Push }.to_string(),
            "server busy: too many pushes running; retry in 5 s"
        );
    }
}
