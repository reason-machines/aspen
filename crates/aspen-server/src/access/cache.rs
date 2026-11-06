//! Short-lived in-memory copies of the rows every Git request reads before
//! it does any work: its repository (`repos`) and its token (`tokens`).
//!
//! Without them each HTTP request costs the metadata database two keyed
//! lookups, which under agent-scale push traffic is a large share of the
//! database's statements (decisions log, "throughput 6a"). A copy is kept
//! for at most its time to live (TTL), so a change made elsewhere (another
//! instance, `aspen token revoke`, an operator's `UPDATE`) is seen on every
//! instance within the TTL; a change made through this instance's own
//! [`Access`](super::Access) or [`CachedTokens`](super::tokens::CachedTokens)
//! drops the copy at once.
//!
//! Only rows that exist are kept. A miss is never remembered, so a
//! repository or token created by another process is usable on its very
//! next request, and a caller cannot fill the cache with names that do not
//! exist.

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Default TTL of a cached repository row (`ASPEN_REPO_CACHE_TTL_MS`).
pub const DEFAULT_REPO_TTL: Duration = Duration::from_secs(5);

/// Default TTL of a cached token row (`ASPEN_TOKEN_CACHE_TTL_MS`): a token
/// revoked on another instance or by `aspen token revoke` is refused
/// everywhere within this long.
pub const DEFAULT_TOKEN_TTL: Duration = Duration::from_secs(5);

/// Most rows one cache holds. Only rows that exist are cached, so this is
/// reached only by a deployment with more live repositories or tokens in
/// use within one TTL; past it the cache drops expired rows, then all of
/// them, and keeps going.
pub const CAPACITY: usize = 65_536;

/// How long the access layer keeps each kind of row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AccessCaches {
    /// Repository rows ([`Access::repo`](super::Access::repo)). Zero turns
    /// the cache off: every request reads its repository.
    pub repo_ttl: Duration,
    /// Token rows ([`CachedTokens`](super::tokens::CachedTokens)). Zero
    /// turns the cache off: every request reads its token.
    pub token_ttl: Duration,
}

impl Default for AccessCaches {
    fn default() -> Self {
        Self {
            repo_ttl: DEFAULT_REPO_TTL,
            token_ttl: DEFAULT_TOKEN_TTL,
        }
    }
}

/// A map whose entries expire `ttl` after the lookup that produced them
/// began.
///
/// A lookup reads [`TtlCache::get`]; on a miss it takes a [`Ticket`] with
/// [`TtlCache::ticket`] *before* it reads the store, and hands the row to
/// [`TtlCache::insert`] with it. An invalidation in between makes that
/// insert a no-op, so a row read before a local change can never be cached
/// after it. Expiry counts from the ticket, not from the insert, so a row
/// read just before a change elsewhere is gone `ttl` after that change at
/// the latest.
#[derive(Debug)]
pub(crate) struct TtlCache<K, V> {
    ttl: Duration,
    capacity: usize,
    inner: Mutex<Inner<K, V>>,
}

#[derive(Debug)]
struct Inner<K, V> {
    entries: HashMap<K, (Instant, V)>,
    /// Bumped by every invalidation.
    generation: u64,
}

/// Proof that a lookup began at `at`, before any invalidation newer than
/// `generation`.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Ticket {
    at: Instant,
    generation: u64,
}

impl<K: Eq + Hash, V: Clone> TtlCache<K, V> {
    /// An empty cache; a zero `ttl` keeps nothing.
    pub(crate) fn new(ttl: Duration, capacity: usize) -> Self {
        Self {
            ttl,
            capacity,
            inner: Mutex::new(Inner {
                entries: HashMap::new(),
                generation: 0,
            }),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner<K, V>> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// The cached row for `key`, if it has not expired.
    pub(crate) fn get(&self, key: &K) -> Option<V> {
        if self.ttl.is_zero() {
            return None;
        }
        let mut inner = self.lock();
        match inner.entries.get(key) {
            Some((at, value)) if at.elapsed() < self.ttl => Some(value.clone()),
            Some(_) => {
                inner.entries.remove(key);
                None
            }
            None => None,
        }
    }

    /// Call before reading the store on a miss.
    pub(crate) fn ticket(&self) -> Ticket {
        Ticket {
            at: Instant::now(),
            generation: self.lock().generation,
        }
    }

    /// Keeps `value`, read from the store after `ticket` was taken, unless
    /// something was invalidated since.
    pub(crate) fn insert(&self, key: K, value: V, ticket: Ticket) {
        if self.ttl.is_zero() {
            return;
        }
        let mut inner = self.lock();
        if inner.generation != ticket.generation {
            return;
        }
        if inner.entries.len() >= self.capacity && !inner.entries.contains_key(&key) {
            let ttl = self.ttl;
            inner.entries.retain(|_, (at, _)| at.elapsed() < ttl);
            if inner.entries.len() >= self.capacity {
                inner.entries.clear();
            }
        }
        inner.entries.insert(key, (ticket.at, value));
    }

    /// Drops `key`, and refuses every insert of a lookup that began before
    /// now.
    pub(crate) fn remove(&self, key: &K) {
        let mut inner = self.lock();
        inner.generation = inner.generation.wrapping_add(1);
        inner.entries.remove(key);
    }

    /// Drops everything, and refuses every insert of a lookup that began
    /// before now.
    pub(crate) fn clear(&self) {
        let mut inner = self.lock();
        inner.generation = inner.generation.wrapping_add(1);
        inner.entries.clear();
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.lock().entries.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const LONG: Duration = Duration::from_secs(3600);

    #[test]
    fn a_row_is_served_until_its_ttl_runs_out() {
        let c = TtlCache::new(Duration::from_millis(50), 8);
        let t = c.ticket();
        c.insert("a", 1, t);
        assert_eq!(c.get(&"a"), Some(1));
        std::thread::sleep(Duration::from_millis(60));
        assert_eq!(c.get(&"a"), None, "expired");
        assert_eq!(c.len(), 0, "an expired row is dropped when read");
    }

    /// Expiry counts from when the lookup began, so a slow store read
    /// cannot stretch how long a stale row lives past the TTL.
    #[test]
    fn expiry_counts_from_the_ticket_not_the_insert() {
        let c = TtlCache::new(Duration::from_millis(50), 8);
        let t = c.ticket();
        std::thread::sleep(Duration::from_millis(60));
        c.insert("a", 1, t);
        assert_eq!(c.get(&"a"), None);
    }

    /// Invariant: a row read before a local invalidation is never cached
    /// after it (a revoke racing a lookup of the same token).
    #[test]
    fn an_invalidation_refuses_lookups_already_in_flight() {
        let c = TtlCache::new(LONG, 8);
        let before = c.ticket();
        c.remove(&"a");
        c.insert("a", 1, before);
        assert_eq!(c.get(&"a"), None);
        let before = c.ticket();
        c.clear();
        c.insert("a", 1, before);
        assert_eq!(c.get(&"a"), None);
        // A lookup that begins after the invalidation is cached again.
        let after = c.ticket();
        c.insert("a", 2, after);
        assert_eq!(c.get(&"a"), Some(2));
    }

    #[test]
    fn a_zero_ttl_keeps_nothing() {
        let c = TtlCache::new(Duration::ZERO, 8);
        let t = c.ticket();
        c.insert("a", 1, t);
        assert_eq!(c.get(&"a"), None);
        assert_eq!(c.len(), 0);
    }

    #[test]
    fn the_cache_never_holds_more_than_its_capacity() {
        let c = TtlCache::new(LONG, 4);
        for i in 0..100 {
            let t = c.ticket();
            c.insert(i, i, t);
            assert!(c.len() <= 4, "{} rows after {i}", c.len());
            assert_eq!(c.get(&i), Some(i), "the newest row is kept");
        }
    }
}
