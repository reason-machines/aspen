//! Rolling-window counters: "at most N events per key per window".
//!
//! Keys are client addresses (an IPv6 address by its /64, see
//! [`aspen_server::access::address_key`]), sessions, or caller and branch.
//! When the table is full, only keys whose window has emptied are
//! evicted; if none has, a new key is refused rather than resetting some
//! caller's spent budget (decisions log, B3 review obligation 2).

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

/// Counters per key (module docs).
#[derive(Debug)]
pub struct Windows {
    limit: usize,
    window_ms: u64,
    max_keys: usize,
    keys: HashMap<String, VecDeque<u64>>,
}

impl Windows {
    /// `limit` events per `window_ms` per key, for at most `max_keys` keys.
    pub fn new(limit: usize, window_ms: u64, max_keys: usize) -> Self {
        Self {
            limit,
            window_ms,
            max_keys,
            keys: HashMap::new(),
        }
    }

    fn trim(&mut self, key: &str, now: u64) -> usize {
        let window = self.window_ms;
        match self.keys.get_mut(key) {
            Some(q) => {
                while q.front().is_some_and(|&t| t + window <= now) {
                    q.pop_front();
                }
                q.len()
            }
            None => 0,
        }
    }

    /// Counts one event for `key` at `now`, or returns how many
    /// milliseconds until one would be admitted.
    pub fn admit(&mut self, key: &str, now: u64) -> Result<(), u64> {
        let used = self.trim(key, now);
        if used >= self.limit {
            let oldest = self.keys[key].front().copied().unwrap_or(now);
            return Err((oldest + self.window_ms).saturating_sub(now).max(1));
        }
        if !self.keys.contains_key(key) && self.keys.len() >= self.max_keys {
            let window = self.window_ms;
            self.keys
                .retain(|_, q| q.back().is_some_and(|&t| t + window > now));
            if self.keys.len() >= self.max_keys {
                return Err(self.window_ms);
            }
        }
        self.keys.entry(key.to_owned()).or_default().push_back(now);
        Ok(())
    }

    /// Events left for `key` in the current window, and when the oldest
    /// counted one leaves it (`now` when none is counted).
    pub fn remaining(&mut self, key: &str, now: u64) -> (usize, u64) {
        let used = self.trim(key, now);
        let reset = self
            .keys
            .get(key)
            .and_then(|q| q.front())
            .map_or(now, |&t| t + self.window_ms);
        (self.limit.saturating_sub(used), reset)
    }

    /// Forgets `key`.
    pub fn forget(&mut self, key: &str) {
        self.keys.remove(key);
    }
}

/// Long-lived requests (event streams) open at once: at most `per_key` per
/// key and `total` in all. A [`Slot`] holds one until it is dropped, so a
/// stream frees its slot however it ends.
#[derive(Debug)]
pub struct Slots {
    per_key: usize,
    total: usize,
    open: Mutex<OpenSlots>,
}

#[derive(Debug, Default)]
struct OpenSlots {
    total: usize,
    // Only keys with at least one open slot, so at most `total` entries.
    by_key: HashMap<String, usize>,
}

/// Why [`Slots::acquire`] refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotRefusal {
    /// The key has `per_key` slots open.
    PerKey,
    /// `total` slots are open.
    Total,
}

/// One open slot of [`Slots`]; dropping it frees the slot.
#[derive(Debug)]
pub struct Slot {
    slots: Arc<Slots>,
    key: String,
}

impl Slots {
    /// At most `per_key` open slots per key and `total` in all.
    pub fn new(per_key: usize, total: usize) -> Arc<Self> {
        Arc::new(Self {
            per_key,
            total,
            open: Mutex::default(),
        })
    }

    /// Takes a slot for `key`, checking both caps and counting it in one
    /// step.
    pub fn acquire(self: &Arc<Self>, key: &str) -> Result<Slot, SlotRefusal> {
        let mut open = self.open.lock().expect("not poisoned");
        if open.by_key.get(key).copied().unwrap_or(0) >= self.per_key {
            return Err(SlotRefusal::PerKey);
        }
        if open.total >= self.total {
            return Err(SlotRefusal::Total);
        }
        open.total += 1;
        *open.by_key.entry(key.to_owned()).or_default() += 1;
        Ok(Slot {
            slots: self.clone(),
            key: key.to_owned(),
        })
    }

    /// Slots open now, in all and for `key`.
    pub fn open(&self, key: &str) -> (usize, usize) {
        let open = self.open.lock().expect("not poisoned");
        (open.total, open.by_key.get(key).copied().unwrap_or(0))
    }
}

impl Drop for Slot {
    fn drop(&mut self) {
        let mut open = self.slots.open.lock().expect("not poisoned");
        open.total -= 1;
        if let Some(n) = open.by_key.get_mut(&self.key) {
            *n -= 1;
            if *n == 0 {
                open.by_key.remove(&self.key);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slots_are_capped_per_key_and_in_total_and_freed_on_drop() {
        let s = Slots::new(2, 3);
        let a1 = s.acquire("a").unwrap();
        let a2 = s.acquire("a").unwrap();
        assert_eq!(s.acquire("a").unwrap_err(), SlotRefusal::PerKey);
        let b1 = s.acquire("b").unwrap();
        assert_eq!(s.acquire("c").unwrap_err(), SlotRefusal::Total);
        assert_eq!(s.open("a"), (3, 2));
        drop(a1);
        assert_eq!(s.open("a"), (2, 1));
        let a3 = s.acquire("a").unwrap();
        drop((a2, a3, b1));
        assert_eq!(s.open("a"), (0, 0));
        assert!(
            s.open.lock().unwrap().by_key.is_empty(),
            "no empty keys kept"
        );
    }

    #[test]
    fn a_key_gets_its_limit_per_rolling_window() {
        let mut w = Windows::new(2, 1000, 10);
        assert!(w.admit("a", 0).is_ok());
        assert!(w.admit("a", 500).is_ok());
        assert_eq!(w.admit("a", 600), Err(400));
        assert!(w.admit("b", 600).is_ok(), "keys are independent");
        assert!(
            w.admit("a", 1000).is_ok(),
            "the first event left the window"
        );
        assert_eq!(w.remaining("a", 1000), (0, 1500));
    }

    #[test]
    fn a_full_table_evicts_only_idle_keys_and_never_resets_a_spent_one() {
        let mut w = Windows::new(1, 1000, 2);
        assert!(w.admit("a", 0).is_ok());
        assert!(w.admit("b", 0).is_ok());
        // Both keys are spent and live: a new key is refused, and "a" is
        // not evicted (which would reset its budget).
        assert!(w.admit("c", 10).is_err());
        assert!(w.admit("a", 20).is_err());
        // Once "a" and "b" are idle they make room.
        assert!(w.admit("c", 1000).is_ok());
    }
}
