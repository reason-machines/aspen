//! Rate limits: rolling-window counters per key ([`Windows`]), open
//! slots per key ([`Slots`]) and one server-wide token bucket
//! ([`TokenBucket`]).
//!
//! [`Windows`] counts "at most N events per key per window". Keys are
//! client addresses (an IPv6 address by its /64, see
//! [`aspen_server::access::address_key`]), sessions, or caller and branch.
//! When the table is full, only keys whose window has emptied are
//! evicted; if none has, a new key is refused rather than resetting some
//! caller's spent budget (decisions log, B3 review obligation 2).

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// A token bucket: `per_second` tokens a second, at most `burst` saved
/// up, one taken per event. Starts full.
///
/// `place` keeps one for every push the server takes from outside, web
/// strokes and Git updates together ([`crate::hook::Shared::take_push`]),
/// so the sustained rate of pushes it commits and merges has a ceiling
/// however many callers there are.
///
/// It is kept as a theoretical arrival time (GCRA) in whole nanoseconds,
/// not a fractional token count, so the count it admits is exact: one
/// event per `interval`, and up to `burst` at once after a quiet spell.
#[derive(Debug)]
pub struct TokenBucket {
    per_second: f64,
    /// Time one token takes to refill.
    interval: Duration,
    /// How far the arrival time may run ahead of now: `burst - 1` tokens.
    tolerance: Duration,
    base: Instant,
    /// When the bucket is next full, as time since `base`; at or before
    /// now means full.
    full_at: Duration,
}

/// The slowest rate a [`TokenBucket`] takes: one token every 1000 s.
pub const MIN_BUCKET_PER_SECOND: f64 = 0.001;

/// The longest a [`TokenBucket`]'s burst may take to refill, in seconds
/// (`burst / per_second`): about 11.5 days. This is a sanity cap against
/// settings that are surely mistakes, not an arithmetic limit: with the
/// rate at or above [`MIN_BUCKET_PER_SECOND`], even a burst of `u32::MAX`
/// fits the bucket's `Duration` arithmetic.
pub const MAX_BUCKET_REFILL_SECONDS: f64 = 1e6;

/// Why a rate and burst cannot make a [`TokenBucket`]
/// ([`bucket_problem`]).
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BucketProblem {
    /// The rate is not a number, or below [`MIN_BUCKET_PER_SECOND`].
    Rate(f64),
    /// The burst is 0.
    Burst,
    /// The burst takes longer than [`MAX_BUCKET_REFILL_SECONDS`] to refill
    /// at the rate.
    Refill {
        /// The rate.
        per_second: f64,
        /// The burst.
        burst: u32,
    },
}

impl std::fmt::Display for BucketProblem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            Self::Rate(r) => write!(
                f,
                "the rate must be a number of at least {MIN_BUCKET_PER_SECOND} a second, not {r}"
            ),
            Self::Burst => f.write_str("the burst must be at least 1"),
            Self::Refill { per_second, burst } => write!(
                f,
                "a burst of {burst} at {per_second} a second takes {:.0} s to refill, \
                 more than {MAX_BUCKET_REFILL_SECONDS:.0} s; lower the burst or raise the rate",
                f64::from(burst) / per_second
            ),
        }
    }
}

/// What, if anything, keeps `per_second` and `burst` from making a
/// [`TokenBucket`]: the rate must be finite and at least
/// [`MIN_BUCKET_PER_SECOND`], the burst at least 1, and the burst must
/// refill within [`MAX_BUCKET_REFILL_SECONDS`]. Within these bounds the
/// bucket's durations fit easily; far outside them, its arithmetic
/// overflows.
pub fn bucket_problem(per_second: f64, burst: u32) -> Option<BucketProblem> {
    if !(per_second.is_finite() && per_second >= MIN_BUCKET_PER_SECOND) {
        return Some(BucketProblem::Rate(per_second));
    }
    if burst == 0 {
        return Some(BucketProblem::Burst);
    }
    if f64::from(burst) / per_second > MAX_BUCKET_REFILL_SECONDS {
        return Some(BucketProblem::Refill { per_second, burst });
    }
    None
}

impl TokenBucket {
    /// A full bucket of `burst` tokens refilled at `per_second`.
    ///
    /// # Panics
    ///
    /// If [`bucket_problem`] finds a problem with `per_second` and `burst`.
    pub fn new(per_second: f64, burst: u32, now: Instant) -> Self {
        if let Some(problem) = bucket_problem(per_second, burst) {
            panic!("a token bucket cannot be made: {problem}");
        }
        let interval = Duration::from_secs_f64(1.0 / per_second).max(Duration::from_nanos(1));
        Self {
            per_second,
            interval,
            tolerance: interval * (burst - 1),
            base: now,
            full_at: Duration::ZERO,
        }
    }

    /// The sustained rate.
    pub fn per_second(&self) -> f64 {
        self.per_second
    }

    /// `now` as time since `base`; an `Instant` from before `base` counts
    /// as `base`.
    fn since(&self, now: Instant) -> Duration {
        now.saturating_duration_since(self.base)
    }

    /// Takes one token, or returns how long until one is there.
    pub fn take(&mut self, now: Instant) -> Result<(), Duration> {
        let now = self.since(now);
        let full_at = self.full_at.max(now);
        let ahead = full_at - now;
        if ahead > self.tolerance {
            return Err(ahead - self.tolerance);
        }
        self.full_at = full_at + self.interval;
        Ok(())
    }

    /// Tokens a [`Self::take`] at `now` would find, without taking one
    /// (0 when it would refuse).
    pub fn available(&self, now: Instant) -> u64 {
        let now = self.since(now);
        let ahead = self.full_at.max(now) - now;
        match self.tolerance.checked_sub(ahead) {
            None => 0,
            Some(spare) => (spare.as_nanos() / self.interval.as_nanos()) as u64 + 1,
        }
    }

    /// Returns a token taken for an event that then did not happen. A full
    /// bucket stays full.
    pub fn give_back(&mut self, now: Instant) {
        let now = self.since(now);
        self.full_at = self.full_at.saturating_sub(self.interval).max(now);
    }
}

/// How often, at most, [`PushCeiling`] asks for a log line about its
/// refusals.
pub const REFUSAL_REPORT_EVERY: Duration = Duration::from_secs(10);

/// The server-wide push ceiling: a [`TokenBucket`] plus a count of what it
/// refused, reported in a log line at most every
/// [`REFUSAL_REPORT_EVERY`], so a flood past the ceiling leaves a trace
/// without a line per push.
#[derive(Debug)]
pub struct PushCeiling {
    bucket: TokenBucket,
    burst: u32,
    refused: u64,
    unreported: u64,
    reported_at: Option<Instant>,
}

/// One refusal of [`PushCeiling::take`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Refused {
    /// How long until a push fits again.
    pub wait: Duration,
    /// Set when this refusal is due a log line: refusals since the last
    /// line (this one included) and in all since the start.
    pub report: Option<(u64, u64)>,
}

impl PushCeiling {
    /// A full ceiling ([`TokenBucket::new`]).
    pub fn new(per_second: f64, burst: u32, now: Instant) -> Self {
        Self {
            bucket: TokenBucket::new(per_second, burst, now),
            burst,
            refused: 0,
            unreported: 0,
            reported_at: None,
        }
    }

    /// The sustained rate.
    pub fn per_second(&self) -> f64 {
        self.bucket.per_second()
    }

    /// The burst.
    pub fn burst(&self) -> u32 {
        self.burst
    }

    /// Pushes refused since the start.
    pub fn refused(&self) -> u64 {
        self.refused
    }

    /// Takes one push. The first refusal is reported at once, and later
    /// ones in a line at most every [`REFUSAL_REPORT_EVERY`]: refusals that
    /// end a spell wait for the next refusal's line, which counts them.
    pub fn take(&mut self, now: Instant) -> Result<(), Refused> {
        let wait = match self.bucket.take(now) {
            Ok(()) => return Ok(()),
            Err(wait) => wait,
        };
        self.refused += 1;
        self.unreported += 1;
        let due = self
            .reported_at
            .is_none_or(|at| now.saturating_duration_since(at) >= REFUSAL_REPORT_EVERY);
        let report = due.then(|| {
            self.reported_at = Some(now);
            (std::mem::take(&mut self.unreported), self.refused)
        });
        Err(Refused { wait, report })
    }

    /// Pushes the ceiling would let through at `now`, without taking one
    /// ([`TokenBucket::available`]). The personas read it to back off.
    pub fn available(&self, now: Instant) -> u64 {
        self.bucket.available(now)
    }

    /// Returns a push taken for one a later check refused
    /// ([`TokenBucket::give_back`]).
    pub fn give_back(&mut self, now: Instant) {
        self.bucket.give_back(now);
    }
}

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

    /// Invariant: past the burst, the bucket admits `per_second` events a
    /// second and refuses the rest, saying when the next one fits; a token
    /// given back is taken again, and saving up stops at the burst.
    #[test]
    fn the_bucket_refuses_above_its_rate_and_says_when_to_retry() {
        let t0 = Instant::now();
        let ms = |n: u64| t0 + Duration::from_millis(n);
        let mut b = TokenBucket::new(10.0, 3, t0);
        for _ in 0..3 {
            assert!(b.take(t0).is_ok(), "the burst is there at the start");
        }
        let wait = b.take(t0).unwrap_err();
        assert_eq!(wait, Duration::from_millis(100), "one token every 100 ms");
        assert!(b.take(ms(60)).is_err(), "0.6 of a token is not one");
        assert!(b.take(ms(100)).is_ok());
        assert!(b.take(ms(100)).is_err());
        // A full second admits exactly `per_second` more, however often
        // callers knock.
        let mut admitted = 0;
        for n in 101..=1100 {
            admitted += usize::from(b.take(ms(n)).is_ok());
        }
        assert_eq!(admitted, 10);
        // `available` reads without taking.
        assert_eq!(b.available(ms(1100)), 0);
        assert_eq!(b.available(ms(1200)), 1);
        assert_eq!(b.available(ms(1200)), 1);
        assert_eq!(b.available(ms(9000)), 3, "at most the burst");
        // A token given back is there for the next caller.
        b.give_back(ms(1100));
        assert!(b.take(ms(1100)).is_ok());
        assert!(b.take(ms(1100)).is_err());
        // An idle hour saves up only the burst.
        let later = ms(3_600_000);
        for _ in 0..3 {
            assert!(b.take(later).is_ok());
        }
        assert!(b.take(later).is_err());
        b.give_back(later);
        b.give_back(later);
        b.give_back(later);
        b.give_back(later);
        for _ in 0..3 {
            assert!(b.take(later).is_ok());
        }
        assert!(
            b.take(later).is_err(),
            "giving back never exceeds the burst"
        );
    }

    /// Invariant: a rate and burst are refused, with the reason, when the
    /// bucket's arithmetic could fail (a rate that is not a number, at or
    /// below 0, or below the floor; a burst of 0) and when the burst takes
    /// longer than the sanity cap to refill. Values at each bound make a
    /// working bucket.
    #[test]
    fn a_bucket_refuses_rates_and_bursts_outside_its_bounds() {
        for rate in [f64::NAN, f64::INFINITY, -1.0, 0.0, 5e-20, 1e-16, 0.000_999] {
            match bucket_problem(rate, 1) {
                Some(BucketProblem::Rate(r)) => assert!(r == rate || (r.is_nan() && rate.is_nan())),
                other => panic!("{rate}: {other:?}"),
            }
        }
        assert_eq!(bucket_problem(550.0, 0), Some(BucketProblem::Burst));
        let p = bucket_problem(0.001, 1001).expect("1 001 000 s to refill");
        assert!(matches!(p, BucketProblem::Refill { .. }), "{p:?}");
        assert!(p.to_string().contains("1001000 s to refill"), "{p}");
        assert!(
            matches!(
                bucket_problem(1.0, u32::MAX),
                Some(BucketProblem::Refill { .. })
            ),
            "about 136 years"
        );
        // At each bound, and at extremes inside them, the bucket works.
        let t0 = Instant::now();
        for (rate, burst) in [
            (MIN_BUCKET_PER_SECOND, 1),
            (MIN_BUCKET_PER_SECOND, 1000),
            (1e300, u32::MAX),
            (f64::MAX, u32::MAX),
            (550.0, 2750),
        ] {
            assert_eq!(bucket_problem(rate, burst), None, "{rate} {burst}");
            let mut b = TokenBucket::new(rate, burst, t0);
            assert!(b.take(t0).is_ok(), "{rate} {burst}");
            b.give_back(t0 + Duration::from_secs(1));
        }
    }

    /// Invariant: every refusal is counted, and a flood of them is reported
    /// in one line per [`REFUSAL_REPORT_EVERY`], each counting every refusal
    /// since the line before.
    #[test]
    fn the_ceiling_counts_refusals_and_reports_them_at_most_every_ten_seconds() {
        let t0 = Instant::now();
        let ms = |n: u64| t0 + Duration::from_millis(n);
        // One push a minute: nothing refills during the test.
        let mut c = PushCeiling::new(1.0 / 60.0, 1, t0);
        assert!(c.take(t0).is_ok());
        assert_eq!(
            c.take(ms(1)).unwrap_err().report,
            Some((1, 1)),
            "the first is reported at once"
        );
        for n in 2..1000 {
            assert_eq!(c.take(ms(n)).unwrap_err().report, None);
        }
        let r = c.take(ms(10_001)).unwrap_err();
        assert_eq!(r.report, Some((999, 1000)), "one line for the 999 since");
        assert!(r.wait > Duration::from_secs(49) && r.wait <= Duration::from_secs(50));
        assert_eq!(c.take(ms(10_002)).unwrap_err().report, None);
        assert_eq!(c.refused(), 1001);
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
