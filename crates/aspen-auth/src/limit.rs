//! Rate limits and push quotas.
//!
//! Both are token buckets kept in this process's memory, keyed by caller
//! (the principal for signed-in callers, the client address for anonymous
//! ones; the server picks the key). With several server instances behind a
//! load balancer each instance keeps its own buckets, so the effective
//! limit is the configured one times the number of instances that a caller
//! reaches (decisions log, B3).
//!
//! - **Request rates** ([`RateLimit`]): requests per second with a burst,
//!   one bucket for reads (clone, fetch, `ls-refs`) and one for pushes.
//! - **Push bytes** ([`PushQuota`]): a cap on one push's pack size, a cap on
//!   its object count (checked by receive-pack against the pack header,
//!   before the body is read), and a rolling budget of pushed bytes per
//!   caller. The budget is charged as the body streams in, so a push that
//!   runs out fails before its pack is complete, and an incomplete pack is
//!   never stored.
//!
//! - **Failed authentications** ([`LimitsConfig::auth_failures`]): a
//!   bucket per client address that every presented credential which
//!   fails to authenticate drains by one. Each credential takes its token
//!   before it is checked and gives it back if it authenticates
//!   ([`Limits::charge_credential`]), so while the bucket is empty a
//!   request that presents a credential is refused before the credential
//!   is checked, and requests that arrive together cannot all pass on the
//!   same token: a stream of bad tokens, JWTs or SSH keys costs at most the
//!   bucket's level in metadata reads and signature checks per address,
//!   however many run at once (issue #198). On by default
//!   ([`DEFAULT_AUTH_FAILURES`]); successful authentications cost nothing
//!   once they finish.
//!
//! - **Concurrent work** ([`ConcurrentLimits`]): how many fetches, pushes
//!   and stitches (building a fetch's inline pack) one caller may run at
//!   once. Only the configuration lives here; the server counts running
//!   work per caller and per instance (`aspen_server::admission`, issue
//!   #118). On by default ([`DEFAULT_CONCURRENT`]).
//!
//! Memory is bounded: at most `max_keys` buckets per limiter. When it is
//! reached, only buckets that have refilled completely are dropped (a full
//! bucket is the same as no bucket). If none has, the new caller is refused
//! until the first one does, and every caller already tracked keeps its
//! bucket: dropping a drained bucket would hand that caller a fresh
//! allowance, so a caller churning through addresses could reset its own
//! budget (decisions log, D4).

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use serde::Deserialize;

/// A request rate: `per_second` sustained, up to `burst` at once.
#[derive(Debug, Clone, Copy, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RateLimit {
    /// Bucket size: requests allowed at once after a quiet period.
    pub burst: u32,
    /// Refill rate.
    pub per_second: f64,
}

/// Limits on pushes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PushQuota {
    /// Largest pack one push may send, in bytes.
    pub max_pack_bytes: Option<u64>,
    /// Most objects one push's pack may hold (its header count).
    pub max_objects: Option<u64>,
    /// Bytes one caller may push per hour, as a rolling budget.
    pub bytes_per_hour: Option<u64>,
}

/// Every limit, as configured. Absent fields mean no limit, except
/// `auth_failures`, which defaults to [`DEFAULT_AUTH_FAILURES`].
#[derive(Debug, Clone, Copy, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LimitsConfig {
    /// Read requests per caller.
    pub read: Option<RateLimit>,
    /// Push requests per caller.
    pub push: Option<RateLimit>,
    /// Push sizes and budgets.
    #[serde(default)]
    pub quota: PushQuota,
    /// Failed authentications per client address. Absent means
    /// [`DEFAULT_AUTH_FAILURES`]; `null` turns the limit off.
    #[serde(default = "default_auth_failures")]
    pub auth_failures: Option<RateLimit>,
    /// Most callers tracked per limiter (default 100,000).
    pub max_keys: Option<usize>,
    /// Work one caller may run at once. Absent fields mean their
    /// [`DEFAULT_CONCURRENT`] value; `null` turns that cap off.
    #[serde(default)]
    pub concurrent: ConcurrentLimits,
}

/// How many requests of each kind one caller (the same key as the rate
/// limits) may have running at once, over HTTP and SSH together. `None`
/// is no cap. A request over a cap is refused at once (HTTP `429`), never
/// queued: only the instance-wide limits queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConcurrentLimits {
    /// Fetch requests: `git-upload-pack` endpoints over HTTP (each request
    /// while the server reads, plans and answers it) and SSH sessions.
    #[serde(default = "default_concurrent_fetches")]
    pub fetches: Option<u32>,
    /// Push requests: `git-receive-pack` endpoints over HTTP and SSH.
    #[serde(default = "default_concurrent_pushes")]
    pub pushes: Option<u32>,
    /// Inline packs being stitched and streamed to the caller.
    #[serde(default = "default_concurrent_stitches")]
    pub stitches: Option<u32>,
}

/// The default per-caller caps on concurrent work: 16 fetches, 4 pushes
/// and 8 stitches. A Git client runs one request at a time per command, so
/// only parallel jobs (CI fan-out, agents) come near them.
pub const DEFAULT_CONCURRENT: ConcurrentLimits = ConcurrentLimits {
    fetches: Some(16),
    pushes: Some(4),
    stitches: Some(8),
};

fn default_concurrent_fetches() -> Option<u32> {
    DEFAULT_CONCURRENT.fetches
}

fn default_concurrent_pushes() -> Option<u32> {
    DEFAULT_CONCURRENT.pushes
}

fn default_concurrent_stitches() -> Option<u32> {
    DEFAULT_CONCURRENT.stitches
}

impl Default for ConcurrentLimits {
    /// [`DEFAULT_CONCURRENT`].
    fn default() -> Self {
        DEFAULT_CONCURRENT
    }
}

impl ConcurrentLimits {
    /// No caps at all.
    pub const UNLIMITED: Self = Self {
        fetches: None,
        pushes: None,
        stitches: None,
    };
}

/// The default limit on failed authentications per client address: 100 at
/// once, then one a second. A Git client fails at most once per request
/// (a mistyped or revoked token) and an SSH client once per unregistered
/// key its agent offers, so honest callers stay far below it.
pub const DEFAULT_AUTH_FAILURES: RateLimit = RateLimit {
    burst: 100,
    per_second: 1.0,
};

fn default_auth_failures() -> Option<RateLimit> {
    Some(DEFAULT_AUTH_FAILURES)
}

impl Default for LimitsConfig {
    /// No request rates and no push quotas; the default limits on failed
    /// authentications and on concurrent work.
    fn default() -> Self {
        Self {
            read: None,
            push: None,
            quota: PushQuota::default(),
            auth_failures: default_auth_failures(),
            max_keys: None,
            concurrent: ConcurrentLimits::default(),
        }
    }
}

/// A limiter refusal: try again after this long.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryAfter(pub Duration);

#[derive(Debug, Clone, Copy)]
struct Bucket {
    level: f64,
    updated: Instant,
}

/// Token buckets per key.
#[derive(Debug)]
pub struct Limiter {
    capacity: f64,
    per_second: f64,
    max_keys: usize,
    table: Mutex<Table>,
}

#[derive(Debug, Default)]
struct Table {
    buckets: HashMap<String, Bucket>,
    /// While the table is full: no bucket can have refilled before this
    /// instant, so a new key is refused without scanning the table.
    /// Buckets only drain while the table is full (no key can be added),
    /// except for a refund ([`Limiter::give_back`]), which clears this, so
    /// a later scan never finds a bucket refilled sooner.
    full_until: Option<Instant>,
}

/// Default for [`LimitsConfig::max_keys`].
pub const DEFAULT_MAX_KEYS: usize = 100_000;

impl Limiter {
    /// A limiter whose buckets hold `capacity` and refill at `per_second`.
    /// A capacity or rate that is not a positive finite number allows
    /// nothing.
    pub fn new(capacity: f64, per_second: f64, max_keys: usize) -> Self {
        let sane = |v: f64| if v.is_finite() && v > 0.0 { v } else { 0.0 };
        Self {
            capacity: sane(capacity),
            per_second: sane(per_second),
            max_keys: max_keys.max(1),
            table: Mutex::new(Table::default()),
        }
    }

    /// A request-rate limiter.
    pub fn rate(limit: RateLimit, max_keys: usize) -> Self {
        Self::new(f64::from(limit.burst), limit.per_second, max_keys)
    }

    /// A byte budget of `per_hour`, all of which may be spent at once.
    pub fn bytes_per_hour(per_hour: u64, max_keys: usize) -> Self {
        // u64 to f64 loses precision only above 2^53 bytes, far past any
        // budget.
        let per_hour = per_hour as f64;
        Self::new(per_hour, per_hour / 3600.0, max_keys)
    }

    /// Takes `cost` from `key`'s bucket if it holds that much; otherwise
    /// takes nothing and says how long until it would.
    ///
    /// A key that is not tracked while the table is full and no bucket has
    /// refilled is refused (module docs), with the time until one will.
    pub fn try_take(&self, key: &str, cost: f64, now: Instant) -> Result<(), RetryAfter> {
        let mut table = self.table.lock().unwrap_or_else(|e| e.into_inner());
        if !table.buckets.contains_key(key) && table.buckets.len() >= self.max_keys {
            self.evict(&mut table, now)?;
        }
        let bucket = table.buckets.entry(key.to_owned()).or_insert(Bucket {
            level: self.capacity,
            updated: now,
        });
        self.refill(bucket, now);
        if cost <= bucket.level {
            bucket.level -= cost;
            Ok(())
        } else {
            Err(RetryAfter(self.wait_for(cost, bucket.level)))
        }
    }

    /// Whether `cost` could be taken from `key`'s bucket now, without
    /// taking it: what [`Limiter::try_take`] would answer, a key that is
    /// not tracked while the table is full and no bucket has refilled
    /// included. Unlike [`Limiter::available`], which gives an untracked
    /// key a full bucket, this never admits what `try_take` would refuse.
    pub fn check(&self, key: &str, cost: f64, now: Instant) -> Result<(), RetryAfter> {
        let mut table = self.table.lock().unwrap_or_else(|e| e.into_inner());
        let level = if let Some(b) = table.buckets.get_mut(key) {
            self.refill(b, now);
            b.level
        } else {
            if table.buckets.len() >= self.max_keys {
                self.evict(&mut table, now)?;
            }
            self.capacity
        };
        if cost <= level {
            Ok(())
        } else {
            Err(RetryAfter(self.wait_for(cost, level)))
        }
    }

    /// Gives `cost` back to `key`'s bucket: undoes a [`Limiter::try_take`]
    /// whose reason went away, never above the bucket's capacity. A key
    /// that is not tracked already has a full bucket and is left alone; a
    /// bucket that the refund fills is dropped (a full bucket is the same
    /// as no bucket), so keys that only ever take and give back do not
    /// fill the table.
    pub fn give_back(&self, key: &str, cost: f64, now: Instant) {
        let mut table = self.table.lock().unwrap_or_else(|e| e.into_inner());
        let Some(bucket) = table.buckets.get_mut(key) else {
            return;
        };
        self.refill(bucket, now);
        bucket.level += cost;
        if bucket.level >= self.capacity {
            table.buckets.remove(key);
        }
        // The bucket refills sooner than `full_until` assumed (or is gone,
        // leaving room): the next new key scans again.
        table.full_until = None;
    }

    /// What `key`'s bucket holds now; a full bucket for a key that is not
    /// tracked, even when the table has no room for it
    /// ([`Limiter::check`] answers whether it may take).
    pub fn available(&self, key: &str, now: Instant) -> f64 {
        let mut table = self.table.lock().unwrap_or_else(|e| e.into_inner());
        match table.buckets.get_mut(key) {
            Some(b) => {
                self.refill(b, now);
                b.level
            }
            None => self.capacity,
        }
    }

    /// How many keys are tracked.
    pub fn tracked(&self) -> usize {
        self.table
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .buckets
            .len()
    }

    fn refill(&self, b: &mut Bucket, now: Instant) {
        let elapsed = now.saturating_duration_since(b.updated).as_secs_f64();
        b.level = (b.level + elapsed * self.per_second).min(self.capacity);
        b.updated = b.updated.max(now);
    }

    fn wait_for(&self, cost: f64, level: f64) -> Duration {
        let missing = cost - level;
        if self.per_second <= 0.0 || cost > self.capacity {
            // Never refills enough: a cost larger than the bucket, or no
            // refill at all. Tell the client to back off for an hour.
            return Duration::from_secs(3600);
        }
        Duration::from_secs_f64(missing / self.per_second).max(Duration::from_secs(1))
    }

    /// Makes room for one key by dropping every bucket that has refilled
    /// completely; if none has, refuses the new key until the first one
    /// will have.
    fn evict(&self, table: &mut Table, now: Instant) -> Result<(), RetryAfter> {
        let refuse = |until: Instant| {
            RetryAfter(
                until
                    .saturating_duration_since(now)
                    .max(Duration::from_secs(1)),
            )
        };
        if let Some(until) = table.full_until
            && now < until
        {
            return Err(refuse(until));
        }
        table.full_until = None;
        table.buckets.retain(|_, b| {
            let elapsed = now.saturating_duration_since(b.updated).as_secs_f64();
            b.level + elapsed * self.per_second < self.capacity
        });
        if table.buckets.len() < self.max_keys {
            return Ok(());
        }
        // Every bucket is still drained: the earliest any refills.
        let until = if self.per_second > 0.0 {
            let soonest = table
                .buckets
                .values()
                .map(|b| {
                    let elapsed = now.saturating_duration_since(b.updated).as_secs_f64();
                    let missing = self.capacity - (b.level + elapsed * self.per_second);
                    missing / self.per_second
                })
                .fold(f64::INFINITY, f64::min);
            now + Duration::try_from_secs_f64(soonest).unwrap_or(Duration::from_secs(3600))
        } else {
            now + Duration::from_secs(3600)
        };
        table.full_until = Some(until);
        Err(refuse(until))
    }
}

/// Which bucket a request draws on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestClass {
    /// Clone, fetch, `ls-refs`, and their advertisement.
    Read,
    /// Push, and its advertisement.
    Push,
}

/// Why a request was refused by a limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum LimitError {
    /// Too many requests; retry after the given time (HTTP 429).
    #[error("rate limit exceeded; retry in {} s", .0.0.as_secs().max(1))]
    Rate(RetryAfter),
    /// The caller's push byte budget is spent (HTTP 429).
    #[error("push quota exceeded; retry in {} s", .0.0.as_secs().max(1))]
    Quota(RetryAfter),
    /// Too many failed authentications from this address; retry after the
    /// given time (HTTP 429).
    #[error("too many failed authentications; retry in {} s", .0.0.as_secs().max(1))]
    AuthFailures(RetryAfter),
    /// The push is larger than one push may be (HTTP 413).
    #[error("push too large: at most {max} bytes per push")]
    TooLarge {
        /// The cap.
        max: u64,
    },
}

/// One failed authentication taken from an address's bucket by
/// [`Limits::charge_credential`] while its credential is checked. Dropping
/// it keeps the charge: the credential failed (or its check was cancelled
/// and may have cost a store read). [`CredentialCharge::refund`] gives it
/// back.
#[derive(Debug)]
#[must_use = "dropping a charge counts it as a failed authentication; refund it on success"]
pub struct CredentialCharge<'a> {
    limiter: Option<&'a Limiter>,
    address: &'a str,
}

impl CredentialCharge<'_> {
    /// The credential authenticated, or could not be checked (a store
    /// outage is not the client's failure): gives the token back, so a
    /// success costs nothing once it is done.
    pub fn refund(self, now: Instant) {
        if let Some(l) = self.limiter {
            l.give_back(self.address, 1.0, now);
        }
    }
}

/// Every limiter of a server.
#[derive(Debug)]
pub struct Limits {
    read: Option<Limiter>,
    push: Option<Limiter>,
    push_bytes: Option<Limiter>,
    auth_failures: Option<Limiter>,
    quota: PushQuota,
    concurrent: ConcurrentLimits,
}

impl Limits {
    /// Limiters for a configuration.
    pub fn new(config: LimitsConfig) -> Self {
        let max_keys = config.max_keys.unwrap_or(DEFAULT_MAX_KEYS);
        Self {
            read: config.read.map(|r| Limiter::rate(r, max_keys)),
            push: config.push.map(|r| Limiter::rate(r, max_keys)),
            push_bytes: config
                .quota
                .bytes_per_hour
                .map(|b| Limiter::bytes_per_hour(b, max_keys)),
            auth_failures: config.auth_failures.map(|r| Limiter::rate(r, max_keys)),
            quota: config.quota,
            concurrent: config.concurrent,
        }
    }

    /// No limits at all, not even on failed authentications or concurrent
    /// work.
    pub fn unlimited() -> Self {
        Self::new(LimitsConfig {
            auth_failures: None,
            concurrent: ConcurrentLimits::UNLIMITED,
            ..LimitsConfig::default()
        })
    }

    /// The per-caller caps on concurrent work, which the server enforces.
    pub fn concurrent(&self) -> ConcurrentLimits {
        self.concurrent
    }

    /// Takes one failed authentication from `address`'s bucket before a
    /// credential from it is checked. Refused while the address has used
    /// up its failed authentications, before the credential costs a store
    /// read or a signature check, and while the address is not tracked and
    /// the table is full of drained buckets (a failure from it could not be
    /// charged, so it is not checked either).
    ///
    /// The token is taken now, not when the check fails, so concurrent
    /// requests from one address cannot all pass on the same token: at
    /// most the bucket's level of credentials are checked at once, and so
    /// at most that many can fail (issue #198). Give it back with
    /// [`CredentialCharge::refund`] when the credential authenticates, or
    /// when it could not be checked; a charge that is dropped instead
    /// (the credential failed, or the check was cancelled) stays taken.
    pub fn charge_credential<'a>(
        &'a self,
        address: &'a str,
        now: Instant,
    ) -> Result<CredentialCharge<'a>, LimitError> {
        if let Some(l) = &self.auth_failures {
            l.try_take(address, 1.0, now)
                .map_err(LimitError::AuthFailures)?;
        }
        Ok(CredentialCharge {
            limiter: self.auth_failures.as_ref(),
            address,
        })
    }

    /// The push size limits.
    pub fn quota(&self) -> &PushQuota {
        &self.quota
    }

    /// Admits one request of `class` for `key`.
    pub fn admit(&self, class: RequestClass, key: &str, now: Instant) -> Result<(), LimitError> {
        let limiter = match class {
            RequestClass::Read => &self.read,
            RequestClass::Push => &self.push,
        };
        match limiter {
            Some(l) => l.try_take(key, 1.0, now).map_err(LimitError::Rate),
            None => Ok(()),
        }
    }

    /// Admits a push body for `key` before any of it is read: refuses one
    /// whose declared length is over the per-push cap or over what the
    /// caller's budget holds, and one whose caller has no budget left.
    /// Returns the most bytes the body may then carry.
    pub fn admit_push_body(
        &self,
        key: &str,
        declared_len: Option<u64>,
        now: Instant,
    ) -> Result<Option<u64>, LimitError> {
        if let (Some(max), Some(len)) = (self.quota.max_pack_bytes, declared_len)
            && len > max
        {
            return Err(LimitError::TooLarge { max });
        }
        if let Some(budget) = &self.push_bytes {
            let wanted = declared_len.map_or(1.0, |l| l as f64);
            budget
                .check(key, wanted.max(1.0), now)
                .map_err(LimitError::Quota)?;
        }
        Ok(self.quota.max_pack_bytes)
    }

    /// Charges `bytes` of a push body that is streaming in to `key`'s
    /// budget. A refusal means the budget ran out mid-push: the body must
    /// fail, so the pack is never completed or stored.
    pub fn charge_push_bytes(&self, key: &str, bytes: u64, now: Instant) -> Result<(), LimitError> {
        match &self.push_bytes {
            Some(b) => b
                .try_take(key, bytes as f64, now)
                .map_err(LimitError::Quota),
            None => Ok(()),
        }
    }

    /// Whether pushed bytes are budgeted at all.
    pub fn budgets_push_bytes(&self) -> bool {
        self.push_bytes.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_bucket_allows_its_burst_then_refills_at_its_rate() {
        let l = Limiter::new(3.0, 1.0, 10);
        let t0 = Instant::now();
        for _ in 0..3 {
            assert!(l.try_take("a", 1.0, t0).is_ok());
        }
        let refused = l.try_take("a", 1.0, t0).unwrap_err();
        assert_eq!(refused, RetryAfter(Duration::from_secs(1)));
        // Keys are independent.
        assert!(l.try_take("b", 1.0, t0).is_ok());
        assert!(
            l.try_take("a", 1.0, t0 + Duration::from_millis(1000))
                .is_ok()
        );
        assert!(
            l.try_take("a", 1.0, t0 + Duration::from_millis(1000))
                .is_err()
        );
        // Never above capacity, however long it rests.
        let later = t0 + Duration::from_secs(3600);
        assert_eq!(l.available("a", later), 3.0);
    }

    #[test]
    fn a_refused_take_takes_nothing() {
        let l = Limiter::new(10.0, 1.0, 10);
        let t0 = Instant::now();
        assert!(l.try_take("a", 11.0, t0).is_err());
        assert_eq!(l.available("a", t0), 10.0);
        assert_eq!(
            l.try_take("a", 11.0, t0),
            Err(RetryAfter(Duration::from_secs(3600)))
        );
    }

    /// Invariant (B3 review obligation 2): when the table is full and no
    /// bucket has refilled, a new key is refused and every tracked bucket
    /// keeps its level, so a caller churning through keys cannot reset a
    /// drained bucket, its own or anyone else's. Once a bucket refills, a
    /// new key takes its place.
    #[test]
    fn a_full_table_refuses_new_keys_and_resets_nothing() {
        let l = Limiter::new(2.0, 0.5, 4);
        let t0 = Instant::now();
        for k in ["a", "b", "c", "d"] {
            l.try_take(k, 2.0, t0).unwrap();
        }
        for i in 0..1_000u32 {
            let refused = l.try_take(&format!("churn-{i}"), 1.0, t0).unwrap_err();
            assert_eq!(refused, RetryAfter(Duration::from_secs(4)));
        }
        assert_eq!(l.tracked(), 4);
        for k in ["a", "b", "c", "d"] {
            assert!(l.try_take(k, 1.0, t0).is_err(), "{k} was reset");
        }
        // `a` was drained at t0 and refills by t0 + 4 s; the others too.
        let t4 = t0 + Duration::from_secs(4);
        l.try_take("new", 1.0, t4).unwrap();
        assert!(l.tracked() <= 4);
    }

    #[test]
    fn auth_failures_are_limited_per_address_and_successes_are_free() {
        let limits = Limits::new(LimitsConfig {
            auth_failures: Some(RateLimit {
                burst: 3,
                per_second: 1.0,
            }),
            ..LimitsConfig::default()
        });
        let t0 = Instant::now();
        // Successes, refunded, cost nothing however many there are.
        for _ in 0..10 {
            limits.charge_credential("ip:a", t0).unwrap().refund(t0);
        }
        // Failures (charges kept) drain the address's bucket.
        for _ in 0..3 {
            drop(limits.charge_credential("ip:a", t0).unwrap());
        }
        assert!(matches!(
            limits.charge_credential("ip:a", t0),
            Err(LimitError::AuthFailures(_))
        ));
        drop(limits.charge_credential("ip:b", t0).unwrap());
        drop(
            limits
                .charge_credential("ip:a", t0 + Duration::from_secs(1))
                .unwrap(),
        );
        // Off when configured `null`, and in `Limits::unlimited`.
        let off: LimitsConfig = serde_json::from_str(r#"{"auth_failures": null}"#).unwrap();
        assert_eq!(off.auth_failures, None);
        let default: LimitsConfig = serde_json::from_str("{}").unwrap();
        assert_eq!(default.auth_failures, Some(DEFAULT_AUTH_FAILURES));
        assert_eq!(LimitsConfig::default(), default);
        assert_eq!(default.concurrent, DEFAULT_CONCURRENT);
        let partial: LimitsConfig =
            serde_json::from_str(r#"{"concurrent": {"fetches": 2, "stitches": null}}"#).unwrap();
        assert_eq!(
            partial.concurrent,
            ConcurrentLimits {
                fetches: Some(2),
                pushes: DEFAULT_CONCURRENT.pushes,
                stitches: None,
            }
        );
        assert_eq!(
            Limits::unlimited().concurrent(),
            ConcurrentLimits::UNLIMITED
        );
        let unlimited = Limits::unlimited();
        for _ in 0..1_000 {
            drop(unlimited.charge_credential("ip:a", t0).unwrap());
        }
    }

    /// Invariant (issue #198): a credential's token is taken before it is
    /// checked, so no more credentials from one address are in their check
    /// at once than its bucket holds; one that authenticates gives its
    /// token back, and then another may be checked.
    #[test]
    fn credentials_in_their_check_at_once_are_bounded_by_the_bucket() {
        let limits = Limits::new(LimitsConfig {
            auth_failures: Some(RateLimit {
                burst: 3,
                per_second: 0.001,
            }),
            ..LimitsConfig::default()
        });
        let t0 = Instant::now();
        let held: Vec<_> = (0..3)
            .map(|_| limits.charge_credential("ip:a", t0).unwrap())
            .collect();
        assert!(matches!(
            limits.charge_credential("ip:a", t0),
            Err(LimitError::AuthFailures(_))
        ));
        let mut held = held.into_iter();
        held.next().unwrap().refund(t0);
        drop(limits.charge_credential("ip:a", t0).unwrap());
        assert!(limits.charge_credential("ip:a", t0).is_err());
        for charge in held {
            charge.refund(t0);
        }
        // Two refunds, one failure kept: two left.
        for _ in 0..2 {
            drop(limits.charge_credential("ip:a", t0).unwrap());
        }
        assert!(limits.charge_credential("ip:a", t0).is_err());
    }

    /// Invariant: a refund never lifts a bucket above its capacity, leaves
    /// an untracked key alone, and drops a bucket it fills, so a full table
    /// has room again at once.
    #[test]
    fn a_refund_is_capped_and_frees_a_full_bucket() {
        let l = Limiter::new(3.0, 0.001, 2);
        let t0 = Instant::now();
        l.try_take("a", 2.0, t0).unwrap();
        l.give_back("a", 5.0, t0);
        assert_eq!(l.available("a", t0), 3.0);
        assert_eq!(l.tracked(), 0);
        l.give_back("untracked", 1.0, t0);
        assert_eq!(l.tracked(), 0);
        // A full table of drained buckets refuses a new key until a refund
        // fills one of them.
        l.try_take("a", 3.0, t0).unwrap();
        l.try_take("b", 3.0, t0).unwrap();
        assert!(l.try_take("c", 1.0, t0).is_err());
        l.give_back("a", 2.0, t0);
        assert!(l.try_take("c", 1.0, t0).is_err(), "a is not full yet");
        l.give_back("a", 1.0, t0);
        l.try_take("c", 1.0, t0).unwrap();
        assert!(l.try_take("b", 1.0, t0).is_err(), "b was reset");
    }

    /// Invariant (D4 review): a full table of drained failure buckets does
    /// not switch the failed-authentication limit off. An address that is
    /// not tracked is refused before its credential is checked, as long as
    /// no tracked bucket has refilled; then it is admitted again.
    #[test]
    fn a_full_table_refuses_unchecked_credentials_from_new_addresses() {
        let limits = Limits::new(LimitsConfig {
            auth_failures: Some(RateLimit {
                burst: 2,
                per_second: 0.5,
            }),
            max_keys: Some(4),
            ..LimitsConfig::default()
        });
        let t0 = Instant::now();
        for a in ["ip:a", "ip:b", "ip:c", "ip:d"] {
            for _ in 0..2 {
                drop(limits.charge_credential(a, t0).unwrap());
            }
        }
        for i in 0..100 {
            assert_eq!(
                limits
                    .charge_credential(&format!("ip:new-{i}"), t0)
                    .map(drop),
                Err(LimitError::AuthFailures(RetryAfter(Duration::from_secs(4)))),
            );
        }
        let t4 = t0 + Duration::from_secs(4);
        limits.charge_credential("ip:new-0", t4).unwrap().refund(t4);
        // The same holds for a push budget: a new caller is not promised
        // bytes the table has no room to count.
        let quota = Limits::new(LimitsConfig {
            quota: PushQuota {
                bytes_per_hour: Some(3600),
                ..PushQuota::default()
            },
            max_keys: Some(1),
            ..LimitsConfig::default()
        });
        quota.charge_push_bytes("a", 3600, t0).unwrap();
        assert!(matches!(
            quota.admit_push_body("b", Some(10), t0),
            Err(LimitError::Quota(_))
        ));
    }

    /// Invariant: the number of tracked keys never exceeds `max_keys`, and
    /// a caller with a drained bucket keeps it through eviction of idle
    /// callers.
    #[test]
    fn tracked_keys_stay_bounded() {
        let l = Limiter::new(2.0, 0.001, 100);
        let t0 = Instant::now();
        l.try_take("hot", 2.0, t0).unwrap();
        for i in 0..10_000u32 {
            let _ = l.try_take(
                &format!("ip-{i}"),
                1.0,
                t0 + Duration::from_micros(u64::from(i)),
            );
            assert!(l.tracked() <= 100);
        }
        let l = Limiter::new(2.0, 1.0, 3);
        l.try_take("hot", 2.0, t0).unwrap();
        l.try_take("idle1", 1.0, t0).unwrap();
        l.try_take("idle2", 1.0, t0).unwrap();
        // idle1 and idle2 have refilled by t0 + 2 s; hot was drained more.
        let t = t0 + Duration::from_millis(1500);
        l.try_take("new", 1.0, t).unwrap();
        assert!(l.try_take("hot", 2.0, t).is_err(), "hot bucket was reset");
    }

    #[test]
    fn limits_refuse_by_class_and_size() {
        let limits = Limits::new(LimitsConfig {
            read: Some(RateLimit {
                burst: 1,
                per_second: 0.5,
            }),
            push: None,
            quota: PushQuota {
                max_pack_bytes: Some(1000),
                max_objects: Some(10),
                bytes_per_hour: Some(1500),
            },
            ..LimitsConfig::default()
        });
        let t0 = Instant::now();
        assert!(limits.admit(RequestClass::Read, "k", t0).is_ok());
        assert!(matches!(
            limits.admit(RequestClass::Read, "k", t0),
            Err(LimitError::Rate(_))
        ));
        assert!(limits.admit(RequestClass::Push, "k", t0).is_ok());
        assert_eq!(
            limits.admit_push_body("k", Some(1001), t0),
            Err(LimitError::TooLarge { max: 1000 })
        );
        assert_eq!(limits.admit_push_body("k", Some(900), t0), Ok(Some(1000)));
        limits.charge_push_bytes("k", 900, t0).unwrap();
        assert!(matches!(
            limits.admit_push_body("k", Some(900), t0),
            Err(LimitError::Quota(_))
        ));
        assert!(limits.admit_push_body("k", None, t0).is_ok());
        assert!(matches!(
            limits.charge_push_bytes("k", 700, t0),
            Err(LimitError::Quota(_))
        ));
        // Another caller has its own budget.
        assert!(limits.admit_push_body("other", Some(900), t0).is_ok());
    }
}
