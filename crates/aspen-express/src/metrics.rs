//! Express counters, in the Prometheus text format. The server appends
//! them to its `/-/metrics` (the B2 stand-in that C5 replaces, keeping the
//! names).

use std::fmt::Write;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::url::Refusal;

/// Counters for one express service. All are monotonic except the cache
/// size.
#[derive(Debug, Default)]
pub struct ExpressMetrics {
    requests: AtomicU64,
    refused: [AtomicU64; Refusal::ALL.len()],
    not_found: AtomicU64,
    store_errors: AtomicU64,
    not_modified: AtomicU64,
    hits: AtomicU64,
    misses: AtomicU64,
    bytes_served: AtomicU64,
    origin_bytes: AtomicU64,
    fills: AtomicU64,
    fill_failures: AtomicU64,
    fills_skipped: AtomicU64,
    corrupt: AtomicU64,
    evictions: AtomicU64,
    cached_bytes: AtomicU64,
}

impl ExpressMetrics {
    pub(crate) fn request(&self) {
        self.requests.fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn refused(&self, r: Refusal) {
        let i = Refusal::ALL.iter().position(|x| *x == r).expect("listed");
        self.refused[i].fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn not_found(&self) {
        self.not_found.fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn store_error(&self) {
        self.store_errors.fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn not_modified(&self) {
        self.not_modified.fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn hit(&self) {
        self.hits.fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn miss(&self) {
        self.misses.fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn served(&self, n: u64) {
        self.bytes_served.fetch_add(n, Ordering::Relaxed);
    }
    pub(crate) fn origin(&self, n: u64) {
        self.origin_bytes.fetch_add(n, Ordering::Relaxed);
    }
    pub(crate) fn fill(&self, ok: bool) {
        if ok {
            self.fills.fetch_add(1, Ordering::Relaxed);
        } else {
            self.fill_failures.fetch_add(1, Ordering::Relaxed);
        }
    }
    pub(crate) fn fill_skipped(&self) {
        self.fills_skipped.fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn corrupt(&self) {
        self.corrupt.fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn evicted(&self) {
        self.evictions.fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn set_cached_bytes(&self, n: u64) {
        self.cached_bytes.store(n, Ordering::Relaxed);
    }

    /// Requests answered from the disk cache.
    pub fn hits(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }
    /// Requests answered from the blob store (cache off, a miss, or a
    /// blob too large to wait for).
    pub fn misses(&self) -> u64 {
        self.misses.load(Ordering::Relaxed)
    }
    /// Requests refused for `reason`.
    pub fn refusals(&self, reason: Refusal) -> u64 {
        let i = Refusal::ALL
            .iter()
            .position(|x| *x == reason)
            .expect("listed");
        self.refused[i].load(Ordering::Relaxed)
    }
    /// Bytes read from the blob store, for responses and cache fills.
    pub fn origin_bytes(&self) -> u64 {
        self.origin_bytes.load(Ordering::Relaxed)
    }
    /// Cache fills that completed.
    pub fn fills(&self) -> u64 {
        self.fills.load(Ordering::Relaxed)
    }
    /// Blobs whose bytes did not hash to the checksum in their key (pages).
    pub fn corrupt_blobs(&self) -> u64 {
        self.corrupt.load(Ordering::Relaxed)
    }
    /// Cache entries evicted to stay within the budget.
    pub fn evictions(&self) -> u64 {
        self.evictions.load(Ordering::Relaxed)
    }

    /// Appends the Prometheus text exposition.
    pub fn render(&self, s: &mut String) {
        let get = |a: &AtomicU64| a.load(Ordering::Relaxed);
        counter(
            s,
            "aspen_express_requests_total",
            "Requests to express URLs.",
            get(&self.requests),
        );
        let name = "aspen_express_refused_total";
        let _ = writeln!(
            s,
            "# HELP {name} Requests refused with 403 (bad, expired or foreign URLs).\n# TYPE {name} counter"
        );
        for (r, n) in Refusal::ALL.iter().zip(&self.refused) {
            let _ = writeln!(s, "{name}{{reason=\"{}\"}} {}", r.label(), get(n));
        }
        counter(
            s,
            "aspen_express_not_found_total",
            "Validly signed URLs whose pack is not in the blob store.",
            get(&self.not_found),
        );
        counter(
            s,
            "aspen_express_store_errors_total",
            "Requests that failed reading the blob store.",
            get(&self.store_errors),
        );
        counter(
            s,
            "aspen_express_not_modified_total",
            "Conditional requests answered 304.",
            get(&self.not_modified),
        );
        counter(
            s,
            "aspen_express_cache_hits_total",
            "Requests served from the disk cache.",
            get(&self.hits),
        );
        counter(
            s,
            "aspen_express_cache_misses_total",
            "Requests served from the blob store.",
            get(&self.misses),
        );
        counter(
            s,
            "aspen_express_bytes_served_total",
            "Response body bytes sent.",
            get(&self.bytes_served),
        );
        counter(
            s,
            "aspen_express_origin_bytes_total",
            "Bytes read from the blob store (responses and cache fills).",
            get(&self.origin_bytes),
        );
        counter(
            s,
            "aspen_express_cache_fills_total",
            "Packs copied into the disk cache.",
            get(&self.fills),
        );
        counter(
            s,
            "aspen_express_cache_fill_failures_total",
            "Cache fills that failed (store or disk errors, or corruption).",
            get(&self.fill_failures),
        );
        counter(
            s,
            "aspen_express_cache_fills_skipped_total",
            "Cache misses that started no fill (the fill cap was reached, or fills in progress held the budget); served from the blob store.",
            get(&self.fills_skipped),
        );
        counter(
            s,
            "aspen_alert_express_corrupt_pack_total",
            "Stored packs whose bytes do not hash to the checksum in their key. Pages.",
            get(&self.corrupt),
        );
        counter(
            s,
            "aspen_express_cache_evictions_total",
            "Packs evicted from the disk cache.",
            get(&self.evictions),
        );
        let name = "aspen_express_cache_bytes";
        let _ = writeln!(
            s,
            "# HELP {name} Bytes in the disk cache.\n# TYPE {name} gauge\n{name} {}",
            get(&self.cached_bytes)
        );
    }
}

pub(crate) fn counter(s: &mut String, name: &str, help: &str, v: u64) {
    let _ = writeln!(s, "# HELP {name} {help}\n# TYPE {name} counter\n{name} {v}");
}
