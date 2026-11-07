//! The limits the `aspen-place` binary runs Aspen with, kept here so the
//! tests run against the values a deployment gets.
//!
//! | Limit | Value | Why |
//! |---|---|---|
//! | Largest object ([`max_object_bytes`]) | [`DELTA_HEADROOM`] (4) times the canvas's full `tiles/` tree, at least 64 KiB | The `tiles/` tree is the largest object a legal push carries, and resolving it as a delta needs room for two copies plus the delta; everything else (commit messages, author lines, a stray file) fits far below it |
//! | Largest pushed pack ([`MAX_PACK_BYTES`], in [`limits`]) | 1 MiB | A stroke is a commit, a tree and at most 64 tile blobs |
//! | Resident memory mark ([`memory_high_water`]) | 80% of the task's memory | Past it new Git requests get `503` instead of the process being killed |
//! | Git pushes per client address ([`PUSHES_PER_SECOND`], [`PUSH_BURST`]) | 200/s, 2,000 at once | One address (an agent fleet behind one NAT) can come near the demo's wall of about 500 pushes/s in all |
//! | Git fetches per client address ([`FETCHES_PER_SECOND`], [`FETCH_BURST`]) | 200/s, 2,000 at once | Agents fetch about as often as they push |
//! | Pushes running at once ([`instance_limits`], [`limits`]) | 192 per address, 256 in all | A few hundred pushes/s from one address at push latencies up to about half a second, with room left for everyone else |
//! | Fetches running at once | 64 per address, 128 in all | |
//!
//! The rates are limits of abuse, not of use: the instance-wide caps, the
//! queue in front of them and the memory mark are what keep the demo up
//! when everyone pushes at once.

use std::time::Duration;

use aspen_auth::{ConcurrentLimits, LimitsConfig, PushQuota, RateLimit};
use aspen_pack::MaxObjectBytes;
use aspen_server::admission::{InstanceLimits, MEMORY_HIGH_WATER_PERCENT, memory_limit};

use crate::rules::Rules;

/// The floor of [`max_object_bytes`]: 64 KiB.
pub const MIN_OBJECT_BYTES: u64 = 64 << 10;

/// [`MIN_OBJECT_BYTES`] as a limit, checked when this compiles.
const OBJECT_FLOOR: MaxObjectBytes = match MaxObjectBytes::new(MIN_OBJECT_BYTES) {
    Some(floor) => floor,
    None => panic!("MIN_OBJECT_BYTES is not zero"),
};

/// How many full `tiles/` trees [`max_object_bytes`] allows. aspen-pack
/// passes the object limit to gix as its cap on one allocation, and gix
/// resolves a delta in one buffer of twice the result plus the delta data,
/// so a stroke sent as a delta of the full tree (as stock Git sends it)
/// needs more than twice the tree; at exactly the tree size it is refused
/// as `internal storage error` (test
/// `a_full_canvas_still_paints_merges_and_restarts_its_history`).
pub const DELTA_HEADROOM: u64 = 4;

/// The largest pack one Git push may send: 1 MiB.
pub const MAX_PACK_BYTES: u64 = 1 << 20;

/// Git pushes per second one client address may sustain.
pub const PUSHES_PER_SECOND: u32 = 200;
/// Git pushes one client address may send at once after a quiet period.
pub const PUSH_BURST: u32 = 2_000;
/// Git fetches (and clones) per second one client address may sustain.
pub const FETCHES_PER_SECOND: u32 = 200;
/// Git fetches one client address may send at once after a quiet period.
pub const FETCH_BURST: u32 = 2_000;

/// HTTP requests one `git push` makes: the receive-pack advertisement and
/// the pack, both counted as push requests.
pub const REQUESTS_PER_PUSH: u32 = 2;
/// HTTP requests one protocol-v2 `git fetch` makes: the advertisement,
/// `ls-refs` and `fetch`, all counted as read requests.
pub const REQUESTS_PER_FETCH: u32 = 3;

/// Push requests (both endpoints) one client address may run at once.
pub const PUSHES_AT_ONCE_PER_ADDRESS: u32 = 192;
/// Push requests running at once on the instance. With the fetches and
/// stitches below, at most 416 requests hold one of tokio's 512 blocking
/// threads.
pub const PUSHES_AT_ONCE: usize = 256;
/// Fetch requests one client address may run at once.
pub const FETCHES_AT_ONCE_PER_ADDRESS: u32 = 64;
/// Fetch requests running at once on the instance.
pub const FETCHES_AT_ONCE: usize = 128;
/// Requests of each kind that may wait for a slot when the instance is
/// full, for at most [`QUEUE_WAIT`]; a waiting request has not sent its
/// body yet, so it holds a connection and no pack.
pub const QUEUE: usize = 1_024;
/// The longest a request waits for a slot before it gets `503`.
pub const QUEUE_WAIT: Duration = Duration::from_secs(10);

/// Bytes one client address may push per hour: about 100 KiB (a stroke
/// on a full 64 x 64 canvas carries its 150 KB `tiles/` tree) at
/// [`PUSHES_PER_SECOND`], with room to spare.
pub const PUSH_BYTES_PER_HOUR: u64 = 256 << 30;

/// The size in bytes of the `tiles/` tree of a `width` x `height` canvas
/// with every tile painted: one entry `100644 <x>_<y>.txt\0<20-byte id>`
/// per tile (`demo/place/docs/TILES.md`). No tree a legal push writes is
/// larger (64 x 64: 150,272 bytes).
///
/// Saturates at `u64::MAX` rather than overflowing, for any `u32` sizes;
/// [`crate::PlaceConfig::validate`] caps both at 4096 anyway (at most
/// about 590 MB).
pub fn full_tiles_tree_bytes(width: u32, height: u32) -> u64 {
    let (w, h) = (u64::from(width), u64::from(height));
    // "100644 " + "_" + ".txt" + NUL + id, then the digits of x and y.
    const FIXED: u64 = 7 + 1 + 4 + 1 + 20;
    w.saturating_mul(h)
        .saturating_mul(FIXED)
        .saturating_add(h.saturating_mul(coordinate_digits(width)))
        .saturating_add(w.saturating_mul(coordinate_digits(height)))
}

/// The decimal digits of every coordinate in `0..n`, summed: for each
/// length `d`, the coordinates below `n` that have `d` digits, times `d`.
fn coordinate_digits(n: u32) -> u64 {
    let n = u64::from(n);
    let (mut total, mut digits, mut low) = (0, 1, 0);
    while low < n {
        // `0..10` has one digit (0 included), `10..100` two, and so on.
        let high = 10u64.pow(digits).min(n);
        total += (high - low) * u64::from(digits);
        low = high;
        digits += 1;
    }
    total
}

/// The largest object any step reads or a push may carry:
/// [`DELTA_HEADROOM`] times the canvas's full `tiles/` tree, and at least
/// [`MIN_OBJECT_BYTES`] (64 x 64: 601,088 bytes). It also bounds a commit's
/// message and author lines, which no step needs larger.
pub fn max_object_bytes(rules: &Rules) -> MaxObjectBytes {
    let bytes = full_tiles_tree_bytes(rules.width, rules.height).saturating_mul(DELTA_HEADROOM);
    MaxObjectBytes::new(bytes.max(MIN_OBJECT_BYTES)).unwrap_or(OBJECT_FLOOR)
}

/// The per-caller limits on Git requests when `PLACE_LIMITS_FILE` is
/// unset. A caller is a client address (an IPv6 address by its /64).
pub fn limits() -> LimitsConfig {
    LimitsConfig {
        read: Some(RateLimit {
            burst: FETCH_BURST * REQUESTS_PER_FETCH,
            per_second: f64::from(FETCHES_PER_SECOND * REQUESTS_PER_FETCH),
        }),
        push: Some(RateLimit {
            burst: PUSH_BURST * REQUESTS_PER_PUSH,
            per_second: f64::from(PUSHES_PER_SECOND * REQUESTS_PER_PUSH),
        }),
        quota: PushQuota {
            max_pack_bytes: Some(MAX_PACK_BYTES),
            max_objects: Some(20_000),
            bytes_per_hour: Some(PUSH_BYTES_PER_HOUR),
        },
        concurrent: ConcurrentLimits {
            fetches: Some(FETCHES_AT_ONCE_PER_ADDRESS),
            pushes: Some(PUSHES_AT_ONCE_PER_ADDRESS),
            ..ConcurrentLimits::default()
        },
        ..LimitsConfig::default()
    }
}

/// The resident-memory mark when `PLACE_MEMORY_HIGH_WATER_BYTES` is unset:
/// [`MEMORY_HIGH_WATER_PERCENT`] (80%) of the memory this process may use
/// (its cgroup limit, which is the Fargate task's memory, or the
/// machine's), as `aspen serve` does; `None` where that cannot be read.
pub fn memory_high_water() -> Option<u64> {
    memory_limit().map(|limit| limit / 100 * MEMORY_HIGH_WATER_PERCENT)
}

/// Requests running at once on this instance ([`PUSHES_AT_ONCE`],
/// [`FETCHES_AT_ONCE`], [`QUEUE`]), and the resident memory
/// (`memory_high_water`, `None` for no mark) over which new Git requests
/// are refused with `503`.
pub fn instance_limits(memory_high_water: Option<u64>) -> InstanceLimits {
    InstanceLimits {
        fetches: FETCHES_AT_ONCE,
        pushes: PUSHES_AT_ONCE,
        queue: QUEUE,
        queue_wait: QUEUE_WAIT,
        memory_high_water,
        ..InstanceLimits::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::git::{MODE_FILE, TreeEntry, write_tree};
    use aspen_core::id::ObjectId;

    /// The formula is the size of the tree `place` and Git really write
    /// for a full canvas, for canvases with one-, two-, three- and
    /// four-digit coordinates.
    #[test]
    fn the_full_tiles_tree_is_the_size_of_the_tree_written() {
        for (w, h) in [(1, 1), (16, 16), (64, 64), (100, 7), (3, 1001)] {
            let entries = (0..w)
                .flat_map(|x| (0..h).map(move |y| (x, y)))
                .map(|(x, y)| TreeEntry {
                    mode: MODE_FILE,
                    name: format!("{x}_{y}.txt").into_bytes(),
                    id: ObjectId::from_bytes([7; 20]),
                })
                .collect();
            assert_eq!(
                write_tree(entries).len() as u64,
                full_tiles_tree_bytes(w, h),
                "{w} x {h}"
            );
        }
        // The live 64 x 64 demo's `tiles/` tree, measured with
        // `git cat-file -s HEAD:tiles` on a full canvas.
        assert_eq!(full_tiles_tree_bytes(64, 64), 150_272);
    }

    /// Summing the digits by length gives what counting each coordinate
    /// gives, and the largest sizes saturate instead of overflowing.
    #[test]
    fn the_size_never_overflows_and_digits_are_counted_exactly() {
        let counted = |n: u32| -> u64 {
            (0..n)
                .map(|i| u64::from(i.checked_ilog10().unwrap_or(0) + 1))
                .sum()
        };
        for n in [0, 1, 9, 10, 11, 99, 100, 101, 999, 1000, 4096, 12_345] {
            assert_eq!(coordinate_digits(n), counted(n), "{n}");
        }
        // The largest canvas `validate` allows, and sizes past it.
        assert_eq!(
            full_tiles_tree_bytes(4096, 4096),
            4096 * 4096 * 33 + 2 * 4096 * counted(4096)
        );
        assert_eq!(full_tiles_tree_bytes(u32::MAX, u32::MAX), u64::MAX);
        let mut rules = crate::PlaceConfig::new("x").rules;
        (rules.width, rules.height) = (u32::MAX, u32::MAX);
        assert_eq!(max_object_bytes(&rules).get(), u64::MAX);
        (rules.width, rules.height) = (0, 0);
        assert_eq!(max_object_bytes(&rules).get(), MIN_OBJECT_BYTES);
    }

    /// Small canvases get the 64 KiB floor; the default 64 x 64 canvas
    /// gets four full trees, far below the 16 MiB it ran with.
    #[test]
    fn the_object_limit_is_the_full_tree_with_a_floor() {
        let mut rules = crate::PlaceConfig::new("x").rules;
        rules.width = 16;
        rules.height = 16;
        assert_eq!(max_object_bytes(&rules).get(), MIN_OBJECT_BYTES);
        rules.width = 64;
        rules.height = 64;
        assert_eq!(max_object_bytes(&rules).get(), 4 * 150_272);
    }

    /// The mark is 80% of what `memory_limit` reads, as in `aspen serve`.
    #[test]
    fn the_memory_mark_is_eighty_percent_of_the_limit() {
        assert_eq!(memory_high_water(), memory_limit().map(|l| l / 100 * 80));
        assert_eq!(
            instance_limits(Some(5)).memory_high_water,
            Some(5),
            "the mark is passed through"
        );
    }

    /// One address may make [`PUSH_BURST`] pushes at once and then
    /// [`PUSHES_PER_SECOND`] a second, counting both requests of each
    /// push; the same for fetches. Before, it was 5 pushes at once and
    /// one every 2 s.
    #[test]
    fn one_address_may_push_two_hundred_times_a_second() {
        use aspen_auth::{Limits, RequestClass};
        let limits = Limits::new(limits());
        let t0 = std::time::Instant::now();
        let push = |at| {
            (0..REQUESTS_PER_PUSH).all(|_| limits.admit(RequestClass::Push, "ip:a", at).is_ok())
        };
        for i in 0..PUSH_BURST {
            assert!(push(t0), "push {i} of the burst");
        }
        assert!(!push(t0), "the burst is spent");
        let t1 = t0 + Duration::from_secs(1);
        for i in 0..PUSHES_PER_SECOND {
            assert!(push(t1), "push {i} of the next second");
        }
        assert!(!push(t1));
        assert!(push(t0 + Duration::from_millis(1_005)));
        assert!(
            limits.admit(RequestClass::Push, "ip:b", t1).is_ok(),
            "another address has its own bucket"
        );
        let fetch = |at| {
            (0..REQUESTS_PER_FETCH).all(|_| limits.admit(RequestClass::Read, "ip:a", at).is_ok())
        };
        for i in 0..FETCH_BURST {
            assert!(fetch(t0), "fetch {i} of the burst");
        }
        assert!(!fetch(t0));
        assert!((0..FETCHES_PER_SECOND).all(|_| fetch(t1)));
        assert!(!fetch(t1));
    }

    /// One address may run [`PUSHES_AT_ONCE_PER_ADDRESS`] pushes at once
    /// (4 before), and the instance [`PUSHES_AT_ONCE`] (16 before), so
    /// another address still gets slots while the first is at its cap.
    #[tokio::test]
    async fn one_address_may_run_hundreds_of_pushes_at_once() {
        use aspen_server::admission::{Admission, Busy, Work};
        let admission = Admission::new(limits().concurrent, instance_limits(None));
        let mut held = Vec::new();
        for _ in 0..PUSHES_AT_ONCE_PER_ADDRESS {
            held.push(admission.enter(Work::Push, "ip:a").await.unwrap());
        }
        assert!(matches!(
            admission.enter(Work::Push, "ip:a").await,
            Err(Busy::Caller { work: Work::Push, max }) if max == PUSHES_AT_ONCE_PER_ADDRESS
        ));
        let others = PUSHES_AT_ONCE - PUSHES_AT_ONCE_PER_ADDRESS as usize;
        for _ in 0..others {
            held.push(admission.enter(Work::Push, "ip:b").await.unwrap());
        }
        assert_eq!(held.len(), PUSHES_AT_ONCE);
        for _ in 0..FETCHES_AT_ONCE_PER_ADDRESS {
            held.push(admission.enter(Work::Fetch, "ip:a").await.unwrap());
        }
        assert!(matches!(
            admission.enter(Work::Fetch, "ip:a").await,
            Err(Busy::Caller {
                work: Work::Fetch,
                ..
            })
        ));
    }

    /// The requests that can hold a blocking thread at once stay under
    /// tokio's default of 512 (the `InstanceLimits::pushes` rule).
    #[test]
    fn running_requests_fit_in_the_blocking_pool() {
        let i = instance_limits(None);
        assert!(i.pushes + i.fetches + i.stitches < 512);
    }
}
