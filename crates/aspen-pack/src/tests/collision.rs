//! A known SHA-1 collision attack is refused (issue #129).
//!
//! Every SHA-1 that Aspen computes over pushed bytes has Git's collision
//! detection (`sha1dc`, through `gix-hash`). There are two hashers:
//!
//! - gix-pack 0.77 builds `gix_hash::hasher(Kind::Sha1)` for the client's
//!   pack trailer in pass 1 of [`crate::ingest::receive`]
//!   (`BytesToEntriesIter`) and, through `gix_object::compute_hash`, for
//!   every object ID that [`crate::ingest::complete`] resolves. `receive`
//!   passes `Kind::Sha1`.
//! - Aspen's own [`crate::Sha1`] hashes the packs Aspen writes (the stored
//!   pack's trailer, repack, the stitcher), and [`crate::sha1`] wraps it.
//!
//! A whole pushed pack cannot carry a published collision. Collision blocks
//! collide only after their own prefix, and every hash over pushed bytes
//! starts with bytes that Git fixes: `PACK` and the header for the trailer,
//! `<kind> <size>\0` for an object ID. So these tests feed the SHA-mbles pair
//! (`data/sha1-collision/README.md`) to each hasher directly. Then they check
//! that the error gix-pack raises for a detected collision becomes the push's
//! `unpack` error. They fail if either hasher stops detecting the
//! collision, or if the error mapping changes.

use std::cell::Cell;

use aspen_core::error::{Surface, ToWire, UnpackError};
use aspen_core::id::ObjectId;
use aspen_core::pack::PushLimits;
use gix_error::{ErrorExt, ResultExt, message};

use crate::ingest::{completion_error, gix_chain, pass1_error};
use crate::{Collision, Sha1};

const SHAMBLES_1: &[u8] = include_bytes!("data/sha1-collision/sha-mbles-1.bin");
const SHAMBLES_2: &[u8] = include_bytes!("data/sha1-collision/sha-mbles-2.bin");

/// The plain SHA-1 of both messages (coreutils `sha1sum`, which has no
/// collision detection).
const COLLIDING_SHA1: &str = "8ac60ba76f1999a1ab70223f225aefdc78d4ddc0";

/// The plain SHA-1 of `SHAMBLES_1` with its first byte XORed with 1. That
/// changes the prefix, so the later blocks no longer collide.
const CONTROL_SHA1: &str = "eed902ce525f03c0a731690e0e245dc37efa997f";

fn pair() -> [(&'static str, &'static [u8]); 2] {
    [("sha-mbles-1", SHAMBLES_1), ("sha-mbles-2", SHAMBLES_2)]
}

/// The first message with one prefix byte changed: same length and the same
/// near-collision blocks, but no attack, because the blocks no longer follow
/// the chaining value they were built for.
fn control() -> Vec<u8> {
    let mut data = SHAMBLES_1.to_vec();
    data[0] ^= 1;
    data
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// The collision error exactly as `gix_hash::Hasher::try_finalize` raises
/// it, from the constructor that gix-pack uses, with the kind that
/// `receive` passes.
fn gix_collision(data: &[u8]) -> gix_error::Error {
    let mut h = gix_hash::hasher(gix_hash::Kind::Sha1);
    h.update(data);
    h.try_finalize()
        .expect_err("gix-hash must detect the SHA-mbles collision")
}

#[test]
fn the_fixture_is_two_different_messages() {
    assert_eq!(SHAMBLES_1.len(), 640);
    assert_eq!(SHAMBLES_2.len(), 640);
    assert_ne!(SHAMBLES_1, SHAMBLES_2);
}

#[test]
fn aspen_sha1_refuses_a_known_collision_in_any_chunking() {
    for (name, data) in pair() {
        for chunk in [data.len(), 1, 63, 64, 65, 100] {
            let mut h = Sha1::new();
            for part in data.chunks(chunk) {
                h.update(part);
            }
            assert_eq!(h.finish(), Err(Collision), "{name}, chunks of {chunk}");
        }
    }
    let mut h = Sha1::new();
    h.update(&control());
    let digest = h.finish().expect("a changed prefix is no attack");
    assert_eq!(hex(&digest), CONTROL_SHA1);
}

#[test]
fn the_sha1_helper_fails_closed_on_a_collision() {
    let zero = ObjectId::from_bytes([0; 20]);
    for (name, data) in pair() {
        let id = crate::sha1(data);
        assert_eq!(id, zero, "{name}");
        assert_ne!(hex(id.as_bytes()), COLLIDING_SHA1, "{name}");
    }
    assert_eq!(hex(crate::sha1(&control()).as_bytes()), CONTROL_SHA1);
}

/// gix-hash reports the plain digest of the attack. Both messages report
/// the same digest, which shows that they really collide.
#[test]
fn gix_pack_s_hasher_detects_both_halves_of_the_collision() {
    for (name, data) in pair() {
        let text = gix_chain(&gix_collision(data));
        assert!(text.contains("collision"), "{name}: {text}");
        assert!(text.contains(COLLIDING_SHA1), "{name}: {text}");
    }
    let mut h = gix_hash::hasher(gix_hash::Kind::Sha1);
    h.update(&control());
    let id = h.try_finalize().expect("a changed prefix is no attack");
    assert_eq!(hex(id.as_slice()), CONTROL_SHA1);
}

/// Asserts that `e` is the push's `unpack` error for a collision: the whole
/// pack is refused, the text names the attack, and a retry cannot succeed.
#[track_caller]
fn assert_refused_as_collision(e: UnpackError, path: &str) {
    let UnpackError::Malformed { reason } = &e else {
        panic!("{path}: a collision must be a malformed pack, got {e:?}");
    };
    assert!(reason.contains("collision"), "{path}: {reason}");
    let wire = e.to_wire();
    assert_eq!(wire.surface, Surface::PushUnpack, "{path}");
    assert!(!wire.retryable, "{path}: a retry resends the same attack");
    assert!(!wire.alert, "{path}: client input, not stored data");
    assert!(
        wire.message.starts_with("malformed pack: "),
        "{path}: {}",
        wire.message
    );
    assert!(
        wire.message.contains("collision"),
        "{path}: {}",
        wire.message
    );
}

#[test]
fn a_detected_collision_is_the_push_s_unpack_error() {
    let limits = PushLimits {
        max_objects: 1_000,
        max_pack_bytes: 1 << 20,
    };
    for (name, data) in pair() {
        // Pass 1: gix-pack 0.77's `BytesToEntriesIter::try_read_trailer`
        // wraps the hasher's error with `hash_io_error`.
        let trailer = gix_collision(data)
            .and_raise(message("An IO operation failed while streaming an entry"));
        assert_refused_as_collision(
            pass1_error(&trailer, &Cell::new(false), &limits),
            &format!("receive, {name}"),
        );

        // Completion: gix-pack 0.77's delta resolution (`traverse::resolve`)
        // wraps `gix_object::compute_hash`'s error like this.
        let object = Err::<(), _>(gix_collision(data))
            .or_raise(|| message("Failed to hash an object while resolving in-pack ref-deltas"))
            .unwrap_err();
        assert_refused_as_collision(completion_error(&object), &format!("complete, {name}"));

        // The hasher's own error, with no wrapper, maps the same way.
        assert_refused_as_collision(
            pass1_error(&gix_collision(data), &Cell::new(false), &limits),
            &format!("receive, unwrapped, {name}"),
        );
        assert_refused_as_collision(
            completion_error(&gix_collision(data)),
            &format!("complete, unwrapped, {name}"),
        );
    }
}
