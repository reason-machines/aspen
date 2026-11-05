//! The per-ref decision: given Aspen's value of a ref, the upstream's, and
//! the value both last agreed on (the ledger), what one sync step does.
//!
//! | Mode | Aspen = upstream | Only the upstream moved | Only Aspen moved | Both moved |
//! |---|---|---|---|---|
//! | proxy | in sync | import | import (Aspen follows) | import |
//! | async | in sync | import | push | conflict |
//!
//! "Moved" means "differs from the ledger". A ref with no ledger row
//! counts as agreed absent, so a first import is "only the upstream
//! moved", and a branch first pushed to Aspen is "only Aspen moved".
//!
//! The rule never loses a change silently in async mode: an import
//! overwrites only a ref Aspen has not changed since the agreement, and a
//! push overwrites only a ref the upstream has not changed since. Both
//! writes are compare-and-swaps on the value read (Aspen's push path and
//! the upstream's `receive-pack` both check the old value), so a change
//! that lands between the read and the write fails the step instead of
//! being overwritten, and the next pass decides again.
//!
//! Resolving a conflict only moves the ledger ([`resolve`]): taking
//! Aspen's side records the upstream's conflicting value as agreed, so the
//! next step is a push; taking the upstream's side records Aspen's, so the
//! next step is an import. If either side moved again since the conflict
//! was recorded, the next step finds a conflict again rather than
//! overwriting the newer change.

use aspen_core::id::ObjectId;

use crate::store::{Conflict, MirrorMode};

/// What one sync step does with a ref.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    /// Both sides hold the same value: record it as agreed.
    InSync,
    /// Make Aspen's ref equal the upstream's.
    Import,
    /// Make the upstream's ref equal Aspen's.
    Push,
    /// Both moved: record the conflict and change neither side.
    Conflict,
}

/// The step for a ref whose values are `aspen` and `upstream` and whose
/// ledger agreement is `synced` (`None` for absent).
pub fn decide(
    mode: MirrorMode,
    aspen: Option<ObjectId>,
    upstream: Option<ObjectId>,
    synced: Option<ObjectId>,
) -> Action {
    if aspen == upstream {
        return Action::InSync;
    }
    match mode {
        MirrorMode::Proxy => Action::Import,
        MirrorMode::Async if aspen == synced => Action::Import,
        MirrorMode::Async if upstream == synced => Action::Push,
        MirrorMode::Async => Action::Conflict,
    }
}

/// Which side a conflict resolution keeps.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    /// Keep Aspen's value: it is pushed to the upstream.
    Aspen,
    /// Keep the upstream's value: it is imported into Aspen.
    Upstream,
}

/// The ledger agreement that resolves `conflict` in favour of `keep`.
pub fn resolve(conflict: &Conflict, keep: Side) -> Option<ObjectId> {
    match keep {
        Side::Aspen => conflict.upstream,
        Side::Upstream => conflict.aspen,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aspen_core::id::UnixMicros;

    fn values() -> [Option<ObjectId>; 4] {
        [
            None,
            Some(ObjectId::from_bytes([1; 20])),
            Some(ObjectId::from_bytes([2; 20])),
            Some(ObjectId::from_bytes([3; 20])),
        ]
    }

    #[test]
    fn async_mode_never_overwrites_a_change_on_the_other_side() {
        for a in values() {
            for u in values() {
                for l in values() {
                    let act = decide(MirrorMode::Async, a, u, l);
                    let aspen_moved = a != l;
                    let upstream_moved = u != l;
                    match act {
                        Action::InSync => assert_eq!(a, u),
                        // An import replaces Aspen's value: only when Aspen
                        // did not change it.
                        Action::Import => assert!(!aspen_moved && a != u),
                        // A push replaces the upstream's: only when the
                        // upstream did not change it.
                        Action::Push => assert!(!upstream_moved && a != u),
                        Action::Conflict => assert!(aspen_moved && upstream_moved && a != u),
                    }
                    // And every case has a step: a difference is never left
                    // without an import, a push or a recorded conflict.
                    if a != u {
                        assert_ne!(act, Action::InSync, "{a:?} {u:?} {l:?}");
                    }
                }
            }
        }
    }

    #[test]
    fn proxy_mode_always_follows_the_upstream() {
        for a in values() {
            for u in values() {
                for l in values() {
                    let want = if a == u {
                        Action::InSync
                    } else {
                        Action::Import
                    };
                    assert_eq!(decide(MirrorMode::Proxy, a, u, l), want);
                }
            }
        }
    }

    #[test]
    fn first_sync_and_new_refs() {
        let [none, x, y, _] = values();
        // First import: nothing agreed, the upstream has the ref.
        assert_eq!(decide(MirrorMode::Async, none, x, none), Action::Import);
        // A branch first pushed to Aspen.
        assert_eq!(decide(MirrorMode::Async, x, none, none), Action::Push);
        // Created on both sides independently, differently.
        assert_eq!(decide(MirrorMode::Async, x, y, none), Action::Conflict);
        // Deleted in Aspen, untouched upstream: the delete is pushed.
        assert_eq!(decide(MirrorMode::Async, none, x, x), Action::Push);
        // Deleted upstream, untouched in Aspen: the delete is imported.
        assert_eq!(decide(MirrorMode::Async, x, none, x), Action::Import);
    }

    #[test]
    fn resolving_moves_the_ledger_to_the_other_sides_value() {
        let [_, x, y, z] = values();
        let c = Conflict {
            at: UnixMicros(1),
            aspen: x,
            upstream: y,
        };
        let keep_aspen = resolve(&c, Side::Aspen);
        assert_eq!(decide(MirrorMode::Async, x, y, keep_aspen), Action::Push);
        let keep_upstream = resolve(&c, Side::Upstream);
        assert_eq!(
            decide(MirrorMode::Async, x, y, keep_upstream),
            Action::Import
        );
        // If the upstream moved again after the conflict, keeping Aspen's
        // side does not overwrite the newer change.
        assert_eq!(
            decide(MirrorMode::Async, x, z, keep_aspen),
            Action::Conflict
        );
        assert_eq!(
            decide(MirrorMode::Async, z, y, keep_upstream),
            Action::Conflict
        );
    }
}
