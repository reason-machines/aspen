//! Pure fetch planning for Aspen.
//!
//! A fetch is answered by selecting stored packs, never by building a pack
//! per client (`docs/PLAN.md`, fetch path). This crate decides **which**
//! packs: given the ref versions a client wants and the ones whose closure it
//! is known to hold ([`HaveVersion`]), it returns packs whose union contains
//! every object reachable from the wants and not from the haves. It does no
//! I/O and depends only on `aspen-core` (`cargo xtask check-deps` enforces
//! both).
//!
//! # How a plan is built
//!
//! A ref version's **closure** is its own pack plus, through its
//! dependencies (`trunk_dep`, `scoped_dep`), the closures of the versions it
//! was built against. Dependencies, not a ref's version history, define what
//! a version holds: a branch pushed through the plain URL depends on trunk
//! only, not on its own previous version.
//!
//! 1. **Walk** ([`walk`]). The planner paints the dependency graph from the
//!    wants and the haves at once and collects the versions reachable from a
//!    want and not from any have, skipping held history through cover
//!    chains (below). Every dependency has a smaller ID than its
//!    dependent (schema v1 `CHECK`), so visiting versions from the highest ID
//!    down finalises each version's paint before it is expanded, the way
//!    `git rev-list ^<have>` walks commits by date, but exactly: the walk
//!    stops as soon as nothing left to visit is reachable from a want alone.
//!    It visits the versions between the wants and the haves, not the whole
//!    history.
//! 2. **Select** ([`covers`]). Each needed version contributes its own pack,
//!    except where a compacted cover spans a run of needed versions of one
//!    ref: then the cover's single pack replaces theirs. Covers are tried
//!    from the highest level down, so a long run costs a handful of packs
//!    (the levels act as a skip list) instead of one pack per push.
//!
//! # What a plan costs
//!
//! Covers bound the packs sent; cover chains bound the versions visited.
//! The walk visits the versions between the wants and the haves, and on the
//! have side it skips through covers that carry a verified
//! [`CoverChain`](aspen_core::meta::CoverChain) (see "When a cover stands
//! for a have" below): a held version inside a chained cover's range
//! holds every version of the range below it, so the walk jumps to the
//! range's first version instead of stepping through its parents. With the default
//! [`CoverLayout`](aspen_core::meta::CoverLayout) (fanout 16, four
//! levels), a client current on trunk that fetches a branch created N
//! trunk pushes ago visits a few dozen versions, not about N: at most two
//! per cover jumped (its last and first versions), fewer than 16 jumps per
//! level, plus the trunk versions above the newest level-1 cover. A full
//! clone still visits every version in the wants' closure.
//! [`PlanStats::visited`] reports the visits and [`PlanStats::chain_skips`]
//! the jumps. Where no chain exists (a version that does not depend on its
//! parent, such as a forced update, or covers not yet chained) the walk
//! steps version by version, as it always did.
//!
//! # Shallow clients
//!
//! Everything here assumes a complete client: one that holds everything
//! its haves reach. A shallow client does not, so the fetch service plans
//! its requests in [`PlanMode::FullClosure`](aspen_core::fetch::PlanMode),
//! whose [`PlanInput`] carries no haves, and the plan subtracts nothing.
//!
//! # When a cover stands for a have
//!
//! Covers follow one ref's version history; closures follow dependencies.
//! "The client holds version `n` of a ref, so it holds every cover of that
//! ref ending at or below `n`" is false whenever the ref was force-updated
//! (a branch's old versions are not in its new version's closure) or a
//! compare-and-swap recompute recorded a `trunk_dep` that skips a version.
//! An exact forced trunk version (B1's forced-trunk rule) cuts the old
//! trunk history out of later closures the same way.
//!
//! The rule ([`HaveVersion`]): a cover's range may be treated as held for a
//! held version `V` only when the cover carries a
//! [`CoverChain`](aspen_core::meta::CoverChain) and `V`'s `seq` is inside
//! the range; then the part of the range at or below `V` is held, because
//! each version in it depends on its parent (closure containment). Each of
//! the histories above breaks the chain: the forced or recomputed version
//! does not depend on its parent, so
//! [`VerifiedRange::verify`](aspen_core::meta::VerifiedRange::verify)
//! refuses any range that has it above the first seq. An unchained cover
//! never affects what the planner considers held.
//!
//! Only the walk applies the rule. Its result is a [`walk::Needed`] set,
//! which has no way to remove a version, and cover selection only ever
//! reads it. The property tests include a planner that subtracts unchained
//! covers, and the real planner run over forged chains, and check that the
//! oracle catches both.
//!
//! # What the planner relies on
//!
//! - Every live version is **connected**: its tip's objects are in its pack
//!   or its dependencies' closures (push path, connectivity check).
//! - A [`HaveVersion`] is exact and its tip is the have or an ancestor of it,
//!   so the client holds the version's whole closure (`aspen_core::fetch`).
//! - A compacted cover's pack holds exactly the objects of the packs of the
//!   versions in its range (compaction, node C1).
//! - A cover's chain, when it has one, was built by
//!   [`VerifiedRange`](aspen_core::meta::VerifiedRange) over the cover's
//!   own ref and range (node C1). A forged chain loses objects. "At least" is what makes
//!   sending it in their place safe; "exactly" is what keeps it from
//!   over-fetching.
//!
//! # What it sends beyond what the client lacks
//!
//! The selected packs hold exactly the objects of the needed versions'
//! packs (see [`CoverPlanner`]). That is a superset of the objects the
//! client lacks (`git rev-list --objects <wants>` minus
//! `git rev-list --objects <haves>`), and it is larger when:
//!
//! - a have resolves to no [`HaveVersion`] (a non-tip commit with no exact
//!   version at or below it, the tip of a [`Closure::Wider`] version, or a
//!   commit the server has never seen): it is dropped;
//! - a needed pack repeats objects the client has (thin-pack completion
//!   bases, or a branch pushed through the plain URL, whose every pack
//!   carries the branch's history since trunk);
//! - the have-side skip left a held version unproven: one a want reaches
//!   whose only route to the haves runs through a dependency, other than
//!   the parent, of a version the walk skipped (see [`walk`]).
//!
//! [`Closure::Wider`]: aspen_core::refs::Closure::Wider

#![deny(missing_docs)]

pub mod covers;
pub mod walk;

use aspen_core::fetch::{FetchPlan, HaveVersion, PlanError, PlanInput, Planner, VersionGraph};

/// The planner: an exact dependency walk, then cover selection.
///
/// Invariant (property-tested against `git rev-list --objects`): the
/// returned packs' objects are exactly the union of the packs of the
/// versions the walk finds needed ([`walk::Needed`]), with covers standing
/// in for runs of those versions. That set contains every version
/// reachable from a want and not from a have, and nothing outside the
/// wants' closures; it equals the versions reachable from a want and not
/// from a have unless a have-side chain skip left a held version unproven
/// (see [`walk`]). So the packs contain every object reachable from the
/// wants and not from the haves, and no object outside the wants'
/// closures.
#[derive(Debug, Clone, Copy, Default)]
pub struct CoverPlanner;

/// Counters from one plan, for metrics and for tests that bound the cost.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PlanStats {
    /// Versions the walk expanded (needed ones plus the have-side versions
    /// it had to visit to prove them held). Versions skipped through cover
    /// chains are not counted.
    pub visited: usize,
    /// Held ranges the walk registered from cover chains: each one let it
    /// skip the versions of a range below a held version.
    pub chain_skips: usize,
    /// Versions reachable from a want and not from a have.
    pub needed: usize,
    /// Covers whose pack replaced the packs of their versions.
    pub covers_used: usize,
    /// Packs in the plan.
    pub packs: usize,
}

impl CoverPlanner {
    /// Plans one fetch and reports what it cost.
    pub fn plan_with_stats(
        &self,
        graph: &dyn VersionGraph,
        input: &PlanInput,
    ) -> Result<(FetchPlan, PlanStats), PlanError> {
        if input.wants().is_empty() {
            return Err(PlanError::NoWants);
        }
        let haves: &[HaveVersion] = input.haves();
        let needed = walk::Needed::walk(graph, input.wants(), haves)?;
        let selection = covers::select(graph, &needed)?;
        let stats = PlanStats {
            visited: needed.visited(),
            chain_skips: needed.chain_skips(),
            needed: needed.len(),
            covers_used: selection.covers_used,
            packs: selection.packs.len(),
        };
        Ok((
            FetchPlan {
                packs: selection.packs,
            },
            stats,
        ))
    }
}

impl Planner for CoverPlanner {
    fn plan(&self, graph: &dyn VersionGraph, input: &PlanInput) -> Result<FetchPlan, PlanError> {
        self.plan_with_stats(graph, input).map(|(plan, _)| plan)
    }
}
