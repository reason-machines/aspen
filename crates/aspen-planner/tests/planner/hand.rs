//! Hand-built histories, one invariant each. The property tests in
//! `oracle.rs` check the same invariants on random histories against git.

use std::collections::BTreeSet;
use std::num::NonZeroU32;

use aspen_core::fetch::{
    Deepen, FetchRequest, HaveVersion, PlanError, PlanInput, PlanMode, Planner,
};
use aspen_core::id::{ObjectId, PackId, RefVersionId, Seq};
use aspen_core::meta::{ChainError, CoverLayout, SeqRange};
use aspen_core::name::RefName;
use aspen_core::refs::{Closure, RefValue};
use aspen_core::repo::ShallowPolicy;
use aspen_planner::CoverPlanner;

use crate::graph::{Chain, Graph, deps, live, name};

fn obj(n: u32) -> ObjectId {
    let mut b = [0u8; 20];
    b[16..].copy_from_slice(&n.to_be_bytes());
    b[0] = 0xaa;
    ObjectId::from_bytes(b)
}

fn objs(range: std::ops::Range<u32>) -> BTreeSet<ObjectId> {
    range.map(obj).collect()
}

/// The objects of several ranges, spelled out so a test does not derive
/// its expectation through the code under test.
fn objs_in(ranges: &[std::ops::Range<u32>]) -> BTreeSet<ObjectId> {
    ranges.iter().cloned().flatten().map(obj).collect()
}

fn trunk() -> RefName {
    name("refs/heads/main")
}

/// Pushes a trunk version holding `objects`, depending on the current trunk
/// head (an ordinary fast-forward), with tip `tip`.
fn trunk_push(g: &mut Graph, tip: u32, objects: BTreeSet<ObjectId>) -> RefVersionId {
    let pack = g.add_pack(objects, None);
    let dep = g.heads.get(&trunk()).copied();
    g.push(
        &trunk(),
        live(obj(tip), pack, Closure::Exact),
        deps(dep, None),
    )
}

/// A linear trunk of `n` versions; version `i` brings objects
/// `10 i .. 10 i + 3`, with tip `10 i`.
fn linear_trunk(n: u32) -> Graph {
    let mut g = Graph::default();
    for i in 1..=n {
        trunk_push(&mut g, 10 * i, objs(10 * i..10 * i + 3));
    }
    g
}

fn have(g: &Graph, id: RefVersionId) -> HaveVersion {
    let v = &g.versions[&id];
    HaveVersion::exact_tip(v.value.tip().unwrap(), v).expect("an exact tip")
}

fn incremental(wants: Vec<RefVersionId>, haves: Vec<HaveVersion>) -> PlanInput {
    PlanInput::new(PlanMode::Incremental, wants, haves)
}

fn plan_objects(g: &Graph, input: &PlanInput) -> BTreeSet<ObjectId> {
    let plan = CoverPlanner.plan(g, input).unwrap();
    g.objects_of_packs(&plan.packs)
}

/// The objects a correct plan must hold at least: those of the wants'
/// dependency closure minus the haves'. Every pack's objects here are
/// reachable from its own tip, so this is `rev-list <wants> ^<haves>`.
fn expected(g: &Graph, wants: &[RefVersionId], haves: &[RefVersionId]) -> BTreeSet<ObjectId> {
    let w = g.dep_closure(wants.iter().copied());
    let h = g.dep_closure(haves.iter().copied());
    g.objects_of_versions(&w.difference(&h).copied().collect())
}

/// Invariant: the walk ends where the have's closure starts; it does not
/// visit the history below the have.
#[test]
fn an_incremental_fetch_visits_only_the_versions_after_the_have() {
    let g = linear_trunk(50);
    let t = |s| g.at(&trunk(), s).id;
    let input = incremental(vec![t(50)], vec![have(&g, t(47))]);
    let (plan, stats) = CoverPlanner.plan_with_stats(&g, &input).unwrap();
    assert_eq!(
        g.objects_of_packs(&plan.packs),
        expected(&g, &[t(50)], &[t(47)])
    );
    assert_eq!(stats.needed, 3);
    assert_eq!(
        stats.visited, 3,
        "versions 48 to 50; 47 and below are never expanded"
    );
}

/// Invariant: a have on a ref never stands for the ref's earlier versions,
/// nor for a cover of them: after a forced update, the old versions are
/// not in the new version's closure, so a client at the new tip lacks
/// them. Here B1 is the only version holding x's objects, a compacted cover
/// spans B1..B2, and the client holds B2 (an exact version).
#[test]
fn a_forced_branch_version_under_a_cover_is_not_held_by_a_later_have() {
    let mut g = Graph::default();
    let t1 = trunk_push(&mut g, 1, objs(1..4));
    let b = name("refs/heads/b");
    // B1: x, built on trunk, plain URL (depends on trunk only).
    let p1 = g.add_pack(objs(100..105), None);
    let b1 = g.push(&b, live(obj(100), p1, Closure::Exact), deps(Some(t1), None));
    // B2: forced to y, unrelated to x; still exact (trunk's tip is below y).
    let p2 = g.add_pack(objs(200..205), None);
    let b2 = g.push(&b, live(obj(200), p2, Closure::Exact), deps(Some(t1), None));
    g.add_cover(&b, 1, 1, 2, true);
    // B2 does not depend on B1, so the range is no dependency chain: a
    // chain cannot be recorded on this cover.
    assert_eq!(g.add_cover_with(&b, 2, 1, 2, true, Chain::Verified), None);

    let input = incremental(vec![b1], vec![have(&g, b2)]);
    let sent = plan_objects(&g, &input);
    let want = expected(&g, &[b1], &[b2]);
    assert_eq!(want, objs(100..105));
    assert!(sent.is_superset(&want), "lost {:?}", want.difference(&sent));
    // And the cover is not sent instead, because B2 is not needed: that
    // would resend y's objects, which the client holds.
    assert_eq!(sent, want);
}

/// Invariant: an exact forced (or orphan) trunk version cuts the old trunk
/// history out of every later have's closure. Under B1's forced-trunk rule
/// the orphan T3 has no trunk dependency and is exact, and T4 fast-forwards
/// it; a client at T4 holds nothing of T1 and T2, whatever covers span
/// them (the cover's last tip is not an ancestor of the have's tip).
#[test]
fn an_exact_forced_trunk_version_hides_older_trunk_covers_from_later_haves() {
    let mut g = linear_trunk(2);
    let t2 = g.at(&trunk(), 2).id;
    let p3 = g.add_pack(objs(300..305), None);
    g.push(
        &trunk(),
        live(obj(300), p3, Closure::Exact),
        deps(None, None),
    );
    let t4 = trunk_push(&mut g, 400, objs(400..402));
    g.add_cover(&trunk(), 1, 1, 2, true);
    g.add_cover(&trunk(), 2, 1, 4, true);

    let input = incremental(vec![t2], vec![have(&g, t4)]);
    let want = expected(&g, &[t2], &[t4]);
    assert_eq!(want, objs(10..13).union(&objs(20..23)).copied().collect());
    let (plan, stats) = CoverPlanner.plan_with_stats(&g, &input).unwrap();
    assert_eq!(g.objects_of_packs(&plan.packs), want);
    assert_eq!(
        stats.covers_used, 1,
        "the cover over 1..=2 is needed in full"
    );
}

/// Invariant: planning follows dependencies, not a ref's version history.
/// A compare-and-swap recompute can record a trunk dependency that skips a
/// version (T3 was checked against T1 while T2 landed); T3's pack then
/// carries T2's objects itself, and T2 is not in T3's closure.
#[test]
fn a_trunk_dependency_that_skips_a_version_is_followed_exactly() {
    let mut g = Graph::default();
    let t1 = trunk_push(&mut g, 1, objs(1..4));
    let t2 = trunk_push(&mut g, 10, objs(10..13));
    let p3 = g.add_pack(objs(10..13).union(&objs(20..23)).copied().collect(), None);
    let t3 = g.push(
        &trunk(),
        live(obj(20), p3, Closure::Exact),
        deps(Some(t1), None),
    );
    g.add_cover(&trunk(), 1, 1, 3, true);

    // Full closure of T3: T3 and T1, not T2, so the cover over 1..=3
    // (which would also carry T2's pack) is not used.
    let (plan, stats) = CoverPlanner
        .plan_with_stats(&g, &incremental(vec![t3], vec![]))
        .unwrap();
    assert_eq!(stats.needed, 2);
    assert_eq!(stats.covers_used, 0);
    assert_eq!(g.objects_of_packs(&plan.packs), expected(&g, &[t3], &[]));

    // A client at T2 still gets everything T3 brings.
    let input = incremental(vec![t3], vec![have(&g, t2)]);
    assert!(plan_objects(&g, &input).is_superset(&objs(20..23)));
}

/// Invariant: a held version holds everything it depends on, whatever ref
/// it is on. A branch push through the plain URL depends on trunk T3 only;
/// a client holding the branch holds T1 to T3.
#[test]
fn a_have_holds_its_dependencies_on_other_refs() {
    let mut g = linear_trunk(3);
    let t3 = g.at(&trunk(), 3).id;
    let b = name("refs/heads/b");
    let p = g.add_pack(objs(100..103), None);
    let b1 = g.push(&b, live(obj(100), p, Closure::Exact), deps(Some(t3), None));
    trunk_push(&mut g, 40, objs(40..43));
    trunk_push(&mut g, 50, objs(50..53));
    let t5 = g.at(&trunk(), 5).id;
    let (plan, stats) = CoverPlanner
        .plan_with_stats(&g, &incremental(vec![t5], vec![have(&g, b1)]))
        .unwrap();
    assert_eq!(stats.needed, 2);
    assert_eq!(
        g.objects_of_packs(&plan.packs),
        objs(40..43).union(&objs(50..53)).copied().collect()
    );
}

/// Invariant: a scoped push's old version (`scoped_old`) is in its closure,
/// on the want side and the have side. B2 and B3 were pushed through the
/// branch-scoped URL, each naming the previous branch version as its old
/// value, so each pack carries only what that old version lacks: B1's
/// objects are in B2's and B3's closures only through `scoped_old`. The
/// expected sets are spelled out rather than derived through `Deps::iter`,
/// so a walk or `Deps::iter` that drops `scoped_old` fails here, not only
/// against the git oracle.
#[test]
fn a_scoped_push_depends_on_its_old_version() {
    let mut g = linear_trunk(3);
    let t3 = g.at(&trunk(), 3).id;
    let b = name("refs/heads/b");
    let p = g.add_pack(objs(100..103), None);
    let b1 = g.push(&b, live(obj(100), p, Closure::Exact), deps(Some(t3), None));
    let p = g.add_pack(objs(110..113), None);
    let b2 = g.push(
        &b,
        live(obj(110), p, Closure::Exact),
        deps(Some(t3), Some(b1)),
    );
    let p = g.add_pack(objs(120..123), None);
    let b3 = g.push(
        &b,
        live(obj(120), p, Closure::Exact),
        deps(Some(t3), Some(b2)),
    );

    // A client current on trunk wants B2: B2's pack and, through
    // `scoped_old`, B1's.
    let (plan, stats) = CoverPlanner
        .plan_with_stats(&g, &incremental(vec![b2], vec![have(&g, t3)]))
        .unwrap();
    assert_eq!(stats.needed, 2);
    assert_eq!(
        g.objects_of_packs(&plan.packs),
        objs_in(&[100..103, 110..113])
    );

    // A clone of B3 gets trunk and the whole scoped chain.
    let all = objs_in(&[10..13, 20..23, 30..33, 100..103, 110..113, 120..123]);
    let (plan, stats) = CoverPlanner
        .plan_with_stats(&g, &incremental(vec![b3], vec![]))
        .unwrap();
    assert_eq!(stats.needed, 6);
    assert_eq!(g.objects_of_packs(&plan.packs), all);

    // A client holding B2 holds B1 through `scoped_old`: only B3 is sent.
    let (plan, stats) = CoverPlanner
        .plan_with_stats(&g, &incremental(vec![b3], vec![have(&g, b2)]))
        .unwrap();
    assert_eq!(stats.needed, 1);
    assert_eq!(g.objects_of_packs(&plan.packs), objs(120..123));
}

/// Invariant: a shallow request subtracts nothing. "Wants minus haves"
/// holds only for a complete client; a shallow client lacks history below
/// its boundary, so any request carrying `shallow` lines (even without
/// `deepen`), `deepen` or `deepen-relative` plans in `FullClosure` and its
/// plan is the wants' whole closure, although the same exact have would
/// subtract T1 to T4 for a complete client.
#[test]
fn a_shallow_request_plans_the_full_closure() {
    let g = linear_trunk(5);
    let t = |s| g.at(&trunk(), s).id;
    let full = objs_in(&[10..13, 20..23, 30..33, 40..43, 50..53]);
    let requests = [
        FetchRequest {
            client_shallow: vec![obj(40)],
            ..Default::default()
        },
        FetchRequest {
            deepen: Some(Deepen::Depth(NonZeroU32::new(1).unwrap())),
            ..Default::default()
        },
        FetchRequest {
            deepen_relative: true,
            ..Default::default()
        },
    ];
    for request in requests {
        let mode = request.plan_mode(ShallowPolicy::Serve).unwrap();
        assert_eq!(mode, PlanMode::FullClosure, "{request:?}");
        let input = PlanInput::new(mode, vec![t(5)], vec![have(&g, t(4))]);
        assert!(input.haves().is_empty(), "{request:?}");
        assert_eq!(plan_objects(&g, &input), full, "{request:?}");
    }

    let complete = FetchRequest::default()
        .plan_mode(ShallowPolicy::Serve)
        .unwrap();
    let input = PlanInput::new(complete, vec![t(5)], vec![have(&g, t(4))]);
    assert_eq!(plan_objects(&g, &input), objs(50..53));
}

/// Invariant: a cover is sent only when every version in its range is
/// needed, so covers never add objects the client holds; uncompacted
/// covers are never sent.
#[test]
fn a_cover_is_sent_only_for_a_fully_needed_range() {
    let mut g = linear_trunk(8);
    let t = |g: &Graph, s| g.at(&trunk(), s).id;
    g.add_cover(&trunk(), 1, 1, 4, true);
    g.add_cover(&trunk(), 1, 5, 8, false);
    let (t8, t2) = (t(&g, 8), t(&g, 2));

    // Client at T2: 1..=4 is not fully needed, and 5..=8 has no pack.
    let (plan, stats) = CoverPlanner
        .plan_with_stats(&g, &incremental(vec![t8], vec![have(&g, t2)]))
        .unwrap();
    assert_eq!(stats.covers_used, 0);
    assert_eq!(plan.packs.len(), 6);
    assert_eq!(g.objects_of_packs(&plan.packs), expected(&g, &[t8], &[t2]));

    // Full clone: the compacted cover replaces T1 to T4.
    let (plan, stats) = CoverPlanner
        .plan_with_stats(&g, &incremental(vec![t8], vec![]))
        .unwrap();
    assert_eq!(stats.covers_used, 1);
    assert_eq!(plan.packs.len(), 5);
    assert_eq!(g.objects_of_packs(&plan.packs), expected(&g, &[t8], &[]));
}

/// Invariant: the levels work as a skip list. With aligned covers of span
/// 4, 16, 64 and 256 over 1,000 trunk pushes, any incremental fetch of
/// trunk takes at most 3 packs per level and side (30), and sends exactly
/// the missing objects.
#[test]
fn cover_levels_bound_the_pack_count_logarithmically() {
    let n = 1000;
    let mut g = linear_trunk(n);
    for (level, span) in [(1u8, 4u64), (2, 16), (3, 64), (4, 256)] {
        let mut first = 1;
        while first + span - 1 <= u64::from(n) {
            g.add_cover(&trunk(), level, first, first + span - 1, true);
            first += span;
        }
    }
    let t = |s: u64| g.at(&trunk(), s).id;
    let mut worst = 0;
    for h in [
        None,
        Some(1),
        Some(2),
        Some(3),
        Some(17),
        Some(255),
        Some(511),
        Some(700),
        Some(999),
    ] {
        let haves: Vec<_> = h.map(|s| have(&g, t(s))).into_iter().collect();
        let held: Vec<_> = h.map(t).into_iter().collect();
        let input = incremental(vec![t(u64::from(n))], haves);
        let (plan, stats) = CoverPlanner.plan_with_stats(&g, &input).unwrap();
        assert_eq!(
            g.objects_of_packs(&plan.packs),
            expected(&g, &[t(u64::from(n))], &held),
            "have at {h:?}"
        );
        assert_eq!(stats.needed as u64, u64::from(n) - h.unwrap_or(0));
        assert_eq!(
            stats.visited, stats.needed,
            "no have-side version is expanded"
        );
        worst = worst.max(plan.packs.len());
    }
    assert!(worst <= 30, "{worst} packs for one fetch");
}

/// Invariant: writes that share one pack row (identical split packs in
/// one push) send it once.
#[test]
fn a_pack_shared_by_two_versions_is_sent_once() {
    let mut g = linear_trunk(1);
    let t1 = g.at(&trunk(), 1).id;
    let p = g.add_pack(objs(100..103), None);
    let a = g.push(
        &name("refs/heads/a"),
        live(obj(100), p, Closure::Exact),
        deps(Some(t1), None),
    );
    let b = g.push(
        &name("refs/tags/b"),
        live(obj(100), p, Closure::Exact),
        deps(Some(t1), None),
    );
    let plan = CoverPlanner
        .plan(&g, &incremental(vec![a, b], vec![]))
        .unwrap();
    assert_eq!(plan.packs.len(), 2);
    assert_eq!(plan.packs.iter().collect::<BTreeSet<_>>().len(), 2);
}

/// Invariant: a shallow (full-closure) request plans the whole closure even
/// when the client sent haves (S5), because `PlanInput` drops them.
#[test]
fn a_full_closure_request_ignores_haves() {
    let g = linear_trunk(5);
    let t = |s| g.at(&trunk(), s).id;
    let input = PlanInput::new(PlanMode::FullClosure, vec![t(5)], vec![have(&g, t(4))]);
    assert_eq!(plan_objects(&g, &input), expected(&g, &[t(5)], &[]));
}

/// Invariant: a deleted version contributes nothing and needs no pack, and
/// a want held entirely by the haves gives an empty plan.
#[test]
fn deletions_and_fully_held_wants_send_nothing() {
    let mut g = linear_trunk(2);
    let b = name("refs/heads/b");
    let deleted = g.push(&b, RefValue::Deleted, deps(None, None));
    let t2 = g.at(&trunk(), 2).id;
    let t1 = g.at(&trunk(), 1).id;
    let plan = CoverPlanner
        .plan(&g, &incremental(vec![deleted, t1], vec![have(&g, t2)]))
        .unwrap();
    assert!(plan.packs.is_empty());
}

/// Invariant: an incomplete graph fails the plan (a retryable fetch
/// error) instead of producing a plan that silently lacks objects.
#[test]
fn an_incomplete_graph_is_an_error_not_a_short_plan() {
    let mut g = linear_trunk(3);
    let t = |g: &Graph, s| g.at(&trunk(), s).id;
    let (t1, t3) = (t(&g, 1), t(&g, 3));
    assert_eq!(
        CoverPlanner.plan(&g, &incremental(vec![], vec![])),
        Err(PlanError::NoWants)
    );

    let pack: PackId = g.versions[&t1].value.pack().unwrap();
    let row = g.packs.remove(&pack).unwrap();
    assert_eq!(
        CoverPlanner.plan(&g, &incremental(vec![t3], vec![])),
        Err(PlanError::MissingPack(pack))
    );
    g.packs.insert(pack, row);

    g.versions.remove(&t1);
    assert_eq!(
        CoverPlanner.plan(&g, &incremental(vec![t3], vec![])),
        Err(PlanError::MissingVersion(t1))
    );
}

/// Adds every cover `layout` places over `ref_name`'s versions `1..=n`,
/// with chains as `chain` says.
fn lay_covers(
    g: &mut Graph,
    ref_name: &RefName,
    n: u64,
    layout: CoverLayout,
    chain: Chain,
    compacted: bool,
) {
    for seq in 1..=n {
        for (level, range) in layout.ending_at(Seq::new(seq).unwrap()) {
            g.add_cover_with(
                ref_name,
                level.get(),
                range.first().get(),
                range.last().get(),
                compacted,
                chain,
            );
        }
    }
}

fn seq_range(first: u64, last: u64) -> SeqRange {
    SeqRange::new(Seq::new(first).unwrap(), Seq::new(last).unwrap()).unwrap()
}

/// A trunk of `n` versions, each depending on its parent, and a branch
/// created when trunk was at version `k`, depending on it; covers of the
/// default layout over trunk. Returns the graph and the branch version.
fn stale_branch(n: u32, k: u32, chain: Chain, compacted: bool) -> (Graph, RefVersionId) {
    let mut g = Graph::default();
    let mut branch = None;
    for i in 1..=n {
        trunk_push(&mut g, 10 * i, objs(10 * i..10 * i + 3));
        if i == k {
            let tk = g.heads[&trunk()];
            let p = g.add_pack(objs(900_000..900_003), None);
            branch = Some(g.push(
                &name("refs/heads/stale"),
                live(obj(900_000), p, Closure::Exact),
                deps(Some(tk), None),
            ));
        }
    }
    lay_covers(
        &mut g,
        &trunk(),
        u64::from(n),
        CoverLayout::default(),
        chain,
        compacted,
    );
    (g, branch.expect("k <= n"))
}

/// The visits the stale-branch fetch may cost with chains: fanout 16, four
/// levels. A jump costs at most two visits (the range's last and first
/// versions); the newest versions are reached in at most 16 + 2 × 15 per
/// level in the worst case, but the trunk lengths tested here need far
/// fewer, so the bound is "about one level's worth per level".
const LOG_VISITS: usize = 4 * 16 + 4;

/// Invariant: the have-side skip makes the stale-branch fetch logarithmic.
/// A client current on trunk fetches a branch created at trunk version k:
/// without chains the walk expands every trunk version from the tip down to
/// k (about N); with aligned chained covers it jumps through them, and the
/// plan is the same: exactly the branch's pack.
#[test]
fn a_stale_branch_fetch_visits_logarithmically_many_versions_through_chains() {
    for n in [1000u32, 5000] {
        for k in [7, n / 2 + 3] {
            let mut visits = Vec::new();
            for chain in [Chain::Verified, Chain::None] {
                let (g, branch) = stale_branch(n, k, chain, true);
                let tip = g.heads[&trunk()];
                let input = incremental(vec![branch], vec![have(&g, tip)]);
                let (plan, stats) = CoverPlanner.plan_with_stats(&g, &input).unwrap();
                let branch_pack = g.versions[&branch].value.pack().unwrap();
                assert_eq!(plan.packs, vec![branch_pack], "n {n}, k {k}, {chain:?}");
                assert_eq!(stats.needed, 1);
                if chain == Chain::Verified {
                    assert!(stats.chain_skips > 0);
                    assert!(
                        stats.visited <= LOG_VISITS,
                        "n {n}, k {k}: {} visits with chains",
                        stats.visited
                    );
                } else {
                    assert_eq!(stats.chain_skips, 0);
                    assert!(
                        stats.visited as u32 >= n - k,
                        "n {n}, k {k}: {} visits without chains",
                        stats.visited
                    );
                }
                visits.push(stats.visited);
            }
            eprintln!(
                "n {n}, k {k}: {} visits with chains, {} without",
                visits[0], visits[1]
            );
        }
    }
}

/// Invariant: a chain needs no compaction pack. Covers that are chained
/// but not compacted (no version in the range has a pack, or the repack
/// has not run) skip the same way.
#[test]
fn chained_covers_without_a_pack_still_skip() {
    let (g, branch) = stale_branch(1000, 7, Chain::Verified, false);
    assert!(
        g.covers[&trunk()]
            .iter()
            .all(|c| c.pack.is_none() && c.chain.is_some())
    );
    let tip = g.heads[&trunk()];
    let input = incremental(vec![branch], vec![have(&g, tip)]);
    let (plan, stats) = CoverPlanner.plan_with_stats(&g, &input).unwrap();
    assert_eq!(plan.packs, vec![g.versions[&branch].value.pack().unwrap()]);
    assert!(stats.chain_skips > 0);
    assert!(stats.visited <= LOG_VISITS, "{} visits", stats.visited);
}

/// A trunk of 64 versions with covers of fanout 4 (spans 4, 16, 64), in
/// which T40 depends on T38, not on its parent T39 (a compare-and-swap
/// recompute; its pack carries T39's objects itself).
fn trunk_with_a_skipping_version() -> Graph {
    let mut g = Graph::default();
    for i in 1..=64u32 {
        if i == 40 {
            let t38 = g.at(&trunk(), 38).id;
            let p = g.add_pack(objs_in(&[390..393, 400..403]), None);
            g.push(
                &trunk(),
                live(obj(400), p, Closure::Exact),
                deps(Some(t38), None),
            );
        } else {
            trunk_push(&mut g, 10 * i, objs(10 * i..10 * i + 3));
        }
    }
    lay_covers(
        &mut g,
        &trunk(),
        64,
        CoverLayout::new(4).unwrap(),
        Chain::Verified,
        true,
    );
    g
}

/// Invariant: a version that does not depend on its parent breaks every
/// range that has it above the first seq; such covers carry no chain, and
/// the walk steps version by version there instead of skipping. T39 is not
/// in T64's closure (T40 skips it), so a client at T64 that wants T39 gets
/// T39's pack, and the walk visits T64, jumps (T49, T48, T45, T44, T41),
/// steps through T40 and ends at T39.
#[test]
fn a_version_that_does_not_depend_on_its_parent_breaks_the_chain() {
    let g = trunk_with_a_skipping_version();
    let broken = Err(ChainError::Broken {
        name: trunk(),
        seq: Seq::new(40).unwrap(),
    });
    for (first, last) in [(37, 40), (33, 48), (1, 64)] {
        assert_eq!(
            g.verify(&trunk(), seq_range(first, last))
                .map(|r| r.range()),
            broken.clone()
        );
        let cover = g.covers[&trunk()]
            .iter()
            .find(|c| c.range == seq_range(first, last))
            .unwrap();
        assert_eq!(cover.chain, None);
    }
    // A range that starts at T40 is a chain: the first version's own
    // dependencies do not matter.
    assert!(g.verify(&trunk(), seq_range(40, 44)).is_ok());

    let t = |s| g.at(&trunk(), s).id;
    let input = incremental(vec![t(39)], vec![have(&g, t(64))]);
    let (plan, stats) = CoverPlanner.plan_with_stats(&g, &input).unwrap();
    assert_eq!(expected(&g, &[t(39)], &[t(64)]), objs(390..393));
    assert_eq!(g.objects_of_packs(&plan.packs), objs(390..393));
    assert_eq!(stats.needed, 1);
    assert_eq!(stats.chain_skips, 3);
    assert_eq!(stats.visited, 8);

    // Below the break the walk skips again: T20 is held through T40's
    // dependency T38 and the chains under it.
    let input = incremental(vec![t(20)], vec![have(&g, t(64))]);
    let (plan, stats) = CoverPlanner.plan_with_stats(&g, &input).unwrap();
    assert!(plan.packs.is_empty());
    assert!(stats.chain_skips >= 5, "{stats:?}");
    assert!(stats.visited < 20, "{stats:?}");
}

/// Invariant: held ranges belong to one ref. A branch's versions have
/// sequence numbers inside trunk's held range, and are still sent.
#[test]
fn a_held_range_never_hides_a_needed_version_of_another_ref() {
    let mut g = linear_trunk(10);
    let t10 = g.at(&trunk(), 10).id;
    let b = name("refs/heads/b");
    let p = g.add_pack(objs(100..103), None);
    let b1 = g.push(&b, live(obj(100), p, Closure::Exact), deps(Some(t10), None));
    let p = g.add_pack(objs(110..113), None);
    let b2 = g.push(
        &b,
        live(obj(110), p, Closure::Exact),
        deps(Some(t10), Some(b1)),
    );
    let p = g.add_pack(objs(120..123), None);
    let b3 = g.push(
        &b,
        live(obj(120), p, Closure::Exact),
        deps(Some(t10), Some(b2)),
    );
    for i in 11..=32 {
        trunk_push(&mut g, 10 * i, objs(10 * i..10 * i + 3));
    }
    lay_covers(
        &mut g,
        &trunk(),
        32,
        CoverLayout::new(4).unwrap(),
        Chain::Verified,
        true,
    );
    // The branch's own range is a chain too, but no branch version is held.
    g.add_cover_with(&b, 1, 1, 3, true, Chain::Verified)
        .unwrap();

    let t32 = g.heads[&trunk()];
    let input = incremental(vec![b3], vec![have(&g, t32)]);
    let (plan, stats) = CoverPlanner.plan_with_stats(&g, &input).unwrap();
    assert!(stats.chain_skips > 0);
    assert_eq!(stats.needed, 3);
    assert_eq!(
        g.objects_of_packs(&plan.packs),
        objs_in(&[100..103, 110..113, 120..123])
    );
    assert_eq!(g.objects_of_packs(&plan.packs), expected(&g, &[b3], &[t32]));
}

/// Invariant: a want inside a held range is held. The client at T64 holds
/// every trunk version through the chained cover over 1..=64; wants at T20
/// and T50 send nothing, and the walk expands only T64 and the two wants.
#[test]
fn a_want_inside_a_held_range_is_not_sent() {
    let mut g = linear_trunk(64);
    lay_covers(
        &mut g,
        &trunk(),
        64,
        CoverLayout::new(4).unwrap(),
        Chain::Verified,
        true,
    );
    let t = |s| g.at(&trunk(), s).id;
    let input = incremental(vec![t(20), t(50)], vec![have(&g, t(64))]);
    let (plan, stats) = CoverPlanner.plan_with_stats(&g, &input).unwrap();
    assert!(plan.packs.is_empty());
    assert_eq!(stats.needed, 0);
    assert_eq!(stats.chain_skips, 1);
    assert_eq!(stats.visited, 3);
}

/// The skip's documented cost (`aspen_planner::walk`): it can send a
/// version the client holds, never lose one. Branch b was pushed through
/// the scoped URL, so b1..b4 is a dependency chain, and each version also
/// depends on the trunk of its time; trunk was forced to an orphan at T3.
/// A client at b4 holds T2 (through b3 -> b2 -> T2), but the walk skips b3
/// and b2, and T2 is held by nothing else, so it is sent. Without the chain
/// the walk proves it held. A walk that tightens this may update the test.
#[test]
fn a_skip_may_resend_what_only_a_skipped_versions_other_dependency_holds() {
    let b = name("refs/heads/b");
    let mut plans = Vec::new();
    for chain in [Chain::Verified, Chain::None] {
        let mut g = Graph::default();
        let t1 = trunk_push(&mut g, 1, objs(1..4));
        let p = g.add_pack(objs(100..103), None);
        let b1 = g.push(&b, live(obj(100), p, Closure::Exact), deps(Some(t1), None));
        let t2 = trunk_push(&mut g, 10, objs(10..13));
        let p = g.add_pack(objs(110..113), None);
        let b2 = g.push(
            &b,
            live(obj(110), p, Closure::Exact),
            deps(Some(t2), Some(b1)),
        );
        let p = g.add_pack(objs(20..23), None);
        let t3 = g.push(&trunk(), live(obj(20), p, Closure::Exact), deps(None, None));
        let p = g.add_pack(objs(120..123), None);
        let b3 = g.push(
            &b,
            live(obj(120), p, Closure::Exact),
            deps(Some(t3), Some(b2)),
        );
        let t4 = trunk_push(&mut g, 30, objs(30..33));
        let p = g.add_pack(objs(130..133), None);
        let b4 = g.push(
            &b,
            live(obj(130), p, Closure::Exact),
            deps(Some(t4), Some(b3)),
        );
        g.add_cover_with(&b, 1, 1, 4, true, chain);

        assert!(expected(&g, &[t2], &[b4]).is_empty());
        let input = incremental(vec![t2], vec![have(&g, b4)]);
        let (plan, stats) = CoverPlanner.plan_with_stats(&g, &input).unwrap();
        plans.push((g.objects_of_packs(&plan.packs), stats.chain_skips));
    }
    assert_eq!(plans[0], (objs(10..13), 1), "with the chain: T2 is resent");
    assert_eq!(plans[1], (BTreeSet::new(), 0), "without: nothing is sent");
}
