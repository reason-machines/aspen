//! Property tests against `git rev-list --objects`.
//!
//! Each seed builds a [`World`]: a real repository, the ref-version graph a
//! sequence of pushes would leave, and covers over it. For each query the
//! planned packs must hold every object `git rev-list --objects <wants>
//! --not <haves>` lists (no object lost), and exactly the objects of the
//! packs of the versions reachable from a want and not from a have (bounded
//! over-fetch: nothing the dependency graph does not require). A plan that
//! skipped held history through a cover chain is held to "at least those
//! packs, and nothing outside the wants' closure" instead: the skip may
//! leave a held version unproven (`aspen_planner::walk`), which costs
//! bytes, never objects.
//!
//! Replay one seed:
//!
//! ```text
//! ASPEN_PLANNER_SEED=0x2a cargo test -p aspen-planner --test planner replay_one_seed -- --ignored --nocapture
//! ```
//!
//! The nightly workflow runs `many_random_histories` (ignored here) with
//! fresh seeds; `ASPEN_PLANNER_CASES` sets how many.

use std::collections::BTreeSet;

use aspen_core::fetch::PlanInput;
use aspen_core::id::{PackId, RefVersionId};
use aspen_planner::CoverPlanner;

use crate::graph::Graph;
use crate::world::{Chains, Coverage, World};

/// What a planner under test selected.
struct Planned {
    packs: Vec<PackId>,
    covers_used: usize,
    chain_skips: usize,
}

/// A planner under test.
type PlanFn<'a> = &'a dyn Fn(&Graph, &PlanInput) -> Planned;

fn cover_planner(g: &Graph, input: &PlanInput) -> Planned {
    let (plan, stats) = CoverPlanner
        .plan_with_stats(g, input)
        .expect("the graph is complete");
    Planned {
        packs: plan.packs,
        covers_used: stats.covers_used,
        chain_skips: stats.chain_skips,
    }
}

/// Checks every query of the world. `exact` also checks that nothing beyond
/// the needed versions' packs is sent (for a plan that took a chain skip:
/// at least those packs, and nothing outside the wants' closure).
fn check(world: &World, plan: PlanFn<'_>, exact: bool) -> Result<Coverage, String> {
    let g = &world.graph;
    let mut coverage = world.coverage;
    for (i, q) in world.queries.iter().enumerate() {
        let Planned {
            packs,
            covers_used,
            chain_skips,
        } = plan(g, &q.input);
        coverage.covers_used += covers_used;
        coverage.chain_skips += chain_skips;
        coverage.queries_with_chain_skips += usize::from(chain_skips > 0);
        let sent = g.objects_of_packs(&packs);
        let missing = world.missing_objects(q);
        let lost: Vec<_> = missing.difference(&sent).collect();
        coverage.objects_missing += missing.len();
        coverage.objects_planned += sent.len();
        if std::env::var_os("ASPEN_PLANNER_EXPLAIN").is_some() {
            let boundary = world.boundary_objects(q);
            eprintln!(
                "query {i}: exact missing {}, rev-list --not lists {} ({} extra), planned {}",
                missing.len(),
                boundary.len(),
                boundary.difference(&missing).count(),
                sent.len()
            );
        }
        let fail = |what: String| {
            Err(format!(
                "seed {:#x}, query {i}: {what}\n  query: {}\n  plan: {:?}\n  history:\n{}\n  replay: ASPEN_PLANNER_SEED={:#x} cargo test -p aspen-planner --test planner replay_one_seed -- --ignored --nocapture",
                world.seed,
                q.describe,
                packs.iter().map(|p| p.get()).collect::<Vec<_>>(),
                world.log,
                world.seed,
            ))
        };
        if !lost.is_empty() {
            return fail(format!(
                "{} of {} objects git lists as missing are in no planned pack, e.g. {:?}",
                lost.len(),
                missing.len(),
                &lost[..lost.len().min(3)]
            ));
        }
        if exact {
            if packs.iter().collect::<BTreeSet<_>>().len() != packs.len() {
                return fail("a pack is planned twice".into());
            }
            let held = g.dep_closure(q.input.haves().iter().map(|h| h.version()));
            let wanted = g.dep_closure(q.input.wants().iter().copied());
            let needed: BTreeSet<RefVersionId> = wanted.difference(&held).copied().collect();
            let expected = g.objects_of_versions(&needed);
            if chain_skips == 0 && sent != expected {
                return fail(format!(
                    "planned {} objects, the needed versions' packs hold {}",
                    sent.len(),
                    expected.len()
                ));
            }
            if chain_skips > 0 {
                if !sent.is_superset(&expected) {
                    return fail(format!(
                        "after a chain skip, {} objects of the needed versions' packs are not planned",
                        expected.difference(&sent).count()
                    ));
                }
                let reachable = g.objects_of_versions(&wanted);
                if !sent.is_subset(&reachable) {
                    return fail(format!(
                        "after a chain skip, {} planned objects are outside the wants' closure",
                        sent.difference(&reachable).count()
                    ));
                }
                coverage.chain_skip_overfetches += usize::from(sent != expected);
            }
        }
    }
    Ok(coverage)
}

fn run_seeds(seeds: impl IntoIterator<Item = u64>) -> Coverage {
    let mut total = Coverage::default();
    for seed in seeds {
        let world = World::generate(seed);
        match check(&world, &cover_planner, true) {
            Ok(c) => total.add(&c),
            Err(e) => panic!("{e}"),
        }
    }
    total
}

fn env_u64(name: &str) -> Option<u64> {
    let v = std::env::var(name).ok()?;
    let v = v.trim();
    Some(match v.strip_prefix("0x") {
        Some(hex) => u64::from_str_radix(hex, 16).expect("hex seed"),
        None => v.parse().expect("decimal number"),
    })
}

// Required CI: 64 fixed seeds, split so the test harness runs them in
// parallel. Each seed is a fresh history with six queries.
#[test]
fn plans_never_lose_objects_seeds_00_to_15() {
    run_seeds(0..16);
}
#[test]
fn plans_never_lose_objects_seeds_16_to_31() {
    run_seeds(16..32);
}
#[test]
fn plans_never_lose_objects_seeds_32_to_47() {
    run_seeds(32..48);
}
#[test]
fn plans_never_lose_objects_seeds_48_to_63() {
    run_seeds(48..64);
}

/// The generator exercises every history the planner must survive; a
/// change that stops producing one of them makes the property tests above
/// vacuous for it.
#[test]
fn the_generator_produces_every_hard_case() {
    let c = run_seeds(1000..1016);
    eprintln!("{c:#?}");
    let cases = [
        (
            "forced versions under compacted covers",
            c.forced_under_compacted_cover,
        ),
        (
            "trunk dependencies that skip a version (CAS recompute)",
            c.trunk_dep_skips,
        ),
        ("forced trunk updates", c.forced_trunk),
        (
            "exact forced trunk updates (B1's rule)",
            c.exact_forced_trunk,
        ),
        ("wider versions", c.wider_versions),
        ("scoped-URL pushes", c.scoped_pushes),
        ("deletions", c.deletions),
        ("annotated tags", c.tags),
        ("compacted covers", c.compacted_covers),
        ("full-closure (shallow) queries", c.full_closure_queries),
        (
            "haves resolved through ancestry (non-tip commits)",
            c.haves_resolved_by_ancestry,
        ),
        ("haves the server has never seen", c.haves_unknown_to_server),
        ("haves dropped as unresolvable", c.haves_dropped),
        ("plans that used a cover", c.covers_used),
        ("covers with a verified chain", c.chained_covers),
        ("plans that skipped through a cover chain", c.chain_skips),
    ];
    for (what, n) in cases {
        assert!(n > 0, "no {what} in 16 seeds");
    }
}

/// A planner that subtracts unchained covers for haves, the rule the
/// planner must not follow: "the client holds version n of a ref, so it
/// holds every compacted cover of that ref ending at or below n". The
/// property tests must catch it; if they ever stop doing so, they have lost
/// the histories (forced branch versions under covers) that make it unsafe.
/// Chained covers are left out: treating those as held below a held
/// version is what the chain makes safe.
fn subtracting_planner(g: &Graph, input: &PlanInput) -> Planned {
    let mut held: BTreeSet<RefVersionId> = input.haves().iter().map(|h| h.version()).collect();
    for h in input.haves() {
        let hv = &g.versions[&h.version()];
        for c in aspen_core::fetch::VersionGraph::covers(g, &hv.name) {
            if c.chain.is_none() && c.pack.is_some() && c.range.last() <= hv.seq {
                held.extend(
                    g.versions
                        .values()
                        .filter(|v| v.name == hv.name && c.range.contains(v.seq))
                        .map(|v| v.id),
                );
            }
        }
    }
    let held = g.dep_closure(held);
    let wanted = g.dep_closure(input.wants().iter().copied());
    let packs: BTreeSet<PackId> = wanted
        .difference(&held)
        .filter_map(|id| g.versions[id].value.pack())
        .collect();
    Planned {
        packs: packs.into_iter().collect(),
        covers_used: 0,
        chain_skips: 0,
    }
}

#[test]
fn unsafe_cover_subtraction_is_caught_by_the_oracle() {
    let mut caught = Vec::new();
    for seed in 0..64 {
        let world = World::generate(seed);
        if let Err(e) = check(&world, &subtracting_planner, false) {
            caught.push(seed);
            if caught.len() == 1 {
                eprintln!("first catch:\n{e}");
            }
        }
    }
    eprintln!("caught on {} of 64 seeds: {caught:?}", caught.len());
    assert!(
        !caught.is_empty(),
        "the oracle never caught cover subtraction"
    );
}

/// The real planner over worlds whose unverifiable covers carry forged
/// chains ([`Chains::Forged`]): a chain is what lets the walk treat a range
/// as held, so a store that recorded one on a range that is not a
/// dependency chain would lose objects. The oracle must catch that, which
/// shows the property tests reach the histories a chain must exclude.
#[test]
fn a_planner_trusting_forged_chains_is_caught_by_the_oracle() {
    let mut caught = Vec::new();
    let mut forged = 0;
    for seed in 0..64 {
        let world = World::generate_with(seed, Chains::Forged);
        forged += world.coverage.forged_chains;
        if let Err(e) = check(&world, &cover_planner, false) {
            caught.push(seed);
            if caught.len() == 1 {
                eprintln!("first catch:\n{e}");
            }
        }
    }
    eprintln!(
        "{forged} forged chains; caught on {} of 64 seeds: {caught:?}",
        caught.len()
    );
    assert!(!caught.is_empty(), "the oracle never caught a forged chain");
}

/// Nightly: fresh seeds each run; the base seed is printed so a failure
/// replays.
#[test]
#[ignore = "slow; runs nightly"]
fn many_random_histories() {
    let cases = env_u64("ASPEN_PLANNER_CASES").unwrap_or(2000);
    let base = env_u64("ASPEN_PLANNER_SEED").unwrap_or_else(aspen_testkit::oracle::seed_from_clock);
    eprintln!("base seed {base:#x}, {cases} histories");
    let c = run_seeds((0..cases).map(|i| base.wrapping_add(i)));
    eprintln!("{c:#?}");
}

/// Replays `ASPEN_PLANNER_SEED` and prints its history. Without the
/// variable it does nothing, so `--include-ignored` runs (nightly) pass it.
#[test]
#[ignore = "manual replay"]
fn replay_one_seed() {
    let Some(seed) = env_u64("ASPEN_PLANNER_SEED") else {
        eprintln!("set ASPEN_PLANNER_SEED to replay a seed");
        return;
    };
    let world = World::generate(seed);
    eprintln!("history:\n{}", world.log);
    for q in &world.queries {
        eprintln!("query: {}", q.describe);
    }
    if let Err(e) = check(&world, &cover_planner, true) {
        panic!("{e}");
    }
}
