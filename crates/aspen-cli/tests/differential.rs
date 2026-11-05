//! The differential oracle (`aspen-testkit`, `docs/PLAN.md` section 4.1)
//! against the real `aspen` binary: `aspen serve` over SQLite and a local
//! blob store, compared step by step with stock `git http-backend`.
//!
//! Required CI runs a small fixed-seed slice (a few seconds); the nightly
//! workflow runs thousands of operations with fresh seeds, on SQLite and on
//! PostgreSQL (`.github/workflows/nightly.yml`, `differential-long`).
//! `ASPEN_ORACLE_SEQUENCES` raises the sequence count here too.
//! `testkit/scripts/check-aspen-mutations.py` shows this run fails when
//! Aspen misbehaves.

use aspen_testkit::oracle::seeds;
use aspen_testkit::{AspenOptions, AspenServer, Branch, Config, Op, Oracle, StockServer};

fn aspen() -> AspenServer {
    AspenServer::start(AspenOptions::new(env!("CARGO_BIN_EXE_aspen"))).expect("aspen serve starts")
}

/// Pushes and races also go through the plain repository URL, where Aspen
/// sees a zero old value for every branch but trunk and decides
/// fast-forwards itself (issue #135).
fn config() -> Config {
    Config {
        steps: 12,
        clones: 3,
        branches: 4,
        protect_trunk: true,
        plain_url_pushes: true,
    }
}

/// Aspen agrees with stock Git on a fixed slice of random sequences:
/// pushes, forced pushes, deletes, merges, fetches, concurrent pushes and
/// same-branch races, through branch URLs and through the plain repository
/// URL. Nothing is stepped over.
#[test]
fn aspen_agrees_with_stock_git_on_a_fixed_seed_slice() {
    let reference = StockServer::start().unwrap();
    let candidate = aspen();
    let sequences = std::env::var("ASPEN_ORACLE_SEQUENCES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3);
    let oracle = Oracle::new(&reference, &candidate, config());
    let report = oracle.check_seeds(seeds(0xa5be_0000, sequences)).unwrap();
    if let Some(divergence) = &report.divergence {
        panic!(
            "aspen diverged from stock git:\n{divergence}\n\naspen log tail:\n{}",
            candidate.log_tail(40)
        );
    }
    assert_eq!(report.clean_sequences, sequences);
    // A slice that pushed nothing, rejected nothing or never raced proves
    // little; these seeds do all of it.
    let stats = report.stats;
    assert_eq!(stats.local_failures, 0, "harness bug: {stats}");
    assert!(stats.refs_accepted >= 3 * sequences, "{stats}");
    assert!(stats.refs_rejected > 0, "{stats}");
    assert!(stats.fetches > 0, "{stats}");
    assert!(stats.races > 0, "{stats}");
    // Rejections off trunk through the plain URL are Aspen's own
    // fast-forward check: the client sent a zero old value.
    assert!(stats.plain_url_branch_rejected > 0, "{stats}");
    assert!(
        stats.plain_url_updates > stats.plain_url_branch_rejected,
        "{stats}"
    );
}

/// Through the plain repository URL Aspen advertises only trunk, so a push
/// of a topic branch carries a zero old value and Aspen alone decides
/// whether it is a fast-forward (S2; issue #135). A stale clone's push,
/// which stock Git's client refuses itself, must be refused by Aspen too;
/// with `-o force` it is accepted on both; and a race through the plain URL
/// has exactly one winner whose losers Aspen refuses as non-fast-forward.
/// `testkit/scripts/check-aspen-mutations.py` (L4) shows the oracle catches
/// an Aspen that accepts the stale push without force.
#[test]
fn a_stale_push_through_the_plain_url_is_refused_by_aspen_itself() {
    let reference = StockServer::start().unwrap();
    let candidate = aspen();
    let topic = Branch(1);
    let ops = [
        Op::Push {
            clone: 1,
            branch: topic,
            commits: 1,
        },
        Op::Fetch { clone: 2 },
        Op::Push {
            clone: 1,
            branch: topic,
            commits: 1,
        },
        // Clone 2 last saw topic-1 one commit ago.
        Op::PlainPush {
            clone: 2,
            branch: topic,
            commits: 1,
            force: false,
        },
        Op::PlainPush {
            clone: 2,
            branch: topic,
            commits: 1,
            force: true,
        },
        Op::PlainRace {
            clones: vec![0, 1],
            branch: topic,
        },
        // A fast-forward through the plain URL is accepted.
        Op::Fetch { clone: 0 },
        Op::PlainPush {
            clone: 0,
            branch: topic,
            commits: 2,
            force: false,
        },
    ];
    let oracle = Oracle::new(&reference, &candidate, config());
    let run = oracle.run(&ops).unwrap();
    if let Some((step, kind)) = &run.divergence {
        panic!(
            "aspen diverged after {step}: {kind}

aspen log tail:
{}",
            candidate.log_tail(40)
        );
    }
    let stats = run.stats;
    assert_eq!(stats.local_failures, 0, "harness bug: {stats}");
    assert_eq!(stats.races, 1, "{stats}");
    // The stale push and the race's loser; the forced push, the race's
    // winner and the fast-forward were accepted.
    assert_eq!(stats.plain_url_branch_rejected, 2, "{stats}");
    assert_eq!(stats.plain_url_updates, 5, "{stats}");
}

/// Issue #133: a delete of a ref that does not exist is accepted as a
/// no-op, as stock Git accepts it, so the oracle needs no step-over for it.
/// This is the sequence the oracle shrank the divergence to; on `main`
/// before the fix it stopped at step 0 with an outcome divergence.
#[test]
fn a_delete_of_a_missing_ref_agrees_with_stock_git() {
    let reference = StockServer::start().unwrap();
    let candidate = aspen();
    let delete = [Op::Delete {
        clone: 1,
        branch: aspen_testkit::Branch(3),
    }];
    let run = Oracle::new(&reference, &candidate, config())
        .run(&delete)
        .unwrap();
    assert!(
        run.divergence.is_none(),
        "{:?}\n\naspen log tail:\n{}",
        run.divergence,
        candidate.log_tail(40)
    );
    assert_eq!(run.stats.known, 0);
    assert_eq!(run.stats.refs_accepted, 1, "{}", run.stats);
}
