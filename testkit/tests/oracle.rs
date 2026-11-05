//! The differential oracle checked against itself and against broken servers.
//!
//! These run in required CI, so they stay short (seconds). The nightly
//! workflow runs the same oracle for much longer through the CLI.
//! `ASPEN_ORACLE_SEQUENCES` raises the sequence count of the clean runs.

use aspen_testkit::oracle::seeds;
use aspen_testkit::{
    Branch, Config, DivergenceKind, Fault, FaultyServer, GitClient, GitServer, Op, Oracle,
    ScopedServer, Step, StockServer,
};

fn config() -> Config {
    Config {
        steps: 12,
        clones: 3,
        branches: 4,
        protect_trunk: true,
        plain_url_pushes: false,
    }
}

/// The seeds of the clean runs in required CI.
fn clean_seeds() -> impl Iterator<Item = u64> {
    let sequences = std::env::var("ASPEN_ORACLE_SEQUENCES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(6);
    seeds(0xa5be_0000, sequences)
}

/// Runs the clean seeds and checks that they found nothing and exercised
/// real behaviour (accepted and rejected updates, fetches, races), or "no
/// divergence" proves nothing.
fn assert_clean(reference: &dyn GitServer, candidate: &dyn GitServer) {
    assert_clean_with(reference, candidate, config());
}

fn assert_clean_with(reference: &dyn GitServer, candidate: &dyn GitServer, config: Config) {
    let plain = config.plain_url_pushes;
    let oracle = Oracle::new(reference, candidate, config);
    let seeds: Vec<u64> = clean_seeds().collect();
    let report = oracle.check_seeds(seeds.iter().copied()).unwrap();
    if let Some(divergence) = &report.divergence {
        panic!(
            "false divergence between {} and {}:\n{divergence}",
            reference.name(),
            candidate.name()
        );
    }
    assert_eq!(report.clean_sequences, seeds.len());
    let stats = report.stats;
    assert_eq!(stats.local_failures, 0, "harness bug: {stats}");
    assert!(
        stats.refs_accepted >= seeds.len() * 3,
        "too few accepted updates: {stats}"
    );
    assert!(
        stats.refs_rejected > 0,
        "no rejected update exercised: {stats}"
    );
    assert!(stats.fetches > 0, "no fetch exercised: {stats}");
    assert!(stats.races > 0, "no race exercised: {stats}");
    if plain {
        assert!(
            stats.plain_url_branch_rejected > 0
                && stats.plain_url_updates > stats.plain_url_branch_rejected,
            "plain-URL pushes not exercised both ways: {stats}"
        );
    } else {
        assert_eq!(stats.plain_url_updates, 0, "{stats}");
    }
}

/// Stock `git http-backend` against itself must never diverge: any divergence
/// here is a false positive in the oracle.
#[test]
fn stock_vs_stock_has_no_divergence() {
    let reference = StockServer::start().unwrap();
    let candidate = StockServer::start_named("git-http-backend (candidate)").unwrap();
    assert_clean(&reference, &candidate);
}

/// With pushes and races through the plain repository URL (issue #135),
/// stock against itself still never diverges: a plain-URL push on a stock
/// server is the same push as through the branch URL.
#[test]
fn stock_vs_stock_with_plain_url_pushes_has_no_divergence() {
    let reference = StockServer::start().unwrap();
    let candidate = StockServer::start_named("git-http-backend (candidate)").unwrap();
    let config = Config {
        plain_url_pushes: true,
        ..config()
    };
    assert_clean_with(&reference, &candidate, config);
}

/// A server that hides every branch except trunk behind branch-scoped URLs,
/// as Aspen does, must not diverge from one that shows every branch on one
/// URL: the oracle reaches each branch through its own URL.
#[test]
fn stock_vs_branch_scoped_has_no_divergence() {
    let reference = StockServer::start().unwrap();
    let candidate = ScopedServer::start().unwrap();
    assert_clean(&reference, &candidate);
}

/// The scoped server really hides branches: the plain URL shows only trunk,
/// and a branch's scoped URL shows trunk plus that branch. Without this, the
/// clean run above would prove nothing.
#[test]
fn branch_scoped_server_hides_branches_from_the_plain_url() {
    let server = ScopedServer::start().unwrap();
    let url = server.create_repo("hidden.git").unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let git = GitClient::new(tmp.path());
    let work = tmp.path().join("work");
    let work_str = work.to_str().unwrap();
    git.run(tmp.path(), ["init", "--quiet", "-b", "main", work_str])
        .unwrap();
    git.run(&work, ["commit", "--quiet", "--allow-empty", "-m", "one"])
        .unwrap();
    let topic_url = server.branch_url(&url, "refs/heads/topic");
    git.run(&work, ["push", "--quiet", &url, "main"]).unwrap();
    git.run(
        &work,
        ["push", "--quiet", &topic_url, "main:refs/heads/topic"],
    )
    .unwrap();

    let names = |listing: String| -> Vec<String> {
        listing
            .lines()
            .map(|l| l.split('\t').nth(1).unwrap().to_owned())
            .collect()
    };
    let plain = git.run(tmp.path(), ["ls-remote", "--refs", &url]).unwrap();
    assert_eq!(names(plain), ["refs/heads/main"]);
    let scoped = git
        .run(tmp.path(), ["ls-remote", "--refs", &topic_url])
        .unwrap();
    assert_eq!(names(scoped), ["refs/heads/main", "refs/heads/topic"]);
    let all: Vec<_> = server.list_refs(&url).unwrap().into_keys().collect();
    assert_eq!(all, ["refs/heads/main", "refs/heads/topic"]);
    assert!(
        !git.output(&work, ["push", "--quiet", &url, ":refs/heads/topic"])
            .unwrap()
            .status
            .success(),
        "stock receive-pack should refuse to touch a ref hidden on this URL"
    );
}

/// Races the two servers resolve differently are not divergences: the oracle
/// checks each server's invariants, forces the candidate's winner onto the
/// reference, notes it, and carries on. `SlowRacer` makes the winners
/// deterministic: racer 0 always loses on the reference, racer 1 on the
/// candidate.
#[test]
fn race_winners_may_differ_and_are_resynced() {
    let reference = FaultyServer::start(Fault::SlowRacer {
        racer: 0,
        trunk_fast_forward_only: false,
    })
    .unwrap();
    let candidate = FaultyServer::start(Fault::SlowRacer {
        racer: 1,
        trunk_fast_forward_only: false,
    })
    .unwrap();
    let oracle = Oracle::new(&reference, &candidate, config());
    let ops = [
        Op::Race {
            clones: vec![0, 1],
            branch: Branch(1),
        },
        Op::Race {
            clones: vec![2, 0],
            branch: Branch::TRUNK,
        },
        Op::Push {
            clone: 1,
            branch: Branch(1),
            commits: 1,
        },
        Op::Merge {
            clone: 2,
            into: Branch::TRUNK,
            from: Branch(1),
        },
        Op::Fetch { clone: 0 },
    ];
    let run = oracle.run(&ops).unwrap();
    if let Some((step, kind)) = &run.divergence {
        panic!("diverged after {step}: {kind}");
    }
    assert_eq!(run.stats.races, 2, "{}", run.stats);
    assert_eq!(run.stats.resyncs, 2, "{}", run.stats);
    assert_eq!(run.notes.len(), 2);
    for (step, note) in &run.notes {
        assert!(*step < 2 && note.starts_with("resync:"), "{note}");
    }
    assert_eq!(run.stats.local_failures, 0, "{}", run.stats);
}

/// A race on trunk whose winners differ must not ask the candidate to
/// rewrite trunk: Aspen refuses forced trunk updates by default (S2,
/// `allow_force = branches`). The candidate here refuses every
/// non-fast-forward update of `main`, and `SlowRacer` makes it pick a
/// different winner than the reference. The oracle resyncs by forcing the
/// candidate's winner onto the reference, so the run has no divergence.
#[test]
fn trunk_race_resync_never_rewrites_candidate_trunk() {
    let reference = FaultyServer::start(Fault::SlowRacer {
        racer: 0,
        trunk_fast_forward_only: false,
    })
    .unwrap();
    let candidate = FaultyServer::start(Fault::SlowRacer {
        racer: 1,
        trunk_fast_forward_only: true,
    })
    .unwrap();
    let oracle = Oracle::new(&reference, &candidate, config());
    let ops = [
        Op::Race {
            clones: vec![0, 1],
            branch: Branch::TRUNK,
        },
        Op::Push {
            clone: 2,
            branch: Branch::TRUNK,
            commits: 1,
        },
        Op::Fetch { clone: 0 },
    ];
    let run = oracle.run(&ops).unwrap();
    if let Some((step, kind)) = &run.divergence {
        panic!("diverged after {step}: {kind}");
    }
    assert_eq!(run.stats.races, 1, "{}", run.stats);
    assert_eq!(run.stats.resyncs, 1, "{}", run.stats);
    let [(0, note)] = run.notes.as_slice() else {
        panic!("expected one note at step 0, got {:?}", run.notes);
    };
    assert!(note.starts_with("resync:"), "{note}");
    assert_eq!(run.stats.local_failures, 0, "{}", run.stats);
}

/// A server without compare-and-swap (it acknowledges both racing pushes and
/// keeps the last) is caught by the race invariants, the seed replays to the
/// same step, and shrinking keeps the race.
#[test]
fn last_writer_wins_is_caught_reproducibly() {
    let reference = StockServer::start().unwrap();
    let faulty = FaultyServer::start(Fault::LastWriterWins).unwrap();
    let oracle = Oracle::new(&reference, &faulty, config()).shrink_budget(12);
    let report = oracle.check_seeds(seeds(1, 10)).unwrap();
    let first = report
        .divergence
        .expect("the last-writer-wins server was not caught");
    match &first.kind {
        DivergenceKind::RaceInvariant { server, problems } => {
            assert!(server.starts_with("faulty"), "{first}");
            assert!(
                problems.iter().any(|p| p.contains("expected exactly 1")),
                "{first}"
            );
        }
        other => panic!("expected a race invariant divergence, got {other:?}\n{first}"),
    }
    let Step::Op(i) = first.step else {
        panic!("diverged at setup: {first}")
    };
    assert!(matches!(first.ops[i], Op::Race { .. }), "{first}");

    let again = oracle
        .shrink_budget(0)
        .check_seed(first.seed)
        .unwrap()
        .expect("the divergence did not reproduce from its seed");
    assert_eq!(again.step, first.step);
    assert_eq!(again.ops, first.ops);

    if let Some((shrunk, step, kind)) = &first.shrunk {
        assert!(shrunk.len() < first.ops.len());
        assert_eq!(*step, Step::Op(shrunk.len() - 1));
        assert!(matches!(shrunk.last(), Some(Op::Race { .. })), "{first}");
        assert!(matches!(kind, DivergenceKind::RaceInvariant { .. }));
    }
    println!("{first}");
}

/// A server that silently loses an acknowledged ref update is caught, the
/// report names the seed, and replaying the seed reproduces the same
/// divergence at the same step. Shrinking finds a shorter sequence.
#[test]
fn dropped_ref_update_is_caught_reproducibly() {
    let reference = StockServer::start().unwrap();
    let faulty = FaultyServer::start(Fault::DropRefUpdate { nth_push: 3 }).unwrap();
    let oracle = Oracle::new(&reference, &faulty, config()).shrink_budget(12);
    let report = oracle.check_seeds(seeds(1, 5)).unwrap();
    let first = report.divergence.expect("the faulty server was not caught");
    assert!(
        matches!(first.kind, DivergenceKind::Refs { .. }),
        "expected a ref divergence, got: {first}"
    );
    let text = first.to_string();
    assert!(
        text.contains(&format!("seed: {:#018x}", first.seed)),
        "{text}"
    );
    assert!(
        text.contains("replay: cargo run -p aspen-testkit -- diff --seed"),
        "{text}"
    );

    let again = oracle
        .shrink_budget(0)
        .check_seed(first.seed)
        .unwrap()
        .expect("the divergence did not reproduce from its seed");
    assert_eq!(again.step, first.step);
    assert_eq!(again.ops, first.ops);
    assert_eq!(
        std::mem::discriminant(&again.kind),
        std::mem::discriminant(&first.kind)
    );

    if let Some((shrunk, step, _)) = &first.shrunk {
        assert!(shrunk.len() < first.ops.len());
        assert_eq!(*step, Step::Op(shrunk.len() - 1));
    }
}

fn caught(fault: Fault) -> aspen_testkit::Divergence {
    let reference = StockServer::start().unwrap();
    let faulty = FaultyServer::start(fault).unwrap();
    let oracle = Oracle::new(&reference, &faulty, config()).shrink_budget(0);
    let report = oracle.check_seeds(seeds(1, 5)).unwrap();
    assert!(faulty.fired() > 0, "{fault:?} never fired");
    report
        .divergence
        .unwrap_or_else(|| panic!("{fault:?} was not caught"))
}

/// A server that loses an object of an acknowledged push can no longer serve
/// a fetch of that ref.
#[test]
fn lost_object_is_caught() {
    let d = caught(Fault::LoseObject { nth_push: 3 });
    assert!(
        matches!(&d.kind, DivergenceKind::CheckFailed { server, .. } if server.starts_with("faulty")),
        "{d}"
    );
}

/// A candidate that breaks during setup (here it loses an object of the
/// setup push, so the other clones' first fetch fails) has diverged at
/// setup: a report, not a harness error.
#[test]
fn a_candidate_broken_at_setup_is_a_divergence() {
    let d = caught(Fault::LoseObject { nth_push: 1 });
    assert_eq!(d.step, Step::Setup, "{d}");
    assert!(
        matches!(&d.kind, DivergenceKind::CheckFailed { server, check: "setup", .. } if server.starts_with("faulty")),
        "{d}"
    );
}

/// A server that stores the wrong bytes under an object's name is caught.
#[test]
fn corrupted_object_is_caught() {
    let d = caught(Fault::CorruptObject { nth_push: 3 });
    assert!(
        matches!(&d.kind, DivergenceKind::CheckFailed { server, .. } if server.starts_with("faulty")),
        "{d}"
    );
}

/// A server that rejects a push the reference accepts is caught from the
/// client's point of view, before any state comparison.
#[test]
fn rejected_push_is_caught() {
    let d = caught(Fault::RejectPush { nth_push: 3 });
    match &d.kind {
        DivergenceKind::Outcome {
            reference,
            candidate,
        } => assert_ne!(reference, candidate),
        other => panic!("expected an outcome divergence, got {other:?}"),
    }
}
