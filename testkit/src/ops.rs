//! Random operation sequences for the differential oracle.
//!
//! An [`Op`] is something a group of client clones does to one repository:
//! push, force-push, delete, create a branch, merge, fetch, push from
//! several clones at once, or race several clones to update one branch;
//! with [`Config::plain_url_pushes`], also push and race through the plain
//! repository URL. Ops refer to clones and branches by index, so the same
//! sequence can be replayed against any server, and [`generate`] derives a
//! sequence from a seed alone.

use std::fmt;

use crate::rng::Rng;

/// A branch, by index: 0 is the trunk (`main`), the rest are `topic-<n>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Branch(pub u8);

impl Branch {
    /// The trunk branch, `main`.
    pub const TRUNK: Branch = Branch(0);

    /// The short branch name, such as `main` or `topic-2`.
    pub fn name(self) -> String {
        if self == Self::TRUNK {
            "main".to_owned()
        } else {
            format!("topic-{}", self.0)
        }
    }
}

impl fmt::Display for Branch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.name())
    }
}

/// One step of a sequence. `clone` is an index into the sequence's client
/// clones. Clones see the server only as of their last fetch (or clone), so
/// pushes from stale clones are rejected as non-fast-forward, just as they
/// would be for a real user.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Op {
    /// Start from the clone's view of `branch` (or of `main`, or from scratch
    /// if neither exists), add `commits` commits, and push.
    Push {
        /// Which clone acts.
        clone: usize,
        /// The branch to push.
        branch: Branch,
        /// How many new commits, at least 1.
        commits: u8,
    },
    /// Replace the tip of `branch` with a new commit on top of its parent and
    /// push with `--force -o force` (the push option is Aspen's force intent,
    /// spike S2; stock servers ignore it).
    ForcePush {
        /// Which clone acts.
        clone: usize,
        /// The branch to rewrite.
        branch: Branch,
    },
    /// Delete `branch` through its branch URL (so the client sends its real
    /// old object ID: a compare-and-swap delete).
    Delete {
        /// Which clone acts.
        clone: usize,
        /// The branch to delete.
        branch: Branch,
    },
    /// Push the clone's view of `from` to a new (or existing) branch `new`,
    /// without creating any objects.
    CreateBranch {
        /// Which clone acts.
        clone: usize,
        /// The branch to create or update.
        new: Branch,
        /// The branch whose tip it gets.
        from: Branch,
    },
    /// Merge the clone's view of `from` into `into` with a merge commit, and
    /// push `into`.
    Merge {
        /// Which clone acts.
        clone: usize,
        /// The branch that receives the merge commit.
        into: Branch,
        /// The branch merged in.
        from: Branch,
    },
    /// Refresh the clone's view of every branch, one branch URL at a time,
    /// dropping branches that no longer exist (like `git fetch --prune`).
    Fetch {
        /// Which clone acts.
        clone: usize,
    },
    /// Several clones each add a commit to a different branch and push at the
    /// same time. Clones and branches are distinct, so the outcome does not
    /// depend on which push the server handles first.
    ConcurrentPushes {
        /// `(clone, branch)` pairs; no clone or branch appears twice.
        pushes: Vec<(usize, Branch)>,
    },
    /// Several clones refresh their view of `branch`, each add a different
    /// commit on top of it, and push at the same time: a compare-and-swap
    /// race. Which push wins may differ between servers, so the oracle checks
    /// invariants instead (exactly one accepted, the tip is the winner's
    /// commit, every loser rejected) and then makes the servers agree again.
    Race {
        /// The racing clones, distinct, at least 2.
        clones: Vec<usize>,
        /// The contested branch.
        branch: Branch,
    },
    /// Like [`Op::Push`] (or, with `force`, [`Op::ForcePush`]), but pushed
    /// through the plain repository URL instead of the branch's URL.
    ///
    /// On a server that shows every ref on that URL (stock) it is the same
    /// push. On Aspen the repository URL advertises only trunk (spike S2), so
    /// for any other branch the client sends a zero old value and cannot
    /// refuse a non-fast-forward itself: Aspen's own fast-forward check, and
    /// its `-o force` rule, decide. A stale push that stock's client refuses
    /// (`fetch first`, `non-fast-forward`) must be refused by Aspen's `ng`.
    /// Only generated with [`Config::plain_url_pushes`].
    PlainPush {
        /// Which clone acts.
        clone: usize,
        /// The branch to push.
        branch: Branch,
        /// How many new commits, at least 1.
        commits: u8,
        /// Replace the tip instead of extending it, and push with
        /// `--force -o force`.
        force: bool,
    },
    /// Like [`Op::Race`], but every racer pushes through the plain
    /// repository URL, so on Aspen each sends a zero old value and the
    /// losers are refused by Aspen's own fast-forward check after a lost
    /// compare-and-swap. Only generated with [`Config::plain_url_pushes`].
    PlainRace {
        /// The racing clones, distinct, at least 2.
        clones: Vec<usize>,
        /// The contested branch.
        branch: Branch,
    },
}

/// Writes ` 1, 2, 3` for `clones`.
fn write_clones(f: &mut fmt::Formatter<'_>, clones: &[usize]) -> fmt::Result {
    for (i, clone) in clones.iter().enumerate() {
        let sep = if i == 0 { " " } else { ", " };
        write!(f, "{sep}{clone}")?;
    }
    Ok(())
}

impl fmt::Display for Op {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Op::Push {
                clone,
                branch,
                commits,
            } => write!(f, "clone {clone}: push {commits} commit(s) to {branch}"),
            Op::ForcePush { clone, branch } => write!(f, "clone {clone}: force-push {branch}"),
            Op::Delete { clone, branch } => write!(f, "clone {clone}: delete {branch}"),
            Op::CreateBranch { clone, new, from } => {
                write!(f, "clone {clone}: create {new} from {from}")
            }
            Op::Merge { clone, into, from } => {
                write!(f, "clone {clone}: merge {from} into {into} and push")
            }
            Op::Fetch { clone } => write!(f, "clone {clone}: fetch --prune"),
            Op::Race { clones, branch } => {
                write!(f, "race on {branch}: clones")?;
                write_clones(f, clones)?;
                f.write_str(" push at once")
            }
            Op::PlainPush {
                clone,
                branch,
                commits,
                force: false,
            } => write!(
                f,
                "clone {clone}: push {commits} commit(s) to {branch} through the repository URL"
            ),
            Op::PlainPush {
                clone,
                branch,
                force: true,
                ..
            } => write!(
                f,
                "clone {clone}: force-push {branch} through the repository URL"
            ),
            Op::PlainRace { clones, branch } => {
                write!(f, "race on {branch} through the repository URL: clones")?;
                write_clones(f, clones)?;
                f.write_str(" push at once")
            }
            Op::ConcurrentPushes { pushes } => {
                f.write_str("concurrently:")?;
                for (i, (clone, branch)) in pushes.iter().enumerate() {
                    let sep = if i == 0 { " " } else { ", " };
                    write!(f, "{sep}clone {clone} pushes {branch}")?;
                }
                Ok(())
            }
        }
    }
}

/// The shape of generated sequences.
#[derive(Debug, Clone)]
pub struct Config {
    /// Ops per sequence.
    pub steps: usize,
    /// Client clones per sequence, at least 1.
    pub clones: usize,
    /// Branches in play, including `main`; at least 1 and at most 255.
    pub branches: usize,
    /// Never delete or force-push `main`. Trunk deletion and rewriting are a
    /// policy question for the server (Aspen refuses both by default, spike
    /// S2), not a correctness one, so they are off by default.
    pub protect_trunk: bool,
    /// Also generate [`Op::PlainPush`] and [`Op::PlainRace`]: pushes
    /// through the plain repository URL, where Aspen sees a zero old value
    /// for every branch but trunk and decides fast-forwards itself (issue
    /// #135). Off by default, so sequences of earlier seeds replay
    /// unchanged. The candidate's repository URL must accept pushes of
    /// every branch: Aspen and stock do, [`crate::ScopedServer`] does not
    /// (stock `receive-pack` refuses updates of hidden refs).
    pub plain_url_pushes: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            steps: 20,
            clones: 3,
            branches: 4,
            protect_trunk: true,
            plain_url_pushes: false,
        }
    }
}

impl Config {
    fn validate(&self) {
        assert!(self.clones >= 1, "Config::clones must be at least 1");
        assert!(
            (1..=255).contains(&self.branches),
            "Config::branches must be in 1..=255"
        );
    }
}

// Relative weights of each op kind, in `OpKind` order. The plain-URL kinds
// come last and weigh 0 unless `Config::plain_url_pushes` is on, so that
// with it off `Rng::weighted` maps every draw exactly as before they
// existed, and earlier seeds replay unchanged.
const WEIGHTS: [u32; 10] = [30, 8, 8, 10, 10, 14, 8, 12, 0, 0];

// The plain-URL kinds' weights with `Config::plain_url_pushes` on.
const PLAIN_PUSH_WEIGHT: u32 = 24;
const PLAIN_RACE_WEIGHT: u32 = 8;

#[derive(Clone, Copy)]
enum OpKind {
    Push,
    ForcePush,
    Delete,
    CreateBranch,
    Merge,
    Fetch,
    Concurrent,
    Race,
    PlainPush,
    PlainRace,
}

const KINDS: [OpKind; 10] = [
    OpKind::Push,
    OpKind::ForcePush,
    OpKind::Delete,
    OpKind::CreateBranch,
    OpKind::Merge,
    OpKind::Fetch,
    OpKind::Concurrent,
    OpKind::Race,
    OpKind::PlainPush,
    OpKind::PlainRace,
];

/// The sequence for `seed`. The same seed and config always give the same
/// ops, on every platform.
pub fn generate(seed: u64, config: &Config) -> Vec<Op> {
    config.validate();
    let mut rng = Rng::new(seed);
    let mut weights = WEIGHTS;
    if config.plain_url_pushes {
        weights[OpKind::PlainPush as usize] = PLAIN_PUSH_WEIGHT;
        weights[OpKind::PlainRace as usize] = PLAIN_RACE_WEIGHT;
    }
    let deletable = if config.protect_trunk {
        config.branches - 1
    } else {
        config.branches
    };
    if deletable == 0 {
        weights[OpKind::Delete as usize] = 0;
        weights[OpKind::ForcePush as usize] = 0;
    }
    if config.clones < 2 || config.branches < 2 {
        weights[OpKind::Concurrent as usize] = 0;
    }
    if config.clones < 2 {
        weights[OpKind::Race as usize] = 0;
        weights[OpKind::PlainRace as usize] = 0;
    }
    let first_deletable = config.branches - deletable;
    (0..config.steps)
        .map(|_| {
            let clone = rng.below(config.clones);
            let branch = |rng: &mut Rng| Branch(rng.below(config.branches) as u8);
            match KINDS[rng.weighted(&weights)] {
                OpKind::Push => Op::Push {
                    clone,
                    branch: branch(&mut rng),
                    commits: 1 + rng.below(3) as u8,
                },
                OpKind::ForcePush => Op::ForcePush {
                    clone,
                    branch: Branch((first_deletable + rng.below(deletable)) as u8),
                },
                OpKind::Delete => Op::Delete {
                    clone,
                    branch: Branch((first_deletable + rng.below(deletable)) as u8),
                },
                OpKind::CreateBranch => Op::CreateBranch {
                    clone,
                    new: branch(&mut rng),
                    from: branch(&mut rng),
                },
                OpKind::Merge => Op::Merge {
                    clone,
                    into: branch(&mut rng),
                    from: branch(&mut rng),
                },
                OpKind::Fetch => Op::Fetch { clone },
                OpKind::Concurrent => {
                    let max = config.clones.min(config.branches);
                    let n = 2 + rng.below(max - 1);
                    let clones = pick_distinct(&mut rng, config.clones, n);
                    let branches = pick_distinct(&mut rng, config.branches, n);
                    Op::ConcurrentPushes {
                        pushes: clones
                            .into_iter()
                            .zip(branches)
                            .map(|(c, b)| (c, Branch(b as u8)))
                            .collect(),
                    }
                }
                OpKind::Race => {
                    let n = 2 + rng.below(config.clones.min(3) - 1);
                    Op::Race {
                        clones: pick_distinct(&mut rng, config.clones, n),
                        branch: branch(&mut rng),
                    }
                }
                OpKind::PlainPush => {
                    // A forced push only where `ForcePush` may go: never
                    // trunk while it is protected.
                    let force = deletable > 0 && rng.below(4) == 0;
                    let branch = if force {
                        Branch((first_deletable + rng.below(deletable)) as u8)
                    } else {
                        branch(&mut rng)
                    };
                    Op::PlainPush {
                        clone,
                        branch,
                        commits: 1 + rng.below(3) as u8,
                        force,
                    }
                }
                OpKind::PlainRace => {
                    let n = 2 + rng.below(config.clones.min(3) - 1);
                    Op::PlainRace {
                        clones: pick_distinct(&mut rng, config.clones, n),
                        branch: branch(&mut rng),
                    }
                }
            }
        })
        .collect()
}

/// `n` distinct values from `0..len`, in random order.
fn pick_distinct(rng: &mut Rng, len: usize, n: usize) -> Vec<usize> {
    let mut all: Vec<usize> = (0..len).collect();
    for i in 0..n {
        let j = i + rng.below(len - i);
        all.swap(i, j);
    }
    all.truncate(n);
    all
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_seed_same_sequence() {
        let config = Config::default();
        assert_eq!(generate(42, &config), generate(42, &config));
        assert_ne!(generate(42, &config), generate(43, &config));
    }

    #[test]
    fn sequences_respect_the_config() {
        let config = Config {
            steps: 500,
            clones: 3,
            branches: 4,
            protect_trunk: true,
            plain_url_pushes: true,
        };
        for op in generate(1, &config) {
            match op {
                Op::Delete { branch, .. }
                | Op::ForcePush { branch, .. }
                | Op::PlainPush {
                    branch,
                    force: true,
                    ..
                } => {
                    assert_ne!(branch, Branch::TRUNK)
                }
                Op::Race { clones, branch } | Op::PlainRace { clones, branch } => {
                    assert!((2..=3).contains(&clones.len()));
                    let mut distinct = clones.clone();
                    distinct.sort();
                    distinct.dedup();
                    assert_eq!(distinct.len(), clones.len(), "racers repeat");
                    assert!(clones.iter().all(|&c| c < 3) && branch.0 < 4);
                }
                Op::ConcurrentPushes { pushes } => {
                    assert!(pushes.len() >= 2);
                    let mut clones: Vec<_> = pushes.iter().map(|p| p.0).collect();
                    let mut branches: Vec<_> = pushes.iter().map(|p| p.1).collect();
                    clones.sort();
                    clones.dedup();
                    branches.sort();
                    branches.dedup();
                    assert_eq!(clones.len(), pushes.len(), "clones repeat");
                    assert_eq!(branches.len(), pushes.len(), "branches repeat");
                    assert!(clones.iter().all(|&c| c < 3));
                    assert!(branches.iter().all(|b| b.0 < 4));
                }
                Op::Push {
                    clone,
                    branch,
                    commits,
                }
                | Op::PlainPush {
                    clone,
                    branch,
                    commits,
                    ..
                } => {
                    assert!(clone < 3 && branch.0 < 4 && (1..=3).contains(&commits));
                }
                _ => {}
            }
        }
    }

    #[test]
    fn every_kind_shows_up() {
        let ops = generate(
            3,
            &Config {
                steps: 300,
                plain_url_pushes: true,
                ..Config::default()
            },
        );
        let has = |f: fn(&Op) -> bool| ops.iter().any(f);
        assert!(has(|o| matches!(o, Op::Push { .. })));
        assert!(has(|o| matches!(o, Op::ForcePush { .. })));
        assert!(has(|o| matches!(o, Op::Delete { .. })));
        assert!(has(|o| matches!(o, Op::CreateBranch { .. })));
        assert!(has(|o| matches!(o, Op::Merge { .. })));
        assert!(has(|o| matches!(o, Op::Fetch { .. })));
        assert!(has(|o| matches!(o, Op::ConcurrentPushes { .. })));
        assert!(has(|o| matches!(o, Op::Race { .. })));
        assert!(has(|o| matches!(o, Op::PlainPush { force: false, .. })));
        assert!(has(|o| matches!(o, Op::PlainPush { force: true, .. })));
        assert!(has(|o| matches!(o, Op::PlainRace { .. })));
    }

    /// With `plain_url_pushes` off, sequences are exactly those generated
    /// before the plain-URL ops existed (issue #135), so every printed seed
    /// keeps replaying. Pinned against the ops of seed 0xa5be0000, the
    /// first seed of the required Aspen slice, as they were on main before.
    #[test]
    fn plain_url_pushes_off_keeps_earlier_sequences() {
        let config = Config {
            steps: 12,
            ..Config::default()
        };
        let ops: Vec<String> = generate(0xa5be_0000, &config)
            .iter()
            .map(ToString::to_string)
            .collect();
        assert_eq!(ops, PINNED_A5BE0000);
        for seed in 0..200 {
            let ops = generate(seed, &Config::default());
            assert!(
                !ops.iter()
                    .any(|o| matches!(o, Op::PlainPush { .. } | Op::PlainRace { .. })),
                "seed {seed}"
            );
        }
    }

    const PINNED_A5BE0000: [&str; 12] = [
        "clone 1: push 2 commit(s) to topic-1",
        "clone 2: push 2 commit(s) to topic-1",
        "clone 0: create topic-2 from topic-3",
        "clone 1: delete topic-3",
        "clone 1: push 1 commit(s) to topic-1",
        "clone 0: create topic-2 from topic-2",
        "clone 1: fetch --prune",
        "clone 0: push 1 commit(s) to topic-3",
        "clone 1: fetch --prune",
        "clone 1: delete topic-1",
        "clone 0: force-push topic-3",
        "clone 1: delete topic-1",
    ];

    #[test]
    fn single_clone_single_branch_still_generates() {
        let config = Config {
            steps: 100,
            clones: 1,
            branches: 1,
            protect_trunk: true,
            plain_url_pushes: true,
        };
        let ops = generate(9, &config);
        assert_eq!(ops.len(), 100);
        assert!(!ops.iter().any(|o| matches!(
            o,
            Op::Delete { .. }
                | Op::ForcePush { .. }
                | Op::ConcurrentPushes { .. }
                | Op::Race { .. }
                | Op::PlainRace { .. }
                | Op::PlainPush { force: true, .. }
        )));
    }
}
