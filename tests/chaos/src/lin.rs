//! A linearizability checker for one ref, modelled as a compare-and-swap
//! register (section 4 of `docs/PLAN.md`: "history per ref is
//! linearizable, checked with a compare-and-swap-register linearizability
//! checker").
//!
//! The model: the register holds the ref's tip (or nothing). A push of
//! commit `to` built on `from` is `cas(from, to)`; a read returns the
//! value. The checker searches for a total order of the operations that
//! (1) respects real time (an operation that completed before another
//! started comes first), (2) contains every operation that completed, and
//! (3) replays correctly on the model: every successful push found the
//! register at `from`, every rejected push found it elsewhere, and every
//! read saw the value the order gives. An operation with an unknown
//! outcome may appear anywhere after it started, or not at all.
//!
//! The search is the Wing and Gong algorithm with Lowe's memoisation of
//! visited (set of linearized operations, register value) pairs. Commits
//! are unique, so each value names one push and the search stays narrow.

use std::collections::{HashMap, HashSet};

use crate::history::{Call, Op, Outcome, short};

/// The checker's answer for one ref.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    /// A valid order exists.
    Linearizable,
    /// No valid order exists; the explanation says how far the best order
    /// got and why nothing could come next.
    Violation(String),
    /// The search visited more states than its budget allows.
    Inconclusive { visited: usize },
}

#[derive(Debug, Clone, Copy)]
enum Kind {
    Cas {
        from: u32,
        to: u32,
        outcome: Outcome,
    },
    Read {
        value: u32,
    },
}

#[derive(Debug, Clone, Copy)]
struct Entry {
    invoked: u64,
    /// `u64::MAX` for an operation with an unknown outcome.
    completed: u64,
    kind: Kind,
    required: bool,
    /// Index into the caller's slice.
    op: usize,
}

/// The default search budget, in visited states.
pub const DEFAULT_BUDGET: usize = 2_000_000;

/// Checks the operations on one ref, which held `initial` before any of
/// them started.
pub fn check(initial: Option<&str>, ops: &[Op]) -> Verdict {
    check_with_budget(initial, ops, DEFAULT_BUDGET)
}

/// [`check`] with an explicit budget.
pub fn check_with_budget(initial: Option<&str>, ops: &[Op], budget: usize) -> Verdict {
    let mut values: HashMap<String, u32> = HashMap::new();
    let mut names: Vec<Option<String>> = vec![None];
    let mut intern = |v: Option<&str>| -> u32 {
        match v {
            None => 0,
            Some(s) => *values.entry(s.to_owned()).or_insert_with(|| {
                names.push(Some(s.to_owned()));
                (names.len() - 1) as u32
            }),
        }
    };
    let start = intern(initial);
    let mut entries: Vec<Entry> = ops
        .iter()
        .enumerate()
        .filter_map(|(i, op)| {
            let kind = match &op.call {
                // A push refused without comparing against the ref, and a
                // read with no answer, say nothing about the register.
                Call::Push { .. } if op.outcome == Outcome::NoEffect => return None,
                Call::Push { from, to } => Kind::Cas {
                    from: intern(from.as_deref()),
                    to: intern(Some(to)),
                    outcome: op.outcome,
                },
                Call::Read { value } if op.outcome == Outcome::Ok => Kind::Read {
                    value: intern(value.as_deref()),
                },
                Call::Read { .. } => return None,
            };
            Some(Entry {
                invoked: op.invoked,
                completed: match (op.outcome, op.completed) {
                    (Outcome::Unknown, _) | (_, None) => u64::MAX,
                    (_, Some(t)) => t,
                },
                kind,
                required: op.outcome != Outcome::Unknown && op.completed.is_some(),
                op: i,
            })
        })
        .collect();
    entries.sort_by_key(|e| (e.invoked, e.op));
    let n = entries.len();
    let required = entries.iter().filter(|e| e.required).count();
    let words = n.div_ceil(64).max(1);

    struct Frame {
        done: Vec<u64>,
        state: u32,
        linearized_required: usize,
        candidates: Vec<usize>,
        next: usize,
    }
    let candidates = |done: &[u64]| -> Vec<usize> {
        let is_done = |i: usize| done[i / 64] & (1 << (i % 64)) != 0;
        let min_ret = (0..n)
            .filter(|&i| !is_done(i) && entries[i].required)
            .map(|i| entries[i].completed)
            .min()
            .unwrap_or(u64::MAX);
        (0..n)
            .filter(|&i| !is_done(i) && entries[i].invoked <= min_ret)
            .collect()
    };
    let step = |state: u32, e: &Entry| -> Option<u32> {
        match e.kind {
            Kind::Cas {
                from,
                to,
                outcome: Outcome::Ok | Outcome::Unknown,
            } => (state == from).then_some(to),
            Kind::Cas {
                from,
                outcome: Outcome::Fail,
                ..
            } => (state != from).then_some(state),
            Kind::Cas {
                outcome: Outcome::NoEffect,
                ..
            } => Some(state),
            Kind::Read { value } => (state == value).then_some(state),
        }
    };

    let mut visited: HashSet<(Vec<u64>, u32)> = HashSet::new();
    let root = vec![0u64; words];
    let mut stack = vec![Frame {
        candidates: candidates(&root),
        done: root,
        state: start,
        linearized_required: 0,
        next: 0,
    }];
    // The deepest point the search reached, for the explanation.
    let mut best: (usize, Vec<u64>, u32) = (0, vec![0u64; words], start);
    while let Some(top) = stack.last_mut() {
        if top.linearized_required == required {
            return Verdict::Linearizable;
        }
        if top.next >= top.candidates.len() {
            stack.pop();
            continue;
        }
        let i = top.candidates[top.next];
        top.next += 1;
        let Some(state) = step(top.state, &entries[i]) else {
            continue;
        };
        let mut done = top.done.clone();
        done[i / 64] |= 1 << (i % 64);
        let linearized_required = top.linearized_required + usize::from(entries[i].required);
        if !visited.insert((done.clone(), state)) {
            continue;
        }
        if visited.len() > budget {
            return Verdict::Inconclusive {
                visited: visited.len(),
            };
        }
        if linearized_required > best.0 {
            best = (linearized_required, done.clone(), state);
        }
        stack.push(Frame {
            candidates: candidates(&done),
            done,
            state,
            linearized_required,
            next: 0,
        });
    }

    // No order exists. Explain from the deepest point reached.
    let (got, done, state) = best;
    let name = |v: u32| short(names[v as usize].as_deref());
    let mut text = format!(
        "no valid order: the best one places {got} of {required} completed operations, \
         leaving the ref at {}, and then none of these can come next:\n",
        name(state)
    );
    for i in candidates(&done) {
        let e = &entries[i];
        if !e.required {
            continue;
        }
        let why = match e.kind {
            Kind::Cas {
                from,
                outcome: Outcome::Ok,
                ..
            } => format!("acknowledged, but needs the ref at {}", name(from)),
            Kind::Cas {
                from,
                outcome: Outcome::Fail,
                ..
            } => format!("rejected, but the ref is at {} (its base)", name(from)),
            Kind::Read { value } => {
                format!("read {}, but the ref is at {}", name(value), name(state))
            }
            Kind::Cas { .. } => continue,
        };
        text.push_str(&format!("    {}: {why}\n", ops[e.op]));
    }
    Verdict::Violation(text)
}

#[cfg(test)]
mod tests {
    use super::*;
    use aspen_testkit::rng::Rng;

    fn op(id: u64, call: Call, outcome: Outcome, invoked: u64, completed: Option<u64>) -> Op {
        Op {
            id,
            client: 0,
            key: "refs/heads/main".into(),
            call,
            outcome,
            invoked,
            completed: if outcome == Outcome::Unknown {
                None
            } else {
                completed
            },
            note: String::new(),
        }
    }

    fn push(from: Option<&str>, to: &str) -> Call {
        Call::Push {
            from: from.map(str::to_owned),
            to: to.to_owned(),
        }
    }

    fn read(v: Option<&str>) -> Call {
        Call::Read {
            value: v.map(str::to_owned),
        }
    }

    fn is_violation(v: &Verdict) -> bool {
        matches!(v, Verdict::Violation(_))
    }

    #[test]
    fn a_sequential_chain_is_linearizable() {
        let ops = [
            op(0, push(Some("a"), "b"), Outcome::Ok, 0, Some(10)),
            op(1, push(Some("b"), "c"), Outcome::Ok, 20, Some(30)),
            op(2, push(Some("b"), "x"), Outcome::Fail, 40, Some(50)),
            op(3, read(Some("c")), Outcome::Ok, 60, Some(70)),
        ];
        assert_eq!(check(Some("a"), &ops), Verdict::Linearizable);
    }

    #[test]
    fn two_acknowledged_pushes_from_one_base_are_a_lost_update() {
        // Both racers were told their push landed: one of them was lost.
        let ops = [
            op(0, push(Some("a"), "b"), Outcome::Ok, 0, Some(10)),
            op(1, push(Some("a"), "c"), Outcome::Ok, 1, Some(11)),
        ];
        assert!(is_violation(&check(Some("a"), &ops)));
    }

    #[test]
    fn a_stale_read_after_an_acknowledged_push_is_caught() {
        let ops = [
            op(0, push(Some("a"), "b"), Outcome::Ok, 0, Some(10)),
            op(1, read(Some("a")), Outcome::Ok, 20, Some(30)),
        ];
        let v = check(Some("a"), &ops);
        let Verdict::Violation(text) = v else {
            panic!("{v:?}")
        };
        assert!(text.contains("read a"), "{text}");
    }

    #[test]
    fn a_read_concurrent_with_a_push_may_see_either_value() {
        for seen in ["a", "b"] {
            let ops = [
                op(0, push(Some("a"), "b"), Outcome::Ok, 0, Some(10)),
                op(1, read(Some(seen)), Outcome::Ok, 5, Some(15)),
            ];
            assert_eq!(check(Some("a"), &ops), Verdict::Linearizable, "{seen}");
        }
    }

    #[test]
    fn a_rejection_with_nothing_to_explain_it_is_caught() {
        let ops = [op(0, push(Some("a"), "b"), Outcome::Fail, 0, Some(10))];
        assert!(is_violation(&check(Some("a"), &ops)));
        // With a concurrent push that moved the ref, it is fine.
        let ops = [
            op(0, push(Some("a"), "b"), Outcome::Fail, 0, Some(10)),
            op(1, push(Some("a"), "c"), Outcome::Ok, 1, Some(5)),
        ];
        assert_eq!(check(Some("a"), &ops), Verdict::Linearizable);
    }

    #[test]
    fn a_push_refused_without_comparing_says_nothing_about_the_ref() {
        // "cannot decide ancestry" or a lock failure: the ref may well
        // have been at the base.
        let ops = [
            op(0, push(Some("a"), "b"), Outcome::NoEffect, 0, Some(10)),
            op(1, read(Some("a")), Outcome::Ok, 20, Some(30)),
        ];
        assert_eq!(check(Some("a"), &ops), Verdict::Linearizable);
        // Classified as a stale rejection, the same history is a violation.
        let ops = [
            op(0, push(Some("a"), "b"), Outcome::Fail, 0, Some(10)),
            op(1, read(Some("a")), Outcome::Ok, 20, Some(30)),
        ];
        assert!(is_violation(&check(Some("a"), &ops)));
    }

    #[test]
    fn an_unknown_push_may_land_late_or_never() {
        // It landed: a read much later sees it.
        let ops = [
            op(0, push(Some("a"), "b"), Outcome::Unknown, 0, None),
            op(1, read(Some("b")), Outcome::Ok, 1_000, Some(1_010)),
        ];
        assert_eq!(check(Some("a"), &ops), Verdict::Linearizable);
        // It never landed.
        let ops = [
            op(0, push(Some("a"), "b"), Outcome::Unknown, 0, None),
            op(1, read(Some("a")), Outcome::Ok, 1_000, Some(1_010)),
        ];
        assert_eq!(check(Some("a"), &ops), Verdict::Linearizable);
        // It cannot land before it started.
        let ops = [
            op(0, read(Some("b")), Outcome::Ok, 0, Some(10)),
            op(1, push(Some("a"), "b"), Outcome::Unknown, 20, None),
        ];
        assert!(is_violation(&check(Some("a"), &ops)));
    }

    #[test]
    fn a_value_nobody_wrote_is_caught() {
        let ops = [op(0, read(Some("z")), Outcome::Ok, 0, Some(10))];
        assert!(is_violation(&check(Some("a"), &ops)));
    }

    #[test]
    fn a_lost_acknowledged_push_is_caught() {
        // Acknowledged, then gone: the ref went back to its old value.
        let ops = [
            op(0, push(Some("a"), "b"), Outcome::Ok, 0, Some(10)),
            op(1, push(Some("b"), "c"), Outcome::Ok, 20, Some(30)),
            op(2, read(Some("b")), Outcome::Ok, 40, Some(50)),
        ];
        assert!(is_violation(&check(Some("a"), &ops)));
    }

    /// Simulates a correct register under concurrency (each operation takes
    /// effect at a random instant inside its interval) and checks the
    /// history passes; then breaks one read and checks the result fails
    /// whenever the broken value cannot be explained.
    #[test]
    fn random_histories_of_a_correct_register_pass() {
        for seed in 0..200u64 {
            let mut rng = Rng::new(seed);
            let (ops, final_value) = simulate(&mut rng, 4, 12);
            let v = check(Some("v0"), &ops);
            assert_eq!(v, Verdict::Linearizable, "seed {seed}:\n{ops:#?}");
            // A read issued after everything ended must see the final value;
            // one that claims an older value is caught.
            let end = ops.iter().filter_map(|o| o.completed).max().unwrap_or(0) + 1;
            let mut bad = ops.clone();
            bad.push(op(999, read(Some("v0")), Outcome::Ok, end, Some(end + 1)));
            if final_value != "v0" {
                assert!(is_violation(&check(Some("v0"), &bad)), "seed {seed}");
            }
        }
    }

    /// `clients` clients each run `per_client` operations against a real
    /// register; returns the history and the final value.
    fn simulate(rng: &mut Rng, clients: usize, per_client: usize) -> (Vec<Op>, String) {
        // Events: (time the operation takes effect, client, invoked, completed).
        let mut plans = Vec::new();
        for c in 0..clients {
            let mut t = rng.below(5) as u64;
            for _ in 0..per_client {
                let invoked = t;
                let len = 1 + rng.below(20) as u64;
                let effect = invoked + rng.below(len as usize) as u64;
                let completed = invoked + len;
                plans.push((effect, c, invoked, completed, rng.below(3)));
                t = completed + rng.below(5) as u64;
            }
        }
        plans.sort();
        let mut value = "v0".to_owned();
        let mut next = 1;
        let mut ops = Vec::new();
        // Each client builds on the value it last read (or v0).
        let mut seen = vec!["v0".to_owned(); clients];
        for (id, (_effect, c, invoked, completed, kind)) in plans.into_iter().enumerate() {
            let (call, outcome) = match kind {
                0 => {
                    let r = value.clone();
                    seen[c] = r.clone();
                    (read(Some(&r)), Outcome::Ok)
                }
                _ => {
                    let to = format!("v{next}");
                    next += 1;
                    let from = seen[c].clone();
                    if value == from {
                        value = to.clone();
                        seen[c] = to.clone();
                        let outcome = if kind == 2 && rng.below(4) == 0 {
                            Outcome::Unknown
                        } else {
                            Outcome::Ok
                        };
                        (push(Some(&from), &to), outcome)
                    } else {
                        (push(Some(&from), &to), Outcome::Fail)
                    }
                }
            };
            ops.push(op(id as u64, call, outcome, invoked, Some(completed)));
        }
        (ops, value)
    }
}
