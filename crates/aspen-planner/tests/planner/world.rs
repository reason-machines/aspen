//! Random ref-version histories over a real git repository.
//!
//! A seed drives three phases:
//!
//! 1. **Script.** Commits, annotated tags and pushes are generated in
//!    memory, following Aspen's push rules (`docs/PLAN.md`, push path): a
//!    push through the plain URL depends on trunk only, a push through a
//!    branch-scoped URL also on the branch's old version, a deletion on
//!    nothing. The mix includes the histories that make a cover unsafe to
//!    subtract for a have: forced branch updates (which later get covered),
//!    forced trunk updates, compare-and-swap recomputes whose trunk
//!    dependency skips a version, branches created at an old trunk commit,
//!    merges, unreachable blobs in a pack, and thin-pack completion bases.
//! 2. **Repository.** One `git fast-import` writes every object.
//! 3. **Graph.** Each version's pack holds what a client sends:
//!    `git rev-list --objects <tip> --not <dependency tips>`, plus any
//!    unreachable blob and completion bases. Its closure kind comes from
//!    [`Closure::of_new_version`], the function the push path uses. Covers
//!    are laid over random ranges of every ref, and a compacted cover's
//!    pack holds exactly its range's objects. A cover gets a chain only
//!    when [`VerifiedRange::verify`](aspen_core::meta::VerifiedRange::verify)
//!    accepts its range, and not always then ([`Chains`]).
//!
//! Queries then resolve haves only through [`HaveVersion`]'s constructors,
//! with truthful ancestry answers, and compare plans with
//! `git rev-list --objects <wants> --not <haves>`.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::Stdio;

use aspen_core::fetch::{HaveVersion, PlanInput, PlanMode};
use aspen_core::id::{ObjectId, RefVersionId, Seq};
use aspen_core::meta::SeqRange;
use aspen_core::refs::{Ancestry, AncestryAnswer, Closure, RefValue};
use aspen_testkit::isolated_git;
use aspen_testkit::rng::Rng;

use crate::graph::{Chain, Graph, deps, live, name};

const TRUNK: &str = "refs/heads/main";
const PATHS: [&str; 6] = ["a", "b", "d/c", "d/e", "d/f/g", "h"];
/// Few distinct file contents, so blobs recur across commits (a file
/// reverted to an old version, two branches with the same file).
const CONTENTS: u32 = 10;

/// An object of the script.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Obj {
    Commit(usize),
    Tag(usize),
}

#[derive(Debug, Clone)]
struct CommitSpec {
    parents: Vec<usize>,
    files: BTreeMap<&'static str, u32>,
    /// Pushed to the server; `false` for commits only a client has.
    server: bool,
}

/// One push write, or a deletion when `tip` is `None`.
#[derive(Debug, Clone)]
struct PushSpec {
    ref_name: String,
    tip: Option<Obj>,
    /// Versions (indexes into the push list) the pack was built against.
    trunk_dep: Option<usize>,
    scoped_old: Option<usize>,
    /// An unreachable blob the client sent too.
    junk: Option<usize>,
    /// How many completion bases (objects of the dependencies' closures)
    /// the stored pack repeats.
    bases: usize,
    kind: &'static str,
}

/// What the generator produced, so a test can check it exercised every
/// scenario it claims to.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct Coverage {
    pub versions: usize,
    pub forced_under_compacted_cover: usize,
    pub trunk_dep_skips: usize,
    pub forced_trunk: usize,
    pub exact_forced_trunk: usize,
    pub wider_versions: usize,
    pub scoped_pushes: usize,
    pub deletions: usize,
    pub tags: usize,
    pub compacted_covers: usize,
    pub queries: usize,
    pub full_closure_queries: usize,
    pub haves_sent: usize,
    pub haves_resolved_by_ancestry: usize,
    pub haves_unknown_to_server: usize,
    pub haves_dropped: usize,
    pub covers_used: usize,
    /// Covers that carry a chain: verified ones, and forged ones in a
    /// [`Chains::Forged`] world.
    pub chained_covers: usize,
    pub forged_chains: usize,
    /// Held ranges the planner registered from chains, the queries that
    /// took at least one, and those of them whose plan sent more than the
    /// needed versions' packs (the cost the walk's docs describe).
    pub chain_skips: usize,
    pub queries_with_chain_skips: usize,
    pub chain_skip_overfetches: usize,
    /// Objects git says the clients lacked, and objects the plans held.
    pub objects_missing: usize,
    pub objects_planned: usize,
}

impl Coverage {
    pub(crate) fn add(&mut self, o: &Coverage) {
        self.versions += o.versions;
        self.forced_under_compacted_cover += o.forced_under_compacted_cover;
        self.trunk_dep_skips += o.trunk_dep_skips;
        self.forced_trunk += o.forced_trunk;
        self.exact_forced_trunk += o.exact_forced_trunk;
        self.wider_versions += o.wider_versions;
        self.scoped_pushes += o.scoped_pushes;
        self.deletions += o.deletions;
        self.tags += o.tags;
        self.compacted_covers += o.compacted_covers;
        self.queries += o.queries;
        self.full_closure_queries += o.full_closure_queries;
        self.haves_sent += o.haves_sent;
        self.haves_resolved_by_ancestry += o.haves_resolved_by_ancestry;
        self.haves_unknown_to_server += o.haves_unknown_to_server;
        self.haves_dropped += o.haves_dropped;
        self.covers_used += o.covers_used;
        self.chained_covers += o.chained_covers;
        self.forged_chains += o.forged_chains;
        self.chain_skips += o.chain_skips;
        self.queries_with_chain_skips += o.queries_with_chain_skips;
        self.chain_skip_overfetches += o.chain_skip_overfetches;
        self.objects_missing += o.objects_missing;
        self.objects_planned += o.objects_planned;
    }
}

/// One fetch to plan, with what git needs to answer it.
#[derive(Debug)]
pub(crate) struct Query {
    pub input: PlanInput,
    pub want_oids: Vec<ObjectId>,
    /// The haves as the client sent them. Empty for a full-closure query,
    /// where the planner must not use them.
    pub have_oids: Vec<ObjectId>,
    pub describe: String,
}

#[derive(Debug)]
struct Script {
    commits: Vec<CommitSpec>,
    /// Annotated tags, by target commit.
    tags: Vec<usize>,
    junk: usize,
    pushes: Vec<PushSpec>,
    ancestors: Vec<BTreeSet<usize>>,
}

impl Script {
    fn generate(rng: &mut Rng) -> Self {
        let mut s = Script {
            commits: Vec::new(),
            tags: Vec::new(),
            junk: 0,
            pushes: Vec::new(),
            ancestors: Vec::new(),
        };
        let mut heads: BTreeMap<String, usize> = BTreeMap::new();
        let first = s.chain(rng, None, 1, 2);
        s.record(
            &mut heads,
            rng,
            TRUNK,
            Some(Obj::Commit(first)),
            None,
            None,
            "trunk create",
        );

        let steps = 8 + rng.below(14);
        for _ in 0..steps {
            let trunk = heads[TRUNK];
            let trunk_tip = s.commit_of(trunk);
            let branches: Vec<String> = heads
                .iter()
                .filter(|(n, v)| n.starts_with("refs/heads/b") && s.pushes[**v].tip.is_some())
                .map(|(n, _)| n.clone())
                .collect();
            let op = rng.weighted(&[30, 4, 12, 14, 10, 12, 5, 5]);
            match (op, branches.is_empty()) {
                // Trunk fast-forward, sometimes merging a branch, sometimes
                // a compare-and-swap recompute whose trunk dependency is the
                // version before the current one.
                (0, _) | (3..=6, true) => {
                    let mut base = trunk_tip;
                    if !branches.is_empty() && rng.below(4) == 0 {
                        let b = s.commit_of(heads[&branches[rng.below(branches.len())]]);
                        base = s.commit(rng, vec![base, b]);
                    }
                    let tip = s.chain(rng, Some(base), 0, 2);
                    let tip = if tip == trunk_tip {
                        s.commit(rng, vec![tip])
                    } else {
                        tip
                    };
                    let parent = s.parent_version(trunk);
                    let (dep, kind) = match parent {
                        Some(p) if rng.below(5) == 0 => (p, "trunk ff, trunk_dep skips a version"),
                        _ => (trunk, "trunk ff"),
                    };
                    s.record(
                        &mut heads,
                        rng,
                        TRUNK,
                        Some(Obj::Commit(tip)),
                        Some(dep),
                        None,
                        kind,
                    );
                }
                // Forced trunk update (a repository with allow_force = all),
                // onto an older trunk commit or onto unrelated (orphan)
                // history. Mostly under B1's rule (decisions log): the trunk
                // dependency is the newest trunk version whose tip the new
                // tip reaches, or none, so the pack carries what the skipped
                // versions held and the version stays exact. Sometimes the
                // rule before B1: the dependency is the old trunk version,
                // which makes trunk wider for good.
                (1, _) => {
                    let base = (rng.below(4) != 0).then(|| s.random_ancestor(rng, trunk_tip));
                    let tip = s.chain(rng, base, 1, 1);
                    let (dep, kind) = if rng.below(4) != 0 {
                        let newest_below = (0..s.pushes.len()).rev().find(|&p| {
                            s.pushes[p].ref_name == TRUNK
                                && s.pushes[p]
                                    .tip
                                    .is_some_and(|t| s.reaches(t, Obj::Commit(tip)))
                        });
                        (newest_below, "trunk forced, B1 rule")
                    } else {
                        (Some(trunk), "trunk forced, dep is the old tip")
                    };
                    s.record(
                        &mut heads,
                        rng,
                        TRUNK,
                        Some(Obj::Commit(tip)),
                        dep,
                        None,
                        kind,
                    );
                }
                // Branch create (or re-create after a delete) at a trunk
                // commit, often an old one, with zero or more new commits.
                (2, _) => {
                    let name = format!("refs/heads/b{}", rng.below(3));
                    if branches.contains(&name) {
                        continue;
                    }
                    let base = if rng.below(2) == 0 {
                        trunk_tip
                    } else {
                        s.random_ancestor(rng, trunk_tip)
                    };
                    let tip = s.chain(rng, Some(base), 0, 2);
                    s.record(
                        &mut heads,
                        rng,
                        &name,
                        Some(Obj::Commit(tip)),
                        Some(trunk),
                        None,
                        "branch create",
                    );
                }
                // Branch fast-forward, through the plain or the scoped URL,
                // sometimes merging trunk in first.
                (3 | 4, false) => {
                    let name = branches[rng.below(branches.len())].clone();
                    let old = heads[&name];
                    let mut base = s.commit_of(old);
                    if rng.below(3) == 0 {
                        base = s.commit(rng, vec![base, trunk_tip]);
                    }
                    let tip = s.chain(rng, Some(base), 1, 1);
                    let scoped = (op == 4).then_some(old);
                    let kind = if scoped.is_some() {
                        "branch ff, scoped"
                    } else {
                        "branch ff"
                    };
                    s.record(
                        &mut heads,
                        rng,
                        &name,
                        Some(Obj::Commit(tip)),
                        Some(trunk),
                        scoped,
                        kind,
                    );
                }
                // Forced branch update onto an unrelated commit.
                (5, false) => {
                    let name = branches[rng.below(branches.len())].clone();
                    let old = heads[&name];
                    let base = match rng.below(3) {
                        0 => trunk_tip,
                        1 => s.random_ancestor(rng, trunk_tip),
                        _ => s.commit_of(heads[&branches[rng.below(branches.len())]]),
                    };
                    let mut tip = s.chain(rng, Some(base), 0, 2);
                    if tip == s.commit_of(old) {
                        // Same tip: a no-op, which writes no version.
                        tip = s.commit(rng, vec![tip]);
                    }
                    let scoped = (rng.below(2) == 0).then_some(old);
                    s.record(
                        &mut heads,
                        rng,
                        &name,
                        Some(Obj::Commit(tip)),
                        Some(trunk),
                        scoped,
                        "branch forced",
                    );
                }
                (6, false) => {
                    let name = branches[rng.below(branches.len())].clone();
                    s.record(&mut heads, rng, &name, None, None, None, "branch delete");
                }
                // Annotated tag on a commit the server has.
                _ => {
                    let name = format!("refs/tags/v{}", s.tags.len());
                    let target = s.random_server_commit(rng);
                    s.tags.push(target);
                    let tag = Obj::Tag(s.tags.len() - 1);
                    s.record(&mut heads, rng, &name, Some(tag), Some(trunk), None, "tag");
                }
            }
        }
        // Commits only a client has: haves the server has never seen.
        for _ in 0..1 + rng.below(3) {
            let base = s.random_server_commit(rng);
            let start = s.commits.len();
            s.chain(rng, Some(base), 1, 1);
            // `chain` marks commits as pushed; these were not.
            for c in &mut s.commits[start..] {
                c.server = false;
            }
        }
        s
    }

    #[allow(clippy::too_many_arguments)]
    fn record(
        &mut self,
        heads: &mut BTreeMap<String, usize>,
        rng: &mut Rng,
        ref_name: &str,
        tip: Option<Obj>,
        trunk_dep: Option<usize>,
        scoped_old: Option<usize>,
        kind: &'static str,
    ) {
        let live = tip.is_some();
        // One unreachable blob in a trunk push makes trunk, and everything
        // built on it, wider for good; keep that rare so most histories
        // still have exact versions to resolve haves to.
        let junk_odds = if ref_name == TRUNK { 40 } else { 8 };
        let junk = (live && rng.below(junk_odds) == 0).then(|| {
            self.junk += 1;
            self.junk - 1
        });
        let bases = if live && rng.below(3) == 0 {
            1 + rng.below(3)
        } else {
            0
        };
        self.pushes.push(PushSpec {
            ref_name: ref_name.to_owned(),
            tip,
            trunk_dep,
            scoped_old,
            junk,
            bases,
            kind,
        });
        heads.insert(ref_name.to_owned(), self.pushes.len() - 1);
    }

    /// The commit a live version points at (a tag's target for a tag).
    fn commit_of(&self, push: usize) -> usize {
        match self.pushes[push].tip.expect("a live version") {
            Obj::Commit(c) => c,
            Obj::Tag(t) => self.tags[t],
        }
    }

    fn parent_version(&self, push: usize) -> Option<usize> {
        let name = &self.pushes[push].ref_name;
        self.pushes[..push]
            .iter()
            .rposition(|p| &p.ref_name == name)
    }

    fn commit(&mut self, rng: &mut Rng, parents: Vec<usize>) -> usize {
        let mut files = parents
            .first()
            .map(|&p| self.commits[p].files.clone())
            .unwrap_or_default();
        if let Some(&second) = parents.get(1) {
            for (path, content) in &self.commits[second].files {
                if rng.below(2) == 0 {
                    files.insert(path, *content);
                }
            }
        }
        for _ in 0..1 + rng.below(2) {
            let path = PATHS[rng.below(PATHS.len())];
            if rng.below(5) == 0 {
                files.remove(path);
            } else {
                files.insert(path, rng.below(CONTENTS as usize) as u32);
            }
        }
        let mut ancestors: BTreeSet<usize> = BTreeSet::new();
        for &p in &parents {
            ancestors.extend(self.ancestors[p].iter().copied());
        }
        let id = self.commits.len();
        ancestors.insert(id);
        self.ancestors.push(ancestors);
        self.commits.push(CommitSpec {
            parents,
            files,
            server: true,
        });
        id
    }

    /// `min + rng.below(extra + 1)` commits on `base` (at least one without
    /// a base); returns the last, or `base` when there are none.
    fn chain(&mut self, rng: &mut Rng, base: Option<usize>, min: usize, extra: usize) -> usize {
        let k = min + rng.below(extra + 1);
        let mut tip = base;
        for _ in 0..k.max(usize::from(base.is_none())) {
            tip = Some(self.commit(rng, tip.into_iter().collect()));
        }
        tip.expect("at least one commit")
    }

    fn random_ancestor(&self, rng: &mut Rng, c: usize) -> usize {
        let a = &self.ancestors[c];
        *a.iter().nth(rng.below(a.len())).unwrap()
    }

    fn random_server_commit(&self, rng: &mut Rng) -> usize {
        let server: Vec<usize> = (0..self.commits.len())
            .filter(|&c| self.commits[c].server)
            .collect();
        server[rng.below(server.len())]
    }

    /// Whether `a` is `b` or reachable from it.
    fn reaches(&self, a: Obj, b: Obj) -> bool {
        match (a, b) {
            _ if a == b => true,
            (Obj::Tag(_), _) => false,
            (Obj::Commit(x), Obj::Commit(c)) => self.ancestors[c].contains(&x),
            (Obj::Commit(x), Obj::Tag(t)) => self.ancestors[self.tags[t]].contains(&x),
        }
    }
}

/// Which covers of a world carry a chain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Chains {
    /// Three in four of the covers whose range verifies, as a compactor
    /// that has not chained every cover yet would leave them.
    Verified,
    /// Every cover: a verified chain where the range verifies, and a
    /// forged one ([`CoverChain::recorded`](aspen_core::meta::CoverChain::recorded))
    /// where it does not.
    Forged,
}

/// A generated history: the git repository, the planner's graph and the
/// queries to check.
#[derive(Debug)]
pub(crate) struct World {
    pub seed: u64,
    pub graph: Graph,
    pub queries: Vec<Query>,
    pub coverage: Coverage,
    pub log: String,
    pub repo: PathBuf,
    _dir: tempfile::TempDir,
}

impl World {
    pub(crate) fn generate(seed: u64) -> Self {
        Self::generate_with(seed, Chains::Verified)
    }

    /// A world whose covers carry chains as `chains` says. The chain
    /// decisions draw from their own generator, so a seed's history,
    /// covers and queries are the same with either policy.
    pub(crate) fn generate_with(seed: u64, chains: Chains) -> Self {
        let mut rng = Rng::new(seed);
        let script = Script::generate(&mut rng);
        let dir = tempfile::tempdir().expect("temp dir");
        let repo = dir.path().join("repo.git");
        let oids = import(dir.path(), &repo, &script);
        let mut world = World {
            seed,
            graph: Graph::default(),
            queries: Vec::new(),
            coverage: Coverage::default(),
            log: String::new(),
            repo,
            _dir: dir,
        };
        let version_ids = world.build_graph(&mut rng, &script, &oids);
        let mut chain_rng = Rng::new(seed ^ 0xc1c1_c4a1_0000_0000);
        world.add_covers(&mut rng, &mut chain_rng, chains, &script, &version_ids);
        world.add_queries(&mut rng, &script, &oids, &version_ids);
        world
    }

    fn build_graph(&mut self, rng: &mut Rng, s: &Script, oids: &Oids) -> Vec<RefVersionId> {
        let mut ids = Vec::new();
        let mut closures: Vec<BTreeSet<ObjectId>> = Vec::new();
        for p in &s.pushes {
            let rname = name(&p.ref_name);
            let Some(tip) = p.tip else {
                let id = self.graph.push(&rname, RefValue::Deleted, deps(None, None));
                writeln!(self.log, "  v{id} {} {}: deleted", p.ref_name, p.kind).unwrap();
                ids.push(id);
                closures.push(BTreeSet::new());
                self.coverage.deletions += 1;
                continue;
            };
            let dep_pushes: Vec<usize> = p.trunk_dep.into_iter().chain(p.scoped_old).collect();
            let live_dep_tips: Vec<Obj> =
                dep_pushes.iter().filter_map(|&d| s.pushes[d].tip).collect();
            let mut objects = rev_list(
                &self.repo,
                &[oids.of(tip)],
                &live_dep_tips
                    .iter()
                    .map(|&t| oids.of(t))
                    .collect::<Vec<_>>(),
            );
            if !objects.is_empty() && p.bases > 0 {
                let dep_closure: Vec<ObjectId> = dep_pushes
                    .iter()
                    .flat_map(|&d| closures[d].iter().copied())
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect();
                for _ in 0..p.bases.min(dep_closure.len()) {
                    objects.insert(dep_closure[rng.below(dep_closure.len())]);
                }
            }
            if let Some(j) = p.junk {
                objects.insert(oids.junk[j]);
            }
            let dep_values: Vec<(RefValue, Option<AncestryAnswer>)> = dep_pushes
                .iter()
                .map(|&d| {
                    let value = self.graph.versions[&ids[d]].value;
                    let answer = s.pushes[d].tip.map(|dt| {
                        let a = if s.reaches(dt, tip) {
                            Ancestry::Descendant
                        } else {
                            Ancestry::NotDescendant
                        };
                        AncestryAnswer::new(oids.of(dt), oids.of(tip), a)
                    });
                    (value, answer)
                })
                .collect();
            let closure = Closure::of_new_version(
                oids.of(tip),
                p.junk.is_none(),
                dep_values.iter().map(|(v, a)| (v, *a)),
            )
            .expect("answers are for the asked pairs");
            let mut closure_objects = objects.clone();
            for &d in &dep_pushes {
                closure_objects.extend(closures[d].iter().copied());
            }
            closures.push(closure_objects);
            let pack = self.graph.add_pack(objects.clone(), None);
            let dep_ids = deps(p.trunk_dep.map(|d| ids[d]), p.scoped_old.map(|d| ids[d]));
            let id = self
                .graph
                .push(&rname, live(oids.of(tip), pack, closure), dep_ids);
            writeln!(
                self.log,
                "  v{id} {} seq {} {}: {:?} tip {:?}, deps {:?}/{:?}, {} objects{}{}",
                p.ref_name,
                self.graph.versions[&id].seq,
                p.kind,
                closure,
                tip,
                dep_ids.trunk.map(|d| d.get()),
                dep_ids.scoped_old.map(|d| d.get()),
                objects.len(),
                if p.junk.is_some() {
                    ", unreachable blob"
                } else {
                    ""
                },
                if p.bases > 0 {
                    ", completion bases"
                } else {
                    ""
                },
            )
            .unwrap();
            ids.push(id);
            self.coverage.versions += 1;
            self.coverage.wider_versions += usize::from(closure == Closure::Wider);
            self.coverage.scoped_pushes += usize::from(p.scoped_old.is_some());
            self.coverage.tags += usize::from(matches!(tip, Obj::Tag(_)));
            self.coverage.trunk_dep_skips += usize::from(p.kind.contains("skips"));
            self.coverage.forced_trunk += usize::from(p.kind.starts_with("trunk forced"));
            self.coverage.exact_forced_trunk +=
                usize::from(p.kind.starts_with("trunk forced") && closure == Closure::Exact);
        }
        ids
    }

    fn add_covers(
        &mut self,
        rng: &mut Rng,
        chain_rng: &mut Rng,
        chains: Chains,
        s: &Script,
        ids: &[RefVersionId],
    ) {
        let heads: Vec<_> = self
            .graph
            .heads
            .iter()
            .map(|(n, v)| (n.clone(), *v))
            .collect();
        for (rname, head) in heads {
            let max = self.graph.versions[&head].seq.get();
            let mut spans = BTreeSet::new();
            // Aligned, LSM-like covers on trunk, sometimes.
            if rname.as_str() == TRUNK && rng.below(3) == 0 {
                for (level, span) in [(1u8, 2u64), (2, 4), (3, 8), (4, 16)] {
                    let mut first = 1;
                    while first + span - 1 <= max {
                        spans.insert((level, first, first + span - 1));
                        first += span;
                    }
                }
            }
            let random_covers = if rname.as_str() == TRUNK {
                rng.below(4)
            } else {
                1 + rng.below(4)
            };
            for _ in 0..random_covers {
                let level = 1 + rng.below(4) as u8;
                let first = 1 + rng.below(max as usize) as u64;
                let last = first + rng.below((max - first + 1) as usize) as u64;
                spans.insert((level, first, last));
            }
            let mut seen = BTreeSet::new();
            for (level, first, last) in spans {
                if !seen.insert((level, first)) {
                    continue;
                }
                let compacted = rng.below(4) != 0;
                let range =
                    SeqRange::new(Seq::new(first).unwrap(), Seq::new(last).unwrap()).unwrap();
                let verifies = self.graph.verify(&rname, range).is_ok();
                let chain = match chains {
                    Chains::Verified if verifies && chain_rng.below(4) != 0 => Chain::Verified,
                    Chains::Verified => Chain::None,
                    Chains::Forged if verifies => Chain::Verified,
                    Chains::Forged => Chain::Forged,
                };
                let recorded = self
                    .graph
                    .add_cover_with(&rname, level, first, last, compacted, chain);
                self.coverage.chained_covers += usize::from(recorded.is_some());
                self.coverage.forged_chains += usize::from(chain == Chain::Forged);
                writeln!(
                    self.log,
                    "  cover {rname} L{level} {first}..={last}{}{}",
                    if compacted { "" } else { " (not compacted)" },
                    match chain {
                        Chain::None if verifies => ", chain left off",
                        Chain::None => "",
                        Chain::Verified => ", chained",
                        Chain::Forged => ", FORGED chain",
                    }
                )
                .unwrap();
                if compacted {
                    self.coverage.compacted_covers += 1;
                    let forced = s.pushes.iter().zip(ids).any(|(p, id)| {
                        let v = &self.graph.versions[id];
                        (p.kind == "branch forced" || p.kind.starts_with("trunk forced"))
                            && v.name == rname
                            && (first..=last).contains(&v.seq.get())
                    });
                    self.coverage.forced_under_compacted_cover += usize::from(forced);
                }
            }
        }
    }

    fn add_queries(&mut self, rng: &mut Rng, s: &Script, oids: &Oids, ids: &[RefVersionId]) {
        let live: Vec<usize> = (0..s.pushes.len())
            .filter(|&i| s.pushes[i].tip.is_some())
            .collect();
        let unknown: Vec<usize> = (0..s.commits.len())
            .filter(|&c| !s.commits[c].server)
            .collect();
        for _ in 0..6 {
            let mode = if rng.below(6) == 0 {
                PlanMode::FullClosure
            } else {
                PlanMode::Incremental
            };
            let mut wants = Vec::new();
            let mut want_oids = Vec::new();
            let mut describe = String::new();
            for _ in 0..1 + rng.below(3) {
                // A current or historical tip, or any commit the server has,
                // resolved as `commit_index` would: to some version whose
                // tip reaches it.
                let (obj, push) = if rng.below(2) == 0 {
                    let p = live[rng.below(live.len())];
                    (s.pushes[p].tip.unwrap(), p)
                } else {
                    let c = Obj::Commit(s.random_server_commit(rng));
                    let holders: Vec<usize> = live
                        .iter()
                        .copied()
                        .filter(|&p| s.reaches(c, s.pushes[p].tip.unwrap()))
                        .collect();
                    (c, holders[rng.below(holders.len())])
                };
                wants.push(ids[push]);
                want_oids.push(oids.of(obj));
                write!(describe, "want {obj:?} as v{}; ", ids[push]).unwrap();
            }
            let mut have_objs = Vec::new();
            for _ in 0..rng.below(5) {
                let obj = match rng.below(3) {
                    0 => Obj::Commit(s.commit_of(live[rng.below(live.len())])),
                    1 => Obj::Commit(s.random_server_commit(rng)),
                    _ => Obj::Commit(unknown[rng.below(unknown.len())]),
                };
                have_objs.push(obj);
            }
            // A client at a later version of a ref fetching an earlier
            // version's tip by ID (after a forced update, the old tip is
            // not in the new version's closure). This is the shape that
            // makes subtracting a cover of that ref for the have unsafe.
            if rng.below(2) == 0 {
                let refs: Vec<&String> = s.pushes.iter().map(|p| &p.ref_name).collect();
                let name = refs[rng.below(refs.len())];
                let chain: Vec<usize> = live
                    .iter()
                    .copied()
                    .filter(|&p| &s.pushes[p].ref_name == name)
                    .collect();
                // Half the time, insist on the hard case: the later version
                // is exact (so the have resolves to it) and its tip does not
                // reach the earlier tip (a force in between).
                let chain = &chain;
                let forced_pairs: Vec<(usize, usize)> = (0..chain.len())
                    .flat_map(|j| (0..j).map(move |i| (chain[i], chain[j])))
                    .filter(|&(old, new)| {
                        self.graph.versions[&ids[new]].value.closure() == Some(Closure::Exact)
                            && !s.reaches(s.pushes[old].tip.unwrap(), s.pushes[new].tip.unwrap())
                    })
                    .collect();
                let pair = if !forced_pairs.is_empty() && rng.below(2) == 0 {
                    Some(forced_pairs[rng.below(forced_pairs.len())])
                } else if chain.len() >= 2 {
                    let j = 1 + rng.below(chain.len() - 1);
                    Some((chain[rng.below(j)], chain[j]))
                } else {
                    None
                };
                if let Some((old, new)) = pair {
                    let old_tip = s.pushes[old].tip.unwrap();
                    wants.push(ids[old]);
                    want_oids.push(oids.of(old_tip));
                    have_objs.push(Obj::Commit(s.commit_of(new)));
                    write!(
                        describe,
                        "want {old_tip:?} as v{} (earlier version of {name}); ",
                        ids[old]
                    )
                    .unwrap();
                }
            }
            let mut haves = BTreeMap::new();
            for &h in &have_objs {
                let Obj::Commit(hc) = h else { unreachable!() };
                let known = s.commits[hc].server;
                self.coverage.haves_sent += 1;
                self.coverage.haves_unknown_to_server += usize::from(!known);
                let mut resolved = false;
                for &p in &live {
                    // Skip 1 in 4 candidate versions, as a fetch service that
                    // resolves haves only partly would. This, plus completion
                    // bases, is most of the generator's over-fetch: the ratio
                    // of planned to lacking objects is a property of this
                    // generator, not of Aspen (D2 measures real bytes).
                    if rng.below(4) == 0 {
                        continue;
                    }
                    let tip = s.pushes[p].tip.unwrap();
                    let version = &self.graph.versions[&ids[p]];
                    if let Some(hv) = HaveVersion::exact_tip(oids.of(h), version) {
                        haves.insert(hv.version(), hv);
                        resolved = true;
                        continue;
                    }
                    // The server cannot always decide ancestry for a commit
                    // it has never seen; then it answers Undecided.
                    let answer = if !known && rng.below(2) == 0 {
                        Ancestry::Undecided
                    } else if s.reaches(tip, h) {
                        Ancestry::Descendant
                    } else {
                        Ancestry::NotDescendant
                    };
                    let answer = AncestryAnswer::new(oids.of(tip), oids.of(h), answer);
                    if let Some(hv) = HaveVersion::ancestor_of_have(oids.of(h), version, answer)
                        .expect("answers the asked pair")
                    {
                        haves.insert(hv.version(), hv);
                        resolved = true;
                        self.coverage.haves_resolved_by_ancestry += 1;
                    }
                }
                self.coverage.haves_dropped += usize::from(!resolved);
                write!(
                    describe,
                    "have {h:?}{}; ",
                    if resolved { "" } else { " (dropped)" }
                )
                .unwrap();
            }
            let input = PlanInput::new(mode, wants, haves.into_values().collect());
            write!(
                describe,
                "{mode:?}, have versions {:?}",
                input
                    .haves()
                    .iter()
                    .map(|h| h.version().get())
                    .collect::<Vec<_>>()
            )
            .unwrap();
            let have_oids = match mode {
                PlanMode::Incremental => have_objs.iter().map(|&h| oids.of(h)).collect(),
                PlanMode::FullClosure => Vec::new(),
            };
            self.coverage.queries += 1;
            self.coverage.full_closure_queries += usize::from(mode == PlanMode::FullClosure);
            self.queries.push(Query {
                input,
                want_oids,
                have_oids,
                describe,
            });
        }
    }

    /// What the client lacks: `git rev-list --objects <wants>` minus
    /// `git rev-list --objects <haves>`.
    ///
    /// Not `git rev-list --objects <wants> --not <haves>`: that form marks
    /// only the trees of the boundary commits uninteresting, so it also
    /// lists objects the haves reach through older commits (a blob deleted
    /// and later restored, say). Git sends those again, which is safe; a
    /// check against them would demand objects the client already has.
    pub(crate) fn missing_objects(&self, q: &Query) -> BTreeSet<ObjectId> {
        let wanted = rev_list(&self.repo, &q.want_oids, &[]);
        if q.have_oids.is_empty() {
            return wanted;
        }
        let held = rev_list(&self.repo, &q.have_oids, &[]);
        wanted.difference(&held).copied().collect()
    }

    /// `git rev-list --objects <wants> --not <haves>`, the approximate form
    /// that `git upload-pack` sends (see [`World::missing_objects`]).
    pub(crate) fn boundary_objects(&self, q: &Query) -> BTreeSet<ObjectId> {
        rev_list(&self.repo, &q.want_oids, &q.have_oids)
    }
}

#[derive(Debug)]
struct Oids {
    commits: Vec<ObjectId>,
    tags: Vec<ObjectId>,
    junk: Vec<ObjectId>,
}

impl Oids {
    fn of(&self, o: Obj) -> ObjectId {
        match o {
            Obj::Commit(c) => self.commits[c],
            Obj::Tag(t) => self.tags[t],
        }
    }
}

const COMMIT_MARK: usize = 1;
const TAG_MARK: usize = 1_000_000;
const JUNK_MARK: usize = 2_000_000;

/// Writes every object of the script with one `git fast-import`.
fn import(home: &Path, repo: &Path, s: &Script) -> Oids {
    aspen_testkit::git::run(
        isolated_git(home)
            .args(["init", "--quiet", "--bare"])
            .arg(repo),
    )
    .expect("git init");
    let mut stream = Vec::new();
    let data = |out: &mut Vec<u8>, bytes: &[u8]| {
        writeln!(out, "data {}", bytes.len()).unwrap();
        out.extend_from_slice(bytes);
        out.push(b'\n');
    };
    for j in 0..s.junk {
        writeln!(stream, "blob\nmark :{}", JUNK_MARK + j).unwrap();
        data(&mut stream, format!("unreachable blob {j}\n").as_bytes());
    }
    for (i, c) in s.commits.iter().enumerate() {
        let when = 1_700_000_000 + i as u64 * 60;
        writeln!(stream, "reset refs/heads/scratch").unwrap();
        writeln!(
            stream,
            "commit refs/heads/scratch\nmark :{}",
            COMMIT_MARK + i
        )
        .unwrap();
        writeln!(stream, "author A <a@aspen.invalid> {when} +0000").unwrap();
        writeln!(stream, "committer A <a@aspen.invalid> {when} +0000").unwrap();
        data(&mut stream, format!("commit {i}\n").as_bytes());
        for (k, p) in c.parents.iter().enumerate() {
            let verb = if k == 0 { "from" } else { "merge" };
            writeln!(stream, "{verb} :{}", COMMIT_MARK + p).unwrap();
        }
        writeln!(stream, "deleteall").unwrap();
        for (path, content) in &c.files {
            writeln!(stream, "M 100644 inline {path}").unwrap();
            data(&mut stream, format!("content {content}\n").as_bytes());
        }
    }
    for (t, &target) in s.tags.iter().enumerate() {
        writeln!(
            stream,
            "tag v{t}\nmark :{}\nfrom :{}",
            TAG_MARK + t,
            COMMIT_MARK + target
        )
        .unwrap();
        writeln!(stream, "tagger A <a@aspen.invalid> 1700000000 +0000").unwrap();
        data(&mut stream, format!("tag {t}\n").as_bytes());
    }
    let marks = home.join("marks");
    let mut child = isolated_git(home)
        .current_dir(repo)
        .args(["fast-import", "--quiet"])
        .arg(format!("--export-marks={}", marks.display()))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("git fast-import");
    child.stdin.take().unwrap().write_all(&stream).unwrap();
    let out = child.wait_with_output().unwrap();
    assert!(
        out.status.success(),
        "fast-import: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let mut by_mark = BTreeMap::new();
    for line in std::fs::read_to_string(&marks).unwrap().lines() {
        let (mark, oid) = line.split_once(' ').unwrap();
        by_mark.insert(
            mark[1..].parse::<usize>().unwrap(),
            ObjectId::parse_hex(oid).unwrap(),
        );
    }
    Oids {
        commits: (0..s.commits.len())
            .map(|i| by_mark[&(COMMIT_MARK + i)])
            .collect(),
        tags: (0..s.tags.len())
            .map(|t| by_mark[&(TAG_MARK + t)])
            .collect(),
        junk: (0..s.junk).map(|j| by_mark[&(JUNK_MARK + j)]).collect(),
    }
}

/// `git rev-list --objects <include> --not <exclude>`.
fn rev_list(repo: &Path, include: &[ObjectId], exclude: &[ObjectId]) -> BTreeSet<ObjectId> {
    let mut cmd = isolated_git(repo);
    cmd.current_dir(repo).args(["rev-list", "--objects"]);
    cmd.args(include.iter().map(ObjectId::to_hex));
    if !exclude.is_empty() {
        cmd.arg("--not").args(exclude.iter().map(ObjectId::to_hex));
    }
    let out = aspen_testkit::git::run(&mut cmd).expect("git rev-list");
    out.lines()
        .map(|l| ObjectId::parse_hex(&l[..40]).expect("rev-list prints object IDs"))
        .collect()
}
