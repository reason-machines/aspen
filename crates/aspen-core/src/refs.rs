//! Refs, ref versions, push commands and the commit check.
//!
//! A ref's history is a chain of [`RefVersion`]s; the `refs` table points at
//! the current one and is the only compare-and-swap target in Aspen. The
//! commit check ([`CommitCheck`]) turns one push command into a [`Verdict`]
//! computed against one exact version of the ref, following the rule table
//! of spike S2 (`bench/spikes/s2/RESULT.md`, "Commit check").

use std::collections::{BTreeMap, BTreeSet};

use crate::error::RejectReason;
use crate::id::{ObjectId, PackId, RefVersionId, RepoId, Seq, UnixMicros};
use crate::name::{NameError, RefName};
use crate::repo::RepoSettings;

/// How many times a push recomputes its verdicts after losing a
/// compare-and-swap before it answers with a retryable `ng`.
pub const CAS_RECOMPUTE_LIMIT: u32 = 3;

/// How a live version's **closure** (the objects in its pack and, through
/// its dependencies, every earlier pack it builds on) relates to the
/// objects reachable from its tip. The connected invariant makes the closure
/// a superset of what the tip reaches; this says whether it is exactly that.
///
/// The difference matters for fetch haves. A client that has commit `h` is
/// guaranteed only the objects `h` reaches. If the planner subtracted the
/// closure of a version `V` with `V.tip = h` while `V`'s closure holds more
/// than `h` reaches, it would leave out objects the client lacks. Two
/// ordinary pushes make closures wider than their tips:
///
/// - a branch created at an old trunk commit `c` while trunk is at `T2`:
///   its trunk dependency is `T2`, so its closure holds `T2`'s objects that
///   `c` does not reach;
/// - a forced update whose dependency's tip is not an ancestor of the new
///   tip. That is a forced push through a branch-scoped URL (the scoped
///   dependency is the branch's old version) or a forced trunk update. A
///   forced push through the plain URL depends only on trunk, so it is
///   judged like any other branch write. B1 makes a forced trunk update
///   exact at push time by choosing its trunk dependency and copying the
///   skipped objects into its pack ([`crate::repo::ForcePolicy::All`]).
///
/// So only [`Closure::Exact`] versions may stand for a have
/// ([`crate::fetch::HaveVersion`]); a [`Closure::Wider`] version is still
/// served in full and costs bytes when the client already has some of it.
/// Wider is inherited: a version that depends on a wider one is wider, so a
/// single wider trunk version makes every later trunk version, and every
/// branch built on them, wider for good. The push path must therefore never
/// make trunk wider in ordinary use (see [`Closure::of_new_version`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Closure {
    /// The closure is exactly the objects reachable from the tip
    /// (`ref_versions.closure_exact = 1`).
    Exact,
    /// The closure may hold objects the tip does not reach (`0`).
    Wider,
}

impl Closure {
    /// The closure of one write's version about to be committed, as the
    /// push path computes it.
    ///
    /// `tip` is the write's new tip. `pack_reachable` is whether every object
    /// in **this write's own pack** is reachable from **this write's own
    /// tip** (vacuously true when the write stores no pack). It is computed
    /// per write, never over the union of the push's tips: with the union, a
    /// `git push main --follow-tags` pack would make main's version exact
    /// although its pack holds the tag object, and a have of main's tip
    /// would then subtract the tag object from a fetch that wants the tag.
    /// The connectivity check already parses every object's references, so
    /// this is an in-memory walk over the pack's own objects. Completion
    /// bases must be excluded from the walk: they come from the
    /// dependencies' closures and are often objects the new tip does not
    /// reach, so counting them would make every thin push `Wider`. B1 owns
    /// the walk, preferably as one core helper whose result
    /// [`crate::meta::RefWrite::from_verdict`] takes; see the decisions log.
    ///
    /// A pack shared by several writes therefore makes every write it holds
    /// foreign objects for [`Closure::Wider`], and a wider trunk version
    /// makes every later trunk version wider too (each depends on the
    /// previous one). B1 splits a multi-ref push into one pack per write so
    /// that this never happens; see the decisions log in `docs/PLAN.md`.
    ///
    /// `deps` holds, for each dependency, its value and the answer to "is
    /// the dependency's tip an ancestor of (or equal to) `tip`?". A deleted
    /// dependency has an empty closure and needs no answer. A live
    /// dependency without an answer, or with [`Ancestry::NotDescendant`] or
    /// [`Ancestry::Undecided`], makes the result [`Closure::Wider`]: a wrong
    /// `Wider` costs bytes (and, on trunk, keeps costing them), a wrong
    /// [`Closure::Exact`] breaks fetches. An answer about any other pair of
    /// commits, including the reversed pair, is an [`AncestryMismatch`].
    pub fn of_new_version<'a>(
        tip: ObjectId,
        pack_reachable: bool,
        deps: impl IntoIterator<Item = (&'a RefValue, Option<AncestryAnswer>)>,
    ) -> Result<Closure, AncestryMismatch> {
        let mut exact = pack_reachable;
        for (dep, answer) in deps {
            let RefValue::Live {
                tip: dep_tip,
                closure,
                ..
            } = *dep
            else {
                continue;
            };
            let ancestry = match answer {
                Some(a) => a.answers(dep_tip, tip)?,
                None => Ancestry::Undecided,
            };
            exact &= closure == Closure::Exact && ancestry == Ancestry::Descendant;
        }
        Ok(if exact {
            Closure::Exact
        } else {
            Closure::Wider
        })
    }
}

/// What a ref version says about its ref.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RefValue {
    /// The ref points at `tip`.
    Live {
        /// The commit (or tag object) the ref points at. Never the zero ID.
        tip: ObjectId,
        /// The pack this push stored, or `None` when the push brought no new
        /// objects (for example a new branch at an existing trunk commit).
        pack: Option<PackId>,
        /// How the version's closure relates to what `tip` reaches.
        closure: Closure,
    },
    /// The ref was deleted. A deletion is a version too, so a later create
    /// is a compare-and-swap against the deletion, and history stays one
    /// chain.
    Deleted,
}

impl RefValue {
    /// The tip, if the ref is live.
    pub fn tip(&self) -> Option<ObjectId> {
        match self {
            Self::Live { tip, .. } => Some(*tip),
            Self::Deleted => None,
        }
    }

    /// The closure kind, if the ref is live.
    pub fn closure(&self) -> Option<Closure> {
        match self {
            Self::Live { closure, .. } => Some(*closure),
            Self::Deleted => None,
        }
    }

    /// The pack, if the version stored one.
    pub fn pack(&self) -> Option<PackId> {
        match self {
            Self::Live { pack, .. } => *pack,
            Self::Deleted => None,
        }
    }
}

/// The ref versions a version's pack depends on (`ref_versions.trunk_dep_id`
/// and `scoped_dep_id`).
///
/// Invariant (**connected**): every object reachable from the version's tip
/// is in its pack or in the closure of these dependencies. The push path's
/// connectivity check enforces it before the version is committed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Deps {
    /// The exact trunk version the push's base lookup and connectivity check
    /// searched. Never re-read at commit time (S2 follow-up). `None` only for
    /// a push into an empty repository.
    pub trunk: Option<RefVersionId>,
    /// For a push through a branch-scoped URL, the version of the scoped ref
    /// that the client named as its old value and the compare-and-swap
    /// verified. For a client's push of one branch through the plain
    /// repository URL that is not forced or leased, that branch's exact
    /// version the push searched (issue #203); after a lost
    /// compare-and-swap that is older than the new version's parent.
    /// `None` otherwise.
    pub scoped_old: Option<RefVersionId>,
}

impl Deps {
    /// The dependencies that are set, trunk first.
    pub fn iter(&self) -> impl Iterator<Item = RefVersionId> {
        self.trunk.into_iter().chain(self.scoped_old)
    }
}

/// One version of one ref (`ref_versions` row).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefVersion {
    /// Row ID.
    pub id: RefVersionId,
    /// Repository.
    pub repo: RepoId,
    /// Ref.
    pub name: RefName,
    /// Position in the ref's history, starting at 1. A version with `seq = n`
    /// has `parent` set to the version with `seq = n - 1`. `.have` tips and
    /// compaction covers address versions by this number.
    pub seq: Seq,
    /// The previous version of the same ref, `None` exactly when `seq == 1`.
    pub parent: Option<RefVersionId>,
    /// Tip and pack, or deleted.
    pub value: RefValue,
    /// Dependencies of the pack.
    pub deps: Deps,
    /// When the version was committed.
    pub created_at: UnixMicros,
}

/// One command of a push, as the client sent it: `<old> <new> <ref>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefCommand {
    /// The ref to update.
    pub name: RefName,
    /// The value the client believes the ref has, or zero. For every
    /// unadvertised ref this is zero (S2).
    pub old: ObjectId,
    /// The new value, or zero for a delete.
    pub new: ObjectId,
}

/// Malformed push option.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PushOptionError {
    /// `force=` or `lease=` names an invalid ref.
    #[error("push option {option:?}: {source}")]
    BadRef {
        /// The option as sent.
        option: String,
        /// Why the ref name is invalid.
        source: NameError,
    },
    /// `lease=<ref>:<oid>` has a bad object ID or no `:`.
    #[error(
        "push option {0:?}: expected lease=<full ref>:<oid>, with <oid> empty for \"must not exist\""
    )]
    BadLease(String),
    /// Two different leases for one ref.
    #[error("two different leases for {0}")]
    ConflictingLease(RefName),
}

/// The push options Aspen interprets (S2). Every other option is ignored and
/// left for hooks.
///
/// - `force`: every ref in the push may be forced.
/// - `force=<full ref name>`: that ref may be forced.
/// - `lease=<full ref name>:<oid>`: expect the ref at `<oid>`, or absent when
///   `<oid>` is empty. A lease replaces the command's old value for that ref,
///   and a matching lease counts as an explicit force.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PushOptions {
    force_all: bool,
    force_refs: BTreeSet<RefName>,
    leases: BTreeMap<RefName, Option<ObjectId>>,
}

impl PushOptions {
    /// Parses the option strings a client sent with `-o`.
    pub fn parse<'a>(options: impl IntoIterator<Item = &'a str>) -> Result<Self, PushOptionError> {
        let mut out = Self::default();
        for option in options {
            if option == "force" {
                out.force_all = true;
            } else if let Some(name) = option.strip_prefix("force=") {
                let name = RefName::new(name).map_err(|source| PushOptionError::BadRef {
                    option: option.to_owned(),
                    source,
                })?;
                out.force_refs.insert(name);
            } else if let Some(spec) = option.strip_prefix("lease=") {
                let (name, oid) = spec
                    .rsplit_once(':')
                    .ok_or_else(|| PushOptionError::BadLease(option.to_owned()))?;
                let name = RefName::new(name).map_err(|source| PushOptionError::BadRef {
                    option: option.to_owned(),
                    source,
                })?;
                let expect = if oid.is_empty() {
                    None
                } else {
                    Some(
                        ObjectId::parse_hex(oid)
                            .map_err(|_| PushOptionError::BadLease(option.to_owned()))?,
                    )
                };
                if let Some(prev) = out.leases.insert(name.clone(), expect)
                    && prev != expect
                {
                    return Err(PushOptionError::ConflictingLease(name));
                }
            }
        }
        Ok(out)
    }

    /// Whether the client asked to force `name` (`-o force` or
    /// `-o force=<name>`).
    pub fn force_requested(&self, name: &RefName) -> bool {
        self.force_all || self.force_refs.contains(name)
    }

    /// The lease for `name`: `Some(None)` means "must not exist".
    pub fn lease(&self, name: &RefName) -> Option<Option<ObjectId>> {
        self.leases.get(name).copied()
    }
}

/// The answer to "is `ancestor` an ancestor of `descendant`?", computed by
/// the caller from `commit_index`, falling back to the packs of the ref
/// version for unindexed commits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ancestry {
    /// `ancestor` is reachable from `descendant`.
    Descendant,
    /// It is not.
    NotDescendant,
    /// It could not be decided: a commit on the walk is neither indexed nor
    /// readable from the packs. Never guessed; becomes a retryable `ng`.
    Undecided,
}

/// An [`Ancestry`] answer together with the pair it answers: "is
/// `ancestor` an ancestor of (or equal to) `descendant`?".
///
/// Every consumer checks the pair against the question it needs answered,
/// so an answer to the reversed question (the one `commit_index` answers
/// most naturally when resolving a have) is an [`AncestryMismatch`], never
/// a silently accepted answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AncestryAnswer {
    ancestor: ObjectId,
    descendant: ObjectId,
    answer: Ancestry,
}

impl AncestryAnswer {
    /// The answer to "is `ancestor` an ancestor of `descendant`?". Every
    /// object reaches itself, so equal operands always answer
    /// [`Ancestry::Descendant`], whatever `answer` says.
    pub fn new(ancestor: ObjectId, descendant: ObjectId, answer: Ancestry) -> Self {
        let answer = if ancestor == descendant {
            Ancestry::Descendant
        } else {
            answer
        };
        Self {
            ancestor,
            descendant,
            answer,
        }
    }

    /// The object asked about as the ancestor.
    pub fn ancestor(&self) -> ObjectId {
        self.ancestor
    }

    /// The object asked about as the descendant.
    pub fn descendant(&self) -> ObjectId {
        self.descendant
    }

    /// The answer.
    pub fn answer(&self) -> Ancestry {
        self.answer
    }

    /// The answer, if this is the answer to "is `ancestor` an ancestor of
    /// `descendant`?".
    pub fn answers(
        &self,
        ancestor: ObjectId,
        descendant: ObjectId,
    ) -> Result<Ancestry, AncestryMismatch> {
        if (self.ancestor, self.descendant) == (ancestor, descendant) {
            Ok(self.answer)
        } else {
            Err(AncestryMismatch {
                asked_ancestor: ancestor,
                asked_descendant: descendant,
                answered_ancestor: self.ancestor,
                answered_descendant: self.descendant,
            })
        }
    }
}

/// An [`AncestryAnswer`] was given for a different pair of objects than the
/// question asked. Always a bug in the caller, never a client error.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error(
    "ancestry answer is about {answered_ancestor} -> {answered_descendant}, \
     but the question was {asked_ancestor} -> {asked_descendant}"
)]
pub struct AncestryMismatch {
    /// The ancestor the question named.
    pub asked_ancestor: ObjectId,
    /// The descendant the question named.
    pub asked_descendant: ObjectId,
    /// The ancestor the answer is about.
    pub answered_ancestor: ObjectId,
    /// The descendant the answer is about.
    pub answered_descendant: ObjectId,
}

/// What a verdict does to the ref.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VerdictKind {
    /// Create a ref that is absent or deleted.
    Create,
    /// Move the ref to a descendant of its tip.
    FastForward,
    /// Move the ref to a commit that does not descend from its tip, or
    /// replace a tag. Needed an explicit signal and the force policy.
    Forced,
    /// Delete the ref; the client named its current value.
    DeleteCas,
    /// Delete the ref without a known old value (`-o force`).
    DeleteUnconditional,
    /// The ref already has the new value: report `ok`, write nothing. A
    /// delete of a ref that does not exist, with no known old value, is a
    /// no-op too, and so is an exact replay: the ref's last move matches the
    /// command's old and new values, and the command's rules accept that
    /// move (see [`CheckInput::previous`]).
    Noop,
    /// Reject the command.
    Reject(RejectReason),
}

impl VerdictKind {
    /// Whether the command is accepted.
    pub fn is_accepted(&self) -> bool {
        !matches!(self, Self::Reject(_))
    }

    /// Whether the command writes a new ref version.
    pub fn writes(&self) -> bool {
        self.is_accepted() && !matches!(self, Self::Noop)
    }
}

/// The result of the commit check for one command, computed against one
/// exact version of the ref.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Verdict {
    /// The ref.
    pub name: RefName,
    /// The version the verdict was computed against, `None` when the ref
    /// has no `refs` row. The commit transaction's compare-and-swap expects
    /// exactly this value; if the ref moved, the verdict is void and must be
    /// recomputed (up to [`CAS_RECOMPUTE_LIMIT`] times).
    pub against: Option<RefVersionId>,
    /// The new value, when the verdict writes one.
    pub new: ObjectId,
    /// What happens.
    pub kind: VerdictKind,
}

/// Everything the commit check reads, besides ancestry.
#[derive(Debug, Clone, Copy)]
pub struct CheckInput<'a> {
    /// Repository settings (trunk, force policy, writable namespaces).
    pub settings: &'a RepoSettings,
    /// The command.
    pub command: &'a RefCommand,
    /// The ref's current version, read in the same transaction view as the
    /// compare-and-swap that will follow; `None` if it has no `refs` row.
    pub current: Option<&'a RefVersion>,
    /// Parsed push options.
    pub options: &'a PushOptions,
    /// The ref named by a branch-scoped URL, if the push came through one.
    pub scoped: Option<&'a RefName>,
    /// The version `current` replaced (its parent), read by the caller only
    /// when [`replay_parent`] names it; `None` otherwise.
    ///
    /// It decides one case: an **exact replay**. libcurl resends a request
    /// when a reused keep-alive connection dies before any answer, so one
    /// `git push` can deliver its receive-pack POST twice; the first copy
    /// commits and its answer is lost (issue #132). The second copy names
    /// an old value (its own, or a lease's) that is no longer the tip, and
    /// the current-tip rules refuse it ([`RejectReason::StaleOld`],
    /// [`RejectReason::LeaseMismatch`] or [`RejectReason::NoSuchRef`]).
    ///
    /// What is checked is an outcome match, not the identity of the
    /// request: the ref's current tip is the command's new value (deleted,
    /// for a delete), the version immediately before it on the same ref had
    /// the command's claimed old value, and the command's own rules (lease,
    /// force policy and signal, fast-forward, tag and trunk protection),
    /// run against that previous version, accept it. Then the verdict is
    /// [`VerdictKind::Noop`] (`ok`, nothing written). If those rules refuse
    /// it, or need ancestry that cannot be decided, the verdict is the
    /// refusal it would have had without `previous` (or the retryable
    /// [`RejectReason::AncestryUndecided`]). A different pusher's update
    /// with the same old and new value that the command would also have
    /// been allowed to make counts as a match. Creates take no part: a
    /// command with a zero old value and no lease claims no old value, so
    /// this rule never applies to it. A resent create (or update of an
    /// unadvertised branch) is checked like any zero-old command; when the
    /// ref is still at its new value that is the plain no-op it always was,
    /// and otherwise it gets the usual zero-old verdict. A version that is
    /// not `current`'s parent on the same ref is ignored.
    pub previous: Option<&'a RefVersion>,
}

/// The old value `command` claims the ref has: a lease's value when the
/// push carries one for the ref, otherwise the command's old value unless
/// it is zero. `None` when it claims nothing, or (a lease of "must not
/// exist") claims absence.
fn claimed_old(command: &RefCommand, options: &PushOptions) -> Option<ObjectId> {
    match options.lease(&command.name) {
        Some(lease) => lease,
        None => Some(command.old).filter(|o| !o.is_zero()),
    }
}

/// The version the commit check needs as [`CheckInput::previous`]:
/// `current`'s parent, when the ref already has the value `command` asks
/// for (its new tip, or deleted for a delete) while the command claims an
/// old value other than the current tip. Only then can the command be an
/// exact replay; otherwise `None`, and the caller reads nothing more.
pub fn replay_parent(
    command: &RefCommand,
    current: Option<&RefVersion>,
    options: &PushOptions,
) -> Option<RefVersionId> {
    let current = current?;
    let claimed = claimed_old(command, options)?;
    let asked = Some(command.new).filter(|n| !n.is_zero());
    let tip = current.value.tip();
    if tip == asked && tip != Some(claimed) {
        current.parent
    } else {
        None
    }
}

/// Whether `current`'s own update matches `command`'s outcome: its parent
/// is `previous`, on the same ref, at the command's claimed old value (see
/// [`CheckInput::previous`]). The command's rules are checked separately.
fn is_exact_replay(
    command: &RefCommand,
    current: Option<&RefVersion>,
    previous: Option<&RefVersion>,
    options: &PushOptions,
) -> bool {
    let (Some(current), Some(previous)) = (current, previous) else {
        return false;
    };
    replay_parent(command, Some(current), options) == Some(previous.id)
        && previous.repo == current.repo
        && previous.name == current.name
        && previous.value.tip() == claimed_old(command, options)
}

/// The commit check for one command: either decided, or waiting for one
/// ancestry answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitCheck {
    /// The verdict needs no history.
    Decided(Verdict),
    /// The verdict depends on whether `ancestor` is an ancestor of
    /// `descendant`; answer with [`NeedsAncestry::resolve`].
    NeedsAncestry(NeedsAncestry),
}

/// A commit check waiting for an ancestry answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NeedsAncestry {
    /// The ref's current tip; for an exact replay (see
    /// [`CheckInput::previous`]), the previous version's tip.
    pub ancestor: ObjectId,
    /// The command's new value.
    pub descendant: ObjectId,
    name: RefName,
    against: Option<RefVersionId>,
    /// Whether a non-descendant may be accepted as a force. `Err` holds the
    /// rejection to use when it may not.
    on_not_descendant: Result<(), RejectReason>,
    /// Set when the question is about an exact replay (see
    /// [`CheckInput::previous`]): [`Self::ancestor`] is the previous
    /// version's tip, an accepting answer is [`VerdictKind::Noop`], and a
    /// refusing one is this refusal, the verdict against the current tip.
    replay_refusal: Option<RejectReason>,
}

impl NeedsAncestry {
    /// Finishes the check with the caller's answer to "is
    /// [`Self::ancestor`] an ancestor of [`Self::descendant`]?". An answer
    /// about any other pair is an [`AncestryMismatch`].
    pub fn resolve(self, answer: AncestryAnswer) -> Result<Verdict, AncestryMismatch> {
        let ancestry = answer.answers(self.ancestor, self.descendant)?;
        let kind = match ancestry {
            Ancestry::Descendant => VerdictKind::FastForward,
            Ancestry::Undecided => VerdictKind::Reject(RejectReason::AncestryUndecided),
            Ancestry::NotDescendant => match self.on_not_descendant {
                Ok(()) => VerdictKind::Forced,
                Err(reason) => VerdictKind::Reject(reason),
            },
        };
        let kind = match (self.replay_refusal, kind) {
            (None, kind) => kind,
            // Never a guessed verdict: retryable, as for any update.
            (Some(_), kind @ VerdictKind::Reject(RejectReason::AncestryUndecided)) => kind,
            (Some(_), kind) if kind.is_accepted() => VerdictKind::Noop,
            (Some(refusal), _) => VerdictKind::Reject(refusal),
        };
        Ok(Verdict {
            name: self.name,
            against: self.against,
            new: self.descendant,
            kind,
        })
    }
}

impl CommitCheck {
    /// Runs the S2 rule table for one command.
    pub fn start(input: CheckInput<'_>) -> CommitCheck {
        let CheckInput {
            settings,
            command,
            current,
            options,
            scoped,
            previous,
        } = input;
        let name = &command.name;
        let against = current.map(|v| v.id);
        let decided = |kind| {
            CommitCheck::Decided(Verdict {
                name: name.clone(),
                against,
                new: command.new,
                kind,
            })
        };
        let reject = |reason| decided(VerdictKind::Reject(reason));
        let needs = |rule: Rule, replay_refusal| match rule {
            Rule::Decided(kind) => decided(kind),
            Rule::Ancestry {
                ancestor,
                on_not_descendant,
            } => CommitCheck::NeedsAncestry(NeedsAncestry {
                ancestor,
                descendant: command.new,
                name: name.clone(),
                against,
                on_not_descendant,
                replay_refusal,
            }),
        };

        if !settings.is_writable(name) {
            return reject(RejectReason::NotWritable);
        }
        let rules = |tip| rule_table(settings, command, tip, options, scoped);
        let normal = rules(current.and_then(|v| v.value.tip()));
        // Issue #132: a refusal only because the ref already made the
        // requested move. The command's own rules, run against the version
        // before that move, decide: accepted there means `ok` with nothing
        // written; refused there keeps the refusal.
        if let Rule::Decided(VerdictKind::Reject(refusal)) = normal
            && let Some(previous) =
                previous.filter(|p| is_exact_replay(command, current, Some(p), options))
        {
            return match rules(previous.value.tip()) {
                Rule::Decided(kind) if kind.is_accepted() => decided(VerdictKind::Noop),
                Rule::Decided(_) => reject(refusal),
                ancestry => needs(ancestry, Some(refusal)),
            };
        }
        needs(normal, None)
    }
}

/// One pass of the S2 rule table, before ancestry.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Rule {
    /// The verdict needs no history.
    Decided(VerdictKind),
    /// The verdict depends on whether `ancestor` is an ancestor of the
    /// command's new value.
    Ancestry {
        ancestor: ObjectId,
        on_not_descendant: Result<(), RejectReason>,
    },
}

/// The S2 rule table for a writable ref whose tip is `cur` (`None` when it
/// is absent or deleted).
fn rule_table(
    settings: &RepoSettings,
    command: &RefCommand,
    cur: Option<ObjectId>,
    options: &PushOptions,
    scoped: Option<&RefName>,
) -> Rule {
    let name = &command.name;
    let decided = Rule::Decided;
    let reject = |reason| Rule::Decided(VerdictKind::Reject(reason));

    // A lease replaces the old value and, when it matches, counts as an
    // explicit force.
    let (old, leased) = match options.lease(name) {
        Some(expected) if expected != cur => {
            return reject(RejectReason::LeaseMismatch);
        }
        Some(_) => (cur, true),
        None => (Some(command.old).filter(|o| !o.is_zero()), false),
    };
    let trunk = settings.trunk();
    let force_allowed = settings.allow_force().permits(name, trunk);
    let forced_by_client = leased || options.force_requested(name);

    if command.new.is_zero() {
        let Some(cur) = cur else {
            // Nothing to delete. With no known old value (zero, or a lease
            // that expects absence) the ref already has the value the client
            // asked for, so it is a no-op, as in stock `git receive-pack`
            // ("deleting a non-existent ref", issue #133). A real old value
            // is a compare-and-swap that cannot hold.
            return match old {
                None => decided(VerdictKind::Noop),
                Some(_) => reject(RejectReason::NoSuchRef),
            };
        };
        if name == trunk {
            return reject(RejectReason::TrunkDelete);
        }
        return match old {
            Some(old) if old != cur => reject(RejectReason::StaleOld),
            Some(_) => decided(VerdictKind::DeleteCas),
            None if options.force_requested(name) && force_allowed => {
                decided(VerdictKind::DeleteUnconditional)
            }
            None if options.force_requested(name) => reject(RejectReason::ForceNotAllowed),
            None => reject(RejectReason::DeleteNeedsForce),
        };
    }

    if let Some(old) = old
        && Some(old) != cur
    {
        return reject(RejectReason::StaleOld);
    }
    let Some(cur) = cur else {
        return decided(VerdictKind::Create);
    };
    if cur == command.new {
        return decided(VerdictKind::Noop);
    }
    let advertised = name == trunk || scoped == Some(name);
    if old.is_none() && advertised {
        return reject(RejectReason::RefExists);
    }
    // The client saw the tip (old = cur) or forced explicitly.
    let explicit = old.is_some() || forced_by_client;
    if name.is_tag() {
        // Tags never fast-forward. Replacing one needs the same explicit
        // signal as a branch force: `-o force`, a matching lease, or the
        // current value as old (stock `git push
        // --force-with-lease=refs/tags/v1:<oid>` sends exactly that).
        return match (explicit, force_allowed) {
            (true, true) => decided(VerdictKind::Forced),
            (true, false) => reject(RejectReason::ForceNotAllowed),
            (false, _) => reject(RejectReason::TagExists),
        };
    }
    let on_not_descendant = match (explicit, force_allowed) {
        (true, true) => Ok(()),
        (true, false) => Err(RejectReason::ForceNotAllowed),
        (false, _) => Err(RejectReason::NonFastForward),
    };
    Rule::Ancestry {
        ancestor: cur,
        on_not_descendant,
    }
}

/// Applies `--atomic`: if any verdict rejects, every accepted verdict is
/// replaced with [`RejectReason::AtomicPeerRejected`].
pub fn apply_atomic(verdicts: Vec<Verdict>) -> Vec<Verdict> {
    if verdicts.iter().all(|v| v.kind.is_accepted()) {
        return verdicts;
    }
    verdicts
        .into_iter()
        .map(|mut v| {
            if v.kind.is_accepted() {
                v.kind = VerdictKind::Reject(RejectReason::AtomicPeerRejected);
            }
            v
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repo::ForcePolicy;

    fn r(s: &str) -> RefName {
        RefName::new(s).unwrap()
    }
    fn oid(n: u8) -> ObjectId {
        ObjectId::from_bytes([n; 20])
    }
    const Z: ObjectId = ObjectId::ZERO;

    fn version(id: i64, name: &str, tip: Option<ObjectId>) -> RefVersion {
        RefVersion {
            id: RefVersionId::new(id).unwrap(),
            repo: RepoId::new(1).unwrap(),
            name: r(name),
            seq: Seq::FIRST,
            parent: None,
            value: match tip {
                Some(tip) => RefValue::Live {
                    tip,
                    pack: None,
                    closure: Closure::Exact,
                },
                None => RefValue::Deleted,
            },
            deps: Deps::default(),
            created_at: UnixMicros(0),
        }
    }

    struct Case {
        policy: ForcePolicy,
        name: &'static str,
        old: ObjectId,
        new: ObjectId,
        cur: Option<Option<ObjectId>>,
        options: Vec<&'static str>,
        scoped: bool,
        /// The tip of the version before `cur` (`None` inside: deleted),
        /// when the ref has one.
        prev: Option<Option<ObjectId>>,
        /// The version before `cur` is named as its parent but cannot be
        /// read (collected, or never written).
        prev_gone: bool,
    }

    impl Case {
        fn new(name: &'static str, old: ObjectId, new: ObjectId) -> Self {
            Self {
                policy: ForcePolicy::Branches,
                name,
                old,
                new,
                cur: None,
                options: vec![],
                scoped: false,
                prev: None,
                prev_gone: false,
            }
        }
        fn prev(mut self, tip: ObjectId) -> Self {
            self.prev = Some(Some(tip));
            self
        }
        fn prev_deleted(mut self) -> Self {
            self.prev = Some(None);
            self
        }
        fn prev_gone(mut self) -> Self {
            self.prev_gone = true;
            self
        }
        fn cur(mut self, tip: ObjectId) -> Self {
            self.cur = Some(Some(tip));
            self
        }
        fn deleted(mut self) -> Self {
            self.cur = Some(None);
            self
        }
        fn opt(mut self, o: &'static str) -> Self {
            self.options.push(o);
            self
        }
        fn policy(mut self, p: ForcePolicy) -> Self {
            self.policy = p;
            self
        }
        fn scoped(mut self) -> Self {
            self.scoped = true;
            self
        }
        fn check(&self) -> CommitCheck {
            let settings = RepoSettings::builder(
                r("refs/heads/main"),
                vec![
                    crate::name::RefPattern::parse("refs/heads/*").unwrap(),
                    crate::name::RefPattern::parse("refs/tags/*").unwrap(),
                ],
            )
            .allow_force(self.policy)
            .build()
            .unwrap();
            let command = RefCommand {
                name: r(self.name),
                old: self.old,
                new: self.new,
            };
            let previous = self.prev.map(|tip| version(8, self.name, tip));
            let current = self.cur.map(|tip| RefVersion {
                parent: previous.as_ref().map(|p| p.id),
                ..version(9, self.name, tip)
            });
            let options = PushOptions::parse(self.options.iter().copied()).unwrap();
            let scoped = self.scoped.then(|| r(self.name));
            // Read the previous version only when the check asks for it, as
            // receive-pack does.
            let wanted = replay_parent(&command, current.as_ref(), &options);
            assert!(wanted.is_none() || wanted == previous.as_ref().map(|p| p.id));
            CommitCheck::start(CheckInput {
                settings: &settings,
                command: &command,
                current: current.as_ref(),
                options: &options,
                scoped: scoped.as_ref(),
                previous: previous
                    .as_ref()
                    .filter(|_| wanted.is_some() && !self.prev_gone),
            })
        }
        fn kind(&self) -> VerdictKind {
            match self.check() {
                CommitCheck::Decided(v) => v.kind,
                CommitCheck::NeedsAncestry(n) => panic!("unexpectedly needs ancestry: {n:?}"),
            }
        }
        fn with(&self, a: Ancestry) -> VerdictKind {
            match self.check() {
                CommitCheck::NeedsAncestry(n) => {
                    assert_eq!(n.descendant, self.new);
                    let answer = AncestryAnswer::new(n.ancestor, n.descendant, a);
                    n.resolve(answer).unwrap().kind
                }
                CommitCheck::Decided(v) => panic!("unexpectedly decided: {v:?}"),
            }
        }
    }

    use RejectReason as R;
    use VerdictKind as K;

    const B: &str = "refs/heads/topic";
    const T: &str = "refs/heads/main";
    const TAG: &str = "refs/tags/v1";

    #[test]
    fn deletes() {
        assert_eq!(Case::new(B, oid(1), Z).kind(), K::Reject(R::NoSuchRef));
        assert_eq!(
            Case::new(B, oid(1), Z).deleted().kind(),
            K::Reject(R::NoSuchRef)
        );
        assert_eq!(
            Case::new(T, oid(1), Z).cur(oid(1)).opt("force").kind(),
            K::Reject(R::TrunkDelete)
        );
        assert_eq!(
            Case::new(T, oid(1), Z)
                .cur(oid(1))
                .policy(ForcePolicy::All)
                .kind(),
            K::Reject(R::TrunkDelete)
        );
        assert_eq!(
            Case::new(B, oid(2), Z).cur(oid(1)).kind(),
            K::Reject(R::StaleOld)
        );
        assert_eq!(Case::new(B, oid(1), Z).cur(oid(1)).kind(), K::DeleteCas);
        assert_eq!(
            Case::new(B, Z, Z).cur(oid(1)).opt("force").kind(),
            K::DeleteUnconditional
        );
        assert_eq!(
            Case::new(B, Z, Z)
                .cur(oid(1))
                .opt("force=refs/heads/topic")
                .kind(),
            K::DeleteUnconditional
        );
        assert_eq!(
            Case::new(B, Z, Z)
                .cur(oid(1))
                .opt("force=refs/heads/other")
                .kind(),
            K::Reject(R::DeleteNeedsForce)
        );
        assert_eq!(
            Case::new(B, Z, Z)
                .cur(oid(1))
                .opt("force")
                .policy(ForcePolicy::None)
                .kind(),
            K::Reject(R::ForceNotAllowed)
        );
        assert_eq!(
            Case::new(B, Z, Z).cur(oid(1)).kind(),
            K::Reject(R::DeleteNeedsForce)
        );
    }

    /// Stock `git receive-pack` accepts a delete of a ref that does not
    /// exist, with no old value, as a no-op (issue #133). Every way the
    /// client can say "no old value" gives the same verdict, and none of
    /// them needs `-o force`: there is nothing to overwrite.
    #[test]
    fn a_delete_of_a_missing_ref_without_an_old_value_is_a_noop() {
        let absent = "lease=refs/heads/topic:";
        for case in [
            Case::new(B, Z, Z),
            Case::new(B, Z, Z).deleted(),
            Case::new(B, Z, Z).scoped(),
            Case::new(B, Z, Z).opt("force").policy(ForcePolicy::None),
            Case::new(B, Z, Z).opt(absent),
            Case::new(B, oid(1), Z).deleted().opt(absent),
            Case::new(TAG, Z, Z),
            // A trunk that was never pushed: nothing to protect.
            Case::new(T, Z, Z),
        ] {
            let CommitCheck::Decided(v) = case.check() else {
                panic!("needs ancestry");
            };
            assert_eq!(v.kind, K::Noop, "{}", case.name);
            assert!(v.new.is_zero());
            assert!(v.kind.is_accepted() && !v.kind.writes());
        }
        // The namespace rule still comes first.
        assert_eq!(
            Case::new("refs/notes/x", Z, Z).kind(),
            K::Reject(R::NotWritable)
        );
        // A lease that expects a value the ref does not have.
        let lease = "lease=refs/heads/topic:0101010101010101010101010101010101010101";
        assert_eq!(
            Case::new(B, Z, Z).opt(lease).kind(),
            K::Reject(R::LeaseMismatch)
        );
    }

    #[test]
    fn leased_delete_is_a_compare_and_swap() {
        let lease = "lease=refs/heads/topic:0101010101010101010101010101010101010101";
        assert_eq!(
            Case::new(B, Z, Z).cur(oid(1)).opt(lease).kind(),
            K::DeleteCas
        );
        assert_eq!(
            Case::new(B, Z, Z).cur(oid(2)).opt(lease).kind(),
            K::Reject(R::LeaseMismatch)
        );
    }

    #[test]
    fn creates_and_noops() {
        assert_eq!(Case::new(B, Z, oid(1)).kind(), K::Create);
        assert_eq!(Case::new(B, Z, oid(1)).deleted().kind(), K::Create);
        assert_eq!(Case::new(TAG, Z, oid(1)).kind(), K::Create);
        assert_eq!(Case::new(B, Z, oid(1)).cur(oid(1)).kind(), K::Noop);
        assert_eq!(Case::new(T, oid(1), oid(1)).cur(oid(1)).kind(), K::Noop);
        // Client saw a value the ref no longer has.
        assert_eq!(Case::new(B, oid(1), oid(2)).kind(), K::Reject(R::StaleOld));
        assert_eq!(
            Case::new(B, oid(1), oid(2)).deleted().kind(),
            K::Reject(R::StaleOld)
        );
    }

    #[test]
    fn unadvertised_update_checks_ancestry_on_the_server() {
        let c = Case::new(B, Z, oid(2)).cur(oid(1));
        let CommitCheck::NeedsAncestry(n) = c.check() else {
            panic!()
        };
        assert_eq!((n.ancestor, n.descendant), (oid(1), oid(2)));
        assert_eq!(c.with(Ancestry::Descendant), K::FastForward);
        assert_eq!(
            c.with(Ancestry::NotDescendant),
            K::Reject(R::NonFastForward)
        );
        assert_eq!(c.with(Ancestry::Undecided), K::Reject(R::AncestryUndecided));
    }

    #[test]
    fn force_needs_both_a_signal_and_the_policy() {
        let c = Case::new(B, Z, oid(2)).cur(oid(1)).opt("force");
        assert_eq!(c.with(Ancestry::NotDescendant), K::Forced);
        // `-o force` on a fast-forward is still a fast-forward.
        assert_eq!(c.with(Ancestry::Descendant), K::FastForward);
        assert_eq!(c.with(Ancestry::Undecided), K::Reject(R::AncestryUndecided));
        let none = Case::new(B, Z, oid(2))
            .cur(oid(1))
            .opt("force")
            .policy(ForcePolicy::None);
        assert_eq!(
            none.with(Ancestry::NotDescendant),
            K::Reject(R::ForceNotAllowed)
        );
        assert_eq!(none.with(Ancestry::Descendant), K::FastForward);
    }

    #[test]
    fn trunk_rules() {
        // Trunk is advertised, so a zero old-oid can't come from a stock client.
        assert_eq!(
            Case::new(T, Z, oid(2)).cur(oid(1)).kind(),
            K::Reject(R::RefExists)
        );
        let seen = Case::new(T, oid(1), oid(2)).cur(oid(1));
        assert_eq!(seen.with(Ancestry::Descendant), K::FastForward);
        // The client saw the tip and sent a non-descendant: a force, which
        // `branches` never allows on trunk.
        assert_eq!(
            seen.with(Ancestry::NotDescendant),
            K::Reject(R::ForceNotAllowed)
        );
        let all = Case::new(T, oid(1), oid(2))
            .cur(oid(1))
            .policy(ForcePolicy::All);
        assert_eq!(all.with(Ancestry::NotDescendant), K::Forced);
        // Trunk moved since the advertisement.
        assert_eq!(
            Case::new(T, oid(1), oid(3)).cur(oid(2)).kind(),
            K::Reject(R::StaleOld)
        );
    }

    #[test]
    fn scoped_url_behaves_like_an_advertised_ref() {
        assert_eq!(
            Case::new(B, Z, oid(2)).cur(oid(1)).scoped().kind(),
            K::Reject(R::RefExists)
        );
        let lease_like = Case::new(B, oid(1), oid(2)).cur(oid(1)).scoped();
        assert_eq!(lease_like.with(Ancestry::NotDescendant), K::Forced);
    }

    #[test]
    fn leases() {
        let lease = "lease=refs/heads/topic:0101010101010101010101010101010101010101";
        let ok = Case::new(B, Z, oid(2)).cur(oid(1)).opt(lease);
        assert_eq!(ok.with(Ancestry::NotDescendant), K::Forced);
        assert_eq!(
            Case::new(B, Z, oid(2)).cur(oid(3)).opt(lease).kind(),
            K::Reject(R::LeaseMismatch)
        );
        // Empty lease: must not exist.
        let absent = "lease=refs/heads/topic:";
        assert_eq!(Case::new(B, Z, oid(2)).opt(absent).kind(), K::Create);
        assert_eq!(
            Case::new(B, Z, oid(2)).cur(oid(1)).opt(absent).kind(),
            K::Reject(R::LeaseMismatch)
        );
        // A lease replaces a stale old value.
        let replaced = Case::new(B, oid(7), oid(2)).cur(oid(1)).opt(lease);
        assert_eq!(replaced.with(Ancestry::Descendant), K::FastForward);
    }

    /// Issue #132: a request resent after its first copy landed (libcurl
    /// resends a POST when a reused connection dies before any answer) is
    /// `ok` when the ref's last move matches it and its own rules accept
    /// that move.
    #[test]
    fn an_exact_replay_is_a_noop() {
        // Trunk and a scoped branch: the client sent the tip it saw. The
        // move is checked as the command would have been, so it asks
        // whether the previous tip is an ancestor of the new one.
        for (name, scoped) in [(T, false), (B, true), (B, false)] {
            let case = |c: Case| if scoped { c.scoped() } else { c };
            let replay = case(Case::new(name, oid(1), oid(2)).cur(oid(2)).prev(oid(1)));
            match replay.check() {
                CommitCheck::NeedsAncestry(n) => {
                    assert_eq!((n.ancestor, n.descendant), (oid(1), oid(2)), "{name}");
                    assert_eq!(n.against, Some(RefVersionId::new(9).unwrap()), "{name}");
                }
                CommitCheck::Decided(v) => panic!("{name}: decided {v:?}"),
            }
            assert_eq!(replay.with(Ancestry::Descendant), K::Noop, "{name}");
            // The ref moved on after the first copy: not a replay.
            assert_eq!(
                case(Case::new(name, oid(1), oid(2)).cur(oid(3)).prev(oid(2))).kind(),
                K::Reject(R::StaleOld),
                "{name}"
            );
            // Another update to the same value from another tip.
            assert_eq!(
                case(Case::new(name, oid(1), oid(2)).cur(oid(2)).prev(oid(3))).kind(),
                K::Reject(R::StaleOld),
                "{name}"
            );
            // The version before was a deletion.
            assert_eq!(
                case(Case::new(name, oid(1), oid(2)).cur(oid(2)).prev_deleted()).kind(),
                K::Reject(R::StaleOld),
                "{name}"
            );
            // No version before: the ref was created at the new value.
            assert_eq!(
                case(Case::new(name, oid(1), oid(2)).cur(oid(2))).kind(),
                K::Reject(R::StaleOld),
                "{name}"
            );
            // The version before is named but cannot be read (collected).
            assert_eq!(
                case(Case::new(name, oid(1), oid(2)).cur(oid(2)).prev(oid(1)))
                    .prev_gone()
                    .kind(),
                K::Reject(R::StaleOld),
                "{name}"
            );
        }
        // A forced update with a lease, replayed.
        let lease = "lease=refs/heads/topic:0101010101010101010101010101010101010101";
        let leased = Case::new(B, Z, oid(2)).cur(oid(2)).prev(oid(1)).opt(lease);
        assert_eq!(leased.with(Ancestry::NotDescendant), K::Noop);
        assert_eq!(
            Case::new(B, Z, oid(2))
                .cur(oid(2))
                .prev(oid(3))
                .opt(lease)
                .kind(),
            K::Reject(R::LeaseMismatch)
        );
        // A compare-and-swap delete, replayed: the ref is deleted and was
        // at the old value before.
        assert_eq!(
            Case::new(B, oid(1), Z).deleted().prev(oid(1)).kind(),
            K::Noop
        );
        assert_eq!(
            Case::new(B, oid(1), Z).deleted().prev(oid(3)).kind(),
            K::Reject(R::NoSuchRef)
        );
        // A tag replaced with its tip as old value (the explicit signal for
        // tags), replayed.
        assert_eq!(
            Case::new(TAG, oid(1), oid(2))
                .cur(oid(2))
                .prev(oid(1))
                .kind(),
            K::Noop
        );
        // Namespaces still come first.
        assert_eq!(
            Case::new("refs/x/y", oid(1), oid(2))
                .cur(oid(2))
                .prev(oid(1))
                .kind(),
            K::Reject(R::NotWritable)
        );
    }

    /// The coordinator's review of #132's first fix: a matching last move
    /// is not enough. A request whose own rules refuse the move (here a
    /// non-fast-forward the force policy forbids) is refused even when
    /// someone else already made the same move, as it would have been had
    /// it arrived first.
    #[test]
    fn a_replay_is_ok_only_if_its_own_rules_accept_the_move() {
        // Trunk, X -> Y not a fast-forward: trunk may not be forced under
        // the default policy, with or without `-o force`.
        let trunk = || Case::new(T, oid(1), oid(2)).cur(oid(2)).prev(oid(1));
        assert_eq!(
            trunk().with(Ancestry::NotDescendant),
            K::Reject(R::StaleOld)
        );
        assert_eq!(
            trunk().opt("force").with(Ancestry::NotDescendant),
            K::Reject(R::StaleOld)
        );
        assert_eq!(
            trunk()
                .opt("force")
                .policy(ForcePolicy::All)
                .with(Ancestry::NotDescendant),
            K::Noop
        );
        // A branch where no force is allowed, and where it is.
        let branch = || Case::new(B, oid(1), oid(2)).cur(oid(2)).prev(oid(1));
        assert_eq!(
            branch()
                .policy(ForcePolicy::None)
                .with(Ancestry::NotDescendant),
            K::Reject(R::StaleOld)
        );
        assert_eq!(
            branch()
                .policy(ForcePolicy::None)
                .opt("force")
                .with(Ancestry::NotDescendant),
            K::Reject(R::StaleOld)
        );
        assert_eq!(
            branch()
                .policy(ForcePolicy::None)
                .with(Ancestry::Descendant),
            K::Noop
        );
        assert_eq!(branch().with(Ancestry::NotDescendant), K::Noop);
        assert_eq!(branch().opt("force").with(Ancestry::NotDescendant), K::Noop);
        // Ancestry that cannot be decided is never guessed.
        assert_eq!(
            branch().with(Ancestry::Undecided),
            K::Reject(R::AncestryUndecided)
        );
        // A lease replay where force is not allowed.
        let lease = "lease=refs/heads/topic:0101010101010101010101010101010101010101";
        assert_eq!(
            Case::new(B, Z, oid(2))
                .cur(oid(2))
                .prev(oid(1))
                .opt(lease)
                .policy(ForcePolicy::None)
                .with(Ancestry::NotDescendant),
            K::Reject(R::LeaseMismatch)
        );
        // A tag move where tags may not be replaced.
        assert_eq!(
            Case::new(TAG, oid(1), oid(2))
                .cur(oid(2))
                .prev(oid(1))
                .policy(ForcePolicy::None)
                .kind(),
            K::Reject(R::StaleOld)
        );
        // Trunk cannot be deleted, so a trunk delete is never a replay.
        assert_eq!(
            Case::new(T, oid(1), Z)
                .deleted()
                .prev(oid(1))
                .opt("force")
                .policy(ForcePolicy::All)
                .kind(),
            K::Reject(R::NoSuchRef)
        );
        // Creates take no part (a zero old value claims nothing): a resent
        // create still at its value is the plain no-op, without a replay.
        assert_eq!(
            Case::new(B, Z, oid(2)).cur(oid(2)).prev(oid(1)).kind(),
            K::Noop,
            "a plain no-op, not a replay"
        );
        assert_eq!(
            Case::new(TAG, Z, oid(2)).cur(oid(2)).prev(oid(1)).kind(),
            K::Noop,
            "a plain no-op, not a replay"
        );
    }

    #[test]
    fn the_previous_version_is_read_only_for_a_possible_replay() {
        let wanted = |old, new, cur: Option<ObjectId>, opts: &[&'static str]| {
            let command = RefCommand {
                name: r(B),
                old,
                new,
            };
            let current = RefVersion {
                parent: Some(RefVersionId::new(8).unwrap()),
                ..version(9, B, cur)
            };
            let options = PushOptions::parse(opts.iter().copied()).unwrap();
            replay_parent(&command, Some(&current), &options)
        };
        let parent = Some(RefVersionId::new(8).unwrap());
        assert_eq!(wanted(oid(1), oid(2), Some(oid(2)), &[]), parent);
        assert_eq!(wanted(oid(1), Z, None, &[]), parent);
        // An ordinary update, a plain no-op, an unadvertised update: no read.
        assert_eq!(wanted(oid(1), oid(2), Some(oid(1)), &[]), None);
        assert_eq!(wanted(Z, oid(2), Some(oid(2)), &[]), None);
        assert_eq!(wanted(oid(2), oid(2), Some(oid(2)), &[]), None);
        assert_eq!(wanted(oid(1), oid(2), Some(oid(3)), &[]), None);
        assert_eq!(wanted(Z, Z, None, &["force"]), None);
        // A must-not-exist lease claims no old value.
        assert_eq!(
            wanted(Z, oid(2), Some(oid(2)), &["lease=refs/heads/topic:"]),
            None
        );
    }

    #[test]
    fn tags_never_fast_forward() {
        assert_eq!(
            Case::new(TAG, Z, oid(2)).cur(oid(1)).kind(),
            K::Reject(R::TagExists)
        );
        assert_eq!(
            Case::new(TAG, Z, oid(2)).cur(oid(1)).opt("force").kind(),
            K::Forced
        );
        assert_eq!(
            Case::new(TAG, Z, oid(2))
                .cur(oid(1))
                .opt("force")
                .policy(ForcePolicy::None)
                .kind(),
            K::Reject(R::ForceNotAllowed)
        );
        assert_eq!(Case::new(TAG, Z, oid(1)).cur(oid(1)).kind(), K::Noop);
    }

    #[test]
    fn a_tag_replaced_with_its_current_value_as_old_is_explicit() {
        // git push --force-with-lease=refs/tags/v1:<oid> sends old = <oid>
        // and no push option.
        assert_eq!(Case::new(TAG, oid(1), oid(2)).cur(oid(1)).kind(), K::Forced);
        assert_eq!(
            Case::new(TAG, oid(1), oid(2))
                .cur(oid(1))
                .policy(ForcePolicy::None)
                .kind(),
            K::Reject(R::ForceNotAllowed)
        );
        // A stale old value is still stale.
        assert_eq!(
            Case::new(TAG, oid(3), oid(2)).cur(oid(1)).kind(),
            K::Reject(R::StaleOld)
        );
        // A matching lease is explicit too.
        let lease = "lease=refs/tags/v1:0101010101010101010101010101010101010101";
        assert_eq!(
            Case::new(TAG, Z, oid(2)).cur(oid(1)).opt(lease).kind(),
            K::Forced
        );
    }

    fn live(tip: ObjectId, closure: Closure) -> RefValue {
        RefValue::Live {
            tip,
            pack: None,
            closure,
        }
    }

    #[test]
    fn closures_are_exact_only_when_every_part_is() {
        use Ancestry::{Descendant as D, NotDescendant as N, Undecided as U};
        use Closure::{Exact as E, Wider as W};
        let new = oid(2);
        let dep = |closure| live(oid(1), closure);
        let ans = |a| Some(AncestryAnswer::new(oid(1), new, a));
        let of = |reach, deps: &[(RefValue, Option<AncestryAnswer>)]| {
            Closure::of_new_version(new, reach, deps.iter().map(|(v, a)| (v, *a))).unwrap()
        };
        assert_eq!(of(true, &[]), E);
        assert_eq!(of(false, &[]), W);
        // Fast-forward on an exact trunk version.
        assert_eq!(of(true, &[(dep(E), ans(D))]), E);
        // Branch created at an old trunk commit: trunk's tip is not an
        // ancestor of the branch tip.
        assert_eq!(of(true, &[(dep(E), ans(N))]), W);
        // Forced, not decided, or not asked: wider.
        assert_eq!(of(true, &[(dep(E), ans(U))]), W);
        assert_eq!(of(true, &[(dep(E), None)]), W);
        // Built on a wider version.
        assert_eq!(of(true, &[(dep(W), ans(D))]), W);
        // A deleted scoped dependency contributes nothing and needs no
        // answer.
        assert_eq!(of(true, &[(dep(E), ans(D)), (RefValue::Deleted, None)]), E);
        // A dependency at the new tip itself is trivially an ancestor.
        let same = live(new, E);
        let same_answer = AncestryAnswer::new(new, new, N);
        assert_eq!(same_answer.answer(), D);
        assert_eq!(of(true, &[(same, Some(same_answer))]), E);
    }

    #[test]
    fn ancestry_answers_are_tied_to_their_pair() {
        let dep = live(oid(1), Closure::Exact);
        // "Is the new tip an ancestor of the dependency's tip?" is the
        // wrong question; its answer must not be accepted.
        let reversed = AncestryAnswer::new(oid(2), oid(1), Ancestry::Descendant);
        assert_eq!(
            Closure::of_new_version(oid(2), true, [(&dep, Some(reversed))]),
            Err(AncestryMismatch {
                asked_ancestor: oid(1),
                asked_descendant: oid(2),
                answered_ancestor: oid(2),
                answered_descendant: oid(1),
            })
        );
        // An answer about an unrelated pair is refused too.
        let other = AncestryAnswer::new(oid(1), oid(3), Ancestry::Descendant);
        assert!(Closure::of_new_version(oid(2), true, [(&dep, Some(other))]).is_err());
        // The commit check refuses a reversed answer as well.
        let CommitCheck::NeedsAncestry(n) = Case::new(B, Z, oid(2)).cur(oid(1)).check() else {
            panic!()
        };
        assert!(
            n.resolve(AncestryAnswer::new(oid(2), oid(1), Ancestry::Descendant))
                .is_err()
        );
    }

    /// `git push main --follow-tags` (or `git push main feature`): one
    /// received pack holds objects for two writes. Objects are small
    /// integers; `graph` maps each object to the objects it references.
    #[test]
    fn exactness_is_per_write_and_a_shared_pack_makes_trunk_wider() {
        // m1 is trunk's old tip (in the dependency). The push adds commit m2
        // (tree 20, blob 21, parent m1) for main and annotated tag 30 -> m2.
        let graph: BTreeMap<u8, Vec<u8>> = [
            (1, vec![10]),
            (10, vec![]),
            (2, vec![20, 1]),
            (20, vec![21]),
            (21, vec![]),
            (30, vec![2]),
        ]
        .into();
        let reach = |tip: u8| {
            let mut seen = BTreeSet::new();
            let mut todo = vec![tip];
            while let Some(o) = todo.pop() {
                if seen.insert(o) {
                    todo.extend(&graph[&o]);
                }
            }
            seen
        };
        let pack_reachable = |pack: &[u8], tip: u8| pack.iter().all(|o| reach(tip).contains(o));
        let trunk_old = live(oid(1), Closure::Exact);
        let closure_of = |pack: &[u8], tip: u8| {
            let answer = AncestryAnswer::new(oid(1), oid(tip), Ancestry::Descendant);
            Closure::of_new_version(
                oid(tip),
                pack_reachable(pack, tip),
                [(&trunk_old, Some(answer))],
            )
            .unwrap()
        };

        // One shared pack, judged per write: main's write holds the tag
        // object it does not reach, so it is wider; the tag's write reaches
        // everything in the pack and is exact.
        let shared = [2, 20, 21, 30];
        let main_v = closure_of(&shared, 2);
        assert_eq!(main_v, Closure::Wider);
        assert_eq!(closure_of(&shared, 30), Closure::Exact);
        // Judged over the union of the tips, main would be exact, which is
        // unsafe: a have of m2 would then subtract the tag object.
        let union_reach: BTreeSet<u8> = reach(2).union(&reach(30)).copied().collect();
        assert!(shared.iter().all(|o| union_reach.contains(o)));
        assert!(!reach(2).contains(&30));

        // Wider is sticky: the next trunk version depends on this one.
        let main_after = live(oid(2), main_v);
        let next = Closure::of_new_version(
            oid(4),
            true,
            [(
                &main_after,
                Some(AncestryAnswer::new(oid(2), oid(4), Ancestry::Descendant)),
            )],
        )
        .unwrap();
        assert_eq!(next, Closure::Wider);

        // B1's remedy: one pack per write, holding what that write's tip
        // reaches minus its dependencies' closure. Both writes are exact;
        // m2's objects are in both packs and the stitcher keeps the first.
        let minus_dep =
            |tip: u8| -> Vec<u8> { reach(tip).difference(&reach(1)).copied().collect() };
        assert_eq!(minus_dep(2), vec![2, 20, 21]);
        assert_eq!(minus_dep(30), vec![2, 20, 21, 30]);
        assert_eq!(closure_of(&minus_dep(2), 2), Closure::Exact);
        assert_eq!(closure_of(&minus_dep(30), 30), Closure::Exact);
    }

    #[test]
    fn namespaces_are_enforced_first() {
        assert_eq!(
            Case::new("refs/remotes/origin/main", Z, oid(1)).kind(),
            K::Reject(R::NotWritable)
        );
    }

    #[test]
    fn verdict_records_the_exact_version() {
        let CommitCheck::Decided(v) = Case::new(B, Z, oid(1)).deleted().check() else {
            panic!()
        };
        assert_eq!(v.against, Some(RefVersionId::new(9).unwrap()));
        let CommitCheck::Decided(v) = Case::new(B, Z, oid(1)).check() else {
            panic!()
        };
        assert_eq!(v.against, None);
        let CommitCheck::NeedsAncestry(n) = Case::new(B, Z, oid(2)).cur(oid(1)).check() else {
            panic!()
        };
        let answer = AncestryAnswer::new(oid(1), oid(2), Ancestry::Descendant);
        assert_eq!(
            n.resolve(answer).unwrap().against,
            Some(RefVersionId::new(9).unwrap())
        );
    }

    #[test]
    fn atomic_rejects_everything_if_anything_is_rejected() {
        let v = |kind| Verdict {
            name: r(B),
            against: None,
            new: oid(1),
            kind,
        };
        let all_ok = apply_atomic(vec![v(K::Create), v(K::Noop)]);
        assert!(all_ok.iter().all(|v| v.kind.is_accepted()));
        let mixed = apply_atomic(vec![v(K::Create), v(K::Reject(R::StaleOld))]);
        assert_eq!(mixed[0].kind, K::Reject(R::AtomicPeerRejected));
        assert_eq!(mixed[1].kind, K::Reject(R::StaleOld));
    }

    #[test]
    fn verdict_kinds_report_writes() {
        assert!(K::Create.writes() && K::DeleteCas.writes() && K::Forced.writes());
        assert!(K::Noop.is_accepted() && !K::Noop.writes());
        assert!(!K::Reject(R::StaleOld).writes());
    }

    #[test]
    fn push_option_parsing() {
        let o = PushOptions::parse([
            "ci.skip",
            "force=refs/heads/a",
            "lease=refs/heads/b:0101010101010101010101010101010101010101",
            "lease=refs/heads/c:",
        ])
        .unwrap();
        assert!(o.force_requested(&r("refs/heads/a")));
        assert!(!o.force_requested(&r("refs/heads/b")));
        assert_eq!(o.lease(&r("refs/heads/b")), Some(Some(oid(1))));
        assert_eq!(o.lease(&r("refs/heads/c")), Some(None));
        assert_eq!(o.lease(&r("refs/heads/a")), None);
        assert!(
            PushOptions::parse(["force"])
                .unwrap()
                .force_requested(&r(B))
        );

        assert!(matches!(
            PushOptions::parse(["force=main"]),
            Err(PushOptionError::BadRef { .. })
        ));
        assert!(matches!(
            PushOptions::parse(["lease=refs/heads/a"]),
            Err(PushOptionError::BadLease(_))
        ));
        assert!(matches!(
            PushOptions::parse(["lease=refs/heads/a:xyz"]),
            Err(PushOptionError::BadLease(_))
        ));
        assert_eq!(
            PushOptions::parse([
                "lease=refs/heads/a:",
                "lease=refs/heads/a:0101010101010101010101010101010101010101"
            ]),
            Err(PushOptionError::ConflictingLease(r("refs/heads/a")))
        );
        // The same lease twice is fine.
        assert!(PushOptions::parse(["lease=refs/heads/a:", "lease=refs/heads/a:"]).is_ok());
    }

    #[test]
    fn deps_iterate_trunk_first() {
        let d = Deps {
            trunk: Some(RefVersionId::new(3).unwrap()),
            scoped_old: Some(RefVersionId::new(5).unwrap()),
        };
        assert_eq!(d.iter().map(|v| v.get()).collect::<Vec<_>>(), vec![3, 5]);
        assert_eq!(Deps::default().iter().count(), 0);
    }
}
