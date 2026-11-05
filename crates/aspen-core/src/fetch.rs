//! Fetch contracts: the parsed request, the planner's inputs and outputs,
//! and the response pieces that depend on spike findings (S3, S5).

use std::num::NonZeroU32;

use crate::error::FetchError;
use crate::id::{ObjectId, PackChecksum, PackId, RefVersionId};
use crate::meta::Cover;
use crate::name::RefName;
use crate::pack::PackRecord;
use crate::refs::{Ancestry, AncestryAnswer, AncestryMismatch, Closure, RefVersion};
use crate::repo::ShallowPolicy;

/// How a shallow fetch bounds history.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Deepen {
    /// `deepen <n>`: `n` commits deep.
    Depth(NonZeroU32),
    /// `deepen-since` and/or `deepen-not`, which Git allows together.
    Boundary {
        /// `deepen-since <time>`: commits newer than this, in seconds since
        /// the epoch.
        since: Option<i64>,
        /// `deepen-not <rev>`: exclude history reachable from these, as the
        /// client sent them; the fetch service resolves them to refs.
        not: Vec<String>,
    },
}

/// A fetch request, parsed from protocol v2 (or v0) into fields (S5).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct FetchRequest {
    /// Objects the client wants. They may name non-commit objects, and
    /// resolving them can fail.
    pub wants: Vec<ObjectId>,
    /// Objects the client has.
    pub haves: Vec<ObjectId>,
    /// The client's current shallow boundary (`shallow` lines).
    pub client_shallow: Vec<ObjectId>,
    /// `deepen`, `deepen-since` or `deepen-not`.
    pub deepen: Option<Deepen>,
    /// `deepen-relative`.
    pub deepen_relative: bool,
    /// A `filter` line. Never advertised, so always refused; parsed so the
    /// refusal is clear.
    pub filter: Option<String>,
    /// `done`: no more negotiation rounds.
    pub done: bool,
    /// `thin-pack`. Aspen never needs it: stored packs are self-contained.
    pub thin_pack: bool,
    /// `ofs-delta`.
    pub ofs_delta: bool,
    /// `packfile-uris <protocols>`: URI schemes the client accepts, for
    /// example `https`. Empty when the client did not opt in.
    pub packfile_uri_protocols: Vec<String>,
    /// The client's `agent=` capability.
    pub agent: Option<String>,
}

/// Whether the planner may use the client's haves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanMode {
    /// An ordinary fetch: plan the wants minus what the haves cover.
    Incremental,
    /// A shallow fetch: plan the full closure of the wants and ignore the
    /// haves. A have-based plan for a shallow client drops objects below its
    /// boundary and fails the fetch (S5).
    FullClosure,
}

impl FetchRequest {
    /// Whether this is a shallow request: `deepen*` lines, or a client that
    /// is already shallow.
    pub fn is_shallow(&self) -> bool {
        self.deepen.is_some() || !self.client_shallow.is_empty() || self.deepen_relative
    }

    /// Checks what Aspen refuses outright, and picks the planning mode.
    pub fn plan_mode(&self, shallow: ShallowPolicy) -> Result<PlanMode, FetchError> {
        if self.filter.is_some() {
            return Err(FetchError::filter());
        }
        if self.is_shallow() {
            return match shallow {
                ShallowPolicy::Serve => Ok(PlanMode::FullClosure),
                ShallowPolicy::Refuse => Err(FetchError::shallow_refused()),
            };
        }
        Ok(PlanMode::Incremental)
    }
}

/// A client have, resolved to a ref version whose whole closure the client
/// is guaranteed to hold, so the planner may subtract that closure.
///
/// Only for a complete client: a shallow client holds nothing below its
/// boundary, so even an exact tip says nothing about what it holds. Haves
/// of a shallow request are never planned with ([`PlanMode::FullClosure`]).
///
/// A client that has commit `h` is guaranteed only the objects `h` reaches.
/// Subtracting the closure of a version `V` is therefore safe only when
/// `closure(V)` is within what `h` reaches, which holds when `V`'s closure is
/// [`Closure::Exact`] (exactly what `V.tip` reaches) **and** `V.tip` is `h`
/// or an ancestor of `h`. The two constructors are the only ways to build
/// one, and they check both.
///
/// Resolving a have through `commit_index` alone is unsafe: a row maps a
/// commit to *a version whose tip reaches it*, usually a descendant, and
/// subtracting that version's closure drops objects of every commit between
/// `h` and the version's tip. Such a row may resolve a want, never a have.
/// A have that resolves to no `HaveVersion` is dropped; that only costs
/// bytes. (Property obligation for A5: haves that are non-tip commits, and
/// tips of [`Closure::Wider`] versions, must never lose objects.)
///
/// **Covers on the have side.** Covers follow a ref's version history;
/// closures follow dependencies. A have's version `V` lets the planner
/// treat `closure(V)` as held, and through dependencies every version `V`
/// reaches. A cover's range may be treated as held for a held version `V`
/// only when the cover carries a [`crate::meta::CoverChain`] and `V`'s
/// `seq` is inside the range: then every version of the range from its
/// first `seq` up to `V`'s is held, because each one after the first
/// depends on its parent (closure containment). The same holds for any
/// version the planner has proven held, not only a have's own version.
/// The chain needs neither exactness nor ancestry, only dependency edges;
/// node C1 records it with [`crate::meta::VerifiedRange`]. An unchained
/// cover is never treated as held: a plain-URL branch push depends only on
/// trunk, so a forced branch update can be exact while a cover over its old
/// and new versions holds the old, unrelated history, and such a range does
/// not verify.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct HaveVersion(RefVersionId);

impl HaveVersion {
    /// `have` is exactly the tip of `version`, whose closure is exact.
    pub fn exact_tip(have: ObjectId, version: &RefVersion) -> Option<Self> {
        (version.value.tip() == Some(have) && version.value.closure() == Some(Closure::Exact))
            .then_some(Self(version.id))
    }

    /// `version`'s tip is an ancestor of `have`, and `version`'s closure is
    /// exact. `answer` must answer "is `version.tip` an ancestor of
    /// `have`?"; only [`Ancestry::Descendant`] counts.
    ///
    /// The reversed question ("is `have` an ancestor of `version.tip`?",
    /// which a `commit_index` lookup of the have answers for free) is the
    /// unsafe direction, so an answer about any other pair is an
    /// [`AncestryMismatch`]. A deleted version is `Ok(None)`.
    pub fn ancestor_of_have(
        have: ObjectId,
        version: &RefVersion,
        answer: AncestryAnswer,
    ) -> Result<Option<Self>, AncestryMismatch> {
        let Some(tip) = version.value.tip() else {
            return Ok(None);
        };
        let ancestry = answer.answers(tip, have)?;
        Ok(
            (ancestry == Ancestry::Descendant && version.value.closure() == Some(Closure::Exact))
                .then_some(Self(version.id)),
        )
    }

    /// The version whose closure the client holds.
    pub fn version(self) -> RefVersionId {
        self.0
    }
}

/// The planner's input: wants resolved to ref versions whose closure holds
/// them (any `commit_index` row will do), and haves as [`HaveVersion`]s.
///
/// Build it with the mode [`FetchRequest::plan_mode`] returns. A request
/// carrying `shallow` lines or `deepen*` gets [`PlanMode::FullClosure`], so
/// its input has no haves and its plan subtracts nothing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanInput {
    wants: Vec<RefVersionId>,
    haves: Vec<HaveVersion>,
}

impl PlanInput {
    /// Builds the input. In [`PlanMode::FullClosure`] the haves are
    /// discarded here, so no planner can use them by mistake.
    pub fn new(mode: PlanMode, wants: Vec<RefVersionId>, haves: Vec<HaveVersion>) -> Self {
        let haves = match mode {
            PlanMode::Incremental => haves,
            PlanMode::FullClosure => Vec::new(),
        };
        Self { wants, haves }
    }

    /// Versions to send.
    pub fn wants(&self) -> &[RefVersionId] {
        &self.wants
    }

    /// Versions whose closure the client holds; empty for a full-closure
    /// plan.
    pub fn haves(&self) -> &[HaveVersion] {
        &self.haves
    }
}

/// The in-memory view of version history the planner reads. The fetch
/// service loads it from the metadata store; the planner does no I/O.
pub trait VersionGraph {
    /// A version by ID.
    fn version(&self, id: RefVersionId) -> Option<&RefVersion>;
    /// Covers of one ref's version chain, ordered by level then `first_seq`.
    fn covers(&self, name: &RefName) -> &[Cover];
    /// A pack row by ID.
    fn pack(&self, id: PackId) -> Option<&PackRecord>;
}

/// Why planning failed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PlanError {
    /// No wants.
    #[error("nothing wanted")]
    NoWants,
    /// The graph lacks a version the plan needs: the loaded view is
    /// incomplete. The fetch fails with a retryable error.
    #[error("ref version {0} is not in the loaded graph")]
    MissingVersion(RefVersionId),
    /// The graph lacks a pack row the plan needs.
    #[error("pack {0} is not in the loaded graph")]
    MissingPack(PackId),
}

/// The packs to send, in send order.
///
/// Every pack is self-contained and base-first, so any order stitches in
/// one pass with keep-first deduplication, and each pack can also be sent
/// alone as a packfile URI.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct FetchPlan {
    /// The packs.
    pub packs: Vec<PackId>,
}

/// Pure fetch planning (`aspen-planner`): selects stored packs whose union
/// contains every object reachable from the wants and not from the haves.
/// The same call with no haves gives the search set of a push's base
/// lookup. Receive-pack computes that set without walking the history
/// (issue #149: remembered closures and aligned cover blocks), and its
/// tests check it against this call.
///
/// **Cost.** Compacted covers bound the number of packs sent, and cover
/// chains bound the versions visited on the have side: the walk visits the
/// versions between the wants and the haves, but from a held version inside
/// a chained cover it jumps to the range's first version (the rule on
/// [`HaveVersion`]). With the default [`crate::meta::CoverLayout`], a
/// client current on trunk that fetches a branch created N trunk pushes ago
/// visits a few dozen versions instead of about N. Where a ref's history
/// has no chain (a version that does not depend on its parent, or covers
/// not yet chained), the walk steps version by version there. A full clone
/// visits every version in the wants' closure (`docs/PLAN.md`, fetch path).
///
/// **Complete clients only.** The property below, and [`HaveVersion`]'s
/// premise that a client with commit `h` holds everything `h` reaches,
/// hold only for a complete client. A shallow client lacks the history
/// below its boundary, so subtracting what its haves reach can drop
/// objects it needs. For every shallow request
/// ([`FetchRequest::is_shallow`]: `shallow` lines or `deepen*`) the fetch
/// service plans in [`PlanMode::FullClosure`], where [`PlanInput::new`]
/// drops the haves. The pack is then the full closure of the wants; the
/// deepen boundary bounds only `shallow-info` (S5; exact shallow packs are
/// node C8).
///
/// Property (A5): the selected packs contain every object of
/// `git rev-list --objects <wants>` that is not in
/// `git rev-list --objects <haves>`, including when haves are
/// non-tip commits or tips of [`Closure::Wider`] versions (which the fetch
/// service must drop or resolve elsewhere, see [`HaveVersion`]). A cover's
/// range is treated as held only under [`HaveVersion`]'s rule for covers. (`git rev-list --objects <wants> ^<haves>` is not the oracle: it
/// marks only the boundary commits' trees uninteresting, so it also lists
/// objects the haves reach through older commits, which the client has.)
pub trait Planner {
    /// Plans one fetch.
    fn plan(&self, graph: &dyn VersionGraph, input: &PlanInput) -> Result<FetchPlan, PlanError>;
}

/// `shallow-info`, sent before `packfile-uris` and `packfile`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ShallowInfo {
    /// `shallow <oid>` lines: the new boundary.
    pub shallow: Vec<ObjectId>,
    /// `unshallow <oid>` lines: commits that are no longer boundary.
    pub unshallow: Vec<ObjectId>,
}

/// One `packfile-uris` line: the pack's trailing SHA-1 and a URL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackfileUri {
    /// `packs.checksum`; the client checks the downloaded pack against it.
    pub hash: PackChecksum,
    /// A direct, self-authorizing URL that honours `Range`. Its scheme is
    /// one the client listed.
    pub uri: String,
}

/// The `(major, minor)` Git version in an `agent=` capability such as
/// `git/2.56.0` or `git/2.39.5.(Apple Git-154)`; `None` if it doesn't parse
/// as stock Git. Unparseable agents count as old.
pub fn git_agent_version(agent: &str) -> Option<(u32, u32)> {
    let rest = agent.strip_prefix("git/")?;
    let mut parts = rest.split(|c: char| !c.is_ascii_digit());
    let major = parts.next()?.parse().ok()?;
    let minor = parts.next()?.parse().ok()?;
    Some((major, minor))
}

/// Whether a client may get direct-to-bucket packfile URIs, whose bucket
/// answers `416` past the end: only `git/2.56` and later (S3, finding 14).
/// Self-reported, so a reliability rule, not a security control.
pub fn allows_direct_bucket_uris(agent: Option<&str>) -> bool {
    agent
        .and_then(git_agent_version)
        .is_some_and(|v| v >= (2, 56))
}

/// Whether `no-ref-delta` may be advertised for a plan: only when no planned
/// pack has a REF_DELTA entry (S3).
pub fn no_ref_delta_safe<'a>(packs: impl IntoIterator<Item = &'a PackRecord>) -> bool {
    packs.into_iter().all(|p| p.pack.ref_delta_count == 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn oid(n: u8) -> ObjectId {
        ObjectId::from_bytes([n; 20])
    }
    fn v(n: i64) -> RefVersionId {
        RefVersionId::new(n).unwrap()
    }
    fn version(id: i64, tip: Option<ObjectId>, closure: Closure) -> RefVersion {
        use crate::id::{RepoId, Seq, UnixMicros};
        use crate::refs::{Deps, RefValue};
        RefVersion {
            id: v(id),
            repo: RepoId::new(1).unwrap(),
            name: RefName::new("refs/heads/main").unwrap(),
            seq: Seq::FIRST,
            parent: None,
            value: match tip {
                Some(tip) => RefValue::Live {
                    tip,
                    pack: None,
                    closure,
                },
                None => RefValue::Deleted,
            },
            deps: Deps::default(),
            created_at: UnixMicros(0),
        }
    }

    #[test]
    fn a_have_stands_for_a_version_only_when_the_client_holds_its_closure() {
        let exact = version(2, Some(oid(7)), Closure::Exact);
        let wider = version(3, Some(oid(7)), Closure::Wider);
        let deleted = version(4, None, Closure::Exact);
        // Exact tip of an exact version.
        assert_eq!(
            HaveVersion::exact_tip(oid(7), &exact).map(HaveVersion::version),
            Some(v(2))
        );
        // A non-tip commit is not the tip: commit_index would map it to a
        // descendant version, whose closure the client does not hold.
        assert_eq!(HaveVersion::exact_tip(oid(6), &exact), None);
        // A wider closure holds objects the tip does not reach.
        assert_eq!(HaveVersion::exact_tip(oid(7), &wider), None);
        assert_eq!(HaveVersion::exact_tip(oid(7), &deleted), None);
        // The version's tip (7) is below the have (8).
        let below = |a| AncestryAnswer::new(oid(7), oid(8), a);
        assert_eq!(
            HaveVersion::ancestor_of_have(oid(8), &exact, below(Ancestry::Descendant))
                .unwrap()
                .map(HaveVersion::version),
            Some(v(2))
        );
        for a in [Ancestry::NotDescendant, Ancestry::Undecided] {
            assert_eq!(
                HaveVersion::ancestor_of_have(oid(8), &exact, below(a)),
                Ok(None)
            );
        }
        assert_eq!(
            HaveVersion::ancestor_of_have(oid(8), &wider, below(Ancestry::Descendant)),
            Ok(None)
        );
        assert_eq!(
            HaveVersion::ancestor_of_have(oid(8), &deleted, below(Ancestry::Descendant)),
            Ok(None)
        );
    }

    #[test]
    fn a_have_refuses_the_reversed_ancestry_question() {
        let exact = version(2, Some(oid(7)), Closure::Exact);
        // The have (6) is an ancestor of the version's tip (7): what a
        // commit_index row of the have says. Subtracting the version's
        // closure would drop the objects between 6 and 7, so the answer to
        // this question must be refused, not read as "Descendant".
        let reversed = AncestryAnswer::new(oid(6), oid(7), Ancestry::Descendant);
        assert!(matches!(
            HaveVersion::ancestor_of_have(oid(6), &exact, reversed),
            Err(AncestryMismatch { .. })
        ));
        // An answer for a different have is refused too.
        let other = AncestryAnswer::new(oid(7), oid(9), Ancestry::Descendant);
        assert!(HaveVersion::ancestor_of_have(oid(8), &exact, other).is_err());
    }

    #[test]
    fn filter_is_always_refused() {
        let r = FetchRequest {
            filter: Some("blob:none".into()),
            ..Default::default()
        };
        assert_eq!(r.plan_mode(ShallowPolicy::Serve), Err(FetchError::filter()));
    }

    #[test]
    fn shallow_requests_plan_the_full_closure() {
        let depth = FetchRequest {
            deepen: Some(Deepen::Depth(NonZeroU32::new(1).unwrap())),
            ..Default::default()
        };
        let already_shallow = FetchRequest {
            client_shallow: vec![oid(1)],
            ..Default::default()
        };
        let since = FetchRequest {
            deepen: Some(Deepen::Boundary {
                since: Some(1_700_000_000),
                not: vec!["refs/heads/old".into()],
            }),
            ..Default::default()
        };
        for r in [depth, already_shallow, since] {
            assert!(r.is_shallow());
            assert_eq!(r.plan_mode(ShallowPolicy::Serve), Ok(PlanMode::FullClosure));
            assert_eq!(
                r.plan_mode(ShallowPolicy::Refuse),
                Err(FetchError::shallow_refused())
            );
        }
        let normal = FetchRequest::default();
        assert_eq!(
            normal.plan_mode(ShallowPolicy::Refuse),
            Ok(PlanMode::Incremental)
        );
    }

    #[test]
    fn full_closure_input_drops_haves() {
        let have =
            HaveVersion::exact_tip(oid(7), &version(2, Some(oid(7)), Closure::Exact)).unwrap();
        let i = PlanInput::new(PlanMode::FullClosure, vec![v(1)], vec![have]);
        assert!(i.haves().is_empty());
        assert_eq!(i.wants(), &[v(1)]);
        let i = PlanInput::new(PlanMode::Incremental, vec![v(1)], vec![have]);
        assert_eq!(i.haves(), &[have]);
    }

    #[test]
    fn agent_versions() {
        assert_eq!(git_agent_version("git/2.56.0"), Some((2, 56)));
        assert_eq!(
            git_agent_version("git/2.39.5.(Apple.Git-154)"),
            Some((2, 39))
        );
        assert_eq!(git_agent_version("git/2.55.0.921.gabcdef"), Some((2, 55)));
        assert_eq!(git_agent_version("git/2.47.1.windows.2"), Some((2, 47)));
        assert_eq!(git_agent_version("JGit/6.10"), None);
        assert_eq!(git_agent_version("git/"), None);
        assert_eq!(git_agent_version("git/3"), None);
    }

    #[test]
    fn direct_bucket_uris_need_git_2_56() {
        assert!(allows_direct_bucket_uris(Some("git/2.56.0")));
        assert!(allows_direct_bucket_uris(Some("git/3.0.0")));
        // The OpenAI fork reports a 2.55 build and is treated as 2.55.
        assert!(!allows_direct_bucket_uris(Some("git/2.55.0.921.g0123")));
        assert!(!allows_direct_bucket_uris(Some("libgit2 1.9")));
        assert!(!allows_direct_bucket_uris(None));
    }
}
