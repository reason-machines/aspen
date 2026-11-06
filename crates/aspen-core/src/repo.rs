//! Repositories and their settings (`repos` table).

use crate::id::RepoId;
use crate::name::{NameError, RefName, RefPattern, RepoName};

/// Default number of earlier trunk tips advertised as `.have` lines (S2).
pub const DEFAULT_HAVE_TIPS: u8 = 8;

/// Most earlier trunk tips a repository may advertise as `.have` lines.
pub const MAX_HAVE_TIPS: u8 = 32;

/// Which refs a push may rewrite (non-fast-forward update, replace a tag, or
/// delete without a known old value).
///
/// The policy only says whether a force is *possible*. A force still needs
/// an explicit signal from the client (`-o force`, `-o force=<ref>` or a
/// matching lease), because `git push --force` is invisible to the server
/// for unadvertised refs (S2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum ForcePolicy {
    /// No ref may be forced.
    None,
    /// Every writable ref except trunk may be forced. The default.
    #[default]
    Branches,
    /// Every writable ref, trunk included, may be forced.
    ///
    /// A forced trunk update keeps trunk [`crate::refs::Closure::Exact`]
    /// under the forced-trunk rule the receive-pack service (B1)
    /// implements: the new version depends on the newest recent trunk
    /// version whose tip is an ancestor of the new tip (none for orphan
    /// history), and its pack also holds the objects the new tip reaches
    /// that only the skipped versions' closures had (`docs/PLAN.md`,
    /// decisions log). Without the rule, one forced update would make every
    /// later trunk version wider and every fetch a full clone.
    All,
}

impl ForcePolicy {
    /// Every value, in the order of schema v1's `CHECK` list.
    pub const ALL: [ForcePolicy; 3] = [Self::None, Self::Branches, Self::All];

    /// Whether this policy lets `name` be forced in a repository whose trunk
    /// is `trunk`.
    pub fn permits(self, name: &RefName, trunk: &RefName) -> bool {
        match self {
            Self::None => false,
            Self::Branches => name != trunk,
            Self::All => true,
        }
    }

    /// The value stored in `repos.allow_force`.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Branches => "branches",
            Self::All => "all",
        }
    }
}

/// How a repository answers shallow fetches (S5).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum ShallowPolicy {
    /// Serve them by sending the full closure of the wants plus an exact
    /// `shallow-info`. The default.
    #[default]
    Serve,
    /// Answer with a clear `ERR`.
    Refuse,
}

impl ShallowPolicy {
    /// Every value, in the order of schema v1's `CHECK` list.
    pub const ALL: [ShallowPolicy; 2] = [Self::Serve, Self::Refuse];

    /// The value stored in `repos.shallow`.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Serve => "serve",
            Self::Refuse => "refuse",
        }
    }
}

/// An unknown value in a stored enum column.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("unknown {column} value {value:?}")]
pub struct UnknownValue {
    /// Column the value came from.
    pub column: &'static str,
    /// The value.
    pub value: String,
}

macro_rules! from_str_via_all {
    ($ty:ty, $column:literal) => {
        impl ::std::str::FromStr for $ty {
            type Err = $crate::repo::UnknownValue;
            fn from_str(s: &str) -> Result<Self, $crate::repo::UnknownValue> {
                <$ty>::ALL
                    .into_iter()
                    .find(|v| v.as_str() == s)
                    .ok_or_else(|| $crate::repo::UnknownValue {
                        column: $column,
                        value: s.to_owned(),
                    })
            }
        }
        impl ::std::fmt::Display for $ty {
            fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                f.write_str(self.as_str())
            }
        }
    };
}
pub(crate) use from_str_via_all;

from_str_via_all!(ForcePolicy, "repos.allow_force");
from_str_via_all!(ShallowPolicy, "repos.shallow");

/// Why repository settings are invalid.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SettingsError {
    /// Trunk must be a branch.
    #[error("trunk {0} is not under refs/heads/")]
    TrunkNotABranch(RefName),
    /// Trunk must match a writable namespace, or nobody could push to it.
    #[error("trunk {0} is not in any writable namespace")]
    TrunkNotWritable(RefName),
    /// At least one writable namespace is required.
    #[error("no writable ref namespace")]
    NoWritableNamespace,
    /// Too many `.have` tips.
    #[error("have_tips {0} is above the limit of {MAX_HAVE_TIPS}")]
    TooManyHaveTips(u8),
    /// A stored namespace list does not parse.
    #[error("bad writable namespace: {0}")]
    BadNamespace(#[from] NameError),
}

/// Per-repository settings (`repos` columns besides `id` and `name`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepoSettings {
    trunk: RefName,
    allow_force: ForcePolicy,
    writable: Vec<RefPattern>,
    no_thin: bool,
    have_tips: u8,
    shallow: ShallowPolicy,
}

impl RepoSettings {
    /// Defaults from `docs/PLAN.md`: force policy `branches`, writable
    /// `refs/heads/*` and `refs/tags/*`, thin packs accepted, 8 `.have` tips,
    /// shallow served.
    pub fn new(trunk: RefName) -> Result<Self, SettingsError> {
        let writable = vec![
            RefPattern::parse("refs/heads/*").expect("valid pattern"),
            RefPattern::parse("refs/tags/*").expect("valid pattern"),
        ];
        Self::builder(trunk, writable)
            .allow_force(ForcePolicy::default())
            .build()
    }

    /// Starts settings with explicit values; see [`RepoSettingsBuilder`].
    pub fn builder(trunk: RefName, writable: Vec<RefPattern>) -> RepoSettingsBuilder {
        RepoSettingsBuilder(Self {
            trunk,
            allow_force: ForcePolicy::default(),
            writable,
            no_thin: false,
            have_tips: DEFAULT_HAVE_TIPS,
            shallow: ShallowPolicy::default(),
        })
    }

    /// The trunk ref: the only named ref a push advertisement shows.
    pub fn trunk(&self) -> &RefName {
        &self.trunk
    }

    /// Which refs may be forced. See [`ForcePolicy::All`] for how a forced
    /// trunk update stays exact.
    pub fn allow_force(&self) -> ForcePolicy {
        self.allow_force
    }

    /// The same settings with force policy `policy` (any policy is valid
    /// with any other setting).
    pub fn with_allow_force(mut self, policy: ForcePolicy) -> Self {
        self.allow_force = policy;
        self
    }

    /// The same settings with `extra` namespaces writable too (ones already
    /// listed are not repeated). The server adds the merge queue's
    /// namespaces this way, per request, without changing what is stored.
    pub fn with_writable(mut self, extra: impl IntoIterator<Item = RefPattern>) -> Self {
        for p in extra {
            if !self.writable.contains(&p) {
                self.writable.push(p);
            }
        }
        self
    }

    /// The writable ref namespaces.
    pub fn writable(&self) -> &[RefPattern] {
        &self.writable
    }

    /// Whether `name` is in a writable namespace.
    pub fn is_writable(&self, name: &RefName) -> bool {
        self.writable.iter().any(|p| p.matches(name))
    }

    /// Whether `no-thin` is advertised on push.
    pub fn no_thin(&self) -> bool {
        self.no_thin
    }

    /// How many earlier trunk tips go out as `.have` lines.
    pub fn have_tips(&self) -> u8 {
        self.have_tips
    }

    /// How shallow fetches are answered.
    pub fn shallow(&self) -> ShallowPolicy {
        self.shallow
    }

    /// The writable namespaces as stored in `repos.writable_refs`: patterns
    /// separated by single spaces (ref names cannot contain spaces).
    pub fn writable_column(&self) -> String {
        self.writable
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(" ")
    }

    /// Parses a `repos.writable_refs` value.
    pub fn parse_writable_column(column: &str) -> Result<Vec<RefPattern>, SettingsError> {
        column
            .split(' ')
            .filter(|s| !s.is_empty())
            .map(|s| RefPattern::parse(s).map_err(SettingsError::from))
            .collect()
    }
}

/// Builder for [`RepoSettings`]; [`RepoSettingsBuilder::build`] validates.
#[derive(Debug, Clone)]
pub struct RepoSettingsBuilder(RepoSettings);

impl RepoSettingsBuilder {
    /// Sets the force policy.
    pub fn allow_force(mut self, policy: ForcePolicy) -> Self {
        self.0.allow_force = policy;
        self
    }

    /// Sets whether `no-thin` is advertised.
    pub fn no_thin(mut self, no_thin: bool) -> Self {
        self.0.no_thin = no_thin;
        self
    }

    /// Sets the number of `.have` trunk tips.
    pub fn have_tips(mut self, n: u8) -> Self {
        self.0.have_tips = n;
        self
    }

    /// Sets the shallow policy.
    pub fn shallow(mut self, policy: ShallowPolicy) -> Self {
        self.0.shallow = policy;
        self
    }

    /// Validates and returns the settings.
    pub fn build(self) -> Result<RepoSettings, SettingsError> {
        let s = self.0;
        if !s.trunk.is_branch() {
            return Err(SettingsError::TrunkNotABranch(s.trunk));
        }
        if s.writable.is_empty() {
            return Err(SettingsError::NoWritableNamespace);
        }
        if !s.is_writable(&s.trunk) {
            return Err(SettingsError::TrunkNotWritable(s.trunk));
        }
        if s.have_tips > MAX_HAVE_TIPS {
            return Err(SettingsError::TooManyHaveTips(s.have_tips));
        }
        Ok(s)
    }
}

/// A repository row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Repo {
    /// Row ID.
    pub id: RepoId,
    /// URL name.
    pub name: RepoName,
    /// Settings.
    pub settings: RepoSettings,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn r(s: &str) -> RefName {
        RefName::new(s).unwrap()
    }

    #[test]
    fn defaults_match_the_plan() {
        let s = RepoSettings::new(r("refs/heads/main")).unwrap();
        assert_eq!(s.allow_force(), ForcePolicy::Branches);
        assert_eq!(s.writable_column(), "refs/heads/* refs/tags/*");
        assert!(!s.no_thin());
        assert_eq!(s.have_tips(), 8);
        assert_eq!(s.shallow(), ShallowPolicy::Serve);
        assert!(s.is_writable(&r("refs/tags/v1")));
        assert!(!s.is_writable(&r("refs/remotes/origin/main")));
    }

    #[test]
    fn branches_policy_never_forces_trunk() {
        let trunk = r("refs/heads/main");
        assert!(!ForcePolicy::Branches.permits(&trunk, &trunk));
        assert!(ForcePolicy::Branches.permits(&r("refs/heads/x"), &trunk));
        assert!(ForcePolicy::Branches.permits(&r("refs/tags/v1"), &trunk));
        assert!(ForcePolicy::All.permits(&trunk, &trunk));
        assert!(!ForcePolicy::None.permits(&r("refs/heads/x"), &trunk));
    }

    #[test]
    fn settings_validation() {
        let heads = vec![RefPattern::parse("refs/heads/*").unwrap()];
        assert_eq!(
            RepoSettings::builder(r("refs/tags/main"), heads.clone()).build(),
            Err(SettingsError::TrunkNotABranch(r("refs/tags/main")))
        );
        assert_eq!(
            RepoSettings::builder(r("refs/heads/main"), vec![]).build(),
            Err(SettingsError::NoWritableNamespace)
        );
        assert_eq!(
            RepoSettings::builder(
                r("refs/heads/main"),
                vec![RefPattern::parse("refs/heads/x/*").unwrap()]
            )
            .build(),
            Err(SettingsError::TrunkNotWritable(r("refs/heads/main")))
        );
        assert_eq!(
            RepoSettings::builder(r("refs/heads/main"), heads)
                .have_tips(33)
                .build(),
            Err(SettingsError::TooManyHaveTips(33))
        );
    }

    #[test]
    fn writable_column_round_trips() {
        let s = RepoSettings::new(r("refs/heads/main")).unwrap();
        let parsed = RepoSettings::parse_writable_column(&s.writable_column()).unwrap();
        assert_eq!(parsed, s.writable());
    }

    #[test]
    fn stored_enum_values_round_trip() {
        for p in ForcePolicy::ALL {
            assert_eq!(p.as_str().parse::<ForcePolicy>().unwrap(), p);
        }
        for p in ShallowPolicy::ALL {
            assert_eq!(p.as_str().parse::<ShallowPolicy>().unwrap(), p);
        }
        assert!("sometimes".parse::<ForcePolicy>().is_err());
    }
}
