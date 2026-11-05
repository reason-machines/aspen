//! Grants: what a principal may do, to which repositories and refs.
//!
//! A grant is written `<permission>:<repositories>[:<refs>]`, for example
//! `read:demo/*`, `write:acme/mono` or
//! `write:acme/mono:refs/heads/agent-7/*`. The same text form is used in
//! configuration files, on the command line and in the token store.

use std::fmt;
use std::str::FromStr;

use aspen_core::name::{RefName, RefPattern, RepoName};

/// How much a grant allows, in increasing order: each level includes the
/// ones below it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Permission {
    /// Clone, fetch and `ls-refs`.
    Read,
    /// Read, and push to the refs the grant names.
    Write,
    /// Write to every ref, and change repository settings.
    Admin,
}

impl Permission {
    /// The name used in the text form of a grant.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Write => "write",
            Self::Admin => "admin",
        }
    }
}

impl FromStr for Permission {
    type Err = GrantError;

    fn from_str(s: &str) -> Result<Self, GrantError> {
        match s {
            "read" => Ok(Self::Read),
            "write" => Ok(Self::Write),
            "admin" => Ok(Self::Admin),
            other => Err(GrantError(format!(
                "unknown permission `{other}` (expected read, write or admin)"
            ))),
        }
    }
}

/// Which repositories a grant covers.
///
/// `*` is every repository, `acme/*` every repository whose name starts
/// with `acme/` (at any depth), and anything else exactly one repository.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RepoPattern {
    /// Every repository.
    Any,
    /// Every repository below this prefix, which ends with `/`.
    Prefix(String),
    /// Exactly this repository.
    Exact(RepoName),
}

impl RepoPattern {
    /// Whether `repo` matches.
    pub fn matches(&self, repo: &RepoName) -> bool {
        match self {
            Self::Any => true,
            Self::Prefix(prefix) => repo.as_str().starts_with(prefix.as_str()),
            Self::Exact(name) => name == repo,
        }
    }
}

impl FromStr for RepoPattern {
    type Err = GrantError;

    fn from_str(s: &str) -> Result<Self, GrantError> {
        if s == "*" {
            return Ok(Self::Any);
        }
        if let Some(prefix) = s.strip_suffix("/*") {
            // A prefix is valid when a repository below it would be.
            RepoName::new(format!("{prefix}/x"))
                .map_err(|e| GrantError(format!("repository pattern `{s}`: {e}")))?;
            return Ok(Self::Prefix(format!("{prefix}/")));
        }
        if s.contains('*') {
            return Err(GrantError(format!(
                "repository pattern `{s}`: `*` is allowed only alone or as a whole last segment"
            )));
        }
        RepoName::new(s)
            .map(Self::Exact)
            .map_err(|e| GrantError(format!("repository pattern `{s}`: {e}")))
    }
}

impl fmt::Display for RepoPattern {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Any => f.write_str("*"),
            Self::Prefix(prefix) => write!(f, "{prefix}*"),
            Self::Exact(name) => write!(f, "{name}"),
        }
    }
}

/// One grant. Only a [`Permission::Write`] grant may name refs: it then
/// allows pushes to those refs only. Reading is always repository-wide, and
/// [`Permission::Admin`] covers every ref.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Grant {
    permission: Permission,
    repos: RepoPattern,
    refs: Option<RefPattern>,
}

impl Grant {
    /// A grant on every ref of the matching repositories.
    pub fn new(permission: Permission, repos: RepoPattern) -> Self {
        Self {
            permission,
            repos,
            refs: None,
        }
    }

    /// A write grant limited to refs matching `refs`.
    pub fn write_refs(repos: RepoPattern, refs: RefPattern) -> Self {
        Self {
            permission: Permission::Write,
            repos,
            refs: Some(refs),
        }
    }

    /// The permission.
    pub fn permission(&self) -> Permission {
        self.permission
    }

    /// The repositories.
    pub fn repos(&self) -> &RepoPattern {
        &self.repos
    }

    /// The refs a write grant is limited to, if any.
    pub fn refs(&self) -> Option<&RefPattern> {
        self.refs.as_ref()
    }
}

impl FromStr for Grant {
    type Err = GrantError;

    fn from_str(s: &str) -> Result<Self, GrantError> {
        // Neither repository names nor ref names may contain `:`.
        let mut parts = s.split(':');
        let (Some(permission), Some(repos)) = (parts.next(), parts.next()) else {
            return Err(GrantError(format!(
                "grant `{s}`: expected <permission>:<repositories>[:<refs>]"
            )));
        };
        let refs = parts.next();
        if parts.next().is_some() {
            return Err(GrantError(format!("grant `{s}`: too many `:`")));
        }
        let permission: Permission = permission.parse()?;
        let repos: RepoPattern = repos.parse()?;
        match refs {
            None => Ok(Self::new(permission, repos)),
            Some(refs) if permission == Permission::Write => {
                let refs = RefPattern::parse(refs)
                    .map_err(|e| GrantError(format!("grant `{s}`: ref pattern: {e}")))?;
                Ok(Self::write_refs(repos, refs))
            }
            Some(_) => Err(GrantError(format!(
                "grant `{s}`: only a write grant may name refs"
            ))),
        }
    }
}

impl fmt::Display for Grant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.permission.as_str(), self.repos)?;
        if let Some(refs) = &self.refs {
            write!(f, ":{refs}")?;
        }
        Ok(())
    }
}

impl serde::Serialize for Grant {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(self)
    }
}

impl<'de> serde::Deserialize<'de> for Grant {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

/// A grant that does not parse.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct GrantError(String);

/// Every grant a principal holds for one request.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Grants(Vec<Grant>);

impl Grants {
    /// No access at all.
    pub fn none() -> Self {
        Self(Vec::new())
    }

    /// The grants given.
    pub fn new(grants: impl IntoIterator<Item = Grant>) -> Self {
        Self(grants.into_iter().collect())
    }

    /// Adds grants.
    pub fn extend(&mut self, grants: impl IntoIterator<Item = Grant>) {
        self.0.extend(grants);
    }

    /// The grants, in the order they were added.
    pub fn as_slice(&self) -> &[Grant] {
        &self.0
    }

    /// The highest permission any grant gives on `repo`, whatever refs it
    /// names; `None` if no grant matches the repository.
    pub fn highest(&self, repo: &RepoName) -> Option<Permission> {
        self.0
            .iter()
            .filter(|g| g.repos.matches(repo))
            .map(|g| g.permission)
            .max()
    }

    /// Whether some grant allows pushing to `name` in `repo`.
    pub fn may_write_ref(&self, repo: &RepoName, name: &RefName) -> bool {
        self.0.iter().any(|g| {
            g.repos.matches(repo)
                && match g.permission {
                    Permission::Read => false,
                    Permission::Admin => true,
                    Permission::Write => g.refs.as_ref().is_none_or(|p| p.matches(name)),
                }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn repo(s: &str) -> RepoName {
        RepoName::new(s).unwrap()
    }

    fn r(s: &str) -> RefName {
        RefName::new(s).unwrap()
    }

    #[test]
    fn grants_round_trip_through_text() {
        for s in [
            "read:*",
            "read:demo/*",
            "write:acme/mono",
            "write:acme/mono:refs/heads/agent-7/*",
            "write:acme/*:refs/tags/v1",
            "admin:acme/mono",
        ] {
            let g: Grant = s.parse().unwrap();
            assert_eq!(g.to_string(), s);
        }
    }

    #[test]
    fn malformed_grants_are_refused() {
        for s in [
            "",
            "read",
            "own:acme/mono",
            "read:acme/mono:refs/heads/*",
            "admin:acme/mono:refs/heads/*",
            "write:acme/mono:refs/heads/*:x",
            "write:acme/mo*",
            "write:acme/*/x",
            "write:acme.git",
            "write:acme/-/x",
            "write:acme/mono:main",
            "write:acme/mono:refs/heads/ma*",
        ] {
            assert!(s.parse::<Grant>().is_err(), "{s} parsed");
        }
    }

    #[test]
    fn a_prefix_pattern_never_matches_a_sibling_with_the_same_leading_text() {
        let p: RepoPattern = "acme/*".parse().unwrap();
        assert!(p.matches(&repo("acme/mono")));
        assert!(p.matches(&repo("acme/a/b")));
        assert!(!p.matches(&repo("acme")));
        assert!(!p.matches(&repo("acme-evil/mono")));
        assert!(!p.matches(&repo("acmex/mono")));
        let exact: RepoPattern = "acme/mono".parse().unwrap();
        assert!(!exact.matches(&repo("acme/mono2")));
        assert!(!exact.matches(&repo("acme/mono/x")));
    }

    #[test]
    fn ref_limited_write_grants_allow_only_their_refs() {
        let g = Grants::new(["write:acme/mono:refs/heads/agent-7/*".parse().unwrap()]);
        let mono = repo("acme/mono");
        assert_eq!(g.highest(&mono), Some(Permission::Write));
        assert!(g.may_write_ref(&mono, &r("refs/heads/agent-7/fix")));
        assert!(!g.may_write_ref(&mono, &r("refs/heads/main")));
        assert!(!g.may_write_ref(&mono, &r("refs/heads/agent-70")));
        assert!(!g.may_write_ref(&repo("acme/other"), &r("refs/heads/agent-7/fix")));
    }

    #[test]
    fn highest_takes_the_maximum_over_matching_grants_only() {
        let g = Grants::new([
            "read:*".parse().unwrap(),
            "admin:acme/mono".parse().unwrap(),
        ]);
        assert_eq!(g.highest(&repo("acme/mono")), Some(Permission::Admin));
        assert_eq!(g.highest(&repo("acme/other")), Some(Permission::Read));
        assert!(g.may_write_ref(&repo("acme/mono"), &r("refs/heads/main")));
        assert!(!g.may_write_ref(&repo("acme/other"), &r("refs/heads/main")));
        assert_eq!(Grants::none().highest(&repo("acme/mono")), None);
    }
}
