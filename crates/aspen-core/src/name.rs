//! Validated names: refs, ref patterns and repositories.

use std::fmt;
use std::str::FromStr;

/// Longest ref name Aspen accepts, in bytes. Git sets no limit; this one
/// keeps ref names indexable in every supported database.
pub const MAX_REF_NAME_LEN: usize = 1024;

/// Longest repository name Aspen accepts, in bytes.
pub const MAX_REPO_NAME_LEN: usize = 255;

/// Why a string is not a valid name.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum NameError {
    /// The name is empty.
    #[error("name is empty")]
    Empty,
    /// The name is longer than the limit.
    #[error("name is {len} bytes, longer than the limit of {max}")]
    TooLong {
        /// Length of the name in bytes.
        len: usize,
        /// The limit.
        max: usize,
    },
    /// A ref name does not start with `refs/` followed by at least one more
    /// component. Aspen stores only full ref names; `HEAD` is never stored.
    #[error("ref name must start with `refs/` and have at least two components after it")]
    NotUnderRefs,
    /// The name breaks one of Git's `check-ref-format` rules, or Aspen's
    /// repository-name rules.
    #[error("invalid name: {rule}")]
    Invalid {
        /// The rule that was broken, in words.
        rule: &'static str,
    },
}

fn invalid(rule: &'static str) -> NameError {
    NameError::Invalid { rule }
}

/// A full Git ref name such as `refs/heads/main`.
///
/// Valid names follow `git check-ref-format` (without `--allow-onelevel`)
/// and additionally:
/// - start with `refs/` and have at least two components after it, so
///   `refs/heads/main` is valid but `refs/main` is not;
/// - are valid UTF-8 (Git allows arbitrary bytes; Aspen rejects non-UTF-8
///   names so that every name has one text form in the database and logs);
/// - are at most [`MAX_REF_NAME_LEN`] bytes.
///
/// Ordering is bytewise, which is the order Git sorts refs in and the order
/// schema v1's `COLLATE "C"` columns index them in.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RefName(String);

impl RefName {
    /// Validates a full ref name.
    pub fn new(name: impl Into<String>) -> Result<Self, NameError> {
        let name = name.into();
        validate_ref_name(&name)?;
        Ok(Self(name))
    }

    /// The name as a string.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Whether this is a branch (`refs/heads/...`).
    pub fn is_branch(&self) -> bool {
        self.0.starts_with("refs/heads/")
    }

    /// Whether this is a tag (`refs/tags/...`).
    pub fn is_tag(&self) -> bool {
        self.0.starts_with("refs/tags/")
    }
}

fn validate_ref_name(name: &str) -> Result<(), NameError> {
    if name.is_empty() {
        return Err(NameError::Empty);
    }
    if name.len() > MAX_REF_NAME_LEN {
        return Err(NameError::TooLong {
            len: name.len(),
            max: MAX_REF_NAME_LEN,
        });
    }
    let Some(rest) = name.strip_prefix("refs/") else {
        return Err(NameError::NotUnderRefs);
    };
    if !rest.contains('/') {
        return Err(NameError::NotUnderRefs);
    }
    if name.ends_with('/') {
        return Err(invalid("must not end with `/`"));
    }
    if name.ends_with('.') {
        return Err(invalid("must not end with `.`"));
    }
    if name.contains("..") {
        return Err(invalid("must not contain `..`"));
    }
    if name.contains("@{") {
        return Err(invalid("must not contain `@{`"));
    }
    for c in name.chars() {
        if c.is_ascii_control() {
            return Err(invalid("must not contain control characters"));
        }
        if matches!(c, ' ' | '~' | '^' | ':' | '?' | '*' | '[' | '\\') {
            return Err(invalid(
                "must not contain space, `~`, `^`, `:`, `?`, `*`, `[` or `\\`",
            ));
        }
    }
    for component in name.split('/') {
        if component.is_empty() {
            return Err(invalid("must not contain `//`"));
        }
        if component.starts_with('.') {
            return Err(invalid("no component may start with `.`"));
        }
        if component.ends_with(".lock") {
            return Err(invalid("no component may end with `.lock`"));
        }
    }
    Ok(())
}

impl fmt::Display for RefName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl fmt::Debug for RefName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RefName({:?})", self.0)
    }
}

impl FromStr for RefName {
    type Err = NameError;
    fn from_str(s: &str) -> Result<Self, NameError> {
        Self::new(s)
    }
}

impl AsRef<str> for RefName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

/// A pattern of writable refs in a repository's settings: either one exact
/// ref name, or a prefix ending in `/*` that matches every ref below it.
///
/// `refs/heads/*` matches `refs/heads/main` and `refs/heads/a/b`, but not
/// `refs/heads` itself. No other wildcards exist.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RefPattern {
    /// Exactly this ref.
    Exact(RefName),
    /// Every ref that starts with this prefix. The prefix ends with `/`.
    Prefix(String),
}

impl RefPattern {
    /// Parses `refs/heads/*` (prefix) or `refs/heads/main` (exact).
    pub fn parse(pattern: &str) -> Result<Self, NameError> {
        if let Some(prefix) = pattern.strip_suffix('*') {
            if !prefix.ends_with('/') {
                return Err(invalid("`*` is allowed only as a whole last component"));
            }
            // A prefix is valid when some ref below it is valid.
            validate_ref_name(&format!("{prefix}x"))?;
            if prefix.matches('/').count() < 2 {
                return Err(NameError::NotUnderRefs);
            }
            Ok(Self::Prefix(prefix.to_owned()))
        } else {
            RefName::new(pattern).map(Self::Exact)
        }
    }

    /// Whether `name` matches this pattern.
    pub fn matches(&self, name: &RefName) -> bool {
        match self {
            Self::Exact(exact) => exact == name,
            Self::Prefix(prefix) => name.as_str().starts_with(prefix.as_str()),
        }
    }
}

impl fmt::Display for RefPattern {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Exact(name) => write!(f, "{name}"),
            Self::Prefix(prefix) => write!(f, "{prefix}*"),
        }
    }
}

/// A repository's name, the path in its URL without the `.git` suffix,
/// for example `acme/monorepo`.
///
/// One or more `/`-separated segments; each segment is non-empty, uses only
/// ASCII letters, digits, `.`, `_` and `-`, does not start with `.`, is not
/// exactly `-`, and does not end with `.git`. The name is at most
/// [`MAX_REPO_NAME_LEN`] bytes. Names are case-sensitive.
///
/// The last two rules keep URLs unambiguous. The protocol layer strips a
/// `.git` suffix from the URL path, and `/-/` starts the branch-scoped part
/// of `/<repo>.git/-/refs/heads/<branch>` (S2). If a segment could be `-` or
/// end in `.git`, `acme/mono/-/refs/heads/x` would name both a repository
/// and `acme/mono` scoped to `refs/heads/x`, so one repository could shadow
/// another's scoped URL.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RepoName(String);

impl RepoName {
    /// Validates a repository name.
    pub fn new(name: impl Into<String>) -> Result<Self, NameError> {
        let name = name.into();
        if name.is_empty() {
            return Err(NameError::Empty);
        }
        if name.len() > MAX_REPO_NAME_LEN {
            return Err(NameError::TooLong {
                len: name.len(),
                max: MAX_REPO_NAME_LEN,
            });
        }
        for segment in name.split('/') {
            if segment.is_empty() {
                return Err(invalid("repository name has an empty segment"));
            }
            if segment.starts_with('.') {
                return Err(invalid("repository name segment must not start with `.`"));
            }
            if segment == "-" {
                return Err(invalid(
                    "repository name segment must not be `-`, which starts a branch-scoped URL",
                ));
            }
            if segment.ends_with(".git") {
                return Err(invalid("repository name segment must not end with `.git`"));
            }
            if !segment
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
            {
                return Err(invalid(
                    "repository name may use only ASCII letters, digits, `.`, `_`, `-` and `/`",
                ));
            }
        }
        Ok(Self(name))
    }

    /// The name as a string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for RepoName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl fmt::Debug for RepoName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RepoName({:?})", self.0)
    }
}

impl FromStr for RepoName {
    type Err = NameError;
    fn from_str(s: &str) -> Result<Self, NameError> {
        Self::new(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_ordinary_ref_names() {
        for ok in [
            "refs/heads/main",
            "refs/heads/feature/x-1",
            "refs/tags/v1.0.0",
            "refs/heads/agent/7f3a/task_9",
            "refs/heads/ünïcode",
            "refs/heads/a.b",
            "refs/heads/@",
        ] {
            assert!(RefName::new(ok).is_ok(), "{ok} should be valid");
        }
    }

    #[test]
    fn rejects_every_check_ref_format_rule() {
        let cases = [
            ("", NameError::Empty),
            ("HEAD", NameError::NotUnderRefs),
            ("heads/main", NameError::NotUnderRefs),
            ("refs/main", NameError::NotUnderRefs),
            ("refs/heads/main/", invalid("must not end with `/`")),
            ("refs/heads/main.", invalid("must not end with `.`")),
            ("refs/heads/a..b", invalid("must not contain `..`")),
            ("refs/heads/a@{1}", invalid("must not contain `@{`")),
            (
                "refs/heads/a\tb",
                invalid("must not contain control characters"),
            ),
            (
                "refs/heads/a\u{7f}",
                invalid("must not contain control characters"),
            ),
            ("refs//heads/a", invalid("must not contain `//`")),
            (
                "refs/heads/.hidden",
                invalid("no component may start with `.`"),
            ),
            (
                "refs/heads/x.lock",
                invalid("no component may end with `.lock`"),
            ),
            (
                "refs/heads/x.lock/y",
                invalid("no component may end with `.lock`"),
            ),
        ];
        for (name, err) in cases {
            assert_eq!(RefName::new(name), Err(err), "{name:?}");
        }
        for c in [' ', '~', '^', ':', '?', '*', '[', '\\'] {
            let name = format!("refs/heads/a{c}b");
            assert!(
                matches!(RefName::new(&name), Err(NameError::Invalid { .. })),
                "{name:?}"
            );
        }
    }

    #[test]
    fn rejects_overlong_ref_names() {
        let long = format!("refs/heads/{}", "a".repeat(MAX_REF_NAME_LEN));
        assert!(matches!(RefName::new(long), Err(NameError::TooLong { .. })));
    }

    #[test]
    fn classifies_branches_and_tags() {
        let b = RefName::new("refs/heads/x").unwrap();
        let t = RefName::new("refs/tags/x").unwrap();
        let n = RefName::new("refs/notes/x").unwrap();
        assert!(b.is_branch() && !b.is_tag());
        assert!(t.is_tag() && !t.is_branch());
        assert!(!n.is_branch() && !n.is_tag());
    }

    #[test]
    fn ref_patterns_match_prefixes_and_exact_names() {
        let heads = RefPattern::parse("refs/heads/*").unwrap();
        let exact = RefPattern::parse("refs/notes/commits").unwrap();
        let r = |s: &str| RefName::new(s).unwrap();
        assert!(heads.matches(&r("refs/heads/main")));
        assert!(heads.matches(&r("refs/heads/a/b")));
        assert!(!heads.matches(&r("refs/headsx/a")));
        assert!(!heads.matches(&r("refs/tags/v1")));
        assert!(exact.matches(&r("refs/notes/commits")));
        assert!(!exact.matches(&r("refs/notes/commits2")));
        assert_eq!(heads.to_string(), "refs/heads/*");
    }

    #[test]
    fn ref_patterns_reject_other_wildcards() {
        assert!(RefPattern::parse("refs/heads/a*").is_err());
        assert!(RefPattern::parse("refs/*/main").is_err());
        assert!(RefPattern::parse("refs/*").is_err());
        assert!(RefPattern::parse("*").is_err());
    }

    #[test]
    fn repo_names() {
        for ok in [
            "demo",
            "acme/monorepo",
            "a.b_c-d/e",
            "x/y/z",
            "a/-b",
            "a/b-",
            "--/x",
            "a/git",
            "a/x.gitx",
        ] {
            assert!(RepoName::new(ok).is_ok(), "{ok}");
        }
        for bad in [
            "",
            "demo.git",
            "/demo",
            "demo/",
            "a//b",
            ".hidden",
            "a/../b",
            "a b",
            "ü",
            // Would collide with the branch-scoped URL of `acme/mono`.
            "acme/mono/-/refs/heads/x",
            "acme/mono/-",
            "-",
            "-/x",
            // `.git` inside the path, not only at the end.
            "acme/mono.git/x",
            "a.git/b",
        ] {
            assert!(RepoName::new(bad).is_err(), "{bad:?}");
        }
    }
}
