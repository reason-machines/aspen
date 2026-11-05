//! Capability lists and the `agent` capability.
//!
//! In protocol v0/v1 capabilities travel as one space-separated list: after a
//! NUL on the first advertised ref, and after a NUL on the first command or
//! want the client sends back. In protocol v2 each capability is its own
//! pkt-line. Either way a capability is `name` or `name=value`.

use std::fmt;

/// One capability: `name` or `name=value`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Capability {
    /// The name.
    pub name: String,
    /// The value after `=`, if any.
    pub value: Option<String>,
}

impl Capability {
    /// Parses `name` or `name=value`.
    pub fn parse(s: &str) -> Self {
        match s.split_once('=') {
            Some((n, v)) => Self {
                name: n.to_owned(),
                value: Some(v.to_owned()),
            },
            None => Self {
                name: s.to_owned(),
                value: None,
            },
        }
    }
}

impl fmt::Display for Capability {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.value {
            Some(v) => write!(f, "{}={v}", self.name),
            None => f.write_str(&self.name),
        }
    }
}

/// Splits a v0/v1 capability list on spaces. Empty items (doubled spaces)
/// are skipped, as Git does.
pub fn parse_list(list: &str) -> Vec<Capability> {
    list.split(' ')
        .filter(|s| !s.is_empty())
        .map(Capability::parse)
        .collect()
}

/// The only object format Aspen serves (SHA-256 is out of scope for v1).
pub const OBJECT_FORMAT: &str = "sha1";

/// Why an `agent` value is not usable.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("agent must be 1 to 256 printable ASCII characters without spaces")]
pub struct BadAgent;

/// The value of the `agent` capability Aspen advertises, such as
/// `aspen/0.1.0`.
///
/// It goes inside space-separated v0/v1 capability lists, so it may not
/// contain a space; Git also restricts it to printable ASCII.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Agent(String);

impl Agent {
    /// Validates an agent string.
    pub fn new(agent: impl Into<String>) -> Result<Self, BadAgent> {
        let agent = agent.into();
        let ok = (1..=256).contains(&agent.len()) && agent.bytes().all(|b| b.is_ascii_graphic());
        if ok { Ok(Self(agent)) } else { Err(BadAgent) }
    }

    /// `aspen/<version>`, from the workspace version.
    pub fn aspen() -> Self {
        Self(format!("aspen/{}", aspen_core::VERSION))
    }

    /// The value.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lists_parse_names_and_values() {
        let caps =
            parse_list("report-status  side-band-64k agent=git/2.56.0-Linux object-format=sha1");
        let shown: Vec<String> = caps.iter().map(ToString::to_string).collect();
        assert_eq!(
            shown,
            [
                "report-status",
                "side-band-64k",
                "agent=git/2.56.0-Linux",
                "object-format=sha1"
            ]
        );
        assert_eq!(caps[2].value.as_deref(), Some("git/2.56.0-Linux"));
        // Only the first `=` separates.
        assert_eq!(Capability::parse("a=b=c").value.as_deref(), Some("b=c"));
    }

    #[test]
    fn agents_cannot_break_a_capability_list() {
        assert!(Agent::new("aspen/1.0").is_ok());
        assert_eq!(Agent::new("aspen 1.0"), Err(BadAgent));
        assert_eq!(Agent::new(""), Err(BadAgent));
        assert_eq!(Agent::new("a\0b"), Err(BadAgent));
        assert_eq!(Agent::new("é"), Err(BadAgent));
        assert!(Agent::aspen().as_str().starts_with("aspen/"));
    }
}
