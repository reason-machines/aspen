//! Services, protocol versions and the smart-HTTP framing around them.
//!
//! A client asks for a protocol version in the `Git-Protocol` HTTP header
//! (or the `GIT_PROTOCOL` environment variable over SSH), for example
//! `version=2`. Aspen answers:
//!
//! - `git-upload-pack` (fetch, clone, ls-remote) in protocol v2 when the
//!   client asked for v2. A client that asked for v0 or v1 (libgit2,
//!   go-git, or git configured with `protocol.version=0`) gets the v0
//!   upload-pack of [`crate::upload_v0`], never a v2 advertisement
//!   ([`UploadVersion`]);
//! - `git-receive-pack` (push) in v1 when the client asked for v1, otherwise
//!   v0. Push has no v2 form: a client that asks for v2 gets v0, as stock
//!   `git receive-pack` does.

/// A Git wire-protocol version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ProtocolVersion {
    /// The original protocol (no version line).
    V0,
    /// v0 preceded by a `version 1` line.
    V1,
    /// Protocol v2: a capability advertisement, then commands.
    V2,
}

impl ProtocolVersion {
    /// The version a client requested in a `Git-Protocol` header or
    /// `GIT_PROTOCOL` value: colon-separated `key[=value]` items, of which
    /// the highest valid `version=<n>` wins (as in Git's
    /// `determine_protocol_version_server`). Absent or unparseable means v0.
    pub fn requested(git_protocol: Option<&str>) -> Self {
        git_protocol
            .into_iter()
            .flat_map(|h| h.split(':'))
            .filter_map(|item| match item.strip_prefix("version=")? {
                "0" => Some(Self::V0),
                "1" => Some(Self::V1),
                "2" => Some(Self::V2),
                _ => None,
            })
            .max()
            .unwrap_or(Self::V0)
    }
}

/// Which protocol a push is answered in: receive-pack has no v2.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReceiveVersion {
    /// No version line.
    V0,
    /// A `version 1` line before the advertisement.
    V1,
}

impl ReceiveVersion {
    /// The answer to a client that requested `requested`.
    pub fn answering(requested: ProtocolVersion) -> Self {
        match requested {
            ProtocolVersion::V1 => Self::V1,
            ProtocolVersion::V0 | ProtocolVersion::V2 => Self::V0,
        }
    }
}

/// Protocol v0 or v1: v1 is v0 with a `version 1` line before the
/// advertisement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum V0Version {
    /// No version line.
    V0,
    /// A `version 1` line first.
    V1,
}

impl From<V0Version> for ProtocolVersion {
    fn from(v: V0Version) -> Self {
        match v {
            V0Version::V0 => Self::V0,
            V0Version::V1 => Self::V1,
        }
    }
}

/// How a fetch is answered, given the version the client requested.
///
/// A v2 advertisement sent to a client that did not ask for v2 is a
/// protocol error on that client (stock git happens to accept one over
/// `ext::`, but libgit2 and go-git do not), so the answer is an exhaustive
/// enum: a caller has to handle [`UploadVersion::V0`] on its own path and
/// cannot fall through to v2.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub enum UploadVersion {
    /// The client asked for v2: answer with [`crate::v2::V2Advertisement`].
    V2,
    /// The client asked for v0 or v1, or for nothing: answer with
    /// [`crate::upload_v0::write_advertisement`] in this version (the
    /// `version 1` line goes only before the advertisement, never in a
    /// stateless POST response, as `git upload-pack` does).
    V0(V0Version),
}

impl UploadVersion {
    /// The answer to a client that requested `requested`.
    pub fn answering(requested: ProtocolVersion) -> Self {
        match requested {
            ProtocolVersion::V2 => Self::V2,
            ProtocolVersion::V1 => Self::V0(V0Version::V1),
            ProtocolVersion::V0 => Self::V0(V0Version::V0),
        }
    }
}

/// A Git service, as named in `info/refs?service=` and the POST path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Service {
    /// `git-upload-pack`: fetch, clone, ls-remote.
    UploadPack,
    /// `git-receive-pack`: push.
    ReceivePack,
}

impl Service {
    /// Parses `git-upload-pack` or `git-receive-pack`.
    pub fn parse(name: &str) -> Option<Self> {
        match name {
            "git-upload-pack" => Some(Self::UploadPack),
            "git-receive-pack" => Some(Self::ReceivePack),
            _ => None,
        }
    }

    /// The service name.
    pub const fn name(self) -> &'static str {
        match self {
            Self::UploadPack => "git-upload-pack",
            Self::ReceivePack => "git-receive-pack",
        }
    }

    /// `Content-Type` of the `GET info/refs?service=...` response.
    pub const fn advertisement_content_type(self) -> &'static str {
        match self {
            Self::UploadPack => "application/x-git-upload-pack-advertisement",
            Self::ReceivePack => "application/x-git-receive-pack-advertisement",
        }
    }

    /// `Content-Type` a client sends on the POST.
    pub const fn request_content_type(self) -> &'static str {
        match self {
            Self::UploadPack => "application/x-git-upload-pack-request",
            Self::ReceivePack => "application/x-git-receive-pack-request",
        }
    }

    /// `Content-Type` of the POST response.
    pub const fn result_content_type(self) -> &'static str {
        match self {
            Self::UploadPack => "application/x-git-upload-pack-result",
            Self::ReceivePack => "application/x-git-receive-pack-result",
        }
    }
}

/// Appends what a smart-HTTP `info/refs` response starts with, before the
/// advertisement: `# service=<name>` and a flush, except in protocol v2,
/// where the response starts directly with `version 2` (as
/// `git http-backend` does).
pub fn write_info_refs_header(out: &mut Vec<u8>, service: Service, answered: ProtocolVersion) {
    if answered != ProtocolVersion::V2 {
        crate::pkt::write_text(out, &format!("# service={}", service.name()))
            .expect("static text fits");
        crate::pkt::write_flush(out);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn highest_valid_requested_version_wins() {
        use ProtocolVersion::*;
        assert_eq!(ProtocolVersion::requested(None), V0);
        assert_eq!(ProtocolVersion::requested(Some("version=2")), V2);
        assert_eq!(ProtocolVersion::requested(Some("version=1:version=2")), V2);
        assert_eq!(ProtocolVersion::requested(Some("version=2:version=1")), V2);
        assert_eq!(
            ProtocolVersion::requested(Some("object-format=sha1:version=1")),
            V1
        );
        assert_eq!(ProtocolVersion::requested(Some("version=3")), V0);
        assert_eq!(ProtocolVersion::requested(Some("")), V0);
    }

    #[test]
    fn push_never_answers_v2() {
        assert_eq!(
            ReceiveVersion::answering(ProtocolVersion::V2),
            ReceiveVersion::V0
        );
        assert_eq!(
            ReceiveVersion::answering(ProtocolVersion::V1),
            ReceiveVersion::V1
        );
        assert_eq!(
            ReceiveVersion::answering(ProtocolVersion::V0),
            ReceiveVersion::V0
        );
    }

    /// Invariant: only a client that asked for v2 gets a v2 answer to a
    /// fetch; v0, v1 and no request at all get v0 upload-pack (A1b), in
    /// the version they asked for.
    #[test]
    fn fetch_answers_v2_only_when_asked() {
        use UploadVersion as U;
        assert_eq!(U::answering(ProtocolVersion::V2), U::V2);
        assert_eq!(U::answering(ProtocolVersion::V0), U::V0(V0Version::V0));
        assert_eq!(U::answering(ProtocolVersion::V1), U::V0(V0Version::V1));
        assert_eq!(
            U::answering(ProtocolVersion::requested(None)),
            U::V0(V0Version::V0)
        );
        assert_eq!(
            U::answering(ProtocolVersion::requested(Some("version=1"))),
            U::V0(V0Version::V1)
        );
        assert_eq!(
            ProtocolVersion::from(V0Version::V1),
            ProtocolVersion::V1,
            "the info/refs header follows the answered version"
        );
    }

    #[test]
    fn info_refs_header_only_before_v0_and_v1() {
        let mut out = Vec::new();
        write_info_refs_header(&mut out, Service::ReceivePack, ProtocolVersion::V0);
        assert_eq!(out, b"001f# service=git-receive-pack\n0000");
        let mut out = Vec::new();
        write_info_refs_header(&mut out, Service::UploadPack, ProtocolVersion::V2);
        assert!(out.is_empty());
        for s in [Service::UploadPack, Service::ReceivePack] {
            assert_eq!(Service::parse(s.name()), Some(s));
            assert!(s.result_content_type().ends_with("-result"));
        }
        assert_eq!(Service::parse("git-upload-archive"), None);
    }
}
