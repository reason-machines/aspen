//! Credential and principal types.
//!
//! Access is decided in `aspen-auth`, not here: `aspen_auth::AuthService`
//! resolves a [`Credential`] to an `aspen_auth::Identity` (the
//! [`Principal`] plus every grant it holds) once per request or SSH
//! command, and the server decides on it by repository name, before the
//! repository is looked up; per-ref checks of a push use
//! `Identity::authorize_ref`. Smart HTTP and SSH share that one decision
//! (B3 and C3 rows of the decisions log in `docs/PLAN.md`). The
//! `Authenticator` and `Authorizer` hooks that used to sit here saw only a
//! [`Principal`], which carries no grants; no transport used them, and C3
//! removed them.

/// A secret string: a token or password. Its `Debug` form is redacted, so
/// it never reaches a log through `{:?}`.
///
/// It deliberately has no `PartialEq`: comparing a presented secret with a
/// stored one through `==` would short-circuit on the first differing byte
/// and leak timing. Verifiers compare digests (or use a constant-time
/// comparison) on [`Secret::expose`].
#[derive(Clone)]
pub struct Secret(String);

impl Secret {
    /// Wraps a secret.
    pub fn new(secret: impl Into<String>) -> Self {
        Self(secret.into())
    }

    /// The secret, for verification only.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for Secret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Secret(<redacted>)")
    }
}

/// What a client presented. Not comparable, because it may hold a
/// [`Secret`].
#[derive(Debug, Clone)]
pub enum Credential {
    /// Nothing.
    Anonymous,
    /// `Authorization: Bearer <token>`.
    Bearer(Secret),
    /// HTTP Basic, as Git's credential helpers send it.
    Basic {
        /// User name.
        user: String,
        /// Password or token.
        password: Secret,
    },
    /// An SSH public key that the transport already verified the client
    /// holds (`aspen-server`'s SSH transport, node C3).
    SshKey {
        /// The key's SHA-256 fingerprint as OpenSSH prints it,
        /// `SHA256:<base64>` (`aspen_auth::SshFingerprint`).
        fingerprint: String,
    },
}

/// Who is making a request.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Principal {
    /// No credential.
    Anonymous,
    /// A user or service account.
    Subject {
        /// Stable identifier from the identity provider.
        id: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secrets_never_print() {
        let c = Credential::Basic {
            user: "agent".into(),
            password: Secret::new("hunter2"),
        };
        let shown = format!("{c:?}");
        assert!(!shown.contains("hunter2"));
        assert!(shown.contains("agent"));
        assert_eq!(Secret::new("t").expose(), "t");
    }
}
