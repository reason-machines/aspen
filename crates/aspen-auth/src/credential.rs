//! Reading a [`Credential`] from an HTTP `Authorization` header.

use aspen_core::auth::{Credential, Secret};
use aspen_core::error::AuthError;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;

/// Longest `Authorization` value read, in bytes. An OIDC identity token is
/// a few KiB; anything far longer is refused before it is decoded.
pub const MAX_AUTHORIZATION_LEN: usize = 24 * 1024;

/// The challenge a 401 carries, so Git asks its credential helper for a
/// user name and password (the password is the token).
pub const CHALLENGE: &str = "Basic realm=\"Aspen\", charset=\"UTF-8\"";

/// The credential in an `Authorization` header value; no header is
/// [`Credential::Anonymous`]. `Basic` (what Git's credential helpers send)
/// and `Bearer` are understood. Any other scheme, or a malformed value, is
/// [`AuthError::Unauthenticated`]: a client that presents something
/// unreadable is refused, never treated as anonymous.
pub fn from_authorization(value: Option<&[u8]>) -> Result<Credential, AuthError> {
    let Some(value) = value else {
        return Ok(Credential::Anonymous);
    };
    if value.len() > MAX_AUTHORIZATION_LEN {
        return Err(AuthError::Unauthenticated);
    }
    let value = std::str::from_utf8(value).map_err(|_| AuthError::Unauthenticated)?;
    let (scheme, rest) = value
        .trim()
        .split_once(' ')
        .ok_or(AuthError::Unauthenticated)?;
    let rest = rest.trim();
    if scheme.eq_ignore_ascii_case("bearer") {
        if rest.is_empty() {
            return Err(AuthError::Unauthenticated);
        }
        Ok(Credential::Bearer(Secret::new(rest)))
    } else if scheme.eq_ignore_ascii_case("basic") {
        let decoded = STANDARD
            .decode(rest)
            .map_err(|_| AuthError::Unauthenticated)?;
        let decoded = String::from_utf8(decoded).map_err(|_| AuthError::Unauthenticated)?;
        let (user, password) = decoded.split_once(':').ok_or(AuthError::Unauthenticated)?;
        if password.is_empty() {
            return Err(AuthError::Unauthenticated);
        }
        Ok(Credential::Basic {
            user: user.to_owned(),
            password: Secret::new(password),
        })
    } else {
        Err(AuthError::Unauthenticated)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn basic(s: &str) -> String {
        format!("Basic {}", STANDARD.encode(s))
    }

    #[test]
    fn reads_basic_and_bearer() {
        let c = from_authorization(Some(basic("x-access-token:tok").as_bytes())).unwrap();
        let Credential::Basic { user, password } = c else {
            panic!("{c:?}")
        };
        assert_eq!(user, "x-access-token");
        assert_eq!(password.expose(), "tok");
        let c = from_authorization(Some(b"bearer  abc ")).unwrap();
        let Credential::Bearer(s) = c else { panic!() };
        assert_eq!(s.expose(), "abc");
        assert!(matches!(
            from_authorization(None).unwrap(),
            Credential::Anonymous
        ));
    }

    #[test]
    fn unreadable_credentials_are_refused_not_anonymous() {
        let long = format!("Bearer {}", "a".repeat(MAX_AUTHORIZATION_LEN));
        let no_colon = basic("nocolon");
        let empty_password = basic("user:");
        for v in [
            "",
            "Bearer",
            "Bearer ",
            "Digest abc",
            "Basic !!!",
            &no_colon,
            &empty_password,
            &long,
        ] {
            assert_eq!(
                from_authorization(Some(v.as_bytes())).unwrap_err(),
                AuthError::Unauthenticated,
                "{v:.40}"
            );
        }
        assert!(from_authorization(Some(&[0xff, 0x20, 0x41])).is_err());
    }

    /// Invariant: no `Authorization` header is ever an SSH-key credential.
    /// A key's fingerprint is public; only the SSH transport, after it has
    /// checked the client's signature, may stand for a key. Covers headers
    /// built from a fingerprint or a key under every scheme, plus 20,000
    /// random headers over the same alphabet.
    #[test]
    fn no_header_is_an_ssh_key_credential() {
        let fp = "SHA256:uNiVztksCsDhcc0u9e8BujQXVUpKZIDTMczCvj3tD2s";
        let key =
            "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIAcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcH";
        let mut headers: Vec<String> = Vec::new();
        for scheme in [
            "Bearer",
            "Basic",
            "SshKey",
            "ssh",
            "PublicKey",
            "ssh-ed25519",
            "Signature",
        ] {
            for value in [fp, key, "SHA256:", ""] {
                headers.push(format!("{scheme} {value}"));
            }
        }
        for (user, password) in [("git", fp), (fp, fp), (fp, "x"), ("git", key)] {
            headers.push(basic(&format!("{user}:{password}")));
        }
        headers.push(key.to_owned());
        // SplitMix64, fixed seed: the same headers every run.
        let mut state = 0x5eed_c3c3_u64;
        let mut next = || {
            state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
            let mut z = state;
            z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
            z ^ (z >> 31)
        };
        let words = [
            "Bearer",
            "Basic",
            "SshKey",
            "ssh-ed25519",
            fp,
            key,
            ":",
            " ",
            "=",
            "SHA256:",
        ];
        for _ in 0..20_000 {
            let n = next() % 5;
            let header: String = (0..=n)
                .map(|_| words[(next() % words.len() as u64) as usize])
                .collect::<Vec<_>>()
                .join(if next() % 2 == 0 { " " } else { "" });
            headers.push(header);
        }
        for h in &headers {
            if let Ok(Credential::SshKey { .. }) = from_authorization(Some(h.as_bytes())) {
                panic!("{h:.60} read as an SSH key");
            }
        }
    }
}
