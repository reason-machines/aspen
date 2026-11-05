//! Test signers for JWTs: an RSA key from a test-only fixture (ring cannot
//! generate RSA keys) and P-256 keys generated per run.
#![allow(dead_code, unreachable_pub)]

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ring::rand::SystemRandom;
use ring::signature::{
    ECDSA_P256_SHA256_FIXED_SIGNING, EcdsaKeyPair, KeyPair, RSA_PKCS1_SHA256, RsaKeyPair,
    RsaPublicKeyComponents,
};
use serde_json::{Value, json};

/// A 2048-bit RSA key made with `openssl genpkey` for these tests only. It
/// signs nothing outside them.
const TEST_ONLY_RSA_PKCS8: &[u8] = include_bytes!("../fixtures/test-only-rsa-2048.pk8");

pub fn b64(bytes: &[u8]) -> String {
    URL_SAFE_NO_PAD.encode(bytes)
}

pub enum Signer {
    Rsa(RsaKeyPair),
    Ec(EcdsaKeyPair),
}

impl Signer {
    pub fn rsa() -> Self {
        Self::Rsa(RsaKeyPair::from_pkcs8(TEST_ONLY_RSA_PKCS8).unwrap())
    }

    pub fn ec() -> Self {
        let rng = SystemRandom::new();
        let pkcs8 = EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &rng).unwrap();
        Self::Ec(
            EcdsaKeyPair::from_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, pkcs8.as_ref(), &rng)
                .unwrap(),
        )
    }

    /// This key as a JWK.
    pub fn jwk(&self, kid: Option<&str>) -> Value {
        let mut k = match self {
            Self::Rsa(k) => {
                let c = RsaPublicKeyComponents::<Vec<u8>>::from(k.public());
                json!({ "kty": "RSA", "n": b64(&c.n), "e": b64(&c.e) })
            }
            Self::Ec(k) => {
                let point = k.public_key().as_ref();
                json!({
                    "kty": "EC",
                    "crv": "P-256",
                    "x": b64(&point[1..33]),
                    "y": b64(&point[33..65]),
                })
            }
        };
        if let Some(kid) = kid {
            k["kid"] = json!(kid);
        }
        k
    }

    pub fn alg(&self) -> &'static str {
        match self {
            Self::Rsa(_) => "RS256",
            Self::Ec(_) => "ES256",
        }
    }

    pub fn sign(&self, message: &[u8]) -> Vec<u8> {
        let rng = SystemRandom::new();
        match self {
            Self::Rsa(k) => {
                let mut sig = vec![0; k.public().modulus_len()];
                k.sign(&RSA_PKCS1_SHA256, &rng, message, &mut sig).unwrap();
                sig
            }
            Self::Ec(k) => k.sign(&rng, message).unwrap().as_ref().to_vec(),
        }
    }

    /// A compact JWT with this header and these claims, signed by this key.
    pub fn token(&self, header: &Value, claims: &Value) -> String {
        let signing_input = format!(
            "{}.{}",
            b64(header.to_string().as_bytes()),
            b64(claims.to_string().as_bytes())
        );
        let sig = self.sign(signing_input.as_bytes());
        format!("{signing_input}.{}", b64(&sig))
    }

    /// A token with the usual header for this key.
    pub fn simple(&self, kid: Option<&str>, claims: &Value) -> String {
        let mut header = json!({ "alg": self.alg(), "typ": "JWT" });
        if let Some(kid) = kid {
            header["kid"] = json!(kid);
        }
        self.token(&header, claims)
    }
}

pub const NOW: i64 = 1_800_000_000;
pub const ISSUER: &str = "https://token.actions.githubusercontent.com";
pub const AUDIENCE: &str = "https://aspen.example";

/// GitHub Actions-shaped claims, valid at [`NOW`].
pub fn claims() -> Value {
    json!({
        "iss": ISSUER,
        "aud": AUDIENCE,
        "sub": "repo:acme/mono:ref:refs/heads/main",
        "repository": "acme/mono",
        "ref": "refs/heads/main",
        "iat": NOW - 10,
        "nbf": NOW - 10,
        "exp": NOW + 300,
    })
}
