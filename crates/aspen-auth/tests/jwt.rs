//! JWT verification: every accepted shape, and every refusal the module
//! documents.

mod common;

use aspen_auth::jwt::{Clock, JwtError, JwtVerifier, KeySet, MAX_JWT_LEN};
use common::{AUDIENCE, ISSUER, NOW, Signer, b64, claims};
use serde_json::{Value, json};

const CLOCK: Clock = Clock {
    now: NOW,
    leeway: 60,
};

fn verifier(keys: &[Value]) -> JwtVerifier {
    let set = KeySet::from_json(&json!({ "keys": keys }).to_string()).unwrap();
    JwtVerifier::new(ISSUER, AUDIENCE, set)
}

#[test]
fn accepts_rs256_and_es256() {
    let rsa = Signer::rsa();
    let ec = Signer::ec();
    let v = verifier(&[rsa.jwk(Some("r1")), ec.jwk(Some("e1"))]);
    let c = v.verify(&rsa.simple(Some("r1"), &claims()), CLOCK).unwrap();
    assert_eq!(c.subject, "repo:acme/mono:ref:refs/heads/main");
    assert_eq!(c.string("repository").as_deref(), Some("acme/mono"));
    assert!(v.verify(&ec.simple(Some("e1"), &claims()), CLOCK).is_ok());
    // Without `kid`, every key that fits the algorithm is tried.
    assert!(v.verify(&ec.simple(None, &claims()), CLOCK).is_ok());
    // An array audience that contains ours.
    let mut c = claims();
    c["aud"] = json!(["other", AUDIENCE]);
    assert!(v.verify(&rsa.simple(Some("r1"), &c), CLOCK).is_ok());
}

/// Invariant: only the asymmetric algorithms are accepted; `none` and the
/// HMAC family are refused whatever the key set holds, so a public key can
/// never act as an HMAC secret.
#[test]
fn refuses_none_and_symmetric_algorithms() {
    let rsa = Signer::rsa();
    let v = verifier(&[rsa.jwk(Some("r1"))]);
    let payload = b64(claims().to_string().as_bytes());
    for alg in ["none", "HS256", "HS384", "HS512", "PS256", "EdDSA", ""] {
        let header = b64(json!({ "alg": alg, "kid": "r1" }).to_string().as_bytes());
        for sig in ["", "AAAA"] {
            let t = format!("{header}.{payload}.{sig}");
            assert!(
                matches!(v.verify(&t, CLOCK), Err(JwtError::Algorithm(_))),
                "{alg}"
            );
        }
    }
}

#[test]
fn a_key_must_fit_the_algorithm_and_the_kid() {
    let rsa = Signer::rsa();
    let ec = Signer::ec();
    let v = verifier(&[rsa.jwk(Some("r1")), ec.jwk(Some("e1"))]);
    // ES256 naming the RSA key's kid: no key of the right type.
    let t = ec.token(&json!({ "alg": "ES256", "kid": "r1" }), &claims());
    assert_eq!(v.verify(&t, CLOCK), Err(JwtError::NoKey));
    // An unknown kid.
    assert_eq!(
        v.verify(&rsa.simple(Some("r2"), &claims()), CLOCK),
        Err(JwtError::NoKey)
    );
    // A key that names another algorithm.
    let mut jwk = rsa.jwk(Some("r1"));
    jwk["alg"] = json!("RS512");
    let v = verifier(&[jwk]);
    assert_eq!(
        v.verify(&rsa.simple(Some("r1"), &claims()), CLOCK),
        Err(JwtError::NoKey)
    );
    // Keys for encryption are not used for signatures.
    let mut jwk = rsa.jwk(Some("r1"));
    jwk["use"] = json!("enc");
    assert!(
        KeySet::from_json(&json!({ "keys": [jwk] }).to_string())
            .unwrap()
            .is_empty()
    );
}

#[test]
fn a_changed_token_fails_its_signature() {
    let rsa = Signer::rsa();
    let other = Signer::ec();
    let v = verifier(&[rsa.jwk(Some("r1"))]);
    let good = rsa.simple(Some("r1"), &claims());
    let (head, rest) = good.split_once('.').unwrap();
    let (_, sig) = rest.split_once('.').unwrap();
    let mut forged = claims();
    forged["repository"] = json!("acme/other");
    let forged = format!("{head}.{}.{sig}", b64(forged.to_string().as_bytes()));
    assert_eq!(v.verify(&forged, CLOCK), Err(JwtError::Signature));
    // Signed by a key the issuer never published.
    let v = verifier(&[Signer::ec().jwk(None)]);
    assert_eq!(
        v.verify(&other.simple(None, &claims()), CLOCK),
        Err(JwtError::Signature)
    );
}

#[test]
fn claims_are_checked() {
    let rsa = Signer::rsa();
    let v = verifier(&[rsa.jwk(None)]);
    let check = |edit: &dyn Fn(&mut Value), want: Result<(), JwtError>| {
        let mut c = claims();
        edit(&mut c);
        let got = v.verify(&rsa.simple(None, &c), CLOCK).map(|_| ());
        assert_eq!(got, want, "{c}");
    };
    check(
        &|c| c["iss"] = json!("https://evil.example"),
        Err(JwtError::Claim("iss")),
    );
    check(
        &|c| {
            c.as_object_mut().unwrap().remove("iss");
        },
        Err(JwtError::Claim("iss")),
    );
    check(&|c| c["aud"] = json!("other"), Err(JwtError::Claim("aud")));
    check(
        &|c| c["aud"] = json!(["other"]),
        Err(JwtError::Claim("aud")),
    );
    check(
        &|c| {
            c.as_object_mut().unwrap().remove("aud");
        },
        Err(JwtError::Claim("aud")),
    );
    check(&|c| c["exp"] = json!(NOW - 61), Err(JwtError::Claim("exp")));
    check(&|c| c["exp"] = json!(NOW - 59), Ok(()));
    check(
        &|c| {
            c.as_object_mut().unwrap().remove("exp");
        },
        Err(JwtError::Claim("exp")),
    );
    // A time claim of the wrong type is refused, never read as absent.
    check(
        &|c| c["exp"] = json!("99999999999"),
        Err(JwtError::Claim("exp")),
    );
    check(&|c| c["nbf"] = json!("0"), Err(JwtError::Claim("nbf")));
    check(&|c| c["exp"] = json!((NOW + 10) as f64 + 0.5), Ok(()));
    check(&|c| c["nbf"] = json!(NOW + 61), Err(JwtError::Claim("nbf")));
    check(&|c| c["nbf"] = json!(NOW + 59), Ok(()));
    check(&|c| c["sub"] = json!(""), Err(JwtError::Claim("sub")));
    check(&|c| c["sub"] = json!(5), Err(JwtError::Claim("sub")));
    check(&|c| c["sub"] = json!("a\nb"), Err(JwtError::Claim("sub")));
    check(
        &|c| {
            c.as_object_mut().unwrap().remove("sub");
        },
        Err(JwtError::Claim("sub")),
    );
}

#[test]
fn malformed_tokens_are_refused() {
    let rsa = Signer::rsa();
    let v = verifier(&[rsa.jwk(None)]);
    let good = rsa.simple(None, &claims());
    let crit = rsa.token(&json!({ "alg": "RS256", "crit": ["exp"] }), &claims());
    assert_eq!(v.verify(&crit, CLOCK), Err(JwtError::Critical));
    let too_long = format!("{good}{}", "A".repeat(MAX_JWT_LEN));
    for t in [
        "",
        "a.b",
        "a.b.c.d",
        &format!("{good}.x"),
        // Not base64url: in the header, and in the signature.
        &format!("!{good}"),
        &format!("{good}!"),
        &too_long,
    ] {
        assert!(
            matches!(v.verify(t, CLOCK), Err(JwtError::Malformed(_))),
            "{t:.40}"
        );
    }
    // The payload is read only after the signature verified.
    assert_eq!(
        v.verify(&good.replacen('.', ".!", 1), CLOCK),
        Err(JwtError::Signature)
    );
    // A payload that is JSON but not an object.
    let t = rsa.token(&json!({ "alg": "RS256" }), &json!([1, 2]));
    assert!(matches!(v.verify(&t, CLOCK), Err(JwtError::Malformed(_))));
}
