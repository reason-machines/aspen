//! The `jwt` target: OIDC identity-token verification
//! (`aspen_auth::jwt`, issue #139).
//!
//! A fuzzer cannot sign, so most inputs are decisions for a token the
//! target builds and signs itself with keys it generates at start-up: a
//! header and a payload written field by field (wrong types, duplicate
//! fields, `crit`, every accepted and refused algorithm, times on both
//! sides of the leeway), a signature by the right key, the wrong key or
//! none, and damage to the segments. A model predicts the verifier's exact
//! answer, in the order the checks run (shape, header, `crit`, algorithm,
//! key, signature, payload, then `iss`, `aud`, `exp`, `nbf`, `sub`), and
//! the claims of an accepted token. The rest of the inputs are raw tokens,
//! which no key signed, so they must be refused before any claim is read.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::LazyLock;

use aspen_auth::jwt::{
    Claims, Clock, JwtError, JwtVerifier, KeySet, MAX_JWT_LEN, looks_like_jwt, unverified_issuer,
};
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ring::rand::SystemRandom;
use ring::signature::{
    ECDSA_P256_SHA256_FIXED_SIGNING, ECDSA_P384_SHA384_FIXED_SIGNING, EcdsaKeyPair, KeyPair as _,
};
use serde_json::{Value, json};

use crate::Reached;
use crate::front::RAW;
use crate::input::Input;

const ISSUER: &str = "https://issuer.example";
const AUDIENCE: &str = "aspen";
const NOW: i64 = 1_800_000_000;

/// A signing key and what the key set says about it.
struct Signer {
    name: &'static str,
    /// Its `kid` in the key set; `p256b` has none.
    kid: Option<&'static str>,
    pair: EcdsaKeyPair,
    /// The algorithm it signs with.
    alg: &'static str,
}

/// A key in the verifier's set, as the model sees it.
struct ModelKey {
    name: &'static str,
    kid: Option<&'static str>,
    /// The algorithms its type fits.
    fits: &'static [&'static str],
    /// The `alg` the key set names for it, if any.
    alg: Option<&'static str>,
}

struct Keys {
    rng: SystemRandom,
    /// `p256` (ES256, `alg` named), `p256b` (ES256, no `alg` and no
    /// `kid`), `p384`.
    signers: Vec<Signer>,
    model: Vec<ModelKey>,
    verifier: JwtVerifier,
}

fn b64(bytes: &[u8]) -> String {
    URL_SAFE_NO_PAD.encode(bytes)
}

/// Fresh keys for this process: the target needs no secret on disk, and a
/// fuzzer that has never seen them cannot forge a signature.
static KEYS: LazyLock<Keys> = LazyLock::new(|| {
    let rng = SystemRandom::new();
    let pair = |alg, which| {
        let pkcs8 = EcdsaKeyPair::generate_pkcs8(alg, &rng).expect("key generation");
        EcdsaKeyPair::from_pkcs8(alg, pkcs8.as_ref(), &rng)
            .unwrap_or_else(|e| panic!("{which}: {e}"))
    };
    let signers = vec![
        Signer {
            name: "p256",
            kid: Some("p256"),
            pair: pair(&ECDSA_P256_SHA256_FIXED_SIGNING, "p256"),
            alg: "ES256",
        },
        Signer {
            name: "p256b",
            kid: None,
            pair: pair(&ECDSA_P256_SHA256_FIXED_SIGNING, "p256b"),
            alg: "ES256",
        },
        Signer {
            name: "p384",
            kid: Some("p384"),
            pair: pair(&ECDSA_P384_SHA384_FIXED_SIGNING, "p384"),
            alg: "ES384",
        },
    ];
    let ec = |s: &Signer, crv: &str, alg: Option<&str>| {
        let point = s.pair.public_key().as_ref();
        let half = (point.len() - 1) / 2;
        let mut key = json!({
            "kty": "EC", "crv": crv, "use": "sig",
            "x": b64(&point[1..1 + half]), "y": b64(&point[1 + half..]),
        });
        if let Some(kid) = s.kid {
            key["kid"] = json!(kid);
        }
        if let Some(alg) = alg {
            key["alg"] = json!(alg);
        }
        key
    };
    // An RSA key nobody holds the private half of: RS256 tokens find a key
    // and fail the signature check, RS384 and RS512 find none (its `alg`).
    let mut modulus = vec![0xc3u8; 256];
    modulus[255] |= 1;
    let jwks = json!({ "keys": [
        ec(&signers[0], "P-256", Some("ES256")),
        ec(&signers[1], "P-256", None),
        ec(&signers[2], "P-384", Some("ES384")),
        { "kty": "RSA", "kid": "rsa", "alg": "RS256", "n": b64(&modulus), "e": "AQAB" },
        // Skipped by the key set: an encryption key and an HMAC secret.
        { "kty": "EC", "crv": "P-256", "kid": "enc", "use": "enc",
          "x": b64(&[1; 32]), "y": b64(&[2; 32]) },
        { "kty": "oct", "kid": "hmac", "k": b64(b"secret") },
    ]});
    let keys = KeySet::from_json(&jwks.to_string()).expect("the target's key set");
    assert_eq!(
        keys.len(),
        4,
        "the key set skips exactly the enc and oct keys"
    );
    let model = vec![
        ModelKey {
            name: "p256",
            kid: Some("p256"),
            fits: &["ES256"],
            alg: Some("ES256"),
        },
        ModelKey {
            name: "p256b",
            kid: None,
            fits: &["ES256"],
            alg: None,
        },
        ModelKey {
            name: "p384",
            kid: Some("p384"),
            fits: &["ES384"],
            alg: Some("ES384"),
        },
        ModelKey {
            name: "rsa",
            kid: Some("rsa"),
            fits: &["RS256", "RS384", "RS512"],
            alg: Some("RS256"),
        },
    ];
    Keys {
        rng,
        signers,
        model,
        verifier: JwtVerifier::new(ISSUER, AUDIENCE, keys),
    }
});

const ACCEPTED: &[&str] = &["RS256", "RS384", "RS512", "ES256", "ES384"];

/// A JSON object written field by field, so it can hold what a map cannot:
/// a field twice, in any order.
#[derive(Debug, Default)]
struct Fields(Vec<(String, Value)>);

impl Fields {
    fn push(&mut self, key: &str, value: Value) {
        self.0.push((key.to_owned(), value));
    }

    fn text(&self) -> String {
        let body: Vec<String> = self
            .0
            .iter()
            .map(|(k, v)| format!("{}:{v}", Value::from(k.as_str())))
            .collect();
        format!("{{{}}}", body.join(","))
    }

    fn count(&self, key: &str) -> usize {
        self.0.iter().filter(|(k, _)| k == key).count()
    }

    /// The value a map keeps: the last one.
    fn last(&self, key: &str) -> Option<&Value> {
        self.0.iter().rev().find(|(k, _)| k == key).map(|(_, v)| v)
    }

    /// The object as a map reads it (the last copy of a field wins).
    fn map(&self) -> BTreeMap<String, Value> {
        self.0.iter().cloned().collect()
    }
}

fn pick<T: Clone>(input: &mut Input<'_>, from: &[T]) -> T {
    from[input.below(from.len())].clone()
}

/// What the model knows about a built header.
#[derive(Debug)]
enum HeaderModel {
    /// A JWS header with these fields.
    Parsed {
        alg: String,
        kid: Option<String>,
        crit: bool,
    },
    /// Not a JWS header the verifier can read: not an object, a field
    /// twice (serde refuses a struct field twice), `alg` missing or not a
    /// string, or `kid` neither a string nor null.
    Malformed,
}

fn built_header(input: &mut Input<'_>, signer: Option<&Signer>) -> (String, HeaderModel) {
    if input.one_in(32) {
        return (
            pick(input, &["[]", "\"x\"", "5", "{", "null"]).to_owned(),
            HeaderModel::Malformed,
        );
    }
    let mut fields = Fields::default();
    let alg = if input.one_in(6) {
        pick(
            input,
            &[
                json!("none"),
                json!("HS256"),
                json!("es256"),
                json!(""),
                json!("RS256"),
                json!("RS384"),
                json!("RS512"),
                json!("ES256"),
                json!("ES384"),
                json!("PS256"),
                json!("ES256\u{0}"),
                json!("A".repeat(40)),
                json!(5),
                Value::Null,
            ],
        )
    } else {
        json!(signer.map_or("ES256", |s| s.alg))
    };
    let skip_alg = input.one_in(24);
    if !skip_alg {
        fields.push("alg", alg.clone());
    }
    let kid = if input.one_in(3) {
        pick(
            input,
            &[
                None,
                Some(json!("p256")),
                Some(json!("p256b")),
                Some(json!("")),
                Some(json!("p384")),
                Some(json!("rsa")),
                Some(json!("enc")),
                Some(json!("hmac")),
                Some(json!("other")),
                Some(Value::Null),
                Some(json!(7)),
            ],
        )
    } else {
        signer.and_then(|s| s.kid).map(|kid| json!(kid))
    };
    if let Some(kid) = &kid {
        fields.push("kid", kid.clone());
    }
    let crit = input.one_in(16);
    if crit {
        fields.push(
            "crit",
            pick(input, &[json!(["b64"]), json!("x"), json!({}), json!([])]),
        );
    }
    if input.one_in(2) {
        fields.push("typ", json!("JWT"));
    }
    if input.one_in(8) {
        fields.push("x5u", json!("https://attacker.example/key"));
    }
    if input.one_in(24) && !fields.0.is_empty() {
        let dup = fields.0[input.below(fields.0.len())].clone();
        fields.0.push(dup);
    }
    if input.one_in(4) && fields.0.len() > 1 {
        fields.0.rotate_left(1);
    }
    let text = fields.text();
    // Serde refuses a field of the header struct twice; it skips every
    // other field, however often it appears.
    let twice = ["alg", "kid", "crit"].iter().any(|k| fields.count(k) > 1);
    let model = match (fields.last("alg"), fields.last("kid")) {
        _ if twice => HeaderModel::Malformed,
        (Some(Value::String(alg)), None | Some(Value::Null)) => HeaderModel::Parsed {
            alg: alg.clone(),
            kid: None,
            crit,
        },
        (Some(Value::String(alg)), Some(Value::String(kid))) => HeaderModel::Parsed {
            alg: alg.clone(),
            kid: Some(kid.clone()),
            crit,
        },
        _ => HeaderModel::Malformed,
    };
    (text, model)
}

/// A time claim's value, around [`NOW`] and the leeway.
fn time(input: &mut Input<'_>, leeway: i64, typical: i64) -> Value {
    if !input.one_in(4) {
        return json!(typical);
    }
    pick(
        input,
        &[
            json!(NOW - 1000),
            json!(NOW - leeway - 1),
            json!(NOW - leeway),
            json!(NOW - leeway + 1),
            json!(NOW),
            json!(NOW + leeway - 1),
            json!(NOW + leeway),
            json!(NOW + leeway + 1),
            json!(NOW + 1000),
            json!(i64::MAX),
            json!(i64::MIN),
            json!(u64::MAX),
            json!(1e300),
            json!(-1e300),
            json!(NOW as f64 + 0.5),
            json!(NOW as f64 - 0.5),
            json!(NOW.to_string()),
            json!(true),
            Value::Null,
            json!([NOW + 1000]),
        ],
    )
}

/// Builds the payload; `None` for a payload that is not a JSON object.
fn built_payload(input: &mut Input<'_>, leeway: i64) -> (String, Option<Fields>) {
    if input.one_in(32) {
        return (
            pick(input, &["[]", "[1]", "\"x\"", "5", "{", "{\"iss\":}"]).to_owned(),
            None,
        );
    }
    let mut fields = Fields::default();
    if !input.one_in(16) {
        let iss = if input.one_in(8) {
            pick(
                input,
                &[
                    json!("https://issuer.example/"),
                    json!("https://other.example"),
                    json!(""),
                    json!(5),
                    Value::Null,
                    json!([ISSUER]),
                ],
            )
        } else {
            json!(ISSUER)
        };
        fields.push("iss", iss);
    }
    if !input.one_in(16) {
        let aud = if input.one_in(6) {
            pick(
                input,
                &[
                    json!("other"),
                    json!(["other", AUDIENCE]),
                    json!(["other"]),
                    json!([]),
                    json!(5),
                    json!([5]),
                    json!({ "aud": AUDIENCE }),
                    json!("Aspen"),
                ],
            )
        } else {
            json!(AUDIENCE)
        };
        fields.push("aud", aud);
    }
    if !input.one_in(16) {
        fields.push("exp", time(input, leeway, NOW + 600));
    }
    if input.one_in(2) {
        fields.push("nbf", time(input, leeway, NOW - 600));
    }
    if input.one_in(2) {
        fields.push("iat", json!(NOW - 600));
    }
    if !input.one_in(16) {
        let sub = if input.one_in(6) {
            pick(
                input,
                &[
                    json!(""),
                    json!("a\u{7}b"),
                    json!("a\nb"),
                    json!("é"),
                    json!(5),
                    Value::Null,
                    json!(["repo:acme/mono"]),
                ],
            )
        } else {
            json!("repo:acme/mono:ref:refs/heads/main")
        };
        fields.push("sub", sub);
    }
    for _ in 0..input.below(3) {
        let key = String::from_utf8_lossy(input.bytes(8)).into_owned();
        let value = pick(
            input,
            &[json!("x"), json!(true), json!(1.5), Value::Null, json!({})],
        );
        fields.push(&key, value);
    }
    if input.one_in(16) && !fields.0.is_empty() {
        let dup = fields.0[input.below(fields.0.len())].clone();
        let at = input.below(fields.0.len() + 1);
        fields.0.insert(at, dup);
    }
    if input.one_in(64) {
        fields.push("pad", json!("a".repeat(MAX_JWT_LEN)));
    }
    (fields.text(), Some(fields))
}

/// A time claim as the verifier reads it, or `None` for a value that is not
/// a number. Wide arithmetic, so the comparisons below need no saturation.
fn seconds(v: &Value) -> Option<i128> {
    let Value::Number(n) = v else {
        return None;
    };
    Some(if let Some(i) = n.as_i64() {
        i128::from(i)
    } else if n.as_u64().is_some() {
        // Past i64: read as a float, which saturates.
        i128::from(i64::MAX)
    } else {
        let f = n.as_f64()?.trunc();
        if f >= i64::MAX as f64 {
            i128::from(i64::MAX)
        } else if f <= i64::MIN as f64 {
            i128::from(i64::MIN)
        } else {
            f as i128
        }
    })
}

/// The claim checks, written from the module's documentation.
fn claims_model(map: BTreeMap<String, Value>, leeway: i64) -> Result<Claims, JwtError> {
    if map.get("iss") != Some(&json!(ISSUER)) {
        return Err(JwtError::Claim("iss"));
    }
    let aud_ok = match map.get("aud") {
        Some(Value::String(a)) => a == AUDIENCE,
        Some(Value::Array(list)) => list.contains(&json!(AUDIENCE)),
        _ => false,
    };
    if !aud_ok {
        return Err(JwtError::Claim("aud"));
    }
    let (now, leeway) = (i128::from(NOW), i128::from(leeway));
    let exp = map.get("exp").ok_or(JwtError::Claim("exp"))?;
    let exp = seconds(exp).ok_or(JwtError::Claim("exp"))?;
    if now >= exp + leeway {
        return Err(JwtError::Claim("exp"));
    }
    if let Some(nbf) = map.get("nbf") {
        let nbf = seconds(nbf).ok_or(JwtError::Claim("nbf"))?;
        if now + leeway < nbf {
            return Err(JwtError::Claim("nbf"));
        }
    }
    let subject = match map.get("sub") {
        Some(Value::String(s)) if !s.is_empty() && !s.chars().any(char::is_control) => s.clone(),
        _ => return Err(JwtError::Claim("sub")),
    };
    Ok(Claims { subject, all: map })
}

/// `jwt`: on a built token, `JwtVerifier::verify` gives exactly the
/// model's answer and claims, `unverified_issuer` gives the payload's one
/// string `iss` (and nothing for a payload with `iss` twice, which serde
/// refuses), and `looks_like_jwt` holds exactly for three segments. On a
/// raw token, which no key signed, `verify` refuses before any claim
/// check, and an issuer read without verifying never contradicts an
/// accepted token's.
pub fn jwt(data: &[u8]) {
    jwt_reached(data);
}

/// [`jwt`], returning the oracle paths the input reached.
pub fn jwt_reached(data: &[u8]) -> BTreeSet<Reached> {
    let keys = &*KEYS;
    let mut input = Input::new(data);
    let raw = input.byte() % 4 == RAW;
    let leeway = if input.flag() { 60 } else { 0 };
    let clock = Clock { now: NOW, leeway };
    if raw {
        let token = String::from_utf8_lossy(input.rest()).into_owned();
        let got = keys.verifier.verify(&token, clock);
        assert!(
            matches!(
                got,
                Err(JwtError::Malformed(_)
                    | JwtError::Algorithm(_)
                    | JwtError::Critical
                    | JwtError::NoKey
                    | JwtError::Signature)
            ),
            "a token no key signed got past the signature check: {got:?} for {token:?}"
        );
        if !looks_like_jwt(&token) {
            assert!(matches!(got, Err(JwtError::Malformed(_))), "{got:?}");
        }
        let _ = unverified_issuer(&token);
        return BTreeSet::from([reached(&got)]);
    }

    let signer = match input.below(8) {
        0 => None,
        n => Some(&keys.signers[n % keys.signers.len()]),
    };
    let (header_text, header) = built_header(&mut input, signer);
    let (payload_text, payload) = built_payload(&mut input, leeway);
    let mut h = b64(header_text.as_bytes());
    let p = b64(payload_text.as_bytes());
    // Padding, which base64url in a JWS never has.
    let h_padded = input.one_in(32);
    if h_padded {
        h.push('=');
    }
    let message = format!("{h}.{p}");
    let mut sig = match signer {
        Some(s) => s
            .pair
            .sign(&keys.rng, message.as_bytes())
            .expect("signing")
            .as_ref()
            .to_vec(),
        None => input.bytes(96).to_vec(),
    };
    let damaged = match input.below(16) {
        0 if !sig.is_empty() => {
            let at = input.below(sig.len());
            sig[at] ^= 1 | input.byte();
            true
        }
        1 if !sig.is_empty() => {
            sig.truncate(input.below(sig.len()));
            true
        }
        2 => {
            sig.push(0);
            true
        }
        _ => false,
    };
    let mut s = b64(&sig);
    let s_padded = input.one_in(32);
    if s_padded {
        s.push('=');
    }
    let token = match input.below(32) {
        0 => message.clone(),
        1 => format!("{message}.{s}.x"),
        2 => format!("{message}.{s} "),
        _ => format!("{message}.{s}"),
    };

    let want = verify_model(
        &token,
        &header,
        h_padded,
        s_padded,
        signer.filter(|_| !damaged),
        payload.as_ref(),
        leeway,
    );
    let got = keys.verifier.verify(&token, clock);
    assert_eq!(
        got, want,
        "header {header_text}, payload {payload_text}, token {token:?}"
    );
    assert_eq!(
        looks_like_jwt(&token),
        token.split('.').count() == 3,
        "{token:?}"
    );
    let issuer = match &payload {
        Some(fields) if token.len() <= MAX_JWT_LEN && fields.count("iss") == 1 => fields
            .last("iss")
            .and_then(Value::as_str)
            .map(str::to_owned),
        _ => None,
    };
    assert_eq!(unverified_issuer(&token), issuer, "{token:?}");
    BTreeSet::from([reached(&got)])
}

/// The verifier's answer, check by check.
fn verify_model(
    token: &str,
    header: &HeaderModel,
    h_padded: bool,
    s_padded: bool,
    signed_by: Option<&Signer>,
    payload: Option<&Fields>,
    leeway: i64,
) -> Result<Claims, JwtError> {
    if token.len() > MAX_JWT_LEN {
        return Err(JwtError::Malformed("too long"));
    }
    if token.split('.').count() != 3 {
        return Err(JwtError::Malformed("expected three segments"));
    }
    if h_padded {
        return Err(JwtError::Malformed("segment is not base64url"));
    }
    let HeaderModel::Parsed { alg, kid, crit } = header else {
        return Err(JwtError::Malformed("header is not a JWS header"));
    };
    if *crit {
        return Err(JwtError::Critical);
    }
    if !ACCEPTED.contains(&alg.as_str()) {
        return Err(JwtError::Algorithm(alg.chars().take(32).collect()));
    }
    // The trailing blank of a damaged token is part of the signature
    // segment, which then does not decode.
    if s_padded || token.ends_with(' ') {
        return Err(JwtError::Malformed("segment is not base64url"));
    }
    let candidates: Vec<&ModelKey> = KEYS
        .model
        .iter()
        .filter(|k| k.fits.contains(&alg.as_str()) && k.alg.is_none_or(|a| a == alg))
        // A token with a `kid` is checked only against the key with that
        // `kid`; a key without one never matches it.
        .filter(|k| kid.as_deref().is_none_or(|want| k.kid == Some(want)))
        .collect();
    if candidates.is_empty() {
        return Err(JwtError::NoKey);
    }
    let verifies =
        signed_by.is_some_and(|s| s.alg == alg && candidates.iter().any(|k| k.name == s.name));
    if !verifies {
        return Err(JwtError::Signature);
    }
    let Some(fields) = payload else {
        return Err(JwtError::Malformed("payload is not a JSON object"));
    };
    claims_model(fields.map(), leeway)
}

fn reached(got: &Result<Claims, JwtError>) -> Reached {
    match got {
        Ok(_) => Reached::JwtOk,
        Err(JwtError::Malformed(_)) => Reached::JwtMalformed,
        Err(JwtError::Algorithm(_)) => Reached::JwtAlgorithm,
        Err(JwtError::Critical) => Reached::JwtCritical,
        Err(JwtError::NoKey) => Reached::JwtNoKey,
        Err(JwtError::Signature) => Reached::JwtSignature,
        Err(JwtError::Claim(name)) => Reached::JwtClaim(name),
    }
}
