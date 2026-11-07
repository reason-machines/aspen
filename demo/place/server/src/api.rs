//! The HTTP API of `demo/place/api/openapi.yaml`, under `/api/v1`.
//!
//! Every response but `GET /canvas.svg` (`no-cache` with an ETag) is
//! `Cache-Control: no-store`, and every one has
//! `Access-Control-Allow-Origin: *`; every error is the contract's `Error`
//! body, with `Retry-After` on 429 and 503. Request bodies are JSON of at
//! most 64 KiB.

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use aspen_core::blob::BlobStore;
use aspen_core::meta::MetaStore;
use aspen_server::access::ClientAddr;
use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::{ConnectInfo, Request, State};
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use base64::Engine;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::limits::{SlotRefusal, Windows};
use crate::place::{Place, PlaceConfig, PlaceError, StartRefusal};
use crate::rules::{self, StrokeTile};
use crate::state::{Actor, Subscription, iso, now_ms};

/// The API's path prefix.
pub const API_BASE: &str = "/api/v1";
const MAX_BODY: usize = 64 * 1024;
const HEARTBEAT: Duration = Duration::from_secs(15);

/// Web sessions: a bearer token per visitor and their branch.
#[derive(Debug)]
pub struct Sessions {
    by_digest: HashMap<[u8; 32], Session>,
    max: usize,
    ttl_ms: u64,
    per_address: Windows,
    pushes: Windows,
    pub(crate) swarm_starts: Windows,
}

#[derive(Debug, Clone)]
struct Session {
    branch: String,
    expires_ms: u64,
}

impl Sessions {
    /// Empty, with `config`'s limits.
    pub fn new(config: &PlaceConfig) -> Self {
        Self {
            by_digest: HashMap::new(),
            max: config.max_sessions,
            ttl_ms: config.branch_ttl.as_millis() as u64,
            per_address: Windows::new(config.sessions_per_minute, 60_000, 100_000),
            pushes: Windows::new(config.pushes_per_minute, 60_000, config.max_sessions.max(1)),
            swarm_starts: Windows::new(config.swarm_starts_per_hour, 3_600_000, 100_000),
        }
    }

    fn digest(token: &str) -> [u8; 32] {
        Sha256::digest(token.as_bytes()).into()
    }

    fn get(&self, token: Option<&str>, now: u64) -> Option<Session> {
        let s = self.by_digest.get(&Self::digest(token?))?;
        (s.expires_ms > now).then(|| s.clone())
    }
}

/// The API router over `place`, to be merged beside the Git router. Serve
/// it with `into_make_service_with_connect_info::<SocketAddr>()` so
/// per-address limits see the client.
pub fn router<M, B>(place: Arc<Place<M, B>>) -> Router
where
    M: MetaStore + Clone + 'static,
    B: BlobStore + Clone + 'static,
{
    let api = Router::new()
        .route("/config", get(config::<M, B>))
        .route("/canvas", get(canvas::<M, B>))
        .route("/canvas.svg", get(canvas_svg::<M, B>))
        .route("/branches", get(branches::<M, B>))
        .route("/sessions", post(create_session::<M, B>))
        .route("/quota", get(quota::<M, B>))
        .route("/pushes", post(push::<M, B>))
        .route("/events", get(events::<M, B>))
        .route("/swarm", get(swarm::<M, B>).post(start_swarm::<M, B>))
        .route("/admin/kill-switch", post(kill_switch::<M, B>))
        .route("/admin/reset-history", post(reset_history::<M, B>))
        .fallback(not_found)
        .with_state(place)
        .layer(axum::middleware::from_fn(common_headers));
    Router::new().nest(API_BASE, api)
}

async fn common_headers(request: Request, next: axum::middleware::Next) -> Response {
    let preflight = request.method() == Method::OPTIONS;
    let mut r = if preflight {
        let mut r = StatusCode::NO_CONTENT.into_response();
        let h = r.headers_mut();
        h.insert(
            "access-control-allow-methods",
            HeaderValue::from_static("GET, POST, OPTIONS"),
        );
        h.insert(
            "access-control-allow-headers",
            HeaderValue::from_static("Authorization, Content-Type, Last-Event-ID"),
        );
        h.insert("access-control-max-age", HeaderValue::from_static("600"));
        r
    } else {
        next.run(request).await
    };
    let h = r.headers_mut();
    if !h.contains_key(header::CACHE_CONTROL) {
        h.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    }
    h.insert("access-control-allow-origin", HeaderValue::from_static("*"));
    h.insert(
        "access-control-expose-headers",
        HeaderValue::from_static("Retry-After"),
    );
    r
}

fn json_response(status: StatusCode, body: &Value) -> Response {
    (
        status,
        [(header::CONTENT_TYPE, "application/json; charset=utf-8")],
        body.to_string(),
    )
        .into_response()
}

fn error(status: StatusCode, code: &str, message: &str, retry_after: Option<u64>) -> Response {
    let mut body = json!({ "error": code, "message": message });
    if let Some(s) = retry_after {
        body["retryAfterSeconds"] = json!(s);
    }
    let mut r = json_response(status, &body);
    if let Some(s) = retry_after
        && let Ok(v) = HeaderValue::from_str(&s.to_string())
    {
        r.headers_mut().insert(header::RETRY_AFTER, v);
    }
    r
}

async fn not_found(request: Request) -> Response {
    error(
        StatusCode::NOT_FOUND,
        "not-found",
        &format!(
            "No route for {} {}.",
            request.method(),
            request.uri().path()
        ),
        None,
    )
}

fn retry_secs(ms: u64) -> u64 {
    ms.div_ceil(1000).max(1)
}

fn retry_secs_of(e: &aspen_auth::LimitError) -> u64 {
    use aspen_auth::LimitError::*;
    match e {
        Rate(r) | Quota(r) | AuthFailures(r) => r.0.as_secs().max(1),
        TooLarge { .. } => 3600,
    }
}

/// The client address the per-address limits count: the TCP peer, or the
/// address trusted proxies recorded in `X-Forwarded-For`
/// ([`ClientAddr::client_ip`]); an IPv6 address by its /64.
fn client_key(headers: &HeaderMap, peer: Option<SocketAddr>, source: &ClientAddr) -> String {
    client_ip(headers, peer, source)
        .map_or_else(|| "unknown".to_owned(), aspen_server::access::address_key)
}

/// The client address of a request, as [`client_key`] picks it; `None`
/// only when the TCP peer is unknown.
fn client_ip(headers: &HeaderMap, peer: Option<SocketAddr>, source: &ClientAddr) -> Option<IpAddr> {
    source.client_ip(peer.map(|p| p.ip()), headers)
}

fn bearer(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")
        .map(str::trim)
}

fn parse_json(body: &Bytes) -> Option<Value> {
    if body.len() > MAX_BODY {
        return None;
    }
    serde_json::from_slice(body).ok()
}

async fn config<M, B>(State(place): State<Arc<Place<M, B>>>) -> Response {
    let c = &place.config;
    json_response(
        StatusCode::OK,
        &json!({
            "cloneUrl": c.clone_url,
            "dashboardUrl": c.dashboard_url,
            "canvas": { "width": c.rules.width, "height": c.rules.height, "palette": c.rules.palette },
            "limits": {
                "tilesPerPush": c.rules.tiles_per_push,
                "pushesPerMinute": c.pushes_per_minute,
                "branchTtlSeconds": c.branch_ttl.as_secs().max(1),
            },
            "mergeIntervalMs": (c.merge_interval.as_millis() as u64).max(1),
        }),
    )
}

async fn canvas<M, B>(State(place): State<Arc<Place<M, B>>>) -> Response {
    let ttl = place.config.branch_ttl.as_millis() as u64;
    let body = place
        .shared
        .state
        .lock()
        .expect("not poisoned")
        .snapshot(ttl);
    json_response(StatusCode::OK, &body)
}

/// `GET /canvas.svg`: `main`'s tiles as an image ([`crate::image`]). The
/// ETag is `main`'s commit; `no-cache` makes caches such as GitHub's image
/// proxy revalidate on every view, so a README shows the canvas as it is.
async fn canvas_svg<M, B>(State(place): State<Arc<Place<M, B>>>, headers: HeaderMap) -> Response
where
    M: MetaStore + Clone + 'static,
    B: BlobStore + Clone + 'static,
{
    let main = place.with_state(|s| s.main.clone());
    let etag = format!("\"{}\"", main.commit);
    let fresh = headers
        .get(header::IF_NONE_MATCH)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.split(',').any(|t| t.trim() == etag || t.trim() == "*"));
    let mut response = if fresh {
        StatusCode::NOT_MODIFIED.into_response()
    } else {
        let rules = &place.shared.rules;
        let svg = place.svg.get(main.commit, || {
            crate::image::canvas_svg(&main.pixels, rules.width, rules.height, &rules.palette)
        });
        (StatusCode::OK, svg).into_response()
    };
    let h = response.headers_mut();
    h.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("image/svg+xml"),
    );
    h.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-cache, max-age=0"),
    );
    if let Ok(v) = HeaderValue::from_str(&etag) {
        h.insert(header::ETAG, v);
    }
    response
}

async fn branches<M, B>(State(place): State<Arc<Place<M, B>>>) -> Response {
    let ttl = place.config.branch_ttl.as_millis() as u64;
    let body = place
        .shared
        .state
        .lock()
        .expect("not poisoned")
        .branch_list(ttl);
    json_response(StatusCode::OK, &body)
}

fn quota_json(
    place_config: &PlaceConfig,
    sessions: &mut Sessions,
    branch: &str,
    now: u64,
) -> Value {
    let (remaining, reset) = sessions.pushes.remaining(branch, now);
    json!({
        "pushesPerMinute": place_config.pushes_per_minute,
        "remaining": remaining,
        "resetAt": iso(reset),
        "tilesPerPush": place_config.rules.tiles_per_push,
    })
}

fn peer(request: &Request) -> Option<SocketAddr> {
    request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|c| c.0)
}

async fn create_session<M, B>(State(place): State<Arc<Place<M, B>>>, request: Request) -> Response {
    let now = now_ms();
    let addr = client_key(request.headers(), peer(&request), &place.config.client_addr);
    let mut id = [0u8; 7];
    let mut secret = [0u8; 24];
    if getrandom::fill(&mut id).is_err() || getrandom::fill(&mut secret).is_err() {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "rate-limited",
            "No randomness; retry.",
            Some(1),
        );
    }
    let alphabet = b"abcdefghijkmnpqrstuvwxyz23456789";
    let branch: String = id
        .iter()
        .map(|b| char::from(alphabet[usize::from(b & 31)]))
        .collect();
    let branch = format!("visitors/{branch}");
    let token = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(secret);
    let mut sessions = place.sessions.lock().expect("not poisoned");
    if let Err(ms) = sessions.per_address.admit(&addr, now) {
        return error(
            StatusCode::TOO_MANY_REQUESTS,
            "rate-limited",
            "Too many sessions from this address.",
            Some(retry_secs(ms)),
        );
    }
    if sessions.by_digest.len() >= sessions.max {
        sessions.by_digest.retain(|_, s| s.expires_ms > now);
        if sessions.by_digest.len() >= sessions.max {
            return error(
                StatusCode::TOO_MANY_REQUESTS,
                "rate-limited",
                "Too many visitors right now; try again in a minute.",
                Some(60),
            );
        }
    }
    let expires_ms = now + sessions.ttl_ms;
    sessions.by_digest.insert(
        Sessions::digest(&token),
        Session {
            branch: branch.clone(),
            expires_ms,
        },
    );
    let quota = quota_json(&place.config, &mut sessions, &branch, now);
    json_response(
        StatusCode::CREATED,
        &json!({ "branch": branch, "token": token, "expiresAt": iso(expires_ms), "quota": quota }),
    )
}

fn unauthorized() -> Response {
    error(
        StatusCode::UNAUTHORIZED,
        "unauthorized",
        "Missing or unknown session token. Create one with POST /sessions.",
        None,
    )
}

async fn quota<M, B>(State(place): State<Arc<Place<M, B>>>, headers: HeaderMap) -> Response {
    let now = now_ms();
    let mut sessions = place.sessions.lock().expect("not poisoned");
    let Some(s) = sessions.get(bearer(&headers), now) else {
        return unauthorized();
    };
    let body = quota_json(&place.config, &mut sessions, &s.branch, now);
    json_response(StatusCode::OK, &body)
}

/// A `PushRequest`, type-checked: a body that does not match the schema is
/// a 400, a stroke that breaks a tile rule a 422.
fn parse_push(body: &Bytes) -> Result<(String, Vec<StrokeTile>, Option<String>), &'static str> {
    let v = parse_json(body).ok_or("Body must be a JSON object of at most 64 KiB.")?;
    let o = v.as_object().ok_or("Body must be a JSON object.")?;
    if o.keys()
        .any(|k| !matches!(k.as_str(), "branch" | "tiles" | "message"))
    {
        return Err("Unknown field; a push is {branch, tiles, message?}.");
    }
    let branch = o
        .get("branch")
        .and_then(Value::as_str)
        .ok_or("\"branch\" must be a string.")?;
    let tiles = o
        .get("tiles")
        .and_then(Value::as_array)
        .ok_or("\"tiles\" must be an array of {x, y, color}.")?;
    let mut out = Vec::with_capacity(tiles.len());
    for t in tiles {
        let t = t
            .as_object()
            .ok_or("Each tile must be an object {x, y, color}.")?;
        let bad = "Each tile must be {x: integer, y: integer, color: \"#rrggbb\"}.";
        if t.len() != 3 {
            return Err(bad);
        }
        let x = t.get("x").and_then(Value::as_f64).ok_or(bad)?;
        let y = t.get("y").and_then(Value::as_f64).ok_or(bad)?;
        let color = t.get("color").and_then(Value::as_str).ok_or(bad)?;
        out.push(StrokeTile {
            x,
            y,
            color: color.to_owned(),
        });
    }
    let message = match o.get("message") {
        None => None,
        Some(Value::String(m)) if m.chars().count() <= 200 => Some(m.clone()),
        Some(_) => return Err("\"message\" must be a string of at most 200 characters."),
    };
    Ok((branch.to_owned(), out, message))
}

async fn push<M, B>(
    State(place): State<Arc<Place<M, B>>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response
where
    M: MetaStore + Clone + 'static,
    B: BlobStore + Clone + 'static,
{
    let now = now_ms();
    let session = {
        let sessions = place.sessions.lock().expect("not poisoned");
        sessions.get(bearer(&headers), now)
    };
    let (branch, tiles, message) = match parse_push(&body) {
        Ok(p) => p,
        Err(m) => return error(StatusCode::BAD_REQUEST, "bad-request", m, None),
    };
    let Some(session) = session else {
        return unauthorized();
    };
    if branch != session.branch {
        return error(
            StatusCode::FORBIDDEN,
            "forbidden",
            &format!("This session can push only to {}.", session.branch),
            None,
        );
    }
    if tiles.len() > place.config.web_tiles_per_push {
        let n = place.config.web_tiles_per_push;
        return json_response(
            StatusCode::UNPROCESSABLE_ENTITY,
            &json!({
                "error": "rejected",
                "message": format!("A push from the page carries at most {n} {}. Click one tile at a time, or push more with git.", if n == 1 { "tile" } else { "tiles" }),
                "rejections": [],
            }),
        );
    }
    let tiles = match rules::validate_stroke(&tiles, &place.shared.rules) {
        Ok(t) => t,
        Err(rejections) => {
            let message: Vec<&str> = rejections.iter().map(|r| r.message.as_str()).collect();
            return json_response(
                StatusCode::UNPROCESSABLE_ENTITY,
                &json!({ "error": "rejected", "message": message.join("\n"), "rejections": rejections }),
            );
        }
    };
    // The server-wide ceiling first, so a push it refuses spends none of
    // the session's own budget; a token taken for a push the session's
    // budget then refuses is given back.
    if let Err(at_limit) = place.shared.take_push() {
        return error(
            StatusCode::TOO_MANY_REQUESTS,
            "rate-limited",
            &format!("{}.", at_limit.message()),
            Some(at_limit.retry_after_secs),
        );
    }
    let admitted = place
        .sessions
        .lock()
        .expect("not poisoned")
        .pushes
        .admit(&session.branch, now);
    if let Err(ms) = admitted {
        place.shared.give_back_push();
        return error(
            StatusCode::TOO_MANY_REQUESTS,
            "rate-limited",
            &format!(
                "You can push {} times a minute. Try again in {} s.",
                place.config.pushes_per_minute,
                retry_secs(ms)
            ),
            Some(retry_secs(ms)),
        );
    }
    let label = branch
        .strip_prefix("visitors/")
        .unwrap_or(&branch)
        .to_owned();
    match place
        .paint(
            &branch,
            Actor::visitor(label),
            &tiles,
            message.as_deref().unwrap_or(""),
        )
        .await
    {
        Ok(p) => {
            let quota = quota_json(
                &place.config,
                &mut place.sessions.lock().expect("not poisoned"),
                &branch,
                now_ms(),
            );
            let palette = &place.shared.rules.palette;
            let tiles: Vec<Value> = tiles
                .iter()
                .map(
                    |t| json!({ "x": t.x, "y": t.y, "color": palette[usize::from(t.color)].color }),
                )
                .collect();
            json_response(
                StatusCode::CREATED,
                &json!({
                    "seq": p.seq,
                    "branch": branch,
                    "commit": p.commit.to_string(),
                    "parent": p.parent.to_string(),
                    "tiles": tiles,
                    "quota": quota,
                }),
            )
        }
        Err(PlaceError::Refused(r)) => error(
            StatusCode::TOO_MANY_REQUESTS,
            "rate-limited",
            &format!("The server is busy ({r}); try again."),
            Some(1),
        ),
        Err(e) => {
            eprintln!("place: push to {branch} failed: {e}");
            error(
                StatusCode::SERVICE_UNAVAILABLE,
                "rate-limited",
                "Internal storage error; try again.",
                Some(1),
            )
        }
    }
}

async fn swarm<M, B>(State(place): State<Arc<Place<M, B>>>) -> Response {
    let body = place
        .shared
        .state
        .lock()
        .expect("not poisoned")
        .swarm_status(now_ms());
    json_response(StatusCode::OK, &body)
}

async fn start_swarm<M, B>(State(place): State<Arc<Place<M, B>>>, request: Request) -> Response
where
    M: MetaStore + Clone + 'static,
    B: BlobStore + Clone + 'static,
{
    if !place.config.swarm_enabled {
        return error(
            StatusCode::NOT_FOUND,
            "not-found",
            "The swarm is not available on this demo.",
            None,
        );
    }
    let addr = client_key(request.headers(), peer(&request), &place.config.client_addr);
    // One call checks the kill switch, the run state and the address's
    // starts and starts the run, under one lock (#124).
    match place.start_swarm(&addr) {
        Ok(status) => json_response(StatusCode::ACCEPTED, &status),
        Err((StartRefusal::KillSwitch { reason }, s)) => {
            let m = match reason {
                Some(r) => format!("The swarm is paused by the operators: {r}"),
                None => "The swarm is paused by the operators.".to_owned(),
            };
            error(StatusCode::SERVICE_UNAVAILABLE, "kill-switch", &m, Some(s))
        }
        Err((StartRefusal::Cooldown, s)) => error(
            StatusCode::TOO_MANY_REQUESTS,
            "cooldown",
            &format!("A swarm ran recently. The next one can start in {s} s."),
            Some(s),
        ),
        Err((StartRefusal::StartsUsed, s)) => error(
            StatusCode::TOO_MANY_REQUESTS,
            "rate-limited",
            &format!("This address has started the swarm too often. Try again in {s} s."),
            Some(s),
        ),
    }
}

/// `None` when the request carries an operator credential, otherwise the
/// error response.
async fn refuse_non_operator<M, B>(
    place: &Place<M, B>,
    connect: Option<axum::Extension<ConnectInfo<SocketAddr>>>,
    headers: &HeaderMap,
) -> Option<Response>
where
    M: MetaStore + Clone + 'static,
    B: BlobStore + Clone + 'static,
{
    let addr = client_ip(headers, connect.map(|c| c.0.0), &place.config.client_addr);
    let mut auth = headers.get_all(header::AUTHORIZATION).iter();
    let first = auth.next().map(HeaderValue::as_bytes);
    let operator = if auth.next().is_some() {
        Ok(false)
    } else {
        place.operators.is_operator(first, addr).await
    };
    if let Err(limited) = operator {
        return Some(error(
            StatusCode::TOO_MANY_REQUESTS,
            "rate-limited",
            &format!("Operator credentials refused: {limited}."),
            Some(retry_secs_of(&limited)),
        ));
    }
    if operator != Ok(true) {
        return Some(error(
            StatusCode::FORBIDDEN,
            "forbidden",
            "Operator credentials required: an Aspen token or OIDC token with an admin grant on the canvas repository.",
            None,
        ));
    }
    None
}

/// `POST /admin/reset-history`: [`Place::reset_history`].
async fn reset_history<M, B>(
    State(place): State<Arc<Place<M, B>>>,
    connect: Option<axum::Extension<ConnectInfo<SocketAddr>>>,
    headers: HeaderMap,
) -> Response
where
    M: MetaStore + Clone + 'static,
    B: BlobStore + Clone + 'static,
{
    if let Some(refused) = refuse_non_operator(&place, connect, &headers).await {
        return refused;
    }
    match place.reset_history().await {
        Ok(commit) => json_response(
            StatusCode::OK,
            &serde_json::json!({ "commit": commit.to_string() }),
        ),
        Err(e) => error(
            StatusCode::CONFLICT,
            "reset-failed",
            &format!("The history was not reset: {e}"),
            None,
        ),
    }
}

async fn kill_switch<M, B>(
    State(place): State<Arc<Place<M, B>>>,
    connect: Option<axum::Extension<ConnectInfo<SocketAddr>>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response
where
    M: MetaStore + Clone + 'static,
    B: BlobStore + Clone + 'static,
{
    if let Some(refused) = refuse_non_operator(&place, connect, &headers).await {
        return refused;
    }
    let bad = || {
        error(
            StatusCode::BAD_REQUEST,
            "bad-request",
            "Body must be {\"engaged\": boolean, \"reason\"?: string of at most 200 characters}.",
            None,
        )
    };
    let Some(Value::Object(o)) = parse_json(&body) else {
        return bad();
    };
    if o.keys().any(|k| k != "engaged" && k != "reason") {
        return bad();
    }
    let Some(engaged) = o.get("engaged").and_then(Value::as_bool) else {
        return bad();
    };
    let reason = match o.get("reason") {
        None | Some(Value::Null) => None,
        Some(Value::String(r)) if r.chars().count() <= 200 => Some(r.clone()),
        Some(_) => return bad(),
    };
    json_response(StatusCode::OK, &place.set_kill_switch(engaged, reason))
}

/// How long one event stream may run: a random point in the last tenth of
/// `max`, so streams opened together (a deploy, a network blip) do not all
/// end and reconnect together.
fn stream_age(max: Duration) -> Duration {
    let mut b = [0u8; 2];
    let r = if getrandom::fill(&mut b).is_ok() {
        u32::from(u16::from_le_bytes(b))
    } else {
        0
    };
    max - (max / 10).mul_f64(f64::from(r) / f64::from(u16::MAX))
}

/// `GET /events`: the backlog after `after` (or `Last-Event-ID`), then
/// live events. With `instance` (the snapshot's) naming another instance's
/// feed, a `resync` instead: a client that moved to another task during a
/// start-first deploy never reads its old cursor against this feed.
///
/// Each stream holds a slot of `Place::event_streams` (capped per client
/// address and in all; more get 429) until it ends, and ends between 90%
/// and 100% of `PlaceConfig::event_stream_max_age`; the client reconnects with the
/// `seq` of the last event it saw (#124). A stream whose client went away
/// frees its slot when a write to it fails, at the latest on the next
/// heartbeat.
async fn events<M, B>(
    State(place): State<Arc<Place<M, B>>>,
    headers: HeaderMap,
    request: Request,
) -> Response {
    let query_after = request.uri().query().and_then(|q| {
        q.split('&')
            .filter_map(|kv| kv.split_once('='))
            .find(|(k, _)| *k == "after")
            .map(|(_, v)| v.to_owned())
    });
    // The feed the client's `after` belongs to (`instance` from the
    // snapshot): another instance's is answered with a `resync`.
    let instance = request.uri().query().and_then(|q| {
        q.split('&')
            .filter_map(|kv| kv.split_once('='))
            .find(|(k, _)| *k == "instance")
            .map(|(_, v)| v.to_owned())
    });
    let header_after = headers
        .get("last-event-id")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    let raw = header_after.or(query_after);
    let after = match raw {
        None => None,
        Some(r) => match r.parse::<u64>() {
            Ok(n) if r.bytes().all(|b| b.is_ascii_digit()) => Some(n),
            _ => {
                return error(
                    StatusCode::BAD_REQUEST,
                    "bad-request",
                    "\"after\" and Last-Event-ID must be a non-negative integer.",
                    None,
                );
            }
        },
    };
    let addr = client_key(&headers, peer(&request), &place.config.client_addr);
    let slot = match place.event_streams.acquire(&addr) {
        Ok(slot) => slot,
        Err(refusal) => {
            let m = match refusal {
                SlotRefusal::PerKey => format!(
                    "Too many event streams from this address (at most {}).",
                    place.config.max_event_streams_per_address
                ),
                SlotRefusal::Total => "Too many event streams right now; try again soon.".into(),
            };
            return error(StatusCode::TOO_MANY_REQUESTS, "rate-limited", &m, Some(5));
        }
    };
    let ends = tokio::time::Instant::now() + stream_age(place.config.event_stream_max_age);
    let sub = place.shared.state.lock().expect("not poisoned").subscribe(
        after,
        instance.as_deref(),
        now_ms(),
    );
    let (first, rx, mut last) = match sub {
        Subscription::Backlog(events, rx) => {
            let last = events.last().map_or(after.unwrap_or(0), |e| e.seq);
            let text: String = events.iter().map(|e| e.sse()).collect();
            (text, rx, last)
        }
        Subscription::Resync(e, rx) => (e.sse(), rx, e.seq),
    };
    // Live events at or below `last` were already in the backlog.
    if after.is_none() {
        last = 0;
    }
    let start = format!("retry: 2000\n\n{first}");
    // The slot lives in the stream's state, so it is freed whenever the
    // body is dropped: at the age limit, on a resync, on a failed write or
    // at shutdown.
    let stream = futures_util::stream::unfold(
        (
            Some(start),
            rx,
            last,
            tokio::time::interval(HEARTBEAT),
            slot,
        ),
        move |(pending, mut rx, mut last, mut beat, slot)| async move {
            if let Some(p) = pending {
                beat.reset();
                return Some((
                    Ok::<_, std::io::Error>(Bytes::from(p)),
                    (None, rx, last, beat, slot),
                ));
            }
            loop {
                tokio::select! {
                    () = tokio::time::sleep_until(ends) => return None,
                    _ = beat.tick() => {
                        return Some((Ok(Bytes::from_static(b": keepalive\n\n")), (None, rx, last, beat, slot)));
                    }
                    got = rx.recv() => match got {
                        Ok(e) if e.seq <= last => continue,
                        Ok(e) => {
                            last = e.seq;
                            return Some((Ok(Bytes::from(e.sse())), (None, rx, last, beat, slot)));
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                            // Too far behind: ask the client to reload, and
                            // end the stream; it reconnects from the snapshot.
                            let text = format!(
                                "id: {last}\nevent: resync\ndata: {}\n\n",
                                json!({ "type": "resync", "seq": last, "at": iso(now_ms()) })
                            );
                            let closed = tokio::sync::broadcast::channel(1).1;
                            return Some((Ok(Bytes::from(text)), (None, closed, u64::MAX, beat, slot)));
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => return None,
                    }
                }
            }
        },
    );
    let mut r = Response::new(Body::from_stream(stream));
    let h = r.headers_mut();
    h.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/event-stream; charset=utf-8"),
    );
    h.insert("x-accel-buffering", HeaderValue::from_static("no"));
    r
}
