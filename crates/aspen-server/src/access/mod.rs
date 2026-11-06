//! Access control for Git over smart HTTP and SSH: authentication,
//! authorization, repository routing, rate limits and push quotas, applied
//! before any Git handler runs.
//!
//! Steps 3 to 6 below are one function, [`Access::admit`], which the SSH
//! transport ([`crate::ssh`]) runs too, once per `git-upload-pack` or
//! `git-receive-pack` command, with the identity of the key the client
//! authenticated with. So both transports make the same decision, apply
//! the same per-caller buckets and the same push quota.
//!
//! [`guard`] is an axum middleware. For every request whose path is a Git
//! endpoint ([`route`]) it runs these steps in order, and the first refusal
//! answers the request:
//!
//! 1. **Route.** Parse the repository, the optional branch scope and the
//!    endpoint from the path. A bad name is 404; `info/refs` without a
//!    smart `service` is 403 (no dumb HTTP); a wrong method is 405.
//! 2. **Authenticate.** Read the `Authorization` header and resolve it to an
//!    [`Identity`] ([`aspen_auth::AuthService::identify`]). A bad credential
//!    is 401 with a `Basic` challenge; a request with more than one
//!    `Authorization` header is 400. Every presented credential that fails
//!    is charged to the client address's failed-authentication bucket
//!    ([`aspen_auth::LimitsConfig::auth_failures`]); while it is empty, a
//!    request with a credential is 429 before the credential is checked.
//!    The charge is taken before the check and given back on success, so
//!    concurrent requests cannot overshoot the bucket (issue #198).
//! 3. **Authorize** by name, before the repository is looked up: fetches
//!    need read, both push endpoints need write. Anonymous callers that may
//!    not are 401 (so Git asks for credentials), signed-in callers 404 or
//!    403 ([`aspen_auth::policy`] has the table).
//! 4. **Rate limit** per caller: the principal when signed in, the client
//!    address when anonymous. 429 with `Retry-After`. Then take a
//!    [`Slot`] for the request's kind of work ([`crate::admission`]): 429
//!    with `Retry-After` when the caller already runs as many as it may,
//!    503 with `Retry-After` when the instance does and its queue is full
//!    or the wait runs out. The slot is held until the request is done,
//!    before its body is read; the request's body holds it too.
//! 5. **Route to the repository**: look it up in the metadata store, or in
//!    the instance's short-lived copy of it ([`Access::repo`], [`cache`]).
//!    A missing repository is 404, the same answer as step 3 gives a
//!    signed-in caller with no grant.
//! 6. **Push quota** (`POST git-receive-pack` only): a declared length over
//!    the per-push cap is 413, a spent byte budget 429, both before the body
//!    is read (the declared length is the body as sent, compressed or not).
//!    The body is then wrapped so that it fails with [`PushBodyError`] as
//!    soon as it exceeds the cap or the budget, so an over-quota pack is
//!    never completed and so never stored. A body with a `Content-Encoding`
//!    is not wrapped: its meter goes to receive-pack as an
//!    `EncodedPushMeter`, which charges the larger of the bytes sent and
//!    the bytes inflated (decisions log, "#134").
//!
//! Only then does the request reach the Git handler, with an [`Authorized`]
//! request extension. Handlers that serve repository data take
//! `Extension<Authorized>` and nothing else to find their repository, so a
//! handler reached without this middleware fails to extract it and serves
//! nothing. Each command of a push is checked again with
//! [`Authorized::authorize_ref`].
//!
//! A path that is not a Git endpoint answers 404 and never reaches the
//! handler (a handler behind this middleware could only fail to extract
//! [`Authorized`]). Mount routes that are not Git, such as health checks or
//! the presigned blob route, on a router this middleware does not wrap.

pub mod cache;
mod client_addr;
pub mod route;
pub mod tokens;

use std::fmt;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;

use aspen_auth::credential::{CHALLENGE, from_authorization};
use aspen_auth::{
    AuthService, Identity, LimitError, Limits, Need, NoSshKeys, RequestClass, SshKeyStore,
    TokenStore,
};
use aspen_core::auth::{Credential, Principal};
use aspen_core::error::{AuthError, StoreError, ToWire};
use aspen_core::id::UnixMicros;
use aspen_core::meta::MetaStore;
use aspen_core::name::{RefName, RepoName};
use aspen_core::repo::Repo;
use axum::body::Body;
use axum::extract::{ConnectInfo, Request, State};
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use http_body::Frame;

pub use cache::AccessCaches;
pub use client_addr::{ClientAddr, ClientAddrError, Fallback, Forwarded, TrustedProxies};

use crate::admission::{Admission, Busy, InstanceLimits, RETRY_AFTER, Slot, Work};
use cache::TtlCache;
pub use route::{Endpoint, GitRoute, RouteError};

/// Everything [`guard`] and the SSH transport need: the auth service (with
/// its token store and SSH key store `K`), the limits and the metadata
/// store.
pub struct Access<M, T, K = NoSshKeys> {
    auth: AuthService<T, K>,
    limits: Limits,
    meta: M,
    client_addr: ClientAddr,
    admission: Arc<Admission>,
    /// Repository rows by name, kept for [`AccessCaches::repo_ttl`].
    repos: TtlCache<RepoName, Repo>,
}

impl<M, T, K> fmt::Debug for Access<M, T, K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Access")
            .field("limits", &self.limits)
            .field("client_addr", &self.client_addr)
            .field("admission", &self.admission)
            .finish_non_exhaustive()
    }
}

impl<M: MetaStore + 'static, T: TokenStore + 'static, K: SshKeyStore + 'static> Access<M, T, K> {
    /// Access control over these parts, with the default
    /// [`InstanceLimits`], the per-caller caps of `limits`
    /// ([`Limits::concurrent`]) and repository rows cached for
    /// [`cache::DEFAULT_REPO_TTL`].
    pub fn new(auth: AuthService<T, K>, limits: Limits, meta: M, client_addr: ClientAddr) -> Self {
        let admission = Arc::new(Admission::new(
            limits.concurrent(),
            InstanceLimits::default(),
        ));
        Self {
            auth,
            limits,
            meta,
            client_addr,
            admission,
            repos: TtlCache::new(cache::DEFAULT_REPO_TTL, cache::CAPACITY),
        }
    }

    /// Keeps repository rows for `ttl` instead of the default; zero reads
    /// the repository on every request ([`cache`]).
    pub fn with_repo_cache_ttl(mut self, ttl: std::time::Duration) -> Self {
        self.repos = TtlCache::new(ttl, cache::CAPACITY);
        self
    }

    /// The repository named `name`, from this instance's copy when it is
    /// younger than the repository TTL, else from the metadata store (and
    /// then kept). A missing repository is never remembered.
    pub async fn repo(&self, name: &RepoName) -> Result<Option<Repo>, StoreError> {
        if let Some(repo) = self.repos.get(name) {
            return Ok(Some(repo));
        }
        let ticket = self.repos.ticket();
        let repo = self.meta.repo(name).await?;
        if let Some(repo) = &repo {
            self.repos.insert(name.clone(), repo.clone(), ticket);
        }
        Ok(repo)
    }

    /// Bounds the work this instance runs at once with `instance` instead
    /// of the defaults ([`crate::admission`]).
    pub fn with_instance_limits(mut self, instance: InstanceLimits) -> Self {
        self.admission = Arc::new(Admission::new(self.limits.concurrent(), instance));
        self
    }

    /// What runs at once, per caller and per instance.
    pub fn admission(&self) -> &Arc<Admission> {
        &self.admission
    }

    /// The auth service (to rotate issuer keys, or reach the token and SSH
    /// key stores).
    pub fn auth(&self) -> &AuthService<T, K> {
        &self.auth
    }

    /// Where client addresses come from (and its fallback counters).
    pub fn client_addr(&self) -> &ClientAddr {
        &self.client_addr
    }

    /// The metadata store.
    pub fn meta(&self) -> &M {
        &self.meta
    }

    /// Drops this instance's copy of repository `name`: call it after
    /// changing or deleting the repository through this process, so its
    /// next request reads the change. Other instances see it within the
    /// repository TTL.
    pub fn forget_repo(&self, name: &RepoName) {
        self.repos.remove(name);
    }

    /// Drops every cached repository row.
    pub fn forget_repos(&self) {
        self.repos.clear();
    }

    /// Steps 3 to 6 of the module docs for an identified caller: authorize
    /// `route` by name, take a request from the caller's bucket (`key`, see
    /// [`subject_key`] and [`ip_key`]), look the repository up and, for
    /// [`Endpoint::ReceivePack`], admit the push against its quota
    /// (`declared` is the body length the client announced, if any). The
    /// first refusal is returned; nothing after it runs.
    ///
    /// On success the caller gets the [`Authorized`] a Git handler needs
    /// and, for a push whose bytes are capped or budgeted, the
    /// [`PushMeter`] that must see every byte of the pack.
    pub async fn admit(
        self: &Arc<Self>,
        identity: Arc<Identity>,
        route: GitRoute,
        key: &str,
        declared: Option<u64>,
        now: Instant,
    ) -> Result<(Authorized, Option<PushMeter>), Refusal> {
        let (need, class) = if route.endpoint.is_push() {
            (Need::Push, RequestClass::Push)
        } else {
            (Need::Read, RequestClass::Read)
        };
        identity
            .authorize(&route.repo, need)
            .map_err(Refusal::Auth)?;
        self.limits.admit(class, key, now).map_err(Refusal::Limit)?;
        let work = if route.endpoint.is_push() {
            Work::Push
        } else {
            Work::Fetch
        };
        let slot = self
            .admission
            .enter(work, key)
            .await
            .map_err(Refusal::Busy)?;
        let repo = match self.repo(&route.repo).await {
            Ok(Some(repo)) => repo,
            Ok(None) => return Err(Refusal::Auth(AuthError::NotFound)),
            // Core's wording for every storage failure a client sees; the
            // cause stays in the server's logs.
            Err(_) => return Err(Refusal::Unavailable),
        };
        let (push, meter) = if route.endpoint == Endpoint::ReceivePack {
            let max_pack_bytes = self
                .limits
                .admit_push_body(key, declared, now)
                .map_err(Refusal::Limit)?;
            let meter = (max_pack_bytes.is_some() || self.limits.budgets_push_bytes()).then(|| {
                let access = Arc::clone(self);
                let key = key.to_owned();
                PushMeter::new(max_pack_bytes, move |n| {
                    access.limits.charge_push_bytes(&key, n, Instant::now())
                })
            });
            let limits = PushLimits {
                max_objects: self.limits.quota().max_objects,
                max_pack_bytes,
            };
            (Some(limits), meter)
        } else {
            (None, None)
        };
        Ok((
            Authorized {
                repo,
                route,
                identity,
                push,
                caller_key: key.to_owned(),
                slot,
                admission: Arc::clone(&self.admission),
            },
            meter,
        ))
    }
}

impl<M, T, K> Access<M, T, K> {
    /// Authenticates `credential` for a client at `address` (an
    /// [`address_key`]): refused with [`Refusal::Limit`] while the address
    /// has used up its failed authentications, before the credential costs
    /// a store read or a signature check. The credential takes one failed
    /// authentication from the address before it is checked and gives it
    /// back if it authenticates or could not be checked (a store outage),
    /// so requests that arrive together cannot all pass on one token
    /// ([`aspen_auth::Limits::charge_credential`], issue #198). No
    /// credential (anonymous) is never limited here, and neither is
    /// [`UNKNOWN_ADDRESS`] (see [`counts_failures`]).
    pub async fn identify(
        &self,
        credential: &Credential,
        address: &str,
    ) -> Result<Identity, Refusal>
    where
        T: TokenStore,
        K: SshKeyStore,
    {
        let charge = if !matches!(credential, Credential::Anonymous) && counts_failures(address) {
            Some(
                self.limits
                    .charge_credential(address, Instant::now())
                    .map_err(Refusal::Limit)?,
            )
        } else {
            None
        };
        let result = self.auth.identify(credential, UnixMicros::now()).await;
        let refund = |charge: Option<aspen_auth::CredentialCharge<'_>>| {
            if let Some(charge) = charge {
                charge.refund(Instant::now());
            }
        };
        match result {
            Ok(identity) => {
                refund(charge);
                Ok(identity)
            }
            Err(AuthError::Unavailable) => {
                refund(charge);
                Err(Refusal::Unavailable)
            }
            // The charge stays taken: this is the failure it paid for.
            Err(e) => Err(Refusal::Auth(e)),
        }
    }

    /// Takes one more request from `key`'s bucket for `class`: for a
    /// transport that carries several requests on one admitted connection
    /// (an SSH session of protocol v2 commands), so each costs what one
    /// HTTP request costs.
    pub fn charge(&self, class: RequestClass, key: &str, now: Instant) -> Result<(), LimitError> {
        self.limits.admit(class, key, now)
    }
}

/// Why [`Access::admit`] refused a request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refusal {
    /// The caller may not, or the repository does not exist (the same
    /// answer to a signed-in caller without a grant).
    Auth(AuthError),
    /// A rate limit or push quota.
    Limit(LimitError),
    /// The metadata store failed; retry.
    Unavailable,
    /// Too much of the request's kind of work is running, for this caller
    /// or on this instance ([`crate::admission`]); retry.
    Busy(Busy),
}

impl Refusal {
    /// The text a client is shown.
    pub fn message(&self) -> String {
        match self {
            Self::Auth(e) => e.to_wire().message,
            Self::Limit(e) => e.to_string(),
            Self::Unavailable => "internal storage error; retry".to_owned(),
            Self::Busy(b) => b.to_string(),
        }
    }
}

/// What a Git handler receives: the repository, the route, the caller and
/// the push limits. Present only on requests that passed [`guard`].
#[derive(Debug, Clone)]
pub struct Authorized {
    /// The repository, as stored.
    pub repo: Repo,
    /// The route (repository name, scope, endpoint).
    pub route: GitRoute,
    /// The caller.
    pub identity: Arc<Identity>,
    /// Limits for a push (`POST git-receive-pack`), `None` otherwise.
    pub push: Option<PushLimits>,
    /// The key this caller is rate-limited by: `subject:<id>` when signed
    /// in, `ip:<address>` when anonymous, with an IPv6 address reduced to
    /// its /64 (`ip:2001:db8:1:2::/64`).
    pub caller_key: String,
    /// The request's own slot ([`crate::admission`]), released when the
    /// last clone of this is dropped.
    slot: Slot,
    admission: Arc<Admission>,
}

impl Authorized {
    /// The request's slot, for whatever must keep it after the handler
    /// returns (the blocking thread that reads a push's pack).
    pub(crate) fn slot(&self) -> &Slot {
        &self.slot
    }

    /// A stitch slot for this caller's fetch: held while its inline pack
    /// is built and streamed.
    pub(crate) async fn stitch(&self) -> Result<Slot, Busy> {
        self.admission.enter(Work::Stitch, &self.caller_key).await
    }

    /// Whether the caller may update `name`. Receive-pack calls this for
    /// every command and answers a refusal with an `ng` line for that ref
    /// ([`aspen_core::error::RejectReason::PermissionDenied`]).
    pub fn authorize_ref(&self, name: &RefName) -> Result<(), AuthError> {
        self.identity.authorize_ref(&self.repo.name, name)
    }
}

/// Limits receive-pack enforces itself, because they need the pack.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PushLimits {
    /// Most objects the pack may hold: receive-pack compares it with the
    /// pack header's count before it reads the entries (S4).
    pub max_objects: Option<u64>,
    /// Most bytes the body may carry; the body itself already enforces it.
    pub max_pack_bytes: Option<u64>,
}

/// The middleware. Use with
/// `axum::middleware::from_fn_with_state(Arc<Access<M, T, K>>, guard)`.
pub async fn guard<M, T, K>(
    State(access): State<Arc<Access<M, T, K>>>,
    mut request: Request,
    next: Next,
) -> Response
where
    M: MetaStore + 'static,
    T: TokenStore + 'static,
    K: SshKeyStore + 'static,
{
    let route = match GitRoute::parse(request.uri().path(), request.uri().query()) {
        Ok(route) => route,
        Err(RouteError::NotGit | RouteError::BadName) => return refuse(AuthError::NotFound),
        Err(RouteError::DumbHttp) => {
            return text(
                StatusCode::FORBIDDEN,
                "Aspen serves only Git's smart HTTP protocol",
            );
        }
    };
    let method_ok = if route.endpoint.is_post() {
        request.method() == Method::POST
    } else {
        request.method() == Method::GET || request.method() == Method::HEAD
    };
    if !method_ok {
        return text(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }

    // Several `Authorization` headers are refused rather than resolved by
    // picking one: a proxy and the handler could pick differently.
    let mut authorization = request.headers().get_all(header::AUTHORIZATION).iter();
    let first = authorization.next();
    if authorization.next().is_some() {
        return text(
            StatusCode::BAD_REQUEST,
            "more than one Authorization header",
        );
    }
    let credential = match from_authorization(first.map(HeaderValue::as_bytes)) {
        Ok(c) => c,
        Err(e) => return refuse(e),
    };
    let address = client_address_key(&request, &access.client_addr);
    let identity = match access.identify(&credential, &address).await {
        Ok(identity) => identity,
        Err(refusal) => return refused(refusal),
    };
    let key = subject_key(&identity).unwrap_or(address);
    let declared = declared_len(request.headers());
    let (authorized, meter) = match access
        .admit(Arc::new(identity), route, &key, declared, Instant::now())
        .await
    {
        Ok(admitted) => admitted,
        Err(refusal) => return refused(refusal),
    };
    // The body holds the slot too: a push's body is read on a blocking
    // thread that can outlive the handler.
    let slot = authorized.slot().clone();
    request = request.map(move |body| {
        Body::new(Holding {
            inner: body,
            _slot: slot,
        })
    });
    if let Some(meter) = meter {
        if crate::body::Encoding::of(request.headers()) == Ok(crate::body::Encoding::Identity) {
            request = request.map(move |body| Body::new(PushBody { inner: body, meter }));
        } else {
            // Charged where the body is inflated; an encoding receive-pack
            // does not read is refused before a byte is read.
            request
                .extensions_mut()
                .insert(EncodedPushMeter(Arc::new(std::sync::Mutex::new(Some(
                    meter,
                )))));
        }
    }
    request.extensions_mut().insert(authorized);
    next.run(request).await
}

/// The rate-limit and quota key of a signed-in caller: its principal, so a
/// subject's HTTP requests and SSH commands share one bucket. `None` for an
/// anonymous caller, whose key is its address.
pub fn subject_key(identity: &Identity) -> Option<String> {
    match identity.principal() {
        Principal::Subject { id } => Some(format!("subject:{id}")),
        Principal::Anonymous => None,
    }
}

/// The client-address key of an HTTP request: the rate-limit and quota key
/// of an anonymous caller (module docs, step 4) and the key of every
/// caller's failed-authentication bucket (step 2).
fn client_address_key(request: &Request, source: &ClientAddr) -> String {
    let peer = request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|c| c.0.ip());
    match source.client_ip(peer, request.headers()) {
        Some(ip) => ip_key(ip),
        None => UNKNOWN_ADDRESS.to_owned(),
    }
}

/// The address key of a request whose client address is unknown: the
/// transport gave no peer address (no `ConnectInfo`; Aspen's own listeners
/// always give one). Behind proxies, a request without a readable
/// forwarded address is keyed by its TCP peer instead ([`ClientAddr`],
/// issue #121), so no client can choose this key. Every anonymous caller
/// without an address shares its rate limit, which errs on the side of
/// limiting; its failed authentications are not limited
/// ([`counts_failures`]).
pub const UNKNOWN_ADDRESS: &str = "ip:unknown";

/// Whether failed authentications from `address` are limited: every
/// address but [`UNKNOWN_ADDRESS`], which only an embedding that serves
/// without `ConnectInfo` produces; one shared failure bucket there would
/// let any caller lock every credential out (decisions log, D4 review).
/// Hops set lower than the proxies in front key every caller by the
/// nearest proxy's address: one limited bucket per proxy. Hops set higher
/// let a client choose its key: Aspen reads an entry the client wrote in
/// its own `X-Forwarded-For`, so its failures are spread over buckets it
/// picks. Only callers that send no header fall back to the proxy's
/// bucket, counted in
/// `aspen_client_address_fallbacks_total{reason="missing"}`. The deploy
/// README warns about both.
pub fn counts_failures(address: &str) -> bool {
    address != UNKNOWN_ADDRESS
}

/// The limiter key of a client address: `ip:` and its [`address_key`].
pub fn ip_key(ip: IpAddr) -> String {
    format!("ip:{}", address_key(ip))
}

/// The part of a client address one anonymous caller is counted by: the
/// whole IPv4 address, or an IPv6 address's /64. One customer usually gets
/// a whole /64, so keying by full IPv6 addresses would let one caller
/// rotate through 2^64 rate-limit buckets (decisions log, B3 review).
/// IPv4-mapped IPv6 addresses count as their IPv4 address.
pub fn address_key(ip: IpAddr) -> String {
    match ip.to_canonical() {
        IpAddr::V4(v4) => v4.to_string(),
        IpAddr::V6(v6) => {
            let s = v6.segments();
            let net = std::net::Ipv6Addr::new(s[0], s[1], s[2], s[3], 0, 0, 0, 0);
            format!("{net}/64")
        }
    }
}

fn declared_len(headers: &HeaderMap) -> Option<u64> {
    headers
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse().ok())
}

fn text(status: StatusCode, message: &str) -> Response {
    let mut r = (status, format!("{message}\n")).into_response();
    r.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    r.headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    r
}

/// The response for an [`AuthError`]: its status from core's wire mapping,
/// plus the `Basic` challenge on 401 and a `Retry-After` on 503.
pub fn refuse(e: AuthError) -> Response {
    let wire = e.to_wire();
    let status = match wire.surface {
        aspen_core::error::Surface::Http(s) => {
            StatusCode::from_u16(s).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR)
        }
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    let mut r = text(status, &wire.message);
    match e {
        AuthError::Unauthenticated => {
            r.headers_mut().insert(
                header::WWW_AUTHENTICATE,
                HeaderValue::from_static(CHALLENGE),
            );
        }
        AuthError::Unavailable => {
            r.headers_mut()
                .insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
        }
        AuthError::Forbidden | AuthError::NotFound => {}
    }
    r
}

fn refused(refusal: Refusal) -> Response {
    match refusal {
        Refusal::Auth(e) => refuse(e),
        Refusal::Limit(e) => limited(e),
        Refusal::Unavailable => {
            let mut r = text(
                StatusCode::SERVICE_UNAVAILABLE,
                "internal storage error; retry",
            );
            r.headers_mut()
                .insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
            r
        }
        Refusal::Busy(busy) => {
            let status = match busy {
                Busy::Caller { .. } => StatusCode::TOO_MANY_REQUESTS,
                Busy::Instance { .. } | Busy::Memory { .. } => StatusCode::SERVICE_UNAVAILABLE,
            };
            let mut r = text(status, &busy.to_string());
            r.headers_mut().insert(
                header::RETRY_AFTER,
                HeaderValue::from(RETRY_AFTER.as_secs()),
            );
            r
        }
    }
}

/// A request body that keeps its request's [`Slot`] until it is dropped.
struct Holding {
    inner: Body,
    _slot: Slot,
}

impl http_body::Body for Holding {
    type Data = Bytes;
    type Error = axum::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        Pin::new(&mut self.inner).poll_frame(cx)
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.size_hint()
    }
}

fn limited(e: LimitError) -> Response {
    match e {
        LimitError::Rate(after) | LimitError::Quota(after) | LimitError::AuthFailures(after) => {
            let mut r = text(StatusCode::TOO_MANY_REQUESTS, &e.to_string());
            r.headers_mut().insert(
                header::RETRY_AFTER,
                HeaderValue::from(after.0.as_secs().max(1)),
            );
            r
        }
        LimitError::TooLarge { .. } => text(StatusCode::PAYLOAD_TOO_LARGE, &e.to_string()),
    }
}

/// Why a push body stopped: it went over the per-push cap or the caller's
/// byte budget. Receive-pack sees it as the body's read error, before the
/// pack is complete, and reports the push as failed; nothing is stored.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct PushBodyError(pub LimitError);

type Charge = Box<dyn Fn(u64) -> Result<(), LimitError> + Send + Sync>;

/// Counts the bytes of one push's pack against its cap and charges them to
/// the caller's byte budget, as they arrive ([`Access::admit`]). Every byte
/// of the request must pass through [`PushMeter::charge`] before the push
/// reads it, so an over-quota pack fails before it is complete and is
/// never stored.
pub struct PushMeter {
    read: u64,
    max: Option<u64>,
    charge: Charge,
}

impl PushMeter {
    /// A meter that refuses more than `max` bytes in all (no cap when
    /// `None`) and passes every charge to `charge`.
    pub(crate) fn new(
        max: Option<u64>,
        charge: impl Fn(u64) -> Result<(), LimitError> + Send + Sync + 'static,
    ) -> Self {
        Self {
            read: 0,
            max,
            charge: Box::new(charge),
        }
    }

    /// Charges `n` more bytes; an error ends the push.
    pub fn charge(&mut self, n: u64) -> Result<(), PushBodyError> {
        self.read = self.read.saturating_add(n);
        if let Some(max) = self.max
            && self.read > max
        {
            return Err(PushBodyError(LimitError::TooLarge { max }));
        }
        (self.charge)(n).map_err(PushBodyError)
    }
}

impl fmt::Debug for PushMeter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PushMeter")
            .field("read", &self.read)
            .field("max", &self.max)
            .finish_non_exhaustive()
    }
}

/// The [`PushMeter`] of a push whose body has a `Content-Encoding`, left
/// in the request's extensions by [`guard`] for the handler that decodes
/// the body: counting the bytes as sent would let a small compressed body
/// inflate past the cap and the budget. Receive-pack takes it
/// ([`EncodedPushMeter::take`]) and charges every byte through it.
#[derive(Debug, Clone)]
pub(crate) struct EncodedPushMeter(Arc<std::sync::Mutex<Option<PushMeter>>>);

impl EncodedPushMeter {
    /// The meter, once.
    pub(crate) fn take(&self) -> Option<PushMeter> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
    }
}

struct PushBody {
    inner: Body,
    meter: PushMeter,
}

impl http_body::Body for PushBody {
    type Data = Bytes;
    type Error = axum::BoxError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        let frame = match Pin::new(&mut self.inner).poll_frame(cx) {
            Poll::Ready(Some(Ok(frame))) => frame,
            Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e.into()))),
            other => return other.map_err(Into::into),
        };
        if let Some(data) = frame.data_ref()
            && let Err(e) = self.meter.charge(data.len() as u64)
        {
            return Poll::Ready(Some(Err(e.into())));
        }
        Poll::Ready(Some(Ok(frame)))
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.size_hint()
    }
}

impl fmt::Debug for PushBody {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PushBody")
            .field("meter", &self.meter)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Invariant (B3 review obligation 2): one anonymous IPv6 caller cannot
    /// rotate through rate-limit buckets within its /64; IPv4, and IPv4
    /// mapped into IPv6, count whole addresses.
    #[test]
    fn anonymous_ipv6_callers_are_keyed_by_their_slash_64() {
        let key = |s: &str| address_key(s.parse().unwrap());
        assert_eq!(key("2001:db8:1:2:aaaa::1"), "2001:db8:1:2::/64");
        assert_eq!(key("2001:db8:1:2:ffff:ffff:ffff:ffff"), "2001:db8:1:2::/64");
        assert_ne!(key("2001:db8:1:3::1"), key("2001:db8:1:2::1"));
        assert_eq!(key("192.0.2.7"), "192.0.2.7");
        assert_eq!(key("::ffff:192.0.2.7"), "192.0.2.7");
        // IPv4 clients of a `[::]` listener arrive as mapped addresses,
        // which share one /64 (`::ffff:0:0/96`); each keeps its own key.
        let ip = |s: &str| ip_key(s.parse().unwrap());
        assert_eq!(ip("::ffff:192.0.2.7"), "ip:192.0.2.7");
        assert_ne!(ip("::ffff:192.0.2.7"), ip("::ffff:192.0.2.8"));
        assert_eq!(ip("::ffff:192.0.2.7"), ip("192.0.2.7"));
    }

    #[test]
    fn only_the_unknown_address_skips_the_failure_limit() {
        assert!(!counts_failures(UNKNOWN_ADDRESS));
        assert!(counts_failures(&ip_key("192.0.2.7".parse().unwrap())));
    }
}
