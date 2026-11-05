//! The receive-pack arm of the Git handler (`crate::serve_git`):
//!
//! ```text
//! GET  /<repo>[.git][/-/<ref>]/info/refs?service=git-receive-pack
//! POST /<repo>[.git][/-/<ref>]/git-receive-pack
//! ```
//!
//! It runs behind [`crate::access::guard`] and takes the repository, the
//! endpoint, the branch scope, the caller (for per-ref permission) and the
//! push quota from the guard's [`Authorized`] extension, never from the
//! path. The guard already answered anonymous callers that may not push
//! (401, so Git asks for credentials).

use std::io;

use aspen_core::blob::BlobStore;
use aspen_core::meta::MetaStore;
use aspen_proto::service::{ProtocolVersion, Service};
use axum::body::Body;
use axum::http::request::Parts;
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::Response;
use futures_util::TryStreamExt;

use super::{ReceivePack, ServiceError};
use crate::access::{Authorized, EncodedPushMeter, Endpoint};
use crate::body::{Encoding, GzipPush};
use crate::plain;

fn git(content_type: &'static str, body: Vec<u8>) -> Response {
    let mut r = Response::new(Body::from(body));
    let h = r.headers_mut();
    h.insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
    h.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    r
}

fn service_error(e: ServiceError) -> Response {
    match e {
        ServiceError::Store(_) => {
            let mut r = plain(
                StatusCode::SERVICE_UNAVAILABLE,
                "internal storage error; retry",
            );
            r.headers_mut()
                .insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
            r
        }
    }
}

/// Serves a receive-pack endpoint of an authorized request.
pub(crate) async fn serve<M, B>(
    svc: &std::sync::Arc<ReceivePack<M, B>>,
    authorized: &Authorized,
    parts: &Parts,
    body: Body,
) -> Response
where
    M: MetaStore + Clone + 'static,
    B: BlobStore + Clone + 'static,
{
    let requested = ProtocolVersion::requested(
        parts
            .headers
            .get("git-protocol")
            .and_then(|v| v.to_str().ok()),
    );
    let scope = authorized.route.scope.as_ref();
    match authorized.route.endpoint {
        Endpoint::ReceivePackAdvertisement => {
            return match svc.advertise(&authorized.repo, scope, requested).await {
                Ok(body) => git(Service::ReceivePack.advertisement_content_type(), body),
                Err(e) => service_error(e),
            };
        }
        Endpoint::ReceivePack => {}
        Endpoint::UploadPackAdvertisement | Endpoint::UploadPack => {
            return plain(StatusCode::NOT_FOUND, "not found");
        }
    }
    if parts
        .headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        != Some(Service::ReceivePack.request_content_type())
    {
        return plain(StatusCode::UNSUPPORTED_MEDIA_TYPE, "bad content type");
    }
    // C Git never compresses a push body; JGit gzips every one (#134).
    let Ok(encoding) = Encoding::of(&parts.headers) else {
        return plain(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "unsupported push body encoding (identity and gzip are read)",
        );
    };
    // The quota's errors (`PushBodyError`, from the guard's body or from
    // `GzipPush`) surface as read errors of this body, which refuse the
    // pack.
    let stream = body.into_data_stream().map_err(io::Error::other);
    let sent = tokio_util::io::SyncIoBridge::new(tokio_util::io::StreamReader::new(stream));
    let reader: Box<dyn io::Read + Send> = match encoding {
        Encoding::Identity => Box::new(sent),
        Encoding::Gzip => {
            let meter = parts
                .extensions
                .get::<EncodedPushMeter>()
                .and_then(EncodedPushMeter::take);
            Box::new(GzipPush::new(sent, meter, svc.max_sent_bytes()))
        }
    };
    match svc
        .push(&authorized.repo, scope, authorized, authorized.push, reader)
        .await
    {
        Ok(outcome) => git(Service::ReceivePack.result_content_type(), outcome.response),
        Err(e) => service_error(e),
    }
}
