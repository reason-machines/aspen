//! Closing the connections still open when the shutdown drain runs out
//! (#140).
//!
//! After `shutdown`, `aspen_server::listen::serve` stops accepting and
//! waits for every request in flight; hyper closes each connection once its
//! last request ends. A page's event stream lasts up to its age limit (10
//! minutes by default, far past the drain), and each connection
//! runs in its own task, so returning from `serve` early would leave those
//! connections served until the process exits, through the last merge.
//! Instead, every request passes through [`cut_off`], and at the drain
//! deadline [`Cutoff::fire`] ends the ones still running:
//!
//! - a request with no response yet is dropped and answered `503`;
//! - a response body still streaming fails, which makes hyper close the
//!   connection (HTTP/1.1) or reset the stream (HTTP/2). A body of known
//!   size is already in memory and is left alone, so it keeps its
//!   `Content-Length`.
//!
//! With nothing left in flight, `serve` returns.

use axum::body::{Body, HttpBody};
use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use futures_util::StreamExt;
use tokio::sync::watch;

/// Ends every request in flight through [`cut_off`] when fired.
pub(crate) struct Cutoff(watch::Sender<bool>);

/// The receiving side of a [`Cutoff`], the state of [`cut_off`].
#[derive(Clone)]
pub(crate) struct Watch(watch::Receiver<bool>);

impl Cutoff {
    pub(crate) fn new() -> (Self, Watch) {
        let (tx, rx) = watch::channel(false);
        (Self(tx), Watch(rx))
    }

    /// Ends every request in flight, and answers every later one `503`.
    pub(crate) fn fire(&self) {
        self.0.send_replace(true);
    }
}

impl Watch {
    /// Resolves once the cutoff fires; never, if its [`Cutoff`] is
    /// dropped unfired.
    async fn fired(mut self) {
        if self.0.wait_for(|fired| *fired).await.is_err() {
            std::future::pending::<()>().await;
        }
    }
}

/// Middleware: runs the request until it ends or the cutoff fires.
pub(crate) async fn cut_off(State(cutoff): State<Watch>, request: Request, next: Next) -> Response {
    let response = tokio::select! {
        response = next.run(request) => response,
        () = cutoff.clone().fired() => {
            return (StatusCode::SERVICE_UNAVAILABLE, "aspen-place is shutting down\n")
                .into_response();
        }
    };
    if response.body().size_hint().exact().is_some() {
        return response;
    }
    let (parts, body) = response.into_parts();
    let data = body.into_data_stream();
    let fired = Box::pin(cutoff.fired());
    let body = futures_util::stream::unfold(Some((data, fired)), |state| async move {
        let (mut data, mut fired) = state?;
        tokio::select! {
            chunk = data.next() => {
                let chunk = chunk?.map_err(axum::BoxError::from);
                Some((chunk, Some((data, fired))))
            }
            () = &mut fired => Some((Err("aspen-place is shutting down".into()), None)),
        }
    });
    Response::from_parts(parts, Body::from_stream(body))
}
