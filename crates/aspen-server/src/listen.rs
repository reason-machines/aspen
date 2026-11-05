//! The HTTP listener: serves a router over HTTP/1.1 and cleartext HTTP/2
//! (prior knowledge) with timeouts that `axum::serve` does not set.
//!
//! `axum::serve` gives hyper no timer, so hyper's header-read timeout never
//! fires and a client that sends its request headers one byte at a time
//! holds a connection (and a task) for as long as it likes. [`serve`]
//! builds the connection with a timer and these limits ([`HttpLimits`]):
//!
//! - **Header read timeout** (HTTP/1.1): the request line and headers must
//!   arrive within this long of the first byte, or the connection closes.
//! - **HTTP/2 keep-alive**: a ping every interval; a peer that does not
//!   answer within the timeout is disconnected.
//! - **HTTP/2 streams**: at most this many requests at once per connection.
//!
//! Request bodies have their own idle timeout in the server
//! ([`crate::ServerConfig::body_idle_timeout`]), since a push is one long
//! body.

use std::convert::Infallible;
use std::future::Future;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::ConnectInfo;
use axum::http::Request;
use hyper::body::Incoming;
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use hyper_util::server::conn::auto::Builder;
use hyper_util::server::graceful::GracefulShutdown;
use tokio::net::TcpListener;
use tower_service::Service;

/// Connection limits for [`serve`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HttpLimits {
    /// Time from a request's first byte to the end of its headers
    /// (HTTP/1.1).
    pub header_read_timeout: Duration,
    /// Interval between HTTP/2 keep-alive pings.
    pub h2_keep_alive_interval: Duration,
    /// How long an HTTP/2 peer has to answer a ping.
    pub h2_keep_alive_timeout: Duration,
    /// Requests at once on one HTTP/2 connection.
    pub h2_max_concurrent_streams: u32,
}

impl Default for HttpLimits {
    /// 30 s for headers, a ping every 30 s answered within 20 s, and 100
    /// streams per connection.
    fn default() -> Self {
        Self {
            header_read_timeout: Duration::from_secs(30),
            h2_keep_alive_interval: Duration::from_secs(30),
            h2_keep_alive_timeout: Duration::from_secs(20),
            h2_max_concurrent_streams: 100,
        }
    }
}

/// Serves `router` on `listener` until `shutdown` completes, then waits
/// for the requests in flight to end. Handlers see the TCP peer as
/// `ConnectInfo<SocketAddr>`, as with
/// `into_make_service_with_connect_info`.
pub async fn serve(
    listener: TcpListener,
    router: Router,
    limits: HttpLimits,
    shutdown: impl Future<Output = ()> + Send,
) -> std::io::Result<()> {
    let mut builder = Builder::new(TokioExecutor::new());
    builder
        .http1()
        .timer(TokioTimer::new())
        .header_read_timeout(limits.header_read_timeout);
    builder
        .http2()
        .timer(TokioTimer::new())
        .keep_alive_interval(limits.h2_keep_alive_interval)
        .keep_alive_timeout(limits.h2_keep_alive_timeout)
        .max_concurrent_streams(limits.h2_max_concurrent_streams);
    let graceful = GracefulShutdown::new();
    let mut shutdown = std::pin::pin!(shutdown);
    loop {
        let (socket, peer) = tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok(accepted) => accepted,
                Err(e) if transient(&e) => {
                    // Out of file descriptors and the like: a listener
                    // must not stop over one failed accept.
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
                Err(e) => return Err(e),
            },
            () = &mut shutdown => break,
        };
        let _ = socket.set_nodelay(true);
        let router = router.clone();
        let service = hyper::service::service_fn(move |mut request: Request<Incoming>| {
            request.extensions_mut().insert(ConnectInfo(peer));
            let mut router = router.clone();
            async move {
                let response: Result<_, Infallible> = router.call(request.map(Body::new)).await;
                response
            }
        });
        let connection = builder
            .serve_connection_with_upgrades(TokioIo::new(socket), service)
            .into_owned();
        let connection = graceful.watch(connection);
        tokio::spawn(async move {
            // A client that hangs up or times out is not a server error.
            let _ = connection.await;
        });
    }
    drop(listener);
    graceful.shutdown().await;
    Ok(())
}

pub(crate) fn transient(e: &std::io::Error) -> bool {
    use std::io::ErrorKind::*;
    matches!(
        e.kind(),
        ConnectionAborted | ConnectionReset | Interrupted | WouldBlock | OutOfMemory
    ) || e.raw_os_error().is_some_and(|n| n == 23 || n == 24)
}
