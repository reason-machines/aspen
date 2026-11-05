//! Reading a smart-HTTP request body: bounded, and gunzipped when the
//! client compressed it (`Content-Encoding: gzip`, which Git uses for large
//! upload-pack requests and JGit for every push).

use std::future::Future;
use std::io::Read;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use axum::body::Body;
use axum::http::HeaderMap;
use bytes::Bytes;
use futures_util::StreamExt;
use http_body::Frame;

use crate::access::PushMeter;

/// Why a body could not be read.
#[derive(Debug, thiserror::Error)]
pub(crate) enum BodyError {
    /// Larger than the limit, before or after decompression.
    #[error("request body larger than {0} bytes")]
    TooLarge(usize),
    /// A `Content-Encoding` other than `gzip` or identity.
    #[error("unsupported content encoding {0:?}")]
    Encoding(String),
    /// The connection failed, or the gzip stream is corrupt.
    #[error("cannot read the request body: {0}")]
    Read(String),
}

/// The whole body, at most `limit` bytes both as sent and as decoded.
pub(crate) async fn read(
    headers: &HeaderMap,
    body: Body,
    limit: usize,
) -> Result<Vec<u8>, BodyError> {
    let mut raw = Vec::new();
    let mut stream = body.into_data_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| BodyError::Read(e.to_string()))?;
        if raw.len() + chunk.len() > limit {
            return Err(BodyError::TooLarge(limit));
        }
        raw.extend_from_slice(&chunk);
    }
    match Encoding::of(headers).map_err(BodyError::Encoding)? {
        Encoding::Identity => Ok(raw),
        Encoding::Gzip => {
            // Inflating up to 16 MiB is CPU work: keep it off the async
            // workers.
            tokio::task::spawn_blocking(move || gunzip(&raw, limit))
                .await
                .map_err(|e| BodyError::Read(e.to_string()))?
        }
    }
}

/// How a request body is encoded (`Content-Encoding`), for the encodings
/// Aspen reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Encoding {
    /// No header, an empty one or `identity`.
    Identity,
    /// `gzip` or `x-gzip`.
    Gzip,
}

impl Encoding {
    /// The encoding `headers` declare: `Err` with the name of one Aspen
    /// does not read.
    pub(crate) fn of(headers: &HeaderMap) -> Result<Self, String> {
        let encoding = headers
            .get("content-encoding")
            .map(|v| v.to_str().unwrap_or("?").trim().to_ascii_lowercase());
        match encoding.as_deref() {
            None | Some("" | "identity") => Ok(Self::Identity),
            Some("gzip" | "x-gzip") => Ok(Self::Gzip),
            Some(other) => Err(other.to_owned()),
        }
    }
}

/// Why a gzip push body stopped: more bytes arrived than any push the
/// service accepts could take, however little they inflate to.
#[derive(Debug, thiserror::Error)]
#[error("compressed push body larger than {0} bytes as sent")]
pub(crate) struct SentTooMuch(u64);

/// A push body the client sent gzip-compressed, read inflated
/// (decisions log, "#134"). Each push is charged to its [`PushMeter`] (the
/// per-push cap and the caller's byte budget) as the larger of the bytes
/// sent so far and the bytes inflated so far: inflated, so a small body
/// cannot unpack into more than the cap or the budget allows; sent, so a
/// stream that inflates to little still pays for what it costs to read.
/// Without a meter, the sent bytes are still bounded by `max_sent`.
pub(crate) struct GzipPush<R: Read> {
    /// The body before its first read. The decoder is made then: making
    /// it reads the gzip header, which must happen on the thread that
    /// reads the body (a blocking one), not where the reader is built.
    unread: Option<Counted<R>>,
    decoder: Option<flate2::read::GzDecoder<Counted<R>>>,
}

impl<R: Read> GzipPush<R> {
    /// Reads `sent` as gzip, charging `meter` (if any) and refusing more
    /// than `max_sent` bytes as sent.
    pub(crate) fn new(sent: R, meter: Option<PushMeter>, max_sent: u64) -> Self {
        Self {
            unread: Some(Counted {
                inner: sent,
                sent: 0,
                inflated: 0,
                charged: 0,
                max_sent,
                meter,
            }),
            decoder: None,
        }
    }
}

impl<R: Read> Read for GzipPush<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if let Some(counted) = self.unread.take() {
            // Only the first gzip member is read, as `git http-backend`
            // does (it stops at zlib's end of stream); JGit sends one.
            self.decoder = Some(flate2::read::GzDecoder::new(counted));
        }
        let Some(decoder) = self.decoder.as_mut() else {
            return Ok(0);
        };
        let n = decoder.read(buf)?;
        let counted = decoder.get_mut();
        counted.inflated += n as u64;
        counted.settle()?;
        Ok(n)
    }
}

/// The compressed side of a [`GzipPush`]: counts what is sent, and keeps
/// the meter both sides charge.
struct Counted<R> {
    inner: R,
    sent: u64,
    inflated: u64,
    /// What the meter has been charged: `max(sent, inflated)` as of the
    /// last [`Counted::settle`].
    charged: u64,
    max_sent: u64,
    meter: Option<PushMeter>,
}

impl<R> Counted<R> {
    /// Charges the meter for whatever `max(sent, inflated)` grew by.
    fn settle(&mut self) -> std::io::Result<()> {
        let seen = self.sent.max(self.inflated);
        if seen > self.charged {
            if let Some(meter) = self.meter.as_mut() {
                meter
                    .charge(seen - self.charged)
                    .map_err(std::io::Error::other)?;
            }
            self.charged = seen;
        }
        Ok(())
    }
}

impl<R: Read> Read for Counted<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.sent += n as u64;
        if self.sent > self.max_sent {
            return Err(std::io::Error::other(SentTooMuch(self.max_sent)));
        }
        self.settle()?;
        Ok(n)
    }
}

/// Inflates `raw`, refusing more than `limit` decoded bytes.
fn gunzip(raw: &[u8], limit: usize) -> Result<Vec<u8>, BodyError> {
    let mut out = Vec::new();
    // One byte past the limit tells "exactly at the limit" from "over".
    flate2::read::GzDecoder::new(raw)
        .take(limit as u64 + 1)
        .read_to_end(&mut out)
        .map_err(|e| BodyError::Read(e.to_string()))?;
    if out.len() > limit {
        return Err(BodyError::TooLarge(limit));
    }
    Ok(out)
}

/// How fast a Git request body must arrive ([`IdleBody`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BodyPace {
    /// [`crate::ServerConfig::body_idle_timeout`].
    pub(crate) idle: Duration,
    /// [`crate::ServerConfig::body_min_bytes_per_sec`]; `0` is no rule.
    pub(crate) min_bytes_per_sec: u64,
    /// [`crate::ServerConfig::body_rate_grace`].
    pub(crate) grace: Duration,
}

/// A request body that fails once a read has waited `idle` without a
/// frame arriving ([`crate::ServerConfig::body_idle_timeout`]), or once it
/// has arrived slower than `min_bytes_per_sec` on average over the time
/// the server waited for it, after the first `grace` of waiting
/// ([`crate::ServerConfig::body_min_bytes_per_sec`], issue #118): a client
/// that sends a byte just before every idle timeout still cannot hold a
/// request (and, for a push, a blocking thread) for long.
///
/// Both clocks run only while the handler polls for data and none is
/// ready, and stop when a frame arrives. So a handler that reads in a
/// loop (as the Git handlers do, writing each chunk before the next read)
/// is not charged for the time between its reads. A handler that stops
/// polling while a read is pending (one branch of a `select!`) leaves the
/// clocks running, and its next poll can fail at once.
pub(crate) struct IdleBody {
    inner: Body,
    pace: BodyPace,
    /// Time spent waiting for frames, before the current wait.
    waited: Duration,
    /// Data bytes received.
    received: u64,
    /// When the current wait began.
    since: Option<tokio::time::Instant>,
    timer: Option<Pin<Box<tokio::time::Sleep>>>,
}

impl IdleBody {
    pub(crate) fn new(inner: Body, pace: BodyPace) -> Self {
        Self {
            inner,
            pace,
            waited: Duration::ZERO,
            received: 0,
            since: None,
            timer: None,
        }
    }

    /// When the current wait (begun at `since`) makes the body too slow:
    /// the total wait at which `received` falls below the minimum rate,
    /// and never before the grace period.
    fn slow_at(&self, since: tokio::time::Instant) -> Option<tokio::time::Instant> {
        let rate = self.pace.min_bytes_per_sec;
        if rate == 0 {
            return None;
        }
        let enough_for = Duration::from_secs_f64(self.received as f64 / rate as f64);
        let fails_after = enough_for.max(self.pace.grace);
        Some(since + fails_after.saturating_sub(self.waited))
    }
}

/// The error an idle body ends with.
#[derive(Debug, thiserror::Error)]
#[error("request body idle for more than {} s", .0.as_secs())]
pub(crate) struct BodyIdle(Duration);

/// The error a body that arrives too slowly ends with.
#[derive(Debug, thiserror::Error)]
#[error("request body too slow: less than {0} bytes/s on average")]
pub(crate) struct BodyTooSlow(u64);

impl http_body::Body for IdleBody {
    type Data = Bytes;
    type Error = axum::BoxError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        let this = &mut *self;
        match Pin::new(&mut this.inner).poll_frame(cx) {
            Poll::Ready(frame) => {
                if let Some(since) = this.since.take() {
                    this.waited += since.elapsed();
                }
                this.timer = None;
                if let Some(Ok(f)) = &frame
                    && let Some(data) = f.data_ref()
                {
                    this.received += data.len() as u64;
                }
                Poll::Ready(frame.map(|f| f.map_err(Into::into)))
            }
            Poll::Pending => {
                let since = *this.since.get_or_insert_with(tokio::time::Instant::now);
                let idle_at = since + this.pace.idle;
                let (deadline, slow) = match this.slow_at(since) {
                    Some(slow_at) if slow_at <= idle_at => (slow_at, true),
                    _ => (idle_at, false),
                };
                let timer = this
                    .timer
                    .get_or_insert_with(|| Box::pin(tokio::time::sleep_until(deadline)));
                if timer.deadline() != deadline {
                    timer.as_mut().reset(deadline);
                }
                match timer.as_mut().poll(cx) {
                    Poll::Ready(()) if slow => {
                        Poll::Ready(Some(Err(BodyTooSlow(this.pace.min_bytes_per_sec).into())))
                    }
                    Poll::Ready(()) => Poll::Ready(Some(Err(BodyIdle(this.pace.idle).into()))),
                    Poll::Pending => Poll::Pending,
                }
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.size_hint()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    /// A body fed by hand: each `send` is one chunk; dropping the sender
    /// ends it.
    fn hand_fed() -> (tokio::sync::mpsc::Sender<Bytes>, Body) {
        let (tx, rx) = tokio::sync::mpsc::channel::<Bytes>(1);
        let stream = futures_util::stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|b| (Ok::<_, std::io::Error>(b), rx))
        });
        (tx, Body::from_stream(stream))
    }

    /// Invariant: a body whose client stops sending fails after the idle
    /// timeout instead of holding the request forever; one that keeps
    /// sending is read to its end however long it takes in total.
    #[tokio::test]
    async fn an_idle_body_times_out_and_a_slow_one_does_not() {
        let timeout = Duration::from_millis(300);
        for quiet_at_the_end in [true, false] {
            let (tx, body) = hand_fed();
            let pace = BodyPace {
                idle: timeout,
                min_bytes_per_sec: 0,
                grace: Duration::ZERO,
            };
            let body = Body::new(IdleBody::new(body, pace));
            let reader = tokio::spawn(async move { read(&HeaderMap::new(), body, 1 << 20).await });
            // 600 ms in total, twice the timeout, but never idle that long.
            for _ in 0..6 {
                tokio::time::sleep(Duration::from_millis(100)).await;
                tx.send(Bytes::from_static(b"x")).await.unwrap();
            }
            if quiet_at_the_end {
                let err = tokio::time::timeout(Duration::from_secs(5), reader)
                    .await
                    .expect("an idle body never timed out")
                    .unwrap()
                    .unwrap_err();
                assert!(
                    matches!(err, BodyError::Read(ref m) if m.contains("idle")),
                    "{err}"
                );
                drop(tx);
            } else {
                drop(tx);
                assert_eq!(reader.await.unwrap().unwrap(), b"xxxxxx");
            }
        }
    }

    /// Invariant (issue #118): a body that keeps trickling, never idle
    /// for long, fails once it averages less than the minimum rate after
    /// the grace period; one that keeps up is read to its end.
    #[tokio::test(start_paused = true)]
    async fn a_body_slower_than_the_minimum_rate_fails_after_the_grace() {
        let pace = BodyPace {
            idle: Duration::from_secs(60),
            min_bytes_per_sec: 100,
            grace: Duration::from_secs(10),
        };
        for (chunk, expect_slow) in [(10usize, true), (200, false)] {
            let (tx, body) = hand_fed();
            let body = Body::new(IdleBody::new(body, pace));
            let started = tokio::time::Instant::now();
            let reader = tokio::spawn(async move { read(&HeaderMap::new(), body, 1 << 20).await });
            let feeder = tokio::spawn(async move {
                // 30 s of one chunk a second.
                for _ in 0..30 {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    if tx.send(Bytes::from(vec![b'x'; chunk])).await.is_err() {
                        return;
                    }
                }
            });
            let result = reader.await.unwrap();
            if expect_slow {
                let err = result.unwrap_err();
                assert!(
                    matches!(err, BodyError::Read(ref m) if m.contains("too slow")),
                    "{err}"
                );
                let after = started.elapsed();
                assert!(
                    after >= Duration::from_secs(10) && after < Duration::from_secs(11),
                    "refused after {after:?}"
                );
            } else {
                assert_eq!(result.unwrap().len(), 200 * 30);
            }
            feeder.await.unwrap();
        }
    }

    fn gz(data: &[u8]) -> Vec<u8> {
        let mut e = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::best());
        e.write_all(data).unwrap();
        e.finish().unwrap()
    }

    fn headers(encoding: Option<&str>) -> HeaderMap {
        let mut h = HeaderMap::new();
        if let Some(e) = encoding {
            h.insert("content-encoding", e.parse().unwrap());
        }
        h
    }

    /// Invariant: the limit bounds the decoded size too, so a small gzip
    /// bomb cannot make the server buffer more than the limit.
    #[tokio::test]
    async fn bodies_are_bounded_before_and_after_gunzip() {
        let data = vec![b'a'; 1000];
        let plain = read(&headers(None), Body::from(data.clone()), 1000)
            .await
            .unwrap();
        assert_eq!(plain, data);
        let z = read(&headers(Some("gzip")), Body::from(gz(&data)), 1000)
            .await
            .unwrap();
        assert_eq!(z, data);
        let bomb = gz(&vec![0u8; 1 << 20]);
        assert!(bomb.len() < 4096, "compressed below the limit");
        assert!(matches!(
            read(&headers(Some("gzip")), Body::from(bomb), 4096).await,
            Err(BodyError::TooLarge(4096))
        ));
        assert!(matches!(
            read(&headers(None), Body::from(vec![0u8; 1001]), 1000).await,
            Err(BodyError::TooLarge(1000))
        ));
        assert!(matches!(
            read(&headers(Some("br")), Body::from(vec![0u8; 3]), 1000).await,
            Err(BodyError::Encoding(_))
        ));
    }
}
