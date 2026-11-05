//! Entry points into parsers that are private to this crate, for the fuzz
//! targets in `aspen-fuzz` (`fuzz/`, issue #139) and nothing else. The
//! `fuzzing` feature turns this module on; the `aspen` binary never does.
//!
//! Each function runs the code the server runs, unchanged, and only turns
//! its crate-private types into public ones:
//!
//! - [`ssh_command`]: the command line of an SSH `exec` request
//!   (`ssh::command`);
//! - [`read_body`]: an upload-pack request body, bounded as sent and as
//!   inflated (`body::read`);
//! - [`gzip_push`]: a gzip push body, read inflated and charged to a push
//!   meter (`body::GzipPush`).
//!
//! The route parser ([`crate::access::GitRoute`]) is public already.

use std::io::Read;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};

use aspen_proto::service::Service;
use axum::body::Body;
use axum::http::{HeaderMap, HeaderValue};
use bytes::Bytes;

use crate::access::{PushBodyError, PushMeter};
use crate::body::{self, BodyError, GzipPush, SentTooMuch};
use crate::ssh::command::{self, CommandError};

/// Longest SSH command accepted, in bytes.
pub const MAX_SSH_COMMAND_LEN: usize = command::MAX_COMMAND_LEN;

/// Why an SSH command is not served.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SshCommandError {
    /// Not a Git service Aspen runs, or shell syntax Aspen does not read.
    NotGit,
    /// A Git service with an argument Aspen cannot read.
    BadPath,
}

/// Parses an SSH `exec` command line as the SSH transport does: the
/// service and the repository path without its leading `/`.
pub fn ssh_command(command: &[u8]) -> Result<(Service, String), SshCommandError> {
    match command::parse(command) {
        Ok(c) => Ok((c.service, c.path)),
        Err(CommandError::NotGit) => Err(SshCommandError::NotGit),
        Err(CommandError::BadPath) => Err(SshCommandError::BadPath),
    }
}

/// Why [`read_body`] refused a body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadBodyError {
    /// Larger than the limit, as sent or as inflated.
    TooLarge,
    /// A `Content-Encoding` Aspen does not read, with its normalized name.
    Encoding(String),
    /// A gzip stream that does not inflate.
    Read(String),
}

static RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("a current-thread runtime")
});

/// Reads an upload-pack request body that arrives in `chunks`, with the
/// `Content-Encoding` header `encoding` (none when `None`), bounded to
/// `limit` bytes as sent and as inflated: `body::read`, which upload-pack
/// runs on every request.
///
/// # Panics
///
/// If `encoding` is not a valid header value (`HeaderValue::from_bytes`).
pub fn read_body(
    encoding: Option<&[u8]>,
    chunks: Vec<Vec<u8>>,
    limit: usize,
) -> Result<Vec<u8>, ReadBodyError> {
    let mut headers = HeaderMap::new();
    if let Some(e) = encoding {
        let value = HeaderValue::from_bytes(e).expect("a valid header value");
        headers.insert("content-encoding", value);
    }
    let stream = futures_util::stream::iter(
        chunks
            .into_iter()
            .map(|c| Ok::<_, std::io::Error>(Bytes::from(c))),
    );
    let read = RUNTIME.block_on(body::read(&headers, Body::from_stream(stream), limit));
    read.map_err(|e| match e {
        BodyError::TooLarge(_) => ReadBodyError::TooLarge,
        BodyError::Encoding(name) => ReadBodyError::Encoding(name),
        BodyError::Read(why) => ReadBodyError::Read(why),
    })
}

/// Why [`gzip_push`] stopped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GzipPushError {
    /// The push meter refused a charge: more than its cap, as sent or as
    /// inflated.
    OverCap,
    /// More bytes arrived than `max_sent`.
    SentTooMuch,
    /// The gzip stream does not inflate, or reading it failed.
    Corrupt(String),
}

/// What [`gzip_push`] read.
#[derive(Debug)]
pub struct GzipPushOutcome {
    /// The inflated body, read to its end, or why reading stopped.
    pub inflated: Result<Vec<u8>, GzipPushError>,
    /// Every byte charged to the meter (0 without one).
    pub charged: u64,
}

/// Reads `sent` to its end as receive-pack reads a gzip push body
/// (`body::GzipPush`): inflated, with at most `max_sent` bytes as sent,
/// and, when `meter_cap` is `Some`, charged to a push meter whose cap is
/// the inner value (`None` inside: a meter with no cap, only a budget,
/// which here always has room).
pub fn gzip_push<R: Read>(
    sent: R,
    meter_cap: Option<Option<u64>>,
    max_sent: u64,
) -> GzipPushOutcome {
    let charged = Arc::new(AtomicU64::new(0));
    let meter = meter_cap.map(|max| {
        let charged = Arc::clone(&charged);
        PushMeter::new(max, move |n| {
            charged.fetch_add(n, Ordering::Relaxed);
            Ok(())
        })
    });
    let mut out = Vec::new();
    let inflated = match GzipPush::new(sent, meter, max_sent).read_to_end(&mut out) {
        Ok(_) => Ok(out),
        Err(e) => Err(match e.get_ref() {
            Some(inner) if inner.is::<PushBodyError>() => GzipPushError::OverCap,
            Some(inner) if inner.is::<SentTooMuch>() => GzipPushError::SentTooMuch,
            _ => GzipPushError::Corrupt(e.to_string()),
        }),
    };
    GzipPushOutcome {
        inflated,
        charged: charged.load(Ordering::Relaxed),
    }
}
