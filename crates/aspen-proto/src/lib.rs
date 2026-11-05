//! The Git wire protocol, as Aspen speaks it.
//!
//! | Module | What |
//! |---|---|
//! | [`pkt`] | pkt-line framing: decode, encode, `ERR`, a blocking reader |
//! | [`sideband`] | side-band multiplexing (bands 1, 2, 3) |
//! | [`caps`] | capability lists and the `agent` value |
//! | [`service`] | services, protocol-version negotiation, smart-HTTP framing |
//! | [`receive`] | protocol v0/v1 `git-receive-pack`: advertisement, request, report |
//! | [`v2`] | protocol v2 `git-upload-pack`: advertisement, `ls-refs`, `fetch` |
//! | [`upload_v0`] | protocol v0/v1 `git-upload-pack`: advertisement, want/have negotiation, pack |
//!
//! The crate is sans-I/O: parsers consume [`pkt::Packet`]s and writers
//! append to a `Vec<u8>`, so the same code serves smart HTTP (`aspen-server`
//! with axum), SSH (node C3) and the tests here, which drive stock `git`
//! through its `ext::` transport (`tests/interop.rs`). It depends only on
//! `aspen-core`, whose types it parses into and renders from:
//! [`aspen_core::refs::RefCommand`] for push commands,
//! [`aspen_core::fetch::FetchRequest`] for fetches, and
//! [`aspen_core::error::WireError`] for every error a client sees. It holds
//! no policy: which refs to advertise, what to accept and which packs to
//! send are decided by the receive-pack (B1) and upload-pack (B2) services.
//!
//! What Aspen speaks, and why (`docs/PLAN.md`, section 2):
//!
//! - **Push** is protocol v0, or v1 when asked; Git has no v2 push. The
//!   advertisement shows trunk (plus the branch of a branch-scoped URL) and
//!   up to a few earlier trunk tips as `.have` lines (S2).
//! - **Fetch** is protocol v2: `ls-refs` with `ref-prefix`, and `fetch`
//!   with `shallow` and optionally `packfile-uris`; `filter` is never
//!   advertised (S3, S5). Clients without v2 fetch (libgit2, go-git) get
//!   v0 upload-pack ([`upload_v0`]): the same refs push shows, a
//!   `multi_ack_detailed`/`no-done` or single-`ACK` negotiation, shallow
//!   and deepen, and the stitched pack (v0 has no packfile URIs).
//! - Request parsers bound the bytes they buffer, check every limit on the
//!   packet that crosses it, and refuse a ref name the report could not
//!   echo on one line.
//! - Every server-authored message goes through [`pkt::one_line`], so no
//!   reason text can break the framing or forge a protocol line.

#![deny(missing_docs)]

pub mod caps;
pub mod pkt;
pub mod receive;
pub mod service;
pub mod sideband;
pub mod upload_v0;
pub mod v2;
