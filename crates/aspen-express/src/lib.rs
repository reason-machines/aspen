//! `aspen-express`: packfile-URI downloads through a regional pack cache
//! and, optionally, a CDN (`docs/PLAN.md`, fetch path step 3, node C2).
//!
//! Clients that set `git config fetch.uriprotocols https` get packfile URIs
//! instead of an inline pack (S3). Without express, those URIs point at the
//! blob store: Aspen's own `/-/blobs` route for a local store, or a
//! presigned bucket URL, which answers `416` past the end and so only goes
//! to git 2.56 and later. Express adds a URL that is
//!
//! - **signed and short-lived** ([`url`]): HMAC over the key ID, the expiry
//!   and the pack's key, which names its repository, so a URL opens one
//!   pack of one repository until it expires, and nothing else;
//! - **immutable and cacheable** ([`serve`]): the pack key embeds the
//!   pack's checksum, so one URL always returns the same bytes, with a
//!   strong `ETag`, `Range` support, the empty-`206` rule past the end, and
//!   `Cache-Control` that lets an edge share the response until the URL
//!   expires and never after;
//! - **shared across clients** ([`url::SigningPolicy`]): expiries are
//!   rounded up to a window, so every fetch of a pack in one window gets
//!   the same URL and an edge cache keyed on the URL holds one entry;
//! - **served from a regional disk cache** ([`ExpressConfig::cache`]) that
//!   keeps only packs whose bytes hash to the checksum in their key;
//! - **routed by consistent hashing** over express nodes, with health
//!   probes and tiers ([`routes`]), so a CDN can serve new clients, express
//!   nodes old ones, and an outage falls back to the stitched pack.
//!
//! The crate has two halves, used by different processes:
//!
//! - [`Routes`] is what an Aspen server uses to **emit** URIs.
//! - [`Express`] is the HTTP service that **serves** them, over any
//!   [`aspen_core::blob::BlobStore`]: inside the Aspen server at
//!   `/-/express`, or as a standalone regional node.
//!
//! Both need the same [`UrlKeys`]. [`settings`] reads both from the
//! environment, and `aspen express` runs a standalone node. Access control happens before a URL
//! exists: Aspen signs one only for a fetch that B3's guard allowed, so
//! a caller without a grant on a hidden repository never gets one, and the
//! express route itself (like `/-/blobs`) sits outside the guard, with the
//! URL's signature as its only credential. `docs/EXPRESS.md` covers
//! running it and putting a CDN in front.
//!
//! Layering: this crate depends only on `aspen-core` among Aspen's crates
//! (enforced by `cargo xtask check-deps`); the composition roots give it a
//! concrete blob store.

#![deny(missing_docs)]

mod cache;
pub mod key;
pub mod metrics;
pub mod routes;
pub mod serve;
pub mod settings;
pub mod url;

pub use cache::CacheConfig;
pub use key::PackKey;
pub use metrics::ExpressMetrics;
pub use routes::{Client, Endpoint, ProbeConfig, Routes, Tier};
pub use serve::{Express, ExpressConfig};
pub use url::{Refusal, SigningPolicy, UrlConfigError, UrlKey, UrlKeys};
