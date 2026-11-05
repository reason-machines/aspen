//! Express and CDN packfile URIs (node C2): the server side of
//! [`aspen_express`].
//!
//! With [`Server::with_express_routes`], the fetch path signs a URI on an
//! express or CDN endpoint for each planned pack before it tries the blob
//! store's own presigned URL ([`crate::upload_pack`]'s delivery order):
//!
//! 1. an express/CDN URI, when a tier fits the client (scheme listed,
//!    past-end behaviour it survives) and has a healthy endpoint;
//! 2. the blob store's presigned URL, under B2's rules (Aspen's `/-/blobs`
//!    for every client, a bucket URL only for `git/2.56`+);
//! 3. the stitched inline pack.
//!
//! Express URIs come first even for clients that could use a bucket URL:
//! they are cacheable and shared across clients (one URL per pack per
//! signing window), so an edge or the regional cache absorbs a fetch storm
//! instead of the bucket. Packfile URIs are still only sent when
//! [`crate::ServerConfig::packfile_uris`] is on and the client asked for
//! them.
//!
//! With [`Server::with_express_route`], this server also serves express
//! URLs itself at `/-/express`, outside B3's access guard like
//! `/-/blobs`: the URL's signature is the credential, and a URL is only
//! signed for a fetch the guard allowed.

use aspen_core::fetch::{FetchRequest, PackfileUri};
use aspen_core::pack::PackRecord;
use aspen_express::{Client, PackKey};

use crate::Server;

/// Where an in-process express service is mounted.
pub const EXPRESS_PATH: &str = "/-/express";

/// The express or CDN URI for `pack`, if a tier can serve this client.
pub(crate) fn uri<M, B>(
    server: &Server<M, B>,
    pack: &PackRecord,
    request: &FetchRequest,
    tolerates_416: bool,
) -> Option<PackfileUri> {
    let routes = server.express_routes()?;
    let key = PackKey::of(pack);
    let url = routes.uri(
        &key,
        Client {
            schemes: &request.packfile_uri_protocols,
            tolerates_416,
        },
    )?;
    Some(PackfileUri {
        hash: pack.pack.checksum,
        uri: url.url,
    })
}
