//! Fuzz targets for Aspen (`docs/PLAN.md`, node D3; section 4, item 5).
//!
//! Every target is a plain function from bytes to nothing that panics when
//! an invariant breaks. The same functions run in three places:
//!
//! - under libFuzzer, through the thin `fuzz_target!` wrappers in
//!   `fuzz/libfuzzer` (nightly toolchain, `cargo fuzz`; see `fuzz/README.md`);
//! - in required CI, where `tests/corpus.rs` replays every committed seed
//!   and every saved crash (`fuzz/corpus/<target>`, `fuzz/regressions/<target>`)
//!   on the stable toolchain in a few seconds;
//! - by hand: `cargo run -p aspen-fuzz -- run <target> <file>...`.
//!
//! [`TARGETS`] lists them.

#![deny(missing_docs)]

pub mod front;
pub mod input;
pub mod jwt;
pub mod pack;
pub mod packgen;
pub mod planner;
pub mod proto;
pub mod seeds;

/// An oracle path a structured target's input took. The generated targets
/// (`pack_ingest`, `stitch`, `planner`, and the front-end targets
/// `git_route`, `ssh_command`, `gzip_body` and `jwt`) report the paths each input reached,
/// and `tests/corpus.rs` requires a few hundred generated inputs to reach
/// every one, so a generator change that starves a check fails required CI
/// instead of leaving the check silently unreached.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Reached {
    /// Accepted, and the answer equals the model's exact one: the stored
    /// objects and completion bases (`pack_ingest`), the stitched union
    /// (`stitch`), the needed versions' objects with no chain skip
    /// (`planner`).
    ExactAccept,
    /// `pack_ingest`: refused a pack that holds an object twice.
    RefusedRepeat,
    /// `pack_ingest`: refused a pack with a base that resolves nowhere.
    RefusedUnresolved,
    /// `pack_ingest`: refused a pack with an object over the object limit.
    RefusedOverLimit,
    /// `pack_ingest`: refused a pack with a delta whose data is shorter than
    /// the 4 bytes Git requires (issue #162).
    RefusedShortDelta,
    /// `pack_ingest`: refused a pack with an entry that lies in a way that
    /// makes it certainly invalid (its header size or its zlib stream).
    RefusedLie,
    /// `pack_ingest`: refused a pack whose header count is not its entry
    /// count.
    RefusedCount,
    /// `stitch`: a damaged segment failed the stitch.
    RefusedDamage,
    /// `planner`: refused a request with no wants.
    RefusedNoWants,
    /// `planner`: asked for a version hidden from its view.
    MissingVersion,
    /// `git_route`: routed, from exactly where the route is served.
    RouteOk,
    /// `git_route`: refused as not a Git path.
    RouteNotGit,
    /// `git_route`: refused a Git path with a bad repository or scope name.
    RouteBadName,
    /// `git_route`: refused `info/refs` without a smart service.
    RouteDumbHttp,
    /// `ssh_command`: accepted a command, as a shell would read it.
    SshOk,
    /// `ssh_command`: refused as not a Git service (or shell syntax).
    SshNotGit,
    /// `ssh_command`: refused a Git service's path.
    SshBadPath,
    /// `gzip_body`: a reader returned the body.
    GzipOk,
    /// `gzip_body`: upload-pack refused a body over its limit.
    GzipTooLarge,
    /// `gzip_body`: upload-pack refused a `Content-Encoding`.
    GzipEncoding,
    /// `gzip_body`: a reader refused a stream that does not inflate.
    GzipCorrupt,
    /// `gzip_body`: the push meter refused more than its cap.
    GzipOverCap,
    /// `gzip_body`: receive-pack refused more bytes than its ceiling.
    GzipSentTooMuch,
    /// `jwt`: accepted a token, with the model's claims.
    JwtOk,
    /// `jwt`: refused a malformed token.
    JwtMalformed,
    /// `jwt`: refused an algorithm.
    JwtAlgorithm,
    /// `jwt`: refused a header with `crit`.
    JwtCritical,
    /// `jwt`: found no key for the token.
    JwtNoKey,
    /// `jwt`: refused a signature.
    JwtSignature,
    /// `jwt`: refused the named claim.
    JwtClaim(&'static str),
}

/// One fuzz target.
#[derive(Debug, Clone, Copy)]
pub struct Target {
    /// Its name: the libFuzzer binary, and the corpus directory.
    pub name: &'static str,
    /// Runs one input; panics on a broken invariant.
    pub run: fn(&[u8]),
}

/// Every target, by name.
pub const TARGETS: &[Target] = &[
    Target {
        name: "pkt_line",
        run: proto::pkt_line,
    },
    Target {
        name: "receive_request",
        run: proto::receive_request,
    },
    Target {
        name: "v2_command",
        run: proto::v2_command,
    },
    Target {
        name: "upload_v0",
        run: proto::upload_v0,
    },
    Target {
        name: "pack_bytes",
        run: pack::pack_bytes,
    },
    Target {
        name: "pack_ingest",
        run: pack::pack_ingest,
    },
    Target {
        name: "pack_formats",
        run: pack::pack_formats,
    },
    Target {
        name: "stitch",
        run: pack::stitch,
    },
    Target {
        name: "planner",
        run: planner::planner,
    },
    Target {
        name: "git_route",
        run: front::git_route,
    },
    Target {
        name: "ssh_command",
        run: front::ssh_command,
    },
    Target {
        name: "gzip_body",
        run: front::gzip_body,
    },
    Target {
        name: "jwt",
        run: jwt::jwt,
    },
];

/// The target called `name`.
pub fn target(name: &str) -> Option<&'static Target> {
    TARGETS.iter().find(|t| t.name == name)
}
