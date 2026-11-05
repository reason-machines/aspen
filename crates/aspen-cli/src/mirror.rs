//! `aspen import` and `aspen mirror`: set up and operate mirror mode
//! (node C4, `docs/MIRROR.md`).
//!
//! These commands open the metadata database (`DATABASE_URL`, applying
//! migrations) and the blob store (`ASPEN_BLOB_URL`) themselves and write
//! through the same push path the server uses, so they run beside a live
//! server. Credentials come from `ASPEN_MIRROR_*` variables
//! (`aspen_mirror::settings`). The running server does the continuous
//! work (sync passes and `mirror_push` jobs) once node C6's `aspen serve`
//! starts the mirror worker.

use std::path::PathBuf;
use std::sync::Arc;

use aspen_blob::AnyBlobStore;
use aspen_core::meta::MetaStore;
use aspen_core::name::{RefName, RepoName};
use aspen_core::repo::Repo;
use aspen_meta::SqlStore;
use aspen_mirror::{
    CredentialName, MirrorMode, MirrorStore, RefOutcome, Side, SyncReport, UpstreamUrl,
};
use aspen_pack::{MaxInflatedBytes, MaxObjectBytes};
use aspen_server::mirror::{ServerMirror, SqlMirrors};
use aspen_server::receive::ReceiveConfig;
use clap::{Args, Subcommand, ValueEnum};

type Service = ServerMirror<SqlStore, AnyBlobStore, SqlMirrors>;

/// The stores and the mirror service a command runs on. The service holds
/// the server weakly, so the server is kept here for the command's life.
struct Opened {
    meta: SqlStore,
    mirror: Arc<Service>,
    _server: aspen_server::Server<SqlStore, AnyBlobStore>,
}

/// Where the commands find Aspen's stores and limits.
#[derive(Debug, Args)]
pub(crate) struct Stores {
    /// Metadata database: `postgres://...` (password in `PGPASSWORD`) or
    /// `sqlite:<path>`.
    #[arg(long, env = "DATABASE_URL", hide_env_values = true)]
    database_url: String,
    /// Pack store: `s3://<bucket>[/<prefix>]` or `file:///<path>`.
    #[arg(long, env = "ASPEN_BLOB_URL", hide_env_values = true)]
    blob_url: String,
    /// Local spool for packs in transit (a real disk, sized for twice the
    /// largest pack).
    #[arg(long, env = "ASPEN_WORK_DIR", hide_env_values = true, default_value_os_t = std::env::temp_dir().join("aspen"))]
    work_dir: PathBuf,
    /// Largest single object accepted from the upstream.
    #[arg(long, default_value_t = 512 << 20)]
    max_object_bytes: u64,
    /// Most one imported pack may inflate to.
    #[arg(long, default_value_t = 64 << 30)]
    max_inflated_bytes: u64,
}

/// What `aspen import` sets up.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub(crate) enum ImportMode {
    /// Keep following the upstream, and send pushes to it.
    Mirror,
    /// Copy once; Aspen is then the only home of the repository.
    Copy,
}

/// How a mirror's pushes reach the upstream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub(crate) enum PushThrough {
    /// Forward each push before accepting it (the upstream stays the
    /// source of truth).
    Proxy,
    /// Accept pushes, then push them to the upstream in the background
    /// (Aspen is the primary).
    Async,
}

/// `aspen mirror` subcommands.
#[derive(Debug, Subcommand)]
pub(crate) enum MirrorCommand {
    /// Lists mirrors, or one mirror's refs in conflict and its last error.
    Status {
        /// One repository.
        repo: Option<String>,
    },
    /// Runs one sync pass now.
    Sync {
        /// The repository.
        repo: String,
    },
    /// Resolves a conflict: keeps one side's value, which the next pass
    /// (run at once) pushes or imports.
    Resolve {
        /// The repository.
        repo: String,
        /// The ref in conflict, such as `refs/heads/main`.
        r#ref: String,
        /// The side whose value is kept.
        #[arg(long, value_parser = ["aspen", "upstream"])]
        keep: String,
    },
    /// Stops mirroring; the repository and its refs stay in Aspen.
    Remove {
        /// The repository.
        repo: String,
    },
}

/// The arguments of `aspen import`.
#[derive(Debug, Args)]
pub(crate) struct ImportArgs {
    /// The upstream: `github.com/org/repo`, or an `https://` URL.
    upstream: String,
    /// The Aspen repository name; defaults to the upstream's path
    /// (`org/repo`).
    #[arg(long)]
    name: Option<String>,
    /// `mirror` keeps following the upstream; `copy` imports once.
    #[arg(long, value_enum, default_value_t = ImportMode::Mirror)]
    mode: ImportMode,
    /// How pushes reach the upstream (mirror mode only).
    #[arg(long, value_enum, default_value_t = PushThrough::Proxy)]
    push_through: PushThrough,
    /// The configured credential to use (`ASPEN_MIRROR_CREDENTIALS`);
    /// omit for a public upstream.
    #[arg(long)]
    credential: Option<String>,
}

async fn open(stores: &Stores) -> Result<Opened, String> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    // The same rule as `aspen serve`: a password in the URL would show in
    // process listings and logs (decisions log, A8).
    crate::config::check_database_url(&stores.database_url).map_err(|e| e.to_string())?;
    let meta = SqlStore::connect(&stores.database_url)
        .await
        .map_err(|e| format!("database: {e}"))?;
    meta.migrate()
        .await
        .map_err(|e| format!("database migrations: {e}"))?;
    let blobs = aspen_blob::open(&stores.blob_url).map_err(|e| format!("blob store: {e}"))?;
    let max_object = MaxObjectBytes::new(stores.max_object_bytes)
        .ok_or("--max-object-bytes must be positive")?;
    let max_inflated = MaxInflatedBytes::new(stores.max_inflated_bytes)
        .ok_or("--max-inflated-bytes must be positive")?;
    let get = |k: &str| std::env::var(k).ok();
    let credentials = aspen_mirror::settings::credentials(get, |p: &str| {
        crate::secret_file::read(std::path::Path::new(p))
    })
    .map_err(|e| e.to_string())?;
    let config = aspen_mirror::settings::config(get, stores.work_dir.join("mirror"))?;
    let (server, mirror) = aspen_server::Server::new(
        meta.clone(),
        blobs,
        aspen_server::ServerConfig::new(max_object),
    )
    .with_receive_pack(ReceiveConfig::new(
        stores.work_dir.join("spool"),
        max_object,
        max_inflated,
    ))
    .map_err(|e| format!("spool directory: {e}"))?
    .with_mirror(SqlMirrors(meta.clone()), credentials, config)
    .map_err(|e| e.to_string())?;
    Ok(Opened {
        meta,
        mirror,
        _server: server,
    })
}

async fn repo(meta: &SqlStore, name: &str) -> Result<Repo, String> {
    let n = RepoName::new(name).map_err(|e| format!("repository name {name:?}: {e}"))?;
    meta.repo(&n)
        .await
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("no repository {name}"))
}

fn print_report(report: &SyncReport) -> bool {
    let n = |f: fn(&RefOutcome) -> bool| report.count(f);
    println!(
        "{} in sync, {} imported, {} pushed, {} in conflict, {} failed",
        n(|o| *o == RefOutcome::InSync),
        n(|o| *o == RefOutcome::Imported),
        n(|o| *o == RefOutcome::Pushed),
        n(|o| matches!(o, RefOutcome::Conflict(_))),
        n(|o| matches!(o, RefOutcome::Failed(_))),
    );
    for (name, outcome) in &report.refs {
        match outcome {
            RefOutcome::Conflict(c) => println!(
                "  conflict {name}: aspen {} upstream {}",
                show(c.aspen),
                show(c.upstream)
            ),
            RefOutcome::Failed(why) => println!("  failed {name}: {why}"),
            _ => {}
        }
    }
    for s in &report.skipped {
        println!("  skipped {s}: not storable or not in a writable namespace");
    }
    report.failures().is_empty()
}

fn show(v: Option<aspen_core::id::ObjectId>) -> String {
    v.map_or_else(|| "(absent)".to_owned(), |o| o.to_string())
}

/// `aspen import`.
pub(crate) async fn import(stores: Stores, args: ImportArgs) -> Result<bool, String> {
    let upstream = UpstreamUrl::parse(&args.upstream).map_err(|e| e.to_string())?;
    let name = args.name.unwrap_or_else(|| upstream.repo_path());
    let name = RepoName::new(&name).map_err(|e| format!("repository name {name:?}: {e}"))?;
    let credential = args
        .credential
        .as_deref()
        .map(CredentialName::new)
        .transpose()
        .map_err(|e| e.to_string())?;
    let mode = match args.push_through {
        PushThrough::Proxy => MirrorMode::Proxy,
        PushThrough::Async => MirrorMode::Async,
    };
    let opened = open(&stores).await?;
    let mirror = &opened.mirror;
    let (repo, report) = mirror
        .create(&name, upstream.clone(), mode, credential)
        .await
        .map_err(|e| e.to_string())?;
    println!(
        "imported {upstream} as {} (trunk {})",
        repo.name,
        repo.settings.trunk()
    );
    let ok = print_report(&report);
    if args.mode == ImportMode::Copy {
        if !ok {
            return Err(
                "some refs failed; the repository is left as a mirror so `aspen mirror sync` \
                 can finish the copy, then `aspen mirror remove`"
                    .into(),
            );
        }
        mirror
            .store()
            .delete_mirror(repo.id)
            .await
            .map_err(|e| e.to_string())?;
        println!("copied: {} is no longer linked to {upstream}", repo.name);
    } else {
        println!("mirror mode: {mode} push-through");
    }
    Ok(ok)
}

/// `aspen mirror ...`.
pub(crate) async fn command(stores: Stores, command: MirrorCommand) -> Result<bool, String> {
    let opened = open(&stores).await?;
    let (meta, mirror) = (&opened.meta, &opened.mirror);
    match command {
        MirrorCommand::Status { repo: None } => {
            for m in mirror.store().mirrors().await.map_err(|e| e.to_string())? {
                let name = meta
                    .repo_by_id(m.repo)
                    .await
                    .map_err(|e| e.to_string())?
                    .map_or_else(|| m.repo.to_string(), |r| r.name.to_string());
                let conflicts = mirror
                    .store()
                    .ledger(m.repo)
                    .await
                    .map_err(|e| e.to_string())?
                    .iter()
                    .filter(|(_, e)| e.conflict.is_some())
                    .count();
                println!(
                    "{name}\t{}\t{}\tconflicts {conflicts}\t{}",
                    m.mode,
                    m.upstream,
                    m.last_error.as_deref().unwrap_or("ok")
                );
            }
            Ok(true)
        }
        MirrorCommand::Status { repo: Some(r) } => {
            let repo = repo(meta, &r).await?;
            let m = mirror
                .store()
                .mirror(repo.id)
                .await
                .map_err(|e| e.to_string())?
                .ok_or_else(|| format!("{r} is not a mirror"))?;
            println!("upstream {}\nmode {}", m.upstream, m.mode);
            println!(
                "credential {}",
                m.credential.as_ref().map_or("(none)", |c| c.as_str())
            );
            println!(
                "last sync {}",
                m.last_sync_at
                    .map_or_else(|| "never".to_owned(), |t| format!("{} us", t.0))
            );
            println!("last error {}", m.last_error.as_deref().unwrap_or("(none)"));
            let mut ok = true;
            for (name, e) in mirror
                .store()
                .ledger(repo.id)
                .await
                .map_err(|e| e.to_string())?
            {
                if let Some(c) = e.conflict {
                    ok = false;
                    println!(
                        "conflict {name}: agreed {} aspen {} upstream {}",
                        show(e.synced),
                        show(c.aspen),
                        show(c.upstream)
                    );
                }
            }
            Ok(ok)
        }
        MirrorCommand::Sync { repo: r } => {
            let repo = repo(meta, &r).await?;
            let report = mirror.sync(&repo).await.map_err(|e| e.to_string())?;
            Ok(print_report(&report))
        }
        MirrorCommand::Resolve {
            repo: r,
            r#ref,
            keep,
        } => {
            let repo = repo(meta, &r).await?;
            let name = RefName::new(&r#ref).map_err(|e| format!("ref {ref:?}: {e}"))?;
            let side: Side = keep.parse()?;
            mirror
                .resolve(&repo, &name, side)
                .await
                .map_err(|e| e.to_string())?;
            let report = mirror.sync(&repo).await.map_err(|e| e.to_string())?;
            Ok(print_report(&report))
        }
        MirrorCommand::Remove { repo: r } => {
            let repo = repo(meta, &r).await?;
            if !mirror
                .store()
                .delete_mirror(repo.id)
                .await
                .map_err(|e| e.to_string())?
            {
                return Err(format!("{r} is not a mirror"));
            }
            println!("{r} is no longer a mirror; its refs stay");
            Ok(true)
        }
    }
}
