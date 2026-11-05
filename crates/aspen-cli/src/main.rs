//! The `aspen` command-line tool.
//!
//! - `aspen serve` runs the Git server (smart HTTP: push, fetch, packfile
//!   URIs) configured from the environment, the contract in
//!   `deploy/README.md`: PostgreSQL or SQLite metadata, any supported blob
//!   store, token and OIDC access control.
//! - `aspen dev` runs the same server on one machine with SQLite and local
//!   disk, listening on loopback only and letting anyone there push.
//! - `aspen express` runs a standalone express node (node C2): it serves
//!   signed packfile-URI downloads from the blob store through a regional
//!   disk cache. See `docs/EXPRESS.md`.
//! - `aspen migrate`, `aspen repo create`, `aspen token ...` and
//!   `aspen key ...` administer the metadata database.
//! - `aspen import` imports a repository from GitHub, GitLab or any
//!   smart-HTTP server, as a mirror or a one-time copy, and `aspen mirror`
//!   shows, syncs, resolves and removes mirrors (node C4). See
//!   `docs/MIRROR.md`.
//! - `aspen doctor jobs` lists the background job queue and requeues or
//!   resolves dead jobs (node C5). See `docs/OBSERVABILITY.md`.
//! - `aspen healthcheck` checks a running server, for container health
//!   checks in images that have no `curl`.
//!
//! Every command logs to stderr and, when an OTLP endpoint is configured,
//! exports traces ([`telemetry`]).

mod admin;
mod config;
mod doctor;
mod healthcheck;
mod mirror;
mod secret_file;
mod serve;
mod telemetry;

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Duration;

use aspen_auth::{Grant, PolicyConfig};
use aspen_core::name::RepoName;
use clap::{Parser, Subcommand};

use config::{ServeConfig, Switch};
use serve::{BoxError, Startup};

#[derive(Debug, Parser)]
#[command(name = "aspen", version = aspen_core::VERSION, about = "A Git server for agent-scale traffic")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

const SERVE_HELP: &str = "\
Configuration (environment variables; see deploy/README.md):
  ASPEN_LISTEN                 address for Git over HTTP [default: 0.0.0.0:7070]
  ASPEN_PUBLIC_URL             URL clients use, e.g. https://git.example.com
  DATABASE_URL                 postgres://user@host/db (password in PGPASSWORD) or sqlite://path?mode=rwc  (required)
  ASPEN_BLOB_URL               s3://bucket[/prefix] or file:///path  (required)
  ASPEN_BLOB_URL_SECRET        hex key (>= 32 bytes) signing /-/blobs URLs of a file:// store
  ASPEN_POLICY_FILE            JSON access policy (anonymous grants, OIDC issuers); default: tokens only
  ASPEN_POLICY                 the same policy inline, instead of a file
  ASPEN_LIMITS_FILE            JSON rate limits and push quotas; default: none (warned at startup)
  ASPEN_LIMITS                 the same limits inline, instead of a file
  ASPEN_TRUSTED_PROXY_HOPS     proxies in front of Aspen that set X-Forwarded-For [default: 0]
  ASPEN_TRUSTED_PROXIES        CIDR blocks the nearest proxy connects from [default: private ranges]
  ASPEN_SPOOL_DIR              push spool on a real disk, never tmpfs [default: ./spool]
  ASPEN_MAX_OBJECT_BYTES       largest object inflated [default: 536870912]
  ASPEN_MAX_INFLATED_BYTES     most one push inflates to [default: 8589934592]
  ASPEN_MAX_PUSH_BYTES         largest pack per push [default: 8589934592]
  ASPEN_MAX_PUSH_OBJECTS       most objects per push [default: 10000000]
  ASPEN_PREFETCH_MEMORY_BYTES  delta-base memory shared by concurrent pushes [default: 4294967296]
  ASPEN_MAX_CONCURRENT_FETCHES fetch requests and SSH fetch sessions at once [default: 32]
  ASPEN_MAX_CONCURRENT_PUSHES  push requests and SSH push sessions at once [default: 16]
  ASPEN_MAX_CONCURRENT_STITCHES inline packs stitched at once [default: 32]
  ASPEN_ADMISSION_QUEUE        requests of each kind waiting for a slot, up to 10 s [default: 64]
  ASPEN_PACKFILE_URIS          auto|true|false [default: auto: on when URLs can be presigned]
  ASPEN_EXPRESS_SERVE          auto|true|false: serve /-/express [default: auto: when ASPEN_EXPRESS_URLS names this server]
  ASPEN_MIGRATE                apply schema migrations at startup [default: true]
  ASPEN_METRICS_LISTEN         private listener for /-/metrics, or off [default: 127.0.0.1:9464]
  ASPEN_SSH_LISTEN             address for Git over SSH [default: off]
  ASPEN_SSH_HOST_KEY_FILE      the SSH host key (OpenSSH private key file; Ed25519 or ECDSA; never world-readable)
  ASPEN_SSH_HOST_KEY           the same key inline, from a secret store
  ASPEN_MIRROR_*               mirror mode: credentials, sync interval, work dir (docs/MIRROR.md)
  ASPEN_EXPRESS_*, ASPEN_CDN_* express and CDN packfile URIs (docs/EXPRESS.md)
  ASPEN_LOG, ASPEN_LOG_FORMAT, OTEL_* logs and traces (docs/OBSERVABILITY.md)";

#[derive(Debug, Subcommand)]
enum Command {
    /// Run the Git server, configured from the environment.
    #[command(after_help = SERVE_HELP)]
    Serve,
    /// Run a single-process development server (SQLite and local disk).
    /// It listens on loopback only and lets anyone who can reach it push.
    Dev {
        /// Address to listen on for Git over HTTP (a loopback address).
        #[arg(long, default_value = "127.0.0.1:7070")]
        listen: SocketAddr,
        /// Directory for the SQLite database, stored packs and the spool.
        #[arg(long, default_value = ".aspen")]
        data_dir: PathBuf,
        /// Repository to create if it does not exist (repeatable).
        #[arg(long = "repo", default_value = "demo")]
        repos: Vec<String>,
        /// Don't run the background worker: no compaction, commit indexing
        /// or garbage collection. For benchmarks that measure fetches
        /// without covers; pushes still enqueue their jobs.
        #[arg(long)]
        no_background_worker: bool,
    },
    /// Run an express node: serve signed packfile-URI downloads from the
    /// blob store (`ASPEN_BLOB_URL`) through a disk cache. Configured by
    /// the `ASPEN_EXPRESS_*` variables (docs/EXPRESS.md).
    Express {
        /// Address to listen on.
        #[arg(
            long,
            env = "ASPEN_EXPRESS_LISTEN",
            hide_env_values = true,
            default_value = "0.0.0.0:7071"
        )]
        listen: SocketAddr,
    },
    /// Check a running server: exit 0 only if it answers 200.
    Healthcheck {
        /// URL to check [default: http://127.0.0.1:PORT/healthz, PORT from ASPEN_LISTEN].
        #[arg(long)]
        url: Option<String>,
        /// Seconds to wait for the answer.
        #[arg(long, default_value_t = 5)]
        timeout_secs: u64,
    },
    /// Apply the metadata database's schema migrations.
    Migrate {
        /// The metadata database (password in PGPASSWORD).
        #[arg(long, env = "DATABASE_URL", hide_env_values = true)]
        database_url: String,
    },
    /// Manage repositories.
    #[command(subcommand)]
    Repo(RepoCommand),
    /// Manage API tokens.
    #[command(subcommand)]
    Token(TokenCommand),
    /// Manage SSH keys for Git over SSH.
    #[command(subcommand)]
    Key(KeyCommand),
    /// Check on a running installation (docs/OBSERVABILITY.md).
    Doctor {
        #[command(subcommand)]
        check: Doctor,
    },
    /// Import a repository from an upstream (GitHub, GitLab, any smart-HTTP
    /// Git server), as a mirror that keeps following it or as a one-time
    /// copy. docs/MIRROR.md.
    Import {
        #[command(flatten)]
        stores: mirror::Stores,
        #[command(flatten)]
        args: mirror::ImportArgs,
    },
    /// Inspect and operate mirrors: status, sync, resolve, remove.
    Mirror {
        #[command(flatten)]
        stores: mirror::Stores,
        #[command(subcommand)]
        command: mirror::MirrorCommand,
    },
}

#[derive(Debug, Subcommand)]
enum Doctor {
    /// The background job queue: jobs per kind and state, and dead jobs
    /// with their last error. Exits 1 when any job is dead, 2 on error.
    Jobs {
        /// The metadata database, as the server is given it
        /// (`postgres://...` without a password, which comes from
        /// `PGPASSWORD`; or `sqlite://...`).
        #[arg(long, env = "DATABASE_URL", hide_env_values = true)]
        database_url: String,
        /// Print JSON instead of text.
        #[arg(long)]
        json: bool,
        /// Most dead jobs to show, oldest first.
        #[arg(long, default_value_t = 100)]
        limit: u32,
        #[command(subcommand)]
        action: Option<doctor::JobsAction>,
    },
}

#[derive(Debug, Subcommand)]
enum KeyCommand {
    /// Register a public key for Git over SSH; prints its fingerprint.
    Add {
        /// The public key: an OpenSSH line (`ssh-ed25519 AAAA... comment`)
        /// or the path of a `.pub` file holding one.
        key: String,
        /// Who the key acts as, for logs and rate limits (`user:alice`).
        #[arg(long)]
        subject: String,
        /// What it may do, as for tokens (repeatable).
        #[arg(long = "grant", required = true)]
        grants: Vec<Grant>,
        /// A note for operators.
        #[arg(long, default_value = "")]
        description: String,
        /// Days until it stops working [default: never].
        #[arg(long)]
        expires_in_days: Option<u32>,
        /// The metadata database (password in PGPASSWORD).
        #[arg(long, env = "DATABASE_URL", hide_env_values = true)]
        database_url: String,
    },
    /// List registered keys.
    List {
        /// The metadata database (password in PGPASSWORD).
        #[arg(long, env = "DATABASE_URL", hide_env_values = true)]
        database_url: String,
    },
    /// Revoke a key by its fingerprint (`SHA256:...`). Revocation is final:
    /// the same key cannot be registered again.
    Revoke {
        /// The key's fingerprint, as `aspen key list` or `ssh-keygen -l` shows it.
        fingerprint: String,
        /// The metadata database (password in PGPASSWORD).
        #[arg(long, env = "DATABASE_URL", hide_env_values = true)]
        database_url: String,
    },
}

#[derive(Debug, Subcommand)]
enum RepoCommand {
    /// Create a repository.
    Create {
        /// Repository name, such as `acme/app` (cloned as `<url>/acme/app.git`).
        name: String,
        /// The trunk branch: the one ref a plain push URL advertises.
        #[arg(long, default_value = "refs/heads/main")]
        trunk: String,
        /// Which refs a push may force: none, branches (never trunk) or all.
        #[arg(long, default_value = "branches", value_parser = admin::parse_force)]
        allow_force: aspen_core::repo::ForcePolicy,
        /// The metadata database (password in PGPASSWORD).
        #[arg(long, env = "DATABASE_URL", hide_env_values = true)]
        database_url: String,
    },
}

#[derive(Debug, Subcommand)]
enum TokenCommand {
    /// Create a token and print it once on stdout.
    Create {
        /// Who the token acts as, for logs and rate limits (`agent:ci-7`).
        #[arg(long)]
        subject: String,
        /// What it may do: `read:<repos>`, `write:<repos>[:<refs>]` or
        /// `admin:<repos>`, such as `write:acme/*` (repeatable).
        #[arg(long = "grant", required = true)]
        grants: Vec<Grant>,
        /// A note for operators.
        #[arg(long, default_value = "")]
        description: String,
        /// Days until it stops working [default: never].
        #[arg(long)]
        expires_in_days: Option<u32>,
        /// The metadata database (password in PGPASSWORD).
        #[arg(long, env = "DATABASE_URL", hide_env_values = true)]
        database_url: String,
    },
    /// List tokens (never their secrets).
    List {
        /// The metadata database (password in PGPASSWORD).
        #[arg(long, env = "DATABASE_URL", hide_env_values = true)]
        database_url: String,
    },
    /// Revoke a token by its ID.
    Revoke {
        /// The token's public ID (32 hex digits; `aspen token list`).
        id: String,
        /// The metadata database (password in PGPASSWORD).
        #[arg(long, env = "DATABASE_URL", hide_env_values = true)]
        database_url: String,
    },
}

/// Runs an async command to an exit code: success only when it reports
/// everything succeeded.
fn run_async(what: &str, f: impl std::future::Future<Output = Result<bool, String>>) -> ExitCode {
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(r) => r,
        Err(e) => {
            eprintln!("aspen {what}: {e}");
            return ExitCode::FAILURE;
        }
    };
    match runtime.block_on(f) {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::FAILURE,
        Err(e) => {
            eprintln!("aspen {what}: {e}");
            ExitCode::FAILURE
        }
    }
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    // Held until the command returns: dropping it flushes the traces.
    let _telemetry = match telemetry::init() {
        Ok(t) => t,
        Err(e) => {
            eprintln!("aspen: {e}");
            return ExitCode::from(2);
        }
    };
    let (name, result): (&str, Result<(), BoxError>) = match cli.command {
        Command::Serve => ("serve", serve_from_env()),
        Command::Dev {
            listen,
            data_dir,
            repos,
            no_background_worker,
        } => ("dev", dev(listen, &data_dir, &repos, !no_background_worker)),
        Command::Express { listen } => ("express", express(listen)),
        Command::Healthcheck { url, timeout_secs } => {
            let url = url.unwrap_or_else(|| {
                healthcheck::default_url(std::env::var("ASPEN_LISTEN").ok().as_deref())
            });
            (
                "healthcheck",
                healthcheck::check(&url, Duration::from_secs(timeout_secs)).map_err(Into::into),
            )
        }
        Command::Migrate { database_url } => ("migrate", admin::migrate(&database_url)),
        Command::Repo(RepoCommand::Create {
            name,
            trunk,
            allow_force,
            database_url,
        }) => (
            "repo create",
            admin::repo_create(&database_url, &name, &trunk, allow_force),
        ),
        Command::Token(TokenCommand::Create {
            subject,
            grants,
            description,
            expires_in_days,
            database_url,
        }) => (
            "token create",
            admin::token_create(&database_url, subject, grants, description, expires_in_days),
        ),
        Command::Token(TokenCommand::List { database_url }) => {
            ("token list", admin::token_list(&database_url))
        }
        Command::Token(TokenCommand::Revoke { id, database_url }) => {
            ("token revoke", admin::token_revoke(&database_url, &id))
        }
        Command::Key(KeyCommand::Add {
            key,
            subject,
            grants,
            description,
            expires_in_days,
            database_url,
        }) => (
            "key add",
            admin::key_add(
                &database_url,
                &key,
                subject,
                grants,
                description,
                expires_in_days,
            ),
        ),
        Command::Key(KeyCommand::List { database_url }) => {
            ("key list", admin::key_list(&database_url))
        }
        Command::Key(KeyCommand::Revoke {
            fingerprint,
            database_url,
        }) => ("key revoke", admin::key_revoke(&database_url, &fingerprint)),
        Command::Import { stores, args } => {
            return run_async("import", mirror::import(stores, args));
        }
        Command::Mirror { stores, command } => {
            return run_async("mirror", mirror::command(stores, command));
        }
        Command::Doctor {
            check:
                Doctor::Jobs {
                    database_url,
                    json,
                    limit,
                    action,
                },
        } => {
            return doctor::jobs(
                &database_url,
                action.unwrap_or(doctor::JobsAction::List),
                json,
                limit,
            );
        }
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            // The subscriber writes this to stderr; printing it too would
            // show it twice.
            tracing::error!("aspen {name}: {e}");
            ExitCode::FAILURE
        }
    }
}

fn env(k: &str) -> Option<String> {
    std::env::var(k).ok()
}

fn no_env(_: &str) -> Option<String> {
    None
}

fn serve_from_env() -> Result<(), BoxError> {
    let config = ServeConfig::from_env(&env, &|p| std::fs::read_to_string(p), &|p| {
        secret_file::read(std::path::Path::new(p))
    })?;
    serve::run(
        config,
        Startup {
            label: "serve",
            create_repos: Vec::new(),
            public_url_from_listener: false,
            ephemeral_url_secret: false,
            warn_unlimited_rates: true,
            env,
        },
    )
}

/// `aspen dev`: the whole server on one machine. Anyone who can reach it
/// may push, so it refuses to listen anywhere but loopback.
fn dev(
    listen: SocketAddr,
    data_dir: &Path,
    repos: &[String],
    background_worker: bool,
) -> Result<(), BoxError> {
    if !listen.ip().is_loopback() {
        return Err(format!(
            "{listen} is not a loopback address. aspen dev lets anyone who can reach it \
             push, so it listens on 127.0.0.1 or ::1 only; use aspen serve with tokens \
             to serve a network"
        )
        .into());
    }
    let create_repos = repos
        .iter()
        .map(|r| RepoName::new(r.as_str()).map_err(|e| format!("--repo {r:?}: {e}")))
        .collect::<Result<Vec<_>, _>>()?;
    std::fs::create_dir_all(data_dir)
        .map_err(|e| format!("creating {}: {e}", data_dir.display()))?;
    let data_dir = data_dir.canonicalize()?;
    let blobs = data_dir.join("blobs");
    let config = ServeConfig {
        listen,
        public_url: None,
        database_url: format!("sqlite://{}?mode=rwc", data_dir.join("meta.db").display()),
        store: aspen_meta::StoreOptions::default(),
        blob_url: format!("file://{}", blobs.display()),
        blob_url_secret: None,
        policy: PolicyConfig {
            anonymous: vec!["write:*".parse::<Grant>().map_err(|e| e.to_string())?],
            ..PolicyConfig::default()
        },
        // Every local client shares the loopback address: only the
        // instance bounds how much runs at once.
        limits: aspen_auth::LimitsConfig {
            concurrent: aspen_auth::ConcurrentLimits::UNLIMITED,
            ..aspen_auth::LimitsConfig::default()
        },
        client_addr: aspen_server::access::ClientAddr::Peer,
        instance: aspen_server::admission::InstanceLimits::default(),
        spool_dir: data_dir.join("spool"),
        max_object_bytes: config::DEFAULT_MAX_OBJECT_BYTES,
        max_inflated_bytes: config::DEFAULT_MAX_INFLATED_BYTES,
        max_push_bytes: config::DEFAULT_MAX_PUSH_BYTES,
        max_push_objects: u32::try_from(config::DEFAULT_MAX_PUSH_OBJECTS)?,
        prefetch_memory_bytes: config::DEFAULT_PREFETCH_MEMORY_BYTES,
        packfile_uris: Switch::Auto,
        express_serve: false,
        migrate: true,
        shallow_packs: true,
        shallow_max_repo_bytes: aspen_server::upload_pack::exact::DEFAULT_MAX_REPO_BYTES,
        shallow_unused: aspen_compact::DEFAULT_SHALLOW_UNUSED,
        // A laptop's loopback listener is private: metrics live beside Git.
        metrics_listen: None,
        metrics_on_git_listener: true,
        background_worker,
        ssh: None,
        // Public upstreams only: `aspen dev` reads no credentials.
        mirror_credentials: aspen_mirror::Credentials::default(),
        mirror: aspen_mirror::MirrorConfig::new(data_dir.join("spool").join("mirror")),
    };
    tracing::info!("aspen dev: data in {}", data_dir.display());
    serve::run(
        config,
        Startup {
            label: "dev",
            create_repos,
            public_url_from_listener: true,
            ephemeral_url_secret: true,
            warn_unlimited_rates: false,
            env: no_env,
        },
    )
}

fn express(listen: SocketAddr) -> Result<(), BoxError> {
    let blob_url = env("ASPEN_BLOB_URL").ok_or("ASPEN_BLOB_URL is required")?;
    let (keys, config) = aspen_express::settings::service(env)?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async move {
        // The blob store's S3 client needs a rustls crypto provider (A4).
        let _ = rustls::crypto::ring::default_provider().install_default();
        let blobs = aspen_blob::open(&blob_url)?;
        let service = aspen_express::Express::new(blobs, keys, config)?;
        let metrics = service.clone();
        let router = axum::Router::new()
            .route(
                "/-/metrics",
                axum::routing::get(move || {
                    let metrics = metrics.clone();
                    async move {
                        let mut text = String::new();
                        metrics.metrics().render(&mut text);
                        (
                            [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
                            text,
                        )
                    }
                }),
            )
            .fallback_service(service.router());
        let listener = tokio::net::TcpListener::bind(listen).await?;
        tracing::info!(%listen, "aspen express: listening");
        axum::serve(listener, router).await?;
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use clap::CommandFactory;

    /// Issue #113: help must never print an environment variable's value,
    /// since `DATABASE_URL`, `ASPEN_BLOB_URL` and the like can carry
    /// credentials. Every option of every subcommand that reads a variable
    /// hides its value, so a new one cannot leak by omission.
    #[test]
    fn every_env_backed_option_hides_its_value() {
        fn walk(cmd: &clap::Command, path: &str, leaks: &mut Vec<String>) {
            for arg in cmd.get_arguments() {
                if let Some(var) = arg.get_env()
                    && !arg.is_hide_env_values_set()
                {
                    leaks.push(format!("{path} --{}: {var:?}", arg.get_id()));
                }
            }
            for sub in cmd.get_subcommands() {
                walk(sub, &format!("{path} {}", sub.get_name()), leaks);
            }
        }
        let mut cmd = super::Cli::command();
        cmd.build();
        let mut leaks = Vec::new();
        walk(&cmd, "aspen", &mut leaks);
        assert!(
            leaks.is_empty(),
            "options showing env values in help: {leaks:?}"
        );
    }
}
