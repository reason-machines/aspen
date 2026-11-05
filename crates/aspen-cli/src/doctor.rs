//! `aspen doctor jobs` (node C5): the operator's view of the background job
//! queue, and the two procedures for a dead job (`crates/aspen-meta/SCHEMA.md`,
//! "jobs").
//!
//! A job is parked as `dead` after it failed or lost its lease 10 times.
//! Nothing retries it, and it keeps its slot in the queue, so the work it
//! stands for does not happen until an operator acts. The server's metrics
//! export the number of dead jobs per kind (`aspen_jobs{state="dead"}`),
//! and the shipped alert rules page on any value above zero. The operator
//! reads the job's last error here, fixes the cause, and then either:
//!
//! - `requeue <id>`: in one transaction, marks the dead job done and
//!   enqueues a fresh job (new ID, attempt 0) for the same kind and target;
//! - `resolve <id>`: marks it done, when the work was done some other way.
//!
//! The dead row is never reset in place: workers fence every update on the
//! job ID and attempt, and reusing attempt numbers on the same row would
//! let a stale worker finish the new attempt.
//!
//! Exit status: 0 when the command succeeded (for the listing: and no job
//! is dead), 1 when the listing found dead jobs, 2 on any error.

use std::process::ExitCode;
use std::time::{SystemTime, UNIX_EPOCH};

use aspen_core::id::{JobId, UnixMicros};
use aspen_core::meta::{Job, JobKind, JobTag, MetaStore as _};
use aspen_meta::{JobCount, OpenJobState, SqlStore};
use clap::Subcommand;
use tracing::Instrument as _;

/// What `aspen doctor jobs` does.
#[derive(Debug, Clone, Copy, Subcommand)]
pub(crate) enum JobsAction {
    /// Counts of jobs that are not done, per kind and state, and every dead
    /// job with its last error (the default).
    List,
    /// Retries a dead job's work after its cause is fixed: marks it done
    /// and enqueues a fresh job for the same kind and target, in one
    /// transaction.
    Requeue {
        /// The dead job's ID, as `list` shows it.
        id: i64,
    },
    /// Marks a dead job done, for when its work was done some other way.
    Resolve {
        /// The dead job's ID, as `list` shows it.
        id: i64,
    },
}

/// Exit status of a listing that found dead jobs.
const DEAD_JOBS: u8 = 1;
/// Exit status of a failure.
const FAILED: u8 = 2;

/// Runs `aspen doctor jobs` against the metadata database at `url`.
pub(crate) fn jobs(url: &str, action: JobsAction, json: bool, limit: u32) -> ExitCode {
    let span = tracing::info_span!("doctor_jobs", action = action.name());
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(r) => r,
        Err(e) => return fail(&format!("starting the runtime: {e}")),
    };
    if let Err(e) = crate::config::check_database_url(url) {
        return fail(&e.to_string());
    }
    runtime.block_on(async {
        // TLS for `sslmode=require` (RDS) uses rustls on `ring`.
        let _ = rustls::crypto::ring::default_provider().install_default();
        let store = match SqlStore::connect(url).instrument(span.clone()).await {
            Ok(s) => s,
            Err(e) => return fail(&format!("connecting to the metadata database: {e}")),
        };
        let result = async {
            match action {
                JobsAction::List => list(&store, json, limit).await,
                JobsAction::Requeue { id } => requeue(&store, id, json).await,
                JobsAction::Resolve { id } => resolve(&store, id, json).await,
            }
        }
        .instrument(span.clone())
        .await;
        // Closed outside the span, on purpose. sqlx's SQLite worker threads
        // keep a clone of the caller's span for each command and drop it
        // only after replying, so a span still current here could close
        // after `main` has flushed the trace exporter, and be lost. The
        // worker handles the close after it has dropped every earlier
        // command's span, so once this returns the span's last reference
        // is ours, and it closes (and is exported) when `jobs` returns.
        store.pool().close().await;
        result.unwrap_or_else(|e| fail(&e))
    })
}

impl JobsAction {
    fn name(self) -> &'static str {
        match self {
            Self::List => "list",
            Self::Requeue { .. } => "requeue",
            Self::Resolve { .. } => "resolve",
        }
    }
}

fn fail(msg: &str) -> ExitCode {
    eprintln!("aspen doctor jobs: {msg}");
    ExitCode::from(FAILED)
}

fn job_id(id: i64) -> Result<JobId, String> {
    JobId::new(id).map_err(|e| format!("job ID {id}: {e}"))
}

fn now() -> UnixMicros {
    let micros = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_micros());
    UnixMicros(i64::try_from(micros).unwrap_or(i64::MAX))
}

/// `version 42` or `pack 9`.
fn target(kind: &JobKind) -> String {
    match kind {
        JobKind::IndexCommits { version }
        | JobKind::Compact { version }
        | JobKind::MirrorPush { version } => format!("version {}", version.get()),
        JobKind::RewritePack { pack } => format!("pack {}", pack.get()),
    }
}

fn target_json(kind: &JobKind) -> serde_json::Value {
    match kind {
        JobKind::IndexCommits { version }
        | JobKind::Compact { version }
        | JobKind::MirrorPush { version } => serde_json::json!({ "version": version.get() }),
        JobKind::RewritePack { pack } => serde_json::json!({ "pack": pack.get() }),
    }
}

/// One line: control characters (a multi-line error) become spaces, so a
/// stored error cannot forge more lines of the listing.
fn one_line(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect()
}

fn job_json(j: &Job, repo: Option<&str>) -> serde_json::Value {
    serde_json::json!({
        "id": j.id.get(),
        "repo_id": j.repo.get(),
        "repo": repo,
        "kind": j.kind.tag().as_str(),
        "target": target_json(&j.kind),
        "state": j.state.as_str(),
        "attempts": j.attempts,
        "last_error": j.last_error,
    })
}

/// Repository names for the dead jobs' repository IDs (`None` when the row
/// is gone, which the schema's foreign keys prevent).
async fn repo_names(store: &SqlStore, jobs: &[Job]) -> Result<Vec<Option<String>>, String> {
    let mut names = Vec::with_capacity(jobs.len());
    for j in jobs {
        let repo = store
            .repo_by_id(j.repo)
            .await
            .map_err(|e| format!("reading repository {}: {e}", j.repo.get()))?;
        names.push(repo.map(|r| r.name.to_string()));
    }
    Ok(names)
}

async fn list(store: &SqlStore, json: bool, limit: u32) -> Result<ExitCode, String> {
    let counts = store
        .job_counts()
        .await
        .map_err(|e| format!("counting jobs: {e}"))?;
    let dead = store
        .dead_jobs(limit)
        .await
        .map_err(|e| format!("reading dead jobs: {e}"))?;
    let names = repo_names(store, &dead).await?;
    let dead_total: u64 = counts
        .iter()
        .filter(|c| c.state == OpenJobState::Dead)
        .map(|c| c.count)
        .sum();
    let now = now();
    if json {
        let doc = serde_json::json!({
            "counts": counts.iter().map(|c| serde_json::json!({
                "kind": c.kind.as_str(),
                "state": c.state.as_str(),
                "count": c.count,
                "earliest_run_after_us": c.earliest_run_after.map(|t| t.0),
            })).collect::<Vec<_>>(),
            "dead_total": dead_total,
            "dead": dead.iter().zip(&names).map(|(j, n)| job_json(j, n.as_deref())).collect::<Vec<_>>(),
        });
        println!("{doc:#}");
    } else {
        print!("{}", render_counts(&counts, now));
        print!("{}", render_dead(&dead, &names, dead_total));
    }
    Ok(if dead_total == 0 {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(DEAD_JOBS)
    })
}

/// The counts table: one row per kind that has open jobs.
fn render_counts(counts: &[JobCount], now: UnixMicros) -> String {
    if counts.is_empty() {
        return "No jobs are queued, running or dead.\n".to_owned();
    }
    let mut out = format!(
        "{:<14} {:>8} {:>8} {:>8}  {}\n",
        "KIND", "QUEUED", "RUNNING", "DEAD", "OLDEST DUE"
    );
    for kind in JobTag::ALL {
        let of = |state| {
            counts
                .iter()
                .find(|c| c.kind == kind && c.state == state)
                .copied()
        };
        let n = |state| of(state).map_or(0, |c: JobCount| c.count);
        if OpenJobState::ALL.iter().all(|s| n(*s) == 0) {
            continue;
        }
        let due = of(OpenJobState::Queued)
            .and_then(|c| c.earliest_run_after)
            .map_or_else(
                || "-".to_owned(),
                |t| {
                    let behind = now.0.saturating_sub(t.0);
                    if behind > 0 {
                        format!("{}s ago", behind / 1_000_000)
                    } else {
                        "not yet due".to_owned()
                    }
                },
            );
        out.push_str(&format!(
            "{:<14} {:>8} {:>8} {:>8}  {}\n",
            kind.as_str(),
            n(OpenJobState::Queued),
            n(OpenJobState::Running),
            n(OpenJobState::Dead),
            due
        ));
    }
    out
}

fn render_dead(dead: &[Job], names: &[Option<String>], total: u64) -> String {
    if total == 0 {
        return String::new();
    }
    let mut out = format!("\n{total} dead job(s)");
    if (dead.len() as u64) < total {
        out.push_str(&format!(", oldest {} shown (--limit)", dead.len()));
    }
    out.push_str(":\n");
    for (j, name) in dead.iter().zip(names) {
        let repo = name
            .clone()
            .unwrap_or_else(|| format!("repository {}", j.repo.get()));
        out.push_str(&format!(
            "  job {}: {} {} in {}, {} attempts\n    last error: {}\n",
            j.id.get(),
            j.kind.tag().as_str(),
            target(&j.kind),
            one_line(&repo),
            j.attempts,
            one_line(j.last_error.as_deref().unwrap_or("(none recorded)")),
        ));
    }
    out.push_str(
        "\nFix the cause first (the last error says what failed), then either:\n  \
         aspen doctor jobs requeue <id>   retry the work as a fresh job\n  \
         aspen doctor jobs resolve <id>   mark it done, if the work was done some other way\n",
    );
    out
}

async fn requeue(store: &SqlStore, id: i64, json: bool) -> Result<ExitCode, String> {
    let id = job_id(id)?;
    let fresh = store
        .requeue_dead_job(id)
        .await
        .map_err(|e| format!("requeue job {}: {e}", id.get()))?;
    tracing::info!(
        job = id.get(),
        new_job = fresh.id.get(),
        kind = fresh.kind.tag().as_str(),
        "requeued a dead job as a fresh job"
    );
    if json {
        println!(
            "{:#}",
            serde_json::json!({ "resolved": id.get(), "requeued": job_json(&fresh, None) })
        );
    } else {
        println!(
            "Job {} is done; its work is queued again as job {} ({} {}).",
            id.get(),
            fresh.id.get(),
            fresh.kind.tag().as_str(),
            target(&fresh.kind)
        );
    }
    Ok(ExitCode::SUCCESS)
}

async fn resolve(store: &SqlStore, id: i64, json: bool) -> Result<ExitCode, String> {
    let id = job_id(id)?;
    store
        .resolve_dead_job(id)
        .await
        .map_err(|e| format!("resolve job {}: {e}", id.get()))?;
    tracing::info!(job = id.get(), "marked a dead job done");
    if json {
        println!("{:#}", serde_json::json!({ "resolved": id.get() }));
    } else {
        println!("Job {} is marked done; nothing was requeued.", id.get());
    }
    Ok(ExitCode::SUCCESS)
}
