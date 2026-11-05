//! How `aspen-place serve` ends: the server stops, by a signal or by
//! failing, and then the last merge runs before the process exits.
//!
//! Accepted pushes wait on their branches for the merger bot, so every
//! way out of [`serve_then_merge`] tells the [`Worker`] to stop and waits
//! for it: its last merge puts those pushes on `main`.

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::drain::Cutoff;

/// How long the connections still open at the end of the drain get to
/// close once their requests are cut off.
pub(crate) const CLOSE_WAIT: Duration = Duration::from_secs(5);

/// A future that resolves once its [`Worker`] is told to stop.
pub(crate) type Stopped = Pin<Box<dyn Future<Output = ()> + Send>>;

/// The merger bot and the agents (`Place::run`) in their own task.
pub(crate) struct Worker {
    stop: watch::Sender<bool>,
    task: JoinHandle<()>,
}

impl Worker {
    /// Spawns `run`, handing it the future that tells it to stop.
    pub(crate) fn spawn<F>(run: impl FnOnce(Stopped) -> F) -> Self
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let (stop, mut stopped) = watch::channel(false);
        let task = tokio::spawn(run(Box::pin(async move {
            let _ = stopped.changed().await;
        })));
        Self { stop, task }
    }
}

/// Runs `server` until it returns, or until `drain` has passed since
/// `signal` turned true; then tells `worker` to stop and waits for it, so
/// its last merge runs however the server ended, an error included. Returns
/// the server's error first, then the worker's.
///
/// SIGTERM (what ECS, Kubernetes and `docker stop` send) or Ctrl-C sets
/// `signal`; `server` should then stop accepting connections and let the
/// requests in flight finish. With none open it returns at once, so the
/// process exits without waiting out `drain`. A page's event stream lasts
/// up to its age limit (10 minutes by default), so at most `drain` after
/// the signal `cutoff` ends every request still running and `server` gets [`CLOSE_WAIT`] more to return:
/// the last merge must run before the platform's kill timeout (ECS waits
/// 120 s at most). The worst case is `drain`, plus `CLOSE_WAIT`, plus the
/// last merge, plus `EXIT_WAIT` in `main`.
pub(crate) async fn serve_then_merge(
    server: impl Future<Output = std::io::Result<()>>,
    mut signal: watch::Receiver<bool>,
    drain: Duration,
    cutoff: Cutoff,
    worker: Worker,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut server = std::pin::pin!(server);
    let served = tokio::select! {
        result = &mut server => result,
        () = async {
            let _ = signal.wait_for(|stop| *stop).await;
            tokio::time::sleep(drain).await;
        } => {
            // Each connection runs in its own task, which `serve` does not
            // own: returning without ending their requests would leave them
            // served through the last merge (#140). The cutoff ends every
            // request still in flight, and then hyper closes its connection.
            // Work a push already handed to a blocking thread may still
            // commit its branch, during the last merge or after it; it then
            // stays on its branch, like a push at a crash (#55).
            eprintln!("aspen-place: connections still open after {}s; closing them", drain.as_secs());
            cutoff.fire();
            match tokio::time::timeout(CLOSE_WAIT, &mut server).await {
                Ok(result) => result,
                Err(_) => {
                    // A peer that stopped reading can keep hyper from
                    // seeing the cut; the runtime's shutdown in `main`
                    // closes what is left.
                    eprintln!(
                        "aspen-place: connections still open {}s after closing them; merging anyway",
                        CLOSE_WAIT.as_secs()
                    );
                    Ok(())
                }
            }
        }
    };
    // However the server ended, an error included, run the last merge before
    // exiting: accepted pushes are on their branches, not yet on main.
    let _ = worker.stop.send(true);
    let merged = worker.task.await;
    if let (Err(served), Err(merged)) = (&served, &merged) {
        eprintln!("aspen-place: server failed ({served}); the last merge failed too ({merged})");
    }
    served?;
    merged?;
    eprintln!("aspen-place: stopped");
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::process::ExitCode;
    use std::sync::Arc;
    use std::time::Instant;

    use aspen_auth::MemoryTokenStore;
    use aspen_blob::LocalBlobStore;
    use aspen_core::meta::MetaStore;
    use aspen_core::name::RefName;
    use aspen_meta::SqlStore;
    use aspen_pack::{MaxInflatedBytes, MaxObjectBytes};
    use aspen_place::rules::Tile;
    use aspen_place::state::Actor;
    use aspen_place::{Place, PlaceConfig, ServerSettings};
    use aspen_server::access::ClientAddr;

    use super::*;

    /// `place` on a throwaway SQLite database and local blob store. Its
    /// merger runs only when started and when stopped: the interval's
    /// first tick is immediate, the next an hour later.
    async fn place(dir: &std::path::Path) -> (Arc<Place<SqlStore, LocalBlobStore>>, SqlStore) {
        let url = format!("sqlite://{}?mode=rwc", dir.join("meta.db").display());
        let meta = SqlStore::connect(&url).await.unwrap();
        meta.migrate().await.unwrap();
        let mut config = PlaceConfig::new("http://localhost/canvas.git");
        config.ambient_agents = 0;
        config.merge_interval = Duration::from_secs(3600);
        let settings = ServerSettings {
            spool_dir: dir.join("spool"),
            max_object_bytes: MaxObjectBytes::new(16 << 20).unwrap(),
            max_inflated_bytes: MaxInflatedBytes::new(256 << 20).unwrap(),
            policy: serde_json::from_value(serde_json::json!({ "anonymous": [] })).unwrap(),
            limits: aspen_auth::LimitsConfig::default(),
            client_addr: ClientAddr::Peer,
        };
        let blobs = LocalBlobStore::open(dir.join("blobs")).unwrap();
        let built = aspen_place::build(
            meta.clone(),
            blobs,
            MemoryTokenStore::default(),
            config,
            settings,
        )
        .await
        .unwrap();
        (built.place, meta)
    }

    /// When the server fails (an accept error, say), the worker is still
    /// told to stop and awaited, so the last merge puts accepted pushes on
    /// `main`, and the failure reaches `main` as an error, so the process
    /// exits non-zero. No signal is sent here, so the drain never starts:
    /// the time bound only catches a hang, not waiting out the drain (the
    /// binary tests cover the drain).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_failed_server_still_runs_the_last_merge_and_fails_without_a_drain() {
        let dir = tempfile::tempdir().unwrap();
        let (place, meta) = place(dir.path()).await;
        let main = RefName::new("refs/heads/main").unwrap();
        let rules = &place.config().rules;
        let red = rules.color_index("#d6262b").expect("a palette color");
        let tile = |x, y| Tile { x, y, color: red };

        // A push painted before the worker starts, which only the merger's
        // immediate first tick can merge. Once it is merged, that first
        // pass is over and the next tick is an hour away, so only the last
        // merge can merge what is painted after it.
        let first = place
            .paint("paint/first", Actor::git("test"), &[tile(1, 2)], "paint")
            .await
            .unwrap();
        let worker = Worker::spawn({
            let place = place.clone();
            |stopped| place.run(stopped)
        });
        let deadline = Instant::now() + Duration::from_secs(30);
        while place.with_state(|s| s.merged_through < first.seq) {
            assert!(Instant::now() < deadline, "the first tick merges");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let tile = tile(3, 4);
        let index = rules.index(tile.x, tile.y);
        let before = place.with_state(|s| s.main.commit);
        let painted = place
            .paint("paint/err", Actor::git("test"), &[tile], "paint")
            .await
            .unwrap();
        assert!(
            place.with_state(|s| s.merged_through < painted.seq && s.main.commit == before),
            "the push is accepted onto its branch, not yet merged"
        );

        let (_signal_tx, signal) = watch::channel(false);
        let (cutoff, _watch) = Cutoff::new();
        let drain = Duration::from_secs(60);
        let began = Instant::now();
        let result = serve_then_merge(
            async { Err(std::io::Error::other("accept failed (test)")) },
            signal,
            drain,
            cutoff,
            worker,
        )
        .await;
        let elapsed = began.elapsed();

        let error = result.expect_err("the server's failure is returned");
        assert!(
            error.to_string().contains("accept failed (test)"),
            "{error}"
        );
        assert!(elapsed < Duration::from_secs(20), "took {elapsed:?}");
        let (commit, merged_through, pixel) =
            place.with_state(|s| (s.main.commit, s.merged_through, s.main.pixels[index]));
        assert_ne!(commit, before, "the last merge moved main");
        assert!(merged_through >= painted.seq, "the push is merged");
        assert_eq!(pixel, red, "the tile is on main");
        let stored = meta
            .current_version(place.repo().id, &main)
            .await
            .unwrap()
            .and_then(|v| v.value.tip());
        assert_eq!(stored, Some(commit), "main's ref holds the merge");
        assert_eq!(crate::exit_code(Err(error)), ExitCode::FAILURE);
    }
}
