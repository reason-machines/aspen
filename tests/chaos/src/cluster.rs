//! An Aspen deployment on one machine, wired for fault injection.
//!
//! ```text
//!  git clients ──► front proxy ──► aspen serve × N ──► db proxy ──► Postgres primary
//!                  (TcpProxy,          (real binary,      (TcpProxy)      │ synchronous
//!                   round robin)        own spool each)                   ▼ replication
//!                                            │                     Postgres standby
//!                                            └──► S3 proxy ──► S3-compatible store
//!                                                 (S3Proxy)     (MinIO in CI), or the
//!                                                               local file store
//! ```
//!
//! Every instance is a separate `aspen serve` process with the same
//! configuration, as replicas behind a load balancer are, so concurrent
//! pushes meet in the database's compare-and-swap, not in one process's
//! memory. Each runs the background worker (compaction, commit indexing,
//! garbage collection), so killing one can cut a job short.
//!
//! The child processes get a cleared environment: nothing from the machine
//! (cloud credentials included) reaches them, and the S3 endpoint must be
//! on loopback.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use aspen_testkit::git::isolated_git;
use aspen_testkit::{GitServer, RefMap};

use crate::pg::PgNode;
use crate::process::{free_port, run_for, run_ok};
use crate::proxy::TcpProxy;
use crate::report::Invariant;
use crate::s3proxy::S3Proxy;

/// An S3-compatible bucket for the suite, on loopback.
#[derive(Debug, Clone)]
pub struct S3Endpoint {
    pub addr: SocketAddr,
    pub bucket: String,
    pub access_key: String,
    pub secret_key: String,
}

impl S3Endpoint {
    /// Reads the `ASPEN_TEST_S3_*` settings that
    /// `crates/aspen-blob/scripts/test-minio.sh` prints (and CI sets).
    /// `None` when `ASPEN_TEST_S3_ENDPOINT` is unset.
    pub fn from_env() -> Option<io::Result<Self>> {
        let endpoint = std::env::var("ASPEN_TEST_S3_ENDPOINT").ok()?;
        let get =
            |k: &str| std::env::var(k).map_err(|_| io::Error::other(format!("{k} is not set")));
        Some((|| {
            let hostport = endpoint
                .strip_prefix("http://")
                .ok_or_else(|| {
                    io::Error::other("ASPEN_TEST_S3_ENDPOINT must be http://<loopback>:<port>")
                })?
                .trim_end_matches('/');
            let addr: SocketAddr = hostport
                .replace("localhost", "127.0.0.1")
                .parse()
                .map_err(|e| io::Error::other(format!("ASPEN_TEST_S3_ENDPOINT: {e}")))?;
            if !addr.ip().is_loopback() {
                return Err(io::Error::other(
                    "ASPEN_TEST_S3_ENDPOINT must be on loopback: the chaos suite only uses a local test store",
                ));
            }
            Ok(Self {
                addr,
                bucket: get("ASPEN_TEST_S3_BUCKET")?,
                access_key: get("ASPEN_TEST_S3_ACCESS_KEY_ID")?,
                secret_key: get("ASPEN_TEST_S3_SECRET_ACCESS_KEY")?,
            })
        })())
    }
}

/// How to build a [`Cluster`].
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    /// The `aspen` binary.
    pub aspen_bin: PathBuf,
    /// The PostgreSQL binaries.
    pub pg_bin: PathBuf,
    /// Working directory: spools, blobs (local store), logs.
    pub root: PathBuf,
    /// How many `aspen serve` processes.
    pub instances: usize,
    /// The S3 store, or `None` for the local file store.
    pub s3: Option<S3Endpoint>,
    /// Run a synchronous standby, so the database can fail over.
    pub standby: bool,
    /// Seeds the S3 proxy's choice of faulted requests.
    pub seed: u64,
    /// Extra environment for every `aspen serve` process, applied last
    /// (so it can override the suite's own settings; the suite's tests use
    /// it to break things on purpose).
    pub extra_env: Vec<(String, String)>,
}

/// One `aspen serve` process slot.
#[derive(Debug)]
struct Instance {
    index: usize,
    port: u16,
    metrics_port: u16,
    spool: PathBuf,
    log: PathBuf,
    child: Option<Child>,
    starts: u32,
}

/// The whole deployment; everything stops when it is dropped.
#[derive(Debug)]
pub struct Cluster {
    cfg: ClusterConfig,
    // Field order is drop order: clients' proxy and instances first, the
    // database last.
    front: TcpProxy,
    instances: Vec<Mutex<Instance>>,
    s3_proxy: Option<S3Proxy>,
    db_proxy: TcpProxy,
    primary: Mutex<PgNode>,
    standby: Mutex<Option<PgNode>>,
    standbys_made: AtomicU32,
    env: Vec<(String, String)>,
    /// Alert counters summed over every scrape (instances lose theirs when
    /// they restart, so each is scraped before it is killed).
    alerts: Mutex<BTreeMap<String, u64>>,
    /// The alert counters every scrape must contain.
    required_alerts: Vec<&'static str>,
    /// The front proxy's upstreams, one per instance slot.
    upstreams: Mutex<Vec<SocketAddr>>,
    /// Problems seen since [`crate::Deployment::take_findings`] last ran.
    problems: Mutex<Vec<(Invariant, String)>>,
}

/// Alert counters `aspen serve` always exports
/// (`crates/aspen-server/src/metrics/mod.rs`), at zero until they page.
pub const SERVER_ALERTS: [&str; 2] = [
    "aspen_alert_stored_pack_forward_base_total",
    "aspen_alert_store_corruption_total",
];

/// The alert counter `aspen serve` exports when it serves `/-/express`
/// (`crates/aspen-express/src/metrics.rs`).
pub const EXPRESS_ALERT: &str = "aspen_alert_express_corrupt_pack_total";

/// Grants for anonymous callers: the suite tests storage, not access.
const POLICY: &str = r#"{"anonymous": ["write:*"]}"#;

impl Cluster {
    /// Starts the database (and standby), the proxies and every instance,
    /// and waits until all instances are ready.
    pub fn start(cfg: ClusterConfig) -> io::Result<Self> {
        fs::create_dir_all(&cfg.root)?;
        let primary = PgNode::primary(&cfg.pg_bin, &cfg.root, "primary")?;
        let standby = if cfg.standby {
            let s = PgNode::standby_of(&primary, &cfg.root, "standby1")?;
            primary.set_synchronous_standby(Some("standby1"))?;
            primary.wait_synchronous("standby1", Duration::from_secs(60))?;
            Some(s)
        } else {
            None
        };
        let db_proxy = TcpProxy::start("db", vec![primary.addr()])?;
        let s3_proxy = match &cfg.s3 {
            Some(s3) => Some(S3Proxy::start(s3.addr, cfg.seed ^ 0x5353)?),
            None => None,
        };

        let mut env = vec![
            (
                "DATABASE_URL".into(),
                PgNode::url_via(db_proxy.addr(), "postgres"),
            ),
            ("ASPEN_POLICY".into(), POLICY.into()),
            ("ASPEN_LOG".into(), "info".into()),
        ];
        // The front proxy binds its own port, so nothing can take it first;
        // it learns the instances' ports below.
        let front = TcpProxy::start("front", Vec::new())?;
        let public = format!("http://{}", front.addr());
        let mut required_alerts = SERVER_ALERTS.to_vec();
        match (&cfg.s3, &s3_proxy) {
            (Some(s3), Some(proxy)) => {
                // Git before 2.56 only takes Aspen-served packfile URIs
                // (spike S3), so with S3 every instance also serves
                // express URLs through the front proxy; without them
                // every fetch would quietly get a stitched pack.
                required_alerts.push(EXPRESS_ALERT);
                let prefix = format!("chaos-{:016x}", cfg.seed ^ nonce());
                env.extend([
                    (
                        "ASPEN_BLOB_URL".into(),
                        format!("s3://{}/{prefix}", s3.bucket),
                    ),
                    (
                        "AWS_ENDPOINT_URL".into(),
                        format!("http://{}", proxy.addr()),
                    ),
                    ("AWS_ALLOW_HTTP".into(), "true".into()),
                    ("AWS_ACCESS_KEY_ID".into(), s3.access_key.clone()),
                    ("AWS_SECRET_ACCESS_KEY".into(), s3.secret_key.clone()),
                    ("AWS_REGION".into(), "us-east-1".into()),
                    ("AWS_EC2_METADATA_DISABLED".into(), "true".into()),
                    ("ASPEN_EXPRESS_SERVE".into(), "true".into()),
                    ("ASPEN_EXPRESS_URLS".into(), format!("{public}/-/express")),
                    (
                        "ASPEN_EXPRESS_KEYS".into(),
                        format!("k1:{}", format!("{:016x}", nonce()).repeat(4)),
                    ),
                ]);
            }
            _ => {
                let blobs = cfg.root.join("blobs");
                fs::create_dir_all(&blobs)?;
                env.extend([
                    (
                        "ASPEN_BLOB_URL".into(),
                        format!("file://{}", blobs.display()),
                    ),
                    (
                        "ASPEN_BLOB_URL_SECRET".into(),
                        format!("{:016x}", nonce()).repeat(4),
                    ),
                    ("ASPEN_PUBLIC_URL".into(), public.clone()),
                ]);
            }
        }

        let mut instances = Vec::new();
        for index in 0..cfg.instances.max(1) {
            let spool = cfg.root.join(format!("spool-{index}"));
            fs::create_dir_all(&spool)?;
            instances.push(Mutex::new(Instance {
                index,
                port: free_port()?,
                metrics_port: free_port()?,
                spool,
                log: cfg.root.join(format!("aspen-{index}.log")),
                child: None,
                starts: 0,
            }));
        }
        let upstreams: Vec<SocketAddr> = instances
            .iter()
            .map(|i| SocketAddr::from(([127, 0, 0, 1], lock(i).port)))
            .collect();
        front.retarget(upstreams.clone());
        let cluster = Self {
            cfg,
            front,
            instances,
            s3_proxy,
            db_proxy,
            primary: Mutex::new(primary),
            standby: Mutex::new(standby),
            standbys_made: AtomicU32::new(1),
            env,
            alerts: Mutex::new(BTreeMap::new()),
            required_alerts,
            upstreams: Mutex::new(upstreams),
            problems: Mutex::new(Vec::new()),
        };
        // The first instance applies the migrations before the others start.
        for i in 0..cluster.instances.len() {
            cluster.start_instance(i)?;
        }
        Ok(cluster)
    }

    /// The URL clients use, through the front proxy.
    pub fn base_url(&self) -> String {
        format!("http://{}", self.front.addr())
    }

    /// How many instance slots there are.
    pub fn instance_count(&self) -> usize {
        self.instances.len()
    }

    /// The proxy between clients and the instances.
    pub fn front(&self) -> &TcpProxy {
        &self.front
    }

    /// The proxy between the instances and Postgres.
    pub fn db_proxy(&self) -> &TcpProxy {
        &self.db_proxy
    }

    /// The proxy between the instances and S3, when S3 is used.
    pub fn s3_proxy(&self) -> Option<&S3Proxy> {
        self.s3_proxy.as_ref()
    }

    /// Whether the deployment has a standby to fail over to.
    pub fn has_standby(&self) -> bool {
        lock(&self.standby).is_some()
    }

    fn aspen(&self) -> Command {
        let mut c = Command::new(&self.cfg.aspen_bin);
        c.env_clear()
            .env("PATH", std::env::var_os("PATH").unwrap_or_default())
            .env("HOME", &self.cfg.root)
            .envs(self.env.iter().map(|(k, v)| (k, v)));
        c
    }

    fn problem(&self, invariant: Invariant, detail: String) {
        lock(&self.problems).push((invariant, detail));
    }

    /// Starts instance `i` if it is not running, and waits until it is
    /// ready (`/readyz` answers 200). An instance that exited without
    /// [`Cluster::kill_instance`] is recorded as [`Invariant::Crashed`]
    /// before it is started again.
    pub fn start_instance(&self, i: usize) -> io::Result<()> {
        let mut inst = lock(&self.instances[i]);
        if let Some(child) = inst.child.as_mut() {
            match child.try_wait()? {
                None => return Ok(()),
                Some(status) => {
                    let detail = format!(
                        "aspen instance {i} (pid {}) exited on its own ({status}); see {}",
                        child.id(),
                        inst.log.display()
                    );
                    inst.child = None;
                    self.problem(Invariant::Crashed, detail);
                }
            }
        }
        // The ports came from `free_port`, and another process can take
        // one before Aspen binds it: then try fresh ports.
        let mut tries = 0;
        loop {
            tries += 1;
            match self.spawn_instance(&mut inst, i) {
                Err(e) if tries < 5 && e.kind() == io::ErrorKind::AddrInUse => {
                    inst.port = free_port()?;
                    inst.metrics_port = free_port()?;
                    let mut ups = lock(&self.upstreams);
                    ups[i] = SocketAddr::from(([127, 0, 0, 1], inst.port));
                    self.front.retarget(ups.clone());
                }
                other => return other,
            }
        }
    }

    /// Spawns `aspen serve` in slot `i` and waits until it is ready. A
    /// start that fails because a port is taken returns
    /// [`io::ErrorKind::AddrInUse`].
    fn spawn_instance(&self, inst: &mut Instance, i: usize) -> io::Result<()> {
        let log = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&inst.log)?;
        writeln!(
            &log,
            "---- chaos: start #{} of instance {i}",
            inst.starts + 1
        )?;
        let mut cmd = self.aspen();
        cmd.arg("serve")
            .env("ASPEN_LISTEN", format!("127.0.0.1:{}", inst.port))
            .env(
                "ASPEN_METRICS_LISTEN",
                format!("127.0.0.1:{}", inst.metrics_port),
            )
            .env("ASPEN_SPOOL_DIR", &inst.spool)
            // Job leases name the host; this maps a lease to its instance.
            .env("HOSTNAME", format!("chaos{i}"))
            .envs(self.cfg.extra_env.iter().map(|(k, v)| (k, v)))
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::from(log));
        inst.child = Some(cmd.spawn()?);
        inst.starts += 1;
        let port = inst.port;
        let deadline = Instant::now() + Duration::from_secs(90);
        loop {
            if http_get(port, "/readyz").is_ok_and(|(code, _)| code == 200) {
                return Ok(());
            }
            if let Some(status) = inst
                .child
                .as_mut()
                .and_then(|c| c.try_wait().ok().flatten())
            {
                inst.child = None;
                let log = fs::read_to_string(&inst.log).unwrap_or_default();
                let tail = log.rsplit("---- chaos: start").next().unwrap_or("");
                let kind = if tail.contains("Address already in use") {
                    io::ErrorKind::AddrInUse
                } else {
                    io::ErrorKind::Other
                };
                return Err(io::Error::new(
                    kind,
                    format!(
                        "aspen instance {i} exited at startup ({status}); see {}",
                        inst.log.display()
                    ),
                ));
            }
            if Instant::now() > deadline {
                return Err(io::Error::other(format!(
                    "aspen instance {i} was not ready within 90 s; see {}",
                    inst.log.display()
                )));
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    /// Whether instance `i` is running.
    pub fn is_running(&self, i: usize) -> bool {
        let mut inst = lock(&self.instances[i]);
        inst.child
            .as_mut()
            .is_some_and(|c| matches!(c.try_wait(), Ok(None)))
    }

    /// Kills instance `i` with SIGKILL, after reading its alert counters,
    /// and expires its job leases (see [`Cluster::expire_leases_of`]).
    /// Returns whether it was running.
    pub fn kill_instance(&self, i: usize) -> io::Result<bool> {
        let mut inst = lock(&self.instances[i]);
        let Some(mut child) = inst.child.take() else {
            return Ok(false);
        };
        if let Some(status) = child.try_wait()? {
            self.problem(
                Invariant::Crashed,
                format!(
                    "aspen instance {i} (pid {}) had exited on its own ({status}) before the \
                     suite killed it; see {}",
                    child.id(),
                    inst.log.display()
                ),
            );
            return Ok(false);
        }
        if let Err(e) = self.scrape_alerts(inst.metrics_port) {
            self.problem(
                Invariant::Alert,
                format!("instance {i} before it was killed: {e}"),
            );
        }
        let pid = child.id();
        child.kill()?;
        child.wait()?;
        let _ = writeln!(
            OpenOptions::new().append(true).open(&inst.log)?,
            "---- chaos: SIGKILL instance {} (pid {pid})",
            inst.index
        );
        drop(inst);
        self.expire_leases_of(i, pid)?;
        Ok(true)
    }

    /// Ends the job leases a killed instance held, as if its lease time
    /// had passed. Leases last 10 minutes (`WorkerOptions::lease`); without
    /// this a job cut short by a kill would wait that long before another
    /// worker may claim it. Only leases of the dead process change, so no
    /// live worker is affected.
    pub fn expire_leases_of(&self, i: usize, pid: u32) -> io::Result<()> {
        self.sql(&format!(
            "UPDATE jobs SET lease_expires_us = 0 WHERE state = 'running' \
             AND lease_owner LIKE 'aspen-serve:chaos{i}:{pid}:%'"
        ))
        .map(drop)
    }

    /// Runs `sql` on the current primary over its Unix socket.
    pub fn sql(&self, sql: &str) -> io::Result<String> {
        lock(&self.primary).psql("postgres", sql)
    }

    /// Running jobs: `(kind, instance index, pid)` of each lease holder.
    pub fn running_jobs(&self) -> io::Result<Vec<(String, usize, u32)>> {
        let out =
            self.sql("SELECT kind || ' ' || lease_owner FROM jobs WHERE state = 'running'")?;
        Ok(out
            .lines()
            .filter_map(|l| {
                let (kind, owner) = l.split_once(' ')?;
                // aspen-serve:chaos<i>:<pid>:<nonce>
                let mut parts = owner.split(':');
                let _label = parts.next()?;
                let index = parts.next()?.strip_prefix("chaos")?.parse().ok()?;
                let pid = parts.next()?.parse().ok()?;
                Some((kind.to_owned(), index, pid))
            })
            .collect())
    }

    /// The pid of instance `i`, if running.
    pub fn pid(&self, i: usize) -> Option<u32> {
        lock(&self.instances[i]).child.as_ref().map(Child::id)
    }

    /// Crashes Postgres (immediate stop, no checkpoint) and starts it
    /// again on the same data directory.
    pub fn crash_and_restart_database(&self, down: Duration) -> io::Result<()> {
        let primary = lock(&self.primary);
        primary.crash()?;
        std::thread::sleep(down);
        primary.start()
    }

    /// Fails the database over: crashes the primary, promotes the
    /// synchronous standby, points the proxy at it and closes every open
    /// connection, then builds a new synchronous standby from the new
    /// primary. Returns a description, or `None` without a ready standby.
    pub fn fail_over(&self) -> io::Result<Option<String>> {
        let mut primary = lock(&self.primary);
        let mut standby = lock(&self.standby);
        let Some(next) = standby.take() else {
            return Ok(None);
        };
        let old = primary.name().to_owned();
        primary.crash()?;
        next.promote()?;
        // Until a new standby is in sync, commits are acknowledged by the
        // new primary alone (as a managed database does while it rebuilds
        // its standby); no second failover happens in that window.
        next.set_synchronous_standby(None)?;
        self.db_proxy.retarget(vec![next.addr()]);
        self.db_proxy.close_all();
        let promoted = next.name().to_owned();
        *primary = next;
        let n = self.standbys_made.fetch_add(1, Ordering::SeqCst) + 1;
        let name = format!("standby{n}");
        let fresh = PgNode::standby_of(&primary, &self.cfg.root, &name)?;
        primary.set_synchronous_standby(Some(&name))?;
        primary.wait_synchronous(&name, Duration::from_secs(60))?;
        *standby = Some(fresh);
        Ok(Some(format!(
            "failed over from {old} to {promoted}; new standby {name}"
        )))
    }

    /// Clears every proxy fault and starts every instance that is down.
    pub fn heal(&self) -> io::Result<()> {
        self.front.heal();
        self.db_proxy.heal();
        if let Some(s3) = &self.s3_proxy {
            s3.heal();
        }
        for i in 0..self.instances.len() {
            self.start_instance(i)?;
        }
        Ok(())
    }

    /// Reads one instance's alert counters and adds them to the totals.
    /// Fails if `/-/metrics` does not answer 200, a counter's value does
    /// not parse, or a counter every scrape must have is missing (a renamed
    /// counter would otherwise read as zero forever).
    fn scrape_alerts(&self, metrics_port: u16) -> Result<(), String> {
        let body = match http_get(metrics_port, "/-/metrics") {
            Ok((200, body)) => body,
            Ok((code, _)) => {
                return Err(format!(
                    "GET /-/metrics on port {metrics_port} answered {code}"
                ));
            }
            Err(e) => return Err(format!("GET /-/metrics on port {metrics_port}: {e}")),
        };
        let read = parse_alerts(&body, &self.required_alerts)?;
        let mut alerts = lock(&self.alerts);
        for (name, value) in read {
            *alerts.entry(name).or_default() += value;
        }
        Ok(())
    }

    /// Alert counters (`aspen_alert_*`) summed over every instance and
    /// every restart. Reads every instance now: one that cannot be read is
    /// an [`Invariant::Alert`] problem, and one that is not running is an
    /// [`Invariant::Crashed`] problem (the final checks heal first, so an
    /// instance down here died after that, say in a panic while the job
    /// queue drained). Its counters died with it, so they cannot count as
    /// zero.
    pub fn alerts(&self) -> BTreeMap<String, u64> {
        for i in 0..self.instances.len() {
            let mut inst = lock(&self.instances[i]);
            let port = inst.metrics_port;
            let down = match inst.child.as_mut().map(Child::try_wait) {
                Some(Ok(None)) => None,
                Some(Ok(Some(status))) => Some(format!("exited on its own ({status})")),
                Some(Err(e)) => Some(format!("could not be waited on: {e}")),
                None => Some("had been killed and was never started again".to_owned()),
            };
            if let Some(why) = down {
                let pid = inst.child.take().map_or(0, |c| c.id());
                self.problem(
                    Invariant::Crashed,
                    format!(
                        "aspen instance {i} (pid {pid}) was not running at the final alert \
                         scrape: it {why}; see {}",
                        inst.log.display()
                    ),
                );
                continue;
            }
            drop(inst);
            if let Err(e) = self.scrape_alerts(port) {
                self.problem(Invariant::Alert, format!("instance {i}: {e}"));
            }
        }
        lock(&self.alerts).clone()
    }

    /// How many times each instance was started.
    pub fn starts(&self) -> Vec<u32> {
        self.instances.iter().map(|i| lock(i).starts).collect()
    }

    /// Creates repository `name` with `aspen repo create`, straight on the
    /// primary.
    fn create(&self, name: &str) -> io::Result<()> {
        let url = format!(
            "postgres://aspen@127.0.0.1:{}/postgres?sslmode=disable",
            lock(&self.primary).port()
        );
        run_ok(
            self.aspen()
                .env("DATABASE_URL", url)
                .args(["repo", "create", name]),
            Duration::from_secs(60),
        )
        .map(drop)
    }

    /// What the database says about stuck work: open jobs, sessions that
    /// are not idle (with what they wait on) and replication. For a
    /// liveness finding.
    pub fn diagnostics(&self) -> String {
        let mut out = String::new();
        for (label, sql) in [
            (
                "open jobs",
                "SELECT id || ' ' || kind || ' ' || state || ' attempts=' || attempts || ' owner=' || \
                 coalesce(lease_owner, '-') || ' lease_expires_us=' || coalesce(lease_expires_us::text, '-') || \
                 ' error=' || coalesce(left(last_error, 200), '-') FROM jobs WHERE state IN ('queued', 'running') \
                 ORDER BY id",
            ),
            (
                "sessions",
                "SELECT pid || ' ' || state || ' wait=' || coalesce(wait_event_type, '-') || '/' || \
                 coalesce(wait_event, '-') || ' for ' || coalesce((clock_timestamp() - state_change)::text, '-') || \
                 ': ' || left(query, 160) FROM pg_stat_activity WHERE backend_type = 'client backend' \
                 AND state <> 'idle' AND pid <> pg_backend_pid()",
            ),
            (
                "replication",
                "SELECT application_name || ' ' || state || ' ' || sync_state FROM pg_stat_replication",
            ),
            (
                "synchronous_standby_names",
                "SHOW synchronous_standby_names",
            ),
        ] {
            let text = self
                .sql(sql)
                .unwrap_or_else(|e| format!("(could not read: {e})"));
            out.push_str(&format!("--- {label}:\n{text}\n"));
        }
        for i in 0..self.instances.len() {
            out.push_str(&format!("--- instance {i}: pid {:?}\n", self.pid(i)));
        }
        out
    }

    /// Where logs and spools are.
    pub fn root(&self) -> &Path {
        &self.cfg.root
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for inst in &self.instances {
            if let Some(mut c) = lock(inst).child.take() {
                let _ = c.kill();
                let _ = c.wait();
            }
        }
    }
}

impl GitServer for Cluster {
    fn name(&self) -> &str {
        "aspen"
    }

    fn create_repo(&self, name: &str) -> io::Result<String> {
        let repo = name.strip_suffix(".git").unwrap_or(name);
        self.create(repo)?;
        Ok(format!("{}/{repo}.git", self.base_url()))
    }

    fn branch_url(&self, repo_url: &str, refname: &str) -> String {
        format!("{repo_url}/-/{refname}")
    }

    fn list_refs(&self, repo_url: &str) -> io::Result<RefMap> {
        // Protocol v2 `ls-refs` without a prefix lists every ref, hidden
        // or not.
        let home = tempfile::tempdir()?;
        let out = run_ok(
            isolated_git(home.path()).args(["-c", "protocol.version=2", "ls-remote", repo_url]),
            Duration::from_secs(60),
        )?;
        Ok(out
            .lines()
            .filter_map(|l| {
                let (oid, name) = l.split_once('\t')?;
                (name.starts_with("refs/") && !name.ends_with("^{}"))
                    .then(|| (name.to_owned(), oid.to_owned()))
            })
            .collect())
    }
}

/// The `aspen_alert_*` counters in a Prometheus text exposition, summed
/// over labels. Fails on a value that is not a non-negative number and on
/// a missing `required` name.
pub fn parse_alerts(body: &str, required: &[&str]) -> Result<BTreeMap<String, u64>, String> {
    let mut out = BTreeMap::new();
    for line in body.lines().filter(|l| l.starts_with("aspen_alert_")) {
        let (series, value) = line
            .rsplit_once(' ')
            .ok_or_else(|| format!("unparsable metrics line {line:?}"))?;
        let name = series.split('{').next().unwrap_or(series).to_owned();
        let v: f64 = value
            .trim()
            .parse()
            .map_err(|_| format!("{name}: value {value:?} is not a number"))?;
        if !(v.is_finite() && v >= 0.0) {
            return Err(format!("{name}: value {value:?} is not a count"));
        }
        *out.entry(name).or_default() += v as u64;
    }
    let missing: Vec<&str> = required
        .iter()
        .copied()
        .filter(|r| !out.contains_key(*r))
        .collect();
    if !missing.is_empty() {
        return Err(format!(
            "alert counter(s) missing from /-/metrics: {}",
            missing.join(", ")
        ));
    }
    Ok(out)
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// A process-unique value for names (not a secret: the suite's stores
/// are local and throwaway).
fn nonce() -> u64 {
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    t ^ (u64::from(std::process::id()) << 32)
}

/// `GET path` on 127.0.0.1:`port`: status code and body.
fn http_get(port: u16, path: &str) -> io::Result<(u16, String)> {
    let mut s = TcpStream::connect_timeout(&([127, 0, 0, 1], port).into(), Duration::from_secs(2))?;
    s.set_read_timeout(Some(Duration::from_secs(5)))?;
    write!(
        s,
        "GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n"
    )?;
    let mut text = String::new();
    s.read_to_string(&mut text)?;
    let code = text
        .split(' ')
        .nth(1)
        .and_then(|c| c.parse().ok())
        .ok_or_else(|| io::Error::other("no status line"))?;
    let body = text
        .split_once("\r\n\r\n")
        .map(|(_, b)| b.to_owned())
        .unwrap_or_default();
    Ok((code, body))
}

/// Finds the `aspen` binary: `ASPEN_BIN`; else the one next to the running
/// executable (`target/<profile>/aspen`). Under cargo (`CARGO` is set, as
/// in tests) a missing binary, or one older than any file under `crates/`
/// or the workspace's `Cargo.toml` and `Cargo.lock`, is built with that
/// cargo first, so a test never runs a stale server. (Not unconditionally:
/// `cargo build -p aspen-cli` resolves features differently from
/// `cargo test --workspace`, and rebuilt much of the graph in required CI,
/// about three minutes, for a binary that was already current.)
pub fn find_aspen_bin() -> io::Result<PathBuf> {
    if let Some(bin) = std::env::var_os("ASPEN_BIN") {
        return Ok(PathBuf::from(bin));
    }
    let exe = std::env::current_exe()?;
    let mut dir = exe.parent().map(Path::to_path_buf);
    // A test binary sits in target/<profile>/deps.
    if dir.as_ref().is_some_and(|d| d.ends_with("deps")) {
        dir = dir.and_then(|d| d.parent().map(Path::to_path_buf));
    }
    let dir = dir.ok_or_else(|| io::Error::other("no directory for the current executable"))?;
    let candidate = dir.join("aspen");
    if let Some(cargo) = std::env::var_os("CARGO")
        && !is_current(&candidate)
    {
        let mut cmd = Command::new(cargo);
        cmd.args(["build", "--locked", "-p", "aspen-cli", "--bin", "aspen"]);
        if dir.ends_with("release") {
            cmd.arg("--release");
        }
        let done = run_for(&mut cmd, Duration::from_secs(3600))?;
        if done.success() && candidate.is_file() {
            return Ok(candidate);
        }
        return Err(io::Error::other(format!(
            "building the aspen binary failed: {}",
            done.describe()
        )));
    }
    if candidate.is_file() {
        return Ok(candidate);
    }
    Err(io::Error::other(format!(
        "no aspen binary at {}; run `cargo build -p aspen-cli` or set ASPEN_BIN",
        candidate.display()
    )))
}

/// Whether `bin` exists and is newer than every source it is built from.
fn is_current(bin: &Path) -> bool {
    let Ok(built) = fs::metadata(bin).and_then(|m| m.modified()) else {
        return false;
    };
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let mut newest = std::time::SystemTime::UNIX_EPOCH;
    let mut note = |p: &Path| {
        if let Ok(t) = fs::metadata(p).and_then(|m| m.modified()) {
            newest = newest.max(t);
        }
    };
    note(&root.join("Cargo.toml"));
    note(&root.join("Cargo.lock"));
    let mut stack = vec![root.join("crates")];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = fs::read_dir(&dir) else {
            continue;
        };
        for e in entries.flatten() {
            let path = e.path();
            match e.file_type() {
                Ok(t) if t.is_dir() => {
                    if e.file_name() != "target" {
                        stack.push(path);
                    }
                }
                Ok(_) => note(&path),
                Err(_) => {}
            }
        }
    }
    built >= newest
}

/// Writes `text` to `path`, creating parent directories.
pub fn write_file(path: &Path, text: &str) -> io::Result<()> {
    if let Some(p) = path.parent() {
        fs::create_dir_all(p)?;
    }
    File::create(path)?.write_all(text.as_bytes())
}

impl crate::run::Deployment for Cluster {
    fn heal(&self) -> io::Result<()> {
        Cluster::heal(self)
    }

    /// After the last epoch: every instance is up and no fault is active.
    /// Background work must finish: no job may stay running or due but
    /// unclaimed for long. Dead jobs are reported as warnings (they page in
    /// production, and under injected faults a job can legitimately run out
    /// of attempts). Every `aspen_alert_*` counter must still be zero.
    fn take_findings(&self, epoch: usize) -> Vec<crate::report::Finding> {
        std::mem::take(&mut *lock(&self.problems))
            .into_iter()
            .map(|(inv, detail)| crate::report::Finding::new(inv, epoch, detail))
            .collect()
    }

    fn final_checks(&self, report: &mut crate::report::Report) {
        use crate::report::Finding;
        if let Err(e) = Cluster::heal(self) {
            report.violations.push(Finding::new(
                Invariant::Unavailable,
                usize::MAX,
                format!("could not bring every instance back: {e}"),
            ));
            return;
        }
        let now_us = "(EXTRACT(EPOCH FROM clock_timestamp()) * 1000000)::BIGINT";
        let open_sql = format!(
            "SELECT kind || ' ' || state || ' ' || count(*) FROM jobs \
             WHERE state = 'running' OR (state = 'queued' AND run_after_us <= {now_us}) \
             GROUP BY kind, state ORDER BY 1"
        );
        // First let the workers drain the queue on their own. Then end the
        // leases live workers still hold (what their 10-minute lease clock
        // would do) and give the queue as long again. A job that needed
        // that second step was dropped by its worker without being
        // recorded as done or failed: a liveness gap, reported as a
        // warning. A job that does not finish even then is a violation.
        let drain = |limit: Duration| -> Result<String, String> {
            let deadline = Instant::now() + limit;
            loop {
                // Leases held by processes that no longer exist (a kill
                // left them) end now, as their lease time would.
                if let Ok(running) = self.running_jobs() {
                    for (_, i, pid) in running {
                        if self.pid(i) != Some(pid) {
                            let _ = self.expire_leases_of(i, pid);
                        }
                    }
                }
                match self.sql(&open_sql) {
                    Ok(open) if open.is_empty() => return Ok(String::new()),
                    Ok(open) if Instant::now() > deadline => return Ok(open),
                    Ok(_) => {}
                    Err(e) => return Err(e.to_string()),
                }
                std::thread::sleep(Duration::from_secs(1));
            }
        };
        let limit = Duration::from_secs(90);
        match drain(limit) {
            Ok(open) if open.is_empty() => {}
            Ok(open) => {
                let before = self.diagnostics();
                let _ = self.sql("UPDATE jobs SET lease_expires_us = 0 WHERE state = 'running'");
                match drain(limit) {
                    Ok(still) if still.is_empty() => report.warnings.push(format!(
                        "jobs stayed leased by live workers {limit:?} after the faults stopped, and \
                         finished only once their leases were ended (their workers had dropped them \
                         without recording an outcome; without the suite they would wait out the \
                         10-minute lease; issue #131):\n{open}\n{before}"
                    )),
                    Ok(still) => report.violations.push(Finding::new(
                        Invariant::Liveness,
                        usize::MAX,
                        format!(
                            "jobs still running or due {limit:?} after the faults stopped and \
                             another {limit:?} after their leases were ended:\n{still}\n{}",
                            self.diagnostics()
                        ),
                    )),
                    Err(e) => report.violations.push(Finding::new(
                        Invariant::Unavailable,
                        usize::MAX,
                        format!("the job queue could not be read: {e}"),
                    )),
                }
            }
            Err(e) => report.violations.push(Finding::new(
                Invariant::Unavailable,
                usize::MAX,
                format!("the job queue could not be read: {e}"),
            )),
        }
        if let Ok(waiting) = self.sql(&format!(
            "SELECT count(*) FROM jobs WHERE state = 'queued' AND run_after_us > {now_us}"
        )) && waiting != "0"
        {
            report.notes.push(format!(
                "{waiting} job(s) waiting out a retry backoff at the end"
            ));
        }
        if let Ok(dead) = self.sql(
            "SELECT id || ' ' || kind || ' after ' || attempts || ' attempts: ' || \
             coalesce(last_error, '') FROM jobs WHERE state = 'dead' ORDER BY id",
        ) {
            for line in dead.lines().filter(|l| !l.is_empty()) {
                report.warnings.push(format!("dead job {line}"));
            }
        }
        for (label, sql) in [
            ("ref versions", "SELECT count(*) FROM ref_versions"),
            ("packs", "SELECT count(*) FROM packs"),
            (
                "covers with a pack",
                "SELECT count(*) FROM covers WHERE pack_id IS NOT NULL",
            ),
            (
                "jobs done",
                "SELECT count(*) FROM jobs WHERE state = 'done'",
            ),
        ] {
            if let Ok(n) = self.sql(sql)
                && let Ok(n) = n.parse::<u64>()
            {
                report.count(format!("db: {label}"), n);
            }
        }
        let starts: u32 = self.starts().iter().sum();
        report.count("instance starts", u64::from(starts));
        if let Some(s3) = self.s3_proxy() {
            let c = s3.counts();
            for (k, v) in [
                ("s3: requests", c.requests),
                ("s3: 503 slow down", c.slow_down),
                ("s3: 500 internal error", c.internal_error),
                ("s3: closed before forwarding", c.close_before),
                ("s3: closed after forwarding", c.close_after),
                ("s3: truncated answer", c.truncated),
                ("s3: delayed", c.delayed),
                ("s3: stalled", c.stalled),
            ] {
                report.count(k, v);
            }
        }
        let alerts = self.alerts();
        for (name, value) in &alerts {
            if *value > 0 {
                report.violations.push(Finding::new(
                    Invariant::Alert,
                    usize::MAX,
                    format!(
                        "{name} = {value}; see the instance logs in {}",
                        self.root().display()
                    ),
                ));
            }
        }
        report.notes.push(format!(
            "alert counters read: {}",
            if alerts.is_empty() {
                "none exported".to_owned()
            } else {
                alerts
                    .iter()
                    .map(|(k, v)| format!("{k}={v}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            }
        ));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alert_counters_are_summed_and_checked() {
        let body = "# HELP aspen_alert_store_corruption_total x\n\
                    # TYPE aspen_alert_store_corruption_total counter\n\
                    aspen_alert_store_corruption_total 0\n\
                    aspen_alert_stored_pack_forward_base_total{repo=\"a\"} 2\n\
                    aspen_alert_stored_pack_forward_base_total{repo=\"b\"} 1\n\
                    aspen_fetch_total 9\n";
        let read = parse_alerts(body, &SERVER_ALERTS).unwrap();
        assert_eq!(read["aspen_alert_store_corruption_total"], 0);
        assert_eq!(read["aspen_alert_stored_pack_forward_base_total"], 3);
        // A renamed or missing counter must not read as zero.
        let err = parse_alerts(body, &[SERVER_ALERTS[0], EXPRESS_ALERT]).unwrap_err();
        assert!(err.contains(EXPRESS_ALERT), "{err}");
        // An unparsable value is an error, not zero.
        let err = parse_alerts("aspen_alert_store_corruption_total NaNx\n", &[]).unwrap_err();
        assert!(err.contains("not a number"), "{err}");
    }

    #[test]
    fn a_scrape_of_a_dead_port_fails() {
        let port = free_port().unwrap();
        assert!(http_get(port, "/-/metrics").is_err());
    }
}
