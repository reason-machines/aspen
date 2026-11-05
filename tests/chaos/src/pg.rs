//! Throwaway PostgreSQL nodes started from the machine's binaries.
//!
//! A node listens on a TCP port of 127.0.0.1 (Aspen reaches it through a
//! [`TcpProxy`](crate::proxy::TcpProxy)) and on a Unix socket in a short
//! directory under `/tmp` (the suite's `psql` checks use that, so they
//! never meet an injected fault). Its data directory and log live under
//! the run's directory; the data directory is deleted when the node is
//! dropped, the log is kept. For failover the suite runs a primary with a
//! **synchronous** standby, as managed Postgres with a standby in another
//! zone does: a commit is acknowledged only once the standby has flushed
//! it, so promoting the standby after the primary dies loses nothing that
//! was acknowledged.
//!
//! Binaries: `ASPEN_TEST_PG_BIN` (a directory holding `initdb`, `pg_ctl`,
//! `pg_basebackup` and `psql`), else the newest `/usr/lib/postgresql/<n>/bin`
//! (Debian, Ubuntu and GitHub's runners), else `PATH`.

use std::io;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use crate::process::{free_port, run_for, run_ok};

const TOOLS: [&str; 4] = ["initdb", "pg_ctl", "pg_basebackup", "psql"];

/// The directory holding the PostgreSQL binaries, if any.
pub fn find_bin() -> Option<PathBuf> {
    let has_tools = |d: &Path| TOOLS.iter().all(|t| d.join(t).is_file());
    if let Some(dir) = std::env::var_os("ASPEN_TEST_PG_BIN") {
        let dir = PathBuf::from(dir);
        return has_tools(&dir).then_some(dir);
    }
    let mut majors: Vec<(u32, PathBuf)> = std::fs::read_dir("/usr/lib/postgresql")
        .into_iter()
        .flatten()
        .flatten()
        .filter_map(|e| Some((e.file_name().to_str()?.parse().ok()?, e.path().join("bin"))))
        .collect();
    majors.sort_by_key(|m| std::cmp::Reverse(m.0));
    if let Some((_, dir)) = majors.into_iter().find(|(_, d)| has_tools(d)) {
        return Some(dir);
    }
    std::env::var_os("PATH")
        .into_iter()
        .flat_map(|p| std::env::split_paths(&p).collect::<Vec<_>>())
        .find(|d| has_tools(d))
}

const TIMEOUT: Duration = Duration::from_secs(120);

/// One PostgreSQL node (a data directory and the server running on it).
#[derive(Debug)]
pub struct PgNode {
    bin: PathBuf,
    /// Holds only the Unix socket: a short path under `/tmp`, because
    /// socket paths are limited to about 100 bytes.
    dir: PathBuf,
    /// Holds `data/` and `log`, under the run's own directory (which may
    /// be on a real disk where `/tmp` is memory).
    home: PathBuf,
    port: u16,
    name: String,
}

impl PgNode {
    /// Initialises a primary in `root/<name>` and starts it.
    pub fn primary(bin: &Path, root: &Path, name: &str) -> io::Result<Self> {
        let mut node = Self {
            bin: bin.to_owned(),
            port: free_port()?,
            dir: short_dir(name)?,
            home: home_dir(root, name)?,
            name: name.to_owned(),
        };
        run_ok(
            node.tool("initdb").arg("-D").arg(node.data()).args([
                "-U",
                "aspen",
                "-A",
                "trust",
                "-E",
                "UTF8",
                "--no-locale",
                "--no-sync",
            ]),
            TIMEOUT,
        )?;
        std::fs::write(
            node.data().join("pg_hba.conf"),
            "local all all trust\nlocal replication all trust\n\
             host all all 127.0.0.1/32 trust\nhost replication all 127.0.0.1/32 trust\n",
        )?;
        // Settings every node shares; a standby copies this file.
        std::fs::write(
            node.data().join("postgresql.auto.conf"),
            "wal_level = replica\nmax_wal_senders = 8\nhot_standby = on\n\
             max_connections = 300\nfsync = off\nfull_page_writes = off\n\
             wal_sender_timeout = 5s\nwal_receiver_timeout = 5s\n",
        )?;
        node.start_first()?;
        Ok(node)
    }

    /// A standby of `primary`, copied with `pg_basebackup` and started,
    /// streaming as `application_name = <name>`.
    pub fn standby_of(primary: &PgNode, root: &Path, name: &str) -> io::Result<Self> {
        let mut node = Self {
            bin: primary.bin.clone(),
            port: free_port()?,
            dir: short_dir(name)?,
            home: home_dir(root, name)?,
            name: name.to_owned(),
        };
        run_ok(
            node.tool("pg_basebackup")
                .arg("-D")
                .arg(node.data())
                .args([
                    "-h",
                    "127.0.0.1",
                    "-p",
                    &primary.port.to_string(),
                    "-U",
                    "aspen",
                ])
                .args(["-X", "stream", "-R", "--no-sync"]),
            TIMEOUT,
        )?;
        // `-R` wrote a primary_conninfo; name this standby in it so the
        // primary can list it as synchronous.
        let auto = node.data().join("postgresql.auto.conf");
        let mut conf = std::fs::read_to_string(&auto)?;
        conf = conf
            .lines()
            .filter(|l| {
                !l.starts_with("primary_conninfo") && !l.starts_with("synchronous_standby_names")
            })
            .collect::<Vec<_>>()
            .join("\n");
        conf.push_str(&format!(
            "\nprimary_conninfo = 'host=127.0.0.1 port={} user=aspen application_name={name}'\n",
            primary.port
        ));
        std::fs::write(&auto, conf)?;
        node.start_first()?;
        Ok(node)
    }

    fn tool(&self, name: &str) -> Command {
        let mut c = Command::new(self.bin.join(name));
        c.env_remove("PGHOST")
            .env_remove("PGPORT")
            .env_remove("PGUSER")
            .env_remove("PGDATABASE")
            .env_remove("PGPASSWORD");
        c
    }

    fn data(&self) -> PathBuf {
        self.home.join("data")
    }

    /// The label used in reports.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The node's TCP port on 127.0.0.1.
    pub fn port(&self) -> u16 {
        self.port
    }

    /// The node's address.
    pub fn addr(&self) -> std::net::SocketAddr {
        ([127, 0, 0, 1], self.port).into()
    }

    /// Starts the server and waits until it accepts connections.
    pub fn start(&self) -> io::Result<()> {
        let opts = format!(
            "-c listen_addresses=127.0.0.1 -p {} -k {}",
            self.port,
            self.dir.display()
        );
        run_ok(
            self.tool("pg_ctl")
                .arg("-D")
                .arg(self.data())
                .arg("-l")
                .arg(self.home.join("log"))
                .args(["-w", "-t", "60", "-o", &opts, "start"]),
            TIMEOUT,
        )?;
        Ok(())
    }

    /// Starts the node for the first time. Its port came from
    /// [`free_port`], and another process can take it before Postgres
    /// binds it; after a failed start the node tries a fresh port, up to
    /// five times. (A restart after a crash keeps the port: clients and the
    /// standby's `primary_conninfo` name it.)
    fn start_first(&mut self) -> io::Result<()> {
        let mut last = None;
        for _ in 0..5 {
            match self.start() {
                Ok(()) => return Ok(()),
                Err(e) => {
                    last = Some(e);
                    self.port = free_port()?;
                }
            }
        }
        Err(last.unwrap_or_else(|| io::Error::other("postgres did not start")))
    }

    /// Kills the server the way a crash does (`pg_ctl stop -m immediate`:
    /// no checkpoint, recovery from WAL at the next start).
    pub fn crash(&self) -> io::Result<()> {
        run_for(
            self.tool("pg_ctl")
                .arg("-D")
                .arg(self.data())
                .args(["-m", "immediate", "-w", "stop"]),
            TIMEOUT,
        )?;
        Ok(())
    }

    /// Promotes a standby to primary and waits until it accepts writes.
    pub fn promote(&self) -> io::Result<()> {
        run_ok(
            self.tool("pg_ctl")
                .arg("-D")
                .arg(self.data())
                .args(["-w", "-t", "60", "promote"]),
            TIMEOUT,
        )?;
        Ok(())
    }

    /// Runs `sql` with `psql` over the node's Unix socket (never through a
    /// proxy) and returns the unaligned, tuples-only output.
    pub fn psql(&self, db: &str, sql: &str) -> io::Result<String> {
        run_ok(
            self.tool("psql")
                .args(["-X", "-q", "-At", "-v", "ON_ERROR_STOP=1"])
                .arg("-h")
                .arg(&self.dir)
                .args([
                    "-p",
                    &self.port.to_string(),
                    "-U",
                    "aspen",
                    "-d",
                    db,
                    "-c",
                    sql,
                ]),
            Duration::from_secs(60),
        )
        .map(|s| s.trim().to_owned())
    }

    /// Makes every commit wait for the standby named `standby` (or for no
    /// standby, with `None`) and reloads the configuration.
    pub fn set_synchronous_standby(&self, standby: Option<&str>) -> io::Result<()> {
        let names = standby.unwrap_or("");
        self.psql(
            "postgres",
            &format!("ALTER SYSTEM SET synchronous_standby_names = '{names}'"),
        )?;
        self.psql("postgres", "SELECT pg_reload_conf()")?;
        Ok(())
    }

    /// Waits until `standby` streams from this node as a synchronous
    /// standby.
    pub fn wait_synchronous(&self, standby: &str, timeout: Duration) -> io::Result<()> {
        let deadline = Instant::now() + timeout;
        loop {
            let state = self.psql(
                "postgres",
                &format!(
                    "SELECT sync_state FROM pg_stat_replication \
                     WHERE application_name = '{standby}' AND state = 'streaming'"
                ),
            )?;
            if state == "sync" {
                return Ok(());
            }
            if Instant::now() > deadline {
                return Err(io::Error::other(format!(
                    "{standby} is not a synchronous standby of {} (state {state:?})",
                    self.name
                )));
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    /// The URL Aspen uses to reach this database through `proxy`.
    pub fn url_via(proxy: std::net::SocketAddr, db: &str) -> String {
        format!("postgres://aspen@{proxy}/{db}?sslmode=disable")
    }
}

impl Drop for PgNode {
    fn drop(&mut self) {
        let _ = self.crash();
        let _ = std::fs::remove_dir_all(&self.dir);
        let _ = std::fs::remove_dir_all(self.data());
    }
}

fn home_dir(root: &Path, name: &str) -> io::Result<PathBuf> {
    let home = root.join(format!("postgres-{name}"));
    let _ = std::fs::remove_dir_all(&home);
    std::fs::create_dir_all(&home)?;
    Ok(home)
}

fn short_dir(name: &str) -> io::Result<PathBuf> {
    Ok(tempfile::Builder::new()
        .prefix(&format!("aspen-chaos-{name}-"))
        .tempdir_in("/tmp")?
        .keep())
}
