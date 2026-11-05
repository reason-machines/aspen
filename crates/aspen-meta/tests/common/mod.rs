//! Helpers shared by the integration tests: a throwaway PostgreSQL cluster
//! started from the local binaries (`initdb` + `pg_ctl`, listening only on
//! a Unix socket in a temporary directory).
//!
//! GitHub's `ubuntu-24.04` runners ship the binaries under
//! `/usr/lib/postgresql/<major>/bin`; elsewhere set `ASPEN_TEST_PG_BIN` to a
//! directory holding `initdb` and `pg_ctl`. Without binaries, Postgres
//! tests skip locally and fail under `CI` ([`pg_or_skip`]).

// Each test binary uses a different subset of these helpers.
#![allow(dead_code, unreachable_pub)]

use std::path::{Path, PathBuf};
use std::process::Command;

/// The binaries directory, or `None` to skip (only outside CI).
pub fn pg_or_skip() -> Option<PathBuf> {
    let bin = pg_bin_dir();
    if bin.is_none() {
        assert!(
            std::env::var_os("CI").is_none(),
            "PostgreSQL binaries not found; set ASPEN_TEST_PG_BIN to a directory holding initdb and pg_ctl"
        );
        eprintln!("skipping: PostgreSQL binaries not found (set ASPEN_TEST_PG_BIN)");
    }
    bin
}

/// The directory holding `initdb` and `pg_ctl`.
pub fn pg_bin_dir() -> Option<PathBuf> {
    let has_tools = |d: &Path| d.join("initdb").is_file() && d.join("pg_ctl").is_file();
    if let Some(dir) = std::env::var_os("ASPEN_TEST_PG_BIN") {
        let dir = PathBuf::from(dir);
        assert!(
            has_tools(&dir),
            "ASPEN_TEST_PG_BIN={} has no initdb/pg_ctl",
            dir.display()
        );
        return Some(dir);
    }
    // Debian and Ubuntu: /usr/lib/postgresql/<major>/bin, newest first.
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

/// A throwaway cluster on a Unix socket, stopped on drop.
pub struct PgCluster {
    bin: PathBuf,
    dir: tempfile::TempDir,
}

impl PgCluster {
    pub fn start(bin: &Path) -> PgCluster {
        // Unix socket paths are limited to about 100 bytes, so stay short.
        let dir = tempfile::Builder::new()
            .prefix("aspen-pg-")
            .tempdir_in("/tmp")
            .unwrap();
        let data = dir.path().join("data");
        run(Command::new(bin.join("initdb")).arg("-D").arg(&data).args([
            "-U",
            "aspen",
            "-A",
            "trust",
            "-E",
            "UTF8",
            "--no-locale",
            "--no-sync",
        ]));
        let opts = format!(
            "-k {} -c listen_addresses='' -p 5432 -c fsync=off",
            dir.path().display()
        );
        run(Command::new(bin.join("pg_ctl"))
            .arg("-D")
            .arg(&data)
            .arg("-l")
            .arg(dir.path().join("log"))
            .args(["-w", "-o", &opts, "start"]));
        PgCluster {
            bin: bin.to_owned(),
            dir,
        }
    }

    /// A cluster that also listens on 127.0.0.1 with TLS (a throwaway
    /// self-signed certificate) and accepts TCP clients only over TLS with
    /// a SCRAM password: `pg_hba.conf` has a `hostssl` line and no `host`
    /// line. Returns the cluster and its TCP port.
    pub fn start_tls(bin: &Path, password: &str) -> (PgCluster, u16) {
        let dir = tempfile::Builder::new()
            .prefix("aspen-pg-")
            .tempdir_in("/tmp")
            .unwrap();
        let data = dir.path().join("data");
        let pwfile = dir.path().join("pw");
        std::fs::write(&pwfile, password).unwrap();
        run(Command::new(bin.join("initdb"))
            .arg("-D")
            .arg(&data)
            .arg(format!("--pwfile={}", pwfile.display()))
            .args([
                "-U",
                "aspen",
                "--auth-local=trust",
                "--auth-host=scram-sha-256",
                "-E",
                "UTF8",
                "--no-locale",
                "--no-sync",
            ]));
        std::fs::write(
            data.join("pg_hba.conf"),
            "local all all trust\nhostssl all all 127.0.0.1/32 scram-sha-256\n",
        )
        .unwrap();
        let (crt, key) = (dir.path().join("server.crt"), dir.path().join("server.key"));
        run(Command::new("openssl")
            .args([
                "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
            ])
            .args(["-subj", "/CN=localhost", "-keyout"])
            .arg(&key)
            .arg("-out")
            .arg(&crt));
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(&key, std::fs::Permissions::from_mode(0o600)).unwrap();
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let opts = format!(
            "-k {} -c listen_addresses=127.0.0.1 -p {port} -c fsync=off -c ssl=on \
             -c ssl_cert_file={} -c ssl_key_file={}",
            dir.path().display(),
            crt.display(),
            key.display()
        );
        run(Command::new(bin.join("pg_ctl"))
            .arg("-D")
            .arg(&data)
            .arg("-l")
            .arg(dir.path().join("log"))
            .args(["-w", "-o", &opts, "start"]));
        (
            PgCluster {
                bin: bin.to_owned(),
                dir,
            },
            port,
        )
    }

    pub fn url(&self) -> String {
        self.url_for("postgres")
    }

    /// The URL of database `db` on this cluster.
    pub fn url_for(&self, db: &str) -> String {
        format!(
            "postgres://aspen@localhost:5432/{db}?host={}",
            self.dir.path().display()
        )
    }
}

impl Drop for PgCluster {
    fn drop(&mut self) {
        let _ = Command::new(self.bin.join("pg_ctl"))
            .arg("-D")
            .arg(self.dir.path().join("data"))
            .args(["-m", "immediate", "-w", "stop"])
            .output();
    }
}

pub fn run(cmd: &mut Command) {
    let out = cmd.output().unwrap_or_else(|e| panic!("{cmd:?}: {e}"));
    assert!(
        out.status.success(),
        "{cmd:?} failed:\n{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}
