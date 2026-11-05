//! The deploy configuration contract (`deploy/README.md`): Aspen connects
//! with `DATABASE_URL` carrying no password, takes the password from
//! `PGPASSWORD`, and on AWS uses `sslmode=require` against an RDS instance
//! with `rds.force_ssl = 1`.
//!
//! The server here accepts TCP clients only over TLS, with a SCRAM
//! password. The store runs in a child process of this test binary, so
//! `PGPASSWORD` is set for it without touching this process's environment.

mod common;

use std::process::Command;

use aspen_core::meta::MetaStore;
use aspen_core::name::{RefName, RepoName};
use aspen_core::repo::RepoSettings;
use aspen_meta::SqlStore;
use sqlx::Row as _;

const TEST: &str = "postgres_over_tls_with_the_password_from_pgpassword";
/// A password for a throwaway local cluster, not a secret.
const PASSWORD: &str = "aspen-test-only";

#[tokio::test]
async fn postgres_over_tls_with_the_password_from_pgpassword() {
    if let Some(url) = std::env::var_os("ASPEN_TEST_TLS_URL") {
        return child(url.to_str().unwrap()).await;
    }
    let Some(bin) = common::pg_or_skip() else {
        return;
    };
    let (_cluster, port) = common::PgCluster::start_tls(&bin, PASSWORD);
    let url = format!("postgres://aspen@127.0.0.1:{port}/postgres?sslmode=require");

    // The server really refuses plaintext, so a success below used TLS.
    let plain = SqlStore::connect(&format!(
        "postgres://aspen:{PASSWORD}@127.0.0.1:{port}/postgres?sslmode=disable"
    ))
    .await;
    assert!(plain.is_err(), "plaintext must be refused");

    let out = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", TEST, "--nocapture"])
        .env("ASPEN_TEST_TLS_URL", &url)
        .env("PGPASSWORD", PASSWORD)
        .output()
        .unwrap();
    let log = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(out.status.success(), "child failed:\n{log}");
    assert!(log.contains("1 passed"), "the child ran the test:\n{log}");
}

/// Runs in the child process, with `PGPASSWORD` set.
async fn child(url: &str) {
    assert!(!url.contains(PASSWORD), "the URL carries no password");
    let store = SqlStore::connect(url).await.expect("connect over TLS");
    store.migrate().await.unwrap();
    let ssl: bool = sqlx::query("SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()")
        .fetch_one(store.pool())
        .await
        .unwrap()
        .get("ssl");
    assert!(ssl, "the session is encrypted");
    let name = RepoName::new("acme/tls").unwrap();
    store
        .create_repo(
            &name,
            &RepoSettings::new(RefName::new("refs/heads/main").unwrap()).unwrap(),
        )
        .await
        .unwrap();
    assert!(store.repo(&name).await.unwrap().is_some());
}
