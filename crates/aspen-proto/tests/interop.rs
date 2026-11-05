//! Stock `git` against a server built from `aspen-proto`.
//!
//! Git's `ext::` transport runs a command and speaks the Git protocol over
//! its stdin and stdout, the way `git://` and SSH do. This test binary is
//! that command: when `ASPEN_PROTO_SERVE` is set it acts as the server,
//! speaking only through `aspen-proto` (advertisement, request parsing,
//! report and fetch response). Storage is delegated to a stock bare
//! repository (`git index-pack` for received packs, `git pack-objects` for
//! fetches), so what is under test is the protocol, not pack handling. The
//! client side is a stock `git` with an isolated configuration.
//!
//! `ext::` does not forward `GIT_PROTOCOL`, so the server picks the
//! version: v0 or v1 for push (by scenario), v2 for fetch, or v0 for fetch
//! with the `upload-v0` flag. Stock clients accept the version the server
//! answers in.
//!
//! v0 upload-pack, for clients without v2 fetch, is in
//! `interop/upload_v0.rs`: stateful over `ext::`, stateless behind a small
//! smart-HTTP server in this process, against stock git, pygit2 (libgit2)
//! and go-git, plus a replay of their recorded transcripts
//! (`tests/transcripts/`).
//!
//! Each test names the invariant it pins. Run with
//! `cargo test -p aspen-proto --test interop [name-filter] [--skip filter]`.
//! Unlike libtest, a command line that selects no test fails, so a typo in
//! a filter cannot pass as "ok"; run unit tests by name with `--lib`.

use std::collections::HashSet;
use std::fs;
use std::io::{self, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode, Stdio};

use aspen_core::error::{FetchError, RejectReason, ToWire, UnpackError};
use aspen_core::fetch::{PlanMode, ShallowInfo};
use aspen_core::id::{ObjectId, PackId};
use aspen_core::name::RefName;
use aspen_core::refs::PushOptions;
use aspen_core::repo::ShallowPolicy;
use aspen_proto::caps::Agent;
use aspen_proto::pkt::PktReader;
use aspen_proto::receive::{
    self, AdvertisedRef, ReceiveAdvertisement, ReceiveLimits, RefStatus, ReportStatus,
    RequestParser,
};
use aspen_proto::service::ReceiveVersion;
use aspen_proto::v2::{
    Acknowledgments, Command as V2Command, CommandParser, FetchResponse, LsRefsEntry, LsRefsName,
    V2Advertisement, V2Limits,
};
use aspen_testkit::isolated_git;

/// v0 upload-pack against stock git, pygit2 and go-git (`interop/upload_v0.rs`).
#[path = "interop/upload_v0.rs"]
mod upload_v0;

// ---------------------------------------------------------------- server

/// Server settings, passed from the test to the server through the
/// environment the `git` client hands to its `ext::` command.
struct Scenario {
    repo: PathBuf,
    home: PathBuf,
    flags: HashSet<String>,
}

impl Scenario {
    fn from_env() -> Self {
        let var = |k| {
            std::env::var_os(k)
                .map(PathBuf::from)
                .unwrap_or_else(|| panic!("{k:?} unset"))
        };
        let flags = std::env::var("ASPEN_PROTO_FLAGS").unwrap_or_default();
        Self {
            repo: var("ASPEN_PROTO_REPO"),
            home: var("ASPEN_PROTO_HOME"),
            flags: flags
                .split(',')
                .filter(|s| !s.is_empty())
                .map(str::to_owned)
                .collect(),
        }
    }
    fn has(&self, flag: &str) -> bool {
        self.flags.contains(flag)
    }
    fn git(&self) -> Command {
        let mut c = isolated_git(&self.home);
        c.current_dir(&self.repo);
        c
    }
    fn git_out(&self, args: &[&str]) -> Option<String> {
        let out = self
            .git()
            .args(args)
            .stderr(Stdio::null())
            .output()
            .unwrap();
        out.status
            .success()
            .then(|| String::from_utf8(out.stdout).unwrap())
    }
}

fn oid(s: &str) -> ObjectId {
    ObjectId::parse_hex(s.trim()).unwrap()
}

fn serve() {
    let sc = Scenario::from_env();
    let service = std::env::var("GIT_EXT_SERVICE").expect("run by git's ext:: transport");
    let mut out = BufWriter::new(io::stdout().lock());
    match service.as_str() {
        "git-receive-pack" => serve_receive(&sc, &mut out),
        "git-upload-pack" => serve_upload(&sc, &mut out),
        other => panic!("unexpected service {other}"),
    }
    out.flush().unwrap();
}

fn send(out: &mut impl Write, bytes: &[u8]) {
    out.write_all(bytes).unwrap();
    out.flush().unwrap();
}

fn serve_receive(sc: &Scenario, out: &mut impl Write) {
    let mut wire = Vec::new();
    if sc.has("err-advert") {
        receive::write_fatal(&mut wire, None, "repository not found (aspen-proto test)");
        return send(out, &wire);
    }
    let advert = ReceiveAdvertisement::new(Agent::aspen());
    let main = sc
        .git_out(&["rev-parse", "--verify", "-q", "refs/heads/main"])
        .map(|s| oid(&s));
    let (refs, haves) = match main {
        // Like Aspen's plain URL with the trunk tip shown only as a `.have`.
        Some(tip) if sc.has("haves-only") => (vec![], vec![tip]),
        Some(tip) => (
            vec![AdvertisedRef {
                name: RefName::new("refs/heads/main").unwrap(),
                tip,
            }],
            vec![],
        ),
        None => (vec![], vec![]),
    };
    let version = if sc.has("v1") {
        ReceiveVersion::V1
    } else {
        ReceiveVersion::V0
    };
    receive::write_advertisement(&mut wire, version, &advert, &refs, &haves).unwrap();
    send(out, &wire);

    let mut reader = PktReader::new(io::stdin());
    let mut parser = RequestParser::new(advert, ReceiveLimits::default());
    let req = loop {
        let Some(p) = reader.read_packet().unwrap() else {
            return;
        };
        if let Some(req) = parser.packet(p).unwrap() {
            break req;
        }
    };
    fs::write(
        sc.repo.join("aspen-push-options"),
        req.push_options.join("\n"),
    )
    .unwrap();
    let opts = PushOptions::parse(req.push_options.iter().map(String::as_str)).unwrap();
    if req.push_options.iter().any(|o| o == "fatal") {
        let mut wire = Vec::new();
        receive::write_fatal(
            &mut wire,
            Some(&req.caps),
            "push refused by test: fatal requested",
        );
        return send(out, &wire);
    }

    let mut report = ReportStatus::new(&req);
    if req.expects_pack() {
        let mut rest = reader.into_remainder();
        let mut header = [0u8; 12];
        rest.read_exact(&mut header).unwrap();
        assert_eq!(&header[..4], b"PACK");
        let count = u32::from_be_bytes(header[8..12].try_into().unwrap());
        fs::write(sc.repo.join("aspen-pack-count"), count.to_string()).unwrap();
        let mut child = sc
            .git()
            .args(["index-pack", "--stdin", "--fix-thin"])
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .spawn()
            .unwrap();
        let mut stdin = child.stdin.take().unwrap();
        // The client keeps the connection open for the report, so copying
        // never sees end of stream: the copy stops when index-pack exits and
        // closes the pipe, and the thread ends with the process.
        std::thread::spawn(move || {
            let _ = stdin
                .write_all(&header)
                .and_then(|()| io::copy(&mut rest, &mut stdin).map(drop));
        });
        let ok = child.wait().unwrap().success();
        if !ok || req.push_options.iter().any(|o| o == "fail-unpack") {
            let e = UnpackError::MissingBase {
                id: ObjectId::from_bytes([0xab; 20]),
            }
            .to_wire();
            report.unpack_failed(&e).unwrap();
            let mut wire = Vec::new();
            report.write(&mut wire, &req.caps).unwrap();
            return send(out, &wire);
        }
    }
    for c in &req.commands {
        let n = c.name.as_str();
        let refused = n.starts_with("refs/heads/reject-")
            || (n.starts_with("refs/heads/needs-force-") && !opts.force_requested(&c.name));
        let status = if refused {
            RejectReason::NonFastForward.into()
        } else {
            let ok = if c.new.is_zero() {
                sc.git()
                    .args(["update-ref", "-d", n])
                    .status()
                    .unwrap()
                    .success()
            } else {
                sc.git()
                    .args(["update-ref", n, &c.new.to_hex()])
                    .status()
                    .unwrap()
                    .success()
            };
            if ok {
                RefStatus::Ok
            } else {
                RejectReason::CasLost.into()
            }
        };
        report.set(&c.name, status).unwrap();
    }
    let mut wire = Vec::new();
    report.write(&mut wire, &req.caps).unwrap();
    send(out, &wire);
}

fn serve_upload(sc: &Scenario, out: &mut impl Write) {
    if sc.has("upload-v0") {
        return upload_v0::serve_stateful(sc, out);
    }
    let advert = V2Advertisement::new(Agent::aspen());
    let mut wire = Vec::new();
    advert.write(&mut wire);
    send(out, &wire);
    let mut reader = PktReader::new(io::stdin());
    loop {
        let mut parser = CommandParser::new(advert.clone(), V2Limits::default());
        let command = loop {
            let Some(p) = reader.read_packet().unwrap() else {
                return;
            };
            if let Some(c) = parser.packet(p).unwrap() {
                break c;
            }
        };
        let mut wire = Vec::new();
        match command {
            V2Command::End => return,
            V2Command::LsRefs(req) => {
                fs::write(sc.repo.join("aspen-ref-prefixes"), req.prefixes.join("\n")).unwrap();
                let entries = ls_refs_entries(sc);
                aspen_proto::v2::write_ls_refs(&mut wire, &req, &entries).unwrap();
            }
            V2Command::Fetch(args) => fetch(sc, &args, &mut wire),
        }
        send(out, &wire);
    }
}

fn ls_refs_entries(sc: &Scenario) -> Vec<LsRefsEntry> {
    let mut entries = Vec::new();
    let target = sc
        .git_out(&["symbolic-ref", "HEAD"])
        .map(|t| RefName::new(t.trim()).unwrap());
    let tip = sc
        .git_out(&["rev-parse", "--verify", "-q", "HEAD"])
        .map(|s| oid(&s));
    entries.push(LsRefsEntry {
        name: LsRefsName::Head,
        tip,
        symref_target: target,
        peeled: None,
    });
    let list = sc
        .git_out(&[
            "for-each-ref",
            "--format=%(objectname) %(refname) %(*objectname)",
        ])
        .unwrap();
    for line in list.lines() {
        let mut f = line.split(' ');
        let (tip, name, peeled) = (f.next().unwrap(), f.next().unwrap(), f.next().unwrap_or(""));
        entries.push(LsRefsEntry {
            name: LsRefsName::Ref(RefName::new(name).unwrap()),
            tip: Some(oid(tip)),
            symref_target: None,
            peeled: (!peeled.is_empty()).then(|| oid(peeled)),
        });
    }
    entries
}

fn fetch(sc: &Scenario, args: &aspen_proto::v2::FetchArgs, wire: &mut Vec<u8>) {
    let mut resp = FetchResponse::new(args);
    let policy = if sc.has("shallow-refuse") {
        ShallowPolicy::Refuse
    } else {
        ShallowPolicy::Serve
    };
    let mode = match args.request.plan_mode(policy) {
        Ok(m) => m,
        Err(e) => return resp.error(wire, &e.to_wire()).unwrap(),
    };
    let common: Vec<ObjectId> = args
        .request
        .haves
        .iter()
        .copied()
        .filter(|h| sc.git_out(&["cat-file", "-e", &h.to_hex()]).is_some())
        .collect();
    if !args.request.done {
        resp.acknowledgments(
            wire,
            &Acknowledgments {
                common: common.clone(),
                ready: true,
            },
        )
        .unwrap();
    }
    if args.request.is_shallow() {
        // Depth 1 only: the boundary is every want that has parents. The
        // pack is the full closure of the wants (S5's over-fetch).
        let mut info = ShallowInfo::default();
        for w in &args.request.wants {
            let parents = sc
                .git_out(&["rev-list", "--parents", "-n1", &w.to_hex()])
                .unwrap();
            if parents.split_whitespace().count() > 1 {
                info.shallow.push(*w);
            }
        }
        resp.shallow_info(wire, &info).unwrap();
    }
    let mut revs: Vec<String> = args.request.wants.iter().map(ObjectId::to_hex).collect();
    if mode == PlanMode::Incremental {
        revs.extend(common.iter().map(|h| format!("^{h}")));
    }
    let mut cmd = sc.git();
    cmd.args(["pack-objects", "--revs", "--stdout", "-q"]);
    if args.request.ofs_delta {
        cmd.arg("--delta-base-offset");
    }
    if args.include_tag {
        cmd.arg("--include-tag");
    }
    let mut child = cmd
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all((revs.join("\n") + "\n").as_bytes())
        .unwrap();
    let pack = child.wait_with_output().unwrap();
    assert!(pack.status.success());
    resp.start_packfile(wire).unwrap();
    resp.progress(wire, "aspen-proto test server: sending pack")
        .unwrap();
    if sc.has("fatal-mid-pack") {
        resp.pack_data(wire, &pack.stdout[..12]).unwrap();
        let e = FetchError::StoredPackNotBaseFirst {
            pack: PackId::new(7).unwrap(),
            offset: 12,
        }
        .to_wire();
        return resp.error(wire, &e).unwrap();
    }
    resp.pack_data(wire, &pack.stdout).unwrap();
    resp.finish(wire).unwrap();
}

// ---------------------------------------------------------------- client

struct World {
    _tmp: tempfile::TempDir,
    root: PathBuf,
    home: PathBuf,
    repo: PathBuf,
    flags: String,
}

impl World {
    /// A bare server repository, empty or with `commits` commits on main.
    fn new(commits: usize, flags: &str) -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().to_owned();
        let home = root.join("home");
        fs::create_dir_all(&home).unwrap();
        let repo = root.join("server.git");
        let w = Self {
            _tmp: tmp,
            root: root.clone(),
            home,
            repo: repo.clone(),
            flags: flags.to_owned(),
        };
        w.run(
            &root,
            &[
                "init",
                "-q",
                "--bare",
                "--initial-branch=main",
                repo.to_str().unwrap(),
            ],
        );
        if commits > 0 {
            let seed = root.join("seed");
            w.run(
                &root,
                &[
                    "init",
                    "-q",
                    "--initial-branch=main",
                    seed.to_str().unwrap(),
                ],
            );
            for i in 0..commits {
                w.commit(&seed, &format!("seed{i}.txt"));
            }
            w.run(&seed, &["push", "-q", repo.to_str().unwrap(), "main"]);
        }
        w
    }

    fn url(&self) -> String {
        format!("ext::{}", std::env::current_exe().unwrap().display())
    }

    fn git(&self, dir: &Path) -> Command {
        let mut c = isolated_git(&self.home);
        c.current_dir(dir)
            .args(["-c", "protocol.ext.allow=always"])
            .env("ASPEN_PROTO_SERVE", "1")
            .env("ASPEN_PROTO_REPO", &self.repo)
            .env("ASPEN_PROTO_HOME", &self.home)
            .env("ASPEN_PROTO_FLAGS", &self.flags);
        c
    }

    /// Runs git; returns (success, stdout, stderr).
    fn try_run(&self, dir: &Path, args: &[&str]) -> (bool, String, String) {
        let out = self.git(dir).args(args).output().unwrap();
        let s = |b: Vec<u8>| String::from_utf8_lossy(&b).into_owned();
        (out.status.success(), s(out.stdout), s(out.stderr))
    }

    fn run(&self, dir: &Path, args: &[&str]) -> String {
        let (ok, stdout, stderr) = self.try_run(dir, args);
        assert!(
            ok,
            "git {args:?} failed\nstdout:\n{stdout}\nstderr:\n{stderr}"
        );
        stdout
    }

    fn commit(&self, work: &Path, file: &str) -> String {
        fs::write(work.join(file), format!("{file}\n")).unwrap();
        self.run(work, &["add", file]);
        self.run(work, &["commit", "-q", "-m", file]);
        self.run(work, &["rev-parse", "HEAD"]).trim().to_owned()
    }

    /// A working clone of the server repository, made over the file system
    /// so it does not depend on the code under test.
    fn work(&self, name: &str) -> PathBuf {
        let w = self.root.join(name);
        self.run(
            &self.root,
            &[
                "clone",
                "-q",
                self.repo.to_str().unwrap(),
                w.to_str().unwrap(),
            ],
        );
        self.run(&w, &["remote", "set-url", "origin", &self.url()]);
        w
    }

    fn server_ref(&self, name: &str) -> Option<String> {
        let (ok, out, _) = self.try_run(&self.repo, &["rev-parse", "--verify", "-q", name]);
        ok.then(|| out.trim().to_owned())
    }

    fn server_file(&self, name: &str) -> String {
        fs::read_to_string(self.repo.join(name)).unwrap_or_default()
    }

    fn fsck(&self, dir: &Path) {
        self.run(dir, &["fsck", "--strict", "--no-progress"]);
    }
}

/// Invariant: a v0 push of a create, an update and a delete-only push all
/// land, push options reach the server in order, and a branch that needs
/// `-o force` is accepted with it.
fn push_v0_create_update_delete_and_options() {
    let w = World::new(2, "");
    let work = w.work("work");
    let tip = w.commit(&work, "a.txt");
    w.run(&work, &["push", "-q", "origin", "main"]);
    assert_eq!(
        w.server_ref("refs/heads/main").as_deref(),
        Some(tip.as_str())
    );
    w.run(
        &work,
        &[
            "push",
            "-q",
            "-o",
            "one",
            "-o",
            "force",
            "origin",
            "main:refs/heads/needs-force-x",
            "main:refs/heads/topic",
        ],
    );
    assert_eq!(w.server_file("aspen-push-options"), "one\nforce");
    assert_eq!(
        w.server_ref("refs/heads/topic").as_deref(),
        Some(tip.as_str())
    );
    assert_eq!(
        w.server_ref("refs/heads/needs-force-x").as_deref(),
        Some(tip.as_str())
    );
    // Delete only: no pack follows, and the server must not wait for one.
    fs::remove_file(w.repo.join("aspen-pack-count")).unwrap();
    w.run(&work, &["push", "-q", "origin", ":refs/heads/topic"]);
    assert_eq!(w.server_ref("refs/heads/topic"), None);
    assert_eq!(w.server_file("aspen-pack-count"), "", "no pack was read");
    w.fsck(&w.repo);
}

/// Invariant: a `version 1` advertisement is accepted by a v1 client.
fn push_v1() {
    let w = World::new(1, "v1");
    let work = w.work("work");
    let tip = w.commit(&work, "a.txt");
    let trace = w.root.join("trace");
    let out = w
        .git(&work)
        .env("GIT_TRACE_PACKET", &trace)
        .args(["-c", "protocol.version=1", "push", "-q", "origin", "main"])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(fs::read_to_string(&trace).unwrap().contains("< version 1"));
    assert_eq!(
        w.server_ref("refs/heads/main").as_deref(),
        Some(tip.as_str())
    );
}

/// Invariant: a rejection reaches the user twice, in the `ng` line and as a
/// `remote:` notice, even though the client asked for `quiet` (S2), and the
/// other refs of the push still land.
fn push_rejection_shows_reason_in_ng_and_on_band_2() {
    let w = World::new(1, "");
    let work = w.work("work");
    let tip = w.commit(&work, "a.txt");
    let (ok, _, err) = w.try_run(
        &work,
        &[
            "push",
            "origin",
            "main:refs/heads/reject-x",
            "main:refs/heads/fine",
            "main:refs/heads/needs-force-y",
        ],
    );
    assert!(!ok);
    let reason = RejectReason::NonFastForward.message();
    assert!(
        err.contains(&format!("remote: aspen: refs/heads/reject-x: {reason}")),
        "{err}"
    );
    assert!(
        err.contains(&format!("! [remote rejected] main -> reject-x ({reason})")),
        "{err}"
    );
    assert!(err.contains("main -> needs-force-y ("), "{err}");
    assert_eq!(
        w.server_ref("refs/heads/fine").as_deref(),
        Some(tip.as_str())
    );
    assert_eq!(w.server_ref("refs/heads/reject-x"), None);
}

/// Invariant: a refused pack rejects every ref with "unpacker error" and
/// the client shows the unpack reason.
fn push_unpack_failure_rejects_every_ref() {
    let w = World::new(1, "");
    let work = w.work("work");
    w.commit(&work, "a.txt");
    let (ok, _, err) = w.try_run(
        &work,
        &[
            "push",
            "-o",
            "fail-unpack",
            "origin",
            "main",
            "main:refs/heads/b",
        ],
    );
    assert!(!ok);
    assert!(
        err.contains("remote: aspen: unpack failed: missing delta base"),
        "{err}"
    );
    assert!(err.contains("main -> main (unpacker error)"), "{err}");
    assert!(err.contains("main -> b (unpacker error)"), "{err}");
    assert_eq!(w.server_ref("refs/heads/b"), None);
}

/// Invariant: a push whose only non-delete command has a name Git allows
/// but Aspen refuses (`refs/x`) still has its pack read, so the client
/// gets that command's `ng` line (and the band-2 notice) rather than a
/// broken connection, and the valid delete beside it lands.
fn push_invalid_name_still_reads_the_pack() {
    let w = World::new(1, "");
    w.run(
        &w.repo,
        &["update-ref", "refs/heads/old", "refs/heads/main"],
    );
    let work = w.work("work");
    w.commit(&work, "a.txt");
    let (ok, _, err) = w.try_run(&work, &["push", "origin", "main:refs/x", ":refs/heads/old"]);
    assert!(!ok, "{err}");
    assert!(
        err.contains("! [remote rejected] main -> refs/x (invalid ref name: "),
        "{err}"
    );
    assert!(
        err.contains("remote: aspen: refs/x: invalid ref name: "),
        "{err}"
    );
    assert!(err.contains("[deleted]") && err.contains("old"), "{err}");
    assert_eq!(
        w.server_ref("refs/heads/old"),
        None,
        "the valid delete landed"
    );
    assert_eq!(w.server_ref("refs/x"), None);
    assert_eq!(w.server_file("aspen-pack-count"), "3", "the pack was read");
}

/// Invariant: fatal errors are shown both before the exchange (an `ERR`
/// line in place of the advertisement, printed as `remote error: ...`) and
/// after it (band 3, printed as `remote: ...` before the client gives up).
fn push_fatal_errors_are_shown() {
    let w = World::new(1, "err-advert");
    let work = w.work("work");
    let (ok, _, err) = w.try_run(&work, &["push", "origin", "main:refs/heads/x"]);
    assert!(!ok);
    assert!(
        err.contains("remote error: repository not found (aspen-proto test)"),
        "{err}"
    );

    let w = World::new(1, "");
    let work = w.work("work");
    w.commit(&work, "a.txt");
    let (ok, _, err) = w.try_run(&work, &["push", "-o", "fatal", "origin", "main"]);
    assert!(!ok);
    assert!(
        err.contains("remote: push refused by test: fatal requested"),
        "{err}"
    );
}

/// Invariants: an empty repository's `capabilities^{}` advertisement takes
/// a first push; and a `.have`-only advertisement (no named refs) makes the
/// client send only what the server lacks (S2's 19 vs 20).
fn push_to_empty_repo_and_against_have_lines() {
    let w = World::new(0, "");
    let seed = w.root.join("fresh");
    w.run(
        &w.root,
        &[
            "init",
            "-q",
            "--initial-branch=main",
            seed.to_str().unwrap(),
        ],
    );
    let tip = w.commit(&seed, "first.txt");
    w.run(&seed, &["push", "-q", &w.url(), "main"]);
    assert_eq!(
        w.server_ref("refs/heads/main").as_deref(),
        Some(tip.as_str())
    );

    let w = World::new(3, "haves-only");
    let work = w.work("work");
    let tip = w.commit(&work, "new.txt");
    w.run(&work, &["push", "-q", "origin", "main:refs/heads/feature"]);
    assert_eq!(
        w.server_ref("refs/heads/feature").as_deref(),
        Some(tip.as_str())
    );
    assert_eq!(
        w.server_file("aspen-pack-count"),
        "3",
        "commit, tree and blob only"
    );
    w.fsck(&w.repo);
}

/// Invariant: a v2 clone and an incremental fetch (with haves, so the
/// acknowledgments section is used) give a repository that passes
/// `fsck --strict`, with HEAD on the server's default branch and tags
/// peeled.
fn clone_and_fetch_v2() {
    let w = World::new(2, "");
    let seed = w.work("writer");
    w.run(&seed, &["tag", "-a", "-m", "v1", "v1"]);
    w.run(&seed, &["push", "-q", w.repo.to_str().unwrap(), "v1"]);
    let clone = w.root.join("clone");
    let trace = w.root.join("trace");
    let out = w
        .git(&w.root)
        .env("GIT_TRACE_PACKET", &trace)
        .args([
            "-c",
            "protocol.version=2",
            "clone",
            "-q",
            &w.url(),
            clone.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(fs::read_to_string(&trace).unwrap().contains("< version 2"));
    w.fsck(&clone);
    assert_eq!(
        w.run(&clone, &["rev-parse", "HEAD"]).trim(),
        w.server_ref("refs/heads/main").unwrap()
    );
    assert_eq!(
        w.run(&clone, &["symbolic-ref", "HEAD"]).trim(),
        "refs/heads/main"
    );
    assert!(w.run(&clone, &["tag"]).contains("v1"));

    let tip = w.commit(&seed, "later.txt");
    w.run(&seed, &["push", "-q", w.repo.to_str().unwrap(), "main"]);
    let trace2 = w.root.join("trace2");
    let out = w
        .git(&clone)
        .env("GIT_TRACE_PACKET", &trace2)
        .args(["-c", "protocol.version=2", "fetch", "-q", "origin"])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let t = fs::read_to_string(&trace2).unwrap();
    assert!(
        t.contains("< acknowledgments") && t.contains("< ready"),
        "{t}"
    );
    assert_eq!(w.run(&clone, &["rev-parse", "origin/main"]).trim(), tip);
    w.fsck(&clone);

    let (ok, out, _) = w.try_run(
        &w.root,
        &["-c", "protocol.version=2", "ls-remote", "--tags", &w.url()],
    );
    assert!(ok);
    assert_eq!(
        w.server_file("aspen-ref-prefixes"),
        "refs/tags/",
        "ref-prefix reaches the server"
    );
    assert!(
        out.contains("refs/tags/v1\n") && out.contains("refs/tags/v1^{}"),
        "{out}"
    );
    assert!(!out.contains("refs/heads/"), "{out}");
}

/// Invariant: cloning an empty repository uses `unborn` to learn the
/// server's default branch.
fn clone_empty_repo_learns_default_branch() {
    let w = World::new(0, "");
    w.run(&w.repo, &["symbolic-ref", "HEAD", "refs/heads/trunk"]);
    let clone = w.root.join("clone");
    let (ok, _, err) = w.try_run(
        &w.root,
        &[
            "-c",
            "protocol.version=2",
            "-c",
            "init.defaultBranch=other",
            "clone",
            "-q",
            &w.url(),
            clone.to_str().unwrap(),
        ],
    );
    assert!(ok, "{err}");
    assert_eq!(
        w.run(&clone, &["symbolic-ref", "HEAD"]).trim(),
        "refs/heads/trunk"
    );
}

/// Invariant: a depth-1 clone gets shallow-info before the (over-fetched)
/// pack, and the client ends up shallow with one commit visible (S5).
fn shallow_clone_v2() {
    let w = World::new(3, "");
    let clone = w.root.join("clone");
    w.run(
        &w.root,
        &[
            "-c",
            "protocol.version=2",
            "clone",
            "-q",
            "--depth",
            "1",
            "--no-local",
            &w.url(),
            clone.to_str().unwrap(),
        ],
    );
    assert_eq!(
        w.run(&clone, &["rev-parse", "--is-shallow-repository"])
            .trim(),
        "true"
    );
    assert_eq!(w.run(&clone, &["rev-list", "--count", "HEAD"]).trim(), "1");
    w.fsck(&clone);
}

/// Invariant: errors are shown verbatim whether they come before the pack
/// (`ERR`, printed as `remote error: ...`) or inside it (band 3, printed
/// as `remote: ...`, and the fetch fails).
fn fetch_errors_are_shown() {
    let w = World::new(2, "shallow-refuse");
    let clone = w.root.join("clone");
    let (ok, _, err) = w.try_run(
        &w.root,
        &[
            "-c",
            "protocol.version=2",
            "clone",
            "-q",
            "--depth",
            "1",
            &w.url(),
            clone.to_str().unwrap(),
        ],
    );
    assert!(!ok);
    assert!(
        err.contains(&format!(
            "remote error: {}",
            FetchError::shallow_refused().to_wire().message
        )),
        "{err}"
    );

    let w = World::new(2, "fatal-mid-pack");
    let clone = w.root.join("clone");
    let (ok, _, err) = w.try_run(
        &w.root,
        &[
            "-c",
            "protocol.version=2",
            "clone",
            "-q",
            &w.url(),
            clone.to_str().unwrap(),
        ],
    );
    assert!(!ok);
    assert!(
        err.contains("remote: stored pack 7 has a forward delta base at offset 12; retry"),
        "{err}"
    );
}

/// Invariant: the harness reads the libtest command line cargo passes it:
/// `--skip` and its value are not taken as name filters, flags with values
/// skip them, `--exact` compares whole names, and `--ignored` selects only
/// ignored tests (none here) while `--include-ignored` selects them all, as
/// in libtest.
fn harness_reads_libtest_arguments() {
    let parse = |a: &[&str]| Args::parse(a.iter().map(|s| (*s).to_owned())).unwrap();
    let a = parse(&[
        "--skip",
        "push",
        "--test-threads",
        "4",
        "--nocapture",
        "clone",
    ]);
    assert_eq!(a.filters, ["clone"]);
    assert_eq!(a.skip, ["push"]);
    assert!(a.selects("clone_and_fetch_v2") && !a.selects("push_v1"));
    // `--report-time` is a plain flag, `--shuffle-seed` takes a value.
    let a = parse(&["--report-time", "clone", "--shuffle-seed", "7"]);
    assert_eq!(a.filters, ["clone"]);
    let a = parse(&["--skip=push", "-q"]);
    assert!(a.filters.is_empty() && a.selects("clone_and_fetch_v2") && !a.selects("push_v1"));
    let a = parse(&["--exact", "push_v1"]);
    assert!(a.selects("push_v1") && !a.selects("push_v1_more"));
    assert!(Args::parse(["--skip".to_owned()]).is_err());
    let a = parse(&["--ignored"]);
    assert!(TESTS.iter().all(|(n, _)| !a.selects(n)));
    let a = parse(&["--include-ignored", "--ignored"]);
    assert!(TESTS.iter().all(|(n, _)| a.selects(n)));
    let a = parse(&["--include-ignored", "push_v1"]);
    assert!(a.selects("push_v1") && !a.selects("clone_and_fetch_v2"));
    // A filter meant for a unit test selects nothing here; main reports
    // that and succeeds, as libtest does for each test binary.
    let a = parse(&["parser_is_total_on_arbitrary_packets"]);
    assert!(TESTS.iter().all(|(n, _)| !a.selects(n)));
}

const TESTS: &[(&str, fn())] = &[
    (
        "harness_reads_libtest_arguments",
        harness_reads_libtest_arguments,
    ),
    (
        "push_v0_create_update_delete_and_options",
        push_v0_create_update_delete_and_options,
    ),
    ("push_v1", push_v1),
    (
        "push_rejection_shows_reason_in_ng_and_on_band_2",
        push_rejection_shows_reason_in_ng_and_on_band_2,
    ),
    (
        "push_unpack_failure_rejects_every_ref",
        push_unpack_failure_rejects_every_ref,
    ),
    (
        "push_invalid_name_still_reads_the_pack",
        push_invalid_name_still_reads_the_pack,
    ),
    ("push_fatal_errors_are_shown", push_fatal_errors_are_shown),
    (
        "push_to_empty_repo_and_against_have_lines",
        push_to_empty_repo_and_against_have_lines,
    ),
    ("clone_and_fetch_v2", clone_and_fetch_v2),
    (
        "clone_empty_repo_learns_default_branch",
        clone_empty_repo_learns_default_branch,
    ),
    ("shallow_clone_v2", shallow_clone_v2),
    ("fetch_errors_are_shown", fetch_errors_are_shown),
    (
        "v0_http_git_negotiation_ready_no_done",
        upload_v0::v0_http_git_negotiation_ready_no_done,
    ),
    (
        "v0_http_git_negotiation_until_done",
        upload_v0::v0_http_git_negotiation_until_done,
    ),
    (
        "v0_stateful_git_negotiation",
        upload_v0::v0_stateful_git_negotiation,
    ),
    (
        "v0_git_shallow_deepen_unshallow",
        upload_v0::v0_git_shallow_deepen_unshallow,
    ),
    (
        "v0_git_v1_and_empty_repository",
        upload_v0::v0_git_v1_and_empty_repository,
    ),
    (
        "v0_git_errors_are_shown",
        upload_v0::v0_git_errors_are_shown,
    ),
    ("v0_http_pygit2", upload_v0::v0_http_pygit2),
    ("v0_http_go_git", upload_v0::v0_http_go_git),
    ("v0_transcripts_replay", upload_v0::v0_transcripts_replay),
];

/// The libtest command line, as far as this harness needs it: name
/// filters, `--skip <filter>` (repeatable), `--exact`, `--ignored`,
/// `--include-ignored` and `--list`. Other
/// flags (`--nocapture`, `-q`, ...) are accepted and ignored, and those
/// that take a value (`--test-threads 4`, `--format x`, ...) skip it.
#[derive(Debug, Default, PartialEq, Eq)]
struct Args {
    filters: Vec<String>,
    skip: Vec<String>,
    exact: bool,
    list: bool,
    /// `--ignored`: run only ignored tests. This harness has none.
    only_ignored: bool,
    /// `--include-ignored`: run ignored tests too; overrides `--ignored`.
    include_ignored: bool,
}

impl Args {
    fn parse(argv: impl IntoIterator<Item = String>) -> Result<Self, String> {
        const WITH_VALUE: [&str; 6] = [
            "--test-threads",
            "--format",
            "--color",
            "--logfile",
            "--shuffle-seed",
            "-Z",
        ];
        let mut a = Self::default();
        let mut argv = argv.into_iter();
        while let Some(arg) = argv.next() {
            match arg.as_str() {
                "--skip" => a.skip.push(argv.next().ok_or("--skip needs a filter")?),
                "--exact" => a.exact = true,
                "--list" => a.list = true,
                "--ignored" => a.only_ignored = true,
                "--include-ignored" => a.include_ignored = true,
                s if s.starts_with("--skip=") => a.skip.push(s["--skip=".len()..].to_owned()),
                s if WITH_VALUE.contains(&s) => {
                    argv.next();
                }
                s if s.starts_with('-') => {}
                _ => a.filters.push(arg),
            }
        }
        Ok(a)
    }

    fn matches(&self, name: &str, filter: &str) -> bool {
        if self.exact {
            name == filter
        } else {
            name.contains(filter)
        }
    }

    fn selects(&self, name: &str) -> bool {
        // No interop test is ignored, so `--ignored` alone selects none.
        if self.only_ignored && !self.include_ignored {
            return false;
        }
        (self.filters.is_empty() || self.filters.iter().any(|f| self.matches(name, f)))
            && !self.skip.iter().any(|f| self.matches(name, f))
    }
}

fn main() -> ExitCode {
    if std::env::var_os("ASPEN_PROTO_SERVE").is_some() {
        serve();
        return ExitCode::SUCCESS;
    }
    let args = match Args::parse(std::env::args().skip(1)) {
        Ok(a) => a,
        Err(e) => {
            eprintln!("error: {e}");
            return ExitCode::FAILURE;
        }
    };
    if args.list {
        for (name, _) in TESTS {
            println!("{name}: test");
        }
        return ExitCode::SUCCESS;
    }
    let selected: Vec<_> = TESTS.iter().filter(|(n, _)| args.selects(n)).collect();
    if selected.is_empty() {
        // As libtest does: `cargo test <name>` passes the filter to every
        // test binary, so selecting nothing here is not a failure. Say so
        // plainly, so a typo in a filter is visible.
        println!(
            "0 of {} interop tests selected (filters {:?}, skipping {:?})",
            TESTS.len(),
            args.filters,
            args.skip
        );
        println!("test result: ok. 0 passed; 0 failed");
        return ExitCode::SUCCESS;
    }
    let version = Command::new("git")
        .arg("--version")
        .output()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_owned());
    println!(
        "running {} of {} interop tests against {}",
        selected.len(),
        TESTS.len(),
        version.unwrap_or_else(|e| format!("no git ({e})"))
    );
    let mut failed = Vec::new();
    for (name, test) in selected {
        match std::panic::catch_unwind(test) {
            Ok(()) => println!("test {name} ... ok"),
            Err(_) => {
                println!("test {name} ... FAILED");
                failed.push(*name);
            }
        }
    }
    if failed.is_empty() {
        println!("test result: ok");
        ExitCode::SUCCESS
    } else {
        println!("test result: FAILED: {failed:?}");
        ExitCode::FAILURE
    }
}
