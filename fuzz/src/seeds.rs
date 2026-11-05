//! The seed corpus: a few dozen inputs per target that reach the
//! interesting paths at once, so libFuzzer starts from real requests and
//! real packs instead of random bytes. `aspen-fuzz seeds <dir>` writes them;
//! `fuzz/corpus` holds the committed copy, and `tests/corpus.rs` checks
//! that every generated seed is committed. Generation is deterministic and
//! needs no `git` binary.

use std::path::Path;

use aspen_core::id::ObjectId;
use aspen_core::ingest::ObjectKind;

use crate::input::header;
use crate::packgen::{RawEntry, deflate, delta, object_id, type_code, write_pack};

/// One seed: target, file name, bytes.
pub type Seed = (&'static str, String, Vec<u8>);

/// The recorded v0 upload-pack transcripts of `aspen-proto`, relative to
/// this crate.
const TRANSCRIPTS: &str = "../crates/aspen-proto/tests/transcripts";

fn pkt(line: &str) -> Vec<u8> {
    let mut v = format!("{:04x}", line.len() + 4).into_bytes();
    v.extend_from_slice(line.as_bytes());
    v
}

fn pkt_bytes(data: &[u8]) -> Vec<u8> {
    let mut v = format!("{:04x}", data.len() + 4).into_bytes();
    v.extend_from_slice(data);
    v
}

const FLUSH: &[u8] = b"0000";
const DELIM: &[u8] = b"0001";

fn oid(n: u8) -> String {
    ObjectId::from_bytes([n; 20]).to_hex()
}

const ZERO: &str = "0000000000000000000000000000000000000000";

fn cat(parts: &[&[u8]]) -> Vec<u8> {
    parts.concat()
}

/// Every seed, in a fixed order.
pub fn all() -> Vec<Seed> {
    let mut out = Vec::new();
    proto_seeds(&mut out);
    pack_seeds(&mut out);
    structured_seeds(&mut out);
    front_seeds(&mut out);
    out
}

/// Writes every seed to `dir/<target>/<name>`.
pub fn write(dir: &Path) -> std::io::Result<usize> {
    let seeds = all();
    for (target, name, bytes) in &seeds {
        let d = dir.join(target);
        std::fs::create_dir_all(&d)?;
        std::fs::write(d.join(name), bytes)?;
    }
    Ok(seeds.len())
}

/// One recorded scenario: its directory name and its `.pkt` files in order.
type Scenario = (String, Vec<(String, Vec<u8>)>);

fn transcripts() -> Vec<Scenario> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join(TRANSCRIPTS);
    let mut scenarios = Vec::new();
    // Missing transcripts would silently drop every upload_v0 seed and
    // their pkt_line copies, so fail instead.
    let dirs = std::fs::read_dir(&root)
        .unwrap_or_else(|e| panic!("the transcripts at {}: {e}", root.display()));
    let mut dirs: Vec<_> = dirs.flatten().filter(|d| d.path().is_dir()).collect();
    dirs.sort_by_key(|d| d.file_name());
    for d in dirs {
        let mut files: Vec<_> = std::fs::read_dir(d.path())
            .expect("a transcript directory")
            .flatten()
            .map(|f| f.file_name().to_string_lossy().into_owned())
            .filter(|n| n.ends_with(".pkt"))
            .collect();
        files.sort();
        let files = files
            .into_iter()
            .map(|n| {
                let bytes = std::fs::read(d.path().join(&n)).expect("a transcript file");
                (n, bytes)
            })
            .collect();
        scenarios.push((d.file_name().to_string_lossy().into_owned(), files));
    }
    scenarios
}

fn proto_seeds(out: &mut Vec<Seed>) {
    let caps = "report-status side-band-64k quiet atomic ofs-delta push-options agent=git/2.56.0";
    let push = cat(&[
        &pkt(&format!("{ZERO} {} refs/heads/main\0{caps}\n", oid(1))),
        &pkt(&format!("{} {} refs/heads/topic\n", oid(2), oid(3))),
        &pkt(&format!("{} {ZERO} refs/heads/gone\n", oid(4))),
        &pkt(&format!("{ZERO} {} refs/x\n", oid(5))),
        FLUSH,
        &pkt("force=refs/heads/topic\n"),
        &pkt(&format!("lease=refs/heads/topic:{}\n", oid(2))),
        &pkt("ci.skip\n"),
        FLUSH,
        b"PACK\0\0\0\x02\0\0\0\0",
    ]);
    let plain_push = cat(&[
        &pkt(&format!(
            "{ZERO} {} refs/heads/main\0report-status ofs-delta\n",
            oid(1)
        )),
        FLUSH,
    ]);
    let shallow_push = cat(&[
        &pkt(&format!("shallow {}\n", oid(9))),
        &pkt(&format!(
            "{ZERO} {} refs/heads/main\0report-status side-band quiet\n",
            oid(1)
        )),
        FLUSH,
    ]);
    let bad_push = cat(&[
        &pkt(&format!(
            "{ZERO} {} refs/heads/a\0report-status-v2 push-cert\n",
            oid(1)
        )),
        &pkt(&format!("{ZERO} {} refs/heads/a\n", oid(1))),
        FLUSH,
    ]);
    for (name, wire) in [
        ("push-sideband-options", &push),
        ("push-plain", &plain_push),
        ("push-shallow-side-band", &shallow_push),
        ("push-repeated-ref", &bad_push),
    ] {
        for config in 0..4u8 {
            let mut v = header(config, 0x5eed + u64::from(config));
            v.extend_from_slice(wire);
            out.push(("receive_request", format!("{name}-{config}"), v));
        }
    }

    let fetch = cat(&[
        &pkt("command=fetch\n"),
        &pkt("agent=git/2.56.0\n"),
        &pkt("object-format=sha1\n"),
        DELIM,
        &pkt("thin-pack\n"),
        &pkt("ofs-delta\n"),
        &pkt("include-tag\n"),
        &pkt(&format!("want {}\n", oid(1))),
        &pkt(&format!("want {}\n", oid(2))),
        &pkt(&format!("have {}\n", oid(3))),
        &pkt(&format!("have {}\n", oid(4))),
        FLUSH,
    ]);
    let fetch_done_uris = cat(&[
        &pkt("command=fetch\n"),
        &pkt("agent=git/2.39.5\n"),
        DELIM,
        &pkt(&format!("want {}\n", oid(1))),
        &pkt("packfile-uris https,http\n"),
        &pkt("no-progress\n"),
        &pkt("done\n"),
        FLUSH,
    ]);
    let fetch_shallow = cat(&[
        &pkt("command=fetch\n"),
        DELIM,
        &pkt(&format!("want {}\n", oid(1))),
        &pkt(&format!("shallow {}\n", oid(5))),
        &pkt("deepen 1\n"),
        &pkt("deepen-relative\n"),
        &pkt("deepen-since 1700000000\n"),
        &pkt("deepen-not refs/heads/main\n"),
        &pkt("filter blob:none\n"),
        &pkt("done\n"),
        FLUSH,
    ]);
    let ls_refs = cat(&[
        &pkt("command=ls-refs\n"),
        &pkt("agent=git/2.56.0\n"),
        &pkt("server-option=x\n"),
        DELIM,
        &pkt("peel\n"),
        &pkt("symrefs\n"),
        &pkt("unborn\n"),
        &pkt("ref-prefix HEAD\n"),
        &pkt("ref-prefix refs/heads/\n"),
        FLUSH,
        &pkt("command=fetch\n"),
        DELIM,
        &pkt(&format!("want {}\n", oid(1))),
        &pkt("done\n"),
        FLUSH,
        FLUSH,
    ]);
    for (name, wire) in [
        ("fetch-negotiate", &fetch),
        ("fetch-done-uris", &fetch_done_uris),
        ("fetch-shallow-filter", &fetch_shallow),
        ("ls-refs-then-fetch", &ls_refs),
    ] {
        for config in [0u8, 1, 2, 5] {
            let mut v = header(config, 0xfe7c + u64::from(config));
            v.extend_from_slice(wire);
            out.push(("v2_command", format!("{name}-{config}"), v));
        }
    }

    for (scenario, files) in transcripts() {
        let mut stateful = header(1, 0x0b0e);
        for (file, bytes) in &files {
            if file.ends_with("-request.pkt") {
                let stem = file.trim_end_matches(".pkt");
                for config in [0u8, 2] {
                    let mut v = header(config, 0x7a1e + u64::from(config));
                    v.extend_from_slice(bytes);
                    out.push(("upload_v0", format!("{scenario}-{stem}-{config}"), v));
                }
                stateful.extend_from_slice(bytes);
            }
            let mut v = header(0, 0x9c7);
            v.extend_from_slice(bytes);
            out.push(("pkt_line", format!("{scenario}-{file}"), v));
        }
        out.push(("upload_v0", format!("{scenario}-stateful"), stateful));
    }
    let special = cat(&[
        FLUSH,
        DELIM,
        b"0002",
        b"0004",
        &pkt_bytes(&[1, b'x']),
        &pkt("\x02progress\n"),
    ]);
    let mut big = header(0, 1);
    big.extend_from_slice(&pkt_bytes(&vec![b'a'; 65_516]));
    for (name, wire) in [
        ("special-packets", special),
        ("max-length", big[9..].to_vec()),
        ("too-long", b"fff1".to_vec()),
        ("short-length", b"0003".to_vec()),
        ("not-hex", b"00zz".to_vec()),
        ("truncated", b"0010abc".to_vec()),
        ("push-request", push.clone()),
    ] {
        let mut v = header(0, 0x9c7);
        v.extend_from_slice(&wire);
        out.push(("pkt_line", name.to_owned(), v));
    }
}

fn full(kind: ObjectKind, data: &[u8]) -> RawEntry {
    RawEntry {
        type_code: type_code(kind),
        declared: data.len() as u64,
        ofs_base: None,
        ofs_distance: None,
        ref_base: None,
        body: deflate(data),
    }
}

fn ofs(base_index: usize, base: &[u8], target: &[u8]) -> RawEntry {
    let d = delta(base, target);
    RawEntry {
        type_code: 6,
        declared: d.len() as u64,
        ofs_base: Some(base_index),
        ofs_distance: None,
        ref_base: None,
        body: deflate(&d),
    }
}

fn refd(base_id: ObjectId, base: &[u8], target: &[u8]) -> RawEntry {
    let d = delta(base, target);
    RawEntry {
        type_code: 7,
        declared: d.len() as u64,
        ofs_base: None,
        ofs_distance: None,
        ref_base: Some(base_id),
        body: deflate(&d),
    }
}

/// Small packs for the raw pack targets, as a client could send them.
pub fn sample_packs() -> Vec<(&'static str, Vec<u8>)> {
    let a = b"line one\nline two\nline three\n".to_vec();
    let b = b"line one\nline 2\nline three\nline four\n".to_vec();
    let c = b"line one\nline 2\nline three\nline four\nline five\n".to_vec();
    let id = |d: &[u8]| object_id(ObjectKind::Blob, d);
    let commit = format!(
        "tree {}\nparent {}\nauthor A <a@x> 1 +0000\ncommitter C <c@x> 1700000000 +0000\n\nmsg\n",
        oid(7),
        oid(8)
    );
    let mut tree = b"100644 a\0".to_vec();
    tree.extend_from_slice(id(&a).as_bytes());
    tree.extend_from_slice(b"40000 dir\0");
    tree.extend_from_slice(&[9; 20]);
    let tag = format!(
        "object {}\ntype commit\ntag v1\ntagger T <t@x> 1 +0000\n\nv1\n",
        oid(3)
    );
    let zeros = vec![0u8; 2 << 20];
    vec![
        ("empty", write_pack(&[], None)),
        (
            "full-objects",
            write_pack(
                &[
                    full(ObjectKind::Commit, commit.as_bytes()),
                    full(ObjectKind::Tree, &tree),
                    full(ObjectKind::Blob, &a),
                    full(ObjectKind::Tag, tag.as_bytes()),
                ],
                None,
            ),
        ),
        (
            "ofs-chain",
            write_pack(
                &[full(ObjectKind::Blob, &a), ofs(0, &a, &b), ofs(1, &b, &c)],
                None,
            ),
        ),
        (
            "ref-in-pack",
            write_pack(&[full(ObjectKind::Blob, &a), refd(id(&a), &a, &b)], None),
        ),
        (
            "ref-forward-base",
            write_pack(&[refd(id(&a), &a, &b), full(ObjectKind::Blob, &a)], None),
        ),
        ("thin", write_pack(&[refd(id(&a), &a, &b)], None)),
        (
            "ref-cycle",
            write_pack(&[refd(id(&c), &c, &b), refd(id(&b), &b, &c)], None),
        ),
        (
            "repeated-object",
            write_pack(
                &[full(ObjectKind::Blob, &a), full(ObjectKind::Blob, &a)],
                None,
            ),
        ),
        (
            "zip-bomb",
            write_pack(&[full(ObjectKind::Blob, &zeros)], None),
        ),
        (
            "delta-claims-huge-result",
            write_pack(
                &[
                    full(ObjectKind::Blob, &a),
                    RawEntry {
                        type_code: 6,
                        declared: 12,
                        ofs_base: Some(0),
                        ofs_distance: None,
                        ref_base: None,
                        body: deflate(&[a.len() as u8, 0x80, 0x80, 0x80, 0x80, 0x10, 0x91, 0, 1]),
                    },
                ],
                None,
            ),
        ),
        (
            "ofs-into-header",
            write_pack(
                &[
                    full(ObjectKind::Blob, &a),
                    RawEntry {
                        type_code: 6,
                        declared: 3,
                        ofs_base: None,
                        ofs_distance: Some(1),
                        ref_base: None,
                        body: deflate(&[1, 1, 0x90]),
                    },
                ],
                None,
            ),
        ),
        (
            "count-too-high",
            write_pack(&[full(ObjectKind::Blob, &a)], Some(3)),
        ),
    ]
}

fn pack_seeds(out: &mut Vec<Seed>) {
    for (name, pack) in sample_packs() {
        for config in [0u8, 1] {
            let mut v = vec![config];
            v.extend_from_slice(&pack);
            out.push(("pack_bytes", format!("{name}-{config}"), v));
        }
    }
    // Stored formats: the `.idx` and entry table of a real pack, a header
    // and a commit.
    let packs = sample_packs();
    let ofs_chain = &packs
        .iter()
        .find(|(n, _)| *n == "ofs-chain")
        .expect("listed")
        .1;
    let dir = tempfile::tempdir().expect("scratch");
    let path = dir.path().join("p.pack");
    std::fs::write(&path, ofs_chain).expect("scratch write");
    let v = aspen_pack::verify::verify_pack(
        &path,
        dir.path(),
        aspen_pack::MaxObjectBytes::new(crate::pack::MAX_OBJECT).expect("non-zero"),
        aspen_pack::MaxInflatedBytes::new(crate::pack::MAX_INFLATED).expect("non-zero"),
    )
    .expect("the sample pack verifies");
    let mut idx = vec![0u8];
    idx.extend_from_slice(&v.idx);
    out.push(("pack_formats", "idx".into(), idx));
    let mut table = vec![1u8];
    table.extend_from_slice(&v.table.encode());
    out.push(("pack_formats", "entry-table".into(), table));
    out.push(("pack_formats", "header".into(), vec![2, 0xb5, 0x80, 0x01]));
    let commit = format!(
        "\x03tree {}\nparent {}\nparent {}\nauthor A <a@x> 1 +0000\ncommitter C <c@x>> 1700000000 +0000\n\nm\n",
        oid(1),
        oid(2),
        oid(3)
    );
    out.push(("pack_formats", "commit".into(), commit.into_bytes()));
}

fn structured_seeds(out: &mut Vec<Seed>) {
    // Structured targets read decisions from bytes; a few patterns reach
    // different shapes (zeros take the common path everywhere).
    let patterns: Vec<(&str, Vec<u8>)> = vec![
        ("zeros", vec![0; 256]),
        ("ramp", (0..=255u8).collect()),
        ("ramp-twice", (0..=255u8).chain(0..=255u8).collect()),
        ("odd", (0..512).map(|i| (i * 37 % 251) as u8 | 1).collect()),
        ("sevens", (0..512).map(|i| (i * 7 + 3) as u8).collect()),
        ("long-chain", [&[1u8][..], &[0; 64]].concat()),
    ];
    for target in [
        "pack_ingest",
        "stitch",
        "planner",
        "git_route",
        "ssh_command",
        "gzip_body",
        "jwt",
    ] {
        for (name, bytes) in &patterns {
            out.push((target, (*name).to_owned(), bytes.clone()));
        }
    }
}

/// Raw-mode seeds for the front-end targets (`front`, `jwt`): real request
/// paths, SSH commands, a gzip request body and a token shape. Their
/// built modes start from the structured patterns above.
fn front_seeds(out: &mut Vec<Seed>) {
    use crate::front::RAW;
    let raw = |text: &[u8]| [&[RAW][..], text].concat();
    for (name, path) in [
        (
            "raw-advertisement",
            "/acme/mono.git/info/refs?service=git-upload-pack",
        ),
        (
            "raw-scoped-push",
            "/acme/mono.git/-/refs/heads/agent/7/git-receive-pack",
        ),
        ("raw-fetch", "/demo/git-upload-pack?x=1"),
        ("raw-dumb", "/demo.git/info/refs"),
        ("raw-health", "/healthz"),
    ] {
        out.push(("git_route", name.to_owned(), raw(path.as_bytes())));
    }
    for (name, command) in [
        ("raw-ssh-url", "git-upload-pack '/acme/mono.git'"),
        ("raw-scp-like", "git-receive-pack 'acme/mono.git'"),
        (
            "raw-two-words-scoped",
            "git upload-pack '/acme/mono.git/-/refs/heads/agent/7'",
        ),
        ("raw-quoted-quote", r"git-upload-pack '/a'\''b'"),
        ("raw-shell", "git-upload-pack '/demo.git'; rm -rf /"),
    ] {
        out.push(("ssh_command", name.to_owned(), raw(command.as_bytes())));
    }
    // A v2 fetch request as Git gzips it, after the raw-mode byte and the
    // 8-byte seed of the chunking.
    let request = cat(&[
        &pkt("command=fetch\n"),
        DELIM,
        &pkt(&format!("want {}\n", oid(1))),
        &pkt("done\n"),
        FLUSH,
    ]);
    let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    std::io::Write::write_all(&mut gz, &request).expect("in-memory write");
    let gz = gz.finish().expect("in-memory write");
    out.push((
        "gzip_body",
        "raw-fetch-request".to_owned(),
        cat(&[&[RAW], &7u64.to_le_bytes(), &gz]),
    ));
    // A token with the shape of an ES256 identity token and a signature
    // no key made; after the raw-mode byte and the leeway flag.
    let token = "eyJhbGciOiJFUzI1NiIsImtpZCI6InAyNTYifQ.\
                 eyJpc3MiOiJodHRwczovL2lzc3Vlci5leGFtcGxlIiwiYXVkIjoiYXNwZW4iLCJzdWIiOiJ4In0.\
                 c2lnbmF0dXJl";
    out.push((
        "jwt",
        "raw-token".to_owned(),
        cat(&[&[RAW, 1], token.as_bytes()]),
    ));
}
