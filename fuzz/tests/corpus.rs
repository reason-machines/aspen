//! Required-CI replay of the fuzz corpus on the stable toolchain: every
//! committed seed (`fuzz/corpus/<target>`) and every saved crash
//! (`fuzz/regressions/<target>`) runs through its target, so a fixed bug
//! stays fixed and the targets keep compiling and passing on known inputs.
//! It also checks that the committed corpus is exactly what the seed
//! generator writes, and that generated inputs still reach every oracle
//! path of the structured targets. The fuzzing itself runs in
//! `.github/workflows/fuzz.yml`.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use aspen_fuzz::Reached;
use aspen_fuzz::input::Mix;

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).to_owned()
}

fn inputs(dir: &Path) -> Vec<PathBuf> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut files: Vec<PathBuf> = entries
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.is_file())
        .collect();
    files.sort();
    files
}

fn replay(kind: &str, require: bool) {
    for target in aspen_fuzz::TARGETS {
        let dir = root().join(kind).join(target.name);
        let files = inputs(&dir);
        assert!(
            !require || !files.is_empty(),
            "{} has no inputs; run `cargo run -p aspen-fuzz -- seeds fuzz/corpus`",
            dir.display()
        );
        for f in files {
            let bytes = std::fs::read(&f).expect("readable input");
            let result = std::panic::catch_unwind(|| (target.run)(&bytes));
            assert!(
                result.is_ok(),
                "{} fails on {}; reproduce with `cargo run -p aspen-fuzz -- run {} {}`",
                target.name,
                f.display(),
                target.name,
                f.display()
            );
        }
    }
}

#[test]
fn every_target_passes_its_seed_corpus() {
    replay("corpus", true);
}

#[test]
fn every_saved_crash_stays_fixed() {
    replay("regressions", false);
}

#[test]
fn the_committed_corpus_is_exactly_the_generated_seeds() {
    let seeds = aspen_fuzz::seeds::all();
    let generated: BTreeSet<PathBuf> = seeds
        .iter()
        .map(|(target, name, _)| root().join("corpus").join(target).join(name))
        .collect();
    for target in aspen_fuzz::TARGETS {
        for f in inputs(&root().join("corpus").join(target.name)) {
            assert!(
                generated.contains(&f),
                "{} is not a generated seed: delete it, or add it to src/seeds.rs \
                 (inputs that once failed belong in fuzz/regressions/)",
                f.display()
            );
        }
    }
    for (target, name, bytes) in seeds {
        let path = root().join("corpus").join(target).join(&name);
        let committed = std::fs::read(&path).unwrap_or_else(|_| {
            panic!(
                "{} is missing; run `cargo run -p aspen-fuzz -- seeds fuzz/corpus`",
                path.display()
            )
        });
        assert_eq!(
            committed,
            bytes,
            "{} differs from the generator; run `cargo run -p aspen-fuzz -- seeds fuzz/corpus`",
            path.display()
        );
    }
}

#[test]
fn every_target_has_a_libfuzzer_wrapper() {
    for target in aspen_fuzz::TARGETS {
        let wrapper = root()
            .join("libfuzzer/fuzz_targets")
            .join(format!("{}.rs", target.name));
        let text = std::fs::read_to_string(&wrapper)
            .unwrap_or_else(|_| panic!("{} is missing", wrapper.display()));
        assert!(
            text.contains(&format!("aspen_fuzz::{}", path_of(target.name))),
            "{} does not call its target",
            wrapper.display()
        );
    }
    let manifest = std::fs::read_to_string(root().join("libfuzzer/Cargo.toml")).expect("manifest");
    for target in aspen_fuzz::TARGETS {
        assert!(
            manifest.contains(&format!("name = \"{}\"", target.name)),
            "fuzz/libfuzzer/Cargo.toml has no [[bin]] for {}",
            target.name
        );
    }
}

/// Runs `count` inputs of random bytes (a fixed sequence) through `run` and
/// returns every path they reached.
fn reached_by(run: fn(&[u8]) -> BTreeSet<Reached>, seed: u64, count: usize) -> BTreeSet<Reached> {
    let mut mix = Mix::new(seed);
    let mut all = BTreeSet::new();
    for _ in 0..count {
        let len = mix.below(768);
        let data: Vec<u8> = (0..len).map(|_| mix.next_u64() as u8).collect();
        all.extend(run(&data));
    }
    all
}

fn assert_reaches(name: &str, run: fn(&[u8]) -> BTreeSet<Reached>, count: usize, want: &[Reached]) {
    let got = reached_by(run, 0xd3, count);
    let missing: Vec<&Reached> = want.iter().filter(|w| !got.contains(w)).collect();
    assert!(
        missing.is_empty(),
        "{count} generated inputs of {name} never reached {missing:?} (reached {got:?}); \
         a generator change starved these checks"
    );
}

#[test]
fn generated_pack_ingest_inputs_reach_every_oracle_path() {
    assert_reaches(
        "pack_ingest",
        aspen_fuzz::pack::pack_ingest_reached,
        100,
        &[
            Reached::ExactAccept,
            Reached::RefusedRepeat,
            Reached::RefusedUnresolved,
            Reached::RefusedOverLimit,
            Reached::RefusedShortDelta,
            Reached::RefusedLie,
            Reached::RefusedCount,
        ],
    );
}

#[test]
fn the_short_delta_regression_is_refused_for_its_short_delta() {
    // D3's sweep input 10679 (issue #162): a thin REF_DELTA to the empty
    // blob, 2 bytes of delta data. Replaying it must take the short-delta
    // refusal, not pass for another reason.
    let path = root().join("regressions/pack_ingest/short-delta-empty-result");
    let bytes = std::fs::read(&path).expect("a committed regression");
    let reached = aspen_fuzz::pack::pack_ingest_reached(&bytes);
    assert!(
        reached.contains(&Reached::RefusedShortDelta),
        "{} reached {reached:?}",
        path.display()
    );
}

#[test]
fn generated_stitch_inputs_reach_every_oracle_path() {
    assert_reaches(
        "stitch",
        aspen_fuzz::pack::stitch_reached,
        40,
        &[Reached::ExactAccept, Reached::RefusedDamage],
    );
}

#[test]
fn generated_planner_inputs_reach_every_oracle_path() {
    assert_reaches(
        "planner",
        aspen_fuzz::planner::planner_reached,
        100,
        &[
            Reached::ExactAccept,
            Reached::RefusedNoWants,
            Reached::MissingVersion,
        ],
    );
}

#[test]
fn generated_git_route_inputs_reach_every_oracle_path() {
    assert_reaches(
        "git_route",
        aspen_fuzz::front::git_route_reached,
        300,
        &[
            Reached::RouteOk,
            Reached::RouteNotGit,
            Reached::RouteBadName,
            Reached::RouteDumbHttp,
        ],
    );
}

#[test]
fn generated_ssh_command_inputs_reach_every_oracle_path() {
    assert_reaches(
        "ssh_command",
        aspen_fuzz::front::ssh_command_reached,
        300,
        &[Reached::SshOk, Reached::SshNotGit, Reached::SshBadPath],
    );
}

#[test]
fn generated_gzip_body_inputs_reach_every_oracle_path() {
    assert_reaches(
        "gzip_body",
        aspen_fuzz::front::gzip_body_reached,
        200,
        &[
            Reached::GzipOk,
            Reached::GzipTooLarge,
            Reached::GzipEncoding,
            Reached::GzipCorrupt,
            Reached::GzipOverCap,
            Reached::GzipSentTooMuch,
        ],
    );
}

#[test]
fn generated_jwt_inputs_reach_every_oracle_path() {
    assert_reaches(
        "jwt",
        aspen_fuzz::jwt::jwt_reached,
        300,
        &[
            Reached::JwtOk,
            Reached::JwtMalformed,
            Reached::JwtAlgorithm,
            Reached::JwtCritical,
            Reached::JwtNoKey,
            Reached::JwtSignature,
            Reached::JwtClaim("iss"),
            Reached::JwtClaim("aud"),
            Reached::JwtClaim("exp"),
            Reached::JwtClaim("nbf"),
            Reached::JwtClaim("sub"),
        ],
    );
}

#[test]
fn the_stored_format_seeds_decode() {
    for name in ["idx", "entry-table"] {
        let path = root().join("corpus/pack_formats").join(name);
        let bytes = std::fs::read(&path).expect("a committed seed");
        assert!(
            aspen_fuzz::pack::pack_formats_decodes(&bytes),
            "{} does not get past its parser, so it seeds nothing beyond the first check",
            path.display()
        );
    }
}

/// The module path of a target's function, as the wrappers call it.
fn path_of(name: &str) -> String {
    let module = match name {
        "pkt_line" | "receive_request" | "v2_command" | "upload_v0" => "proto",
        "planner" => "planner",
        "git_route" | "ssh_command" | "gzip_body" => "front",
        "jwt" => "jwt",
        _ => "pack",
    };
    format!("{module}::{name}")
}
