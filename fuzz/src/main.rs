//! `aspen-fuzz`: run fuzz-target inputs on the stable toolchain, and write
//! the seed corpus.
//!
//! ```text
//! aspen-fuzz list                         every target
//! aspen-fuzz run <target> <path>...       run files (or every file in directories)
//! aspen-fuzz seeds <dir>                  write the seed corpus to <dir>/<target>/
//! ```
//!
//! `run` is how to reproduce a crash libFuzzer saved: a panic prints the
//! broken invariant and exits non-zero.

use std::path::{Path, PathBuf};
use std::process::ExitCode;

fn files(paths: &[String]) -> Vec<PathBuf> {
    let mut out = Vec::new();
    for p in paths {
        let p = Path::new(p);
        if p.is_dir() {
            let mut inner: Vec<PathBuf> = std::fs::read_dir(p)
                .unwrap_or_else(|e| panic!("{}: {e}", p.display()))
                .flatten()
                .map(|e| e.path())
                .filter(|p| p.is_file())
                .collect();
            inner.sort();
            out.extend(inner);
        } else {
            out.push(p.to_owned());
        }
    }
    out
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("list") => {
            for t in aspen_fuzz::TARGETS {
                println!("{}", t.name);
            }
            ExitCode::SUCCESS
        }
        Some("run") if args.len() >= 3 => {
            let Some(target) = aspen_fuzz::target(&args[1]) else {
                eprintln!("unknown target {}; see `aspen-fuzz list`", args[1]);
                return ExitCode::FAILURE;
            };
            let files = files(&args[2..]);
            for f in &files {
                let bytes = std::fs::read(f).unwrap_or_else(|e| panic!("{}: {e}", f.display()));
                eprintln!("{}: {} ({} bytes)", target.name, f.display(), bytes.len());
                (target.run)(&bytes);
            }
            println!("{}: {} inputs ran clean", target.name, files.len());
            ExitCode::SUCCESS
        }
        Some("seeds") if args.len() == 2 => {
            let n = aspen_fuzz::seeds::write(Path::new(&args[1]))
                .unwrap_or_else(|e| panic!("writing seeds: {e}"));
            println!("wrote {n} seeds to {}", args[1]);
            ExitCode::SUCCESS
        }
        _ => {
            eprintln!(
                "usage: aspen-fuzz list | run <target> <path>... | seeds <dir>\n\
                 see fuzz/README.md"
            );
            ExitCode::FAILURE
        }
    }
}
