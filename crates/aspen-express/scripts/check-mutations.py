#!/usr/bin/env python3
"""Shows that the C2 (aspen-express) tests pin their invariants.

Each mutation below breaks one guarded behaviour in the source, runs the
test that should catch it, and restores the file. Every line must say
"FAILS (caught)". Run from the repository root:

    python3 crates/aspen-express/scripts/check-mutations.py

It edits files in place while it runs; do not run it with uncommitted
changes to those files. Exits non-zero if any mutation is not caught.
"""
import os
import subprocess
import sys

muts = [
 ("M1 max-age past the URL expiry", "crates/aspen-express/src/serve.rs",
  'granted.expires.saturating_sub(unix_now())\n', 'granted.expires.saturating_sub(unix_now()) + 3600\n', ["-p","aspen-express","--test","serve","responses_are_immutable"]),
 ("M2 signature ignores the repository", "crates/aspen-express/src/url.rs",
  'mac.update(key.as_str().as_bytes());', 'mac.update(key.as_str().rsplit(\'/\').next().unwrap().as_bytes());', ["-p","aspen-express","--test","serve","cross_repo"]),
 ("M3 expiry not checked", "crates/aspen-express/src/url.rs",
  'if now >= expires_n {', 'if false && now >= expires_n {', ["-p","aspen-express","--test","serve","cross_repo"]),
 ("M4 cache skips checksum verification", "crates/aspen-express/src/cache.rs",
  'if digest.as_slice() != trailer.as_slice()', 'if false && digest.as_slice() != trailer.as_slice()', ["-p","aspen-express","--test","serve","match_their_key"]),
 ("M5 refusals cacheable", "crates/aspen-express/src/serve.rs",
  '.header(header::CACHE_CONTROL, "no-store")', '.header(header::CACHE_CONTROL, "public, max-age=60")', ["-p","aspen-server","--test","express","cdn_mode"]),
 ("M6 health ignored when routing", "crates/aspen-express/src/routes.rs",
  'if !e.is_up() {', 'if false && !e.is_up() {', ["-p","aspen-server","--test","express","stitched"]),
 ("M7 express tried after the store URL", "crates/aspen-server/src/upload_pack/deliver.rs",
  'if let Some(uri) = crate::express::uri(server, &p, request, direct_ok) {', 'if let Some(uri) = None::<aspen_core::fetch::PackfileUri> {', ["-p","aspen-server","--test","express","express_mode"]),
 ("M8 expiry not rounded to the window", "crates/aspen-express/src/url.rs",
  '.div_ceil(w)\n            .saturating_mul(w)', '', ["-p","aspen-express","--lib","expiries_round_up"]),
 ("M9 panicked fill keeps its single-flight slot", "crates/aspen-express/src/cache.rs",
  '            st.fills.remove(&self.key);\n', '            if !std::thread::panicking() {\n                st.fills.remove(&self.key);\n            }\n', ["-p","aspen-express","--lib","a_panicked_fill"]),
 ("M10 no cap on concurrent fills (#123)", "crates/aspen-express/src/cache.rs",
  'if st.fills.len() >= self.max_fills || ', 'if false || ', ["-p","aspen-express","--lib","at_most_max_fills"]),
 ("M11 fills copy without reserving room (#123)", "crates/aspen-express/src/cache.rs",
  'if st.reserved.saturating_add(size) > self.max_bytes {', 'if false {', ["-p","aspen-express","--lib","fills_in_progress_count"]),
 ("M12 eviction ignores fills in progress (#123)", "crates/aspen-express/src/cache.rs",
  'while st.used + st.reserved + incoming > self.max_bytes {', 'while st.used + incoming > self.max_bytes {', ["-p","aspen-express","--lib","fills_in_progress_count"]),
 ("M13 max-age counted from the request's arrival (#68)", "crates/aspen-express/src/serve.rs",
  'granted.expires.saturating_sub(unix_now())\n', 'granted.expires.saturating_sub(now)\n', ["-p","aspen-express","--test","serve","max_age_counts"]),
]
env = dict(os.environ)
missed = False
for name, path, old, new, args in muts:
    src = open(path).read()
    assert src.count(old) == 1, (name, src.count(old))
    open(path, "w").write(src.replace(old, new))
    try:
        r = subprocess.run(["cargo","test","-q",*args], capture_output=True, text=True, env=env)
        out = r.stdout + r.stderr
        verdict = "FAILS (caught)" if r.returncode != 0 else "PASSES (NOT caught)"
        failed = [l for l in out.splitlines() if l.startswith("test ") and "FAILED" in l] or [l for l in out.splitlines() if "error" in l][:2]
        print(f"{name}: {verdict} {failed[:3]}", flush=True)
        missed = missed or r.returncode == 0
    finally:
        open(path, "w").write(src)
sys.exit(1 if missed else 0)
