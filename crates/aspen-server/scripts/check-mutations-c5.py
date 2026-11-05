#!/usr/bin/env python3
"""Breaks each guard node C5's tests rely on (observability: metrics, the
database gauges, `aspen doctor jobs`, telemetry, and the shipped dashboard
and alert rules), one at a time, and shows that a test fails without it.
Run from the repository root:

    python3 crates/aspen-server/scripts/check-mutations-c5.py [O1 O2 ...]

Each mutation is an exact text replacement in one file; the file is
restored afterwards. A mutation whose pattern no longer matches is
reported, so the list fails loudly when the code it targets changes.
Exit status 1 if any mutation survives or no longer applies.

O1 to O4 run every `sqlite_store` test of aspen-meta's store suite, so a
kill there shows that some store test catches the broken query, not which
one; the gauge tests in `stats` are the ones written for them."""
import subprocess, sys

ST = 'crates/aspen-meta/src/stats.rs'
LIB = 'crates/aspen-server/src/lib.rs'
PUSH = 'crates/aspen-server/src/receive/push.rs'
MP = 'crates/aspen-server/src/metrics/push.rs'
MS = 'crates/aspen-server/src/metrics/store.rs'
EX = 'crates/aspen-server/src/metrics/expo.rs'
AL = 'deploy/observability/prometheus/aspen-alerts.yml'
DB = 'deploy/observability/grafana/aspen.json'
DOC = 'crates/aspen-cli/src/doctor.rs'
TEL = 'crates/aspen-cli/src/telemetry.rs'

META = ["-p", "aspen-meta", "--test", "store", "sqlite_store"]
OBS = ["-p", "aspen-server", "--test", "observability"]
CLI = ["-p", "aspen-cli", "--test", "doctor"]

muts = [
 ("O1 job counts include done jobs", ST,
  "FROM jobs WHERE state <> 'done' GROUP BY kind, state",
  "FROM jobs GROUP BY kind, state", META),
 ("O2 queue lag taken over every state, not queued jobs", ST,
  "MIN(CASE WHEN state = 'queued' THEN run_after_us END) AS earliest",
  "MIN(run_after_us) AS earliest", META),
 ("O3 trunk closures count every trunk version, not the current one", ST,
  "JOIN ref_versions v ON v.id = f.version_id",
  "JOIN ref_versions v ON v.repo_id = f.repo_id AND v.ref_name = f.ref_name", META),
 ("O4 trunk closures count every ref, not trunk", ST,
  "JOIN refs f ON f.repo_id = r.id AND f.ref_name = r.trunk_ref",
  "JOIN refs f ON f.repo_id = r.id", META),
 ("O5 /-/metrics on the public Git listener by default", LIB,
  "if self.inner.config.metrics_on_git_listener {",
  "if true {", OBS + ["metrics_are_served_on_their_own_listener_by_default"]),
 ("O6 every committed ref counted as a trunk version", PUSH,
  ".filter(|w| w.name == trunk)",
  ".filter(|_w| true)", OBS + ["pushes_and_fetches_are_counted_by_result"]),
 ("O7 refused ref updates counted as ok", PUSH,
  "VerdictKind::Reject(r) => (RefStatus::from(*r), RefOutcome::Rejected(*r)),",
  "VerdictKind::Reject(r) => (RefStatus::from(*r), RefOutcome::Ok),",
  OBS + ["pushes_and_fetches_are_counted_by_result"]),
 ("O8 wider trunk versions counted as exact", MP,
  "Closure::Wider => &self.trunk_wider,",
  "Closure::Wider => &self.trunk_exact,",
  OBS + ["wider_trunk_versions_show_in_the_gauge_and_the_push_counter"]),
 ("O9 job gauge ignores the state", MS,
  ".find(|c| c.kind == kind && c.state == state)",
  ".find(|c| c.kind == kind)", OBS + ["dead_jobs_are_exported_per_kind_until_requeued"]),
 ("O10 database gauges never refreshed after the first read", MS,
  "if state.tried.is_none_or(|t| t.elapsed() >= self.refresh) {",
  "if state.tried.is_none() {", OBS + ["dead_jobs_are_exported_per_kind_until_requeued"]),
 ("O11 label values not escaped", EX,
  "'\"' => out.push_str(\"\\\\\\\"\"),",
  "'\"' => out.push('\"'),", ["-p", "aspen-server", "--lib", "label_values_are_escaped"]),
 ("O12 the dead-job alert uses a metric the server does not export", AL,
  "expr: max by (kind) (aspen_jobs{state=\"dead\"}) > 0",
  "expr: max by (kind) (aspen_dead_jobs) > 0",
  OBS + ["the_dashboard_and_alerts_use_only_exported_metrics"]),
 ("O13 a dashboard panel uses a metric the server does not export", DB,
  "aspen_push_ref_updates_total",
  "aspen_push_ref_results_total",
  OBS + ["the_dashboard_and_alerts_use_only_exported_metrics"]),
 ("O14 no alert for a paging counter", AL,
  "expr: sum(increase(aspen_alert_store_corruption_total[10m])) > 0",
  "expr: vector(0) > 0", OBS + ["the_dashboard_and_alerts_use_only_exported_metrics"]),
 ("O15 doctor exits 0 with dead jobs", DOC,
  "Ok(if dead_total == 0 {",
  "Ok(if true {", CLI + ["dead_jobs_are_listed_with_their_last_error_and_exit_1"]),
 ("O16 stored errors printed raw (can forge listing lines)", DOC,
  ".map(|c| if c.is_control() { ' ' } else { c })",
  ".map(|c| c)", CLI + ["a_multi_line_error_stays_on_one_line"]),
 ("O17 OTEL_SDK_DISABLED ignored", TEL,
  "let disabled = get(\"OTEL_SDK_DISABLED\").is_some_and(|v| v.eq_ignore_ascii_case(\"true\"))",
  "let disabled = get(\"OTEL_SDK_DISABLED\").is_some_and(|_| false)",
  CLI + ["the_sdk_switch_turns_export_off"]),
 ("O18 traces not flushed at exit", TEL,
  "if let Some(provider) = self.provider.take()\n",
  "if let Some(provider) = self.provider.take().filter(|_| false)\n",
  CLI + ["spans_are_exported_over_otlp_http_before_exit"]),
 ("O19 an unsupported OTLP protocol accepted", TEL,
  "                && p != \"http/protobuf\"\n",
  "                && false\n",
  CLI + ["an_unsupported_otlp_protocol_is_refused"]),
 ("O20 database gauges read on every scrape, whatever the refresh interval", MS,
  "if state.tried.is_none_or(|t| t.elapsed() >= self.refresh) {",
  "if true {", OBS + ["store_gauges_are_read_at_most_once_per_refresh_interval"]),
 ("O21 a database gauge read without a timeout", MS,
  "let read = tokio::time::timeout(self.timeout, self.read()).await;",
  "let read: Result<_, tokio::time::error::Elapsed> = Ok(self.read().await);",
  OBS + ["a_hung_database_read_times_out_and_serves_the_last_values"]),
 ("O22 the access log inside the guard (refused requests unlogged)", LIB,
  "            .layer(axum::middleware::from_fn_with_state(\n"
  "                access,\n"
  "                access::guard::<M, T>,\n"
  "            ))\n"
  "            // Outside the guard, so requests it refuses are logged too.\n"
  "            .layer(axum::middleware::from_fn(access_log));",
  "            .layer(axum::middleware::from_fn(access_log))\n"
  "            .layer(axum::middleware::from_fn_with_state(\n"
  "                access,\n"
  "                access::guard::<M, T>,\n"
  "            ));",
  OBS + ["the_access_log_covers_refused_requests_without_credentials_or_queries"]),
 ("O23 the access log records the query string", LIB,
  "        path = request.uri().path(),",
  "        path = request.uri().path_and_query().map(|p| p.as_str()),",
  OBS + ["the_access_log_covers_refused_requests_without_credentials_or_queries"]),
]

only = sys.argv[1:]
bad = False
for name, f, old, new, args in muts:
    if only and not any(name.startswith(o + " ") for o in only):
        continue
    src = open(f).read()
    if src.count(old) != 1:
        print((name, "PATTERN NOT FOUND x%d" % src.count(old)), flush=True)
        bad = True
        continue
    open(f, 'w').write(src.replace(old, new))
    try:
        r = subprocess.run(["cargo", "test", "--locked"] + args[:-1] + ["--", args[-1]],
                           capture_output=True, text=True, timeout=1500)
        out = r.stdout + r.stderr
        if "error[" in out or "could not compile" in out:
            verdict = "COMPILE ERROR"
            bad = True
        elif r.returncode != 0 and "test result: FAILED" in out:
            verdict = "KILLED (test fails)"
        elif r.returncode == 0:
            verdict = "SURVIVED"
            bad = True
        else:
            verdict = "ERROR exit %d" % r.returncode
            bad = True
    finally:
        open(f, 'w').write(src)
    print((name, verdict), flush=True)
sys.exit(1 if bad else 0)
