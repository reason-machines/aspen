#!/usr/bin/env python3
"""Breaks each guard node D4 (security review) added, one at a time, and
shows that a test fails without it. Run from the repository root:

    python3 crates/aspen-server/scripts/check-mutations-d4.py [D1 D2 ...]

Each mutation is an exact text replacement in one file; the file is
restored afterwards. A mutation whose pattern no longer matches is
reported, so the list fails loudly when the code it targets changes.
Exit status 1 if any mutation survives or no longer applies."""
import subprocess, sys

LIMIT = 'crates/aspen-auth/src/limit.rs'
POLICY = 'crates/aspen-auth/src/policy.rs'
ACCESS = 'crates/aspen-server/src/access/mod.rs'
SSH = 'crates/aspen-server/src/ssh/mod.rs'
EXEC = 'crates/aspen-server/src/ssh/exec.rs'
LISTEN = 'crates/aspen-server/src/listen.rs'
BODY = 'crates/aspen-server/src/body.rs'
LIB = 'crates/aspen-server/src/lib.rs'
SERVE = 'crates/aspen-cli/src/serve.rs'
CONFIG = 'crates/aspen-cli/src/config.rs'
MIRROR = 'crates/aspen-cli/src/mirror.rs'
DOCTOR = 'crates/aspen-cli/src/doctor.rs'
PLACE = 'demo/place/server/src/place.rs'
PLACEAPI = 'demo/place/server/src/api.rs'
PLACEMAIN = 'demo/place/server/src/main.rs'
CLIENT = 'crates/aspen-server/src/access/client_addr.rs'

AUTH = ["-p", "aspen-auth", "--lib"]
IDENT = ["-p", "aspen-auth", "--test", "identify"]
HTTP = ["-p", "aspen-server", "--test", "access"]
SSHL = ["-p", "aspen-server", "--test", "ssh_limits"]
LIS = ["-p", "aspen-server", "--test", "listen"]
CLI = ["-p", "aspen-cli", "--test", "cli"]
CLIU = ["-p", "aspen-cli", "--bin", "aspen"]
PLACET = ["-p", "aspen-place", "--test", "place"]
SRVU = ["-p", "aspen-server", "--lib"]

muts = [
 ("D1 a full limiter table drops drained buckets", LIMIT,
  "        if table.buckets.len() < self.max_keys {\n            return Ok(());\n        }",
  "        if true {\n            table.buckets.clear();\n            return Ok(());\n        }",
  AUTH + ["a_full_table_refuses_new_keys_and_resets_nothing"]),
 ("D2 failed authentications are never charged", LIMIT,
  "            l.try_take(address, 1.0, now)\n                .map_err(LimitError::AuthFailures)?;",
  "            l.check(address, 1.0, now)\n                .map_err(LimitError::AuthFailures)?;",
  AUTH + ["auth_failures_are_limited_per_address_and_successes_are_free"]),
 ("D3 the HTTP guard checks credentials from a drained address", ACCESS,
  "            Some(\n                self.limits\n                    .charge_credential(address, Instant::now())\n                    .map_err(Refusal::Limit)?,\n            )",
  "            self.limits\n                .charge_credential(address, Instant::now())\n                .ok()",
  HTTP + ["failed_authentications_are_limited_per_address"]),
 ("D4 a JWT is verified by every issuer", POLICY,
  "                != iss\n            {\n                continue;",
  "                != iss\n                && false\n            {\n                continue;",
  IDENT + ["a_forged_token_is_checked_by_the_issuer_it_names_only"]),
 ("D5 repeated SSH key offers are looked up each time", SSH,
  "if let Some(&ok) = self.offers.get(fingerprint) {",
  "if let Some(&ok) = self.offers.get(fingerprint).filter(|_| false) {",
  SSHL + ["repeated_offers_of_a_registered_key_cost_one_lookup"]),
 ("D6 distinct SSH key offers are not capped", SSH,
  "if self.offers.len() >= self.shared.max_offers {",
  "if self.offers.len() >= usize::MAX {",
  SSHL + ["distinct_offers_per_connection_are_capped"]),
 ("D7 SSH key offers ignore the failed-authentication bucket", SSH,
  "            .identify(&credential, &self.address)\n            .await\n        {\n            Ok(_) => true,",
  "            .auth()\n            .identify(&credential, aspen_core::id::UnixMicros::now())\n            .await\n            .map_err(Refusal::Auth)\n        {\n            Ok(_) => true,",
  SSHL + ["failed_authentications_are_limited_per_address"]),
 ("D8 no SSH login deadline", SSH,
  "() = tokio::time::sleep_until(deadline) => {",
  "() = std::future::pending::<()>() => {",
  SSHL + ["a_connection_must_authenticate_within_the_deadline"]),
 ("D9 unauthenticated SSH connections per address are not capped", SSH,
  "if *n >= self.per_address {",
  "if *n >= usize::MAX {",
  SSHL + ["unauthenticated_connections_per_address_are_capped"]),
 ("D10 SSH connections are not capped in total", SSH,
  "let permit = Arc::clone(&self.open).try_acquire_owned().ok()?;",
  "let permit = Arc::new(Semaphore::new(1)).try_acquire_owned().ok()?;",
  SSHL + ["connections_are_capped_in_total"]),
 ("D11 refused SSH commands cost nothing", EXEC,
  "            let _ = shared\n                .access\n                .charge(RequestClass::Read, &key, Instant::now());",
  "            let _ = (&shared, &key);",
  SSHL + ["refused_commands_and_greetings_draw_on_the_request_bucket"]),
 ("D12 request headers never time out", LISTEN,
  ".header_read_timeout(limits.header_read_timeout);",
  ".header_read_timeout(None);",
  LIS + ["slow_headers_time_out_and_requests_see_their_peer"]),
 ("D13 an idle body never times out", BODY,
  "Poll::Ready(()) => Poll::Ready(Some(Err(BodyIdle(timeout).into()))),",
  "Poll::Ready(()) => {\n                        this.waiting = None;\n                        Poll::Pending\n                    }",
  ["-p", "aspen-server", "--lib", "an_idle_body_times_out_and_a_slow_one_does_not"]),
 ("D14 the server does not wrap Git bodies in the idle timeout", LIB,
  "    next.run(request.map(|b| axum::body::Body::new(body::IdleBody::new(b, timeout))))",
  "    let _ = timeout;\n    next.run(request)",
  LIS + ["a_stalled_git_request_body_is_cut_off"]),
 ("D15 /readyz queries the database on every probe", SERVE,
  "            && at.elapsed() < self.fresh_for",
  "            && at.elapsed() < Duration::ZERO",
  CLIU + ["readyz_follows_the_metadata_database"]),
 ("D16 aspen mirror accepts a password in DATABASE_URL", MIRROR,
  "    crate::config::check_database_url(&stores.database_url).map_err(|e| e.to_string())?;\n",
  "",
  CLI + ["operator_commands_refuse_and_hide_a_password_in_the_database_url"]),
 ("D17 aspen doctor jobs accepts a password in DATABASE_URL", DOCTOR,
  "    if let Err(e) = crate::config::check_database_url(url) {",
  "    if let Err(e) = crate::config::check_database_url(\"sqlite:x\") {",
  CLI + ["operator_commands_refuse_and_hide_a_password_in_the_database_url"]),
 ("D18 aspen mirror --help shows DATABASE_URL", MIRROR,
  '    #[arg(long, env = "DATABASE_URL", hide_env_values = true)]',
  '    #[arg(long, env = "DATABASE_URL")]',
  CLI + ["operator_commands_refuse_and_hide_a_password_in_the_database_url"]),
 ("D20 /-/blobs responses may be sniffed into a page", 'crates/aspen-server/src/blobs.rs',
  '        .header(header::X_CONTENT_TYPE_OPTIONS, "nosniff")\n',
  '',
  ["-p", "aspen-server", "--test", "fetch", "the_blob_route_checks_signatures_and_serves_ranges"]),
 ("D21 a full failure table admits new addresses unchecked", LIMIT,
  "            if table.buckets.len() >= self.max_keys {\n                self.evict(&mut table, now)?;\n            }\n            self.capacity",
  "            self.capacity",
  AUTH + ["a_full_table_refuses_unchecked_credentials_from_new_addresses"]),
 ("D22 unknown addresses share one failure bucket", ACCESS,
  "    address != UNKNOWN_ADDRESS\n",
  "    true\n",
  HTTP + ["an_unknown_address_does_not_share_a_failure_bucket"]),
 ("D23 the kill switch keys failures apart from the Git guard", PLACE,
  "                aspen_server::access::ip_key,\n",
  "                |ip| format!(\"place:{ip}\"),\n",
  PLACET + ["bad_operator_credentials_drain_the_git_failure_bucket"]),
 ("D24 a limited operator is answered 403", PLACEAPI,
  '            StatusCode::TOO_MANY_REQUESTS,\n            "rate-limited",\n            &format!("Operator credentials refused',
  '            StatusCode::FORBIDDEN,\n            "forbidden",\n            &format!("Operator credentials refused',
  PLACET + ["bad_operator_credentials_drain_the_git_failure_bucket"]),
 ("D25 IPv4-mapped clients share one /64 key", ACCESS,
  "    match ip.to_canonical() {",
  "    match ip {",
  ["-p", "aspen-server", "--lib", "anonymous_ipv6_callers_are_keyed_by_their_slash_64"]),
 ("D26 the h2 stream cap is not sent", LISTEN,
  "        .max_concurrent_streams(limits.h2_max_concurrent_streams);",
  "        ;",
  LIS + ["cleartext_http2_is_served"]),
 ("D27 shutdown drops requests in flight", LISTEN,
  "    graceful.shutdown().await;",
  "    drop(graceful);",
  LIS + ["shutdown_lets_requests_in_flight_finish_then_returns"]),
 ("D28 a push body never times out", LIB,
  "    next.run(request.map(|b| axum::body::Body::new(body::IdleBody::new(b, timeout))))",
  "    let _ = timeout;\n    next.run(request)",
  LIS + ["a_stalled_push_body_is_cut_off"]),
 # Issue #121: client addresses behind trusted proxies.
 ("D29 X-Forwarded-For is read from any peer", CLIENT,
  "        if !self.proxies.contains(peer) {",
  "        if false {",
  HTTP + ["a_client_cannot_choose_its_failure_bucket_through_x_forwarded_for"]),
 ("D30 an unreadable forwarded address is the unknown address", CLIENT,
  "                        Some(peer)\n",
  "                        None\n",
  HTTP + ["a_client_cannot_choose_its_failure_bucket_through_x_forwarded_for"]),
 ("D31 a header line that is not ASCII is dropped whole", CLIENT,
  "            for entry in line.as_bytes().rsplit(",
  "            for entry in line.to_str().unwrap_or_default().as_bytes().rsplit(",
  HTTP + ["a_client_cannot_choose_its_failure_bucket_through_x_forwarded_for"]),
 ("D32 a forwarded entry with a port is not an address", CLIENT,
  "    } else if let Ok(sa) = s.parse::<SocketAddr>() {",
  "    } else if let Ok(sa) = \"\".parse::<SocketAddr>() {",
  HTTP + ["a_client_cannot_choose_its_failure_bucket_through_x_forwarded_for"]),
 ("D33 empty X-Forwarded-For entries count as hops", CLIENT,
  "                if entry.is_empty() {\n                    continue;\n                }\n",
  "",
  ["-p", "aspen-server", "--lib", "entries_are_parsed_one_at_a_time_from_raw_bytes"]),
 ("D34 client-address fallbacks are not exported", LIB,
  "            .unwrap_or(&access::ClientAddr::Peer)\n            .render_metrics(&mut text);",
  "            .map(|_| ());",
  ["-p", "aspen-server", "--test", "observability", "client_address_fallbacks_are_exported_by_reason"]),
 ("D35 place's API trusts X-Forwarded-For from any peer", PLACEAPI,
  "    source.client_ip(peer.map(|p| p.ip()), headers)",
  "    ClientAddr::from_settings(source.hops(), Some(\"0.0.0.0/0, ::/0\"))\n        .ok()?\n        .client_ip(peer.map(|p| p.ip()), headers)",
  PLACET + ["session_limits_key_on_the_address_a_trusted_proxy_recorded"]),
 ("D36 an IPv4-mapped forwarded entry is returned as IPv6", CLIENT,
  "    Some(ip.to_canonical())",
  "    Some(ip)",
  SRVU + ["entries_are_parsed_one_at_a_time_from_raw_bytes"]),
 ("D37 an IPv4-mapped peer is not matched against IPv4 proxies", CLIENT,
  "        match (self.addr, ip.to_canonical()) {",
  "        match (self.addr, ip) {",
  SRVU + ["access::client_addr::tests::"]),
 ("D38 a bracketed IPv6 entry without a port is not an address", CLIENT,
  "        IpAddr::V6(inner.parse::<Ipv6Addr>().ok()?)",
  "        IpAddr::V6(inner.parse::<Ipv6Addr>().ok().filter(|_| false)?)",
  SRVU + ["entries_are_parsed_one_at_a_time_from_raw_bytes"]),
 ("D39 a trusted-proxy list with hops 0 is accepted", CLIENT,
  "            (None, Some(_)) => Err(ClientAddrError::ProxiesWithoutHops),",
  "            (None, Some(_)) => Ok(Self::Peer),",
  CLIU + ["numbers_and_switches_are_checked"]),
 ("D40 a trusted-proxy prefix with a sign is accepted", CLIENT,
  "                    Some(p) if p.is_empty() || !p.bytes().all(|b| b.is_ascii_digit()) => {",
  "                    Some(p) if p.is_empty() => {",
  SRVU + ["trusted_proxies_parse_and_match"]),
 ("D41 an empty ASPEN_TRUSTED_PROXIES is read as a list", CONFIG,
  'ClientAddr::from_settings(hops, var("ASPEN_TRUSTED_PROXIES").as_deref())',
  'ClientAddr::from_settings(hops, get("ASPEN_TRUSTED_PROXIES").as_deref())',
  CLIU + ["numbers_and_switches_are_checked"]),
 ("D42 an empty PLACE_TRUSTED_PROXIES is read as a list", PLACEMAIN,
  '        var("PLACE_TRUSTED_PROXIES").as_deref(),',
  '        std::env::var("PLACE_TRUSTED_PROXIES").ok().as_deref(),',
  ["-p", "aspen-place", "--test", "binary", "an_empty_trusted_proxy_list_is_unset"]),
 ("D19 ServeConfig's Debug shows the URL signing key", CONFIG,
  '        f.write_str("UrlSecret(..)")',
  '        write!(f, "{:?}", self.0)',
  CLIU + ["the_local_url_secret_needs_a_file_store_a_public_url_and_32_bytes"]),

 # Issue #198: a credential's token is taken before its check.
 ("D43 a credential is charged only once its check failed", LIMIT,
  "            l.try_take(address, 1.0, now)\n                .map_err(LimitError::AuthFailures)?;",
  "            l.check(address, 1.0, now)\n                .map_err(LimitError::AuthFailures)?;",
  HTTP + ["concurrent_bad_credentials_cannot_overshoot_the_failure_bucket"]),
 ("D44 a successful authentication keeps its charge", ACCESS,
  "            Ok(identity) => {\n                refund(charge);",
  "            Ok(identity) => {\n                drop(charge);",
  HTTP + ["concurrent_bad_credentials_cannot_overshoot_the_failure_bucket"]),
 ("D45 a store outage keeps its charge", ACCESS,
  "            Err(AuthError::Unavailable) => {\n                refund(charge);",
  "            Err(AuthError::Unavailable) => {\n                drop(charge);",
  HTTP + ["concurrent_bad_credentials_cannot_overshoot_the_failure_bucket"]),
 ("D46 a refund that fills a bucket keeps it in a full table", LIMIT,
  "            table.buckets.remove(key);",
  "            let _ = key;",
  AUTH + ["a_refund_is_capped_and_frees_a_full_bucket"]),
 # Issue #243: an authenticated SSH key's re-checks are charged too.
 ("D47 an SSH re-check bypasses the failure bucket", SSH,
  "    access.identify(credential, address).await.map(Arc::new)",
  "    let _ = address;\n    access\n        .auth()\n        .identify(credential, aspen_core::id::UnixMicros::now())\n        .await\n        .map(Arc::new)\n        .map_err(|e| match e {\n            AuthError::Unavailable => Refusal::Unavailable,\n            e => Refusal::Auth(e),\n        })",
  SSHL + ["failed_rechecks_of_a_revoked_key_drain_the_failure_bucket"]),
 ("D48 an SSH command's re-check is charged to no address", SSH,
  "identify_now(&self.shared.access, key, &self.address)",
  "identify_now(&self.shared.access, key, crate::access::UNKNOWN_ADDRESS)",
  SSHL + ["failed_rechecks_of_a_revoked_key_drain_the_failure_bucket"]),
 ("D49 a v2 command's re-check is charged to no address", EXEC,
  "identify_now(&shared.access, &credential, address)",
  "identify_now(&shared.access, &credential, crate::access::UNKNOWN_ADDRESS)",
  SSHL + ["failed_v2_rechecks_drain_the_failure_bucket"]),
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
