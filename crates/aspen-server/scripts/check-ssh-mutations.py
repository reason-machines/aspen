#!/usr/bin/env python3
"""Removes each check the SSH transport's tests (node C3) rely on, one at a
time, and shows that a test fails without it. Run from the repository root:

    python3 crates/aspen-server/scripts/check-ssh-mutations.py [S1 S2 ...]

Each mutation is one or more exact text replacements in one source file;
the file is restored afterwards. A mutation whose pattern no longer matches is reported,
so the list fails loudly when the code it targets changes. Mutations that
make the server wait for input the client never sends show up as a test
that times out (60 s per command) and fails."""
import subprocess, sys

SSH = 'crates/aspen-server/src/ssh/mod.rs'
EXEC = 'crates/aspen-server/src/ssh/exec.rs'
CMD = 'crates/aspen-server/src/ssh/command.rs'
KEYS = 'crates/aspen-server/src/ssh/keys.rs'
V0 = 'crates/aspen-server/src/upload_pack/v0.rs'
UP = 'crates/aspen-server/src/upload_pack/mod.rs'
ACCESS = 'crates/aspen-server/src/access/mod.rs'
PUSH = 'crates/aspen-server/src/receive/push.rs'
POLICY = 'crates/aspen-auth/src/policy.rs'
AUTH_SSH = 'crates/aspen-auth/src/ssh.rs'
CRED = 'crates/aspen-auth/src/credential.rs'

E2E = ["-p", "aspen-server", "--test", "ssh"]

muts = [
 ("S1 a command runs as the connection's identity, not one resolved now", SSH,
  "    access.identify(credential, address).await.map(Arc::new)",
  "    access.identify(credential, address).await.or_else(|_| Ok::<_, Refusal>(Identity::new(aspen_core::auth::Principal::Subject { id: \"stale\".into() }, aspen_auth::Via::Anonymous, aspen_auth::Grants::new([\"write:*\".parse().unwrap()])))).map(Arc::new)",
  E2E + ["a_revoked_key_is_refused_on_a_connection_it_opened_before"]),
 ("S2 pipelined input read before the pack is sent (A1b's C3 obligation)", V0,
  "    tokio::spawn(input.drain());",
  "    input.drain().await;",
  E2E + ["pipelined_haves_after_ready_are_left_unread"]),
 ("S3 the shallow section is not sent before the client's first round", V0,
  "    // A deepen client reads its shallow section before it sends haves.\n    conn::send(out, &buf).await?;\n    buf.clear();",
  "",
  E2E + ["shallow_clone_and_deepen_in_v0_and_v2"]),
 ("S4 a negotiation round is not answered until a later one", V0,
  "                    Answered::More(n) => {\n                        conn::send(out, &buf).await?;\n                        buf.clear();\n                        n\n                    }",
  "                    Answered::More(n) => n,",
  E2E + ["pipelined_haves_after_ready_are_left_unread"]),
 ("S5 v2 commands after the first are free", UP,
  "        if !std::mem::take(&mut first)",
  "        if false && !std::mem::take(&mut first)",
  E2E + ["rate_limits_count_every_v2_command"]),
 ("S6 an SSH push body is not metered against the push quota", EXEC,
  "SyncIoBridge::new(metered(read, meter))",
  "SyncIoBridge::new(metered(read, { let _ = meter; None }))",
  E2E + ["the_push_quota_applies_over_ssh"]),
 ("S7 shell syntax read as literal text", CMD,
  "            c if safe(c) => word.get_or_insert_with(String::new).push(c),",
  "            c if true || safe(c) => word.get_or_insert_with(String::new).push(c),",
  E2E + ["no_shell_and_no_other_commands"]),
 # Both checks: russh asks about the offer before it verifies a signature,
 # so the offer check alone already refuses an unknown key.
 ("S8 unregistered keys authenticate", SSH,
  ["            Some(c) if self.identify(&c).await.is_ok() => Auth::Accept,",
   "        if self.identify(&credential).await.is_err() {\n            return Ok(Auth::reject());\n        }"],
  ["            Some(_) => Auth::Accept,",
   ""],
  E2E + ["only_registered_live_keys_authenticate"]),
 ("S9 access not decided by repository name", ACCESS,
  "            .authorize(&route.repo, need)\n            .map_err(Refusal::Auth)?;",
  "            .authorize(&route.repo, need)\n            .ok();",
  E2E + ["each_key_reaches_what_it_was_registered_for"]),
 ("S10 the branch scope of an SSH path is dropped", EXEC,
  "GitRoute::from_repo_path(&command.path, endpoint)",
  "GitRoute::from_repo_path(command.path.split(\"/-/\").next().unwrap_or_default(), endpoint)",
  E2E + ["branch_scoped_urls_over_ssh"]),
 ("S11 smart-HTTP framing sent over SSH", EXEC,
  "svc.advertise_refs(&authorized.repo, scope, requested)",
  "svc.advertise(&authorized.repo, scope, requested)",
  E2E + ["clone_fetch_and_push_in_every_protocol_version"]),
 ("S12 revoked and expired keys identified", POLICY,
  "        if record.fingerprint() != fingerprint\n            || !record.is_live(now)",
  "        if record.fingerprint() != fingerprint",
  E2E + ["only_registered_live_keys_authenticate"]),
 ("S13 a stored record trusted for another key", POLICY,
  "        if record.fingerprint() != fingerprint\n            || ",
  "        if false\n            || ",
  ["-p", "aspen-auth", "--test", "identify", "the_record_must_be_for_the_presented_key"]),
 ("S14 a key row trusted when its key does not hash to its fingerprint", KEYS,
  "    if key.fingerprint().as_bytes() != &row.fingerprint {",
  "    if false && key.fingerprint().as_bytes() != &row.fingerprint {",
  ["-p", "aspen-server", "--lib", "a_row_that_does_not_match_its_key_is_refused"]),
 ("S15 unknown key types read as Ed25519", AUTH_SSH,
  "        let algorithm = SshKeyAlgorithm::from_name(name)\n            .ok_or_else(|| SshKeyError::Unsupported(printable(name)))?;",
  "        let algorithm = SshKeyAlgorithm::from_name(name).unwrap_or(SshKeyAlgorithm::Ed25519);",
  ["-p", "aspen-auth", "--lib", "rsa_and_other_types_are_refused"]),
 ("S16 per-ref permission ignored over SSH", PUSH,
  "        if !self.access.may_update(&command.name) {",
  "        if false && !self.access.may_update(&command.name) {",
  E2E + ["each_key_reaches_what_it_was_registered_for"]),
 ("S17 GIT_PROTOCOL ignored (every SSH client gets v0)", SSH,
  "            Some(opened) if name == \"GIT_PROTOCOL\"",
  "            Some(opened) if name == \"NOT_GIT_PROTOCOL\"",
  E2E + ["rate_limits_count_every_v2_command"]),
 ("S18 later v2 commands in a session not identified and authorized again", EXEC,
  "                    let identity = identify_now(&shared.access, &credential, address)\n                        .await\n                        .map_err(|r| r.message())?;\n                    identity\n                        .authorize(repo, Need::Read)\n                        .map_err(|e| Refusal::Auth(e).message())?;",
  "                    if false {\n                    let identity = identify_now(&shared.access, &credential, address)\n                        .await\n                        .map_err(|r| r.message())?;\n                    identity\n                        .authorize(repo, Need::Read)\n                        .map_err(|e| Refusal::Auth(e).message())?;\n                    }",
  E2E + ["a_revoked_key_is_refused_by_the_next_command_of_a_running_v2_session"]),
 ("S19 a v2 session refused mid-stream exits 0", EXEC,
  ".map(|end| end == upload_pack::SessionEnd::Done)",
  ".map(|_| true)",
  E2E + ["a_revoked_key_is_refused_by_the_next_command_of_a_running_v2_session"]),
 ("S20 no cap on session channels per connection", SSH,
  "Semaphore::new(self.shared.max_sessions)",
  "Semaphore::new(Semaphore::MAX_PERMITS)",
  E2E + ["sessions_per_connection_are_capped"]),
 # russh 0.65.1 drops a channel open that arrives before authentication
 # (src/server/encrypted.rs), so removing Aspen's own check alone changes
 # nothing on the wire. The check is there for an authentication that
 # succeeds without a verified key; this mutation adds one (`none`
 # accepted) and removes the check. With `none` accepted and the check
 # kept, the test passes: the check alone keeps the channel closed.
 ("S21 a channel opens without a verified key", SSH,
  ["methods: MethodSet::from(&[MethodKind::PublicKey][..]),",
   "    async fn auth_publickey_offered(",
   "            Ok(p) if self.key.is_some() => p,"],
  ["methods: MethodSet::from(&[MethodKind::PublicKey, MethodKind::None][..]),",
   "    async fn auth_none(&mut self, _user: &str) -> Result<Auth, Self::Error> {\n        Ok(Auth::Accept)\n    }\n\n    async fn auth_publickey_offered(",
   "            Ok(p) => p,"],
  E2E + ["no_channel_opens_without_a_verified_key"]),
 ("S22 environment variables other than GIT_PROTOCOL accepted", SSH,
  "Some(opened) if name == \"GIT_PROTOCOL\" && value.len() <= MAX_GIT_PROTOCOL_LEN",
  "Some(opened) if value.len() <= MAX_GIT_PROTOCOL_LEN",
  E2E + ["only_git_protocol_is_accepted_from_the_environment"]),
 ("S23 an HTTP header read as an SSH key (unit)", CRED,
  "    } else {\n        Err(AuthError::Unauthenticated)\n    }\n}",
  "    } else {\n        Ok(Credential::SshKey { fingerprint: rest.to_owned() })\n    }\n}",
  ["-p", "aspen-auth", "--lib", "no_header_is_an_ssh_key_credential"]),
 ("S24 an HTTP header read as an SSH key (guard, registered key)", CRED,
  "    } else {\n        Err(AuthError::Unauthenticated)\n    }\n}",
  "    } else {\n        Ok(Credential::SshKey { fingerprint: rest.to_owned() })\n    }\n}",
  ["-p", "aspen-server", "--test", "access", "an_ssh_key_fingerprint_is_not_an_http_credential"]),
]

only = sys.argv[1:]
res = []
for name, f, old, new, args in muts:
    if only and not any(name.startswith(o + " ") for o in only):
        continue
    src = open(f).read()
    olds, news = (old, new) if isinstance(old, list) else ([old], [new])
    missing = [o for o in olds if src.count(o) != 1]
    if missing:
        res.append((name, "PATTERN NOT FOUND: %r" % missing[0][:60]))
        print(res[-1], flush=True)
        continue
    mutated = src
    for o, n in zip(olds, news):
        mutated = mutated.replace(o, n)
    open(f, 'w').write(mutated)
    try:
        r = subprocess.run(["cargo", "test", "--locked"] + args[:-1] + ["--", args[-1]],
                           capture_output=True, text=True, timeout=1500)
        out = r.stdout + r.stderr
        if "error[" in out or "could not compile" in out:
            verdict = "COMPILE ERROR"
        elif r.returncode != 0 and "FAILED" in out:
            verdict = "test FAILS (good)"
        elif "running 0 tests" in out and "1 passed" not in out:
            verdict = "NO TEST RAN"
        else:
            verdict = "test PASSES (bad)"
    finally:
        open(f, 'w').write(src)
    res.append((name, verdict))
    print(res[-1], flush=True)

sys.exit(0 if all(v == 'test FAILS (good)' for _, v in res) else 1)
