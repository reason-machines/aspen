"""Spike S3 test server: protocol v2 `packfile-uris` plus a fault-injecting blob store.

This is throwaway spike code, not Aspen. It exists to observe how stock git
clients behave when a server answers `fetch` with packfile URIs, and what
happens when the URI download is interrupted, refused or wrong.

One HTTP server plays two roles on one port:

* A smart-HTTP Git endpoint at /repo.git. `ls-refs` and every protocol v0
  request are passed through to the real `git upload-pack` over a backing bare
  repository. A protocol v2 `fetch` that carries a `packfile-uris` argument is
  answered by this file: it acknowledges, lists the URIs from the scenario,
  and sends an inline pack (empty by default, the way Pando does). A v2
  `fetch` without `packfile-uris` is passed through, so it gets an ordinary
  pack.

* A blob store at /blobs/<name>.pack that serves pack files from the scenario
  directory, honours Range requests, and can misbehave on purpose, one
  behaviour per attempt (see `Blob behaviours` below).

The scenario is a JSON file that the test runner rewrites between runs; it is
re-read on every request. Every request is appended to a JSON-lines log so
the runner can assert what the client actually sent (Range headers,
Authorization headers, the `fetch` arguments, and so on).

Blob behaviours (strings in a per-blob list, consumed one per request; the
last one repeats):

  ok            200, or 206 for a satisfiable Range, or 416 when the Range
                starts at or past the end (what S3 and GCS do)
  norange       200 with the whole body, ignoring any Range header
  truncate:F    like ok, but close the connection after F (0..1) of the
                *whole file* has been sent counting from byte 0
  stall:F       like truncate:F, but hang instead of closing (the runner
                kills the client, like Ctrl-C)
  status:N      reply with HTTP status N and a short body
  retry-after:N reply 503 with Retry-After: N
  eof-empty-200 like ok, but a Range starting at or past the end gets 200
                with an empty body instead of 416
  eof-empty-206 like ok, but a Range starting at or past the end gets 206
                with an empty body and Content-Range: bytes */<size>
  redirect      302 to /blobs-direct/<name>.pack, which serves it as `ok`
  auth          401 unless Basic credentials are present, then `ok`
"""

from __future__ import annotations

import base64
import gzip
import hashlib
import json
import os
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

AGENT = "aspen-s3-spike/0"


# --------------------------------------------------------------------------
# pkt-line helpers


def pkt(data: bytes | str) -> bytes:
    if isinstance(data, str):
        data = data.encode()
    return b"%04x" % (len(data) + 4) + data


FLUSH = b"0000"
DELIM = b"0001"


def parse_pkts(body: bytes) -> list[bytes | None]:
    """Return payloads; None stands for flush/delim/response-end packets."""
    out: list[bytes | None] = []
    i = 0
    while i + 4 <= len(body):
        n = int(body[i : i + 4], 16)
        if n < 4:
            out.append(None)
            i += 4
            continue
        out.append(body[i + 4 : i + n])
        i += n
    return out


def empty_pack() -> bytes:
    head = b"PACK" + (2).to_bytes(4, "big") + (0).to_bytes(4, "big")
    return head + hashlib.sha1(head).digest()


def sideband(data: bytes, band: int = 1, chunk: int = 65515) -> bytes:
    out = bytearray()
    for i in range(0, len(data), chunk):
        out += pkt(bytes([band]) + data[i : i + chunk])
    return bytes(out)


# --------------------------------------------------------------------------
# state shared with the runner


class State:
    def __init__(self, root: Path, backing_repo: Path):
        self.root = root
        self.backing = backing_repo
        self.scenario_path = root / "scenario.json"
        self.log_path = root / "requests.jsonl"
        self.lock = threading.Lock()
        self._attempts: dict[str, int] = {}
        self._scenario_mtime = 0.0

    def scenario(self) -> dict:
        try:
            st = self.scenario_path.stat()
        except FileNotFoundError:
            return {}
        with self.lock:
            if st.st_mtime_ns != self._scenario_mtime:
                self._scenario_mtime = st.st_mtime_ns
                self._attempts.clear()
        return json.loads(self.scenario_path.read_text())

    def next_behaviour(self, blob: str, behaviours: list[str]) -> str:
        with self.lock:
            n = self._attempts.get(blob, 0)
            self._attempts[blob] = n + 1
        return behaviours[min(n, len(behaviours) - 1)]

    def log(self, **entry) -> None:
        entry["t"] = round(time.time(), 3)
        with self.lock, open(self.log_path, "a") as f:
            f.write(json.dumps(entry) + "\n")


STATE: State  # set in serve()


# --------------------------------------------------------------------------
# request handler


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = AGENT

    def log_message(self, fmt, *args):  # keep stderr quiet
        pass

    # ---- plumbing

    def read_body(self) -> bytes:
        if self.headers.get("Transfer-Encoding", "").lower() == "chunked":
            data = bytearray()
            while True:
                size = int(self.rfile.readline().split(b";")[0], 16)
                if size == 0:
                    self.rfile.readline()
                    break
                data += self.rfile.read(size)
                self.rfile.readline()
            body = bytes(data)
        else:
            body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        if self.headers.get("Content-Encoding", "") in ("gzip", "x-gzip"):
            body = gzip.decompress(body)
        return body

    def reply(self, status: int, body: bytes, ctype: str, extra: dict | None = None):
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-cache")
        for k, v in (extra or {}).items():
            self.send_header(k, v)
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def has_basic_auth(self) -> bool:
        return self.headers.get("Authorization", "").startswith("Basic ")

    def require_repo_auth(self, scenario: dict) -> bool:
        """Return True if the request may proceed."""
        if not scenario.get("repo_auth"):
            return True
        if self.has_basic_auth():
            return True
        self.reply(401, b"auth required\n", "text/plain",
                   {"WWW-Authenticate": 'Basic realm="aspen-spike-repo"'})
        return False

    def git_protocol(self) -> str:
        return self.headers.get("Git-Protocol", "")

    def upload_pack(self, body: bytes, advertise: bool = False) -> bytes:
        env = dict(os.environ, GIT_PROTOCOL=self.git_protocol())
        cmd = ["git", "upload-pack", "--stateless-rpc"]
        if advertise:
            cmd.append("--advertise-refs")
        cmd.append(str(STATE.backing))
        return subprocess.run(cmd, input=body, stdout=subprocess.PIPE,
                              env=env, check=True).stdout

    # ---- routing

    def do_GET(self):
        scenario = STATE.scenario()
        path, _, query = self.path.partition("?")
        if path == "/repo.git/info/refs":
            return self.info_refs(scenario, query)
        if path.startswith("/blobs/") or path.startswith("/blobs-direct/"):
            return self.blob(scenario, path)
        self.reply(404, b"not found\n", "text/plain")

    do_HEAD = do_GET

    def do_POST(self):
        scenario = STATE.scenario()
        if self.path.split("?")[0] == "/repo.git/git-upload-pack":
            return self.post_upload_pack(scenario)
        self.reply(404, b"not found\n", "text/plain")

    # ---- Git endpoint

    def info_refs(self, scenario: dict, query: str):
        STATE.log(kind="info_refs", git_protocol=self.git_protocol(),
                  auth=self.has_basic_auth(), host=self.headers.get("Host"))
        if not self.require_repo_auth(scenario):
            return
        if "service=git-upload-pack" not in query:
            return self.reply(403, b"dumb http not supported\n", "text/plain")
        ctype = "application/x-git-upload-pack-advertisement"
        if "version=2" not in self.git_protocol():
            body = pkt("# service=git-upload-pack\n") + FLUSH
            body += self.upload_pack(b"", advertise=True)
            return self.reply(200, body, ctype)
        fetch_features = ["shallow"]
        if scenario.get("advertise_packfile_uris", True):
            fetch_features.append("packfile-uris")
        if scenario.get("advertise_no_ref_delta"):
            fetch_features.append("no-ref-delta")
        lines = [
            "version 2\n",
            f"agent={AGENT}\n",
            "ls-refs=unborn\n",
            "fetch=" + " ".join(fetch_features) + "\n",
            "server-option\n",
            "object-format=sha1\n",
        ]
        body = b"".join(pkt(l) for l in lines) + FLUSH
        self.reply(200, body, ctype)

    def post_upload_pack(self, scenario: dict):
        if not self.require_repo_auth(scenario):
            return
        body = self.read_body()
        ctype = "application/x-git-upload-pack-result"
        pkts = parse_pkts(body)
        lines = [p.decode(errors="replace").rstrip("\n") for p in pkts if p is not None]
        command = next((l[8:] for l in lines if l.startswith("command=")), None)
        args = self.fetch_args(pkts) if command == "fetch" else []
        STATE.log(kind="upload_pack", git_protocol=self.git_protocol(),
                  command=command or "v0", args=args, auth=self.has_basic_auth())

        wants_uris = any(a.startswith("packfile-uris ") for a in args)
        if command != "fetch" or not wants_uris:
            return self.reply(200, self.upload_pack(body), ctype)
        self.reply(200, self.uri_fetch_response(scenario, args), ctype)

    @staticmethod
    def fetch_args(pkts: list[bytes | None]) -> list[str]:
        """Arguments after the delimiter of a v2 fetch request."""
        out, seen_delim = [], False
        for p in pkts:
            if p is None:
                if seen_delim:
                    break
                seen_delim = True
                continue
            if seen_delim:
                out.append(p.decode(errors="replace").rstrip("\n"))
        return out

    def uri_fetch_response(self, scenario: dict, args: list[str]) -> bytes:
        haves = [a[5:] for a in args if a.startswith("have ")]
        done = "done" in args
        out = bytearray()
        if not done:
            # Pando-style: never negotiate further; declare "ready" at once.
            out += pkt("acknowledgments\n")
            common = [h for h in haves if self.backing_has(h)]
            if common:
                for h in common:
                    out += pkt(f"ACK {h}\n")
            else:
                out += pkt("NAK\n")
            out += pkt("ready\n")
            out += DELIM
        uris = scenario.get("uris", [])
        if uris:
            out += pkt("packfile-uris\n")
            for u in uris:
                out += pkt(f"{u['hash']} {u['url']}\n")
            out += DELIM
        inline = scenario.get("inline", "empty")
        if inline == "none":
            # Deliberately malformed: no packfile section at all.
            out += FLUSH
            return bytes(out)
        data = empty_pack() if inline == "empty" else (STATE.root / inline).read_bytes()
        out += pkt("packfile\n")
        out += sideband(data)
        out += FLUSH
        return bytes(out)

    @staticmethod
    def backing_has(oid: str) -> bool:
        return subprocess.run(
            ["git", "-C", str(STATE.backing), "cat-file", "-e", oid],
            stderr=subprocess.DEVNULL).returncode == 0

    # ---- blob store

    def blob(self, scenario: dict, path: str):
        name = path.rsplit("/", 1)[-1]
        direct = path.startswith("/blobs-direct/")
        behaviours = ["ok"] if direct else scenario.get("blobs", {}).get(name, ["ok"])
        behaviour = STATE.next_behaviour(path, behaviours)
        rng = self.headers.get("Range")
        entry = dict(kind="blob", path=path, behaviour=behaviour, range=rng,
                     auth=self.has_basic_auth(), host=self.headers.get("Host"),
                     user_agent=self.headers.get("User-Agent"))

        file = STATE.root / "packs" / name
        if not file.exists():
            STATE.log(status=404, **entry)
            return self.reply(404, b"no such blob\n", "text/plain")
        data = file.read_bytes()
        size = len(data)

        kind, _, arg = behaviour.partition(":")
        if kind == "status":
            STATE.log(status=int(arg), **entry)
            return self.reply(int(arg), b"injected failure\n", "text/plain")
        if kind == "retry-after":
            STATE.log(status=503, **entry)
            return self.reply(503, b"slow down\n", "text/plain", {"Retry-After": arg})
        if kind == "redirect":
            STATE.log(status=302, **entry)
            return self.reply(302, b"", "text/plain",
                              {"Location": "/blobs-direct/" + name})
        if kind == "auth" and not self.has_basic_auth():
            STATE.log(status=401, **entry)
            return self.reply(401, b"auth required\n", "text/plain",
                              {"WWW-Authenticate": 'Basic realm="aspen-spike-blobs"'})

        start = 0
        honour_range = kind != "norange"
        if rng and honour_range and rng.startswith("bytes="):
            start = int(rng[6:].split("-")[0] or 0)
            if start >= size and kind == "eof-empty-200":
                STATE.log(status=200, sent=0, **entry)
                return self.reply(200, b"", "application/x-git-packed-objects")
            if start >= size and kind == "eof-empty-206":
                STATE.log(status=206, sent=0, **entry)
                return self.reply(206, b"", "application/x-git-packed-objects",
                                  {"Content-Range": f"bytes */{size}"})
            if start >= size:
                STATE.log(status=416, sent=0, **entry)
                return self.reply(416, b"", "text/plain",
                                  {"Content-Range": f"bytes */{size}"})
        status = 206 if start else 200
        body = data[start:]
        limit = len(body)
        if kind in ("truncate", "stall"):
            limit = max(0, int(size * float(arg)) - start)

        self.send_response(status)
        self.send_header("Content-Type", "application/x-git-packed-objects")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Accept-Ranges", "bytes")
        if status == 206:
            self.send_header("Content-Range", f"bytes {start}-{size - 1}/{size}")
        self.end_headers()
        STATE.log(status=status, sent=min(limit, len(body)), start=start, size=size, **entry)
        if self.command == "HEAD":
            return
        try:
            self.wfile.write(body[:limit])
            self.wfile.flush()
            if kind == "stall":
                time.sleep(60)
        except (BrokenPipeError, ConnectionResetError):
            pass
        if limit < len(body):
            self.close_connection = True


def serve(root: Path, backing: Path, port: int) -> ThreadingHTTPServer:
    global STATE
    STATE = State(root, backing)
    httpd = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    httpd.daemon_threads = True
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    return httpd


if __name__ == "__main__":
    root, backing, port = Path(sys.argv[1]), Path(sys.argv[2]), int(sys.argv[3])
    serve(root, backing, port)
    print(f"serving on :{port}", flush=True)
    while True:
        time.sleep(3600)
