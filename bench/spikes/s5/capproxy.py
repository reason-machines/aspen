#!/usr/bin/env python3
"""Capability-rewriting smart-HTTP Git server for spike S5.

Serves every bare repository under --root through stock `git http-backend`,
but lets the URL choose how the server treats shallow and filter requests:

    http://127.0.0.1:PORT/<mode>/<repo>.git

Modes:

  stock      Pass-through. Advertises shallow and filter (uploadpack.allowFilter).
  strip      Advertise neither shallow nor filter. Records a violation if the
             client sends shallow/deepen/filter lines anyway.
  nofilter   Advertise shallow but not filter. Shallow is served exactly by stock
             upload-pack (the "materializing" path: a minimal per-request pack).
  refuse     Advertise shallow but not filter, then answer any request that
             carries shallow/deepen lines with an ERR packet. This is how a
             server can refuse shallow with its own message.
  overfetch  Advertise shallow but not filter. Answer a shallow request with the
             correct shallow-info section (computed by stock git) followed by a
             pack holding the *full* history of the wants, as if the client had
             nothing. This models serving shallow clients from stored packs
             without building a per-request pack.
  planner    Like overfetch for requests that deepen (clones, --deepen,
             --unshallow). For a fetch from an already-shallow client that
             only sends "shallow <oid>" lines plus haves, drop the shallow
             lines and serve want-minus-have as if the client had full
             history, excluding every object reachable from the haves (via a
             reachability bitmap). This models a planner that maps haves to
             ref versions and ignores the client's shallow boundary.
  tipsonly   Like nofilter, but upload-pack accepts only wants that name an
             advertised ref tip (no allowAnySHA1InWant /
             allowReachableSHA1InWant). Shows which requests need the server
             to resolve arbitrary object IDs.

Every request is appended as one JSON line to --log, with the pkt-lines the
client sent and the capabilities the server advertised, so the transcripts
show exactly what each client did.

Only the Python standard library is used.
"""

import argparse
import gzip
import json
import os
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit

MODES = ("stock", "strip", "nofilter", "refuse", "overfetch", "planner", "tipsonly")

V0_SHALLOW_CAPS = {"shallow", "deepen-since", "deepen-not", "deepen-relative"}
V0_FILTER_CAPS = {"filter"}
SHALLOW_LINE_PREFIXES = (b"shallow ", b"deepen ", b"deepen-since ", b"deepen-not ", b"deepen-relative")
FILTER_LINE_PREFIXES = (b"filter ",)

REFUSE_MESSAGE = (
    "aspen: shallow fetches are not supported by this server; "
    "clone without --depth/--shallow-since/--shallow-exclude, "
    "or run 'git fetch --unshallow' against another remote first"
)

LOG_LOCK = threading.Lock()


# ---------------------------------------------------------------- pkt-line


FLUSH, DELIM, RESPONSE_END = "flush", "delim", "response-end"


def parse_pkts(data):
    """Split a pkt-line stream into a list of bytes payloads / special markers.

    Returns (pkts, rest) where rest is any trailing bytes that are not pkt-lines
    (for example a raw pack after a v0 NAK when side-band is off)."""
    pkts = []
    i = 0
    while i + 4 <= len(data):
        try:
            n = int(data[i : i + 4], 16)
        except ValueError:
            break
        if n == 0:
            pkts.append(FLUSH)
            i += 4
        elif n == 1:
            pkts.append(DELIM)
            i += 4
        elif n == 2:
            pkts.append(RESPONSE_END)
            i += 4
        elif n < 4 or i + n > len(data):
            break
        else:
            pkts.append(data[i + 4 : i + n])
            i += n
    return pkts, data[i:]


def encode_pkts(pkts):
    out = bytearray()
    for p in pkts:
        if p == FLUSH:
            out += b"0000"
        elif p == DELIM:
            out += b"0001"
        elif p == RESPONSE_END:
            out += b"0002"
        else:
            out += b"%04x" % (len(p) + 4) + p
    return bytes(out)


def pkt_text(p, limit=200):
    if isinstance(p, str):
        return "<%s>" % p
    if p[:1] in (b"\x01", b"\x02", b"\x03"):
        return "<sideband %d, %d bytes>" % (p[0], len(p) - 1)
    if p.startswith(b"PACK"):
        return "<raw pack, %d bytes>" % len(p)
    s = p.decode("utf-8", "replace").rstrip("\n")
    return s if len(s) <= limit else s[:limit] + "..."


# ----------------------------------------------------- capability rewriting


def strip_caps(pkts, drop_shallow, drop_filter):
    """Remove shallow/filter capabilities from a v0 or v2 advertisement."""
    v0_drop = set()
    v2_drop = set()
    if drop_shallow:
        v0_drop |= V0_SHALLOW_CAPS
        v2_drop.add("shallow")
    if drop_filter:
        v0_drop |= V0_FILTER_CAPS
        v2_drop.add("filter")
    out = []
    for p in pkts:
        if isinstance(p, bytes) and b"\0" in p:
            # v0: "<oid> <ref>\0cap cap cap\n"
            head, caps = p.split(b"\0", 1)
            nl = caps.endswith(b"\n")
            toks = [t for t in caps.strip().split(b" ") if t.decode() not in v0_drop]
            p = head + b"\0" + b" ".join(toks) + (b"\n" if nl else b"")
        elif isinstance(p, bytes) and p.startswith(b"fetch"):
            # v2: "fetch=shallow wait-for-done filter\n" (or bare "fetch\n")
            line = p.decode().rstrip("\n")
            if "=" in line:
                _, feats = line.split("=", 1)
                kept = [f for f in feats.split(" ") if f and f not in v2_drop]
                line = "fetch=" + " ".join(kept) if kept else "fetch"
            p = (line + "\n").encode()
        out.append(p)
    return out


def advertisement_summary(pkts):
    """Keep capability lines, drop the (possibly long) list of refs."""
    out = []
    for p in pkts:
        if isinstance(p, bytes) and b"\0" in p:
            out.append("caps: " + p.split(b"\0", 1)[1].decode("utf-8", "replace").strip())
        elif isinstance(p, bytes) and b" refs/" in p:
            continue
        else:
            out.append(pkt_text(p))
    return out


# ------------------------------------------------------------ http-backend


def run_backend(root, path_info, method, query, protocol, content_type, body, extra_config):
    env = {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "GIT_PROJECT_ROOT": root,
        "GIT_HTTP_EXPORT_ALL": "1",
        "PATH_INFO": path_info,
        "REQUEST_METHOD": method,
        "QUERY_STRING": query,
        "CONTENT_TYPE": content_type or "",
        "CONTENT_LENGTH": str(len(body)),
        "REMOTE_ADDR": "127.0.0.1",
    }
    if protocol:
        env["GIT_PROTOCOL"] = protocol
    if os.environ.get("CAPPROXY_GIT_EXEC_PATH"):
        env["GIT_EXEC_PATH"] = os.environ["CAPPROXY_GIT_EXEC_PATH"]
    cfg = list(extra_config.items())
    env["GIT_CONFIG_COUNT"] = str(len(cfg))
    for i, (k, v) in enumerate(cfg):
        env["GIT_CONFIG_KEY_%d" % i] = k
        env["GIT_CONFIG_VALUE_%d" % i] = v
    backend = os.environ.get("CAPPROXY_HTTP_BACKEND", "git")
    cmd = [backend, "http-backend"] if backend == "git" else [backend]
    proc = subprocess.run(cmd, input=body, env=env, capture_output=True)
    raw = proc.stdout
    sep = raw.find(b"\r\n\r\n")
    sep_len = 4
    if sep < 0:
        sep = raw.find(b"\n\n")
        sep_len = 2
    head, payload = raw[:sep], raw[sep + sep_len :]
    status = 200
    headers = []
    for line in head.decode("latin-1").splitlines():
        if not line.strip():
            continue
        k, v = line.split(":", 1)
        if k.lower() == "status":
            status = int(v.strip().split()[0])
        else:
            headers.append((k.strip(), v.strip()))
    return status, headers, payload, proc.stderr.decode("utf-8", "replace")


def mode_config(mode):
    any_want = "false" if mode == "tipsonly" else "true"
    cfg = {"uploadpack.allowAnySHA1InWant": any_want, "uploadpack.allowReachableSHA1InWant": any_want,
           "uploadpack.allowTipSHA1InWant": any_want}
    cfg["uploadpack.allowFilter"] = "true" if mode == "stock" else "false"
    return cfg


# ------------------------------------------------------------- the server


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.0"
    server_version = "s5-capproxy"

    def log_message(self, fmt, *args):  # quiet stderr
        pass

    def _record(self, entry):
        with LOG_LOCK:
            with open(self.server.log_path, "a") as f:
                f.write(json.dumps(entry) + "\n")

    def _send(self, status, headers, payload):
        self.send_response(status)
        sent = set()
        for k, v in headers:
            if k.lower() in ("content-length",):
                continue
            self.send_header(k, v)
            sent.add(k.lower())
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _split(self):
        url = urlsplit(self.path)
        parts = url.path.lstrip("/").split("/", 1)
        if len(parts) != 2 or parts[0] not in MODES:
            return None, None, None
        return parts[0], "/" + parts[1], url.query

    def do_GET(self):
        mode, path_info, query = self._split()
        if mode is None:
            return self._send(404, [("Content-Type", "text/plain")], b"unknown mode\n")
        protocol = self.headers.get("Git-Protocol", "")
        status, headers, payload, err = run_backend(
            self.server.root, path_info, "GET", query, protocol, None, b"", mode_config(mode)
        )
        entry = {"mode": mode, "method": "GET", "path": path_info, "query": query,
                 "git_protocol": protocol, "user_agent": self.headers.get("User-Agent", "")}
        if "service=git-upload-pack" in query and status == 200:
            pkts, rest = parse_pkts(payload)
            if mode == "strip":
                pkts = strip_caps(pkts, drop_shallow=True, drop_filter=True)
            elif mode in ("nofilter", "refuse", "overfetch", "planner", "tipsonly"):
                pkts = strip_caps(pkts, drop_shallow=False, drop_filter=True)
            payload = encode_pkts(pkts) + rest
            entry["advertisement"] = advertisement_summary(pkts)[:40]
        if err.strip():
            entry["backend_stderr"] = err.strip()[:2000]
        self._record(entry)
        self._send(status, headers, payload)

    def do_POST(self):
        mode, path_info, query = self._split()
        if mode is None:
            return self._send(404, [("Content-Type", "text/plain")], b"unknown mode\n")
        n = int(self.headers.get("Content-Length", "0") or 0)
        if n:
            body = self.rfile.read(n)
        elif self.headers.get("Transfer-Encoding", "").lower() == "chunked":
            body = bytearray()
            while True:
                size = int(self.rfile.readline().strip().split(b";")[0], 16)
                if size == 0:
                    self.rfile.readline()
                    break
                body += self.rfile.read(size)
                self.rfile.readline()
            body = bytes(body)
        else:
            body = b""
        if self.headers.get("Content-Encoding", "") in ("gzip", "x-gzip"):
            body = gzip.decompress(body)
        protocol = self.headers.get("Git-Protocol", "")
        ctype = self.headers.get("Content-Type", "")
        pkts, _ = parse_pkts(body)
        lines = [p for p in pkts if isinstance(p, bytes)]
        sent_shallow = [pkt_text(p) for p in lines if p.startswith(SHALLOW_LINE_PREFIXES)]
        sent_filter = [pkt_text(p) for p in lines if p.startswith(FILTER_LINE_PREFIXES)]
        entry = {"mode": mode, "method": "POST", "path": path_info, "git_protocol": protocol,
                 "user_agent": self.headers.get("User-Agent", ""),
                 "request": [pkt_text(p) for p in pkts][:80]}
        violations = []
        if mode == "strip" and sent_shallow:
            violations.append("client sent shallow/deepen lines although shallow was not advertised")
        if mode != "stock" and sent_filter:
            violations.append("client sent a filter line although filter was not advertised")
        if violations:
            entry["violations"] = violations
        is_upload = path_info.endswith("/git-upload-pack")
        is_v2 = "version=2" in protocol

        if is_upload and mode == "refuse" and sent_shallow:
            payload = encode_pkts([b"ERR " + REFUSE_MESSAGE.encode() + b"\n"])
            entry["response"] = ["ERR " + REFUSE_MESSAGE]
            self._record(entry)
            return self._send(200, [("Content-Type", "application/x-git-upload-pack-result"),
                                    ("Cache-Control", "no-cache")], payload)

        sent_deepen = any(p.startswith((b"deepen ", b"deepen-since ", b"deepen-not ")) for p in lines)
        if is_upload and mode == "planner" and sent_shallow and not sent_deepen and is_v2:
            body = encode_pkts([p for p in pkts if not (isinstance(p, bytes) and p.startswith(b"shallow "))])
            # Exclude everything reachable from the haves, as a planner that
            # skips whole ref versions does. Stock pack-objects only does that
            # with a reachability bitmap; without one it excludes just the
            # boundary commits' trees.
            repo = os.path.join(self.server.root, path_info.lstrip("/").rsplit("/", 1)[0])
            subprocess.run(["git", "-C", repo, "repack", "-adbq"], check=True)
            entry["planner"] = "dropped client shallow lines; served want minus everything reachable from haves"
        elif is_upload and mode in ("overfetch", "planner") and sent_shallow and is_v2:
            payload, note = self._overfetch_v2(path_info, protocol, ctype, pkts)
            entry["overfetch"] = note
            self._record(entry)
            return self._send(200, [("Content-Type", "application/x-git-upload-pack-result"),
                                    ("Cache-Control", "no-cache")], payload)

        status, headers, payload, err = run_backend(
            self.server.root, path_info, "POST", query, protocol, ctype, body, mode_config(mode)
        )
        rpkts, _ = parse_pkts(payload)
        entry["response"] = [pkt_text(p) for p in rpkts if not (isinstance(p, bytes) and p[:1] in (b"\x01", b"\x02"))][:40]
        if err.strip():
            entry["backend_stderr"] = err.strip()[:2000]
        self._record(entry)
        self._send(status, headers, payload)

    def _overfetch_v2(self, path_info, protocol, ctype, pkts):
        """shallow-info from the real shallow request + a full-history pack."""
        # Request 1: the client's own request, forced to completion with "done",
        # so stock upload-pack computes the exact shallow boundary.
        req1 = [p for p in pkts if p not in (FLUSH,)]
        if b"done\n" not in req1:
            req1.append(b"done\n")
        req1.append(FLUSH)
        _, _, out1, _ = run_backend(self.server.root, path_info, "POST", "", protocol, ctype,
                                    encode_pkts(req1), mode_config("overfetch"))
        r1, _ = parse_pkts(out1)
        shallow_info = []
        if b"shallow-info\n" in r1:
            i = r1.index(b"shallow-info\n")
            j = i + 1
            while j < len(r1) and r1[j] != DELIM:
                j += 1
            shallow_info = r1[i:j]
        # Request 2: same wants and capabilities, but no haves, no shallow lines,
        # no deepen: the full closure of the wants, like stored packs would give.
        keep = []
        for p in pkts:
            if p in (FLUSH,):
                continue
            if isinstance(p, bytes) and (p.startswith(SHALLOW_LINE_PREFIXES) or p.startswith(b"have ")
                                         or p == b"done\n" or p.startswith(FILTER_LINE_PREFIXES)):
                continue
            keep.append(p)
        keep += [b"done\n", FLUSH]
        _, _, out2, _ = run_backend(self.server.root, path_info, "POST", "", protocol, ctype,
                                    encode_pkts(keep), mode_config("overfetch"))
        r2, rest2 = parse_pkts(out2)
        if b"packfile\n" not in r2:
            return out2, {"error": "no packfile section in full response",
                          "response": [pkt_text(p) for p in r2][:20]}
        k = r2.index(b"packfile\n")
        # A client still negotiating (no "done") expects an acknowledgments
        # section first. "ready" tells it the server will send the pack now.
        acks = [] if b"done\n" in pkts else [b"acknowledgments\n", b"ready\n", DELIM]
        resp = acks + (shallow_info + [DELIM] if shallow_info else []) + r2[k:]
        pack_bytes = sum(len(p) - 1 for p in r2[k:] if isinstance(p, bytes) and p[:1] == b"\x01")
        exact_bytes = sum(len(p) - 1 for p in r1 if isinstance(p, bytes) and p[:1] == b"\x01")
        note = {"shallow_info": [pkt_text(p) for p in shallow_info],
                "full_pack_bytes": pack_bytes, "exact_shallow_pack_bytes": exact_bytes}
        return encode_pkts(resp) + rest2, note


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--root", required=True, help="directory holding bare repositories")
    ap.add_argument("--port", type=int, default=0)
    ap.add_argument("--log", required=True, help="JSON-lines request log")
    ap.add_argument("--port-file", help="write the bound port here once listening")
    args = ap.parse_args()
    srv = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    srv.root = os.path.abspath(args.root)
    srv.log_path = args.log
    if args.port_file:
        with open(args.port_file, "w") as f:
            f.write(str(srv.server_address[1]))
    print("listening on 127.0.0.1:%d" % srv.server_address[1], file=sys.stderr, flush=True)
    srv.serve_forever()


if __name__ == "__main__":
    main()
