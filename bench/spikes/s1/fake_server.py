#!/usr/bin/env python3
"""A deliberately dumb upload-pack that always serves one pre-built pack.

Spike S1 asks one question: will stock clients accept a stitched pack? So this
server ignores what the client says it has and answers every fetch with the
scenario's `stitched.pack`. It speaks:

  * protocol v0/v1 and v2 over a pipe, for `git fetch --upload-pack=...`
    (`python3 fake_server.py upload-pack <scenario-dir>`), and
  * smart HTTP v0 (stateless) and v2, for libraries
    (`python3 fake_server.py http --root <scenarios-dir> --port N`).

A scenario directory holds `scenario.json` (`tip`, `head_ref`) and
`stitched.pack`. If `S1_LOG` is set, each request appends one JSON line with
the protocol and the capabilities the client asked for, so the matrix can
report what each client requested (thin-pack, ofs-delta, ...).

Not a real server: no negotiation, no ACKs, no shallow, no filters.
"""

from __future__ import annotations

import argparse
import gzip
import io
import json
import os
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

FLUSH, DELIM, RESPONSE_END = object(), object(), object()
AGENT = "agent=aspen-s1-fake"


def pkt(data: bytes | str) -> bytes:
    if isinstance(data, str):
        data = data.encode()
    return b"%04x" % (len(data) + 4) + data


def read_pkt(f):
    hdr = f.read(4)
    if len(hdr) < 4:
        return None
    n = int(hdr, 16)
    if n == 0:
        return FLUSH
    if n == 1:
        return DELIM
    if n == 2:
        return RESPONSE_END
    return f.read(n - 4)


def sideband(data: bytes, max_payload: int) -> bytes:
    out = bytearray()
    for i in range(0, len(data), max_payload):
        out += pkt(b"\x01" + data[i : i + max_payload])
    return bytes(out)


class Scenario:
    def __init__(self, path: str):
        with open(os.path.join(path, "scenario.json")) as f:
            meta = json.load(f)
        self.name = meta["name"]
        self.tip = meta["tip"]
        self.head_ref = meta.get("head_ref", "refs/heads/main")
        with open(os.path.join(path, "stitched.pack"), "rb") as f:
            self.pack = f.read()


def log(entry: dict) -> None:
    path = os.environ.get("S1_LOG")
    if path:
        with open(path, "a") as f:
            f.write(json.dumps(entry) + "\n")


# ---------------------------------------------------------------- v0 / v1


def v0_advertisement(sc: Scenario) -> bytes:
    caps = (
        f"multi_ack_detailed thin-pack side-band side-band-64k ofs-delta no-progress include-tag "
        f"symref=HEAD:{sc.head_ref} object-format=sha1 {AGENT}"
    )
    return pkt(f"{sc.tip} HEAD\0{caps}\n") + pkt(f"{sc.tip} {sc.head_ref}\n") + b"0000"


def v0_pack_response(sc: Scenario, caps: list[str]) -> bytes:
    out = pkt("NAK\n")
    if "side-band-64k" in caps:
        out += sideband(sc.pack, 65515) + b"0000"
    elif "side-band" in caps:
        out += sideband(sc.pack, 995) + b"0000"
    else:
        out += sc.pack
    return out


def parse_first_want(line: bytes) -> list[str]:
    # "want <oid> cap cap ...\n"
    parts = line.decode().strip().split(" ")
    return parts[2:]


def serve_v0_pipe(sc: Scenario, rd, wr, proto: str) -> None:
    wr.write(v0_advertisement(sc))
    wr.flush()
    p = read_pkt(rd)
    if p is None or p is FLUSH or not p.startswith(b"want "):
        return  # client wanted nothing
    caps = parse_first_want(p)
    while True:  # remaining want / shallow lines
        p = read_pkt(rd)
        if p is None:
            return
        if p is FLUSH:
            break
    log({"scenario": sc.name, "transport": "pipe", "proto": proto, "caps": caps})
    while True:  # have lines
        p = read_pkt(rd)
        if p is None:
            return
        if p is FLUSH:
            wr.write(pkt("NAK\n"))
            wr.flush()
        elif p.strip() == b"done":
            wr.write(v0_pack_response(sc, caps))
            wr.flush()
            return


# ---------------------------------------------------------------- v2


def v2_advertisement() -> bytes:
    return (
        pkt("version 2\n")
        + pkt(AGENT + "\n")
        + pkt("ls-refs\n")
        + pkt("fetch\n")
        + pkt("object-format=sha1\n")
        + b"0000"
    )


def read_v2_request(rd):
    """Return (command, caps, args) or None at EOF."""
    command, caps, args = None, [], []
    in_args = False
    while True:
        p = read_pkt(rd)
        if p is None:
            return None
        if p is FLUSH:
            return command, caps, args
        if p is DELIM:
            in_args = True
            continue
        line = p.decode().rstrip("\n")
        if in_args:
            args.append(line)
        elif line.startswith("command="):
            command = line[len("command=") :]
        else:
            caps.append(line)


def v2_response(sc: Scenario, command: str, args: list[str], transport: str) -> bytes:
    if command == "ls-refs":
        prefixes = [a[len("ref-prefix ") :] for a in args if a.startswith("ref-prefix ")]
        refs = [("HEAD", f" symref-target:{sc.head_ref}" if "symrefs" in args else ""), (sc.head_ref, "")]
        out = b""
        for name, extra in refs:
            if prefixes and not any(name.startswith(p) for p in prefixes):
                continue
            out += pkt(f"{sc.tip} {name}{extra}\n")
        return out + b"0000"
    if command == "fetch":
        feats = [a for a in args if not a.startswith(("want ", "have "))]
        if "done" not in args:
            return pkt("acknowledgments\n") + pkt("NAK\n") + b"0000"
        log({"scenario": sc.name, "transport": transport, "proto": "v2", "caps": feats})
        return pkt("packfile\n") + sideband(sc.pack, 65515) + b"0000"
    return b"0000"


def serve_v2_pipe(sc: Scenario, rd, wr) -> None:
    wr.write(v2_advertisement())
    wr.flush()
    while True:
        req = read_v2_request(rd)
        if req is None:
            return
        command, _caps, args = req
        if command is None:
            return
        wr.write(v2_response(sc, command, args, "pipe"))
        wr.flush()


def upload_pack_main(path: str) -> None:
    sc = Scenario(path)
    rd, wr = sys.stdin.buffer, sys.stdout.buffer
    proto = os.environ.get("GIT_PROTOCOL", "")
    if "version=2" in proto:
        serve_v2_pipe(sc, rd, wr)
    else:
        serve_v0_pipe(sc, rd, wr, "v1" if "version=1" in proto else "v0")


# ---------------------------------------------------------------- HTTP


class Handler(BaseHTTPRequestHandler):
    root = "."
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *a):  # quiet
        pass

    def _scenario(self, suffix: str) -> Scenario | None:
        path = self.path.split("?", 1)[0]
        if not path.endswith(suffix):
            return None
        name = path[: -len(suffix)].strip("/")
        if name.endswith(".git"):
            name = name[:-4]
        d = os.path.join(self.root, name)
        if not os.path.isfile(os.path.join(d, "scenario.json")):
            return None
        return Scenario(d)

    def _send(self, code: int, ctype: str, body: bytes) -> None:
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        self.wfile.write(body)

    def _v2(self) -> bool:
        return "version=2" in (self.headers.get("Git-Protocol") or "")

    def do_GET(self):
        sc = self._scenario("/info/refs")
        if sc is None or "service=git-upload-pack" not in self.path:
            return self._send(404, "text/plain", b"not found\n")
        if self._v2():
            body = v2_advertisement()
        else:
            body = pkt("# service=git-upload-pack\n") + b"0000" + v0_advertisement(sc)
        self._send(200, "application/x-git-upload-pack-advertisement", body)

    def _body(self) -> bytes:
        if self.headers.get("Transfer-Encoding", "").lower() == "chunked":
            data = bytearray()
            while True:
                n = int(self.rfile.readline().strip().split(b";")[0], 16)
                if n == 0:
                    self.rfile.readline()
                    break
                data += self.rfile.read(n)
                self.rfile.readline()
            data = bytes(data)
        else:
            data = self.rfile.read(int(self.headers.get("Content-Length", "0")))
        if self.headers.get("Content-Encoding", "").lower() in ("gzip", "x-gzip"):
            data = gzip.decompress(data)
        return data

    def do_POST(self):
        sc = self._scenario("/git-upload-pack")
        if sc is None:
            return self._send(404, "text/plain", b"not found\n")
        rd = io.BytesIO(self._body())
        ctype = "application/x-git-upload-pack-result"
        if self._v2():
            req = read_v2_request(rd)
            command, _caps, args = req if req else (None, [], [])
            return self._send(200, ctype, v2_response(sc, command, args, "http"))
        caps: list[str] = []
        done = False
        while True:
            p = read_pkt(rd)
            if p is None:
                break
            if isinstance(p, bytes):
                if p.startswith(b"want ") and not caps:
                    caps = parse_first_want(p)
                if p.strip() == b"done":
                    done = True
        if done:
            log({"scenario": sc.name, "transport": "http", "proto": "v0", "caps": caps})
            return self._send(200, ctype, v0_pack_response(sc, caps))
        self._send(200, ctype, pkt("NAK\n"))


class QuietServer(ThreadingHTTPServer):
    daemon_threads = True

    def handle_error(self, request, client_address):
        # Clients that reject a pack often hang up mid-response; that is the
        # result being measured, not a server error.
        if not isinstance(sys.exc_info()[1], (ConnectionResetError, BrokenPipeError)):
            super().handle_error(request, client_address)


def start_http(root: str, port: int = 0) -> ThreadingHTTPServer:
    handler = type("H", (Handler,), {"root": root})
    srv = QuietServer(("127.0.0.1", port), handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv


def main() -> None:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    up = sub.add_parser("upload-pack")
    up.add_argument("path")
    ht = sub.add_parser("http")
    ht.add_argument("--root", required=True)
    ht.add_argument("--port", type=int, default=7071)
    a = ap.parse_args()
    if a.cmd == "upload-pack":
        upload_pack_main(a.path)
    else:
        srv = start_http(a.root, a.port)
        print(f"serving {a.root} on http://127.0.0.1:{srv.server_address[1]}/<scenario>", flush=True)
        threading.Event().wait()


if __name__ == "__main__":
    main()
