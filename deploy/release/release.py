#!/usr/bin/env python3
"""Release helpers for .github/workflows/release.yml. Standard library only.

    release.py check-version [<tag>]
        The workspace version (Cargo.toml), the Helm chart's appVersion and,
        when given, the tag (v<version>) must all agree. Run on every pull
        request, so a version bump that misses one of them fails early.

    release.py archive <version> <target> <binary> <out-dir>
        Packages one built binary as aspen-<version>-<target>.tar.gz with
        README.md, plus a .sha256 file in `sha256sum` format.

    release.py formula <version> <checksum-dir> [--base-url URL] [--out FILE]
        Renders deploy/release/aspen.rb.in into a Homebrew formula from the
        .sha256 files of all four release targets.

Exits non-zero, naming the problem, on any inconsistency.
"""
import argparse
import gzip
import hashlib
import io
import os
import re
import sys
import tarfile

ROOT = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
TARGETS = (
    "aarch64-apple-darwin",
    "x86_64-apple-darwin",
    "aarch64-unknown-linux-gnu",
    "x86_64-unknown-linux-gnu",
)
DEFAULT_BASE_URL = "https://github.com/reason-machines/aspen/releases/download"
VERSION_RE = re.compile(r"^\d+\.\d+\.\d+(-[0-9A-Za-z.-]+)?$")


class ReleaseError(Exception):
    pass


def read(path, mode="r"):
    with open(path, mode) as f:
        return f.read()


def write(path, data, mode="w"):
    with open(path, mode) as f:
        f.write(data)


def workspace_version(root=ROOT):
    text = read(os.path.join(root, "Cargo.toml"))
    section = re.search(r"^\[workspace\.package\]\n(.*?)(?=^\[|\Z)", text, re.M | re.S)
    if not section:
        raise ReleaseError("Cargo.toml has no [workspace.package] table")
    m = re.search(r'^version\s*=\s*"([^"]+)"', section.group(1), re.M)
    if not m:
        raise ReleaseError("Cargo.toml [workspace.package] has no version")
    return m.group(1)


def chart_app_version(root=ROOT):
    text = read(os.path.join(root, "deploy/helm/aspen/Chart.yaml"))
    m = re.search(r'^appVersion:\s*"?([^"\s]+)"?\s*$', text, re.M)
    if not m:
        raise ReleaseError("deploy/helm/aspen/Chart.yaml has no appVersion")
    return m.group(1)


def check_version(tag=None, root=ROOT):
    version = workspace_version(root)
    if not VERSION_RE.match(version):
        raise ReleaseError(f"workspace version {version!r} is not MAJOR.MINOR.PATCH[-PRE]")
    app = chart_app_version(root)
    if app != version:
        raise ReleaseError(
            f"Helm chart appVersion {app} differs from the workspace version {version}; "
            "bump deploy/helm/aspen/Chart.yaml with Cargo.toml"
        )
    if tag is not None and tag != f"v{version}":
        raise ReleaseError(f"tag {tag} does not match the workspace version: expected v{version}")
    return version


def archive(version, target, binary, out_dir, root=ROOT):
    if target not in TARGETS:
        raise ReleaseError(f"unknown target {target}; expected one of {', '.join(TARGETS)}")
    if not VERSION_RE.match(version):
        raise ReleaseError(f"version {version!r} is not MAJOR.MINOR.PATCH[-PRE]")
    name = f"aspen-{version}-{target}.tar.gz"
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, name)
    buf = io.BytesIO()
    # Fixed metadata, so the same binary always gives the same archive.
    with tarfile.open(fileobj=buf, mode="w", format=tarfile.USTAR_FORMAT) as tar:
        for src, arc, mode in ((binary, "aspen", 0o755), (os.path.join(root, "README.md"), "README.md", 0o644)):
            data = read(src, "rb")
            info = tarfile.TarInfo(arc)
            info.size, info.mode, info.mtime, info.uname, info.gname = len(data), mode, 0, "", ""
            tar.addfile(info, io.BytesIO(data))
    data = gzip.compress(buf.getvalue(), compresslevel=9, mtime=0)
    write(path, data, "wb")
    digest = hashlib.sha256(data).hexdigest()
    write(path + ".sha256", f"{digest}  {name}\n")
    return path, digest


def read_checksums(version, checksum_dir):
    sums = {}
    for target in TARGETS:
        name = f"aspen-{version}-{target}.tar.gz"
        path = os.path.join(checksum_dir, name + ".sha256")
        if not os.path.exists(path):
            raise ReleaseError(f"missing checksum for {target}: {path}")
        fields = read(path).split()
        if len(fields) != 2 or fields[1] != name or not re.fullmatch(r"[0-9a-f]{64}", fields[0]):
            raise ReleaseError(f"{path} is not '<sha256>  {name}'")
        sums[target] = fields[0]
    return sums


def formula(version, checksum_dir, base_url=DEFAULT_BASE_URL, root=ROOT):
    if not VERSION_RE.match(version):
        raise ReleaseError(f"version {version!r} is not MAJOR.MINOR.PATCH[-PRE]")
    sums = read_checksums(version, checksum_dir)
    text = read(os.path.join(root, "deploy/release/aspen.rb.in"))
    text = text.replace("@VERSION@", version)
    for target, digest in sums.items():
        url = f"{base_url.rstrip('/')}/v{version}/aspen-{version}-{target}.tar.gz"
        text = text.replace(f"@URL_{target}@", url).replace(f"@SHA256_{target}@", digest)
    left = re.findall(r"@[A-Za-z0-9_-]+@", text)
    if left:
        raise ReleaseError(f"unfilled placeholders in the formula: {sorted(set(left))}")
    return text


def main(argv):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    c = sub.add_parser("check-version")
    c.add_argument("tag", nargs="?")
    a = sub.add_parser("archive")
    a.add_argument("version")
    a.add_argument("target")
    a.add_argument("binary")
    a.add_argument("out_dir")
    f = sub.add_parser("formula")
    f.add_argument("version")
    f.add_argument("checksum_dir")
    f.add_argument("--base-url", default=DEFAULT_BASE_URL)
    f.add_argument("--out")
    args = p.parse_args(argv)
    try:
        if args.cmd == "check-version":
            print(check_version(args.tag))
        elif args.cmd == "archive":
            path, digest = archive(args.version, args.target, args.binary, args.out_dir)
            print(f"{digest}  {path}")
        else:
            text = formula(args.version, args.checksum_dir, args.base_url)
            if args.out:
                write(args.out, text)
            else:
                sys.stdout.write(text)
    except ReleaseError as e:
        print(f"release.py: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
