#!/usr/bin/env python3
"""Fails when a container image the deployment files pull is not pinned.

Issue #125: a tag can be moved to different bytes, so every image Aspen's
own files pull by default is pinned to a digest (`image:tag@sha256:<64 hex>`).
Checked here:

- deploy/docker/Dockerfile: the `# syntax=` frontend, and every `FROM`,
  which must name an earlier stage or a digest-pinned image (directly, or
  through an `ARG` whose default is one). The tags beside the digests must
  agree with the versions the rest of the repository uses: the Rust image
  with `ARG RUST_VERSION` (itself checked against rust-toolchain.toml by
  check-rust-version.sh), the Node image with demo/place/.nvmrc, and every
  Debian-based image with one Debian release.
- deploy/compose/compose.yaml: every `image:` (the value, or the default
  of `${VAR:-default}`), except a service built from this checkout.
- Terraform (deploy/terraform, outside tests): every default of a variable
  whose name ends in `image`, and every string literal that looks like an
  image reference (`<registry>/<path>:<tag>`).

Images an operator passes in (the Aspen image itself) are validated by the
modules, which accept a version tag or a digest.

Usage, from the repository root (the arguments exist for the tests):

    deploy/scripts/check-image-pins.py [repository root]
"""
import pathlib
import re
import sys

DIGEST = re.compile(r"@sha256:[0-9a-f]{64}$")
# A string literal that is a whole image reference with a registry host.
IMAGE_LITERAL = re.compile(r'"((?:[a-z0-9-]+\.)+[a-z]{2,}(?::[0-9]+)?/[a-z0-9._/-]+(?::[A-Za-z0-9._-]+)?(?:@sha256:[0-9a-f]+)?)"')


def pinned(ref):
    return bool(DIGEST.search(ref))


def tag_of(ref):
    """`name:tag` of an `name:tag@sha256:...` reference (no registry port)."""
    name = ref.split("@", 1)[0]
    last = name.rsplit("/", 1)[-1]
    return last.split(":", 1) if ":" in last else [last, ""]


def check_dockerfile(path, nvmrc):
    problems = []
    text = path.read_text()
    first = text.splitlines()[0] if text else ""
    if first.startswith("# syntax="):
        if not pinned(first[len("# syntax="):].strip()):
            problems.append(f"{path}: the `# syntax=` frontend is not pinned to a digest (add @sha256:...)")
    args, stages = {}, set()
    for n, line in enumerate(text.splitlines(), 1):
        line = line.strip()
        m = re.match(r"ARG\s+([A-Za-z_][A-Za-z0-9_]*)=(\S+)", line)
        if m:
            args.setdefault(m.group(1), m.group(2))
            continue
        m = re.match(r"FROM\s+(?:--platform=\S+\s+)?(\S+)(?:\s+AS\s+(\S+))?", line, re.I)
        if not m:
            continue
        ref, stage = m.group(1), m.group(2)
        var = re.fullmatch(r"\$\{?([A-Za-z_][A-Za-z0-9_]*)\}?", ref)
        if ref in stages:
            pass
        elif var:
            default = args.get(var.group(1), "")
            if not pinned(default):
                problems.append(f"{path}:{n}: FROM {ref}: ARG {var.group(1)} must default to an image pinned "
                                f"to a digest (name:tag@sha256:...), not {default or 'nothing'}")
        elif "$" in ref:
            problems.append(f"{path}:{n}: FROM {ref}: pin it through one ARG holding the whole "
                            "reference (name:tag@sha256:...), so the digest cannot drift from the tag")
        elif not pinned(ref):
            problems.append(f"{path}:{n}: FROM {ref} is not pinned to a digest (add @sha256:...)")
        if stage:
            stages.add(stage)

    images = {k: v for k, v in args.items() if pinned(v)}
    releases = set()
    for var, ref in images.items():
        name, tag = tag_of(ref)
        if name == "rust":
            want = args.get("RUST_VERSION")
            if not tag.startswith(f"{want}-"):
                problems.append(f"{path}: ARG {var} is rust:{tag}, but RUST_VERSION is {want}: "
                                f"use a rust:{want}-... image and its digest")
        if name == "node" and nvmrc is not None and not tag.startswith(f"{nvmrc}-"):
            problems.append(f"{path}: ARG {var} is node:{tag}, but demo/place/.nvmrc is {nvmrc}: "
                            f"use a node:{nvmrc}-... image and its digest")
        if name in ("rust", "node", "debian"):
            m = re.search(r"(?:^|-)(bookworm|trixie|forky|bullseye)(?:-|$)", tag)
            if m:
                releases.add(m.group(1))
    if len(releases) > 1:
        problems.append(f"{path}: the base images mix Debian releases {sorted(releases)}: "
                        "build and run on one release")
    return problems


def check_compose(path):
    problems = []
    current, built = None, set()
    lines = path.read_text().splitlines()
    for line in lines:
        m = re.match(r"^  ([A-Za-z0-9_-]+):\s*$", line)
        if m:
            current = m.group(1)
        if current and re.match(r"^    build:", line):
            built.add(current)
    current = None
    for n, line in enumerate(lines, 1):
        m = re.match(r"^  ([A-Za-z0-9_-]+):\s*$", line)
        if m:
            current = m.group(1)
        m = re.match(r"^\s*image:\s*(\S+)", line)
        if not m or current in built:
            continue
        value = m.group(1).strip("'\"")
        d = re.fullmatch(r"\$\{[A-Za-z_][A-Za-z0-9_]*:-(.+)\}", value)
        ref = d.group(1) if d else value
        if not pinned(ref):
            problems.append(f"{path}:{n}: image {ref} is not pinned to a digest (add @sha256:...)")
    return problems


def check_terraform(root):
    problems = []
    for path in sorted(root.rglob("*")):
        if path.suffix not in (".tf", ".tftpl") or "tests" in path.parts or ".terraform" in path.parts:
            continue
        text = path.read_text()
        for m in re.finditer(r'variable\s+"([a-z0-9_]*image)"\s*\{(.*?)\n\}', text, re.S):
            d = re.search(r'^\s*default\s*=\s*"([^"]*)"', m.group(2), re.M)
            if d and (":" in d.group(1) or "/" in d.group(1)) and not pinned(d.group(1)):
                problems.append(f"{path}: variable {m.group(1)} defaults to {d.group(1)}, "
                                "which is not pinned to a digest (add @sha256:...)")
        for n, line in enumerate(text.splitlines(), 1):
            if re.match(r"\s*(description|error_message)\s*=", line) or line.lstrip().startswith("#"):
                continue
            for lit in IMAGE_LITERAL.finditer(line):
                if ":" in lit.group(1).split("/", 1)[1] and not pinned(lit.group(1)):
                    problems.append(f"{path}:{n}: {lit.group(1)} is not pinned to a digest (add @sha256:...)")
    return problems


def check(root):
    root = pathlib.Path(root)
    nvmrc_path = root / "demo/place/.nvmrc"
    nvmrc = nvmrc_path.read_text().strip().lstrip("v") if nvmrc_path.exists() else None
    return (check_dockerfile(root / "deploy/docker/Dockerfile", nvmrc)
            + check_compose(root / "deploy/compose/compose.yaml")
            + check_terraform(root / "deploy/terraform"))


def main():
    problems = check(sys.argv[1] if len(sys.argv) > 1 else ".")
    for p in problems:
        print(f"error: {p}", file=sys.stderr)
    if problems:
        sys.exit(1)
    print("ok: every base and default image is pinned to a digest")


if __name__ == "__main__":
    main()
