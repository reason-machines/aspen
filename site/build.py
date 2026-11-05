#!/usr/bin/env python3
"""Build Aspen's documentation site and check it.

The site is an mdBook. Its hand-written pages live in `site/src`; the
reference documents that live next to the code (`docs/*.md`,
`deploy/README.md`, `crates/aspen-meta/SCHEMA.md`, `bench/README.md`,
`bench/campaign/README.md`) are
copied in at build time, so there is one copy of each and it stays next to
what it describes.

    python3 site/build.py            # stage, build with mdBook, run every check
    python3 site/build.py --no-build # only the checks that need no mdBook

What `build` does, in order:

1. **Stage.** Copy every page into `site/target/book/src`, rewriting each
   relative Markdown link: a link to another page of the book points at
   that page; a link to any other file in the repository becomes a link to
   it on GitHub (`ASPEN_DOCS_REPO_URL`, `ASPEN_DOCS_REF`); a link to a path
   that does not exist is an error. Links inside code are left alone.
2. **Check the configuration page.** Every environment variable the code
   reads (`"ASPEN_..."`, `"OTEL_..."` and `"DATABASE_URL"` string literals
   outside tests) must be named on `configuration.md`, and every `ASPEN_`
   name the page shows must be one the code reads (or be listed in
   `DOCUMENTED_NOT_READ` with the reason).
3. **Check the summary.** Every page in `site/src` is in `SUMMARY.md`.
4. **Build** with mdBook into `site/target/html`.
5. **Check the HTML.** Every internal link resolves to a built file, and
   every `#fragment` to an `id` in that file.

Any failure prints every problem found and exits 1.
"""

from __future__ import annotations

import argparse
import html.parser
import os
import posixpath
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
SITE = REPO / "site"

MDBOOK_VERSION = "0.5.4"

# Reference documents copied into the book: repository path -> book path.
REFERENCE = {
    "deploy/README.md": "reference/deploy.md",
    "docs/SSH.md": "reference/ssh.md",
    "docs/MIRROR.md": "reference/mirror.md",
    "docs/EXPRESS.md": "reference/express.md",
    "docs/OBSERVABILITY.md": "reference/observability.md",
    "crates/aspen-meta/SCHEMA.md": "reference/schema.md",
    "bench/README.md": "reference/bench.md",
    "bench/campaign/README.md": "reference/bench-campaign.md",
    "docs/PLAN.md": "reference/plan.md",
}

# `ASPEN_` names the configuration page may show although no code reads
# them, each with the reason. Keep this list short and every entry true.
DOCUMENTED_NOT_READ = {
    "ASPEN_CDN_KEY_PAIR_ID": "superseded by ASPEN_EXPRESS_KEYS (node C2); no deployment sets it since #45",
    "ASPEN_CDN_PRIVATE_KEY": "superseded by ASPEN_EXPRESS_KEYS (node C2); no deployment sets it since #45",
}

# Mirror credentials are read as ASPEN_MIRROR_<NAME>_<SUFFIX>; the page
# documents them in that form.
MIRROR_SETTINGS = "crates/aspen-mirror/src/settings.rs"

LINK = re.compile(r"(!?\[(?:[^\[\]]|\[[^\[\]]*\])*\]\()(<[^>]*>|[^)\s]+)((?:\s+\"[^\"]*\")?\))")
CODE_SPAN = re.compile(r"(`+)(.+?)\1")
FENCE = re.compile(r"^\s*(```+|~~~+)")
EXTERNAL = re.compile(r"^[a-zA-Z][a-zA-Z0-9+.-]*:")


@dataclass
class Problems:
    items: list[str] = field(default_factory=list)

    def add(self, msg: str) -> None:
        self.items.append(msg)

    def __bool__(self) -> bool:
        return bool(self.items)


@dataclass
class Layout:
    """Where each page comes from and where it goes in the book."""

    repo: Path
    # Real file -> path inside the book's `src`, POSIX form.
    pages: dict[Path, str]
    repo_url: str = "https://github.com/reason-machines/aspen"
    ref: str = "main"

    @classmethod
    def of(cls, repo: Path, src: Path, reference: dict[str, str], **kw: str) -> "Layout":
        pages: dict[Path, str] = {}
        for path in sorted(src.rglob("*.md")):
            pages[path.resolve()] = path.relative_to(src).as_posix()
        for real, book in reference.items():
            pages[(repo / real).resolve()] = book
        return cls(repo=repo.resolve(), pages=pages, **kw)

    @property
    def book_paths(self) -> set[str]:
        return set(self.pages.values())

    def github_url(self, target: Path, fragment: str) -> str:
        rel = target.relative_to(self.repo).as_posix()
        kind = "tree" if target.is_dir() else "blob"
        return f"{self.repo_url}/{kind}/{self.ref}/{rel}{fragment}"


def rewrite_links(line: str, fn) -> str:
    """Replace each link target on `line` with `fn(target)`, except links that
    start inside inline code (link text may itself contain code)."""
    spans = [m.span() for m in CODE_SPAN.finditer(line)]

    def sub(m: re.Match) -> str:
        if any(a <= m.start() < b for a, b in spans):
            return m.group(0)
        return m.group(1) + fn(m.group(2)) + m.group(3)

    return LINK.sub(sub, line)


def rewrite_target(layout: Layout, page: Path, target: str, problems: Problems) -> str:
    """The book's form of one link `target` found on `page` (a real file)."""
    bracketed = target.startswith("<") and target.endswith(">")
    raw = target[1:-1] if bracketed else target
    if raw.startswith("#") or EXTERNAL.match(raw):
        m = re.match(re.escape(layout.repo_url) + r"/(?:blob|tree)/[^/]+/([^#?]+)", raw)
        if m and not (layout.repo / m.group(1)).exists():
            problems.add(f"{rel(layout, page)}: link to a repository path that does not exist: {raw}")
        return target
    path, hash_, frag = raw.partition("#")
    fragment = hash_ + frag
    book_page = layout.pages[page]
    # A link written against the book's own layout (SUMMARY.md, reference/...).
    in_book = posixpath.normpath(posixpath.join(posixpath.dirname(book_page), path))
    if in_book in layout.book_paths:
        return target
    real = (page.parent / path).resolve()
    try:
        real.relative_to(layout.repo)
    except ValueError:
        problems.add(f"{rel(layout, page)}: link leaves the repository: {raw}")
        return target
    if real in layout.pages:
        new = posixpath.relpath(layout.pages[real], posixpath.dirname(book_page) or ".")
        return new + fragment
    if real.exists():
        return layout.github_url(real, fragment)
    problems.add(f"{rel(layout, page)}: broken link: {raw}")
    return target


def rewrite_page(layout: Layout, page: Path, text: str, problems: Problems) -> str:
    out, fence = [], None
    for line in text.splitlines(keepends=True):
        m = FENCE.match(line)
        if fence:
            if m and m.group(1)[0] == fence[0] and len(m.group(1)) >= len(fence):
                fence = None
            out.append(line)
            continue
        if m:
            fence = m.group(1)
            out.append(line)
            continue

        out.append(rewrite_links(line, lambda t: rewrite_target(layout, page, t, problems)))
    return "".join(out)


def rel(layout: Layout, path: Path) -> str:
    try:
        return path.relative_to(layout.repo).as_posix()
    except ValueError:
        return str(path)


def stage(layout: Layout, book_root: Path, book_toml: Path, problems: Problems) -> None:
    if book_root.exists():
        shutil.rmtree(book_root)
    src = book_root / "src"
    for real, book in layout.pages.items():
        if not real.exists():
            problems.add(f"reference document missing: {rel(layout, real)}")
            continue
        dest = src / book
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(rewrite_page(layout, real, real.read_text(encoding="utf-8"), problems), encoding="utf-8")
    shutil.copy(book_toml, book_root / "book.toml")


# --- configuration page ----------------------------------------------------

VAR_LITERAL = re.compile(r'"((?:ASPEN|OTEL)_[A-Z0-9_]*[A-Z0-9]|DATABASE_URL)"')
MIRROR_SUFFIX = re.compile(r'\b(?:var|from_file)\("([A-Z0-9_]+)"\)')
DOC_NAME = re.compile(r"`((?:ASPEN|OTEL)_[A-Za-z0-9_<>]*[A-Za-z0-9>]|DATABASE_URL|PGPASSWORD)`")


def production_source(path: Path) -> str:
    """A Rust file's text up to its first `#[cfg(test)]`."""
    text = path.read_text(encoding="utf-8")
    cut = text.find("#[cfg(test)]")
    return text if cut < 0 else text[:cut]


def variables_read(repo: Path) -> set[str]:
    names: set[str] = set()
    for path in sorted((repo / "crates").glob("*/src/**/*.rs")):
        if "tests" in path.relative_to(repo).parts or path.name == "tests.rs":
            continue
        names.update(VAR_LITERAL.findall(production_source(path)))
    settings = repo / MIRROR_SETTINGS
    if settings.exists():
        for suffix in MIRROR_SUFFIX.findall(production_source(settings)):
            names.add(f"ASPEN_MIRROR_<NAME>_{suffix}")
    return names


def check_configuration(repo: Path, page: Path, problems: Problems) -> None:
    read = variables_read(repo)
    shown = set(DOC_NAME.findall(page.read_text(encoding="utf-8")))
    where = page.relative_to(repo).as_posix() if page.is_relative_to(repo) else str(page)
    for name in sorted(read - shown):
        problems.add(f"{where}: the code reads `{name}`, but the page does not document it")
    for name in sorted(n for n in shown - read if n.startswith("ASPEN_")):
        if name not in DOCUMENTED_NOT_READ:
            problems.add(
                f"{where}: documents `{name}`, which no code reads "
                "(remove it, or list it in DOCUMENTED_NOT_READ with the reason)"
            )


def check_summary(src: Path, problems: Problems) -> None:
    summary = (src / "SUMMARY.md").read_text(encoding="utf-8")
    listed = {posixpath.normpath(t) for t in re.findall(r"\]\(([^)#\s]+)", summary)}
    for path in sorted(src.rglob("*.md")):
        name = path.relative_to(src).as_posix()
        if name != "SUMMARY.md" and name not in listed:
            problems.add(f"site/src/{name} is not in SUMMARY.md, so the book would not show it")
    for real, book in REFERENCE.items():
        if book not in listed:
            problems.add(f"{real} is copied to {book}, which is not in SUMMARY.md")


# --- built HTML ----------------------------------------------------------------


class Anchors(html.parser.HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.ids: set[str] = set()
        self.hrefs: list[str] = []

    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        for key in ("id", "name") if tag == "a" else ("id",):
            if a.get(key):
                self.ids.add(a[key])
        if tag == "a" and a.get("href"):
            self.hrefs.append(a["href"])


# mdBook's own pages: the print view repeats every page, and 404/toc pages
# use the site root.
SKIP_HTML = {"print.html", "404.html", "toc.html"}


def check_html(out: Path, problems: Problems) -> int:
    parsed: dict[Path, Anchors] = {}

    def anchors(path: Path) -> Anchors:
        if path not in parsed:
            p = Anchors()
            p.feed(path.read_text(encoding="utf-8", errors="replace"))
            parsed[path] = p
        return parsed[path]

    checked = 0
    for page in sorted(out.rglob("*.html")):
        if page.name in SKIP_HTML:
            continue
        for href in anchors(page).hrefs:
            if EXTERNAL.match(href) or href.startswith("//"):
                continue
            path, _, frag = href.partition("#")
            path = path.split("?", 1)[0]
            target = page if not path else (page.parent / path).resolve()
            if target.is_dir():
                target = target / "index.html"
            checked += 1
            where = page.relative_to(out).as_posix()
            if not target.exists():
                problems.add(f"{where}: link to a missing page: {href}")
            elif frag and target.suffix == ".html" and frag not in anchors(target).ids:
                problems.add(f"{where}: link to a missing heading: {href}")
    return checked


# --- driver ----------------------------------------------------------------


def mdbook_binary() -> str:
    exe = shutil.which("mdbook")
    if not exe:
        sys.exit(
            f"mdbook not found: run site/install-mdbook.sh (Linux x86_64), or "
            f"`cargo install mdbook --version {MDBOOK_VERSION} --locked`"
        )
    version = subprocess.run([exe, "--version"], capture_output=True, text=True, check=True).stdout.strip()
    if version != f"mdbook v{MDBOOK_VERSION}":
        sys.exit(f"site/build.py wants mdbook v{MDBOOK_VERSION}, found {version!r}")
    return exe


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--no-build", action="store_true", help="stage and check, but do not run mdBook")
    args = ap.parse_args()

    problems = Problems()
    target = SITE / "target"
    layout = Layout.of(
        REPO,
        SITE / "src",
        REFERENCE,
        repo_url=os.environ.get("ASPEN_DOCS_REPO_URL", "https://github.com/reason-machines/aspen"),
        ref=os.environ.get("ASPEN_DOCS_REF", "main"),
    )
    stage(layout, target / "book", SITE / "book.toml", problems)
    check_configuration(REPO, SITE / "src" / "configuration.md", problems)
    check_summary(SITE / "src", problems)
    checked = None
    if not args.no_build and not problems:
        out = target / "html"
        if out.exists():
            shutil.rmtree(out)
        subprocess.run([mdbook_binary(), "build", str(target / "book"), "--dest-dir", str(out)], check=True)
        checked = check_html(out, problems)
    if problems:
        print(f"{len(problems.items)} problem(s) in the docs site:", file=sys.stderr)
        for p in problems.items:
            print(f"  {p}", file=sys.stderr)
        return 1
    pages = len(layout.pages)
    if checked is None:
        print(f"docs site: {pages} pages staged and checked (not built)")
    else:
        print(f"docs site: {pages} pages built into site/target/html; {checked} internal links resolve")
    return 0


if __name__ == "__main__":
    sys.exit(main())
