#!/usr/bin/env python3
"""Tests for site/build.py: each check must fail on the mistake it exists for.

    python3 site/test_build.py
"""

from __future__ import annotations

import sys
import tempfile
import textwrap
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import build  # noqa: E402


def write(root: Path, rel: str, text: str) -> Path:
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(textwrap.dedent(text), encoding="utf-8")
    return path


class Repo(unittest.TestCase):
    """A throwaway repository: site/src pages, one reference doc, some files."""

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name).resolve()
        write(self.root, "deploy/compose/compose.yaml", "services: {}\n")
        self.ref = write(self.root, "docs/SSH.md", "# SSH\n\nSee [compose](../deploy/compose/compose.yaml).\n")
        self.page = write(self.root, "site/src/guide.md", "# Guide\n")
        self.layout = build.Layout.of(
            self.root, self.root / "site/src", {"docs/SSH.md": "reference/ssh.md"}, repo_url="https://example.com/r"
        )

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def rewrite(self, page: Path, text: str) -> tuple[str, list[str]]:
        problems = build.Problems()
        return build.rewrite_page(self.layout, page, text, problems), problems.items


class Links(Repo):
    def test_a_link_to_a_reference_doc_points_at_its_book_page(self) -> None:
        out, problems = self.rewrite(self.page, "[ssh](../../docs/SSH.md#keys)\n")
        self.assertEqual(out, "[ssh](reference/ssh.md#keys)\n")
        self.assertEqual(problems, [])

    def test_a_link_written_against_the_book_layout_is_kept(self) -> None:
        out, problems = self.rewrite(self.page, "[ssh](reference/ssh.md)\n")
        self.assertEqual(out, "[ssh](reference/ssh.md)\n")
        self.assertEqual(problems, [])

    def test_a_link_to_another_repository_file_goes_to_github(self) -> None:
        out, problems = self.rewrite(self.ref, "[`compose`](../deploy/compose) and [y](../deploy/compose/compose.yaml)\n")
        self.assertEqual(
            out,
            "[`compose`](https://example.com/r/tree/main/deploy/compose) and "
            "[y](https://example.com/r/blob/main/deploy/compose/compose.yaml)\n",
        )
        self.assertEqual(problems, [])

    def test_a_broken_link_is_a_problem(self) -> None:
        _, problems = self.rewrite(self.page, "[gone](../../docs/GONE.md)\n")
        self.assertEqual(len(problems), 1)
        self.assertIn("broken link", problems[0])

    def test_a_github_link_to_a_missing_path_is_a_problem(self) -> None:
        _, problems = self.rewrite(self.page, "[x](https://example.com/r/blob/main/no/such/file)\n")
        self.assertEqual(len(problems), 1)

    def test_a_link_leaving_the_repository_is_a_problem(self) -> None:
        _, problems = self.rewrite(self.page, "[x](../../../outside.md)\n")
        self.assertIn("leaves the repository", problems[0])

    def test_links_in_code_are_left_alone(self) -> None:
        text = "```md\n[gone](GONE.md)\n```\n`[gone](GONE.md)` but [ssh](../../docs/SSH.md)\n"
        out, problems = self.rewrite(self.page, text)
        self.assertEqual(out, text.replace("../../docs/SSH.md", "reference/ssh.md"))
        self.assertEqual(problems, [])

    def test_staging_writes_rewritten_pages(self) -> None:
        problems = build.Problems()
        book_toml = write(self.root, "site/book.toml", "[book]\n")
        build.stage(self.layout, self.root / "site/target/book", book_toml, problems)
        self.assertEqual(problems.items, [])
        staged = (self.root / "site/target/book/src/reference/ssh.md").read_text()
        self.assertIn("https://example.com/r/blob/main/deploy/compose/compose.yaml", staged)


class Configuration(Repo):
    def setUp(self) -> None:
        super().setUp()
        write(
            self.root,
            "crates/aspen-cli/src/config.rs",
            """\
            fn f() { var("ASPEN_LISTEN"); var("DATABASE_URL"); }
            #[cfg(test)]
            mod tests { fn t() { var("ASPEN_ONLY_IN_TESTS"); } }
            """,
        )
        write(self.root, "crates/aspen-x/src/tests/a.rs", 'var("ASPEN_TEST_DIR");\n')
        write(
            self.root,
            build.MIRROR_SETTINGS,
            'let token = var("TOKEN"); from_file("TOKEN_FILE"); get("ASPEN_MIRROR_CREDENTIALS");\n',
        )

    def check(self, page_text: str) -> list[str]:
        page = write(self.root, "site/src/configuration.md", page_text)
        problems = build.Problems()
        build.check_configuration(self.root, page, problems)
        return problems.items

    def test_the_variables_read_skip_tests(self) -> None:
        self.assertEqual(
            build.variables_read(self.root),
            {
                "ASPEN_LISTEN",
                "DATABASE_URL",
                "ASPEN_MIRROR_CREDENTIALS",
                "ASPEN_MIRROR_<NAME>_TOKEN",
                "ASPEN_MIRROR_<NAME>_TOKEN_FILE",
            },
        )

    def test_a_complete_page_passes(self) -> None:
        page = (
            "`ASPEN_LISTEN` `DATABASE_URL` `PGPASSWORD` `ASPEN_MIRROR_CREDENTIALS` "
            "`ASPEN_MIRROR_<NAME>_TOKEN` `ASPEN_MIRROR_<NAME>_TOKEN_FILE` `ASPEN_CDN_KEY_PAIR_ID`\n"
        )
        self.assertEqual(self.check(page), [])

    def test_an_undocumented_variable_is_a_problem(self) -> None:
        problems = self.check("`DATABASE_URL` `ASPEN_MIRROR_CREDENTIALS` `ASPEN_MIRROR_<NAME>_TOKEN`\n")
        self.assertEqual(len(problems), 2)
        self.assertTrue(any("`ASPEN_LISTEN`" in p for p in problems))
        self.assertTrue(any("`ASPEN_MIRROR_<NAME>_TOKEN_FILE`" in p for p in problems))

    def test_a_documented_variable_nothing_reads_is_a_problem(self) -> None:
        page = (
            "`ASPEN_LISTEN` `DATABASE_URL` `ASPEN_MIRROR_CREDENTIALS` `ASPEN_MIRROR_<NAME>_TOKEN` "
            "`ASPEN_MIRROR_<NAME>_TOKEN_FILE` `ASPEN_LISTN`\n"
        )
        problems = self.check(page)
        self.assertEqual(len(problems), 1)
        self.assertIn("`ASPEN_LISTN`", problems[0])


class Summary(unittest.TestCase):
    def test_a_page_missing_from_the_summary_is_a_problem(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            src = Path(tmp)
            write(src, "SUMMARY.md", "- [A](a.md)\n" + "".join(f"- [r]({b})\n" for b in build.REFERENCE.values()))
            write(src, "a.md", "# A\n")
            write(src, "b.md", "# B\n")
            problems = build.Problems()
            build.check_summary(src, problems)
            self.assertEqual(len(problems.items), 1)
            self.assertIn("b.md", problems.items[0])


class Html(unittest.TestCase):
    def test_missing_pages_and_headings_are_problems(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            write(out, "a.html", '<h2 id="here">x</h2><a href="b.html#there">ok</a><a href="#here">ok</a>')
            write(out, "b.html", '<h2 id="there">y</h2><a href="a.html#nowhere">bad</a><a href="c.html">bad</a>')
            write(out, "print.html", '<a href="ignored.html">print view</a>')
            problems = build.Problems()
            checked = build.check_html(out, problems)
            self.assertEqual(checked, 4)
            self.assertEqual(
                sorted(problems.items),
                ["b.html: link to a missing heading: a.html#nowhere", "b.html: link to a missing page: c.html"],
            )


if __name__ == "__main__":
    unittest.main(verbosity=1)
