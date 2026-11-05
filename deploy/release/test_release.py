#!/usr/bin/env python3
"""Tests for deploy/release/release.py. Run from anywhere:

    python3 deploy/release/test_release.py

Also checks, on the real repository, that the workspace version and the Helm
chart's appVersion agree, that a rendered formula is valid Ruby when `ruby`
is installed, and two promises of .github/workflows/release.yml (needs
PyYAML): no secret is interpolated into a script, and a publish request
from a branch fails in the plan job.
"""
import os
import shutil
import subprocess
import sys
import tarfile
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import release  # noqa: E402


def fake_root(tmp, cargo_version="1.2.3", chart_version="1.2.3"):
    os.makedirs(os.path.join(tmp, "deploy/helm/aspen"))
    os.makedirs(os.path.join(tmp, "deploy/release"))
    open(os.path.join(tmp, "Cargo.toml"), "w").write(
        '[workspace]\nmembers = []\n\n[workspace.package]\nversion = "%s"\nedition = "2024"\n\n'
        '[workspace.dependencies]\nversion = "9.9.9"\n' % cargo_version
    )
    open(os.path.join(tmp, "deploy/helm/aspen/Chart.yaml"), "w").write(
        'apiVersion: v2\nname: aspen\nversion: 0.1.0\nappVersion: "%s"\n' % chart_version
    )
    open(os.path.join(tmp, "README.md"), "w").write("# Aspen\n")
    shutil.copy(os.path.join(release.ROOT, "deploy/release/aspen.rb.in"), os.path.join(tmp, "deploy/release"))
    return tmp


class Versions(unittest.TestCase):
    def test_the_repository_is_consistent(self):
        release.check_version()

    def test_tag_cargo_and_chart_must_agree(self):
        with tempfile.TemporaryDirectory() as t:
            root = fake_root(t)
            self.assertEqual(release.check_version("v1.2.3", root), "1.2.3")
            with self.assertRaisesRegex(release.ReleaseError, "expected v1.2.3"):
                release.check_version("v1.2.4", root)
            with self.assertRaisesRegex(release.ReleaseError, "expected v1.2.3"):
                release.check_version("1.2.3", root)
        with tempfile.TemporaryDirectory() as t:
            root = fake_root(t, chart_version="1.2.2")
            with self.assertRaisesRegex(release.ReleaseError, "appVersion 1.2.2 differs"):
                release.check_version(None, root)

    def test_only_the_workspace_package_version_counts(self):
        with tempfile.TemporaryDirectory() as t:
            root = fake_root(t)
            self.assertEqual(release.workspace_version(root), "1.2.3")


REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def workflow():
    import yaml

    with open(os.path.join(REPO, ".github/workflows/release.yml")) as f:
        return yaml.safe_load(f)


class Workflow(unittest.TestCase):
    def test_no_secret_is_interpolated_into_a_script(self):
        for name, job in workflow()["jobs"].items():
            for step in job.get("steps", []):
                script = step.get("run", "")
                for needle in ("secrets.", "github.token"):
                    self.assertNotIn(needle, script, f"{name}: {step.get('name')}: pass it through env:")

    def plan(self, **env):
        step = next(s for s in workflow()["jobs"]["plan"]["steps"] if s.get("id") == "v")
        with tempfile.TemporaryDirectory() as tmp:
            out = os.path.join(tmp, "out")
            r = subprocess.run(
                ["bash", "-e", "-c", step["run"]], cwd=REPO, capture_output=True, text=True,
                env={**os.environ, "GITHUB_OUTPUT": out, "GITHUB_STEP_SUMMARY": os.path.join(tmp, "sum"),
                     "ENABLED": "true", **env},
            )
            outputs = open(out).read() if os.path.exists(out) else ""
        return r, outputs

    def test_a_publish_request_from_a_branch_fails_before_anything_is_built(self):
        r, outputs = self.plan(TAG="", WANT_PUBLISH="true")
        self.assertNotEqual(r.returncode, 0)
        self.assertIn("needs a v<version> tag ref", r.stdout + r.stderr)
        self.assertNotIn("publish=true", outputs)

    def test_a_branch_run_is_a_dry_run_and_a_tag_run_publishes(self):
        r, outputs = self.plan(TAG="", WANT_PUBLISH="false")
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn("publish=false", outputs)
        r, outputs = self.plan(TAG=f"v{release.workspace_version(REPO)}", WANT_PUBLISH="true")
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn("publish=true", outputs)


class Archives(unittest.TestCase):
    def test_archives_are_reproducible_and_carry_the_binary(self):
        with tempfile.TemporaryDirectory() as t:
            root = fake_root(os.path.join(t, "root"))
            binary = os.path.join(t, "aspen")
            open(binary, "wb").write(b"\x7fELF fake binary")
            p1, d1 = release.archive("1.2.3", "x86_64-unknown-linux-gnu", binary, os.path.join(t, "a"), root)
            p2, d2 = release.archive("1.2.3", "x86_64-unknown-linux-gnu", binary, os.path.join(t, "b"), root)
            self.assertEqual(d1, d2, "same input, same archive")
            self.assertTrue(p1.endswith("aspen-1.2.3-x86_64-unknown-linux-gnu.tar.gz"))
            with tarfile.open(p1) as tar:
                member = tar.getmember("aspen")
                self.assertEqual(member.mode, 0o755)
                self.assertEqual(tar.extractfile(member).read(), b"\x7fELF fake binary")
                self.assertIn("README.md", tar.getnames())
            self.assertEqual(open(p1 + ".sha256").read(), f"{d1}  aspen-1.2.3-x86_64-unknown-linux-gnu.tar.gz\n")
            with self.assertRaisesRegex(release.ReleaseError, "unknown target"):
                release.archive("1.2.3", "riscv64gc-unknown-linux-gnu", binary, t, root)


class Formula(unittest.TestCase):
    def sums(self, d, version="1.2.3", skip=None):
        for i, target in enumerate(release.TARGETS):
            if target == skip:
                continue
            name = f"aspen-{version}-{target}.tar.gz"
            open(os.path.join(d, name + ".sha256"), "w").write(f"{str(i) * 64}  {name}\n")

    def test_each_platform_gets_its_own_url_and_checksum(self):
        with tempfile.TemporaryDirectory() as d:
            self.sums(d)
            text = release.formula("1.2.3", d)
            self.assertIn('version "1.2.3"', text)
            for i, target in enumerate(release.TARGETS):
                url = f"{release.DEFAULT_BASE_URL}/v1.2.3/aspen-1.2.3-{target}.tar.gz"
                block = text[text.index(url):]
                self.assertTrue(block.split("\n")[1].strip() == f'sha256 "{str(i) * 64}"',
                                f"{target}'s checksum follows its URL")
            self.assertNotIn("@", text.replace("brew@example.com", ""))
            if shutil.which("ruby"):
                with tempfile.NamedTemporaryFile("w", suffix=".rb") as f:
                    f.write(text)
                    f.flush()
                    subprocess.run(["ruby", "-c", f.name], check=True, capture_output=True)

    def test_a_missing_or_malformed_checksum_is_refused(self):
        with tempfile.TemporaryDirectory() as d:
            self.sums(d, skip="aarch64-apple-darwin")
            with self.assertRaisesRegex(release.ReleaseError, "missing checksum for aarch64-apple-darwin"):
                release.formula("1.2.3", d)
        with tempfile.TemporaryDirectory() as d:
            self.sums(d)
            name = "aspen-1.2.3-x86_64-apple-darwin.tar.gz"
            open(os.path.join(d, name + ".sha256"), "w").write(f"{'0' * 64}  aspen-1.2.2-x86_64-apple-darwin.tar.gz\n")
            with self.assertRaisesRegex(release.ReleaseError, "is not '<sha256>"):
                release.formula("1.2.3", d)
        with tempfile.TemporaryDirectory() as d:
            self.sums(d, version="1.2.3")
            with self.assertRaisesRegex(release.ReleaseError, "not MAJOR.MINOR.PATCH"):
                release.formula("v1.2.3", d)


if __name__ == "__main__":
    unittest.main(verbosity=2)
