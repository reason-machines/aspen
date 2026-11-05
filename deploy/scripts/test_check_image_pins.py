#!/usr/bin/env python3
"""Tests for check-image-pins.py, on small copies of the files it reads.

    python3 deploy/scripts/test_check_image_pins.py
"""
import importlib.util
import pathlib
import tempfile
import unittest

HERE = pathlib.Path(__file__).parent
spec = importlib.util.spec_from_file_location("pins", HERE / "check-image-pins.py")
pins = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pins)

D = "@sha256:" + "a" * 64
DOCKERFILE = f"""# syntax=docker/dockerfile:1{D}
ARG RUST_VERSION=1.99.1
ARG RUST_IMAGE=rust:1.99.1-slim-trixie{D}
ARG DEBIAN_IMAGE=debian:trixie-slim{D}
ARG NODE_IMAGE=node:22.23.3-trixie-slim{D}
FROM ${{RUST_IMAGE}} AS build
FROM ${{DEBIAN_IMAGE}} AS base
FROM ${{NODE_IMAGE}} AS web
FROM base AS runtime
"""
COMPOSE = f"""services:
  aspen:
    image: ${{ASPEN_IMAGE:-aspen:local}}
    build:
      context: ../..
  postgres:
    image: ${{POSTGRES_IMAGE:-postgres:17-alpine{D}}}
"""
TF = f'''variable "minio_image" {{
  description = "MinIO image, e.g. cgr.dev/chainguard/minio:latest"
  type        = string
  default     = "cgr.dev/chainguard/minio:latest{D}"
}}

variable "server_image" {{
  default = "ubuntu-24.04"
}}

locals {{
  image = "public.ecr.aws/docker/library/busybox:1.37{D}"
}}
'''


class CheckImagePins(unittest.TestCase):
    def tree(self, dockerfile=DOCKERFILE, compose=COMPOSE, tf=TF, nvmrc="22.23.3\n"):
        d = tempfile.TemporaryDirectory()
        self.addCleanup(d.cleanup)
        root = pathlib.Path(d.name)
        for rel, text in [("deploy/docker/Dockerfile", dockerfile), ("deploy/compose/compose.yaml", compose),
                          ("deploy/terraform/m/main.tf", tf), ("demo/place/.nvmrc", nvmrc),
                          ("deploy/terraform/m/tests/t.tftest.hcl", 'x = "registry.example.test/a:1"\n')]:
            (root / rel).parent.mkdir(parents=True, exist_ok=True)
            (root / rel).write_text(text)
        return pins.check(root)

    def assert_one(self, problems, text):
        self.assertEqual(len(problems), 1, problems)
        self.assertIn(text, problems[0])

    def test_pinned_files_pass(self):
        self.assertEqual(self.tree(), [])

    def test_an_unpinned_from_is_refused(self):
        self.assert_one(self.tree(dockerfile=DOCKERFILE.replace("FROM base AS runtime", "FROM alpine:3 AS x")),
                        "FROM alpine:3 is not pinned")

    def test_an_arg_without_a_digest_is_refused(self):
        self.assert_one(self.tree(dockerfile=DOCKERFILE.replace(f"debian:trixie-slim{D}", "debian:trixie-slim")),
                        "ARG DEBIAN_IMAGE must default to an image pinned")

    def test_an_unpinned_syntax_frontend_is_refused(self):
        self.assert_one(self.tree(dockerfile=DOCKERFILE.replace(f"dockerfile:1{D}", "dockerfile:1")),
                        "`# syntax=` frontend")

    def test_the_rust_image_follows_rust_version(self):
        self.assert_one(self.tree(dockerfile=DOCKERFILE.replace("RUST_VERSION=1.99.1", "RUST_VERSION=1.100.0")),
                        "but RUST_VERSION is 1.100.0")

    def test_the_node_image_follows_nvmrc(self):
        self.assert_one(self.tree(nvmrc="24.1.0\n"), "but demo/place/.nvmrc is 24.1.0")

    def test_one_debian_release(self):
        self.assert_one(self.tree(dockerfile=DOCKERFILE.replace("debian:trixie-slim", "debian:bookworm-slim")),
                        "mix Debian releases")

    def test_compose_defaults_are_pinned_except_local_builds(self):
        self.assert_one(self.tree(compose=COMPOSE.replace(D, "")), "image postgres:17-alpine is not pinned")

    def test_terraform_defaults_and_literals_are_pinned(self):
        problems = self.tree(tf=TF.replace(f"latest{D}", "latest").replace(f"1.37{D}", "1.37"))
        self.assertEqual(len(problems), 3, problems)  # minio's default (variable and literal), busybox
        self.assertTrue(any("variable minio_image" in p for p in problems), problems)
        self.assertTrue(any("busybox:1.37 is not pinned" in p for p in problems), problems)


if __name__ == "__main__":
    unittest.main()
