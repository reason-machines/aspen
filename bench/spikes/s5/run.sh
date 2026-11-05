#!/usr/bin/env bash
# Run the S5 matrix against one or more git binaries.
#
#   bench/spikes/s5/run.sh [GIT ...]
#
# With no arguments it tests the `git` on PATH. The server side is always the
# `git http-backend` on PATH, behind capproxy.py. Results go to
# bench/spikes/s5/results/ (matrix.md, matrix.json, transcripts/,
# requests.jsonl). Only matrix.md and wire-examples.md are committed; the
# rest is gitignored.
#
# To test another git release without installing it system-wide:
#
#   curl -sSL https://github.com/git/git/archive/refs/tags/v2.30.0.tar.gz | tar xz
#   make -C git-2.30.0 -j"$(nproc)" prefix=/opt/git-2.30.0 \
#       NO_GETTEXT=1 NO_TCLTK=1 NO_PERL=1 NO_PYTHON=1 install
#   bench/spikes/s5/run.sh git /opt/git-2.30.0/bin/git
#
# The build needs zlib and libcurl headers (zlib1g-dev, libcurl4-openssl-dev
# on Debian) so that git-remote-http is built.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
[ $# -eq 0 ] && set -- git
args=()
for g in "$@"; do args+=(--git "$g"); done
exec python3 "$here/matrix.py" "${args[@]}" --out "$here/results"
