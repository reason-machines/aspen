#!/bin/sh
# Build the Git clients used by spike S2 into $PREFIX (default ./clients).
#
#   ./build-clients.sh            # git 2.30.9, git 2.56.0, openai/git (codex branch)
#
# Needs a C toolchain, make, curl, and the libcurl and zlib development headers
# (Debian/Ubuntu: libcurl4-openssl-dev zlib1g-dev). If the headers live in a
# non-standard place, pass EXTRA_CFLAGS / EXTRA_LDFLAGS.
#
# The system git is used as a fourth client; the spike was run with 2.39.5.
set -eu

PREFIX=${PREFIX:-$(pwd)/clients}
SRC=${SRC:-$(pwd)/clients-src}
mkdir -p "$PREFIX" "$SRC"

build() { # build <srcdir> <prefix>
	make -C "$1" -j"$(nproc)" prefix="$2" \
		CFLAGS="-O2 ${EXTRA_CFLAGS:-}" LDFLAGS="${EXTRA_LDFLAGS:-}" \
		CURL_LDFLAGS=-lcurl CURL_CONFIG=false \
		NO_GETTEXT=1 NO_TCLTK=1 NO_PERL=1 NO_PYTHON=1 NO_EXPAT=1 NO_OPENSSL=1 \
		NO_INSTALL_HARDLINKS=1 install >"$2.log" 2>&1
	"$2/bin/git" --version
}

for v in 2.30.9 2.56.0; do
	[ -d "$SRC/git-$v" ] || curl -fsSL "https://github.com/git/git/archive/refs/tags/v$v.tar.gz" | tar xz -C "$SRC"
	build "$SRC/git-$v" "$PREFIX/$v"
done

# OpenAI's fork carries the explicit-haves capability (see RESULT.md).
[ -d "$SRC/openai-git" ] || git clone -q --depth 1 https://github.com/openai/git "$SRC/openai-git"
build "$SRC/openai-git" "$PREFIX/openai"
