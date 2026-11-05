#!/usr/bin/env bash
# Install every client in the S1 matrix into $ASPEN_S1_CACHE without root or Docker.
#
#   ./setup.sh            # everything
#   ./setup.sh git go     # only some components: git | go | java | rust
#
# Needs: curl, tar, xz, make, a C compiler, cargo (for the Rust harness).
# Everything else (zlib, Go, a JDK, JGit) is downloaded.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
CACHE="${ASPEN_S1_CACHE:-$HOME/.cache/aspen-s1}"
JOBS="${JOBS:-$(nproc)}"
mkdir -p "$CACHE/src" "$CACHE/dl"

# Pinned versions. Change these, rerun, and the matrix picks them up.
GIT_VERSIONS="${GIT_VERSIONS:-2.30.9 2.39.5 2.43.7 2.47.3 2.56.0}"
ZLIB_VERSION=1.3.2
GO_VERSION=go1.27.3
JDK_URL="https://github.com/adoptium/temurin21-binaries/releases/download/jdk-21.0.13%2B9/OpenJDK21U-jdk_x64_linux_hotspot_21.0.13_9.tar.gz"
JGIT_VERSION=7.8.0.202609011348-r

fetch() { # url dest
  [ -s "$2" ] || curl -fsSL --retry 3 -o "$2.part" "$1" && { [ -s "$2" ] || mv "$2.part" "$2"; }
}

setup_zlib() {
  [ -f "$CACHE/zlib/lib/libz.a" ] && return
  fetch "https://zlib.net/zlib-$ZLIB_VERSION.tar.gz" "$CACHE/dl/zlib.tar.gz"
  rm -rf "$CACHE/src/zlib-$ZLIB_VERSION"
  tar -C "$CACHE/src" -xzf "$CACHE/dl/zlib.tar.gz"
  (cd "$CACHE/src/zlib-$ZLIB_VERSION" && ./configure --static --prefix="$CACHE/zlib" >/dev/null && make -j"$JOBS" >/dev/null && make install >/dev/null)
}

setup_git() {
  setup_zlib
  for v in $GIT_VERSIONS; do
    [ -x "$CACHE/git-$v/bin/git" ] && { echo "git $v: cached"; continue; }
    echo "git $v: building"
    fetch "https://mirrors.edge.kernel.org/pub/software/scm/git/git-$v.tar.xz" "$CACHE/dl/git-$v.tar.xz"
    rm -rf "$CACHE/src/git-$v"
    tar -C "$CACHE/src" -xJf "$CACHE/dl/git-$v.tar.xz"
    # Transport does not matter for pack acceptance (the harness drives fetch
    # through --upload-pack over file://), so build without curl/expat/openssl.
    make -C "$CACHE/src/git-$v" -j"$JOBS" prefix="$CACHE/git-$v" \
      ZLIB_PATH="$CACHE/zlib" NO_CURL=1 NO_EXPAT=1 NO_OPENSSL=1 NO_GETTEXT=1 \
      NO_TCLTK=1 NO_PERL=1 NO_PYTHON=1 NO_ICONV=1 NO_REGEX=NeedsStartEnd \
      CFLAGS="-O2 -w" install >"$CACHE/src/git-$v.build.log" 2>&1 \
      || { tail -30 "$CACHE/src/git-$v.build.log"; exit 1; }
    rm -rf "$CACHE/src/git-$v"
  done
}

setup_go() {
  [ -x "$CACHE/go/bin/go" ] || {
    fetch "https://go.dev/dl/$GO_VERSION.linux-amd64.tar.gz" "$CACHE/dl/go.tar.gz"
    tar -C "$CACHE" -xzf "$CACHE/dl/go.tar.gz"
  }
  (cd "$HERE/harness-go" && GOPATH="$CACHE/gopath" GOFLAGS=-mod=mod "$CACHE/go/bin/go" build -o "$CACHE/bin/s1-gogit" .)
}

setup_java() {
  [ -x "$CACHE/jdk/bin/java" ] || {
    fetch "$JDK_URL" "$CACHE/dl/jdk.tar.gz"
    mkdir -p "$CACHE/jdk" && tar -C "$CACHE/jdk" --strip-components=1 -xzf "$CACHE/dl/jdk.tar.gz"
  }
  mkdir -p "$CACHE/jgit"
  local m=https://repo1.maven.org/maven2
  fetch "$m/org/eclipse/jgit/org.eclipse.jgit/$JGIT_VERSION/org.eclipse.jgit-$JGIT_VERSION.jar" "$CACHE/jgit/jgit.jar"
  fetch "$m/org/slf4j/slf4j-api/2.0.16/slf4j-api-2.0.16.jar" "$CACHE/jgit/slf4j-api.jar"
  fetch "$m/org/slf4j/slf4j-nop/2.0.16/slf4j-nop-2.0.16.jar" "$CACHE/jgit/slf4j-nop.jar"
  fetch "$m/com/googlecode/javaewah/JavaEWAH/1.2.3/JavaEWAH-1.2.3.jar" "$CACHE/jgit/javaewah.jar"
  fetch "$m/commons-codec/commons-codec/1.17.1/commons-codec-1.17.1.jar" "$CACHE/jgit/commons-codec.jar"
  local cp="$CACHE/jgit/jgit.jar:$CACHE/jgit/slf4j-api.jar:$CACHE/jgit/slf4j-nop.jar:$CACHE/jgit/javaewah.jar:$CACHE/jgit/commons-codec.jar"
  mkdir -p "$CACHE/jgit/classes"
  "$CACHE/jdk/bin/javac" -d "$CACHE/jgit/classes" -cp "$cp" "$HERE/harness-java/S1JGit.java"
  mkdir -p "$CACHE/bin"
  cat >"$CACHE/bin/s1-jgit" <<EOF
#!/bin/sh
exec "$CACHE/jdk/bin/java" -cp "$CACHE/jgit/classes:$cp" S1JGit "\$@"
EOF
  chmod +x "$CACHE/bin/s1-jgit"
}

setup_rust() {
  (cd "$HERE/harness-rust" && CARGO_TARGET_DIR="$CACHE/rust-target" cargo build --release --quiet)
  mkdir -p "$CACHE/bin"
  cp "$CACHE/rust-target/release/s1-rust" "$CACHE/bin/s1-rust"
}

parts=("$@")
[ ${#parts[@]} -eq 0 ] && parts=(git go java rust)
for p in "${parts[@]}"; do "setup_$p"; done
echo "S1 clients installed in $CACHE"
