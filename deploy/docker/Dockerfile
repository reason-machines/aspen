# syntax=docker/dockerfile:1@sha256:4edf897a3ffa55b89f906fc8cc78afdb3f1834cc9c7083565e611a8a7d5fe99e
#
# Container image for the `aspen` binary, and (target `place`) for the
# `place` demo's back end with its built page.
#
# Build from the repository root (the build context must be the whole
# workspace):
#
#   docker build -f deploy/docker/Dockerfile -t aspen .
#   docker run --rm aspen --version
#   docker run --rm aspen serve --help
#
#   docker build -f deploy/docker/Dockerfile --target place -t aspen-place .
#
# The runtime image is Debian slim with CA certificates, runs as an
# unprivileged user (uid 10001) and contains only the `aspen` binary. It runs
# `aspen serve` by default, configured from the environment
# (deploy/README.md); `aspen healthcheck` is its health check, since the
# image has no curl or wget.

# Matches rust-toolchain.toml (deploy/scripts/check-rust-version.sh).
ARG RUST_VERSION=1.99.1
# The base images, each pinned to the digest of its multi-arch index, so a
# build of the same commit always starts from the same bytes (issue #125).
# The tag before each digest says what it is; deploy/scripts/check-image-pins.py
# (required CI) checks every FROM, and that the tags agree with RUST_VERSION,
# demo/place/.nvmrc and one Debian release. To bump one, change its tag and
# take the new digest from `docker buildx imagetools inspect <image>:<tag>`.
ARG RUST_IMAGE=rust:1.99.1-slim-trixie@sha256:ea0999ff67355d958b8799b6751c879a6d770076d26047dca66f6e51e77930d3
ARG DEBIAN_IMAGE=debian:trixie-slim@sha256:f6b08b09a05725f9431502d3b5714f5c4576e82c1a84fa1debe0c24a7e0c3ec4
ARG NODE_IMAGE=node:22.23.3-trixie-slim@sha256:28d03803f09ba76560e273af33beb7eda4ed68fa0698bc9bd15e9a6e35693923

FROM ${RUST_IMAGE} AS build
# Use the image's toolchain as-is instead of letting rust-toolchain.toml pull
# in extra components (clippy, rustfmt) that a release build does not need.
ARG RUST_VERSION
ENV RUSTUP_TOOLCHAIN=${RUST_VERSION} \
    CARGO_TERM_COLOR=never
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/src/target,sharing=locked \
    cargo build --release --locked --package aspen-cli --bin aspen \
 && install -D -m 0755 target/release/aspen /out/aspen

# What every Aspen image ships: Debian slim with CA certificates, an
# unprivileged user and a spool directory, but no binary yet.
FROM ${DEBIAN_IMAGE} AS base
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --system --uid 10001 --user-group --home-dir /var/lib/aspen --create-home \
      --shell /usr/sbin/nologin aspen \
 && install -d -o 10001 -g 10001 -m 0750 /var/lib/aspen/spool
# The push spool must be a real disk (never tmpfs): mount a volume here.
# /-/metrics stays on the binary's default, 127.0.0.1:9464, inside the
# container: a deployment that scrapes it sets ASPEN_METRICS_LISTEN to
# 0.0.0.0:9464 itself (compose and the Helm chart do) and keeps the port off
# any public load balancer (docs/OBSERVABILITY.md).
ENV ASPEN_LISTEN=0.0.0.0:7070 \
    ASPEN_SPOOL_DIR=/var/lib/aspen/spool
USER 10001:10001
WORKDIR /var/lib/aspen
# 7070: Git over HTTP. 2222: Git over SSH, when ASPEN_SSH_LISTEN and a host
# key are set (docs/SSH.md).
EXPOSE 7070 2222
ENTRYPOINT ["/usr/local/bin/aspen"]
CMD ["serve"]

# The place demo (demo/place): the page, built with Node, and aspen-place,
# which serves it beside its API and canvas.git (PLACE_WEB_DIR).
FROM ${NODE_IMAGE} AS place-web
WORKDIR /src/demo/place
COPY demo/place/package.json demo/place/package-lock.json ./
RUN --mount=type=cache,target=/root/.npm,sharing=locked \
    npm ci --no-audit --no-fund
COPY demo/place/ ./
RUN npm run build

FROM build AS place-build
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/src/target,sharing=locked \
    cargo build --release --locked --package aspen-place --bin aspen-place \
 && install -D -m 0755 target/release/aspen-place /out/aspen-place

# The canvas lives in PostgreSQL and an S3 bucket in production
# (PLACE_DATABASE_URL, PLACE_BLOB_URL); without them, under
# /var/lib/aspen/place. Pushes spool under /var/lib/aspen/spool.
FROM base AS place
COPY --from=place-build /out/aspen-place /usr/local/bin/aspen-place
COPY --from=place-web /src/demo/place/dist /usr/share/aspen-place/web
ENV PLACE_LISTEN=0.0.0.0:7080 \
    PLACE_WEB_DIR=/usr/share/aspen-place/web \
    PLACE_DATA_DIR=/var/lib/aspen/place \
    PLACE_SPOOL_DIR=/var/lib/aspen/spool
EXPOSE 7080
ENTRYPOINT ["/usr/local/bin/aspen-place"]
CMD ["serve"]

# Release images (.github/workflows/release.yml): the binary was built and
# tested per architecture before, and is passed in as a named build context,
# so a multi-arch image needs no emulated compile:
#
#   docker buildx build -f deploy/docker/Dockerfile --target release \
#     --build-context dist=dist --platform linux/amd64,linux/arm64 .
#
# where dist/linux-amd64/aspen and dist/linux-arm64/aspen are the binaries.
FROM base AS release
ARG TARGETARCH
COPY --from=dist --chmod=0755 linux-${TARGETARCH}/aspen /usr/local/bin/aspen

# The default target: compile from this checkout (compose, local builds, CI).
FROM base AS runtime
COPY --from=build /out/aspen /usr/local/bin/aspen
