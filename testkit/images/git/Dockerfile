# Stock Git built from the release tarball, for the client matrix.
#
#   docker build --build-arg GIT_VERSION=2.47.3 -t aspen-matrix-git-2.47 testkit/images/git
#
# run-matrix.sh builds one image per version in its client list.
FROM debian:bookworm-slim AS build
ARG GIT_VERSION
# cargo: Git 2.55 and later build their Rust code by default (NO_RUST opts
# out; Rust becomes mandatory in Git 3.0), and make fails without cargo.
# Git's crate has no dependencies and needs Rust 1.49 or later; bookworm's
# is 1.63. The result is a static library, so the runtime stage needs nothing.
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      ca-certificates curl xz-utils make gcc libc6-dev cargo \
      libcurl4-openssl-dev zlib1g-dev libexpat1-dev \
 && rm -rf /var/lib/apt/lists/*
# kernel.org's mirrors sometimes fail or stall for minutes: on 2026-11-05 a
# bare `curl | tar` from one mirror failed four nightly jobs, two of them
# after 18 minutes. So each mirror is tried in turn, three rounds, each
# transfer bounded in time and retried; the tarball goes to a file first, so
# a transfer cut off midway is never extracted.
RUN test -n "$GIT_VERSION" \
 && file="git-${GIT_VERSION}.tar.xz" \
 && for round in 1 2 3; do \
      for host in mirrors.edge.kernel.org cdn.kernel.org; do \
        if curl -fsSL --connect-timeout 20 --max-time 300 --retry 2 --retry-all-errors \
             -o "/tmp/$file" "https://$host/pub/software/scm/git/$file"; then \
          break 2; \
        fi; \
        rm -f "/tmp/$file"; \
        echo "round $round: downloading $file from $host failed" >&2; \
      done; \
      if [ "$round" -lt 3 ]; then sleep $((round * 20)); fi; \
    done \
 && if [ ! -s "/tmp/$file" ]; then echo "cannot download $file from kernel.org" >&2; exit 1; fi \
 && mkdir /src \
 && tar -xJf "/tmp/$file" -C /src --strip-components=1 \
 && rm "/tmp/$file"
WORKDIR /src
# NO_OPENSSL: without it Git's build includes <openssl/ssl.h> (imap-send and
# git-compat-util.h), and this stage has no libssl-dev, so make fails on every
# version. HTTPS does not need it: git-remote-https gets TLS from libcurl.
RUN make -j"$(nproc)" prefix=/opt/git \
      NO_TCLTK=1 NO_GETTEXT=1 NO_PERL=1 NO_PYTHON=1 NO_OPENSSL=1 INSTALL_SYMLINKS=1 \
      install

FROM debian:bookworm-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates libcurl4 zlib1g libexpat1 \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /opt/git /opt/git
ENV PATH=/opt/git/bin:$PATH
COPY scenario.sh /usr/local/bin/aspen-scenario
RUN chmod 755 /usr/local/bin/aspen-scenario && git --version
ENTRYPOINT ["aspen-scenario"]
