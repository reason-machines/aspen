# gitoxide's `gix` tool (static musl release build), for the client matrix.
FROM debian:bookworm-slim
ARG GITOXIDE_VERSION=v0.61.0
ARG GITOXIDE_SHA256=ffbaf6cfc6a7e3bf0534f72841741a81f2ae995145d6ab8df709c590b53d678d
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl \
 && rm -rf /var/lib/apt/lists/*
RUN name="gitoxide-max-pure-${GITOXIDE_VERSION}-x86_64-unknown-linux-musl" \
 && curl -fsSL -o /tmp/gix.tar.gz \
      "https://github.com/GitoxideLabs/gitoxide/releases/download/${GITOXIDE_VERSION}/${name}.tar.gz" \
 && echo "${GITOXIDE_SHA256}  /tmp/gix.tar.gz" | sha256sum -c - \
 && tar -xzf /tmp/gix.tar.gz -C /tmp \
 && install -m 755 "/tmp/${name}/gix" /usr/local/bin/gix \
 && rm -rf /tmp/gix.tar.gz "/tmp/${name}" \
 && gix --version
COPY scenario.sh /usr/local/bin/aspen-scenario
RUN chmod 755 /usr/local/bin/aspen-scenario
ENTRYPOINT ["aspen-scenario"]
