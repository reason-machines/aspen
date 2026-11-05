# JGit's command-line front end, for the client matrix.
FROM eclipse-temurin:21-jre
ARG JGIT_VERSION=7.8.0.202609011348-r
ARG JGIT_SHA256=0a3a1d2cc7999e427193f8f81edba43a3f602517c91d0923ec4bff10093a6265
RUN curl -fsSL -o /usr/local/bin/jgit \
      "https://repo1.maven.org/maven2/org/eclipse/jgit/org.eclipse.jgit.pgm/${JGIT_VERSION}/org.eclipse.jgit.pgm-${JGIT_VERSION}.sh" \
 && echo "${JGIT_SHA256}  /usr/local/bin/jgit" | sha256sum -c - \
 && chmod 755 /usr/local/bin/jgit \
 && jgit --version
COPY scenario.sh /usr/local/bin/aspen-scenario
RUN chmod 755 /usr/local/bin/aspen-scenario
ENTRYPOINT ["aspen-scenario"]
