# libgit2 through pygit2 (its wheels bundle libgit2), for the client matrix.
FROM python:3.13-slim
ARG PYGIT2_VERSION=1.20.1
RUN pip install --no-cache-dir "pygit2==${PYGIT2_VERSION}" \
 && python -c 'import pygit2; print(pygit2.LIBGIT2_VERSION)'
COPY scenario.py /usr/local/bin/aspen-scenario
RUN chmod 755 /usr/local/bin/aspen-scenario
ENTRYPOINT ["aspen-scenario"]
