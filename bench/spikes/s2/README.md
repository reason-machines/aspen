# Spike S2: push semantics with a trunk-only advertisement

Throwaway code that answers one question for the Aspen plan: what stock Git
clients do when a server's push advertisement shows only the trunk branch,
and which server rules keep creates, updates, force pushes, deletes,
`--force-with-lease` and atomic pushes correct.

- [RESULT.md](RESULT.md): findings and decisions. Start here.
- [results/matrix.md](results/matrix.md): outcome of every push step for every
  client, plus pack contents.
- [wire-examples.md](wire-examples.md): the decisive lines from the
  transcripts, quoted.
- `results/<client>/<scenario>.md`: full transcripts (client output, what the
  server advertised, saw and decided, and the client's packet trace). About
  120 files, so they are gitignored, not committed. `run.py` regenerates
  them in about two minutes (below).
- [server.py](server.py): the spike server (Python 3, standard library only;
  shells out to `git`).
- [run.py](run.py): the scenario runner.
- [build-clients.sh](build-clients.sh): builds the Git versions that were
  tested.

## Run it

Requires Python 3.9+ and a system `git` (used for the server side and for
setup steps).

```sh
./build-clients.sh                     # optional: builds git 2.30.9, 2.56.0, openai/git
./run.py --out results \
  git-2.30=clients/2.30.9/bin/git \
  git-2.39=/usr/bin/git \
  git-2.56=clients/2.56.0/bin/git \
  openai=clients/openai/bin/git
```

`--only 01-create,24-scoped-url` runs a subset. Each scenario gets a fresh
server repository and clone under a temporary directory, which is removed
afterwards. Ports in the transcripts differ between runs; object IDs do not.

`run.py` rewrites `results/matrix.md` (committed) and the per-client
transcripts (gitignored). If the C toolchain lacks the zlib and libcurl
headers, unpack the distribution's `zlib1g-dev` and `libcurl4-openssl-dev`
packages into a directory and pass `EXTRA_CFLAGS=-I<dir>/usr/include
-I<dir>/usr/include/x86_64-linux-gnu EXTRA_LDFLAGS=-L<dir>/usr/lib/x86_64-linux-gnu`
to `build-clients.sh`.

This is not Aspen code and is not built or tested by CI.
