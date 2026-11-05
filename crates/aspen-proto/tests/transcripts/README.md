# v0 upload-pack transcripts

Byte-level recordings of real clients fetching over protocol v0, stateless
smart HTTP, from the `aspen-proto` test server
(`tests/interop/upload_v0.rs`). Each directory is one scenario of one client:

| File | What |
|---|---|
| `client.txt` | The client and its version |
| `manifest.txt` | One line per HTTP request: `<seq> get advertisement`, or `<seq> post <ending>` where the ending is `pack` (negotiation ended, the pack followed), `round` (a round answered without the pack), `header` (a header-only deepen request, answered with the shallow section) or `err` |
| `<seq>-advertisement.pkt` | The `GET info/refs` response body the client read |
| `<seq>-request.pkt` | A `POST git-upload-pack` body exactly as the client sent it (after gzip decoding) |
| `<seq>-response.pkt` | The response body up to the start of the pack: the shallow section, ACK and NAK lines |

The interop test `v0_transcripts_replay` (required CI) rebuilds every
recorded advertisement from the refs, `symref` and agent it shows and checks
it byte for byte, and parses every request
with `upload_v0::RequestParser`, answers it with the shares and `ready`
that the recorded response shows, and checks that the answer is the same
bytes and ends the same way, and that each client asked for the
capabilities it is known to use.

To record fresh transcripts (the object IDs change on every run, since the
test repositories are made at run time):

```bash
python3 -m pip install --target /tmp/pygit2 pygit2   # libgit2 client
(cd tests/clients/go-git && go build -o /tmp/aspen-v0-client .)
ASPEN_PROTO_REQUIRE_CLIENTS=1 PYTHONPATH=/tmp/pygit2 \
ASPEN_GO_GIT_CLIENT=/tmp/aspen-v0-client ASPEN_PROTO_RECORD=/tmp/transcripts \
  cargo test -p aspen-proto --test interop -- v0_ --skip transcripts
```

Then copy the scenario directories you want from `/tmp/transcripts`. The
nightly job `interop-v0-clients` runs the same live tests with both clients
required.
