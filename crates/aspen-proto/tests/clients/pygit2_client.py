"""A libgit2 (pygit2) fetch client for aspen-proto's v0 upload-pack interop
tests (crates/aspen-proto/tests/interop/upload_v0.rs). libgit2 has no
protocol v2, so every fetch it makes is v0.

usage:
    python3 pygit2_client.py clone <url> <dir> [depth]
    python3 pygit2_client.py fetch <dir> [depth]

Prints the client versions on the first line of stdout.
"""

import sys

import pygit2


def main(argv):
    print(f"pygit2 {pygit2.__version__} libgit2 {pygit2.LIBGIT2_VERSION}")
    cmd = argv[0]
    if cmd == "clone":
        url, path = argv[1], argv[2]
        depth = int(argv[3]) if len(argv) > 3 else 0
        pygit2.clone_repository(url, path, depth=depth)
    elif cmd == "fetch":
        path = argv[1]
        depth = int(argv[2]) if len(argv) > 2 else 0
        repo = pygit2.Repository(path)
        repo.remotes["origin"].fetch(depth=depth)
    else:
        raise SystemExit(f"unknown command {cmd!r}")


if __name__ == "__main__":
    main(sys.argv[1:])
