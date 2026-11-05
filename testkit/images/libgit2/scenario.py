#!/usr/bin/env python3
"""Client-matrix scenario for libgit2, through its Python binding pygit2.

usage: scenario.py <url> <branch> [<branch-url>]

Same steps and output as ../git/scenario.sh, minus `fsck` (run-matrix.sh runs
it with stock Git afterwards). The second clone goes through <branch-url>
(default <url>): libgit2 fetches with protocol v0, so on Aspen only the
branch-scoped URL advertises <branch>.
"""

import os
import sys
import tempfile

import pygit2


class Callbacks(pygit2.RemoteCallbacks):
    def push_update_reference(self, refname, message):
        # libgit2 reports per-ref rejections here, not as an exception.
        if message:
            sys.exit(f"push of {refname} was rejected: {message}")


def main():
    url, branch = sys.argv[1], sys.argv[2]
    branch_url = sys.argv[3] if len(sys.argv) > 3 else url
    work = tempfile.mkdtemp()
    print(f"client pygit2 {pygit2.__version__} (libgit2 {pygit2.LIBGIT2_VERSION})")

    a = pygit2.clone_repository(url, os.path.join(work, "a"))
    print(f"cloned {a.head.target}")

    with open(os.path.join(a.workdir, "matrix.txt"), "w") as f:
        f.write("pushed by libgit2\n")
    a.index.add("matrix.txt")
    a.index.write()
    sig = pygit2.Signature("Aspen Matrix", "matrix@aspen.invalid")
    head = a.create_commit(
        "HEAD", sig, sig, f"matrix: {branch}", a.index.write_tree(), [a.head.target]
    )
    a.remotes["origin"].push([f"{a.head.name}:refs/heads/{branch}"], callbacks=Callbacks())
    print(f"pushed {head}")

    b = pygit2.clone_repository(branch_url, os.path.join(work, "b"), checkout_branch=branch)
    print(f"fetched {b.head.target}")

    a.remotes["origin"].fetch()


if __name__ == "__main__":
    main()
