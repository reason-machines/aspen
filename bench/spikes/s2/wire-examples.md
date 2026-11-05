# Spike S2: decisive wire excerpts

These lines are copied from the transcripts that `run.py` writes to
`results/<client>/<scenario>.md`. The transcripts are not committed;
`./run.py --out results ...` regenerates them in about two minutes (see
[README.md](README.md)). Excerpts are from git 2.56.0 unless noted; the
matrix shows the same outcome for 2.30.9, 2.39.5 and OpenAI's fork. Object
IDs are stable across runs because commit dates are fixed. Ports and repo
names in URLs are omitted.

`git>` lines are what the client sent (`GIT_TRACE_PACKET`). Capability
lists after `\0` are shortened to `...`.

## Updating a hidden branch sends a zero old-oid (02)

Second push of `feature`, which the server already has at `7e37a29`:

```text
git> 0000000000000000000000000000000000000000 8f60919bd7f980c9c6f874f741514559977ac10c refs/heads/feature\0 ...
remote: aspen: refs/heads/feature: update: fast-forward (checked server-side; branch was not advertised)
 * [new branch]      feature -> feature
```

Pack: 8 objects, 4 of them already on the server. The client re-sends every
commit since trunk.

## `--force` is invisible (04 vs 05)

`git push origin feature` and `git push --force origin feature` both send:

```text
git> 0000000000000000000000000000000000000000 9304a397f31c60df3c5c7312b8493f6639858c55 refs/heads/feature\0 report-status side-band-64k quiet object-format=sha1 agent=git/2.56.0-Linux
```

and the same pack (4 objects, 402 B). Diffing the two packet traces shows
only the client-internal refspec line (`refs/heads/feature:...` vs
`+refs/heads/feature:...`) and the position of the `PACK ...` marker in
the log. Nothing the server receives differs.

## Short-name delete and every lease are refused by the client (08, 10, 11)

Nothing is sent to the server in any of these:

```text
$ git push origin --delete feature
error: unable to delete 'feature': remote ref does not exist

$ git push --force-with-lease=feature:72c7a243b7112e014d80b61fafeafffe2d873948 origin feature
 ! [rejected]        feature -> feature (stale info)

$ git push --force-with-lease origin feature
 ! [rejected]        feature -> feature (stale info)
```

## Full-name delete: zero old-oid, so gated (09, 16)

```text
$ git push origin :refs/heads/feature
git> 0000000000000000000000000000000000000000 0000000000000000000000000000000000000000 refs/heads/feature\0 ...
remote: aspen: refs/heads/feature: rejected: delete of a ref this client has not seen needs -o force, -o lease=<ref>:<oid>, or the branch-scoped URL

$ git push -o force origin :refs/heads/feature
git> force
remote: aspen: refs/heads/feature: delete: unconditional delete accepted because of -o force
 - [deleted]         feature

$ git push -o lease=refs/heads/feature:faa9b70bb081ea59efd6b23d61a40fbba7f41dd9 origin --delete refs/heads/feature
git> lease=refs/heads/feature:faa9b70bb081ea59efd6b23d61a40fbba7f41dd9
remote: aspen: refs/heads/feature: delete: compare-and-swap delete (old-oid matched)
 - [deleted]         feature
```

With `--atomic`, the gated delete takes the create down with it (16):

```text
$ git push --atomic origin a :refs/heads/old
 ! [remote rejected] a -> a (atomic push failed)
 ! [remote rejected] old (delete of a ref this client has not seen needs -o force, ...)
```

## Lease as a push option (12, 13)

```text
$ git push -o lease=refs/heads/feature:72c7a243b7112e014d80b61fafeafffe2d873948 origin feature
git> lease=refs/heads/feature:72c7a243b7112e014d80b61fafeafffe2d873948
remote: aspen: refs/heads/feature: forced: non-fast-forward with matching old-oid (client forced or leased)

# same lease after someone else moved the branch
remote: aspen: refs/heads/feature: rejected: stale lease (expected 72c7a243b711, server has db0b18643a14)
```

## Branch-scoped URL gives the client a real old-oid (24)

Advertisement at `/<repo>.git/-/refs/heads/feature`, then stock
`--force-with-lease` and `--delete feature`:

```text
72c7a243b7112e014d80b61fafeafffe2d873948 refs/heads/feature \0 ...
git> 72c7a243b7112e014d80b61fafeafffe2d873948 8bc21841272d4c88fb70d44987211668ed3ebc55 refs/heads/feature\0 ...
 + 72c7a24...8bc2184 feature -> feature (forced update)
git> 8bc21841272d4c88fb70d44987211668ed3ebc55 0000000000000000000000000000000000000000 refs/heads/feature\0 ...
 - [deleted]         feature
```

## `.have` lines stop a stale client from resending history (19 vs 20)

The client is 20 commits behind trunk (one push of 20 commits, so trunk has
one earlier ref version to offer):

```text
60179987eec450865d6092a082673eeb24cd50b1 refs/heads/main \0 ...
962438447fa4c4f1e6de04b91d56d1618e0b21bd .have
```

Without the `.have` line: 16 objects, 2401 B, 12 already on the server. With
it: 4 objects, 386 B, none already on the server.

## Trunk rewound between advertisement and push (27)

Server's view of the pack (`allow_force = all`, trunk reset to `main~1`
after the GET):

```text
"pack_header_objects": 4,
"thin_bases_appended": 2,
"objects_already_on_server": 2,
"objects_needed_beyond_trunk_not_in_pack": 2,
```

Two objects the pack depends on are no longer reachable from trunk. A
connectivity check against the dependency rejects this push.

## Thin vs `no-thin` (29, 30)

Same one-line edit to a 200-line trunk file:

```text
29-thin:    "pack_bytes": 413, "thin_bases_appended": 1
30-no-thin: "pack_bytes": 806, "thin_bases_appended": 0
```

## `explicit-haves` (25, OpenAI's fork only)

```text
[Git-Protocol: explicit-haves]
72c7a243b7112e014d80b61fafeafffe2d873948 refs/heads/feature \0 ... explicit-haves
```

The stock clients do not send the header, get a trunk-only advertisement,
and refuse the lease as in 10.
