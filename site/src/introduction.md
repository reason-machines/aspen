# Aspen

Aspen is a Git server for repositories that many agents, CI jobs and people
push to and fetch from at the same time. You run it yourself: one binary,
PostgreSQL, and any S3-compatible bucket (or SQLite and a local directory,
on one machine).

It works with stock `git` over HTTP and SSH. No client changes are needed.

> **Status: pre-alpha.** Aspen is not released yet. There are no published
> binaries or images, and the public demo is not running. Aspen is licensed
> under the Apache License 2.0. Everything on this site describes the code on the `main` branch.
> The [limitations](limitations.md) page lists what is missing.

## Why another Git server

A classic Git server keeps each repository as a directory of loose objects
and packs on one disk. Every push takes a lock on the repository, and every
fetch makes the server build a new pack for that client. With a thousand
agents pushing to their own branches and CI fetching after each push, the
server spends its time on locks and on compressing the same objects again
and again.

Aspen follows the design OpenAI described for its internal Git service,
Pando ("[Scaling Git at OpenAI](https://openai.com/index/scaling-git-at-openai/)"):

- **Packs go to object storage as they arrive.** A push is checked,
  completed into a self-contained pack and uploaded. It is never unpacked
  into loose objects.
- **Branches live in a database.** Each branch has its own history of
  versions in PostgreSQL, and a push to one branch never waits for a push
  to another. The only contended write is a compare-and-swap on one
  branch's tip.
- **A push sees only the trunk branch.** The server advertises trunk and
  nothing else, so what a client reads before pushing stays small however
  many branches the repository has.
- **Fetches select stored packs.** The server picks the packs that hold
  what the client is missing and streams them, joined into one pack, or
  hands out URLs so the client downloads them from the bucket or a CDN.
  It does not build a pack per client.
- **Small packs are merged in the background,** in levels, like an LSM
  tree, so a fetch needs few packs however many pushes came before it.

The [architecture](architecture.md) page explains each part.

## Two ways to run it

- **As your Git host.** Point clients at Aspen and push to it.
- **In front of GitHub or GitLab** ([mirror mode](reference/mirror.md)).
  Fetches are served from Aspen's own packs; pushes are forwarded to the
  upstream, either before Aspen accepts them or in the background. Nobody
  has to migrate.

## Where to start

- [Run it yourself in 5 minutes](getting-started.md) on your own machine.
- [Using Git with Aspen](using-git.md): what is different for clients,
  in particular force pushes and deletes.
- [Self-hosting](self-hosting.md): Docker Compose, Kubernetes, AWS, Google
  Cloud, Azure and Hetzner.
