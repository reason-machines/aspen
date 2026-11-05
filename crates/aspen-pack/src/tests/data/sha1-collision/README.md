# A known SHA-1 collision (issue #129)

`sha-mbles-1.bin` and `sha-mbles-2.bin` are two different 640-byte messages
with the same plain SHA-1, `8ac60ba76f1999a1ab70223f225aefdc78d4ddc0`. They
are the chosen-prefix collision published by Gaëtan Leurent and Thomas Peyrin
with "SHA-1 is a Shambles" (USENIX Security 2020, <https://sha-mbles.github.io/>):
two OpenPGP key packets with different prefixes, followed by the near-collision
blocks that make their hashes equal.

The copies here are byte-for-byte the test data of the `sha1dc` crate, version
0.1.5 (`tests/data/`, crates.io, licensed MIT OR Apache-2.0), which `gix-hash`
uses for collision detection. SHA-256:

```text
3ead211681cec93d265c8ac123dd062e105408cebf82fa6e2b126f4f40bcb88c  sha-mbles-1.bin
208feafe1c6a95c73f662514ac48761f25e1f3b74922521a98d9ce287f4a2197  sha-mbles-2.bin
```

To check that they collide, hash them with a SHA-1 that has no collision
detection, such as coreutils:

```sh
sha1sum sha-mbles-1.bin sha-mbles-2.bin
```

The tests that use them are in `../../collision.rs`.
