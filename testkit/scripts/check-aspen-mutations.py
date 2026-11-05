#!/usr/bin/env python3
"""Shows that the differential oracle's run against a real Aspen server
fails when Aspen misbehaves (node L0). Run from the repository root:

    python3 testkit/scripts/check-aspen-mutations.py [L1 L2 ...]

It first runs the oracle against the unmodified `aspen` binary, which must
find no divergence. Then, for each mutation, it applies one or more exact
text replacements to Aspen's source, rebuilds `aspen`, and runs the same
fixed seeds, which must now report a divergence (exit status 1 and a
"diverged" report). The source file is restored afterwards, and `aspen` is
rebuilt unmodified at the end. A mutation whose pattern no longer matches
is reported, so the list fails loudly when the code it targets changes.

Each mutation takes a few minutes: a change to aspen-core rebuilds most of
the workspace."""
import subprocess, sys

PUSH = 'crates/aspen-server/src/receive/push.rs'
REFS = 'crates/aspen-core/src/refs.rs'
COVERS = 'crates/aspen-planner/src/covers.rs'

# Every run: the same fixed seeds, pushes and races through the plain
# repository URL too (issue #135), nothing stepped over, no shrinking (the
# first divergence is enough here).
DIFF = ['target/debug/aspen-testkit', 'diff', '--candidate', 'aspen',
        '--aspen-bin', 'target/debug/aspen',
        '--plain-url-pushes',
        '--seed', '0xa5be0000', '--sequences', '4', '--steps', '12',
        '--shrink-budget', '0']

muts = [
    ("L1 a push stores its pack minus one object (the last entry it keeps)", PUSH,
     [("        let whole = spill.is_none() && !keep.is_empty() && keep.iter().all(|k| *k);",
       "        let mut keep = keep;\n"
       "        if keep.iter().filter(|k| **k).count() > 1 {\n"
       "            let last = keep.iter().rposition(|k| *k).unwrap();\n"
       "            keep[last] = false;\n"
       "        }\n"
       "        let whole = spill.is_none() && !keep.is_empty() && keep.iter().all(|k| *k);")]),
    # A racing push carries the tip its clone saw as its old value; once
    # another racer has moved the branch, that old value is stale. Without
    # the check, the stale push counts as an explicit force and overwrites
    # the winner: two racers acknowledged, one update lost.
    ("L2 the stale-old check is skipped (a racing push overwrites the winner)", REFS,
     [("    if let Some(old) = old\n        && Some(old) != cur\n    {",
       "    if let Some(old) = old\n        && false\n        && Some(old) != cur\n    {")]),
    ("L3 a fetch plan leaves out one needed pack (the oldest)", COVERS,
     [("        packs: packs.into_iter().collect(),",
       "        packs: {\n"
       "            let n = packs.len();\n"
       "            packs.into_iter().skip(usize::from(n > 1)).collect()\n"
       "        },")]),
    # Issue #135: a push of a topic branch through the plain repository URL
    # carries a zero old value, so Aspen alone decides whether it is a
    # fast-forward. With this fault a stale push that is not one is
    # accepted as a force without `-o force`: an acknowledged update lost.
    ("L4 a non-fast-forward through the plain URL is accepted without force", REFS,
     [("                Err(reason) => VerdictKind::Reject(reason),",
       "                Err(_) => VerdictKind::Forced,")]),
]


def build():
    subprocess.run(['cargo', 'build', '--quiet', '-p', 'aspen-cli', '-p', 'aspen-testkit'],
                   check=True)


def diff():
    p = subprocess.run(DIFF, capture_output=True, text=True)
    return p.returncode, p.stdout + p.stderr


def summary(out):
    keep = [l for l in out.splitlines()
            if l.startswith(('diff:', 'differential oracle', '  first divergence', '  what'))]
    return '\n'.join('    ' + l for l in keep[-6:])


def main():
    wanted = set(sys.argv[1:])
    failures = []
    build()
    code, out = diff()
    print(f"baseline (unmodified aspen): exit {code}\n{summary(out)}")
    if code != 0:
        sys.exit("the unmodified server must not diverge; fix that first")
    for name, path, reps in muts:
        if wanted and name.split()[0] not in wanted:
            continue
        original = open(path).read()
        mutated = original
        missing = [a for a, _ in reps if mutated.count(a) != 1]
        if missing:
            failures.append(f"{name}: pattern not found exactly once: {missing[0][:60]!r}")
            continue
        for a, b in reps:
            mutated = mutated.replace(a, b)
        try:
            open(path, 'w').write(mutated)
            build()
            code, out = diff()
        finally:
            open(path, 'w').write(original)
        caught = code == 1 and 'diverged' in out
        print(f"{'CAUGHT ' if caught else 'MISSED '} {name}: exit {code}\n{summary(out)}")
        if not caught:
            failures.append(name)
    build()
    if failures:
        print('\nnot caught:\n  ' + '\n  '.join(failures))
        sys.exit(1)
    print('\nevery mutation was caught')


if __name__ == '__main__':
    main()
