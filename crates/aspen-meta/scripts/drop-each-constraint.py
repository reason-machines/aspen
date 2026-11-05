#!/usr/bin/env python3
"""Shows that tests/schema.rs notices the loss of every schema constraint.

For each named constraint and unique index in each migration file, this script
deletes it from that file, runs the schema test for that dialect, records
whether the test failed and why, and restores the file. Any constraint whose
deletion leaves the test green is reported as SURVIVED and the script exits
non-zero.

Run it from the repository root:

    python3 crates/aspen-meta/scripts/drop-each-constraint.py postgres
    python3 crates/aspen-meta/scripts/drop-each-constraint.py sqlite
    python3 crates/aspen-meta/scripts/drop-each-constraint.py sqlite 0002_tokens.sql

The Postgres run needs PostgreSQL binaries (see tests/schema.rs and
ASPEN_TEST_PG_BIN). Each mutation recompiles the test (the migrations are
embedded), so a full run takes a few minutes; it is a review tool, not part
of CI.
"""

import pathlib
import re
import subprocess
import sys

TEST = {"postgres": "postgres_enforces", "sqlite": "sqlite_enforces"}


def drop(sql: str, name: str) -> str:
    """The migration without constraint or unique index `name`."""
    index = re.search(r"CREATE UNIQUE INDEX " + name + r"\b[^\n]*\n", sql)
    if index:
        return sql[: index.start()] + sql[index.end() :]
    column_pk = re.search(r"CONSTRAINT " + name + r" PRIMARY KEY( AUTOINCREMENT)?,", sql)
    if column_pk:
        # Drop the clause, keep the column.
        return sql[: column_pk.start()] + "," + sql[column_pk.end() :]
    start = re.search(r"CONSTRAINT " + name + r"\s", sql).start()
    end, depth = start, 0
    while True:
        c = sql[end]
        if c == "(":
            depth += 1
        elif c == ")":
            if depth == 0:
                break  # closing parenthesis of the table
            depth -= 1
        elif c == "," and depth == 0:
            end += 1
            break
        end += 1
    if sql[start:end].endswith(","):
        out = sql[:start] + sql[end:]
    else:
        comma = sql.rindex(",", 0, start)
        out = sql[:comma] + sql[comma + 1 : start] + sql[end:]
    if name.endswith("_pkey"):
        # SQLite refuses WITHOUT ROWID tables without a primary key; drop
        # that option too, so the test sees the table, not a syntax error.
        next_table = out.find("CREATE TABLE", start)
        rowid = out.find(", WITHOUT ROWID", start, next_table if next_table != -1 else None)
        if rowid != -1:
            out = out[:rowid] + out[rowid + len(", WITHOUT ROWID") :]
    return out


def main() -> int:
    dialect = sys.argv[1]
    # An optional second argument limits the run to one migration file,
    # such as 0002_tokens.sql.
    only = sys.argv[2] if len(sys.argv) > 2 else None
    paths = sorted(pathlib.Path(f"crates/aspen-meta/migrations/{dialect}").glob("*.sql"))
    total, survived = 0, []
    for path in paths:
        if only and path.name != only:
            continue
        original = path.read_text()
        names = sorted(
            set(re.findall(r"CONSTRAINT (\w+)", original))
            | set(re.findall(r"CREATE UNIQUE INDEX (\w+)", original))
        )
        total += len(names)
        try:
            for name in names:
                path.write_text(drop(original, name))
                run = subprocess.run(
                    ["cargo", "test", "-q", "-p", "aspen-meta", "--test", "schema", TEST[dialect]],
                    capture_output=True,
                    text=True,
                )
                out = run.stdout + run.stderr
                if run.returncode == 0:
                    survived.append(name)
                    print(f"{dialect} {name} SURVIVED")
                    continue
                lines = out.splitlines()
                reason = next(
                    (lines[i + 1] for i, l in enumerate(lines) if "panicked at" in l and i + 1 < len(lines)),
                    "test failed",
                )
                print(f"{dialect} {name} DETECTED: {reason[:160]}")
        finally:
            path.write_text(original)
    print(f"{dialect}: {total - len(survived)} of {total} deletions detected")
    return 1 if survived else 0


if __name__ == "__main__":
    sys.exit(main())
