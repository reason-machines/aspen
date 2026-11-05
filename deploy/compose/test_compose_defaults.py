#!/usr/bin/env python3
"""Checks that the compose stack keeps its default request rates (issue #125).

compose.yaml must point Aspen at a limits file mounted from this directory,
and that file must limit read and push requests per caller.

    python3 deploy/compose/test_compose_defaults.py
"""
import json
import pathlib
import sys

import yaml

HERE = pathlib.Path(__file__).parent
aspen = yaml.safe_load((HERE / "compose.yaml").read_text())["services"]["aspen"]
path = aspen["environment"].get("ASPEN_LIMITS_FILE")
mounts = {v.split(":")[1]: v.split(":")[0] for v in aspen.get("volumes", []) if v.startswith("./")}
problems = []
if not path or path not in mounts:
    problems.append(f"ASPEN_LIMITS_FILE ({path}) must name a file compose.yaml mounts from this directory")
else:
    limits = json.loads((HERE / mounts[path]).read_text())
    for kind in ("read", "push"):
        rate = (limits.get(kind) or {}).get("per_second", 0)
        if not rate > 0:
            problems.append(f"{mounts[path]}: {kind}.per_second must be above 0 (got {rate})")
for p in problems:
    print(f"FAIL: {p}", file=sys.stderr)
if problems:
    sys.exit(1)
print("ok: compose limits read and push requests by default")
