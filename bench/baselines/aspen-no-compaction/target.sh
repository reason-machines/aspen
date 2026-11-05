# Target: Aspen as the `aspen` target, with `aspen dev
# --no-background-worker`: no compaction, commit indexing or garbage
# collection, so fetches plan over every ref version without covers. Used
# for the stale-branch comparison "with and without compaction" (PLAN
# section 4.6). Sourced by ../run.sh.
ASPEN_DEV_ARGS=(--no-background-worker)
# shellcheck source=../aspen/target.sh
. "$BASELINES/aspen/target.sh"
# The stale-branch workload must not wait for jobs that never run.
TARGET_STALE_ARGS+=(--no-background-worker)
