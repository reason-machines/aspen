"""Launch, control and clean up the item 10 load generator on EC2 (Ubuntu 24.04 arm64).

Every run owns a fresh private S3 bucket, one egress-only security group and 15 instances
(12 API workers on c7g.medium, 3 Git workers on c7g.2xlarge) in an AWS account you name
explicitly. Nothing is adopted: the script refuses to reuse an existing bucket.

Subcommands:

  launch   --account-id ID --vpc-id VPC --subnet-ids S1,S2 --ami AMI --run RUN
           [--region us-east-2] [--bench-dir DIR] [--state-dir DIR] [--ttl-minutes 45]
      Checks that the signed-in caller, the VPC and every subnet belong to ID before any
      write, then creates the bucket aspen-item10-<run>-<nonce>, uploads bench.tgz
      (place_load.py, git_load.py, aggregate.py), creates the security group and launches
      the workers. The state file <state-dir>/<run>-<nonce>.json is created exclusively (an
      existing file is refused, never overwritten) and rewritten atomically before and after
      every allocation. If any step fails, everything allocated so far is torn down and the
      original error is re-raised (a teardown failure is attached to it as a note).
  go       --state-file F --start-at UNIX_TIME
      Writes the run-scoped start object <run>/<nonce>/go. Workers poll for it, then start
      at that time.
  watch    --state-file F
      Polls <run>/<nonce>/results/ until every worker has reported or the run's ExpiresAt
      passes, then runs teardown, also when polling fails or is interrupted. This deadline
      is enforced by the controller, not by the guests.
  teardown --state-file F [--discard-evidence]
      Finds this run's resources by the saved IDs and by their exact Project/Run/Nonce tags
      (and the deterministic bucket name), so resources created just before a crash are found
      too. The bucket is emptied or deleted only when its tags carry this run's exact
      Project/Run/Nonce; a same-named bucket without them (another actor took the name, or a
      crash came between create and tagging) is left untouched and reported. In order, each
      step attempted even when an earlier one failed: terminate the instances (a recorded ID
      that no longer exists is dropped, never blocking the live ones) and wait for them,
      download results/ and stderr/ into <state-dir>/<run>-<nonce>/ (never overwriting other
      bytes), delete the security groups, empty and delete the bucket. The bucket is kept when
      the evidence download failed, unless --discard-evidence. Any failed step makes teardown
      raise one TeardownError listing every failure, and the state is not marked torn down.
  reap     --account-id ID [--region us-east-2]
      Terminates every Project=aspen-item10-load instance whose ExpiresAt tag is in the past.

Each instance reports ready, waits for the go object, runs one worker, uploads its result and
stderr through presigned URLs and shuts down (InstanceInitiatedShutdownBehavior=terminate, plus
`shutdown -h +40` as a guest-side backstop). Instances, volumes and network interfaces carry
Project, Run, Nonce and ExpiresAt tags so `reap` can find leftovers independently of the guest.

Credentials: ASPEN_AWS_ACCESS_KEY_ID / ASPEN_AWS_SECRET_ACCESS_KEY when set, otherwise the
default boto3 chain. boto3 is imported only when a real AWS session is created, so the tests
(python3 -m unittest discover -s bench/throughput -p "test_*.py") run without it.
"""
from __future__ import annotations

import argparse
import datetime as dt
import io
import json
import os
import secrets
import sys
import tarfile
import time

PROJECT = "aspen-item10-load"
BENCH_FILES = ("place_load.py", "git_load.py", "aggregate.py")
PRESIGN_SECONDS = 7200
WORKERS = [("api", i, "c7g.medium") for i in range(12)] + [("git", i, "c7g.2xlarge") for i in range(3)]
LIVE_STATES = ["pending", "running", "shutting-down", "stopping", "stopped"]


class IsolationError(RuntimeError):
    """The caller, VPC or a subnet is not in the approved account."""


class RefusedError(RuntimeError):
    """A resource this run would create already exists; nothing is adopted."""


class TeardownError(RuntimeError):
    """One or more teardown steps failed; every step was still attempted."""

    def __init__(self, failures):
        self.failures = failures
        super().__init__("teardown incomplete: " + "; ".join(f"{step}: {exc!r}" for step, exc in failures))


class Clients:
    """The three AWS clients the script uses. Tests pass a fake with the same attributes."""

    def __init__(self, s3, ec2, sts):
        self.s3, self.ec2, self.sts = s3, ec2, sts


def real_clients(region: str) -> Clients:
    import boto3  # imported lazily so the tests never need boto3

    key, secret = os.environ.get("ASPEN_AWS_ACCESS_KEY_ID"), os.environ.get("ASPEN_AWS_SECRET_ACCESS_KEY")
    kwargs = {"region_name": region}
    if key and secret:
        kwargs.update(aws_access_key_id=key, aws_secret_access_key=secret)
    session = boto3.session.Session(**kwargs)
    return Clients(session.client("s3"), session.client("ec2"), session.client("sts"))


def error_code(exc: BaseException) -> str | None:
    """AWS error code of a botocore ClientError, found by duck typing (no botocore import)."""
    response = getattr(exc, "response", None)
    if isinstance(response, dict):
        return str(response.get("Error", {}).get("Code"))
    return None


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def iso(t: dt.datetime) -> str:
    return t.astimezone(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_iso(s: str) -> dt.datetime:
    return dt.datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=dt.timezone.utc)


# ---------------------------------------------------------------- isolation

def check_account(clients: Clients, account_id: str) -> None:
    caller = clients.sts.get_caller_identity()["Account"]
    if caller != account_id:
        raise IsolationError(f"signed in to account {caller}, expected {account_id}")


def check_isolation(clients: Clients, account_id: str, vpc_id: str, subnet_ids: list[str]) -> None:
    """Refuse unless the caller, the VPC and every subnet belong to account_id. Read-only."""
    check_account(clients, account_id)
    vpcs = clients.ec2.describe_vpcs(VpcIds=[vpc_id])["Vpcs"]
    if len(vpcs) != 1 or vpcs[0].get("OwnerId") != account_id:
        raise IsolationError(f"VPC {vpc_id} is not owned by {account_id}")
    if not subnet_ids:
        raise IsolationError("at least one subnet is required")
    subnets = {s["SubnetId"]: s for s in clients.ec2.describe_subnets(SubnetIds=subnet_ids)["Subnets"]}
    for sid in subnet_ids:
        s = subnets.get(sid)
        if s is None or s.get("OwnerId") != account_id:
            raise IsolationError(f"subnet {sid} is not owned by {account_id}")
        if s.get("VpcId") != vpc_id:
            raise IsolationError(f"subnet {sid} is in {s.get('VpcId')}, not {vpc_id}")


# ---------------------------------------------------------------- state

def save_state(path: str, state: dict) -> None:
    """Write state atomically: a reader sees the old file or the new one, never half of one."""
    tmp = f"{path}.tmp"
    with open(tmp, "w") as f:
        json.dump(state, f, indent=2, sort_keys=True)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def create_state(path: str, state: dict) -> None:
    """Create the state file, refusing one that exists (O_EXCL), then write it atomically."""
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    os.close(fd)
    save_state(path, state)


def load_state(path: str) -> dict:
    with open(path) as f:
        return json.load(f)


def prefix(state: dict) -> str:
    return f"{state['run']}/{state['nonce']}"


def pages(call, key: str, **kwargs) -> list:
    """Every item under key across all pages of an EC2 describe call (NextToken)."""
    items = []
    while True:
        page = call(**kwargs)
        items += page.get(key, [])
        token = page.get("NextToken")
        if not token:
            return items
        kwargs["NextToken"] = token


def identity_filters(state: dict) -> list[dict]:
    return [{"Name": "tag:Project", "Values": [PROJECT]}, {"Name": "tag:Run", "Values": [state["run"]]},
            {"Name": "tag:Nonce", "Values": [state["nonce"]]}]


def run_tags(state: dict) -> list[dict]:
    return [{"Key": "Project", "Value": PROJECT}, {"Key": "Run", "Value": state["run"]},
            {"Key": "Nonce", "Value": state["nonce"]}, {"Key": "ExpiresAt", "Value": state["expires_at"]}]


# ---------------------------------------------------------------- launch

def bench_tarball(bench_dir: str) -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as t:
        for f in BENCH_FILES:
            t.add(os.path.join(bench_dir, f), arcname=f)
    return buf.getvalue()


def worker_command(kind: str, i: int, run: str) -> str:
    if kind == "api":
        return (f"sleep $(( START - 20 - $(date +%s) )); python3 place_load.py --execute --acknowledge-persona-impact "
                f"--target-url https://place.reasonmachines.com/api/v1 --worker {i} --workers 12 --rate 45 --sessions 110 "
                f"--ramp-seconds 60 --hold-seconds 300 --deadline-seconds 420 --start-at $START --run {run}-api")
    return (f"python3 git_load.py --execute --acknowledge-persona-impact --total-offered 600 "
            f"--target-url https://place.reasonmachines.com/canvas.git --worker {i} --workers 3 --rate 20 --lanes 120 "
            f"--ramp-seconds 60 --hold-seconds 300 --deadline-seconds 600 --start-at $START --run {run}-git "
            f"--workdir /mnt/w --max-inflight 320")


def user_data(urls: dict, cmd: str) -> str:
    """Guest script. urls holds presigned URLs for bench, ready, go, result and stderr."""
    return f"""#!/bin/bash
shutdown -h +40
export DEBIAN_FRONTEND=noninteractive
apt-get update -q && apt-get install -yq python3-aiohttp git curl
ulimit -n 65535
mkdir -p /mnt/w /opt/b && cd /opt/b
curl -fsS '{urls['bench']}' | tar xz
git config --system protocol.version 2
curl -fsS -X PUT -H "Content-Type:" --data-binary "$(python3 --version; git --version)" '{urls['ready']}'
while ! curl -fsS -o /tmp/go '{urls['go']}'; do sleep 2; done
START=$(cat /tmp/go)
{cmd} > /tmp/result.json 2>/tmp/err.txt
curl -fsS -X PUT -H "Content-Type:" --data-binary @/tmp/result.json '{urls['result']}'
tail -c 4000 /tmp/err.txt | curl -fsS -X PUT -H "Content-Type:" --data-binary @- '{urls['stderr']}'
shutdown -h now
"""


def launch(clients: Clients, *, account_id: str, vpc_id: str, subnet_ids: list[str], ami: str, run: str,
           region: str, bench_dir: str, state_dir: str, ttl_minutes: int = 45, now=utc_now,
           token=lambda: secrets.token_hex(4), sleep=time.sleep, workers=WORKERS) -> dict:
    check_isolation(clients, account_id, vpc_id, subnet_ids)
    nonce = token()
    bucket = f"aspen-item10-{run}-{nonce}".lower()
    s3, ec2 = clients.s3, clients.ec2
    if region == "us-east-1":
        # In us-east-1 CreateBucket on a bucket this account already owns returns 200, so a
        # same-account bucket created after the absence check below would be adopted, tagged as
        # ours and later deleted. Elsewhere it fails with BucketAlreadyOwnedByYou, which proves
        # a successful create made a new bucket. Refuse before any write.
        raise RefusedError("us-east-1 cannot prove the control bucket is new (CreateBucket succeeds on a bucket "
                           "this account already owns); pass --region for another region")
    try:
        s3.head_bucket(Bucket=bucket)
    except Exception as exc:  # only "does not exist" lets us continue
        if error_code(exc) not in ("404", "NoSuchBucket", "NotFound"):
            raise RefusedError(f"bucket {bucket} is not confirmed absent ({error_code(exc) or exc!r})") from exc
    else:
        raise RefusedError(f"bucket {bucket} already exists; refusing to adopt it")

    os.makedirs(state_dir, exist_ok=True)
    state_path = os.path.join(state_dir, f"{run}-{nonce}.json")
    # bucket_status: absent -> creating (saved before create_bucket, so a crash right after it
    # still leads teardown to the bucket) -> created, or refused (it existed: never touch it).
    state = {"run": run, "nonce": nonce, "account_id": account_id, "region": region, "vpc_id": vpc_id,
             "subnet_ids": subnet_ids, "bucket": bucket, "bucket_status": "absent", "security_group": None,
             "instance_ids": [], "workers": [f"{k}-{i}" for k, i, _ in workers],
             "expires_at": iso(now() + dt.timedelta(minutes=ttl_minutes)), "state_path": state_path}
    create_state(state_path, state)
    try:
        _allocate(clients, state, state_path, ami, bench_dir, workers)
    except BaseException as exc:
        print(f"launch failed; tearing down run {run}", file=sys.stderr)
        try:
            teardown(clients, state, state_path, sleep=sleep)
        except Exception as cleanup:
            exc.add_note(f"rollback failed, rerun teardown --state-file {state_path}: {cleanup}")
        raise
    return state


def _allocate(clients, state, state_path, ami, bench_dir, workers):
    s3, ec2, bucket, p = clients.s3, clients.ec2, state["bucket"], prefix(state)
    state["bucket_status"] = "creating"
    save_state(state_path, state)
    try:
        # Never us-east-1 (launch refuses it): only outside it does a successful create prove new creation.
        s3.create_bucket(Bucket=bucket, CreateBucketConfiguration={"LocationConstraint": state["region"]})
    except Exception as exc:
        if error_code(exc) in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
            state["bucket_status"] = "refused"
            save_state(state_path, state)
            raise RefusedError(f"bucket {bucket} already exists; refusing to adopt it") from exc
        raise
    # The run's identity tags go on first: teardown deletes or empties a bucket only when it
    # carries this run's exact Project/Run/Nonce tags, never on the "creating" marker alone.
    s3.put_bucket_tagging(Bucket=bucket, Tagging={"TagSet": run_tags(state)})
    state["bucket_status"] = "created"
    save_state(state_path, state)
    s3.put_public_access_block(Bucket=bucket, PublicAccessBlockConfiguration={k: True for k in (
        "BlockPublicAcls", "IgnorePublicAcls", "BlockPublicPolicy", "RestrictPublicBuckets")})
    s3.put_object(Bucket=bucket, Key=f"{p}/bench.tgz", Body=bench_tarball(bench_dir))

    def url(op, key):
        return s3.generate_presigned_url(op, Params={"Bucket": bucket, "Key": f"{p}/{key}"}, ExpiresIn=PRESIGN_SECONDS)

    state["go_key"] = f"{p}/go"
    sg = ec2.create_security_group(
        GroupName=f"aspen-item10-{state['run']}-{state['nonce']}", Description="item 10 load generator, egress only",
        VpcId=state["vpc_id"], TagSpecifications=[{"ResourceType": "security-group", "Tags": run_tags(state)}])["GroupId"]
    state["security_group"] = sg
    save_state(state_path, state)

    subnets = state["subnet_ids"]
    for n, (kind, i, itype) in enumerate(workers):
        name = f"{kind}-{i}"
        urls = {"bench": url("get_object", "bench.tgz"), "ready": url("put_object", f"ready/{name}"),
                "go": url("get_object", "go"), "result": url("put_object", f"results/{name}.json"),
                "stderr": url("put_object", f"stderr/{name}.txt")}
        tags = run_tags(state)
        r = ec2.run_instances(
            ImageId=ami, InstanceType=itype, MinCount=1, MaxCount=1,
            UserData=user_data(urls, worker_command(kind, i, state["run"])),
            InstanceInitiatedShutdownBehavior="terminate",
            NetworkInterfaces=[{"DeviceIndex": 0, "SubnetId": subnets[n % len(subnets)], "Groups": [sg],
                                "AssociatePublicIpAddress": True}],
            TagSpecifications=[
                {"ResourceType": "instance",
                 "Tags": [{"Key": "Name", "Value": f"aspen-item10-{state['run']}-{name}"}] + tags},
                {"ResourceType": "volume", "Tags": tags},
                {"ResourceType": "network-interface", "Tags": tags},
            ])
        state["instance_ids"].append(r["Instances"][0]["InstanceId"])
        save_state(state_path, state)


# ---------------------------------------------------------------- go / watch

def go(clients: Clients, state: dict, start_at: int) -> None:
    clients.s3.put_object(Bucket=state["bucket"], Key=f"{prefix(state)}/go", Body=str(int(start_at)).encode())


def list_keys(s3, bucket: str, key_prefix: str = "") -> list[str]:
    keys, kwargs = [], {"Bucket": bucket, "Prefix": key_prefix}
    while True:
        page = s3.list_objects_v2(**kwargs)
        keys += [o["Key"] for o in page.get("Contents", [])]
        if not page.get("IsTruncated"):
            return keys
        kwargs["ContinuationToken"] = page["NextContinuationToken"]


def watch(clients: Clients, state: dict, state_path: str, *, now=utc_now, sleep=time.sleep, poll_seconds=15) -> str:
    """Wait for every worker's result or the deadline, then tear down. Returns why it stopped."""
    deadline = parse_iso(state["expires_at"])
    want = len(state["workers"])
    reason = "deadline"
    try:
        while now() < deadline:
            got = len(list_keys(clients.s3, state["bucket"], f"{prefix(state)}/results/"))
            print(f"{got}/{want} results", file=sys.stderr)
            if got >= want:
                reason = "complete"
                break
            sleep(poll_seconds)
    finally:
        teardown(clients, state, state_path, sleep=sleep)
    return reason


# ---------------------------------------------------------------- teardown / reap

def _tagged_live_instances(ec2, state: dict) -> list[str]:
    filters = identity_filters(state) + [{"Name": "instance-state-name", "Values": LIVE_STATES}]
    return [i["InstanceId"] for r in pages(ec2.describe_instances, "Reservations", Filters=filters)
            for i in r.get("Instances", [])]


def _tagged_security_groups(ec2, state: dict) -> list[str]:
    filters = identity_filters(state) + [{"Name": "vpc-id", "Values": [state["vpc_id"]]}]
    return [g["GroupId"] for g in pages(ec2.describe_security_groups, "SecurityGroups", Filters=filters)]


def _terminate(ec2, ids: list[str]) -> list[str]:
    """Terminate ids; returns the ones that exist. A recorded ID that no longer exists
    (InvalidInstanceID.NotFound fails the whole batch) must not shield live ones, so on that
    error each ID is terminated alone and the vanished ones are dropped."""
    try:
        ec2.terminate_instances(InstanceIds=ids)
        return ids
    except Exception as exc:
        if error_code(exc) not in ("InvalidInstanceID.NotFound", "InvalidInstanceID.Malformed"):
            raise
    alive, errors = [], []
    for iid in ids:
        try:
            ec2.terminate_instances(InstanceIds=[iid])
            alive.append(iid)
        except Exception as exc:
            if error_code(exc) not in ("InvalidInstanceID.NotFound", "InvalidInstanceID.Malformed"):
                errors.append(exc)
    if errors:
        raise errors[0]
    return alive


def _wait_terminated(ec2, ids: list[str], sleep, attempts=60, poll_seconds=10) -> None:
    for _ in range(attempts):
        found = [i for r in pages(ec2.describe_instances, "Reservations", InstanceIds=ids)
                 for i in r.get("Instances", [])]
        if len(found) >= len(ids) and all(i["State"]["Name"] == "terminated" for i in found):
            return
        sleep(poll_seconds)
    raise TimeoutError(f"instances not terminated after {attempts * poll_seconds}s: {ids}")


class UnverifiedBucket(Exception):
    """A bucket with this run's name exists but does not prove it belongs to this run."""


def _bucket_present(s3, state: dict) -> bool:
    """Whether this run's own bucket exists. A refused (pre-existing) bucket is never looked at.

    Account ownership (ExpectedBucketOwner) is not enough: another actor in the same account can
    hold the name. The bucket counts only when its tags carry this run's exact Project, Run and
    Nonce; anything else raises UnverifiedBucket and is left untouched."""
    if state.get("bucket_status", "created" if state.get("bucket_created") else "absent") not in ("creating", "created"):
        return False
    bucket, owner = state["bucket"], state["account_id"]
    try:
        s3.head_bucket(Bucket=bucket, ExpectedBucketOwner=owner)
    except Exception as exc:
        if error_code(exc) in ("404", "NoSuchBucket", "NotFound"):
            return False
        raise
    try:
        tags = s3.get_bucket_tagging(Bucket=bucket, ExpectedBucketOwner=owner).get("TagSet", [])
    except Exception as exc:
        if error_code(exc) != "NoSuchTagSet":
            raise
        tags = []
    have = {t["Key"]: t["Value"] for t in tags}
    want = {t["Key"]: t["Value"] for t in run_tags(state) if t["Key"] in ("Project", "Run", "Nonce")}
    if any(have.get(k) != v for k, v in want.items()):
        raise UnverifiedBucket(f"left {bucket} untouched: its tags do not prove it belongs to run "
                               f"{state['run']}/{state['nonce']} (found {have or 'no tags'})")
    return True


def _download_evidence(s3, state: dict, out_dir: str) -> int:
    p, bucket, n = prefix(state), state["bucket"], 0
    for key in list_keys(s3, bucket, f"{p}/"):
        rel = key[len(p) + 1:]
        if rel.startswith(("results/", "stderr/")):
            dest = os.path.join(out_dir, f"{state['run']}-{state['nonce']}", rel)
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
            if os.path.exists(dest):  # a rerun may meet its own earlier copy; never overwrite other bytes
                with open(dest, "rb") as f:
                    if f.read() != body:
                        raise FileExistsError(f"refusing to overwrite different evidence at {dest}")
            else:
                with open(dest, "xb") as f:
                    f.write(body)
            n += 1
    return n


def _delete_security_group(ec2, sg: str, sleep, attempts: int) -> None:
    for attempt in range(attempts):
        try:
            ec2.delete_security_group(GroupId=sg)
            return
        except Exception as exc:
            code = error_code(exc)
            if code == "InvalidGroup.NotFound":
                return
            if code != "DependencyViolation" or attempt == attempts - 1:
                raise
            sleep(15)


def _empty_and_delete_bucket(s3, bucket: str) -> None:
    keys = list_keys(s3, bucket)
    for start in range(0, len(keys), 1000):
        s3.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": k} for k in keys[start:start + 1000]]})
    s3.delete_bucket(Bucket=bucket)


def teardown(clients: Clients, state: dict, state_path: str, *, sleep=time.sleep, sg_attempts=20,
             discard_evidence: bool = False) -> None:
    """Remove everything this run owns, found by saved IDs and by exact tag identity.

    Compute goes first and does not depend on evidence collection; every later step is
    attempted even if an earlier one failed. Raises TeardownError listing all failures."""
    s3, ec2, bucket = clients.s3, clients.ec2, state["bucket"]
    out_dir = os.path.dirname(os.path.abspath(state_path))
    failures = []

    def attempt(step, fn, default=None):
        try:
            return fn()
        except Exception as exc:
            failures.append((step, exc))
            return default

    ids = set(state.get("instance_ids", []))
    ids |= set(attempt("discover_instances", lambda: _tagged_live_instances(ec2, state), []))
    ids = sorted(ids)
    if ids:
        ids = attempt("terminate_instances", lambda: _terminate(ec2, ids), [])
    if ids:
        attempt("wait_terminated", lambda: _wait_terminated(ec2, ids, sleep))

    present = attempt("find_bucket", lambda: _bucket_present(s3, state), False)
    evidence_ok = True
    if present:
        evidence_ok = attempt("download_evidence", lambda: _download_evidence(s3, state, out_dir)) is not None

    sgs = set(filter(None, [state.get("security_group")]))
    sgs |= set(attempt("discover_security_groups", lambda: _tagged_security_groups(ec2, state), []))
    for sg in sorted(sgs):
        attempt(f"delete_security_group {sg}", lambda sg=sg: _delete_security_group(ec2, sg, sleep, sg_attempts))

    if present:
        if evidence_ok or discard_evidence:
            attempt("delete_bucket", lambda: _empty_and_delete_bucket(s3, bucket))
        else:
            failures.append(("delete_bucket", RuntimeError(
                f"kept {bucket}: evidence download failed; rerun teardown, or pass --discard-evidence")))

    if failures:
        state["teardown_failures"] = [f"{step}: {exc!r}" for step, exc in failures]
        attempt_save = failures.copy()
        try:
            save_state(state_path, state)
        except Exception as exc:
            attempt_save.append(("save_state", exc))
        raise TeardownError(attempt_save)
    state.pop("teardown_failures", None)
    state["torn_down_at"] = iso(utc_now())
    save_state(state_path, state)


def reap(clients: Clients, account_id: str, *, now=utc_now) -> list[str]:
    """Terminate every Project-tagged instance whose ExpiresAt is past. Returns the IDs."""
    check_account(clients, account_id)
    reservations = pages(clients.ec2.describe_instances, "Reservations",
                         Filters=[{"Name": "tag:Project", "Values": [PROJECT]},
                                  {"Name": "instance-state-name", "Values": LIVE_STATES}])
    expired = []
    for r in reservations:
        for inst in r.get("Instances", []):
            tags = {t["Key"]: t["Value"] for t in inst.get("Tags", [])}
            if tags.get("Project") != PROJECT or "ExpiresAt" not in tags:
                continue
            try:
                if parse_iso(tags["ExpiresAt"]) < now():
                    expired.append(inst["InstanceId"])
            except ValueError:
                print(f"skipping {inst['InstanceId']}: bad ExpiresAt {tags['ExpiresAt']!r}", file=sys.stderr)
    if expired:
        clients.ec2.terminate_instances(InstanceIds=expired)
    return expired


# ---------------------------------------------------------------- CLI

def main(argv=None, clients_factory=real_clients) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("launch")
    p.add_argument("--account-id", required=True)
    p.add_argument("--vpc-id", required=True)
    p.add_argument("--subnet-ids", required=True, help="comma-separated")
    p.add_argument("--ami", required=True)
    p.add_argument("--run", required=True)
    p.add_argument("--region", default="us-east-2")
    p.add_argument("--bench-dir", default=os.path.dirname(os.path.abspath(__file__)))
    p.add_argument("--state-dir", default="l10-state")
    p.add_argument("--ttl-minutes", type=int, default=45)
    p = sub.add_parser("go")
    p.add_argument("--state-file", required=True)
    p.add_argument("--start-at", type=int, required=True)
    sub.add_parser("watch").add_argument("--state-file", required=True)
    p = sub.add_parser("teardown")
    p.add_argument("--state-file", required=True)
    p.add_argument("--discard-evidence", action="store_true",
                   help="delete the bucket even if results/ and stderr/ could not be downloaded")
    p = sub.add_parser("reap")
    p.add_argument("--account-id", required=True)
    p.add_argument("--region", default="us-east-2")
    args = ap.parse_args(argv)

    if args.cmd == "launch":
        state = launch(clients_factory(args.region), account_id=args.account_id, vpc_id=args.vpc_id,
                       subnet_ids=[s for s in args.subnet_ids.split(",") if s], ami=args.ami, run=args.run,
                       region=args.region, bench_dir=args.bench_dir, state_dir=args.state_dir,
                       ttl_minutes=args.ttl_minutes)
        print(json.dumps({k: state[k] for k in ("state_path", "bucket", "security_group", "instance_ids",
                                                 "expires_at")}))
        return 0
    if args.cmd == "reap":
        print(json.dumps({"terminated": reap(clients_factory(args.region), args.account_id)}))
        return 0
    state = load_state(args.state_file)
    clients = clients_factory(state["region"])
    check_account(clients, state["account_id"])
    if args.cmd == "go":
        go(clients, state, args.start_at)
    elif args.cmd == "watch":
        print(watch(clients, state, args.state_file))
    else:
        teardown(clients, state, args.state_file, discard_evidence=args.discard_evidence)
    return 0


if __name__ == "__main__":
    sys.exit(main())
