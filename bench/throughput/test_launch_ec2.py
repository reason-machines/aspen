"""Tests for launch_ec2.py against an in-memory fake AWS. No network, no boto3."""
import datetime as dt
import io
import json
import os
import sys
import tempfile
import unittest
import unittest.mock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import launch_ec2 as L  # noqa: E402

ACCOUNT, OTHER = "111111111111", "222222222222"
VPC, SUBNETS = "vpc-good", ["subnet-a", "subnet-b"]
NOW = dt.datetime(2026, 11, 5, 12, 0, tzinfo=dt.timezone.utc)
MUTATING = {"create_bucket", "put_public_access_block", "put_bucket_tagging", "put_object", "create_security_group",
            "run_instances", "terminate_instances", "delete_security_group", "delete_objects", "delete_bucket"}


class ClientError(Exception):
    def __init__(self, code):
        super().__init__(code)
        self.response = {"Error": {"Code": code}}


class _Client:
    def __init__(self, fake):
        self._fake = fake

    def __getattr__(self, name):
        method = getattr(self._fake, name)

        def call(*args, **kwargs):
            self._fake.calls.append((name, kwargs))
            return method(*args, **kwargs)
        return call


class FakeAws:
    """Records every call; holds buckets, security groups and instances; can fail the Nth launch."""

    def __init__(self, caller=ACCOUNT, vpc_owner=ACCOUNT, subnets=None, fail_run_instances_at=None,
                 existing_buckets=(), state_path=None, page_size=None, region="us-east-2"):
        self.region = region
        self.calls, self.caller, self.vpc_owner = [], caller, vpc_owner
        self.subnets = subnets or {s: {"SubnetId": s, "OwnerId": ACCOUNT, "VpcId": VPC} for s in SUBNETS}
        self.buckets = {b: {} for b in existing_buckets}
        self.bucket_tags = {}
        self.sgs, self.sg_tags, self.instances, self.launches = set(), {}, {}, 0
        self.page_size = page_size  # when set, describe_* calls return pages of this size
        self.fail_run_instances_at, self.state_path, self.snapshot_at_failure = fail_run_instances_at, state_path, None
        self.s3 = self.ec2 = self.sts = _Client(self)

    def mutating(self):
        return [n for n, _ in self.calls if n in MUTATING]

    # sts
    def get_caller_identity(self):
        return {"Account": self.caller}

    # s3
    def _page(self, items, key, NextToken=None):
        if not self.page_size:
            return {key: items}
        start = int(NextToken or 0)
        page = {key: items[start:start + self.page_size]}
        if start + self.page_size < len(items):
            page["NextToken"] = str(start + self.page_size)
        return page

    def head_bucket(self, Bucket, ExpectedBucketOwner=None):
        if ExpectedBucketOwner not in (None, self.caller):
            raise ClientError("403")
        if Bucket not in self.buckets:
            raise ClientError("404")
        return {}

    def create_bucket(self, Bucket, CreateBucketConfiguration=None):
        # Real S3, by region: us-east-1 takes no LocationConstraint and answers 200 when this
        # account already owns the bucket; any other region needs its own LocationConstraint and
        # answers BucketAlreadyOwnedByYou for an owned bucket.
        constraint = (CreateBucketConfiguration or {}).get("LocationConstraint")
        if self.region == "us-east-1":
            if constraint is not None:
                raise ClientError("InvalidLocationConstraint")
            self.buckets.setdefault(Bucket, {})
            return {"Location": "/" + Bucket}
        if constraint != self.region:
            raise ClientError("IllegalLocationConstraintException")
        if Bucket in self.buckets:
            raise ClientError("BucketAlreadyOwnedByYou")
        self.buckets[Bucket] = {}
        return {"Location": "/" + Bucket}

    def put_public_access_block(self, **_):
        pass

    def put_bucket_tagging(self, Bucket, Tagging):
        self.bucket_tags[Bucket] = {t["Key"]: t["Value"] for t in Tagging["TagSet"]}

    def get_bucket_tagging(self, Bucket, ExpectedBucketOwner=None):
        if Bucket not in self.bucket_tags:
            raise ClientError("NoSuchTagSet")
        return {"TagSet": [{"Key": k, "Value": v} for k, v in self.bucket_tags[Bucket].items()]}

    def put_object(self, Bucket, Key, Body):
        self.buckets[Bucket][Key] = Body

    def get_object(self, Bucket, Key):
        return {"Body": io.BytesIO(self.buckets[Bucket][Key])}

    def generate_presigned_url(self, op, Params, ExpiresIn):
        return f"https://presigned/{op}/{Params['Bucket']}/{Params['Key']}"

    def list_objects_v2(self, Bucket, Prefix="", **_):
        return {"Contents": [{"Key": k} for k in sorted(self.buckets[Bucket]) if k.startswith(Prefix)]}

    def delete_objects(self, Bucket, Delete):
        for o in Delete["Objects"]:
            self.buckets[Bucket].pop(o["Key"], None)

    def delete_bucket(self, Bucket):
        assert not self.buckets[Bucket], "bucket not empty"
        del self.buckets[Bucket]
        self.bucket_tags.pop(Bucket, None)

    # ec2
    def describe_vpcs(self, VpcIds):
        return {"Vpcs": [{"VpcId": v, "OwnerId": self.vpc_owner} for v in VpcIds]}

    def describe_subnets(self, SubnetIds):
        return {"Subnets": [self.subnets[s] for s in SubnetIds if s in self.subnets]}

    def create_security_group(self, VpcId, TagSpecifications, **_):
        self.sgs.add("sg-1")
        self.sg_tags["sg-1"] = {"VpcId": VpcId, "Tags": {t["Key"]: t["Value"] for t in TagSpecifications[0]["Tags"]}}
        return {"GroupId": "sg-1"}

    def add_security_group(self, sg, tags, vpc=VPC):
        self.sgs.add(sg)
        self.sg_tags[sg] = {"VpcId": vpc, "Tags": tags}

    def describe_security_groups(self, Filters=(), NextToken=None):
        found = sorted(self.sgs)
        for f in Filters:
            if f["Name"] == "vpc-id":
                found = [g for g in found if self.sg_tags.get(g, {}).get("VpcId") in f["Values"]]
            else:
                key = f["Name"][len("tag:"):]
                found = [g for g in found if self.sg_tags.get(g, {}).get("Tags", {}).get(key) in f["Values"]]
        return self._page([{"GroupId": g} for g in found], "SecurityGroups", NextToken)

    def delete_security_group(self, GroupId):
        if GroupId not in self.sgs:
            raise ClientError("InvalidGroup.NotFound")
        if any(i["State"]["Name"] != "terminated" and i.get("Group") == GroupId for i in self.instances.values()):
            raise ClientError("DependencyViolation")
        self.sgs.discard(GroupId)

    def run_instances(self, TagSpecifications, NetworkInterfaces, **_):
        self.launches += 1
        if self.launches == self.fail_run_instances_at:
            if self.state_path:
                with open(self.state_path) as f:
                    self.snapshot_at_failure = json.load(f)
            raise ClientError("InsufficientInstanceCapacity")
        iid = f"i-{self.launches:04d}"
        tags = next(t["Tags"] for t in TagSpecifications if t["ResourceType"] == "instance")
        self.instances[iid] = {"InstanceId": iid, "State": {"Name": "running"}, "Tags": tags,
                               "Group": NetworkInterfaces[0]["Groups"][0]}
        return {"Instances": [{"InstanceId": iid}]}

    def add_instance(self, iid, tags, state="running", group=None):
        self.instances[iid] = {"InstanceId": iid, "State": {"Name": state}, "Group": group,
                               "Tags": [{"Key": k, "Value": v} for k, v in tags.items()]}

    def terminate_instances(self, InstanceIds):
        if any(i not in self.instances for i in InstanceIds):  # AWS fails the whole batch
            raise ClientError("InvalidInstanceID.NotFound")
        for i in InstanceIds:
            self.instances[i]["State"]["Name"] = "terminated"

    def describe_instances(self, InstanceIds=None, Filters=(), NextToken=None):
        found = [self.instances[i] for i in InstanceIds] if InstanceIds else list(self.instances.values())
        for f in Filters:
            if f["Name"] == "instance-state-name":
                found = [i for i in found if i["State"]["Name"] in f["Values"]]
            else:
                key = f["Name"][len("tag:"):]
                found = [i for i in found if {t["Key"]: t["Value"] for t in i["Tags"]}.get(key) in f["Values"]]
        # one reservation per instance, so paging splits them
        return self._page([{"Instances": [i]} for i in found], "Reservations", NextToken)


class LaunchTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.bench = os.path.join(self.tmp.name, "bench")
        os.mkdir(self.bench)
        for f in L.BENCH_FILES:
            with open(os.path.join(self.bench, f), "w") as fh:
                fh.write("# stub\n")
        self.state_dir = os.path.join(self.tmp.name, "state", "nested")
        self.state_path = os.path.join(self.state_dir, "r1-abcd1234.json")

    def tearDown(self):
        self.tmp.cleanup()

    def launch(self, fake, subnet_ids=SUBNETS, workers=L.WORKERS[:5]):
        return L.launch(fake, account_id=ACCOUNT, vpc_id=VPC, subnet_ids=subnet_ids, ami="ami-x", run="r1",
                        region="us-east-2", bench_dir=self.bench, state_dir=self.state_dir, now=lambda: NOW,
                        token=lambda: "abcd1234", sleep=lambda s: None, workers=workers)

    def test_wrong_caller_account_makes_no_mutating_call(self):
        fake = FakeAws(caller=OTHER)
        with self.assertRaises(L.IsolationError):
            self.launch(fake)
        self.assertEqual(fake.mutating(), [])
        self.assertFalse(os.path.exists(self.state_path))

    def test_foreign_vpc_makes_no_mutating_call(self):
        fake = FakeAws(vpc_owner=OTHER)
        with self.assertRaises(L.IsolationError):
            self.launch(fake)
        self.assertEqual(fake.mutating(), [])

    def test_foreign_subnet_makes_no_mutating_call(self):
        fake = FakeAws(subnets={"subnet-a": {"SubnetId": "subnet-a", "OwnerId": ACCOUNT, "VpcId": VPC},
                                "subnet-b": {"SubnetId": "subnet-b", "OwnerId": OTHER, "VpcId": VPC}})
        with self.assertRaises(L.IsolationError):
            self.launch(fake)
        self.assertEqual(fake.mutating(), [])

    def test_subnet_in_other_vpc_makes_no_mutating_call(self):
        fake = FakeAws(subnets={"subnet-a": {"SubnetId": "subnet-a", "OwnerId": ACCOUNT, "VpcId": VPC},
                                "subnet-b": {"SubnetId": "subnet-b", "OwnerId": ACCOUNT, "VpcId": "vpc-other"}})
        with self.assertRaises(L.IsolationError):
            self.launch(fake)
        self.assertEqual(fake.mutating(), [])

    def test_existing_bucket_is_refused(self):
        fake = FakeAws(existing_buckets=["aspen-item10-r1-abcd1234"])
        with self.assertRaises(L.RefusedError):
            self.launch(fake)
        self.assertEqual(fake.mutating(), [])

    def test_head_bucket_forbidden_is_refused(self):
        fake = FakeAws()
        fake.head_bucket = lambda Bucket: (_ for _ in ()).throw(ClientError("403"))
        with self.assertRaises(L.RefusedError):
            self.launch(fake)
        self.assertEqual(fake.mutating(), [])

    def test_create_bucket_already_owned_is_refusal_not_adoption(self):
        fake = FakeAws()
        fake.create_bucket = lambda Bucket, **_: (_ for _ in ()).throw(ClientError("BucketAlreadyOwnedByYou"))
        with self.assertRaises(L.RefusedError):
            self.launch(fake)
        self.assertNotIn("put_object", fake.mutating())
        self.assertNotIn("delete_bucket", fake.mutating())

    def test_fourth_launch_failure_tears_down_everything_and_state_was_incremental(self):
        fake = FakeAws(fail_run_instances_at=4, state_path=self.state_path)
        with self.assertRaises(ClientError):
            self.launch(fake)
        self.assertEqual(fake.snapshot_at_failure["instance_ids"], ["i-0001", "i-0002", "i-0003"])
        self.assertEqual(fake.snapshot_at_failure["security_group"], "sg-1")
        self.assertEqual(fake.snapshot_at_failure["bucket_status"], "created")
        self.assertTrue(all(i["State"]["Name"] == "terminated" for i in fake.instances.values()))
        self.assertEqual(len(fake.instances), 3)
        self.assertEqual(fake.sgs, set())
        self.assertEqual(fake.buckets, {})
        self.assertEqual(L.load_state(self.state_path)["instance_ids"], ["i-0001", "i-0002", "i-0003"])

    def test_run_instances_tags_and_shutdown_behaviour(self):
        fake = FakeAws()
        self.launch(fake, workers=L.WORKERS[:1])
        (_, kwargs), = [c for c in fake.calls if c[0] == "run_instances"]
        self.assertEqual(kwargs["InstanceInitiatedShutdownBehavior"], "terminate")
        specs = {s["ResourceType"]: {t["Key"]: t["Value"] for t in s["Tags"]} for s in kwargs["TagSpecifications"]}
        self.assertEqual(set(specs), {"instance", "volume", "network-interface"})
        for tags in specs.values():
            self.assertEqual(tags["Project"], L.PROJECT)
            self.assertEqual(tags["Run"], "r1")
            self.assertEqual(tags["Nonce"], "abcd1234")
            self.assertEqual(tags["ExpiresAt"], "2026-11-05T12:45:00Z")

    def test_user_data_uses_only_nonce_scoped_keys(self):
        fake = FakeAws()
        self.launch(fake, workers=L.WORKERS[:1])
        (_, kwargs), = [c for c in fake.calls if c[0] == "run_instances"]
        ud = kwargs["UserData"]
        self.assertIn("/aspen-item10-r1-abcd1234/r1/abcd1234/go'", ud)
        urls = [part.split("'")[0] for part in ud.split("'https://presigned/")[1:]]
        self.assertEqual(len(urls), 5)
        for u in urls:
            self.assertIn("/r1/abcd1234/", u)
        self.assertNotIn("aspen-item10-r1-abcd1234/go", ud)

    def test_go_writes_nonce_scoped_key(self):
        fake = FakeAws()
        state = self.launch(fake, workers=L.WORKERS[:1])
        L.go(fake, state, 1794000000)
        self.assertEqual(fake.buckets[state["bucket"]]["r1/abcd1234/go"], b"1794000000")

    def test_standalone_teardown_cleans_everything_and_spares_other_runs(self):
        fake = FakeAws()
        self.launch(fake, workers=L.WORKERS[:2])
        state = L.load_state(self.state_path)
        bucket = state["bucket"]
        fake.buckets[bucket]["r1/abcd1234/results/api-0.json"] = b'{"ok": 1}'
        fake.buckets[bucket]["r1/abcd1234/stderr/api-0.txt"] = b"warn"
        # an instance launched for this run but never recorded (crash between launch and save)
        fake.add_instance("i-orphan", {"Project": L.PROJECT, "Run": "r1", "Nonce": "abcd1234"}, group="sg-1")
        # same run name, different nonce: another run's instance, must survive
        fake.add_instance("i-other", {"Project": L.PROJECT, "Run": "r1", "Nonce": "ffff0000"})
        self.assertEqual(L.main(["teardown", "--state-file", self.state_path], clients_factory=lambda r: fake), 0)
        for iid in ("i-0001", "i-0002", "i-orphan"):
            self.assertEqual(fake.instances[iid]["State"]["Name"], "terminated")
        self.assertEqual(fake.instances["i-other"]["State"]["Name"], "running")
        self.assertEqual(fake.sgs, set())
        self.assertEqual(fake.buckets, {})
        with open(os.path.join(self.state_dir, "r1-abcd1234", "results", "api-0.json")) as f:
            self.assertEqual(json.load(f), {"ok": 1})
        self.assertTrue(os.path.exists(os.path.join(self.state_dir, "r1-abcd1234", "stderr", "api-0.txt")))

    def test_teardown_retries_security_group_dependency_violation(self):
        fake = FakeAws()
        self.launch(fake, workers=L.WORKERS[:1])
        state = L.load_state(self.state_path)
        failures = iter([ClientError("DependencyViolation"), ClientError("DependencyViolation")])
        real = fake.delete_security_group

        def flaky(GroupId):
            err = next(failures, None)
            if err:
                raise err
            return real(GroupId=GroupId)
        fake.delete_security_group = flaky
        L.teardown(fake, state, self.state_path, sleep=lambda s: None)
        self.assertEqual(fake.sgs, set())

    def test_teardown_refuses_other_account(self):
        fake = FakeAws()
        self.launch(fake, workers=L.WORKERS[:1])
        fake.caller = OTHER
        before = len(fake.mutating())
        with self.assertRaises(L.IsolationError):
            L.main(["teardown", "--state-file", self.state_path], clients_factory=lambda r: fake)
        self.assertEqual(len(fake.mutating()), before)

    def test_watch_tears_down_at_deadline(self):
        fake = FakeAws()
        state = self.launch(fake, workers=L.WORKERS[:2])
        clock = iter([NOW, NOW + dt.timedelta(minutes=46)])
        reason = L.watch(fake, state, self.state_path, now=lambda: next(clock), sleep=lambda s: None)
        self.assertEqual(reason, "deadline")
        self.assertEqual(fake.buckets, {})

    def test_watch_tears_down_when_all_results_arrive(self):
        fake = FakeAws()
        state = self.launch(fake, workers=L.WORKERS[:1])
        fake.buckets[state["bucket"]]["r1/abcd1234/results/api-0.json"] = b"{}"
        self.assertEqual(L.watch(fake, state, self.state_path, now=lambda: NOW, sleep=lambda s: None), "complete")
        self.assertTrue(os.path.exists(os.path.join(self.state_dir, "r1-abcd1234", "results", "api-0.json")))

    def test_reap_terminates_only_expired_tagged_instances(self):
        fake = FakeAws()
        fake.add_instance("i-expired", {"Project": L.PROJECT, "ExpiresAt": "2026-11-05T11:00:00Z"})
        fake.add_instance("i-fresh", {"Project": L.PROJECT, "ExpiresAt": "2026-11-05T13:00:00Z"})
        fake.add_instance("i-untagged", {"Project": L.PROJECT})
        fake.add_instance("i-foreign", {"Project": "something-else", "ExpiresAt": "2026-11-05T11:00:00Z"})
        self.assertEqual(L.reap(fake, ACCOUNT, now=lambda: NOW), ["i-expired"])
        states = {i: v["State"]["Name"] for i, v in fake.instances.items()}
        self.assertEqual(states, {"i-expired": "terminated", "i-fresh": "running", "i-untagged": "running",
                                  "i-foreign": "running"})

    def test_reap_checks_account_first(self):
        fake = FakeAws(caller=OTHER)
        fake.add_instance("i-expired", {"Project": L.PROJECT, "ExpiresAt": "2026-11-05T11:00:00Z"})
        with self.assertRaises(L.IsolationError):
            L.reap(fake, ACCOUNT, now=lambda: NOW)
        self.assertEqual(fake.mutating(), [])

    def test_cli_requires_account_and_network_ids(self):
        with self.assertRaises(SystemExit):
            L.main(["launch", "--run", "r1", "--ami", "ami-x"], clients_factory=lambda r: FakeAws())

    # ------------------------------------------------ teardown is independent of evidence (re-review 1)

    def launched(self, fake, n=2):
        self.launch(fake, workers=L.WORKERS[:n])
        return L.load_state(self.state_path)

    def assert_compute_and_sg_gone(self, fake):
        self.assertTrue(fake.instances)
        self.assertTrue(all(i["State"]["Name"] == "terminated" for i in fake.instances.values()))
        self.assertEqual(fake.sgs, set())

    def test_evidence_access_denied_still_terminates_and_keeps_bucket(self):
        fake = FakeAws()
        state = self.launched(fake)
        fake.buckets[state["bucket"]]["r1/abcd1234/results/api-0.json"] = b"{}"
        fake.get_object = lambda **_: (_ for _ in ()).throw(ClientError("AccessDenied"))
        with self.assertRaises(L.TeardownError) as caught:
            L.teardown(fake, state, self.state_path, sleep=lambda s: None)
        steps = [step for step, _ in caught.exception.failures]
        self.assertEqual(steps, ["download_evidence", "delete_bucket"])
        self.assert_compute_and_sg_gone(fake)
        self.assertIn(state["bucket"], fake.buckets)  # evidence kept for a rerun
        saved = L.load_state(self.state_path)
        self.assertNotIn("torn_down_at", saved)
        self.assertEqual(len(saved["teardown_failures"]), 2)

    def test_evidence_listing_unavailable_still_terminates(self):
        fake = FakeAws()
        state = self.launched(fake)
        fake.list_objects_v2 = lambda **_: (_ for _ in ()).throw(ClientError("ServiceUnavailable"))
        with self.assertRaises(L.TeardownError):
            L.teardown(fake, state, self.state_path, sleep=lambda s: None)
        self.assert_compute_and_sg_gone(fake)

    def test_local_write_failure_still_terminates(self):
        fake = FakeAws()
        state = self.launched(fake)
        fake.buckets[state["bucket"]]["r1/abcd1234/results/api-0.json"] = b"{}"
        with open(os.path.join(self.state_dir, "r1-abcd1234"), "w") as f:  # a file where the evidence dir goes
            f.write("x")
        with self.assertRaises(L.TeardownError) as caught:
            L.teardown(fake, state, self.state_path, sleep=lambda s: None)
        self.assertIsInstance(dict(caught.exception.failures)["download_evidence"], OSError)
        self.assert_compute_and_sg_gone(fake)

    def test_discard_evidence_deletes_bucket_after_download_failure(self):
        fake = FakeAws()
        state = self.launched(fake)
        fake.buckets[state["bucket"]]["r1/abcd1234/results/api-0.json"] = b"{}"
        fake.get_object = lambda **_: (_ for _ in ()).throw(ClientError("AccessDenied"))
        with self.assertRaises(L.TeardownError) as caught:
            L.teardown(fake, state, self.state_path, sleep=lambda s: None, discard_evidence=True)
        self.assertEqual([s for s, _ in caught.exception.failures], ["download_evidence"])
        self.assertEqual(fake.buckets, {})

    def test_missing_bucket_is_not_an_error_and_compute_still_goes(self):
        fake = FakeAws()
        state = self.launched(fake)
        del fake.buckets[state["bucket"]]
        L.teardown(fake, state, self.state_path, sleep=lambda s: None)
        self.assert_compute_and_sg_gone(fake)
        self.assertIn("torn_down_at", L.load_state(self.state_path))

    def test_terminate_failure_does_not_stop_later_steps(self):
        fake = FakeAws()
        state = self.launched(fake)
        fake.terminate_instances = lambda **_: (_ for _ in ()).throw(ClientError("UnauthorizedOperation"))
        with self.assertRaises(L.TeardownError) as caught:
            L.teardown(fake, state, self.state_path, sleep=lambda s: None, sg_attempts=2)
        steps = [s for s, _ in caught.exception.failures]
        self.assertEqual(steps, ["terminate_instances", "delete_security_group sg-1"])
        self.assertEqual(fake.buckets, {})  # the bucket step still ran

    def test_rollback_survives_evidence_failure_and_reraises_original(self):
        fake = FakeAws(fail_run_instances_at=4)
        fake.list_objects_v2 = lambda **_: (_ for _ in ()).throw(ClientError("ServiceUnavailable"))
        with self.assertRaises(ClientError) as caught:
            self.launch(fake)
        self.assertEqual(caught.exception.response["Error"]["Code"], "InsufficientInstanceCapacity")
        self.assertTrue(any("rollback failed" in n for n in getattr(caught.exception, "__notes__", [])))
        self.assert_compute_and_sg_gone(fake)
        self.assertEqual(len(fake.instances), 3)

    def test_watch_tears_down_when_polling_fails(self):
        fake = FakeAws()
        state = self.launched(fake)
        real = fake.list_objects_v2
        calls = {"n": 0}

        def flaky(**kw):
            calls["n"] += 1
            if calls["n"] == 1:
                raise ClientError("ServiceUnavailable")
            return real(**kw)
        fake.list_objects_v2 = flaky
        with self.assertRaises(ClientError):
            L.watch(fake, state, self.state_path, now=lambda: NOW, sleep=lambda s: None)
        self.assert_compute_and_sg_gone(fake)
        self.assertEqual(fake.buckets, {})

    # ------------------------------------------------ pagination (re-review 2)

    def test_teardown_finds_tagged_instance_and_group_on_a_later_page(self):
        fake = FakeAws(page_size=1)
        state = self.launched(fake, n=2)
        fake.add_instance("i-orphan", {"Project": L.PROJECT, "Run": "r1", "Nonce": "abcd1234"})
        fake.add_security_group("sg-z", {"Project": L.PROJECT, "Run": "r1", "Nonce": "abcd1234"})
        state["instance_ids"] = []  # only tag discovery can find them
        L.teardown(fake, state, self.state_path, sleep=lambda s: None)
        self.assertEqual(fake.instances["i-orphan"]["State"]["Name"], "terminated")
        self.assertEqual(fake.sgs, set())

    def test_wait_terminated_reads_every_page(self):
        fake = FakeAws(page_size=1)
        fake.add_instance("i-a", {}, state="terminated")
        fake.add_instance("i-b", {}, state="shutting-down")
        with self.assertRaises(TimeoutError):
            L._wait_terminated(fake.ec2, ["i-a", "i-b"], lambda s: None, attempts=2)

    def test_reap_reads_every_page(self):
        fake = FakeAws(page_size=1)
        fake.add_instance("i-fresh", {"Project": L.PROJECT, "ExpiresAt": "2026-11-05T13:00:00Z"})
        fake.add_instance("i-expired", {"Project": L.PROJECT, "ExpiresAt": "2026-11-05T11:00:00Z"})
        self.assertEqual(L.reap(fake, ACCOUNT, now=lambda: NOW), ["i-expired"])

    # ------------------------------------------------ state file per run and nonce (re-review 3)

    def test_existing_state_file_is_refused_not_overwritten(self):
        os.makedirs(self.state_dir)
        with open(self.state_path, "w") as f:
            f.write('{"earlier": true}')
        with self.assertRaises(FileExistsError):
            self.launch(FakeAws())
        with open(self.state_path) as f:
            self.assertEqual(json.load(f), {"earlier": True})

    def test_reused_run_name_gets_its_own_state_file(self):
        fake = FakeAws()
        self.launch(fake, workers=L.WORKERS[:1])
        L.launch(fake, account_id=ACCOUNT, vpc_id=VPC, subnet_ids=SUBNETS, ami="ami-x", run="r1",
                 region="us-east-2", bench_dir=self.bench, state_dir=self.state_dir, now=lambda: NOW,
                 token=lambda: "beef0000", sleep=lambda s: None, workers=L.WORKERS[:1])
        self.assertEqual(sorted(f for f in os.listdir(self.state_dir) if f.endswith(".json")),
                         ["r1-abcd1234.json", "r1-beef0000.json"])
        self.assertEqual(L.load_state(self.state_path)["nonce"], "abcd1234")

    # ------------------------------------------------ crash windows (re-review 4)

    def test_security_group_created_but_not_recorded_is_found_by_tags(self):
        fake = FakeAws()
        real = fake.create_security_group

        def crash_after_create(**kw):
            real(**kw)
            raise KeyboardInterrupt  # the controller dies before saving the group's ID
        fake.create_security_group = crash_after_create
        with self.assertRaises(KeyboardInterrupt):
            self.launch(fake)
        self.assertIsNone(L.load_state(self.state_path)["security_group"])
        self.assertEqual(fake.sgs, set())
        self.assertEqual(fake.buckets, {})

    def test_bucket_created_but_not_recorded_is_found_by_name(self):
        fake = FakeAws()
        real = fake.put_bucket_tagging

        def crash_after_tagging(**kw):
            real(**kw)
            raise KeyboardInterrupt  # dies before "created" is saved
        fake.put_bucket_tagging = crash_after_tagging
        with self.assertRaises(KeyboardInterrupt):
            self.launch(fake)
        self.assertEqual(L.load_state(self.state_path)["bucket_status"], "creating")
        self.assertEqual(fake.buckets, {})

    # ------------------------------------------------ re-review 2

    def foreign_bucket_appears(self, fake, error):
        """Another same-account actor creates and tags the name after our HEAD; our create fails."""
        def create(Bucket, **_):
            fake.buckets[Bucket] = {"theirs/data": b"precious"}
            fake.bucket_tags[Bucket] = {"Owner": "someone-else"}
            raise ClientError(error)
        fake.create_bucket = create

    def assert_bucket_untouched_and_reported(self, fake, caught):
        bucket = "aspen-item10-r1-abcd1234"
        self.assertEqual(fake.buckets[bucket], {"theirs/data": b"precious"})
        self.assertFalse({"delete_objects", "delete_bucket"} & set(fake.mutating()))
        reported = [str(exc) for step, exc in caught.failures if step == "find_bucket"]
        self.assertEqual(len(reported), 1)
        self.assertIn("left aspen-item10-r1-abcd1234 untouched", reported[0])

    def test_name_taken_after_head_with_ambiguous_create_error_is_not_deleted(self):
        for error in ("AccessDenied", "RequestTimeout"):
            with self.subTest(error=error):
                self.state_dir = os.path.join(self.tmp.name, error)
                self.state_path = os.path.join(self.state_dir, "r1-abcd1234.json")
                fake = FakeAws()
                self.foreign_bucket_appears(fake, error)
                with self.assertRaises(ClientError) as caught:
                    self.launch(fake)
                self.assertEqual(L.load_state(self.state_path)["bucket_status"], "creating")
                note = "\n".join(getattr(caught.exception, "__notes__", []))
                self.assertIn("untouched", note)  # rollback reported it
                with self.assertRaises(L.TeardownError) as again:  # and so does a standalone teardown
                    L.teardown(fake, L.load_state(self.state_path), self.state_path, sleep=lambda s: None)
                self.assert_bucket_untouched_and_reported(fake, again.exception)

    def test_refused_create_whose_marker_was_not_saved_is_not_deleted(self):
        fake = FakeAws()
        self.foreign_bucket_appears(fake, "BucketAlreadyOwnedByYou")
        real_save = L.save_state

        def save(path, state):
            if state.get("bucket_status") == "refused":
                raise OSError("disk full")
            real_save(path, state)
        with unittest.mock.patch.object(L, "save_state", save):
            with self.assertRaises(OSError):
                self.launch(fake)
        self.assertEqual(L.load_state(self.state_path)["bucket_status"], "creating")
        with self.assertRaises(L.TeardownError) as caught:
            L.teardown(fake, L.load_state(self.state_path), self.state_path, sleep=lambda s: None)
        self.assert_bucket_untouched_and_reported(fake, caught.exception)

    def test_bucket_with_another_runs_nonce_is_not_deleted(self):
        fake = FakeAws()
        state = self.launched(fake, n=1)
        fake.bucket_tags[state["bucket"]]["Nonce"] = "ffff0000"
        with self.assertRaises(L.TeardownError):
            L.teardown(fake, state, self.state_path, sleep=lambda s: None)
        self.assertIn(state["bucket"], fake.buckets)
        self.assertNotIn("delete_objects", fake.mutating())
        self.assert_compute_and_sg_gone(fake)  # everything provably ours still goes

    def test_crash_between_create_and_tagging_leaves_untagged_bucket_reported(self):
        fake = FakeAws()
        fake.put_bucket_tagging = lambda **_: (_ for _ in ()).throw(KeyboardInterrupt())
        with self.assertRaises(KeyboardInterrupt) as caught:
            self.launch(fake)
        self.assertIn("aspen-item10-r1-abcd1234", fake.buckets)
        self.assertNotIn("delete_bucket", fake.mutating())
        self.assertIn("no tags", "\n".join(caught.exception.__notes__))

    def test_bucket_is_tagged_before_it_is_marked_created(self):
        # Every time the state file records bucket_status "created", the bucket must already carry
        # this run's exact identity tags (checked at the moment of the save, not by call order).
        fake = FakeAws()
        seen = []
        real_save = L.save_state

        def spy(path, state):
            if state.get("bucket_status") == "created":
                seen.append(dict(fake.bucket_tags.get(state["bucket"], {})))
            real_save(path, state)
        with unittest.mock.patch.object(L, "save_state", spy):
            self.launch(fake, workers=L.WORKERS[:1])
        self.assertTrue(seen)
        for tags in seen:
            self.assertEqual({k: tags.get(k) for k in ("Project", "Run", "Nonce")},
                             {"Project": "aspen-item10-load", "Run": "r1", "Nonce": "abcd1234"})

    def test_us_east_1_is_refused_before_any_write(self):
        # us-east-1 CreateBucket returns 200 for a bucket this account already owns, so a
        # same-account bucket made after the absence check would be adopted and later deleted.
        fake = FakeAws(region="us-east-1")
        theirs = "aspen-item10-r1-abcd1234"
        real_head = fake.head_bucket

        def head_then_race(Bucket, ExpectedBucketOwner=None):
            try:
                return real_head(Bucket, ExpectedBucketOwner)
            finally:
                fake.buckets.setdefault(theirs, {"theirs": b"keep"})  # appears right after our check
        fake.head_bucket = head_then_race
        with self.assertRaises(L.RefusedError) as caught:
            L.launch(fake, account_id=ACCOUNT, vpc_id=VPC, subnet_ids=SUBNETS, ami="ami-x", run="r1",
                     region="us-east-1", bench_dir=self.bench, state_dir=self.state_dir, now=lambda: NOW,
                     token=lambda: "abcd1234", sleep=lambda s: None, workers=L.WORKERS[:1])
        self.assertIn("us-east-1", str(caught.exception))
        self.assertEqual(fake.mutating(), [])
        self.assertEqual(fake.buckets.get(theirs, {}).get("theirs", b"keep"), b"keep")
        self.assertNotIn(theirs, fake.bucket_tags)
        self.assertFalse(os.path.exists(self.state_path))

    def test_create_never_uses_the_us_east_1_form(self):
        # Second layer under the launch refusal: even if _allocate were reached with us-east-1, the
        # create carries a LocationConstraint, which us-east-1 rejects, so an owned bucket is
        # never silently "created" (200) and then tagged as this run's.
        fake = FakeAws(region="us-east-1", existing_buckets=["aspen-item10-r1-abcd1234"])
        state = {"run": "r1", "nonce": "abcd1234", "region": "us-east-1", "bucket": "aspen-item10-r1-abcd1234",
                 "bucket_status": "absent", "vpc_id": VPC, "subnet_ids": SUBNETS, "instance_ids": [],
                 "expires_at": L.iso(NOW)}
        os.makedirs(self.state_dir)
        with self.assertRaises(Exception):
            L._allocate(fake, state, self.state_path, "ami-x", self.bench, L.WORKERS[:1])
        self.assertNotIn("aspen-item10-r1-abcd1234", fake.bucket_tags)
        self.assertNotIn("put_bucket_tagging", fake.mutating())

    def test_bucket_created_after_absence_check_is_refused_not_adopted(self):
        # Outside us-east-1 the race ends in BucketAlreadyOwnedByYou: refused, never tagged,
        # and teardown leaves it alone.
        fake = FakeAws()
        bucket = "aspen-item10-r1-abcd1234"
        real_head = fake.head_bucket

        def head_then_race(Bucket, ExpectedBucketOwner=None):
            try:
                return real_head(Bucket, ExpectedBucketOwner)
            finally:
                if ExpectedBucketOwner is None:
                    fake.buckets.setdefault(bucket, {"theirs": b"keep"})
        fake.head_bucket = head_then_race
        with self.assertRaises(L.RefusedError):
            self.launch(fake, workers=L.WORKERS[:1])
        self.assertEqual(fake.buckets[bucket], {"theirs": b"keep"})
        self.assertNotIn(bucket, fake.bucket_tags)
        self.assertNotIn("put_bucket_tagging", fake.mutating())
        self.assertNotIn("delete_bucket", fake.mutating())
        self.assertEqual(L.load_state(self.state_path)["bucket_status"], "refused")

    def test_default_region_is_not_us_east_1(self):
        regions = []

        def factory(region):
            regions.append(region)
            return FakeAws(caller=OTHER)  # isolation fails right after the client is built
        with self.assertRaises(L.IsolationError):
            L.main(["launch", "--account-id", ACCOUNT, "--vpc-id", VPC, "--subnet-ids", ",".join(SUBNETS),
                    "--ami", "ami-x", "--run", "r1", "--state-dir", self.state_dir], clients_factory=factory)
        self.assertNotEqual(regions, ["us-east-1"])

    def test_reused_run_name_keeps_both_runs_evidence(self):
        fake = FakeAws()
        states = []
        for nonce, body in (("abcd1234", b'{"run": 1}'), ("beef0000", b'{"run": 2}')):
            L.launch(fake, account_id=ACCOUNT, vpc_id=VPC, subnet_ids=SUBNETS, ami="ami-x", run="same",
                     region="us-east-2", bench_dir=self.bench, state_dir=self.state_dir, now=lambda: NOW,
                     token=lambda nonce=nonce: nonce, sleep=lambda s: None, workers=L.WORKERS[:1])
            path = os.path.join(self.state_dir, f"same-{nonce}.json")
            state = L.load_state(path)
            fake.buckets[state["bucket"]][f"same/{nonce}/results/api-0.json"] = body
            L.teardown(fake, state, path, sleep=lambda s: None)
            states.append((path, state, body))
        self.assertEqual(fake.buckets, {})
        for path, state, body in states:
            with open(os.path.join(self.state_dir, f"same-{state['nonce']}", "results", "api-0.json"), "rb") as f:
                self.assertEqual(f.read(), body)

    def test_evidence_never_overwrites_different_bytes_but_reruns_cleanly(self):
        fake = FakeAws()
        state = self.launched(fake, n=1)
        fake.buckets[state["bucket"]]["r1/abcd1234/results/api-0.json"] = b'{"new": 1}'
        dest = os.path.join(self.state_dir, "r1-abcd1234", "results", "api-0.json")
        os.makedirs(os.path.dirname(dest))
        with open(dest, "wb") as f:
            f.write(b'{"old": 1}')
        with self.assertRaises(L.TeardownError) as caught:
            L.teardown(fake, state, self.state_path, sleep=lambda s: None)
        self.assertEqual([s for s, _ in caught.exception.failures], ["download_evidence", "delete_bucket"])
        with open(dest, "rb") as f:
            self.assertEqual(f.read(), b'{"old": 1}')
        self.assertIn(state["bucket"], fake.buckets)
        with open(dest, "wb") as f:  # an identical earlier copy (a rerun) is fine
            f.write(b'{"new": 1}')
        L.teardown(fake, state, self.state_path, sleep=lambda s: None)
        self.assertEqual(fake.buckets, {})

    def test_vanished_recorded_instance_does_not_block_live_orphan(self):
        fake = FakeAws()
        state = self.launched(fake, n=1)
        fake.add_instance("i-orphan", {"Project": L.PROJECT, "Run": "r1", "Nonce": "abcd1234"}, group="sg-1")
        del fake.instances["i-0001"]  # expired long ago: AWS no longer knows the recorded ID
        L.teardown(fake, state, self.state_path, sleep=lambda s: None)
        self.assertEqual(fake.instances["i-orphan"]["State"]["Name"], "terminated")
        self.assertEqual(fake.sgs, set())
        self.assertIn("torn_down_at", L.load_state(self.state_path))

    def test_standalone_teardown_after_crash_finds_unrecorded_resources(self):
        fake = FakeAws()
        state = self.launched(fake, n=1)
        fake.add_security_group("sg-lost", {"Project": L.PROJECT, "Run": "r1", "Nonce": "abcd1234"})
        fake.add_security_group("sg-foreign", {"Project": L.PROJECT, "Run": "r1", "Nonce": "ffff0000"})
        fake.add_security_group("sg-othervpc", {"Project": L.PROJECT, "Run": "r1", "Nonce": "abcd1234"}, vpc="vpc-x")
        state.update(security_group=None, bucket_status="creating")
        L.teardown(fake, state, self.state_path, sleep=lambda s: None)
        self.assertEqual(fake.sgs, {"sg-foreign", "sg-othervpc"})
        self.assertEqual(fake.buckets, {})

    def test_refused_bucket_is_never_touched_by_teardown(self):
        fake = FakeAws()
        real = fake.create_bucket

        def taken(Bucket, **kw):
            real(Bucket, **kw)  # someone else's bucket appears between head_bucket and create
            raise ClientError("BucketAlreadyOwnedByYou")
        fake.create_bucket = taken
        with self.assertRaises(L.RefusedError) as caught:
            self.launch(fake)
        self.assertIn("aspen-item10-r1-abcd1234", fake.buckets)
        self.assertNotIn("delete_bucket", fake.mutating())
        self.assertEqual(L.load_state(self.state_path)["bucket_status"], "refused")
        # refused means never looked at again: not even probed for ownership tags
        self.assertNotIn("get_bucket_tagging", [n for n, _ in fake.calls])
        self.assertFalse(getattr(caught.exception, "__notes__", None))



if __name__ == "__main__":
    unittest.main()
