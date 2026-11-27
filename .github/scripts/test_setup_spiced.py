#!/usr/bin/env python3
"""Tests that this branch stores its spiced builds under its own CI store prefix, and that
setup-spiced reads a build where the ref that built it stored it.

Off trunk the CI store lets a run write only `artifacts/untrusted/<repository id>/<ref>/`, so
build_and_release.yml uploads there (ci-store-prefix), and attests each spiced it uploads. A
reader finds a release branch's build under that branch's prefix, and trunk's builds at the
shared keys. The actions' own steps run here, from their action.yml files, against a local Git
server holding trunk and a release branch and a stand-in `mc` that answers only for the keys a
test stores, so nothing is downloaded.
"""

import io
import os
import re
import subprocess
import tarfile
import tempfile
import unittest
from pathlib import Path

import yaml

GITHUB_DIR = Path(__file__).resolve().parent.parent
SETUP_SPICED = GITHUB_DIR / "actions" / "setup-spiced" / "action.yml"
CI_STORE_PREFIX = GITHUB_DIR / "actions" / "ci-store-prefix" / "action.yml"
BUILD = GITHUB_DIR / "workflows" / "build_and_release.yml"
REPOSITORY_ID = "394089041"
RELEASE = "refs/heads/release/2.4"
PULL_REQUEST = "refs/pull/16720/merge"
FIND = "Find the latest built spiced commit (Linux/macOS)"
DOWNLOAD = "Download spiced from MinIO (Linux/macOS)"


def git(cwd, *args):
    return subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True).stdout.strip()


# Logs every call. `stat` succeeds only for a key listed in $MC_STORED, and `cp` copies
# $MC_ARCHIVE for such a key.
FAKE_MC = """#!/usr/bin/env bash
echo "mc $*" >> "$MC_LOG"
case "$1" in
  stat) grep -qxF -- "$2" "$MC_STORED" ;;
  cp) grep -qxF -- "$2" "$MC_STORED" && cp "$MC_ARCHIVE" "$3" ;;
esac
"""

# Logs the install instead of running it.
FAKE_SUDO = """#!/usr/bin/env bash
echo "sudo $*" >> "$MC_LOG"
"""

# The expressions the Linux steps of setup-spiced read, as a Linux runner fills them in.
LINUX_EXPRESSIONS = {
    "steps.platform.outputs.os": "linux",
    "steps.platform.outputs.arch": "x86_64",
    "steps.platform.outputs.ext": "tar.gz",
    "inputs.with_odbc": "",
}


def render(script, expressions):
    def value(match):
        name = match.group(1).strip()
        if name not in expressions:
            raise AssertionError(f"unexpected expression ${{{{ {name} }}}}")
        return expressions[name]

    return re.sub(r"\$\{\{(.*?)\}\}", value, script)


def steps_by_name(steps):
    return {s.get("id") or s["name"]: s for s in steps}


class StoreTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        root = Path(cls.tmp.name).resolve()
        work = root / "work"
        work.mkdir()
        git(work, "init", "-q", "-b", "trunk")
        git(work, "config", "user.email", "ci@example.invalid")
        git(work, "config", "user.name", "ci")
        for n in range(3):
            (work / "f").write_text(str(n))
            git(work, "add", "f")
            git(work, "commit", "-qm", f"trunk {n}")
        cls.trunk = git(work, "rev-parse", "HEAD")
        git(work, "checkout", "-qb", "release/2.4")
        for n in range(2):
            (work / "f").write_text(f"release {n}")
            git(work, "commit", "-qam", f"release {n}")
        cls.release = git(work, "rev-parse", "HEAD")
        cls.release_parent = git(work, "rev-parse", "HEAD~1")
        git(work, "checkout", "-q", "trunk")
        server = root / "server"
        (server / "owner").mkdir(parents=True)
        bare = server / "owner" / "repo.git"
        git(root, "clone", "-q", "--bare", str(work), str(bare))
        git(bare, "config", "uploadpack.allowFilter", "true")
        cls.server = f"file://{server}"
        # The job's checkout, whose `origin` the find step fetches `ref`'s last commits from.
        cls.checkout = root / "checkout"
        git(root, "clone", "-q", str(bare), str(cls.checkout))
        cls.steps = steps_by_name(yaml.safe_load(SETUP_SPICED.read_text())["runs"]["steps"])
        cls.prefix_steps = steps_by_name(yaml.safe_load(CI_STORE_PREFIX.read_text())["runs"]["steps"])
        cls.build = yaml.safe_load(BUILD.read_text())["jobs"]["build"]
        cls.bin = root / "bin"
        cls.bin.mkdir()
        for name, text in (("mc", FAKE_MC), ("sudo", FAKE_SUDO)):
            (cls.bin / name).write_text(text)
            (cls.bin / name).chmod(0o755)
        cls.archive = root / "spiced.tar.gz"
        with tarfile.open(cls.archive, "w:gz") as tar:
            data = b"#!/bin/sh\n"
            info = tarfile.TarInfo("spiced")
            info.size = len(data)
            info.mode = 0o755
            tar.addfile(info, io.BytesIO(data))

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def setUp(self):
        self.dir = Path(tempfile.mkdtemp(dir=self.tmp.name))
        self.mc_log = self.dir / "mc.log"
        self.stored = self.dir / "stored"
        self.mc_log.write_text("")
        self.stored.write_text("")

    def key(self, prefix, commit, odbc=False):
        archive = "spiced_odbc_linux_x86_64.tar.gz" if odbc else "spiced_linux_x86_64.tar.gz"
        return f"spice-minio/artifacts/{prefix}spiced/linux-x86_64/{commit}/{archive}"

    def run_script(self, script, env):
        path = self.dir / "step.sh"
        path.write_text(script)
        github_output = self.dir / "github_output"
        github_output.write_text("")
        github_env = self.dir / "github_env"
        github_env.write_text("")
        full_env = {
            "PATH": f"{self.bin}{os.pathsep}{os.environ['PATH']}",
            "HOME": str(self.dir),
            "RUNNER_TEMP": str(self.dir),
            "GITHUB_OUTPUT": str(github_output),
            "GITHUB_ENV": str(github_env),
            "GITHUB_SERVER_URL": self.server,
            "GITHUB_REPOSITORY": "owner/repo",
            "GITHUB_REPOSITORY_ID": REPOSITORY_ID,
            "GH_TOKEN": "test-token",
            "MC_LOG": str(self.mc_log),
            "MC_STORED": str(self.stored),
            "MC_ARCHIVE": str(self.archive),
            **env,
        }
        proc = subprocess.run(["bash", "-e", str(path)], cwd=self.dir, env=full_env,
                              capture_output=True, text=True, timeout=60)
        values = dict(line.split("=", 1) for line in github_output.read_text().splitlines() if "=" in line)
        values.update(dict(line.split("=", 1) for line in github_env.read_text().splitlines() if "=" in line))
        return proc, values

    def run_step(self, step, env):
        script = render(self.steps[step]["run"], LINUX_EXPRESSIONS)
        if step == FIND:
            # The find step fetches from the job's checkout.
            script = f"cd {self.checkout}\n{script}"
        return self.run_script(script, env)

    def locate(self, github_ref, ref="refs/heads/trunk", spiced_commit="", event="push", base_ref=""):
        return self.run_step("locate", {"GITHUB_REF": github_ref, "REF": ref, "INPUT_SPICED_COMMIT": spiced_commit,
                                        "RUN_EVENT": event, "PULL_REQUEST_BASE": base_ref if event == "pull_request" else ""})

    def store_prefix(self, github_ref, repository_id=REPOSITORY_ID):
        return self.run_script(self.prefix_steps["prefix"]["run"],
                               {"GITHUB_REF": github_ref, "GITHUB_REPOSITORY_ID": repository_id, "SPICEPOD": ""})

    def uploads(self):
        return [s for s in self.build["steps"] if s.get("uses") == "./.github/actions/upload-to-minio"]


class CiStorePrefixTest(StoreTest):
    def test_trunk_uses_the_shared_keys_and_every_other_ref_its_own(self):
        for github_ref, prefix in (
            ("refs/heads/trunk", ""),
            (RELEASE, f"untrusted/{REPOSITORY_ID}/{RELEASE}/"),
            (PULL_REQUEST, f"untrusted/{REPOSITORY_ID}/{PULL_REQUEST}/"),
            ("refs/tags/v2.4.1", f"untrusted/{REPOSITORY_ID}/refs/tags/v2.4.1/"),
        ):
            with self.subTest(github_ref=github_ref):
                proc, values = self.store_prefix(github_ref)
                self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
                self.assertEqual(values, {"prefix": prefix, "CI_STORE_PREFIX": prefix})

    def test_no_repository_id_or_ref_fails_closed(self):
        proc, values = self.store_prefix(RELEASE, repository_id="")
        self.assertEqual(proc.returncode, 1)
        self.assertIn("not a repository id", proc.stdout)
        self.assertEqual(values, {})
        proc, values = self.store_prefix("trunk")
        self.assertEqual(proc.returncode, 1)
        self.assertIn("not a ref", proc.stdout)
        self.assertEqual(values, {})


class BuildStoresUnderItsPrefixTest(StoreTest):
    def test_every_upload_is_under_the_runs_prefix_once_it_is_chosen(self):
        order = [s.get("name") or s.get("uses") for s in self.build["steps"]]
        prefix_step = next(s for s in self.build["steps"] if s.get("uses") == "./.github/actions/ci-store-prefix")
        self.assertEqual(prefix_step["if"], "matrix.target.use_minio")
        uploads = self.uploads()
        self.assertEqual(len(uploads), 3)
        for upload in uploads:
            with self.subTest(upload=upload["name"]):
                self.assertTrue(upload["with"]["destination_path"].startswith(
                    "spice-minio/artifacts/${{ env.CI_STORE_PREFIX }}"), upload["with"]["destination_path"])
                self.assertTrue(upload["if"].startswith("matrix.target.use_minio && "), upload["if"])
                self.assertLess(order.index(prefix_step["name"]), order.index(upload["name"]))

    def test_each_stored_spiced_is_attested_before_its_upload_on_a_release_branch(self):
        permissions = self.build["permissions"]
        self.assertEqual(permissions.get("id-token"), "write")
        self.assertEqual(permissions.get("attestations"), "write")
        steps = self.build["steps"]
        names = [s.get("name") for s in steps]
        for upload in self.uploads():
            source = upload["with"]["source_path"]
            if not source.startswith("spiced"):
                continue
            with self.subTest(source=source):
                attest = [s for s in steps if str(s.get("uses", "")).startswith("actions/attest-build-provenance@")
                          and s["with"]["subject-path"] == source]
                self.assertEqual(len(attest), 1, source)
                attest = attest[0]
                self.assertRegex(attest["uses"], r"^actions/attest-build-provenance@[0-9a-f]{40}$")
                self.assertEqual(attest["if"],
                                 f"{upload['if']} && startsWith(github.ref, 'refs/heads/release/')")
                self.assertLess(names.index(attest["name"]), names.index(upload["name"]))

    def test_the_key_a_release_build_writes_is_the_key_its_readers_read(self):
        # The release branch builds commit C and stores it. Its leg of the daily dispatch, a run
        # on the branch that trunk's dispatch starts, finds C as the branch's latest build, and
        # the bench run that leg dispatches on the branch downloads C.
        proc, values = self.store_prefix(RELEASE)
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        written = []
        for upload in self.uploads():
            if not upload["with"]["source_path"].startswith("spiced"):
                continue
            destination = upload["with"]["destination_path"].replace(
                "${{ env.CI_STORE_PREFIX }}", values["CI_STORE_PREFIX"]).replace("${{ github.sha }}", self.release)
            written.append(destination + upload["with"]["source_path"])
        prefix = f"untrusted/{REPOSITORY_ID}/{RELEASE}/"
        self.assertEqual(written, [self.key(prefix, self.release), self.key(prefix, self.release, odbc=True)])
        self.stored.write_text(written[0] + "\n")

        # The leg's read: no commit, its own branch as `ref`.
        proc, located = self.locate(RELEASE, ref=RELEASE)
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertEqual(located, {"ref": RELEASE, "prefix": prefix})
        proc, found = self.run_step(FIND, {"REF": located["ref"], "SOURCE_PREFIX": located["prefix"]})
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertEqual(found["SPICED_COMMIT"], self.release)

        # The bench run on the branch: the commit the dispatch found, `ref` left at trunk.
        proc, located = self.locate(RELEASE, spiced_commit=self.release)
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertEqual(located["prefix"], prefix)
        proc, _ = self.run_step(DOWNLOAD, {"SOURCE_PREFIX": located["prefix"], "SPICED_COMMIT": self.release})
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        log = self.mc_log.read_text()
        self.assertIn(f"mc cp {written[0]} ", log)
        self.assertIn("sudo cp ./spiced /usr/local/bin/spiced", log)


class ToolBuildsStoreUnderThePrefixTest(unittest.TestCase):
    """The testoperator and spicepod-validator binaries the release leg's jobs build and reuse."""

    def test_each_tool_build_reads_and_writes_under_the_runs_prefix(self):
        for name in ("build-testoperator", "build-spicepod-validator", "build-spidapter"):
            with self.subTest(action=name):
                steps = yaml.safe_load((GITHUB_DIR / "actions" / name / "action.yml").read_text())["runs"]["steps"]
                self.assertEqual(steps[0].get("uses"), "./.github/actions/ci-store-prefix")
                keys = [line for step in steps for line in str(step.get("run", "")).splitlines()
                        if "spice-minio/artifacts/" in line]
                self.assertEqual(len(keys), 3, keys)
                for line in keys:
                    self.assertIn("spice-minio/artifacts/${CI_STORE_PREFIX}", line)


class MssqlBenchImageTest(unittest.TestCase):
    """The spice-mssql-bench image this branch's benchmark legs run as a service."""

    def test_every_job_that_runs_it_pins_its_digest_and_only_trunk_publishes_it(self):
        pinned = re.compile(r"'ghcr\.io/spiceai/spice-mssql-bench@sha256:[0-9a-f]{64}'")
        users = 0
        for workflow in sorted((GITHUB_DIR / "workflows").glob("*.yml")):
            text = workflow.read_text()
            if workflow.name == "spice_mssql_tpch_bench_docker.yml" or "spice-mssql-bench" not in text:
                continue
            users += 1
            for line in text.splitlines():
                if "spice-mssql-bench" in line and "image:" in line:
                    self.assertRegex(line, pinned)
        self.assertEqual(users, 2)
        build = yaml.safe_load((GITHUB_DIR / "workflows" / "spice_mssql_tpch_bench_docker.yml").read_text())
        first = build["jobs"]["build"]["steps"][0]
        self.assertEqual(first["name"], "Refuse a run off trunk")
        self.assertIn('[ "$GITHUB_REF" != refs/heads/trunk ]', first["run"])


class StoreWritersUseThePrefixTest(unittest.TestCase):
    """This branch's other writers to the store's shared prefixes, which refuse its writes."""

    def test_the_integration_test_archive_is_under_the_runs_prefix(self):
        workflow = yaml.safe_load((GITHUB_DIR / "workflows" / "integration.yml").read_text())
        jobs_using = 0
        for name, job in workflow["jobs"].items():
            steps = job.get("steps") or []
            runs = [str(step.get("run", "")) for step in steps]
            using = [i for i, run in enumerate(runs) if "INTEGRATION_TESTS_ARCHIVE_S3_URI" in run]
            if not using:
                continue
            jobs_using += 1
            with self.subTest(job=name):
                uses = [step.get("uses") for step in steps]
                self.assertIn("./.github/actions/ci-store-prefix", uses)
                self.assertLess(uses.index("./.github/actions/ci-store-prefix"), using[0])
                for i in using:
                    self.assertIn("spice-minio/build/test_binaries/${CI_STORE_PREFIX?}", runs[i])
                    self.assertNotIn("spice-minio/${INTEGRATION_TESTS_ARCHIVE_S3_URI#s3://}", runs[i])
        self.assertGreater(jobs_using, 1)

    def test_the_compiler_cache_keys_are_under_the_runs_prefix(self):
        steps = yaml.safe_load((GITHUB_DIR / "actions" / "setup-sccache" / "action.yml").read_text())["runs"]["steps"]
        self.assertEqual(steps[0].get("uses"), "./.github/actions/ci-store-prefix")
        configs = [line for step in steps for line in str(step.get("run", "")).splitlines() if "[cache.s3]" in line]
        self.assertEqual(len(configs), 3)
        for line in configs:
            self.assertIn("key_prefix", line)
            self.assertIn("CI_STORE_PREFIX", line)

    def test_the_cold_start_fixture_is_seeded_under_the_runs_prefix_and_trunks_copy_is_read(self):
        steps = yaml.safe_load((GITHUB_DIR / "actions" / "seed-cold-start-fixture" / "action.yml").read_text())["runs"]["steps"]
        self.assertEqual(steps[0].get("uses"), "./.github/actions/ci-store-prefix")
        # The E2E job only reads the fixture, and a merge-queue ref is new for every entry, so it
        # reads trunk's copy, which no run off trunk can write.
        job = yaml.safe_load((GITHUB_DIR / "workflows" / "e2e_test_ci.yml").read_text())["jobs"]["test_cold_start_unkeyed_append"]
        self.assertNotIn("./.github/actions/ci-store-prefix", [step.get("uses") for step in job["steps"]])
        self.assertIn("spice-minio/benchmarks/cold_start/audit_events_3m/_SEEDED", yaml.safe_dump(job, width=10**6))


class SetupSpicedTest(StoreTest):
    def test_trunks_latest_build_is_read_at_trunks_keys(self):
        for github_ref in (PULL_REQUEST, RELEASE, "refs/heads/trunk"):
            with self.subTest(github_ref=github_ref):
                proc, located = self.locate(github_ref)
                self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
                self.assertEqual(located, {"ref": "refs/heads/trunk", "prefix": ""})
        self.stored.write_text(self.key("", self.trunk) + "\n")
        proc, found = self.run_step(FIND, {"REF": "refs/heads/trunk", "SOURCE_PREFIX": ""})
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertEqual(found["SPICED_COMMIT"], self.trunk)
        self.assertNotIn("untrusted", self.mc_log.read_text())

    def test_a_trunk_commit_is_read_at_trunks_keys_from_any_ref(self):
        for github_ref in (PULL_REQUEST, RELEASE, "refs/heads/trunk"):
            with self.subTest(github_ref=github_ref):
                proc, located = self.locate(github_ref, spiced_commit=self.trunk)
                self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
                self.assertEqual(located, {"ref": "refs/heads/trunk", "prefix": ""})

    def test_a_release_commit_is_read_under_the_release_branchs_prefix(self):
        prefix = f"untrusted/{REPOSITORY_ID}/{RELEASE}/"
        for ref in ("refs/heads/trunk", RELEASE):
            with self.subTest(ref=ref):
                proc, located = self.locate(RELEASE, ref=ref, spiced_commit=self.release)
                self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
                self.assertEqual(located["prefix"], prefix)

    def test_a_run_never_takes_another_refs_build(self):
        # A build of a ref other than trunk runs only in a run of that ref, with that ref's
        # credentials: a trunk run's store credentials write trunk's keys.
        for github_ref, ref, commit in (
            ("refs/heads/trunk", RELEASE, ""),
            ("refs/heads/trunk", "release/2.4", ""),
            ("refs/heads/trunk", RELEASE, "release"),
            ("refs/heads/trunk", "refs/heads/trunk", "release"),
            (PULL_REQUEST, RELEASE, ""),
            (PULL_REQUEST, RELEASE, "release"),
            ("refs/heads/feature", RELEASE, ""),
        ):
            with self.subTest(github_ref=github_ref, ref=ref, commit=commit):
                proc, located = self.locate(github_ref, ref=ref, spiced_commit=self.release if commit else "")
                self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
                self.assertIn("runs only a spiced that trunk or this run's own ref built", proc.stdout)
                self.assertEqual(located, {})

    def test_a_pull_request_reads_its_base_branchs_build(self):
        prefix = f"untrusted/{REPOSITORY_ID}/{RELEASE}/"
        proc, located = self.locate(PULL_REQUEST, ref=RELEASE, event="pull_request", base_ref="release/2.4")
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertEqual(located["prefix"], prefix)
        # Only its own base, and only for a pull_request event: the base name comes from the runner.
        for event, base_ref in (("pull_request", "release/2.5"), ("pull_request", ""), ("push", "release/2.4"),
                                ("pull_request_target", "release/2.4"), ("workflow_dispatch", "release/2.4")):
            with self.subTest(event=event, base_ref=base_ref):
                proc, located = self.locate(PULL_REQUEST, ref=RELEASE, event=event, base_ref=base_ref)
                self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
                self.assertEqual(located, {})

    def test_the_pull_request_check_reads_the_event_not_the_environment(self):
        env = yaml.safe_load((Path(__file__).resolve().parents[1] / "actions" / "setup-spiced" / "action.yml").read_text())
        locate = next(s for s in env["runs"]["steps"] if s.get("id") == "locate")
        self.assertEqual(locate["env"]["RUN_EVENT"], "${{ github.event_name }}")
        self.assertEqual(locate["env"]["PULL_REQUEST_BASE"], "${{ github.event_name == 'pull_request' && github.base_ref || '' }}")
        self.assertNotIn("GITHUB_BASE_REF", locate["run"])
        self.assertNotIn("GITHUB_EVENT_NAME", locate["run"])
        # GITHUB_* values a step could add to the environment change nothing.
        proc, located = self.run_step("locate", {"GITHUB_REF": PULL_REQUEST, "REF": RELEASE, "INPUT_SPICED_COMMIT": "",
                                                 "RUN_EVENT": "push", "PULL_REQUEST_BASE": "",
                                                 "GITHUB_EVENT_NAME": "pull_request", "GITHUB_BASE_REF": "release/2.4"})
        self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
        self.assertEqual(located, {})

    def test_release_24s_cold_start_reads_the_branchs_own_build(self):
        workflow = yaml.safe_load((Path(__file__).resolve().parents[1] / "workflows" / "testoperator_run_cold_start.yml").read_text())
        step = next(s for s in workflow["jobs"]["cold-start"]["steps"] if s.get("id") == "setup-spiced")
        self.assertEqual(
            step["with"]["ref"],
            "${{ github.event_name == 'pull_request' && format('refs/heads/{0}', github.base_ref) || github.ref }}",
        )
        # What that ref is on each run: the branch's own prefix, never trunk's keys.
        for github_ref, event, base_ref, ref in ((RELEASE, "workflow_dispatch", "", RELEASE),
                                                 (PULL_REQUEST, "pull_request", "release/2.4", RELEASE)):
            with self.subTest(event=event):
                proc, located = self.locate(github_ref, ref=ref, event=event, base_ref=base_ref)
                self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
                self.assertEqual(located["prefix"], f"untrusted/{REPOSITORY_ID}/{RELEASE}/")

    def test_the_latest_build_is_the_newest_stored_commit_under_the_prefix(self):
        prefix = f"untrusted/{REPOSITORY_ID}/{RELEASE}/"
        # The head has no build (a docs-only commit, say), and its parent's build is also at
        # the shared key, where this branch no longer stores it.
        self.stored.write_text(self.key(prefix, self.release_parent) + "\n" + self.key("", self.release) + "\n")
        proc, found = self.run_step(FIND, {"REF": RELEASE, "SOURCE_PREFIX": prefix})
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertEqual(found["SPICED_COMMIT"], self.release_parent)

    def test_no_build_of_the_ref_says_which_ref_and_where(self):
        # A ref with no build of its own, such as a feature branch's dispatch of cold-start, fails
        # with the ref and the prefix it searched, not by testing trunk's runtime instead.
        prefix = f"untrusted/{REPOSITORY_ID}/{RELEASE}/"
        self.stored.write_text(self.key("", self.release) + "\n")
        proc, found = self.run_step(FIND, {"REF": RELEASE, "SOURCE_PREFIX": prefix})
        self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
        self.assertNotIn("SPICED_COMMIT", found)
        self.assertIn(f"::error::No spiced build of {RELEASE}'s last 10 commits is stored", proc.stdout)
        self.assertIn(f"under artifacts/{prefix}spiced/", proc.stdout)
        self.assertIn(f"run build_and_release.yml on {RELEASE} first", proc.stdout)

    def test_a_bare_branch_name_is_that_branch(self):
        proc, located = self.locate(PULL_REQUEST, ref="trunk")
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertEqual(located, {"ref": "refs/heads/trunk", "prefix": ""})
        proc, located = self.locate(RELEASE, ref="release/2.4")
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertEqual(located, {"ref": RELEASE, "prefix": f"untrusted/{REPOSITORY_ID}/{RELEASE}/"})

    def test_a_ref_that_is_not_a_plain_ref_is_refused(self):
        for ref in ("refs/heads/x\nprefix<<E\nE\nz=", "refs/heads/a/../../../../", "refs/heads//x", "refs/heads/x/",
                    "refs/heads/x y", "refs/heads/x$(id)"):
            with self.subTest(ref=ref):
                proc, located = self.locate(PULL_REQUEST, ref=ref)
                self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
                self.assertIn("is not a plain ref", proc.stdout)
                self.assertEqual(located, {})

    def test_a_spiced_commit_that_is_not_a_full_hash_is_refused(self):
        for commit in (f"{self.release}\nprefix=", self.release[:12], f"{self.release[:39]}$(id)",
                       "../../../../trunk", self.release.upper()):
            with self.subTest(commit=commit):
                proc, located = self.locate(RELEASE, spiced_commit=commit)
                self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
                self.assertIn("must be a full 40-character commit hash", proc.stdout)
                self.assertEqual(located, {})

    def test_every_read_uses_the_located_prefix(self):
        for step in (FIND, "Find the latest built spiced commit (Windows)", DOWNLOAD,
                     "Download spiced from MinIO (Windows)"):
            with self.subTest(step=step):
                definition = self.steps[step]
                self.assertEqual(definition["env"]["SOURCE_PREFIX"], "${{ steps.locate.outputs.prefix }}")
                self.assertNotIn("CI_STORE_PREFIX", definition["run"])
                self.assertNotIn("artifacts/spiced/", definition["run"])
                self.assertRegex(definition["run"], r"artifacts/(\$\{SOURCE_PREFIX\}|\$\(\$env:SOURCE_PREFIX\))spiced/")
        order = list(self.steps)
        self.assertLess(order.index("locate"), order.index(FIND))


if __name__ == "__main__":
    unittest.main()
