#!/usr/bin/env python3
"""Tests for where setup-spiced reads spiced, and that a read of trunk's build is attested.

A build lives where the ref that built it stores it: trunk's builds at the shared keys,
another ref's under `artifacts/untrusted/<repository id>/<ref>/`. So the place to read is the
build's ref, never the reader's. The action's own steps run here, from action.yml, against a
local Git server holding trunk and a feature commit and a stand-in `mc` that answers only
for the keys a test stores, so nothing is downloaded.
"""

import os
import subprocess
import tempfile
import unittest
from pathlib import Path

import yaml

ACTION = Path(__file__).resolve().parent.parent / "actions" / "setup-spiced" / "action.yml"
REPOSITORY_ID = "394089041"
PULL_REQUEST = "refs/pull/16720/merge"


def git(cwd, *args):
    return subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True).stdout.strip()


# Logs every call, and `stat` succeeds only for a key listed in $MC_STORED.
FAKE_MC = """#!/usr/bin/env bash
echo "mc $*" >> "$MC_LOG"
case "$1" in
  stat) grep -qxF -- "$2" "$MC_STORED" ;;
  cp) grep -qxF -- "$2" "$MC_STORED" && : > "$3" ;;
esac
"""


class SetupSpicedTest(unittest.TestCase):
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
        git(work, "checkout", "-qb", "feature")
        (work / "f").write_text("feature")
        git(work, "commit", "-qam", "feature")
        cls.feature = git(work, "rev-parse", "HEAD")
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
        action = yaml.safe_load(ACTION.read_text())
        cls.steps = {s.get("id") or s["name"]: s for s in action["runs"]["steps"]}
        cls.bin = root / "bin"
        cls.bin.mkdir()
        (cls.bin / "mc").write_text(FAKE_MC)
        (cls.bin / "mc").chmod(0o755)

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def setUp(self):
        self.dir = Path(tempfile.mkdtemp(dir=self.tmp.name))
        self.mc_log = self.dir / "mc.log"
        self.stored = self.dir / "stored"
        self.mc_log.write_text("")
        self.stored.write_text("")

    def key(self, prefix, commit):
        return f"spice-minio/artifacts/{prefix}spiced/linux-x86_64/{commit}/spiced_linux_x86_64.tar.gz"

    def run_step(self, step, env):
        script = self.dir / "step.sh"
        script.write_text(self.steps[step]["run"])
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
            "PLATFORM_OS": "linux",
            "PLATFORM_ARCH": "x86_64",
            "PLATFORM_EXT": "tar.gz",
            "INPUT_WITH_ODBC": "",
            **env,
        }
        proc = subprocess.run(["bash", "-e", str(script)], cwd=self.checkout, env=full_env,
                              capture_output=True, text=True, timeout=60)
        values = dict(line.split("=", 1) for line in github_output.read_text().splitlines() if "=" in line)
        values.update(dict(line.split("=", 1) for line in github_env.read_text().splitlines() if "=" in line))
        return proc, values

    def locate(self, github_ref, ref="refs/heads/trunk", spiced_commit="", verify="false"):
        return self.run_step("locate", {"GITHUB_REF": github_ref, "REF": ref, "INPUT_SPICED_COMMIT": spiced_commit,
                                   "INPUT_VERIFY": verify})

    def test_a_pull_request_finds_trunks_latest_build_at_trunks_keys(self):
        # The cold-start regression: a pull request looked for trunk's builds under its own prefix.
        proc, located = self.locate(PULL_REQUEST)
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertEqual(located, {"ref": "refs/heads/trunk", "prefix": "", "trunk": "true"})
        self.stored.write_text(self.key("", self.trunk) + "\n")
        proc, found = self.run_step("Find the latest built spiced commit", {"REF": "refs/heads/trunk", "SOURCE_PREFIX": ""})
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertEqual(found["SPICED_COMMIT"], self.trunk)
        self.assertIn(f"mc stat {self.key('', self.trunk)}", self.mc_log.read_text())
        self.assertNotIn("untrusted", self.mc_log.read_text())

    def test_a_trunk_commit_is_read_at_trunks_keys_from_any_ref(self):
        for github_ref in (PULL_REQUEST, "refs/heads/feature", "refs/heads/trunk"):
            with self.subTest(github_ref=github_ref):
                proc, located = self.locate(github_ref, spiced_commit=self.trunk)
                self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
                self.assertEqual(located, {"ref": "refs/heads/trunk", "prefix": "", "trunk": "true"})

    def test_a_branch_build_is_read_where_that_branch_stored_it(self):
        proc, located = self.locate("refs/heads/feature", spiced_commit=self.feature)
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertEqual(located, {"ref": "refs/heads/trunk", "prefix": f"untrusted/{REPOSITORY_ID}/refs/heads/feature/", "trunk": "false"})
        proc, located = self.locate(PULL_REQUEST, ref="refs/heads/release/1.0")
        self.assertEqual(located, {"ref": "refs/heads/release/1.0", "prefix": f"untrusted/{REPOSITORY_ID}/refs/heads/release/1.0/", "trunk": "false"})
        self.stored.write_text(self.key(located["prefix"], self.trunk) + "\n")
        self.run_step("download", {"SOURCE_PREFIX": located["prefix"], "SPICED_COMMIT": self.trunk})
        self.assertIn(f"mc cp {self.key(located['prefix'], self.trunk)} ", self.mc_log.read_text())

    def test_a_bare_branch_name_is_that_branch(self):
        proc, located = self.locate(PULL_REQUEST, ref="trunk")
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertEqual(located, {"ref": "refs/heads/trunk", "prefix": "", "trunk": "true"})
        proc, located = self.locate(PULL_REQUEST, ref="release/1.0")
        self.assertEqual(located["ref"], "refs/heads/release/1.0")
        self.assertEqual(located["prefix"], f"untrusted/{REPOSITORY_ID}/refs/heads/release/1.0/")

    def test_a_ref_that_is_not_a_plain_ref_is_refused(self):
        for ref in ("refs/heads/x\nprefix<<E\nE\nz=", "refs/heads/a/../../../../", "refs/heads//x", "refs/heads/x/",
                    "refs/heads/x y"):
            with self.subTest(ref=ref):
                proc, located = self.locate(PULL_REQUEST, ref=ref)
                self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
                self.assertIn("is not a plain ref", proc.stdout)
                self.assertEqual(located, {})

    def test_no_spiced_commit_means_no_download(self):
        proc, outputs = self.run_step("output_spiced_commit", {"SPICED_COMMIT": ""})
        self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
        self.assertIn("no spiced commit was chosen", proc.stdout)

    def test_a_full_role_job_never_takes_a_branch_build(self):
        proc, located = self.locate("refs/heads/trunk", ref="refs/heads/release/1.0", verify="true")
        self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
        self.assertIn("runs only a spiced built and attested on trunk", proc.stdout)
        self.assertEqual(located, {})

    def test_a_branch_commit_named_on_trunk_is_read_at_trunks_keys_and_must_verify(self):
        # A trunk run given a branch's commit reads trunk's keys, where no attested build of it is.
        proc, located = self.locate("refs/heads/trunk", spiced_commit=self.feature)
        self.assertEqual(located, {"ref": "refs/heads/trunk", "prefix": "", "trunk": "true"})

    def test_every_read_at_trunks_keys_is_verified_whatever_the_role(self):
        verify = self.steps["Verify spiced's build attestation"]
        self.assertEqual(verify["if"], "steps.locate.outputs.trunk == 'true' || inputs.verify == 'true'")
        self.assertEqual(verify["with"]["source-commit"], "${{ steps.output_spiced_commit.outputs.SPICED_COMMIT }}")
        order = list(self.steps)
        self.assertLess(order.index("download"), order.index("Verify spiced's build attestation"))
        self.assertLess(order.index("Verify spiced's build attestation"), order.index("Install spiced"))

    def test_no_read_uses_the_readers_own_prefix(self):
        for step in ("Find the latest built spiced commit", "download"):
            with self.subTest(step=step):
                self.assertNotIn("CI_STORE_PREFIX", self.steps[step]["run"])
                self.assertIn("${SOURCE_PREFIX}spiced/", self.steps[step]["run"])


if __name__ == "__main__":
    unittest.main()
