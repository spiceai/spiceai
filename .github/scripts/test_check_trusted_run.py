#!/usr/bin/env python3
"""Tests that check-trusted-run gives a trunk run only builds of trunk commits (`for-ref`).

A trunk run's store credentials write trunk's keys, so trunk_tests and spicescript_dispatch,
which run another run's build, take only a build of a commit on trunk when they run on trunk;
a release branch's build is tested in a run on that branch. The action's own step runs here,
from action.yml, against a local Git server holding trunk and a release branch and a stand-in
`gh` that answers the run lookup, so nothing reaches GitHub.
"""

import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

import yaml

ACTION = Path(__file__).resolve().parent.parent / "actions" / "check-trusted-run" / "action.yml"
TRUNK_TESTS = Path(__file__).resolve().parent.parent / "workflows" / "trunk_tests.yml"


def git(cwd, *args):
    return subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True).stdout.strip()


# Answers the run lookup with $FAKE_RUN, the six fields the action reads, one per line.
FAKE_GH = """#!/usr/bin/env bash
case "$*" in
  *actions/runs/*) cat "$FAKE_RUN" ;;
  *) echo "unexpected gh call: $*" >&2; exit 1 ;;
esac
"""


class CheckTrustedRunTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        root = Path(cls.tmp.name).resolve()
        work = root / "work"
        work.mkdir()
        git(work, "init", "-q", "-b", "trunk")
        git(work, "config", "user.email", "ci@example.invalid")
        git(work, "config", "user.name", "ci")
        (work / "f").write_text("0")
        git(work, "add", "f")
        git(work, "commit", "-qm", "trunk")
        cls.trunk = git(work, "rev-parse", "HEAD")
        git(work, "checkout", "-qb", "release/1.0")
        (work / "f").write_text("release")
        git(work, "commit", "-qam", "release fix")
        cls.release = git(work, "rev-parse", "HEAD")
        git(work, "checkout", "-q", "trunk")
        server = root / "server"
        (server / "owner").mkdir(parents=True)
        bare = server / "owner" / "repo.git"
        git(root, "clone", "-q", "--bare", str(work), str(bare))
        git(bare, "config", "uploadpack.allowFilter", "true")
        cls.server = f"file://{server}"
        steps = yaml.safe_load(ACTION.read_text())["runs"]["steps"]
        cls.script = next(s for s in steps if s.get("id") == "check")["run"]
        cls.bin = root / "bin"
        cls.bin.mkdir()
        (cls.bin / "gh").write_text(FAKE_GH)
        (cls.bin / "gh").chmod(0o755)

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def check(self, event, head_branch, head_sha, for_ref):
        work = Path(tempfile.mkdtemp(dir=self.tmp.name))
        run = work / "run"
        run.write_text("\n".join(["E2E Test CI", ".github/workflows/e2e_test_ci.yml", event, "owner/repo",
                                  head_branch, head_sha]) + "\n")
        output = work / "github_output"
        output.write_text("")
        (work / "step.sh").write_text(self.script)
        env = {
            "PATH": f"{self.bin}{os.pathsep}{os.environ['PATH']}",
            "HOME": str(work),
            "RUNNER_TEMP": str(work),
            "GITHUB_OUTPUT": str(output),
            "GITHUB_SERVER_URL": self.server,
            "GITHUB_REPOSITORY": "owner/repo",
            "GH_TOKEN": "test-token",
            "RUN_ID": "123",
            "FOR_REF": for_ref,
            "FAKE_RUN": str(run),
        }
        return subprocess.run(["bash", "-e", str(work / "step.sh")], cwd=work, env=env,
                              capture_output=True, text=True, timeout=60)

    def test_a_trunk_run_takes_only_a_build_of_a_trunk_commit(self):
        queue = f"gh-readonly-queue/trunk/pr-7-{self.trunk}"
        for event, branch, sha, accepted in (
            ("push", "trunk", self.trunk, True),
            ("merge_group", queue, self.trunk, True),
            ("push", "release/1.0", self.release, False),
            ("merge_group", queue, self.release, False),
            # Another ref's run of a trunk commit used that ref's caches and store keys.
            ("push", "release/1.0", self.trunk, False),
        ):
            with self.subTest(event=event, branch=branch, sha=sha):
                proc = self.check(event, branch, sha, "refs/heads/trunk")
                if accepted:
                    self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
                else:
                    self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
                    self.assertIn("A trunk run runs only trunk's builds", proc.stdout)

    def test_another_ref_keeps_the_trusted_builds_it_had(self):
        for for_ref in ("", "refs/heads/release/1.0", "refs/heads/feature"):
            with self.subTest(for_ref=for_ref):
                proc = self.check("push", "release/1.0", self.release, for_ref)
                self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)

    def test_a_build_it_refused_before_is_still_refused(self):
        proc = self.check("push", "feature", self.release, "refs/heads/feature")
        self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
        self.assertIn("not trunk, a release branch or a release tag", proc.stdout)


# trunk_tests's lookups: a branch's head, the E2E Test CI runs of a commit ($RUNS_JSON), and
# each run's artifacts: every run holds a build.
FAKE_TRUNK_TESTS_GH = """#!/usr/bin/env bash
case "$*" in
  *branches/* | *commits/trunk*) echo "$BRANCH_SHA" ;;
  *actions/workflows/*) cat "$RUNS_JSON" ;;
  *actions/runs/*/artifacts*) echo build_linux_x86_64 ;;
  *) echo "unexpected gh call: $*" >&2; exit 1 ;;
esac
"""


class TrunkTestsBranchTest(unittest.TestCase):
    """trunk_tests tests a release branch's head only in a run on that branch."""

    def find(self, github_ref, branch, runs=()):
        steps = yaml.safe_load(TRUNK_TESTS.read_text())["jobs"]["resolve"]["steps"]
        script = next(s for s in steps if s.get("id") == "build")["run"]
        with tempfile.TemporaryDirectory() as tmp:
            work = Path(tmp)
            (work / "bin").mkdir()
            (work / "bin" / "gh").write_text(FAKE_TRUNK_TESTS_GH)
            (work / "bin" / "gh").chmod(0o755)
            (work / "step.sh").write_text(script)
            output = work / "github_output"
            output.write_text("")
            (work / "runs.json").write_text(json.dumps({"workflow_runs": [
                {"id": run_id, "event": event, "head_branch": head_branch} for run_id, event, head_branch in runs]}))
            env = {
                "RUNS_JSON": str(work / "runs.json"),
                "PATH": f"{work / 'bin'}{os.pathsep}{os.environ['PATH']}",
                "HOME": str(work),
                "GITHUB_OUTPUT": str(output),
                "GITHUB_REPOSITORY": "owner/repo",
                "GITHUB_REF": github_ref,
                "GITHUB_EVENT_NAME": "workflow_dispatch",
                "BRANCH": branch,
                "RUN_ID": "",
                "BRANCH_SHA": "a" * 40,
            }
            proc = subprocess.run(["bash", "-e", str(work / "step.sh")], cwd=work, env=env,
                                  capture_output=True, text=True, timeout=60)
            return proc, output.read_text()

    def test_a_release_branch_is_tested_only_in_a_run_on_it(self):
        proc, output = self.find("refs/heads/trunk", "release/1.0")
        self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
        self.assertIn("tests only its own branch's builds. Run it on release/1.0", proc.stdout)
        self.assertEqual(output, "")
        proc, output = self.find("refs/heads/release/1.0", "release/1.0")
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertIn(f"sha={'a' * 40}", output)

    def test_the_build_it_finds_is_a_run_of_the_branch_under_test(self):
        # Runs of the same commit, newest first: another branch's push and merge queue, then this
        # branch's merge queue.
        runs = ((3, "push", "release/1.0"), (2, "merge_group", "gh-readonly-queue/release/1.0/pr-1-abc"),
                (1, "merge_group", "gh-readonly-queue/trunk/pr-9-abc"), (0, "push", "trunk"))
        proc, output = self.find("refs/heads/trunk", "", runs)
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertIn("run=1\n", output)
        proc, output = self.find("refs/heads/release/1.0", "release/1.0", runs)
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertIn("run=3\n", output)
        proc, output = self.find("refs/heads/trunk", "", ((3, "push", "release/1.0"), (4, "push", "feature")))
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertIn("run=\n", output)


if __name__ == "__main__":
    unittest.main()
