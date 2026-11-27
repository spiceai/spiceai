#!/usr/bin/env python3
"""Tests that each leg of testoperator_dispatch.yml's scheduled dispatch runs in a run of its own branch.

A run's store credentials write its own ref's keys, so a trunk run builds and runs only trunk's
code and builds. The daily schedule's release leg is therefore a run on the release branch
(`schedule: release`), which the trunk run's `dispatch-release` job starts. The planning step runs
here, from the workflow file, against a local Git server holding trunk and release branches.
"""

import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

import yaml

WORKFLOW = Path(__file__).resolve().parent.parent / "workflows" / "testoperator_dispatch.yml"
DAILY = "0 9 * * *"
WEEKLY = "0 0 * * 0"


def git(cwd, *args):
    return subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True).stdout.strip()


class TestoperatorDispatchTest(unittest.TestCase):
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
        for branch in ("release/1.9", "release/1.10", "release/1.10-rc.1", "release/2.0-rc.1"):
            git(work, "branch", branch)
        server = root / "server"
        (server / "owner").mkdir(parents=True)
        git(root, "clone", "-q", "--bare", str(work), str(server / "owner" / "repo.git"))
        cls.server = f"file://{server}"
        cls.jobs = yaml.safe_load(WORKFLOW.read_text())["jobs"]

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def plan(self, github_ref, cron="", schedule=""):
        step = next(s for s in self.jobs["plan-scheduled"]["steps"] if s.get("id") == "plan")
        work = Path(tempfile.mkdtemp(dir=self.tmp.name))
        (work / "step.sh").write_text(step["run"])
        output = work / "github_output"
        output.write_text("")
        env = {
            "PATH": os.environ["PATH"],
            "HOME": str(work),
            "GITHUB_OUTPUT": str(output),
            "GITHUB_SERVER_URL": self.server,
            "GITHUB_REPOSITORY": "owner/repo.git",
            "GITHUB_REF": github_ref,
            "CRON": cron,
            "INPUT_SCHEDULE": schedule,
        }
        proc = subprocess.run(["bash", "-e", str(work / "step.sh")], cwd=work, env=env,
                              capture_output=True, text=True, timeout=60)
        values = dict(line.split("=", 1) for line in output.read_text().splitlines() if "=" in line)
        if "matrix" in values:
            values["matrix"] = json.loads(values["matrix"])
        return proc, values

    def test_a_trunk_run_plans_only_trunks_leg_and_names_the_release_branch(self):
        for cron, schedule in ((DAILY, ""), ("", "daily")):
            with self.subTest(cron=cron, schedule=schedule):
                proc, planned = self.plan("refs/heads/trunk", cron=cron, schedule=schedule)
                self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
                self.assertEqual(planned["matrix"], [{"branch": "trunk", "schedule": "daily"}])
                self.assertEqual(planned["release_branch"], "release/1.10")
        proc, planned = self.plan("refs/heads/trunk", cron=WEEKLY)
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertEqual(planned["matrix"], [{"branch": "trunk", "schedule": "weekly"}])
        self.assertEqual(planned["release_branch"], "")

    def test_a_release_run_plans_only_its_own_leg(self):
        proc, planned = self.plan("refs/heads/release/1.10", schedule="release")
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertEqual(planned["matrix"], [{"branch": "release/1.10", "schedule": ""}])
        self.assertEqual(planned["release_branch"], "")

    def test_a_leg_on_another_branchs_run_is_refused(self):
        for github_ref, cron, schedule in (
            ("refs/heads/trunk", "", "release"),
            ("refs/heads/release/1.10", "", "daily"),
            ("refs/heads/release/1.10", "", "weekly"),
            ("refs/heads/release/1.10-rc.1", "", "release"),
            ("refs/heads/feature", "", "release"),
        ):
            with self.subTest(github_ref=github_ref, schedule=schedule):
                proc, planned = self.plan(github_ref, cron=cron, schedule=schedule)
                self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
                self.assertNotIn("matrix", planned)

    def test_the_release_leg_is_started_on_the_release_branch_by_a_job_without_credentials(self):
        job = self.jobs["dispatch-release"]
        self.assertEqual(job["environment"], "secret-free")
        self.assertEqual(job["permissions"], {"actions": "write"})
        self.assertFalse(any("uses" in step for step in job["steps"]))
        run = job["steps"][0]["run"]
        self.assertIn('--ref "$RELEASE_BRANCH" -f schedule=release', run)
        self.assertIn("^release/[0-9]+\\.[0-9]+$", run)

    def start_leg(self, definition):
        """Run the leg's step with a stub gh that serves `definition` as the branch's copy."""
        run = self.jobs["dispatch-release"]["steps"][0]["run"]
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            (tmp / "definition.yml").write_text(definition)
            calls = tmp / "calls"
            (tmp / "gh").write_text(
                "#!/usr/bin/env bash\n"
                f'echo "$*" >> "{calls}"\n'
                f'if [ "$1" = api ]; then cat "{tmp / "definition.yml"}"; fi\n'
            )
            (tmp / "gh").chmod(0o755)
            env = {
                **os.environ,
                "PATH": f"{tmp}:{os.environ['PATH']}",
                "RELEASE_BRANCH": "release/2.4",
                "GITHUB_REPOSITORY": "spiceai/spiceai",
                "GH_TOKEN": "x",
            }
            proc = subprocess.run(["bash", "-e", "-c", run], env=env, capture_output=True, text=True)
            return proc, calls.read_text().splitlines() if calls.exists() else []

    def test_the_leg_starts_only_on_a_branch_whose_copy_has_it(self):
        own = (Path(__file__).resolve().parents[1] / "workflows" / "testoperator_dispatch.yml").read_text()
        proc, calls = self.start_leg(own)
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertIn("workflow run testoperator_dispatch.yml --repo spiceai/spiceai --ref release/2.4 -f schedule=release", calls)
        without = own.replace("          - 'release'\n", "")
        self.assertNotEqual(without, own)
        proc, calls = self.start_leg(without)
        self.assertEqual(proc.returncode, 1)
        self.assertIn("has no release leg", proc.stdout)
        self.assertFalse(any(c.startswith("workflow run") for c in calls), calls)

    def test_a_scheduled_run_dispatches_no_manual_suite(self):
        condition = self.jobs["dispatch-input"]["if"]
        for schedule in ("daily", "weekly", "release"):
            self.assertIn(f"github.event.inputs.schedule != '{schedule}'", condition)

    def test_the_dispatch_job_checks_out_and_reads_only_the_runs_own_branch(self):
        steps = self.jobs["dispatch-scheduled"]["steps"]
        checkout = next(s for s in steps if str(s.get("uses", "")).startswith("actions/checkout@"))
        self.assertNotIn("ref", checkout.get("with") or {})
        setup = next(s for s in steps if s.get("id") == "setup-spiced")
        self.assertEqual(setup["with"]["ref"], "${{ github.ref }}")


if __name__ == "__main__":
    unittest.main()
