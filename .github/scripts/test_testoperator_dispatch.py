#!/usr/bin/env python3
"""Tests that this release branch's leg of the scheduled dispatch runs in a run of the branch itself.

A run's store credentials write its own ref's keys, so this branch's code and builds run only
in a run on this branch. trunk's daily dispatch starts this workflow here with
`schedule: release`, and the leg plans only this branch. The planning step runs here, from the
workflow file.
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


class TestoperatorDispatchTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
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

    def test_a_release_run_plans_only_its_own_leg(self):
        proc, planned = self.plan("refs/heads/release/2.4", schedule="release")
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertEqual(planned["matrix"], [{"branch": "release/2.4", "schedule": ""}])

    def test_a_leg_on_another_branchs_run_is_refused(self):
        for github_ref, cron, schedule in (
            ("refs/heads/trunk", "", "release"),
            ("refs/heads/release/2.4", "", "daily"),
            ("refs/heads/release/2.4", "", "weekly"),
            ("refs/heads/release/2.4", DAILY, ""),
            ("refs/heads/release/2.4-rc.1", "", "release"),
            ("refs/heads/feature", "", "release"),
        ):
            with self.subTest(github_ref=github_ref, schedule=schedule):
                proc, planned = self.plan(github_ref, cron=cron, schedule=schedule)
                self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
                self.assertNotIn("matrix", planned)

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
