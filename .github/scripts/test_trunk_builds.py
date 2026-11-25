#!/usr/bin/env python3
"""Tests that a trunk run builds and runs only trunk's images.

- spiced_docker_dev builds only a commit on trunk in a run on trunk: its build jobs hold that
  run's OIDC token, trunk's store and registry identity. Its provenance step runs here, from the
  workflow file, in a clone of a local Git server holding trunk and a feature commit.
- The cluster dispatcher names a `trunk-` image only in a run on trunk.
- The spice-mssql-bench image that full-role benchmark jobs run as a service is published only
  from trunk and pinned by digest.
"""

import os
import re
import subprocess
import tempfile
import unittest
from pathlib import Path

import yaml

WORKFLOWS = Path(__file__).resolve().parents[1] / "workflows"


def git(cwd, *args):
    return subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True).stdout.strip()


def job_steps(workflow, job):
    return yaml.safe_load((WORKFLOWS / workflow).read_text())["jobs"][job]["steps"]


class SpicedDockerDevTest(unittest.TestCase):
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
        git(work, "checkout", "-qb", "feature")
        (work / "f").write_text("feature")
        git(work, "commit", "-qam", "feature")
        cls.feature = git(work, "rev-parse", "HEAD")
        cls.bare = root / "repo.git"
        git(root, "clone", "-q", "--bare", str(work), str(cls.bare))
        cls.root = root
        steps = job_steps("spiced_docker_dev.yml", "setup")
        cls.script = next(s for s in steps if s.get("id") == "provenance")["run"]

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def provenance(self, github_ref, commit):
        checkout = Path(tempfile.mkdtemp(dir=self.root))
        git(checkout, "clone", "-q", str(self.bare), ".")
        git(checkout, "checkout", "-q", commit)
        output = checkout / "github_output"
        output.write_text("")
        (checkout / "step.sh").write_text(self.script)
        env = {
            "PATH": os.environ["PATH"],
            "HOME": str(checkout),
            "GITHUB_OUTPUT": str(output),
            "GITHUB_REF": github_ref,
            "GITHUB_REF_TYPE": "branch",
            "GITHUB_REF_NAME": github_ref.removeprefix("refs/heads/"),
        }
        proc = subprocess.run(["bash", "-e", str(checkout / "step.sh")], cwd=checkout, env=env,
                              capture_output=True, text=True, timeout=60)
        return proc, output.read_text()

    def test_a_trunk_run_builds_only_a_trunk_commit(self):
        proc, output = self.provenance("refs/heads/trunk", self.feature)
        self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
        self.assertIn("builds only a commit on trunk", proc.stdout)
        proc, output = self.provenance("refs/heads/trunk", self.trunk)
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertIn("on_trunk=true", output)

    def test_another_refs_run_builds_its_commit(self):
        proc, output = self.provenance("refs/heads/feature", self.feature)
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertIn("on_trunk=false", output)

    def test_the_build_jobs_build_the_commit_setup_checked_and_keep_no_git_credentials(self):
        for job in ("build-arm64", "build-amd64"):
            with self.subTest(job=job):
                checkout = next(s for s in job_steps("spiced_docker_dev.yml", job)
                                if str(s.get("uses", "")).startswith("actions/checkout@"))
                self.assertEqual(checkout["with"]["ref"], "${{ needs.setup.outputs.revision }}")
                self.assertIs(checkout["with"]["persist-credentials"], False)

    def test_an_image_tag_is_validated(self):
        steps = job_steps("spiced_docker_dev.yml", "setup")
        script = next(s for s in steps if s.get("id") == "set_image_metadata")["run"]
        for tag, ok in (("my-feature", True), ("trunk-0123456789ab", True), ("a b", False),
                        ("x\nimage=evil", False), ("-x", False), ("a" * 129, False)):
            with self.subTest(tag=tag):
                with tempfile.TemporaryDirectory() as tmp:
                    output = Path(tmp) / "out"
                    output.write_text("")
                    proc = subprocess.run(["bash", "-e", "-c", script], capture_output=True, text=True,
                                          env={"PATH": os.environ["PATH"], "INPUT_TAG": tag, "GITHUB_OUTPUT": str(output)})
                    self.assertEqual(proc.returncode == 0, ok, proc.stdout + proc.stderr)
                    self.assertEqual(output.read_text() != "", ok)


class ClusterImageTest(unittest.TestCase):
    def test_only_a_trunk_run_names_a_trunk_image(self):
        steps = job_steps("testoperator_dispatch_cluster.yml", "resolve-image")
        run = next(s for s in steps if s.get("id") == "resolve")["run"]
        self.assertRegex(run, r'refs/heads/trunk\) image_tag="trunk-\$\{spiced_sha_short\}"')
        self.assertRegex(run, r'\*\) image_tag="dev-\$\{spiced_sha_short\}"')
        self.assertEqual(len(re.findall(r'image_tag="trunk-', run)), 1)


class MssqlBenchImageTest(unittest.TestCase):
    PINNED = re.compile(r"'ghcr\.io/spiceai/spice-mssql-bench@sha256:[0-9a-f]{64}'")

    def test_it_is_published_only_from_trunk(self):
        steps = job_steps("spice_mssql_tpch_bench_docker.yml", "build")
        self.assertEqual(steps[0]["name"], "Refuse a run off trunk")
        self.assertIn('[ "$GITHUB_REF" != refs/heads/trunk ]', steps[0]["run"])

    def test_every_job_that_runs_it_pins_its_digest(self):
        users = 0
        for workflow in sorted(WORKFLOWS.glob("*.yml")):
            text = workflow.read_text()
            if workflow.name == "spice_mssql_tpch_bench_docker.yml" or "spice-mssql-bench" not in text:
                continue
            users += 1
            with self.subTest(workflow=workflow.name):
                for line in text.splitlines():
                    if "spice-mssql-bench" in line and "image:" in line:
                        self.assertRegex(line, self.PINNED)
        self.assertGreaterEqual(users, 2)


if __name__ == "__main__":
    unittest.main()
