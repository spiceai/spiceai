#!/usr/bin/env python3
"""Tests for the external git checkouts in setup-bird-bench and setup-adbc-bigquery.

Each test runs the action's own checkout script, read from its action.yml, against a local
repository with two commits: the pinned one and a newer one on the default branch. git's
`url.<base>.insteadOf` (set through GIT_CONFIG_* in the environment) sends the action's
https://github.com URL to that repository, so nothing reaches the network. A checkout must
land on the pinned commit, never on the default branch's head, and must refuse anything but
a full commit SHA.
"""

import os
import subprocess
import tempfile
import unittest
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
BIRD_BENCH = ROOT / ".github/actions/setup-bird-bench/action.yml"
ADBC_BIGQUERY = ROOT / ".github/actions/setup-adbc-bigquery/action.yml"


def step_script(action: Path, name: str) -> str:
    for step in yaml.safe_load(action.read_text())["runs"]["steps"]:
        if step.get("name") == name:
            return step["run"]
    raise AssertionError(f"{action} has no step named {name!r}")


def git(*args, cwd):
    return subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True).stdout.strip()


class PinnedCheckoutTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.dir = Path(self.tmp.name)
        work = self.dir / "work"
        work.mkdir()
        git("init", "-q", "-b", "trunk", cwd=work)
        git("config", "user.email", "test@example.com", cwd=work)
        git("config", "user.name", "test", cwd=work)
        for content in ("pinned", "newer"):
            for path in ("dev_20240627/dev.json", "go/driver.go"):
                (work / path).parent.mkdir(parents=True, exist_ok=True)
                (work / path).write_text(content + "\n")
            git("add", "-A", cwd=work)
            git("commit", "-q", "-m", content, cwd=work)
        self.newer = git("rev-parse", "HEAD", cwd=work)
        self.pinned = git("rev-parse", "HEAD~1", cwd=work)
        self.remote = self.dir / "remote.git"
        git("clone", "-q", "--bare", str(work), str(self.remote), cwd=self.dir)
        git("config", "uploadpack.allowFilter", "true", cwd=self.remote)
        git("config", "uploadpack.allowAnySHA1InWant", "true", cwd=self.remote)

    def run_script(self, script: str, url: str, cwd: Path, **env):
        environment = {
            **os.environ,
            "GIT_CONFIG_COUNT": "1",
            "GIT_CONFIG_KEY_0": f"url.file://{self.remote}.insteadOf",
            "GIT_CONFIG_VALUE_0": url,
            "GIT_TERMINAL_PROMPT": "0",
            **env,
        }
        return subprocess.run(["bash", "-e", "-c", script], cwd=cwd, env=environment, capture_output=True, text=True, timeout=120)

    def bird_bench(self, commit):
        workspace = self.dir / "workspace"
        workspace.mkdir(exist_ok=True)
        script = step_script(BIRD_BENCH, "Clone bird-bench repo (no checkout action) (Linux/macOS)")
        proc = self.run_script(
            script, "https://github.com/spiceai/spice-bird-bench.git", workspace, BIRD_BENCH_COMMIT=commit
        )
        return proc, workspace / "bird-bench"

    def adbc_bigquery(self, commit):
        script = step_script(ADBC_BIGQUERY, "Build ADBC BigQuery driver")
        # Only the checkout, into this test's directory rather than the action's /tmp path: the
        # rest checks the driver's patches and builds it with Go.
        self.assertIn("/tmp/adbc-bigquery", script)
        script = script.split("\ncd go\n")[0].replace("/tmp/adbc-bigquery", str(self.dir / "adbc-bigquery"))
        script += "\ngit rev-parse HEAD > \"$OUT\"\n"
        out = self.dir / "adbc-head"
        proc = self.run_script(
            script, "https://github.com/spiceai/bigquery.git", self.dir, ADBC_BIGQUERY_COMMIT=commit, OUT=str(out)
        )
        return proc, out

    def test_bird_bench_checks_out_the_pinned_commit_not_the_default_branch(self):
        proc, checkout = self.bird_bench(self.pinned)
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertEqual(git("rev-parse", "HEAD", cwd=checkout), self.pinned)
        self.assertEqual((checkout / "dev_20240627/dev.json").read_text(), "pinned\n")
        self.assertFalse((checkout / "go").exists(), "the sparse checkout should hold dev_20240627 only")

    def test_bird_bench_refuses_anything_but_a_full_commit_sha(self):
        for commit in ("trunk", self.pinned[:12], self.pinned.upper()):
            with self.subTest(commit=commit):
                proc, checkout = self.bird_bench(commit)
                self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
                self.assertIn("bird_bench_commit must be a full commit SHA", proc.stdout)
                self.assertFalse(checkout.exists())

    def test_the_bird_bench_default_is_a_full_commit_sha(self):
        default = yaml.safe_load(BIRD_BENCH.read_text())["inputs"]["bird_bench_commit"]["default"]
        self.assertRegex(default, r"^[0-9a-f]{40}$")

    def test_adbc_bigquery_checks_out_the_pinned_commit(self):
        proc, out = self.adbc_bigquery(self.pinned)
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertEqual(out.read_text().strip(), self.pinned)

    def test_adbc_bigquery_refuses_anything_but_a_full_commit_sha(self):
        for commit in ("trunk", self.pinned[:12]):
            with self.subTest(commit=commit):
                proc, out = self.adbc_bigquery(commit)
                self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
                self.assertIn("adbc_bigquery_commit must be a full", proc.stdout)
                self.assertFalse(out.exists())


if __name__ == "__main__":
    unittest.main()
