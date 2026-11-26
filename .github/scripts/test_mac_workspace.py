#!/usr/bin/env python3
"""Tests that a job on a self-hosted Mac starts from a workspace and git config of its own.

Each runner instance on those Macs reuses its workspace across jobs of every ref, and
actions/checkout keeps an existing repository's local config and hooks. So:
- `clean-mac-workspace`, run before the checkout, empties GITHUB_WORKSPACE there, and does
  nothing on any other runner;
- `push-snap-changes` refuses a checkout whose local git config could redirect its push or pull
  in other config, and runs no hook.
Each test runs the action's own `run:` step under bash, in a temporary directory.
"""

import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]


def step(action, name):
    steps = yaml.safe_load((ROOT / ".github" / "actions" / action / "action.yml").read_text())["runs"]["steps"]
    (found,) = [s for s in steps if s.get("name") == name]
    return found["run"]


def git(cwd, *args):
    return subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True).stdout


class CleanMacWorkspaceTest(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.addCleanup(self._cleanup)
        self.workspace = self.tmp / "work" / "spiceai" / "spiceai"
        self.workspace.mkdir(parents=True)
        git(self.workspace, "init", "-q")
        git(self.workspace, "config", "url.https://evil.invalid/.insteadOf", "https://github.com/")
        hook = self.workspace / ".git" / "hooks" / "post-checkout"
        hook.write_text("#!/bin/sh\necho planted\n")
        hook.chmod(0o755)
        (self.workspace / ".cargo").mkdir()
        (self.workspace / ".cargo" / "config.toml").write_text("[build]\nrustc-wrapper = '/tmp/x'\n")
        self.outside = self.tmp / "outside"
        self.outside.write_text("keep\n")
        (self.workspace / "link").symlink_to(self.outside)
        (self.workspace / "locked").mkdir()
        (self.workspace / "locked" / "file").write_text("x")
        (self.workspace / "locked").chmod(0o500)

    def _cleanup(self):
        if shutil.which("chflags"):
            subprocess.run(["chflags", "-R", "nouchg,nouappnd", str(self.tmp)], capture_output=True)
        subprocess.run(["chmod", "-R", "u+rwx", str(self.tmp)], capture_output=True)
        shutil.rmtree(self.tmp, ignore_errors=True)

    def run_step(self, **env):
        base = {k: v for k, v in os.environ.items() if not k.startswith(("RUNNER_", "GITHUB_"))}
        base.update({"GITHUB_WORKSPACE": str(self.workspace), "RUNNER_OS": "macOS", "RUNNER_ENVIRONMENT": "self-hosted",
                     "RUNNER_TEMP": str(self.tmp)})
        base.update(env)
        base = {k: v for k, v in base.items() if v is not None}
        return subprocess.run(["bash", "-e", "-c", step("clean-mac-workspace", "Empty the workspace")],
                              env=base, capture_output=True, text=True, timeout=60)

    def test_a_self_hosted_mac_starts_from_an_empty_workspace(self):
        if sys.platform == "darwin":
            subprocess.run(["chflags", "uchg", str(self.workspace / ".cargo" / "config.toml")], check=True)
        proc = self.run_step()
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertEqual(list(self.workspace.iterdir()), [])
        self.assertEqual(self.outside.read_text(), "keep\n")

    def test_what_an_earlier_job_locked_inside_cannot_block_the_next_job(self):
        if sys.platform != "darwin":
            self.skipTest("chflags is macOS's")
        deep = self.workspace / "deep"
        deep.mkdir()
        (deep / "appendonly").write_text("x")
        subprocess.run(["chflags", "uappnd", str(deep / "appendonly")], check=True)
        (deep / "locked").write_text("x")
        subprocess.run(["chflags", "uchg", str(deep / "locked")], check=True)
        deep.chmod(0o000)
        proc = self.run_step(RUNNER_TEMP=str(self.tmp))
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertTrue(self.workspace.is_dir())
        self.assertEqual(list(self.workspace.iterdir()), [])

    def test_a_link_left_as_the_workspace_is_moved_aside_not_followed(self):
        target = self.tmp / "target"
        target.mkdir()
        (target / "keep").write_text("keep\n")
        shutil.rmtree(self.workspace, ignore_errors=True)
        subprocess.run(["chmod", "-R", "u+rwx", str(self.tmp / "work")], capture_output=True)
        shutil.rmtree(self.workspace, ignore_errors=True)
        self.workspace.symlink_to(target)
        proc = self.run_step(RUNNER_TEMP=str(self.tmp))
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertTrue(self.workspace.is_dir() and not self.workspace.is_symlink())
        self.assertEqual((target / "keep").read_text(), "keep\n")

    def test_a_mac_that_does_not_say_where_it_runs_is_emptied_too(self):
        proc = self.run_step(RUNNER_ENVIRONMENT=None)
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertEqual(list(self.workspace.iterdir()), [])

    def test_other_runners_keep_their_workspace(self):
        for env in ({"RUNNER_OS": "Linux"}, {"RUNNER_ENVIRONMENT": "github-hosted"}):
            with self.subTest(env=env):
                proc = self.run_step(**env)
                self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
                self.assertTrue((self.workspace / ".git" / "hooks" / "post-checkout").exists())


class PushSnapChangesConfigTest(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.repo = self.tmp / "repo"
        self.repo.mkdir()
        git(self.repo, "init", "-q")
        (self.repo / "a.snap").write_text("snapshot\n")
        git(self.repo, "add", "a.snap")
        git(self.repo, "-c", "user.name=t", "-c", "user.email=t@example.invalid", "commit", "-qm", "base")
        (self.tmp / "temp").mkdir()
        self.bin = self.tmp / "bin"
        self.bin.mkdir()
        self.calls = self.tmp / "calls"
        (self.bin / "gh").write_text(f'#!/bin/sh\necho "gh $*" >> "{self.calls}"\n')
        (self.bin / "gh").chmod(0o755)

    def run_step(self):
        env = {k: v for k, v in os.environ.items() if not k.startswith(("GIT_", "RUNNER_"))}
        env.update({"PATH": f"{self.bin}{os.pathsep}{os.environ['PATH']}", "RUNNER_TEMP": str(self.tmp / "temp"),
                    "HOME": str(self.tmp), "GH_TOKEN": "t", "GITHUB_REPOSITORY": "spiceai/spiceai", "PR_TITLE": "x"})
        return subprocess.run(["bash", "-e", "-c", step("push-snap-changes", "Upload snapshots to branch")],
                              cwd=self.repo, env=env, capture_output=True, text=True, timeout=60)

    def test_config_that_redirects_or_includes_is_refused_before_anything_runs(self):
        for key, value in (("url.https://evil.invalid/.insteadOf", "https://github.com/"),
                           ("url.https://evil.invalid/.pushInsteadOf", "https://github.com/"),
                           ("core.hooksPath", str(self.tmp)),
                           ("include.path", str(self.tmp / "other")),
                           ("includeIf.gitdir:/.path", str(self.tmp / "other")),
                           ("core.fsmonitor", "/tmp/x"),
                           ("filter.x.clean", "/tmp/x"),
                           ("gpg.program", "/tmp/x"),
                           ("http.proxy", "http://127.0.0.1:9"),
                           ("remote.origin.pushurl", "https://evil.invalid/x.git"),
                           ("core.askPass", "/tmp/x")):
            with self.subTest(key=key):
                git(self.repo, "config", key, value)
                proc = self.run_step()
                git(self.repo, "config", "--unset", key)
                self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
                self.assertIn("not pushing", proc.stdout)
                self.assertFalse(self.calls.exists())

    def test_no_hook_runs_and_a_clean_checkout_proceeds(self):
        marker = self.tmp / "hook-ran"
        hook = self.repo / ".git" / "hooks" / "pre-commit"
        hook.write_text(f"#!/bin/sh\ntouch '{marker}'\n")
        hook.chmod(0o755)
        script = step("push-snap-changes", "Upload snapshots to branch")
        self.assertIn("export GIT_CONFIG_COUNT=1 GIT_CONFIG_KEY_0=core.hooksPath GIT_CONFIG_VALUE_0=/dev/null", script)
        self.assertLess(script.index("GIT_CONFIG_KEY_0=core.hooksPath"), script.index("git add"))
        proc = self.run_step()
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertIn("No changes to commit", proc.stdout)
        # The hooks setting the step exports wins over the repository's own.
        env = {**os.environ, "GIT_CONFIG_COUNT": "1", "GIT_CONFIG_KEY_0": "core.hooksPath", "GIT_CONFIG_VALUE_0": "/dev/null"}
        (self.repo / "f").write_text("x")
        subprocess.run(["git", "add", "f"], cwd=self.repo, env=env, check=True)
        subprocess.run(["git", "-c", "user.name=t", "-c", "user.email=t@example.invalid", "commit", "-qm", "x"],
                       cwd=self.repo, env=env, check=True)
        self.assertFalse(marker.exists())


if __name__ == "__main__":
    unittest.main()
