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

    def test_a_locked_parent_directory_cannot_block_the_next_job(self):
        parent = self.workspace.parent
        parent.chmod(0o555)
        if sys.platform == "darwin":
            subprocess.run(["chflags", "uchg", str(parent)], check=True)
        proc = self.run_step()
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
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
    """The snapshot push builds and pushes its commit in a repository of its own, so nothing an
    earlier job or a co-tenant put in the checkout's .git runs or redirects it."""

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.repo = self.tmp / "repo"
        self.repo.mkdir()
        git(self.repo, "init", "-q", "-b", "trunk")
        (self.repo / "a.snap").write_text("snapshot\n")
        git(self.repo, "add", "a.snap")
        git(self.repo, "-c", "user.name=t", "-c", "user.email=t@example.invalid", "commit", "-qm", "base")
        self.base = git(self.repo, "rev-parse", "HEAD").strip()
        # The repository the push goes to, and one a planted insteadOf would send it to.
        self.server = self.tmp / "server"
        (self.server / "owner").mkdir(parents=True)
        git(self.tmp, "clone", "-q", "--bare", str(self.repo), str(self.server / "owner" / "repo.git"))
        self.evil = self.tmp / "evil.git"
        git(self.tmp, "init", "-q", "--bare", str(self.evil))
        # As a checkout has it; the step does not read it.
        git(self.repo, "remote", "add", "origin", f"file://{self.server}/owner/repo.git")
        (self.repo / ".github" / "scripts").mkdir(parents=True)
        (self.repo / ".github" / "scripts" / "approve_bot_pr_runs.sh").write_text("exit 0\n")
        (self.tmp / "temp").mkdir()
        self.bin = self.tmp / "bin"
        self.bin.mkdir()
        self.calls = self.tmp / "calls"
        (self.bin / "gh").write_text(f'#!/bin/sh\necho "gh $*" >> "{self.calls}"\n')
        (self.bin / "gh").chmod(0o755)
        self.marker = self.tmp / "ran"

    def run_step(self, workspace=None):
        env = {k: v for k, v in os.environ.items() if not k.startswith(("GIT_", "RUNNER_"))}
        env.update({"PATH": f"{self.bin}{os.pathsep}{os.environ['PATH']}", "RUNNER_TEMP": str(self.tmp / "temp"),
                    "HOME": str(self.tmp), "GH_TOKEN": "the-token", "GITHUB_REPOSITORY": "owner/repo",
                    "GITHUB_SERVER_URL": f"file://{self.server}", "GITHUB_RUN_ID": "7", "GITHUB_JOB": "job",
                    "GITHUB_REF_NAME": "trunk", "PR_TITLE": "x", "PR_DESCRIPTION": ""})
        return subprocess.run(["bash", "-e", "-c", step("push-snap-changes", "Upload snapshots to branch")],
                              cwd=workspace or self.repo, env=env, capture_output=True, text=True, timeout=60)

    def pushed(self, repository=None):
        return git(repository or self.server / "owner" / "repo.git", "for-each-ref", "--format=%(refname)").split()

    def plant(self, repo):
        """Everything in a checkout's .git that could run a program or send the push elsewhere."""
        program = self.tmp / "program"
        program.write_text(f"#!/bin/sh\ntouch '{self.marker}'\ncat\n")
        program.chmod(0o755)
        hooks = self.tmp / "hooks"
        hooks.mkdir(exist_ok=True)
        for name in ("pre-commit", "post-commit", "pre-push", "reference-transaction"):
            for directory in (hooks, repo / ".git" / "hooks"):
                (directory / name).write_text(f"#!/bin/sh\ntouch '{self.marker}'\n")
                (directory / name).chmod(0o755)
        included = self.tmp / "included"
        included.write_text(f"[hook \"inc\"]\n\tevent = pre-push\n\tcommand = {program}\n")
        for key, value in (("core.hooksPath", str(hooks)), ("core.fsmonitor", str(program)),
                           ("hook.planted.event", "pre-commit"), ("hook.planted.command", str(program)),
                           ("filter.evil.clean", str(program)), ("filter.evil.required", "true"),
                           ("diff.evil.command", str(program)), ("include.path", str(included)),
                           ("core.repositoryformatversion", "1"), ("extensions.worktreeConfig", "true")):
            git(repo, "config", key, value)
        git(repo, "config", "--worktree", f"url.file://{self.evil}.insteadOf", f"file://{self.server}/owner/repo.git")
        git(repo, "config", "--worktree", f"url.file://{self.evil}.pushInsteadOf", f"file://{self.server}/owner/repo.git")
        (repo / ".gitattributes").write_text("*.snap filter=evil diff=evil\n")

    def test_nothing_in_the_checkouts_git_runs_or_redirects_the_push(self):
        self.plant(self.repo)
        (self.repo / "a.snap").write_text("updated\n")
        proc = self.run_step()
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertFalse(self.marker.exists(), "a program from the checkout's git config or hooks ran")
        self.assertEqual(self.pushed(), ["refs/heads/snapshot-update/job-7", "refs/heads/trunk"])
        self.assertEqual(self.pushed(self.evil), [])
        server = self.server / "owner" / "repo.git"
        self.assertEqual(git(server, "show", "snapshot-update/job-7:a.snap"), "updated\n")
        self.assertEqual(git(server, "rev-parse", "snapshot-update/job-7^").strip(), self.base)
        self.assertEqual(git(server, "log", "-1", "--format=%an <%ae>", "snapshot-update/job-7").strip(),
                         "Spice Snapshot Update Bot <spiceaibot@spice.ai>")
        # The token is in no file of the checkout's, and the checkout is left as it was.
        for path in (self.repo / ".git").rglob("*"):
            if path.is_file():
                self.assertNotIn(b"the-token", path.read_bytes(), path)
        self.assertEqual(git(self.repo, "rev-parse", "HEAD").strip(), self.base)
        self.assertIn("gh pr create", self.calls.read_text())

    def test_hooks_defined_in_config_do_not_run(self):
        # git 2.54's `hook.<name>.command` runs whatever core.hooksPath says.
        program = self.tmp / "program"
        program.write_text(f"#!/bin/sh\ntouch '{self.marker}'\n")
        program.chmod(0o755)
        for event in ("pre-commit", "commit-msg", "post-commit", "pre-push"):
            git(self.repo, "config", "--add", f"hook.planted-{event}.event", event)
            git(self.repo, "config", f"hook.planted-{event}.command", str(program))
        (self.repo / "a.snap").write_text("updated\n")
        proc = self.run_step()
        self.assertFalse(self.marker.exists(), "a hook defined in the checkout's config ran")
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertIn("refs/heads/snapshot-update/job-7", self.pushed())

    def test_a_shallow_checkout_pushes_its_snapshots(self):
        shallow = self.tmp / "shallow"
        git(self.tmp, "clone", "-q", "--depth", "1", f"file://{self.server}/owner/repo.git", str(shallow))
        (shallow / ".github" / "scripts").mkdir(parents=True)
        (shallow / ".github" / "scripts" / "approve_bot_pr_runs.sh").write_text("exit 0\n")
        (shallow / "b.snap").write_text("new\n")
        proc = self.run_step(shallow)
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertEqual(git(self.server / "owner" / "repo.git", "show", "snapshot-update/job-7:b.snap"), "new\n")

    def test_unchanged_snapshots_push_nothing(self):
        (self.repo / "other.txt").write_text("not a snapshot\n")
        proc = self.run_step()
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertIn("No changes to commit", proc.stdout)
        self.assertEqual(self.pushed(), ["refs/heads/trunk"])
        self.assertFalse(self.calls.exists())


if __name__ == "__main__":
    unittest.main()
