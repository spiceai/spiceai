#!/usr/bin/env python3
"""Tests for .github/actions/ci-role, which chooses the CI AWS role a job assumes.

Runs the action's own `run:` step under bash, as GitHub would, against a local Git
repository that stands in for this one: its trunk, a feature branch, and a commit that
no branch of it holds. Each case sets the event and ref GitHub would, and reads back the
step's `full` output, what it adds to GITHUB_ENV, and how it ends.
"""

import os
import subprocess
import tempfile
import unittest
from pathlib import Path

import yaml

ACTION = Path(__file__).resolve().parents[1] / "actions" / "ci-role" / "action.yml"
QUEUE_REF = "refs/heads/gh-readonly-queue/trunk/pr-16720-0123456789abcdef0123456789abcdef01234567"


def git(cwd, *args):
    return subprocess.run(
        ["git", *args], cwd=cwd, check=True, capture_output=True, text=True,
        env={**os.environ, "GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@example.com",
             "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@example.com"},
    ).stdout.strip()


class CiRoleTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        root = Path(cls.tmp.name)
        step = yaml.safe_load(ACTION.read_text())["runs"]["steps"][0]
        cls.script = root / "choose.sh"
        cls.script.write_text(step["run"])
        work = root / "work"
        work.mkdir()
        git(work, "init", "-q", "-b", "trunk")
        git(work, "commit", "-q", "--allow-empty", "-m", "one")
        cls.old_trunk = git(work, "rev-parse", "HEAD")
        git(work, "commit", "-q", "--allow-empty", "-m", "two")
        cls.trunk = git(work, "rev-parse", "HEAD")
        git(work, "checkout", "-q", "-b", "feature")
        git(work, "commit", "-q", "--allow-empty", "-m", "feature")
        cls.feature = git(work, "rev-parse", "HEAD")
        git(work, "checkout", "-q", "--detach", "trunk")
        git(work, "commit", "-q", "--allow-empty", "-m", "nowhere")
        cls.nowhere = git(work, "rev-parse", "HEAD")
        server = root / "server"
        (server / "owner").mkdir(parents=True)
        bare = server / "owner" / "repo.git"
        git(root, "clone", "-q", "--bare", str(work), str(bare))
        git(bare, "config", "uploadpack.allowFilter", "true")
        git(bare, "update-ref", "-d", "refs/heads/feature")
        git(bare, "update-ref", "refs/heads/feature", cls.feature)
        cls.server = f"file://{server}"
        # A history in which the feature commit is trunk's, as a planted insteadOf could serve.
        fake = root / "fake"
        (fake / "owner").mkdir(parents=True)
        fake_bare = fake / "owner" / "repo.git"
        git(root, "clone", "-q", "--bare", str(work), str(fake_bare))
        git(fake_bare, "config", "uploadpack.allowFilter", "true")
        git(fake_bare, "update-ref", "refs/heads/trunk", cls.feature)
        cls.fake_server = f"file://{fake}"

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def choose(self, event, ref, builds="", pull_request_role=True, server=None, images="", gitconfig=""):
        with tempfile.TemporaryDirectory() as run:
            if gitconfig:
                (Path(run) / ".gitconfig").write_text(gitconfig)
            out, env_file = Path(run) / "output", Path(run) / "env"
            out.touch()
            env_file.touch()
            env = {
                "PATH": os.environ["PATH"],
                "HOME": run,
                "GITHUB_EVENT_NAME": event,
                "GITHUB_REF": ref,
                "GITHUB_SERVER_URL": server or self.server,
                "GITHUB_REPOSITORY": "owner/repo",
                "GITHUB_OUTPUT": str(out),
                "GITHUB_ENV": str(env_file),
                "RUNNER_TEMP": run,
                "PULL_REQUEST_ROLE_SET": "true" if pull_request_role else "false",
                "BUILDS": builds,
                "IMAGES": images,
                "GH_TOKEN": "a-token",
            }
            proc = subprocess.run(["bash", "-e", str(self.script)], env=env, capture_output=True, text=True, timeout=60)
            return proc, out.read_text(), env_file.read_text()

    def assert_role(self, proc, output, env, full):
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertEqual(output, f"full={'true' if full else 'false'}\n")
        self.assertEqual(env, "CI_ROLE_FULL=true\n" if full else "CI_CACHE_READ_ONLY=true\n")
        self.assertNotIn("a-token", proc.stdout + proc.stderr)

    def test_a_planted_global_git_config_cannot_serve_another_trunk(self):
        planted = f'[url "{self.fake_server}/"]\n\tinsteadOf = {self.server}/\n'
        proc, output, _ = self.choose("push", "refs/heads/trunk", f"spiced_commit={self.feature}", gitconfig=planted)
        self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertNotIn("full=true", output)

    # --- the run's own ref --------------------------------------------------------------

    def test_trunk_gets_the_full_roles_on_every_event(self):
        for event in ("push", "schedule", "workflow_dispatch"):
            with self.subTest(event=event):
                self.assert_role(*self.choose(event, "refs/heads/trunk"), full=True)

    def test_the_merge_queue_gets_the_read_only_role(self):
        # A queue run tests a pull request that is not on trunk yet.
        for event in ("merge_group", "push", "workflow_dispatch"):
            with self.subTest(event=event):
                self.assert_role(*self.choose(event, QUEUE_REF), full=False)

    def test_any_other_ref_gets_the_read_only_role_and_skips_writes(self):
        for event, ref in (
            ("workflow_dispatch", "refs/heads/feature"),
            ("workflow_dispatch", "refs/heads/release/2.4"),
            ("push", "refs/heads/release/2.4"),
            ("pull_request", "refs/pull/16720/merge"),
            ("merge_group", "refs/heads/gh-readonly-queue/release/2.4/pr-1-abc"),
            ("workflow_dispatch", "refs/tags/v2.4.0"),
        ):
            with self.subTest(ref=ref):
                proc, output, env = self.choose(event, ref)
                self.assert_role(proc, output, env, full=False)
                self.assertIn(f"::notice::{ref} is not trunk", proc.stdout)

    def test_the_read_only_role_unset_fails_clearly(self):
        proc, output, env = self.choose("workflow_dispatch", "refs/heads/feature", pull_request_role=False)
        self.assertEqual(proc.returncode, 1)
        self.assertEqual(output, "")
        self.assertIn("::error::refs/heads/feature is not trunk,", proc.stdout)
        self.assertIn("CI_AWS_PULL_REQUEST_ROLE_ARN secret is not set", proc.stdout)

    def test_trunk_needs_no_read_only_role(self):
        self.assert_role(*self.choose("push", "refs/heads/trunk", pull_request_role=False), full=True)

    # --- the other builds a job runs ----------------------------------------------------

    def test_a_trunk_build_keeps_the_full_roles(self):
        for builds in (
            f"spiced_commit={self.trunk}",
            f"spiced_commit={self.old_trunk}\nspiced_ref=trunk",
            "spiced_commit=\nspiced_ref=",
            "spiced_ref=refs/heads/trunk",
            "\n  \n",
        ):
            with self.subTest(builds=builds):
                self.assert_role(*self.choose("workflow_dispatch", "refs/heads/trunk", builds), full=True)

    def test_a_build_off_trunk_fails_a_trunk_run(self):
        # The job's token still names trunk, so it must not run a build from elsewhere at all.
        for builds, named in (
            (f"spiced_commit={self.feature}", f"spiced_commit is {self.feature}"),
            (f"spiced_commit={self.nowhere}", f"spiced_commit is {self.nowhere}"),
            (f"spiced_commit={self.trunk[:12]}", f"spiced_commit is {self.trunk[:12]}"),
            ("spiced_ref=release/2.4", "spiced_ref is release/2.4"),
            ("spiced_ref=v2.1.0", "spiced_ref is v2.1.0"),
        ):
            with self.subTest(builds=builds):
                proc, output, env = self.choose("workflow_dispatch", "refs/heads/trunk", builds)
                self.assertEqual((proc.returncode, output, env), (1, "", ""), proc.stdout)
                self.assertIn(f"::error::{named}, which is not trunk or a commit on trunk", proc.stdout)

    def test_any_image_gets_the_read_only_role(self):
        # A tag names no commit, and can be published from any branch, even `trunk` or a SHA.
        for images, named in (
            ("channel=nightly", "channel is nightly"),
            (f"custom_image_tag={self.trunk}", f"custom_image_tag is {self.trunk}"),
            ("custom_image_tag=trunk", "custom_image_tag is trunk"),
            ("custom_image_tag=\nec2_images=spiceai/spiceai:2.3.2", "ec2_images is spiceai/spiceai:2.3.2"),
        ):
            with self.subTest(images=images):
                proc, output, env = self.choose("push", "refs/heads/trunk", f"spiced_commit={self.trunk}", images=images)
                self.assert_role(proc, output, env, full=False)
                self.assertIn(f"::notice::{named}, an image, which names no trunk commit", proc.stdout)

    def test_empty_images_keep_the_full_roles(self):
        self.assert_role(*self.choose("push", "refs/heads/trunk", images="custom_image_tag=\nec2_images=\n"), full=True)

    def test_trunk_that_cannot_be_fetched_fails_rather_than_guessing(self):
        proc, output, _ = self.choose(
            "push", "refs/heads/trunk", f"spiced_commit={self.trunk}", server=f"file://{self.tmp.name}/missing"
        )
        self.assertEqual((proc.returncode, output), (1, ""))
        self.assertIn("::error::Could not fetch trunk", proc.stdout)

    def test_builds_are_not_checked_off_trunk(self):
        # The run already gets the read-only role, so nothing is fetched.
        proc, output, env = self.choose(
            "workflow_dispatch", "refs/heads/feature", f"spiced_commit={self.trunk}", server="file:///nonexistent"
        )
        self.assert_role(proc, output, env, full=False)


if __name__ == "__main__":
    unittest.main()
