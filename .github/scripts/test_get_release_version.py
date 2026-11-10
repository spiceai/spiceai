"""Tests for get_release_version.py.

Each test runs the script as the workflows do: a fresh process whose working
directory holds version.txt (and optionally docs/release_notes/), with the
GitHub Actions ref variables set and GITHUB_ENV pointing at a file.
"""
import os
import subprocess
import sys
import tempfile
import unittest

SCRIPT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "get_release_version.py")


class GetReleaseVersionTests(unittest.TestCase):
    def run_script(self, version, ref_type=None, ref_name=None, release_notes=False, release_version=None):
        with tempfile.TemporaryDirectory() as workdir:
            with open(os.path.join(workdir, "version.txt"), "w") as f:
                f.write(version + "\n")
            if release_notes:
                notes_dir = os.path.join(workdir, "docs", "release_notes")
                os.makedirs(notes_dir)
                with open(os.path.join(notes_dir, "v{}.md".format(version)), "w") as f:
                    f.write("notes\n")

            github_env = os.path.join(workdir, "github_env")
            env = {
                k: v
                for k, v in os.environ.items()
                if k not in ("GITHUB_REF_TYPE", "GITHUB_REF_NAME", "RELEASE_VERSION")
            }
            env["GITHUB_ENV"] = github_env
            if ref_type is not None:
                env["GITHUB_REF_TYPE"] = ref_type
            if ref_name is not None:
                env["GITHUB_REF_NAME"] = ref_name
            if release_version is not None:
                env["RELEASE_VERSION"] = release_version

            proc = subprocess.run(
                [sys.executable, SCRIPT],
                cwd=workdir,
                env=env,
                capture_output=True,
                text=True,
            )
            written = None
            if os.path.exists(github_env):
                with open(github_env) as f:
                    written = f.read()
            return proc, written

    def assert_ok(self, proc):
        self.assertEqual(proc.returncode, 0, msg=proc.stdout + proc.stderr)

    def assert_rejected(self, proc, written, tag, version):
        self.assertNotEqual(proc.returncode, 0, msg=proc.stdout + proc.stderr)
        # Nothing reaches GITHUB_ENV, so a later step cannot publish with it.
        self.assertIn(written, (None, ""))
        self.assertIn("::error", proc.stdout)
        self.assertIn("'{}'".format(tag), proc.stdout)
        self.assertIn("version.txt '{}'".format(version), proc.stdout)
        self.assertIn("expected tag 'v{}'".format(version), proc.stdout)

    def test_matching_tag_writes_rel_version(self):
        proc, written = self.run_script("2.4.1", "tag", "v2.4.1", release_notes=True)

        self.assert_ok(proc)
        self.assertEqual(written, "HAS_RELEASE_NOTES=true\nREL_VERSION=2.4.1\n")

    def test_tag_ahead_of_version_txt_is_rejected(self):
        # The v2.4.1 tag pushed on a commit whose version.txt still said 2.4.0.
        proc, written = self.run_script("2.4.0", "tag", "v2.4.1", release_notes=True)

        self.assert_rejected(proc, written, "v2.4.1", "2.4.0")

    def test_enterprise_tag_on_oss_version_is_rejected(self):
        proc, written = self.run_script("2.4.1", "tag", "v2.4.1-enterprise")

        self.assert_rejected(proc, written, "v2.4.1-enterprise", "2.4.1")

    def test_tag_without_v_prefix_is_rejected(self):
        proc, written = self.run_script("2.4.1", "tag", "2.4.1")

        self.assert_rejected(proc, written, "2.4.1", "2.4.1")

    def test_rc_tag_matches(self):
        proc, written = self.run_script("2.4.0-rc.1", "tag", "v2.4.0-rc.1", release_notes=True)

        self.assert_ok(proc)
        self.assertEqual(
            written,
            "HAS_RELEASE_NOTES=true\nPRE_RELEASE=true\nREL_VERSION=2.4.0-rc.1\n",
        )

    def test_enterprise_tag_matches(self):
        proc, written = self.run_script("2.0.1-enterprise", "tag", "v2.0.1-enterprise", release_notes=True)

        self.assert_ok(proc)
        self.assertEqual(written, "HAS_RELEASE_NOTES=true\nREL_VERSION=2.0.1-enterprise\n")

    def test_rc_enterprise_tag_matches(self):
        proc, written = self.run_script("2.4.0-rc.1-enterprise", "tag", "v2.4.0-rc.1-enterprise")

        self.assert_ok(proc)
        self.assertEqual(written, "PRE_RELEASE=true\nREL_VERSION=2.4.0-rc.1-enterprise\n")

    def test_release_build_matching_version_txt(self):
        proc, written = self.run_script("2.4.2", "branch", "release/2.4", release_notes=True, release_version="2.4.2")

        self.assert_ok(proc)
        self.assertEqual(written, "HAS_RELEASE_NOTES=true\nREL_VERSION=2.4.2\n")

    def test_release_build_of_another_version_is_rejected(self):
        proc, written = self.run_script("2.4.1", "branch", "release/2.4", release_version="2.4.2")

        self.assertNotEqual(proc.returncode, 0, msg=proc.stdout + proc.stderr)
        self.assertIn(written, (None, ""))
        self.assertIn("::error title=Release version does not match version.txt::Release version '2.4.2' does not match version.txt '2.4.1'", proc.stdout)

    def test_empty_release_version_is_not_checked(self):
        proc, written = self.run_script("2.5.0-dev", "branch", "trunk", release_version="")

        self.assert_ok(proc)

    def test_branch_ref_is_not_checked(self):
        # trunk, release branches, PRs and branch dispatches never match a tag.
        proc, written = self.run_script("2.5.0-unstable", "branch", "trunk")

        self.assert_ok(proc)
        self.assertEqual(written, "PRE_RELEASE=true\nREL_VERSION=2.5.0-unstable\n")

    def test_release_branch_with_notes(self):
        proc, written = self.run_script("2.4.1", "branch", "release/2.4", release_notes=True)

        self.assert_ok(proc)
        self.assertEqual(written, "HAS_RELEASE_NOTES=true\nREL_VERSION=2.4.1\n")

    def test_no_ref_variables(self):
        proc, written = self.run_script("2.4.1")

        self.assert_ok(proc)
        self.assertEqual(written, "PRE_RELEASE=true\nREL_VERSION=2.4.1\n")


if __name__ == "__main__":
    unittest.main()
