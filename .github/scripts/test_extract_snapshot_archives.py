#!/usr/bin/env python3
"""Tests for extract_snapshot_archives.py: snapshots pass, anything else is refused."""
import io
import os
import subprocess
import sys
import tarfile
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).with_name("extract_snapshot_archives.py")


def add_file(tar, name, data=b"---\nsnapshot\n"):
    info = tarfile.TarInfo(name)
    info.size = len(data)
    tar.addfile(info, io.BytesIO(data))


def add_link(tar, name, target, kind=tarfile.SYMTYPE):
    info = tarfile.TarInfo(name)
    info.type = kind
    info.linkname = target
    tar.addfile(info)


class ExtractSnapshotArchivesTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.archives = root / "archives"
        self.archives.mkdir()
        self.dest = root / "checkout"
        (self.dest / ".github/actions/push-snap-changes").mkdir(parents=True)
        self.action = self.dest / ".github/actions/push-snap-changes/action.yml"
        self.action.write_text("original\n")

    def tearDown(self):
        self.tmp.cleanup()

    def archive(self, build, name="part.tar.gz"):
        with tarfile.open(self.archives / name, "w:gz") as tar:
            build(tar)

    def run_script(self):
        return subprocess.run(
            [sys.executable, str(SCRIPT), str(self.archives), str(self.dest)],
            capture_output=True, text=True,
        )

    def assert_refused(self, build, needle):
        self.archive(build)
        result = self.run_script()
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("refusing snapshot artifact", result.stdout)
        self.assertIn(needle, result.stdout)
        self.assertEqual(self.action.read_text(), "original\n")
        self.assertFalse((self.dest / "crates").exists(), "nothing is written when any member is refused")

    def test_extracts_snapshots(self):
        self.archive(lambda t: add_file(t, "crates/a/src/snapshots/a__x.snap", b"new\n"), "one.tar.gz")
        self.archive(lambda t: add_file(t, "./crates/b/tests/snapshots/b__y.snap"), "two.tar.gz")
        result = self.run_script()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual((self.dest / "crates/a/src/snapshots/a__x.snap").read_bytes(), b"new\n")
        self.assertTrue((self.dest / "crates/b/tests/snapshots/b__y.snap").is_file())
        self.assertIn("2 snapshot file(s) from 2 archive(s)", result.stdout)

    def test_no_archives_is_a_no_op(self):
        result = self.run_script()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("0 snapshot file(s) from 0 archive(s)", result.stdout)

    def test_refuses_workflow_files(self):
        self.assert_refused(lambda t: add_file(t, ".github/actions/push-snap-changes/action.yml"), "under .github/")

    def test_refuses_snap_named_file_under_github(self):
        self.assert_refused(lambda t: add_file(t, ".github/x.snap"), "under .github/")

    def test_refuses_git_dir(self):
        self.assert_refused(lambda t: add_file(t, ".git/hooks/x.snap"), "under .git/")

    def test_refuses_non_snapshot(self):
        self.assert_refused(lambda t: add_file(t, "crates/a/build.rs"), "not a .snap file")

    def test_refuses_parent_traversal(self):
        self.assert_refused(lambda t: add_file(t, "crates/../../evil.snap"), "escapes the checkout")

    def test_refuses_absolute_path(self):
        self.assert_refused(lambda t: add_file(t, "/tmp/evil.snap"), "not a relative POSIX path")

    def test_refuses_symlink(self):
        self.assert_refused(lambda t: add_link(t, "crates/a.snap", "/etc/passwd"), "not a regular file")

    def test_refuses_hardlink(self):
        self.assert_refused(lambda t: add_link(t, "crates/a.snap", ".github/actions/push-snap-changes/action.yml", tarfile.LNKTYPE), "not a regular file")

    def test_one_bad_member_blocks_the_good_ones(self):
        def build(t):
            add_file(t, "crates/a/src/snapshots/ok.snap")
            add_file(t, ".github/scripts/approve_bot_pr_runs.sh")
        self.assert_refused(build, "approve_bot_pr_runs.sh")

    def test_does_not_follow_symlinked_directory_out_of_checkout(self):
        outside = Path(self.tmp.name) / "outside"
        outside.mkdir()
        os.symlink(outside, self.dest / "crates")
        self.archive(lambda t: add_file(t, "crates/x.snap"))
        result = self.run_script()
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("resolves outside", result.stdout)
        self.assertEqual(list(outside.iterdir()), [])


if __name__ == "__main__":
    unittest.main()
