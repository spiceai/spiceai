#!/usr/bin/env python3
"""Tests for .github/actions/verify-attestation, which a job with the full CI roles runs on a
binary or archive it downloaded before running it.

Runs the action's own `run:` step under bash with a stand-in `gh` that records its arguments
and answers as `gh attestation verify` does: exit 0 for an attestation that matches, 1 for
none (an unattested file, another signer, a branch other than trunk). The real command was
also run against real attestations; see the pull request that added this test.
"""

import os
import subprocess
import tempfile
import unittest
from pathlib import Path

import yaml

ACTION = Path(__file__).resolve().parents[1] / "actions" / "verify-attestation" / "action.yml"


class VerifyAttestationTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        self.script = self.dir / "verify.sh"
        self.script.write_text(yaml.safe_load(ACTION.read_text())["runs"]["steps"][0]["run"])
        self.bin = self.dir / "bin"
        self.bin.mkdir()
        self.calls = self.dir / "calls"
        gh = self.bin / "gh"
        gh.write_text(
            "#!/usr/bin/env bash\n"
            f'printf "%s\\n" "$*" >> "{self.calls}"\n'
            'echo "verification output"\n'
            'exit "${FAKE_GH_EXIT:-0}"\n'
        )
        gh.chmod(0o755)
        self.file = self.dir / "spiced.tar.gz"
        self.file.write_bytes(b"spiced")

    def tearDown(self):
        self.tmp.cleanup()

    def verify(self, file=None, gh_exit=0):
        env = {
            "PATH": f"{self.bin}{os.pathsep}{os.environ['PATH']}",
            "FILE": str(file or self.file),
            "SIGNER_WORKFLOW": ".github/workflows/build_release.yml",
            "GITHUB_REPOSITORY": "spiceai/spiceai",
            "GH_TOKEN": "a-token",
            "FAKE_GH_EXIT": str(gh_exit),
        }
        return subprocess.run(["bash", str(self.script)], env=env, capture_output=True, text=True, timeout=30)

    def test_it_asks_for_this_repository_its_signer_and_trunk(self):
        proc = self.verify()
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertEqual(
            self.calls.read_text(),
            f"attestation verify {self.file} --repo spiceai/spiceai --signer-workflow "
            "spiceai/spiceai/.github/workflows/build_release.yml --source-ref refs/heads/trunk\n",
        )
        self.assertIn("attested by spiceai/spiceai/.github/workflows/build_release.yml on trunk", proc.stdout)

    def test_no_matching_attestation_fails_closed(self):
        # An unattested file, another signer and a branch other than trunk all fail the same way.
        proc = self.verify(gh_exit=1)
        self.assertEqual(proc.returncode, 1)
        self.assertIn("verification output", proc.stdout)
        self.assertIn(
            f"::error::{self.file} has no build attestation from "
            "spiceai/spiceai/.github/workflows/build_release.yml on trunk; not running it.",
            proc.stdout,
        )

    def test_a_missing_file_fails_without_asking(self):
        proc = self.verify(file=self.dir / "absent.tar.gz")
        self.assertEqual(proc.returncode, 1)
        self.assertFalse(self.calls.exists())
        self.assertIn("does not exist", proc.stdout)

    def test_the_token_is_never_printed(self):
        for gh_exit in (0, 1):
            with self.subTest(gh_exit=gh_exit):
                proc = self.verify(gh_exit=gh_exit)
                self.assertNotIn("a-token", proc.stdout + proc.stderr)


if __name__ == "__main__":
    unittest.main()
