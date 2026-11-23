#!/usr/bin/env python3
"""Tests for check_ci_cache_pins.py, run on a copy of the files it reads.

Each test copies the checker, pins.json, rust-toolchain.toml and the install-cargo-deny
action into a temporary tree laid out like the repository, changes one of them, and runs
the checker there as CI does.
"""

import json
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
FILES = [
    ".github/scripts/check_ci_cache_pins.py",
    ".github/ci-cache/pins.json",
    ".github/actions/install-cargo-deny/action.yml",
    "rust-toolchain.toml",
]


class CheckCiCachePinsTest(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.tmp)
        for name in FILES:
            (self.tmp / name).parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(ROOT / name, self.tmp / name)

    def run_check(self):
        return subprocess.run(
            [sys.executable, str(self.tmp / ".github/scripts/check_ci_cache_pins.py")],
            capture_output=True,
            text=True,
            check=False,
        )

    def edit_pins(self, change):
        path = self.tmp / ".github/ci-cache/pins.json"
        pins = json.loads(path.read_text())
        change(pins)
        path.write_text(json.dumps(pins))

    def test_the_repository_pins_pass(self):
        result = self.run_check()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("OK: .github/ci-cache/pins.json pins Rust", result.stdout)

    def test_every_target_the_action_installs_must_be_pinned(self):
        self.edit_pins(lambda pins: pins["cargo-deny"]["sha256"].pop("aarch64-apple-darwin"))
        result = self.run_check()
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("cargo-deny.sha256 has no pin for aarch64-apple-darwin", result.stdout)

    def test_an_action_with_no_target_arms_fails(self):
        action = self.tmp / ".github/actions/install-cargo-deny/action.yml"
        action.write_text("name: Install cargo-deny\nruns:\n  using: composite\n  steps: []\n")
        result = self.run_check()
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("Found no `<os>-<arch>) target=<target> ;;` arm", result.stdout)

    def test_an_unpinned_toolchain_fails(self):
        (self.tmp / "rust-toolchain.toml").write_text('[toolchain]\nchannel = "1.0.0"\n')
        result = self.run_check()
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("rust-toolchain.toml names Rust 1.0.0", result.stdout)


if __name__ == "__main__":
    unittest.main()
