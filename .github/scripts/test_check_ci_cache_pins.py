#!/usr/bin/env python3
"""Tests for check_ci_cache_pins.py, run on a copy of the files it reads.

Each test copies the checker, pins.json, rust-toolchain.toml and the install-cargo-deny
and setup-rust actions into a temporary tree laid out like the repository, changes one of
them, and runs the checker there as CI does.
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
    ".github/actions/setup-rust/action.yml",
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

    def test_the_repository_pins_rustup_init_for_every_mac_setup_rust_maps(self):
        result = self.run_check()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertRegex(result.stdout, r"rustup-init \d+\.\d+\.\d+\.")

    def test_every_mac_host_setup_rust_maps_must_have_a_rustup_init_pin(self):
        self.edit_pins(lambda pins: pins["rustup-init"]["sha256"].pop("x86_64-apple-darwin"))
        result = self.run_check()
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("rustup-init.sha256 has no pin for x86_64-apple-darwin", result.stdout)
        self.assertIn("/x86_64-apple-darwin/rustup-init.sha256", result.stdout)

    def test_a_missing_rustup_init_entry_fails(self):
        self.edit_pins(lambda pins: pins.pop("rustup-init"))
        result = self.run_check()
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("has no rustup-init entry", result.stdout)

    def test_a_malformed_rustup_init_pin_fails(self):
        def change(pins):
            pins["rustup-init"]["version"] = "latest"
            pins["rustup-init"]["url"] = "https://sh.rustup.rs/{version}/{target}"
            pins["rustup-init"]["sha256"]["aarch64-apple-darwin"] = "ABC"

        self.edit_pins(change)
        result = self.run_check()
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("rustup-init.version 'latest' is not a release version.", result.stdout)
        self.assertIn("rustup-init.url must start with https://static.rust-lang.org/rustup/archive/", result.stdout)
        self.assertIn("rustup-init.sha256.aarch64-apple-darwin is not a lowercase hex SHA-256.", result.stdout)

    def test_a_setup_rust_with_no_mac_arms_fails(self):
        action = self.tmp / ".github/actions/setup-rust/action.yml"
        action.write_text("name: Sets up the Rust toolchain\nruns:\n  using: composite\n  steps: []\n")
        result = self.run_check()
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("Found no `Darwin-<arch>) host_triple=<triple> ;;` arm", result.stdout)


if __name__ == "__main__":
    unittest.main()
