#!/usr/bin/env python3
"""Tests for setup-nextest: cargo-nextest only from its pinned, checked release archive.

The action's own step runs from action.yml with the real cached_fetch.sh, a pins file of
its own whose URL is a file:// archive holding a stand-in cargo-nextest, and CARGO_HOME,
HOME and RUNNER_TEMP in a temporary directory, so nothing is downloaded.
"""

import hashlib
import io
import json
import os
import platform
import shutil
import subprocess
import tarfile
import tempfile
import unittest
from pathlib import Path

import yaml

GITHUB_DIR = Path(__file__).resolve().parent.parent
ACTION = GITHUB_DIR / "actions" / "setup-nextest" / "action.yml"
TARGET = {
    ("Linux", "x86_64"): "x86_64-unknown-linux-gnu",
    ("Linux", "aarch64"): "aarch64-unknown-linux-gnu",
    ("Darwin", "arm64"): "universal-apple-darwin",
    ("Darwin", "x86_64"): "universal-apple-darwin",
}.get((platform.system(), platform.machine()), "")
STAND_IN = b'#!/bin/sh\necho "cargo-nextest stand-in $*"\n'


def archive(members: dict) -> bytes:
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as tf:
        for name, data in members.items():
            info = tarfile.TarInfo(name)
            info.size = len(data)
            info.mode = 0o755
            tf.addfile(info, io.BytesIO(data))
    return buffer.getvalue()


@unittest.skipUnless(TARGET, "setup-nextest maps no target for this machine")
class SetupNextestTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.dir = Path(self.tmp.name).resolve()
        action = yaml.safe_load(ACTION.read_text())
        (step,) = [s for s in action["runs"]["steps"] if s.get("name") == "Install Nextest (Linux/macOS)"]
        self.step = self.dir / "step.sh"
        self.step.write_text(step["run"])
        gh = self.dir / "gh"
        self.action_path = gh / "actions" / "setup-nextest"
        self.action_path.mkdir(parents=True)
        (gh / "scripts").mkdir()
        (gh / "ci-cache").mkdir()
        shutil.copy(GITHUB_DIR / "scripts" / "cached_fetch.sh", gh / "scripts" / "cached_fetch.sh")
        self.pins = gh / "ci-cache" / "pins.json"
        self.release = self.dir / "release" / "cargo-nextest-0.0.1.tar.gz"
        self.release.parent.mkdir()
        # A member besides cargo-nextest, which is never extracted.
        self.release.write_bytes(archive({"cargo-nextest": STAND_IN, "extra": b"#!/bin/sh\n"}))
        self.write_pins(hashlib.sha256(self.release.read_bytes()).hexdigest())
        self.cargo_home = self.dir / "cargo"
        self.runner_temp = self.dir / "temp"
        self.runner_temp.mkdir()
        self.github_path = self.dir / "github_path"

    def write_pins(self, sha256, target=TARGET):
        url = f"file://{self.release.parent}/cargo-nextest-{{version}}.tar.gz"
        self.pins.write_text(json.dumps({"cargo-nextest": {"version": "0.0.1", "url": url, "sha256": {target: sha256}}}))

    def run_step(self):
        self.github_path.write_text("")
        env = {
            "PATH": os.environ["PATH"],
            "HOME": str(self.dir / "home"),
            "CARGO_HOME": str(self.cargo_home),
            "RUNNER_TEMP": str(self.runner_temp),
            "RUNNER_ENV": "github-hosted",
            "GITHUB_PATH": str(self.github_path),
            "ACTION_PATH": str(self.action_path),
            "CI_SOURCE_ATTEMPTS": "1",
        }
        return subprocess.run(["bash", "-e", str(self.step)], env=env, capture_output=True, text=True, timeout=60)

    def test_the_pinned_archive_is_checked_and_its_cargo_nextest_installed(self):
        proc = self.run_step()
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertIn("cargo-nextest-0.0.1.tar.gz: verified from file://", proc.stdout)
        self.assertIn("cargo-nextest stand-in nextest --version", proc.stdout)
        installed = self.cargo_home / "bin" / "cargo-nextest"
        self.assertEqual(installed.read_bytes(), STAND_IN)
        self.assertEqual(sorted(os.listdir(self.cargo_home / "bin")), ["cargo-nextest"])
        self.assertEqual(self.github_path.read_text().splitlines(), [str(self.cargo_home / "bin")])
        self.assertEqual(os.listdir(self.runner_temp), [])

    def test_an_archive_that_does_not_match_its_pin_installs_nothing(self):
        self.write_pins(hashlib.sha256(b"another archive").hexdigest())
        proc = self.run_step()
        self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertIn("does not match the pinned SHA-256", proc.stdout)
        self.assertFalse((self.cargo_home / "bin" / "cargo-nextest").exists())
        self.assertEqual(self.github_path.read_text(), "")

    def test_a_target_without_a_pin_fails(self):
        self.write_pins("0" * 64, target="some-other-target")
        proc = self.run_step()
        self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
        self.assertIn(f"pins no cargo-nextest release for {TARGET}", proc.stdout)
        self.assertFalse((self.cargo_home / "bin").exists())

    def test_no_step_downloads_cargo_nextest_unpinned(self):
        text = ACTION.read_text()
        self.assertNotIn("get.nexte.st", text)
        self.assertNotIn("actions/cache", text)


if __name__ == "__main__":
    unittest.main()
