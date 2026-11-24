#!/usr/bin/env python3
"""Tests for the download failures that stop a toolchain install instead of falling back.

cached_fetch.sh exits 4 when a source offers more than CI_FETCH_MAX_BYTES, and 2 when its
arguments or settings are wrong, such as a malformed CI_FETCH_MAX_BYTES
(test_cached_fetch.py). rust_dist_mirror.py must pass either on as its own, and
setup-rust must then fail rather than install the toolchain with rustup, which would
download the same files without that bound or those settings. Every other failure keeps
falling back to rustup, so a first install still works when the cache or the network
fails.

The mirror runs with a stand-in for cached_fetch.sh and a pins file of its own, so a
toolchain bump in the real pins.json changes nothing here. setup-rust's own step runs from
its action.yml, with HOME, CARGO_HOME and RUSTUP_HOME in a temporary directory and
stand-ins for rustup, cargo, rustc and the mirror, so nothing is downloaded or installed.
"""

import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import yaml

GITHUB_DIR = Path(__file__).resolve().parent.parent
MIRROR = GITHUB_DIR / "scripts" / "rust_dist_mirror.py"
SETUP_RUST = GITHUB_DIR / "actions" / "setup-rust" / "action.yml"
# A toolchain no real pin names, so these tests never depend on the one in use.
TOOLCHAIN = "9.9.9"
PINS = {"rust": {"components": ["rustc"], "toolchains": {TOOLCHAIN: {"manifest_sha256": "0" * 64}}}}

# Logs every call with the dist server it was given, and does what setup-rust needs.
FAKE_RUSTUP = """#!/usr/bin/env bash
echo "rustup $* dist=${RUSTUP_DIST_SERVER:-}" >> "$RUSTUP_LOG"
case "$(uname -s)-$(uname -m)" in
  Linux-x86_64) host=x86_64-unknown-linux-gnu ;;
  Linux-aarch64) host=aarch64-unknown-linux-gnu ;;
  Darwin-arm64) host=aarch64-apple-darwin ;;
  *) host=x86_64-apple-darwin ;;
esac
case "$1" in
  show) echo "Default host: $host" ;;
  toolchain)
    bin="$RUSTUP_HOME/toolchains/$3-$host/bin"
    mkdir -p "$bin"
    for tool in rustc cargo rustfmt cargo-clippy; do
      printf '#!/bin/sh\\n' > "$bin/$tool"
      chmod +x "$bin/$tool"
    done
    ;;
  which) echo "$RUSTUP_HOME/toolchains/$3-$host/bin/rustc" ;;
esac
"""


class MirrorExitTest(unittest.TestCase):
    """rust_dist_mirror.py, with cached_fetch.sh replaced by one that exits as told."""

    def run_mirror(self, fetch_status, toolchain=TOOLCHAIN):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "scripts").mkdir()
            (root / "ci-cache").mkdir()
            shutil.copy(MIRROR, root / "scripts" / MIRROR.name)
            (root / "ci-cache" / "pins.json").write_text(json.dumps(PINS))
            (root / "scripts" / "cached_fetch.sh").write_text(f"exit {fetch_status}\n")
            return subprocess.run(
                [sys.executable, str(root / "scripts" / MIRROR.name), "--toolchain", toolchain,
                 "--host", "x86_64-unknown-linux-gnu", "--out", str(root / "out")],
                capture_output=True, text=True, timeout=60,
            )

    def test_a_download_over_the_bound_exits_4(self):
        self.assertEqual(self.run_mirror(4).returncode, 4)

    def test_refused_download_settings_exit_2(self):
        self.assertEqual(self.run_mirror(2).returncode, 2)

    def test_any_other_failed_download_exits_1(self):
        proc = self.run_mirror(1)
        self.assertEqual(proc.returncode, 1, proc.stderr)

    def test_a_toolchain_without_a_pin_exits_3_before_any_download(self):
        self.assertEqual(self.run_mirror(4, toolchain="9.9.8").returncode, 3)


class SetupRustTest(unittest.TestCase):
    """setup-rust's Linux/macOS step, with each answer the mirror can give."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        action = yaml.safe_load(SETUP_RUST.read_text())
        (step,) = [
            s for s in action["runs"]["steps"] if s.get("name") == "Set up Rust toolchain (Linux/macOS)"
        ]
        self.step = self.dir / "step.sh"
        self.step.write_text(step["run"])
        # The step finds the mirror at $GITHUB_ACTION_PATH/../../scripts/rust_dist_mirror.py.
        self.action_path = self.dir / "gh" / "actions" / "setup-rust"
        self.action_path.mkdir(parents=True)
        (self.dir / "gh" / "scripts").mkdir()
        self.mirror = self.dir / "gh" / "scripts" / "rust_dist_mirror.py"
        self.cargo_bin = self.dir / "cargo" / "bin"
        self.cargo_bin.mkdir(parents=True)
        (self.cargo_bin / "rustup").write_text(FAKE_RUSTUP)
        for tool in ("cargo", "rustc"):
            (self.cargo_bin / tool).write_text(f"#!/bin/sh\necho '{tool} stand-in'\n")
        for tool in self.cargo_bin.iterdir():
            tool.chmod(0o755)
        self.work = self.dir / "work"
        self.work.mkdir()
        (self.work / "rust-toolchain.toml").write_text(f'[toolchain]\nchannel = "{TOOLCHAIN}"\n')
        self.log = self.dir / "rustup.log"

    def tearDown(self):
        self.tmp.cleanup()

    def run_step(self, mirror_status):
        self.mirror.write_text(f"import sys\nsys.exit({mirror_status})\n")
        self.log.write_text("")
        home = self.dir / "home"
        home.mkdir(exist_ok=True)
        env = {
            "PATH": os.environ["PATH"],
            "HOME": str(home),
            "CARGO_HOME": str(self.cargo_bin.parent),
            "RUSTUP_HOME": str(self.dir / "rustup"),
            "RUSTUP_LOG": str(self.log),
            "RUNNER_TEMP": str(self.dir),
            "GITHUB_PATH": str(self.dir / "github_path"),
            "GITHUB_ENV": str(self.dir / "github_env"),
            "GITHUB_ACTION_PATH": str(self.action_path),
            "INPUT_TOOLCHAIN": "",
            "CI_CACHE_URL": "http://127.0.0.1:9/ci-cache",
        }
        proc = subprocess.run(
            ["bash", "-e", str(self.step)],
            cwd=self.work, env=env, capture_output=True, text=True, timeout=120,
        )
        return proc, self.log.read_text().splitlines()

    def installs(self, calls):
        return [call for call in calls if call.startswith(f"rustup toolchain install {TOOLCHAIN}")]

    def test_a_download_over_the_bound_fails_the_step_without_rustup(self):
        proc, calls = self.run_step(4)
        self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
        self.assertIn("larger than CI_FETCH_MAX_BYTES", proc.stdout)
        self.assertIn("::error::", proc.stdout)
        self.assertEqual(self.installs(calls), [])

    def test_refused_download_settings_fail_the_step_without_rustup(self):
        proc, calls = self.run_step(2)
        self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
        self.assertIn("download settings", proc.stdout)
        self.assertIn("::error::", proc.stdout)
        self.assertEqual(self.installs(calls), [])

    def test_any_other_mirror_failure_still_installs_with_rustup(self):
        proc, calls = self.run_step(1)
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertIn(f"Could not assemble Rust {TOOLCHAIN} from its pinned release (exit 1)", proc.stdout)
        (install,) = self.installs(calls)
        self.assertTrue(install.endswith(" dist="), install)

    def test_a_toolchain_without_a_pin_installs_with_rustup_quietly(self):
        proc, calls = self.run_step(3)
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertNotIn("::warning::", proc.stdout)
        (install,) = self.installs(calls)
        self.assertTrue(install.endswith(" dist="), install)

    def test_a_mirror_installs_from_the_mirror(self):
        proc, calls = self.run_step(0)
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertIn(f"Installed Rust {TOOLCHAIN} from the CI cache's pinned release.", proc.stdout)
        (install,) = self.installs(calls)
        self.assertIn(" dist=file://", install)


if __name__ == "__main__":
    unittest.main()
