#!/usr/bin/env python3
"""Tests for setup-libduckdb: a cache of the job's own, and only a safe path into GITHUB_ENV.

The action's own step runs from action.yml in a temporary workspace whose
scripts/libduckdb.py is a stand-in that prints what a test tells it to, so nothing is
downloaded. GITHUB_ENV takes one variable per line, so a directory with a line break in it
would add variables of its own.
"""

import os
import subprocess
import tempfile
import unittest
from pathlib import Path

import yaml

ACTION = Path(__file__).resolve().parent.parent / "actions" / "setup-libduckdb" / "action.yml"

STAND_IN = """\
import os, sys
cache = os.environ["LIBDUCKDB_CACHE_DIR"]
open(os.environ["STUB_LOG"], "w").write(cache)
mode = os.environ["STUB_MODE"]
target = os.path.join(cache, "9.9.9")
os.makedirs(target, exist_ok=True)
open(os.path.join(target, "duckdb.toml"), "w").write('[Driver]\\nshared = "x"\\n')
if mode == "line-break":
    target = os.path.join(cache, "9.9.9\\nBASH_ENV=/tmp/evil.sh")
elif mode == "separator":
    target = os.path.join(cache, "9.9.9:/tmp/evil")
elif mode == "outside":
    target = os.environ["STUB_OUTSIDE"]
print(target)
"""


class SetupLibduckdbTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.dir = Path(self.tmp.name).resolve()
        action = yaml.safe_load(ACTION.read_text())
        (step,) = [s for s in action["runs"]["steps"] if s.get("name") == "Install libduckdb"]
        self.step = self.dir / "step.sh"
        self.step.write_text(step["run"])
        self.workspace = self.dir / "workspace"
        (self.workspace / "scripts").mkdir(parents=True)
        (self.workspace / "scripts" / "libduckdb.py").write_text(STAND_IN)
        self.runner_temp = self.dir / "temp"
        self.runner_temp.mkdir()
        self.github_env = self.dir / "github_env"
        self.log = self.dir / "stub.log"
        self.outside = self.dir / "outside"
        (self.outside).mkdir()
        (self.outside / "duckdb.toml").write_text('[Driver]\nshared = "x"\n')

    def run_step(self, mode="ok"):
        self.github_env.write_text("")
        env = {
            "PATH": os.environ["PATH"],
            "RUNNER_TEMP": str(self.runner_temp),
            "GITHUB_ENV": str(self.github_env),
            "LIBDUCKDB_VERSION": "",
            "STUB_MODE": mode,
            "STUB_LOG": str(self.log),
            "STUB_OUTSIDE": str(self.outside),
        }
        return subprocess.run(
            ["bash", "-e", str(self.step)], cwd=self.workspace, env=env, capture_output=True, text=True, timeout=60
        )

    def test_the_cache_is_a_new_directory_and_one_line_is_written(self):
        # What an earlier job could leave at the old fixed cache path.
        (self.runner_temp / "libduckdb").symlink_to(self.outside)
        proc = self.run_step()
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        cache = Path(self.log.read_text())
        self.assertEqual(cache.parent, self.runner_temp)
        self.assertTrue(cache.name.startswith("libduckdb.") and not cache.is_symlink(), cache)
        self.assertEqual(self.github_env.read_text().splitlines(), [f"ADBC_DRIVER_PATH={cache / '9.9.9'}"])

    def test_a_directory_with_a_line_break_is_not_written(self):
        proc = self.run_step("line-break")
        self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
        self.assertIn("a line break or a ':' in it", proc.stdout)
        self.assertEqual(self.github_env.read_text(), "")

    def test_a_directory_with_a_path_separator_is_not_written(self):
        proc = self.run_step("separator")
        self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
        self.assertIn("a line break or a ':' in it", proc.stdout)
        self.assertEqual(self.github_env.read_text(), "")

    def test_a_directory_outside_the_cache_is_not_written(self):
        proc = self.run_step("outside")
        self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
        self.assertIn("printed a directory outside", proc.stdout)
        self.assertEqual(self.github_env.read_text(), "")


if __name__ == "__main__":
    unittest.main()
