#!/usr/bin/env python3
"""Tests for install-spice's `job` scope: a directory of the job's own, checked before PATH.

The runner skips what it cannot delete when it empties RUNNER_TEMP, so a fixed directory
there can hold what an earlier job left, and a directory first on PATH runs ahead of the
system's commands. The action's own step runs here from action.yml with stand-in binaries.
"""

import os
import subprocess
import tempfile
import unittest
from pathlib import Path

import yaml

ACTION = Path(__file__).resolve().parent.parent / "actions" / "install-spice" / "action.yml"


class InstallSpiceJobScopeTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.dir = Path(self.tmp.name).resolve()
        action = yaml.safe_load(ACTION.read_text())
        (step,) = [s for s in action["runs"]["steps"] if s.get("name") == "Install Spice binaries"]
        self.step = self.dir / "step.sh"
        self.step.write_text(step["run"])
        self.build = self.dir / "build"
        self.build.mkdir()
        for name in ("spice", "spiced"):
            (self.build / name).write_text(f"#!/bin/sh\necho {name}\n")
        self.runner_temp = self.dir / "temp"
        self.runner_temp.mkdir()
        self.github_path = self.dir / "github_path"
        self.github_output = self.dir / "github_output"

    def run_step(self, scope="job"):
        self.github_path.write_text("")
        self.github_output.write_text("")
        env = {
            "PATH": os.environ["PATH"],
            "HOME": str(self.dir / "home"),
            "RUNNER_TEMP": str(self.runner_temp),
            "GITHUB_PATH": str(self.github_path),
            "GITHUB_OUTPUT": str(self.github_output),
            "INPUT_BUILD_PATH": str(self.build),
            "INPUT_INSTALL_SCOPE": scope,
        }
        return subprocess.run(["bash", "-e", str(self.step)], env=env, capture_output=True, text=True, timeout=60)

    def test_a_job_install_goes_into_a_new_directory_and_only_that_goes_on_path(self):
        # What an earlier job could leave at the old fixed path.
        planted = self.runner_temp / "spice" / "bin"
        planted.mkdir(parents=True)
        (planted / "cp").write_text("#!/bin/sh\necho planted\n")
        proc = self.run_step()
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        (added,) = self.github_path.read_text().splitlines()
        self.assertNotEqual(added, str(planted))
        self.assertTrue(Path(added).parent == self.runner_temp and Path(added).name.startswith("spice-bin."), added)
        self.assertEqual(sorted(os.listdir(added)), ["spice", "spiced"])
        self.assertEqual(Path(added, "spice").read_text(), "#!/bin/sh\necho spice\n")
        self.assertIn(f"dir={added}", self.github_output.read_text())

    def test_two_installs_get_two_directories(self):
        self.assertEqual(self.run_step().returncode, 0)
        first = self.github_path.read_text().strip()
        for name in ("spice", "spiced"):
            (self.build / name).write_text(f"#!/bin/sh\necho {name}\n")
        self.assertEqual(self.run_step().returncode, 0)
        self.assertNotEqual(self.github_path.read_text().strip(), first)

    def test_a_binary_that_is_a_link_is_not_put_on_path(self):
        target = self.dir / "elsewhere"
        target.write_text("#!/bin/sh\necho elsewhere\n")
        (self.build / "spiced").unlink()
        (self.build / "spiced").symlink_to(target)
        proc = self.run_step()
        self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
        self.assertIn("does not hold exactly the spice and spiced this step installed", proc.stdout)
        self.assertEqual(self.github_path.read_text(), "")
        self.assertNotIn("dir=", self.github_output.read_text())

    def test_the_user_scope_is_unchanged(self):
        proc = self.run_step(scope="user")
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertEqual(self.github_path.read_text().splitlines(), [str(self.dir / "home" / ".spice" / "bin")])


if __name__ == "__main__":
    unittest.main()
