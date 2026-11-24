#!/usr/bin/env python3
"""Tests for .github/actions/ci-store-prefix: the key prefix a run reads and writes in the CI
object store's shared buckets, and the spicepod rewrite that points spiced at it.

Runs the action's own `run:` step under bash, as GitHub would.
"""

import os
import subprocess
import tempfile
import unittest
from pathlib import Path

import yaml

ACTION = Path(__file__).resolve().parents[1] / "actions" / "ci-store-prefix" / "action.yml"
SPICEPOD = """\
datasets:
  - from: delta_lake:s3://benchmarks/cold_start/audit_events_3m/
    params:
      cayenne_datalake_location: s3://benchmarks/cayenne-cold-sf10/
      other_location: s3://benchmarks/cayenne-cold-sf1000/x
      run_location: s3://benchmarks/cayenne-datalake/123/
      public: s3://spice-datasets/cold_start/
"""


class CiStorePrefixTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        self.script = self.dir / "prefix.sh"
        self.script.write_text(yaml.safe_load(ACTION.read_text())["runs"]["steps"][0]["run"])

    def tearDown(self):
        self.tmp.cleanup()

    def prefix(self, ref, spicepod=""):
        out, env_file = self.dir / "out", self.dir / "env"
        out.write_text("")
        env_file.write_text("")
        env = {"PATH": os.environ["PATH"], "GITHUB_REF": ref, "GITHUB_OUTPUT": str(out),
               "GITHUB_ENV": str(env_file), "SPICEPOD": spicepod}
        proc = subprocess.run(["bash", str(self.script)], env=env, capture_output=True, text=True, timeout=30)
        return proc, out.read_text(), env_file.read_text()

    def test_trunk_uses_the_shared_keys(self):
        proc, out, env = self.prefix("refs/heads/trunk")
        self.assertEqual((proc.returncode, out, env), (0, "prefix=\n", "CI_STORE_PREFIX=\n"))

    def test_any_other_ref_uses_its_own_untrusted_prefix(self):
        for ref in ("refs/pull/123/merge", "refs/heads/feature/x", "refs/heads/release/2.4",
                    "refs/heads/gh-readonly-queue/trunk/pr-1-abc", "refs/tags/v2.4.0"):
            with self.subTest(ref=ref):
                proc, out, env = self.prefix(ref)
                self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
                self.assertEqual(out, f"prefix=untrusted/{ref}/\n")
                self.assertEqual(env, f"CI_STORE_PREFIX=untrusted/{ref}/\n")

    def test_something_that_is_not_a_ref_is_refused(self):
        for ref in ("", "trunk", "../trunk"):
            with self.subTest(ref=ref):
                proc, out, _ = self.prefix(ref)
                self.assertEqual((proc.returncode, out), (1, ""))
                self.assertIn("::error::", proc.stdout)

    def test_the_spicepod_gets_this_runs_fixed_benchmark_locations(self):
        pod = self.dir / "spicepod.yaml"
        pod.write_text(SPICEPOD)
        proc, _, _ = self.prefix("refs/pull/7/merge", str(pod))
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertEqual(
            pod.read_text(),
            SPICEPOD.replace("s3://benchmarks/cold_start/", "s3://benchmarks/untrusted/refs/pull/7/merge/cold_start/")
            .replace("s3://benchmarks/cayenne-cold-sf10/", "s3://benchmarks/untrusted/refs/pull/7/merge/cayenne-cold-sf10/")
            .replace("s3://benchmarks/cayenne-cold-sf1000/", "s3://benchmarks/untrusted/refs/pull/7/merge/cayenne-cold-sf1000/"),
        )
        self.assertFalse((self.dir / "spicepod.yaml.bak").exists())

    def test_on_trunk_the_spicepod_is_left_alone(self):
        pod = self.dir / "spicepod.yaml"
        pod.write_text(SPICEPOD)
        self.assertEqual(self.prefix("refs/heads/trunk", str(pod))[0].returncode, 0)
        self.assertEqual(pod.read_text(), SPICEPOD)


if __name__ == "__main__":
    unittest.main()
