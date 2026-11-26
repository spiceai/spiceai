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

    def prefix(self, ref, spicepod="", repository_id="394089041"):
        out, env_file = self.dir / "out", self.dir / "env"
        out.write_text("")
        env_file.write_text("")
        env = {"PATH": os.environ["PATH"], "GITHUB_REF": ref, "GITHUB_OUTPUT": str(out),
               "GITHUB_ENV": str(env_file), "SPICEPOD": spicepod, "GITHUB_REPOSITORY_ID": repository_id}
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
                self.assertEqual(out, f"prefix=untrusted/394089041/{ref}/\n")
                self.assertEqual(env, f"CI_STORE_PREFIX=untrusted/394089041/{ref}/\n")

    def test_a_missing_or_odd_repository_id_is_refused(self):
        for repository_id in ("", "spiceai/spiceai", "1/2"):
            with self.subTest(repository_id=repository_id):
                proc, out, _ = self.prefix("refs/pull/1/merge", repository_id=repository_id)
                self.assertEqual((proc.returncode, out), (1, ""))

    def test_trunk_needs_no_repository_id(self):
        self.assertEqual(self.prefix("refs/heads/trunk", repository_id="")[1], "prefix=\n")

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
            SPICEPOD.replace("s3://benchmarks/cold_start/", "s3://benchmarks/cold_start/untrusted/394089041/refs/pull/7/merge/")
            .replace("s3://benchmarks/cayenne-cold-sf10/", "s3://benchmarks/cayenne-cold-sf10/untrusted/394089041/refs/pull/7/merge/")
            .replace("s3://benchmarks/cayenne-cold-sf1000/", "s3://benchmarks/cayenne-cold-sf1000/untrusted/394089041/refs/pull/7/merge/"),
        )

    def test_only_a_plain_ref_keys_the_store(self):
        # `#` would cut an s3:// URL short and `"` a TOML string, so such a ref is refused, and the
        # spicepod is left as it was.
        for ref in ("refs/heads/a&b#c\\d", 'refs/heads/a"b', "refs/heads/a..b", "refs/heads/a//b", "refs/heads/a/"):
            with self.subTest(ref=ref):
                pod = self.dir / "spicepod.yaml"
                pod.write_text(SPICEPOD)
                proc, _, _ = self.prefix(ref, str(pod))
                self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
                self.assertIn("is not a plain ref", proc.stdout)
                self.assertEqual(pod.read_text(), SPICEPOD)
        pod = self.dir / "spicepod.yaml"
        pod.write_text(SPICEPOD)
        ref = "refs/heads/gh-readonly-queue/trunk/pr-7-0123abc"
        self.assertEqual(self.prefix(ref, str(pod))[0].returncode, 0)
        self.assertIn(f"s3://benchmarks/cold_start/untrusted/394089041/{ref}/audit_events_3m/", pod.read_text())
        self.assertIn("s3://benchmarks/cayenne-datalake/123/", pod.read_text())

    def test_on_trunk_the_spicepod_is_left_alone(self):
        pod = self.dir / "spicepod.yaml"
        pod.write_text(SPICEPOD)
        self.assertEqual(self.prefix("refs/heads/trunk", str(pod))[0].returncode, 0)
        self.assertEqual(pod.read_text(), SPICEPOD)


WORKFLOWS = Path(__file__).resolve().parents[1] / "workflows"

# Logs each call; `ls` lists $MC_CHILDREN, one name per line, as `mc ls` prints them.
FAKE_MC = """#!/usr/bin/env bash
echo "mc $*" >> "$MC_LOG"
case "$1" in
  ls) while read -r child; do echo "[2026-11-25 10:00:00 UTC]     0B $child"; done < "$MC_CHILDREN" ;;
esac
"""


class ColdFixtureReadersTest(unittest.TestCase):
    def test_the_e2e_job_reads_trunks_fixture(self):
        # It only reads the fixture, and a merge-queue ref is new for every entry, so a copy under
        # its own prefix would never be seeded.
        import yaml

        job = yaml.safe_load((WORKFLOWS / "e2e_test_ci.yml").read_text())["jobs"]["test_cold_start_unkeyed_append"]
        self.assertNotIn("$/.github/actions/ci-store-prefix", [step.get("uses") for step in job["steps"]])
        self.assertIn("spice-minio/benchmarks/cold_start/audit_events_3m/_SEEDED", yaml.safe_dump(job, width=10**6))

    def cleanup(self, store_prefix):
        import yaml

        steps = yaml.safe_load((WORKFLOWS / "testoperator_run_htap.yml").read_text())["jobs"]["run-htap"]["steps"]
        script = next(s for s in steps if s.get("name") == "Clean up Cayenne datalake-tier S3 objects")["run"]
        with tempfile.TemporaryDirectory() as tmp:
            work = Path(tmp)
            (work / "bin").mkdir()
            (work / "bin" / "mc").write_text(FAKE_MC)
            (work / "bin" / "mc").chmod(0o755)
            (work / "spicepod-cold.yaml").write_text("cayenne_datalake_location: s3://benchmarks/x\n")
            (work / "children").write_text("data/\nmetadata/\nuntrusted/\n")
            log = work / "log"
            log.write_text("")
            env = {"PATH": f"{work / 'bin'}{os.pathsep}{os.environ['PATH']}", "SPICEPOD": str(work / "spicepod-cold.yaml"),
                   "GITHUB_RUN_ID": "42", "MC_LOG": str(log), "MC_CHILDREN": str(work / "children"),
                   "CI_STORE_PREFIX": store_prefix}
            proc = subprocess.run(["bash", "-e", "-c", script], env=env, capture_output=True, text=True, timeout=30)
            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            return [line for line in log.read_text().splitlines() if line.startswith("mc rm")]

    def test_trunks_cleanup_leaves_other_refs_copies(self):
        removed = self.cleanup("")
        self.assertIn("mc rm --recursive --force spice-minio/benchmarks/cayenne-datalake/42/", removed)
        for tier in ("cayenne-cold-sf1000", "cayenne-cold-sf10"):
            self.assertIn(f"mc rm --recursive --force spice-minio/benchmarks/{tier}/data/", removed)
            self.assertIn(f"mc rm --recursive --force spice-minio/benchmarks/{tier}/metadata/", removed)
        self.assertFalse([line for line in removed if "untrusted" in line], removed)
        self.assertNotIn("mc rm --recursive --force spice-minio/benchmarks/cayenne-cold-sf10/", removed)

    def test_another_refs_cleanup_removes_only_its_own_copy(self):
        prefix = "untrusted/394089041/refs/pull/7/merge/"
        removed = self.cleanup(prefix)
        self.assertEqual(sorted(removed), sorted([
            "mc rm --recursive --force spice-minio/benchmarks/cayenne-datalake/42/",
            f"mc rm --recursive --force spice-minio/benchmarks/cayenne-cold-sf1000/{prefix}",
            f"mc rm --recursive --force spice-minio/benchmarks/cayenne-cold-sf10/{prefix}",
        ]))


if __name__ == "__main__":
    unittest.main()
