#!/usr/bin/env python3
"""Tests that setup-released-spiced runs a release's spiced only once its archive is verified.

The archive must match the digest .github/release-digests.json records for a release published
before SHA256SUMS, or the release's SHA256SUMS, whose file and archive must then each carry a
build attestation from build_release.yml on a release branch. The action's download step runs
here, from action.yml, with a stand-in `curl` that serves a fake release, so nothing reaches
GitHub.
"""

import hashlib
import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

import yaml

GITHUB_DIR = Path(__file__).resolve().parents[1]
ACTION = GITHUB_DIR / "actions" / "setup-released-spiced" / "action.yml"
ASSET = "spiced_linux_x86_64.tar.gz"
COMMIT = "c" * 40

# Serves each URL from the file the routes file names for it, to stdout or to `-o FILE`.
FAKE_CURL = """#!/usr/bin/env bash
out=""
url=""
while [ $# -gt 0 ]; do
  case "$1" in
    -o) out="$2"; shift 2 ;;
    -H|--retry) shift 2 ;;
    -*) shift ;;
    *) url="$1"; shift ;;
  esac
done
file="$(awk -F '\\t' -v u="$url" '$1 == u { print $2 }' "$ROUTES")"
[ -n "$file" ] || { echo "curl: (22) 404 $url" >&2; exit 22; }
if [ -n "$out" ]; then cp "$file" "$out"; else cat "$file"; fi
"""


class SetupReleasedSpicedTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        steps = yaml.safe_load(ACTION.read_text())["runs"]["steps"]
        cls.steps = {s.get("id") or s["name"]: s for s in steps}

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        (self.dir / "bin").mkdir()
        (self.dir / "bin" / "curl").write_text(FAKE_CURL)
        (self.dir / "bin" / "curl").chmod(0o755)
        self.archive = self.dir / "served-archive"
        self.archive.write_bytes(b"a spiced archive")
        self.digest = hashlib.sha256(b"a spiced archive").hexdigest()
        (self.dir / "spicepod.yaml").write_text("version: v1\n")

    def tearDown(self):
        self.tmp.cleanup()

    def serve(self, tag, sums=None, digests=None):
        api = f"https://api.github.com/repos/owner/repo"
        assets = [{"name": ASSET, "id": 1}]
        routes = {f"{api}/releases/assets/1": self.archive}
        if sums is not None:
            assets.append({"name": "SHA256SUMS", "id": 2})
            (self.dir / "served-sums").write_text(sums)
            routes[f"{api}/releases/assets/2"] = self.dir / "served-sums"
        (self.dir / "release.json").write_text(json.dumps({"assets": assets}))
        routes[f"{api}/releases/tags/{tag}"] = self.dir / "release.json"
        (self.dir / "commit.json").write_text(json.dumps({"sha": COMMIT}))
        routes[f"{api}/commits/refs/tags/{tag}"] = self.dir / "commit.json"
        (self.dir / "routes").write_text("".join(f"{url}\t{path}\n" for url, path in routes.items()))
        (self.dir / "digests.json").write_text(json.dumps({"releases": digests or {}}))

    def find(self, tag, with_odbc="false", github_ref="refs/pull/1/merge", full_role=""):
        (self.dir / "step.sh").write_text(self.steps["find"]["run"])
        output = self.dir / "github_output"
        output.write_text("")
        env = {
            "PATH": f"{self.dir / 'bin'}{os.pathsep}{os.environ['PATH']}",
            "HOME": str(self.dir),
            "RUNNER_TEMP": str(self.dir),
            "GITHUB_OUTPUT": str(output),
            "GITHUB_REPOSITORY": "owner/repo",
            "GITHUB_REF": github_ref,
            "CI_ROLE_FULL": full_role,
            "GH_TOKEN": "a-token",
            "RELEASE": tag,
            "SPICEPOD_FILE": str(self.dir / "spicepod.yaml"),
            "WITH_ODBC": with_odbc,
            "DIGESTS": str(self.dir / "digests.json"),
            "ROUTES": str(self.dir / "routes"),
        }
        proc = subprocess.run(["bash", "-e", str(self.dir / "step.sh")], cwd=self.dir, env=env,
                              capture_output=True, text=True, timeout=60)
        values = dict(line.split("=", 1) for line in output.read_text().splitlines() if "=" in line)
        return proc, values

    def test_a_release_before_sha256sums_runs_only_with_its_pinned_digest(self):
        self.serve("v2.4.3", digests={"v2.4.3": {ASSET: self.digest}})
        proc, found = self.find("v2.4.3")
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertEqual(found["attested"], "false")
        self.assertEqual(found["SPICED_COMMIT"], COMMIT)
        self.assertIn("matches .github/release-digests.json", proc.stdout)

    def test_a_changed_archive_is_refused(self):
        self.serve("v2.4.3", digests={"v2.4.3": {ASSET: "0" * 64}})
        proc, found = self.find("v2.4.3")
        self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
        self.assertIn(f"has SHA-256 {self.digest}, not {'0' * 64}", proc.stdout)
        self.assertNotIn("archive", found)

    def test_a_release_with_neither_is_refused(self):
        # GitHub's own upload digest of the asset no longer counts: anyone who can publish a
        # release controls it.
        self.serve("v2.4.9")
        proc, found = self.find("v2.4.9")
        self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
        self.assertIn("publishes no SHA256SUMS, so its spiced cannot be verified", proc.stdout)
        self.assertEqual(found, {})

    def test_a_release_with_sha256sums_is_checked_against_it_and_its_attestations(self):
        self.serve("v2.5.0", sums=f"{'b' * 64}  spice_linux_x86_64.tar.gz\n{self.digest}  {ASSET}\n")
        proc, found = self.find("v2.5.0")
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertEqual(found["attested"], "true")
        self.assertIn("matches the SHA256SUMS of v2.5.0", proc.stdout)
        # On trunk, or with the full CI roles, only a recorded release runs.
        for github_ref, full_role in (("refs/heads/trunk", ""), ("refs/pull/1/merge", "true")):
            with self.subTest(github_ref=github_ref, full_role=full_role):
                proc, found = self.find("v2.5.0", github_ref=github_ref, full_role=full_role)
                self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
                self.assertIn("runs only a recorded release", proc.stdout)
                self.assertEqual(found, {})
        for name in ("Verify SHA256SUMS's build attestation", "Verify the archive's build attestation"):
            with self.subTest(step=name):
                step = self.steps[name]
                self.assertEqual(step["if"], "steps.find.outputs.attested == 'true'")
                self.assertEqual(step["uses"], "$/.github/actions/verify-attestation")
                self.assertEqual(step["with"]["release"], "true")
                self.assertEqual(step["with"]["source-commit"], "${{ steps.find.outputs.SPICED_COMMIT }}")
        order = list(self.steps)
        self.assertLess(order.index("Verify the archive's build attestation"), order.index("install"))

    def test_a_recorded_release_with_sha256sums_needs_both_to_agree(self):
        self.serve("v2.5.0", sums=f"{self.digest}  {ASSET}\n", digests={"v2.5.0": {ASSET: self.digest}})
        proc, found = self.find("v2.5.0", github_ref="refs/heads/trunk")
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertEqual(found["attested"], "true")
        self.assertIn("matches .github/release-digests.json and the SHA256SUMS of v2.5.0", proc.stdout)
        self.serve("v2.5.0", sums=f"{'b' * 64}  {ASSET}\n", digests={"v2.5.0": {ASSET: self.digest}})
        proc, found = self.find("v2.5.0", github_ref="refs/heads/trunk")
        self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
        self.assertIn("but its SHA256SUMS lists", proc.stdout)

    def test_sha256sums_that_does_not_list_the_archive_is_refused(self):
        self.serve("v2.5.0", sums=f"{self.digest}  spice_linux_x86_64.tar.gz\n")
        proc, _ = self.find("v2.5.0")
        self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
        self.assertIn("does not list", proc.stdout)

    def test_a_destination_installs_spiced_there_without_sudo(self):
        import io
        import tarfile

        archive = self.dir / "spiced_linux_x86_64.tar.gz"
        data = b"#!/bin/sh\necho spiced v2.4.3\n"
        with tarfile.open(archive, "w:gz") as tar:
            info = tarfile.TarInfo("spiced")
            info.size = len(data)
            info.mode = 0o755
            tar.addfile(info, io.BytesIO(data))
        (self.dir / "install.sh").write_text(self.steps["install"]["run"])
        output = self.dir / "github_output"
        output.write_text("")
        destination = self.dir / "baseline" / "spiced"
        proc = subprocess.run(["bash", "-e", str(self.dir / "install.sh")], cwd=self.dir, capture_output=True, text=True,
                              env={"PATH": os.environ["PATH"], "ARCHIVE": str(archive), "SPICED_COMMIT": COMMIT,
                                   "DESTINATION": str(destination), "GITHUB_OUTPUT": str(output)})
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertTrue(destination.is_file())
        self.assertIn(f"path={destination}", output.read_text())
        self.assertIn("spiced v2.4.3", proc.stdout)

    def test_a_value_that_is_not_a_release_tag_is_refused(self):
        for tag in ("release/2.4", "v2.4.3/../../x", "v2.4", "trunk"):
            with self.subTest(tag=tag):
                proc, found = self.find(tag)
                self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
                self.assertIn("is not a release tag", proc.stdout)
                self.assertEqual(found, {})


class ColdStartBaselineTest(unittest.TestCase):
    """The cold-start comparison's baseline is a recorded release, verified before it runs."""

    WORKFLOW = GITHUB_DIR / "workflows" / "testoperator_run_cold_start.yml"

    def choose(self, version):
        steps = yaml.safe_load(self.WORKFLOW.read_text())["jobs"]["cold-start"]["steps"]
        script = next(s for s in steps if s.get("id") == "baseline")["run"]
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "out"
            output.write_text("")
            proc = subprocess.run(["bash", "-e", "-c", script], cwd=GITHUB_DIR.parent, capture_output=True, text=True,
                                  env={"PATH": os.environ["PATH"], "INPUT_BASELINE_VERSION": version,
                                       "GITHUB_OUTPUT": str(output)})
            return proc, output.read_text()

    def test_the_default_is_the_newest_recorded_release_and_an_input_must_be_a_tag(self):
        recorded = json.loads((GITHUB_DIR / "release-digests.json").read_text())["releases"]
        newest = max(recorded, key=lambda tag: tuple(int(p) for p in tag[1:].split(".")))
        proc, output = self.choose("")
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertEqual(output, f"version={newest}\n")
        for version, ok in (("v2.3.4", True), ("latest", False), ("v2.3.4/../../x", False), ("2.3.4", False)):
            with self.subTest(version=version):
                proc, output = self.choose(version)
                self.assertEqual(proc.returncode == 0, ok, proc.stdout + proc.stderr)

    def test_the_baseline_runs_only_through_setup_released_spiced(self):
        job = yaml.safe_load(self.WORKFLOW.read_text())["jobs"]["cold-start"]
        install = next(s for s in job["steps"] if s.get("id") == "baseline-spiced")
        self.assertEqual(install["uses"], "$/.github/actions/setup-released-spiced")
        self.assertEqual(install["with"]["release"], "${{ steps.baseline.outputs.version }}")
        self.assertNotIn("releases/download", yaml.safe_dump(job))


if __name__ == "__main__":
    unittest.main()
