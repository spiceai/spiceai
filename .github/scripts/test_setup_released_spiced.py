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

    def find(self, tag, with_odbc="false"):
        (self.dir / "step.sh").write_text(self.steps["find"]["run"])
        output = self.dir / "github_output"
        output.write_text("")
        env = {
            "PATH": f"{self.dir / 'bin'}{os.pathsep}{os.environ['PATH']}",
            "HOME": str(self.dir),
            "RUNNER_TEMP": str(self.dir),
            "GITHUB_OUTPUT": str(output),
            "GITHUB_REPOSITORY": "owner/repo",
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
        self.assertIn("cannot be verified; not running it", proc.stdout)
        self.assertEqual(found, {})

    def test_a_release_with_sha256sums_is_checked_against_it_and_its_attestations(self):
        self.serve("v2.5.0", sums=f"{'b' * 64}  spice_linux_x86_64.tar.gz\n{self.digest}  {ASSET}\n")
        proc, found = self.find("v2.5.0")
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertEqual(found["attested"], "true")
        self.assertIn("matches the SHA256SUMS of v2.5.0", proc.stdout)
        for name in ("Verify SHA256SUMS's build attestation", "Verify the archive's build attestation"):
            with self.subTest(step=name):
                step = self.steps[name]
                self.assertEqual(step["if"], "steps.find.outputs.attested == 'true'")
                self.assertEqual(step["uses"], "$/.github/actions/verify-attestation")
                self.assertEqual(step["with"]["release"], "true")
                self.assertEqual(step["with"]["source-commit"], "${{ steps.find.outputs.SPICED_COMMIT }}")
        order = list(self.steps)
        self.assertLess(order.index("Verify the archive's build attestation"), order.index("Install the released spiced"))

    def test_sha256sums_that_does_not_list_the_archive_is_refused(self):
        self.serve("v2.5.0", sums=f"{self.digest}  spice_linux_x86_64.tar.gz\n")
        proc, _ = self.find("v2.5.0")
        self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
        self.assertIn("cannot be verified", proc.stdout)

    def test_a_value_that_is_not_a_release_tag_is_refused(self):
        for tag in ("release/2.4", "v2.4.3/../../x", "v2.4", "trunk"):
            with self.subTest(tag=tag):
                proc, found = self.find(tag)
                self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
                self.assertIn("is not a release tag", proc.stdout)
                self.assertEqual(found, {})


if __name__ == "__main__":
    unittest.main()
