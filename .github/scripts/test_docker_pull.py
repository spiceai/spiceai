#!/usr/bin/env python3
"""Tests for docker_pull.sh, run against a stub `docker` that replays scripted results."""

import json
import os
import stat
import subprocess
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).with_name("docker_pull.sh")

# Each line of the results file is "<exit code>\t<message>", consumed one per call.
STUB = """#!/usr/bin/env bash
set -euo pipefail
echo "$*" >> "$STUB_CALLS"
line="$(head -n 1 "$STUB_RESULTS")"
tail -n +2 "$STUB_RESULTS" > "$STUB_RESULTS.next"
mv "$STUB_RESULTS.next" "$STUB_RESULTS"
code="${line%%$'\\t'*}"
message="${line#*$'\\t'}"
if [ "$code" -eq 0 ]; then echo "$message"; else echo "$message" >&2; fi
exit "$code"
"""

BAD_GATEWAY = (
    'Error response from daemon: unknown: failed to resolve reference "docker.io/library/mongo:9.0": '
    "unexpected status from HEAD request to https://registry-1.docker.io/v2/library/mongo/manifests/9.0: "
    "502 Bad Gateway"
)
UNKNOWN_TAG = "Error response from daemon: manifest for postgres:99 not found: manifest unknown: manifest unknown"
DENIED = (
    "Error response from daemon: pull access denied for private/image, repository does not exist "
    "or may require 'docker login': denied: requested access to the resource is denied"
)
UNKNOWN_DIGEST = (
    "Error response from daemon: manifest for postgres@sha256:500a1b2c not found: manifest unknown"
)
TLS_TIMEOUT = 'Error response from daemon: Get "https://registry-1.docker.io/v2/": net/http: TLS handshake timeout'
PULLED = "Status: Downloaded newer image"


class DockerPullTest(unittest.TestCase):
    def run_pull(self, results, images, **env):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            stub = tmp_path / "docker"
            stub.write_text(STUB)
            stub.chmod(stub.stat().st_mode | stat.S_IEXEC)
            results_file = tmp_path / "results"
            results_file.write_text("".join(f"{code}\t{message}\n" for code, message in results))
            calls_file = tmp_path / "calls"
            calls_file.touch()
            proc_env = {
                **os.environ,
                "PATH": f"{tmp}{os.pathsep}{os.environ['PATH']}",
                "STUB_RESULTS": str(results_file),
                "STUB_CALLS": str(calls_file),
                "DOCKER_PULL_DELAY_SECONDS": "0",
                **env,
            }
            proc = subprocess.run(
                ["bash", str(SCRIPT), *images],
                env=proc_env,
                capture_output=True,
                text=True,
                check=False,
            )
            calls = calls_file.read_text().splitlines()
        return proc, calls

    def test_retries_a_registry_or_network_error_then_succeeds(self):
        for message in (BAD_GATEWAY, TLS_TIMEOUT):
            with self.subTest(message=message):
                proc, calls = self.run_pull([(1, message), (0, PULLED)], ["mongo:9.0"])
                self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
                self.assertEqual(calls, ["pull mongo:9.0", "pull mongo:9.0"])
                self.assertIn("attempt 1 of 4", proc.stdout)

    def test_gives_up_after_the_attempt_budget(self):
        proc, calls = self.run_pull(
            [(1, BAD_GATEWAY)] * 3, ["mongo:9.0"], DOCKER_PULL_ATTEMPTS="3"
        )
        self.assertEqual(proc.returncode, 1)
        self.assertEqual(len(calls), 3)
        self.assertIn("failed 3 times with a registry or network error", proc.stdout)

    def test_does_not_retry_other_errors(self):
        # An unknown tag, a denied pull, and a digest that starts with "500", which
        # must not read as an HTTP 500.
        for message, image in (
            (UNKNOWN_TAG, "postgres:99"),
            (DENIED, "private/image"),
            (UNKNOWN_DIGEST, "postgres@sha256:500a1b2c"),
        ):
            with self.subTest(image=image):
                proc, calls = self.run_pull([(1, message), (0, PULLED)], [image])
                self.assertEqual(proc.returncode, 1)
                self.assertEqual(calls, [f"pull {image}"])
                self.assertIn(message, proc.stdout)
                self.assertIn("was not retried", proc.stdout)

    def test_keeps_the_docker_exit_code(self):
        proc, _ = self.run_pull([(125, UNKNOWN_TAG)], ["postgres:99"])
        self.assertEqual(proc.returncode, 125)

    def test_stops_at_the_first_real_failure(self):
        proc, calls = self.run_pull(
            [(0, PULLED), (1, UNKNOWN_TAG), (0, PULLED)], ["mysql:9.7", "postgres:99", "mongo:9.0"]
        )
        self.assertEqual(proc.returncode, 1)
        self.assertEqual(calls, ["pull mysql:9.7", "pull postgres:99"])

    def test_pulls_every_image(self):
        proc, calls = self.run_pull([(0, PULLED)] * 2, ["mysql:9.7", "mongo:9.0"])
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertEqual(calls, ["pull mysql:9.7", "pull mongo:9.0"])

    def test_rejects_a_non_numeric_attempt_budget(self):
        proc, calls = self.run_pull([(0, PULLED)], ["mysql:9.7"], DOCKER_PULL_ATTEMPTS="three")
        self.assertEqual(proc.returncode, 2)
        self.assertEqual(calls, [])

    def test_requires_an_image(self):
        proc, calls = self.run_pull([], [])
        self.assertEqual(proc.returncode, 2)
        self.assertEqual(calls, [])


class IntegrationPrePullTest(unittest.TestCase):
    """The integration workflows pre-pull each test image by the digest the tests run it by."""

    ROOT = Path(__file__).resolve().parents[2]

    def step(self, workflow, job, name):
        import yaml

        document = yaml.safe_load((self.ROOT / ".github" / "workflows" / workflow).read_text())
        (step,) = [s for s in document["jobs"][job]["steps"] if s.get("name") == name]
        return step

    def run_step(self, step, env=None, images=None):
        with tempfile.TemporaryDirectory() as tmp:
            tree = Path(tmp)
            (tree / ".github" / "scripts").mkdir(parents=True)
            (tree / "test").mkdir()
            calls = tree / "calls"
            stub = tree / ".github" / "scripts" / "docker_pull.sh"
            stub.write_text(f'#!/usr/bin/env bash\nprintf "%s\\n" "$@" >> "{calls}"\n')
            for name in ("source_versions.json", "container_images.json"):
                (tree / "test" / name).write_text((self.ROOT / "test" / name).read_text())
            if images is not None:
                (tree / "test" / "container_images.json").write_text(json.dumps(images))
            script = step["run"]
            self.assertNotIn("${{", script)
            result = subprocess.run(
                [self.bash(), "-e", "-c", script], cwd=tree, env={**os.environ, **(env or {})}, capture_output=True, text=True
            )
            return result, calls.read_text().splitlines() if calls.exists() else []

    def bash(self):
        """A bash with `mapfile` (4.0 or later), as the Linux runners have."""
        for candidate in ("bash", "/opt/homebrew/bin/bash", "/usr/local/bin/bash"):
            try:
                if subprocess.run([candidate, "-c", "type mapfile"], capture_output=True).returncode == 0:
                    return candidate
            except FileNotFoundError:
                continue
        self.skipTest("no bash 4 or later here")

    def pinned(self):
        return json.loads((self.ROOT / "test" / "container_images.json").read_text())

    def test_the_default_lines_and_oracle_are_pulled_by_their_pinned_digests(self):
        step = self.step("integration_aws.yml", "test-partitions", "Pull database images")
        result, pulled = self.run_step(step)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        images = self.pinned()["images"]
        self.assertTrue(pulled)
        self.assertIn("docker.io/gvenzl/oracle-free:23.26.4@" + images["docker.io/gvenzl/oracle-free:23.26.4"], pulled)
        for ref in pulled:
            tag, _, digest = ref.partition("@")
            self.assertEqual(images.get(tag), digest, ref)

    def test_an_image_the_map_does_not_pin_fails_before_any_pull(self):
        step = self.step("integration_aws.yml", "test-partitions", "Pull database images")
        images = self.pinned()
        images["images"].pop("docker.io/gvenzl/oracle-free:23.26.4")
        result, pulled = self.run_step(step, images=images)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(pulled, [])
        self.assertIn("pins no digest for docker.io/gvenzl/oracle-free:23.26.4", result.stdout)

    def test_an_older_line_is_pulled_by_its_pinned_digest(self):
        step = self.step("integration.yml", "source-versions", "Pull ${{ matrix.source }} ${{ matrix.version }}")
        versions = json.loads((self.ROOT / "test" / "source_versions.json").read_text())
        images = self.pinned()["images"]
        for source, entry in versions.items():
            if source == "description":
                continue
            for version in entry["versions"]:
                with self.subTest(source=source, version=version["version"]):
                    result, pulled = self.run_step(step, {"SOURCE": source, "VERSION": version["version"]})
                    self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                    tag = f"{entry['image']}:{version['version']}"
                    self.assertEqual(pulled, [f"{tag}@{images[tag]}"])


if __name__ == "__main__":
    unittest.main()
