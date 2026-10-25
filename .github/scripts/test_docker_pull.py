#!/usr/bin/env python3
"""Tests for docker_pull.sh, run against a stub `docker` that replays scripted results."""

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


if __name__ == "__main__":
    unittest.main()
