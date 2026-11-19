#!/usr/bin/env python3
"""Tests for mc_cp.sh, run against a stub `mc` that replays scripted results."""

import os
import stat
import subprocess
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).with_name("mc_cp.sh")

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

SOURCE = "spice-minio/ci-artifacts/integration/run-503/integration_tests.tar.zst"
TARGET = "./integration_tests/integration_tests.tar.zst"
CALL = f"cp {SOURCE} {TARGET}"

UNAVAILABLE = "mc: <ERROR> Unable to prepare URL for copying. 503 Service Unavailable"
RESET = (
    'mc: <ERROR> Unable to prepare URL for copying. Get "https://minio.example/ci-artifacts/": '
    "read tcp 10.0.0.2:51234->10.0.0.9:443: read: connection reset by peer"
)
SLOW_DOWN = "mc: <ERROR> Unable to prepare URL for copying. Please reduce your request rate."
MISSING = "mc: <ERROR> Unable to prepare URL for copying. Object does not exist."
DENIED = "mc: <ERROR> Unable to prepare URL for copying. Access Denied."
EXPIRED = (
    "mc: <ERROR> Unable to prepare URL for copying. "
    "The security token included in the request is expired"
)
# A 404 for a key that contains "503", which must not read as an HTTP 503.
MISSING_503_KEY = f"mc: <ERROR> Unable to validate source `{SOURCE}`: Object does not exist."
COPIED = "integration_tests.tar.zst: 1.21 GiB / 1.21 GiB"


class McCpTest(unittest.TestCase):
    def run_cp(self, results, args=(SOURCE, TARGET), **env):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            stub = tmp_path / "mc"
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
                "MC_CP_DELAY_SECONDS": "0",
                **env,
            }
            proc = subprocess.run(
                ["bash", str(SCRIPT), *args],
                env=proc_env,
                capture_output=True,
                text=True,
                check=False,
            )
            calls = calls_file.read_text().splitlines()
        return proc, calls

    def test_retries_a_store_or_network_error_then_succeeds(self):
        for message in (UNAVAILABLE, RESET, SLOW_DOWN):
            with self.subTest(message=message):
                proc, calls = self.run_cp([(1, message), (0, COPIED)])
                self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
                self.assertEqual(calls, [CALL, CALL])
                self.assertIn("attempt 1 of 4", proc.stdout)

    def test_gives_up_after_the_attempt_budget(self):
        proc, calls = self.run_cp([(1, UNAVAILABLE)] * 3, MC_CP_ATTEMPTS="3")
        self.assertEqual(proc.returncode, 1)
        self.assertEqual(len(calls), 3)
        self.assertIn("failed 3 times with an object store or network error", proc.stdout)

    def test_does_not_retry_other_errors(self):
        for message in (MISSING, DENIED, EXPIRED, MISSING_503_KEY):
            with self.subTest(message=message):
                proc, calls = self.run_cp([(1, message), (0, COPIED)])
                self.assertEqual(proc.returncode, 1)
                self.assertEqual(calls, [CALL])
                self.assertIn(message, proc.stdout)
                self.assertIn("was not retried", proc.stdout)

    def test_keeps_the_mc_exit_code(self):
        proc, _ = self.run_cp([(7, MISSING)])
        self.assertEqual(proc.returncode, 7)

    def test_copies_once_on_success(self):
        proc, calls = self.run_cp([(0, COPIED)])
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertEqual(calls, [CALL])
        self.assertNotIn("::warning::", proc.stdout)

    def test_rejects_a_non_numeric_attempt_budget(self):
        proc, calls = self.run_cp([(0, COPIED)], MC_CP_ATTEMPTS="three")
        self.assertEqual(proc.returncode, 2)
        self.assertEqual(calls, [])

    def test_requires_a_source_and_a_target(self):
        for args in ((), (SOURCE,), (SOURCE, TARGET, "extra")):
            with self.subTest(args=args):
                proc, calls = self.run_cp([], args=args)
                self.assertEqual(proc.returncode, 2)
                self.assertEqual(calls, [])


if __name__ == "__main__":
    unittest.main()
