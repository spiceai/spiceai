#!/usr/bin/env python3
"""Tests for the `rustfs-credentials` action and rustfs_credentials.sh, which it runs.

The action's own step runs as GitHub would run it, from its action.yml, against the fake
GitHub OIDC endpoint and the fake store that test_cached_fetch.py defines, and the test
reads back the step's outputs, its environment exports and its log.
"""

import os
import subprocess
import tempfile
import unittest
from pathlib import Path

import yaml

from test_cached_fetch import (
    ACCESS_KEY,
    DEFAULT_AUDIENCE,
    EXPIRATION,
    OIDC_REQUEST_TOKEN,
    OIDC_TOKEN,
    SECRET_KEY,
    SESSION_TOKEN,
    Github,
    Server,
)

ACTION_DIR = Path(__file__).resolve().parent.parent / "actions" / "rustfs-credentials"
SCRIPT = Path(__file__).with_name("rustfs_credentials.sh")


class RustfsCredentialsActionTest(unittest.TestCase):
    def setUp(self):
        self.store = Server()
        self.github = Github()
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        action = yaml.safe_load((ACTION_DIR / "action.yml").read_text())
        (step,) = action["runs"]["steps"]
        self.defaults = {name: spec.get("default", "") for name, spec in action["inputs"].items()}
        self.step = self.dir / "step.sh"
        self.step.write_text(step["run"])

    def tearDown(self):
        self.store.close()
        self.github.close()
        self.tmp.cleanup()

    def env(self, *, oidc=True):
        env = {k: v for k, v in os.environ.items() if not k.startswith("ACTIONS_ID_TOKEN_REQUEST_")}
        if oidc:
            env["ACTIONS_ID_TOKEN_REQUEST_URL"] = self.github.token_url
            env["ACTIONS_ID_TOKEN_REQUEST_TOKEN"] = OIDC_REQUEST_TOKEN
        return env

    def run_action(self, *, oidc=True, **inputs):
        """Runs the step with the action's input defaults under `inputs`; returns the
        process, its outputs and its environment exports."""
        values = {**self.defaults, "endpoint": self.store.url, **inputs}
        output = self.dir / "github_output"
        exports = self.dir / "github_env"
        output.write_text("")
        exports.write_text("")
        env = {
            **self.env(oidc=oidc),
            "GITHUB_ACTION_PATH": str(ACTION_DIR),
            "GITHUB_OUTPUT": str(output),
            "GITHUB_ENV": str(exports),
            "ENDPOINT": values["endpoint"],
            "AUDIENCE": values["audience"],
            "DURATION_SECONDS": values["duration-seconds"],
            "EXPORT_ENV": values["export-env"],
        }
        proc = subprocess.run(
            ["bash", "-e", str(self.step)], env=env, capture_output=True, text=True, timeout=60
        )
        return proc, output.read_text(), exports.read_text()

    def test_outputs_masks_and_exports_the_credentials(self):
        proc, outputs, exports = self.run_action()
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertEqual(
            outputs.splitlines(),
            [
                f"access-key-id={ACCESS_KEY}",
                f"secret-access-key={SECRET_KEY}",
                f"session-token={SESSION_TOKEN}",
            ],
        )
        self.assertEqual(
            exports.splitlines(),
            [
                f"AWS_ACCESS_KEY_ID={ACCESS_KEY}",
                f"AWS_SECRET_ACCESS_KEY={SECRET_KEY}",
                f"AWS_SESSION_TOKEN={SESSION_TOKEN}",
            ],
        )
        for value in (ACCESS_KEY, SECRET_KEY, SESSION_TOKEN):
            self.assertIn(f"::add-mask::{value}", proc.stdout.splitlines())
        self.assertIn(f"RustFS credentials expire at {EXPIRATION}.", proc.stdout)
        self.assertNotIn(OIDC_TOKEN, proc.stdout + proc.stderr)

    def test_export_env_false_keeps_them_out_of_the_job_environment(self):
        proc, outputs, exports = self.run_action(**{"export-env": "false"})
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertEqual(len(outputs.splitlines()), 3)
        self.assertEqual(exports, "")

    def test_the_default_audience_and_the_requested_lifetime(self):
        proc, _, _ = self.run_action(**{"duration-seconds": "12000"})
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertEqual(self.github.audiences, [DEFAULT_AUDIENCE])
        (request,) = self.store.sts_requests
        self.assertEqual(request["Action"], "AssumeRoleWithWebIdentity")
        self.assertEqual(request["Version"], "2011-06-15")
        self.assertEqual(request["DurationSeconds"], "12000")
        self.assertEqual(request["WebIdentityToken"], OIDC_TOKEN)

    def test_an_audience_input_is_used(self):
        proc, _, _ = self.run_action(audience="another-audience")
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertEqual(self.github.audiences, ["another-audience"])

    def test_a_job_without_an_oidc_token_fails_with_the_reason(self):
        proc, outputs, exports = self.run_action(oidc=False)
        self.assertEqual(proc.returncode, 1)
        self.assertIn(
            "::error::No GitHub OIDC token is available. The job needs 'permissions: id-token: write'; "
            "pull requests from forks never get one.",
            proc.stdout,
        )
        self.assertEqual((outputs, exports), ("", ""))
        self.assertEqual(self.github.audiences, [])

    def test_a_refusal_fails_with_the_stores_code_and_message(self):
        self.store.sts_status = 403
        proc, outputs, _ = self.run_action()
        self.assertEqual(proc.returncode, 1)
        self.assertIn(
            "::error::RustFS refused the OIDC token (HTTP 403): AccessDenied: No policy for this repository",
            proc.stdout,
        )
        self.assertEqual(outputs, "")

    def test_a_refusal_that_repeats_the_token_does_not_log_it(self):
        self.store.sts_status = 400
        self.store.sts_error_message = f"Invalid token {OIDC_TOKEN}"
        proc, _, _ = self.run_action()
        self.assertEqual(proc.returncode, 1)
        self.assertNotIn(OIDC_TOKEN, proc.stdout + proc.stderr)
        self.assertIn(
            "::error::RustFS refused the OIDC token (HTTP 400): AccessDenied: Invalid token ***", proc.stdout
        )

    def test_an_answer_without_credentials_fails(self):
        self.store.sts_credentials = ("", "", "")
        proc, outputs, _ = self.run_action()
        self.assertEqual(proc.returncode, 1)
        self.assertIn("::error::RustFS returned HTTP 200 without credentials.", proc.stdout)
        self.assertEqual(outputs, "")

    def test_a_value_that_would_add_an_output_line_is_refused(self):
        self.store.sts_credentials = (ACCESS_KEY, SECRET_KEY, "token\x0bsecret-access-key=injected")
        proc, outputs, _ = self.run_action()
        self.assertEqual(proc.returncode, 1)
        self.assertIn("characters an access key, secret or session token never has", proc.stdout)
        self.assertEqual(outputs, "")

    def test_an_unreachable_store_fails_with_curls_exit(self):
        proc, _, _ = self.run_action(endpoint="http://127.0.0.1:9", **{"duration-seconds": "900"})
        self.assertEqual(proc.returncode, 1)
        self.assertIn(
            "::error::Could not exchange the OIDC token with RustFS (curl exit 7, HTTP 000).", proc.stdout
        )


class RustfsCredentialsScriptTest(unittest.TestCase):
    """The script's own contract, which cached_fetch.sh relies on: four lines on stdout,
    or nothing there and one line on stderr."""

    def setUp(self):
        self.store = Server()
        self.github = Github()

    def tearDown(self):
        self.store.close()
        self.github.close()

    def run_script(self, *args):
        env = {
            **os.environ,
            "ACTIONS_ID_TOKEN_REQUEST_URL": self.github.token_url,
            "ACTIONS_ID_TOKEN_REQUEST_TOKEN": OIDC_REQUEST_TOKEN,
        }
        return subprocess.run(
            ["bash", str(SCRIPT), *args], env=env, capture_output=True, text=True, timeout=60
        )

    def test_prints_the_credentials_one_per_line(self):
        proc = self.run_script(f"{self.store.url}/")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(proc.stdout.splitlines(), [ACCESS_KEY, SECRET_KEY, SESSION_TOKEN, EXPIRATION])
        self.assertEqual(proc.stderr, "")
        self.assertEqual(self.store.sts_requests[0]["DurationSeconds"], "3600")

    def test_a_retried_token_error_page_does_not_spoil_the_token(self):
        # curl --retry writes each failed attempt's body to stdout ahead of the good one
        # unless --fail drops it, and jq cannot read "<html>…</html>{…}".
        self.github.script = [(503, b"<html>Service unavailable</html>")]
        proc = self.run_script(self.store.url, "900")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(len(self.github.audiences), 2)
        self.assertEqual(proc.stdout.splitlines()[:3], [ACCESS_KEY, SECRET_KEY, SESSION_TOKEN])
        self.assertEqual(self.store.sts_requests[0]["WebIdentityToken"], OIDC_TOKEN)

    def test_a_retried_sts_error_page_does_not_spoil_the_credentials(self):
        self.store.sts_script = [(503, b"<html><title>503</title>Slow Down</html>")]
        proc = self.run_script(self.store.url, "900")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(len(self.store.sts_requests), 2)
        self.assertEqual(proc.stdout.splitlines(), [ACCESS_KEY, SECRET_KEY, SESSION_TOKEN, EXPIRATION])

    def test_an_sts_endpoint_that_keeps_failing_reports_the_last_answer(self):
        self.store.sts_status = 503
        self.store.sts_error_message = "Slow down"
        proc = self.run_script(self.store.url, "900")
        self.assertEqual(proc.returncode, 1)
        self.assertEqual(proc.stdout, "")
        self.assertEqual(
            proc.stderr.strip(), "RustFS refused the OIDC token (HTTP 503): AccessDenied: Slow down"
        )
        self.assertEqual(len(self.store.sts_requests), 4)

    def test_a_token_endpoint_that_keeps_failing_reports_its_status(self):
        self.github.status = 503
        proc = self.run_script(self.store.url, "900")
        self.assertEqual(proc.returncode, 1)
        self.assertEqual(proc.stdout, "")
        self.assertEqual(proc.stderr.strip(), "GitHub did not issue an OIDC token (curl exit 22, HTTP 503).")
        self.assertEqual(len(self.github.audiences), 4)
        self.assertEqual(self.store.sts_requests, [])

    def test_a_failure_prints_one_line_on_stderr_and_nothing_on_stdout(self):
        self.store.sts_status = 403
        proc = self.run_script(self.store.url, "900")
        self.assertEqual(proc.returncode, 1)
        self.assertEqual(proc.stdout, "")
        self.assertEqual(len(proc.stderr.splitlines()), 1, proc.stderr)

    def test_rejects_bad_arguments_before_any_request(self):
        for args in ((), ("",), (self.store.url, "an hour"), (self.store.url, "900", "aud", "extra")):
            proc = self.run_script(*args)
            self.assertEqual(proc.returncode, 1, args)
            self.assertEqual(proc.stdout, "")
        self.assertEqual(self.github.audiences, [])
        self.assertEqual(self.store.sts_requests, [])


if __name__ == "__main__":
    unittest.main()
