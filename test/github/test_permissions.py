#!/usr/bin/env python3
# Copyright 2024-2026 The Spice.ai OSS Authors
# SPDX-License-Identifier: Apache-2.0
"""Exercise the permission-check CLI against an actual local HTTP server."""

from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import unittest


TOKEN = "test-credential-never-log"
SUCCESS = {
    "data": {
        "repository": {
            "stargazers": {
                "edges": [
                    {"node": {"login": "fixture", "email": "never-log@example.test"}}
                ]
            }
        }
    }
}
FORBIDDEN = {
    "data": {"repository": {"stargazers": None}},
    "errors": [
        {
            "type": "FORBIDDEN",
            "path": ["repository", "stargazers"],
            "message": "Resource not accessible by integration",
        }
    ],
}


class PermissionCheckTests(unittest.TestCase):
    def setUp(self):
        self.responses = deque()
        self.requests = []
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_args):
                pass

            def do_POST(self):
                owner.requests.append(
                    (
                        self.headers.get("Authorization"),
                        json.loads(
                            self.rfile.read(int(self.headers["Content-Length"]))
                        ),
                    )
                )
                status, body, headers = owner.responses.popleft()
                payload = body if isinstance(body, bytes) else json.dumps(body).encode()
                self.send_response(status)
                self.send_header("Content-Length", str(len(payload)))
                for name, value in headers.items():
                    self.send_header(name, value)
                self.end_headers()
                self.wfile.write(payload)

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(
            target=self.server.serve_forever, kwargs={"poll_interval": 0.01}
        )
        self.thread.start()
        self.directory = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()
        self.directory.cleanup()

    def run_check(self, responses, token=TOKEN):
        self.responses.extend(responses)
        result = subprocess.run(
            [
                sys.executable,
                str(Path(__file__).with_name("check_permissions.py")),
                "--endpoint",
                f"http://127.0.0.1:{self.server.server_port}/graphql",
                "--artifacts",
                self.directory.name,
            ],
            env={**os.environ, "GITHUB_TOKEN": token},
            capture_output=True,
            text=True,
            timeout=65,
            check=False,
        )
        artifact = (Path(self.directory.name) / "permissions.json").read_text()
        for sensitive in (
            TOKEN,
            "never-log@example.test",
            token.strip(),
            json.dumps(token.strip())[1:-1],
        ):
            if not sensitive:
                continue
            self.assertNotIn(sensitive, result.stdout + result.stderr + artifact)
        return result, json.loads(artifact)

    def test_full_selection_succeeds(self):
        result, artifact = self.run_check([(200, SUCCESS, {})])
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(artifact[0]["rows"], 1)
        self.assertEqual(self.requests[0][0], f"Bearer {TOKEN}")
        self.assertIn("email", self.requests[0][1]["query"])
        self.assertIn("first: 1", self.requests[0][1]["query"])

    def test_surrounding_credential_whitespace_is_removed(self):
        result, _ = self.run_check([(200, SUCCESS, {})], token=f" {TOKEN}\n")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.requests[0][0], f"Bearer {TOKEN}")

    def test_credential_control_characters_fail_without_disclosure(self):
        result, _ = self.run_check([], token=f"{TOKEN}\ninvalid")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("invalid whitespace", result.stderr)
        self.assertEqual(self.requests, [])

    def test_echoed_credential_is_redacted(self):
        result, artifact = self.run_check(
            [(200, {"errors": [{"type": "FORBIDDEN", "message": TOKEN}]}, {})]
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(artifact[0]["errors"][0]["message"], "[REDACTED]")

    def test_json_escaped_credentials_are_redacted(self):
        for token in ('test"credential', "test\\credential"):
            with self.subTest(token_shape="quote" if '"' in token else "backslash"):
                result, artifact = self.run_check(
                    [(200, {"errors": [{"type": "FORBIDDEN", "message": token}]}, {})],
                    token=token,
                )
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(artifact[0]["errors"][0]["message"], "[REDACTED]")

    def test_graphql_errors_fail_even_with_http_success(self):
        result, artifact = self.run_check([(200, FORBIDDEN, {})])
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("FORBIDDEN", result.stderr)
        self.assertIn("contents: write", result.stderr)
        self.assertEqual(artifact[0]["errors"][0]["path"], ["repository", "stargazers"])
        self.assertEqual(len(self.requests), 1)

    def test_http_authorization_failure_is_not_retried(self):
        result, artifact = self.run_check([(403, {}, {})])
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(artifact[0]["http_status"], 403)
        self.assertEqual(len(self.requests), 1)

    def test_empty_result_fails(self):
        result, _ = self.run_check([(200, {"data": {"repository": None}}, {})])
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(len(self.requests), 1)

    def test_malformed_response_fails_with_artifact(self):
        result, artifact = self.run_check([(200, b"not json", {})])
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Invalid GitHub API response", result.stderr)
        self.assertIn("invalid_response", artifact[0])

    def test_transient_http_failure_recovers(self):
        result, artifact = self.run_check([(503, {}, {}), (200, SUCCESS, {})])
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(artifact), 2)

    def test_persistent_transient_failure_is_bounded(self):
        result, artifact = self.run_check([(503, {}, {})] * 3)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(len(artifact), 3)

    def test_secondary_rate_limit_recovers(self):
        result, _ = self.run_check(
            [(429, {}, {"Retry-After": "1"}), (200, SUCCESS, {})]
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(self.requests), 2)

    def test_long_rate_limit_fails_without_retrying_early(self):
        result, artifact = self.run_check([(429, {}, {"Retry-After": "3600"})])
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("60-second", result.stderr)
        self.assertEqual(len(artifact), 1)

    def test_missing_credential_fails_without_network(self):
        result, _ = self.run_check([], token="")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("GITHUB_TOKEN is missing", result.stderr)
        self.assertEqual(self.requests, [])

    def test_http_date_rate_limit_fails_without_retrying_early(self):
        result, _ = self.run_check(
            [(429, {}, {"Retry-After": "Fri, 01 Jan 2100 00:00:00 GMT"})]
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("60-second", result.stderr)
        self.assertEqual(len(self.requests), 1)

    def test_primary_rate_limit_respects_reset(self):
        result, _ = self.run_check(
            [
                (
                    403,
                    {},
                    {"x-ratelimit-remaining": "0", "x-ratelimit-reset": "4102444800"},
                )
            ]
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("60-second", result.stderr)
        self.assertEqual(len(self.requests), 1)

    def test_graphql_rate_limit_recovers(self):
        result, _ = self.run_check(
            [
                (200, {"errors": [{"type": "RATE_LIMITED"}]}, {"Retry-After": "1"}),
                (200, SUCCESS, {}),
            ]
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(self.requests), 2)

    def test_secondary_403_rate_limit_recovers(self):
        result, _ = self.run_check(
            [
                (403, {}, {"Retry-After": "1", "x-ratelimit-remaining": "100"}),
                (200, SUCCESS, {}),
            ]
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(self.requests), 2)

    def test_headerless_rate_limit_does_not_retry_early(self):
        result, _ = self.run_check([(200, {"errors": [{"type": "RATE_LIMITED"}]}, {})])
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("60-second", result.stderr)
        self.assertEqual(len(self.requests), 1)

    def test_headerless_secondary_403_does_not_retry_early(self):
        result, _ = self.run_check(
            [(403, {"message": "You have exceeded a secondary rate limit."}, {})]
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("60-second", result.stderr)
        self.assertEqual(len(self.requests), 1)

    def test_partial_data_with_errors_fails(self):
        result, _ = self.run_check(
            [(200, {**SUCCESS, "errors": FORBIDDEN["errors"]}, {})]
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("FORBIDDEN", result.stderr)
        self.assertEqual(len(self.requests), 1)


if __name__ == "__main__":
    unittest.main()
