#!/usr/bin/env python3
"""Tests for fetch_pinned.sh, run with real curl against a local HTTP server.

Each test lays out a temporary tree like the repository's, with fetch_pinned.sh and
cached_fetch.sh under .github/scripts and a pins.json written for the test under
.github/ci-cache, and runs the script there as CI does. The server counts the requests,
so a test asserts that a missing pin fails before anything is downloaded.
"""

import hashlib
import http.server
import json
import os
import shutil
import subprocess
import tempfile
import threading
import unittest
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parent
CONTENT = b"the pinned file\n"
SHA = hashlib.sha256(CONTENT).hexdigest()


class Server:
    """Serves `files[path]` and records every path asked for; anything else is a 404."""

    def __init__(self):
        self.files = {}
        self.requests = []
        server = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):  # noqa: N802 (http.server's naming)
                server.requests.append(self.path)
                body = server.files.get(self.path)
                self.send_response(200 if body is not None else 404)
                self.send_header("Content-Length", str(len(body or b"")))
                self.end_headers()
                self.wfile.write(body or b"")

            def log_message(self, *args):
                pass

        self.httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.httpd.server_port}"
        threading.Thread(target=self.httpd.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True).start()

    def close(self):
        self.httpd.shutdown()
        self.httpd.server_close()


class FetchPinnedTest(unittest.TestCase):
    def setUp(self):
        self.server = Server()
        self.addCleanup(self.server.close)
        self.tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.tmp)
        scripts = self.tmp / ".github" / "scripts"
        scripts.mkdir(parents=True)
        for name in ("fetch_pinned.sh", "cached_fetch.sh", "rustfs_credentials.sh"):
            shutil.copy(SCRIPTS / name, scripts / name)
        (self.tmp / ".github" / "ci-cache").mkdir()
        self.dest = self.tmp / "out" / "tool"
        self.write_pins(
            {
                "description": "test pins",
                "tool": {
                    "version": "1.2.3",
                    "url": self.server.url + "/releases/{version}/tool-{target}.tar.gz",
                    "sha256": {"linux-amd64": SHA, "linux-arm64": "1" * 64},
                },
                "keyring": {"url": self.server.url + "/keyring.gpg", "sha256": SHA},
            }
        )

    def write_pins(self, pins):
        (self.tmp / ".github" / "ci-cache" / "pins.json").write_text(json.dumps(pins))

    def run_fetch(self, *args):
        env = {k: v for k, v in os.environ.items() if not k.startswith(("CI_", "ACTIONS_ID_TOKEN"))}
        env["CI_SOURCE_DELAY_SECONDS"] = "0"
        return subprocess.run(
            ["bash", str(self.tmp / ".github" / "scripts" / "fetch_pinned.sh"), *args],
            env=env,
            capture_output=True,
            text=True,
            timeout=60,
        )

    def test_a_target_is_fetched_from_its_release_url(self):
        self.server.files["/releases/1.2.3/tool-linux-amd64.tar.gz"] = CONTENT
        proc = self.run_fetch("tool", str(self.dest), "linux-amd64")
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertEqual(self.dest.read_bytes(), CONTENT)
        self.assertIn(f"Fetching tool 1.2.3 for linux-amd64, pinned at sha256 {SHA}.", proc.stdout)

    def test_a_single_file_pin_takes_no_target(self):
        self.server.files["/keyring.gpg"] = CONTENT
        proc = self.run_fetch("keyring", str(self.dest))
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertEqual(self.dest.read_bytes(), CONTENT)

    def test_bytes_that_do_not_match_the_pin_never_reach_dest(self):
        self.server.files["/releases/1.2.3/tool-linux-arm64.tar.gz"] = CONTENT
        proc = self.run_fetch("tool", str(self.dest), "linux-arm64")
        self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
        self.assertIn("does not match the pinned SHA-256", proc.stdout)
        self.assertFalse(self.dest.exists())

    def test_a_target_without_a_pin_fails_before_any_download(self):
        proc = self.run_fetch("tool", str(self.dest), "darwin-arm64")
        self.assertEqual(proc.returncode, 2, proc.stdout + proc.stderr)
        self.assertIn("pins no tool 1.2.3 for darwin-arm64; it pins linux-amd64, linux-arm64.", proc.stderr)
        self.assertEqual(self.server.requests, [])
        self.assertFalse(self.dest.exists())

    def test_a_name_without_a_pin_fails_before_any_download(self):
        for name in ("other", "description"):
            with self.subTest(name=name):
                proc = self.run_fetch(name, str(self.dest), "linux-amd64")
                self.assertEqual(proc.returncode, 2, proc.stdout + proc.stderr)
                self.assertIn(f"has no pin named {name}.", proc.stderr)
        self.assertEqual(self.server.requests, [])

    def test_a_per_target_pin_needs_a_target_and_a_single_file_pin_refuses_one(self):
        proc = self.run_fetch("tool", str(self.dest))
        self.assertEqual(proc.returncode, 2, proc.stdout + proc.stderr)
        self.assertIn("so name the target (linux-amd64, linux-arm64)", proc.stderr)
        proc = self.run_fetch("keyring", str(self.dest), "linux-amd64")
        self.assertEqual(proc.returncode, 2, proc.stdout + proc.stderr)
        self.assertIn("pins one keyring file", proc.stderr)
        self.assertEqual(self.server.requests, [])

    def test_a_malformed_sha256_fails_before_any_download(self):
        self.write_pins({"keyring": {"url": self.server.url + "/keyring.gpg", "sha256": SHA.upper()}})
        proc = self.run_fetch("keyring", str(self.dest))
        self.assertEqual(proc.returncode, 2, proc.stdout + proc.stderr)
        self.assertIn("has no lowercase hex SHA-256", proc.stderr)
        self.assertEqual(self.server.requests, [])


if __name__ == "__main__":
    unittest.main()
