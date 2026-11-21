#!/usr/bin/env python3
"""Tests for cached_fetch.sh, run with real curl against local HTTP servers.

One server plays the CI cache and one the source. Each answers a scripted sequence of
responses per path (a status, the wrong bytes, or a dropped connection) and counts the
requests, so a test asserts how many times each tier was asked as well as what landed.
"""

import hashlib
import http.server
import os
import socket
import subprocess
import tempfile
import threading
import unittest
from pathlib import Path

SCRIPT = Path(__file__).with_name("cached_fetch.sh")
CONTENT = b"the pinned file\n"
SHA = hashlib.sha256(CONTENT).hexdigest()
WRONG = b"not the pinned file\n"
DROP = "drop"


class Server:
    """Serves `script[path]`, a list of responses consumed in order; the last repeats."""

    def __init__(self):
        self.script = {}
        self.requests = []
        server = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):  # noqa: N802 (http.server's naming)
                server.requests.append(self.path)
                responses = server.script.get(self.path, [404])
                response = responses.pop(0) if len(responses) > 1 else responses[0]
                if response == DROP:
                    # Close without a reply: curl reports exit 52, an empty reply.
                    self.close_connection = True
                    self.connection.shutdown(2)
                    return
                status, body = (200, response) if isinstance(response, bytes) else (response, b"")
                self.send_response(status)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):
                pass

        self.httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.httpd.server_port}"
        threading.Thread(target=self.httpd.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True).start()

    def count(self, path):
        return self.requests.count(path)

    def close(self):
        self.httpd.shutdown()
        self.httpd.server_close()


CACHE_PATH = f"/ci-cache/sha256/{SHA}"
SOURCE_PATH = "/releases/tool.tar.gz"
MIRROR_PATH = "/mirror/tool.tar.gz"


class CachedFetchTest(unittest.TestCase):
    def setUp(self):
        self.cache = Server()
        self.source = Server()
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        self.dest = self.dir / "out" / "tool.tar.gz"

    def tearDown(self):
        self.cache.close()
        self.source.close()
        self.tmp.cleanup()

    def run_fetch(self, *, cache=True, cache_url=None, cache_dir=None, sources=None, sha=SHA):
        env = {
            **os.environ,
            "CI_CACHE_DELAY_SECONDS": "0",
            "CI_SOURCE_DELAY_SECONDS": "0",
        }
        env.pop("CI_CACHE_URL", None)
        env.pop("CI_CACHE_DIR", None)
        if cache:
            env["CI_CACHE_URL"] = cache_url or f"{self.cache.url}/ci-cache"
        if cache_dir is not None:
            env["CI_CACHE_DIR"] = str(cache_dir)
        urls = sources or [f"{self.source.url}{SOURCE_PATH}"]
        return subprocess.run(
            ["bash", str(SCRIPT), sha, str(self.dest), *urls],
            env=env,
            capture_output=True,
            text=True,
            timeout=60,
        )

    def assert_fetched(self, proc):
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertEqual(self.dest.read_bytes(), CONTENT)

    def assert_failed(self, proc):
        self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
        self.assertFalse(self.dest.exists())
        self.assertEqual(list(self.dest.parent.glob(".cached_fetch.*")), [])

    # --- the cache ---------------------------------------------------------------------

    def test_cache_hit_never_asks_the_source(self):
        self.cache.script[CACHE_PATH] = [CONTENT]
        proc = self.run_fetch()
        self.assert_fetched(proc)
        self.assertEqual(self.cache.count(CACHE_PATH), 1)
        self.assertEqual(self.source.requests, [])
        self.assertIn("verified from the CI cache", proc.stdout)

    def test_cache_miss_is_not_retried(self):
        self.cache.script[CACHE_PATH] = [404]
        self.source.script[SOURCE_PATH] = [CONTENT]
        self.assert_fetched(self.run_fetch())
        self.assertEqual(self.cache.count(CACHE_PATH), 1)
        self.assertEqual(self.source.count(SOURCE_PATH), 1)

    def test_cache_server_error_is_retried_once(self):
        self.cache.script[CACHE_PATH] = [503, CONTENT]
        self.assert_fetched(self.run_fetch())
        self.assertEqual(self.cache.count(CACHE_PATH), 2)
        self.assertEqual(self.source.requests, [])

    def test_cache_dropped_connection_is_retried_once(self):
        self.cache.script[CACHE_PATH] = [DROP, CONTENT]
        self.assert_fetched(self.run_fetch())
        self.assertEqual(self.cache.count(CACHE_PATH), 2)
        self.assertEqual(self.source.requests, [])

    def test_two_cache_failures_fall_back_to_the_source(self):
        self.cache.script[CACHE_PATH] = [503, 503, CONTENT]
        self.source.script[SOURCE_PATH] = [CONTENT]
        proc = self.run_fetch()
        self.assert_fetched(proc)
        self.assertEqual(self.cache.count(CACHE_PATH), 2)
        self.assertEqual(self.source.count(SOURCE_PATH), 1)
        self.assertIn("attempt 2 of 2", proc.stdout)

    def test_cache_refusal_is_not_retried(self):
        self.cache.script[CACHE_PATH] = [403]
        self.source.script[SOURCE_PATH] = [CONTENT]
        self.assert_fetched(self.run_fetch())
        self.assertEqual(self.cache.count(CACHE_PATH), 1)

    def test_wrong_bytes_in_the_cache_fall_back_to_the_source(self):
        self.cache.script[CACHE_PATH] = [WRONG]
        self.source.script[SOURCE_PATH] = [CONTENT]
        proc = self.run_fetch()
        self.assert_fetched(proc)
        self.assertEqual(self.cache.count(CACHE_PATH), 1)
        self.assertEqual(self.source.count(SOURCE_PATH), 1)
        self.assertIn("does not match its pinned SHA-256", proc.stdout)

    def test_no_cache_url_goes_straight_to_the_source(self):
        self.source.script[SOURCE_PATH] = [CONTENT]
        self.assert_fetched(self.run_fetch(cache=False))
        self.assertEqual(self.cache.requests, [])

    def test_unreachable_cache_falls_back_to_the_source(self):
        # A port nothing listens on refuses the connection (curl exit 7), a transient
        # failure, so the cache gets its one retry before the source is asked.
        probe = socket.socket()
        probe.bind(("127.0.0.1", 0))
        closed_port = probe.getsockname()[1]
        probe.close()
        self.source.script[SOURCE_PATH] = [CONTENT]
        proc = self.run_fetch(cache_url=f"http://127.0.0.1:{closed_port}/ci-cache")
        self.assert_fetched(proc)
        self.assertIn("curl exit 7", proc.stdout)
        self.assertIn("attempt 2 of 2", proc.stdout)
        self.assertEqual(self.source.count(SOURCE_PATH), 1)

    # --- the source --------------------------------------------------------------------

    def test_source_server_errors_get_three_retries(self):
        self.source.script[SOURCE_PATH] = [503, 502, 429, CONTENT]
        self.assert_fetched(self.run_fetch(cache=False))
        self.assertEqual(self.source.count(SOURCE_PATH), 4)

    def test_source_fails_after_four_server_errors(self):
        self.source.script[SOURCE_PATH] = [503, 503, 503, 503, CONTENT]
        proc = self.run_fetch(cache=False)
        self.assert_failed(proc)
        self.assertEqual(self.source.count(SOURCE_PATH), 4)
        self.assertIn("after 4 attempts", proc.stdout)

    def test_source_not_found_is_not_retried(self):
        self.source.script[SOURCE_PATH] = [404]
        proc = self.run_fetch(cache=False)
        self.assert_failed(proc)
        self.assertEqual(self.source.count(SOURCE_PATH), 1)

    def test_source_with_the_wrong_bytes_fails_without_writing_dest(self):
        self.source.script[SOURCE_PATH] = [WRONG]
        proc = self.run_fetch(cache=False)
        self.assert_failed(proc)
        self.assertEqual(self.source.count(SOURCE_PATH), 1)
        self.assertIn("does not match the pinned SHA-256", proc.stdout)

    def test_a_second_source_url_serves_when_the_first_has_no_file(self):
        self.source.script[SOURCE_PATH] = [404]
        self.source.script[MIRROR_PATH] = [CONTENT]
        sources = [f"{self.source.url}{SOURCE_PATH}", f"{self.source.url}{MIRROR_PATH}"]
        self.assert_fetched(self.run_fetch(cache=False, sources=sources))
        self.assertEqual(self.source.count(SOURCE_PATH), 1)
        self.assertEqual(self.source.count(MIRROR_PATH), 1)

    # --- the host's cache directory ----------------------------------------------------

    def test_cache_dir_hit_makes_no_request(self):
        cache_dir = self.dir / "host-cache"
        (cache_dir / "sha256").mkdir(parents=True)
        (cache_dir / "sha256" / SHA).write_bytes(CONTENT)
        self.assert_fetched(self.run_fetch(cache_dir=cache_dir))
        self.assertEqual(self.cache.requests, [])
        self.assertEqual(self.source.requests, [])

    def test_a_download_is_kept_in_the_cache_dir(self):
        cache_dir = self.dir / "host-cache"
        self.cache.script[CACHE_PATH] = [404]
        self.source.script[SOURCE_PATH] = [CONTENT]
        self.assert_fetched(self.run_fetch(cache_dir=cache_dir))
        self.assertEqual((cache_dir / "sha256" / SHA).read_bytes(), CONTENT)
        self.assertEqual(list((cache_dir / "sha256").glob(".part.*")), [])

    def test_a_wrong_cache_dir_entry_is_replaced(self):
        cache_dir = self.dir / "host-cache"
        (cache_dir / "sha256").mkdir(parents=True)
        (cache_dir / "sha256" / SHA).write_bytes(WRONG)
        self.cache.script[CACHE_PATH] = [CONTENT]
        proc = self.run_fetch(cache_dir=cache_dir)
        self.assert_fetched(proc)
        self.assertEqual((cache_dir / "sha256" / SHA).read_bytes(), CONTENT)
        self.assertIn("does not match its pinned SHA-256", proc.stdout)

    # --- arguments ---------------------------------------------------------------------

    def test_rejects_a_malformed_sha(self):
        proc = self.run_fetch(sha="ABC")
        self.assertEqual(proc.returncode, 2)
        self.assertEqual(self.cache.requests + self.source.requests, [])


if __name__ == "__main__":
    unittest.main()
