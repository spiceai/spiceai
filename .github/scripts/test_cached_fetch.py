#!/usr/bin/env python3
"""Tests for cached_fetch.sh, run with real curl against local HTTP servers.

One server plays the CI cache and one the source. Each answers a scripted sequence of
responses per path (a status, the wrong bytes, or a dropped connection) and counts the
requests, so a test asserts how many times each tier was asked as well as what landed.

The CI cache server behaves as the CI object store does: anyone may read, but a write
needs an AWS SigV4 signature by the temporary credentials its STS endpoint issues for a
GitHub OIDC token, which a third server issues as GitHub's token endpoint does.
"""

import hashlib
import hmac
import http.server
import json
import os
import re
import shutil
import socket
import subprocess
import tempfile
import threading
import unittest
import urllib.parse
from pathlib import Path

SCRIPT = Path(__file__).with_name("cached_fetch.sh")
CONTENT = b"the pinned file\n"
SHA = hashlib.sha256(CONTENT).hexdigest()
WRONG = b"not the pinned file\n"
DROP = "drop"

# What the fake GitHub and the fake store issue. The secret holds the base64 characters
# an AWS secret can, so the curl configuration that carries it is exercised with them.
OIDC_REQUEST_TOKEN = "github-request-token"
OIDC_TOKEN = "eyJhbGciOiJSUzI1NiJ9.eyJyZXBvc2l0b3J5X2lkIjoiMSJ9.c2lnbmF0dXJl"
DEFAULT_AUDIENCE = "rustfs-dev-teraswitch"
ACCESS_KEY = "STSACCESSKEY0EXAMPLE"
SECRET_KEY = "sts/Secret+Key=example_0123456789abcdef"
SESSION_TOKEN = "eyJhbGciOiJIUzUxMiJ9.eyJwYXJlbnQiOiJjaSJ9.session-token_part"
EXPIRATION = "2026-11-23T21:00:00.000Z"


class Unsized(bytes):
    """A body sent without a Content-Length, ended by closing the connection."""


def sigv4_problem(method, raw_path, headers, body):
    """Why the request is not signed by the issued credentials, or None when it is.

    Recomputes the AWS SigV4 signature as the store does, over the headers the request
    says it signed, and requires the session token and the body's hash among them.
    """
    match = re.fullmatch(
        r"AWS4-HMAC-SHA256 Credential=([^/]+)/(\d{8})/([^/]+)/s3/aws4_request, ?"
        r"SignedHeaders=([a-z0-9;-]+), ?Signature=([0-9a-f]{64})",
        headers.get("Authorization", ""),
    )
    if not match:
        return "not signed"
    access_key, date, region, signed_headers, signature = match.groups()
    signed = signed_headers.split(";")
    if access_key != ACCESS_KEY:
        return f"signed by {access_key}"
    for header in ("host", "x-amz-date", "x-amz-content-sha256", "x-amz-security-token"):
        if header not in signed:
            return f"{header} not signed"
    if headers.get("x-amz-security-token") != SESSION_TOKEN:
        return "wrong session token"
    payload_hash = headers["x-amz-content-sha256"]
    if hashlib.sha256(body).hexdigest() != payload_hash:
        return "the body does not match its signed hash"
    path, _, query = raw_path.partition("?")
    canonical_headers = "".join(f"{h}:{' '.join(headers[h].split())}\n" for h in signed)
    canonical = "\n".join([method, path, query, canonical_headers, signed_headers, payload_hash])
    scope = f"{date}/{region}/s3/aws4_request"
    to_sign = "\n".join(
        ["AWS4-HMAC-SHA256", headers["x-amz-date"], scope, hashlib.sha256(canonical.encode()).hexdigest()]
    )
    key = f"AWS4{SECRET_KEY}".encode()
    for part in (date, region, "s3", "aws4_request"):
        key = hmac.new(key, part.encode(), hashlib.sha256).digest()
    expected = hmac.new(key, to_sign.encode(), hashlib.sha256).hexdigest()
    return None if hmac.compare_digest(expected, signature) else "signature mismatch"


def sts_response(credentials):
    access_key, secret_key, session_token = credentials
    return (
        '<AssumeRoleWithWebIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">'
        "<AssumeRoleWithWebIdentityResult><Credentials>"
        f"<AccessKeyId>{access_key}</AccessKeyId><SecretAccessKey>{secret_key}</SecretAccessKey>"
        f"<SessionToken>{session_token}</SessionToken><Expiration>{EXPIRATION}</Expiration>"
        "</Credentials></AssumeRoleWithWebIdentityResult></AssumeRoleWithWebIdentityResponse>"
    ).encode()


class Server:
    """Serves `script[path]`, a list of responses consumed in order; the last repeats.

    A PUT signed by the issued credentials stores its body (answering `put_status`), and a
    GET of a path with no script serves what was stored there, as the bucket would. Any
    other PUT is refused with 403 and noted in `refused_puts`. A POST is an STS
    AssumeRoleWithWebIdentity call, answered with `sts_credentials` for the OIDC token
    GitHub issues, or with `sts_status` and an error.
    """

    def __init__(self):
        self.script = {}
        self.requests = []
        self.stored = {}
        self.puts = []
        self.refused_puts = []
        self.put_status = 200
        self.sts_requests = []
        self.sts_status = 200
        self.sts_error_message = "No policy for this repository"
        self.sts_script = []
        self.sts_credentials = (ACCESS_KEY, SECRET_KEY, SESSION_TOKEN)
        server = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_PUT(self):  # noqa: N802 (http.server's naming)
                body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
                problem = sigv4_problem("PUT", self.path, self.headers, body)
                if problem:
                    server.refused_puts.append((self.path, problem))
                    status = 403
                else:
                    server.puts.append(self.path)
                    status = server.put_status
                    if status == 200:
                        server.stored[self.path] = body
                self.send_response(status)
                self.send_header("Content-Length", "0")
                self.end_headers()

            def do_POST(self):  # noqa: N802 (http.server's naming)
                body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
                form = {k: v[0] for k, v in urllib.parse.parse_qs(body.decode()).items()}
                server.sts_requests.append(form)
                if server.sts_script:
                    status, reply = server.sts_script.pop(0)
                elif server.sts_status != 200:
                    status, reply = server.sts_status, (
                        "<ErrorResponse><Error><Code>AccessDenied</Code>"
                        f"<Message>{server.sts_error_message}</Message></Error></ErrorResponse>"
                    ).encode()
                elif (
                    form.get("Action") != "AssumeRoleWithWebIdentity"
                    or form.get("WebIdentityToken") != OIDC_TOKEN
                ):
                    status, reply = 400, (
                        b"<ErrorResponse><Error><Code>InvalidIdentityToken</Code></Error></ErrorResponse>"
                    )
                else:
                    status, reply = 200, sts_response(server.sts_credentials)
                self.send_response(status)
                self.send_header("Content-Length", str(len(reply)))
                self.end_headers()
                self.wfile.write(reply)

            def do_GET(self):  # noqa: N802 (http.server's naming)
                server.requests.append(self.path)
                default = [server.stored[self.path]] if self.path in server.stored else [404]
                responses = server.script.get(self.path, default)
                response = responses.pop(0) if len(responses) > 1 else responses[0]
                if response == DROP:
                    # Close without a reply: curl reports exit 52, an empty reply.
                    self.close_connection = True
                    self.connection.shutdown(2)
                    return
                status, body = (200, response) if isinstance(response, bytes) else (response, b"")
                self.send_response(status)
                if isinstance(body, Unsized):
                    self.close_connection = True
                else:
                    self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                try:
                    self.wfile.write(body)
                except (BrokenPipeError, ConnectionResetError):
                    pass  # curl stopped reading, as it does at its size bound.

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


class Github:
    """GitHub's OIDC token endpoint: a GET with the job's request token as its bearer
    returns OIDC_TOKEN for the audience asked for, which `audiences` records. `script`
    holds (status, body) answers to give first, one per request."""

    def __init__(self):
        self.audiences = []
        self.status = 200
        self.script = []
        github = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):  # noqa: N802 (http.server's naming)
                query = urllib.parse.parse_qs(urllib.parse.urlsplit(self.path).query)
                github.audiences.append(query.get("audience", [None])[0])
                if self.headers.get("Authorization") != f"Bearer {OIDC_REQUEST_TOKEN}":
                    status, reply = 401, b"{}"
                elif github.script:
                    status, reply = github.script.pop(0)
                else:
                    status, reply = github.status, json.dumps({"count": 1, "value": OIDC_TOKEN}).encode()
                self.send_response(status)
                self.send_header("Content-Length", str(len(reply)))
                self.end_headers()
                self.wfile.write(reply)

            def log_message(self, *args):
                pass

        self.httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.token_url = f"http://127.0.0.1:{self.httpd.server_port}/token?api-version=2.0"
        threading.Thread(target=self.httpd.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True).start()

    def close(self):
        self.httpd.shutdown()
        self.httpd.server_close()


def curl_version():
    first = subprocess.run(["curl", "--version"], capture_output=True, text=True).stdout.split()[1]
    return tuple(int(part) for part in re.findall(r"\d+", first)[:2])


CACHE_PATH = f"/ci-cache/sha256/{SHA}"
SOURCE_PATH = "/releases/tool.tar.gz"
MIRROR_PATH = "/mirror/tool.tar.gz"


class CachedFetchTest(unittest.TestCase):
    def setUp(self):
        self.cache = Server()
        self.source = Server()
        self.github = Github()
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        self.dest = self.dir / "out" / "tool.tar.gz"

    def tearDown(self):
        self.cache.close()
        self.source.close()
        self.github.close()
        self.tmp.cleanup()

    def run_fetch(
        self,
        *,
        cache=True,
        cache_url=None,
        cache_dir=None,
        sources=None,
        sha=SHA,
        oidc=False,
        max_bytes=None,
        path=None,
        read_only=False,
    ):
        env = {
            **os.environ,
            "CI_CACHE_DELAY_SECONDS": "0",
            "CI_SOURCE_DELAY_SECONDS": "0",
        }
        for name in ("CI_CACHE_URL", "CI_CACHE_DIR", "CI_FETCH_MAX_BYTES", "CI_CACHE_READ_ONLY"):
            env.pop(name, None)
        if read_only:
            env["CI_CACHE_READ_ONLY"] = "true"
        # A job's OIDC token only where the test gives it one, even when the tests run in
        # a job that has one.
        env.pop("ACTIONS_ID_TOKEN_REQUEST_URL", None)
        env.pop("ACTIONS_ID_TOKEN_REQUEST_TOKEN", None)
        if oidc:
            env["ACTIONS_ID_TOKEN_REQUEST_URL"] = self.github.token_url
            env["ACTIONS_ID_TOKEN_REQUEST_TOKEN"] = OIDC_REQUEST_TOKEN
        if cache:
            env["CI_CACHE_URL"] = cache_url or f"{self.cache.url}/ci-cache"
        if cache_dir is not None:
            env["CI_CACHE_DIR"] = str(cache_dir)
        if max_bytes is not None:
            env["CI_FETCH_MAX_BYTES"] = str(max_bytes)
        if path is not None:
            env["PATH"] = f"{path}{os.pathsep}{env['PATH']}"
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

    def assert_failed(self, proc, status=1):
        self.assertEqual(proc.returncode, status, proc.stdout + proc.stderr)
        self.assertFalse(self.dest.exists())
        self.assertEqual(list(self.dest.parent.glob(".cached_fetch*")), [])

    def curl_reporting_size_as_56(self):
        """A directory whose `curl` reports a size refusal as macOS's curl does over HTTP/2:
        exit 56 with curl's size message, rather than exit 63."""
        real = shutil.which("curl")
        bin_dir = self.dir / "curl-56"
        bin_dir.mkdir()
        fake = bin_dir / "curl"
        fake.write_text(
            "#!/usr/bin/env bash\n"
            'err="$(mktemp)"; status=0\n'
            f'"{real}" "$@" 2>"$err" || status=$?\n'
            'if [ "$status" -eq 63 ]; then echo "curl: (56) Maximum file size exceeded" >&2; status=56\n'
            'else cat "$err" >&2; fi\n'
            'rm -f "$err"; exit "$status"\n'
        )
        fake.chmod(0o755)
        return bin_dir

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

    # --- writing back ----------------------------------------------------------------

    def assert_no_write_attempt(self):
        self.assertEqual(self.cache.puts, [])
        self.assertEqual(self.cache.refused_puts, [])

    def test_a_miss_is_written_back_signed_and_the_next_job_hits(self):
        self.source.script[SOURCE_PATH] = [CONTENT]
        first = self.run_fetch(oidc=True)
        self.assert_fetched(first)
        self.assertEqual(self.cache.refused_puts, [])
        self.assertEqual(self.cache.puts, [CACHE_PATH])
        self.assertEqual(self.cache.stored[CACHE_PATH], CONTENT)
        self.assertIn("written to the CI cache", first.stdout)
        # One exchange, for the audience the store trusts, of the job's own token, for
        # the shortest lifetime STS grants.
        self.assertEqual(self.github.audiences, [DEFAULT_AUDIENCE])
        self.assertEqual(len(self.cache.sts_requests), 1)
        self.assertEqual(self.cache.sts_requests[0]["DurationSeconds"], "900")

        self.dest.unlink()
        second = self.run_fetch(oidc=True)
        self.assert_fetched(second)
        self.assertIn("verified from the CI cache", second.stdout)
        self.assertEqual(self.source.count(SOURCE_PATH), 1)
        self.assertEqual(self.cache.puts, [CACHE_PATH])
        self.assertEqual(len(self.cache.sts_requests), 1)

    def test_the_credentials_never_reach_the_log(self):
        self.source.script[SOURCE_PATH] = [CONTENT]
        proc = self.run_fetch(oidc=True)
        self.assert_fetched(proc)
        for secret in (SECRET_KEY, SESSION_TOKEN, OIDC_TOKEN, OIDC_REQUEST_TOKEN):
            self.assertNotIn(secret, proc.stdout + proc.stderr)

    def test_without_an_oidc_token_the_write_is_skipped_in_one_line(self):
        self.source.script[SOURCE_PATH] = [CONTENT]
        proc = self.run_fetch()
        self.assert_fetched(proc)
        self.assert_no_write_attempt()
        self.assertEqual(self.cache.sts_requests, [])
        notes = [line for line in proc.stdout.splitlines() if "not written to the CI cache" in line]
        self.assertEqual(len(notes), 1, proc.stdout)
        self.assertNotIn("::warning::", notes[0])
        self.assertNotIn("Could not write", proc.stdout)

    def test_a_read_only_run_writes_nothing_and_says_so_in_one_line(self):
        self.cache.script[CACHE_PATH] = [404]
        self.source.script[SOURCE_PATH] = [CONTENT]
        proc = self.run_fetch(oidc=True, read_only=True)
        self.assert_fetched(proc)
        self.assertEqual(self.cache.puts, [])
        self.assertEqual(self.github.audiences, [])
        self.assertIn("not written to the CI cache, since this run is read-only", proc.stdout)

    def test_an_unsigned_write_is_what_the_store_refuses(self):
        # The model this suite holds the script to: the store answers 403 to the
        # anonymous PUT the script used to send.
        proc = subprocess.run(
            ["curl", "--silent", "--output", "/dev/null", "--write-out", "%{http_code}",
             "--upload-file", "-", f"{self.cache.url}{CACHE_PATH}"],
            input=CONTENT, capture_output=True,
        )
        self.assertEqual(proc.stdout, b"403")
        self.assertEqual(self.cache.refused_puts, [(CACHE_PATH, "not signed")])

    def test_wrong_bytes_are_replaced_with_the_verified_download(self):
        self.cache.script[CACHE_PATH] = [WRONG]
        self.source.script[SOURCE_PATH] = [CONTENT]
        self.assert_fetched(self.run_fetch(oidc=True))
        self.assertEqual(self.cache.puts, [CACHE_PATH])
        self.assertEqual(self.cache.stored[CACHE_PATH], CONTENT)

    def test_a_hit_writes_nothing(self):
        self.cache.script[CACHE_PATH] = [CONTENT]
        self.assert_fetched(self.run_fetch(oidc=True))
        self.assert_no_write_attempt()
        self.assertEqual(self.cache.sts_requests, [])

    def test_nothing_is_written_when_the_cache_refused_the_read(self):
        self.cache.script[CACHE_PATH] = [403]
        self.source.script[SOURCE_PATH] = [CONTENT]
        self.assert_fetched(self.run_fetch(oidc=True))
        self.assert_no_write_attempt()
        self.assertEqual(self.cache.sts_requests, [])

    def test_nothing_is_written_when_the_cache_kept_failing(self):
        self.cache.script[CACHE_PATH] = [503, 503]
        self.source.script[SOURCE_PATH] = [CONTENT]
        self.assert_fetched(self.run_fetch(oidc=True))
        self.assert_no_write_attempt()

    def test_a_refused_token_exchange_skips_the_write_with_a_warning(self):
        self.cache.sts_status = 403
        self.source.script[SOURCE_PATH] = [CONTENT]
        proc = self.run_fetch(oidc=True)
        self.assert_fetched(proc)
        self.assert_no_write_attempt()
        self.assertIn(
            "::warning::Could not get credentials to write tool.tar.gz to the CI cache: "
            "RustFS refused the OIDC token (HTTP 403): AccessDenied: No policy for this repository",
            proc.stdout,
        )

    def test_a_refusal_that_repeats_the_token_does_not_log_it(self):
        self.cache.sts_status = 400
        self.cache.sts_error_message = f"Invalid token {OIDC_TOKEN} for this role"
        self.source.script[SOURCE_PATH] = [CONTENT]
        proc = self.run_fetch(oidc=True)
        self.assert_fetched(proc)
        self.assert_no_write_attempt()
        self.assertNotIn(OIDC_TOKEN, proc.stdout + proc.stderr)
        self.assertIn("AccessDenied: Invalid token *** for this role", proc.stdout)

    def test_a_token_github_will_not_issue_skips_the_write_with_a_warning(self):
        self.github.status = 500
        self.source.script[SOURCE_PATH] = [CONTENT]
        proc = self.run_fetch(oidc=True)
        self.assert_fetched(proc)
        self.assert_no_write_attempt()
        self.assertEqual(self.cache.sts_requests, [])
        self.assertIn("GitHub did not issue an OIDC token", proc.stdout)

    def test_credentials_that_could_break_out_of_the_curl_configuration_are_refused(self):
        for credentials in (
            (ACCESS_KEY, 'secret" --url "http://elsewhere', SESSION_TOKEN),
            ("KEY:WITH:COLONS", SECRET_KEY, SESSION_TOKEN),
            (ACCESS_KEY, SECRET_KEY, "token\\with\\backslashes"),
        ):
            self.cache.sts_credentials = credentials
            self.source.script[SOURCE_PATH] = [CONTENT]
            self.dest.unlink(missing_ok=True)
            proc = self.run_fetch(oidc=True)
            self.assert_fetched(proc)
            self.assert_no_write_attempt()
            self.assertIn("characters an access key, secret or session token never has", proc.stdout)

    def test_a_secret_split_across_lines_is_no_credential(self):
        self.cache.sts_credentials = (ACCESS_KEY, "secret\nmore", SESSION_TOKEN)
        self.source.script[SOURCE_PATH] = [CONTENT]
        proc = self.run_fetch(oidc=True)
        self.assert_fetched(proc)
        self.assert_no_write_attempt()
        self.assertIn("RustFS returned HTTP 200 without credentials", proc.stdout)

    def test_a_curl_too_old_to_sign_skips_the_write_with_a_warning(self):
        real = shutil.which("curl")
        bin_dir = self.dir / "old-curl"
        bin_dir.mkdir()
        fake = bin_dir / "curl"
        fake.write_text(
            "#!/bin/sh\n"
            'if [ "$1" = --version ]; then\n'
            '  echo "curl 7.81.0 (x86_64-pc-linux-gnu) libcurl/7.81.0"; exit 0\n'
            "fi\n"
            f'exec "{real}" "$@"\n'
        )
        fake.chmod(0o755)
        self.source.script[SOURCE_PATH] = [CONTENT]
        proc = self.run_fetch(oidc=True, path=bin_dir)
        self.assert_fetched(proc)
        self.assert_no_write_attempt()
        self.assertEqual(self.cache.sts_requests, [])
        self.assertIn("curl 7.81.0 cannot sign the write", proc.stdout)

    def test_a_failed_write_does_not_fail_the_job(self):
        self.cache.put_status = 500
        self.source.script[SOURCE_PATH] = [CONTENT]
        proc = self.run_fetch(oidc=True)
        self.assert_fetched(proc)
        self.assertEqual(self.cache.puts, [CACHE_PATH])
        self.assertIn("Could not write", proc.stdout)

    def test_a_download_that_fails_its_pin_is_not_written(self):
        self.source.script[SOURCE_PATH] = [WRONG]
        self.assert_failed(self.run_fetch(oidc=True))
        self.assert_no_write_attempt()

    # --- the size bound ----------------------------------------------------------------

    def test_an_oversized_cache_copy_is_a_miss_and_is_replaced(self):
        self.cache.script[CACHE_PATH] = [b"x" * 4096]
        self.source.script[SOURCE_PATH] = [CONTENT]
        proc = self.run_fetch(oidc=True, max_bytes=1024)
        self.assert_fetched(proc)
        self.assertEqual(self.cache.count(CACHE_PATH), 1)
        self.assertIn("larger than CI_FETCH_MAX_BYTES (1024 bytes)", proc.stdout)
        self.assertEqual(self.cache.stored[CACHE_PATH], CONTENT)
        self.assertEqual(list(self.dest.parent.glob(".cached_fetch.*")), [])

    def test_an_oversized_source_download_fails_with_exit_4_and_is_not_retried(self):
        self.source.script[SOURCE_PATH] = [b"x" * 4096]
        proc = self.run_fetch(cache=False, max_bytes=1024)
        self.assert_failed(proc, status=4)
        self.assertEqual(self.source.count(SOURCE_PATH), 1)
        self.assertIn("larger than CI_FETCH_MAX_BYTES (1024 bytes)", proc.stdout)
        self.assertIn("a source offered more than CI_FETCH_MAX_BYTES (1024 bytes)", proc.stdout)

    def test_a_size_refusal_reported_as_exit_56_is_still_a_size_refusal(self):
        # macOS's curl over HTTP/2: without the message check this is a receive failure,
        # retried four times and reported as a network error with exit 1.
        self.source.script[SOURCE_PATH] = [b"x" * 4096]
        proc = self.run_fetch(cache=False, max_bytes=1024, path=self.curl_reporting_size_as_56())
        self.assert_failed(proc, status=4)
        self.assertIn("curl: (56) Maximum file size exceeded", proc.stderr)
        self.assertEqual(self.source.count(SOURCE_PATH), 1)

    def test_a_cache_copy_refused_as_exit_56_is_a_miss_and_not_retried(self):
        self.cache.script[CACHE_PATH] = [b"x" * 4096]
        self.source.script[SOURCE_PATH] = [CONTENT]
        proc = self.run_fetch(max_bytes=1024, path=self.curl_reporting_size_as_56())
        self.assert_fetched(proc)
        self.assertEqual(self.cache.count(CACHE_PATH), 1)
        self.assertIn("The CI cache's copy of tool.tar.gz is larger than CI_FETCH_MAX_BYTES", proc.stdout)

    def test_an_oversized_source_fails_with_exit_4_when_the_other_source_lacks_the_file(self):
        self.source.script[SOURCE_PATH] = [404]
        self.source.script[MIRROR_PATH] = [b"x" * 4096]
        sources = [f"{self.source.url}{SOURCE_PATH}", f"{self.source.url}{MIRROR_PATH}"]
        proc = self.run_fetch(cache=False, max_bytes=1024, sources=sources)
        self.assert_failed(proc, status=4)

    def test_another_source_still_serves_when_one_is_oversized(self):
        self.source.script[SOURCE_PATH] = [b"x" * 4096]
        self.source.script[MIRROR_PATH] = [CONTENT]
        sources = [f"{self.source.url}{SOURCE_PATH}", f"{self.source.url}{MIRROR_PATH}"]
        self.assert_fetched(self.run_fetch(cache=False, max_bytes=1024, sources=sources))

    @unittest.skipIf(curl_version() < (8, 4), "curl bounds a body of unannounced size from 8.4")
    def test_a_body_without_a_length_is_cut_at_the_bound(self):
        self.source.script[SOURCE_PATH] = [Unsized(b"x" * (1 << 20))]
        proc = self.run_fetch(cache=False, max_bytes=64 * 1024)
        self.assert_failed(proc, status=4)
        self.assertEqual(self.source.count(SOURCE_PATH), 1)
        self.assertIn("larger than CI_FETCH_MAX_BYTES (65536 bytes)", proc.stdout)

    def test_a_file_exactly_at_the_bound_is_fetched(self):
        self.source.script[SOURCE_PATH] = [CONTENT]
        self.assert_fetched(self.run_fetch(cache=False, max_bytes=len(CONTENT)))

    def test_an_oversized_cache_dir_entry_is_not_copied(self):
        cache_dir = self.dir / "host-cache"
        (cache_dir / "sha256").mkdir(parents=True)
        (cache_dir / "sha256" / SHA).write_bytes(b"x" * 4096)
        self.cache.script[CACHE_PATH] = [CONTENT]
        proc = self.run_fetch(cache_dir=cache_dir, max_bytes=1024)
        self.assert_fetched(proc)
        self.assertIn("larger than CI_FETCH_MAX_BYTES (1024 bytes)", proc.stdout)
        self.assertEqual((cache_dir / "sha256" / SHA).read_bytes(), CONTENT)

    def test_a_cache_dir_entry_replaced_while_it_is_read_stays_bounded(self):
        # Another job renames a larger file over the entry at the worst moment: just after
        # the script's first command that touches it starts. Reading the entry twice (once
        # to measure, once to copy) would then copy the larger file in full.
        cache_dir = self.dir / "host-cache"
        entry = cache_dir / "sha256" / SHA
        entry.parent.mkdir(parents=True)
        entry.write_bytes(CONTENT)
        larger = self.dir / "larger"
        larger.write_bytes(b"x" * 1048576)
        bin_dir = self.dir / "race-bin"
        bin_dir.mkdir()
        swapped, hashed = self.dir / "swapped", self.dir / "hashed.log"
        for tool in ("wc", "cp", "head", "cat", "dd"):
            real = shutil.which(tool)
            if real is None:
                continue
            wrapper = bin_dir / tool
            wrapper.write_text(
                "#!/usr/bin/env bash\n"
                f"if [ ! -e '{swapped}' ]; then : >'{swapped}'; mv -f '{larger}' '{entry}'; fi\n"
                f'exec "{real}" "$@"\n'
            )
            wrapper.chmod(0o755)
        for tool in ("sha256sum", "shasum"):
            real = shutil.which(tool)
            if real is None:
                continue
            wrapper = bin_dir / tool
            wrapper.write_text(
                "#!/usr/bin/env bash\n"
                f'wc -c <"${{@: -1}}" >>\'{hashed}\'\n'
                f'exec "{real}" "$@"\n'
            )
            wrapper.chmod(0o755)
        self.cache.script[CACHE_PATH] = [CONTENT]
        proc = self.run_fetch(cache_dir=cache_dir, max_bytes=1024, path=bin_dir)
        self.assert_fetched(proc)
        self.assertTrue(swapped.exists(), "the entry was never replaced")
        sizes = [int(line) for line in hashed.read_text().split()]
        self.assertTrue(sizes, proc.stdout + proc.stderr)
        self.assertLessEqual(max(sizes), 1025, proc.stdout + proc.stderr)

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

    def test_a_retry_asks_only_the_source_urls_that_failed_transiently(self):
        self.source.script[SOURCE_PATH] = [404]
        self.source.script[MIRROR_PATH] = [503, CONTENT]
        sources = [f"{self.source.url}{SOURCE_PATH}", f"{self.source.url}{MIRROR_PATH}"]
        self.assert_fetched(self.run_fetch(cache=False, sources=sources))
        self.assertEqual(self.source.requests, [SOURCE_PATH, MIRROR_PATH, MIRROR_PATH])

    def test_a_source_url_without_the_file_is_asked_only_once(self):
        self.source.script[SOURCE_PATH] = [404]
        self.source.script[MIRROR_PATH] = [503]
        sources = [f"{self.source.url}{SOURCE_PATH}", f"{self.source.url}{MIRROR_PATH}"]
        proc = self.run_fetch(cache=False, sources=sources)
        self.assert_failed(proc)
        self.assertEqual(self.source.requests, [SOURCE_PATH] + [MIRROR_PATH] * 4)
        self.assertIn("after 4 attempts", proc.stdout)

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

    def test_rejects_a_malformed_size_bound(self):
        # 19 digits passes the digit pattern, but curl would refuse it as --max-filesize.
        for value in ("0", "1e9", "256M", "9" * 19):
            proc = self.run_fetch(max_bytes=value)
            self.assertEqual(proc.returncode, 2, value)
            self.assertIn("CI_FETCH_MAX_BYTES must ", proc.stdout)
        self.assertEqual(self.cache.requests + self.source.requests, [])


if __name__ == "__main__":
    unittest.main()
