#!/usr/bin/env python3
"""Tests for install-protoc's Linux and macOS step, run as CI runs it.

Each test lays out a temporary tree with the action, fetch_pinned.sh and cached_fetch.sh,
and a pins.json whose protoc entry names a zip a local HTTP server serves. The zip holds a
stand-in `protoc` that prints the pinned version. The runner's tool cache and HOME hold a
planted protoc of the same version, which records any run, as a copy an earlier job on a
shared self-hosted host could leave.
"""

import hashlib
import http.server
import io
import json
import os
import re
import shutil
import subprocess
import tempfile
import threading
import unittest
import zipfile
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
ACTION = ROOT / ".github" / "actions" / "install-protoc" / "action.yml"
VERSION = "36.2"
TARGETS = ["linux-x86_64", "linux-aarch_64", "linux-s390_64", "linux-ppcle_64", "osx-x86_64", "osx-aarch_64"]


def protoc_zip(text):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("bin/protoc", f"#!/bin/sh\necho 'libprotoc {VERSION}'\n{text}\n")
        zf.writestr("include/google/protobuf/empty.proto", 'syntax = "proto3";\n')
    return buf.getvalue()


class Server:
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


def unix_step():
    steps = yaml.safe_load(ACTION.read_text())["runs"]["steps"]
    (step,) = [s for s in steps if s.get("name") == "Download protoc (Linux/macOS)"]
    assert "${{" not in step["run"], "the step must read no expression"
    return step["run"]


class InstallProtocTest(unittest.TestCase):
    def setUp(self):
        self.server = Server()
        self.addCleanup(self.server.close)
        self.tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.tmp)
        scripts = self.tmp / ".github" / "scripts"
        scripts.mkdir(parents=True)
        for name in ("fetch_pinned.sh", "cached_fetch.sh", "rustfs_credentials.sh"):
            shutil.copy(ROOT / ".github" / "scripts" / name, scripts / name)
        self.action_path = self.tmp / ".github" / "actions" / "install-protoc"
        self.action_path.mkdir(parents=True)
        (self.tmp / ".github" / "ci-cache").mkdir()
        self.zip = protoc_zip("")
        self.server.files[f"/v{VERSION}/protoc.zip"] = self.zip
        self.pin(hashlib.sha256(self.zip).hexdigest())

        self.runner_temp = self.tmp / "runner-temp"
        self.runner_temp.mkdir()
        self.tool_cache = self.tmp / "tool-cache"
        self.home = self.tmp / "home"
        self.marker = self.tmp / "planted-ran"
        planted = f"#!/bin/sh\necho ran >> '{self.marker}'\necho 'libprotoc {VERSION}'\n"
        for suffix in ("x86_64", "aarch_64", "aarch_64-runner-1", "x86_64-runner-1"):
            for base in (self.tool_cache / "protoc" / VERSION, self.home / ".local" / "protoc" / VERSION):
                b = base / suffix / "bin"
                b.mkdir(parents=True, exist_ok=True)
                (b / "protoc").write_text(planted)
                (b / "protoc").chmod(0o755)
        self.github_path = self.tmp / "github_path"

    def pin(self, sha):
        pins = {
            "description": "test pins",
            "protoc": {
                "version": VERSION,
                "url": self.server.url + "/v{version}/protoc.zip",
                "sha256": {t: sha for t in TARGETS},
            },
        }
        (self.tmp / ".github" / "ci-cache" / "pins.json").write_text(json.dumps(pins))

    def run_step(self):
        self.github_path.write_text("")
        env = {k: v for k, v in os.environ.items() if not k.startswith(("CI_", "ACTIONS_ID_TOKEN", "RUNNER_"))}
        env.update(
            {
                "GITHUB_ACTION_PATH": str(self.action_path),
                "GITHUB_PATH": str(self.github_path),
                "RUNNER_TEMP": str(self.runner_temp),
                "RUNNER_TOOL_CACHE": str(self.tool_cache),
                "RUNNER_NAME": "runner-1",
                "HOME": str(self.home),
                "CI_SOURCE_DELAY_SECONDS": "0",
            }
        )
        script = re.sub(r"sleep \$\(\(attempt \* 2\)\)", ":", unix_step())
        return subprocess.run(
            ["bash", "-e", "-o", "pipefail", "-c", script], env=env, capture_output=True, text=True, timeout=120
        )

    def installed(self):
        return [line for line in self.github_path.read_text().splitlines() if line]

    def test_a_job_runs_its_own_checked_copy_and_never_a_kept_one(self):
        r = self.run_step()
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        (bin_dir,) = self.installed()
        self.assertTrue(Path(bin_dir).is_relative_to(self.runner_temp), bin_dir)
        self.assertEqual((Path(bin_dir) / "protoc").read_bytes(), zipfile.ZipFile(io.BytesIO(self.zip)).read("bin/protoc"))
        self.assertFalse(self.marker.exists(), "a protoc kept in the tool cache or HOME ran")
        self.assertIn(f"/v{VERSION}/protoc.zip", self.server.requests)
        self.assertNotIn("skipping download", r.stdout)

    def test_each_job_gets_a_new_directory(self):
        self.assertEqual(self.run_step().returncode, 0)
        first = self.installed()
        self.assertEqual(self.run_step().returncode, 0)
        second = self.installed()
        self.assertNotEqual(first, second)

    def test_a_zip_that_does_not_match_its_pin_installs_nothing(self):
        self.server.files[f"/v{VERSION}/protoc.zip"] = protoc_zip("echo tampered")
        r = self.run_step()
        self.assertNotEqual(r.returncode, 0)
        self.assertEqual(self.installed(), [])
        self.assertIn("Failed to install protoc", r.stdout + r.stderr)
        self.assertFalse(self.marker.exists())
        self.assertEqual([p for p in self.runner_temp.iterdir() if p.name.startswith("protoc.")], [])

    def test_a_protoc_that_reports_another_version_is_not_installed(self):
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("bin/protoc", "#!/bin/sh\necho 'libprotoc 1.0'\n")
        self.zip = buf.getvalue()
        self.server.files[f"/v{VERSION}/protoc.zip"] = self.zip
        self.pin(hashlib.sha256(self.zip).hexdigest())
        r = self.run_step()
        self.assertNotEqual(r.returncode, 0)
        self.assertEqual(self.installed(), [])


if __name__ == "__main__":
    unittest.main()
