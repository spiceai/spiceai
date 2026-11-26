#!/usr/bin/env python3
"""Tests that the pinned CI tools a job installs are the ones it runs.

Each test runs an action's own `run:` step, as CI runs it, in a temporary tree laid out like
the repository (the action, fetch_pinned.sh, cached_fetch.sh and a pins.json written for
the test), with the pinned file served by a local HTTP server. The runner's PATH already
holds a planted copy of the tool, as a runner image or an earlier job on a shared host can
leave one; it records any run. A job must install the pinned copy into a directory of its
own, first on PATH, and never run the planted one.
"""

import hashlib
import http.server
import io
import json
import os
import shutil
import subprocess
import tarfile
import tempfile
import threading
import unittest
import zipfile
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]


class Server:
    def __init__(self):
        self.files = {}
        server = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):  # noqa: N802 (http.server's naming)
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


def recorder(log, name):
    return f'#!/bin/sh\necho "{name} $*" >> "{log}"\n'


def zip_of(files):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for path, (text, mode) in files.items():
            info = zipfile.ZipInfo(path)
            info.external_attr = mode << 16
            zf.writestr(info, text)
    return buf.getvalue()


def tar_gz_of(files):
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        for path, (text, mode) in files.items():
            data = text.encode()
            info = tarfile.TarInfo(path)
            info.size = len(data)
            info.mode = mode
            tf.addfile(info, io.BytesIO(data))
    return buf.getvalue()


def step(action, name):
    steps = yaml.safe_load((ROOT / ".github" / "actions" / action / "action.yml").read_text())["runs"]["steps"]
    found = [s for s in steps if s.get("name") == name]
    assert len(found) == 1, f"{action} has no single step {name!r}"
    return found[0]


class PinnedToolsTest(unittest.TestCase):
    def setUp(self):
        self.server = Server()
        self.addCleanup(self.server.close)
        self.tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.tmp)
        scripts = self.tmp / ".github" / "scripts"
        scripts.mkdir(parents=True)
        for name in ("fetch_pinned.sh", "cached_fetch.sh", "rustfs_credentials.sh"):
            shutil.copy(ROOT / ".github" / "scripts" / name, scripts / name)
        (self.tmp / ".github" / "ci-cache").mkdir()
        self.pins = {"description": "test pins"}
        self.runner_temp = self.tmp / "runner-temp"
        self.runner_temp.mkdir()
        self.log = self.tmp / "calls.log"
        self.log.write_text("")
        # The runner's own bin, first on PATH, with a planted copy of each tool.
        self.planted = self.tmp / "planted-bin"
        self.planted.mkdir()
        for tool in ("mc", "sccache", "aws", "gh"):
            (self.planted / tool).write_text(recorder(self.log, f"planted-{tool}"))
            (self.planted / tool).chmod(0o755)
        # `uname -m` answers what the test says, so a test can be any architecture.
        (self.planted / "uname").write_text(
            '#!/bin/sh\ncase "$1" in -m) echo "${FAKE_UNAME_M:-x86_64}" ;; *) echo "${FAKE_UNAME_S:-Linux}" ;; esac\n'
        )
        (self.planted / "uname").chmod(0o755)
        for name in ("github_path", "github_env", "github_output"):
            (self.tmp / name).write_text("")

    def serve(self, name, body, target=None):
        sha = hashlib.sha256(body).hexdigest()
        if target is None:
            path = f"/{name}"
            self.pins[name] = {"url": self.server.url + path, "sha256": sha}
        else:
            path = f"/{name}/{target}"
            self.pins[name] = {"version": "1", "url": self.server.url + f"/{name}/{{target}}", "sha256": {target: sha}}
        self.server.files[path] = body
        (self.tmp / ".github" / "ci-cache" / "pins.json").write_text(json.dumps(self.pins))

    def run_step(self, action, name, env=None):
        s = step(action, name)
        action_path = self.tmp / ".github" / "actions" / action
        action_path.mkdir(parents=True, exist_ok=True)
        base = {k: v for k, v in os.environ.items() if not k.startswith(("CI_", "ACTIONS_ID_TOKEN", "RUNNER_", "GITHUB_"))}
        base.update(
            {
                "PATH": f"{self.planted}:{os.environ['PATH']}",
                "GITHUB_ACTION_PATH": str(action_path),
                "GITHUB_PATH": str(self.tmp / "github_path"),
                "GITHUB_ENV": str(self.tmp / "github_env"),
                "GITHUB_OUTPUT": str(self.tmp / "github_output"),
                "RUNNER_TEMP": str(self.runner_temp),
                "RUNNER_OS": "Linux",
                "CI_SOURCE_DELAY_SECONDS": "0",
                "CI_STORE_PREFIX": "",
            }
        )
        for key, value in (s.get("env") or {}).items():
            base[key] = "" if "${{" in str(value) else str(value)
        base.update(env or {})
        script = s["run"]
        self.assertNotIn("${{", script)
        return subprocess.run(["bash", "-e", "-c", script], env=base, capture_output=True, text=True, timeout=120)

    def path_dirs(self):
        return [Path(line) for line in (self.tmp / "github_path").read_text().splitlines() if line]

    def calls(self):
        return self.log.read_text().splitlines()

    def test_setup_minio_runs_its_pinned_mc_not_the_runners(self):
        self.serve("mc", recorder(self.log, "pinned-mc").encode(), "linux-amd64")
        r = self.run_step("setup-minio", "Install MinIO on Linux")
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        (bin_dir,) = self.path_dirs()
        self.assertTrue(bin_dir.is_relative_to(self.runner_temp), bin_dir)
        self.assertTrue(os.access(bin_dir / "mc", os.X_OK))
        # The alias step, run with the PATH a later step gets, uses the pinned mc.
        r = self.run_step(
            "setup-minio",
            "Set MinIO alias",
            {
                "PATH": f"{bin_dir}:{self.planted}:{os.environ['PATH']}",
                "MINIO_ENDPOINT": "http://store:9000",
                "MINIO_ACCESS_KEY": "a",
                "MINIO_SECRET_KEY": "b",
            },
        )
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertEqual(self.calls(), ["pinned-mc alias set spice-minio http://store:9000 a b"])

    def test_setup_minio_installs_nothing_unpinned(self):
        self.serve("mc", b"another mc", "linux-amd64")
        self.pins["mc"]["sha256"]["linux-amd64"] = "1" * 64
        (self.tmp / ".github" / "ci-cache" / "pins.json").write_text(json.dumps(self.pins))
        r = self.run_step("setup-minio", "Install MinIO on Linux")
        self.assertNotEqual(r.returncode, 0)
        self.assertEqual(self.path_dirs(), [])
        r = self.run_step("setup-minio", "Install MinIO on Linux", {"FAKE_UNAME_M": "aarch64"})
        self.assertNotEqual(r.returncode, 0)
        self.assertIn("pins no MinIO client for Linux aarch64", r.stdout + r.stderr)
        steps = yaml.safe_load((ROOT / ".github/actions/setup-minio/action.yml").read_text())["runs"]["steps"]
        for s in steps:
            self.assertNotIn("brew", s.get("run", ""))
            self.assertNotIn("dl.min.io", s.get("run", ""))
        refuse = step("setup-minio", "Refuse a MinIO client that is not pinned")
        self.assertEqual(refuse["if"], "runner.os != 'Linux'")

    def test_upload_to_minio_runs_its_pinned_mc_in_the_same_step(self):
        self.serve("mc", recorder(self.log, "pinned-mc").encode(), "linux-amd64")
        r = self.run_step(
            "upload-to-minio",
            "Install MinIO client",
            {"MINIO_ENDPOINT": "http://store:9000", "MINIO_ACCESS_KEY": "a", "MINIO_SECRET_KEY": "b"},
        )
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertEqual(self.calls(), ["pinned-mc alias set spice-minio http://store:9000 a b"])
        (bin_dir,) = self.path_dirs()
        self.assertTrue(bin_dir.is_relative_to(self.runner_temp), bin_dir)

    def test_setup_sccache_wraps_rustc_only_in_its_pinned_sccache(self):
        archive = tar_gz_of({"sccache-v1/sccache": (recorder(self.log, "pinned-sccache"), 0o755)})
        self.serve("sccache", archive, "x86_64-unknown-linux-musl.tar.gz")
        r = self.run_step("setup-sccache", "Set up sccache (Linux)", {"INPUT_MINIO_ENDPOINT": "http://store:9000"})
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        (bin_dir,) = self.path_dirs()
        self.assertTrue(bin_dir.is_relative_to(self.runner_temp), bin_dir)
        subprocess.run([str(bin_dir / "sccache"), "--version"], check=True)
        self.assertEqual(self.calls(), ["pinned-sccache --version"])
        self.assertIn("installed=true", (self.tmp / "github_output").read_text())
        self.assertIn(f"RUSTC_WRAPPER={bin_dir / 'sccache'}", (self.tmp / "github_env").read_text().splitlines())

    def test_a_mac_wraps_rustc_only_in_the_pinned_sccache(self):
        archive = tar_gz_of({"sccache-v1/sccache": (recorder(self.log, "pinned-sccache"), 0o755)})
        self.serve("sccache", archive, "aarch64-apple-darwin.tar.gz")
        r = self.run_step("setup-sccache", "Install sccache (macOS)", {"FAKE_UNAME_S": "Darwin", "FAKE_UNAME_M": "arm64"})
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        (bin_dir,) = self.path_dirs()
        self.assertTrue(bin_dir.is_relative_to(self.runner_temp), bin_dir)
        self.assertIn("installed=true", (self.tmp / "github_output").read_text())
        self.assertIn(f"RUSTC_WRAPPER={bin_dir / 'sccache'}", (self.tmp / "github_env").read_text().splitlines())
        subprocess.run([str(bin_dir / "sccache"), "--version"], check=True)
        self.assertEqual(self.calls(), ["pinned-sccache --version"])
        # The configuration and the pre-flight, which run sccache, follow only that install.
        self.assertEqual(step("setup-sccache", "Set up sccache (macOS)")["if"],
                         "runner.os == 'macOS' && steps.macos.outputs.installed == 'true'")
        preflight = step("setup-sccache", "Confirm the compiler cache answers before wrapping rustc")
        self.assertIn("(runner.os == 'macOS' && steps.macos.outputs.installed == 'true')", preflight["if"])

    def test_a_mac_without_a_pin_wraps_nothing(self):
        self.serve("sccache", b"unused", "aarch64-apple-darwin.tar.gz")
        r = self.run_step("setup-sccache", "Install sccache (macOS)", {"FAKE_UNAME_S": "Darwin", "FAKE_UNAME_M": "ppc"})
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertEqual(self.path_dirs(), [])
        self.assertIn("RUSTC_WRAPPER=", (self.tmp / "github_env").read_text().splitlines())
        self.assertIn("installed=false", (self.tmp / "github_output").read_text())
        self.assertEqual(self.calls(), [])

    def test_the_cold_start_fixture_runs_the_pinned_delta_jars(self):
        run = step("seed-cold-start-fixture", "Generate and upload the fixture")["run"]
        for jar in ("delta-spark-jar", "delta-storage-jar", "antlr4-runtime-jar"):
            self.assertIn(f'fetch_pinned.sh" {jar} "$jars/{jar}.jar"', run)
        self.assertIn('--jars "$jars/delta-spark-jar.jar,$jars/delta-storage-jar.jar,$jars/antlr4-runtime-jar.jar"', run)
        script = (ROOT / "test" / "scripts" / "cold_start_delta_fixture.py").read_text()
        self.assertIn('builder.config("spark.jars", args.jars).getOrCreate()', script)
        self.assertLess(script.index("if args.jars:"), script.index("configure_spark_with_delta_pip(builder)"))

    def test_only_setup_sccache_makes_sccache_the_rustc_wrapper(self):
        for path in sorted((ROOT / ".github").rglob("*.y*ml")):
            if path.parent.name == "setup-sccache":
                continue
            with self.subTest(path=str(path.relative_to(ROOT))):
                self.assertNotRegex(path.read_text(), r"RUSTC_WRAPPER=sccache|RUSTC_WRAPPER:\s*'?sccache")

    def test_setup_sccache_wraps_nothing_where_no_sccache_is_pinned(self):
        self.serve("sccache", b"unused", "x86_64-unknown-linux-musl.tar.gz")
        r = self.run_step(
            "setup-sccache",
            "Set up sccache (Linux)",
            {"INPUT_MINIO_ENDPOINT": "http://store:9000", "FAKE_UNAME_M": "aarch64"},
        )
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertEqual(self.path_dirs(), [])
        env = (self.tmp / "github_env").read_text().splitlines()
        self.assertIn("RUSTC_WRAPPER=", env)
        self.assertIn("SCCACHE_SETUP=false", env)
        self.assertIn("installed=false", (self.tmp / "github_output").read_text())
        # The pre-flight, which runs sccache, runs on Linux only once the pinned one is installed.
        preflight = step("setup-sccache", "Confirm the compiler cache answers before wrapping rustc")
        self.assertIn("steps.linux.outputs.installed == 'true'", preflight["if"])
        self.assertEqual(self.calls(), [])

    @unittest.skipUnless(shutil.which("unzip"), "the step unpacks the zip with unzip, which every runner has")
    def test_the_aws_cli_is_the_pinned_one(self):
        install = (
            "#!/bin/sh\nset -e\n"
            'while [ $# -gt 0 ]; do case "$1" in --install-dir) i="$2"; shift 2 ;; --bin-dir) b="$2"; shift 2 ;; *) exit 9 ;; esac; done\n'
            f'mkdir -p "$i" "$b"\nprintf \'%s\\n\' \'#!/bin/sh\' \'echo "pinned-aws $*" >> "{self.log}"\' > "$b/aws"\nchmod +x "$b/aws"\n'
        )
        self.serve("awscli", zip_of({"aws/install": (install, 0o755)}))
        r = self.run_step("install-aws-cli", "Install the AWS CLI")
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        (bin_dir,) = self.path_dirs()
        self.assertTrue(bin_dir.is_relative_to(self.runner_temp), bin_dir)
        self.assertEqual(self.calls(), ["pinned-aws --version"])
        # The pin is the x86_64 build: anywhere else the action refuses before downloading.
        r = self.run_step("install-aws-cli", "Install the AWS CLI", {"FAKE_UNAME_M": "aarch64"})
        self.assertNotEqual(r.returncode, 0)
        self.assertIn("Linux x86_64 build", r.stdout + r.stderr)
        # Every caller installs it through the action.
        for path in (
            ".github/actions/setup-spicescript/action.yml",
            ".github/workflows/spicescript_dispatch.yml",
            ".github/workflows/testoperator_run_cluster_bench_aws.yml",
        ):
            text = (ROOT / path).read_text()
            self.assertIn("uses: $/.github/actions/install-aws-cli", text, path)
            self.assertNotIn("aws/install", text, path)

    def test_gh_is_the_pinned_one_on_linux_and_macos(self):
        for system, machine, target in (("Linux", "x86_64", "linux_amd64.tar.gz"), ("Darwin", "arm64", "macOS_arm64.zip")):
            with self.subTest(target=target):
                (self.tmp / "github_path").write_text("")
                (self.tmp / "github_output").write_text("")
                self.log.write_text("")
                inner = {f"gh_9.9.9_{target.split('.')[0]}/bin/gh": (recorder(self.log, "pinned-gh"), 0o755)}
                archive = tar_gz_of(inner) if target.endswith(".tar.gz") else zip_of(inner)
                self.serve("gh", archive, target)
                r = self.run_step("install-gh-cli", "Install the GitHub CLI", {"FAKE_UNAME_S": system, "FAKE_UNAME_M": machine})
                self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
                (bin_dir,) = self.path_dirs()
                self.assertTrue(bin_dir.is_relative_to(self.runner_temp), bin_dir)
                self.assertIn(f"path={bin_dir / 'gh'}", (self.tmp / "github_output").read_text())
                self.assertEqual(self.calls(), ["pinned-gh --version"])
        self.assertNotIn("command -v gh", step("install-gh-cli", "Install the GitHub CLI")["run"])
        r = self.run_step("install-gh-cli", "Install the GitHub CLI", {"FAKE_UNAME_S": "Windows_NT"})
        self.assertNotEqual(r.returncode, 0)
        self.assertIn("pins no gh", r.stdout + r.stderr)


if __name__ == "__main__":
    unittest.main()
