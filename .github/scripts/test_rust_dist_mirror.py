#!/usr/bin/env python3
"""Tests for the download failures that stop a toolchain install instead of falling back.

cached_fetch.sh exits 4 when a source offers more than CI_FETCH_MAX_BYTES, and 2 when its
arguments or settings are wrong, such as a malformed CI_FETCH_MAX_BYTES
(test_cached_fetch.py). rust_dist_mirror.py must pass either on as its own, and
setup-rust must then fail rather than install the toolchain with rustup, which would
download the same files without that bound or those settings. Every other failure keeps
falling back to rustup, so a first install still works when the cache or the network
fails.

The mirror runs with a stand-in for cached_fetch.sh and a pins file of its own, so a
toolchain bump in the real pins.json changes nothing here. setup-rust's own step runs from
its action.yml, with HOME, CARGO_HOME and RUSTUP_HOME in a temporary directory and
stand-ins for rustup, cargo, rustc and the mirror, so nothing is downloaded or installed.

rustup comes only from the pinned rustup-init, and on a self-hosted Mac the step gives the
job its own CARGO_HOME and RUSTUP_HOME (PerJobHomeTest). There the step runs with the real
cached_fetch.sh, a stand-in rustup-init served from a file:// URL its own pins file names,
and shared ~/.cargo and ~/.rustup homes full of stand-ins that log any use.
"""

import hashlib
import json
import os
import platform
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import yaml

GITHUB_DIR = Path(__file__).resolve().parent.parent
MIRROR = GITHUB_DIR / "scripts" / "rust_dist_mirror.py"
SETUP_RUST = GITHUB_DIR / "actions" / "setup-rust" / "action.yml"
# A toolchain no real pin names, so these tests never depend on the one in use.
TOOLCHAIN = "9.9.9"
PINS = {"rust": {"components": ["rustc"], "toolchains": {TOOLCHAIN: {"manifest_sha256": "0" * 64}}}}

# Logs every call with the dist server it was given, and does what setup-rust needs.
FAKE_RUSTUP = """#!/usr/bin/env bash
echo "rustup $* dist=${RUSTUP_DIST_SERVER:-}" >> "$RUSTUP_LOG"
case "$(uname -s)-$(uname -m)" in
  Linux-x86_64) host=x86_64-unknown-linux-gnu ;;
  Linux-aarch64) host=aarch64-unknown-linux-gnu ;;
  Darwin-arm64) host=aarch64-apple-darwin ;;
  *) host=x86_64-apple-darwin ;;
esac
case "$1" in
  show) echo "Default host: $host" ;;
  toolchain)
    bin="${RUSTUP_HOME:-$HOME/.rustup}/toolchains/$3-$host/bin"
    mkdir -p "$bin"
    for tool in rustc cargo rustfmt cargo-clippy; do
      printf '#!/bin/sh\\n' > "$bin/$tool"
      chmod +x "$bin/$tool"
    done
    ;;
  which) echo "${RUSTUP_HOME:-$HOME/.rustup}/toolchains/$3-$host/bin/rustc" ;;
esac
"""


class MirrorExitTest(unittest.TestCase):
    """rust_dist_mirror.py, with cached_fetch.sh replaced by one that exits as told."""

    def run_mirror(self, fetch_status, toolchain=TOOLCHAIN):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "scripts").mkdir()
            (root / "ci-cache").mkdir()
            shutil.copy(MIRROR, root / "scripts" / MIRROR.name)
            (root / "ci-cache" / "pins.json").write_text(json.dumps(PINS))
            (root / "scripts" / "cached_fetch.sh").write_text(f"exit {fetch_status}\n")
            return subprocess.run(
                [sys.executable, str(root / "scripts" / MIRROR.name), "--toolchain", toolchain,
                 "--host", "x86_64-unknown-linux-gnu", "--out", str(root / "out")],
                capture_output=True, text=True, timeout=60,
            )

    def test_a_download_over_the_bound_exits_4(self):
        self.assertEqual(self.run_mirror(4).returncode, 4)

    def test_refused_download_settings_exit_2(self):
        self.assertEqual(self.run_mirror(2).returncode, 2)

    def test_any_other_failed_download_exits_1(self):
        proc = self.run_mirror(1)
        self.assertEqual(proc.returncode, 1, proc.stderr)

    def test_a_toolchain_without_a_pin_exits_3_before_any_download(self):
        self.assertEqual(self.run_mirror(4, toolchain="9.9.8").returncode, 3)


class SetupRustTest(unittest.TestCase):
    """setup-rust's Linux/macOS step, with each answer the mirror can give."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        action = yaml.safe_load(SETUP_RUST.read_text())
        (step,) = [
            s for s in action["runs"]["steps"] if s.get("name") == "Set up Rust toolchain (Linux/macOS)"
        ]
        self.step = self.dir / "step.sh"
        self.step.write_text(step["run"])
        # The step finds the mirror at $GITHUB_ACTION_PATH/../../scripts/rust_dist_mirror.py.
        self.action_path = self.dir / "gh" / "actions" / "setup-rust"
        self.action_path.mkdir(parents=True)
        (self.dir / "gh" / "scripts").mkdir()
        self.mirror = self.dir / "gh" / "scripts" / "rust_dist_mirror.py"
        self.cargo_bin = self.dir / "cargo" / "bin"
        self.cargo_bin.mkdir(parents=True)
        (self.cargo_bin / "rustup").write_text(FAKE_RUSTUP)
        for tool in ("cargo", "rustc"):
            (self.cargo_bin / tool).write_text(f"#!/bin/sh\necho '{tool} stand-in'\n")
        for tool in self.cargo_bin.iterdir():
            tool.chmod(0o755)
        self.work = self.dir / "work"
        self.work.mkdir()
        (self.work / "rust-toolchain.toml").write_text(f'[toolchain]\nchannel = "{TOOLCHAIN}"\n')
        self.log = self.dir / "rustup.log"

    def tearDown(self):
        self.tmp.cleanup()

    def run_step(self, mirror_status):
        self.mirror.write_text(f"import sys\nsys.exit({mirror_status})\n")
        self.log.write_text("")
        home = self.dir / "home"
        home.mkdir(exist_ok=True)
        env = {
            "PATH": os.environ["PATH"],
            "HOME": str(home),
            "CARGO_HOME": str(self.cargo_bin.parent),
            "RUSTUP_HOME": str(self.dir / "rustup"),
            "RUSTUP_LOG": str(self.log),
            "RUNNER_TEMP": str(self.dir),
            "GITHUB_PATH": str(self.dir / "github_path"),
            "GITHUB_ENV": str(self.dir / "github_env"),
            "GITHUB_ACTION_PATH": str(self.action_path),
            "INPUT_TOOLCHAIN": "",
            "CI_CACHE_URL": "http://127.0.0.1:9/ci-cache",
        }
        proc = subprocess.run(
            ["bash", "-e", str(self.step)],
            cwd=self.work, env=env, capture_output=True, text=True, timeout=120,
        )
        return proc, self.log.read_text().splitlines()

    def installs(self, calls):
        return [call for call in calls if call.startswith(f"rustup toolchain install {TOOLCHAIN}")]

    def test_a_download_over_the_bound_fails_the_step_without_rustup(self):
        proc, calls = self.run_step(4)
        self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
        self.assertIn("larger than CI_FETCH_MAX_BYTES", proc.stdout)
        self.assertIn("::error::", proc.stdout)
        self.assertEqual(self.installs(calls), [])

    def test_refused_download_settings_fail_the_step_without_rustup(self):
        proc, calls = self.run_step(2)
        self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
        self.assertIn("download settings", proc.stdout)
        self.assertIn("::error::", proc.stdout)
        self.assertEqual(self.installs(calls), [])

    def test_any_other_mirror_failure_still_installs_with_rustup(self):
        proc, calls = self.run_step(1)
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertIn(f"Could not assemble Rust {TOOLCHAIN} from its pinned release (exit 1)", proc.stdout)
        (install,) = self.installs(calls)
        self.assertTrue(install.endswith(" dist="), install)

    def test_a_toolchain_without_a_pin_installs_with_rustup_quietly(self):
        proc, calls = self.run_step(3)
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertNotIn("::warning::", proc.stdout)
        (install,) = self.installs(calls)
        self.assertTrue(install.endswith(" dist="), install)

    def test_a_mirror_installs_from_the_mirror(self):
        proc, calls = self.run_step(0)
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertIn(f"Installed Rust {TOOLCHAIN} from the CI cache's pinned release.", proc.stdout)
        (install,) = self.installs(calls)
        self.assertIn(" dist=file://", install)


# The host triple setup-rust maps this machine to.
HOST = {
    ("Linux", "x86_64"): "x86_64-unknown-linux-gnu",
    ("Linux", "aarch64"): "aarch64-unknown-linux-gnu",
    ("Darwin", "arm64"): "aarch64-apple-darwin",
    ("Darwin", "x86_64"): "x86_64-apple-darwin",
}.get((platform.system(), platform.machine()), "")
RUSTUP_VERSION = "9.9.9"

# Stands in for rustup-init: logs how it was run, then installs the stub rustup and two
# proxies into CARGO_HOME (default ~/.cargo), as the real one installs rustup and its
# proxies there.
FAKE_RUSTUP_INIT = """#!/usr/bin/env bash
echo "rustup-init $* CARGO_HOME=${CARGO_HOME:-} RUSTUP_HOME=${RUSTUP_HOME:-}" >> "$RUSTUP_LOG"
bin="${CARGO_HOME:-$HOME/.cargo}/bin"
mkdir -p "$bin" "${RUSTUP_HOME:-$HOME/.rustup}"
cp "$STUB_RUSTUP" "$bin/rustup"
for tool in cargo rustc; do
  printf '#!/bin/sh\\necho "%s from the job home"\\n' "$tool" > "$bin/$tool"
done
chmod 755 "$bin/rustup" "$bin/cargo" "$bin/rustc"
"""
# What a pull request job could leave in the shared homes: anything run or sourced from
# them logs itself.
TROJAN = '#!/bin/sh\necho "ran $0 $*" >> "$TROJAN_LOG"\n'


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def snapshot(root: Path) -> dict:
    """Every path under root, with its mode, size, mtime and content hash."""
    found = {}
    for path in sorted(root.rglob("*")):
        st = path.lstat()
        digest = sha256(path.read_bytes()) if path.is_file() and not path.is_symlink() else ""
        found[str(path.relative_to(root))] = (st.st_mode, st.st_size, st.st_mtime_ns, digest)
    return found


@unittest.skipUnless(HOST, "setup-rust maps no Rust host for this machine")
class PerJobHomeTest(unittest.TestCase):
    """setup-rust's Linux/macOS step as a self-hosted Mac runs it: homes of the job's own."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        # Resolved, so the paths the step prints match these (macOS puts temp under /var,
        # a symlink to /private/var).
        self.dir = Path(self.tmp.name).resolve()
        action = yaml.safe_load(SETUP_RUST.read_text())
        (step,) = [
            s for s in action["runs"]["steps"] if s.get("name") == "Set up Rust toolchain (Linux/macOS)"
        ]
        self.step = self.dir / "step.sh"
        self.step.write_text(step["run"])
        gh = self.dir / "gh"
        self.action_path = gh / "actions" / "setup-rust"
        self.action_path.mkdir(parents=True)
        (gh / "scripts").mkdir()
        (gh / "ci-cache").mkdir()
        shutil.copy(GITHUB_DIR / "scripts" / "cached_fetch.sh", gh / "scripts" / "cached_fetch.sh")
        self.pins = gh / "ci-cache" / "pins.json"
        # Logs its arguments and succeeds, so the toolchain installs from the "mirror".
        self.mirror_log = self.dir / "mirror.log"
        (gh / "scripts" / "rust_dist_mirror.py").write_text(
            "import os, sys\n"
            "open(os.environ['MIRROR_LOG'], 'a').write(' '.join(sys.argv[1:]) + '\\n')\n"
        )

        # The rustup-init the pins file names, served from a file:// URL.
        self.rustup_init = FAKE_RUSTUP_INIT.encode()
        dist = self.dir / "dist" / RUSTUP_VERSION / HOST
        dist.mkdir(parents=True)
        (dist / "rustup-init").write_bytes(self.rustup_init)
        self.stub_rustup = self.dir / "stub-rustup"
        self.stub_rustup.write_text(FAKE_RUSTUP)
        self.write_pins({HOST: sha256(self.rustup_init)})

        # A runner laid out as on the Macs: the workspace and RUNNER_TEMP under its _work
        # directory, the shared homes in HOME. The checkout's own .cargo/config.toml is
        # the repository's and is allowed.
        self.home = self.dir / "home"
        work = self.dir / "runner" / "_work"
        self.workspace = work / "repo" / "repo"
        (self.workspace / ".cargo").mkdir(parents=True)
        (self.workspace / ".cargo" / "config.toml").write_text("[net]\nretry = 3\n")
        (self.workspace / "rust-toolchain.toml").write_text(f'[toolchain]\nchannel = "{TOOLCHAIN}"\n')
        self.runner_temp = work / "_temp"
        self.runner_temp.mkdir(parents=True)
        self.rust_home = self.runner_temp / "rust-home"

        # Shared homes a pull request job could have filled: a rustup, proxies and a
        # complete toolchain of its own, and an env file that would be sourced.
        self.shared_bin = self.home / ".cargo" / "bin"
        self.shared_bin.mkdir(parents=True)
        for tool in ("rustup", "cargo", "rustc", "rustup-init"):
            (self.shared_bin / tool).write_text(TROJAN)
        (self.home / ".cargo" / "env").write_text('echo "sourced $HOME/.cargo/env" >> "$TROJAN_LOG"\n')
        shared_toolchain = self.home / ".rustup" / "toolchains" / f"{TOOLCHAIN}-{HOST}" / "bin"
        shared_toolchain.mkdir(parents=True)
        for tool in ("rustc", "cargo", "rustfmt", "cargo-clippy"):
            (shared_toolchain / tool).write_text(TROJAN)
        (self.home / ".rustup" / "settings.toml").write_text('default_toolchain = "evil"\n')
        for path in list(self.shared_bin.iterdir()) + list(shared_toolchain.iterdir()):
            path.chmod(0o755)
        self.shared = {name: snapshot(self.home / name) for name in (".cargo", ".rustup")}

        self.log = self.dir / "rustup.log"
        self.trojan_log = self.dir / "trojan.log"
        self.github_path = self.dir / "github_path"
        self.github_env = self.dir / "github_env"
        # The runner's own PATH, with the shared bin ahead of everything, the worst case.
        self.base_path = f"{self.shared_bin}{os.pathsep}{os.environ['PATH']}"

    def tearDown(self):
        # Whatever a test flagged immutable or read-only, so the directory can go.
        if shutil.which("chflags"):
            subprocess.run(["chflags", "-R", "nouchg", str(self.dir)], check=False)
        self.runner_temp.chmod(0o755)
        self.tmp.cleanup()

    def write_pins(self, hashes):
        url = f"file://{self.dir}/dist/{{version}}/{{target}}/rustup-init"
        self.pins.write_text(json.dumps(
            {"rustup-init": {"version": RUSTUP_VERSION, "url": url, "sha256": hashes}}
        ))

    def run_step(self, **overrides):
        for path in (self.log, self.trojan_log, self.mirror_log, self.github_path, self.github_env):
            path.write_text("")
        env = {
            "PATH": self.base_path,
            "HOME": str(self.home),
            "RUNNER_OS": "macOS",
            "RUNNER_ENVIRONMENT": "self-hosted",
            "RUNNER_TEMP": str(self.runner_temp),
            "GITHUB_WORKSPACE": str(self.workspace),
            "GITHUB_PATH": str(self.github_path),
            "GITHUB_ENV": str(self.github_env),
            "GITHUB_ACTION_PATH": str(self.action_path),
            "INPUT_TOOLCHAIN": "",
            "RUSTUP_LOG": str(self.log),
            "TROJAN_LOG": str(self.trojan_log),
            "MIRROR_LOG": str(self.mirror_log),
            "STUB_RUSTUP": str(self.stub_rustup),
            "CI_SOURCE_ATTEMPTS": "1",
        }
        env.update(overrides)
        env = {key: value for key, value in env.items() if value is not None}
        proc = subprocess.run(
            ["bash", "-e", str(self.step)],
            cwd=self.workspace, env=env, capture_output=True, text=True, timeout=120,
        )
        return proc, self.log.read_text().splitlines()

    def github_env_values(self):
        return dict(line.split("=", 1) for line in self.github_env.read_text().splitlines() if "=" in line)

    def assert_shared_homes_untouched(self, proc):
        self.assertEqual(self.trojan_log.read_text(), "", proc.stdout + proc.stderr)
        for name, before in self.shared.items():
            self.assertEqual(snapshot(self.home / name), before, f"~/{name} changed")

    def test_the_job_gets_its_own_homes_and_never_uses_the_shared_ones(self):
        proc, calls = self.run_step()
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assert_shared_homes_untouched(proc)

        cargo_home, rustup_home = self.rust_home / "cargo", self.rust_home / "rustup"
        self.assertEqual(
            calls[0],
            f"rustup-init --no-modify-path -y --default-toolchain none "
            f"CARGO_HOME={cargo_home} RUSTUP_HOME={rustup_home}",
        )
        self.assertTrue(all(call.startswith("rustup ") for call in calls[1:]), calls)
        self.assertEqual(calls[1], "rustup set auto-self-update disable dist=")
        self.assertIn(f"rustup-init: verified from file://{self.dir}/dist/", proc.stdout)
        # The toolchain came from the mirror, which ran without a CI_CACHE_URL.
        self.assertIn(f"--host {HOST}", self.mirror_log.read_text())
        (install,) = [call for call in calls if call.startswith(f"rustup toolchain install {TOOLCHAIN}")]
        self.assertIn(" dist=file://", install)
        self.assertTrue((rustup_home / "toolchains" / f"{TOOLCHAIN}-{HOST}" / "bin" / "rustc").is_file())
        self.assertIn("cargo from the job home", proc.stdout)
        # rustup-init is not left behind, and the verified copy is kept on the host.
        self.assertEqual(list(self.runner_temp.glob("rustup-init*")), [])
        kept = self.home / "Library/Caches/spice-ci/downloads/sha256" / sha256(self.rustup_init)
        self.assertEqual(kept.read_bytes(), self.rustup_init)

        self.assertEqual(self.github_env_values(), {
            "CARGO_HOME": str(cargo_home),
            "RUSTUP_HOME": str(rustup_home),
            "SCCACHE_DIR": str(self.rust_home / "sccache"),
            "RUSTUP_TOOLCHAIN": f"{TOOLCHAIN}-{HOST}",
        })

    def test_later_steps_find_the_job_home_before_the_shared_bin(self):
        proc, _ = self.run_step()
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        job_bin = str(self.rust_home / "cargo" / "bin")
        added = self.github_path.read_text().splitlines()
        self.assertEqual(added, [job_bin])
        # The runner puts GITHUB_PATH entries, last added first, ahead of its own PATH.
        later = os.pathsep.join(list(reversed(added)) + [self.base_path])
        entries = later.split(os.pathsep)
        self.assertLess(entries.index(job_bin), entries.index(str(self.shared_bin)))
        for tool in ("rustup", "cargo", "rustc"):
            self.assertEqual(shutil.which(tool, path=later), os.path.join(job_bin, tool))

    def test_a_rustup_init_that_does_not_match_its_pin_fails_closed(self):
        self.write_pins({HOST: sha256(b"some other rustup-init")})
        proc, calls = self.run_step()
        self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
        self.assertIn("does not match the pinned SHA-256 of rustup-init", proc.stdout)
        self.assertIn("::error::Could not get rustup-init 9.9.9", proc.stdout)
        self.assertEqual(calls, [])
        self.assertFalse((self.rust_home / "cargo" / "bin" / "rustup").exists())
        self.assertNotIn("sh.rustup.rs", proc.stdout + proc.stderr)
        self.assertEqual(self.mirror_log.read_text(), "")
        self.assert_shared_homes_untouched(proc)

    def test_a_tampered_copy_in_the_host_cache_is_not_run(self):
        planted = self.home / "Library/Caches/spice-ci/downloads/sha256" / sha256(self.rustup_init)
        planted.parent.mkdir(parents=True)
        planted.write_text(TROJAN)
        planted.chmod(0o755)
        proc, calls = self.run_step()
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertIn("does not match its pinned SHA-256; ignoring it.", proc.stdout)
        self.assertTrue(calls[0].startswith("rustup-init --no-modify-path"), calls)
        self.assertEqual(planted.read_bytes(), self.rustup_init)
        self.assert_shared_homes_untouched(proc)

    def test_a_host_without_a_rustup_init_pin_fails_closed(self):
        self.write_pins({"some-other-host": sha256(self.rustup_init)})
        proc, calls = self.run_step()
        self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
        self.assertIn(f"pins no rustup-init for {platform.system()} {platform.machine()} ({HOST})", proc.stdout)
        self.assertEqual(calls, [])
        self.assert_shared_homes_untouched(proc)

    def test_cargo_configuration_above_the_checkout_stops_the_job(self):
        planted = self.dir / "runner" / "_work" / ".cargo" / "config.toml"
        planted.parent.mkdir()
        planted.write_text('[build]\nrustc-wrapper = "/tmp/evil"\n')
        legacy = self.dir / "runner" / ".cargo" / "config"
        legacy.parent.mkdir()
        legacy.write_text("[build]\n")
        proc, calls = self.run_step()
        self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
        # Nearest first, as the walk up from the checkout finds them.
        self.assertIn(f"::error::cargo would read {planted} {legacy}:", proc.stdout)
        self.assertEqual(calls, [])

    def plant_rust_home(self):
        """What a job could leave in RUNNER_TEMP: a complete toolchain, a rustup and a config."""
        planted = self.rust_home
        toolchain = planted / "rustup" / "toolchains" / f"{TOOLCHAIN}-{HOST}" / "bin"
        toolchain.mkdir(parents=True)
        for tool in ("rustc", "cargo", "rustfmt", "cargo-clippy"):
            (toolchain / tool).write_text(TROJAN)
            (toolchain / tool).chmod(0o755)
        (planted / "cargo" / "bin").mkdir(parents=True)
        (planted / "cargo" / "bin" / "rustup").write_text(TROJAN)
        (planted / "cargo" / "bin" / "rustup").chmod(0o755)
        (planted / "cargo" / "config.toml").write_text('[build]\nrustc-wrapper = "/tmp/evil"\n')
        return toolchain

    def test_a_rust_home_an_earlier_job_left_is_replaced_not_used(self):
        toolchain = self.plant_rust_home()
        if shutil.which("chflags"):
            # Immutable, as the runner's cleanup cannot delete it.
            subprocess.run(["chflags", "uchg", str(toolchain / "rustc")], check=True)
        proc, calls = self.run_step()
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertIn(f"::warning::Removing {self.rust_home}, which an earlier job left behind.", proc.stdout)
        self.assertTrue(calls[0].startswith("rustup-init --no-modify-path"), calls)
        self.assertIn(f"--host {HOST}", self.mirror_log.read_text())
        self.assertFalse((self.rust_home / "cargo" / "config.toml").exists())
        self.assert_shared_homes_untouched(proc)

    @unittest.skipIf(hasattr(os, "geteuid") and os.geteuid() == 0, "root can remove anything")
    def test_a_rust_home_that_cannot_be_removed_fails_closed(self):
        self.plant_rust_home()
        self.runner_temp.chmod(0o555)
        proc, calls = self.run_step()
        self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
        self.assertIn(f"::error::Could not create {self.rust_home} afresh", proc.stdout)
        self.assertEqual(calls, [])
        self.assertEqual(self.github_env_values(), {})
        self.assert_shared_homes_untouched(proc)

    def test_a_second_setup_rust_in_the_job_keeps_its_homes(self):
        proc, _ = self.run_step()
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        homes = {key: self.github_env_values()[key] for key in ("CARGO_HOME", "RUSTUP_HOME")}
        # setup-nextest put cargo-nextest in the job's bin between the two.
        nextest = self.rust_home / "cargo" / "bin" / "cargo-nextest"
        nextest.write_text("#!/bin/sh\n")
        proc, calls = self.run_step(**homes)
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertIn(f"Keeping this job's Rust homes in {self.rust_home}.", proc.stdout)
        self.assertTrue(nextest.exists())
        self.assertFalse(any(call.startswith("rustup-init") for call in calls), calls)
        self.assertEqual(self.mirror_log.read_text(), "")
        self.assertIn(f"Toolchain {TOOLCHAIN} already available", proc.stdout)

    def test_a_mac_runner_that_does_not_say_where_it_runs_is_isolated_too(self):
        proc, calls = self.run_step(RUNNER_ENVIRONMENT=None)
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertTrue(calls[0].startswith("rustup-init "), calls)
        self.assertEqual(self.github_env_values()["CARGO_HOME"], str(self.rust_home / "cargo"))
        self.assert_shared_homes_untouched(proc)

    def test_github_hosted_macs_and_linux_runners_keep_their_homes(self):
        # A home with a rustup already in it, as a hosted image or a pod has.
        own = self.dir / "own"
        (own / "cargo" / "bin").mkdir(parents=True)
        shutil.copy(self.stub_rustup, own / "cargo" / "bin" / "rustup")
        for tool in ("cargo", "rustc"):
            (own / "cargo" / "bin" / tool).write_text(f"#!/bin/sh\necho '{tool} stand-in'\n")
        for tool in (own / "cargo" / "bin").iterdir():
            tool.chmod(0o755)
        homes = {"CARGO_HOME": str(own / "cargo"), "RUSTUP_HOME": str(own / "rustup")}
        for runner in ({"RUNNER_ENVIRONMENT": "github-hosted"}, {"RUNNER_OS": "Linux"}):
            with self.subTest(**runner):
                proc, calls = self.run_step(**homes, **runner)
                self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
                self.assertFalse(any(call.startswith("rustup-init") for call in calls), calls)
                self.assertNotIn("CARGO_HOME", self.github_env_values())
                self.assertEqual(self.github_path.read_text().splitlines(), [str(own / "cargo" / "bin")])
                self.assertFalse(self.rust_home.exists())

    def linux_pod(self):
        """A Linux pod's step: its own empty HOME, no CARGO_HOME or RUSTUP_HOME, no rustup."""
        pod_home = self.dir / "pod-home"
        pod_home.mkdir(exist_ok=True)
        return pod_home, {"RUNNER_OS": "Linux", "HOME": str(pod_home), "CARGO_HOME": None, "RUSTUP_HOME": None}

    def test_a_runner_without_rustup_installs_it_from_the_pinned_rustup_init(self):
        pod_home, env = self.linux_pod()
        proc, calls = self.run_step(**env)
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertEqual(calls[0], "rustup-init --no-modify-path -y --default-toolchain none CARGO_HOME= RUSTUP_HOME=")
        self.assertIn(f"rustup-init: verified from file://{self.dir}/dist/", proc.stdout)
        self.assertTrue((pod_home / ".cargo" / "bin" / "rustup").is_file())
        self.assertEqual(self.github_path.read_text().splitlines(), [str(pod_home / ".cargo" / "bin")])
        self.assertNotIn("CARGO_HOME", self.github_env_values())
        self.assertFalse(self.rust_home.exists())
        self.assertNotIn("sh.rustup.rs", proc.stdout + proc.stderr)
        self.assert_shared_homes_untouched(proc)

    def test_a_runner_without_rustup_fails_closed_on_a_mismatched_pin(self):
        self.write_pins({HOST: sha256(b"some other rustup-init")})
        pod_home, env = self.linux_pod()
        proc, calls = self.run_step(**env)
        self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
        self.assertIn("does not match the pinned SHA-256 of rustup-init", proc.stdout)
        self.assertIn("::error::Could not get rustup-init 9.9.9", proc.stdout)
        self.assertEqual(calls, [])
        self.assertFalse((pod_home / ".cargo" / "bin" / "rustup").exists())


if __name__ == "__main__":
    unittest.main()
