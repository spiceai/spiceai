#!/usr/bin/env python3
"""Tests for check_ci_cache_pins.py, run on a copy of the files it reads.

Each test copies the checker, pins.json, rust-toolchain.toml, Dockerfile-cuda, the
install-cargo-deny, setup-rust and setup-nextest actions, and every file that downloads a pin
through fetch_pinned.sh, into a temporary tree laid out like the repository, changes one of
them, and runs the checker there as CI does.
"""

import json
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
FILES = [
    ".github/scripts/check_ci_cache_pins.py",
    ".github/ci-cache/pins.json",
    ".github/actions/install-cargo-deny/action.yml",
    ".github/actions/setup-rust/action.yml",
    ".github/actions/setup-nextest/action.yml",
    "Dockerfile-cuda",
    "rust-toolchain.toml",
]
sys.path.insert(0, str(ROOT / ".github" / "scripts"))
from check_ci_cache_pins import FETCHED_PINS  # noqa: E402

FILES += sorted({user for users in FETCHED_PINS.values() for user in users})


class CheckCiCachePinsTest(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.tmp)
        for name in FILES:
            (self.tmp / name).parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(ROOT / name, self.tmp / name)

    def run_check(self):
        return subprocess.run(
            [sys.executable, str(self.tmp / ".github/scripts/check_ci_cache_pins.py")],
            capture_output=True,
            text=True,
            check=False,
        )

    def edit_pins(self, change):
        path = self.tmp / ".github/ci-cache/pins.json"
        pins = json.loads(path.read_text())
        change(pins)
        path.write_text(json.dumps(pins))

    def test_the_repository_pins_pass(self):
        result = self.run_check()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("OK: .github/ci-cache/pins.json pins Rust", result.stdout)

    def test_every_target_the_action_installs_must_be_pinned(self):
        self.edit_pins(lambda pins: pins["cargo-deny"]["sha256"].pop("aarch64-apple-darwin"))
        result = self.run_check()
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("cargo-deny.sha256 has no pin for aarch64-apple-darwin", result.stdout)

    def test_an_action_with_no_target_arms_fails(self):
        action = self.tmp / ".github/actions/install-cargo-deny/action.yml"
        action.write_text("name: Install cargo-deny\nruns:\n  using: composite\n  steps: []\n")
        result = self.run_check()
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("Found no `<os>-<arch>) target=<target> ;;` arm", result.stdout)

    def test_an_unpinned_toolchain_fails(self):
        (self.tmp / "rust-toolchain.toml").write_text('[toolchain]\nchannel = "1.0.0"\n')
        result = self.run_check()
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("rust-toolchain.toml names Rust 1.0.0", result.stdout)

    def test_the_repository_pins_rustup_init_for_every_host_setup_rust_maps(self):
        result = self.run_check()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertRegex(result.stdout, r"rustup-init \d+\.\d+\.\d+\.")

    def test_every_host_setup_rust_maps_must_have_a_rustup_init_pin(self):
        for host in ("x86_64-apple-darwin", "aarch64-unknown-linux-gnu"):
            with self.subTest(host=host):
                self.setUp()
                self.edit_pins(lambda pins: pins["rustup-init"]["sha256"].pop(host))
                result = self.run_check()
                self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
                self.assertIn(f"rustup-init.sha256 has no pin for {host}", result.stdout)
                self.assertIn(f"/{host}/rustup-init.sha256", result.stdout)

    def test_a_missing_rustup_init_entry_fails(self):
        self.edit_pins(lambda pins: pins.pop("rustup-init"))
        result = self.run_check()
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("has no rustup-init entry", result.stdout)

    def test_a_malformed_rustup_init_pin_fails(self):
        def change(pins):
            pins["rustup-init"]["version"] = "latest"
            pins["rustup-init"]["url"] = "https://sh.rustup.rs/{version}/{target}"
            pins["rustup-init"]["sha256"]["aarch64-apple-darwin"] = "ABC"

        self.edit_pins(change)
        result = self.run_check()
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("rustup-init.version 'latest' is not a release version.", result.stdout)
        self.assertIn("rustup-init.url must start with https://static.rust-lang.org/rustup/archive/", result.stdout)
        self.assertIn("rustup-init.sha256.aarch64-apple-darwin is not a lowercase hex SHA-256.", result.stdout)

    def test_a_setup_rust_with_no_host_arms_fails(self):
        action = self.tmp / ".github/actions/setup-rust/action.yml"
        action.write_text("name: Sets up the Rust toolchain\nruns:\n  using: composite\n  steps: []\n")
        result = self.run_check()
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("Found no `<os>-<arch>) host_triple=<triple> ;;` arm", result.stdout)


    def test_every_target_setup_nextest_installs_must_be_pinned(self):
        self.edit_pins(lambda pins: pins["cargo-nextest"]["sha256"].pop("universal-apple-darwin"))
        result = self.run_check()
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("cargo-nextest.sha256 has no pin for universal-apple-darwin", result.stdout)

    def test_a_cargo_nextest_pin_off_its_release_page_fails(self):
        def change(pins):
            pins["cargo-nextest"]["url"] = "https://get.nexte.st/{version}/{target}"
            pins["cargo-nextest"]["sha256"]["x86_64-unknown-linux-gnu"] = "ABC"

        self.edit_pins(change)
        result = self.run_check()
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("cargo-nextest.url must start with https://github.com/nextest-rs/nextest/releases/download/", result.stdout)
        self.assertIn("cargo-nextest.sha256.x86_64-unknown-linux-gnu is not a lowercase hex SHA-256.", result.stdout)

    def test_dockerfile_cuda_must_install_the_pinned_rustup_init(self):
        self.edit_pins(lambda pins: pins["rustup-init"]["sha256"].update({"x86_64-unknown-linux-gnu": "1" * 64}))
        result = self.run_check()
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("Dockerfile-cuda has ARG RUSTUP_INIT_SHA256_X86_64=", result.stdout)
        self.edit_pins(lambda pins: pins["rustup-init"].update({"version": "9.9.9"}))
        result = self.run_check()
        self.assertIn("Dockerfile-cuda has ARG RUSTUP_VERSION=", result.stdout)

    # --- pins fetched through fetch_pinned.sh ---------------------------------------------

    def write(self, name, text):
        (self.tmp / name).write_text(text)

    def test_every_fetched_pin_passes(self):
        result = self.run_check()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("OK: fetched through fetch_pinned.sh", result.stdout)
        self.assertIn("mc RELEASE.2025-08-13T08-35-41Z", result.stdout)

    def test_every_target_a_minio_action_asks_for_must_be_pinned(self):
        self.edit_pins(lambda pins: pins["mc"]["sha256"].pop("linux-amd64"))
        result = self.run_check()
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        for user in FETCHED_PINS["mc"]:
            self.assertIn(f"mc.sha256 has no pin for linux-amd64, which {user} asks for", result.stdout)

    def test_a_missing_fetched_entry_fails(self):
        self.edit_pins(lambda pins: pins.pop("mc"))
        result = self.run_check()
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("has no mc entry", result.stdout)

    def test_a_file_that_downloads_a_pinned_tool_itself_fails(self):
        self.write(
            ".github/actions/upload-to-minio/action.yml",
            "name: Upload to MinIO\nruns:\n  using: composite\n  steps:\n    - shell: bash\n"
            "      run: sudo wget https://public-data.spiceai.org/mc -O /usr/local/bin/mc\n",
        )
        result = self.run_check()
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn(
            ".github/actions/upload-to-minio/action.yml does not download mc through .github/scripts/fetch_pinned.sh",
            result.stdout,
        )

    def test_a_target_variable_with_no_case_arm_fails(self):
        path = self.tmp / ".github/actions/setup-minio/action.yml"
        path.write_text(path.read_text().replace("x86_64) target=linux-amd64 ;;", "x86_64) arch=linux-amd64 ;;"))
        result = self.run_check()
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("fetches mc for the target in $target, but has no `<case>) target=<target> ;;` arm", result.stdout)

    def test_a_malformed_fetched_pin_fails(self):
        def change(pins):
            pins["mc"]["version"] = "latest release"
            pins["mc"]["url"] = "http://public-data.spiceai.org/mc"
            pins["mc"]["sha256"]["linux-amd64"] = "ABC"

        self.edit_pins(change)
        result = self.run_check()
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("mc.version 'latest release' is not a release version.", result.stdout)
        self.assertIn("mc.url must be an https URL.", result.stdout)
        self.assertIn("mc.sha256.linux-amd64 is not a lowercase hex SHA-256.", result.stdout)

    def test_a_single_file_pin_takes_no_template_or_target(self):
        def change(pins):
            pins["mc"] = {"url": "https://example.com/{version}/mc", "sha256": "1" * 64}

        self.edit_pins(change)
        result = self.run_check()
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("mc.url names a template, but mc pins one file.", result.stdout)
        self.assertIn("asks for mc for linux-amd64, but mc pins one file for no target.", result.stdout)


    def test_every_target_setup_sccache_asks_for_must_be_pinned(self):
        # The Linux step's `case` arm and the Windows step's `$target`.
        for target in ("x86_64-unknown-linux-musl.tar.gz", "x86_64-pc-windows-msvc.zip"):
            with self.subTest(target=target):
                self.setUp()
                self.edit_pins(lambda pins: pins["sccache"]["sha256"].pop(target))
                result = self.run_check()
                self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
                self.assertIn(
                    f"sccache.sha256 has no pin for {target}, which .github/actions/setup-sccache/action.yml asks for",
                    result.stdout,
                )


    def test_every_platform_install_protoc_supports_must_be_pinned(self):
        result = self.run_check()
        self.assertIn("protoc 36.2", result.stdout)
        for target in ("linux-aarch_64", "osx-aarch_64", "win64", "win32"):
            with self.subTest(target=target):
                self.setUp()
                self.edit_pins(lambda pins: pins["protoc"]["sha256"].pop(target))
                result = self.run_check()
                self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
                self.assertIn(
                    f"protoc.sha256 has no pin for {target}, which .github/actions/install-protoc/action.yml asks for",
                    result.stdout,
                )

    def test_install_protoc_takes_no_release_from_the_releases_api(self):
        action = self.tmp / ".github/actions/install-protoc/action.yml"
        self.assertNotIn("api.github.com", action.read_text())
        self.assertNotIn("releases/latest", action.read_text())


if __name__ == "__main__":
    unittest.main()
