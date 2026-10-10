#!/usr/bin/env python3
# Copyright 2024-2026 The Spice.ai OSS Authors
"""Exercise signoff coverage with real Git, Make, and small Rust test binaries."""

from __future__ import annotations

import json
import os
import re
import shlex
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
SUBJECT = REPO / "scripts/signoff"


class SignoffCoverageTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="signoff-coverage-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.env = os.environ.copy()
        for name in (
            "MAKEFLAGS", "GNUMAKEFLAGS", "MFLAGS", "MAKEOVERRIDES", "MAKEFILES", "RUSTC_WRAPPER",
            "RUSTC_WORKSPACE_WRAPPER", "CARGO_TARGET_DIR", "NEXTEST_PROFILE",
            "NEXTEST_RETRIES", "NEXTEST_NO_TESTS", "NEXTEST_FLAG", "NEXTEST_FILTER_EXTRA",
            "SIGNOFF_REMOTE_RUN", "SIGNOFF_STEP_BUDGET_MINUTES",
        ):
            self.env.pop(name, None)
        self.env.update(SIGNOFF_ACTOR="audit", SIGNOFF_SKIP_TARGETED_LINT="1",
                        SIGNOFF_SKIP_TARGETED_TESTS="1", SIGNOFF_STATUS_POST_BACKOFF="")
        self.run_ok("git", "init", "-q", "-b", "trunk")
        self.run_ok("git", "config", "user.name", "signoff test")
        self.run_ok("git", "config", "user.email", "signoff@example.invalid")
        self.write(".gitignore", "target/\nstatus\nrefresh\nprobe.sh\n")
        self.write("crates/probe/src/expected.snap", "expected\n")
        self.write("crates/probe/src/lib.rs", '''#[test]
fn snapshot_matches() {
    assert_eq!(include_str!("expected.snap").trim(), "expected");
}
''')
        self.write("Makefile", """.PHONY: lint-rust nextest verify-cli
lint-rust:
	@mkdir -p target
	rustc --test crates/probe/src/lib.rs -o target/probe
nextest:
	./target/probe
verify-cli:
	@echo fixture verification completed
""")
        self.commit()
        self.run_ok("git", "checkout", "-q", "-b", "change")

    def write(self, path, text):
        dest = self.root / path
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(text)

    def run_command(self, *args, env=None, stdin=None):
        return subprocess.run(args, cwd=self.root, env=self.env | (env or {}),
                              input=stdin, text=True, stdout=subprocess.PIPE,
                              stderr=subprocess.STDOUT, timeout=90)

    def run_ok(self, *args, stdin=None):
        result = self.run_command(*args, stdin=stdin)
        self.assertEqual(result.returncode, 0, result.stdout)
        return result.stdout.strip()

    def commit(self):
        # A private fixture has no remote, hook, or signing configuration.
        self.run_ok("git", "add", ".")
        tree = self.run_ok("git", "write-tree")
        parent = self.run_command("git", "rev-parse", "--verify", "HEAD")
        args = ["git", "commit-tree", tree]
        if parent.returncode == 0:
            args += ["-p", parent.stdout.strip()]
        commit = self.run_ok(*args, stdin="fixture\n")
        self.run_ok("git", "update-ref", "HEAD", commit)

    def signoff(self, env=None):
        # Keep the check runner and Make recipes intact. Replace only GitHub
        # transport and the pushed-branch precondition; nothing leaves this fixture.
        script = '''source "$1"
VCS=git
require_tools() { :; }
ensure_clean_and_pushed() { :; }
repo_slug() { echo audit/fixture; }
post_commit_status() { printf '%s\\n' "$3" > status; }
refresh_attestation_check() { printf '%s\\n' "$*" > refresh; }
cmd_signoff
'''
        return self.run_command("bash", "-c", script, "test", str(SUBJECT), env=env)

    def assert_rejected(self, result):
        self.assertEqual(result.returncode, 1, result.stdout)
        self.assertTrue((self.root / "status").exists(), result.stdout)
        self.assertEqual((self.root / "status").read_text().strip(), "failure", result.stdout)

    def test_snapshot_input_runs_the_failing_test(self):
        initial = self.run_command("make", "lint-rust", "nextest")
        self.assertEqual(initial.returncode, 0, initial.stdout)
        self.write("crates/probe/src/expected.snap", "incorrect\n")
        self.commit()
        result = self.signoff()
        self.assert_rejected(result)
        self.assertIn('left: "incorrect"', result.stdout)
        self.assertIn('right: "expected"', result.stdout)

    def test_root_fixture_input_runs_the_failing_test(self):
        # `crates/runtime-tls` embeds `test/tls/spiced_cert.pem`, outside every
        # workspace tree. A branch changing only that file must still run the
        # test that embeds it, so the trunk side carries the source and fixture.
        self.run_ok("git", "checkout", "-q", "trunk")
        self.write("test/tls/spiced_cert.pem", "expected\n")
        self.write("crates/probe/src/lib.rs", '''#[test]
fn certificate_matches() {
    assert_eq!(include_str!("../../../test/tls/spiced_cert.pem").trim(), "expected");
}
''')
        self.commit()
        self.run_ok("git", "checkout", "-q", "-B", "change")
        self.write("test/tls/spiced_cert.pem", "incorrect\n")
        self.commit()
        self.assertEqual(self.run_ok("git", "diff", "--name-only", "trunk", "HEAD"),
                         "test/tls/spiced_cert.pem")
        result = self.signoff()
        self.assert_rejected(result)
        self.assertIn('left: "incorrect"', result.stdout)
        self.assertIn('right: "expected"', result.stdout)

    def test_rename_out_of_rust_source_runs_checks(self):
        self.run_ok("git", "mv", "crates/probe/src/lib.rs", "removed.txt")
        self.commit()
        self.assertIn("R100", self.run_ok("git", "diff", "--name-status", "trunk", "HEAD"))
        result = self.signoff()
        self.assert_rejected(result)
        self.assertIn("crates/probe/src/lib.rs", result.stdout)

    def test_jj_rename_checks_both_paths(self):
        self.run_ok("git", "mv", "crates/probe/src/lib.rs", "removed.txt")
        self.commit()
        # Both Git and JJ name-only diffs emit the destination of a rename.
        # Use real Git rename detection as the transport so this guard needs no
        # JJ installation. The gate still takes its JJ branch and runs Make.
        self.assertEqual(self.run_ok("git", "diff", "--name-only", "--find-renames",
                                     "trunk", "HEAD"), "removed.txt")
        script = '''source "$1"
VCS=jj
resolve_local_trunk_ref() { echo trunk; }
jj() {
    local from="" to=""
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --from) from="$2"; shift 2 ;;
            --to) to="$2"; shift 2 ;;
            *) shift ;;
        esac
    done
    git diff --name-only --find-renames "$from" "$to"
}
changed_files_vs_trunk HEAD
run_checks HEAD
'''
        result = self.run_command("bash", "-c", script, "test", str(SUBJECT))
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn("crates/probe/src/lib.rs", result.stdout)
        self.assertIn("removed.txt", result.stdout)
        self.assertIn("rustc --test crates/probe/src/lib.rs", result.stdout)

    def test_local_failure_invalidates_previous_success(self):
        self.write("crates/probe/src/lib.rs", "#[test] fn wrong_rows() { assert_eq!(2, 1); }\n")
        self.commit()
        self.write("status", "success\n")
        result = self.signoff()
        self.assert_rejected(result)
        self.assertIn("wrong_rows ... FAILED", result.stdout)
        self.assertTrue((self.root / "refresh").read_text().strip().endswith(" force"))

    def test_make_dry_run_cannot_attest(self):
        self.write("crates/probe/src/lib.rs", "#[test] fn wrong_rows() { assert_eq!(2, 1); }\n")
        self.commit()
        # GNU make 4 also reads its flags from GNUMAKEFLAGS, but macOS ships
        # make 3.81, which ignores it. Passing them on make's command line gives
        # every host the 4.x behavior, so this case fails wherever it runs.
        shim = self.root / "target/gnu-make/make"
        self.write(str(shim.relative_to(self.root)),
                   f'#!/bin/sh\nexec {shlex.quote(shutil.which("make"))} $GNUMAKEFLAGS "$@"\n')
        shim.chmod(0o755)
        gnu_make = {"PATH": f"{shim.parent}{os.pathsep}{self.env['PATH']}"}
        for controls in ({"MAKEFLAGS": "n"}, {"GNUMAKEFLAGS": "-n"} | gnu_make):
            with self.subTest(controls=sorted(controls)):
                (self.root / "status").unlink(missing_ok=True)
                result = self.signoff(controls)
                self.assert_rejected(result)
                self.assertIn("wrong_rows ... FAILED", result.stdout)

    def test_diff_error_cannot_skip_checks(self):
        self.write("crates/probe/src/lib.rs", "#[test] fn wrong_rows() { assert_eq!(2, 1); }\n")
        self.commit()
        self.write("target/broken-index", "")
        result = self.signoff({"GIT_INDEX_FILE": str(self.root / "target/broken-index")})
        self.assert_rejected(result)
        self.assertIn("wrong_rows ... FAILED", result.stdout)

    def test_compare_api_requires_complete_paths(self):
        # Exercise the actual jq expression on API-shaped data. Replace only
        # transport, so count and rename parsing are the shipped implementation.
        script = '''source "$1"
repo_slug() { echo audit/fixture; }
gh() { jq -r "$4" compare.json; }
github_compare_files HEAD
'''
        cases = (
            ({"files": [{"filename": "docs/example.md"}]}, 0, "docs/example.md\n"),
            ({"files": []}, 0, ""),
            ({"files": [{"filename": "docs/moved.txt", "previous_filename": "src/lib.rs"}]},
             0, "docs/moved.txt\nsrc/lib.rs\n"),
            ({"files": [{"filename": "docs/example.md"}] * 300}, 1, ""),
            ({}, 1, None),
        )
        for payload, expected_status, expected_output in cases:
            with self.subTest(count=len(payload.get("files", [])), expected_status=expected_status):
                self.write("compare.json", json.dumps(payload))
                result = self.run_command("bash", "-c", script, "test", str(SUBJECT))
                self.assertEqual(result.returncode, expected_status, result.stdout)
                if expected_output is not None:
                    self.assertEqual(result.stdout, expected_output)

    def test_nextest_filter_cannot_hide_failure(self):
        # Run the repository's actual nextest + verify-cli recipes. Only workspace
        # selection and lint are adapted to this dependency-free external crate.
        makefile = re.sub(r"(?m)^NEXTEST_SELECTION :=[^\n]*(?:\n\t[^\n]*)*",
                          "NEXTEST_SELECTION := --workspace", (REPO / "Makefile").read_text())
        makefile = re.sub(r"(?m)^NEXTEST_FILTER :=.*$", "NEXTEST_FILTER := kind(=lib)", makefile)
        self.write("Makefile", makefile + """
lint-rust:
	@echo fixture lint
""")
        self.write("Cargo.toml", '''[package]
name = "spice"
version = "0.1.0"
edition = "2024"
[profile.lint]
inherits = "dev"
''')
        self.write("src/lib.rs", """#[test] fn harmless_pass() { assert_eq!(1, 1); }
#[test] fn deterministic_failure() { assert_eq!(2, 1); }
""")
        self.write("src/main.rs", 'fn main() { println!("spice 0.1.0"); }\n')
        self.write("tests/cli.rs", "#[test] fn cli() {}\n")
        self.write(".config/nextest.toml", "[profile.default]\nretries = 0\n")
        self.write("version.txt", "0.1.0\n")
        self.write("scripts/verify_cli_build.py", (REPO / "scripts/verify_cli_build.py").read_text())
        self.run_ok("cargo", "generate-lockfile", "--offline")
        self.commit()
        for controls in (
            {"NEXTEST_FILTER_EXTRA": "test(=harmless_pass)"},
            {"NEXTEST_FLAG": "harmless_pass"},
            {"MAKEFLAGS": "-- NEXTEST_FILTER_EXTRA=test(=harmless_pass)"},
        ):
            with self.subTest(controls=controls):
                result = self.signoff(controls)
                self.assert_rejected(result)
                self.assertIn("deterministic_failure", result.stdout)
                self.assertIn("1 passed, 1 failed", result.stdout)

    def test_docs_only_still_skips_rust(self):
        self.write("docs/example.md", "Documentation only.\n")
        self.commit()
        result = self.signoff()
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertEqual((self.root / "status").read_text().strip(), "success")
        self.assertIn("no Rust changes (lint/test skipped)", result.stdout)
        self.assertFalse((self.root / "target").exists())

    def test_gate_features_match(self):
        result = subprocess.run(
            ["make", "--no-print-directory", "-n", "lint-rust", "nextest", "verify-cli"],
            cwd=REPO, env=self.env, text=True, stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, timeout=30,
        )
        self.assertEqual(result.returncode, 0, result.stdout)
        features = []
        for line in result.stdout.replace("\\\n", " ").splitlines():
            if not any(command in line for command in
                       ("cargo clippy ", "cargo nextest run ", "cargo test --no-run ")):
                continue
            words = shlex.split(line)
            if "cargo" in words and "--features" in words:
                features.append(set(words[words.index("--features") + 1].split(",")))
        self.assertEqual(len(features), 4, result.stdout)
        for selected in features:
            self.assertIn("cayenne/result-correctness-duckdb", selected)
            self.assertIn("rate-control", selected)
            self.assertIn("http-functions", selected)
            self.assertEqual(selected, features[0])


if __name__ == "__main__":
    unittest.main(verbosity=2)
