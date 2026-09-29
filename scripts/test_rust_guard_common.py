#!/usr/bin/env python3
# Copyright 2024-2026 The Spice.ai OSS Authors
#
# Tests for scripts/rust_guard_common.py, and for the exit code each guard that
# calls it reports when cargo cannot answer.
#
# Every guard exits 1 only for an actual violation and 2 when it could not check
# at all. A guard that parsed `cargo metadata` output itself exited 1 with a
# traceback on output that was not JSON, reporting a broken toolchain as a
# layering violation (#13121). Each case here runs the real script against a
# stand-in `cargo` on PATH, so it exercises the same subprocess call the guard
# makes.
#
# Run: python3 scripts/test_rust_guard_common.py

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent

# The guards that read the workspace through `cargo_metadata()`.
CARGO_METADATA_GUARDS = ("check_crate_layers.py", "check_module_reachability.py")

failures = 0
checks = 0


def check(name: str, got, want) -> None:
    global failures, checks
    checks += 1
    if got == want:
        print(f"  ok: {name}")
    else:
        failures += 1
        print(f"  FAIL: {name}\n    got:  {got!r}\n    want: {want!r}")


def run_with_cargo(argv: list[str], cargo_body: str | None) -> subprocess.CompletedProcess:
    """Run Python with PATH holding only a stand-in `cargo` (or no cargo at all)."""
    with tempfile.TemporaryDirectory() as bin_dir:
        if cargo_body is not None:
            cargo = Path(bin_dir) / "cargo"
            cargo.write_text(f"#!/bin/sh\n{cargo_body}\n", encoding="utf-8")
            cargo.chmod(0o755)
        return subprocess.run(
            [sys.executable, *argv],
            cwd=REPO,
            capture_output=True,
            text=True,
            env={**os.environ, "PATH": bin_dir},
        )


def run_helper(cargo_body: str | None) -> subprocess.CompletedProcess:
    """Call `cargo_metadata()` the way a guard does, printing its `ok` field."""
    return run_with_cargo(
        [
            "-c",
            "import sys; sys.path.insert(0, 'scripts'); "
            "from rust_guard_common import cargo_metadata; print(cargo_metadata()['ok'])",
        ],
        cargo_body,
    )


print("cargo_metadata")

result = run_helper('echo \'{"ok": "parsed"}\'')
check("valid JSON is returned parsed", (result.returncode, result.stdout.strip()), (0, "parsed"))

result = run_helper("echo 'warning: not json'")
check("output that is not JSON exits 2", result.returncode, 2)
check("  ...and names the cause", "emitted invalid JSON" in result.stderr, True)
check("  ...without a traceback", "Traceback" in result.stderr, False)

result = run_helper("echo 'error: failed to parse manifest' >&2; exit 101")
check("a failing cargo exits 2", result.returncode, 2)
check("  ...and shows cargo's own diagnostics", "failed to parse manifest" in result.stderr, True)

result = run_helper(None)
check("no cargo on PATH exits 2", result.returncode, 2)
check("  ...and points at the toolchain", "Rust toolchain installed" in result.stderr, True)

result = run_helper('echo "{\\"ok\\": \\"$*\\"}"')
check(
    "cargo is asked for the workspace only, never resolving dependencies",
    result.stdout.split(),
    ["metadata", "--format-version", "1", "--no-deps", "--locked"],
)

# Regression test for #13121: each guard, not just the helper, must report a
# cargo it cannot read as a tooling error.
for guard in CARGO_METADATA_GUARDS:
    print(guard)
    for label, body in (
        ("output that is not JSON", "echo 'warning: not json'"),
        ("a failing cargo", "exit 101"),
        ("no cargo on PATH", None),
    ):
        result = run_with_cargo([str(REPO / "scripts" / guard)], body)
        check(f"{label} exits 2, not 1", result.returncode, 2)
        check("  ...without a traceback", "Traceback" in result.stderr, False)


if failures:
    print(f"\n{failures} of {checks} checks FAILED")
    raise SystemExit(1)
print(f"\nall {checks} checks passed")
