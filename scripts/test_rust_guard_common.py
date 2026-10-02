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


def run_with_cargo(
    argv: list[str], cargo_body: str | None, mode: int = 0o755
) -> subprocess.CompletedProcess:
    """Run Python with PATH holding only a stand-in `cargo` (or no cargo at all)."""
    with tempfile.TemporaryDirectory() as bin_dir:
        if cargo_body is not None:
            cargo = Path(bin_dir) / "cargo"
            cargo.write_text(f"#!/bin/sh\n{cargo_body}\n", encoding="utf-8")
            cargo.chmod(mode)
        return subprocess.run(
            [sys.executable, *argv],
            cwd=REPO,
            capture_output=True,
            text=True,
            env={**os.environ, "PATH": bin_dir},
        )


def run_helper(cargo_body: str | None, mode: int = 0o755) -> subprocess.CompletedProcess:
    """Call `cargo_metadata()` the way a guard does, printing its `ok` field."""
    call = (
        "import sys; sys.path.insert(0, 'scripts'); "
        "from rust_guard_common import cargo_metadata; print(cargo_metadata()['ok'])"
    )
    return run_with_cargo(["-c", call], cargo_body, mode)


print("cargo_metadata() answering")

result = run_helper('echo \'{"ok": "parsed", "packages": [], "workspace_members": []}\'')
check("valid JSON is returned parsed", (result.returncode, result.stdout.strip()), (0, "parsed"))

result = run_helper(
    'echo "{\\"ok\\": \\"$*\\", \\"packages\\": [], \\"workspace_members\\": []}"'
)
check(
    "cargo is asked for the workspace only, never resolving dependencies",
    result.stdout.split(),
    ["metadata", "--format-version", "1", "--no-deps", "--locked"],
)

# Every way cargo can fail to answer: (label, stand-in body or None for no
# cargo at all, its file mode, what the error must say).
FAILURE_MODES = (
    ("output that is not JSON", "echo 'warning: not json'", 0o755, "emitted invalid JSON"),
    ("JSON that is not an object", "echo '[]'", 0o755, "workspace layout cannot be read"),
    ("an object without packages", "echo '{}'", 0o755, "workspace layout cannot be read"),
    (
        "packages that is not an array",
        'echo \'{"packages": {}, "workspace_members": []}\'',
        0o755,
        "workspace layout cannot be read",
    ),
    (
        "a package that is not an object",
        'echo \'{"packages": [null], "workspace_members": []}\'',
        0o755,
        "workspace layout cannot be read",
    ),
    (
        "a workspace member that is not a string",
        'echo \'{"packages": [], "workspace_members": [null]}\'',
        0o755,
        "workspace layout cannot be read",
    ),
    (
        "no workspace_members array",
        'echo \'{"packages": []}\'',
        0o755,
        "workspace layout cannot be read",
    ),
    ("a failing cargo", "echo 'error: bad manifest' >&2; exit 101", 0o755, "bad manifest"),
    ("no cargo on PATH", None, 0o755, "Rust toolchain installed"),
    ("a cargo that cannot be executed", "echo '{}'", 0o644, "could not be run"),
    ("stdout that is not valid text", "printf '\\377'", 0o755, "could not be decoded"),
    (
        "stderr that is not valid text",
        "printf '{}'; printf '\\377' >&2; exit 101",
        0o755,
        "could not be decoded",
    ),
)

# Regression test for #13121: each guard, not just the helper, must report a
# cargo it cannot read as a tooling error.
targets = [("cargo_metadata() failing", None)]
targets += [(g, str(REPO / "scripts" / g)) for g in CARGO_METADATA_GUARDS]
for target, script in targets:
    print(target)
    for label, body, mode, says in FAILURE_MODES:
        if script is None:
            result = run_helper(body, mode)
        else:
            result = run_with_cargo([script], body, mode)
        check(f"{label} exits 2, not 1", result.returncode, 2)
        check("  ...and names the cause", says in result.stderr, True)
        check("  ...without a traceback", "Traceback" in result.stderr, False)

if failures:
    print(f"\n{failures} of {checks} checks FAILED")
    raise SystemExit(1)
print(f"\nall {checks} checks passed")
