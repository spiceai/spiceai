#!/usr/bin/env python3
# Copyright 2024-2026 The Spice.ai OSS Authors
#
# Assert that the unit-test gate's build produced an up-to-date `spice` CLI.
#
# The gate does not build the CLI separately: `make nextest`'s `--tests` build
# already emits it, because cargo builds a package's bins alongside that package's
# integration tests, and `spice` has three. That is an assumption about cargo's
# target selection which nothing else would notice breaking — remove `spice`'s
# `tests/` targets and its bin silently stops being built.
#
# Checking for the file on disk is not enough: a warm `target/` from an earlier
# build would still hold a stale binary, so the check would pass at exactly the
# moment coverage was lost. Instead this reads cargo's own artifact stream for the
# same selection nextest used. An artifact message means the bin is in that build
# graph, and cargo has just brought it up to date (rebuilding it if it was stale).
import json
import subprocess
import sys
from pathlib import Path


def fail(msg: str) -> None:
    print(f"verify-cli: {msg}", file=sys.stderr)
    sys.exit(1)


def main() -> None:
    if len(sys.argv) != 3:
        fail("usage: verify_cli_build.py <cargo-json-output> <version-file>")
    stream, version_file = Path(sys.argv[1]), Path(sys.argv[2])

    executable = None
    fresh = None
    for line in stream.read_text(errors="replace").splitlines():
        try:
            msg = json.loads(line)
        except ValueError:
            continue  # cargo interleaves non-JSON lines; ignore them
        if msg.get("reason") != "compiler-artifact":
            continue
        target = msg.get("target") or {}
        if target.get("name") != "spice" or target.get("kind") != ["bin"]:
            continue
        # `--tests` builds the bin twice: once for real, once as a `--cfg test`
        # harness. Both are reported with name `spice` and kind `["bin"]`, and only
        # `profile.test` tells them apart — the harness answers `--version` with a
        # libtest error, so matching it would fail this check for the wrong reason.
        if (msg.get("profile") or {}).get("test"):
            continue
        executable = msg.get("executable")
        fresh = msg.get("fresh")

    if not executable:
        fail(
            "the test build produced no `spice` bin artifact.\n"
            "  The gate relies on cargo building a package's bins alongside its integration\n"
            "  tests, so `make nextest` is expected to build the CLI. If spice's tests/\n"
            "  targets were removed, add an explicit `cargo build -p spice` back to the gate."
        )

    path = Path(executable)
    if not path.is_file():
        fail(f"cargo reported {path} but it is not on disk")

    want = f"spice {version_file.read_text().strip()}"
    try:
        got = subprocess.run(
            [str(path), "--version"], capture_output=True, text=True, timeout=120, check=True
        ).stdout.strip()
    except (OSError, subprocess.SubprocessError) as err:
        fail(f"{path} did not run: {err}")

    if got != want:
        fail(f"{path} reports {got!r}, expected {want!r}")

    state = "already up to date" if fresh else "rebuilt by this run"
    print(f"verify-cli: {path} ({state}) runs and reports {got}")


if __name__ == "__main__":
    main()
