#!/usr/bin/env python3
# Copyright 2024-2026 The Spice.ai OSS Authors
#
# Helpers shared by the no-compile guards that `make lint-rust` runs.
#
# Every guard reports through the same exit codes: 0 clean, 1 an actual
# violation, 2 a tooling or configuration error that kept it from checking at
# all. A helper here exits 2 on its own failures, so a guard that calls it can
# never report a broken toolchain as a violation — or as a clean tree.
#
# Pure stdlib; no third-party deps.

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent


def cargo_metadata() -> dict:
    """The workspace's own packages and targets, as `cargo metadata --no-deps` reports them.

    `--no-deps` skips dependency resolution, so cargo neither reads nor writes
    `Cargo.lock` here; `--locked` keeps that a guarantee rather than a side
    effect of the flag set.
    """
    try:
        out = subprocess.run(
            ["cargo", "metadata", "--format-version", "1", "--no-deps", "--locked"],
            cwd=REPO,
            capture_output=True,
            text=True,
            check=True,
        )
    except FileNotFoundError:
        print(
            "error: `cargo` not found on PATH, so the workspace layout cannot be read. "
            "Is the Rust toolchain installed?",
            file=sys.stderr,
        )
        raise SystemExit(2)
    except OSError as e:
        # A `cargo` that is on PATH but cannot be executed (not executable, a
        # sandbox or noexec mount refusing it).
        print(f"error: `cargo` could not be run: {e}", file=sys.stderr)
        raise SystemExit(2)
    except UnicodeDecodeError as e:
        # `text=True` decodes both streams inside `subprocess.run`, so bytes that are
        # not valid in the locale's encoding fail here, before either is inspected.
        print(f"error: `cargo metadata` output could not be decoded: {e}", file=sys.stderr)
        raise SystemExit(2)
    except subprocess.CalledProcessError as e:
        print(f"error: `cargo metadata` failed (exit {e.returncode}).", file=sys.stderr)
        if e.stderr:
            print(e.stderr.strip(), file=sys.stderr)
        raise SystemExit(2)
    try:
        meta = json.loads(out.stdout)
    except json.JSONDecodeError as e:
        print(f"error: `cargo metadata` emitted invalid JSON: {e}", file=sys.stderr)
        raise SystemExit(2)
    # Valid JSON of the wrong shape is still a tooling error: a guard that indexed
    # it would crash with a traceback (exit 1), and one that defaulted a missing
    # key to empty would check nothing and report a clean tree (exit 0).
    if not (
        isinstance(meta, dict)
        and isinstance(meta.get("packages"), list)
        and isinstance(meta.get("workspace_members"), list)
        and all(isinstance(pkg, dict) for pkg in meta["packages"])
        and all(isinstance(member, str) for member in meta["workspace_members"])
    ):
        print(
            "error: `cargo metadata` emitted JSON without a `packages` array of objects "
            "and a `workspace_members` array of strings, so the workspace layout cannot "
            "be read.",
            file=sys.stderr,
        )
        raise SystemExit(2)
    return meta
