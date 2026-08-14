#!/usr/bin/env python3
# Copyright 2024-2026 The Spice.ai OSS Authors
#
# Workspace-membership guard.
#
# Every package in the tree must be reachable from exactly one workspace root.
# A package that is NOT a member of the repository's root workspace has to say
# so itself, with an empty `[workspace]` table in its own manifest. Otherwise
# cargo resolves its workspace by walking up the directory tree, and whichever
# ancestor manifest it lands on claims the package:
#
#   error: current package believes it's in a workspace when it's not:
#   current:   .../.claude/worktrees/<name>/crates/data-connectors/connector-nfs/Cargo.toml
#   workspace: .../Cargo.toml
#
# The root manifest's `exclude` list does not prevent this, because it excludes
# a path relative to *itself*. When the repository is checked out inside another
# copy of itself — which is where `git worktree` checkouts under `.claude/`
# live — the nested copy's path does not match the outer root's `exclude` entry,
# the outer root claims the package, and the resolution fails. That aborts
# `cargo metadata`, and with it `cargo fmt --all`, `make lint`, and `make
# signoff` for the whole tree (#13093).
#
# The check is on the DECLARATION, not on whether `cargo metadata` currently
# resolves: in a plain checkout the root `exclude` does match, so a resolution
# probe passes on exactly the manifests this guard exists to catch.
#
# Usage:
#   scripts/check_workspace_membership.py         # validate (exit 1 on violation)
#   scripts/check_workspace_membership.py --list  # print every package and its workspace root
#
# Pure stdlib; no third-party deps. Python 3.11+ for tomllib.

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

try:
    import tomllib  # Python 3.11+
except ModuleNotFoundError:
    # Exit 2 (tooling/config error), never 1 — 1 signals an actual violation.
    print(
        "error: this script needs Python 3.11+ for the stdlib `tomllib` module "
        f"(found {sys.version_info.major}.{sys.version_info.minor}). "
        "Install/select a newer python3 and re-run `make lint-rust`.",
        file=sys.stderr,
    )
    raise SystemExit(2)

REPO = Path(__file__).resolve().parent.parent


def read_manifest(path: Path) -> dict:
    """Parse a manifest, or exit 2 — an unreadable manifest is a tooling error."""
    try:
        with path.open("rb") as f:
            return tomllib.load(f)
    except FileNotFoundError:
        print(f"error: manifest not found at {path}.", file=sys.stderr)
        raise SystemExit(2)
    except tomllib.TOMLDecodeError as e:
        print(f"error: {path} is not valid TOML: {e}", file=sys.stderr)
        raise SystemExit(2)


def is_package(manifest: dict) -> bool:
    """A manifest that defines a package. A virtual manifest defines only a workspace."""
    return "package" in manifest


def declares_workspace(manifest: dict) -> bool:
    """A manifest that is its own workspace root, however it fills the table in."""
    return "workspace" in manifest


def find_violations(
    manifests: dict[Path, dict], members: set[Path], root: Path
) -> list[Path]:
    """Non-member packages that leave their workspace root to an ancestor lookup.

    `manifests` maps a manifest path to its parsed contents, `members` holds the
    manifest paths of the root workspace's members, and `root` is the repository
    root manifest.
    """
    return sorted(
        path
        for path, manifest in manifests.items()
        if path != root
        and path not in members
        and is_package(manifest)
        and not declares_workspace(manifest)
    )


def tracked_manifests() -> list[Path]:
    """Every `Cargo.toml` git tracks, so a nested worktree's copies stay out of it."""
    try:
        out = subprocess.run(
            ["git", "-C", str(REPO), "ls-files", "-z", "--", "*Cargo.toml"],
            capture_output=True,
            check=True,
            text=True,
        ).stdout
    except (OSError, subprocess.CalledProcessError) as e:
        print(f"error: could not list tracked manifests: {e}", file=sys.stderr)
        raise SystemExit(2)
    return [REPO / p for p in out.split("\0") if p]


def workspace_members() -> set[Path]:
    """Manifest paths of the root workspace's members, from cargo itself."""
    try:
        out = subprocess.run(
            ["cargo", "metadata", "--no-deps", "--format-version", "1"],
            cwd=REPO,
            capture_output=True,
            check=True,
            text=True,
        ).stdout
    except FileNotFoundError:
        print("error: `cargo` not found on PATH.", file=sys.stderr)
        raise SystemExit(2)
    except subprocess.CalledProcessError as e:
        print(f"error: `cargo metadata` failed:\n{e.stderr}", file=sys.stderr)
        raise SystemExit(2)
    return {Path(p["manifest_path"]) for p in json.loads(out)["packages"]}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--list",
        action="store_true",
        help="print every tracked package and which workspace root owns it",
    )
    args = parser.parse_args()

    root = REPO / "Cargo.toml"
    members = workspace_members()
    manifests = {p: read_manifest(p) for p in tracked_manifests()}

    if args.list:
        for path in sorted(manifests):
            manifest = manifests[path]
            if not is_package(manifest) and path != root:
                continue
            if path == root or path in members:
                owner = "root workspace"
            elif declares_workspace(manifest):
                owner = "itself"
            else:
                owner = "UNDECLARED"
            print(f"{owner:<15} {path.relative_to(REPO).as_posix()}")
        return 0

    violations = find_violations(manifests, members, root)
    if not violations:
        return 0

    print(
        "error: these packages are not members of the root workspace and do not "
        "declare a workspace of their own, so cargo resolves their workspace by "
        "walking up to an ancestor manifest:",
        file=sys.stderr,
    )
    for path in violations:
        print(f"  {path.relative_to(REPO).as_posix()}", file=sys.stderr)
    print(
        "\nAdd an empty `[workspace]` table to each manifest above, or add the "
        "package to `workspace.members` in the root Cargo.toml. Excluding it via "
        "`workspace.exclude` is not enough: that entry is a path relative to the "
        "root manifest, so it stops matching as soon as the repository is checked "
        "out inside another copy of itself (a `git worktree` under .claude/).",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
