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
# the outer root claims the package, and the resolution fails.
#
# A package a workspace member path-depends on is resolved as part of walking
# that member, so the failure is not local to it: it aborts `cargo fmt --all`,
# and with it `make lint` and `make signoff`, for the whole tree (#13093). A
# package nothing depends on fails only when addressed directly, but it is the
# same defect one dependency edge away from the same outcome.
#
# The check is on the DECLARATION, not on whether `cargo metadata` currently
# resolves: in a plain checkout the root `exclude` does match, so a resolution
# probe passes on exactly the manifests this guard exists to catch.
#
# Membership is read from the ROOT workspace only. A legitimate nested
# sub-workspace would have its own members flagged here; none exists today, and
# the remedy if one appears is to union in the members of every manifest that
# declares a workspace of its own.
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


def find_violations(manifests: dict[Path, dict], members: set[Path]) -> list[Path]:
    """Non-member packages that leave their workspace root to an ancestor lookup.

    The root manifest needs no special case: a workspace root declares
    `[workspace]` by definition, and this repository's is virtual besides.
    """
    return sorted(
        path
        for path, manifest in manifests.items()
        if path not in members
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
    """Manifest paths of the root workspace's members, from cargo itself.

    `--locked` because this is a fast, side-effect-free lint guard, and it opens
    `lint-rust`: it should fail on a stale `Cargo.lock` rather than rewrite one.
    """
    try:
        out = subprocess.run(
            ["cargo", "metadata", "--no-deps", "--locked", "--format-version", "1"],
            cwd=REPO,
            capture_output=True,
            check=True,
            text=True,
        ).stdout
    except FileNotFoundError:
        print(
            "error: `cargo` not found on PATH — is the Rust toolchain installed?",
            file=sys.stderr,
        )
        raise SystemExit(2)
    except subprocess.CalledProcessError as e:
        print(f"error: `cargo metadata` failed:\n{e.stderr}", file=sys.stderr)
        raise SystemExit(2)
    try:
        return {Path(p["manifest_path"]) for p in json.loads(out)["packages"]}
    except (json.JSONDecodeError, KeyError) as e:
        print(f"error: could not read `cargo metadata` output: {e}", file=sys.stderr)
        raise SystemExit(2)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Check that every package outside the root workspace declares its own."
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="print every tracked package and which workspace root owns it",
    )
    args = parser.parse_args()

    members = workspace_members()
    manifests = {p: read_manifest(p) for p in tracked_manifests()}
    violations = find_violations(manifests, members)

    if args.list:
        # Classified by the same predicate the check uses, so the listing cannot
        # disagree with the verdict.
        undeclared = set(violations)
        for path, manifest in sorted(manifests.items()):
            if not is_package(manifest):
                continue
            if path in members:
                owner = "root workspace"
            elif path in undeclared:
                owner = "UNDECLARED"
            else:
                owner = "itself"
            print(f"{owner:<15} {path.relative_to(REPO).as_posix()}")
        return 0

    if not violations:
        outside = sum(
            1
            for path, manifest in manifests.items()
            if is_package(manifest) and path not in members
        )
        print(
            f"workspace membership OK: {len(manifests)} tracked manifest(s), "
            f"{outside} outside the root workspace, each declaring its own."
        )
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
