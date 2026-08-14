#!/usr/bin/env python3
# Copyright 2024-2026 The Spice.ai OSS Authors
#
# Tests for scripts/check_workspace_membership.py.
#
# The last case is the one that matters: it rebuilds the nested-checkout shape
# from #13093 with cargo itself, so the guard is pinned to the behaviour it
# exists to prevent rather than to an assumption about it. The live-tree scan
# cannot stand in for that — in a plain checkout the root `exclude` matches and
# every manifest resolves, including the ones this guard rejects.
#
# Run: python3 scripts/test_check_workspace_membership.py

from __future__ import annotations

import subprocess
import sys
import tempfile
import tomllib
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from check_workspace_membership import (  # noqa: E402
    declares_workspace,
    find_violations,
    is_package,
)

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


def parse(source: str) -> dict:
    return tomllib.loads(source)


PACKAGE = '[package]\nname = "p"\nversion = "0.0.0"\nedition = "2024"\n'

print("is_package / declares_workspace")
check("a package manifest is a package", is_package(parse(PACKAGE)), True)
check(
    "a virtual manifest is not a package",
    is_package(parse('[workspace]\nmembers = ["a"]\n')),
    False,
)
check(
    "an empty [workspace] table declares a workspace",
    declares_workspace(parse("[workspace]\n" + PACKAGE)),
    True,
)
check(
    "a [workspace] carrying keys still declares one",
    declares_workspace(parse('[workspace]\nmembers = []\n' + PACKAGE)),
    True,
)
check(
    "[workspace.package] alone does not declare one",
    # Inherited-field tables key off `workspace`, so a manifest that only reads
    # them (`version.workspace = true`) must not be mistaken for a root.
    declares_workspace(parse(PACKAGE + '[dependencies]\nx = { workspace = true }\n')),
    False,
)

print("\nfind_violations")
root = Path("/repo/Cargo.toml")
member = Path("/repo/crates/a/Cargo.toml")
loose = Path("/repo/crates/b/Cargo.toml")
virtual_child = Path("/repo/crates/c/Cargo.toml")

check(
    "a non-member package with no [workspace] is a violation",
    find_violations(
        {root: parse('[workspace]\nmembers = ["crates/a"]\n'), loose: parse(PACKAGE)},
        {member},
        root,
    ),
    [loose],
)
check(
    "the same package is clean once it declares its own workspace",
    find_violations(
        {
            root: parse('[workspace]\nmembers = ["crates/a"]\n'),
            loose: parse("[workspace]\n" + PACKAGE),
        },
        {member},
        root,
    ),
    [],
)
check(
    "a member does not need to declare a workspace",
    find_violations(
        {root: parse('[workspace]\nmembers = ["crates/a"]\n'), member: parse(PACKAGE)},
        {member},
        root,
    ),
    [],
)
check(
    "the root manifest is never its own violation",
    # It defines the workspace every member resolves to, and a virtual root has
    # no `[package]` table to test.
    find_violations({root: parse('[workspace]\nmembers = []\n')}, set(), root),
    [],
)
check(
    "a non-member virtual manifest is not a package, so it is skipped",
    find_violations(
        {
            root: parse('[workspace]\nmembers = ["crates/a"]\n'),
            virtual_child: parse('[workspace]\nmembers = ["d"]\n'),
        },
        {member},
        root,
    ),
    [],
)
check(
    "every violating manifest is reported, not just the first",
    find_violations(
        {
            root: parse('[workspace]\nmembers = []\n'),
            loose: parse(PACKAGE),
            virtual_child: parse(PACKAGE),
        },
        set(),
        root,
    ),
    [loose, virtual_child],
)

print("\nnested checkout (cargo)")


def nested_tree(directory: str, *, package_declares_workspace: bool) -> Path:
    """The #13093 shape: a checkout that excludes a package, inside one that does not.

    Both roots exclude the path `pkg` relative to themselves, so the inner root
    excludes exactly the package below it — the arrangement the repository has
    for `crates/data-connectors/connector-nfs`. Returns the package manifest.
    """
    workspace = '[workspace]\nresolver = "3"\nmembers = []\nexclude = ["pkg"]\n'
    outer = Path(directory) / "outer"
    pkg = outer / "nested" / "pkg"
    (pkg / "src").mkdir(parents=True)
    (outer / "Cargo.toml").write_text(workspace, encoding="utf-8")
    (outer / "nested" / "Cargo.toml").write_text(workspace, encoding="utf-8")
    (pkg / "src" / "lib.rs").write_text("", encoding="utf-8")
    manifest = pkg / "Cargo.toml"
    manifest.write_text(
        ("[workspace]\n\n" if package_declares_workspace else "") + PACKAGE,
        encoding="utf-8",
    )
    return manifest


def resolves(manifest: Path) -> bool:
    """Whether cargo can settle the package's workspace at all."""
    return (
        subprocess.run(
            ["cargo", "metadata", "--no-deps", "--format-version", "1",
             "--manifest-path", str(manifest)],
            capture_output=True,
            text=True,
        ).returncode
        == 0
    )


with tempfile.TemporaryDirectory() as d:
    # Without the declaration the inner root's `exclude` does not end the search:
    # cargo keeps walking up, and the outer root — whose `exclude` names a path
    # one level too shallow to match — claims the package and the resolve fails.
    check(
        "a nested non-member package with no [workspace] does not resolve",
        resolves(nested_tree(d, package_declares_workspace=False)),
        False,
    )

with tempfile.TemporaryDirectory() as d:
    check(
        "the same package resolves once it declares its own workspace",
        resolves(nested_tree(d, package_declares_workspace=True)),
        True,
    )

print()
if failures:
    print(f"{failures} of {checks} checks failed")
    raise SystemExit(1)
print(f"all {checks} checks passed")
