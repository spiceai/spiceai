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
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

# Imported before `tomllib` so that on Python < 3.11 the guard's own actionable
# message is what prints. This file runs first in `lint-rust`, so importing
# `tomllib` here directly would front the recipe with a raw ModuleNotFoundError.
from check_workspace_membership import (  # noqa: E402
    declares_workspace,
    find_violations,
    is_package,
)

import tomllib  # noqa: E402

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
    "an inherited `workspace = true` value does not declare one",
    # `workspace = true` on a dependency reads a value from the workspace this
    # package belongs to. It is nested under `dependencies`, so it must not be
    # mistaken for the top-level table that makes a package a root.
    declares_workspace(parse(PACKAGE + '[dependencies]\nx = { workspace = true }\n')),
    False,
)

print("\nfind_violations")
ROOT_MANIFEST = parse('[workspace]\nmembers = ["crates/a"]\n')
root = Path("/repo/Cargo.toml")
member = Path("/repo/crates/a/Cargo.toml")
loose = Path("/repo/crates/b/Cargo.toml")
virtual_child = Path("/repo/crates/c/Cargo.toml")

check(
    "a non-member package with no [workspace] is a violation",
    find_violations({root: ROOT_MANIFEST, loose: parse(PACKAGE)}, {member}),
    [loose],
)
check(
    "the same package is clean once it declares its own workspace",
    find_violations(
        {root: ROOT_MANIFEST, loose: parse("[workspace]\n" + PACKAGE)}, {member}
    ),
    [],
)
check(
    "a member does not need to declare a workspace",
    find_violations({root: ROOT_MANIFEST, member: parse(PACKAGE)}, {member}),
    [],
)
check(
    "a non-member virtual manifest is not a package, so it is skipped",
    find_violations(
        {root: ROOT_MANIFEST, virtual_child: parse('[workspace]\nmembers = ["d"]\n')},
        {member},
    ),
    [],
)
check(
    "every violating manifest is reported, not just the first",
    find_violations(
        {root: ROOT_MANIFEST, loose: parse(PACKAGE), virtual_child: parse(PACKAGE)},
        {member},
    ),
    [loose, virtual_child],
)

print("\nnested checkout (cargo)")


def nested_tree(directory: str, *, package_declares_workspace: bool) -> Path:
    """The #13093 shape: a checkout that excludes a package, inside one that does not.

    Both roots exclude the path `pkg` relative to themselves, so the inner root
    excludes exactly the package below it — the arrangement the repository has
    for `crates/data-connectors/connector-nfs`. A member of the inner workspace
    path-depends on `pkg`, which is what puts the excluded package on the
    dependency walk and so makes `cargo fmt --all` resolve it. Returns the inner
    workspace root.
    """
    outer = Path(directory) / "outer"
    inner = outer / "nested"
    pkg = inner / "pkg"
    app = inner / "app"
    (pkg / "src").mkdir(parents=True)
    (app / "src").mkdir(parents=True)
    # Both roots exclude `pkg` relative to themselves, so only the inner one
    # actually names this package. The outer root must otherwise be a valid,
    # empty workspace: give it a member it does not have and cargo fails on
    # *that* instead, which would pass the negative case for the wrong reason.
    (outer / "Cargo.toml").write_text(
        '[workspace]\nresolver = "3"\nmembers = []\nexclude = ["pkg"]\n',
        encoding="utf-8",
    )
    (inner / "Cargo.toml").write_text(
        '[workspace]\nresolver = "3"\nmembers = ["app"]\nexclude = ["pkg"]\n',
        encoding="utf-8",
    )
    # Already rustfmt-clean, so a formatting difference cannot be mistaken for
    # the resolution failure under test.
    (pkg / "src" / "lib.rs").write_text("pub fn f() {}\n", encoding="utf-8")
    (app / "src" / "lib.rs").write_text("pub fn g() {}\n", encoding="utf-8")
    (app / "Cargo.toml").write_text(
        '[package]\nname = "app"\nversion = "0.0.0"\nedition = "2024"\n\n'
        '[dependencies]\np = { path = "../pkg", optional = true }\n',
        encoding="utf-8",
    )
    (pkg / "Cargo.toml").write_text(
        ("[workspace]\n\n" if package_declares_workspace else "") + PACKAGE,
        encoding="utf-8",
    )
    return inner


def fmt_all(workspace_root: Path) -> tuple[int, str]:
    """Run the gate's opening command, returning its status and combined output.

    This is the harm the issue reports: `cargo fmt --all` aborting for the whole
    tree because one package it path-walks into cannot be resolved.
    """
    p = subprocess.run(
        ["cargo", "fmt", "--all", "--", "--check"],
        cwd=workspace_root,
        capture_output=True,
        text=True,
    )
    return p.returncode, p.stdout + p.stderr


with tempfile.TemporaryDirectory() as d:
    # Without the declaration the inner root's `exclude` does not end the search:
    # cargo keeps walking up, and the outer root — whose `exclude` names a path
    # one level too shallow to match — claims the package. `cargo fmt --all`
    # reaches it through `app`'s path dependency and dies on the whole tree.
    status, output = fmt_all(nested_tree(d, package_declares_workspace=False))
    check("cargo fmt --all fails when the package declares no workspace", status != 0, True)
    # Asserted on the message, not just the status: every other way this fixture
    # could fail (a malformed root, a formatting difference, a missing member)
    # also exits non-zero, and would pass the check above for the wrong reason.
    check(
        "...and it fails on the workspace resolution, not something else",
        "believes it's in a workspace when it's not" in output,
        True,
    )

with tempfile.TemporaryDirectory() as d:
    status, output = fmt_all(nested_tree(d, package_declares_workspace=True))
    # Compared as a pair so a failure reports cargo's own reason rather than
    # just "1 != 0".
    check(
        "the same tree formats once the package declares its own workspace",
        (status, output.strip()[:200]),
        (0, ""),
    )

print()
if failures:
    print(f"{failures} of {checks} checks failed")
    raise SystemExit(1)
print(f"all {checks} checks passed")
