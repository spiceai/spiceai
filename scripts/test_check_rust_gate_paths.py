#!/usr/bin/env python3
# Copyright 2024-2026 The Spice.ai OSS Authors
#
# Tests for scripts/check_rust_gate_paths.py.
#
# The guard's live-tree scan only covers the paths today's workspace happens to
# contain, so a derivation that stopped working would pass unnoticed on a clean
# tree — the same reason `test_check_module_reachability.py` runs ahead of its
# guard. The cases here pin the source-tree derivation (#13120: `vendor/` held
# 17 tracked `.rs` files matched by no `code_changes` glob, and the guard was
# green) and the glob matcher it depends on.
#
# Run: python3 scripts/test_check_rust_gate_paths.py

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from check_rust_gate_paths import (  # noqa: E402
    coverage_gaps,
    extract_code_change_globs,
    gate_config_errors,
    glob_matches,
    read_patterns,
    rust_source_errors,
    rust_source_trees,
    tracked_files,
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


# A pattern that classifies every `.rs` file as Rust-affecting, like the real one.
RS_PATTERN = {"scripts/signoff": r"\.rs$"}


print("glob_matches")

check(
    "a `dir/**` glob reaches a nested source file",
    glob_matches("vendor/**", "vendor/mysql-common-derive/src/lib.rs"),
    True,
)
check(
    "a `dir/**` glob does not reach a sibling directory",
    glob_matches("vendor/**", "vendored/lib.rs"),
    False,
)
# picomatch descends into a dot-prefixed segment only with `dot: true`, which
# this filter does not set — so a wildcard must never be credited for one.
check(
    "a wildcard does not match a dot-prefixed segment",
    glob_matches("**", ".ci/clippy.toml"),
    False,
)
check(
    "a glob spelling the dot segment out does match",
    glob_matches(".ci/**", ".ci/clippy.toml"),
    True,
)
check(
    "a leading `**/` may match nothing",
    glob_matches("**/clippy.toml", "clippy.toml"),
    True,
)


print("coverage_gaps")

# The single definition of "is this path gated" that both reports read from —
# so a list that stops being checked fails here rather than in one report only.
check(
    "a path missing from both lists is reported by both halves",
    coverage_gaps(["vendor/x.rs", "crates/y.rs"], ["crates/**"], {"scripts/signoff": r"^crates/"}),
    ({"scripts/signoff": ["vendor/x.rs"]}, ["vendor/x.rs"]),
)
check(
    "a fully covered path is in neither half",
    coverage_gaps(["crates/y.rs"], ["crates/**"], RS_PATTERN),
    ({"scripts/signoff": []}, []),
)

check(
    "a config path the globs miss names the merge-queue consequence",
    gate_config_errors([".ci/clippy.toml"], ["crates/**"], RS_PATTERN),
    [
        ".ci/clippy.toml changes what the Rust gate does, but the pattern in scripts/signoff "
        "does not match it — the branch would skip every Rust check.",
        ".ci/clippy.toml changes what the Rust gate does, but no check-code-changes glob "
        "matches it — the merge queue would report `Rust Lint` and `Build and Test` green "
        "without running a step.",
    ],
)
check(
    "a config path both lists cover is silent",
    gate_config_errors([".ci/clippy.toml"], [".ci/**"], {"scripts/signoff": r"clippy\.toml$"}),
    [],
)


print("rust_source_trees")

check(
    "tracked sources group by top-level directory",
    rust_source_trees(
        [
            "crates/runtime/src/lib.rs",
            "crates/app/src/lib.rs",
            "vendor/x/src/lib.rs",
            "README.md",
            "vendor/x/Cargo.toml",
        ]
    ),
    {
        "crates": ["crates/runtime/src/lib.rs", "crates/app/src/lib.rs"],
        "vendor": ["vendor/x/src/lib.rs"],
    },
)
check("a tree with no Rust sources is absent", rust_source_trees(["docs/dev/ci_signoff.md"]), {})
# No glob covers a bare root-level `.rs`, so this is a shape an error can take —
# and keying it on the filename would report a directory that does not exist.
check("a root-level source keys on the root", rust_source_trees(["build.rs"]), {".": ["build.rs"]})


print("rust_source_errors")

# This is #13120 in miniature: the tree is real, compiled, and in no glob.
ungated = rust_source_errors(
    {"vendor": ["vendor/x/src/lib.rs", "vendor/x/src/error.rs"]},
    ["crates/**", "bin/**"],
    RS_PATTERN,
)
check("an ungated source tree is reported", len(ungated), 1)
check("the error names the tree", "vendor/ holds 2 tracked" in ungated[0], True)
check("the error names an example file", "vendor/x/src/lib.rs" in ungated[0], True)
check(
    "the error says what goes wrong",
    "green without compiling them" in ungated[0],
    True,
)

check(
    "a glob covering the tree clears it",
    rust_source_errors(
        {"vendor": ["vendor/x/src/lib.rs"]}, ["crates/**", "vendor/**"], RS_PATTERN
    ),
    [],
)

# One error per tree, not per file: the fix is a single glob either way, and a
# per-file list would bury it. 200 files must still read as one problem.
many = rust_source_errors(
    {"vendor": [f"vendor/x/src/f{i}.rs" for i in range(200)]}, ["crates/**"], RS_PATTERN
)
check("a large tree still reports one error", len(many), 1)
check("the count is the file count", "holds 200 tracked" in many[0], True)

# The pattern half of the same check: covered by a glob, but a sign-off would
# skip the branch, so the merge queue is green and nothing was ever linted.
pattern_only = rust_source_errors(
    {"vendor": ["vendor/x/src/lib.rs"]},
    ["vendor/**"],
    {"scripts/signoff": r"^crates/"},
)
check("a source tree outside the sign-off pattern is reported", len(pattern_only), 1)
check(
    "that error names the pattern's file",
    "scripts/signoff" in pattern_only[0] and "skip every Rust check" in pattern_only[0],
    True,
)

# Both halves can fail at once, and both must be reported — fixing only the glob
# leaves the branch unsigned-off.
both = rust_source_errors(
    {"vendor": ["vendor/x/src/lib.rs"]}, ["crates/**"], {"scripts/signoff": r"^crates/"}
)
check("a tree failing both lists reports both", len(both), 2)

root = rust_source_errors({".": ["build.rs"]}, ["crates/**"], RS_PATTERN)
check("a root-level source reads as the repo root", "the repo root holds 1" in root[0], True)

check(
    "trees are reported in a stable order",
    [e.split("/", 1)[0] for e in rust_source_errors(
        {"vendor": ["vendor/a.rs"], "attic": ["attic/a.rs"]}, ["crates/**"], RS_PATTERN
    )],
    ["attic", "vendor"],
)


print("live tree")

# Regression test for #13120. The synthetic cases above prove the derivation
# works; this proves it is wired to the lists the repo actually ships.
tracked, notes = tracked_files()
if notes:
    # Not a skip: the guard exits 2 here rather than reporting a green tree it
    # never read, so this harness must not pass on the same condition either.
    failures += 1
    print(f"  FAIL: could not read the tracked files ({notes[0]})")
else:
    patterns = read_patterns()
    globs = extract_code_change_globs()
    if patterns is None or not globs:
        failures += 1
        print("  FAIL: could not read the shipped patterns or globs")
    else:
        trees = rust_source_trees(tracked)
        check("vendor/ is a tracked Rust source tree", "vendor" in trees, True)
        check(
            "every shipped Rust source tree is covered by all three lists",
            rust_source_errors(trees, globs, patterns),
            [],
        )


if failures:
    print(f"\n{failures} of {checks} checks FAILED")
    raise SystemExit(1)
print(f"\nall {checks} checks passed")
