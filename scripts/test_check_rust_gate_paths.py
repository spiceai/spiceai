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
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import check_rust_gate_paths  # noqa: E402
from check_rust_gate_paths import (  # noqa: E402
    coverage_gaps,
    derived_gate_paths,
    extract_code_change_globs,
    gate_config_errors,
    glob_matches,
    read_patterns,
    referenced_inputs,
    rust_source_errors,
    rust_source_trees,
    sibling_imports,
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
# dorny/paths-filter builds its matchers with `{dot: true}`, so a wildcard does
# reach a dot-prefixed segment. Modelling it as excluded would make this guard
# stricter than the filter and report a covered path as ungated.
check(
    "a wildcard reaches a dot-prefixed segment",
    glob_matches("**", ".ci/clippy.toml"),
    True,
)
check(
    "a glob spelling the dot segment out also matches",
    glob_matches(".ci/**", ".ci/clippy.toml"),
    True,
)
check(
    "a leading `**/` reaches a dot-prefixed segment",
    glob_matches("**/clippy.toml", ".ci/clippy.toml"),
    True,
)
check("a wildcard still does not cross a segment boundary", glob_matches("*", "a/b"), False)
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

# Built as named values rather than adjacent literals inside the list: two long
# messages sitting side by side in a list is one missing comma away from
# silently becoming a single element that still compares as "close enough".
PATTERN_MISS = (
    ".ci/clippy.toml changes what the Rust gate does, but the pattern in scripts/signoff "
    + "does not match it — the branch would skip every Rust check."
)
GLOB_MISS = (
    ".ci/clippy.toml changes what the Rust gate does, but no check-code-changes glob "
    + "matches it — the merge queue would report `Rust Lint` and `Build and Test` green "
    + "without running a step."
)
check(
    "a config path the globs miss names the merge-queue consequence",
    gate_config_errors([".ci/clippy.toml"], ["crates/**"], RS_PATTERN),
    [PATTERN_MISS, GLOB_MISS],
)
check(
    "a config path both lists cover is silent",
    gate_config_errors([".ci/clippy.toml"], [".ci/**"], {"scripts/signoff": r"clippy\.toml$"}),
    [],
)


print("rust_source_trees")

fixture_paths = [
    "crates/example/src/snapshots/query.snap",
    "crates/example/tests/data/expected.custom",
    "tools/example/fixtures/input.json",
    "bin/spice/tests/expected.txt",
    "vendor/example/schema.proto",
    "fixtures/root.snap",
]
derived_inputs, _ = derived_gate_paths(fixture_paths)
check("all workspace test inputs are derived regardless of extension",
      sorted(p for p in derived_inputs if p in fixture_paths), sorted(fixture_paths))

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


print("derived_gate_paths")

# The guard reads the real Makefile through `lint_recipe`, so the live-tree run
# below only ever exercises the interpreter spelling today's recipe happens to
# use. Swapping in a synthetic recipe pins the derivation itself: the failure
# this harness exists to catch is a regex that quietly stops deriving a guard
# path, which leaves that path ungated with every check still green.


def derived_from(recipe: str) -> list[str]:
    """Guard paths `derived_gate_paths` reads out of a synthetic `lint-rust` recipe."""
    original = check_rust_gate_paths.lint_recipe
    check_rust_gate_paths.lint_recipe = lambda: recipe
    try:
        # The guards' imports are read from the real files, which would tie these
        # recipe-spelling cases to whatever those files import today; they are
        # pinned separately below.
        paths, _ = derived_gate_paths([], imports=lambda guards: set())
    finally:
        check_rust_gate_paths.lint_recipe = original
    # `RUST_SOURCE_PATHS` is seeded unconditionally and derived from nothing, so
    # dropping it leaves exactly what the recipe contributed.
    return sorted(set(paths) - set(check_rust_gate_paths.RUST_SOURCE_PATHS))


# The recipe invokes the guards through `$(PYTHON)`; a bare `python3` is still
# accepted so a recipe line written the old way keeps deriving. Every gap here is
# one make hands the shell as a separator, confirmed by running each form through
# make: the guard runs in all of them. So a guard written with any of them must
# still derive, or it runs while silently leaving its own path ungated
# (spiceai/spiceai#13783).
GAPS = {"space": " ", "spaces": "   ", "tab": "\t", "space-tab": " \t", "continuation": " \\\n\t\t"}
for spelling in ("$(PYTHON)", "${PYTHON}", "python3"):
    for name, gap in GAPS.items():
        check(
            f"`{spelling}` + {name} + `scripts/...` derives the guard it runs",
            derived_from(f"\t{spelling}{gap}scripts/check_crate_layers.py"),
            ["scripts/check_crate_layers.py"],
        )

# The two directions are not symmetric, so this only pins the one that bites: a
# spelling that stops deriving drops a guard from the "must be gated" set and
# leaves that path ungated with every check still green. (Over-matching only adds
# a path to the set, which fails closed, so it is not pinned here.)
for spelling in ("$(PYTHON3)", "${PY}", "$(PYTHON}", "python"):
    check(
        f"`{spelling} scripts/...` derives nothing",
        derived_from(f"\t{spelling} scripts/check_crate_layers.py"),
        [],
    )

check(
    "every guard in a multi-line recipe derives",
    derived_from(
        "\t$(PYTHON) scripts/check_crate_layers.py\n"
        "\t${PYTHON} scripts/check_table_layers.py\n"
        "\tpython3 scripts/check_fork_patches.py"
    ),
    [
        "scripts/check_crate_layers.py",
        "scripts/check_fork_patches.py",
        "scripts/check_table_layers.py",
    ],
)

print("lint_recipe")

# The cases above hand `derived_gate_paths` a recipe directly, so they cannot see
# a guard that `lint_recipe` never extracted from the Makefile. These read a
# synthetic Makefile through it. Each shape was confirmed by running it through
# make: every guard in it runs, so every guard in it must derive.


def extracted_from(makefile: str) -> list[str]:
    """Guard paths derived from `makefile` read through `lint_recipe`."""
    original = check_rust_gate_paths.MAKEFILE
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "Makefile"
        path.write_text(makefile, encoding="utf-8")
        check_rust_gate_paths.MAKEFILE = path
        try:
            # As above: these cases pin what `lint_recipe` extracts, so the
            # guards' imports are held out rather than tying them to whatever
            # the real guard files import today.
            paths, _ = derived_gate_paths([], imports=lambda guards: set())
        finally:
            check_rust_gate_paths.MAKEFILE = original
    return sorted(set(paths) - set(check_rust_gate_paths.RUST_SOURCE_PATHS))


RECIPE_SHAPES = {
    "a space-indented continuation": "\t$(PYTHON) \\\n    scripts/check_crate_layers.py\n",
    "an unindented continuation": "\t$(PYTHON) \\\nscripts/check_crate_layers.py\n",
    "a blank line": "\t@echo lint\n\n\t$(PYTHON) scripts/check_crate_layers.py\n",
    "a make comment": "\t@echo lint\n# comment\n\t$(PYTHON) scripts/check_crate_layers.py\n",
}
for name, body in RECIPE_SHAPES.items():
    check(
        f"a guard after {name} is extracted",
        extracted_from(
            f"lint-rust: deps\n{body}"
            "\t$(PYTHON) scripts/check_table_layers.py\n"
            "other:\n"
            "\t$(PYTHON) scripts/check_fork_patches.py\n"
        ),
        ["scripts/check_crate_layers.py", "scripts/check_table_layers.py"],
    )

check(
    "the clippy config directory derives its clippy.toml",
    derived_from('\tCLIPPY_CONF_DIR=".ci" cargo clippy'),
    [".ci/clippy.toml"],
)

print("sibling_imports")

SCRIPTS = {
    "scripts/check_a.py": "import json\nfrom common import cargo_metadata\n",
    "scripts/check_b.py": "try:\n    import tomllib\nexcept ModuleNotFoundError:\n    pass\nimport common\n",
    "scripts/common.py": "import sys\nfrom deeper import helper  # noqa: E402\n",
    "scripts/deeper.py": "import common\n",
}
reader = SCRIPTS.get

check(
    "a guard's `from x import` pulls in its sibling, transitively",
    sorted(sibling_imports(["scripts/check_a.py"], reader)),
    ["scripts/common.py", "scripts/deeper.py"],
)
check(
    "an indented import is read, and a stdlib module is not a gate path",
    sorted(sibling_imports(["scripts/check_b.py"], reader)),
    ["scripts/common.py", "scripts/deeper.py"],
)
check(
    "every module of a multi-module import is read",
    sibling_imports(
        ["scripts/check_d.py"],
        {"scripts/check_d.py": "import json, common\n", "scripts/common.py": ""}.get,
    ),
    {"scripts/common.py"},
)
check(
    "an import that is only text in a docstring is not read",
    sibling_imports(
        ["scripts/check_e.py"],
        {"scripts/check_e.py": '"""\nimport common\n"""\n', "scripts/common.py": ""}.get,
    ),
    set(),
)
check(
    "a guard that is not on disk contributes nothing",
    sibling_imports(["scripts/check_missing.py"], reader),
    set(),
)
check(
    "a commented-out import is not read",
    sibling_imports(["scripts/check_c.py"], {"scripts/check_c.py": "# import nowhere\n"}.get),
    set(),
)
# The shipped helper is imported by both cargo-metadata guards and named by no
# recipe line, so this is the only derivation that gates it.
check(
    "the shipped guards derive scripts/rust_guard_common.py",
    "scripts/rust_guard_common.py"
    in sibling_imports(["scripts/check_crate_layers.py", "scripts/check_module_reachability.py"]),
    True,
)

print("referenced_inputs")

# A source tree's files are all gated already, so only a path outside every tree
# needs deriving — and it can only be found from the source that names it.
SOURCES = {
    "crates/tls/Cargo.toml": "",
    # Compile-time: resolved against the source's own directory.
    "crates/tls/src/reload.rs": 'let cert = include_bytes!("../../../test/tls/cert.pem");\n',
    "crates/pod/Cargo.toml": "",
    # Test-time: resolved against the package directory, where `cargo test` runs.
    "crates/pod/src/lib.rs": 'const FILE: &str = "../../test/spicepods/pod.yaml";\n',
    "crates/env/Cargo.toml": "",
    "crates/env/src/lib.rs": 'concat!(env!("CARGO_MANIFEST_DIR"), "/../../test/data/rows.csv")\n',
    "crates/inner/Cargo.toml": "",
    "crates/inner/src/lib.rs": (
        # Inside a source tree: gated as a tree file, not derived here.
        'include_str!("../tests/fixture.json");\n'
        # Not tracked, and above the repository root: neither names a gate input.
        'include_str!("../../../test/tls/missing.pem");\n'
        'include_str!("../../../../../outside.pem");\n'
    ),
    "crates/inner/tests/fixture.json": "",
    "test/tls/cert.pem": "",
    "test/spicepods/pod.yaml": "",
    "test/data/rows.csv": "",
    # Tracked under `test/`, but no Rust source names it.
    "test/spicepods/unread.yaml": "",
    "outside.pem": "",
    # Not Rust, so its literal is not a Rust input.
    "scripts/example.py": 'open("../test/spicepods/unread.yaml")\n',
}
check(
    "every relative path a Rust source names outside the trees is derived",
    sorted(referenced_inputs(sorted(SOURCES), SOURCES.get)),
    ["test/data/rows.csv", "test/spicepods/pod.yaml", "test/tls/cert.pem"],
)
derived_paths, _ = derived_gate_paths(
    sorted(SOURCES), references=lambda tracked: referenced_inputs(tracked, SOURCES.get)
)
check("derived_gate_paths includes what the sources name", "test/tls/cert.pem" in derived_paths, True)

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
        # `crates/runtime-tls` pins this certificate's SHA-256 in a unit test, so a
        # branch changing only the certificate must still be signed off.
        gated, _ = derived_gate_paths(tracked)
        check(
            "the shipped TLS fixture a unit test embeds is derived and gated by all three lists",
            ("test/tls/spiced_cert.pem" in gated,
             coverage_gaps(["test/tls/spiced_cert.pem"], globs, patterns)),
            (True, ({name: [] for name in patterns}, [])),
        )


if failures:
    print(f"\n{failures} of {checks} checks FAILED")
    raise SystemExit(1)
print(f"\nall {checks} checks passed")
