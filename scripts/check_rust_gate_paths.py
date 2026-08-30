#!/usr/bin/env python3
# Copyright 2024-2026 The Spice.ai OSS Authors
#
# Rust-gate path-list guard.
#
# Three lists decide whether a branch gets any Rust validation:
#
#   1. `RUST_AFFECTING_PATH_PATTERN` in `scripts/signoff` — when nothing in the
#      branch diff matches, `make signoff` skips fmt, clippy, build, and tests.
#   2. `rustAffecting` in `.github/workflows/pr.yml` — when nothing in the PR
#      diff matches, the required `Attestation` check auto-passes. Must classify
#      the same paths as (1); the two are compared here.
#   3. The `code_changes` filter in `.github/actions/check-code-changes` — when
#      nothing matches, the merge queue's `Rust Lint` and `Build and Test` run
#      zero steps and report success. A deliberate superset of (1): it is the
#      shared "did any code change" default, so it also gates integration and
#      E2E. Only its coverage of the Rust-gate paths is pinned here.
#
# A path missing from all three lands on trunk having never been linted, built,
# or tested — where `.ci/clippy.toml`, `.config/nextest.toml`, `layers.toml`, and
# `scripts/check_crate_layers.py` all sat until #12111.
#
# The paths that must be gated are DERIVED, not listed: from what the `lint-rust`
# recipe reads (`CLIPPY_CONF_DIR`, each `$(PYTHON) scripts/…` guard), from the
# tracked files whose name marks them as lint/test config, and from every tracked
# `.rs` file. So this catches "a config file the gate reads is in none of the
# lists" and "a Rust source tree is in none of the lists" — the actual bugs — and
# not merely "the lists disagree about a path someone already thought of".
#
# Usage:
#   scripts/check_rust_gate_paths.py    # validate (exit 1 on drift, 2 if unreadable)
#
# Pure stdlib; no third-party deps.

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent

SIGNOFF = REPO / "scripts" / "signoff"
PR_WORKFLOW = REPO / ".github" / "workflows" / "pr.yml"
CHECK_CHANGES = REPO / ".github" / "actions" / "check-code-changes" / "action.yml"
MAKEFILE = REPO / "Makefile"

# Tracked files whose basename means "this configures clippy, rustfmt, nextest,
# or the layering guard", wherever in the tree they live.
GATE_CONFIG_BASENAMES = (
    "clippy.toml",
    ".clippy.toml",
    "rustfmt.toml",
    ".rustfmt.toml",
    "nextest.toml",
    "layers.toml",
)

# Rust inputs that are neither config files nor `.rs` sources, so there is
# nothing to derive them from.
RUST_SOURCE_PATHS = (
    "Cargo.toml",
    "Cargo.lock",
    "crates/cayenne/Cargo.toml",
    "rust-toolchain.toml",
    ".cargo/config.toml",
    # Holds every -Dclippy::… flag the gate enforces.
    "Makefile",
    # `check_fork_patches.py` validates this file against `Cargo.lock`, so it is
    # an input the gate reads. Gated by name rather than derived: nothing in the
    # `lint-rust` recipe names it, only the guard it feeds. Left ungated, a
    # ledger-only edit skips the very check that would have rejected it, and the
    # mismatch surfaces on someone else's unrelated Rust PR.
    "docs/dev/fork_patches.md",
)

# Paths that must NOT drag in the Rust gate — otherwise the fast-track is dead
# and every docs PR pays a full sign-off.
MUST_SKIP_RUST_CHECKS = (
    "README.md",
    "docs/dev/ci_signoff.md",
    "docs/cayenne/cayenne.md",
    "test/spicepods/tpch/sf1/federated/duckdb.yaml",
    ".github/workflows/pr.yml",
    "scripts/tpcds_explain.sh",
    # A non-Rust guard must not inherit the Rust gate by naming convention.
    "scripts/check_helm_chart.py",
    # Only the root Makefile carries the lint flags.
    "test/tpc-bench/Makefile",
)


def extract_signoff_pattern() -> str | None:
    """The ERE assigned to RUST_AFFECTING_PATH_PATTERN in scripts/signoff."""
    match = re.search(
        r"^RUST_AFFECTING_PATH_PATTERN='(?P<pattern>.*)'$",
        SIGNOFF.read_text(encoding="utf-8"),
        re.MULTILINE,
    )
    return match.group("pattern") if match else None


def extract_workflow_pattern() -> str | None:
    """The `rustAffecting` regex literal in pr.yml, as an ERE.

    A JS literal has to escape `/`, which the shell ERE does not, so unescaping
    it is what makes the two directly comparable.
    """
    match = re.search(
        r"^\s*const rustAffecting = /(?P<pattern>.*)/;$",
        PR_WORKFLOW.read_text(encoding="utf-8"),
        re.MULTILINE,
    )
    return match.group("pattern").replace("\\/", "/") if match else None


def extract_code_change_globs() -> list[str]:
    """The `code_changes` glob list from the check-code-changes default filter."""
    text = CHECK_CHANGES.read_text(encoding="utf-8")
    block = text.split("code_changes:", 1)[-1].split("\noutputs:", 1)[0]
    return re.findall(r"^\s+- '([^']+)'$", block, re.MULTILINE)


def lint_recipe() -> str:
    """The recipe lines of the Makefile's `lint-rust` target (tab-indented)."""
    after_target = MAKEFILE.read_text(encoding="utf-8").split("\nlint-rust:", 1)[-1]
    # Drop the rest of the target line (its prerequisites) before reading the recipe.
    lines = []
    for line in after_target.split("\n", 1)[-1].splitlines():
        if not line.startswith("\t"):
            break
        lines.append(line)
    return "\n".join(lines)


def tracked_files() -> tuple[list[str], list[str]]:
    """Every tracked path in the repo, plus notes if git could not be read."""
    try:
        listing = subprocess.run(
            ["git", "ls-files", "-z"],
            cwd=REPO,
            capture_output=True,
            check=True,
            text=True,
        ).stdout
    except (OSError, subprocess.CalledProcessError) as error:
        return [], [f"could not list tracked files ({error})"]
    return [p for p in listing.split("\0") if p], []


def derived_gate_paths(tracked: list[str]) -> tuple[list[str], list[str]]:
    """Paths the Rust gate reads, plus notes on anything that could not be derived.

    Derived from the `lint-rust` recipe (the clippy config directory it points
    at, and every `$(PYTHON) scripts/…` guard it runs) plus the tracked files whose
    basename marks them as lint/test config.
    """
    paths: set[str] = set(RUST_SOURCE_PATHS)
    notes: list[str] = []

    recipe = lint_recipe()
    if not recipe:
        notes.append(
            "could not read the `lint-rust` recipe from Makefile — derived only from "
            "tracked config-file names"
        )
    for conf_dir in re.findall(r'CLIPPY_CONF_DIR="([^"]+)"', recipe):
        paths.add(f"{conf_dir.rstrip('/')}/clippy.toml")
    # The recipe invokes the guards through $(PYTHON) — the Makefile variable
    # that resolves a Python 3.11+ interpreter. Both make spellings and a literal
    # `python3` are accepted so a recipe line written any of those ways still
    # derives; anything else is deliberately NOT matched, because a guard that
    # silently stopped deriving a path is the exact failure this script exists to
    # catch.
    paths.update(re.findall(r"(?:\$[({]PYTHON[)}]|python3) (scripts/[\w./-]+\.py)", recipe))

    paths.update(p for p in tracked if Path(p).name in GATE_CONFIG_BASENAMES)

    return sorted(paths), notes


def rust_source_trees(tracked: list[str]) -> dict[str, list[str]]:
    """Tracked `.rs` files, grouped by their top-level directory.

    The config-file derivation above reads what the gate *configures*, so it is
    blind to a Rust source tree the lists never mention: `vendor/` held 17
    tracked `.rs` files compiled through a `[patch.crates-io]` entry, in no
    `code_changes` glob, and this guard passed (#13120). Grouping keeps one
    error per tree rather than one per file.

    A `.rs` file at the repo root has no directory to group under and is keyed
    `"."` — no glob covers one today, so that is the shape an error would take.
    """
    trees: dict[str, list[str]] = {}
    for path in tracked:
        if path.endswith(".rs"):
            head, separator, _ = path.partition("/")
            trees.setdefault(head if separator else ".", []).append(path)
    return trees


def glob_matches(glob: str, path: str) -> bool:
    """Match one dorny/paths-filter (picomatch) glob against a path.

    Equivalent to `glob.translate(glob, recursive=True, include_hidden=True)`,
    hand-rolled only because that landed in Python 3.13 and this repo's guards
    run on 3.11. `include_hidden=True` is the rule that matters, and it is read
    off the action rather than assumed: `dorny/paths-filter` builds every
    matcher with `MatchOptions = {dot: true}` (`src/filter.ts`, at the SHA
    pinned in `check-code-changes/action.yml`), so a wildcard *does* reach a
    segment starting with `.` and `**` alone covers `.ci/clippy.toml`. Modelling
    it the other way makes this guard stricter than the filter it stands in for,
    which reports a covered path as ungated.
    """
    if glob.startswith("**/"):
        # picomatch lets a leading `**/` match nothing, so `**/x` matches `x`.
        return glob_matches(glob[3:], path) or glob_matches(f"*/{glob}", path)
    pattern = (
        re.escape(glob)
        .replace(r"\*\*/", "(?:[^/]+/)*")
        .replace(r"\*\*", "[^/]+(?:/[^/]+)*")
        .replace(r"\*", "[^/]+")
    )
    return re.fullmatch(pattern, path) is not None


def coverage_gaps(
    paths: list[str], globs: list[str], patterns: dict[str, str]
) -> tuple[dict[str, list[str]], list[str]]:
    """Which of `paths` each list would skip: per pattern, then for the globs.

    The one place that decides "is this path gated". Both callers below report
    the answer differently — per path for a config file, per tree for a source
    directory — and the two reports drifting apart is how a list quietly stops
    being checked, so the decision itself has exactly one definition.
    """
    return (
        {
            name: [p for p in paths if not re.search(pattern, p)]
            for name, pattern in patterns.items()
        },
        [p for p in paths if not any(glob_matches(g, p) for g in globs)],
    )


def gate_config_errors(
    gated: list[str], globs: list[str], patterns: dict[str, str]
) -> list[str]:
    """One error per config path the Rust gate reads that a list would skip."""
    pattern_misses, glob_misses = coverage_gaps(gated, globs, patterns)
    errors = [
        f"{path} changes what the Rust gate does, but the pattern in {name} "
        "does not match it — the branch would skip every Rust check."
        for name, missed in pattern_misses.items()
        for path in missed
    ]
    errors.extend(
        f"{path} changes what the Rust gate does, but no check-code-changes glob "
        "matches it — the merge queue would report `Rust Lint` and `Build and "
        "Test` green without running a step."
        for path in glob_misses
    )
    return errors


def rust_source_errors(
    trees: dict[str, list[str]], globs: list[str], patterns: dict[str, str]
) -> list[str]:
    """One error per source tree that any of the three lists would skip.

    Reported per tree, naming the file count and one example: the fix is always
    a glob or pattern for the tree, so a per-file list would be noise.
    """
    errors: list[str] = []
    for tree, sources in sorted(trees.items()):
        label = "the repo root" if tree == "." else f"{tree}/"
        pattern_misses, glob_misses = coverage_gaps(sources, globs, patterns)
        for name, missed in pattern_misses.items():
            if missed:
                errors.append(
                    f"{label} holds {len(missed)} tracked Rust source file(s) the pattern in "
                    f"{name} does not match (e.g. {missed[0]}) — a branch changing only those "
                    "would skip every Rust check."
                )
        if glob_misses:
            errors.append(
                f"{label} holds {len(glob_misses)} tracked Rust source file(s) matched by no "
                f"check-code-changes glob (e.g. {glob_misses[0]}) — the merge queue would report "
                "`Rust Lint` and `Build and Test` green without compiling them."
            )
    return errors


def read_patterns() -> dict[str, str] | None:
    """Both Rust-affecting path patterns, keyed by the file they came from."""
    patterns = {}
    for name, extractor, shape in (
        (
            "scripts/signoff",
            extract_signoff_pattern,
            "RUST_AFFECTING_PATH_PATTERN='…' as a single-quoted single-line assignment",
        ),
        (
            ".github/workflows/pr.yml",
            extract_workflow_pattern,
            "const rustAffecting = /…/; on one line",
        ),
    ):
        pattern = extractor()
        if pattern is None:
            print(
                f"error: could not read the Rust-affecting path pattern from {name}. "
                f"Keep it {shape} so this guard can read it.",
                file=sys.stderr,
            )
            return None
        patterns[name] = pattern
    return patterns


def main() -> int:
    patterns = read_patterns()
    if patterns is None:
        return 2

    globs = extract_code_change_globs()
    if not globs:
        print(
            f"error: could not read the `code_changes` glob list from {CHECK_CHANGES}.",
            file=sys.stderr,
        )
        return 2

    errors: list[str] = []
    signoff_pattern, workflow_pattern = patterns.values()
    if signoff_pattern != workflow_pattern:
        errors.append(
            "the Rust-affecting path patterns have drifted apart:\n"
            f"  scripts/signoff: {signoff_pattern}\n"
            f"  pr.yml:          {workflow_pattern}\n"
            "They gate the same decision (local sign-off vs the required "
            "Attestation check) and must classify paths identically."
        )

    # A guard that passes when it could not look is the defect this file exists
    # to catch, so an unreadable tree is exit 2 (unreadable) rather than a
    # warning and a green "0 source tree(s)" line.
    tracked, listing_notes = tracked_files()
    if listing_notes:
        for note in listing_notes:
            print(f"error: {note} — nothing could be checked.", file=sys.stderr)
        return 2

    gated, notes = derived_gate_paths(tracked)
    for note in notes:
        print(f"warning: {note}", file=sys.stderr)

    trees = rust_source_trees(tracked)
    errors.extend(rust_source_errors(trees, globs, patterns))

    errors.extend(gate_config_errors(gated, globs, patterns))

    for path in MUST_SKIP_RUST_CHECKS:
        for name, pattern in patterns.items():
            if re.search(pattern, path):
                errors.append(
                    f"{path} cannot affect Rust lint/build/tests, but the pattern in "
                    f"{name} matches it — the no-Rust fast-track would never fire."
                )

    if errors:
        for error in errors:
            print(f"error: {error}", file=sys.stderr)
        print(
            f"\n{len(errors)} Rust-gate path-list problem(s). Update all three lists "
            "together: RUST_AFFECTING_PATH_PATTERN in scripts/signoff, `rustAffecting` "
            "in .github/workflows/pr.yml, and the `code_changes` filter in "
            ".github/actions/check-code-changes.\nSee docs/dev/ci_signoff.md.",
            file=sys.stderr,
        )
        return 1

    print(
        f"Rust-gate paths OK: patterns agree, {len(gated)} gated path(s) and "
        f"{len(trees)} Rust source tree(s) matched by all three lists, "
        f"{len(MUST_SKIP_RUST_CHECKS)} fast-track path(s) still skipped."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
