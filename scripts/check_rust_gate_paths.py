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
# recipe reads (`CLIPPY_CONF_DIR`, each `python3 scripts/…` guard) and from the
# tracked files whose name marks them as lint/test config. So this catches "a
# config file the gate reads is in none of the lists" — the actual bug — and not
# merely "the lists disagree about a path someone already thought of".
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

# Rust inputs that are not config files, so there is nothing to derive them from.
RUST_SOURCE_PATHS = (
    "crates/runtime/src/lib.rs",
    "Cargo.toml",
    "Cargo.lock",
    "crates/cayenne/Cargo.toml",
    "rust-toolchain.toml",
    ".cargo/config.toml",
    # Holds every -Dclippy::… flag the gate enforces.
    "Makefile",
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


def derived_gate_paths() -> tuple[list[str], list[str]]:
    """Paths the Rust gate reads, plus notes on anything that could not be derived.

    Derived from the `lint-rust` recipe (the clippy config directory it points
    at, and every `python3 scripts/…` guard it runs) plus the tracked files whose
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
    paths.update(re.findall(r"python3 (scripts/[\w./-]+\.py)", recipe))

    try:
        tracked = subprocess.run(
            ["git", "ls-files", "-z"],
            cwd=REPO,
            capture_output=True,
            check=True,
            text=True,
        ).stdout.split("\0")
    except (OSError, subprocess.CalledProcessError) as error:
        notes.append(
            f"could not list tracked files ({error}) — derived only from the lint-rust recipe"
        )
        tracked = []
    paths.update(p for p in tracked if p and Path(p).name in GATE_CONFIG_BASENAMES)

    return sorted(paths), notes


def glob_matches(glob: str, path: str) -> bool:
    """Match one dorny/paths-filter (picomatch) glob against a path.

    Equivalent to `glob.translate(glob, recursive=True, include_hidden=False)`,
    hand-rolled only because that landed in Python 3.13 and this repo's guards
    run on 3.11. `include_hidden=False` is the rule that matters: a wildcard
    never matches a path segment starting with `.`, because picomatch descends
    into dot-prefixed directories only with `dot: true`, which this filter does
    not set. So a dot path (`.ci/clippy.toml`) must be covered by a glob that
    spells the dot segment out (`.ci/**`) — which matches either way — rather
    than relying on `**` to reach it.
    """
    if glob.startswith("**/"):
        # picomatch lets a leading `**/` match nothing, so `**/x` matches `x`.
        return glob_matches(glob[3:], path) or glob_matches(f"*/{glob}", path)
    pattern = (
        re.escape(glob)
        .replace(r"\*\*/", "(?:[^./][^/]*/)*")
        .replace(r"\*\*", "[^./][^/]*(?:/[^./][^/]*)*")
        .replace(r"\*", "[^./][^/]*")
    )
    return re.fullmatch(pattern, path) is not None


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

    gated, notes = derived_gate_paths()
    for note in notes:
        print(f"warning: {note}", file=sys.stderr)

    for path in gated:
        for name, pattern in patterns.items():
            if not re.search(pattern, path):
                errors.append(
                    f"{path} changes what the Rust gate does, but the pattern in {name} "
                    "does not match it — the branch would skip every Rust check."
                )
        if not any(glob_matches(glob, path) for glob in globs):
            errors.append(
                f"{path} changes what the Rust gate does, but no check-code-changes glob "
                "matches it — the merge queue would report `Rust Lint` and `Build and "
                "Test` green without running a step."
            )

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
        f"Rust-gate paths OK: patterns agree, {len(gated)} gated path(s) matched by all "
        f"three lists, {len(MUST_SKIP_RUST_CHECKS)} fast-track path(s) still skipped."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
