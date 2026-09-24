#!/usr/bin/env python3
# Copyright 2024-2026 The Spice.ai OSS Authors
#
# MySQL bench-loader NULL guard.
#
# The benchmark fleet seeds every engine from one set of pipe-delimited files, in
# which a NULL is written as an EMPTY field — dsdgen quotes nothing, and the
# DuckDB tpch/tpcds extensions export with QUOTE '', so they write one too. The
# loaders do not agree on what an empty field means:
#
#   * PostgreSQL `\copy … CSV`  reads an empty field as NULL.
#   * MySQL `LOAD DATA`         reads it as the column type's zero value — an
#                               empty integer field lands as 0, never NULL.
#
# So a MySQL loader that feeds those files in unchanged silently seeds different
# data from every other arm of the fleet, and nothing downstream can tell: row
# counts still match, and only a query keying on NULL-ness disagrees. `tpcds_q76`
# is that query — its three predicates are all `IS NULL` — and it answered 0 rows
# on all four MySQL configs against 11 on the other 49 (#13152).
#
# `mysql-null-spec.awk` is the repair: it turns the table's own column types into
# a `LOAD DATA` spec that NULLIFs an empty field only where the type says it can
# mean nothing else. This guard pins that every MySQL bulk-load recipe builds and
# passes that spec, so a loader added later cannot reintroduce the divergence by
# copying a recipe from before the fix.
#
# Usage:
#   scripts/check_bench_mysql_load_nulls.py    # validate (exit 1 on drift, 2 if unreadable)
#
# Pure stdlib; no third-party deps.

from __future__ import annotations

import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent

BENCH_MAKEFILE = REPO / "test" / "tpc-bench" / "Makefile"
SPEC_AWK = REPO / "test" / "tpc-bench" / "mysql-null-spec.awk"

SPEC_VAR = "MYSQL_NULL_SPEC"

# A recipe line that bulk-loads a local file into MySQL.
LOAD_DATA_RE = re.compile(r"LOAD\s+DATA\s+(?:LOCAL\s+)?INFILE\s+'([^']+)'", re.IGNORECASE)

# The line that builds the column spec, e.g.
#   spec=$(mysql … -e "$(MYSQL_COLUMN_TYPES)'$$table' …" | $(MYSQL_NULL_SPEC)); \
SPEC_BUILD_RE = re.compile(rf"(?P<var>\w+)=\$\$\(.*\|\s*\$\({re.escape(SPEC_VAR)}\)\s*\)")

# The start of a recipe: a target name, then a colon that is not `:=`.
TARGET_RE = re.compile(r"^([A-Za-z0-9_.\-/]+)\s*:(?!=)")


def read(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except OSError as exc:  # unreadable tree, not a policy failure
        print(f"error: cannot read {path}: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc


def recipe_blocks(text: str) -> list[tuple[str, list[str]]]:
    """Split a Makefile into (target, recipe-lines) pairs."""
    blocks: list[tuple[str, list[str]]] = []
    current: list[str] | None = None
    for line in text.splitlines():
        if line.startswith("\t"):
            if current is not None:
                current.append(line)
        elif match := TARGET_RE.match(line):
            current = []
            blocks.append((match.group(1), current))
        else:
            current = None
    return blocks


def loader_errors(text: str, rel: str) -> tuple[list[str], int]:
    """Every problem in a bench Makefile's MySQL loaders, and how many it inspected.

    Split out of `main` so the self-test can drive it in-process: asserting on the
    returned strings is what distinguishes the failure modes from each other,
    which a subprocess exit code cannot.
    """
    if not re.search(rf"^{re.escape(SPEC_VAR)}\s*[:+?]?=", text, re.MULTILINE):
        return [
            f"{rel}: `{SPEC_VAR}` is not defined, so no MySQL loader can tell an empty "
            f"field that means NULL from one that means an empty string."
        ], 0

    errors: list[str] = []
    checked = 0
    for target, lines in recipe_blocks(text):
        # The shell variables this recipe assigns from the spec generator. A
        # LOAD DATA must interpolate one of them.
        spec_vars = {
            match.group("var")
            for line in lines
            if (match := SPEC_BUILD_RE.search(line))
        }

        for line in lines:
            load = LOAD_DATA_RE.search(line)
            if not load:
                continue
            checked += 1
            infile = load.group(1)
            if not any(f"$${var}" in line for var in spec_vars):
                errors.append(
                    f"{rel}: target `{target}` loads {infile!r} into MySQL without a "
                    f"column spec from $({SPEC_VAR}), so every empty field in a "
                    f"numeric column lands as 0 instead of NULL. Build the spec from "
                    f"`information_schema` and pass it after the FIELDS/LINES clauses."
                )

    if not checked:
        errors.append(
            f"{rel}: found no `LOAD DATA … INFILE` recipe at all. This guard is "
            f"matching nothing, so it would pass however the loaders are written."
        )
    return errors, checked


def main() -> int:
    rel = str(BENCH_MAKEFILE.relative_to(REPO))
    errors, checked = loader_errors(read(BENCH_MAKEFILE), rel)

    if not SPEC_AWK.exists():
        errors.append(
            f"{SPEC_AWK.relative_to(REPO)} is missing, so $({SPEC_VAR}) cannot run."
        )

    if errors:
        for error in errors:
            print(f"error: {error}", file=sys.stderr)
        print(
            f"\n{len(errors)} MySQL bench-loader NULL problem(s). A pipe-delimited "
            f"bench file writes NULL as an empty field, which MySQL's LOAD DATA reads "
            f"as 0 — see `{SPEC_VAR}` in {rel} and issue #13152.",
            file=sys.stderr,
        )
        return 1

    print(
        f"MySQL bench loaders OK: {checked} `LOAD DATA` recipe(s) pass a column spec "
        f"from $({SPEC_VAR})."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
