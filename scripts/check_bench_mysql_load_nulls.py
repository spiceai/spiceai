#!/usr/bin/env python3
# Copyright 2024-2026 The Spice.ai OSS Authors
#
# MySQL bench-loader NULL guard.
#
# The benchmark fleet seeds every engine from one set of pipe-delimited files, in
# which a NULL is written as an EMPTY field — that is the only spelling it has,
# because dsdgen/dbgen quote nothing and the DuckDB tpch/tpcds extensions export
# with QUOTE ''. The loaders do not agree on what an empty field means:
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
# `$(MYSQL_LOAD_PREP)` in `test/tpc-bench/Makefile` is the repair: it renders each
# empty field as `\N`, which LOAD DATA reads as NULL. This guard pins that every
# MySQL bulk-load recipe routes its input through it, so a loader added later
# cannot reintroduce the divergence by copying the older recipe.
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

PREP_VAR = "MYSQL_LOAD_PREP"

# A recipe line that bulk-loads a local file into MySQL.
LOAD_DATA_RE = re.compile(r"LOAD\s+DATA\s+(LOCAL\s+)?INFILE\s+'([^']+)'", re.IGNORECASE)

# The line that writes the staged copy the LOAD DATA above reads, e.g.
#   $(MYSQL_LOAD_PREP) "$(TPCDS_DATA_DIR)/$$table.dat" > ./tmp/$$table.dat; \
STAGE_RE = re.compile(r"^\s*(?P<prep>.*?)\s*\"[^\"]+\"\s*>\s*(?P<dest>\S+?);?\s*\\?\s*$")


def read(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except OSError as exc:  # unreadable tree, not a policy failure
        print(f"error: cannot read {path}: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc


def recipe_blocks(text: str) -> list[tuple[str, list[str]]]:
    """Split a Makefile into (target, recipe-lines) pairs."""
    blocks: list[tuple[str, list[str]]] = []
    target: str | None = None
    lines: list[str] = []
    for line in text.splitlines():
        if line.startswith("\t"):
            if target is not None:
                lines.append(line)
            continue
        if target is not None:
            blocks.append((target, lines))
            target, lines = None, []
        match = re.match(r"^([A-Za-z0-9_.\-/]+)\s*:(?!=)", line)
        if match:
            target, lines = match.group(1), []
    if target is not None:
        blocks.append((target, lines))
    return blocks


def main() -> int:
    text = read(BENCH_MAKEFILE)
    rel = BENCH_MAKEFILE.relative_to(REPO)

    errors: list[str] = []

    if not re.search(rf"^{re.escape(PREP_VAR)}\s*[:+?]?=", text, re.MULTILINE):
        errors.append(
            f"{rel}: `{PREP_VAR}` is not defined, so no MySQL loader can render an "
            f"empty field as NULL."
        )
        for error in errors:
            print(f"error: {error}", file=sys.stderr)
        return 1

    checked = 0
    for target, lines in recipe_blocks(text):
        # Map each staged destination to the command that produced it, so a LOAD
        # DATA can be traced back to whether its input went through the prep.
        staged: dict[str, str] = {}
        for line in lines:
            stage = STAGE_RE.match(line)
            if stage and stage.group("prep"):
                staged[stage.group("dest").strip()] = stage.group("prep")

        for line in lines:
            load = LOAD_DATA_RE.search(line)
            if not load:
                continue
            checked += 1
            infile = load.group(2)
            prep = staged.get(infile)
            if prep is None:
                errors.append(
                    f"{rel}: target `{target}` loads {infile!r} into MySQL but no "
                    f"line in the recipe stages that file, so this guard cannot tell "
                    f"whether its empty fields become NULL. Stage it with "
                    f"`$({PREP_VAR}) <source> > {infile}`."
                )
            elif f"$({PREP_VAR})" not in prep:
                errors.append(
                    f"{rel}: target `{target}` stages {infile!r} with `{prep}` instead "
                    f"of `$({PREP_VAR})`, so every empty field in it loads into MySQL "
                    f"as the column type's zero value rather than NULL."
                )

    if not checked:
        errors.append(
            f"{rel}: found no `LOAD DATA … INFILE` recipe at all. This guard is "
            f"matching nothing, so it would pass however the loaders are written."
        )

    if errors:
        for error in errors:
            print(f"error: {error}", file=sys.stderr)
        print(
            f"\n{len(errors)} MySQL bench-loader NULL problem(s). A pipe-delimited "
            f"bench file writes NULL as an empty field, which MySQL's LOAD DATA reads "
            f"as 0 unless it is rendered as `\\N` first — see `{PREP_VAR}` in "
            f"{rel} and issue #13152.",
            file=sys.stderr,
        )
        return 1

    print(
        f"MySQL bench loaders OK: {checked} `LOAD DATA` recipe(s) stage their input "
        f"through $({PREP_VAR})."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
