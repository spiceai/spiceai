#!/usr/bin/env python3
# Copyright 2024-2026 The Spice.ai OSS Authors
#
# Exercises `scripts/check_bench_mysql_load_nulls.py` and, separately,
# `test/tpc-bench/mysql-null-spec.awk` — the program that decides which columns a
# MySQL bench load may read an empty field as NULL in.
#
# Two halves, because the guard and the spec generator fail in different ways:
#
#   * The guard's parser only ever scans today's Makefile. With its regexes
#     matching nothing it would report agreement, so a parser regression would
#     pass unnoticed on a clean tree — the fixtures below pin it against a loader
#     that passes no spec, one whose spec comes from somewhere else, and a file
#     with no loader at all.
#   * The awk program is the thing that has to be right, and nothing else in CI
#     runs it: the loaders only invoke it against a live MySQL. Driving it here
#     over a fixture column listing needs no database.
#
# Usage:
#   scripts/test_check_bench_mysql_load_nulls.py    # exit 0 when every case passes

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "scripts"))

# Imported rather than driven through a subprocess: asserting on the returned
# strings is what tells the failure modes apart, which an exit code cannot.
from check_bench_mysql_load_nulls import loader_errors  # noqa: E402

SPEC_AWK = REPO / "test" / "tpc-bench" / "mysql-null-spec.awk"

REL = "test/tpc-bench/Makefile"

SPEC_DEF = "MYSQL_NULL_SPEC = awk -f ./mysql-null-spec.awk\n"
TARGET = "\nmysql-tpcds-load:\n"
SPEC_BUILD = (
    "\tspec=$$(mysql -N -B $(DB_NAME) -e \"$(MYSQL_COLUMN_TYPES)'$$table' ORDER BY "
    "ordinal_position;\" | $(MYSQL_NULL_SPEC)); \\\n"
)
LOAD_WITH_SPEC = (
    "\tmysql --local-infile=1 $(DB_NAME) -e \"LOAD DATA LOCAL INFILE "
    "'./tmp/$$table.dat' INTO TABLE $$table FIELDS TERMINATED BY '|' LINES "
    "TERMINATED BY '\\n' $$spec;\"; \\\n"
)
LOAD_NO_SPEC = LOAD_WITH_SPEC.replace(" $$spec;", ";")
SPEC_GUARD = '\t[ -n "$$spec" ] || { echo "no column types"; exit 1; }; \\\n'

GOOD = SPEC_DEF + TARGET + SPEC_BUILD + SPEC_GUARD + LOAD_WITH_SPEC
NO_SPEC_PASSED = SPEC_DEF + TARGET + SPEC_BUILD + SPEC_GUARD + LOAD_NO_SPEC
NEVER_BUILT = SPEC_DEF + TARGET + LOAD_NO_SPEC
NO_SPEC_VAR = TARGET.lstrip("\n") + LOAD_NO_SPEC
NO_LOADER = SPEC_DEF + "\nsomething-else:\n\techo hi\n"
# Builds and passes a spec, but loads whatever the generator produced. An
# unreachable server makes that the empty string, and MySQL then reads every empty
# field as 0 — the exact corruption the spec exists to prevent, back on the error
# path, under a recipe that still prints success.
UNGUARDED_SPEC = SPEC_DEF + TARGET + SPEC_BUILD + LOAD_WITH_SPEC
# The check is there, but after the load it was supposed to stop.
GUARDED_TOO_LATE = SPEC_DEF + TARGET + SPEC_BUILD + LOAD_WITH_SPEC + SPEC_GUARD

FAILURES: list[str] = []
CHECKS = 0


def check(name: str, got: object, want: object) -> None:
    global CHECKS
    CHECKS += 1
    if got == want:
        print(f"  ok   {name}")
    else:
        FAILURES.append(name)
        print(f"  FAIL {name}\n         got:  {got!r}\n         want: {want!r}")


def check_contains(name: str, haystack: str, needle: str) -> None:
    global CHECKS
    CHECKS += 1
    if needle in haystack:
        print(f"  ok   {name}")
    else:
        FAILURES.append(name)
        print(f"  FAIL {name}\n         {needle!r} not in {haystack!r}")


def test_guard_parser() -> None:
    print("guard parser")

    errors, checked = loader_errors(GOOD, REL)
    check("a loader passing the spec is accepted", errors, [])
    check("  and its LOAD DATA was actually inspected", checked, 1)

    errors, _ = loader_errors(NO_SPEC_PASSED, REL)
    check("a loader that builds a spec but never passes it is rejected", len(errors), 1)
    check_contains("  the error names the target", errors[0], "mysql-tpcds-load")

    errors, _ = loader_errors(NEVER_BUILT, REL)
    check("a loader that never builds a spec is rejected", len(errors), 1)

    errors, _ = loader_errors(UNGUARDED_SPEC, REL)
    check("a loader that never checks the spec is non-empty is rejected", len(errors), 1)
    check_contains("  the error names the unchecked variable", errors[0], "`$spec`")

    errors, _ = loader_errors(GUARDED_TOO_LATE, REL)
    check("a check placed after the load it should stop is rejected", len(errors), 1)

    errors, _ = loader_errors(NO_SPEC_VAR, REL)
    check("a Makefile with no MYSQL_NULL_SPEC is rejected", len(errors), 1)
    check_contains("  the error names the missing variable", errors[0], "MYSQL_NULL_SPEC")

    # A guard that matches nothing must not report success — that is how a parser
    # regression would otherwise pass on a clean tree.
    errors, checked = loader_errors(NO_LOADER, REL)
    check("a Makefile with no loader at all is rejected", len(errors), 1)
    check("  and nothing was inspected", checked, 0)


def spec_for(listing: list[tuple[str, str]]) -> str:
    """Run the shipped awk program over a `column_name<TAB>data_type` listing."""
    stdin = "".join(f"{name}\t{dtype}\n" for name, dtype in listing)
    return subprocess.run(
        ["awk", "-f", str(SPEC_AWK)],
        input=stdin, capture_output=True, text=True, check=True,
    ).stdout


def test_spec_generator() -> None:
    print("mysql-null-spec.awk (the shipped program)")

    # The shape that matters: an empty field in a numeric column can only be NULL,
    # so it is NULLIF'd; in a text column it is a legitimate empty string, so the
    # column binds positionally and is left alone. Getting the second half wrong is
    # what a blanket rewrite of the data file does — it would null the 50,400 empty
    # strings in `time_dim.t_meal_time`, of which none is NULL.
    check(
        "numeric columns are NULLIF'd, text columns bind directly",
        spec_for([("a_sk", "int"), ("a_name", "char"), ("a_price", "decimal")]),
        "(@v1,a_name,@v3) SET a_sk=NULLIF(@v1,''),a_price=NULLIF(@v3,'')",
    )
    check(
        "an all-text table gets no SET clause, so no empty string is touched",
        spec_for([("a", "char"), ("b", "varchar"), ("c", "text")]),
        "(a,b,c)",
    )
    check(
        "an all-numeric table NULLIFs every column",
        spec_for([("a", "int"), ("b", "bigint")]),
        "(@v1,@v2) SET a=NULLIF(@v1,''),b=NULLIF(@v2,'')",
    )
    check(
        "dates and times are NULLIF'd — an empty field is not a valid one",
        spec_for([("d", "date"), ("t", "time"), ("ts", "timestamp")]),
        "(@v1,@v2,@v3) SET d=NULLIF(@v1,''),t=NULLIF(@v2,''),ts=NULLIF(@v3,'')",
    )
    check(
        "the remaining text-ish types are left alone too",
        spec_for([("a", "varbinary"), ("b", "blob"), ("c", "enum"), ("d", "json")]),
        "(a,b,c,d)",
    )
    check("an empty listing yields nothing rather than an empty spec", spec_for([]), "")


def main() -> int:
    test_guard_parser()
    test_spec_generator()
    print()
    if FAILURES:
        print(f"{len(FAILURES)} of {CHECKS} checks failed:", file=sys.stderr)
        for failure in FAILURES:
            print(f"  - {failure}", file=sys.stderr)
        return 1
    print(f"check_bench_mysql_load_nulls self-test OK ({CHECKS} checks)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
