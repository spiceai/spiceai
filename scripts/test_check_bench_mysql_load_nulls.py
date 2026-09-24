#!/usr/bin/env python3
# Copyright 2024-2026 The Spice.ai OSS Authors
#
# Exercises `scripts/check_bench_mysql_load_nulls.py` and, separately, the
# `$(MYSQL_LOAD_PREP)` transform the guard pins.
#
# Two halves, because the guard and the transform fail in different ways:
#
#   * The guard's parser only ever scans today's Makefile. With both its regexes
#     matching nothing it would report agreement, so a parser regression would
#     pass unnoticed on a clean tree — the shapes below pin it against a loader
#     that skips the prep, one that stages with a bare `sed`, and a file with no
#     loader at all.
#   * The transform is the thing that actually has to be right, and it is a sed
#     program in a Makefile that nothing else executes in CI. It is read out of
#     the shipped Makefile — not restated here — and run, so this cannot drift
#     away from what the loaders use.
#
# Usage:
#   scripts/test_check_bench_mysql_load_nulls.py    # exit 0 when every case passes

from __future__ import annotations

import re
import subprocess
import sys
import tempfile
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
GUARD = REPO / "scripts" / "check_bench_mysql_load_nulls.py"
BENCH_MAKEFILE = REPO / "test" / "tpc-bench" / "Makefile"

LOAD_LINE = (
    "\tmysql -h$(DB_HOST) --local-infile=1 $(DB_NAME) -e \"LOAD DATA LOCAL INFILE "
    "'./tmp/$$table.dat' INTO TABLE $$table FIELDS TERMINATED BY '|' LINES "
    "TERMINATED BY '\\n';\"; \\\n"
)

PREP_DEF = (
    "MYSQL_LOAD_PREP = sed -e 's/|$$//' -e ':a' -e 's/||/|\\\\N|/g' -e 'ta' "
    "-e 's/^|/\\\\N|/' -e 's/|$$/|\\\\N/'\n"
)

GOOD = PREP_DEF + "\nmysql-tpcds-load:\n" + (
    "\t$(MYSQL_LOAD_PREP) \"$(TPCDS_DATA_DIR)/$$table.dat\" > ./tmp/$$table.dat; \\\n"
) + LOAD_LINE

BARE_SED = PREP_DEF + "\nmysql-tpcds-load:\n" + (
    "\tsed 's/|$$//' \"$(TPCDS_DATA_DIR)/$$table.dat\" > ./tmp/$$table.dat; \\\n"
) + LOAD_LINE

UNSTAGED = PREP_DEF + "\nmysql-tpcds-load:\n" + LOAD_LINE

NO_PREP_VAR = "mysql-tpcds-load:\n" + (
    "\tsed 's/|$$//' \"$(TPCDS_DATA_DIR)/$$table.dat\" > ./tmp/$$table.dat; \\\n"
) + LOAD_LINE

NO_LOADER = PREP_DEF + "\nsomething-else:\n\techo hi\n"

FAILURES: list[str] = []


def check(name: str, condition: bool, detail: str = "") -> None:
    if condition:
        print(f"  ok   {name}")
    else:
        FAILURES.append(f"{name}{': ' + detail if detail else ''}")
        print(f"  FAIL {name}{': ' + detail if detail else ''}")


def run_guard_against(makefile_text: str) -> subprocess.CompletedProcess[str]:
    """Run the shipped guard against a synthetic bench Makefile."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "scripts").mkdir()
        (root / "test" / "tpc-bench").mkdir(parents=True)
        (root / "test" / "tpc-bench" / "Makefile").write_text(
            makefile_text, encoding="utf-8"
        )
        guard = root / "scripts" / GUARD.name
        guard.write_text(GUARD.read_text(encoding="utf-8"), encoding="utf-8")
        return subprocess.run(
            [sys.executable, str(guard)], capture_output=True, text=True, check=False
        )


def test_guard_parser() -> None:
    print("guard parser")
    good = run_guard_against(GOOD)
    check("accepts a loader staged through $(MYSQL_LOAD_PREP)", good.returncode == 0,
          good.stderr.strip())

    bare = run_guard_against(BARE_SED)
    check("rejects a loader staged with a bare sed", bare.returncode == 1)
    check("  names the offending target", "mysql-tpcds-load" in bare.stderr)

    unstaged = run_guard_against(UNSTAGED)
    check("rejects a loader whose input is never staged", unstaged.returncode == 1)

    missing = run_guard_against(NO_PREP_VAR)
    check("rejects a Makefile with no MYSQL_LOAD_PREP at all", missing.returncode == 1)

    empty = run_guard_against(NO_LOADER)
    check("rejects a Makefile it matched no loader in", empty.returncode == 1,
          "a guard that matches nothing must not report success")


def shipped_prep_command() -> str:
    """The `MYSQL_LOAD_PREP` recipe as `make` expands it, read from the real Makefile."""
    expanded = subprocess.run(
        ["make", "-C", str(BENCH_MAKEFILE.parent), "-n", "mysql-tpcds-load"],
        capture_output=True, text=True, check=False,
    ).stdout
    for line in expanded.splitlines():
        stripped = line.strip()
        if stripped.startswith("sed ") and " > ./tmp/" in stripped:
            return stripped.split(' "', 1)[0]
    raise AssertionError(
        "could not read the expanded MYSQL_LOAD_PREP out of `make -n mysql-tpcds-load`"
    )


def test_transform_semantics() -> None:
    print("MYSQL_LOAD_PREP transform (read from the shipped Makefile)")
    try:
        prep = shipped_prep_command()
    except AssertionError as exc:
        check("reads the shipped transform", False, str(exc))
        return
    check("reads the shipped transform", True)

    # dsdgen/dbgen format: pipe-separated, one trailing '|', NULL written as an
    # empty field. Covers a leading, middle, trailing, and consecutive NULL, plus
    # a row with none, and a value that merely contains 'N'.
    rows = [
        "1|101|11|19.99|",
        "2|102||29.99|",
        "|106|16|69.99|",
        "7|107|17||",
        "8|108|||",
        "9|N|Nz|0|",
    ]
    expected = [
        ["1", "101", "11", "19.99"],
        ["2", "102", r"\N", "29.99"],
        [r"\N", "106", "16", "69.99"],
        ["7", "107", "17", r"\N"],
        ["8", "108", r"\N", r"\N"],
        ["9", "N", "Nz", "0"],
    ]

    with tempfile.TemporaryDirectory() as tmp:
        src = Path(tmp) / "t.dat"
        src.write_text("\n".join(rows) + "\n", encoding="utf-8")
        out = subprocess.run(
            f"{prep} {src}", shell=True, capture_output=True, text=True, check=False
        )
    check("transform runs", out.returncode == 0, out.stderr.strip())
    got = [line.split("|") for line in out.stdout.splitlines()]

    check("row count preserved", len(got) == len(expected),
          f"{len(got)} != {len(expected)}")
    for i, (g, e) in enumerate(zip(got, expected)):
        check(f"row {i + 1} field parity with PostgreSQL CSV NULL semantics", g == e,
              f"{g!r} != {e!r}")

    # The point of the whole transform: an empty field becomes NULL, and exactly
    # the fields that were empty. `0` must never become NULL, and NULL never 0.
    empties = sum(1 for row in rows for f in row[:-1].split("|") if f == "")
    nulls = sum(1 for row in expected for f in row if f == r"\N")
    check("one \\N per empty field, and no others", empties == nulls,
          f"{empties} empty field(s) in, {nulls} \\N out")
    check("a literal 0 is never turned into NULL",
          expected[5][3] == "0" and got and got[5][3] == "0")


def main() -> int:
    test_guard_parser()
    test_transform_semantics()
    print()
    if FAILURES:
        print(f"{len(FAILURES)} failure(s):", file=sys.stderr)
        for failure in FAILURES:
            print(f"  - {failure}", file=sys.stderr)
        return 1
    print("check_bench_mysql_load_nulls self-test OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
