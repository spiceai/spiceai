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
#     pass unnoticed on a clean tree — the fixtures below pin it against a loader
#     that skips the prep, one staged with a bare `sed`, one whose input is never
#     staged, and a file with no loader at all.
#   * The transform is the thing that actually has to be right, and it is a sed
#     program in a Makefile that nothing else executes in CI. It is read out of
#     the shipped Makefile — not restated here — and run, so this cannot drift
#     away from what the loaders use.
#
# Usage:
#   scripts/test_check_bench_mysql_load_nulls.py    # exit 0 when every case passes

from __future__ import annotations

import subprocess
import sys
import tempfile
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "scripts"))

# Imported rather than driven through a subprocess: asserting on the returned
# strings is what tells the three failure modes apart, which an exit code cannot.
from check_bench_mysql_load_nulls import loader_errors  # noqa: E402

BENCH_MAKEFILE = REPO / "test" / "tpc-bench" / "Makefile"

REL = "test/tpc-bench/Makefile"

PREP_DEF = (
    "MYSQL_LOAD_PREP = sed -e 's/|$$//' -e ':a' -e 's/||/|\\\\N|/g' -e 'ta' "
    "-e 's/^|/\\\\N|/' -e 's/|$$/|\\\\N/'\n"
)
TARGET = "\nmysql-tpcds-load:\n"
PREP_STAGE = "\t$(MYSQL_LOAD_PREP) \"$(TPCDS_DATA_DIR)/$$table.dat\" > ./tmp/$$table.dat; \\\n"
BARE_STAGE = "\tsed 's/|$$//' \"$(TPCDS_DATA_DIR)/$$table.dat\" > ./tmp/$$table.dat; \\\n"
LOAD_LINE = (
    "\tmysql -h$(DB_HOST) --local-infile=1 $(DB_NAME) -e \"LOAD DATA LOCAL INFILE "
    "'./tmp/$$table.dat' INTO TABLE $$table FIELDS TERMINATED BY '|' LINES "
    "TERMINATED BY '\\n';\"; \\\n"
)

GOOD = PREP_DEF + TARGET + PREP_STAGE + LOAD_LINE
BARE_SED = PREP_DEF + TARGET + BARE_STAGE + LOAD_LINE
UNSTAGED = PREP_DEF + TARGET + LOAD_LINE
NO_PREP_VAR = TARGET.lstrip("\n") + BARE_STAGE + LOAD_LINE
NO_LOADER = PREP_DEF + "\nsomething-else:\n\techo hi\n"

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
    check("a loader staged through $(MYSQL_LOAD_PREP) is accepted", errors, [])
    check("  and its LOAD DATA was actually inspected", checked, 1)

    errors, _ = loader_errors(BARE_SED, REL)
    check("a loader staged with a bare sed yields one error", len(errors), 1)
    check_contains("  the error names the target", errors[0], "mysql-tpcds-load")
    check_contains("  and says what the bad staging was", errors[0], "instead")

    errors, _ = loader_errors(UNSTAGED, REL)
    check("a loader whose input is never staged yields one error", len(errors), 1)
    check_contains("  the error says nothing stages it", errors[0], "no ")
    check(
        "  the unstaged error is distinct from the bare-sed error",
        errors[0] == loader_errors(BARE_SED, REL)[0][0],
        False,
    )

    errors, _ = loader_errors(NO_PREP_VAR, REL)
    check("a Makefile with no MYSQL_LOAD_PREP yields one error", len(errors), 1)
    check_contains("  the error names the missing variable", errors[0], "MYSQL_LOAD_PREP")

    # A guard that matches nothing must not report success — that is how a parser
    # regression would otherwise pass on a clean tree.
    errors, checked = loader_errors(NO_LOADER, REL)
    check("a Makefile with no loader at all is rejected", len(errors), 1)
    check("  and nothing was inspected", checked, 0)


def shipped_prep_command() -> str:
    """The `MYSQL_LOAD_PREP` recipe as `make` expands it, read from the real Makefile.

    Via `make -n` rather than by re-parsing the assignment, so make's own escaping
    (`$$` -> `$`) is applied by make and this cannot disagree with what the loaders run.
    """
    expanded = subprocess.run(
        ["make", "-C", str(BENCH_MAKEFILE.parent), "-n", "mysql-tpcds-load"],
        capture_output=True, text=True, check=False,
    ).stdout
    for line in expanded.splitlines():
        stripped = line.strip()
        if stripped.startswith("sed "):
            # The staged source is the first quoted argument; everything before it
            # is the transform.
            return stripped.split(' "', 1)[0]
    raise AssertionError(
        "could not read the expanded MYSQL_LOAD_PREP out of `make -n mysql-tpcds-load`"
    )


def test_transform_semantics() -> None:
    print("MYSQL_LOAD_PREP transform (read from the shipped Makefile)")
    try:
        prep = shipped_prep_command()
    except AssertionError as exc:
        check("reads the shipped transform", str(exc), "")
        return
    print("  ok   reads the shipped transform")

    # dsdgen/dbgen format: pipe-separated, one trailing '|', NULL written as an
    # empty field. Covers a leading, middle, trailing, and consecutive NULL, plus
    # a row with none, and values that merely contain 'N' or are a literal 0.
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
    check("transform exits cleanly", (out.returncode, out.stderr.strip()), (0, ""))

    got = [line.split("|") for line in out.stdout.splitlines()]
    check("every row parses to the PostgreSQL CSV NULL reading", got, expected)

    # The point of the whole transform: exactly the empty fields become NULL. A
    # literal 0 must never become NULL, and a NULL never 0.
    empties = sum(1 for row in rows for field in row[:-1].split("|") if field == "")
    nulls = sum(1 for row in got for field in row if field == r"\N")
    check("one \\N out per empty field in, and no others", nulls, empties)


def main() -> int:
    test_guard_parser()
    test_transform_semantics()
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
