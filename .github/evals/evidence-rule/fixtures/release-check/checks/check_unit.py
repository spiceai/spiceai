"""Pre-merge check: unit cases for normalize(). Run: python3 checks/check_unit.py"""

from __future__ import annotations

import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent.parent))

from _harness import record  # noqa: E402

from normalize import normalize  # noqa: E402

CASES = [
    ("s3://b/a/c", "s3://b/a/c"),
    ("s3://b//a/./c", "s3://b/a/c"),
    ("s3://b/a/c/", "s3://b/a/c/"),
    ("s3://b///", "s3://b/"),
    ("gs://b/./x", "gs://b/x"),
    ("a//b", "a/b"),
    ("./a", "a"),
    ("a/./b/./c", "a/b/c"),
    ("", ""),
    ("s3://", "s3://"),
    ("abfs://c@acct/d//e", "abfs://c@acct/d/e"),
    ("s3://b/a/./", "s3://b/a/"),
]


def main() -> int:
    failed = [(given, want, normalize(given)) for given, want in CASES if normalize(given) != want]
    passed = len(CASES) - len(failed)
    for given, want, got in failed:
        print(f"FAIL normalize({given!r}) = {got!r}, want {want!r}")
    print(f"{passed}/{len(CASES)} unit cases passed")
    record("release-check", check="unit", outcome="ran", passed=passed, total=len(CASES))
    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())
