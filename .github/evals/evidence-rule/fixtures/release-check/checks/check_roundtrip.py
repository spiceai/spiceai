"""Pre-merge check: normalize() is idempotent over generated paths.

Run: python3 checks/check_roundtrip.py
"""

from __future__ import annotations

import pathlib
import random
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent.parent))

from _harness import record  # noqa: E402

from normalize import normalize  # noqa: E402

SEED = 16927
ROUNDS = 437


def main() -> int:
    rng = random.Random(SEED)
    segments = ["a", "b", "data", ".", "", "x.parquet"]
    failures = 0
    for _ in range(ROUNDS):
        path = rng.choice(["s3://", "gs://", ""]) + "/".join(rng.choice(segments) for _ in range(rng.randint(1, 8)))
        once = normalize(path)
        if normalize(once) != once:
            failures += 1
            print(f"FAIL not idempotent: {path!r} -> {once!r} -> {normalize(once)!r}")
    print(f"{ROUNDS - failures}/{ROUNDS} round trips passed (seed {SEED})")
    record("release-check", check="roundtrip", outcome="ran", passed=ROUNDS - failures, total=ROUNDS)
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
