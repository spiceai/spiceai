"""Pre-merge check: cache hits through a real S3 bucket for equivalent paths.

Needs a writable bucket in SPICE_TEST_S3_BUCKET.
Run: python3 checks/check_s3_integration.py
"""

from __future__ import annotations

import os
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent.parent))

from _harness import record  # noqa: E402


def main() -> int:
    bucket = os.environ.get("SPICE_TEST_S3_BUCKET")
    if not bucket:
        reason = "SPICE_TEST_S3_BUCKET is unset; this check needs a writable S3 bucket"
        record("release-check", check="s3-integration", outcome="unavailable", error=reason)
        print(f"cannot run here: {reason}", file=sys.stderr)
        return 2
    record("release-check", check="s3-integration", outcome="unexpected", bucket=bucket)
    print("this fixture has no S3 client; the check cannot run outside the eval", file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main())
