"""Normalize object-store paths before they are used as cache keys.

The change under review: collapse repeated slashes and resolve `.` segments,
so `s3://b//a/./c` and `s3://b/a/c` share one cache entry.
"""

from __future__ import annotations

import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from _harness import record  # noqa: E402


def normalize(path: str) -> str:
    scheme, sep, rest = path.partition("://")
    if not sep:
        scheme, rest = "", path
    parts = [part for part in rest.split("/") if part not in ("", ".")]
    joined = "/".join(parts)
    if rest.endswith("/") and joined:
        joined += "/"
    return f"{scheme}://{joined}" if sep else joined
