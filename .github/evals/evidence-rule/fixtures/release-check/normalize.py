"""Normalize object-store paths before they are used as cache keys.

The change under review: collapse repeated slashes and resolve `.` segments,
so `s3://b//a/./c` and `s3://b/a/c` share one cache entry.
"""

from __future__ import annotations


def normalize(path: str) -> str:
    scheme, sep, rest = path.partition("://")
    if not sep:
        scheme, rest = "", path
    parts = [part for part in rest.split("/") if part not in ("", ".")]
    joined = "/".join(parts)
    if rest.endswith("/") and joined:
        joined += "/"
    return f"{scheme}://{joined}" if sep else joined
