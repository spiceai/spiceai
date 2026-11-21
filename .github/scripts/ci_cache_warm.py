#!/usr/bin/env python3
"""Upload every file .github/ci-cache/pins.json pins to the CI cache that lacks it.

Run by the `CI cache warm-up` workflow, the one job that writes to the cache; every other job
only reads it, anonymously. For each pinned file it reads <CI_CACHE_URL>/sha256/<sha256>
anonymously, as a job would, and checks the bytes; a missing or wrong one is downloaded
from its source with cached_fetch.sh (checked against its pin), copied in with mc_cp.sh,
and read back anonymously, so a file counts as cached only once a job could read it.

Environment: CI_CACHE_URL (the bucket's anonymous HTTP URL), CI_CACHE_BUCKET (its `mc`
path, default spice-minio/ci-cache), and an `mc` alias with write access to it.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
import urllib.error
import urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent
PINS = json.loads((HERE.parent / "ci-cache" / "pins.json").read_text())


def pinned_files() -> dict[str, str]:
    """sha256 -> source URL of every file the pins name."""
    files: dict[str, str] = {}
    deny = PINS["cargo-deny"]
    for target, sha256 in deny["sha256"].items():
        files[sha256] = deny["url"].format(version=deny["version"], target=target)
    rust = PINS["rust"]
    for toolchain in rust["toolchains"]:
        for host in rust["hosts"]:
            listing = subprocess.run(
                [sys.executable, str(HERE / "rust_dist_mirror.py"), "--toolchain", toolchain, "--host", host, "--list"],
                check=True,
                capture_output=True,
                text=True,
            ).stdout
            for line in listing.splitlines():
                sha256, _, url = line.partition(" ")
                if not re.fullmatch(r"[0-9a-f]{64}", sha256) or not url.startswith("https://"):
                    sys.exit(f"rust_dist_mirror.py --list printed a line that is not '<sha256> <url>': {line!r}")
                files[sha256] = url
    return files


def cached(cache_url: str, sha256: str) -> bool:
    """Whether an anonymous read of the cache returns the pinned bytes.

    The whole object is read and hashed, not only checked for existence: an object with
    the wrong bytes under the right key would otherwise count as cached forever, and every
    job would quietly fall back to the source for it.
    """
    digest = hashlib.sha256()
    try:
        with urllib.request.urlopen(f"{cache_url}/sha256/{sha256}", timeout=60) as response:
            for chunk in iter(lambda: response.read(1 << 20), b""):
                digest.update(chunk)
    except urllib.error.HTTPError as error:
        if error.code == 404:
            return False
        raise
    if digest.hexdigest() != sha256:
        print(f"::warning::The cache's object sha256/{sha256} does not match its key; replacing it.")
        return False
    return True


def main() -> int:
    cache_url = os.environ["CI_CACHE_URL"].rstrip("/")
    bucket = os.environ.get("CI_CACHE_BUCKET", "spice-minio/ci-cache").rstrip("/")
    files = pinned_files()
    added = 0
    for sha256, url in sorted(files.items(), key=lambda item: item[1]):
        name = url.rsplit("/", 1)[-1]
        if cached(cache_url, sha256):
            print(f"cached   {name}")
            continue
        with tempfile.TemporaryDirectory() as scratch:
            path = Path(scratch) / name
            # Straight from the source: the cache was just found not to have it.
            env = {key: value for key, value in os.environ.items() if key not in ("CI_CACHE_URL", "CI_CACHE_DIR")}
            subprocess.run(["bash", str(HERE / "cached_fetch.sh"), sha256, str(path), url], check=True, env=env)
            subprocess.run(["bash", str(HERE / "mc_cp.sh"), str(path), f"{bucket}/sha256/{sha256}"], check=True)
        if not cached(cache_url, sha256):
            print(f"::error::{name} was uploaded to {bucket}/sha256/{sha256}, but an anonymous read of it failed, so jobs cannot use it. Check the bucket's anonymous read policy.")
            return 1
        print(f"added    {name}")
        added += 1
    print(f"{len(files)} pinned files: {added} added, {len(files) - added} already cached.")
    return 0


if __name__ == "__main__":
    sys.stdout.reconfigure(line_buffering=True)
    sys.exit(main())
