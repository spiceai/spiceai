#!/usr/bin/env python3
# Copyright 2024-2026 The Spice.ai OSS Authors
#
# Install DuckDB's release shared library and print the directory that holds
# its ADBC driver manifest, for `ADBC_DRIVER_PATH`.
#
# The tests that use DuckDB as an oracle load an installed `libduckdb` through
# ADBC instead of compiling DuckDB into the test binaries, and so do `duckdb:`
# and `ducklake:` datasets. `make nextest`, `make nextest-packages` and the
# `setup-libduckdb` CI action run this, so a sign-off, the merge queue and CI
# all load the same release without anyone installing it by hand.
#
# The release defaults to the newest one `test/duckdb_versions.json` lists
# (`LIBDUCKDB_VERSION` or `--version` picks another listed one). Each archive
# is checked against the SHA-256 recorded there before anything is extracted;
# a release or platform with no recorded checksum is an error, never an
# unchecked download. The library is cached under
# `${LIBDUCKDB_CACHE_DIR:-~/.cache/spice/libduckdb}/<version>/`, so only the
# first run downloads.
#
# Usage:
#   scripts/libduckdb.py [--version VERSION] [--cache-dir DIR]
#
# Pure stdlib; no third-party deps.

from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import platform
import shutil
import sys
import tarfile
import tempfile
import time
import urllib.request
import zipfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
VERSIONS_FILE = REPO_ROOT / "test" / "duckdb_versions.json"
MANIFEST = "duckdb.toml"


class Error(Exception):
    pass


def release_platform() -> tuple[str, str]:
    """DuckDB's name for this platform's release, and the library file in it."""
    system, machine = platform.system(), platform.machine().lower()
    if system == "Darwin":
        return "osx-universal", "libduckdb.dylib"
    if system == "Linux" and machine in ("x86_64", "amd64"):
        return "linux-amd64", "libduckdb.so"
    if system == "Linux" and machine in ("aarch64", "arm64"):
        return "linux-arm64", "libduckdb.so"
    raise Error(f"DuckDB publishes no shared library for {system}/{machine}")


def listed_release(version: str | None) -> dict:
    releases = json.loads(VERSIONS_FILE.read_text())["versions"]
    if not releases:
        raise Error(f"{VERSIONS_FILE} lists no DuckDB releases")
    if version is None:
        return releases[-1]
    version = version.removeprefix("v")
    for release in releases:
        if release["version"] == version:
            return release
    listed = ", ".join(r["version"] for r in releases)
    raise Error(
        f"DuckDB {version} is not listed in {VERSIONS_FILE} (listed: {listed}). "
        "Add it there with its archive checksums to use it."
    )


def archive_url(version: str, release: str) -> str:
    base = f"https://github.com/duckdb/duckdb/releases/download/v{version}"
    # Releases from 2.0 ship the library as duckdb-shared-libs-<platform>.tar.gz.
    if int(version.split(".")[0]) >= 2:
        return f"{base}/duckdb-shared-libs-{release}.tar.gz"
    return f"{base}/libduckdb-{release}.zip"


def download(url: str, version: str, attempts: int = 5) -> bytes:
    """The archive at `url`, retried with backoff so a transient release-CDN
    5xx or connection reset does not fail a sign-off."""
    for attempt in range(1, attempts + 1):
        try:
            with urllib.request.urlopen(url, timeout=300) as response:
                return response.read()
        except OSError as e:
            if attempt == attempts:
                raise Error(
                    f"Failed to download DuckDB {version} from {url} after {attempts} attempts: {e}"
                ) from e
            print(f"warning: downloading {url} failed ({e}); retrying", file=sys.stderr)
            time.sleep(2**attempt)
    raise AssertionError("unreachable")


def extract(archive: bytes, url: str, library: str) -> bytes:
    if url.endswith(".zip"):
        with zipfile.ZipFile(io.BytesIO(archive)) as zf:
            names = [n for n in zf.namelist() if Path(n).name == library]
            if names:
                return zf.read(names[0])
    else:
        with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as tf:
            for member in tf.getmembers():
                if member.isfile() and Path(member.name).name == library:
                    data = tf.extractfile(member)
                    if data is not None:
                        return data.read()
    raise Error(f"{url} does not contain {library}")


def install(version: str | None, cache_dir: Path) -> Path:
    cache_dir = cache_dir.expanduser().resolve()
    entry = listed_release(version)
    version = entry["version"]
    release, library = release_platform()
    expected = entry.get("sha256", {}).get(release)
    if not expected:
        raise Error(
            f"{VERSIONS_FILE} records no sha256 for DuckDB {version} on {release}. "
            "Add the checksum GitHub publishes for the release archive."
        )

    target = cache_dir / version
    manifest = target / MANIFEST
    stamp = target / ".sha256"
    if manifest.is_file() and stamp.is_file() and stamp.read_text().strip() == expected:
        return target

    url = archive_url(version, release)
    archive = download(url, version)
    actual = hashlib.sha256(archive).hexdigest()
    if actual != expected:
        raise Error(
            f"DuckDB {version} archive {url} has sha256 {actual}, but "
            f"{VERSIONS_FILE} records {expected}"
        )

    # Build the directory beside its final place and rename it in, so a
    # concurrent run never sees a manifest without its library.
    cache_dir.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{version}-", dir=cache_dir))
    staging.chmod(0o755)
    try:
        (staging / library).write_bytes(extract(archive, url, library))
        (staging / MANIFEST).write_text(
            "[Driver]\n"
            f'shared = "{target / library}"\n'
            'entrypoint = "duckdb_adbc_init"\n'
        )
        (staging / ".sha256").write_text(expected + "\n")
        if target.exists():
            shutil.rmtree(target)
        try:
            staging.rename(target)
        except OSError:
            # Another run installed the same release first.
            if not (stamp.is_file() and stamp.read_text().strip() == expected):
                raise
    finally:
        shutil.rmtree(staging, ignore_errors=True)
    return target


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", default=os.environ.get("LIBDUCKDB_VERSION") or None)
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=Path(
            os.environ.get("LIBDUCKDB_CACHE_DIR")
            or Path.home() / ".cache" / "spice" / "libduckdb"
        ),
    )
    args = parser.parse_args()
    try:
        print(install(args.version, args.cache_dir))
    except Error as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
