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
# `${LIBDUCKDB_CACHE_DIR:-~/.cache/spice/libduckdb}/<version>/`, with the archive
# it came from, so only the first run downloads. A cached install is never taken
# on trust, since CI keeps it in a directory another job on the host can write:
# each run hashes the kept archive against the recorded SHA-256, and reuses the
# install only when its library is the one that archive holds, byte for byte,
# and its manifest is the one this script writes. Anything else is replaced from
# a fresh, checked download.
#
# Usage:
#   scripts/libduckdb.py [--version VERSION] [--cache-dir DIR]
#
# Pure stdlib; no third-party deps.

from __future__ import annotations

import argparse
import fcntl
import hashlib
import http.client
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
# The verified release archive, kept beside the library it was extracted from.
ARCHIVE = "release-archive"


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


def download(url: str, version: str, attempts: int = 8) -> bytes:
    """The archive at `url`, retried with backoff so a transient release-CDN
    5xx, connection reset or truncated body does not fail a sign-off. A
    truncated body raises `http.client.IncompleteRead`, which is not an
    `OSError`.

    The waits double from 2s and stop growing at 60s, about three minutes in
    all over eight attempts: GitHub's release downloads answer 504 in bursts
    that outlast the 30s the first four waits add up to."""
    for attempt in range(1, attempts + 1):
        try:
            with urllib.request.urlopen(url, timeout=300) as response:
                return response.read()
        except (OSError, http.client.HTTPException) as e:
            if attempt == attempts:
                raise Error(
                    f"Failed to download DuckDB {version} from {url} after {attempts} attempts: {e}"
                ) from e
            print(f"warning: downloading {url} failed ({e}); retrying", file=sys.stderr)
            time.sleep(min(2**attempt, 60))
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
    # The directory printed goes into ADBC_DRIVER_PATH, which CI writes as one line of
    # GITHUB_ENV, so a path that could break a line, or end a value, is refused.
    if any(ord(ch) < 0x20 or ord(ch) == 0x7F for ch in str(cache_dir)):
        raise Error(f"the cache directory {str(cache_dir)!r} has a control character in its path")
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
    # A complete install is never replaced, so it can be reused without the lock.
    if installed(target, library, expected):
        return target

    # One run installs a release at a time. Without the lock, a run that found
    # no install could publish over one another run had just returned, and the
    # caller of that run would find its manifest gone.
    cache_dir.mkdir(parents=True, exist_ok=True)
    try:
        # Never through a symbolic link, which would truncate whatever it points at.
        fd = os.open(cache_dir / f".{version}.lock", os.O_WRONLY | os.O_CREAT | os.O_NOFOLLOW, 0o644)
    except OSError as e:
        raise Error(f"cannot open the lock {cache_dir / f'.{version}.lock'}: {e}") from e
    with os.fdopen(fd, "w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if installed(target, library, expected):
            return target
        publish(target, library, expected, version, release)
    return target


def publish(target: Path, library: str, expected: str, version: str, release: str) -> None:
    """Download, check and install the release at `target`. The caller holds
    the release's lock and found no complete install there."""
    url = archive_url(version, release)
    archive = download(url, version)
    actual = sha256(archive)
    if actual != expected:
        raise Error(
            f"DuckDB {version} archive {url} has sha256 {actual}, but "
            f"{VERSIONS_FILE} records {expected}"
        )

    # Build the directory beside its final place and rename it in, so a run
    # taking the unlocked path never sees a manifest without its library.
    staging = Path(tempfile.mkdtemp(prefix=f".{version}-", dir=target.parent))
    staging.chmod(0o755)
    old = None
    try:
        (staging / library).write_bytes(extract(archive, url, library))
        (staging / MANIFEST).write_text(manifest(target, library))
        # Written last: `installed` needs the archive, so an install without it
        # is incomplete and replaced.
        (staging / ARCHIVE).write_bytes(archive)
        # An incomplete install is renamed aside, not deleted in place, so the
        # directory is only ever absent between two renames.
        if target.exists():
            old = Path(tempfile.mkdtemp(prefix=f".{version}-old-", dir=target.parent))
            target.rename(old / "old")
        staging.rename(target)
    finally:
        shutil.rmtree(staging, ignore_errors=True)
        if old is not None:
            shutil.rmtree(old, ignore_errors=True)


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def manifest(target: Path, library: str) -> str:
    """The driver manifest for the library at `target`."""
    # A JSON string is a valid TOML basic string, so a quote or backslash in
    # the cache path is escaped rather than ending the value.
    shared = json.dumps(str(target / library), ensure_ascii=False)
    return f'[Driver]\nshared = {shared}\nentrypoint = "duckdb_adbc_init"\n'


def installed(target: Path, library: str, expected: str) -> bool:
    """Whether `target` holds a complete, unaltered install of the archive with
    SHA-256 `expected`. Nothing there is taken on trust: the kept archive is
    hashed against `expected`, the library must be the one that archive holds,
    byte for byte, and the manifest the one `publish` writes. A symbolic link,
    a missing or extra file, or any difference means the install is replaced."""
    files = [target / ARCHIVE, target / library, target / MANIFEST]
    if target.is_symlink() or not target.is_dir():
        return False
    # Nothing else either: the directory goes first on ADBC_DRIVER_PATH, where another
    # driver manifest would be found.
    if sorted(os.listdir(target)) != sorted(path.name for path in files):
        return False
    if any(path.is_symlink() or not path.is_file() for path in files):
        return False
    archive = (target / ARCHIVE).read_bytes()
    if sha256(archive) != expected:
        return False
    try:
        held = extract(archive, archive_url(target.name, release_platform()[0]), library)
    except (Error, OSError, ValueError, EOFError, zipfile.BadZipFile, tarfile.TarError):
        return False
    return (
        sha256((target / library).read_bytes()) == sha256(held)
        and (target / MANIFEST).read_text() == manifest(target, library)
    )


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
