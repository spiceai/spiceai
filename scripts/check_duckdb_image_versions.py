#!/usr/bin/env python3
# Copyright 2024-2026 The Spice.ai OSS Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Guard: every spiced image ships the newest DuckDB in test/duckdb_versions.json.

`duckdb:` and `ducklake:` datasets load the DuckDB library installed on the
host, so each image built from a `Dockerfile*` in the repository root downloads
`libduckdb.so` and copies it onto the default library path. A Dockerfile that
drops the library, or pins a DuckDB other than the one CI tests by default (the
last entry of test/duckdb_versions.json), ships an image whose DuckDB sources
fail to load or run on an untested release. This check fails on either.

Usage: scripts/check_duckdb_image_versions.py [REPO_ROOT]
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

VERSIONS_FILE = Path("test") / "duckdb_versions.json"
ARGS = {
    "DUCKDB_VERSION": None,
    "DUCKDB_SHA256_AMD64": "linux-amd64",
    "DUCKDB_SHA256_ARM64": "linux-arm64",
}
COPY_RE = re.compile(r"^COPY\s+--from=\S+\s+/root/libduckdb\.so\s+\S*/usr/lib/libduckdb\.so\s*$", re.M)


def dockerfiles(root: Path) -> list[Path]:
    return sorted(p for p in root.glob("Dockerfile*") if p.is_file())


def check(root: Path) -> list[str]:
    versions = json.loads((root / VERSIONS_FILE).read_text())["versions"]
    if not versions:
        return [f"{VERSIONS_FILE} lists no DuckDB releases"]
    newest = versions[-1]
    expected = {
        arg: newest["version"] if platform is None else newest["sha256"][platform]
        for arg, platform in ARGS.items()
    }

    errors = []
    files = dockerfiles(root)
    if not files:
        errors.append(f"no Dockerfile* found in {root}")
    for path in files:
        text = path.read_text()
        values = {
            arg: re.findall(rf"^ARG\s+{arg}=(\S*)\s*$", text, re.M) for arg in ARGS
        }
        if not values["DUCKDB_VERSION"]:
            errors.append(
                f"{path.name} ships no libduckdb: add the pinned download of DuckDB "
                f"{newest['version']} (ARG DUCKDB_VERSION and its checksums), as in Dockerfile"
            )
            continue
        for arg, found in values.items():
            if len(found) > 1:
                errors.append(f"{path.name} declares ARG {arg} {len(found)} times")
                continue
            # Every image needs the version and the amd64 checksum; an image
            # built only for amd64 (Dockerfile-cuda-release) has no arm64 one.
            if not found:
                if arg != "DUCKDB_SHA256_ARM64":
                    errors.append(f"{path.name} has no ARG {arg}")
                continue
            if found[0] != expected[arg]:
                errors.append(
                    f"{path.name} has ARG {arg}={found[0]}, but the newest release in "
                    f"{VERSIONS_FILE} is DuckDB {newest['version']}, which needs "
                    f"{arg}={expected[arg]}"
                )
        if not COPY_RE.search(text):
            errors.append(
                f"{path.name} downloads libduckdb but never copies it to /usr/lib/libduckdb.so"
            )
    return errors


def main() -> int:
    root = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).resolve().parent.parent
    errors = check(root)
    for error in errors:
        print(f"error: {error}", file=sys.stderr)
    if errors:
        return 1
    print(f"{len(dockerfiles(root))} Dockerfiles ship the newest DuckDB in {VERSIONS_FILE}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
