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
# The library must land on the default library path of the final image: copied
# there by the final stage (or a stage it is built `FROM`), or copied into the
# sandbox root of a stage whose whole `/spice_sandbox/` the final stage copies
# onto `/`. A copy in any other stage never reaches the image.
FROM_RE = re.compile(r"^FROM\s+(?:--\S+\s+)*(\S+)(?:\s+AS\s+(\S+))?\s*$", re.M | re.I)
COPY_RE = re.compile(
    r"^COPY\s+--from=\S+\s+/root/libduckdb\.so\s+(/spice_sandbox)?/usr/lib/libduckdb\.so\s*$", re.M
)
SANDBOX_ROOT_RE = re.compile(r"^COPY\s+--from=(\S+)\s+/spice_sandbox/?\s+/\s*$", re.M)
# Docker drops a line whose first non-blank character is `#`, even inside a
# continued RUN, so a commented-out step must not count.
COMMENT_RE = re.compile(r"^\s*#.*\n?", re.M)
# The ARGs only pin the image if the download consumes them: every DuckDB
# release URL must name `v${DUCKDB_VERSION}`, and every checksum the archive is
# verified against must come from a checksum ARG, never a literal.
DOWNLOAD_RE = re.compile(r"duckdb/duckdb/releases/download/([^/\s\"]+)/duckdb-shared-libs")
VERIFY_RE = re.compile(r"echo\s+\"([^\"\s]*)\s+\S*libduckdb\S*\"\s*\|\s*sha256sum\s+-c")
ASSIGN_RE = re.compile(r"\b(duckdb_sha256)=(\"[^\"]*\"|\S+?)\s*;")
CHECKSUM_ARGS = ("${DUCKDB_SHA256_AMD64}", "${DUCKDB_SHA256_ARM64}")


def download_errors(name: str, text: str) -> list[str]:
    errors = []
    urls = DOWNLOAD_RE.findall(text)
    if not urls:
        errors.append(f"{name} never downloads a DuckDB release")
    for tag in urls:
        if tag != "v${DUCKDB_VERSION}":
            errors.append(
                f"{name} downloads DuckDB release {tag}: use v${{DUCKDB_VERSION}} so the pinned version is the one installed"
            )
    assignments = ASSIGN_RE.findall(text)
    for var, value in assignments:
        if value.strip('"') not in CHECKSUM_ARGS:
            errors.append(
                f"{name} sets {var}={value}: set it from ${{DUCKDB_SHA256_AMD64}} or ${{DUCKDB_SHA256_ARM64}}"
            )
    checks = VERIFY_RE.findall(text)
    if not checks:
        errors.append(f"{name} never verifies the DuckDB archive with sha256sum -c")
    for checksum in checks:
        if checksum in CHECKSUM_ARGS or (checksum == "${duckdb_sha256}" and assignments):
            continue
        errors.append(
            f"{name} verifies the DuckDB archive against {checksum}: use ${{DUCKDB_SHA256_AMD64}} or ${{DUCKDB_SHA256_ARM64}}"
        )
    return errors


def stages(text: str) -> list[tuple[str, str | None, str]]:
    """Each build stage as (base image or stage, name, body)."""
    matches = list(FROM_RE.finditer(text))
    return [
        (m.group(1), m.group(2), text[m.end() : matches[i + 1].start() if i + 1 < len(matches) else len(text)])
        for i, m in enumerate(matches)
    ]


def lineage(all_stages: list[tuple[str, str | None, str]], index: int) -> list[str]:
    """The bodies of a stage and of every stage it is built `FROM`."""
    by_name = {name.lower(): i for i, (_, name, _) in enumerate(all_stages[:index]) if name}
    bodies = []
    while index is not None:
        base, _, body = all_stages[index]
        bodies.append(body)
        index = by_name.get(base.lower())
    return bodies


def copy_errors(name: str, text: str) -> list[str]:
    all_stages = stages(text)
    if not all_stages:
        return [f"{name} has no FROM"]
    names = {stage_name.lower(): i for i, (_, stage_name, _) in enumerate(all_stages) if stage_name}
    final = lineage(all_stages, len(all_stages) - 1)
    # An empty group is a copy straight to /usr/lib in the image itself.
    if any("" in COPY_RE.findall(body) for body in final):
        return []
    for body in final:
        for source in SANDBOX_ROOT_RE.findall(body):
            index = names.get(source.lower())
            if index is not None and any(
                "/spice_sandbox" in COPY_RE.findall(b) for b in lineage(all_stages, index)
            ):
                return []
    if COPY_RE.search(text):
        return [
            f"{name} copies libduckdb only into a build stage the final image does not include: copy it to "
            "/usr/lib/libduckdb.so in the final stage, or to /spice_sandbox/usr/lib/libduckdb.so in the stage "
            "whose /spice_sandbox/ the final stage copies onto /"
        ]
    return [
        f"{name} downloads libduckdb but never copies it to /usr/lib/libduckdb.so "
        "(or /spice_sandbox/usr/lib/libduckdb.so in a sandboxed image)"
    ]


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
        text = COMMENT_RE.sub("", path.read_text())
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
            # Every image needs the version and the amd64 checksum. Only an
            # image that never downloads the arm64 archive (Dockerfile-cuda-release
            # is built for amd64 alone) may leave out the arm64 checksum.
            if not found:
                if arg != "DUCKDB_SHA256_ARM64" or "linux-arm64" in text:
                    errors.append(f"{path.name} has no ARG {arg}")
                continue
            if found[0] != expected[arg]:
                errors.append(
                    f"{path.name} has ARG {arg}={found[0]}, but the newest release in "
                    f"{VERSIONS_FILE} is DuckDB {newest['version']}, which needs "
                    f"{arg}={expected[arg]}"
                )
        errors.extend(download_errors(path.name, text))
        errors.extend(copy_errors(path.name, text))
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
