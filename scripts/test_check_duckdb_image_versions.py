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
"""Tests for check_duckdb_image_versions.py, run against synthetic repositories."""

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from check_duckdb_image_versions import check  # noqa: E402

AMD64 = "a" * 64
ARM64 = "b" * 64


def dockerfile(version="2.0.0", amd64=AMD64, arm64=ARM64, copy=True) -> str:
    lines = ["FROM ubuntu:26.04 AS libduckdb"]
    if version is not None:
        lines.append(f"ARG DUCKDB_VERSION={version}")
    if amd64 is not None:
        lines.append(f"ARG DUCKDB_SHA256_AMD64={amd64}")
    if arm64 is not None:
        lines.append(f"ARG DUCKDB_SHA256_ARM64={arm64}")
    lines.append("FROM ubuntu:26.04")
    if copy:
        lines.append("COPY --from=libduckdb /root/libduckdb.so /usr/lib/libduckdb.so")
    return "\n".join(lines) + "\n"


class CheckTest(unittest.TestCase):
    def run_check(self, files: dict[str, str]) -> list[str]:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "test").mkdir()
            (root / "test" / "duckdb_versions.json").write_text(
                json.dumps(
                    {
                        "versions": [
                            {"version": "1.5.6", "sha256": {"linux-amd64": "c" * 64, "linux-arm64": "d" * 64}},
                            {"version": "2.0.0", "sha256": {"linux-amd64": AMD64, "linux-arm64": ARM64}},
                        ]
                    }
                )
            )
            for name, text in files.items():
                (root / name).write_text(text)
            return check(root)

    def test_matching_images_pass(self):
        self.assertEqual(
            self.run_check(
                {
                    "Dockerfile": dockerfile(),
                    # An amd64-only image needs no arm64 checksum, and may copy
                    # outside a sandbox.
                    "Dockerfile-cuda-release": dockerfile(arm64=None).replace(
                        "/usr/lib/", "/spice_sandbox/usr/lib/"
                    ),
                }
            ),
            [],
        )

    def test_image_without_libduckdb_fails(self):
        self.assertEqual(
            self.run_check({"Dockerfile": dockerfile(), "Dockerfile.local": "FROM ubuntu:26.04\n"}),
            [
                "Dockerfile.local ships no libduckdb: add the pinned download of DuckDB 2.0.0 "
                "(ARG DUCKDB_VERSION and its checksums), as in Dockerfile"
            ],
        )

    def test_older_version_fails(self):
        self.assertEqual(
            self.run_check({"Dockerfile": dockerfile(version="1.5.6")}),
            [
                "Dockerfile has ARG DUCKDB_VERSION=1.5.6, but the newest release in "
                "test/duckdb_versions.json is DuckDB 2.0.0, which needs DUCKDB_VERSION=2.0.0"
            ],
        )

    def test_wrong_checksums_fail(self):
        self.assertEqual(
            self.run_check({"Dockerfile": dockerfile(amd64="e" * 64, arm64="f" * 64)}),
            [
                f"Dockerfile has ARG DUCKDB_SHA256_AMD64={'e' * 64}, but the newest release in "
                f"test/duckdb_versions.json is DuckDB 2.0.0, which needs DUCKDB_SHA256_AMD64={AMD64}",
                f"Dockerfile has ARG DUCKDB_SHA256_ARM64={'f' * 64}, but the newest release in "
                f"test/duckdb_versions.json is DuckDB 2.0.0, which needs DUCKDB_SHA256_ARM64={ARM64}",
            ],
        )

    def test_missing_amd64_checksum_fails(self):
        self.assertEqual(
            self.run_check({"Dockerfile": dockerfile(amd64=None)}),
            ["Dockerfile has no ARG DUCKDB_SHA256_AMD64"],
        )

    def test_download_without_copy_fails(self):
        self.assertEqual(
            self.run_check({"Dockerfile": dockerfile(copy=False)}),
            ["Dockerfile downloads libduckdb but never copies it to /usr/lib/libduckdb.so"],
        )

    def test_duplicate_arg_fails(self):
        self.assertEqual(
            self.run_check({"Dockerfile": dockerfile() + "ARG DUCKDB_VERSION=2.0.0\n"}),
            ["Dockerfile declares ARG DUCKDB_VERSION 2 times"],
        )


if __name__ == "__main__":
    unittest.main()
