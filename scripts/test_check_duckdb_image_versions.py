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


def download(tag="v${DUCKDB_VERSION}", amd64="${DUCKDB_SHA256_AMD64}", arm64="${DUCKDB_SHA256_ARM64}", multi_arch=True) -> list[str]:
    """The download step as the real Dockerfiles write it."""
    url = f"https://github.com/duckdb/duckdb/releases/download/{tag}/duckdb-shared-libs-"
    if not multi_arch:
        return [
            "RUN set -eux; \\",
            f'    curl -fSLo /tmp/libduckdb.tar.gz "{url}linux-amd64.tar.gz"; \\',
            f'    echo "{amd64}  /tmp/libduckdb.tar.gz" | sha256sum -c -; \\',
            "    tar -C /root -xzf /tmp/libduckdb.tar.gz libduckdb.so",
        ]
    return [
        "RUN set -eux; \\",
        '    case "${TARGETARCH}" in \\',
        f'      amd64|"") duckdb_platform=linux-amd64; duckdb_sha256="{amd64}" ;; \\',
        f'      arm64) duckdb_platform=linux-arm64; duckdb_sha256="{arm64}" ;; \\',
        "    esac; \\",
        f'    curl -fSLo /tmp/libduckdb.tar.gz "{url}${{duckdb_platform}}.tar.gz"; \\',
        '    echo "${duckdb_sha256}  /tmp/libduckdb.tar.gz" | sha256sum -c -; \\',
        "    tar -C /root -xzf /tmp/libduckdb.tar.gz libduckdb.so",
    ]


def dockerfile(
    version="2.0.0",
    amd64=AMD64,
    arm64=ARM64,
    copy=True,
    platforms=("linux-amd64", "linux-arm64"),
    steps=None,
) -> str:
    lines = ["FROM ubuntu:26.04 AS libduckdb"]
    if version is not None:
        lines.append(f"ARG DUCKDB_VERSION={version}")
    if amd64 is not None:
        lines.append(f"ARG DUCKDB_SHA256_AMD64={amd64}")
    if arm64 is not None:
        lines.append(f"ARG DUCKDB_SHA256_ARM64={arm64}")
    lines += steps if steps is not None else download(multi_arch="linux-arm64" in platforms)
    lines.append("FROM ubuntu:26.04")
    if copy:
        lines.append("COPY --from=libduckdb /root/libduckdb.so /usr/lib/libduckdb.so")
    return "\n".join(lines) + "\n"


UNUSED_STAGE = (
    "{} copies libduckdb only into a build stage the final image does not include: copy it to "
    "/usr/lib/libduckdb.so in the final stage, or to /spice_sandbox/usr/lib/libduckdb.so in the stage "
    "whose /spice_sandbox/ the final stage copies onto /"
)


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
                    # An amd64-only image needs no arm64 checksum, and a sandboxed
                    # image may copy into the sandbox root it copies onto /.
                    "Dockerfile-cuda-release": dockerfile(arm64=None, platforms=("linux-amd64",))
                    .replace("/usr/lib/", "/spice_sandbox/usr/lib/")
                    .replace("FROM ubuntu:26.04\n", "FROM ubuntu:26.04 AS sandbox-setup\n")
                    + "FROM scratch\nCOPY --from=sandbox-setup /spice_sandbox/ /\n",
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

    def test_missing_arm64_checksum_fails_for_a_multi_arch_image(self):
        self.assertEqual(
            self.run_check({"Dockerfile.local": dockerfile(arm64=None)}),
            ["Dockerfile.local has no ARG DUCKDB_SHA256_ARM64"],
        )

    def test_download_without_copy_fails(self):
        self.assertEqual(
            self.run_check({"Dockerfile": dockerfile(copy=False)}),
            [
                "Dockerfile downloads libduckdb but never copies it to /usr/lib/libduckdb.so "
                "(or /spice_sandbox/usr/lib/libduckdb.so in a sandboxed image)"
            ],
        )

    def test_copy_outside_the_library_path_fails(self):
        self.assertEqual(
            self.run_check({"Dockerfile": dockerfile().replace(" /usr/lib/", " /opt/usr/lib/")}),
            [
                "Dockerfile downloads libduckdb but never copies it to /usr/lib/libduckdb.so "
                "(or /spice_sandbox/usr/lib/libduckdb.so in a sandboxed image)"
            ],
        )

    def test_sandbox_destination_needs_the_sandbox_root_copied_onto_root(self):
        sandboxed = dockerfile().replace(" /usr/lib/", " /spice_sandbox/usr/lib/")
        self.assertEqual(
            self.run_check({"Dockerfile": sandboxed}),
            [UNUSED_STAGE.format("Dockerfile")],
        )
        # The final stage must copy the sandbox root of the stage that holds the library.
        sandboxed = sandboxed.replace("FROM ubuntu:26.04\n", "FROM ubuntu:26.04 AS sandbox-setup\n")
        self.assertEqual(
            self.run_check({"Dockerfile": sandboxed + "FROM scratch\nCOPY --from=libduckdb /spice_sandbox/ /\n"}),
            [UNUSED_STAGE.format("Dockerfile")],
        )
        self.assertEqual(
            self.run_check({"Dockerfile": sandboxed + "FROM scratch\nCOPY --from=sandbox-setup /spice_sandbox/ /\n"}),
            [],
        )

    def test_copy_into_a_stage_the_image_does_not_include_fails(self):
        unused = dockerfile().replace("FROM ubuntu:26.04\n", "FROM ubuntu:26.04 AS unused\n") + "FROM ubuntu:26.04\n"
        self.assertEqual(self.run_check({"Dockerfile": unused}), [UNUSED_STAGE.format("Dockerfile")])
        # A final stage built FROM the stage that copied the library includes it.
        self.assertEqual(self.run_check({"Dockerfile": unused.replace("FROM ubuntu:26.04\n", "FROM unused\n")}), [])

    def test_commented_out_verification_fails(self):
        # Docker drops the commented line from the continued RUN, so nothing
        # verifies the archive.
        steps = [("#" + line) if "sha256sum" in line else line for line in download()]
        self.assertEqual(
            self.run_check({"Dockerfile": dockerfile(steps=steps)}),
            ["Dockerfile never verifies the DuckDB archive with sha256sum -c"],
        )

    def test_download_of_another_release_fails(self):
        # The ARGs still say 2.0.0, but the image installs 1.5.6.
        self.assertEqual(
            self.run_check(
                {"Dockerfile": dockerfile(steps=download(tag="v1.5.6", amd64="c" * 64, arm64="d" * 64))}
            ),
            [
                "Dockerfile downloads DuckDB release v1.5.6: use v${DUCKDB_VERSION} so the pinned version is the one installed",
                f'Dockerfile sets duckdb_sha256="{"c" * 64}": set it from ${{DUCKDB_SHA256_AMD64}} or ${{DUCKDB_SHA256_ARM64}}',
                f'Dockerfile sets duckdb_sha256="{"d" * 64}": set it from ${{DUCKDB_SHA256_AMD64}} or ${{DUCKDB_SHA256_ARM64}}',
            ],
        )

    def test_literal_checksum_in_an_amd64_only_image_fails(self):
        self.assertEqual(
            self.run_check(
                {
                    "Dockerfile-cuda-release": dockerfile(
                        arm64=None,
                        platforms=("linux-amd64",),
                        steps=download(amd64="c" * 64, multi_arch=False),
                    )
                }
            ),
            [
                f"Dockerfile-cuda-release verifies the DuckDB archive against {'c' * 64}: "
                "use ${DUCKDB_SHA256_AMD64} or ${DUCKDB_SHA256_ARM64}"
            ],
        )

    def test_unverified_download_fails(self):
        steps = [line for line in download() if "sha256sum" not in line]
        self.assertEqual(
            self.run_check({"Dockerfile": dockerfile(steps=steps)}),
            ["Dockerfile never verifies the DuckDB archive with sha256sum -c"],
        )

    def test_duplicate_arg_fails(self):
        self.assertEqual(
            self.run_check({"Dockerfile": dockerfile() + "ARG DUCKDB_VERSION=2.0.0\n"}),
            ["Dockerfile declares ARG DUCKDB_VERSION 2 times"],
        )


if __name__ == "__main__":
    unittest.main()
