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
"""Keep filesystem calls out of Cayenne's data path.

Cayenne reads and writes data files through each dataset's `ObjectStore`, so the
same code serves a local directory and a bucket. A `std::fs` or `tokio::fs` call in
`crates/cayenne/src/provider/` bypasses the store: it writes beside an object-store
dataset instead of into it, or reads a path that only exists on one machine.

The calls that remain belong to the local-directory branches that have not moved
onto the store yet. `scripts/cayenne_data_path_fs_allowlist.json` records how many
each file has. This check fails when a file has more than its entry allows, and
when a file has fewer (lower the entry, so the list only ever shrinks). Test code
(`#[cfg(test)]` modules and files under `tests/`) is not counted.

Usage: check_cayenne_data_path_fs.py [--update]
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA_PATH = ROOT / "crates" / "cayenne" / "src" / "provider"
ALLOWLIST = ROOT / "scripts" / "cayenne_data_path_fs_allowlist.json"

FS_CALL = re.compile(r"\b(?:std|tokio)::fs\b")
# An inline test module runs to the end of its file in this crate's layout.
TEST_MODULE = re.compile(r"^\s*#\[cfg\(test\)\]\s*\n\s*(?:pub(?:\([^)]*\))?\s+)?mod\s+\w+\s*\{", re.M)


def is_test_file(path: Path, data_path: Path = DATA_PATH) -> bool:
    parts = path.relative_to(data_path).parts
    return "tests" in parts[:-1] or path.name in {"tests.rs", "test_utils.rs"} or path.stem.endswith("_tests")


def production_text(text: str) -> str:
    match = TEST_MODULE.search(text)
    return text[: match.start()] if match else text


def strip_comments(text: str) -> str:
    return "\n".join(line.split("//", 1)[0] for line in text.splitlines())


def count_calls(text: str) -> int:
    return len(FS_CALL.findall(strip_comments(production_text(text))))


def scan(data_path: Path = DATA_PATH) -> dict[str, int]:
    counts: dict[str, int] = {}
    for path in sorted(data_path.rglob("*.rs")):
        if is_test_file(path, data_path):
            continue
        n = count_calls(path.read_text(encoding="utf-8"))
        if n:
            counts[str(path.relative_to(ROOT))] = n
    return counts


def compare(found: dict[str, int], allowed: dict[str, int]) -> list[str]:
    problems = []
    for file, n in sorted(found.items()):
        limit = allowed.get(file, 0)
        if n > limit:
            problems.append(
                f"{file}: {n} filesystem call(s), {limit} allowed. Read and write data files through the "
                "table's ObjectStore (see crate::resolve_object_url and the table's object_store_config) "
                "instead of std::fs/tokio::fs."
            )
        elif n < limit:
            problems.append(
                f"{file}: {n} filesystem call(s), the allowlist still allows {limit}. "
                "Lower its entry in scripts/cayenne_data_path_fs_allowlist.json (run with --update)."
            )
    for file, limit in sorted(allowed.items()):
        if file not in found and limit:
            problems.append(
                f"{file}: no filesystem calls left, the allowlist still allows {limit}. "
                "Remove its entry (run with --update)."
            )
    return problems


def main(argv: list[str]) -> int:
    found = scan()
    if "--update" in argv:
        ALLOWLIST.write_text(json.dumps(found, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        print(f"Wrote {ALLOWLIST.relative_to(ROOT)}: {sum(found.values())} calls in {len(found)} files")
        return 0
    allowed = json.loads(ALLOWLIST.read_text(encoding="utf-8"))
    problems = compare(found, allowed)
    for problem in problems:
        print(f"error: {problem}", file=sys.stderr)
    if problems:
        return 1
    print(f"Cayenne data path: {sum(found.values())} allowlisted filesystem calls in {len(found)} files")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
