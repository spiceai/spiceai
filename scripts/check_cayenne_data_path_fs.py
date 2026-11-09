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

`scripts/cayenne_data_path_fs_allowlist.json` records the calls that remain, in two
groups:

- `host_features`: calls that need a filesystem path and touch no data file through
  it: cgroup and procfs reads, sync and page-cache primitives, the direct compaction
  writer, the data-directory size probe. Keyed by file, or by `file::function` when
  the rest of the file is data path. These stay.
- `local_data_path`: the data path of a local dataset with
  `runtime.params.cayenne_vnext` off. With the flag on, a local dataset reads and
  writes through its `file://` store (`cayenne::dataset_storage`), so these calls
  run only with the flag off; they go when the flag-off local path does
  (#16134, #16138).

The check fails when a key has more calls than its entry allows, and when it has
fewer (lower the entry, so the list only ever shrinks). Test code (`#[cfg(test)]`
modules and files under `tests/`) is not counted.

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
FN_START = re.compile(r"^\s*(?:pub(?:\([^)]*\))?\s+)?(?:const\s+)?(?:async\s+)?(?:unsafe\s+)?fn\s+(\w+)")
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


def calls_by_function(text: str) -> dict[str, int]:
    """Filesystem calls per enclosing function (the last `fn` line above each call)."""
    counts: dict[str, int] = {}
    function = ""
    for line in strip_comments(production_text(text)).splitlines():
        match = FN_START.match(line)
        if match:
            function = match.group(1)
        n = len(FS_CALL.findall(line))
        if n:
            counts[function] = counts.get(function, 0) + n
    return counts


def count_calls(text: str) -> int:
    return sum(calls_by_function(text).values())


def scan(data_path: Path = DATA_PATH, host_keys: set[str] | frozenset[str] = frozenset()) -> dict[str, dict[str, int]]:
    """Calls per allowlist key, split into `host_features` and `local_data_path`.

    A call is a host feature when its `file::function` or its file is a key in
    `host_keys`; every other call counts against its file in `local_data_path`.
    """
    found: dict[str, dict[str, int]] = {"host_features": {}, "local_data_path": {}}
    for path in sorted(data_path.rglob("*.rs")):
        if is_test_file(path, data_path):
            continue
        file = str(path.relative_to(ROOT))
        for function, n in calls_by_function(path.read_text(encoding="utf-8")).items():
            if f"{file}::{function}" in host_keys:
                group, key = "host_features", f"{file}::{function}"
            elif file in host_keys:
                group, key = "host_features", file
            else:
                group, key = "local_data_path", file
            found[group][key] = found[group].get(key, 0) + n
    return found


def compare(found: dict[str, int], allowed: dict[str, int], group: str = "local_data_path") -> list[str]:
    problems = []
    for key, n in sorted(found.items()):
        limit = allowed.get(key, 0)
        if n > limit:
            problems.append(
                f"{key}: {n} filesystem call(s), {limit} allowed. Read and write data files through the "
                "dataset's store (`cayenne::dataset_storage::DatasetStorage`, the table's object_store_config) "
                "instead of std::fs/tokio::fs. A call that needs a filesystem path and touches no data file "
                "(a probe, a sync primitive) belongs under `host_features`."
            )
        elif n < limit:
            problems.append(
                f"{key}: {n} filesystem call(s), the allowlist still allows {limit}. "
                f"Lower its entry under `{group}` in scripts/cayenne_data_path_fs_allowlist.json (run with --update)."
            )
    for key, limit in sorted(allowed.items()):
        if key not in found and limit:
            problems.append(
                f"{key}: no filesystem calls left, the allowlist still allows {limit}. "
                f"Remove its entry under `{group}` (run with --update)."
            )
    return problems


def main(argv: list[str]) -> int:
    allowed = json.loads(ALLOWLIST.read_text(encoding="utf-8"))
    host_keys = set(allowed.get("host_features", {}))
    found = scan(host_keys=host_keys)
    if "--update" in argv:
        ALLOWLIST.write_text(json.dumps(found, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        print(
            f"Wrote {ALLOWLIST.relative_to(ROOT)}: {sum(found['host_features'].values())} host-feature and "
            f"{sum(found['local_data_path'].values())} local data-path calls"
        )
        return 0
    problems = []
    for group in ("host_features", "local_data_path"):
        problems += compare(found[group], allowed.get(group, {}), group)
    for problem in problems:
        print(f"error: {problem}", file=sys.stderr)
    if problems:
        return 1
    print(
        f"Cayenne data path: {sum(found['host_features'].values())} host-feature filesystem calls in "
        f"{len(found['host_features'])} entries; {sum(found['local_data_path'].values())} flag-off local "
        f"data-path calls in {len(found['local_data_path'])} files"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
