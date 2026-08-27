#!/usr/bin/env python3
# Copyright 2024-2026 The Spice.ai OSS Authors
#
# Tests for scripts/check_fork_patches.py.
#
# The guard's live-tree run only exercises the shapes today's `Cargo.lock` and
# ledger happen to contain — and while both agree, it reports success whether or
# not it is still reading either file correctly. A regex that stopped matching
# would find zero forks, zero rows, and no disagreements, so the guard would pass
# green on a workspace it had gone blind to. The same reason
# `test_check_module_reachability.py` runs ahead of its guard.
#
# The cases below pin both parsers against the manifest spellings this workspace
# actually uses (`?rev=`, `?tag=`, `.git` and bare repository names) and pin each
# drift the guard exists to catch.
#
# Run: python3 scripts/test_check_fork_patches.py

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from check_fork_patches import (  # noqa: E402
    LEDGER,
    LOCK,
    drift,
    gap_accounting,
    ledger_pins,
    pinned_forks,
)

failures = 0
checks = 0

A = "a" * 40
B = "b" * 40
C = "c" * 40


def check(name: str, got, want) -> None:
    global failures, checks
    checks += 1
    if got == want:
        print(f"  ok: {name}")
    else:
        failures += 1
        print(f"  FAIL: {name}\n    got:  {got!r}\n    want: {want!r}")


def check_contains(name: str, haystack: list[str], needle: str) -> None:
    global failures, checks
    checks += 1
    if any(needle in item for item in haystack):
        print(f"  ok: {name}")
    else:
        failures += 1
        print(f"  FAIL: {name}\n    no item contains {needle!r}\n    items: {haystack!r}")


print("lockfile parsing")

# The three source spellings this workspace's lockfile uses: a `rev=` pin on a
# `.git` URL, a `tag=` pin (clickhouse-rs), and a bare repository name with no
# `.git` suffix (async-openai, tiberius, graph-rs-sdk).
LOCK_SAMPLE = f"""
[[package]]
name = "vortex-array"
version = "0.79.0"
source = "git+https://github.com/spiceai/vortex.git?rev={A}#{A}"

[[package]]
name = "vortex-io"
version = "0.79.0"
source = "git+https://github.com/spiceai/vortex.git?rev={A}#{A}"

[[package]]
name = "clickhouse-rs"
version = "0.2.2"
source = "git+https://github.com/spiceai/clickhouse-rs.git?tag=0.2.2#{B}"

[[package]]
name = "async-openai"
version = "0.32.0"
source = "git+https://github.com/spiceai/async-openai?rev={C}#{C}"

[[package]]
name = "spiceai"
version = "3.0.0"
source = "git+https://github.com/spiceai/spice-rs.git?rev={A}#{A}"

[[package]]
name = "tokio"
version = "1.0.0"
source = "registry+https://github.com/rust-lang/crates.io-index"

[[package]]
name = "duckdb"
version = "1.5.5"
source = "git+https://github.com/duckdb/duckdb-rs.git?rev={B}#{B}"
"""

parsed = pinned_forks(LOCK_SAMPLE)
check("a `.git` URL with a rev pin is read once per repo, not once per crate", parsed.get("vortex"), {A})
check("a tag pin resolves to the commit after the fragment", parsed.get("clickhouse-rs"), {B})
check("a repository name with no `.git` suffix is read", parsed.get("async-openai"), {C})
check("a non-spiceai git dependency is not a fork of ours", "duckdb-rs" in parsed, False)
check("a registry dependency is ignored", "tokio" in parsed, False)
check("a spiceai repo with no upstream is not audited", "spice-rs" in parsed, False)

# A `[patch]` the resolve did not apply. Cargo records it with the same
# name/version/source shape as a package, so a scan of every `source` line reads
# it as a second pin for a fork that is in fact built at one revision — and the
# guard then demands the workspace "resolve them to one", which nothing can do,
# because the entry pins nothing to begin with.
UNUSED_PATCH_SAMPLE = f"""
[[package]]
name = "datafusion-federation"
version = "0.4.2"
source = "git+https://github.com/spiceai/datafusion-federation.git?rev={A}#{A}"

[[patch.unused]]
name = "datafusion-federation"
version = "0.4.2"
source = "git+https://github.com/spiceai/datafusion-federation.git?rev={B}#{B}"
"""

check(
    "an unused `[patch]` entry is not a pin — it is in no crate's dependency graph",
    pinned_forks(UNUSED_PATCH_SAMPLE).get("datafusion-federation"),
    {A},
)

print("\nledger parsing")

LEDGER_SAMPLE = f"""
| Fork | Pinned revision | Branch | Spice patches | Guarded |
|---|---|---|---|---|
| [vortex](#vortex) | `{A}` | `spiceai-54` | 14 | 9 |
| [clickhouse-rs](#clickhouse-rs) | `{B}` | tag `0.2.2` | 2 | 0 |

Prose that mentions `{C}` in passing must not be read as a pin row.

| Patch | What breaks if it is lost | Loss | Guard |
|---|---|---|---|
| Arrow `Map` alias | every write fails | silent | some test |
"""

recorded = ledger_pins(LEDGER_SAMPLE)
check("a pin row is read from its link text", recorded.get("vortex"), [A])
check("a tag-pinned fork is read the same way", recorded.get("clickhouse-rs"), [B])
check("a revision mentioned in prose is not a pin row", len(recorded), 2)
check("a patch row is not mistaken for a pin row", "Arrow `Map` alias" in recorded, False)

print("\ndrift detection")

check("agreement is silent", drift({"vortex": {A}}, {"vortex": [A]}), [])
check_contains(
    "a moved pin is reported, and says to re-audit",
    drift({"vortex": {B}}, {"vortex": [A]}),
    "re-audit",
)
check_contains(
    "a fork with no ledger row is reported",
    drift({"vortex": {A}}, {}),
    "no row in docs/dev/fork_patches.md",
)
check_contains(
    "a ledger row for an unpinned fork is reported",
    drift({}, {"vortex": [A]}),
    "no longer pinned",
)
check_contains(
    "two rows for one fork is reported",
    drift({"vortex": {A}}, {"vortex": [A, B]}),
    "keep one per fork",
)
check_contains(
    "one fork pinned at two revisions at once is reported",
    drift({"vortex": {A, B}}, {"vortex": [A]}),
    "different revisions at once",
)

print("\ngap accounting")

GAPS_HEAD = """
| Patch | What breaks if it is lost | Loss | Guard |
|---|---|---|---|
| one | a thing | silent | **GAP** |
| two | another | silent | some test |
| three | a third | silent | **GAP** |

## Open gaps

**{count} rows above are marked GAP** — they have no repo-side guard.
"""

check("a count matching the tables is silent", gap_accounting(GAPS_HEAD.format(count=2)), [])
check_contains(
    "a count that has fallen behind the tables is reported",
    gap_accounting(GAPS_HEAD.format(count=1)),
    "2 table row(s) are marked **GAP** but the `Open gaps` section accounts for 1",
)
check_contains(
    "a section that no longer states a count is reported",
    gap_accounting("| one | x | silent | **GAP** |\n\n## Open gaps\n\nprose only.\n"),
    "no longer states how many rows it accounts for",
)
check_contains(
    "a ledger with no Open gaps section at all is reported",
    gap_accounting("| one | x | silent | **GAP** |\n"),
    "no `## Open gaps` section",
)

print("\nshipped tree")

# The parsers have to find something in the real files: a rewrite that broke
# either regex would leave both sides empty and agreeing.
lock_forks = pinned_forks(LOCK.read_text(encoding="utf-8"))
ledger_rows = ledger_pins(LEDGER.read_text(encoding="utf-8"))
check("the lockfile yields forks", len(lock_forks) > 20, True)
check("the ledger yields rows", len(ledger_rows) > 20, True)
check("the shipped tree has no drift", drift(lock_forks, ledger_rows), [])
ledger_text = LEDGER.read_text(encoding="utf-8")
check("the shipped tree marks at least one gap", "**GAP**" in ledger_text, True)
check("the shipped Open gaps list accounts for every gap", gap_accounting(ledger_text), [])

if failures:
    print(f"\n{failures} of {checks} checks FAILED")
    raise SystemExit(1)
print(f"\nall {checks} checks passed")
