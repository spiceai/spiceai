#!/usr/bin/env python3
"""Guard: a provider-wrapping `TableProvider` silently terminates every layer walk.

A dataset's provider is a stack of `spice_table::TableLayer`s composed by one
`SpiceTable`. Walks over that stack (index discovery, CDC stream detection,
source peeling for change streams, the CDC write path, retention deletes) step
from layer to layer by asking each one where the walk goes.

A type that wraps an `Arc<dyn TableProvider>` and implements `TableProvider`
itself is invisible to that machinery: a walk reaching it cannot tell it apart
from a connector's own provider, so the walk simply stops. Nothing errors — index
discovery just reports no indexes, a change stream attaches to the wrong table,
or a source bootstrap `SELECT` references a column the source does not have.

This is the failure mode the layer table used to have (a missing entry stopped a
walk with no diagnostic) and the reason it was replaced. Rather than reintroduce
it a different way, every provider-wrapping provider must be either:

  * a `TableLayer`, so it answers for itself where each walk goes; or
  * listed below, with a reason it is not part of a dataset's layer stack.

Deliberately a source scan rather than a compile-time check: the point is to
catch a *new* wrapper at review time, and no trait bound can express "do not wrap
a provider" without naming every wrapper — the dependency the layer model exists
to remove.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

CRATES = Path(__file__).resolve().parent.parent / "crates"

# Types that wrap a provider and implement `TableProvider`, each with the reason
# it is not a layer in a dataset's stack. Add an entry only when the wrapper
# genuinely sits outside that stack — if a walk should ever see through it, make
# it a `TableLayer` instead.
ALLOWED: dict[str, str] = {
    "SpiceTable": "the stack itself: composes the layers every walk steps through",
    # Opaque by design — a walk must not route around what these do.
    "UpsertDedupTableProvider": "rewrites writes (dedup/last-write-wins); routing past it drops those semantics",
    "EnsureSchema": "casts the accelerator's schema; carries no walkable semantics",
    "SwappableTableProvider": "swaps the accelerator underneath; not a dataset layer",
    "DuckDBUniqueIndexGuardTableProvider": "guards DuckDB unique-index DDL; not a dataset layer",
    # Write-side adaptors, reached through a connector rather than a dataset stack.
    "FlightTableWriter": "connector write adaptor",
    "SnowflakeTableProvider": "connector write adaptor",
    "DuckDbFederatedTableWriter": "connector write adaptor",
    # Table-valued functions: their own plan, not a dataset's stack.
    "RerankUDTFProvider": "table-valued function provider",
    "VectorSearchUDTFProvider": "table-valued function provider",
    "SearchQueryProvider": "table-valued function provider",
    # Connector- and cluster-specific providers.
    "CommitsTableProvider": "github connector provider",
    "FederatedTaskHistoryTable": "fans task history across cluster peers; not a dataset layer",
    # Test doubles.
    "CountingInsertProvider": "test double",
    "CountingDeleteProvider": "test double",
    "FailFirstWriteProvider": "test double",
    "SlowProvider": "test double",
    "WriteOrderRecordingProvider": "test double",
    "DelayedNativeTableProvider": "test double",
    "CountingAccelerator": "test double",
}

STRUCT = re.compile(r"(?:pub(?:\([^)]*\))? )?struct (\w+)\s*\{([^}]*)\}")
PROVIDER_FIELD = re.compile(r"Arc<dyn (?:datafusion::datasource::)?TableProvider>")
PROVIDER_IMPL = re.compile(
    r"impl(?:<[^>]*>)? (?:datafusion::datasource::)?TableProvider for (\w+)"
)


def main() -> int:
    wrapping: dict[str, Path] = {}
    provider_impls: set[str] = set()

    for path in CRATES.rglob("*.rs"):
        if "/vendor/" in path.as_posix():
            continue
        source = path.read_text(errors="ignore")
        for match in STRUCT.finditer(source):
            if PROVIDER_FIELD.search(match.group(2)):
                wrapping.setdefault(match.group(1), path)
        provider_impls.update(PROVIDER_IMPL.findall(source))

    offenders = sorted(set(wrapping) & provider_impls - set(ALLOWED))
    if offenders:
        print(
            "error: these types wrap a TableProvider and implement TableProvider, so every\n"
            "layer walk stops at them without any diagnostic — index discovery reports no\n"
            "indexes, a change stream attaches to the wrong table, a source query sees\n"
            "columns the source does not have.\n",
            file=sys.stderr,
        )
        for name in offenders:
            print(f"  {name}  ({wrapping[name].relative_to(CRATES.parent)})", file=sys.stderr)
        print(
            "\nMake each one a `spice_table::TableLayer` so it answers for itself where each\n"
            "walk goes, or add it to ALLOWED in this script with the reason it is not part\n"
            "of a dataset's layer stack.",
            file=sys.stderr,
        )
        return 1

    stale = sorted(set(ALLOWED) - set(wrapping))
    if stale:
        print(
            "error: ALLOWED lists types that no longer wrap a TableProvider. Remove them so\n"
            "the list keeps meaning what it says:",
            file=sys.stderr,
        )
        for name in stale:
            print(f"  {name}", file=sys.stderr)
        return 1

    print(
        f"table layers OK: {len(wrapping)} provider-wrapping types, "
        f"{len(ALLOWED)} justified non-layers, no silent walk terminators."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
