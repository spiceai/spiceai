#!/usr/bin/env python3
# Copyright 2024-2026 The Spice.ai OSS Authors
#
# Tests for scripts/add_test_reference_datasets.py.
#
# Run: python3 scripts/test_add_test_reference_datasets.py

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from add_test_reference_datasets import (  # noqa: E402
    REFERENCE_SCHEMA,
    add_test_reference_datasets,
)

failures = 0
checks = 0


def check(cond: bool, msg: str) -> None:
    global checks, failures
    checks += 1
    if not cond:
        failures += 1
        print(f"FAIL: {msg}")


def test_clones_unqualified_and_strips_acceleration() -> None:
    spicepod = {
        "version": "v1",
        "kind": "Spicepod",
        "name": "file[parquet]-cayenne[file]",
        "datasets": [
            {
                "from": "file:data/store_sales.parquet",
                "name": "store_sales",
                "acceleration": {"engine": "cayenne", "mode": "file", "enabled": True},
            },
            {
                "from": "file:data/item.parquet",
                "name": "item",
                "acceleration": {"engine": "cayenne", "mode": "file", "enabled": True},
            },
            {
                "from": "file:data/store_sales.parquet",
                "name": "already.store_sales",
            },
        ],
    }
    added = add_test_reference_datasets(spicepod)
    check(added == 2, f"expected 2 clones, got {added}")
    names = [ds["name"] for ds in spicepod["datasets"]]
    check(
        f"{REFERENCE_SCHEMA}.store_sales" in names,
        "missing __test_reference.store_sales",
    )
    check(f"{REFERENCE_SCHEMA}.item" in names, "missing __test_reference.item")
    check(
        f"{REFERENCE_SCHEMA}.already.store_sales" not in names,
        "must not clone a schema-qualified dataset",
    )
    clone = next(
        ds
        for ds in spicepod["datasets"]
        if ds["name"] == f"{REFERENCE_SCHEMA}.store_sales"
    )
    check("acceleration" not in clone, "clone must strip acceleration")
    check(
        clone.get("check_availability") == "disabled",
        "clone must disable availability probes",
    )
    check(
        clone.get("from") == "file:data/store_sales.parquet",
        "clone must keep the original from:",
    )


def test_idempotent() -> None:
    spicepod = {
        "datasets": [
            {"from": "file:data/item.parquet", "name": "item"},
        ]
    }
    first = add_test_reference_datasets(spicepod)
    second = add_test_reference_datasets(spicepod)
    check(first == 1, f"first pass should add 1, got {first}")
    check(second == 0, f"second pass should add 0, got {second}")
    names = [ds["name"] for ds in spicepod["datasets"]]
    check(names.count(f"{REFERENCE_SCHEMA}.item") == 1, "must not duplicate clones")


def main() -> int:
    test_clones_unqualified_and_strips_acceleration()
    test_idempotent()
    if failures:
        print(f"{failures}/{checks} checks failed")
        return 1
    print(f"{checks} checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
