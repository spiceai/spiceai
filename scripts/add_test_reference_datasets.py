#!/usr/bin/env python3
# Copyright 2024-2026 The Spice.ai OSS Authors
"""Clone unqualified spicepod datasets under ``__test_reference.*`` without acceleration.

Used so ``testoperator run query --validate`` against an already-running
``spiced`` has a live oracle: the same ``from:`` as each under-test table,
unaccelerated, registered as ``__test_reference.<table>``. testoperator injects
the same clones itself when it starts ``spiced`` (``run bench --validate``).

Does not rewrite a spicepod that already has a complete ``__test_reference``
set. Inserts the clones at the end of the ``datasets:`` list.

Usage:
    scripts/add_test_reference_datasets.py path/to/spicepod.yaml
    scripts/add_test_reference_datasets.py in.yaml -o out.yaml
"""

from __future__ import annotations

import argparse
import copy
import sys
from pathlib import Path
from typing import Any

REFERENCE_SCHEMA = "__test_reference"

# Dataset fields that add startup/runtime overhead or failure modes unrelated
# to a known-good source scan. Mirrors add_automatic_reference_datasets in
# tools/testoperator/src/commands/mod.rs.
STRIP_FIELDS = (
    "acceleration",
    "depends_on",
    "embeddings",
    "vectors",
    "full_text_search",
    "replication",
    "metrics",
)


def _load_yaml(path: Path) -> Any:
    try:
        import yaml
    except ImportError as exc:
        raise SystemExit(
            "PyYAML is required: python3 -m pip install pyyaml"
        ) from exc
    with path.open(encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def _dump_yaml(data: Any, path: Path) -> None:
    import yaml

    with path.open("w", encoding="utf-8") as handle:
        yaml.safe_dump(
            data,
            handle,
            sort_keys=False,
            default_flow_style=False,
            allow_unicode=True,
        )


def add_test_reference_datasets(spicepod: dict[str, Any]) -> int:
    """Mutate ``spicepod`` in place. Returns the number of clones added."""
    datasets = spicepod.get("datasets")
    if not isinstance(datasets, list):
        raise ValueError("spicepod has no datasets: list")

    existing = {
        dataset.get("name")
        for dataset in datasets
        if isinstance(dataset, dict) and isinstance(dataset.get("name"), str)
    }

    added = 0
    clones: list[dict[str, Any]] = []
    for dataset in datasets:
        if not isinstance(dataset, dict):
            continue
        name = dataset.get("name")
        if not isinstance(name, str) or "." in name:
            continue
        reference_name = f"{REFERENCE_SCHEMA}.{name}"
        if reference_name in existing:
            continue
        clone = copy.deepcopy(dataset)
        clone["name"] = reference_name
        for field in STRIP_FIELDS:
            clone.pop(field, None)
        clone["check_availability"] = "disabled"
        clones.append(clone)
        existing.add(reference_name)
        added += 1

    datasets.extend(clones)
    return added


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Clone unqualified datasets under __test_reference.* without "
            "acceleration, for testoperator --validate against an external spiced."
        )
    )
    parser.add_argument("spicepod", type=Path, help="Input spicepod.yaml")
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=None,
        help="Write here instead of overwriting the input",
    )
    args = parser.parse_args(argv)

    spicepod = _load_yaml(args.spicepod)
    if not isinstance(spicepod, dict):
        print(f"{args.spicepod}: not a YAML mapping", file=sys.stderr)
        return 1

    added = add_test_reference_datasets(spicepod)
    output = args.output or args.spicepod
    _dump_yaml(spicepod, output)
    print(
        f"Added {added} unaccelerated {REFERENCE_SCHEMA}.* datasets to {output}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
