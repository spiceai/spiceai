#!/usr/bin/env python3
# Copyright 2024-2026 The Spice.ai OSS Authors
#
# Workspace crate-layering guard.
#
# Reads `layers.toml` (tier order + per-crate assignment) and the workspace
# dependency graph from `cargo metadata`, then verifies that every *normal*
# (non-dev, non-build) dependency edge points to a crate in the same tier or a
# lower one. An edge that points "up" the stack is a layering regression and
# fails the check.
#
# This codifies the layering that ALREADY holds today so it cannot regress; it
# does not attempt to move the tree toward the target architecture (see
# docs/dev/crate_layering.md for that roadmap).
#
# Usage:
#   scripts/check_crate_layers.py            # validate (exit 1 on violation)
#   scripts/check_crate_layers.py --list     # print the tier of every crate
#   scripts/check_crate_layers.py --mermaid  # emit a tier DAG as mermaid
#
# Pure stdlib; no third-party deps. Python 3.11+ for tomllib.

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

try:
    import tomllib  # Python 3.11+
except ModuleNotFoundError:
    sys.exit(
        "error: this script needs Python 3.11+ for the stdlib `tomllib` module "
        f"(found {sys.version_info.major}.{sys.version_info.minor}). "
        "Install/select a newer python3 and re-run `make lint-rust`."
    )

REPO = Path(__file__).resolve().parent.parent


def load_layers() -> dict:
    with (REPO / "layers.toml").open("rb") as f:
        return tomllib.load(f)


def load_metadata() -> dict:
    out = subprocess.run(
        # --locked: this is a fast, side-effect-free lint guard; fail rather
        # than let cargo mutate Cargo.lock, keeping CI/dev runs deterministic.
        ["cargo", "metadata", "--format-version", "1", "--no-deps", "--locked"],
        cwd=REPO,
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(out.stdout)


def rel(manifest_path: str) -> str:
    # Workspace-relative crate dir, always forward-slashed so the layers.toml
    # `path_prefix` rules match on every platform (Windows included) rather than
    # relying on string-substituting a hard-coded separator.
    crate_dir = Path(manifest_path).parent
    try:
        return crate_dir.relative_to(REPO).as_posix()
    except ValueError:
        # Not under the repo root — shouldn't happen for `--no-deps` workspace
        # members, but degrade to the full posix path rather than misassign.
        return crate_dir.as_posix()


def assign_tier(name: str, path: str, cfg: dict) -> str:
    overrides = cfg.get("override", {})
    if name in overrides:
        return overrides[name]
    for rule in cfg.get("rules", []):
        prefix = rule.get("path_prefix")
        if prefix and path.startswith(prefix):
            return rule["tier"]
    return cfg["default_tier"]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--list", action="store_true", help="print each crate's tier")
    ap.add_argument("--mermaid", action="store_true", help="emit tier DAG as mermaid")
    args = ap.parse_args()

    cfg = load_layers()
    order: list[str] = cfg["order"]
    rank = {tier: i for i, tier in enumerate(order)}

    meta = load_metadata()
    pkgs = meta["packages"]
    names = {p["name"] for p in pkgs}
    path_of = {p["name"]: rel(p["manifest_path"]) for p in pkgs}

    tier_of: dict[str, str] = {}
    for p in pkgs:
        tier_of[p["name"]] = assign_tier(p["name"], path_of[p["name"]], cfg)

    unknown = {t for t in tier_of.values() if t not in rank}
    if unknown:
        print(f"error: crates assigned to unknown tier(s): {sorted(unknown)}", file=sys.stderr)
        return 2

    if args.list:
        for name in sorted(names, key=lambda n: (rank[tier_of[n]], n)):
            print(f"{rank[tier_of[name]]}  {tier_of[name]:<12} {name}  [{path_of[name]}]")
        return 0

    if args.mermaid:
        print("graph TD")
        seen = set()
        for p in pkgs:
            src = p["name"]
            for d in p["dependencies"]:
                if (d.get("kind") or "normal") != "normal":
                    continue  # only normal edges layer the shipped graph (skip dev + build)
                dep = d["name"]
                if dep not in names or dep == src:
                    continue
                key = (tier_of[src], tier_of[dep])
                if key[0] != key[1] and key not in seen:
                    seen.add(key)
                    print(f"  {key[0]} --> {key[1]}")
        return 0

    violations = []
    for p in pkgs:
        src = p["name"]
        for d in p["dependencies"]:
            if (d.get("kind") or "normal") != "normal":
                continue  # only normal edges layer the shipped graph; dev + build deps
                # may point anywhere (e.g. connector integration tests -> runtime)
            dep = d["name"]
            if dep not in names or dep == src:
                continue
            if rank[tier_of[dep]] > rank[tier_of[src]]:
                violations.append((src, tier_of[src], dep, tier_of[dep]))

    if violations:
        print("Crate-layering violations (a crate depends on a HIGHER tier):\n")
        for src, st, dep, dt in sorted(violations):
            print(f"  {src} [{st}] -> {dep} [{dt}]")
        print(
            f"\n{len(violations)} violation(s). Either fix the dependency direction, "
            "or (if this is an intentional, documented layering change) update layers.toml.\n"
            "See docs/dev/crate_layering.md."
        )
        return 1

    print(f"crate layering OK: {len(names)} crates, {len(order)} tiers, no upward edges.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
