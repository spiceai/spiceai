#!/usr/bin/env python3
"""Lay out a local Rust distribution mirror of one pinned toolchain for rustup.

rustup verifies every component it downloads against the hash in the channel manifest,
and .github/ci-cache/pins.json pins the manifest's own SHA-256. So the manifest, then
each component it names, is fetched with cached_fetch.sh (the CI cache first, the source
after) and checked against those hashes, and the files are laid out as
static.rust-lang.org lays them out. Point rustup at the result with
`RUSTUP_DIST_SERVER=file://<out>`; it then installs without the network.

Usage:
  rust_dist_mirror.py --toolchain 1.99.1 --host x86_64-unknown-linux-gnu --out DIR
  rust_dist_mirror.py --toolchain 1.99.1 --host HOST --list
      prints "<sha256> <url>" for the manifest and each component, without downloading
      the components (the manifest itself is fetched).

Exit status 3 means the toolchain has no pin, so the caller should install from the
source as before.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import tempfile
from pathlib import Path

DIST_ROOT = "https://static.rust-lang.org/"
HERE = Path(__file__).resolve().parent
PINS = HERE.parent / "ci-cache" / "pins.json"
FETCH = HERE / "cached_fetch.sh"


def fetch(sha256: str, dest: Path, url: str) -> None:
    # cached_fetch.sh reports progress on stdout; keep this script's stdout for its result.
    subprocess.run(["bash", str(FETCH), sha256, str(dest), url], check=True, stdout=sys.stderr)


def manifest_tables(text: str) -> tuple[dict, dict]:
    """The `[pkg.<name>.target.<triple>]` tables and the `[renames.*]` map of a manifest.

    A channel manifest is flat TOML: `[section]` headers followed by `key = value` lines.
    This reads only that shape, so it runs on a Python without `tomllib`.
    """
    tables: dict[str, dict[str, str]] = {}
    section = None
    for line in text.splitlines():
        header = re.fullmatch(r"\[([^\]]+)\]", line.strip())
        if header:
            section = header.group(1)
            tables.setdefault(section, {})
            continue
        pair = re.fullmatch(r'([A-Za-z_][A-Za-z0-9_-]*) = "?([^"]*)"?', line.strip())
        if pair and section is not None:
            tables[section][pair.group(1)] = pair.group(2)
    renames = {
        section[len("renames.") :]: values["to"]
        for section, values in tables.items()
        if section.startswith("renames.") and "to" in values
    }
    return tables, renames


def components(manifest: Path, host: str, names: list[str]) -> list[tuple[str, str]]:
    """(sha256, url) of each named component for `host`, as the manifest pins them."""
    tables, renames = manifest_tables(manifest.read_text())
    found = []
    for name in names:
        package = renames.get(name, name)
        table = tables.get(f"pkg.{package}.target.{host}")
        if not table or table.get("available") != "true":
            sys.exit(f"{manifest.name} has no {name} ({package}) for {host}.")
        if "xz_url" in table and "xz_hash" in table:
            found.append((table["xz_hash"], table["xz_url"]))
        else:
            found.append((table["hash"], table["url"]))
    for sha256, url in found:
        if not url.startswith(DIST_ROOT) or not re.fullmatch(r"[0-9a-f]{64}", sha256):
            sys.exit(f"{manifest.name} names a component outside {DIST_ROOT}, or without a SHA-256: {url}")
    return found


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--toolchain", required=True)
    parser.add_argument("--host", required=True)
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--out", type=Path)
    target.add_argument("--list", action="store_true")
    args = parser.parse_args()

    pins = json.loads(PINS.read_text())["rust"]
    pin = pins["toolchains"].get(args.toolchain)
    if pin is None:
        print(
            f"Rust {args.toolchain} has no manifest pin in {PINS.relative_to(HERE.parent.parent)}; "
            "installing it from static.rust-lang.org.",
            file=sys.stderr,
        )
        return 3

    manifest_name = f"channel-rust-{args.toolchain}.toml"
    manifest_url = f"{DIST_ROOT}dist/{manifest_name}"
    with tempfile.TemporaryDirectory() as scratch:
        root = args.out if args.out else Path(scratch)
        manifest = root / "dist" / manifest_name
        fetch(pin["manifest_sha256"], manifest, manifest_url)
        wanted = components(manifest, args.host, pins["components"])

        if args.list:
            print(pin["manifest_sha256"], manifest_url)
            for sha256, url in wanted:
                print(sha256, url)
            return 0

        # rustup reads the manifest's checksum file before the manifest.
        (root / "dist" / f"{manifest_name}.sha256").write_text(f"{pin['manifest_sha256']}  {manifest_name}\n")
        for sha256, url in wanted:
            fetch(sha256, root / url[len(DIST_ROOT) :], url)
    print(f"file://{args.out.resolve()}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
