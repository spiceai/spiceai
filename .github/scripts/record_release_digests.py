#!/usr/bin/env python3
"""Record a published release's spiced archive digests in .github/release-digests.json.

A CI run on trunk, or with the full CI roles, runs a release's spiced only when this file, which
only a reviewed trunk change edits, records the archive's digest. publish_release.yml runs this
after publishing, with the release's attested SHA256SUMS, and opens that change.

Usage: record_release_digests.py --tag vX.Y.Z --sums SHA256SUMS [--digests .github/release-digests.json]
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

# The spiced archives a base release or a benchmark's release spiced is run from.
ASSETS = ("spiced_darwin_aarch64.tar.gz", "spiced_linux_aarch64.tar.gz", "spiced_linux_x86_64.tar.gz")
TAG = re.compile(r"^v[0-9]+\.[0-9]+\.[0-9]+$")
SHA256 = re.compile(r"^[0-9a-f]{64}$")


class Error(Exception):
    pass


def listed(sums: str) -> dict[str, str]:
    """Each file SHA256SUMS lists, by name (`<digest>  <name>` or `<digest> *<name>`)."""
    found = {}
    for line in sums.splitlines():
        parts = line.split(None, 1)
        if len(parts) != 2:
            continue
        digest, name = parts[0], parts[1].strip().removeprefix("*")
        if SHA256.match(digest):
            found[name] = digest
    return found


def record(document: dict, tag: str, sums: str) -> dict:
    """The document with `tag`'s spiced archives recorded from SHA256SUMS."""
    if not TAG.match(tag):
        raise Error(f"{tag} is not a release tag vX.Y.Z; a release candidate is not recorded")
    files = listed(sums)
    missing = [asset for asset in ASSETS if asset not in files]
    if missing:
        raise Error(f"the SHA256SUMS of {tag} does not list {', '.join(missing)}")
    entry = {asset: files[asset] for asset in ASSETS}
    releases = dict(document.get("releases", {}))
    if tag in releases and releases[tag] != entry:
        raise Error(f"{tag} is already recorded with other digests; a published release does not change")
    releases[tag] = entry
    return {**document, "releases": dict(sorted(releases.items(), key=lambda kv: version_key(kv[0])))}


def version_key(tag: str) -> tuple[int, ...]:
    return tuple(int(part) for part in tag.removeprefix("v").split("."))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    parser.add_argument("--tag", required=True)
    parser.add_argument("--sums", type=Path, required=True)
    parser.add_argument("--digests", type=Path, default=Path(".github/release-digests.json"))
    args = parser.parse_args(argv)
    try:
        document = json.loads(args.digests.read_text())
        updated = record(document, args.tag, args.sums.read_text())
    except (Error, OSError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    if updated == document:
        print(f"{args.tag} is already recorded.")
        return 0
    args.digests.write_text(json.dumps(updated, indent=2) + "\n")
    print(f"Recorded {args.tag} in {args.digests}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
