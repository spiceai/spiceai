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
"""Pin every GitHub Actions `uses:` to a full-length commit SHA.

Workflows and in-repo actions reference a remote action only by its 40-character
commit SHA, with the release tag in a trailing comment:

    uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1

`gh actions-lock` writes release tags (`actions/checkout@v7.0.1`) into the
`uses:` lines it pins. Run this script after it: each tag is replaced with the
commit .github/workflows/actions.lock locked for that tag, and the tag moves to
the comment. The lockfile matches a SHA pin by its commit, so it still verifies.

    gh actions-lock                          # update .github/workflows/actions.lock
    python3 scripts/pin_actions_to_sha.py    # put the SHAs back in the uses: lines

With --check, nothing is written, and the script fails on any `uses:` that is
not pinned to a full-length SHA. CI runs it in verify-lockfile.yml.

Usage: scripts/pin_actions_to_sha.py [--check] [REPO_ROOT]
"""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path

LOCKFILE = Path(".github") / "workflows" / "actions.lock"
WORKFLOWS_DIR = Path(".github") / "workflows"
ACTIONS_DIR = Path(".github") / "actions"

FULL_SHA = re.compile(r"[0-9a-f]{40}")
# A step's or a job's `uses:`, optionally quoted, with an optional trailing comment.
USES = re.compile(
    r"""^(?P<lead>\s*(?:-\s+)?uses:\s*)(?P<q>['"]?)(?P<action>[^\s'"@#]+)@(?P<ref>[^\s'"#]+)(?P=q)"""
    r"""(?P<comment>\s*(?:#.*)?)$"""
)
# Local actions run at the workflow's own commit and container images are not
# actions, so neither takes a commit SHA.
NOT_REMOTE = ("./", "$/", "docker://")
LOCK_KEY = re.compile(r"""^    '?(?P<key>[^'\s]+@[^'\s]+)'?:\s*$""")
LOCK_COMMIT = re.compile(r"""^        commit:\s*'?sha1-(?P<sha>[0-9a-f]{40})'?\s*$""")


@dataclass(frozen=True)
class Uses:
    path: Path
    line: int
    action: str
    ref: str

    @property
    def repo(self) -> str:
        """`owner/repo` of `owner/repo[/path]`, lowercased as the lockfile keys it."""
        return "/".join(self.action.split("/")[:2]).lower()

    def location(self, root: Path) -> str:
        return f"{self.path.relative_to(root)}:{self.line}"


def lockfile_commits(root: Path) -> dict[tuple[str, str], str]:
    """Map each locked `(owner/repo, ref)` to the commit the lockfile pins it to."""
    commits: dict[tuple[str, str], str] = {}
    in_dependencies = False
    key: tuple[str, str] | None = None
    for text in (root / LOCKFILE).read_text().splitlines():
        if not text.startswith(" "):
            in_dependencies = text.rstrip() == "dependencies:"
            key = None
            continue
        if not in_dependencies:
            continue
        if match := LOCK_KEY.match(text):
            repo, ref = match["key"].rsplit("@", 1)
            key = (repo.lower(), ref)
        elif (match := LOCK_COMMIT.match(text)) and key is not None:
            commits[key] = match["sha"]
    return commits


def action_files(root: Path) -> list[Path]:
    workflows = [p for p in (root / WORKFLOWS_DIR).glob("*") if p.suffix in (".yml", ".yaml")]
    actions = [p for p in (root / ACTIONS_DIR).rglob("action.y*ml") if p.suffix in (".yml", ".yaml")]
    return sorted(p for p in workflows + actions if p.is_file())


def unpinned(path: Path, lines: list[str]) -> list[tuple[int, re.Match[str], Uses]]:
    """Each remote `uses:` in `lines` whose ref is not a full-length commit SHA."""
    found = []
    for index, text in enumerate(lines):
        match = USES.match(text)
        if not match or match["action"].startswith(NOT_REMOTE) or FULL_SHA.fullmatch(match["ref"]):
            continue
        found.append((index, match, Uses(path, index + 1, match["action"], match["ref"])))
    return found


def pinned_line(match: re.Match[str], sha: str) -> str:
    """The `uses:` line pinned to `sha`, with its tag first in the trailing comment."""
    note = match["comment"].strip().removeprefix("#").strip()
    comment = f" # {match['ref']}" + (f" {note}" if note else "")
    return f"{match['lead']}{match['q']}{match['action']}@{sha}{match['q']}{comment}"


def check(root: Path) -> int:
    refs = [uses for path in action_files(root) for _, _, uses in unpinned(path, path.read_text().split("\n"))]
    for uses in refs:
        print(f"{uses.location(root)}: uses: {uses.action}@{uses.ref} is not pinned to a full-length commit SHA")
    if refs:
        print(
            f"{len(refs)} `uses:` refs are not pinned to a full-length commit SHA. Pin them with "
            "`gh actions-lock` followed by `python3 scripts/pin_actions_to_sha.py`.",
            file=sys.stderr,
        )
        return 1
    print("Every remote `uses:` is pinned to a full-length commit SHA.")
    return 0


def pin(root: Path) -> int:
    commits = lockfile_commits(root)
    rewritten: dict[Path, list[str]] = {}
    missing: list[Uses] = []
    count = 0
    for path in action_files(root):
        lines = path.read_text().split("\n")
        refs = unpinned(path, lines)
        for index, match, uses in refs:
            sha = commits.get((uses.repo, uses.ref))
            if sha is None:
                missing.append(uses)
                continue
            lines[index] = pinned_line(match, sha)
            count += 1
        if refs:
            rewritten[path] = lines
    if missing:
        for uses in missing:
            print(f"{uses.location(root)}: {LOCKFILE} has no entry for {uses.repo}@{uses.ref}", file=sys.stderr)
        print(
            f"{len(missing)} `uses:` refs have no lockfile entry, so nothing was rewritten. "
            "Run `gh actions-lock` first, then run this script again.",
            file=sys.stderr,
        )
        return 1
    for path, lines in rewritten.items():
        path.write_text("\n".join(lines))
    if count:
        print(f"Pinned {count} `uses:` refs in {len(rewritten)} files to the commits {LOCKFILE} locks.")
    else:
        print("Every remote `uses:` is already pinned to a full-length commit SHA.")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    parser.add_argument("--check", action="store_true", help="write nothing; fail on any ref that is not a full-length SHA")
    parser.add_argument("root", nargs="?", type=Path, default=Path(__file__).resolve().parent.parent)
    args = parser.parse_args(argv)
    return check(args.root) if args.check else pin(args.root)


if __name__ == "__main__":
    sys.exit(main())
