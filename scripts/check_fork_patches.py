#!/usr/bin/env python3
# Copyright 2024-2026 The Spice.ai OSS Authors
#
# Fork-pin drift guard.
#
# Spice carries patches on forks of upstream crates. Those patches exist only as
# commits on a fork branch, and every fork branch is re-cut when its upstream
# releases a new major. A patch that is not deliberately carried across the
# re-cut is lost silently: nothing fails, the crate reverts to upstream
# behaviour, and the bug it fixed comes back in the next Spice release. It has
# happened — twice in the Vortex fork alone (spiceai/spiceai#13524, and the
# reentrant-waker use-after-free that shipped as a SIGSEGV).
#
# `docs/dev/fork_patches.md` is the ledger: every fork, the revision this
# workspace pins, and for every patch the repo-side test that fails if the patch
# goes missing. This guard keeps the ledger honest by pinning it to `Cargo.lock`:
# move a pin without re-auditing the ledger and the build fails here, which is
# the moment the audit is cheap and the loss is still recoverable.
#
# `Cargo.lock` is the authority rather than `Cargo.toml` because it resolves tags
# and branch specs to the commit cargo actually builds.
#
# Usage:
#   scripts/check_fork_patches.py          # validate (exit 1 on drift)
#   scripts/check_fork_patches.py --list   # print every fork and its status
#
# Pure stdlib; no third-party deps.

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
LEDGER = REPO / "docs" / "dev" / "fork_patches.md"
LOCK = REPO / "Cargo.lock"

# A `Cargo.lock` source line for a crate that comes from a fork in the spiceai
# org, e.g.
#   source = "git+https://github.com/spiceai/vortex.git?rev=ba043de0…#ba043de0…"
# The fragment after `#` is the resolved commit, which is what a pin means no
# matter whether the manifest asked for a rev, a tag or a branch.
LOCK_SOURCE_RE = re.compile(
    r'^source = "git\+https://github\.com/spiceai/(?P<repo>[^/?#"]+?)(?:\.git)?\?[^#"]*#(?P<rev>[0-9a-f]{40})"$',
    re.M,
)

# A ledger pin row:
#   | [vortex](#vortex) | `ba043de0ab6e214e825932210cc336b7ce5e8309` | `spiceai-54` | … |
# The repo name is read from the link text so the row stays a working anchor.
LEDGER_ROW_RE = re.compile(
    r"^\|\s*\[(?P<repo>[A-Za-z0-9._-]+)\]\([^)]*\)\s*\|\s*`(?P<rev>[0-9a-f]{40})`\s*\|",
    re.M,
)

# Repositories in the spiceai org that are not forks: they have no upstream, so
# nothing can drop a patch from them and there is nothing to audit. Everything
# else needs a ledger row, including forks that carry no patch today — "no
# patches" is a finding to re-confirm at the next bump, not a reason to leave a
# fork undocumented.
NOT_FORKS = frozenset({"spice-rs", "spicebench"})


def pinned_forks(lock_text: str) -> dict[str, set[str]]:
    """Every spiceai fork in the lockfile, mapped to the revisions pinned for it.

    A repo maps to more than one revision only if the workspace pins two
    different commits of it at once, which is a mistake in its own right; the
    caller reports it rather than picking one.
    """
    forks: dict[str, set[str]] = {}
    for match in LOCK_SOURCE_RE.finditer(lock_text):
        repo = match.group("repo")
        if repo in NOT_FORKS:
            continue
        forks.setdefault(repo, set()).add(match.group("rev"))
    return forks


def ledger_pins(ledger_text: str) -> dict[str, list[str]]:
    """Every fork the ledger records, mapped to the revisions its rows claim."""
    pins: dict[str, list[str]] = {}
    for match in LEDGER_ROW_RE.finditer(ledger_text):
        pins.setdefault(match.group("repo"), []).append(match.group("rev"))
    return pins


def drift(pinned: dict[str, set[str]], recorded: dict[str, list[str]]) -> list[str]:
    """Every disagreement between what the workspace builds and what the ledger says."""
    errors = []
    for repo in sorted(pinned):
        revs = pinned[repo]
        if len(revs) > 1:
            joined = ", ".join(sorted(rev[:12] for rev in revs))
            errors.append(
                f"{repo}: the workspace pins {len(revs)} different revisions at once ({joined}); "
                f"resolve them to one before recording it"
            )
            continue
        rev = next(iter(revs))
        if repo not in recorded:
            errors.append(
                f"{repo}: pinned at {rev[:12]} but has no row in docs/dev/fork_patches.md. "
                f"Add one recording what Spice patches this fork carries and which test in this "
                f"repo fails if each is lost"
            )
            continue
        rows = recorded[repo]
        if len(rows) > 1:
            errors.append(f"{repo}: has {len(rows)} rows in docs/dev/fork_patches.md; keep one per fork")
            continue
        if rows[0] != rev:
            errors.append(
                f"{repo}: pinned at {rev} but docs/dev/fork_patches.md records {rows[0]}. "
                f"The pin moved: re-audit the fork's patches against the new revision, confirm each "
                f"one is still present and still guarded, then update the row"
            )
    for repo in sorted(recorded):
        if repo not in pinned:
            errors.append(
                f"{repo}: recorded in docs/dev/fork_patches.md but no longer pinned by Cargo.lock. "
                f"Drop the row, or the pin"
            )
    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--list", action="store_true", help="print every fork and its recorded revision")
    args = parser.parse_args()

    for path in (LOCK, LEDGER):
        if not path.is_file():
            print(f"error: {path.relative_to(REPO)} not found", file=sys.stderr)
            return 1

    pinned = pinned_forks(LOCK.read_text(encoding="utf-8"))
    recorded = ledger_pins(LEDGER.read_text(encoding="utf-8"))

    if args.list:
        for repo in sorted(set(pinned) | set(recorded)):
            lock_rev = ", ".join(sorted(pinned.get(repo, set()))) or "-"
            doc_rev = ", ".join(recorded.get(repo, [])) or "-"
            status = "ok" if lock_rev == doc_rev else "DRIFT"
            print(f"{status:6} {repo:28} lock={lock_rev[:12]:14} ledger={doc_rev[:12]}")

    errors = drift(pinned, recorded)
    if errors:
        print(
            f"\n{len(errors)} fork pin(s) disagree with docs/dev/fork_patches.md:\n",
            file=sys.stderr,
        )
        for error in errors:
            print(f"  - {error}", file=sys.stderr)
        print(
            "\nThe ledger is what tells us a fork lost a Spice patch. It is only true of the "
            "revision it names, so it has to move with the pin.",
            file=sys.stderr,
        )
        return 1

    if not args.list:
        print(f"fork-patch ledger: {len(pinned)} pinned forks, all recorded")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
