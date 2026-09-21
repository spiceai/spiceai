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
# Pure stdlib; no third-party deps. The duckdb-rs Thrift-equality check also
# shells out to `cargo metadata` only when the git checkout is not already on
# disk, so a cold cache still resolves the tarball cargo would build.

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import tarfile
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
LEDGER = REPO / "docs" / "dev" / "fork_patches.md"
LOCK = REPO / "Cargo.lock"
MAKEFILE = REPO / "Makefile"

# A `Cargo.lock` source line for a crate that comes from a fork in the spiceai
# org, e.g.
#   source = "git+https://github.com/spiceai/vortex.git?rev=ba043de0…#ba043de0…"
# The fragment after `#` is the resolved commit, which is what a pin means no
# matter whether the manifest asked for a rev, a tag or a branch.
LOCK_SOURCE_RE = re.compile(
    r'^source = "git\+https://github\.com/spiceai/(?P<repo>[^/?#"]+?)(?:\.git)?\?[^#"]*#(?P<rev>[0-9a-f]{40})"$',
    re.M,
)

# The count of unguarded patches the "Open gaps" section claims to account for,
# e.g. "**36 rows above are marked GAP**". Pinned against the tables so the
# prioritised list cannot quietly fall behind them — a row that gets a **GAP**
# marker and no entry in that list is a patch the ledger hides.
GAP_CLAIM_RE = re.compile(r"^\*\*(?P<count>\d+) rows above are marked GAP\*\*", re.M)

# A table row whose Guard cell reports no repo-side coverage.
GAP_ROW_RE = re.compile(r"^\|.*\*\*GAP\*\*.*\|$", re.M)

# A ledger pin row:
#   | [vortex](#vortex) | `ba043de0ab6e214e825932210cc336b7ce5e8309` | `spiceai-54` | … |
# The repo name is read from the link text so the row stays a working anchor.
LEDGER_ROW_RE = re.compile(
    r"^\|\s*\[(?P<repo>[A-Za-z0-9._-]+)\]\([^)]*\)\s*\|\s*`(?P<rev>[0-9a-f]{40})`\s*\|",
    re.M,
)

# A ledger row whose branch cell declares the pin temporary, e.g.
#   | [vortex](#vortex) | `2f1a22ad…` | `in-list-hashed-probe` (TEMPORARY: spiceai/vortex#95) |
# Pinning a fork at a pull request's branch is the normal way to review a change
# that spans both repositories, and it must never land: the branch is deleted
# when that pull request merges, so trunk would name a revision no branch
# reaches and the next clone could not resolve it. The marker makes the pin
# reviewable and un-landable at once — this guard fails while it is present.
TEMPORARY_PIN_RE = re.compile(
    r"^\|\s*\[(?P<repo>[A-Za-z0-9._-]+)\]\([^)]*\)\s*\|\s*`[0-9a-f]{40}`\s*\|"
    r"[^|]*\(TEMPORARY:\s*(?P<blocker>[^)]+)\)",
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


def gap_accounting(ledger_text: str) -> list[str]:
    """Whether the "Open gaps" list still accounts for every **GAP** row."""
    body, _, gaps = ledger_text.partition("## Open gaps")
    if not gaps:
        return ["docs/dev/fork_patches.md has no `## Open gaps` section"]
    marked = len(GAP_ROW_RE.findall(body))
    claim = GAP_CLAIM_RE.search(gaps)
    if not claim:
        return [
            "docs/dev/fork_patches.md: the `Open gaps` section no longer states how many "
            "rows it accounts for, so nothing pins it to the tables"
        ]
    claimed = int(claim.group("count"))
    if claimed != marked:
        return [
            f"docs/dev/fork_patches.md: {marked} table row(s) are marked **GAP** but the "
            f"`Open gaps` section accounts for {claimed}. Add the missing patch(es) to that "
            f"list and update the count, or the ledger hides an unguarded patch"
        ]
    return []


def temporary_pins(ledger_text: str) -> list[str]:
    """Pins the ledger itself declares un-landable, one message each."""
    return [
        f"{match['repo']} is pinned to a branch rather than a landed revision: "
        f"{match['blocker'].strip()} has to merge first, then repoint the pin at the "
        f"merge commit and drop the TEMPORARY marker"
        for match in TEMPORARY_PIN_RE.finditer(ledger_text)
    ]


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


# Marker the macOS 27 / libc++ Thrift backport adds to the bundled header.
# Exact signature so a coincidental `operator==` elsewhere in the archive does
# not count, and so a re-cut that drops only this method fails the guard.
DUCKDB_THRIFT_EQUALITY_MARKER = "bool operator==(const TEnumIterator& end)"
DUCKDB_THRIFT_HEADER_SUFFIX = "third_party/thrift/thrift/Thrift.h"
DUCKDB_RS_REPO = "duckdb-rs"


def duckdb_rs_rev(pinned: dict[str, set[str]]) -> str | None:
    """The single duckdb-rs revision Cargo.lock pins, or None if unpinned/ambiguous."""
    revs = pinned.get(DUCKDB_RS_REPO)
    if revs is None or len(revs) != 1:
        return None
    return next(iter(revs))


def duckdb_tarball_from_checkouts(rev: str) -> Path | None:
    """Locate libduckdb-sys/duckdb.tar.gz in the local cargo git checkouts."""
    cargo_home = Path(os.environ.get("CARGO_HOME", Path.home() / ".cargo"))
    checkouts = cargo_home / "git" / "checkouts"
    if not checkouts.is_dir():
        return None
    for repo_dir in checkouts.glob("duckdb-rs-*"):
        if not repo_dir.is_dir():
            continue
        for short in repo_dir.iterdir():
            if not short.is_dir() or not rev.startswith(short.name):
                continue
            candidate = short / "crates" / "libduckdb-sys" / "duckdb.tar.gz"
            if candidate.is_file():
                return candidate
    return None


def duckdb_tarball_from_cargo_metadata(rev: str) -> Path | None:
    """Ask cargo where libduckdb-sys lives for the pinned revision (fetches if needed)."""
    for offline in (True, False):
        cmd = ["cargo", "metadata", "--format-version", "1", "--locked"]
        if offline:
            cmd.append("--offline")
        try:
            completed = subprocess.run(
                cmd,
                cwd=REPO,
                capture_output=True,
                text=True,
                timeout=300,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired):
            continue
        if completed.returncode != 0 or not completed.stdout.strip():
            continue
        try:
            metadata = json.loads(completed.stdout)
        except json.JSONDecodeError:
            continue
        for package in metadata.get("packages", []):
            if package.get("name") != "libduckdb-sys":
                continue
            source = package.get("source") or ""
            if DUCKDB_RS_REPO not in source or rev not in source:
                continue
            manifest = Path(package["manifest_path"])
            candidate = manifest.parent / "duckdb.tar.gz"
            if candidate.is_file():
                return candidate
    return None


def resolve_duckdb_tarball(rev: str) -> Path | None:
    """Prefer an existing checkout; fall back to cargo metadata to populate one."""
    return duckdb_tarball_from_checkouts(rev) or duckdb_tarball_from_cargo_metadata(rev)


def thrift_header_from_tarball(tarball: Path) -> str | None:
    """Return the bundled Thrift.h text from duckdb.tar.gz, or None if absent."""
    with tarfile.open(tarball, "r:gz") as archive:
        for member in archive.getmembers():
            if member.isfile() and member.name.endswith(DUCKDB_THRIFT_HEADER_SUFFIX):
                extracted = archive.extractfile(member)
                if extracted is None:
                    return None
                return extracted.read().decode("utf-8", errors="replace")
    return None


def duckdb_thrift_iterator_equality(pinned: dict[str, set[str]]) -> list[str]:
    """Repo-side guard: the pinned duckdb-rs tarball still carries Thrift operator==.

    The failure mode only surfaces as a compile error on the macOS 27 SDK, so a
    behaviour test in this workspace cannot catch a re-cut that drops the
    backport. Reading the marker out of the tarball cargo actually builds is what
    fails here instead. The fork's own C++ regression test does not protect us.
    """
    rev = duckdb_rs_rev(pinned)
    if rev is None:
        return []
    tarball = resolve_duckdb_tarball(rev)
    if tarball is None:
        return [
            f"{DUCKDB_RS_REPO}: could not locate crates/libduckdb-sys/duckdb.tar.gz for "
            f"pinned revision {rev[:12]}. Run `cargo metadata --locked` (or any build that "
            f"fetches git deps) so the checkout exists, then re-run this guard"
        ]
    header = thrift_header_from_tarball(tarball)
    if header is None:
        return [
            f"{DUCKDB_RS_REPO}: {tarball} has no {DUCKDB_THRIFT_HEADER_SUFFIX}; the bundled "
            f"Parquet/Thrift sources are missing"
        ]
    if DUCKDB_THRIFT_EQUALITY_MARKER not in header:
        return [
            f"{DUCKDB_RS_REPO}: pinned revision {rev[:12]} lost the Thrift "
            f"`TEnumIterator::operator==` backport (macOS 27 / libc++). Expected "
            f"`{DUCKDB_THRIFT_EQUALITY_MARKER}` in the bundled Thrift.h. Re-carry fork PR #47 "
            f"(or the upstream duckdb Thrift equality fix) before moving this pin"
        ]
    return []


# The gate's own filterset, read from the Makefile rather than restated here, so
# this check cannot drift from the run it is about.
NEXTEST_FILTER_RE = re.compile(r"^NEXTEST_FILTER\s*:?=\s*(?P<filter>.+)$", re.M)

# Any Rust source path the ledger names. Deliberately not anchored to a
# `::<test>` suffix: the Guard column names a test both as `path.rs::name` and as
# `path.rs`: `name`, and the reachability question below is about the file's
# target, not about the test's name. A path mentioned for some other reason is
# harmless — the check only has anything to say about paths inside a `tests/`
# directory.
GUARD_RE = re.compile(r"(?P<path>(?:crates|bin|tools)/[A-Za-z0-9_./-]+\.rs)")

# A `mod <name>;` declaration, which is what ties a `tests/<name>/mod.rs` guard to
# the integration-test binary that actually compiles it.
def _module_declaration(module: str) -> re.Pattern[str]:
    return re.compile(rf"^\s*(?:pub\s+)?mod\s+{re.escape(module)}\s*;", re.M)


# Integration-test binaries that deliberately run outside `make nextest`, mapped
# to what does run them. Keyed by `(package, binary)` rather than by file: a
# binary is what the filterset selects and what a workflow names, so one entry
# covers every module compiled into it and a new module does not need a new
# entry.
#
# Everything else in an integration-test target has to be named by the gate's
# filterset. `--tests` compiles the binary either way, so a missing clause buys
# nothing but the seconds of running it.
TARGETS_RUN_OUTSIDE_THE_UNIT_GATE = {
    (
        "runtime",
        "integration",
    ): ".github/workflows/integration.yml — needs credentials and live services",
}


def _package_of(path: Path) -> str | None:
    """The package a repo-relative source path belongs to."""
    for parent in path.parents:
        manifest = REPO / parent / "Cargo.toml"
        if not manifest.is_file():
            continue
        name = re.search(r'^name\s*=\s*"(?P<name>[^"]+)"', manifest.read_text(encoding="utf-8"), re.M)
        return name.group("name") if name else None
    return None


def _integration_target(path: str) -> tuple[str, str] | None:
    """`(package, binary)` when `path` is compiled into an integration-test binary.

    `None` for a guard in a `src/` tree — a lib or bin target, which the gate
    selects wholesale by kind.
    """
    parts = Path(path).parts
    if "tests" not in parts:
        return None
    index = parts.index("tests")
    crate_dir = Path(*parts[:index])
    inside = parts[index + 1 :]
    if not inside:
        return None
    package = _package_of(Path(path))
    if package is None:
        return None
    if len(inside) == 1:
        # `tests/<name>.rs` is its own target.
        return package, inside[0].removesuffix(".rs")
    # `tests/<name>/…` is a module; the target is whichever `tests/*.rs` declares it.
    declaration = _module_declaration(inside[0])
    for candidate in sorted((REPO / crate_dir / "tests").glob("*.rs")):
        if declaration.search(candidate.read_text(encoding="utf-8")):
            return package, candidate.stem
    return None


def guard_reachability(ledger_text: str) -> list[str]:
    """Whether `make nextest` actually runs every guard the ledger names.

    A guard is a test that fails when a patch goes missing, so a guard the gate
    never selects is a comment: the ledger keeps claiming coverage while nothing
    checks it. `kind(=lib)` sweeps up every unit test, so the exposure is the
    integration-test targets, which the filterset has to name one at a time.

    This matches the filterset's clauses textually rather than evaluating them —
    it looks for the `binary(=…)` or `package(=…) & kind(=test)` forms the
    Makefile is written in. A clause written some other way reads here as
    unreachable, which fails loudly and is fixed by naming it the usual way.

    A guard in a `src/` tree is skipped, because `kind(=lib)` and the per-crate
    `kind(=bin)` clauses already cover those. So is a path whose owning target
    cannot be resolved — a `tests/<dir>/` module no `tests/*.rs` declares is not
    compiled at all, which is a different problem from not being selected.
    """
    if not MAKEFILE.is_file():
        return [f"{MAKEFILE.relative_to(REPO)} not found, so the nextest filterset cannot be read"]
    filterset = NEXTEST_FILTER_RE.search(MAKEFILE.read_text(encoding="utf-8"))
    if not filterset:
        return ["Makefile no longer defines NEXTEST_FILTER, so nothing pins the gate's selection"]
    selection = filterset.group("filter")

    errors = []
    for path in sorted({match.group("path") for match in GUARD_RE.finditer(ledger_text)}):
        if not (REPO / path).is_file():
            errors.append(
                f"docs/dev/fork_patches.md names a guard in {path}, which does not exist"
            )
            continue
        target = _integration_target(path)
        if target is None:
            continue
        if target in TARGETS_RUN_OUTSIDE_THE_UNIT_GATE:
            continue
        package, binary = target
        if f"binary(={binary})" in selection or f"package(={package}) & kind(=test)" in selection:
            continue
        errors.append(
            f"docs/dev/fork_patches.md names {path} as a guard, but `make nextest` does not "
            f"select it: add `(package(={package}) & binary(={binary}))` to NEXTEST_FILTER, or "
            f"record ({package}, {binary}) in TARGETS_RUN_OUTSIDE_THE_UNIT_GATE with the runner "
            f"that does run it"
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

    ledger_text = LEDGER.read_text(encoding="utf-8")
    pinned = pinned_forks(LOCK.read_text(encoding="utf-8"))
    recorded = ledger_pins(ledger_text)

    if args.list:
        for repo in sorted(set(pinned) | set(recorded)):
            lock_rev = ", ".join(sorted(pinned.get(repo, set()))) or "-"
            doc_rev = ", ".join(recorded.get(repo, [])) or "-"
            status = "ok" if lock_rev == doc_rev else "DRIFT"
            print(f"{status:6} {repo:28} lock={lock_rev[:12]:14} ledger={doc_rev[:12]}")

    blocking = temporary_pins(ledger_text)
    if blocking:
        print(f"\n{len(blocking)} pin(s) not ready to land:\n", file=sys.stderr)
        for item in blocking:
            print(f"  - {item}", file=sys.stderr)
        print(
            "\nA fork pinned at a pull request's branch is reviewable but not landable: the "
            "branch goes away when that pull request merges, leaving trunk on a revision no "
            "branch reaches.",
            file=sys.stderr,
        )
        return 1

    errors = (
        drift(pinned, recorded)
        + gap_accounting(ledger_text)
        + duckdb_thrift_iterator_equality(pinned)
        + guard_reachability(ledger_text)
    )
    if errors:
        print(
            f"\n{len(errors)} problem(s) with docs/dev/fork_patches.md:\n",
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
        extra = ""
        if duckdb_rs_rev(pinned) is not None:
            extra = "; duckdb-rs Thrift iterator equality present"
        print(f"fork-patch ledger: {len(pinned)} pinned forks, all recorded{extra}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
