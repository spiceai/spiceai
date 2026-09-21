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
# Pure stdlib; no third-party deps. It does shell out to `cargo metadata`: the
# guard-reachability check needs cargo's own target list, since that is what
# resolves `[[bin]] path`, the auto-discovered `src/bin/*.rs` binaries, the
# default binary a bare `src/main.rs` declares, and each target's
# `required-features` — and the duckdb-rs Thrift-equality check falls back to it
# when the git checkout is not already on disk. No compile either way; the
# resolve costs a fraction of a second.

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


# Target resolution reuses the workspace's own module walker rather than
# standing a second parser beside it. `check_module_reachability.py` already
# handles `#[path]` overrides, inline `mod { … }`, per-platform `cfg`
# alternatives and mod-rs directory ownership; a hand-rolled walk here would
# disagree with cargo in a different set of cases than that one does, and the
# disagreements would be silent.
sys.path.insert(0, str(Path(__file__).resolve().parent))

from check_module_reachability import (  # noqa: E402
    parse_mods,
    resolve_child,
    walk_from_root,
)

# The gate's package/feature selection, which sits beside the filterset and is
# read from the Makefile for the same reason: so this check cannot drift from
# the run it is about. Line continuations are joined before it is parsed.
NEXTEST_SELECTION_RE = re.compile(
    r"^NEXTEST_SELECTION\s*:?=\s*(?P<selection>(?:[^\n]*\\\n)*[^\n]*)", re.M
)

# `package(=…)` / `binary(=…)` inside one clause of the gate's filterset.
_PACKAGE_CLAUSE_RE = re.compile(r"package\(=(?P<name>[^)]+)\)")
_BINARY_CLAUSE_RE = re.compile(r"binary\(=(?P<name>[^)]+)\)")

# Cargo target kinds that mean "library" to a nextest `kind(=lib)` clause.
_LIBRARY_KINDS = {"lib", "rlib", "dylib", "cdylib", "staticlib"}


def _gate_features(makefile_text: str) -> set[str]:
    """The `package/feature` pairs the gate's nextest run turns on explicitly."""
    match = NEXTEST_SELECTION_RE.search(makefile_text)
    if not match:
        return set()
    selection = match.group("selection").replace("\\\n", " ")
    enabled: set[str] = set()
    for group in re.findall(r"--features[=\s]+(\S+)", selection):
        enabled.update(part for part in group.split(",") if part)
    return enabled


def _cargo_metadata(features: set[str]) -> dict:
    """The workspace as cargo resolves it for the gate's own feature selection.

    Resolved rather than `--no-deps`, because the resolve is what answers *is
    this feature on in this build* — including one an unrelated crate turns on
    by depending on it. That is the question both the `required-features` check
    and the `cfg(feature = …)` walk have to ask, and guessing at it from the
    manifest gets unification wrong in both directions.
    """
    command = ["cargo", "metadata", "--format-version", "1"]
    for feature in sorted(features):
        command += ["--features", feature]
    try:
        out = subprocess.run(command, cwd=REPO, capture_output=True, text=True, check=True)
    except FileNotFoundError:
        # Exit 2 (tooling error), never 1 — 1 means an actual ledger problem.
        print("error: `cargo` not found on PATH, so the workspace cannot be read.", file=sys.stderr)
        raise SystemExit(2)
    except subprocess.CalledProcessError as e:
        print(f"error: `cargo metadata` failed:\n{e.stderr}", file=sys.stderr)
        raise SystemExit(2)
    try:
        return json.loads(out.stdout)
    except json.JSONDecodeError as e:
        print(f"error: `cargo metadata` emitted invalid JSON: {e}", file=sys.stderr)
        raise SystemExit(2)


# A `feature = "…"` predicate, which is the only kind this evaluates.
_FEATURE_PREDICATE_RE = re.compile(r'^feature\s*=\s*"(?P<name>[^"]+)"$')


def _split_top_level(text: str) -> list[str]:
    """`text` split on the commas that sit outside any parentheses."""
    parts: list[str] = []
    depth = 0
    start = 0
    for index, char in enumerate(text):
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        elif char == "," and depth == 0:
            parts.append(text[start:index])
            start = index + 1
    parts.append(text[start:])
    return [part.strip() for part in parts if part.strip()]


def _is_feature_only(predicate: str) -> bool:
    """Whether `predicate` is built from nothing but features and `all/any/not`."""
    residue = re.sub(r'feature\s*=\s*"[^"]*"', "", predicate)
    residue = re.sub(r"\b(?:all|any|not)\b", "", residue)
    return not re.sub(r"[(),\s]", "", residue)


def _cfg_allows(predicate: str, enabled: set[str]) -> bool:
    """Whether a `#[cfg(…)]` predicate holds for this build's features.

    Only the feature dimension is decided, and only when the predicate is made
    of nothing else. A predicate mentioning `target_os`, `unix`, `test` or
    anything else is treated as satisfiable: the module compiles under some
    configuration, and a guard that runs on another platform must not be
    reported as unrun. The bias is deliberate and one-directional — this can
    fail to report, never invent.
    """
    if not predicate.strip() or not _is_feature_only(predicate):
        return True
    terms = _split_top_level(predicate)
    if len(terms) != 1:
        # Several `cfg` attributes on one declaration all have to hold.
        return all(_cfg_allows(term, enabled) for term in terms)
    term = terms[0]
    named = _FEATURE_PREDICATE_RE.match(term)
    if named:
        return named.group("name") in enabled
    for operator in ("all", "any", "not"):
        if term.startswith(f"{operator}(") and term.endswith(")"):
            inner = _split_top_level(term[len(operator) + 1 : -1])
            if operator == "all":
                return all(_cfg_allows(item, enabled) for item in inner)
            if operator == "any":
                return any(_cfg_allows(item, enabled) for item in inner)
            return not _cfg_allows(inner[0], enabled) if inner else True
    return True


def _union_clauses(selection: str) -> list[str]:
    """The gate's filterset split into the clauses it unions with `+`.

    Splitting at paren depth zero is what lets a clause be read whole. Tested as
    a bare substring of the whole expression, `binary(=x)` also matches a clause
    that pairs that binary with a *different* package — which selects nothing of
    ours, and would excuse a guard the gate never runs.
    """
    clauses: list[str] = []
    depth = 0
    start = 0
    for index, char in enumerate(selection):
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        elif char == "+" and depth == 0:
            clauses.append(selection[start:index])
            start = index + 1
    clauses.append(selection[start:])
    return [clause.strip() for clause in clauses if clause.strip()]


def _clause_selects(clause: str, package: str, kind: str, name: str) -> bool:
    """Whether one union clause selects this target.

    Three forms count, which are the ones the Makefile is written in: a bare
    `kind(=lib)` or `kind(=proc-macro)`, which sweep those targets across the
    workspace; a `binary(=…)` naming this target — qualified with this package,
    or unqualified, as `binary(=metrics)` is — and `package(=…) & kind(=…)`,
    which takes every target of that kind in the package.
    """
    packages = set(_PACKAGE_CLAUSE_RE.findall(clause))
    if packages and packages != {package}:
        return False
    if f"kind(={kind})" in clause and (kind in {"lib", "proc-macro"} or packages):
        return True
    return name in set(_BINARY_CLAUSE_RE.findall(clause))


_WORKSPACE_TARGETS: list[dict] | None = None
_TARGETS_BY_FILE: dict[Path, list[dict]] | None = None


def _workspace_targets(gate_features: frozenset[str] = frozenset()) -> list[dict]:
    """Every workspace target, as cargo itself reports it.

    Cargo is the authority here rather than the manifest text: it is what
    resolves `[[bin]] path`, the auto-discovered `src/bin/*.rs` binaries, the
    default binary a bare `src/main.rs` declares, and each target's
    `required-features`. Parsing those by hand got two of them wrong.

    Behind a function so the checker's own tests can hand it a synthetic
    workspace, and cached because one ledger names dozens of guards.
    """
    global _WORKSPACE_TARGETS
    if _WORKSPACE_TARGETS is not None:
        return _WORKSPACE_TARGETS
    metadata = _cargo_metadata(set(gate_features))
    members = set(metadata.get("workspace_members", []))
    resolved: dict[str, set[str]] = {}
    for node in metadata.get("resolve", {}).get("nodes", []):
        resolved[node.get("id", "")] = set(node.get("features", []))
    targets: list[dict] = []
    for package in metadata.get("packages", []):
        if package.get("id") not in members:
            continue
        enabled = resolved.get(package.get("id", ""), set())
        for target in package.get("targets", []):
            kinds = target.get("kind", [])
            if "custom-build" in kinds:
                continue
            targets.append(
                {
                    "package": package["name"],
                    "name": target.get("name", ""),
                    "kind": _filterset_kind(kinds),
                    "src_path": target.get("src_path", ""),
                    "crate_dir": str(Path(package["manifest_path"]).parent),
                    "required_features": target.get("required-features") or [],
                    "enabled_features": enabled,
                }
            )
    _WORKSPACE_TARGETS = targets
    return targets


def _filterset_kind(kinds: list[str]) -> str:
    """The kind a nextest `kind(=…)` clause would use for this cargo target."""
    if any(kind in _LIBRARY_KINDS for kind in kinds):
        return "lib"
    return kinds[0] if kinds else "unknown"


def _walk_target(root: Path, enabled: set[str]) -> set[Path]:
    """Every source file the target rooted at `root` compiles.

    A *target root* behaves like `mod.rs` whatever it is called: its submodules
    live beside it, not in a directory named after it. `walk_from_root` knows
    that for `lib.rs` and `main.rs`, which is all its own caller ever hands it —
    it walks only roots under `src/`. Here the roots include integration tests,
    where it matters: `mod abfs;` in `crates/runtime/tests/integration.rs`
    is `tests/abfs/mod.rs`, not `tests/integration/abfs/mod.rs`. So the root's
    own declarations are resolved against its directory and everything below it
    is handed to the shared walker, which has the ordinary rule right.

    `#[cfg(feature = …)]` is honoured, unlike in the shared walker's own caller:
    a module the gate's feature resolve switches off is not compiled, so a guard
    inside one does not run however well the filterset names its target.
    """

    def allows(predicate: str) -> bool:
        return _cfg_allows(predicate, enabled)

    reached: set[Path] = {root.resolve()}
    for name, _, overrides, inline, cfg in parse_mods(root):
        if cfg and not allows(cfg):
            continue
        base = root.parent.joinpath(*inline)
        for override in overrides or (None,):
            child = resolve_child(base, name, override)
            if child is not None:
                walk_from_root(child, reached, cfg_enabled=allows)
    return reached


def _targets_by_file(
    wanted: frozenset[Path], gate_features: frozenset[str]
) -> dict[Path, list[dict]]:
    """Which targets compile each of `wanted`, by walking the roots that could.

    Per target rather than per crate, because each binary has a module tree of
    its own: `src/main.rs` and `src/bin/aux.rs` are separate roots, and a module
    only one of them declares is compiled only into that one.

    Only the crates holding a wanted path are walked. Walking all 160 costs nine
    seconds of parsing for an answer about a few dozen files, and this guard
    sits in `make lint-rust` beside the other no-compile checks.
    """
    global _TARGETS_BY_FILE
    if _TARGETS_BY_FILE is not None:
        return _TARGETS_BY_FILE
    mapping: dict[Path, list[dict]] = {}
    for target in _workspace_targets(gate_features):
        crate_dir = Path(target["crate_dir"]).resolve()
        if not any(_is_under(source, crate_dir) for source in wanted):
            continue
        root = Path(target["src_path"])
        if not root.is_file():
            continue
        for source in _walk_target(root, target["enabled_features"]):
            mapping.setdefault(source, []).append(target)
    _TARGETS_BY_FILE = mapping
    return mapping


def _is_under(source: Path, directory: Path) -> bool:
    return source == directory or directory in source.parents


def _reached_ignoring_features(source: Path, gate_features: frozenset[str]) -> bool:
    """Whether some target would compile `source` if no `cfg(feature)` applied.

    Only asked about a path already found unreachable, to separate "nothing
    declares this" from "a feature this build leaves off declares it" — the same
    file, two different fixes, and one second of walking to tell them apart.
    """
    for target in _workspace_targets(gate_features):
        crate_dir = Path(target["crate_dir"]).resolve()
        if not _is_under(source.resolve(), crate_dir):
            continue
        root = Path(target["src_path"])
        if not root.is_file():
            continue
        reached: set[Path] = {root.resolve()}
        for name, _, overrides, inline, _cfg in parse_mods(root):
            base = root.parent.joinpath(*inline)
            for override in overrides or (None,):
                child = resolve_child(base, name, override)
                if child is not None:
                    walk_from_root(child, reached)
        if source.resolve() in reached:
            return True
    return False


def _unmet_required_features(target: dict) -> list[str]:
    """Required features of `target` that the gate's build does not turn on.

    Being named in the filterset is not enough to make a target run: cargo skips
    one whose `required-features` are unmet **without saying so**. That has
    already happened here — `result_correctness_vs_duckdb_test` was selected and
    silently never built until `NEXTEST_SELECTION` gained the feature (see the
    comment above it in the Makefile) — which is the shape this whole check
    exists to catch.

    "Turned on" is cargo's resolve for the gate's own selection, so a feature an
    unrelated crate enables by depending on it counts, because it does in fact
    build the target. If that dependency edge later goes away the resolve
    changes and this starts failing, which is the moment it should.
    """
    return sorted(
        feature
        for feature in target["required_features"]
        if feature not in target["enabled_features"]
    )


def _why_unrun(target: dict, unmet: list[str]) -> str:
    """What to change so this target's tests run in the gate."""
    package, name, kind = target["package"], target["name"], target["kind"]
    if unmet:
        return (
            f"({package}, {name}) requires {', '.join(f'`{f}`' for f in unmet)}, which the "
            f"gate's run does not turn on — cargo skips such a target without saying so, so it "
            f"is selected and never built. Add `--features {package}/{unmet[0]}` to "
            f"NEXTEST_SELECTION"
        )
    if kind in {"lib", "proc-macro"}:
        return f"({package}, {name}) is a {kind}, so restore `kind(={kind})` to NEXTEST_FILTER"
    if kind == "bin":
        return f"add `(package(={package}) & kind(=bin))` to NEXTEST_FILTER"
    if kind == "test":
        return f"add `(package(={package}) & binary(={name}))` to NEXTEST_FILTER"
    return f"({package}, {name}) is a {kind} target, which `make nextest` does not run at all"


def guard_reachability(ledger_text: str) -> list[str]:
    """Whether `make nextest` actually runs every guard the ledger names.

    A guard is a test that fails when a patch goes missing, so a guard the gate
    never runs is a comment: the ledger keeps claiming coverage while nothing
    checks it.

    Two questions have to be asked of each one, and only the pair is sufficient.
    *Does cargo build it* — which target's module tree reaches the file, and are
    that target's `required-features` on. *Does the gate run it* — does some
    clause of the filterset select that target. A guard that fails either is
    named here.

    The filterset is matched textually rather than evaluated: the union is split
    at `+` and each clause read whole, looking for the `kind(=lib)`,
    `binary(=…)` and `package(=…) & kind(=…)` forms the Makefile is written in.
    A clause written some other way reads here as unreachable, which fails
    loudly and is fixed by naming it the usual way.
    """
    if not MAKEFILE.is_file():
        return [f"{MAKEFILE.relative_to(REPO)} not found, so the nextest filterset cannot be read"]
    makefile_text = MAKEFILE.read_text(encoding="utf-8")
    filterset = NEXTEST_FILTER_RE.search(makefile_text)
    if not filterset:
        return ["Makefile no longer defines NEXTEST_FILTER, so nothing pins the gate's selection"]
    selection = filterset.group("filter")
    enabled_features = frozenset(_gate_features(makefile_text))
    clauses = _union_clauses(selection)

    named = sorted({match.group("path") for match in GUARD_RE.finditer(ledger_text)})
    wanted = frozenset((REPO / path).resolve() for path in named if (REPO / path).is_file())

    errors = []
    for path in named:
        source = REPO / path
        if not source.is_file():
            errors.append(f"docs/dev/fork_patches.md names a guard in {path}, which does not exist")
            continue
        targets = _targets_by_file(wanted, enabled_features).get(source.resolve(), [])
        if not targets:
            # Two different failures look the same from here and take different
            # fixes: a file nothing declares, and one declared only behind a
            # feature this build leaves off. Telling them apart costs a second
            # walk, so it is done only for a path that has already failed.
            if _reached_ignoring_features(source, enabled_features):
                errors.append(
                    f"docs/dev/fork_patches.md names {path} as a guard, but every `mod` "
                    f"declaration reaching it is behind a `cfg(feature = …)` the gate's build "
                    f"leaves off, so cargo compiles it into nothing — turn the feature on in "
                    f"NEXTEST_SELECTION, or record the target in "
                    f"TARGETS_RUN_OUTSIDE_THE_UNIT_GATE with the runner that does build it"
                )
            else:
                errors.append(
                    f"docs/dev/fork_patches.md names {path} as a guard, but no target's module "
                    f"tree reaches it, so cargo compiles it into nothing and it cannot run — "
                    f"declare the module from its parent, or correct the path"
                )
            continue
        # A guard runs if *any* target that compiles it both builds and is
        # selected. Several can: a module a library and a binary both declare is
        # compiled into each.
        blocked = []
        for target in targets:
            if (target["package"], target["name"]) in TARGETS_RUN_OUTSIDE_THE_UNIT_GATE:
                break
            unmet = _unmet_required_features(target)
            if not unmet and any(
                _clause_selects(clause, target["package"], target["kind"], target["name"])
                for clause in clauses
            ):
                break
            blocked.append(_why_unrun(target, unmet))
        else:
            errors.append(
                f"docs/dev/fork_patches.md names {path} as a guard, but `make nextest` does not "
                f"run it: {'; '.join(blocked)} — or record the target in "
                f"TARGETS_RUN_OUTSIDE_THE_UNIT_GATE with the runner that does"
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
