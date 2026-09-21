#!/usr/bin/env python3
# Copyright 2024-2026 The Spice.ai OSS Authors
#
# Tests for scripts/check_fork_patches.py.
#
# The guard's live-tree run only exercises the shapes today's `Cargo.lock` and
# ledger happen to contain — and while both agree, it reports success whether or
# not it is still reading either file correctly. A regex that stopped matching
# would find zero forks, zero rows, and no disagreements, so the guard would pass
# green on a workspace it had gone blind to. The same reason
# `test_check_module_reachability.py` runs ahead of its guard.
#
# The cases below pin both parsers against the manifest spellings this workspace
# actually uses (`?rev=`, `?tag=`, `.git` and bare repository names) and pin each
# drift the guard exists to catch.
#
# Run: python3 scripts/test_check_fork_patches.py

from __future__ import annotations

import io
import sys
import tarfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

# The module object as well as the names below: the reachability cases point the
# checker at a temporary tree, which means rebinding `REPO`, `MAKEFILE` and
# `TARGETS_RUN_OUTSIDE_THE_UNIT_GATE` on the module itself. Imported names cannot
# do that — a rebound local would leave the functions reading the originals.
import check_fork_patches as cfp  # noqa: E402
from check_fork_patches import (  # noqa: E402
    DUCKDB_RS_REPO,
    DUCKDB_THRIFT_EQUALITY_MARKER,
    DUCKDB_THRIFT_HEADER_SUFFIX,
    LEDGER,
    LOCK,
    drift,
    duckdb_thrift_iterator_equality,
    gap_accounting,
    ledger_pins,
    pinned_forks,
    temporary_pins,
    thrift_header_from_tarball,
)

failures = 0
checks = 0

A = "a" * 40
B = "b" * 40
C = "c" * 40


def check(name: str, got, want) -> None:
    global failures, checks
    checks += 1
    if got == want:
        print(f"  ok: {name}")
    else:
        failures += 1
        print(f"  FAIL: {name}\n    got:  {got!r}\n    want: {want!r}")


def check_contains(name: str, haystack: list[str], needle: str) -> None:
    global failures, checks
    checks += 1
    if any(needle in item for item in haystack):
        print(f"  ok: {name}")
    else:
        failures += 1
        print(f"  FAIL: {name}\n    no item contains {needle!r}\n    items: {haystack!r}")


print("lockfile parsing")

# The three source spellings this workspace's lockfile uses: a `rev=` pin on a
# `.git` URL, a `tag=` pin (clickhouse-rs), and a bare repository name with no
# `.git` suffix (async-openai, tiberius, graph-rs-sdk).
LOCK_SAMPLE = f"""
[[package]]
name = "vortex-array"
version = "0.79.0"
source = "git+https://github.com/spiceai/vortex.git?rev={A}#{A}"

[[package]]
name = "vortex-io"
version = "0.79.0"
source = "git+https://github.com/spiceai/vortex.git?rev={A}#{A}"

[[package]]
name = "clickhouse-rs"
version = "0.2.2"
source = "git+https://github.com/spiceai/clickhouse-rs.git?tag=0.2.2#{B}"

[[package]]
name = "async-openai"
version = "0.32.0"
source = "git+https://github.com/spiceai/async-openai?rev={C}#{C}"

[[package]]
name = "spiceai"
version = "3.0.0"
source = "git+https://github.com/spiceai/spice-rs.git?rev={A}#{A}"

[[package]]
name = "tokio"
version = "1.0.0"
source = "registry+https://github.com/rust-lang/crates.io-index"

[[package]]
name = "duckdb"
version = "1.5.5"
source = "git+https://github.com/duckdb/duckdb-rs.git?rev={B}#{B}"
"""

parsed = pinned_forks(LOCK_SAMPLE)
check("a `.git` URL with a rev pin is read once per repo, not once per crate", parsed.get("vortex"), {A})
check("a tag pin resolves to the commit after the fragment", parsed.get("clickhouse-rs"), {B})
check("a repository name with no `.git` suffix is read", parsed.get("async-openai"), {C})
check("a non-spiceai git dependency is not a fork of ours", "duckdb-rs" in parsed, False)
check("a registry dependency is ignored", "tokio" in parsed, False)
check("a spiceai repo with no upstream is not audited", "spice-rs" in parsed, False)

print("\nledger parsing")

LEDGER_SAMPLE = f"""
| Fork | Pinned revision | Branch | Spice patches | Guarded |
|---|---|---|---|---|
| [vortex](#vortex) | `{A}` | `spiceai-54` | 14 | 9 |
| [clickhouse-rs](#clickhouse-rs) | `{B}` | tag `0.2.2` | 2 | 0 |

Prose that mentions `{C}` in passing must not be read as a pin row.

| Patch | What breaks if it is lost | Loss | Guard |
|---|---|---|---|
| Arrow `Map` alias | every write fails | silent | some test |
"""

recorded = ledger_pins(LEDGER_SAMPLE)
check("a pin row is read from its link text", recorded.get("vortex"), [A])
check("a tag-pinned fork is read the same way", recorded.get("clickhouse-rs"), [B])
check("a revision mentioned in prose is not a pin row", len(recorded), 2)
check("a patch row is not mistaken for a pin row", "Arrow `Map` alias" in recorded, False)

print("\ndrift detection")

check("agreement is silent", drift({"vortex": {A}}, {"vortex": [A]}), [])
check_contains(
    "a moved pin is reported, and says to re-audit",
    drift({"vortex": {B}}, {"vortex": [A]}),
    "re-audit",
)
check_contains(
    "a fork with no ledger row is reported",
    drift({"vortex": {A}}, {}),
    "no row in docs/dev/fork_patches.md",
)
check_contains(
    "a ledger row for an unpinned fork is reported",
    drift({}, {"vortex": [A]}),
    "no longer pinned",
)
check_contains(
    "two rows for one fork is reported",
    drift({"vortex": {A}}, {"vortex": [A, B]}),
    "keep one per fork",
)
check_contains(
    "one fork pinned at two revisions at once is reported",
    drift({"vortex": {A, B}}, {"vortex": [A]}),
    "different revisions at once",
)

print("\ngap accounting")

GAPS_HEAD = """
| Patch | What breaks if it is lost | Loss | Guard |
|---|---|---|---|
| one | a thing | silent | **GAP** |
| two | another | silent | some test |
| three | a third | silent | **GAP** |

## Open gaps

**{count} rows above are marked GAP** — they have no repo-side guard.
"""

check("a count matching the tables is silent", gap_accounting(GAPS_HEAD.format(count=2)), [])
check_contains(
    "a count that has fallen behind the tables is reported",
    gap_accounting(GAPS_HEAD.format(count=1)),
    "2 table row(s) are marked **GAP** but the `Open gaps` section accounts for 1",
)
check_contains(
    "a section that no longer states a count is reported",
    gap_accounting("| one | x | silent | **GAP** |\n\n## Open gaps\n\nprose only.\n"),
    "no longer states how many rows it accounts for",
)
check_contains(
    "a ledger with no Open gaps section at all is reported",
    gap_accounting("| one | x | silent | **GAP** |\n"),
    "no `## Open gaps` section",
)

print("\nshipped tree")

# The parsers have to find something in the real files: a rewrite that broke
# either regex would leave both sides empty and agreeing.
lock_forks = pinned_forks(LOCK.read_text(encoding="utf-8"))
ledger_rows = ledger_pins(LEDGER.read_text(encoding="utf-8"))
check("the lockfile yields forks", len(lock_forks) > 20, True)
check("the ledger yields rows", len(ledger_rows) > 20, True)
check("the shipped tree has no drift", drift(lock_forks, ledger_rows), [])
ledger_text = LEDGER.read_text(encoding="utf-8")
check("the shipped tree marks at least one gap", "**GAP**" in ledger_text, True)
check("the shipped Open gaps list accounts for every gap", gap_accounting(ledger_text), [])

# A pin on a pull request's branch is reviewable and must not land. The marker is
# what makes it un-landable, so the guard has to see it — and has to stay quiet
# for the ordinary rows it sits beside.
_ORDINARY_ROW = (
    "| [vortex](#vortex) | `" + "a" * 40 + "` | `spiceai-54` |\n"
)
_TEMPORARY_ROW = (
    "| [vortex](#vortex) | `" + "b" * 40 + "` | `in-list-hashed-probe` "
    "(TEMPORARY: spiceai/vortex#95) |\n"
)

check(
    "a pin recorded against a long-lived branch does not block",
    temporary_pins(_ORDINARY_ROW),
    [],
)

_blocked = temporary_pins(_TEMPORARY_ROW)
check("a pin marked temporary blocks", len(_blocked), 1)
check(
    "the message names what has to merge first",
    "spiceai/vortex#95" in _blocked[0] and "vortex" in _blocked[0],
    True,
)
check(
    "a temporary row beside ordinary rows is still caught",
    len(temporary_pins(_ORDINARY_ROW + _TEMPORARY_ROW + _ORDINARY_ROW)),
    1,
)

print("\nduckdb-rs Thrift iterator equality")

# Synthetic tarball helpers — the live checkout is also exercised below, but the
# parser shapes have to hold when the archive is a fixture, or a cold CI agent
# that has not yet fetched duckdb-rs would leave the negative cases untested.


def _tarball_with_thrift(header_text: str, dest: Path) -> Path:
    dest.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(dest, "w:gz") as tf:
        payload = header_text.encode()
        info = tarfile.TarInfo(name=f"duckdb/{DUCKDB_THRIFT_HEADER_SUFFIX}")
        info.size = len(payload)
        tf.addfile(info, io.BytesIO(payload))
    return dest


_tmp = Path(__file__).resolve().parent / ".test_thrift_tmp"
try:
    with_eq = _tarball_with_thrift(
        "class TEnumIterator {\n  bool operator!=(const TEnumIterator& end);\n  "
        + DUCKDB_THRIFT_EQUALITY_MARKER
        + " { return !(*this != end); }\n};\n",
        _tmp / "with_eq.tar.gz",
    )
    without_eq = _tarball_with_thrift(
        "class TEnumIterator {\n  bool operator!=(const TEnumIterator& end);\n};\n",
        _tmp / "without_eq.tar.gz",
    )

    check(
        "a header carrying the marker is accepted",
        DUCKDB_THRIFT_EQUALITY_MARKER in (thrift_header_from_tarball(with_eq) or ""),
        True,
    )
    check(
        "a header missing the marker is rejected by the reader",
        DUCKDB_THRIFT_EQUALITY_MARKER in (thrift_header_from_tarball(without_eq) or ""),
        False,
    )
finally:
    import shutil

    shutil.rmtree(_tmp, ignore_errors=True)

# Live-tree: the pinned duckdb-rs revision must still carry the backport. A
# silent drop on the next re-cut is exactly what this guard exists to catch.
_live = duckdb_thrift_iterator_equality(pinned_forks(LOCK.read_text(encoding="utf-8")))
check("the shipped duckdb-rs pin still carries Thrift iterator equality", _live, [])
check(
    "the marker string names TEnumIterator",
    "TEnumIterator" in DUCKDB_THRIFT_EQUALITY_MARKER,
    True,
)
check(
    "the header suffix reaches Thrift.h",
    DUCKDB_THRIFT_HEADER_SUFFIX.endswith("Thrift.h"),
    True,
)
check("the duckdb-rs repo constant is set", DUCKDB_RS_REPO, "duckdb-rs")



print("\nguard reachability")

# `guard_reachability` answers two questions about a guard — does cargo build it,
# does the gate run it — against the live workspace and the live Makefile. The
# only way to pin what it does with a shape this workspace does not currently
# contain is to point it at one that does: a temporary tree of real `.rs` files,
# plus the target list cargo would report for it.
_reach = Path(__file__).resolve().parent / ".test_reachability_tmp"
try:
    _demo = _reach / "crates" / "demo"
    (_demo / "src" / "bin").mkdir(parents=True, exist_ok=True)
    (_demo / "tests" / "covered").mkdir(parents=True, exist_ok=True)
    (_demo / "tests" / "second").mkdir(parents=True, exist_ok=True)
    (_demo / "tests" / "orphan").mkdir(parents=True, exist_ok=True)
    (_demo / "Cargo.toml").write_text('[package]\nname = "demo"\n', encoding="utf-8")
    (_demo / "src" / "lib.rs").write_text("mod shared;\n", encoding="utf-8")
    (_demo / "src" / "shared.rs").write_text("", encoding="utf-8")
    # The default binary declares `guard`; the second binary declares nothing.
    # Each binary has a module tree of its own, which is what stops one being
    # excused because the other is selected.
    (_demo / "src" / "main.rs").write_text("mod guard;\n", encoding="utf-8")
    (_demo / "src" / "guard.rs").write_text("", encoding="utf-8")
    (_demo / "src" / "bin" / "aux.rs").write_text("", encoding="utf-8")
    # One integration binary, two modules, a nested module below one of them,
    # and two directories nothing declares.
    (_demo / "tests" / "integration.rs").write_text("mod covered;\npub mod second;\n", encoding="utf-8")
    (_demo / "tests" / "covered" / "mod.rs").write_text("mod nested;\n", encoding="utf-8")
    (_demo / "tests" / "covered" / "nested.rs").write_text("", encoding="utf-8")
    (_demo / "tests" / "covered" / "orphan.rs").write_text("", encoding="utf-8")
    (_demo / "tests" / "second" / "mod.rs").write_text("", encoding="utf-8")
    (_demo / "tests" / "orphan" / "mod.rs").write_text("", encoding="utf-8")
    (_demo / "tests" / "standalone.rs").write_text("", encoding="utf-8")

    # A crate that builds no library: `kind(=lib)` does not reach its `src/`.
    _tool = _reach / "tools" / "harness"
    (_tool / "src").mkdir(parents=True, exist_ok=True)
    (_tool / "Cargo.toml").write_text('[package]\nname = "harness"\n', encoding="utf-8")
    (_tool / "src" / "main.rs").write_text("mod mode_a;\n", encoding="utf-8")
    (_tool / "src" / "mode_a.rs").write_text("", encoding="utf-8")

    # A test target cargo builds only when a feature is on.
    _gated = _reach / "crates" / "gated"
    (_gated / "tests").mkdir(parents=True, exist_ok=True)
    (_gated / "src").mkdir(parents=True, exist_ok=True)
    (_gated / "src" / "lib.rs").write_text("", encoding="utf-8")
    (_gated / "Cargo.toml").write_text('[package]\nname = "gated"\n', encoding="utf-8")
    (_gated / "tests" / "needs_extra.rs").write_text("", encoding="utf-8")
    (_gated / "tests" / "needs_baseline.rs").write_text("", encoding="utf-8")

    def _target(package, name, kind, src, *, crate, required=(), defaults=()):
        return {
            "package": package,
            "name": name,
            "kind": kind,
            "src_path": str(src),
            "required_features": list(required),
            "default_features": set(defaults),
            "crate_dir": str(crate),
        }

    # What `cargo metadata` reports for the tree above.
    TARGETS = [
        _target("demo", "demo", "lib", _demo / "src" / "lib.rs", crate=_demo),
        _target("demo", "demo", "bin", _demo / "src" / "main.rs", crate=_demo),
        _target("demo", "aux", "bin", _demo / "src" / "bin" / "aux.rs", crate=_demo),
        _target("demo", "integration", "test", _demo / "tests" / "integration.rs", crate=_demo),
        _target("demo", "standalone", "test", _demo / "tests" / "standalone.rs", crate=_demo),
        _target("harness", "harness", "bin", _tool / "src" / "main.rs", crate=_tool),
        _target("gated", "gated", "lib", _gated / "src" / "lib.rs", crate=_gated),
        _target(
            "gated", "needs_extra", "test", _gated / "tests" / "needs_extra.rs",
            crate=_gated, required=["extra"], defaults=["baseline"],
        ),
        _target(
            "gated", "needs_baseline", "test", _gated / "tests" / "needs_baseline.rs",
            crate=_gated, required=["baseline"], defaults=["baseline"],
        ),
    ]

    def reachability(
        ledger: str, filterset: str, outside: dict | None = None, features: str = ""
    ) -> list[str]:
        """`guard_reachability` against the temporary tree rather than the repo."""
        (_reach / "Makefile").write_text(
            f"NEXTEST_SELECTION := --all{features}\nNEXTEST_FILTER := {filterset}\n",
            encoding="utf-8",
        )
        saved = (
            cfp.REPO,
            cfp.MAKEFILE,
            cfp.TARGETS_RUN_OUTSIDE_THE_UNIT_GATE,
            cfp._WORKSPACE_TARGETS,
            cfp._TARGETS_BY_FILE,
        )
        cfp.REPO = _reach
        cfp.MAKEFILE = _reach / "Makefile"
        cfp.TARGETS_RUN_OUTSIDE_THE_UNIT_GATE = {} if outside is None else outside
        cfp._WORKSPACE_TARGETS = TARGETS
        cfp._TARGETS_BY_FILE = None
        try:
            return cfp.guard_reachability(ledger)
        finally:
            (
                cfp.REPO,
                cfp.MAKEFILE,
                cfp.TARGETS_RUN_OUTSIDE_THE_UNIT_GATE,
                cfp._WORKSPACE_TARGETS,
                cfp._TARGETS_BY_FILE,
            ) = saved

    LIB_ONLY = "kind(=lib)"
    NAMES_THE_BINARY = "kind(=lib) + (package(=demo) & binary(=integration))"

    # Both spellings the Guard column uses have to reach the same target. The
    # second is the one a `::<test>`-anchored pattern skipped silently.
    check_contains(
        "a `path.rs::test` guard the filterset does not name is reported",
        reachability("| `crates/demo/tests/standalone.rs::a_guard` |", LIB_ONLY),
        "binary(=standalone)",
    )
    check_contains(
        "a `` `path.rs`: `test` `` guard is parsed the same way",
        reachability("| `crates/demo/tests/standalone.rs`: `a_guard` |", LIB_ONLY),
        "binary(=standalone)",
    )

    # A `tests/<dir>/` guard belongs to whichever `tests/*.rs` declares the
    # module, not to a target named after the directory.
    check_contains(
        "a module guard resolves to the binary that declares it",
        reachability("`crates/demo/tests/covered/mod.rs::a_guard`", LIB_ONLY),
        "binary(=integration)",
    )
    check(
        "naming that binary in the filterset satisfies the check",
        reachability("`crates/demo/tests/covered/mod.rs::a_guard`", NAMES_THE_BINARY),
        [],
    )
    check(
        "the `package(=…) & kind(=test)` form is accepted too",
        reachability(
            "`crates/demo/tests/covered/mod.rs::a_guard`",
            "kind(=lib) + (package(=demo) & kind(=test))",
        ),
        [],
    )

    # The allowlist is keyed by `(package, target)`, so one entry covers every
    # module compiled into that target — adding a module needs no new entry.
    check(
        "one allowlist entry covers every module in the binary",
        reachability(
            "`crates/demo/tests/covered/mod.rs` and `crates/demo/tests/second/mod.rs`",
            LIB_ONLY,
            outside={("demo", "integration"): "runs in another workflow"},
        ),
        [],
    )
    check_contains(
        "the allowlist does not excuse a different binary",
        reachability(
            "`crates/demo/tests/standalone.rs::a_guard`",
            LIB_ONLY,
            outside={("demo", "integration"): "runs in another workflow"},
        ),
        "binary(=standalone)",
    )

    # A clause is read whole, so the package it names has to be this one. Tested
    # as a bare substring, `binary(=integration)` matches a clause that pairs the
    # name with someone else's package and excuses a guard nothing runs.
    check_contains(
        "a binary selector qualified with another package does not count",
        reachability(
            "`crates/demo/tests/covered/mod.rs::a_guard`",
            "kind(=lib) + (package(=other) & binary(=integration))",
        ),
        "binary(=integration)",
    )
    check(
        "an unqualified binary selector counts for any package",
        reachability("`crates/demo/tests/covered/mod.rs::a_guard`", "kind(=lib) + binary(=integration)"),
        [],
    )
    check_contains(
        "this package paired with another binary does not count",
        reachability(
            "`crates/demo/tests/covered/mod.rs::a_guard`",
            "kind(=lib) + (package(=demo) & binary(=standalone))",
        ),
        "binary(=integration)",
    )

    # Reaching a target through the first component below `tests/` says nothing
    # about the rest of the chain, and a file its own parent never declares is
    # compiled by nothing at all — a different failure from not being selected.
    check(
        "a nested module its parent declares resolves to the same binary",
        reachability("`crates/demo/tests/covered/nested.rs::a_guard`", NAMES_THE_BINARY),
        [],
    )
    check_contains(
        "a nested module its parent does not declare is compiled by nothing",
        reachability("`crates/demo/tests/covered/orphan.rs::a_guard`", NAMES_THE_BINARY),
        "no target's module tree reaches it",
    )
    check_contains(
        "a `tests/` directory no target declares is compiled by nothing",
        reachability("`crates/demo/tests/orphan/mod.rs::a_guard`", NAMES_THE_BINARY),
        "no target's module tree reaches it",
    )
    check(
        "a `<dir>/mod.rs` guard is not read as a submodule named `mod`",
        reachability("`crates/demo/tests/covered/mod.rs::a_guard`", NAMES_THE_BINARY),
        [],
    )

    # Libraries are swept wholesale — but only because the clause is there.
    check(
        "a `src/` guard in a library is left to the `kind(=lib)` sweep",
        reachability("`crates/demo/src/shared.rs::a_guard`", LIB_ONLY),
        [],
    )
    check_contains(
        "a library guard is reported when `kind(=lib)` is gone",
        reachability("`crates/demo/src/shared.rs::a_guard`", "kind(=proc-macro)"),
        "restore `kind(=lib)`",
    )

    # A crate with no library: its `src/` guards run only because a clause names
    # its binary, and dropping that clause was invisible.
    check_contains(
        "a guard in a bin-only crate is reported when nothing names its binary",
        reachability("`tools/harness/src/mode_a.rs::a_guard`", LIB_ONLY),
        "(package(=harness) & kind(=bin))",
    )
    check(
        "naming the crate's binaries satisfies it",
        reachability(
            "`tools/harness/src/mode_a.rs::a_guard`",
            "kind(=lib) + (package(=harness) & kind(=bin))",
        ),
        [],
    )
    check_contains(
        "`kind(=bin)` for another package does not count",
        reachability(
            "`tools/harness/src/mode_a.rs::a_guard`",
            "kind(=lib) + (package(=demo) & kind(=bin))",
        ),
        "(package(=harness) & kind(=bin))",
    )

    # Each binary has a module tree of its own. `guard.rs` is declared by the
    # crate's default binary and by nothing else, so selecting the *other*
    # binary of the same crate must not excuse it — and the default binary is a
    # target even though `src/bin/` also exists.
    check_contains(
        "another binary of the same crate does not excuse a module it never declares",
        reachability("`crates/demo/src/guard.rs::a_guard`", "kind(=lib) + binary(=aux)"),
        "(package(=demo) & kind(=bin))",
    )
    check(
        "selecting the binary that does declare it satisfies the check",
        reachability("`crates/demo/src/guard.rs::a_guard`", "kind(=lib) + binary(=demo)"),
        [],
    )

    # Selection is necessary but not sufficient: cargo skips a target whose
    # `required-features` are unmet and says nothing, so a guard there is
    # selected and never built. This has happened in this repo — see the
    # NEXTEST_SELECTION comment in the Makefile.
    GATED = "kind(=lib) + (package(=gated) & binary(=needs_extra))"
    check_contains(
        "a guard in a target whose required feature is off is reported",
        reachability("`crates/gated/tests/needs_extra.rs::a_guard`", GATED),
        "requires `extra`",
    )
    check(
        "naming that feature in NEXTEST_SELECTION clears it",
        reachability(
            "`crates/gated/tests/needs_extra.rs::a_guard`", GATED, features=" --features gated/extra"
        ),
        [],
    )
    check(
        "a required feature the crate enables by default is satisfied",
        reachability(
            "`crates/gated/tests/needs_baseline.rs::a_guard`",
            "kind(=lib) + (package(=gated) & binary(=needs_baseline))",
        ),
        [],
    )

    # Live tree: the feature parser has to keep finding the real variable, or a
    # Makefile reformat turns the check above into one that reports nothing —
    # and reads green while doing it.
    check(
        "the live Makefile's NEXTEST_SELECTION is still parsed",
        bool(cfp._gate_features((Path(__file__).resolve().parent.parent / "Makefile").read_text(encoding="utf-8"))),
        True,
    )

    check_contains(
        "a guard path that does not exist is reported",
        reachability("`crates/demo/tests/gone.rs::a_guard`", NAMES_THE_BINARY),
        "does not exist",
    )
finally:
    import shutil

    shutil.rmtree(_reach, ignore_errors=True)


if failures:
    print(f"\n{failures} of {checks} checks FAILED")
    raise SystemExit(1)
print(f"\nall {checks} checks passed")

