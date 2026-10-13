#!/usr/bin/env python3
# Copyright 2024-2026 The Spice.ai OSS Authors
#
# Tests for scripts/check_module_reachability.py.
#
# Each case here is one of the ways a first attempt at this check gets it wrong
# (#12737): a `mod` hidden in a comment or a string, a `#[path]` redirect, a
# declaration behind `#[cfg]`, an inline module, or a file cargo reaches through
# a target root rather than a `mod` declaration.
#
# Run: python3 scripts/test_check_module_reachability.py

from __future__ import annotations

import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from check_module_reachability import (  # noqa: E402
    blank_source,
    package_sources,
    parse_mods,
    resolve_child,
    walk_from_root,
)

failures = 0
checks = 0


def check(name: str, got, want) -> None:
    global failures, checks
    checks += 1
    if got == want:
        print(f"  ok: {name}")
    else:
        failures += 1
        print(f"  FAIL: {name}\n    got:  {got!r}\n    want: {want!r}")


def tmp_src(d: str) -> Path:
    """The `src` root under temp directory `d`, resolved.

    `walk_from_root` records resolved paths, and on macOS a temp directory sits
    behind the `/var` -> `/private/var` symlink, so measuring those paths against
    an unresolved root makes `relative_to` raise instead of compare.
    """
    return (Path(d) / "src").resolve()


def mods_of(source: str) -> list[tuple[str, str, tuple[str, ...], tuple[str, ...]]]:
    """Every file-module declaration in `source`, as parse_mods returns them."""
    with tempfile.TemporaryDirectory() as d:
        f = Path(d) / "lib.rs"
        f.write_text(source, encoding="utf-8")
        return parse_mods(f)


print("blank_source")

# A string preceded by whitespace is the common case; an earlier version used a
# word-boundary anchor here, which does not match between a space and a quote.
blanked, literals = blank_source('let p = "value";')
check("an ordinary string is blanked", "mod" in blanked, False)
check("an ordinary string is captured", literals, ["value"])

blanked, _ = blank_source('let s = "mod fake;";')
check("a mod inside a string is not a declaration", mods_of('let s = "mod fake;";'), [])

check("a mod inside a line comment is ignored", mods_of("// mod fake;\n"), [])
check("a mod inside a block comment is ignored", mods_of("/* mod fake; */"), [])
check(
    "a mod inside a nested block comment is ignored",
    mods_of("/* outer /* mod fake; */ still */"),
    [],
)
check("a mod inside a doc comment is ignored", mods_of("/// mod fake;\n"), [])
check(
    "a mod inside a raw string is ignored",
    mods_of('const S: &str = r#"mod fake;"#;'),
    [],
)
check(
    "a lifetime does not swallow the rest of the file",
    mods_of("struct S<'a>(&'a str);\nmod real;\n"),
    [("real", ";", (), ())],
)
check(
    "a char literal does not swallow the rest of the file",
    mods_of("const C: char = '\\'';\nmod real;\n"),
    [("real", ";", (), ())],
)

print()
print("parse_mods")

check("a plain declaration", mods_of("mod alpha;"), [("alpha", ";", (), ())])
check("a pub declaration", mods_of("pub mod alpha;"), [("alpha", ";", (), ())])
check(
    "a pub(crate) declaration",
    mods_of("pub(crate) mod alpha;"),
    [("alpha", ";", (), ())],
)
check("an inline module declares no file of its own", mods_of("mod alpha { }"), [])
check(
    "a cfg-gated declaration is still a declaration",
    mods_of('#[cfg(feature = "x")]\nmod alpha;'),
    [("alpha", ";", (), ())],
)
check(
    "a path attribute is captured",
    mods_of('#[path = "shared/helper.rs"]\nmod alpha;'),
    [("alpha", ";", ("shared/helper.rs",), ())],
)
check(
    "a path attribute does not leak to the next declaration",
    mods_of('#[path = "shared/helper.rs"]\nmod alpha;\nmod beta;'),
    [("alpha", ";", ("shared/helper.rs",), ()), ("beta", ";", (), ())],
)
check(
    "a cfg attribute alongside a path attribute",
    mods_of('#[cfg(test)]\n#[path = "t.rs"]\nmod alpha;'),
    [("alpha", ";", ("t.rs",), ())],
)

# A module routed per platform carries one `cfg_attr` per configuration. `cfg`
# is never evaluated, so every candidate compiles under some build — keeping
# only the last would report the other platforms' files as dead and fail the
# lint gate on live source.
check(
    "every cfg_attr path candidate is kept",
    mods_of(
        '#[cfg_attr(unix, path = "unix.rs")]\n'
        '#[cfg_attr(windows, path = "windows.rs")]\n'
        "mod platform;"
    ),
    [("platform", ";", ("unix.rs", "windows.rs"), ())],
)
check(
    "repeated identical path candidates collapse",
    mods_of(
        '#[cfg_attr(unix, path = "shared.rs")]\n'
        '#[cfg_attr(windows, path = "shared.rs")]\n'
        "mod platform;"
    ),
    [("platform", ";", ("shared.rs",), ())],
)
check(
    "multiple candidates do not leak to the next declaration",
    mods_of(
        '#[cfg_attr(unix, path = "unix.rs")]\n'
        '#[cfg_attr(windows, path = "windows.rs")]\n'
        "mod platform;\nmod beta;"
    ),
    [("platform", ";", ("unix.rs", "windows.rs"), ()), ("beta", ";", (), ())],
)

# Inline nesting decides which directory a declaration resolves against, so the
# stack has to pop as well as push: without the pop, `gamma` below would be
# attributed to `alpha` and looked for in the wrong directory.
check(
    "a declaration inside an inline module records its parent",
    mods_of("mod alpha { mod beta; }"),
    [("beta", ";", (), ("alpha",))],
)
check(
    "the inline stack pops at the closing brace",
    mods_of("mod alpha { mod beta; }\nmod gamma;"),
    [("beta", ";", (), ("alpha",)), ("gamma", ";", (), ())],
)
check(
    "a nested inline module records the whole chain",
    mods_of("mod alpha { mod beta { mod delta; } }"),
    [("delta", ";", (), ("alpha", "beta"))],
)
check(
    "a function body's braces do not disturb the stack",
    mods_of("fn f() { if true { } }\nmod alpha;"),
    [("alpha", ";", (), ())],
)

print()
print("resolve_child / walk_from_root")

with tempfile.TemporaryDirectory() as d:
    src = tmp_src(d)
    (src / "nested").mkdir(parents=True)
    (src / "shared").mkdir(parents=True)

    (src / "lib.rs").write_text(
        '#[path = "shared/redirected.rs"]\n'
        "mod redirected;\n"
        "mod flat;\n"
        "mod nested;\n"
        '#[cfg(feature = "off-by-default")]\n'
        "mod gated;\n",
        encoding="utf-8",
    )
    (src / "flat.rs").write_text("", encoding="utf-8")
    (src / "gated.rs").write_text("", encoding="utf-8")
    (src / "shared" / "redirected.rs").write_text("", encoding="utf-8")
    (src / "nested" / "mod.rs").write_text("mod leaf;\n", encoding="utf-8")
    (src / "nested" / "leaf.rs").write_text("", encoding="utf-8")
    (src / "orphan.rs").write_text("fn dead() {}\n", encoding="utf-8")

    reached: set[Path] = set()
    walk_from_root(src / "lib.rs", reached)
    names = sorted(p.relative_to(src).as_posix() for p in reached)

    check(
        "the walk reaches every declared file and no orphan",
        names,
        [
            "flat.rs",
            "gated.rs",
            "lib.rs",
            "nested/leaf.rs",
            "nested/mod.rs",
            "shared/redirected.rs",
        ],
    )
    check(
        "an undeclared file is not reached",
        (src / "orphan.rs").resolve() in reached,
        False,
    )
    check(
        "a path redirect resolves relative to the declaring file's directory",
        resolve_child(src, "redirected", "shared/redirected.rs"),
        (src / "shared" / "redirected.rs").resolve(),
    )

# A `#[path]` on a declaration outside any inline block resolves against the
# directory of the file that declares it, even when that file is not a mod-rs
# file and so owns a differently-named module directory. Resolving it against
# the module directory instead reports `tools/spidapter`'s live sources as dead.
with tempfile.TemporaryDirectory() as d:
    src = tmp_src(d)
    (src / "sources").mkdir(parents=True)
    (src / "main.rs").write_text("mod server;\n", encoding="utf-8")
    (src / "server.rs").write_text(
        '#[path = "sources/mod.rs"]\nmod sources;\n', encoding="utf-8"
    )
    (src / "sources" / "mod.rs").write_text("mod backend;\n", encoding="utf-8")
    (src / "sources" / "backend.rs").write_text("", encoding="utf-8")

    reached = set()
    walk_from_root(src / "main.rs", reached)
    check(
        "a non-mod-rs file's #[path] resolves beside the file, not under its module dir",
        sorted(p.relative_to(src).as_posix() for p in reached),
        ["main.rs", "server.rs", "sources/backend.rs", "sources/mod.rs"],
    )

# The end-to-end shape of the multi-candidate case: a platform-routed module
# reaches a different file under each configuration, and every one of them is
# live source. Following only the last candidate fails the gate on the rest.
with tempfile.TemporaryDirectory() as d:
    src = tmp_src(d)
    (src / "platform" / "unix").mkdir(parents=True)
    (src / "lib.rs").write_text(
        '#[cfg_attr(unix, path = "platform/unix.rs")]\n'
        '#[cfg_attr(windows, path = "platform/windows.rs")]\n'
        "mod platform;\n",
        encoding="utf-8",
    )
    # `platform/unix.rs` is not a mod-rs file, so its own children live in the
    # sibling directory named after it — the walk has to keep descending from
    # each candidate, not merely mark the candidate itself.
    (src / "platform" / "unix.rs").write_text("mod unix_leaf;\n", encoding="utf-8")
    (src / "platform" / "unix" / "unix_leaf.rs").write_text("", encoding="utf-8")
    (src / "platform" / "windows.rs").write_text("", encoding="utf-8")

    reached = set()
    walk_from_root(src / "lib.rs", reached)
    check(
        "every platform's file is reached, and so is what it declares",
        sorted(p.relative_to(src).as_posix() for p in reached),
        [
            "lib.rs",
            "platform/unix.rs",
            "platform/unix/unix_leaf.rs",
            "platform/windows.rs",
        ],
    )

print()
print("package_sources (tests/, benches/, examples/)")


def fake_package(crate: Path, *src_paths: str) -> dict:
    """A `cargo metadata` package whose targets are exactly `src_paths`."""
    (crate / "Cargo.toml").write_text("", encoding="utf-8")
    return {
        "name": "fake",
        "manifest_path": str(crate / "Cargo.toml"),
        "targets": [{"src_path": str(crate / p)} for p in src_paths],
    }


def write_tree(crate: Path, files: dict[str, str]) -> None:
    for rel, text in files.items():
        path = crate / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")


def unreached(crate: Path, package: dict) -> list[str]:
    return [p.relative_to(crate).as_posix() for p, ok in package_sources(package) if not ok]


# The shape every crate with an integration binary has: `autotests = false`, one
# declared `tests/integration.rs` root, and one module per test file. A crate
# root owns its own directory whatever it is named, so `mod common;` in
# `tests/integration.rs` is `tests/common/mod.rs`. Resolving it under
# `tests/integration/` instead reports every test file in the crate as dead.
with tempfile.TemporaryDirectory() as d:
    crate = Path(d).resolve()
    write_tree(crate, {
        "src/lib.rs": "",
        "tests/integration.rs": "mod common;\nmod alpha;\nmod suite;\n",
        "tests/common/mod.rs": "",
        "tests/alpha.rs": "",
        "tests/suite/mod.rs": "mod case_a;\n",
        "tests/suite/case_a.rs": "",
    })
    check(
        "a test target root owns its directory, so its modules are reached",
        unreached(crate, fake_package(crate, "src/lib.rs", "tests/integration.rs")),
        [],
    )

# What the per-crate check missed (#15005 review): with `autotests = false` a
# new top-level file is not a target, and a file inside a test module's
# directory is never one. Neither is compiled unless a `mod` reaches it.
with tempfile.TemporaryDirectory() as d:
    crate = Path(d).resolve()
    write_tree(crate, {
        "src/lib.rs": "",
        "tests/integration.rs": "mod suite;\n",
        "tests/suite/mod.rs": "mod case_a;\n",
        "tests/suite/case_a.rs": "",
        "tests/suite/case_b.rs": "#[test]\nfn never_runs() {}\n",
        "tests/undeclared.rs": "#[test]\nfn never_runs() {}\n",
        "benches/utils/mod.rs": "",
        "examples/demo.rs": "",
        "fixtures/not_source.rs": "",
    })
    check(
        "an undeclared top-level test, a nested test, a bench helper and an example are reported",
        unreached(crate, fake_package(crate, "src/lib.rs", "tests/integration.rs")),
        ["benches/utils/mod.rs", "examples/demo.rs", "tests/suite/case_b.rs", "tests/undeclared.rs"],
    )

# Auto-discovered targets are in cargo's target list too, so a crate that keeps
# one binary per test file reports nothing.
with tempfile.TemporaryDirectory() as d:
    crate = Path(d).resolve()
    write_tree(crate, {
        "src/lib.rs": "",
        "tests/one.rs": "mod common;\n",
        "tests/two.rs": "",
        "tests/common/mod.rs": "",
        "benches/speed.rs": "",
        "examples/demo.rs": "",
    })
    check(
        "auto-discovered tests, benches and examples are reached from their own roots",
        unreached(crate, fake_package(
            crate, "src/lib.rs", "tests/one.rs", "tests/two.rs", "benches/speed.rs", "examples/demo.rs",
        )),
        [],
    )

# `src/bin/tool.rs` is a crate root too: its `mod helper;` is `src/bin/helper.rs`.
with tempfile.TemporaryDirectory() as d:
    crate = Path(d).resolve()
    write_tree(crate, {
        "src/bin/tool.rs": "mod helper;\n",
        "src/bin/helper.rs": "",
    })
    check(
        "a binary root's modules resolve beside it",
        unreached(crate, fake_package(crate, "src/bin/tool.rs")),
        [],
    )

print()
if failures:
    print(f"{failures} of {checks} checks failed")
    raise SystemExit(1)
print(f"all {checks} checks passed")
