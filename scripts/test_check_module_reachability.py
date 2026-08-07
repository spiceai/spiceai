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


def mods_of(source: str) -> list[tuple[str, str, str | None, tuple[str, ...]]]:
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
    [("real", ";", None, ())],
)
check(
    "a char literal does not swallow the rest of the file",
    mods_of("const C: char = '\\'';\nmod real;\n"),
    [("real", ";", None, ())],
)

print()
print("parse_mods")

check("a plain declaration", mods_of("mod alpha;"), [("alpha", ";", None, ())])
check("a pub declaration", mods_of("pub mod alpha;"), [("alpha", ";", None, ())])
check(
    "a pub(crate) declaration",
    mods_of("pub(crate) mod alpha;"),
    [("alpha", ";", None, ())],
)
check("an inline module declares no file of its own", mods_of("mod alpha { }"), [])
check(
    "a cfg-gated declaration is still a declaration",
    mods_of('#[cfg(feature = "x")]\nmod alpha;'),
    [("alpha", ";", None, ())],
)
check(
    "a path attribute is captured",
    mods_of('#[path = "shared/helper.rs"]\nmod alpha;'),
    [("alpha", ";", "shared/helper.rs", ())],
)
check(
    "a path attribute does not leak to the next declaration",
    mods_of('#[path = "shared/helper.rs"]\nmod alpha;\nmod beta;'),
    [("alpha", ";", "shared/helper.rs", ()), ("beta", ";", None, ())],
)
check(
    "a cfg attribute alongside a path attribute",
    mods_of('#[cfg(test)]\n#[path = "t.rs"]\nmod alpha;'),
    [("alpha", ";", "t.rs", ())],
)

# Inline nesting decides which directory a declaration resolves against, so the
# stack has to pop as well as push: without the pop, `gamma` below would be
# attributed to `alpha` and looked for in the wrong directory.
check(
    "a declaration inside an inline module records its parent",
    mods_of("mod alpha { mod beta; }"),
    [("beta", ";", None, ("alpha",))],
)
check(
    "the inline stack pops at the closing brace",
    mods_of("mod alpha { mod beta; }\nmod gamma;"),
    [("beta", ";", None, ("alpha",)), ("gamma", ";", None, ())],
)
check(
    "a nested inline module records the whole chain",
    mods_of("mod alpha { mod beta { mod delta; } }"),
    [("delta", ";", None, ("alpha", "beta"))],
)
check(
    "a function body's braces do not disturb the stack",
    mods_of("fn f() { if true { } }\nmod alpha;"),
    [("alpha", ";", None, ())],
)

print()
print("resolve_child / walk_from_root")

with tempfile.TemporaryDirectory() as d:
    src = Path(d) / "src"
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
    src = Path(d) / "src"
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

print()
if failures:
    print(f"{failures} of {checks} checks failed")
    raise SystemExit(1)
print(f"all {checks} checks passed")
