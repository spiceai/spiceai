#!/usr/bin/env python3
# Copyright 2024-2026 The Spice.ai OSS Authors
#
# Unreachable-module guard.
#
# A file under a crate's `src/` tree that no `mod` declaration reaches is
# invisible to `cargo build`, `cargo clippy`, `cargo fmt --all` and `cargo test`
# alike: it looks like source, it gets edited like source, and it compiles
# nothing. Three such files had accumulated in this workspace, together holding
# 808 lines and 26 tests that had never run (#12735, #12737).
#
# This walks every workspace crate's module tree from its own target roots and
# reports any `.rs` file under `src/` the walk never reaches.
#
# Only `src/` is in scope. Cargo auto-discovers `benches/`, `tests/` and
# `examples/` files as their own targets, so they are reachable with no `mod`
# declaration anywhere and including them reports ~200 files that are all fine.
#
# Usage:
#   scripts/check_module_reachability.py          # validate (exit 1 on violation)
#   scripts/check_module_reachability.py --list   # print every file and its status
#
# Pure stdlib; no third-party deps.

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent

# `mod name;` / `mod name { … }`, with the leading `pub`, `pub(crate)`, `unsafe`
# and `async` qualifiers Rust allows in front of it. Matched against a source
# text whose comments and string literals have already been blanked, so a `mod`
# inside a comment or a string cannot reach here.
MOD_RE = re.compile(
    r"\bmod\s+(?P<name>[A-Za-z_][A-Za-z0-9_]*)\s*(?P<kind>[;{])",
)

# `#[path = "…"]`, including the `#[cfg_attr(…, path = "…")]` spelling. The
# value is read from the blanked text's companion literal table, not from here.
PATH_ATTR_RE = re.compile(r"\bpath\s*=\s*(?P<lit>\x00L(?P<idx>\d+)\x00)")



def run_cargo_metadata() -> dict:
    """Workspace manifest as cargo sees it — the authority on targets and paths."""
    try:
        out = subprocess.run(
            ["cargo", "metadata", "--no-deps", "--format-version", "1"],
            cwd=REPO,
            capture_output=True,
            text=True,
            check=True,
        )
    except FileNotFoundError:
        # Exit 2 (tooling error), never 1 — 1 means an actual unreachable file.
        print(
            "error: `cargo` not found on PATH, so the workspace layout cannot be read.",
            file=sys.stderr,
        )
        raise SystemExit(2)
    except subprocess.CalledProcessError as e:
        print(f"error: `cargo metadata` failed:\n{e.stderr}", file=sys.stderr)
        raise SystemExit(2)
    try:
        return json.loads(out.stdout)
    except json.JSONDecodeError as e:
        print(f"error: `cargo metadata` emitted invalid JSON: {e}", file=sys.stderr)
        raise SystemExit(2)


# Openers for everything that can hide a `mod` token. Ordinary code is skipped by
# the regex engine between matches, which is what keeps this fast enough to sit in
# `make lint-rust`: a per-character Python loop over this workspace takes minutes.
TOKEN_RE = re.compile(
    r"""
      (?P<line>//)
    | (?P<block>/\*)
    | (?P<raw>(?<![A-Za-z0-9_])(?:b|c)?r(?P<hashes>\#*)")
    | (?P<string>(?<![A-Za-z0-9_])(?:b|c)?")
    | (?P<char>')
    """,
    re.VERBOSE,
)


def blank_source(text: str) -> tuple[str, list[str]]:
    """Blank comments and string literals, keeping offsets stable.

    Returns the blanked text plus the literal values, in order. Each literal is
    replaced in-place by a `\\x00L<index>\\x00` marker padded to its original
    width, so `#[path = "…"]` can still be resolved while a `mod` inside a
    comment or a string can never be mistaken for a declaration.
    """
    out: list[str] = []
    literals: list[str] = []
    pos, n = 0, len(text)

    while pos < n:
        m = TOKEN_RE.search(text, pos)
        if m is None:
            out.append(text[pos:])
            break

        out.append(text[pos : m.start()])
        i = m.start()

        if m.group("line"):
            j = text.find("\n", i)
            j = n if j == -1 else j
            out.append(" " * (j - i))

        elif m.group("block"):
            # Block comments nest in Rust, so match them by depth.
            depth, j = 1, i + 2
            while j < n and depth:
                if text.startswith("/*", j):
                    depth += 1
                    j += 2
                elif text.startswith("*/", j):
                    depth -= 1
                    j += 2
                else:
                    j += 1
            # Keep newlines so line numbers in diagnostics stay honest.
            out.append(re.sub(r"[^\n]", " ", text[i:j]))

        elif m.group("raw"):
            close = '"' + m.group("hashes")
            k = text.find(close, m.end())
            j = n if k == -1 else k + len(close)
            literals.append(text[m.end() : j - len(close)] if k != -1 else "")
            out.append(_marker(len(literals) - 1, j - i))

        elif m.group("string"):
            j, value = m.end(), []
            while j < n:
                if text[j] == "\\" and j + 1 < n:
                    value.append(text[j : j + 2])
                    j += 2
                    continue
                if text[j] == '"':
                    j += 1
                    break
                value.append(text[j])
                j += 1
            literals.append("".join(value))
            out.append(_marker(len(literals) - 1, j - i))

        else:
            # `'` is a char literal (`'a'`, `'\n'`, `'\u{1F}'`) or a lifetime
            # (`'a`, `'static`). Neither can be a path, so both are simply
            # blanked; only a closing quote on the same line ends a char.
            j = i + 1
            if j < n and text[j] == "\\":
                j += 1
                while j < n and text[j] not in "'\n":
                    j += 1
                j = min(j + 1, n)
            elif j + 1 < n and text[j + 1] == "'":
                j += 2
            else:
                while j < n and (text[j].isalnum() or text[j] == "_"):
                    j += 1
            out.append(" " * (j - i))

        pos = max(j, i + 1)

    return "".join(out), literals


def _marker(index: int, width: int) -> str:
    """A literal placeholder padded to `width`, so offsets never shift."""
    marker = f"\x00L{index}\x00"
    if len(marker) <= width:
        return marker + " " * (width - len(marker))
    # Pathologically short literal (`""` is 2 chars, the marker is 4). Offsets
    # after it shift by a couple of columns; nothing here depends on absolute
    # position, only on relative order, so this is safe.
    return marker


def parse_mods(path: Path) -> list[tuple[str, str, str | None, tuple[str, ...]]]:
    """Every `mod` declaration in `path`, in source order.

    Each entry is `(name, kind, path_override, inline_parents)`, where `kind` is
    `;` for a file module or `{` for an inline one, and `inline_parents` names the
    inline `mod` blocks the declaration sits inside — which is what decides the
    directory it resolves against.

    `#[cfg(...)]` is never evaluated: a module declared only under a non-default
    feature is still declared, so gating it must not make its file look dead.
    """
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError as e:
        print(f"error: cannot read {path}: {e}", file=sys.stderr)
        raise SystemExit(2)

    blanked, literals = blank_source(text)

    # Merge `mod` matches with brace events so inline nesting can be tracked in
    # one ordered pass; a stack that only pushed would mis-attribute every
    # declaration after the first inline module.
    events: list[tuple[int, int, re.Match[str] | str]] = []
    for m in MOD_RE.finditer(blanked):
        events.append((m.start(), 0, m))
    for b in re.finditer(r"[{}]", blanked):
        events.append((b.start(), 1, b.group()))
    events.sort(key=lambda e: (e[0], e[1]))

    mods: list[tuple[str, str, str | None, tuple[str, ...]]] = []
    stack: list[tuple[int, str]] = []
    depth = 0
    pending: str | None = None

    for _, _, item in events:
        if isinstance(item, str):
            if item == "{":
                depth += 1
                if pending is not None:
                    stack.append((depth, pending))
                    pending = None
            else:
                if stack and stack[-1][0] == depth:
                    stack.pop()
                depth -= 1
            continue

        if item.group("kind") == "{":
            pending = item.group("name")
            continue

        # The nearest `path = "…"` in the attributes immediately preceding this
        # declaration. Bounded by the previous `;`/`{`/`}` so an attribute
        # belonging to an earlier item cannot be picked up by this one.
        start = max(
            blanked.rfind(";", 0, item.start()),
            blanked.rfind("}", 0, item.start()),
            blanked.rfind("{", 0, item.start()),
        )
        override = None
        for pm in PATH_ATTR_RE.finditer(blanked[start + 1 : item.start()]):
            idx = int(pm.group("idx"))
            if idx < len(literals):
                override = literals[idx]
        mods.append(
            (item.group("name"), ";", override, tuple(n for _, n in stack))
        )

    return mods


def resolve_child(mod_dir: Path, name: str, override: str | None) -> Path | None:
    """The file a `mod name;` declaration inside `mod_dir` refers to."""
    if override is not None:
        return (mod_dir / override).resolve()
    flat = mod_dir / f"{name}.rs"
    if flat.is_file():
        return flat.resolve()
    nested = mod_dir / name / "mod.rs"
    if nested.is_file():
        return nested.resolve()
    return None


# A file that owns the directory it sits in, rather than a sibling directory
# named after itself. The Reference calls these "mod-rs" files.
MOD_RS_NAMES = {"mod.rs", "lib.rs", "main.rs"}


def walk_from_root(root: Path, reached: set[Path]) -> None:
    """Mark `root` and everything its module tree reaches.

    Two different directories are in play, and conflating them is what makes a
    naive version report live files:

    * `module_dir` — where an ordinary `mod name;` looks for `name.rs` or
      `name/mod.rs`. A mod-rs file owns its own directory; any other file owns
      the sibling directory named after it.
    * the *declaring file's own directory* — what a `#[path = "…"]` on a
      declaration outside any inline block resolves against, whether or not the
      file is mod-rs. `tools/spidapter/src/stdio_server.rs` reaches
      `src/sources/mod.rs` this way, not `src/stdio_server/sources/mod.rs`.

    Inside an inline `mod a { … }`, both forms descend through the inline names.
    """
    stack = [root.resolve()]

    while stack:
        current = stack.pop()
        if current in reached or not current.is_file():
            continue
        reached.add(current)

        file_dir = current.parent
        module_dir = file_dir if current.name in MOD_RS_NAMES else file_dir / current.stem

        for name, _, override, inline in parse_mods(current):
            if override is not None:
                base = module_dir.joinpath(*inline) if inline else file_dir
            else:
                base = module_dir.joinpath(*inline)
            child = resolve_child(base, name, override)
            if child is not None:
                stack.append(child)


def collect_sources(src_dir: Path) -> set[Path]:
    return {p.resolve() for p in src_dir.rglob("*.rs") if p.is_file()}


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Fail when a file under a workspace crate's src/ is unreachable "
        "from that crate's roots."
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="print every src/ file and whether it is reachable",
    )
    args = parser.parse_args()

    metadata = run_cargo_metadata()
    workspace_ids = set(metadata.get("workspace_members", []))

    violations: list[tuple[str, Path]] = []
    listed: list[tuple[str, Path, bool]] = []

    for package in metadata.get("packages", []):
        if package.get("id") not in workspace_ids:
            continue
        name = package["name"]
        crate_dir = Path(package["manifest_path"]).parent
        src_dir = crate_dir / "src"
        if not src_dir.is_dir():
            continue

        # Roots come from cargo's own target list, so a `path = "…"` override in
        # Cargo.toml is honoured without this script parsing the manifest.
        reached: set[Path] = set()
        for target in package.get("targets", []):
            src_path = Path(target["src_path"]).resolve()
            try:
                src_path.relative_to(src_dir.resolve())
            except ValueError:
                # A bench/test/example target outside src/ — not in scope, and
                # its own module tree cannot make an src/ file reachable that
                # the library does not already reach.
                continue
            walk_from_root(src_path, reached)

        # No name is exempt: every genuine root — `lib.rs`, `main.rs`, each
        # `src/bin/*.rs` — is already seeded above from cargo's own target list,
        # and exempting `mod.rs` by name would hide a whole dead directory.
        for source in sorted(collect_sources(src_dir)):
            ok = source in reached
            listed.append((name, source, ok))
            if not ok:
                violations.append((name, source))

    if args.list:
        for crate, source, ok in listed:
            mark = "ok  " if ok else "DEAD"
            print(f"{mark} {crate}: {source.relative_to(REPO)}")

    if violations:
        print(
            f"error: {len(violations)} source file(s) under src/ are not reachable "
            "from their crate's root, so nothing compiles them:\n",
            file=sys.stderr,
        )
        for crate, source in violations:
            print(f"  {crate}: {source.relative_to(REPO)}", file=sys.stderr)
        print(
            "\nEither declare each one with a `mod` declaration from its parent "
            "module, or delete it if the live code moved elsewhere.",
            file=sys.stderr,
        )
        return 1

    if not args.list:
        print(f"module reachability: {len(listed)} src/ file(s) all reachable")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
