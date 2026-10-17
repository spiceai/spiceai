#!/usr/bin/env python3
# Copyright 2024-2026 The Spice.ai OSS Authors
#
# Cargo target flags for `make nextest` and `make verify-cli`.
#
# `cargo nextest run --tests` compiles and links every integration-test target
# in the workspace, and the gate's filterset then runs only some of them. The
# rest are built for nothing: dozens of binaries, several of which link all of
# `runtime`. This prints `--lib --bins` plus one `--test <name>` per
# integration-test target the filterset can select, so cargo builds only those.
#
# The filterset stays the single source of truth. Each test target is evaluated
# against it before anything is built, with three-valued logic: a predicate that
# needs the built binary to decide (`test()`, `deps()`, `platform()`, …) is
# "unknown", and an unknown target is built. The result is therefore always a
# superset of what nextest can select, never a subset.
#
# A target whose `required-features` are not all enabled is left out, which is
# what `--tests` does silently (an explicit `--test` for it is a hard cargo
# error). Anything this script cannot decide — a filterset it cannot parse, a
# `dep/feature` requirement, `cargo metadata` failing — prints `--tests` instead,
# so a failure here costs build time, never coverage.
#
# Usage:
#   scripts/nextest_build_targets.py --filter '<filterset>' -- <cargo selection args>
#
# Pure stdlib; no third-party deps.

from __future__ import annotations

import argparse
import fnmatch
import json
import re
import subprocess
import sys
from dataclasses import dataclass, field
from typing import Callable, Optional

FALLBACK = "--tests"

# Kleene three-valued logic: None is "cannot tell before the build".
Tri = Optional[bool]


class FiltersetError(ValueError):
    pass


@dataclass(frozen=True)
class TestTarget:
    package: str
    name: str
    required_features: tuple[str, ...]

    @property
    def binary_id(self) -> str:
        return f"{self.package}::{self.name}"


@dataclass
class Selection:
    """The package and feature flags of a cargo command line."""

    packages: list[str] = field(default_factory=list)
    excludes: list[str] = field(default_factory=list)
    feature_args: list[str] = field(default_factory=list)


def _and(a: Tri, b: Tri) -> Tri:
    if a is False or b is False:
        return False
    if a is True and b is True:
        return True
    return None


def _or(a: Tri, b: Tri) -> Tri:
    if a is True or b is True:
        return True
    if a is False and b is False:
        return False
    return None


def _not(a: Tri) -> Tri:
    return None if a is None else not a


Expr = Callable[[TestTarget], Tri]


def _matcher(arg: str, default: str) -> Callable[[str], bool]:
    if arg.startswith("="):
        value = arg[1:]
        return lambda s: s == value
    if arg.startswith("~"):
        value = arg[1:]
        return lambda s: value in s
    if arg.startswith("#"):
        value = arg[1:]
        return lambda s: fnmatch.fnmatchcase(s, value)
    if len(arg) >= 2 and arg.startswith("/") and arg.endswith("/"):
        pattern = re.compile(arg[1:-1])
        return lambda s: pattern.search(s) is not None
    if default == "glob":
        return lambda s: fnmatch.fnmatchcase(s, arg)
    return lambda s: s == arg


# Predicates nextest can decide from the target alone. `kind` is always `test`
# here: lib, bin and proc-macro targets are all built by `--lib --bins`.
_DECIDABLE: dict[str, tuple[str, Callable[[TestTarget], str]]] = {
    "package": ("glob", lambda t: t.package),
    "binary": ("glob", lambda t: t.name),
    "binary_id": ("glob", lambda t: t.binary_id),
    "kind": ("equal", lambda t: "test"),
}
# Predicates that need the built binary, the dependency graph or the platform.
_UNDECIDABLE = {"test", "deps", "rdeps", "platform", "default"}


class _Parser:
    """Recursive-descent parser for nextest's filterset language.

    Precedence, loosest first: union (`|`, `+`, `or`), then intersection and
    difference (`&`, `and`, `-`), then negation (`not`, `!`).
    """

    def __init__(self, text: str) -> None:
        self.text = text
        self.pos = 0

    def parse(self) -> Expr:
        expr = self._union()
        self._skip_ws()
        if self.pos != len(self.text):
            raise FiltersetError(f"unexpected input at {self.pos}: {self.text[self.pos:]!r}")
        return expr

    def _skip_ws(self) -> None:
        while self.pos < len(self.text) and self.text[self.pos].isspace():
            self.pos += 1

    def _take(self, *tokens: str) -> Optional[str]:
        self._skip_ws()
        for token in tokens:
            if not self.text.startswith(token, self.pos):
                continue
            end = self.pos + len(token)
            # A word operator must not be the prefix of a predicate name.
            if token[0].isalpha() and end < len(self.text) and (self.text[end].isalnum() or self.text[end] == "_"):
                continue
            self.pos = end
            return token
        return None

    def _union(self) -> Expr:
        left = self._intersection()
        while self._take("|", "+", "or"):
            right = self._intersection()
            left = (lambda a, b: lambda t: _or(a(t), b(t)))(left, right)
        return left

    def _intersection(self) -> Expr:
        left = self._negation()
        while True:
            op = self._take("&", "and", "-")
            if op is None:
                return left
            right = self._negation()
            if op == "-":
                left = (lambda a, b: lambda t: _and(a(t), _not(b(t))))(left, right)
            else:
                left = (lambda a, b: lambda t: _and(a(t), b(t)))(left, right)

    def _negation(self) -> Expr:
        if self._take("not", "!"):
            inner = self._negation()
            return lambda t: _not(inner(t))
        return self._atom()

    def _atom(self) -> Expr:
        if self._take("("):
            inner = self._union()
            if not self._take(")"):
                raise FiltersetError(f"missing ')' at {self.pos}")
            return inner
        self._skip_ws()
        m = re.compile(r"[a-z_]+").match(self.text, self.pos)
        if not m:
            raise FiltersetError(f"expected a predicate at {self.pos}: {self.text[self.pos:]!r}")
        name = m.group(0)
        self.pos = m.end()
        if not self._take("("):
            raise FiltersetError(f"expected '(' after {name!r}")
        arg = self._argument()
        if name == "all":
            return lambda t: True
        if name == "none":
            return lambda t: False
        if name in _UNDECIDABLE:
            return lambda t: None
        if name in _DECIDABLE:
            default, field_of = _DECIDABLE[name]
            match = _matcher(arg, default)
            return lambda t: match(field_of(t))
        raise FiltersetError(f"unknown predicate {name!r}")

    def _argument(self) -> str:
        # A regex argument may itself contain ')', so it ends at '/)'.
        start = self.pos
        if self.text.startswith("/", start):
            end = self.text.find("/)", start + 1)
            if end < 0:
                raise FiltersetError("unterminated regex argument")
            self.pos = end + 2
            return self.text[start : end + 1].strip()
        end = self.text.find(")", start)
        if end < 0:
            raise FiltersetError("unterminated predicate argument")
        self.pos = end + 1
        return self.text[start:end].strip()


def parse_filterset(text: str) -> Expr:
    return _Parser(text).parse()


def parse_selection(args: list[str]) -> Selection:
    sel = Selection()
    i = 0
    while i < len(args):
        arg = args[i]
        value = args[i + 1] if i + 1 < len(args) else None
        if arg in ("-p", "--package", "--exclude", "-F", "--features") and value is not None:
            if arg == "--exclude":
                sel.excludes.append(value)
            elif arg in ("-p", "--package"):
                sel.packages.append(value)
            else:
                sel.feature_args += ["--features", value]
            i += 2
            continue
        if arg.startswith("--exclude="):
            sel.excludes.append(arg.split("=", 1)[1])
        elif arg.startswith("--package="):
            sel.packages.append(arg.split("=", 1)[1])
        elif arg.startswith("--features="):
            sel.feature_args += ["--features", arg.split("=", 1)[1]]
        elif arg in ("--all-features", "--no-default-features"):
            sel.feature_args.append(arg)
        i += 1
    return sel


@dataclass(frozen=True)
class ScopedTarget:
    target: TestTarget
    buildable: bool


def scoped_test_targets(metadata: dict, selection: Selection) -> list[ScopedTarget]:
    """Every integration-test target in the selected packages.

    `buildable` is false when cargo skips the target for unmet required
    features. Raises ValueError for a requirement this script cannot evaluate.
    """
    members = set(metadata["workspace_members"])
    enabled = {node["id"]: set(node.get("features", [])) for node in metadata["resolve"]["nodes"]}
    out = []
    for pkg in metadata["packages"]:
        if pkg["id"] not in members or pkg["name"] in selection.excludes:
            continue
        if selection.packages and pkg["name"] not in selection.packages:
            continue
        for target in pkg["targets"]:
            if target["kind"] != ["test"] or not target.get("test", True):
                continue
            required = tuple(target.get("required-features") or ())
            if any("/" in f or f.startswith("dep:") for f in required):
                raise ValueError(f"{pkg['name']}::{target['name']} requires {required}, which this script cannot evaluate")
            buildable = set(required) <= enabled.get(pkg["id"], set())
            out.append(ScopedTarget(TestTarget(pkg["name"], target["name"], required), buildable))
    return out


def target_args(metadata: dict, selection: Selection, filterset: Expr) -> list[str]:
    scoped = scoped_test_targets(metadata, selection)
    names = sorted({s.target.name for s in scoped if s.buildable and filterset(s.target) is not False})
    # `--test` names are not scoped to a package, so a name also matches every
    # same-named target elsewhere. Cargo fails outright on one it cannot build.
    for s in scoped:
        if s.target.name in names and not s.buildable:
            raise ValueError(f"--test {s.target.name} would also name {s.target.binary_id}, whose required features are off")
    args = ["--lib", "--bins"]
    for name in names:
        args += ["--test", name]
    return args


def cargo_metadata(selection: Selection) -> dict:
    host = None
    rustc = subprocess.run(["rustc", "-vV"], capture_output=True, text=True, check=True)
    for line in rustc.stdout.splitlines():
        if line.startswith("host: "):
            host = line.split(": ", 1)[1]
    cmd = ["cargo", "metadata", "--format-version", "1", "--color", "never"]
    if host:
        cmd += ["--filter-platform", host]
    cmd += selection.feature_args
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    return json.loads(result.stdout)


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--filter", required=True, help="the nextest filterset the run uses")
    parser.add_argument("selection", nargs=argparse.REMAINDER, help="cargo package/feature args, after --")
    ns = parser.parse_args(argv)
    selection_args = ns.selection[1:] if ns.selection[:1] == ["--"] else ns.selection
    try:
        filterset = parse_filterset(ns.filter)
        selection = parse_selection(selection_args)
        args = target_args(cargo_metadata(selection), selection, filterset)
    except (FiltersetError, ValueError, KeyError, OSError, subprocess.CalledProcessError, json.JSONDecodeError) as err:
        print(f"warning: building every test target ({FALLBACK}): {err}", file=sys.stderr)
        print(FALLBACK)
        return 0
    print(" ".join(args))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
