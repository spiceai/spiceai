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
"""Pin every GitHub Actions `uses:` to a full-length commit SHA, and key the lockfile to match.

Workflows and in-repo actions reference a remote action only by its 40-character
commit SHA, with the release tag in a trailing comment:

    uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1

`gh actions-lock` rewrites every such pin to its release tag
(`actions/checkout@v7.0.1`) and keys its .github/workflows/actions.lock entry
by that tag. The Actions backend looks a `uses:` up in the lockfile by its
exact `owner/repo@ref`, so a SHA `uses:` needs an entry keyed by that SHA:
against a tag-keyed entry, the workflow fails at startup.

Run this script after `gh actions-lock`. It replaces each tag in a `uses:` with
the commit the lockfile locks for it, moves the tag to the comment, and re-keys
the entry by the commit. The entry keeps the tag as its `ref:`, the form
`gh actions-lock` itself writes for an action a composite action pins by SHA.

    gh actions-lock                          # update .github/workflows/actions.lock
    python3 scripts/pin_actions_to_sha.py    # put the SHAs back, in both places

With --check, nothing is written, and the script fails on any remote `uses:`
that is not a full-length SHA, or that has no lockfile entry under exactly
that `owner/repo@sha`. CI runs it in verify-lockfile.yml.

Usage: scripts/pin_actions_to_sha.py [--check] [REPO_ROOT]
"""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass, field
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

# The layout `gh actions-lock` v0.4.1 writes. Anything else fails closed.
LOCK_ENTRY = re.compile(r"^    '(?P<key>[^']+)':(?P<empty> \[\])?$")
LOCK_LIST_ITEM = re.compile(r"^        - '(?P<pin>[^']+)'$")
LOCK_USES_ITEM = re.compile(r"^            - '(?P<pin>[^']+)'$")
LOCK_COMMIT = re.compile(r"^        commit: '(?P<commit>[^']*)'$")
LOCK_FIELD = re.compile(r"^        [a-z_]+:( .*)?$")


class LockfileError(Exception):
    pass


def is_full_sha(ref: str) -> bool:
    return FULL_SHA.fullmatch(ref.lower()) is not None


def pin_key(repo: str, ref: str) -> str:
    """The lockfile key for `repo@ref`: owner/repo folded, a full SHA lowercased."""
    return f"{repo.lower()}@{ref.lower() if is_full_sha(ref) else ref}"


@dataclass
class Lockfile:
    """actions.lock as `gh actions-lock` writes it: per-workflow root pins, and one entry per pin."""

    sections: list[tuple[str, list[str]]]
    workflows: dict[str, list[str]] = field(default_factory=dict)
    dependencies: dict[str, list[str]] = field(default_factory=dict)

    @classmethod
    def parse(cls, text: str) -> Lockfile:
        sections: list[tuple[str, list[str]]] = []
        for number, line in enumerate(text.split("\n"), 1):
            if line == "":
                continue
            if not line.startswith(" "):
                sections.append((line, []))
            elif not sections:
                raise LockfileError(f"{LOCKFILE}:{number}: indented line before any section")
            else:
                sections[-1][1].append(line)
        lock = cls(sections)
        for head, body in sections:
            if head == "workflows:":
                lock.workflows = cls._entries(body, "workflows", lambda line: LOCK_LIST_ITEM.match(line), True)
            elif head == "dependencies:":
                lock.dependencies = cls._entries(body, "dependencies", cls._dependency_line, False)
        return lock

    @staticmethod
    def _dependency_line(line: str) -> bool:
        return bool(LOCK_FIELD.match(line) or LOCK_USES_ITEM.match(line))

    @staticmethod
    def _entries(body: list[str], section: str, valid, lists: bool) -> dict[str, list[str]]:
        """Each entry's lines. A workflow with no remote action is written `'<path>': []`."""
        entries: dict[str, list[str]] = {}
        current: list[str] | None = None
        for line in body:
            if (match := LOCK_ENTRY.match(line)) and (lists or not match["empty"]):
                if match["key"] in entries:
                    raise LockfileError(f"{LOCKFILE}: {section} lists {match['key']} twice")
                current = entries[match["key"]] = []
                if match["empty"]:
                    current = None
            elif current is not None and valid(line):
                current.append(line)
            else:
                raise LockfileError(
                    f"{LOCKFILE}: unexpected line in {section}: {line.strip()!r}; this script reads the layout "
                    "`gh actions-lock` v0.4.1 writes"
                )
        return entries

    def commit(self, key: str) -> str | None:
        """The 40-character commit the entry `key` locks."""
        for line in self.dependencies.get(key, []):
            if match := LOCK_COMMIT.match(line):
                algorithm, _, sha = match["commit"].partition("-")
                if algorithm != "sha1" or not is_full_sha(sha):
                    raise LockfileError(f"{LOCKFILE}: entry {key} locks {match['commit']!r}, not a sha1 commit")
                return sha.lower()
        return None

    def workflow_pins(self, path: str) -> list[str]:
        return [m["pin"] for line in self.workflows.get(path, []) if (m := LOCK_LIST_ITEM.match(line))]

    def referenced_pins(self) -> list[tuple[str, str]]:
        """Each pin a workflow's roots or an entry's `uses:` names, with where it is named."""
        pins = [(f"workflows: {path}", pin) for path in self.workflows for pin in self.workflow_pins(path)]
        for key, body in self.dependencies.items():
            pins += [(f"dependencies: {key} uses", m["pin"]) for line in body if (m := LOCK_USES_ITEM.match(line))]
        return pins

    def rekey_by_commit(self) -> None:
        """Key every tag-keyed entry by the commit it locks, and point every reference at the new key.

        The entry keeps the tag as its `ref:`. Two entries that land on one key lock one commit, so they
        can differ only in the tag they record; the one already keyed by the commit is kept.
        """
        renames = {}
        for key in self.dependencies:
            repo, ref = key.rsplit("@", 1)
            if not is_full_sha(ref):
                sha = self.commit(key)
                if sha is None:
                    raise LockfileError(f"{LOCKFILE}: entry {key} records no commit")
                renames[key] = f"{repo}@{sha}"

        def renamed(body: list[str], pattern: re.Pattern[str], indent: str) -> list[str]:
            """`body` with each run of list items renamed, de-duplicated and sorted as the tool writes them."""
            out: list[str] = []
            run: set[str] = set()
            for line in [*body, ""]:
                if match := pattern.match(line):
                    run.add(renames.get(match["pin"], match["pin"]))
                    continue
                out += [f"{indent}- '{pin}'" for pin in sorted(run)]
                run = set()
                if line:
                    out.append(line)
            return out

        self.workflows = {path: renamed(body, LOCK_LIST_ITEM, " " * 8) for path, body in self.workflows.items()}
        dependencies: dict[str, list[str]] = {}
        for key in sorted(self.dependencies, key=lambda k: k in renames):
            body = renamed(self.dependencies[key], LOCK_USES_ITEM, " " * 12)
            new_key = renames.get(key, key)
            if new_key in dependencies:
                kept = [line for line in dependencies[new_key] if not line.startswith("        ref:")]
                if kept != [line for line in body if not line.startswith("        ref:")]:
                    raise LockfileError(
                        f"{LOCKFILE}: entries {key} and {new_key} lock one commit but record different "
                        "dependencies; run `gh actions-lock --relock` to rebuild them"
                    )
                continue
            dependencies[new_key] = body
        self.dependencies = dependencies

    def render(self) -> str:
        out: list[str] = []
        for head, body in self.sections:
            out.append(head)
            if head == "workflows:":
                entries = self.workflows
            elif head == "dependencies:":
                entries = self.dependencies
            else:
                out.extend(body)
                continue
            for key in sorted(entries):
                out.append(f"    '{key}':" if entries[key] else f"    '{key}': []")
                out.extend(entries[key])
        return "\n".join(out) + "\n"


@dataclass(frozen=True)
class Uses:
    path: Path
    line: int
    action: str
    ref: str

    @property
    def repo(self) -> str:
        """`owner/repo` of `owner/repo[/path]`: the lockfile keys a repository, not a sub-action."""
        return "/".join(self.action.split("/")[:2])

    @property
    def key(self) -> str:
        return pin_key(self.repo, self.ref)


def action_files(root: Path) -> list[Path]:
    workflows = [p for p in (root / WORKFLOWS_DIR).glob("*") if p.suffix in (".yml", ".yaml")]
    actions = [p for p in (root / ACTIONS_DIR).rglob("action.y*ml") if p.suffix in (".yml", ".yaml")]
    return sorted(p for p in workflows + actions if p.is_file())


def remote_uses(path: Path, lines: list[str]) -> list[tuple[int, re.Match[str], Uses]]:
    found = []
    for index, text in enumerate(lines):
        match = USES.match(text)
        if match and not match["action"].startswith(NOT_REMOTE):
            found.append((index, match, Uses(path, index + 1, match["action"], match["ref"])))
    return found


def pinned_line(match: re.Match[str], sha: str) -> str:
    """The `uses:` line pinned to `sha`, with its tag first in the trailing comment."""
    note = match["comment"].strip().removeprefix("#").strip()
    comment = f" # {match['ref']}" + (f" {note}" if note else "")
    return f"{match['lead']}{match['q']}{match['action']}@{sha}{match['q']}{comment}"


def problems(root: Path, lock: Lockfile, files: dict[Path, list[str]]) -> list[str]:
    """Each remote `uses:` that is not a full SHA or that the backend would find no lockfile entry for,
    and each lockfile pin that is not keyed by a full SHA or names no entry."""
    found = []
    for path, lines in files.items():
        relative = path.relative_to(root).as_posix()
        in_workflows = path.parent == root / WORKFLOWS_DIR
        roots = set(lock.workflow_pins(relative))
        for _, _, uses in remote_uses(path, lines):
            where = f"{relative}:{uses.line}: uses: {uses.action}@{uses.ref}"
            if not is_full_sha(uses.ref):
                found.append(f"{where} is not pinned to a full-length commit SHA")
            elif in_workflows and uses.key not in roots:
                found.append(f"{where} has no {LOCKFILE} pin {uses.key} under {relative}")
            elif not in_workflows and uses.key not in lock.dependencies:
                found.append(f"{where} has no {LOCKFILE} entry {uses.key}")
    for key in lock.dependencies:
        if not is_full_sha(key.rsplit("@", 1)[1]):
            found.append(f"{LOCKFILE}: dependencies: {key} is keyed by a tag, not the commit it locks")
    for where, pin in lock.referenced_pins():
        if pin not in lock.dependencies:
            found.append(f"{LOCKFILE}: {where} {pin}, which has no entry")
    return found


def read_all(root: Path) -> tuple[Lockfile, str, dict[Path, list[str]]]:
    text = (root / LOCKFILE).read_text()
    return Lockfile.parse(text), text, {path: path.read_text().split("\n") for path in action_files(root)}


def check(root: Path) -> int:
    lock, _, files = read_all(root)
    found = problems(root, lock, files)
    for problem in found:
        print(problem)
    if found:
        print(
            f"{len(found)} problems: every remote `uses:` must be pinned to a full-length commit SHA, with a "
            "lockfile entry keyed by that SHA. Fix them with `gh actions-lock` followed by "
            "`python3 scripts/pin_actions_to_sha.py`.",
            file=sys.stderr,
        )
        return 1
    print(f"Every remote `uses:` is pinned to a full-length commit SHA, and {LOCKFILE} is keyed by it.")
    return 0


def pin(root: Path) -> int:
    lock, lock_text, files = read_all(root)
    missing = []
    count = 0
    for path, lines in files.items():
        for index, match, uses in remote_uses(path, lines):
            if is_full_sha(uses.ref):
                continue
            sha = lock.commit(uses.key)
            if sha is None:
                missing.append(f"{path.relative_to(root)}:{uses.line}: {LOCKFILE} has no entry for {uses.key}")
                continue
            lines[index] = pinned_line(match, sha)
            count += 1
    if missing:
        for line in missing:
            print(line, file=sys.stderr)
        print(
            f"{len(missing)} `uses:` refs have no lockfile entry, so nothing was rewritten. "
            "Run `gh actions-lock` first, then run this script again.",
            file=sys.stderr,
        )
        return 1
    lock.rekey_by_commit()
    if found := problems(root, lock, files):
        for problem in found:
            print(problem, file=sys.stderr)
        print(
            f"{len(found)} problems remain after pinning, so nothing was rewritten. "
            "Run `gh actions-lock` first, then run this script again.",
            file=sys.stderr,
        )
        return 1
    changed = [path for path, lines in files.items() if "\n".join(lines) != path.read_text()]
    for path in changed:
        path.write_text("\n".join(files[path]))
    rendered = lock.render()
    rekeyed = rendered != lock_text
    if rekeyed:
        (root / LOCKFILE).write_text(rendered)
    if count or rekeyed:
        print(f"Pinned {count} `uses:` refs in {len(changed)} files, and keyed {LOCKFILE} by commit.")
    else:
        print("Every remote `uses:` is already pinned to a full-length commit SHA, and the lockfile is keyed by it.")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    parser.add_argument("--check", action="store_true", help="write nothing; fail on a ref the lockfile can't serve")
    parser.add_argument("root", nargs="?", type=Path, default=Path(__file__).resolve().parent.parent)
    args = parser.parse_args(argv)
    try:
        return check(args.root) if args.check else pin(args.root)
    except LockfileError as error:
        print(error, file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
