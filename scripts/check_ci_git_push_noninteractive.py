#!/usr/bin/env python3
"""Every `git push` in CI must be unable to block on an interactive credential prompt.

A self-hosted runner carries whatever credential helper its host is configured with
(`osxkeychain`, `git-credential-manager`). When a push's URL-embedded token is rejected --
or when git decides to consult a helper for any other reason -- the helper is spawned and
waits for input that never arrives on a headless runner. `git push` then blocks forever.

Nothing bounds that wait. The job runs to GitHub's 360-minute job limit and is killed, so a
step that exists only to *report* a result instead destroys it: the run's own test verdict,
already known hours earlier, is replaced by a timed-out job. Observed daily on `integration
tests (models)`, where `Push snapshots to branch` blocked for 4h15m after the commit it had
just made, with `git-credential-` and `git-remote-http` still alive at job cleanup.

The invariant, applied to every `run:` block in `.github/workflows` and `.github/actions`:

    a step that runs `git push` must invoke it with an empty `credential.helper`,
    and must set `GIT_TERMINAL_PROMPT: '0'` in its `env:`

The two halves cover different escapes. `credential.helper=''` stops a *configured* helper
from being consulted at all; `GIT_TERMINAL_PROMPT=0` stops git's own built-in terminal
prompt, which needs no helper. Either alone still hangs.

There is deliberately no exemption list. A push that "cannot" hang today is one runner
migration away from hanging, and an allowlist entry is where the next one would hide.
"""

import pathlib
import re
import sys

ROOTS = (pathlib.Path(".github/workflows"), pathlib.Path(".github/actions"))

# `git push`, allowing any `-c key=value` / `--flag` between `git` and the subcommand, and
# tolerating a line continuation before it (`\s` already spans the newline it escapes).
# `\bgit\b` so `gh` or `legit` never matches.
GIT_PUSH = re.compile(r"\bgit\b(?P<opts>(?:\s+(?:-c\s+\S+|--?[\w-]+(?:=\S+)?))*)\s*\\?\s*push\b")

# `-c credential.helper=` with an empty value: bare, '' or "". A NON-empty value would name
# a helper to use, which is the opposite of what is wanted, so it must not satisfy this.
EMPTY_HELPER = re.compile(r"-c\s+credential\.helper=(?:''|\"\"|(?=\s|$))")

TERMINAL_PROMPT = re.compile(r"^\s*GIT_TERMINAL_PROMPT\s*:\s*['\"]?0['\"]?\s*$", re.M)


def steps(text):
    """Yield (line_no, step_text) for each top-level list item in a YAML steps: block.

    Parsed textually rather than with a YAML loader on purpose: the loader resolves
    `${{ }}` expressions into plain strings and discards the source line numbers this
    check reports, and a workflow that fails to load would silently check nothing.

    A step starts at a `- ` item whose indentation is the shallowest seen for a list item
    carrying `run:`/`uses:`, and runs until the next item at that same indentation.
    """
    lines = text.splitlines(keepends=True)
    starts = [
        (i, len(m.group(1)))
        for i, ln in enumerate(lines)
        if (m := re.match(r"^(\s*)-\s+\S", ln))
    ]
    if not starts:
        return
    # Step items are the ones at the indentation used by `- name:` / `- uses:` entries.
    depths = [d for i, d in starts if re.match(r"^\s*-\s+(name|uses|run|id|if)\s*:", lines[i])]
    if not depths:
        return
    depth = min(depths)
    bounds = [i for i, d in starts if d == depth]
    for n, start in enumerate(bounds):
        end = bounds[n + 1] if n + 1 < len(bounds) else len(lines)
        yield start + 1, "".join(lines[start:end])


def run_block(step):
    """The shell body of a step's `run:`, with whole-line comments removed.

    Only the shell body may be scanned for `git push`. A step *named* "…git push", or a
    comment explaining why a push is guarded, is prose -- matching it would report a
    violation against a step that runs no push at all.
    """
    lines = step.splitlines(keepends=True)
    for i, ln in enumerate(lines):
        m = re.match(r"^(\s*)run\s*:\s*(.*)$", ln)
        if not m:
            continue
        indent, inline = len(m.group(1)), m.group(2).strip()
        if inline and inline not in ("|", ">", "|-", ">-", "|+", ">+"):
            body = [inline + "\n"]  # `run: git push …` on one line
        else:
            body = []
            for nxt in lines[i + 1:]:
                if nxt.strip() and len(nxt) - len(nxt.lstrip()) <= indent:
                    break
                body.append(nxt)
        return "".join(b for b in body if not b.lstrip().startswith("#"))
    return ""


def violations(path, text):
    """Every reason this file's steps could block on a credential prompt."""
    found = []
    for line_no, step in steps(text):
        body = run_block(step)
        pushes = list(GIT_PUSH.finditer(body))
        if not pushes:
            continue
        where = f"{path}:{line_no}"
        for m in pushes:
            if not EMPTY_HELPER.search(m.group("opts")):
                found.append(
                    f"{where}: `git push` runs without `-c credential.helper=''`, so a "
                    f"credential helper on the runner can intercept it and block on an "
                    f"interactive prompt until the job's 360-minute limit"
                )
        if not TERMINAL_PROMPT.search(step):
            found.append(
                f"{where}: the step runs `git push` but does not set "
                f"`GIT_TERMINAL_PROMPT: '0'` in its `env:`, so git's own terminal prompt "
                f"can still block it"
            )
    return found


def manifests():
    """Every workflow and action manifest, in both spellings GitHub accepts.

    `.yaml` is not a stylistic variant to normalise away -- GitHub runs those manifests
    exactly as it runs `.yml`, and this repository already has three `action.yaml` files. A
    scan that saw only `.yml` would leave them as a silent hole in a guard whose whole design
    is to have no exemptions.
    """
    for root in ROOTS:
        if not root.is_dir():
            continue
        for pattern in ("*.yml", "*.yaml", "*/action.yml", "*/action.yaml"):
            yield from sorted(root.glob(pattern))


def main():
    found, checked = [], 0
    for path in manifests():
        try:
            text = path.read_text(encoding="utf8")
        except OSError as exc:
            print(f"could not read {path}: {exc}", file=sys.stderr)
            return 2
        checked += 1
        found.extend(violations(path.as_posix(), text))

    if not checked:
        print("no workflow or action manifests found -- run from the repository root", file=sys.stderr)
        return 2

    if found:
        print("CI `git push` invocations that can block on a credential prompt:\n", file=sys.stderr)
        for v in found:
            print(f"  {v}", file=sys.stderr)
        print(
            "\nPrefix the push with `git -c credential.helper='' -c credential.interactive=false`"
            "\nand add `GIT_TERMINAL_PROMPT: '0'` to the step's `env:`.",
            file=sys.stderr,
        )
        return 1

    print(f"CI git-push guard OK: {checked} manifest(s) checked, no unbounded credential prompts")
    return 0


if __name__ == "__main__":
    sys.exit(main())
