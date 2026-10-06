#!/usr/bin/env python3
"""A call site that configures the compiler cache must also supply its credentials.

`setup-sccache` writes a cache config that signs its requests on every path except macOS
via `spiceio_endpoint`, and it probes the cache before wrapping rustc. A call site that
names an endpoint but no credentials therefore gets a probe that cannot sign, and the
pre-flight disables the cache for the whole job -- surfacing as nothing louder than a
`::warning::` while every build in that job compiles uncached. Three jobs ran that way
unnoticed, and four composite actions were dropping credentials their own callers passed.

The invariant, applied to `setup-sccache` and to any local action that declares the same
credential inputs (so a forwarding action is held to it too):

    a call site passing `minio_endpoint` must also pass `minio_access_key`
    and `minio_secret_key`

A call site that names no endpoint configures no cache and is left alone -- credentials
without an endpoint would do nothing.
"""

import pathlib
import re
import sys

ROOTS = (pathlib.Path(".github/workflows"), pathlib.Path(".github/actions"))
CREDS = ("minio_access_key", "minio_secret_key")


def forwards_to_setup_sccache(action_yml):
    """True if this action calls `setup-sccache` and takes the credential inputs to forward.

    Deliberately not "declares the inputs": `setup-minio` and `upload-to-minio` take the
    same key names for the `mc` client and have nothing to do with the compiler cache.
    """
    try:
        text = action_yml.read_text(encoding="utf8")
    except OSError:
        return False
    if "./.github/actions/setup-sccache" not in text:
        return False
    return all(re.search(rf"^  {c}\s*:", text, re.M) for c in CREDS)


def guarded_actions():
    names = {"setup-sccache"}
    for action_yml in pathlib.Path(".github/actions").glob("*/action.yml"):
        if forwards_to_setup_sccache(action_yml):
            names.add(action_yml.parent.name)
    return names


def call_sites(text, names):
    """Yield (line_no, action_name, step_body) for each call of a guarded action.

    `uses:` and `with:` are sibling keys of the same step, so collect everything at or
    deeper than the `uses:` indent and stop at the dedent that ends the step.
    """
    lines = text.split("\n")
    for i, line in enumerate(lines):
        m = re.search(r"uses:\s*\./\.github/actions/([A-Za-z0-9_-]+)", line)
        if not m or m.group(1) not in names:
            continue
        indent = len(line) - len(line.lstrip())
        body, j = [], i + 1
        while j < len(lines):
            nxt = lines[j]
            if not nxt.strip():
                j += 1
                continue
            ind = len(nxt) - len(nxt.lstrip())
            if ind < indent or (ind == indent and nxt.lstrip().startswith("- ")):
                break
            body.append(nxt)
            j += 1
        yield i + 1, m.group(1), "\n".join(body)


def main():
    if not all(r.is_dir() for r in ROOTS):
        print("run from the repository root", file=sys.stderr)
        return 1
    names = guarded_actions()
    failures, checked, skipped = [], 0, 0
    files = sorted(ROOTS[0].glob("*.yml")) + sorted(ROOTS[1].glob("*/action.yml"))
    for path in files:
        text = path.read_text(encoding="utf8")
        for line_no, action, body in call_sites(text, names):
            # No endpoint means no cache is being configured here.
            if not re.search(r"^\s*minio_endpoint\s*:\s*\S", body, re.M):
                skipped += 1
                continue
            checked += 1
            missing = [c for c in CREDS if not re.search(rf"^\s*{c}\s*:\s*\S", body, re.M)]
            if missing:
                failures.append(f"{path}:{line_no}: `{action}` is missing {', '.join(missing)}")
    if failures:
        print("Compiler-cache call sites that configure a cache without credentials:\n", file=sys.stderr)
        for f in failures:
            print(f"  {f}", file=sys.stderr)
        print(
            "\nPass both alongside `minio_endpoint` (`secrets.` in a workflow,\n"
            "`inputs.` when forwarding from a composite action). Without them the cache\n"
            "probe cannot sign its request, so the pre-flight disables the cache and the\n"
            "job compiles uncached with only a warning.",
            file=sys.stderr,
        )
        return 1
    print(
        f"All {checked} compiler-cache call site(s) across {len(names)} guarded action(s) "
        f"pass credentials ({skipped} configure no endpoint)."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
