#!/usr/bin/env python3
"""Every `setup-sccache` call site must pass the cache credentials.

The action writes a cache config that signs its requests on every path except macOS via
`spiceio_endpoint`, and it probes the cache before wrapping rustc. A call site that omits
the credentials therefore gets a probe that cannot sign, and the pre-flight disables the
cache for the whole job -- which surfaces as nothing louder than a `::warning::` while
every build in that job compiles uncached. That is how three jobs ran uncached unnoticed.

Passing the keys everywhere also covers the latent case: callers that pass both endpoints
fall back to the signing minio path whenever the spiceio endpoint resolves empty.
"""

import pathlib
import re
import sys

WORKFLOWS = pathlib.Path(".github/workflows")
REQUIRED = ("minio_access_key", "minio_secret_key")


def call_sites(text):
    """Yield (line_no, step_body) for each setup-sccache usage.

    `uses:` and `with:` are sibling keys of the same step, so they share an indent --
    collecting only *more*-indented lines would stop before `with:` and see nothing.
    Collect everything at or deeper than the `uses:` indent, and stop at the dedent that
    ends the step (the next list item sits two columns further out).
    """
    lines = text.split("\n")
    for i, line in enumerate(lines):
        if "setup-sccache" not in line or "uses:" not in line:
            continue
        indent = len(line) - len(line.lstrip())
        block, j = [], i + 1
        while j < len(lines):
            nxt = lines[j]
            if not nxt.strip():
                j += 1
                continue
            ind = len(nxt) - len(nxt.lstrip())
            if ind < indent or (ind == indent and nxt.lstrip().startswith("- ")):
                break
            block.append(nxt)
            j += 1
        yield i + 1, "\n".join(block)


def main():
    if not WORKFLOWS.is_dir():
        print("no .github/workflows directory; run from the repository root", file=sys.stderr)
        return 1
    failures = []
    checked = 0
    for path in sorted(WORKFLOWS.glob("*.yml")):
        text = path.read_text(encoding="utf8")
        for line_no, block in call_sites(text):
            checked += 1
            missing = [k for k in REQUIRED if not re.search(rf"^\s*{k}\s*:", block, re.M)]
            if missing:
                failures.append(f"{path}:{line_no}: setup-sccache is missing {', '.join(missing)}")
    if failures:
        print("Compiler-cache call sites missing credentials:\n", file=sys.stderr)
        for f in failures:
            print(f"  {f}", file=sys.stderr)
        print(
            "\nAdd both to that step's `with:`:\n"
            "          minio_access_key: ${{ secrets.TEST_MINIO_ACCESS_KEY }}\n"
            "          minio_secret_key: ${{ secrets.TEST_MINIO_SECRET_KEY }}\n"
            "\nWithout them the cache probe cannot sign its request, so the pre-flight\n"
            "disables the cache and the job compiles uncached with only a warning.",
            file=sys.stderr,
        )
        return 1
    print(f"All {checked} setup-sccache call site(s) pass the cache credentials.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
