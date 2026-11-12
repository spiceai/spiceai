#!/usr/bin/env python3
"""Extract snapshot-update artifacts into the checkout, refusing anything but snapshots.

The jobs that run the test suite upload their changed `*.snap` files as
`.tar.gz` artifacts and hold a read-only GITHUB_TOKEN. The job that pushes the
snapshot-update branch holds the write token and runs no test code, so the
archive is the only thing that crosses from test code to the write token. This
script is the check at that boundary: every member must be a regular file
whose relative path ends in `.snap`, with no absolute path, no `..`, and
nothing under `.git/` or `.github/`, and it must land inside the checkout.
Anything else fails the step before a single file is written, so an archive
can't replace the workflow's own scripts or actions (`push-snap-changes`,
`approve_bot_pr_runs.sh`) or write outside the checkout.

Usage: extract_snapshot_archives.py <archive-dir> [<dest>]
"""
from __future__ import annotations

import os
import sys
import tarfile
from pathlib import Path, PurePosixPath

MAX_MEMBER_BYTES = 16 * 1024 * 1024
FORBIDDEN_TOP = {".git", ".github"}


def member_error(member: tarfile.TarInfo) -> str | None:
    name = member.name
    if not member.isreg():
        return f"{name!r} is not a regular file"
    path = PurePosixPath(name)
    if path.is_absolute() or name.startswith("/") or "\\" in name:
        return f"{name!r} is not a relative POSIX path"
    parts = [p for p in path.parts if p != "."]
    if not parts or ".." in parts:
        return f"{name!r} escapes the checkout"
    if parts[0] in FORBIDDEN_TOP:
        return f"{name!r} is under {parts[0]}/"
    if not parts[-1].endswith(".snap"):
        return f"{name!r} is not a .snap file"
    if member.size > MAX_MEMBER_BYTES:
        return f"{name!r} is {member.size} bytes, over the {MAX_MEMBER_BYTES}-byte limit"
    return None


def extract(archive_dir: Path, dest: Path) -> int:
    dest = dest.resolve()
    archives = sorted(archive_dir.glob("*.tar.gz")) if archive_dir.is_dir() else []
    planned = []
    errors = []
    for archive in archives:
        with tarfile.open(archive, "r:gz") as tar:
            for member in tar.getmembers():
                err = member_error(member)
                if err is None:
                    target = (dest / member.name).resolve()
                    if dest not in target.parents:
                        err = f"{member.name!r} resolves outside {dest}"
                if err:
                    errors.append(f"{archive.name}: {err}")
                else:
                    planned.append((archive, member.name, target))
    if errors:
        for err in errors:
            print(f"::error::refusing snapshot artifact: {err}")
        return 1
    for archive, name, target in planned:
        with tarfile.open(archive, "r:gz") as tar:
            src = tar.extractfile(tar.getmember(name))
            assert src is not None
            target.parent.mkdir(parents=True, exist_ok=True)
            if target.is_symlink():
                target.unlink()
            with open(target, "wb") as out:
                out.write(src.read())
        print(f"extracted {name}")
    print(f"{len(planned)} snapshot file(s) from {len(archives)} archive(s)")
    return 0


def main(argv: list[str]) -> int:
    if len(argv) not in (2, 3):
        print(__doc__.strip().splitlines()[-1], file=sys.stderr)
        return 2
    dest = Path(argv[2]) if len(argv) == 3 else Path(os.getcwd())
    return extract(Path(argv[1]), dest)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
