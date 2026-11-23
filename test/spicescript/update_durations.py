#!/usr/bin/env python3
# Copyright 2024-2026 The Spice.ai OSS Authors
# Licensed under the Apache License, Version 2.0: https://www.apache.org/licenses/LICENSE-2.0
"""Rewrite the scripts' recorded durations from the script times of recent CI runs.

`--partition` deals scripts to CI shards by these durations, so they should be
the times the scripts take on the CI runners, at the `--jobs` CI uses, not on a
developer's machine. This reads the `spicescript-results-*` artifacts of the
given runs (by default the newest successful E2E Test CI merge-queue runs and
trunk tests runs), takes each script's median, and writes it to the script's
`recordings/durations/<script>` file. A script no run reached keeps its
recorded duration.

    python3 test/spicescript/update_durations.py            # newest runs
    python3 test/spicescript/update_durations.py 123 456    # these runs

Needs the `gh` CLI, authenticated for the repository.
"""
import argparse
import collections
import json
import math
from pathlib import Path
import statistics
import subprocess
import tempfile

REPO_ROOT = Path(__file__).resolve().parents[2]
# Beside the scripts: one file per script, named for it, holding its whole seconds.
DURATIONS_DIR = Path("recordings/durations")
# The workflows whose runs carry spicescript results, and the event of the runs to read.
SOURCES = [("e2e_test_ci.yml", "merge_group"), ("trunk_tests.yml", "schedule")]


def gh(*args):
    return subprocess.run(["gh", *args], check=True, capture_output=True, text=True).stdout


def medians(result_files):
    """Each script's median run time, in whole seconds rounded up, over `--results` files.

    Only runs whose outcome is `passed` count, which includes a known-failing
    script that failed exactly as recorded: a script that failed otherwise
    may have stopped early (a source that never started ends it in under a
    second), and its time says nothing about how long it takes.
    """
    times = collections.defaultdict(list)
    for path in result_files:
        for script in json.loads(Path(path).read_text())["scripts"]:
            if script["outcome"] == "passed":
                times[script["file"]].append(script["seconds"])
    return {file: math.ceil(statistics.median(seconds)) for file, seconds in times.items()}


def write_durations(root, measured):
    """Writes each measured script's seconds to its duration file under `root`; returns how many changed.

    Only a script that already has a recorded duration is rewritten: the
    runner records a new script's first one (`--record-durations`).
    """
    changed = 0
    for file, seconds in sorted(measured.items()):
        file = Path(file)
        path = root / file.parent / DURATIONS_DIR / file.stem
        text = f"{seconds}\n"
        if path.exists() and path.read_text() != text:
            path.write_text(text)
            changed += 1
    return changed


def recent_runs(repo, per_source):
    runs = []
    for workflow, event in SOURCES:
        try:
            out = gh("run", "list", "-R", repo, "-w", workflow, "-e", event, "--status", "success",
                     "-L", str(per_source), "--json", "databaseId", "-q", ".[].databaseId")
        except subprocess.CalledProcessError as e:
            print(f"skipping {workflow}: {e.stderr.strip()}")
            continue
        runs += out.split()
    return runs


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("runs", nargs="*", help="run ids to read (default: the newest successful runs)")
    parser.add_argument("--repo", default="spiceai/spiceai")
    parser.add_argument("--per-workflow", type=int, default=5,
                        help="how many of each workflow's newest successful runs to read (default 5)")
    args = parser.parse_args()
    runs = args.runs or recent_runs(args.repo, args.per_workflow)
    if not runs:
        raise SystemExit("no runs to read")

    with tempfile.TemporaryDirectory() as tmp:
        files = []
        for run in runs:
            dest = Path(tmp) / run
            try:
                gh("run", "download", run, "-R", args.repo, "-p", "spicescript-results-*", "-D", str(dest))
            except subprocess.CalledProcessError as e:
                print(f"run {run}: no results ({e.stderr.strip()})")
                continue
            # Only full runs of a script: the older-DuckDB job's files
            # (spicescript-results-duckdb-*.json) run a narrowed matrix.
            files += dest.glob("*/spicescript-results.json")
            print(f"run {run}: read")
        measured = medians(files)

    changed = write_durations(REPO_ROOT, measured)
    print(f"{len(measured)} scripts measured over {len(runs)} runs; {changed} durations changed")


if __name__ == "__main__":
    main()
