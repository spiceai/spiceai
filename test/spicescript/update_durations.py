#!/usr/bin/env python3
# Copyright 2024-2026 The Spice.ai OSS Authors
# Licensed under the Apache License, Version 2.0: https://www.apache.org/licenses/LICENSE-2.0
"""Rewrite recordings/durations.json from the script times of recent CI runs.

`--partition` deals scripts to CI shards by these durations, so they should be
the times the scripts take on the CI runners, at the `--jobs` CI uses, not on a
developer's machine. This reads the `spicescript-results-*` artifacts of the
given runs (by default the newest successful E2E Test CI merge-queue runs and
spicescript nightly runs), takes each script's median, and writes it beside the
script. A script no run reached keeps its recorded duration.

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
DURATIONS_FILE = Path("recordings/durations.json")
# The workflows whose runs carry spicescript results, and the event of the runs to read.
SOURCES = [("e2e_test_ci.yml", "merge_group"), ("nightly_tests.yml", "schedule")]


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

    by_dir = collections.defaultdict(dict)
    for file, seconds in measured.items():
        file = Path(file)
        by_dir[file.parent][file.stem] = seconds
    changed = 0
    for directory, in_dir in sorted(by_dir.items()):
        path = REPO_ROOT / directory / DURATIONS_FILE
        if not path.exists():
            print(f"skipping {directory}: it has no {DURATIONS_FILE}")
            continue
        recorded = json.loads(path.read_text())
        updated = {**recorded, **{k: v for k, v in in_dir.items() if k in recorded}}
        if updated != recorded:
            changed += sum(updated[k] != recorded[k] for k in recorded)
            path.write_text(json.dumps(dict(sorted(updated.items())), indent=2) + "\n")
    print(f"{len(measured)} scripts measured over {len(runs)} runs; {changed} durations changed")


if __name__ == "__main__":
    main()
