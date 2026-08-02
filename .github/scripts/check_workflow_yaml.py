#!/usr/bin/env python3
# Copyright 2024-2026 The Spice.ai OSS Authors
#
# GitHub Actions definition guard.
#
# Verifies that every workflow under `.github/workflows/` and every composite
# action under `.github/actions/` parses as YAML and carries the keys GitHub
# needs to schedule it.
#
# A definition GitHub cannot parse fails *open*: the run reports no jobs, the
# declared `on:` triggers and `paths:` filters cannot be honoured, and the run is
# not a required check, so the workflow is silently disabled while every push
# raises a failed run nobody is obliged to look at. Regression guard for #12181,
# where a mis-indented block scalar left the daily Rust advisory scan unable to
# run at all.
#
# It also rejects a step budget its own job's budget pre-empts, which is dead
# config with the same symptom — the job is terminated as `cancelled` with no
# failed step to explain it (#12340).
"""Validate the repository's GitHub Actions workflow and composite action YAML."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import yaml

# PyYAML resolves an unquoted `on:` key to the boolean `True` (YAML 1.1), which
# is also how it reaches GitHub's parser, so a trigger block is present under
# either spelling.
TRIGGER_KEYS = ("on", True)


def workflow_files(github_dir: Path) -> list[Path]:
    """Return the workflow definitions under `<github_dir>/workflows`, sorted."""
    workflows = github_dir / "workflows"
    return sorted(p for p in workflows.glob("*.y*ml") if p.is_file())


def action_files(github_dir: Path) -> list[Path]:
    """Return the composite action definitions under `<github_dir>/actions`, sorted."""
    actions = github_dir / "actions"
    return sorted(p for p in actions.glob("*/action.y*ml") if p.is_file())


def _parse_mapping(text: str) -> tuple[dict | None, list[str]]:
    """Parse a definition into its top-level mapping, or report why it has none."""
    try:
        document = yaml.safe_load(text)
    except yaml.YAMLError as error:
        return None, [f"is not valid YAML: {_format_yaml_error(error)}"]

    if not isinstance(document, dict):
        return None, ["does not parse to a mapping of top-level keys"]

    return document, []


def check_workflow(text: str) -> list[str]:
    """Return the problems with one workflow definition's contents.

    An empty list means the definition is well-formed. Kept free of filesystem
    access so the failure modes can be unit-tested from strings.
    """
    document, problems = _parse_mapping(text)
    if document is None:
        return problems

    if not any(key in document for key in TRIGGER_KEYS):
        problems.append("has no `on:` trigger block")

    jobs = document.get("jobs")
    if jobs is None:
        problems.append("has no `jobs:` block")
    elif not isinstance(jobs, dict) or not jobs:
        problems.append("has a `jobs:` block that is not a non-empty mapping")
    else:
        problems.extend(check_step_budgets(jobs))

    return problems


def check_step_budgets(jobs: dict) -> list[str]:
    """Report step budgets that the job's own budget pre-empts.

    A step whose `timeout-minutes` is not below its job's can never fire: the job
    is terminated first, which reports as `cancelled` with no failed step and
    gives the steps after it no chance to run. That is the shape of #12340, where
    the whole sign-off job reached the runner pool's wall and its `if: always()`
    cleanup never got to resolve the status the run had posted.

    This catches dead config, not a budget that is merely too tight: whether the
    gap between the two also covers the job's setup and cleanup steps depends on
    how long those take, which is not in the definition.
    """
    problems = []
    for name, job in jobs.items():
        if not isinstance(job, dict):
            continue
        job_budget = job.get("timeout-minutes")
        if not isinstance(job_budget, int):
            continue
        steps = job.get("steps")
        if not isinstance(steps, list):
            continue
        for position, step in enumerate(steps, start=1):
            if not isinstance(step, dict):
                continue
            step_budget = step.get("timeout-minutes")
            if not isinstance(step_budget, int):
                continue
            if step_budget < job_budget:
                continue
            label = step.get("name") or f"step {position}"
            problems.append(
                f"job `{name}` gives `{label}` a {step_budget}-minute budget that its "
                f"own {job_budget}-minute budget pre-empts"
            )
    return problems


def check_action(text: str) -> list[str]:
    """Return the problems with one composite action definition's contents."""
    document, problems = _parse_mapping(text)
    if document is None:
        return problems

    return [] if "runs" in document else ["has no `runs:` block"]


def _format_yaml_error(error: yaml.YAMLError) -> str:
    """Render a PyYAML error as a single line, with the mark when it carries one."""
    problem = getattr(error, "problem", None) or str(error)
    mark = getattr(error, "problem_mark", None)
    if mark is None:
        return problem.replace("\n", " ")
    return f"{problem} at line {mark.line + 1} column {mark.column + 1}"


def check_definitions(github_dir: Path) -> list[str]:
    """Check every workflow and composite action, returning one failure per problem."""
    failures = []
    for paths, check in (
        (workflow_files(github_dir), check_workflow),
        (action_files(github_dir), check_action),
    ):
        for path in paths:
            failures.extend(
                f"{path}: {problem}"
                for problem in check(path.read_text(encoding="utf-8"))
            )
    return failures


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--github-dir",
        type=Path,
        default=Path(__file__).resolve().parents[1],
        help="path to the .github directory (default: the one containing this script)",
    )
    args = parser.parse_args(argv)

    github_dir: Path = args.github_dir
    if not github_dir.is_dir():
        print(f"error: {github_dir} is not a directory", file=sys.stderr)
        return 2

    checked = len(workflow_files(github_dir)) + len(action_files(github_dir))
    if checked == 0:
        print(f"error: found no definitions under {github_dir}", file=sys.stderr)
        return 2

    failures = check_definitions(github_dir)
    if failures:
        print(
            f"{len(failures)} GitHub Actions definition problem(s) found. A definition "
            "GitHub cannot parse is silently disabled, and a budget it pre-empts never "
            "fires, so this fails the build:",
            file=sys.stderr,
        )
        for failure in failures:
            print(f"  {failure}", file=sys.stderr)
        return 1

    print(f"OK: {checked} GitHub Actions definitions are well-formed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
