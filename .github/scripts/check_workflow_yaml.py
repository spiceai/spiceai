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
#
# And it rejects an `if:` naming a context GitHub does not make available there,
# which fails the same way for a different reason: GitHub rejects the definition
# rather than the expression, so the workflow never schedules (#12396).
#
# Finally, it requires a nightly's later test suites to survive an earlier one's
# failure. A step's default condition is `success()`, so in a job that runs its
# suites as a sequence of steps the first red suite skips every suite behind it:
# their state stops being reported and nobody is told it stopped (#12625).
"""Validate the repository's GitHub Actions workflow and composite action YAML."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import yaml

# PyYAML resolves an unquoted `on:` key to the boolean `True` (YAML 1.1), which
# is also how it reaches GitHub's parser, so a trigger block is present under
# either spelling.
TRIGGER_KEYS = ("on", True)

# Every context GitHub defines. Only these names are reported when they appear
# somewhere they are not available, so an unrecognised `word.` / `word[` is left
# alone. String literals are removed before the scan (see `unavailable_contexts`),
# so an incidental dotted or bracketed string inside a condition cannot fail the
# build even when it leads with a real context name.
KNOWN_CONTEXTS = frozenset(
    {
        "env",
        "github",
        "inputs",
        "job",
        "jobs",
        "matrix",
        "needs",
        "runner",
        "secrets",
        "steps",
        "strategy",
        "vars",
    }
)

# What each kind of `if:` may name. A job-level `if:` is evaluated before the job
# has an environment, a matrix row, or any completed step, so it sees far less
# than a step's does.
JOB_IF_CONTEXTS = frozenset({"github", "inputs", "needs", "vars"})
STEP_IF_CONTEXTS = JOB_IF_CONTEXTS | frozenset(
    {"env", "job", "matrix", "runner", "steps", "strategy"}
)

# A context reference is `name.` or `name[` — GitHub expressions accept property
# access in either form, and `secrets['TOKEN']` makes a definition unschedulable
# exactly as `secrets.TOKEN` does. The match holds only when `name` starts the
# token: the lookbehind is what keeps `needs.setup-model-matrix.outputs.matrix`
# from reading as a use of the `matrix` context (`e2e_test_ci.yml` has that).
CONTEXT_REFERENCE = re.compile(r"(?<![\w.-])([a-z]+)\s*[.\[]")

# A GitHub expression quotes string literals with `'`, doubling the quote to
# escape it. Literals are dropped before a condition is scanned so that text which
# merely reads like an expression is not mistaken for one: `== 'env.FOO'` is not a
# use of the `env` context, and `== 'always()'` is not a call to `always()`.
STRING_LITERAL = re.compile(r"'[^']*'")

# `secrets` is the case that has actually bitten, so its diagnosis names the fix
# trunk already uses rather than leaving the reader to find it.
CONTEXT_REMEDIES = {
    "secrets": (
        "hoist the test into a job-level `env:` "
        "(`HAS_X: ${{ secrets.X != '' }}`) and test `env.HAS_X == 'true'`"
    ),
}


# A step that runs a test suite. Both spellings reach the same place: a suite
# whose result the run exists to report.
SUITE_COMMANDS = ("cargo nextest run", "cargo test")

# The action that publishes a nightly's refreshed snapshots. It is the last step
# of the jobs below, so a suite that skips it costs the run its whole output.
SNAPSHOT_ACTION = "push-snap-changes"

# The two status functions that let a step run after an earlier one failed.
# `always()` also survives cancellation; `!cancelled()` does not, and is the
# better default — a cancelled run should stop.
SURVIVES_FAILURE = re.compile(r"(?:!\s*cancelled|always)\s*\(\s*\)")


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
        problems.extend(check_workflow_conditions(jobs))
        problems.extend(check_suite_visibility(jobs, is_scheduled(document)))

    return problems


def unavailable_contexts(condition: object, available: frozenset[str]) -> list[str]:
    """Return the contexts `condition` names that are not available to it.

    Property access counts in either of the forms GitHub accepts, `name.KEY` and
    `name['KEY']`. Quoted string literals are removed first, so text that merely
    reads like a context reference is not mistaken for one.

    Order follows first appearance in the condition so the message reads in the
    same order as the line it is about, and each context is reported once.
    """
    scanned = STRING_LITERAL.sub("''", str(condition))
    found = []
    for name in CONTEXT_REFERENCE.findall(scanned):
        if name in KNOWN_CONTEXTS and name not in available and name not in found:
            found.append(name)
    return found


def _condition_problem(where: str, condition: object, available: frozenset[str]) -> str | None:
    """Describe one `if:` that names an unavailable context, or None if it is fine."""
    unavailable = unavailable_contexts(condition, available)
    if not unavailable:
        return None
    named = ", ".join(f"`{name}`" for name in unavailable)
    problem = (
        f"{where} has an `if:` naming {named}, which GitHub does not make available "
        f"there, so it cannot schedule this definition at all"
    )
    remedies = [CONTEXT_REMEDIES[name] for name in unavailable if name in CONTEXT_REMEDIES]
    return f"{problem} — {'; '.join(remedies)}" if remedies else problem


def check_workflow_conditions(jobs: dict) -> list[str]:
    """Report `if:` conditions naming a context GitHub does not provide there.

    GitHub validates an `if:` expression's contexts when it reads the file, not
    when it evaluates the condition, so naming an unavailable one does not fail
    the step — it makes the whole workflow unschedulable. The run that appears is
    a startup failure: zero jobs, no downloadable log, and a run named after the
    file path because `name:` was never reached, which reads as a mystery rather
    than as a definition error.

    #12396 is the shape: ten `if: ${{ secrets.X != '' }}` conditionals left
    `integration tests (models)` unable to start on any push or PR to the
    release/2.1 line. `secrets` is available in `env:` and `with:`, just not in an
    `if:`, so the fix is an indirection through the job's environment rather than
    dropping the condition.
    """
    problems = []
    for name, job in jobs.items():
        if not isinstance(job, dict):
            continue
        if "if" in job:
            problem = _condition_problem(f"job `{name}`", job["if"], JOB_IF_CONTEXTS)
            if problem:
                problems.append(problem)
        steps = job.get("steps")
        if not isinstance(steps, list):
            continue
        for position, step in enumerate(steps, start=1):
            if not isinstance(step, dict) or "if" not in step:
                continue
            label = step.get("name") or f"step {position}"
            problem = _condition_problem(
                f"job `{name}` step `{label}`", step["if"], STEP_IF_CONTEXTS
            )
            if problem:
                problems.append(problem)
    return problems


def check_action_conditions(runs: object) -> list[str]:
    """Report the same unavailable-context problem in a composite action's steps.

    Reuses the workflow step set, which is deliberately the permissive choice: a
    composite step sees a little less than a workflow step (no `needs`, no matrix),
    so allowing those cannot produce a false positive — and `secrets`, the one that
    has actually broken a definition here, is excluded either way.
    """
    if not isinstance(runs, dict):
        return []
    steps = runs.get("steps")
    if not isinstance(steps, list):
        return []
    problems = []
    for position, step in enumerate(steps, start=1):
        if not isinstance(step, dict) or "if" not in step:
            continue
        label = step.get("name") or f"step {position}"
        problem = _condition_problem(f"step `{label}`", step["if"], STEP_IF_CONTEXTS)
        if problem:
            problems.append(problem)
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


def is_scheduled(document: dict) -> bool:
    """Report whether the workflow carries a `schedule:` trigger."""
    for key in TRIGGER_KEYS:
        triggers = document.get(key)
        if isinstance(triggers, dict) and "schedule" in triggers:
            return True
    return False


def _runs_a_suite(step: dict) -> bool:
    """Report whether the step's `run:` invokes a test suite."""
    command = step.get("run")
    return isinstance(command, str) and any(c in command for c in SUITE_COMMANDS)


def _publishes_snapshots(step: dict) -> bool:
    """Report whether the step publishes the run's refreshed snapshots."""
    action = step.get("uses")
    return isinstance(action, str) and SNAPSHOT_ACTION in action


def _survives_an_earlier_failure(step: dict) -> bool:
    """Report whether the step's condition lets it run after an earlier one failed.

    String literals are removed first, so a condition that merely compares against
    the text `'always()'` does not read as a call to it and buy the step an
    exemption it has not earned.
    """
    condition = step.get("if")
    if not isinstance(condition, str):
        return False
    return bool(SURVIVES_FAILURE.search(STRING_LITERAL.sub("''", condition)))


def check_suite_visibility(jobs: dict, scheduled: bool) -> list[str]:
    """Report a scheduled job's test suites that an earlier suite's failure hides.

    A step's default condition is `success()`. A job that runs its suites as a
    sequence of steps therefore reports the first failure and skips everything
    behind it, so the remaining suites' state goes unobserved while the job's
    conclusion says only that something failed — one red suite is indistinguishable
    from nine. That is #12625, where an OpenAI failure skipped eight suites and the
    search suite's state was unknown for two days.

    The first suite in a job needs no condition: nothing precedes it to hide it.
    Everything after it does, as does the step that publishes the snapshots, which
    is what a nightly refresh exists to produce.

    Scoped to scheduled workflows. On a PR gate, stopping at the first failure is
    the point, and this says nothing about those.

    A suite is recognised by the command in its own `run:`, so a job that reaches
    its suites through a `make` target or a composite action is not covered — the
    command lives outside the workflow file. No scheduled job does that today.
    """
    if not scheduled:
        return []

    problems = []
    for name, job in jobs.items():
        if not isinstance(job, dict):
            continue
        steps = job.get("steps")
        if not isinstance(steps, list):
            continue

        a_suite_ran = False
        for position, step in enumerate(steps, start=1):
            if not isinstance(step, dict):
                continue
            runs_a_suite = _runs_a_suite(step)
            hideable = runs_a_suite or _publishes_snapshots(step)
            if hideable and a_suite_ran and not _survives_an_earlier_failure(step):
                label = step.get("name") or f"step {position}"
                problems.append(
                    f"job `{name}` lets an earlier suite's failure skip `{label}`; "
                    "give it `if: ${{ !cancelled() && <its existing condition> }}` so "
                    "the job's conclusion reports every suite"
                )
            a_suite_ran = a_suite_ran or runs_a_suite
    return problems


def check_action(text: str) -> list[str]:
    """Return the problems with one composite action definition's contents."""
    document, problems = _parse_mapping(text)
    if document is None:
        return problems

    if "runs" not in document:
        return ["has no `runs:` block"]

    return check_action_conditions(document["runs"])


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
            "GitHub cannot parse or schedule is silently disabled, and a budget it "
            "pre-empts never fires, so this fails the build:",
            file=sys.stderr,
        )
        for failure in failures:
            print(f"  {failure}", file=sys.stderr)
        return 1

    print(f"OK: {checked} GitHub Actions definitions are well-formed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
