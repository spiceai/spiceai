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
# Finally it rejects a step that runs the Spice runtime while the compiler
# cache's `AWS_ENDPOINT_URL` is still in the job's environment, which redirects
# the runtime's S3 traffic to the cache and reports as a dataset that never
# loads (#12624).
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
# escape it. Literals are dropped before the scan so that a condition comparing
# against text that happens to lead with a context name — `== 'env.FOO'` — is not
# read as a use of that context.
STRING_LITERAL = re.compile(r"'[^']*'")

# `secrets` is the case that has actually bitten, so its diagnosis names the fix
# trunk already uses rather than leaving the reader to find it.
CONTEXT_REMEDIES = {
    "secrets": (
        "hoist the test into a job-level `env:` "
        "(`HAS_X: ${{ secrets.X != '' }}`) and test `env.HAS_X == 'true'`"
    ),
}

# The compiler-cache proxy's setup action, which exports `AWS_ENDPOINT_URL` into
# `$GITHUB_ENV` for the rest of the job.
CACHE_SETUP_ACTION = "spiceio/.github/actions/setup"

# Ways a step starts the Spice runtime from the workflow file: `nextest`, a test
# binary the job downloaded rather than built, and the CLI. All three inherit the
# step's environment, so all three see the cache endpoint. Optional `KEY=value`
# prefixes are skipped so the binary form still reads as the command.
#
# What this cannot see: a `make` target or a composite action that starts the
# runtime, because the command is in the Makefile or the action, not here. No job
# combines those with the cache setup today. If one comes to, the guard has to
# grow rather than be trusted.
RUNTIME_INVOCATION = re.compile(
    r"\bcargo\s+nextest\s+run\b"
    r"|\bspice\s+run\b"
    r"|^(?:\S+=\S+[ \t]+)*\./\S*integration_test\S*",
    re.MULTILINE,
)

# Dropping it for the step's children. `unset` is the only form that works:
# writing an empty value back to `$GITHUB_ENV` leaves the variable *set*, and
# `object_store` then builds a store whose bucket endpoint is the empty string.
#
# The command has to sit at the top level of the script — indentation means it is
# inside an `if` or a function and may not run — must not be `unset -f`, which
# removes a function of that name and leaves the variable alone, and anything
# after a `#` is its comment rather than its argument.
DROPS_CACHE_ENDPOINT = re.compile(
    r"^unset[ \t]+(?!-)[^\n#]*\bAWS_ENDPOINT_URL\b", re.MULTILINE
)

# A line whose first non-blank character is `#`. Removed before looking for a
# runtime invocation, so a command named in a comment does not read as one.
COMMENT_LINE = re.compile(r"^[ \t]*#[^\n]*$", re.MULTILINE)


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
        problems.extend(check_cache_endpoint_isolation(jobs))

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


def check_cache_endpoint_isolation(jobs: dict) -> list[str]:
    """Report steps that run the Spice runtime with the compiler cache's S3 endpoint set.

    The cache proxy's setup action exports `AWS_ENDPOINT_URL` into `$GITHUB_ENV`
    so the steps that follow it — sccache, the AWS CLI — reach the cache. The
    build steps need that. A step that runs `spiced` must not inherit it:
    `AmazonS3Builder::from_env()` maps `AWS_ENDPOINT_URL` to the S3 endpoint, so
    every `s3://` dataset with no explicit endpoint of its own is fetched from
    the cache, and every S3 Vectors call goes there too.

    Nothing about the resulting failure names S3. The dataset stays unhealthy,
    the runtime never reports ready, and the test fails on a readiness timeout —
    #12624, where this took out all three test jobs of the models nightly, and
    the first one masked the eight suites queued behind it.

    Only steps that follow the setup see the export, so only those are checked,
    and the drop has to come before the runtime starts to be worth anything. What
    this does not model is shell semantics: a drop that is later re-exported, or
    one the guard reads as top-level while a wrapper re-adds the variable, still
    passes. It is a guard against the mistake that happened, not a proof.
    """
    problems = []
    for name, job in jobs.items():
        if not isinstance(job, dict):
            continue
        steps = job.get("steps")
        if not isinstance(steps, list):
            continue
        setup_at = next(
            (
                index
                for index, step in enumerate(steps)
                if isinstance(step, dict) and CACHE_SETUP_ACTION in str(step.get("uses", ""))
            ),
            None,
        )
        if setup_at is None:
            continue
        for position, step in enumerate(steps[setup_at + 1 :], start=setup_at + 2):
            if not isinstance(step, dict):
                continue
            # Both searches read the same text so their offsets are comparable.
            script = COMMENT_LINE.sub("", str(step.get("run", "")))
            invocation = RUNTIME_INVOCATION.search(script)
            if invocation is None:
                continue
            drop = DROPS_CACHE_ENDPOINT.search(script)
            if drop is not None and drop.start() < invocation.start():
                continue
            label = step.get("name") or f"step {position}"
            problems.append(
                f"job `{name}` step `{label}` runs the Spice runtime after `{CACHE_SETUP_ACTION}` "
                f"has exported `AWS_ENDPOINT_URL`, which sends every `s3://` dataset to the "
                f"compiler cache — start the step with `unset AWS_ENDPOINT_URL`"
            )
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
            "GitHub cannot parse or schedule is silently disabled, a budget it "
            "pre-empts never fires, and a leaked cache endpoint reports as an "
            "unrelated test failure, so this fails the build:",
            file=sys.stderr,
        )
        for failure in failures:
            print(f"  {failure}", file=sys.stderr)
        return 1

    print(f"OK: {checked} GitHub Actions definitions are well-formed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
