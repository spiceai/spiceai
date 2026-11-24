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
# It rejects a step that runs the Spice runtime while the compiler cache's
# `AWS_ENDPOINT_URL` is still in the job's environment, which redirects the
# runtime's S3 traffic to the cache and reports as a dataset that never loads
# (#12624).
#
# It requires a nightly's later test suites to survive an earlier one's
# failure. A step's default condition is `success()`, so in a job that runs its
# suites as a sequence of steps the first red suite skips every suite behind it:
# their state stops being reported and nobody is told it stopped (#12625).
#
# Finally, in a workflow that holds `id-token: write`, it rejects a script that
# expands a value its dispatcher or a branch name chooses in place: GitHub pastes
# `${{ }}` into the script before the shell parses it, so `$(…)` in a dispatch
# input runs as code, with the token the job can trade for the store's
# credentials.
#
# And it keeps every job that assumes a CI AWS role in a reusable workflow (`on:
# workflow_call` only), called at trunk (`spiceai/spiceai/.github/workflows/<file>@trunk`)
# by every caller except the two the merge queue tests, which call theirs with `./`. Each
# such job chooses its role in its first step (`.github/actions/ci-role`), asks for the
# read-only pull request role off trunk, gives that step every other build it runs, and
# writes to the repository only with the full roles. The roles' AWS trust policies make the
# same split by `job_workflow_ref` and `ref`; this keeps the workflows asking for what AWS
# would grant.
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

# Values whoever starts the run chooses: dispatch and call inputs (one of them or the
# whole object), the branch and ref names, an event's free text, and the `github` or
# `github.event` object as a whole. Matched in an expression after `_expression_text`,
# so bracket reads count as dot reads and a string literal never matches. Inside
# `${{ }}` in a script they become code; through a step's `env:` they stay values.
UNTRUSTED_CONTEXT = re.compile(
    r"(?<![\w.-])(?:inputs\b|github\b(?!\s*\.)|github\.event\b(?!\s*\.)|toJSON\(\s*github\b"
    r"|github\.event\.inputs\b|github\.(?:head_ref|ref_name|ref|base_ref)\b"
    r"|github\.event\.(?:ref|release|merge_group|pull_request|issue|comment|review|review_comment"
    r"|head_commit|commits|pages|workflow_run|discussion|client_payload)\b)",
    re.IGNORECASE,
)
# In one left-to-right pass over an expression: a literal-key bracket read, `x['y']`,
# which reads the same value as `x.y`, and a string literal, with `''` for a quote.
# One pass, so a `[` inside one literal never pairs with a `]` inside another.
EXPRESSION_TOKEN = re.compile(r"\s*\[\s*'((?:[^']|'')*)'\s*\]|'(?:[^']|'')*'")


# A CI AWS role, read from its secret. The read-only pull request role is the one every such
# job may fall back to, so it alone does not make a job a CI role job.
CI_ROLE_SECRET = re.compile(r"secrets\.(CI_(?:NEW_)?AWS_[A-Z0-9_]*ROLE_ARN)\b")
PULL_REQUEST_ROLE = "CI_AWS_PULL_REQUEST_ROLE_ARN"
CI_ROLE_ACTION = "$/.github/actions/ci-role"
# The one `role-to-assume` a CI role job may write: the full role only when the first step
# chose it, else the read-only one. `&&`/`||` with the full role first, so an unset full
# role falls back to the read-only one and never the other way.
ROLE_CHOICE = re.compile(
    r"\$\{\{\s*steps\.ci-role\.outputs\.full\s*==\s*'true'\s*&&\s*secrets\.(CI_(?:NEW_)?AWS_[A-Z0-9_]*ROLE_ARN)"
    r"\s*\|\|\s*secrets\.CI_AWS_PULL_REQUEST_ROLE_ARN\s*\}\}"
)
# Steps that write to the repository: a snapshot branch or pull request. Only the role step's
# `full` output may gate them, which no later step can change.
REPOSITORY_WRITES = re.compile(r"push-snap-changes|\bgh\s+pr\s+create\b|\bgit\s+push\b")
FULL_ROLE = re.compile(r"steps\.ci-role\.outputs\.full\s*==\s*'true'")
# Uploads to the artifact store, directly or through mc_cp.sh, which counts when the
# destination, the argument after the source, is the store. These may also be gated on the
# read-only flag the role step sets.
STORE_WRITES = re.compile(
    r"upload-to-minio"
    r"|\b(?:mc\s+(?:cp|mirror|put)|mc_cp\.sh)\s+(?:-\S+\s+)*(?:\"[^\"\n]*\"|'[^'\n]*'|[^\s\"']+)\s+[\"']?spice-minio/"
)
# A download of a binary or archive from the CI object store (or the build bucket's spiceio
# mirror), which a job with the full roles must verify before it runs it: the store takes
# writes from more than trunk. `actions/download-artifact` is the same file's fallback in the
# same step pair. setup-spiced verifies inside the action, and the build actions build their
# tools from the checkout with the full roles, so neither counts.
STORE_DOWNLOAD = re.compile(
    r"(?:\bmc\s+cp|mc_cp\.sh)\s+(?:-\S+\s+)*[\"']?spice-minio/(?:build|artifacts)/"
    r"|ARCHIVE_S3_URI|TEST_BINARY_OBJECT_KEY"
)
VERIFY_ACTION = "$/.github/actions/verify-attestation"
SETUP_SPICED = "$/.github/actions/setup-spiced"
WRITE_GATE = re.compile(r"steps\.ci-role\.outputs\.full\s*==\s*'true'|env\.CI_CACHE_READ_ONLY\s*!=\s*'true'")
# Inputs that name a build other than the run's own commit, which a job has to hand to the
# role step: commits and refs as `builds`, which keep the full roles only for trunk's, and
# container images as `images`, which never do.
BUILD_INPUTS = {
    "spiced_commit": "builds",
    "spiced_ref": "builds",
    "ref": "builds",
    "needs_resolve_sha": "builds",
    "custom_image_tag": "images",
    "ec2_images": "images",
    "channel": "images",
}
# This repository's reusable workflow at trunk, as CI role jobs are called.
TRUNK_CALL = re.compile(r"spiceai/spiceai/\.github/workflows/([\w.-]+\.ya?ml)@trunk")
LOCAL_CALL = re.compile(r"\./\.github/workflows/([\w.-]+\.ya?ml)")
# The two callers whose runs are required merge-queue checks. A `@trunk` call there would
# test trunk's copy of the called jobs, never the pull request's, so they call theirs with
# `./`. A queue run gets the read-only role; only their trunk runs get the full roles.
LOCAL_CI_ROLE_CALLERS = {"e2e_test_ci.yml": "e2e_test_ci_aws.yml", "integration.yml": "integration_aws.yml"}


def _triggers(document: dict) -> object:
    return next((document[key] for key in TRIGGER_KEYS if key in document), None)


def ci_role_jobs(document: dict) -> list[str]:
    """The jobs of a workflow that assume a CI AWS role."""
    jobs = document.get("jobs")
    if not isinstance(jobs, dict):
        return []
    return [
        name
        for name, job in jobs.items()
        if isinstance(job, dict)
        and any(role != PULL_REQUEST_ROLE for role in CI_ROLE_SECRET.findall(yaml.safe_dump(job.get("steps") or [])))
    ]


def check_ci_role_jobs(document: dict) -> list[str]:
    """Report a CI role job outside a reusable workflow, or one that does not choose its role."""
    names = ci_role_jobs(document)
    if not names:
        return []
    triggers = _triggers(document)
    problems = []
    if not (isinstance(triggers, dict) and set(triggers) == {"workflow_call"}):
        problems.extend(
            f"job `{name}` assumes a CI AWS role in a workflow with triggers of its own; move it into a "
            "reusable workflow (`on: workflow_call` only) and call that at "
            "`spiceai/spiceai/.github/workflows/<file>@trunk`"
            for name in names
        )
        return problems
    call = triggers.get("workflow_call") or {}
    declared = set((call.get("inputs") or {}) if isinstance(call, dict) else {})
    for name in names:
        steps = [s for s in document["jobs"][name].get("steps") or [] if isinstance(s, dict)]
        first = steps[0] if steps else {}
        if first.get("uses") != CI_ROLE_ACTION or first.get("id") != "ci-role":
            problems.append(
                f"job `{name}` assumes a CI AWS role but does not choose it first; make its first step "
                f"`id: ci-role`, `uses: {CI_ROLE_ACTION}`"
            )
            continue
        given = first.get("with") or {}
        for build in sorted(declared & set(BUILD_INPUTS)):
            key = BUILD_INPUTS[build]
            if not re.search(rf"\binputs\.{re.escape(build)}\b", str(given.get(key, ""))):
                problems.append(
                    f"job `{name}` takes input `{build}`, which names another build, but does not give it to "
                    f"the `ci-role` step's `{key}`, so it would get the full roles for a build off trunk"
                )
        for position, step in enumerate(steps, start=1):
            label = step.get("name") or f"step {position}"
            if STORE_DOWNLOAD.search(str(step.get("run", ""))):
                rest = steps[position:]
                while rest and (
                    "actions/download-artifact" in str(rest[0].get("uses", ""))
                    or STORE_DOWNLOAD.search(str(rest[0].get("run", "")))
                ):
                    rest = rest[1:]
                if not rest or rest[0].get("uses") != VERIFY_ACTION:
                    problems.append(
                        f"job `{name}` step `{label}` downloads from the CI object store, and the next step "
                        f"does not verify it with `{VERIFY_ACTION}`, so a full-role job would run a file "
                        "the store took from off trunk"
                    )
            if step.get("uses") == SETUP_SPICED and str((step.get("with") or {}).get("verify", "")).strip() != (
                "${{ steps.ci-role.outputs.full }}"
            ):
                problems.append(
                    f"job `{name}` step `{label}` runs setup-spiced without `verify: ${{{{ steps.ci-role.outputs.full }}}}`, "
                    "so with the full roles it would run a spiced nobody checked"
                )
            condition = str(step.get("if", "")).strip()
            if step.get("uses") == VERIFY_ACTION and step.get("if") is not None and (
                not re.match(r"(?:\$\{\{\s*)?steps\.ci-role\.outputs\.full\s*==\s*'true'\s*(?:&&|\}\}|$)", condition)
                or re.search(r"(?<![\w.-])env\s*[.\[]", condition)
            ):
                problems.append(
                    f"job `{name}` step `{label}` verifies only `if: {step.get('if')}`; gate it on "
                    "`steps.ci-role.outputs.full == 'true'` first and on no `env` value, which an earlier step can change"
                )
            if "aws-actions/configure-aws-credentials" in str(step.get("uses", "")):
                role = str((step.get("with") or {}).get("role-to-assume", ""))
                if not ROLE_CHOICE.fullmatch(role.strip()):
                    problems.append(
                        f"job `{name}` step `{label}` sets `role-to-assume: {role}`; write "
                        "`${{ steps.ci-role.outputs.full == 'true' && secrets.<ROLE> || "
                        "secrets.CI_AWS_PULL_REQUEST_ROLE_ARN }}`"
                    )
            else:
                body = f"{step.get('uses', '')}\n{step.get('run', '')}\n{yaml.safe_dump(step.get('with') or {})}"
                condition = str(step.get("if", ""))
                if (REPOSITORY_WRITES.search(body) and not FULL_ROLE.search(condition)) or (
                    STORE_WRITES.search(body) and not WRITE_GATE.search(condition)
                ):
                    problems.append(
                        f"job `{name}` step `{label}` writes to shared state without "
                        "`steps.ci-role.outputs.full == 'true'` in its `if:`, so it would write off trunk too"
                    )
    return problems


def check_ci_role_calls(workflows: dict[str, dict]) -> list[str]:
    """Report a call to a CI role workflow that is not at trunk, or one that names it wrongly.

    `workflows` maps each file name under `.github/workflows/` to its parsed document.
    """
    reusable = {name for name, document in workflows.items() if ci_role_jobs(document)}
    problems = []
    for caller, document in sorted(workflows.items()):
        jobs = document.get("jobs")
        if not isinstance(jobs, dict):
            continue
        for job_name, job in jobs.items():
            uses = job.get("uses") if isinstance(job, dict) else None
            if not isinstance(uses, str):
                continue
            trunk, local = TRUNK_CALL.fullmatch(uses), LOCAL_CALL.fullmatch(uses)
            target = (trunk or local).group(1) if (trunk or local) else None
            where = f"{caller}: job `{job_name}`"
            if trunk and target not in workflows:
                problems.append(f"{where} calls `{uses}`, but there is no .github/workflows/{target}")
                continue
            if target not in reusable:
                other_ref = next(
                    (name for name in reusable if uses.startswith(f"spiceai/spiceai/.github/workflows/{name}@")), None
                )
                if other_ref:
                    problems.append(
                        f"{where} calls the CI AWS role workflow `{uses}`; call "
                        f"`spiceai/spiceai/.github/workflows/{other_ref}@trunk`, so trunk's definition chooses the role"
                    )
                continue
            if LOCAL_CI_ROLE_CALLERS.get(caller) == target:
                if not local:
                    problems.append(
                        f"{where} calls `{uses}`; {caller} is a required merge-queue check, so it calls "
                        f"`./.github/workflows/{target}`, which the queue then tests"
                    )
            elif not trunk:
                problems.append(
                    f"{where} calls the CI AWS role workflow `{uses}`; call "
                    f"`spiceai/spiceai/.github/workflows/{target}@trunk`, so trunk's definition chooses the role"
                )
            called = _triggers(workflows[target])
            spec = (called.get("workflow_call") or {}) if isinstance(called, dict) else {}
            declared = (spec.get("inputs") or {}) if isinstance(spec, dict) else {}
            given = job.get("with") if isinstance(job.get("with"), dict) else {}
            for key in sorted(set(given) - set(declared)):
                problems.append(f"{where} passes input `{key}`, which {target} does not declare")
            for key, value in sorted(declared.items()):
                if isinstance(value, dict) and value.get("required") and key not in given:
                    problems.append(f"{where} does not pass `{key}`, which {target} requires")
    return problems


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
        problems.extend(check_suite_visibility(jobs, is_scheduled(document)))
        problems.extend(check_untrusted_expansions(document, jobs))
        problems.extend(check_ci_role_jobs(document))

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


def _holds_id_token(permissions: object) -> bool:
    return permissions == "write-all" or (
        isinstance(permissions, dict) and permissions.get("id-token") == "write"
    )


def _ref(term: str) -> str:
    return rf"(?<![\w.-]){term}"


def _expression_text(expression: str) -> str:
    """An expression as the taint patterns read it.

    `x['y']` becomes `x.y`, so a bracket read matches the pattern its dot form does,
    and every string literal is emptied, so a literal such as `'github'` never does.
    A bracket with any other key (`env[matrix.k]`) is kept, and the patterns treat it
    as reading every member.
    """
    def token(match: re.Match) -> str:
        if match.group(1) is not None:
            return "." + match.group(1).replace("''", "'")
        return "''"

    return EXPRESSION_TOKEN.sub(token, expression)


def _expansions(text: object) -> list[tuple[str, str]]:
    """(whole, inner) for each `${{ … }}` in a text.

    An expansion ends at the first `}}` outside a string literal, as GitHub reads it, so
    `format('{{"k":"{0}"}}', inputs.x)` keeps its `inputs.x`. An unterminated one ends
    the text.
    """
    s, found, i = str(text or ""), [], 0
    while (start := s.find("${{", i)) >= 0:
        j, quoted = start + 3, False
        while j < len(s):
            if quoted:
                if s.startswith("''", j):
                    j += 2
                    continue
                quoted = s[j] != "'"
            elif s[j] == "'":
                quoted = True
            elif s.startswith("}}", j):
                break
            j += 1
        found.append((s[start : j + 2], s[start + 3 : j]))
        i = j + 2
    return found


def _expressions(value: object) -> str:
    """The `${{ }}` expressions in a value, each as `_expression_text` reads it."""
    return " ".join(_expression_text(inner) for _, inner in _expansions(value))


def _tainted_references(document: dict, jobs: dict) -> dict[str, re.Pattern]:
    """Per job, the references whose value carries something the dispatcher chose.

    Starts from UNTRUSTED_CONTEXT and follows the value, to a fixpoint, into an `env`
    entry (read later as `${{ env.X }}`), into a step's outputs when the step reads
    such a value (as `${{ }}` or as `$X` from its environment), into a job's outputs
    and so `needs.<job>.outputs`, and into a job's `matrix`. A github-script step's
    outputs are not followed: its script decides what it outputs, and the matrices
    built that way here hold only literals.
    """
    global_terms: set[str] = set()
    job_terms: dict[str, set[str]] = {name: set() for name in jobs}

    def pattern(job_name: str, extra: tuple = ()) -> re.Pattern:
        terms = [
            UNTRUSTED_CONTEXT.pattern,
            *sorted(global_terms),
            *sorted(job_terms.get(job_name, ())),
            *extra,
        ]
        return re.compile("|".join(terms), re.IGNORECASE)

    workflow_env = document.get("env") if isinstance(document.get("env"), dict) else {}
    # Until a pass adds nothing. Each pass can carry the value one job further, so a
    # chain of jobs declared last-first needs a pass per link; a fixed number of passes
    # would let a longer chain through. Terms are only added, and there are finitely
    # many (one per env key, step id and job output), so the loop ends.
    while True:
        before = (len(global_terms), sum(len(t) for t in job_terms.values()))
        anywhere = pattern("")
        tainted_workflow_env = {
            key for key, value in workflow_env.items() if anywhere.search(_expressions(value))
        }
        global_terms.update(_ref(rf"env\.{re.escape(key)}\b") for key in tainted_workflow_env)
        if tainted_workflow_env:
            global_terms.add(_ref(r"env\s*\["))
        for name, job in jobs.items():
            if not isinstance(job, dict):
                continue
            here = pattern(name)
            job_env = job.get("env") if isinstance(job.get("env"), dict) else {}
            tainted_env = tainted_workflow_env | {
                k for k, v in job_env.items() if here.search(_expressions(v))
            }
            job_terms[name].update(_ref(rf"env\.{re.escape(key)}\b") for key in tainted_env)
            if tainted_env:
                job_terms[name].add(_ref(r"env\s*\["))
            strategy = job.get("strategy")
            if isinstance(strategy, dict) and here.search(_expressions(strategy.get("matrix", ""))):
                job_terms[name].add(_ref(r"matrix\s*[.\[]"))
            for step in job.get("steps") or []:
                if not isinstance(step, dict) or not step.get("id"):
                    continue
                if "github-script" in str(step.get("uses", "")):
                    continue
                step_env = step.get("env") if isinstance(step.get("env"), dict) else {}
                names = tainted_env | {k for k, v in step_env.items() if here.search(_expressions(v))}
                run = str(step.get("run", ""))
                reads = any(re.search(rf"\$\{{?{re.escape(n)}\b", run) for n in names)
                read_here = " ".join(_expressions(step.get(k, "")) for k in ("run", "env", "with"))
                if reads or here.search(read_here):
                    job_terms[name].add(_ref(rf"steps\.{re.escape(str(step['id']))}\.outputs\b"))
                    job_terms[name].add(_ref(r"steps\s*\["))
            outputs = job.get("outputs") if isinstance(job.get("outputs"), dict) else {}
            here = pattern(name)
            for key, value in outputs.items():
                if here.search(_expressions(value)):
                    global_terms.add(_ref(rf"needs\.{re.escape(name)}\.outputs\.{re.escape(key)}\b"))
                    global_terms.add(_ref(rf"needs\.{re.escape(name)}\.outputs\s*\["))
                    global_terms.add(_ref(r"needs\s*\["))
        if before == (len(global_terms), sum(len(t) for t in job_terms.values())):
            break
    return {name: pattern(name) for name in jobs}


def _script_expansions(step: dict, tainted: re.Pattern) -> list[tuple[str, str]]:
    """(key, expansion) for each tainted `${{ }}` in a step's `run:` or github-script `script:`."""
    scripts = [("run", step.get("run"))]
    if "github-script" in str(step.get("uses", "")) and isinstance(step.get("with"), dict):
        scripts.append(("script", step["with"].get("script")))
    step_env = step.get("env") if isinstance(step.get("env"), dict) else {}
    local = [_ref(rf"env\.{re.escape(k)}\b") for k, v in step_env.items() if tainted.search(_expressions(v))]
    if local:
        tainted = re.compile("|".join([tainted.pattern, *local, _ref(r"env\s*\[")]), re.IGNORECASE)
    found = []
    for key, script in scripts:
        for whole, inner in _expansions(script):
            if tainted.search(_expression_text(inner)):
                found.append((key, whole.strip()))
    return found


def check_untrusted_expansions(document: dict, jobs: dict) -> list[str]:
    """Report scripts that expand a dispatcher's or a branch's value in place.

    Scoped to workflows where some job can mint an OIDC token (`id-token: write`),
    because that token buys the repository's store credentials. Every job of such a
    workflow is checked, since one job's outputs feed the next. A `run:` script and a
    github-script `script:` are code; `name:`, `if:`, `with:` and `env:` are not, and
    `env:` is the fix: `NAME: ${{ inputs.x }}` there, `"$NAME"` in the script.

    The value is followed through `env`, step and job outputs and the matrix
    (`_tainted_references`). What this does not follow: a github-script step's
    outputs, and a value a third-party action passes through. Composite actions in
    this repository are checked by `check_action_expansions`.
    """
    workflow_permissions = document.get("permissions")
    if not (
        _holds_id_token(workflow_permissions)
        or any(isinstance(job, dict) and _holds_id_token(job.get("permissions")) for job in jobs.values())
    ):
        return []
    tainted = _tainted_references(document, jobs)
    problems = []
    for name, job in jobs.items():
        if not isinstance(job, dict) or not isinstance(job.get("steps"), list):
            continue
        for position, step in enumerate(job["steps"], start=1):
            if not isinstance(step, dict):
                continue
            for key, expansion in _script_expansions(step, tainted[name]):
                label = step.get("name") or f"step {position}"
                problems.append(
                    f"job `{name}` step `{label}` expands `{expansion}` inside its `{key}:`, where a "
                    f"value with `$(…)` runs as code in a workflow that holds `id-token: write` — pass "
                    f"it through the step's `env:` and read the variable"
                )
    return problems


def check_action_expansions(document: dict) -> list[str]:
    """Report a composite action script that expands one of its inputs in place.

    A caller decides an action's inputs, and the workflows that hold `id-token: write`
    pass dispatch inputs to these actions, so an action reads its inputs through
    `env:` as a workflow step does.
    """
    runs = document.get("runs")
    if not isinstance(runs, dict) or not isinstance(runs.get("steps"), list):
        return []
    tainted = _tainted_references({}, {"action": {"steps": runs["steps"]}})["action"]
    problems = []
    for position, step in enumerate(runs["steps"], start=1):
        if not isinstance(step, dict):
            continue
        for key, expansion in _script_expansions(step, tainted):
            label = step.get("name") or f"step {position}"
            problems.append(
                f"step `{label}` expands `{expansion}` inside its `{key}:`, where a caller's value "
                f"with `$(…)` runs as code — pass it through the step's `env:` and read the variable"
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


def check_action_descriptions(document: dict) -> list[str]:
    """Report a `${{` in an action's descriptions.

    GitHub evaluates expressions anywhere in an action's metadata when a job loads the
    action, descriptions included, and the contexts a step has (`runner`, `github`, …)
    do not exist there. So an example such as `${{ runner.temp }}` written into an input's
    description fails every job that uses the action in `Set up job`, with "Unrecognized
    named-value: 'runner'", before any step runs.
    """
    places = [("the action's description", document.get("description"))]
    for section in ("inputs", "outputs"):
        entries = document.get(section)
        if isinstance(entries, dict):
            for name, entry in entries.items():
                if isinstance(entry, dict):
                    places.append((f"the description of {section[:-1]} `{name}`", entry.get("description")))
    return [
        f"{where} contains `${{{{`, which GitHub evaluates as an expression when it loads the action, "
        "failing every job that uses it; describe the value in words instead"
        for where, text in places
        if isinstance(text, str) and "${{" in text
    ]


def check_action(text: str) -> list[str]:
    """Return the problems with one composite action definition's contents."""
    document, problems = _parse_mapping(text)
    if document is None:
        return problems

    if "runs" not in document:
        return ["has no `runs:` block"]

    return (
        check_action_descriptions(document)
        + check_action_conditions(document["runs"])
        + check_action_expansions(document)
    )


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
    workflows = {}
    for path in workflow_files(github_dir):
        document, _ = _parse_mapping(path.read_text(encoding="utf-8"))
        if document is not None:
            workflows[path.name] = document
    failures.extend(f"{github_dir / 'workflows'}/{problem}" for problem in check_ci_role_calls(workflows))
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
