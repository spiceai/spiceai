#!/usr/bin/env python3
# Copyright 2024-2026 The Spice.ai OSS Authors
#
# Ensure every GitHub Actions workflow explicitly uses environments.
#
# Every credential a workflow uses lives in a GitHub environment, not in the
# repository or organization. This workflow checks
#   1. An environment is set at all times (`secret-free` if not needing env secrets) and it is a known environment ( GitHub creates any
#      environment a job names that does not exist yet, with no protection rules,
#      so a misspelled or new name silently gets an unprotected environment). 
#   2. If `secret-free` environment, no secrets are provided.
#   3. A job calling a reusable workflow uses a local `./.github/workflows/` file, so its jobs are
#      checked too (`secrets: inherit` would otherwise hand an unscanned workflow every secret).

"""Require every workflow job to select an environment, and keep `secret-free` jobs free of secrets."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import yaml

SECRET_FREE = "secret-free"

# The environments configured in this repository's settings, with the protection
# rules listed in docs/dev/github_actions_environments.md. Adding one here means
# creating and configuring it first. GitHub matches environment names
# case-insensitively, so these are compared in lowercase.
ALLOWED_ENVIRONMENTS = frozenset(
    {
        "ci",
        "ci-build",
        "ci-integration",
        "github-publish",
        "github-publish-autoapprove",
        "release-publish",
        SECRET_FREE,
    }
)

# The token GitHub mints for every run. It is not stored in any environment and
# its permissions come from the job's `permissions:` block, so it is not a
# custom credential and `secret-free` jobs may use it.
ALLOWED_SECRETS = frozenset({"GITHUB_TOKEN"})

LOCAL_WORKFLOW_PREFIX = "./.github/workflows/"

# GitHub expressions quote string literals with `'`, doubling it to escape. They
# are blanked first so `contains(github.ref, 'secrets.X')` is not a secret use,
# except where the literal is the index of `secrets['NAME']`.
STRING_LITERAL = re.compile(r"'(?:[^']|'')*'")
SECRETS_INDEX_OPEN = re.compile(r"(?<![\w.-])secrets\s*\[\s*$", re.IGNORECASE)

# `secrets.NAME`, `secrets['NAME']`, or the bare context (`toJSON(secrets)`),
# matched only where `secrets` starts the token so `inputs.secrets.x` or
# `steps.secrets.outputs.y` are not mistaken for the context. Context names are
# case-insensitive in GitHub expressions, so `SECRETS.NAME` matches too.
SECRET_REFERENCE = re.compile(
    r"(?<![\w.-])secrets\b\s*(?:\.\s*([A-Za-z_][\w-]*)|\[\s*'([^']*)'\s*\]|(\[)|)",
    re.IGNORECASE,
)


def workflow_files(github_dir: Path) -> list[Path]:
    workflows = github_dir / "workflows"
    return sorted([*workflows.glob("*.yml"), *workflows.glob("*.yaml")])


def environment_name(environment: object) -> str | None:
    """The environment a job's `environment:` selects: a string, or a mapping's `name`."""
    if isinstance(environment, dict):
        environment = environment.get("name")
    if isinstance(environment, str) and environment.strip():
        return environment.strip()
    return None


def _embedded_expressions(text: str):
    """The body of each `${{ ... }}` in a string.

    An expression ends at the first `}}` outside a string literal, so a literal such
    as `format('{{"a":"{0}"}}', secrets.X)` does not end it early. Literals quote
    with `'` and escape it by doubling. One forward pass per expression keeps this
    linear; a backtracking regex for the same grammar is exponential on runs of `'`.
    An unterminated expression is skipped and the scan resumes just after its `${{`,
    so a later expression in the same string is still found.
    """
    start = text.find("${{")
    while start != -1:
        i = start + 3
        in_literal = False
        end = -1
        while i < len(text):
            if in_literal:
                if text[i] == "'":
                    if text.startswith("''", i):
                        i += 2
                        continue
                    in_literal = False
            elif text[i] == "'":
                in_literal = True
            elif text.startswith("}}", i):
                end = i
                break
            i += 1
        if end == -1:
            start = text.find("${{", start + 3)
            continue
        yield text[start + 3 : end]
        start = text.find("${{", end + 2)


def _expressions(node: object, *, conditional: bool = False):
    """The text of every expression in a node: each `${{ }}`, plus bare `if:` conditions.

    `conditional` marks a job or step mapping, the only places where `if:` is a
    condition. Elsewhere `if` is an ordinary key, such as an `env:` variable or a
    `with:` input named `if`, and its value is literal text.
    """
    if isinstance(node, str):
        yield from _embedded_expressions(node)
    elif isinstance(node, dict):
        for key, value in node.items():
            if isinstance(key, str):
                yield from _embedded_expressions(key)
            # A condition is an expression whether or not it is wrapped in `${{ }}`.
            if conditional and key == "if" and isinstance(value, str) and "${{" not in value:
                yield value
            elif conditional and key == "steps" and isinstance(value, list):
                for step in value:
                    yield from _expressions(step, conditional=True)
            else:
                yield from _expressions(value)
    elif isinstance(node, list):
        for item in node:
            yield from _expressions(item)


def secret_references(node: object, *, job: bool = False) -> list[str]:
    """Every secret a node's expressions read, other than `ALLOWED_SECRETS`.

    Only expressions are scanned: a `run:` script that mentions `secrets.txt`, or a
    comment (which the YAML loader has already dropped), reads no secret. `job`
    marks a job mapping, so its `if:` and its steps' `if:` are read as conditions.
    """
    found: set[str] = set()
    for expression in _expressions(node, conditional=job):
        expression = STRING_LITERAL.sub(
            lambda m, e=expression: m.group(0) if SECRETS_INDEX_OPEN.search(e, 0, m.start()) else "''",
            expression,
        )
        for match in SECRET_REFERENCE.finditer(expression):
            dotted, quoted, dynamic = match.groups()
            name = dotted or quoted
            if name is not None:
                if name.upper() not in ALLOWED_SECRETS:
                    found.add(f"secrets.{name}")
            elif dynamic:
                found.add("secrets[...]")
            else:
                found.add("secrets")
    return sorted(found)


def check_workflow(document: dict) -> list[str]:
    jobs = document.get("jobs")
    if not isinstance(jobs, dict):
        return []
    workflow_env_secrets = secret_references(document.get("env"))
    problems = []
    for job_id, job in jobs.items():
        if not isinstance(job, dict):
            continue
        environment = job.get("environment")
        if "uses" in job:
            if environment is not None:
                problems.append(
                    f"job '{job_id}' calls a reusable workflow and sets `environment:`, "
                    "which GitHub rejects; select the environment in the called workflow's jobs instead"
                )
            uses = job["uses"]
            if not (isinstance(uses, str) and uses.startswith(LOCAL_WORKFLOW_PREFIX)):
                problems.append(
                    f"job '{job_id}' calls reusable workflow '{uses}', which is not a local "
                    f"`{LOCAL_WORKFLOW_PREFIX}` file, so its jobs' environments and secret use cannot be "
                    "checked; copy it into this repository "
                    "(see docs/dev/github_actions_environments.md)"
                )
            continue
        name = environment_name(environment)
        if name is None:
            problems.append(
                f"job '{job_id}' selects no `environment:`; add the environment whose secrets it uses, "
                f"or `environment: {SECRET_FREE}` if it uses none "
                "(see docs/dev/github_actions_environments.md)"
            )
            continue
        # An expression is resolved at run time, so the environment it selects, and
        # whether that is `secret-free`, cannot be checked here.
        if "${{" in name:
            problems.append(
                f"job '{job_id}' selects its environment with an expression ('{name}'), which this check "
                "cannot verify; name the environment literally "
                "(see docs/dev/github_actions_environments.md)"
            )
            continue
        if name.lower() not in ALLOWED_ENVIRONMENTS:
            problems.append(
                f"job '{job_id}' selects environment '{name}', which is not one of "
                f"{', '.join(f'`{e}`' for e in sorted(ALLOWED_ENVIRONMENTS))}; "
                "GitHub would create it with no protection rules "
                "(see docs/dev/github_actions_environments.md)"
            )
            continue
        if name.lower() != SECRET_FREE:
            continue
        secrets = secret_references(job, job=True)
        if secrets:
            problems.append(
                f"job '{job_id}' is in the `{SECRET_FREE}` environment but reads {', '.join(secrets)}; "
                "move it to the environment that holds those secrets, or remove the references"
            )
        if workflow_env_secrets:
            problems.append(
                f"job '{job_id}' is in the `{SECRET_FREE}` environment but inherits "
                f"{', '.join(workflow_env_secrets)} from the workflow-level `env:`; "
                "move those entries into the `env:` of the jobs that need them"
            )
    return problems


def check_definitions(github_dir: Path) -> list[str]:
    problems = []
    for path in workflow_files(github_dir):
        try:
            document = yaml.safe_load(path.read_text(encoding="utf-8"))
        except yaml.YAMLError as error:
            # `check_workflow_yaml.py` reports parse errors in detail; failing here
            # too keeps an unparseable file from passing this guard unchecked.
            problems.append(f"{path}: not valid YAML ({error.__class__.__name__}), so its jobs could not be checked")
            continue
        if not isinstance(document, dict):
            continue
        problems.extend(f"{path}: {problem}" for problem in check_workflow(document))
    return problems


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--github-dir",
        type=Path,
        default=Path(".github"),
        help="Path to the repository's .github directory (default: .github).",
    )
    args = parser.parse_args(argv)
    problems = check_definitions(args.github_dir)
    for problem in problems:
        print(f"::error::{problem}")
    if problems:
        print(f"{len(problems)} workflow environment problem(s) found.", file=sys.stderr)
        return 1
    print(f"Checked {len(workflow_files(args.github_dir))} workflows: every job selects an environment.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
