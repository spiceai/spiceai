#!/usr/bin/env python3
# Copyright 2024-2026 The Spice.ai OSS Authors
#
# GitHub Actions environment guard.
#
# Every credential a workflow uses lives in a GitHub environment, not in the
# repository or organization (see `docs/dev/github_actions_environments.md`).
# That boundary only holds while two rules hold for every job under
# `.github/workflows/`:
#
# 1. A job that runs steps selects an `environment:`. A job without one still
#    resolves repository and organization secrets, so a new job that forgets the
#    key silently bypasses the environment's ref restrictions and reviewers. A
#    job that calls a reusable workflow (`uses:`) is the exception: GitHub does
#    not allow `environment:` there, and the callee's own jobs select theirs.
#
# 2. The environment is one this repository has configured. GitHub creates any
#    environment a job names that does not exist yet, with no protection rules,
#    so a misspelled or new name silently gets an unprotected environment.
#
# 3. A job in the `secret-free` environment references no secret other than the
#    GitHub-managed `GITHUB_TOKEN`. `secret-free` carries no protection rules, so
#    it is what fork and Dependabot PR runs execute under; a `secrets.X` reference
#    there either reads a repository/organization fallback the migration is meant
#    to remove, or quietly resolves to an empty string and hides a misfiled job.
#    A workflow-level `env:` that names a secret counts against every
#    `secret-free` job in that workflow, because each of them inherits it.
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
        "github-publish",
        "github-publish-autoapprove",
        SECRET_FREE,
    }
)

# The token GitHub mints for every run. It is not stored in any environment and
# its permissions come from the job's `permissions:` block, so it is not a
# custom credential and `secret-free` jobs may use it.
ALLOWED_SECRETS = frozenset({"GITHUB_TOKEN"})

# `${{ ... }}`, ending at the first `}}` outside a string literal, so a literal
# such as `format('{{"a":"{0}"}}', secrets.X)` does not end the expression early.
EXPRESSION = re.compile(r"\$\{\{((?:[^'}]|'(?:[^']|'')*'|\}(?!\}))*)\}\}")

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


def _expressions(node: object):
    """The text of every expression in a node: each `${{ }}`, plus bare `if:` conditions."""
    if isinstance(node, str):
        yield from EXPRESSION.findall(node)
    elif isinstance(node, dict):
        for key, value in node.items():
            if isinstance(key, str):
                yield from EXPRESSION.findall(key)
            # An `if:` is an expression whether or not it is wrapped in `${{ }}`.
            if key == "if" and isinstance(value, str) and "${{" not in value:
                yield value
            else:
                yield from _expressions(value)
    elif isinstance(node, list):
        for item in node:
            yield from _expressions(item)


def secret_references(node: object) -> list[str]:
    """Every secret a node's expressions read, other than `ALLOWED_SECRETS`.

    Only expressions are scanned: a `run:` script that mentions `secrets.txt`, or a
    comment (which the YAML loader has already dropped), reads no secret.
    """
    found: set[str] = set()
    for expression in _expressions(node):
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
        secrets = secret_references(job)
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
