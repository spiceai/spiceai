#!/usr/bin/env python3
# Copyright 2024-2026 The Spice.ai OSS Authors
#
# CUDA compute-capability drift guard.
#
# Two workflows have to agree on which CUDA compute capabilities exist:
#
#   build_and_release_cuda.yml  builds one `spiced_cuda_<cap>_linux_x86_64.tar.gz`
#                               release asset per capability
#   spiced_docker.yml           packages each of those assets into an image
#
# They agree by convention, in three hand-maintained lists (a JS matrix, a shell
# string, and two `choice` input option lists). Nothing connected them, so
# spiced_docker.yml sat at a single hardcoded capability while the release
# workflow grew to five — the CUDA image shipped sm_80 only and was unusable on
# anything newer than an A100, with the binaries sitting in the release the
# whole time. That is #10622.
#
# The failure mode is silence: adding a capability to the release workflow and
# forgetting the packaging side produces no error, just an image that quietly
# does not exist. This guard makes the lists disagree loudly instead.
"""Validate that the CUDA compute-capability lists in the workflows agree."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import yaml

RELEASE_WORKFLOW = "build_and_release_cuda.yml"
DOCKER_WORKFLOW = "spiced_docker.yml"

# `build_and_release_cuda.yml` declares its matrix inside an inline
# `actions/github-script` body, so it is JavaScript rather than YAML and has to
# be read as text.
_JS_COMPUTE_CAP = re.compile(r'compute_cap:\s*"(\d+)"')

# `spiced_docker.yml` declares its list as a shell word list in the `resolve`
# step. Matched on the assignment rather than on any digit sequence so an
# unrelated number in that step cannot be mistaken for a capability.
_SHELL_COMPUTE_CAPS = re.compile(r"^\s*all_compute_caps='([^']*)'", re.MULTILINE)


def load_workflow(path: Path) -> dict:
    """Parse a workflow file, or raise ValueError describing why it could not be."""
    if not path.is_file():
        raise ValueError(f"{path}: not found")
    try:
        # utf-8-sig so a stray BOM reports as a config problem, not a crash.
        document = yaml.safe_load(path.read_text(encoding="utf-8-sig"))
    except yaml.YAMLError as exc:
        raise ValueError(f"{path.name}: does not parse as YAML: {exc}") from exc
    if not isinstance(document, dict):
        raise ValueError(f"{path.name}: is not a YAML mapping")
    return document


def dispatch_choices(workflow: dict, input_name: str) -> list[str]:
    """Return the `choice` options of a `workflow_dispatch` input.

    PyYAML resolves the unquoted `on:` key to the boolean True (YAML 1.1), so it
    is looked up under both spellings rather than assuming either one.
    """
    triggers = workflow.get("on", workflow.get(True))
    if not isinstance(triggers, dict):
        raise ValueError("has no `on:` mapping")
    dispatch = triggers.get("workflow_dispatch")
    if not isinstance(dispatch, dict):
        raise ValueError("has no `workflow_dispatch:` mapping")
    inputs = dispatch.get("inputs")
    if not isinstance(inputs, dict) or input_name not in inputs:
        raise ValueError(f"has no `{input_name}` workflow_dispatch input")
    options = inputs[input_name].get("options")
    if not isinstance(options, list):
        raise ValueError(f"`{input_name}` input has no `options` list")
    # YAML resolves an unquoted 80 to an int; compare as text either way.
    return [str(option) for option in options]


def release_matrix_caps(text: str) -> list[str]:
    """Return the compute capabilities in the release workflow's JS matrix."""
    caps = _JS_COMPUTE_CAP.findall(text)
    if not caps:
        raise ValueError(f"{RELEASE_WORKFLOW}: found no `compute_cap: \"<n>\"` matrix entries")
    return caps


def docker_shell_caps(text: str) -> list[str]:
    """Return the compute capabilities in the docker workflow's shell list."""
    match = _SHELL_COMPUTE_CAPS.search(text)
    if match is None:
        raise ValueError(f"{DOCKER_WORKFLOW}: found no `all_compute_caps='...'` assignment")
    caps = match.group(1).split()
    if not caps:
        raise ValueError(f"{DOCKER_WORKFLOW}: `all_compute_caps` is empty")
    return caps


def default_cap(workflow: dict) -> str:
    """Return the docker workflow's DEFAULT_CUDA_COMPUTE_CAP."""
    env = workflow.get("env")
    if not isinstance(env, dict) or "DEFAULT_CUDA_COMPUTE_CAP" not in env:
        raise ValueError(f"{DOCKER_WORKFLOW}: has no top-level DEFAULT_CUDA_COMPUTE_CAP env")
    return str(env["DEFAULT_CUDA_COMPUTE_CAP"])


def _duplicates(caps: list[str]) -> list[str]:
    return sorted({cap for cap in caps if caps.count(cap) > 1})


def check(workflows_dir: Path) -> list[str]:
    """Return a list of problems with the CUDA capability lists."""
    release_path = workflows_dir / RELEASE_WORKFLOW
    docker_path = workflows_dir / DOCKER_WORKFLOW

    problems: list[str] = []
    try:
        release_text = release_path.read_text(encoding="utf-8-sig")
        docker_text = docker_path.read_text(encoding="utf-8-sig")
        release_workflow = load_workflow(release_path)
        docker_workflow = load_workflow(docker_path)
        release_caps = release_matrix_caps(release_text)
        docker_caps = docker_shell_caps(docker_text)
        release_choices = dispatch_choices(release_workflow, "compute_cap")
        docker_choices = dispatch_choices(docker_workflow, "compute_cap")
        default = default_cap(docker_workflow)
    except (OSError, ValueError) as exc:
        # Every check below compares two lists, so failing to read one of them
        # makes the rest meaningless rather than merely incomplete.
        return [str(exc)]

    for name, caps in ((RELEASE_WORKFLOW, release_caps), (DOCKER_WORKFLOW, docker_caps)):
        repeated = _duplicates(caps)
        if repeated:
            problems.append(
                f"{name}: compute capabilities {repeated} are listed more than once. "
                "A duplicate builds the same image twice and makes the two lists "
                "compare unequal for a reason that is not a real difference."
            )

    if set(release_caps) != set(docker_caps):
        only_release = sorted(set(release_caps) - set(docker_caps))
        only_docker = sorted(set(docker_caps) - set(release_caps))
        detail = []
        if only_release:
            detail.append(
                f"{only_release} have release binaries but are never packaged into an image"
            )
        if only_docker:
            detail.append(
                f"{only_docker} would be packaged but have no release asset to download, "
                "so the image build fails at the curl"
            )
        problems.append(
            f"{RELEASE_WORKFLOW}'s matrix and {DOCKER_WORKFLOW}'s `all_compute_caps` "
            f"disagree: {'; '.join(detail)}. See #10622."
        )

    # Derived from the release matrix, not from either choice list: that matrix
    # is what decides which binaries exist, so it is the one list the other three
    # have to follow.
    expected_choices = ["all", *release_caps]
    for name, choices in (
        (RELEASE_WORKFLOW, release_choices),
        (DOCKER_WORKFLOW, docker_choices),
    ):
        if choices != expected_choices:
            problems.append(
                f"{name}: the `compute_cap` input options are {choices}, expected "
                f"{expected_choices}. A capability that is built but cannot be "
                "selected is unreachable from a manual dispatch."
            )

    if default not in docker_caps:
        problems.append(
            f"{DOCKER_WORKFLOW}: DEFAULT_CUDA_COMPUTE_CAP is {default!r}, which is not "
            f"one of {docker_caps}. The unsuffixed `-cuda` tags alias that capability, "
            "so they would never be published."
        )

    return problems


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--workflows-dir",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "workflows",
        help="path to .github/workflows (default: this repository's)",
    )
    args = parser.parse_args(argv)

    problems = check(args.workflows_dir)
    if problems:
        print(f"FAIL: {len(problems)} problem(s) in {args.workflows_dir}:", file=sys.stderr)
        for problem in problems:
            print(f"  - {problem}", file=sys.stderr)
        return 1

    print(
        f"OK: {RELEASE_WORKFLOW} and {DOCKER_WORKFLOW} agree on the CUDA compute "
        "capabilities, every capability is selectable, and the default is one of them"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
