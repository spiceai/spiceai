#!/usr/bin/env python3
# Copyright 2024-2026 The Spice.ai OSS Authors
#
# Tests for check_cuda_compute_caps.py — the CUDA compute-capability drift guard.
"""Unit tests for the CUDA compute-capability drift guard."""

from __future__ import annotations

import io
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from check_cuda_compute_caps import (  # noqa: E402
    DOCKER_WORKFLOW,
    RELEASE_WORKFLOW,
    check,
    default_cap,
    dispatch_choices,
    docker_shell_caps,
    load_workflow,
    main,
    release_matrix_caps,
)

# Minimal stand-ins for the two real workflows: only the parts this guard reads.


def release_workflow(caps: list[str], choices: list[str] | None = None) -> str:
    entries = ",\n".join(
        f"""              {{
                compute_cap: "{cap}",
                runner: runner,
                target_os: "linux",
                target_arch: "x86_64"
              }}"""
        for cap in caps
    )
    options = "\n".join(f"          - '{choice}'" for choice in (choices or ["all", *caps]))
    return f"""---
name: build_and_release_cuda

on:
  workflow_dispatch:
    inputs:
      compute_cap:
        description: 'Which CUDA compute capability to build for?'
        required: false
        type: choice
        options:
{options}
        default: 'all'

jobs:
  setup-matrix:
    runs-on: ubuntu-24.04
    steps:
      - uses: actions/github-script@v9
        with:
          script: |
            const matrix = [
{entries}
            ];
            if (context.eventName === 'pull_request') {{
              return matrix.filter(m => m.compute_cap === "90");
            }}
            return matrix;
"""


def docker_workflow(
    caps: list[str],
    choices: list[str] | None = None,
    default: str = "80",
) -> str:
    options = "\n".join(f"          - '{choice}'" for choice in (choices or ["all", *caps]))
    return f"""---
name: spiced_docker

on:
  workflow_dispatch:
    inputs:
      compute_cap:
        description: Which CUDA compute capability to package
        required: false
        type: choice
        options:
{options}
        default: all

env:
  DEFAULT_CUDA_COMPUTE_CAP: '{default}'

jobs:
  setup:
    runs-on: ubuntu-24.04
    steps:
      - id: resolve
        run: |
          all_compute_caps='{" ".join(caps)}'
          echo "cuda_compute_caps=${{cuda_compute_caps}}" >> "$GITHUB_OUTPUT"
"""


class WorkflowsDir:
    """A temporary .github/workflows holding the two workflows under test."""

    def __init__(self, release: str | None, docker: str | None):
        self._tmp = tempfile.TemporaryDirectory()
        self.path = Path(self._tmp.name)
        if release is not None:
            (self.path / RELEASE_WORKFLOW).write_text(release, encoding="utf-8")
        if docker is not None:
            (self.path / DOCKER_WORKFLOW).write_text(docker, encoding="utf-8")

    def __enter__(self) -> Path:
        return self.path

    def __exit__(self, *exc) -> None:
        self._tmp.cleanup()


CAPS = ["80", "86", "87", "89", "90"]


class CheckTest(unittest.TestCase):
    def test_matching_lists_are_accepted(self):
        with WorkflowsDir(release_workflow(CAPS), docker_workflow(CAPS)) as workflows:
            self.assertEqual(check(workflows), [])

    def test_the_pre_fix_docker_workflow_is_rejected(self):
        """Regression test for #10622: the release workflow built five capabilities
        and the docker workflow packaged one.

        Two problems, because the pre-fix workflow had two: the four capabilities
        were neither packaged nor selectable.
        """
        with WorkflowsDir(release_workflow(CAPS), docker_workflow(["80"])) as workflows:
            problems = check(workflows)
        self.assertEqual(len(problems), 2)
        joined = "\n".join(problems)
        self.assertIn("['86', '87', '89', '90']", joined)
        self.assertIn("never packaged into an image", joined)
        self.assertIn("unreachable from a manual dispatch", joined)
        self.assertIn("#10622", joined)

    def test_a_capability_with_no_release_asset_is_rejected(self):
        """The inverse drift: packaging a capability nothing builds a binary for."""
        with WorkflowsDir(release_workflow(CAPS), docker_workflow([*CAPS, "120"])) as workflows:
            problems = check(workflows)
        self.assertEqual(len(problems), 2)  # the mismatch, and the unreachable choice
        self.assertIn("no release asset to download", problems[0])
        self.assertIn("['120']", problems[0])

    def test_an_unselectable_capability_is_rejected(self):
        """A capability that is built but missing from the dispatch choice list."""
        with WorkflowsDir(
            release_workflow(CAPS),
            docker_workflow(CAPS, choices=["all", "80", "86", "87", "89"]),
        ) as workflows:
            problems = check(workflows)
        self.assertEqual(len(problems), 1)
        self.assertIn("`compute_cap` input options", problems[0])
        self.assertIn("unreachable from a manual dispatch", problems[0])

    def test_the_release_workflows_choices_are_checked_too(self):
        with WorkflowsDir(
            release_workflow(CAPS, choices=["all", "80"]),
            docker_workflow(CAPS),
        ) as workflows:
            problems = check(workflows)
        self.assertEqual(len(problems), 1)
        self.assertIn(RELEASE_WORKFLOW, problems[0])

    def test_a_default_outside_the_list_is_rejected(self):
        """The unsuffixed `-cuda` tags alias the default, so it has to be built."""
        with WorkflowsDir(
            release_workflow(CAPS), docker_workflow(CAPS, default="75")
        ) as workflows:
            problems = check(workflows)
        self.assertEqual(len(problems), 1)
        self.assertIn("DEFAULT_CUDA_COMPUTE_CAP", problems[0])
        self.assertIn("would never be published", problems[0])

    def test_a_duplicate_capability_is_rejected(self):
        with WorkflowsDir(
            release_workflow(CAPS),
            docker_workflow([*CAPS, "90"], choices=["all", *CAPS]),
        ) as workflows:
            problems = check(workflows)
        self.assertTrue(any("listed more than once" in problem for problem in problems))

    def test_a_missing_workflow_is_reported(self):
        with WorkflowsDir(None, docker_workflow(CAPS)) as workflows:
            problems = check(workflows)
        self.assertEqual(len(problems), 1)
        self.assertIn(RELEASE_WORKFLOW, problems[0])

    def test_malformed_yaml_is_reported(self):
        with WorkflowsDir(release_workflow(CAPS), "name: [unclosed\n") as workflows:
            problems = check(workflows)
        self.assertEqual(len(problems), 1)
        self.assertIn("does not parse as YAML", problems[0])

    def test_a_missing_matrix_is_reported(self):
        with WorkflowsDir("name: nothing-here\n", docker_workflow(CAPS)) as workflows:
            problems = check(workflows)
        self.assertEqual(len(problems), 1)
        self.assertIn("found no", problems[0])


class ParsingTest(unittest.TestCase):
    def test_release_matrix_caps_ignores_js_comparisons(self):
        """`m.compute_cap === "90"` is a filter, not a matrix entry."""
        self.assertEqual(release_matrix_caps(release_workflow(CAPS)), CAPS)

    def test_docker_shell_caps_reads_the_assignment(self):
        self.assertEqual(docker_shell_caps(docker_workflow(CAPS)), CAPS)

    def test_docker_shell_caps_rejects_an_empty_list(self):
        with self.assertRaises(ValueError):
            docker_shell_caps("          all_compute_caps=''\n")

    def test_dispatch_choices_handles_the_yaml_on_key(self):
        """PyYAML resolves an unquoted `on:` to the boolean True."""
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / DOCKER_WORKFLOW
            path.write_text(docker_workflow(CAPS), encoding="utf-8")
            workflow = load_workflow(path)
        self.assertEqual(dispatch_choices(workflow, "compute_cap"), ["all", *CAPS])

    def test_dispatch_choices_reports_a_missing_input(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / DOCKER_WORKFLOW
            path.write_text(docker_workflow(CAPS), encoding="utf-8")
            workflow = load_workflow(path)
        with self.assertRaises(ValueError):
            dispatch_choices(workflow, "not_an_input")

    def test_default_cap_is_read_as_text(self):
        """An unquoted 80 parses as an int; it still has to compare to the caps."""
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / DOCKER_WORKFLOW
            path.write_text(
                docker_workflow(CAPS).replace("'80'", "80"), encoding="utf-8"
            )
            workflow = load_workflow(path)
        self.assertEqual(default_cap(workflow), "80")


class RepositoryTest(unittest.TestCase):
    def test_this_repositorys_workflows_agree(self):
        """Regression test for #10622 against the real .github/workflows."""
        workflows = Path(__file__).resolve().parents[1] / "workflows"
        self.assertEqual(check(workflows), [])


class MainTest(unittest.TestCase):
    def test_exit_codes(self):
        with WorkflowsDir(release_workflow(CAPS), docker_workflow(CAPS)) as workflows:
            out = io.StringIO()
            with redirect_stdout(out):
                self.assertEqual(main(["--workflows-dir", str(workflows)]), 0)
            self.assertIn("OK:", out.getvalue())

        with WorkflowsDir(release_workflow(CAPS), docker_workflow(["80"])) as workflows:
            err = io.StringIO()
            with redirect_stderr(err):
                self.assertEqual(main(["--workflows-dir", str(workflows)]), 1)
            self.assertIn("FAIL:", err.getvalue())


if __name__ == "__main__":
    unittest.main(verbosity=2)
