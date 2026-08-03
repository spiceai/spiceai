"""Unit tests for check_workflow_yaml.py."""
import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import check_workflow_yaml  # noqa: E402

VALID_WORKFLOW = """\
---
name: Check Rust Advisories

on:
  schedule:
    - cron: '20 5 * * *'
  workflow_dispatch:

jobs:
  advisories:
    name: Check Rust Advisories
    runs-on: ubuntu-24.04
    steps:
      - run: echo hello
"""

VALID_ACTION = """\
name: Set up make
description: Installs make

runs:
  using: composite
  steps:
    - run: echo hello
      shell: bash
"""

# The exact shape of #12181: a non-empty line inside a `script: |` block scalar
# sits at column 0, which terminates the scalar and leaves YAML reading a bare
# string where a mapping key belongs.
DEDENTED_BLOCK_SCALAR_WORKFLOW = """\
---
name: Check Rust Advisories

on:
  workflow_dispatch:

jobs:
  advisories:
    runs-on: ubuntu-24.04
    steps:
      - uses: actions/github-script@v9
        with:
          script: |
            const body = [
              marker,
'`cargo deny check advisories` found Rust dependency advisory violations.',
              '',
            ].join('\\n');
"""


class CheckWorkflowTest(unittest.TestCase):
    def test_valid_workflow_has_no_problems(self):
        self.assertEqual(check_workflow_yaml.check_workflow(VALID_WORKFLOW), [])

    def test_dedented_block_scalar_is_reported_as_a_syntax_error(self):
        problems = check_workflow_yaml.check_workflow(DEDENTED_BLOCK_SCALAR_WORKFLOW)
        self.assertEqual(len(problems), 1, problems)
        self.assertIn("is not valid YAML", problems[0])
        # The line of the offending dedent is what makes the failure actionable.
        self.assertIn("line 16 ", problems[0])

    def test_quoted_on_key_is_accepted(self):
        problems = check_workflow_yaml.check_workflow(
            VALID_WORKFLOW.replace("\non:\n", '\n"on":\n')
        )
        self.assertEqual(problems, [])

    def test_missing_trigger_block_is_reported(self):
        without_on = VALID_WORKFLOW.replace(
            "on:\n  schedule:\n    - cron: '20 5 * * *'\n  workflow_dispatch:\n", ""
        )
        self.assertEqual(
            check_workflow_yaml.check_workflow(without_on), ["has no `on:` trigger block"]
        )

    def test_missing_jobs_block_is_reported(self):
        problems = check_workflow_yaml.check_workflow(
            "name: x\non:\n  workflow_dispatch:\n"
        )
        self.assertEqual(problems, ["has no `jobs:` block"])

    def test_empty_jobs_block_is_reported(self):
        problems = check_workflow_yaml.check_workflow(
            "name: x\non:\n  workflow_dispatch:\njobs: {}\n"
        )
        self.assertEqual(
            problems, ["has a `jobs:` block that is not a non-empty mapping"]
        )

    def test_jobs_as_a_list_is_reported(self):
        problems = check_workflow_yaml.check_workflow(
            "name: x\non:\n  workflow_dispatch:\njobs:\n  - build\n"
        )
        self.assertEqual(
            problems, ["has a `jobs:` block that is not a non-empty mapping"]
        )

    def test_empty_file_is_reported(self):
        self.assertEqual(
            check_workflow_yaml.check_workflow(""),
            ["does not parse to a mapping of top-level keys"],
        )

    def test_a_workflow_missing_both_keys_reports_both(self):
        self.assertEqual(
            check_workflow_yaml.check_workflow("name: x\n"),
            ["has no `on:` trigger block", "has no `jobs:` block"],
        )


class CheckActionTest(unittest.TestCase):
    def test_valid_action_has_no_problems(self):
        self.assertEqual(check_workflow_yaml.check_action(VALID_ACTION), [])

    def test_missing_runs_block_is_reported(self):
        problems = check_workflow_yaml.check_action("name: x\ndescription: y\n")
        self.assertEqual(problems, ["has no `runs:` block"])

    def test_invalid_yaml_is_reported(self):
        problems = check_workflow_yaml.check_action("runs:\n  using: composite\n bad\n")
        self.assertEqual(len(problems), 1, problems)
        self.assertIn("is not valid YAML", problems[0])

    def test_an_action_is_not_required_to_declare_triggers_or_jobs(self):
        """A composite action legitimately has neither `on:` nor `jobs:`."""
        self.assertEqual(check_workflow_yaml.check_action(VALID_ACTION), [])
        self.assertNotEqual(check_workflow_yaml.check_workflow(VALID_ACTION), [])


class DiscoveryTest(unittest.TestCase):
    def _github_dir(self, tmp: str) -> Path:
        github_dir = Path(tmp) / ".github"
        (github_dir / "workflows").mkdir(parents=True)
        (github_dir / "actions" / "setup-make").mkdir(parents=True)
        (github_dir / "actions" / "notes").mkdir(parents=True)
        return github_dir

    def test_discovery_finds_workflows_and_actions_but_not_other_yaml(self):
        with tempfile.TemporaryDirectory() as tmp:
            github_dir = self._github_dir(tmp)
            (github_dir / "workflows" / "pr.yml").write_text(VALID_WORKFLOW)
            (github_dir / "workflows" / "release.yaml").write_text(VALID_WORKFLOW)
            (github_dir / "workflows" / "notes.md").write_text("not yaml")
            (github_dir / "actions" / "setup-make" / "action.yml").write_text(VALID_ACTION)
            # A stray YAML file beside a composite action is not a definition.
            (github_dir / "actions" / "notes" / "data.yml").write_text("a: 1\n")

            self.assertEqual(
                [p.name for p in check_workflow_yaml.workflow_files(github_dir)],
                ["pr.yml", "release.yaml"],
            )
            self.assertEqual(
                [p.parent.name for p in check_workflow_yaml.action_files(github_dir)],
                ["setup-make"],
            )
            self.assertEqual(check_workflow_yaml.check_definitions(github_dir), [])

    def test_a_broken_workflow_is_reported_with_its_path(self):
        with tempfile.TemporaryDirectory() as tmp:
            github_dir = self._github_dir(tmp)
            (github_dir / "workflows" / "ok.yml").write_text(VALID_WORKFLOW)
            broken = github_dir / "workflows" / "broken.yml"
            broken.write_text(DEDENTED_BLOCK_SCALAR_WORKFLOW)

            failures = check_workflow_yaml.check_definitions(github_dir)
            self.assertEqual(len(failures), 1, failures)
            self.assertTrue(failures[0].startswith(f"{broken}: "), failures[0])

    def test_main_returns_1_for_a_broken_definition_and_0_when_clean(self):
        with tempfile.TemporaryDirectory() as tmp:
            github_dir = self._github_dir(tmp)
            workflow = github_dir / "workflows" / "pr.yml"
            workflow.write_text(DEDENTED_BLOCK_SCALAR_WORKFLOW)
            self.assertEqual(
                check_workflow_yaml.main(["--github-dir", str(github_dir)]), 1
            )

            workflow.write_text(VALID_WORKFLOW)
            self.assertEqual(
                check_workflow_yaml.main(["--github-dir", str(github_dir)]), 0
            )

    def test_main_returns_2_when_it_finds_nothing_to_check(self):
        """An empty result means the path is wrong, not that the repo is clean."""
        with tempfile.TemporaryDirectory() as tmp:
            github_dir = self._github_dir(tmp)
            self.assertEqual(
                check_workflow_yaml.main(["--github-dir", str(github_dir)]), 2
            )
            self.assertEqual(
                check_workflow_yaml.main(["--github-dir", str(Path(tmp) / "absent")]), 2
            )


class RepositoryTest(unittest.TestCase):
    def test_this_repositorys_definitions_are_all_well_formed(self):
        """Regression test for #12181: cargo_deny_advisories.yml must parse."""
        github_dir = Path(__file__).resolve().parents[1]
        self.assertEqual(check_workflow_yaml.check_definitions(github_dir), [])


if __name__ == "__main__":
    unittest.main()
