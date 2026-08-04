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


def _workflow_with_budgets(job_budget: str, step_budget: str) -> str:
    """A one-step workflow, with each budget line omitted when passed empty."""
    return "".join(
        [
            "---\non:\n  workflow_dispatch:\n\njobs:\n  gate:\n    runs-on: ubuntu-24.04\n",
            f"    timeout-minutes: {job_budget}\n" if job_budget else "",
            "    steps:\n      - name: Run the checks\n        run: echo hello\n",
            f"        timeout-minutes: {step_budget}\n" if step_budget else "",
        ]
    )


class CheckStepBudgetTest(unittest.TestCase):
    """#12340: a step budget its job's budget pre-empts can never fire."""

    def test_a_step_budget_below_the_jobs_is_accepted(self):
        self.assertEqual(
            check_workflow_yaml.check_workflow(_workflow_with_budgets("358", "355")), []
        )

    def test_a_step_budget_equal_to_the_jobs_is_reported(self):
        self.assertEqual(
            check_workflow_yaml.check_workflow(_workflow_with_budgets("358", "358")),
            [
                "job `gate` gives `Run the checks` a 358-minute budget that its own "
                "358-minute budget pre-empts"
            ],
        )

    def test_a_step_budget_above_the_jobs_is_reported(self):
        self.assertEqual(
            check_workflow_yaml.check_workflow(_workflow_with_budgets("358", "360")),
            [
                "job `gate` gives `Run the checks` a 360-minute budget that its own "
                "358-minute budget pre-empts"
            ],
        )

    def test_a_step_budget_without_a_job_budget_is_left_alone(self):
        """Common and legitimate: a short probe inside an otherwise unbounded job."""
        self.assertEqual(
            check_workflow_yaml.check_workflow(_workflow_with_budgets("", "1")), []
        )

    def test_a_job_budget_without_step_budgets_is_left_alone(self):
        self.assertEqual(
            check_workflow_yaml.check_workflow(_workflow_with_budgets("358", "")), []
        )

    def test_an_unnamed_step_is_reported_by_position(self):
        workflow = (
            "---\non:\n  workflow_dispatch:\n\njobs:\n  gate:\n"
            "    runs-on: ubuntu-24.04\n    timeout-minutes: 5\n"
            "    steps:\n      - run: echo first\n"
            "      - run: echo second\n        timeout-minutes: 5\n"
        )
        self.assertEqual(
            check_workflow_yaml.check_workflow(workflow),
            ["job `gate` gives `step 2` a 5-minute budget that its own 5-minute budget pre-empts"],
        )


def _workflow_with_conditions(job_if: str = "", step_if: str = "") -> str:
    """A one-step workflow, with each `if:` line omitted when passed empty."""
    return "".join(
        [
            "---\non:\n  workflow_dispatch:\n\njobs:\n  gate:\n    runs-on: ubuntu-24.04\n",
            f"    if: {job_if}\n" if job_if else "",
            "    steps:\n      - name: Run the checks\n        run: echo hello\n",
            f"        if: {step_if}\n" if step_if else "",
        ]
    )


class CheckConditionContextTest(unittest.TestCase):
    """#12396: an `if:` naming an unavailable context makes the file unschedulable."""

    def test_secrets_in_a_step_condition_is_reported_with_the_remedy(self):
        problems = check_workflow_yaml.check_workflow(
            _workflow_with_conditions(step_if="${{ secrets.SPICE_SECRET_HF_TOKEN != '' }}")
        )
        self.assertEqual(len(problems), 1, problems)
        self.assertIn("job `gate` step `Run the checks`", problems[0])
        self.assertIn("naming `secrets`", problems[0])
        self.assertIn("hoist the test into a job-level `env:`", problems[0])

    def test_secrets_in_a_job_condition_is_reported(self):
        problems = check_workflow_yaml.check_workflow(
            _workflow_with_conditions(job_if="${{ secrets.A != '' }}")
        )
        self.assertEqual(len(problems), 1, problems)
        self.assertIn("job `gate` has an `if:`", problems[0])

    def test_bracket_property_access_is_reported_like_dotted_access(self):
        """`secrets['A']` is the same unschedulable definition as `secrets.A`."""
        problems = check_workflow_yaml.check_workflow(
            _workflow_with_conditions(job_if="${{ secrets['A'] != '' }}")
        )
        self.assertEqual(len(problems), 1, problems)
        self.assertIn("naming `secrets`", problems[0])

    def test_bracket_access_in_a_step_condition_is_reported_with_the_remedy(self):
        problems = check_workflow_yaml.check_workflow(
            _workflow_with_conditions(step_if="${{ secrets['SPICE_SECRET_HF_TOKEN'] != '' }}")
        )
        self.assertEqual(len(problems), 1, problems)
        self.assertIn("job `gate` step `Run the checks`", problems[0])
        self.assertIn("hoist the test into a job-level `env:`", problems[0])

    def test_bracket_access_on_an_available_context_is_accepted(self):
        """The lookbehind must still keep `needs['x'].outputs` from reading as a use."""
        self.assertEqual(
            check_workflow_yaml.unavailable_contexts(
                "needs['setup'].outputs.matrix != ''",
                check_workflow_yaml.JOB_IF_CONTEXTS,
            ),
            [],
        )

    def test_a_context_name_inside_a_quoted_literal_is_not_a_reference(self):
        """A literal compared against is text, not property access on a context."""
        for condition in (
            "github.event.head_commit.message == 'env.READY'",
            "contains(github.ref, 'secrets.A')",
            "github.ref == 'matrix[0]'",
        ):
            with self.subTest(condition=condition):
                self.assertEqual(
                    check_workflow_yaml.unavailable_contexts(
                        condition, check_workflow_yaml.JOB_IF_CONTEXTS
                    ),
                    [],
                )

    def test_a_real_reference_beside_a_quoted_literal_is_still_reported(self):
        """Dropping literals must not swallow the reference next to them."""
        self.assertEqual(
            check_workflow_yaml.unavailable_contexts(
                "github.ref == 'env.READY' && secrets.A != ''",
                check_workflow_yaml.JOB_IF_CONTEXTS,
            ),
            ["secrets"],
        )

    def test_the_env_indirection_trunk_uses_is_accepted(self):
        self.assertEqual(
            check_workflow_yaml.check_workflow(
                _workflow_with_conditions(step_if="env.HAS_HF_SECRET == 'true'")
            ),
            [],
        )

    def test_env_is_rejected_in_a_job_condition_but_allowed_in_a_step_condition(self):
        """A job's `if:` is evaluated before the job has an environment."""
        self.assertEqual(
            check_workflow_yaml.check_workflow(
                _workflow_with_conditions(step_if="env.READY == 'true'")
            ),
            [],
        )
        problems = check_workflow_yaml.check_workflow(
            _workflow_with_conditions(job_if="env.READY == 'true'")
        )
        self.assertEqual(len(problems), 1, problems)
        self.assertIn("naming `env`", problems[0])

    def test_steps_matrix_and_runner_are_rejected_in_a_job_condition(self):
        for context, condition in (
            ("steps", "steps.probe.outputs.ok == 'true'"),
            ("matrix", "matrix.target == 'linux'"),
            ("runner", "runner.os == 'macOS'"),
        ):
            with self.subTest(context=context):
                problems = check_workflow_yaml.check_workflow(
                    _workflow_with_conditions(job_if=condition)
                )
                self.assertEqual(len(problems), 1, problems)
                self.assertIn(f"naming `{context}`", problems[0])

    def test_contexts_every_condition_may_name_are_accepted(self):
        for condition in (
            "github.event_name == 'push'",
            "needs.build.result == 'success'",
            "inputs.run_all_tests == 'true'",
            "vars.FLAG == '1'",
        ):
            with self.subTest(condition=condition):
                self.assertEqual(
                    check_workflow_yaml.check_workflow(
                        _workflow_with_conditions(job_if=condition, step_if=condition)
                    ),
                    [],
                )

    def test_a_hyphenated_job_name_is_not_read_as_a_context(self):
        """`e2e_test_ci.yml` has exactly this: `needs.setup-model-matrix.outputs.matrix`."""
        self.assertEqual(
            check_workflow_yaml.check_workflow(
                _workflow_with_conditions(
                    job_if="${{ needs.setup-model-matrix.outputs.matrix != '[]' }}"
                )
            ),
            [],
        )

    def test_a_dotted_path_after_an_allowed_context_is_not_read_as_a_context(self):
        self.assertEqual(
            check_workflow_yaml.check_workflow(
                _workflow_with_conditions(
                    step_if="github.event.pull_request.user.login == 'dependabot[bot]'"
                )
            ),
            [],
        )

    def test_every_unavailable_context_in_one_condition_is_named_once(self):
        problems = check_workflow_yaml.check_workflow(
            _workflow_with_conditions(job_if="env.A == 'x' && secrets.B != '' && env.C == 'y'")
        )
        self.assertEqual(len(problems), 1, problems)
        self.assertIn("naming `env`, `secrets`", problems[0])

    def test_an_unknown_dotted_word_is_left_alone(self):
        """Only real GitHub context names are reported, so a stray string cannot fail CI."""
        self.assertEqual(
            check_workflow_yaml.unavailable_contexts(
                "contains(github.ref, 'refs/heads/release.candidate')",
                check_workflow_yaml.JOB_IF_CONTEXTS,
            ),
            [],
        )

    def test_a_condition_without_any_if_key_is_not_invented(self):
        self.assertEqual(check_workflow_yaml.check_workflow(_workflow_with_conditions()), [])

    def test_secrets_in_a_composite_action_step_condition_is_reported(self):
        action = (
            "name: Set up thing\ndescription: d\n\nruns:\n  using: composite\n  steps:\n"
            "    - name: Configure\n      shell: bash\n      if: secrets.TOKEN != ''\n"
            "      run: echo hello\n"
        )
        problems = check_workflow_yaml.check_action(action)
        self.assertEqual(len(problems), 1, problems)
        self.assertIn("step `Configure`", problems[0])
        self.assertIn("naming `secrets`", problems[0])

    def test_a_composite_action_step_may_name_inputs_steps_and_runner(self):
        action = (
            "name: Set up thing\ndescription: d\n\nruns:\n  using: composite\n  steps:\n"
            "    - name: Configure\n      shell: bash\n"
            "      if: runner.os == 'macOS' && inputs.enabled == 'true' && "
            "steps.probe.outputs.ok == '1'\n      run: echo hello\n"
        )
        self.assertEqual(check_workflow_yaml.check_action(action), [])


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
