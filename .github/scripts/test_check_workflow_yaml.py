"""Unit tests for check_workflow_yaml.py."""
import os
import sys
import tempfile
import unittest
from pathlib import Path

import yaml

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


def _nightly_with_suites(*suites: tuple[str, str], trigger: str = "schedule") -> str:
    """A scheduled job running each `(name, if)` suite in turn; empty `if` omits it."""
    header = (
        "---\non:\n"
        + ("  schedule:\n    - cron: '0 0 * * *'\n" if trigger == "schedule" else "")
        + ("  workflow_dispatch:\n" if trigger != "schedule" else "")
        + "\njobs:\n  nightly:\n    runs-on: ubuntu-24.04\n    steps:\n"
    )
    body = "".join(
        f"      - name: {name}\n"
        + (f"        if: {condition}\n" if condition else "")
        + "        run: cargo nextest run -- suite\n"
        for name, condition in suites
    )
    return header + body


def _hidden(label: str) -> str:
    return (
        f"job `nightly` lets an earlier suite's failure skip `{label}`; give it "
        "`if: ${{ !cancelled() && <its existing condition> }}` so the job's "
        "conclusion reports every suite"
    )


CREDENTIALED = """\
---
name: Bench
on:
  workflow_dispatch:
    inputs:
      spicepod_path:
        type: string
jobs:
  bench:
    runs-on: ubuntu-24.04
    environment: ci
    permissions:
      contents: read
      id-token: write
    steps:
{steps}
"""


class CheckUntrustedExpansionsTest(unittest.TestCase):
    def problems(self, steps, document=CREDENTIALED):
        return [p for p in check_workflow_yaml.check_workflow(document.format(steps=steps)) if "expands" in p]

    def test_a_dispatch_input_in_a_script_is_reported(self):
        problems = self.problems(
            "      - name: Run\n        run: testoperator -p ${{ github.event.inputs.spicepod_path }}"
        )
        self.assertEqual(len(problems), 1)
        self.assertIn(
            "job `bench` step `Run` expands `${{ github.event.inputs.spicepod_path }}`", problems[0]
        )

    def test_every_untrusted_context_is_reported(self):
        for expression in (
            "inputs.spicepod_path",
            "github.ref_name",
            "github.head_ref",
            "github.event.pull_request.title",
            "github.event.inputs.x != '' && format('--x {0}', github.event.inputs.x) || ''",
        ):
            with self.subTest(expression=expression):
                self.assertEqual(len(self.problems(f"      - run: echo \"${{{{ {expression} }}}}\"")), 1)

    def test_a_github_script_is_checked_too(self):
        steps = (
            "      - uses: actions/github-script@v7\n"
            "        with:\n"
            "          script: core.info('${{ inputs.spicepod_path }}')"
        )
        self.assertEqual(len(self.problems(steps)), 1)

    def test_the_value_through_env_is_accepted(self):
        steps = (
            "      - env:\n"
            "          INPUT_SPICEPOD_PATH: ${{ github.event.inputs.spicepod_path }}\n"
            "        run: testoperator -p \"$INPUT_SPICEPOD_PATH\""
        )
        self.assertEqual(self.problems(steps), [])

    def test_trusted_contexts_and_non_script_keys_are_accepted(self):
        steps = (
            "      - name: Run ${{ github.event.inputs.spicepod_path }}\n"
            "        if: ${{ contains(github.event.inputs.spicepod_path, 'mysql') }}\n"
            "        with:\n"
            "          path: ${{ inputs.spicepod_path }}\n"
            "        uses: ./.github/actions/x\n"
            "      - run: echo ${{ github.sha }} ${{ runner.temp }} ${{ secrets.TOKEN }}"
            " ${{ steps.a.outputs.b }}"
        )
        self.assertEqual(self.problems(steps), [])

    def test_a_workflow_without_id_token_is_left_alone(self):
        document = CREDENTIALED.replace("      id-token: write\n", "")
        self.assertEqual(self.problems("      - run: echo ${{ inputs.spicepod_path }}", document), [])

    def test_write_all_at_the_workflow_level_counts_as_holding_the_token(self):
        document = CREDENTIALED.replace(
            "    permissions:\n      contents: read\n      id-token: write\n", ""
        ).replace("jobs:\n", "permissions: write-all\njobs:\n")
        self.assertEqual(len(self.problems("      - run: echo ${{ inputs.spicepod_path }}", document)), 1)

    def test_a_step_output_built_from_an_input_is_followed(self):
        # The step reads the input from env as $X, so only its output carries it on.
        steps = (
            "      - id: path\n"
            "        env:\n"
            "          RAW: ${{ inputs.spicepod_path }}\n"
            "        run: echo \"p=./test/$RAW\" >> \"$GITHUB_OUTPUT\"\n"
            "      - run: test -f \"${{ steps.path.outputs.p }}\""
        )
        problems = self.problems(steps)
        self.assertEqual(len(problems), 1)
        self.assertIn("`${{ steps.path.outputs.p }}`", problems[0])

    def test_a_job_env_holding_an_input_is_followed_through_shell_and_env_reads(self):
        document = CREDENTIALED.replace(
            "    steps:\n", "    env:\n      CB_PATH: ${{{{ inputs.spicepod_path }}}}\n    steps:\n"
        )
        steps = (
            "      - id: set\n"
            "        run: echo \"p=$CB_PATH\" >> \"$GITHUB_OUTPUT\"\n"
            "      - run: echo \"${{ steps.set.outputs.p }}\" \"${{ env.CB_PATH }}\""
        )
        self.assertEqual(len(self.problems(steps, document)), 2)

    def test_a_job_output_and_a_matrix_built_from_an_input_are_followed(self):
        document = CREDENTIALED + (
            "    outputs:\n"
            "      p: ${{{{ inputs.spicepod_path }}}}\n"
            "  use:\n"
            "    needs: bench\n"
            "    runs-on: ubuntu-24.04\n"
            "    strategy:\n"
            "      matrix:\n"
            "        p: ${{{{ fromJson(needs.bench.outputs.p) }}}}\n"
            "    steps:\n"
            "      - run: echo ${{{{ needs.bench.outputs.p }}}} ${{{{ matrix.p }}}}\n"
        )
        self.assertEqual(len(self.problems("      - run: echo ok", document)), 2)

    def test_a_long_chain_of_job_outputs_declared_last_first_is_followed(self):
        # Each pass of the fixpoint carries the value one job further when the jobs are
        # declared after the job that reads them, so the chain needs a pass per link.
        # Doubled braces twice over: an f-string, then str.format.
        hops = 20
        jobs = (
            "  sink:\n"
            f"    needs: j{hops}\n"
            "    runs-on: ubuntu-24.04\n"
            "    steps:\n"
            f"      - run: echo ${{{{{{{{ needs.j{hops}.outputs.v }}}}}}}}\n"
        )
        for k in range(hops, 0, -1):
            source = "inputs.spicepod_path" if k == 1 else f"needs.j{k - 1}.outputs.v"
            jobs += (
                f"  j{k}:\n"
                + (f"    needs: j{k - 1}\n" if k > 1 else "")
                + "    runs-on: ubuntu-24.04\n"
                "    outputs:\n"
                f"      v: ${{{{{{{{ {source} }}}}}}}}\n"
                "    steps:\n"
                "      - run: echo ok\n"
            )
        problems = self.problems("      - run: echo ok", CREDENTIALED + jobs)
        self.assertEqual(len(problems), 1, "\n".join(problems))
        self.assertIn("job `sink`", problems[0])

    def test_bracket_syntax_and_json_dumps_are_reported(self):
        expressions = ("inputs['spicepod_path']", "github['head_ref']", "toJSON(github.event)", "github.ref")
        for expression in expressions:
            with self.subTest(expression=expression):
                self.assertEqual(len(self.problems(f"      - run: echo '${{{{ {expression} }}}}'")), 1)

    def test_a_whole_context_dump_is_reported(self):
        dumps = ("toJSON(inputs)", "inputs", "toJSON(github.event)", "github.event", "toJSON(github)")
        for expression in dumps:
            with self.subTest(expression=expression):
                self.assertEqual(len(self.problems(f"      - run: printf '%s' '${{{{ {expression} }}}}'")), 1)

    def test_bracket_reads_match_their_dot_forms(self):
        for expression in (
            "github.event['inputs'].spicepod_path",
            "github['event']['inputs']['spicepod_path']",
            "github['ref_name']",
            "inputs[matrix.key]",
        ):
            with self.subTest(expression=expression):
                self.assertEqual(len(self.problems(f"      - run: echo \"${{{{ {expression} }}}}\"")), 1)

    def test_bracket_reads_of_tainted_env_steps_needs_and_matrix_are_followed(self):
        document = CREDENTIALED.replace(
            "    steps:\n", "    env:\n      CB_PATH: ${{{{ inputs.spicepod_path }}}}\n    steps:\n"
        ) + (
            "    outputs:\n"
            "      p: ${{{{ inputs.spicepod_path }}}}\n"
            "  use:\n"
            "    needs: bench\n"
            "    runs-on: ubuntu-24.04\n"
            "    strategy:\n"
            "      matrix:\n"
            "        p: ${{{{ fromJson(needs['bench'].outputs['p']) }}}}\n"
            "    steps:\n"
            "      - run: echo ${{{{ needs['bench'].outputs['p'] }}}}"
            " ${{{{ needs.bench.outputs[matrix.k] }}}}\n"
            "      - run: echo ${{{{ matrix['p'] }}}}\n"
        )
        steps = (
            "      - id: set\n"
            "        run: echo \"p=$CB_PATH\" >> \"$GITHUB_OUTPUT\"\n"
            "      - run: echo \"${{ env['CB_PATH'] }}\" \"${{ steps['set'].outputs['p'] }}\""
            " \"${{ env[matrix.k] }}\""
        )
        problems = self.problems(steps, document)
        self.assertEqual(len(problems), 6, "\n".join(problems))

    def test_brackets_in_separate_literals_do_not_hide_a_read_between_them(self):
        steps = "      - run: echo \"${{ format('{0}{1}{2}', '[', inputs.spicepod_path, ']') }}\""
        self.assertEqual(len(self.problems(steps)), 1)

    def test_closing_braces_inside_a_literal_do_not_end_the_expansion(self):
        # GitHub ends `${{ }}` at the first `}}` outside a string literal.
        with self.subTest(where="script"):
            steps = "      - run: echo '${{ format('{{\"k\":\"{0}\"}}', inputs.spicepod_path) }}'"
            self.assertEqual(len(self.problems(steps)), 1)
        with self.subTest(where="env source"):
            document = CREDENTIALED.replace(
                "    steps:\n",
                "    env:\n"
                "      J: ${{{{ format('{{{{\"k\":\"{{0}}\"}}}}', inputs.spicepod_path) }}}}\n"
                "    steps:\n",
            )
            self.assertEqual(len(self.problems("      - run: echo \"${{ env.J }}\"", document)), 1)

    def test_string_literals_naming_a_context_are_not_a_read(self):
        steps = (
            "      - run: echo \"${{ format('github {0} inputs', github.sha) }}\""
            " \"${{ contains(github.event_name, 'github.event') && 'x' || 'y' }}\""
        )
        self.assertEqual(self.problems(steps), [])

    def test_a_github_script_step_output_is_not_followed(self):
        steps = (
            "      - id: m\n"
            "        uses: actions/github-script@v7\n"
            "        with:\n"
            "          script: return context.payload.inputs.x === 'a' ? 'one' : 'two'\n"
            "      - run: echo ${{ steps.m.outputs.result }}"
        )
        self.assertEqual(self.problems(steps), [])

    def test_a_job_without_the_token_in_a_credentialed_workflow_is_checked(self):
        document = CREDENTIALED + (
            "  setup:\n"
            "    runs-on: ubuntu-24.04\n"
            "    steps:\n"
            # Doubled braces: the document goes through str.format.
            "      - run: echo ${{{{ inputs.spicepod_path }}}}\n"
        )
        problems = self.problems("      - run: echo ok", document)
        self.assertEqual(len(problems), 1)
        self.assertIn("job `setup`", problems[0])


REUSABLE_CI_ROLE = """\
---
on:
  workflow_call:
    inputs:
      spiced_commit:
        type: string
        default: ''
      runner_type:
        type: string
        required: true
jobs:
  bench:
    runs-on: ubuntu-24.04
    environment: ci
    permissions:
      id-token: write
    steps:
{steps}
"""

CHOOSE = (
    "      - name: Choose the CI role\n"
    "        id: ci-role\n"
    f"        uses: {check_workflow_yaml.CI_ROLE_ACTION}\n"
    "        with:\n"
    "          pull-request-role-set: ${{ secrets.CI_AWS_PULL_REQUEST_ROLE_ARN != '' }}\n"
    "          builds: |\n"
    "            spiced_commit=${{ inputs.spiced_commit }}\n"
)
ASSUME = (
    "      - uses: aws-actions/configure-aws-credentials@e1253824e5c10ff9df46874f81ed3ec929e19cfd\n"
    "        with:\n"
    "          role-to-assume: {role}\n"
)
CHOSEN = "${{ steps.ci-role.outputs.full == 'true' && secrets.CI_AWS_DYNAMODB_ROLE_ARN || secrets.CI_AWS_PULL_REQUEST_ROLE_ARN }}"


class CheckCiRoleJobsTest(unittest.TestCase):
    def problems(self, steps, document=REUSABLE_CI_ROLE):
        return [p for p in check_workflow_yaml.check_workflow(document.replace("{steps}", steps)) if "role" in p]

    def test_a_chosen_role_in_a_reusable_workflow_is_accepted(self):
        self.assertEqual(self.problems(CHOOSE + ASSUME.format(role=CHOSEN)), [])

    def test_a_ci_role_job_in_a_dispatched_workflow_is_reported(self):
        document = REUSABLE_CI_ROLE.replace("  workflow_call:\n", "  workflow_dispatch:\n")
        problems = self.problems(CHOOSE + ASSUME.format(role=CHOSEN), document)
        self.assertEqual(len(problems), 1)
        self.assertIn("job `bench` assumes a CI AWS role in a workflow with triggers of its own", problems[0])

    def test_a_reusable_workflow_that_also_has_its_own_trigger_is_reported(self):
        document = REUSABLE_CI_ROLE.replace("on:\n", "on:\n  push:\n")
        self.assertEqual(len(self.problems(CHOOSE + ASSUME.format(role=CHOSEN), document)), 1)

    def test_the_pull_request_role_alone_makes_no_ci_role_job(self):
        document = REUSABLE_CI_ROLE.replace("  workflow_call:\n", "  workflow_dispatch:\n")
        steps = ASSUME.format(role="${{ secrets.CI_AWS_PULL_REQUEST_ROLE_ARN }}")
        self.assertEqual(self.problems(steps, document), [])

    def test_a_job_that_does_not_choose_its_role_first_is_reported(self):
        for steps in (ASSUME.format(role=CHOSEN), "      - run: echo first\n" + CHOOSE + ASSUME.format(role=CHOSEN)):
            with self.subTest(steps=steps[:30]):
                problems = self.problems(steps)
                self.assertEqual(len(problems), 1)
                self.assertIn("does not choose it first", problems[0])

    def test_a_role_not_chosen_by_the_first_step_is_reported(self):
        for role in (
            "${{ secrets.CI_AWS_DYNAMODB_ROLE_ARN }}",
            "${{ steps.ci-role.outputs.full != 'true' && secrets.CI_AWS_PULL_REQUEST_ROLE_ARN || secrets.CI_AWS_DYNAMODB_ROLE_ARN }}",
            "${{ env.FULL == 'true' && secrets.CI_AWS_DYNAMODB_ROLE_ARN || secrets.CI_AWS_PULL_REQUEST_ROLE_ARN }}",
        ):
            with self.subTest(role=role):
                problems = self.problems(CHOOSE + ASSUME.format(role=role))
                self.assertEqual(len(problems), 1, problems)
                self.assertIn("sets `role-to-assume:", problems[0])

    def test_an_artifact_upload_needs_a_gate(self):
        upload = (
            "      - name: Upload\n{cond}        run: mc cp target/release/x \"spice-minio/artifacts/x/$SHA/x\"\n"
            "      - name: Download\n        run: mc cp \"spice-minio/artifacts/x/$SHA/x\" target/x\n"
            f"      - name: Verify\n        uses: {check_workflow_yaml.VERIFY_ACTION}\n"
            "        with:\n          file: target/x\n          signer-workflow: .github/workflows/x.yml\n"
        )
        steps = CHOOSE + ASSUME.format(role=CHOSEN)
        with self.subTest(gated=False):
            problems = self.problems(steps + upload.format(cond=""))
            self.assertEqual(len(problems), 1, problems)
            self.assertIn("step `Upload` writes to shared state", problems[0])
        with self.subTest(gated=True):
            cond = "        if: env.CI_CACHE_READ_ONLY != 'true'\n"
            self.assertEqual(self.problems(steps + upload.format(cond=cond)), [])
        with self.subTest(wrapper=True):
            wrapped = "      - name: Upload\n        run: bash .github/scripts/mc_cp.sh out/x spice-minio/artifacts/x\n"
            self.assertEqual(len(self.problems(steps + wrapped)), 1)

    def test_a_repository_write_is_not_gated_by_the_environment_flag(self):
        # Any earlier step can rewrite CI_CACHE_READ_ONLY through GITHUB_ENV; the step output cannot change.
        write = (
            "      - name: Push snapshots\n        if: env.CI_CACHE_READ_ONLY != 'true'\n"
            "        uses: ./.github/actions/push-snap-changes\n"
        )
        problems = self.problems(CHOOSE + ASSUME.format(role=CHOSEN) + write)
        self.assertEqual(len(problems), 1, problems)
        script = (
            "      - name: Open the pull request\n        uses: actions/github-script@v9\n"
            "        with:\n          script: require('child_process').execSync('git push origin x')\n"
        )
        self.assertEqual(len(self.problems(CHOOSE + ASSUME.format(role=CHOSEN) + script)), 1)

    def test_a_store_download_must_be_verified_before_it_runs(self):
        download = (
            "      - name: Download\n"
            "        run: bash .github/scripts/mc_cp.sh \"spice-minio/build/${CI_STORE_PREFIX?}x.tar.zst\" ./x.tar.zst\n"
            "      - name: Fallback\n        uses: actions/download-artifact@v8\n"
        )
        verify = (
            "      - name: Verify\n        if: steps.ci-role.outputs.full == 'true'\n"
            f"        uses: {check_workflow_yaml.VERIFY_ACTION}\n"
            "        with:\n          file: ./x.tar.zst\n          signer-workflow: .github/workflows/x.yml\n"
        )
        run = "      - name: Run\n        run: tar -xf x.tar.zst && ./x\n"
        steps = CHOOSE + ASSUME.format(role=CHOSEN)
        self.assertEqual(self.problems(steps + download + verify + run), [])
        problems = self.problems(steps + download + run + verify)
        self.assertEqual(len(problems), 1, problems)
        self.assertIn("step `Download` downloads from the CI object store", problems[0])

    def test_spiced_and_verify_steps_follow_the_role_steps_output(self):
        steps = CHOOSE + ASSUME.format(role=CHOSEN)
        spiced = f"      - name: Setup spiced\n        uses: {check_workflow_yaml.SETUP_SPICED}\n"
        with self.subTest(case="setup-spiced without verify"):
            problems = self.problems(steps + spiced)
            self.assertEqual(len(problems), 1, problems)
            self.assertIn("runs setup-spiced without", problems[0])
        with self.subTest(case="setup-spiced with verify"):
            ok = spiced + "        with:\n          verify: ${{ steps.ci-role.outputs.full }}\n"
            self.assertEqual(self.problems(steps + ok), [])
        with self.subTest(case="verify gated on the environment"):
            verify = (
                "      - name: Verify\n        if: env.CI_ROLE_FULL == 'true'\n"
                f"        uses: {check_workflow_yaml.VERIFY_ACTION}\n"
            )
            problems = self.problems(steps + verify)
            self.assertEqual(len(problems), 1, problems)
            self.assertIn("on no `env` value", problems[0])
        with self.subTest(case="verify gated on the role and an input"):
            verify = (
                "      - name: Verify\n        if: steps.ci-role.outputs.full == 'true' && inputs.x == 'true'\n"
                f"        uses: {check_workflow_yaml.VERIFY_ACTION}\n"
            )
            self.assertEqual(self.problems(steps + verify), [])

    def test_an_image_input_goes_to_images_not_builds(self):
        document = REUSABLE_CI_ROLE.replace(
            "      runner_type:\n", "      custom_image_tag:\n        type: string\n      runner_type:\n"
        )
        as_build = CHOOSE + "            custom_image_tag=${{ inputs.custom_image_tag }}\n"
        problems = self.problems(as_build + ASSUME.format(role=CHOSEN), document)
        self.assertEqual(len(problems), 1, problems)
        self.assertIn("does not give it to the `ci-role` step's `images`", problems[0])
        as_image = CHOOSE + "          images: |\n            custom_image_tag=${{ inputs.custom_image_tag }}\n"
        self.assertEqual(self.problems(as_image + ASSUME.format(role=CHOSEN), document), [])

    def test_a_build_input_the_role_step_is_not_given_is_reported(self):
        steps = CHOOSE.replace("          builds: |\n            spiced_commit=${{ inputs.spiced_commit }}\n", "")
        problems = self.problems(steps + ASSUME.format(role=CHOSEN))
        self.assertEqual(len(problems), 1)
        self.assertIn("takes input `spiced_commit`, which names another build", problems[0])

    def test_a_repository_write_needs_the_full_roles(self):
        write = "      - name: Push snapshots\n{cond}        uses: ./.github/actions/push-snap-changes\n"
        with self.subTest(gated=False):
            problems = self.problems(CHOOSE + ASSUME.format(role=CHOSEN) + write.format(cond=""))
            self.assertEqual(len(problems), 1)
            self.assertIn("step `Push snapshots` writes to shared state", problems[0])
        with self.subTest(gated=True):
            cond = "        if: ${{ always() && steps.ci-role.outputs.full == 'true' }}\n"
            self.assertEqual(self.problems(CHOOSE + ASSUME.format(role=CHOSEN) + write.format(cond=cond)), [])


class CheckAttestedBuildsTest(unittest.TestCase):
    DOC = (
        "on: push\njobs:\n  build:\n    runs-on: ubuntu-24.04\n    steps:\n"
        "      - name: Set up sccache\n{cond}        uses: $/.github/actions/setup-sccache\n"
        "      - run: make\n"
        "      - name: Attest\n        uses: actions/attest-build-provenance@v4\n"
        "        with:\n          subject-path: out\n"
    )

    def problems(self, cond):
        text = self.DOC.replace("{cond}", cond)
        return [p for p in check_workflow_yaml.check_workflow(text) if "shared build cache" in p]

    def test_a_cache_on_trunk_in_an_attesting_job_is_reported(self):
        for cond in ("", "        if: runner.os == 'macOS'\n"):
            with self.subTest(cond=cond):
                self.assertEqual(len(self.problems(cond)), 1)

    def test_a_cache_kept_off_trunk_is_accepted(self):
        self.assertEqual(self.problems("        if: runner.os == 'macOS' && github.ref != 'refs/heads/trunk'\n"), [])

    def test_a_job_that_attests_nothing_may_use_the_cache(self):
        text = self.DOC.replace("{cond}", "").split("      - name: Attest")[0]
        self.assertEqual([p for p in check_workflow_yaml.check_workflow(text) if "shared build cache" in p], [])


class CheckCiRoleCallsTest(unittest.TestCase):
    REUSABLE = yaml.safe_load(REUSABLE_CI_ROLE.replace("{steps}", CHOOSE + ASSUME.format(role=CHOSEN)))

    def caller(self, uses, with_="      runner_type: x\n"):
        return yaml.safe_load(
            "on:\n  workflow_dispatch:\njobs:\n  call:\n"
            f"    uses: {uses}\n" + ("    with:\n" + with_ if with_ else "")
        )

    def problems(self, workflows):
        return check_workflow_yaml.check_ci_role_calls(workflows)

    def test_a_call_at_trunk_is_accepted(self):
        calls = {"x_aws.yml": self.REUSABLE, "x.yml": self.caller("spiceai/spiceai/.github/workflows/x_aws.yml@trunk")}
        self.assertEqual(self.problems(calls), [])

    def test_a_call_at_any_other_ref_or_from_the_working_tree_is_reported(self):
        for uses in ("./.github/workflows/x_aws.yml", "spiceai/spiceai/.github/workflows/x_aws.yml@main"):
            with self.subTest(uses=uses):
                problems = self.problems({"x_aws.yml": self.REUSABLE, "x.yml": self.caller(uses)})
                self.assertEqual(len(problems), 1, problems)
                self.assertIn("spiceai/spiceai/.github/workflows/x_aws.yml@trunk`", problems[0])

    def test_the_merge_queue_callers_use_their_local_file(self):
        local = {"integration_aws.yml": self.REUSABLE}
        ok = dict(local, **{"integration.yml": self.caller("./.github/workflows/integration_aws.yml")})
        self.assertEqual(self.problems(ok), [])
        at_trunk = dict(local, **{"integration.yml": self.caller("spiceai/spiceai/.github/workflows/integration_aws.yml@trunk")})
        problems = self.problems(at_trunk)
        self.assertEqual(len(problems), 1)
        self.assertIn("is a required merge-queue check", problems[0])

    def test_inputs_are_checked_against_the_called_file(self):
        uses = "spiceai/spiceai/.github/workflows/x_aws.yml@trunk"
        problems = self.problems({"x_aws.yml": self.REUSABLE, "x.yml": self.caller(uses, "      typo: y\n")})
        self.assertEqual(
            sorted(p.split(": ", 1)[1] for p in problems),
            ["job `call` does not pass `runner_type`, which x_aws.yml requires",
             "job `call` passes input `typo`, which x_aws.yml does not declare"],
        )

    def test_a_call_at_trunk_to_a_missing_file_is_reported(self):
        problems = self.problems({"x.yml": self.caller("spiceai/spiceai/.github/workflows/gone.yml@trunk")})
        self.assertEqual(len(problems), 1)
        self.assertIn("there is no .github/workflows/gone.yml", problems[0])


class CheckActionExpansionsTest(unittest.TestCase):
    ACTION = """\
name: Setup
description: Sets up a thing
inputs:
  ref:
    description: The ref
runs:
  using: composite
  steps:
{steps}
"""

    def problems(self, steps):
        problems = check_workflow_yaml.check_action(self.ACTION.format(steps=steps))
        return [p for p in problems if "expands" in p]

    def test_an_input_in_an_action_script_is_reported(self):
        problems = self.problems("    - shell: bash\n      run: git fetch origin ${{ inputs.ref }}")
        self.assertEqual(len(problems), 1)
        self.assertIn("a caller's value", problems[0])

    def test_an_input_through_env_is_accepted(self):
        steps = (
            "    - shell: bash\n"
            "      env:\n"
            "        REF: ${{ inputs.ref }}\n"
            "      run: git fetch origin \"$REF\""
        )
        self.assertEqual(self.problems(steps), [])

    def test_the_repository_actions_are_clean(self):
        github_dir = Path(__file__).resolve().parent.parent
        for action in check_workflow_yaml.action_files(github_dir):
            with self.subTest(action=action.name):
                problems = [p for p in check_workflow_yaml.check_action(action.read_text()) if "expands" in p]
                self.assertEqual(problems, [])


class CheckSuiteVisibilityTest(unittest.TestCase):
    """#12625: a nightly's first failing suite skips every suite behind it."""

    def test_the_first_suite_needs_no_condition(self):
        """Nothing precedes it, so nothing can hide it."""
        self.assertEqual(
            check_workflow_yaml.check_workflow(_nightly_with_suites(("Run A", ""))), []
        )

    def test_a_later_suite_with_no_condition_is_reported(self):
        self.assertEqual(
            check_workflow_yaml.check_workflow(
                _nightly_with_suites(("Run A", ""), ("Run B", ""))
            ),
            [_hidden("Run B")],
        )

    def test_a_later_suite_gated_only_on_a_secret_is_reported(self):
        """The shape the nightly actually had: a condition that `success()` still gates."""
        self.assertEqual(
            check_workflow_yaml.check_workflow(
                _nightly_with_suites(("Run A", ""), ("Run B", "env.HAS_KEY == 'true'"))
            ),
            [_hidden("Run B")],
        )

    def test_every_hidden_suite_is_reported(self):
        self.assertEqual(
            check_workflow_yaml.check_workflow(
                _nightly_with_suites(("Run A", ""), ("Run B", ""), ("Run C", ""))
            ),
            [_hidden("Run B"), _hidden("Run C")],
        )

    def test_a_not_cancelled_guard_is_accepted(self):
        self.assertEqual(
            check_workflow_yaml.check_workflow(
                _nightly_with_suites(
                    ("Run A", ""),
                    ("Run B", "${{ !cancelled() && env.HAS_KEY == 'true' }}"),
                )
            ),
            [],
        )

    def test_an_always_guard_is_accepted(self):
        """Weaker than `!cancelled()` — it also survives cancellation — but sufficient."""
        self.assertEqual(
            check_workflow_yaml.check_workflow(
                _nightly_with_suites(("Run A", ""), ("Run B", "${{ always() }}"))
            ),
            [],
        )

    def test_a_status_function_inside_a_quoted_literal_does_not_count(self):
        """Comparing against the text `'always()'` is not a call to it."""
        self.assertEqual(
            check_workflow_yaml.check_workflow(
                _nightly_with_suites(
                    ("Run A", ""), ("Run B", "${{ env.MODE == 'always()' }}")
                )
            ),
            [_hidden("Run B")],
        )

    def test_a_real_status_function_beside_a_quoted_literal_is_accepted(self):
        """Dropping the literal must not take the genuine call out with it."""
        self.assertEqual(
            check_workflow_yaml.check_workflow(
                _nightly_with_suites(
                    ("Run A", ""),
                    ("Run B", "${{ !cancelled() && env.MODE == 'always()' }}"),
                )
            ),
            [],
        )

    def test_a_pr_gate_is_left_alone(self):
        """Stopping at the first failure is the point of a gate; this only covers nightlies."""
        self.assertEqual(
            check_workflow_yaml.check_workflow(
                _nightly_with_suites(("Run A", ""), ("Run B", ""), trigger="dispatch")
            ),
            [],
        )

    def test_a_step_that_runs_no_suite_is_left_alone(self):
        """Setup and teardown steps are not results the run exists to report."""
        workflow = (
            "---\non:\n  schedule:\n    - cron: '0 0 * * *'\n\njobs:\n  nightly:\n"
            "    runs-on: ubuntu-24.04\n    steps:\n"
            "      - name: Run A\n        run: cargo nextest run -- suite\n"
            "      - name: Tidy up\n        run: rm -rf ./scratch\n"
        )
        self.assertEqual(check_workflow_yaml.check_workflow(workflow), [])

    def test_a_snapshot_push_behind_a_suite_is_reported(self):
        """The refresh is what a nightly exists to produce, so a red suite must not skip it."""
        workflow = (
            "---\non:\n  schedule:\n    - cron: '0 0 * * *'\n\njobs:\n  nightly:\n"
            "    runs-on: ubuntu-24.04\n    steps:\n"
            "      - name: Run A\n        run: cargo nextest run -- suite\n"
            "      - name: Push snapshots to branch\n"
            "        if: github.event_name == 'schedule'\n"
            "        uses: ./.github/actions/push-snap-changes\n"
        )
        self.assertEqual(
            check_workflow_yaml.check_workflow(workflow),
            [_hidden("Push snapshots to branch")],
        )

    def test_an_unnamed_suite_is_reported_by_position(self):
        workflow = (
            "---\non:\n  schedule:\n    - cron: '0 0 * * *'\n\njobs:\n  nightly:\n"
            "    runs-on: ubuntu-24.04\n    steps:\n"
            "      - run: cargo test --workspace\n"
            "      - run: cargo test -p spice\n"
        )
        self.assertEqual(check_workflow_yaml.check_workflow(workflow), [_hidden("step 2")])


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

    def test_an_expression_in_an_input_description_is_reported(self):
        # Every job that used install-spice failed in `Set up job` with "Unrecognized
        # named-value: 'runner'" when an input's description showed `${{ runner.temp }}`.
        action = VALID_ACTION.replace(
            "runs:",
            "inputs:\n  install-dir:\n    description: 'such as ${{ runner.temp }}/spice/bin'\nruns:",
            1,
        )
        problems = check_workflow_yaml.check_action(action)
        self.assertEqual(len(problems), 1, problems)
        self.assertIn("the description of input `install-dir` contains `${{`", problems[0])

    def test_expressions_in_output_values_are_allowed(self):
        action = VALID_ACTION.replace(
            "runs:",
            "outputs:\n  path:\n    description: 'Where it went'\n    value: ${{ steps.x.outputs.path }}\nruns:",
            1,
        )
        self.assertEqual(check_workflow_yaml.check_action(action), [])

    def test_a_composite_action_step_may_name_inputs_steps_and_runner(self):
        action = (
            "name: Set up thing\ndescription: d\n\nruns:\n  using: composite\n  steps:\n"
            "    - name: Configure\n      shell: bash\n"
            "      if: runner.os == 'macOS' && inputs.enabled == 'true' && "
            "steps.probe.outputs.ok == '1'\n      run: echo hello\n"
        )
        self.assertEqual(check_workflow_yaml.check_action(action), [])


def _workflow_running_the_runtime(
    command: str = "cargo nextest run --archive-file ./integration.tar.zst -- openai_test",
    prelude: str = "",
    with_cache_setup: bool = True,
) -> str:
    """A job that sets up the compiler cache and then runs the Spice runtime."""
    return "".join(
        [
            "---\non:\n  workflow_dispatch:\n\njobs:\n  test-models:\n",
            "    runs-on: spiceai-macos\n    steps:\n",
            (
                "      - name: Set up spiceio\n"
                "        uses: spiceai/spiceio/.github/actions/setup@0870da5\n"
                if with_cache_setup
                else ""
            ),
            "      - name: Run OpenAI integration test\n        run: |\n",
            f"          {prelude}\n" if prelude else "",
            f"          {command}\n",
        ]
    )


class CheckCacheEndpointIsolationTest(unittest.TestCase):
    """#12624: the cache's `AWS_ENDPOINT_URL` redirects the runtime's S3 traffic."""

    EXPECTED = (
        "job `test-models` step `Run OpenAI integration test` runs the Spice runtime after "
        "`spiceio/.github/actions/setup` has exported `AWS_ENDPOINT_URL`, which sends every "
        "`s3://` dataset to the compiler cache — start the step with `unset AWS_ENDPOINT_URL`"
    )

    def test_a_test_run_after_the_cache_setup_is_reported(self):
        """The shape trunk shipped on 2026-08-05, before the guard existed."""
        self.assertEqual(
            check_workflow_yaml.check_workflow(_workflow_running_the_runtime()),
            [self.EXPECTED],
        )

    def test_dropping_the_endpoint_first_is_accepted(self):
        self.assertEqual(
            check_workflow_yaml.check_workflow(
                _workflow_running_the_runtime(prelude="unset AWS_ENDPOINT_URL")
            ),
            [],
        )

    def test_a_spice_run_is_covered_too(self):
        """The CLI reads the same environment as the archived test binary."""
        self.assertEqual(
            check_workflow_yaml.check_workflow(
                _workflow_running_the_runtime(command="spice run &")
            ),
            [self.EXPECTED],
        )

    def test_a_job_without_the_cache_setup_is_left_alone(self):
        self.assertEqual(
            check_workflow_yaml.check_workflow(
                _workflow_running_the_runtime(with_cache_setup=False)
            ),
            [],
        )

    def test_building_the_archive_still_gets_the_cache(self):
        """`nextest archive` compiles — that is exactly what the endpoint is for."""
        self.assertEqual(
            check_workflow_yaml.check_workflow(
                _workflow_running_the_runtime(
                    command="cargo nextest archive -p runtime --archive-file integration.tar.zst"
                )
            ),
            [],
        )

    def test_a_downloaded_test_binary_is_covered_too(self):
        """`integration_llms.yml` runs a binary it downloaded, with env prefixes."""
        self.assertEqual(
            check_workflow_yaml.check_workflow(
                _workflow_running_the_runtime(
                    command='CARGO_MANIFEST_DIR="${PWD}" ./integration_test/llms_integration_test'
                )
            ),
            [self.EXPECTED],
        )

    def test_the_variable_named_only_in_a_comment_does_not_count(self):
        """A mention is not a drop; the guard must read the command, not the prose."""
        self.assertEqual(
            check_workflow_yaml.check_workflow(
                _workflow_running_the_runtime(prelude="# unset AWS_ENDPOINT_URL one day")
            ),
            [self.EXPECTED],
        )

    def test_a_runtime_named_only_in_a_comment_is_not_an_invocation(self):
        """The converse false positive: prose about the command must not fail a step."""
        workflow = (
            "---\non:\n  workflow_dispatch:\n\njobs:\n  test-models:\n"
            "    runs-on: spiceai-macos\n    steps:\n"
            "      - name: Set up spiceio\n"
            "        uses: spiceai/spiceio/.github/actions/setup@0870da5\n"
            "      - name: Note\n        run: |\n"
            "          # cargo nextest run happens in the next job\n"
            "          echo noted\n"
        )
        self.assertEqual(check_workflow_yaml.check_workflow(workflow), [])

    def test_dropping_it_after_the_runtime_has_started_is_reported(self):
        """Order matters: the child process is already gone by then."""
        workflow = (
            "---\non:\n  workflow_dispatch:\n\njobs:\n  test-models:\n"
            "    runs-on: spiceai-macos\n    steps:\n"
            "      - name: Set up spiceio\n"
            "        uses: spiceai/spiceio/.github/actions/setup@0870da5\n"
            "      - name: Run OpenAI integration test\n        run: |\n"
            "          cargo nextest run -- openai_test\n"
            "          unset AWS_ENDPOINT_URL\n"
        )
        self.assertEqual(check_workflow_yaml.check_workflow(workflow), [self.EXPECTED])

    def test_a_drop_inside_a_conditional_is_reported(self):
        """Indented means it is inside an `if` or a function and may not run."""
        workflow = (
            "---\non:\n  workflow_dispatch:\n\njobs:\n  test-models:\n"
            "    runs-on: spiceai-macos\n    steps:\n"
            "      - name: Set up spiceio\n"
            "        uses: spiceai/spiceio/.github/actions/setup@0870da5\n"
            "      - name: Run OpenAI integration test\n        run: |\n"
            '          if [ "$CI" = "false" ]; then\n'
            "            unset AWS_ENDPOINT_URL\n"
            "          fi\n"
            "          cargo nextest run -- openai_test\n"
        )
        self.assertEqual(check_workflow_yaml.check_workflow(workflow), [self.EXPECTED])

    def test_unset_f_removes_a_function_not_the_variable(self):
        self.assertEqual(
            check_workflow_yaml.check_workflow(
                _workflow_running_the_runtime(prelude="unset -f AWS_ENDPOINT_URL")
            ),
            [self.EXPECTED],
        )

    def test_a_runtime_step_before_the_cache_setup_is_left_alone(self):
        """The export does not exist yet, so there is nothing to inherit."""
        workflow = (
            "---\non:\n  workflow_dispatch:\n\njobs:\n  test-models:\n"
            "    runs-on: spiceai-macos\n    steps:\n"
            "      - name: Run OpenAI integration test\n        run: |\n"
            "          cargo nextest run -- openai_test\n"
            "      - name: Set up spiceio\n"
            "        uses: spiceai/spiceio/.github/actions/setup@0870da5\n"
        )
        self.assertEqual(check_workflow_yaml.check_workflow(workflow), [])


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
