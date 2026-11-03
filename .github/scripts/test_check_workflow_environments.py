"""Unit tests for check_workflow_environments.py."""
import os
import sys
import tempfile
import unittest
from pathlib import Path

import yaml

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import check_workflow_environments  # noqa: E402


def problems(text: str) -> list[str]:
    return check_workflow_environments.check_workflow(yaml.safe_load(text))


class EnvironmentRequiredTest(unittest.TestCase):
    def test_job_without_environment_fails(self):
        self.assertEqual(
            problems(
                """\
jobs:
  build:
    runs-on: ubuntu-24.04
    steps:
      - run: echo hi
"""
            ),
            [
                "job 'build' selects no `environment:`; add the environment whose secrets it uses, "
                "or `environment: secret-free` if it uses none (see docs/dev/github_actions_environments.md)"
            ],
        )

    def test_empty_environment_fails(self):
        result = problems(
            """\
jobs:
  build:
    environment: ''
    runs-on: ubuntu-24.04
    steps:
      - run: echo hi
"""
        )
        self.assertEqual(len(result), 1)
        self.assertTrue(result[0].startswith("job 'build' selects no `environment:`"), result)

    def test_string_and_mapping_environments_pass(self):
        self.assertEqual(
            problems(
                """\
jobs:
  a:
    environment: ci
    runs-on: ubuntu-24.04
    steps:
      - run: echo ${{ secrets.MINIO_ACCESS_KEY }}
  b:
    environment:
      name: github-publish
      url: https://example.com
    runs-on: ubuntu-24.04
    steps:
      - run: echo ${{ secrets.DOCKERHUB_TOKEN }}
  c:
    environment: ${{ inputs.environment }}
    runs-on: ubuntu-24.04
    steps:
      - run: echo hi
"""
            ),
            [],
        )

    def test_unknown_environment_fails(self):
        self.assertEqual(
            problems(
                """\
jobs:
  a:
    environment: ci-build
    runs-on: ubuntu-24.04
    steps:
      - run: echo hi
  b:
    environment:
      name: publsh
    runs-on: ubuntu-24.04
    steps:
      - run: echo hi
"""
            ),
            [
                f"job '{job}' selects environment '{env}', which is not one of "
                "`ci`, `github-publish`, `github-publish-autoapprove`, `secret-free`; "
                "GitHub would create it with no protection rules (see docs/dev/github_actions_environments.md)"
                for job, env in (("a", "ci-build"), ("b", "publsh"))
            ],
        )

    def test_reusable_workflow_caller_needs_no_environment(self):
        self.assertEqual(
            problems(
                """\
jobs:
  call:
    uses: ./.github/workflows/other.yml
    secrets: inherit
"""
            ),
            [],
        )

    def test_reusable_workflow_caller_with_environment_fails(self):
        self.assertEqual(
            problems(
                """\
jobs:
  call:
    uses: ./.github/workflows/other.yml
    environment: ci
"""
            ),
            [
                "job 'call' calls a reusable workflow and sets `environment:`, which GitHub rejects; "
                "select the environment in the called workflow's jobs instead"
            ],
        )


class SecretFreeTest(unittest.TestCase):
    def test_secret_reference_fails(self):
        self.assertEqual(
            problems(
                """\
jobs:
  test:
    environment: secret-free
    runs-on: ubuntu-24.04
    env:
      TOKEN: ${{ secrets.DOCKERHUB_TOKEN }}
    steps:
      - uses: some/action@v1
        with:
          key: ${{ secrets['MINIO_SECRET_KEY'] }}
"""
            ),
            [
                "job 'test' is in the `secret-free` environment but reads "
                "secrets.DOCKERHUB_TOKEN, secrets.MINIO_SECRET_KEY; "
                "move it to the environment that holds those secrets, or remove the references"
            ],
        )

    def test_mapping_form_secret_free_is_checked(self):
        result = problems(
            """\
jobs:
  test:
    environment:
      name: secret-free
    runs-on: ubuntu-24.04
    steps:
      - run: echo ${{ secrets.X }}
"""
        )
        self.assertEqual(len(result), 1)
        self.assertIn("reads secrets.X;", result[0])

    def test_whole_secrets_context_and_dynamic_index_fail(self):
        result = problems(
            """\
jobs:
  test:
    environment: secret-free
    runs-on: ubuntu-24.04
    steps:
      - run: echo '${{ toJSON(secrets) }}'
      - run: echo ${{ secrets[matrix.key] }}
"""
        )
        self.assertEqual(len(result), 1)
        self.assertIn("reads secrets, secrets[...];", result[0])

    def test_bare_if_condition_is_scanned(self):
        result = problems(
            """\
jobs:
  test:
    environment: secret-free
    runs-on: ubuntu-24.04
    steps:
      - if: secrets.X != ''
        run: echo hi
"""
        )
        self.assertEqual(len(result), 1)
        self.assertIn("reads secrets.X;", result[0])

    def test_workflow_level_env_secret_fails(self):
        self.assertEqual(
            problems(
                """\
env:
  KEY: ${{ secrets.SPICE_SECRET_OPENAI_API_KEY }}
jobs:
  test:
    environment: secret-free
    runs-on: ubuntu-24.04
    steps:
      - run: echo hi
  ai:
    environment: ci
    runs-on: ubuntu-24.04
    steps:
      - run: echo hi
"""
            ),
            [
                "job 'test' is in the `secret-free` environment but inherits "
                "secrets.SPICE_SECRET_OPENAI_API_KEY from the workflow-level `env:`; "
                "move those entries into the `env:` of the jobs that need them"
            ],
        )

    def test_github_token_and_non_secret_text_pass(self):
        self.assertEqual(
            problems(
                """\
jobs:
  test:
    environment: secret-free
    runs-on: ubuntu-24.04
    env:
      GH_TOKEN: ${{ secrets.GITHUB_TOKEN }}
      OTHER: ${{ github.token }}
    steps:
      # ${{ secrets.IN_A_COMMENT }}
      - run: cat secrets.txt && echo "secrets.NOT_AN_EXPRESSION"
      - if: contains(github.ref, 'secrets.X')
        run: echo ${{ inputs.secrets.y }} ${{ steps.secrets.outputs.z }}
"""
            ),
            [],
        )

    def test_secrets_in_other_environment_pass(self):
        self.assertEqual(
            problems(
                """\
jobs:
  test:
    environment: ci
    runs-on: ubuntu-24.04
    steps:
      - run: echo ${{ secrets.TEST_MYSQL_PASSWORD }}
"""
            ),
            [],
        )


class DefinitionsTest(unittest.TestCase):
    def test_reports_file_path(self):
        with tempfile.TemporaryDirectory() as tmp:
            workflows = Path(tmp) / "workflows"
            workflows.mkdir()
            (workflows / "ok.yml").write_text(
                "jobs:\n  a:\n    environment: secret-free\n    runs-on: x\n    steps: [{run: echo}]\n"
            )
            (workflows / "bad.yaml").write_text("jobs:\n  a:\n    runs-on: x\n    steps: [{run: echo}]\n")
            result = check_workflow_environments.check_definitions(Path(tmp))
            self.assertEqual(len(result), 1)
            self.assertTrue(result[0].startswith(f"{workflows / 'bad.yaml'}: job 'a' selects no"), result)
            self.assertEqual(check_workflow_environments.main(["--github-dir", tmp]), 1)

    def test_repository_workflows_pass(self):
        github_dir = Path(__file__).resolve().parent.parent
        self.assertEqual(check_workflow_environments.check_definitions(github_dir), [])


if __name__ == "__main__":
    unittest.main()
