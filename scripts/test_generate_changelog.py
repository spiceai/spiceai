#!/usr/bin/env python3
# Copyright 2024-2026 The Spice.ai OSS Authors
"""Tests for `scripts/generate_changelog.py` and the workflow step that runs it.

Both inputs of `.github/workflows/generate_changelog.yml` are free text typed by
whoever dispatches the run. These tests run that workflow's own `Generate
Changelog` step -- its `env:` and its `run:` body, read from the workflow file,
with `${{ inputs.* }}` substituted the way GitHub substitutes it -- in a scratch
repository with an offline `gh`, and require that:

* hostile inputs (shell syntax, quote breakouts, Git options) run no command and
  stop with an error naming the input, before any other command runs;
* a release ref whose name contains shell syntax runs no command either, even
  with the script's name check disabled, because no command goes through a shell;
* real inputs produce the exact changelog, through each of its three paths.

Run: python3 scripts/test_generate_changelog.py -v
"""

import os
import re
import shutil
import subprocess
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "scripts" / "generate_changelog.py"
WORKFLOW = ROOT / ".github" / "workflows" / "generate_changelog.yml"
STEP_NAME = "Generate Changelog"

# Offline stand-in for the GitHub CLI. It records every call, so a test can tell
# that a rejected input stopped the script before it reached the network.
GH_STUB = """\
#!/bin/sh
echo "$*" >> "$GH_STUB_LOG"
case "$1 $2" in
  "pr list") cat "$GH_STUB_PR_LIST" ;;
  "pr view") printf '{"number":%s,"author":{"login":"viewed-%s"}}\\n' "$3" "$3" ;;
  *) echo "gh stub: unexpected call: $*" >&2; exit 1 ;;
esac
"""


def workflow_step(text, name):
    """Return (shell, env, run) for the step called `name` in workflow `text`.

    Parsed textually so the test needs nothing beyond the standard library; it
    accepts only the shape the step has (`name`, `shell`, `env` mapping, `run`
    block scalar) and fails loudly on anything else rather than guess.
    """
    lines = text.splitlines()
    starts = [i for i, ln in enumerate(lines) if re.fullmatch(rf"\s*- name: {re.escape(name)}\s*", ln)]
    if len(starts) != 1:
        raise AssertionError(f"expected one step named {name!r} in {WORKFLOW}, found {len(starts)}")
    first = starts[0]
    dash = len(lines[first]) - len(lines[first].lstrip())
    keys = {}
    current = None
    for ln in lines[first + 1:]:
        if not ln.strip():
            if current is not None:
                keys[current][1].append("")
            continue
        indent = len(ln) - len(ln.lstrip())
        if indent <= dash:
            break
        if indent == dash + 2:
            key, _, inline = ln.strip().partition(":")
            current = key
            keys[key] = (inline.strip(), [])
        elif current is not None:
            keys[current][1].append(ln)
    shell = keys.get("shell", ("", []))[0]
    env = {}
    for ln in keys.get("env", ("", []))[1]:
        if ln.strip() and not ln.strip().startswith("#"):
            key, _, value = ln.strip().partition(":")
            env[key.strip()] = value.strip().strip("'\"")
    run_inline, run_lines = keys["run"]
    if run_inline != "|":
        raise AssertionError(f"expected `run: |` in step {name!r}, found `run: {run_inline}`")
    return shell, env, textwrap.dedent("\n".join(run_lines)).strip() + "\n"


def evaluate_expressions(text, inputs):
    """Substitute `${{ inputs.NAME }}` and `${{ secrets.GITHUB_TOKEN }}` as GitHub does.

    GitHub pastes the value in as plain text before the shell ever sees the
    script, so a value interpolated into `run:` becomes shell source.
    """

    def substitute(match):
        expression = match.group(1).strip()
        if expression == "secrets.GITHUB_TOKEN":
            return "test-token"
        input_match = re.fullmatch(r"inputs\.([A-Za-z_][A-Za-z0-9_-]*)", expression)
        if input_match is None:
            raise AssertionError(f"the test does not model the expression `${{{{ {expression} }}}}`")
        return inputs[input_match.group(1)]

    return re.sub(r"\$\{\{(.*?)\}\}", substitute, text)


class Repository:
    """A scratch repository shaped like a checkout made with `fetch-depth: 0`.

    trunk:   initial -> "feat: add b (#2)" -> "fix: c (#3)" -> "chore: d"
    v1.0.0:  annotated tag on `initial`
    release/1.0: initial -> cherry-pick of b -> "fix: e (#5)" -> "docs: f"

    Only #2 is a merged trunk PR known to `gh pr list`, so the release branch
    exercises each of the script's three paths: a patch-id match to #2, a PR
    number read from the commit title (#5, through `gh pr view`), and a commit
    with no PR at all.
    """

    def __init__(self, root):
        self.root = Path(root)
        self.repo = self.root / "repo"
        self.bin = self.root / "bin"
        self.gh_log = self.root / "gh-calls.log"
        self.repo.mkdir()
        self.bin.mkdir()
        (self.bin / "gh").write_text(GH_STUB)
        (self.bin / "gh").chmod(0o755)
        # The step runs `python3`; make that the interpreter running these tests.
        (self.bin / "python3").write_text(f'#!/bin/sh\nexec "{sys.executable}" "$@"\n')
        (self.bin / "python3").chmod(0o755)
        self.env = {
            "PATH": f"{self.bin}{os.pathsep}{os.environ.get('PATH', '/usr/bin:/bin')}",
            "HOME": str(self.root),
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_CONFIG_GLOBAL": os.devnull,
            "GIT_AUTHOR_NAME": "Test",
            "GIT_AUTHOR_EMAIL": "test@example.invalid",
            "GIT_COMMITTER_NAME": "Test",
            "GIT_COMMITTER_EMAIL": "test@example.invalid",
            "GH_STUB_LOG": str(self.gh_log),
            "GH_STUB_PR_LIST": str(self.root / "pr-list.json"),
        }
        self.git("init", "-q", "-b", "trunk")
        self.commit("a.txt", "a\n", "initial")
        self.git("tag", "-a", "-m", "v1.0.0", "v1.0.0")
        self.git("checkout", "-q", "-b", "release/1.0")
        self.git("checkout", "-q", "trunk")
        trunk_b = self.commit("b.txt", "b\n", "feat: add b (#2)")
        self.commit("c.txt", "c\n", "fix: c (#3)")
        self.commit("d.txt", "d\n", "chore: d")
        self.git("checkout", "-q", "release/1.0")
        self.git("cherry-pick", "-x", trunk_b)
        self.commit("e.txt", "e\n", "fix: e (#5)")
        self.commit("f.txt", "f\n", "docs: f")
        self.git("update-ref", "refs/remotes/origin/trunk", "trunk")
        self.git("update-ref", "refs/remotes/origin/release/1.0", "release/1.0")
        self.git("checkout", "-q", "--detach", "trunk")
        (self.root / "pr-list.json").write_text(
            '[{"number": 2, "mergeCommit": {"oid": "%s"}, "author": {"login": "octo"},'
            ' "title": "feat: add b"}]\n' % trunk_b
        )
        # The step runs `./scripts/generate_changelog.py` from the checkout root.
        (self.repo / "scripts").mkdir()
        shutil.copy(SCRIPT, self.repo / "scripts" / "generate_changelog.py")

    def git(self, *args):
        return subprocess.run(
            ["git", *args], cwd=self.repo, env=self.env, check=True, capture_output=True, text=True
        ).stdout.strip()

    def commit(self, path, content, message):
        (self.repo / path).write_text(content)
        self.git("add", path)
        self.git("commit", "-q", "-m", message)
        return self.git("rev-parse", "HEAD")

    def run_workflow_step(self, previous_release_tag, release_branch):
        """Run the workflow's step as GitHub would, with these dispatch inputs.

        Each run starts with no `gh` calls recorded and no `ran-a-command` marker, so a
        command run by one input is attributed to that input alone.
        """
        for leftover in (self.gh_log, self.repo / "ran-a-command"):
            if leftover.exists():
                leftover.unlink()
        shell, env, run = workflow_step(WORKFLOW.read_text(), STEP_NAME)
        if shell != "bash":
            raise AssertionError(f"step {STEP_NAME!r} runs under `shell: {shell}`; the test models bash")
        inputs = {"previous_release_tag": previous_release_tag, "release_branch": release_branch}
        script = self.root / "step.sh"
        script.write_text(evaluate_expressions(run, inputs))
        step_env = dict(self.env)
        step_env.update({key: evaluate_expressions(value, inputs) for key, value in env.items()})
        # GitHub's `shell: bash` is `bash --noprofile --norc -eo pipefail {0}`.
        return subprocess.run(
            ["bash", "--noprofile", "--norc", "-eo", "pipefail", str(script)],
            cwd=self.repo,
            env=step_env,
            capture_output=True,
            text=True,
            timeout=120,
        )

    def gh_calls(self):
        return self.gh_log.read_text().splitlines() if self.gh_log.exists() else []


EXPECTED_CHANGELOG = """
### Changelog

- feat: add b by [@octo](https://github.com/octo) in [#2](https://github.com/spiceai/spiceai/pull/2)
- fix: e by [@viewed-5](https://github.com/viewed-5) in [#5](https://github.com/spiceai/spiceai/pull/5)
- !!!! (PR NOT FOUND): docs: f - https://github.com/spiceai/spiceai/commit/{f}/
"""


def invalid_name_error(input_name, value, kind, example):
    return (
        f"error: Failed to generate the changelog: {input_name} {value!r} is not a valid {kind} "
        f"name: use only letters, digits, '.', '_', '-' and '/', starting with a letter, digit "
        f"or '_'. Pass the name of the {kind}, for example '{example}'."
    )


class GenerateChangelogStepTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="changelog-test-")
        self.addCleanup(shutil.rmtree, self.tmp)
        self.repo = Repository(self.tmp)
        self.marker = self.repo.repo / "ran-a-command"

    def assert_rejected(self, result, expected_stderr):
        self.assertFalse(self.marker.exists(), "an input ran a command")
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertEqual(result.stderr.strip(), expected_stderr)
        self.assertEqual(result.stdout, "")
        self.assertEqual(self.repo.gh_calls(), [], "a rejected input must stop the script before `gh`")

    def test_release_inputs_produce_the_changelog(self):
        result = self.repo.run_workflow_step("v1.0.0", "release/1.0")
        self.assertEqual(result.returncode, 0, result.stderr)
        f = self.repo.git("rev-parse", "refs/remotes/origin/release/1.0")
        self.assertEqual(result.stdout, EXPECTED_CHANGELOG.format(f=f))
        # The #2 line came from the patch-id match (its login is from `pr list`),
        # and only #5 needed `pr view`.
        self.assertEqual(
            self.repo.gh_calls(),
            [
                "pr list --state merged --base trunk --limit 10000 --json number,mergeCommit,author,title --repo spiceai/spiceai",
                "pr view 5 --json number,author --repo spiceai/spiceai",
            ],
        )

    def test_shell_syntax_in_an_input_runs_nothing(self):
        cases = [
            # (input, value): command separators, substitutions, quote breakouts
            # of `"..."` and `'...'`, and a newline.
            ("previous_release_tag", "v1.0.0;touch${IFS}ran-a-command;#"),
            ("previous_release_tag", "v1.0.0`touch ran-a-command`"),
            ("previous_release_tag", 'v1.0.0"; touch ran-a-command; echo "'),
            ("previous_release_tag", "v1.0.0'; touch ran-a-command; echo '"),
            ("previous_release_tag", "v1.0.0 && touch ran-a-command"),
            ("release_branch", "release/1.0$(touch ran-a-command)"),
            ("release_branch", "release/1.0$(touch${IFS}ran-a-command)"),
            ("release_branch", "release/1.0|touch ran-a-command"),
            ("release_branch", 'release/1.0"; touch ran-a-command; echo "'),
            ("release_branch", "release/1.0\ntouch ran-a-command"),
        ]
        for input_name, value in cases:
            with self.subTest(input=input_name, value=value):
                if input_name == "previous_release_tag":
                    result = self.repo.run_workflow_step(value, "release/1.0")
                    expected = invalid_name_error(input_name, value, "tag", "v1.4.0")
                else:
                    result = self.repo.run_workflow_step("v1.0.0", value)
                    expected = invalid_name_error(input_name, value, "branch", "release/1.5")
                self.assert_rejected(result, expected)

    def test_option_shaped_input_is_not_passed_to_git(self):
        written = self.repo.root / "written-by-git-log"
        value = f"--output={written}"
        result = self.repo.run_workflow_step(value, "release/1.0")
        self.assert_rejected(result, invalid_name_error("previous_release_tag", value, "tag", "v1.4.0"))
        self.assertEqual(list(self.repo.root.glob("written-by-git-log*")), [])

    def test_input_with_a_space_arrives_as_one_argument(self):
        # Quoted expansion: the whole value reaches the script as one argument,
        # so the error quotes all of it rather than a usage message.
        result = self.repo.run_workflow_step("v1.0.0 v0.9.0", "release/1.0")
        self.assert_rejected(result, invalid_name_error("previous_release_tag", "v1.0.0 v0.9.0", "tag", "v1.4.0"))

    def test_refs_that_do_not_exist_are_named(self):
        result = self.repo.run_workflow_step("v9.9.9", "release/1.0")
        self.assert_rejected(
            result,
            "error: Failed to generate the changelog: previous_release_tag 'v9.9.9' does not name a "
            "tag in this checkout: 'refs/tags/v9.9.9' does not exist. Pass the name of the tag, for "
            "example 'v1.4.0'. If it exists on GitHub, fetch it first (`git fetch --tags origin`).",
        )
        # A branch name is not a tag, even though Git would resolve it as a revision.
        result = self.repo.run_workflow_step("trunk", "release/1.0")
        self.assert_rejected(
            result,
            "error: Failed to generate the changelog: previous_release_tag 'trunk' does not name a "
            "tag in this checkout: 'refs/tags/trunk' does not exist. Pass the name of the tag, for "
            "example 'v1.4.0'. If it exists on GitHub, fetch it first (`git fetch --tags origin`).",
        )
        result = self.repo.run_workflow_step("v1.0.0", "release/9.9")
        self.assert_rejected(
            result,
            "error: Failed to generate the changelog: release_branch 'release/9.9' does not name a "
            "branch in this checkout: 'refs/remotes/origin/release/9.9' does not exist. Pass the name "
            "of the branch, for example 'release/1.5'. If it exists on GitHub, fetch it first "
            "(`git fetch --tags origin`).",
        )

    def test_names_git_rejects_are_named(self):
        result = self.repo.run_workflow_step("v1.0.0", "release/../1.0")
        self.assert_rejected(
            result,
            "error: Failed to generate the changelog: release_branch 'release/../1.0' is not a valid "
            "branch name: Git does not accept 'refs/remotes/origin/release/../1.0' as a ref name. "
            "Pass the name of the branch, for example 'release/1.5'.",
        )


class NoShellTest(unittest.TestCase):
    """No command goes through a shell, independent of the name check.

    Git's ref grammar accepts `;`, `$`, `{`, `}` and `#`, so a real tag can carry
    shell syntax in its name. With the script's name check disabled, such a tag
    still runs nothing, because Git receives it as one argument and every later
    command receives only commit IDs.
    """

    def test_a_ref_named_with_shell_syntax_runs_nothing(self):
        tmp = tempfile.mkdtemp(prefix="changelog-test-")
        self.addCleanup(shutil.rmtree, tmp)
        repo = Repository(tmp)
        tag = "v1.0.0;touch${IFS}ran-a-command;#"
        repo.git("tag", tag, "v1.0.0^{commit}")
        driver = textwrap.dedent(
            """\
            import importlib.util, re, sys
            spec = importlib.util.spec_from_file_location("generate_changelog", sys.argv[1])
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            module.REF_NAME = re.compile(r"(?s).*")  # disable the name check
            sys.argv = sys.argv[1:]
            module.main()
            """
        )
        result = subprocess.run(
            [sys.executable, "-I", "-c", driver, str(SCRIPT), tag, "release/1.0"],
            cwd=repo.repo,
            env=repo.env,
            capture_output=True,
            text=True,
            timeout=120,
        )
        self.assertFalse((repo.repo / "ran-a-command").exists(), "a ref name ran a command")
        self.assertEqual(result.returncode, 0, result.stderr)
        f = repo.git("rev-parse", "refs/remotes/origin/release/1.0")
        self.assertEqual(result.stdout, EXPECTED_CHANGELOG.format(f=f))


if __name__ == "__main__":
    unittest.main()
