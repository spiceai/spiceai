#!/usr/bin/env python3
# Copyright 2024-2026 The Spice.ai OSS Authors
"""Tests for scripts/signoff-batch, with real Git, a bare "origin", and stub gh/signoff."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from importlib.machinery import SourceFileLoader
from importlib.util import module_from_spec, spec_from_loader
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
SUBJECT = REPO / "scripts/signoff-batch"
# The script has no .py suffix, so name its loader explicitly.
_loader = SourceFileLoader("signoff_batch", str(SUBJECT))
batch = module_from_spec(spec_from_loader("signoff_batch", _loader))
sys.modules["signoff_batch"] = batch  # @dataclass resolves its module by name
_loader.exec_module(batch)

# Stands in for scripts/signoff in the fixture tree. `verify` fails when the tree
# has a FAIL file; an attestation records "<HEAD> <note>".
STUB_SIGNOFF = """#!/usr/bin/env bash
# signoff-batch accepts a scripts/signoff that defines cmd_verify.
case "$1" in
  verify)
    if [[ -n "${STUB_VERIFY_HOOK:-}" ]]; then bash "$STUB_VERIFY_HOOK" >&2; fi
    if [[ -e FAIL ]]; then echo 'error: could not compile `probe` (lib)'; exit 1; fi
    echo "✓ Verified $(git rev-parse --short=12 HEAD) — lint+test passed in 1s (no status posted)" ;;
  -f) printf '%s %s\\n' "$(git rev-parse HEAD)" "$4" >> "$STUB_ATTEST_LOG" ;;
  *) exit 2 ;;
esac
"""

STUB_GH = """#!/usr/bin/env bash
# gh pr view <n> --json <fields>
cat "$STUB_GH_DIR/pr-$3.json"
"""


class PureFunctionTest(unittest.TestCase):
    def test_parse_pr_arg(self):
        for arg in ("15040", "#15040", " 15040 "):
            self.assertEqual(batch.parse_pr_arg(arg), 15040, arg)
        for arg in ("trunk", "x/pull/15040/", "https://example.com/a/b/c/pull/15040"):
            with self.assertRaises(batch.BatchError, msg=arg):
                batch.parse_pr_arg(arg, "spiceai/spiceai")
        # A URL is only accepted for the repository the batch pushes to.
        for repo in ("spiceai/spiceai", "SpiceAI/SpiceAI"):
            for arg in ("https://github.com/spiceai/spiceai/pull/15040", "spiceai/spiceai/pull/15040/"):
                self.assertEqual(batch.parse_pr_arg(arg, repo), 15040, arg)
        for arg in ("https://github.com/other/project/pull/15040", "other/project/pull/15040"):
            with self.assertRaises(batch.BatchError, msg=arg):
                batch.parse_pr_arg(arg, "spiceai/spiceai")
        with self.assertRaises(batch.BatchError):
            batch.parse_pr_arg("https://github.com/spiceai/spiceai/pull/15040", None)
        self.assertEqual(batch.parse_pr_arg("15040", None), 15040)

    def test_repo_slug_from_remote_url(self):
        for url in ("https://github.com/spiceai/spiceai.git", "git@github.com:spiceai/spiceai.git",
                    "https://github.com/spiceai/spiceai", "ssh://git@github.com/spiceai/spiceai.git"):
            self.assertEqual(batch.slug_from_remote_url(url), "spiceai/spiceai", url)
        self.assertIsNone(batch.slug_from_remote_url("/tmp/origin.git"))

    def test_remote_repo_slug_refuses_a_split_remote(self):
        with tempfile.TemporaryDirectory(prefix="signoff-batch-remote-") as tmp:
            repo = Path(tmp)
            env = {"GIT_CONFIG_GLOBAL": os.devnull, "GIT_CONFIG_NOSYSTEM": "1"}

            def git(*args):
                subprocess.run(["git", *args], cwd=repo, env=os.environ | env, check=True, capture_output=True)

            git("init", "-q")
            self.assertIsNone(batch.remote_repo_slug(repo))  # no remote: not a GitHub repository
            git("remote", "add", "origin", "https://github.com/spiceai/spiceai.git")
            self.assertEqual(batch.remote_repo_slug(repo), "spiceai/spiceai")
            git("remote", "set-url", "--push", "origin", "git@github.com:other/project.git")
            with self.assertRaises(batch.BatchError):
                batch.remote_repo_slug(repo)

    def test_failing_crates_from_clippy_and_nextest(self):
        log = "\n".join([
            "error: could not compile `runtime` (lib test) due to 2 previous errors",
            "        FAIL [   0.004s] (12/340) cayenne::integration some::test",
            "     SIGSEGV [   1.000s] data_components tests::crash",
            "        PASS [   0.001s] (13/340) runtime other::test",
            "error: could not compile `runtime` (lib)",
        ])
        self.assertEqual(batch.failing_crates(log), ["runtime", "cayenne", "data_components"])

    def test_attribute_direct_and_via_dependency(self):
        crates = {
            "runtime": batch.Crate("runtime", "crates/runtime", frozenset({"cayenne"})),
            "cayenne": batch.Crate("cayenne", "crates/cayenne", frozenset({"arrow_tools"})),
            "arrow_tools": batch.Crate("arrow_tools", "crates/arrow_tools", frozenset()),
            "spice": batch.Crate("spice", "bin/spice", frozenset()),
        }
        self.assertEqual(batch.crate_for_file(crates, "crates/cayenne/src/lib.rs"), "cayenne")
        self.assertIsNone(batch.crate_for_file(crates, "docs/readme.md"))
        touched = {1: {"runtime"}, 2: {"arrow_tools"}, 3: {"spice"}}
        [entry] = batch.attribute(["runtime"], touched, crates)
        self.assertEqual((entry.direct, entry.via_dependency), ([1], [2]))

    def test_note_fits_the_status_description(self):
        prs = [batch.PullRequest(n, f"b{n}", "t", "trunk", "0" * 40) for n in range(15000, 15012)]
        heads = tuple(batch.UpdatedHead(pr, pr.head) for pr in prs)
        full = batch.Batch("a" * 40, heads, "b" * 40)
        note = batch.compose_note("lint+test passed", prs[0], full)
        self.assertLessEqual(len(note), batch.NOTE_MAX_CHARS)
        self.assertIn("11 other PRs", note)
        pair = batch.Batch("a" * 40, heads[:2], "b" * 40)
        self.assertEqual(batch.compose_note("lint+test passed", prs[0], pair),
                         "lint+test passed in batch with #15001")


class EndToEndTest(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory(prefix="signoff-batch-")
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.origin, self.dev, self.worktree = self.root / "origin.git", self.root / "dev", self.root / "batch"
        stubs = self.root / "stubs"
        stubs.mkdir()
        (stubs / "gh").write_text(STUB_GH)
        (stubs / "gh").chmod(0o755)
        self.attest_log = self.root / "attest.log"
        self.env = {
            k: v for k, v in os.environ.items()
            if not k.startswith(("GIT_", "SIGNOFF_"))
        } | {
            "PATH": f"{stubs}{os.pathsep}{os.environ['PATH']}",
            "STUB_GH_DIR": str(self.root),
            "STUB_ATTEST_LOG": str(self.attest_log),
            "GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@example.invalid",
            "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@example.invalid",
            "GIT_CONFIG_GLOBAL": os.devnull, "GIT_CONFIG_NOSYSTEM": "1",
        }
        self.git(self.root, "init", "-q", "--bare", "-b", "trunk", str(self.origin))
        self.git(self.root, "clone", "-q", str(self.origin), str(self.dev))
        self.git(self.dev, "checkout", "-q", "-b", "trunk")
        self.write("scripts/signoff", STUB_SIGNOFF, mode=0o755)
        for name in ("a", "b", "c"):
            self.write(f"{name}.txt", f"{name}\n")
        self.commit_and_push("trunk", "base")

    def git(self, cwd, *args):
        result = subprocess.run(["git", *args], cwd=cwd, env=self.env, text=True, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        return result.stdout.strip()

    def write(self, path, text, mode=0o644):
        dest = self.dev / path
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(text)
        dest.chmod(mode)

    def commit_and_push(self, branch, message):
        self.git(self.dev, "add", "-A")
        self.git(self.dev, "commit", "-q", "-m", message)
        self.git(self.dev, "push", "-q", "origin", f"HEAD:refs/heads/{branch}")

    def open_pr(self, number, branch, edits):
        self.git(self.dev, "checkout", "-q", "-b", branch, "origin/trunk")
        for path, text in edits.items():
            self.write(path, text)
        self.commit_and_push(branch, f"pr {number}")
        (self.root / f"pr-{number}.json").write_text(json.dumps({
            "number": number, "headRefName": branch, "baseRefName": "trunk",
            "title": f"PR {number}", "state": "OPEN", "isCrossRepository": False,
        }))

    def advance_trunk(self):
        self.git(self.dev, "checkout", "-q", "trunk")
        self.git(self.dev, "pull", "-q", "origin", "trunk")
        self.write("c.txt", "c on trunk\n")
        self.commit_and_push("trunk", "trunk moves")

    def origin_tip(self, branch):
        return self.git(self.origin, "rev-parse", f"refs/heads/{branch}")

    def run_batch(self, *args, env=None):
        return subprocess.run(
            [str(SUBJECT), "--yes", "--worktree", str(self.worktree), *args],
            cwd=self.dev, env=self.env | (env or {}), text=True, capture_output=True, timeout=60,
        )

    def test_pass_pushes_trunk_merges_and_attests_each_head(self):
        self.open_pr(1, "pr-one", {"a.txt": "a from pr one\n"})
        self.open_pr(2, "pr-two", {"b.txt": "b from pr two\n"})
        self.advance_trunk()
        trunk = self.origin_tip("trunk")

        result = self.run_batch("1", "#2")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

        attested = dict(line.split(" ", 1) for line in self.attest_log.read_text().splitlines())
        for number, branch, other in ((1, "pr-one", "#2"), (2, "pr-two", "#1")):
            tip = self.origin_tip(branch)
            self.git(self.origin, "merge-base", "--is-ancestor", trunk, tip)
            self.assertIn(tip, attested, f"#{number} head was not attested")
            self.assertEqual(attested[tip], f"lint+test passed in batch with {other}")
        # The attestation worktrees are gone again.
        self.assertNotIn("signoff-batch-attest", self.git(self.dev, "worktree", "list"))

    def test_branch_that_moves_during_the_checks_is_not_attested(self):
        # The PR already has trunk, so the batch has no merge of its own to push.
        self.open_pr(1, "pr-one", {"a.txt": "a from pr one\n"})
        checked = self.origin_tip("pr-one")
        other = self.root / "other"
        self.git(self.root, "clone", "-q", "-b", "pr-one", str(self.origin), str(other))
        hook = self.root / "move-branch.sh"
        hook.write_text(
            f"cd {other} && echo moved > a.txt && git commit -qam 'moved during checks'"
            " && git push -q origin HEAD:refs/heads/pr-one\n"
        )
        result = self.run_batch("1", env={"STUB_VERIFY_HOOK": str(hook)})
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("#1: push rejected, not attested", result.stderr)
        self.assertNotEqual(self.origin_tip("pr-one"), checked)
        attested = self.attest_log.read_text() if self.attest_log.exists() else ""
        self.assertNotIn(checked, attested)

    def test_branch_deleted_during_the_checks_is_not_recreated(self):
        # A PR merged mid-run deletes its branch; the batch must not push it back.
        self.open_pr(1, "pr-one", {"a.txt": "a from pr one\n"})
        checked = self.origin_tip("pr-one")
        hook = self.root / "delete-branch.sh"
        hook.write_text(f"git --git-dir={self.origin} update-ref -d refs/heads/pr-one\n")
        result = self.run_batch("1", env={"STUB_VERIFY_HOOK": str(hook)})
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("#1: push rejected, not attested", result.stderr)
        self.assertEqual(self.git(self.origin, "branch", "--list", "pr-one"), "")
        attested = self.attest_log.read_text() if self.attest_log.exists() else ""
        self.assertNotIn(checked, attested)

    def test_failure_pushes_and_attests_nothing(self):
        self.open_pr(1, "pr-one", {"a.txt": "a from pr one\n"})
        self.open_pr(2, "pr-two", {"FAIL": "x\n"})
        self.advance_trunk()
        before = {b: self.origin_tip(b) for b in ("pr-one", "pr-two")}

        result = self.run_batch("1", "2")
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("Nothing was pushed or attested", result.stderr)
        self.assertEqual(before, {b: self.origin_tip(b) for b in before})
        self.assertFalse(self.attest_log.exists())
        # The reviewer can inspect the failing batch.
        self.assertEqual(self.git(self.worktree, "rev-parse", "--abbrev-ref", "HEAD"), "signoff-batch")
        self.assertTrue((self.worktree / "FAIL").exists())

    def test_conflict_between_prs_names_both(self):
        self.open_pr(1, "pr-one", {"a.txt": "a from pr one\n"})
        self.open_pr(2, "pr-two", {"a.txt": "a from pr two\n"})
        result = self.run_batch("1", "2")
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("#2 (pr-two) conflicts with PRs earlier in the batch", result.stderr)
        self.assertIn("a.txt  (also changed by #1)", result.stderr)
        self.assertFalse(self.worktree.exists())

    def test_conflict_with_trunk_stops_before_the_batch(self):
        self.open_pr(1, "pr-one", {"c.txt": "c from pr one\n"})
        self.advance_trunk()
        before = self.origin_tip("pr-one")
        result = self.run_batch("1")
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("#1 (pr-one) conflicts with trunk in:\n  c.txt", result.stderr)
        self.assertEqual(before, self.origin_tip("pr-one"))

    def test_plan_checks_out_the_batch_and_pushes_nothing(self):
        self.open_pr(1, "pr-one", {"a.txt": "a from pr one\n"})
        self.advance_trunk()
        before = self.origin_tip("pr-one")
        result = self.run_batch("--plan", "1")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual((self.worktree / "a.txt").read_text(), "a from pr one\n")
        self.assertEqual((self.worktree / "c.txt").read_text(), "c on trunk\n")
        self.assertEqual(before, self.origin_tip("pr-one"))
        # A second run reuses the worktree.
        self.assertEqual(self.run_batch("--plan", "1").returncode, 0)

    def test_dirty_worktree_is_refused(self):
        self.open_pr(1, "pr-one", {"a.txt": "a from pr one\n"})
        self.assertEqual(self.run_batch("--plan", "1").returncode, 0)
        (self.worktree / "a.txt").write_text("local edit\n")
        result = self.run_batch("1")
        self.assertEqual(result.returncode, 1)
        self.assertIn("has local changes", result.stderr)


if __name__ == "__main__":
    unittest.main(verbosity=2)
