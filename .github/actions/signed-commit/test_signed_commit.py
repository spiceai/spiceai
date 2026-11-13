#!/usr/bin/env python3
# Copyright 2024-2026 The Spice.ai OSS Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Tests for signed_commit.py against a fake GitHub backed by a real bare repository.

The fake builds every commit the way GitHub does for createCommitOnBranch: the
parent's tree plus the request's additions and deletions, refused when the branch
is not at expectedHeadOid. The run under test only reaches that repository by
fetching, as it would reach github.com, so what lands on the branch is read back
from git, not from the requests.

Usage: python3 .github/actions/signed-commit/test_signed_commit.py
"""

import base64
import contextlib
import io
import os
import socket
import subprocess
import sys
import tempfile
import threading
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import signed_commit as sc  # noqa: E402

IDENTITY = {
    "GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@example.invalid",
    "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@example.invalid",
}


def git(cwd, *args, input=None):
    proc = subprocess.run(
        ["git", *args], cwd=cwd, input=input, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        env={**os.environ, **IDENTITY},
    )
    if proc.returncode != 0:
        raise AssertionError(f"git {' '.join(args)}: {proc.stderr.decode()}")
    return proc.stdout.decode().strip()


class FakeGitHub:
    """createCommitOnBranch and the git refs API over a bare repository."""

    def __init__(self, server):
        self.server = server
        self.calls = []
        self.fail_create_branch = False
        self.fail_commit_on_call = None
        self.before_commit = None
        self.before_delete = None

    def _ref(self, branch):
        out = subprocess.run(
            ["git", "rev-parse", "--verify", "--quiet", f"refs/heads/{branch}"],
            cwd=self.server, stdout=subprocess.PIPE,
        )
        return out.stdout.decode().strip() or None

    def branches(self):
        out = git(self.server, "for-each-ref", "--format=%(refname:short)", "refs/heads/")
        return sorted(out.split()) if out else []

    def branch_tip(self, branch):
        self.calls.append(("branch_tip", branch))
        return self._ref(branch)

    def create_branch(self, branch, sha):
        self.calls.append(("create_branch", branch, sha))
        if self.fail_create_branch:
            raise sc.ApiError("Resource not accessible by integration", 403)
        if self._ref(branch):
            raise sc.ApiError("Reference already exists", 422)
        git(self.server, "update-ref", f"refs/heads/{branch}", sha)

    def fast_forward(self, branch, sha):
        self.calls.append(("fast_forward", branch, sha))
        tip = self._ref(branch)
        if subprocess.run(["git", "merge-base", "--is-ancestor", tip, sha], cwd=self.server).returncode != 0:
            raise sc.ApiError("Update is not a fast forward", 422)
        git(self.server, "update-ref", f"refs/heads/{branch}", sha, tip)

    def delete_branch(self, branch):
        self.calls.append(("delete_branch", branch))
        if self.before_delete:
            hook, self.before_delete = self.before_delete, None
            hook()
        git(self.server, "update-ref", "-d", f"refs/heads/{branch}")

    def create_commit(self, branch, expected_head, headline, body, changes, read_blob):
        self.calls.append(("create_commit", branch, len(changes)))
        commits = sum(1 for c in self.calls if c[0] == "create_commit")
        if self.before_commit:
            hook, self.before_commit = self.before_commit, None
            hook()
        if self.fail_commit_on_call == commits:
            raise sc.ApiError("createCommitOnBranch: something went wrong")
        if not changes:
            raise sc.ApiError("createCommitOnBranch: no file changes")
        tip = self._ref(branch)
        if tip != expected_head:
            raise sc.ApiError(f'Expected branch to point to "{expected_head}" but it did not.')
        with tempfile.TemporaryDirectory() as scratch:
            env = {**os.environ, **IDENTITY, "GIT_INDEX_FILE": os.path.join(scratch, "index")}
            run = lambda *a, input=None: subprocess.run(  # noqa: E731
                ["git", *a], cwd=self.server, env=env, input=input, check=True, stdout=subprocess.PIPE
            ).stdout.decode().strip()
            run("read-tree", tip)
            for change in changes:
                if change.blob is None:
                    # Mode 0 removes the entry; `--force-remove` needs a worktree the bare repo lacks.
                    run("update-index", "--index-info", input=f"0 {'0' * 40}\t{change.path}\n".encode())
                else:
                    # The request carries content, not the blob id: send it the way the API does.
                    content = base64.b64decode(base64.b64encode(read_blob(change.blob)))
                    blob = run("hash-object", "-w", "--stdin", input=content)
                    run("update-index", "--add", "--cacheinfo", f"100644,{blob},{change.path}")
            tree = run("write-tree")
            message = f"{headline}\n\n{body}".strip() + "\n"
            commit = run("commit-tree", tree, "-p", tip, input=message.encode())
        git(self.server, "update-ref", f"refs/heads/{branch}", commit, tip)
        return commit


class SignedCommitTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        root = self._tmp.name
        self.server = os.path.join(root, "server.git")
        self.work = os.path.join(root, "work")
        git(root, "init", "-q", "--bare", self.server)
        git(root, "init", "-q", self.work)
        self.write(".gitignore", "*.ignored\n")
        self.write("snapshots/q1.snap", "q1 v1\n")
        self.write("snapshots/q2.snap", "q2 v1\n")
        self.write("src/lib.rs", "fn main() {}\n")
        git(self.work, "add", "-A")
        git(self.work, "commit", "-q", "-m", "base")
        self.base = git(self.work, "rev-parse", "HEAD")
        git(self.work, "push", "-q", self.server, "HEAD:refs/heads/trunk")
        self.api = FakeGitHub(self.server)
        self.git = sc.Git(self.work)
        self.fetched = []
        self.logs = []

    def tearDown(self):
        self._tmp.cleanup()

    def write(self, path, text):
        full = os.path.join(self.work, path)
        os.makedirs(os.path.dirname(full), exist_ok=True)
        with open(full, "w") as f:
            f.write(text)

    def remove(self, path):
        os.remove(os.path.join(self.work, path))

    def fetch(self, sha):
        self.fetched.append(sha)
        git(self.work, "fetch", "-q", self.server, sha)

    def publish(self, **kwargs):
        kwargs.setdefault("branch", "snap/1")
        kwargs.setdefault("message", "fix: Update snapshots")
        kwargs.setdefault("paths", ["*.snap"])
        req = sc.Request(**kwargs)
        self.last_request = req
        return sc.publish(self.git, self.api, req, self.fetch, sleep=lambda s: None, log=self.logs.append)

    def server_file(self, rev, path):
        out = subprocess.run(["git", "show", f"{rev}:{path}"], cwd=self.server, stdout=subprocess.PIPE,
                             stderr=subprocess.DEVNULL)
        return out.stdout.decode() if out.returncode == 0 else None

    def server_branch_commit(self, branch, files):
        """Push a commit to branch on the server, as another run would, from a scratch clone."""
        clone = os.path.join(self._tmp.name, f"other-{len(os.listdir(self._tmp.name))}")
        git(self._tmp.name, "clone", "-q", self.server, clone)
        start = branch if git(self.server, "branch", "--list", branch) else "trunk"
        git(clone, "checkout", "-q", "-B", branch, f"origin/{start}")
        for path, text in files.items():
            full = os.path.join(clone, path)
            if text is None:
                os.remove(full)
            else:
                os.makedirs(os.path.dirname(full), exist_ok=True)
                with open(full, "w") as f:
                    f.write(text)
        git(clone, "add", "-A")
        git(clone, "commit", "-q", "-m", "other run")
        git(clone, "push", "-q", "origin", f"HEAD:refs/heads/{branch}")
        return git(clone, "rev-parse", "HEAD")

    # ------------------------------------------------------------------ new branch

    def test_new_branch_commits_added_modified_and_deleted_files_under_the_pathspec_only(self):
        self.write("snapshots/q1.snap", "q1 v2\n")
        self.remove("snapshots/q2.snap")
        self.write("snapshots/new dir/q3 é.snap", "q3 v1\n")
        self.write("snapshots/q4.snap.ignored", "ignored\n")
        self.write("src/lib.rs", "fn main() { changed }\n")
        tip = self.publish(message="fix: Update snapshots\n\nBody line.", run_url="https://example/run/7")
        self.assertEqual(self.api.branches(), ["snap/1", "trunk"])
        self.assertEqual(git(self.server, "rev-parse", "snap/1"), tip)
        self.assertEqual(git(self.server, "rev-parse", f"{tip}^"), self.base)
        self.assertEqual(self.server_file(tip, "snapshots/q1.snap"), "q1 v2\n")
        self.assertIsNone(self.server_file(tip, "snapshots/q2.snap"))
        self.assertEqual(self.server_file(tip, "snapshots/new dir/q3 é.snap"), "q3 v1\n")
        self.assertIsNone(self.server_file(tip, "snapshots/q4.snap.ignored"))
        self.assertEqual(self.server_file(tip, "src/lib.rs"), "fn main() {}\n")
        self.assertEqual(
            git(self.server, "log", "-1", "--format=%B", tip),
            "fix: Update snapshots\n\nBody line.\n\nWorkflow run: https://example/run/7",
        )
        # The caller's index is untouched: the change is still unstaged.
        self.assertEqual(git(self.work, "diff", "--cached", "--name-only"), "")

    def test_nothing_to_commit_creates_no_branch_and_sends_no_commit(self):
        self.write("src/lib.rs", "changed outside the pathspec\n")
        self.assertIsNone(self.publish())
        self.assertEqual(self.api.branches(), ["trunk"])
        self.assertFalse([c for c in self.api.calls if c[0] in ("create_branch", "create_commit")])

    def test_a_failed_commit_on_a_new_branch_leaves_no_branch(self):
        self.write("snapshots/q1.snap", "q1 v2\n")
        self.api.fail_commit_on_call = 1
        with self.assertRaises(sc.ApiError):
            self.publish()
        self.assertEqual(self.api.branches(), ["trunk"])

    def test_cleanup_after_a_failed_commit_never_deletes_another_runs_commit(self):
        # Another run commits to the same new branch just as this run cleans up after a
        # failed commit. Its commit must survive: the target branch is never deleted.
        self.write("snapshots/q1.snap", "q1 from this run\n")
        self.api.fail_commit_on_call = 1
        other = []
        self.api.before_delete = lambda: other.append(
            self.server_branch_commit("snap/1", {"snapshots/q5.snap": "q5 from another run\n"}))
        try:
            self.publish()
        except sc.ApiError:
            pass
        self.assertEqual(len(other), 1)
        self.assertIn("snap/1", self.api.branches(), "the other run's branch was deleted")
        tip = git(self.server, "rev-parse", "snap/1")
        self.assertEqual(
            subprocess.run(["git", "merge-base", "--is-ancestor", other[0], tip], cwd=self.server).returncode, 0,
            "the other run's commit is no longer on the branch")

    # ------------------------------------------------------------------ existing branch

    def test_existing_branch_is_made_to_match_the_working_tree_and_fetched_first(self):
        older = self.server_branch_commit("openapi/1", {"snapshots/q1.snap": "q1 stale\n",
                                                         "snapshots/q9.snap": "q9 only on the branch\n"})
        self.write("snapshots/q1.snap", "q1 v3\n")
        tip = self.publish(branch="openapi/1")
        self.assertEqual(self.fetched, [older])
        self.assertEqual(git(self.server, "rev-parse", f"{tip}^"), older)
        self.assertEqual(self.server_file(tip, "snapshots/q1.snap"), "q1 v3\n")
        # Matching the working tree removes what only the branch had.
        self.assertIsNone(self.server_file(tip, "snapshots/q9.snap"))

    def test_an_explicit_parent_the_branch_is_not_at_is_refused(self):
        self.server_branch_commit("snap/1", {"snapshots/q1.snap": "q1 other\n"})
        self.write("snapshots/q1.snap", "q1 v2\n")
        with self.assertRaisesRegex(sc.SignedCommitError, "not at the parent"):
            self.publish(parent=self.base)

    def test_a_concurrent_push_is_rebuilt_on_the_new_tip(self):
        self.server_branch_commit("bench/1", {"snapshots/q5.snap": "q5 from run A\n"})
        self.write("snapshots/q1.snap", "q1 from run B\n")
        self.api.before_commit = lambda: self.server_branch_commit(
            "bench/1", {"snapshots/q6.snap": "q6 from run C\n"})
        tip = self.publish(branch="bench/1", changed_since="HEAD")
        self.assertEqual(self.server_file(tip, "snapshots/q1.snap"), "q1 from run B\n")
        self.assertEqual(self.server_file(tip, "snapshots/q5.snap"), "q5 from run A\n")
        self.assertEqual(self.server_file(tip, "snapshots/q6.snap"), "q6 from run C\n")
        self.assertEqual(sum(1 for c in self.api.calls if c[0] == "create_commit"), 2)

    # ------------------------------------------------------------------ --changed-since

    def test_changed_since_writes_only_this_runs_files_onto_the_branch(self):
        self.server_branch_commit("bench/1", {"snapshots/q5.snap": "q5 from another run\n"})
        self.write("snapshots/q1.snap", "q1 from this run\n")
        self.remove("snapshots/q2.snap")
        tip = self.publish(branch="bench/1", changed_since="HEAD")
        self.assertEqual(self.server_file(tip, "snapshots/q1.snap"), "q1 from this run\n")
        self.assertIsNone(self.server_file(tip, "snapshots/q2.snap"))
        self.assertEqual(self.server_file(tip, "snapshots/q5.snap"), "q5 from another run\n")

    def test_changed_since_on_a_file_the_branch_also_changed_is_a_conflict(self):
        self.server_branch_commit("bench/1", {"snapshots/q1.snap": "q1 from another run\n"})
        self.write("snapshots/q1.snap", "q1 from this run\n")
        with self.assertRaisesRegex(sc.ConflictError, "snapshots/q1.snap") as ctx:
            self.publish(branch="bench/1", changed_since="HEAD")
        self.assertEqual(ctx.exception.exit_code, 3)

    def test_changed_since_skips_content_the_branch_already_has(self):
        self.server_branch_commit("bench/1", {"snapshots/q1.snap": "q1 same\n"})
        self.write("snapshots/q1.snap", "q1 same\n")
        self.assertIsNone(self.publish(branch="bench/1", changed_since="HEAD"))

    # ------------------------------------------------------------------ split commits

    def test_a_large_change_lands_in_one_step_from_a_temporary_branch(self):
        for i in range(5):
            self.write(f"snapshots/big{i}.snap", f"{i}" * 300 + "\n")
        tip = self.publish(max_commit_bytes=500, run_id="77")
        commits = git(self.server, "rev-list", f"{self.base}..{tip}").split()
        self.assertEqual(len(commits), 5)
        for i in range(5):
            self.assertEqual(self.server_file(tip, f"snapshots/big{i}.snap"), f"{i}" * 300 + "\n")
        self.assertIn("Part 5 of 5", git(self.server, "log", "-1", "--format=%B", tip))
        # The target branch is created once, at the last part; the temporary branch is gone.
        target = [c for c in self.api.calls if c[0] in ("create_branch", "fast_forward") and c[1] == "snap/1"]
        self.assertEqual(target, [("create_branch", "snap/1", tip)])
        self.assertEqual(self.api.branches(), ["snap/1", "trunk"])

    def test_a_split_change_that_fails_part_way_leaves_the_branch_as_it_was(self):
        old = self.server_branch_commit("snap/1", {"snapshots/q7.snap": "q7\n"})
        for i in range(3):
            self.write(f"snapshots/big{i}.snap", f"{i}" * 300 + "\n")
        self.api.fail_commit_on_call = 2
        with self.assertRaises(sc.ApiError):
            self.publish(max_commit_bytes=500)
        self.assertEqual(git(self.server, "rev-parse", "snap/1"), old)
        self.assertEqual(self.api.branches(), ["snap/1", "trunk"])

    def test_split_moves_an_existing_branch_by_fast_forward(self):
        old = self.server_branch_commit("snap/1", {"snapshots/q7.snap": "q7\n"})
        for i in range(2):
            self.write(f"snapshots/big{i}.snap", f"{i}" * 300 + "\n")
        tip = self.publish(max_commit_bytes=500, changed_since="HEAD")
        self.assertIn(("fast_forward", "snap/1", tip), self.api.calls)
        self.assertEqual(git(self.server, "rev-list", "--count", f"{old}..{tip}"), "2")
        self.assertEqual(self.server_file(tip, "snapshots/q7.snap"), "q7\n")

    def test_split_keeps_files_whole_and_in_budget(self):
        # A 10-byte file counts 16 encoded bytes + 2 for its name + 64 overhead = 82, so two fit
        # in 200; the 5000-byte file is over budget alone and still goes whole, in its own commit.
        changes = [sc.Change(f"f{i}", "x", size) for i, size in enumerate([10, 10, 5000, 10])]
        chunks = sc.split_changes(changes, 200)
        self.assertEqual([[c.path for c in chunk] for chunk in chunks], [["f0", "f1"], ["f2"], ["f3"]])
        self.assertEqual([len(c) for c in sc.split_changes(changes, 10_000)], [4])

    # ------------------------------------------------------------------ refused file kinds

    def test_an_executable_file_is_refused(self):
        self.write("snapshots/run.snap", "#!/bin/sh\n")
        os.chmod(os.path.join(self.work, "snapshots/run.snap"), 0o755)
        with self.assertRaisesRegex(sc.UsageError, "executable"):
            self.publish()
        self.assertEqual(self.api.branches(), ["trunk"])

    def test_a_symlink_is_refused(self):
        os.symlink("q1.snap", os.path.join(self.work, "snapshots/link.snap"))
        with self.assertRaisesRegex(sc.UsageError, "symlink"):
            self.publish()

    # ------------------------------------------------------------------ patch on failure

    def test_a_refused_branch_is_saved_as_a_patch_that_applies_onto_its_parent(self):
        self.write("snapshots/q1.snap", "q1 v2\n")
        self.write("snapshots/q8.snap", "q8 new\n")
        self.remove("snapshots/q2.snap")
        self.api.fail_create_branch = True
        with self.assertRaises(sc.BranchCreateError) as ctx:
            self.publish(branch="testoperator-snapshot-update/abc")
        patch_dir = os.path.join(self._tmp.name, "unpushed")
        summary = os.path.join(self._tmp.name, "summary.md")
        env = {"GITHUB_STEP_SUMMARY": summary, "GITHUB_RUN_ID": "4242"}
        with patched_env(env), contextlib.redirect_stderr(io.StringIO()) as err:
            sc.save_patch(self.git, self.last_request, ctx.exception, patch_dir, "unpushed-snapshots",
                          "spiceai/spiceai")
        self.assertIn("Resource not accessible by integration", err.getvalue())
        text = read_file(summary)
        self.assertIn("gh run download 4242 --repo spiceai/spiceai --name unpushed-snapshots", text)
        self.assertIn(f"git switch -c testoperator-snapshot-update/abc {self.base}", text)
        self.assertIn("`workflows` permission", text)
        clone = os.path.join(self._tmp.name, "landing")
        git(self._tmp.name, "clone", "-q", self.server, clone)
        git(clone, "switch", "-q", "-c", "landed", self.base)
        git(clone, "am", "-q", os.path.join(patch_dir, "changes.patch"))
        self.assertEqual(read_file(os.path.join(clone, "snapshots/q1.snap")), "q1 v2\n")
        self.assertEqual(read_file(os.path.join(clone, "snapshots/q8.snap")), "q8 new\n")
        self.assertFalse(os.path.exists(os.path.join(clone, "snapshots/q2.snap")))
        self.assertEqual(read_file(os.path.join(clone, "src/lib.rs")), "fn main() {}\n")

    def test_a_failed_commit_to_an_existing_branch_is_saved_on_the_branch_tip(self):
        tip = self.server_branch_commit("schema/1", {"snapshots/q9.snap": "q9 on the branch\n"})
        self.write("snapshots/q1.snap", "q1 v2\n")
        self.write("snapshots/q9.snap", "q9 regenerated\n")
        self.api.fail_commit_on_call = 1
        with self.assertRaises(sc.ApiError) as ctx:
            self.publish(branch="schema/1")
        patch_dir = os.path.join(self._tmp.name, "unpushed")
        with patched_env({"GITHUB_STEP_SUMMARY": None}), contextlib.redirect_stderr(io.StringIO()):
            sc.save_patch(self.git, self.last_request, ctx.exception, patch_dir, "a", "o/r")
        clone = os.path.join(self._tmp.name, "landing")
        git(self._tmp.name, "clone", "-q", self.server, clone)
        git(clone, "switch", "-q", "-c", "landed", tip)
        git(clone, "am", "-q", os.path.join(patch_dir, "changes.patch"))
        self.assertEqual(read_file(os.path.join(clone, "snapshots/q9.snap")), "q9 regenerated\n")
        self.assertEqual(read_file(os.path.join(clone, "snapshots/q1.snap")), "q1 v2\n")

    # ------------------------------------------------------------------ HTTP failures

    def test_a_truncated_response_is_an_api_error(self):
        # Promises 100 bytes of body, sends 10, and closes.
        with raw_http_server(b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n{\"ref\": 1", close=True) as url:
            api = sc.GitHub("o/r", "t", url, f"{url}/graphql")
            with self.assertRaisesRegex(sc.ApiError, "failed"):
                api.branch_tip("b")

    def test_a_response_that_stalls_mid_body_is_an_api_error(self):
        # Sends the headers and part of the body, then neither finishes nor closes.
        with raw_http_server(b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n{\"ref\": 1", close=False) as url:
            api = sc.GitHub("o/r", "t", url, f"{url}/graphql")
            api.timeout = 0.5
            with self.assertRaisesRegex(sc.ApiError, "failed"):
                api.branch_tip("b")

    def test_a_body_that_is_not_json_is_an_api_error(self):
        with raw_http_server(b"HTTP/1.1 200 OK\r\nContent-Length: 9\r\n\r\nnot json!", close=True) as url:
            api = sc.GitHub("o/r", "t", url, f"{url}/graphql")
            with self.assertRaisesRegex(sc.ApiError, "not JSON"):
                api.branch_tip("b")

    # ------------------------------------------------------------------ CLI

    def test_the_cli_refuses_to_run_outside_the_repository_root(self):
        os.makedirs(os.path.join(self.work, "snapshots"), exist_ok=True)
        with chdir(os.path.join(self.work, "snapshots")), patched_env(
                {"GH_TOKEN": "t", "GITHUB_REPOSITORY": "o/r"}), contextlib.redirect_stderr(io.StringIO()) as err:
            status = sc.main(["--branch", "b", "--message", "m", "--", "*.snap"])
        self.assertEqual(status, 2)
        self.assertIn("repository root", err.getvalue())

    def test_the_cli_needs_a_token(self):
        with chdir(self.work), patched_env({"GH_TOKEN": None, "GITHUB_TOKEN": None, "GITHUB_REPOSITORY": "o/r"}), \
                contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(sc.main(["--branch", "b", "--message", "m", "--", "*.snap"]), 2)


@contextlib.contextmanager
def raw_http_server(response, close):
    """Answer one request with exactly `response` bytes; then close the connection, or hold it open."""
    server = socket.socket()
    server.bind(("127.0.0.1", 0))
    server.listen(1)
    done = threading.Event()

    def serve():
        conn, _ = server.accept()
        conn.recv(65536)
        conn.sendall(response)
        if not close:
            done.wait(10)
        conn.close()

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.getsockname()[1]}"
    finally:
        done.set()
        thread.join(10)
        server.close()


def read_file(path):
    with open(path) as f:
        return f.read()


@contextlib.contextmanager
def patched_env(values):
    saved = {k: os.environ.get(k) for k in values}
    try:
        for k, v in values.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
        yield
    finally:
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


@contextlib.contextmanager
def chdir(path):
    old = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(old)


if __name__ == "__main__":
    unittest.main(verbosity=2)
