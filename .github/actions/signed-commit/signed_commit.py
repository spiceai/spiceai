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
"""Commit working-tree files to a GitHub branch as commits GitHub signs.

A `git push` from CI publishes commits nobody signed, which a branch ruleset that
requires signed commits refuses. A commit made through the GraphQL
`createCommitOnBranch` mutation is signed by GitHub instead (signer `web-flow`) and
shows as Verified, with the token's identity (`github-actions[bot]` for
`GITHUB_TOKEN`, `<app>[bot]` for an app token) as its author.

    signed_commit.py --branch BRANCH --message MESSAGE [options] -- PATHSPEC...

The new commit makes every file matched by PATHSPEC look like the working tree
does: new and modified files are added with their working-tree content (after
the repository's clean filters, as `git add` stores them), and files that are
gone are deleted. Files outside PATHSPEC are left as the parent commit has them.
Only regular, non-executable files can be committed this way: the API takes no
file mode, so an executable, a symlink or a submodule is refused rather than
committed with a different mode.

The parent commit:

* `--parent SHA`: build on SHA. The branch must point at SHA, or not exist, in
  which case it is created there.
* Otherwise the branch's current tip, or the local HEAD when the branch does not
  exist yet (it is created at HEAD). A concurrent push to the branch is retried:
  the tip is read again and the commit rebuilt on it.

`--changed-since REV` narrows the commit to the files this run changed: those
under PATHSPEC whose working-tree content differs from REV (usually HEAD, the
commit the run checked out). Each is then written onto the parent, like a
rebase of the run's own changes. A file the branch also changed since REV is a
conflict (exit 3), unless the branch already has this run's content.

The API takes every file's content inline in one request, so a large change is
split into commits of at most `--max-commit-bytes` of encoded content. A split
change, and every commit to a new branch, is built on a temporary branch of the
run's own; the target branch is created or fast-forwarded to the last commit only
when all of them exist. The branch never shows part of a change, and a failed
commit never leaves, or has to delete, a target branch another run may share.

When the change cannot be published, `--patch-dir DIR` keeps it: DIR receives
`changes.patch`, a `git format-patch` of one commit on the parent, and the step
summary says how to land it with `git am`.

Prints the commit the branch now points at, or nothing when there was nothing to
commit. The token comes from `GH_TOKEN` or `GITHUB_TOKEN`; the repository from
`--repo` or `GITHUB_REPOSITORY`.

Exit status: 0 committed, or nothing to commit; 1 the change was not published;
2 usage error; 3 conflict with the branch (see `--changed-since`).
"""

from __future__ import annotations

import argparse
import base64
import http.client
import json
import os
import random
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Callable, Optional

# The only mode the API can write. Anything else would change mode on the branch.
REGULAR_FILE = "100644"
NULL_SHA = "0" * 40
# Encoded content per createCommitOnBranch request. GitHub does not document the
# request limit; this stays well under the sizes routine API clients send.
DEFAULT_MAX_COMMIT_BYTES = 4 * 1024 * 1024
# Per-file JSON overhead counted against the budget besides the encoded content.
PER_FILE_OVERHEAD = 64
RACE_ATTEMPTS = 8
TEMP_BRANCH_PREFIX = "signed-commit-tmp"

BOT_NAME = "github-actions[bot]"
BOT_EMAIL = "41898282+github-actions[bot]@users.noreply.github.com"

CREATE_COMMIT = """
mutation($input: CreateCommitOnBranchInput!) {
  createCommitOnBranch(input: $input) { commit { oid } }
}
"""


class SignedCommitError(Exception):
    """The change was not published."""

    exit_code = 1


class UsageError(SignedCommitError):
    exit_code = 2


class ConflictError(SignedCommitError):
    exit_code = 3


class ApiError(SignedCommitError):
    def __init__(self, message: str, status: Optional[int] = None):
        super().__init__(message)
        self.status = status


@dataclass(frozen=True)
class Change:
    """One file of the commit: its new blob, or None to delete it."""

    path: str
    blob: Optional[str]
    size: int = 0

    def encoded_size(self) -> int:
        return (self.size + 2) // 3 * 4 + len(self.path.encode()) + PER_FILE_OVERHEAD


# --------------------------------------------------------------------------- git


class Git:
    def __init__(self, cwd: str = "."):
        self.cwd = cwd

    def run(self, *args: str, env: Optional[dict] = None, input: Optional[bytes] = None) -> bytes:
        proc = subprocess.run(
            ["git", *args],
            cwd=self.cwd,
            env=env,
            input=input,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if proc.returncode != 0:
            raise SignedCommitError(
                f"git {' '.join(args[:3])} failed: {proc.stderr.decode(errors='replace').strip()}"
            )
        return proc.stdout

    def text(self, *args: str, env: Optional[dict] = None) -> str:
        return self.run(*args, env=env).decode().strip()

    def has_commit(self, rev: str) -> bool:
        proc = subprocess.run(
            ["git", "cat-file", "-e", f"{rev}^{{commit}}"],
            cwd=self.cwd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        return proc.returncode == 0

    def resolve(self, rev: str) -> str:
        return self.text("rev-parse", "--verify", f"{rev}^{{commit}}")

    def tree_files(self, commit: str) -> dict:
        """Every file of commit's tree: path -> (mode, blob)."""
        files = {}
        for entry in self.run("ls-tree", "-r", "-z", "--full-tree", commit).split(b"\0"):
            if not entry:
                continue
            meta, path = entry.split(b"\t", 1)
            mode, _kind, blob = meta.decode().split(" ")
            files[path.decode()] = (mode, blob)
        return files

    def blob_bytes(self, blob: str) -> bytes:
        return self.run("cat-file", "blob", blob)

    def working_tree_changes(self, base: str, paths: list) -> list:
        """The files under paths whose working-tree content differs from base's.

        Uses a scratch index read from base, so the caller's index is untouched.
        Returns (status, old_mode, new_mode, old_blob, new_blob, path) tuples.
        """
        with tempfile.TemporaryDirectory(prefix="signed-commit-") as scratch:
            env = {**os.environ, "GIT_INDEX_FILE": os.path.join(scratch, "index")}
            self.run("read-tree", base, env=env)
            # Tracked files that changed or went missing, and new untracked files the
            # repository does not ignore. `update-index` hashes them through the same
            # clean filters `git add` uses, and drops the missing ones.
            listed = self.run(
                "ls-files", "-z", "--modified", "--deleted", "--others", "--exclude-standard",
                "--", *paths, env=env,
            )
            candidates = sorted({p for p in listed.split(b"\0") if p})
            if candidates:
                self.run(
                    "update-index", "--add", "--remove", "-z", "--stdin",
                    env=env, input=b"\0".join(candidates) + b"\0",
                )
            raw = self.run(
                "diff-index", "--cached", "--no-renames", "--raw", "-z", base, "--", *paths, env=env,
            )
        fields = raw.split(b"\0")
        out = []
        i = 0
        while i < len(fields) - 1:
            meta = fields[i].decode()
            if not meta:
                i += 1
                continue
            old_mode, new_mode, old_blob, new_blob, status = meta.lstrip(":").split(" ")
            out.append((status, old_mode, new_mode, old_blob, new_blob, fields[i + 1].decode()))
            i += 2
        return out

    def blob_size(self, blob: str) -> int:
        return int(self.text("cat-file", "-s", blob))


def check_mode(path: str, mode: str, where: str) -> None:
    if mode != REGULAR_FILE:
        kind = {"100755": "an executable file", "120000": "a symlink", "160000": "a submodule"}.get(
            mode, f"mode {mode}"
        )
        raise UsageError(
            f"{path} is {kind} {where}. A signed commit made through the GitHub API can only "
            f"write regular, non-executable files, so it is not committed this way."
        )


def changes_against(git: Git, base: str, paths: list) -> list:
    """The changes that make paths on base look like the working tree."""
    changes = []
    for status, old_mode, new_mode, _old_blob, new_blob, path in git.working_tree_changes(base, paths):
        if status == "D":
            if old_mode == "160000":
                check_mode(path, old_mode, "in the parent commit")
            changes.append(Change(path, None))
            continue
        if status not in ("A", "M"):
            raise UsageError(f"{path} changed type ({status}); a signed commit cannot write that change.")
        check_mode(path, new_mode, "in the working tree")
        if status == "M":
            check_mode(path, old_mode, "in the parent commit")
        changes.append(Change(path, new_blob, git.blob_size(new_blob)))
    return changes


def plan_changes(git: Git, parent: str, since: Optional[str], paths: list) -> list:
    """The file changes of the commit on parent."""
    if since is None:
        return changes_against(git, parent, paths)
    ours = changes_against(git, since, paths)
    if not ours or since == parent:
        return ours
    since_files = git.tree_files(since)
    parent_files = git.tree_files(parent)
    planned, conflicts = [], []
    for change in ours:
        theirs = parent_files.get(change.path)
        base = since_files.get(change.path)
        if (theirs[1] if theirs else None) == change.blob:
            continue  # The branch already has this content.
        if theirs != base:
            conflicts.append(change.path)
            continue
        if theirs is not None:
            check_mode(change.path, theirs[0], "on the branch")
        planned.append(change)
    if conflicts:
        listed = ", ".join(conflicts[:10]) + (f" and {len(conflicts) - 10} more" if len(conflicts) > 10 else "")
        raise ConflictError(
            f"The branch changed {len(conflicts)} of this run's files since {since[:12]} too: {listed}. "
            f"Resolve them on the branch by hand."
        )
    return planned


def split_changes(changes: list, max_bytes: int) -> list:
    """Groups of changes, each within max_bytes of encoded content where a file allows."""
    chunks, current, used = [], [], 0
    for change in changes:
        size = change.encoded_size()
        if current and used + size > max_bytes:
            chunks.append(current)
            current, used = [], 0
        current.append(change)
        used += size
    if current:
        chunks.append(current)
    return chunks


# --------------------------------------------------------------------------- GitHub API


class GitHub:
    def __init__(self, repo: str, token: str, api_url: str, graphql_url: str):
        self.repo = repo
        self.token = token
        self.api_url = api_url.rstrip("/")
        self.graphql_url = graphql_url
        self.timeout = 300

    def _request(self, method: str, url: str, payload: Optional[dict] = None):
        data = json.dumps(payload).encode() if payload is not None else None
        request = urllib.request.Request(url, data=data, method=method)
        request.add_header("Authorization", f"Bearer {self.token}")
        request.add_header("Accept", "application/vnd.github+json")
        request.add_header("X-GitHub-Api-Version", "2022-11-28")
        if data is not None:
            request.add_header("Content-Type", "application/json")
        where = f"{method} {urllib.parse.urlparse(url).path}"
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                body = response.read()
        except urllib.error.HTTPError as err:
            try:
                detail = err.read().decode(errors="replace")[:500]
            except (OSError, http.client.HTTPException) as read_err:
                detail = f"(body unreadable: {read_err!r})"
            raise ApiError(f"{where} returned {err.code}: {detail}", err.code) from err
        except (OSError, http.client.HTTPException) as err:
            # URLError (connecting) is an OSError, and so is a timeout while reading the
            # body; a body cut short is an HTTPException (IncompleteRead). Each must reach
            # the caller as an ApiError, or neither the retry nor --patch-dir applies.
            raise ApiError(f"{where} failed: {getattr(err, 'reason', None) or repr(err)}") from err
        if not body:
            return None
        try:
            return json.loads(body)
        except ValueError as err:
            raise ApiError(f"{where} returned a body that is not JSON: {body[:200]!r}") from err

    def _ref_url(self, branch: str) -> str:
        return f"{self.api_url}/repos/{self.repo}/git/refs/heads/{urllib.parse.quote(branch, safe='/')}"

    def branch_tip(self, branch: str) -> Optional[str]:
        url = f"{self.api_url}/repos/{self.repo}/git/ref/heads/{urllib.parse.quote(branch, safe='/')}"
        try:
            ref = self._request("GET", url)
        except ApiError as err:
            if err.status == 404:
                return None
            raise
        return ref["object"]["sha"]

    def create_branch(self, branch: str, sha: str) -> None:
        self._request(
            "POST", f"{self.api_url}/repos/{self.repo}/git/refs", {"ref": f"refs/heads/{branch}", "sha": sha}
        )

    def fast_forward(self, branch: str, sha: str) -> None:
        self._request("PATCH", self._ref_url(branch), {"sha": sha, "force": False})

    def delete_branch(self, branch: str) -> None:
        self._request("DELETE", self._ref_url(branch))

    def create_commit(self, branch: str, expected_head: str, headline: str, body: str, changes: list, read_blob) -> str:
        additions = [
            {"path": c.path, "contents": base64.b64encode(read_blob(c.blob)).decode()}
            for c in changes
            if c.blob is not None
        ]
        deletions = [{"path": c.path} for c in changes if c.blob is None]
        variables = {
            "input": {
                "branch": {"repositoryNameWithOwner": self.repo, "branchName": branch},
                "expectedHeadOid": expected_head,
                "message": {"headline": headline, "body": body},
                "fileChanges": {"additions": additions, "deletions": deletions},
            }
        }
        result = self._request("POST", self.graphql_url, {"query": CREATE_COMMIT, "variables": variables})
        errors = (result or {}).get("errors")
        if errors:
            raise ApiError("createCommitOnBranch: " + "; ".join(e.get("message", str(e)) for e in errors))
        return result["data"]["createCommitOnBranch"]["commit"]["oid"]


# --------------------------------------------------------------------------- publishing


@dataclass
class Request:
    branch: str
    message: str
    paths: list
    parent: Optional[str] = None
    changed_since: Optional[str] = None
    max_commit_bytes: int = DEFAULT_MAX_COMMIT_BYTES
    run_id: str = ""
    run_url: str = ""
    # The parent the last attempt built on, so a saved patch applies where the commit would have.
    attempted_parent: Optional[str] = None


def split_message(message: str, run_url: str) -> tuple:
    headline, _, body = message.strip().partition("\n")
    body = body.strip()
    if run_url:
        body = f"{body}\n\nWorkflow run: {run_url}".strip()
    return headline.strip(), body


def publish(
    git: Git,
    api: GitHub,
    req: Request,
    fetch: Callable[[str], None],
    sleep: Callable[[float], None] = time.sleep,
    log: Callable[[str], None] = lambda m: print(m, file=sys.stderr),
) -> Optional[str]:
    """Commit the change onto req.branch. Returns the branch's new tip, or None."""
    headline, body = split_message(req.message, req.run_url)
    since = git.resolve(req.changed_since) if req.changed_since else None
    explicit = req.parent is not None
    for attempt in range(1, RACE_ATTEMPTS + 1):
        tip = api.branch_tip(req.branch)
        if explicit:
            parent = req.parent
            if tip is not None and tip != parent:
                raise SignedCommitError(
                    f"{req.branch} points at {tip}, not at the parent {parent} this commit was asked to build on."
                )
        else:
            parent = tip or git.resolve("HEAD")
        if not git.has_commit(parent):
            fetch(parent)
        req.attempted_parent = parent
        changes = plan_changes(git, parent, since, req.paths)
        if not changes:
            log(f"Nothing to commit: {req.branch or 'the branch'} already has the content of {' '.join(req.paths)}.")
            # After a lost race the tip may hold this run's commit already.
            return tip if attempt > 1 else None
        chunks = split_changes(changes, req.max_commit_bytes)
        try:
            new_tip = _commit_chunks(api, git, req, parent, tip, chunks, headline, body, attempt, log)
        except ApiError as err:
            if explicit or api.branch_tip(req.branch) == tip:
                raise
            log(f"{req.branch} moved while committing (attempt {attempt}); rebuilding on its new tip. ({err})")
            sleep(random.uniform(1, 5))
            continue
        log(f"Committed {len(changes)} file(s) to {req.branch} in {len(chunks)} commit(s); tip {new_tip}.")
        return new_tip
    raise SignedCommitError(f"{req.branch} kept moving; gave up after {RACE_ATTEMPTS} attempts.")


class BranchCreateError(ApiError):
    """GitHub refused to create a branch."""


def _create_branch(api, branch: str, sha: str) -> None:
    try:
        api.create_branch(branch, sha)
    except ApiError as err:
        raise BranchCreateError(f"creating branch {branch} at {sha}: {err}", err.status) from err


def _commit_chunks(api, git, req, parent, tip, chunks, headline, body, attempt, log) -> str:
    if tip is not None and len(chunks) == 1:
        # expectedHeadOid makes this one atomic step: the branch moves to the new commit,
        # or the request fails and the branch is untouched.
        return api.create_commit(req.branch, parent, headline, body, chunks[0], git.blob_bytes)
    # A new branch, or a change in several parts: build on a temporary branch of this
    # run's own, then create or fast-forward the target only once every commit exists.
    # A failure leaves the target as it was, so there is never a target branch to clean
    # up, and no cleanup can delete a commit another run published to it meanwhile.
    temp = f"{TEMP_BRANCH_PREFIX}/{req.run_id or os.getpid()}-{attempt}-{random.randrange(1 << 32):08x}"
    _create_branch(api, temp, parent)
    try:
        sha = parent
        for i, chunk in enumerate(chunks, 1):
            message = body
            if len(chunks) > 1:
                part = f"Part {i} of {len(chunks)} of one change, split to fit the API's request size."
                message = f"{body}\n\n{part}".strip()
            sha = api.create_commit(temp, sha, headline, message, chunk, git.blob_bytes)
        if tip is None:
            _create_branch(api, req.branch, sha)
        else:
            api.fast_forward(req.branch, sha)
        return sha
    finally:
        try:
            api.delete_branch(temp)
        except ApiError as err:
            log(f"::warning::Could not delete the temporary branch {temp}: {err}")


def save_patch(git: Git, req: Request, error: SignedCommitError, patch_dir: str, artifact: str, repo: str) -> None:
    """Write the unpublished change as a patch on its parent, and say how to land it."""
    parent = req.attempted_parent or req.parent or git.resolve("HEAD")
    changes = plan_changes(git, parent, git.resolve(req.changed_since) if req.changed_since else None, req.paths)
    if not changes:
        return
    headline, body = split_message(req.message, req.run_url)
    with tempfile.TemporaryDirectory(prefix="signed-commit-patch-") as scratch:
        env = {**os.environ, "GIT_INDEX_FILE": os.path.join(scratch, "index")}
        git.run("read-tree", parent, env=env)
        index_info = "".join(
            f"{REGULAR_FILE} {c.blob}\t{c.path}\n" if c.blob else f"0 {NULL_SHA}\t{c.path}\n" for c in changes
        )
        git.run("update-index", "--index-info", env=env, input=index_info.encode())
        tree = git.text("write-tree", env=env)
    identity = {
        "GIT_AUTHOR_NAME": BOT_NAME, "GIT_AUTHOR_EMAIL": BOT_EMAIL,
        "GIT_COMMITTER_NAME": BOT_NAME, "GIT_COMMITTER_EMAIL": BOT_EMAIL,
    }
    commit = git.run(
        "commit-tree", tree, "-p", parent, env={**os.environ, **identity},
        input=f"{headline}\n\n{body}\n".encode(),
    ).decode().strip()
    os.makedirs(patch_dir, exist_ok=True)
    patch = os.path.join(patch_dir, "changes.patch")
    with open(patch, "wb") as out:
        out.write(git.run("format-patch", "-1", commit, "--stdout"))
    print(f"::error::The change to {req.branch} ({len(changes)} file(s)) was not published, so it is saved as "
          f"{patch} for the '{artifact}' artifact. GitHub said: {error}", file=sys.stderr)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if not summary:
        return
    run = os.environ.get("GITHUB_RUN_ID", "<run-id>")
    hint = ""
    if isinstance(error, BranchCreateError):
        hint = ("GitHub refused to create a branch. One known cause: when the branch starts from a commit far "
                "from the default branch (a release branch), GitHub cannot confirm in time that it adds no "
                "workflow files, which a token without the `workflows` permission must show, and retrying "
                "does not help.\n\n")
    with open(summary, "a") as out:
        out.write(
            f"### Not published: `{req.branch}`\n\n"
            f"GitHub refused the change ({len(changes)} file(s)): `{error}`\n\n{hint}"
            f"To land it with your own credentials:\n\n```bash\n"
            f"gh run download {run} --repo {repo} --name {artifact}\n"
            f"git fetch origin {parent}\n"
            f"git switch -c {req.branch} {parent}\n"
            f"git am changes.patch\n"
            f"git push origin {req.branch}\n```\n"
        )


# --------------------------------------------------------------------------- CLI


def git_fetcher(git: Git, repo: str, token: str, server_url: str) -> Callable[[str], None]:
    """Fetch one commit by SHA, authenticating with token and never prompting."""
    header = "AUTHORIZATION: basic " + base64.b64encode(f"x-access-token:{token}".encode()).decode()
    server = server_url.rstrip("/")

    def fetch(sha: str) -> None:
        # The empty values drop any header actions/checkout persisted, so exactly one
        # Authorization header is sent; the empty helper keeps a runner's credential
        # helper from prompting.
        git.run(
            "-c", "credential.helper=", "-c", "http.extraheader=",
            "-c", f"http.{server}/.extraheader=", "-c", f"http.{server}/.extraheader={header}",
            "fetch", "--quiet", "--no-tags", "--depth=1", f"{server}/{repo}", sha,
            env={**os.environ, "GIT_TERMINAL_PROMPT": "0", "GCM_INTERACTIVE": "never"},
        )

    return fetch


def main(argv: Optional[list] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--repo", default=os.environ.get("GITHUB_REPOSITORY"))
    parser.add_argument("--branch", required=True)
    parser.add_argument("--message", required=True)
    parser.add_argument("--parent")
    parser.add_argument("--changed-since")
    parser.add_argument("--max-commit-bytes", type=int, default=DEFAULT_MAX_COMMIT_BYTES)
    parser.add_argument("--patch-dir")
    parser.add_argument("--patch-artifact", default="unpublished-changes")
    parser.add_argument("paths", nargs="+")
    args = parser.parse_args(argv)

    token = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN")
    if not args.repo or not token:
        print("::error::signed_commit.py needs --repo (or GITHUB_REPOSITORY) and GH_TOKEN or GITHUB_TOKEN.",
              file=sys.stderr)
        return 2
    if args.max_commit_bytes <= 0 or not args.message.strip() or args.branch.startswith("refs/"):
        print("::error::signed_commit.py: --max-commit-bytes must be positive, --message non-empty, and --branch "
              "a branch name without refs/heads/.", file=sys.stderr)
        return 2

    server = os.environ.get("GITHUB_SERVER_URL", "https://github.com")
    run_id = os.environ.get("GITHUB_RUN_ID", "")
    req = Request(
        branch=args.branch,
        message=args.message,
        paths=args.paths,
        parent=args.parent,
        changed_since=args.changed_since,
        max_commit_bytes=args.max_commit_bytes,
        run_id=run_id,
        run_url=f"{server}/{args.repo}/actions/runs/{run_id}" if run_id else "",
    )
    git = Git()
    try:
        prefix = git.text("rev-parse", "--show-prefix")
    except SignedCommitError as err:
        print(f"::error::signed_commit.py must run inside a git checkout: {err}", file=sys.stderr)
        return 2
    if prefix:
        # Paths in the commit are relative to the repository root, and so must the pathspecs be.
        print(f"::error::signed_commit.py must run from the repository root, not from {prefix}.", file=sys.stderr)
        return 2
    api = GitHub(
        args.repo,
        token,
        os.environ.get("GITHUB_API_URL", "https://api.github.com"),
        os.environ.get("GITHUB_GRAPHQL_URL", "https://api.github.com/graphql"),
    )
    try:
        tip = publish(git, api, req, git_fetcher(git, args.repo, token, server))
    except SignedCommitError as err:
        print(f"::error::Could not commit to {args.branch}: {err}", file=sys.stderr)
        if args.patch_dir and not isinstance(err, UsageError):
            try:
                save_patch(git, req, err, args.patch_dir, args.patch_artifact, args.repo)
            except SignedCommitError as save_err:
                print(f"::error::The change could not be saved as a patch either: {save_err}", file=sys.stderr)
        return err.exit_code
    if tip:
        print(tip)
    return 0


if __name__ == "__main__":
    sys.exit(main())
