#!/usr/bin/env python3
# Copyright 2024-2026 The Spice.ai OSS Authors
#
# Cancel workflow runs left behind by superseded merge-queue batches.
#
# When the merge queue re-forms a batch, GitHub creates a new
# `gh-readonly-queue/<base>/pr-<N>-<sha>` branch and deletes the previous one.
# The workflow runs already triggered on the deleted branch are not cancelled:
# they run to completion against a batch that can never merge, holding
# self-hosted runners the whole time.
#
# Workflow-level `concurrency` cannot express this. Every generation of a batch
# gets its own branch name (`pr-12126-f925f912` -> `pr-12126-bef39e02` -> ...),
# so `github.ref` — and `github.ref_name`, which this repository already groups
# on — is unique per generation and two generations never share a group. A batch
# branch that no longer resolves is the signal instead: its runs are superseded,
# and cancelling them returns the runners to the live batches.
#
# Only runs that satisfy every one of these are cancelled:
#   * triggered by `merge_group`,
#   * on a `gh-readonly-queue/` branch,
#   * not yet completed,
#   * older than --min-age-minutes (so a run is never raced at creation),
#   * whose branch returns 404 from the branches API.
#
# Usage:
#   .github/scripts/reap_superseded_merge_queue_runs.py --dry-run   # report only
#   .github/scripts/reap_superseded_merge_queue_runs.py             # cancel
#
# Reads GITHUB_TOKEN (needs the `actions: write` scope) and GITHUB_REPOSITORY.
# Uses only the standard library so the workflow that holds `actions: write`
# installs no dependencies.

import argparse
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

API_URL = "https://api.github.com"
TIMEOUT_SECONDS = 30

BATCH_BRANCH_PREFIX = "gh-readonly-queue/"

# Every run status that is not `completed`. The runs API filters on one status
# per request, so each is queried separately. Listing without a status filter
# and discarding completed runs client-side would be one request instead of
# five, but the endpoint pages newest-first over *all* runs, so a long-stuck
# orphan is exactly the run that busy completed traffic pushes off the page.
UNFINISHED_STATUSES = ("queued", "in_progress", "waiting", "requested", "pending")

DEFAULT_MIN_AGE_MINUTES = 5
DEFAULT_MAX_CANCELLATIONS = 200

PAGE_SIZE = 100
MAX_PAGES = 20


class GitHubApiError(RuntimeError):
    """An unexpected response from the GitHub API."""


def parse_timestamp(value):
    """Parse a GitHub ISO-8601 timestamp into an aware UTC datetime, or None."""
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def is_batch_branch(branch):
    """True when `branch` is a merge-queue batch branch."""
    return isinstance(branch, str) and branch.startswith(BATCH_BRANCH_PREFIX)


class GitHubApi:
    def __init__(self, token, repository):
        self._token = token
        self._repository = repository

    def _request(self, method, path, allowed_statuses=()):
        request = urllib.request.Request(f"{API_URL}{path}", method=method)
        request.add_header("Authorization", f"Bearer {self._token}")
        request.add_header("Accept", "application/vnd.github+json")
        request.add_header("X-GitHub-Api-Version", "2022-11-28")
        try:
            with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
                body = response.read()
                return response.status, (json.loads(body) if body else None)
        except urllib.error.HTTPError as error:
            if error.code in allowed_statuses:
                return error.code, None
            raise GitHubApiError(
                f"{method} {path} failed with HTTP {error.code}"
            ) from error

    def list_unfinished_merge_group_runs(self):
        """Every `merge_group` run in the repository that has not completed."""
        runs = []
        for status in UNFINISHED_STATUSES:
            for page in range(1, MAX_PAGES + 1):
                _, payload = self._request(
                    "GET",
                    f"/repos/{self._repository}/actions/runs"
                    f"?event=merge_group&status={status}"
                    f"&per_page={PAGE_SIZE}&page={page}",
                )
                batch = (payload or {}).get("workflow_runs") or []
                runs.extend(batch)
                if len(batch) < PAGE_SIZE:
                    break
            else:
                # The page cap was reached without a short page, so runs older
                # than the ones listed may exist and this pass cannot see them.
                # Say so: the whole point of the reaper is the long-stuck
                # orphan, and silently dropping the tail of a newest-first
                # listing is exactly how that orphan goes unreaped.
                print(
                    f"WARNING: stopped listing {status} runs at the "
                    f"{MAX_PAGES}-page cap ({MAX_PAGES * PAGE_SIZE} runs); "
                    "older superseded runs may remain",
                    file=sys.stderr,
                )
        return runs

    def branch_exists(self, branch):
        """True while the branch resolves; False once the queue has deleted it."""
        quoted = urllib.parse.quote(branch, safe="/")
        status, _ = self._request(
            "GET",
            f"/repos/{self._repository}/branches/{quoted}",
            allowed_statuses=(404, 422),
        )
        return status == 200

    def cancel_run(self, run_id):
        """Cancel a run. A run that finished first reports 409 and is not an error."""
        status, _ = self._request(
            "POST",
            f"/repos/{self._repository}/actions/runs/{run_id}/cancel",
            allowed_statuses=(409,),
        )
        return status != 409


def select_superseded_runs(
    runs,
    branch_exists,
    now,
    min_age_minutes=DEFAULT_MIN_AGE_MINUTES,
    max_cancellations=DEFAULT_MAX_CANCELLATIONS,
):
    """The runs whose merge-queue batch branch has already been replaced.

    `branch_exists` is called at most once per distinct branch, so a queue with
    many runs per batch costs one API call per batch rather than per run.
    """
    cutoff = now - timedelta(minutes=min_age_minutes)
    branch_is_live = {}
    seen_run_ids = set()
    superseded = []

    for run in runs:
        run_id = run.get("id")
        if run_id is None or run_id in seen_run_ids:
            continue
        seen_run_ids.add(run_id)

        if run.get("event") != "merge_group":
            continue

        branch = run.get("head_branch")
        if not is_batch_branch(branch):
            continue

        created_at = parse_timestamp(run.get("created_at"))
        if created_at is None or created_at > cutoff:
            continue

        if branch not in branch_is_live:
            branch_is_live[branch] = branch_exists(branch)
        if branch_is_live[branch]:
            continue

        superseded.append(run)
        if len(superseded) >= max_cancellations:
            break

    return superseded


def describe(run):
    return (
        f"run {run.get('id')} "
        f"[{run.get('name') or run.get('workflow_id')}] "
        f"on {run.get('head_branch')} ({run.get('status')})"
    )


def cancel_runs(api, runs, dry_run):
    """Cancel each run, returning the ones the API accepted."""
    cancelled = []
    for run in runs:
        if dry_run:
            print(f"would cancel {describe(run)}")
            cancelled.append(run)
            continue
        if api.cancel_run(run["id"]):
            print(f"cancelled {describe(run)}")
            cancelled.append(run)
        else:
            print(f"already finished, left alone: {describe(run)}")
    return cancelled


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Cancel workflow runs left behind by superseded "
        "merge-queue batches."
    )
    parser.add_argument(
        "--repository",
        default=os.getenv("GITHUB_REPOSITORY"),
        help="owner/repo (defaults to $GITHUB_REPOSITORY)",
    )
    parser.add_argument(
        "--min-age-minutes",
        type=int,
        default=DEFAULT_MIN_AGE_MINUTES,
        help="leave runs younger than this alone",
    )
    parser.add_argument(
        "--max-cancellations",
        type=int,
        default=DEFAULT_MAX_CANCELLATIONS,
        help="stop after cancelling this many runs",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="report what would be cancelled without cancelling it",
    )
    args = parser.parse_args(argv)

    token = os.getenv("GITHUB_TOKEN")
    if not token:
        print("ERROR: Specify GITHUB_TOKEN environment variable", file=sys.stderr)
        return 1
    if not args.repository:
        print("ERROR: Specify --repository or GITHUB_REPOSITORY", file=sys.stderr)
        return 1

    api = GitHubApi(token, args.repository)
    runs = api.list_unfinished_merge_group_runs()
    superseded = select_superseded_runs(
        runs,
        api.branch_exists,
        datetime.now(timezone.utc),
        min_age_minutes=args.min_age_minutes,
        max_cancellations=args.max_cancellations,
    )

    print(f"{len(runs)} unfinished merge_group run(s); {len(superseded)} superseded")
    cancelled = cancel_runs(api, superseded, args.dry_run)
    print(f"{len(cancelled)} run(s) {'would be ' if args.dry_run else ''}cancelled")

    if len(superseded) >= args.max_cancellations:
        print(
            f"stopped at the --max-cancellations limit of {args.max_cancellations}; "
            "re-run to reap the rest"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
