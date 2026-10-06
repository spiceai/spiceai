#!/usr/bin/env python3
# Copyright 2024-2026 The Spice.ai OSS Authors
#
# Unit tests for reap_superseded_merge_queue_runs.py.

import contextlib
import io
import json
import os
import sys
import unittest
import unittest.mock
import urllib.error
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import reap_superseded_merge_queue_runs as reaper  # noqa: E402

NOW = datetime(2026, 7, 29, 17, 8, 0, tzinfo=timezone.utc)

LIVE_BRANCH = "gh-readonly-queue/trunk/pr-12126-aa58e347c18587bc1d058cc7ac244e"
SUPERSEDED_BRANCH = "gh-readonly-queue/trunk/pr-12126-f925f91252bbb914e6d43e8"


def make_run(
    run_id,
    branch=SUPERSEDED_BRANCH,
    event="merge_group",
    minutes_old=60,
    status="in_progress",
):
    created_at = NOW - timedelta(minutes=minutes_old)
    return {
        "id": run_id,
        "name": "integration tests",
        "event": event,
        "head_branch": branch,
        "status": status,
        "created_at": created_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


class BranchOracle:
    """Stands in for the branches API, recording which branches were queried."""

    def __init__(self, live_branches):
        self.live_branches = set(live_branches)
        self.queried = []

    def __call__(self, branch):
        self.queried.append(branch)
        return branch in self.live_branches


class IsBatchBranchTests(unittest.TestCase):
    def test_batch_branches_are_recognized(self):
        self.assertTrue(reaper.is_batch_branch(LIVE_BRANCH))

    def test_other_branches_are_not(self):
        for branch in ("trunk", "fix/12170-reaper", "", None, 42):
            self.assertFalse(reaper.is_batch_branch(branch), branch)


class ParseTimestampTests(unittest.TestCase):
    def test_parses_zulu_timestamp_as_utc(self):
        parsed = reaper.parse_timestamp("2026-07-29T16:13:35Z")

        self.assertEqual(parsed, datetime(2026, 7, 29, 16, 13, 35, tzinfo=timezone.utc))

    def test_parses_offset_timestamp(self):
        parsed = reaper.parse_timestamp("2026-07-29T18:13:35+02:00")

        self.assertEqual(parsed, datetime(2026, 7, 29, 16, 13, 35, tzinfo=timezone.utc))

    def test_returns_none_for_missing_or_malformed(self):
        for value in (None, "", "not a timestamp"):
            self.assertIsNone(reaper.parse_timestamp(value), value)


class SelectSupersededRunsTests(unittest.TestCase):
    def select(self, runs, live_branches=(), **kwargs):
        self.oracle = BranchOracle(live_branches)
        return reaper.select_superseded_runs(runs, self.oracle, NOW, **kwargs)

    def test_selects_runs_whose_batch_branch_is_gone(self):
        selected = self.select([make_run(1), make_run(2)])

        self.assertEqual([run["id"] for run in selected], [1, 2])

    def test_leaves_runs_on_a_live_batch_alone(self):
        selected = self.select(
            [make_run(1, branch=LIVE_BRANCH)], live_branches=[LIVE_BRANCH]
        )

        self.assertEqual(selected, [])

    def test_two_generations_of_one_queue_entry_are_judged_separately(self):
        # The bug this guards: both branches belong to PR 12126 and differ only
        # by the base sha, so no `concurrency` group can tell them apart. Only
        # the generation whose branch was deleted may be cancelled.
        runs = [make_run(1, branch=LIVE_BRANCH), make_run(2, branch=SUPERSEDED_BRANCH)]

        selected = self.select(runs, live_branches=[LIVE_BRANCH])

        self.assertEqual([run["id"] for run in selected], [2])

    def test_ignores_runs_from_other_events(self):
        runs = [
            make_run(1, event="pull_request"),
            make_run(2, event="push"),
            make_run(3, event="schedule"),
        ]

        self.assertEqual(self.select(runs), [])

    def test_ignores_runs_outside_the_merge_queue_namespace(self):
        runs = [make_run(1, branch="trunk"), make_run(2, branch=None)]

        self.assertEqual(self.select(runs), [])
        self.assertEqual(self.oracle.queried, [])

    def test_leaves_freshly_created_runs_alone(self):
        runs = [make_run(1, minutes_old=1), make_run(2, minutes_old=30)]

        selected = self.select(runs, min_age_minutes=5)

        self.assertEqual([run["id"] for run in selected], [2])

    def test_ignores_a_run_with_no_usable_created_at(self):
        run = make_run(1)
        del run["created_at"]

        self.assertEqual(self.select([run]), [])

    def test_queries_each_branch_once(self):
        runs = [make_run(run_id) for run_id in range(1, 7)]
        runs.append(make_run(7, branch=LIVE_BRANCH))

        self.select(runs, live_branches=[LIVE_BRANCH])

        self.assertEqual(self.oracle.queried, [SUPERSEDED_BRANCH, LIVE_BRANCH])

    def test_deduplicates_runs_by_id(self):
        selected = self.select([make_run(1), make_run(1), make_run(2)])

        self.assertEqual([run["id"] for run in selected], [1, 2])

    def test_stops_at_the_cancellation_cap(self):
        runs = [make_run(run_id) for run_id in range(1, 11)]

        selected = self.select(runs, max_cancellations=3)

        self.assertEqual(len(selected), 3)

    def test_no_runs_selects_nothing(self):
        self.assertEqual(self.select([]), [])


class FakeApi:
    def __init__(self, refused=()):
        self.refused = set(refused)
        self.cancelled = []

    def cancel_run(self, run_id):
        self.cancelled.append(run_id)
        return run_id not in self.refused


class CancelRunsTests(unittest.TestCase):
    def setUp(self):
        patcher = unittest.mock.patch("sys.stdout", new_callable=io.StringIO)
        self.addCleanup(patcher.stop)
        patcher.start()

    def test_cancels_every_run(self):
        api = FakeApi()

        cancelled = reaper.cancel_runs(api, [make_run(1), make_run(2)], dry_run=False)

        self.assertEqual(api.cancelled, [1, 2])
        self.assertEqual([run["id"] for run in cancelled], [1, 2])

    def test_dry_run_cancels_nothing(self):
        api = FakeApi()

        cancelled = reaper.cancel_runs(api, [make_run(1)], dry_run=True)

        self.assertEqual(api.cancelled, [])
        self.assertEqual([run["id"] for run in cancelled], [1])

    def test_a_run_that_finished_first_is_not_counted(self):
        api = FakeApi(refused=[2])

        cancelled = reaper.cancel_runs(api, [make_run(1), make_run(2)], dry_run=False)

        self.assertEqual([run["id"] for run in cancelled], [1])


def http_error(code):
    return urllib.error.HTTPError("https://api.github.com", code, "", None, None)


class FakeResponse(io.BytesIO):
    def __init__(self, status, body=b""):
        super().__init__(body)
        self.status = status

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
        return False


class GitHubApiTests(unittest.TestCase):
    def setUp(self):
        self.api = reaper.GitHubApi("test-token", "spiceai/spiceai")

    @unittest.mock.patch("urllib.request.urlopen")
    def test_branch_exists_when_the_branch_resolves(self, urlopen):
        urlopen.return_value = FakeResponse(200, b'{"name": "x"}')

        self.assertTrue(self.api.branch_exists(LIVE_BRANCH))

    @unittest.mock.patch("urllib.request.urlopen")
    def test_branch_does_not_exist_once_deleted(self, urlopen):
        urlopen.side_effect = http_error(404)

        self.assertFalse(self.api.branch_exists(SUPERSEDED_BRANCH))

    @unittest.mock.patch("urllib.request.urlopen")
    def test_branch_slashes_are_not_escaped(self, urlopen):
        urlopen.return_value = FakeResponse(200, b'{"name": "x"}')

        self.api.branch_exists(LIVE_BRANCH)

        requested_url = urlopen.call_args[0][0].full_url
        self.assertTrue(requested_url.endswith(f"/branches/{LIVE_BRANCH}"))

    @unittest.mock.patch("urllib.request.urlopen")
    def test_an_unexpected_status_is_an_error_not_a_missing_branch(self, urlopen):
        # A 401 must never be read as "the branch is gone", which would cancel
        # every live batch in the repository.
        urlopen.side_effect = http_error(401)

        with self.assertRaises(reaper.GitHubApiError):
            self.api.branch_exists(LIVE_BRANCH)

    @unittest.mock.patch("urllib.request.urlopen")
    def test_cancel_run_reports_success(self, urlopen):
        urlopen.return_value = FakeResponse(202)

        self.assertTrue(self.api.cancel_run(123))

    @unittest.mock.patch("urllib.request.urlopen")
    def test_cancel_run_tolerates_an_already_finished_run(self, urlopen):
        urlopen.side_effect = http_error(409)

        self.assertFalse(self.api.cancel_run(123))

    @unittest.mock.patch("urllib.request.urlopen")
    def test_listing_stops_on_a_short_page(self, urlopen):
        urlopen.side_effect = lambda *args, **kwargs: FakeResponse(
            200, b'{"workflow_runs": [{"id": 1}]}'
        )

        runs = self.api.list_unfinished_merge_group_runs()

        # One request per unfinished status, each returning a short page.
        self.assertEqual(urlopen.call_count, len(reaper.UNFINISHED_STATUSES))
        self.assertEqual(len(runs), len(reaper.UNFINISHED_STATUSES))

    @unittest.mock.patch("urllib.request.urlopen")
    def test_listing_warns_when_it_hits_the_page_cap(self, urlopen):
        full_page = json.dumps(
            {"workflow_runs": [{"id": i} for i in range(reaper.PAGE_SIZE)]}
        ).encode()
        urlopen.side_effect = lambda *args, **kwargs: FakeResponse(200, full_page)

        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            runs = self.api.list_unfinished_merge_group_runs()

        # Every page is full, so each status runs to the cap rather than
        # stopping short, and the truncation is reported once per status.
        per_status = reaper.MAX_PAGES * reaper.PAGE_SIZE
        self.assertEqual(len(runs), per_status * len(reaper.UNFINISHED_STATUSES))
        self.assertEqual(
            stderr.getvalue().count("WARNING"), len(reaper.UNFINISHED_STATUSES)
        )


if __name__ == "__main__":
    unittest.main()
