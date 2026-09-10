#!/usr/bin/env python3
# Copyright 2024-2026 The Spice.ai OSS Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Offline checks for the BigQuery release harness's fixture and failure handling."""

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import bigquery_corpus as corpus
from bigquery_federation import HarnessError


def explain(physical, logical="Projection: test"):
    return json.dumps(
        [
            {"plan_type": "initial_physical_plan", "plan": physical},
            {"plan_type": "logical_plan", "plan": logical},
        ]
    )


REMOTE = "VirtualExecutionPlan name=adbc compute_context=test base_sql=SELECT 1"
FULL = "SchemaCastScanExec\n  " + REMOTE
PARTIAL = (
    "ProjectionExec: expr=[p50]\n  WindowAggExec: median(x), approx_percentile_cont(x)\n    SchemaCastScanExec\n      "
    + REMOTE
)


class CorpusTests(unittest.TestCase):
    def test_all_cases_and_source_types(self):
        self.assertEqual([index for index, _ in corpus.corpus()], list(range(271)))
        aliases, tables = corpus.fixtures()
        self.assertEqual(len(aliases), 72)
        self.assertEqual(len(tables), 70)
        self.assertEqual(len({dataset for dataset, _ in tables}), 17)
        fields = [field for schema in tables.values() for field in schema]
        self.assertTrue(
            any(
                field.field_type == "JSON" and field.mode == "REPEATED"
                for field in fields
            )
        )
        self.assertTrue(
            any(
                field.field_type == "JSON" and field.mode == "NULLABLE"
                for field in fields
            )
        )
        self.assertTrue(any(field.field_type == "STRING" for field in fields))
        self.assertTrue(any(field.field_type == "DATETIME" for field in fields))
        self.assertTrue(any(field.field_type == "TIMESTAMP" for field in fields))
        nested = [field for field in fields if field.field_type == "RECORD"]
        self.assertEqual(len(nested), 1)
        self.assertEqual(len(nested[0].fields), 3)
        datasets = {dataset: "test_" + dataset for dataset, _ in tables}
        pod = json.loads(
            corpus.spicepod(aliases, "test-project", datasets, Path("/a driver.so"))
        )
        self.assertEqual(len({entry["from"] for entry in pod["datasets"]}), 70)
        self.assertFalse(pod["runtime"]["caching"]["sql_results"]["enabled"])

    def test_full_and_explicit_partial(self):
        corpus.check_plan(5, explain(FULL))
        corpus.check_plan(241, explain(PARTIAL))
        corpus.check_plan(0, explain("DataSourceExec: values"))

    def test_one_remote_does_not_hide_local_work(self):
        for name in (
            "ProjectionExec",
            "FilterExec",
            "AggregateExec",
            "SortExec",
            "GlobalLimitExec",
        ):
            with self.subTest(name=name), self.assertRaises(HarnessError):
                corpus.check_plan(5, explain(f"{name}\n  " + REMOTE))
        with self.assertRaises(HarnessError):
            corpus.check_plan(
                5, explain("CoalescePartitionsExec: fetch=1\n  " + REMOTE)
            )

    def test_partial_does_not_allow_join_or_lower_projection(self):
        for plan in (
            FULL,
            PARTIAL.replace("SchemaCastScanExec", "ProjectionExec"),
            PARTIAL.replace("SchemaCastScanExec", "HashJoinExec"),
            PARTIAL.replace("approx_percentile_cont(x)", "row_number()"),
        ):
            with self.subTest(plan=plan), self.assertRaises(HarnessError):
                corpus.check_plan(241, explain(plan))

    def test_missing_extra_and_branched_remote_fail(self):
        for plan in (
            "EmptyExec",
            FULL + "\n  " + REMOTE,
            FULL + "\n  CoalescePartitionsExec",
        ):
            with self.subTest(plan=plan), self.assertRaises(HarnessError):
                corpus.check_plan(5, explain(plan))
        with self.assertRaises(HarnessError):
            corpus.check_plan(0, explain(FULL))
        with self.assertRaises(HarnessError):
            corpus.check_plan(0, explain("EmptyExec", "TableScan: forbidden"))

    def test_executes_even_after_a_plan_failure(self):
        with (
            tempfile.TemporaryDirectory() as directory,
            patch.object(
                corpus.harness,
                "http_sql",
                side_effect=[
                    (200, {}, explain("FilterExec\n  " + REMOTE)),
                    (200, {}, "[]"),
                ],
            ) as request,
        ):
            record = corpus.execute_case(5, "SELECT x", 1234, Path(directory))
        self.assertEqual(request.call_count, 2)
        self.assertEqual(record["rows"], 0)
        self.assertEqual(len(record["errors"]), 1)

    def test_remote_errors_and_invalid_results_fail(self):
        for response in (
            (400, {}, "remote query error"),
            (200, {}, '{"error":"bad result"}'),
            (200, {}, "truncated JSON"),
        ):
            with (
                self.subTest(response=response),
                tempfile.TemporaryDirectory() as directory,
                patch.object(
                    corpus.harness,
                    "http_sql",
                    side_effect=[(200, {}, explain(FULL)), response],
                ),
            ):
                self.assertTrue(
                    corpus.execute_case(5, "SELECT x", 1234, Path(directory))["errors"]
                )

    def test_nonempty_oracle_rejects_empty_or_wrong_rows(self):
        expected = [{"count": 2, "ratio": 0.5}]
        for rows in ([], [{"count": 2, "ratio": 0.0}]):
            with (
                self.subTest(rows=rows),
                tempfile.TemporaryDirectory() as directory,
                patch.object(
                    corpus.harness,
                    "http_sql",
                    side_effect=[(200, {}, explain(FULL)), (200, {}, json.dumps(rows))],
                ),
            ):
                record = corpus.execute_case(
                    168, "SELECT x", 1234, Path(directory), expected
                )
                self.assertEqual(len(record["errors"]), 1)
                self.assertIn("nonempty fixture oracle", record["errors"][0])


if __name__ == "__main__":
    unittest.main()
