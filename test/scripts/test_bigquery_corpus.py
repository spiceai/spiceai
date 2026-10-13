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


def explain(physical, logical="Projection: test", schema_plan=None):
    if schema_plan is None:
        schema_plan = "\n".join(
            line + ", schema=[x:Utf8;N]" for line in physical.splitlines()
        )
    return json.dumps(
        [
            {"plan_type": "initial_physical_plan", "plan": physical},
            {"plan_type": "initial_physical_plan_with_schema", "plan": schema_plan},
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

    def test_rounding_cast_partial_pins_its_remote_count_and_its_local_cast(self):
        # Query 147 keeps its one scan and lifts the aggregate over it; the
        # local `CAST(… AS Int32)` is what the connector policy refused to push
        # down, so a plan without it is no longer this case.
        aggregate = (
            "AggregateExec: gby=[CAST(v0006@0 / 1000 AS Int32) * 10 as v0120]\n  "
            + FULL
        )
        corpus.check_plan(147, explain(aggregate))
        with self.assertRaises(HarnessError):
            corpus.check_plan(147, explain(FULL))
        # Query 033 splits into three scans, so neither one nor five will do.
        scan = "  SchemaCastScanExec\n    " + REMOTE
        three = "ProjectionExec: expr=[CAST(v0201@1 AS Int32) as v0316]\n" + "\n".join(
            [scan] * 3
        )
        corpus.check_plan(33, explain(three))
        for wrong in (5, 124):
            with self.assertRaises(HarnessError):
                corpus.check_plan(wrong, explain(three))

    def test_ordered_aggregate_partial_pins_its_remote_count_and_its_local_aggregate(
        self,
    ):
        # Query 226 keeps its one scan under a local `STRING_AGG(… ORDER BY …)`.
        # The BigQuery dialect does not render the ordering, so a plan that sent
        # the aggregate to BigQuery, or lost its ordering, is no longer this case.
        ordered = (
            "AggregateExec: mode=Single, gby=[], "
            "aggr=[string_agg(DISTINCT v1169.rule, Utf8(\", \")) "
            "ORDER BY [v1169.rule ASC NULLS LAST]]\n  " + FULL
        )
        corpus.check_plan(226, explain(ordered))
        unordered = ordered.replace(" ORDER BY [v1169.rule ASC NULLS LAST]", "")
        for plan in (FULL, unordered):
            with self.assertRaises(HarnessError):
                corpus.check_plan(226, explain(plan))
        # Query 085 splits into three scans, so neither one nor six will do.
        scan = "  SchemaCastScanExec\n    " + REMOTE
        three = (
            "AggregateExec: mode=Single, gby=[], "
            "aggr=[array_agg(v0177) ORDER BY [v0479 ASC NULLS LAST]]\n"
            + "\n".join([scan] * 3)
        )
        corpus.check_plan(85, explain(three))
        for wrong in (86, 226):
            with self.assertRaises(HarnessError):
                corpus.check_plan(wrong, explain(three))

    def test_expected_jobs_exclude_subtrees_an_empty_build_side_skips(self):
        # 278 planned remote subtrees, 14 of them behind an empty join build
        # side (#14848).
        self.assertEqual(corpus.expected_execution_jobs(corpus.corpus()), 264)
        partial = corpus.ROUNDING_CAST_PARTIAL | corpus.ORDERED_AGGREGATE_PARTIAL
        for index, skipped in corpus.EMPTY_BUILD_SKIPPED.items():
            self.assertLess(skipped, partial[index])

    def test_transport_cannot_change_field_type_name_or_nullability(self):
        for name in corpus.TRANSPORT:
            physical = name + "\n  " + REMOTE
            for schema in ("[x:Int64;N]", "[renamed:Utf8;N]", "[x:Utf8]"):
                with self.subTest(name=name, schema=schema):
                    schema_plan = (
                        f"{name}, schema={schema}\n  {REMOTE}, schema=[x:Utf8;N]"
                    )
                    with self.assertRaisesRegex(HarnessError, "child schema"):
                        corpus.check_plan(5, explain(physical, schema_plan=schema_plan))

    def test_schema_evidence_must_match_the_initial_plan(self):
        plans = json.loads(explain(FULL))
        del plans[1]
        with self.assertRaisesRegex(HarnessError, "missing initial physical plan"):
            corpus.check_plan(5, json.dumps(plans))
        plans.insert(1, {"plan_type": "physical_plan_with_schema", "plan": FULL})
        with self.assertRaisesRegex(HarnessError, "missing initial physical plan"):
            corpus.check_plan(5, json.dumps(plans))
        valid = json.loads(explain(FULL))[1]["plan"]
        for invalid in (
            "",
            FULL,
            valid.replace("SELECT 1", "SELECT 2"),
            valid.replace("  " + REMOTE, "    " + REMOTE),
            valid.replace("  " + REMOTE + ", schema=[x:Utf8;N]", "  " + REMOTE),
        ):
            with self.subTest(invalid=invalid), self.assertRaises(HarnessError):
                corpus.check_plan(5, explain(FULL, schema_plan=invalid))

    def test_nested_schema_and_schema_text_in_sql(self):
        physical = FULL.replace("SELECT 1", "SELECT ', schema=[text]' AS x")
        schema = "[x:Struct(a:List(Utf8);N, b:Int64);N]"
        schema_plan = "\n".join(
            line + ", schema=" + schema for line in physical.splitlines()
        )
        corpus.check_plan(5, explain(physical, schema_plan=schema_plan))
        with self.assertRaisesRegex(HarnessError, "child schema"):
            corpus.check_plan(
                5,
                explain(
                    physical,
                    schema_plan=schema_plan.replace("List(Utf8)", "List(Int64)", 1),
                ),
            )

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
