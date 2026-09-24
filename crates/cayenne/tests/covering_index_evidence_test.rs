/*
Copyright 2024-2026 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

//! Baseline path evidence for the planned Cayenne covering-index implementation.
//!
//! These tests deliberately characterize today's public SQL path. They do not
//! assert the future zero-Vortex/index-join target; the common acceptance
//! helpers are enabled by the later implementation stages.

#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use common::covering_index::{
    CoveringIndexFixture, FixtureMode, PathSelector, TypedRow, TypedValue,
    assert_same_schema_and_bag, assert_typed_bag,
};

const README_INNER: &str = "
    SELECT a.one, b.two
    FROM a JOIN b ON b.id = a.foreign_id
    WHERE a.some_value = 'blahblah'
";

const README_LEFT: &str = "
    SELECT a.one, b.two
    FROM a LEFT JOIN b ON b.id = a.foreign_id
    WHERE a.some_value = 'blahblah'
";

const README_LEFT_ON_ACTIVE: &str = "
    SELECT a.one, b.two
    FROM a LEFT JOIN b ON b.id = a.foreign_id AND b.active = 1
    WHERE a.some_value = 'blahblah'
";

const README_LEFT_WHERE_ACTIVE: &str = "
    SELECT a.one, b.two
    FROM a LEFT JOIN b ON b.id = a.foreign_id
    WHERE a.some_value = 'blahblah' AND b.active = 1
";

/// The fixed Arrow rows, duplicate keys, NULLs, and left-join residuals stay
/// equal between the indexed and ordinary public provider paths.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fixture_rows_and_join_null_semantics_match_controls() {
    for mode in [FixtureMode::File, FixtureMode::Memory] {
        let fixture = CoveringIndexFixture::new(mode).await;

        assert_query_matches(
            &fixture,
            "SELECT a_id, some_value, foreign_id, one FROM a",
            vec![
                row([int64(1), text("blahblah"), int64(10), text("a1")]),
                row([int64(2), text("blahblah"), int64(10), text("a2")]),
                row([int64(3), text("blahblah"), null_int64(), text("a3")]),
                row([int64(4), text("other"), int64(20), text("a4")]),
                row([int64(5), text("blahblah"), int64(99), text("a5")]),
            ],
        )
        .await;
        assert_query_matches(
            &fixture,
            "SELECT b_row_id, id, two, active FROM b",
            vec![
                row([int64(101), int64(10), text("b1"), int32(1)]),
                row([int64(102), int64(10), text("b2"), int32(0)]),
                row([int64(103), int64(20), text("b3"), int32(1)]),
                row([int64(104), null_int64(), text("b4"), int32(1)]),
            ],
        )
        .await;
        assert_query_matches(
            &fixture,
            README_INNER,
            vec![
                row([text("a1"), text("b1")]),
                row([text("a1"), text("b2")]),
                row([text("a2"), text("b1")]),
                row([text("a2"), text("b2")]),
            ],
        )
        .await;
        assert_query_matches(
            &fixture,
            README_LEFT,
            vec![
                row([text("a1"), text("b1")]),
                row([text("a1"), text("b2")]),
                row([text("a2"), text("b1")]),
                row([text("a2"), text("b2")]),
                row([text("a3"), null_text()]),
                row([text("a5"), null_text()]),
            ],
        )
        .await;
        assert_query_matches(
            &fixture,
            README_LEFT_ON_ACTIVE,
            vec![
                row([text("a1"), text("b1")]),
                row([text("a2"), text("b1")]),
                row([text("a3"), null_text()]),
                row([text("a5"), null_text()]),
            ],
        )
        .await;
        assert_query_matches(
            &fixture,
            README_LEFT_WHERE_ACTIVE,
            vec![row([text("a1"), text("b1")]), row([text("a2"), text("b1")])],
        )
        .await;
    }
}

/// The unindexed selector remains a runnable Vortex baseline after a later
/// covering implementation changes the indexed path. This test also records
/// today's indexed point lookup without claiming it is the future target.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn baseline_point_path() {
    let fixture = CoveringIndexFixture::new(FixtureMode::File).await;
    let sql = "SELECT a_id, one FROM a WHERE some_value = 'blahblah'";

    let baseline = fixture.execute(PathSelector::Baseline, sql).await;
    let indexed = fixture.execute(PathSelector::Indexed, sql).await;
    baseline.print("baseline_point_path ordinary control");
    indexed.print("baseline_point_path indexed current path");

    assert_same_schema_and_bag(&indexed, &baseline);
    assert_typed_bag(
        &baseline,
        &[
            row([int64(1), text("a1")]),
            row([int64(2), text("a2")]),
            row([int64(3), text("a3")]),
            row([int64(5), text("a5")]),
        ],
    );
    assert!(
        baseline.reads.execution.has_vortex_data_access() && baseline.vortex_metrics.reads > 0,
        "the ordinary point control did not observe Vortex data/reader activity: {:?}, {:?}",
        baseline.reads.execution,
        baseline.vortex_metrics,
    );
}

/// The same selector makes the ordinary two-Cayenne-table join a durable
/// baseline while reporting the current indexed join plan and counter deltas.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn baseline_join_path() {
    let fixture = CoveringIndexFixture::new(FixtureMode::File).await;

    let baseline = fixture.execute(PathSelector::Baseline, README_INNER).await;
    let indexed = fixture.execute(PathSelector::Indexed, README_INNER).await;
    baseline.print("baseline_join_path ordinary control");
    indexed.print("baseline_join_path indexed current path");

    assert_same_schema_and_bag(&indexed, &baseline);
    assert_typed_bag(
        &baseline,
        &[
            row([text("a1"), text("b1")]),
            row([text("a1"), text("b2")]),
            row([text("a2"), text("b1")]),
            row([text("a2"), text("b2")]),
        ],
    );
    assert!(
        baseline.explain_analyze.contains("HashJoinExec"),
        "ordinary baseline did not expose its hash join:\n{}",
        baseline.explain_analyze
    );
    assert!(
        baseline.reads.execution.has_vortex_data_access() && baseline.vortex_metrics.reads > 0,
        "the ordinary join control did not observe Vortex data/reader activity: {:?}, {:?}",
        baseline.reads.execution,
        baseline.vortex_metrics,
    );
}

/// A control must prove the object-store spy and the Vortex reader metrics can
/// see an ordinary unindexed file scan before future zero-read assertions use
/// them as evidence.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ordinary_unindexed_file_query_records_vortex_reads() {
    let fixture = CoveringIndexFixture::new(FixtureMode::File).await;
    let evidence = fixture
        .execute(
            PathSelector::Baseline,
            "SELECT one FROM a WHERE some_value = 'blahblah'",
        )
        .await;
    evidence.print("ordinary_unindexed_file_query_records_vortex_reads");

    assert!(
        evidence.reads.execution.has_vortex_data_access(),
        "object-store data counters did not observe the ordinary file query: {:?}",
        evidence.reads.execution
    );
    assert!(
        evidence.vortex_metrics.reads > 0 && evidence.vortex_metrics.bytes > 0,
        "Vortex reader metrics did not observe the ordinary file query: {:?}",
        evidence.vortex_metrics
    );
}

async fn assert_query_matches(fixture: &CoveringIndexFixture, sql: &str, expected: Vec<TypedRow>) {
    let indexed = fixture.execute(PathSelector::Indexed, sql).await;
    let control = fixture.execute(PathSelector::Baseline, sql).await;
    assert_same_schema_and_bag(&indexed, &control);
    assert_typed_bag(&indexed, &expected);
}

fn row(values: impl IntoIterator<Item = TypedValue>) -> TypedRow {
    values.into_iter().collect()
}

fn int64(value: i64) -> TypedValue {
    TypedValue::Int64(Some(value))
}

fn null_int64() -> TypedValue {
    TypedValue::Int64(None)
}

fn int32(value: i32) -> TypedValue {
    TypedValue::Int32(Some(value))
}

fn text(value: &str) -> TypedValue {
    TypedValue::Text(Some(value.to_string()))
}

fn null_text() -> TypedValue {
    TypedValue::Text(None)
}
