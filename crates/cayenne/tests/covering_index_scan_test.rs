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

//! Public SQL evidence for a covered literal scan.

#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use common::covering_index::{
    CoveringIndexFixture, FixtureMode, PathSelector, TypedRow, TypedValue,
    assert_no_vortex_data_access, assert_typed_bag,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn covering_scan_serves_file_point_query_without_vortex_data() {
    let fixture = CoveringIndexFixture::new(FixtureMode::File).await;
    let evidence = fixture
        .execute(
            PathSelector::Indexed,
            "SELECT one, foreign_id FROM a WHERE some_value = 'blahblah'",
        )
        .await;
    evidence.print("covering_scan_serves_file_point_query_without_vortex_data");

    assert_typed_bag(
        &evidence,
        &[
            row([text("a1"), int64(10)]),
            row([text("a2"), int64(10)]),
            row([text("a3"), null_int64()]),
            row([text("a5"), int64(99)]),
        ],
    );
    assert!(
        evidence.physical_plan.contains("CayenneIndexScanExec"),
        "expected covering literal scan:\n{}",
        evidence.physical_plan
    );
    assert_no_vortex_data_access(&evidence);
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

fn text(value: &str) -> TypedValue {
    TypedValue::Text(Some(value.to_string()))
}
