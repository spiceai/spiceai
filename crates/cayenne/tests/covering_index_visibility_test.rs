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

//! Engine evidence for visibility across a re-keying source replacement.
//!
//! The covering executor is deliberately not selected yet. This test exercises
//! the actual file-backed publication path and records returned rows while the
//! optional path remains unavailable; the library visibility tests establish
//! the source-role rules that its future executor must apply.

#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use std::sync::Arc;

use arrow::array::{Int32Array, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use common::covering_index::{
    CoveringIndexFixture, FixtureMode, PathSelector, TypedRow, TypedValue, a_rows, assert_typed_bag,
};

/// A re-keyed replacement has no stale old-key row and exposes the replacement
/// key. This uses the real refresh publication boundary rather than inspecting
/// a private catalog.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn full_refresh_rekey_removes_old_key_rows_and_exposes_replacement_rows() {
    let fixture = CoveringIndexFixture::new(FixtureMode::File).await;
    let old_key_sql = "SELECT a_id, one FROM a WHERE some_value = 'blahblah'";
    let before = fixture.execute(PathSelector::Indexed, old_key_sql).await;

    fixture.overwrite_indexed_a(rekeyed_a_rows()).await;
    fixture.overwrite_indexed_b(payload_updated_b_rows()).await;

    let old_key_after = fixture.execute(PathSelector::Indexed, old_key_sql).await;
    let replacement_key = fixture
        .execute(
            PathSelector::Indexed,
            "SELECT a_id, one FROM a WHERE some_value = 'other'",
        )
        .await;
    let payload_update = fixture
        .execute(
            PathSelector::Indexed,
            "SELECT b_row_id, two FROM b WHERE id = 10",
        )
        .await;
    before.print("covering-visibility rekey before");
    old_key_after.print("covering-visibility rekey old key after");
    replacement_key.print("covering-visibility rekey replacement key");
    payload_update.print("covering-visibility payload update");

    assert_typed_bag(
        &old_key_after,
        &[
            row([int64(2), text("a2")]),
            row([int64(3), text("a3")]),
            row([int64(5), text("a5")]),
        ],
    );
    assert_typed_bag(
        &replacement_key,
        &[
            row([int64(1), text("a1-rekeyed")]),
            row([int64(4), text("a4")]),
        ],
    );
    assert_typed_bag(
        &payload_update,
        &[
            row([int64(101), text("b1-updated")]),
            row([int64(102), text("b2")]),
        ],
    );
}

fn payload_updated_b_rows() -> RecordBatch {
    let source = common::covering_index::b_rows();
    RecordBatch::try_new(
        source.schema(),
        vec![
            Arc::new(Int64Array::from(vec![101, 102, 103, 104])),
            Arc::new(Int64Array::from(vec![Some(10), Some(10), Some(20), None])),
            Arc::new(StringArray::from(vec!["b1-updated", "b2", "b3", "b4"])),
            Arc::new(Int32Array::from(vec![1, 0, 1, 1])),
        ],
    )
    .expect("payload-updated b fixture batch")
}

fn rekeyed_a_rows() -> RecordBatch {
    let source = a_rows();
    RecordBatch::try_new(
        source.schema(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec![
                "other", "blahblah", "blahblah", "other", "blahblah",
            ])),
            Arc::new(Int64Array::from(vec![
                Some(10),
                Some(10),
                None,
                Some(20),
                Some(99),
            ])),
            Arc::new(StringArray::from(vec![
                "a1-rekeyed",
                "a2",
                "a3",
                "a4",
                "a5",
            ])),
        ],
    )
    .expect("re-keyed a fixture batch")
}

fn row(values: impl IntoIterator<Item = TypedValue>) -> TypedRow {
    values.into_iter().collect()
}

fn int64(value: i64) -> TypedValue {
    TypedValue::Int64(Some(value))
}

fn text(value: &str) -> TypedValue {
    TypedValue::Text(Some(value.to_string()))
}
