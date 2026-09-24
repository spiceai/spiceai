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

//! Engine evidence for file-backed covering-artifact publication.
//!
//! Covering scans are intentionally not active until the later visibility step,
//! but this test drives the real full-refresh writer that captures and publishes
//! an artifact. It proves ordinary SQL rows remain stable across that optional
//! work; private catalog and stale-builder assertions live beside the publisher.

#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use common::covering_index::{
    CoveringIndexFixture, FixtureMode, PathSelector, TypedRow, TypedValue, a_rows,
    assert_same_schema_and_bag, assert_typed_bag,
};

/// A full refresh invokes the finalized Vortex write observer. Its optional
/// covering build must publish only alongside the replacement snapshot and can
/// never change the ordinary query result.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn full_refresh_preserves_normal_query_rows_while_publishing_covering_artifacts() {
    let fixture = CoveringIndexFixture::new(FixtureMode::File).await;
    let sql = "SELECT a_id, one FROM a WHERE some_value = 'blahblah'";
    let before = fixture.execute(PathSelector::Indexed, sql).await;

    fixture.overwrite_indexed_a(a_rows()).await;

    let after = fixture.execute(PathSelector::Indexed, sql).await;
    before.print("covering-publication full-refresh before");
    after.print("covering-publication full-refresh after");
    assert_same_schema_and_bag(&after, &before);
    assert_typed_bag(
        &after,
        &[
            row([int64(1), text("a1")]),
            row([int64(2), text("a2")]),
            row([int64(3), text("a3")]),
            row([int64(5), text("a5")]),
        ],
    );
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
