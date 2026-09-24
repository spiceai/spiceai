/*
Copyright 2026 The Spice.ai OSS Authors

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

//! Public SQL controls for the inactive covering index join.
//!
//! Step 08 turns the indexed selector into an automatic index-join path. Until
//! then this preserves the expected SQL bag and demonstrates that the ordinary
//! hash-join fallback remains the only public execution route.

#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use common::covering_index::{
    CoveringIndexFixture, FixtureMode, PathSelector, TypedRow, TypedValue, assert_typed_bag,
};

const INNER_SQL: &str = "
    SELECT a.one, b.two
    FROM a JOIN b ON a.foreign_id = b.id
    WHERE a.some_value = 'blahblah'
";

const LEFT_ON_SQL: &str = "
    SELECT a.one, b.two
    FROM a LEFT JOIN b ON a.foreign_id = b.id AND b.active = 1
    WHERE a.some_value = 'blahblah'
";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inactive_index_join_preserves_public_inner_and_left_on_bags() {
    let fixture = CoveringIndexFixture::new(FixtureMode::Memory).await;
    for (sql, expected) in [
        (
            INNER_SQL,
            vec![
                row([text("a1"), text("b1")]),
                row([text("a1"), text("b2")]),
                row([text("a2"), text("b1")]),
                row([text("a2"), text("b2")]),
            ],
        ),
        (
            LEFT_ON_SQL,
            vec![
                row([text("a1"), text("b1")]),
                row([text("a2"), text("b1")]),
                row([text("a3"), null_text()]),
                row([text("a5"), null_text()]),
            ],
        ),
    ] {
        let evidence = fixture.execute(PathSelector::Indexed, sql).await;
        evidence.print("inactive_index_join_preserves_public_inner_and_left_on_bags");
        assert_typed_bag(&evidence, &expected);
        assert!(
            !evidence.physical_plan.contains("CayenneIndexJoinExec"),
            "the optimizer is introduced in step 08, not direct-execution step 07:\n{}",
            evidence.physical_plan
        );
    }
}

fn row(values: impl IntoIterator<Item = TypedValue>) -> TypedRow {
    values.into_iter().collect()
}

fn null_text() -> TypedValue {
    TypedValue::Text(None)
}

fn text(value: &str) -> TypedValue {
    TypedValue::Text(Some(value.to_string()))
}
