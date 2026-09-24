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

//! Public-provider budget evidence for resident covering indexes.
//!
//! This deliberately drives the real SQL/provider path. Private page-store
//! fault and cancellation mechanics live in the library contract suite, while
//! this test proves a bounded `DataFusion` pool can contain resident catalog
//! charges and still preserve query results.

#![allow(clippy::expect_used)]

mod common;

use common::covering_index::{
    CoveringIndexFixture, FixtureMode, PathSelector, assert_same_schema_and_bag,
};

const JOIN_SQL: &str = "
    SELECT a.one, b.two
    FROM a JOIN b ON b.id = a.foreign_id
    WHERE a.some_value = 'blahblah'
";

/// The fixed 64 MiB pool admits the fixture's two resident covering catalogs
/// with allocator-independent headroom. The emitted plan/rows are retained in
/// the test output as the engine-level artifact; exact process RSS is not a
/// pool-accounting assertion.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bounded_pool_preserves_covering_query_results_and_reports_reservation() {
    let fixture =
        CoveringIndexFixture::new_with_memory_limit(FixtureMode::File, 64 * 1024 * 1024).await;
    let after_build = fixture
        .pool_reserved_bytes()
        .expect("bounded fixture exposes its shared pool");
    assert!(
        after_build > 0,
        "the real provider must charge resident table/index state to its bounded pool"
    );

    let indexed = fixture.execute(PathSelector::Indexed, JOIN_SQL).await;
    let baseline = fixture.execute(PathSelector::Baseline, JOIN_SQL).await;
    indexed.print("covering-index bounded-pool indexed query");
    baseline.print("covering-index bounded-pool ordinary control");
    assert_same_schema_and_bag(&indexed, &baseline);

    let after_query = fixture
        .pool_reserved_bytes()
        .expect("bounded fixture retains its shared pool");
    println!(
        "covering-index pool reservation: after_build={after_build} after_query={after_query}"
    );
    assert!(
        after_query >= after_build,
        "a live query result may retain Arrow views, but must never make resident catalog charges disappear"
    );
}
