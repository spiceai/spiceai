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

//! Same-rig comparison of the ordinary and covering-index join paths.
//!
//! The fixture is deliberately the public two-table Cayenne provider path used
//! by the integration tests. Setup and index publication happen before
//! Criterion starts timing; every iteration includes SQL planning and execution
//! but excludes `EXPLAIN ANALYZE` and fixture construction.

#![expect(
    clippy::expect_used,
    reason = "the benchmark fixture is shared with integration tests and setup errors must stop measurement"
)]

#[path = "../tests/common/mod.rs"]
mod common;

use std::{hint::black_box, time::Duration};

use common::covering_index::{
    CoveringIndexFixture, FixtureMode, PathSelector, assert_same_schema_and_bag,
};
use criterion::{Criterion, criterion_group, criterion_main};

const SQL: &str = "
    SELECT a.one, b.two
    FROM a JOIN b ON b.id = a.foreign_id
    WHERE a.some_value = 'blahblah'
";

fn bench_covering_index_join(criterion: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().expect("create benchmark runtime");
    let fixture = runtime.block_on(CoveringIndexFixture::new(FixtureMode::File));

    // A wrong fast result is not a benchmark result. Validate complete typed
    // bags outside the timed loop, then retain the row count as a low-cost
    // iteration guard against an accidental empty execution path.
    let indexed = runtime.block_on(fixture.execute(PathSelector::Indexed, SQL));
    let baseline = runtime.block_on(fixture.execute(PathSelector::Baseline, SQL));
    assert_same_schema_and_bag(&indexed, &baseline);
    let indexed_rows = indexed.rows.len();

    let mut group = criterion.benchmark_group("covering_index_join");
    for (name, selector) in [
        ("ordinary", PathSelector::Baseline),
        ("covering", PathSelector::Indexed),
    ] {
        group.bench_function(name, |bencher| {
            bencher.to_async(&runtime).iter(|| async {
                let rows = fixture
                    .collect_query(selector, SQL)
                    .await
                    .iter()
                    .map(arrow::record_batch::RecordBatch::num_rows)
                    .sum::<usize>();
                assert_eq!(
                    rows, indexed_rows,
                    "benchmark query returned wrong row count"
                );
                black_box(rows);
            });
        });
    }
    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(10))
        .measurement_time(Duration::from_secs(60))
        .sample_size(10);
    targets = bench_covering_index_join
}
criterion_main!(benches);
