/*
Copyright 2024-2026 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

    10|Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

//! Two-executor cluster regression for checked integer `SUM`, run as a
//! Ballista job.
//!
//! `FlightSQLPartialAggregatePushdown` used to send `SUM(col)` to each executor,
//! where production `CheckedSum` narrowed to `Int64` before the scheduler
//! merged. The accelerated path (each executor serving its own partitions over
//! Flight SQL) is covered by `test/spicescript/cluster-checked-sum.yaml`, against
//! separate `spiced` processes; this file covers the job path, where the
//! scheduler serializes a plan that executors decode and run.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use app::AppBuilder;
use arrow::array::{AsArray, Int32Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Int64Type, Schema};
use datafusion::parquet::arrow::ArrowWriter;
use spicepod::component::dataset::Dataset;

use crate::{configure_test_datafusion, init_tracing, utils::test_request_context};

use super::harness::ClusterHarness;

const TABLE: &str = "t";

/// Rows of `part` and an `INT` column `w`.
fn int_parts(parts: &[(i64, Vec<Option<i32>>)]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("part", DataType::Int64, false),
        Field::new("w", DataType::Int32, true),
    ]));
    let mut part_col = Vec::new();
    let mut value_col = Vec::new();
    for (part, values) in parts {
        for value in values {
            part_col.push(*part);
            value_col.push(*value);
        }
    }
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(part_col)),
            Arc::new(Int32Array::from(value_col)),
        ],
    )
    .expect("partitioned int batch")
}

fn write_parquet(path: &std::path::Path, batch: &RecordBatch) {
    let file = std::fs::File::create(path).expect("create parquet");
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).expect("parquet writer");
    writer.write(batch).expect("write parquet batch");
    writer.close().expect("close parquet writer");
}

fn format_batches(rows: &[RecordBatch]) -> String {
    arrow::util::pretty::pretty_format_batches(rows)
        .expect("format batches")
        .to_string()
}

const PLUS_SUMS_SQL: &str = "SELECT SUM(w + 1), SUM(w + 2), SUM(w + 3) FROM t";

/// `PLUS_SUMS_SQL` over [`plus_literal_rows`]: the three non-NULL values of
/// `w` add to 2147483650.
const PLUS_SUMS: [i64; 3] = [2_147_483_653, 2_147_483_656, 2_147_483_659];

/// `w` split `[i32::MAX, 1]` and `[2, NULL]` across partitions 0 and 1.
fn plus_literal_rows() -> RecordBatch {
    int_parts(&[(0, vec![Some(i32::MAX), Some(1)]), (1, vec![Some(2), None])])
}

/// The `Int64` values of a one-row result.
fn i64_row(rows: &[RecordBatch]) -> Vec<i64> {
    let batch = rows
        .iter()
        .find(|batch| batch.num_rows() > 0)
        .expect("one result row");
    assert_eq!(batch.num_rows(), 1, "expected one row, got {batch:?}");
    batch
        .columns()
        .iter()
        .map(|column| column.as_primitive::<Int64Type>().value(0))
        .collect()
}

/// The same sums as a Ballista job over a file dataset: the scheduler
/// serializes a plan that calls both new functions, and two executors decode
/// and run its tasks.
#[tokio::test(flavor = "multi_thread")]
async fn cluster_job_sums_of_a_column_plus_literals_share_one_sum() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=info,warn"));

    test_request_context()
        .scope(async {
            configure_test_datafusion();
            let data_dir = tempfile::tempdir().expect("data tempdir");
            let parquet_path = data_dir.path().join("t.parquet");
            write_parquet(&parquet_path, &plus_literal_rows());
            let dataset = || {
                let mut dataset = Dataset::new(format!("file:{}", parquet_path.display()), TABLE);
                dataset.params = Some(spicepod::param::Params::from_string_map(HashMap::from([(
                    "file_format".to_string(),
                    "parquet".to_string(),
                )])));
                dataset
            };
            let harness = ClusterHarness::builder()
                .scheduler(
                    AppBuilder::new("cluster_plus_literal_sums")
                        .with_dataset(dataset())
                        .build(),
                )
                .executor_with_app(
                    AppBuilder::new("executor_plus_literal_sums_0")
                        .with_dataset(dataset())
                        .build(),
                )
                .executor_with_app(
                    AppBuilder::new("executor_plus_literal_sums_1")
                        .with_dataset(dataset())
                        .build(),
                )
                .start()
                .await?;

            let result = async {
                harness.wait_for_executors(Duration::from_secs(30)).await?;
                let plan = format_batches(&harness.explain(PLUS_SUMS_SQL).await?);
                anyhow::ensure!(
                    plan.contains("__spice_checked_sum_plus_count"),
                    "SUM(w + k) must plan one wide SUM and one COUNT of w, got:\n{plan}"
                );
                let rows = harness
                    .query_distributed(PLUS_SUMS_SQL, "plus_literal_sums")
                    .await?;
                assert_eq!(i64_row(&rows), PLUS_SUMS, "plan:\n{plan}");
                Ok::<(), anyhow::Error>(())
            }
            .await;

            harness.shutdown().await;
            result
        })
        .await
}
