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

#![expect(
    clippy::expect_used,
    reason = "Test fixtures use descriptive failure messages"
)]

use crate::common;
use arrow::array::{ArrayRef, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use cayenne::metadata::{CreateTableOptions, VortexConfig};
use cayenne::{CayenneTableProvider, MetadataCatalog};
use datafusion::datasource::TableProvider;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use datafusion_common::{ScalarValue, stats::Precision};
use datafusion_datasource::{
    PartitionedFile, file_groups::FileGroup, file_scan_config::FileScanConfig,
    source::DataSourceExec,
};
use std::sync::Arc;

fn scan_files(plan: &Arc<dyn ExecutionPlan>, files: &mut Vec<PartitionedFile>) {
    if let Some(exec) = plan.downcast_ref::<DataSourceExec>()
        && let Some(scan) = exec.data_source().downcast_ref::<FileScanConfig>()
    {
        files.extend(scan.file_groups.iter().flat_map(FileGroup::iter).cloned());
    }
    for child in plan.children() {
        scan_files(child, files);
    }
}

#[tokio::test]
async fn composite_file_pk_bounds_are_exact() {
    check_composite_file_pk_bounds("").await;
}

#[tokio::test]
async fn composite_file_long_utf8_pk_bounds_are_inexact() {
    check_composite_file_pk_bounds(&"a".repeat(128)).await;
}

async fn write_composite_files(
    prefix: &str,
) -> (common::TestFixture, CayenneTableProvider, SessionContext) {
    let fixture = common::TestFixture::new(common::BackendType::Sqlite)
        .await
        .expect("fixture");
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::Int32, false),
        Field::new("c", DataType::Int32, false),
        Field::new("d", DataType::Int32, false),
        Field::new("s", DataType::Utf8, false),
        Field::new("n", DataType::Int64, false),
    ]));
    let ctx = SessionContext::new();
    let table = CayenneTableProvider::create_table(
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>,
        CreateTableOptions {
            table_name: "composite_file_stats".into(),
            schema: Arc::clone(&schema),
            primary_key: schema.fields().iter().map(|f| f.name().clone()).collect(),
            on_conflict: None,
            base_path: fixture.data_path.to_string_lossy().into_owned(),
            partition_column: None,
            vortex_config: VortexConfig {
                inline_max_rows: 0,
                inline_max_bytes: 0,
                inline_max_buffer_bytes: 0,
                write_concurrency: Some(1),
                compaction_background_interval_ms: 0,
                compaction_trigger_files: usize::MAX,
                compaction_trigger_snapshot_age_ms: 0,
                ..VortexConfig::default()
            },
        },
        ctx.runtime_env(),
    )
    .await
    .expect("create composite PK table");
    for file in 0..3_i32 {
        let values: Vec<_> = (0..64).map(|i| file * 100 + i).collect();
        let mut columns: Vec<ArrayRef> = (0..4)
            .map(|_| Arc::new(Int32Array::from(values.clone())) as ArrayRef)
            .collect();
        columns.push(Arc::new(StringArray::from(
            values
                .iter()
                .map(|i| format!("{prefix}cust-{i:04}"))
                .collect::<Vec<_>>(),
        )));
        columns.push(Arc::new(Int64Array::from(
            values.iter().map(|&i| i64::from(i)).collect::<Vec<_>>(),
        )));
        let batch = RecordBatch::try_new(Arc::clone(&schema), columns).expect("batch");
        assert_eq!(
            common::insert_batch(&table, batch)
                .await
                .expect("write file"),
            64
        );
    }
    table
        .drain_in_flight_maintenance()
        .await
        .expect("settle writes");
    (fixture, table, ctx)
}

async fn check_composite_file_pk_bounds(prefix: &str) {
    let (_fixture, table, ctx) = write_composite_files(prefix).await;
    let plan = table
        .scan(&ctx.state(), None, &[], None)
        .await
        .expect("plan composite scan");
    eprintln!(
        "PLAN {}",
        datafusion::physical_plan::displayable(plan.as_ref()).indent(true)
    );
    let mut files = Vec::new();
    scan_files(&plan, &mut files);
    assert_eq!(files.len(), 3, "three physical data files: {files:?}");
    files.sort_by_key(|file| {
        file.statistics
            .as_ref()
            .and_then(|s| s.column_statistics[0].min_value.get_value())
            .and_then(|value| match value {
                ScalarValue::Int32(value) => *value,
                _ => None,
            })
    });
    for (i, file) in files.iter().enumerate() {
        let stats = file
            .statistics
            .as_ref()
            .expect("per-file statistics present");
        eprintln!("FILE {} STATS {:?}", file.object_meta.location, stats);
        let start = i32::try_from(i).expect("small index") * 100;
        for column in 0..6 {
            let value = |v: i32| match column {
                0..4 => ScalarValue::Int32(Some(v)),
                4 => ScalarValue::Utf8(Some(format!("{prefix}cust-{v:04}"))),
                _ => ScalarValue::Int64(Some(i64::from(v))),
            };
            let (min, max) = if column == 4 && !prefix.is_empty() {
                // The writer bounds variable-length statistics to 64 bytes.
                // The upper bound increments the final retained byte.
                (
                    Precision::Inexact(ScalarValue::Utf8(Some("a".repeat(64)))),
                    Precision::Inexact(ScalarValue::Utf8(Some(format!("{}b", "a".repeat(63))))),
                )
            } else {
                (
                    Precision::Exact(value(start)),
                    Precision::Exact(value(start + 63)),
                )
            };
            assert_eq!(
                stats.column_statistics[column].min_value, min,
                "file={i}, column={column}, min"
            );
            assert_eq!(
                stats.column_statistics[column].max_value, max,
                "file={i}, column={column}, max"
            );
        }
    }
}
