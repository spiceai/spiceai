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

#![allow(clippy::expect_used)]

//! A scan of a table with pending merge-on-read deletes must keep its row count
//! and column bounds.
//!
//! Such a scan is wrapped in `Int64PkDeletionFilterExec` (single `Int64` PK) or
//! `KeyBasedDeletionFilterExec` (any other PK). The optimizer reads statistics
//! through `StatisticsContext`, which hands each node its children's statistics;
//! a filter that ignores them and asks its child directly gets nothing back from
//! `DataFusion`'s built-in scans, so every table with a pending delete looks
//! unknown-sized to `JoinSelection`.

mod common;

use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use cayenne::metadata::{CreateTableOptions, VortexConfig};
use cayenne::{CayenneTableProvider, MetadataCatalog};
use datafusion::datasource::TableProvider;
use datafusion::physical_plan::{ExecutionPlan, StatisticsArgs, StatisticsContext, displayable};
use datafusion::prelude::*;
use datafusion_common::ScalarValue;
use datafusion_common::stats::Precision;

type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

const ROWS: i64 = 512;
const DELETED: i64 = 100;

test_with_backends!(int64_pk_scan_with_pending_deletes_keeps_statistics);
test_with_backends!(key_based_scan_with_pending_deletes_keeps_statistics);

async fn int64_pk_scan_with_pending_deletes_keeps_statistics(
    fixture: common::TestFixture,
) -> TestResult<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from((0..ROWS).collect::<Vec<_>>())),
            Arc::new(StringArray::from(
                (0..ROWS).map(|i| format!("n{i}")).collect::<Vec<_>>(),
            )),
        ],
    )?;
    assert_scan_keeps_statistics(
        &fixture,
        "del_stats_int64",
        schema,
        vec!["id".to_string()],
        batch,
        col("id").lt(lit(DELETED)),
        "Int64PkDeletionFilterExec",
    )
    .await
}

async fn key_based_scan_with_pending_deletes_keeps_statistics(
    fixture: common::TestFixture,
) -> TestResult<()> {
    // A `Utf8` PK takes the row-converter (key-based) deletion strategy. The
    // `Int64` value column carries the min/max this test checks survive.
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("code", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from((0..ROWS).collect::<Vec<_>>())),
            Arc::new(StringArray::from(
                (0..ROWS).map(|i| format!("c{i:05}")).collect::<Vec<_>>(),
            )),
        ],
    )?;
    assert_scan_keeps_statistics(
        &fixture,
        "del_stats_key",
        schema,
        vec!["code".to_string()],
        batch,
        col("id").lt(lit(DELETED)),
        "KeyBasedDeletionFilterExec",
    )
    .await
}

async fn assert_scan_keeps_statistics(
    fixture: &common::TestFixture,
    table_name: &str,
    schema: Arc<Schema>,
    primary_key: Vec<String>,
    batch: RecordBatch,
    delete_filter: Expr,
    expected_node: &str,
) -> TestResult<()> {
    let ctx = SessionContext::new();
    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let table = Arc::new(
        CayenneTableProvider::create_table(
            catalog,
            CreateTableOptions {
                table_name: table_name.to_string(),
                schema,
                primary_key,
                on_conflict: None,
                base_path: fixture.data_path.to_string_lossy().to_string(),
                partition_column: None,
                vortex_config: VortexConfig::default(),
            },
            ctx.runtime_env(),
        )
        .await?,
    );

    // Land the rows in a durable file so the delete is recorded as a pending
    // tombstone over file-resident rows, not applied to the inline memtable.
    common::insert_batch(&table, batch).await?;
    table.checkpoint_inlined_data().await?;
    let _ = table.checkpoint_mem_tier().await;
    table.flush_pending_maintenance().await?;

    let delete_plan = table.delete_from(&ctx.state(), vec![delete_filter]).await?;
    datafusion::physical_plan::collect(delete_plan, ctx.task_ctx()).await?;

    let plan = table.scan(&ctx.state(), None, &[], None).await?;
    let rendered = displayable(plan.as_ref()).indent(true).to_string();
    // The filter over the file-backed branch; the inline branch's filter sits
    // over an empty source and has no bounds to keep.
    let filter = find_filter_over_file_scan(&plan, expected_node).unwrap_or_else(|| {
        panic!("the file scan must be wrapped in {expected_node} for this test to mean anything, got:\n{rendered}")
    });
    let input =
        StatisticsContext::new().compute(filter.children()[0].as_ref(), &StatisticsArgs::new())?;
    let stats = StatisticsContext::new().compute(filter.as_ref(), &StatisticsArgs::new())?;

    let upper = usize::try_from(ROWS)?;
    let live = usize::try_from(ROWS - DELETED)?;
    assert!(
        matches!(input.num_rows, Precision::Exact(n) | Precision::Inexact(n) if n == upper),
        "the file scan must report its on-disk row count for this test to mean anything, got {:?}",
        input.num_rows
    );
    match stats.num_rows {
        Precision::Inexact(n) => assert!(
            (live..=upper).contains(&n),
            "num_rows must stay between the live count {live} and the on-disk count {upper}, got {n}"
        ),
        other => panic!(
            "{expected_node} over a scan with pending deletes must report an inexact row count, got {other:?}"
        ),
    }

    let id_stats = &stats.column_statistics[0];
    assert_eq!(
        id_stats.min_value,
        Precision::Inexact(ScalarValue::Int64(Some(0))),
        "id min must survive {expected_node}"
    );
    assert_eq!(
        id_stats.max_value,
        Precision::Inexact(ScalarValue::Int64(Some(ROWS - 1))),
        "id max must survive {expected_node}"
    );
    Ok(())
}

fn find_filter_over_file_scan(
    plan: &Arc<dyn ExecutionPlan>,
    name: &str,
) -> Option<Arc<dyn ExecutionPlan>> {
    if plan.name() == name
        && plan
            .children()
            .first()
            .is_some_and(|c| c.name() == "DataSourceExec")
    {
        return Some(Arc::clone(plan));
    }
    plan.children()
        .into_iter()
        .find_map(|c| find_filter_over_file_scan(c, name))
}
