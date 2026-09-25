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

//! A file's min/max bounds have to arrive tagged as the column's own Arrow type.
//!
//! `FilterExec` derives an `Interval` per column from the scan's statistics and
//! `DataFusion` asserts both endpoints share one type, taking any bound the file
//! does not describe from the column instead. Vortex models one string and one
//! binary dtype where Arrow has several representations, so a `LargeUtf8`
//! column's footer bounds can surface as `Utf8`. The interval for such a column
//! is then built from `Utf8` and `LargeUtf8`, and planning fails with
//! `Endpoints of an Interval should have the same type` for a query that projects
//! the column. A query that filters without projecting it, such as `COUNT(*)`,
//! plans regardless.

mod common;

use std::sync::Arc;

use arrow::array::{Int64Array, LargeStringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use cayenne::metadata::{CreateTableOptions, VortexConfig};
use cayenne::{CayenneTableProvider, MetadataCatalog};
use datafusion::datasource::TableProvider;
use datafusion::prelude::*;
use datafusion_common::{ColumnStatistics, ScalarValue};

type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

const TABLE: &str = "large_string_bounds";

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("trip_id", DataType::Int64, false),
        // The representation the taxi datasets use for their flag columns, and
        // the one a footer bound comes back mistagged for.
        Field::new("store_and_fwd_flag", DataType::LargeUtf8, true),
    ]))
}

/// Mostly one value, a second one, and nulls — the shape of a flag column.
fn batch() -> RecordBatch {
    let ids: Vec<i64> = (0..2_000).collect();
    let flags: Vec<Option<&str>> = ids
        .iter()
        .map(|id| match id % 10 {
            0 => None,
            1 => Some("Y"),
            _ => Some("N"),
        })
        .collect();

    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(LargeStringArray::from(flags)),
        ],
    )
    .expect("batch")
}

async fn table_with_durable_file(
    fixture: &common::TestFixture,
    ctx: &SessionContext,
) -> TestResult<Arc<CayenneTableProvider>> {
    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let table = Arc::new(
        CayenneTableProvider::create_table(
            catalog,
            CreateTableOptions {
                table_name: TABLE.to_string(),
                schema: schema(),
                primary_key: vec![],
                on_conflict: None,
                base_path: fixture.data_path.to_string_lossy().to_string(),
                partition_column: None,
                vortex_config: VortexConfig::default(),
            },
            ctx.runtime_env(),
        )
        .await?,
    );
    common::insert_batch(&table, batch()).await?;
    let _ = table.checkpoint_inlined_data().await;
    let _ = table.checkpoint_mem_tier().await;
    table.flush_pending_maintenance().await?;
    Ok(table)
}

/// The bounds one scan reports, pulled out of the plan the way `FilterExec`
/// reads them.
async fn scan_bounds(
    table: &Arc<CayenneTableProvider>,
    ctx: &SessionContext,
    column: &str,
) -> TestResult<(Option<ScalarValue>, Option<ScalarValue>)> {
    let plan = table.scan(&ctx.state(), None, &[], None).await?;
    let index = schema().index_of(column)?;
    let stats = plan.partition_statistics(None)?;
    let column_stats: &ColumnStatistics = stats
        .column_statistics
        .get(index)
        .ok_or_else(|| format!("no statistics for column {column}"))?;
    Ok((
        column_stats.min_value.get_value().cloned(),
        column_stats.max_value.get_value().cloned(),
    ))
}

/// Every bound a scan reports carries its column's Arrow type, and a string
/// column reports the bounds it actually has.
#[tokio::test]
async fn scan_reports_string_bounds_typed_as_the_column() -> TestResult<()> {
    let fixture = common::TestFixture::new(common::BackendType::Sqlite).await?;
    let ctx = SessionContext::new();
    let table = table_with_durable_file(&fixture, &ctx).await?;

    let (min, max) = scan_bounds(&table, &ctx, "store_and_fwd_flag").await?;
    let expected_min = ScalarValue::LargeUtf8(Some("N".to_string()));
    let expected_max = ScalarValue::LargeUtf8(Some("Y".to_string()));

    assert_eq!(
        min,
        Some(expected_min),
        "the string column's lower bound must be a `LargeUtf8`, since that is the \
         type the column has"
    );
    assert_eq!(
        max,
        Some(expected_max),
        "the string column's upper bound must be a `LargeUtf8`, since that is the \
         type the column has"
    );

    let (min, max) = scan_bounds(&table, &ctx, "trip_id").await?;
    assert_eq!(
        min,
        Some(ScalarValue::Int64(Some(0))),
        "the numeric column's lower bound"
    );
    assert_eq!(
        max,
        Some(ScalarValue::Int64(Some(1_999))),
        "the numeric column's upper bound"
    );

    Ok(())
}

/// A filter on a numeric column over a scan that also projects a `LargeUtf8`
/// column: the projected column's interval is the one that asserts. Whether the
/// bounds are usable at all is decided by their types, which
/// [`scan_reports_string_bounds_typed_as_the_column`] asserts; this covers
/// planning and the rows.
#[tokio::test]
async fn filters_plan_when_a_string_column_is_scanned() -> TestResult<()> {
    let fixture = common::TestFixture::new(common::BackendType::Sqlite).await?;
    let ctx = SessionContext::new();
    let table = table_with_durable_file(&fixture, &ctx).await?;
    ctx.register_table(TABLE, Arc::clone(&table) as Arc<dyn TableProvider>)?;

    for sql in [
        "SELECT * FROM large_string_bounds WHERE trip_id = 221",
        "SELECT store_and_fwd_flag FROM large_string_bounds WHERE trip_id = 221",
        "SELECT * FROM large_string_bounds WHERE trip_id > 100 AND trip_id < 500",
    ] {
        ctx.sql(sql).await?.create_physical_plan().await?;
    }

    // And the rows still come back.
    let batches = ctx
        .sql("SELECT * FROM large_string_bounds WHERE trip_id = 221")
        .await?
        .collect()
        .await?;
    let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(rows, 1, "the filtered row");

    Ok(())
}
