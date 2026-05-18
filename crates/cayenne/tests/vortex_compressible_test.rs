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

#![expect(
    clippy::expect_used,
    reason = "test code uses .expect() for i32 conversion"
)]

//! Test for Cayenne support of highly compressed Vortex data.
//!
//! Validates that Cayenne correctly handles Vortex files whose compressed size
//! is smaller than the row count (sub-1-byte-per-row). This is common with
//! schemas dominated by NULL-cast columns (e.g. view materializations) and a
//! single low-cardinality grouping key.
//!
//! Writes such data through the full Cayenne write + read path and runs a
//! `GROUP BY` aggregation — which triggers `DataFusion` file re-partitioning —
//! to verify correctness.

mod common;

use std::sync::Arc;

use arrow::array::{Int32Array, NullArray, StringViewBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use cayenne::metadata::CreateTableOptions;
use cayenne::{CayenneTableProvider, MetadataCatalog};

use datafusion::assert_batches_eq;
use datafusion::prelude::SessionContext;

test_with_backends!(test_vortex_highly_compressible_data_impl);

/// Validates support for highly compressed Vortex data where the file size is
/// smaller than the row count (sub-1-byte-per-row). Writes a schema with a
/// single non-null grouping column and all-NULL nullable columns into Cayenne,
/// then runs a `GROUP BY` query that forces `DataFusion` to re-partition the
/// Vortex file.
async fn test_vortex_highly_compressible_data_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    // One non-null grouping column + nullable columns that are
    // entirely NULL — enough to achieve sub-1-byte-per-row compression.
    let schema = Arc::new(Schema::new(vec![
        Field::new("org_id", DataType::Int32, false),
        Field::new("col_a", DataType::Utf8View, true),
        Field::new("col_b", DataType::Utf8View, true),
        Field::new("col_c", DataType::Int32, true),
    ]));

    let table_options = CreateTableOptions {
        table_name: "compressible".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec![],
        on_conflict: None,
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };

    let catalog_arc = Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let ctx = SessionContext::new();
    let table =
        CayenneTableProvider::create_table(catalog_arc, table_options, ctx.runtime_env()).await?;
    let table = Arc::new(table);

    ctx.register_table(
        "compressible",
        Arc::clone(&table) as Arc<dyn datafusion::datasource::TableProvider>,
    )?;

    // 200 000 rows — large enough that DataFusion partitions across
    // multiple threads (default batch size 8 192).
    // - org_id cycles through 3 values (low cardinality)
    // - all other columns are NULL → extreme compression
    let num_rows: usize = 200_000;

    let org_ids: Int32Array = (0..num_rows)
        .map(|i| i32::try_from(i % 3 + 1).expect("value fits in i32"))
        .collect();

    // Build NULL arrays for each nullable column type.
    let null_view: Arc<dyn arrow::array::Array> = {
        let mut b = StringViewBuilder::new();
        for _ in 0..num_rows {
            b.append_null();
        }
        Arc::new(b.finish())
    };
    let null_int = arrow::compute::cast(&NullArray::new(num_rows), &DataType::Int32)?;

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(org_ids),
            Arc::clone(&null_view), // col_a
            Arc::clone(&null_view), // col_b
            null_int,               // col_c
        ],
    )?;

    // Write — Vortex produces a file whose size is smaller than its row count.
    common::insert_batch(&table, batch).await?;

    // Simple COUNT(*) — verifies basic read path.
    let result = ctx
        .sql("SELECT COUNT(*) as cnt FROM compressible")
        .await?
        .collect()
        .await?;
    let expected = [
        "+--------+",
        "| cnt    |",
        "+--------+",
        "| 200000 |",
        "+--------+",
    ];
    assert_batches_eq!(expected, &result);

    // GROUP BY to simulate DataFusion re-partition query
    let result = ctx
        .sql(
            "SELECT org_id, COUNT(*) as cnt \
             FROM compressible \
             GROUP BY org_id \
             ORDER BY org_id",
        )
        .await?
        .collect()
        .await?;

    assert!(!result.is_empty(), "GROUP BY query returned no results");

    Ok(())
}
