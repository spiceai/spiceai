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

#![allow(clippy::expect_used)]
#![allow(clippy::clone_on_ref_ptr)]

//! Direct unit tests for `CayenneTableProvider::update`.
//!
//! These tests exercise the DataFusion-native `TableProvider::update` method
//! introduced on this branch by calling it directly on a constructed
//! `CayenneTableProvider`, bypassing the SQL planner. Complements the SQL-level
//! UPDATE coverage in `crates/runtime/tests/cayenne_catalog_ddl/mod.rs` and
//! `crates/runtime/tests/cluster/distributed_cayenne_catalog.rs`.

mod common;

use std::sync::Arc;

use arrow::array::{Array, Int64Array, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};

use cayenne::metadata::CreateTableOptions;
use cayenne::{CayenneTableProvider, MetadataCatalog};

use datafusion::datasource::TableProvider;
use datafusion::prelude::*;
use datafusion_physical_plan::collect;

test_with_backends!(test_update_single_row_by_pk_impl);
test_with_backends!(test_update_with_expression_impl);
test_with_backends!(test_update_multi_column_impl);
test_with_backends!(test_update_set_null_impl);
test_with_backends!(test_update_zero_match_impl);
test_with_backends!(test_update_no_filter_all_rows_impl);
test_with_backends!(test_update_preserves_inline_append_path_impl);

/// Build a `CayenneTableProvider` with schema (id, name, value) and `id` as PK.
async fn make_test_table(
    fixture: &common::TestFixture,
    table_name: &str,
) -> Result<Arc<CayenneTableProvider>, Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("value", DataType::Int64, true),
    ]));

    let table_options = CreateTableOptions {
        table_name: table_name.to_string(),
        schema,
        primary_key: vec!["id".to_string()],
        on_conflict: None,
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };

    let catalog_arc: Arc<dyn MetadataCatalog> = fixture.catalog.clone();
    let ctx = SessionContext::new();
    let table =
        CayenneTableProvider::create_table(catalog_arc, table_options, ctx.runtime_env()).await?;
    Ok(Arc::new(table))
}

/// Extract the `UInt64` count from the first column of the first result batch.
fn extract_u64_count(
    batches: &[arrow::record_batch::RecordBatch],
) -> Result<u64, Box<dyn std::error::Error>> {
    let batch = batches
        .first()
        .ok_or("no result batches from update plan")?;
    let arr = batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or("expected UInt64Array in update result")?;
    Ok(arr.value(0))
}

async fn test_update_single_row_by_pk_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let table = make_test_table(&fixture, "upd_single").await?;
    let ctx = SessionContext::new();
    ctx.register_table("upd_single", Arc::clone(&table) as _)?;

    ctx.sql(
        "INSERT INTO upd_single VALUES (1, 'Alice', 100), (2, 'Bob', 200), (3, 'Charlie', 300)",
    )
    .await?
    .collect()
    .await?;

    // UPDATE SET value = 999 WHERE id = 2
    let assignments = vec![("value".to_string(), lit(999_i64))];
    let filters = vec![col("id").eq(lit(2_i64))];

    let plan = table.update(&ctx.state(), assignments, filters).await?;
    let results = collect(plan, ctx.task_ctx()).await?;
    let count = extract_u64_count(&results)?;
    assert_eq!(count, 1, "expected 1 row updated");

    // Verify via SELECT.
    let df = ctx
        .sql("SELECT id, name, value FROM upd_single WHERE id = 2")
        .await?;
    let batches = df.collect().await?;
    let row = &batches[0];
    assert_eq!(
        row.column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("value col")
            .value(0),
        999,
        "value should be 999 after UPDATE"
    );

    Ok(())
}

async fn test_update_with_expression_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let table = make_test_table(&fixture, "upd_expr").await?;
    let ctx = SessionContext::new();
    ctx.register_table("upd_expr", Arc::clone(&table) as _)?;

    ctx.sql("INSERT INTO upd_expr VALUES (1, 'A', 10), (2, 'B', 20), (3, 'C', 30)")
        .await?
        .collect()
        .await?;

    // UPDATE SET value = value + 100 WHERE value > 15
    // Expression references the column itself; update() must resolve it
    // against the scan of the same table.
    let assignments = vec![("value".to_string(), col("value") + lit(100_i64))];
    let filters = vec![col("value").gt(lit(15_i64))];

    let plan = table.update(&ctx.state(), assignments, filters).await?;
    let results = collect(plan, ctx.task_ctx()).await?;
    let count = extract_u64_count(&results)?;
    // rows 2 (value=20) and 3 (value=30) match.
    assert_eq!(count, 2, "expected 2 rows updated");

    // Verify: ids 2, 3 should have new values 120, 130. Id 1 unchanged.
    let df = ctx
        .sql("SELECT id, value FROM upd_expr ORDER BY id")
        .await?;
    let batches = df.collect().await?;
    let row = &batches[0];
    let ids = row
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("id col");
    let values = row
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("value col");
    let mut seen = std::collections::HashMap::new();
    for i in 0..row.num_rows() {
        seen.insert(ids.value(i), values.value(i));
    }
    assert_eq!(seen.get(&1), Some(&10), "id=1 unchanged");
    assert_eq!(seen.get(&2), Some(&120), "id=2: 20 + 100 = 120");
    assert_eq!(seen.get(&3), Some(&130), "id=3: 30 + 100 = 130");

    Ok(())
}

async fn test_update_multi_column_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let table = make_test_table(&fixture, "upd_multi").await?;
    let ctx = SessionContext::new();
    ctx.register_table("upd_multi", Arc::clone(&table) as _)?;

    ctx.sql("INSERT INTO upd_multi VALUES (1, 'Alice', 100)")
        .await?
        .collect()
        .await?;

    let assignments = vec![
        ("name".to_string(), lit("Alicia")),
        ("value".to_string(), lit(42_i64)),
    ];
    let filters = vec![col("id").eq(lit(1_i64))];

    let plan = table.update(&ctx.state(), assignments, filters).await?;
    let results = collect(plan, ctx.task_ctx()).await?;
    let count = extract_u64_count(&results)?;
    assert_eq!(count, 1);

    let batches = ctx
        .sql("SELECT name, value FROM upd_multi WHERE id = 1")
        .await?
        .collect()
        .await?;
    let row = &batches[0];
    let name = row
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("name col")
        .value(0);
    let value = row
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("value col")
        .value(0);
    assert_eq!(name, "Alicia", "name should be updated");
    assert_eq!(value, 42, "value should be updated");

    Ok(())
}

async fn test_update_set_null_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let table = make_test_table(&fixture, "upd_null").await?;
    let ctx = SessionContext::new();
    ctx.register_table("upd_null", Arc::clone(&table) as _)?;

    ctx.sql("INSERT INTO upd_null VALUES (1, 'Alice', 100)")
        .await?
        .collect()
        .await?;

    // Use a typed NULL literal so DataFusion can infer the column type.
    let assignments = vec![(
        "name".to_string(),
        Expr::Literal(datafusion::scalar::ScalarValue::Utf8(None), None),
    )];
    let filters = vec![col("id").eq(lit(1_i64))];

    let plan = table.update(&ctx.state(), assignments, filters).await?;
    let results = collect(plan, ctx.task_ctx()).await?;
    let count = extract_u64_count(&results)?;
    assert_eq!(count, 1);

    let batches = ctx
        .sql("SELECT name FROM upd_null WHERE id = 1")
        .await?
        .collect()
        .await?;
    let col_arr = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("name col");
    assert!(col_arr.is_null(0), "name should be NULL after UPDATE");

    Ok(())
}

async fn test_update_zero_match_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let table = make_test_table(&fixture, "upd_zero").await?;
    let ctx = SessionContext::new();
    ctx.register_table("upd_zero", Arc::clone(&table) as _)?;

    ctx.sql("INSERT INTO upd_zero VALUES (1, 'Alice', 100)")
        .await?
        .collect()
        .await?;

    let assignments = vec![("value".to_string(), lit(0_i64))];
    let filters = vec![col("id").eq(lit(9999_i64))];

    let plan = table.update(&ctx.state(), assignments, filters).await?;
    let results = collect(plan, ctx.task_ctx()).await?;
    let count = extract_u64_count(&results)?;
    assert_eq!(count, 0, "zero-match UPDATE should report 0 rows");

    // Original row is untouched.
    let batches = ctx
        .sql("SELECT value FROM upd_zero WHERE id = 1")
        .await?
        .collect()
        .await?;
    let value = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("value col")
        .value(0);
    assert_eq!(value, 100, "value must remain 100 after zero-match UPDATE");

    Ok(())
}

async fn test_update_no_filter_all_rows_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let table = make_test_table(&fixture, "upd_all").await?;
    let ctx = SessionContext::new();
    ctx.register_table("upd_all", Arc::clone(&table) as _)?;

    ctx.sql("INSERT INTO upd_all VALUES (1, 'A', 10), (2, 'B', 20), (3, 'C', 30)")
        .await?
        .collect()
        .await?;

    let assignments = vec![("value".to_string(), lit(77_i64))];
    let filters: Vec<Expr> = vec![];

    let plan = table.update(&ctx.state(), assignments, filters).await?;
    let results = collect(plan, ctx.task_ctx()).await?;
    let count = extract_u64_count(&results)?;
    assert_eq!(count, 3, "no-filter UPDATE must touch all 3 rows");

    let batches = ctx
        .sql("SELECT id, value FROM upd_all ORDER BY id")
        .await?
        .collect()
        .await?;
    let values = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("value col");
    assert_eq!(values.value(0), 77);
    assert_eq!(values.value(1), 77);
    assert_eq!(values.value(2), 77);

    Ok(())
}

async fn test_update_preserves_inline_append_path_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let table_name = "upd_inline_writer";
    let table = make_test_table(&fixture, table_name).await?;
    let ctx = SessionContext::new();
    ctx.register_table(table_name, Arc::clone(&table) as _)?;
    let table_id = fixture.catalog.get_table(table_name).await?.table_id;

    ctx.sql("INSERT INTO upd_inline_writer VALUES (1, 'A', 10), (2, 'B', 20)")
        .await?
        .collect()
        .await?;
    assert_eq!(fixture.catalog.get_inlined_data_count(&table_id).await?, 2);

    let assignments = vec![
        ("name".to_string(), lit("B-updated")),
        ("value".to_string(), lit(200_i64)),
    ];
    let filters = vec![col("id").eq(lit(2_i64))];

    let plan = table.update(&ctx.state(), assignments, filters).await?;
    let results = collect(plan, ctx.task_ctx()).await?;
    let count = extract_u64_count(&results)?;
    assert_eq!(count, 1);

    let batches = ctx
        .sql("SELECT id, name, value FROM upd_inline_writer ORDER BY id")
        .await?
        .collect()
        .await?;
    let rows = &batches[0];
    let ids = rows
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("id col");
    let names = rows
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("name col");
    let values = rows
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("value col");

    assert_eq!(ids.values(), &[1_i64, 2]);
    assert_eq!(names.value(0), "A");
    assert_eq!(names.value(1), "B-updated");
    assert_eq!(values.values(), &[10_i64, 200]);
    assert_eq!(fixture.catalog.get_inlined_data_count(&table_id).await?, 2);
    assert!(
        fixture
            .catalog
            .get_table_delete_files(&table_id)
            .await?
            .is_empty()
    );

    Ok(())
}
