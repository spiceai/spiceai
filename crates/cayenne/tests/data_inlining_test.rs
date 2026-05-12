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

//! Tests for data inlining: small batches stored in the metastore as Arrow IPC blobs.

mod common;

use arrow::array::{
    Array, BooleanArray, Date32Array, Float64Array, Int32Array, Int64Array, StringArray,
    TimestampMillisecondArray, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use cayenne::metadata::CreateTableOptions;
use cayenne::{CayenneTableProvider, MetadataCatalog};
use datafusion::datasource::TableProvider;
use datafusion::prelude::*;
use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};
use std::sync::Arc;

test_with_backends!(test_inlined_data_crud);
test_with_backends!(test_small_insert_inlined);
test_with_backends!(test_inlined_data_visible_in_scan);
test_with_backends!(test_limit_on_inlined_scan);
test_with_backends!(test_roundtrip_preserves_values);
test_with_backends!(test_roundtrip_preserves_nulls);
test_with_backends!(test_roundtrip_mixed_types);
test_with_backends!(test_roundtrip_many_small_batches);
test_with_backends!(test_roundtrip_mixed_inline_and_vortex);
test_with_backends!(test_roundtrip_across_reopen);
test_with_backends!(test_roundtrip_exceeds_byte_threshold);
test_with_backends!(test_pk_upsert_inline_mutation);
test_with_backends!(test_pk_delete_inline_mutation);

/// Test basic CRUD for inlined data via the catalog API.
async fn test_inlined_data_crud(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let catalog = &fixture.catalog;

    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

    let table_id = catalog
        .create_table(CreateTableOptions {
            table_name: "inline_crud_test".to_string(),
            schema: Arc::clone(&schema),
            primary_key: vec![],
            on_conflict: None,
            base_path: fixture.data_path.to_string_lossy().to_string(),
            partition_column: None,
            vortex_config: cayenne::metadata::VortexConfig::default(),
        })
        .await?;

    // Initially no inlined data
    let data = catalog.get_inlined_data(&table_id).await?;
    assert!(data.is_empty());
    assert_eq!(catalog.get_inlined_data_count(&table_id).await?, 0);

    // Add inlined data
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
    )?;
    let mut ipc_buf = Vec::new();
    {
        let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut ipc_buf, &schema)?;
        writer.write(&batch)?;
        writer.finish()?;
    }

    let id = catalog
        .add_inlined_data(cayenne::InlinedData {
            inlined_id: String::new(),
            table_id: table_id.clone(),
            partition_key: None,
            data_ipc: ipc_buf,
            record_count: 3,
            sequence_number: 0,
            created_at: String::new(),
        })
        .await?;
    assert!(!id.is_empty());

    // Verify count
    assert_eq!(catalog.get_inlined_data_count(&table_id).await?, 3);

    // Read back
    let data = catalog.get_inlined_data(&table_id).await?;
    assert_eq!(data.len(), 1);
    assert_eq!(data[0].record_count, 3);

    // Clear
    catalog.clear_inlined_data(&table_id).await?;
    assert_eq!(catalog.get_inlined_data_count(&table_id).await?, 0);

    Ok(())
}

/// Test that a small insert (< 1024 rows) gets inlined in the metastore.
async fn test_small_insert_inlined(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let catalog = &fixture.catalog;
    let data_path = &fixture.data_path;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));

    let ctx = SessionContext::new();
    let table = CayenneTableProvider::create_table(
        Arc::clone(catalog) as Arc<dyn MetadataCatalog>,
        CreateTableOptions {
            table_name: "small_inline_test".to_string(),
            schema: Arc::clone(&schema),
            primary_key: vec![],
            on_conflict: None,
            base_path: data_path.to_string_lossy().to_string(),
            partition_column: None,
            vortex_config: cayenne::metadata::VortexConfig::default(),
        },
        ctx.runtime_env(),
    )
    .await?;

    let table_id = catalog.get_table("small_inline_test").await?.table_id;

    // Insert a small batch (5 rows — well under 1024 threshold)
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"])),
        ],
    )?;

    common::insert_batch(&table, batch).await?;

    // Verify data was inlined in the metastore
    let inlined_count = catalog.get_inlined_data_count(&table_id).await?;
    assert_eq!(
        inlined_count, 5,
        "Expected 5 rows to be inlined in the metastore"
    );

    Ok(())
}

/// Test that inlined data is visible when scanning the table.
async fn test_inlined_data_visible_in_scan(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let catalog = &fixture.catalog;
    let data_path = &fixture.data_path;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let ctx = SessionContext::new();
    let table = CayenneTableProvider::create_table(
        Arc::clone(catalog) as Arc<dyn MetadataCatalog>,
        CreateTableOptions {
            table_name: "scan_inline_test".to_string(),
            schema: Arc::clone(&schema),
            primary_key: vec![],
            on_conflict: None,
            base_path: data_path.to_string_lossy().to_string(),
            partition_column: None,
            vortex_config: cayenne::metadata::VortexConfig::default(),
        },
        ctx.runtime_env(),
    )
    .await?;

    // Insert small batch (should be inlined)
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![10, 20, 30])),
            Arc::new(Int64Array::from(vec![100, 200, 300])),
        ],
    )?;
    common::insert_batch(&table, batch).await?;

    // Query should see inlined data
    let table_arc: Arc<dyn TableProvider> = Arc::new(table);
    ctx.register_table("scan_inline_test", Arc::clone(&table_arc))?;

    let df = ctx
        .sql("SELECT * FROM scan_inline_test ORDER BY id")
        .await?;
    let results = df.collect().await?;
    let total_rows: usize = results.iter().map(RecordBatch::num_rows).sum();

    assert_eq!(total_rows, 3, "Expected 3 rows from inlined data scan");

    // Concatenate all result batches to avoid flaky assertions when the planner
    // splits results across multiple RecordBatches.
    let combined =
        arrow::compute::concat_batches(&results[0].schema(), &results).expect("concat batches");
    let id_col = combined
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("id column");
    assert_eq!(id_col.value(0), 10);
    assert_eq!(id_col.value(1), 20);
    assert_eq!(id_col.value(2), 30);

    Ok(())
}

async fn test_limit_on_inlined_scan(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let (table, ctx) = create_table(&fixture, "inline_limit", Arc::clone(&schema)).await?;
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(Int64Array::from(vec![10, 20, 30, 40, 50])),
        ],
    )?;
    common::insert_batch(&table, batch).await?;

    ctx.register_table("inline_limit", Arc::new(table))?;
    let results = ctx
        .sql("SELECT id FROM inline_limit LIMIT 2")
        .await?
        .collect()
        .await?;
    assert!(!results.is_empty(), "no result batches for LIMIT query");
    let got = arrow::compute::concat_batches(&results[0].schema(), &results)?;
    let ids = got
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("id");

    assert_eq!(got.num_rows(), 2);
    assert_eq!(ids.values(), &[1_i64, 2]);

    Ok(())
}

// ============================================================================
// Round-trip value-level tests
//
// These verify that data inserted via the inlining path is returned with
// EXACT value fidelity from subsequent scans. Data correctness is the top
// project priority — row counts alone are not enough.
// ============================================================================

/// Helper: create a no-PK table and register it in a `SessionContext`.
async fn create_table(
    fixture: &common::TestFixture,
    name: &str,
    schema: Arc<Schema>,
) -> Result<(CayenneTableProvider, SessionContext), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    let table = CayenneTableProvider::create_table(
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>,
        CreateTableOptions {
            table_name: name.to_string(),
            schema,
            primary_key: vec![],
            on_conflict: None,
            base_path: fixture.data_path.to_string_lossy().to_string(),
            partition_column: None,
            vortex_config: cayenne::metadata::VortexConfig::default(),
        },
        ctx.runtime_env(),
    )
    .await?;
    Ok((table, ctx))
}

async fn create_pk_upsert_table(
    fixture: &common::TestFixture,
    name: &str,
) -> Result<(Arc<CayenneTableProvider>, SessionContext, String), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let ctx = SessionContext::new();
    let table = Arc::new(
        CayenneTableProvider::create_table(
            Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>,
            CreateTableOptions {
                table_name: name.to_string(),
                schema,
                primary_key: vec!["id".to_string()],
                on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
                    "id".to_string(),
                ]))),
                base_path: fixture.data_path.to_string_lossy().to_string(),
                partition_column: None,
                vortex_config: cayenne::metadata::VortexConfig::default(),
            },
            ctx.runtime_env(),
        )
        .await?,
    );
    ctx.register_table(name, Arc::clone(&table) as Arc<dyn TableProvider>)?;
    let table_id = fixture.catalog.get_table(name).await?.table_id;

    Ok((table, ctx, table_id))
}

async fn collect_delete_count(
    ctx: &SessionContext,
    sql: &str,
) -> Result<u64, Box<dyn std::error::Error>> {
    let results = ctx.sql(sql).await?.collect().await?;
    let batch = results.first().ok_or("delete returned no batches")?;
    let count = batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or("delete count should be UInt64")?
        .value(0);
    Ok(count)
}

async fn test_pk_upsert_inline_mutation(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_table, ctx, table_id) = create_pk_upsert_table(&fixture, "inline_pk_upsert").await?;

    ctx.sql("INSERT INTO inline_pk_upsert VALUES (1, 'Alice'), (2, 'Bob')")
        .await?
        .collect()
        .await?;
    assert_eq!(fixture.catalog.get_inlined_data_count(&table_id).await?, 2);

    ctx.sql("INSERT INTO inline_pk_upsert VALUES (1, 'Alicia')")
        .await?
        .collect()
        .await?;

    let got = collect_sorted(&ctx, "SELECT id, name FROM inline_pk_upsert ORDER BY id").await?;
    let ids = got
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("id");
    let names = got
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("name");

    assert_eq!(got.num_rows(), 2);
    assert_eq!(ids.values(), &[1_i64, 2]);
    assert_eq!(names.value(0), "Alicia");
    assert_eq!(names.value(1), "Bob");
    assert_eq!(fixture.catalog.get_inlined_data_count(&table_id).await?, 2);
    let mut inlined_record_counts = fixture
        .catalog
        .get_inlined_data(&table_id)
        .await?
        .into_iter()
        .map(|entry| entry.record_count)
        .collect::<Vec<_>>();
    inlined_record_counts.sort_unstable();
    assert_eq!(inlined_record_counts, vec![1_i64, 1]);
    assert_eq!(
        fixture.catalog.get_inlined_deletes(&table_id).await?.len(),
        0
    );
    assert!(
        fixture
            .catalog
            .get_table_delete_files(&table_id)
            .await?
            .is_empty()
    );

    Ok(())
}

async fn test_pk_delete_inline_mutation(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_table, ctx, table_id) = create_pk_upsert_table(&fixture, "inline_pk_delete").await?;

    ctx.sql("INSERT INTO inline_pk_delete VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Cara')")
        .await?
        .collect()
        .await?;

    let deleted = collect_delete_count(&ctx, "DELETE FROM inline_pk_delete WHERE id = 2").await?;
    assert_eq!(deleted, 1);

    let got = collect_sorted(&ctx, "SELECT id, name FROM inline_pk_delete ORDER BY id").await?;
    let ids = got
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("id");
    let names = got
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("name");

    assert_eq!(got.num_rows(), 2);
    assert_eq!(ids.values(), &[1_i64, 3]);
    assert_eq!(names.value(0), "Alice");
    assert_eq!(names.value(1), "Cara");
    assert_eq!(fixture.catalog.get_inlined_data_count(&table_id).await?, 2);
    let inlined_data = fixture.catalog.get_inlined_data(&table_id).await?;
    assert_eq!(inlined_data.len(), 1);
    assert_eq!(inlined_data[0].record_count, 2);
    assert_eq!(
        fixture.catalog.get_inlined_deletes(&table_id).await?.len(),
        0
    );
    assert!(
        fixture
            .catalog
            .get_table_delete_files(&table_id)
            .await?
            .is_empty()
    );

    Ok(())
}

/// Collect all rows from `SELECT * FROM t ORDER BY <key>` into a single batch.
async fn collect_sorted(
    ctx: &SessionContext,
    sql: &str,
) -> Result<RecordBatch, Box<dyn std::error::Error>> {
    let df = ctx.sql(sql).await?;
    let results = df.collect().await?;
    assert!(!results.is_empty(), "no result batches for `{sql}`");
    let schema = results[0].schema();
    Ok(arrow::compute::concat_batches(&schema, &results)?)
}

/// Insert a small batch and verify scan returns identical values.
async fn test_roundtrip_preserves_values(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("score", DataType::Float64, false),
    ]));

    let (table, ctx) = create_table(&fixture, "rt_values", Arc::clone(&schema)).await?;
    let table_id = fixture.catalog.get_table("rt_values").await?.table_id;

    let ids = vec![1_i64, 2, 3, 4, 5];
    let names = vec!["alpha", "beta", "gamma", "delta", "epsilon"];
    let scores = vec![1.5_f64, 2.5, -3.75, 0.0, f64::INFINITY];

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(ids.clone())),
            Arc::new(StringArray::from(names.clone())),
            Arc::new(Float64Array::from(scores.clone())),
        ],
    )?;
    common::insert_batch(&table, batch).await?;

    assert_eq!(
        fixture.catalog.get_inlined_data_count(&table_id).await?,
        5,
        "small insert should be inlined on {}",
        fixture.backend_type.name(),
    );

    ctx.register_table("rt_values", Arc::new(table))?;
    let got = collect_sorted(&ctx, "SELECT id, name, score FROM rt_values ORDER BY id").await?;

    let id_col = got
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("id");
    let name_col = got
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("name");
    let score_col = got
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("score");

    assert_eq!(id_col.len(), 5);
    for (i, expected) in ids.iter().enumerate() {
        assert_eq!(id_col.value(i), *expected, "id[{i}]");
        assert_eq!(name_col.value(i), names[i], "name[{i}]");
        // Compare score round-trip bit-exactly: Arrow IPC preserves every
        // f64 bit pattern (including NaN / +Inf / -Inf / -0.0), so equality
        // on raw bits is the correct assertion (avoids `clippy::float_cmp`
        // and handles NaN, which `==` would not).
        assert_eq!(
            score_col.value(i).to_bits(),
            scores[i].to_bits(),
            "score[{i}] bits differ: got {} want {}",
            score_col.value(i),
            scores[i],
        );
    }

    Ok(())
}

/// Round-trip should preserve NULL values from inlined data.
async fn test_roundtrip_preserves_nulls(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("score", DataType::Float64, true),
    ]));

    let (table, ctx) = create_table(&fixture, "rt_nulls", Arc::clone(&schema)).await?;

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec![Some("alice"), None, Some("carol")])),
            Arc::new(Float64Array::from(vec![Some(1.5), Some(2.5), None])),
        ],
    )?;
    common::insert_batch(&table, batch).await?;

    ctx.register_table("rt_nulls", Arc::new(table))?;
    let got = collect_sorted(&ctx, "SELECT id, name, score FROM rt_nulls ORDER BY id").await?;

    let name_col = got
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("name");
    let score_col = got
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("score");

    assert!(!name_col.is_null(0) && name_col.value(0) == "alice");
    assert!(name_col.is_null(1), "NULL name at row 1 must round-trip");
    assert!(!name_col.is_null(2) && name_col.value(2) == "carol");

    assert!(!score_col.is_null(0) && (score_col.value(0) - 1.5).abs() < f64::EPSILON);
    assert!(!score_col.is_null(1) && (score_col.value(1) - 2.5).abs() < f64::EPSILON);
    assert!(score_col.is_null(2), "NULL score at row 2 must round-trip");

    Ok(())
}

/// Round-trip should preserve a diverse set of Arrow types from inlined data.
async fn test_roundtrip_mixed_types(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("i32", DataType::Int32, false),
        Field::new("flt", DataType::Float64, false),
        Field::new("flag", DataType::Boolean, false),
        Field::new("s", DataType::Utf8, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("d", DataType::Date32, false),
    ]));

    let (table, ctx) = create_table(&fixture, "rt_mixed", Arc::clone(&schema)).await?;

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1_i64, 2, 3])),
            Arc::new(Int32Array::from(vec![10_i32, 20, 30])),
            Arc::new(Float64Array::from(vec![1.1_f64, 2.2, 3.3])),
            Arc::new(BooleanArray::from(vec![true, false, true])),
            Arc::new(StringArray::from(vec!["x", "y", "z"])),
            Arc::new(TimestampMillisecondArray::from(vec![
                1_000_i64, 2_000, 3_000,
            ])),
            Arc::new(Date32Array::from(vec![18_000_i32, 18_100, 18_200])),
        ],
    )?;
    common::insert_batch(&table, batch).await?;

    ctx.register_table("rt_mixed", Arc::new(table))?;
    let got = collect_sorted(
        &ctx,
        "SELECT id, i32, flt, flag, s, ts, d FROM rt_mixed ORDER BY id",
    )
    .await?;

    assert_eq!(got.num_rows(), 3, "mixed-type round-trip row count");

    let i32_col = got
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("i32");
    let flt_col = got
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("flt");
    let flag_col = got
        .column(3)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("flag");
    let s_col = got
        .column(4)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("s");
    let ts_col = got
        .column(5)
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .expect("ts");
    let d_col = got
        .column(6)
        .as_any()
        .downcast_ref::<Date32Array>()
        .expect("d");

    assert_eq!(i32_col.values(), &[10_i32, 20, 30]);
    for (i, exp) in [1.1_f64, 2.2, 3.3].iter().enumerate() {
        assert!((flt_col.value(i) - exp).abs() < f64::EPSILON);
    }
    assert!(flag_col.value(0) && !flag_col.value(1) && flag_col.value(2));
    assert_eq!(s_col.value(0), "x");
    assert_eq!(s_col.value(1), "y");
    assert_eq!(s_col.value(2), "z");
    assert_eq!(ts_col.values(), &[1_000_i64, 2_000, 3_000]);
    assert_eq!(d_col.values(), &[18_000_i32, 18_100, 18_200]);

    Ok(())
}

/// Many small inlined batches should accumulate and round-trip completely.
async fn test_roundtrip_many_small_batches(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let (table, ctx) = create_table(&fixture, "rt_many", Arc::clone(&schema)).await?;
    let table_id = fixture.catalog.get_table("rt_many").await?.table_id;

    // 10 batches of 10 rows each => 100 rows total, all inlined.
    for i in 0..10_i64 {
        let ids: Vec<i64> = (i * 10..i * 10 + 10).collect();
        let vals: Vec<i64> = ids.iter().map(|v| v * 7).collect();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(Int64Array::from(vals)),
            ],
        )?;
        common::insert_batch(&table, batch).await?;
    }

    assert_eq!(
        fixture.catalog.get_inlined_data_count(&table_id).await?,
        100,
        "10 × 10 small inserts should produce 100 inlined rows",
    );

    ctx.register_table("rt_many", Arc::new(table))?;
    let got = collect_sorted(&ctx, "SELECT id, value FROM rt_many ORDER BY id").await?;

    assert_eq!(got.num_rows(), 100);
    let id_col = got
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("id");
    let val_col = got
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("value");

    for i in 0..100 {
        let expected_id = i64::try_from(i).expect("fits in i64");
        assert_eq!(id_col.value(i), expected_id, "id[{i}]");
        assert_eq!(val_col.value(i), expected_id * 7, "value[{i}]");
    }

    Ok(())
}

/// Mixing inlined data with a large insert that bypasses inlining must return
/// every row exactly once from the combined scan.
async fn test_roundtrip_mixed_inline_and_vortex(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let (table, ctx) = create_table(&fixture, "rt_mixed_paths", Arc::clone(&schema)).await?;
    let table_id = fixture.catalog.get_table("rt_mixed_paths").await?.table_id;

    // Small insert — goes through the inlining path.
    let small = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(Int64Array::from(vec![10, 20, 30, 40, 50])),
        ],
    )?;
    common::insert_batch(&table, small).await?;

    assert_eq!(fixture.catalog.get_inlined_data_count(&table_id).await?, 5);

    // Large insert (>INLINE_MAX_ROWS=1024) — bypasses inlining, written to Vortex.
    let n = 1500_i64;
    let big_ids: Vec<i64> = (100..100 + n).collect();
    let big_vals: Vec<i64> = big_ids.iter().map(|v| v * 2).collect();
    let big = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(big_ids.clone())),
            Arc::new(Int64Array::from(big_vals.clone())),
        ],
    )?;
    common::insert_batch(&table, big).await?;

    ctx.register_table("rt_mixed_paths", Arc::new(table))?;

    // Aggregate query verifies every row was read exactly once and values are intact.
    let df = ctx
        .sql(
            "SELECT COUNT(*) AS c, SUM(id) AS s_id, SUM(value) AS s_val \
             FROM rt_mixed_paths",
        )
        .await?;
    let results = df.collect().await?;
    let batch = arrow::compute::concat_batches(&results[0].schema(), &results)?;

    let c = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count");
    let s_id = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("sum id");
    let s_val = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("sum value");

    let expected_count = 5 + n;
    let expected_sum_id: i64 = (1..=5).sum::<i64>() + big_ids.iter().sum::<i64>();
    let expected_sum_val: i64 =
        [10_i64, 20, 30, 40, 50].iter().sum::<i64>() + big_vals.iter().sum::<i64>();

    assert_eq!(c.value(0), expected_count);
    assert_eq!(s_id.value(0), expected_sum_id);
    assert_eq!(s_val.value(0), expected_sum_val);

    Ok(())
}

/// Data inserted via the inlining path must survive dropping and reopening the
/// table provider, because the blob lives in the metastore (not in-process
/// state).
async fn test_roundtrip_across_reopen(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let (table, _ctx) = create_table(&fixture, "rt_reopen", Arc::clone(&schema)).await?;
    let table_id = fixture.catalog.get_table("rt_reopen").await?.table_id;

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["apple", "banana", "cherry"])),
        ],
    )?;
    common::insert_batch(&table, batch).await?;

    assert_eq!(fixture.catalog.get_inlined_data_count(&table_id).await?, 3);

    // Drop the provider; open a fresh one from the same catalog.
    drop(table);

    let ctx2 = SessionContext::new();
    let reopened = cayenne::CayenneTableProviderBuilder::new(
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>,
        ctx2.runtime_env(),
    )
    .open("rt_reopen")
    .await?;

    ctx2.register_table("rt_reopen", Arc::new(reopened))?;
    let got = collect_sorted(&ctx2, "SELECT id, name FROM rt_reopen ORDER BY id").await?;

    assert_eq!(got.num_rows(), 3, "inlined rows must survive reopen");

    let id_col = got
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("id");
    let name_col = got
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("name");

    assert_eq!(id_col.values(), &[1_i64, 2, 3]);
    assert_eq!(name_col.value(0), "apple");
    assert_eq!(name_col.value(1), "banana");
    assert_eq!(name_col.value(2), "cherry");

    // Inlined count should still be 3 after reopen (blob lives in metastore).
    assert_eq!(fixture.catalog.get_inlined_data_count(&table_id).await?, 3);

    Ok(())
}

/// A batch under `INLINE_MAX_ROWS` rows but whose serialized IPC exceeds the
/// byte ceiling (1 MiB) must bypass inlining and land in Vortex, while still
/// round-tripping correctly.
async fn test_roundtrip_exceeds_byte_threshold(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("blob", DataType::Utf8, false),
    ]));

    let (table, ctx) = create_table(&fixture, "rt_bytes", Arc::clone(&schema)).await?;
    let table_id = fixture.catalog.get_table("rt_bytes").await?.table_id;

    // 200 rows × ~8 KiB string each ≈ 1.6 MiB — well above the 1 MiB byte
    // ceiling but well below the 1024-row ceiling.
    let row_count: usize = 200;
    let payload = "A".repeat(8 * 1024);
    let ids: Vec<i64> = (0..i64::try_from(row_count).expect("row_count fits in i64")).collect();
    let blobs: Vec<&str> = (0..row_count).map(|_| payload.as_str()).collect();

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(ids.clone())),
            Arc::new(StringArray::from(blobs)),
        ],
    )?;
    common::insert_batch(&table, batch).await?;

    // Must NOT be inlined — exceeded INLINE_MAX_BYTES.
    assert_eq!(
        fixture.catalog.get_inlined_data_count(&table_id).await?,
        0,
        "batch exceeding INLINE_MAX_BYTES must bypass inlining",
    );

    ctx.register_table("rt_bytes", Arc::new(table))?;
    let df = ctx
        .sql("SELECT COUNT(*) AS c, MIN(id) AS mn, MAX(id) AS mx FROM rt_bytes")
        .await?;
    let results = df.collect().await?;
    let batch = arrow::compute::concat_batches(&results[0].schema(), &results)?;

    let c = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count");
    let mn = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("min");
    let mx = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("max");

    assert_eq!(c.value(0), i64::try_from(row_count).expect("fits"));
    assert_eq!(mn.value(0), 0);
    assert_eq!(mx.value(0), i64::try_from(row_count - 1).expect("fits"));

    Ok(())
}
