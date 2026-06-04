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

//! Tests that inline writes proceed when pending deletions exist and when
//! upserts conflict with file-backed PKs.

#![allow(clippy::expect_used)]

mod common;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use cayenne::metadata::DEFAULT_INLINE_MAX_ROWS;
use cayenne::{
    CayenneCatalog, CayenneTableProvider, MetadataCatalog, metadata::CreateTableOptions,
};
use common::TestFixture;
use datafusion::datasource::TableProvider;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::{col, lit};
use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};
use std::sync::Arc;

type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

// =============================================================================
// Helpers
// =============================================================================

fn simple_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

fn utf8_pk_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("email", DataType::Utf8, false),
        Field::new("items_bought", DataType::Int64, false),
    ]))
}

fn composite_pk_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("k1", DataType::Int64, false),
        Field::new("k2", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

async fn setup_table(
    fixture: &TestFixture,
    table_name: &str,
    schema: Arc<Schema>,
    primary_key: Vec<String>,
) -> TestResult<(Arc<CayenneTableProvider>, SessionContext)> {
    let table_options = CreateTableOptions {
        table_name: table_name.to_string(),
        schema: Arc::clone(&schema),
        primary_key: primary_key.clone(),
        on_conflict: Some(OnConflict::Upsert(ColumnReference::new(primary_key))),
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };

    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let ctx = SessionContext::new();
    let table = Arc::new(
        CayenneTableProvider::create_table(catalog, table_options, ctx.runtime_env()).await?,
    );
    ctx.register_table(table_name, Arc::clone(&table) as Arc<dyn TableProvider>)?;
    Ok((table, ctx))
}

async fn insert_batch(table: &CayenneTableProvider, batch: RecordBatch) -> TestResult<u64> {
    common::insert_batch(table, batch).await.map_err(Into::into)
}

async fn query_value(ctx: &SessionContext, sql: &str) -> TestResult<Vec<i64>> {
    let df = ctx.sql(sql).await?;
    let batches = df.collect().await?;
    let mut values = Vec::new();
    for batch in &batches {
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("expected Int64Array");
        for i in 0..col.len() {
            values.push(col.value(i));
        }
    }
    Ok(values)
}

async fn row_count(ctx: &SessionContext, table_name: &str) -> TestResult<usize> {
    let df = ctx.sql(&format!("SELECT * FROM {table_name}")).await?;
    let batches = df.collect().await?;
    Ok(batches.iter().map(RecordBatch::num_rows).sum())
}

async fn delete_by_id(table: &CayenneTableProvider, id: i64) -> TestResult<u64> {
    let ctx = SessionContext::new();
    let plan = table
        .delete_from(&ctx.state(), vec![col("id").eq(lit(id))])
        .await?;
    let results = datafusion_physical_plan::collect(plan, ctx.task_ctx()).await?;
    Ok(results
        .first()
        .and_then(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<arrow::array::UInt64Array>()
        })
        .and_then(|a| a.values().first())
        .copied()
        .unwrap_or(0))
}

// =============================================================================
// Test 1: Insert goes inline despite pending deletions
// =============================================================================
//
// Write rows to file → delete one PK → insert a *new* PK.
// The new PK must be inlined (not fall through to a protected snapshot),
// and query results must be correct.

async fn test_insert_inlines_despite_pending_deletions_impl(
    fixture: TestFixture,
) -> TestResult<()> {
    let schema = simple_schema();
    let (table, ctx) = setup_table(
        &fixture,
        "pending_del",
        Arc::clone(&schema),
        vec!["id".into()],
    )
    .await?;

    // Initial insert: enough rows to go to file (exceeds inline limit).
    let n = DEFAULT_INLINE_MAX_ROWS + 1;
    let ids: Vec<i64> = (0..i64::try_from(n)?).collect();
    let vals: Vec<i64> = ids.iter().map(|i| i * 10).collect();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(vals)),
        ],
    )?;
    insert_batch(&table, batch).await?;

    // Verify initial state.
    assert_eq!(row_count(&ctx, "pending_del").await?, n);

    // Delete PK 0 → creates a pending deletion in the cache.
    delete_by_id(&table, 0).await?;

    // Insert a NEW PK (not conflicting) while pending deletions exist.
    // Previously this fell through to a protected snapshot; now it should inline.
    let new_id = i64::try_from(n)? + 100;
    let new_batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![new_id])),
            Arc::new(Int64Array::from(vec![9999])),
        ],
    )?;
    insert_batch(&table, new_batch).await?;

    // Query correctness: total rows = n - 1 (deleted PK 0) + 1 (new PK).
    assert_eq!(row_count(&ctx, "pending_del").await?, n);

    // The new PK is queryable with the correct value.
    let vals = query_value(
        &ctx,
        &format!("SELECT value FROM pending_del WHERE id = {new_id}"),
    )
    .await?;
    assert_eq!(vals, vec![9999]);

    // Deleted PK 0 is gone.
    let vals = query_value(&ctx, "SELECT value FROM pending_del WHERE id = 0").await?;
    assert!(vals.is_empty(), "deleted PK 0 should not be visible");

    Ok(())
}

test_with_backends!(test_insert_inlines_despite_pending_deletions_impl);

// =============================================================================
// Test 2: Upsert with file-backed conflict inlines + writes deletion vector
// =============================================================================
//
// Write PK to file → upsert same PK with new value.
// The replacement must be inlined, the old file-backed row hidden by a
// deletion vector, and queries return only the new value.

async fn test_upsert_file_conflict_inlines_impl(fixture: TestFixture) -> TestResult<()> {
    let schema = simple_schema();
    let (table, ctx) = setup_table(
        &fixture,
        "file_upsert",
        Arc::clone(&schema),
        vec!["id".into()],
    )
    .await?;

    // Initial insert: exceeds inline limit so data goes to file.
    let n = DEFAULT_INLINE_MAX_ROWS + 1;
    let ids: Vec<i64> = (0..i64::try_from(n)?).collect();
    let vals: Vec<i64> = ids.iter().map(|i| i * 10).collect();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(vals)),
        ],
    )?;
    insert_batch(&table, batch).await?;
    assert_eq!(row_count(&ctx, "file_upsert").await?, n);

    // Upsert PK 0 with a new value (1 row → fits inline).
    let upsert_batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![0])),
            Arc::new(Int64Array::from(vec![7777])),
        ],
    )?;
    insert_batch(&table, upsert_batch).await?;

    // Total row count unchanged (upsert replaces, not duplicates).
    assert_eq!(row_count(&ctx, "file_upsert").await?, n);

    // PK 0 returns the updated value.
    let vals = query_value(&ctx, "SELECT value FROM file_upsert WHERE id = 0").await?;
    assert_eq!(vals, vec![7777], "PK 0 should have the upserted value");

    Ok(())
}

test_with_backends!(test_upsert_file_conflict_inlines_impl);

// =============================================================================
// Test 3: Inlined upsert survives restart
// =============================================================================
//
// Same as test 2 but verifies correctness after dropping the provider and
// reopening from the catalog.  This exercises the pre-reserved deletion
// sequence ordering that keeps `filter_inlined_batch_for_deletions` from
// discarding the inline row.

async fn test_inlined_upsert_survives_restart_impl(fixture: TestFixture) -> TestResult<()> {
    let schema = utf8_pk_schema();
    let (table, _ctx) = setup_table(
        &fixture,
        "restart_tbl",
        Arc::clone(&schema),
        vec!["email".into()],
    )
    .await?;

    // Initial insert: large enough to go to file.
    let n = DEFAULT_INLINE_MAX_ROWS + 1;
    let mut emails: Vec<String> = (0..n).map(|i| format!("user{i}@test.com")).collect();
    emails[0] = "alice@test.com".to_string();
    let mut vals: Vec<i64> = (0..i64::try_from(n)?).collect();
    vals[0] = 100;
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(emails)),
            Arc::new(Int64Array::from(vals)),
        ],
    )?;
    insert_batch(&table, batch).await?;

    // Upsert alice with new value → inlined.
    let upsert = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["alice@test.com"])),
            Arc::new(Int64Array::from(vec![999])),
        ],
    )?;
    insert_batch(&table, upsert).await?;

    let connection_string = format!("sqlite://{}", fixture.db_path().to_string_lossy());

    // ---- Restart ----
    drop(table);

    let catalog2 = Arc::new(CayenneCatalog::new(&connection_string)?);
    catalog2.init().await?;
    let catalog_trait2: Arc<dyn MetadataCatalog> =
        Arc::clone(&catalog2) as Arc<dyn MetadataCatalog>;

    let ctx2 = SessionContext::new();
    let provider2 = cayenne::CayenneTableProviderBuilder::new(catalog_trait2, ctx2.runtime_env())
        .open("restart_tbl")
        .await?;
    let provider2 = Arc::new(provider2);
    ctx2.register_table(
        "restart_tbl",
        Arc::clone(&provider2) as Arc<dyn TableProvider>,
    )?;

    // Alice must still be visible with the upserted value.
    let vals = query_value(
        &ctx2,
        "SELECT items_bought FROM restart_tbl WHERE email = 'alice@test.com'",
    )
    .await?;
    assert_eq!(
        vals,
        vec![999],
        "alice should have the upserted value after restart"
    );

    // Total row count is preserved.
    assert_eq!(row_count(&ctx2, "restart_tbl").await?, n);

    Ok(())
}

test_with_backends!(test_inlined_upsert_survives_restart_impl);

// =============================================================================
// Test 4: Interleaved delete + insert + upsert (TPC-C pattern)
// =============================================================================
//
// Simulates TPC-C new_order: delete oldest PK, insert newest PK, upsert an
// existing PK — all while pending deletions exist.

async fn test_interleaved_delete_insert_upsert_impl(fixture: TestFixture) -> TestResult<()> {
    let schema = simple_schema();
    let (table, ctx) =
        setup_table(&fixture, "tpcc", Arc::clone(&schema), vec!["id".into()]).await?;

    // Initial insert to file: ids 1..=N.
    let n = DEFAULT_INLINE_MAX_ROWS + 1;
    let ids: Vec<i64> = (1..=i64::try_from(n)?).collect();
    let vals: Vec<i64> = ids.iter().map(|i| i * 10).collect();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(vals)),
        ],
    )?;
    insert_batch(&table, batch).await?;
    assert_eq!(row_count(&ctx, "tpcc").await?, n);

    // Round 1: delete id=1, insert new id=N+1, upsert id=2.
    let high = i64::try_from(n)? + 1;
    delete_by_id(&table, 1).await?;

    let insert_batch_1 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![high])),
            Arc::new(Int64Array::from(vec![high * 10])),
        ],
    )?;
    insert_batch(&table, insert_batch_1).await?;

    let upsert_batch_1 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![2])),
            Arc::new(Int64Array::from(vec![2222])),
        ],
    )?;
    insert_batch(&table, upsert_batch_1).await?;

    // Row count: n - 1 (del id=1) + 1 (new id=N+1) = n. Upsert replaces, no change.
    assert_eq!(row_count(&ctx, "tpcc").await?, n);

    // Verify values.
    let v = query_value(&ctx, "SELECT value FROM tpcc WHERE id = 1").await?;
    assert!(v.is_empty(), "id=1 should be deleted");

    let v = query_value(&ctx, &format!("SELECT value FROM tpcc WHERE id = {high}")).await?;
    assert_eq!(v, vec![high * 10], "newly inserted id should be visible");

    let v = query_value(&ctx, "SELECT value FROM tpcc WHERE id = 2").await?;
    assert_eq!(v, vec![2222], "upserted id=2 should have new value");

    // Round 2: another cycle while prior pending deletions exist.
    let high2 = high + 1;
    delete_by_id(&table, 3).await?;

    let insert_batch_2 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![high2])),
            Arc::new(Int64Array::from(vec![high2 * 10])),
        ],
    )?;
    insert_batch(&table, insert_batch_2).await?;

    // n - 1 (del id=3) + 1 (new id=high2) = n.
    assert_eq!(row_count(&ctx, "tpcc").await?, n);

    Ok(())
}

test_with_backends!(test_interleaved_delete_insert_upsert_impl);

// =============================================================================
// Test 5: Composite PK (RowConverterBased) upsert with file conflict inlines
// =============================================================================
//
// Same as test 2 but with a composite primary key to exercise the
// `RowConverterBased` deletion strategy branch.

async fn test_composite_pk_upsert_file_conflict_inlines_impl(
    fixture: TestFixture,
) -> TestResult<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("region", DataType::Utf8, false),
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let (table, ctx) = setup_table(
        &fixture,
        "composite_pk",
        Arc::clone(&schema),
        vec!["region".into(), "id".into()],
    )
    .await?;

    // Initial insert to file.
    let n = DEFAULT_INLINE_MAX_ROWS + 1;
    let regions: Vec<String> = (0..n)
        .map(|i| if i % 2 == 0 { "east" } else { "west" }.into())
        .collect();
    let ids: Vec<i64> = (0..i64::try_from(n)?).collect();
    let vals: Vec<i64> = ids.iter().map(|i| i * 10).collect();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(regions)),
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(vals)),
        ],
    )?;
    insert_batch(&table, batch).await?;
    assert_eq!(row_count(&ctx, "composite_pk").await?, n);

    // Upsert (east, 0) → inlined.
    let upsert = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["east"])),
            Arc::new(Int64Array::from(vec![0])),
            Arc::new(Int64Array::from(vec![5555])),
        ],
    )?;
    insert_batch(&table, upsert).await?;

    assert_eq!(row_count(&ctx, "composite_pk").await?, n);

    let v = query_value(
        &ctx,
        "SELECT value FROM composite_pk WHERE region = 'east' AND id = 0",
    )
    .await?;
    assert_eq!(v, vec![5555], "composite PK upsert should return new value");

    Ok(())
}

test_with_backends!(test_composite_pk_upsert_file_conflict_inlines_impl);

// =============================================================================
// Test 6: Checkpoint after inlined upsert materialises correctly
// =============================================================================
//
// After an inlined upsert with file-backed conflict, trigger a checkpoint and
// verify the data is correct in the flushed state.

async fn test_checkpoint_after_inlined_upsert_impl(fixture: TestFixture) -> TestResult<()> {
    let schema = simple_schema();
    let (table, ctx) =
        setup_table(&fixture, "ckpt", Arc::clone(&schema), vec!["id".into()]).await?;

    // Initial insert to file.
    let n = DEFAULT_INLINE_MAX_ROWS + 1;
    let ids: Vec<i64> = (0..i64::try_from(n)?).collect();
    let vals: Vec<i64> = ids.iter().map(|i| i * 10).collect();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(vals)),
        ],
    )?;
    insert_batch(&table, batch).await?;

    // Upsert PK 0 → inlined.
    let upsert = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![0])),
            Arc::new(Int64Array::from(vec![4242])),
        ],
    )?;
    insert_batch(&table, upsert).await?;

    // Checkpoint: flush inline data to a Vortex file.
    table.checkpoint_inlined_data().await?;

    // Data correctness survives checkpoint.
    assert_eq!(row_count(&ctx, "ckpt").await?, n);
    let v = query_value(&ctx, "SELECT value FROM ckpt WHERE id = 0").await?;
    assert_eq!(v, vec![4242], "upserted value should survive checkpoint");

    Ok(())
}

test_with_backends!(test_checkpoint_after_inlined_upsert_impl);

// =============================================================================
// Test 7: Two successive inline upserts on same PK
// =============================================================================
//
// Insert to file → upsert PK (inlined) → upsert same PK again (inlined).
// The second upsert must see the first inline row as a conflict source,
// rewrite the inline entry, and only the latest value should be visible.
// Also verifies correctness after restart.

async fn test_double_inline_upsert_same_pk_impl(fixture: TestFixture) -> TestResult<()> {
    let schema = simple_schema();
    let (table, ctx) = setup_table(
        &fixture,
        "dbl_upsert",
        Arc::clone(&schema),
        vec!["id".into()],
    )
    .await?;

    // Initial insert to file.
    let n = DEFAULT_INLINE_MAX_ROWS + 1;
    let ids: Vec<i64> = (0..i64::try_from(n)?).collect();
    let vals: Vec<i64> = ids.iter().map(|i| i * 10).collect();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(vals)),
        ],
    )?;
    insert_batch(&table, batch).await?;
    assert_eq!(row_count(&ctx, "dbl_upsert").await?, n);

    // First inline upsert: PK 0 → value 1111.
    let upsert1 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![0])),
            Arc::new(Int64Array::from(vec![1111])),
        ],
    )?;
    insert_batch(&table, upsert1).await?;

    // Second inline upsert: PK 0 → value 2222 (conflict is now the inline row).
    let upsert2 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![0])),
            Arc::new(Int64Array::from(vec![2222])),
        ],
    )?;
    insert_batch(&table, upsert2).await?;

    // Row count unchanged — no duplicates.
    assert_eq!(row_count(&ctx, "dbl_upsert").await?, n);

    // Only the latest value is visible.
    let v = query_value(&ctx, "SELECT value FROM dbl_upsert WHERE id = 0").await?;
    assert_eq!(v, vec![2222], "second upsert value should win");

    // ---- Restart ----
    let connection_string = format!("sqlite://{}", fixture.db_path().to_string_lossy());
    drop(table);
    drop(ctx);

    let catalog2 = Arc::new(CayenneCatalog::new(&connection_string)?);
    catalog2.init().await?;
    let catalog_trait2: Arc<dyn MetadataCatalog> =
        Arc::clone(&catalog2) as Arc<dyn MetadataCatalog>;

    let ctx2 = SessionContext::new();
    let provider2 = cayenne::CayenneTableProviderBuilder::new(catalog_trait2, ctx2.runtime_env())
        .open("dbl_upsert")
        .await?;
    let provider2 = Arc::new(provider2);
    ctx2.register_table(
        "dbl_upsert",
        Arc::clone(&provider2) as Arc<dyn TableProvider>,
    )?;

    assert_eq!(row_count(&ctx2, "dbl_upsert").await?, n);
    let v = query_value(&ctx2, "SELECT value FROM dbl_upsert WHERE id = 0").await?;
    assert_eq!(v, vec![2222], "latest value should survive restart");

    Ok(())
}

test_with_backends!(test_double_inline_upsert_same_pk_impl);

// =============================================================================
// Test 8: Delete of an inlined PK
// =============================================================================
//
// Insert to file → upsert PK (inlined) → delete that same PK.
// The delete must remove both the file-backed deletion vector AND the inline
// row, resulting in 0 rows for that PK.

async fn test_delete_of_inlined_pk_impl(fixture: TestFixture) -> TestResult<()> {
    let schema = simple_schema();
    let (table, ctx) = setup_table(
        &fixture,
        "del_inline",
        Arc::clone(&schema),
        vec!["id".into()],
    )
    .await?;

    // Initial insert to file.
    let n = DEFAULT_INLINE_MAX_ROWS + 1;
    let ids: Vec<i64> = (0..i64::try_from(n)?).collect();
    let vals: Vec<i64> = ids.iter().map(|i| i * 10).collect();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(vals)),
        ],
    )?;
    insert_batch(&table, batch).await?;
    assert_eq!(row_count(&ctx, "del_inline").await?, n);

    // Upsert PK 0 → inlined with new value.
    let upsert = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![0])),
            Arc::new(Int64Array::from(vec![9999])),
        ],
    )?;
    insert_batch(&table, upsert).await?;

    // Verify upsert worked.
    let v = query_value(&ctx, "SELECT value FROM del_inline WHERE id = 0").await?;
    assert_eq!(v, vec![9999]);

    // Delete PK 0 — must remove the inlined row too.
    delete_by_id(&table, 0).await?;

    // PK 0 is gone entirely.
    let v = query_value(&ctx, "SELECT value FROM del_inline WHERE id = 0").await?;
    assert!(v.is_empty(), "deleted inlined PK should not be visible");

    // Total rows decreased by 1.
    assert_eq!(row_count(&ctx, "del_inline").await?, n - 1);

    Ok(())
}

test_with_backends!(test_delete_of_inlined_pk_impl);

// =============================================================================
// Test 9: Mixed inline + file conflicts in one upsert batch
// =============================================================================
//
// Insert to file → upsert PK A (inlined) → upsert batch containing both PK A
// (conflict with inline) and PK B (conflict with file).  Both conflict sources
// must be resolved in a single `try_inline_batches_with_inlined_deletions` call.

async fn test_mixed_inline_and_file_conflicts_impl(fixture: TestFixture) -> TestResult<()> {
    let schema = simple_schema();
    let (table, ctx) = setup_table(
        &fixture,
        "mixed_conflict",
        Arc::clone(&schema),
        vec!["id".into()],
    )
    .await?;

    // Initial insert to file: ids 0..n.
    let n = DEFAULT_INLINE_MAX_ROWS + 1;
    let ids: Vec<i64> = (0..i64::try_from(n)?).collect();
    let vals: Vec<i64> = ids.iter().map(|i| i * 10).collect();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(vals)),
        ],
    )?;
    insert_batch(&table, batch).await?;
    assert_eq!(row_count(&ctx, "mixed_conflict").await?, n);

    // Upsert PK 0 → inlined (conflict with file, now PK 0 lives inline).
    let upsert1 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![0])),
            Arc::new(Int64Array::from(vec![1000])),
        ],
    )?;
    insert_batch(&table, upsert1).await?;

    // Now upsert both PK 0 (inline conflict) and PK 1 (file conflict) together.
    let upsert_mixed = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![0, 1])),
            Arc::new(Int64Array::from(vec![5555, 6666])),
        ],
    )?;
    insert_batch(&table, upsert_mixed).await?;

    // Row count unchanged — two replacements, no new PKs.
    assert_eq!(row_count(&ctx, "mixed_conflict").await?, n);

    // Both PKs have latest values.
    let v0 = query_value(&ctx, "SELECT value FROM mixed_conflict WHERE id = 0").await?;
    assert_eq!(v0, vec![5555], "PK 0 should have mixed-upsert value");

    let v1 = query_value(&ctx, "SELECT value FROM mixed_conflict WHERE id = 1").await?;
    assert_eq!(v1, vec![6666], "PK 1 should have mixed-upsert value");

    // Other PKs untouched.
    let v2 = query_value(&ctx, "SELECT value FROM mixed_conflict WHERE id = 2").await?;
    assert_eq!(v2, vec![20], "PK 2 should be unchanged");

    Ok(())
}

test_with_backends!(test_mixed_inline_and_file_conflicts_impl);

// =============================================================================
// Test: Upsert after an inline checkpoint tombstones the flushed prior version
// =============================================================================
//
// Insert a PK small enough to stay inline → checkpoint (flush the memtable to a
// file) → upsert the same PK. The checkpoint rewrites the keyset entry from
// `Inlined` to `FileUnlocated`, so the upsert's supersede delete routes to the
// file delete list and tombstones the now-on-disk row — one live row per key,
// no duplicate PK.

async fn test_inline_checkpoint_then_upsert_no_duplicate_pk_int64_impl(
    fixture: TestFixture,
) -> TestResult<()> {
    let schema = simple_schema();
    let (table, ctx) = setup_table(
        &fixture,
        "inline_ckpt_dup_int64",
        Arc::clone(&schema),
        vec!["id".into()],
    )
    .await?;

    // 1. Insert id=1 → absorbed into the inline memtable; keyset entry = Inlined.
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![10])),
        ],
    )?;
    insert_batch(&table, batch).await?;

    // 2. Checkpoint: flush the inline memtable to a file; the keyset entry for
    //    id=1 is rewritten from Inlined to FileUnlocated.
    table.checkpoint_inlined_data().await?;

    // 3. Upsert id=1 → routed as a file delete and tombstones the flushed row.
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![100])),
        ],
    )?;
    insert_batch(&table, batch).await?;

    assert_eq!(
        row_count(&ctx, "inline_ckpt_dup_int64").await?,
        1,
        "an upsert after an inline checkpoint must tombstone the flushed prior version (no duplicate PK)"
    );
    let vals = query_value(&ctx, "SELECT value FROM inline_ckpt_dup_int64 WHERE id = 1").await?;
    assert_eq!(vals, vec![100], "upsert must keep only the latest value");

    Ok(())
}

test_with_backends!(test_inline_checkpoint_then_upsert_no_duplicate_pk_int64_impl);

// Composite-PK counterpart (`RowConverterBased` strategy — the shape used by the
// CDC tables exercised in benchmarks). Same guarantee as the int64 variant: the
// checkpoint rewrites the flushed key's entry from `Inlined` to `FileUnlocated`,
// so the next upsert's key-based delete tombstones the now-on-disk row.

async fn test_inline_checkpoint_then_upsert_no_duplicate_pk_composite_impl(
    fixture: TestFixture,
) -> TestResult<()> {
    let schema = composite_pk_schema();
    let (table, ctx) = setup_table(
        &fixture,
        "inline_ckpt_dup_composite",
        Arc::clone(&schema),
        vec!["k1".into(), "k2".into()],
    )
    .await?;

    // 1. Insert composite key (1,1) → inlined; keyset entry = Inlined.
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![10])),
        ],
    )?;
    insert_batch(&table, batch).await?;

    // 2. Checkpoint: flush to a file; keyset entry rewritten Inlined → FileUnlocated.
    table.checkpoint_inlined_data().await?;

    // 3. Upsert (1,1) → key-based file delete tombstones the flushed row.
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![100])),
        ],
    )?;
    insert_batch(&table, batch).await?;

    assert_eq!(
        row_count(&ctx, "inline_ckpt_dup_composite").await?,
        1,
        "a composite-PK upsert after an inline checkpoint must tombstone the flushed prior version (no duplicate PK)"
    );
    let vals = query_value(
        &ctx,
        "SELECT value FROM inline_ckpt_dup_composite WHERE k1 = 1 AND k2 = 1",
    )
    .await?;
    assert_eq!(vals, vec![100], "upsert must keep only the latest value");

    Ok(())
}

test_with_backends!(test_inline_checkpoint_then_upsert_no_duplicate_pk_composite_impl);
