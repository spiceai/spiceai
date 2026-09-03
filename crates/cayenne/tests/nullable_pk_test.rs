/*
Copyright 2026 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required in applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

//! Tests for nullable primary-key support.
//!
//! A primary-key column declared nullable in the table schema makes a `NULL`
//! primary-key value a legal, coalescing key (all nulls encode to the same
//! `RowConverter` null-sentinel key) rather than a validation error. These
//! tests validate:
//!
//! 1. **Nullable single-Int64 PK accepts nulls**: a nullable Int64 PK routes
//!    through the `RowConverter` byte-key strategy (not the raw-Int64 fast
//!    path, which has no null representation), so `NULL` PK values insert.
//! 2. **Nulls coalesce to one key**: upserting two rows with a `NULL` PK
//!    supersedes the first with the second — only one null-keyed row remains.
//! 3. **NOT NULL PK still rejects nulls**: a non-nullable PK column keeps
//!    `NULL` a validation error (regression guard).
//! 4. **Delete by `IS NULL`**: a `DELETE … WHERE pk IS NULL` removes the
//!    null-keyed row via the byte-key deletion vector.

#![allow(clippy::expect_used)]

mod common;

use arrow::array::{Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use cayenne::{CayenneTableProvider, MetadataCatalog, metadata::CreateTableOptions};
use common::TestFixture;
use datafusion::datasource::TableProvider;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::{Expr, col};
use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};
use std::sync::Arc;

type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

// =============================================================================
// Schema + setup helpers
// =============================================================================

/// A single-Int64 PK declared **nullable** — the feature under test.
fn nullable_int64_pk_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, false),
    ]))
}

/// A single-Int64 PK declared **NOT NULL** — the regression guard.
fn non_nullable_int64_pk_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]))
}

async fn setup_upsert_table(
    fixture: &TestFixture,
    table_name: &str,
    schema: Arc<Schema>,
) -> TestResult<(Arc<CayenneTableProvider>, SessionContext)> {
    let table_options = CreateTableOptions {
        table_name: table_name.to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["id".to_string()],
        on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
            "id".to_string(),
        ]))),
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

async fn insert_batch(table: &Arc<CayenneTableProvider>, batch: RecordBatch) -> TestResult<u64> {
    common::insert_batch(table.as_ref(), batch)
        .await
        .map_err(Into::into)
}

async fn delete_records(table: &Arc<CayenneTableProvider>, filter: Expr) -> TestResult<u64> {
    let ctx = SessionContext::new();
    let plan = table.delete_from(&ctx.state(), vec![filter]).await?;
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

/// Collect `(id, name)` pairs, preserving the null id as `None`.
async fn get_rows(
    ctx: &SessionContext,
    table_name: &str,
) -> TestResult<Vec<(Option<i64>, String)>> {
    let df = ctx
        .sql(&format!("SELECT id, name FROM {table_name} ORDER BY id NULLS FIRST"))
        .await?;
    let results = df.collect().await?;
    let mut rows = Vec::new();
    for batch in &results {
        let id_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id column is Int64");
        let name_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("name column is Utf8");
        for i in 0..batch.num_rows() {
            let id = if id_col.is_null(i) {
                None
            } else {
                Some(id_col.value(i))
            };
            rows.push((id, name_col.value(i).to_string()));
        }
    }
    Ok(rows)
}

// =============================================================================
// Tests
// =============================================================================

async fn nullable_int64_pk_accepts_nulls_and_coalesces_impl(
    fixture: TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let (table, ctx) = setup_upsert_table(&fixture, "nullable_pk", nullable_int64_pk_schema())
        .await?;

    // (1) a real key, (2) a null key, (3) another null key — the two nulls
    // coalesce to the same key, so the upsert supersedes row 2 with row 3.
    let batch = RecordBatch::try_new(
        nullable_int64_pk_schema(),
        vec![
            Arc::new(Int64Array::from(vec![Some(1), None, None])),
            Arc::new(StringArray::from(vec!["a", "c", "d"])),
        ],
    )
    .expect("build batch");

    // A nullable PK accepts NULL values (the raw-Int64 fast path would have
    // rejected these; the RowConverter path encodes the null sentinel).
    insert_batch(&table, batch).await?;

    let rows = get_rows(&ctx, "nullable_pk").await?;
    // Row 1 survives; the two null-keyed rows coalesced to a single row "d".
    assert_eq!(
        rows,
        vec![(None, "d".to_string()), (Some(1), "a".to_string())],
        "null PKs should coalesce to one key; got {rows:?}"
    );

    Ok(())
}

async fn nullable_int64_pk_delete_by_null_filter_impl(
    fixture: TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let (table, ctx) = setup_upsert_table(&fixture, "nullable_pk_del", nullable_int64_pk_schema())
        .await?;

    let batch = RecordBatch::try_new(
        nullable_int64_pk_schema(),
        vec![
            Arc::new(Int64Array::from(vec![Some(1), None])),
            Arc::new(StringArray::from(vec!["a", "c"])),
        ],
    )
    .expect("build batch");
    insert_batch(&table, batch).await?;

    // Delete the null-keyed row via a `pk IS NULL` filter — the byte-key
    // deletion vector must match the null-sentinel key.
    let deleted = delete_records(&table, col("id").is_null()).await?;
    assert_eq!(deleted, 1, "expected to delete the single null-keyed row");

    let rows = get_rows(&ctx, "nullable_pk_del").await?;
    assert_eq!(
        rows,
        vec![(Some(1), "a".to_string())],
        "null-keyed row should be deleted; got {rows:?}"
    );

    Ok(())
}

async fn non_nullable_int64_pk_still_rejects_nulls_impl(
    fixture: TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let (table, _ctx) =
        setup_upsert_table(&fixture, "nonnull_pk", non_nullable_int64_pk_schema()).await?;

    // A NOT NULL PK column keeps NULL a validation error. The *batch* declares
    // `id` nullable (so Arrow accepts the null), while the *table* declares it
    // NOT NULL — cayenne's write-path backstop must reject the null PK.
    let batch = RecordBatch::try_new(
        nullable_int64_pk_schema(),
        vec![
            Arc::new(Int64Array::from(vec![Some(1), None])),
            Arc::new(StringArray::from(vec![Some("a"), Some("bad")])),
        ],
    )
    .expect("build batch");

    let result = insert_batch(&table, batch).await;
    assert!(
        result.is_err(),
        "a NULL in a NOT NULL PK column must be rejected; got Ok({result:?})"
    );

    Ok(())
}

test_with_backends!(nullable_int64_pk_accepts_nulls_and_coalesces_impl);
test_with_backends!(nullable_int64_pk_delete_by_null_filter_impl);
test_with_backends!(non_nullable_int64_pk_still_rejects_nulls_impl);
