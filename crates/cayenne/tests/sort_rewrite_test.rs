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

//! Integration tests for `CayenneTableProvider::sort_and_rewrite_data`.
//!
//! These tests exercise the full sort-and-rewrite pipeline:
//! 1. Read all data from the current listing table
//! 2. Sort via `DataFusion`'s `SortExec`
//! 3. Write sorted data to a NEW snapshot directory
//! 4. Atomically swap the catalog to the new snapshot
//! 5. Verify data correctness and ordering after the rewrite

mod common;

use std::sync::Arc;

use arrow::array::{Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use cayenne::metadata::{CreateTableOptions, VortexConfig};
use cayenne::provider::CayenneContext;
use cayenne::{CayenneTableProvider, CayenneTableProviderBuilder, MetadataCatalog};

use datafusion::datasource::TableProvider;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::prelude::SessionContext;

// ============================================================================
// Test Helpers
// ============================================================================

/// Create a `CayenneTableProvider` with sort columns and a custom `CayenneContext`.
///
/// Returns the provider, the catalog (for re-opening), and the temp dir
/// (must be kept alive for the test duration).
async fn create_sorted_table(
    fixture: &common::TestFixture,
    table_name: &str,
    schema: Arc<Schema>,
    sort_columns: Vec<String>,
    runtime_env: Arc<RuntimeEnv>,
) -> Arc<CayenneTableProvider> {
    let vortex_config = VortexConfig {
        sort_columns,
        ..VortexConfig::default()
    };

    let context = CayenneContext::new(&vortex_config, Arc::clone(&runtime_env));

    let options = CreateTableOptions {
        table_name: table_name.to_string(),
        schema,
        primary_key: vec![],
        on_conflict: None,
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config,
    };

    let catalog_arc = Arc::clone(&fixture.catalog);
    let catalog_arc: Arc<dyn MetadataCatalog> = catalog_arc;
    Arc::new(
        CayenneTableProviderBuilder::new(catalog_arc, runtime_env)
            .with_context(context)
            .create(options)
            .await
            .expect("table should be created"),
    )
}

/// Read all data from a table via SQL, returning collected `RecordBatch`es.
async fn query_all(
    ctx: &SessionContext,
    provider: &Arc<CayenneTableProvider>,
    table_name: &str,
) -> Vec<RecordBatch> {
    // Table may already be registered from a previous call; ignore "already exists" errors.
    let _ = ctx.register_table(table_name, Arc::clone(provider) as Arc<dyn TableProvider>);
    ctx.sql(&format!("SELECT * FROM {table_name}"))
        .await
        .expect("query should succeed")
        .collect()
        .await
        .expect("collect should succeed")
}

/// Read all data ordered by a column.
async fn query_ordered(
    ctx: &SessionContext,
    provider: &Arc<CayenneTableProvider>,
    table_name: &str,
    order_col: &str,
) -> Vec<RecordBatch> {
    // Table may already be registered from a previous call; ignore "already exists" errors.
    let _ = ctx.register_table(table_name, Arc::clone(provider) as Arc<dyn TableProvider>);
    ctx.sql(&format!("SELECT * FROM {table_name} ORDER BY {order_col}"))
        .await
        .expect("query should succeed")
        .collect()
        .await
        .expect("collect should succeed")
}

/// SQL insert helper.
async fn sql_insert(provider: &Arc<CayenneTableProvider>, table_name: &str, sql_values: &str) {
    let ctx = SessionContext::new();
    ctx.register_table(table_name, Arc::clone(provider) as Arc<dyn TableProvider>)
        .expect("table should be registered");
    ctx.sql(&format!("INSERT INTO {table_name} VALUES {sql_values}"))
        .await
        .expect("insert should succeed")
        .collect()
        .await
        .expect("collect should succeed");
}

/// Extract all i64 values from a named column across batches (in batch order).
fn collect_i64_column(batches: &[RecordBatch], col_name: &str) -> Vec<i64> {
    let mut values = Vec::new();
    for batch in batches {
        let col_idx = batch
            .schema()
            .index_of(col_name)
            .unwrap_or_else(|_| panic!("column '{col_name}' should exist"));
        let array = batch
            .column(col_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap_or_else(|| panic!("column '{col_name}' should be Int64"));
        for i in 0..array.len() {
            if array.is_null(i) {
                // Use i64::MIN as sentinel for NULL in tests
                values.push(i64::MIN);
            } else {
                values.push(array.value(i));
            }
        }
    }
    values
}

/// Extract all String values from a named column across batches (in batch order).
fn collect_string_column(batches: &[RecordBatch], col_name: &str) -> Vec<Option<String>> {
    let mut values = Vec::new();
    for batch in batches {
        let col_idx = batch
            .schema()
            .index_of(col_name)
            .unwrap_or_else(|_| panic!("column '{col_name}' should exist"));
        let array = batch
            .column(col_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap_or_else(|| panic!("column '{col_name}' should be Utf8"));
        for i in 0..array.len() {
            if array.is_null(i) {
                values.push(None);
            } else {
                values.push(Some(array.value(i).to_string()));
            }
        }
    }
    values
}

/// Count total rows across batches.
fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}

// ============================================================================
// Tests
// ============================================================================

test_with_backends!(test_sort_rewrite_basic_impl);

/// Basic test: insert unsorted data, sort-rewrite, verify sorted output.
async fn test_sort_rewrite_basic_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let ctx = SessionContext::new();
    let table = create_sorted_table(
        &fixture,
        "sort_basic",
        Arc::clone(&schema),
        vec!["id".to_string()],
        ctx.runtime_env(),
    )
    .await;

    // Insert data in deliberately unsorted order
    sql_insert(
        &table,
        "sort_basic",
        "(50, 500), (10, 100), (30, 300), (20, 200), (40, 400)",
    )
    .await;

    // Sort and rewrite
    table
        .sort_and_rewrite_data(128 * 1024 * 1024)
        .await
        .expect("sort_and_rewrite_data should succeed");

    // Verify data is sorted (query without ORDER BY to check physical sort order)
    let results = query_all(&ctx, &table, "sort_basic").await;
    assert_eq!(total_rows(&results), 5, "should have 5 rows");

    let ids = collect_i64_column(&results, "id");
    let vals = collect_i64_column(&results, "value");
    assert_eq!(ids, vec![10, 20, 30, 40, 50], "ids should be sorted");
    assert_eq!(
        vals,
        vec![100, 200, 300, 400, 500],
        "values should follow their ids"
    );

    Ok(())
}

test_with_backends!(test_sort_rewrite_multiple_inserts_impl);

/// Multiple inserts then sort: data from separate append operations is merged and sorted.
async fn test_sort_rewrite_multiple_inserts_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("label", DataType::Utf8, false),
    ]));

    let ctx = SessionContext::new();
    let table = create_sorted_table(
        &fixture,
        "sort_multi",
        Arc::clone(&schema),
        vec!["ts".to_string()],
        ctx.runtime_env(),
    )
    .await;

    // Insert 3 separate batches in reverse order
    sql_insert(&table, "sort_multi", "(90, 'batch1_a'), (70, 'batch1_b')").await;
    sql_insert(&table, "sort_multi", "(50, 'batch2_a'), (30, 'batch2_b')").await;
    sql_insert(&table, "sort_multi", "(10, 'batch3_a'), (80, 'batch3_b')").await;

    // Sort and rewrite
    table
        .sort_and_rewrite_data(128 * 1024 * 1024)
        .await
        .expect("sort_and_rewrite_data should succeed");

    let results = query_all(&ctx, &table, "sort_multi").await;
    assert_eq!(total_rows(&results), 6, "should have 6 rows");

    let timestamps = collect_i64_column(&results, "ts");
    assert_eq!(
        timestamps,
        vec![10, 30, 50, 70, 80, 90],
        "all data from multiple inserts should be merged and sorted"
    );

    // Verify label correspondence
    let labels = collect_string_column(&results, "label");
    assert_eq!(
        labels,
        vec![
            Some("batch3_a".to_string()),
            Some("batch2_b".to_string()),
            Some("batch2_a".to_string()),
            Some("batch1_b".to_string()),
            Some("batch3_b".to_string()),
            Some("batch1_a".to_string()),
        ],
        "labels should follow their timestamps"
    );

    Ok(())
}

test_with_backends!(test_sort_rewrite_with_nulls_impl);

/// Sort with nullable columns: NULLs should sort to the end (default ASC NULLS LAST).
async fn test_sort_rewrite_with_nulls_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),  // nullable
        Field::new("name", DataType::Utf8, true), // nullable
    ]));

    let ctx = SessionContext::new();
    let table = create_sorted_table(
        &fixture,
        "sort_nulls",
        Arc::clone(&schema),
        vec!["id".to_string()],
        ctx.runtime_env(),
    )
    .await;

    // Insert data with NULLs
    sql_insert(
        &table,
        "sort_nulls",
        "(3, 'three'), (NULL, 'no_id'), (1, NULL), (NULL, 'another_no_id'), (2, 'two')",
    )
    .await;

    table
        .sort_and_rewrite_data(128 * 1024 * 1024)
        .await
        .expect("sort_and_rewrite_data should succeed");

    let results = query_all(&ctx, &table, "sort_nulls").await;
    assert_eq!(total_rows(&results), 5, "should have 5 rows");

    let ids = collect_i64_column(&results, "id");
    // Non-null values should be sorted ascending, NULLs at the end
    // i64::MIN is our sentinel for NULL
    assert_eq!(ids[0], 1, "first non-null id should be 1");
    assert_eq!(ids[1], 2, "second non-null id should be 2");
    assert_eq!(ids[2], 3, "third non-null id should be 3");
    // The last two should be NULLs (i64::MIN sentinel)
    assert_eq!(ids[3], i64::MIN, "fourth should be NULL");
    assert_eq!(ids[4], i64::MIN, "fifth should be NULL");

    Ok(())
}

test_with_backends!(test_sort_rewrite_idempotent_impl);

/// Sort idempotency: sorting already-sorted data should produce identical results.
async fn test_sort_rewrite_idempotent_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("data", DataType::Utf8, false),
    ]));

    let ctx = SessionContext::new();
    let table = create_sorted_table(
        &fixture,
        "sort_idem",
        Arc::clone(&schema),
        vec!["id".to_string()],
        ctx.runtime_env(),
    )
    .await;

    sql_insert(
        &table,
        "sort_idem",
        "(5, 'e'), (3, 'c'), (1, 'a'), (4, 'd'), (2, 'b')",
    )
    .await;

    // First sort
    table
        .sort_and_rewrite_data(128 * 1024 * 1024)
        .await
        .expect("first sort should succeed");

    let after_first = query_all(&ctx, &table, "sort_idem").await;
    let ids_first = collect_i64_column(&after_first, "id");
    let data_first = collect_string_column(&after_first, "data");

    // Second sort (already sorted)
    table
        .sort_and_rewrite_data(128 * 1024 * 1024)
        .await
        .expect("second sort should succeed");

    let after_second = query_all(&ctx, &table, "sort_idem").await;
    let ids_second = collect_i64_column(&after_second, "id");
    let data_second = collect_string_column(&after_second, "data");

    assert_eq!(
        ids_first, ids_second,
        "ids should be identical after re-sort"
    );
    assert_eq!(
        data_first, data_second,
        "data should be identical after re-sort"
    );
    assert_eq!(ids_first, vec![1, 2, 3, 4, 5]);

    Ok(())
}

test_with_backends!(test_sort_rewrite_empty_table_impl);

/// Sorting an empty table is a safe no-op.
async fn test_sort_rewrite_empty_table_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));

    let ctx = SessionContext::new();
    let table = create_sorted_table(
        &fixture,
        "sort_empty",
        Arc::clone(&schema),
        vec!["x".to_string()],
        ctx.runtime_env(),
    )
    .await;

    table
        .sort_and_rewrite_data(128 * 1024 * 1024)
        .await
        .expect("sort on empty table should succeed");

    let results = query_all(&ctx, &table, "sort_empty").await;
    assert_eq!(total_rows(&results), 0, "empty table should stay empty");

    Ok(())
}

test_with_backends!(test_sort_rewrite_snapshot_changes_impl);

/// Verify that sort-rewrite creates a new snapshot (the snapshot ID changes).
async fn test_sort_rewrite_snapshot_changes_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

    let ctx = SessionContext::new();
    let table = create_sorted_table(
        &fixture,
        "sort_snap",
        Arc::clone(&schema),
        vec!["id".to_string()],
        ctx.runtime_env(),
    )
    .await;

    sql_insert(&table, "sort_snap", "(3), (1), (2)").await;

    // Capture snapshot ID before sort
    let metadata_before = table.metadata().clone();
    let snapshot_before = metadata_before.current_snapshot_id.clone();

    table
        .sort_and_rewrite_data(128 * 1024 * 1024)
        .await
        .expect("sort should succeed");

    // The snapshot should have changed (new UUID)
    let catalog = table.catalog();
    let metadata_after = catalog
        .get_table("sort_snap")
        .await
        .expect("table should exist in catalog");
    assert_ne!(
        snapshot_before, metadata_after.current_snapshot_id,
        "sort-rewrite should create a new snapshot"
    );

    // Data should still be correct in the new snapshot
    let results = query_all(&ctx, &table, "sort_snap").await;
    let ids = collect_i64_column(&results, "id");
    assert_eq!(ids, vec![1, 2, 3]);

    Ok(())
}

test_with_backends!(test_sort_rewrite_then_insert_impl);

/// Sort-rewrite followed by more inserts: new data should be appendable after sort.
async fn test_sort_rewrite_then_insert_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("val", DataType::Utf8, false),
    ]));

    let ctx = SessionContext::new();
    let table = create_sorted_table(
        &fixture,
        "sort_then_ins",
        Arc::clone(&schema),
        vec!["id".to_string()],
        ctx.runtime_env(),
    )
    .await;

    // Initial unsorted data
    sql_insert(&table, "sort_then_ins", "(30, 'c'), (10, 'a'), (20, 'b')").await;

    // Sort
    table
        .sort_and_rewrite_data(128 * 1024 * 1024)
        .await
        .expect("sort should succeed");

    // Insert more data after sort
    sql_insert(&table, "sort_then_ins", "(5, 'pre'), (25, 'mid')").await;

    // Query ordered to verify all data is present
    let results = query_ordered(&ctx, &table, "sort_then_ins", "id").await;
    assert_eq!(total_rows(&results), 5, "should have 5 rows total");

    let ids = collect_i64_column(&results, "id");
    assert_eq!(ids, vec![5, 10, 20, 25, 30]);

    let vals = collect_string_column(&results, "val");
    assert_eq!(
        vals,
        vec![
            Some("pre".to_string()),
            Some("a".to_string()),
            Some("b".to_string()),
            Some("mid".to_string()),
            Some("c".to_string()),
        ]
    );

    Ok(())
}

test_with_backends!(test_sort_rewrite_large_dataset_impl);

/// Sort a larger dataset to exercise chunking behavior.
async fn test_sort_rewrite_large_dataset_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]));

    let ctx = SessionContext::new();
    let table = create_sorted_table(
        &fixture,
        "sort_large",
        Arc::clone(&schema),
        vec!["id".to_string()],
        ctx.runtime_env(),
    )
    .await;

    // Insert 500 rows in 5 batches of 100, in reverse order
    for batch_num in (0..5).rev() {
        let mut values = Vec::new();
        for i in 0..100 {
            let id = batch_num * 100 + i;
            values.push(format!("({id}, 'row_{id}')"));
        }
        let values_str = values.join(", ");
        sql_insert(&table, "sort_large", &values_str).await;
    }

    // Sort and rewrite
    table
        .sort_and_rewrite_data(128 * 1024 * 1024)
        .await
        .expect("sort should succeed");

    let results = query_all(&ctx, &table, "sort_large").await;
    assert_eq!(total_rows(&results), 500, "should have 500 rows");

    let ids = collect_i64_column(&results, "id");

    // Verify sorted ascending
    for i in 1..ids.len() {
        assert!(
            ids[i] >= ids[i - 1],
            "data should be sorted: ids[{}]={} < ids[{}]={}",
            i - 1,
            ids[i - 1],
            i,
            ids[i]
        );
    }

    // Verify all expected values are present
    assert_eq!(ids[0], 0, "first id should be 0");
    assert_eq!(ids[499], 499, "last id should be 499");

    Ok(())
}

test_with_backends!(test_sort_rewrite_multi_column_sort_impl);

/// Sort-rewrite on a table with multiple sort columns.
///
/// Verifies that data is sorted by the first column, then by the second column
/// within ties.
async fn test_sort_rewrite_multi_column_sort_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("category", DataType::Int64, false),
        Field::new("rank", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let ctx = SessionContext::new();
    let table = create_sorted_table(
        &fixture,
        "sort_multi_col",
        Arc::clone(&schema),
        vec!["category".to_string(), "rank".to_string()],
        ctx.runtime_env(),
    )
    .await;

    // Insert data in random order
    sql_insert(
        &table,
        "sort_multi_col",
        "(2, 3, 'b3'), (1, 2, 'a2'), (2, 1, 'b1'), (1, 1, 'a1'), (2, 2, 'b2'), (1, 3, 'a3')",
    )
    .await;

    // Sort and rewrite
    table
        .sort_and_rewrite_data(128 * 1024 * 1024)
        .await
        .expect("multi-column sort should succeed");

    let results = query_all(&ctx, &table, "sort_multi_col").await;
    assert_eq!(total_rows(&results), 6, "should have 6 rows");

    let categories = collect_i64_column(&results, "category");
    let ranks = collect_i64_column(&results, "rank");
    let names = collect_string_column(&results, "name");

    // Should be sorted by category ASC, then rank ASC
    assert_eq!(categories, vec![1, 1, 1, 2, 2, 2]);
    assert_eq!(ranks, vec![1, 2, 3, 1, 2, 3]);
    assert_eq!(
        names,
        vec![
            Some("a1".to_string()),
            Some("a2".to_string()),
            Some("a3".to_string()),
            Some("b1".to_string()),
            Some("b2".to_string()),
            Some("b3".to_string()),
        ]
    );

    Ok(())
}

test_with_backends!(test_sort_rewrite_reopen_table_impl);

/// Verify sort-rewrite persists: re-opening the table from catalog shows sorted data.
async fn test_sort_rewrite_reopen_table_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("txt", DataType::Utf8, false),
    ]));

    let ctx = SessionContext::new();
    let runtime_env = ctx.runtime_env();

    let vortex_config = VortexConfig {
        sort_columns: vec!["id".to_string()],
        ..VortexConfig::default()
    };

    let context = CayenneContext::new(&vortex_config, Arc::clone(&runtime_env));

    let options = CreateTableOptions {
        table_name: "sort_reopen".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec![],
        on_conflict: None,
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: vortex_config.clone(),
    };

    let catalog_arc = Arc::clone(&fixture.catalog);
    let catalog_arc: Arc<dyn MetadataCatalog> = catalog_arc;

    let table = Arc::new(
        CayenneTableProviderBuilder::new(Arc::clone(&catalog_arc), Arc::clone(&runtime_env))
            .with_context(Arc::clone(&context))
            .create(options)
            .await
            .expect("table should be created"),
    );

    // Insert unsorted data
    sql_insert(&table, "sort_reopen", "(30, 'c'), (10, 'a'), (20, 'b')").await;

    // Sort and rewrite
    table
        .sort_and_rewrite_data(128 * 1024 * 1024)
        .await
        .expect("sort should succeed");

    // Drop the provider and re-open from catalog
    drop(table);

    let reopened = Arc::new(
        CayenneTableProviderBuilder::new(Arc::clone(&catalog_arc), runtime_env)
            .with_context(context)
            .open("sort_reopen")
            .await
            .expect("re-opening table should succeed"),
    );

    // Verify data is still sorted in the re-opened table
    let results = query_all(&ctx, &reopened, "sort_reopen").await;
    assert_eq!(total_rows(&results), 3, "should have 3 rows");

    let ids = collect_i64_column(&results, "id");
    assert_eq!(
        ids,
        vec![10, 20, 30],
        "data should still be sorted after re-open"
    );

    let txt = collect_string_column(&results, "txt");
    assert_eq!(
        txt,
        vec![
            Some("a".to_string()),
            Some("b".to_string()),
            Some("c".to_string()),
        ]
    );

    Ok(())
}

test_with_backends!(test_sort_rewrite_duplicate_values_impl);

/// Sort with duplicate sort-column values: duplicates should be preserved (stable-ish).
async fn test_sort_rewrite_duplicate_values_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("priority", DataType::Int64, false),
        Field::new("item", DataType::Utf8, false),
    ]));

    let ctx = SessionContext::new();
    let table = create_sorted_table(
        &fixture,
        "sort_dups",
        Arc::clone(&schema),
        vec!["priority".to_string()],
        ctx.runtime_env(),
    )
    .await;

    // Insert rows with duplicate priorities
    sql_insert(
        &table,
        "sort_dups",
        "(3, 'c1'), (1, 'a1'), (2, 'b1'), (1, 'a2'), (3, 'c2'), (2, 'b2')",
    )
    .await;

    table
        .sort_and_rewrite_data(128 * 1024 * 1024)
        .await
        .expect("sort with duplicates should succeed");

    let results = query_all(&ctx, &table, "sort_dups").await;
    assert_eq!(total_rows(&results), 6, "all 6 rows should be preserved");

    let priorities = collect_i64_column(&results, "priority");

    // Verify sorted ascending (duplicates are fine)
    for i in 1..priorities.len() {
        assert!(
            priorities[i] >= priorities[i - 1],
            "priorities should be non-decreasing"
        );
    }

    // Verify grouping: first two are 1, next two are 2, last two are 3
    assert_eq!(priorities[0], 1);
    assert_eq!(priorities[1], 1);
    assert_eq!(priorities[2], 2);
    assert_eq!(priorities[3], 2);
    assert_eq!(priorities[4], 3);
    assert_eq!(priorities[5], 3);

    // Verify all items are present (order within same priority is not guaranteed)
    let items = collect_string_column(&results, "item");
    let mut item_strs: Vec<String> = items.into_iter().flatten().collect();
    item_strs.sort();
    assert_eq!(
        item_strs,
        vec!["a1", "a2", "b1", "b2", "c1", "c2"],
        "all items should be preserved"
    );

    Ok(())
}
