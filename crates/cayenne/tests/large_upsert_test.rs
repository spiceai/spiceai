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

//! Regression test: upserting more than `SQLITE_MAX_VARIABLE_NUMBER` / 4 rows
//! must not fail with a `SQLite` "too many SQL variables" error.
//!
//! `add_insert_records_batch` builds an INSERT with 4 params per row.
//! `SQLite`'s default `SQLITE_MAX_VARIABLE_NUMBER` is 32 766, so batches with
//! more than ~8 000 rows would exceed the limit without the inline-SQL
//! fallback path.

mod common;

use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use cayenne::metadata::CreateTableOptions;
use cayenne::{CayenneTableProvider, MetadataCatalog};

use datafusion::prelude::SessionContext;

use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};

test_with_backends!(test_large_upsert_exceeds_sqlite_param_limit);
test_with_backends!(test_large_sharded_upsert_preserves_pk_and_row_count);

async fn test_large_upsert_exceeds_sqlite_param_limit(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    // 10 000 conflicting PKs → 40 000 SQL parameters in add_insert_records_batch.
    // This exceeds SQLITE_MAX_VARIABLE_NUMBER (32 766) if the INSERT is unbatched.
    const ROW_COUNT: usize = 10_000;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Utf8, false),
    ]));

    let table_options = CreateTableOptions {
        table_name: "large_upsert".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["id".to_string()],
        on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
            "id".to_string(),
        ]))),
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };

    let catalog_arc: Arc<dyn MetadataCatalog> = fixture.catalog.clone();
    let ctx = SessionContext::new();
    let table =
        CayenneTableProvider::create_table(catalog_arc, table_options, ctx.runtime_env()).await?;
    let table = Arc::new(table);

    ctx.register_table(
        "large_upsert",
        Arc::clone(&table) as Arc<dyn datafusion::datasource::TableProvider>,
    )?;

    // Step 1: Insert ROW_COUNT rows (ids 0..ROW_COUNT).
    let ids: Vec<i64> = (0..i64::try_from(ROW_COUNT).expect("ROW_COUNT fits in i64")).collect();
    let values: Vec<String> = (0..ROW_COUNT).map(|i| format!("original_{i}")).collect();
    let initial_batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(ids.clone())),
            Arc::new(StringArray::from(values)),
        ],
    )?;
    common::insert_batch(&table, initial_batch).await?;

    // Step 2: Upsert the same ROW_COUNT PKs with updated values.
    // This triggers add_insert_records_batch with ROW_COUNT entries.
    let updated_values: Vec<String> = (0..ROW_COUNT).map(|i| format!("updated_{i}")).collect();
    let upsert_batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(updated_values)),
        ],
    )?;
    common::insert_batch(&table, upsert_batch).await?;

    // Step 3: Verify all rows have updated values.
    let results = ctx
        .sql("SELECT id, value FROM large_upsert ORDER BY id")
        .await?
        .collect()
        .await?;

    let total_rows: usize = results.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(
        total_rows, ROW_COUNT,
        "Expected {ROW_COUNT} rows after upsert, got {total_rows}"
    );

    // Spot-check a few values.
    let first_batch = &results[0];
    let values_col = first_batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("value column");
    assert_eq!(values_col.value(0), "updated_0");

    Ok(())
}

/// After the shard count was made size-aware, a write that *does* warrant
/// multiple shards must still round-trip every PK exactly once (no rows
/// dropped, duplicated, or mis-routed by the hash partitioner). This drives a
/// keyed (PK) table through a genuine multi-shard Vortex write — small target
/// file size + a write-concurrency override so the size-aware count resolves to
/// more than one shard, plus an upsert, then verifies the exact row count and
/// per-key latest value survive.
async fn test_large_sharded_upsert_preserves_pk_and_row_count(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    // Above the default inline_max_rows (1024) so the write falls out of the
    // inline memtable into a real (sharded) Vortex write.
    const ROW_COUNT: usize = 5_000;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Utf8, false),
    ]));

    // Tiny target file size + a write-concurrency override: the inline buffer
    // (~4 MiB) is several 1 MiB target files' worth, so the size-aware shard
    // count resolves above 1 and the keyed table takes the hash-partitioned
    // multi-shard write path.
    let vortex_config = cayenne::metadata::VortexConfig {
        target_vortex_file_size_mb: 1,
        write_concurrency: Some(4),
        ..cayenne::metadata::VortexConfig::default()
    };

    let table_options = CreateTableOptions {
        table_name: "large_sharded_upsert".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["id".to_string()],
        on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
            "id".to_string(),
        ]))),
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config,
    };

    let catalog_arc: Arc<dyn MetadataCatalog> = fixture.catalog.clone();
    let ctx = SessionContext::new();
    let table =
        CayenneTableProvider::create_table(catalog_arc, table_options, ctx.runtime_env()).await?;
    let table = Arc::new(table);

    ctx.register_table(
        "large_sharded_upsert",
        Arc::clone(&table) as Arc<dyn datafusion::datasource::TableProvider>,
    )?;

    // Step 1: Insert ROW_COUNT distinct PKs.
    let ids: Vec<i64> = (0..i64::try_from(ROW_COUNT).expect("ROW_COUNT fits in i64")).collect();
    let values: Vec<String> = (0..ROW_COUNT).map(|i| format!("original_{i}")).collect();
    let initial_batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(ids.clone())),
            Arc::new(StringArray::from(values)),
        ],
    )?;
    common::insert_batch(&table, initial_batch).await?;

    // Step 2: Upsert the same PKs with new values (exercises the keyed
    // hash-sharded write under conflict resolution).
    let updated_values: Vec<String> = (0..ROW_COUNT).map(|i| format!("updated_{i}")).collect();
    let upsert_batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(updated_values)),
        ],
    )?;
    common::insert_batch(&table, upsert_batch).await?;

    // Step 3: Exactly ROW_COUNT live rows — no PK dropped or duplicated by the
    // multi-shard write.
    let results = ctx
        .sql("SELECT id, value FROM large_sharded_upsert ORDER BY id")
        .await?
        .collect()
        .await?;
    let total_rows: usize = results.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(
        total_rows, ROW_COUNT,
        "sharded upsert must keep exactly one row per PK ({ROW_COUNT}), got {total_rows}"
    );

    // Step 4: Every PK maps to its updated value (correct routing + latest-wins).
    let mut seen = 0usize;
    for batch in &results {
        let ids_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id column");
        let values_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("value column");
        for row in 0..batch.num_rows() {
            let id = ids_col.value(row);
            assert_eq!(
                values_col.value(row),
                format!("updated_{id}"),
                "PK {id} should hold its upserted value"
            );
            assert_eq!(
                id,
                i64::try_from(seen).expect("row index fits in i64"),
                "rows must be the full contiguous PK range with none missing"
            );
            seen += 1;
        }
    }
    assert_eq!(seen, ROW_COUNT, "must observe every PK exactly once");

    Ok(())
}
