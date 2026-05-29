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

//! End-to-end correctness tests for `deletion_mode: position` (merge-on-read
//! position deletes) on primary-key upsert tables. Inlining is disabled so each
//! insert materializes a Vortex file (position deletes apply to files, not the
//! inline memtable), and `run_position_capture()` is called explicitly to make
//! the write-time read-back deterministic in tests.

mod common;

use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};

use cayenne::metadata::{CreateTableOptions, DeletionMode, VortexConfig};
use cayenne::{CayenneTableProvider, MetadataCatalog};

use datafusion::prelude::SessionContext;
use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};

test_with_backends!(test_position_mode_composite_pk_upsert_impl);
test_with_backends!(test_position_mode_resurrection_impl);
test_with_backends!(test_position_mode_without_capture_falls_back_to_key_impl);

/// Force-file-write Vortex config in `deletion_mode: position`.
fn position_mode_config() -> VortexConfig {
    VortexConfig {
        deletion_mode: DeletionMode::Position,
        // Disable inlining so inserts produce Vortex files (position deletes
        // apply to files; inlined rows use the inline-rewrite delete path).
        inline_max_rows: 0,
        inline_max_bytes: 0,
        inline_max_buffer_bytes: 0,
        ..VortexConfig::default()
    }
}

/// Composite-PK (`RowConverterBased`) upsert with position deletes: the prior
/// version is tombstoned by file position and the new version is visible, with
/// no duplicates.
async fn test_position_mode_composite_pk_upsert_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("w_id", DataType::Int64, false),
        Field::new("o_id", DataType::Int64, false),
        Field::new("amount", DataType::Int64, false),
    ]));

    let table_options = CreateTableOptions {
        table_name: "pos_upsert".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["w_id".to_string(), "o_id".to_string()],
        on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
            "w_id".to_string(),
            "o_id".to_string(),
        ]))),
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: position_mode_config(),
    };

    let catalog: Arc<dyn MetadataCatalog> = fixture.catalog.clone();
    let ctx = SessionContext::new();
    let table = Arc::new(
        CayenneTableProvider::create_table(catalog, table_options, ctx.runtime_env()).await?,
    );
    ctx.register_table(
        "pos_upsert",
        Arc::clone(&table) as Arc<dyn datafusion::datasource::TableProvider>,
    )?;

    // Initial insert -> Vortex file(s).
    ctx.sql("INSERT INTO pos_upsert VALUES (1, 10, 100), (1, 20, 200), (2, 10, 300)")
        .await?
        .collect()
        .await?;

    // Capture file-local positions into the keyset (FileUnlocated -> FilePositioned).
    table.run_position_capture().await?;

    // Upsert (1,10) and (2,10) with new amounts; (1,20) untouched.
    ctx.sql("INSERT INTO pos_upsert VALUES (1, 10, 111), (2, 10, 333)")
        .await?
        .collect()
        .await?;

    let batches = ctx
        .sql("SELECT w_id, o_id, amount FROM pos_upsert ORDER BY w_id, o_id")
        .await?
        .collect()
        .await?;

    let rows = collect_triplets(&batches);
    assert_eq!(
        rows,
        vec![(1, 10, 111), (1, 20, 200), (2, 10, 333)],
        "position-mode upsert must show new values with no duplicate prior versions"
    );

    Ok(())
}

/// Resurrection: deleting then re-inserting the same PK must leave exactly one
/// (new) row visible — the old position is tombstoned, the new file is not.
async fn test_position_mode_resurrection_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));
    let table_options = CreateTableOptions {
        table_name: "pos_resurrect".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["id".to_string()],
        on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
            "id".to_string(),
        ]))),
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: position_mode_config(),
    };
    let catalog: Arc<dyn MetadataCatalog> = fixture.catalog.clone();
    let ctx = SessionContext::new();
    let table = Arc::new(
        CayenneTableProvider::create_table(catalog, table_options, ctx.runtime_env()).await?,
    );
    ctx.register_table(
        "pos_resurrect",
        Arc::clone(&table) as Arc<dyn datafusion::datasource::TableProvider>,
    )?;

    // Three rounds of upserting the SAME key, capturing positions between each,
    // so every round tombstones the prior file position.
    for value in [1_i64, 2, 3] {
        ctx.sql(&format!("INSERT INTO pos_resurrect VALUES (7, {value})"))
            .await?
            .collect()
            .await?;
        table.run_position_capture().await?;
    }

    let batches = ctx
        .sql("SELECT id, v FROM pos_resurrect ORDER BY id")
        .await?
        .collect()
        .await?;
    let pairs = collect_pairs(&batches);
    assert_eq!(
        pairs,
        vec![(7, 3)],
        "after repeated upserts only the latest value of the key must be visible"
    );

    Ok(())
}

/// Without an explicit capture pass, the located positions are not yet known,
/// so the upsert falls back to key-based deletes — which must still be correct.
async fn test_position_mode_without_capture_falls_back_to_key_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let table_options = CreateTableOptions {
        table_name: "pos_fallback".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["id".to_string()],
        on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
            "id".to_string(),
        ]))),
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: position_mode_config(),
    };
    let catalog: Arc<dyn MetadataCatalog> = fixture.catalog.clone();
    let ctx = SessionContext::new();
    let table = Arc::new(
        CayenneTableProvider::create_table(catalog, table_options, ctx.runtime_env()).await?,
    );
    ctx.register_table(
        "pos_fallback",
        Arc::clone(&table) as Arc<dyn datafusion::datasource::TableProvider>,
    )?;

    ctx.sql("INSERT INTO pos_fallback VALUES (1, 'Alice'), (2, 'Bob')")
        .await?
        .collect()
        .await?;
    // Deliberately DO NOT call run_position_capture() — exercise the key fallback.
    ctx.sql("INSERT INTO pos_fallback VALUES (1, 'Updated')")
        .await?
        .collect()
        .await?;

    let batches = ctx
        .sql("SELECT id, name FROM pos_fallback ORDER BY id")
        .await?
        .collect()
        .await?;
    // Collect (id, name) across all batches — `collect()` may split rows over
    // several `RecordBatch`es, so inspecting only `batches[0]` could miss rows.
    let mut rows: Vec<(i64, String)> = Vec::new();
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id column");
        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("name column");
        for i in 0..batch.num_rows() {
            rows.push((ids.value(i), names.value(i).to_string()));
        }
    }
    assert_eq!(
        rows,
        vec![(1, "Updated".to_string()), (2, "Bob".to_string())],
        "key-fallback upsert must update id=1 and leave id=2 unchanged"
    );

    Ok(())
}

fn collect_triplets(batches: &[arrow::record_batch::RecordBatch]) -> Vec<(i64, i64, i64)> {
    let mut out = Vec::new();
    for batch in batches {
        let a = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("col0");
        let b = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("col1");
        let c = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("col2");
        for i in 0..batch.num_rows() {
            out.push((a.value(i), b.value(i), c.value(i)));
        }
    }
    out
}

fn collect_pairs(batches: &[arrow::record_batch::RecordBatch]) -> Vec<(i64, i64)> {
    let mut out = Vec::new();
    for batch in batches {
        let a = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("col0");
        let b = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("col1");
        for i in 0..batch.num_rows() {
            out.push((a.value(i), b.value(i)));
        }
    }
    out
}
