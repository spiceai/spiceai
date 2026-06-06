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

//! Integration tests for `cayenne_delta_encoding` (delta-write encoding levels).
//!
//! Two things must hold, and the second is the validity gate for the first:
//!
//! 1. **Correctness** — a table written at any encoding level returns exactly
//!    the same rows as one written at the full default level. Every level
//!    emits standard Vortex encodings; the scan path is encoding-agnostic.
//! 2. **Engagement** — the level actually reaches the Vortex writer. A
//!    level-0 (uncompressed) table's data files must be measurably LARGER on
//!    compressible data than a full-level table's. Without this assertion the
//!    correctness test could pass with the strategy override silently
//!    ignored (both tables writing identical default-encoded files).

#![allow(clippy::expect_used)]

mod common;

use std::path::Path;
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use cayenne::metadata::{CreateTableOptions, DeltaEncoding, VortexConfig};
use cayenne::{CayenneTableProvider, MetadataCatalog};
use common::{BackendType, TestFixture, insert_batch};
use datafusion::datasource::TableProvider;
use datafusion::physical_plan::collect;
use datafusion::prelude::SessionContext;

/// Enough rows to exceed the inline-memtable admission cap (default 1024),
/// forcing the staged file-write path the encoding level applies to.
const ROW_COUNT: usize = 5_000;

/// Distinct repeated string values — highly compressible (dictionary / FSST),
/// so the level-0 vs full-level on-disk size gap is large and robust.
const DISTINCT_NAMES: usize = 32;

fn test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]))
}

fn compressible_batch(rows: usize) -> RecordBatch {
    let ids: Vec<i64> = (0..rows as i64).collect();
    let names: Vec<String> = (0..rows)
        .map(|i| format!("repeated_delta_encoding_value_{:02}", i % DISTINCT_NAMES))
        .collect();
    RecordBatch::try_new(
        test_schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names)),
        ],
    )
    .expect("build compressible batch")
}

async fn create_table_with_encoding(
    fixture: &TestFixture,
    table_name: &str,
    delta_encoding: DeltaEncoding,
) -> Arc<CayenneTableProvider> {
    let vortex_config = VortexConfig {
        delta_encoding,
        ..VortexConfig::default()
    };
    let options = CreateTableOptions {
        table_name: table_name.to_string(),
        schema: test_schema(),
        primary_key: vec![],
        on_conflict: None,
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config,
    };
    let catalog: Arc<dyn MetadataCatalog> = Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let ctx = SessionContext::new();
    Arc::new(
        CayenneTableProvider::create_table(catalog, options, ctx.runtime_env())
            .await
            .expect("create table"),
    )
}

/// Sum the sizes of all `.vortex` data files under a table's data directory.
fn vortex_bytes_under(dir: &Path) -> u64 {
    let mut total = 0;
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(_) => return 0,
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            total += vortex_bytes_under(&path);
        } else if path.extension().is_some_and(|ext| ext == "vortex") {
            total += entry.metadata().map(|m| m.len()).unwrap_or(0);
        }
    }
    total
}

async fn scan_all_rows(table: &Arc<CayenneTableProvider>, name: &str) -> Vec<RecordBatch> {
    let ctx = SessionContext::new();
    ctx.register_table(name, Arc::clone(table) as Arc<dyn TableProvider>)
        .expect("register table");
    let df = ctx
        .sql(&format!("SELECT id, name FROM {name} ORDER BY id"))
        .await
        .expect("plan scan");
    let plan = df.create_physical_plan().await.expect("physical plan");
    collect(plan, ctx.task_ctx()).await.expect("collect rows")
}

fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}

#[tokio::test]
async fn delta_encoding_levels_are_correct_and_actually_engage() {
    let fixture = TestFixture::new(BackendType::Sqlite)
        .await
        .expect("fixture");

    // Two identical tables, differing only in the pinned delta-encoding level.
    let uncompressed_table =
        create_table_with_encoding(&fixture, "delta_enc_level0", DeltaEncoding::Level(0)).await;
    let full_table =
        create_table_with_encoding(&fixture, "delta_enc_level9", DeltaEncoding::Level(9)).await;

    let batch = compressible_batch(ROW_COUNT);
    let written_level0 = insert_batch(&uncompressed_table, batch.clone())
        .await
        .expect("insert level 0");
    let written_full = insert_batch(&full_table, batch)
        .await
        .expect("insert level 9");
    assert_eq!(written_level0 as usize, ROW_COUNT, "level-0 insert row count");
    assert_eq!(written_full as usize, ROW_COUNT, "level-9 insert row count");

    // Correctness: both tables return exactly the inserted rows.
    let level0_rows = scan_all_rows(&uncompressed_table, "delta_enc_level0").await;
    let full_rows = scan_all_rows(&full_table, "delta_enc_level9").await;
    assert_eq!(total_rows(&level0_rows), ROW_COUNT, "level-0 scan row count");
    assert_eq!(total_rows(&full_rows), ROW_COUNT, "level-9 scan row count");
    let level0_pretty =
        arrow::util::pretty::pretty_format_batches(&level0_rows).expect("format level-0");
    let full_pretty =
        arrow::util::pretty::pretty_format_batches(&full_rows).expect("format level-9");
    assert_eq!(
        level0_pretty.to_string(),
        full_pretty.to_string(),
        "level-0 and full-level tables must return identical rows"
    );

    // Engagement (validity gate): the level must actually reach the Vortex
    // writer. On this highly-repetitive data the uncompressed (level-0) files
    // must be substantially larger than the full-level (BtrBlocks) files. If
    // the strategy override were silently ignored, both totals would be equal
    // and this assertion fails loudly.
    let level0_table_id = fixture
        .catalog
        .get_table("delta_enc_level0")
        .await
        .expect("get level-0 table")
        .table_id;
    let full_table_id = fixture
        .catalog
        .get_table("delta_enc_level9")
        .await
        .expect("get level-9 table")
        .table_id;
    let level0_bytes = vortex_bytes_under(&fixture.data_path.join(&level0_table_id));
    let full_bytes = vortex_bytes_under(&fixture.data_path.join(&full_table_id));
    assert!(
        level0_bytes > 0 && full_bytes > 0,
        "both tables must have produced vortex data files \
         (level0={level0_bytes} B, full={full_bytes} B)"
    );
    assert!(
        level0_bytes as f64 > full_bytes as f64 * 1.3,
        "level-0 (uncompressed) files must be substantially larger than \
         full-level files on compressible data — got level0={level0_bytes} B \
         vs full={full_bytes} B. If these are equal, the delta-encoding \
         strategy override never reached the Vortex writer."
    );
}
