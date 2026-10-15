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

use crate::common;

use std::path::Path;
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use cayenne::metadata::{CompressionStrategy, CreateTableOptions, DeltaEncoding, VortexConfig};
use cayenne::{CayenneTableProvider, MetadataCatalog};
use common::{BackendType, TestFixture, insert_batch, write_batches};
use datafusion::datasource::TableProvider;
use datafusion::physical_plan::collect;
use datafusion::prelude::SessionContext;
use datafusion_expr::dml::InsertOp;

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
    let row_count = i64::try_from(rows).expect("row count fits i64");
    let ids: Vec<i64> = (0..row_count).collect();
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
    create_table(fixture, table_name, test_schema(), vortex_config).await
}

async fn create_table(
    fixture: &TestFixture,
    table_name: &str,
    schema: SchemaRef,
    vortex_config: VortexConfig,
) -> Arc<CayenneTableProvider> {
    let options = CreateTableOptions {
        table_name: table_name.to_string(),
        schema,
        primary_key: vec![],
        on_conflict: None,
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config,
    };
    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
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
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            total += vortex_bytes_under(&path);
        } else if path.extension().is_some_and(|ext| ext == "vortex") {
            total += entry.metadata().map_or(0, |m| m.len());
        }
    }
    total
}

async fn scan_all_rows(table: &Arc<CayenneTableProvider>, name: &str) -> Vec<RecordBatch> {
    let ctx = SessionContext::new();
    ctx.register_table(name, Arc::clone(table) as Arc<dyn TableProvider>)
        .expect("register table");
    let df = ctx
        .sql(&format!("SELECT * FROM {name} ORDER BY id"))
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
    let expected_rows = u64::try_from(ROW_COUNT).expect("row count fits u64");
    assert_eq!(written_level0, expected_rows, "level-0 insert row count");
    assert_eq!(written_full, expected_rows, "level-9 insert row count");

    // Correctness: both tables return exactly the inserted rows.
    let level0_rows = scan_all_rows(&uncompressed_table, "delta_enc_level0").await;
    let full_rows = scan_all_rows(&full_table, "delta_enc_level9").await;
    assert_eq!(
        total_rows(&level0_rows),
        ROW_COUNT,
        "level-0 scan row count"
    );
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
    // Blocking std::fs walk — run on the blocking pool so it can't stall a
    // tokio worker under contention.
    let level0_dir = fixture.data_path.join(&level0_table_id);
    let full_dir = fixture.data_path.join(&full_table_id);
    let (level0_bytes, full_bytes) = tokio::task::spawn_blocking(move || {
        (
            vortex_bytes_under(&level0_dir),
            vortex_bytes_under(&full_dir),
        )
    })
    .await
    .expect("size walk task");
    assert!(
        level0_bytes > 0 && full_bytes > 0,
        "both tables must have produced vortex data files \
         (level0={level0_bytes} B, full={full_bytes} B)"
    );
    // Integer form of `level0 > full * 1.3` (avoids u64→f64 precision lints).
    assert!(
        level0_bytes.saturating_mul(10) > full_bytes.saturating_mul(13),
        "level-0 (uncompressed) files must be substantially larger than \
         full-level files on compressible data — got level0={level0_bytes} B \
         vs full={full_bytes} B. If these are equal, the delta-encoding \
         strategy override never reached the Vortex writer."
    );
}

/// Rows per write in the custom-strategy regression test.
const STRATEGY_ROWS: i64 = 20_000;

fn monotone_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new("name", DataType::Utf8, false),
    ]))
}

/// Rows `start..start + rows`. `id` increases by irregular steps, so its
/// best encoding in the default `BtrBlocks` search is `fastlanes.delta`; `ts`
/// advances one second per row and `name` is derived from the row number, as
/// in the #15175 reproduction.
fn monotone_batch(start: i64, rows: i64) -> RecordBatch {
    // 2026-01-01T00:00:00Z
    const EPOCH_MICROS: i64 = 1_767_225_600_000_000;
    let row_numbers = start..start + rows;
    let ids: Vec<i64> = row_numbers
        .clone()
        .map(|row| row * 64 + (row * 2_654_435_761).rem_euclid(61))
        .collect();
    let timestamps: Vec<i64> = row_numbers
        .clone()
        .map(|row| EPOCH_MICROS + row * 1_000_000)
        .collect();
    let names: Vec<String> = row_numbers.map(|row| format!("k{row}")).collect();
    RecordBatch::try_new(
        monotone_schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(TimestampMicrosecondArray::from(timestamps)),
            Arc::new(StringArray::from(names)),
        ],
    )
    .expect("build monotone batch")
}

/// Regression test for #15175: a write strategy built from the default
/// `BtrBlocks` scheme set failed the write with "Serialized array ID
/// fastlanes.delta not permitted by ctx". `cayenne_compression_strategy: zstd`
/// uses one for maintenance writes, such as an overwrite, and
/// `cayenne_delta_encoding` levels 4 to 6 use one for delta writes, such as an
/// append. Each case writes twice and reads every row back.
#[tokio::test]
async fn default_scheme_set_strategies_write_and_read_back() {
    let fixture = TestFixture::new(BackendType::Sqlite)
        .await
        .expect("fixture");

    let delta_level = |level| VortexConfig {
        delta_encoding: DeltaEncoding::Level(level),
        ..VortexConfig::default()
    };
    let cases = [
        (
            "compression_zstd",
            VortexConfig {
                compression_strategy: CompressionStrategy::Zstd,
                ..VortexConfig::default()
            },
            InsertOp::Overwrite,
        ),
        ("delta_level4", delta_level(4), InsertOp::Append),
        ("delta_level5", delta_level(5), InsertOp::Append),
        ("delta_level6", delta_level(6), InsertOp::Append),
    ];

    let first = monotone_batch(0, STRATEGY_ROWS);
    let second = monotone_batch(STRATEGY_ROWS, STRATEGY_ROWS);
    let expected = arrow::compute::concat_batches(&monotone_schema(), [&first, &second])
        .expect("concat expected rows");

    for (name, vortex_config, first_op) in cases {
        let table = create_table(&fixture, name, monotone_schema(), vortex_config).await;
        for (op, batch) in [(first_op, &first), (InsertOp::Append, &second)] {
            let written = write_batches(&table, vec![batch.clone()], op)
                .await
                .unwrap_or_else(|e| panic!("{name}: {op} failed: {e}"));
            assert_eq!(
                written,
                u64::try_from(STRATEGY_ROWS).expect("row count fits u64"),
                "{name}: {op} row count"
            );
        }

        let rows = scan_all_rows(&table, name).await;
        let actual = arrow::compute::concat_batches(&monotone_schema(), &rows)
            .unwrap_or_else(|e| panic!("{name}: concat scanned rows: {e}"));
        assert_eq!(
            actual.num_rows(),
            expected.num_rows(),
            "{name}: scanned row count"
        );
        for (index, field) in monotone_schema().fields().iter().enumerate() {
            assert_eq!(
                actual.column(index).as_ref(),
                expected.column(index).as_ref(),
                "{name}: column `{}` must read back exactly as written",
                field.name()
            );
        }
    }
}
