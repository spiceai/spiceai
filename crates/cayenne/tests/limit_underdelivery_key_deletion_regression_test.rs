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

//! P0 regression test: `LIMIT N` UNDER-DELIVERS on key-deletion tables.
//!
//! Expected: `SELECT * FROM t LIMIT 100` returns 100 rows whenever >= 100 live
//! (post-deletion) rows exist.
//!
//! Broken on trunk: `collect_scan_files_with_limit` (`provider/table.rs`)
//! truncates the scanned file set once cumulative RAW, pre-deletion
//! `file.statistics.num_rows` exceeds the limit. The key-based deletion filter
//! runs ABOVE the scan, so files dropped by the truncation are never read, and a
//! file whose rows are mostly key-deleted contributes far fewer live rows than
//! its raw count. The `collect_stats` guard that disabled this for position-based
//! deletion (`is_position_based() && has_pending_deletions()`) did NOT cover
//! key-based deletion, so the truncation fired on stale counts. The Vortex
//! `with_limit` push had the same gap.
//!
//! Repro is ORDER-INDEPENDENT: every file is heavily key-deleted so no single
//! file has `LIMIT` live rows, yet the union does — whichever file the scan
//! visits first trips the raw-count truncation (raw 1000 > limit 100), capping
//! the result at one file's 40 live rows.
//!
//! `inline_max_rows: 0` forces every insert into a Vortex FILE (not the inline
//! memtable), so the file-scan limit path under test deterministically engages.
//!
//! Tiger Style: every helper asserts a pre/postcondition; loops are bounded; no
//! `unwrap()`; the precondition (120 live rows survive deletion) is asserted
//! BEFORE the limit assertion so a pass can't be a false negative.

#![allow(clippy::expect_used)]

mod common;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use cayenne::metadata::{CreateTableOptions, VortexConfig};
use cayenne::{CayenneTableProvider, MetadataCatalog};
use common::TestFixture;
use datafusion::datasource::TableProvider;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::*;
use std::sync::Arc;

type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

/// Raw rows written per file. Must exceed `QUERY_LIMIT` so a single file trips
/// the raw-count truncation, and exceed nothing inline-related because
/// `inline_max_rows: 0` already forces files.
const RAW_ROWS_PER_FILE: i64 = 1_000;
/// Live rows left per file after key-deletion (< `QUERY_LIMIT`).
const LIVE_ROWS_PER_FILE: i64 = 40;
const FILE_COUNT: i64 = 3;
/// 40 < QUERY_LIMIT < 120 (= FILE_COUNT * LIVE_ROWS_PER_FILE).
const QUERY_LIMIT: usize = 100;

// Compile-time invariant: each file's raw count exceeds its live count, so a
// single file trips the raw-count truncation regardless of scan file ordering.
const _: () = assert!(RAW_ROWS_PER_FILE > LIVE_ROWS_PER_FILE);

fn pk_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Utf8, true),
    ]))
}

async fn setup_table(
    fixture: &TestFixture,
    name: &str,
) -> TestResult<(Arc<CayenneTableProvider>, SessionContext)> {
    let options = CreateTableOptions {
        table_name: name.to_string(),
        schema: pk_schema(),
        primary_key: vec!["id".to_string()], // Int64 PK -> KEY-based deletion (the unguarded path)
        on_conflict: None,
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: VortexConfig {
            // Every write lands in a Vortex file immediately, so the file-scan
            // LIMIT-truncation path under test engages deterministically.
            inline_max_rows: 0,
            ..VortexConfig::default()
        },
    };
    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let ctx = SessionContext::new();
    let table =
        Arc::new(CayenneTableProvider::create_table(catalog, options, ctx.runtime_env()).await?);
    ctx.register_table(name, Arc::clone(&table) as Arc<dyn TableProvider>)?;
    Ok((table, ctx))
}

/// Insert one file's worth of rows in id range `[start, start+RAW)`, then
/// key-delete all but the last `LIVE_ROWS_PER_FILE` of them.
async fn write_heavily_deleted_file(
    table: &Arc<CayenneTableProvider>,
    start_id: i64,
) -> TestResult<()> {
    let ids: Vec<i64> = (start_id..start_id + RAW_ROWS_PER_FILE).collect();
    let values: Vec<String> = ids.iter().map(|id| format!("v{id}")).collect();
    let batch = RecordBatch::try_new(
        pk_schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(values)),
        ],
    )?;
    let inserted = common::insert_batch(table.as_ref(), batch).await?;
    assert_eq!(
        inserted as i64, RAW_ROWS_PER_FILE,
        "insert must write every raw row before deletion"
    );

    // Key-delete the first (RAW - LIVE) ids in THIS file's range only. The
    // range is bounded below by `start_id` so a later file's delete (with a
    // higher threshold) cannot cascade into an earlier file's survivors.
    let delete_below = start_id + (RAW_ROWS_PER_FILE - LIVE_ROWS_PER_FILE);
    let ctx = SessionContext::new();
    let plan = table
        .delete_from(
            &ctx.state(),
            vec![
                col("id")
                    .gt_eq(lit(start_id))
                    .and(col("id").lt(lit(delete_below))),
            ],
        )
        .await?;
    let _ = datafusion_physical_plan::collect(plan, ctx.task_ctx()).await?;
    Ok(())
}

async fn collected_row_count(ctx: &SessionContext, sql: &str) -> TestResult<usize> {
    let batches = ctx.sql(sql).await?.collect().await?;
    Ok(batches.iter().map(RecordBatch::num_rows).sum())
}

async fn limit_underdelivery_key_deletion_impl(fixture: TestFixture) -> TestResult<()> {
    let (table, ctx) = setup_table(&fixture, "limit_underdelivery").await?;

    // Bounded loop: exactly FILE_COUNT files, disjoint id ranges.
    for file_index in 0..FILE_COUNT {
        write_heavily_deleted_file(&table, file_index * RAW_ROWS_PER_FILE + 1).await?;
    }

    // PRECONDITION: deletions applied — total live rows is FILE_COUNT*LIVE, not
    // FILE_COUNT*RAW. If this fails, the deletion path didn't engage and the
    // limit assertion below would be meaningless.
    let total_live = collected_row_count(&ctx, "SELECT * FROM limit_underdelivery").await?;
    let expected_live = (FILE_COUNT * LIVE_ROWS_PER_FILE) as usize;
    assert_eq!(
        total_live, expected_live,
        "precondition: key-deletion must leave exactly {expected_live} live rows (got {total_live})"
    );
    assert!(
        expected_live > QUERY_LIMIT,
        "precondition: more than {QUERY_LIMIT} live rows must exist so LIMIT can be satisfied"
    );

    // THE BUG: LIMIT must return exactly QUERY_LIMIT rows because expected_live >
    // QUERY_LIMIT live rows exist. On trunk it returns LIVE_ROWS_PER_FILE (40):
    // the file set is truncated by raw pre-deletion counts after the first file.
    let limited = collected_row_count(
        &ctx,
        &format!("SELECT * FROM limit_underdelivery LIMIT {QUERY_LIMIT}"),
    )
    .await?;
    assert_eq!(
        limited, QUERY_LIMIT,
        "LIMIT {QUERY_LIMIT} returned {limited} rows but {expected_live} live rows exist — \
         key-deletion LIMIT under-delivery: collect_scan_files_with_limit truncates the file \
         set by raw pre-deletion counts (and the Vortex with_limit push caps by raw rows); \
         key-based deletion is not guarded the way position-based is"
    );

    Ok(())
}

test_with_backends!(limit_underdelivery_key_deletion_impl);
