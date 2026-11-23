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
//!
//! `limit_with_in_memory_cdc_deletes` covers the same contract when the
//! only pending deletes are CDC deletes held in the in-memory tier
//! (`cdc_durability: memory`, before a checkpoint): they are not in the durable
//! deletion state, but the scan's key-deletion filter applies them, so no raw
//! limit may reach the file scan beneath that filter.

#![allow(clippy::expect_used)]

use crate::common;

use arrow::array::{Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use cayenne::metadata::{CdcDurability, CreateTableOptions, VortexConfig};
use cayenne::{CayenneTableProvider, CayenneTableProviderBuilder, MetadataCatalog, SlotAdvancer};
use common::TestFixture;
use datafusion::datasource::TableProvider;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::prelude::*;
use datafusion_table_providers::util::column_reference::ColumnReference;
use datafusion_table_providers::util::on_conflict::OnConflict;
use std::sync::Arc;

type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

/// Raw rows written per file. Must exceed `QUERY_LIMIT` so a single file trips
/// the raw-count truncation, and exceed nothing inline-related because
/// `inline_max_rows: 0` already forces files.
const RAW_ROWS_PER_FILE: i64 = 1_000;
/// Live rows left per file after key-deletion (< `QUERY_LIMIT`).
const LIVE_ROWS_PER_FILE: i64 = 40;
const FILE_COUNT: i64 = 3;
/// 40 < `QUERY_LIMIT` < 120 (= `FILE_COUNT` * `LIVE_ROWS_PER_FILE`).
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
    let inserted = i64::try_from(inserted)?;
    assert_eq!(
        inserted, RAW_ROWS_PER_FILE,
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
    let expected_live = usize::try_from(FILE_COUNT * LIVE_ROWS_PER_FILE)?;
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

/// Rows written durably before the in-memory deletes.
const MEM_TIER_ROWS: i64 = 1_000;
/// Keys `1..=MEM_TIER_DELETED` are deleted through the in-memory CDC tier, which
/// hides the first rows of every data file and leaves `MEM_TIER_ROWS -
/// MEM_TIER_DELETED` live.
const MEM_TIER_DELETED: i64 = 990;

struct NoopSlotAdvancer;

#[async_trait::async_trait]
impl SlotAdvancer for NoopSlotAdvancer {
    async fn on_checkpoint_durable(&self, _durable_epoch: u64) {}
}

fn id_value_rows(ids: std::ops::RangeInclusive<i64>) -> TestResult<RecordBatch> {
    let ids: Vec<i64> = ids.collect();
    let values: Vec<String> = ids.iter().map(|id| format!("v{id}")).collect();
    Ok(RecordBatch::try_new(
        pk_schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(values)),
        ],
    )?)
}

async fn collected_ids(ctx: &SessionContext, sql: &str) -> TestResult<Vec<i64>> {
    let batches = ctx.sql(sql).await?.collect().await?;
    let mut ids = Vec::new();
    for batch in &batches {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or("the first column must be the Int64 `id`")?;
        ids.extend((0..column.len()).map(|row| column.value(row)));
    }
    Ok(ids)
}

/// `(snapshots_scanned, files_scanned)` from the `CayenneAccelerationExec` line of
/// `EXPLAIN <sql>`.
async fn scan_shape(ctx: &SessionContext, sql: &str) -> TestResult<(usize, usize)> {
    let batches = ctx.sql(&format!("EXPLAIN {sql}")).await?.collect().await?;
    let plan = arrow::util::pretty::pretty_format_batches(&batches)?.to_string();
    let field = |name: &str| -> TestResult<usize> {
        let value = plan
            .split(&format!("{name}="))
            .nth(1)
            .and_then(|rest| rest.split(|c: char| !c.is_ascii_digit()).next())
            .ok_or_else(|| format!("no {name} in the plan:\n{plan}"))?;
        Ok(value.parse()?)
    };
    Ok((field("snapshots_scanned")?, field("files_scanned")?))
}

async fn count_star(ctx: &SessionContext) -> TestResult<i64> {
    let batches = ctx.sql("SELECT COUNT(*) FROM t").await?.collect().await?;
    let column = batches
        .first()
        .ok_or("COUNT(*) returned no batch")?
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or("COUNT(*) must be Int64")?;
    Ok(column.value(0))
}

/// How the durable rows reach the table before the in-memory deletes.
#[derive(Clone, Copy, Debug)]
enum DurableRows {
    /// A whole-table overwrite of the first quarter, then three appends, write
    /// them to the current snapshot as four or more files.
    Overwrite,
    /// The same writes, then the table is reopened, so the in-memory deletes and
    /// the queries run on a provider that has not served a scan yet.
    OverwriteThenReopen,
    /// CDC upserts land in the in-memory tier; a checkpoint after each of four
    /// batches writes them to files.
    CdcCheckpoints,
}

async fn limit_with_in_memory_cdc_deletes(
    fixture: &TestFixture,
    durable_rows: DurableRows,
) -> TestResult<()> {
    let name = format!("limit_mem_tier_deletes_{durable_rows:?}").to_lowercase();
    let options = CreateTableOptions {
        table_name: name.clone(),
        schema: pk_schema(),
        primary_key: vec!["id".to_string()],
        on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
            "id".to_string(),
        ]))),
        base_path: fixture.data_path.join(&name).to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: VortexConfig {
            cdc_durability: CdcDurability::Memory,
            // The test alone decides when the in-memory tier is checkpointed.
            cdc_mem_tier_checkpoint_interval_ms: 0,
            cdc_mem_tier_max_age_ms: 0,
            cdc_mem_tier_seal_age_ms: 0,
            compaction_background_interval_ms: 3_600_000,
            inline_max_rows: 0,
            // One encoder per write, whatever the host's cores, so each write
            // below lands in its own file on every machine; and no small-file
            // compaction to merge those files back together.
            write_concurrency: Some(1),
            compaction_trigger_files: usize::MAX,
            ..VortexConfig::default()
        },
    };
    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let ctx = SessionContext::new();
    let mut table = Arc::new(
        CayenneTableProvider::create_table(Arc::clone(&catalog), options, ctx.runtime_env())
            .await?,
    );
    table.install_slot_advancer(Arc::new(NoopSlotAdvancer));

    match durable_rows {
        DurableRows::Overwrite | DurableRows::OverwriteThenReopen => {
            let per_write = MEM_TIER_ROWS / 4;
            for write in 0..4 {
                let first = write * per_write + 1;
                let rows = id_value_rows(first..=first + per_write - 1)?;
                let input = MemorySourceConfig::try_new_exec(&[vec![rows]], pk_schema(), None)?;
                let op = if write == 0 {
                    InsertOp::Overwrite
                } else {
                    InsertOp::Append
                };
                let plan = table.insert_into(&ctx.state(), input, op).await?;
                datafusion_physical_plan::collect(plan, ctx.task_ctx()).await?;
            }
        }
        DurableRows::CdcCheckpoints => {
            let per_batch = MEM_TIER_ROWS / 4;
            for batch in 0..4 {
                let first = batch * per_batch + 1;
                let rows = id_value_rows(first..=first + per_batch - 1)?;
                let write = table
                    .write_cdc_append_stream(common::single_batch_stream(rows), &ctx.task_ctx())
                    .await?;
                if write.has_pending_finalize() {
                    write.finish().await?;
                }
                let flushed = table.checkpoint_mem_tier().await?;
                assert!(
                    flushed > 0,
                    "{durable_rows:?}: precondition: each checkpoint must write its rows to a file"
                );
            }
        }
    }
    ctx.register_table("t", Arc::clone(&table) as Arc<dyn TableProvider>)?;

    // The durable rows span several files, so both a raw scan limit and a
    // listing cut short by file row counts could drop live rows.
    let (snapshots, files) = scan_shape(&ctx, "SELECT id FROM t").await?;
    assert!(
        files >= 2,
        "{durable_rows:?}: precondition: the durable rows must span several files, got {files}"
    );
    // Each checkpoint after the first leaves the previous one's rows in a
    // protected snapshot, which the scan reads as its own branch.
    if matches!(durable_rows, DurableRows::CdcCheckpoints) {
        assert!(
            snapshots >= 2,
            "{durable_rows:?}: precondition: the scan must read protected snapshots, got {snapshots}"
        );
    }

    if matches!(durable_rows, DurableRows::OverwriteThenReopen) {
        ctx.deregister_table("t")?;
        drop(table);
        table = Arc::new(
            CayenneTableProviderBuilder::new(Arc::clone(&catalog), ctx.runtime_env())
                .open(&name)
                .await?,
        );
        table.install_slot_advancer(Arc::new(NoopSlotAdvancer));
        ctx.register_table("t", Arc::clone(&table) as Arc<dyn TableProvider>)?;
    }

    let epoch = table
        .write_cdc_delete_keys_in_memory(&id_value_rows(1..=MEM_TIER_DELETED)?)
        .await?;
    assert!(
        epoch.is_some(),
        "{durable_rows:?}: precondition: the in-memory tier must absorb the deletes"
    );
    let live: Vec<i64> = (MEM_TIER_DELETED + 1..=MEM_TIER_ROWS).collect();
    let mut all = collected_ids(&ctx, "SELECT id FROM t").await?;
    all.sort_unstable();
    assert_eq!(
        all, live,
        "{durable_rows:?}: precondition: exactly the undeleted keys are live"
    );

    assert_eq!(
        count_star(&ctx).await?,
        i64::try_from(live.len())?,
        "{durable_rows:?}: COUNT(*) must count only live rows"
    );

    let live_count = live.len();
    for limit in [1_usize, 5, 10, 20] {
        for sql in [
            format!("SELECT id FROM t LIMIT {limit}"),
            format!("SELECT * FROM t LIMIT {limit}"),
        ] {
            let got = collected_ids(&ctx, &sql).await?;
            assert_eq!(
                got.len(),
                limit.min(live_count),
                "{durable_rows:?}: `{sql}` returned {} rows while {live_count} rows are live",
                got.len()
            );
            assert!(
                got.iter().all(|id| live.contains(id)),
                "{durable_rows:?}: `{sql}` returned a deleted key: {got:?}"
            );
        }
    }
    Ok(())
}

async fn limit_with_in_memory_cdc_deletes_after_overwrite_impl(
    fixture: TestFixture,
) -> TestResult<()> {
    limit_with_in_memory_cdc_deletes(&fixture, DurableRows::Overwrite).await
}

async fn limit_with_in_memory_cdc_deletes_after_reopen_impl(
    fixture: TestFixture,
) -> TestResult<()> {
    limit_with_in_memory_cdc_deletes(&fixture, DurableRows::OverwriteThenReopen).await
}

async fn limit_with_in_memory_cdc_deletes_after_checkpoints_impl(
    fixture: TestFixture,
) -> TestResult<()> {
    limit_with_in_memory_cdc_deletes(&fixture, DurableRows::CdcCheckpoints).await
}

test_with_backends!(limit_with_in_memory_cdc_deletes_after_overwrite_impl);
test_with_backends!(limit_with_in_memory_cdc_deletes_after_reopen_impl);
test_with_backends!(limit_with_in_memory_cdc_deletes_after_checkpoints_impl);
