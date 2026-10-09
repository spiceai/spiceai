/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! A checkpoint of a sharded in-memory CDC tier must release exactly the bytes
//! its applies reserved from the process-global budget.
//!
//! A sharded apply splits each batch by PK shard, and the split copies every
//! column into per-shard allocations, each with its own buffer padding and
//! array bookkeeping — so the shards can hold more bytes than the raw batches
//! did. The shard segments record their own bytes, and a checkpoint releases
//! that figure. If the apply reserved only the raw batches' bytes, every
//! checkpoint releases more than was reserved; the release saturates at zero
//! rather than failing, so the surplus is silently taken from other tables'
//! reservations, and the budget then admits more than it holds.
//!
//! A `DoNothing` apply whose keys all exist keeps no rows and supersedes none,
//! so it appends no segment at all — nothing records the bytes reserved for it,
//! and the apply itself must hand them back.
//!
//! Its own test binary with a single test: the budget is process-global, so
//! the cases run in sequence rather than on parallel test threads.

use crate::common;

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use cayenne::metadata::{CdcDurability, CreateTableOptions, DeletionMode, VortexConfig};
use cayenne::{
    CayenneTableProvider, MetadataCatalog, SlotAdvancer, global_mem_tier_used,
    release_global_mem_tier_bytes, set_global_mem_tier_bytes, try_reserve_global_mem_tier_bytes,
};
use datafusion::prelude::*;
use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};

type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

struct NoopSlotAdvancer;
#[async_trait::async_trait]
impl SlotAdvancer for NoopSlotAdvancer {
    async fn on_checkpoint_durable(&self, _durable_epoch: u64) {}
}

const SHARDS: usize = 8;
const APPLIES: i64 = 200;
const ROWS_PER_APPLY: i64 = 10;
/// Stands in for every other table's live reservation.
const OTHER_TABLES_BYTES: u64 = 4 << 20;

#[test]
fn a_sharded_checkpoint_releases_exactly_what_its_applies_reserved() -> Result<(), String> {
    common::run_with_backend_blocking(common::BackendType::Sqlite, |fixture| async move {
        // Run every case before asserting, so one failure does not hide another.
        let mut got = Vec::new();
        for (name, on_conflict, passes) in [
            ("upsert", OnConflict::Upsert(id_column()), 1),
            // The second pass replays every key: all of it is filtered out.
            ("do_nothing_replay", OnConflict::DoNothing(id_column()), 2),
        ] {
            let result = run(&fixture, name, on_conflict, passes).await;
            set_global_mem_tier_bytes(0);
            got.push((name, result?));
        }
        let want: Vec<_> = got
            .iter()
            .map(|(name, _)| (*name, (OTHER_TABLES_BYTES, APPLIES * ROWS_PER_APPLY)))
            .collect();
        assert_eq!(
            got, want,
            "after the checkpoint, only the other tables' {OTHER_TABLES_BYTES} B stay reserved, \
             and every applied row is served (bytes reserved, rows served)"
        );
        Ok(())
    })
}

fn id_column() -> ColumnReference {
    ColumnReference::new(vec!["id".to_string()])
}

async fn run(
    fixture: &common::TestFixture,
    name: &str,
    on_conflict: OnConflict,
    passes: usize,
) -> TestResult<(u64, i64)> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("maybe", DataType::Int64, true),
        Field::new("text", DataType::Utf8, false),
    ]));
    let ctx = SessionContext::new();
    let table = Arc::new(
        CayenneTableProvider::create_table(
            Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>,
            CreateTableOptions {
                table_name: name.to_string(),
                schema: Arc::clone(&schema),
                primary_key: vec!["id".to_string()],
                on_conflict: Some(on_conflict),
                base_path: fixture.data_path.join(name).to_string_lossy().to_string(),
                partition_column: None,
                vortex_config: VortexConfig {
                    cdc_durability: CdcDurability::Memory,
                    deletion_mode: DeletionMode::Key,
                    cdc_mem_tier_shards: SHARDS,
                    // Every automatic trigger off, so the only checkpoint is the
                    // one the test drives.
                    cdc_mem_tier_max_age_ms: 0,
                    cdc_mem_tier_checkpoint_interval_ms: 0,
                    cdc_mem_tier_seal_age_ms: 0,
                    cdc_mem_tier_max_bytes: 0,
                    compaction_background_interval_ms: 3_600_000,
                    ..VortexConfig::default()
                },
            },
            ctx.runtime_env(),
        )
        .await?,
    );
    table.install_slot_advancer(Arc::new(NoopSlotAdvancer));
    ctx.register_table(name, Arc::clone(&table) as _)?;

    set_global_mem_tier_bytes(256 << 20);
    assert!(
        try_reserve_global_mem_tier_bytes(OTHER_TABLES_BYTES),
        "the stand-in reservation fits"
    );

    // Many small applies, each spread over every shard: the split's per-shard
    // overhead is largest relative to the raw batch here.
    for apply in (0..passes).flat_map(|_| 0..APPLIES) {
        let first = apply * ROWS_PER_APPLY;
        let keys = first..first + ROWS_PER_APPLY;
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from_iter_values(keys.clone())),
            Arc::new(Int64Array::from_iter(
                keys.clone().map(|k| (k % 2 == 0).then_some(k)),
            )),
            Arc::new(StringArray::from_iter_values(
                keys.map(|k| format!("{k:0>16}")),
            )),
        ];
        let batch = RecordBatch::try_new(Arc::clone(&schema), columns)?;
        let write = table
            .write_cdc_append_stream(common::single_batch_stream(batch), &ctx.task_ctx())
            .await?;
        if write.has_pending_finalize() {
            write.finish().await?;
        }
    }

    let flushed = table.checkpoint_mem_tier().await?;
    assert_eq!(
        flushed,
        u64::try_from(APPLIES * ROWS_PER_APPLY)?,
        "the checkpoint flushes every applied row out of RAM"
    );
    let used = global_mem_tier_used().expect("a budget is installed");
    release_global_mem_tier_bytes(OTHER_TABLES_BYTES);

    let batches = ctx
        .sql(&format!("SELECT COUNT(*) FROM {name}"))
        .await?
        .collect()
        .await?;
    let rows = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64 count")
        .value(0);
    Ok((used, rows))
}
