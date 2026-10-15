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

//! A first load into an indexed table that repeats keys is covered by its
//! secondary index the moment it returns: the first lookup after it reads only
//! indexed files, though the fold rewrote every file the load wrote.

use crate::common;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use cayenne::metadata::DeletionMode;
use common::lookup_index::{
    TableSpec, counters, file_mode_config, int64_column, open_table, query,
};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion_expr::dml::InsertOp;
use std::sync::Arc;

/// Rows per input batch. The load hands whole batches to its two shards in
/// turn, so each of its two files holds 12 of the 24 batches, 1.2M rows: more
/// rows than a write may leave for its index to finish in the background, so a
/// fold that did would publish files it had not indexed yet.
const BATCH_ROWS: i64 = 100_000;
const BATCHES: i64 = 24;

/// In every batch after the first, each row at position 999 mod 1000 repeats
/// the key of the row at 500 mod 1000 of the batch before it, so both files
/// hold superseded copies and the fold rewrites both.
const REPEAT_EVERY: i64 = 1000;

test_with_backends!(bulk_load_with_repeats_is_indexed_when_it_returns_impl);

async fn bulk_load_with_repeats_is_indexed_when_it_returns_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("payload", DataType::Int64, false),
        Field::new("label", DataType::Utf8, false),
    ]));
    let mut config = file_mode_config();
    config.deletion_mode = DeletionMode::Key;
    config.inline_max_rows = 0;
    config.inline_max_buffer_bytes = 0;
    config.target_vortex_file_size_mb = 512;
    config.write_concurrency = Some(2);
    config.compaction_background_interval_ms = 0;
    config.compaction_trigger_files = usize::MAX;
    let provider = open_table(
        &fixture,
        Arc::new(RuntimeEnv::default()),
        TableSpec::new("loaded", Arc::clone(&schema), &[&["id"]])
            .config(config)
            .upsert_key("id"),
    )
    .await;

    let batches = (0..BATCHES)
        .map(|batch| {
            let rows = batch * BATCH_ROWS..(batch + 1) * BATCH_ROWS;
            let ids: Vec<i64> = rows
                .clone()
                .map(|row| {
                    if batch > 0 && row % REPEAT_EVERY == REPEAT_EVERY - 1 {
                        row - BATCH_ROWS - (REPEAT_EVERY - 1 - 500)
                    } else {
                        row
                    }
                })
                .collect();
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(ids)),
                    Arc::new(Int64Array::from(rows.clone().collect::<Vec<_>>())),
                    Arc::new(StringArray::from(
                        rows.map(|row| format!("{row:016x}")).collect::<Vec<_>>(),
                    )),
                ],
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    common::write_batches(&provider, batches, InsertOp::Append).await?;

    // Key 500 is written by row 500 of the first batch and repeated by row
    // 100,999 of the second, a copy in the other file, so the later one wins.
    let before = counters(&provider);
    let rows = query(
        &provider,
        "loaded",
        "SELECT payload FROM loaded WHERE id = 500",
    )
    .await;
    let after = counters(&provider);
    assert_eq!(int64_column(&rows), vec![BATCH_ROWS + 999]);
    assert_eq!(
        (
            after.none - before.none,
            after.partial - before.partial,
            after.full - before.full,
        ),
        (0, 0, 1),
        "the first lookup after the load must read only indexed files (none, partial, full)"
    );

    let verification = provider.verify_lookup_index_against_read_back().await?;
    assert!(verification.agrees(), "{verification:?}");
    assert_eq!(verification.uncovered_files, 0, "{verification:?}");
    assert!(
        verification.files > 1,
        "the load must write several files, got {verification:?}"
    );
    Ok(())
}
