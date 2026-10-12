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
//! indexed files, though the fold rewrites every file the load wrote.

mod common;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use cayenne::metadata::DeletionMode;
use common::lookup_index::{
    TableSpec, counters, file_mode_config, insert, int64_column, open_table, query,
};
use datafusion::execution::runtime_env::RuntimeEnv;
use std::sync::Arc;

/// Two files of 1.2M rows each: more rows per file than a write may leave
/// for its index to finish in the background, so a fold that did would publish
/// files it had not indexed yet.
const ROWS: i64 = 2_400_000;

/// Every 1000th row repeats the key of the row half its position, an earlier
/// row, so the repeats spread across every file of the load.
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

    let ids: Vec<i64> = (0..ROWS)
        .map(|row| {
            if row % REPEAT_EVERY == REPEAT_EVERY - 1 {
                row / 2
            } else {
                row
            }
        })
        .collect();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from((0..ROWS).collect::<Vec<_>>())),
            Arc::new(StringArray::from(
                (0..ROWS)
                    .map(|row| format!("{row:016x}"))
                    .collect::<Vec<_>>(),
            )),
        ],
    )?;
    insert(&provider, "loaded", batch).await;

    // Row 1999 repeats the key of row 999, so the later copy wins.
    let before = counters(&provider);
    let rows = query(
        &provider,
        "loaded",
        "SELECT payload FROM loaded WHERE id = 999",
    )
    .await;
    let after = counters(&provider);
    assert_eq!(int64_column(&rows), vec![1999]);
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
