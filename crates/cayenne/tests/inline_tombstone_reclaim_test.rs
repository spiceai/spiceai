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
#![allow(clippy::clone_on_ref_ptr)]

//! Reclamation of inline tombstones (`cayenne_inlined_delete`).
//!
//! An upsert writes an inline tombstone for every superseded primary key —
//! including one whose prior copy lives in a Vortex file rather than in the
//! inline corpus, so the copy is masked wherever it lives. Their only reader
//! applies them to inline entries alone, so on a workload whose rows never land
//! inline they mask nothing and are pure metastore garbage.
//!
//! Regression test for #13621: that garbage was unreclaimable. The clear was
//! gated on `cayenne_inlined_data` being non-empty, and every path that could
//! reach the clear was itself gated on a non-empty inline corpus — so on exactly
//! the workload that produces the tombstones, nothing ever removed them.

mod common;

use std::sync::Arc;

use arrow::array::{BinaryArray, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::ipc::writer::StreamWriter;
use cayenne::metadata::{CreateTableOptions, VortexConfig};
use cayenne::{CayenneTableProvider, InlinedDelete, MetadataCatalog};
use datafusion::datasource::TableProvider;
use datafusion::prelude::SessionContext;
use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};

type TestResult = Result<(), Box<dyn std::error::Error>>;

test_with_backends!(inline_tombstones_drain_when_corpus_is_empty);
test_with_backends!(file_backed_upserts_reclaim_inline_tombstones);

fn table_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]))
}

/// An Int64-PK upsert table whose writes always go to a Vortex file
/// (`inline_max_rows: 0`) — the shape that produces inline tombstones against an
/// inline corpus that is empty forever.
///
/// `pk_keyset_cache_mb: Some(0)` degrades the primary-key keyset to a bounded
/// bloom existence filter, which is what an upsert table whose keyset outgrows
/// its byte budget does in production. A bloom hit has no row location, so the
/// supersede is emitted to BOTH the file and the inline delete list — the
/// tombstone masks the prior version wherever it lives. That is the write that
/// #13621 could never reclaim.
async fn create_file_backed_upsert_table(
    fixture: &common::TestFixture,
    name: &str,
    inline_flush_max_segments: i64,
    pk_keyset_cache_mb: Option<usize>,
) -> Result<(Arc<CayenneTableProvider>, SessionContext), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    let options = CreateTableOptions {
        table_name: name.to_string(),
        schema: table_schema(),
        primary_key: vec!["id".to_string()],
        on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
            "id".to_string(),
        ]))),
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: VortexConfig {
            inline_max_rows: 0,
            inline_flush_max_segments,
            pk_keyset_cache_mb,
            ..VortexConfig::default()
        },
    };

    let table = Arc::new(
        CayenneTableProvider::create_table(
            Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>,
            options,
            ctx.runtime_env(),
        )
        .await?,
    );
    ctx.register_table(name, Arc::clone(&table) as Arc<dyn TableProvider>)?;
    Ok((table, ctx))
}

/// Serialize `keys` into the single-column `BinaryArray` IPC layout an inline
/// tombstone carries.
fn delete_ipc(keys: &[i64]) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "row_key",
        DataType::Binary,
        false,
    )]));
    let key_bytes: Vec<[u8; 8]> = keys.iter().copied().map(i64::to_be_bytes).collect();
    let key_slices: Vec<&[u8]> = key_bytes.iter().map(<[u8; 8]>::as_slice).collect();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(BinaryArray::from_vec(key_slices))],
    )?;

    let mut ipc = Vec::new();
    let mut writer = StreamWriter::try_new(&mut ipc, &schema)?;
    writer.write(&batch)?;
    writer.finish()?;
    Ok(ipc)
}

fn row_batch(ids: &[i64], names: &[&str]) -> Result<RecordBatch, Box<dyn std::error::Error>> {
    Ok(RecordBatch::try_new(
        table_schema(),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(StringArray::from(names.to_vec())),
        ],
    )?)
}

/// Read `(id, name)` back, sorted by id.
async fn read_rows(
    ctx: &SessionContext,
    name: &str,
) -> Result<Vec<(i64, String)>, Box<dyn std::error::Error>> {
    let batches = ctx
        .sql(&format!("SELECT id, name FROM {name} ORDER BY id"))
        .await?
        .collect()
        .await?;

    let mut rows = Vec::new();
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id column is Int64");
        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("name column is Utf8");
        for row in 0..batch.num_rows() {
            rows.push((ids.value(row), names.value(row).to_string()));
        }
    }
    Ok(rows)
}

/// The gate itself: a table holding inline tombstones over an EMPTY inline
/// corpus must drain to zero tombstones on a checkpoint.
///
/// Before #13621 the clear was gated on `get_inlined_data_stats().entry_count`,
/// which counts `cayenne_inlined_data` only — so this checkpoint left every
/// tombstone in place.
async fn inline_tombstones_drain_when_corpus_is_empty(fixture: common::TestFixture) -> TestResult {
    const TABLE: &str = "inline_tombstone_gate";

    let (table, ctx) = create_file_backed_upsert_table(&fixture, TABLE, 64, None).await?;
    let table_id = fixture.catalog.get_table(TABLE).await?.table_id;

    // Rows go straight to a Vortex file, so the inline corpus stays empty.
    let inserted =
        common::insert_batch(table.as_ref(), row_batch(&[1, 2, 3], &["a", "b", "c"])?).await?;
    assert_eq!(inserted, 3);
    assert_eq!(
        fixture.catalog.get_inlined_data_count(&table_id).await?,
        0,
        "inline_max_rows: 0 must keep the inline corpus empty"
    );

    // Plant published tombstones over that empty corpus — the state an upsert
    // reaches when the prior copy of a superseded PK lives in a file.
    for (batch_index, keys) in [[10_i64, 11].as_slice(), [12, 13].as_slice()]
        .iter()
        .enumerate()
    {
        fixture
            .catalog
            .add_inlined_delete(InlinedDelete {
                inlined_id: String::new(),
                table_id: table_id.clone(),
                delete_ipc: delete_ipc(keys)?,
                delete_count: 2,
                sequence_number: 100 + i64::try_from(batch_index)?,
                created_at: String::new(),
                published: true,
            })
            .await?;
    }
    assert_eq!(
        fixture.catalog.get_inlined_delete_count(&table_id).await?,
        2,
        "planted tombstones must be durable before the checkpoint"
    );

    table.checkpoint_inlined_data().await?;

    assert_eq!(
        fixture.catalog.get_inlined_delete_count(&table_id).await?,
        0,
        "a checkpoint over an empty inline corpus must reclaim the inline tombstones (#13621)"
    );

    // The tombstones masked nothing, so reclaiming them cannot change results.
    assert_eq!(
        read_rows(&ctx, TABLE).await?,
        vec![
            (1, "a".to_string()),
            (2, "b".to_string()),
            (3, "c".to_string())
        ],
        "reclaiming inert tombstones must not disturb the visible rows"
    );

    Ok(())
}

/// The reclamation must be REACHABLE from the workload that produces the
/// garbage: repeated upserts over a table whose rows only ever live in Vortex
/// files and whose keyset has degraded to a bloom.
///
/// Before the fix every inline-checkpoint trigger was gated on a non-empty inline
/// corpus, so on this workload the reclamation never ran at all and
/// `cayenne_inlined_delete` grew by a row per upsert batch without bound.
async fn file_backed_upserts_reclaim_inline_tombstones(fixture: common::TestFixture) -> TestResult {
    const TABLE: &str = "inline_tombstone_reclaim";
    // Reclaim after a handful of tombstones so the test does not have to write
    // the 64 of the shipped default.
    const FLUSH_MAX_SEGMENTS: i64 = 2;
    const ROUNDS: i64 = 8;

    let (table, ctx) =
        create_file_backed_upsert_table(&fixture, TABLE, FLUSH_MAX_SEGMENTS, Some(0)).await?;
    let table_id = fixture.catalog.get_table(TABLE).await?.table_id;

    let ids: Vec<i64> = (1..=3).collect();
    let seed: Vec<String> = ids.iter().map(|id| format!("v0_{id}")).collect();
    let seed_refs: Vec<&str> = seed.iter().map(String::as_str).collect();
    common::insert_batch(table.as_ref(), row_batch(&ids, &seed_refs)?).await?;
    assert_eq!(
        fixture.catalog.get_inlined_data_count(&table_id).await?,
        0,
        "inline_max_rows: 0 must keep the inline corpus empty"
    );

    // Each round supersedes the same PKs, whose prior copies live in files.
    let mut peak_tombstones = 0_i64;
    for round in 1..=ROUNDS {
        let names: Vec<String> = ids.iter().map(|id| format!("v{round}_{id}")).collect();
        let name_refs: Vec<&str> = names.iter().map(String::as_str).collect();
        common::insert_batch(table.as_ref(), row_batch(&ids, &name_refs)?).await?;

        peak_tombstones =
            peak_tombstones.max(fixture.catalog.get_inlined_delete_count(&table_id).await?);
    }

    assert_eq!(
        fixture.catalog.get_inlined_data_count(&table_id).await?,
        0,
        "the whole point of the workload: the inline corpus is empty throughout"
    );
    assert!(
        peak_tombstones > FLUSH_MAX_SEGMENTS,
        "the workload must push inline tombstones past the reclamation threshold, \
         or it does not cover #13621 (peaked at {peak_tombstones})"
    );

    // The reclamation runs in a background task scheduled from the write path,
    // so give it a bounded window — the same race the inline auto-checkpoint has.
    let settled = poll_tombstone_count_at_most(
        &fixture,
        &table_id,
        FLUSH_MAX_SEGMENTS,
        std::time::Duration::from_secs(10),
    )
    .await?;

    // Unreclaimed, the count is one row per round (ROUNDS); reclaimed, it cannot
    // exceed the threshold plus the row that trips it.
    assert!(
        settled <= FLUSH_MAX_SEGMENTS + 1,
        "inline tombstones must be reclaimed once they pass the threshold, not accumulate: \
         peaked at {peak_tombstones}, settled at {settled} after {ROUNDS} upsert rounds (#13621)"
    );

    // Correctness gate: the reclamation must never change what the table returns.
    let expected: Vec<(i64, String)> = ids
        .iter()
        .map(|id| (*id, format!("v{ROUNDS}_{id}")))
        .collect();
    assert_eq!(
        read_rows(&ctx, TABLE).await?,
        expected,
        "every PK must show its last upserted value, with nothing resurrected or lost"
    );

    Ok(())
}

/// Poll until the tombstone count settles at or below `limit`, returning the
/// last count observed.
async fn poll_tombstone_count_at_most(
    fixture: &common::TestFixture,
    table_id: &str,
    limit: i64,
    timeout: std::time::Duration,
) -> Result<i64, Box<dyn std::error::Error>> {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        let count = fixture.catalog.get_inlined_delete_count(table_id).await?;
        if count <= limit || std::time::Instant::now() >= deadline {
            return Ok(count);
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
}
