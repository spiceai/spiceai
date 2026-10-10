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

//! Reclamation of the in-memory key deletion index on a primary-key table
//! configured with an explicit `deletion_mode: position` (issue #13676).
//!
//! Such a table is not position-only: a position delete needs a known `(file
//! path, file-local position)`, and every upsert conflict without one records a
//! KEY tombstone instead. Its writes publish PROTECTED snapshots, which never
//! advance the current-snapshot file counter, and the seq-prefix bake — the
//! reclaimer for a key-delete table — declines it. So the current-snapshot full
//! rewrite is the only pass that can clear its index, and these tests drive
//! writes and maintenance to prove it reaches a bounded steady state instead of
//! growing for the life of the table.

mod common;

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use cayenne::metadata::{CreateTableOptions, DeletionMode, VortexConfig};
use cayenne::{CayenneTableProvider, MetadataCatalog};
use datafusion::prelude::SessionContext;
use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};

test_with_backends!(position_mode_deletion_index_reaches_bounded_steady_state);
test_with_backends!(key_mode_deletion_index_still_reclaims);

/// Rows per upsert round. Above `INLINE_MAX_ROWS` so each round writes a Vortex
/// file rather than landing in the inline memtable.
const ROWS_PER_ROUND: i64 = 1_500;

/// Upsert rounds driven per test. Every round after the first re-writes the same
/// key range, so it tombstones exactly `ROWS_PER_ROUND` rows.
const ROUNDS: i64 = 10;

/// Deletion-index size that triggers reclamation, lowered from the 50_000-tombstone
/// default so two rounds cross it.
const RECLAIM_TRIGGER: usize = 2_000;

/// A maintenance pass that cannot finish is the defect these tests exist to
/// catch — a position-delete pass that holds `write_lock` while waiting for it
/// wedges the table for every writer — so each pass is bounded and a breach is
/// reported as a failure rather than left to hang the suite.
const PASS_TIMEOUT: Duration = Duration::from_mins(2);

fn pk_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Utf8, false),
    ]))
}

/// `ROWS_PER_ROUND` rows over round `round`'s own slice of the key space, with a
/// round-dependent payload.
///
/// Each round takes a DISTINCT slice, which is what makes the index grow: a key
/// tombstone is per key, so re-upserting one key repeatedly replaces its single
/// entry, while superseding a fresh slice each round adds `ROWS_PER_ROUND` new
/// entries — the CDC steady state of an upsert stream walking the key space.
fn round_batch(schema: &Arc<Schema>, round: i64, generation: i64) -> RecordBatch {
    let start = round * ROWS_PER_ROUND;
    let ids: Vec<i64> = (start..start + ROWS_PER_ROUND).collect();
    let values: Vec<String> = ids
        .iter()
        .map(|row_id| format!("gen_{generation:04}_row_{row_id:020}"))
        .collect();
    RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(values)),
        ],
    )
    .expect("test batch is valid")
}

fn reclaim_config(deletion_mode: DeletionMode) -> VortexConfig {
    VortexConfig {
        deletion_mode,
        bake_deletion_index_trigger: RECLAIM_TRIGGER,
        // Inlining off so each round materializes a Vortex file: position deletes
        // apply to files, and an inlined row takes the inline-rewrite path instead.
        inline_max_rows: 0,
        inline_max_bytes: 0,
        inline_max_buffer_bytes: 0,
        // Stops the interval scheduler so the tests drive maintenance explicitly
        // and the index readings are deterministic.
        compaction_background_interval_ms: 0,
        ..VortexConfig::default()
    }
}

async fn build_table(
    fixture: &common::TestFixture,
    name: &str,
    schema: &Arc<Schema>,
    deletion_mode: DeletionMode,
) -> Result<Arc<CayenneTableProvider>, Box<dyn std::error::Error>> {
    let options = CreateTableOptions {
        table_name: name.to_string(),
        schema: Arc::clone(schema),
        primary_key: vec!["id".to_string()],
        on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
            "id".to_string(),
        ]))),
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: reclaim_config(deletion_mode),
    };

    let catalog: Arc<dyn MetadataCatalog> = fixture.catalog.clone();
    let ctx = SessionContext::new();
    Ok(Arc::new(
        CayenneTableProvider::create_table(catalog, options, ctx.runtime_env()).await?,
    ))
}

/// Drive `ROUNDS` upsert rounds, running a maintenance pass after each, and
/// return the deletion-index length observed after every round.
async fn drive_rounds(
    table: &Arc<CayenneTableProvider>,
    schema: &Arc<Schema>,
) -> Result<Vec<usize>, Box<dyn std::error::Error>> {
    // Seed every slice first, so the upsert rounds below all supersede an
    // existing row rather than appending a new one.
    for round in 0..ROUNDS {
        tokio::time::timeout(
            PASS_TIMEOUT,
            common::insert_batch(table, round_batch(schema, round, 0)),
        )
        .await
        .map_err(|_| format!("seeding round {round} did not finish within {PASS_TIMEOUT:?}"))??;
    }

    let mut observed =
        Vec::with_capacity(usize::try_from(ROUNDS).expect("round count fits in usize"));
    for round in 0..ROUNDS {
        tokio::time::timeout(
            PASS_TIMEOUT,
            common::insert_batch(table, round_batch(schema, round, 1)),
        )
        .await
        .map_err(|_| {
            format!("upsert round {round} did not finish within {PASS_TIMEOUT:?}: a maintenance pass is holding the write lock")
        })??;

        tokio::time::timeout(PASS_TIMEOUT, table.compact_current_snapshot_small_files())
            .await
            .map_err(|_| {
                format!("maintenance pass after round {round} did not finish within {PASS_TIMEOUT:?}: the pass deadlocked against its own rewrite")
            })??;

        tokio::time::timeout(PASS_TIMEOUT, table.drain_in_flight_maintenance())
            .await
            .map_err(|_| {
                format!("background maintenance after round {round} did not drain within {PASS_TIMEOUT:?}")
            })??;

        observed.push(table.deletion_index_len());
    }
    Ok(observed)
}

/// Row count via `SELECT COUNT(*)`, so reclamation is checked against results and
/// not only against the index counter.
async fn count_rows(table: &Arc<CayenneTableProvider>, name: &str) -> i64 {
    let ctx = SessionContext::new();
    ctx.register_table(
        name,
        Arc::clone(table) as Arc<dyn datafusion::datasource::TableProvider>,
    )
    .expect("register table");
    let batches = ctx
        .sql(&format!("SELECT COUNT(*) FROM {name}"))
        .await
        .expect("count sql planned")
        .collect()
        .await
        .expect("count collected");
    let merged =
        arrow::compute::concat_batches(&batches[0].schema(), &batches).expect("concat batches");
    merged
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count column")
        .value(0)
}

/// An explicit `deletion_mode: position` primary-key table under repeated upserts
/// reclaims its key deletion index instead of accumulating one tombstone per
/// superseded row for the life of the table.
async fn position_mode_deletion_index_reaches_bounded_steady_state(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = pk_schema();
    let table = build_table(&fixture, "pos_reclaim", &schema, DeletionMode::Position).await?;

    let observed = drive_rounds(&table, &schema).await?;

    let rows_per_round = usize::try_from(ROWS_PER_ROUND).expect("row count fits in usize");
    let peak = observed.iter().copied().max().unwrap_or(0);

    // Without reclamation the index holds one tombstone per superseded row, so it
    // would end at ROUNDS * ROWS_PER_ROUND. Bound the steady state at the trigger
    // plus the one round that can accumulate on top of it before the next pass
    // runs.
    let bound = RECLAIM_TRIGGER + rows_per_round;
    assert!(
        peak <= bound,
        "deletion index grew past its steady state: peak {peak} > {bound}, observations {observed:?}"
    );

    // Guard the assertion above against passing for the wrong reason: it must have
    // taken a reclamation to stay under the bound, not a workload that never
    // reached the trigger in the first place.
    assert!(
        observed.iter().any(|len| *len >= RECLAIM_TRIGGER)
            || observed.windows(2).any(|pair| pair[1] < pair[0]),
        "the index never crossed the reclaim trigger and never shrank, so this test proved nothing: {observed:?}"
    );

    // Reclamation must not change what the table returns: every round rewrote the
    // same key range, so exactly one row per key survives.
    assert_eq!(
        count_rows(&table, "pos_reclaim").await,
        ROUNDS * ROWS_PER_ROUND,
        "reclamation changed the visible row set"
    );

    Ok(())
}

/// The same workload in `deletion_mode: key`, which reclaims through the
/// seq-prefix bake. Pins that the position-mode path added for #13676 did not
/// take over the cheaper key-mode one, and that both modes agree on results.
async fn key_mode_deletion_index_still_reclaims(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = pk_schema();
    let table = build_table(&fixture, "key_reclaim", &schema, DeletionMode::Key).await?;

    let observed = drive_rounds(&table, &schema).await?;

    let rows_per_round = usize::try_from(ROWS_PER_ROUND).expect("row count fits in usize");
    let unbounded = usize::try_from(ROUNDS).expect("round count fits in usize") * rows_per_round;
    let peak = observed.iter().copied().max().unwrap_or(0);
    assert!(
        peak < unbounded,
        "key-mode deletion index accumulated every tombstone: peak {peak}, observations {observed:?}"
    );
    assert!(
        observed.iter().any(|len| *len >= RECLAIM_TRIGGER)
            || observed.windows(2).any(|pair| pair[1] < pair[0]),
        "the index never crossed the reclaim trigger and never shrank, so this test proved nothing: {observed:?}"
    );

    assert_eq!(
        count_rows(&table, "key_reclaim").await,
        ROUNDS * ROWS_PER_ROUND,
        "key-mode reclamation changed the visible row set"
    );

    Ok(())
}
