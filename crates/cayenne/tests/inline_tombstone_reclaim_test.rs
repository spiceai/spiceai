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
//! the workload that produces the tombstones, nothing ever removed them. The two
//! tests below cover those two gates; each fails without its own half of the fix.

mod common;

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use cayenne::metadata::{CreateTableOptions, VortexConfig};
use cayenne::{CayenneTableProvider, MetadataCatalog};
use datafusion::datasource::TableProvider;
use datafusion::prelude::SessionContext;
use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};

type TestResult = Result<(), Box<dyn std::error::Error>>;

test_with_backends!(inline_tombstones_drain_when_corpus_is_empty);
test_with_backends!(file_backed_upserts_reclaim_inline_tombstones);

/// Tombstone budget for the reclamation test, in metastore bytes — the shipped
/// default is megabytes.
///
/// One tombstone here costs `INLINED_DELETE_ROW_OVERHEAD_BYTES` (128) plus a
/// 25-byte payload (a format tag and three 8-byte keys), so 153. The budget sits
/// above one and below two, which is what makes the test deterministic: the first
/// round leaves an observable row because it cannot yet arm the reclamation, and
/// the second crosses the budget. A budget under the per-row overhead would arm
/// on the very first tombstone and could clear it before the count is sampled.
const TOMBSTONE_BUDGET_BYTES: i64 = 256;

fn table_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]))
}

/// An Int64-PK upsert table whose writes always go to a Vortex file
/// (`inline_max_rows: 0`) and whose primary-key keyset is a bloom existence
/// filter (`pk_keyset_cache_mb: 0`) — what an upsert table whose keyset outgrows
/// its byte budget degrades to in production.
///
/// That combination is what writes inline tombstones against an inline corpus
/// that is empty forever: a bloom hit has no row location, so the supersede goes
/// to BOTH the file and the inline delete list, masking the prior version
/// wherever it lives. Those are the rows #13621 could never reclaim.
async fn create_file_backed_upsert_table(
    fixture: &common::TestFixture,
    name: &str,
    inline_flush_max_bytes: i64,
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
            inline_flush_max_bytes,
            pk_keyset_cache_mb: Some(0),
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

fn row_batch(ids: &[i64], names: &[&str]) -> Result<RecordBatch, Box<dyn std::error::Error>> {
    Ok(RecordBatch::try_new(
        table_schema(),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(StringArray::from(names.to_vec())),
        ],
    )?)
}

/// Upsert `ids` with values tagged `round`, so each round supersedes the last.
async fn upsert_round(
    table: &CayenneTableProvider,
    ids: &[i64],
    round: i64,
) -> Result<(), Box<dyn std::error::Error>> {
    let names: Vec<String> = ids.iter().map(|id| format!("v{round}_{id}")).collect();
    let name_refs: Vec<&str> = names.iter().map(String::as_str).collect();
    common::insert_batch(table, row_batch(ids, &name_refs)?).await?;
    Ok(())
}

/// The value `upsert_round` leaves behind, for the assertion side.
fn expected_rows(ids: &[i64], round: i64) -> Vec<(i64, String)> {
    ids.iter()
        .map(|id| (*id, format!("v{round}_{id}")))
        .collect()
}

async fn tombstone_count(
    fixture: &common::TestFixture,
    table_id: &str,
) -> Result<i64, Box<dyn std::error::Error>> {
    Ok(fixture
        .catalog
        .get_inlined_data_stats(table_id)
        .await?
        .tombstone_entry_count)
}

async fn corpus_count(
    fixture: &common::TestFixture,
    table_id: &str,
) -> Result<i64, Box<dyn std::error::Error>> {
    Ok(fixture.catalog.get_inlined_data_count(table_id).await?)
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

/// The gate itself: a checkpoint over an EMPTY inline corpus must drain the
/// tombstones the upserts left behind.
///
/// Before #13621 the clear was gated on `get_inlined_data_stats().entry_count`,
/// which described `cayenne_inlined_data` alone — so this checkpoint left every
/// tombstone in place. The budget here is the shipped default, far above what
/// two rounds write, so nothing is reclaimed until the explicit checkpoint: this
/// test isolates the gate from the trigger the next one covers.
async fn inline_tombstones_drain_when_corpus_is_empty(fixture: common::TestFixture) -> TestResult {
    const TABLE: &str = "inline_tombstone_gate";
    const ROUNDS: i64 = 2;

    let (table, ctx) = create_file_backed_upsert_table(
        &fixture,
        TABLE,
        VortexConfig::default().inline_flush_max_bytes,
    )
    .await?;
    let table_id = fixture.catalog.get_table(TABLE).await?.table_id;

    let ids: Vec<i64> = (1..=3).collect();
    for round in 0..=ROUNDS {
        upsert_round(table.as_ref(), &ids, round).await?;
    }

    assert_eq!(
        corpus_count(&fixture, &table_id).await?,
        0,
        "inline_max_rows: 0 must keep the inline corpus empty"
    );
    assert_eq!(
        tombstone_count(&fixture, &table_id).await?,
        ROUNDS,
        "each superseding round must leave one inline tombstone over that empty corpus"
    );

    table.checkpoint_inlined_data().await?;

    assert_eq!(
        tombstone_count(&fixture, &table_id).await?,
        0,
        "a checkpoint over an empty inline corpus must reclaim the inline tombstones (#13621)"
    );

    // The tombstones masked nothing, so reclaiming them cannot change results.
    assert_eq!(
        read_rows(&ctx, TABLE).await?,
        expected_rows(&ids, ROUNDS),
        "reclaiming inert tombstones must not disturb the visible rows"
    );

    Ok(())
}

/// The reclamation must be REACHABLE from the workload that produces the
/// garbage, with no explicit checkpoint to help it.
///
/// Before the fix every inline-checkpoint trigger was gated on a non-empty inline
/// corpus, so on this workload the reclamation never ran at all and
/// `cayenne_inlined_delete` grew by a row per upsert batch without bound.
async fn file_backed_upserts_reclaim_inline_tombstones(fixture: common::TestFixture) -> TestResult {
    const TABLE: &str = "inline_tombstone_reclaim";
    const ROUNDS: i64 = 8;

    let (table, ctx) =
        create_file_backed_upsert_table(&fixture, TABLE, TOMBSTONE_BUDGET_BYTES).await?;
    let table_id = fixture.catalog.get_table(TABLE).await?.table_id;

    let ids: Vec<i64> = (1..=3).collect();
    let mut peak_tombstones = 0_i64;
    for round in 0..=ROUNDS {
        upsert_round(table.as_ref(), &ids, round).await?;
        peak_tombstones = peak_tombstones.max(tombstone_count(&fixture, &table_id).await?);
    }

    assert_eq!(
        corpus_count(&fixture, &table_id).await?,
        0,
        "the whole point of the workload: the inline corpus is empty throughout"
    );
    assert!(
        peak_tombstones > 0,
        "the workload must write inline tombstones, or it does not cover #13621"
    );

    // Every round after the seed supersedes the same PKs, so each writes exactly
    // one tombstone row: unreclaimed, the count is ROUNDS and never falls. That
    // is the count to beat — not the observed peak, which depends on when the
    // background reclamation happens to land relative to the samples.
    let settled =
        common::poll_inlined_delete_count_at_most(&fixture.catalog, &table_id, ROUNDS - 1).await?;

    assert!(
        settled < ROUNDS,
        "inline tombstones must be reclaimed once they pass the budget, not accumulate: \
         peaked at {peak_tombstones}, settled at {settled} after {ROUNDS} upsert rounds (#13621)"
    );

    // Correctness gate: the reclamation must never change what the table returns.
    assert_eq!(
        read_rows(&ctx, TABLE).await?,
        expected_rows(&ids, ROUNDS),
        "every PK must show its last upserted value, with nothing resurrected or lost"
    );

    Ok(())
}
