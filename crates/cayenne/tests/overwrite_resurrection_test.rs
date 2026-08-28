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

//! Reproducers for an overwrite/delete/re-upsert resurrection bug: after
//! `INSERT OVERWRITE` creates a row, deleting it and then re-upserting the same
//! key loses the re-upsert — the row stays hidden, as if the delete tombstone
//! still applied. No compaction is involved.
//!
//! Root cause (key-delete path): the upsert's on-conflict insert-record is only
//! recorded for keys present in the visible PK existence index. `INSERT
//! OVERWRITE` clears that index, so the re-inserted key (which now carries only
//! a pending delete tombstone) takes the plain-insert path and records no
//! insert-record, leaving it hidden (a row is visible iff `insert_seq >
//! delete_seq`). The fix records an insert-record for any re-inserted key that
//! has a pending tombstone.
//!
//! Run per deletion mode so both the key-index and position paths are covered.

#![allow(clippy::expect_used)]
#![allow(clippy::clone_on_ref_ptr)]

mod common;

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use cayenne::metadata::{CreateTableOptions, DeletionMode, VortexConfig};
use cayenne::{CayenneTableProvider, CayenneTableProviderBuilder, MetadataCatalog};
use common::TestFixture;
use datafusion::datasource::TableProvider;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::{col, lit};
use datafusion_expr::dml::InsertOp;
use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};

type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

#[derive(Clone, Copy, Debug)]
enum Mode {
    /// Explicit `deletion_mode: key` — the deletion index is authoritative.
    Key,
    /// Explicit `deletion_mode: position` (merge-on-read position deletes).
    Position,
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

fn config(mode: Mode) -> VortexConfig {
    // `inline_max_rows: 0` so every write lands as a snapshot file (matches the
    // shape that surfaced the bug).
    let base = VortexConfig {
        inline_max_rows: 0,
        ..VortexConfig::default()
    };
    match mode {
        Mode::Key => VortexConfig {
            deletion_mode: DeletionMode::Key,
            ..base
        },
        Mode::Position => VortexConfig {
            deletion_mode: DeletionMode::Position,
            ..base
        },
    }
}

async fn create_table(
    fixture: &TestFixture,
    name: &str,
    mode: Mode,
) -> TestResult<(Arc<CayenneTableProvider>, SessionContext)> {
    create_table_with(fixture, name, config(mode)).await
}

async fn create_table_with(
    fixture: &TestFixture,
    name: &str,
    vortex_config: VortexConfig,
) -> TestResult<(Arc<CayenneTableProvider>, SessionContext)> {
    let opts = CreateTableOptions {
        table_name: name.to_string(),
        schema: schema(),
        primary_key: vec!["id".to_string()],
        on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
            "id".to_string(),
        ]))),
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config,
    };
    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let ctx = SessionContext::new();
    let table =
        Arc::new(CayenneTableProvider::create_table(catalog, opts, ctx.runtime_env()).await?);
    ctx.register_table(name, Arc::clone(&table) as Arc<dyn TableProvider>)?;
    Ok((table, ctx))
}

/// Reopen an existing table from the catalog, rebuilding all in-memory state
/// (including the PK existence index) from durable metadata.
async fn reopen_table(
    fixture: &TestFixture,
    name: &str,
) -> TestResult<(Arc<CayenneTableProvider>, SessionContext)> {
    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let ctx = SessionContext::new();
    let table = Arc::new(
        CayenneTableProviderBuilder::new(catalog, ctx.runtime_env())
            .open(name)
            .await?,
    );
    ctx.register_table(name, Arc::clone(&table) as Arc<dyn TableProvider>)?;
    Ok((table, ctx))
}

fn batch(rows: &[(i64, i64)]) -> RecordBatch {
    let ids: Vec<i64> = rows.iter().map(|(k, _)| *k).collect();
    let vals: Vec<i64> = rows.iter().map(|(_, v)| *v).collect();
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(vals)),
        ],
    )
    .expect("valid batch")
}

async fn overwrite(table: &Arc<CayenneTableProvider>, rows: &[(i64, i64)]) -> TestResult<()> {
    let ctx = SessionContext::new();
    let exec = MemorySourceConfig::try_new_exec(&[vec![batch(rows)]], schema(), None)?;
    let plan = table
        .insert_into(&ctx.state(), exec, InsertOp::Overwrite)
        .await?;
    datafusion_physical_plan::collect(plan, ctx.task_ctx()).await?;
    Ok(())
}

async fn upsert(table: &Arc<CayenneTableProvider>, key: i64, value: i64) -> TestResult<()> {
    common::insert_batch(table.as_ref(), batch(&[(key, value)])).await?;
    Ok(())
}

async fn delete_key(table: &Arc<CayenneTableProvider>, key: i64) -> TestResult<()> {
    let ctx = SessionContext::new();
    let plan = table
        .delete_from(&ctx.state(), vec![col("id").eq(lit(key))])
        .await?;
    datafusion_physical_plan::collect(plan, ctx.task_ctx()).await?;
    Ok(())
}

async fn read_value(ctx: &SessionContext, name: &str, key: i64) -> TestResult<Option<i64>> {
    let df = ctx
        .sql(&format!("SELECT value FROM {name} WHERE id = {key}"))
        .await?;
    let results = df.collect().await?;

    let mut values: Vec<i64> = Vec::new();
    for b in &results {
        if b.num_rows() == 0 {
            continue;
        }
        let v = b
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64");
        for i in 0..v.len() {
            values.push(v.value(i));
        }
    }

    match values.as_slice() {
        [] => Ok(None),
        [single] => Ok(Some(*single)),
        _ => Err(format!(
            "Expected at most one row for id {key}, got {}",
            values.len()
        )
        .into()),
    }
}

/// Row created via OVERWRITE, deleted, then re-upserted. Expected: present.
async fn reupsert_after_deleting_overwritten_row(
    fixture: &TestFixture,
    mode: Mode,
) -> TestResult<()> {
    let (table, ctx) = create_table(fixture, "ov_single", mode).await?;
    overwrite(&table, &[(1, 100)]).await?;
    delete_key(&table, 1).await?;
    upsert(&table, 1, 200).await?;
    assert_eq!(
        read_value(&ctx, "ov_single", 1).await?,
        Some(200),
        "{mode:?}: re-upsert after deleting an OVERWRITE-created row must resurrect it"
    );
    Ok(())
}

/// Same, with a second untouched key, to confirm only the re-upserted key is affected.
async fn reupsert_after_deleting_overwritten_row_multi_key(
    fixture: &TestFixture,
    mode: Mode,
) -> TestResult<()> {
    let (table, ctx) = create_table(fixture, "ov_multi", mode).await?;
    overwrite(&table, &[(1, 100), (2, 200)]).await?;
    delete_key(&table, 1).await?;
    upsert(&table, 1, 999).await?;
    assert_eq!(
        read_value(&ctx, "ov_multi", 1).await?,
        Some(999),
        "{mode:?}: key 1 re-upsert lost"
    );
    assert_eq!(
        read_value(&ctx, "ov_multi", 2).await?,
        Some(200),
        "{mode:?}: key 2 must remain"
    );
    Ok(())
}

/// Control: row created via UPSERT (not overwrite), deleted, re-upserted. Always
/// worked; included so a regression there would also be caught.
async fn reupsert_after_deleting_upserted_row_control(
    fixture: &TestFixture,
    mode: Mode,
) -> TestResult<()> {
    let (table, ctx) = create_table(fixture, "up_single", mode).await?;
    upsert(&table, 1, 100).await?;
    delete_key(&table, 1).await?;
    upsert(&table, 1, 200).await?;
    assert_eq!(
        read_value(&ctx, "up_single", 1).await?,
        Some(200),
        "{mode:?}: control re-upsert after deleting an UPSERT-created row must resurrect it"
    );
    Ok(())
}

async fn run_all(fixture: TestFixture, mode: Mode) -> TestResult<()> {
    reupsert_after_deleting_overwritten_row(&fixture, mode).await?;
    reupsert_after_deleting_overwritten_row_multi_key(&fixture, mode).await?;
    reupsert_after_deleting_upserted_row_control(&fixture, mode).await?;
    Ok(())
}

async fn overwrite_resurrection_key_impl(fixture: TestFixture) -> TestResult<()> {
    run_all(fixture, Mode::Key).await
}
async fn overwrite_resurrection_position_impl(fixture: TestFixture) -> TestResult<()> {
    run_all(fixture, Mode::Position).await
}

test_with_backends!(overwrite_resurrection_key_impl);
test_with_backends!(overwrite_resurrection_position_impl);

// ===========================================================================
// Bloom existence-fallback variant
// ===========================================================================
//
// An over-budget upsert table falls back from the exact PK keyset to a bounded
// bloom existence filter (`PkExistenceRef::Bloom`). The reinsert-over-tombstone
// probe has to run on the bloom MISS path too: a key absent from the bloom but
// still carrying a pending DELETE tombstone otherwise takes the plain-insert
// path, records no `insert_seq`, and stays hidden — the same resurrection bug,
// on the bloom fallback. The per-mode tests above never reach this; their tiny
// keyset stays exact, so this gap was invisible until now.
//
// Forcing the bloom path deterministically:
//   * `pk_keyset_cache_mb: 0` converts any non-empty keyset to a bloom (a 64-bit
//     floor — low false-positive rate for the handful of keys here).
//   * A delete cannot be removed from a bloom in place, so we REOPEN the table:
//     the existence index is rebuilt from the post-delete live keyset (just the
//     sentinel key), which therefore excludes the victims while their tombstones
//     stay durable. A warm-up upsert of the sentinel materializes that bloom, so
//     the victim re-upserts below are checked against it (a MISS), not a freshly
//     built exact keyset.
// Several victims are re-upserted: the early ones are true bloom misses (the
// fix's target); a late false-positive hit is still resolved correctly, so the
// assertion holds either way, while a regression loses the true misses.

fn bloom_config() -> VortexConfig {
    VortexConfig {
        deletion_mode: DeletionMode::Key,
        pk_keyset_cache_mb: Some(0),
        inline_max_rows: 0,
        ..VortexConfig::default()
    }
}

async fn reupsert_over_tombstone_survives_bloom_fallback_impl(
    fixture: TestFixture,
) -> TestResult<()> {
    let name = "bloom_resurrect";
    let sentinel = 0;
    let victims: Vec<i64> = (1..=16).collect();

    {
        let (table, _ctx) = create_table_with(&fixture, name, bloom_config()).await?;
        // Sentinel stays live so the keyset is never empty (an empty keyset is
        // kept exact, not converted to a bloom).
        upsert(&table, sentinel, 1).await?;
        for &k in &victims {
            upsert(&table, k, k * 10).await?;
        }
        for &k in &victims {
            delete_key(&table, k).await?;
        }
    }

    // Reopen: existence index is rebuilt from the live keyset ({sentinel}); the
    // victims' tombstones survive in the durable deletion index.
    let (table2, ctx2) = reopen_table(&fixture, name).await?;
    // Materialize the bloom from the rebuilt {sentinel} keyset.
    upsert(&table2, sentinel, 2).await?;
    // Each victim re-upsert is a bloom MISS with a pending tombstone.
    for &k in &victims {
        upsert(&table2, k, k * 100).await?;
    }

    for &k in &victims {
        assert_eq!(
            read_value(&ctx2, name, k).await?,
            Some(k * 100),
            "key {k} lost after reinsert-over-tombstone on the bloom existence-fallback path"
        );
    }
    Ok(())
}

test_with_backends!(reupsert_over_tombstone_survives_bloom_fallback_impl);
