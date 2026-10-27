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

//! A warm-to-datalake promotion writes only the warm tier's rows and never
//! rewrites a datalake file that is already registered, even when upserts and
//! deletes have superseded rows inside it.
//!
//! The superseded datalake rows stay physically present and are hidden at read
//! time by the tombstones that superseded them, so every assertion on query
//! results is exact against an in-memory oracle of the same operation history:
//! one row per key carrying its newest value, the exact live count, point
//! lookups, predicate deletes that match only a superseded value, and the same
//! answers again after the table is reopened from the metastore.

use crate::common;

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use arrow::array::{Int64Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use cayenne::metadata::{ColdTierFile, CreateTableOptions, DeletionMode, VortexConfig};
use cayenne::{CayenneCatalog, CayenneTableProvider, CayenneTableProviderBuilder, MetadataCatalog};
use datafusion::datasource::TableProvider;
use datafusion::prelude::*;
use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};

type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

const TABLE: &str = "warm_only_t";

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

/// An upsert table with the datalake tier on a local `file://` store, promoted
/// whenever the warm tier holds any file so each promotion is deterministic.
fn table_options(fixture: &common::TestFixture, cold_dir: &std::path::Path) -> CreateTableOptions {
    CreateTableOptions {
        table_name: TABLE.to_string(),
        schema: schema(),
        primary_key: vec!["id".to_string()],
        on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
            "id".to_string(),
        ]))),
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: VortexConfig {
            cold_tier_location: Some(format!("file://{}", cold_dir.to_string_lossy())),
            cluster_by: vec!["id".to_string()],
            cold_tier_warm_max_files: 1,
            cold_target_file_size_mb: 16,
            // No background promoter or GC timer: every promotion in this test
            // is the explicit call below.
            cold_tier_background_interval_ms: 0,
            cold_tier_gc_interval_ms: 300_000,
            deletion_mode: DeletionMode::Key,
            ..VortexConfig::default()
        },
    }
}

/// Upsert `rows` and record them in the oracle.
async fn upsert(
    table: &CayenneTableProvider,
    oracle: &mut BTreeMap<i64, i64>,
    rows: &[(i64, i64)],
) -> TestResult<()> {
    let batch = RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|(_, value)| *value).collect::<Vec<_>>(),
            )),
        ],
    )?;
    common::insert_batch(table, batch).await?;
    for (id, value) in rows {
        oracle.insert(*id, *value);
    }
    Ok(())
}

/// Run a `DELETE ... WHERE <predicate>` and return the rows-affected count the
/// statement reports.
async fn delete_where(table: &CayenneTableProvider, predicate: Expr) -> TestResult<u64> {
    let ctx = SessionContext::new();
    let plan = table.delete_from(&ctx.state(), vec![predicate]).await?;
    let batches = datafusion::physical_plan::collect(plan, ctx.task_ctx()).await?;
    Ok(batches
        .first()
        .and_then(|b| b.column(0).as_any().downcast_ref::<UInt64Array>())
        .and_then(|a| a.values().first())
        .copied()
        .unwrap_or(0))
}

/// Flush the in-RAM and inline tiers into durable warm files, so the promotion
/// trigger sees them.
async fn flush_warm(table: &CayenneTableProvider) {
    let _ = table.checkpoint_inlined_data().await;
    let _ = table.checkpoint_mem_tier().await;
}

async fn select_pairs(ctx: &SessionContext, sql: &str) -> TestResult<Vec<(i64, i64)>> {
    let batches = ctx.sql(sql).await?.collect().await?;
    let mut rows = Vec::new();
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or("id column must be Int64")?;
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or("value column must be Int64")?;
        for row in 0..batch.num_rows() {
            rows.push((ids.value(row), values.value(row)));
        }
    }
    Ok(rows)
}

async fn count_star(ctx: &SessionContext) -> TestResult<i64> {
    let batches = ctx
        .sql(&format!("SELECT COUNT(*) FROM {TABLE}"))
        .await?
        .collect()
        .await?;
    batches
        .first()
        .and_then(|b| b.column(0).as_any().downcast_ref::<Int64Array>())
        .and_then(|a| a.values().first())
        .copied()
        .ok_or_else(|| "COUNT(*) returned no value".into())
}

/// Every query answer this test checks, against the oracle: the full ordered
/// row set (one row per key, newest value), the exact count, and point lookups
/// on keys whose datalake copies were superseded.
async fn assert_matches_oracle(
    ctx: &SessionContext,
    oracle: &BTreeMap<i64, i64>,
    lookups: &[i64],
    phase: &str,
) -> TestResult<()> {
    let expected: Vec<(i64, i64)> = oracle.iter().map(|(id, value)| (*id, *value)).collect();
    let actual = select_pairs(ctx, &format!("SELECT id, value FROM {TABLE} ORDER BY id")).await?;
    assert_eq!(
        actual, expected,
        "{phase}: every key must appear exactly once carrying its newest value"
    );
    assert_eq!(
        count_star(ctx).await?,
        i64::try_from(oracle.len())?,
        "{phase}: COUNT(*) must equal the number of live keys"
    );
    for id in lookups {
        let expected: Vec<(i64, i64)> = oracle.get(id).map(|v| (*id, *v)).into_iter().collect();
        assert_eq!(
            select_pairs(
                ctx,
                &format!("SELECT id, value FROM {TABLE} WHERE id = {id}")
            )
            .await?,
            expected,
            "{phase}: point lookup of id {id} must return only its newest version"
        );
    }
    Ok(())
}

async fn manifest(
    catalog: &CayenneCatalog,
    table: &CayenneTableProvider,
) -> TestResult<Vec<ColdTierFile>> {
    Ok(catalog.list_cold_tier_files(table.table_id()).await?)
}

/// The promotion that produced `after` from `before` must have carried every
/// datalake file of `before` forward untouched — same URL, same row count — and
/// written new files holding exactly `promoted_rows` rows, the warm tier's live
/// rows. A promotion that rewrote a datalake file drops its URL from the
/// manifest and re-writes its rows into the new files.
fn assert_carried_forward(
    before: &[ColdTierFile],
    after: &[ColdTierFile],
    promoted_rows: i64,
    phase: &str,
) {
    let after_rows: HashMap<&str, i64> = after
        .iter()
        .map(|f| (f.file_url.as_str(), f.row_count))
        .collect();
    let rewritten: Vec<&str> = before
        .iter()
        .filter(|f| after_rows.get(f.file_url.as_str()) != Some(&f.row_count))
        .map(|f| f.file_url.as_str())
        .collect();
    assert!(
        rewritten.is_empty(),
        "{phase}: the promotion rewrote {} of {} existing datalake files: {rewritten:?}",
        rewritten.len(),
        before.len()
    );
    let before_urls: Vec<&str> = before.iter().map(|f| f.file_url.as_str()).collect();
    let new_files: Vec<&ColdTierFile> = after
        .iter()
        .filter(|f| !before_urls.contains(&f.file_url.as_str()))
        .collect();
    assert_eq!(
        after.len(),
        before.len() + new_files.len(),
        "{phase}: the manifest must grow by exactly the newly written files"
    );
    assert!(
        !new_files.is_empty(),
        "{phase}: the promotion must write at least one new datalake file"
    );
    assert_eq!(
        new_files.iter().map(|f| f.row_count).sum::<i64>(),
        promoted_rows,
        "{phase}: the new datalake files must hold exactly the warm tier's live rows"
    );
}

test_with_backends!(test_promotion_never_rewrites_existing_datalake_files_impl);

async fn test_promotion_never_rewrites_existing_datalake_files_impl(
    fixture: common::TestFixture,
) -> TestResult<()> {
    let cold_dir = fixture.temp_dir.path().join("cold");
    std::fs::create_dir_all(&cold_dir)?;
    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let ctx = SessionContext::new();
    let table = Arc::new(
        CayenneTableProvider::create_table(
            catalog,
            table_options(&fixture, &cold_dir),
            ctx.runtime_env(),
        )
        .await?,
    );
    ctx.register_table(TABLE, Arc::clone(&table) as Arc<dyn TableProvider>)?;
    let mut oracle: BTreeMap<i64, i64> = BTreeMap::new();
    // 7 and 8 are upserted twice, 100 and 210 are deleted from the datalake,
    // 150 is never touched, 205 lives only in the second generation.
    let lookups = [7, 8, 100, 150, 205, 210];

    // Promotion 1: ids 0..200 become the first datalake generation.
    let initial: Vec<(i64, i64)> = (0..200).map(|id| (id, id * 2)).collect();
    upsert(&table, &mut oracle, &initial).await?;
    flush_warm(&table).await;
    assert!(table.promote_warm_to_cold().await?, "promotion 1 fires");
    let generation_1 = manifest(&fixture.catalog, &table).await?;
    assert!(!generation_1.is_empty(), "promotion 1 registers datalake files");
    assert_eq!(
        generation_1.iter().map(|f| f.row_count).sum::<i64>(),
        200,
        "promotion 1 writes every row once"
    );
    assert_matches_oracle(&ctx, &oracle, &lookups, "after promotion 1").await?;

    // Round 2: re-insert ids 0..50 with a new value (genuine upserts of keys that
    // now live only in the newest datalake file) plus 30 new keys, then promote.
    let round_2: Vec<(i64, i64)> = (0..50)
        .map(|id| (id, id * 10 + 1))
        .chain((200..230).map(|id| (id, id * 2)))
        .collect();
    upsert(&table, &mut oracle, &round_2).await?;
    assert_matches_oracle(&ctx, &oracle, &lookups, "after round 2 upserts").await?;
    flush_warm(&table).await;
    assert!(table.promote_warm_to_cold().await?, "promotion 2 fires");
    let generation_2 = manifest(&fixture.catalog, &table).await?;
    assert_carried_forward(&generation_1, &generation_2, 80, "promotion 2");
    assert_matches_oracle(&ctx, &oracle, &lookups, "after promotion 2").await?;

    // Round 3: upsert ids 0..50 a second time, so each has a superseded copy in
    // both earlier generations, and delete one key from each generation.
    let round_3: Vec<(i64, i64)> = (0..50).map(|id| (id, id * 10 + 2)).collect();
    upsert(&table, &mut oracle, &round_3).await?;
    let deleted = delete_where(&table, col("id").eq(lit(100_i64))).await?;
    eprintln!("[warm_only_promotion] DELETE id = 100 reported {deleted} rows affected");
    oracle.remove(&100);
    let deleted = delete_where(&table, col("id").eq(lit(210_i64))).await?;
    eprintln!("[warm_only_promotion] DELETE id = 210 reported {deleted} rows affected");
    oracle.remove(&210);
    assert_matches_oracle(&ctx, &oracle, &lookups, "after round 3 writes").await?;
    flush_warm(&table).await;
    assert!(table.promote_warm_to_cold().await?, "promotion 3 fires");
    let generation_3 = manifest(&fixture.catalog, &table).await?;
    assert_carried_forward(&generation_2, &generation_3, 50, "promotion 3");
    assert_matches_oracle(&ctx, &oracle, &lookups, "after promotion 3").await?;

    // Predicate deletes judged against datalake rows: id 8's first-generation
    // value (16) and second-generation value (81) are both superseded, so a
    // delete matching either must leave the key's live row alone. Only a delete
    // matching its live value (82) removes it.
    for stale in [16_i64, 81] {
        let deleted = delete_where(&table, col("value").eq(lit(stale))).await?;
        eprintln!(
            "[warm_only_promotion] DELETE value = {stale} (superseded) reported {deleted} rows affected"
        );
        assert_matches_oracle(
            &ctx,
            &oracle,
            &lookups,
            &format!("after a delete matching only the superseded value {stale}"),
        )
        .await?;
    }
    let deleted = delete_where(&table, col("value").eq(lit(82_i64))).await?;
    eprintln!("[warm_only_promotion] DELETE value = 82 (live) reported {deleted} rows affected");
    oracle.remove(&8);
    assert_matches_oracle(&ctx, &oracle, &lookups, "after deleting id 8 by its live value")
        .await?;

    // Reopen from a fresh metastore connection: the retained tombstones and the
    // datalake manifest must reload and answer every query identically.
    table.drain_in_flight_maintenance().await?;
    drop(ctx);
    drop(table);
    let catalog_2 = Arc::new(CayenneCatalog::new(fixture.connection_string())?);
    catalog_2.init().await?;
    let ctx_2 = SessionContext::new();
    let reopened = Arc::new(
        CayenneTableProviderBuilder::new(
            Arc::clone(&catalog_2) as Arc<dyn MetadataCatalog>,
            ctx_2.runtime_env(),
        )
        .open(TABLE)
        .await?,
    );
    ctx_2.register_table(TABLE, Arc::clone(&reopened) as Arc<dyn TableProvider>)?;
    assert_eq!(
        manifest(&catalog_2, &reopened).await?.len(),
        generation_3.len(),
        "the reopened table sees the same datalake manifest"
    );
    assert_matches_oracle(&ctx_2, &oracle, &lookups, "after reopen").await?;

    // The rebuilt key set must still recognise id 7 as live (its newest copy is
    // in the third generation), so this upsert supersedes it rather than adding
    // a second row.
    upsert(&reopened, &mut oracle, &[(7, 73)]).await?;
    assert_matches_oracle(&ctx_2, &oracle, &lookups, "after an upsert on the reopened table")
        .await?;

    Ok(())
}
