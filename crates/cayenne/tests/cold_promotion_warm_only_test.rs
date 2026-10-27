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
    table_options_with_conflict(
        fixture,
        cold_dir,
        OnConflict::Upsert(ColumnReference::new(vec!["id".to_string()])),
    )
}

/// [`table_options`] with the given conflict behavior.
fn table_options_with_conflict(
    fixture: &common::TestFixture,
    cold_dir: &std::path::Path,
    on_conflict: OnConflict,
) -> CreateTableOptions {
    CreateTableOptions {
        table_name: TABLE.to_string(),
        schema: schema(),
        primary_key: vec!["id".to_string()],
        on_conflict: Some(on_conflict),
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
    batches
        .first()
        .and_then(|b| b.column(0).as_any().downcast_ref::<UInt64Array>())
        .and_then(|a| a.values().first())
        .copied()
        .ok_or_else(|| "DELETE returned no rows-affected count".into())
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
    assert_eq!(
        delete_where(&table, col("id").eq(lit(100_i64))).await?,
        1,
        "DELETE id = 100 removes its one first-generation row"
    );
    oracle.remove(&100);
    assert_eq!(
        delete_where(&table, col("id").eq(lit(210_i64))).await?,
        1,
        "DELETE id = 210 removes its one second-generation row"
    );
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
        assert_eq!(
            delete_where(&table, col("value").eq(lit(stale))).await?,
            0,
            "DELETE value = {stale} matches only a superseded copy, so it affects no row"
        );
        assert_matches_oracle(
            &ctx,
            &oracle,
            &lookups,
            &format!("after a delete matching only the superseded value {stale}"),
        )
        .await?;
    }
    assert_eq!(
        delete_where(&table, col("value").eq(lit(82_i64))).await?,
        1,
        "DELETE value = 82 removes id 8's live row, which has an upserted tombstone history"
    );
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

/// Distinct `max_sequence` values in the manifest: one per promotion, or per
/// datalake merge.
fn generation_count(files: &[ColdTierFile]) -> usize {
    files
        .iter()
        .map(|f| f.max_sequence)
        .collect::<std::collections::BTreeSet<_>>()
        .len()
}

test_with_backends!(test_cold_compaction_merges_generations_impl);

/// A datalake merge folds several promotion generations — each still holding
/// rows later upserts and deletes superseded — into one, keeps every query
/// answer exact, physically drops the superseded rows, and clears the
/// tombstones that hid them; a reopen answers the same.
async fn test_cold_compaction_merges_generations_impl(
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
    let lookups = [7, 8, 100, 101, 150, 215, 241];

    // Generation 1: ids 0..200.
    let initial: Vec<(i64, i64)> = (0..200).map(|id| (id, id * 2)).collect();
    upsert(&table, &mut oracle, &initial).await?;
    flush_warm(&table).await;
    assert!(table.promote_warm_to_cold().await?, "promotion 1 fires");

    // Generations 2..=5: replay upserts of ids 0..50 (each superseding the
    // previous generation's copy), delete one datalake-resident key, and add
    // ten new keys per round.
    for round in 1..=4_i64 {
        let rows: Vec<(i64, i64)> = (0..50)
            .map(|id| (id, id * 10 + round))
            .chain((200 + round * 10..210 + round * 10).map(|id| (id, id * 2)))
            .collect();
        upsert(&table, &mut oracle, &rows).await?;
        assert_eq!(
            delete_where(&table, col("id").eq(lit(99 + round))).await?,
            1,
            "DELETE id = {} removes its one datalake row",
            99 + round
        );
        oracle.remove(&(99 + round));
        flush_warm(&table).await;
        assert!(
            table.promote_warm_to_cold().await?,
            "promotion {} fires",
            round + 1
        );
    }
    let before = manifest(&fixture.catalog, &table).await?;
    assert_eq!(
        generation_count(&before),
        5,
        "five promotions leave five datalake generations"
    );
    let tombstones_before = fixture
        .catalog
        .get_table_delete_files(table.table_id())
        .await?
        .len();
    assert!(
        tombstones_before > 0,
        "the replayed upserts and deletes must leave tombstones hiding datalake rows"
    );
    assert_matches_oracle(&ctx, &oracle, &lookups, "before the datalake merge").await?;

    assert!(
        table.compact_cold_tier().await?,
        "five small generations qualify for a merge"
    );
    let after = manifest(&fixture.catalog, &table).await?;
    assert_eq!(
        generation_count(&after),
        1,
        "every generation is merged into one"
    );
    assert!(
        after.len() < before.len(),
        "the merge reduces the datalake file count ({} -> {})",
        before.len(),
        after.len()
    );
    assert_eq!(
        after.iter().map(|f| f.row_count).sum::<i64>(),
        i64::try_from(oracle.len())?,
        "the merged generation holds exactly the live rows: every superseded copy is gone"
    );
    assert_eq!(
        fixture
            .catalog
            .get_table_delete_files(table.table_id())
            .await?
            .len(),
        0,
        "no tombstone can hide a row of the merged generation, so all are cleared"
    );
    assert_matches_oracle(&ctx, &oracle, &lookups, "after the datalake merge").await?;

    // Writes after the merge still supersede datalake rows correctly.
    upsert(&table, &mut oracle, &[(7, 777), (150, 1500)]).await?;
    assert_eq!(
        delete_where(&table, col("id").eq(lit(215_i64))).await?,
        1,
        "DELETE id = 215 removes its one merged-generation row"
    );
    oracle.remove(&215);
    assert_matches_oracle(&ctx, &oracle, &lookups, "after writes following the merge").await?;

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
        generation_count(&manifest(&catalog_2, &reopened).await?),
        1,
        "the reopened table sees the merged manifest"
    );
    assert_matches_oracle(&ctx_2, &oracle, &lookups, "after reopen").await?;
    Ok(())
}

/// Open `TABLE` from a fresh metastore connection, registered in a new context.
async fn reopen(
    fixture: &common::TestFixture,
) -> TestResult<(Arc<CayenneCatalog>, Arc<CayenneTableProvider>, SessionContext)> {
    let catalog = Arc::new(CayenneCatalog::new(fixture.connection_string())?);
    catalog.init().await?;
    let ctx = SessionContext::new();
    let table = Arc::new(
        CayenneTableProviderBuilder::new(
            Arc::clone(&catalog) as Arc<dyn MetadataCatalog>,
            ctx.runtime_env(),
        )
        .open(TABLE)
        .await?,
    );
    ctx.register_table(TABLE, Arc::clone(&table) as Arc<dyn TableProvider>)?;
    Ok((catalog, table, ctx))
}

async fn delete_file_count(
    catalog: &CayenneCatalog,
    table: &CayenneTableProvider,
) -> TestResult<usize> {
    Ok(catalog.get_table_delete_files(table.table_id()).await?.len())
}

test_with_backends!(test_partial_cold_compaction_keeps_tombstones_of_unmerged_generations_impl);

/// A merge that takes only part of the datalake — sixteen of seventeen
/// generations, the per-pass cap — leaves one generation unmerged while
/// tombstones written after every promotion hide rows in both the merged and
/// the unmerged files. Those must survive the merge's tombstone clear; the
/// ones that hid only rows the merge dropped must not.
async fn test_partial_cold_compaction_keeps_tombstones_of_unmerged_generations_impl(
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
    let lookups = [1, 16, 17, 50, 51, 66, 98, 99, 1016];

    // Generation 1: ids 0..100.
    let initial: Vec<(i64, i64)> = (0..100).map(|id| (id, id * 2)).collect();
    upsert(&table, &mut oracle, &initial).await?;
    flush_warm(&table).await;
    assert!(table.promote_warm_to_cold().await?, "promotion 1 fires");
    // Generations 2..=17: each upserts one first-generation key, deletes another,
    // and adds a new key.
    for round in 1..=16_i64 {
        upsert(&table, &mut oracle, &[(round, 1000 + round), (1000 + round, round)]).await?;
        assert_eq!(
            delete_where(&table, col("id").eq(lit(50 + round))).await?,
            1,
            "DELETE id = {} removes its one first-generation row",
            50 + round
        );
        oracle.remove(&(50 + round));
        flush_warm(&table).await;
        assert!(
            table.promote_warm_to_cold().await?,
            "promotion {} fires",
            round + 1
        );
    }
    // After the last promotion: supersede a key whose live copy is in generation 2
    // (merged below), one whose live copy is in generation 17 (left unmerged), and
    // delete a first-generation key. These tombstones are newer than every
    // datalake file.
    upsert(&table, &mut oracle, &[(1, 5001), (16, 5016)]).await?;
    assert_eq!(
        delete_where(&table, col("id").eq(lit(98_i64))).await?,
        1,
        "DELETE id = 98 removes its one first-generation row"
    );
    oracle.remove(&98);
    let before = manifest(&fixture.catalog, &table).await?;
    assert_eq!(generation_count(&before), 17, "seventeen promotions, seventeen generations");
    let tombstones_before = delete_file_count(&fixture.catalog, &table).await?;
    assert_matches_oracle(&ctx, &oracle, &lookups, "before the partial merge").await?;

    assert!(table.compact_cold_tier().await?, "the oldest sixteen generations merge");
    let after = manifest(&fixture.catalog, &table).await?;
    assert_eq!(
        generation_count(&after),
        2,
        "the merged generation plus the one left over by the per-pass cap"
    );
    let tombstones_after = delete_file_count(&fixture.catalog, &table).await?;
    assert!(
        tombstones_after < tombstones_before,
        "the tombstones that hid only merged-away rows are cleared ({tombstones_before} -> {tombstones_after})"
    );
    assert!(
        tombstones_after >= 2,
        "the tombstones newer than every datalake file (the last upsert and delete) remain, got {tombstones_after}"
    );
    assert_matches_oracle(&ctx, &oracle, &lookups, "after the partial merge").await?;

    table.drain_in_flight_maintenance().await?;
    drop(ctx);
    drop(table);
    let (_catalog_2, _reopened, ctx_2) = reopen(&fixture).await?;
    assert_matches_oracle(&ctx_2, &oracle, &lookups, "after reopen").await?;
    Ok(())
}

test_with_backends!(test_do_nothing_table_over_the_datalake_impl);

/// Insert-or-ignore over the datalake: an insert of a key whose live row is in
/// a datalake file is dropped; an insert of a key deleted from the datalake is
/// accepted, whether or not a promotion ran between the delete and the insert;
/// and both still hold after a datalake merge and a reopen. The exact keyset
/// these depend on is rebuilt from the datalake with per-file cutoffs.
async fn test_do_nothing_table_over_the_datalake_impl(
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
            table_options_with_conflict(&fixture, &cold_dir, OnConflict::DoNothingAll),
            ctx.runtime_env(),
        )
        .await?,
    );
    ctx.register_table(TABLE, Arc::clone(&table) as Arc<dyn TableProvider>)?;
    let mut oracle: BTreeMap<i64, i64> = BTreeMap::new();
    let lookups = [5, 7, 8, 9, 300];
    // Insert-or-ignore: the oracle keeps the first value of a key.
    let insert = |oracle: &mut BTreeMap<i64, i64>, rows: &[(i64, i64)]| {
        for (id, value) in rows {
            oracle.entry(*id).or_insert(*value);
        }
    };

    let initial: Vec<(i64, i64)> = (0..100).map(|id| (id, id * 2)).collect();
    common::insert_batch(table.as_ref(), id_value_batch(&initial)?).await?;
    insert(&mut oracle, &initial);
    flush_warm(&table).await;
    assert!(table.promote_warm_to_cold().await?, "promotion 1 fires");

    // A key whose live row is in the datalake is ignored.
    common::insert_batch(table.as_ref(), id_value_batch(&[(5, 999)])?).await?;
    insert(&mut oracle, &[(5, 999)]);
    assert_matches_oracle(&ctx, &oracle, &lookups, "after inserting a datalake key").await?;

    // Delete then re-insert with no promotion between: accepted.
    assert_eq!(delete_where(&table, col("id").eq(lit(7_i64))).await?, 1);
    oracle.remove(&7);
    common::insert_batch(table.as_ref(), id_value_batch(&[(7, 777)])?).await?;
    insert(&mut oracle, &[(7, 777)]);
    assert_matches_oracle(&ctx, &oracle, &lookups, "after re-inserting a deleted key").await?;

    // Delete, promote, then re-insert: accepted.
    assert_eq!(delete_where(&table, col("id").eq(lit(8_i64))).await?, 1);
    oracle.remove(&8);
    common::insert_batch(table.as_ref(), id_value_batch(&[(300, 600)])?).await?;
    insert(&mut oracle, &[(300, 600)]);
    flush_warm(&table).await;
    assert!(table.promote_warm_to_cold().await?, "promotion 2 fires");
    common::insert_batch(table.as_ref(), id_value_batch(&[(8, 888), (9, 999)])?).await?;
    insert(&mut oracle, &[(8, 888), (9, 999)]);
    assert_matches_oracle(&ctx, &oracle, &lookups, "after re-inserting across a promotion")
        .await?;

    // Two more generations, then merge all four.
    for (round, id) in [(3_i64, 301_i64), (4, 302)] {
        common::insert_batch(table.as_ref(), id_value_batch(&[(id, id * 2)])?).await?;
        insert(&mut oracle, &[(id, id * 2)]);
        flush_warm(&table).await;
        assert!(table.promote_warm_to_cold().await?, "promotion {round} fires");
    }
    assert!(table.compact_cold_tier().await?, "four generations merge");
    assert_eq!(generation_count(&manifest(&fixture.catalog, &table).await?), 1);
    common::insert_batch(table.as_ref(), id_value_batch(&[(5, 555), (8, 8888)])?).await?;
    insert(&mut oracle, &[(5, 555), (8, 8888)]);
    assert_matches_oracle(&ctx, &oracle, &lookups, "after the merge").await?;

    table.drain_in_flight_maintenance().await?;
    drop(ctx);
    drop(table);
    let (_catalog_2, reopened, ctx_2) = reopen(&fixture).await?;
    common::insert_batch(reopened.as_ref(), id_value_batch(&[(9, 9999), (7, 7777)])?).await?;
    insert(&mut oracle, &[(9, 9999), (7, 7777)]);
    assert_matches_oracle(&ctx_2, &oracle, &lookups, "after reopen").await?;
    Ok(())
}

fn id_value_batch(rows: &[(i64, i64)]) -> TestResult<RecordBatch> {
    Ok(RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|(_, value)| *value).collect::<Vec<_>>(),
            )),
        ],
    )?)
}

const COMPOSITE_TABLE: &str = "composite_t";

fn composite_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("tenant", DataType::Utf8, false),
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

type CompositeOracle = BTreeMap<(String, i64), i64>;

async fn upsert_composite(
    table: &CayenneTableProvider,
    oracle: &mut CompositeOracle,
    rows: &[(&str, i64, i64)],
) -> TestResult<()> {
    let batch = RecordBatch::try_new(
        composite_schema(),
        vec![
            Arc::new(arrow::array::StringArray::from(
                rows.iter().map(|(tenant, _, _)| *tenant).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|(_, id, _)| *id).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|(_, _, value)| *value).collect::<Vec<_>>(),
            )),
        ],
    )?;
    common::insert_batch(table, batch).await?;
    for (tenant, id, value) in rows {
        oracle.insert(((*tenant).to_string(), *id), *value);
    }
    Ok(())
}

/// Every row, ordered by key, with the tenant read as `Utf8` whatever string
/// type the scan produced.
async fn select_composite(ctx: &SessionContext) -> TestResult<Vec<(String, i64, i64)>> {
    let batches = ctx
        .sql(&format!(
            "SELECT tenant, id, value FROM {COMPOSITE_TABLE} ORDER BY tenant, id"
        ))
        .await?
        .collect()
        .await?;
    let mut rows = Vec::new();
    for batch in &batches {
        let tenants = arrow::compute::cast(batch.column(0), &DataType::Utf8)?;
        let tenants = tenants
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .ok_or("tenant column must cast to Utf8")?;
        let ids = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or("id column must be Int64")?;
        let values = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or("value column must be Int64")?;
        for row in 0..batch.num_rows() {
            rows.push((
                tenants.value(row).to_string(),
                ids.value(row),
                values.value(row),
            ));
        }
    }
    Ok(rows)
}

async fn assert_composite_matches(
    ctx: &SessionContext,
    oracle: &CompositeOracle,
    phase: &str,
) -> TestResult<()> {
    let expected: Vec<(String, i64, i64)> = oracle
        .iter()
        .map(|((tenant, id), value)| (tenant.clone(), *id, *value))
        .collect();
    assert_eq!(
        select_composite(ctx).await?,
        expected,
        "{phase}: every (tenant, id) appears exactly once carrying its newest value"
    );
    Ok(())
}

test_with_backends!(test_composite_key_datalake_promotion_and_merge_impl);

/// The warm-only promotion, the per-file datalake cutoffs, DELETE scans, and a
/// datalake merge over a composite (`Utf8`, `Int64`) primary key, whose
/// tombstones are row-encoded keys rather than `Int64` values.
async fn test_composite_key_datalake_promotion_and_merge_impl(
    fixture: common::TestFixture,
) -> TestResult<()> {
    let cold_dir = fixture.temp_dir.path().join("cold");
    std::fs::create_dir_all(&cold_dir)?;
    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let ctx = SessionContext::new();
    let mut options = table_options(&fixture, &cold_dir);
    options.table_name = COMPOSITE_TABLE.to_string();
    options.schema = composite_schema();
    options.primary_key = vec!["tenant".to_string(), "id".to_string()];
    options.on_conflict = Some(OnConflict::Upsert(ColumnReference::new(vec![
        "tenant".to_string(),
        "id".to_string(),
    ])));
    options.vortex_config.cluster_by = vec!["tenant".to_string(), "id".to_string()];
    let table =
        Arc::new(CayenneTableProvider::create_table(catalog, options, ctx.runtime_env()).await?);
    ctx.register_table(
        COMPOSITE_TABLE,
        Arc::clone(&table) as Arc<dyn TableProvider>,
    )?;
    let mut oracle = CompositeOracle::new();

    // Generation 1: three tenants, ids 0..50 each.
    let initial: Vec<(&str, i64, i64)> = ["a", "b", "c"]
        .into_iter()
        .flat_map(|tenant| (0..50).map(move |id| (tenant, id, id)))
        .collect();
    upsert_composite(&table, &mut oracle, &initial).await?;
    flush_warm(&table).await;
    assert!(table.promote_warm_to_cold().await?, "promotion 1 fires");
    let generation_1 = manifest(&fixture.catalog, &table).await?;

    // Generations 2..=4: re-upsert tenant a's ids 0..20 and delete one of tenant
    // b's ids, each round.
    for round in 1..=3_i64 {
        let rows: Vec<(&str, i64, i64)> = (0..20).map(|id| ("a", id, round * 100 + id)).collect();
        upsert_composite(&table, &mut oracle, &rows).await?;
        let deleted = delete_where(
            &table,
            col("tenant").eq(lit("b")).and(col("id").eq(lit(round))),
        )
        .await?;
        assert_eq!(deleted, 1, "DELETE (b, {round}) removes its one first-generation row");
        oracle.remove(&("b".to_string(), round));
        flush_warm(&table).await;
        assert!(
            table.promote_warm_to_cold().await?,
            "promotion {} fires",
            round + 1
        );
        if round == 1 {
            assert_carried_forward(
                &generation_1,
                &manifest(&fixture.catalog, &table).await?,
                20,
                "composite promotion 2",
            );
        }
    }
    assert_composite_matches(&ctx, &oracle, "after four promotions").await?;

    // A predicate matching only tenant a's superseded values (id 5 had 5, 105,
    // 205; its live value is 305) affects no row; one matching the live value
    // removes exactly that row.
    for stale in [5_i64, 105, 205] {
        assert_eq!(
            delete_where(&table, col("value").eq(lit(stale)).and(col("tenant").eq(lit("a"))))
                .await?,
            0,
            "DELETE matching the superseded value {stale} affects no row"
        );
    }
    assert_eq!(
        delete_where(&table, col("value").eq(lit(305_i64)).and(col("tenant").eq(lit("a"))))
            .await?,
        1,
        "DELETE matching the live value removes (a, 5)"
    );
    oracle.remove(&("a".to_string(), 5));
    assert_composite_matches(&ctx, &oracle, "after the predicate deletes").await?;

    assert!(table.compact_cold_tier().await?, "four generations merge");
    let merged = manifest(&fixture.catalog, &table).await?;
    assert_eq!(generation_count(&merged), 1);
    assert_eq!(
        merged.iter().map(|f| f.row_count).sum::<i64>(),
        i64::try_from(oracle.len())?,
        "the merged generation holds exactly the live rows"
    );
    assert_composite_matches(&ctx, &oracle, "after the merge").await?;

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
        .open(COMPOSITE_TABLE)
        .await?,
    );
    ctx_2.register_table(
        COMPOSITE_TABLE,
        Arc::clone(&reopened) as Arc<dyn TableProvider>,
    )?;
    assert_composite_matches(&ctx_2, &oracle, "after reopen").await?;
    Ok(())
}
