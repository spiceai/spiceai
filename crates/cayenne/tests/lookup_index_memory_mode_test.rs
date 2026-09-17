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

//! Secondary indexes on `mode: memory` tables, which keep their rows in memory
//! and index them there.
//!
//! Every check runs the same lookups on an indexed table and an identical
//! unindexed one, through full refreshes, appends, upserts that leave superseded
//! versions in memory, and deletes that rewrite batches — the transitions an
//! in-memory index has to follow row for row.

#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use cayenne::lookup_index::LookupIndexCounters;
use cayenne::metadata::{CdcDurability, CreateTableOptions, DeletionMode, VortexConfig};
use cayenne::provider::CayenneContext;
use cayenne::{CayenneTableProvider, CayenneTableProviderBuilder, MetadataCatalog};

use datafusion::datasource::TableProvider;
use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::prelude::{SessionContext, lit};
use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};

/// A unique key and a heavily repeated one.
const INDEXES: [&[&str]; 2] = [&["TenantId", "ServiceId"], &["TenantId", "PoolId"]];

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("AutoId", DataType::Int64, false),
        Field::new("TenantId", DataType::Int64, false),
        Field::new("PoolId", DataType::Int64, true),
        Field::new("ServiceId", DataType::Utf8, false),
        Field::new("Payload", DataType::Utf8, false),
    ]))
}

/// Rows `offset..offset + count`, their payload tagged with `version`. Every
/// 50th row has a NULL `PoolId`, which the repeated key must never match.
fn rows(offset: i64, count: usize, version: &str) -> RecordBatch {
    let ids: Vec<i64> = (0..i64::try_from(count).expect("fits"))
        .map(|i| offset + i)
        .collect();
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(ids.clone())),
            Arc::new(Int64Array::from(
                ids.iter().map(|id| id % 97).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                ids.iter()
                    .map(|id| (id % 50 != 7).then_some(id % 31))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                ids.iter()
                    .map(|id| format!("SV{id:032x}"))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                ids.iter()
                    .map(|id| format!("{version}-{id}"))
                    .collect::<Vec<_>>(),
            )),
        ],
    )
    .expect("fixture batch")
}

/// A `mode: memory` table, configured the way `apply_memory_mode_overrides`
/// configures one.
async fn memory_table(
    fixture: &common::TestFixture,
    runtime_env: Arc<RuntimeEnv>,
    name: &str,
    indexes: &[&[&str]],
    upsert: bool,
) -> Arc<CayenneTableProvider> {
    let vortex_config = VortexConfig {
        memory_mode: true,
        cdc_mem_tier_shards: 1,
        cdc_mem_tier_max_age_ms: 0,
        cdc_mem_tier_checkpoint_interval_ms: 0,
        cdc_mem_tier_seal_age_ms: 0,
        compaction_background_interval_ms: 0,
        cold_tier_location: None,
        inline_max_rows: 0,
        inline_max_bytes: 0,
        inline_max_buffer_bytes: 0,
        cdc_mem_tier_max_bytes: 0,
        cdc_durability: CdcDurability::Memory,
        deletion_mode: DeletionMode::Key,
        ..VortexConfig::default()
    };
    let context = CayenneContext::new(&vortex_config, Arc::clone(&runtime_env), name);
    let options = CreateTableOptions {
        table_name: name.to_string(),
        schema: schema(),
        primary_key: if upsert {
            vec!["AutoId".to_string()]
        } else {
            vec![]
        },
        on_conflict: upsert
            .then(|| OnConflict::Upsert(ColumnReference::new(vec!["AutoId".to_string()]))),
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config,
    };
    let catalog = Arc::clone(&fixture.catalog);
    let catalog: Arc<dyn MetadataCatalog> = catalog;
    Arc::new(
        CayenneTableProviderBuilder::new(catalog, runtime_env)
            .with_context(context)
            .with_secondary_indexes(
                indexes
                    .iter()
                    .map(|columns| columns.iter().map(|c| (*c).to_string()).collect())
                    .collect(),
            )
            .create(options)
            .await
            .expect("create memory table"),
    )
}

async fn overwrite(provider: &Arc<CayenneTableProvider>, batches: Vec<RecordBatch>) {
    let ctx = SessionContext::new();
    let exec = datafusion::datasource::memory::MemorySourceConfig::try_new_exec(
        &[batches],
        schema(),
        None,
    )
    .expect("overwrite source");
    let plan = provider
        .insert_into(
            &ctx.state(),
            exec,
            datafusion_expr::dml::InsertOp::Overwrite,
        )
        .await
        .expect("overwrite plan");
    datafusion_physical_plan::collect(plan, ctx.task_ctx())
        .await
        .expect("overwrite");
}

async fn append(provider: &Arc<CayenneTableProvider>, batch: RecordBatch) {
    common::insert_batches(provider, vec![batch])
        .await
        .expect("append");
}

async fn delete_ids(provider: &Arc<CayenneTableProvider>, ids: &[i64]) {
    let ctx = SessionContext::new();
    let predicate =
        datafusion_expr::ident("AutoId").in_list(ids.iter().map(|id| lit(*id)).collect(), false);
    let plan = provider
        .delete_from(&ctx.state(), vec![predicate])
        .await
        .expect("delete plan");
    datafusion_physical_plan::collect(plan, ctx.task_ctx())
        .await
        .expect("delete");
}

/// The rows `sql` returns, rendered and sorted so two tables compare exactly.
async fn query(provider: &Arc<CayenneTableProvider>, name: &str, sql: &str) -> Vec<String> {
    let ctx = SessionContext::new();
    ctx.register_table(name, Arc::clone(provider) as Arc<dyn TableProvider>)
        .expect("register");
    let batches = ctx
        .sql(&sql.replace("{t}", name))
        .await
        .expect("plan")
        .collect()
        .await
        .expect("execute");
    let mut rendered = Vec::new();
    for batch in &batches {
        for row in 0..batch.num_rows() {
            let cells: Vec<String> = (0..batch.num_columns())
                .map(|column| {
                    let array = batch.column(column);
                    if array.is_null(row) {
                        "NULL".to_string()
                    } else {
                        arrow::util::display::array_value_to_string(array, row)
                            .expect("render cell")
                    }
                })
                .collect();
            rendered.push(cells.join("|"));
        }
    }
    rendered.sort();
    rendered
}

fn unique_lookup(id: i64) -> String {
    format!(
        "SELECT * FROM {{t}} WHERE \"TenantId\" = {} AND \"ServiceId\" = 'SV{id:032x}'",
        id % 97
    )
}

fn repeated_lookup(tenant: i64, pool: i64) -> String {
    format!("SELECT * FROM {{t}} WHERE \"TenantId\" = {tenant} AND \"PoolId\" = {pool}")
}

fn counters(provider: &Arc<CayenneTableProvider>) -> LookupIndexCounters {
    provider
        .lookup_index_counters()
        .expect("the table declares indexes")
}

/// Runs the same lookups on both tables and requires identical rows. Returns
/// how many rows the indexed table returned in total.
async fn compare(
    indexed: &Arc<CayenneTableProvider>,
    plain: &Arc<CayenneTableProvider>,
    ids: impl IntoIterator<Item = i64>,
    stage: &str,
) -> usize {
    let mut returned = 0;
    for id in ids {
        for sql in [unique_lookup(id), repeated_lookup(id % 97, id % 31)] {
            let from_index = query(indexed, "indexed", &sql).await;
            let from_scan = query(plain, "plain", &sql).await;
            assert_eq!(from_index, from_scan, "{stage}: {sql}");
            returned += from_index.len();
        }
    }
    returned
}

/// Lookups on an indexed memory table return exactly what an unindexed one does
/// through a full refresh, an append, a delete that rewrites batches, and a
/// second full refresh — and the index, not a scan, answers them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn memory_mode_lookups_match_an_unindexed_table() {
    const ROWS: usize = 40_000;
    let fixture = common::TestFixture::new(common::BackendType::Sqlite)
        .await
        .expect("fixture");
    let env = Arc::new(RuntimeEnv::default());
    let indexed = memory_table(&fixture, Arc::clone(&env), "indexed", &INDEXES, false).await;
    let plain = memory_table(&fixture, Arc::clone(&env), "plain", &[], false).await;
    assert!(plain.lookup_index_counters().is_none());

    let refresh: Vec<RecordBatch> = (0..5)
        .map(|chunk| rows(chunk * 8_000, 8_000, "v1"))
        .collect();
    overwrite(&indexed, refresh.clone()).await;
    overwrite(&plain, refresh).await;
    let found = compare(&indexed, &plain, (0..40).map(|i| i * 997), "after refresh").await;
    assert!(
        found > 40,
        "the lookups returned too few rows to prove anything: {found}"
    );

    let after_refresh = counters(&indexed);
    assert!(after_refresh.selected > 0, "{after_refresh:?}");
    assert_eq!(
        after_refresh.unbuilt, 0,
        "every batch fits an unbounded pool: {after_refresh:?}"
    );
    assert!(after_refresh.index_bytes > 0, "{after_refresh:?}");
    // 80 lookups over 40,000 rows: an index reads a few rows per lookup, a scan
    // would read every row.
    assert!(
        after_refresh.candidate_rows < 80 * 1_000,
        "the index read nearly as much as a scan: {after_refresh:?}"
    );

    // A key no row holds, and a NULL literal no row can equal.
    for sql in [
        "SELECT * FROM {t} WHERE \"TenantId\" = 5 AND \"ServiceId\" = 'absent'",
        "SELECT * FROM {t} WHERE \"TenantId\" = 5 AND \"PoolId\" = NULL",
    ] {
        assert_eq!(query(&indexed, "indexed", sql).await, Vec::<String>::new());
        assert_eq!(query(&plain, "plain", sql).await, Vec::<String>::new());
    }
    assert!(counters(&indexed).empty > 0);

    let rows_i64 = i64::try_from(ROWS).expect("fits");
    append(&indexed, rows(rows_i64, 5_000, "v1")).await;
    append(&plain, rows(rows_i64, 5_000, "v1")).await;
    compare(
        &indexed,
        &plain,
        (0..20).map(|i| rows_i64 + i * 211).chain([3, 7_777]),
        "after append",
    )
    .await;

    let deleted: Vec<i64> = (0..400).map(|i| i * 97 + i % 5).collect();
    delete_ids(&indexed, &deleted).await;
    delete_ids(&plain, &deleted).await;
    compare(
        &indexed,
        &plain,
        deleted.iter().copied().step_by(7),
        "after delete",
    )
    .await;
    compare(
        &indexed,
        &plain,
        (0..20).map(|i| i * 1_999 + 1),
        "after delete",
    )
    .await;

    overwrite(&indexed, vec![rows(1_000_000, 10_000, "v2")]).await;
    overwrite(&plain, vec![rows(1_000_000, 10_000, "v2")]).await;
    compare(
        &indexed,
        &plain,
        (0..20).map(|i| 1_000_000 + i * 487).chain([0, 997]),
        "after second refresh",
    )
    .await;
    let end = counters(&indexed);
    assert_eq!(end.unbuilt, 0, "{end:?}");
    println!("memory-mode index counters: {end:?}");
}

/// An upsert leaves the superseded version of a row in memory, hidden only by a
/// tombstone. A lookup must never return it, and must return the new version.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn memory_mode_lookups_never_return_a_superseded_version() {
    let fixture = common::TestFixture::new(common::BackendType::Sqlite)
        .await
        .expect("fixture");
    let env = Arc::new(RuntimeEnv::default());
    let indexed = memory_table(&fixture, Arc::clone(&env), "indexed", &INDEXES, true).await;
    let plain = memory_table(&fixture, Arc::clone(&env), "plain", &[], true).await;

    for table in [&indexed, &plain] {
        append(table, rows(0, 20_000, "v1")).await;
        append(table, rows(5_000, 3_000, "v2")).await;
    }
    compare(
        &indexed,
        &plain,
        (0..30).map(|i| 4_900 + i * 20),
        "after upsert",
    )
    .await;
    for id in [5_000i64, 6_500, 7_999] {
        let found = query(&indexed, "indexed", &unique_lookup(id)).await;
        assert_eq!(
            found.len(),
            1,
            "exactly one live version of {id}: {found:?}"
        );
        assert!(found[0].ends_with(&format!("v2-{id}")), "{found:?}");
    }

    delete_ids(&indexed, &[5_000, 6_500, 12_000]).await;
    delete_ids(&plain, &[5_000, 6_500, 12_000]).await;
    compare(
        &indexed,
        &plain,
        [5_000, 6_500, 12_000, 6_501, 11_999],
        "after delete",
    )
    .await;
    let end = counters(&indexed);
    assert!(end.selected > 0, "{end:?}");
    assert_eq!(end.unbuilt, 0, "{end:?}");
}

fn runtime_with_pool(bytes: usize) -> (Arc<RuntimeEnv>, Arc<dyn MemoryPool>) {
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(bytes));
    let runtime_env = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::clone(&pool))
        .build_arc()
        .expect("runtime env");
    (runtime_env, pool)
}

/// The index's bytes are reserved in the query pool for exactly as long as the
/// rows they index exist: a refresh that replaces the rows releases them, and
/// dropping the table releases the rest.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn memory_mode_index_memory_follows_its_rows() {
    let fixture = common::TestFixture::new(common::BackendType::Sqlite)
        .await
        .expect("fixture");
    let (env, pool) = runtime_with_pool(256 * 1024 * 1024);
    let table = memory_table(&fixture, Arc::clone(&env), "indexed", &INDEXES, false).await;

    overwrite(
        &table,
        (0..4)
            .map(|chunk| rows(chunk * 8_000, 8_000, "v1"))
            .collect(),
    )
    .await;
    let large = counters(&table).index_bytes;
    assert!(large > 0);
    assert_eq!(pool.reserved(), usize::try_from(large).expect("fits"));

    overwrite(&table, vec![rows(0, 8_000, "v2")]).await;
    let small = counters(&table).index_bytes;
    assert!(
        small * 3 < large,
        "replacing 32,000 rows with 8,000 must release the replaced rows' index: {large} -> {small}"
    );
    assert_eq!(pool.reserved(), usize::try_from(small).expect("fits"));

    drop(table);
    drop(env);
    let deadline = Instant::now() + Duration::from_secs(10);
    while pool.reserved() > 0 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(pool.reserved(), 0, "a dropped table must release its index");
}

/// When the pool cannot fit a batch's index, lookups read that batch in full
/// and still answer correctly.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn memory_mode_batches_the_pool_cannot_fit_are_read_whole() {
    let fixture = common::TestFixture::new(common::BackendType::Sqlite)
        .await
        .expect("fixture");
    let (env, _pool) = runtime_with_pool(64 * 1024);
    let indexed = memory_table(&fixture, Arc::clone(&env), "indexed", &INDEXES, false).await;
    let plain = memory_table(
        &fixture,
        Arc::new(RuntimeEnv::default()),
        "plain",
        &[],
        false,
    )
    .await;
    let refresh: Vec<RecordBatch> = (0..5)
        .map(|chunk| rows(chunk * 8_000, 8_000, "v1"))
        .collect();
    overwrite(&indexed, refresh.clone()).await;
    overwrite(&plain, refresh).await;
    compare(&indexed, &plain, (0..20).map(|i| i * 1_931), "refused").await;
    let end = counters(&indexed);
    assert!(end.builds_unpublished > 0, "{end:?}");
    assert!(
        end.unbuilt > 0,
        "lookups must report reading unindexed rows: {end:?}"
    );
}
