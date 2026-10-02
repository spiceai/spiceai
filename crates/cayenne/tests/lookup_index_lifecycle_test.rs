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

//! The secondary index across a table's lifetime: re-registration with other
//! `indexes`, a build the memory pool cannot fit, a lookup cancelled while it
//! holds the build, a dropped table, and rows appended after the build.
//!
//! Every lookup here also checks its rows, so an index that went wrong in any of
//! these transitions fails on the result, not only on the counters.

#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use cayenne::lookup_index::LookupIndexCounters;
use cayenne::metadata::{CreateTableOptions, VortexConfig};
use cayenne::provider::CayenneContext;
use cayenne::{CayenneTableProvider, CayenneTableProviderBuilder, MetadataCatalog};

use datafusion::datasource::TableProvider;
use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::prelude::SessionContext;

const KEY: [&str; 2] = ["TenantId", "ServiceId"];
const MIB: usize = 1024 * 1024;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("AutoId", DataType::Int64, false),
        Field::new("TenantId", DataType::Int64, false),
        Field::new("ServiceId", DataType::Utf8, false),
        Field::new("Payload", DataType::Utf8, false),
    ]))
}

fn rows(offset: i64, count: usize) -> RecordBatch {
    let ids: Vec<i64> = (0..i64::try_from(count).expect("fits i64"))
        .map(|i| offset + i)
        .collect();
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(ids.clone())),
            Arc::new(Int64Array::from(
                ids.iter().map(|id| id % 997).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                ids.iter()
                    .map(|id| format!("SV{id:032x}"))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                ids.iter()
                    .map(|id| format!("payload-{id:08}"))
                    .collect::<Vec<_>>(),
            )),
        ],
    )
    .expect("fixture batch")
}

fn lookup_sql(table: &str, id: i64) -> String {
    format!(
        "SELECT \"AutoId\" FROM {table} WHERE \"TenantId\" = {} AND \"ServiceId\" = 'SV{id:032x}'",
        id % 997
    )
}

fn runtime_with_pool(bytes: usize) -> (Arc<RuntimeEnv>, Arc<dyn MemoryPool>) {
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(bytes));
    let runtime_env = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::clone(&pool))
        .build_arc()
        .expect("runtime env");
    (runtime_env, pool)
}

/// Creates the table, or reopens it when the catalog already holds one of this
/// name — the same call the accelerator makes on every registration.
async fn open(
    fixture: &common::TestFixture,
    runtime_env: Arc<RuntimeEnv>,
    name: &str,
    indexes: &[&[&str]],
) -> Arc<CayenneTableProvider> {
    let vortex_config = VortexConfig {
        target_vortex_file_size_mb: 1,
        ..VortexConfig::default()
    };
    open_with(fixture, runtime_env, name, indexes, vortex_config).await
}

async fn open_with(
    fixture: &common::TestFixture,
    runtime_env: Arc<RuntimeEnv>,
    name: &str,
    indexes: &[&[&str]],
    vortex_config: VortexConfig,
) -> Arc<CayenneTableProvider> {
    open_configured(fixture, runtime_env, name, indexes, vortex_config).await
}

async fn open_configured(
    fixture: &common::TestFixture,
    runtime_env: Arc<RuntimeEnv>,
    name: &str,
    indexes: &[&[&str]],
    vortex_config: VortexConfig,
) -> Arc<CayenneTableProvider> {
    let context = CayenneContext::new(&vortex_config, Arc::clone(&runtime_env), name);
    let options = CreateTableOptions {
        table_name: name.to_string(),
        schema: schema(),
        primary_key: vec![],
        on_conflict: None,
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
            .expect("create or reopen table"),
    )
}

async fn overwrite(provider: &Arc<CayenneTableProvider>, batch: RecordBatch) {
    let ctx = SessionContext::new();
    let exec = datafusion::datasource::memory::MemorySourceConfig::try_new_exec(
        &[vec![batch]],
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

async fn insert(provider: &Arc<CayenneTableProvider>, name: &str, batch: RecordBatch) {
    let ctx = SessionContext::new();
    ctx.register_table(name, Arc::clone(provider) as Arc<dyn TableProvider>)
        .expect("register target");
    let mem =
        datafusion::datasource::MemTable::try_new(schema(), vec![vec![batch]]).expect("memtable");
    ctx.register_table("src", Arc::new(mem))
        .expect("register src");
    ctx.sql(&format!("INSERT INTO {name} SELECT * FROM src"))
        .await
        .expect("insert plan")
        .collect()
        .await
        .expect("insert");
}

/// Looks up `id` and checks the table returns exactly its one row.
async fn lookup(provider: &Arc<CayenneTableProvider>, name: &str, id: i64) {
    let ctx = SessionContext::new();
    ctx.register_table(name, Arc::clone(provider) as Arc<dyn TableProvider>)
        .expect("register");
    let batches = ctx
        .sql(&lookup_sql(name, id))
        .await
        .expect("plan lookup")
        .collect()
        .await
        .expect("run lookup");
    let found: Vec<i64> = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("AutoId is i64")
                .values()
                .to_vec()
        })
        .collect();
    assert_eq!(
        found,
        vec![id],
        "lookup of {id} on '{name}' returned the wrong rows"
    );
}

async fn dynamic_lookup(provider: &Arc<CayenneTableProvider>, name: &str, ids: &[i64]) -> Vec<i64> {
    let ctx = SessionContext::new();
    ctx.register_table(name, Arc::clone(provider) as Arc<dyn TableProvider>)
        .expect("register target");
    let key_schema = Arc::new(Schema::new(vec![
        Field::new("tenant", DataType::Int64, false),
        Field::new("service", DataType::Utf8, false),
    ]));
    let key_batch = RecordBatch::try_new(
        Arc::clone(&key_schema),
        vec![
            Arc::new(Int64Array::from(
                ids.iter().map(|id| id % 997).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                ids.iter()
                    .map(|id| format!("SV{id:032x}"))
                    .collect::<Vec<_>>(),
            )),
        ],
    )
    .expect("key batch");
    let keys = datafusion::datasource::MemTable::try_new(key_schema, vec![vec![key_batch]])
        .expect("key table");
    ctx.register_table("dynamic_keys", Arc::new(keys))
        .expect("register keys");
    let sql = format!(
        "SELECT s.\"AutoId\" FROM dynamic_keys k INNER JOIN {name} s \
         ON k.tenant = s.\"TenantId\" AND k.service = s.\"ServiceId\" \
         ORDER BY s.\"AutoId\""
    );

    ctx.sql(&sql)
        .await
        .expect("dynamic lookup plan")
        .collect()
        .await
        .expect("dynamic lookup")
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("AutoId")
                .values()
                .to_vec()
        })
        .collect()
}

fn counters(provider: &Arc<CayenneTableProvider>) -> LookupIndexCounters {
    provider
        .lookup_index_counters()
        .expect("the table declares an index")
}

/// Runs lookups over ids `0..rows` until `done` holds, failing with the last
/// counters after `timeout`.
async fn lookups_until(
    provider: &Arc<CayenneTableProvider>,
    name: &str,
    rows: i64,
    timeout: Duration,
    done: impl Fn(&LookupIndexCounters) -> bool,
) -> LookupIndexCounters {
    let deadline = Instant::now() + timeout;
    let mut i = 0i64;
    loop {
        lookup(provider, name, (i * 7919) % rows).await;
        i += 1;
        let now = counters(provider);
        if done(&now) {
            return now;
        }
        assert!(
            Instant::now() < deadline,
            "'{name}' did not reach the expected index state within {timeout:?}: {now:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Runs lookups until a rebuild covers every file of `name` again, failing
/// with the last verification after 30 seconds; returns that verification.
async fn healed(
    provider: &Arc<CayenneTableProvider>,
    name: &str,
) -> cayenne::lookup_index::LookupIndexVerification {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        lookup(provider, name, 7).await;
        let verification = provider
            .verify_lookup_index_against_read_back()
            .await
            .expect("verify");
        if verification.uncovered_files == 0 {
            return verification;
        }
        assert!(
            Instant::now() < deadline,
            "'{name}' was not rebuilt to cover every file: {verification:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// The index follows the `indexes` each registration passes, not whatever the
/// table was first created with: adding an entry to an existing table indexes
/// it, and removing it stops indexing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn indexes_follow_each_registration_not_the_stored_table() {
    const ROWS: usize = 4_000;
    let fixture = common::TestFixture::new(common::BackendType::Sqlite)
        .await
        .expect("fixture");
    let env = Arc::new(RuntimeEnv::default());
    let name = "reregistered";

    {
        let first = open(&fixture, Arc::clone(&env), name, &[]).await;
        overwrite(&first, rows(0, ROWS)).await;
        assert!(first.lookup_index_counters().is_none());
    }

    let added = open(&fixture, Arc::clone(&env), name, &[&KEY]).await;
    let indexed = lookups_until(
        &added,
        name,
        i64::try_from(ROWS).expect("fits"),
        Duration::from_secs(30),
        |c| c.selected > 0,
    )
    .await;
    assert!(indexed.builds_published >= 1, "{indexed:?}");
    drop(added);

    let removed = open(&fixture, Arc::clone(&env), name, &[]).await;
    assert!(
        removed.lookup_index_counters().is_none(),
        "a registration without `indexes` must not index the table"
    );
    lookup(&removed, name, 7).await;
}

/// A runtime join lookup queues the same paced read-back build as a literal
/// lookup. Its first execution scans; a later execution uses the published
/// index without requiring a literal query to prime it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dynamic_lookup_rebuilds_the_index_after_reopen() {
    const ROWS: usize = 4_000;
    let fixture = common::TestFixture::new(common::BackendType::Sqlite)
        .await
        .expect("fixture");
    let env = Arc::new(RuntimeEnv::default());
    let name = "dynamic_rebuild";

    let initial = open(&fixture, Arc::clone(&env), name, &[&KEY]).await;
    overwrite(&initial, rows(0, ROWS)).await;
    drop(initial);

    let reopened = open(&fixture, env, name, &[&KEY]).await;
    reopened.init_scan_view_cache();
    assert_eq!(counters(&reopened).index_bytes, 0);

    let ctx = SessionContext::new();
    ctx.register_table(name, Arc::clone(&reopened) as Arc<dyn TableProvider>)
        .expect("register target");
    let key_schema = Arc::new(Schema::new(vec![
        Field::new("tenant", DataType::Int64, false),
        Field::new("service", DataType::Utf8, false),
    ]));
    let ids = [7i64, 1_234, 3_999];
    let key_batch = RecordBatch::try_new(
        Arc::clone(&key_schema),
        vec![
            Arc::new(Int64Array::from(
                ids.iter().map(|id| id % 997).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                ids.iter()
                    .map(|id| format!("SV{id:032x}"))
                    .collect::<Vec<_>>(),
            )),
        ],
    )
    .expect("key batch");
    let keys = datafusion::datasource::MemTable::try_new(key_schema, vec![vec![key_batch]])
        .expect("key table");
    ctx.register_table("dynamic_keys", Arc::new(keys))
        .expect("register keys");
    let sql = format!(
        "SELECT s.\"AutoId\" FROM dynamic_keys k INNER JOIN {name} s \
         ON k.tenant = s.\"TenantId\" AND k.service = s.\"ServiceId\" \
         ORDER BY s.\"AutoId\""
    );

    let first = ctx
        .sql(&sql)
        .await
        .expect("first plan")
        .collect()
        .await
        .expect("first execution");
    let first = first
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("AutoId")
                .values()
                .to_vec()
        })
        .collect::<Vec<_>>();
    assert_eq!(first, ids);
    let after_first = counters(&reopened);
    assert!(
        after_first.unbuilt > 0,
        "first lookup should scan: {after_first:?}"
    );
    assert_eq!(
        after_first.builds_started, 1,
        "the dynamic lookup should claim one background build: {after_first:?}"
    );

    let deadline = Instant::now() + Duration::from_secs(30);
    while counters(&reopened).builds_published == 0 {
        assert!(
            Instant::now() < deadline,
            "dynamic lookup build did not publish: {:?}",
            counters(&reopened)
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let selected_before = counters(&reopened).selected;
    let second = ctx
        .sql(&sql)
        .await
        .expect("second plan")
        .collect()
        .await
        .expect("second execution");
    assert_eq!(
        second.iter().map(RecordBatch::num_rows).sum::<usize>(),
        ids.len()
    );
    assert_eq!(
        counters(&reopened).selected,
        selected_before + 1,
        "the next dynamic lookup should use the rebuilt index"
    );
}

/// A build the pool cannot fit backs off instead of re-reading the table for
/// every lookup, and lookups keep answering correctly by scanning.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_build_the_pool_cannot_fit_is_not_retried_on_every_lookup() {
    const ROWS: usize = 40_000;
    // A refused build's pause doubles each time, so over the window below builds
    // land at roughly 0s, 2s and 6s: 4s admits two, with one slot of slack.
    const MAX_BUILDS: u64 = 3;
    // Enough lookups that a build on every one of them would be unmissable.
    const MIN_LOOKUPS: u64 = 5 * MAX_BUILDS;
    let fixture = common::TestFixture::new(common::BackendType::Sqlite)
        .await
        .expect("fixture");
    let (env, _pool) = runtime_with_pool(MIB);
    let name = "refused";
    let initial = open(&fixture, Arc::clone(&env), name, &[&KEY]).await;
    overwrite(&initial, rows(0, ROWS)).await;
    // The overwrite's own write-time build is refused by this pool too, and a
    // refused write-time build seeds the very schedule the loop below measures:
    // the next build waits `2 * max(1s, 10 * write_build_time)`, which passes the
    // window as soon as that write takes 200ms. Reopening drops that schedule, so
    // what the first lookup claims is the subject rather than the host's speed.
    drop(initial);
    let table = open(&fixture, env, name, &[&KEY]).await;

    let window = Duration::from_secs(4);
    let started = Instant::now();
    let mut lookups = 0u64;
    let mut i = 0i64;
    while started.elapsed() < window {
        lookup(
            &table,
            name,
            (i * 7919) % i64::try_from(ROWS).expect("fits"),
        )
        .await;
        i += 1;
        lookups += 1;
    }
    let after = counters(&table);
    assert_eq!(
        after.selected, 0,
        "nothing was published to select from: {after:?}"
    );
    assert!(
        after.builds_started >= 1,
        "the first lookup on an unbuilt index must claim a build: {after:?}"
    );
    assert!(
        after.builds_started <= MAX_BUILDS,
        "{lookups} lookups in {window:?} started {} background builds; a refused build must back off: {after:?}",
        after.builds_started
    );
    // How often the loop got to run is not the subject, so the floor stays well
    // clear of what a loaded runner can deliver: an absolute floor high enough
    // to double as a throughput assert fails there 6/6 (#14219).
    assert!(
        lookups >= MIN_LOOKUPS,
        "the lookup loop ran only {lookups} times in {window:?}; too few to show that a refused build backs off: {after:?}"
    );
}

/// Drops a lookup after `polls` polls, as a client disconnect or timeout would.
async fn cancel_after(
    provider: &Arc<CayenneTableProvider>,
    name: &str,
    id: i64,
    polls: usize,
) -> bool {
    let ctx = SessionContext::new();
    ctx.register_table(name, Arc::clone(provider) as Arc<dyn TableProvider>)
        .expect("register");
    let sql = lookup_sql(name, id);
    let query = async { ctx.sql(&sql).await?.collect().await };
    let mut query = std::pin::pin!(query);
    let mut cx = Context::from_waker(std::task::Waker::noop());
    for _ in 0..polls {
        if let Poll::Ready(result) = query.as_mut().poll(&mut cx) {
            result.expect("uncancelled lookup");
            return true;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    false
}

/// A lookup dropped at any point — including while it holds the one background
/// build and lists files for it — leaves the table indexable by the next lookup.
///
/// A claim outlives the query that took it unless dropping the query releases
/// it, and a claim nothing releases keeps every later build from starting.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lookup_dropped_mid_build_claim_does_not_strand_the_index() {
    const ROWS: usize = 40_000;
    let fixture = common::TestFixture::new(common::BackendType::Sqlite)
        .await
        .expect("fixture");
    let env = Arc::new(RuntimeEnv::default());
    for polls in 1..=16usize {
        let name = format!("dropped_after_{polls}_polls");
        let table = open(&fixture, Arc::clone(&env), &name, &[&KEY]).await;
        // An INSERT leaves the snapshot unindexed, so the first lookup claims
        // the background build.
        insert(&table, &name, rows(0, ROWS)).await;
        let completed = cancel_after(&table, &name, 7, polls).await;
        let indexed = lookups_until(
            &table,
            &name,
            i64::try_from(ROWS).expect("fits"),
            Duration::from_secs(30),
            |c| c.builds_published >= 1 && c.selected > 0,
        )
        .await;
        println!("polls={polls} completed_before_drop={completed} counters={indexed:?}");
    }
}

/// Dropping an indexed table returns every byte it reserved in the query pool.
///
/// Anything outside the provider that holds the index — or the table's memory
/// account — keeps the reservation after the table is gone, until restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dropping_an_indexed_table_releases_its_memory() {
    const ROWS: usize = 20_000;
    let fixture = common::TestFixture::new(common::BackendType::Sqlite)
        .await
        .expect("fixture");
    let (env, pool) = runtime_with_pool(1024 * MIB);
    let name = "dropped";
    let table = open(&fixture, Arc::clone(&env), name, &[&KEY]).await;
    overwrite(&table, rows(0, ROWS)).await;
    let used = lookups_until(
        &table,
        name,
        i64::try_from(ROWS).expect("fits"),
        Duration::from_secs(30),
        |c| c.selected > 0,
    )
    .await;
    assert!(used.index_bytes > 0, "{used:?}");
    let reserved = pool.reserved();
    assert!(
        reserved >= usize::try_from(used.index_bytes).expect("fits"),
        "the index must be reserved in the pool: {reserved} reserved, {used:?}"
    );

    drop(table);
    drop(env);
    let deadline = Instant::now() + Duration::from_secs(10);
    while pool.reserved() > 0 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        pool.reserved(),
        0,
        "a dropped table must release its reservation ({reserved} bytes before the drop)"
    );
}

/// Every write indexes the files it writes, so appends and compactions never
/// leave the index stale: the first lookups after each, literal and join
/// alike, are answered from the index with no rebuild, and the index still
/// agrees row for row with a read-back of the files.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn appends_and_compactions_keep_the_index_current() {
    const ROWS: usize = 20_000;
    let fixture = common::TestFixture::new(common::BackendType::Sqlite)
        .await
        .expect("fixture");
    let env = Arc::new(RuntimeEnv::default());
    let name = "appended";
    // Small files compact after four, so the test can drive a compaction.
    let config = VortexConfig {
        target_vortex_file_size_mb: 1,
        compaction_trigger_files: 4,
        compaction_background_interval_ms: 0,
        ..VortexConfig::default()
    };
    let table = open_with(&fixture, Arc::clone(&env), name, &[&KEY], config).await;
    table.init_scan_view_cache();
    overwrite(&table, rows(0, ROWS)).await;
    let rows_i64 = i64::try_from(ROWS).expect("fits");
    // Above the inline cap, so each append writes a file, and small.
    let appended_rows: i64 = 3_000;
    lookups_until(&table, name, rows_i64, Duration::from_secs(30), |c| {
        c.selected > 0
    })
    .await;

    let check = |label: &str, before: &LookupIndexCounters, after: &LookupIndexCounters| {
        assert_eq!(
            (
                after.selected - before.selected,
                after.unbuilt - before.unbuilt,
                after.builds_started - before.builds_started,
            ),
            (2, 0, 0),
            "the literal and join lookups right after {label} were not both answered from the index: {before:?} -> {after:?}"
        );
    };
    for round in 1..=6 {
        let appended = rows_i64 * round;
        insert(
            &table,
            name,
            rows(appended, usize::try_from(appended_rows).expect("fits")),
        )
        .await;
        let before = counters(&table);
        lookup(&table, name, appended + 7).await;
        let ids = [appended, appended + 1, appended + appended_rows - 1];
        assert_eq!(dynamic_lookup(&table, name, &ids).await, ids);
        check(&format!("append {round}"), &before, &counters(&table));
    }

    // A compaction replaces the appended files; one may already be running
    // after the last append.
    let snapshot = |table: &Arc<CayenneTableProvider>| {
        let table = Arc::clone(table);
        async move {
            table
                .verify_lookup_index_against_read_back()
                .await
                .expect("verify")
                .snapshot_id
        }
    };
    let appended_snapshot = snapshot(&table).await;
    let deadline = Instant::now() + Duration::from_secs(30);
    while !table
        .compact_current_snapshot_small_files()
        .await
        .expect("compaction")
        && snapshot(&table).await == appended_snapshot
    {
        assert!(
            Instant::now() < deadline,
            "the appended small files were never compacted"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let before = counters(&table);
    lookup(&table, name, 7).await;
    let ids = [3, rows_i64 + 5, rows_i64 * 6 + appended_rows - 1];
    assert_eq!(dynamic_lookup(&table, name, &ids).await, ids);
    check("a compaction", &before, &counters(&table));

    let verification = table
        .verify_lookup_index_against_read_back()
        .await
        .expect("verify");
    assert!(verification.agrees(), "{verification:?}");
    assert_eq!(
        verification.uncovered_files, 0,
        "every file is indexed by the write that produced it: {verification:?}"
    );
    println!("verification after appends and a compaction: {verification:?}");

    // The index identifies a file by its name alone, across the moves and
    // hardlinks between snapshot directories: two data files may share a
    // name only when they are the same rows.
    let mut by_name: std::collections::HashMap<String, Vec<std::path::PathBuf>> =
        std::collections::HashMap::new();
    vortex_files(&fixture.data_path, &mut by_name);
    assert!(
        by_name.len() >= 2,
        "the table wrote data files: {by_name:?}"
    );
    for (name, paths) in &by_name {
        let first = std::fs::read(&paths[0]).expect("read data file");
        for other in &paths[1..] {
            assert!(
                std::fs::read(other).expect("read data file") == first,
                "two different data files are both named {name}: {paths:?}"
            );
        }
    }
}

/// Every Vortex data file under `dir`, grouped by file name.
fn vortex_files(
    dir: &std::path::Path,
    by_name: &mut std::collections::HashMap<String, Vec<std::path::PathBuf>>,
) {
    for entry in std::fs::read_dir(dir).expect("read dir") {
        let path = entry.expect("dir entry").path();
        if path.is_dir() {
            vortex_files(&path, by_name);
        } else if path.extension().is_some_and(|ext| ext == "vortex") {
            let name = path
                .file_name()
                .expect("file name")
                .to_string_lossy()
                .to_string();
            by_name.entry(name).or_default().push(path);
        }
    }
}

/// A key column relaxed to nullable on an open table: rows written after it,
/// including one whose key column is NULL, are found by their keys, and the
/// rows written before it still are. The change alters the key's encoding, so
/// the index built before it is dropped and rebuilt from the table's files by
/// the next lookups, and then covers the files written before and after it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_key_column_relaxed_to_nullable_on_an_open_table_keeps_lookups_exact() {
    use arrow_tools::schema_evolution::{EvolutionContext, SchemaEvolution, classify};
    const ROWS: usize = 20_000;
    // Rows written after relaxing: enough for files of their own, the last
    // with a NULL key column.
    const AFTER: i64 = 20_000;
    let fixture = common::TestFixture::new(common::BackendType::Sqlite)
        .await
        .expect("fixture");
    let env = Arc::new(RuntimeEnv::default());
    let name = "relaxed_live";
    let table = open(&fixture, Arc::clone(&env), name, &[&KEY]).await;
    overwrite(&table, rows(0, ROWS)).await;
    lookup(&table, name, 11).await;

    let relaxed = Arc::new(Schema::new(vec![
        Field::new("AutoId", DataType::Int64, false),
        Field::new("TenantId", DataType::Int64, false),
        Field::new("ServiceId", DataType::Utf8, true),
        Field::new("Payload", DataType::Utf8, false),
    ]));
    let SchemaEvolution::Widening(plan) = classify(
        table.schema().as_ref(),
        &relaxed,
        &EvolutionContext {
            constraint_columns: &[],
        },
    ) else {
        panic!("relaxing ServiceId to nullable must be a widening");
    };
    table
        .evolve_schema_live(&plan)
        .await
        .expect("relax ServiceId to nullable");

    let first = i64::try_from(ROWS).expect("fits");
    let ids: Vec<i64> = (first..first + AFTER).collect();
    let (id, null_id) = (first + 1, first + AFTER - 1);
    let batch = RecordBatch::try_new(
        Arc::clone(&relaxed),
        vec![
            Arc::new(Int64Array::from(ids.clone())),
            Arc::new(Int64Array::from(
                ids.iter().map(|i| i % 997).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                ids.iter()
                    .map(|&i| (i != null_id).then(|| format!("SV{i:032x}")))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                ids.iter().map(|i| format!("after-{i}")).collect::<Vec<_>>(),
            )),
        ],
    )
    .expect("batch after relaxing");
    let ctx = SessionContext::new();
    ctx.register_table(name, Arc::clone(&table) as Arc<dyn TableProvider>)
        .expect("register target");
    let mem =
        datafusion::datasource::MemTable::try_new(relaxed, vec![vec![batch]]).expect("memtable");
    ctx.register_table("src", Arc::new(mem))
        .expect("register src");
    let inserted = ctx
        .sql(&format!("INSERT INTO {name} SELECT * FROM src"))
        .await
        .expect("insert plan")
        .collect()
        .await;
    println!("insert after relaxing: {:?}", inserted.as_ref().map(|_| ()));
    inserted.expect("insert after relaxing");

    lookup(&table, name, id).await;
    lookup(&table, name, 11).await;
    let nulls = ctx
        .sql(&format!(
            "SELECT \"AutoId\" FROM {name} WHERE \"TenantId\" = {} AND \"ServiceId\" IS NULL",
            null_id % 997
        ))
        .await
        .expect("plan null lookup")
        .collect()
        .await
        .expect("run null lookup");
    let found: usize = nulls.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(found, 1, "the row with a NULL key column must be read");
    let verification = table
        .verify_lookup_index_against_read_back()
        .await
        .expect("verify");
    println!("after relaxing on an open table: {verification:?}");
    assert!(verification.agrees(), "{verification:?}");
    // The index heals: a lookup over the uncovered files rebuilds it, after
    // which every file is covered again and lookups use it.
    let healed = healed(&table, name).await;
    println!("after the rebuild: {healed:?}");
    assert!(healed.agrees(), "{healed:?}");
    let before = counters(&table);
    lookup(&table, name, 7).await;
    assert_eq!(
        counters(&table).selected - before.selected,
        1,
        "a lookup after the rebuild must use the index"
    );
}
