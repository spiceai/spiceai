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

use common::lookup_index::{
    TableSpec, counters, insert, int64_column, open_table, overwrite, poll_until, query,
    runtime_with_pool, until_covered,
};

use async_trait::async_trait;
use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Notify;

use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use cayenne::lookup_index::{IndexPersistence, LookupIndexCounters};
use cayenne::metadata::{IndexRunRecord, VortexConfig};
use cayenne::{CayenneTableProvider, MetadataCatalog};

use datafusion::datasource::TableProvider;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::prelude::SessionContext;
use tracing::instrument::WithSubscriber;

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

/// Creates the file-mode table, or reopens it when the catalog already holds
/// one of this name — the same call the accelerator makes on every
/// registration.
async fn open(
    fixture: &common::TestFixture,
    runtime_env: Arc<RuntimeEnv>,
    name: &str,
    indexes: &[&[&str]],
) -> Arc<CayenneTableProvider> {
    open_table(
        fixture,
        runtime_env,
        TableSpec::new(name, schema(), indexes),
    )
    .await
}

/// [`open`], with a table config and index persistence of the test's choosing.
async fn open_configured(
    fixture: &common::TestFixture,
    runtime_env: Arc<RuntimeEnv>,
    name: &str,
    indexes: &[&[&str]],
    vortex_config: VortexConfig,
    persistence: IndexPersistence,
) -> Arc<CayenneTableProvider> {
    open_table(
        fixture,
        runtime_env,
        TableSpec::new(name, schema(), indexes)
            .config(vortex_config)
            .persistence(persistence),
    )
    .await
}

/// Looks up `id` and checks the table returns exactly its one row.
async fn lookup(provider: &Arc<CayenneTableProvider>, name: &str, id: i64) {
    let found = int64_column(&query(provider, name, &lookup_sql(name, id)).await);
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

    int64_column(
        &ctx.sql(&sql)
            .await
            .expect("dynamic lookup plan")
            .collect()
            .await
            .expect("dynamic lookup"),
    )
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
    let mut i = 0i64;
    poll_until(
        timeout,
        Duration::from_millis(20),
        async || {
            lookup(provider, name, (i * 7919) % rows).await;
            i += 1;
            let now = counters(provider);
            if done(&now) { Ok(now) } else { Err(now) }
        },
        |now| format!("'{name}' did not reach the expected index state: {now:?}"),
    )
    .await
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
        overwrite(&first, vec![rows(0, ROWS)]).await;
        assert!(first.lookup_index_counters().is_none());
    }

    let added = open(&fixture, Arc::clone(&env), name, &[&KEY]).await;
    let indexed = lookups_until(
        &added,
        name,
        i64::try_from(ROWS).expect("fits"),
        Duration::from_secs(30),
        |c| c.full > 0,
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
    overwrite(&initial, vec![rows(0, ROWS)]).await;
    drop(initial);

    let reopened = open(&fixture, env, name, &[&KEY]).await;
    reopened.init_scan_view_cache();
    assert_eq!(counters(&reopened).index_bytes, 0);

    let ids = [7i64, 1_234, 3_999];
    let first = dynamic_lookup(&reopened, name, &ids).await;
    assert_eq!(first, ids);
    let after_first = counters(&reopened);
    assert!(
        after_first.none > 0,
        "first lookup should scan: {after_first:?}"
    );
    assert_eq!(
        after_first.builds_started, 1,
        "the dynamic lookup should claim one background build: {after_first:?}"
    );

    poll_until(
        Duration::from_secs(30),
        Duration::from_millis(20),
        async || {
            let now = counters(&reopened);
            if now.builds_published == 0 {
                Err(now)
            } else {
                Ok(())
            }
        },
        |now| format!("dynamic lookup build did not publish: {now:?}"),
    )
    .await;

    let full_before = counters(&reopened).full;
    let second = dynamic_lookup(&reopened, name, &ids).await;
    assert_eq!(second.len(), ids.len());
    assert_eq!(
        counters(&reopened).full,
        full_before + 1,
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
    overwrite(&initial, vec![rows(0, ROWS)]).await;
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
        after.full, 0,
        "nothing was published to select from: {after:?}"
    );
    assert!(
        after.builds_started >= 1,
        "the first lookup on an index that covers nothing must claim a build: {after:?}"
    );
    assert!(
        after.builds_started <= MAX_BUILDS,
        "{lookups} lookups in {window:?} started {} background builds; a refused build must back off: {after:?}",
        after.builds_started
    );
    assert!(
        after.builds_unpublished <= after.builds_started,
        "each refused build is counted once as unpublished: {after:?}"
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
            |c| c.builds_published >= 1 && c.full > 0,
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
    overwrite(&table, vec![rows(0, ROWS)]).await;
    let used = lookups_until(
        &table,
        name,
        i64::try_from(ROWS).expect("fits"),
        Duration::from_secs(30),
        |c| c.full > 0,
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
    poll_until(
        Duration::from_secs(10),
        Duration::from_millis(50),
        async || match pool.reserved() {
            0 => Ok(()),
            still => Err(still),
        },
        |still| {
            format!(
                "a dropped table must release its reservation: {still} bytes still reserved, \
                 {reserved} before the drop"
            )
        },
    )
    .await;
}

/// Every write indexes the files it writes, so appends and compactions never
/// leave the index stale: the first lookups after each, literal and join
/// alike, are answered from the index with no rebuild, and the index still
/// agrees row for row with a read-back of the files.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn appends_and_compactions_keep_the_index_current() {
    const ROWS: usize = 20_000;
    const ROUNDS: usize = 6;
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
    let table = open_table(
        &fixture,
        Arc::clone(&env),
        TableSpec::new(name, schema(), &[&KEY]).config(config),
    )
    .await;
    table.init_scan_view_cache();
    overwrite(&table, vec![rows(0, ROWS)]).await;
    let rows_i64 = i64::try_from(ROWS).expect("fits");
    // Above the inline cap, so each append writes a file, and small.
    let appended_rows: i64 = 3_000;
    lookups_until(&table, name, rows_i64, Duration::from_secs(30), |c| {
        c.full > 0
    })
    .await;
    // The table's current data files, indexed or not.
    let file_count = |table: &Arc<CayenneTableProvider>| {
        let table = Arc::clone(table);
        async move {
            let verification = table
                .verify_lookup_index_against_read_back()
                .await
                .expect("verify");
            verification.files + verification.uncovered_files
        }
    };
    let base_files = file_count(&table).await;

    let check = |label: &str, before: &LookupIndexCounters, after: &LookupIndexCounters| {
        assert_eq!(
            (
                after.full - before.full,
                after.none - before.none,
                after.builds_started - before.builds_started,
            ),
            (2, 0, 0),
            "the literal and join lookups right after {label} were not both answered from the index: {before:?} -> {after:?}"
        );
    };
    for round in 1..=i64::try_from(ROUNDS).expect("fits") {
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

    // A compaction replaces the appended files. One may already be running
    // after the last append, or have finished: each append wrote at least one
    // file, so fewer files than the appends left means one already ran.
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
    poll_until(
        Duration::from_secs(30),
        Duration::from_millis(50),
        async || {
            let pending = !table
                .compact_current_snapshot_small_files()
                .await
                .expect("compaction")
                && snapshot(&table).await == appended_snapshot
                && file_count(&table).await >= base_files + ROUNDS;
            if pending { Err(()) } else { Ok(()) }
        },
        |()| "the appended small files were never compacted".to_string(),
    )
    .await;
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

/// Run files persisted under `dir`.
fn run_file_count(dir: &std::path::Path) -> usize {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    entries
        .map(|entry| entry.expect("dir entry").path())
        .map(|path| {
            if path.is_dir() {
                run_file_count(&path)
            } else {
                usize::from(path.extension().is_some_and(|ext| ext == "run"))
            }
        })
        .sum()
}

/// The runs the metastore records as persisted for table `name`.
async fn registered_runs(fixture: &common::TestFixture, name: &str) -> Vec<IndexRunRecord> {
    let table_id = fixture
        .catalog
        .get_table(name)
        .await
        .expect("table metadata")
        .table_id;
    fixture
        .catalog
        .list_index_runs(&table_id)
        .await
        .expect("list persisted runs")
}

/// Waits until at least `runs` runs are registered and every run file on
/// disk is a registered one.
async fn wait_for_persisted_runs(fixture: &common::TestFixture, name: &str, runs: usize) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let registered = registered_runs(fixture, name).await.len();
        let files = run_file_count(&fixture.data_path);
        if registered >= runs && files == registered {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "the runs were not persisted: {registered} registered, {files} run files"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Every run file under `dir`.
fn run_files(dir: &std::path::Path, found: &mut Vec<std::path::PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries {
        let path = entry.expect("dir entry").path();
        if path.is_dir() {
            run_files(&path, found);
        } else if path.extension().is_some_and(|ext| ext == "run") {
            found.push(path);
        }
    }
}

/// The metastore, not a directory listing, decides which persisted runs a reopened
/// table loads. A run file with no registered run (a write that stopped
/// before registering it) is deleted; a registered run whose file cannot be
/// read, or is missing, is unregistered and its files are indexed again. The
/// loaded index still agrees with a read-back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_metastore_decides_which_persisted_runs_a_reopened_table_loads() {
    const ROWS: usize = 20_000;
    let fixture = common::TestFixture::new(common::BackendType::Sqlite)
        .await
        .expect("fixture");
    let env = Arc::new(RuntimeEnv::default());
    let name = "registered";
    let config = || VortexConfig {
        target_vortex_file_size_mb: 1,
        ..VortexConfig::default()
    };
    let table = open_configured(
        &fixture,
        Arc::clone(&env),
        name,
        &[&KEY],
        config(),
        IndexPersistence::Enabled,
    )
    .await;
    overwrite(&table, vec![rows(0, ROWS)]).await;
    let rows_i64 = i64::try_from(ROWS).expect("fits");
    insert(&table, name, rows(rows_i64, 3_000)).await;
    insert(&table, name, rows(rows_i64 * 2, 3_000)).await;
    wait_for_persisted_runs(&fixture, name, 3).await;
    drop(table);

    let registered = registered_runs(&fixture, name).await;
    let mut files = Vec::new();
    run_files(&fixture.data_path, &mut files);
    let file_of = |record: &IndexRunRecord| {
        files
            .iter()
            .find(|path| {
                path.file_name().is_some_and(|n| *n == *record.run_name)
                    && path
                        .parent()
                        .and_then(|dir| dir.file_name())
                        .is_some_and(|n| *n == *record.index_key)
            })
            .cloned()
            .expect("every registered run has its file")
    };
    // A registered run whose file is corrupt.
    let corrupt = registered[0].clone();
    let corrupt_file = file_of(&corrupt);
    std::fs::write(&corrupt_file, b"not a run").expect("corrupt a persisted run");
    // A run file no run is registered for.
    let orphan_file = corrupt_file.with_file_name("00000000deadbeef.run");
    std::fs::copy(file_of(&registered[1]), &orphan_file).expect("write an orphan persisted run");
    // A registered run with no file.
    let phantom = IndexRunRecord {
        run_name: "00000000feedface.run".to_string(),
        ..registered[1].clone()
    };
    fixture
        .catalog
        .register_index_run(&phantom)
        .await
        .expect("register a run with no file");

    let reopened = open_configured(
        &fixture,
        Arc::clone(&env),
        name,
        &[&KEY],
        config(),
        IndexPersistence::Enabled,
    )
    .await;
    let after: Vec<String> = registered_runs(&fixture, name)
        .await
        .into_iter()
        .map(|record| record.run_name)
        .collect();
    assert!(
        !orphan_file.exists(),
        "a run file with no registered run must be deleted at open"
    );
    assert!(
        !after.contains(&corrupt.run_name) && !corrupt_file.exists(),
        "a registered run that cannot be read must be unregistered and deleted: {after:?}"
    );
    assert!(
        !after.contains(&phantom.run_name),
        "a registered run with no file must be unregistered: {after:?}"
    );
    assert_eq!(
        after.len(),
        registered.len() - 1,
        "the readable runs stay registered: {after:?}"
    );
    let verification = reopened
        .verify_lookup_index_against_read_back()
        .await
        .expect("verify");
    println!(
        "after reopening with a corrupt, an orphan and a phantom persisted run: {verification:?}"
    );
    assert!(verification.agrees(), "{verification:?}");
    assert!(
        verification.uncovered_files > 0,
        "the corrupt run's files are no longer covered by a loaded run: {verification:?}"
    );
    let ids = [3, rows_i64 + 5, rows_i64 * 2 + 7];
    assert_eq!(dynamic_lookup(&reopened, name, &ids).await, ids);
}

/// Distinct composite keys with the same display label keep separate persisted
/// identities, so reopening with a different key never loads the other key's
/// postings as coverage for the new key.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn composite_keys_with_the_same_label_do_not_reuse_persisted_runs() {
    const NAME: &str = "ambiguous_key_labels";
    const ROWS: i64 = 20_000;
    let fixture = common::TestFixture::new(common::BackendType::Sqlite)
        .await
        .expect("fixture");
    let env = Arc::new(RuntimeEnv::default());
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("a, b", DataType::Int64, false),
        Field::new("c", DataType::Int64, false),
        Field::new("a", DataType::Int64, false),
        Field::new("b, c", DataType::Int64, false),
    ]));
    let first = open_table(
        &fixture,
        Arc::clone(&env),
        TableSpec::new(NAME, Arc::clone(&schema), &[&["a, b", "c"]])
            .persistence(IndexPersistence::Enabled),
    )
    .await;
    let columns = (0..5_i64)
        .map(|column| {
            Arc::new(Int64Array::from_iter_values(
                (0..ROWS).map(|id| id + column * 100_000),
            )) as arrow::array::ArrayRef
        })
        .collect();
    overwrite(
        &first,
        vec![RecordBatch::try_new(Arc::clone(&schema), columns).expect("batch")],
    )
    .await;
    wait_for_persisted_runs(&fixture, NAME, 1).await;
    let original = registered_runs(&fixture, NAME).await;
    assert!(!original.is_empty(), "the original key must be persisted");
    drop(first);

    let reopened = open_table(
        &fixture,
        env,
        TableSpec::new(NAME, schema, &[&["a", "b, c"]]).persistence(IndexPersistence::Enabled),
    )
    .await;
    let verification = reopened
        .verify_lookup_index_against_read_back()
        .await
        .expect("verify reopened index");
    let sql = "SELECT id FROM ambiguous_key_labels WHERE a = 300007 AND \"b, c\" = 400007";
    assert_eq!(
        int64_column(&query(&reopened, NAME, sql).await),
        vec![7],
        "the first lookup must find the row through the uncovered-file fallback"
    );
    assert!(
        verification.uncovered_files > 0,
        "the new key must not load coverage from the other key: {verification:?}"
    );
    until_covered(&reopened, async || {
        assert_eq!(int64_column(&query(&reopened, NAME, sql).await), vec![7]);
    })
    .await;
    wait_for_persisted_runs(&fixture, NAME, 1).await;
    let rebuilt = registered_runs(&fixture, NAME).await;
    assert!(
        rebuilt
            .iter()
            .all(|run| original.iter().all(|old| old.index_key != run.index_key)),
        "the two keys must have distinct persisted identities: {original:?} -> {rebuilt:?}"
    );
    let before = counters(&reopened);
    assert_eq!(int64_column(&query(&reopened, NAME, sql).await), vec![7]);
    assert_eq!(counters(&reopened).full - before.full, 1);
}

/// Removing the last configured index removes its registrations and run files,
/// including files left without a registration, while the table remains readable.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn removing_every_index_cleans_up_its_persisted_runs() {
    const NAME: &str = "removed_indexes";
    let fixture = common::TestFixture::new(common::BackendType::Sqlite)
        .await
        .expect("fixture");
    let env = Arc::new(RuntimeEnv::default());
    let table = open_table(
        &fixture,
        Arc::clone(&env),
        TableSpec::new(NAME, schema(), &[&KEY]).persistence(IndexPersistence::Enabled),
    )
    .await;
    overwrite(&table, vec![rows(0, 20_000)]).await;
    wait_for_persisted_runs(&fixture, NAME, 1).await;
    drop(table);
    let mut files = Vec::new();
    run_files(&fixture.data_path, &mut files);
    let registered_file = files.first().expect("a persisted run file");
    std::fs::copy(
        registered_file,
        registered_file.with_file_name("00000000deadbeef.run"),
    )
    .expect("create an unregistered run file");

    let reopened = open_table(
        &fixture,
        env,
        TableSpec::new(NAME, schema(), &[]).persistence(IndexPersistence::Enabled),
    )
    .await;
    let registered = registered_runs(&fixture, NAME).await;
    let remaining_files = run_file_count(&fixture.data_path);
    println!(
        "after removing every index: registrations={registered:?}, run_files={remaining_files}"
    );
    assert!(
        registered.is_empty() && remaining_files == 0,
        "removing every index must clean up registered and orphan runs: {registered:?}, {remaining_files} files"
    );
    lookup(&reopened, NAME, 7).await;
    assert!(reopened.lookup_index_counters().is_none());
}

/// Failed unregistering must retain the file while the metastore still owns it;
/// the orphan sweep can remove unrelated files and a later open retries cleanup.
async fn failed_unregistration_retains_run_file(corrupt: bool) {
    const NAME: &str = "failed_unregistration";
    let fixture = common::TestFixture::new(common::BackendType::Sqlite)
        .await
        .expect("fixture");
    let env = Arc::new(RuntimeEnv::default());
    let table = open_table(
        &fixture,
        Arc::clone(&env),
        TableSpec::new(NAME, schema(), &[&KEY]).persistence(IndexPersistence::Enabled),
    )
    .await;
    overwrite(&table, vec![rows(0, 20_000)]).await;
    wait_for_persisted_runs(&fixture, NAME, 1).await;
    drop(table);
    let registered = registered_runs(&fixture, NAME).await;
    let mut files = Vec::new();
    run_files(&fixture.data_path, &mut files);
    let run_file = files.first().expect("a persisted run file");
    let orphan = run_file.with_file_name("00000000deadbeef.run");
    std::fs::copy(run_file, &orphan).expect("create an orphan run");
    if corrupt {
        std::fs::write(run_file, b"unreadable run").expect("corrupt registered run");
    }
    let expected_bytes = std::fs::read(run_file).expect("read registered file");
    let metastore = rusqlite::Connection::open(fixture.db_path()).expect("open SQLite metastore");
    metastore
        .execute_batch(
            "CREATE TRIGGER reject_index_run_delete BEFORE DELETE ON cayenne_index_run BEGIN SELECT RAISE(ABORT, 'injected unregister failure'); END;",
        )
        .expect("install failing unregister trigger");
    let indexes: &[&[&str]] = if corrupt { &[&KEY] } else { &[] };
    let reopened = open_table(
        &fixture,
        Arc::clone(&env),
        TableSpec::new(NAME, schema(), indexes).persistence(IndexPersistence::Enabled),
    )
    .await;
    let retained = registered_runs(&fixture, NAME).await;
    let file_exists = run_file.exists();
    println!(
        "after rejected unregister (corrupt={corrupt}): registrations={retained:?}, registered_file_exists={file_exists}, orphan_exists={}",
        orphan.exists()
    );
    assert_eq!(
        retained, registered,
        "the trigger must retain the registration"
    );
    assert!(
        file_exists,
        "a run must retain its file when unregistering fails"
    );
    assert_eq!(
        std::fs::read(run_file).expect("retained registered file"),
        expected_bytes,
        "failed cleanup must not change the registered file"
    );
    assert!(
        !orphan.exists(),
        "unregistered files must still be cleaned up"
    );
    drop(reopened);
    metastore
        .execute_batch("DROP TRIGGER reject_index_run_delete")
        .expect("allow cleanup retry");
    let retried = open_table(
        &fixture,
        env,
        TableSpec::new(NAME, schema(), &[]).persistence(IndexPersistence::Enabled),
    )
    .await;
    assert!(registered_runs(&fixture, NAME).await.is_empty());
    assert_eq!(run_file_count(&fixture.data_path), 0);
    lookup(&retried, NAME, 7).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_unregistration_keeps_a_removed_index_run_file() {
    failed_unregistration_retains_run_file(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_unregistration_keeps_an_unreadable_index_run_file() {
    failed_unregistration_retains_run_file(true).await;
}

/// The load message counts the intersection of coverage across index keys.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reopened_disjoint_index_runs_report_no_fully_covered_files() {
    #[derive(Clone)]
    struct Capture(Arc<std::sync::Mutex<Vec<u8>>>);
    impl std::io::Write for Capture {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0
                .lock()
                .map_err(|error| std::io::Error::other(error.to_string()))?
                .extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let fixture = common::TestFixture::new(common::BackendType::Sqlite)
        .await
        .expect("fixture");
    let env = Arc::new(RuntimeEnv::default());
    let name = "disjoint_persisted_runs";
    let indexes: &[&[&str]] = &[&["AutoId"], &KEY];
    let config = || VortexConfig {
        target_vortex_file_size_mb: 1,
        ..VortexConfig::default()
    };
    let table = open_configured(
        &fixture,
        Arc::clone(&env),
        name,
        indexes,
        config(),
        IndexPersistence::Enabled,
    )
    .await;
    overwrite(&table, vec![rows(0, 20_000)]).await;
    wait_for_persisted_runs(&fixture, name, 2).await;
    let first = registered_runs(&fixture, name).await;
    assert_eq!(first.len(), 2);
    insert(&table, name, rows(20_000, 3_000)).await;
    wait_for_persisted_runs(&fixture, name, 4).await;
    let all = registered_runs(&fixture, name).await;
    assert_eq!(all.len(), 4);
    let remove_first = &first[0];
    let remove_second = all
        .iter()
        .find(|run| {
            run.index_key != remove_first.index_key
                && !first
                    .iter()
                    .any(|old| old.index_key == run.index_key && old.run_name == run.run_name)
        })
        .expect("other key's second write run");
    drop(table);
    for run in [remove_first, remove_second] {
        fixture
            .catalog
            .remove_index_run(&run.table_id, &run.index_key, &run.run_name)
            .await
            .expect("omit a persisted run");
    }
    let captured = Arc::new(std::sync::Mutex::new(Vec::new()));
    let writer = Capture(Arc::clone(&captured));
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .without_time()
        .with_max_level(tracing::Level::INFO)
        .with_writer(move || writer.clone())
        .finish();
    // tracing-core's single-dispatcher callsite cache consults the registering
    // thread's default subscriber. Other tests can register the shared load
    // event without this subscriber; a second dispatcher makes interest depend
    // on all registered subscribers instead.
    let _callsite_dispatch = tracing::Dispatch::new(tracing_subscriber::fmt().finish());
    let reopened = open_configured(
        &fixture,
        env,
        name,
        indexes,
        config(),
        IndexPersistence::Enabled,
    )
    .with_subscriber(subscriber)
    .await;
    let output =
        String::from_utf8(captured.lock().expect("captured log").clone()).expect("UTF-8 log");
    println!("reopened coverage log: {output}");
    assert!(output.contains("covering 0 of its 4 files"), "{output}");
    lookup(&reopened, name, 7).await;
    lookup(&reopened, name, 20_007).await;
}

/// With persisted runs, a reopened table loads its index runs instead of reading
/// its files back: on reopen every file is covered before any build runs, and
/// the loaded runs agree row for row with a read-back. Without them the same
/// reopen starts uncovered.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reopened_table_loads_its_persisted_runs() {
    const ROWS: usize = 20_000;
    let fixture = common::TestFixture::new(common::BackendType::Sqlite)
        .await
        .expect("fixture");
    let env = Arc::new(RuntimeEnv::default());
    let name = "persisted_runs";
    let config = || VortexConfig {
        target_vortex_file_size_mb: 1,
        ..VortexConfig::default()
    };
    let table = open_configured(
        &fixture,
        Arc::clone(&env),
        name,
        &[&KEY],
        config(),
        IndexPersistence::Enabled,
    )
    .await;
    overwrite(&table, vec![rows(0, ROWS)]).await;
    let rows_i64 = i64::try_from(ROWS).expect("fits");
    insert(&table, name, rows(rows_i64, 3_000)).await;
    insert(&table, name, rows(rows_i64 * 2, 3_000)).await;
    // One run per write, each persisted in the background.
    wait_for_persisted_runs(&fixture, name, 3).await;
    drop(table);

    let reopened = open_configured(
        &fixture,
        Arc::clone(&env),
        name,
        &[&KEY],
        config(),
        IndexPersistence::Enabled,
    )
    .await;
    let verification = reopened
        .verify_lookup_index_against_read_back()
        .await
        .expect("verify");
    println!("after reopening with persisted runs: {verification:?}");
    assert!(verification.agrees(), "{verification:?}");
    assert_eq!(
        (
            verification.uncovered_files,
            counters(&reopened).builds_started
        ),
        (0, 0),
        "a reopened table must be covered by its persisted runs, not by a build: {verification:?}"
    );
    let before = counters(&reopened);
    lookup(&reopened, name, rows_i64 * 2 + 7).await;
    let after = counters(&reopened);
    assert_eq!(
        (after.full - before.full, after.none - before.none),
        (1, 0),
        "the first lookup after reopening did not use the loaded index: {after:?}"
    );
    drop(reopened);

    let without = open_configured(
        &fixture,
        Arc::clone(&env),
        name,
        &[&KEY],
        config(),
        IndexPersistence::Disabled,
    )
    .await;
    let verification = without
        .verify_lookup_index_against_read_back()
        .await
        .expect("verify");
    assert!(
        verification.files == 0 && verification.uncovered_files > 0,
        "without persisted runs a reopened table starts uncovered: {verification:?}"
    );
}

/// A table that cannot list its persisted runs when it opens loads none of
/// them, and must not then delete them as unwanted: they are still valid,
/// and the next open loads them. The listing is made to fail by hiding the
/// metastore table that registers the runs while the table opens.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_load_of_the_persisted_runs_deletes_none_of_them() {
    const ROWS: usize = 20_000;
    let fixture = common::TestFixture::new(common::BackendType::Sqlite)
        .await
        .expect("fixture");
    let env = Arc::new(RuntimeEnv::default());
    let name = "load_failure";
    let config = || VortexConfig {
        target_vortex_file_size_mb: 1,
        ..VortexConfig::default()
    };
    let open_persisted = || {
        open_configured(
            &fixture,
            Arc::clone(&env),
            name,
            &[&KEY],
            config(),
            IndexPersistence::Enabled,
        )
    };
    let table = open_persisted().await;
    overwrite(&table, vec![rows(0, ROWS)]).await;
    let rows_i64 = i64::try_from(ROWS).expect("fits");
    insert(&table, name, rows(rows_i64, 3_000)).await;
    wait_for_persisted_runs(&fixture, name, 2).await;
    drop(table);
    let mut persisted: Vec<String> = registered_runs(&fixture, name)
        .await
        .into_iter()
        .map(|run| run.run_name)
        .collect();
    persisted.sort();

    let metastore = rusqlite::Connection::open(fixture.temp_dir.path().join("test.db"))
        .expect("open metastore");
    metastore
        .execute_batch("ALTER TABLE cayenne_index_run RENAME TO cayenne_index_run_hidden")
        .expect("hide the registered runs");
    let reopened = open_persisted().await;
    metastore
        .execute_batch("ALTER TABLE cayenne_index_run_hidden RENAME TO cayenne_index_run")
        .expect("restore the registered runs");
    // A change that would sync the runs: had the failed load enabled syncing,
    // this write's run would be persisted and the unloaded runs deleted. An
    // absence has to be waited out, so the wait is bounded.
    insert(&reopened, name, rows(rows_i64 * 2, 3_000)).await;
    let deadline = Instant::now() + Duration::from_secs(3);
    while Instant::now() < deadline && registered_runs(&fixture, name).await.len() <= 2 {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let mut after: Vec<String> = registered_runs(&fixture, name)
        .await
        .into_iter()
        .map(|run| run.run_name)
        .collect();
    after.sort();
    assert!(
        persisted.iter().all(|run| after.contains(run)),
        "the runs the table could not load were deleted: {persisted:?} -> {after:?}"
    );
    drop(reopened);

    let next = open_persisted().await;
    let verification = next
        .verify_lookup_index_against_read_back()
        .await
        .expect("verify");
    assert!(
        verification.agrees() && verification.files > 0,
        "the next open loads the runs the failed load left in place: {verification:?}"
    );
    lookup(&next, name, 7).await;
    lookup(&next, name, rows_i64 * 2 + 7).await;
}

/// Refusing persisted runs preserves them for an open with a larger budget.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn persisted_runs_refused_by_the_pool_remain_registered() {
    let fixture = common::TestFixture::new(common::BackendType::Sqlite)
        .await
        .expect("fixture");
    let name = "persisted_memory_refusal";
    let config = || VortexConfig {
        target_vortex_file_size_mb: 1,
        ..VortexConfig::default()
    };
    let table = open_configured(
        &fixture,
        Arc::new(RuntimeEnv::default()),
        name,
        &[&KEY],
        config(),
        IndexPersistence::Enabled,
    )
    .await;
    overwrite(&table, vec![rows(0, 20_000)]).await;
    wait_for_persisted_runs(&fixture, name, 1).await;
    drop(table);
    let before = registered_runs(&fixture, name).await;
    let mut paths = Vec::new();
    run_files(&fixture.data_path, &mut paths);
    let files: Vec<_> = paths
        .into_iter()
        .map(|path| {
            let bytes = std::fs::read(&path).expect("read persisted run");
            (path, bytes)
        })
        .collect();
    let (env, pool) = runtime_with_pool(128 * 1024);
    let reopened = open_configured(
        &fixture,
        env,
        name,
        &[&KEY],
        config(),
        IndexPersistence::Enabled,
    )
    .await;
    assert_eq!(counters(&reopened).index_bytes, 0);
    // Opening schedules the first sync. Give it time to finish before checking
    // that budget refusal did not turn valid runs into unwanted files.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let after = registered_runs(&fixture, name).await;
    println!(
        "persisted memory refusal: registered_before={} registered_after={} pool_reserved={} limit={}",
        before.len(),
        after.len(),
        pool.reserved(),
        128 * 1024
    );
    assert_eq!(after, before, "budget refusal must retain runs");
    for (path, bytes) in &files {
        assert_eq!(std::fs::read(path).expect("retained persisted run"), *bytes);
    }
    lookup(&reopened, name, 7).await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(registered_runs(&fixture, name).await, before);
    for (path, bytes) in &files {
        assert_eq!(
            std::fs::read(path).expect("run retained after lookup"),
            *bytes
        );
    }
    drop(reopened);
    let next = open_configured(
        &fixture,
        Arc::new(RuntimeEnv::default()),
        name,
        &[&KEY],
        config(),
        IndexPersistence::Enabled,
    )
    .await;
    let verification = next
        .verify_lookup_index_against_read_back()
        .await
        .expect("verify");
    println!("after budget refusal and reopen: {verification:?}");
    assert!(verification.agrees(), "{verification:?}");
    assert_eq!(verification.uncovered_files, 0);
    lookup(&next, name, 7).await;
}

/// A valid empty persisted frame completes loading and leaves data uncovered.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_empty_persisted_run_completes_loading() {
    let fixture = common::TestFixture::new(common::BackendType::Sqlite)
        .await
        .expect("fixture");
    let env = Arc::new(RuntimeEnv::default());
    let name = "empty_persisted_run";
    let table = open_configured(
        &fixture,
        Arc::clone(&env),
        name,
        &[&KEY],
        VortexConfig::default(),
        IndexPersistence::Enabled,
    )
    .await;
    overwrite(&table, vec![rows(0, 20_000)]).await;
    wait_for_persisted_runs(&fixture, name, 1).await;
    drop(table);
    let mut paths = Vec::new();
    run_files(&fixture.data_path, &mut paths);
    let original_path = paths.first().expect("persisted run");
    let original = std::fs::read(original_path).expect("run bytes");
    // Preserve the frame and encoder, with no files, rows, words or postings.
    let mut empty = original[..20].to_vec();
    empty.extend_from_slice(&0_u32.to_le_bytes());
    empty.extend_from_slice(&[0_u8; 24]);
    let checksum = hash_index::hash_key_bytes_oneshot(&empty);
    empty.extend_from_slice(&checksum.to_le_bytes());
    let decoded = key_index::tiered::IndexRun::from_bytes(&empty).expect("valid empty run");
    println!(
        "empty persisted bounds: decode={} publication={}",
        key_index::tiered::IndexRun::decode_memory_bound(&empty).expect("decode bound"),
        decoded
            .publication_memory_bound()
            .expect("publication bound")
    );
    let empty_name = format!(
        "{:016x}.run",
        hash_index::hash_key_bytes(&[&0_u64.to_le_bytes()])
    );
    let empty_path = original_path.with_file_name(&empty_name);
    std::fs::write(&empty_path, &empty).expect("write empty frame");
    let metastore = rusqlite::Connection::open(fixture.db_path()).expect("metastore");
    assert_eq!(
        metastore
            .execute(
                "UPDATE cayenne_index_run SET run_name = ?1, row_count = 0, size_bytes = ?2",
                rusqlite::params![
                    empty_name,
                    i64::try_from(empty.len()).expect("frame size fits")
                ]
            )
            .expect("register empty frame"),
        1
    );
    std::fs::remove_file(original_path).expect("remove replaced run");
    drop(metastore);
    let reopened = open_configured(
        &fixture,
        env,
        name,
        &[&KEY],
        VortexConfig::default(),
        IndexPersistence::Enabled,
    )
    .await;
    poll_until(
        Duration::from_secs(10),
        Duration::from_millis(50),
        async || {
            let remaining = registered_runs(&fixture, name).await.len();
            if remaining == 0 {
                Ok(())
            } else {
                Err(remaining)
            }
        },
        |remaining| format!("empty run loading did not finish: {remaining} registrations remain"),
    )
    .await;
    assert_eq!(counters(&reopened).index_bytes, 0);
    lookup(&reopened, name, 7).await;
}

/// Relaxing a key column from `NOT NULL` to nullable is an in-place schema
/// evolution: the table keeps its id and its persisted runs. A nullable column
/// encodes each value behind a validity byte, so a run built while the column
/// was `NOT NULL` holds other words for the same keys. A reopened table must not
/// answer lookups from those runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_key_column_relaxed_to_nullable_does_not_reuse_its_persisted_runs() {
    const ROWS: usize = 20_000;
    let fixture = common::TestFixture::new(common::BackendType::Sqlite)
        .await
        .expect("fixture");
    let env = Arc::new(RuntimeEnv::default());
    let name = "relaxed";
    let config = || VortexConfig {
        target_vortex_file_size_mb: 1,
        ..VortexConfig::default()
    };
    let table = open_configured(
        &fixture,
        Arc::clone(&env),
        name,
        &[&KEY],
        config(),
        IndexPersistence::Enabled,
    )
    .await;
    overwrite(&table, vec![rows(0, ROWS)]).await;
    wait_for_persisted_runs(&fixture, name, 1).await;
    drop(table);

    let stored = fixture
        .catalog
        .get_table(name)
        .await
        .expect("table metadata");
    let relaxed = Arc::new(Schema::new(
        stored
            .schema
            .fields()
            .iter()
            .map(|field| {
                let field = field.as_ref().clone();
                if field.name() == "ServiceId" {
                    field.with_nullable(true)
                } else {
                    field
                }
            })
            .collect::<Vec<_>>(),
    ));
    fixture
        .catalog
        .update_table_schema(&stored.table_id, &relaxed)
        .await
        .expect("relax ServiceId to nullable");
    let stale: Vec<String> = registered_runs(&fixture, name)
        .await
        .into_iter()
        .map(|record| record.index_key)
        .collect();

    let reopened = open_configured(
        &fixture,
        Arc::clone(&env),
        name,
        &[&KEY],
        config(),
        IndexPersistence::Enabled,
    )
    .await;
    let verification = reopened
        .verify_lookup_index_against_read_back()
        .await
        .expect("verify");
    println!("after relaxing a key column to nullable: {verification:?}");
    assert!(verification.agrees(), "{verification:?}");
    assert_eq!(
        verification.files, 0,
        "no run built before the column became nullable may cover a file: {verification:?}"
    );
    let kept: Vec<String> = registered_runs(&fixture, name)
        .await
        .into_iter()
        .map(|record| record.index_key)
        .filter(|key| stale.contains(key))
        .collect();
    assert!(
        kept.is_empty(),
        "the runs built before the column became nullable must be deleted: {kept:?}"
    );
    lookup(&reopened, name, 7).await;
    // The index heals: a lookup over the uncovered files rebuilds it, after
    // which every file is covered again and lookups use it.
    let healed = until_covered(&reopened, async || lookup(&reopened, name, 7).await).await;
    println!("after the rebuild: {healed:?}");
    assert!(healed.agrees(), "{healed:?}");
    let before = counters(&reopened);
    lookup(&reopened, name, 7).await;
    assert_eq!(
        counters(&reopened).full - before.full,
        1,
        "a lookup after the rebuild must use the index"
    );
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
    overwrite(&table, vec![rows(0, ROWS)]).await;
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
    let healed = until_covered(&table, async || lookup(&table, name, 7).await).await;
    println!("after the rebuild: {healed:?}");
    assert!(healed.agrees(), "{healed:?}");
    let before = counters(&table);
    lookup(&table, name, 7).await;
    assert_eq!(
        counters(&table).full - before.full,
        1,
        "a lookup after the rebuild must use the index"
    );
}

/// An `indexes` entry may spell a column in another case than the table does;
/// the key resolves to the table's column, and a lookup filtering on it uses
/// the index.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_key_spelled_in_another_case_is_used_by_lookups() {
    const ROWS: usize = 4_000;
    let fixture = common::TestFixture::new(common::BackendType::Sqlite)
        .await
        .expect("fixture");
    let env = Arc::new(RuntimeEnv::default());
    let name = "cased";
    let table = open(
        &fixture,
        Arc::clone(&env),
        name,
        &[&["tenantid", "SERVICEID"]],
    )
    .await;
    overwrite(&table, vec![rows(0, ROWS)]).await;
    let before = counters(&table);
    lookup(&table, name, 7).await;
    let after = counters(&table);
    assert_eq!(
        after.full - before.full,
        1,
        "the lookup did not use the index: {before:?} -> {after:?}"
    );
}

#[derive(Debug)]
struct FaultInjectingIndexStore {
    inner: Arc<dyn ObjectStore>,
    armed: Arc<AtomicBool>,
    entered: Arc<Notify>,
    release: Arc<Notify>,
    fail_delete: Arc<AtomicBool>,
    failed_delete: Arc<std::sync::Mutex<Option<Path>>>,
    delete_attempts: Arc<std::sync::Mutex<HashMap<Path, usize>>>,
}

impl FaultInjectingIndexStore {
    fn new() -> Self {
        Self {
            inner: Arc::new(object_store::local::LocalFileSystem::new()),
            armed: Arc::new(AtomicBool::new(false)),
            entered: Arc::new(Notify::new()),
            release: Arc::new(Notify::new()),
            fail_delete: Arc::new(AtomicBool::new(false)),
            failed_delete: Arc::new(std::sync::Mutex::new(None)),
            delete_attempts: Arc::new(std::sync::Mutex::new(HashMap::new())),
        }
    }

    fn attempts_for(&self, path: &Path) -> usize {
        self.delete_attempts
            .lock()
            .expect("delete attempts lock")
            .get(path)
            .copied()
            .unwrap_or_default()
    }
}

impl fmt::Display for FaultInjectingIndexStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("FaultInjectingIndexStore")
    }
}

#[async_trait]
impl ObjectStore for FaultInjectingIndexStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        if std::path::Path::new(location.as_ref())
            .extension()
            .is_some_and(|ext| ext.eq_ignore_ascii_case("run"))
            && self.armed.swap(false, Ordering::AcqRel)
        {
            self.entered.notify_one();
            self.release.notified().await;
        }
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        let inner = Arc::clone(&self.inner);
        let fail_delete = Arc::clone(&self.fail_delete);
        let failed_delete = Arc::clone(&self.failed_delete);
        let delete_attempts = Arc::clone(&self.delete_attempts);
        locations
            .then(move |location| {
                let inner = Arc::clone(&inner);
                let fail_delete = Arc::clone(&fail_delete);
                let failed_delete = Arc::clone(&failed_delete);
                let delete_attempts = Arc::clone(&delete_attempts);
                async move {
                    let location = location?;
                    if std::path::Path::new(location.as_ref())
                        .extension()
                        .is_some_and(|ext| ext.eq_ignore_ascii_case("run"))
                    {
                        *delete_attempts
                            .lock()
                            .expect("delete attempts lock")
                            .entry(location.clone())
                            .or_default() += 1;
                        if fail_delete.swap(false, Ordering::AcqRel) {
                            *failed_delete.lock().expect("failed delete lock") =
                                Some(location.clone());
                            return Err(object_store::Error::Generic {
                                store: "fault injection",
                                source: std::io::Error::other(
                                    "injected index run deletion failure",
                                )
                                .into(),
                            });
                        }
                    }
                    inner.delete(&location).await?;
                    Ok(location)
                }
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// An old provider's sync must finish before a replacement removes its indexes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reopening_without_indexes_fences_an_old_pending_sync() {
    const NAME: &str = "pending_removed_index";
    let fixture = Arc::new(
        common::TestFixture::new(common::BackendType::Sqlite)
            .await
            .expect("fixture"),
    );
    let env = Arc::new(RuntimeEnv::default());
    let store = Arc::new(FaultInjectingIndexStore::new());
    env.register_object_store(
        &url::Url::parse("file:///").expect("file URL"),
        Arc::clone(&store) as Arc<dyn ObjectStore>,
    );
    let table = open_table(
        &fixture,
        Arc::clone(&env),
        TableSpec::new(NAME, schema(), &[&KEY]).persistence(IndexPersistence::Enabled),
    )
    .await;
    store.armed.store(true, Ordering::Release);
    overwrite(&table, vec![rows(0, 20_000)]).await;
    tokio::time::timeout(Duration::from_secs(10), store.entered.notified())
        .await
        .expect("old sync reached run put");
    drop(table);
    let reopening_fixture = Arc::clone(&fixture);
    let mut reopening = tokio::spawn(async move {
        open_table(
            &reopening_fixture,
            env,
            TableSpec::new(NAME, schema(), &[]).persistence(IndexPersistence::Enabled),
        )
        .await
    });
    let early = tokio::time::timeout(Duration::from_secs(1), &mut reopening).await;
    println!(
        "replacement completed while old sync paused: {}",
        early.is_ok()
    );
    store.release.notify_one();
    let reopened = match early {
        Ok(result) => result.expect("reopen task"),
        Err(_) => reopening.await.expect("reopen after old sync"),
    };
    // The resumed local put and catalog registration must have time to finish.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let registered = registered_runs(&fixture, NAME).await;
    let files = run_file_count(&fixture.data_path);
    println!(
        "after old sync and index removal: registrations={} run_files={files}",
        registered.len()
    );
    assert!(
        registered.is_empty(),
        "an old sync resurrected a removed index registration"
    );
    assert_eq!(files, 0, "an old sync resurrected a removed index file");
    lookup(&reopened, NAME, 7).await;
}

/// Late publications from an old provider cannot change a replacement's runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stale_syncs_do_not_modify_replacement_runs() {
    for (name, indexes, persistence) in [
        ("stale_removed", &[][..], IndexPersistence::Enabled),
        (
            "stale_replaced",
            &[&["AutoId"][..]][..],
            IndexPersistence::Enabled,
        ),
        (
            "stale_disabled",
            &[&KEY[..]][..],
            IndexPersistence::Disabled,
        ),
    ] {
        let fixture = common::TestFixture::new(common::BackendType::Sqlite)
            .await
            .expect("fixture");
        let env = Arc::new(RuntimeEnv::default());
        let old = open_table(
            &fixture,
            Arc::clone(&env),
            TableSpec::new(name, schema(), &[&KEY]).persistence(IndexPersistence::Enabled),
        )
        .await;
        overwrite(&old, vec![rows(0, 20_000)]).await;
        wait_for_persisted_runs(&fixture, name, 1).await;
        let replacement = open_table(
            &fixture,
            env,
            TableSpec::new(name, schema(), indexes).persistence(persistence),
        )
        .await;
        if name == "stale_replaced" {
            until_covered(&replacement, async || {
                let sql = format!("SELECT \"AutoId\" FROM {name} WHERE \"AutoId\" = 7");
                let found = int64_column(&query(&replacement, name, &sql).await);
                assert_eq!(found, vec![7]);
            })
            .await;
            wait_for_persisted_runs(&fixture, name, 1).await;
        }
        let mut before = registered_runs(&fixture, name).await;
        before.sort_by(|left, right| left.run_name.cmp(&right.run_name));
        let mut paths = Vec::new();
        run_files(&fixture.data_path, &mut paths);
        paths.sort();
        let before_files: Vec<_> = paths
            .iter()
            .map(|path| {
                (
                    path.clone(),
                    std::fs::read(path).expect("read existing run"),
                )
            })
            .collect();
        let publications_before = counters(&old).builds_published;
        insert(&old, name, rows(20_000, 20_000)).await;
        assert!(
            counters(&old).builds_published > publications_before,
            "the old provider must publish a run to exercise stale scheduling"
        );
        tokio::time::sleep(Duration::from_secs(2)).await;
        let mut after = registered_runs(&fixture, name).await;
        after.sort_by(|left, right| left.run_name.cmp(&right.run_name));
        println!(
            "late old publication ({name}): registered_before={} registered_after={} run_files={}",
            before.len(),
            after.len(),
            run_file_count(&fixture.data_path)
        );
        assert_eq!(
            after, before,
            "stale persistence changed the replacement registrations"
        );
        let mut remaining = Vec::new();
        run_files(&fixture.data_path, &mut remaining);
        remaining.sort();
        assert_eq!(
            remaining, paths,
            "stale persistence changed the replacement files"
        );
        for (path, bytes) in before_files {
            assert_eq!(std::fs::read(path).expect("retained run"), bytes);
        }
        lookup(&replacement, name, 7).await;
    }
}

/// A failed deletion after unregistration remains eligible for the open's sweep.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_run_file_deletion_does_not_prevent_orphan_cleanup() {
    const NAME: &str = "failed_orphan_delete";
    let fixture = common::TestFixture::new(common::BackendType::Sqlite)
        .await
        .expect("fixture");
    let env = Arc::new(RuntimeEnv::default());
    let store = Arc::new(FaultInjectingIndexStore::new());
    env.register_object_store(
        &url::Url::parse("file:///").expect("file URL"),
        Arc::clone(&store) as Arc<dyn ObjectStore>,
    );
    let table = open_table(
        &fixture,
        Arc::clone(&env),
        TableSpec::new(NAME, schema(), &[&KEY]).persistence(IndexPersistence::Enabled),
    )
    .await;
    overwrite(&table, vec![rows(0, 20_000)]).await;
    wait_for_persisted_runs(&fixture, NAME, 1).await;
    drop(table);
    store.fail_delete.store(true, Ordering::Release);
    let reopened = open_table(
        &fixture,
        env,
        TableSpec::new(NAME, schema(), &[]).persistence(IndexPersistence::Enabled),
    )
    .await;
    let registered = registered_runs(&fixture, NAME).await.len();
    let files = run_file_count(&fixture.data_path);
    let orphan = store
        .failed_delete
        .lock()
        .expect("failed delete lock")
        .clone()
        .expect("failed path");
    let attempts = store.attempts_for(&orphan);
    println!(
        "after index removal and sweep: registered={registered} run_files={files} orphan_attempts={attempts}"
    );
    lookup(&reopened, NAME, 7).await;
    assert_eq!(registered, 0);
    assert_eq!(
        files, 0,
        "the unregistered file remains eligible for the sweep"
    );
    assert_eq!(attempts, 2, "the same open retries the orphan deletion");
}
