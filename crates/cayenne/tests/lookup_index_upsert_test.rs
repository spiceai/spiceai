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

//! The secondary index on a table that is upserted continuously rather than
//! refreshed — the shape `refresh_mode: caching` produces. Its rows live in
//! protected snapshots written by inline checkpoints and merged by compaction,
//! so each of those snapshots must carry its own write-time index, and a
//! lookup must still return exactly the latest version of its key.

#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use std::sync::Arc;

use arrow::array::{Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use cayenne::lookup_index::LookupIndexCounters;
use cayenne::metadata::{CreateTableOptions, DeletionMode, VortexConfig};
use cayenne::provider::CayenneContext;
use cayenne::{CayenneTableProvider, CayenneTableProviderBuilder, MetadataCatalog};

use datafusion::datasource::TableProvider;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::prelude::SessionContext;
use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};

const NAME: &str = "cached";
const ROUND: i64 = 2_000;
const KEY: [&str; 2] = ["request_path", "request_query"];
const NO_AUTOMATIC_COMPACTION: usize = 1_000_000;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("request_path", DataType::Utf8, false),
        Field::new("request_query", DataType::Utf8, false),
        Field::new("content", DataType::Utf8, false),
    ]))
}

fn query_for(id: i64) -> String {
    format!("DeviceEUI=0x{id:016X}")
}

/// Rows for ids `ids`, each carrying `version` in its content.
fn rows(ids: std::ops::Range<i64>, version: u32) -> RecordBatch {
    let ids: Vec<i64> = ids.collect();
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(StringArray::from(vec!["/v1/devices"; ids.len()])),
            Arc::new(StringArray::from(
                ids.iter().map(|id| query_for(*id)).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                ids.iter()
                    .map(|id| format!("v{version}-{id}"))
                    .collect::<Vec<_>>(),
            )),
        ],
    )
    .expect("fixture batch")
}

async fn open(
    fixture: &common::TestFixture,
    protected_trigger: usize,
) -> Arc<CayenneTableProvider> {
    let runtime_env = Arc::new(RuntimeEnv::default());
    // Background compaction is parked, so only the write-driven pass that
    // `protected_trigger` arms can fold protected snapshots.
    let vortex_config = VortexConfig {
        deletion_mode: DeletionMode::Key,
        compaction_trigger_files: NO_AUTOMATIC_COMPACTION,
        compaction_trigger_protected_snapshots: protected_trigger,
        compaction_trigger_snapshot_age_ms: 0,
        compaction_background_interval_ms: 0,
        ..VortexConfig::default()
    };
    let context = CayenneContext::new(&vortex_config, Arc::clone(&runtime_env), NAME);
    let options = CreateTableOptions {
        table_name: NAME.to_string(),
        schema: schema(),
        primary_key: KEY.iter().map(|c| (*c).to_string()).collect(),
        on_conflict: Some(OnConflict::Upsert(ColumnReference::new(
            KEY.iter().map(|c| (*c).to_string()).collect(),
        ))),
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config,
    };
    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    Arc::new(
        CayenneTableProviderBuilder::new(catalog, runtime_env)
            .with_context(context)
            .with_secondary_indexes(vec![KEY.iter().map(|c| (*c).to_string()).collect()])
            .create(options)
            .await
            .expect("create table"),
    )
}

/// Upserts `batch` and checkpoints the inline rows into a protected snapshot,
/// as a caching refresh does once its inline memtable fills.
async fn upsert_and_checkpoint(provider: &Arc<CayenneTableProvider>, batch: RecordBatch) {
    let ctx = SessionContext::new();
    ctx.register_table(NAME, Arc::clone(provider) as Arc<dyn TableProvider>)
        .expect("register target");
    let mem =
        datafusion::datasource::MemTable::try_new(schema(), vec![vec![batch]]).expect("memtable");
    ctx.register_table("src", Arc::new(mem))
        .expect("register src");
    ctx.sql(&format!("INSERT INTO {NAME} SELECT * FROM src"))
        .await
        .expect("upsert plan")
        .collect()
        .await
        .expect("upsert");
    provider
        .checkpoint_inlined_data()
        .await
        .expect("checkpoint inline rows");
}

/// Looks `id` up by the indexed key and returns every `content` it finds.
async fn lookup(provider: &Arc<CayenneTableProvider>, id: i64) -> Vec<String> {
    let ctx = SessionContext::new();
    ctx.register_table(NAME, Arc::clone(provider) as Arc<dyn TableProvider>)
        .expect("register");
    let batches = ctx
        .sql(&lookup_sql(id))
        .await
        .expect("plan lookup")
        .collect()
        .await
        .expect("run lookup");
    let mut found = Vec::new();
    for batch in &batches {
        let column = arrow::compute::cast(batch.column(0), &DataType::Utf8).expect("content");
        let column = column
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("content is a string");
        for row in 0..column.len() {
            found.push(column.value(row).to_string());
        }
    }
    found
}

fn counters(provider: &Arc<CayenneTableProvider>) -> LookupIndexCounters {
    provider
        .lookup_index_counters()
        .expect("the table declares an index")
}

/// `EXPLAIN ANALYZE` of `sql`.
async fn explain(provider: &Arc<CayenneTableProvider>, sql: &str) -> String {
    let ctx = SessionContext::new();
    ctx.register_table(NAME, Arc::clone(provider) as Arc<dyn TableProvider>)
        .expect("register");
    let batches = ctx
        .sql(&format!("EXPLAIN ANALYZE {sql}"))
        .await
        .expect("plan explain")
        .collect()
        .await
        .expect("run explain");
    arrow::util::pretty::pretty_format_batches(&batches)
        .expect("format plan")
        .to_string()
}

/// A scan no index can narrow, so its plan counts every snapshot the table has.
const FULL_SCAN: &str = "SELECT content FROM cached WHERE content LIKE 'v%'";

/// A lookup whose plan shows how many snapshots the index left to read.
fn lookup_sql(id: i64) -> String {
    format!(
        "SELECT content FROM {NAME} WHERE request_path = '/v1/devices' AND request_query = '{}'",
        query_for(id)
    )
}

/// Four rounds of upserts, each checkpointed into its own protected snapshot:
/// three rounds of new keys, then one that upserts every key of the first.
async fn write_four_snapshots(table: &Arc<CayenneTableProvider>) {
    for round in 0..3 {
        upsert_and_checkpoint(table, rows(round * ROUND..(round + 1) * ROUND, 1)).await;
    }
    upsert_and_checkpoint(table, rows(0..ROUND, 2)).await;
}

/// Checks that each probed key returns exactly its latest version, and an
/// unknown key nothing.
async fn check_lookups(table: &Arc<CayenneTableProvider>) {
    for id in [0, ROUND - 1, ROUND, 2 * ROUND + 7, 3 * ROUND - 1] {
        let version = if id < ROUND { 2 } else { 1 };
        assert_eq!(
            lookup(table, id).await,
            [format!("v{version}-{id}")],
            "lookup of {id} returned the wrong rows"
        );
    }
    assert!(lookup(table, 10 * ROUND).await.is_empty());
}

/// The current snapshot's own index is built in the background by the first
/// lookup, as before this change; waits for it so later counters describe the
/// write-time indexes alone.
async fn await_current_index(table: &Arc<CayenneTableProvider>) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        lookup(table, 0).await;
        if counters(table).index_bytes > 0 {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the current snapshot's index was never published: {:?}",
            counters(table)
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

/// Every protected snapshot a checkpoint writes is indexed as it is written, so
/// lookups over them are answered from the index — never `unbuilt` — and a key
/// upserted across snapshots returns only its latest version.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn checkpointed_protected_snapshots_are_indexed_as_they_are_written() {
    let fixture = common::TestFixture::new(common::BackendType::Sqlite)
        .await
        .expect("fixture");
    let table = open(&fixture, NO_AUTOMATIC_COMPACTION).await;
    write_four_snapshots(&table).await;

    let plan = explain(&table, FULL_SCAN).await;
    assert!(
        plan.contains("snapshots_scanned=4"),
        "expected four protected snapshots (the current one holds no file):\n{plan}"
    );
    // Only the snapshot that holds the key is read.
    let plan = explain(&table, &lookup_sql(ROUND + 1)).await;
    assert!(
        plan.contains("snapshots_scanned=1, files_scanned=1"),
        "the lookup read snapshots that cannot hold its key:\n{plan}"
    );

    await_current_index(&table).await;
    let before = counters(&table);
    check_lookups(&table).await;
    let after = counters(&table);
    // `check_lookups` runs 6 lookups, each reading the current snapshot and four
    // protected ones, and each is counted as ONE probe outcome.
    let outcomes = |c: &LookupIndexCounters| c.selected + c.empty + c.unbuilt + c.snapshot_mismatch;
    assert_eq!(
        outcomes(&after) - outcomes(&before),
        6,
        "expected one probe outcome per lookup: {before:?} -> {after:?}"
    );
    assert_eq!(
        after.unbuilt, before.unbuilt,
        "a protected snapshot was read without an index: {after:?}"
    );
    assert_eq!(
        after.snapshot_mismatch, before.snapshot_mismatch,
        "an index was refused for its own snapshot: {after:?}"
    );
    assert!(
        after.selected > before.selected
            && after.access_plans_attached > before.access_plans_attached,
        "no lookup was served by row selection: {after:?}"
    );
}

/// Once compaction folds the protected snapshots — into a merged protected
/// snapshot or a rewritten current one — the snapshot it writes is indexed in
/// the same way, so lookups keep their index across the fold.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compaction_output_is_indexed_as_it_is_written() {
    let fixture = common::TestFixture::new(common::BackendType::Sqlite)
        .await
        .expect("fixture");
    let table = open(&fixture, 4).await;
    write_four_snapshots(&table).await;
    table
        .drain_in_flight_maintenance()
        .await
        .expect("drain maintenance");

    let plan = explain(&table, FULL_SCAN).await;
    assert!(
        plan.contains("snapshots_scanned=1"),
        "compaction did not fold the four protected snapshots:\n{plan}"
    );

    await_current_index(&table).await;
    let before = counters(&table);
    check_lookups(&table).await;
    let after = counters(&table);
    assert_eq!(
        after.unbuilt, before.unbuilt,
        "a compacted snapshot was read without an index: {after:?}\n{plan}"
    );
    assert_eq!(
        after.snapshot_mismatch, before.snapshot_mismatch,
        "an index was refused for its own snapshot: {after:?}\n{plan}"
    );
    assert!(
        after.selected > before.selected,
        "no lookup was served by row selection: {after:?}\n{plan}"
    );
}
