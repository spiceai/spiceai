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

//! A rewrite of the current snapshot — the full-rewrite compaction that folds
//! the protected snapshots, and the sort rewrite — builds the new snapshot's
//! secondary index as it writes and promotes it in the same flip that makes the
//! snapshot current. So after the rewrite, lookups use the index straight away,
//! without the background read-back build a stale index would need, and the
//! folded protected snapshots' indexes are released.

#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use std::sync::Arc;

use arrow::array::{Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use cayenne::lookup_index::LookupIndexCounters;
use cayenne::metadata::{CreateTableOptions, DeletionMode, VortexConfig};
use cayenne::provider::CayenneContext;
use cayenne::{
    CayenneTableProvider, CayenneTableProviderBuilder, LastSmallFileCompactPath, MetadataCatalog,
};

use datafusion::datasource::TableProvider;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::prelude::SessionContext;
use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};

const NAME: &str = "rewritten";
const KEYS: i64 = 2_000;
/// Protected snapshots written before the rewrite.
const SNAPSHOTS: i64 = 3;
/// Parks the count-driven protected-snapshot merges, so the snapshots stay
/// protected until the test's own rewrite folds them.
const NO_COUNT_TRIGGER: usize = 1_000_000;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

/// A primary-key upsert table indexed on `id`, whose every write is its own
/// protected snapshot, in deletion `mode`.
async fn open(
    fixture: &common::TestFixture,
    mode: DeletionMode,
    sort_columns: Vec<String>,
) -> Arc<CayenneTableProvider> {
    let runtime_env = Arc::new(RuntimeEnv::default());
    let vortex_config = VortexConfig {
        deletion_mode: mode,
        // Every upsert goes straight to its own protected snapshot.
        inline_max_rows: 0,
        compaction_trigger_protected_snapshots: NO_COUNT_TRIGGER,
        // Any protected snapshot older than this makes the next compaction pass
        // a full rewrite that folds them all into the current snapshot.
        compaction_trigger_snapshot_age_ms: 1,
        compaction_background_interval_ms: 0,
        sort_columns,
        ..VortexConfig::default()
    };
    let context = CayenneContext::new(&vortex_config, Arc::clone(&runtime_env), NAME);
    let options = CreateTableOptions {
        table_name: NAME.to_string(),
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
    Arc::new(
        CayenneTableProviderBuilder::new(catalog, runtime_env)
            .with_context(context)
            .with_secondary_indexes(vec![vec!["id".to_string()]])
            .create(options)
            .await
            .expect("create table"),
    )
}

async fn sql(table: &Arc<CayenneTableProvider>, query: &str) -> Vec<RecordBatch> {
    let ctx = SessionContext::new();
    ctx.register_table(NAME, Arc::clone(table) as Arc<dyn TableProvider>)
        .expect("register");
    ctx.sql(query)
        .await
        .expect("plan")
        .collect()
        .await
        .expect("run")
}

/// Upserts every key at `version`: `value = id * 10 + version`.
async fn upsert_version(table: &Arc<CayenneTableProvider>, version: i64) {
    let ids: Vec<i64> = (0..KEYS).collect();
    let values: Vec<i64> = ids.iter().map(|id| id * 10 + version).collect();
    let batch = RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(values)),
        ],
    )
    .expect("batch");
    let ctx = SessionContext::new();
    ctx.register_table(NAME, Arc::clone(table) as Arc<dyn TableProvider>)
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
}

async fn lookup(table: &Arc<CayenneTableProvider>, id: i64) -> Vec<i64> {
    let batches = sql(table, &format!("SELECT value FROM {NAME} WHERE id = {id}")).await;
    let mut values = Vec::new();
    for batch in &batches {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("value is i64");
        for row in 0..column.len() {
            values.push(column.value(row));
        }
    }
    values
}

fn counters(table: &Arc<CayenneTableProvider>) -> LookupIndexCounters {
    table
        .lookup_index_counters()
        .expect("the table declares an index")
}

/// Writes `SNAPSHOTS` versions of every key, each its own indexed protected
/// snapshot.
async fn write_versions(table: &Arc<CayenneTableProvider>) {
    for version in 1..=SNAPSHOTS {
        upsert_version(table, version).await;
    }
    table
        .drain_in_flight_maintenance()
        .await
        .expect("drain maintenance");
}

/// After a rewrite made one snapshot current: lookups return each key's latest
/// version from the promoted index, with no background build and no snapshot
/// read without its index, and no protected snapshot's index is left.
async fn assert_served_by_promoted_index(table: &Arc<CayenneTableProvider>, what: &str) {
    assert_eq!(
        table.lookup_index_snapshot_footprint(),
        Some((0, 0)),
        "{what}: the folded snapshots' indexes should be released"
    );
    let before = counters(table);
    assert!(
        before.index_bytes > 0,
        "{what}: the current snapshot should hold the promoted index: {before:?}"
    );
    for id in [0, 1, KEYS / 2, KEYS - 1] {
        assert_eq!(
            lookup(table, id).await,
            [id * 10 + SNAPSHOTS],
            "{what}: lookup of {id} returned the wrong rows"
        );
    }
    assert!(
        lookup(table, KEYS * 10).await.is_empty(),
        "{what}: unknown key"
    );
    let after = counters(table);
    assert_eq!(
        after.builds_started, before.builds_started,
        "{what}: the promoted index needed no background build: {after:?}"
    );
    assert_eq!(
        (after.unbuilt, after.snapshot_mismatch),
        (before.unbuilt, before.snapshot_mismatch),
        "{what}: a lookup read the rewritten snapshot without its index: {after:?}"
    );
    assert!(
        after.selected > before.selected,
        "{what}: no lookup was served by the promoted index: {after:?}"
    );
}

async fn full_rewrite_promotes_its_index(mode: DeletionMode) {
    let fixture = common::TestFixture::new(common::BackendType::Sqlite)
        .await
        .expect("fixture");
    let table = open(&fixture, mode, Vec::new()).await;
    write_versions(&table).await;
    // The snapshot-age trigger makes this pass a full rewrite that folds the
    // protected snapshots into the current snapshot.
    table
        .maybe_compact_small_files()
        .await
        .expect("compaction pass");
    table
        .drain_in_flight_maintenance()
        .await
        .expect("drain maintenance");
    assert_eq!(
        table.last_small_file_compact_path(),
        LastSmallFileCompactPath::Full,
        "{mode:?}: the protected snapshots were not folded by a full rewrite"
    );
    assert_served_by_promoted_index(&table, &format!("full rewrite ({mode:?})")).await;
}

async fn sort_rewrite_promotes_its_index(mode: DeletionMode) {
    let fixture = common::TestFixture::new(common::BackendType::Sqlite)
        .await
        .expect("fixture");
    let table = open(&fixture, mode, vec!["id".to_string()]).await;
    write_versions(&table).await;
    table
        .sort_and_rewrite_data(1024 * 1024)
        .await
        .expect("sort rewrite");
    assert_served_by_promoted_index(&table, &format!("sort rewrite ({mode:?})")).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn full_rewrite_promotes_its_index_key_mode() {
    full_rewrite_promotes_its_index(DeletionMode::Key).await;
}

/// `Position` is the deletion mode a primary-key table resolves to by default.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn full_rewrite_promotes_its_index_position_mode() {
    full_rewrite_promotes_its_index(DeletionMode::Position).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sort_rewrite_promotes_its_index_key_mode() {
    sort_rewrite_promotes_its_index(DeletionMode::Key).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sort_rewrite_promotes_its_index_position_mode() {
    sort_rewrite_promotes_its_index(DeletionMode::Position).await;
}
