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

//! A merge of protected snapshots — here the seq-prefix bake, which a
//! key-deletion caching table runs as its deletion index grows — publishes the
//! merged snapshot's index and releases the indexes of the snapshots it folded.
//! A lookup planned after the merge must read the merged snapshot through its
//! index, not a scan view cached before the merge, whose folded snapshots no
//! longer have an index and are read in full.

#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use std::sync::Arc;

use arrow::array::{Array, Int64Array};
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

const NAME: &str = "merged";
const ROWS_PER_SNAPSHOT: i64 = 500;
/// The bake keeps the newest three protected snapshots and folds the rest.
const SNAPSHOTS: i64 = 6;
const BAKE_KEEPS: usize = 3;
/// Parks the automatic compaction triggers, so only the test's own bake folds
/// the protected snapshots.
const NO_TRIGGER: usize = 1_000_000;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

async fn open(fixture: &common::TestFixture) -> Arc<CayenneTableProvider> {
    let runtime_env = Arc::new(RuntimeEnv::default());
    let vortex_config = VortexConfig {
        // The bake folds key-deletion tables only.
        deletion_mode: DeletionMode::Key,
        // Every upsert goes straight to its own protected snapshot.
        inline_max_rows: 0,
        compaction_trigger_files: NO_TRIGGER,
        compaction_trigger_protected_snapshots: NO_TRIGGER,
        bake_deletion_index_trigger: NO_TRIGGER,
        compaction_trigger_snapshot_age_ms: 0,
        compaction_background_interval_ms: 0,
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

fn context(table: &Arc<CayenneTableProvider>) -> SessionContext {
    let ctx = SessionContext::new();
    ctx.register_table(NAME, Arc::clone(table) as Arc<dyn TableProvider>)
        .expect("register");
    ctx
}

/// Upserts `id` in `ids` with `value = id * 10`, as one protected snapshot.
async fn upsert(table: &Arc<CayenneTableProvider>, ids: std::ops::Range<i64>) {
    let ids: Vec<i64> = ids.collect();
    let values: Vec<i64> = ids.iter().map(|id| id * 10).collect();
    let batch = RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(values)),
        ],
    )
    .expect("batch");
    let ctx = context(table);
    let mem = datafusion::datasource::MemTable::try_new(schema(), vec![vec![batch]]).expect("mem");
    ctx.register_table("src", Arc::new(mem)).expect("src");
    ctx.sql(&format!("INSERT INTO {NAME} SELECT * FROM src"))
        .await
        .expect("upsert plan")
        .collect()
        .await
        .expect("upsert");
}

async fn lookup(table: &Arc<CayenneTableProvider>, id: i64) -> Vec<i64> {
    let batches = context(table)
        .sql(&format!("SELECT value FROM {NAME} WHERE id = {id}"))
        .await
        .expect("plan")
        .collect()
        .await
        .expect("lookup");
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lookups_after_a_bake_use_the_merged_index() {
    let fixture = common::TestFixture::new(common::BackendType::Sqlite)
        .await
        .expect("fixture");
    let table = open(&fixture).await;
    for s in 0..SNAPSHOTS {
        upsert(&table, s * ROWS_PER_SNAPSHOT..(s + 1) * ROWS_PER_SNAPSHOT).await;
    }
    table
        .drain_in_flight_maintenance()
        .await
        .expect("drain maintenance");
    assert_eq!(
        table
            .lookup_index_snapshot_footprint()
            .map(|(count, _)| count),
        usize::try_from(SNAPSHOTS).ok(),
        "the writes should leave one indexed protected snapshot each"
    );
    // A key in a folded snapshot, one in a kept snapshot, and the newest.
    let keys = [0, ROWS_PER_SNAPSHOT + 1, SNAPSHOTS * ROWS_PER_SNAPSHOT - 1];

    // Lookups before the merge cache the scan view. The first also starts the
    // background build of the (empty) current snapshot's index, so it is counted
    // `unbuilt`; drain it so every later lookup has an index for every snapshot.
    assert_eq!(
        lookup(&table, keys[0]).await,
        [keys[0] * 10],
        "first lookup"
    );
    table
        .drain_in_flight_maintenance()
        .await
        .expect("drain maintenance");
    let warmed = counters(&table);
    for id in keys {
        assert_eq!(lookup(&table, id).await, [id * 10], "before merge");
    }
    let before = counters(&table);
    assert_eq!(
        before.unbuilt, warmed.unbuilt,
        "a lookup before the merge read a snapshot without its index: {before:?}"
    );

    assert!(
        table
            .bake_seq_prefix_protected_snapshots()
            .await
            .expect("bake"),
        "the oldest protected snapshots were not baked"
    );
    let (snapshot_indexes, _) = table
        .lookup_index_snapshot_footprint()
        .expect("the table declares an index");
    assert_eq!(
        snapshot_indexes,
        BAKE_KEEPS + 1,
        "the kept snapshots' and the merged snapshot's indexes should remain"
    );

    for id in keys {
        assert_eq!(lookup(&table, id).await, [id * 10], "after merge");
    }
    assert!(
        lookup(&table, SNAPSHOTS * ROWS_PER_SNAPSHOT * 10)
            .await
            .is_empty(),
        "unknown key"
    );
    let after = counters(&table);
    assert_eq!(
        after.unbuilt, before.unbuilt,
        "a lookup after the merge read a folded snapshot without its index: {after:?}"
    );
    assert!(
        after.selected > before.selected,
        "no lookup after the merge was served by the merged index: {after:?}"
    );
}
