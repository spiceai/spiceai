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

//! Measurement, not an assertion: how a point lookup's planning time grows with
//! the number of protected snapshots on an indexed table. Run with
//! `cargo test -p cayenne --test lookup_index_planning_probe -- --ignored --nocapture`.

#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use std::sync::Arc;
use std::time::Instant;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use cayenne::metadata::{CreateTableOptions, DeletionMode, VortexConfig};
use cayenne::provider::CayenneContext;
use cayenne::{CayenneTableProvider, CayenneTableProviderBuilder, MetadataCatalog};

use datafusion::datasource::TableProvider;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::prelude::SessionContext;
use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

async fn open(fixture: &common::TestFixture, name: &str) -> Arc<CayenneTableProvider> {
    let runtime_env = Arc::new(RuntimeEnv::default());
    let vortex_config = VortexConfig {
        deletion_mode: DeletionMode::Key,
        inline_max_rows: 0,
        compaction_trigger_files: 1_000_000,
        compaction_trigger_protected_snapshots: 1_000_000,
        compaction_trigger_snapshot_age_ms: 0,
        compaction_background_interval_ms: 0,
        ..VortexConfig::default()
    };
    let context = CayenneContext::new(&vortex_config, Arc::clone(&runtime_env), name);
    let options = CreateTableOptions {
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
    Arc::new(
        CayenneTableProviderBuilder::new(catalog, runtime_env)
            .with_context(context)
            .with_secondary_indexes(vec![vec!["id".to_string()]])
            .create(options)
            .await
            .expect("create table"),
    )
}

async fn upsert(ctx: &SessionContext, name: &str, ids: std::ops::Range<i64>) {
    let ids: Vec<i64> = ids.collect();
    let values = ids.clone();
    let batch = RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(values)),
        ],
    )
    .expect("batch");
    let mem = datafusion::datasource::MemTable::try_new(schema(), vec![vec![batch]]).expect("mem");
    let _ = ctx.deregister_table("src");
    ctx.register_table("src", Arc::new(mem)).expect("src");
    ctx.sql(&format!("INSERT INTO {name} SELECT * FROM src"))
        .await
        .expect("plan")
        .collect()
        .await
        .expect("upsert");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "measurement; run explicitly"]
async fn planning_time_by_protected_snapshot_count() {
    const ROWS_PER_SNAPSHOT: i64 = 20;
    const LOOKUPS: i64 = 200;
    for snapshots in [10_i64, 50, 150] {
        let fixture = common::TestFixture::new(common::BackendType::Sqlite)
            .await
            .expect("fixture");
        let name = format!("probe_{snapshots}");
        let table = open(&fixture, &name).await;
        let ctx = SessionContext::new();
        ctx.register_table(&name, Arc::clone(&table) as Arc<dyn TableProvider>)
            .expect("register");
        for s in 0..snapshots {
            upsert(
                &ctx,
                &name,
                s * ROWS_PER_SNAPSHOT..(s + 1) * ROWS_PER_SNAPSHOT,
            )
            .await;
        }
        // Keys that no snapshot holds: every protected branch is an index `empty`.
        let mut planning = Vec::new();
        for i in 0..LOOKUPS {
            let key = 1_000_000 + i;
            let started = Instant::now();
            ctx.sql(&format!(
                "EXPLAIN SELECT value FROM {name} WHERE id = {key}"
            ))
            .await
            .expect("plan")
            .collect()
            .await
            .expect("explain");
            planning.push(started.elapsed().as_secs_f64() * 1000.0);
        }
        planning.sort_by(f64::total_cmp);
        // Nearest-rank percentile over the sorted samples, in integer arithmetic.
        let q = |percent: usize| planning[(planning.len() - 1) * percent / 100];
        let counters = table.lookup_index_counters().expect("indexed");
        println!(
            "protected_snapshots={snapshots} lookups={LOOKUPS} planning_ms p50={:.2} p99={:.2} max={:.2} | empty={} unbuilt={}",
            q(50),
            q(99),
            q(100),
            counters.empty,
            counters.unbuilt
        );
    }
}
