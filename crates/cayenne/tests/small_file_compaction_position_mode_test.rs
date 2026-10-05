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

//! Regression test for #14420: the background compactor's current-snapshot
//! small-file pass over a primary-key table in `position` deletion mode takes
//! the write lock, falls through to the full rewrite, and must not wait for
//! that same lock again — it is a non-reentrant `tokio::sync::Mutex`, so the
//! pass would park forever and every later write to the table would hang
//! behind it.

#![expect(clippy::expect_used, reason = "test code")]

mod common;

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use cayenne::metadata::{CreateTableOptions, DeletionMode, VortexConfig};
use cayenne::provider::CayenneContext;
use cayenne::{
    CayenneTableProvider, CayenneTableProviderBuilder, LastSmallFileCompactPath, MetadataCatalog,
};
use datafusion::datasource::TableProvider;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::prelude::SessionContext;

const APPENDS: i64 = 64;
const ROWS_PER_APPEND: i64 = 5_000;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]))
}

fn rows(start: i64, count: i64) -> RecordBatch {
    let ids: Vec<i64> = (start..start + count).collect();
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(ids.clone())),
            Arc::new(StringArray::from(
                ids.iter()
                    .map(|id| format!("payload-{id:016x}"))
                    .collect::<Vec<_>>(),
            )),
        ],
    )
    .expect("batch")
}

async fn append(table: &Arc<CayenneTableProvider>, batch: RecordBatch) {
    common::insert_batch(table, batch).await.expect("insert");
}

async fn count_rows(table: &Arc<CayenneTableProvider>) -> i64 {
    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::clone(table) as Arc<dyn TableProvider>)
        .expect("register");
    let batches = ctx
        .sql("SELECT COUNT(*) AS n, COUNT(DISTINCT id) AS d FROM t")
        .await
        .expect("sql")
        .collect()
        .await
        .expect("collect");
    let n = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count column")
        .value(0);
    let distinct = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("distinct column")
        .value(0);
    assert_eq!(n, distinct, "a primary key must not be returned twice");
    n
}

/// Appends small files with pauses between them, as a serving workload does
/// (the position-mode pass takes the write lock only when no append holds
/// it), and asserts every append finishes, a small-file compaction committed,
/// and no row was lost or duplicated.
async fn run_small_file_workload(name: &str, deletion_mode: DeletionMode) {
    let fixture = common::TestFixture::new(common::BackendType::Sqlite)
        .await
        .expect("fixture");
    let env = Arc::new(RuntimeEnv::default());
    // A 1 MiB target file size makes every append a small file. The background
    // compactor is what runs the current-snapshot small-file pass in
    // production (`CompactionRunner::run_compaction_trigger`); a short interval
    // lets it fire between the appends below.
    let config = VortexConfig {
        target_vortex_file_size_mb: 1,
        compaction_background_interval_ms: 100,
        deletion_mode,
        ..VortexConfig::default()
    };
    let context = CayenneContext::new(&config, Arc::clone(&env), name);
    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let table = Arc::new(
        CayenneTableProviderBuilder::new(catalog, env)
            .with_context(context)
            .create(CreateTableOptions {
                table_name: name.to_string(),
                schema: schema(),
                primary_key: vec!["id".to_string()],
                on_conflict: None,
                base_path: fixture.data_path.to_string_lossy().to_string(),
                partition_column: None,
                vortex_config: config,
            })
            .await
            .expect("create table"),
    );
    assert!(
        table.spawn_background_compaction(Arc::new(tokio::sync::Semaphore::new(1))),
        "the background compactor must be running for this test to mean anything"
    );

    for i in 0..APPENDS {
        let appended = tokio::time::timeout(
            Duration::from_secs(30),
            append(&table, rows(i * ROWS_PER_APPEND, ROWS_PER_APPEND)),
        )
        .await;
        assert!(
            appended.is_ok(),
            "append {i} did not finish within 30 s: the small-file compaction is holding the write lock"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // Let the compactor catch up on what the last appends left behind.
    tokio::time::timeout(Duration::from_mins(1), async {
        while table.last_small_file_compact_path() == LastSmallFileCompactPath::None {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("no small-file compaction committed within 1 min");
    tokio::time::timeout(Duration::from_mins(1), table.flush_pending_maintenance())
        .await
        .expect("post-write maintenance did not finish within 1 min")
        .expect("flush pending maintenance");

    assert_eq!(
        count_rows(&table).await,
        APPENDS * ROWS_PER_APPEND,
        "every appended row must be visible exactly once after compaction"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn small_file_compaction_of_a_default_mode_primary_key_table_finishes() {
    // `auto` resolves to `position` for a primary-key table that is not
    // CDC-fed: this is the configuration every `full`/`append` PK dataset gets.
    run_small_file_workload("pk_small_files_auto", DeletionMode::Auto).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn small_file_compaction_of_a_position_mode_primary_key_table_finishes() {
    run_small_file_workload("pk_small_files_position", DeletionMode::Position).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn small_file_compaction_of_a_key_mode_primary_key_table_finishes() {
    run_small_file_workload("pk_small_files_key", DeletionMode::Key).await;
}
