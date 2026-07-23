/*
Copyright 2026 The Spice.ai OSS Authors

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

//! Path-asserted subset vs full current-snapshot small-file compaction (P1-1).
//!
//! - **Subset**: PK + `DeletionMode::Key` + `max_files_per_pick` proper subset.
//! - **Full**: append-only (position strategy) always rewrites the whole snapshot.

#![allow(clippy::expect_used)]
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use cayenne::metadata::{CreateTableOptions, DeletionMode, VortexConfig};
use cayenne::{CayenneCatalog, CayenneTableProvider, LastSmallFileCompactPath, MetadataCatalog};
use datafusion::datasource::TableProvider;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::prelude::SessionContext;
use datafusion_expr::dml::InsertOp;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Utf8, false),
    ]))
}

fn batch(start: i64, n: i64) -> RecordBatch {
    let ids: Vec<i64> = (start..start + n).collect();
    let values: Vec<String> = ids
        .iter()
        .map(|i| {
            let u = (*i).cast_unsigned();
            format!(
                "v_{i:020}_{:016x}_{:016x}_{:016x}",
                u.wrapping_mul(0x9E37_79B9_7F4A_7C15),
                u.wrapping_mul(0xC2B2_AE3D_27D4_EB4F),
                u.wrapping_mul(0x1656_67B1_9E37_79F9),
            )
        })
        .collect();
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(values)),
        ],
    )
    .expect("batch")
}

async fn insert(table: &Arc<CayenneTableProvider>, b: RecordBatch) {
    let ctx = SessionContext::new();
    let s = Arc::clone(b.schema_ref());
    let input = MemorySourceConfig::try_new_exec(&[vec![b]], s, None).expect("mem");
    let plan = table
        .insert_into(&ctx.state(), input, InsertOp::Append)
        .await
        .expect("plan");
    datafusion_physical_plan::collect(plan, ctx.task_ctx())
        .await
        .expect("collect");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn probe_subset_path() {
    let temp = tempfile::tempdir().expect("tmp");
    let data_path = temp.path().join("data");
    tokio::fs::create_dir_all(&data_path).await.expect("dir");
    let catalog = Arc::new(
        CayenneCatalog::new(format!(
            "sqlite://{}",
            temp.path().join("c.db").to_string_lossy()
        ))
        .expect("cat"),
    );
    catalog.init().await.expect("init");
    let ctx = SessionContext::new();
    let table = Arc::new(
        CayenneTableProvider::create_table(
            Arc::clone(&catalog) as Arc<dyn MetadataCatalog>,
            CreateTableOptions {
                table_name: "probe".into(),
                schema: schema(),
                primary_key: vec!["id".into()],
                on_conflict: None,
                base_path: data_path.to_string_lossy().into(),
                partition_column: None,
                vortex_config: VortexConfig {
                    target_vortex_file_size_mb: 1,
                    compaction_trigger_files: 4,
                    compaction_max_levels: 3,
                    compaction_max_files_per_pick: 4,
                    compaction_background_interval_ms: 0,
                    inline_max_rows: 0,
                    deletion_mode: DeletionMode::Key,
                    ..VortexConfig::default()
                },
            },
            ctx.runtime_env(),
        )
        .await
        .expect("create"),
    );

    let t0 = Instant::now();
    for i in 0..12 {
        insert(&table, batch(i * 1500, 1500)).await;
    }
    // Drain post-write compaction, then force an explicit pass if needed.
    for _ in 0..50 {
        if table.last_small_file_compact_path() == LastSmallFileCompactPath::Subset {
            break;
        }
        let _ = table.maybe_compact_small_files().await.expect("compact");
        tokio::task::yield_now().await;
    }
    let path = table.last_small_file_compact_path();
    eprintln!("done path={path:?} elapsed={:?}", t0.elapsed());
    assert_eq!(path, LastSmallFileCompactPath::Subset);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn probe_full_path_append_only() {
    let temp = tempfile::tempdir().expect("tmp");
    let data_path = temp.path().join("data");
    tokio::fs::create_dir_all(&data_path).await.expect("dir");
    let catalog = Arc::new(
        CayenneCatalog::new(format!(
            "sqlite://{}",
            temp.path().join("c.db").to_string_lossy()
        ))
        .expect("cat"),
    );
    catalog.init().await.expect("init");
    let ctx = SessionContext::new();
    let table = Arc::new(
        CayenneTableProvider::create_table(
            Arc::clone(&catalog) as Arc<dyn MetadataCatalog>,
            CreateTableOptions {
                table_name: "full".into(),
                schema: schema(),
                primary_key: vec![],
                on_conflict: None,
                base_path: data_path.to_string_lossy().into(),
                partition_column: None,
                vortex_config: VortexConfig {
                    target_vortex_file_size_mb: 1,
                    compaction_trigger_files: 4,
                    compaction_max_files_per_pick: 4,
                    compaction_background_interval_ms: 0,
                    inline_max_rows: 0,
                    ..VortexConfig::default()
                },
            },
            ctx.runtime_env(),
        )
        .await
        .expect("create"),
    );
    for i in 0..12 {
        insert(&table, batch(i * 1500, 1500)).await;
    }
    for _ in 0..50 {
        if table.last_small_file_compact_path() == LastSmallFileCompactPath::Full {
            break;
        }
        let _ = table.maybe_compact_small_files().await.expect("compact");
        tokio::task::yield_now().await;
    }
    assert_eq!(
        table.last_small_file_compact_path(),
        LastSmallFileCompactPath::Full
    );
}
