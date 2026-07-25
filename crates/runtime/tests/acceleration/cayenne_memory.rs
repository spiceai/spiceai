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

//! Integration tests for the Cayenne accelerator in `mode: memory` (fully in-RAM).
//!
//! These exercise the full runtime path — accelerator wiring + refresh chain — not
//! just the provider: register a `mode: memory` Cayenne dataset from a local file
//! source, load it, query it, confirm a full refresh ATOMICALLY REPLACES the in-RAM
//! tier, and confirm no data files are written to disk. Uses a `file://` source so
//! it needs no Docker/credentials and runs unconditionally in CI on Linux.

use std::sync::Arc;

use app::AppBuilder;
use arrow::array::RecordBatch;
use datafusion::{assert_batches_eq, sql::TableReference};
use futures::TryStreamExt;
use runtime::Runtime;
use spicepod::{
    acceleration::{Acceleration, Mode, RefreshMode},
    component::dataset::Dataset,
};

use crate::utils::{runtime_ready_check, test_request_context};

async fn execute_sql(rt: &Arc<Runtime>, sql: &str) -> Result<Vec<RecordBatch>, anyhow::Error> {
    rt.datafusion()
        .query_builder(sql)
        .build()
        .run()
        .await
        .map_err(|e| anyhow::anyhow!("Query failed: {e}"))?
        .data
        .try_collect()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to collect results: {e}"))
}

async fn refresh(rt: &Arc<Runtime>, table: &str) -> Result<(), anyhow::Error> {
    let notifier = rt
        .datafusion()
        .refresh_table(&TableReference::from(table), None)
        .await
        .map_err(|e| anyhow::anyhow!("refresh_table failed: {e}"))?;
    notifier
        .ok_or_else(|| anyhow::anyhow!("no refresh notifier for {table}"))?
        .notified()
        .await;
    Ok(())
}

/// Full-runtime memory-mode test: register a `mode: memory` Cayenne dataset from a
/// local CSV, load + query it, then full-refresh with a disjoint source set (which
/// must ATOMICALLY REPLACE the in-RAM tier), and confirm no data files touch disk.
/// No primary key — the common Arrow (full-refresh) case.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[cfg(not(target_os = "windows"))]
async fn test_cayenne_memory_mode_full_refresh_and_query() -> Result<(), anyhow::Error> {
    let _tracing = crate::init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let temp_dir = tempfile::tempdir()?;
            let csv = temp_dir.path().join("rows.csv");
            std::fs::write(&csv, "id,name\n1,alpha\n2,beta\n3,gamma\n")?;

            crate::configure_test_datafusion();

            // Memory mode must never touch disk. Compute its (derived, never-created)
            // data directory up front and clear any stale directory a PRIOR local run
            // may have left, so the end-of-test "does not exist" assertion reflects
            // only what THIS run wrote rather than tripping over leftover state.
            let data_path =
                std::path::PathBuf::from(runtime::spice_data_base_path()).join("cayenne_mem_it");
            let _ = std::fs::remove_dir_all(&data_path);

            // #11922: memory mode must also not leave a stray, empty `file:`
            // directory in the process working directory. `CayenneCatalog::init()`
            // took `Path::parent()` of the in-RAM metastore path
            // (`file:/cayenne-mem-N?vfs=memdb`), which is the bare `file:` scheme
            // component, and `create_dir_all`'d it. Compute and clear it up front
            // (like `data_path`) so the end-of-test assertion reflects only what
            // THIS run wrote.
            let stray_file_dir = std::env::current_dir()?.join("file:");
            let _ = std::fs::remove_dir_all(&stray_file_dir);

            let mut dataset = Dataset::new(format!("file://{}", csv.display()), "cayenne_mem_it");
            dataset.acceleration = Some(Acceleration {
                enabled: true,
                engine: Some("cayenne".to_string()),
                mode: Mode::Memory,
                refresh_mode: Some(RefreshMode::Full),
                ..Acceleration::default()
            });

            let app = AppBuilder::new("test_cayenne_memory")
                .with_dataset(dataset)
                .build();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
                    return Err(anyhow::Error::msg("Timeout waiting for components to load"));
                }
                () = Arc::clone(&rt).load_components() => {}
            }
            runtime_ready_check(&rt).await;

            // Initial full refresh loaded all three rows into the in-RAM accelerator.
            let result = execute_sql(&rt, "SELECT COUNT(*) AS cnt FROM cayenne_mem_it").await?;
            let expected = ["+-----+", "| cnt |", "+-----+", "| 3   |", "+-----+"];
            assert_batches_eq!(expected, &result);

            // A point query returns the right row from RAM.
            let result = execute_sql(&rt, "SELECT name FROM cayenne_mem_it WHERE id = 2").await?;
            let expected = ["+------+", "| name |", "+------+", "| beta |", "+------+"];
            assert_batches_eq!(expected, &result);

            // Rewrite the source with a smaller, DISJOINT set and full-refresh: the
            // in-RAM tier must be ATOMICALLY REPLACED (overwrite, not append).
            std::fs::write(&csv, "id,name\n10,ten\n20,twenty\n")?;
            refresh(&rt, "cayenne_mem_it").await?;

            // New count is 2 (replaced, not 5 appended).
            let result = execute_sql(&rt, "SELECT COUNT(*) AS cnt FROM cayenne_mem_it").await?;
            let expected = ["+-----+", "| cnt |", "+-----+", "| 2   |", "+-----+"];
            assert_batches_eq!(expected, &result);
            // Old rows are gone...
            let result = execute_sql(
                &rt,
                "SELECT COUNT(*) AS cnt FROM cayenne_mem_it WHERE id = 1",
            )
            .await?;
            let expected = ["+-----+", "| cnt |", "+-----+", "| 0   |", "+-----+"];
            assert_batches_eq!(expected, &result);
            // ...and the new rows are present.
            let result = execute_sql(
                &rt,
                "SELECT COUNT(*) AS cnt FROM cayenne_mem_it WHERE id = 10",
            )
            .await?;
            let expected = ["+-----+", "| cnt |", "+-----+", "| 1   |", "+-----+"];
            assert_batches_eq!(expected, &result);

            // Memory mode is fully in-RAM: its (derived, never-created) data
            // directory must not exist on disk at all — no data files and no
            // snapshot directories (the metastore is an in-RAM memdb). It was
            // cleared before the run, so its presence now would mean a disk write.
            assert!(
                !data_path.exists(),
                "memory mode must not write anything to disk, but {data_path:?} exists"
            );

            // Regression for #11922: init() must skip metastore-directory setup
            // for the in-RAM memdb, so no `file:` directory is created. It was
            // cleared before the run, so its presence now would mean init()
            // created it.
            assert!(
                !stray_file_dir.exists(),
                "memory mode must not create a stray {stray_file_dir:?} directory (#11922)"
            );

            Ok(())
        })
        .await
}
