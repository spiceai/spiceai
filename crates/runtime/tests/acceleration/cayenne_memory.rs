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
//!
//! The DML cases at the bottom cover #12008: `mode: memory` makes the RAM
//! mem-tier the permanent store, and Cayenne's deletion sink scans only the
//! durable tiers, so a statement whose row selection has to FIND rows finds
//! none. Each is paired with a `mode: file` control running the same statements
//! against the same fixture — the control is the oracle, and the two arms differ
//! in exactly one input.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::ensure;
use app::AppBuilder;
use arrow::array::RecordBatch;
use arrow::util::pretty::pretty_format_batches;
use datafusion::{assert_batches_eq, sql::TableReference};
use futures::TryStreamExt;
use runtime::{Runtime, accelerated::AcceleratedTable};
use runtime_request_context::{CacheControl, Protocol, RequestContext, UserAgent};
use spicepod::{
    acceleration::{Acceleration, Mode, OnConflictBehavior, RefreshMode},
    component::{access::AccessMode, dataset::Dataset},
    param::Params,
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
        .wait()
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

// ── #12008: DML against a `mode: memory` Cayenne acceleration ──────────────
//
// One `cfg` for the whole section rather than one per test: the engine is not
// built on Windows, so without it the helpers below compile there with no callers.
#[cfg(not(target_os = "windows"))]
mod dml {
    use super::*;
    use super::{execute_sql, refresh};

    /// A request context that bypasses the results cache, so a `SELECT` after a
    /// mutation reflects the accelerator rather than a cached answer.
    fn no_cache_context() -> Arc<RequestContext> {
        Arc::new(
            RequestContext::builder(Protocol::Internal)
                .with_user_agent(UserAgent::from_ua_str(&format!(
                    "spiceci/{}",
                    env!("CARGO_PKG_VERSION")
                )))
                .with_cache_control(CacheControl::NoCache)
                .build(),
        )
    }

    /// Stand up a one-dataset runtime whose `table_name` is a Cayenne acceleration in
    /// `mode`, over a five-row CSV, and assert the two preconditions every DML case
    /// below depends on.
    ///
    /// `primary_key` + `on_conflict` is what routes writes to the accelerator alone
    /// (`select_accelerated_write_mode`), which is how a client statement reaches
    /// Cayenne rather than the file source.
    ///
    /// Returns the temp dir alongside the runtime because the CSV source lives in it
    /// and must outlive the runtime.
    async fn cayenne_dml_runtime(
        mode: Mode,
        table_name: &str,
    ) -> Result<(tempfile::TempDir, Arc<Runtime>), anyhow::Error> {
        let temp_dir = tempfile::tempdir()?;
        let csv = temp_dir.path().join(format!("{table_name}.csv"));
        std::fs::write(
            &csv,
            "id,name,value\n\
         1,alpha,100\n\
         2,beta,200\n\
         3,gamma,300\n\
         4,delta,400\n\
         5,epsilon,500\n",
        )?;

        crate::configure_test_datafusion();

        let is_memory = matches!(mode, Mode::Memory);
        let mode_label = format!("{mode:?}");

        // A file acceleration needs somewhere to put its Vortex files and its
        // metastore; a memory acceleration builds both in RAM and takes neither.
        let params = if is_memory {
            None
        } else {
            let mut params = HashMap::new();
            params.insert(
                "cayenne_file_path".to_string(),
                temp_dir.path().join("cayenne").display().to_string(),
            );
            params.insert(
                "cayenne_metadata_dir".to_string(),
                temp_dir.path().join("metadata").display().to_string(),
            );
            Some(Params::from_string_map(params))
        };

        let mut dataset = Dataset::new(format!("file://{}", csv.display()), table_name);
        dataset.access = AccessMode::ReadWrite;
        dataset.acceleration = Some(Acceleration {
            enabled: true,
            engine: Some("cayenne".to_string()),
            mode,
            refresh_mode: Some(RefreshMode::Full),
            params,
            primary_key: Some("id".to_string()),
            on_conflict: HashMap::from([("id".to_string(), OnConflictBehavior::Upsert)]),
            ..Acceleration::default()
        });

        let app = AppBuilder::new("test_cayenne_memory_dml")
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

        // Premise: the acceleration really resolved to the residency this arm is
        // about. Without this a `mode: memory` arm would still fail if it had
        // silently fallen back to a file acceleration, and the failure would say
        // nothing about the mem-tier.
        let table = rt
            .datafusion()
            .get_table(&TableReference::bare(table_name))
            .await
            .ok_or_else(|| anyhow::anyhow!("table '{table_name}' not found"))?;
        let accelerated_table = spice_table::find_layer::<AcceleratedTable>(
            table.as_ref(),
            spice_table::LayerWalk::Read,
        )
        .ok_or_else(|| anyhow::anyhow!("table '{table_name}' is not an AcceleratedTable"))?;
        let accelerator = accelerated_table.get_accelerator();
        // The accelerator may be wrapped in `SpiceTable` layers; walk down to the
        // Cayenne provider itself.
        let cayenne = accelerator
            .downcast_ref::<cayenne::CayenneTableProvider>()
            .or_else(|| {
                spice_table::nodes(accelerator.as_ref(), spice_table::LayerWalk::Read).find_map(
                    |node| {
                        node.base_provider()
                            .downcast_ref::<cayenne::CayenneTableProvider>()
                    },
                )
            })
            .ok_or_else(|| anyhow::anyhow!("accelerator is not a CayenneTableProvider"))?;
        ensure!(
            cayenne.is_memory_resident_mode() == is_memory,
            "precondition: acceleration mode {mode_label} must resolve to \
         is_memory_resident_mode() == {is_memory}, got {}",
            cayenne.is_memory_resident_mode()
        );
        // Premise: writes go to the accelerator, so the statements below are
        // Cayenne's and not the file connector's.
        ensure!(
            accelerated_table.is_accelerator_only(),
            "precondition: on_conflict must route writes to the accelerator alone, \
         otherwise the statement never reaches Cayenne"
        );

        Ok((temp_dir, rt))
    }

    /// The `count` a DML statement reported. Every mutation below asserts this as
    /// well as the resulting rows: the count is what a client is told changed, and a
    /// path that removes the right rows while reporting the wrong number is still
    /// wrong (`with_exact_count` makes a user DELETE an exact count, not an estimate).
    fn reported_count(batches: &[RecordBatch]) -> u64 {
        batches
            .first()
            .and_then(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::UInt64Array>()
            })
            .and_then(|a| a.values().first())
            .copied()
            .expect("a DML result must carry a UInt64 count column")
    }

    /// Rows whose primary key appears more than once. Empty is the only correct
    /// answer for a table declaring `primary_key: id` with `on_conflict: upsert`.
    async fn duplicate_keys(
        rt: &Arc<Runtime>,
        table_name: &str,
    ) -> Result<Vec<RecordBatch>, anyhow::Error> {
        execute_sql(
        rt,
        &format!(
            "SELECT id, COUNT(*) AS n FROM {table_name} GROUP BY id HAVING COUNT(*) > 1 ORDER BY id"
        ),
    )
    .await
    }

    /// `DELETE … WHERE`, per-key and range.
    async fn filtered_delete_by_mode(mode: Mode, table_name: &str) -> Result<(), anyhow::Error> {
        let _tracing = crate::init_tracing(Some("integration=debug,info"));
        no_cache_context()
            .scope(async {
                let mode_label = format!("{mode:?}");
                let (_temp_dir, rt) = cayenne_dml_runtime(mode, table_name).await?;

                let result =
                    execute_sql(&rt, &format!("SELECT id FROM {table_name} ORDER BY id")).await?;
                let expected = [
                    "+----+", "| id |", "+----+", "| 1  |", "| 2  |", "| 3  |", "| 4  |", "| 5  |",
                    "+----+",
                ];
                assert_batches_eq!(expected, &result);

                let deleted =
                    execute_sql(&rt, &format!("DELETE FROM {table_name} WHERE id = 2")).await?;
                let after =
                    execute_sql(&rt, &format!("SELECT id FROM {table_name} ORDER BY id")).await?;
                eprintln!(
                    "[{mode_label}] DELETE WHERE id = 2 reported:\n{}\nrows now:\n{}",
                    pretty_format_batches(&deleted)?,
                    pretty_format_batches(&after)?
                );
                assert_eq!(
                    reported_count(&deleted),
                    1,
                    "exactly one row matches `id = 2`"
                );
                let expected = [
                    "+----+", "| id |", "+----+", "| 1  |", "| 3  |", "| 4  |", "| 5  |", "+----+",
                ];
                assert_batches_eq!(expected, &after);

                let deleted =
                    execute_sql(&rt, &format!("DELETE FROM {table_name} WHERE id > 3")).await?;
                let after =
                    execute_sql(&rt, &format!("SELECT id FROM {table_name} ORDER BY id")).await?;
                eprintln!(
                    "[{mode_label}] DELETE WHERE id > 3 reported:\n{}\nrows now:\n{}",
                    pretty_format_batches(&deleted)?,
                    pretty_format_batches(&after)?
                );
                assert_eq!(
                    reported_count(&deleted),
                    2,
                    "exactly two rows match `id > 3`"
                );
                let expected = ["+----+", "| id |", "+----+", "| 1  |", "| 3  |", "+----+"];
                assert_batches_eq!(expected, &after);

                Ok(())
            })
            .await
    }

    /// `UPDATE … WHERE`, which `UpdateExec` runs as delete-then-insert, so its
    /// delete leg hits the same gap. The failure is not a no-op: the insert leg
    /// still lands, so the row is DUPLICATED under a declared primary key.
    async fn update_by_mode(mode: Mode, table_name: &str) -> Result<(), anyhow::Error> {
        let _tracing = crate::init_tracing(Some("integration=debug,info"));
        no_cache_context()
        .scope(async {
            let mode_label = format!("{mode:?}");
            let (_temp_dir, rt) = cayenne_dml_runtime(mode, table_name).await?;

            let updated = execute_sql(
                &rt,
                &format!("UPDATE {table_name} SET value = 999 WHERE id = 2"),
            )
            .await?;
            let after = execute_sql(
                &rt,
                &format!("SELECT id, value FROM {table_name} ORDER BY id, value"),
            )
            .await?;
            eprintln!(
                "[{mode_label}] UPDATE SET value = 999 WHERE id = 2 reported:\n{}\nrows now:\n{}",
                pretty_format_batches(&updated)?,
                pretty_format_batches(&after)?
            );
            assert_eq!(
                reported_count(&updated),
                1,
                "exactly one row matches `id = 2`"
            );
            let expected = [
                "+----+-------+",
                "| id | value |",
                "+----+-------+",
                "| 1  | 100   |",
                "| 2  | 999   |",
                "| 3  | 300   |",
                "| 4  | 400   |",
                "| 5  | 500   |",
                "+----+-------+",
            ];
            assert_batches_eq!(expected, &after);

            // Asserted on its own so a regression that changes the surviving
            // value still fails here: the duplicate is what makes this data
            // corruption rather than a lost update.
            let dupes = duplicate_keys(&rt, table_name).await?;
            eprintln!(
                "[{mode_label}] duplicate primary keys after UPDATE:\n{}",
                pretty_format_batches(&dupes)?
            );
            assert_batches_eq!(["++", "++"], &dupes);

            Ok(())
        })
        .await
    }

    /// `INSERT` of a primary key that already exists.
    ///
    /// No `DELETE` is involved, which is what makes this a defect of its own rather
    /// than a consequence of the delete gap: the memory-mode standard-DML append
    /// (`write_batches_memory_mode`) passes `OnConflictDeletions::default()`, so the
    /// prior version is never superseded.
    async fn upsert_insert_by_mode(mode: Mode, table_name: &str) -> Result<(), anyhow::Error> {
        let _tracing = crate::init_tracing(Some("integration=debug,info"));
        no_cache_context()
        .scope(async {
            let mode_label = format!("{mode:?}");
            let (_temp_dir, rt) = cayenne_dml_runtime(mode, table_name).await?;

            let inserted = execute_sql(
                &rt,
                &format!("INSERT INTO {table_name} (id, name, value) VALUES (3, 'gamma2', 3333)"),
            )
            .await?;
            let after = execute_sql(
                &rt,
                &format!("SELECT id, name, value FROM {table_name} ORDER BY id, value"),
            )
            .await?;
            eprintln!(
                "[{mode_label}] INSERT (3, gamma2, 3333) over existing PK 3 reported:\n{}\nrows now:\n{}",
                pretty_format_batches(&inserted)?,
                pretty_format_batches(&after)?
            );
            assert_eq!(
                reported_count(&inserted),
                1,
                "one row was inserted — not the mem-tier epoch"
            );
            let expected = [
                "+----+---------+-------+",
                "| id | name    | value |",
                "+----+---------+-------+",
                "| 1  | alpha   | 100   |",
                "| 2  | beta    | 200   |",
                "| 3  | gamma2  | 3333  |",
                "| 4  | delta   | 400   |",
                "| 5  | epsilon | 500   |",
                "+----+---------+-------+",
            ];
            assert_batches_eq!(expected, &after);

            let dupes = duplicate_keys(&rt, table_name).await?;
            eprintln!(
                "[{mode_label}] duplicate primary keys after INSERT:\n{}",
                pretty_format_batches(&dupes)?
            );
            assert_batches_eq!(["++", "++"], &dupes);

            Ok(())
        })
        .await
    }

    /// A `DELETE` over an upsert history must count the rows a scan SERVES, not the
    /// raw versions the store happens to hold.
    ///
    /// After `INSERT (3, gamma2)` supersedes `(3, gamma)`, the superseded version is
    /// still resident and hidden by its successor's tombstone. `DELETE WHERE id = 3`
    /// must report ONE row, and a predicate matching only the hidden version must
    /// report NONE and remove nothing a client can see.
    async fn delete_over_upsert_history_by_mode(
        mode: Mode,
        table_name: &str,
    ) -> Result<(), anyhow::Error> {
        let _tracing = crate::init_tracing(Some("integration=debug,info"));
        no_cache_context()
        .scope(async {
            let mode_label = format!("{mode:?}");
            let (_temp_dir, rt) = cayenne_dml_runtime(mode, table_name).await?;

            execute_sql(
                &rt,
                &format!("INSERT INTO {table_name} (id, name, value) VALUES (3, 'gamma2', 3333)"),
            )
            .await?;

            // Matches only the SUPERSEDED version, which no scan serves.
            let deleted = execute_sql(
                &rt,
                &format!("DELETE FROM {table_name} WHERE name = 'gamma'"),
            )
            .await?;
            let after = execute_sql(
                &rt,
                &format!("SELECT id, name FROM {table_name} WHERE id = 3 ORDER BY name"),
            )
            .await?;
            eprintln!(
                "[{mode_label}] DELETE WHERE name = 'gamma' (superseded version only) reported:\n{}\nid=3 rows now:\n{}",
                pretty_format_batches(&deleted)?,
                pretty_format_batches(&after)?
            );
            assert_eq!(
                reported_count(&deleted),
                0,
                "no row a scan serves matches `name = 'gamma'` — the superseded version is not one"
            );
            let expected = [
                "+----+--------+",
                "| id | name   |",
                "+----+--------+",
                "| 3  | gamma2 |",
                "+----+--------+",
            ];
            assert_batches_eq!(expected, &after);

            // Matches the LIVE version. One row, not two.
            let deleted =
                execute_sql(&rt, &format!("DELETE FROM {table_name} WHERE id = 3")).await?;
            let after = execute_sql(
                &rt,
                &format!("SELECT id FROM {table_name} WHERE id = 3"),
            )
            .await?;
            eprintln!(
                "[{mode_label}] DELETE WHERE id = 3 over an upsert history reported:\n{}\nid=3 rows now:\n{}",
                pretty_format_batches(&deleted)?,
                pretty_format_batches(&after)?
            );
            assert_eq!(
                reported_count(&deleted),
                1,
                "one row is served for id = 3, however many versions the store holds"
            );
            assert_batches_eq!(["++", "++"], &after);

            Ok(())
        })
        .await
    }

    // ── control arms: `mode: file`, where all three statements work ──

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_cayenne_file_mode_filtered_delete() -> Result<(), anyhow::Error> {
        filtered_delete_by_mode(Mode::File, "file_mode_delete_test").await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_cayenne_file_mode_update() -> Result<(), anyhow::Error> {
        update_by_mode(Mode::File, "file_mode_update_test").await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_cayenne_file_mode_upsert_insert() -> Result<(), anyhow::Error> {
        upsert_insert_by_mode(Mode::File, "file_mode_upsert_test").await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_cayenne_file_mode_delete_over_upsert_history() -> Result<(), anyhow::Error> {
        delete_over_upsert_history_by_mode(Mode::File, "file_mode_history_test").await
    }

    // ── reproduction arms: `mode: memory` (#12008 and its upsert sibling) ──

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_cayenne_memory_mode_filtered_delete() -> Result<(), anyhow::Error> {
        filtered_delete_by_mode(Mode::Memory, "memory_mode_delete_test").await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_cayenne_memory_mode_update() -> Result<(), anyhow::Error> {
        update_by_mode(Mode::Memory, "memory_mode_update_test").await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_cayenne_memory_mode_upsert_insert() -> Result<(), anyhow::Error> {
        upsert_insert_by_mode(Mode::Memory, "memory_mode_upsert_test").await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_cayenne_memory_mode_delete_over_upsert_history() -> Result<(), anyhow::Error> {
        delete_over_upsert_history_by_mode(Mode::Memory, "memory_mode_history_test").await
    }
}
