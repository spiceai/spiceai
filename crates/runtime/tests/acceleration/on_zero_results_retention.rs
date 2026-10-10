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

#![expect(clippy::expect_used, reason = "test schema and runtime invariants")]

//! Soft-deleted and time-expired rows that retention removed from the
//! accelerator must not come back through `on_zero_results: use_source`.

use std::{path::Path, sync::Arc, time::Duration};

use app::AppBuilder;
use arrow::array::{Array, Int64Array, RecordBatch};
use runtime::{Runtime, accelerated::AcceleratedTable};
use spicepod::{
    acceleration::{Acceleration, ZeroResultsAction},
    component::dataset::{Dataset, TimeFormat},
    param::Params,
};

use crate::{
    acceleration::load_runtime_datasets,
    configure_test_datafusion,
    utils::{
        register_test_connectors, run_query, runtime_ready_check, test_request_context,
        wait_until_true,
    },
};

fn ids(batches: &[RecordBatch]) -> Vec<i64> {
    let mut ids: Vec<_> = batches
        .iter()
        .flat_map(|batch| {
            let column = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("the query returns BIGINT ids");
            assert_eq!(column.null_count(), 0);
            column.values().to_vec()
        })
        .collect();
    ids.sort_unstable();
    ids
}

async fn accelerator_ids(rt: &Arc<Runtime>, table: &str) -> Vec<i64> {
    let provider = rt
        .datafusion()
        .get_accelerated_table_provider(table)
        .await
        .expect("accelerated table is registered");
    let accelerated = spice_table::find_layer::<AcceleratedTable>(
        provider.as_ref(),
        spice_table::LayerWalk::Read,
    )
    .expect("the runtime registered an accelerated table");
    let batches = rt
        .datafusion()
        .ctx
        .read_table(accelerated.get_accelerator())
        .expect("read accelerator")
        .collect()
        .await
        .expect("collect accelerator");
    ids(&batches)
}

#[cfg(feature = "duckdb")]
fn events_dataset(dir: &Path, name: &str, refresh_sql: Option<&str>) -> Dataset {
    let mut dataset = Dataset::new(format!("file://{}", dir.join("events.csv").display()), name);
    dataset.params = Some(Params::from_string_map(
        [("file_format".to_string(), "csv".to_string())].into(),
    ));
    dataset.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some("duckdb".to_string()),
        on_zero_results: ZeroResultsAction::UseSource,
        refresh_sql: refresh_sql.map(ToString::to_string),
        retention_sql: Some(format!("DELETE FROM {name} WHERE deleted = true")),
        retention_check_enabled: true,
        retention_check_interval: Some("200ms".to_string()),
        ..Acceleration::default()
    });
    dataset
}

#[cfg(feature = "duckdb")]
fn timed_events_dataset(dir: &Path, name: &str) -> Dataset {
    let mut dataset = Dataset::new(format!("file://{}", dir.join("timed.csv").display()), name);
    dataset.params = Some(Params::from_string_map(
        [("file_format".to_string(), "csv".to_string())].into(),
    ));
    dataset.time_column = Some("ts".to_string());
    dataset.time_format = Some(TimeFormat::UnixSeconds);
    dataset.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some("duckdb".to_string()),
        on_zero_results: ZeroResultsAction::UseSource,
        refresh_sql: Some(format!("SELECT * FROM {name} WHERE id != 3")),
        // Wider than the 2001 fixture timestamp so refresh loads id=2; the 1h
        // retention period is what then evicts it. `d` is a fundu unit; `y` is not.
        refresh_data_window: Some("10000d".to_string()),
        retention_check_enabled: true,
        // Longer than dataset load so the expired row is still visible after ready.
        retention_check_interval: Some("15s".to_string()),
        retention_period: Some("1h".to_string()),
        ..Acceleration::default()
    });
    dataset
}

/// Both retention policies at once: `retention_sql` deletes soft-deleted rows
/// and `retention_period` deletes rows older than an hour.
#[cfg(feature = "duckdb")]
fn combined_retention_dataset(dir: &Path, name: &str) -> Dataset {
    let mut dataset = Dataset::new(
        format!("file://{}", dir.join("retained.csv").display()),
        name,
    );
    dataset.params = Some(Params::from_string_map(
        [("file_format".to_string(), "csv".to_string())].into(),
    ));
    dataset.time_column = Some("ts".to_string());
    dataset.time_format = Some(TimeFormat::UnixSeconds);
    dataset.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some("duckdb".to_string()),
        on_zero_results: ZeroResultsAction::UseSource,
        refresh_sql: Some(format!("SELECT * FROM {name} WHERE id <= 4")),
        refresh_data_window: Some("10000d".to_string()),
        retention_sql: Some(format!("DELETE FROM {name} WHERE deleted = true")),
        retention_check_enabled: true,
        retention_check_interval: Some("200ms".to_string()),
        retention_period: Some("1h".to_string()),
        ..Acceleration::default()
    });
    dataset
}

#[cfg(not(target_os = "windows"))]
fn cayenne_time_dataset(source: &Path, name: &str, scheduled_ticker: bool) -> Dataset {
    let mut dataset = Dataset::new(format!("file://{}", source.display()), name);
    dataset.params = Some(Params::from_string_map(
        [("file_format".to_string(), "csv".to_string())].into(),
    ));
    // ISO-8601 CSV values so the file connector yields Timestamp rather
    // than Int64. Cayenne's scan-time keep builder rejects numeric columns
    // without a unix scale (`data_type_to_timestamp_format(..., None)`).
    // `time_format` is Timestamp because that is the inferred Arrow type;
    // ISO8601 is only valid while the column stays Utf8. The DuckDB fixture
    // can stay numeric.
    dataset.time_column = Some("ts".to_string());
    dataset.time_format = Some(TimeFormat::Timestamp);
    dataset.time_partition_column = Some("partition_ts".to_string());
    dataset.time_partition_format = Some(TimeFormat::Timestamp);
    dataset.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some("cayenne".to_string()),
        mode: spicepod::acceleration::Mode::Memory,
        refresh_mode: Some(spicepod::acceleration::RefreshMode::Full),
        on_zero_results: ZeroResultsAction::UseSource,
        refresh_sql: Some(format!("SELECT * FROM {name} WHERE id != 3")),
        refresh_data_window: Some("10000d".to_string()),
        retention_check_enabled: scheduled_ticker,
        retention_check_interval: scheduled_ticker.then(|| "15s".to_string()),
        retention_period: Some("1h".to_string()),
        ..Acceleration::default()
    });
    dataset
}

fn arrow_write_time_events_dataset(dir: &Path, name: &str) -> Dataset {
    let mut dataset = Dataset::new(format!("file://{}", dir.join("events.csv").display()), name);
    dataset.params = Some(Params::from_string_map(
        [("file_format".to_string(), "csv".to_string())].into(),
    ));
    dataset.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some("arrow".to_string()),
        on_zero_results: ZeroResultsAction::UseSource,
        refresh_sql: Some(format!("SELECT * FROM {name} WHERE id != 3")),
        retention_sql: Some(format!("DELETE FROM {name} WHERE id = 2")),
        retention_check_enabled: false,
        retention_check_interval: None,
        ..Acceleration::default()
    });
    dataset
}

#[cfg(feature = "duckdb")]
#[tokio::test]
async fn duckdb_retention_sql_does_not_resurrect_via_fallback() -> anyhow::Result<()> {
    register_test_connectors().await;
    test_request_context()
        .scope(async {
            let dir = tempfile::tempdir()?;
            std::fs::write(
                dir.path().join("events.csv"),
                "id,name,deleted\n1,keep,false\n2,gone,true\n3,miss,false\n",
            )?;

            let app = AppBuilder::new("retention_fallback")
                .with_dataset(events_dataset(
                    dir.path(),
                    "events",
                    Some("SELECT * FROM events WHERE id != 3"),
                ))
                .build();
            configure_test_datafusion();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);
            load_runtime_datasets(&rt, Duration::from_mins(1)).await?;

            let evicted = wait_until_true(Duration::from_secs(10), || {
                let rt = Arc::clone(&rt);
                async move {
                    let ids = accelerator_ids(&rt, "events").await;
                    ids.contains(&1) && !ids.contains(&2)
                }
            })
            .await;
            assert!(
                evicted,
                "refresh must load id=1 and retention_sql must remove id=2, leftover {:?}",
                accelerator_ids(&rt, "events").await
            );
            assert!(
                !accelerator_ids(&rt, "events").await.contains(&3),
                "refresh_sql must leave id=3 out of the accelerator so fallback is the only path"
            );

            let evicted_row = run_query(&rt, "SELECT id FROM events WHERE id = 2").await?;
            assert_eq!(
                ids(&evicted_row),
                Vec::<i64>::new(),
                "an evicted soft-deleted row must not come back from the source"
            );

            let projected = run_query(&rt, "SELECT name FROM events WHERE id = 2").await?;
            let projected_rows: usize = projected.iter().map(RecordBatch::num_rows).sum();
            assert_eq!(
                projected_rows, 0,
                "a projection that omits `deleted` must still hide the evicted row"
            );

            let fallback = run_query(&rt, "SELECT id FROM events WHERE id = 3").await?;
            assert_eq!(
                ids(&fallback),
                vec![3],
                "a row never loaded, that retention would keep, must still fall back"
            );

            let kept = run_query(&rt, "SELECT id FROM events WHERE id = 1").await?;
            assert_eq!(ids(&kept), vec![1]);

            rt.shutdown().await;
            Ok(())
        })
        .await
}

#[cfg(feature = "duckdb")]
#[tokio::test]
async fn duckdb_time_retention_does_not_resurrect_via_fallback() -> anyhow::Result<()> {
    register_test_connectors().await;
    test_request_context()
        .scope(async {
            let dir = tempfile::tempdir()?;
            // 1_000_000_000 is 2001-09-09; 4_102_444_800 is 2100-01-01.
            std::fs::write(
                dir.path().join("timed.csv"),
                "id,ts\n1,4102444800\n2,1000000000\n3,4102444800\n",
            )?;

            let app = AppBuilder::new("time_retention_fallback")
                .with_dataset(timed_events_dataset(dir.path(), "timed"))
                .build();
            configure_test_datafusion();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);
            load_runtime_datasets(&rt, Duration::from_mins(1)).await?;

            let loaded = accelerator_ids(&rt, "timed").await;
            assert!(
                loaded.contains(&2),
                "refresh_data_window must load id=2 before retention evicts it, leftover {loaded:?}"
            );

            let evicted = wait_until_true(Duration::from_secs(30), || {
                let rt = Arc::clone(&rt);
                async move {
                    let ids = accelerator_ids(&rt, "timed").await;
                    ids.contains(&1) && !ids.contains(&2)
                }
            })
            .await;
            assert!(
                evicted,
                "time retention must remove id=2 after it was loaded, leftover {:?}",
                accelerator_ids(&rt, "timed").await
            );

            let evicted_row = run_query(&rt, "SELECT id FROM timed WHERE id = 2").await?;
            assert_eq!(
                ids(&evicted_row),
                Vec::<i64>::new(),
                "a time-expired row must not come back from the source"
            );

            let fallback = run_query(&rt, "SELECT id FROM timed WHERE id = 3").await?;
            assert_eq!(
                ids(&fallback),
                vec![3],
                "a recent row never loaded must still fall back"
            );

            rt.shutdown().await;
            Ok(())
        })
        .await
}

/// With `retention_sql` and `retention_period` both set, fallback returns only
/// the rows neither policy deletes. A NULL `deleted` or `ts` matches neither
/// delete, so the accelerator would keep that row, and so must fallback.
#[cfg(feature = "duckdb")]
#[tokio::test]
async fn duckdb_combined_retention_does_not_resurrect_via_fallback() -> anyhow::Result<()> {
    register_test_connectors().await;
    test_request_context()
        .scope(async {
            let dir = tempfile::tempdir()?;
            // 4_102_444_800 is 2100-01-01; 1_000_000_000 is 2001-09-09. Refresh
            // loads ids 1-4: 1 is kept, 2 is soft-deleted, 3 has expired, and 4
            // is both. Ids 5-9 are never loaded: 5 is kept, 6 has a NULL
            // `deleted`, 7 has a NULL `ts`, 8 is soft-deleted, and 9 has expired.
            std::fs::write(
                dir.path().join("retained.csv"),
                "id,ts,deleted\n\
                 1,4102444800,false\n\
                 2,4102444800,true\n\
                 3,1000000000,false\n\
                 4,1000000000,true\n\
                 5,4102444800,false\n\
                 6,4102444800,\n\
                 7,,false\n\
                 8,4102444800,true\n\
                 9,1000000000,false\n",
            )?;

            let app = AppBuilder::new("combined_retention_fallback")
                .with_dataset(combined_retention_dataset(dir.path(), "retained"))
                .build();
            configure_test_datafusion();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);
            load_runtime_datasets(&rt, Duration::from_mins(1)).await?;

            let evicted = wait_until_true(Duration::from_secs(10), || {
                let rt = Arc::clone(&rt);
                async move { accelerator_ids(&rt, "retained").await == vec![1] }
            })
            .await;
            assert!(
                evicted,
                "refresh must load ids 1-4 and retention must leave only id=1, leftover {:?}",
                accelerator_ids(&rt, "retained").await
            );

            for (id, what) in [
                (2, "a soft-deleted row"),
                (3, "an expired row"),
                (4, "a soft-deleted, expired row"),
                (8, "a soft-deleted row never loaded"),
                (9, "an expired row never loaded"),
            ] {
                let rows =
                    run_query(&rt, &format!("SELECT id FROM retained WHERE id = {id}")).await?;
                assert_eq!(
                    ids(&rows),
                    Vec::<i64>::new(),
                    "{what} (id={id}) must not come back from the source"
                );
            }

            let fallback = run_query(&rt, "SELECT id FROM retained WHERE id >= 5").await?;
            assert_eq!(
                ids(&fallback),
                vec![5, 6, 7],
                "the never-loaded rows retention keeps, including a NULL `deleted` and a NULL `ts`, must fall back"
            );

            let kept = run_query(&rt, "SELECT id FROM retained WHERE id = 1").await?;
            assert_eq!(ids(&kept), vec![1]);

            rt.shutdown().await;
            Ok(())
        })
        .await
}

#[tokio::test]
async fn arrow_write_time_retention_sql_does_not_resurrect_via_fallback() -> anyhow::Result<()> {
    register_test_connectors().await;
    test_request_context()
        .scope(async {
            let dir = tempfile::tempdir()?;
            std::fs::write(
                dir.path().join("events.csv"),
                "id,name\n1,keep\n2,gone\n3,miss\n",
            )?;

            let app = AppBuilder::new("arrow_write_time_retention_fallback")
                .with_dataset(arrow_write_time_events_dataset(dir.path(), "events"))
                .build();
            configure_test_datafusion();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);
            load_runtime_datasets(&rt, Duration::from_mins(1)).await?;

            let ids_in_accel = accelerator_ids(&rt, "events").await;
            assert!(
                ids_in_accel.contains(&1),
                "refresh must load id=1, leftover {ids_in_accel:?}"
            );
            assert!(
                !ids_in_accel.contains(&2),
                "write-time retention_sql must remove id=2 without a scheduled worker, leftover {ids_in_accel:?}"
            );
            assert!(
                !ids_in_accel.contains(&3),
                "refresh_sql must leave id=3 out of the accelerator so fallback is the only path"
            );

            let evicted_row = run_query(&rt, "SELECT id FROM events WHERE id = 2").await?;
            assert_eq!(
                ids(&evicted_row),
                Vec::<i64>::new(),
                "a row removed on the refresh write path must not come back from the source"
            );

            let fallback = run_query(&rt, "SELECT id FROM events WHERE id = 3").await?;
            assert_eq!(
                ids(&fallback),
                vec![3],
                "a row never loaded, that retention would keep, must still fall back"
            );

            rt.shutdown().await;
            Ok(())
        })
        .await
}

#[cfg(not(target_os = "windows"))]
async fn cayenne_time_retention_fallback_case(
    table: &str,
    scheduled_ticker: bool,
) -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let source = dir.path().join("timed.csv");
    // id=2 is expired on `ts` but recent on `partition_ts`. Cayenne's
    // scan-time keep uses only `ts`, so a partition-AND fallback would
    // resurrect it. A NULL-timestamp row is not in this fixture:
    // `refresh_data_window` is `ts >= cutoff` and never loads NULLs;
    // that keep is covered by `unscheduled_time_keep_matches_cayenne_without_partition`.
    std::fs::write(
        &source,
        "id,ts,partition_ts\n\
         1,2100-01-01T00:00:00Z,2100-01-01T00:00:00Z\n\
         2,2001-09-09T01:46:40Z,2100-01-01T00:00:00Z\n\
         3,2100-01-01T00:00:00Z,2100-01-01T00:00:00Z\n",
    )?;

    configure_test_datafusion();
    let app = AppBuilder::new(table)
        .with_dataset(cayenne_time_dataset(&source, table, scheduled_ticker))
        .build();
    let rt = Arc::new(Runtime::builder().with_app(app).build().await);
    tokio::select! {
        () = tokio::time::sleep(Duration::from_mins(1)) => {
            return Err(anyhow::anyhow!("Timeout waiting for components to load"));
        }
        () = Arc::clone(&rt).load_components() => {}
    }
    runtime_ready_check(&rt).await;

    let ids_in_accel = accelerator_ids(&rt, table).await;
    assert!(
        ids_in_accel.contains(&1),
        "refresh must load id=1, leftover {ids_in_accel:?}"
    );
    assert!(
        !ids_in_accel.contains(&2),
        "Cayenne scan-time retention must hide expired id=2 even when partition_ts is recent, leftover {ids_in_accel:?}"
    );
    assert!(
        !ids_in_accel.contains(&3),
        "refresh_sql must leave id=3 out of the accelerator so fallback is the only path"
    );

    let evicted_row = run_query(&rt, &format!("SELECT id FROM {table} WHERE id = 2")).await?;
    assert_eq!(
        ids(&evicted_row),
        Vec::<i64>::new(),
        "a row Cayenne hides at scan time must not come back from the source"
    );

    let fallback = run_query(&rt, &format!("SELECT id FROM {table} WHERE id = 3")).await?;
    assert_eq!(
        ids(&fallback),
        vec![3],
        "a recent row never loaded must still fall back"
    );

    rt.shutdown().await;
    Ok(())
}

#[cfg(not(target_os = "windows"))]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cayenne_unscheduled_time_retention_does_not_resurrect_via_fallback() -> anyhow::Result<()>
{
    let _tracing = crate::init_tracing(Some("integration=debug,info"));
    test_request_context()
        .scope(cayenne_time_retention_fallback_case(
            "cayenne_unscheduled_time_it",
            false,
        ))
        .await
}

#[cfg(not(target_os = "windows"))]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cayenne_scheduled_time_retention_does_not_resurrect_via_fallback() -> anyhow::Result<()> {
    let _tracing = crate::init_tracing(Some("integration=debug,info"));
    test_request_context()
        .scope(cayenne_time_retention_fallback_case(
            "cayenne_scheduled_time_it",
            true,
        ))
        .await
}
