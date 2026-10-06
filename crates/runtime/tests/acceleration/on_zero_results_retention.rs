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
    utils::{register_test_connectors, run_query, test_request_context, wait_until_true},
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
        retention_check_enabled: true,
        retention_check_interval: Some("200ms".to_string()),
        retention_period: Some("1h".to_string()),
        ..Acceleration::default()
    });
    dataset
}

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

            let evicted = wait_until_true(Duration::from_secs(10), || {
                let rt = Arc::clone(&rt);
                async move {
                    let ids = accelerator_ids(&rt, "timed").await;
                    ids.contains(&1) && !ids.contains(&2)
                }
            })
            .await;
            assert!(
                evicted,
                "refresh must load id=1 and time retention must remove id=2, leftover {:?}",
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
