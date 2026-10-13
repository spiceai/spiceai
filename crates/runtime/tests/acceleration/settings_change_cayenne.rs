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

//! Restarting a file-mode Cayenne acceleration with changed acceleration settings
//! (spiceai/spiceai#15059). Layout settings are applied to the existing table in place;
//! settings that define the stored rows rebuild it from the source under
//! `refresh_mode: full`, and are kept with their stored values where a rebuild could
//! lose rows.
//!
//! Each phase restarts the runtime against the same data and metastore directories and
//! asserts on what the metastore records, not only on query results: the Cayenne
//! table id is minted when a table is created, so an unchanged id proves the existing
//! acceleration was reused and a new one proves it was rebuilt.

use std::{collections::HashMap, path::Path, sync::Arc};

use app::AppBuilder;
use arrow::array::{Int64Array, RecordBatch};
use cayenne::{CayenneCatalog, MetadataCatalog, metadata::TableMetadata};
use futures::TryStreamExt;
use runtime::Runtime;
use spicepod::{
    acceleration::{Acceleration, Mode, RefreshMode},
    component::dataset::{Dataset, TimeFormat},
    param::Params,
    partitioning::PartitionedBy,
};

use crate::utils::{runtime_ready_check, test_request_context};

/// Five rows over three ids: ids 1 and 2 appear twice, so the row count shows whether
/// the acceleration keeps one row per primary key. `ts` (Unix seconds) is the append
/// dataset's time column, so its restarts find nothing new to append.
const CSV: &str =
    "id,name,ts\n1,a,1704067200\n2,b,1704153600\n1,c,1704240000\n3,d,1704326400\n2,e,1704412800\n";

/// The settings a phase restarts the dataset with.
#[derive(Clone, Default)]
struct Settings {
    refresh_mode: Option<RefreshMode>,
    primary_key: Option<&'static str>,
    sort_columns: Option<&'static str>,
    partition_by: Option<&'static str>,
}

struct Fixture {
    _dir: tempfile::TempDir,
    csv: String,
    data_dir: String,
    metadata_dir: String,
}

impl Fixture {
    fn new() -> Result<Self, anyhow::Error> {
        let dir = tempfile::tempdir()?;
        let csv = dir.path().join("trips.csv");
        std::fs::write(&csv, CSV)?;
        let path = |name: &str| dir.path().join(name).to_string_lossy().to_string();
        Ok(Self {
            csv: csv.to_string_lossy().to_string(),
            data_dir: path("data"),
            metadata_dir: path("metadata"),
            _dir: dir,
        })
    }

    fn dataset(&self, name: &str, settings: &Settings) -> Dataset {
        let mut params = HashMap::from([
            ("cayenne_file_path".to_string(), self.data_dir.clone()),
            (
                "cayenne_metadata_dir".to_string(),
                self.metadata_dir.clone(),
            ),
        ]);
        if let Some(sort_columns) = settings.sort_columns {
            params.insert("cayenne_sort_columns".to_string(), sort_columns.to_string());
        }
        let mut dataset = Dataset::new(format!("file:{}", self.csv), name);
        if settings.refresh_mode == Some(RefreshMode::Append) {
            dataset.time_column = Some("ts".to_string());
            dataset.time_format = Some(TimeFormat::UnixSeconds);
        }
        dataset.acceleration = Some(Acceleration {
            enabled: true,
            engine: Some("cayenne".to_string()),
            mode: Mode::File,
            refresh_mode: settings.refresh_mode.clone(),
            primary_key: settings.primary_key.map(ToString::to_string),
            partition_by: settings
                .partition_by
                .map(|expression| PartitionedBy {
                    name: "expr0".to_string(),
                    expression: expression.to_string(),
                })
                .into_iter()
                .collect(),
            params: Some(Params::from_string_map(params)),
            ..Acceleration::default()
        });
        dataset
    }

    /// Starts a runtime serving `name` with `settings`, waits for it to be ready, and
    /// returns the ids of its rows in order, then shuts it down.
    async fn ids_after_restart(
        &self,
        name: &str,
        settings: &Settings,
    ) -> Result<Vec<i64>, anyhow::Error> {
        let app = AppBuilder::new("settings_change_cayenne")
            .with_dataset(self.dataset(name, settings))
            .build();
        crate::configure_test_datafusion();
        let rt = Arc::new(Runtime::builder().with_app(app).build().await);
        tokio::select! {
            () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
                return Err(anyhow::anyhow!("Timed out waiting for dataset '{name}' to load"));
            }
            () = Arc::clone(&rt).load_components() => {}
        }
        runtime_ready_check(&rt).await;

        let batches: Vec<RecordBatch> = rt
            .datafusion()
            .query_builder(&format!("SELECT id FROM {name} ORDER BY id"))
            .build()
            .run()
            .await
            .map_err(|e| anyhow::anyhow!("Query of dataset '{name}' failed: {e}"))?
            .data
            .try_collect()
            .await?;
        rt.shutdown().await;

        let mut ids = Vec::new();
        for batch in &batches {
            let column = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow::anyhow!("`id` is not Int64: {:?}", batch.schema()))?;
            ids.extend(column.iter().flatten());
        }
        Ok(ids)
    }

    /// What the Cayenne metastore records for `name`, read while no runtime holds it.
    async fn stored(&self, name: &str) -> Result<TableMetadata, anyhow::Error> {
        let catalog = CayenneCatalog::new(format!(
            "sqlite://{}",
            Path::new(&self.metadata_dir).join("cayenne.db").display()
        ))?;
        catalog.init().await?;
        Ok(catalog.get_table(name).await?)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn full_refresh_acceleration_adopts_changed_settings_on_restart() -> Result<(), anyhow::Error>
{
    let _tracing = crate::init_tracing(Some("integration=debug,info"));
    test_request_context()
        .scope(async {
            let fixture = Fixture::new()?;
            let full = Settings {
                refresh_mode: Some(RefreshMode::Full),
                ..Settings::default()
            };

            // Built with no primary key and no sort order: every source row is kept.
            let ids = fixture.ids_after_restart("trips", &full).await?;
            assert_eq!(ids, vec![1, 1, 2, 2, 3]);
            let built = fixture.stored("trips").await?;
            assert!(built.vortex_config.sort_columns.is_empty());

            // A sort order is a layout setting: the existing acceleration is reused (same
            // table) and records the new setting.
            let sorted = Settings {
                sort_columns: Some("id"),
                ..full.clone()
            };
            let ids = fixture.ids_after_restart("trips", &sorted).await?;
            assert_eq!(ids, vec![1, 1, 2, 2, 3]);
            let stored = fixture.stored("trips").await?;
            assert_eq!(
                stored.table_id, built.table_id,
                "a sort order change must not rebuild the acceleration"
            );
            assert_eq!(stored.vortex_config.sort_columns, vec!["id".to_string()]);

            // A primary key changes what the stored rows mean: the acceleration is rebuilt
            // from the source with the key, so it now holds one row per id.
            let keyed = Settings {
                primary_key: Some("id"),
                ..sorted.clone()
            };
            let ids = fixture.ids_after_restart("trips", &keyed).await?;
            assert_eq!(ids, vec![1, 2, 3]);
            let rebuilt = fixture.stored("trips").await?;
            assert_ne!(
                rebuilt.table_id, built.table_id,
                "a primary key change must rebuild the acceleration"
            );
            assert_eq!(rebuilt.primary_key, vec!["id".to_string()]);
            assert_eq!(rebuilt.vortex_config.sort_columns, vec!["id".to_string()]);

            // Partitioning an unpartitioned acceleration rebuilds it too; before
            // spiceai/spiceai#15059 this failed the dataset with `PartitionByChanged`.
            let partitioned = Settings {
                partition_by: Some("bucket(2, id)"),
                ..keyed.clone()
            };
            let ids = fixture.ids_after_restart("trips", &partitioned).await?;
            assert_eq!(ids, vec![1, 2, 3]);
            let repartitioned = fixture.stored("trips").await?;
            assert_ne!(
                repartitioned.table_id, rebuilt.table_id,
                "a partition_by change must rebuild the acceleration"
            );

            // An unchanged restart reuses what is there.
            let ids = fixture.ids_after_restart("trips", &partitioned).await?;
            assert_eq!(ids, vec![1, 2, 3]);
            assert_eq!(
                fixture.stored("trips").await?.table_id,
                repartitioned.table_id,
                "an unchanged restart must not rebuild the acceleration"
            );
            Ok(())
        })
        .await
}

/// Under `refresh_mode: append` the acceleration can hold rows the source no longer
/// has, so a primary key change keeps the stored settings — and every stored row —
/// instead of rebuilding, while a layout change still applies in place.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn append_acceleration_keeps_stored_identity_settings_on_restart() -> Result<(), anyhow::Error>
{
    let _tracing = crate::init_tracing(Some("integration=debug,info"));
    test_request_context()
        .scope(async {
            let fixture = Fixture::new()?;
            let append = Settings {
                refresh_mode: Some(RefreshMode::Append),
                ..Settings::default()
            };

            let ids = fixture.ids_after_restart("events", &append).await?;
            assert_eq!(ids, vec![1, 1, 2, 2, 3]);
            let built = fixture.stored("events").await?;

            let keyed_and_sorted = Settings {
                primary_key: Some("id"),
                sort_columns: Some("id"),
                ..append.clone()
            };
            let ids = fixture
                .ids_after_restart("events", &keyed_and_sorted)
                .await?;
            assert_eq!(
                ids,
                vec![1, 1, 2, 2, 3],
                "no stored row may be lost or deduplicated"
            );
            let stored = fixture.stored("events").await?;
            assert_eq!(stored.table_id, built.table_id);
            assert!(
                stored.primary_key.is_empty(),
                "an append acceleration keeps the primary key it was built with"
            );
            assert!(
                stored.vortex_config.sort_columns.is_empty(),
                "a kept acceleration persists none of the requested settings"
            );

            // The layout change on its own applies in place.
            let sorted = Settings {
                sort_columns: Some("id"),
                ..append.clone()
            };
            let ids = fixture.ids_after_restart("events", &sorted).await?;
            assert_eq!(ids, vec![1, 1, 2, 2, 3]);
            let stored = fixture.stored("events").await?;
            assert_eq!(stored.table_id, built.table_id);
            assert_eq!(stored.vortex_config.sort_columns, vec!["id".to_string()]);
            Ok(())
        })
        .await
}

/// A dataset over a source other than a local file registers against its existing
/// acceleration before the source is contacted. A changed primary key must still rebuild
/// that acceleration from the source — whether the source answers at once or only later —
/// rather than keep retrying the registration or serving rows built under the old key.
///
/// The source answers `SUM(v)` over three rows; changing its value between restarts tells
/// a rebuilt acceleration (the new value) from a reused one (the old value). Nothing else
/// reloads it: the dataset has no `refresh_check_interval`, so a reused acceleration is
/// not refreshed at startup.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_changed_primary_key_rebuilds_an_acceleration_served_before_its_source_is_reached()
-> Result<(), anyhow::Error> {
    use crate::source_unavailable::{UnreachableSource, dataset_status};
    use crate::utils::{run_query, wait_until_true};
    use runtime::status::ComponentStatus;
    use std::time::Duration;

    let _tracing = crate::init_tracing(Some("integration=debug,info"));
    let dir = tempfile::tempdir()?;
    let metadata_dir = dir.path().join("metadata");
    let source = UnreachableSource::new("settings-rebuild", 1);
    source.register().await;
    let dataset = |primary_key: Option<&str>| {
        let mut dataset = Dataset::new(format!("{}://orders", source.prefix), "orders");
        dataset.acceleration = Some(Acceleration {
            enabled: true,
            engine: Some("cayenne".to_string()),
            mode: Mode::File,
            refresh_mode: Some(RefreshMode::Full),
            primary_key: primary_key.map(ToString::to_string),
            params: Some(Params::from_string_map(HashMap::from([
                (
                    "cayenne_file_path".to_string(),
                    dir.path().join("data").to_string_lossy().to_string(),
                ),
                (
                    "cayenne_metadata_dir".to_string(),
                    metadata_dir.to_string_lossy().to_string(),
                ),
            ]))),
            ..Acceleration::default()
        });
        dataset
    };
    let start = |dataset: Dataset| async move {
        let app = AppBuilder::new("settings_change_cayenne")
            .with_dataset(dataset)
            .build();
        crate::configure_test_datafusion();
        let rt = Arc::new(Runtime::builder().with_app(app).build().await);
        let loader = tokio::spawn({
            let rt = Arc::clone(&rt);
            async move { rt.load_components().await }
        });
        (rt, loader)
    };
    let sum_and_count = |rt: Arc<Runtime>| async move {
        let batches = run_query(&rt, "SELECT CAST(SUM(v) AS BIGINT), COUNT(*) FROM orders")
            .await
            .ok()?;
        let batch = batches.first()?;
        let value = |column: usize| {
            batch
                .column(column)
                .as_any()
                .downcast_ref::<Int64Array>()
                .map(|values| values.value(0))
        };
        Some((value(0)?, value(1)?))
    };
    let loads = |rt: Arc<Runtime>, expected: (i64, i64)| async move {
        wait_until_true(Duration::from_secs(60), || {
            let rt = Arc::clone(&rt);
            async move {
                dataset_status(&rt, "orders") == Some(ComponentStatus::Ready)
                    && sum_and_count(Arc::clone(&rt)).await == Some(expected)
            }
        })
        .await
    };
    let stored = || async {
        let catalog = CayenneCatalog::new(format!(
            "sqlite://{}",
            metadata_dir.join("cayenne.db").display()
        ))?;
        catalog.init().await?;
        Ok::<_, anyhow::Error>(catalog.get_table("orders").await?)
    };

    // Built with no primary key from the source's first value.
    source.bring_up();
    let (rt, loader) = start(dataset(None)).await;
    let built = loads(Arc::clone(&rt), (3, 3)).await;
    let observed = sum_and_count(Arc::clone(&rt)).await;
    rt.shutdown().await;
    loader.abort();
    anyhow::ensure!(built, "building the acceleration, got {observed:?}");
    let first = stored().await?;
    assert!(first.primary_key.is_empty());

    // The source changes, and the dataset restarts with a primary key while the source
    // answers: the acceleration is rebuilt and loads the source's new value.
    source.set_value(2);
    let (rt, loader) = start(dataset(Some("id"))).await;
    let rebuilt = loads(Arc::clone(&rt), (6, 3)).await;
    let observed = sum_and_count(Arc::clone(&rt)).await;
    let status = dataset_status(&rt, "orders");
    rt.shutdown().await;
    loader.abort();
    assert!(
        rebuilt,
        "a changed primary key must rebuild the acceleration from the source, got {observed:?} ({status:?})"
    );
    let second = stored().await?;
    assert_ne!(second.table_id, first.table_id);
    assert_eq!(second.primary_key, vec!["id".to_string()]);

    // The key changes again while the source is down: the acceleration is rebuilt again
    // and loads once the source answers, never serving rows built under the old key.
    // Keyed on `v`, which all three rows share, the reload holds one row.
    source.set_value(3);
    source.take_down();
    let (rt, loader) = start(dataset(Some("v"))).await;
    let served_early = wait_until_true(Duration::from_secs(5), || {
        let rt = Arc::clone(&rt);
        async move { sum_and_count(rt).await.is_some() }
    })
    .await;
    source.bring_up();
    let reloaded = loads(Arc::clone(&rt), (3, 1)).await;
    let observed = sum_and_count(Arc::clone(&rt)).await;
    rt.shutdown().await;
    loader.abort();
    assert!(
        !served_early,
        "a rebuilt acceleration must not answer before it is reloaded"
    );
    assert!(
        reloaded,
        "the rebuilt acceleration must load once the source answers, got {observed:?}"
    );
    let third = stored().await?;
    assert_ne!(third.table_id, second.table_id);
    assert_eq!(third.primary_key, vec!["v".to_string()]);
    Ok(())
}
