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

use std::{collections::HashMap, path::Path, sync::Arc, time::Duration};

use app::AppBuilder;
use arrow::array::{Int64Array, RecordBatch};
use cayenne::{CayenneCatalog, MetadataCatalog, metadata::TableMetadata};
use futures::TryStreamExt;
use runtime::{
    Runtime, component::dataset::builder::DatasetBuilder,
    dataaccelerator::spice_sys::dataset_checkpointer,
};
use runtime_acceleration::{sidecar::OpenOption, snapshot::SnapshotBehavior};
use spicepod::{
    acceleration::{Acceleration, Mode, RefreshMode, WriteMode},
    component::{
        access::AccessMode,
        dataset::{Dataset, TimeFormat},
    },
    param::Params,
    partitioning::PartitionedBy,
};

use crate::utils::{runtime_ready_check, test_request_context, wait_until_true};

/// Five rows over three ids: ids 1 and 2 appear twice, so the row count shows whether
/// the acceleration keeps one row per primary key. `ts` (Unix seconds) is the append
/// dataset's time column, so its restarts find nothing new to append.
const CSV: &str =
    "id,name,ts\n1,a,1704067200\n2,b,1704153600\n1,c,1704240000\n3,d,1704326400\n2,e,1704412800\n";

/// [`CSV`] with a column added: a source schema change, which the default
/// `on_schema_change: block` holds back.
const CSV_WITH_COLUMN: &str = "id,name,ts,note\n1,a,1704067200,x\n2,b,1704153600,x\n1,c,1704240000,x\n3,d,1704326400,x\n2,e,1704412800,x\n";

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

    /// Replaces the source file's contents.
    fn write_source(&self, contents: &str) -> Result<(), anyhow::Error> {
        std::fs::write(&self.csv, contents)?;
        Ok(())
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
        self.ids_after_restart_with(self.dataset(name, settings))
            .await
    }

    /// [`Self::ids_after_restart`] for a dataset built by the caller.
    async fn ids_after_restart_with(&self, dataset: Dataset) -> Result<Vec<i64>, anyhow::Error> {
        let rt = start(dataset.clone()).await?;
        let ids = ids(&rt, &dataset.name).await?;
        wait_for_checkpoint(&rt, dataset).await?;
        rt.shutdown().await;
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

/// Starts a runtime serving `dataset` and waits for it to be ready.
async fn start(dataset: Dataset) -> Result<Arc<Runtime>, anyhow::Error> {
    let name = dataset.name.clone();
    let app = AppBuilder::new("settings_change_cayenne")
        .with_dataset(dataset)
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
    Ok(rt)
}

/// Runs `sql` to completion and returns its batches; a write only happens once its
/// stream is drained.
async fn run(rt: &Arc<Runtime>, sql: &str) -> Result<Vec<RecordBatch>, anyhow::Error> {
    Ok(rt
        .datafusion()
        .query_builder(sql)
        .build()
        .run()
        .await
        .map_err(|e| anyhow::anyhow!("`{sql}` failed: {e}"))?
        .data
        .try_collect()
        .await?)
}

/// The ids of `name`'s rows, in order.
async fn ids(rt: &Arc<Runtime>, name: &str) -> Result<Vec<i64>, anyhow::Error> {
    let mut ids = Vec::new();
    for batch in &run(rt, &format!("SELECT id FROM {name} ORDER BY id")).await? {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| anyhow::anyhow!("`id` is not Int64: {:?}", batch.schema()))?;
        ids.extend(column.iter().flatten());
    }
    Ok(ids)
}

/// Waits for the checkpoint the runtime writes once `dataset` has loaded, after it is
/// ready. The next restart compares the source's schema with the one it records, and
/// serves the existing acceleration before the source is reached only when it exists.
async fn wait_for_checkpoint(rt: &Arc<Runtime>, dataset: Dataset) -> Result<(), anyhow::Error> {
    let app = rt
        .app()
        .read()
        .await
        .clone()
        .ok_or_else(|| anyhow::anyhow!("the runtime has no app"))?;
    let dataset = DatasetBuilder::try_from(dataset)?
        .with_app(app)
        .with_runtime(Arc::clone(rt))
        .build()?;
    let checkpoint = dataset_checkpointer(
        &dataset,
        rt.accelerator_engine_registry(),
        OpenOption::OpenExisting,
        SnapshotBehavior::Disabled,
    )
    .await
    .map_err(|e| {
        anyhow::anyhow!(
            "Failed to open the checkpoint of dataset '{}': {e}",
            dataset.name
        )
    })?;
    anyhow::ensure!(
        wait_until_true(Duration::from_secs(30), || async {
            checkpoint.exists().await
        })
        .await,
        "dataset '{}' wrote no checkpoint within 30 s",
        dataset.name
    );
    Ok(())
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

/// Writes to a `write_mode: acceleration` dataset stay in the acceleration and never reach
/// the source, so a reload would not bring them back: a primary-key change keeps the stored
/// settings, and the written rows, even under `refresh_mode: full`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn accelerator_only_writes_keep_their_acceleration_on_a_key_change()
-> Result<(), anyhow::Error> {
    let _tracing = crate::init_tracing(Some("integration=debug,info"));
    test_request_context()
        .scope(async {
            let fixture = Fixture::new()?;
            let dataset = |primary_key: Option<&'static str>| {
                let mut dataset = fixture.dataset(
                    "trips",
                    &Settings {
                        refresh_mode: Some(RefreshMode::Full),
                        primary_key,
                        ..Settings::default()
                    },
                );
                dataset.access = AccessMode::ReadWrite;
                if let Some(acceleration) = dataset.acceleration.as_mut() {
                    acceleration.write_mode = WriteMode::Acceleration;
                }
                dataset
            };

            // Built from the source, then given a row only the acceleration holds.
            let rt = start(dataset(None)).await?;
            run(
                &rt,
                "INSERT INTO trips (id, name, ts) VALUES (9, 'z', 1704499200)",
            )
            .await?;
            let written = ids(&rt, "trips").await?;
            let checkpointed = wait_for_checkpoint(&rt, dataset(None)).await;
            rt.shutdown().await;
            assert_eq!(written, vec![1, 1, 2, 2, 3, 9]);
            checkpointed?;
            let built = fixture.stored("trips").await?;

            // A primary key would rebuild a write-through dataset; this one keeps its rows.
            let ids = fixture.ids_after_restart_with(dataset(Some("id"))).await?;
            assert_eq!(
                ids,
                vec![1, 1, 2, 2, 3, 9],
                "a row written only to the acceleration must survive the restart"
            );
            let kept = fixture.stored("trips").await?;
            assert_eq!(
                kept.table_id, built.table_id,
                "an accelerator-only dataset must not be rebuilt"
            );
            assert!(
                kept.primary_key.is_empty(),
                "an accelerator-only dataset keeps the primary key it was built with"
            );
            Ok(())
        })
        .await
}

/// Under the default `on_schema_change: block`, a source whose schema changed keeps the
/// dataset serving its existing acceleration on the schema it was built with. A settings
/// change in the same restart must not rebuild that acceleration — the rebuilt table could
/// not reload while the schema change is held — so the stored settings, and every row, are
/// kept until the schema change is resolved.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_changed_primary_key_is_held_while_a_source_schema_change_is_blocked()
-> Result<(), anyhow::Error> {
    let _tracing = crate::init_tracing(Some("integration=debug,info"));
    test_request_context()
        .scope(async {
            let fixture = Fixture::new()?;
            let full = Settings {
                refresh_mode: Some(RefreshMode::Full),
                ..Settings::default()
            };
            let ids = fixture.ids_after_restart("trips", &full).await?;
            assert_eq!(ids, vec![1, 1, 2, 2, 3]);
            let built = fixture.stored("trips").await?;

            // The source gains a column, which `block` holds back, in the same restart that
            // adds a primary key.
            fixture.write_source(CSV_WITH_COLUMN)?;
            let keyed = Settings {
                primary_key: Some("id"),
                ..full.clone()
            };
            let ids = fixture.ids_after_restart("trips", &keyed).await?;
            assert_eq!(
                ids,
                vec![1, 1, 2, 2, 3],
                "the existing acceleration must keep serving every row while the schema change is held"
            );
            let held = fixture.stored("trips").await?;
            assert_eq!(
                held.table_id, built.table_id,
                "a settings change must not rebuild an acceleration whose source schema change is held"
            );
            assert!(
                held.primary_key.is_empty(),
                "a held acceleration keeps the primary key it was built with"
            );

            // With the schema change reverted, the next restart rebuilds with the key.
            fixture.write_source(CSV)?;
            let ids = fixture.ids_after_restart("trips", &keyed).await?;
            assert_eq!(ids, vec![1, 2, 3]);
            let rebuilt = fixture.stored("trips").await?;
            assert_ne!(
                rebuilt.table_id, built.table_id,
                "a readable source must let the held rebuild happen"
            );
            assert_eq!(rebuilt.primary_key, vec!["id".to_string()]);
            Ok(())
        })
        .await
}

/// A dataset over a source other than a local file registers against its existing
/// acceleration before the source is contacted. A changed primary key must still rebuild
/// that acceleration from the source once the source answers, rather than keep retrying the
/// registration — and must keep the existing acceleration serving while the source is down,
/// since a rebuilt one could not reload.
///
/// The source answers `SUM(v)` over three rows; changing its value between restarts tells
/// a rebuilt acceleration (the new value) from a reused one (the old value). Nothing else
/// reloads it: the dataset has no `refresh_check_interval`, so a reused acceleration is
/// not refreshed at startup.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_changed_primary_key_rebuilds_an_acceleration_served_before_its_source_is_reached()
-> Result<(), anyhow::Error> {
    use crate::source_unavailable::{UnreachableSource, dataset_status};
    use crate::utils::run_query;
    use runtime::status::ComponentStatus;

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
    let partitioned = |primary_key: Option<&str>, expression: &str| {
        let mut dataset = dataset(primary_key);
        if let Some(acceleration) = dataset.acceleration.as_mut() {
            acceleration.partition_by = vec![PartitionedBy {
                name: "expr0".to_string(),
                expression: expression.to_string(),
            }];
        }
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
    let checkpointed = wait_for_checkpoint(&rt, dataset(None)).await;
    rt.shutdown().await;
    loader.abort();
    anyhow::ensure!(built, "building the acceleration, got {observed:?}");
    checkpointed?;
    let first = stored().await?;
    assert!(first.primary_key.is_empty());

    // The source changes, and the dataset restarts with a primary key while the source
    // answers: the acceleration is rebuilt and loads the source's new value.
    source.set_value(2);
    let (rt, loader) = start(dataset(Some("id"))).await;
    let rebuilt = loads(Arc::clone(&rt), (6, 3)).await;
    let observed = sum_and_count(Arc::clone(&rt)).await;
    let status = dataset_status(&rt, "orders");
    let checkpointed = wait_for_checkpoint(&rt, dataset(Some("id"))).await;
    rt.shutdown().await;
    loader.abort();
    assert!(
        rebuilt,
        "a changed primary key must rebuild the acceleration from the source, got {observed:?} ({status:?})"
    );
    checkpointed?;
    let second = stored().await?;
    assert_ne!(second.table_id, first.table_id);
    assert_eq!(second.primary_key, vec!["id".to_string()]);

    // The key changes again while the source is down. A rebuilt acceleration could not
    // reload, so the existing one keeps serving with the settings it was built with.
    source.set_value(3);
    source.take_down();
    let (rt, loader) = start(dataset(Some("v"))).await;
    let kept = loads(Arc::clone(&rt), (6, 3)).await;
    let observed = sum_and_count(Arc::clone(&rt)).await;
    rt.shutdown().await;
    loader.abort();
    assert!(
        kept,
        "with its source down the existing acceleration must keep serving, got {observed:?}"
    );
    let third = stored().await?;
    assert_eq!(third.table_id, second.table_id);
    assert_eq!(third.primary_key, vec!["id".to_string()]);

    // A partitioning change cannot be held that way: the stored rows cannot be served
    // under another partitioning. The dataset fails to load, saying how to keep the
    // existing data or apply the change, and the existing acceleration is kept for either.
    // `ComponentStatus` equality ignores the message, so the message is compared itself.
    let held_partitioning = "Failed to setup the dataset orders (settings-rebuild). Unable to create dataset acceleration: Acceleration creation failed: Failed to load dataset orders (cayenne): its acceleration data was written without 'partition_by', but the dataset now sets 'partition_by: [bucket(2, id)]'. Applying it rebuilds the acceleration from the source, which cannot be read on the acceleration's schema yet. To keep using the existing data, remove 'partition_by'. To apply the new 'partition_by', restart once the source is reachable and any change to its schema is resolved (revert it, or set 'on_schema_change' to adopt it). See: https://spiceai.org/docs/features/data-acceleration/partitioning";
    let (rt, loader) = start(partitioned(Some("id"), "bucket(2, id)")).await;
    let refused = wait_until_true(Duration::from_secs(60), || {
        let rt = Arc::clone(&rt);
        async move {
            matches!(
                dataset_status(&rt, "orders"),
                Some(ComponentStatus::Error(Some(message))) if message == held_partitioning
            )
        }
    })
    .await;
    let status = dataset_status(&rt, "orders");
    rt.shutdown().await;
    loader.abort();
    assert!(
        refused,
        "a held partitioning change must fail the dataset with the held message, got {status:?}"
    );
    assert_eq!(stored().await?.table_id, third.table_id);

    // Once the source answers, the next restart rebuilds with the new key. Keyed on `v`,
    // which all three rows share, the reload holds one row.
    source.bring_up();
    let (rt, loader) = start(dataset(Some("v"))).await;
    let rebuilt = loads(Arc::clone(&rt), (3, 1)).await;
    let observed = sum_and_count(Arc::clone(&rt)).await;
    rt.shutdown().await;
    loader.abort();
    assert!(
        rebuilt,
        "a reachable source must let the held rebuild happen, got {observed:?}"
    );
    let fourth = stored().await?;
    assert_ne!(fourth.table_id, third.table_id);
    assert_eq!(fourth.primary_key, vec!["v".to_string()]);
    Ok(())
}

/// [`a_changed_primary_key_is_held_while_a_source_schema_change_is_blocked`] for a dataset
/// that registers against its existing acceleration before its source is reached. Its
/// source answers, but on a changed schema, so it never resolves on the schema the dataset
/// registered with: once the wait for it runs out, the stored settings, and the rows, are
/// kept.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_changed_primary_key_is_held_while_a_source_reached_after_registration_has_a_new_schema()
-> Result<(), anyhow::Error> {
    use crate::source_unavailable::{UnreachableSource, dataset_status};
    use crate::utils::run_query;
    use runtime::status::ComponentStatus;

    let _tracing = crate::init_tracing(Some("integration=debug,info"));
    let dir = tempfile::tempdir()?;
    let metadata_dir = dir.path().join("metadata");
    let source = UnreachableSource::new("settings-held", 1);
    source.register().await;
    source.bring_up();
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
    let serves = |rt: Arc<Runtime>, expected: (i64, i64)| async move {
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

    let (rt, loader) = start(dataset(None)).await;
    let built = serves(Arc::clone(&rt), (3, 3)).await;
    let checkpointed = wait_for_checkpoint(&rt, dataset(None)).await;
    rt.shutdown().await;
    loader.abort();
    anyhow::ensure!(built, "building the acceleration");
    checkpointed?;
    let first = stored().await?;

    // The source gains a column and new values, and the dataset restarts with a primary
    // key: the schema change is held, so the existing acceleration keeps serving.
    source.add_column();
    source.set_value(2);
    let (rt, loader) = start(dataset(Some("id"))).await;
    let kept = serves(Arc::clone(&rt), (3, 3)).await;
    let observed = sum_and_count(Arc::clone(&rt)).await;
    let status = dataset_status(&rt, "orders");
    rt.shutdown().await;
    loader.abort();
    assert!(
        kept,
        "the existing acceleration must keep serving while the schema change is held, got {observed:?} ({status:?})"
    );
    let second = stored().await?;
    assert_eq!(second.table_id, first.table_id);
    assert!(
        second.primary_key.is_empty(),
        "the stored settings are kept while the schema change is held"
    );
    Ok(())
}
