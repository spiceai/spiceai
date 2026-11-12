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

//! What compaction leaves behind is reclaimed on an object store as on a local
//! directory, with no further write to prompt it.
//!
//! A protected-snapshot merge retires its input snapshot directories and lowers
//! the surviving-sequence floor, which orphans key deletion vectors. Once the
//! cleanup grace has passed, the table's storage must hold exactly the files its
//! catalog still names, the scan must return the same rows, and
//! `cayenne_maintenance_reclaimed_{files,bytes}_total` must count what was
//! deleted. The table takes no write after the merge, so only maintenance the
//! table schedules for itself can do the reclaiming.

#![expect(clippy::expect_used, reason = "test helpers")]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::common::{self, TestFixture};
use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use cayenne::metadata::{CreateTableOptions, DeletionMode, ObjectStoreConfig, VortexConfig};
use cayenne::{CayenneContext, CayenneTableProvider, CayenneTableProviderBuilder, MetadataCatalog};
use datafusion::datasource::TableProvider;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::prelude::SessionContext;
use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};
use futures::TryStreamExt;
use object_store::ObjectStore;
use object_store::memory::InMemory;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

const TABLE: &str = "reclaimed";

/// Upserts of one key: each after the first writes a key deletion vector and a
/// protected snapshot of its own. Above the orphan sweep's 20-file threshold.
const UPSERTS: usize = 26;

/// No automatic compaction trigger fires, so the test runs the merge itself.
const NO_AUTOMATIC_COMPACTION: usize = 1_000_000;

/// How long reclamation may take: a few cleanup graces (5 s each).
const RECLAIM_DEADLINE: Duration = Duration::from_secs(60);

/// Where the table keeps its files.
enum Storage {
    Local(std::path::PathBuf),
    Store {
        store: Arc<dyn ObjectStore>,
        url: url::Url,
    },
}

impl Storage {
    fn base_path(&self) -> String {
        match self {
            Self::Local(dir) => dir.to_string_lossy().into_owned(),
            Self::Store { url, .. } => url.to_string(),
        }
    }

    fn builder(
        &self,
        catalog: Arc<dyn MetadataCatalog>,
        runtime_env: Arc<RuntimeEnv>,
    ) -> CayenneTableProviderBuilder {
        match self {
            Self::Local(_) => CayenneTableProviderBuilder::new(catalog, runtime_env),
            Self::Store { store, url } => common::table_builder_on_store(
                catalog,
                runtime_env,
                ObjectStoreConfig {
                    url: url.clone(),
                    store: Arc::clone(store),
                },
            ),
        }
    }

    /// The fixture backend a table on this storage pairs with: SQLite for a local
    /// directory, object-native for an object store.
    fn backend(&self) -> common::BackendType {
        match self {
            Self::Local(_) => common::BackendType::Sqlite,
            Self::Store { .. } => common::BackendType::ObjectNative,
        }
    }

    /// Every file under the table's data path, by path relative to it, with its size.
    async fn files(&self) -> TestResult<BTreeMap<String, u64>> {
        match self {
            Self::Local(dir) => {
                let root = dir.clone();
                let mut files = BTreeMap::new();
                let mut pending = vec![root.clone()];
                while let Some(next) = pending.pop() {
                    let Ok(entries) = std::fs::read_dir(&next) else {
                        continue;
                    };
                    for entry in entries {
                        let entry = entry?;
                        let meta = entry.metadata()?;
                        if meta.is_dir() {
                            pending.push(entry.path());
                        } else {
                            let relative = entry
                                .path()
                                .strip_prefix(&root)?
                                .to_string_lossy()
                                .into_owned();
                            files.insert(relative, meta.len());
                        }
                    }
                }
                Ok(files)
            }
            Self::Store { store, url } => {
                let root = format!("{}/", url.path().trim_matches('/'));
                let listed: Vec<_> = store.list(None).try_collect().await?;
                Ok(listed
                    .into_iter()
                    .filter_map(|meta| {
                        meta.location
                            .as_ref()
                            .strip_prefix(&root)
                            .map(|relative| (relative.to_string(), meta.size))
                    })
                    .collect())
            }
        }
    }
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

/// Key deletes, every write a data file, and every automatic trigger parked.
fn parked_config() -> VortexConfig {
    VortexConfig {
        inline_max_rows: 0,
        deletion_mode: DeletionMode::Key,
        compaction_trigger_files: NO_AUTOMATIC_COMPACTION,
        compaction_trigger_protected_snapshots: NO_AUTOMATIC_COMPACTION,
        compaction_trigger_snapshot_age_ms: 0,
        compaction_background_interval_ms: 0,
        ..VortexConfig::default()
    }
}

/// Install a meter provider this process reads `cayenne_maintenance_*` from. Before
/// any table is built: Cayenne binds its instruments on first use.
fn install_metrics() -> telemetry::metrics_reader::MetricsReader {
    let reader = telemetry::metrics_reader::MetricsReader::new_cumulative();
    opentelemetry::global::set_meter_provider(
        opentelemetry_sdk::metrics::SdkMeterProvider::builder()
            .with_reader(reader.clone())
            .build(),
    );
    reader
}

/// The cumulative value of the counter `name` for this table and `op`.
fn counter(reader: &telemetry::metrics_reader::MetricsReader, name: &str, op: &str) -> u64 {
    use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData, ResourceMetrics};
    use opentelemetry_sdk::metrics::reader::MetricReader as _;
    let mut resource_metrics = ResourceMetrics::default();
    reader
        .collect(&mut resource_metrics)
        .expect("collect metrics");
    let mut total = 0;
    for metric in resource_metrics
        .scope_metrics()
        .flat_map(opentelemetry_sdk::metrics::data::ScopeMetrics::metrics)
    {
        if metric.name() != name {
            continue;
        }
        if let AggregatedMetrics::U64(MetricData::Sum(sum)) = metric.data() {
            for point in sum.data_points() {
                let label = |key: &str| {
                    point
                        .attributes()
                        .find(|attribute| attribute.key.as_str() == key)
                        .map(|attribute| attribute.value.as_str().to_string())
                        .unwrap_or_default()
                };
                if label("table") == TABLE && label("op") == op {
                    total += point.value();
                }
            }
        }
    }
    total
}

async fn upsert(table: &CayenneTableProvider, id: i64, value: i64) -> TestResult {
    let batch = RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(vec![id])),
            Arc::new(Int64Array::from(vec![value])),
        ],
    )?;
    common::insert_batch(table, batch).await?;
    Ok(())
}

async fn rows(table: &Arc<CayenneTableProvider>) -> TestResult<Vec<(i64, i64)>> {
    let ctx = SessionContext::new();
    ctx.register_table(TABLE, Arc::clone(table) as Arc<dyn TableProvider>)?;
    let batches = ctx
        .sql(&format!("SELECT id, value FROM {TABLE} ORDER BY id"))
        .await?
        .collect()
        .await?;
    let mut out = Vec::new();
    for batch in batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or("id is not Int64")?;
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or("value is not Int64")?;
        out.extend((0..batch.num_rows()).map(|i| (ids.value(i), values.value(i))));
    }
    Ok(out)
}

/// Every file the catalog names for the table, relative to its data path: the data
/// files of its snapshots (`<table id>/<snapshot id>/…`) and its deletion vectors
/// (`<snapshot id>/deletions/…`).
async fn named_by_catalog(
    catalog: &Arc<dyn MetadataCatalog>,
    table: &CayenneTableProvider,
    storage: &Storage,
) -> TestResult<BTreeSet<String>> {
    let table_id = table.table_id().to_string();
    let mut named: BTreeSet<String> = catalog
        .get_all_snapshot_files(&table_id)
        .await?
        .into_iter()
        .map(|file| format!("{table_id}/{}/{}", file.snapshot_id, file.file_path))
        .collect();
    let root = match storage {
        Storage::Local(dir) => format!("{}/", dir.to_string_lossy()),
        Storage::Store { url, .. } => format!("{}/", url.as_str().trim_end_matches('/')),
    };
    for delete_file in catalog.get_table_delete_files(&table_id).await? {
        let relative = delete_file
            .path
            .strip_prefix(&root)
            .ok_or_else(|| format!("deletion vector '{}' is outside '{root}'", delete_file.path))?;
        named.insert(relative.to_string());
    }
    Ok(named)
}

/// Data files (`.vortex`) and deletion vectors (`.arrow`) among `files`.
fn reclaimable(files: &BTreeMap<String, u64>) -> BTreeMap<String, u64> {
    files
        .iter()
        .filter(|(path, _)| {
            std::path::Path::new(path.as_str())
                .extension()
                .is_some_and(|extension| extension == "vortex" || extension == "arrow")
        })
        .map(|(path, size)| (path.clone(), *size))
        .collect()
}

async fn compaction_leftovers_are_reclaimed(storage: Storage) -> TestResult {
    let reader = install_metrics();
    let fixture = TestFixture::new(storage.backend()).await?;
    let catalog: Arc<dyn MetadataCatalog> = Arc::clone(&fixture.catalog);
    let ctx = SessionContext::new();
    let table = Arc::new(
        storage
            .builder(Arc::clone(&catalog), ctx.runtime_env())
            .create(CreateTableOptions {
                table_name: TABLE.to_string(),
                schema: schema(),
                primary_key: vec!["id".to_string()],
                on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
                    "id".to_string(),
                ]))),
                base_path: storage.base_path(),
                partition_column: None,
                vortex_config: parked_config(),
            })
            .await?,
    );
    for value in 0..UPSERTS {
        upsert(&table, 1, i64::try_from(value)?).await?;
        common::poll_inlined_data_count_zero(&fixture.catalog, table.table_id()).await?;
    }
    table.flush_pending_maintenance().await?;
    table.drain_in_flight_maintenance().await?;
    drop(table);

    // The merge needs its tier threshold armed; a reopen takes no writes, so no
    // automatic pass runs besides the one asked for.
    let config = VortexConfig {
        compaction_trigger_protected_snapshots: VortexConfig::default()
            .compaction_trigger_protected_snapshots,
        ..parked_config()
    };
    let table = Arc::new(
        storage
            .builder(Arc::clone(&catalog), ctx.runtime_env())
            .with_context(CayenneContext::new(&config, ctx.runtime_env(), TABLE))
            .open(TABLE)
            .await?,
    );
    table.drain_in_flight_maintenance().await?;
    let before = reclaimable(&storage.files().await?);
    assert_eq!(
        before.keys().cloned().collect::<BTreeSet<_>>(),
        named_by_catalog(&catalog, &table, &storage).await?,
        "before the merge every data file and deletion vector is named by the catalog"
    );

    assert!(
        table.compact_protected_snapshots_subset(usize::MAX).await?,
        "the protected-snapshot merge commits"
    );
    table.drain_in_flight_maintenance().await?;
    let expected_rows = vec![(1, i64::try_from(UPSERTS - 1)?)];
    assert_eq!(rows(&table).await?, expected_rows);

    let after_merge = reclaimable(&storage.files().await?);
    let named = named_by_catalog(&catalog, &table, &storage).await?;
    assert!(
        after_merge.keys().any(|path| !named.contains(path)),
        "the merge leaves files no snapshot names (its inputs), so there is something to reclaim: {after_merge:?}"
    );
    assert!(
        named.iter().all(|path| after_merge.contains_key(path)),
        "every file the catalog names is in storage after the merge"
    );

    // No write from here on: only maintenance the table schedules for itself runs.
    let deadline = Instant::now() + RECLAIM_DEADLINE;
    let reclaimed_files = loop {
        let files = reclaimable(&storage.files().await?);
        let named = named_by_catalog(&catalog, &table, &storage).await?;
        let missing: Vec<_> = named
            .iter()
            .filter(|path| !files.contains_key(*path))
            .collect();
        assert!(
            missing.is_empty(),
            "files the catalog names were deleted: {missing:?}"
        );
        let leftover: Vec<_> = files.keys().filter(|path| !named.contains(*path)).collect();
        if leftover.is_empty() {
            break files;
        }
        assert!(
            Instant::now() < deadline,
            "{} file(s) no snapshot names were still in storage {}s after the merge, with no further write: {leftover:?}",
            leftover.len(),
            RECLAIM_DEADLINE.as_secs()
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    };
    assert_eq!(rows(&table).await?, expected_rows);

    // Against the listing before the merge: the merge's own orphan sweep has
    // already run by the time `after_merge` is taken.
    let removed: BTreeMap<_, _> = before
        .iter()
        .filter(|(path, _)| !reclaimed_files.contains_key(*path))
        .map(|(path, size)| (path.clone(), *size))
        .collect();
    let has_extension = |path: &str, wanted: &str| {
        std::path::Path::new(path)
            .extension()
            .is_some_and(|extension| extension == wanted)
    };
    let removed_dvs = removed
        .keys()
        .filter(|path| has_extension(path, "arrow"))
        .count();
    let removed_data: Vec<_> = removed
        .iter()
        .filter(|(path, _)| has_extension(path, "vortex"))
        .collect();
    assert!(
        removed_dvs > 0 && !removed_data.is_empty(),
        "both orphaned deletion vectors and merged-away data files were reclaimed: {removed:?}"
    );
    assert_eq!(
        counter(
            &reader,
            "cayenne_maintenance_reclaimed_files_total",
            "retired_dir_sweep"
        ),
        u64::try_from(removed_data.len())?,
        "the retired-directory sweep counts the data files it deleted"
    );
    assert_eq!(
        counter(
            &reader,
            "cayenne_maintenance_reclaimed_bytes_total",
            "retired_dir_sweep"
        ),
        removed_data.iter().map(|(_, size)| **size).sum::<u64>(),
        "the retired-directory sweep counts the bytes it deleted"
    );
    assert_eq!(
        counter(
            &reader,
            "cayenne_maintenance_reclaimed_files_total",
            "orphan_dv_sweep"
        ),
        u64::try_from(removed_dvs)?,
        "the orphaned deletion-vector sweep counts the vectors it deleted"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "asserts the SQLite metastore's reclamation (the catalog stops naming merge inputs at the merge), but data files in an object store pair only with the object-native metastore, which names retired snapshots until it reclaims them"]
async fn compaction_leftovers_are_reclaimed_on_an_object_store() -> TestResult {
    common::require_process_per_test("it installs the process-wide meter provider");
    let url = url::Url::parse(&format!(
        "s3://cayenne-test/{}/data",
        uuid::Uuid::now_v7().simple()
    ))?;
    compaction_leftovers_are_reclaimed(Storage::Store {
        store: Arc::new(InMemory::new()),
        url,
    })
    .await
}

#[tokio::test(flavor = "multi_thread")]
async fn compaction_leftovers_are_reclaimed_on_a_local_directory() -> TestResult {
    common::require_process_per_test("it installs the process-wide meter provider");
    let dir = tempfile::TempDir::new()?;
    compaction_leftovers_are_reclaimed(Storage::Local(dir.path().to_path_buf())).await
}

/// How long the manifest may take to name a write's files once nothing holds
/// the compaction lock.
const MANIFEST_DEADLINE: Duration = Duration::from_secs(10);

/// Data files and deletion vectors in storage that the catalog does not name.
async fn unnamed_files(
    catalog: &Arc<dyn MetadataCatalog>,
    table: &CayenneTableProvider,
    storage: &Storage,
) -> TestResult<Vec<String>> {
    let named = named_by_catalog(catalog, table, storage).await?;
    Ok(reclaimable(&storage.files().await?)
        .into_keys()
        .filter(|path| !named.contains(path))
        .collect())
}

/// A table not placed in the object-native location names a write's files in
/// its manifest from the post-write maintenance pass, which skips that step
/// when another pass holds the compaction lock. The manifest must still name
/// the write's files once the lock is free, without another write to prompt it.
async fn a_skipped_manifest_rebuild_names_the_files_once_the_lock_is_free(
    storage: Storage,
) -> TestResult {
    let fixture = TestFixture::new(storage.backend()).await?;
    let catalog: Arc<dyn MetadataCatalog> = Arc::clone(&fixture.catalog);
    let ctx = SessionContext::new();
    let table = Arc::new(
        storage
            .builder(Arc::clone(&catalog), ctx.runtime_env())
            .create(CreateTableOptions {
                table_name: TABLE.to_string(),
                schema: schema(),
                primary_key: vec!["id".to_string()],
                on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
                    "id".to_string(),
                ]))),
                base_path: storage.base_path(),
                partition_column: None,
                vortex_config: parked_config(),
            })
            .await?,
    );
    for value in 0..3 {
        upsert(&table, 1, value).await?;
        common::poll_inlined_data_count_zero(&fixture.catalog, table.table_id()).await?;
    }
    table.drain_in_flight_maintenance().await?;
    assert_eq!(
        unnamed_files(&catalog, &table, &storage).await?,
        Vec::<String>::new(),
        "after the maintenance of the first writes, the catalog names every file"
    );

    // The write's maintenance pass runs while the lock is held, so its manifest
    // rebuild is skipped.
    let held = table.hold_compaction().await;
    upsert(&table, 1, 3).await?;
    common::poll_inlined_data_count_zero(&fixture.catalog, table.table_id()).await?;
    table.flush_pending_maintenance().await?;
    let unnamed = unnamed_files(&catalog, &table, &storage).await?;
    assert!(
        unnamed.iter().any(|path| {
            std::path::Path::new(path)
                .extension()
                .is_some_and(|extension| extension == "vortex")
        }),
        "while the lock is held the write's data file is not named yet: {unnamed:?}"
    );

    drop(held);
    let deadline = Instant::now() + MANIFEST_DEADLINE;
    loop {
        let unnamed = unnamed_files(&catalog, &table, &storage).await?;
        if unnamed.is_empty() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "{} file(s) still not named by the catalog {}s after the compaction lock was released, with no further write: {unnamed:?}",
            unnamed.len(),
            MANIFEST_DEADLINE.as_secs()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(rows(&table).await?, vec![(1, 3)]);
    table.drain_in_flight_maintenance().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_skipped_manifest_rebuild_names_the_files_once_the_lock_is_free_on_an_object_store()
-> TestResult {
    let url = url::Url::parse(&format!(
        "s3://cayenne-test/state/cayenne/v1/datasets/{}/data",
        uuid::Uuid::now_v7().simple()
    ))?;
    a_skipped_manifest_rebuild_names_the_files_once_the_lock_is_free(Storage::Store {
        store: Arc::new(InMemory::new()),
        url,
    })
    .await
}

#[tokio::test(flavor = "multi_thread")]
async fn a_skipped_manifest_rebuild_names_the_files_once_the_lock_is_free_on_a_local_directory()
-> TestResult {
    let dir = tempfile::TempDir::new()?;
    a_skipped_manifest_rebuild_names_the_files_once_the_lock_is_free(Storage::Local(
        dir.path().to_path_buf(),
    ))
    .await
}
