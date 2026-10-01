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

//! Snapshot compaction for Cayenne (`snapshots_compaction: enabled`).
//!
//! A CDC-fed table carries merge-on-read state: protected snapshots, deletion
//! files, inlined rows and an in-memory tier. Instead of archiving that
//! layout, compaction publishes the visible table as one snapshot with every
//! deletion applied, without modifying the live table:
//!
//! 1. [`CompactionCapture::capture`], under the accelerator write lock: build
//!    a scan of the live table (Cayenne captures the snapshot pointer,
//!    deletion view, protected set, inline and in-memory tiers once, under its
//!    listing fence) and export the dataset's `cayenne_table` row. No data is
//!    read yet.
//! 2. [`CompactionCapture::materialize`], after the lock is released: seed a
//!    scratch metastore with that row (same `table_id`, schema, primary key,
//!    `on_conflict` and sequence counter), open a provider on it and replay the
//!    captured scan as `INSERT OVERWRITE` — the full-refresh path, which
//!    yields one snapshot with no deletion files or protected snapshots. The
//!    scratch data directory and its metastore slice are archived.
//!
//! The captured plan pins the directories it reads (`SnapshotScanRef`), so
//! the live table's sweeps cannot remove them during the rewrite.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use cayenne::metadata::VortexConfig;
use cayenne::metastore::EXPECTED_TABLES;
use cayenne::metastore::snapshot::{
    DatasetMetastoreSlice, SLICE_ENGINE, SLICE_FORMAT_VERSION, SliceRow, SliceValue,
};
use cayenne::{CayenneCatalog, CayenneTableProviderBuilder, MetadataCatalog};
use datafusion::datasource::TableProvider;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::{ExecutionPlan, collect};
use datafusion::prelude::SessionContext;
use runtime_acceleration::snapshot::engine::{
    DirectoryArchiveExtra, MaterializedDirectorySnapshot,
};
use snafu::{OptionExt, ResultExt, Snafu, ensure};

const CAYENNE_TABLE: &str = "cayenne_table";

#[derive(Debug, Snafu)]
pub enum CompactionError {
    #[snafu(display(
        "Failed to read the live table of dataset '{dataset}' for snapshot compaction: {source}"
    ))]
    CaptureScan {
        dataset: String,
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display(
        "Failed to export the Cayenne metadata of dataset '{dataset}' for snapshot compaction: {source}"
    ))]
    ExportLive {
        dataset: String,
        source: cayenne::CatalogError,
    },

    #[snafu(display(
        "Cannot compact the snapshot of dataset '{dataset}': its Cayenne metadata has no '{CAYENNE_TABLE}' row"
    ))]
    MissingTableRow { dataset: String },

    #[snafu(display(
        "Cannot compact the snapshot of dataset '{dataset}': the Cayenne metadata layout has no column '{column}' in '{CAYENNE_TABLE}'"
    ))]
    MissingColumn {
        dataset: String,
        column: &'static str,
    },

    #[snafu(display(
        "Cannot compact the snapshot of dataset '{dataset}': its data directory {path:?} is not under the dataset's acceleration directory, so a compacted copy could not be re-anchored on the reader. Set `cayenne_file_path` to a directory that contains the dataset's data, or set `snapshots_compaction: disabled`"
    ))]
    DataDirNotUnderAnchor { dataset: String, path: String },

    #[snafu(display(
        "Cannot compact the snapshot of dataset '{dataset}': snapshots of a partitioned Cayenne acceleration are not supported"
    ))]
    PartitionedTable { dataset: String },

    #[snafu(display(
        "Cannot compact the snapshot of dataset '{dataset}': it has a cold tier (`cayenne_datalake_location`), and a compacted snapshot would re-encode every cold row into local files on each snapshot. Set `snapshots_compaction: disabled` for this dataset"
    ))]
    ColdTierTable { dataset: String },

    #[snafu(display(
        "Failed to prepare the scratch directory for compacting the snapshot of dataset '{dataset}': {source}"
    ))]
    Scratch {
        dataset: String,
        source: std::io::Error,
    },

    #[snafu(display(
        "Failed to open the scratch Cayenne metadata for compacting the snapshot of dataset '{dataset}': {source}"
    ))]
    ScratchCatalog {
        dataset: String,
        source: cayenne::CatalogError,
    },

    #[snafu(display(
        "Failed to rewrite the Cayenne configuration of dataset '{dataset}' for snapshot compaction: {source}"
    ))]
    VortexConfigJson {
        dataset: String,
        source: serde_json::Error,
    },

    #[snafu(display(
        "Failed to open the scratch Cayenne table for compacting the snapshot of dataset '{dataset}': {source}"
    ))]
    ScratchTable {
        dataset: String,
        #[snafu(source(from(cayenne::provider::Error, Box::new)))]
        source: Box<cayenne::provider::Error>,
    },

    #[snafu(display(
        "Failed to write the compacted copy of dataset '{dataset}' for its snapshot: {source}"
    ))]
    Rewrite {
        dataset: String,
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display(
        "The compacted copy of dataset '{dataset}' is not the clean layout snapshot compaction promises ({detail}); the snapshot was not published"
    ))]
    NotCompact { dataset: String, detail: String },
}

pub type Result<T, E = CompactionError> = std::result::Result<T, E>;

/// Position of `column` in a slice's `cayenne_table` row.
fn table_column(dataset: &str, column: &'static str) -> Result<usize> {
    EXPECTED_TABLES
        .iter()
        .find(|table| table.name == CAYENNE_TABLE)
        .and_then(|table| table.columns.iter().position(|c| *c == column))
        .context(MissingColumnSnafu { dataset, column })
}

/// The live table's view and identity, taken under the accelerator write lock.
pub struct CompactionCapture {
    dataset_name: String,
    /// The live `cayenne_table` row, paths relative to the live data directory.
    table_row: SliceRow,
    /// Scan of the live table, bound to the view captured at plan build.
    scan: Arc<dyn ExecutionPlan>,
    /// Session the scan was planned in; the rewrite runs in it too.
    session: SessionContext,
}

impl CompactionCapture {
    /// Captures the live table's view and metadata. Runs under the write lock.
    ///
    /// # Errors
    ///
    /// The live table cannot be scanned or exported, or the dataset is not
    /// supported (data directory outside the acceleration directory,
    /// partitioned table, cold tier).
    pub async fn capture(
        catalog: &Arc<dyn MetadataCatalog>,
        dataset_name: &str,
        live_data_dir: &Path,
        live_table: &Arc<dyn TableProvider>,
    ) -> Result<Self> {
        let session = SessionContext::new();
        let scan = live_table
            .scan(&session.state(), None, &[], None)
            .await
            .context(CaptureScanSnafu {
                dataset: dataset_name,
            })?;

        let slice = catalog
            .export_dataset_slice(dataset_name, live_data_dir)
            .await
            .context(ExportLiveSnafu {
                dataset: dataset_name,
            })?;
        let table_row = slice
            .tables
            .get(CAYENNE_TABLE)
            .and_then(|rows| rows.first())
            .cloned()
            .context(MissingTableRowSnafu {
                dataset: dataset_name,
            })?;

        // The path is re-anchored at the scratch directory and again on the
        // reader, which needs it to be relative.
        let path_is_relative = table_column(dataset_name, "path_is_relative")?;
        let path = table_column(dataset_name, "path")?;
        ensure!(
            matches!(
                table_row.get(path_is_relative),
                Some(SliceValue::Bool(true))
            ),
            DataDirNotUnderAnchorSnafu {
                dataset: dataset_name,
                path: match table_row.get(path) {
                    Some(SliceValue::Text(p)) => p.clone(),
                    _ => String::new(),
                },
            }
        );
        let partition_column = table_column(dataset_name, "partition_column")?;
        ensure!(
            matches!(
                table_row.get(partition_column),
                Some(SliceValue::Null) | None
            ),
            PartitionedTableSnafu {
                dataset: dataset_name
            }
        );
        // The scan would pull the whole cold tier into local files.
        let vortex_config_column = table_column(dataset_name, "vortex_config_json")?;
        if let Some(SliceValue::Text(json)) = table_row.get(vortex_config_column) {
            let config: VortexConfig =
                serde_json::from_str(json).context(VortexConfigJsonSnafu {
                    dataset: dataset_name,
                })?;
            ensure!(
                config.cold_tier_location.is_none(),
                ColdTierTableSnafu {
                    dataset: dataset_name
                }
            );
        }

        Ok(Self {
            dataset_name: dataset_name.to_string(),
            table_row,
            scan,
            session,
        })
    }

    /// Re-encodes the captured view into a scratch table and returns its data
    /// directory and metastore slice for archiving. Runs after the write lock
    /// is released. `metadata_dirs` are archived alongside, minus
    /// `skip_metadata_files`.
    ///
    /// # Errors
    ///
    /// The scratch table cannot be prepared, the rewrite fails, or the result
    /// is not a clean layout; the snapshot is then not published.
    pub async fn materialize(
        self,
        metadata_dirs: Vec<(PathBuf, String)>,
        skip_metadata_files: &[&str],
        slice_archive_path: String,
    ) -> Result<MaterializedDirectorySnapshot> {
        let dataset = self.dataset_name.as_str();
        let scratch = tempfile::Builder::new()
            .prefix("cayenne-snapshot-compaction-")
            .tempdir()
            .context(ScratchSnafu { dataset })?;
        let scratch_metadata_dir = scratch.path().join("metadata");
        let scratch_data_dir = scratch.path().join("data");
        for dir in [&scratch_metadata_dir, &scratch_data_dir] {
            tokio::fs::create_dir_all(dir)
                .await
                .context(ScratchSnafu { dataset })?;
        }

        let scratch_catalog: Arc<dyn MetadataCatalog> = Arc::new(
            CayenneCatalog::new(format!(
                "sqlite://{}",
                scratch_metadata_dir.join("cayenne.db").display()
            ))
            .context(ScratchCatalogSnafu { dataset })?,
        );
        scratch_catalog
            .init()
            .await
            .context(ScratchCatalogSnafu { dataset })?;

        // Seed the scratch metastore with the live table's identity and an
        // empty snapshot; the overwrite below replaces the snapshot.
        let seed_snapshot_id = uuid::Uuid::now_v7().to_string();
        let mut row = self.table_row.clone();
        row[table_column(dataset, "current_snapshot_id")?] =
            SliceValue::Text(seed_snapshot_id.clone());
        let vortex_config_column = table_column(dataset, "vortex_config_json")?;
        if let Some(SliceValue::Text(json)) = row.get(vortex_config_column) {
            row[vortex_config_column] = SliceValue::Text(scratch_vortex_config(dataset, json)?);
        }
        let exported_at_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |elapsed| {
                i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX)
            });
        let seed = DatasetMetastoreSlice {
            format_version: SLICE_FORMAT_VERSION,
            engine: SLICE_ENGINE.to_string(),
            dataset_name: dataset.to_string(),
            exported_at_ms,
            tables: BTreeMap::from([(CAYENNE_TABLE.to_string(), vec![row])]),
        };
        scratch_catalog
            .import_dataset_slice(&seed, &scratch_data_dir)
            .await
            .context(ScratchCatalogSnafu { dataset })?;
        let seeded = scratch_catalog
            .get_table(dataset)
            .await
            .context(ScratchCatalogSnafu { dataset })?;
        let seed_snapshot_dir = Path::new(&seeded.path)
            .join(&seeded.table_id)
            .join(&seed_snapshot_id);
        tokio::fs::create_dir_all(&seed_snapshot_dir)
            .await
            .context(ScratchSnafu { dataset })?;

        let provider = CayenneTableProviderBuilder::new(
            Arc::clone(&scratch_catalog),
            self.session.runtime_env(),
        )
        .open(dataset)
        .await
        .context(ScratchTableSnafu { dataset })?;

        let rows_written = {
            // The sink reads one input partition; the captured plan bypasses
            // the planner that would normally insert this coalesce.
            let input: Arc<dyn ExecutionPlan> = if self
                .scan
                .properties()
                .output_partitioning()
                .partition_count()
                > 1
            {
                Arc::new(CoalescePartitionsExec::new(self.scan))
            } else {
                self.scan
            };
            let sink = provider
                .insert_into(&self.session.state(), input, InsertOp::Overwrite)
                .await
                .context(RewriteSnafu { dataset })?;
            let batches = collect(sink, self.session.task_ctx())
                .await
                .context(RewriteSnafu { dataset })?;
            rows_written(&batches)
        };
        // Wait for detached post-write maintenance before reading the layout.
        provider
            .drain_in_flight_maintenance()
            .await
            .context(ScratchCatalogSnafu { dataset })?;
        drop(provider);

        // Verify the clean layout rather than publish a snapshot that is not.
        let compacted = scratch_catalog
            .get_table(dataset)
            .await
            .context(ScratchCatalogSnafu { dataset })?;
        ensure!(
            compacted.current_snapshot_id != seed_snapshot_id,
            NotCompactSnafu {
                dataset,
                detail: "the rewrite published no snapshot".to_string(),
            }
        );
        let delete_files = scratch_catalog
            .get_table_delete_files(&compacted.table_id)
            .await
            .context(ScratchCatalogSnafu { dataset })?;
        ensure!(
            delete_files.is_empty(),
            NotCompactSnafu {
                dataset,
                detail: format!("{} deletion files remain", delete_files.len()),
            }
        );
        let protected = scratch_catalog
            .get_all_snapshot_sequences(&compacted.table_id)
            .await
            .context(ScratchCatalogSnafu { dataset })?;
        ensure!(
            protected.is_empty(),
            NotCompactSnafu {
                dataset,
                detail: format!("{} protected snapshots remain", protected.len()),
            }
        );
        let inlined = scratch_catalog
            .get_inlined_data_count(&compacted.table_id)
            .await
            .context(ScratchCatalogSnafu { dataset })?;
        ensure!(
            inlined == 0,
            NotCompactSnafu {
                dataset,
                detail: format!("{inlined} inlined row batches remain"),
            }
        );

        // The seed snapshot is superseded; the sweep may already have removed it.
        if let Err(err) = tokio::fs::remove_dir_all(&seed_snapshot_dir).await
            && err.kind() != std::io::ErrorKind::NotFound
        {
            return Err(err).context(ScratchSnafu { dataset });
        }

        let slice = scratch_catalog
            .export_dataset_slice(dataset, &scratch_data_dir)
            .await
            .context(ScratchCatalogSnafu { dataset })?;
        let slice_bytes =
            slice
                .to_json_bytes()
                .map_err(|source| CompactionError::VortexConfigJson {
                    dataset: dataset.to_string(),
                    source,
                })?;

        let (files, bytes) = directory_footprint(&scratch_data_dir)
            .await
            .context(ScratchSnafu { dataset })?;
        tracing::info!(
            "Compacted the snapshot of dataset '{dataset}': {rows_written} rows in {files} data files ({bytes} bytes), one snapshot, no deletion files"
        );

        let mut dirs = metadata_dirs;
        dirs.push((scratch_data_dir, "data/".to_string()));
        Ok(MaterializedDirectorySnapshot {
            dirs,
            skip_relative_paths: skip_metadata_files.iter().map(PathBuf::from).collect(),
            extra_entries: vec![DirectoryArchiveExtra {
                archive_path: slice_archive_path,
                bytes: slice_bytes,
            }],
            cleanup_dirs: vec![scratch.keep()],
        })
    }
}

/// The live table's Vortex configuration adjusted for a one-shot scratch
/// rewrite: rows always go to Vortex files (never the inline tier), nothing
/// schedules maintenance on the scratch table, and no cold tier is written.
/// Layout settings — sort columns, clustering, target file size, write
/// concurrency — are kept so the compacted files match the operator's layout.
fn scratch_vortex_config(dataset: &str, json: &str) -> Result<String> {
    let mut config: VortexConfig =
        serde_json::from_str(json).context(VortexConfigJsonSnafu { dataset })?;
    config.inline_max_rows = 0;
    config.inline_max_bytes = 0;
    config.compaction_trigger_files = usize::MAX;
    config.compaction_trigger_protected_snapshots = usize::MAX;
    config.compaction_trigger_snapshot_age_ms = 0;
    config.compaction_background_interval_ms = 0;
    config.cdc_mem_tier_checkpoint_interval_ms = 0;
    config.cold_tier_location = None;
    serde_json::to_string(&config).context(VortexConfigJsonSnafu { dataset })
}

/// Sums the `count` column a `DataSinkExec` returns.
fn rows_written(batches: &[arrow::record_batch::RecordBatch]) -> u64 {
    batches
        .iter()
        .filter_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::UInt64Array>()
        })
        .map(|counts| counts.iter().flatten().sum::<u64>())
        .sum()
}

/// `(files, bytes)` under `dir`, recursively.
async fn directory_footprint(dir: &Path) -> std::io::Result<(u64, u64)> {
    let (mut files, mut bytes) = (0u64, 0u64);
    let mut pending = vec![dir.to_path_buf()];
    while let Some(dir) = pending.pop() {
        let mut entries = tokio::fs::read_dir(&dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let metadata = entry.metadata().await?;
            if metadata.is_dir() {
                pending.push(entry.path());
            } else {
                files += 1;
                bytes += metadata.len();
            }
        }
    }
    Ok((files, bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot_engine::CayenneSnapshotEngine;
    use arrow::array::{Int64Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use cayenne::metadata::CreateTableOptions;
    use cayenne::{CayenneTableProvider, MetadataCatalog};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::prelude::{col, lit};
    use datafusion_table_providers::util::{
        column_reference::ColumnReference, on_conflict::OnConflict,
    };
    use runtime_acceleration::snapshot::directory_archive::{
        ExtractOptions, archive_directories_to_file_with_plan, extract_archive_file_with_options,
    };
    use runtime_acceleration::snapshot::engine::SnapshotEngine;
    use std::collections::BTreeMap;

    const DATASET: &str = "trips";

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    fn batch(rows: &[(i64, i64)]) -> RecordBatch {
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter().map(|(_, v)| *v).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter()
                        .map(|(id, _)| Some(format!("row-{id}")))
                        .collect::<Vec<_>>(),
                )),
            ],
        )
        .expect("valid batch")
    }

    /// A writer-shaped layout: shared `metadata/` beside a per-dataset data dir.
    struct Node {
        metadata_dir: PathBuf,
        data_dir: PathBuf,
        catalog: Arc<dyn MetadataCatalog>,
    }

    impl Node {
        async fn new(root: &Path) -> Self {
            let metadata_dir = root.join("metadata");
            let data_dir = root.join(DATASET);
            for dir in [&metadata_dir, &data_dir] {
                std::fs::create_dir_all(dir).expect("mkdir");
            }
            let catalog: Arc<dyn MetadataCatalog> = Arc::new(
                CayenneCatalog::new(format!(
                    "sqlite://{}",
                    metadata_dir.join("cayenne.db").display()
                ))
                .expect("catalog"),
            );
            catalog.init().await.expect("init");
            Self {
                metadata_dir,
                data_dir,
                catalog,
            }
        }

        fn dirs(&self) -> Vec<(PathBuf, String)> {
            vec![
                (self.metadata_dir.clone(), "metadata/".to_string()),
                (self.data_dir.clone(), "data/".to_string()),
            ]
        }

        /// Snapshot directories under `<data_dir>/<table_id>/`.
        fn snapshot_dirs(&self, table_id: &str) -> usize {
            std::fs::read_dir(self.data_dir.join(table_id))
                .expect("table dir")
                .filter_map(Result::ok)
                .filter(|e| e.path().is_dir() && !e.file_name().to_string_lossy().starts_with('_'))
                .count()
        }
    }

    async fn create_live_table(node: &Node) -> Arc<CayenneTableProvider> {
        let ctx = SessionContext::new();
        // Every write goes to Vortex files so upserts publish protected snapshots
        // and deletes write deletion files: the merge state compaction removes.
        let vortex_config = VortexConfig {
            inline_max_rows: 0,
            inline_max_bytes: 0,
            ..VortexConfig::default()
        };
        let provider =
            CayenneTableProviderBuilder::new(Arc::clone(&node.catalog), ctx.runtime_env())
                .create(CreateTableOptions {
                    table_name: DATASET.to_string(),
                    schema: schema(),
                    primary_key: vec!["id".to_string()],
                    on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
                        "id".to_string(),
                    ]))),
                    base_path: node.data_dir.to_string_lossy().into_owned(),
                    partition_column: None,
                    vortex_config,
                })
                .await
                .expect("create table");
        Arc::new(provider)
    }

    async fn insert(table: &CayenneTableProvider, rows: &[(i64, i64)]) {
        let ctx = SessionContext::new();
        let input = MemorySourceConfig::try_new_exec(&[vec![batch(rows)]], schema(), None)
            .expect("memory exec");
        let plan = table
            .insert_into(&ctx.state(), input, InsertOp::Append)
            .await
            .expect("insert plan");
        collect(plan, ctx.task_ctx()).await.expect("insert");
    }

    async fn delete_where(table: &CayenneTableProvider, filter: datafusion::prelude::Expr) {
        let ctx = SessionContext::new();
        let plan = table
            .delete_from(&ctx.state(), vec![filter])
            .await
            .expect("delete plan");
        collect(plan, ctx.task_ctx()).await.expect("delete");
    }

    /// `id -> value` of every visible row, in id order.
    async fn rows(table: &Arc<CayenneTableProvider>) -> BTreeMap<i64, i64> {
        let ctx = SessionContext::new();
        let batches = ctx
            .read_table(Arc::clone(table) as Arc<dyn TableProvider>)
            .expect("read_table")
            .select_columns(&["id", "value"])
            .expect("select")
            .collect()
            .await
            .expect("collect");
        let mut out = BTreeMap::new();
        for batch in batches {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("id column");
            let values = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("value column");
            for i in 0..batch.num_rows() {
                assert!(
                    out.insert(ids.value(i), values.value(i)).is_none(),
                    "duplicate id {} in scan output",
                    ids.value(i)
                );
            }
        }
        out
    }

    /// Writer with merge-on-read state → compacted snapshot → reader bootstrap:
    /// the reader serves exactly the rows visible at capture time from one
    /// snapshot with no deletion files, protected snapshots or inlined rows,
    /// and writes made after the capture (while the rewrite ran) are absent.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn compacted_snapshot_round_trips_visible_rows_and_drops_merge_state() {
        let tmp = tempfile::tempdir().expect("tmp");
        let writer = Node::new(&tmp.path().join("writer")).await;
        let live = create_live_table(&writer).await;

        // Base load, then upserts (protected snapshot + tombstones) and a delete.
        let base: Vec<(i64, i64)> = (1..=100).map(|id| (id, id * 10)).collect();
        insert(&live, &base).await;
        let upserts: Vec<(i64, i64)> = (1..=20).map(|id| (id, id * 100)).collect();
        insert(&live, &upserts).await;
        delete_where(&live, col("id").gt(lit(90_i64))).await;

        let mut expected: BTreeMap<i64, i64> = (1..=90).map(|id| (id, id * 10)).collect();
        for id in 1..=20 {
            expected.insert(id, id * 100);
        }
        assert_eq!(rows(&live).await, expected, "live table before compaction");

        let live_meta = writer.catalog.get_table(DATASET).await.expect("live meta");
        let live_delete_files = writer
            .catalog
            .get_table_delete_files(&live_meta.table_id)
            .await
            .expect("live delete files")
            .len();
        let live_protected = writer
            .catalog
            .get_all_snapshot_sequences(&live_meta.table_id)
            .await
            .expect("live protected")
            .len();
        let live_snapshot_dirs = writer.snapshot_dirs(&live_meta.table_id);
        assert!(
            live_snapshot_dirs > 1 || live_delete_files > 0,
            "the live table must carry merge state for this test to mean anything: snapshot_dirs={live_snapshot_dirs} delete_files={live_delete_files} protected={live_protected}"
        );

        // Capture under the (simulated) write lock.
        let engine = CayenneSnapshotEngine::new(
            Arc::clone(&writer.catalog),
            DATASET,
            writer.data_dir.clone(),
        )
        .with_compaction(true);
        let live_dyn: Arc<dyn TableProvider> = Arc::clone(&live) as Arc<dyn TableProvider>;
        let plan = engine
            .prepare_directory_snapshot(&writer.dirs(), DATASET, Some(&live_dyn))
            .await
            .expect("prepare");
        let deferred = plan.deferred.expect("compaction defers the build");

        // Writes resume before the rewrite runs; they must not leak into it.
        insert(&live, &[(5, 555), (200, 2000)]).await;
        delete_where(&live, col("id").eq(lit(30_i64))).await;

        let materialized = deferred.await.expect("materialize");
        assert!(
            materialized
                .dirs
                .iter()
                .any(|(_, prefix)| prefix == "data/"),
            "materialized layout must provide the data directory"
        );
        assert!(
            !materialized
                .dirs
                .iter()
                .any(|(dir, _)| *dir == writer.data_dir),
            "the live data directory must not be archived under compaction"
        );

        // Archive → extract onto a fresh reader, as SnapshotManager does.
        let tar = tmp.path().join("snapshot.tar");
        let skip: Vec<PathBuf> = materialized.skip_relative_paths.into_iter().collect();
        let extras: Vec<(String, Vec<u8>)> = materialized
            .extra_entries
            .into_iter()
            .map(|e| (e.archive_path, e.bytes))
            .collect();
        archive_directories_to_file_with_plan(&materialized.dirs, &tar, &skip, &extras)
            .await
            .expect("archive");
        for dir in &materialized.cleanup_dirs {
            tokio::fs::remove_dir_all(dir)
                .await
                .expect("cleanup scratch");
        }

        let reader = Node::new(&tmp.path().join("reader")).await;
        extract_archive_file_with_options(
            &tar,
            &tmp.path().join("reader"),
            ExtractOptions {
                prefix_mappings: Some(vec![
                    ("metadata/".to_string(), reader.metadata_dir.clone()),
                    ("data/".to_string(), reader.data_dir.clone()),
                ]),
                ..ExtractOptions::skip_existing()
            },
        )
        .await
        .expect("extract");
        CayenneSnapshotEngine::new(
            Arc::clone(&reader.catalog),
            DATASET,
            reader.data_dir.clone(),
        )
        .finalize_directory_snapshot(&reader.dirs(), DATASET)
        .await
        .expect("import slice");

        let ctx = SessionContext::new();
        let restored = Arc::new(
            CayenneTableProviderBuilder::new(Arc::clone(&reader.catalog), ctx.runtime_env())
                .open(DATASET)
                .await
                .expect("open reader table"),
        );
        assert_eq!(
            rows(&restored).await,
            expected,
            "reader must serve the rows visible when the snapshot was captured"
        );

        let restored_meta = reader
            .catalog
            .get_table(DATASET)
            .await
            .expect("reader meta");
        assert_eq!(
            restored_meta.table_id, live_meta.table_id,
            "compaction keeps the dataset's table_id"
        );
        assert!(
            restored_meta.current_sequence_number >= live_meta.current_sequence_number,
            "sequence counter must not go backwards ({} < {})",
            restored_meta.current_sequence_number,
            live_meta.current_sequence_number
        );
        assert_eq!(restored_meta.primary_key, vec!["id".to_string()]);
        assert!(restored_meta.on_conflict.is_some(), "on_conflict preserved");
        assert_eq!(
            reader
                .catalog
                .get_table_delete_files(&restored_meta.table_id)
                .await
                .expect("reader delete files")
                .len(),
            0,
            "no deletion files"
        );
        assert_eq!(
            reader
                .catalog
                .get_all_snapshot_sequences(&restored_meta.table_id)
                .await
                .expect("reader protected")
                .len(),
            0,
            "no protected snapshots"
        );
        assert_eq!(
            reader
                .catalog
                .get_inlined_data_count(&restored_meta.table_id)
                .await
                .expect("reader inlined"),
            0,
            "no inlined rows"
        );
        assert_eq!(
            reader.snapshot_dirs(&restored_meta.table_id),
            1,
            "exactly one snapshot directory"
        );
    }

    /// Without `snapshots_compaction: enabled` the plan is the plain archive of
    /// the live layout, even when the live table is available.
    #[tokio::test]
    async fn compaction_disabled_archives_live_layout() {
        let tmp = tempfile::tempdir().expect("tmp");
        let writer = Node::new(tmp.path()).await;
        let live = create_live_table(&writer).await;
        insert(&live, &[(1, 10)]).await;

        let engine = CayenneSnapshotEngine::new(
            Arc::clone(&writer.catalog),
            DATASET,
            writer.data_dir.clone(),
        );
        assert!(!engine.compaction());
        let live_dyn: Arc<dyn TableProvider> = Arc::clone(&live) as Arc<dyn TableProvider>;
        let plan = engine
            .prepare_directory_snapshot(&writer.dirs(), DATASET, Some(&live_dyn))
            .await
            .expect("prepare");
        assert!(plan.deferred.is_none());
        assert_eq!(plan.extra_entries.len(), 1, "the metastore slice is inline");
    }

    /// A caller with no live table (the pre-recreation snapshot) gets the plain
    /// archive rather than an error: an uncompacted snapshot is still correct.
    #[tokio::test]
    async fn compaction_without_live_table_falls_back_to_live_layout() {
        let tmp = tempfile::tempdir().expect("tmp");
        let writer = Node::new(tmp.path()).await;
        let live = create_live_table(&writer).await;
        insert(&live, &[(1, 10)]).await;

        let engine = CayenneSnapshotEngine::new(
            Arc::clone(&writer.catalog),
            DATASET,
            writer.data_dir.clone(),
        )
        .with_compaction(true);
        let plan = engine
            .prepare_directory_snapshot(&writer.dirs(), DATASET, None)
            .await
            .expect("prepare");
        assert!(plan.deferred.is_none());
        assert_eq!(plan.extra_entries.len(), 1);
    }

    /// The rewrite runs after the write lock is released, so the snapshot it
    /// captured can be retired meanwhile. The captured plan's `SnapshotScanRef`
    /// keeps the retired directory from being swept until the rewrite has read
    /// it: overwrite the live table after the capture, wait past the sweep's
    /// grace with sweeps being scheduled, and the compacted copy still holds
    /// the captured rows; once the plan is consumed the directory is reaped.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn captured_plan_pins_retired_snapshot_until_rewrite_completes() {
        let tmp = tempfile::tempdir().expect("tmp");
        let writer = Node::new(&tmp.path().join("writer")).await;
        let live = create_live_table(&writer).await;
        let base: Vec<(i64, i64)> = (1..=1000).map(|id| (id, id * 10)).collect();
        insert(&live, &base).await;
        insert(&live, &[(1, 100), (2, 200)]).await;
        let expected = rows(&live).await;
        let meta = writer.catalog.get_table(DATASET).await.expect("live meta");
        let captured_dir = writer
            .data_dir
            .join(&meta.table_id)
            .join(&meta.current_snapshot_id);
        assert!(captured_dir.is_dir());

        let engine = CayenneSnapshotEngine::new(
            Arc::clone(&writer.catalog),
            DATASET,
            writer.data_dir.clone(),
        )
        .with_compaction(true);
        let live_dyn: Arc<dyn TableProvider> = Arc::clone(&live) as Arc<dyn TableProvider>;
        let plan = engine
            .prepare_directory_snapshot(&writer.dirs(), DATASET, Some(&live_dyn))
            .await
            .expect("prepare");
        let deferred = plan.deferred.expect("compaction defers the build");

        // Retire the captured snapshot with a whole-table overwrite.
        {
            let ctx = SessionContext::new();
            let input =
                MemorySourceConfig::try_new_exec(&[vec![batch(&[(7, 7), (8, 8)])]], schema(), None)
                    .expect("memory exec");
            let overwrite = live
                .insert_into(&ctx.state(), input, InsertOp::Overwrite)
                .await
                .expect("overwrite plan");
            collect(overwrite, ctx.task_ctx()).await.expect("overwrite");
        }
        let after = writer.catalog.get_table(DATASET).await.expect("live meta");
        assert_ne!(after.current_snapshot_id, meta.current_snapshot_id);

        // The sweep keeps a retired directory for a 5 s grace regardless of
        // pins, so the wait is what puts the pin under test; each commit
        // schedules a sweep.
        for i in 0..4 {
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            insert(&live, &[(100 + i, i)]).await;
        }
        live.drain_in_flight_maintenance().await.expect("drain");
        assert!(
            captured_dir.is_dir(),
            "the captured snapshot directory must survive while the plan is alive"
        );

        let materialized = deferred.await.expect("materialize");
        let reader = Node::new(&tmp.path().join("reader")).await;
        let tar = tmp.path().join("snapshot.tar");
        let skip: Vec<PathBuf> = materialized.skip_relative_paths.into_iter().collect();
        let extras: Vec<(String, Vec<u8>)> = materialized
            .extra_entries
            .into_iter()
            .map(|e| (e.archive_path, e.bytes))
            .collect();
        archive_directories_to_file_with_plan(&materialized.dirs, &tar, &skip, &extras)
            .await
            .expect("archive");
        for dir in &materialized.cleanup_dirs {
            tokio::fs::remove_dir_all(dir)
                .await
                .expect("cleanup scratch");
        }
        extract_archive_file_with_options(
            &tar,
            &tmp.path().join("reader"),
            ExtractOptions {
                prefix_mappings: Some(vec![
                    ("metadata/".to_string(), reader.metadata_dir.clone()),
                    ("data/".to_string(), reader.data_dir.clone()),
                ]),
                ..ExtractOptions::skip_existing()
            },
        )
        .await
        .expect("extract");
        CayenneSnapshotEngine::new(
            Arc::clone(&reader.catalog),
            DATASET,
            reader.data_dir.clone(),
        )
        .finalize_directory_snapshot(&reader.dirs(), DATASET)
        .await
        .expect("import slice");
        let ctx = SessionContext::new();
        let restored = Arc::new(
            CayenneTableProviderBuilder::new(Arc::clone(&reader.catalog), ctx.runtime_env())
                .open(DATASET)
                .await
                .expect("open reader table"),
        );
        assert_eq!(
            rows(&restored).await,
            expected,
            "the compacted copy holds the rows captured before the overwrite"
        );

        // The plan was consumed by the rewrite, so nothing pins the directory.
        // A sweep is scheduled by commits that advance the snapshot pointer, so
        // drive it with overwrites (bounded poll).
        let mut reaped = false;
        for i in 0..15 {
            let ctx = SessionContext::new();
            let input =
                MemorySourceConfig::try_new_exec(&[vec![batch(&[(200 + i, i)])]], schema(), None)
                    .expect("memory exec");
            let overwrite = live
                .insert_into(&ctx.state(), input, InsertOp::Overwrite)
                .await
                .expect("overwrite plan");
            collect(overwrite, ctx.task_ctx()).await.expect("overwrite");
            live.drain_in_flight_maintenance().await.expect("drain");
            if !captured_dir.is_dir() {
                reaped = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
        assert!(
            reaped,
            "retired directory must be swept once no scan pins it"
        );
    }

    /// Every directory the capture references — the current snapshot and each
    /// protected snapshot — survives the live table's own maintenance passes
    /// while the rewrite runs: current-snapshot compaction, the
    /// protected-snapshot fold, the seq-prefix bake, a mem-tier checkpoint and
    /// an overwrite all retire directories through the sweep, which excludes
    /// the pinned set. The compacted copy then holds exactly the captured rows.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn captured_snapshots_survive_live_maintenance_passes() {
        let tmp = tempfile::tempdir().expect("tmp");
        let writer = Node::new(&tmp.path().join("writer")).await;
        let live = create_live_table(&writer).await;
        // Several small base files and several upsert publishes, so both the
        // current-snapshot compaction and the protected-snapshot passes have
        // work to do.
        for chunk in (1..=2000).collect::<Vec<i64>>().chunks(250) {
            let rows: Vec<(i64, i64)> = chunk.iter().map(|id| (*id, id * 10)).collect();
            insert(&live, &rows).await;
        }
        for round in 1..=4_i64 {
            let upserts: Vec<(i64, i64)> = (1..=100).map(|id| (id, id * 1000 + round)).collect();
            insert(&live, &upserts).await;
        }
        delete_where(&live, col("id").gt(lit(1900_i64))).await;
        let expected = rows(&live).await;

        let meta = writer.catalog.get_table(DATASET).await.expect("live meta");
        let protected_at_capture = writer
            .catalog
            .get_all_snapshot_sequences(&meta.table_id)
            .await
            .expect("protected");
        assert!(
            !protected_at_capture.is_empty(),
            "upserts must have published protected snapshots"
        );
        let table_dir = writer.data_dir.join(&meta.table_id);
        let captured_dirs: Vec<PathBuf> = std::iter::once(meta.current_snapshot_id.clone())
            .chain(protected_at_capture.keys().cloned())
            .map(|id| table_dir.join(id))
            .collect();
        assert!(captured_dirs.iter().all(|d| d.is_dir()));

        let engine = CayenneSnapshotEngine::new(
            Arc::clone(&writer.catalog),
            DATASET,
            writer.data_dir.clone(),
        )
        .with_compaction(true);
        let live_dyn: Arc<dyn TableProvider> = Arc::clone(&live) as Arc<dyn TableProvider>;
        let plan = engine
            .prepare_directory_snapshot(&writer.dirs(), DATASET, Some(&live_dyn))
            .await
            .expect("prepare");
        let deferred = plan.deferred.expect("compaction defers the build");

        // Live maintenance while the plan is alive. Each pass reports whether
        // it did work; the protected set must have shrunk for the test to mean
        // anything.
        let folded = live
            .compact_protected_snapshots_subset(32)
            .await
            .expect("protected fold");
        let baked = live
            .bake_seq_prefix_protected_snapshots()
            .await
            .expect("seq-prefix bake");
        let compacted = live
            .compact_current_snapshot_small_files()
            .await
            .expect("current-snapshot compaction");
        insert(&live, &[(5000, 5)]).await;
        live.checkpoint_mem_tier()
            .await
            .expect("mem-tier checkpoint");
        {
            let ctx = SessionContext::new();
            let input =
                MemorySourceConfig::try_new_exec(&[vec![batch(&[(7, 7), (8, 8)])]], schema(), None)
                    .expect("memory exec");
            let overwrite = live
                .insert_into(&ctx.state(), input, InsertOp::Overwrite)
                .await
                .expect("overwrite plan");
            collect(overwrite, ctx.task_ctx()).await.expect("overwrite");
        }
        let protected_now = writer
            .catalog
            .get_all_snapshot_sequences(&meta.table_id)
            .await
            .expect("protected");
        assert!(
            protected_now.len() < protected_at_capture.len(),
            "maintenance must have retired protected snapshots (fold={folded} bake={baked} compact={compacted}: {} -> {})",
            protected_at_capture.len(),
            protected_now.len()
        );
        // Past the sweep's 5 s grace (time itself is under test), with
        // pointer-advancing commits scheduling sweeps.
        for i in 0..3 {
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            let ctx = SessionContext::new();
            let input =
                MemorySourceConfig::try_new_exec(&[vec![batch(&[(9000 + i, i)])]], schema(), None)
                    .expect("memory exec");
            let overwrite = live
                .insert_into(&ctx.state(), input, InsertOp::Overwrite)
                .await
                .expect("overwrite plan");
            collect(overwrite, ctx.task_ctx()).await.expect("overwrite");
        }
        live.drain_in_flight_maintenance().await.expect("drain");
        let missing: Vec<&PathBuf> = captured_dirs.iter().filter(|d| !d.is_dir()).collect();
        assert!(
            missing.is_empty(),
            "captured snapshot directories must survive live maintenance: missing {missing:?}"
        );

        let materialized = deferred.await.expect("materialize");
        let reader = Node::new(&tmp.path().join("reader")).await;
        let tar = tmp.path().join("snapshot.tar");
        let skip: Vec<PathBuf> = materialized.skip_relative_paths.into_iter().collect();
        let extras: Vec<(String, Vec<u8>)> = materialized
            .extra_entries
            .into_iter()
            .map(|e| (e.archive_path, e.bytes))
            .collect();
        archive_directories_to_file_with_plan(&materialized.dirs, &tar, &skip, &extras)
            .await
            .expect("archive");
        for dir in &materialized.cleanup_dirs {
            tokio::fs::remove_dir_all(dir)
                .await
                .expect("cleanup scratch");
        }
        extract_archive_file_with_options(
            &tar,
            &tmp.path().join("reader"),
            ExtractOptions {
                prefix_mappings: Some(vec![
                    ("metadata/".to_string(), reader.metadata_dir.clone()),
                    ("data/".to_string(), reader.data_dir.clone()),
                ]),
                ..ExtractOptions::skip_existing()
            },
        )
        .await
        .expect("extract");
        CayenneSnapshotEngine::new(
            Arc::clone(&reader.catalog),
            DATASET,
            reader.data_dir.clone(),
        )
        .finalize_directory_snapshot(&reader.dirs(), DATASET)
        .await
        .expect("import slice");
        let ctx = SessionContext::new();
        let restored = Arc::new(
            CayenneTableProviderBuilder::new(Arc::clone(&reader.catalog), ctx.runtime_env())
                .open(DATASET)
                .await
                .expect("open reader table"),
        );
        assert_eq!(
            rows(&restored).await,
            expected,
            "the compacted copy holds the rows captured before maintenance ran"
        );
    }

    /// Stands in for the runtime's slot advancer, which arms the RAM CDC tier.
    struct NoopSlotAdvancer;

    #[async_trait::async_trait]
    impl cayenne::SlotAdvancer for NoopSlotAdvancer {
        async fn on_checkpoint_durable(&self, _durable_epoch: u64) {}
    }

    /// Rows that live only in the RAM CDC tier (`cdc_durability: memory`, not
    /// yet checkpointed to a file) are part of the captured view and reach the
    /// compacted snapshot: the capture clones the mem-tier segments into the
    /// plan, so the scratch rewrite reads them like any other input.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn compaction_includes_rows_held_only_in_the_memory_tier() {
        use cayenne::metadata::CdcDurability;
        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;

        let tmp = tempfile::tempdir().expect("tmp");
        let writer = Node::new(&tmp.path().join("writer")).await;
        let ctx = SessionContext::new();
        let vortex_config = VortexConfig {
            inline_max_rows: 0,
            inline_max_bytes: 0,
            cdc_durability: CdcDurability::Memory,
            // No periodic checkpoint: the rows stay in RAM until this test ends.
            cdc_mem_tier_checkpoint_interval_ms: 0,
            ..VortexConfig::default()
        };
        let live = Arc::new(
            CayenneTableProviderBuilder::new(Arc::clone(&writer.catalog), ctx.runtime_env())
                .create(CreateTableOptions {
                    table_name: DATASET.to_string(),
                    schema: schema(),
                    primary_key: vec!["id".to_string()],
                    on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
                        "id".to_string(),
                    ]))),
                    base_path: writer.data_dir.to_string_lossy().into_owned(),
                    partition_column: None,
                    vortex_config,
                })
                .await
                .expect("create table"),
        );
        // The RAM tier engages only once the runtime installs a slot advancer
        // (the replayable-source gate); stand in for the runtime here.
        live.install_slot_advancer(Arc::new(NoopSlotAdvancer));

        // Durable base rows, then CDC rows that go to the RAM tier only.
        let base: Vec<(i64, i64)> = (1..=500).map(|id| (id, id * 10)).collect();
        insert(&live, &base).await;
        let meta = writer.catalog.get_table(DATASET).await.expect("live meta");
        let files_before = walk_files(&writer.data_dir.join(&meta.table_id)).len();

        let cdc_rows: Vec<(i64, i64)> = (1..=50)
            .map(|id| (id, id * 1000))
            .collect::<Vec<_>>()
            .into_iter()
            .chain((900..=950).map(|id| (id, id)))
            .collect();
        let stream = Box::pin(RecordBatchStreamAdapter::new(
            schema(),
            futures::stream::iter([Ok(batch(&cdc_rows))]),
        ));
        let write = live
            .write_cdc_append_stream(stream, &ctx.task_ctx())
            .await
            .expect("cdc write");
        assert!(
            write.in_memory_epoch().is_some() && !write.has_pending_finalize(),
            "the CDC write must have taken the in-memory tier path"
        );
        write.finish().await.expect("finish");

        let files_after = walk_files(&writer.data_dir.join(&meta.table_id)).len();
        assert_eq!(
            files_before, files_after,
            "the CDC rows must not have reached a file yet"
        );
        assert_eq!(
            writer
                .catalog
                .get_inlined_data_count(&meta.table_id)
                .await
                .expect("inlined"),
            0,
            "the CDC rows must not be in the inline tier either"
        );
        let expected = rows(&live).await;
        assert_eq!(
            expected.get(&1),
            Some(&1000),
            "RAM-tier upsert visible on the writer"
        );
        assert_eq!(
            expected.get(&950),
            Some(&950),
            "RAM-tier insert visible on the writer"
        );

        let engine = CayenneSnapshotEngine::new(
            Arc::clone(&writer.catalog),
            DATASET,
            writer.data_dir.clone(),
        )
        .with_compaction(true);
        let live_dyn: Arc<dyn TableProvider> = Arc::clone(&live) as Arc<dyn TableProvider>;
        let plan = engine
            .prepare_directory_snapshot(&writer.dirs(), DATASET, Some(&live_dyn))
            .await
            .expect("prepare");
        let materialized = plan
            .deferred
            .expect("compaction defers the build")
            .await
            .expect("materialize");

        let reader = Node::new(&tmp.path().join("reader")).await;
        let tar = tmp.path().join("snapshot.tar");
        let skip: Vec<PathBuf> = materialized.skip_relative_paths.into_iter().collect();
        let extras: Vec<(String, Vec<u8>)> = materialized
            .extra_entries
            .into_iter()
            .map(|e| (e.archive_path, e.bytes))
            .collect();
        archive_directories_to_file_with_plan(&materialized.dirs, &tar, &skip, &extras)
            .await
            .expect("archive");
        for dir in &materialized.cleanup_dirs {
            tokio::fs::remove_dir_all(dir)
                .await
                .expect("cleanup scratch");
        }
        extract_archive_file_with_options(
            &tar,
            &tmp.path().join("reader"),
            ExtractOptions {
                prefix_mappings: Some(vec![
                    ("metadata/".to_string(), reader.metadata_dir.clone()),
                    ("data/".to_string(), reader.data_dir.clone()),
                ]),
                ..ExtractOptions::skip_existing()
            },
        )
        .await
        .expect("extract");
        CayenneSnapshotEngine::new(
            Arc::clone(&reader.catalog),
            DATASET,
            reader.data_dir.clone(),
        )
        .finalize_directory_snapshot(&reader.dirs(), DATASET)
        .await
        .expect("import slice");
        let restored = Arc::new(
            CayenneTableProviderBuilder::new(Arc::clone(&reader.catalog), ctx.runtime_env())
                .open(DATASET)
                .await
                .expect("open reader table"),
        );
        assert_eq!(
            rows(&restored).await,
            expected,
            "the compacted snapshot must carry the RAM-tier rows"
        );
    }

    fn walk_files(dir: &Path) -> Vec<PathBuf> {
        let mut out = Vec::new();
        let mut pending = vec![dir.to_path_buf()];
        while let Some(dir) = pending.pop() {
            let Ok(entries) = std::fs::read_dir(&dir) else {
                continue;
            };
            for entry in entries.filter_map(Result::ok) {
                let path = entry.path();
                if path.is_dir() {
                    pending.push(path);
                } else if path.extension().is_some_and(|e| e == "vortex") {
                    out.push(path);
                }
            }
        }
        out
    }

    /// Rows held in the inline tier — small batches stored as Arrow IPC blobs in
    /// `cayenne_inlined_data`, not yet flushed to a Vortex file — are part of the
    /// captured view too, and the compacted snapshot carries them as files.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn compaction_includes_rows_held_in_the_inline_tier() {
        let tmp = tempfile::tempdir().expect("tmp");
        let writer = Node::new(&tmp.path().join("writer")).await;
        let ctx = SessionContext::new();
        // Default inline caps (small writes land in `cayenne_inlined_data`), no
        // flush trigger so they stay there.
        let vortex_config = VortexConfig {
            cdc_durability: cayenne::metadata::CdcDurability::File,
            inline_flush_max_rows: i64::MAX,
            inline_flush_max_segments: i64::MAX,
            inline_flush_max_bytes: i64::MAX,
            ..VortexConfig::default()
        };
        let live = Arc::new(
            CayenneTableProviderBuilder::new(Arc::clone(&writer.catalog), ctx.runtime_env())
                .create(CreateTableOptions {
                    table_name: DATASET.to_string(),
                    schema: schema(),
                    primary_key: vec!["id".to_string()],
                    on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
                        "id".to_string(),
                    ]))),
                    base_path: writer.data_dir.to_string_lossy().into_owned(),
                    partition_column: None,
                    vortex_config,
                })
                .await
                .expect("create table"),
        );
        // A base load large enough to go to a file, then small writes that stay
        // inline: an upsert of base rows and new rows.
        let base: Vec<(i64, i64)> = (1..=5000).map(|id| (id, id * 10)).collect();
        insert(&live, &base).await;
        let meta = writer.catalog.get_table(DATASET).await.expect("live meta");
        let files_before = walk_files(&writer.data_dir.join(&meta.table_id)).len();
        assert!(files_before > 0, "the base load must have produced files");

        insert(
            &live,
            &(1..=20).map(|id| (id, id * 1000)).collect::<Vec<_>>(),
        )
        .await;
        insert(&live, &(9000..=9020).map(|id| (id, id)).collect::<Vec<_>>()).await;
        let inlined = writer
            .catalog
            .get_inlined_data_count(&meta.table_id)
            .await
            .expect("inlined count");
        assert!(inlined > 0, "the small writes must be in the inline tier");
        assert_eq!(
            walk_files(&writer.data_dir.join(&meta.table_id)).len(),
            files_before,
            "the small writes must not have produced files"
        );
        let expected = rows(&live).await;
        assert_eq!(expected.get(&1), Some(&1000));
        assert_eq!(expected.get(&9020), Some(&9020));

        let engine = CayenneSnapshotEngine::new(
            Arc::clone(&writer.catalog),
            DATASET,
            writer.data_dir.clone(),
        )
        .with_compaction(true);
        let live_dyn: Arc<dyn TableProvider> = Arc::clone(&live) as Arc<dyn TableProvider>;
        let plan = engine
            .prepare_directory_snapshot(&writer.dirs(), DATASET, Some(&live_dyn))
            .await
            .expect("prepare");
        let materialized = plan
            .deferred
            .expect("compaction defers the build")
            .await
            .expect("materialize");

        let reader = Node::new(&tmp.path().join("reader")).await;
        let tar = tmp.path().join("snapshot.tar");
        let skip: Vec<PathBuf> = materialized.skip_relative_paths.into_iter().collect();
        let extras: Vec<(String, Vec<u8>)> = materialized
            .extra_entries
            .into_iter()
            .map(|e| (e.archive_path, e.bytes))
            .collect();
        archive_directories_to_file_with_plan(&materialized.dirs, &tar, &skip, &extras)
            .await
            .expect("archive");
        for dir in &materialized.cleanup_dirs {
            tokio::fs::remove_dir_all(dir)
                .await
                .expect("cleanup scratch");
        }
        extract_archive_file_with_options(
            &tar,
            &tmp.path().join("reader"),
            ExtractOptions {
                prefix_mappings: Some(vec![
                    ("metadata/".to_string(), reader.metadata_dir.clone()),
                    ("data/".to_string(), reader.data_dir.clone()),
                ]),
                ..ExtractOptions::skip_existing()
            },
        )
        .await
        .expect("extract");
        CayenneSnapshotEngine::new(
            Arc::clone(&reader.catalog),
            DATASET,
            reader.data_dir.clone(),
        )
        .finalize_directory_snapshot(&reader.dirs(), DATASET)
        .await
        .expect("import slice");
        let restored_meta = reader
            .catalog
            .get_table(DATASET)
            .await
            .expect("reader meta");
        assert_eq!(
            reader
                .catalog
                .get_inlined_data_count(&restored_meta.table_id)
                .await
                .expect("reader inlined"),
            0,
            "the compacted snapshot holds the inline rows as files, not inline"
        );
        let restored = Arc::new(
            CayenneTableProviderBuilder::new(Arc::clone(&reader.catalog), ctx.runtime_env())
                .open(DATASET)
                .await
                .expect("open reader table"),
        );
        assert_eq!(
            rows(&restored).await,
            expected,
            "the compacted snapshot must carry the inline-tier rows"
        );
    }

    #[test]
    fn scratch_vortex_config_forces_files_and_disables_maintenance() {
        let live = VortexConfig {
            inline_max_rows: 1_000,
            compaction_trigger_files: 4,
            cold_tier_location: Some("s3://bucket/cold".to_string()),
            sort_columns: vec!["id".to_string()],
            ..VortexConfig::default()
        };
        let json = serde_json::to_string(&live).expect("json");
        let scratch: VortexConfig =
            serde_json::from_str(&scratch_vortex_config(DATASET, &json).expect("rewrite"))
                .expect("parse");
        assert_eq!(scratch.inline_max_rows, 0);
        assert_eq!(scratch.inline_max_bytes, 0);
        assert_eq!(scratch.compaction_trigger_files, usize::MAX);
        assert_eq!(scratch.compaction_background_interval_ms, 0);
        assert!(scratch.cold_tier_location.is_none());
        assert_eq!(scratch.sort_columns, vec!["id".to_string()], "layout kept");
    }
}
