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

//! Compaction of a Cayenne dataset for publishing (`snapshots_compaction: enabled`).
//!
//! A Cayenne table fed by CDC carries merge-on-read state: protected snapshot
//! directories, per-commit deletion files, inlined rows and tombstones, and a
//! RAM tier. Archiving that layout ships every piece to the reader, which then
//! scans the same union the writer does and downloads state that only grows
//! between snapshots. Compaction instead publishes the *visible* table as one
//! snapshot with every deletion applied, and it never touches the live table.
//!
//! The work splits around the accelerator's write lock:
//!
//! 1. [`CompactionCapture::capture`] runs while the lock is held. It builds a
//!    scan of the live table — Cayenne resolves the snapshot pointer, deletion
//!    view, protected set and RAM tier once, under its listing fence, and the
//!    plan reads only that captured view — and exports the dataset's
//!    `cayenne_table` row. This is cheap: no data is read yet.
//! 2. [`CompactionCapture::materialize`] runs after the lock is released, so
//!    writes resume while the table is re-encoded. It seeds a scratch metastore
//!    with the same `cayenne_table` row (same `table_id`, schema, primary key,
//!    `on_conflict` and sequence number, so the published slice re-imports over
//!    the reader's existing table rather than beside it), opens a provider on
//!    it, and replays the captured scan as an `INSERT OVERWRITE`. The overwrite
//!    path is what a full refresh uses: one fresh snapshot, no deletion files,
//!    no protected snapshots, sorted by the operator's `cayenne_sort_columns`
//!    or `cayenne_cluster_by` when set. The scratch data directory and its
//!    metastore slice are what the archive ships.
//!
//! The captured plan pins the directories it reads (`SnapshotScanRef`), so the
//! live table's snapshot sweeps cannot remove them mid-rewrite however long
//! the encode takes.

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

/// Column position of `column` in the `cayenne_table` row of a metastore slice.
fn table_column(dataset: &str, column: &'static str) -> Result<usize> {
    EXPECTED_TABLES
        .iter()
        .find(|table| table.name == CAYENNE_TABLE)
        .and_then(|table| table.columns.iter().position(|c| *c == column))
        .context(MissingColumnSnafu { dataset, column })
}

/// The live table's view and identity, taken while the accelerator's write
/// lock is held. Everything [`Self::materialize`] reads is in here.
pub struct CompactionCapture {
    dataset_name: String,
    /// The live `cayenne_table` row, paths relative to the live data directory.
    table_row: SliceRow,
    /// Scan of the live table, bound to the view captured at plan-build time.
    scan: Arc<dyn ExecutionPlan>,
    /// The session the scan was planned with; the rewrite executes in it too.
    session: SessionContext,
}

impl CompactionCapture {
    /// Captures the live table's view and metadata. Runs under the write lock.
    ///
    /// # Errors
    ///
    /// Fails when the live table cannot be scanned, when its metadata cannot be
    /// exported, or when the dataset is one compaction does not support (a data
    /// directory outside the dataset's acceleration directory, or a partitioned
    /// table).
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

        // The scratch copy is re-anchored at the scratch data directory and the
        // reader re-anchors it again, which only works for a relative path.
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

        Ok(Self {
            dataset_name: dataset_name.to_string(),
            table_row,
            scan,
            session,
        })
    }

    /// Re-encodes the captured view into a scratch Cayenne table and returns
    /// its data directory and metastore slice for archiving. Runs after the
    /// write lock is released; `metadata_dirs` are the live metadata
    /// directories to archive alongside (minus `skip_metadata_files`).
    ///
    /// # Errors
    ///
    /// Fails when the scratch directory or metastore cannot be prepared, when
    /// the rewrite fails, or when the rewritten table is not the clean layout
    /// compaction promises — the snapshot is then not published.
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
        // empty snapshot. The overwrite below replaces that snapshot, so the
        // published slice carries the live `table_id`, schema, primary key,
        // `on_conflict` and sequence counter with a fresh, compact layout.
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
            // The sink reads one input partition; the planner normally inserts
            // this coalesce, but the captured plan bypasses the planner.
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
        // Post-write maintenance is detached from the write; wait for it so the
        // scratch layout is final before it is read and archived.
        provider
            .drain_in_flight_maintenance()
            .await
            .context(ScratchCatalogSnafu { dataset })?;
        drop(provider);

        // The overwrite path guarantees the clean layout; check it anyway, since
        // publishing a snapshot that merely looks compacted is worse than
        // failing this one.
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

        // The seed snapshot is superseded; the provider's own sweep may already
        // have removed it.
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
