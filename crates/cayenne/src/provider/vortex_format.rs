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

//! Cayenne-specific wrapper around `VortexFormat` that supports per-file deletion vectors.
//!
//! This module provides [`CayenneVortexFormat`], a [`FileFormat`] implementation that wraps
//! the upstream `VortexFormat` and injects per-file deletion vectors via `VortexAccessPlan`
//! extensions on `PartitionedFile`s.
//!
//! # Deletion Vector Integration
//!
//! When scanning files, `CayenneVortexFormat` attaches a `VortexAccessPlan` to each
//! `PartitionedFile` based on the deletion cache. The `VortexOpener` then uses the
//! `Selection::ExcludeRoaring` to skip deleted rows during decompression, which is more
//! efficient than post-scan filtering.

use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use datafusion::datasource::file_format::FileFormat;
use datafusion_catalog::Session;
use datafusion_common::Result as DFResult;
use datafusion_common::Statistics;
use datafusion_datasource::file_compression_type::FileCompressionType;
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_scan_config::FileScanConfig;
use datafusion_datasource::PartitionedFile;
use datafusion_physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use object_store::{ObjectMeta, ObjectStore};
use roaring::{RoaringBitmap, RoaringTreemap};
use vortex_datafusion::{VortexAccessPlan, VortexFormat};
use vortex_scan::Selection;
/// A wrapper around `VortexFormat` that injects per-file deletion vectors.
///
/// This format delegates all operations to the underlying `VortexFormat`, except for
/// `create_physical_plan` where it attaches `VortexAccessPlan` extensions to files
/// that have deletion vectors.
pub struct DeletionFilteringVortexFormat {
    /// The underlying Vortex file format.
    inner: Arc<VortexFormat>,
    /// Per-file deletion cache. Key is the file path, value is the bitmap of deleted row indices.
    /// Uses `Arc<RwLock<...>>` to allow shared access across clones.
    deletion_cache: Arc<RwLock<Arc<HashMap<String, RoaringBitmap>>>>,
}

impl std::fmt::Debug for DeletionFilteringVortexFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeletionFilteringVortexFormat")
            .field("inner", &"VortexFormat")
            .finish()
    }
}

/// Attach `VortexAccessPlan` deletion vectors to files in a `FileScanConfig`.
///
/// This is a standalone function that can be used both by `CayenneVortexFormat::create_physical_plan`
/// and by external code (e.g., retention filter scanning in `delete.rs`) that needs to apply
/// deletion vectors to a file scan configuration.
///
/// # Arguments
///
/// * `config` - The file scan configuration to modify
/// * `deletion_cache` - Shared cache of per-file deletion vectors (file path -> deleted row indices)
///
/// # Returns
///
/// A tuple of:
/// - The modified `FileScanConfig` with `VortexAccessPlan` extensions attached to files with deletions
/// - A boolean indicating if any deletions were attached
///
/// # Errors
///
/// Returns an error if the deletion cache lock cannot be acquired.
#[expect(clippy::implicit_hasher)]
pub fn attach_deletion_vectors_to_config(
    mut config: FileScanConfig,
    deletion_cache: &RwLock<Arc<HashMap<String, RoaringBitmap>>>,
) -> DFResult<(FileScanConfig, bool)> {
    let deletion_map = {
        let guard = deletion_cache.read().map_err(|_| {
            datafusion_common::DataFusionError::Execution(
                "Failed to acquire deletion cache lock".to_string(),
            )
        })?;
        Arc::clone(&guard)
    };

    // If no deletions, return config unchanged
    if deletion_map.is_empty() {
        return Ok((config, false));
    }

    // Track if any files had deletions attached
    let mut has_any_deletions = false;

    // Modify file_groups in place to attach VortexAccessPlan extensions
    config.file_groups = config
        .file_groups
        .into_iter()
        .map(|file_group| {
            let modified_files: Vec<PartitionedFile> = file_group
                .into_inner()
                .into_iter()
                .map(|file| {
                    let (modified_file, had_deletion) =
                        attach_access_plan_to_file(file, &deletion_map);
                    if had_deletion {
                        has_any_deletions = true;
                    }
                    modified_file
                })
                .collect();
            FileGroup::new(modified_files)
        })
        .collect();

    Ok((config, has_any_deletions))
}

/// Attach a `VortexAccessPlan` to a single file if it has deletions.
///
/// This is a helper function used by `attach_deletion_vectors_to_config`.
///
/// # Arguments
///
/// * `file` - The partitioned file to potentially modify
/// * `deletion_map` - Map of file path to deletion bitmap
///
/// # Returns
///
/// A tuple of the (potentially modified) file and a boolean indicating if deletions were attached.
fn attach_access_plan_to_file(
    mut file: PartitionedFile,
    deletion_map: &HashMap<String, RoaringBitmap>,
) -> (PartitionedFile, bool) {
    // Extract the file path from the PartitionedFile
    let file_path = file.object_meta.location.to_string();

    // Check if this file has deletions
    if let Some(bitmap) = deletion_map.get(&file_path) {
        if !bitmap.is_empty() {
            // ExcludeRoaring is preferred over ExcludeByIndex: less memory (~2 bits vs 8 bytes/row)
            // and enables native bitmap operations in Vortex (intersection, is_disjoint) which is faster
            let exclude: RoaringTreemap = bitmap.iter().map(u64::from).collect();

            // Use Vortex built-in mechanism for exclusions
            let access_plan =
                VortexAccessPlan::default().with_selection(Selection::ExcludeRoaring(exclude));

            file = file.with_extensions(Arc::new(access_plan));

            tracing::trace!(
                file_path = %file_path,
                deleted_rows = bitmap.len(),
                "Attached VortexAccessPlan with deletion vector"
            );

            return (file, true);
        }
    }

    (file, false)
}

impl DeletionFilteringVortexFormat {
    /// Create a new `CayenneVortexFormat` wrapping the given `VortexFormat`.
    ///
    /// # Arguments
    ///
    /// * `inner` - The underlying `VortexFormat` to delegate to.
    /// * `deletion_cache` - Shared cache of per-file deletion vectors.
    pub fn new(
        inner: Arc<VortexFormat>,
        deletion_cache: Arc<RwLock<Arc<HashMap<String, RoaringBitmap>>>>,
    ) -> Self {
        Self {
            inner,
            deletion_cache,
        }
    }

    /// Attach `VortexAccessPlan` extensions to files with deletion vectors.
    ///
    /// This is a convenience method that delegates to [`attach_deletion_vectors_to_config`].
    fn attach_deletion_vectors(&self, config: FileScanConfig) -> DFResult<(FileScanConfig, bool)> {
        attach_deletion_vectors_to_config(config, &self.deletion_cache)
    }
}

#[async_trait]
impl FileFormat for DeletionFilteringVortexFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn compression_type(&self) -> Option<FileCompressionType> {
        self.inner.compression_type()
    }

    fn get_ext(&self) -> String {
        self.inner.get_ext()
    }

    fn get_ext_with_compression(
        &self,
        file_compression_type: &datafusion_datasource::file_compression_type::FileCompressionType,
    ) -> DFResult<String> {
        self.inner.get_ext_with_compression(file_compression_type)
    }

    async fn infer_schema(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> DFResult<arrow_schema::SchemaRef> {
        self.inner.infer_schema(state, store, objects).await
    }

    async fn infer_stats(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        table_schema: arrow_schema::SchemaRef,
        object: &ObjectMeta,
    ) -> DFResult<datafusion_common::Statistics> {
        let file_stats = self
            .inner
            .infer_stats(state, store, table_schema, object)
            .await?;

        // Check if there are any deletions for this file. If so, we need to return
        // inexact statistics to prevent DataFusion from using AggregateStatistics
        // optimization which would skip the actual scan and return wrong row counts.
        let file_path = object.location.to_string();
        let has_deletions = {
            let guard = self.deletion_cache.read().map_err(|_| {
                datafusion_common::DataFusionError::Execution(
                    "Failed to acquire deletion cache lock".to_string(),
                )
            })?;
            guard.contains_key(&file_path)
                && !guard.get(&file_path).is_some_and(RoaringBitmap::is_empty)
        };

        if has_deletions {
            // Convert exact statistics to inexact to force actual scanning
            Ok(datafusion_common::Statistics {
                num_rows: file_stats.num_rows.to_inexact(),
                total_byte_size: file_stats.total_byte_size,
                column_statistics: file_stats.column_statistics,
            })
        } else {
            Ok(file_stats)
        }
    }

    async fn create_physical_plan(
        &self,
        state: &dyn Session,
        file_scan_config: FileScanConfig,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        // Attach deletion vectors to files before creating the plan
        let (modified_config, has_any_deletions) =
            self.attach_deletion_vectors(file_scan_config)?;

        // Delegate to the underlying VortexFormat
        let plan = self
            .inner
            .create_physical_plan(state, modified_config)
            .await?;

        // If there are deletions, wrap the plan to force inexact statistics.
        // This prevents AggregateStatistics optimizer from short-circuiting
        // COUNT(*) queries using stale exact row counts.
        if has_any_deletions {
            Ok(Arc::new(InexactStatsExec::new(plan)))
        } else {
            Ok(plan)
        }
    }

    async fn create_writer_physical_plan(
        &self,
        input: Arc<dyn ExecutionPlan>,
        state: &dyn Session,
        conf: datafusion_datasource::file_sink_config::FileSinkConfig,
        order_requirements: Option<datafusion_physical_expr::LexRequirement>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        // Delegate to the underlying VortexFormat for writing
        self.inner
            .create_writer_physical_plan(input, state, conf, order_requirements)
            .await
    }

    fn file_source(&self) -> Arc<dyn datafusion_datasource::file::FileSource> {
        self.inner.file_source()
    }
}

/// A wrapper execution plan that forces inexact row count statistics.
///
/// This is used to wrap scan plans when there are deletions, preventing
/// `DataFusion`'s `AggregateStatistics` optimizer from short-circuiting
/// COUNT(*) queries using stale exact statistics.
#[derive(Debug)]
struct InexactStatsExec {
    inner: Arc<dyn ExecutionPlan>,
    properties: PlanProperties,
}

impl InexactStatsExec {
    fn new(inner: Arc<dyn ExecutionPlan>) -> Self {
        let properties = inner.properties().clone();
        Self { inner, properties }
    }
}

impl DisplayAs for InexactStatsExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "InexactStatsExec: ")?;
                self.inner.fmt_as(t, f)
            }
            DisplayFormatType::TreeRender => self.inner.fmt_as(t, f),
        }
    }
}

impl ExecutionPlan for InexactStatsExec {
    fn name(&self) -> &'static str {
        "InexactStatsExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.inner]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(datafusion_common::DataFusionError::Internal(
                "InexactStatsExec requires exactly one child".to_string(),
            ));
        }
        Ok(Arc::new(Self::new(Arc::clone(&children[0]))))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion_execution::TaskContext>,
    ) -> DFResult<datafusion_physical_plan::SendableRecordBatchStream> {
        self.inner.execute(partition, context)
    }

    fn partition_statistics(&self, partition: Option<usize>) -> DFResult<Statistics> {
        let stats = self.inner.partition_statistics(partition)?;
        // Convert exact row count to inexact to prevent AggregateStatistics optimization
        Ok(Statistics {
            num_rows: stats.num_rows.to_inexact(),
            total_byte_size: stats.total_byte_size,
            column_statistics: stats.column_statistics,
        })
    }
}
