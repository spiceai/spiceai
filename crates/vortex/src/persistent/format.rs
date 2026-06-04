// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::any::Any;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::sync::Arc;

use arrow_schema::Schema;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion_catalog::Session;
use datafusion_common::ColumnStatistics;
use datafusion_common::DataFusionError;
use datafusion_common::GetExt;
use datafusion_common::Result as DFResult;
use datafusion_common::Statistics;
use datafusion_common::config::ConfigField;
use datafusion_common::config_namespace;
use datafusion_common::internal_datafusion_err;
use datafusion_common::not_impl_err;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::stats::Precision;
use datafusion_common_runtime::SpawnedTask;
use datafusion_datasource::PartitionedFile;
use datafusion_datasource::TableSchema;
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_compression_type::FileCompressionType;
use datafusion_datasource::file_format::FileFormat;
use datafusion_datasource::file_format::FileFormatFactory;
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_scan_config::FileScanConfig;
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource::file_sink_config::FileSinkConfig;
use datafusion_datasource::sink::DataSinkExec;
use datafusion_datasource::source::DataSourceExec;
use datafusion_expr::dml::InsertOp;
use datafusion_physical_expr::LexRequirement;
use datafusion_physical_expr::PhysicalExprRef;
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::ExecutionPlanProperties;
use datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec;
use futures::FutureExt;
use futures::StreamExt as _;
use futures::TryStreamExt as _;
use futures::stream;
use object_store::ObjectMeta;
use object_store::ObjectStore;
use vortex::VortexSessionDefault;
use vortex::dtype::DType;
use vortex::dtype::Nullability;
use vortex::dtype::PType;
use vortex::dtype::arrow::FromArrowType;
use vortex::error::VortexResult;
use vortex::expr::stats;
use vortex::expr::stats::Stat;
use vortex::file::EOF_SIZE;
use vortex::file::MAX_POSTSCRIPT_SIZE;
use vortex::file::OpenOptionsSessionExt;
use vortex::file::VORTEX_FILE_EXTENSION;
use vortex::io::object_store::ObjectStoreReadAt;
use vortex::io::session::RuntimeSessionExt;
use vortex::scalar::Scalar;
use vortex::session::VortexSession;

use super::access_plan::VortexAccessPlanProvider;
use super::cache::CachedVortexMetadata;
use super::segment_cache::SharedSegmentCache;
use super::sink::{ShardSpec, VortexSink};
use super::source::VortexSource;
use crate::PrecisionExt as _;
use crate::convert::TryToDataFusion;
use datafusion_execution::cache::cache_manager::FileMetadata;

const DEFAULT_FOOTER_INITIAL_READ_SIZE_BYTES: usize = MAX_POSTSCRIPT_SIZE as usize + EOF_SIZE;
const DEFAULT_TARGET_FILE_SIZE_MB: usize = 128;

/// Controls projection-expression pushdown into Vortex scans.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ProjectionPushdown {
    /// Evaluate all projection expressions after the scan.
    #[default]
    Off,
    /// Push safe projection expressions into the scan and evaluate unsafe fragments after the scan.
    On,
    /// Let Vortex choose the safe projection-pushdown behavior for the scan.
    Auto,
}

impl ProjectionPushdown {
    /// Returns whether this mode enables safe projection-expression pushdown.
    #[must_use]
    pub fn enabled(self) -> bool {
        matches!(self, Self::On | Self::Auto)
    }

    /// Converts a legacy boolean projection-pushdown setting into the enum mode.
    #[must_use]
    pub fn from_bool(enabled: bool) -> Self {
        if enabled { Self::On } else { Self::Off }
    }
}

impl Display for ProjectionPushdown {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Off => f.write_str("off"),
            Self::On => f.write_str("on"),
            Self::Auto => f.write_str("auto"),
        }
    }
}

impl ConfigField for ProjectionPushdown {
    fn visit<V: datafusion_common::config::Visit>(
        &self,
        v: &mut V,
        key: &str,
        description: &'static str,
    ) {
        v.some(key, self, description);
    }

    fn set(&mut self, key: &str, value: &str) -> DFResult<()> {
        if !key.is_empty() {
            return Err(DataFusionError::Configuration(format!(
                "Config field projection_pushdown is a scalar and does not have nested field {key}"
            )));
        }

        *self = match value.trim().to_ascii_lowercase().as_str() {
            "auto" => Self::Auto,
            "on" | "enabled" | "true" | "1" => Self::On,
            "off" | "disabled" | "false" | "0" => Self::Off,
            value => {
                return Err(DataFusionError::Configuration(format!(
                    "Invalid projection_pushdown value {value:?}; expected 'auto', 'on', or 'off'"
                )));
            }
        };

        Ok(())
    }

    fn reset(&mut self, key: &str) -> DFResult<()> {
        if key.is_empty() {
            *self = Self::default();
            Ok(())
        } else {
            Err(DataFusionError::Configuration(format!(
                "Config field projection_pushdown is a scalar and does not have nested field {key}"
            )))
        }
    }
}

/// Controls intra-file Vortex scan concurrency.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ScanConcurrency {
    /// Derive per-file scan concurrency from `DataFusion` target partitions and planned file count.
    #[default]
    Auto,
    /// Force serial processing within each Vortex file scan.
    Off,
    /// Use an explicit concurrency value for every Vortex file scan.
    Explicit(usize),
}

impl Display for ScanConcurrency {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Auto => f.write_str("auto"),
            Self::Off => f.write_str("off"),
            Self::Explicit(value) => write!(f, "{value}"),
        }
    }
}

impl ConfigField for ScanConcurrency {
    fn visit<V: datafusion_common::config::Visit>(
        &self,
        v: &mut V,
        key: &str,
        description: &'static str,
    ) {
        v.some(key, self, description);
    }

    fn set(&mut self, key: &str, value: &str) -> DFResult<()> {
        if !key.is_empty() {
            return Err(DataFusionError::Configuration(format!(
                "Config field scan_concurrency is a scalar and does not have nested field {key}"
            )));
        }

        *self = match value.trim().to_ascii_lowercase().as_str() {
            "auto" => Self::Auto,
            "off" | "disabled" | "none" | "0" => Self::Off,
            value => {
                let concurrency = value.parse::<usize>().map_err(|err| {
                    DataFusionError::Configuration(format!(
                        "Invalid scan_concurrency value {value:?}; expected 'auto', 'off', or a positive integer: {err}"
                    ))
                })?;
                if concurrency == 0 {
                    Self::Off
                } else {
                    Self::Explicit(concurrency)
                }
            }
        };

        Ok(())
    }

    fn reset(&mut self, key: &str) -> DFResult<()> {
        if key.is_empty() {
            *self = Self::default();
            Ok(())
        } else {
            Err(DataFusionError::Configuration(format!(
                "Config field scan_concurrency is a scalar and does not have nested field {key}"
            )))
        }
    }
}

/// Programmatic write-time sharding configuration, set by the caller (e.g. the
/// Cayenne accelerator) via [`VortexFormat::with_write_shard`].
///
/// Absent (or `write_concurrency <= 1`) ⇒ a single serial writer, i.e. the
/// historical behavior. When present, `VortexSink` fans the write across
/// `write_concurrency` concurrent shard writers (clamped to the session's
/// `target_partitions`), routing rows round-robin or hashed by
/// `shard_key_columns`.
#[derive(Debug, Clone, Default)]
pub struct WriteShardConfig {
    /// Number of concurrent shard writers to fan a single write across.
    pub write_concurrency: usize,
    /// Optional key columns to hash-partition rows by (e.g. primary key or
    /// partition value), resolved by name against the write schema. Empty ⇒
    /// round-robin distribution.
    pub shard_key_columns: Vec<String>,
}

/// Vortex implementation of a `DataFusion` [`FileFormat`].
pub struct VortexFormat {
    session: VortexSession,
    opts: VortexTableOptions,
    access_plan_provider: Option<Arc<dyn VortexAccessPlanProvider>>,
    segment_cache: Option<Arc<SharedSegmentCache>>,
    write_shard: Option<WriteShardConfig>,
}

impl Debug for VortexFormat {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VortexFormat")
            .field("opts", &self.opts)
            .field(
                "access_plan_provider",
                &self.access_plan_provider.as_ref().map(|_| "configured"),
            )
            .field("segment_cache", &self.segment_cache)
            .finish_non_exhaustive()
    }
}

config_namespace! {
    /// Options to configure the [`VortexFormat`].
    ///
    /// Can be set through a DataFusion [`SessionConfig`].
    ///
    /// [`SessionConfig`]: https://docs.rs/datafusion/latest/datafusion/prelude/struct.SessionConfig.html
    pub struct VortexTableOptions {
        /// The number of bytes to read when parsing a file footer.
        ///
        /// Values smaller than `MAX_POSTSCRIPT_SIZE + EOF_SIZE` will be clamped to that minimum
        /// during footer parsing.
        pub footer_initial_read_size_bytes: usize, default = DEFAULT_FOOTER_INITIAL_READ_SIZE_BYTES
        /// Target file size in megabytes for written Vortex files.
        ///
        /// When greater than 0 for non-partitioned writes, Vortex bypasses
        /// DataFusion's file demuxer and splits output files based on
        /// approximate byte size rather than row count.
        pub target_file_size_mb: usize, default = DEFAULT_TARGET_FILE_SIZE_MB
        /// Projection pushdown behavior for the underlying Vortex scan.
        ///
        /// Accepted values are `off`, `on`, or `auto`. `off` reads referenced columns
        /// and evaluates all projection expressions after the scan. `on` and `auto`
        /// push safe projection expressions into the scan while keeping unsafe
        /// fragments above the scan.
        pub projection_pushdown: ProjectionPushdown, default = ProjectionPushdown::Off
        /// The intra-file scan concurrency, controlling the number of row splits to process
        /// concurrently within each file.
        ///
        /// Accepted values are `auto`, `off`, or a positive integer. In `auto` mode, Vortex derives
        /// per-file scan concurrency from DataFusion's target partitions and the number of planned
        /// files in the scan.
        pub scan_concurrency: ScanConcurrency, default = ScanConcurrency::Auto
        /// Total byte capacity for a path-aware segment cache shared by scans using this format.
        pub segment_cache_size_bytes: Option<usize>, default = None
    }
}

impl Eq for VortexTableOptions {}

/// Minimal factory to create [`VortexFormat`] instances.
#[derive(Debug)]
pub struct VortexFormatFactory {
    session: VortexSession,
    options: Option<VortexTableOptions>,
}

impl GetExt for VortexFormatFactory {
    fn get_ext(&self) -> String {
        VORTEX_FILE_EXTENSION.to_string()
    }
}

impl VortexFormatFactory {
    /// Creates a new instance with a default [`VortexSession`] and default options.
    #[expect(
        clippy::new_without_default,
        reason = "FormatFactory defines `default` method, so having `Default` implementation is confusing"
    )]
    #[must_use]
    pub fn new() -> Self {
        Self {
            session: VortexSession::default(),
            options: None,
        }
    }

    /// Creates a new instance with customized session and default options for all [`VortexFormat`] instances created from this factory.
    ///
    /// The options can be overridden by table-level configuration pass in [`FileFormatFactory::create`].
    #[must_use]
    pub fn new_with_options(session: VortexSession, options: VortexTableOptions) -> Self {
        Self {
            session,
            options: Some(options),
        }
    }

    /// Override the default options for this factory.
    ///
    /// For example:
    /// ```rust
    /// use vortex_datafusion::{VortexFormatFactory, VortexTableOptions};
    ///
    /// let factory = VortexFormatFactory::new().with_options(VortexTableOptions::default());
    /// ```
    #[must_use]
    pub fn with_options(mut self, options: VortexTableOptions) -> Self {
        self.options = Some(options);
        self
    }
}

impl FileFormatFactory for VortexFormatFactory {
    fn create(
        &self,
        _state: &dyn Session,
        format_options: &std::collections::HashMap<String, String>,
    ) -> DFResult<Arc<dyn FileFormat>> {
        let mut opts = self.options.clone().unwrap_or_default();
        for (key, value) in format_options {
            if let Some(key) = key.strip_prefix("format.") {
                opts.set(key, value)?;
            } else {
                tracing::trace!("Ignoring options '{key}'");
            }
        }

        Ok(Arc::new(VortexFormat::new_with_options(
            self.session.clone(),
            opts,
        )))
    }

    fn default(&self) -> Arc<dyn FileFormat> {
        Arc::new(VortexFormat::new(self.session.clone()))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl VortexFormat {
    /// Create a new instance with default options.
    #[must_use]
    pub fn new(session: VortexSession) -> Self {
        Self::new_with_options(session, VortexTableOptions::default())
    }

    /// Creates a new instance with configured by a [`VortexTableOptions`].
    #[must_use]
    pub fn new_with_options(session: VortexSession, opts: VortexTableOptions) -> Self {
        let segment_cache = opts
            .segment_cache_size_bytes
            .and_then(|bytes| u64::try_from(bytes).ok())
            .filter(|bytes| *bytes > 0)
            .map(|bytes| Arc::new(SharedSegmentCache::new(bytes, None)));

        Self {
            session,
            opts,
            access_plan_provider: None,
            segment_cache,
            write_shard: None,
        }
    }

    /// Return the format specific configuration
    #[must_use]
    pub fn options(&self) -> &VortexTableOptions {
        &self.opts
    }

    /// Creates a format that attaches access plans and adjusts footer-derived
    /// statistics using the provided provider.
    #[must_use]
    pub fn with_access_plan_provider(
        &self,
        access_plan_provider: Arc<dyn VortexAccessPlanProvider>,
    ) -> Self {
        Self {
            session: self.session.clone(),
            opts: self.opts.clone(),
            access_plan_provider: Some(access_plan_provider),
            segment_cache: self.segment_cache.clone(),
            write_shard: self.write_shard.clone(),
        }
    }

    /// Returns a format that fans writes across `config.write_concurrency`
    /// concurrent shard writers (clamped to the session `target_partitions`),
    /// routing rows hashed by `config.shard_key_columns` (or round-robin when
    /// empty). Used by the Cayenne accelerator to parallelize the Vortex encode.
    #[must_use]
    pub fn with_write_shard(&self, config: WriteShardConfig) -> Self {
        Self {
            session: self.session.clone(),
            opts: self.opts.clone(),
            access_plan_provider: self.access_plan_provider.clone(),
            segment_cache: self.segment_cache.clone(),
            write_shard: Some(config),
        }
    }

    /// Returns a format whose segment cache reports its right-sizing metrics
    /// (hit rate, fill) under the given `dataset` label. Rebuilds the (empty)
    /// segment cache to attach the label, so call once at construction before any
    /// scans run. No-op label-wise when this format has no segment cache.
    #[must_use]
    pub fn with_dataset_label(&self, dataset: impl Into<Arc<str>>) -> Self {
        let dataset = dataset.into();
        let segment_cache = self
            .opts
            .segment_cache_size_bytes
            .and_then(|bytes| u64::try_from(bytes).ok())
            .filter(|bytes| *bytes > 0)
            .map(|bytes| Arc::new(SharedSegmentCache::new(bytes, Some(Arc::clone(&dataset)))));
        Self {
            session: self.session.clone(),
            opts: self.opts.clone(),
            access_plan_provider: self.access_plan_provider.clone(),
            segment_cache,
            write_shard: self.write_shard.clone(),
        }
    }

    /// The configured intra-write shard config, if write sharding is enabled for
    /// this format (set via [`Self::with_write_shard`]). Read-only; primarily for
    /// inspection and tests.
    #[must_use]
    pub fn write_shard(&self) -> Option<&WriteShardConfig> {
        self.write_shard.as_ref()
    }

    /// Resolve the programmatic [`WriteShardConfig`] into a [`ShardSpec`] for
    /// the write schema. `None` / `write_concurrency <= 1` ⇒ `Single`; an empty
    /// key list ⇒ `RoundRobin`; otherwise `Hash` on the named columns. Unknown
    /// column names fall back to `RoundRobin` rather than failing the write.
    fn build_shard_spec(&self, schema: &SchemaRef, target_partitions: usize) -> ShardSpec {
        let Some(write_shard) = self.write_shard.as_ref() else {
            return ShardSpec::Single;
        };
        let partitions = write_shard
            .write_concurrency
            .clamp(1, target_partitions.max(1));
        if partitions <= 1 {
            return ShardSpec::Single;
        }
        if write_shard.shard_key_columns.is_empty() {
            return ShardSpec::RoundRobin(partitions);
        }
        let mut exprs: Vec<PhysicalExprRef> =
            Vec::with_capacity(write_shard.shard_key_columns.len());
        for name in &write_shard.shard_key_columns {
            if let Ok(idx) = schema.index_of(name) {
                exprs.push(Arc::new(Column::new(name, idx)));
            } else {
                // Defensive: the configured key column is absent from the write
                // schema. Degrade to round-robin rather than fail the write, but
                // warn — files silently lose key-clustering.
                tracing::warn!(
                    column = name.as_str(),
                    "Vortex write shard key column not found in output schema; \
                     falling back to round-robin sharding (files will not be key-clustered)"
                );
                return ShardSpec::RoundRobin(partitions);
            }
        }
        ShardSpec::Hash { exprs, partitions }
    }
}

fn attach_access_plans_to_config(
    mut config: FileScanConfig,
    provider: &dyn VortexAccessPlanProvider,
) -> FileScanConfig {
    config.file_groups = config
        .file_groups
        .into_iter()
        .map(|file_group| {
            let files = file_group
                .into_inner()
                .into_iter()
                .map(|file| attach_access_plan_to_file(file, provider))
                .collect::<Vec<_>>();
            FileGroup::new(files)
        })
        .collect();

    config
}

fn attach_access_plan_to_file(
    file: PartitionedFile,
    provider: &dyn VortexAccessPlanProvider,
) -> PartitionedFile {
    if let Some(access_plan) = provider.access_plan_for_file(&file) {
        file.with_extensions(access_plan)
    } else {
        file
    }
}

#[async_trait]
impl FileFormat for VortexFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn compression_type(&self) -> Option<FileCompressionType> {
        None
    }

    fn get_ext(&self) -> String {
        VORTEX_FILE_EXTENSION.to_string()
    }

    fn get_ext_with_compression(
        &self,
        file_compression_type: &FileCompressionType,
    ) -> DFResult<String> {
        match file_compression_type.get_variant() {
            CompressionTypeVariant::UNCOMPRESSED => Ok(self.get_ext()),
            _ => Err(DataFusionError::Internal(
                "Vortex does not support file level compression.".into(),
            )),
        }
    }

    async fn infer_schema(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> DFResult<SchemaRef> {
        let file_metadata_cache = state.runtime_env().cache_manager.get_file_metadata_cache();

        let mut file_schemas = stream::iter(objects.iter().cloned())
            .map(|object| {
                let store = Arc::clone(store);
                let session = self.session.clone();
                let opts = self.opts.clone();
                let cache = Arc::clone(&file_metadata_cache);

                SpawnedTask::spawn(async move {
                    // Check if we have cached metadata for this file
                    if let Some(cached) = cache.get(&object)
                        && let Some(cached_vortex) =
                            cached.as_any().downcast_ref::<CachedVortexMetadata>()
                    {
                        let inferred_schema = cached_vortex.footer().dtype().to_arrow_schema()?;
                        return VortexResult::Ok((object.location, inferred_schema));
                    }

                    // Not cached or invalid - open the file
                    let reader = Arc::new(ObjectStoreReadAt::new(
                        store,
                        object.location.clone(),
                        session.handle(),
                    ));

                    let vxf = session
                        .open_options()
                        .with_initial_read_size(opts.footer_initial_read_size_bytes)
                        .with_file_size(object.size)
                        .open_read(reader)
                        .await?;

                    // Cache the metadata
                    let cached_metadata = Arc::new(CachedVortexMetadata::new(&vxf));
                    // Footer-cache right-sizing telemetry: the accounted footer
                    // size (what fills the FileMetadataCache budget) per file.
                    tracing::info!(
                        target: "vortex::footer_cache",
                        path = %object.location,
                        footer_bytes = cached_metadata.memory_size(),
                        src = "infer_schema",
                        "footer cached",
                    );
                    cache.put(&object, cached_metadata);

                    let inferred_schema = vxf.dtype().to_arrow_schema()?;
                    VortexResult::Ok((object.location, inferred_schema))
                })
                .map(|result| -> DFResult<_> {
                    result
                        .map_err(|e| {
                            DataFusionError::Execution(format!(
                                "Failed to join Vortex infer_schema task: {e}"
                            ))
                        })?
                        .map_err(|e| {
                            DataFusionError::Execution(format!("Failed to infer schema: {e}"))
                        })
                })
            })
            .buffer_unordered(state.config_options().execution.meta_fetch_concurrency)
            .try_collect::<Vec<_>>()
            .await?;

        // Get consistent order of schemas for `Schema::try_merge`, as some filesystems don't have deterministic listing orders
        file_schemas.sort_by(|(l1, _), (l2, _)| l1.cmp(l2));
        let file_schemas = file_schemas.into_iter().map(|(_, schema)| schema);

        Ok(Arc::new(Schema::try_merge(file_schemas)?))
    }

    #[tracing::instrument(skip_all, fields(location = object.location.as_ref()))]
    async fn infer_stats(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> DFResult<Statistics> {
        let object = object.clone();
        let object_for_adjustment = object.clone();
        let store = Arc::clone(store);
        let session = self.session.clone();
        let opts = self.opts.clone();
        let file_metadata_cache = state.runtime_env().cache_manager.get_file_metadata_cache();

        let statistics = SpawnedTask::spawn(async move {
            // Try to get cached metadata first
            let cached_metadata = file_metadata_cache.get(&object).and_then(|cached| {
                cached
                    .as_any()
                    .downcast_ref::<CachedVortexMetadata>()
                    .map(|m| {
                        (
                            m.footer().dtype().clone(),
                            m.footer().statistics().cloned(),
                            m.footer().row_count(),
                        )
                    })
            });

            let (dtype, file_stats, row_count) = if let Some(metadata) = cached_metadata {
                metadata
            } else {
                // Not cached - open the file
                let reader = Arc::new(ObjectStoreReadAt::new(
                    store,
                    object.location.clone(),
                    session.handle(),
                ));

                let vxf = session
                    .open_options()
                    .with_initial_read_size(opts.footer_initial_read_size_bytes)
                    .with_file_size(object.size)
                    .open_read(reader)
                    .await
                    .map_err(|e| {
                        DataFusionError::Execution(format!(
                            "Failed to open Vortex file {}: {e}",
                            object.location
                        ))
                    })?;

                // Cache the metadata
                let cached = Arc::new(CachedVortexMetadata::new(&vxf));
                // Footer-cache right-sizing telemetry (see infer_schema above).
                tracing::info!(
                    target: "vortex::footer_cache",
                    path = %object.location,
                    footer_bytes = cached.memory_size(),
                    src = "infer_stats",
                    "footer cached",
                );
                file_metadata_cache.put(&object, cached);

                (
                    vxf.dtype().clone(),
                    vxf.file_stats().cloned(),
                    vxf.row_count(),
                )
            };

            let Some(struct_dtype) = dtype.as_struct_fields_opt() else {
                return Err(DataFusionError::Execution(format!(
                    "Failed to infer statistics for Vortex file {}: file dtype is not a struct",
                    object.location
                )));
            };

            let num_rows = usize::try_from(row_count).map_err(|_| {
                DataFusionError::Execution(format!(
                    "Failed to infer statistics for Vortex file {}: row count {row_count} cannot be represented as usize",
                    object.location
                ))
            })?;

            // Evaluate the statistics for each column that we are able to return to DataFusion.
            let Some(file_stats) = file_stats else {
                // If the file has no column stats, the best we can do is return a row count.
                return Ok::<Statistics, DataFusionError>(Statistics {
                    num_rows: Precision::Exact(num_rows),
                    total_byte_size: Precision::Absent,
                    column_statistics: vec![
                        ColumnStatistics::default();
                        table_schema.fields().len()
                    ],
                });
            };

            let mut sum_of_column_byte_sizes = stats::Precision::exact(0_usize);
            let mut column_statistics = Vec::with_capacity(table_schema.fields().len());

            for field in table_schema.fields() {
                // If the column does not exist, continue. This can happen if the schema has evolved
                // but we have not yet updated the Vortex file.
                let Some(col_idx) = struct_dtype.find(field.name()) else {
                    // The default sets all statistics to `Precision<Absent>`.
                    column_statistics.push(ColumnStatistics::default());
                    continue;
                };
                let (stats_set, stats_dtype) = file_stats.get(col_idx);

                // Update the total size in bytes.
                let column_size = stats_set
                    .get_as::<usize>(Stat::UncompressedSizeInBytes, &PType::U64.into())
                    .unwrap_or_else(|| stats::Precision::inexact(0_usize));
                sum_of_column_byte_sizes = sum_of_column_byte_sizes
                    .zip(column_size)
                    .map(|(acc, size)| acc + size);

                // TODO(connor): There's a lot that can go wrong here, should probably handle this
                // more gracefully...
                // Find the min statistic.
                let min = stats_set.get(Stat::Min).and_then(|pstat_val| {
                    pstat_val
                        .map(|stat_val| {
                            // Because of DataFusion's Schema evolution, it is possible that the
                            // type of the min/max stat has changed. Thus we construct the stat as
                            // the file datatype first and only then do we cast accordingly.
                            let stat_dtype = Stat::Min.dtype(stats_dtype)?;
                            Scalar::try_new(stat_dtype, Some(stat_val))
                                .ok()?
                                .cast(&DType::from_arrow(field.as_ref()))
                                .ok()?
                                .try_to_df()
                                .ok()
                        })
                        .transpose()
                });

                // Find the max statistic.
                let max = stats_set.get(Stat::Max).and_then(|pstat_val| {
                    pstat_val
                        .map(|stat_val| {
                            let stat_dtype = Stat::Max.dtype(stats_dtype)?;
                            Scalar::try_new(stat_dtype, Some(stat_val))
                                .ok()?
                                .cast(&DType::from_arrow(field.as_ref()))
                                .ok()?
                                .try_to_df()
                                .ok()
                        })
                        .transpose()
                });

                let null_count = stats_set.get_as::<usize>(Stat::NullCount, &PType::U64.into());

                column_statistics.push(ColumnStatistics {
                    null_count: null_count.to_df(),
                    min_value: min.to_df(),
                    max_value: max.to_df(),
                    sum_value: Precision::Absent,
                    distinct_count: distinct_count_from_is_constant(stats_set.get_as::<bool>(
                        Stat::IsConstant,
                        &DType::Bool(Nullability::NonNullable),
                    )),
                    // TODO(connor): Is this correct?
                    byte_size: column_size.to_df(),
                });
            }

            let total_byte_size = sum_of_column_byte_sizes.to_df();

            Ok::<Statistics, DataFusionError>(Statistics {
                num_rows: Precision::Exact(num_rows),
                total_byte_size,
                column_statistics,
            })
        })
        .await
        .map_err(|e| {
            DataFusionError::Execution(format!("Failed to join Vortex infer_stats task: {e}"))
        })??;

        if let Some(provider) = self.access_plan_provider.as_ref() {
            Ok(provider.adjust_statistics(&object_for_adjustment, statistics))
        } else {
            Ok(statistics)
        }
    }

    async fn create_physical_plan(
        &self,
        state: &dyn Session,
        mut file_scan_config: FileScanConfig,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if let Some(provider) = self.access_plan_provider.as_ref() {
            file_scan_config = attach_access_plans_to_config(file_scan_config, provider.as_ref());
        }

        let mut source = file_scan_config
            .file_source()
            .as_any()
            .downcast_ref::<VortexSource>()
            .cloned()
            .ok_or_else(|| internal_datafusion_err!("Expected VortexSource"))?;

        source = source
            .with_file_metadata_cache(state.runtime_env().cache_manager.get_file_metadata_cache());

        let conf = FileScanConfigBuilder::from(file_scan_config)
            .with_source(Arc::new(source))
            .build();

        Ok(DataSourceExec::from_data_source(conf))
    }

    async fn create_writer_physical_plan(
        &self,
        input: Arc<dyn ExecutionPlan>,
        state: &dyn Session,
        conf: FileSinkConfig,
        order_requirements: Option<LexRequirement>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if conf.insert_op != InsertOp::Append {
            return not_impl_err!("Overwrites are not implemented yet for Vortex");
        }

        let target_file_size = (self.opts.target_file_size_mb > 0)
            .then(|| {
                u64::try_from(self.opts.target_file_size_mb)
                    .map_err(|e| {
                        internal_datafusion_err!(
                            "target_file_size_mb cannot be represented as u64: {e}"
                        )
                    })
                    .map(|v| v.saturating_mul(1024 * 1024).max(1))
            })
            .transpose()?;

        // For non-partitioned writes, force a single input stream so VortexSink
        // performs one coordinated write per statement instead of one
        // independent write per CPU/input partition.
        //
        // Use coalescing rather than repartitioning to avoid introducing a
        // shuffle/dispatcher step that can interleave batches from different
        // input partitions.
        //
        // For partitioned writes, keep DataFusion's demuxer behavior.
        let input: Arc<dyn ExecutionPlan> = if conf.table_partition_cols.is_empty()
            && input.output_partitioning().partition_count() > 1
        {
            Arc::new(CoalescePartitionsExec::new(input))
        } else {
            input
        };

        let schema = Arc::clone(conf.output_schema());
        let target_partitions = state.config().target_partitions().max(1);
        let shard_spec = self.build_shard_spec(&schema, target_partitions);
        let sink = Arc::new(VortexSink::new(
            conf,
            schema,
            self.session.clone(),
            target_file_size,
            shard_spec,
        ));

        Ok(Arc::new(DataSinkExec::new(input, sink, order_requirements)) as _)
    }

    fn file_source(&self, table_schema: TableSchema) -> Arc<dyn FileSource> {
        let mut source = VortexSource::new(table_schema, self.session.clone())
            .with_projection_pushdown(self.opts.projection_pushdown)
            .with_scan_concurrency(self.opts.scan_concurrency);

        if let Some(segment_cache) = self.segment_cache.clone() {
            source = source.with_segment_cache(segment_cache);
        }

        Arc::new(source) as _
    }
}

fn distinct_count_from_is_constant(
    is_constant: Option<stats::Precision<bool>>,
) -> Precision<usize> {
    match is_constant.and_then(stats::Precision::as_exact) {
        Some(true) => Precision::Exact(1),
        Some(false) | None => Precision::Absent,
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::common_tests::TestSessionContext;

    #[tokio::test]
    async fn create_table() -> anyhow::Result<()> {
        let ctx = TestSessionContext::default();

        ctx.session
            .sql(
                "CREATE EXTERNAL TABLE my_tbl \
                (c1 VARCHAR NOT NULL, c2 INT NOT NULL) \
                STORED AS vortex  \
                LOCATION 'table/'",
            )
            .await?;

        assert!(ctx.session.table_exist("my_tbl")?);

        Ok(())
    }

    #[tokio::test]
    async fn configure_format_source() -> anyhow::Result<()> {
        let ctx = TestSessionContext::default();

        ctx.session
            .sql(
                "CREATE EXTERNAL TABLE my_tbl \
                (c1 VARCHAR NOT NULL, c2 INT NOT NULL) \
                STORED AS vortex \
                LOCATION 'table/' \
                OPTIONS( footer_initial_read_size_bytes '12345', scan_concurrency '3' );",
            )
            .await?
            .collect()
            .await?;

        Ok(())
    }

    #[test]
    fn format_plumbs_footer_initial_read_size() {
        let mut opts = VortexTableOptions::default();
        opts.set("footer_initial_read_size_bytes", "12345")
            .expect("setting footer_initial_read_size_bytes should succeed");

        let format = VortexFormat::new_with_options(VortexSession::default(), opts);
        assert_eq!(format.options().footer_initial_read_size_bytes, 12345);
    }

    fn schema_with(cols: &[(&str, arrow_schema::DataType)]) -> SchemaRef {
        Arc::new(Schema::new(
            cols.iter()
                .map(|(n, t)| arrow_schema::Field::new(*n, t.clone(), false))
                .collect::<Vec<_>>(),
        ))
    }

    fn shard_format(write_concurrency: usize, keys: &[&str]) -> VortexFormat {
        VortexFormat::new(VortexSession::default()).with_write_shard(WriteShardConfig {
            write_concurrency,
            shard_key_columns: keys.iter().map(|s| (*s).to_string()).collect(),
        })
    }

    #[test]
    fn build_shard_spec_without_write_shard_is_single() {
        let schema = schema_with(&[("k", arrow_schema::DataType::Int64)]);
        let format = VortexFormat::new(VortexSession::default());
        assert!(matches!(
            format.build_shard_spec(&schema, 8),
            ShardSpec::Single
        ));
    }

    #[test]
    fn build_shard_spec_clamps_write_concurrency_to_target_partitions() {
        let schema = schema_with(&[("k", arrow_schema::DataType::Int64)]);
        // Asking for 100 shards with target_partitions=4 must clamp to 4.
        match shard_format(100, &["k"]).build_shard_spec(&schema, 4) {
            ShardSpec::Hash { partitions, .. } => assert_eq!(partitions, 4),
            _ => panic!("expected Hash with clamped partitions=4"),
        }
    }

    #[test]
    fn build_shard_spec_one_partition_is_single() {
        let schema = schema_with(&[("k", arrow_schema::DataType::Int64)]);
        // write_concurrency 8 but target_partitions 1 → clamp to 1 → Single.
        assert!(matches!(
            shard_format(8, &["k"]).build_shard_spec(&schema, 1),
            ShardSpec::Single
        ));
    }

    #[test]
    fn build_shard_spec_no_keys_is_round_robin() {
        let schema = schema_with(&[("k", arrow_schema::DataType::Int64)]);
        assert!(matches!(
            shard_format(4, &[]).build_shard_spec(&schema, 8),
            ShardSpec::RoundRobin(4)
        ));
    }

    #[test]
    fn build_shard_spec_unknown_key_falls_back_to_round_robin() {
        let schema = schema_with(&[("k", arrow_schema::DataType::Int64)]);
        // "missing" is absent from the schema → degrade to round-robin (warns),
        // never silently produce a Hash over a bogus column.
        assert!(matches!(
            shard_format(4, &["missing"]).build_shard_spec(&schema, 8),
            ShardSpec::RoundRobin(4)
        ));
    }

    #[test]
    fn build_shard_spec_composite_keys_hash_all_columns() {
        let schema = schema_with(&[
            ("w_id", arrow_schema::DataType::Int64),
            ("d_id", arrow_schema::DataType::Int64),
            ("payload", arrow_schema::DataType::Utf8),
        ]);
        match shard_format(4, &["w_id", "d_id"]).build_shard_spec(&schema, 8) {
            ShardSpec::Hash { exprs, partitions } => {
                assert_eq!(partitions, 4);
                assert_eq!(exprs.len(), 2, "composite key must hash both columns");
                let names: String = exprs.iter().map(ToString::to_string).collect();
                assert!(
                    names.contains("w_id") && names.contains("d_id"),
                    "hash exprs must reference both key columns, got: {names}"
                );
            }
            _ => panic!("expected Hash over the composite key"),
        }
    }

    #[test]
    fn format_plumbs_target_file_size_mb() {
        let mut opts = VortexTableOptions::default();
        opts.set("target_file_size_mb", "123")
            .expect("setting target_file_size_mb should succeed");

        let format = VortexFormat::new_with_options(VortexSession::default(), opts);
        assert_eq!(format.options().target_file_size_mb, 123);
    }

    #[test]
    fn format_target_file_size_default_is_128mb() {
        let opts = VortexTableOptions::default();
        assert_eq!(opts.target_file_size_mb, 128);
    }

    #[test]
    fn format_scan_concurrency_default_is_auto() {
        let opts = VortexTableOptions::default();
        assert_eq!(opts.scan_concurrency, ScanConcurrency::Auto);
    }

    #[test]
    fn format_projection_pushdown_default_is_off() {
        let opts = VortexTableOptions::default();
        assert_eq!(opts.projection_pushdown, ProjectionPushdown::Off);
    }

    #[test]
    fn format_plumbs_projection_pushdown_modes() {
        let mut opts = VortexTableOptions::default();
        opts.set("projection_pushdown", "auto")
            .expect("setting projection_pushdown to auto should succeed");
        assert_eq!(opts.projection_pushdown, ProjectionPushdown::Auto);
        assert!(opts.projection_pushdown.enabled());

        opts.set("projection_pushdown", "on")
            .expect("setting projection_pushdown to on should succeed");
        assert_eq!(opts.projection_pushdown, ProjectionPushdown::On);
        assert!(opts.projection_pushdown.enabled());

        opts.set("projection_pushdown", "true")
            .expect("setting projection_pushdown to true should succeed");
        assert_eq!(opts.projection_pushdown, ProjectionPushdown::On);

        opts.set("projection_pushdown", "off")
            .expect("setting projection_pushdown to off should succeed");
        assert_eq!(opts.projection_pushdown, ProjectionPushdown::Off);
        assert!(!opts.projection_pushdown.enabled());
    }

    #[test]
    fn format_plumbs_scan_concurrency_modes() {
        let mut opts = VortexTableOptions::default();
        opts.set("scan_concurrency", "auto")
            .expect("setting scan_concurrency to auto should succeed");
        assert_eq!(opts.scan_concurrency, ScanConcurrency::Auto);

        opts.set("scan_concurrency", "off")
            .expect("setting scan_concurrency to off should succeed");
        assert_eq!(opts.scan_concurrency, ScanConcurrency::Off);

        opts.set("scan_concurrency", "00")
            .expect("setting scan_concurrency to 00 should succeed");
        assert_eq!(opts.scan_concurrency, ScanConcurrency::Off);

        opts.set("scan_concurrency", "3")
            .expect("setting scan_concurrency to 3 should succeed");
        assert_eq!(opts.scan_concurrency, ScanConcurrency::Explicit(3));
    }

    #[test]
    fn distinct_count_is_exact_only_for_exact_constant_true() {
        assert_eq!(
            distinct_count_from_is_constant(Some(stats::Precision::exact(true))),
            Precision::Exact(1)
        );
        assert_eq!(
            distinct_count_from_is_constant(Some(stats::Precision::exact(false))),
            Precision::Absent
        );
        assert_eq!(
            distinct_count_from_is_constant(Some(stats::Precision::inexact(true))),
            Precision::Absent
        );
        assert_eq!(distinct_count_from_is_constant(None), Precision::Absent);
    }
}
