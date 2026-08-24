// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::collections::HashSet;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::str::FromStr;
use std::sync::Arc;

use arrow_schema::Schema;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion_catalog::Session;
use datafusion_common::ColumnStatistics;
use datafusion_common::DataFusionError;
use datafusion_common::GetExt;
use datafusion_common::Result as DFResult;
use datafusion_common::ScalarValue;
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
use object_store::path::Path;
use vortex::VortexSessionDefault;
use vortex::arrow::ArrowSessionExt;
use vortex::arrow::FromArrowType;
use vortex::dtype::DType;
use vortex::dtype::Nullability;
use vortex::dtype::PType;
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
use vortex::scalar::ScalarValue as VortexScalarValue;
use vortex::session::VortexSession;

use super::access_plan::VortexAccessPlanProvider;
use super::cache::CachedVortexMetadata;
use super::cache::cache_footer;
use super::segment_cache;
use super::segment_cache::SharedSegmentCache;
use super::sink::{ShardSpec, VortexSink};
use super::source::VortexSource;
use crate::PrecisionExt as _;
use crate::convert::TryToDataFusion;

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

impl FromStr for ScanConcurrency {
    type Err = String;

    /// Parses `auto`, `off` (also `disabled`/`none`/`0`), or a positive integer.
    ///
    /// The single parser for this setting: `ConfigField::set` delegates here, so a
    /// caller reading the mode from its own configuration (e.g. a Spicepod
    /// parameter) accepts exactly the spellings a `DataFusion` `OPTIONS(...)`
    /// clause does.
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Ok(match value.trim().to_ascii_lowercase().as_str() {
            "auto" => Self::Auto,
            "off" | "disabled" | "none" | "0" => Self::Off,
            other => {
                let concurrency = other.parse::<usize>().map_err(|err| {
                    format!(
                        "Invalid scan_concurrency value {other:?}; expected 'auto', 'off', or a positive integer: {err}"
                    )
                })?;
                if concurrency == 0 {
                    Self::Off
                } else {
                    Self::Explicit(concurrency)
                }
            }
        })
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

        *self = value
            .parse::<Self>()
            .map_err(DataFusionError::Configuration)?;

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
    /// distribute whole batches instead of splitting them row-wise.
    pub shard_key_columns: Vec<String>,
    /// Ascending split points that RANGE-partition rows on the single
    /// `shard_key_columns` entry, giving each output file a disjoint, contiguous
    /// slice of that key's domain so a predicate on it prunes. `None` ⇒ hash the
    /// key instead, which spreads every key range across every file.
    ///
    /// Supply `write_concurrency - 1` bounds. Ignored unless exactly one shard
    /// key column is set: ordering a composite key needs a lexicographic
    /// comparison this does not implement, so a multi-column key hashes.
    pub range_bounds: Option<Vec<ScalarValue>>,
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
    /// Names the segment cache a created format may size for itself, so its
    /// metrics identify the table rather than a bare sequence number.
    cache_name: Option<Arc<str>>,
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
            cache_name: None,
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
            cache_name: None,
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

    /// Name the segment cache a created format may size for itself.
    ///
    /// Only reached when the table sets `segment_cache_size_bytes`; without a
    /// name such a cache reports under a sequence number, which tells an operator
    /// that a cache exists but not which table owns it.
    #[must_use]
    pub fn with_cache_name(mut self, name: impl Into<Arc<str>>) -> Self {
        self.cache_name = Some(name.into());
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

        Ok(Arc::new(VortexFormat::new_with_options_named(
            self.session.clone(),
            opts,
            self.cache_name.clone(),
        )))
    }

    fn default(&self) -> Arc<dyn FileFormat> {
        Arc::new(VortexFormat::new_with_options_named(
            self.session.clone(),
            self.options.clone().unwrap_or_default(),
            self.cache_name.clone(),
        ))
    }
}

impl VortexFormat {
    /// Create a new instance with default options.
    #[must_use]
    pub fn new(session: VortexSession) -> Self {
        Self::new_with_options(session, VortexTableOptions::default())
    }

    /// Creates a new instance with configured by a [`VortexTableOptions`].
    ///
    /// Scans cache segments only when `segment_cache_size_bytes` asks for it. The
    /// process-wide cache is opt-in through [`Self::new_with_process_segment_cache`],
    /// because caching is only sound for a caller whose file paths are immutable.
    #[must_use]
    pub fn new_with_options(session: VortexSession, opts: VortexTableOptions) -> Self {
        Self::new_with_options_named(session, opts, None)
    }

    /// Like [`Self::new_with_options`], but a cache built from
    /// `segment_cache_size_bytes` reports under `name` instead of a bare
    /// sequence number. Callers that know which table they are opening — the
    /// listing connector does — should pass it.
    #[must_use]
    pub fn new_with_options_named(
        session: VortexSession,
        opts: VortexTableOptions,
        cache_name: Option<Arc<str>>,
    ) -> Self {
        let self_cache_name = cache_name;
        let segment_cache = opts
            .segment_cache_size_bytes
            .and_then(|bytes| u64::try_from(bytes).ok())
            .filter(|bytes| *bytes > 0)
            .map(|bytes| SharedSegmentCache::new_private(bytes, self_cache_name.clone()));

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

    /// Invalidates cached Vortex segments for the exact object-store paths,
    /// evicting the ones it can reach before returning.
    ///
    /// Both waits this takes — for the writes already in flight on those paths,
    /// and for the search that finds their cached keys — are bounded, so a host
    /// too saturated to finish them gives up rather than holding this caller.
    /// Returning therefore means the wait is over, not always that every segment
    /// is gone. Giving up on the in-flight writes costs only a moment of
    /// residency; giving up on the search is what leaves segments cached until
    /// capacity evicts them. Neither can serve stale data, because every caller
    /// has already deleted the underlying file.
    pub async fn invalidate_segment_cache_paths(&self, paths: HashSet<Path>) {
        if let Some(cache) = self.segment_cache.as_ref() {
            cache.invalidate_paths(paths).await;
        }
    }

    /// Returns the current number of cached Vortex segments, or `None` when the
    /// segment cache is disabled.
    pub async fn segment_cache_entry_count(&self) -> Option<u64> {
        match self.segment_cache.as_ref() {
            Some(cache) => Some(cache.entry_count().await),
            None => None,
        }
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
    /// routing rows by `config.shard_key_columns` — range-partitioned when
    /// `config.range_bounds` supplies split points for a single key column,
    /// hashed otherwise, and round-robin when no key is set. Used by the Cayenne
    /// accelerator to parallelize the Vortex encode.
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

    /// Serve this format's scans from the process-wide segment cache.
    ///
    /// **Opt-in, and only sound when this format's file paths are immutable.** The
    /// segment cache has no read-time validation: a file overwritten in place
    /// keeps serving the segments cached under its path. Cayenne qualifies —
    /// every data file is `{uuid7}_p{shard}_{index}.vortex` beneath a uuid7
    /// snapshot directory, so a path is written once and never reused, and
    /// retirement invalidates it explicitly. A listing table over externally
    /// managed files does not: those can be replaced under the same name at any
    /// time, which is why they keep the private, opt-in cache above.
    ///
    /// Falls back to whatever this format already had when the process made no
    /// caching decision (an embedded host that skips the runtime builder), and
    /// caches nothing when the decision was to disable it.
    #[must_use]
    pub fn new_with_process_segment_cache(
        session: VortexSession,
        opts: VortexTableOptions,
    ) -> Self {
        // Decide before constructing, so a caller that ends up on the shared
        // cache never builds — and immediately discards — a private one.
        if let Some(process) = segment_cache::process_segment_cache() {
            let mut format = Self::new_with_options_named(
                session,
                VortexTableOptions {
                    segment_cache_size_bytes: None,
                    ..opts
                },
                None,
            );
            format.segment_cache = Some(Arc::clone(process));
            return format;
        }
        if segment_cache::segment_caching_disabled() {
            return Self::new_with_options_named(
                session,
                VortexTableOptions {
                    segment_cache_size_bytes: None,
                    ..opts
                },
                None,
            );
        }
        // No decision: an embedded host that skipped the runtime builder keeps
        // the cache its own options asked for.
        Self::new_with_options_named(session, opts, None)
    }

    /// Byte capacity of the segment cache backing this format's scans, or `None`
    /// when scans run uncached.
    ///
    /// This is the whole cache's budget, not a share of it: the cache is
    /// process-wide, so every format reports the same figure.
    #[must_use]
    pub fn segment_cache_capacity_bytes(&self) -> Option<u64> {
        self.segment_cache
            .as_ref()
            .map(|cache| cache.capacity_bytes())
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
        // Range-partition when the caller supplied bounds for a single key
        // column: same row-wise split as `Hash`, so every encoder is fed from
        // the first batch, but the shards tile the key domain in order instead
        // of scattering it, which is what lets a file's zone maps prune.
        if let (Some(bounds), [expr]) = (write_shard.range_bounds.as_ref(), exprs.as_slice())
            && !bounds.is_empty()
        {
            return ShardSpec::Range {
                expr: Arc::clone(expr),
                bounds: bounds.clone(),
                partitions,
            };
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
        file.with_extension(Arc::unwrap_or_clone(access_plan))
    } else {
        file
    }
}

#[async_trait]
impl FileFormat for VortexFormat {
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
                    if let Some(entry) = cache.get(&object.location)
                        && entry.is_valid_for(&object)
                        && let Some(cached_vortex) = entry
                            .file_metadata
                            .as_any()
                            .downcast_ref::<CachedVortexMetadata>()
                    {
                        let inferred_schema = session
                            .arrow()
                            .to_arrow_schema(cached_vortex.footer().dtype())?;
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
                    cache_footer(&cache, object.clone(), cached_metadata, "infer_schema");

                    let inferred_schema = session.arrow().to_arrow_schema(vxf.dtype())?;
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
            let cached_metadata = file_metadata_cache
                .get(&object.location)
                .filter(|entry| entry.is_valid_for(&object))
                .and_then(|entry| {
                    entry
                        .file_metadata
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
                cache_footer(&file_metadata_cache, object.clone(), cached, "infer_stats");

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
                let column_size =
                    stats_set.get_as::<usize>(Stat::UncompressedSizeInBytes, &PType::U64.into());
                sum_of_column_byte_sizes = sum_of_column_byte_sizes
                    .zip(column_size)
                    .map(|(acc, size)| acc + size);

                let target_dtype = DType::from_arrow(field.as_ref());
                let min = scalar_stat_to_df(
                    Stat::Min,
                    stats_set.get(Stat::Min),
                    stats_dtype,
                    &target_dtype,
                );

                let max = scalar_stat_to_df(
                    Stat::Max,
                    stats_set.get(Stat::Max),
                    stats_dtype,
                    &target_dtype,
                );

                let null_count = stats_set.get_as::<usize>(Stat::NullCount, &PType::U64.into());

                // Surface the column sum from the Vortex footer (`Stat::Sum`) so
                // whole-table `SUM`/`AVG` can be answered from metadata without a
                // scan. We deliberately keep the sum in its *own* widened dtype
                // (`Stat::Sum.dtype`) rather than casting down to the column's
                // arrow type: the sum of e.g. an `Int32` column is an `Int64` in
                // DataFusion, and narrowing here would lose width or overflow.
                let sum = match Stat::Sum.dtype(stats_dtype) {
                    Some(sum_dtype) => scalar_stat_to_df(
                        Stat::Sum,
                        stats_set.get(Stat::Sum),
                        stats_dtype,
                        &sum_dtype,
                    ),
                    None => stats::Precision::Absent,
                };

                column_statistics.push(ColumnStatistics {
                    null_count: null_count.to_df(),
                    min_value: min.to_df(),
                    max_value: max.to_df(),
                    sum_value: sum.to_df(),
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

fn scalar_stat_to_df(
    stat: Stat,
    value: stats::Precision<VortexScalarValue>,
    stats_dtype: &DType,
    target_dtype: &DType,
) -> stats::Precision<datafusion_common::ScalarValue> {
    let Some(scalar_dtype) = stat.dtype(stats_dtype) else {
        return stats::Precision::Absent;
    };

    value
        .map(|stat_value| {
            Scalar::try_new(scalar_dtype, Some(stat_value))?
                .cast(target_dtype)?
                .try_to_df()
        })
        .transpose()
        .unwrap_or(stats::Precision::Absent)
}

fn distinct_count_from_is_constant(is_constant: stats::Precision<bool>) -> Precision<usize> {
    match is_constant.as_exact() {
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

    /// Verifies the Vortex -> DataFusion statistics boundary: a written
    /// Vortex file surfaces per-column byte sizes (from the footer's
    /// `UncompressedSizeInBytes`) into `ColumnStatistics.byte_size` — including for
    /// variable-width (`Utf8`) columns — and a projected scan reports only the
    /// projected columns' bytes rather than the full unprojected row width.
    #[tokio::test]
    async fn propagates_per_column_byte_size() -> anyhow::Result<()> {
        let ctx = TestSessionContext::new(true);

        // Wide schema: fixed-width Int, a narrow Utf8, and a FAT Utf8 (`data`)
        // standing in for a `VARCHAR(500)`-style column.
        ctx.session
            .sql(
                "CREATE EXTERNAL TABLE t \
                 (id INT NOT NULL, s VARCHAR NOT NULL, data VARCHAR NOT NULL) \
                 STORED AS vortex LOCATION 'table/'",
            )
            .await?
            .collect()
            .await?;

        // Write a known number of rows; `data` holds a long value so dropping it
        // via projection produces a large, unmistakable drop in total_byte_size.
        let n = 8usize;
        let wide = "x".repeat(200);
        let values = (1..=n)
            .map(|i| format!("({i}, 's{i}', '{wide}')"))
            .collect::<Vec<_>>()
            .join(", ");
        ctx.session
            .sql(&format!("INSERT INTO t VALUES {values}"))
            .await?
            .collect()
            .await?;

        let provider = ctx.session.table_provider("t").await?;
        let state = ctx.session.state();

        // --- All columns: per-column byte_size present, total == sum ---------
        let all = provider
            .scan(&state, None, &[], None)
            .await?
            .partition_statistics(None)?;
        assert_eq!(all.num_rows.get_value(), Some(&n), "row count");

        let id_bytes = *all.column_statistics[0]
            .byte_size
            .get_value()
            .expect("Int column byte_size must be populated");
        let s_bytes = *all.column_statistics[1]
            .byte_size
            .get_value()
            .expect("narrow Utf8 byte_size must be populated");
        let data_bytes = *all.column_statistics[2]
            .byte_size
            .get_value()
            .expect("wide Utf8 byte_size must be populated");

        assert!(
            id_bytes >= 4 * n,
            "Int32 byte_size should be >= 4*rows, got {id_bytes}"
        );
        assert!(
            data_bytes > s_bytes,
            "wide column must report more bytes than the narrow one ({data_bytes} vs {s_bytes})"
        );

        let all_total = *all
            .total_byte_size
            .get_value()
            .expect("all-columns total present");
        assert_eq!(
            all_total,
            id_bytes + s_bytes + data_bytes,
            "all-columns total_byte_size must equal the sum of per-column byte_size"
        );

        // --- Projected scans: total reflects ONLY the projected columns ------
        // Project [id] (fixed-width): total is just the int column.
        let proj_id_cols = vec![0usize];
        let proj_id = provider
            .scan(&state, Some(&proj_id_cols), &[], None)
            .await?
            .partition_statistics(None)?;
        assert_eq!(
            proj_id.total_byte_size.get_value(),
            Some(&id_bytes),
            "projecting [id] must report only the id column's bytes"
        );

        // Project [s] (variable-width survives, fat `data` dropped).
        let proj_s_cols = vec![1usize];
        let proj_s = provider
            .scan(&state, Some(&proj_s_cols), &[], None)
            .await?
            .partition_statistics(None)?;
        assert_eq!(
            proj_s.total_byte_size.get_value(),
            Some(&s_bytes),
            "projecting [s] must report only the s column's bytes, not the full row"
        );
        assert!(
            *proj_s
                .total_byte_size
                .get_value()
                .expect("projected total present")
                < all_total,
            "projected total must drop the unprojected wide `data` column"
        );

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
        shard_format_with_bounds(write_concurrency, keys, None)
    }

    fn shard_format_with_bounds(
        write_concurrency: usize,
        keys: &[&str],
        range_bounds: Option<Vec<ScalarValue>>,
    ) -> VortexFormat {
        VortexFormat::new(VortexSession::default()).with_write_shard(WriteShardConfig {
            write_concurrency,
            shard_key_columns: keys.iter().map(|s| (*s).to_string()).collect(),
            range_bounds,
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

    /// Bounds on a single key column select the range split, which tiles the
    /// key domain in order instead of scattering it like a hash.
    #[test]
    fn build_shard_spec_bounds_select_range() {
        let schema = schema_with(&[("k", arrow_schema::DataType::Int64)]);
        let bounds = vec![ScalarValue::Int64(Some(10)), ScalarValue::Int64(Some(20))];
        match shard_format_with_bounds(3, &["k"], Some(bounds)).build_shard_spec(&schema, 8) {
            ShardSpec::Range {
                partitions, bounds, ..
            } => {
                assert_eq!(partitions, 3);
                assert_eq!(bounds.len(), 2);
            }
            other => panic!("expected Range, got {other:?}"),
        }
    }

    /// A composite key hashes: ordering it needs a lexicographic comparison the
    /// range split does not implement.
    #[test]
    fn build_shard_spec_composite_key_with_bounds_still_hashes() {
        let schema = schema_with(&[
            ("k", arrow_schema::DataType::Int64),
            ("j", arrow_schema::DataType::Int64),
        ]);
        let bounds = vec![ScalarValue::Int64(Some(10))];
        assert!(matches!(
            shard_format_with_bounds(2, &["k", "j"], Some(bounds)).build_shard_spec(&schema, 8),
            ShardSpec::Hash { .. }
        ));
    }

    /// Without bounds a keyed write hashes, which is the behavior that predates
    /// range partitioning.
    #[test]
    fn build_shard_spec_key_without_bounds_hashes() {
        let schema = schema_with(&[("k", arrow_schema::DataType::Int64)]);
        assert!(matches!(
            shard_format_with_bounds(4, &["k"], None).build_shard_spec(&schema, 8),
            ShardSpec::Hash { .. }
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
            distinct_count_from_is_constant(stats::Precision::exact(true)),
            Precision::Exact(1)
        );
        assert_eq!(
            distinct_count_from_is_constant(stats::Precision::exact(false)),
            Precision::Absent
        );
        assert_eq!(
            distinct_count_from_is_constant(stats::Precision::inexact(true)),
            Precision::Absent
        );
        assert_eq!(
            distinct_count_from_is_constant(stats::Precision::Absent),
            Precision::Absent
        );
    }
}
