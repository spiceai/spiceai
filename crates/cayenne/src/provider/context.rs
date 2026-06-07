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

//! Shared context for Cayenne table operations.

use std::sync::Arc;

use datafusion_execution::{config::SessionConfig, runtime_env::RuntimeEnv};
use tokio::sync::Semaphore;
use vortex::VortexSessionDefault;
use vortex::file::WriteStrategyBuilder;
use vortex_datafusion::{ProjectionPushdown, VortexFormat, VortexTableOptions, WriteShardConfig};
use vortex_session::VortexSession;

use crate::metadata::{DeletionMode, DeltaEncoding, PkConflictDetection, VortexConfig};

/// Shared context for Cayenne table operations.
///
/// Contains cached resources and configuration that can be shared across
/// multiple table providers (e.g., partitions of the same dataset).
///
/// # Sharing
///
/// The shared `RuntimeEnv` carries the `DataFusion` file metadata cache used by
/// Vortex for cached footer metadata. Sharing a `CayenneContext` across table
/// providers means they share that runtime-level cache, reducing repeated footer
/// reads when working with partitioned datasets.
#[derive(Debug)]
pub struct CayenneContext {
    /// Shared Vortex format for reading and writing data files.
    vortex_format: Arc<VortexFormat>,
    /// Configuration for encoding, compression, and file sizing.
    config: VortexConfig,
    /// Dataset label the formats are tagged with (metrics attribution).
    /// Retained so per-write format variants (e.g. delta-encoding strategy
    /// overrides) carry the same label as the shared base format.
    dataset: String,
    /// Session configuration for `DataFusion` listing options.
    session_config: SessionConfig,
    /// Shared semaphore for limiting concurrent file writes / uploads across all partitions.
    upload_semaphore: Arc<Semaphore>,
    /// Shared `RuntimeEnv` from the main Spice runtime.
    ///
    /// Cayenne uses this `RuntimeEnv` for all internal `SessionContext`
    /// creation, ensuring that the `list_files_cache` (and other caches/object stores)
    /// are shared with the main query engine.
    runtime_env: Arc<RuntimeEnv>,
}

/// Default byte budget for the in-memory PK keyset cache when
/// `cayenne_pk_keyset_cache_mb` is unset. 256 MiB preserves historical behavior;
/// raise the param on memory-rich hosts so high-cardinality tables keep their
/// keyset resident instead of rebuilding it from a full-table scan every CDC
/// batch.
pub(crate) const DEFAULT_PK_KEYSET_CACHE_MAX_BYTES: usize = 256 * 1024 * 1024;

/// Hard ceiling on the configurable PK keyset cache budget. The budget doubles as
/// the bloom allocation size (`PkBloom::with_byte_budget`), so an out-of-range or
/// typo'd `cayenne_pk_keyset_cache_mb` must not be able to request a
/// near-`usize::MAX` allocation. Matches the auto-default's 8 GiB ceiling.
pub(crate) const PK_KEYSET_CACHE_MAX_CONFIGURABLE_BYTES: usize = 8 * 1024 * 1024 * 1024;

impl CayenneContext {
    /// Create a new Cayenne context from configuration.
    ///
    /// This creates a new `VortexFormat`. The shared runtime's file metadata cache
    /// is configured once by the owning runtime before table providers are created.
    #[must_use]
    pub fn new(config: &VortexConfig, runtime_env: Arc<RuntimeEnv>, dataset: &str) -> Arc<Self> {
        let vortex_format = Self::create_vortex_format(config, dataset);
        Arc::new(Self {
            vortex_format,
            config: config.clone(),
            dataset: dataset.to_string(),
            session_config: SessionConfig::default(),
            upload_semaphore: Arc::new(Semaphore::new(config.upload_concurrency.max(1))),
            runtime_env,
        })
    }

    /// Encoding effort configured for delta writes (`cayenne_delta_encoding`).
    #[must_use]
    pub fn delta_encoding(&self) -> DeltaEncoding {
        self.config.delta_encoding
    }

    /// Build a write-only `VortexFormat` whose session carries a
    /// [`WriteStrategyBuilder`] override — used by light delta-encoding levels
    /// (see `provider::delta_encoding`). The format mirrors the shared base
    /// format's table options and dataset label; `shard` optionally enables
    /// intra-write sharding exactly like
    /// `CayenneTableProvider::write_shard_format` does on the default path.
    ///
    /// Scans never observe these formats: the scan path keeps using
    /// [`Self::file_format`], so the strategy override affects only the files
    /// this write produces.
    #[must_use]
    pub(crate) fn write_format_with_strategy(
        &self,
        strategy: WriteStrategyBuilder,
        shard: Option<WriteShardConfig>,
    ) -> Arc<VortexFormat> {
        let session = VortexSession::default().set(strategy);
        let format =
            VortexFormat::new_with_options(session, Self::vortex_table_options(&self.config))
                .with_dataset_label(self.dataset.as_str());
        let format = match shard {
            Some(config) => format.with_write_shard(config),
            None => format,
        };
        Arc::new(format)
    }

    /// Get the Vortex file format for creating listing tables.
    ///
    /// The format uses the shared runtime cache for file metadata.
    #[must_use]
    pub fn file_format(&self) -> &Arc<VortexFormat> {
        &self.vortex_format
    }

    /// Get the Vortex configuration.
    #[must_use]
    pub fn config(&self) -> &VortexConfig {
        &self.config
    }

    /// Get the session configuration for `DataFusion` listing options.
    #[must_use]
    pub fn session_config(&self) -> &SessionConfig {
        &self.session_config
    }

    /// Get the target file size in bytes for chunking data files.
    #[must_use]
    pub fn target_file_size_bytes(&self) -> usize {
        self.config.target_vortex_file_size_mb * 1024 * 1024
    }

    /// Get the sort columns if configured.
    #[must_use]
    pub fn sort_columns(&self) -> &[String] {
        &self.config.sort_columns
    }

    /// Check if sorting is enabled.
    #[must_use]
    pub fn has_sort_columns(&self) -> bool {
        !self.config.sort_columns.is_empty()
    }

    /// Get the shared `RuntimeEnv`.
    #[must_use]
    pub fn runtime_env(&self) -> &Arc<RuntimeEnv> {
        &self.runtime_env
    }

    /// Get the maximum number of concurrent file uploads.
    #[must_use]
    pub fn upload_concurrency(&self) -> usize {
        self.config.upload_concurrency.max(1)
    }

    /// Get the configured writer partition override for unsorted snapshot writes.
    #[must_use]
    pub fn write_concurrency(&self) -> Option<usize> {
        self.config.write_concurrency.map(|v| v.max(1))
    }

    /// Maximum rows in one write that may be inlined into the metastore.
    #[must_use]
    pub(crate) fn inline_max_rows(&self) -> usize {
        self.config.inline_max_rows
    }

    /// Maximum serialized IPC bytes in one inlined metastore entry.
    #[must_use]
    pub(crate) fn inline_max_bytes(&self) -> usize {
        self.config.inline_max_bytes
    }

    /// Maximum in-memory Arrow bytes buffered while deciding whether to inline.
    #[must_use]
    pub(crate) fn inline_max_buffer_bytes(&self) -> usize {
        self.config.inline_max_buffer_bytes
    }

    /// Maximum inline rows before checkpointing to Vortex.
    #[must_use]
    pub(crate) fn inline_flush_max_rows(&self) -> i64 {
        self.config.inline_flush_max_rows.max(0)
    }

    /// Maximum inline entries before checkpointing to Vortex.
    #[must_use]
    pub(crate) fn inline_flush_max_segments(&self) -> i64 {
        self.config.inline_flush_max_segments.max(0)
    }

    /// Maximum inline IPC bytes before checkpointing to Vortex.
    #[must_use]
    pub(crate) fn inline_flush_max_bytes(&self) -> i64 {
        self.config.inline_flush_max_bytes.max(0)
    }

    /// Primary-key conflict detection behavior for inserts.
    #[must_use]
    pub(crate) fn pk_conflict_detection(&self) -> PkConflictDetection {
        self.config.pk_conflict_detection
    }

    /// How primary-key deletions are recorded and applied for PK tables.
    /// The default `auto` resolves to `position` (merge-on-read position-delete
    /// vectors); `key` is the opt-out that keeps the above-scan key-based filter.
    #[must_use]
    pub(crate) fn deletion_mode(&self) -> DeletionMode {
        self.config.deletion_mode
    }

    /// Byte budget for the in-memory PK keyset cache used during upsert conflict
    /// detection. See [`DEFAULT_PK_KEYSET_CACHE_MAX_BYTES`].
    #[must_use]
    pub(crate) fn pk_keyset_cache_max_bytes(&self) -> usize {
        self.config
            .pk_keyset_cache_mb
            .map_or(DEFAULT_PK_KEYSET_CACHE_MAX_BYTES, |mb| {
                // Cap the configured budget: it doubles as the bloom allocation
                // size, so a huge/typo'd value would otherwise saturate and try
                // to allocate ~`usize::MAX`. `0` is preserved (forces the bloom).
                mb.saturating_mul(1024 * 1024)
                    .min(PK_KEYSET_CACHE_MAX_CONFIGURABLE_BYTES)
            })
    }

    /// Build the compaction picker config from the underlying `VortexConfig`.
    #[must_use]
    pub(crate) fn compaction_picker_config(&self) -> super::compaction::CompactionPickerConfig {
        // `target_file_size_bytes` returns `usize`; widen via checked
        // conversion so a future 128-bit `usize` couldn't silently truncate
        // the tier thresholds. `u64::MAX` is a safe fallback because the
        // picker only ever asks "is bucket size < threshold".
        let target_bytes = u64::try_from(self.target_file_size_bytes()).unwrap_or(u64::MAX);
        super::compaction::CompactionPickerConfig::new(
            self.config.compaction_trigger_files,
            self.config.compaction_max_files_per_pick,
            target_bytes,
        )
    }

    /// Protected snapshot count that should trigger maintenance compaction.
    #[must_use]
    pub(crate) fn compaction_trigger_protected_snapshots(&self) -> usize {
        self.config.compaction_trigger_protected_snapshots.max(1)
    }

    /// Maximum number of consecutive compaction passes per trigger.
    #[must_use]
    pub(crate) fn compaction_max_levels(&self) -> usize {
        self.config.compaction_max_levels.max(1)
    }

    /// Protected snapshot age that should trigger maintenance compaction.
    #[must_use]
    pub(crate) fn compaction_trigger_snapshot_age(&self) -> Option<std::time::Duration> {
        if self.config.compaction_trigger_snapshot_age_ms == 0 {
            None
        } else {
            Some(std::time::Duration::from_millis(
                self.config.compaction_trigger_snapshot_age_ms,
            ))
        }
    }

    /// Background compaction interval. Returns `None` when disabled (interval = 0).
    #[must_use]
    pub(crate) fn compaction_background_interval(&self) -> Option<std::time::Duration> {
        if self.config.compaction_background_interval_ms == 0 {
            None
        } else {
            Some(std::time::Duration::from_millis(
                self.config.compaction_background_interval_ms,
            ))
        }
    }

    /// Get the shared semaphore for limiting concurrent file writes / uploads.
    #[must_use]
    pub fn upload_semaphore(&self) -> &Arc<Semaphore> {
        &self.upload_semaphore
    }

    /// Create a `VortexFormat` from configuration.
    ///
    /// The format carries Vortex scan/write options, including the shared
    /// segment-cache capacity for scans created from this context.
    fn create_vortex_format(config: &VortexConfig, dataset: &str) -> Arc<VortexFormat> {
        // Create a Vortex session with default encodings. The session's write
        // strategy is the table's FULL encoding tier: the BtrBlocks cascade by
        // default, optionally extended with the Zstd string scheme when
        // `cayenne_compression_strategy=zstd` (this wiring is what makes that
        // param real — see `delta_encoding::full_strategy_builder_for`).
        // Maintenance writes and full-level delta writes inherit it; light
        // delta-encoding levels override it per write via
        // `write_format_with_strategy` (see `provider::delta_encoding`).
        let mut vortex_session = VortexSession::default();
        if let Some(full_strategy) =
            super::delta_encoding::full_strategy_builder_for(&config.compression_strategy)
        {
            vortex_session = vortex_session.set(full_strategy);
        }

        Arc::new(
            VortexFormat::new_with_options(vortex_session, Self::vortex_table_options(config))
                .with_dataset_label(dataset),
        )
    }

    /// Table options shared by the base format and any per-write format
    /// variants (delta-encoding strategy overrides) so write-only formats
    /// behave identically apart from the encoding strategy.
    fn vortex_table_options(config: &VortexConfig) -> VortexTableOptions {
        let segment_cache_size_bytes =
            config
                .segment_cache_mb
                .checked_mul(1024 * 1024)
                .or_else(|| {
                    tracing::warn!(
                        segment_cache_mb = config.segment_cache_mb,
                        "Vortex config `segment_cache_mb` is too large; disabling segment cache"
                    );
                    None
                });

        VortexTableOptions {
            target_file_size_mb: config.target_vortex_file_size_mb,
            projection_pushdown: ProjectionPushdown::On,
            segment_cache_size_bytes,
            ..VortexTableOptions::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cayenne_enables_vortex_projection_pushdown_by_default() {
        let runtime_env = Arc::new(RuntimeEnv::default());
        let context = CayenneContext::new(&VortexConfig::default(), runtime_env, "test");

        assert_eq!(
            context.file_format().options().projection_pushdown,
            ProjectionPushdown::On
        );
    }
}
