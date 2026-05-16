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
use vortex_datafusion::{VortexFormat, VortexTableOptions};
use vortex_session::VortexSession;

use crate::metadata::VortexConfig;

/// Shared context for Cayenne table operations.
///
/// Contains cached resources and configuration that can be shared across
/// multiple table providers (e.g., partitions of the same dataset).
///
/// # Sharing
///
/// The internal `VortexFormat` contains footer and segment caches backed by
/// [`moka::future::Cache`], which uses `Arc` internally. Sharing a `CayenneContext`
/// across table providers means they all share the same caches, reducing memory
/// usage when working with partitioned datasets.
#[derive(Debug)]
pub struct CayenneContext {
    /// Vortex format with shared footer/segment caches.
    vortex_format: Arc<VortexFormat>,
    /// Configuration for encoding, compression, and file sizing.
    config: VortexConfig,
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

impl CayenneContext {
    /// Create a new Cayenne context from configuration.
    ///
    /// This creates a new `VortexFormat` with caches sized according to the config.
    /// The returned `Arc` should be shared across all table providers that should
    /// use the same caches.
    #[must_use]
    pub fn new(config: &VortexConfig, runtime_env: Arc<RuntimeEnv>) -> Arc<Self> {
        let vortex_format = Self::create_vortex_format(config);
        Arc::new(Self {
            vortex_format,
            config: config.clone(),
            session_config: SessionConfig::default(),
            upload_semaphore: Arc::new(Semaphore::new(config.upload_concurrency.max(1))),
            runtime_env,
        })
    }

    /// Get the Vortex file format for creating listing tables.
    ///
    /// The format contains shared footer and segment caches.
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

    /// Maximum inline memtable rows before checkpointing to Vortex.
    #[must_use]
    pub(crate) fn inline_memtable_max_rows(&self) -> i64 {
        self.config.inline_memtable_max_rows.max(0)
    }

    /// Maximum inline memtable entries before checkpointing to Vortex.
    #[must_use]
    pub(crate) fn inline_memtable_max_segments(&self) -> i64 {
        self.config.inline_memtable_max_segments.max(0)
    }

    /// Maximum inline memtable IPC bytes before checkpointing to Vortex.
    #[must_use]
    pub(crate) fn inline_memtable_max_bytes(&self) -> i64 {
        self.config.inline_memtable_max_bytes.max(0)
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

    /// Maximum number of consecutive compaction passes per trigger.
    #[must_use]
    pub(crate) fn compaction_max_levels(&self) -> usize {
        self.config.compaction_max_levels.max(1)
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
    /// The format contains a `VortexFileCache` that can be accessed via `file_cache()`
    /// and shared with other `VortexFormat` instances using `new_with_cache()`.
    fn create_vortex_format(config: &VortexConfig) -> Arc<VortexFormat> {
        // Create a Vortex session with default encodings
        // Note: Write strategy configuration (e.g., compression) is applied at write time via
        // `session.write_options().with_strategy(...)`, not at the VortexFormat level
        let vortex_session = VortexSession::default();

        // Configure VortexFormat - it creates its own VortexFileCache internally
        let default_config = VortexConfig::default();
        if config.footer_cache_mb != default_config.footer_cache_mb {
            tracing::warn!(
                footer_cache_mb = config.footer_cache_mb,
                "Vortex config `footer_cache_mb` is currently ignored in Spice.ai 2.0.0-unstable"
            );
        }
        if config.segment_cache_mb != default_config.segment_cache_mb {
            tracing::warn!(
                segment_cache_mb = config.segment_cache_mb,
                "Vortex config `segment_cache_mb` is currently ignored in Spice.ai 2.0.0-unstable"
            );
        }

        let vortex_opts = VortexTableOptions {
            target_file_size_mb: config.target_vortex_file_size_mb,
            projection_pushdown: true,
            ..VortexTableOptions::default()
        };

        Arc::new(VortexFormat::new_with_options(vortex_session, vortex_opts))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cayenne_enables_vortex_projection_pushdown_by_default() {
        let runtime_env = Arc::new(RuntimeEnv::default());
        let context = CayenneContext::new(&VortexConfig::default(), runtime_env);

        assert!(context.file_format().options().projection_pushdown);
    }
}
