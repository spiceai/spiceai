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

use datafusion_execution::config::SessionConfig;
use vortex::VortexSessionDefault;
use vortex_datafusion::{VortexFormat, VortexOptions};
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
}

impl CayenneContext {
    /// Create a new Cayenne context from configuration.
    ///
    /// This creates a new `VortexFormat` with caches sized according to the config.
    /// The returned `Arc` should be shared across all table providers that should
    /// use the same caches.
    #[must_use]
    pub fn new(config: &VortexConfig) -> Arc<Self> {
        let vortex_format = Self::create_vortex_format(config);
        Arc::new(Self {
            vortex_format,
            config: config.clone(),
            session_config: SessionConfig::default(),
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

    /// Get the maximum number of concurrent file uploads.
    #[must_use]
    pub fn upload_concurrency(&self) -> usize {
        self.config.upload_concurrency.max(1)
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
        let vortex_opts = VortexOptions {
            footer_cache_size_mb: config.footer_cache_mb,
            segment_cache_size_mb: config.segment_cache_mb,
            ..VortexOptions::default()
        };

        Arc::new(VortexFormat::new_with_options(vortex_session, vortex_opts))
    }
}
