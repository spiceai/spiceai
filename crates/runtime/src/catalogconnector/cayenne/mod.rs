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

//! Catalog connector for Cayenne catalogs backed by local file storage.
//!
//! Cayenne catalogs use `SQLite` for metadata and Vortex files for columnar data,
//! with data stored on local disk.

use super::{CatalogConnector, ConnectorComponent, ParameterSpec};
use crate::{
    Runtime, component::catalog::Catalog, dataconnector::parameters::ConnectorParams,
    parameters::Parameters,
};
use async_trait::async_trait;
use cayenne::{CayenneCatalogProvider, CayenneCatalogProviderConfig};
use data_components::RefreshableCatalogProvider as _;
use std::any::Any;
use std::sync::Arc;

pub mod provider;

/// Catalog connector prefix for Cayenne catalogs.
pub static PREFIX: &str = "cayenne";

/// Parameters for configuring a Cayenne catalog.
pub const PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec::component("data_dir")
        .description("Local directory for table data files. Defaults to spice data directory."),
    ParameterSpec::component("metadata_dir").description(
        "Local directory for Cayenne SQLite metadata. Defaults to spice data directory.",
    ),
    ParameterSpec::component("segment_cache_mb")
        .description("Vortex segment cache size in MB. Default: 256.")
        .default("256"),
    ParameterSpec::component("target_file_size_mb")
        .description("Target Vortex file size in MB. Default: 256.")
        .default("256"),
    ParameterSpec::component("compression_strategy")
        .description("Compression: 'btrblocks' (default) or 'zstd'.")
        .default("btrblocks"),
    ParameterSpec::component("pk_conflict_detection")
        .description("Whether Cayenne scans existing primary keys on insert. 'auto' (default) detects conflicts; 'none' skips conflict detection and is only safe when the source enforces primary-key uniqueness and ingestion cannot replay existing rows.")
        .one_of(&["auto", "none"])
        .default("auto"),
    ParameterSpec::component("upload_concurrency")
        .description("Maximum number of concurrent file uploads when writing multiple Vortex files. Defaults to available CPU parallelism."),
    ParameterSpec::component("write_concurrency")
        .description("Optional writer partition override for unsorted Cayenne ingests. Defaults to runtime.query.target_partitions."),
    ParameterSpec::component("inline_max_rows")
        .description("Maximum rows in a single write that can be inlined into the Cayenne metastore instead of writing a Vortex file. Set to 0 to disable write-entry inlining. Default: 1024.")
        .default("1024"),
    ParameterSpec::component("inline_max_bytes")
        .description("Maximum serialized Arrow IPC bytes in a single inlined Cayenne metastore entry. Set to 0 to disable write-entry inlining. Default: 1048576.")
        .default("1048576"),
    ParameterSpec::component("inline_max_buffer_bytes")
        .description("Maximum Arrow in-memory bytes buffered while deciding whether to inline a write. Set to 0 to force the Vortex write path after the first buffered batch. Default: 4194304.")
        .default("4194304"),
    ParameterSpec::component("inline_flush_max_rows")
        .description("Maximum inline rows before checkpointing inline data to Vortex. Default: 10000.")
        .default("10000"),
    ParameterSpec::component("inline_flush_max_segments")
        .description("Maximum inline entries before checkpointing inline data to Vortex. Default: 64.")
        .default("64"),
    ParameterSpec::component("inline_flush_max_bytes")
        .description("Maximum inline IPC bytes before checkpointing inline data to Vortex. Default: 8388608.")
        .default("8388608"),
    ParameterSpec::component("compaction_trigger_files")
        .description("New current-snapshot files since the last compaction before a post-write compaction pass is scheduled. Default: 8."),
    ParameterSpec::component("compaction_trigger_protected_snapshots")
        .description("Protected snapshot count that triggers a fast subset compaction. Lower values reduce per-scan protected-snapshot amplification at the cost of more frequent compaction. Default: 8."),
    ParameterSpec::component("compaction_max_files_per_pick")
        .description("Maximum input files merged per compaction pass. Default: 32."),
    ParameterSpec::component("pk_keyset_cache_mb")
        .description("MB budget for the in-memory primary-key keyset used for upsert conflict detection. Within budget an exact keyset (and position-based deletion capture) is kept; over budget upsert tables degrade to a bloom keyset. Default: 256."),
];

/// A catalog connector for Cayenne lakehouse catalogs.
///
/// Cayenne catalogs provide a high-performance lakehouse format combining:
/// - `SQLite` for transactional metadata management (stored locally)
/// - Vortex columnar files for data (stored locally)
///
/// Used as `from: cayenne` in the spicepod. Does not support a catalog ID.
#[derive(Clone)]
pub struct CayenneCatalogConnector {
    params: Parameters,
}

impl CayenneCatalogConnector {
    /// Create a new Cayenne catalog connector from the given parameters.
    #[must_use]
    pub fn new_connector(params: ConnectorParams) -> Arc<dyn CatalogConnector> {
        Arc::new(Self {
            params: params.parameters,
        })
    }

    fn parse_provider_config(&self) -> CayenneCatalogProviderConfig {
        let data_dir = self
            .params
            .get("data_dir")
            .expose()
            .ok()
            .map(ToOwned::to_owned);
        let metadata_dir = self
            .params
            .get("metadata_dir")
            .expose()
            .ok()
            .map(ToOwned::to_owned);

        let segment_cache_mb = self
            .params
            .get("segment_cache_mb")
            .expose()
            .ok()
            .and_then(|v| v.parse::<usize>().ok());
        let target_file_size_mb = self
            .params
            .get("target_file_size_mb")
            .expose()
            .ok()
            .and_then(|v| v.parse::<usize>().ok());
        let compression_strategy = self
            .params
            .get("compression_strategy")
            .expose()
            .ok()
            .and_then(|v| match v.to_lowercase().as_str() {
                "zstd" => Some(cayenne::metadata::CompressionStrategy::Zstd),
                "btrblocks" => Some(cayenne::metadata::CompressionStrategy::Btrblocks),
                _ => None,
            });
        let pk_conflict_detection = self
            .params
            .get("pk_conflict_detection")
            .expose()
            .ok()
            .and_then(cayenne::metadata::PkConflictDetection::parse);
        let upload_concurrency = self
            .params
            .get("upload_concurrency")
            .expose()
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .map(|v| v.max(1));
        let write_concurrency = self
            .params
            .get("write_concurrency")
            .expose()
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .map(|v| v.max(1));
        let inline_max_rows = self
            .params
            .get("inline_max_rows")
            .expose()
            .ok()
            .and_then(|v| v.parse::<usize>().ok());
        let inline_max_bytes = self
            .params
            .get("inline_max_bytes")
            .expose()
            .ok()
            .and_then(|v| v.parse::<usize>().ok());
        let inline_max_buffer_bytes = self
            .params
            .get("inline_max_buffer_bytes")
            .expose()
            .ok()
            .and_then(|v| v.parse::<usize>().ok());
        let inline_flush_max_rows = self
            .params
            .get("inline_flush_max_rows")
            .expose()
            .ok()
            .and_then(|v| v.parse::<i64>().ok())
            .map(|v| v.max(0));
        let inline_flush_max_segments = self
            .params
            .get("inline_flush_max_segments")
            .expose()
            .ok()
            .and_then(|v| v.parse::<i64>().ok())
            .map(|v| v.max(0));
        let inline_flush_max_bytes = self
            .params
            .get("inline_flush_max_bytes")
            .expose()
            .ok()
            .and_then(|v| v.parse::<i64>().ok())
            .map(|v| v.max(0));
        let compaction_trigger_files = self
            .params
            .get("compaction_trigger_files")
            .expose()
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .map(|v| v.max(1));
        let compaction_trigger_protected_snapshots = self
            .params
            .get("compaction_trigger_protected_snapshots")
            .expose()
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .map(|v| v.max(1));
        let compaction_max_files_per_pick = self
            .params
            .get("compaction_max_files_per_pick")
            .expose()
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .map(|v| v.max(2));
        let pk_keyset_cache_mb = self
            .params
            .get("pk_keyset_cache_mb")
            .expose()
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .map(|v| v.max(1));

        CayenneCatalogProviderConfig {
            data_dir,
            metadata_dir,
            spice_data_base_path: crate::spice_data_base_path(),
            footer_cache_mb: None,
            segment_cache_mb,
            target_file_size_mb,
            compression_strategy,
            pk_conflict_detection,
            upload_concurrency,
            write_concurrency,
            inline_max_rows,
            inline_max_bytes,
            inline_max_buffer_bytes,
            inline_flush_max_rows,
            inline_flush_max_segments,
            inline_flush_max_bytes,
            compaction_trigger_files,
            compaction_trigger_protected_snapshots,
            compaction_max_files_per_pick,
            pk_keyset_cache_mb,
        }
    }
}

#[async_trait]
impl CatalogConnector for CayenneCatalogConnector {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn refreshable_catalog_provider(
        self: Arc<Self>,
        runtime: Arc<Runtime>,
        catalog: &Catalog,
    ) -> super::Result<Arc<dyn data_components::RefreshableCatalogProvider>> {
        if runtime
            .datafusion()
            .cluster_config
            .effective_role()
            .is_none()
        {
            return Err(super::Error::InvalidConfigurationNoSource {
                connector: PREFIX.to_string(),
                connector_component: ConnectorComponent::from(catalog),
                message: "Cayenne catalog is only supported in distributed Spice mode. Start Spice with `--role scheduler` or `--role executor` (with `--scheduler-address`). See https://spiceai.org/docs/features/distributed-query".to_string(),
            });
        }

        let runtime_env = runtime.datafusion().ctx.runtime_env();
        let provider_config = self.parse_provider_config();
        let refreshable_provider = Arc::new(
            CayenneCatalogProvider::try_new(provider_config, runtime_env)
                .await
                .map_err(|e| super::Error::UnableToGetCatalogProvider {
                    connector: PREFIX.to_string(),
                    connector_component: ConnectorComponent::from(catalog),
                    source: Box::new(e),
                })?,
        );

        refreshable_provider.refresh().await.map_err(|source| {
            super::Error::UnableToGetCatalogProvider {
                connector: PREFIX.to_string(),
                connector_component: ConnectorComponent::from(catalog),
                source,
            }
        })?;

        Ok(refreshable_provider)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parameters::Parameters;
    use runtime_secrets::Secrets;
    use secrecy::SecretString;
    use tokio::sync::RwLock;

    #[test]
    fn catalog_parameter_specs_render_single_cayenne_prefix() {
        let display_names: Vec<String> = PARAMETERS
            .iter()
            .map(|parameter| parameter.display_name(PREFIX))
            .collect();

        assert!(display_names.contains(&"cayenne_upload_concurrency".to_string()));
        assert!(display_names.contains(&"cayenne_write_concurrency".to_string()));
        assert!(display_names.contains(&"cayenne_inline_max_rows".to_string()));
        assert!(display_names.contains(&"cayenne_inline_flush_max_bytes".to_string()));
        assert!(display_names.contains(&"cayenne_pk_conflict_detection".to_string()));
        assert!(
            display_names
                .iter()
                .all(|name| !name.starts_with("cayenne_cayenne_")),
            "Cayenne catalog parameter specs should not include the prefix in component names"
        );
    }

    #[tokio::test]
    async fn parse_provider_config_uses_normalized_catalog_params() {
        let params = Parameters::try_new(
            "connector cayenne",
            vec![
                (
                    "cayenne_data_dir".to_string(),
                    SecretString::new("/tmp/cayenne-data".to_string().into()),
                ),
                (
                    "cayenne_upload_concurrency".to_string(),
                    SecretString::new("0".to_string().into()),
                ),
                (
                    "cayenne_write_concurrency".to_string(),
                    SecretString::new("8".to_string().into()),
                ),
                (
                    "cayenne_inline_max_rows".to_string(),
                    SecretString::new("0".to_string().into()),
                ),
                (
                    "cayenne_inline_flush_max_bytes".to_string(),
                    SecretString::new("2097152".to_string().into()),
                ),
                (
                    "cayenne_pk_conflict_detection".to_string(),
                    SecretString::new("none".to_string().into()),
                ),
            ],
            PREFIX,
            Arc::new(RwLock::new(Secrets::new())),
            PARAMETERS,
        )
        .await
        .expect("single-prefixed Cayenne catalog params should validate");
        let connector = CayenneCatalogConnector { params };

        let config = connector.parse_provider_config();

        assert_eq!(config.data_dir.as_deref(), Some("/tmp/cayenne-data"));
        assert_eq!(config.upload_concurrency, Some(1));
        assert_eq!(config.write_concurrency, Some(8));
        assert_eq!(config.inline_max_rows, Some(0));
        assert_eq!(config.inline_flush_max_bytes, Some(2_097_152));
        assert_eq!(
            config.pk_conflict_detection,
            Some(cayenne::metadata::PkConflictDetection::None)
        );
    }
}
