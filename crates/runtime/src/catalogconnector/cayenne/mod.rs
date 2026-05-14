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
    ParameterSpec::component("footer_cache_mb")
        .description("Vortex footer cache size in MB. Default: 128.")
        .default("128"),
    ParameterSpec::component("segment_cache_mb")
        .description("Vortex segment cache size in MB. Default: 256.")
        .default("256"),
    ParameterSpec::component("target_file_size_mb")
        .description("Target Vortex file size in MB. Default: 128.")
        .default("128"),
    ParameterSpec::component("compression_strategy")
        .description("Compression: 'btrblocks' (default) or 'zstd'.")
        .default("btrblocks"),
    ParameterSpec::component("upload_concurrency")
        .description("Maximum number of concurrent file uploads when writing multiple Vortex files. Defaults to available CPU parallelism."),
    ParameterSpec::component("write_concurrency")
        .description("Optional writer partition override for unsorted Cayenne ingests. Defaults to runtime.query.target_partitions."),
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

        let footer_cache_mb = self
            .params
            .get("footer_cache_mb")
            .expose()
            .ok()
            .and_then(|v| v.parse::<usize>().ok());
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

        CayenneCatalogProviderConfig {
            data_dir,
            metadata_dir,
            spice_data_base_path: crate::spice_data_base_path(),
            footer_cache_mb,
            segment_cache_mb,
            target_file_size_mb,
            compression_strategy,
            upload_concurrency,
            write_concurrency,
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
    }
}
