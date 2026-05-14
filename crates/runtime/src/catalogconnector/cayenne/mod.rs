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
    ParameterSpec::component("cayenne_data_dir")
        .description("Local directory for table data files. Defaults to spice data directory."),
    ParameterSpec::component("cayenne_metadata_dir").description(
        "Local directory for Cayenne SQLite metadata. Defaults to spice data directory.",
    ),
    ParameterSpec::component("cayenne_footer_cache_mb")
        .description("Vortex footer cache size in MB. Default: 128.")
        .default("128"),
    ParameterSpec::component("cayenne_segment_cache_mb")
        .description("Vortex segment cache size in MB. Default: 256.")
        .default("256"),
    ParameterSpec::component("cayenne_target_file_size_mb")
        .description("Target Vortex file size in MB. Default: 128.")
        .default("128"),
    ParameterSpec::component("cayenne_compression_strategy")
        .description("Compression: 'btrblocks' (default) or 'zstd'.")
        .default("btrblocks"),
    ParameterSpec::component("cayenne_upload_concurrency")
        .description("Maximum number of concurrent file uploads when writing multiple Vortex files. Defaults to available CPU parallelism."),
    ParameterSpec::component("cayenne_write_concurrency")
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
            .get("cayenne_data_dir")
            .expose()
            .ok()
            .map(ToOwned::to_owned);
        let metadata_dir = self
            .params
            .get("cayenne_metadata_dir")
            .expose()
            .ok()
            .map(ToOwned::to_owned);

        let footer_cache_mb = self
            .params
            .get("cayenne_footer_cache_mb")
            .expose()
            .ok()
            .and_then(|v| v.parse::<usize>().ok());
        let segment_cache_mb = self
            .params
            .get("cayenne_segment_cache_mb")
            .expose()
            .ok()
            .and_then(|v| v.parse::<usize>().ok());
        let target_file_size_mb = self
            .params
            .get("cayenne_target_file_size_mb")
            .expose()
            .ok()
            .and_then(|v| v.parse::<usize>().ok());
        let compression_strategy = self
            .params
            .get("cayenne_compression_strategy")
            .expose()
            .ok()
            .and_then(|v| match v.to_lowercase().as_str() {
                "zstd" => Some(cayenne::metadata::CompressionStrategy::Zstd),
                "btrblocks" => Some(cayenne::metadata::CompressionStrategy::Btrblocks),
                _ => None,
            });
        let upload_concurrency = self
            .params
            .get("cayenne_upload_concurrency")
            .expose()
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .map(|v| v.max(1));
        let write_concurrency = self
            .params
            .get("cayenne_write_concurrency")
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
