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

//! Catalog connector for Cayenne catalogs backed by S3 Express One Zone storage.
//!
//! Cayenne catalogs use SQLite for metadata and Vortex files for columnar data,
//! with data stored exclusively in S3 Express One Zone directory buckets.

use super::{CatalogConnector, ConnectorComponent, ParameterSpec};
use crate::{
    Runtime,
    component::catalog::Catalog,
    dataconnector::parameters::ConnectorParams,
    parameters::Parameters,
};
use async_trait::async_trait;
use data_components::RefreshableCatalogProvider as _;
use std::any::Any;
use std::sync::Arc;

pub mod provider;

use provider::CayenneCatalogProvider;

/// Catalog connector prefix for Cayenne catalogs.
pub static PREFIX: &str = "cayenne";

/// Default schema name for Cayenne catalogs (flat namespace).
pub const CAYENNE_DEFAULT_SCHEMA: &str = "default";

/// The Cayenne catalog also exposes a `public` schema alias so that
/// unqualified DDL (which DataFusion resolves to the `public` schema)
/// works transparently.
pub const CAYENNE_PUBLIC_SCHEMA: &str = "public";

/// Parameters for configuring a Cayenne catalog.
pub const PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec::component("cayenne_data_dir")
        .description("Local directory for table data files. When set, uses local file storage instead of S3. Defaults to spice data directory if neither S3 nor data_dir is configured."),
    ParameterSpec::component("cayenne_s3_bucket")
        .description("Explicit S3 Express One Zone bucket name. If not specified, auto-generated from catalog_id and zone."),
    ParameterSpec::component("cayenne_s3_zone_ids")
        .description("Comma-separated Availability Zone IDs for S3 Express One Zone storage (e.g., 'usw2-az1'). Required when using S3 storage."),
    ParameterSpec::component("cayenne_s3_region")
        .description("AWS region for S3 Express One Zone storage. If not specified, derived from cayenne_s3_zone_ids."),
    ParameterSpec::component("cayenne_s3_endpoint")
        .description("Custom S3 endpoint URL for S3 Express One Zone."),
    ParameterSpec::component("cayenne_s3_key")
        .description("AWS access key ID for S3 authentication.")
        .secret(),
    ParameterSpec::component("cayenne_s3_secret")
        .description("AWS secret access key for S3 authentication.")
        .secret(),
    ParameterSpec::component("cayenne_s3_session_token")
        .description("AWS session token for temporary credentials (optional).")
        .secret(),
    ParameterSpec::component("cayenne_s3_auth")
        .description("Authentication method: 'iam_role' (default) or 'key'.")
        .default("iam_role"),
    ParameterSpec::component("cayenne_s3_client_timeout")
        .description("Timeout for S3 client operations. Default: 120s.")
        .default("120s"),
    ParameterSpec::component("cayenne_s3_allow_http")
        .description("Allow HTTP (non-TLS) connections. Default: false.")
        .default("false"),
    ParameterSpec::component("cayenne_s3_unsigned_payload")
        .description("Use unsigned payload for S3 Express uploads. Default: true.")
        .default("true"),
    ParameterSpec::component("cayenne_metadata_dir")
        .description("Local directory for Cayenne SQLite metadata. Defaults to spice data directory."),
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
        .description("Parallel file upload concurrency. Default: 4.")
        .default("4"),
];

/// A catalog connector for Cayenne lakehouse catalogs.
///
/// Cayenne catalogs provide a high-performance lakehouse format combining:
/// - SQLite for transactional metadata management (stored locally)
/// - Vortex columnar files for data (stored locally or in S3 Express One Zone)
///
/// **Storage modes:**
/// - **Local**: Set `cayenne_data_dir` or omit all S3 parameters. Data is stored on local disk.
/// - **S3 Express One Zone**: Set `cayenne_s3_zone_ids` (and optionally `cayenne_s3_bucket`).
///   Data is stored in S3 Express One Zone directory buckets.
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
}

#[async_trait]
impl CatalogConnector for CayenneCatalogConnector {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn refreshable_catalog_provider(
        self: Arc<Self>,
        _runtime: Arc<Runtime>,
        catalog: &Catalog,
    ) -> super::Result<Arc<dyn data_components::RefreshableCatalogProvider>> {
        let refreshable_provider = Arc::new(
            CayenneCatalogProvider::try_new(self.params.clone(), catalog)
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
