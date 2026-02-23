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

//! Cayenne catalog and schema provider implementations for DataFusion.
//!
//! Provides a flat-namespace catalog backed by a Cayenne [`MetadataCatalog`]
//! (SQLite) and S3 Express One Zone storage for data files.

use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use aws_sdk_credential_bridge::S3CredentialProvider;
use cayenne::metadata::{ObjectStoreConfig, VortexConfig};
use cayenne::{CayenneCatalog, CayenneTableProviderBuilder, MetadataCatalog};
use datafusion::catalog::{CatalogProvider, SchemaProvider, TableProvider};
use datafusion::error::Result as DFResult;
use object_store::aws::AmazonS3Builder;
use object_store::client::SpawnedReqwestConnector;
use object_store::{ClientOptions, RetryConfig};
use snafu::prelude::*;
use url::Url;

use crate::component::catalog::Catalog;
use crate::dataaccelerator::cayenne::s3::{
    derive_region_from_zone, generate_bucket_name,
};
use crate::parameters::Parameters;
use crate::spice_data_base_path;
use data_components::RefreshableCatalogProvider;

use super::CAYENNE_DEFAULT_SCHEMA;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to initialize Cayenne catalog: {source}"))]
    CatalogInit {
        source: cayenne::catalog::CatalogError,
    },

    #[snafu(display("Failed to build S3 object store for Cayenne catalog: {source}"))]
    S3ObjectStore {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Invalid Cayenne catalog configuration: {message}"))]
    InvalidConfiguration { message: String },

    #[snafu(display("Failed to create Cayenne table provider: {source}"))]
    TableProvider {
        source: cayenne::catalog::CatalogError,
    },

    #[snafu(display("Failed to create S3 Express One Zone bucket: {source}"))]
    BucketCreation {
        source: crate::dataaccelerator::cayenne::s3::Error,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// DataFusion [`CatalogProvider`] backed by a Cayenne metadata catalog.
///
/// All tables are in a single "default" schema. Data is stored in S3 Express
/// One Zone, metadata in local SQLite.
pub struct CayenneCatalogProvider {
    /// The underlying Cayenne metadata catalog (SQLite).
    catalog: Arc<dyn MetadataCatalog>,
    /// S3 object store configuration for data files.
    object_store_config: Option<ObjectStoreConfig>,
    /// Vortex configuration for table providers.
    vortex_config: VortexConfig,
    /// Base S3 path for table data.
    data_base_path: String,
    /// Local metadata directory.
    metadata_dir: String,
    /// Schema providers (just "default" for Cayenne).
    schemas: RwLock<HashMap<String, Arc<dyn SchemaProvider>>>,
}

impl std::fmt::Debug for CayenneCatalogProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CayenneCatalogProvider")
            .field("data_base_path", &self.data_base_path)
            .field("metadata_dir", &self.metadata_dir)
            .finish()
    }
}

impl CayenneCatalogProvider {
    /// Create a new Cayenne catalog provider.
    ///
    /// Initializes the SQLite metadata catalog and configures storage.
    /// If S3 parameters (`cayenne_s3_zone_ids` or `cayenne_s3_bucket`) are
    /// provided, uses S3 Express One Zone. Otherwise, uses local file storage.
    pub async fn try_new(params: Parameters, catalog_config: &Catalog) -> Result<Self> {
        let catalog_id = catalog_config
            .catalog_id
            .as_deref()
            .unwrap_or("cayenne_catalog");

        // Resolve metadata directory
        let metadata_dir = params
            .get("cayenne_metadata_dir")
            .expose()
            .ok()
            .map(String::from)
            .unwrap_or_else(|| format!("{}/cayenne_catalog_{catalog_id}/metadata", spice_data_base_path()));

        // Ensure metadata directory exists
        std::fs::create_dir_all(&metadata_dir).map_err(|e| Error::InvalidConfiguration {
            message: format!("Failed to create metadata directory '{metadata_dir}': {e}"),
        })?;

        // Initialize SQLite catalog
        let connection_string = format!("sqlite://{metadata_dir}/cayenne.db");
        let catalog = Arc::new(
            CayenneCatalog::new(connection_string)
                .map_err(|e| Error::CatalogInit { source: e })?,
        ) as Arc<dyn MetadataCatalog>;

        catalog.init().await.context(CatalogInitSnafu)?;

        // Determine storage mode: S3 or local
        let has_s3_zone = params.get("cayenne_s3_zone_ids").expose().ok().is_some();
        let has_s3_bucket = params.get("cayenne_s3_bucket").expose().ok().is_some();
        let use_s3 = has_s3_zone || has_s3_bucket;

        let (data_base_path, object_store_config) = if use_s3 {
            Self::init_s3_storage(&params, catalog_id).await?
        } else {
            Self::init_local_storage(&params, catalog_id)?
        };

        // Parse Vortex config from parameters
        let vortex_config = Self::parse_vortex_config(&params);

        let provider = Self {
            catalog,
            object_store_config,
            vortex_config,
            data_base_path,
            metadata_dir,
            schemas: RwLock::new(HashMap::new()),
        };

        Ok(provider)
    }

    /// Initialize S3 Express One Zone storage.
    ///
    /// Creates the directory bucket if needed and builds the object store config.
    async fn init_s3_storage(
        params: &Parameters,
        catalog_id: &str,
    ) -> Result<(String, Option<ObjectStoreConfig>)> {
        let zone_ids_str = params.get("cayenne_s3_zone_ids").expose().ok()
            .map(String::from)
            .unwrap_or_default();

        let primary_zone = zone_ids_str
            .split(',')
            .next()
            .map(str::trim)
            .filter(|s| !s.is_empty());

        // Use explicit bucket name if provided, otherwise auto-generate
        let bucket_name = if let Some(explicit_bucket) = params
            .get("cayenne_s3_bucket")
            .expose()
            .ok()
        {
            explicit_bucket.to_string()
        } else {
            let zone = primary_zone.ok_or_else(|| Error::InvalidConfiguration {
                message: "S3 storage requires 'cayenne_s3_zone_ids' when 'cayenne_s3_bucket' is not specified".to_string(),
            })?;
            let sanitized_catalog = catalog_id.replace(['.', '/'], "_");
            generate_bucket_name("catalog", &sanitized_catalog, zone)
                .map_err(|e| Error::InvalidConfiguration {
                    message: format!("Failed to generate S3 bucket name: {e}"),
                })?
        };

        let data_base_path = format!("s3://{bucket_name}/");

        // Derive region and zone for bucket creation
        // If zone is known, use it; if only bucket is specified, try to extract zone from bucket name
        let effective_zone = primary_zone
            .map(String::from)
            .or_else(|| {
                crate::dataaccelerator::cayenne::s3::extract_zone_id_from_bucket(&bucket_name)
                    .map(String::from)
            });

        let s3_region = params.get("cayenne_s3_region").expose().ok().map(String::from);
        let effective_region = s3_region
            .clone()
            .or_else(|| {
                effective_zone.as_deref()
                    .and_then(|z| derive_region_from_zone(z).map(String::from))
            })
            .ok_or_else(|| Error::InvalidConfiguration {
                message: "Cannot determine AWS region. Specify 'cayenne_s3_region' or 'cayenne_s3_zone_ids'.".to_string(),
            })?;

        // Extract credentials for bucket creation
        let s3_key = params.get("cayenne_s3_key").expose().ok().map(String::from);
        let s3_secret = params.get("cayenne_s3_secret").expose().ok().map(String::from);
        let s3_session_token = params.get("cayenne_s3_session_token").expose().ok().map(String::from);

        // Ensure the S3 Express One Zone directory bucket exists
        if let Some(ref zone) = effective_zone {
            crate::dataaccelerator::cayenne::s3::create_s3_express_bucket_if_needed(
                &bucket_name,
                zone,
                &effective_region,
                s3_key.clone(),
                s3_secret.clone(),
                s3_session_token.clone(),
            )
            .await
            .context(BucketCreationSnafu)?;
        }

        // Build object store config
        let zone_for_store = effective_zone.as_deref().unwrap_or("");
        let object_store_config =
            Self::build_object_store(params, &data_base_path, zone_for_store).await?;

        Ok((data_base_path, Some(object_store_config)))
    }

    /// Initialize local file storage.
    ///
    /// Creates the data directory and returns the local path.
    fn init_local_storage(
        params: &Parameters,
        catalog_id: &str,
    ) -> Result<(String, Option<ObjectStoreConfig>)> {
        let data_dir = params
            .get("cayenne_data_dir")
            .expose()
            .ok()
            .map(String::from)
            .unwrap_or_else(|| format!("{}/cayenne_catalog_{catalog_id}/data", spice_data_base_path()));

        std::fs::create_dir_all(&data_dir).map_err(|e| Error::InvalidConfiguration {
            message: format!("Failed to create data directory '{data_dir}': {e}"),
        })?;

        // Ensure trailing slash for consistent path joining
        let data_base_path = if data_dir.ends_with('/') {
            data_dir
        } else {
            format!("{data_dir}/")
        };

        tracing::info!("Cayenne catalog using local file storage at '{data_base_path}'");

        Ok((data_base_path, None))
    }

    /// Returns a reference to the underlying Cayenne metadata catalog.
    #[must_use]
    pub fn metadata_catalog(&self) -> &Arc<dyn MetadataCatalog> {
        &self.catalog
    }

    /// Returns the S3 object store configuration, if any.
    #[must_use]
    pub fn object_store_config(&self) -> Option<&ObjectStoreConfig> {
        self.object_store_config.as_ref()
    }

    /// Returns the base data path (S3 Express One Zone URL).
    #[must_use]
    pub fn data_base_path(&self) -> &str {
        &self.data_base_path
    }

    /// Returns the Vortex configuration.
    #[must_use]
    pub fn vortex_config(&self) -> &VortexConfig {
        &self.vortex_config
    }

    /// Returns the metadata directory path.
    #[must_use]
    pub fn metadata_dir(&self) -> &str {
        &self.metadata_dir
    }

    /// Build S3 object store for the Cayenne catalog.
    async fn build_object_store(
        params: &Parameters,
        data_path: &str,
        zone_id: &str,
    ) -> Result<ObjectStoreConfig> {
        let url = Url::parse(data_path).map_err(|e| Error::InvalidConfiguration {
            message: format!("Invalid S3 data path '{data_path}': {e}"),
        })?;

        let bucket_name = aws_sdk_credential_bridge::get_bucket_name(&url)
            .map_err(|e| Error::InvalidConfiguration {
                message: format!("Cannot extract bucket name from '{data_path}': {e}"),
            })?
            .to_string();

        // Get credentials from params
        let s3_region = params.get("cayenne_s3_region").expose().ok().map(String::from);
        let s3_endpoint = params.get("cayenne_s3_endpoint").expose().ok().map(String::from);
        let s3_key = params.get("cayenne_s3_key").expose().ok().map(String::from);
        let s3_secret = params.get("cayenne_s3_secret").expose().ok().map(String::from);
        let s3_session_token = params.get("cayenne_s3_session_token").expose().ok().map(String::from);
        let s3_auth = params
            .get("cayenne_s3_auth")
            .expose()
            .ok()
            .unwrap_or("iam_role");
        let s3_allow_http = params
            .get("cayenne_s3_allow_http")
            .expose()
            .ok()
            .is_some_and(|v| v.eq_ignore_ascii_case("true"));
        let s3_unsigned_payload = params
            .get("cayenne_s3_unsigned_payload")
            .expose()
            .ok()
            .is_none_or(|v| !v.eq_ignore_ascii_case("false"));

        // Derive region
        let effective_region = s3_region
            .or_else(|| derive_region_from_zone(zone_id).map(String::from))
            .ok_or_else(|| Error::InvalidConfiguration {
                message: format!(
                    "Cannot determine AWS region. Specify 'cayenne_s3_region' or use standard zone ID format. Zone: {zone_id}"
                ),
            })?
            .to_string();

        let io_runtime = tokio::runtime::Handle::current();
        let mut s3_builder = AmazonS3Builder::from_env()
            .with_bucket_name(&bucket_name)
            .with_http_connector(SpawnedReqwestConnector::new(io_runtime))
            .with_allow_http(s3_allow_http)
            .with_region(&effective_region)
            .with_s3_express(true)
            .with_virtual_hosted_style_request(true)
            .with_unsigned_payload(s3_unsigned_payload);

        // Set retry config
        let retry_config = RetryConfig {
            max_retries: 3,
            retry_timeout: std::time::Duration::from_secs(600),
            ..Default::default()
        };
        s3_builder = s3_builder.with_retry(retry_config);

        // S3 Express endpoint
        if let Some(ref endpoint) = s3_endpoint {
            s3_builder = s3_builder.with_endpoint(endpoint);
        } else {
            let express_endpoint =
                format!("https://{bucket_name}.s3express-{zone_id}.{effective_region}.amazonaws.com");
            s3_builder = s3_builder.with_endpoint(&express_endpoint);
        }

        // Client timeout
        let mut client_options =
            ClientOptions::default().with_timeout(std::time::Duration::from_secs(120));
        if let Some(timeout_str) = params.get("cayenne_s3_client_timeout").expose().ok() {
            if let Ok(duration) = fundu::parse_duration(timeout_str) {
                client_options = client_options.with_timeout(duration);
            }
        }
        s3_builder = s3_builder.with_client_options(client_options);

        // Credentials
        let mut load_from_env = true;
        if s3_auth == "key" {
            if let (Some(key), Some(secret)) = (&s3_key, &s3_secret) {
                s3_builder = s3_builder.with_access_key_id(key);
                s3_builder = s3_builder.with_secret_access_key(secret);
                if let Some(ref token) = s3_session_token {
                    s3_builder = s3_builder.with_token(token);
                }
                load_from_env = false;
            }
        }

        if load_from_env {
            if let Ok(Some(sdk_config)) =
                aws_sdk_credential_bridge::get_or_init_sdk_config().await
            {
                if sdk_config.credentials_provider().is_some() {
                    s3_builder = s3_builder.with_credentials(Arc::new(
                        S3CredentialProvider::from_config(sdk_config.as_ref())
                            .boxed()
                            .context(S3ObjectStoreSnafu)?,
                    ));
                }
            }
        }

        let store = s3_builder.build().boxed().context(S3ObjectStoreSnafu)?;

        Ok(ObjectStoreConfig {
            url,
            store: Arc::new(store),
        })
    }

    /// Parse Vortex configuration from catalog parameters.
    fn parse_vortex_config(params: &Parameters) -> VortexConfig {
        let mut config = VortexConfig::default();

        if let Some(v) = params.get("cayenne_footer_cache_mb").expose().ok() {
            if let Ok(val) = v.parse::<usize>() {
                config.footer_cache_mb = val;
            }
        }
        if let Some(v) = params.get("cayenne_segment_cache_mb").expose().ok() {
            if let Ok(val) = v.parse::<usize>() {
                config.segment_cache_mb = val;
            }
        }
        if let Some(v) = params.get("cayenne_target_file_size_mb").expose().ok() {
            if let Ok(val) = v.parse::<usize>() {
                config.target_vortex_file_size_mb = val;
            }
        }
        if let Some(v) = params.get("cayenne_compression_strategy").expose().ok() {
            match v.to_lowercase().as_str() {
                "zstd" => {
                    config.compression_strategy =
                        cayenne::metadata::CompressionStrategy::Zstd;
                }
                "btrblocks" => {
                    config.compression_strategy =
                        cayenne::metadata::CompressionStrategy::Btrblocks;
                }
                _ => {}
            }
        }
        if let Some(v) = params.get("cayenne_upload_concurrency").expose().ok() {
            if let Ok(val) = v.parse::<usize>() {
                config.upload_concurrency = val.max(1);
            }
        }

        config
    }

    /// Load all tables from the Cayenne catalog into a schema provider.
    async fn load_schema(
        catalog: &Arc<dyn MetadataCatalog>,
        object_store_config: Option<&ObjectStoreConfig>,
    ) -> Result<Arc<dyn SchemaProvider>> {
        let schema_provider = CayenneSchemaProvider::try_new(
            Arc::clone(catalog),
            object_store_config.cloned(),
        )
        .await?;

        Ok(Arc::new(schema_provider))
    }
}

impl CatalogProvider for CayenneCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        vec![
            CAYENNE_DEFAULT_SCHEMA.to_string(),
            super::CAYENNE_PUBLIC_SCHEMA.to_string(),
        ]
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        // Both "default" and "public" resolve to the same schema provider.
        let key = if name == super::CAYENNE_PUBLIC_SCHEMA {
            CAYENNE_DEFAULT_SCHEMA
        } else {
            name
        };
        self.schemas
            .read()
            .ok()
            .and_then(|schemas| schemas.get(key).cloned())
    }
}

#[async_trait]
impl RefreshableCatalogProvider for CayenneCatalogProvider {
    async fn refresh(&self) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let schema = Self::load_schema(&self.catalog, self.object_store_config.as_ref()).await?;

        match self.schemas.write() {
            Ok(mut schemas) => {
                schemas.insert(CAYENNE_DEFAULT_SCHEMA.to_string(), schema);
            }
            Err(poisoned) => {
                poisoned
                    .into_inner()
                    .insert(CAYENNE_DEFAULT_SCHEMA.to_string(), schema);
            }
        }

        Ok(())
    }
}

/// Schema provider for a Cayenne catalog's "default" schema.
///
/// Discovers all tables in the Cayenne metadata catalog and creates
/// [`CayenneTableProvider`] instances for each.
pub struct CayenneSchemaProvider {
    /// The underlying Cayenne metadata catalog.
    catalog: Arc<dyn MetadataCatalog>,
    /// S3 object store config for creating table providers.
    object_store_config: Option<ObjectStoreConfig>,
    /// Table providers keyed by table name.
    tables: RwLock<HashMap<String, Arc<dyn TableProvider>>>,
}

impl std::fmt::Debug for CayenneSchemaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CayenneSchemaProvider").finish()
    }
}

impl CayenneSchemaProvider {
    /// Create a new schema provider, loading all existing tables from the catalog.
    pub async fn try_new(
        catalog: Arc<dyn MetadataCatalog>,
        object_store_config: Option<ObjectStoreConfig>,
    ) -> Result<Self> {
        // Load all existing tables from the metadata catalog
        let table_names = catalog
            .list_table_names()
            .await
            .unwrap_or_else(|e| {
                tracing::warn!("Failed to list existing Cayenne tables: {e}");
                Vec::new()
            });

        let mut tables: HashMap<String, Arc<dyn TableProvider>> = HashMap::new();
        for name in &table_names {
            match Self::load_table(&catalog, name, object_store_config.as_ref()).await {
                Ok(Some(provider)) => {
                    tables.insert(name.clone(), provider);
                }
                Ok(None) => {
                    tracing::debug!("Table '{name}' listed in catalog but could not be loaded");
                }
                Err(e) => {
                    tracing::warn!("Failed to load Cayenne table '{name}': {e}");
                }
            }
        }

        if !tables.is_empty() {
            tracing::info!(
                "Loaded {} existing Cayenne table{}",
                tables.len(),
                if tables.len() == 1 { "" } else { "s" }
            );
        }

        let provider = Self {
            catalog,
            object_store_config,
            tables: RwLock::new(tables),
        };

        Ok(provider)
    }

    /// Returns a reference to the underlying Cayenne metadata catalog.
    #[must_use]
    pub fn metadata_catalog(&self) -> &Arc<dyn MetadataCatalog> {
        &self.catalog
    }

    /// Create a [`CayenneTableProvider`] for a table by name.
    async fn load_table(
        catalog: &Arc<dyn MetadataCatalog>,
        table_name: &str,
        object_store_config: Option<&ObjectStoreConfig>,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        // Check if the table exists in the catalog
        match catalog.get_table(table_name).await {
            Ok(_metadata) => {
                let mut builder = CayenneTableProviderBuilder::new(Arc::clone(catalog));
                if let Some(config) = object_store_config {
                    builder = builder.with_object_store(config.clone());
                }

                match builder.open(table_name).await {
                    Ok(provider) => Ok(Some(Arc::new(provider))),
                    Err(e) => {
                        tracing::warn!(
                            "Failed to open Cayenne table '{table_name}': {e}"
                        );
                        Ok(None)
                    }
                }
            }
            Err(cayenne::catalog::CatalogError::TableNotFound { .. }) => Ok(None),
            Err(e) => Err(Error::TableProvider { source: e }),
        }
    }
}

#[async_trait]
impl SchemaProvider for CayenneSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.tables
            .read()
            .map(|tables| tables.keys().cloned().collect())
            .unwrap_or_default()
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables
            .read()
            .map(|tables| tables.contains_key(name))
            .unwrap_or(false)
    }

    async fn table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        // Check in-memory cache first
        if let Ok(tables) = self.tables.read() {
            if let Some(provider) = tables.get(name) {
                return Ok(Some(Arc::clone(provider)));
            }
        }

        // Try to load from catalog (lazy loading)
        match Self::load_table(&self.catalog, name, self.object_store_config.as_ref()).await {
            Ok(Some(provider)) => {
                if let Ok(mut tables) = self.tables.write() {
                    tables.insert(name.to_string(), Arc::clone(&provider));
                }
                Ok(Some(provider))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(datafusion::error::DataFusionError::External(Box::new(e))),
        }
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> DFResult<Option<Arc<dyn TableProvider>>> {
        match self.tables.write() {
            Ok(mut tables) => Ok(tables.insert(name, table)),
            Err(_) => Err(datafusion::error::DataFusionError::Internal(
                "Failed to acquire write lock on Cayenne tables".to_string(),
            )),
        }
    }

    fn deregister_table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        match self.tables.write() {
            Ok(mut tables) => Ok(tables.remove(name)),
            Err(_) => Err(datafusion::error::DataFusionError::Internal(
                "Failed to acquire write lock on Cayenne tables".to_string(),
            )),
        }
    }
}
