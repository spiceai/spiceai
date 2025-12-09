/*
Copyright 2025 The Spice.ai OSS Authors

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

use arrow::datatypes::DataType;
use arrow_schema::Schema;
use async_trait::async_trait;
use aws_sdk_credential_bridge::{S3CredentialProvider, get_bucket_name};
use datafusion::common::DFSchema;
use datafusion::common::arrow::datatypes::SchemaRef;
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{CreateExternalTable, TableProviderFilterPushDown};
use datafusion::prelude::Expr;
use datafusion::scalar::ScalarValue;
use datafusion_table_providers::UnsupportedTypeAction;
use object_store::{ClientOptions, aws::AmazonS3Builder, client::SpawnedReqwestConnector};
use runtime_table_partition::Partition;
use runtime_table_partition::creator::filename::{
    encode_key, parse_partition_value, to_hive_partition_dir,
};
use runtime_table_partition::creator::{self, PartitionCreator};
use runtime_table_partition::expression::PartitionedBy;
use runtime_table_partition::provider::PartitionTableProvider;
use snafu::prelude::*;
use std::any::Any;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::OnceCell;
use url::Url;

use super::{AccelerationSource, DataAccelerator};
use crate::component::dataset::acceleration::{Engine, Mode, RefreshMode};
use crate::dataaccelerator::{FilePathError, snapshots::download_snapshot_if_needed};
use crate::parameters::ParameterSpec;
use crate::register_data_accelerator;
use crate::spice_data_base_path;
use runtime_acceleration::snapshot::SnapshotBehavior;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to create table: {source}"))]
    UnableToCreateTable {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Acceleration creation failed: {source}"))]
    AccelerationCreationFailed {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Acceleration initialization failed: {source}"))]
    AccelerationInitializationFailed {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Acceleration not enabled for dataset: {dataset}"))]
    AccelerationNotEnabled { dataset: Arc<str> },

    #[snafu(display("Invalid Cayenne acceleration configuration: {detail}"))]
    InvalidConfiguration { detail: Arc<str> },

    #[snafu(display(
        "Unsupported data type(s) in schema: {details}. By default, unsupported types cause an error. To convert unsupported types to strings, set 'unsupported_type_action: string'; otherwise, remove the unsupported columns."
    ))]
    UnsupportedDataTypes { details: String },

    #[snafu(display(
        "A single partition by expression is required for Partitioned Cayenne acceleration"
    ))]
    PartitionByRequired,

    #[snafu(display("Failed to create S3 Express One Zone object store: {source}"))]
    S3ObjectStoreCreation {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Invalid S3 Express One Zone URL '{url}': {source}"))]
    InvalidS3Url {
        url: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Standard S3 paths are not supported for Cayenne acceleration. Only S3 Express One Zone is supported. \
        S3 Express One Zone buckets use the naming convention: 's3://{{bucket-name}}--{{zone-id}}--x-s3/'. \
        Received: '{path}'"
    ))]
    StandardS3NotSupported { path: String },
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// Check if a data type is supported by Vortex natively
fn is_vortex_supported_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        // Vortex requires Microsecond timestamps but we accept all timestamp types and convert them.
        DataType::Timestamp(_, _)
            // Float16 will be converted to Float32.
            | DataType::Float16
            // Most other basic types are supported as-is.
            | DataType::Null
            | DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Date32
            | DataType::Date64
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Decimal32(_, _)
            | DataType::Decimal64(_, _)
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
            | DataType::List(_)
            | DataType::FixedSizeList(_, _)
            | DataType::LargeList(_)
            | DataType::Struct(_)
    )
}

/// Transform schema according to `unsupported_type_action` policy
/// Always converts Float16 to Float32 and normalizes timestamps to Microsecond (these are compatible transformations)
/// Handles truly unsupported types according to the action: String (convert to Utf8) or Error (return error)
fn transform_schema_for_vortex(
    schema: &arrow::datatypes::Schema,
    unsupported_type_action: UnsupportedTypeAction,
) -> Result<arrow::datatypes::Schema> {
    let mut unsupported_fields = Vec::new();
    let mut transformed_fields = Vec::new();

    for field in schema.fields() {
        let data_type = field.data_type();

        // Always convert Float16 to Float32 (compatible transformation that Vortex can handle)
        if matches!(data_type, DataType::Float16) {
            tracing::debug!(
                "Converting Float16 field '{}' to Float32 for Vortex compatibility",
                field.name()
            );
            transformed_fields.push(Arc::new(arrow::datatypes::Field::new(
                field.name(),
                DataType::Float32,
                field.is_nullable(),
            )));
            continue;
        }

        // Always convert non-Microsecond timestamps to Microsecond (compatible transformation)
        if let DataType::Timestamp(unit, tz) = data_type
            && !matches!(unit, arrow::datatypes::TimeUnit::Microsecond)
        {
            tracing::debug!(
                "Converting timestamp field '{}' from {:?} to Microsecond precision for Vortex compatibility",
                field.name(),
                unit
            );
            transformed_fields.push(Arc::new(arrow::datatypes::Field::new(
                field.name(),
                DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, tz.clone()),
                field.is_nullable(),
            )));
            continue;
        }

        // Handle truly unsupported types (those that Vortex cannot handle natively)
        if is_vortex_supported_type(data_type) {
            // Supported type, keep as-is
            transformed_fields.push(Arc::clone(field));
        } else {
            match unsupported_type_action {
                UnsupportedTypeAction::String => {
                    tracing::warn!(
                        "Converting unsupported type {:?} for field '{}' to Utf8. Note: Data insertion will require the source to provide data already converted to string format.",
                        data_type,
                        field.name()
                    );
                    transformed_fields.push(Arc::new(arrow::datatypes::Field::new(
                        field.name(),
                        DataType::Utf8,
                        field.is_nullable(),
                    )));
                }
                UnsupportedTypeAction::Error => {
                    unsupported_fields.push(format!("'{}' (type: {:?})", field.name(), data_type));
                }
                UnsupportedTypeAction::Ignore => {
                    tracing::warn!(
                        "Ignoring unsupported type {:?} for field '{}' in Vortex acceleration",
                        data_type,
                        field.name()
                    );
                    // Skip this field entirely
                }
                UnsupportedTypeAction::Warn => {
                    tracing::warn!(
                        "Including unsupported type {:?} for field '{}' - insertion may fail",
                        data_type,
                        field.name()
                    );
                    // Include the field as-is and let Vortex fail during insertion
                    transformed_fields.push(Arc::clone(field));
                }
            }
        }
    }

    // If there are unsupported fields and action is Error, return error
    if !unsupported_fields.is_empty() {
        return Err(Error::UnsupportedDataTypes {
            details: unsupported_fields.join(", "),
        });
    }

    Ok(arrow::datatypes::Schema::new(transformed_fields))
}

pub struct CayenneAccelerator {
    catalog: Arc<OnceCell<Arc<dyn cayenne::MetadataCatalog>>>,
}

impl Default for CayenneAccelerator {
    fn default() -> Self {
        Self::new()
    }
}

impl CayenneAccelerator {
    #[must_use]
    pub fn new() -> Self {
        Self {
            catalog: Arc::new(OnceCell::new()),
        }
    }

    /// Returns the `Cayenne` data directory path that would be used for a file-based `Cayenne` accelerator from this dataset.
    /// Cayenne uses a directory-based approach to support append operations.
    ///
    /// If `cayenne_file_path` is an S3 Express One Zone path (e.g., `s3://{bucket}--{zone-id}--x-s3/`),
    /// data files will be stored exclusively in S3 Express One Zone while metadata remains on local disk.
    ///
    /// Order:
    /// 1. `cayenne_file_path` - Custom path (local or S3 Express One Zone)
    /// 2. Default: `spice_data_base_path()/{dataset_name}/`
    pub fn cayenne_data_dir(&self, source: &dyn AccelerationSource) -> Result<String> {
        if !source.is_file_accelerated() {
            Err(Error::InvalidConfiguration {
                detail: Arc::from("Dataset is not file accelerated"),
            })
        } else if let Some(acceleration) = source.acceleration() {
            let acceleration_params = acceleration.params.clone();

            // Get the sanitized dataset name
            let dataset_name = source.name().to_string().replace(['.', '/'], "_");

            let dir_path = if let Some(custom_path) = acceleration_params.get("cayenne_file_path") {
                // Validate the path - reject standard S3, only allow S3 Express One Zone or local
                Self::validate_file_path(custom_path)?;

                // Check if it's an S3 Express One Zone path
                if Self::is_s3_express_path(custom_path) {
                    tracing::info!(
                        "Using S3 Express One Zone storage for Cayenne data files: {}",
                        custom_path
                    );
                }
                // Add dataset name as a suffix for isolation
                let base = custom_path.trim_end_matches('/');
                format!("{base}/{dataset_name}/")
            } else {
                format!("{}/{}", spice_data_base_path(), dataset_name)
            };

            // Ensure the path ends with a trailing slash for directory operations
            if dir_path.ends_with('/') {
                Ok(dir_path)
            } else {
                Ok(format!("{dir_path}/"))
            }
        } else {
            Err(Error::AccelerationNotEnabled {
                dataset: Arc::from(source.name().to_string()),
            })
        }
    }

    /// Returns true if the path is any S3 path (standard or Express).
    fn is_s3_path(path: &str) -> bool {
        path.starts_with("s3://") || path.starts_with("s3a://")
    }

    /// Returns true if the path is an S3 Express One Zone path.
    ///
    /// S3 Express One Zone buckets have the naming convention: `{base-name}--{zone-id}--x-s3`
    /// Example: `s3://mybucket--usw2-az1--x-s3/prefix/`
    fn is_s3_express_path(path: &str) -> bool {
        path.starts_with("s3://") && path.contains("--x-s3")
    }

    /// Validates that the path is either a local path or an S3 Express One Zone path.
    /// Standard S3 paths are not supported.
    fn validate_file_path(path: &str) -> Result<()> {
        if Self::is_s3_path(path) && !Self::is_s3_express_path(path) {
            return Err(Error::StandardS3NotSupported {
                path: path.to_string(),
            });
        }
        Ok(())
    }

    /// Returns true if the data path for this source is an S3 Express One Zone path.
    fn is_s3_express_data_path(source: &dyn AccelerationSource) -> bool {
        source
            .acceleration()
            .and_then(|a| a.params.get("cayenne_file_path"))
            .is_some_and(|path| Self::is_s3_express_path(path))
    }

    /// Build an S3 object store for S3 Express One Zone storage.
    ///
    /// Returns `None` if the path is not an S3 path, or an error if S3 configuration is invalid.
    #[expect(
        clippy::too_many_lines,
        reason = "S3 object store setup requires extensive configuration"
    )]
    async fn build_s3_object_store(
        source: &dyn AccelerationSource,
    ) -> Result<Option<cayenne::metadata::ObjectStoreConfig>> {
        let data_path = match source.acceleration() {
            Some(a) => a.params.get("cayenne_file_path").cloned(),
            None => None,
        };

        let Some(data_path) = data_path else {
            return Ok(None);
        };

        if !Self::is_s3_express_path(&data_path) {
            return Ok(None);
        }

        tracing::info!(
            "Building S3 Express One Zone object store for path: {}",
            data_path
        );

        // Parse the S3 URL
        let url = Url::parse(&data_path).map_err(|e| Error::InvalidS3Url {
            url: data_path.clone(),
            source: Box::new(e),
        })?;

        // Get bucket name from URL
        let bucket_name = get_bucket_name(&url).map_err(|e| Error::InvalidS3Url {
            url: data_path.clone(),
            source: Box::new(e),
        })?;

        // Extract S3 configuration from acceleration params
        let params = source.acceleration().map(|a| &a.params);

        let s3_region = params.and_then(|p| p.get("s3_region"));
        let s3_endpoint = params.and_then(|p| p.get("s3_endpoint"));
        let s3_key = params.and_then(|p| p.get("s3_key"));
        let s3_secret = params.and_then(|p| p.get("s3_secret"));
        let s3_session_token = params.and_then(|p| p.get("s3_session_token"));
        let s3_auth = params
            .and_then(|p| p.get("s3_auth"))
            .map_or("iam_role", String::as_str);
        let s3_client_timeout = params.and_then(|p| p.get("s3_client_timeout"));
        let s3_allow_http = params
            .and_then(|p| p.get("s3_allow_http"))
            .is_some_and(|v| v.eq_ignore_ascii_case("true"));

        // Build the S3 object store
        let io_runtime = tokio::runtime::Handle::current();
        let mut s3_builder = AmazonS3Builder::from_env()
            .with_bucket_name(bucket_name)
            .with_http_connector(SpawnedReqwestConnector::new(io_runtime))
            .with_allow_http(s3_allow_http);

        let mut client_options = ClientOptions::default();

        if let Some(region) = s3_region {
            s3_builder = s3_builder.with_region(region);
        }

        if let Some(endpoint) = s3_endpoint {
            s3_builder = s3_builder.with_endpoint(endpoint);
            if endpoint.starts_with("http://") {
                client_options = client_options.with_allow_http(true);
            }
        }

        if let Some(timeout) = s3_client_timeout {
            client_options =
                client_options.with_timeout(fundu::parse_duration(timeout).map_err(|e| {
                    Error::S3ObjectStoreCreation {
                        source: Box::new(e),
                    }
                })?);
        }

        let mut load_credentials_from_environment = true;

        // Handle explicit key/secret credentials
        if s3_auth == "key" {
            if let (Some(key), Some(secret)) = (s3_key, s3_secret) {
                s3_builder = s3_builder.with_access_key_id(key);
                s3_builder = s3_builder.with_secret_access_key(secret);
                if let Some(token) = s3_session_token {
                    s3_builder = s3_builder.with_token(token);
                }
                load_credentials_from_environment = false;
            } else {
                return Err(Error::InvalidConfiguration {
                    detail: Arc::from(
                        "S3 auth method 'key' requires both 's3_key' and 's3_secret' parameters",
                    ),
                });
            }
        }

        s3_builder = s3_builder.with_client_options(client_options);

        // Load credentials from environment if not using explicit keys
        if load_credentials_from_environment {
            tracing::debug!("Loading S3 credentials from environment for Cayenne");
            match aws_sdk_credential_bridge::get_or_init_sdk_config().await {
                Ok(Some(sdk_config)) => {
                    if sdk_config.credentials_provider().is_some() {
                        tracing::debug!("Using S3 credentials provider from SDK config");
                        s3_builder = s3_builder.with_credentials(Arc::new(
                            S3CredentialProvider::from_config(sdk_config.as_ref()).map_err(
                                |e| Error::S3ObjectStoreCreation {
                                    source: Box::new(e),
                                },
                            )?,
                        ));
                    }
                }
                Ok(None) => {
                    tracing::warn!(
                        "No AWS SDK credentials available for Cayenne S3 Express storage; assuming public access"
                    );
                }
                Err(err) => {
                    tracing::warn!("Unable to initialize AWS credentials for Cayenne: {err}");
                }
            }
        }

        let store = s3_builder
            .build()
            .map_err(|e| Error::S3ObjectStoreCreation {
                source: Box::new(e),
            })?;

        Ok(Some(cayenne::metadata::ObjectStoreConfig {
            url,
            store: Arc::new(store),
        }))
    }

    fn resolve_storage_config(&self, source: &dyn AccelerationSource) -> Result<String> {
        self.file_path(source)
            .map_err(|err| Error::AccelerationCreationFailed {
                source: Box::new(err),
            })
    }

    fn get_unsupported_type_action(source: &dyn AccelerationSource) -> UnsupportedTypeAction {
        // Check if unsupported_type_action is specified in acceleration params
        if let Some(acceleration) = source.acceleration()
            && let Some(action_str) = acceleration.params.get("unsupported_type_action")
        {
            match action_str.to_lowercase().as_str() {
                "error" => return UnsupportedTypeAction::Error,
                "warn" => return UnsupportedTypeAction::Warn,
                "ignore" => return UnsupportedTypeAction::Ignore,
                "string" => return UnsupportedTypeAction::String,
                _ => {
                    tracing::warn!(
                        "Invalid unsupported_type_action value '{}', defaulting to 'error'",
                        action_str
                    );
                }
            }
        }
        // Default to Error - fail fast when encountering unsupported types
        // This provides clear feedback about schema compatibility issues
        UnsupportedTypeAction::Error
    }

    /// Parse Vortex encoding configuration from acceleration parameters.
    /// This allows fine-grained control over which SIMD-optimized encodings to use.
    fn get_vortex_config(source: &dyn AccelerationSource) -> cayenne::metadata::VortexConfig {
        let mut config = cayenne::metadata::VortexConfig::default();

        if let Some(acceleration) = source.acceleration() {
            // Helper to get enabled/disabled parameter with default
            let get_enabled = |key: &str, default: bool| -> bool {
                acceleration
                    .params
                    .get(key)
                    .map_or(default, |v| util::parse_enabled(v))
            };

            // Helper to parse usize parameter
            let parse_usize = |key: &str, default: usize| -> usize {
                acceleration
                    .params
                    .get(key)
                    .and_then(|v| v.parse::<usize>().ok())
                    .unwrap_or(default)
            };

            // Parse encoding options - use VortexConfig defaults if not specified
            config.enable_alp = get_enabled("cayenne_alp", config.enable_alp);
            config.enable_fsst = get_enabled("cayenne_fsst", config.enable_fsst);
            config.enable_bitpacking = get_enabled("cayenne_bitpacking", config.enable_bitpacking);
            config.enable_delta = get_enabled("cayenne_delta", config.enable_delta);
            config.enable_rle = get_enabled("cayenne_rle", config.enable_rle);
            config.enable_dict = get_enabled("cayenne_dict", config.enable_dict);
            config.enable_for = get_enabled("cayenne_for", config.enable_for);
            config.enable_zigzag = get_enabled("cayenne_zigzag", config.enable_zigzag);

            // Parse cache options - use VortexConfig defaults if not specified
            config.footer_cache_mb = parse_usize("cayenne_footer_cache_mb", config.footer_cache_mb);
            config.segment_cache_mb =
                parse_usize("cayenne_segment_cache_mb", config.segment_cache_mb);

            // Parse file size options
            config.target_vortex_file_size_mb = parse_usize(
                "cayenne_target_file_size_mb",
                config.target_vortex_file_size_mb,
            );

            // Parse sort columns
            if let Some(sort_cols_str) = acceleration.params.get("sort_columns") {
                config.sort_columns = sort_cols_str
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
            }

            tracing::debug!(
                "Cayenne Vortex config: ALP={}, FSST={}, BitPacking={}, Delta={}, RLE={}, Dict={}, FOR={}, ZigZag={}, footer_cache={}MB, segment_cache={}MB, target_file_size={}MB, sort_columns={:?}",
                config.enable_alp,
                config.enable_fsst,
                config.enable_bitpacking,
                config.enable_delta,
                config.enable_rle,
                config.enable_dict,
                config.enable_for,
                config.enable_zigzag,
                config.footer_cache_mb,
                config.segment_cache_mb,
                config.target_vortex_file_size_mb,
                config.sort_columns
            );
        }

        config
    }

    fn transformed_arrow_schema(
        cmd: &CreateExternalTable,
        source: &dyn AccelerationSource,
    ) -> Result<SchemaRef> {
        let full_schema: arrow::datatypes::Schema = cmd.schema.as_ref().clone().into();
        let unsupported_type_action = Self::get_unsupported_type_action(source);
        let transformed_schema =
            transform_schema_for_vortex(&full_schema, unsupported_type_action)?;
        Ok(Arc::new(transformed_schema))
    }

    fn ensure_directory(dir_path: &str) -> Result<PathBuf> {
        // Skip directory creation for S3/object store URLs
        if dir_path.starts_with("s3://") || dir_path.starts_with("s3a://") {
            return Ok(PathBuf::from(dir_path));
        }

        let path_buf = PathBuf::from(dir_path);
        if !path_buf.exists() {
            std::fs::create_dir_all(&path_buf).map_err(|err| {
                Error::AccelerationCreationFailed {
                    source: Box::new(err),
                }
            })?;
        }

        Ok(path_buf)
    }

    async fn get_or_create_catalog(
        &self,
        metadata_dir: &str,
        metastore_type: &str,
    ) -> Result<Arc<dyn cayenne::MetadataCatalog>> {
        let connection_string = match metastore_type {
            "turso" => format!("libsql://{metadata_dir}/cayenne.db"),
            _ => format!("sqlite://{metadata_dir}/cayenne.db"), // Default to SQLite
        };

        self.catalog
            .get_or_try_init(move || {
                let connection_string = connection_string;
                async move {
                    let catalog = Arc::new(
                        cayenne::CayenneCatalog::new(connection_string).map_err(|e| {
                            Error::AccelerationInitializationFailed {
                                source: Box::new(e),
                            }
                        })?,
                    ) as Arc<dyn cayenne::MetadataCatalog>;

                    catalog
                        .init()
                        .await
                        .map_err(|e| Error::AccelerationInitializationFailed {
                            source: Box::new(e),
                        })?;

                    Ok::<Arc<dyn cayenne::MetadataCatalog>, Error>(catalog)
                }
            })
            .await
            .map(Arc::clone)
    }

    async fn create_cayenne_table_provider(
        &self,
        table_name: &str,
        dir_path: &str,
        schema: Arc<Schema>,
        source: &dyn AccelerationSource,
        retention_filters: Vec<Expr>,
    ) -> Result<Arc<dyn TableProvider>> {
        use cayenne::{CayenneTableProvider, metadata::CreateTableOptions};

        // Get metastore type and custom metadata directory if provided
        let (metadata_dir, metastore_type) = if let Some(acceleration) = source.acceleration() {
            let metadata_dir =
                if let Some(custom_dir) = acceleration.params.get("cayenne_metadata_dir") {
                    custom_dir.clone()
                } else {
                    format!("{}/metadata", crate::spice_data_base_path())
                };

            let metastore_type = acceleration
                .params
                .get("cayenne_metastore")
                .map_or("sqlite", String::as_str);

            (metadata_dir, metastore_type.to_string())
        } else {
            (
                format!("{}/metadata", crate::spice_data_base_path()),
                "sqlite".to_string(),
            )
        };

        // Ensure metadata directory exists
        std::fs::create_dir_all(&metadata_dir).map_err(|e| Error::AccelerationCreationFailed {
            source: Box::new(e),
        })?;

        // Get or create the shared catalog (lazy initialization)
        let catalog = self
            .get_or_create_catalog(&metadata_dir, &metastore_type)
            .await?;

        let vortex_config = Self::get_vortex_config(source);

        // Build S3 object store if using S3 Express One Zone storage
        let object_store = Self::build_s3_object_store(source).await?;

        let table_options = CreateTableOptions {
            table_name: table_name.to_string(),
            schema: Arc::<arrow_schema::Schema>::clone(&schema),
            primary_key: vec![], // No PK by default, can be set by caller
            base_path: dir_path.to_string(),
            partition_column: None, // Non-partitioned table
            vortex_config,
        };

        // Create CayenneTableProvider with object store for S3 Express One Zone
        let cayenne_table = CayenneTableProvider::create_table_with_retention_and_object_store(
            catalog,
            table_options,
            retention_filters,
            object_store,
        )
        .await
        .map_err(|e| Error::AccelerationCreationFailed {
            source: Box::new(e),
        })?;

        Ok(Arc::new(cayenne_table))
    }
}

const PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec::component("file_path")
        .description("Path for storing Cayenne data files (Vortex files). Can be a local path or an S3 Express One Zone path. For S3 Express One Zone, use format: 's3://{bucket-name}--{zone-id}--x-s3/{prefix}/'. When S3 Express One Zone is specified, data files are stored exclusively in S3 while metadata (SQLite) remains on local disk."),
    ParameterSpec::component("metastore")
        .description("Metastore backend for Cayenne catalog. Options: 'sqlite' (default), 'turso' (requires 'turso' feature enabled at build time)")
        .default("sqlite"),
    ParameterSpec::runtime("file_watcher"),
    ParameterSpec::component("unsupported_type_action")
        .description("How to handle data types not natively supported by Cayenne (internally using Vortex format) (Time32, Time64, Duration, Interval, Map, etc.). Options: 'string' (convert schema to Utf8, default - requires data source to provide string data), 'error' (fail on unsupported types), 'warn' (include in schema, may fail on insert), 'ignore' (skip unsupported fields)")
        .default("string"),
    // S3 Express One Zone authentication parameters (used when file_path is an S3 Express path)
    ParameterSpec::component("s3_region")
        .description("AWS region for S3 Express One Zone storage. If not specified, uses AWS SDK default.")
        .secret(),
    ParameterSpec::component("s3_endpoint")
        .description("Custom S3 endpoint URL. Required for S3 Express One Zone (format: 's3express-{zone-id}.{region}.amazonaws.com').")
        .secret(),
    ParameterSpec::component("s3_key")
        .description("AWS access key ID for S3 authentication.")
        .secret(),
    ParameterSpec::component("s3_secret")
        .description("AWS secret access key for S3 authentication.")
        .secret(),
    ParameterSpec::component("s3_session_token")
        .description("AWS session token for temporary credentials (optional).")
        .secret(),
    ParameterSpec::component("s3_auth")
        .description("Authentication method for S3 Express One Zone. Options: 'iam_role' (default, uses environment credentials), 'key' (uses explicit s3_key/s3_secret).")
        .default("iam_role")
        .one_of(&["iam_role", "key"]),
    ParameterSpec::runtime("s3_client_timeout")
        .description("Timeout for S3 client operations (e.g., '30s', '5m')."),
    ParameterSpec::runtime("s3_allow_http")
        .description("Allow HTTP (non-TLS) connections to S3. Default: false.")
        .default("false"),
    // Vortex encoding configuration for hardware acceleration
    ParameterSpec::component("cayenne_alp")
        .description("Enable Adaptive Lossless Precision (ALP) encoding for numeric columns. Provides 5-10x compression with SIMD decompression on ARM64 (NEON) and x86_64 (AVX2/AVX-512). Options: 'enabled' (default), 'disabled'")
        .default("enabled"),
    ParameterSpec::component("cayenne_fsst")
        .description("Enable Fast String Suffix Trie (FSST) encoding for string columns. Provides 2-5x compression with SIMD acceleration. Options: 'enabled' (default), 'disabled'")
        .default("enabled"),
    ParameterSpec::component("cayenne_bitpacking")
        .description("Enable BitPacking encoding for integer columns. Provides SIMD-optimized integer unpacking, especially effective on ARM64 with NEON. Options: 'enabled' (default), 'disabled'")
        .default("enabled"),
    ParameterSpec::component("cayenne_delta")
        .description("Enable Delta encoding for sorted/sequential numeric data. Options: 'enabled' (default), 'disabled'")
        .default("enabled"),
    ParameterSpec::component("cayenne_rle")
        .description("Enable Run-Length Encoding (RLE) for data with repeated values. Options: 'enabled' (default), 'disabled'")
        .default("enabled"),
    ParameterSpec::component("cayenne_dict")
        .description("Enable Dictionary encoding for low-cardinality columns. Options: 'enabled' (default), 'disabled'")
        .default("enabled"),
    ParameterSpec::component("cayenne_for")
        .description("Enable Frame-of-Reference (FOR) encoding for integer columns with small ranges. Options: 'enabled' (default), 'disabled'")
        .default("enabled"),
    ParameterSpec::component("cayenne_zigzag")
        .description("Enable ZigZag encoding for signed integers. Options: 'enabled' (default), 'disabled'")
        .default("enabled"),
    ParameterSpec::component("cayenne_footer_cache_mb")
        .description("Size of the in-memory Vortex footer cache in MB. Larger values improve query performance for repeated scans. Default: 64 MB")
        .default("64"),
    ParameterSpec::component("cayenne_segment_cache_mb")
        .description("Size of the in-memory Vortex segment cache in MB. Set > 0 to cache decompressed data segments. Default: 0 (disabled)")
        .default("0"),
    ParameterSpec::component("sort_columns")
        .description("Comma-separated list of columns to sort data by during inserts (e.g., 'timestamp,user_id')."),
];

#[async_trait]
impl DataAccelerator for CayenneAccelerator {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "cayenne"
    }

    fn valid_file_extensions(&self) -> Vec<&'static str> {
        vec!["cayenne"]
    }

    fn file_path(&self, source: &dyn AccelerationSource) -> Result<String, FilePathError> {
        self.cayenne_data_dir(source)
            .map_err(|err| FilePathError::External {
                engine: Engine::Cayenne,
                source: err.into(),
            })
    }

    fn is_initialized(&self, source: &dyn AccelerationSource) -> bool {
        if !source.is_file_accelerated() {
            return true; // memory mode Vortex is always initialized
        }

        // S3 Express One Zone paths are always considered initialized
        // (the bucket/prefix is assumed to exist or will be created by the object store)
        if Self::is_s3_express_data_path(source) {
            return true;
        }

        // otherwise, we're initialized if the directory exists
        if let Ok(dir_path) = self.file_path(source) {
            PathBuf::from(dir_path).exists()
        } else {
            false
        }
    }

    /// Initializes a `Cayenne` database for the dataset
    /// If the dataset is not file-accelerated, this is a no-op
    /// Creates the data directory if it doesn't exist
    async fn init(
        &self,
        source: &dyn AccelerationSource,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        tracing::warn!(
            "Cayenne data accelerator (Alpha) is in preview and should not be used in production."
        );

        if !source.is_file_accelerated() {
            return Err(Box::new(Error::InvalidConfiguration {
                detail: Arc::from(
                    "Cayenne data accelerator only supports file mode. Please configure the accelerator with mode: file",
                ),
            }));
        }

        if let Some(acceleration) = source.acceleration() {
            // Validate refresh_mode - append and full are supported
            if let Some(refresh_mode) = acceleration.refresh_mode
                && refresh_mode != RefreshMode::Append
                && refresh_mode != RefreshMode::Full
            {
                return Err(Box::new(Error::InvalidConfiguration {
                    detail: Arc::from(format!(
                        "Cayenne data accelerator supports append and full refresh modes, but {refresh_mode:?} was specified. Please set refresh_mode to either append or full"
                    )),
                }));
            }

            // Validate that refresh_append_overlap is not specified
            if acceleration.refresh_append_overlap.is_some() {
                return Err(Box::new(Error::InvalidConfiguration {
                    detail: Arc::from(
                        "Cayenne data accelerator does not yet support refresh_append_overlap. Please remove this configuration",
                    ),
                }));
            }

            // Validate that snapshots are not enabled
            if !matches!(acceleration.snapshot_behavior, SnapshotBehavior::Disabled) {
                return Err(Box::new(Error::InvalidConfiguration {
                    detail: Arc::from(
                        "Cayenne data accelerator does not support acceleration snapshots. Please set 'acceleration.snapshots: false' or remove the snapshots configuration",
                    ),
                }));
            }
        }

        let dir_path = self.file_path(source)?;
        let is_s3_express = Self::is_s3_express_data_path(source);

        // Log S3 Express One Zone configuration
        if is_s3_express {
            tracing::warn!(
                "Cayenne S3 Express One Zone storage (Alpha) is experimental and may not be fully functional."
            );
            tracing::debug!(
                "Skipping local directory initialization for S3 Express One Zone path: {}",
                dir_path
            );
            return Ok(());
        }

        // If mode is FileCreate, delete the existing directory to start fresh
        if let Some(acceleration) = source.acceleration()
            && acceleration.mode == Mode::FileCreate
        {
            let path_buf = PathBuf::from(&dir_path);
            if path_buf.exists() {
                tracing::warn!(
                    "Cayenne acceleration mode is 'file_create', removing existing directory: {}",
                    dir_path
                );
                std::fs::remove_dir_all(&path_buf).map_err(|err| {
                    Error::AccelerationInitializationFailed { source: err.into() }
                })?;
            }
        }

        // Create the vortex data directory if it doesn't exist
        let path_buf = PathBuf::from(&dir_path);
        if !path_buf.exists() {
            std::fs::create_dir_all(&path_buf)
                .map_err(|err| Error::AccelerationCreationFailed { source: err.into() })?;
        }

        if let Some(acceleration) = source.acceleration() {
            download_snapshot_if_needed(acceleration, source, path_buf).await;
        }

        Ok(())
    }

    /// Creates a new table in the accelerator engine, returning a `TableProvider` that supports reading and writing.
    /// Cayenne supports file mode and can optionally partition data.
    #[expect(clippy::too_many_lines)]
    async fn create_external_table(
        &self,
        cmd: CreateExternalTable,
        source: Option<&dyn AccelerationSource>,
        partition_by: Vec<PartitionedBy>,
    ) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>> {
        // Cayenne requires a source for file mode with directory-based storage
        let source = source.ok_or_else(|| {
            Box::new(Error::InvalidConfiguration {
                detail: Arc::from("Source required for Cayenne accelerator"),
            }) as Box<dyn std::error::Error + Send + Sync>
        })?;

        let dir_path = self.resolve_storage_config(source).boxed()?;
        let arrow_schema = Self::transformed_arrow_schema(&cmd, source).boxed()?;
        let _ = Self::ensure_directory(&dir_path).boxed()?;

        // Validate append mode configuration: requires either none, primary_key or time_column, but not both
        if let Some(acceleration) = source.acceleration()
            && let Some(refresh_mode) = acceleration.refresh_mode
            && refresh_mode == RefreshMode::Append
        {
            // Get primary keys from constraints
            let arrow_schema_for_pk = Arc::new(cmd.schema.as_arrow().clone());
            let primary_keys = if cmd.constraints.is_empty() {
                Vec::new()
            } else {
                super::get_primary_keys_from_constraints(&cmd.constraints, &arrow_schema_for_pk)
            };
            let has_primary_key = !primary_keys.is_empty();

            // Get time_column from the source via the trait method
            let has_time_column = source.time_column().is_some();

            // Validate: must have exactly one (not both, not neither)
            match (has_primary_key, has_time_column) {
                (false, false) => {
                    return Err(Box::new(Error::InvalidConfiguration {
                        detail: Arc::from(
                            "Append mode requires either primary_key or time_column to be specified. \
                            Please add one of these to your dataset configuration.",
                        ),
                    })
                        as Box<dyn std::error::Error + Send + Sync>);
                }
                (true, true) => {
                    return Err(Box::new(Error::InvalidConfiguration {
                        detail: Arc::from(
                            "Append mode currently cannot have both primary_key and time_column specified. \
                            Please specify only one of these in your dataset configuration.",
                        ),
                    })
                        as Box<dyn std::error::Error + Send + Sync>);
                }
                (true, false) => {
                    tracing::info!(
                        "Append mode for dataset '{}': using primary_key {:?} for deduplication",
                        source.name(),
                        primary_keys
                    );
                }
                (false, true) => {
                    tracing::info!(
                        "Append mode for dataset '{}': using time_column for append operations",
                        source.name()
                    );
                }
            }
        }

        // Get the table name from the source
        let table_name = source.name().to_string();

        // Parse retention SQL once so it can be reused for partitioned tables.
        let retention_filters = if let Some(acceleration) = source.acceleration() {
            acceleration
                .retention_sql
                .as_deref()
                .map(str::trim)
                .filter(|sql| !sql.is_empty())
                .map(|retention_sql| {
                    match crate::datafusion::retention_sql::parse_retention_sql(
                        source.name(),
                        retention_sql,
                        Arc::clone(&arrow_schema),
                    ) {
                        Ok(parsed) => vec![parsed.delete_expr],
                        Err(err) => {
                            tracing::warn!(
                                dataset = %source.name(),
                                "Failed to parse retention_sql: {err}. Retention SQL will be skipped."
                            );
                            Vec::new()
                        }
                    }
                })
                .unwrap_or_default()
        } else {
            Vec::new()
        };

        // Always create the base Cayenne table provider
        let cayenne_table = self
            .create_cayenne_table_provider(
                &table_name,
                &dir_path,
                Arc::clone(&arrow_schema),
                source,
                retention_filters.clone(),
            )
            .await
            .boxed()?;

        // If partitioning is requested, wrap with PartitionTableProvider
        if partition_by.is_empty() {
            // Non-partitioned table - return base provider directly
            Ok(cayenne_table)
        } else {
            let partition_by_first = partition_by.first().cloned().ok_or_else(|| {
                Box::new(Error::PartitionByRequired) as Box<dyn std::error::Error + Send + Sync>
            })?;

            // Get metadata catalog for partition tracking
            let metadata_dir = if let Some(acceleration) = source.acceleration() {
                if let Some(custom_dir) = acceleration.params.get("cayenne_metadata_dir") {
                    custom_dir.clone()
                } else {
                    format!("{}/metadata", crate::spice_data_base_path())
                }
            } else {
                format!("{}/metadata", crate::spice_data_base_path())
            };

            // Ensure metadata directory exists
            std::fs::create_dir_all(&metadata_dir).map_err(|e| {
                Error::AccelerationCreationFailed {
                    source: Box::new(e),
                }
            })?;

            // Create a new catalog - it will use WAL mode and busy timeout internally
            let catalog = Arc::new(
                cayenne::CayenneCatalog::new(format!("sqlite://{metadata_dir}/cayenne.db"))
                    .map_err(|e| Error::AccelerationInitializationFailed {
                        source: Box::new(e),
                    })?,
            ) as Arc<dyn cayenne::MetadataCatalog>;

            // Initialize the catalog (creates tables if needed)
            catalog
                .init()
                .await
                .map_err(|e| Error::AccelerationInitializationFailed {
                    source: Box::new(e),
                })?;

            // Get or create table_id from catalog
            let table_metadata = catalog.get_table(&table_name).await.map_err(|e| {
                Error::AccelerationCreationFailed {
                    source: Box::new(e),
                }
            })?;

            // Build S3 object store if using S3 Express One Zone storage
            let object_store_config = Self::build_s3_object_store(source).await?;

            // Create partition creator
            let unsupported_type_action = Self::get_unsupported_type_action(source);
            let vortex_config = Self::get_vortex_config(source);
            let creator = Arc::new(CayennePartitionCreator::new(
                table_name,
                PathBuf::from(&dir_path),
                partition_by_first,
                Arc::clone(&arrow_schema),
                catalog,
                table_metadata.table_id,
                unsupported_type_action,
                retention_filters,
                vortex_config,
                object_store_config,
            ));

            // Wrap the base table provider with partitioning logic
            let table_provider = Arc::new(
                PartitionTableProvider::new(creator, partition_by, arrow_schema)
                    .await
                    .map_err(|e| Error::AccelerationCreationFailed {
                        source: Box::new(e),
                    })?,
            );

            Ok(table_provider as Arc<dyn TableProvider>)
        }
    }

    fn prefix(&self) -> &'static str {
        "cayenne"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS
    }

    async fn shutdown(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        tracing::debug!("Cayenne accelerator shutdown: starting catalog shutdown");

        // Get the catalog if it was initialized
        let catalog = self.catalog.get().map(Arc::clone);

        if let Some(catalog) = catalog {
            // Run shutdown on the catalog to flush WAL and optimize
            catalog.shutdown().await.map_err(|e| {
                tracing::warn!("Failed to shutdown Cayenne catalog: {e}");
                Box::new(e) as Box<dyn std::error::Error + Send + Sync>
            })?;
            tracing::debug!("Cayenne accelerator shutdown: complete");
        } else {
            tracing::debug!("Cayenne catalog was never initialized, skipping shutdown");
        }

        Ok(())
    }
}

/// Partition creator for Cayenne accelerator
struct CayennePartitionCreator {
    table_name: String,
    base_path: PathBuf,
    partition_by: PartitionedBy,
    schema: SchemaRef,
    catalog: Arc<dyn cayenne::MetadataCatalog>,
    table_id: i64,
    unsupported_type_action: UnsupportedTypeAction,
    retention_filters: Vec<Expr>,
    vortex_config: cayenne::metadata::VortexConfig,
    object_store_config: Option<cayenne::metadata::ObjectStoreConfig>,
}

impl std::fmt::Debug for CayennePartitionCreator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CayennePartitionCreator")
            .field("table_name", &self.table_name)
            .field("base_path", &self.base_path)
            .field("partition_by", &self.partition_by)
            .field("schema", &self.schema)
            .field("catalog", &"<dyn MetadataCatalog>")
            .field("table_id", &self.table_id)
            .field("unsupported_type_action", &self.unsupported_type_action)
            .field("retention_filters", &self.retention_filters.len())
            .field("vortex_config", &"<VortexConfig>")
            .field("object_store_config", &self.object_store_config.is_some())
            .finish()
    }
}

impl CayennePartitionCreator {
    #[expect(clippy::too_many_arguments)]
    fn new(
        table_name: String,
        base_path: PathBuf,
        partition_by: PartitionedBy,
        schema: SchemaRef,
        catalog: Arc<dyn cayenne::MetadataCatalog>,
        table_id: i64,
        unsupported_type_action: UnsupportedTypeAction,
        retention_filters: Vec<Expr>,
        vortex_config: cayenne::metadata::VortexConfig,
        object_store_config: Option<cayenne::metadata::ObjectStoreConfig>,
    ) -> Self {
        Self {
            table_name,
            base_path,
            partition_by,
            schema,
            catalog,
            table_id,
            unsupported_type_action,
            retention_filters,
            vortex_config,
            object_store_config,
        }
    }

    fn partition_column_label(&self) -> &str {
        match &self.partition_by.expression {
            Expr::Column(col) => col.name.as_str(),
            _ => self.partition_by.name.as_str(),
        }
    }

    fn partition_table_name(&self, partition_value: &str) -> String {
        format!("{}_{}", self.table_name, partition_value)
    }

    /// Generate partition directory path from partition value
    fn partition_dir(&self, partition_value: &ScalarValue) -> Result<PathBuf, creator::Error> {
        let partition_dir =
            to_hive_partition_dir(&[(self.partition_by.clone(), partition_value.clone())])
                .map_err(|e| creator::Error::CreatePartition {
                    source: Box::new(e),
                })?;
        Ok(self.base_path.join(partition_dir))
    }
}

#[async_trait]
impl PartitionCreator for CayennePartitionCreator {
    async fn create_partition(
        &self,
        partition_value: ScalarValue,
    ) -> Result<Partition, creator::Error> {
        let partition_dir = self.partition_dir(&partition_value)?;
        let partition_path = partition_dir.to_string_lossy().to_string();

        tracing::debug!("creating Cayenne partition at {partition_path}");

        // Create the partition directory
        std::fs::create_dir_all(&partition_dir).map_err(|e| creator::Error::CreatePartition {
            source: Box::new(e),
        })?;

        // Create partition metadata in catalog
        let partition_value_str =
            encode_key(&partition_value).map_err(|e| creator::Error::CreatePartition {
                source: Box::new(e),
            })?;
        let partition_column_name = self.partition_column_label().to_string();

        let partition_metadata = cayenne::PartitionMetadata {
            partition_id: 0, // Will be assigned by catalog
            table_id: self.table_id,
            partition_column: partition_column_name,
            partition_value: partition_value_str.clone(),
            path: partition_path.clone(),
            path_is_relative: false,
            record_count: 0,    // Will be updated as data is written
            file_size_bytes: 0, // Will be updated as data is written
        };

        self.catalog
            .add_partition(partition_metadata)
            .await
            .map_err(|e| creator::Error::CreatePartition {
                source: Box::new(e),
            })?;

        // Create table options for this partition
        let table_options = cayenne::metadata::CreateTableOptions {
            table_name: self.partition_table_name(&partition_value_str),
            schema: Arc::clone(&self.schema),
            primary_key: vec![],
            base_path: partition_path.clone(),
            partition_column: None, // Partitions themselves are not partitioned
            vortex_config: self.vortex_config.clone(),
        };

        // Create Cayenne table provider for this partition with S3 support
        let cayenne_table =
            cayenne::CayenneTableProvider::create_table_with_retention_and_object_store(
                Arc::clone(&self.catalog),
                table_options,
                self.retention_filters.clone(),
                self.object_store_config.clone(),
            )
            .await
            .map_err(|e| creator::Error::CreatePartition {
                source: Box::new(e),
            })?;

        Ok(Partition {
            partition_value,
            table_provider: Arc::new(cayenne_table),
        })
    }

    async fn infer_existing_partitions(&self) -> Result<Vec<Partition>, creator::Error> {
        // Query catalog for existing partitions
        let partitions = self
            .catalog
            .get_partitions(self.table_id)
            .await
            .map_err(|e| creator::Error::InferringPartitions {
                source: Box::new(e),
            })?;

        let mut result = Vec::new();

        let df_schema = DFSchema::try_from(Arc::clone(&self.schema)).map_err(|e| {
            creator::Error::InferringPartitions {
                source: Box::new(e),
            }
        })?;

        for partition_meta in partitions {
            // Parse partition value using proper NULL handling
            let partition_value = parse_partition_value(
                &df_schema,
                &self.partition_by,
                &partition_meta.partition_value,
            )
            .map_err(|e| creator::Error::InferringPartitions {
                source: Box::new(e),
            })?;

            // Create Cayenne table provider for this partition
            let partition_table_name = self.partition_table_name(&partition_meta.partition_value);
            let cayenne_table = cayenne::CayenneTableProvider::new_with_retention(
                &partition_table_name,
                Arc::clone(&self.catalog),
                self.retention_filters.clone(),
            )
            .await
            .map_err(|e| creator::Error::InferringPartitions {
                source: Box::new(e),
            })?;

            result.push(Partition {
                partition_value,
                table_provider: Arc::new(cayenne_table),
            });
        }

        Ok(result)
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>, DataFusionError> {
        // Partition pruning works for filters on partition columns, even though
        // Cayenne doesn't have native filter pushdown to the storage layer
        use datafusion::logical_expr::TableProviderFilterPushDown;

        let partition_columns = self.partition_by.expression.column_refs();

        Ok(filters
            .iter()
            .map(|filter| {
                let filter_columns = filter.column_refs();

                // Check if filter columns match partition columns (ignoring table qualifiers)
                // Both `order_date` and `table.order_date` should match partition column `order_date`
                let matches_partition_cols = filter_columns.is_empty()
                    || filter_columns.iter().all(|filter_col| {
                        partition_columns
                            .iter()
                            .any(|part_col| filter_col.name == part_col.name)
                    });

                // If filter references partition columns or contains the partition expression,
                // it can be used for partition pruning
                if matches_partition_cols {
                    TableProviderFilterPushDown::Inexact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }
}

register_data_accelerator!(Engine::Cayenne, CayenneAccelerator);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::component::dataset::acceleration::{Acceleration, Mode};
    use crate::component::dataset::builder::DatasetBuilder;
    use app::AppBuilder;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_cayenne_file_path_generation() {
        let app = AppBuilder::new("test").build();
        let rt = crate::Runtime::builder().build().await;

        let mut dataset = DatasetBuilder::try_new(
            "cayenne_data_accelerator_test".to_string(),
            "cayenne_data_accelerator_test",
        )
        .expect("Failed to create builder")
        .with_app(Arc::new(app))
        .with_runtime(Arc::new(rt))
        .build()
        .expect("Failed to build dataset");

        dataset.acceleration = Some(Acceleration {
            engine: Engine::Cayenne,
            mode: Mode::File,
            ..Default::default()
        });

        let accelerator = CayenneAccelerator::new();
        let data_dir = accelerator.cayenne_data_dir(&dataset);

        let dir_path = match data_dir {
            Ok(path) => path,
            Err(err) => panic!("Expected Cayenne data directory to resolve, but got {err}"),
        };
        assert!(dir_path.contains("cayenne_data_accelerator_test"));
        assert!(dir_path.ends_with('/'));
    }

    #[test]
    fn test_is_s3_path() {
        // S3 paths
        assert!(CayenneAccelerator::is_s3_path("s3://bucket/prefix/"));
        assert!(CayenneAccelerator::is_s3_path(
            "s3://mybucket--usw2-az1--x-s3/data/"
        ));
        assert!(CayenneAccelerator::is_s3_path("s3a://bucket/prefix/"));

        // Non-S3 paths
        assert!(!CayenneAccelerator::is_s3_path("/local/path/data/"));
        assert!(!CayenneAccelerator::is_s3_path("./relative/path/"));
        assert!(!CayenneAccelerator::is_s3_path("file:///local/path/"));
        assert!(!CayenneAccelerator::is_s3_path("gs://bucket/prefix/"));
        assert!(!CayenneAccelerator::is_s3_path("az://container/prefix/"));
    }

    #[test]
    fn test_is_s3_express_path() {
        // Valid S3 Express One Zone paths
        assert!(CayenneAccelerator::is_s3_express_path(
            "s3://mybucket--usw2-az1--x-s3/prefix/"
        ));
        assert!(CayenneAccelerator::is_s3_express_path(
            "s3://data-bucket--use1-az4--x-s3/"
        ));
        assert!(CayenneAccelerator::is_s3_express_path(
            "s3://my-bucket-name--euw1-az2--x-s3/some/nested/path/"
        ));

        // Standard S3 paths (not Express)
        assert!(!CayenneAccelerator::is_s3_express_path(
            "s3://mybucket/prefix/"
        ));
        assert!(!CayenneAccelerator::is_s3_express_path(
            "s3://mybucket-with-dashes/prefix/"
        ));
        assert!(!CayenneAccelerator::is_s3_express_path(
            "s3://mybucket--partial/prefix/"
        ));

        // Non-S3 paths
        assert!(!CayenneAccelerator::is_s3_express_path("/local/path/"));
        assert!(!CayenneAccelerator::is_s3_express_path(
            "s3a://mybucket--usw2-az1--x-s3/prefix/"
        ));
    }

    #[test]
    fn test_validate_file_path_accepts_local_paths() {
        CayenneAccelerator::validate_file_path("/local/path/data/")
            .expect("local absolute path should be valid");
        CayenneAccelerator::validate_file_path("./relative/path/")
            .expect("relative path should be valid");
        CayenneAccelerator::validate_file_path("/var/spice/data/")
            .expect("another local path should be valid");
    }

    #[test]
    fn test_validate_file_path_accepts_s3_express() {
        CayenneAccelerator::validate_file_path("s3://mybucket--usw2-az1--x-s3/prefix/")
            .expect("S3 Express One Zone path should be valid");
        CayenneAccelerator::validate_file_path("s3://data--use1-az4--x-s3/cayenne/")
            .expect("another S3 Express One Zone path should be valid");
    }

    #[test]
    fn test_validate_file_path_rejects_standard_s3() {
        // Standard S3 paths should be rejected
        let result = CayenneAccelerator::validate_file_path("s3://mybucket/prefix/");
        assert!(result.is_err());
        let err = result.expect_err("expected error");
        assert!(
            matches!(err, Error::StandardS3NotSupported { .. }),
            "Expected StandardS3NotSupported error, got: {err:?}"
        );

        let result = CayenneAccelerator::validate_file_path("s3://my-data-bucket/cayenne/data/");
        assert!(result.is_err());

        // s3a:// scheme should also be rejected (not S3 Express)
        let result = CayenneAccelerator::validate_file_path("s3a://mybucket/prefix/");
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_file_path_error_message() {
        let result = CayenneAccelerator::validate_file_path("s3://regular-bucket/data/");
        let err = result.expect_err("expected error");
        let error_message = err.to_string();

        assert!(
            error_message.contains("Standard S3 paths are not supported"),
            "Error message should mention standard S3 not supported: {error_message}"
        );
        assert!(
            error_message.contains("S3 Express One Zone"),
            "Error message should mention S3 Express One Zone: {error_message}"
        );
        assert!(
            error_message.contains("--x-s3"),
            "Error message should show the bucket naming convention: {error_message}"
        );
        assert!(
            error_message.contains("s3://regular-bucket/data/"),
            "Error message should include the invalid path: {error_message}"
        );
    }
}
