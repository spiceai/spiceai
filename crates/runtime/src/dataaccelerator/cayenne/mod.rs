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

pub(crate) mod s3;

use std::any::Any;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::datatypes::DataType;
use arrow_schema::Schema;
use async_trait::async_trait;
use data_components::delete::DeletionTableProviderAdapter;
use data_components::poly::PolyTableProvider;
use datafusion::common::DFSchema;
use datafusion::common::arrow::datatypes::SchemaRef;
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{CreateExternalTable, TableProviderFilterPushDown};
use datafusion::prelude::Expr;
use datafusion::scalar::ScalarValue;
use datafusion_table_providers::UnsupportedTypeAction;
use runtime_table_partition::Partition;
use runtime_table_partition::creator::filename::{
    encode_key, parse_partition_value, to_hive_partition_dir,
};
use runtime_table_partition::creator::{self, PartitionCreator};
use runtime_table_partition::expression::PartitionedBy;
use runtime_table_partition::provider::PartitionTableProvider;
use snafu::prelude::*;
use tokio::sync::OnceCell;
use util::concat_arrays;

use super::{AccelerationSource, BootstrapStatus, DataAccelerator, upsert_dedup};
use crate::component::dataset::acceleration::{Acceleration, Engine, Mode};
use crate::dataaccelerator::cayenne::s3::{S3_PARAMETERS, S3_PARAMS_LEN};
use crate::dataaccelerator::{FilePathError, snapshots::download_snapshot_if_needed};
use crate::parameters::ParameterSpec;
use crate::register_data_accelerator;
use crate::spice_data_base_path;
use runtime_acceleration::snapshot::{AccelerationEngine, AccelerationLayout};

/// Metadata key to identify the accelerator type in the schema metadata.
const SPICE_ACCELERATOR_METADATA_KEY: &str = "spice.accelerator";

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

    #[snafu(display("{source}"))]
    S3Error { source: s3::Error },
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// Check if a data type is supported by Vortex natively
fn is_vortex_supported_type(data_type: &DataType) -> bool {
    !matches!(
        data_type,
        DataType::Interval(_)
            | DataType::Duration(_)
            | DataType::Map(_, _)
            | DataType::FixedSizeBinary(_)
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

fn parse_usize(acceleration: &Acceleration, key: &str, default: usize) -> usize {
    acceleration
        .params
        .get(key)
        .map_or(default, |v| {
            v.parse::<usize>().unwrap_or_else(|_| {
                tracing::warn!(
                    "An invalid '{key}' value was provided: '{v}'. Expected a positive integer, defaulting to {default}. For details, visit: https://spiceai.org/docs/components/data-accelerators/cayenne#configuration"
                );
                default
            })
        })
}

/// Returns true if the path is a local filesystem path (not a remote object store).
///
/// Local paths include:
/// - Absolute paths: `/data/cayenne`
/// - Relative paths: `./data`
/// - file:// URIs: `file:///data/cayenne`
///
/// Remote paths (S3, etc.) return false.
fn is_local_path(path: &str) -> bool {
    !path.contains("://") || path.starts_with("file://")
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
    /// If `cayenne_s3_zone_ids` is specified (without `cayenne_file_path`), a bucket name will be
    /// auto-generated from the spicepod name and dataset name, and created if it doesn't exist.
    /// The first zone in the comma-separated list is used as the primary zone for reads.
    ///
    /// Order:
    /// 1. `cayenne_file_path` - Custom path (local or S3 Express One Zone)
    /// 2. Auto-generated S3 Express path if `cayenne_s3_zone_ids` is specified (uses first zone)
    /// 3. Default: `spice_data_base_path()/{dataset_name}/`
    pub fn cayenne_data_dir(&self, source: &dyn AccelerationSource) -> Result<String> {
        if !source.is_file_accelerated() {
            return Err(Error::InvalidConfiguration {
                detail: Arc::from("Dataset is not file accelerated"),
            });
        }

        let Some(acceleration) = source.acceleration() else {
            return Err(Error::AccelerationNotEnabled {
                dataset: Arc::from(source.name().to_string()),
            });
        };

        let acceleration_params = acceleration.params.clone();
        let dataset_name = source.name().to_string().replace(['.', '/'], "_");

        if let Some(custom_path) = acceleration_params.get("cayenne_file_path") {
            return Self::resolve_custom_data_path(&dataset_name, custom_path);
        }

        if let Some(zone_ids) = acceleration_params.get("cayenne_s3_zone_ids") {
            // Use the first zone ID as the primary zone for data path
            let primary_zone = zone_ids
                .split(',')
                .next()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| Error::InvalidConfiguration {
                    detail: Arc::from("cayenne_s3_zone_ids is empty or contains no valid zone IDs"),
                })?;
            return Self::resolve_auto_s3_data_path(
                &source.app().name,
                &dataset_name,
                primary_zone,
            );
        }

        Ok(Self::resolve_default_data_path(&dataset_name))
    }

    /// Generates data paths for all configured S3 Express One Zone zones.
    ///
    /// Returns a vector of S3 paths, one for each zone. The first zone is the primary zone
    /// used for reads; all zones are used for writes (ACID replication).
    #[expect(
        dead_code,
        reason = "Will be used when multi-zone write support is implemented"
    )]
    fn cayenne_data_dirs_multi_zone(&self, source: &dyn AccelerationSource) -> Result<Vec<String>> {
        let zone_ids = s3::get_s3_zone_ids(source);
        if zone_ids.is_empty() {
            // No multi-zone config, return single path
            return Ok(vec![self.cayenne_data_dir(source)?]);
        }

        let acceleration = source.acceleration().ok_or(Error::AccelerationNotEnabled {
            dataset: Arc::from(source.name().to_string()),
        })?;

        // If explicit file_path is provided, we can't do multi-zone
        if acceleration.params.contains_key("cayenne_file_path") {
            return Err(Error::InvalidConfiguration {
                detail: Arc::from(
                    "Cannot use 'cayenne_file_path' with multi-zone configuration. \
                    Use 'cayenne_s3_zone_ids' to specify zones and let Spice auto-generate bucket names.",
                ),
            });
        }

        let dataset_name = source.name().to_string().replace(['.', '/'], "_");
        let app_name = source.app().name.clone();

        let paths: Result<Vec<String>, Error> = zone_ids
            .iter()
            .map(|zone_id| {
                let bucket_name =
                    s3::generate_bucket_name(&app_name, &dataset_name, zone_id).context(S3Snafu)?;
                Ok(format!("s3://{bucket_name}/{dataset_name}/"))
            })
            .collect();

        paths
    }

    fn resolve_custom_data_path(dataset_name: &str, custom_path: &str) -> Result<String> {
        s3::validate_file_path(custom_path).context(S3Snafu)?;
        let base = custom_path.trim_end_matches('/');
        Ok(format!("{base}/{dataset_name}/"))
    }

    fn resolve_auto_s3_data_path(
        app_name: &str,
        dataset_name: &str,
        zone_id: &str,
    ) -> Result<String> {
        let bucket_name =
            s3::generate_bucket_name(app_name, dataset_name, zone_id).context(S3Snafu)?;
        Ok(format!("s3://{bucket_name}/{dataset_name}/"))
    }

    fn resolve_default_data_path(dataset_name: &str) -> String {
        format!("{}/{dataset_name}/", spice_data_base_path())
    }

    /// Resolves the metadata directory for Cayenne catalog storage.
    ///
    /// Priority order:
    /// 1. `cayenne_metadata_dir` - Explicit custom metadata directory
    /// 2. `{cayenne_file_path}/metadata` - When `cayenne_file_path` is a local path (not S3)
    /// 3. `{spice_data_base_path()}/metadata` - Default location
    ///
    /// Note: S3 paths are excluded because `SQLite` (used for metadata catalog) cannot run on object storage.
    pub(crate) fn resolve_metadata_dir(acceleration: Option<&Acceleration>) -> String {
        let Some(accel) = acceleration else {
            return format!("{}/metadata", spice_data_base_path());
        };

        if let Some(custom_dir) = accel.params.get("cayenne_metadata_dir") {
            return custom_dir.clone();
        }

        if let Some(file_path) = accel.params.get("cayenne_file_path")
            && is_local_path(file_path)
        {
            let base = file_path.trim_end_matches('/');
            return format!("{base}/metadata");
        }

        format!("{}/metadata", spice_data_base_path())
    }

    fn resolve_storage_config(&self, source: &dyn AccelerationSource) -> Result<String> {
        self.file_path(source)
            .boxed()
            .context(AccelerationCreationFailedSnafu)
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
    ///
    fn get_vortex_config(
        table_name: &str,
        source: &dyn AccelerationSource,
    ) -> cayenne::metadata::VortexConfig {
        let mut config = cayenne::metadata::VortexConfig::default();

        if let Some(acceleration) = source.acceleration() {
            // Parse cache options - use VortexConfig defaults if not specified
            config.footer_cache_mb = parse_usize(
                acceleration,
                "cayenne_footer_cache_mb",
                config.footer_cache_mb,
            );
            config.segment_cache_mb = parse_usize(
                acceleration,
                "cayenne_segment_cache_mb",
                config.segment_cache_mb,
            );

            // Parse file size options
            config.target_vortex_file_size_mb = parse_usize(
                acceleration,
                "cayenne_target_file_size_mb",
                config.target_vortex_file_size_mb,
            );

            // Parse compression strategy
            if let Some(strategy_str) = acceleration.params.get("cayenne_compression_strategy") {
                match strategy_str.to_lowercase().as_str() {
                    "btrblocks" => {
                        config.compression_strategy =
                            cayenne::metadata::CompressionStrategy::Btrblocks;
                    }
                    "zstd" => {
                        config.compression_strategy = cayenne::metadata::CompressionStrategy::Zstd;
                    }
                    _ => {
                        tracing::warn!(
                            "Dataset '{table_name}' contains an invalid `cayenne_compression_strategy` - '{strategy_str}'. Only options of 'btrblocks' or 'zstd' are supported. Defaulting to 'btrblocks'",
                        );
                    }
                }
            }

            // Parse sort columns
            if let Some(sort_cols_str) = acceleration.params.get("sort_columns") {
                config.sort_columns = sort_cols_str
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
            }

            // Parse upload concurrency for parallel file writes
            let parsed_upload_concurrency = parse_usize(
                acceleration,
                "cayenne_upload_concurrency",
                config.upload_concurrency,
            );
            if parsed_upload_concurrency == 0 {
                tracing::warn!(
                    "Invalid cayenne_upload_concurrency value of 0. Using minimum value of 1."
                );
                config.upload_concurrency = 1;
            } else {
                config.upload_concurrency = parsed_upload_concurrency;
            }

            tracing::debug!(
                "Cayenne Vortex config: footer_cache={}MB, segment_cache={}MB, target_file_size={}MB, upload_concurrency={}, sort_columns={:?}, compression_strategy={:?}",
                config.footer_cache_mb,
                config.segment_cache_mb,
                config.target_vortex_file_size_mb,
                config.upload_concurrency,
                config.sort_columns,
                config.compression_strategy
            );
        }

        config
    }

    fn transformed_arrow_schema(
        cmd: &CreateExternalTable,
        source: &dyn AccelerationSource,
    ) -> Result<SchemaRef> {
        let full_schema: arrow::datatypes::Schema = cmd.schema.as_arrow().clone();
        let unsupported_type_action = Self::get_unsupported_type_action(source);
        let transformed_schema =
            transform_schema_for_vortex(&full_schema, unsupported_type_action)?;
        Ok(Arc::new(transformed_schema))
    }

    fn ensure_directory(dir_path: &str) -> Result<PathBuf> {
        // Skip directory creation for S3 object store URLs
        if dir_path.starts_with("s3://") {
            return Ok(PathBuf::from(dir_path));
        }

        let path_buf = PathBuf::from(dir_path);
        if !path_buf.exists() {
            std::fs::create_dir_all(&path_buf)
                .boxed()
                .context(AccelerationCreationFailedSnafu)?;
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
                        cayenne::CayenneCatalog::new(connection_string)
                            .boxed()
                            .context(AccelerationInitializationFailedSnafu)?,
                    ) as Arc<dyn cayenne::MetadataCatalog>;

                    catalog
                        .init()
                        .await
                        .boxed()
                        .context(AccelerationInitializationFailedSnafu)?;

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
    ) -> Result<Arc<cayenne::CayenneTableProvider>> {
        use cayenne::{CayenneTableProviderBuilder, metadata::CreateTableOptions};

        tracing::debug!("create_cayenne_table_provider: starting for table {table_name}");

        // Get metastore type and metadata directory
        let acceleration = source.acceleration();
        let metadata_dir = Self::resolve_metadata_dir(acceleration);
        let metastore_type = acceleration
            .and_then(|a| a.params.get("cayenne_metastore"))
            .map_or("sqlite", String::as_str)
            .to_string();

        // Ensure metadata directory exists
        std::fs::create_dir_all(&metadata_dir)
            .boxed()
            .context(AccelerationCreationFailedSnafu)?;

        // Get or create the shared catalog (lazy initialization)
        let catalog = self
            .get_or_create_catalog(&metadata_dir, &metastore_type)
            .await?;

        // Check if using S3 Express One Zone storage
        let is_s3_express = s3::is_s3_express_data_path(source);
        let vortex_config = Self::get_vortex_config(table_name, source);

        // Build S3 object store if using S3 Express One Zone storage
        let object_store =
            s3::build_s3_object_store(source, CayenneAccelerator::new().cayenne_data_dir(source)?)
                .await
                .context(S3Snafu)?;

        // Log S3 Express configuration
        if is_s3_express {
            tracing::info!(
                "Cayenne acceleration for {} configured with S3 Express One Zone storage (target file size: {} MB)",
                table_name,
                vortex_config.target_vortex_file_size_mb
            );
        }

        let (primary_keys, on_conflict) = if let Some(acceleration) = source.acceleration() {
            // Use configured primary key if provided.
            let pk_vec = acceleration
                .primary_key
                .as_ref()
                .map(|pk| pk.iter().map(std::string::ToString::to_string).collect())
                .unwrap_or_default();

            // Derive on_conflict from acceleration settings.
            let on_conflict = acceleration
                .on_conflict
                .iter()
                .map(|(col_ref, behavior)| {
                    let col =
                        datafusion_table_providers::util::column_reference::ColumnReference::new(
                            col_ref
                                .iter()
                                .map(std::string::ToString::to_string)
                                .collect(),
                        );
                    match behavior {
                        crate::component::dataset::acceleration::OnConflictBehavior::Drop => {
                            datafusion_table_providers::util::on_conflict::OnConflict::DoNothing(
                                col,
                            )
                        }
                        crate::component::dataset::acceleration::OnConflictBehavior::Upsert(
                            _options,
                        ) => datafusion_table_providers::util::on_conflict::OnConflict::Upsert(col),
                    }
                })
                .next();

            (pk_vec, on_conflict)
        } else {
            (Vec::new(), None)
        };

        let table_options = CreateTableOptions {
            table_name: table_name.to_string(),
            schema: Arc::<arrow_schema::Schema>::clone(&schema),
            primary_key: primary_keys,
            on_conflict,
            base_path: dir_path.to_string(),
            partition_column: None, // Non-partitioned table
            vortex_config,
        };

        // Create CayenneTableProvider with object store for S3 Express One Zone
        let mut builder =
            CayenneTableProviderBuilder::new(catalog).with_retention_filters(retention_filters);
        if let Some(object_store) = object_store {
            tracing::info!(
                "Using S3 Express One Zone storage for {} acceleration: {}",
                table_name,
                object_store.url.as_str()
            );
            builder = builder.with_object_store(object_store);
        } else if is_s3_express {
            return Err(Error::AccelerationCreationFailed {
                source: Box::new(std::io::Error::other(
                    "S3 Express One Zone storage detected but object store configuration is missing",
                )),
            });
        }
        tracing::debug!("create_cayenne_table_provider: calling builder.create for {table_name}");
        let cayenne_table = builder
            .create(table_options)
            .await
            .boxed()
            .context(AccelerationCreationFailedSnafu)?;

        tracing::debug!("create_cayenne_table_provider: table {table_name} created successfully");
        Ok(Arc::new(cayenne_table))
    }
}

const PARAMETERS: &[ParameterSpec] = &concat_arrays::<
    ParameterSpec,
    S3_PARAMS_LEN,
    11,
    { S3_PARAMS_LEN + 11 },
>(
    S3_PARAMETERS,
    [
        ParameterSpec::component("file_path")
            .description("Path for storing Cayenne data files (Vortex files). Can be a local path or an S3 Express One Zone path. For S3 Express One Zone, use format: 's3://{bucket-name}--{zone-id}--x-s3/{prefix}/'. When S3 Express One Zone is specified, data files are stored exclusively in S3 while metadata (SQLite) remains on local disk."),
        ParameterSpec::component("metadata_dir")
            .description("Path for storing Cayenne metadata (SQLite catalog). If not specified, defaults to '{cayenne_file_path}/metadata'."),
        ParameterSpec::component("metastore")
            .description("Metastore backend for Cayenne catalog. Options: 'sqlite' (default), 'turso' (requires 'turso' feature enabled at build time)")
            .one_of(&["sqlite", "turso"])
            .default("sqlite"),
        ParameterSpec::runtime("file_watcher"),
        ParameterSpec::component("unsupported_type_action")
            .description("How to handle data types not natively supported by Cayenne (internally using Vortex format) (Time32, Time64, Duration, Interval, Map, etc.). Options: 'string' (convert schema to Utf8, default - requires data source to provide string data), 'error' (fail on unsupported types), 'warn' (include in schema, may fail on insert), 'ignore' (skip unsupported fields)")
            .one_of(&["string", "error", "ignore", "warn"])
            .default("string"),
        ParameterSpec::component("footer_cache_mb")
            .description("Size of the in-memory Vortex footer cache in MB. Larger values improve query performance for repeated scans. Default: 128 MB")
            .default("128"),
        ParameterSpec::component("segment_cache_mb")
            .description("Size of the in-memory Vortex segment cache in MB. Set > 0 to cache decompressed data segments. Default: 256 MB")
            .default("256"),
        ParameterSpec::component("cayenne_target_file_size_mb")
            .description("Target size for Vortex data files in MB. Default: 256 MB. Adjust as needed for S3 Express or remote upload scenarios.")
            .default("256"),
        ParameterSpec::component("sort_columns")
            .description("Comma-separated list of columns to sort data by during inserts (e.g., 'timestamp,user_id')."),
        ParameterSpec::component("compression_strategy")
            .description("Compression strategy to use for Vortex files. Options: 'btrblocks' (default), 'zstd'")
            .one_of(&["btrblocks", "zstd"])
            .default("btrblocks"),
        ParameterSpec::component("cayenne_upload_concurrency")
            .description("Maximum number of concurrent file uploads when writing multiple Vortex files. Default: 4.")
            .default("4"),
    ],
);

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

    fn acceleration_layout(&self, source: &dyn AccelerationSource) -> AccelerationLayout {
        let Ok(data_dir) = self.cayenne_data_dir(source) else {
            return AccelerationLayout::default();
        };

        let metadata_dir = Self::resolve_metadata_dir(source.acceleration());

        AccelerationLayout::cayenne(PathBuf::from(metadata_dir), PathBuf::from(data_dir))
    }

    fn is_initialized(&self, source: &dyn AccelerationSource) -> bool {
        if !source.is_file_accelerated() {
            return true; // memory mode Vortex is always initialized
        }

        // S3 Express One Zone paths are always considered initialized
        // (the bucket/prefix is assumed to exist or will be created by the object store)
        if s3::is_s3_express_data_path(source) {
            // For S3 Express, we need to check if the metadata database exists locally
            let metadata_dir = Self::resolve_metadata_dir(source.acceleration());
            let metadata_db_path = format!("{metadata_dir}/cayenne.db");
            return PathBuf::from(metadata_db_path).exists();
        }

        // For local storage, check if both the data directory and metadata database exist
        let Ok(dir_path) = self.file_path(source) else {
            return false;
        };

        // Check if the data directory exists
        if !PathBuf::from(&dir_path).exists() {
            return false;
        }

        // Also check if the metadata database exists (indicates proper initialization)
        let metadata_dir = Self::resolve_metadata_dir(source.acceleration());
        let metadata_db_path = format!("{metadata_dir}/cayenne.db");
        PathBuf::from(metadata_db_path).exists()
    }

    /// Initializes a `Cayenne` database for the dataset
    /// If the dataset is not file-accelerated, this is a no-op
    /// Creates the data directory if it doesn't exist
    async fn init(
        &self,
        source: &dyn AccelerationSource,
    ) -> Result<BootstrapStatus, Box<dyn std::error::Error + Send + Sync>> {
        tracing::warn!(
            "Cayenne data accelerator (Beta) is in preview and is not recommended for production."
        );

        if !source.is_file_accelerated() {
            return Err(Box::new(Error::InvalidConfiguration {
                detail: Arc::from(
                    "Cayenne data accelerator only supports file mode. Please configure the accelerator with mode: file",
                ),
            }));
        }

        if let Some(acceleration) = source.acceleration() {
            // Validate S3 Express One Zone configuration - only one method allowed
            let has_s3_zone_ids = acceleration.params.contains_key("cayenne_s3_zone_ids");
            let has_s3_express_file_path = acceleration
                .params
                .get("cayenne_file_path")
                .is_some_and(|path| s3::is_s3_express_path(path));

            if has_s3_zone_ids && has_s3_express_file_path {
                return Err(Box::new(Error::InvalidConfiguration {
                    detail: Arc::from(
                        "Cannot specify both 'cayenne_s3_zone_ids' and 'cayenne_file_path' with an S3 Express path. Use either 'cayenne_s3_zone_ids' for auto-generated bucket names, or 'cayenne_file_path' for explicit bucket paths.",
                    ),
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
        }

        let dir_path = self.file_path(source)?;
        let is_s3_express = s3::is_s3_express_data_path(source);

        // Handle S3 Express One Zone configuration
        if is_s3_express {
            // Automatically create the bucket if it doesn't exist and we have the required info
            let (bucket_name, zone_id, region, access_key, secret_key, session_token) =
                s3::get_s3_bucket_info(source, &dir_path).boxed()?;
            if s3::create_s3_express_bucket_if_needed(
                &bucket_name,
                &zone_id,
                &region,
                access_key,
                secret_key,
                session_token,
            )
            .await
            .boxed()?
            {
                tracing::info!("Using S3 Express One Zone storage: {dir_path} (bucket created)");
            } else {
                tracing::info!("Using S3 Express One Zone storage: {dir_path} (bucket exists)");
            }
            tracing::debug!(
                "S3 Express One Zone is optimized for low-latency access within the same AWS Availability Zone. Access from outside AWS may experience higher latency."
            );

            return Ok(BootstrapStatus::none());
        }

        // If mode is FileCreate, delete the existing directory and metadata to start fresh
        if let Some(acceleration) = source.acceleration()
            && acceleration.mode == Mode::FileCreate
        {
            let path_buf = PathBuf::from(&dir_path);
            if path_buf.exists() {
                tracing::warn!(
                    "Cayenne acceleration mode is 'file_create', removing existing directory: {}",
                    dir_path
                );
                tokio::fs::remove_dir_all(&path_buf)
                    .await
                    .boxed()
                    .context(AccelerationInitializationFailedSnafu)?;
            }

            // Also drop the table from metadata catalog to clean up stale metadata
            let metadata_dir = Self::resolve_metadata_dir(Some(acceleration));

            let metastore_type = acceleration
                .params
                .get("cayenne_metastore")
                .map_or("sqlite", String::as_str);

            // Get or create catalog and drop the table if it exists
            if let Ok(catalog) = self
                .get_or_create_catalog(&metadata_dir, metastore_type)
                .await
            {
                let table_name = source.name().to_string();
                match catalog.drop_table(&table_name).await {
                    Ok(true) => {
                        tracing::info!(
                            "Dropped existing Cayenne table metadata for '{table_name}' (file_create mode)"
                        );
                    }
                    Ok(false) => {
                        // Table didn't exist in metadata, nothing to drop
                    }
                    Err(e) => {
                        tracing::warn!(
                            "Failed to drop Cayenne table metadata for '{table_name}': {e}. Continuing anyway."
                        );
                    }
                }
            }
        }

        // Create the vortex data directory if it doesn't exist
        let path_buf = PathBuf::from(&dir_path);
        if !path_buf.exists() {
            tokio::fs::create_dir_all(&path_buf)
                .await
                .boxed()
                .context(AccelerationCreationFailedSnafu)?;
        }

        if let Some(acceleration) = source.acceleration() {
            let metadata_dir = PathBuf::from(Self::resolve_metadata_dir(Some(acceleration)));
            let snapshot_adapter =
                runtime_acceleration::snapshot::AccelerationLayout::cayenne(metadata_dir, path_buf);
            Ok(download_snapshot_if_needed(
                acceleration,
                source,
                snapshot_adapter,
                AccelerationEngine::Cayenne,
            )
            .await)
        } else {
            Ok(BootstrapStatus::none())
        }
    }

    /// Creates a new table in the accelerator engine, returning a `TableProvider` that supports reading and writing.
    /// Cayenne supports file mode and can optionally partition data.
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
            // Non-partitioned table - wrap in PolyTableProvider for proper deletion/retention support
            // Wrap with upsert deduplication if needed based on on_conflict settings
            let (write_provider, delete_provider) = upsert_dedup::wrap_with_upsert_dedup_if_needed(
                cayenne_table,
                &cmd.options,
                cmd.constraints.clone(),
            );

            let mut schema_metadata = HashMap::new();
            schema_metadata.insert(
                SPICE_ACCELERATOR_METADATA_KEY.to_string(),
                "cayenne".to_string(),
            );

            let table_provider = Arc::new(PolyTableProvider::new_with_schema_metadata(
                Arc::clone(&write_provider),
                delete_provider,
                write_provider,
                schema_metadata,
            ));

            Ok(table_provider as Arc<dyn TableProvider>)
        } else {
            // Get metadata catalog for partition tracking
            let metadata_dir = Self::resolve_metadata_dir(source.acceleration());

            // Ensure metadata directory exists
            std::fs::create_dir_all(&metadata_dir)
                .boxed()
                .context(AccelerationCreationFailedSnafu)?;

            // Create a new catalog - it will use WAL mode and busy timeout internally
            let catalog = Arc::new(
                cayenne::CayenneCatalog::new(format!("sqlite://{metadata_dir}/cayenne.db"))
                    .boxed()
                    .context(AccelerationInitializationFailedSnafu)?,
            ) as Arc<dyn cayenne::MetadataCatalog>;

            // Initialize the catalog (creates tables if needed)
            catalog
                .init()
                .await
                .boxed()
                .context(AccelerationInitializationFailedSnafu)?;

            // Get or create table_id from catalog
            let table_metadata = catalog
                .get_table(&table_name)
                .await
                .boxed()
                .context(AccelerationCreationFailedSnafu)?;

            // Build S3 object store if using S3 Express One Zone storage
            let object_store_config = s3::build_s3_object_store(
                source,
                CayenneAccelerator::new().cayenne_data_dir(source)?,
            )
            .await?;

            // Create partition creator
            let unsupported_type_action = Self::get_unsupported_type_action(source);
            let is_s3_express = s3::is_s3_express_data_path(source);
            let vortex_config = Self::get_vortex_config(&table_name, source);

            // Log S3 Express configuration for partitioned tables
            if is_s3_express {
                tracing::info!(
                    "Cayenne acceleration for {} configured with S3 Express One Zone storage (target file size: {} MB)",
                    table_name,
                    vortex_config.target_vortex_file_size_mb
                );
            }

            // Extract primary_key and on_conflict from acceleration settings for partitioned tables
            let (primary_keys, on_conflict) = if let Some(acceleration) = source.acceleration() {
                let pk_vec = acceleration
                    .primary_key
                    .as_ref()
                    .map(|pk| pk.iter().map(std::string::ToString::to_string).collect())
                    .unwrap_or_default();

                let on_conflict = acceleration
                    .on_conflict
                    .iter()
                    .map(|(col_ref, behavior)| {
                        let col =
                            datafusion_table_providers::util::column_reference::ColumnReference::new(
                                col_ref
                                    .iter()
                                    .map(std::string::ToString::to_string)
                                    .collect(),
                            );
                        match behavior {
                            crate::component::dataset::acceleration::OnConflictBehavior::Drop => {
                                datafusion_table_providers::util::on_conflict::OnConflict::DoNothing(
                                    col,
                                )
                            }
                            crate::component::dataset::acceleration::OnConflictBehavior::Upsert(
                                _options,
                            ) => datafusion_table_providers::util::on_conflict::OnConflict::Upsert(col),
                        }
                    })
                    .next();

                (pk_vec, on_conflict)
            } else {
                (Vec::new(), None)
            };

            let creator = Arc::new(CayennePartitionCreator::new(
                table_name,
                PathBuf::from(&dir_path),
                partition_by.clone(),
                Arc::clone(&arrow_schema),
                catalog,
                table_metadata.table_id,
                unsupported_type_action,
                retention_filters,
                vortex_config,
                object_store_config,
                primary_keys,
                on_conflict,
            ));

            // Wrap the base table provider with partitioning logic
            let partition_provider = Arc::new(
                PartitionTableProvider::new(creator, partition_by, arrow_schema)
                    .await
                    .boxed()
                    .context(AccelerationCreationFailedSnafu)?,
            );

            // Wrap with upsert deduplication if needed based on on_conflict settings
            let (write_provider, delete_provider) = upsert_dedup::wrap_with_upsert_dedup_if_needed(
                partition_provider,
                &cmd.options,
                cmd.constraints.clone(),
            );

            let mut schema_metadata = HashMap::new();
            schema_metadata.insert(
                SPICE_ACCELERATOR_METADATA_KEY.to_string(),
                "cayenne".to_string(),
            );

            let table_provider = Arc::new(PolyTableProvider::new_with_schema_metadata(
                Arc::clone(&write_provider),
                delete_provider,
                write_provider,
                schema_metadata,
            ));

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

/// Partition creator for Cayenne accelerator.
///
/// Supports single and composite partition keys (e.g., `partition_by: [year, month, day]`).
/// For composite partitions, data is stored in nested Hive-style directories.
struct CayennePartitionCreator {
    table_name: String,
    base_path: PathBuf,
    /// Partition expressions. For hierarchical partitions like `partition_by: [year, month]`,
    /// this contains all expressions in order.
    partition_by: Vec<PartitionedBy>,
    schema: SchemaRef,
    catalog: Arc<dyn cayenne::MetadataCatalog>,
    table_id: i64,
    unsupported_type_action: UnsupportedTypeAction,
    retention_filters: Vec<Expr>,
    vortex_config: cayenne::metadata::VortexConfig,
    object_store_config: Option<cayenne::metadata::ObjectStoreConfig>,
    primary_key: Vec<String>,
    on_conflict: Option<datafusion_table_providers::util::on_conflict::OnConflict>,
    /// Shared Cayenne context with cache, created once and shared across all partitions.
    context: Arc<cayenne::CayenneContext>,
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
            .field("primary_key", &self.primary_key)
            .field("on_conflict", &self.on_conflict.is_some())
            .field("context", &"<CayenneContext>")
            .finish()
    }
}

impl CayennePartitionCreator {
    #[expect(clippy::too_many_arguments)]
    fn new(
        table_name: String,
        base_path: PathBuf,
        partition_by: Vec<PartitionedBy>,
        schema: SchemaRef,
        catalog: Arc<dyn cayenne::MetadataCatalog>,
        table_id: i64,
        unsupported_type_action: UnsupportedTypeAction,
        retention_filters: Vec<Expr>,
        vortex_config: cayenne::metadata::VortexConfig,
        object_store_config: Option<cayenne::metadata::ObjectStoreConfig>,
        primary_key: Vec<String>,
        on_conflict: Option<datafusion_table_providers::util::on_conflict::OnConflict>,
    ) -> Self {
        // Create shared Cayenne context with cache once, to be shared across all partitions.
        // This ensures all partitions share the same footer/segment caches instead of
        // each partition creating its own cache.
        let context = cayenne::CayenneContext::new(&vortex_config);

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
            primary_key,
            on_conflict,
            context,
        }
    }

    /// Returns the partition column labels for all partition expressions.
    fn partition_column_labels(&self) -> Vec<String> {
        self.partition_by
            .iter()
            .map(|p| match &p.expression {
                Expr::Column(col) => col.name.clone(),
                _ => p.name.clone(),
            })
            .collect()
    }

    /// Generate a unique table name for this partition based on composite key.
    fn partition_table_name(&self, partition_key: &str) -> String {
        // Replace "/" with "_" to create a valid table name
        let safe_key = partition_key.replace('/', "_");
        format!("{}_{}", self.table_name, safe_key)
    }

    /// Generate partition directory path from multiple partition values.
    /// Creates nested Hive-style directories (e.g., `year=2025/month=10/day=15/`).
    fn partition_dir(&self, partition_values: &[ScalarValue]) -> Result<PathBuf, creator::Error> {
        let pairings: Vec<(PartitionedBy, ScalarValue)> = self
            .partition_by
            .iter()
            .cloned()
            .zip(partition_values.iter().cloned())
            .collect();

        let partition_dir = to_hive_partition_dir(&pairings)
            .boxed()
            .context(creator::CreatePartitionSnafu)?;

        Ok(self.base_path.join(partition_dir))
    }
}

#[async_trait]
impl PartitionCreator for CayennePartitionCreator {
    async fn create_partition(
        &self,
        partition_values: Vec<ScalarValue>,
    ) -> Result<Partition, creator::Error> {
        if partition_values.is_empty() {
            return Err(creator::Error::CreatePartition {
                source: "At least one partition value is required".into(),
            });
        }

        if partition_values.len() != self.partition_by.len() {
            return Err(creator::Error::CreatePartition {
                source: format!(
                    "Expected {} partition values but got {} (one per partition_by expression)",
                    self.partition_by.len(),
                    partition_values.len()
                )
                .into(),
            });
        }

        let partition_dir = self.partition_dir(&partition_values)?;
        let partition_path = partition_dir.to_string_lossy().to_string();

        tracing::debug!("creating Cayenne partition at {partition_path}");

        // Create the partition directory (including nested directories for composite partitions)
        std::fs::create_dir_all(&partition_dir)
            .boxed()
            .context(creator::CreatePartitionSnafu)?;

        // Encode partition values as strings for metadata storage
        let partition_value_strings: Vec<String> = partition_values
            .iter()
            .map(encode_key)
            .collect::<Result<Vec<_>, _>>()
            .boxed()
            .context(creator::CreatePartitionSnafu)?;
        let partition_column_names = self.partition_column_labels();

        // Create composite key for table naming (slash-separated values)
        let partition_key = partition_value_strings.join("/");

        // Create partition metadata with composite key support
        let partition_metadata = cayenne::PartitionMetadata::new_composite(
            self.table_id,
            partition_column_names,
            partition_value_strings.clone(),
            partition_path.clone(),
            false, // path_is_relative
        );

        self.catalog
            .add_partition(partition_metadata)
            .await
            .boxed()
            .context(creator::CreatePartitionSnafu)?;

        // Create table options for this partition
        let table_options = cayenne::metadata::CreateTableOptions {
            table_name: self.partition_table_name(&partition_key),
            schema: Arc::clone(&self.schema),
            primary_key: self.primary_key.clone(),
            on_conflict: self.on_conflict.clone(),
            base_path: partition_path.clone(),
            partition_column: None, // Partitions themselves are not partitioned
            vortex_config: self.vortex_config.clone(),
        };

        // Create Cayenne table provider for this partition with S3 support.
        // Use the shared context to share footer/segment caches across partitions.
        let mut builder = cayenne::CayenneTableProviderBuilder::new(Arc::clone(&self.catalog))
            .with_retention_filters(self.retention_filters.clone())
            .with_context(Arc::clone(&self.context));
        if let Some(ref object_store) = self.object_store_config {
            builder = builder.with_object_store(object_store.clone());
        }
        let cayenne_table = builder
            .create(table_options)
            .await
            .boxed()
            .context(creator::CreatePartitionSnafu)?;

        // Wrap in DeletionTableProviderAdapter so get_deletion_provider can find it
        let adapted_table: Arc<dyn TableProvider> =
            Arc::new(DeletionTableProviderAdapter::new(Arc::new(cayenne_table)));

        Ok(Partition {
            partition_values,
            table_provider: adapted_table,
        })
    }

    async fn infer_existing_partitions(&self) -> Result<Vec<Partition>, creator::Error> {
        // Query catalog for existing partitions
        let partitions = self
            .catalog
            .get_partitions(self.table_id)
            .await
            .boxed()
            .context(creator::InferringPartitionsSnafu)?;

        let mut result = Vec::new();

        let df_schema = DFSchema::try_from(Arc::clone(&self.schema))
            .boxed()
            .context(creator::InferringPartitionsSnafu)?;

        let expected_partition_columns = self.partition_column_labels();

        for partition_meta in partitions {
            // Validate that stored partition metadata matches current partition_by expressions.
            // Both the column names and their order must match exactly, otherwise the partition
            // was created with different partition_by configuration and cannot be safely used.
            // Silently skipping mismatched partitions would cause incomplete query results (data loss).
            if partition_meta.partition_columns != expected_partition_columns {
                return Err(creator::Error::PartitionByExpressionsChanged);
            }

            let mut partition_values = Vec::with_capacity(self.partition_by.len());
            for (partition_expr, value_str) in self
                .partition_by
                .iter()
                .zip(&partition_meta.partition_values)
            {
                let partition_value = parse_partition_value(&df_schema, partition_expr, value_str)
                    .map_err(|e| creator::Error::InferringPartitions {
                        source: Box::new(e),
                    })?;
                partition_values.push(partition_value);
            }

            // Create composite key for table lookup
            let partition_key = partition_meta.partition_values.join("/");
            let partition_table_name = self.partition_table_name(&partition_key);

            // Use builder pattern to pass object store config for S3 support.
            // Use the shared context to share footer/segment caches across partitions.
            let mut builder = cayenne::CayenneTableProviderBuilder::new(Arc::clone(&self.catalog))
                .with_retention_filters(self.retention_filters.clone())
                .with_context(Arc::clone(&self.context));
            if let Some(ref object_store) = self.object_store_config {
                builder = builder.with_object_store(object_store.clone());
            }
            let cayenne_table = builder
                .open(&partition_table_name)
                .await
                .boxed()
                .context(creator::InferringPartitionsSnafu)?;

            // Wrap in DeletionTableProviderAdapter so get_deletion_provider can find it
            let adapted_table: Arc<dyn TableProvider> =
                Arc::new(DeletionTableProviderAdapter::new(Arc::new(cayenne_table)));

            result.push(Partition {
                partition_values,
                table_provider: adapted_table,
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

        // Collect all partition columns from all partition expressions
        let partition_columns: std::collections::HashSet<_> = self
            .partition_by
            .iter()
            .flat_map(|p| p.expression.column_refs())
            .collect();

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
    fn test_is_local_path() {
        // Local absolute paths
        assert!(is_local_path("/data/cayenne"));
        assert!(is_local_path("/var/spice/data"));

        // Local relative paths
        assert!(is_local_path("./data"));
        assert!(is_local_path("data/cayenne"));

        // file:// URIs are local
        assert!(is_local_path("file:///data/cayenne"));
        assert!(is_local_path("file://localhost/data"));

        // S3 paths are NOT local
        assert!(!is_local_path("s3://bucket/prefix"));
        assert!(!is_local_path("s3://bucket-usw2-az1-x-s3/prefix"));

        // Other remote schemes are NOT local
        assert!(!is_local_path("gs://bucket/prefix"));
        assert!(!is_local_path("az://container/blob"));
    }

    #[test]
    fn test_resolve_metadata_dir_with_explicit_metadata_dir() {
        let acceleration = Acceleration {
            params: [(
                "cayenne_metadata_dir".to_string(),
                "/custom/metadata".to_string(),
            )]
            .into_iter()
            .collect(),
            ..Default::default()
        };
        assert_eq!(
            CayenneAccelerator::resolve_metadata_dir(Some(&acceleration)),
            "/custom/metadata"
        );
    }

    #[test]
    fn test_resolve_metadata_dir_with_local_file_path() {
        let acceleration = Acceleration {
            params: [(
                "cayenne_file_path".to_string(),
                "/persistent/data".to_string(),
            )]
            .into_iter()
            .collect(),
            ..Default::default()
        };
        assert_eq!(
            CayenneAccelerator::resolve_metadata_dir(Some(&acceleration)),
            "/persistent/data/metadata"
        );
    }

    #[test]
    fn test_resolve_metadata_dir_excludes_s3_path() {
        let acceleration = Acceleration {
            params: [(
                "cayenne_file_path".to_string(),
                "s3://bucket--usw2-az1--x-s3/data".to_string(),
            )]
            .into_iter()
            .collect(),
            ..Default::default()
        };
        // Should fall back to default, not use S3 path
        let result = CayenneAccelerator::resolve_metadata_dir(Some(&acceleration));
        assert!(result.ends_with("/metadata"));
        assert!(!result.starts_with("s3://"));
    }

    #[test]
    fn test_resolve_metadata_dir_explicit_overrides_file_path() {
        // When both are set, cayenne_metadata_dir takes priority
        let acceleration = Acceleration {
            params: [
                (
                    "cayenne_metadata_dir".to_string(),
                    "/explicit/metadata".to_string(),
                ),
                (
                    "cayenne_file_path".to_string(),
                    "/persistent/data".to_string(),
                ),
            ]
            .into_iter()
            .collect(),
            ..Default::default()
        };
        assert_eq!(
            CayenneAccelerator::resolve_metadata_dir(Some(&acceleration)),
            "/explicit/metadata"
        );
    }

    #[test]
    fn test_resolve_metadata_dir_default() {
        // No acceleration - uses default
        let result = CayenneAccelerator::resolve_metadata_dir(None);
        assert!(
            result.ends_with(".spice/data/metadata"),
            "Expected path to end with '.spice/data/metadata', got: {result}"
        );

        // Empty acceleration params - uses default
        let acceleration = Acceleration::default();
        let result = CayenneAccelerator::resolve_metadata_dir(Some(&acceleration));
        assert!(
            result.ends_with(".spice/data/metadata"),
            "Expected path to end with '.spice/data/metadata', got: {result}"
        );
    }

    #[test]
    fn test_resolve_metadata_dir_trims_trailing_slash() {
        let acceleration = Acceleration {
            params: [(
                "cayenne_file_path".to_string(),
                "/persistent/data/".to_string(),
            )]
            .into_iter()
            .collect(),
            ..Default::default()
        };
        // Should not have double slashes
        assert_eq!(
            CayenneAccelerator::resolve_metadata_dir(Some(&acceleration)),
            "/persistent/data/metadata"
        );
    }
}
