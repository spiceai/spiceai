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
use datafusion::common::DFSchema;
use datafusion::common::arrow::datatypes::SchemaRef;
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{CreateExternalTable, ExprSchemable, TableProviderFilterPushDown};
use datafusion::prelude::Expr;
use datafusion::scalar::ScalarValue;
use datafusion_table_providers::UnsupportedTypeAction;
use runtime_table_partition::Partition;
use runtime_table_partition::creator::{self, PartitionCreator};
use runtime_table_partition::expression::PartitionedBy;
use runtime_table_partition::provider::PartitionTableProvider;
use snafu::prelude::*;
use std::any::Any;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::OnceCell;

use super::{AccelerationSource, DataAccelerator};
use crate::component::dataset::acceleration::{Engine, RefreshMode};
use crate::dataaccelerator::{FilePathError, snapshots::download_snapshot_if_needed};
use crate::parameters::ParameterSpec;
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
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
            | DataType::List(_)
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
    pub fn cayenne_data_dir(&self, source: &dyn AccelerationSource) -> Result<String> {
        if !source.is_file_accelerated() {
            Err(Error::InvalidConfiguration {
                detail: Arc::from("Dataset is not file accelerated"),
            })
        } else if let Some(acceleration) = source.acceleration() {
            let acceleration_params = acceleration.params.clone();

            // Get the sanitized dataset name
            let dataset_name = source.name().to_string().replace(['.', '/'], "_");

            // Use file_path if provided as base, otherwise use default: spice_data_base_path() + dataset_name
            let dir_path = if let Some(custom_path) = acceleration_params.get("cayenne_file_path") {
                custom_path.clone()
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

            // Parse encoding options
            config.enable_alp = get_enabled("cayenne_alp", true);
            config.enable_fsst = get_enabled("cayenne_fsst", true);
            config.enable_bitpacking = get_enabled("cayenne_bitpacking", true);
            config.enable_delta = get_enabled("cayenne_delta", true);
            config.enable_rle = get_enabled("cayenne_rle", true);
            config.enable_dict = get_enabled("cayenne_dict", true);
            config.enable_for = get_enabled("cayenne_for", true);
            config.enable_zigzag = get_enabled("cayenne_zigzag", true);

            // Parse cache options
            config.footer_cache_mb = parse_usize("cayenne_footer_cache_mb", 64);
            config.segment_cache_mb = parse_usize("cayenne_segment_cache_mb", 0);

            tracing::info!(
                "Cayenne Vortex config: ALP={}, FSST={}, BitPacking={}, Delta={}, RLE={}, Dict={}, FOR={}, ZigZag={}, footer_cache={}MB, segment_cache={}MB",
                config.enable_alp,
                config.enable_fsst,
                config.enable_bitpacking,
                config.enable_delta,
                config.enable_rle,
                config.enable_dict,
                config.enable_for,
                config.enable_zigzag,
                config.footer_cache_mb,
                config.segment_cache_mb
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
                let connection_string = connection_string.clone();
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

        let table_options = CreateTableOptions {
            table_name: table_name.to_string(),
            schema: Arc::<arrow_schema::Schema>::clone(&schema),
            primary_key: vec![], // No PK by default, can be set by caller
            base_path: dir_path.to_string(),
            partition_column: None, // Non-partitioned table
            vortex_config,
        };

        // Create CayenneTableProvider
        let cayenne_table = CayenneTableProvider::create_table_with_retention(
            catalog,
            table_options,
            retention_filters,
        )
        .await
        .map_err(|e| Error::AccelerationCreationFailed {
            source: Box::new(e),
        })?;

        Ok(Arc::new(cayenne_table))
    }
}

const PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec::component("file_path"),
    ParameterSpec::component("metastore")
        .description("Metastore backend for Cayenne catalog. Options: 'sqlite' (default), 'turso' (requires 'turso' feature enabled at build time)")
        .default("sqlite"),
    ParameterSpec::runtime("file_watcher"),
    ParameterSpec::component("unsupported_type_action")
        .description("How to handle data types not natively supported by Cayenne (internally using Vortex format) (Time32, Time64, Duration, Interval, Map, etc.). Options: 'string' (convert schema to Utf8, default - requires data source to provide string data), 'error' (fail on unsupported types), 'warn' (include in schema, may fail on insert), 'ignore' (skip unsupported fields)")
        .default("string"),
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
            if !matches!(acceleration.snapshots, SnapshotBehavior::Disabled) {
                return Err(Box::new(Error::InvalidConfiguration {
                    detail: Arc::from(
                        "Cayenne data accelerator does not support acceleration snapshots. Please set 'acceleration.snapshots: false' or remove the snapshots configuration",
                    ),
                }));
            }
        }

        let dir_path = self.file_path(source)?;

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
    #[allow(clippy::too_many_lines)]
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
            .finish()
    }
}

impl CayennePartitionCreator {
    #[allow(clippy::too_many_arguments)]
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

    fn partition_data_type(&self) -> Result<DataType, creator::Error> {
        if let Ok(field) = self.schema.field_with_name(self.partition_column_label()) {
            return Ok(field.data_type().clone());
        }

        let df_schema = DFSchema::try_from(Arc::clone(&self.schema)).map_err(|e| {
            creator::Error::InferringPartitions {
                source: Box::new(e),
            }
        })?;

        self.partition_by
            .expression
            .data_type_and_nullable(&df_schema)
            .map(|(data_type, _)| data_type)
            .map_err(|e| creator::Error::InferringPartitions {
                source: Box::new(e),
            })
    }

    /// Generate partition directory path from partition value
    fn partition_dir(&self, partition_value: &ScalarValue) -> PathBuf {
        let partition_str = partition_value.to_string();
        let partition_column_name = self.partition_column_label();

        // Use Hive-style partitioning: partition_column=value
        let partition_name = format!("{partition_column_name}={partition_str}");
        self.base_path.join(partition_name)
    }
}

#[async_trait]
impl PartitionCreator for CayennePartitionCreator {
    async fn create_partition(
        &self,
        partition_value: ScalarValue,
    ) -> Result<Partition, creator::Error> {
        let partition_dir = self.partition_dir(&partition_value);
        let partition_path = partition_dir.to_string_lossy().to_string();

        tracing::debug!("creating Cayenne partition at {partition_path}");

        // Create the partition directory
        std::fs::create_dir_all(&partition_dir).map_err(|e| creator::Error::CreatePartition {
            source: Box::new(e),
        })?;

        // Create partition metadata in catalog
        let partition_value_str = partition_value.to_string();
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

        // Create Cayenne table provider for this partition
        let cayenne_table = cayenne::CayenneTableProvider::create_table_with_retention(
            Arc::clone(&self.catalog),
            table_options,
            self.retention_filters.clone(),
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

        let partition_data_type = self.partition_data_type()?;

        for partition_meta in partitions {
            // Parse partition value
            let partition_value = ScalarValue::try_from_string(
                partition_meta.partition_value.clone(),
                &partition_data_type,
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
        // Cayenne doesn't support filter pushdown yet, but partition pruning works
        Ok(vec![
            TableProviderFilterPushDown::Unsupported;
            filters.len()
        ])
    }
}

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
}
