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
use object_store::{
    ClientOptions, RetryConfig, aws::AmazonS3Builder, client::SpawnedReqwestConnector,
};
use runtime_secrets::get_params_with_secrets;
use runtime_table_partition::Partition;
use runtime_table_partition::creator::filename::{
    encode_key, parse_partition_value, to_hive_partition_dir,
};
use runtime_table_partition::creator::{self, PartitionCreator};
use runtime_table_partition::expression::PartitionedBy;
use runtime_table_partition::provider::PartitionTableProvider;
use secrecy::ExposeSecret;
use snafu::prelude::*;
use std::any::Any;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::OnceCell;
use url::Url;

use super::{AccelerationSource, DataAccelerator};
use crate::component::dataset::acceleration::{Acceleration, Engine, Mode, RefreshMode};
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

    #[snafu(display("Failed to create S3 Express One Zone directory bucket '{bucket}': {source}"))]
    S3DirectoryBucketCreation {
        bucket: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "AWS authentication failed for S3 Express One Zone. Your AWS credentials may be expired or invalid."
    ))]
    S3AuthenticationFailed,

    #[snafu(display(
        "Cannot determine S3 Express One Zone bucket info: {reason}. \
        Either provide a valid 'cayenne_file_path' with an S3 Express bucket, or specify 'cayenne_s3_zone_ids' to auto-generate the bucket name."
    ))]
    CannotAutoCreateBucket { reason: String },

    #[snafu(display(
        "Multi-zone S3 Express One Zone write failed: {failed_count} of {total_zones} zone(s) failed. \
        Failed zones: {failed_zones}. Successful writes have been rolled back for ACID consistency."
    ))]
    MultiZoneWriteFailed {
        failed_count: usize,
        total_zones: usize,
        failed_zones: String,
    },

    #[snafu(display(
        "Multi-zone S3 Express rollback failed for zone '{zone}': {reason}. \
        Manual cleanup may be required."
    ))]
    MultiZoneRollbackFailed { zone: String, reason: String },

    #[snafu(display("Invalid S3 Express One Zone bucket name: {reason}"))]
    InvalidBucketName { reason: String },
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

/// Returns true if the provided error or any of its sources indicates an authentication failure.
///
/// This walks the error chain and inspects both structured error sources (when available) and
/// a last-resort string check. This keeps the string matching centralized instead of duplicating
/// it at each call site.
fn is_auth_error(error: &(dyn std::error::Error + 'static)) -> bool {
    let mut current: Option<&(dyn std::error::Error + 'static)> = Some(error);
    while let Some(err) = current {
        // If the object store wrapped another error (e.g., AWS SDK), inspect the source chain
        if let Some(store_err) = err.downcast_ref::<object_store::Error>()
            && let object_store::Error::Generic { source, .. } = store_err
            && is_auth_error(source.as_ref())
        {
            return true;
        }

        let error_str = format!("{err:?}");
        if error_str.contains("UnauthorizedException")
            || error_str.contains("ExpiredToken")
            || error_str.contains("InvalidAccessKeyId")
            || error_str.contains("SignatureDoesNotMatch")
            || error_str.contains("AccessDenied")
            || error_str.contains("403")
        {
            return true;
        }

        current = err.source();
    }

    false
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
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(default)
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

    fn resolve_custom_data_path(dataset_name: &str, custom_path: &str) -> Result<String> {
        Self::validate_file_path(custom_path)?;
        let base = custom_path.trim_end_matches('/');
        Ok(format!("{base}/{dataset_name}/"))
    }

    fn resolve_auto_s3_data_path(
        app_name: &str,
        dataset_name: &str,
        zone_id: &str,
    ) -> Result<String> {
        let bucket_name = Self::generate_bucket_name(app_name, dataset_name, zone_id)?;
        Ok(format!("s3://{bucket_name}/{dataset_name}/"))
    }

    fn resolve_default_data_path(dataset_name: &str) -> String {
        format!("{}/{dataset_name}/", spice_data_base_path())
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
        if path.starts_with("s3://") && !Self::is_s3_express_path(path) {
            return Err(Error::StandardS3NotSupported {
                path: path.to_string(),
            });
        }
        Ok(())
    }

    /// Returns true if the data path for this source is an S3 Express One Zone path.
    ///
    /// This returns true if either:
    /// - `cayenne_file_path` is set to an S3 Express path, or
    /// - `cayenne_s3_zone_ids` is set (which means we'll auto-generate S3 Express paths)
    fn is_s3_express_data_path(source: &dyn AccelerationSource) -> bool {
        source.acceleration().is_some_and(|a| {
            // Check for explicit S3 Express path
            if a.params
                .get("cayenne_file_path")
                .is_some_and(|path| Self::is_s3_express_path(path))
            {
                return true;
            }
            // Check for auto-generated path via zone_ids
            a.params.contains_key("cayenne_s3_zone_ids")
        })
    }

    /// Returns true if multi-zone S3 Express One Zone storage is configured.
    ///
    /// Multi-zone is enabled when `cayenne_s3_zone_ids` contains multiple zone IDs.
    #[expect(
        dead_code,
        reason = "Will be used when multi-zone write support is implemented"
    )]
    fn is_multi_zone_s3_express(source: &dyn AccelerationSource) -> bool {
        source.acceleration().is_some_and(|a| {
            a.params
                .get("cayenne_s3_zone_ids")
                .is_some_and(|zone_ids| zone_ids.contains(','))
        })
    }

    /// Returns the list of zone IDs for S3 Express One Zone storage.
    ///
    /// Parses the `cayenne_s3_zone_ids` parameter as a comma-separated list of zone IDs.
    /// Returns an empty vector if the parameter is not set.
    fn get_s3_zone_ids(source: &dyn AccelerationSource) -> Vec<String> {
        source
            .acceleration()
            .and_then(|a| a.params.get("cayenne_s3_zone_ids"))
            .map(|zone_ids| {
                zone_ids
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect()
            })
            .unwrap_or_default()
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
        let zone_ids = Self::get_s3_zone_ids(source);
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
                let bucket_name = Self::generate_bucket_name(&app_name, &dataset_name, zone_id)?;
                Ok(format!("s3://{bucket_name}/{dataset_name}/"))
            })
            .collect();

        paths
    }

    /// Extracts the zone ID from an S3 Express One Zone bucket name.
    ///
    /// S3 Express One Zone bucket names have the format: `{base-name}--{zone-id}--x-s3`
    /// Example: `mybucket--usw2-az1--x-s3` returns `Some("usw2-az1")`
    fn extract_zone_id_from_bucket(bucket_name: &str) -> Option<&str> {
        // Find the last occurrence of "--x-s3"
        let suffix_start = bucket_name.rfind("--x-s3")?;
        let before_suffix = &bucket_name[..suffix_start];

        // Find the second-to-last "--" which separates the base name from zone id
        let zone_start = before_suffix.rfind("--")?;
        Some(&before_suffix[zone_start + 2..])
    }

    /// Generates an S3 Express One Zone bucket name from the app name and dataset name.
    ///
    /// Format: `spice-{app_name}-{dataset_name}--{zone_id}--x-s3`
    ///
    /// The names are sanitized to comply with S3 bucket naming rules:
    /// - Lowercase only
    /// - Only alphanumeric and hyphens allowed
    /// - Max 63 characters total (we leave room for the suffix)
    fn generate_bucket_name(app_name: &str, dataset_name: &str, zone_id: &str) -> Result<String> {
        // Sanitize names for S3 bucket naming requirements
        fn sanitize(s: &str) -> String {
            s.to_lowercase()
                .chars()
                .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
                .collect::<String>()
                // Collapse multiple consecutive hyphens into one
                .split('-')
                .filter(|s| !s.is_empty())
                .collect::<Vec<_>>()
                .join("-")
        }

        let sanitized_app = sanitize(app_name);
        let sanitized_dataset = sanitize(dataset_name);

        ensure!(
            !sanitized_app.is_empty() && !sanitized_dataset.is_empty(),
            InvalidBucketNameSnafu {
                reason: "App or dataset name is empty after sanitization".to_string(),
            }
        );

        // S3 bucket names can be max 63 chars
        // We need room for: "spice-" (6) + "--" (2) + zone_id + "--x-s3" (6) = 14 + zone_id.len()
        let suffix_len = 2_usize.saturating_add(zone_id.len()).saturating_add(6); // "--{zone_id}--x-s3"
        let prefix_len = 6_usize; // "spice-"

        // Check if zone_id is too long (max 49 chars to leave at least 1 char for name)
        // 63 (max) - 6 (prefix) - 1 (hyphen) - 6 (--x-s3) - 2 (--) = 48 max for zone_id + 1 for name
        let required_fixed_len = prefix_len.saturating_add(1).saturating_add(suffix_len);
        ensure!(
            required_fixed_len < 63,
            InvalidBucketNameSnafu {
                reason: format!(
                    "Zone ID '{zone_id}' is too long ({} chars). Maximum zone ID length is {} characters to fit S3 bucket naming constraints.",
                    zone_id.len(),
                    63_usize
                        .saturating_sub(prefix_len)
                        .saturating_sub(1)
                        .saturating_sub(8) // 8 = 2 + 6 (-- + --x-s3)
                ),
            }
        );

        let max_name_len = 63_usize.saturating_sub(required_fixed_len);

        ensure!(
            max_name_len > 0,
            InvalidBucketNameSnafu {
                reason: format!(
                    "Zone ID '{zone_id}' leaves no room for bucket naming (max length <= 0)"
                ),
            }
        );

        let base_name = format!("{sanitized_app}-{sanitized_dataset}");
        let truncated_name = if base_name.len() > max_name_len {
            base_name[..max_name_len].trim_end_matches('-').to_string()
        } else {
            base_name
        };

        ensure!(
            !truncated_name.is_empty(),
            InvalidBucketNameSnafu {
                reason: "Bucket name is empty after truncation".to_string(),
            }
        );

        Ok(format!("spice-{truncated_name}--{zone_id}--x-s3"))
    }

    /// Creates an S3 Express One Zone directory bucket if it doesn't exist.
    ///
    /// Uses `initiate_config_with_credentials` from `aws_sdk_credential_bridge` for credential handling,
    /// supporting both explicit credentials and IAM role-based authentication.
    ///
    /// Returns `Ok(true)` if a new bucket was created, `Ok(false)` if the bucket already existed.
    /// Returns `Err` if the bucket creation or verification fails.
    #[expect(
        clippy::too_many_lines,
        reason = "S3 Express bucket creation requires extensive setup, creation, and verification steps"
    )]
    async fn create_s3_express_bucket_if_needed(
        bucket_name: &str,
        zone_id: &str,
        region: &str,
        access_key_id: Option<String>,
        secret_access_key: Option<String>,
        session_token: Option<String>,
    ) -> Result<bool> {
        use aws_sdk_s3::types::{
            BucketInfo, BucketType, CreateBucketConfiguration, DataRedundancy, LocationInfo,
            LocationType,
        };

        // Use the credential bridge to build config with proper credential handling
        let config_loader = aws_sdk_credential_bridge::initiate_config_with_credentials(
            "cayenne-s3-express",
            region.to_string(),
            access_key_id.clone(),
            secret_access_key.clone(),
            session_token.clone(),
        )
        .await;

        let sdk_config = config_loader.load().await;
        let s3_client = aws_sdk_s3::Client::new(&sdk_config);

        // Check if bucket already exists by trying to head it
        let bucket_exists = match s3_client.head_bucket().bucket(bucket_name).send().await {
            Ok(_) => true,
            Err(e) => {
                // Check if it's a "not found" error vs. a permissions error
                let service_error = e.into_service_error();
                if !service_error.is_not_found() {
                    // Could be access denied or other error - log but continue to try creation
                    tracing::debug!(
                        "Head bucket check returned error (will attempt creation): {:?}",
                        service_error
                    );
                }
                false
            }
        };

        // If bucket exists, skip creation but still validate write/read access
        let bucket_created = if bucket_exists {
            tracing::debug!(
                "S3 Express bucket '{}' already exists, validating access...",
                bucket_name
            );
            false
        } else {
            tracing::info!(
                "Creating S3 Express One Zone bucket '{}' in zone '{}' (region: {})",
                bucket_name,
                zone_id,
                region
            );

            // Create the bucket configuration for S3 Express One Zone (directory bucket)
            // Note: LocationConstraint is NOT supported for directory buckets - only Location and Bucket are used
            let bucket_config = CreateBucketConfiguration::builder()
                .location(
                    LocationInfo::builder()
                        .r#type(LocationType::AvailabilityZone)
                        .name(zone_id)
                        .build(),
                )
                .bucket(
                    BucketInfo::builder()
                        .r#type(BucketType::Directory)
                        .data_redundancy(DataRedundancy::SingleAvailabilityZone)
                        .build(),
                )
                .build();

            // Attempt to create the bucket
            match s3_client
                .create_bucket()
                .bucket(bucket_name)
                .create_bucket_configuration(bucket_config)
                .send()
                .await
            {
                Ok(_) => true,
                Err(e) => {
                    let service_error = e.into_service_error();
                    // Check if bucket already exists (race condition or created by another process)
                    if service_error.is_bucket_already_exists()
                        || service_error.is_bucket_already_owned_by_you()
                    {
                        tracing::debug!(
                            "S3 Express bucket '{}' already exists (concurrent creation)",
                            bucket_name
                        );
                        false
                    } else {
                        // Check for authentication errors using structured error codes first
                        let code = service_error.meta().code();
                        if matches!(
                            code,
                            Some(
                                "UnauthorizedException"
                                    | "ExpiredToken"
                                    | "InvalidAccessKeyId"
                                    | "SignatureDoesNotMatch"
                                    | "AccessDenied"
                            )
                        ) || is_auth_error(&service_error)
                        {
                            return Err(Error::S3AuthenticationFailed);
                        }
                        return Err(Error::S3DirectoryBucketCreation {
                            bucket: bucket_name.to_string(),
                            source: Box::new(service_error),
                        });
                    }
                }
            }
        };

        // Verify bucket access with a write/read test using object_store
        // This ensures the same client configuration used for actual data uploads is validated
        tracing::info!(
            "Validating S3 Express One Zone bucket access for '{}' using object_store...",
            bucket_name
        );

        let object_store = Self::build_s3_object_store_for_validation(
            bucket_name,
            zone_id,
            region,
            access_key_id.clone(),
            secret_access_key.clone(),
            session_token.clone(),
        )
        .await?;

        let test_path = object_store::path::Path::from(".cayenne_write_test");
        let test_content = bytes::Bytes::from_static(b"cayenne_s3_express_verification");

        // Write test object using object_store
        object_store
            .put(&test_path, test_content.clone().into())
            .await
            .map_err(|e| {
                if is_auth_error(&e) {
                    Error::S3AuthenticationFailed
                } else {
                    Error::S3DirectoryBucketCreation {
                        bucket: bucket_name.to_string(),
                        source: Box::new(e),
                    }
                }
            })?;

        // Read test object back using object_store
        let get_result = object_store.get(&test_path).await.map_err(|e| {
            if is_auth_error(&e) {
                Error::S3AuthenticationFailed
            } else {
                Error::S3DirectoryBucketCreation {
                    bucket: bucket_name.to_string(),
                    source: Box::new(e),
                }
            }
        })?;

        let body = get_result
            .bytes()
            .await
            .map_err(|e| Error::S3DirectoryBucketCreation {
                bucket: bucket_name.to_string(),
                source: Box::new(e),
            })?;

        if body.as_ref() != test_content.as_ref() {
            return Err(Error::S3DirectoryBucketCreation {
                bucket: bucket_name.to_string(),
                source: "S3 write/read verification failed: content mismatch".into(),
            });
        }

        // Clean up test object
        let _ = object_store.delete(&test_path).await;

        if bucket_created {
            tracing::info!(
                "Created and verified S3 Express One Zone bucket: {}",
                bucket_name
            );
        } else {
            tracing::info!(
                "Validated S3 Express One Zone bucket access: {}",
                bucket_name
            );
        }

        Ok(bucket_created)
    }

    /// Builds an S3 object store for validation using the same configuration as data uploads.
    ///
    /// This ensures validation uses the exact same client configuration (credentials, endpoint, S3 Express mode)
    /// that will be used for actual data uploads, preventing configuration mismatches.
    async fn build_s3_object_store_for_validation(
        bucket_name: &str,
        zone_id: &str,
        region: &str,
        access_key_id: Option<String>,
        secret_access_key: Option<String>,
        session_token: Option<String>,
    ) -> Result<Arc<dyn object_store::ObjectStore>> {
        let io_runtime = tokio::runtime::Handle::current();
        let mut s3_builder = AmazonS3Builder::from_env()
            .with_bucket_name(bucket_name)
            .with_http_connector(SpawnedReqwestConnector::new(io_runtime))
            .with_region(region);

        // Configure S3 Express One Zone mode
        tracing::debug!(
            "Building validation object store for S3 Express bucket (zone: {})",
            zone_id
        );
        s3_builder = s3_builder
            .with_s3_express(true)
            .with_virtual_hosted_style_request(true);

        // Build the S3 Express endpoint with virtual-hosted-style format
        let express_endpoint =
            format!("https://{bucket_name}.s3express-{zone_id}.{region}.amazonaws.com");
        tracing::debug!("Validation using S3 Express endpoint: {express_endpoint}");
        s3_builder = s3_builder.with_endpoint(&express_endpoint);

        // Set default timeout consistent with data upload configuration
        let client_options =
            ClientOptions::default().with_timeout(std::time::Duration::from_secs(300)); // 5 minutes per request
        s3_builder = s3_builder.with_client_options(client_options);

        // Handle credentials
        let mut load_credentials_from_environment = true;
        if let (Some(key), Some(secret)) = (access_key_id, secret_access_key) {
            s3_builder = s3_builder.with_access_key_id(key);
            s3_builder = s3_builder.with_secret_access_key(secret);
            if let Some(token) = session_token {
                s3_builder = s3_builder.with_token(token);
            }
            load_credentials_from_environment = false;
        }

        // Load credentials from environment using our credential bridge
        if load_credentials_from_environment {
            tracing::debug!("Loading S3 credentials from environment for validation");
            match aws_sdk_credential_bridge::get_or_init_sdk_config().await {
                Ok(Some(sdk_config)) => {
                    if sdk_config.credentials_provider().is_some() {
                        tracing::debug!(
                            "Using S3 credentials provider from SDK config for validation"
                        );
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
                        "No AWS SDK credentials available for S3 Express validation; assuming public access"
                    );
                }
                Err(err) => {
                    tracing::warn!("Unable to initialize AWS credentials for validation: {err}");
                }
            }
        }

        let store = s3_builder
            .build()
            .map_err(|e| Error::S3ObjectStoreCreation {
                source: Box::new(e),
            })?;

        Ok(Arc::new(store))
    }

    /// Extracts S3 bucket information (bucket name, zone ID, region, credentials) from the source configuration.
    ///
    /// If `cayenne_file_path` is provided as an S3 Express path, extracts info from that.
    /// Otherwise, generates a bucket name from the app name and dataset name using `cayenne_s3_zone_ids`.
    ///
    /// # Returns
    ///
    /// A tuple of (bucket name, zone ID, region, access key, secret key, session token)
    #[expect(
        clippy::type_complexity,
        reason = "Return type represents distinct S3 configuration fields"
    )]
    fn get_s3_bucket_info(
        source: &dyn AccelerationSource,
        data_path: &str,
    ) -> Result<(
        String,
        String,
        String,
        Option<String>,
        Option<String>,
        Option<String>,
    )> {
        let acceleration = source
            .acceleration()
            .ok_or_else(|| Error::InvalidConfiguration {
                detail: Arc::from("Acceleration settings required for S3 bucket info"),
            })?;

        // Try to extract zone ID from the bucket name in the path
        let url = Url::parse(data_path).map_err(|e| Error::InvalidS3Url {
            url: data_path.to_string(),
            source: Box::new(e),
        })?;

        let bucket_name = get_bucket_name(&url)
            .map_err(|e| Error::InvalidS3Url {
                url: data_path.to_string(),
                source: Box::new(e),
            })?
            .to_string();

        // Extract zone ID from bucket name (e.g., "mybucket--usw2-az1--x-s3" -> "usw2-az1")
        let zone_id = Self::extract_zone_id_from_bucket(&bucket_name)
            .or_else(|| {
                acceleration
                    .params
                    .get("cayenne_s3_zone_ids")
                    .and_then(|ids| ids.split(',').next())
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
            })
            .ok_or_else(|| Error::CannotAutoCreateBucket {
                reason: "Could not determine zone ID. Either use a valid S3 Express bucket name format (bucket--zone-id--x-s3) or specify 'cayenne_s3_zone_ids' parameter".to_string(),
            })?
            .to_string();

        // Get region from params or derive from zone ID
        let region = acceleration
            .params
            .get("cayenne_s3_region")
            .cloned()
            .or_else(|| Self::derive_region_from_zone(&zone_id))
            .ok_or_else(|| Error::CannotAutoCreateBucket {
                reason: format!(
                    "Could not determine region. Specify 'cayenne_s3_region' parameter. Zone ID: {zone_id}"
                ),
            })?;

        // Get optional credentials from params
        let s3_auth = acceleration
            .params
            .get("cayenne_s3_auth")
            .map_or("iam_role", String::as_str);

        let (access_key, secret_key, session_token) = if s3_auth == "key" {
            (
                acceleration.params.get("cayenne_s3_key").cloned(),
                acceleration.params.get("cayenne_s3_secret").cloned(),
                acceleration.params.get("cayenne_s3_session_token").cloned(),
            )
        } else {
            (None, None, None)
        };

        Ok((
            bucket_name,
            zone_id,
            region,
            access_key,
            secret_key,
            session_token,
        ))
    }

    /// Derives the AWS region from a zone ID.
    ///
    /// Zone IDs follow the pattern: `{region-code}-az{n}` (e.g., `usw2-az1`, `use1-az4`)
    /// We need to map the abbreviated region code to the full AWS region name.
    fn derive_region_from_zone(zone_id: &str) -> Option<String> {
        // Extract the region prefix from zone ID (e.g., "usw2" from "usw2-az1")
        let region_prefix = zone_id.split("-az").next()?;

        // Map abbreviated region codes to full AWS region names
        let region = match region_prefix {
            // US regions
            "use1" => "us-east-1",
            "use2" => "us-east-2",
            "usw1" => "us-west-1",
            "usw2" => "us-west-2",
            // EU regions
            "euw1" => "eu-west-1",
            "euw2" => "eu-west-2",
            "euw3" => "eu-west-3",
            "euc1" => "eu-central-1",
            "euc2" => "eu-central-2",
            "eun1" => "eu-north-1",
            "eus1" => "eu-south-1",
            "eus2" => "eu-south-2",
            // AP regions
            "apne1" => "ap-northeast-1",
            "apne2" => "ap-northeast-2",
            "apne3" => "ap-northeast-3",
            "apse1" => "ap-southeast-1",
            "apse2" => "ap-southeast-2",
            "apse3" => "ap-southeast-3",
            "apse4" => "ap-southeast-4",
            "apse5" => "ap-southeast-5",
            "aps1" => "ap-south-1",
            "aps2" => "ap-south-2",
            "ape1" => "ap-east-1",
            // Other regions
            "sae1" => "sa-east-1",
            "cac1" => "ca-central-1",
            "caw1" => "ca-west-1",
            "afs1" => "af-south-1",
            "mes1" => "me-south-1",
            "mec1" => "me-central-1",
            "ilc1" => "il-central-1",
            _ => return None,
        };

        Some(region.to_string())
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
        // Check if this is S3 Express One Zone storage
        if !Self::is_s3_express_data_path(source) {
            return Ok(None);
        }

        // Get the computed data path (handles both explicit cayenne_file_path and auto-generated from zone_id)
        let accelerator = CayenneAccelerator::new();
        let data_path = accelerator.cayenne_data_dir(source)?;

        if !Self::is_s3_express_path(&data_path) {
            return Ok(None);
        }

        tracing::debug!(
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

        // Get params with secrets resolved
        let raw_params = source
            .acceleration()
            .map(|a| a.params.clone())
            .unwrap_or_default();
        let secrets = source.runtime().secrets();
        let params = get_params_with_secrets(secrets, &raw_params).await;

        // Helper to get param value with secret exposed
        let get_param = |key: &str| -> Option<String> {
            params.get(key).map(|v| v.expose_secret().to_string())
        };

        let s3_region = get_param("cayenne_s3_region");
        let s3_endpoint = get_param("cayenne_s3_endpoint");
        let s3_key = get_param("cayenne_s3_key");
        let s3_secret = get_param("cayenne_s3_secret");
        let s3_session_token = get_param("cayenne_s3_session_token");
        let s3_auth = get_param("cayenne_s3_auth").unwrap_or_else(|| "iam_role".to_string());
        let s3_client_timeout = get_param("cayenne_s3_client_timeout");
        let s3_allow_http =
            get_param("cayenne_s3_allow_http").is_some_and(|v| v.eq_ignore_ascii_case("true"));

        // Extract zone ID from bucket name for S3 Express One Zone endpoint
        let zone_id = Self::extract_zone_id_from_bucket(bucket_name);

        // Derive region from zone_id if not explicitly provided
        let derived_region = zone_id.and_then(Self::derive_region_from_zone);

        // Use explicit region if provided, otherwise use derived region. If neither is available, fail fast.
        let effective_region = s3_region
            .or(derived_region)
            .ok_or_else(|| Error::InvalidConfiguration {
                detail: Arc::from(
                    "Cannot determine AWS region for S3 Express One Zone. Specify 'cayenne_s3_region' or use an S3 Express bucket name that encodes the zone (bucket--<zone>--x-s3).",
                ),
            })?;

        // Build the S3 object store
        let io_runtime = tokio::runtime::Handle::current();
        let mut s3_builder = AmazonS3Builder::from_env()
            .with_bucket_name(bucket_name)
            .with_http_connector(SpawnedReqwestConnector::new(io_runtime))
            .with_allow_http(s3_allow_http)
            .with_region(effective_region.clone());

        // Configure longer retry timeout for S3 Express uploads from outside AWS
        // S3 Express One Zone is optimized for same-AZ access; from outside AWS,
        // uploads can be slow and need more time to complete.
        let retry_config = RetryConfig {
            max_retries: 3,
            retry_timeout: std::time::Duration::from_secs(600), // 10 minutes
            ..Default::default()
        };
        s3_builder = s3_builder.with_retry(retry_config);

        let mut client_options = ClientOptions::default();

        // Set a generous default timeout for S3 Express One Zone uploads from outside AWS.
        // The default object_store timeout (~90s) is too short for large file uploads over the internet.
        // S3 Express One Zone is optimized for same-AZ access; external uploads need more time.
        let default_timeout = std::time::Duration::from_secs(300); // 5 minutes per request
        client_options = client_options.with_timeout(default_timeout);

        // For S3 Express One Zone buckets, enable special handling:
        // - with_s3_express(true) enables CreateSession API for session tokens
        // - with_virtual_hosted_style_request(true) uses {bucket}.endpoint format
        // - Endpoint format with virtual-hosted-style: https://{bucket}.s3express-{zone-id}.{region}.amazonaws.com
        if let Some(zid) = zone_id {
            tracing::debug!(
                "Detected S3 Express One Zone bucket (zone: {}), enabling S3 Express mode",
                zid
            );
            s3_builder = s3_builder
                .with_s3_express(true)
                .with_virtual_hosted_style_request(true);

            // For S3 Express with virtual-hosted-style, the endpoint should include the bucket name
            // Format: https://{bucket}.s3express-{zone-id}.{region}.amazonaws.com
            if s3_endpoint.is_none() {
                let express_endpoint = format!(
                    "https://{bucket_name}.s3express-{zid}.{effective_region}.amazonaws.com"
                );
                tracing::debug!("Using S3 Express One Zone endpoint: {express_endpoint}");
                s3_builder = s3_builder.with_endpoint(&express_endpoint);
            }
        }

        // Apply explicit endpoint if provided (overrides auto-generated)
        if let Some(ref endpoint) = s3_endpoint {
            tracing::debug!("Using explicit S3 endpoint: {}", endpoint);
            s3_builder = s3_builder.with_endpoint(endpoint);
            if endpoint.starts_with("http://") {
                client_options = client_options.with_allow_http(true);
            }
        }

        if let Some(ref timeout) = s3_client_timeout {
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
                        "S3 auth method 'key' requires both 'cayenne_s3_key' and 'cayenne_s3_secret' parameters",
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

        tracing::info!(
            "S3 Express One Zone object store configured for data path: {}",
            data_path
        );

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

            tracing::debug!(
                "Cayenne Vortex config: footer_cache={}MB, segment_cache={}MB, target_file_size={}MB, sort_columns={:?}, compression_strategy={:?}",
                config.footer_cache_mb,
                config.segment_cache_mb,
                config.target_vortex_file_size_mb,
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
        let full_schema: arrow::datatypes::Schema = cmd.schema.as_ref().clone().into();
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
        use cayenne::{CayenneTableProviderBuilder, metadata::CreateTableOptions};

        tracing::debug!("create_cayenne_table_provider: starting for table {table_name}");

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

        // Check if using S3 Express One Zone storage
        let is_s3_express = Self::is_s3_express_data_path(source);
        let vortex_config = Self::get_vortex_config(table_name, source);

        // Build S3 object store if using S3 Express One Zone storage
        let object_store = Self::build_s3_object_store(source).await?;

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
                "Attaching S3 Express One Zone object store to CayenneTableProvider for {}: {}",
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
        let cayenne_table =
            builder
                .create(table_options)
                .await
                .map_err(|e| Error::AccelerationCreationFailed {
                    source: Box::new(e),
                })?;

        tracing::debug!("create_cayenne_table_provider: table {table_name} created successfully");
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
        .description("Authentication method for S3 Express One Zone. Options: 'iam_role' (default, uses environment credentials), 'key' (uses explicit cayenne_s3_key/cayenne_s3_secret).")
        .default("iam_role")
        .one_of(&["iam_role", "key"]),
    ParameterSpec::component("cayenne_s3_client_timeout")
        .description("Timeout for S3 client operations (e.g., '30s', '5m')."),
    ParameterSpec::component("cayenne_s3_allow_http")
        .description("Allow HTTP (non-TLS) connections to S3. Default: false.")
        .default("false"),
    // S3 Express One Zone auto-generation parameter
    ParameterSpec::component("cayenne_s3_zone_ids")
        .description("Comma-separated list of Availability Zone IDs for S3 Express One Zone storage (e.g., 'usw2-az1' or 'usw2-az1,usw2-az2'). When specified without 'cayenne_file_path', auto-generates bucket name from app and dataset name, and creates the bucket if needed. For multi-zone redundancy, specify multiple zones. Data is written to all zones with ACID guarantees - writes succeed only if all zones succeed. Reads are served from the primary (first) zone with fallback to replicas."),
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
        .default("btrblocks"),
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
    #[expect(
        clippy::too_many_lines,
        reason = "Initialization requires extensive validation, S3 bucket setup, and directory management"
    )]
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
            // Validate S3 Express One Zone configuration - only one method allowed
            let has_s3_zone_ids = acceleration.params.contains_key("cayenne_s3_zone_ids");
            let has_s3_express_file_path = acceleration
                .params
                .get("cayenne_file_path")
                .is_some_and(|path| Self::is_s3_express_path(path));

            if has_s3_zone_ids && has_s3_express_file_path {
                return Err(Box::new(Error::InvalidConfiguration {
                    detail: Arc::from(
                        "Cannot specify both 'cayenne_s3_zone_ids' and 'cayenne_file_path' with an S3 Express path. Use either 'cayenne_s3_zone_ids' for auto-generated bucket names, or 'cayenne_file_path' for explicit bucket paths.",
                    ),
                }));
            }

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

        // Handle S3 Express One Zone configuration
        if is_s3_express {
            // Automatically create the bucket if it doesn't exist and we have the required info
            match Self::get_s3_bucket_info(source, &dir_path) {
                Ok((bucket_name, zone_id, region, access_key, secret_key, session_token)) => {
                    match Self::create_s3_express_bucket_if_needed(
                        &bucket_name,
                        &zone_id,
                        &region,
                        access_key,
                        secret_key,
                        session_token,
                    )
                    .await
                    {
                        Ok(created) => {
                            if created {
                                tracing::info!(
                                    "Using S3 Express One Zone storage: {} (bucket created)",
                                    dir_path
                                );
                            } else {
                                tracing::info!(
                                    "Using S3 Express One Zone storage: {} (bucket exists)",
                                    dir_path
                                );
                            }
                            tracing::debug!(
                                "S3 Express One Zone is optimized for low-latency access within the same AWS Availability Zone. Access from outside AWS may experience higher latency."
                            );
                        }
                        Err(e) => {
                            // Bucket creation/verification failed - this is a hard error
                            return Err(Box::new(e));
                        }
                    }
                }
                Err(e) => {
                    // Could not determine bucket info - this is a configuration error
                    return Err(Box::new(e));
                }
            }

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
            let partition_by_last = partition_by.last().cloned().ok_or_else(|| {
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
            let is_s3_express = Self::is_s3_express_data_path(source);
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
                partition_by_last,
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
    primary_key: Vec<String>,
    on_conflict: Option<datafusion_table_providers::util::on_conflict::OnConflict>,
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
        primary_key: Vec<String>,
        on_conflict: Option<datafusion_table_providers::util::on_conflict::OnConflict>,
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
            primary_key,
            on_conflict,
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
            primary_key: self.primary_key.clone(),
            on_conflict: self.on_conflict.clone(),
            base_path: partition_path.clone(),
            partition_column: None, // Partitions themselves are not partitioned
            vortex_config: self.vortex_config.clone(),
        };

        // Create Cayenne table provider for this partition with S3 support
        let mut builder = cayenne::CayenneTableProviderBuilder::new(Arc::clone(&self.catalog))
            .with_retention_filters(self.retention_filters.clone());
        if let Some(ref object_store) = self.object_store_config {
            builder = builder.with_object_store(object_store.clone());
        }
        let cayenne_table =
            builder
                .create(table_options)
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

            // Use builder pattern to pass object store config for S3 support
            let mut builder = cayenne::CayenneTableProviderBuilder::new(Arc::clone(&self.catalog))
                .with_retention_filters(self.retention_filters.clone());
            if let Some(ref object_store) = self.object_store_config {
                builder = builder.with_object_store(object_store.clone());
            }
            let cayenne_table = builder.open(&partition_table_name).await.map_err(|e| {
                creator::Error::InferringPartitions {
                    source: Box::new(e),
                }
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

    #[test]
    fn test_extract_zone_id_from_bucket() {
        // Valid S3 Express bucket names
        assert_eq!(
            CayenneAccelerator::extract_zone_id_from_bucket("mybucket--usw2-az1--x-s3"),
            Some("usw2-az1")
        );
        assert_eq!(
            CayenneAccelerator::extract_zone_id_from_bucket("data-bucket--use1-az4--x-s3"),
            Some("use1-az4")
        );
        assert_eq!(
            CayenneAccelerator::extract_zone_id_from_bucket("spice-myapp-dataset--euw1-az2--x-s3"),
            Some("euw1-az2")
        );

        // Invalid bucket names
        assert_eq!(
            CayenneAccelerator::extract_zone_id_from_bucket("mybucket"),
            None
        );
        assert_eq!(
            CayenneAccelerator::extract_zone_id_from_bucket("mybucket--partial"),
            None
        );
        assert_eq!(
            CayenneAccelerator::extract_zone_id_from_bucket("mybucket--x-s3"),
            None
        );
    }

    #[test]
    fn test_generate_bucket_name() {
        // Basic bucket name generation
        assert_eq!(
            CayenneAccelerator::generate_bucket_name("myapp", "orders", "usw2-az1")
                .expect("bucket name"),
            "spice-myapp-orders--usw2-az1--x-s3"
        );

        // Special characters are sanitized
        assert_eq!(
            CayenneAccelerator::generate_bucket_name("My.App", "order_items", "use1-az4")
                .expect("bucket name"),
            "spice-my-app-order-items--use1-az4--x-s3"
        );

        // Uppercase is converted to lowercase
        assert_eq!(
            CayenneAccelerator::generate_bucket_name("MyApp", "MyDataset", "euw1-az2")
                .expect("bucket name"),
            "spice-myapp-mydataset--euw1-az2--x-s3"
        );

        // Names with multiple special chars
        assert_eq!(
            CayenneAccelerator::generate_bucket_name("my--app", "data..set", "aps1-az1")
                .expect("bucket name"),
            "spice-my-app-data-set--aps1-az1--x-s3"
        );
    }

    #[test]
    fn test_generate_bucket_name_truncation() {
        // Very long names should be truncated to fit within S3 bucket name limits
        let long_app = "a".repeat(50);
        let long_dataset = "b".repeat(50);
        let bucket = CayenneAccelerator::generate_bucket_name(&long_app, &long_dataset, "usw2-az1")
            .expect("bucket name");

        // S3 bucket names can be max 63 chars
        assert!(
            bucket.len() <= 63,
            "Bucket name should be <= 63 chars, got {} chars: {}",
            bucket.len(),
            bucket
        );
        assert!(bucket.ends_with("--usw2-az1--x-s3"));
        assert!(bucket.starts_with("spice-"));
    }

    #[test]
    fn test_derive_region_from_zone() {
        // US regions
        assert_eq!(
            CayenneAccelerator::derive_region_from_zone("use1-az1"),
            Some("us-east-1".to_string())
        );
        assert_eq!(
            CayenneAccelerator::derive_region_from_zone("use2-az2"),
            Some("us-east-2".to_string())
        );
        assert_eq!(
            CayenneAccelerator::derive_region_from_zone("usw1-az1"),
            Some("us-west-1".to_string())
        );
        assert_eq!(
            CayenneAccelerator::derive_region_from_zone("usw2-az1"),
            Some("us-west-2".to_string())
        );

        // EU regions
        assert_eq!(
            CayenneAccelerator::derive_region_from_zone("euw1-az1"),
            Some("eu-west-1".to_string())
        );
        assert_eq!(
            CayenneAccelerator::derive_region_from_zone("euc1-az2"),
            Some("eu-central-1".to_string())
        );

        // AP regions
        assert_eq!(
            CayenneAccelerator::derive_region_from_zone("apne1-az1"),
            Some("ap-northeast-1".to_string())
        );
        assert_eq!(
            CayenneAccelerator::derive_region_from_zone("apse1-az2"),
            Some("ap-southeast-1".to_string())
        );

        // Unknown zone format
        assert_eq!(
            CayenneAccelerator::derive_region_from_zone("unknown-az1"),
            None
        );
        assert_eq!(CayenneAccelerator::derive_region_from_zone("invalid"), None);
    }
}
