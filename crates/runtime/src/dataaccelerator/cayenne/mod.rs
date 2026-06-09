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

mod autotune;
pub mod partitioned_insert_strategy;
pub mod s3;
pub mod snapshot_engine;

use std::any::Any;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use regex::Regex;

use arrow_schema::{DataType, Schema};
use async_trait::async_trait;
use data_components::poly::PolyTableProvider;
use datafusion::common::DFSchema;
use datafusion::common::arrow::datatypes::SchemaRef;
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::execution::runtime_env::RuntimeEnv;
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

use super::{
    AccelerationSource, BootstrapStatus, DataAccelerator, get_primary_keys_from_constraints,
    upsert_dedup,
};
use crate::component::dataset::acceleration::{Acceleration, Engine, Mode, RefreshMode};
use crate::dataaccelerator::cayenne::s3::{S3_PARAMETERS, S3_PARAMS_LEN};
use crate::dataaccelerator::{FilePathError, snapshots::download_snapshot_if_needed};
use crate::parameters::ParameterSpec;
use crate::register_data_accelerator;
use crate::spice_data_base_path;
use runtime_acceleration::snapshot::{AccelerationEngine, AccelerationLayout};
use runtime_datafusion_index::{Index, IndexedTableProvider};
use search::index::native_vector::NativeVectorIndex;
use spicepod::acceleration as spicepod_acceleration;

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

    #[snafu(display("Cayenne S3 acceleration error: {source}"))]
    S3Error { source: s3::Error },

    #[snafu(display("RuntimeEnv is required for Cayenne accelerator but was not provided"))]
    RuntimeEnvRequired,
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// Regex pattern for partition values that are not supported on local filesystem.
/// Partition values matching `.*#\d+` (e.g., "abcdef#123") are only supported on S3 Express
/// One Zone locations, not on local filesystem paths.
static UNSUPPORTED_LOCAL_PARTITION_PATTERN: LazyLock<Regex> =
    LazyLock::new(|| match Regex::new(r".*#\d+$") {
        Ok(compiled) => compiled,
        Err(e) => unreachable!("Unable to compile regexp: {e}"),
    });

fn maintained_aggregate_specs_for_cayenne(
    acceleration: Option<&Acceleration>,
) -> Result<Vec<cayenne::maintained_aggregate::MaintainedAggregateSpec>> {
    let Some(acceleration) = acceleration else {
        return Ok(Vec::new());
    };

    if acceleration.maintained_aggregates.is_empty() {
        return Ok(Vec::new());
    }

    if !acceleration.partition_by.is_empty() {
        return Err(Error::InvalidConfiguration {
            detail: Arc::from(
                "Cayenne maintained_aggregates is not yet supported on partitioned tables. Remove maintained_aggregates or remove partition_by from the acceleration configuration.",
            ),
        });
    }

    Ok(acceleration
        .maintained_aggregates
        .iter()
        .map(
            |aggregate| cayenne::maintained_aggregate::MaintainedAggregateSpec {
                group_by: aggregate.group_by.clone(),
                aggregates: aggregate
                    .aggregates
                    .iter()
                    .map(
                        |expr| {
                            cayenne::maintained_aggregate::MaintainedAggregateExpr {
                    function: match expr.function {
                        spicepod_acceleration::MaintainedAggregateFunction::Count => {
                            cayenne::maintained_aggregate::MaintainedAggregateFunction::Count
                        }
                        spicepod_acceleration::MaintainedAggregateFunction::Sum => {
                            cayenne::maintained_aggregate::MaintainedAggregateFunction::Sum
                        }
                        spicepod_acceleration::MaintainedAggregateFunction::Avg => {
                            cayenne::maintained_aggregate::MaintainedAggregateFunction::Avg
                        }
                    },
                    column: expr.column.clone(),
                }
                        },
                    )
                    .collect(),
            },
        )
        .collect())
}

/// Transform schema according to `unsupported_type_action` policy.
/// Delegates to `cayenne::transform_schema_for_vortex`.
pub(crate) fn transform_schema_for_vortex(
    schema: &arrow::datatypes::Schema,
    unsupported_type_action: UnsupportedTypeAction,
) -> Result<arrow::datatypes::Schema> {
    match cayenne::transform_schema_for_vortex(schema, unsupported_type_action) {
        Ok(schema) => Ok(schema),
        Err(datafusion::error::DataFusionError::Execution(msg))
            if msg.starts_with("Unsupported data type(s) in schema:") =>
        {
            // Extract just the field list from the structured error message.
            let details = msg
                .strip_prefix("Unsupported data type(s) in schema: ")
                .and_then(|s| s.split(". By default").next())
                .unwrap_or(&msg)
                .to_string();
            Err(Error::UnsupportedDataTypes { details })
        }
        Err(source) => Err(Error::UnableToCreateTable { source }),
    }
}

pub struct CayenneAccelerator {
    catalog: Arc<OnceCell<Arc<dyn cayenne::MetadataCatalog>>>,
    footer_cache_mb: Option<usize>,
    /// Shared semaphore that bounds the number of concurrent per-table
    /// background compactions across all Cayenne tables registered with this
    /// accelerator. Sized at `available_parallelism()` so a fleet of tables
    /// can't oversubscribe the writer pool.
    compaction_semaphore: Arc<tokio::sync::Semaphore>,
}

impl Default for CayenneAccelerator {
    fn default() -> Self {
        Self::new()
    }
}

fn parse_u64_aliases_with_hint(
    acceleration: &Acceleration,
    keys: &[&str],
    default: u64,
    semantic_hint: &str,
) -> u64 {
    keys.iter()
        .find_map(|&key| {
            acceleration.params.get(key).and_then(|v| {
                v.parse::<u64>().map_or_else(
                    |_| {
                        tracing::warn!(
                            "An invalid '{key}' value was provided: '{v}'. Expected an unsigned integer{semantic_hint}, ignoring the value. For details, visit: https://spiceai.org/docs/components/data-accelerators/cayenne#configuration"
                        );
                        None
                    },
                    Some,
                )
            })
        })
        .unwrap_or(default)
}

fn parse_optional_usize<'a>(
    acceleration: &Acceleration,
    keys: &'a [&'a str],
) -> Option<(&'a str, usize)> {
    keys.iter().find_map(|&key| {
        acceleration.params.get(key).and_then(|v| {
            v.parse::<usize>().map_or_else(|_| {
                tracing::warn!(
                    "An invalid '{key}' value was provided: '{v}'. Expected a positive integer, ignoring the value. For details, visit: https://spiceai.org/docs/components/data-accelerators/cayenne#configuration"
                );
                None
            }, |value| Some((key, value)))
            })
    })
}

fn parse_usize_aliases(acceleration: &Acceleration, keys: &[&str], default: usize) -> usize {
    parse_optional_usize(acceleration, keys).map_or(default, |(_, value)| value)
}

fn parse_usize_aliases_as_i64(acceleration: &Acceleration, keys: &[&str], default: i64) -> i64 {
    let default_usize = usize::try_from(default).unwrap_or(usize::MAX);
    let parsed = parse_usize_aliases(acceleration, keys, default_usize);
    i64::try_from(parsed).unwrap_or(i64::MAX)
}

const SMALL_WRITE_COMPACTION_TRIGGER_FILES: usize = 4;
const SMALL_WRITE_COMPACTION_TRIGGER_PROTECTED_SNAPSHOTS: usize = 4;
const SMALL_WRITE_COMPACTION_TRIGGER_SNAPSHOT_AGE_MS: u64 = 60_000;
const SMALL_WRITE_COMPACTION_BACKGROUND_INTERVAL_MS: u64 = 10_000;
const SMALL_WRITE_INLINE_MAX_ROWS: usize = cayenne::metadata::DEFAULT_INLINE_MAX_ROWS;
const SMALL_WRITE_INLINE_MAX_BYTES: usize = cayenne::metadata::DEFAULT_INLINE_MAX_BYTES;
const SMALL_WRITE_INLINE_MAX_BUFFER_BYTES: usize =
    cayenne::metadata::DEFAULT_INLINE_MAX_BUFFER_BYTES;
const APPEND_SMALL_WRITE_REFRESH_INTERVAL_THRESHOLD: Duration = Duration::from_secs(300);

fn apply_refresh_mode_defaults(
    config: &mut cayenne::metadata::VortexConfig,
    acceleration: &Acceleration,
    inline_flush_caps: autotune::InlineFlushCaps,
) {
    if uses_small_write_refresh_profile(acceleration) {
        config.compaction_trigger_files = SMALL_WRITE_COMPACTION_TRIGGER_FILES;
        config.compaction_trigger_protected_snapshots =
            SMALL_WRITE_COMPACTION_TRIGGER_PROTECTED_SNAPSHOTS;
        config.compaction_trigger_snapshot_age_ms = SMALL_WRITE_COMPACTION_TRIGGER_SNAPSHOT_AGE_MS;
        config.compaction_background_interval_ms = SMALL_WRITE_COMPACTION_BACKGROUND_INTERVAL_MS;
        // Per-entry inline-admission caps stay static by design: inlined entries
        // are re-read on every scan with no zone-map pruning, so raising them is
        // pure read-amp (see `provider::table` inlining-caps comment).
        config.inline_max_rows = SMALL_WRITE_INLINE_MAX_ROWS;
        config.inline_max_bytes = SMALL_WRITE_INLINE_MAX_BYTES;
        config.inline_max_buffer_bytes = SMALL_WRITE_INLINE_MAX_BUFFER_BYTES;
        // Memtable flush caps scale with machine memory + metastore storage class
        // (see `autotune::HardwareProfile::inline_flush_caps`). Explicit operator
        // params still override these in the param-resolution pass below.
        config.inline_flush_max_rows = inline_flush_caps.max_rows;
        config.inline_flush_max_segments = inline_flush_caps.max_segments;
        config.inline_flush_max_bytes = inline_flush_caps.max_bytes;
    } else {
        config.inline_max_rows = 0;
        config.inline_max_bytes = 0;
        config.inline_max_buffer_bytes = 0;
    }
}

fn uses_small_write_refresh_profile(acceleration: &Acceleration) -> bool {
    match acceleration.refresh_mode.unwrap_or(RefreshMode::Full) {
        RefreshMode::Caching | RefreshMode::Changes => true,
        RefreshMode::Append => acceleration
            .refresh_check_interval
            .is_some_and(|interval| interval <= APPEND_SMALL_WRITE_REFRESH_INTERVAL_THRESHOLD),
        RefreshMode::Disabled | RefreshMode::Full | RefreshMode::Snapshot => false,
    }
}

/// Whether the resolved `on_conflict` performs an upsert (replace existing rows
/// on a primary-key conflict) versus do-nothing. This is the heavy-update CDC
/// signal the keyset auto-derivation keys off: upsert tables can size the keyset
/// to the source cardinality (then fall to the cheap bloom past the budget),
/// whereas a do-nothing table cannot use the bloom (its false positives would
/// wrongly drop rows) and keeps the conservative default.
fn is_upsert_on_conflict(
    on_conflict: Option<&datafusion_table_providers::util::on_conflict::OnConflict>,
) -> bool {
    matches!(
        on_conflict,
        Some(datafusion_table_providers::util::on_conflict::OnConflict::Upsert(_))
    )
}

/// Build the auto-tune [`autotune::WorkloadProfile`] from the dataset's refresh
/// mode, its resolved primary keys / `on_conflict`, and any extended-schema-
/// inference metadata carried on the Arrow schema (`spice.inferred_row_count` /
/// `spice.inferred_table_bytes`, see `data_components::inferred_schema`). Every
/// signal degrades gracefully: an unknown one falls back to the hardware-only
/// derivation.
fn build_workload_profile(
    acceleration: Option<&Acceleration>,
    schema: &Schema,
    primary_keys: &[String],
    on_conflict: Option<&datafusion_table_providers::util::on_conflict::OnConflict>,
) -> autotune::WorkloadProfile {
    let small_write = acceleration.is_some_and(uses_small_write_refresh_profile);
    let inferred =
        data_components::inferred_schema::InferredSchema::from_metadata(schema.metadata());
    autotune::WorkloadProfile::from_inferred(
        small_write,
        primary_keys,
        is_upsert_on_conflict(on_conflict),
        &inferred,
    )
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

/// Strip a `file:`/`file://` scheme (including an optional authority such as
/// `localhost`) so on-disk storage detection receives a real filesystem path.
/// `resolve_metadata_dir` can return such URIs (since `cayenne_file_path` accepts
/// them); feeding `file:///x` or `file://localhost/x` into `Path::new` would make
/// `Auto` storage detection misclassify it as `Unknown`. Returns a borrowed slice
/// (no owned path), so callers can pass the result directly as `&str`.
fn fs_probe_path(path: &str) -> &str {
    if let Some(rest) = path.strip_prefix("file://") {
        // `rest` is either `/abs/path` (empty authority, e.g. `file:///x`) or
        // `authority/abs/path` (e.g. `localhost/abs/path`); the filesystem path
        // begins at the first '/'.
        match rest.find('/') {
            Some(slash) => &rest[slash..],
            None => rest,
        }
    } else {
        path.strip_prefix("file:").unwrap_or(path)
    }
}

impl CayenneAccelerator {
    #[must_use]
    pub fn new() -> Self {
        Self::with_footer_cache_mb(None)
    }

    #[must_use]
    pub fn with_footer_cache_mb(footer_cache_mb: Option<usize>) -> Self {
        let permits = std::thread::available_parallelism()
            .map(std::num::NonZeroUsize::get)
            .unwrap_or(1)
            .max(1);
        Self {
            catalog: Arc::new(OnceCell::new()),
            footer_cache_mb,
            compaction_semaphore: Arc::new(tokio::sync::Semaphore::new(permits)),
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
    fn cayenne_data_dirs_multi_zone(&self, source: &dyn AccelerationSource) -> Result<Vec<String>> {
        let zone_ids = s3::get_s3_zone_ids(source).context(S3Snafu)?;
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
        let paths = self
            .cayenne_data_dirs_multi_zone(source)
            .boxed()
            .context(AccelerationCreationFailedSnafu)?;

        paths
            .first()
            .cloned()
            .ok_or_else(|| Error::InvalidConfiguration {
                detail: Arc::from("Unable to resolve Cayenne storage path"),
            })
    }

    fn get_unsupported_type_action(source: &dyn AccelerationSource) -> UnsupportedTypeAction {
        // Check if unsupported_type_action is specified in acceleration params
        if let Some(acceleration) = source.acceleration()
            && let Some(action_str) = acceleration
                .params
                .get("cayenne_unsupported_type_action")
                .or_else(|| acceleration.params.get("unsupported_type_action"))
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
    #[cfg(test)]
    async fn get_vortex_config(
        table_name: &str,
        source: &dyn AccelerationSource,
    ) -> cayenne::metadata::VortexConfig {
        let small_write = source
            .acceleration()
            .is_some_and(uses_small_write_refresh_profile);
        let workload = autotune::WorkloadProfile::hardware_only(small_write);
        Self::get_vortex_config_with_footer_cache(table_name, source, None, &workload).await
    }

    async fn get_vortex_config_with_footer_cache(
        table_name: &str,
        source: &dyn AccelerationSource,
        footer_cache_mb: Option<usize>,
        workload: &autotune::WorkloadProfile,
    ) -> cayenne::metadata::VortexConfig {
        let mut config = cayenne::metadata::VortexConfig {
            footer_cache_mb,
            ..Default::default()
        };

        // Auto-tune the memory-/cpu-/storage-sensitive Vortex knobs from a
        // single detected host profile so they move together for the host
        // instead of being set in isolation. Every numeric knob below also
        // accepts the literal `auto` (or being left unset) to opt into this
        // derivation; an explicit value always overrides it. See `autotune`.
        if let Some(acceleration) = source.acceleration() {
            let is_s3 = acceleration
                .params
                .get("cayenne_s3_zone_ids")
                .is_some_and(|v| !v.trim().is_empty())
                || acceleration
                    .params
                    .get("cayenne_file_path")
                    .is_some_and(|p| p.starts_with("s3://"));
            let small_write = uses_small_write_refresh_profile(acceleration);

            // Detect the host profile once: cores, cgroup-aware memory, and the
            // storage medium under both the Vortex data files and the metastore
            // (where the inline-memtable BLOBs live and the per-scan re-read cost
            // is paid). A remote (`s3://`) or empty data path classifies as
            // Unknown — correct, since the storage-aware file-size override below
            // is skipped for object stores. `*_dir` may be a `file://` URI, so
            // strip the scheme before probing the real filesystem path.
            let data_dir = CayenneAccelerator::new().cayenne_data_dir(source).ok();
            let metadata_dir = CayenneAccelerator::resolve_metadata_dir(Some(acceleration));
            let hw = autotune::HardwareProfile::detect(
                acceleration.storage_profile,
                data_dir.as_deref().map_or("", fs_probe_path),
                fs_probe_path(&metadata_dir),
            )
            .await;

            // Storage-aware target Vortex file size on local disk (the `auto`
            // baseline): smaller files reduce write amplification on EBS-class
            // network storage; larger files improve scan throughput on RAM-backed
            // mounts. Skipped for S3, where the engine default is kept. An
            // explicit operator value (or `auto`) is then applied on top.
            if !is_s3 && let Some(size_mb) = hw.target_file_size_mb_override() {
                config.target_vortex_file_size_mb = size_mb;
            }
            config.target_vortex_file_size_mb = autotune::auto_or_usize(
                acceleration,
                &["cayenne_target_file_size_mb"],
                config.target_vortex_file_size_mb,
            );

            // Inline-memtable flush caps scale with memory + the metastore's
            // storage medium; only the small-write/CDC profile inlines, so other
            // profiles keep the floor (the caps are then ignored downstream).
            let inline_flush_caps = if small_write {
                hw.inline_flush_caps(workload)
            } else {
                autotune::InlineFlushCaps::FLOOR
            };
            apply_refresh_mode_defaults(&mut config, acceleration, inline_flush_caps);

            // Vortex segment cache: memory-aware `auto` default (scales up on
            // memory-rich hosts, never below the historical 256 MiB), overridable.
            config.segment_cache_mb = autotune::auto_or_usize(
                acceleration,
                &["cayenne_segment_cache_mb"],
                hw.segment_cache_mb(),
            );

            // PK keyset cache: `auto`/unset → memory-derived default; 0 → warn +
            // minimum 1 MiB (mirroring upload_concurrency); else the operator value.
            config.pk_keyset_cache_mb = Some(
                match autotune::read_knob(
                    acceleration,
                    &["cayenne_pk_keyset_cache_mb", "pk_keyset_cache_mb"],
                ) {
                    autotune::Knob::Auto => hw.pk_keyset_cache_mb(workload),
                    autotune::Knob::Set(0) => {
                        tracing::warn!(
                            "Invalid cayenne_pk_keyset_cache_mb value of 0. Using minimum value of 1 MB."
                        );
                        1
                    }
                    autotune::Knob::Set(mb) => mb,
                },
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

            // Parse delta-write encoding level ('auto' or 0..=10, zstd-style).
            // Applies only to fresh delta writes; compaction outputs always use
            // the full default encoding. See `cayenne::metadata::DeltaEncoding`.
            if let Some(encoding_str) = acceleration.params.get("cayenne_delta_encoding") {
                match encoding_str.parse::<cayenne::metadata::DeltaEncoding>() {
                    Ok(encoding) => {
                        config.delta_encoding = encoding;
                    }
                    Err(reason) => {
                        tracing::warn!(
                            "Dataset '{table_name}' contains an invalid `cayenne_delta_encoding` - {reason}. Defaulting to 'auto'.",
                        );
                    }
                }
            }

            if let Some((key, value)) = ["cayenne_pk_conflict_detection", "pk_conflict_detection"]
                .iter()
                .find_map(|key| acceleration.params.get(*key).map(|value| (*key, value)))
            {
                if let Some(mode) = cayenne::metadata::PkConflictDetection::parse(value) {
                    config.pk_conflict_detection = mode;
                } else {
                    tracing::warn!(
                        "Dataset '{table_name}' contains an invalid `{key}` value: '{value}'. Expected one of: auto, none. Defaulting to auto."
                    );
                }
            }

            if let Some((key, value)) = ["cayenne_deletion_mode", "deletion_mode"]
                .iter()
                .find_map(|key| acceleration.params.get(*key).map(|value| (*key, value)))
            {
                if let Some(mode) = cayenne::metadata::DeletionMode::parse(value) {
                    config.deletion_mode = mode;
                } else {
                    tracing::warn!(
                        "Dataset '{table_name}' contains an invalid `{key}` value: '{value}'. Expected one of: auto, key, position. Defaulting to auto."
                    );
                }
            }

            // CDC durability mode (file | memory). Memory mode appends CDC
            // batches to an in-RAM tier and defers the source slot ack to a
            // checkpoint; it is only meaningful for the small-write/CDC profile,
            // so it is forced back to `file` for other profiles below. Default
            // `file` is byte-identical to the pre-feature behavior.
            if let Some((key, value)) = ["cayenne_cdc_durability", "cdc_durability"]
                .iter()
                .find_map(|key| acceleration.params.get(*key).map(|value| (*key, value)))
            {
                if let Some(mode) = cayenne::metadata::CdcDurability::parse(value) {
                    config.cdc_durability = mode;
                } else {
                    tracing::warn!(
                        "Dataset '{table_name}' contains an invalid `{key}` value: '{value}'. Expected one of: file, memory. Defaulting to file."
                    );
                }
            }
            if config.cdc_durability.is_memory() && !uses_small_write_refresh_profile(acceleration)
            {
                tracing::warn!(
                    "Dataset '{table_name}' set `cayenne_cdc_durability: memory` but is not using the small-write/CDC refresh profile (refresh_mode: changes/caching, or append with refresh_check_interval <= 5m). In-memory CDC durability only applies to that profile; defaulting to `file`."
                );
                config.cdc_durability = cayenne::metadata::CdcDurability::File;
            }
            config.cdc_mem_tier_max_bytes = parse_usize_aliases_as_i64(
                acceleration,
                &["cayenne_cdc_mem_tier_max_bytes", "cdc_mem_tier_max_bytes"],
                config.cdc_mem_tier_max_bytes,
            );
            config.cdc_mem_tier_max_age_ms = parse_u64_aliases_with_hint(
                acceleration,
                &["cayenne_cdc_mem_tier_max_age_ms", "cdc_mem_tier_max_age_ms"],
                config.cdc_mem_tier_max_age_ms,
                " (milliseconds)",
            );

            // Parse sort columns
            if let Some(sort_cols_str) = acceleration
                .params
                .get("cayenne_sort_columns")
                .or_else(|| acceleration.params.get("sort_columns"))
            {
                config.sort_columns = sort_cols_str
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
            }

            // Upload concurrency: `auto`/unset keeps the available-parallelism
            // default; 0 → warn + minimum 1. The aggregate across all tables is
            // separately bounded by the process-global encode budget.
            match autotune::read_knob(
                acceleration,
                &["cayenne_upload_concurrency", "upload_concurrency"],
            ) {
                autotune::Knob::Auto => {}
                autotune::Knob::Set(0) => {
                    tracing::warn!(
                        "Invalid cayenne_upload_concurrency value of 0. Using minimum value of 1."
                    );
                    config.upload_concurrency = 1;
                }
                autotune::Knob::Set(n) => config.upload_concurrency = n,
            }

            // Write concurrency: `auto`/unset leaves the per-write default
            // (session target_partitions, capped at the host core count and the
            // global encode budget); 0 → warn + minimum 1.
            match autotune::read_knob(
                acceleration,
                &["cayenne_write_concurrency", "write_concurrency"],
            ) {
                autotune::Knob::Auto => {}
                autotune::Knob::Set(0) => {
                    tracing::warn!(
                        "Invalid cayenne_write_concurrency value of 0. Using minimum value of 1."
                    );
                    config.write_concurrency = Some(1);
                }
                autotune::Knob::Set(n) => config.write_concurrency = Some(n),
            }

            config.compaction_trigger_files = autotune::auto_or_usize(
                acceleration,
                &["cayenne_compaction_trigger_files"],
                config.compaction_trigger_files,
            );
            config.compaction_trigger_protected_snapshots = autotune::auto_or_usize(
                acceleration,
                &["cayenne_compaction_trigger_protected_snapshots"],
                config.compaction_trigger_protected_snapshots,
            );
            config.compaction_trigger_snapshot_age_ms = autotune::auto_or_u64(
                acceleration,
                &["cayenne_compaction_trigger_snapshot_age_ms"],
                config.compaction_trigger_snapshot_age_ms,
            );
            config.compaction_max_levels = autotune::auto_or_usize(
                acceleration,
                &["cayenne_compaction_max_levels"],
                config.compaction_max_levels,
            );
            config.compaction_max_files_per_pick = autotune::auto_or_usize(
                acceleration,
                &["cayenne_compaction_max_files_per_pick"],
                config.compaction_max_files_per_pick,
            );

            config.inline_max_rows = autotune::auto_or_usize(
                acceleration,
                &["cayenne_inline_max_rows", "inline_max_rows"],
                config.inline_max_rows,
            );
            config.inline_max_bytes = autotune::auto_or_usize(
                acceleration,
                &["cayenne_inline_max_bytes", "inline_max_bytes"],
                config.inline_max_bytes,
            );
            config.inline_max_buffer_bytes = autotune::auto_or_usize(
                acceleration,
                &["cayenne_inline_max_buffer_bytes", "inline_max_buffer_bytes"],
                config.inline_max_buffer_bytes,
            );
            config.inline_flush_max_rows = autotune::auto_or_i64(
                acceleration,
                &[
                    "cayenne_inline_flush_max_rows",
                    "inline_flush_max_rows",
                    "cayenne_inline_memtable_max_rows",
                    "inline_memtable_max_rows",
                ],
                config.inline_flush_max_rows,
            );
            config.inline_flush_max_segments = autotune::auto_or_i64(
                acceleration,
                &[
                    "cayenne_inline_flush_max_segments",
                    "inline_flush_max_segments",
                    "cayenne_inline_memtable_max_segments",
                    "inline_memtable_max_segments",
                ],
                config.inline_flush_max_segments,
            );
            config.inline_flush_max_bytes = autotune::auto_or_i64(
                acceleration,
                &[
                    "cayenne_inline_flush_max_bytes",
                    "inline_flush_max_bytes",
                    "cayenne_inline_memtable_max_bytes",
                    "inline_memtable_max_bytes",
                ],
                config.inline_flush_max_bytes,
            );

            config.compaction_background_interval_ms = autotune::auto_or_u64(
                acceleration,
                &["cayenne_compaction_background_interval_ms"],
                config.compaction_background_interval_ms,
            );

            // Tuning mode (`cayenne_tuning`): `auto` (default) derives correct
            // values statically from the detected environment + inferred schema;
            // `adaptive` additionally runs the closed-feedback loop that adapts
            // the knobs over time within the environment-derived [floor, ceiling].
            // Independently, an explicit per-knob value always overrides the
            // derived value — and under `adaptive` it *pins* that knob, so the
            // loop leaves it alone (its bounds collapse to a point downstream).
            let tuning_mode = acceleration
                .params
                .get("cayenne_tuning")
                .map(|v| v.trim().to_ascii_lowercase());
            if let Some(mode) = &tuning_mode
                && mode != "auto"
                && mode != "adaptive"
            {
                tracing::warn!(
                    "Dataset '{table_name}' has an invalid `cayenne_tuning` value: '{mode}'. Expected 'auto' or 'adaptive'. Defaulting to 'auto'."
                );
            }
            config.dynamic_tuning = tuning_mode.as_deref() == Some("adaptive");
            // `adaptive` depends on extended schema inference. Any emitted
            // metadata counts: row_count/table_bytes refine memory sizing, while
            // inferred primary key/index/sort metadata is applied upstream and
            // feeds the same warm-start / query-health surface. Without any
            // inferred metadata the loop starts blind, so fall back to `auto` and
            // tell the operator how to enable it.
            if config.dynamic_tuning && !workload.inferred_metadata.is_present() {
                tracing::warn!(
                    "Dataset '{table_name}': `cayenne_tuning: adaptive` requires `schema_inference: extended` (the closed-loop tuner needs inferred source metadata for its warm-start), but no inferred schema metadata was found; falling back to 'auto' (static). Set `schema_inference: extended` on a connector that emits inferred metadata to enable adaptive tuning."
                );
                config.dynamic_tuning = false;
            }
            // The closed-loop controller rides the per-table background compaction
            // task's tick; with that task disabled (interval == 0) it would never
            // run (nor emit the autotune gauges), so adaptive falls back to auto.
            if config.dynamic_tuning && config.compaction_background_interval_ms == 0 {
                tracing::warn!(
                    "Dataset '{table_name}': `cayenne_tuning: adaptive` needs background compaction enabled (the controller runs on its tick), but cayenne_compaction_background_interval_ms is 0; falling back to 'auto'. Set a non-zero interval to enable adaptive tuning."
                );
                config.dynamic_tuning = false;
            }
            if config.dynamic_tuning {
                tracing::warn!(
                    target: "spiced::acceleration::cayenne",
                    table = %table_name,
                    "`cayenne_tuning: adaptive` is in preview; verify query correctness and performance before using it for production workloads"
                );
            }
            config.pinned_tuning_knobs = cayenne::metadata::PinnedTuningKnobs {
                inline_flush: autotune::is_pinned(
                    acceleration,
                    &[
                        "cayenne_inline_flush_max_bytes",
                        "inline_flush_max_bytes",
                        "cayenne_inline_memtable_max_bytes",
                        "inline_memtable_max_bytes",
                        "cayenne_inline_flush_max_rows",
                        "inline_flush_max_rows",
                        "cayenne_inline_memtable_max_rows",
                        "inline_memtable_max_rows",
                        "cayenne_inline_flush_max_segments",
                        "inline_flush_max_segments",
                        "cayenne_inline_memtable_max_segments",
                        "inline_memtable_max_segments",
                    ],
                ),
                compaction_interval: autotune::is_pinned(
                    acceleration,
                    &["cayenne_compaction_background_interval_ms"],
                ),
                compaction_trigger: autotune::is_pinned(
                    acceleration,
                    &["cayenne_compaction_trigger_files"],
                ),
                write_concurrency: autotune::is_pinned(
                    acceleration,
                    &["cayenne_write_concurrency", "write_concurrency"],
                ),
            };

            // Surface cross-parameter and out-of-range issues that parse cleanly
            // but won't behave as intended (silently clamped at use, or don't
            // compose with each other) — see `VortexConfig::config_warnings`.
            for warning in config.config_warnings(hw.cores) {
                tracing::warn!(
                    "Dataset '{table_name}': {warning} For details, visit: https://spiceai.org/docs/components/data-accelerators/cayenne#configuration"
                );
            }

            // One structured line per table recording the host basis and the
            // knobs `auto` resolved to, so an operator (or a benchmark sweep) can
            // see exactly what was chosen on this host — the observability that
            // makes "works regardless of the host machine" verifiable.
            tracing::info!(
                target: "spiced::acceleration::cayenne",
                table = %table_name,
                cores = hw.cores,
                total_mem_mib = hw.total_mem_bytes / (1024 * 1024),
                data_storage = %hw.data_storage,
                metastore_storage = %hw.metastore_storage,
                runtime_footer_cache_mb = ?config.footer_cache_mb,
                tuning = if config.dynamic_tuning { "adaptive" } else { "auto" },
                // Inferred workload signals (from extended schema inference). When
                // these are `None`/false the schema wasn't inferred for this table,
                // so the data-aware sizing fell back to hardware-only and adaptive
                // (if requested) was gated off — makes that immediately visible.
                inferred_row_count = ?workload.row_count,
                inferred_table_bytes = ?workload.table_bytes,
                inferred_extended_schema = workload.inferred_metadata.is_present(),
                has_primary_key = workload.has_primary_key,
                is_upsert = workload.is_upsert,
                "Cayenne auto-tuned config: segment_cache={}MB, pk_keyset_cache={:?}MB, target_file_size={}MB, upload_concurrency={}, write_concurrency_override={:?}, sort_columns={:?}, compression_strategy={:?}, delta_encoding={}, pk_conflict_detection={}, deletion_mode={:?}, compaction_trigger_files={}, compaction_trigger_protected_snapshots={}, compaction_trigger_snapshot_age_ms={}, compaction_max_levels={}, compaction_max_files_per_pick={}, compaction_background_interval_ms={}, inline_max_rows={}, inline_max_bytes={}, inline_max_buffer_bytes={}, inline_flush_max_rows={}, inline_flush_max_segments={}, inline_flush_max_bytes={}",
                config.segment_cache_mb,
                config.pk_keyset_cache_mb,
                config.target_vortex_file_size_mb,
                config.upload_concurrency,
                config.write_concurrency,
                config.sort_columns,
                config.compression_strategy,
                config.delta_encoding,
                config.pk_conflict_detection.as_str(),
                config.deletion_mode,
                config.compaction_trigger_files,
                config.compaction_trigger_protected_snapshots,
                config.compaction_trigger_snapshot_age_ms,
                config.compaction_max_levels,
                config.compaction_max_files_per_pick,
                config.compaction_background_interval_ms,
                config.inline_max_rows,
                config.inline_max_bytes,
                config.inline_max_buffer_bytes,
                config.inline_flush_max_rows,
                config.inline_flush_max_segments,
                config.inline_flush_max_bytes,
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

    /// Builds a [`cayenne::TimeRetentionFilterBuilder`] from the acceleration
    /// `retention_period` and `time_column` configuration.
    ///
    /// Returns `Ok(None)` when `retention_period` or `time_column` is not
    /// configured.  Returns `Err` when the configuration is present but
    /// invalid (unparseable duration, missing/unsupported column).
    fn build_time_retention_filter_builder(
        source: &dyn AccelerationSource,
        schema: &SchemaRef,
    ) -> Result<Option<cayenne::TimeRetentionFilterBuilder>> {
        let Some(acceleration) = source.acceleration() else {
            return Ok(None);
        };
        let Some(retention_period_str) = acceleration.retention_period.as_deref() else {
            return Ok(None);
        };
        let Some(time_column) = source.time_column() else {
            return Ok(None);
        };

        let retention_duration = fundu::parse_duration(retention_period_str).map_err(|e| {
            Error::InvalidConfiguration {
                detail: Arc::from(format!(
                    "Failed to parse retention_period '{retention_period_str}': {e}"
                )),
            }
        })?;

        let retention_seconds = retention_duration.as_secs();

        let builder =
            cayenne::TimeRetentionFilterBuilder::try_new(time_column, retention_seconds, schema)
                .boxed()
                .context(AccelerationCreationFailedSnafu)?;

        tracing::debug!(
            dataset = %source.name(),
            "Built time retention filter builder for column '{time_column}' with {retention_seconds}s retention"
        );

        Ok(Some(builder))
    }

    #[expect(clippy::too_many_arguments)]
    async fn create_cayenne_table_provider(
        &self,
        table_name: &str,
        dir_path: &str,
        schema: Arc<Schema>,
        source: &dyn AccelerationSource,
        retention_filters: Vec<Expr>,
        time_retention_filter_builder: Option<cayenne::TimeRetentionFilterBuilder>,
        primary_keys: Vec<String>,
        on_conflict: Option<datafusion_table_providers::util::on_conflict::OnConflict>,
        runtime_env: Arc<RuntimeEnv>,
    ) -> Result<Arc<cayenne::CayenneTableProvider>> {
        use cayenne::{CayenneTableProviderBuilder, metadata::CreateTableOptions};

        tracing::debug!("create_cayenne_table_provider: starting for table {table_name}");

        // Get metastore type and metadata directory
        let acceleration = source.acceleration();
        let metadata_dir = Self::resolve_metadata_dir(acceleration);
        let maintained_aggregate_specs = maintained_aggregate_specs_for_cayenne(acceleration)?;
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
        let workload = build_workload_profile(
            acceleration,
            schema.as_ref(),
            &primary_keys,
            on_conflict.as_ref(),
        );
        let vortex_config = Self::get_vortex_config_with_footer_cache(
            table_name,
            source,
            self.footer_cache_mb,
            &workload,
        )
        .await;

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

        let table_options = CreateTableOptions {
            table_name: table_name.to_string(),
            schema: Arc::<arrow_schema::Schema>::clone(&schema),
            primary_key: primary_keys,
            on_conflict,
            base_path: dir_path.to_string(),
            partition_column: None, // Non-partitioned table
            vortex_config,
        };

        // Create shared Cayenne context with the runtime's RuntimeEnv
        let context = cayenne::CayenneContext::new(
            &table_options.vortex_config,
            Arc::clone(&runtime_env),
            table_name,
        );

        // Create CayenneTableProvider with object store for S3 Express One Zone
        let mut builder = CayenneTableProviderBuilder::new(catalog, runtime_env)
            .with_context(context)
            .with_retention_filters(retention_filters)
            .with_maintained_aggregates(maintained_aggregate_specs);
        if let Some(retention_builder) = time_retention_filter_builder {
            builder = builder.with_time_retention_filter_builder(retention_builder);
        }
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
        let provider = Arc::new(cayenne_table);
        let spawned = provider.spawn_background_compaction(Arc::clone(&self.compaction_semaphore));
        if spawned {
            tracing::debug!("Background compaction task spawned for Cayenne table {table_name}",);
        }
        Ok(provider)
    }
}

/// Build a [`NativeVectorIndex`] for each `FixedSizeList<Float32, N>` column in
/// the schema. These indexes are attached to the accelerated table via
/// [`IndexedTableProvider`] so the search engine's `get_vector_index()` can
/// discover them and route `vector_search()` queries through the SIMD distance
/// UDFs rather than the on-the-fly `embed()` fallback.
///
/// This pairs with the existing auto-embedding mechanism: when a dataset
/// declares `columns: [{name: body, embeddings: [{use: model}]}]`,
/// `EmbeddingConnector::wrap_table` produces an `EmbeddingTable` that adds
/// `body_embedding: FixedSizeList<Float32, N>` to the schema handed to
/// `create_external_table`. This helper picks that column up without any
/// additional spicepod configuration.
///
/// Empty schemas and schemas without vector columns return an empty vec — the
/// caller should skip the `IndexedTableProvider` wrap in that case.
fn native_vector_indexes_for_schema(
    schema: &Schema,
    table_name: &str,
    primary_keys: &[String],
) -> Vec<Arc<dyn Index + Send + Sync>> {
    let pk_fields: Vec<arrow_schema::Field> = primary_keys
        .iter()
        .filter_map(|pk_name| {
            schema
                .column_with_name(pk_name)
                .map(|(_, f)| f.as_ref().clone())
        })
        .collect();
    let table_ref = datafusion::sql::TableReference::bare(table_name.to_string());

    schema
        .fields()
        .iter()
        .filter_map(|f| match f.data_type() {
            DataType::FixedSizeList(inner, dim)
                if inner.data_type() == &DataType::Float32 && *dim > 0 =>
            {
                let idx = NativeVectorIndex::new(
                    table_ref.clone(),
                    f.name().clone(),
                    pk_fields.clone(),
                    *dim,
                );
                tracing::debug!(
                    table = table_name,
                    column = f.name(),
                    dimension = dim,
                    "attaching NativeVectorIndex to Cayenne table"
                );
                Some(Arc::new(idx) as Arc<dyn Index + Send + Sync>)
            }
            _ => None,
        })
        .collect()
}

/// Wrap a table provider in [`IndexedTableProvider`] when the schema has at
/// least one vector column.
fn wrap_with_native_vector_indexes(
    provider: Arc<dyn TableProvider>,
    schema: &Schema,
    table_name: &str,
    primary_keys: &[String],
) -> Arc<dyn TableProvider> {
    let indexes = native_vector_indexes_for_schema(schema, table_name, primary_keys);
    if indexes.is_empty() {
        provider
    } else {
        Arc::new(IndexedTableProvider::with_indexes(provider, indexes)) as Arc<dyn TableProvider>
    }
}

const PARAMETERS: &[ParameterSpec] = &concat_arrays::<
    ParameterSpec,
    S3_PARAMS_LEN,
    31,
    { S3_PARAMS_LEN + 31 },
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
            .description("How to handle data types not natively supported by Cayenne (internally using Vortex format) (Time32, Time64, Duration, Interval, etc.). Options: 'string' (convert schema to Utf8, default - requires data source to provide string data), 'error' (fail on unsupported types), 'warn' (include in schema, may fail on insert), 'ignore' (skip unsupported fields)")
            .one_of(&["string", "error", "ignore", "warn"])
            .default("string"),
        ParameterSpec::component("segment_cache_mb")
            .description("Size of the in-memory Vortex decompressed-segment cache in MB. 'auto' (default, or when unset) scales with machine memory (~1/128 of RAM) but never below 256 MB and never above 1024 MB. Set an explicit MB value to override.")
            .default("auto"),
        ParameterSpec::component("pk_keyset_cache_mb")
            .description("Byte budget (in MB) for the in-memory primary-key index used to detect upsert conflicts during CDC ingestion. Within budget an exact keyset is kept; over budget, upsert tables fall back to a bounded bloom existence filter (avoiding the per-batch full-table rebuild) while DoNothing tables rebuild from a scan. When unset, an optimal default is derived from available machine memory."),
        ParameterSpec::component("target_file_size_mb")
            .description("Target size for Vortex data files in MB. 'auto' (default, or when unset) is storage-aware: 256 MB on EBS-class network storage, 64 MB on RAM-backed (tmpfs) mounts, and the 256 MB engine default on local SSD / unknown / S3. Set an explicit MB value to override.")
            .default("auto"),
        ParameterSpec::component("sort_columns")
            .description("Comma-separated list of columns to sort data by during inserts (e.g., 'timestamp,user_id')."),
        ParameterSpec::component("compression_strategy")
            .description("Compression strategy to use for Vortex files. Options: 'btrblocks' (default), 'zstd'")
            .one_of(&["btrblocks", "zstd"])
            .default("btrblocks"),
        ParameterSpec::component("delta_encoding")
            .description("Encoding effort for fresh delta writes (CDC/append snapshot files), zstd-style. 'auto' (default) size-gates: deltas smaller than a quarter of the target file size encode with a light scheme set (skipping the per-file encoder-strategy search and FSST training) and are re-encoded by compaction; larger or unknown-size writes use the full default. Explicit levels 0..=10 pin the effort (0 = uncompressed canonical, 7 = the full default cascade i.e. the explicit opt-out, 8..=10 reserved). Compaction and rewrite outputs always use the full default encoding regardless of this setting.")
            .default("auto"),
        ParameterSpec::component("pk_conflict_detection")
            .description("Whether Cayenne scans existing primary keys on insert. 'auto' (default) detects conflicts and applies on_conflict behavior. 'none' skips conflict detection and is only safe when the source enforces primary-key uniqueness and the ingestion path cannot replay existing rows, such as steady-state append-only CDC after bootstrap.")
            .one_of(&["auto", "none"])
            .default("auto"),
        ParameterSpec::component("deletion_mode")
            .description("How primary-key deletions are recorded and applied. 'auto' (default) resolves to 'position' (merge-on-read): per-file row-position bitmaps are pushed into the Vortex scan, skipping deleted pages at the storage layer with no per-row CPU. For a primary-key table positions are captured via a row_idx() read-back after each write, with key-based fallback for any row whose position is not yet known; a table without a primary key uses the existing position-based strategy. 'key' is the explicit opt-out: deletes are applied above the Vortex scan via a per-row RowConverter probe.")
            .one_of(&["auto", "key", "position"])
            .default("auto"),
        ParameterSpec::component("upload_concurrency")
            .description("Maximum number of concurrent file uploads when writing multiple Vortex files. 'auto' (or unset) uses available CPU parallelism. The aggregate encode concurrency across all Cayenne tables is separately bounded by a process-global budget sized to the host core count."),
        ParameterSpec::component("write_concurrency")
            .description("Writer partition override (parallel encoders) for unsorted Cayenne ingests. 'auto' (or unset) uses a small fixed default of 4, capped at the host core count (= runtime.query.target_partitions) and the process-global encode budget — deliberately not the full core count, because each table is sized independently and the per-table values sum across tables under concurrent CDC. Raise it explicitly for a table that needs more encode parallelism."),
        ParameterSpec::component("compaction_trigger_files")
            .description("Minimum number of small Vortex files in the current snapshot before tiered compaction runs. A 'small' file is one whose size is below cayenne_target_file_size_mb / 4. Default: 4 for refresh_mode: caching, changes, or append with refresh_check_interval <= 5m; 8 otherwise."),
        ParameterSpec::component("compaction_trigger_protected_snapshots")
            .description("Number of protected snapshots before snapshot-maintenance compaction runs. This is separate from compaction_trigger_files so small-file tuning does not silently change scan amplification behavior. Default: 4 for refresh_mode: caching, changes, or append with refresh_check_interval <= 5m; 8 otherwise."),
        ParameterSpec::component("compaction_trigger_snapshot_age_ms")
            .description("Maximum age in milliseconds of the oldest protected snapshot before snapshot-maintenance compaction runs. Set to 0 to disable the age trigger. Default: 60000 for refresh_mode: caching, changes, or append with refresh_check_interval <= 5m; 300000 otherwise."),
        ParameterSpec::component("compaction_max_levels")
            .description("Maximum number of consecutive compaction passes per trigger. Bounds write amplification when promotion keeps producing new candidates. Default: 3.")
            .default("3"),
        ParameterSpec::component("compaction_max_files_per_pick")
            .description("Maximum number of eligible file paths retained in one compaction candidate for trigger selection and observability. The current compactor rewrites the whole current snapshot once triggered, so this does not bound rewrite IO or memory. Default: 32.")
            .default("32"),
        ParameterSpec::component("compaction_background_interval_ms")
            .description("Background compaction interval in milliseconds. The accelerator runs a per-table background task at this interval. Set to 0 to disable the background task — inline compaction on writes still runs. Default: 10000 for refresh_mode: caching, changes, or append with refresh_check_interval <= 5m; 30000 otherwise."),
        ParameterSpec::component("inline_max_rows")
            .description("Maximum rows in a single write that can be inlined into the Cayenne metastore instead of writing a Vortex file. Set to 0 to disable write-entry inlining. Default: 1024 for refresh_mode: caching, changes, or append with refresh_check_interval <= 5m; 0 otherwise."),
        ParameterSpec::component("inline_max_bytes")
            .description("Maximum serialized Arrow IPC bytes in a single inlined Cayenne metastore entry. Set to 0 to disable write-entry inlining. Default: 1048576 for refresh_mode: caching, changes, or append with refresh_check_interval <= 5m; 0 otherwise."),
        ParameterSpec::component("inline_max_buffer_bytes")
            .description("Maximum Arrow in-memory bytes buffered while deciding whether to inline a write. Set to 0 to force the Vortex write path after the first buffered batch. Default: 4194304 for refresh_mode: caching, changes, or append with refresh_check_interval <= 5m; 0 otherwise."),
        ParameterSpec::component("inline_flush_max_rows")
            .description("Maximum inline rows before checkpointing inline data to Vortex. Default: 2048 for refresh_mode: caching, changes, or append with refresh_check_interval <= 5m; 10000 otherwise."),
        ParameterSpec::component("inline_flush_max_segments")
            .description("Maximum inline entries before checkpointing inline data to Vortex. Default: 16 for refresh_mode: caching, changes, or append with refresh_check_interval <= 5m; 64 otherwise."),
        ParameterSpec::component("inline_flush_max_bytes")
            .description("Maximum inline IPC bytes before checkpointing inline data to Vortex. Default: 2097152 for refresh_mode: caching, changes, or append with refresh_check_interval <= 5m; 8388608 otherwise."),
        ParameterSpec::component("cdc_durability")
            .description("Durability mode for the inline CDC write path (refresh_mode: changes). 'file' (default) persists each CDC batch durably before advancing the source slot — byte-identical to the prior behavior. 'memory' appends batches to an in-RAM tier and defers the source slot ack to a periodic/cap-triggered checkpoint, collapsing per-batch durability cost; on crash the un-checkpointed tail is replayed from the source slot (the apply is PK-idempotent, so exactly-once). Bounded by a per-table byte cap and a process-global byte budget so it cannot OOM. Only applies to the small-write/CDC profile and non-partitioned tables.")
            .one_of(&["file", "memory"])
            .default("file"),
        ParameterSpec::component("cdc_mem_tier_max_bytes")
            .description("Per-table RAM-tier byte cap before a forced spill (checkpoint) and slot advance, in cdc_durability: memory mode only. 0 (default) disables the per-table cap; the process-global byte budget still bounds aggregate resident memory. When both are set, whichever is breached first triggers the spill."),
        ParameterSpec::component("cdc_mem_tier_max_age_ms")
            .description("Max wall-clock milliseconds a RAM-tier epoch may age before a forced checkpoint, in cdc_durability: memory mode only. Bounds the crash-replay window for cold/low-traffic tables whose byte cap would otherwise never trip. 0 (default) disables the age trigger."),
        ParameterSpec::component("tuning")
            .description("Auto-tuning mode. 'auto' (default): derive the correct configuration values from the detected environment (cgroup-aware cores + memory, storage class) and the inferred schema (cardinality, row width, primary key) — no closed loop. 'adaptive': additionally run a per-table closed-feedback controller that measures the live CDC ingest rate AND the runtime's whole-system response (apply latency vs offered load, read amplification that slows queries, cgroup-aware memory pressure) and adjusts the inline-memtable flush caps, compaction cadence/trigger, and write concurrency over time, within the environment-derived [floor, ceiling]. 'adaptive' requires 'schema_inference: extended' (the loop's data-aware warm-start needs the inferred cardinality/size); without it, 'adaptive' falls back to 'auto'. In BOTH modes an explicit per-knob value (e.g. cayenne_segment_cache_mb: 512) overrides the derived value; under 'adaptive' an explicitly-set knob is pinned (the loop will not move it).")
            .one_of(&["auto", "adaptive"])
            .default("auto"),
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
            if s3::is_multi_zone_s3_express(source) {
                let zone_ids = s3::get_s3_zone_ids(source).map_err(|source| {
                    Box::new(Error::S3Error { source }) as Box<dyn std::error::Error + Send + Sync>
                })?;
                let dataset_name = source.name().to_string().replace(['.', '/'], "_");
                let app_name = source.app().name.clone();

                let acceleration = source.acceleration().ok_or_else(|| {
                    Box::new(Error::InvalidConfiguration {
                        detail: Arc::from(
                            "Acceleration settings are required for multi-zone S3 Express initialization",
                        ),
                    }) as Box<dyn std::error::Error + Send + Sync>
                })?;

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

                for zone_id in &zone_ids {
                    let bucket_name = s3::generate_bucket_name(&app_name, &dataset_name, zone_id)
                        .map_err(|source| {
                        Box::new(Error::S3Error { source })
                            as Box<dyn std::error::Error + Send + Sync>
                    })?;

                    let region = acceleration
                        .params
                        .get("cayenne_s3_region")
                        .cloned()
                        .or_else(|| s3::derive_region_from_zone(zone_id))
                        .ok_or_else(|| Error::InvalidConfiguration {
                            detail: Arc::from(format!(
                                "Could not determine region for S3 Express zone '{zone_id}'. Specify 'cayenne_s3_region' parameter"
                            )),
                        })?;

                    let created = s3::create_s3_express_bucket_if_needed(
                        &bucket_name,
                        zone_id,
                        &region,
                        access_key.clone(),
                        secret_key.clone(),
                        session_token.clone(),
                    )
                    .await
                    .map_err(|source| {
                        Box::new(Error::S3Error { source })
                            as Box<dyn std::error::Error + Send + Sync>
                    })?;

                    if created {
                        tracing::info!(
                            "Using S3 Express One Zone storage replica: s3://{bucket_name}/{dataset_name}/ (bucket created)"
                        );
                    } else {
                        tracing::info!(
                            "Using S3 Express One Zone storage replica: s3://{bucket_name}/{dataset_name}/ (bucket exists)"
                        );
                    }
                }

                tracing::info!(
                    "Using multi-zone S3 Express One Zone storage: {} zone(s), primary path {dir_path}",
                    zone_ids.len()
                );
            } else {
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
                    tracing::info!(
                        "Using S3 Express One Zone storage: {dir_path} (bucket created)"
                    );
                } else {
                    tracing::info!("Using S3 Express One Zone storage: {dir_path} (bucket exists)");
                }
            }
            tracing::debug!(
                "S3 Express One Zone is optimized for low-latency access within the same AWS Availability Zone. Access from outside AWS may experience higher latency."
            );

            return Ok(BootstrapStatus::none());
        }

        // If mode is FileCreate, snapshot existing data (if enabled) then delete the directory and metadata to start fresh
        if let Some(acceleration) = source.acceleration()
            && acceleration.mode == Mode::FileCreate
        {
            let path_buf = PathBuf::from(&dir_path);
            if path_buf.exists() {
                let metadata_dir_for_snapshot =
                    PathBuf::from(Self::resolve_metadata_dir(Some(acceleration)));
                let snapshot_layout = runtime_acceleration::snapshot::AccelerationLayout::cayenne(
                    metadata_dir_for_snapshot,
                    path_buf.clone(),
                );
                super::snapshots::snapshot_before_recreate(
                    acceleration,
                    &source.name().to_string(),
                    snapshot_layout,
                    AccelerationEngine::Cayenne,
                    Arc::new(arrow_schema::Schema::empty()),
                    // For pre-recreate snapshots we don't have a constructed
                    // catalog handy (the metastore directory may even be in
                    // a transient state). Pass None and accept the default
                    // engine; the resulting snapshot will use the legacy
                    // archive-cayenne.db path. This is acceptable because
                    // pre-recreate snapshots are best-effort backups, not
                    // refresh_mode: snapshot sources.
                    None,
                )
                .await;

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
            let snapshot_adapter = runtime_acceleration::snapshot::AccelerationLayout::cayenne(
                metadata_dir.clone(),
                path_buf.clone(),
            );
            // Build a CayenneSnapshotEngine so the snapshot tar uses the
            // per-dataset metastore-slice format (no raw cayenne.db file)
            // and so `download_latest_snapshot` imports the slice into the
            // local metastore as the final extraction step.
            let metastore_type = acceleration
                .params
                .get("cayenne_metastore")
                .map_or("sqlite", String::as_str)
                .to_string();
            let snapshot_engine = match self
                .get_or_create_catalog(&metadata_dir.to_string_lossy(), &metastore_type)
                .await
            {
                Ok(catalog) => Some(Arc::new(
                    crate::dataaccelerator::cayenne::snapshot_engine::CayenneSnapshotEngine::new(
                        catalog,
                        source.name().to_string(),
                        path_buf.clone(),
                    ),
                )
                    as Arc<dyn runtime_acceleration::snapshot::engine::SnapshotEngine>),
                Err(err) => {
                    tracing::warn!(
                        "Failed to build CayenneSnapshotEngine for snapshot bootstrap, \
                         falling back to default engine: {err}"
                    );
                    None
                }
            };
            Ok(download_snapshot_if_needed(
                acceleration,
                source,
                snapshot_adapter,
                AccelerationEngine::Cayenne,
                snapshot_engine,
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
        runtime_env: Option<Arc<RuntimeEnv>>,
    ) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>> {
        // Cayenne requires a RuntimeEnv to share caches (list_files_cache, object stores)
        // with the main query engine. This must always be provided by the runtime.
        let runtime_env = runtime_env.context(RuntimeEnvRequiredSnafu).boxed()?;

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

        // Build the time-based retention filter builder (e.g., retention_period: 30d)
        let time_retention_filter_builder =
            Self::build_time_retention_filter_builder(source, &arrow_schema)?;

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

        // Extract primary keys and on_conflict once, used by both partitioned and non-partitioned paths.
        // Uses explicit user config if provided, otherwise falls back to federated table constraints
        // (e.g. DynamoDB partition key) and on_conflict from options populated by create_accelerator_table.
        let primary_keys = source
            .acceleration()
            .and_then(|a| a.primary_key.as_ref())
            .map(|pk| {
                pk.iter()
                    .map(std::string::ToString::to_string)
                    .collect::<Vec<_>>()
            })
            .filter(|pk| !pk.is_empty())
            .unwrap_or_else(|| get_primary_keys_from_constraints(&cmd.constraints, &arrow_schema));

        let on_conflict = cmd
            .options
            .get("on_conflict")
            .map(|s| {
                datafusion_table_providers::util::on_conflict::OnConflict::try_from(s.as_str())
            })
            .transpose()
            .map_err(|e| Error::InvalidConfiguration {
                detail: Arc::from(format!("on_conflict invalid: {e}")),
            })?;

        // Always create the base Cayenne table provider
        let cayenne_table = self
            .create_cayenne_table_provider(
                &table_name,
                &dir_path,
                Arc::clone(&arrow_schema),
                source,
                retention_filters.clone(),
                time_retention_filter_builder.clone(),
                primary_keys.clone(),
                on_conflict.clone(),
                Arc::clone(&runtime_env),
            )
            .await
            .boxed()?;

        // If partitioning is requested, wrap with PartitionTableProvider
        if partition_by.is_empty() {
            // Non-partitioned table - wrap in PolyTableProvider for proper deletion/retention support
            // Wrap with upsert deduplication if needed based on on_conflict settings
            let write_provider = upsert_dedup::wrap_with_upsert_dedup_if_needed(
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
                write_provider,
                schema_metadata,
            )) as Arc<dyn TableProvider>;

            Ok(wrap_with_native_vector_indexes(
                table_provider,
                &arrow_schema,
                &table_name,
                &primary_keys,
            ))
        } else {
            // Get metadata catalog for partition tracking
            let metadata_dir = Self::resolve_metadata_dir(source.acceleration());

            // Ensure metadata directory exists
            std::fs::create_dir_all(&metadata_dir)
                .boxed()
                .context(AccelerationCreationFailedSnafu)?;

            // Create a new catalog - it will use WAL mode and busy timeout internally.
            // We keep both a concrete and a trait-object handle: the trait
            // object goes to the per-partition CayenneTableProviders (which use
            // the MetadataCatalog API), while the concrete handle is needed by
            // `CayennePartitionedInsertStrategy` to open a shared
            // MetastoreTransaction across all partitions (issue #10125).
            let catalog_concrete: Arc<cayenne::CayenneCatalog> = Arc::new(
                cayenne::CayenneCatalog::new(format!("sqlite://{metadata_dir}/cayenne.db"))
                    .boxed()
                    .context(AccelerationInitializationFailedSnafu)?,
            );
            // Promote to a trait object for the existing per-partition callers.
            // The coercion from Arc<CayenneCatalog> to Arc<dyn MetadataCatalog>
            // happens via the unsizing rule on the let-binding's declared type.
            let catalog: Arc<dyn cayenne::MetadataCatalog> =
                Arc::<cayenne::CayenneCatalog>::clone(&catalog_concrete);

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
            let workload = build_workload_profile(
                source.acceleration(),
                arrow_schema.as_ref(),
                &primary_keys,
                on_conflict.as_ref(),
            );
            let vortex_config = Self::get_vortex_config_with_footer_cache(
                &table_name,
                source,
                self.footer_cache_mb,
                &workload,
            )
            .await;

            // Log S3 Express configuration for partitioned tables
            if is_s3_express {
                tracing::info!(
                    "Cayenne acceleration for {} configured with S3 Express One Zone storage (target file size: {} MB)",
                    table_name,
                    vortex_config.target_vortex_file_size_mb
                );
            }

            let creator = Arc::new(CayennePartitionCreator::new(
                table_name.clone(),
                PathBuf::from(&dir_path),
                partition_by.clone(),
                Arc::clone(&arrow_schema),
                catalog,
                table_metadata.table_id,
                unsupported_type_action,
                retention_filters,
                time_retention_filter_builder,
                vortex_config,
                object_store_config,
                primary_keys.clone(),
                on_conflict,
                runtime_env,
                Arc::clone(&self.compaction_semaphore),
            ));

            // Wrap the base table provider with partitioning logic, installing
            // the Cayenne-specific cross-partition insert strategy so that
            // overwrite-mode writes batch every partition's catalog mutation
            // into a single MetastoreTransaction (#10125).
            let insert_strategy = Arc::new(
                partitioned_insert_strategy::CayennePartitionedInsertStrategy::new(
                    Arc::clone(&catalog_concrete),
                    PathBuf::from(&dir_path),
                ),
            );
            let partition_provider = Arc::new(
                PartitionTableProvider::new(creator, partition_by, Arc::clone(&arrow_schema))
                    .await
                    .boxed()
                    .context(AccelerationCreationFailedSnafu)?
                    .with_insert_strategy(insert_strategy),
            );

            // Wrap with upsert deduplication if needed based on on_conflict settings
            let write_provider = upsert_dedup::wrap_with_upsert_dedup_if_needed(
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
                write_provider,
                schema_metadata,
            )) as Arc<dyn TableProvider>;

            Ok(wrap_with_native_vector_indexes(
                table_provider,
                &arrow_schema,
                &table_name,
                &primary_keys,
            ))
        }
    }

    fn prefix(&self) -> &'static str {
        "cayenne"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS
    }

    fn supports_snapshot_reload(&self) -> bool {
        true
    }

    /// Build a [`CayenneSnapshotEngine`] for this source so the on-disk
    /// archive uses the per-dataset metastore-slice format (and the writer
    /// skips `cayenne.db` / `-wal` / `-shm`). Returning `None` falls back to
    /// the default `SnapshotManager` engine, which will include the raw
    /// `cayenne.db` file (and its journal sidecar) — that legacy format
    /// breaks `refresh_mode: snapshot` because the reader's local metastore
    /// already exists at extract time.
    async fn snapshot_engine_for_source(
        &self,
        source: &dyn AccelerationSource,
    ) -> Option<Arc<dyn runtime_acceleration::snapshot::engine::SnapshotEngine>> {
        let acceleration = source.acceleration()?;
        let metadata_dir = PathBuf::from(Self::resolve_metadata_dir(Some(acceleration)));
        let metastore_type = acceleration
            .params
            .get("cayenne_metastore")
            .map_or("sqlite", String::as_str)
            .to_string();
        let catalog = match self
            .get_or_create_catalog(&metadata_dir.to_string_lossy(), &metastore_type)
            .await
        {
            Ok(catalog) => catalog,
            Err(err) => {
                tracing::warn!(
                    "Failed to build CayenneSnapshotEngine for snapshot create/extract; \
                     falling back to default engine: {err}"
                );
                return None;
            }
        };
        let dir_path = match self.cayenne_data_dir(source) {
            Ok(p) => p,
            Err(err) => {
                tracing::warn!(
                    "Failed to resolve cayenne data dir for snapshot engine; falling back to default engine: {err}"
                );
                return None;
            }
        };
        Some(Arc::new(
            crate::dataaccelerator::cayenne::snapshot_engine::CayenneSnapshotEngine::new(
                catalog,
                source.name().to_string(),
                PathBuf::from(dir_path),
            ),
        ))
    }

    /// Reloads the Cayenne-backed table provider from the snapshot directory
    /// that was just restored to the accelerator's primary location.
    ///
    /// Cayenne uses a per-dataset directory layout; dropping the previous
    /// provider releases the cached `Vortex` segment/footer caches, and the
    /// factory then reopens the directory tree from disk.
    async fn reload_from_snapshot(
        &self,
        _source: &dyn AccelerationSource,
        previous_provider: Arc<dyn TableProvider>,
        provider_factory: super::ReloadProviderFactory,
    ) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>> {
        drop(previous_provider);
        provider_factory().await
    }

    async fn drop_table(
        &self,
        table_name: &str,
        source: &dyn AccelerationSource,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let dir_path = self.cayenne_data_dir(source).boxed()?;
        let path_buf = PathBuf::from(&dir_path);
        if path_buf.exists() {
            tokio::fs::remove_dir_all(&path_buf).await.boxed()?;
            tracing::info!(
                "Removed Cayenne data directory '{dir_path}' for schema recreation (file_update mode)"
            );
        }

        // Also drop the table from metadata catalog
        if let Some(acceleration) = source.acceleration() {
            let metadata_dir = Self::resolve_metadata_dir(Some(acceleration));
            let metastore_type = acceleration
                .params
                .get("cayenne_metastore")
                .map_or("sqlite", String::as_str);
            if let Ok(catalog) = self
                .get_or_create_catalog(&metadata_dir, metastore_type)
                .await
            {
                let _ = catalog.drop_table(table_name).await;
            }
        }

        // Recreate the data directory so the next create_external_table works
        tokio::fs::create_dir_all(&path_buf).await.boxed()?;
        Ok(())
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
pub(crate) struct CayennePartitionCreator {
    table_name: String,
    base_path: PathBuf,
    /// Partition expressions. For hierarchical partitions like `partition_by: [year, month]`,
    /// this contains all expressions in order.
    partition_by: Vec<PartitionedBy>,
    schema: SchemaRef,
    catalog: Arc<dyn cayenne::MetadataCatalog>,
    table_id: String,
    unsupported_type_action: UnsupportedTypeAction,
    retention_filters: Vec<Expr>,
    time_retention_filter_builder: Option<cayenne::TimeRetentionFilterBuilder>,
    vortex_config: cayenne::metadata::VortexConfig,
    object_store_config: Option<cayenne::metadata::ObjectStoreConfig>,
    primary_key: Vec<String>,
    on_conflict: Option<datafusion_table_providers::util::on_conflict::OnConflict>,
    /// Shared Cayenne context with cache, created once and shared across all partitions.
    context: Arc<cayenne::CayenneContext>,
    /// Shared compaction semaphore inherited from the parent
    /// [`CayenneAccelerator`]. Per-partition providers spawn their own
    /// background compaction tasks through this semaphore so the whole accelerator
    /// shares one concurrency budget.
    compaction_semaphore: Arc<tokio::sync::Semaphore>,
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
            .field(
                "time_retention_filter_builder",
                &self.time_retention_filter_builder.is_some(),
            )
            .field("vortex_config", &"<VortexConfig>")
            .field("object_store_config", &self.object_store_config.is_some())
            .field("primary_key", &self.primary_key)
            .field("on_conflict", &self.on_conflict.is_some())
            .field("context", &"<CayenneContext>")
            .finish_non_exhaustive()
    }
}

impl CayennePartitionCreator {
    #[expect(clippy::too_many_arguments)]
    pub(crate) fn new(
        table_name: String,
        base_path: PathBuf,
        partition_by: Vec<PartitionedBy>,
        schema: SchemaRef,
        catalog: Arc<dyn cayenne::MetadataCatalog>,
        table_id: String,
        unsupported_type_action: UnsupportedTypeAction,
        retention_filters: Vec<Expr>,
        time_retention_filter_builder: Option<cayenne::TimeRetentionFilterBuilder>,
        vortex_config: cayenne::metadata::VortexConfig,
        object_store_config: Option<cayenne::metadata::ObjectStoreConfig>,
        primary_key: Vec<String>,
        on_conflict: Option<datafusion_table_providers::util::on_conflict::OnConflict>,
        runtime_env: Arc<RuntimeEnv>,
        compaction_semaphore: Arc<tokio::sync::Semaphore>,
    ) -> Self {
        // Create shared Cayenne context with cache once, to be shared across all partitions.
        // This ensures all partitions share the same footer/segment caches instead of
        // each partition creating its own cache.
        let context = cayenne::CayenneContext::new(&vortex_config, runtime_env, &table_name);

        Self {
            table_name,
            base_path,
            partition_by,
            schema,
            catalog,
            table_id,
            unsupported_type_action,
            retention_filters,
            time_retention_filter_builder,
            vortex_config,
            object_store_config,
            primary_key,
            on_conflict,
            context,
            compaction_semaphore,
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

        // Encode partition values as strings for metadata storage
        let partition_value_strings: Vec<String> = partition_values
            .iter()
            .map(encode_key)
            .collect::<Result<Vec<_>, _>>()
            .boxed()
            .context(creator::CreatePartitionSnafu)?;

        // Validate partition values for local filesystem compatibility.
        // Partition values matching the pattern `.*#\d+` (e.g., "abcdef#123") are not supported
        // on local filesystem paths but are supported on remote Object Store locations.
        if self.object_store_config.is_none() {
            for value in &partition_value_strings {
                if UNSUPPORTED_LOCAL_PARTITION_PATTERN.is_match(value) {
                    return Err(creator::Error::CreatePartition {
                        source: format!(
                            "Partition value '{value}' is not supported for local filesystem locations. \
                            Values matching the pattern '*#<digits>' (e.g., 'abcdef#123') are only supported \
                            for S3 Express One Zone locations."
                        )
                        .into(),
                    });
                }
            }
        }

        tracing::debug!("creating Cayenne partition at {partition_path}");

        // Create the partition directory (including nested directories for composite partitions)
        std::fs::create_dir_all(&partition_dir)
            .boxed()
            .context(creator::CreatePartitionSnafu)?;
        let partition_column_names = self.partition_column_labels();

        // Create composite key for table naming (slash-separated values)
        let partition_key = partition_value_strings.join("/");

        // Create partition metadata with composite key support
        let partition_metadata = cayenne::PartitionMetadata::new_composite(
            self.table_id.clone(),
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
        let mut builder = cayenne::CayenneTableProviderBuilder::new(
            Arc::clone(&self.catalog),
            Arc::clone(self.context.runtime_env()),
        )
        .with_context(Arc::clone(&self.context))
        .with_retention_filters(self.retention_filters.clone());
        if let Some(ref retention_builder) = self.time_retention_filter_builder {
            builder = builder.with_time_retention_filter_builder(retention_builder.clone());
        }
        if let Some(ref object_store) = self.object_store_config {
            builder = builder.with_object_store(object_store.clone());
        }
        let cayenne_table = builder
            .create(table_options)
            .await
            .boxed()
            .context(creator::CreatePartitionSnafu)?;

        let partition_provider = Arc::new(cayenne_table);
        partition_provider.spawn_background_compaction(Arc::clone(&self.compaction_semaphore));
        Ok(Partition {
            partition_values,
            table_provider: partition_provider,
        })
    }

    async fn infer_existing_partitions(&self) -> Result<Vec<Partition>, creator::Error> {
        // Query catalog for existing partitions
        let partitions = self
            .catalog
            .get_partitions(&self.table_id)
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
            let mut builder = cayenne::CayenneTableProviderBuilder::new(
                Arc::clone(&self.catalog),
                Arc::clone(self.context.runtime_env()),
            )
            .with_context(Arc::clone(&self.context))
            .with_retention_filters(self.retention_filters.clone());
            if let Some(ref retention_builder) = self.time_retention_filter_builder {
                builder = builder.with_time_retention_filter_builder(retention_builder.clone());
            }
            if let Some(ref object_store) = self.object_store_config {
                builder = builder.with_object_store(object_store.clone());
            }
            let cayenne_table = builder
                .open(&partition_table_name)
                .await
                .boxed()
                .context(creator::InferringPartitionsSnafu)?;

            let partition_provider = Arc::new(cayenne_table);
            partition_provider.spawn_background_compaction(Arc::clone(&self.compaction_semaphore));
            result.push(Partition {
                partition_values,
                table_provider: partition_provider,
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
    use crate::component::dataset::acceleration::{Acceleration, Mode, RefreshMode};
    use crate::component::dataset::builder::DatasetBuilder;
    use app::AppBuilder;
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion_table_providers::UnsupportedTypeAction;
    use search::index::{SearchIndex, VectorIndex};
    use std::sync::Arc;

    fn http_response_headers_field() -> Field {
        Field::new(
            "response_headers",
            DataType::Map(
                Arc::new(Field::new_struct(
                    "entries",
                    vec![
                        Arc::new(Field::new("keys", DataType::Utf8, false)),
                        Arc::new(Field::new("values", DataType::Utf8, true)),
                    ],
                    false,
                )),
                false,
            ),
            true,
        )
    }

    fn maintained_aggregate_acceleration() -> Acceleration {
        Acceleration {
            maintained_aggregates: vec![spicepod_acceleration::MaintainedAggregate {
                group_by: vec!["customer_id".to_string()],
                aggregates: vec![spicepod_acceleration::MaintainedAggregateExpr {
                    function: spicepod_acceleration::MaintainedAggregateFunction::Count,
                    column: None,
                }],
            }],
            ..Default::default()
        }
    }

    #[test]
    fn maintained_aggregate_specs_convert_for_unpartitioned_cayenne() {
        let acceleration = maintained_aggregate_acceleration();

        let specs = maintained_aggregate_specs_for_cayenne(Some(&acceleration))
            .expect("unpartitioned maintained aggregate config should convert");

        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].group_by, vec!["customer_id".to_string()]);
        assert_eq!(specs[0].aggregates.len(), 1);
        assert_eq!(
            specs[0].aggregates[0].function,
            cayenne::maintained_aggregate::MaintainedAggregateFunction::Count
        );
        assert_eq!(specs[0].aggregates[0].column, None);
    }

    #[test]
    fn maintained_aggregate_specs_error_for_partitioned_cayenne() {
        let mut acceleration = maintained_aggregate_acceleration();
        acceleration.partition_by = vec![spicepod::partitioning::PartitionedBy {
            name: "region".to_string(),
            expression: "region".to_string(),
        }];

        let error = maintained_aggregate_specs_for_cayenne(Some(&acceleration))
            .expect_err("partitioned maintained aggregate config should be rejected");

        let Error::InvalidConfiguration { detail } = error else {
            panic!("expected InvalidConfiguration, got {error:?}");
        };
        assert!(detail.contains("maintained_aggregates"));
        assert!(detail.contains("partitioned"));
    }

    #[test]
    fn native_vector_indexes_skips_non_vector_schemas() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]);
        let indexes = native_vector_indexes_for_schema(&schema, "users", &["id".to_string()]);
        assert!(indexes.is_empty());
    }

    #[test]
    fn native_vector_indexes_attached_for_fsl_f32() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "embedding",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 768),
                true,
            ),
        ]);
        let indexes = native_vector_indexes_for_schema(&schema, "docs", &["id".to_string()]);
        assert_eq!(indexes.len(), 1);
        let native = indexes[0]
            .as_any()
            .downcast_ref::<NativeVectorIndex>()
            .expect("NativeVectorIndex");
        assert_eq!(native.dimension(), 768);
        assert_eq!(native.search_column(), "embedding");
    }

    #[test]
    fn native_vector_indexes_ignores_wrong_element_type() {
        // Only Float32 is supported by the SIMD kernels — Float64 / Int32 must be skipped.
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "embedding_f64",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float64, true)), 384),
                true,
            ),
            Field::new(
                "nums",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Int32, true)), 4),
                true,
            ),
        ]);
        let indexes = native_vector_indexes_for_schema(&schema, "docs", &["id".to_string()]);
        assert!(indexes.is_empty());
    }

    #[test]
    fn native_vector_indexes_attached_for_auto_generated_embedding_column() {
        // Mirrors the schema EmbeddingTable would advertise for a dataset with
        // `columns: [{ name: body, embeddings: [{ use: model }] }]`:
        // original text column + `{col}_embedding: FixedSizeList<Float32, N>`.
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("body", DataType::Utf8, true),
            Field::new(
                "body_embedding",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 384),
                true,
            ),
        ]);
        let indexes = native_vector_indexes_for_schema(&schema, "docs", &["id".to_string()]);
        assert_eq!(indexes.len(), 1);
        let native = indexes[0]
            .as_any()
            .downcast_ref::<NativeVectorIndex>()
            .expect("NativeVectorIndex for auto-generated embedding column");
        assert_eq!(native.search_column(), "body_embedding");
        assert_eq!(native.dimension(), 384);
    }

    #[test]
    fn native_vector_indexes_attached_per_vector_column() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "title_embed",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 256),
                true,
            ),
            Field::new(
                "body_embed",
                DataType::FixedSizeList(
                    Arc::new(Field::new("item", DataType::Float32, true)),
                    1536,
                ),
                true,
            ),
        ]);
        let indexes = native_vector_indexes_for_schema(&schema, "docs", &["id".to_string()]);
        assert_eq!(indexes.len(), 2);
        let dims: Vec<i32> = indexes
            .iter()
            .filter_map(|i| {
                i.as_any()
                    .downcast_ref::<NativeVectorIndex>()
                    .map(VectorIndex::dimension)
            })
            .collect();
        assert!(dims.contains(&256));
        assert!(dims.contains(&1536));
    }

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

    #[tokio::test]
    async fn test_cayenne_multi_zone_primary_path_generation() {
        let app = AppBuilder::new("test-app").build();
        let rt = crate::Runtime::builder().build().await;

        let mut dataset = DatasetBuilder::try_new("orders.dataset".to_string(), "orders.dataset")
            .expect("Failed to create builder")
            .with_app(Arc::new(app))
            .with_runtime(Arc::new(rt))
            .build()
            .expect("Failed to build dataset");

        dataset.acceleration = Some(Acceleration {
            engine: Engine::Cayenne,
            mode: Mode::File,
            params: [(
                "cayenne_s3_zone_ids".to_string(),
                "usw2-az1,usw2-az2".to_string(),
            )]
            .into_iter()
            .collect(),
            ..Default::default()
        });

        let accelerator = CayenneAccelerator::new();
        let primary_data_dir = accelerator
            .resolve_storage_config(&dataset)
            .expect("Expected primary multi-zone path");

        assert!(
            primary_data_dir.starts_with("s3://spice-test-app-orders-dataset--usw2-az1--x-s3/"),
            "Expected first zone to be primary path, got: {primary_data_dir}"
        );
        assert!(primary_data_dir.ends_with("/orders_dataset/"));
    }

    #[test]
    fn test_transform_schema_for_vortex_preserves_http_response_headers_map() {
        let schema = Schema::new(vec![
            Field::new("response_status", DataType::UInt16, false),
            http_response_headers_field(),
        ]);

        let transformed = transform_schema_for_vortex(&schema, UnsupportedTypeAction::Error)
            .expect("HTTP response headers map should be supported by Cayenne/Vortex");

        assert_eq!(transformed, schema);
    }

    #[test]
    fn test_transform_schema_for_vortex_only_flags_truly_unsupported_types() {
        let schema = Schema::new(vec![
            http_response_headers_field(),
            Field::new(
                "duration_col",
                DataType::Duration(TimeUnit::Millisecond),
                true,
            ),
        ]);

        let error = transform_schema_for_vortex(&schema, UnsupportedTypeAction::Error)
            .expect_err("duration should remain unsupported in error mode");

        match error {
            Error::UnsupportedDataTypes { details } => {
                assert!(
                    details.contains("duration_col"),
                    "expected duration column in unsupported type error, got: {details}"
                );
                assert!(
                    !details.contains("response_headers"),
                    "response_headers map should not be reported as unsupported: {details}"
                );
            }
            other => panic!("expected UnsupportedDataTypes error, got: {other}"),
        }
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
    fn test_fs_probe_path_strips_file_scheme() {
        // file:// URIs are reduced to their filesystem path for storage detection.
        assert_eq!(
            fs_probe_path("file:///data/cayenne/metadata"),
            "/data/cayenne/metadata"
        );
        assert_eq!(fs_probe_path("file:/data/cayenne"), "/data/cayenne");
        // An explicit authority (e.g. localhost) is dropped down to the path.
        assert_eq!(
            fs_probe_path("file://localhost/data/cayenne"),
            "/data/cayenne"
        );
        // Plain paths pass through unchanged.
        assert_eq!(
            fs_probe_path("/data/cayenne/metadata"),
            "/data/cayenne/metadata"
        );
        assert_eq!(fs_probe_path("relative/metadata"), "relative/metadata");
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

    #[tokio::test]
    async fn test_write_concurrency_is_resolved_per_dataset() {
        let app = Arc::new(AppBuilder::new("test").build());
        let rt = Arc::new(crate::Runtime::builder().build().await);

        let mut hot_dataset = DatasetBuilder::try_new("hot".to_string(), "hot")
            .expect("hot dataset builder")
            .with_app(Arc::clone(&app))
            .with_runtime(Arc::clone(&rt))
            .build()
            .expect("hot dataset");
        hot_dataset.acceleration = Some(Acceleration {
            engine: Engine::Cayenne,
            mode: Mode::File,
            params: [("cayenne_write_concurrency".to_string(), "16".to_string())]
                .into_iter()
                .collect(),
            ..Default::default()
        });

        let mut quiet_dataset = DatasetBuilder::try_new("quiet".to_string(), "quiet")
            .expect("quiet dataset builder")
            .with_app(app)
            .with_runtime(rt)
            .build()
            .expect("quiet dataset");
        quiet_dataset.acceleration = Some(Acceleration {
            engine: Engine::Cayenne,
            mode: Mode::File,
            params: [("cayenne_write_concurrency".to_string(), "2".to_string())]
                .into_iter()
                .collect(),
            ..Default::default()
        });

        let hot = CayenneAccelerator::get_vortex_config("hot", &hot_dataset).await;
        let quiet = CayenneAccelerator::get_vortex_config("quiet", &quiet_dataset).await;

        assert_eq!(hot.write_concurrency, Some(16));
        assert_eq!(quiet.write_concurrency, Some(2));
    }

    #[tokio::test]
    async fn test_vortex_config_defaults_use_small_write_refresh_profile() {
        let app = Arc::new(AppBuilder::new("test").build());
        let rt = Arc::new(crate::Runtime::builder().build().await);

        for (table_name, refresh_mode) in [
            ("cached_hot", RefreshMode::Caching),
            ("cdc_hot", RefreshMode::Changes),
        ] {
            let mut dataset = DatasetBuilder::try_new(table_name.to_string(), table_name)
                .expect("dataset builder")
                .with_app(Arc::clone(&app))
                .with_runtime(Arc::clone(&rt))
                .build()
                .expect("dataset");
            dataset.acceleration = Some(Acceleration {
                engine: Engine::Cayenne,
                mode: Mode::File,
                refresh_mode: Some(refresh_mode),
                ..Default::default()
            });

            let config = CayenneAccelerator::get_vortex_config(table_name, &dataset).await;

            assert_eq!(
                config.compaction_trigger_files,
                SMALL_WRITE_COMPACTION_TRIGGER_FILES
            );
            assert_eq!(
                config.compaction_trigger_protected_snapshots,
                SMALL_WRITE_COMPACTION_TRIGGER_PROTECTED_SNAPSHOTS
            );
            assert_eq!(
                config.compaction_trigger_snapshot_age_ms,
                SMALL_WRITE_COMPACTION_TRIGGER_SNAPSHOT_AGE_MS
            );
            assert_eq!(
                config.compaction_background_interval_ms,
                SMALL_WRITE_COMPACTION_BACKGROUND_INTERVAL_MS
            );
            assert_eq!(config.inline_max_rows, SMALL_WRITE_INLINE_MAX_ROWS);
            assert_eq!(config.inline_max_bytes, SMALL_WRITE_INLINE_MAX_BYTES);
            assert_eq!(
                config.inline_max_buffer_bytes,
                SMALL_WRITE_INLINE_MAX_BUFFER_BYTES
            );
            // Flush caps are memory/storage-derived; exact scaling is pinned in
            // `test_inline_flush_caps_scale_with_memory_and_storage`. Here assert the
            // deterministic [floor, ceiling] bounds they hold on any host/medium.
            assert!((2 * 1_048_576..=256 * 1_048_576).contains(&config.inline_flush_max_bytes));
            assert!((2_048..=262_144).contains(&config.inline_flush_max_rows));
            assert!((16..=256).contains(&config.inline_flush_max_segments));
        }

        let mut dataset = DatasetBuilder::try_new("append_hot".to_string(), "append_hot")
            .expect("dataset builder")
            .with_app(Arc::clone(&app))
            .with_runtime(Arc::clone(&rt))
            .build()
            .expect("dataset");
        dataset.acceleration = Some(Acceleration {
            engine: Engine::Cayenne,
            mode: Mode::File,
            refresh_mode: Some(RefreshMode::Append),
            refresh_check_interval: Some(APPEND_SMALL_WRITE_REFRESH_INTERVAL_THRESHOLD),
            ..Default::default()
        });

        let config = CayenneAccelerator::get_vortex_config("append_hot", &dataset).await;

        assert_eq!(
            config.compaction_trigger_files,
            SMALL_WRITE_COMPACTION_TRIGGER_FILES
        );
        assert_eq!(
            config.compaction_trigger_protected_snapshots,
            SMALL_WRITE_COMPACTION_TRIGGER_PROTECTED_SNAPSHOTS
        );
        assert!((2 * 1_048_576..=256 * 1_048_576).contains(&config.inline_flush_max_bytes));
        assert!((2_048..=262_144).contains(&config.inline_flush_max_rows));
        assert!((16..=256).contains(&config.inline_flush_max_segments));
    }

    #[tokio::test]
    async fn test_vortex_config_defaults_use_large_write_refresh_profile() {
        let app = Arc::new(AppBuilder::new("test").build());
        let rt = Arc::new(crate::Runtime::builder().build().await);

        for (table_name, refresh_mode) in [
            ("append_manual_load", Some(RefreshMode::Append)),
            ("default_load", None),
            ("full_load", Some(RefreshMode::Full)),
            ("snapshot_load", Some(RefreshMode::Snapshot)),
            ("disabled_load", Some(RefreshMode::Disabled)),
        ] {
            let mut dataset = DatasetBuilder::try_new(table_name.to_string(), table_name)
                .expect("dataset builder")
                .with_app(Arc::clone(&app))
                .with_runtime(Arc::clone(&rt))
                .build()
                .expect("dataset");
            dataset.acceleration = Some(Acceleration {
                engine: Engine::Cayenne,
                mode: Mode::File,
                refresh_mode,
                ..Default::default()
            });

            let config = CayenneAccelerator::get_vortex_config(table_name, &dataset).await;

            assert_eq!(config.inline_max_rows, 0);
            assert_eq!(config.inline_max_bytes, 0);
            assert_eq!(config.inline_max_buffer_bytes, 0);
            assert_eq!(
                config.inline_flush_max_rows,
                cayenne::metadata::DEFAULT_INLINE_FLUSH_MAX_ROWS
            );
            assert_eq!(
                config.compaction_trigger_files,
                cayenne::metadata::VortexConfig::default().compaction_trigger_files
            );
            assert_eq!(
                config.compaction_trigger_protected_snapshots,
                cayenne::metadata::VortexConfig::default().compaction_trigger_protected_snapshots
            );
        }

        let mut dataset =
            DatasetBuilder::try_new("append_batch_load".to_string(), "append_batch_load")
                .expect("dataset builder")
                .with_app(Arc::clone(&app))
                .with_runtime(Arc::clone(&rt))
                .build()
                .expect("dataset");
        dataset.acceleration = Some(Acceleration {
            engine: Engine::Cayenne,
            mode: Mode::File,
            refresh_mode: Some(RefreshMode::Append),
            refresh_check_interval: Some(
                APPEND_SMALL_WRITE_REFRESH_INTERVAL_THRESHOLD + Duration::from_secs(1),
            ),
            ..Default::default()
        });

        let config = CayenneAccelerator::get_vortex_config("append_batch_load", &dataset).await;

        assert_eq!(config.inline_max_rows, 0);
        assert_eq!(config.inline_max_bytes, 0);
        assert_eq!(config.inline_max_buffer_bytes, 0);
        assert_eq!(
            config.inline_flush_max_rows,
            cayenne::metadata::DEFAULT_INLINE_FLUSH_MAX_ROWS
        );
        assert_eq!(
            config.compaction_trigger_files,
            cayenne::metadata::VortexConfig::default().compaction_trigger_files
        );
        assert_eq!(
            config.compaction_trigger_protected_snapshots,
            cayenne::metadata::VortexConfig::default().compaction_trigger_protected_snapshots
        );
    }

    #[tokio::test]
    async fn test_inline_thresholds_are_resolved_from_acceleration_params() {
        let app = Arc::new(AppBuilder::new("test").build());
        let rt = Arc::new(crate::Runtime::builder().build().await);

        let mut dataset = DatasetBuilder::try_new("cdc_hot".to_string(), "cdc_hot")
            .expect("dataset builder")
            .with_app(app)
            .with_runtime(rt)
            .build()
            .expect("dataset");
        dataset.acceleration = Some(Acceleration {
            engine: Engine::Cayenne,
            mode: Mode::File,
            refresh_mode: Some(RefreshMode::Changes),
            params: [
                ("cayenne_inline_max_rows".to_string(), "123".to_string()),
                ("cayenne_inline_max_bytes".to_string(), "262144".to_string()),
                (
                    "cayenne_inline_max_buffer_bytes".to_string(),
                    "524288".to_string(),
                ),
                (
                    "cayenne_inline_flush_max_rows".to_string(),
                    "4096".to_string(),
                ),
                (
                    "cayenne_inline_flush_max_segments".to_string(),
                    "32".to_string(),
                ),
                (
                    "cayenne_inline_flush_max_bytes".to_string(),
                    "3145728".to_string(),
                ),
                (
                    "cayenne_pk_conflict_detection".to_string(),
                    "none".to_string(),
                ),
            ]
            .into_iter()
            .collect(),
            ..Default::default()
        });

        let config = CayenneAccelerator::get_vortex_config("cdc_hot", &dataset).await;

        assert_eq!(config.inline_max_rows, 123);
        assert_eq!(config.inline_max_bytes, 262_144);
        assert_eq!(config.inline_max_buffer_bytes, 524_288);
        assert_eq!(config.inline_flush_max_rows, 4_096);
        assert_eq!(config.inline_flush_max_segments, 32);
        assert_eq!(config.inline_flush_max_bytes, 3_145_728);
        assert_eq!(
            config.pk_conflict_detection,
            cayenne::metadata::PkConflictDetection::None
        );
    }

    #[tokio::test]
    async fn test_documented_cdc_mem_tier_params_are_resolved() {
        let app = Arc::new(AppBuilder::new("test").build());
        let rt = Arc::new(crate::Runtime::builder().build().await);

        let mut dataset = DatasetBuilder::try_new("cdc_hot".to_string(), "cdc_hot")
            .expect("dataset builder")
            .with_app(app)
            .with_runtime(rt)
            .build()
            .expect("dataset");
        dataset.acceleration = Some(Acceleration {
            engine: Engine::Cayenne,
            mode: Mode::File,
            refresh_mode: Some(RefreshMode::Changes),
            params: [
                ("cdc_mem_tier_max_bytes".to_string(), "123456".to_string()),
                ("cdc_mem_tier_max_age_ms".to_string(), "7890".to_string()),
            ]
            .into_iter()
            .collect(),
            ..Default::default()
        });

        let config = CayenneAccelerator::get_vortex_config("cdc_hot", &dataset).await;

        assert_eq!(config.cdc_mem_tier_max_bytes, 123_456);
        assert_eq!(config.cdc_mem_tier_max_age_ms, 7_890);
    }

    #[tokio::test]
    async fn test_inline_partial_override_preserves_refresh_profile_defaults() {
        let app = Arc::new(AppBuilder::new("test").build());
        let rt = Arc::new(crate::Runtime::builder().build().await);

        let mut small_write_dataset =
            DatasetBuilder::try_new("cdc_partial_override".to_string(), "cdc_partial_override")
                .expect("dataset builder")
                .with_app(Arc::clone(&app))
                .with_runtime(Arc::clone(&rt))
                .build()
                .expect("dataset");
        small_write_dataset.acceleration = Some(Acceleration {
            engine: Engine::Cayenne,
            mode: Mode::File,
            refresh_mode: Some(RefreshMode::Changes),
            params: [("cayenne_inline_max_rows".to_string(), "321".to_string())]
                .into_iter()
                .collect(),
            ..Default::default()
        });

        let small_write_config =
            CayenneAccelerator::get_vortex_config("cdc_partial_override", &small_write_dataset)
                .await;

        assert_eq!(small_write_config.inline_max_rows, 321);
        assert_eq!(
            small_write_config.inline_max_bytes,
            SMALL_WRITE_INLINE_MAX_BYTES
        );
        assert_eq!(
            small_write_config.inline_max_buffer_bytes,
            SMALL_WRITE_INLINE_MAX_BUFFER_BYTES
        );

        let mut large_write_dataset =
            DatasetBuilder::try_new("full_partial_override".to_string(), "full_partial_override")
                .expect("dataset builder")
                .with_app(app)
                .with_runtime(rt)
                .build()
                .expect("dataset");
        large_write_dataset.acceleration = Some(Acceleration {
            engine: Engine::Cayenne,
            mode: Mode::File,
            refresh_mode: Some(RefreshMode::Full),
            params: [("cayenne_inline_max_rows".to_string(), "321".to_string())]
                .into_iter()
                .collect(),
            ..Default::default()
        });

        let large_write_config =
            CayenneAccelerator::get_vortex_config("full_partial_override", &large_write_dataset)
                .await;

        assert_eq!(large_write_config.inline_max_rows, 321);
        assert_eq!(large_write_config.inline_max_bytes, 0);
        assert_eq!(large_write_config.inline_max_buffer_bytes, 0);
    }

    #[tokio::test]
    async fn test_compaction_thresholds_are_resolved_from_acceleration_params() {
        let app = Arc::new(AppBuilder::new("test").build());
        let rt = Arc::new(crate::Runtime::builder().build().await);

        let mut dataset = DatasetBuilder::try_new("compact".to_string(), "compact")
            .expect("dataset builder")
            .with_app(app)
            .with_runtime(rt)
            .build()
            .expect("dataset");
        dataset.acceleration = Some(Acceleration {
            engine: Engine::Cayenne,
            mode: Mode::File,
            params: [
                (
                    "cayenne_compaction_trigger_files".to_string(),
                    "12".to_string(),
                ),
                (
                    "cayenne_compaction_trigger_snapshot_age_ms".to_string(),
                    "120000".to_string(),
                ),
                (
                    "cayenne_compaction_trigger_protected_snapshots".to_string(),
                    "9".to_string(),
                ),
                ("cayenne_compaction_max_levels".to_string(), "5".to_string()),
                (
                    "cayenne_compaction_max_files_per_pick".to_string(),
                    "64".to_string(),
                ),
                (
                    "cayenne_compaction_background_interval_ms".to_string(),
                    "45000".to_string(),
                ),
            ]
            .into_iter()
            .collect(),
            ..Default::default()
        });

        let config = CayenneAccelerator::get_vortex_config("compact", &dataset).await;

        assert_eq!(config.compaction_trigger_files, 12);
        assert_eq!(config.compaction_trigger_protected_snapshots, 9);
        assert_eq!(config.compaction_trigger_snapshot_age_ms, 120_000);
        assert_eq!(config.compaction_max_levels, 5);
        assert_eq!(config.compaction_max_files_per_pick, 64);
        assert_eq!(config.compaction_background_interval_ms, 45_000);
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

    #[test]
    fn test_unsupported_local_partition_pattern() {
        // Pattern should match values ending with `#<digits>` (e.g., "abcdef#123")
        // These are only supported on S3 Express One Zone, not local filesystem.

        // Values that should match (unsupported on local filesystem)
        assert!(
            UNSUPPORTED_LOCAL_PARTITION_PATTERN.is_match("abcdef#123"),
            "Expected 'abcdef#123' to match unsupported pattern"
        );
        assert!(
            UNSUPPORTED_LOCAL_PARTITION_PATTERN.is_match("test#1"),
            "Expected 'test#1' to match unsupported pattern"
        );
        assert!(
            UNSUPPORTED_LOCAL_PARTITION_PATTERN.is_match("some_value#999999"),
            "Expected 'some_value#999999' to match unsupported pattern"
        );
        assert!(
            UNSUPPORTED_LOCAL_PARTITION_PATTERN.is_match("#0"),
            "Expected '#0' to match unsupported pattern"
        );
        assert!(
            UNSUPPORTED_LOCAL_PARTITION_PATTERN.is_match("a#1"),
            "Expected 'a#1' to match unsupported pattern"
        );

        // Values that should NOT match (supported on local filesystem)
        assert!(
            !UNSUPPORTED_LOCAL_PARTITION_PATTERN.is_match("abcdef"),
            "Expected 'abcdef' to not match unsupported pattern"
        );
        assert!(
            !UNSUPPORTED_LOCAL_PARTITION_PATTERN.is_match("test_123"),
            "Expected 'test_123' to not match unsupported pattern"
        );
        assert!(
            !UNSUPPORTED_LOCAL_PARTITION_PATTERN.is_match("2024-01-01"),
            "Expected '2024-01-01' to not match unsupported pattern"
        );
        assert!(
            !UNSUPPORTED_LOCAL_PARTITION_PATTERN.is_match("partition_value"),
            "Expected 'partition_value' to not match unsupported pattern"
        );
        assert!(
            !UNSUPPORTED_LOCAL_PARTITION_PATTERN.is_match("123"),
            "Expected '123' (pure digits) to not match unsupported pattern"
        );
        assert!(
            !UNSUPPORTED_LOCAL_PARTITION_PATTERN.is_match("abc#def"),
            "Expected 'abc#def' (# not followed by only digits) to not match unsupported pattern"
        );
        assert!(
            !UNSUPPORTED_LOCAL_PARTITION_PATTERN.is_match("test#"),
            "Expected 'test#' (# with no digits) to not match unsupported pattern"
        );
        assert!(
            !UNSUPPORTED_LOCAL_PARTITION_PATTERN.is_match("test#abc"),
            "Expected 'test#abc' (# followed by non-digits) to not match unsupported pattern"
        );
        assert!(
            !UNSUPPORTED_LOCAL_PARTITION_PATTERN.is_match("test#123abc"),
            "Expected 'test#123abc' (digits followed by non-digits) to not match unsupported pattern"
        );
    }
}
