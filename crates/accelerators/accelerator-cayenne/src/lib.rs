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

// `pub(crate)` (not private) so the Cayenne *catalog* connector
// (the runtime's Cayenne catalog connector) can seed the adaptive-tuning knobs from
// the same hardware-derived profile this accelerator path uses.
pub(crate) mod autotune;
mod imds;
pub mod partitioned_insert_strategy;
pub mod s3;
pub mod snapshot_engine;

use std::any::Any;
use std::collections::HashMap;
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use arrow_schema::{DataType, Schema};
use async_trait::async_trait;
use cayenne::CayennePartitionCreator;
use data_components::poly::PolyTableProvider;
use datafusion::common::arrow::datatypes::SchemaRef;
use datafusion::datasource::TableProvider;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::logical_expr::CreateExternalTable;
use datafusion::prelude::Expr;
use datafusion_table_providers::UnsupportedTypeAction;
use runtime_table_partition::expression::PartitionedBy;
use runtime_table_partition::provider::PartitionTableProvider;
use snafu::prelude::*;
use tokio::sync::OnceCell;
use util::concat_arrays;

use crate::s3::{S3_PARAMETERS, S3_PARAMS_LEN};
use data_accelerator_api::FilePathError;
use data_accelerator_api::snapshots::download_snapshot_if_needed;
use data_accelerator_api::spice_data_base_path;
use data_accelerator_api::{
    AccelerationSource, AcceleratorEngineRegistry, BootstrapStatus, DataAccelerator,
    get_primary_keys_from_constraints, upsert_dedup,
};
use runtime_acceleration::Engine;
use runtime_acceleration::OnSchemaChange;
use runtime_acceleration::acceleration::{Acceleration, Mode, RefreshMode};
use runtime_acceleration::acceleration_source::resolved_refresh_mode;
use runtime_acceleration::sidecar::{AcceleratorSidecar, OpenOption};
use runtime_acceleration::snapshot::{AccelerationEngine, AccelerationLayout};
use runtime_checkpoint_api::CheckpointError;
use runtime_checkpoint_sqlite::SqliteSidecar;
use runtime_parameters::ParameterSpec;
use search::index::native_vector::NativeVectorIndex;
use spice_table::{Index, IndexLayer};
use spicepod::acceleration as spicepod_acceleration;

/// Metadata key to identify the accelerator type in the schema metadata.
const SPICE_ACCELERATOR_METADATA_KEY: &str = "spice.accelerator";

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to create table: {source}"))]
    UnableToCreateTable {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display(
        "Failed to configure dataset {table_name} (cayenne): Invalid 'cayenne_scan_concurrency' value '{value}'. \
        Expected 'auto', 'off', or a positive number of splits. \
        See: https://spiceai.org/docs/components/data-accelerators/cayenne"
    ))]
    InvalidScanConcurrency { table_name: String, value: String },

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
        "Failed to evolve the schema of dataset {dataset} (cayenne): in-place schema evolution is not supported for a partitioned acceleration, \
        because each partition stores its own schema. \
        Set 'mode: file_update', or 'on_schema_change: drop_and_recreate' with 'refresh_mode: full', to rebuild the acceleration with the new schema. \
        See: https://spiceai.org/docs/components/data-accelerators/cayenne"
    ))]
    PartitionedEvolutionUnsupported { dataset: Arc<str> },

    #[snafu(display(
        "Failed to configure dataset {table_name} (cayenne): The acceleration data directory '{data_dir}' contains the Cayenne metastore directory '{metadata_dir}'. \
        Recreating this dataset deletes its data directory, which would take the metastore — the catalog for every Cayenne dataset in this instance — with it. \
        Set 'cayenne_metadata_dir' to a directory outside the data directory, or rename the dataset. \
        See: https://spiceai.org/docs/components/data-accelerators/cayenne"
    ))]
    MetastoreInsideDataDir {
        table_name: String,
        data_dir: String,
        metadata_dir: String,
    },

    #[snafu(display(
        "Failed to configure dataset {table_name} (cayenne): Could not resolve the acceleration data directory '{data_dir}' or the Cayenne metastore directory '{metadata_dir}' against the working directory ({source}). \
        Recreating this dataset deletes its data directory, and Spice will not do that without first proving the metastore is outside it. \
        Set 'cayenne_file_path' and 'cayenne_metadata_dir' to absolute paths. \
        See: https://spiceai.org/docs/components/data-accelerators/cayenne"
    ))]
    CayenneDirsUnresolvable {
        table_name: String,
        data_dir: String,
        metadata_dir: String,
        source: std::io::Error,
    },

    #[snafu(display(
        "Failed to recreate dataset {table_name} (cayenne): The acceleration data directory '{data_dir}' holds a Cayenne metastore at '{metastore_path}'. \
        Recreating this dataset deletes its data directory, which would take that catalog — and every Cayenne dataset recorded in it — with it. \
        Move the metastore out of the data directory, or set 'cayenne_file_path' for this dataset to a directory that does not contain one. \
        See: https://spiceai.org/docs/components/data-accelerators/cayenne"
    ))]
    MetastoreFileInsideDataDir {
        table_name: String,
        data_dir: String,
        metastore_path: String,
    },

    #[snafu(display(
        "Failed to recreate dataset {table_name} (cayenne): Could not read the acceleration data directory '{data_dir}' to check it for a Cayenne metastore ({source}). \
        Recreating this dataset deletes that directory, and Spice will not do that without first proving no metastore is inside it. \
        Restore read permission on '{data_dir}', or set 'cayenne_file_path' to a directory Spice can read. \
        See: https://spiceai.org/docs/components/data-accelerators/cayenne"
    ))]
    MetastoreScanFailed {
        table_name: String,
        data_dir: String,
        source: std::io::Error,
    },

    #[snafu(display(
        "Failed to recreate dataset {table_name} (cayenne): Could not delete the acceleration data directory '{data_dir}' ({source}). \
        The dataset is left as it was, so retrying the recreate is safe once the cause is cleared. \
        Check that Spice can write to '{data_dir}' and that nothing else is holding files open in it. \
        See: https://spiceai.org/docs/components/data-accelerators/cayenne"
    ))]
    AccelerationDataDirRemovalFailed {
        table_name: String,
        data_dir: String,
        source: std::io::Error,
    },

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

fn maintained_aggregate_specs_for_cayenne(
    acceleration: Option<&Acceleration>,
    schema: &Schema,
    primary_keys: &[String],
) -> Result<Vec<cayenne::maintained_aggregate::MaintainedAggregateSpec>> {
    let Some(acceleration) = acceleration else {
        return Ok(Vec::new());
    };

    let maintained_aggregates = acceleration.maintained_aggregates.enabled_aggregates();
    if maintained_aggregates.is_empty() {
        return Ok(Vec::new());
    }

    if !acceleration.partition_by.is_empty() {
        return Err(Error::InvalidConfiguration {
            detail: Arc::from(
                "Cayenne maintained_aggregates is not yet supported on partitioned tables. Remove maintained_aggregates or remove partition_by from the acceleration configuration.",
            ),
        });
    }

    let has_min_or_max = maintained_aggregates.iter().any(|aggregate| {
        aggregate.aggregates.iter().any(|expr| {
            matches!(
                expr.function,
                spicepod_acceleration::MaintainedAggregateFunction::Min
                    | spicepod_acceleration::MaintainedAggregateFunction::Max
            )
        })
    });
    if has_min_or_max && primary_keys.is_empty() {
        return Err(Error::InvalidConfiguration {
            detail: Arc::from(
                "Cayenne maintained_aggregates MIN/MAX require a primary key so UPDATE and DELETE changes can retract prior extrema within the retained-index cap. Set acceleration.primary_key, ensure the source table has a primary key that schema inference can read (the connection role needs catalog read access), or remove MIN/MAX from maintained_aggregates.",
            ),
        });
    }

    maintained_aggregates
        .iter()
        .map(|aggregate| {
            // An optional `filter` is the maintained equivalent of a query
            // `WHERE`: it is parsed against the table schema into a physical
            // predicate so the view maintains only the matching rows and the
            // optimizer can serve a query carrying the identical predicate.
            let filter = aggregate
                .filter_sql
                .as_deref()
                .map(|sql| parse_maintained_aggregate_filter(sql, schema))
                .transpose()?;

            let aggregates = aggregate
                .aggregates
                .iter()
                .map(|expr| {
                    let function = match expr.function {
                        spicepod_acceleration::MaintainedAggregateFunction::Count => {
                            cayenne::maintained_aggregate::MaintainedAggregateFunction::Count
                        }
                        spicepod_acceleration::MaintainedAggregateFunction::Sum => {
                            cayenne::maintained_aggregate::MaintainedAggregateFunction::Sum
                        }
                        spicepod_acceleration::MaintainedAggregateFunction::Avg => {
                            cayenne::maintained_aggregate::MaintainedAggregateFunction::Avg
                        }
                        spicepod_acceleration::MaintainedAggregateFunction::Min => {
                            cayenne::maintained_aggregate::MaintainedAggregateFunction::Min
                        }
                        spicepod_acceleration::MaintainedAggregateFunction::Max => {
                            cayenne::maintained_aggregate::MaintainedAggregateFunction::Max
                        }
                    };

                    cayenne::maintained_aggregate::MaintainedAggregateExpr {
                        function,
                        column: expr.column.clone(),
                    }
                })
                .collect();

            Ok(cayenne::maintained_aggregate::MaintainedAggregateSpec {
                filter,
                group_by: aggregate.group_by.clone(),
                aggregates,
            })
        })
        .collect()
}

/// Parse a maintained-aggregate `filter` SQL predicate into a physical
/// expression over the table `schema`. The resulting expression references the
/// table columns by their schema position, so it lines up with the CDC batches
/// the view is maintained from. For serving, the view answers a query only when
/// this predicate is structurally equal to the query's `FilterExec` predicate
/// (see `MaintainedAggregateView::matches_query`): that holds when the query
/// filters the scan output directly, but a projection or type-coercion between
/// the scan and the filter changes the predicate's column indices/literal types
/// and the view silently falls back to a re-scan. Declare the filter to match
/// the predicate the query carries over the scan output.
fn parse_maintained_aggregate_filter(
    sql: &str,
    schema: &Schema,
) -> Result<Arc<dyn datafusion::physical_expr::PhysicalExpr>> {
    use datafusion::common::ToDFSchema;

    let df_schema = schema
        .clone()
        .to_dfschema()
        .map_err(|source| Error::InvalidConfiguration {
            detail: Arc::from(format!(
                "Cayenne maintained_aggregates filter '{sql}' could not bind to the table schema: {source}"
            )),
        })?;
    let context = datafusion::prelude::SessionContext::new();
    let logical = context
        .parse_sql_expr(sql, &df_schema)
        .map_err(|source| Error::InvalidConfiguration {
            detail: Arc::from(format!(
                "Cayenne maintained_aggregates filter '{sql}' is not a valid SQL predicate over the table columns: {source}"
            )),
        })?;
    // Plan through the session rather than calling `create_physical_expr`
    // directly: the session coerces the expression against the schema first, and
    // a filter written the way SQL is normally written needs that. A predicate
    // like `ts_col > '2007-01-02 00:00:00'` parses to a `Timestamp` compared
    // against a `Utf8` literal, which builds a physical expression happily and
    // then fails at evaluation with "Invalid comparison operation:
    // Timestamp(µs) > Utf8" — the maintained aggregate goes stale on its first
    // delta, and every query silently falls back to a base-table scan for the
    // life of the process.
    let physical = context
        .create_physical_expr(logical, &df_schema)
        .map_err(|source| Error::InvalidConfiguration {
            detail: Arc::from(format!(
                "Cayenne maintained_aggregates filter '{sql}' could not be planned: {source}"
            )),
        })?;
    // A filter is a `WHERE` condition, so it must evaluate to Boolean. Reject a
    // non-Boolean predicate (e.g. `filter: 1`) at config time with a clear error,
    // rather than letting it fail later during maintenance.
    let data_type = physical
        .data_type(schema)
        .map_err(|source| Error::InvalidConfiguration {
            detail: Arc::from(format!(
                "Cayenne maintained_aggregates filter '{sql}' could not be type-checked: {source}"
            )),
        })?;
    if data_type != DataType::Boolean {
        return Err(Error::InvalidConfiguration {
            detail: Arc::from(format!(
                "Cayenne maintained_aggregates filter '{sql}' must be a Boolean predicate (a WHERE condition), but it evaluates to {data_type}"
            )),
        });
    }
    Ok(physical)
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
    /// Separate catalog for `mode: memory` (in-RAM) tables, backed by an in-memory
    /// `SQLite` `memdb` metastore. File-mode and memory-mode tables cannot share one
    /// metastore (memory-mode data must never touch disk), so memory tables use this.
    memory_catalog: Arc<OnceCell<Arc<dyn cayenne::MetadataCatalog>>>,
    /// Process-unique id for this accelerator instance, used to name the in-memory
    /// `memdb` metastore so separate instances (e.g. per-test runtimes) never share
    /// one in-memory database.
    instance_id: u64,
    footer_cache_mb: Option<usize>,
    /// The process-wide semaphore that bounds concurrent per-table background
    /// compactions, held here so the registration path can hand it to each
    /// table. Sized at `cpu_budget().cayenne_compaction_permits()` so a fleet of
    /// tables can't oversubscribe the writer pool. Every Cayenne table draws
    /// on this one budget, including those created by `CREATE TABLE …
    /// PARTITIONED BY`, which belong to no accelerator.
    compaction_semaphore: Arc<tokio::sync::Semaphore>,
}

/// `(available, total)` permits of the fleet-wide compaction budget.
///
/// Read straight from the `cayenne` crate's process-global budget rather than
/// from a handle the accelerator publishes, because a `CREATE TABLE …
/// PARTITIONED BY` table draws on that same budget while belonging to no
/// accelerator: keying the gauges off accelerator registration would leave them
/// silent in a process whose only compaction work is DDL-created.
fn compaction_budget_snapshot() -> (u64, u64) {
    (
        cayenne::compaction_budget().available_permits() as u64,
        cayenne::compaction_budget_permits() as u64,
    )
}

/// Register the Cayenne write-path backpressure gauges (pull-based observable
/// gauges on the global `cayenne` meter) so `/metrics` localizes *where* the CDC
/// apply path is stalling: the process-global encode budget, the in-memory CDC
/// tier byte budget, and the fleet-wide compaction semaphore. Each callback reads
/// a cheap live snapshot at Prometheus scrape time — no sampler task, near-zero
/// cost between scrapes.
///
/// Like [`telemetry::cayenne::register_compaction_metrics`], the binary MUST call
/// this once AFTER `init_metrics` has installed the Prometheus meter provider;
/// otherwise the instruments bind to the early noop meter and never export.
pub fn register_cayenne_telemetry() {
    use opentelemetry::global;
    let meter = global::meter("cayenne");

    // Process-wide Vortex segment cache: fill vs capacity, entries, and the
    // access/hit counters. Registered here rather than where the cache is
    // installed, because the cache must exist before any table is registered —
    // well before the real meter provider replaces the startup noop one.
    vortex_datafusion::register_segment_cache_metrics();

    // --- Process-global encode-concurrency budget ---
    let _ = meter
        .u64_observable_gauge("cayenne_encode_permits_available")
        .with_description(
            "Available permits in the process-global Cayenne encode-concurrency budget; 0 under a growing backlog is the encode-semaphore stall signature.",
        )
        .with_unit("{permit}")
        .with_callback(|obs| {
            if let Some(s) = cayenne::encode_budget_snapshot() {
                obs.observe(s.available, &[]);
            }
        })
        .build();
    let _ = meter
        .u64_observable_gauge("cayenne_encode_permits_total")
        .with_description(
            "Total permits (ceiling) of the process-global Cayenne encode-concurrency budget.",
        )
        .with_unit("{permit}")
        .with_callback(|obs| {
            if let Some(s) = cayenne::encode_budget_snapshot() {
                obs.observe(s.total, &[]);
            }
        })
        .build();
    let _ = meter
        .u64_observable_gauge("cayenne_encode_maintenance_gate_available")
        .with_description(
            "Available permits in the reserved maintenance slice of the Cayenne encode budget (compaction/rewrite outputs).",
        )
        .with_unit("{permit}")
        .with_callback(|obs| {
            if let Some(s) = cayenne::encode_budget_snapshot() {
                obs.observe(s.maintenance_gate_available, &[]);
            }
        })
        .build();

    // --- Process-global in-memory CDC tier byte budget (cdc_durability: memory) ---
    let _ = meter
        .u64_observable_gauge("cayenne_mem_tier_budget_used_bytes")
        .with_description(
            "Currently-reserved bytes across all in-memory CDC tiers; approaching the total forces writes to spill/fall back to the durable path.",
        )
        .with_unit("By")
        .with_callback(|obs| {
            if let Some(used) = cayenne::global_mem_tier_used() {
                obs.observe(used, &[]);
            }
        })
        .build();
    let _ = meter
        .u64_observable_gauge("cayenne_mem_tier_budget_total_bytes")
        .with_description("Total byte ceiling of the process-global in-memory CDC tier budget.")
        .with_unit("By")
        .with_callback(|obs| {
            if let Some(total) = cayenne::global_mem_tier_total() {
                obs.observe(total, &[]);
            }
        })
        .build();

    // --- Fleet-wide compaction semaphore ---
    let _ = meter
        .u64_observable_gauge("cayenne_compaction_permits_available")
        .with_description(
            "Available permits in the fleet-wide Cayenne compaction semaphore; 0 means every compaction slot is in use and peers queue.",
        )
        .with_unit("{permit}")
        .with_callback(|obs| {
            let (available, _total) = compaction_budget_snapshot();
            obs.observe(available, &[]);
        })
        .build();
    let _ = meter
        .u64_observable_gauge("cayenne_compaction_permits_total")
        .with_description("Total permits of the fleet-wide Cayenne compaction semaphore.")
        .with_unit("{permit}")
        .with_callback(|obs| {
            let (_available, total) = compaction_budget_snapshot();
            obs.observe(total, &[]);
        })
        .build();
}

impl Default for CayenneAccelerator {
    fn default() -> Self {
        Self::new()
    }
}

/// Parse a goal duration param (e.g. `"5s"`, `"1m"`, `"250ms"`) to seconds, via
/// `fundu` for consistency with the other Spice duration knobs (e.g.
/// `retention_period`). `None` when unset; a warning + `None` when present but
/// unparseable.
/// Resolve a `cayenne_goal_*` setpoint's raw string with global+override
/// semantics: a per-dataset (`acceleration.params`) value OVERRIDES the
/// runtime-level (`runtime.params`) global default. `None` when neither sets it —
/// the legacy "no goal" case, where the controller stays on its signal-driven path.
fn resolve_goal_raw<'a>(
    acceleration: &'a Acceleration,
    runtime_params: &'a std::collections::HashMap<String, String>,
    key: &str,
) -> Option<&'a str> {
    acceleration
        .params
        .get(key)
        .or_else(|| runtime_params.get(key))
        .map(String::as_str)
}

/// Parse a goal duration setpoint (`5s`/`1m`/`250ms`) from its already-resolved
/// raw value. `None` when unset; a warning + `None` when present but unparseable.
/// `source_desc` names where the value applies, for the diagnostic.
fn parse_goal_duration_secs(raw: Option<&str>, key: &str, source_desc: &str) -> Option<f64> {
    let raw = raw?;
    match fundu::parse_duration(raw) {
        Ok(d) => Some(d.as_secs_f64()),
        Err(e) => {
            tracing::warn!(
                target: "spiced::acceleration::cayenne",
                "Invalid '{key}' duration '{raw}' ({source_desc}): {e}; ignoring. Expected a duration like '5s' or '1m'."
            );
            None
        }
    }
}

/// Parse a positive goal float param (e.g. queries-per-hour) from its resolved raw
/// value. `None` when unset; a warning + `None` when present but unparseable or
/// non-positive. `source_desc` names where the value applies, for the diagnostic.
fn parse_goal_f64(raw: Option<&str>, key: &str, source_desc: &str) -> Option<f64> {
    let raw = raw?;
    match raw.parse::<f64>() {
        Ok(v) if v.is_finite() && v > 0.0 => Some(v),
        _ => {
            tracing::warn!(
                target: "spiced::acceleration::cayenne",
                "Invalid '{key}' value '{raw}' ({source_desc}); expected a positive number, ignoring."
            );
            None
        }
    }
}

const SMALL_WRITE_COMPACTION_TRIGGER_FILES: usize = 4;
const SMALL_WRITE_COMPACTION_TRIGGER_PROTECTED_SNAPSHOTS: usize = 4;
const SMALL_WRITE_COMPACTION_TRIGGER_SNAPSHOT_AGE_MS: u64 = 60_000;
const SMALL_WRITE_COMPACTION_BACKGROUND_INTERVAL_MS: u64 = 10_000;
const SMALL_WRITE_INLINE_MAX_ROWS: usize = cayenne::metadata::DEFAULT_INLINE_MAX_ROWS;
const SMALL_WRITE_INLINE_MAX_BYTES: usize = cayenne::metadata::DEFAULT_INLINE_MAX_BYTES;
const SMALL_WRITE_INLINE_MAX_BUFFER_BYTES: usize =
    cayenne::metadata::DEFAULT_INLINE_MAX_BUFFER_BYTES;
/// Longest `refresh_check_interval` at which a scheduled `append` still counts as
/// a small-write (CDC-shaped) stream rather than a bulk load. Shared by both
/// classification entry points ([`refresh_write_profile`] and
/// [`RefreshWriteProfile::from_spicepod`]) so the pre-init and post-init views of a
/// pod agree.
pub(crate) const APPEND_SMALL_WRITE_REFRESH_INTERVAL_THRESHOLD: Duration = Duration::from_mins(5);

/// Map the runtime's detected acceleration storage class onto the Cayenne-local
/// [`cayenne::metadata::StorageClass`] (the crates can't share the enum — `runtime`
/// depends on `cayenne`). The continuous slow-tier bias is refined further by the
/// measured throughput threaded alongside it.
fn to_cayenne_storage_class(
    storage: data_accelerator_api::storage::ResolvedAccelerationStorage,
) -> cayenne::metadata::StorageClass {
    use cayenne::metadata::StorageClass as Class;
    use data_accelerator_api::storage::ResolvedAccelerationStorage as Resolved;
    match storage {
        Resolved::LocalSsd => Class::LocalSsd,
        Resolved::Ebs => Class::Ebs,
        Resolved::Tmpfs => Class::Tmpfs,
        Resolved::Unknown => Class::Unknown,
    }
}

/// Warn (once per canonicalized path) when the filesystem backing `path` is low on
/// free space.
/// Under memory pressure the in-memory CDC tier spills to this volume; if it fills,
/// ingestion fails — so surface it at startup rather than discovering it on a crash.
async fn warn_if_low_disk(label: &str, path: &str) {
    // `disk_space_bytes` canonicalizes + enumerates every mount (blocking OS I/O);
    // run it off the Tokio runtime so it can't stall a worker during concurrent
    // table registration.
    let label = label.to_string();
    let path = path.to_string();
    let _ = tokio::task::spawn_blocking(move || warn_if_low_disk_blocking(&label, &path)).await;
}

fn warn_if_low_disk_blocking(label: &str, path: &str) {
    use std::sync::{LazyLock, Mutex};
    /// Below this fraction free OR this many absolute bytes free ⇒ warn.
    const LOW_DISK_FRACTION_DENOM: u64 = 10; // < 10% free
    const LOW_DISK_FLOOR_BYTES: u64 = 2 * 1024 * 1024 * 1024; // < 2 GiB free
    static CHECKED: LazyLock<Mutex<std::collections::HashSet<std::path::PathBuf>>> =
        LazyLock::new(|| Mutex::new(std::collections::HashSet::new()));

    // Check each distinct canonicalized path once per process, BEFORE probing free
    // space (`disk_space_bytes` re-enumerates every mount). Canonicalizing collapses
    // equivalent paths (trailing slash, symlinks); distinct dirs on the same mount
    // each warn once — keyed by path, not mount point.
    let key = std::path::Path::new(path)
        .canonicalize()
        .unwrap_or_else(|_| std::path::PathBuf::from(path));
    if !CHECKED
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert(key)
    {
        return;
    }
    let Some((available, total)) = data_accelerator_api::storage::disk_space_bytes(path) else {
        return;
    };
    if total == 0
        || (available >= total / LOW_DISK_FRACTION_DENOM && available >= LOW_DISK_FLOOR_BYTES)
    {
        return;
    }
    tracing::warn!(
        label,
        path,
        available_mib = available / (1024 * 1024),
        total_mib = total / (1024 * 1024),
        // saturating_mul so `available * 100` can't overflow u64 on a very large
        // volume; a purely-informational percentage in a low-disk warning.
        percent_free = available.saturating_mul(100) / total,
        "Cayenne {label} volume is low on free space. Under memory pressure the in-memory CDC tier spills here; if it fills, ingestion fails. Free space or point the acceleration at a larger volume."
    );
}

/// Fingerprint of everything the `Cayenne auto-tuned config` line prints for one
/// table, used to emit it once per resolved configuration instead of once per
/// provider construction.
///
/// Hashes the *rendered* values rather than a hand-listed tuple of typed fields:
/// `{config:?}` covers every [`cayenne::metadata::VortexConfig`] knob, so a knob
/// added to the line later cannot silently fall out of the key and start hiding
/// re-tunes. The measured members of `hw` (`data_perf`, `metastore_perf`) are
/// deliberately excluded — they are calibration readings the line does not print;
/// they still reach the fingerprint where they matter, through the knobs they
/// resolved.
fn auto_tuned_config_fingerprint(
    table_name: &str,
    hw: &autotune::HardwareProfile,
    workload: &autotune::WorkloadProfile,
    config: &cayenne::metadata::VortexConfig,
) -> u64 {
    use std::hash::{DefaultHasher, Hash, Hasher};

    let mut hasher = DefaultHasher::new();
    format!(
        "{table_name}|{cores}|{total_mem_bytes}|{data_storage:?}|{metastore_storage:?}|\
         {row_count:?}|{table_bytes:?}|{schema_present}|{has_primary_key}|{is_upsert}|{config:?}",
        cores = hw.cores,
        total_mem_bytes = hw.total_mem_bytes,
        data_storage = hw.data_storage,
        metastore_storage = hw.metastore_storage,
        row_count = workload.row_count,
        table_bytes = workload.table_bytes,
        schema_present = workload.inferred_metadata.is_present(),
        has_primary_key = workload.has_primary_key,
        is_upsert = workload.is_upsert,
    )
    .hash(&mut hasher);
    hasher.finish()
}

/// Whether the `Cayenne auto-tuned config` line for `table_name` should be emitted
/// for this `fingerprint`: true the first time the table is resolved, and again
/// whenever the resolution *changes*.
///
/// Dataset initialization retries with unbounded backoff and rebuilds the table
/// provider on every attempt, so a dataset that never loads would otherwise repeat
/// its ~1 KB config line at fibonacci-spaced intervals for the life of the process,
/// burying the WARN that explains why it is unhealthy. Keying on the resolved values
/// (rather than logging once per table) keeps the two cases worth seeing: the first
/// resolution, and a genuine re-tune — adaptive tuning moving a knob, or a config
/// edit after a hot reload.
fn auto_tuned_config_is_newly_resolved(table_name: &str, fingerprint: u64) -> bool {
    static LOGGED: LazyLock<std::sync::Mutex<HashMap<String, u64>>> =
        LazyLock::new(|| std::sync::Mutex::new(HashMap::new()));

    let mut logged = LOGGED
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if logged.get(table_name) == Some(&fingerprint) {
        return false;
    }
    logged.insert(table_name.to_string(), fingerprint);
    true
}

/// Default read-current freshness (bounded staleness), in ms, for a READ-ONLY Cayenne
/// CDC replica (`access: read` + `refresh_mode: changes`). Such a replica's data
/// streams in only via CDC and is eventually-consistent by design, so a scan need not
/// be read-your-writes; serving a recently-built `ScanView` within this lag lets
/// concurrent analytical scans share one build (the demand cache's reuse lever) while
/// staying far inside the freshness SLO. Every other table uses 0 (read-your-writes):
/// read-write datasets, and read-only NON-CDC tables (full-refresh/snapshot/append),
/// which must reflect their last refresh immediately and can still take a direct
/// `delete_from` via the accelerator. 1 s is a conservative bounded-staleness default,
/// far inside the freshness SLO; tune as the A/B data lands.
const DEFAULT_READ_ONLY_SCAN_FRESHNESS_MS: u64 = 1000;

/// The read-current lag applied to a READ-ONLY Cayenne CDC replica:
/// [`DEFAULT_READ_ONLY_SCAN_FRESHNESS_MS`], overridable via the
/// `CAYENNE_SCAN_VIEW_FRESHNESS_MS` environment variable (a process-wide operational
/// knob, not per-table data config, so it stays out of `configuration_matches`).
/// Setting it to `0` opts CDC replicas back into read-your-writes (the A/B no-reuse
/// baseline). Never affects any other table, which always uses 0.
///
/// Parsed once into a process-global `LazyLock`: the env var is a process-wide knob, so
/// caching avoids re-parsing on every provider/partition construction AND emits the
/// invalid-value warning at most once (rather than per construction).
fn read_only_scan_freshness() -> std::time::Duration {
    static READ_ONLY_SCAN_FRESHNESS: LazyLock<std::time::Duration> = LazyLock::new(|| {
        let ms = match std::env::var("CAYENNE_SCAN_VIEW_FRESHNESS_MS") {
            Err(_) => DEFAULT_READ_ONLY_SCAN_FRESHNESS_MS,
            // A set-but-invalid value is a misconfiguration; warn (don't silently
            // swallow it) before falling back, mirroring `parse_env_u64`. `{raw:?}`
            // escapes control characters so untrusted input cannot inject log lines.
            Ok(raw) => raw.trim().parse::<u64>().unwrap_or_else(|_| {
                tracing::warn!("Ignoring invalid CAYENNE_SCAN_VIEW_FRESHNESS_MS={raw:?}: expected a non-negative integer (milliseconds); using default {DEFAULT_READ_ONLY_SCAN_FRESHNESS_MS} ms.");
                DEFAULT_READ_ONLY_SCAN_FRESHNESS_MS
            }),
        };
        std::time::Duration::from_millis(ms)
    });
    *READ_ONLY_SCAN_FRESHNESS
}

/// How a dataset's refresh mode writes to its Cayenne table. The three shapes want
/// materially different compaction defaults, so they are named rather than inferred
/// at each use site.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RefreshWriteProfile {
    /// A continuous stream of small deltas (`changes` / `caching`, or a
    /// fast-cadence `append`). Files accumulate between refreshes, so compaction is
    /// load-bearing and runs on a tight cadence; small deltas are inlined into the
    /// metastore instead of becoming tiny Vortex files.
    SmallWrite,
    /// Every refresh REPLACES the whole table (`full`). Nothing accumulates across
    /// refreshes — the previous snapshot is dropped wholesale — so compaction has
    /// nothing to consolidate and is off by default; the write should just reach the
    /// target file size as fast as it can.
    BulkOverwrite,
    /// Bulk data arrives incrementally and accumulates (`append` on a slow cadence,
    /// `snapshot`, or refresh `disabled` with writes arriving another way). Files
    /// build up across writes, so compaction stays on at the conservative cadence.
    BulkAppend,
}

impl RefreshWriteProfile {
    /// The single classification. Every caller — pre-init and post-init alike —
    /// funnels through this, so the two views of a pod cannot drift.
    pub(crate) fn classify(
        refresh_mode: RefreshMode,
        refresh_check_interval: Option<Duration>,
    ) -> Self {
        match refresh_mode {
            RefreshMode::Caching | RefreshMode::Changes => Self::SmallWrite,
            RefreshMode::Append => {
                if refresh_check_interval
                    .is_some_and(|i| i <= APPEND_SMALL_WRITE_REFRESH_INTERVAL_THRESHOLD)
                {
                    Self::SmallWrite
                } else {
                    Self::BulkAppend
                }
            }
            RefreshMode::Full => Self::BulkOverwrite,
            // `snapshot` reloads whole snapshot files rather than issuing an
            // overwrite through the sink, and `disabled` means rows arrive by some
            // other write path entirely (e.g. `INSERT INTO` on a read-write
            // dataset). Neither is a whole-table replace this module can prove, so
            // both keep compaction on.
            RefreshMode::Snapshot | RefreshMode::Disabled => Self::BulkAppend,
        }
    }

    /// Classify a Spicepod acceleration, BEFORE component initialization has turned
    /// it into an [`Acceleration`]. The runtime builder uses this to size host memory
    /// and decide which thread pools to bring up.
    ///
    /// Two things are not yet resolved at this point, and the caller supplies one:
    ///
    /// * `unset_refresh_mode` — an absent `refresh_mode` is filled in by the
    ///   *connector* (`DataConnector::resolve_refresh_mode`), not by a fixed
    ///   default: `debezium` and `cdc` resolve it to `changes`, `sink` to
    ///   `disabled`, everything else to `full`. Assuming `full` here would classify
    ///   an unannotated CDC dataset as a whole-table replace and under-provision its
    ///   memory. See
    ///   [`runtime_acceleration::acceleration::unset_refresh_mode_for_connector`].
    /// * `refresh_check_interval` is still an unparsed string. An unparseable one is
    ///   treated as absent — the same conservative direction the component builder
    ///   takes when it later rejects the value, and the same `fundu` parser it uses.
    pub(crate) fn from_spicepod(
        accel: &spicepod::acceleration::Acceleration,
        unset_refresh_mode: RefreshMode,
    ) -> Self {
        let refresh_mode = accel
            .refresh_mode
            .as_ref()
            .map_or(unset_refresh_mode, |mode| RefreshMode::from(mode.clone()));
        let interval = accel
            .refresh_check_interval
            .as_deref()
            .and_then(|raw| fundu::parse_duration(raw).ok());
        Self::classify(refresh_mode, interval)
    }

    /// Whether a table on this profile ever accumulates files for compaction to
    /// consolidate. False only for the whole-table replace, where each refresh
    /// discards everything the previous one wrote.
    pub(crate) const fn needs_compaction(self) -> bool {
        !matches!(self, RefreshWriteProfile::BulkOverwrite)
    }

    /// Whether a table on this profile can hold rows in the off-pool in-memory CDC
    /// tier. `get_vortex_config` forces `cdc_durability` back to `file` for every
    /// other profile, so this is exactly the set that makes the tier reachable.
    pub(crate) const fn uses_cdc_tier(self) -> bool {
        matches!(self, RefreshWriteProfile::SmallWrite)
    }

    /// Whether a table on this profile keeps small writes in the metastore inline
    /// tier instead of turning them into tiny Vortex files.
    ///
    /// True for the two profiles whose writes are small by shape: a CDC-style
    /// stream of deltas, and a whole-table replace of a table small enough to fit
    /// the admission caps. The whole-table replace is the one that most needs it —
    /// nothing accumulates across its refreshes, so its background compactor is
    /// off and the tiny files one refresh writes would never be merged.
    pub(crate) const fn inlines_small_writes(self) -> bool {
        matches!(
            self,
            RefreshWriteProfile::SmallWrite | RefreshWriteProfile::BulkOverwrite
        )
    }
}

fn apply_refresh_mode_defaults(
    config: &mut cayenne::metadata::VortexConfig,
    source: &dyn AccelerationSource,
    acceleration: &Acceleration,
    inline_flush_caps: autotune::InlineFlushCaps,
) {
    match refresh_write_profile(source, acceleration) {
        RefreshWriteProfile::SmallWrite => {
            config.compaction_trigger_files = SMALL_WRITE_COMPACTION_TRIGGER_FILES;
            config.compaction_trigger_protected_snapshots =
                SMALL_WRITE_COMPACTION_TRIGGER_PROTECTED_SNAPSHOTS;
            config.compaction_trigger_snapshot_age_ms =
                SMALL_WRITE_COMPACTION_TRIGGER_SNAPSHOT_AGE_MS;
            config.compaction_background_interval_ms =
                SMALL_WRITE_COMPACTION_BACKGROUND_INTERVAL_MS;
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
        }
        RefreshWriteProfile::BulkOverwrite => {
            // Nothing survives a whole-table replace for compaction to consolidate:
            // the overwrite publishes a fresh snapshot, `update_current_snapshot_id`
            // resets the small-file counter, and no protected snapshot is ever
            // created. So the background compactor would wake forever only to re-run
            // its early-outs. Turn it off and let the refresh spend the whole machine
            // on ingest instead. Inline compaction on writes is unaffected, so a
            // table that also takes `INSERT`s still consolidates; an operator can
            // also set `cayenne_compaction_background_interval_ms` explicitly.
            config.compaction_background_interval_ms = 0;
            // Same static admission caps as a CDC delta, for the same reason and
            // then some: a refresh that fits them becomes ONE metastore row
            // instead of `write_concurrency` tiny Vortex files that — with the
            // background compactor off, above — nothing would ever merge. The
            // `inline_flush_*` memtable caps are deliberately left at their
            // defaults: a whole-table replace leaves exactly one inline entry, so
            // the flush caps never bind.
            config.inline_max_rows = SMALL_WRITE_INLINE_MAX_ROWS;
            config.inline_max_bytes = SMALL_WRITE_INLINE_MAX_BYTES;
            config.inline_max_buffer_bytes = SMALL_WRITE_INLINE_MAX_BUFFER_BYTES;
        }
        RefreshWriteProfile::BulkAppend => {
            config.inline_max_rows = 0;
            config.inline_max_bytes = 0;
            config.inline_max_buffer_bytes = 0;
        }
    }
}

/// Classify a post-initialization [`Acceleration`], using `resolved_refresh_mode`
/// for the connector-filled value rather than reading `acceleration.refresh_mode`
/// raw.
///
/// That distinction is load-bearing: `DataConnector::resolve_refresh_mode` is never
/// written back into the `Acceleration`, so an unannotated `debezium:`/`cdc:`
/// dataset is a real `changes` stream while its `refresh_mode` field is still
/// `None`. Reading the field raw would classify it as a whole-table replace and
/// switch off its background compactor.
fn refresh_write_profile_for(
    acceleration: &Acceleration,
    resolved_refresh_mode: RefreshMode,
) -> RefreshWriteProfile {
    RefreshWriteProfile::classify(resolved_refresh_mode, acceleration.refresh_check_interval)
}

fn refresh_write_profile(
    source: &dyn AccelerationSource,
    acceleration: &Acceleration,
) -> RefreshWriteProfile {
    refresh_write_profile_for(acceleration, resolved_refresh_mode(source, acceleration))
}

/// Whether the dataset writes small batches continuously, which is what the
/// CDC-shaped defaults (inline memtable, aggressive compaction triggers,
/// `cdc_durability: memory`) are for. It is also exactly the set of tables that can
/// reach the off-pool in-memory CDC tier, so the runtime builder gates the tier's
/// aggregate byte budget — and the reduced query-pool default that leaves room for
/// it — on a pod containing one (see `builder::CayenneWorkload`).
fn uses_small_write_refresh_profile(
    source: &dyn AccelerationSource,
    acceleration: &Acceleration,
) -> bool {
    refresh_write_profile(source, acceleration).uses_cdc_tier()
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
/// mode, its resolved primary keys / `on_conflict`, and any schema-inference
/// metadata carried on the Arrow schema (`spice.inferred_row_count` /
/// `spice.inferred_table_bytes`, see `data_components::inferred_schema`). Every
/// signal degrades gracefully: an unknown one falls back to the hardware-only
/// derivation.
fn build_workload_profile(
    source: &dyn AccelerationSource,
    acceleration: Option<&Acceleration>,
    schema: &Schema,
    primary_keys: &[String],
    on_conflict: Option<&datafusion_table_providers::util::on_conflict::OnConflict>,
) -> autotune::WorkloadProfile {
    let small_write = acceleration
        .is_some_and(|acceleration| uses_small_write_refresh_profile(source, acceleration));
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

/// Make a configured Cayenne directory absolute without resolving it, treating it as a
/// filesystem path unconditionally.
///
/// `Err` when the path cannot be placed — a relative path whose `current_dir()` lookup
/// fails. Everything downstream guards `remove_dir_all`, so a path this cannot place is
/// a path whose overlap with the metastore is unknown, and the caller must refuse rather
/// than assume.
fn absolute_dir(path: &str) -> std::io::Result<PathBuf> {
    let raw = Path::new(fs_probe_path(path));
    if raw.is_absolute() {
        Ok(raw.to_path_buf())
    } else {
        Ok(std::env::current_dir()?.join(raw))
    }
}

/// Make a configured Cayenne *data* directory absolute, or `Ok(None)` when it is an
/// object-store location (`s3://…`) — which can never contain the metastore, since
/// `SQLite`/Turso cannot run on object storage.
///
/// The exemption belongs to the data path alone, because it is the data path a recursive
/// delete walks. It must not be applied to a metadata path: [`is_local_path`] is a
/// substring test, so a value merely *containing* `://` would be exempted while the
/// catalog code goes on treating it as the filesystem path it creates `cayenne.db` at —
/// disabling the guard on a directory that never reached an object store.
///
/// `Err`, never the exemption, when the path cannot be placed: the exemption waves the
/// delete through, so "cannot possibly overlap" and "cannot tell" must stay
/// distinguishable.
fn absolute_data_dir(path: &str) -> std::io::Result<Option<PathBuf>> {
    if !is_local_path(path) {
        return Ok(None);
    }
    absolute_dir(path).map(Some)
}

/// Resolve `absolute` component by component, in the order the filesystem would.
///
/// The order is the whole point: `..` names the parent of the directory the preceding
/// component *resolves to*, not its lexical parent. Collapsing `..` up front and
/// canonicalizing afterwards gets this backwards — with `link -> /data/subdir`,
/// `link/../catalog` is `/data/catalog`, but a lexical collapse yields `/catalog` and a
/// containment check against `/data` then passes something it must refuse. Resolving in
/// order keeps the accumulated path symlink-free, so `..` may simply pop it.
///
/// A component that does not exist yet resolves to itself — neither directory
/// necessarily exists when this runs at open time. That is the *only* `canonicalize`
/// failure this absorbs. Any other one (`PermissionDenied`, a transient filesystem
/// error) means the component could not be resolved, so a symlink may still be
/// unresolved and the containment check would run against a path the delete never walks;
/// those propagate, so the caller refuses the delete instead of comparing a lexical
/// path.
async fn resolve_in_filesystem_order(absolute: &Path) -> std::io::Result<PathBuf> {
    let mut resolved = PathBuf::new();
    for component in absolute.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                resolved.pop();
            }
            Component::Prefix(_) | Component::RootDir => resolved.push(component),
            Component::Normal(name) => {
                resolved.push(name);
                match tokio::fs::canonicalize(&resolved).await {
                    Ok(real) => resolved = real,
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error),
                }
            }
        }
    }
    Ok(resolved)
}

/// Every location a recursive delete of `path` could reach, or `Err` when the path
/// cannot be resolved.
///
/// There is no object-store exemption here: this resolves a *metastore* directory, and
/// the metastore is only ever local — see [`absolute_data_dir`] for why applying the
/// exemption to this side disables the guard rather than skipping an impossible case.
///
/// Two forms, because a symlink is both a place and a name:
///
/// 1. **Fully resolved** — where the directory's contents actually live.
/// 2. **The entry**: parent resolved, final component left literal. `remove_dir_all`
///    unlinks the *entry* it walks onto rather than following it, so a metastore
///    directory whose own last component is a symlink pointing out of the tree still
///    loses its link — the catalog file survives with nothing naming it, and the
///    connection pool keeps writing through handles nothing can reopen.
async fn overlap_candidates(path: &str) -> std::io::Result<Vec<PathBuf>> {
    let absolute = absolute_dir(path)?;

    let mut candidates = vec![resolve_in_filesystem_order(&absolute).await?];
    if let (Some(parent), Some(name)) = (absolute.parent(), absolute.file_name()) {
        let entry = resolve_in_filesystem_order(parent).await?.join(name);
        if !candidates.contains(&entry) {
            candidates.push(entry);
        }
    }
    Ok(candidates)
}

/// `true` when `inner` is `outer` itself or lies beneath it — i.e. a recursive delete
/// of `outer` takes `inner` with it. Compares whole components, so `…/meta` does not
/// read as containing `…/metadata`.
fn dir_contains(outer: &Path, inner: &Path) -> bool {
    inner.starts_with(outer)
}

/// Detect the configuration in which a Cayenne recreate destroys the metastore.
///
/// One metastore holds the catalog — manifests, snapshot pointers, partition rows —
/// for *every* Cayenne dataset sharing a `cayenne_metadata_dir`, and both recreate
/// paths (`mode: file_create` in [`CayenneAccelerator::init`] and
/// [`DataAccelerator::drop_table`] for a `file_update` schema rebuild) recursively
/// delete a single dataset's data directory. When the metastore directory resolves
/// onto or beneath that data directory the delete unlinks the shared catalog, and
/// because the connection pool already holds handles to the now-unlinked file the run
/// appears healthy while the metastore is simply gone on the next restart.
///
/// The stock defaults collide on their own for a dataset named `metadata`:
/// `resolve_default_data_path` yields `{spice_data}/metadata/` and
/// `resolve_metadata_dir` yields `{spice_data}/metadata`. An explicit
/// `cayenne_metadata_dir` set beneath the data directory collides the same way.
///
/// Returns `Ok(Some((data_dir, metadata_dir)))` — resolved — when they overlap, naming
/// whichever metastore location the delete would reach; `Ok(None)` when they provably
/// cannot overlap — the data path is on object storage; and `Err` when either path
/// cannot be resolved, which the caller must treat as a refusal rather than as `Ok(None)`.
///
/// The data directory is compared in its fully resolved form only, because that is where
/// the recursive walk happens: `remove_dir_all` unlinks a final-component symlink rather
/// than descending it (pinned by
/// `remove_dir_all_unlinks_a_symlink_rather_than_descending_it`), so nothing beneath the
/// target is deleted. What the unlink does cost is every *name* under the alias, and a
/// metastore configured through one is not compared here — #13465.
async fn overlapping_metastore_dir(
    data_dir: &str,
    metadata_dir: &str,
) -> std::io::Result<Option<(PathBuf, PathBuf)>> {
    let Some(absolute_data) = absolute_data_dir(data_dir)? else {
        return Ok(None);
    };
    let data = resolve_in_filesystem_order(&absolute_data).await?;
    Ok(overlap_candidates(metadata_dir)
        .await?
        .into_iter()
        .find(|candidate| dir_contains(&data, candidate))
        .map(|metadata| (data, metadata)))
}

/// The `SQLite`/Turso database a Cayenne metastore lives in, inside its metadata
/// directory. Every metastore connection string in this file ends in this name.
const METASTORE_DB_FILE: &str = "cayenne.db";

/// True for the metastore database or one of the sidecars `SQLite` keeps beside it
/// (`-wal`, `-shm`, `-journal`). Any of them means a catalog lives in this directory:
/// the sidecars can hold committed transactions the database file does not yet, so
/// finding one is finding a metastore even if the `.db` itself is elsewhere or absent.
///
/// The suffix must start with `-`, so a dataset directory that happens to be named
/// `cayenne.db.backup` is not mistaken for one.
fn is_metastore_file(file_name: &std::ffi::OsStr) -> bool {
    match file_name.to_string_lossy().strip_prefix(METASTORE_DB_FILE) {
        Some(sidecar) => sidecar.is_empty() || sidecar.starts_with('-'),
        None => false,
    }
}

/// The path of a Cayenne metastore living anywhere under `data_dir`, if there is one.
///
/// [`CayenneAccelerator::ensure_metastore_outside_data_dir`] can only reason about the
/// metastore this dataset's own params *name*. A metastore belonging to another dataset
/// is in no part of that answer: with `cayenne_file_path: /x` for dataset `orders` and
/// `cayenne_metadata_dir: /x/orders/catalog` for another, `orders`'s check compares its
/// data directory against its own sibling metastore, finds no overlap, and the recreate
/// unlinks a catalog holding every Cayenne dataset in the instance. Reading the
/// directory needs no such knowledge: whatever is about to be unlinked is right there to
/// be found, including a metastore left by a configuration nothing names any more.
///
/// The walk itself does not descend symlinks, matching `remove_dir_all`, which unlinks a
/// link rather than walking it — but a directory link is *examined* rather than skipped.
/// The file it points at survives the teardown; the name does not, and a dataset
/// configured with `cayenne_metadata_dir` set to that name gets a fresh empty directory
/// where the link was and opens an empty catalog inside it, leaving every manifest on
/// disk with nothing able to reach it. Nothing on this path can tell an alias somebody
/// configured from an incidental one, so a link whose target directly holds a catalog
/// refuses the teardown: a false refusal is loud and an operator can move the link, while
/// the orphaning is silent and permanent. A `data_dir` that is *itself* a link is
/// dereferenced once and then walked, for the same reason and with the same rule.
///
/// The examination is one level deep by design: an alias points *at* a metadata directory,
/// so the catalog is directly inside it, and following further would have to guard against
/// link cycles. A link to some ancestor of a metadata directory is out of reach here and is
/// the by-name half's to answer — #13465.
///
/// The walk is linear in the entries under `data_dir` — the work the `remove_dir_all`
/// immediately after it was going to do regardless. A missing `data_dir` is not an error
/// here, so the removal is left to report it.
async fn metastore_file_under(data_dir: &Path) -> std::io::Result<Option<PathBuf>> {
    let root = match tokio::fs::symlink_metadata(data_dir).await {
        // A `data_dir` that is itself a link is dereferenced once and then walked. The
        // teardown unlinks the link and the caller recreates a real, empty directory in
        // its place, so *every* name under the alias dies even though the files behind it
        // live — which is the same loss as a catalog directly inside the directory, and
        // is refused the same way.
        Ok(metadata) if metadata.is_symlink() => match tokio::fs::canonicalize(data_dir).await {
            Ok(resolved) if resolved.is_dir() => resolved,
            // Dangling, or naming a file: no catalog is reachable through it.
            Ok(_) => return Ok(None),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        },
        Ok(_) => data_dir.to_path_buf(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };

    let mut pending = vec![root];
    while let Some(dir) = pending.pop() {
        let mut entries = match tokio::fs::read_dir(&dir).await {
            Ok(entries) => entries,
            // Something else removed it mid-walk; it holds no catalog to protect once
            // it is gone. Every other error propagates — a directory this cannot
            // inspect must never be deleted on the assumption it was safe.
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error),
        };

        while let Some(entry) = entries.next_entry().await? {
            // Reports on the entry itself, so a link to a directory is a link here and
            // is never pushed onto the walk.
            let file_type = match entry.file_type().await {
                Ok(file_type) => file_type,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error),
            };

            if file_type.is_symlink() {
                if let Some(aliased) = catalog_directly_inside(&entry.path()).await? {
                    return Ok(Some(aliased));
                }
            } else if file_type.is_dir() {
                pending.push(entry.path());
            } else if is_metastore_file(&entry.file_name()) {
                return Ok(Some(entry.path()));
            }
        }
    }

    Ok(None)
}

/// The metastore file directly inside the directory `link` resolves to, if `link` resolves
/// to a directory holding one.
///
/// Deliberately not recursive and deliberately not part of the walk in
/// [`metastore_file_under`]: this answers only "is this link an alias for a metadata
/// directory", the shape that costs a catalog its name when the link is unlinked. A link
/// resolving to a file, to nothing, or to a directory with no catalog in it is not one.
async fn catalog_directly_inside(link: &Path) -> std::io::Result<Option<PathBuf>> {
    match tokio::fs::metadata(link).await {
        Ok(metadata) if metadata.is_dir() => {}
        // A dangling link, or one naming a file, aliases no metadata directory.
        Ok(_) => return Ok(None),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    }

    let mut entries = match tokio::fs::read_dir(link).await {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    while let Some(entry) = entries.next_entry().await? {
        if is_metastore_file(&entry.file_name()) {
            return Ok(Some(entry.path()));
        }
    }
    Ok(None)
}

/// Process-wide counter giving each [`CayenneAccelerator`] instance a unique id,
/// used to name its in-memory (`memdb`) metastore so distinct instances never
/// share one in-memory database.
static CAYENNE_ACCELERATOR_INSTANCE_COUNTER: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

impl CayenneAccelerator {
    /// Builds the engine with the footer-cache size the runtime published.
    ///
    /// This is the constructor the registration slice calls, and it takes no arguments,
    /// which is why the setting arrives through
    /// [`runtime_acceleration::memory_budget::publish_cayenne_footer_cache_mb`] rather
    /// than as a parameter.
    #[must_use]
    pub fn new() -> Self {
        Self::with_footer_cache_mb(runtime_acceleration::memory_budget::cayenne_footer_cache_mb())
    }

    #[must_use]
    pub fn with_footer_cache_mb(footer_cache_mb: Option<usize>) -> Self {
        Self {
            catalog: Arc::new(OnceCell::new()),
            memory_catalog: Arc::new(OnceCell::new()),
            instance_id: CAYENNE_ACCELERATOR_INSTANCE_COUNTER
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            footer_cache_mb,
            compaction_semaphore: cayenne::compaction_budget(),
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
    /// # Errors
    ///
    /// Returns [`Error::AccelerationNotEnabled`] when the source declares no acceleration,
    /// and [`Error::InvalidConfiguration`] when it is not file-accelerated — a memory-mode
    /// table has no data directory to name.
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

    /// Refuse a configuration whose recreate would delete the shared metastore — see
    /// [`overlapping_metastore_dir`] for the shape and how the defaults reach it.
    ///
    /// Called at open time, where the operator can still fix the spicepod, and again
    /// immediately before each recursive delete: at open time neither directory need
    /// exist yet, so the resolution falls back to a lexical one, and an overlap that
    /// only a symlink reveals appears once the directories are real.
    ///
    /// A path that cannot be placed refuses the recreate. The guard's whole job is to
    /// prove the delete cannot reach the metastore, and it cannot prove that about a
    /// path it failed to resolve.
    async fn ensure_metastore_outside_data_dir(
        source: &dyn AccelerationSource,
        data_dir: &str,
    ) -> Result<()> {
        let metadata_dir = Self::resolve_metadata_dir(source.acceleration());
        let overlap = overlapping_metastore_dir(data_dir, &metadata_dir)
            .await
            .map_err(|source_error| Error::CayenneDirsUnresolvable {
                table_name: source.name().to_string(),
                data_dir: data_dir.to_string(),
                metadata_dir: metadata_dir.clone(),
                source: source_error,
            })?;
        if let Some((data, metadata)) = overlap {
            return Err(Error::MetastoreInsideDataDir {
                table_name: source.name().to_string(),
                data_dir: data.to_string_lossy().into_owned(),
                metadata_dir: metadata.to_string_lossy().into_owned(),
            });
        }
        Ok(())
    }

    /// Delete an acceleration's data directory, having first proved it holds no Cayenne
    /// metastore.
    ///
    /// Every teardown goes through here rather than calling `remove_dir_all` beside its
    /// own copy of the proof, so a teardown path added later cannot forget one half of
    /// it. Both halves are needed and neither implies the other:
    ///
    /// - [`Self::ensure_metastore_outside_data_dir`] answers for the metastore this
    ///   dataset's params name, including one that only exists once Cayenne creates it,
    ///   and including one reached through a symlink it would recreate.
    /// - [`metastore_file_under`] answers for a metastore that is on disk under the
    ///   directory whoever configured it — another dataset, or a configuration nothing
    ///   names any more. Nothing on the params path can see those.
    async fn remove_acceleration_data_dir(
        source: &dyn AccelerationSource,
        data_dir: &str,
    ) -> Result<()> {
        Self::ensure_metastore_outside_data_dir(source, data_dir).await?;
        Self::ensure_no_catalog_under_data_dir(source, data_dir).await?;

        tokio::fs::remove_dir_all(data_dir)
            .await
            .map_err(|source_error| Error::AccelerationDataDirRemovalFailed {
                table_name: source.name().to_string(),
                data_dir: data_dir.to_string(),
                source: source_error,
            })?;
        Ok(())
    }

    /// Refuse when a Cayenne catalog is on disk under `data_dir`, whoever configured it.
    ///
    /// Called twice on every teardown, and both calls are load-bearing. At the delete it
    /// is the proof [`Self::remove_acceleration_data_dir`] rests on. *Before* the
    /// teardown's catalog mutations it is a preflight, because both teardown paths drop
    /// this dataset's rows from the metastore before they reach the directory: a refusal
    /// raised only at the delete would leave the rows gone and the files still there —
    /// the half-torn-down state the ordering comment at each call site exists to prevent.
    /// The preflight cannot make the delete-time call redundant, since the directory can
    /// change in between (#13109), and the delete-time call cannot make the preflight
    /// redundant, since by then the catalog rows are already gone.
    ///
    /// Reads `data_dir` exactly as written, with neither the object-store exemption nor
    /// the `file:` stripping [`overlapping_metastore_dir`] applies. Both would make the
    /// proof describe a different tree from the one that gets deleted: `is_local_path` is
    /// a substring test, so a local directory whose name merely contains `://` would be
    /// waved through while `remove_dir_all` still walked it; and `remove_dir_all` — like
    /// the `exists()` test each caller gates on — is handed the string itself. A path that
    /// is genuinely remote is simply absent from the filesystem, and the walk answers
    /// `None` for it at the cost of one `stat`.
    async fn ensure_no_catalog_under_data_dir(
        source: &dyn AccelerationSource,
        data_dir: &str,
    ) -> Result<()> {
        let found = metastore_file_under(Path::new(data_dir))
            .await
            .map_err(|source_error| Error::MetastoreScanFailed {
                table_name: source.name().to_string(),
                data_dir: data_dir.to_string(),
                source: source_error,
            })?;
        if let Some(metastore_path) = found {
            return Err(Error::MetastoreFileInsideDataDir {
                table_name: source.name().to_string(),
                data_dir: data_dir.to_string(),
                metastore_path: metastore_path.to_string_lossy().into_owned(),
            });
        }
        Ok(())
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
    ) -> Result<cayenne::metadata::VortexConfig> {
        let small_write = source
            .acceleration()
            .is_some_and(|acceleration| uses_small_write_refresh_profile(source, acceleration));
        let workload = autotune::WorkloadProfile::hardware_only(small_write);
        Self::get_vortex_config_with_footer_cache(table_name, source, None, &workload).await
    }

    async fn get_vortex_config_with_footer_cache(
        table_name: &str,
        source: &dyn AccelerationSource,
        footer_cache_mb: Option<usize>,
        workload: &autotune::WorkloadProfile,
    ) -> Result<cayenne::metadata::VortexConfig> {
        let mut config = cayenne::metadata::VortexConfig {
            footer_cache_mb,
            // Default the query/scan path to native Arrow types (Utf8/Binary).
            force_view_read_schema: false,
            ..Default::default()
        };
        if let Some(acceleration) = source.acceleration()
            && let Some(v) = acceleration.params.get("cayenne_force_view_types")
        {
            // Any value other than `false` enables view types
            config.force_view_read_schema = !v.trim().eq_ignore_ascii_case("false");
        }
        if let Some(acceleration) = source.acceleration()
            && let Some(v) = acceleration.params.get("cayenne_integrity_checksums")
        {
            // Opt in to end-to-end WAL/data-file integrity checksums. Any value
            // other than `false` enables the feature; unset keeps it off.
            config.integrity_checksums = !v.trim().eq_ignore_ascii_case("false");
        }

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
            let small_write = uses_small_write_refresh_profile(source, acceleration);

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

            // Thread the detected storage medium + measured throughput onto the
            // Cayenne config so the closed-loop tuner reasons over them. Previously
            // these were left at the `StorageClass::Unknown` default (never wired
            // from `hw`), so the loop applied the EBS slow-tier bias even on local
            // NVMe; mapping the real class fixes that, and the measured throughput
            // refines the class into a *continuous* bias (see `tuning::tier_scale`).
            config.data_storage_class = to_cayenne_storage_class(hw.data_storage);
            config.metastore_storage_class = to_cayenne_storage_class(hw.metastore_storage);
            config.data_storage_write_mbps = hw.data_perf.write_mbps;
            config.metastore_storage_write_mbps = hw.metastore_perf.write_mbps;

            // Bound the process-global encode budget by the instance EBS write
            // bandwidth: a single EBS volume is a shared, bandwidth-bounded pipe, so
            // many parallel uploads to it just fan out small files without adding
            // throughput (the regression that made a lag-violated EBS table keep
            // ADDING write shards). Only ever lowers the budget; local NVMe /
            // instance store propose no cap.
            if let Some(cap) = hw.ebs_upload_concurrency_cap() {
                cayenne::cap_global_encode_concurrency(cap);
            }
            // T-family burstable CPU (IMDS): the tuner withholds CPU-stealing moves
            // at a lower busy-fraction, since CPU credits deplete under sustained
            // load and throttle the vCPUs to a low baseline.
            cayenne::set_cpu_burstable(hw.burstable);

            // Low-disk startup warning: a full data/spill volume turns a
            // memory-pressure spill into a crash. Best-effort, once per path.
            if let Some(dir) = data_dir.as_deref() {
                warn_if_low_disk("data", fs_probe_path(dir)).await;
            }
            warn_if_low_disk("metastore", fs_probe_path(&metadata_dir)).await;

            // Storage-aware target Vortex file size on local disk (the `auto`
            // baseline): smaller files reduce write amplification on EBS-class
            // network storage; larger files improve scan throughput on RAM-backed
            // mounts. On S3, where objects are immutable and billed per request
            // (no fsync), a larger default cuts object count and per-request cost.
            // An explicit operator value (or `auto`) is then applied on top.
            if !is_s3 && let Some(size_mb) = hw.target_file_size_mb_override() {
                config.target_vortex_file_size_mb = size_mb;
            } else if is_s3 {
                // S3 favors large immutable objects: default to 512 MiB (2× the
                // local default) when the operator hasn't set a size.
                config.target_vortex_file_size_mb = config.target_vortex_file_size_mb.max(512);
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
            apply_refresh_mode_defaults(&mut config, source, acceleration, inline_flush_caps);

            // In-RAM CDC tier caps (`cdc_durability: memory`) scale with host
            // memory only — see `autotune::HardwareProfile::mem_tier_caps`.
            // Derived for the small-write/CDC profile (memory mode is forced
            // back to `file` for every other profile below, where these knobs
            // are inert); explicit operator params still override in the
            // param-resolution pass below. The age cap and checkpoint interval
            // deliberately keep their static defaults: they are time-domain
            // durability bounds (crash-replay window / slot-ack cadence), not
            // hardware-capacity quantities.
            if small_write {
                let tier_caps = hw.mem_tier_caps();
                config.cdc_mem_tier_max_bytes = tier_caps.max_bytes;
                config.cdc_mem_tier_min_flush_bytes = tier_caps.min_flush_bytes;
            }

            // Vortex segment cache: one cache serves every table, so its budget is
            // set once at the runtime level and a per-table value has nothing to
            // size. This memory-aware default only reaches a process with no
            // installed cache (an embedded host that skips the runtime builder).
            config.segment_cache_mb = hw.segment_cache_mb();
            // Report on the key being *present*, not on it parsing: `read_knob`
            // folds `auto` and malformed values alike into `Knob::Auto`, so
            // matching on `Set` would leave those operators unaware their setting
            // no longer does anything.
            if let Some(requested_mb) = acceleration.params.get("cayenne_segment_cache_mb") {
                tracing::warn!(
                    "Dataset {table_name}: acceleration.params.cayenne_segment_cache_mb={requested_mb} is ignored. The Vortex segment cache is now a single budget shared by every table instead of one cache per table, so a per-table size has nothing to size. To control it, set runtime.params.cayenne_segment_cache_mb (in MB; 0 disables caching). See: https://spiceai.org/docs/components/data-accelerators/cayenne"
                );
            }

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

            // Intra-file scan concurrency: `auto` derives it from target
            // partitions and the planned file count, `off` decodes serially, an
            // explicit count pins it. The scan charges the query pool for every
            // concurrent split, so this is the lever for trading resident decode
            // memory against scan throughput without moving the whole query
            // fan-out via `runtime.query.target_partitions`.
            //
            // A bad value FAILS the dataset rather than warning and continuing.
            // The siblings below fall back to their defaults, but this one bounds
            // memory: falling back would resolve a value meant to REDUCE the
            // fan-out into `auto`, the widest one, and surface as pool exhaustion
            // under load rather than as the typo it is.
            if let Some(concurrency_str) = acceleration.params.get("cayenne_scan_concurrency") {
                config.scan_concurrency = concurrency_str
                    .parse::<cayenne::metadata::ScanConcurrency>()
                    .ok()
                    .context(InvalidScanConcurrencySnafu {
                        table_name,
                        value: concurrency_str,
                    })?;
            }

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

            // In-memory CDC tier PK-hash shard count (intra-apply fan-out on the
            // `cdc_durability: memory` path). Default 1 = the unchanged serial
            // path; N>1 partitions the PK space into N independent serial
            // domains. Clamped to >=1 in `Table::new_internal`.
            config.cdc_mem_tier_shards = autotune::auto_or_usize(
                acceleration,
                &["cayenne_cdc_mem_tier_shards", "cdc_mem_tier_shards"],
                config.cdc_mem_tier_shards,
            )
            .max(1);

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

            // Auto-resolve the deletion mode for delete-receiving CDC tables:
            // under `refresh_mode: changes`, position mode is pathological when
            // DELETE events arrive continuously — position-delete compaction
            // must serialize with writers (`try_lock`), so a continuously
            // written table starves its own compaction and file count grows
            // unboundedly, while key mode compacts concurrently with writers
            // and rides the in-memory CDC tier. A schema-time auto cannot know
            // the delete rate, so prefer the mode that degrades gracefully in
            // both cases. Gated strictly: only an unresolved `auto`, only the
            // changes/CDC refresh mode, and only when a primary key exists
            // (key mode requires one) — explicit `position`/`key` configs and
            // every non-CDC profile keep today's position resolution
            // (merge-on-read pushdown, zero per-row scan CPU).
            if config.deletion_mode == cayenne::metadata::DeletionMode::Auto
                && acceleration.refresh_mode == Some(RefreshMode::Changes)
                && workload.has_primary_key
            {
                config.deletion_mode = cayenne::metadata::DeletionMode::Key;
                tracing::debug!(
                    "Dataset '{table_name}': auto-resolved cayenne_deletion_mode to 'key' (CDC refresh mode with a primary key). Key-based deletes compact concurrently with writers; set `cayenne_deletion_mode: position` to opt back into merge-on-read position deletes."
                );
            }

            // CDC durability mode (file | memory). Memory mode appends CDC
            // batches to an in-RAM tier and defers the source slot ack to a
            // checkpoint; it is only meaningful for the small-write/CDC
            // profile, so it is forced back to `file` for other profiles
            // below. Memory is the DEFAULT (A/B-validated faster than `file`
            // on the CDC profile end-to-end: analytical QPH, replication lag,
            // and disk footprint, at identical convergence); `file` remains
            // the explicit conservative opt-out.
            let mut cdc_durability_explicit = false;
            if let Some((key, value)) = ["cayenne_cdc_durability", "cdc_durability"]
                .iter()
                .find_map(|key| acceleration.params.get(*key).map(|value| (*key, value)))
            {
                if let Some(mode) = cayenne::metadata::CdcDurability::parse(value) {
                    config.cdc_durability = mode;
                    cdc_durability_explicit = true;
                } else {
                    tracing::warn!(
                        "Dataset '{table_name}' contains an invalid `{key}` value: '{value}'. Expected one of: file, memory. Using the default (memory, eligibility-gated)."
                    );
                }
            }
            if config.cdc_durability.is_memory()
                && !uses_small_write_refresh_profile(source, acceleration)
            {
                // Warn only when memory was explicitly requested: memory is
                // the DEFAULT now, so every full/snapshot-profile dataset
                // lands here by design and silently keeps the durable path.
                if cdc_durability_explicit {
                    tracing::warn!(
                        "Dataset '{table_name}' set `cayenne_cdc_durability: memory` but is not using the small-write/CDC refresh profile (refresh_mode: changes/caching, or append with refresh_check_interval <= 5m). In-memory CDC durability only applies to that profile; using `file`."
                    );
                }
                config.cdc_durability = cayenne::metadata::CdcDurability::File;
            }
            config.cdc_mem_tier_max_bytes = autotune::auto_or_i64(
                acceleration,
                &["cayenne_cdc_mem_tier_max_bytes", "cdc_mem_tier_max_bytes"],
                config.cdc_mem_tier_max_bytes,
            );
            config.cdc_mem_tier_max_age_ms = autotune::auto_or_u64(
                acceleration,
                &["cayenne_cdc_mem_tier_max_age_ms", "cdc_mem_tier_max_age_ms"],
                config.cdc_mem_tier_max_age_ms,
            );
            config.cdc_mem_tier_min_flush_bytes = autotune::auto_or_i64(
                acceleration,
                &[
                    "cayenne_cdc_mem_tier_min_flush_bytes",
                    "cdc_mem_tier_min_flush_bytes",
                ],
                config.cdc_mem_tier_min_flush_bytes,
            );
            config.cdc_mem_tier_checkpoint_interval_ms = autotune::auto_or_u64(
                acceleration,
                &[
                    "cayenne_cdc_mem_tier_checkpoint_interval_ms",
                    "cdc_mem_tier_checkpoint_interval_ms",
                ],
                config.cdc_mem_tier_checkpoint_interval_ms,
            );
            config.cdc_mem_tier_seal_age_ms = autotune::auto_or_u64(
                acceleration,
                &[
                    "cayenne_cdc_mem_tier_seal_age_ms",
                    "cdc_mem_tier_seal_age_ms",
                ],
                config.cdc_mem_tier_seal_age_ms,
            );

            // Widening schema evolution at table open is gated on the source's
            // `on_schema_change` policy. The policy is a source-level fact rather
            // than part of `Acceleration`, so it comes from the accelerator
            // contract (`AccelerationSource::on_schema_change`); a source that
            // states none (a view, DDL) and `block`/`fail` keep the default
            // Disabled = pin the stored schema.
            // `refresh_mode: caching` is excluded from in-place evolution in
            // v1: its hidden `__spice_cache_namespace` column is appended LAST
            // and evolution also appends at the end — the positional
            // disagreement is unfixable via column adds.
            // A partitioned table is excluded here, where the config is built,
            // rather than only where the partition wrapper is: this same config
            // opens the PARENT catalog entry, which is created first. Leaving
            // evolution on for that open lets the catalog widen the parent's
            // stored schema while every partition keeps its own — the accelerated
            // table then advertises a schema its data does not have, which is the
            // silent narrowing cast of #12999 reached through the open path
            // instead of `evolve_table_schema`.
            let is_caching_mode = acceleration.refresh_mode == Some(RefreshMode::Caching);
            let is_partitioned = !acceleration.partition_by.is_empty();
            config.schema_evolution = source
                .on_schema_change()
                .filter(|_| !is_caching_mode && !is_partitioned)
                .map_or(
                    cayenne::metadata::SchemaEvolutionMode::Disabled,
                    |on_schema_change| match on_schema_change {
                        OnSchemaChange::AppendNewColumns => {
                            cayenne::metadata::SchemaEvolutionMode::AddColumnsOnly
                        }
                        OnSchemaChange::SyncAllColumns | OnSchemaChange::DropAndRecreate => {
                            cayenne::metadata::SchemaEvolutionMode::Widen
                        }
                        OnSchemaChange::Block | OnSchemaChange::Fail => {
                            cayenne::metadata::SchemaEvolutionMode::Disabled
                        }
                    },
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

            // Provenance of the sort order. Schema inference sets this to
            // `inferred` when it fills `cayenne_sort_columns` itself; an operator
            // who configured the sort order explicitly leaves it absent, which
            // defaults to `user` (authoritative). Only an authoritative order may
            // shadow the hot filter columns observed on scans — see
            // `SortColumnsOrigin`. Unrecognized values fall back to the
            // conservative `user`, which preserves pre-existing behavior.
            if let Some(origin) = acceleration
                .params
                .get("cayenne_sort_columns_origin")
                .or_else(|| acceleration.params.get("sort_columns_origin"))
                && origin.trim().eq_ignore_ascii_case("inferred")
            {
                config.sort_columns_origin = cayenne::metadata::SortColumnsOrigin::Inferred;
            }

            // Parse shard key columns (the hash-clustering key for intra-write
            // sharding; the engine derives it from the primary key when unset)
            if let Some(shard_cols_str) = acceleration
                .params
                .get("cayenne_shard_key_columns")
                .or_else(|| acceleration.params.get("shard_key_columns"))
            {
                config.shard_key_columns = shard_cols_str
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
            }

            // Datalake / cold object-store tier (storage-cascade bottom tier).
            // Presence of a non-empty `cayenne_datalake_location` enables it; the
            // rest tune the clustering key, cold file size, and the warm→cold
            // promotion trigger.
            if let Some(loc) = acceleration.params.get("cayenne_datalake_location") {
                let loc = loc.trim();
                if !loc.is_empty() {
                    config.cold_tier_location = Some(loc.to_string());
                }
            }
            if let Some(cols) = acceleration
                .params
                .get("cayenne_datalake_clustering_columns")
            {
                config.cold_clustering_columns = cols
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
            }
            // Numeric cold knobs go through the same `auto_or_*` helpers as the
            // warm tuning params, so they honor `auto`, warn on invalid input,
            // and clamp consistently with the rest of the config surface.
            config.cold_target_file_size_mb = autotune::auto_or_usize(
                acceleration,
                &["cayenne_datalake_target_file_size_mb"],
                config.cold_target_file_size_mb,
            )
            .max(1);
            config.cold_tier_warm_max_bytes = autotune::auto_or_i64(
                acceleration,
                &["cayenne_datalake_warm_max_bytes"],
                config.cold_tier_warm_max_bytes,
            );
            config.cold_tier_warm_max_files = autotune::auto_or_usize(
                acceleration,
                &["cayenne_datalake_warm_max_files"],
                config.cold_tier_warm_max_files,
            );
            config.cold_tier_background_interval_ms = autotune::auto_or_u64(
                acceleration,
                &["cayenne_datalake_tiering_check_interval_ms"],
                config.cold_tier_background_interval_ms,
            );
            config.cold_tier_gc_interval_ms = autotune::auto_or_u64(
                acceleration,
                &["cayenne_datalake_gc_interval_ms"],
                config.cold_tier_gc_interval_ms,
            );
            // Default promotion trigger when the tier is enabled but neither
            // expert trigger is set (both `VortexConfig` defaults are 0 = never
            // promote, which would leave a location-only config silently
            // inert): promote once warm accumulates 16 target cold files'
            // worth of data. Data is sorted per clustering run, so a larger
            // accumulation yields better zone-map pruning in the written files.
            if config.cold_tier_enabled()
                && config.cold_tier_warm_max_bytes == 0
                && config.cold_tier_warm_max_files == 0
            {
                config.cold_tier_warm_max_bytes = i64::try_from(
                    config
                        .cold_target_file_size_mb
                        .saturating_mul(16 * 1024 * 1024),
                )
                .unwrap_or(i64::MAX);
                tracing::info!(
                    "Dataset '{table_name}': warm-tier data will move to the datalake once it reaches {} bytes (default). Set 'cayenne_datalake_warm_max_bytes' to override.",
                    config.cold_tier_warm_max_bytes
                );
            }
            // The datalake (cold) tier requires key-based deletes: position
            // deletes are file-path scoped and cannot survive the warm→cold
            // rewrite. An unresolved `auto` resolves to `key` here (otherwise
            // it resolves to `position` for non-CDC tables and the tier is
            // silently inert); an EXPLICIT `position` is left as-is and
            // rejected with a structured error at registration
            // (`validate_datalake_table_options`) — never silently overridden.
            // Must run AFTER cayenne_datalake_location is parsed above.
            if config.cold_tier_enabled()
                && workload.has_primary_key
                && config.deletion_mode == cayenne::metadata::DeletionMode::Auto
            {
                config.deletion_mode = cayenne::metadata::DeletionMode::Key;
                tracing::warn!(
                    "Dataset '{table_name}': auto-resolved cayenne_deletion_mode to 'key' (the datalake tier requires key-based deletes)."
                );
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
            // (session target_partitions, capped at the CPU budget's core count
            // and the global encode budget); 0 → warn + minimum 1.
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
            config.bake_deletion_index_trigger = autotune::auto_or_usize(
                acceleration,
                &["cayenne_bake_deletion_index_trigger"],
                config.bake_deletion_index_trigger,
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

            // Tuning mode (`cayenne_tuning`): `auto` derives the knobs statically
            // from the detected environment + inferred schema; `adaptive` also runs
            // the closed-feedback loop that moves them within the environment-derived
            // [floor, ceiling]. `adaptive` is reached ONLY by asking for it here —
            // unset, unrecognized, and every other signal (inferred schema, a
            // configured `cayenne_goal_*`) resolve to `auto`. Independently of the
            // mode, an explicit per-knob value overrides the derived one, and under
            // `adaptive` it *pins* that knob so the loop leaves it alone.
            let raw_tuning = acceleration
                .params
                .get("cayenne_tuning")
                .map(String::as_str);
            let (tuning_mode, tuning_was_invalid) = autotune::TuningMode::parse(raw_tuning);
            if tuning_was_invalid {
                tracing::warn!(
                    "Dataset '{table_name}' has an invalid `cayenne_tuning` value: '{}'. Expected 'auto' or 'adaptive'. Defaulting to 'auto'.",
                    raw_tuning.unwrap_or_default().trim()
                );
            }
            config.dynamic_tuning = tuning_mode.is_adaptive();
            // Inferred schema metadata SHARPENS the adaptive warm start (row_count/
            // table_bytes refine the memory sizing; inferred PK/index/sort metadata
            // feeds the query-health surface), but it is not REQUIRED for
            // `adaptive`: the controller relearns the observed mean row width from
            // live ingest and converges its actuators from the hardware-derived
            // warm start regardless. When the metadata is absent (the source may not
            // expose catalog metadata, or the connection role lacks read access),
            // note that the warm start is coarser but still let the closed loop run.
            if config.dynamic_tuning && !workload.inferred_metadata.is_present() {
                tracing::info!(
                    target: "spiced::acceleration::cayenne",
                    table = %table_name,
                    "`cayenne_tuning: adaptive`: no inferred schema metadata available for this table (the source may not expose catalog metadata or the connection role lacks read access); starting from the hardware-derived config and adapting from observed ingest."
                );
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
            // Goal-driven tuning: parse the high-level SLO setpoints. Times are
            // duration strings (`5s`/`1m`/`250ms`); QPH is a number. The goals
            // steer the closed loop but never ENABLE it: `adaptive` is reached
            // only by an explicit `cayenne_tuning: adaptive`, so a goal set
            // without it is inert and warns below. Query latency is stored in ms.
            // SLO setpoints resolve a GLOBAL default (`runtime.params`) with a
            // per-dataset (`acceleration.params`) override: set an SLO once for the
            // whole runtime and sharpen it per table where needed.
            let app = source.app();
            let runtime_params = &app.runtime.params;
            config.goal_replication_lag_secs = parse_goal_duration_secs(
                resolve_goal_raw(acceleration, runtime_params, "cayenne_goal_replication_lag"),
                "cayenne_goal_replication_lag",
                table_name,
            );
            config.goal_freshness_secs = parse_goal_duration_secs(
                resolve_goal_raw(acceleration, runtime_params, "cayenne_goal_freshness"),
                "cayenne_goal_freshness",
                table_name,
            );
            config.goal_query_latency_ms = parse_goal_duration_secs(
                resolve_goal_raw(acceleration, runtime_params, "cayenne_goal_query_latency"),
                "cayenne_goal_query_latency",
                table_name,
            )
            .map(|secs| secs * 1000.0);
            // The convergence window paces HOW the loop chases the SLOs (step
            // cadence = window / N), not a target outcome — a control/benchmarking
            // knob with a sensible default. It stays a PER-DATASET advanced override
            // and is intentionally NOT part of the global SLO surface.
            config.goal_convergence_window_secs = parse_goal_duration_secs(
                acceleration
                    .params
                    .get("cayenne_goal_convergence_window")
                    .map(String::as_str),
                "cayenne_goal_convergence_window",
                table_name,
            );
            // QPH is a SYSTEM-WIDE metric — a query (e.g. a join) spans datasets and
            // is counted once globally — so its goal is configured GLOBALLY only,
            // under `runtime.params`. There is no per-dataset QPH goal.
            config.goal_qph = parse_goal_f64(
                runtime_params.get("cayenne_goal_qph").map(String::as_str),
                "cayenne_goal_qph",
                "runtime.params",
            );
            let any_goal = config.goal_replication_lag_secs.is_some()
                || config.goal_freshness_secs.is_some()
                || config.goal_query_latency_ms.is_some()
                || config.goal_qph.is_some();
            // A goal never switches the mode: `adaptive` is a preview feature and
            // is entered only by asking for it, so a goal configured while the
            // loop is off is reported as ignored (below, under the newly-resolved
            // guard so a dataset that keeps failing to load does not repeat it)
            // rather than silently promoting the table into a different regime.
            let goals_are_inert = any_goal && !config.dynamic_tuning;
            if config.dynamic_tuning {
                tracing::warn!(
                    target: "spiced::acceleration::cayenne",
                    table = %table_name,
                    "`cayenne_tuning: adaptive` is in preview; verify query correctness and performance before using it for production workloads"
                );
            }
            config.pinned_tuning_actuators = cayenne::metadata::PinnedTuningActuators {
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
                bake_deletion_index_trigger: autotune::is_pinned(
                    acceleration,
                    &["cayenne_bake_deletion_index_trigger"],
                ),
                write_concurrency: autotune::is_pinned(
                    acceleration,
                    &["cayenne_write_concurrency", "write_concurrency"],
                ),
                mem_tier: autotune::is_pinned(acceleration, &["cayenne_cdc_mem_tier_max_bytes"]),
                target_file_size: autotune::is_pinned(
                    acceleration,
                    &["cayenne_target_file_size_mb"],
                ),
            };

            // Report the resolution once per *resolved configuration*, not once per
            // provider construction: this function runs on every
            // `create_external_table` / `create_cayenne_table_provider`, and a
            // dataset that keeps failing to load is rebuilt on every retry. Every
            // emit below is a pure function of `config` and `hw.cores`, so the one
            // fingerprint covers them all.
            let fingerprint = auto_tuned_config_fingerprint(table_name, &hw, workload, &config);
            if auto_tuned_config_is_newly_resolved(table_name, fingerprint) {
                // A `cayenne_goal_*` SLO with the closed loop off does nothing, and
                // it is easy to set one globally and assume it took effect.
                if goals_are_inert {
                    tracing::warn!(
                        target: "spiced::acceleration::cayenne",
                        table = %table_name,
                        "`cayenne_goal_*` is set but adaptive tuning is off, so the goals are ignored (`cayenne_tuning` defaults to 'auto'). Set `cayenne_tuning: adaptive` on this dataset to enable goal-seeking."
                    );
                }

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
                    // Inferred workload signals (from schema inference). When these are
                    // `None`/false the source exposed no inferred metadata for this
                    // table (no catalog access, or nothing to infer), so the data-aware
                    // sizing fell back to hardware-only — makes that immediately visible.
                    inferred_row_count = ?workload.row_count,
                    inferred_table_bytes = ?workload.table_bytes,
                    inferred_schema_present = workload.inferred_metadata.is_present(),
                    has_primary_key = workload.has_primary_key,
                    is_upsert = workload.is_upsert,
                    "Cayenne auto-tuned config: pk_keyset_cache={:?}MB, target_file_size={}MB, upload_concurrency={}, write_concurrency_override={:?}, sort_columns={:?}, compression_strategy={:?}, delta_encoding={}, pk_conflict_detection={}, deletion_mode={:?}, compaction_trigger_files={}, compaction_trigger_protected_snapshots={}, compaction_trigger_snapshot_age_ms={}, compaction_max_levels={}, compaction_max_files_per_pick={}, compaction_background_interval_ms={}, inline_max_rows={}, inline_max_bytes={}, inline_max_buffer_bytes={}, inline_flush_max_rows={}, inline_flush_max_segments={}, inline_flush_max_bytes={}, cdc_durability={}, cdc_mem_tier_max_bytes={}, cdc_mem_tier_min_flush_bytes={}",
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
                    config.cdc_durability.as_str(),
                    config.cdc_mem_tier_max_bytes,
                    config.cdc_mem_tier_min_flush_bytes,
                );
            }
        }

        Ok(config)
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

    /// Lazily initialize a Cayenne catalog into `cell` from `connection_string`,
    /// sharing the init/`OnceCell` machinery between the file-mode and memory-mode
    /// catalog getters.
    async fn init_cayenne_catalog(
        cell: &OnceCell<Arc<dyn cayenne::MetadataCatalog>>,
        connection_string: String,
    ) -> Result<Arc<dyn cayenne::MetadataCatalog>> {
        cell.get_or_try_init(move || {
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

    async fn get_or_create_catalog(
        &self,
        metadata_dir: &str,
        metastore_type: &str,
    ) -> Result<Arc<dyn cayenne::MetadataCatalog>> {
        let connection_string = match metastore_type {
            "turso" => format!("libsql://{metadata_dir}/cayenne.db"),
            _ => format!("sqlite://{metadata_dir}/cayenne.db"), // Default to SQLite
        };
        Self::init_cayenne_catalog(&self.catalog, connection_string).await
    }

    /// Get or create the shared in-memory (`memdb`) catalog for `mode: memory`
    /// tables. The DSN uses `SQLite`'s `memdb` VFS keyed by this accelerator's
    /// instance id, so the metastore lives entirely in RAM (nothing on disk) and
    /// distinct accelerator instances stay isolated.
    async fn get_or_create_memory_catalog(&self) -> Result<Arc<dyn cayenne::MetadataCatalog>> {
        let connection_string =
            format!("sqlite://file:/cayenne-mem-{}?vfs=memdb", self.instance_id);
        Self::init_cayenne_catalog(&self.memory_catalog, connection_string).await
    }

    /// Apply the `mode: memory` overrides to a table's [`cayenne::metadata::VortexConfig`]:
    /// make the mem-tier the permanent in-RAM store — never checkpoint/seal to
    /// Vortex, no compaction/cold tier, single shard (so a full-refresh overwrite is
    /// one atomic swap), and no inline-corpus publishing. The per-table byte cap
    /// becomes the hard RAM bound (breach => error, never spill); default `0` =
    /// unbounded (Arrow parity) unless the operator sets an explicit
    /// `cayenne_cdc_mem_tier_max_bytes`.
    ///
    /// Note: with the drain disabled, an `append`/`changes` memory table accumulates
    /// mem-tier segments with no in-RAM coalesce valve, so append cost grows with the
    /// segment count. `full` refresh is unaffected (each overwrite resets the tier to
    /// a single segment). A periodic in-RAM segment coalesce is a future follow-up.
    fn apply_memory_mode_overrides(
        config: &mut cayenne::metadata::VortexConfig,
        acceleration: Option<&Acceleration>,
    ) {
        config.memory_mode = true;
        config.cdc_mem_tier_shards = 1;
        config.cdc_mem_tier_max_age_ms = 0;
        config.cdc_mem_tier_checkpoint_interval_ms = 0;
        config.cdc_mem_tier_seal_age_ms = 0;
        config.compaction_background_interval_ms = 0;
        config.cold_tier_location = None;
        config.inline_max_rows = 0;
        config.inline_max_bytes = 0;
        config.inline_max_buffer_bytes = 0;
        // Only a number the config field can actually hold is an explicit limit.
        // `auto` is a request for the derived default, which in memory mode is "no
        // cap" — reading mere key presence would turn
        // `cayenne_cdc_mem_tier_max_bytes: auto` into the file-mode host-derived
        // cap, and memory mode has no spill path to fall back on, so crossing it
        // fails the write. A value above `i64::MAX` is the same story: knobs parse
        // as `usize`, so it reads as set, but `auto_or_i64` cannot represent it and
        // has already fallen back to the derived value — treating it as explicit
        // would pin a cap the operator never asked for.
        let explicit_limit = acceleration.is_some_and(|a| {
            matches!(
                autotune::read_knob(
                    a,
                    &["cayenne_cdc_mem_tier_max_bytes", "cdc_mem_tier_max_bytes"],
                ),
                autotune::Knob::Set(bytes) if i64::try_from(bytes).is_ok()
            )
        });
        if !explicit_limit {
            config.cdc_mem_tier_max_bytes = 0;
        }
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
        let maintained_aggregate_specs =
            maintained_aggregate_specs_for_cayenne(acceleration, &schema, &primary_keys)?;
        let metastore_type = acceleration
            .and_then(|a| a.params.get("cayenne_metastore"))
            .map_or("sqlite", String::as_str)
            .to_string();

        // Memory mode (`mode: memory`): fully in-RAM — an in-memory `memdb`
        // metastore (nothing on disk) and no metadata directory. File mode creates
        // the metadata dir and uses the shared on-disk catalog as before.
        let memory_mode = !source.is_file_accelerated();
        let catalog = if memory_mode {
            self.get_or_create_memory_catalog().await?
        } else {
            // Ensure metadata directory exists
            std::fs::create_dir_all(&metadata_dir)
                .boxed()
                .context(AccelerationCreationFailedSnafu)?;
            // Get or create the shared catalog (lazy initialization)
            self.get_or_create_catalog(&metadata_dir, &metastore_type)
                .await?
        };

        // S3 Express One Zone is file-mode only. Memory mode never builds an
        // object store; if S3 Express params linger while mode is memory, treat
        // them as inactive so we don't fail with a missing object-store error.
        let is_s3_express = !memory_mode && s3::is_s3_express_data_path(source);
        let workload = build_workload_profile(
            source,
            acceleration,
            schema.as_ref(),
            &primary_keys,
            on_conflict.as_ref(),
        );
        let mut vortex_config = Self::get_vortex_config_with_footer_cache(
            table_name,
            source,
            self.footer_cache_mb,
            &workload,
        )
        .await?;

        // Memory mode: make the mem-tier the permanent in-RAM store — never
        // checkpoint/seal to Vortex, no compaction/cold tier, single shard (so a
        // full-refresh overwrite is one atomic swap), no inline-corpus publishing.
        if memory_mode {
            Self::apply_memory_mode_overrides(&mut vortex_config, acceleration);
        }

        // Build S3 object store if using S3 Express One Zone storage. Memory mode
        // has no data directory or object store (and `cayenne_data_dir` errors for
        // it), so skip this entirely — memory-mode data lives only in RAM.
        let object_store = if memory_mode {
            None
        } else {
            s3::build_s3_object_store(source, self.cayenne_data_dir(source)?)
                .await
                .context(S3Snafu)?
        };

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

        // Durable federated write-back (#11838): a write_back + on_conflict +
        // refresh_mode:changes Cayenne dataset resolves to WriteMode::WriteBack
        // (on_conflict with a non-`changes` refresh forces AcceleratorOnly). When
        // so configured, every committed write durably marks its PKs so the
        // delivery worker reconciles them to the federated source.
        // `resolves_to_durable_write_back` is shared with the registration gate
        // that requires the source connector to advertise a safe delivery
        // primitive, so marking can never be switched on for a dataset the gate
        // would have rejected.
        let durable_write_back = source
            .acceleration()
            .is_some_and(Acceleration::resolves_to_durable_write_back);

        // Default per-scan freshness. Bounded staleness is only in-contract for a
        // read-only CDC *replica* (`refresh_mode: changes`): its data streams in
        // continuously and is eventually-consistent by design, so a read tolerates a
        // bounded lag — and there the demand cache's cross-query reuse lever pays off
        // (concurrent analytical scans share one build). Read-only alone is NOT enough:
        // a full-refresh / snapshot / append table is expected to reflect its last
        // refresh immediately (refresh-then-query reads its own writes), and a read-only
        // table can still take direct `delete_from`/DML via the accelerator — so serving
        // a pre-mutation view there is a stale (wrong) result. Any table we cannot prove
        // is a read-only CDC replica therefore uses 0 = read-your-writes, so a scan
        // always sees the latest state. (A read-write dataset requires BOTH a ReadWrite
        // API key and `access: read_write`.)
        let is_cdc_replica = source
            .acceleration()
            .is_some_and(|acceleration| acceleration.refresh_mode == Some(RefreshMode::Changes));
        let default_scan_freshness = if is_cdc_replica && !source.allows_write() {
            read_only_scan_freshness()
        } else {
            std::time::Duration::ZERO
        };

        // Create CayenneTableProvider with object store for S3 Express One Zone
        let mut builder = CayenneTableProviderBuilder::new(catalog, runtime_env)
            .with_context(context)
            .with_retention_filters(retention_filters)
            .with_maintained_aggregates(maintained_aggregate_specs)
            .with_durable_write_back(durable_write_back)
            .with_default_scan_freshness(default_scan_freshness);
        if let Some(retention_builder) = time_retention_filter_builder {
            builder = builder.with_time_retention_filter_builder(retention_builder);
        }
        // The datalake tier supports `s3://` locations only. Anything else
        // would be silently treated as a local directory by the engine's write
        // path — reject it at registration instead.
        if let Some(location) = table_options.vortex_config.cold_tier_location.as_deref()
            && !location.starts_with("s3://")
        {
            return Err(Error::AccelerationCreationFailed {
                source: Box::new(std::io::Error::other(format!(
                    "Failed to register dataset {table_name} (cayenne): unsupported datalake location '{location}'. Expected 's3://bucket/prefix'. Update 'cayenne_datalake_location'."
                ))),
            });
        }
        // The datalake tier supports only continuously-ingesting refresh modes:
        // 'changes' (CDC) and 'append'. A 'full' refresh re-materializes the
        // whole table on every refresh — an overwrite that discards the
        // promoted datalake generation each time, defeating the tier.
        if table_options.vortex_config.cold_tier_enabled() {
            let refresh_mode = source
                .acceleration()
                .and_then(|a| a.refresh_mode)
                .unwrap_or(RefreshMode::Full);
            if !matches!(refresh_mode, RefreshMode::Changes | RefreshMode::Append) {
                return Err(Error::AccelerationCreationFailed {
                    source: Box::new(std::io::Error::other(format!(
                        "Failed to register dataset {table_name} (cayenne): the datalake tier supports refresh_mode 'changes' or 'append'; found '{refresh_mode:?}'. \
                        Set 'refresh_mode: changes' or 'refresh_mode: append', or remove 'cayenne_datalake_location'."
                    ))),
                });
            }
        }
        // Reject configurations that would leave the datalake tier silently
        // inert or unsafe (explicit position deletes, disabled promotion/GC
        // loops) and WARN on degraded ones (PK-less table → tier inactive,
        // unknown clustering columns) — a diagnostic at registration beats a
        // tier that never promotes with no explanation.
        match validate_datalake_table_options(table_name, &table_options) {
            Ok(warnings) => {
                for warning in warnings {
                    tracing::warn!("{warning}");
                }
            }
            Err(message) => {
                return Err(Error::InvalidConfiguration {
                    detail: message.into(),
                });
            }
        }
        // Datalake (cold) tier object store: built from the dedicated
        // `cayenne_datalake_s3_*` params (default `iam_role` auth falls back to environment/SDK credentials).
        let cold_object_store = if table_options.vortex_config.cold_tier_enabled()
            && table_options
                .vortex_config
                .cold_tier_location
                .as_deref()
                .is_some_and(|l| l.starts_with("s3://"))
        {
            let location = table_options
                .vortex_config
                .cold_tier_location
                .clone()
                .unwrap_or_default();
            s3::build_datalake_object_store(source, &location)
                .await
                .context(S3Snafu)?
        } else {
            None
        };
        // Fail fast at registration on datalake-store misconfiguration: verify
        // write access to the location prefix before the table is created.
        if let Some(ref cold) = cold_object_store {
            s3::validate_datalake_store_access(cold, table_name)
                .await
                .context(S3Snafu)?;
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
        if let Some(cold) = cold_object_store {
            builder = builder.with_cold_object_store(cold);
        }
        tracing::debug!("create_cayenne_table_provider: calling builder.create for {table_name}");
        let cayenne_table = builder
            .create(table_options)
            .await
            .boxed()
            .context(AccelerationCreationFailedSnafu)?;

        tracing::debug!("create_cayenne_table_provider: table {table_name} created successfully");
        let provider = Arc::new(cayenne_table);
        // Initialize the demand scan-view cache (weak self-reference for its
        // `spawn_blocking` builds + the periodic eviction sweep that releases idle
        // cached views' pinned snapshot dirs for GC). Must run once, after `Arc::new`.
        provider.init_scan_view_cache();
        // Memory mode never drains to Vortex (no compaction, no mem-tier
        // checkpoint/seal, no cold tier), so skip the background drain tasks
        // entirely; the provider's own guards also no-op them defensively.
        if !memory_mode {
            let spawned =
                provider.spawn_background_compaction(Arc::clone(&self.compaction_semaphore));
            if spawned {
                tracing::debug!(
                    "Background compaction task spawned for Cayenne table {table_name}",
                );
            }
            // Periodic mem-tier checkpoint (cdc_durability: memory only); a no-op for
            // file-mode tables. This is what advances the deferred source slot ack on
            // an idle/pure-upsert stream so replication lag stays bounded.
            if provider.spawn_background_mem_tier_checkpoint() {
                tracing::debug!(
                    "Background mem-tier checkpoint task spawned for Cayenne table {table_name}",
                );
            }
            // Cold-tier promotion (storage-cascade bottom tier); a no-op unless
            // cayenne_datalake_location is set. Runs on the same internal
            // background-worker infra as the mem-tier checkpointer, on its own
            // cadence — no spicepod `workers:` section, nothing user-facing.
            if provider.spawn_background_cold_tier_promotion() {
                tracing::debug!(
                    "Background datalake tiering task spawned for Cayenne table {table_name}",
                );
            }
        }
        Ok(provider)
    }
}

/// Registration-time validation for datalake (cold) tier table options: the
/// misconfigurations rejected here would otherwise leave the tier silently
/// inert (a promoter that early-returns forever) or unsafe (a GC grace of
/// zero). A no-op when the tier is disabled.
///
/// Returns `Err(message)` for configurations that must fail registration and
/// `Ok(warnings)` for configurations that register but degrade — including a
/// PK-less table, where the tier stays INACTIVE rather than failing (the
/// dataset is fully serviceable from the warm tier, so a fleet-wide datalake
/// location must not block PK-less datasets).
/// Pure (no I/O, no logging) so each rule stays unit-testable.
fn validate_datalake_table_options(
    table_name: &str,
    options: &cayenne::metadata::CreateTableOptions,
) -> Result<Vec<String>, String> {
    let vc = &options.vortex_config;
    if !vc.cold_tier_enabled() {
        return Ok(Vec::new());
    }
    let mut warnings = Vec::new();
    if options.primary_key.is_empty() {
        // Promotion classifies and rewrites cold files by primary key, and
        // deletes against cold-resident rows are key-based, so the promoter
        // early-returns for PK-less tables — the tier is configured but
        // inactive. Warn loudly instead of failing registration.
        warnings.push(format!(
            "Dataset '{table_name}': 'cayenne_datalake_location' is set but the dataset has no primary key, so the datalake tier stays INACTIVE (data never moves to the datalake and stays in the warm tier). Add 'primary_key' to the acceleration to activate it, or remove 'cayenne_datalake_location'."
        ));
    }
    // Position deletes are file-path scoped and cannot survive the warm→cold
    // rewrite; the engine skips promotion for position-mode tables. An
    // explicit conflict is an error, not a silent override.
    if vc.deletion_mode == cayenne::metadata::DeletionMode::Position {
        return Err(format!(
            "Failed to register dataset {table_name} (cayenne): the datalake tier requires key-based deletes, but 'cayenne_deletion_mode: position' is set. \
            Set 'cayenne_deletion_mode: key' (or remove it), or remove 'cayenne_datalake_location'."
        ));
    }
    // Default values quoted in the error hints, derived so they can never
    // drift from the actual `VortexConfig` defaults.
    let defaults = cayenne::metadata::VortexConfig::default();
    // The tiering-check interval drives BOTH the data-move evaluation and the
    // physical GC loop; 0 means the background task is never spawned, so warm
    // data never moves to the datalake and superseded objects are never reclaimed.
    if vc.cold_tier_background_interval_ms == 0 {
        return Err(format!(
            "Failed to register dataset {table_name} (cayenne): 'cayenne_datalake_tiering_check_interval_ms' is 0, which disables the datalake tiering loop — warm-tier data would never move to the datalake and superseded objects would never be reclaimed. \
            Set a positive interval (default {}), or remove 'cayenne_datalake_location'.",
            defaults.cold_tier_background_interval_ms
        ));
    }
    // The GC interval doubles as the orphan grace: 0 would let a superseded
    // object be deleted while a running scan still reads it.
    if vc.cold_tier_gc_interval_ms == 0 {
        return Err(format!(
            "Failed to register dataset {table_name} (cayenne): 'cayenne_datalake_gc_interval_ms' is 0, which collapses the garbage-collection grace period to zero — a superseded datalake object could be deleted while a running query still reads it. \
            Set a positive interval (default {}), or remove 'cayenne_datalake_location'.",
            defaults.cold_tier_gc_interval_ms
        ));
    }
    // Unknown clustering columns are dropped by the engine at promotion time
    // (falling back to sort columns, then the primary key) — surface the
    // misconfiguration instead of silently clustering by something else.
    for column in &vc.cold_clustering_columns {
        if options.schema.column_with_name(column).is_none() {
            warnings.push(format!(
                "Dataset '{table_name}': 'cayenne_datalake_clustering_columns' entry '{column}' does not exist in the schema and is ignored; datalake clustering falls back to cayenne_sort_columns, then the primary key."
            ));
        }
    }
    Ok(warnings)
}

/// Build a [`NativeVectorIndex`] for each `FixedSizeList<Float32, N>` column in
/// the schema. These indexes are attached to the accelerated table via
/// [`IndexLayer`] so the search engine's `get_vector_index()` can
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
/// caller should skip the `IndexLayer` wrap in that case.
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

/// Wrap a table provider in [`IndexLayer`] when the schema has at
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
        spice_table::SpiceTable::over(Arc::new(IndexLayer::with_indexes(indexes)), provider)
            as Arc<dyn TableProvider>
    }
}

const PARAMETERS: &[ParameterSpec] = &concat_arrays::<
    ParameterSpec,
    S3_PARAMS_LEN,
    64,
    { S3_PARAMS_LEN + 64 },
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
            .description("Ignored: the in-memory Vortex segment cache is now one budget shared by every Cayenne table rather than a cache per table, so a per-table size no longer has anything to size. Set runtime.params.cayenne_segment_cache_mb instead (unset: ~1/64 of the available memory, clamped to [256 MB, 2048 MB]; 0 disables caching). A value set here is reported at startup and otherwise has no effect."),
        ParameterSpec::component("scan_concurrency")
            .description("How many splits a single Vortex file scan decodes concurrently. 'auto' (default) derives it from the query fan-out and the number of files a scan plans, so a table held in few files still decodes in parallel. 'off' decodes each file serially. An explicit count pins it. Each concurrent split holds a decoded batch, and all of them are charged to 'runtime.query.memory_limit', so lowering this cuts a scan's resident decode memory without shrinking query parallelism everywhere the way 'runtime.query.target_partitions' does.")
            .default("auto"),
        ParameterSpec::component("pk_keyset_cache_mb")
            .description("Byte budget (in MB) for the in-memory primary-key index used to detect upsert conflicts during CDC ingestion. Within budget an exact keyset is kept; over budget, upsert tables fall back to a bounded bloom existence filter (avoiding the per-batch full-table rebuild) while DoNothing tables rebuild from a scan. When unset, an optimal default is derived from available machine memory."),
        ParameterSpec::component("target_file_size_mb")
            .description("Target size for Vortex data files in MB. 'auto' (default, or when unset) is storage-aware: 256 MB on EBS-class network storage, 64 MB on RAM-backed (tmpfs) mounts, and the 256 MB engine default on local SSD / unknown / S3. Set an explicit MB value to override.")
            .default("auto"),
        ParameterSpec::component("force_view_types")
            .description("When 'true' (default), Cayenne advertises and decodes string and binary columns as Arrow view types (Utf8View/BinaryView) on the query/scan path so DataFusion plans joins and aggregates on view arrays, avoiding the i32 2 GiB offset overflow in hash-join build-side batch concatenation at scale. The stored schema keeps Utf8/Binary for writes, CDC, and stats. Set 'false' to opt out.")
            .one_of(&["true", "false"])
            .default("true"),
        ParameterSpec::component("datalake_location")
            .description("Object-store URL prefix for the datalake tier, e.g. 's3://bucket/prefix' — the storage-cascade bottom tier. When set, a background tiering loop moves warm local-disk data to read-optimized, Z-order-clustered Vortex files on this store, and queries span warm + datalake with per-tier pushdown. Unset (default) disables the tier. Requires key-based deletes and a primary key (auto-resolved). Partitioned and position-delete tables are not supported."),
        ParameterSpec::component("datalake_clustering_columns")
            .description("Comma-separated liquid-clustering key columns for datalake files (multi-column Z-order), e.g. 'tenant_id,ts'. When unset, falls back to cayenne_sort_columns, then the primary key. Clustering tightens each cold file's per-column zone maps so selective queries on any clustering dimension prune at the storage layer."),
        ParameterSpec::component("datalake_s3_auth")
            .description("Authentication method for the datalake S3 store. 'iam_role' (default) uses environment/SDK credentials; 'key' uses cayenne_datalake_s3_key/_secret.")
            .one_of(&["iam_role", "key"])
            .default("iam_role"),
        ParameterSpec::component("datalake_s3_key")
            .description("Access key ID for the datalake S3 store (with cayenne_datalake_s3_auth: key).")
            .secret(),
        ParameterSpec::component("datalake_s3_secret")
            .description("Secret access key for the datalake S3 store (with cayenne_datalake_s3_auth: key).")
            .secret(),
        ParameterSpec::component("datalake_s3_session_token")
            .description("Optional session token for the datalake S3 store (with cayenne_datalake_s3_auth: key).")
            .secret(),
        ParameterSpec::component("datalake_s3_region")
            .description("AWS region of the datalake S3 bucket. Defaults to the environment region (AWS_REGION/AWS_DEFAULT_REGION), then us-east-1; inert for S3-compatible endpoints."),
        ParameterSpec::component("datalake_s3_endpoint")
            .description("Custom S3 endpoint URL for the datalake store (e.g. an S3-compatible store such as MinIO). http:// endpoints implicitly allow HTTP."),
        ParameterSpec::component("datalake_s3_allow_http")
            .description("Allow plain-HTTP connections to the datalake S3 endpoint. Default: false.")
            .one_of(&["true", "false"])
            .default("false"),
        ParameterSpec::component("datalake_s3_client_timeout")
            .description("HTTP client timeout for datalake store requests, as a duration (e.g. '2m'). Default: 2m."),
        ParameterSpec::component("datalake_s3_unsigned_payload")
            .description("Use unsigned payloads for datalake S3 uploads. Default: true.")
            .one_of(&["true", "false"])
            .default("true"),
        ParameterSpec::component("datalake_target_file_size_mb")
            .description("Target size for datalake (cold) tier Vortex files in MB. Larger than the warm cayenne_target_file_size_mb because object stores favor fewer, larger objects and cold scans are range reads. Default: 512."),
        ParameterSpec::component("datalake_warm_max_bytes")
            .description("Warm-tier data moves to the datalake once the warm tier's total Vortex bytes reach this threshold. Pairs with cayenne_datalake_warm_max_files; 0 disables the byte trigger, but when BOTH triggers are 0/unset this one defaults to 16 x cayenne_datalake_target_file_size_mb."),
        ParameterSpec::component("datalake_warm_max_files")
            .description("Warm-tier data moves to the datalake once the warm tier's Vortex file count reaches this threshold. 0 (default) disables the file-count trigger; when cayenne_datalake_warm_max_bytes is also 0/unset, the byte trigger defaults to 16 x cayenne_datalake_target_file_size_mb."),
        ParameterSpec::component("datalake_tiering_check_interval_ms")
            .description("How often the background loop checks whether warm-tier data should move to the datalake (a check does not guarantee a move). Normally auto-tuned; override for testing. Default: 60000 (60s)."),
        ParameterSpec::component("datalake_gc_interval_ms")
            .description("Physical-GC cadence and orphan grace for superseded datalake objects: the background sweep runs about this often and deletes an object no longer referenced by the manifest only once it has been observed orphaned for at least this long (so an in-flight scan has a full interval to finish). Default: 300000 (5min)."),
        ParameterSpec::component("sort_columns")
            .description("Comma-separated list of columns to sort data by during inserts (e.g., 'timestamp,user_id')."),
        ParameterSpec::component("sort_columns_origin")
            .description("Provenance of 'sort_columns'. Normally set by schema inference rather than by hand: 'user' (the default when absent) means the sort order was configured explicitly and is authoritative, while 'inferred' means schema inference filled it from the source's declared order (the primary key, for most CDC tables), which is a guess and is outranked by the filter columns actually observed on scans, so the adaptive layout can cluster for the real workload. Setting it explicitly is supported and is useful for reproducing the inferred configuration in a benchmark or test.")
            .one_of(&["user", "inferred"]),
        ParameterSpec::component("shard_key_columns")
            .description("Comma-separated list of columns to hash-cluster rows by during intra-write sharding (the parallel encode fan-out), e.g. 'tenant_id'. When unset, the shard key derives from the primary key (PK-hash clustering); tables without a primary key shard round-robin. Schema inference fills this from the source's declared partition/shard key when the user leaves it unset. Ignored for sorted tables: sort_columns forces a single serial writer."),
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
            .description("How primary-key deletions are recorded and applied. 'auto' (default) resolves to 'key' for refresh_mode: changes tables with a primary key (key-based deletes compact concurrently with writers and ride the in-memory CDC tier, where position-delete compaction must serialize with continuous writes), and to 'position' (merge-on-read) everywhere else: per-file row-position bitmaps are pushed into the Vortex scan, skipping deleted pages at the storage layer with no per-row CPU. For a primary-key table positions are captured via a row_idx() read-back after each write, with key-based fallback for any row whose position is not yet known; a table without a primary key uses the existing position-based strategy. 'key' applies deletes above the Vortex scan via a per-row RowConverter probe; 'position' explicitly opts a CDC table back into merge-on-read.")
            .one_of(&["auto", "key", "position"])
            .default("auto"),
        ParameterSpec::component("upload_concurrency")
            .description("Maximum number of concurrent file uploads when writing multiple Vortex files. 'auto' (or unset) uses the runtime's CPU budget (see `runtime.cpu.cores`). The aggregate encode concurrency across all Cayenne tables is separately bounded by a process-global budget sized from that same value."),
        ParameterSpec::component("write_concurrency")
            .description("Writer partition override (parallel encoders) for unsorted Cayenne ingests. 'auto' (or unset) uses a small fixed default of 4, capped at the runtime's CPU budget (= runtime.query.target_partitions) and the process-global encode budget — deliberately not the full core count, because each table is sized independently and the per-table values sum across tables under concurrent CDC. Raise it explicitly for a table that needs more encode parallelism."),
        ParameterSpec::component("compaction_trigger_files")
            .description("Minimum number of small Vortex files in the current snapshot before tiered compaction runs. A 'small' file is one whose size is below cayenne_target_file_size_mb / 4. Default: 4 for refresh_mode: caching, changes, or append with refresh_check_interval <= 5m; 8 otherwise."),
        ParameterSpec::component("bake_deletion_index_trigger")
            .description("Deletion-index size (count of live primary-key tombstones) at or above which the seq-prefix bake (key-delete merge-on-read compaction) runs. The bake consolidates the settled older prefix of protected snapshots so their tombstones drop out of the live deletion index, lowering per-query merge-on-read probe cost at the cost of write amplification. A larger value bakes less often (bounds write-amp); a smaller value bakes more often (smaller index, cheaper probe). Key-delete tables only. Default: 50000."),
        ParameterSpec::component("compaction_trigger_protected_snapshots")
            .description("Number of protected snapshots before snapshot-maintenance compaction runs. This is separate from compaction_trigger_files so small-file tuning does not silently change scan amplification behavior. Default: 4 for refresh_mode: caching, changes, or append with refresh_check_interval <= 5m; 8 otherwise."),
        ParameterSpec::component("compaction_trigger_snapshot_age_ms")
            .description("Maximum age in milliseconds of the oldest protected snapshot before snapshot-maintenance compaction runs. Set to 0 to disable the age trigger. Default: 60000 for refresh_mode: caching, changes, or append with refresh_check_interval <= 5m; 300000 otherwise."),
        ParameterSpec::component("compaction_max_levels")
            .description("Maximum number of consecutive compaction passes per trigger. Bounds write amplification when tiered compaction keeps producing new candidates. Default: 3.")
            .default("3"),
        ParameterSpec::component("compaction_max_files_per_pick")
            .description("Maximum number of eligible file paths retained in one compaction candidate for trigger selection and observability. The current compactor rewrites the whole current snapshot once triggered, so this does not bound rewrite IO or memory. Default: 32.")
            .default("32"),
        ParameterSpec::component("compaction_background_interval_ms")
            .description("Background compaction interval in milliseconds. The accelerator runs a per-table background task at this interval. Set to 0 to disable the background task — inline compaction on writes still runs. Default: 10000 for refresh_mode: caching, changes, or append with refresh_check_interval <= 5m; 0 (disabled) for refresh_mode: full, whose whole-table replace leaves nothing to consolidate; 30000 otherwise."),
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
            .description("Durability mode for the inline CDC write path. 'memory' (default, eligibility-gated) appends batches to an in-RAM tier and defers the source slot ack to a periodic/cap-triggered checkpoint, collapsing per-batch durability cost; on crash the un-checkpointed tail is replayed from the source slot (the apply is PK-idempotent, so exactly-once). Bounded by a per-table byte cap and a process-global byte budget so it cannot OOM. The memory path applies only to the small-write/CDC profile and non-partitioned tables; other profiles use 'file'. 'file' persists each CDC batch durably before advancing the source slot and remains the explicit conservative opt-out.")
            .one_of(&["file", "memory"])
            .default("memory"),
        ParameterSpec::component("cdc_mem_tier_max_bytes")
            .description("Per-table RAM-tier byte cap before a forced spill (checkpoint) and slot advance, in cdc_durability: memory mode only. Auto-derived from host memory (~1/64 of RAM, clamped to 256 MiB - 1 GiB; 256 MiB on hosts at or under 16 GiB) — a rare backstop now that the non-fence-blocking background checkpointer is the primary flush. Set 0 to disable the per-table cap; the process-global byte budget still bounds aggregate resident memory. When both are set, whichever is breached first triggers the spill."),
        ParameterSpec::component("cdc_mem_tier_max_age_ms")
            .description("Max wall-clock milliseconds a RAM-tier epoch may age before a forced checkpoint, in cdc_durability: memory mode only. Bounds the crash-replay window and the deferred source-slot ack for tables that never reach a byte threshold. Default 10000 (10 s). Set 0 to disable the age trigger."),
        ParameterSpec::component("cdc_mem_tier_min_flush_bytes")
            .description("Minimum resident RAM-tier bytes before the periodic background tick durably checkpoints, in cdc_durability: memory mode only. Bounds snapshot/delete-file churn: below this size a tick is skipped unless the tier age reached cdc_mem_tier_max_age_ms. Query freshness is unaffected (RAM rows are visible immediately); only the deferred slot ack waits. The write-path byte-cap spill is not gated. Auto-derived as 1/8 of the derived cdc_mem_tier_max_bytes (clamped to 32-128 MiB; 32 MiB on hosts at or under 16 GiB). Set 0 to flush on every tick."),
        ParameterSpec::component("cdc_mem_tier_checkpoint_interval_ms")
            .description("Periodic background mem-tier checkpoint interval in milliseconds, in cdc_durability: memory mode only. The accelerator spawns a per-table background task that checkpoints the RAM tier every interval (mirroring the background compactor); this advances the deferred source slot ack on an idle or pure-upsert stream that never trips a delete/truncate event trigger or a write-path cap. Default 1000 (1 s). Set 0 to disable the periodic task."),
        ParameterSpec::component("cdc_mem_tier_shards")
            .description("Number of PK-hash shards the in-RAM CDC tier is partitioned into, in cdc_durability: memory mode only (non-partitioned, key-based merge-on-read tables). Each shard is an independent serial validate->append domain keyed by the RowConverter OwnedRow bytes, so disjoint keys validate and append in parallel within one apply (intra-apply fan-out) while a key's whole version history — upserts AND delete tombstones — stays confined to its one owning shard (last-writer-wins preserved). Checkpoints are always all-shards-atomic on a single source-position axis. Default 1 (the byte-identical serial path). Raise (e.g. 4) on update/insert-heavy CDC tables to lift the per-apply serialization ceiling."),
        ParameterSpec::component("tuning")
            .description("Auto-tuning mode. 'auto': derive the correct configuration values from the detected environment (cgroup-aware cores + memory, storage class) and the inferred schema (cardinality, row width, primary key) — no closed loop. 'adaptive': additionally run a per-table closed-feedback controller that measures the live CDC ingest rate, delete fraction, and arrival burstiness AND the runtime's whole-system response (apply latency vs offered load, read amplification that slows queries, cgroup-aware memory pressure) and adapts the inline-memtable flush caps, the in-memory CDC tier byte cap, compaction cadence/trigger, and write concurrency over time, within the environment-derived [floor, ceiling]. DEFAULT: 'auto'. Nothing else turns the closed loop on — 'adaptive' is entered only by setting it here, and a configured cayenne_goal_* SLO is ignored (with a warning) until you do. Schema inference is always attempted and sharpens the 'adaptive' warm-start (inferred cardinality/size) but is not required for it — without inferred metadata the controller relearns the row width from observed ingest and converges from the hardware-derived warm-start. In BOTH modes an explicit per-parameter value (e.g. cayenne_segment_cache_mb: 512) overrides the derived value; under 'adaptive' an explicitly-set actuator is pinned (the loop will not move it).")
            .one_of(&["auto", "adaptive"])
            .default("auto"),
        ParameterSpec::component("goal_replication_lag")
            .description("Goal-driven adaptive tuning: target end-to-end CDC replication lag as a duration (e.g. '5s'). Best set GLOBALLY at runtime.params (cayenne_goal_replication_lag) and overridden here per-dataset. Requires cayenne_tuning: adaptive — under the 'auto' default the goal is ignored. With the loop enabled, it converges toward this SLO in small, bounded steps."),
        ParameterSpec::component("goal_freshness")
            .description("Goal-driven adaptive tuning: target data freshness — age of the newest queryable data — as a duration (e.g. '30s'). Settable globally at runtime.params and overridden here per-dataset. Requires cayenne_tuning: adaptive."),
        ParameterSpec::component("goal_query_latency")
            .description("Goal-driven adaptive tuning: target p99 query latency on this table as a duration (e.g. '10s' or '250ms'). Settable globally at runtime.params and overridden here per-dataset. Requires cayenne_tuning: adaptive."),
        // NOTE: there is no per-dataset `goal_qph` ParameterSpec — QPH is a
        // system-wide metric (a query/join spans datasets), so its goal is
        // configured globally only, under `runtime.params`.
        ParameterSpec::component("goal_convergence_window")
            .description("Advanced: the control-loop pacing window — the time budget over which the loop steps toward the configured cayenne_goal_* SLOs, as a duration (e.g. '1m'). Default 60s. This paces HOW fast the loop chases the goals, not a target outcome; it is a per-dataset knob, not part of the global SLO surface."),
        ParameterSpec::runtime("cdc_prefetch_buffer")
            .description("Per-dataset override for the CDC source-reader prefetch channel depth (envelopes)."),
        ParameterSpec::runtime("cdc_max_coalesced_envelopes")
            .description("Per-dataset override for the maximum number of CDC envelopes coalesced into a single accelerator write."),
        ParameterSpec::runtime("cdc_max_coalesced_bytes")
            .description("Per-dataset override for the byte budget (in bytes) of a coalesced CDC burst."),
        ParameterSpec::runtime("cdc_max_coalesce_age_ms")
            .description("Per-dataset override for the linger window (ms) the CDC apply loop waits for additional envelopes before flushing."),
        ParameterSpec::runtime("cdc_commit_timeout_ms")
            .description("Per-dataset override for the CDC source-side commit timeout (ms)."),
    ],
);

#[async_trait]
impl DataAccelerator for CayenneAccelerator {
    async fn adaptive_tuning_seeds(
        &self,
        tuning: Option<&str>,
        data_path: &str,
        metastore_path: &str,
    ) -> data_accelerator_api::AdaptiveTuningOutcome {
        let (tuning_mode, tuning_value_invalid) = autotune::TuningMode::parse(tuning);
        if tuning_mode != autotune::TuningMode::Adaptive {
            return data_accelerator_api::AdaptiveTuningOutcome {
                tuning_value_invalid,
                seeds: None,
            };
        }

        // No `StorageProfile` override is plumbed on the catalog path, and a catalog has
        // no schema inference, so the seed comes from the detected hardware alone.
        let hardware = autotune::HardwareProfile::detect(
            runtime_acceleration::acceleration::StorageProfile::Auto,
            data_path,
            metastore_path,
        )
        .await;
        let caps = hardware.inline_flush_caps(&autotune::WorkloadProfile::default());

        data_accelerator_api::AdaptiveTuningOutcome {
            tuning_value_invalid,
            seeds: Some(data_accelerator_api::AdaptiveTuningSeeds {
                // A small-write cadence, so the controller has a tick to ride.
                compaction_background_interval_ms: 10_000,
                compaction_trigger_files: 4,
                inline_flush_max_rows: caps.max_rows,
                inline_flush_max_segments: caps.max_segments,
                inline_flush_max_bytes: caps.max_bytes,
                // The CPU entitlement, so the controller's [1, cores] window matches it.
                write_concurrency: hardware.cores,
            }),
        }
    }

    fn shared_store_key(
        &self,
        acceleration: &runtime_acceleration::acceleration::Acceleration,
    ) -> Option<String> {
        // Every Cayenne dataset in one metadata directory shares its SQLite catalog, so
        // the directory is the identity `validate_snapshot_consistency` groups by. Absent
        // this, that validation silently passes for every Cayenne dataset.
        Some(Self::resolve_metadata_dir(Some(acceleration)))
    }

    fn spicepod_write_profile(
        &self,
        acceleration: &spicepod::acceleration::Acceleration,
        unset_refresh_mode: runtime_acceleration::acceleration::RefreshMode,
    ) -> Option<data_accelerator_api::SpicepodWriteProfile> {
        // The contract is `None` unless the acceleration names this engine. The runtime
        // enumerates Cayenne accelerations before asking, so this is the implementation
        // holding up its own end: another consumer would otherwise get a confident
        // Cayenne classification for a DuckDB or Arrow acceleration.
        if !acceleration
            .engine
            .as_deref()
            .is_some_and(|engine| engine.eq_ignore_ascii_case("cayenne"))
        {
            return None;
        }

        let profile = RefreshWriteProfile::from_spicepod(acceleration, unset_refresh_mode);
        Some(data_accelerator_api::SpicepodWriteProfile {
            uses_cdc_tier: profile.uses_cdc_tier(),
            needs_compaction: profile.needs_compaction(),
            inlines_small_writes: profile.inlines_small_writes(),
        })
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "cayenne"
    }

    fn type_rewrite_rules(&self) -> arrow_tools::type_rewrite::TypeRewriteRules {
        cayenne::CAYENNE_TYPE_REWRITE_RULES
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
    /// Cayenne keeps its sidecar tables in the metastore database beside the dataset's
    /// Cayenne directory, not in the dataset's own store.
    ///
    /// When that metastore is Turso, the pool comes from the **Turso accelerator's**
    /// path-keyed cache rather than one built here: `cayenne.db` is opened by every
    /// sidecar of every Cayenne dataset in the pod, and the lock that serializes their
    /// DDL against each other's `BEGIN CONCURRENT` writes lives on the pool instance —
    /// a pool of our own would hold a lock no other sidecar observes.
    async fn sidecar(
        &self,
        source: &dyn AccelerationSource,
        registry: Arc<AcceleratorEngineRegistry>,
        open_option: OpenOption,
    ) -> Result<Arc<dyn AcceleratorSidecar>, CheckpointError> {
        {
            use datafusion_table_providers::sqlite::SqliteTableProviderFactory;

            // Resolving the data directory validates the acceleration configuration; the
            // metastore path below is derived independently of it.
            self.file_path(source)
                .map_err(|source| CheckpointError::Store {
                    source: Box::new(source),
                })?;

            let metadata_dir = Self::resolve_metadata_dir(source.acceleration());
            let metadata_db_path = format!("{metadata_dir}/cayenne.db");

            if open_option == OpenOption::OpenExisting
                && !std::path::Path::new(&metadata_db_path).exists()
            {
                return Err(CheckpointError::Store {
                    source: format!(
                        "Cayenne metadata directory does not exist at {metadata_db_path}"
                    )
                    .into(),
                });
            }

            if let Some(parent) = std::path::Path::new(&metadata_db_path).parent() {
                tokio::fs::create_dir_all(parent).await.map_err(|source| {
                    CheckpointError::Store {
                        source: Box::new(source),
                    }
                })?;
            }

            {
                let metastore_type = source
                    .acceleration()
                    .and_then(|a| a.params.get("cayenne_metastore"))
                    .map_or("sqlite", String::as_str);
                if metastore_type == "turso" {
                    let turso_engine = registry
                        .get_accelerator_engine(Engine::Turso)
                        .await
                        .ok_or_else(|| CheckpointError::Store {
                            source: "Turso accelerator engine not available".into(),
                        })?;
                    // Through the contract, not a downcast: the Turso engine owns the
                    // path-keyed pool whose lock serializes this sidecar's DDL, and asking
                    // it for the sidecar is what keeps this engine from naming that one.
                    return turso_engine
                        .sidecar_for_path(&metadata_db_path, &source.name().to_string())
                        .await;
                }
            }

            let sqlite_factory = SqliteTableProviderFactory::new();
            let pool = sqlite_factory
                .get_or_init_instance(
                    Arc::from(metadata_db_path.as_str()),
                    datafusion_table_providers::sql::db_connection_pool::Mode::File,
                    std::time::Duration::from_secs(5),
                )
                .await
                .map_err(|source| CheckpointError::Store {
                    source: Box::new(source),
                })?;

            Ok(Arc::new(SqliteSidecar::new(
                Arc::new(pool),
                source.name().to_string(),
            )))
        }
    }

    async fn init(
        &self,
        source: &dyn AccelerationSource,
    ) -> Result<BootstrapStatus, Box<dyn std::error::Error + Send + Sync>> {
        if !source.is_file_accelerated() {
            // Memory mode (`mode: memory`) is fully in-RAM and ephemeral — there is
            // nothing to bootstrap on disk; the dataset reloads from its federated
            // source on startup, like the in-memory Arrow accelerator.
            return Ok(BootstrapStatus::none());
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

        // Fail here rather than at teardown, when the operator is already committed and
        // the delete has nothing left to protect — and before the recreate drops this
        // dataset's catalog rows, so a refusal never leaves them gone with the files
        // still on disk.
        Self::ensure_metastore_outside_data_dir(source, &dir_path).await?;
        Self::ensure_no_catalog_under_data_dir(source, &dir_path).await?;

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
                data_accelerator_api::snapshots::snapshot_before_recreate(
                    acceleration,
                    &source.name().to_string(),
                    snapshot_layout,
                    AccelerationEngine::Cayenne,
                    Arc::new(arrow_schema::Schema::empty()),
                    // The outgoing snapshot becomes the store's current snapshot, so
                    // it has to be one a Cayenne bootstrap can consume:
                    // `CayenneSnapshotEngine::finalize_directory_snapshot` requires the
                    // per-dataset metastore slice, and the default engine archives a
                    // raw `cayenne.db` without one. This exports the slice while the
                    // dataset's table metadata is still in the catalog — the drop below
                    // removes it. If the catalog is unavailable this resolves to `None`
                    // and `snapshot_before_recreate` skips the snapshot rather than
                    // publish an archive nothing can restore.
                    self.snapshot_engine_for_source(source).await,
                    resolved_refresh_mode(source, acceleration),
                )
                .await;
            }

            // Metadata before files, and its failures are fatal — the same
            // ordering `drop_table` uses, and for the same reason. Deleting the
            // directory first and then continuing past a failed catalog drop
            // leaves rows describing files that are gone, and for a partitioned
            // table leaves the per-partition children whose stale schemas this
            // rebuild exists to discard. Failing first leaves an acceleration
            // the operator can retry.
            let metadata_dir = Self::resolve_metadata_dir(Some(acceleration));

            let metastore_type = acceleration
                .params
                .get("cayenne_metastore")
                .map_or("sqlite", String::as_str);

            let table_name = source.name().to_string();
            let catalog = self
                .get_or_create_catalog(&metadata_dir, metastore_type)
                .await
                .boxed()
                .context(AccelerationInitializationFailedSnafu)?;
            if catalog
                .drop_table(&table_name)
                .await
                .boxed()
                .context(AccelerationInitializationFailedSnafu)?
            {
                tracing::info!(
                    "Dropped existing Cayenne table metadata for '{table_name}' (file_create mode)"
                );
            }

            if path_buf.exists() {
                // The proofs run here rather than earlier because
                // `snapshot_before_recreate` creates both directories, so an overlap
                // only a symlink reveals is resolvable now even though the open-time
                // check above had nothing to canonicalize.
                Self::remove_acceleration_data_dir(source, &dir_path).await?;
                tracing::warn!(
                    "Cayenne acceleration mode is 'file_create', removed existing directory: {}",
                    dir_path
                );
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
                Ok(catalog) => Some(Arc::new(crate::snapshot_engine::CayenneSnapshotEngine::new(
                    catalog,
                    source.name().to_string(),
                    path_buf.clone(),
                ))
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
                resolved_refresh_mode(source, acceleration),
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

        // Memory mode (`mode: memory`) writes no data files, so it needs no storage
        // directory: derive a (never-written) base path and skip directory creation.
        // File mode resolves and creates the data dir as before.
        let memory_mode = !source.is_file_accelerated();
        // Memory mode is non-partitioned only: `is_memory_resident_mode()` (the
        // predicate the write/scan paths consult) requires no partition column, so a
        // partitioned memory table would fall through to the durable Vortex path and
        // silently write to disk. Reject it up front rather than half-configuring an
        // on-disk partitioned table.
        if memory_mode && !partition_by.is_empty() {
            return Err(Box::new(Error::InvalidConfiguration {
                detail: Arc::from(
                    "Cayenne mode: memory is not supported with partitioning. Remove partition_by, or use mode: file for a partitioned accelerator.",
                ),
            }));
        }
        let dir_path = if memory_mode {
            Self::resolve_default_data_path(&source.name().to_string().replace(['.', '/'], "_"))
        } else {
            let dir_path = self.resolve_storage_config(source).boxed()?;
            let _ = Self::ensure_directory(&dir_path).boxed()?;
            dir_path
        };
        let arrow_schema = Self::transformed_arrow_schema(&cmd, source).boxed()?;

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
                    match runtime_datafusion::retention_sql::parse_retention_sql(
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
            ))
            .into_table() as Arc<dyn TableProvider>;

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
            // Honor the configured `cayenne_metastore` backend (Turso uses the
            // `libsql://` scheme) rather than hardcoding SQLite. The unpartitioned
            // path (`get_or_create_catalog`) already selects the scheme from this
            // param; without the same logic here, partitioned tables silently
            // ignore `cayenne_metastore: turso` and fall back to SQLite.
            let metastore_type = source
                .acceleration()
                .and_then(|a| a.params.get("cayenne_metastore"))
                .map_or("sqlite", String::as_str);
            let catalog_connection_string = match metastore_type {
                "turso" => format!("libsql://{metadata_dir}/cayenne.db"),
                _ => format!("sqlite://{metadata_dir}/cayenne.db"),
            };
            let catalog_concrete: Arc<cayenne::CayenneCatalog> = Arc::new(
                cayenne::CayenneCatalog::new(catalog_connection_string)
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
                source,
                source.acceleration(),
                arrow_schema.as_ref(),
                &primary_keys,
                on_conflict.as_ref(),
            );
            let mut vortex_config = Self::get_vortex_config_with_footer_cache(
                &table_name,
                source,
                self.footer_cache_mb,
                &workload,
            )
            .await?;
            // Partitioned tables are excluded from v1 schema evolution
            // (per-partition catalog tables would evolve lazily as each
            // partition opens, leaving mixed schemas across partitions); keep
            // the legacy pin-stored-schema behavior. The config already arrives
            // Disabled for a partitioned table — see where `schema_evolution` is
            // built, which has to exclude it there so the parent open cannot
            // widen either — so this only tells an operator whose
            // `on_schema_change` asked for evolution that it will not apply.
            debug_assert!(
                vortex_config.schema_evolution.is_disabled(),
                "a partitioned Cayenne table must never carry an evolution mode"
            );
            if requests_schema_evolution(source) {
                tracing::warn!(
                    dataset = %source.name(),
                    "on_schema_change schema evolution is not supported for partitioned Cayenne tables; schema changes will not be applied in place"
                );
            }

            serialize_partition_child_writes(&mut vortex_config, &table_name);

            // Log S3 Express configuration for partitioned tables
            if is_s3_express {
                tracing::info!(
                    "Cayenne acceleration for {} configured with S3 Express One Zone storage (target file size: {} MB)",
                    table_name,
                    vortex_config.target_vortex_file_size_mb
                );
            }

            let creator = Arc::new(
                CayennePartitionCreator::new(
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
                )
                // Per-partition background compaction draws on the accelerator's
                // one budget, and an accelerated partitioned table is a target for
                // the dual-write path.
                .with_background_compaction(Arc::clone(&self.compaction_semaphore))
                .with_direct_partition_writes(),
            );

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
            let partition_provider =
                PartitionTableProvider::new(creator, partition_by, Arc::clone(&arrow_schema))
                    .await
                    .boxed()
                    .context(AccelerationCreationFailedSnafu)?;
            let partition_table_providers = partition_provider.partition_table_providers().await;
            insert_strategy
                .recover_partitioned_wals(&partition_table_providers)
                .await
                .boxed()
                .context(AccelerationCreationFailedSnafu)?;
            let partition_provider =
                Arc::new(partition_provider.with_insert_strategy(insert_strategy));

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
            ))
            .into_table() as Arc<dyn TableProvider>;

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
            crate::snapshot_engine::CayenneSnapshotEngine::new(
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
        provider_factory: data_accelerator_api::ReloadProviderFactory,
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

        // A schema rebuild reaches this without going through `init`, so the
        // open-time check is not on this path's stack. Refuse before touching
        // the catalog: an overlapping metastore is a configuration the whole
        // rebuild must reject, not just the delete below — opening the nested
        // path as a catalog would fail confusingly (or worse, mutate it) first.
        if path_buf.exists() {
            Self::ensure_metastore_outside_data_dir(source, &dir_path).await?;
            Self::ensure_no_catalog_under_data_dir(source, &dir_path).await?;
        }

        // Metadata first, and its failures are fatal. The caller treats a
        // successful drop as licence to clear the dataset checkpoint and
        // recreate, so a drop that removed the files and then failed to remove
        // the catalog rows would hand the next create a manifest of files that
        // no longer exist and, for a partitioned table, children still pinning
        // the old schema. Failing before anything is deleted leaves a table the
        // operator can retry; the reverse order leaves one nothing can repair.
        if let Some(acceleration) = source.acceleration() {
            let metadata_dir = Self::resolve_metadata_dir(Some(acceleration));
            let metastore_type = acceleration
                .params
                .get("cayenne_metastore")
                .map_or("sqlite", String::as_str);
            let catalog = self
                .get_or_create_catalog(&metadata_dir, metastore_type)
                .await?;
            catalog.drop_table(table_name).await.boxed()?;
        }

        if path_buf.exists() {
            Self::remove_acceleration_data_dir(source, &dir_path).await?;
            tracing::info!(
                "Removed Cayenne data directory '{dir_path}' for schema recreation (file_update mode)"
            );
        }

        // Recreate the data directory so the next create_external_table works
        tokio::fs::create_dir_all(&path_buf).await.boxed()?;
        Ok(())
    }

    /// Widening schema evolution for an existing Cayenne table: persist the
    /// evolved schema to the metastore so the table provider (re)opens with
    /// it. Existing Vortex data files are NOT rewritten — they are
    /// self-describing and the scan adapts old files to the evolved schema at
    /// read time (missing-column null-fill + widened-type cast).
    ///
    /// Idempotent: re-applying a plan whose evolved schema is already stored
    /// is a no-op, so a crash between this engine update and the checkpoint
    /// update self-heals via restart re-classification.
    ///
    /// Refused for a partitioned table: see [`PartitionedEvolutionUnsupported`].
    ///
    /// [`PartitionedEvolutionUnsupported`]: Error::PartitionedEvolutionUnsupported
    async fn evolve_table_schema(
        &self,
        table_name: &str,
        source: &dyn AccelerationSource,
        plan: &arrow_tools::schema_evolution::WideningPlan,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        use arrow_tools::schema_evolution::{EvolutionContext, SchemaEvolution, classify};

        let acceleration = source.acceleration().ok_or_else(|| {
            Box::new(Error::AccelerationNotEnabled {
                dataset: Arc::from(source.name().to_string()),
            }) as Box<dyn std::error::Error + Send + Sync>
        })?;

        // A partitioned table keeps one Vortex table per partition, each with its
        // own stored schema, and this method can only reach the parent catalog
        // entry. Evolving the parent alone would report success while every
        // partition still holds the old schema — so writes narrow-cast into them
        // (silently losing precision) and the caller advances the dataset
        // checkpoint past a change that was never applied, which makes every
        // later restart classify source and checkpoint as identical and leaves
        // the dataset permanently stuck. Refusing hands the change to the
        // caller's fallback, which honors `mode: file_update` and
        // `on_schema_change: drop_and_recreate` by recreating the table with the
        // new schema.
        //
        // NOTE: this check belongs here and not in
        // `engine_supports_in_place_evolution`, which `engine_supports_recreate`
        // delegates to — excluding partitioned Cayenne there would also
        // disqualify it from the recreate that makes this path work.
        if !acceleration.partition_by.is_empty() {
            return Err(Box::new(Error::PartitionedEvolutionUnsupported {
                dataset: Arc::from(source.name().to_string()),
            }));
        }

        let metadata_dir = Self::resolve_metadata_dir(Some(acceleration));
        let metastore_type = acceleration
            .params
            .get("cayenne_metastore")
            .map_or("sqlite", String::as_str);
        let catalog = self
            .get_or_create_catalog(&metadata_dir, metastore_type)
            .await?;
        let table = catalog.get_table(table_name).await.boxed()?;

        // The metastore stores the Vortex-transformed schema (unsupported
        // types may have been converted), so the evolved schema must go
        // through the same transform before comparison/persistence.
        let unsupported_type_action = Self::get_unsupported_type_action(source);
        let evolved: SchemaRef = Arc::new(transform_schema_for_vortex(
            plan.evolved_schema.as_ref(),
            unsupported_type_action,
        )?);

        if table.schema.as_ref() == evolved.as_ref() {
            return Ok(());
        }

        // Re-classify against the STORED schema: re-applies the constraint
        // guard (Cayenne persists typed PK row-encodings that cannot be
        // widened in place) and rejects stale/foreign plans.
        let ctx = EvolutionContext {
            constraint_columns: &table.primary_key,
        };
        match classify(&table.schema, &evolved, &ctx) {
            SchemaEvolution::Widening(_) => {}
            // Reorder/nullability-tighten-only: the stored schema stays canonical.
            SchemaEvolution::Identical => return Ok(()),
            SchemaEvolution::Incompatible { reason } => {
                return Err(Box::new(Error::InvalidConfiguration {
                    detail: Arc::from(format!(
                        "Cannot evolve Cayenne schema for '{table_name}' in place: {reason}"
                    )),
                }));
            }
        }

        catalog
            .update_table_schema(&table.table_id, &evolved)
            .await
            .boxed()?;
        tracing::info!(
            dataset = %source.name(),
            "Evolved Cayenne table schema: {}",
            plan.describe()
        );
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

/// Whether the source's `on_schema_change` asks for in-place schema evolution.
///
/// A source that states no policy (a view, or DDL) never asks for it.
fn requests_schema_evolution(source: &dyn AccelerationSource) -> bool {
    source.on_schema_change().is_some_and(|on_schema_change| {
        matches!(
            on_schema_change,
            OnSchemaChange::AppendNewColumns
                | OnSchemaChange::SyncAllColumns
                | OnSchemaChange::DropAndRecreate
        )
    })
}

/// Force partition child tables to encode serially (one write shard).
///
/// Two reasons, both specific to partitioned datasets:
///
/// 1. **Parallelism already comes from the partition fan-out.** The insert
///    path runs one concurrent insert task per partition
///    (`runtime_table_partition::insert`), so intra-write encode sharding
///    would multiply the aggregate encode-shard count by the partition count.
/// 2. **Child writes bypass the global encode budget, so they must stay
///    serial.** The per-partition insert tasks are coupled through one
///    routing demux over bounded channels; a child write parked on the encode
///    budget stalls the demux, starving the permit-holding sibling writes of
///    input — a hold-and-wait cycle that left partitioned tables permanently
///    unready (spiceai/spiceai#11818). Child tables are therefore created as
///    coupled writers (`CayenneContext::new_for_partition_child`), exempt
///    from the budget (see `cayenne::provider::write_budget`) — and an
///    unmetered writer must contribute the minimum encode footprint, one
///    shard, which this clamp guarantees.
///
/// An operator-pinned `cayenne_write_concurrency > 1` is IGNORED (clamped to
/// 1, with a warning), like schema evolution above: there is no safe way to
/// honor it — child writes are budget-exempt, so a multi-shard child would
/// fan out unmetered, multiplied by a partition count that isn't statically
/// bounded (time-based partitions grow indefinitely).
fn serialize_partition_child_writes(
    config: &mut cayenne::metadata::VortexConfig,
    table_name: &str,
) {
    if config.pinned_tuning_actuators.write_concurrency && config.write_concurrency.unwrap_or(1) > 1
    {
        tracing::warn!(
            dataset = table_name,
            write_concurrency = ?config.write_concurrency,
            "cayenne_write_concurrency is not supported for partitioned Cayenne tables (partition writes bypass the global encode budget, so intra-partition sharding would fan out unmetered); writing each partition serially instead"
        );
    }
    config.write_concurrency = Some(1);
}

data_accelerator_api::register_data_accelerator!(Engine::Cayenne, CayenneAccelerator);

#[cfg(test)]
mod tests {
    use super::*;
    use runtime_acceleration::OnSchemaChange;
    use runtime_acceleration::testing::TestAccelerationSource;

    /// A timestamp column compared against a string literal is how such a filter
    /// is normally written, and it must survive all the way to *evaluation*.
    /// Planning it is not enough: without coercion the physical expression builds
    /// fine and then fails on the first batch with "Invalid comparison operation:
    /// Timestamp(µs) > Utf8", which takes the maintained aggregate stale on its
    /// first delta and silently drops every query back to a base-table scan.
    #[test]
    fn a_timestamp_filter_against_a_string_literal_evaluates() {
        use arrow::array::{Int32Array, TimestampMicrosecondArray};
        use arrow::datatypes::TimeUnit;
        use arrow::record_batch::RecordBatch;

        let schema = Schema::new(vec![
            Field::new(
                "ol_delivery_d",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
            Field::new("ol_quantity", DataType::Int32, true),
        ]);

        let filter = parse_maintained_aggregate_filter(
            "ol_delivery_d > '2007-01-02 00:00:00.000000'",
            &schema,
        )
        .expect("a timestamp-vs-string filter must be accepted");

        // 2007-01-01 (before the bound) and 2008-01-01 (after it).
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(TimestampMicrosecondArray::from(vec![
                    1_167_609_600_000_000,
                    1_199_145_600_000_000,
                ])),
                Arc::new(Int32Array::from(vec![1, 2])),
            ],
        )
        .expect("test batch");

        // The assertion that matters: evaluating does not error.
        let evaluated = filter
            .evaluate(&batch)
            .expect("the filter must evaluate, not just plan");
        let mask = evaluated
            .into_array(batch.num_rows())
            .expect("filter yields an array");
        let mask = mask
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .expect("a WHERE predicate evaluates to Boolean");
        assert!(!mask.value(0), "2007-01-01 is not after the bound");
        assert!(mask.value(1), "2008-01-01 is after the bound");
    }

    /// In memory mode an unset mem-tier cap means "no cap", and `auto` asks for
    /// that same derived default — so it must not be mistaken for an operator's
    /// explicit hard limit, which memory mode (no spill path) enforces by failing
    /// the write.
    #[test]
    fn memory_mode_treats_an_auto_mem_tier_cap_as_unset() {
        let mut config = cayenne::metadata::VortexConfig {
            cdc_mem_tier_max_bytes: 512 * 1024 * 1024,
            ..Default::default()
        };
        let auto = Acceleration {
            params: [(
                "cayenne_cdc_mem_tier_max_bytes".to_string(),
                "AUTO".to_string(),
            )]
            .into_iter()
            .collect(),
            ..Default::default()
        };
        CayenneAccelerator::apply_memory_mode_overrides(&mut config, Some(&auto));
        assert_eq!(
            config.cdc_mem_tier_max_bytes, 0,
            "`auto` resolves identically to omitting the knob (no cap)"
        );

        let mut config = cayenne::metadata::VortexConfig::default();
        let pinned = Acceleration {
            params: [(
                "cayenne_cdc_mem_tier_max_bytes".to_string(),
                "1048576".to_string(),
            )]
            .into_iter()
            .collect(),
            ..Default::default()
        };
        CayenneAccelerator::apply_memory_mode_overrides(&mut config, Some(&pinned));
        assert_ne!(
            config.cdc_mem_tier_max_bytes, 0,
            "a number is still an explicit hard limit"
        );

        // A value the `i64` config field cannot hold reads as set (knobs parse as
        // `usize`) but never reaches the config — `auto_or_i64` has already fallen
        // back to the derived value — so it must not pin a cap either.
        let mut config = cayenne::metadata::VortexConfig {
            cdc_mem_tier_max_bytes: 512 * 1024 * 1024,
            ..Default::default()
        };
        let overflowing = Acceleration {
            params: [(
                "cayenne_cdc_mem_tier_max_bytes".to_string(),
                u64::MAX.to_string(),
            )]
            .into_iter()
            .collect(),
            ..Default::default()
        };
        CayenneAccelerator::apply_memory_mode_overrides(&mut config, Some(&overflowing));
        assert_eq!(
            config.cdc_mem_tier_max_bytes, 0,
            "a value too large for the config field is not an explicit limit"
        );
    }
    use app::AppBuilder;
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion_table_providers::UnsupportedTypeAction;
    use runtime_acceleration::acceleration::{Acceleration, Mode, RefreshMode};
    use search::index::{SearchIndex, VectorIndex};
    use std::sync::Arc;

    /// Partition child tables default to serial writes (`write_concurrency: 1`):
    /// multi-shard child writes can deadlock the partition insert demux against
    /// the global encode budget (spiceai/spiceai#11818).
    #[test]
    fn partition_child_writes_default_to_serial() {
        let mut config = cayenne::metadata::VortexConfig {
            write_concurrency: None,
            ..Default::default()
        };
        serialize_partition_child_writes(&mut config, "t");
        assert_eq!(config.write_concurrency, Some(1));

        // Auto-tuned (unpinned) values are overridden too.
        let mut config = cayenne::metadata::VortexConfig {
            write_concurrency: Some(8),
            ..Default::default()
        };
        serialize_partition_child_writes(&mut config, "t");
        assert_eq!(config.write_concurrency, Some(1));
    }

    /// An operator-pinned `cayenne_write_concurrency > 1` is clamped to serial
    /// for partition children — safety depends on the host-sized encode budget
    /// and the (unbounded) partition count, so it cannot be honored safely.
    #[test]
    fn partition_child_writes_clamp_pinned_write_concurrency() {
        let mut config = cayenne::metadata::VortexConfig {
            write_concurrency: Some(4),
            ..Default::default()
        };
        config.pinned_tuning_actuators.write_concurrency = true;
        serialize_partition_child_writes(&mut config, "t");
        assert_eq!(config.write_concurrency, Some(1));
    }

    #[test]
    fn resolve_goal_raw_global_default_then_per_dataset_override() {
        use std::collections::HashMap;

        // Global SLOs set once at runtime.params.
        let global: HashMap<String, String> = [
            ("cayenne_goal_freshness".to_string(), "10s".to_string()),
            (
                "cayenne_goal_replication_lag".to_string(),
                "30s".to_string(),
            ),
        ]
        .into_iter()
        .collect();

        // Dataset overrides freshness, inherits the global replication_lag, and
        // sets a latency goal that the global doesn't have.
        let accel = Acceleration {
            params: [
                ("cayenne_goal_freshness".to_string(), "2s".to_string()),
                (
                    "cayenne_goal_query_latency".to_string(),
                    "250ms".to_string(),
                ),
            ]
            .into_iter()
            .collect(),
            ..Default::default()
        };

        // Per-dataset value wins over the global default.
        assert_eq!(
            resolve_goal_raw(&accel, &global, "cayenne_goal_freshness"),
            Some("2s")
        );
        // Unset on the dataset → inherits the global default.
        assert_eq!(
            resolve_goal_raw(&accel, &global, "cayenne_goal_replication_lag"),
            Some("30s")
        );
        // Set only on the dataset (no global) → the dataset value.
        assert_eq!(
            resolve_goal_raw(&accel, &global, "cayenne_goal_query_latency"),
            Some("250ms")
        );
        // Set nowhere → None (legacy "no goal"; controller stays signal-driven).
        assert_eq!(resolve_goal_raw(&accel, &global, "cayenne_goal_qph"), None);
    }

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
                filter_sql: None,
            }]
            .into(),
            ..Default::default()
        }
    }

    fn maintained_aggregate_test_schema() -> Schema {
        Schema::new(vec![
            arrow_schema::Field::new("customer_id", DataType::Int64, false),
            arrow_schema::Field::new("amount", DataType::Int64, true),
        ])
    }

    #[test]
    fn maintained_aggregate_specs_convert_for_unpartitioned_cayenne() {
        let acceleration = maintained_aggregate_acceleration();

        let specs = maintained_aggregate_specs_for_cayenne(
            Some(&acceleration),
            &maintained_aggregate_test_schema(),
            &[],
        )
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
    fn maintained_aggregate_specs_convert_min_max() {
        let acceleration = Acceleration {
            maintained_aggregates: vec![spicepod_acceleration::MaintainedAggregate {
                group_by: vec!["customer_id".to_string()],
                aggregates: vec![
                    spicepod_acceleration::MaintainedAggregateExpr {
                        function: spicepod_acceleration::MaintainedAggregateFunction::Min,
                        column: Some("amount".to_string()),
                    },
                    spicepod_acceleration::MaintainedAggregateExpr {
                        function: spicepod_acceleration::MaintainedAggregateFunction::Max,
                        column: Some("amount".to_string()),
                    },
                ],
                filter_sql: None,
            }]
            .into(),
            ..Default::default()
        };

        let specs = maintained_aggregate_specs_for_cayenne(
            Some(&acceleration),
            &maintained_aggregate_test_schema(),
            &["customer_id".to_string()],
        )
        .expect("min/max maintained aggregate config should convert");

        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].aggregates.len(), 2);
        assert_eq!(
            specs[0].aggregates[0].function,
            cayenne::maintained_aggregate::MaintainedAggregateFunction::Min
        );
        assert_eq!(
            specs[0].aggregates[1].function,
            cayenne::maintained_aggregate::MaintainedAggregateFunction::Max
        );
        assert_eq!(specs[0].aggregates[0].column.as_deref(), Some("amount"));
    }

    #[test]
    fn maintained_aggregate_specs_reject_min_max_without_primary_key() {
        let acceleration = Acceleration {
            maintained_aggregates: vec![spicepod_acceleration::MaintainedAggregate {
                group_by: Vec::new(),
                aggregates: vec![spicepod_acceleration::MaintainedAggregateExpr {
                    function: spicepod_acceleration::MaintainedAggregateFunction::Min,
                    column: Some("amount".to_string()),
                }],
                filter_sql: None,
            }]
            .into(),
            ..Default::default()
        };

        let error = maintained_aggregate_specs_for_cayenne(
            Some(&acceleration),
            &maintained_aggregate_test_schema(),
            &[],
        )
        .expect_err("min/max without a primary key must be rejected");

        let Error::InvalidConfiguration { detail } = error else {
            panic!("expected InvalidConfiguration, got {error:?}");
        };
        assert!(detail.contains("MIN/MAX"));
        assert!(detail.contains("primary key"));
    }

    #[test]
    fn maintained_aggregate_specs_empty_when_maintenance_disabled() {
        let mut acceleration = maintained_aggregate_acceleration();
        acceleration.maintained_aggregates = spicepod_acceleration::MaintainedAggregates::new(
            spicepod_acceleration::MaintainAggregates::Disabled,
            acceleration.maintained_aggregates.as_slice().to_vec(),
        );

        let specs = maintained_aggregate_specs_for_cayenne(
            Some(&acceleration),
            &maintained_aggregate_test_schema(),
            &[],
        )
        .expect("disabled maintained aggregate config should parse");

        assert!(specs.is_empty());
    }

    #[test]
    fn maintained_aggregate_specs_error_for_partitioned_cayenne() {
        let mut acceleration = maintained_aggregate_acceleration();
        acceleration.partition_by = vec![spicepod::partitioning::PartitionedBy {
            name: "region".to_string(),
            expression: "region".to_string(),
        }];

        let error = maintained_aggregate_specs_for_cayenne(
            Some(&acceleration),
            &maintained_aggregate_test_schema(),
            &[],
        )
        .expect_err("partitioned maintained aggregate config should be rejected");

        let Error::InvalidConfiguration { detail } = error else {
            panic!("expected InvalidConfiguration, got {error:?}");
        };
        assert!(detail.contains("maintained_aggregates"));
        assert!(detail.contains("partitioned"));
    }

    fn maintained_aggregate_acceleration_with_filter(filter: &str) -> Acceleration {
        let mut views = maintained_aggregate_acceleration()
            .maintained_aggregates
            .as_slice()
            .to_vec();
        views[0].filter_sql = Some(filter.to_string());
        Acceleration {
            maintained_aggregates: views.into(),
            ..Default::default()
        }
    }

    #[test]
    fn maintained_aggregate_specs_parse_config_filter() {
        let acceleration = maintained_aggregate_acceleration_with_filter("amount > 100");

        let specs = maintained_aggregate_specs_for_cayenne(
            Some(&acceleration),
            &maintained_aggregate_test_schema(),
            &[],
        )
        .expect("a valid maintained aggregate filter should convert");

        assert_eq!(specs.len(), 1);
        assert!(
            specs[0].filter.is_some(),
            "the config filter must be parsed onto the maintained aggregate spec"
        );
    }

    #[test]
    fn maintained_aggregate_specs_reject_unknown_filter_column() {
        let acceleration = maintained_aggregate_acceleration_with_filter("nonexistent_column > 1");

        let error = maintained_aggregate_specs_for_cayenne(
            Some(&acceleration),
            &maintained_aggregate_test_schema(),
            &[],
        )
        .expect_err("a filter referencing an unknown column must be rejected");

        let Error::InvalidConfiguration { detail } = error else {
            panic!("expected InvalidConfiguration, got {error:?}");
        };
        assert!(
            detail.contains("filter"),
            "the error must identify the maintained-aggregate filter: {detail}"
        );
    }

    #[test]
    fn maintained_aggregate_specs_reject_non_boolean_filter() {
        // `amount` alone is an Int64 column, not a `WHERE` predicate.
        let acceleration = maintained_aggregate_acceleration_with_filter("amount");

        let error = maintained_aggregate_specs_for_cayenne(
            Some(&acceleration),
            &maintained_aggregate_test_schema(),
            &[],
        )
        .expect_err("a non-Boolean filter must be rejected at config time");

        let Error::InvalidConfiguration { detail } = error else {
            panic!("expected InvalidConfiguration, got {error:?}");
        };
        assert!(
            detail.contains("Boolean"),
            "the error must say the filter must be a Boolean predicate: {detail}"
        );
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

    /// The write profile is the engine's answer about its *own* acceleration, so an
    /// acceleration naming another engine must get `None` rather than a Cayenne
    /// classification. The runtime filters by engine before asking, so only this test
    /// stands between a second consumer and a silently wrong budget.
    #[test]
    fn the_write_profile_is_answered_only_for_a_cayenne_acceleration() {
        let accelerator = CayenneAccelerator::new();
        let named = |engine: Option<&str>| spicepod::acceleration::Acceleration {
            engine: engine.map(ToString::to_string),
            mode: spicepod::acceleration::Mode::File,
            ..Default::default()
        };

        assert!(
            accelerator
                .spicepod_write_profile(&named(Some("cayenne")), RefreshMode::Full)
                .is_some(),
            "the engine must classify its own acceleration"
        );
        assert!(
            accelerator
                .spicepod_write_profile(&named(Some("CAYENNE")), RefreshMode::Full)
                .is_some(),
            "the engine name is matched the way the runtime matches it: case-insensitively"
        );

        // `None` is the default Arrow engine, not an unspecified Cayenne one.
        for other in [Some("duckdb"), Some("arrow"), Some("sqlite"), None] {
            assert!(
                accelerator
                    .spicepod_write_profile(&named(other), RefreshMode::Full)
                    .is_none(),
                "engine {other:?} is not Cayenne, so this engine must not classify it"
            );
        }
    }

    #[tokio::test]
    async fn unset_cdc_refresh_mode_keeps_background_compaction() {
        // Keyed by CONNECTOR NAME rather than a raw `from:` value: parsing one is
        // `DatasetSpec::source`'s job, and that the two agree is asserted by
        // `the_two_routes_to_an_unset_refresh_mode_agree`, which lives with the runtime
        // because it needs both sides.
        let interval_for = |connector: &str| {
            let mut dataset = TestAccelerationSource::new("ds").with_connector_name(connector);
            dataset.set_acceleration(Acceleration {
                engine: Engine::Cayenne,
                mode: Mode::File,
                // No `refresh_mode`: the connector fills it in.
                ..Default::default()
            });
            let acceleration = dataset.acceleration().expect("acceleration set");
            let mut config = cayenne::metadata::VortexConfig::default();
            apply_refresh_mode_defaults(
                &mut config,
                &dataset,
                acceleration,
                autotune::InlineFlushCaps::FLOOR,
            );
            config.compaction_background_interval_ms
        };

        assert_eq!(
            interval_for("debezium"),
            SMALL_WRITE_COMPACTION_BACKGROUND_INTERVAL_MS,
            "an unannotated debezium dataset is a CDC stream and keeps the tight compaction cadence"
        );
        assert_ne!(
            interval_for("cdc"),
            0,
            "an unannotated cdc dataset must keep a background compactor"
        );
        assert_eq!(
            interval_for("s3"),
            0,
            "a whole-table replace has nothing to consolidate, so the compactor stays off"
        );
    }

    #[tokio::test]
    async fn test_cayenne_file_path_generation() {
        let app = AppBuilder::new("test").build();

        let mut dataset =
            TestAccelerationSource::new("cayenne_data_accelerator_test").with_app(app);

        dataset.set_acceleration(Acceleration {
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

        let mut dataset = TestAccelerationSource::new("orders.dataset").with_app(app);

        dataset.set_acceleration(Acceleration {
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

    /// The stock defaults collide with no operator error at all: a dataset literally
    /// named `metadata` resolves its data directory onto the shared metastore
    /// directory, so recreating it unlinks the catalog for every other Cayenne dataset
    /// in the instance. Regression test for #13055 / #13068.
    #[tokio::test]
    async fn overlap_detects_the_default_collision_for_a_dataset_named_metadata() {
        let metastore = CayenneAccelerator::resolve_metadata_dir(None);

        let colliding = CayenneAccelerator::resolve_default_data_path("metadata");
        assert!(
            overlapping_metastore_dir(&colliding, &metastore)
                .await
                .expect("the test paths resolve")
                .is_some(),
            "a dataset named `metadata` puts its data directory on top of the metastore: \
             data={colliding} metastore={metastore}"
        );

        let ordinary = CayenneAccelerator::resolve_default_data_path("orders");
        assert!(
            overlapping_metastore_dir(&ordinary, &metastore)
                .await
                .expect("the test paths resolve")
                .is_none(),
            "an ordinary dataset name is disjoint from the metastore: \
             data={ordinary} metastore={metastore}"
        );
    }

    /// The containment test compares path *components*: `…/data/meta` must not be
    /// read as containing `…/data/metadata` just because it is a string prefix.
    #[tokio::test]
    async fn overlap_ignores_a_sibling_directory_sharing_a_name_prefix() {
        let base = tempfile::tempdir().expect("temp dir");
        let data = base.path().join("meta");
        let metastore = base.path().join("metadata");

        assert!(
            overlapping_metastore_dir(&data.to_string_lossy(), &metastore.to_string_lossy())
                .await
                .expect("the test paths resolve")
                .is_none(),
            "`meta` and `metadata` are siblings, not nested"
        );
    }

    /// An explicit `cayenne_metadata_dir` may legally point anywhere, including
    /// beneath a dataset's data directory — where the recreate deletes it.
    #[tokio::test]
    async fn overlap_detects_an_explicit_metadata_dir_nested_under_the_data_dir() {
        let base = tempfile::tempdir().expect("temp dir");
        let data = base.path().join("orders");

        let nested = data.join("catalog");
        assert!(
            overlapping_metastore_dir(&data.to_string_lossy(), &nested.to_string_lossy())
                .await
                .expect("the test paths resolve")
                .is_some(),
            "a metadata dir beneath the data dir is deleted with it"
        );

        let outside = base.path().join("catalog");
        assert!(
            overlapping_metastore_dir(&data.to_string_lossy(), &outside.to_string_lossy())
                .await
                .expect("the test paths resolve")
                .is_none(),
            "a metadata dir outside the data dir survives the recreate"
        );
    }

    /// Neither `..` nor a symlinked ancestor may hide an overlap — a purely lexical
    /// comparison misses both, and `remove_dir_all` follows neither before deleting.
    #[tokio::test]
    async fn overlap_resolves_dot_dot_and_symlinks_before_comparing() {
        let base = tempfile::tempdir().expect("temp dir");
        let data = base.path().join("orders");
        std::fs::create_dir_all(&data).expect("data dir");

        let traversed = data.join("sibling").join("..").join("catalog");
        assert!(
            overlapping_metastore_dir(&data.to_string_lossy(), &traversed.to_string_lossy())
                .await
                .expect("the test paths resolve")
                .is_some(),
            "`orders/sibling/../catalog` is `orders/catalog`, which the delete takes"
        );

        // A symlink to the data directory resolves onto it, so a metadata dir reached
        // through the link is inside the tree the delete walks.
        #[cfg(unix)]
        {
            let link = base.path().join("link-to-orders");
            std::os::unix::fs::symlink(&data, &link).expect("symlink");
            let through_link = link.join("catalog");
            assert!(
                overlapping_metastore_dir(&data.to_string_lossy(), &through_link.to_string_lossy())
                    .await
                    .expect("the test paths resolve")
                    .is_some(),
                "a metadata dir reached through a symlink to the data dir is still inside it"
            );
        }
    }

    /// `..` names the parent of what the preceding component *resolves to*. With
    /// `link -> {base}/data/subdir`, `link/../catalog` is `{base}/data/catalog` — inside
    /// the data directory — but collapsing `..` before resolving `link` yields
    /// `{base}/catalog` and lets the delete through. Raised by Copilot on #13101.
    #[cfg(unix)]
    #[tokio::test]
    async fn overlap_applies_dot_dot_after_the_symlink_that_precedes_it() {
        let base = tempfile::tempdir().expect("temp dir");
        let data = base.path().join("data");
        std::fs::create_dir_all(data.join("subdir")).expect("data subdir");

        let link = base.path().join("link");
        std::os::unix::fs::symlink(data.join("subdir"), &link).expect("symlink");

        let through_link = link.join("..").join("catalog");
        assert!(
            overlapping_metastore_dir(&data.to_string_lossy(), &through_link.to_string_lossy())
                .await
                .expect("the test paths resolve")
                .is_some(),
            "`link/../catalog` resolves inside the data dir once `link` is followed first"
        );
    }

    /// `remove_dir_all` unlinks the directory entry it walks onto rather than following
    /// it, so a metastore directory whose own last component is a symlink out of the
    /// tree still loses its link — the catalog file survives with nothing naming it.
    /// Raised by Copilot on #13101.
    #[cfg(unix)]
    #[tokio::test]
    async fn overlap_detects_a_metastore_entry_symlinked_out_of_the_data_dir() {
        let base = tempfile::tempdir().expect("temp dir");
        let data = base.path().join("orders");
        std::fs::create_dir_all(&data).expect("data dir");

        // The catalog's contents live outside the data directory, but the name that
        // reaches them is inside it.
        let real_metastore = base.path().join("real-metastore");
        std::fs::create_dir_all(&real_metastore).expect("metastore dir");
        let entry = data.join("catalog");
        std::os::unix::fs::symlink(&real_metastore, &entry).expect("symlink");

        // Compare against the *returned* data path: on macOS the temp dir sits under
        // `/var`, a symlink to `/private/var`, so the path the test built is not the one
        // the guard resolves to.
        let (resolved_data, reached) =
            overlapping_metastore_dir(&data.to_string_lossy(), &entry.to_string_lossy())
                .await
                .expect("the test paths resolve")
                .expect("the entry inside the data dir is unlinked by the delete");
        assert!(
            reached.starts_with(&resolved_data),
            "the error must name the entry the delete removes, not its target: {reached:?}"
        );
        assert!(
            !reached.starts_with(&real_metastore),
            "the target lies outside the data dir; naming it would read as a false positive"
        );
    }

    /// Object-store data paths cannot contain the metastore — `SQLite`/Turso only run
    /// on a local filesystem — so the guard must not reject an S3 configuration.
    #[tokio::test]
    async fn overlap_never_fires_for_an_object_store_data_path() {
        assert!(
            overlapping_metastore_dir("s3://bucket/orders/", "/var/spice/metadata")
                .await
                .expect("the test paths resolve")
                .is_none(),
            "an S3 data path is disjoint from any local metastore"
        );
    }

    /// `Ok(None)` is what waves a recursive delete through, so it must mean "object
    /// store" and nothing else — never "could not work out where this path is". Every
    /// local spelling, including a relative one and a `file://` URI, resolves to a path
    /// the overlap check can compare. Raised by Copilot on #13101.
    #[test]
    fn only_an_object_store_scheme_reaches_the_delete_exemption() {
        let cwd = std::env::current_dir().expect("a working directory");

        assert_eq!(
            absolute_data_dir("relative/orders").expect("a relative path resolves"),
            Some(cwd.join("relative/orders")),
            "a relative local path is placed against the working directory, not exempted"
        );
        assert_eq!(
            absolute_data_dir("/var/spice/orders").expect("an absolute path resolves"),
            Some(PathBuf::from("/var/spice/orders"))
        );
        assert_eq!(
            absolute_data_dir("file:///var/spice/orders").expect("a `file://` URI resolves"),
            Some(PathBuf::from("/var/spice/orders")),
            "a `file://` URI names a local directory, so it must be compared, not exempted"
        );
        assert_eq!(
            absolute_data_dir("s3://bucket/orders/").expect("an object store is not a failure"),
            None,
            "only an object-store scheme may skip the overlap check"
        );
    }

    /// The exemption belongs to the *data* path. `is_local_path` is a substring test, so
    /// applying it to a metadata path exempts any value merely containing `://` — while
    /// the catalog code goes on creating `cayenne.db` at that very filesystem path,
    /// inside the directory the recreate deletes. Raised by Copilot on #13101.
    #[tokio::test]
    async fn a_metadata_dir_containing_a_scheme_separator_is_still_compared() {
        let base = tempfile::tempdir().expect("temp dir");
        let data = base.path().join("orders");
        std::fs::create_dir_all(&data).expect("data dir");

        let nested = data.join("catalog://v1");
        assert!(
            overlapping_metastore_dir(&data.to_string_lossy(), &nested.to_string_lossy())
                .await
                .expect("the test paths resolve")
                .is_some(),
            "`://` inside a metadata path does not put it on object storage, and the \
             delete still reaches it"
        );
    }

    /// `mode: file_create` must refuse the configuration at open time, before the
    /// recreate reaches `remove_dir_all`. Regression test for #13055 / #13068.
    #[tokio::test]
    async fn init_refuses_a_file_create_recreate_that_would_delete_the_metastore() {
        let app = Arc::new(AppBuilder::new("test").build());

        // Named `metadata`, so the default data path resolves onto the default
        // metastore directory with no explicit parameter involved.
        let mut dataset = TestAccelerationSource::new("metadata").with_app(Arc::clone(&app));
        dataset.set_acceleration(Acceleration {
            engine: Engine::Cayenne,
            mode: Mode::FileCreate,
            ..Default::default()
        });

        let err = CayenneAccelerator::new()
            .init(&dataset)
            .await
            .expect_err("init must refuse a data directory that contains the metastore");

        let message = err.to_string();
        assert!(
            message.contains("contains the Cayenne metastore directory"),
            "the error must name the overlap; got: {message}"
        );
        assert!(
            message.contains("metadata"),
            "the error must name the dataset and both resolved paths; got: {message}"
        );
    }

    /// The `file_update` schema rebuild reaches `remove_dir_all` without going through
    /// `init`, so it needs its own guard — and must leave the metastore on disk.
    /// Regression test for #13055 / #13068.
    #[tokio::test]
    async fn drop_table_leaves_a_metastore_nested_in_the_data_directory_intact() {
        let app = Arc::new(AppBuilder::new("test").build());
        let base = tempfile::tempdir().expect("temp dir");

        let mut dataset = TestAccelerationSource::new("orders").with_app(Arc::clone(&app));
        // `cayenne_file_path` puts the data directory at `{base}/orders/`; the
        // metastore is configured inside it.
        let data_dir = base.path().join("orders");
        let metadata_dir = data_dir.join("catalog");
        dataset.set_acceleration(Acceleration {
            engine: Engine::Cayenne,
            mode: Mode::FileUpdate,
            params: [
                (
                    "cayenne_file_path".to_string(),
                    base.path().to_string_lossy().into_owned(),
                ),
                (
                    "cayenne_metadata_dir".to_string(),
                    metadata_dir.to_string_lossy().into_owned(),
                ),
            ]
            .into_iter()
            .collect(),
            ..Default::default()
        });

        std::fs::create_dir_all(&metadata_dir).expect("metastore dir");
        let catalog_file = metadata_dir.join("cayenne.db");
        std::fs::write(&catalog_file, b"catalog").expect("catalog file");

        let err = CayenneAccelerator::new()
            .drop_table("orders", &dataset)
            .await
            .expect_err("drop_table must refuse to delete a data directory holding the metastore");
        assert!(
            err.to_string()
                .contains("contains the Cayenne metastore directory"),
            "the error must name the overlap; got: {err}"
        );
        assert!(
            catalog_file.exists(),
            "the metastore must survive the refused rebuild"
        );
    }

    /// The invariant the whole guard is built on, asserted rather than assumed:
    /// `remove_dir_all` on a path whose final component is a symlink **unlinks the link**
    /// and leaves its target untouched. So a teardown costs an aliased catalog its name,
    /// never its bytes — which is why the refusals below are worded around names, and why
    /// a link is examined rather than descended.
    #[cfg(unix)]
    #[tokio::test]
    async fn remove_dir_all_unlinks_a_symlink_rather_than_descending_it() {
        let base = tempfile::tempdir().expect("temp dir");
        let real = base.path().join("real");
        std::fs::create_dir_all(&real).expect("real dir");
        std::fs::write(real.join("cayenne.db"), b"catalog").expect("catalog");
        let link = base.path().join("link");
        std::os::unix::fs::symlink(&real, &link).expect("symlink");

        tokio::fs::remove_dir_all(&link)
            .await
            .expect("removing a symlinked directory succeeds");

        assert!(
            link.symlink_metadata().is_err(),
            "the link itself is what is removed"
        );
        assert!(
            real.join("cayenne.db").exists(),
            "the catalog behind the link keeps its bytes — only the name is gone"
        );
    }

    /// The `cayenne.db` name and the sidecars `SQLite` keeps beside it are what mark a
    /// directory as holding a catalog. A name that merely starts with it is not one:
    /// `cayenne.db.backup` is an operator's copy, and a dataset may legitimately be
    /// called `cayenne.dbx`.
    #[test]
    fn the_metastore_file_names_are_the_database_and_its_sidecars() {
        for name in [
            "cayenne.db",
            "cayenne.db-wal",
            "cayenne.db-shm",
            "cayenne.db-journal",
        ] {
            assert!(
                is_metastore_file(std::ffi::OsStr::new(name)),
                "'{name}' is part of a metastore"
            );
        }
        for name in [
            "cayenne.dbx",
            "cayenne.db.backup",
            "cayenne",
            "orders.db",
            "db",
        ] {
            assert!(
                !is_metastore_file(std::ffi::OsStr::new(name)),
                "'{name}' is not part of a metastore"
            );
        }
    }

    /// The shape #13436 describes: dataset `q`'s data directory holds a metastore that
    /// `q`'s own parameters never name — it belongs to another dataset, or to a
    /// configuration nothing names any more. The params-based guard compares `q`'s data
    /// directory against `q`'s own metastore, finds no overlap, and the recreate would
    /// unlink a catalog holding every Cayenne dataset in the instance.
    #[tokio::test]
    async fn a_teardown_refuses_a_metastore_no_parameter_of_this_dataset_names() {
        let app = Arc::new(AppBuilder::new("test").build());
        let base = tempfile::tempdir().expect("temp dir");

        let mut dataset = TestAccelerationSource::new("q").with_app(Arc::clone(&app));
        // `cayenne_file_path` puts `q`'s data directory at `{base}/q/` and `q`'s own
        // metastore at the sibling `{base}/metadata`, so `q`'s configuration is safe by
        // the params-only test — which is the point.
        dataset.set_acceleration(Acceleration {
            engine: Engine::Cayenne,
            mode: Mode::FileUpdate,
            params: [(
                "cayenne_file_path".to_string(),
                base.path().to_string_lossy().into_owned(),
            )]
            .into_iter()
            .collect(),
            ..Default::default()
        });

        let data_dir = base.path().join("q");
        // Nested, not directly in the data directory, so the walk is what finds it.
        let foreign_metastore = data_dir.join("nested").join("catalog");
        std::fs::create_dir_all(&foreign_metastore).expect("foreign metastore dir");
        let catalog_file = foreign_metastore.join("cayenne.db");
        std::fs::write(&catalog_file, b"catalog").expect("catalog file");

        let err =
            CayenneAccelerator::remove_acceleration_data_dir(&dataset, &data_dir.to_string_lossy())
                .await
                .expect_err("a data directory holding any metastore must not be deleted");

        assert!(
            catalog_file.exists(),
            "the metastore must survive the refused teardown"
        );
        let message = err.to_string();
        assert!(
            message.contains(&catalog_file.to_string_lossy().into_owned()),
            "the error must name the metastore it found; got: {message}"
        );
        assert!(
            message.contains("holds a Cayenne metastore"),
            "the error must say why the teardown was refused; got: {message}"
        );
    }

    /// The guard must not refuse an ordinary teardown: a data directory with no metastore
    /// under it is deleted, sidecar-shaped names and all.
    #[tokio::test]
    async fn a_data_directory_holding_no_metastore_is_still_deleted() {
        let app = Arc::new(AppBuilder::new("test").build());
        let base = tempfile::tempdir().expect("temp dir");

        let mut dataset = TestAccelerationSource::new("orders").with_app(Arc::clone(&app));
        dataset.set_acceleration(Acceleration {
            engine: Engine::Cayenne,
            mode: Mode::FileUpdate,
            params: [(
                "cayenne_file_path".to_string(),
                base.path().to_string_lossy().into_owned(),
            )]
            .into_iter()
            .collect(),
            ..Default::default()
        });

        let data_dir = base.path().join("orders");
        std::fs::create_dir_all(data_dir.join("shard=0")).expect("data dir");
        std::fs::write(
            data_dir.join("shard=0").join("cayenne.dbx"),
            b"not a metastore",
        )
        .expect("decoy file");

        CayenneAccelerator::remove_acceleration_data_dir(&dataset, &data_dir.to_string_lossy())
            .await
            .expect("a data directory with no metastore under it is deleted");

        assert!(
            !data_dir.exists(),
            "the teardown must actually remove the data directory"
        );
    }

    /// A directory symlink under the data directory that aliases a metadata directory
    /// must refuse the teardown. The catalog file itself survives — `remove_dir_all`
    /// unlinks the link rather than descending it — but the *name* does not, and the
    /// dataset configured with that name gets a fresh empty directory there and opens an
    /// empty catalog inside it. Nothing on this path can tell a configured alias from an
    /// incidental one, so it refuses: the false refusal is loud and clearable, the
    /// orphaning is silent and permanent.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_symlink_aliasing_a_catalog_directory_refuses_the_teardown() {
        let app = Arc::new(AppBuilder::new("test").build());
        let base = tempfile::tempdir().expect("temp dir");

        let mut dataset = TestAccelerationSource::new("orders").with_app(Arc::clone(&app));
        dataset.set_acceleration(Acceleration {
            engine: Engine::Cayenne,
            mode: Mode::FileUpdate,
            params: [(
                "cayenne_file_path".to_string(),
                base.path().to_string_lossy().into_owned(),
            )]
            .into_iter()
            .collect(),
            ..Default::default()
        });

        let outside = base.path().join("elsewhere");
        std::fs::create_dir_all(&outside).expect("outside dir");
        let catalog_file = outside.join("cayenne.db");
        std::fs::write(&catalog_file, b"catalog").expect("catalog file");

        let data_dir = base.path().join("orders");
        std::fs::create_dir_all(&data_dir).expect("data dir");
        let alias = data_dir.join("catalog");
        std::os::unix::fs::symlink(&outside, &alias).expect("symlink");

        let err =
            CayenneAccelerator::remove_acceleration_data_dir(&dataset, &data_dir.to_string_lossy())
                .await
                .expect_err("a link aliasing a metadata directory must refuse the teardown");

        assert!(
            alias.exists(),
            "the alias must survive the refused teardown"
        );
        assert!(
            catalog_file.exists(),
            "the catalog must survive the refused teardown"
        );
        // Named through the link, not at its resolved target: that is the path under the
        // data directory the operator has to move, and the one they can find.
        assert!(
            err.to_string()
                .contains(&alias.join("cayenne.db").to_string_lossy().into_owned()),
            "the error must name the catalog the link reaches; got: {err}"
        );
    }

    /// A data directory that is *itself* a link is dereferenced once and walked. The
    /// teardown unlinks the alias and the caller recreates a real, empty directory in its
    /// place, so a catalog reachable only by a name under the alias is orphaned exactly as
    /// it would be by a link one level down.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_data_directory_that_is_itself_a_link_is_still_read_through() {
        let app = Arc::new(AppBuilder::new("test").build());
        let base = tempfile::tempdir().expect("temp dir");

        let mut dataset = TestAccelerationSource::new("q").with_app(Arc::clone(&app));
        dataset.set_acceleration(Acceleration {
            engine: Engine::Cayenne,
            mode: Mode::FileUpdate,
            params: [(
                "cayenne_file_path".to_string(),
                base.path().to_string_lossy().into_owned(),
            )]
            .into_iter()
            .collect(),
            ..Default::default()
        });

        let elsewhere = base.path().join("elsewhere");
        let foreign_metastore = elsewhere.join("catalog");
        std::fs::create_dir_all(&foreign_metastore).expect("foreign metastore dir");
        let catalog_file = foreign_metastore.join("cayenne.db");
        std::fs::write(&catalog_file, b"catalog").expect("catalog file");

        let data_dir = base.path().join("q");
        std::os::unix::fs::symlink(&elsewhere, &data_dir).expect("symlink the data dir");

        CayenneAccelerator::remove_acceleration_data_dir(&dataset, &data_dir.to_string_lossy())
            .await
            .expect_err("a link whose target holds a catalog must refuse the teardown");

        assert!(
            data_dir.symlink_metadata().is_ok(),
            "the alias must survive the refused teardown"
        );
        assert!(
            catalog_file.exists(),
            "the catalog must survive the refused teardown"
        );
    }

    /// The link rule must stay narrow: a symlink to an ordinary directory aliases no
    /// catalog, and refusing on every link would block teardowns no operator could clear.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_symlink_to_a_directory_holding_no_catalog_does_not_block_the_teardown() {
        let app = Arc::new(AppBuilder::new("test").build());
        let base = tempfile::tempdir().expect("temp dir");

        let mut dataset = TestAccelerationSource::new("orders").with_app(Arc::clone(&app));
        dataset.set_acceleration(Acceleration {
            engine: Engine::Cayenne,
            mode: Mode::FileUpdate,
            params: [(
                "cayenne_file_path".to_string(),
                base.path().to_string_lossy().into_owned(),
            )]
            .into_iter()
            .collect(),
            ..Default::default()
        });

        let outside = base.path().join("elsewhere");
        std::fs::create_dir_all(&outside).expect("outside dir");
        std::fs::write(outside.join("part-0.vortex"), b"data").expect("ordinary file");

        let data_dir = base.path().join("orders");
        std::fs::create_dir_all(&data_dir).expect("data dir");
        std::os::unix::fs::symlink(&outside, data_dir.join("link")).expect("symlink");
        std::os::unix::fs::symlink(base.path().join("nothing"), data_dir.join("dangling"))
            .expect("dangling symlink");

        CayenneAccelerator::remove_acceleration_data_dir(&dataset, &data_dir.to_string_lossy())
            .await
            .expect("a link that aliases no catalog does not block the teardown");

        assert!(
            !data_dir.exists(),
            "the teardown must remove the data directory"
        );
        assert!(
            outside.join("part-0.vortex").exists(),
            "unlinking the symlink must leave what it pointed at"
        );
    }

    /// `is_local_path` is a substring test for `://`, so a perfectly ordinary local
    /// directory whose name contains one is classified as remote. The configured-metastore
    /// half exempts such a data path deliberately, but the disk read must not: the path is
    /// still what `remove_dir_all` walks.
    #[tokio::test]
    async fn a_local_data_directory_whose_name_contains_a_scheme_separator_is_still_read() {
        let app = Arc::new(AppBuilder::new("test").build());
        let base = tempfile::tempdir().expect("temp dir");

        let mut dataset = TestAccelerationSource::new("q").with_app(Arc::clone(&app));
        dataset.set_acceleration(Acceleration {
            engine: Engine::Cayenne,
            mode: Mode::FileUpdate,
            params: [(
                "cayenne_file_path".to_string(),
                base.path().to_string_lossy().into_owned(),
            )]
            .into_iter()
            .collect(),
            ..Default::default()
        });

        // `{base}/q:` then `sub`: the empty component between the two slashes collapses,
        // so this names a real directory and still contains `://`.
        let data_dir = base.path().join("q:").join("sub");
        std::fs::create_dir_all(&data_dir).expect("data dir");
        let catalog_file = data_dir.join("cayenne.db");
        std::fs::write(&catalog_file, b"catalog").expect("catalog file");
        let spelled = format!("{}/q://sub", base.path().to_string_lossy());
        assert!(
            !is_local_path(&spelled),
            "the premise: this local path is classified as remote"
        );

        CayenneAccelerator::remove_acceleration_data_dir(&dataset, &spelled)
            .await
            .expect_err("a scheme separator in the name must not wave the delete through");
        assert!(
            catalog_file.exists(),
            "the catalog must survive the refused teardown"
        );
    }

    /// The deletion may only ever reach the tree the caller authorized. Each teardown
    /// gates on `PathBuf::from(&dir_path).exists()` and hands `remove_dir_all` that same
    /// string, so normalizing the path anywhere in between would delete a directory
    /// nothing checked — here, the real `{base}/q` rather than the relative `file:/…` the
    /// caller tested. The catalog beneath it is the sentinel that proves it was not.
    #[tokio::test]
    async fn the_teardown_deletes_only_the_tree_its_caller_checked() {
        let app = Arc::new(AppBuilder::new("test").build());
        let base = tempfile::tempdir().expect("temp dir");

        let mut dataset = TestAccelerationSource::new("q").with_app(Arc::clone(&app));
        dataset.set_acceleration(Acceleration {
            engine: Engine::Cayenne,
            mode: Mode::FileUpdate,
            params: [(
                "cayenne_file_path".to_string(),
                base.path().to_string_lossy().into_owned(),
            )]
            .into_iter()
            .collect(),
            ..Default::default()
        });

        let resolved = base.path().join("q");
        let catalog_file = resolved.join("catalog").join("cayenne.db");
        std::fs::create_dir_all(resolved.join("catalog")).expect("catalog dir");
        std::fs::write(&catalog_file, b"catalog").expect("catalog file");

        // The `file:` spelling the metastore comparison strips. Nothing exists at the
        // relative path it denotes, so the teardown must fail to remove it and must not
        // reach for the real directory instead.
        let spelled = format!("file://{}", resolved.to_string_lossy());
        CayenneAccelerator::remove_acceleration_data_dir(&dataset, &spelled)
            .await
            .expect_err("nothing exists at the path the caller would have checked");

        assert!(
            catalog_file.exists(),
            "the teardown must not normalize its way into a tree its caller never checked"
        );
    }

    /// Both teardowns drop this dataset's rows from the metastore before they reach the
    /// directory, so a refusal raised only at the delete would leave the rows gone and the
    /// files still there. The proof therefore runs as a preflight too — and this pins it
    /// by its observable consequence: refusing before the catalog is ever opened leaves no
    /// metastore at the dataset's own metadata directory.
    #[tokio::test]
    async fn a_refused_rebuild_does_not_touch_the_catalog_first() {
        let app = Arc::new(AppBuilder::new("test").build());
        let base = tempfile::tempdir().expect("temp dir");

        let mut dataset = TestAccelerationSource::new("q").with_app(Arc::clone(&app));
        dataset.set_acceleration(Acceleration {
            engine: Engine::Cayenne,
            mode: Mode::FileUpdate,
            params: [(
                "cayenne_file_path".to_string(),
                base.path().to_string_lossy().into_owned(),
            )]
            .into_iter()
            .collect(),
            ..Default::default()
        });

        let data_dir = base.path().join("q");
        let foreign_metastore = data_dir.join("catalog");
        std::fs::create_dir_all(&foreign_metastore).expect("foreign metastore dir");
        std::fs::write(foreign_metastore.join("cayenne.db"), b"catalog").expect("catalog file");

        CayenneAccelerator::new()
            .drop_table("q", &dataset)
            .await
            .expect_err("the rebuild must refuse");

        assert!(
            !base.path().join("metadata").join("cayenne.db").exists(),
            "the refusal must come before the rebuild opens this dataset's own metastore, \
             or its rows are already gone by the time the teardown is refused"
        );
    }

    /// The `file_update` schema rebuild and the `file_create` recreate both reach the
    /// deletion, so both must carry the on-disk proof. This pins the rebuild end to end
    /// — through `drop_table`, not through the helper it calls.
    #[tokio::test]
    async fn drop_table_refuses_a_metastore_no_parameter_of_this_dataset_names() {
        let app = Arc::new(AppBuilder::new("test").build());
        let base = tempfile::tempdir().expect("temp dir");

        let mut dataset = TestAccelerationSource::new("q").with_app(Arc::clone(&app));
        dataset.set_acceleration(Acceleration {
            engine: Engine::Cayenne,
            mode: Mode::FileUpdate,
            params: [(
                "cayenne_file_path".to_string(),
                base.path().to_string_lossy().into_owned(),
            )]
            .into_iter()
            .collect(),
            ..Default::default()
        });

        let data_dir = base.path().join("q");
        let foreign_metastore = data_dir.join("catalog");
        std::fs::create_dir_all(&foreign_metastore).expect("foreign metastore dir");
        let catalog_file = foreign_metastore.join("cayenne.db");
        std::fs::write(&catalog_file, b"catalog").expect("catalog file");

        let err = CayenneAccelerator::new()
            .drop_table("q", &dataset)
            .await
            .expect_err("the rebuild must refuse to delete a directory holding a metastore");

        assert!(
            catalog_file.exists(),
            "the metastore must survive the refused rebuild"
        );
        assert!(
            err.to_string().contains("holds a Cayenne metastore"),
            "the error must say why the rebuild was refused; got: {err}"
        );
    }

    /// The `file_create` bootstrap reaches the deletion by a different route from the
    /// schema rebuild, so it needs the on-disk proof attached to its own call. The
    /// dataset's parameters are safe by the configured-metastore test — its own
    /// metastore is the sibling `{base}/metadata` — so only the disk read can refuse it.
    #[tokio::test]
    async fn init_refuses_a_metastore_no_parameter_of_this_dataset_names() {
        let app = Arc::new(AppBuilder::new("test").build());
        let base = tempfile::tempdir().expect("temp dir");

        let mut dataset = TestAccelerationSource::new("q").with_app(Arc::clone(&app));
        dataset.set_acceleration(Acceleration {
            engine: Engine::Cayenne,
            mode: Mode::FileCreate,
            params: [(
                "cayenne_file_path".to_string(),
                base.path().to_string_lossy().into_owned(),
            )]
            .into_iter()
            .collect(),
            ..Default::default()
        });

        let data_dir = base.path().join("q");
        let foreign_metastore = data_dir.join("catalog");
        std::fs::create_dir_all(&foreign_metastore).expect("foreign metastore dir");
        let catalog_file = foreign_metastore.join("cayenne.db");
        std::fs::write(&catalog_file, b"catalog").expect("catalog file");

        let err = CayenneAccelerator::new()
            .init(&dataset)
            .await
            .expect_err("the bootstrap must refuse to delete a directory holding a metastore");

        assert!(
            catalog_file.exists(),
            "the metastore must survive the refused bootstrap"
        );
        assert!(
            err.to_string().contains("holds a Cayenne metastore"),
            "the error must say why the bootstrap was refused; got: {err}"
        );
    }

    #[tokio::test]
    async fn test_write_concurrency_is_resolved_per_dataset() {
        let app = Arc::new(AppBuilder::new("test").build());

        let mut hot_dataset = TestAccelerationSource::new("hot").with_app(Arc::clone(&app));
        hot_dataset.set_acceleration(Acceleration {
            engine: Engine::Cayenne,
            mode: Mode::File,
            params: [("cayenne_write_concurrency".to_string(), "16".to_string())]
                .into_iter()
                .collect(),
            ..Default::default()
        });

        let mut quiet_dataset = TestAccelerationSource::new("quiet");
        quiet_dataset.set_acceleration(Acceleration {
            engine: Engine::Cayenne,
            mode: Mode::File,
            params: [("cayenne_write_concurrency".to_string(), "2".to_string())]
                .into_iter()
                .collect(),
            ..Default::default()
        });

        let hot = CayenneAccelerator::get_vortex_config("hot", &hot_dataset)
            .await
            .expect("hot config should be valid");
        let quiet = CayenneAccelerator::get_vortex_config("quiet", &quiet_dataset)
            .await
            .expect("quiet config should be valid");

        assert_eq!(hot.write_concurrency, Some(16));
        assert_eq!(quiet.write_concurrency, Some(2));
    }

    /// A `MakeWriter` that accumulates formatted log output so a test can assert on
    /// what an operator would actually see in `spice.log`.
    #[derive(Clone, Default)]
    struct CapturedLogs(Arc<std::sync::Mutex<Vec<u8>>>);

    impl CapturedLogs {
        fn occurrences_of(&self, needle: &str) -> usize {
            let buffer = self
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            String::from_utf8_lossy(&buffer).matches(needle).count()
        }
    }

    impl std::io::Write for CapturedLogs {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    thread_local! {
        static CAPTURE_SINK: std::cell::RefCell<Option<CapturedLogs>> =
            const { std::cell::RefCell::new(None) };
    }

    /// Writer that routes each event to the buffer registered for the emitting thread,
    /// and discards events from threads that registered none.
    #[derive(Clone, Default)]
    struct ThreadCapture;

    impl std::io::Write for ThreadCapture {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            CAPTURE_SINK.with(|sink| {
                if let Some(logs) = sink.borrow().as_ref() {
                    logs.0
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .extend_from_slice(buf);
                }
            });
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl tracing_subscriber::fmt::MakeWriter<'_> for ThreadCapture {
        type Writer = Self;

        fn make_writer(&self) -> Self::Writer {
            Self
        }
    }

    /// Captures this thread's log lines until the guard drops.
    ///
    /// The subscriber is installed **globally**, once per test binary, rather than
    /// scoped to the calling thread with `tracing::subscriber::set_default`. A scoped
    /// subscriber cannot observe a callsite that other threads also reach: `tracing`
    /// caches each callsite's interest process-wide, so whichever thread evaluates it
    /// first decides for every thread — and a sibling test reaching it without a
    /// subscriber caches it as "never", after which the event is dropped before any
    /// subscriber sees it. That is invisible under `nextest` (one process per test) and
    /// when the test runs alone, and shows up as a zero count under a parallel
    /// `cargo test`. Installing globally keeps every callsite enabled; the thread-local
    /// sink is what keeps concurrent tests from reading each other's lines.
    fn capture_logs() -> CaptureGuard {
        static INSTALLED: std::sync::OnceLock<()> = std::sync::OnceLock::new();
        INSTALLED.get_or_init(|| {
            tracing::subscriber::set_global_default(
                tracing_subscriber::fmt()
                    .with_writer(ThreadCapture)
                    .with_ansi(false)
                    .with_max_level(tracing::Level::INFO)
                    .finish(),
            )
            .expect("no other global subscriber is installed in this test binary");
        });

        let logs = CapturedLogs::default();
        CAPTURE_SINK.with(|sink| *sink.borrow_mut() = Some(logs.clone()));
        CaptureGuard(logs)
    }

    struct CaptureGuard(CapturedLogs);

    impl CaptureGuard {
        fn occurrences_of(&self, needle: &str) -> usize {
            self.0.occurrences_of(needle)
        }
    }

    impl Drop for CaptureGuard {
        fn drop(&mut self) {
            CAPTURE_SINK.with(|sink| *sink.borrow_mut() = None);
        }
    }

    #[test]
    fn auto_tuned_config_is_reported_once_per_resolution() {
        // Table names are process-global keys; keep them unique to this test.
        const TABLE: &str = "dedupe_decision_table";
        const OTHER: &str = "dedupe_decision_other_table";

        // First resolution reports; the identical re-resolutions an unbounded init
        // retry produces do not.
        assert!(auto_tuned_config_is_newly_resolved(TABLE, 1));
        assert!(!auto_tuned_config_is_newly_resolved(TABLE, 1));
        assert!(!auto_tuned_config_is_newly_resolved(TABLE, 1));

        // A genuine re-tune reports again, then settles at the new resolution.
        assert!(auto_tuned_config_is_newly_resolved(TABLE, 2));
        assert!(!auto_tuned_config_is_newly_resolved(TABLE, 2));

        // Reverting to an earlier resolution is still a change worth reporting —
        // the key is the last value seen, not the set of values ever seen.
        assert!(auto_tuned_config_is_newly_resolved(TABLE, 1));

        // Tables are independent: one table's line never suppresses another's.
        assert!(auto_tuned_config_is_newly_resolved(OTHER, 1));
    }

    #[test]
    fn auto_tuned_config_fingerprint_covers_the_logged_values_only() {
        use data_accelerator_api::storage::ResolvedAccelerationStorage;

        let hw = autotune::HardwareProfile::new(
            8,
            32 * 1024 * 1024 * 1024,
            ResolvedAccelerationStorage::LocalSsd,
            ResolvedAccelerationStorage::LocalSsd,
        );
        let workload = autotune::WorkloadProfile::default();
        let config = cayenne::metadata::VortexConfig::default();
        let baseline = auto_tuned_config_fingerprint("t", &hw, &workload, &config);

        // Deterministic: the same resolution fingerprints the same way, which is
        // what collapses the retry storm.
        assert_eq!(
            baseline,
            auto_tuned_config_fingerprint("t", &hw, &workload, &config)
        );

        // Every printed input participates.
        let mut retuned = config.clone();
        retuned.target_vortex_file_size_mb += 1;
        assert_ne!(
            baseline,
            auto_tuned_config_fingerprint("t", &hw, &workload, &retuned),
            "a knob that appears in the line must change the fingerprint"
        );
        let mut bigger_host = hw;
        bigger_host.cores += 1;
        assert_ne!(
            baseline,
            auto_tuned_config_fingerprint("t", &bigger_host, &workload, &config),
            "the host basis appears in the line and must change the fingerprint"
        );
        let inferred = autotune::WorkloadProfile {
            row_count: Some(1_000),
            ..workload
        };
        assert_ne!(
            baseline,
            auto_tuned_config_fingerprint("t", &hw, &inferred, &config),
            "the inferred workload signals appear in the line"
        );
        assert_ne!(
            baseline,
            auto_tuned_config_fingerprint("other", &hw, &workload, &config),
            "the fingerprint is per table"
        );

        // A knob the line does not print still participates, through `{config:?}`:
        // over-reporting is the safe direction, and it keeps the key from silently
        // narrowing as knobs are added to the line.
        let mut unprinted = config.clone();
        unprinted.stream_publish_interval_ms += 1;
        assert_ne!(
            baseline,
            auto_tuned_config_fingerprint("t", &hw, &workload, &unprinted)
        );

        // The calibration measurements are deliberately excluded: they are not
        // printed, and they reach the fingerprint through the knobs they resolved.
        let mut probed = hw;
        probed.data_perf.write_mbps = Some(125.0);
        probed.metastore_perf.write_mbps = Some(4_000.0);
        assert_eq!(
            baseline,
            auto_tuned_config_fingerprint("t", &probed, &workload, &config),
            "a measured storage rate is not part of the line and must not re-report it"
        );
    }

    /// Regression test for #12330: dataset initialization retries with unbounded
    /// backoff and rebuilds the table provider on every attempt, so a dataset that
    /// never loads used to repeat its ~1 KB config line forever.
    #[tokio::test]
    async fn auto_tuned_config_line_survives_a_provider_rebuild_storm() {
        const TABLE: &str = "dedupe_rebuild_storm";
        const LINE: &str = "Cayenne auto-tuned config:";
        // A configuration warning derived from the same resolution, so it shares the
        // gate: a misconfigured dataset is exactly the one that retries forever.
        const WARNING: &str = "cayenne_compaction_trigger_files is 1";
        let misconfigured = || {
            vec![(
                "cayenne_compaction_trigger_files".to_string(),
                "1".to_string(),
            )]
        };

        let app = Arc::new(AppBuilder::new("test").build());
        let build = |params: Vec<(String, String)>| {
            let mut dataset = TestAccelerationSource::new(TABLE).with_app(Arc::clone(&app));
            dataset.set_acceleration(Acceleration {
                engine: Engine::Cayenne,
                mode: Mode::File,
                params: params.into_iter().collect(),
                ..Default::default()
            });
            dataset
        };

        // Capture only the emits below: `#[tokio::test]` runs the whole test body on one
        // thread, and the sink is registered for that thread alone, so concurrent tests
        // cannot contribute to the count.
        let captured = capture_logs();

        let dataset = build(misconfigured());
        for _ in 0..3 {
            let _ = CayenneAccelerator::get_vortex_config(TABLE, &dataset)
                .await
                .expect("config should be valid");
        }
        assert_eq!(
            captured.occurrences_of(LINE),
            1,
            "rebuilding the provider must not re-report an unchanged resolution"
        );
        assert_eq!(
            captured.occurrences_of(WARNING),
            1,
            "a configuration warning derived from that resolution must not repeat either"
        );

        // A genuine re-tune is still reported — the line's whole purpose is to
        // record what `auto` resolved to, including when that answer changes.
        let mut retuned_params = misconfigured();
        retuned_params.push(("cayenne_target_file_size_mb".to_string(), "512".to_string()));
        let retuned = build(retuned_params);
        let _ = CayenneAccelerator::get_vortex_config(TABLE, &retuned)
            .await
            .expect("config should be valid");
        assert_eq!(
            captured.occurrences_of(LINE),
            2,
            "a changed resolution must still be reported"
        );
        assert_eq!(
            captured.occurrences_of(WARNING),
            2,
            "the warning is re-reported alongside the resolution it describes"
        );
    }

    #[tokio::test]
    async fn test_vortex_config_defaults_use_small_write_refresh_profile() {
        let app = Arc::new(AppBuilder::new("test").build());

        for (table_name, refresh_mode) in [
            ("cached_hot", RefreshMode::Caching),
            ("cdc_hot", RefreshMode::Changes),
        ] {
            let mut dataset = TestAccelerationSource::new(table_name).with_app(Arc::clone(&app));
            dataset.set_acceleration(Acceleration {
                engine: Engine::Cayenne,
                mode: Mode::File,
                refresh_mode: Some(refresh_mode),
                ..Default::default()
            });

            let config = CayenneAccelerator::get_vortex_config(table_name, &dataset)
                .await
                .expect("config should be valid");

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

        let mut dataset = TestAccelerationSource::new("append_hot");
        dataset.set_acceleration(Acceleration {
            engine: Engine::Cayenne,
            mode: Mode::File,
            refresh_mode: Some(RefreshMode::Append),
            refresh_check_interval: Some(APPEND_SMALL_WRITE_REFRESH_INTERVAL_THRESHOLD),
            ..Default::default()
        });

        let config = CayenneAccelerator::get_vortex_config("append_hot", &dataset)
            .await
            .expect("config should be valid");

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

        // The bulk-APPEND profile only. `full` (and an unset mode, which the
        // connector default resolves to `full`) is the bulk-OVERWRITE profile and
        // does inline — see `test_full_refresh_disables_background_compaction`.
        for (table_name, refresh_mode) in [
            ("append_manual_load", Some(RefreshMode::Append)),
            ("snapshot_load", Some(RefreshMode::Snapshot)),
            ("disabled_load", Some(RefreshMode::Disabled)),
        ] {
            let mut dataset = TestAccelerationSource::new(table_name).with_app(Arc::clone(&app));
            dataset.set_acceleration(Acceleration {
                engine: Engine::Cayenne,
                mode: Mode::File,
                refresh_mode,
                ..Default::default()
            });

            let config = CayenneAccelerator::get_vortex_config(table_name, &dataset)
                .await
                .expect("config should be valid");

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

        let mut dataset = TestAccelerationSource::new("append_batch_load");
        dataset.set_acceleration(Acceleration {
            engine: Engine::Cayenne,
            mode: Mode::File,
            refresh_mode: Some(RefreshMode::Append),
            refresh_check_interval: Some(
                APPEND_SMALL_WRITE_REFRESH_INTERVAL_THRESHOLD + Duration::from_secs(1),
            ),
            ..Default::default()
        });

        let config = CayenneAccelerator::get_vortex_config("append_batch_load", &dataset)
            .await
            .expect("config should be valid");

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

        let mut dataset = TestAccelerationSource::new("cdc_hot").with_app(Arc::clone(&app));
        dataset.set_acceleration(Acceleration {
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

        let config = CayenneAccelerator::get_vortex_config("cdc_hot", &dataset)
            .await
            .expect("config should be valid");

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

    /// `auto` is the tuning mode unless the operator asks for `adaptive` by name.
    /// A `cayenne_goal_*` SLO — per-dataset or global — expresses a target, not a
    /// choice of controller, so it must leave the closed loop off.
    #[tokio::test]
    async fn test_goals_do_not_enable_adaptive_tuning() {
        let app = Arc::new(
            AppBuilder::new("test")
                .with_runtime_params(
                    [("cayenne_goal_replication_lag".to_string(), "5s".to_string())]
                        .into_iter()
                        .collect(),
                )
                .build(),
        );

        let cdc_dataset = |name: &str, params: Vec<(String, String)>| {
            let mut dataset = TestAccelerationSource::new(name).with_app(Arc::clone(&app));
            dataset.set_acceleration(Acceleration {
                engine: Engine::Cayenne,
                mode: Mode::File,
                refresh_mode: Some(RefreshMode::Changes),
                params: params.into_iter().collect(),
                ..Default::default()
            });
            dataset
        };

        // Global goal only.
        let global_goal = cdc_dataset("global_goal", vec![]);
        let config = CayenneAccelerator::get_vortex_config("global_goal", &global_goal)
            .await
            .expect("config should be valid");
        assert!(
            !config.dynamic_tuning,
            "a global cayenne_goal_* must not turn on the closed loop"
        );
        assert!(
            config.goal_replication_lag_secs.is_some(),
            "the goal is still parsed so an operator who enables adaptive gets it"
        );

        // Global + per-dataset goals.
        let dataset_goal = cdc_dataset(
            "dataset_goal",
            vec![("cayenne_goal_freshness".to_string(), "30s".to_string())],
        );
        let config = CayenneAccelerator::get_vortex_config("dataset_goal", &dataset_goal)
            .await
            .expect("config should be valid");
        assert!(
            !config.dynamic_tuning,
            "a per-dataset cayenne_goal_* must not turn on the closed loop"
        );

        // Only the explicit mode does.
        let adaptive = cdc_dataset(
            "adaptive",
            vec![("cayenne_tuning".to_string(), "adaptive".to_string())],
        );
        let config = CayenneAccelerator::get_vortex_config("adaptive", &adaptive)
            .await
            .expect("config should be valid");
        assert!(
            config.dynamic_tuning,
            "`cayenne_tuning: adaptive` enables the closed loop"
        );
    }

    #[tokio::test]
    async fn test_documented_cdc_mem_tier_params_are_resolved() {
        let app = Arc::new(AppBuilder::new("test").build());

        let mut dataset = TestAccelerationSource::new("cdc_hot").with_app(Arc::clone(&app));
        dataset.set_acceleration(Acceleration {
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

        let config = CayenneAccelerator::get_vortex_config("cdc_hot", &dataset)
            .await
            .expect("config should be valid");

        assert_eq!(config.cdc_mem_tier_max_bytes, 123_456);
        assert_eq!(config.cdc_mem_tier_max_age_ms, 7_890);
    }

    /// Unset mem-tier caps are auto-derived from host memory for the CDC
    /// profile (range-asserted, since the test host's RAM varies), while a
    /// non-small-write profile keeps the static serde defaults untouched.
    #[tokio::test]
    async fn test_cdc_mem_tier_caps_auto_derived_for_small_write() {
        const MIB: i64 = 1024 * 1024;
        let app = Arc::new(AppBuilder::new("test").build());

        let mut cdc = TestAccelerationSource::new("cdc_auto_tier").with_app(Arc::clone(&app));
        cdc.set_acceleration(Acceleration {
            engine: Engine::Cayenne,
            mode: Mode::File,
            refresh_mode: Some(RefreshMode::Changes),
            ..Default::default()
        });
        let config = CayenneAccelerator::get_vortex_config("cdc_auto_tier", &cdc)
            .await
            .expect("config should be valid");
        assert!(
            (256 * MIB..=1024 * MIB).contains(&config.cdc_mem_tier_max_bytes),
            "derived cap {} outside [256 MiB, 1 GiB]",
            config.cdc_mem_tier_max_bytes
        );
        assert!(
            (32 * MIB..=128 * MIB).contains(&config.cdc_mem_tier_min_flush_bytes),
            "derived flush gate {} outside [32 MiB, 128 MiB]",
            config.cdc_mem_tier_min_flush_bytes
        );
        assert!(
            config.cdc_mem_tier_min_flush_bytes <= config.cdc_mem_tier_max_bytes,
            "flush gate must not exceed the cap"
        );
        // Time-domain knobs are NOT hardware-derived.
        assert_eq!(config.cdc_mem_tier_max_age_ms, 10_000);
        assert_eq!(config.cdc_mem_tier_checkpoint_interval_ms, 1_000);

        let mut full = TestAccelerationSource::new("full_tier");
        full.set_acceleration(Acceleration {
            engine: Engine::Cayenne,
            mode: Mode::File,
            refresh_mode: Some(RefreshMode::Full),
            ..Default::default()
        });
        let config = CayenneAccelerator::get_vortex_config("full_tier", &full)
            .await
            .expect("config should be valid");
        assert_eq!(
            config.cdc_mem_tier_max_bytes,
            256 * MIB,
            "non-small-write profiles keep the static default (knob is inert there)"
        );
        assert_eq!(config.cdc_mem_tier_min_flush_bytes, 32 * MIB);
    }

    /// A full refresh replaces the whole table, so there is nothing for compaction
    /// to consolidate and the background compactor must be off by default.
    #[tokio::test]
    async fn test_full_refresh_disables_background_compaction() {
        let app = Arc::new(AppBuilder::new("test").build());

        let build = |name: &str, refresh_mode: RefreshMode, params: Vec<(String, String)>| {
            let mut ds = TestAccelerationSource::new(name).with_app(Arc::clone(&app));
            ds.set_acceleration(Acceleration {
                engine: Engine::Cayenne,
                mode: Mode::File,
                refresh_mode: Some(refresh_mode),
                params: params.into_iter().collect(),
                ..Default::default()
            });
            ds
        };

        let full = build("full_tbl", RefreshMode::Full, vec![]);
        let config = CayenneAccelerator::get_vortex_config("full_tbl", &full)
            .await
            .expect("config should be valid");
        assert_eq!(
            config.compaction_background_interval_ms, 0,
            "full refresh must not spawn a background compactor"
        );
        assert_eq!(
            config.inline_max_rows, SMALL_WRITE_INLINE_MAX_ROWS,
            "a whole-table replace small enough to be admitted must inline: with the background \
             compactor off, the tiny Vortex files it would otherwise write are never merged"
        );
        assert_eq!(config.inline_max_bytes, SMALL_WRITE_INLINE_MAX_BYTES);
        assert_eq!(
            config.inline_max_buffer_bytes,
            SMALL_WRITE_INLINE_MAX_BUFFER_BYTES
        );
        assert_eq!(
            config.inline_flush_max_rows,
            cayenne::metadata::DEFAULT_INLINE_FLUSH_MAX_ROWS,
            "the cumulative flush caps stay at their defaults — a replace leaves one entry, so \
             the flush gate never binds"
        );

        // An explicit interval still wins: a pod that mixes in-place writes with
        // full refreshes can turn the compactor back on.
        let pinned = build(
            "full_pinned",
            RefreshMode::Full,
            vec![(
                "cayenne_compaction_background_interval_ms".to_string(),
                "15000".to_string(),
            )],
        );
        let config = CayenneAccelerator::get_vortex_config("full_pinned", &pinned)
            .await
            .expect("config should be valid");
        assert_eq!(config.compaction_background_interval_ms, 15_000);

        // CDC is untouched: tight cadence, inlining on.
        let cdc = build("cdc_tbl", RefreshMode::Changes, vec![]);
        let config = CayenneAccelerator::get_vortex_config("cdc_tbl", &cdc)
            .await
            .expect("config should be valid");
        assert_eq!(
            config.compaction_background_interval_ms,
            SMALL_WRITE_COMPACTION_BACKGROUND_INTERVAL_MS
        );
        assert_eq!(config.inline_max_rows, SMALL_WRITE_INLINE_MAX_ROWS);

        // Snapshot mode accumulates files, so it keeps the conservative cadence and
        // stays OUT of the inline tier (its entries would never be drained promptly).
        let snapshot = build("snap_tbl", RefreshMode::Snapshot, vec![]);
        let config = CayenneAccelerator::get_vortex_config("snap_tbl", &snapshot)
            .await
            .expect("config should be valid");
        assert_ne!(
            config.compaction_background_interval_ms, 0,
            "snapshot mode still accumulates files to consolidate"
        );
        assert_eq!(config.inline_max_rows, 0);
    }

    /// `deletion_mode: auto` resolves to `key` ONLY for `refresh_mode`: changes
    /// datasets whose workload has a primary key; explicit configs and every
    /// other profile keep their value (and Auto's downstream position
    /// resolution) untouched.
    #[tokio::test]
    async fn test_deletion_mode_auto_resolves_to_key_for_cdc_pk_tables() {
        let build = |name: &str, refresh_mode: RefreshMode, params: Vec<(String, String)>| {
            let mut ds = TestAccelerationSource::new(name);
            ds.set_acceleration(Acceleration {
                engine: Engine::Cayenne,
                mode: Mode::File,
                refresh_mode: Some(refresh_mode),
                params: params.into_iter().collect(),
                ..Default::default()
            });
            ds
        };
        let pk_workload = autotune::WorkloadProfile {
            small_write: true,
            has_primary_key: true,
            is_upsert: true,
            pk_arity: 1,
            ..Default::default()
        };

        // CDC (changes) + PK + unset mode → auto-resolves to Key.
        let ds = build("cdc_pk", RefreshMode::Changes, vec![]);
        let config = CayenneAccelerator::get_vortex_config_with_footer_cache(
            "cdc_pk",
            &ds,
            None,
            &pk_workload,
        )
        .await
        .expect("config should be valid");
        assert_eq!(config.deletion_mode, cayenne::metadata::DeletionMode::Key);

        // Explicit `position` on the same shape is respected.
        let ds = build(
            "cdc_pk_pos",
            RefreshMode::Changes,
            vec![("cayenne_deletion_mode".to_string(), "position".to_string())],
        );
        let config = CayenneAccelerator::get_vortex_config_with_footer_cache(
            "cdc_pk_pos",
            &ds,
            None,
            &pk_workload,
        )
        .await
        .expect("config should be valid");
        assert_eq!(
            config.deletion_mode,
            cayenne::metadata::DeletionMode::Position
        );

        // CDC without a PK stays Auto (downstream resolution: position — the
        // only mechanism a PK-less table has).
        let ds = build("cdc_nopk", RefreshMode::Changes, vec![]);
        let nopk_workload = autotune::WorkloadProfile {
            small_write: true,
            ..Default::default()
        };
        let config = CayenneAccelerator::get_vortex_config_with_footer_cache(
            "cdc_nopk",
            &ds,
            None,
            &nopk_workload,
        )
        .await
        .expect("config should be valid");
        assert_eq!(config.deletion_mode, cayenne::metadata::DeletionMode::Auto);

        // A non-CDC profile with a PK stays Auto (position downstream).
        let ds = build("full_pk", RefreshMode::Full, vec![]);
        let config = CayenneAccelerator::get_vortex_config_with_footer_cache(
            "full_pk",
            &ds,
            None,
            &pk_workload,
        )
        .await
        .expect("config should be valid");
        assert_eq!(config.deletion_mode, cayenne::metadata::DeletionMode::Auto);
    }

    fn datalake_test_options(
        primary_key: Vec<String>,
        vortex_config: cayenne::metadata::VortexConfig,
    ) -> cayenne::metadata::CreateTableOptions {
        cayenne::metadata::CreateTableOptions {
            table_name: "dl_t".to_string(),
            schema: Arc::new(arrow_schema::Schema::new(vec![
                arrow_schema::Field::new("id", arrow_schema::DataType::Int64, false),
                arrow_schema::Field::new("value", arrow_schema::DataType::Int64, false),
            ])),
            primary_key,
            on_conflict: None,
            base_path: "/tmp/dl_t".to_string(),
            partition_column: None,
            vortex_config,
        }
    }

    fn datalake_enabled_config() -> cayenne::metadata::VortexConfig {
        cayenne::metadata::VortexConfig {
            cold_tier_location: Some("s3://bucket/prefix".to_string()),
            ..Default::default()
        }
    }

    #[test]
    fn test_validate_datalake_disabled_tier_is_silent() {
        let options = datalake_test_options(vec![], cayenne::metadata::VortexConfig::default());
        let warnings = validate_datalake_table_options("dl_t", &options)
            .expect("disabled tier validates cleanly");
        assert!(warnings.is_empty(), "disabled tier emits no warnings");
    }

    #[test]
    fn test_validate_datalake_valid_config_is_silent() {
        let options = datalake_test_options(vec!["id".to_string()], datalake_enabled_config());
        let warnings = validate_datalake_table_options("dl_t", &options)
            .expect("well-formed datalake config validates cleanly");
        assert!(warnings.is_empty(), "well-formed config emits no warnings");
    }

    #[test]
    fn test_validate_datalake_warns_pk_less_table_tier_inactive() {
        let options = datalake_test_options(vec![], datalake_enabled_config());
        let warnings = validate_datalake_table_options("dl_t", &options)
            .expect("a PK-less datalake table registers (tier inactive), it must not fail");
        assert_eq!(warnings.len(), 1, "exactly the inactive-tier warning");
        assert!(
            warnings[0].contains("INACTIVE"),
            "unexpected warning: {}",
            warnings[0]
        );
    }

    #[test]
    fn test_validate_datalake_rejects_explicit_position_deletes() {
        let config = cayenne::metadata::VortexConfig {
            deletion_mode: cayenne::metadata::DeletionMode::Position,
            ..datalake_enabled_config()
        };
        let options = datalake_test_options(vec!["id".to_string()], config);
        let error = validate_datalake_table_options("dl_t", &options)
            .expect_err("explicit position deletes must fail registration");
        assert!(
            error.contains("cayenne_deletion_mode: position"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn test_validate_datalake_rejects_zero_tiering_check_interval() {
        let config = cayenne::metadata::VortexConfig {
            cold_tier_background_interval_ms: 0,
            ..datalake_enabled_config()
        };
        let options = datalake_test_options(vec!["id".to_string()], config);
        let error = validate_datalake_table_options("dl_t", &options)
            .expect_err("tiering-check interval 0 must fail registration");
        assert!(
            error.contains("cayenne_datalake_tiering_check_interval_ms"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn test_validate_datalake_rejects_zero_gc_interval() {
        let config = cayenne::metadata::VortexConfig {
            cold_tier_gc_interval_ms: 0,
            ..datalake_enabled_config()
        };
        let options = datalake_test_options(vec!["id".to_string()], config);
        let error = validate_datalake_table_options("dl_t", &options)
            .expect_err("GC interval 0 must fail registration");
        assert!(
            error.contains("cayenne_datalake_gc_interval_ms"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn test_validate_datalake_warns_on_unknown_clustering_column() {
        let config = cayenne::metadata::VortexConfig {
            cold_clustering_columns: vec!["id".to_string(), "no_such_column".to_string()],
            ..datalake_enabled_config()
        };
        let options = datalake_test_options(vec!["id".to_string()], config);
        let warnings = validate_datalake_table_options("dl_t", &options)
            .expect("unknown clustering column is a warning, not an error");
        assert_eq!(warnings.len(), 1, "exactly the unknown column is flagged");
        assert!(
            warnings[0].contains("no_such_column"),
            "unexpected warning: {}",
            warnings[0]
        );
    }

    #[tokio::test]
    async fn test_inline_partial_override_preserves_refresh_profile_defaults() {
        let app = Arc::new(AppBuilder::new("test").build());

        let mut small_write_dataset =
            TestAccelerationSource::new("cdc_partial_override").with_app(Arc::clone(&app));
        small_write_dataset.set_acceleration(Acceleration {
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
                .await
                .expect("config should be valid");

        assert_eq!(small_write_config.inline_max_rows, 321);
        assert_eq!(
            small_write_config.inline_max_bytes,
            SMALL_WRITE_INLINE_MAX_BYTES
        );
        assert_eq!(
            small_write_config.inline_max_buffer_bytes,
            SMALL_WRITE_INLINE_MAX_BUFFER_BYTES
        );

        let mut large_write_dataset = TestAccelerationSource::new("full_partial_override");
        large_write_dataset.set_acceleration(Acceleration {
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
                .await
                .expect("config should be valid");

        // Bulk-overwrite inlines too, on the same static caps as small-write, so
        // the un-overridden knobs keep those defaults rather than the zeros that
        // meant "this profile never inlines".
        assert_eq!(large_write_config.inline_max_rows, 321);
        assert_eq!(
            large_write_config.inline_max_bytes,
            SMALL_WRITE_INLINE_MAX_BYTES
        );
        assert_eq!(
            large_write_config.inline_max_buffer_bytes,
            SMALL_WRITE_INLINE_MAX_BUFFER_BYTES
        );
    }

    #[tokio::test]
    async fn test_compaction_thresholds_are_resolved_from_acceleration_params() {
        let app = Arc::new(AppBuilder::new("test").build());

        let mut dataset = TestAccelerationSource::new("compact").with_app(Arc::clone(&app));
        dataset.set_acceleration(Acceleration {
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

        let config = CayenneAccelerator::get_vortex_config("compact", &dataset)
            .await
            .expect("config should be valid");

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

    /// A partitioned Cayenne table keeps one Vortex table per partition, each
    /// with its own stored schema, and `evolve_table_schema` can only reach the
    /// parent catalog entry. Reporting success there would advance the dataset
    /// checkpoint past a change no partition ever applied — every later restart
    /// then classifies source and checkpoint as identical, so the acceleration
    /// stays on the old schema forever while writes narrow-cast into it, and the
    /// caller's `file_update` / `drop_and_recreate` recreate never runs (#12999).
    #[tokio::test]
    async fn evolving_a_partitioned_table_in_place_is_refused() {
        use arrow_tools::schema_evolution::WideningPlan;
        use spicepod::partitioning::PartitionedBy;

        let app = Arc::new(AppBuilder::new("test").build());
        // Keep the metastore this test may open inside a temp dir rather than the
        // process-wide Spice data path.
        let metadata_dir = tempfile::TempDir::new().expect("tempdir");
        let build = |partition_by: Vec<PartitionedBy>| {
            let mut dataset = TestAccelerationSource::new("users").with_app(Arc::clone(&app));
            dataset.set_acceleration(Acceleration {
                engine: Engine::Cayenne,
                mode: Mode::FileUpdate,
                refresh_mode: Some(RefreshMode::Full),
                partition_by,
                params: [(
                    "cayenne_metadata_dir".to_string(),
                    metadata_dir.path().to_string_lossy().to_string(),
                )]
                .into_iter()
                .collect(),
                ..Default::default()
            });
            dataset
        };

        let plan = WideningPlan {
            added_columns: vec![Arc::new(Field::new("added", DataType::Utf8, true))],
            widened_columns: Vec::new(),
            relaxed_nullability: Vec::new(),
            evolved_schema: Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("added", DataType::Utf8, true),
            ])),
        };

        let partitioned = build(vec![PartitionedBy {
            name: "bucket".to_string(),
            expression: "bucket".to_string(),
        }]);
        let error = CayenneAccelerator::new()
            .evolve_table_schema("users", &partitioned, &plan)
            .await
            .expect_err("in-place evolution of a partitioned table must be refused");
        let message = error.to_string();
        assert!(
            message.contains("not supported for a partitioned acceleration"),
            "the refusal must name partitioning as the reason, got: {message}"
        );
        assert!(
            message.contains("drop_and_recreate") && message.contains("file_update"),
            "the refusal must point at the settings that rebuild the table, got: {message}"
        );

        // The unpartitioned path is untouched: it still reaches the metastore,
        // which is what the refusal above must not pre-empt. `users` was never
        // created, so it fails on the missing table rather than on partitioning.
        let unpartitioned_error = CayenneAccelerator::new()
            .evolve_table_schema("users", &build(Vec::new()), &plan)
            .await
            .expect_err("no such table exists in a fresh metastore");
        assert!(
            !unpartitioned_error
                .to_string()
                .contains("partitioned acceleration"),
            "an unpartitioned table must not take the partitioned refusal, got: {unpartitioned_error}"
        );
    }

    /// The vortex config built for a partitioned dataset also opens the PARENT
    /// catalog entry, which is created before the partition wrapper exists. If
    /// it carried an evolution mode, the catalog would widen the parent's stored
    /// schema at open while every partition kept its own — the same silent
    /// narrowing as #12999, reached without ever calling `evolve_table_schema`.
    #[tokio::test]
    async fn a_partitioned_dataset_never_carries_a_schema_evolution_mode() {
        use spicepod::partitioning::PartitionedBy;

        let build = |partition_by: Vec<PartitionedBy>, policy| {
            let mut dataset = TestAccelerationSource::new("users");
            dataset.set_on_schema_change(policy);
            dataset.set_acceleration(Acceleration {
                engine: Engine::Cayenne,
                mode: Mode::File,
                partition_by,
                ..Default::default()
            });
            dataset
        };
        let partitioned_by_bucket = || {
            vec![PartitionedBy {
                name: "bucket".to_string(),
                expression: "bucket".to_string(),
            }]
        };
        let workload = autotune::WorkloadProfile::default();
        let evolution_mode = async |dataset: &TestAccelerationSource| {
            CayenneAccelerator::get_vortex_config_with_footer_cache(
                "users", dataset, None, &workload,
            )
            .await
            .expect("the vortex config is built")
            .schema_evolution
        };

        assert!(
            evolution_mode(&build(
                partitioned_by_bucket(),
                OnSchemaChange::SyncAllColumns
            ))
            .await
            .is_disabled(),
            "a partitioned dataset must not carry an evolution mode, whatever its policy asks for"
        );

        // The same policy on an unpartitioned dataset still evolves: the guard
        // above must key on partitioning, not disable evolution outright.
        assert!(
            !evolution_mode(&build(Vec::new(), OnSchemaChange::SyncAllColumns))
                .await
                .is_disabled(),
            "an unpartitioned dataset keeps in-place evolution"
        );
    }
    /// Datasets sharing one metadata directory share its `SQLite` catalog, so a pod where
    /// some snapshot and others do not cannot be restored consistently and must be refused
    /// up front. That check is generic — it groups by
    /// [`DataAccelerator::shared_store_key`] — so it silently passes for every Cayenne
    /// dataset if this engine does not answer that question. Regression test for exactly
    /// that: the validation moved out of `runtime` when the engine did, and an unimplemented
    /// `shared_store_key` would leave it looking green while checking nothing.
    #[tokio::test]
    async fn mixed_snapshot_settings_in_one_metadata_dir_are_refused() {
        use data_accelerator_api::validate_snapshot_consistency;
        use runtime_acceleration::snapshot::SnapshotBehavior;
        use runtime_acceleration::testing::TestAccelerationSource;
        use spicepod::acceleration::SnapshotsCompaction;
        use spicepod::component::snapshot::Snapshots;
        use std::sync::Weak;

        let dir = std::env::temp_dir()
            .join("spice_cayenne_shared_metastore")
            .to_string_lossy()
            .to_string();
        let acceleration = |snapshots: bool| {
            let mut acceleration = Acceleration {
                engine: Engine::Cayenne,
                mode: Mode::File,
                params: [("cayenne_metadata_dir".to_string(), dir.clone())]
                    .into_iter()
                    .collect(),
                ..Default::default()
            };
            // `Disabled` is the default, so the *enabled* side is what has to be built
            // explicitly — a test that left both at the default would compare nothing.
            if snapshots {
                acceleration.snapshot_behavior = SnapshotBehavior::Enabled(
                    Arc::new(Snapshots::default()),
                    Weak::new(),
                    tokio::runtime::Handle::current(),
                    SnapshotsCompaction::Disabled,
                );
            }
            acceleration
        };

        // Both sides of the disagreement, in the same directory.
        let sources: Vec<Arc<dyn AccelerationSource>> = vec![
            Arc::new(
                TestAccelerationSource::new("snapshotting").with_acceleration(acceleration(true)),
            ),
            Arc::new(
                TestAccelerationSource::new("not_snapshotting")
                    .with_acceleration(acceleration(false)),
            ),
        ];
        assert!(
            validate_snapshot_consistency(&sources).is_err(),
            "a metadata directory with both snapshotting and non-snapshotting datasets must be refused"
        );

        // Agreeing datasets in the same directory are supported.
        let agreeing: Vec<Arc<dyn AccelerationSource>> = vec![
            Arc::new(TestAccelerationSource::new("a").with_acceleration(acceleration(true))),
            Arc::new(TestAccelerationSource::new("b").with_acceleration(acceleration(true))),
        ];
        assert!(
            validate_snapshot_consistency(&agreeing).is_ok(),
            "datasets that agree may share a metadata directory"
        );
    }
}
