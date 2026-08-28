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

use crate::Engine;
use crate::snapshot::SnapshotBehavior;
use arrow::datatypes::SchemaRef;
use datafusion::common::{Constraint, Constraints};
use datafusion_table_providers::util::{
    column_reference::ColumnReference, constraints::UpsertOptions, on_conflict::OnConflict,
};
use serde::{Deserialize, Serialize};
use snafu::Snafu;
use spicepod::acceleration::{SnapshotsCompaction, SnapshotsCreationPolicy, SnapshotsTrigger};
use spicepod::{
    acceleration::{self as spicepod_acceleration},
    param::Params,
    partitioning::PartitionedBy,
};
use std::{collections::HashMap, fmt::Display, sync::Arc, time::Duration};

/// Errors that can occur when parsing acceleration configuration.
#[derive(Debug, Snafu)]
pub enum ParseError {
    #[snafu(display("Unable to parse column reference {column_ref}: {source}"))]
    UnableToParseColumnReference {
        column_ref: String,
        source: datafusion_table_providers::util::column_reference::Error,
    },

    #[snafu(display("Error parsing {field} as duration: {source}"))]
    UnableToParseFieldAsDuration {
        field: String,
        source: fundu::ParseError,
    },

    #[snafu(display("Both 'refresh_cron' and 'refresh_check_interval' were specified"))]
    MultipleRefreshExpressionSpecified,

    #[snafu(display("Unknown acceleration engine '{name}'"))]
    AcceleratorEngineNotAvailable { name: String },

    #[snafu(display("Invalid acceleration configuration: {detail}"))]
    InvalidAccelerationConfiguration { detail: String },

    #[snafu(display(
        "Column for index '{index}' was not found in the schema. Valid columns: {valid_columns}"
    ))]
    IndexColumnNotFound {
        index: String,
        valid_columns: String,
    },

    #[snafu(display(
        "Primary key column '{invalid_column}' was not found in the schema. Valid columns: {valid_columns}"
    ))]
    PrimaryKeyColumnNotFound {
        invalid_column: String,
        valid_columns: String,
    },

    #[snafu(display(
        "Cannot configure {constraint} because the dataset schema has no columns. This usually means the source table does not exist or could not be read. Verify the dataset's `from` target exists and is accessible, then try again."
    ))]
    AcceleratedSchemaEmpty { constraint: String },

    #[snafu(display("Failed to retrieve table constraints: {source}"))]
    UnableToGetTableConstraints {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Only one on_conflict target can be specified: {extra_detail}"))]
    OnConflictTargetMismatch { extra_detail: String },
}

// ── Enums ────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub enum RefreshMode {
    Disabled,
    Full,
    Append,
    Changes,
    Caching,
    /// Reload accelerator data from newer snapshots only; the federated
    /// source is never queried for refreshes.
    Snapshot,
}

impl RefreshMode {
    /// Returns true if this refresh mode never reads from the federated source.
    #[must_use]
    pub const fn is_snapshot_only(&self) -> bool {
        matches!(self, RefreshMode::Snapshot)
    }
}

impl From<spicepod_acceleration::RefreshMode> for RefreshMode {
    fn from(refresh_mode: spicepod_acceleration::RefreshMode) -> Self {
        match refresh_mode {
            spicepod_acceleration::RefreshMode::Full => RefreshMode::Full,
            spicepod_acceleration::RefreshMode::Append => RefreshMode::Append,
            spicepod_acceleration::RefreshMode::Changes => RefreshMode::Changes,
            spicepod_acceleration::RefreshMode::Caching => RefreshMode::Caching,
            spicepod_acceleration::RefreshMode::Snapshot => RefreshMode::Snapshot,
        }
    }
}

/// The refresh mode a dataset runs with when its `refresh_mode` is left unset, keyed
/// by CONNECTOR NAME.
///
/// An unset mode is filled in by the *connector*
/// (`DataConnector::resolve_refresh_mode`), not by a fixed default, and that result is
/// never written back into [`Acceleration`] — so `acceleration.refresh_mode` stays
/// `None` for a genuine `debezium:`/`cdc:` stream. A consumer that must know the mode
/// a source will actually run with maps the source's connector name through here.
/// Mirrors the three overrides that differ from the trait default; keep it in sync with
/// them.
///
/// Getting this wrong in the `full` direction is the dangerous one — it would classify
/// an unannotated CDC dataset as a whole-table replace and under-provision its memory —
/// so an unrecognized connector resolves to `full` only because that IS the trait
/// default, not as a guess.
///
/// Takes the parsed connector name, never a raw `from:` value: re-parsing an
/// already-parsed name would be wrong, because `debezium` has no delimiter and a second
/// parse would read it as the `spice.ai` connector. Parsing is the caller's job.
#[must_use]
pub fn unset_refresh_mode_for_connector(connector: &str) -> RefreshMode {
    match connector {
        // Both resolve an unset mode to `changes`.
        "debezium" | "cdc" => RefreshMode::Changes,
        // Resolves to `disabled`: no refresh runs, but rows arrive by `INSERT INTO`
        // and accumulate, so files still need consolidating.
        "sink" => RefreshMode::Disabled,
        // `DataConnector::resolve_refresh_mode`'s default is `full`.
        _ => RefreshMode::Full,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum Mode {
    #[default]
    Memory,
    /// Open an existing file if it exists, otherwise create a new one.
    /// This is the default file behavior that preserves data across restarts.
    File,
    /// Always create a new file, truncating/overwriting any existing file on startup.
    /// Use this when you want a fresh acceleration on each startup.
    /// With `snapshots: enabled` the outgoing file is still snapshotted, but no
    /// snapshot is bootstrapped back in — the next refresh rebuilds from the
    /// source. Datasets whose refresh never reads a source still bootstrap,
    /// because there the snapshot is the only copy of the data: `refresh_mode:
    /// snapshot`, whose source is the snapshot store, and `sink:` datasets,
    /// which never refresh at all.
    FileCreate,
    /// Open an existing file if it exists, then check schema compatibility on refresh.
    /// If the source schema is incompatible (non-additive change), snapshot (if enabled)
    /// and recreate the acceleration file from scratch.
    FileUpdate,
}

impl From<spicepod_acceleration::Mode> for Mode {
    fn from(mode: spicepod_acceleration::Mode) -> Self {
        match mode {
            spicepod_acceleration::Mode::Memory => Mode::Memory,
            spicepod_acceleration::Mode::File => Mode::File,
            spicepod_acceleration::Mode::FileCreate => Mode::FileCreate,
            spicepod_acceleration::Mode::FileUpdate => Mode::FileUpdate,
        }
    }
}

/// The inverse mapping, for callers that synthesize a Spicepod component from
/// already-parsed runtime config (see the `PostgreSQL` catalog connector, which
/// builds a Spicepod `Dataset` per discovered table from the catalog's own
/// acceleration settings).
impl From<Mode> for spicepod_acceleration::Mode {
    fn from(mode: Mode) -> Self {
        match mode {
            Mode::Memory => spicepod_acceleration::Mode::Memory,
            Mode::File => spicepod_acceleration::Mode::File,
            Mode::FileCreate => spicepod_acceleration::Mode::FileCreate,
            Mode::FileUpdate => spicepod_acceleration::Mode::FileUpdate,
        }
    }
}

impl Display for Mode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Mode::Memory => write!(f, "memory"),
            Mode::File => write!(f, "file"),
            Mode::FileCreate => write!(f, "file_create"),
            Mode::FileUpdate => write!(f, "file_update"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StorageProfile {
    #[default]
    Auto,
    LocalSsd,
    Ebs,
    Tmpfs,
}

impl From<spicepod_acceleration::StorageProfile> for StorageProfile {
    fn from(storage: spicepod_acceleration::StorageProfile) -> Self {
        match storage {
            spicepod_acceleration::StorageProfile::Auto => StorageProfile::Auto,
            spicepod_acceleration::StorageProfile::LocalSsd => StorageProfile::LocalSsd,
            spicepod_acceleration::StorageProfile::Ebs => StorageProfile::Ebs,
            spicepod_acceleration::StorageProfile::Tmpfs => StorageProfile::Tmpfs,
        }
    }
}

impl Display for StorageProfile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageProfile::Auto => write!(f, "auto"),
            StorageProfile::LocalSsd => write!(f, "local_ssd"),
            StorageProfile::Ebs => write!(f, "ebs"),
            StorageProfile::Tmpfs => write!(f, "tmpfs"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum RefreshOnStartup {
    /// Always start a new refresh when Spice starts.
    Always,
    /// Only start a refresh if an existing acceleration is not available.
    #[default]
    Auto,
}

impl From<spicepod_acceleration::RefreshOnStartup> for RefreshOnStartup {
    fn from(refresh_on_startup: spicepod_acceleration::RefreshOnStartup) -> Self {
        match refresh_on_startup {
            spicepod_acceleration::RefreshOnStartup::Always => RefreshOnStartup::Always,
            spicepod_acceleration::RefreshOnStartup::Auto => RefreshOnStartup::Auto,
        }
    }
}

impl Display for RefreshOnStartup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RefreshOnStartup::Always => write!(f, "always"),
            RefreshOnStartup::Auto => write!(f, "auto"),
        }
    }
}

/// Behavior when a query on an accelerated table returns zero results.
#[derive(Debug, Clone, PartialEq, Default)]
pub enum ZeroResultsAction {
    /// Return an empty result set. This is the default.
    #[default]
    ReturnEmpty,
    /// Fallback to querying the source table.
    UseSource,
}

impl From<spicepod_acceleration::ZeroResultsAction> for ZeroResultsAction {
    fn from(zero_results_action: spicepod_acceleration::ZeroResultsAction) -> Self {
        match zero_results_action {
            spicepod_acceleration::ZeroResultsAction::ReturnEmpty => ZeroResultsAction::ReturnEmpty,
            spicepod_acceleration::ZeroResultsAction::UseSource => ZeroResultsAction::UseSource,
        }
    }
}

impl Display for ZeroResultsAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ZeroResultsAction::ReturnEmpty => write!(f, "return_empty"),
            ZeroResultsAction::UseSource => write!(f, "use_source"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum IndexType {
    #[default]
    Enabled,
    Unique,
}

impl From<spicepod_acceleration::IndexType> for IndexType {
    fn from(index_type: spicepod_acceleration::IndexType) -> Self {
        match index_type {
            spicepod_acceleration::IndexType::Enabled => IndexType::Enabled,
            spicepod_acceleration::IndexType::Unique => IndexType::Unique,
        }
    }
}

impl From<&str> for IndexType {
    fn from(index_type: &str) -> Self {
        match index_type.to_lowercase().as_str() {
            "unique" => IndexType::Unique,
            _ => IndexType::Enabled,
        }
    }
}

impl Display for IndexType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IndexType::Enabled => write!(f, "enabled"),
            IndexType::Unique => write!(f, "unique"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
pub enum OnConflictBehavior {
    #[default]
    Drop,
    Upsert(UpsertOptions),
}

impl From<spicepod_acceleration::OnConflictBehavior> for OnConflictBehavior {
    fn from(index_type: spicepod_acceleration::OnConflictBehavior) -> Self {
        match index_type {
            spicepod_acceleration::OnConflictBehavior::Drop => OnConflictBehavior::Drop,
            spicepod_acceleration::OnConflictBehavior::Upsert => {
                OnConflictBehavior::Upsert(UpsertOptions::default())
            }
            spicepod_acceleration::OnConflictBehavior::UpsertDedup => {
                OnConflictBehavior::Upsert(UpsertOptions::default().with_remove_duplicates(true))
            }
            spicepod_acceleration::OnConflictBehavior::UpsertDedupByRowId => {
                OnConflictBehavior::Upsert(UpsertOptions::default().with_last_write_wins(true))
            }
        }
    }
}

impl From<&str> for OnConflictBehavior {
    fn from(index_type: &str) -> Self {
        match index_type.to_lowercase().as_str() {
            "upsert" => OnConflictBehavior::Upsert(UpsertOptions::default()),
            _ => OnConflictBehavior::Drop,
        }
    }
}

impl Display for OnConflictBehavior {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OnConflictBehavior::Drop => write!(f, "drop"),
            OnConflictBehavior::Upsert(_options) => write!(f, "upsert"),
        }
    }
}

/// Behavior when a stale-if-error condition occurs in caching mode.
/// When enabled, serves expired cached data if the upstream source returns an error.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum StaleIfError {
    /// Do not serve stale data on error - propagate the error to the client.
    #[default]
    Disabled,
    /// Serve expired data if the upstream source returns an error.
    Enabled,
}

impl StaleIfError {
    #[must_use]
    pub fn is_enabled(self) -> bool {
        matches!(self, StaleIfError::Enabled)
    }
}

impl Display for StaleIfError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StaleIfError::Disabled => write!(f, "disabled"),
            StaleIfError::Enabled => write!(f, "enabled"),
        }
    }
}

// ── Acceleration struct ───────────────────────────────────────────────────────

#[expect(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, PartialEq)]
pub struct Acceleration {
    pub enabled: bool,

    pub mode: Mode,

    pub engine: Engine,

    pub refresh_mode: Option<RefreshMode>,

    pub refresh_on_startup: RefreshOnStartup,

    pub refresh_check_interval: Option<Duration>,

    pub caching_ttl: Option<Duration>,

    pub caching_stale_while_revalidate_ttl: Option<Duration>,

    pub caching_stale_if_error: StaleIfError,

    /// Byte budget for a `refresh_mode: caching` accelerator, from
    /// `caching_max_size`. `None` is no byte budget: what remains is expiry by
    /// `caching_ttl` — and with `caching_stale_if_error: enabled` not even that,
    /// since expired entries are deliberately kept as fallback for a failing
    /// origin, leaving nothing to bound the acceleration.
    ///
    /// The budget measures the payload each entry holds — its text and
    /// fixed-width columns — not bytes on disk, which the engine's own indexes
    /// and compression decide. Two things it does not charge for: a response's
    /// header map, whose size no expression here can measure, and any other
    /// column of a type that cannot be weighed, which is warned about by name at
    /// load. An origin sending unusually large headers therefore holds bytes
    /// outside this ceiling.
    pub caching_max_size: Option<u64>,

    /// Row budget for a `refresh_mode: caching` accelerator, from
    /// `caching_max_items`. `None` is no row budget; see [`Self::caching_max_size`]
    /// for what is left bounding the acceleration.
    pub caching_max_items: Option<u64>,

    pub refresh_cron: Option<Arc<str>>,

    pub refresh_sql: Option<String>,

    pub refresh_data_window: Option<String>,

    pub refresh_append_overlap: Option<Duration>,

    pub refresh_retry_enabled: bool,

    pub refresh_retry_max_attempts: Option<usize>,

    pub refresh_jitter_enabled: bool,

    pub refresh_jitter_max: Option<Duration>,

    pub params: HashMap<String, String>,

    pub retention_period: Option<String>,

    pub retention_sql: Option<String>,

    pub retention_check_interval: Option<String>,

    pub retention_check_enabled: bool,

    pub on_zero_results: ZeroResultsAction,

    pub indexes: HashMap<ColumnReference, IndexType>,

    pub primary_key: Option<ColumnReference>,

    pub on_conflict: HashMap<ColumnReference, OnConflictBehavior>,

    pub maintained_aggregates: spicepod_acceleration::MaintainedAggregates,

    pub write_mode: spicepod_acceleration::WriteMode,

    pub storage_profile: StorageProfile,

    pub disable_federation: bool,

    pub partition_by: Vec<PartitionedBy>,

    pub snapshot_behavior: SnapshotBehavior,

    pub snapshots_trigger: Option<SnapshotsTrigger>,

    pub snapshots_trigger_threshold: Option<String>,

    pub snapshots_compaction: SnapshotsCompaction,

    pub snapshots_reset_expiry_on_load_enabled: bool,

    pub snapshots_creation_policy: SnapshotsCreationPolicy,
}

impl Acceleration {
    #[must_use]
    pub fn with_primary_key(mut self, primary_key: ColumnReference) -> Self {
        self.primary_key = Some(primary_key);
        self
    }

    #[must_use]
    pub fn with_on_conflict(
        mut self,
        on_conflict: HashMap<ColumnReference, OnConflictBehavior>,
    ) -> Self {
        self.on_conflict = on_conflict;
        self
    }

    /// Returns whether this configuration resolves to durable federated
    /// write-back: a Cayenne `write_mode: write_back` dataset that declares an
    /// `on_conflict` key and is CDC-fed (`refresh_mode: changes`).
    ///
    /// Such a dataset marks every committed write's primary keys so a delivery
    /// worker can reconcile them to the federated source. Delivery mutates the
    /// source, so the source connector must advertise a delivery primitive that
    /// cannot lose the write; callers gate on
    /// `DataConnector::supports_durable_write_back_delivery` and reject the
    /// dataset at registration otherwise.
    ///
    /// This is the single source of truth for the condition: both the
    /// registration gate and the Cayenne provider that turns the marking on
    /// call it, so the check and the behavior cannot drift apart.
    #[must_use]
    pub fn resolves_to_durable_write_back(&self) -> bool {
        self.engine == Engine::Cayenne
            && self.write_mode == spicepod_acceleration::WriteMode::WriteBack
            && !self.on_conflict.is_empty()
            && self.refresh_mode == Some(RefreshMode::Changes)
    }

    /// Returns whether Arrow `hash_index` is enabled for primary key upserts or indexes.
    #[must_use]
    pub fn is_hash_index_enabled(&self) -> bool {
        matches!(self.engine, Engine::Arrow | Engine::PartitionedArrow)
            && self.enabled
            && (!self.indexes.is_empty()
                || (self.primary_key.is_some()
                    && !matches!(self.refresh_mode, Some(RefreshMode::Caching))))
    }

    // ── Constraints methods ───────────────────────────────────────────────────

    #[must_use]
    pub fn hashmap_to_option_string<K, V>(map: &HashMap<K, V>) -> String
    where
        K: Display,
        V: Display,
    {
        map.iter()
            .map(|(k, v)| format!("{k}:{v}"))
            .collect::<Vec<String>>()
            .join(";")
    }

    fn valid_columns(schema: &SchemaRef) -> String {
        schema
            .flattened_fields()
            .into_iter()
            .map(|f| f.name().clone())
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// Validates that all configured index columns exist in the source schema.
    ///
    /// # Errors
    ///
    /// Returns an error if an index is configured against an empty schema or if any configured
    /// index column is missing from the source schema.
    pub fn validate_indexes(&self, schema: &SchemaRef) -> Result<(), ParseError> {
        if !self.indexes.is_empty() {
            Self::ensure_schema_populated(schema, "indexes")?;
        }
        for column in self.indexes.keys() {
            for index_column in column.iter() {
                if schema.field_with_name(index_column).is_err() {
                    return IndexColumnNotFoundSnafu {
                        index: index_column.to_string(),
                        valid_columns: Self::valid_columns(schema),
                    }
                    .fail();
                }
            }
        }

        Ok(())
    }

    /// Validates that all configured primary key columns exist in the source schema.
    ///
    /// # Errors
    ///
    /// Returns an error if a primary key is configured against an empty schema or if any
    /// configured primary key column is missing from the source schema.
    pub fn validate_primary_key(&self, schema: &SchemaRef) -> Result<(), ParseError> {
        if let Some(columns) = &self.primary_key {
            Self::ensure_schema_populated(schema, "a primary key")?;
            for column in columns.iter() {
                if schema.field_with_name(column).is_err() {
                    return PrimaryKeyColumnNotFoundSnafu {
                        invalid_column: column.to_string(),
                        valid_columns: Self::valid_columns(schema),
                    }
                    .fail();
                }
            }
        }

        Ok(())
    }

    /// A configured constraint (primary key or index) can only be validated against a schema
    /// that actually has columns. An empty schema means the source table could not be resolved
    /// (e.g. it does not exist), so report that root cause instead of a misleading
    /// "column was not found. Valid columns: " message listing no valid columns.
    fn ensure_schema_populated(schema: &SchemaRef, constraint: &str) -> Result<(), ParseError> {
        if schema.fields().is_empty() {
            return AcceleratedSchemaEmptySnafu {
                constraint: constraint.to_string(),
            }
            .fail();
        }
        Ok(())
    }

    #[expect(clippy::needless_pass_by_value)]
    /// Builds `DataFusion` table constraints from configured indexes and primary keys.
    ///
    /// # Errors
    ///
    /// This method preserves a `Result`-based API for callers but does not currently return an
    /// error.
    pub fn table_constraints(&self, schema: SchemaRef) -> Result<Option<Constraints>, ParseError> {
        if self.indexes.is_empty() && self.primary_key.is_none() {
            tracing::trace!(
                "No indexes or primary key identified for accelerator table constraints",
            );
            return Ok(None);
        }

        tracing::trace!("Primary key definition: {:?}", self.primary_key);
        tracing::trace!("Indexes: {:?}", self.indexes);

        let mut table_constraints: Vec<Constraint> = Vec::new();

        for (column, index_type) in &self.indexes {
            match index_type {
                IndexType::Enabled => {}
                IndexType::Unique => {
                    let index_indices: Vec<usize> = column
                        .iter()
                        .filter_map(|c| schema.index_of(c).ok())
                        .collect();
                    let tc = Constraint::Unique(index_indices);

                    table_constraints.push(tc);
                }
            }
        }

        if let Some(primary_key) = &self.primary_key {
            let pk_indices: Vec<usize> = primary_key
                .iter()
                .filter_map(|c| schema.index_of(c).ok())
                .collect();
            let tc = Constraint::PrimaryKey(pk_indices);

            table_constraints.push(tc);
        }

        tracing::trace!("Table constraints: {table_constraints:?}");

        Ok(Some(Constraints::new_unverified(table_constraints)))
    }

    // ── On-conflict methods ───────────────────────────────────────────────────

    fn on_conflict_targets(&self) -> Vec<ColumnReference> {
        let mut on_conflict_targets: Vec<ColumnReference> = Vec::new();
        for (column, index_type) in &self.indexes {
            if *index_type == IndexType::Unique {
                on_conflict_targets.push(column.clone());
            }
        }
        if let Some(primary_key) = &self.primary_key {
            on_conflict_targets.push(primary_key.clone());
        }

        on_conflict_targets
    }

    /// Returns the conflict behavior implied by the configured unique constraints and primary key.
    ///
    /// # Errors
    ///
    /// Returns an error when the configured `on_conflict` targets do not match the available
    /// primary key and unique-index targets.
    pub fn on_conflict(&self) -> Result<Option<OnConflict>, ParseError> {
        let on_conflict_all_targets = self.on_conflict_targets();

        if self.on_conflict.len() > 1 && on_conflict_all_targets.len() != self.on_conflict.len() {
            return OnConflictTargetMismatchSnafu {
                extra_detail: format!(
                    "The number of column references gathered from primary key/unique indexes ({}) does not match the number of on_conflict targets ({})",
                    on_conflict_all_targets.len(),
                    self.on_conflict.len()
                ),
            }
            .fail();
        }

        if self.on_conflict.len() > 1
            && !self
                .on_conflict
                .iter()
                .all(|c| *c.1 == OnConflictBehavior::Drop)
        {
            return OnConflictTargetMismatchSnafu {
                extra_detail: String::new(),
            }
            .fail();
        } else if self.on_conflict.len() > 1 {
            return Ok(Some(OnConflict::DoNothingAll));
        }

        let Some(on_conflict) = self.on_conflict.iter().next() else {
            return Ok(None);
        };

        match on_conflict.1 {
            OnConflictBehavior::Drop => Ok(Some(OnConflict::DoNothing(on_conflict.0.clone()))),
            OnConflictBehavior::Upsert(_options) => {
                Ok(Some(OnConflict::Upsert(on_conflict.0.clone())))
            }
        }
    }

    /// Returns the `UpsertOptions` if the `on_conflict` behavior is `Upsert`.
    /// Returns `UpsertOptions::default()` if no `on_conflict` is set.
    #[must_use]
    pub fn upsert_options(&self) -> UpsertOptions {
        self.on_conflict
            .values()
            .find_map(|behavior| {
                if let OnConflictBehavior::Upsert(options) = behavior {
                    Some(options.clone())
                } else {
                    None
                }
            })
            .unwrap_or_default()
    }
}

impl TryFrom<spicepod_acceleration::Acceleration> for Acceleration {
    type Error = ParseError;

    fn try_from(
        acceleration: spicepod_acceleration::Acceleration,
    ) -> std::result::Result<Self, Self::Error> {
        let try_parse_column_reference = |column: &str| {
            ColumnReference::try_from(column).map_err(|e| {
                ParseError::UnableToParseColumnReference {
                    column_ref: column.to_string(),
                    source: e,
                }
            })
        };

        let try_parse_duration = |field: &str, duration: Option<String>| {
            let Some(duration) = duration else {
                return Ok(None);
            };
            fundu::parse_duration(&duration).map(Some).map_err(|e| {
                ParseError::UnableToParseFieldAsDuration {
                    source: e,
                    field: field.into(),
                }
            })
        };

        let primary_key = match acceleration.primary_key {
            Some(pk) => Some(try_parse_column_reference(pk.as_str())?),
            None => None,
        };

        let mut indexes = HashMap::new();
        for (k, v) in acceleration.indexes {
            indexes.insert(try_parse_column_reference(k.as_str())?, IndexType::from(v));
        }

        let mut on_conflict = HashMap::new();
        for (k, v) in acceleration.on_conflict {
            on_conflict.insert(
                try_parse_column_reference(k.as_str())?,
                OnConflictBehavior::from(v),
            );
        }

        let mut params = acceleration.params.clone();

        let engine_str = acceleration.engine.as_deref().unwrap_or("arrow");
        let engine = match Engine::try_from(engine_str).map_err(|_| {
            ParseError::AcceleratorEngineNotAvailable {
                name: engine_str.to_string(),
            }
        })? {
            Engine::Arrow if !acceleration.partition_by.is_empty() => Engine::PartitionedArrow,
            engine => engine,
        };

        if matches!(engine, Engine::Arrow | Engine::PartitionedArrow)
            && let Some(params) = &mut params
            && params.data.remove("hash_index").is_some()
        {
            tracing::warn!(
                "The hash_index acceleration parameter is ignored for Arrow acceleration; hash_index alone no longer enables indexing. Hash indexes are automatically enabled only when primary_key or indexes are configured."
            );
        }
        // Note: The warning for hash_index being experimental is logged once
        // at dataset registration time in init/dataset.rs, not during parsing.
        if matches!(engine, Engine::Arrow | Engine::PartitionedArrow)
            && !on_conflict.is_empty()
            && primary_key.is_none()
        {
            tracing::warn!(
                "Conflict resolution for Arrow engine acceleration requires primary_key. Ignoring on_conflict."
            );
        }

        if matches!(
            acceleration.snapshots_reset_expiry_on_load,
            spicepod_acceleration::SnapshotsResetExpiryOnLoad::Enabled
        ) && (engine != Engine::DuckDB
            || !matches!(
                acceleration.refresh_mode,
                Some(spicepod_acceleration::RefreshMode::Caching)
            ))
        {
            tracing::warn!(
                "Resetting expiry on load is only supported for DuckDB engine acceleration with caching refresh mode. Ignoring snapshots_reset_expiry_on_load."
            );
        }

        let disable_federation = parse_is_query_federation_disabled(&mut params)?;

        let caching_ttl = parse_caching_ttl(&mut params)?;
        let caching_stale_while_revalidate_ttl =
            parse_caching_stale_while_revalidate_ttl(&mut params)?;
        let caching_stale_if_error = parse_caching_stale_if_error(&mut params)?;
        let caching_max_size = parse_caching_max_size(&mut params)?;
        let caching_max_items = parse_caching_max_items(&mut params)?;

        let refresh_check_interval = try_parse_duration(
            "refresh_check_interval",
            acceleration.refresh_check_interval,
        )?;

        let refresh_cron = acceleration.refresh_cron.map(Into::into);
        if refresh_cron.is_some() && refresh_check_interval.is_some() {
            return Err(ParseError::MultipleRefreshExpressionSpecified);
        }

        let refresh_jitter_max =
            try_parse_duration("refresh_jitter_max", acceleration.refresh_jitter_max)?;

        Ok(Acceleration {
            enabled: acceleration.enabled,
            mode: Mode::from(acceleration.mode),
            engine,
            refresh_mode: acceleration.refresh_mode.map(RefreshMode::from),
            refresh_on_startup: RefreshOnStartup::from(acceleration.refresh_on_startup),
            refresh_check_interval,
            caching_ttl,
            caching_stale_while_revalidate_ttl,
            caching_stale_if_error,
            caching_max_size,
            caching_max_items,
            refresh_cron,
            refresh_sql: acceleration.refresh_sql,
            refresh_data_window: acceleration.refresh_data_window,
            refresh_append_overlap: try_parse_duration(
                "refresh_append_overlap",
                acceleration.refresh_append_overlap,
            )?,
            refresh_retry_enabled: acceleration.refresh_retry_enabled,
            refresh_retry_max_attempts: acceleration.refresh_retry_max_attempts,
            refresh_jitter_max,
            refresh_jitter_enabled: acceleration.refresh_jitter_enabled,
            params: params
                .as_ref()
                .map(Params::as_string_map)
                .unwrap_or_default(),
            retention_period: acceleration.retention_period,
            retention_sql: acceleration.retention_sql,
            retention_check_interval: acceleration.retention_check_interval,
            retention_check_enabled: acceleration.retention_check_enabled,
            disable_federation,
            on_zero_results: ZeroResultsAction::from(acceleration.on_zero_results),
            indexes,
            primary_key,
            on_conflict,
            maintained_aggregates: acceleration.maintained_aggregates,
            write_mode: acceleration.write_mode,
            storage_profile: StorageProfile::from(acceleration.storage_profile),
            partition_by: acceleration.partition_by,
            snapshot_behavior: SnapshotBehavior::disabled(),
            snapshots_trigger: acceleration.snapshots_trigger,
            snapshots_trigger_threshold: acceleration.snapshots_trigger_threshold,
            snapshots_compaction: acceleration.snapshots_compaction,
            snapshots_reset_expiry_on_load_enabled: matches!(
                acceleration.snapshots_reset_expiry_on_load,
                spicepod_acceleration::SnapshotsResetExpiryOnLoad::Enabled
            ),
            snapshots_creation_policy: acceleration.snapshots_creation_policy,
        })
    }
}

impl Default for Acceleration {
    fn default() -> Self {
        Self {
            enabled: true,
            mode: Mode::Memory,
            engine: Engine::default(),
            refresh_mode: None,
            refresh_check_interval: None,
            caching_ttl: None,
            caching_stale_while_revalidate_ttl: None,
            caching_stale_if_error: StaleIfError::default(),
            caching_max_size: None,
            caching_max_items: None,
            refresh_cron: None,
            refresh_sql: None,
            refresh_data_window: None,
            refresh_append_overlap: None,
            refresh_retry_enabled: true,
            refresh_retry_max_attempts: None,
            refresh_jitter_enabled: false,
            refresh_jitter_max: None,
            params: HashMap::default(),
            retention_period: None,
            retention_sql: None,
            retention_check_interval: None,
            retention_check_enabled: false,
            on_zero_results: ZeroResultsAction::ReturnEmpty,
            indexes: HashMap::default(),
            primary_key: None,
            on_conflict: HashMap::default(),
            maintained_aggregates: spicepod_acceleration::MaintainedAggregates::default(),
            write_mode: spicepod_acceleration::WriteMode::default(),
            storage_profile: StorageProfile::default(),
            disable_federation: false,
            refresh_on_startup: RefreshOnStartup::default(),
            partition_by: vec![],
            snapshot_behavior: SnapshotBehavior::Disabled,
            snapshots_trigger: None,
            snapshots_trigger_threshold: None,
            snapshots_compaction: SnapshotsCompaction::Disabled,
            snapshots_reset_expiry_on_load_enabled: false,
            snapshots_creation_policy: SnapshotsCreationPolicy::default(),
        }
    }
}

// ── Private parse helpers ─────────────────────────────────────────────────────

/// Returns true if the `query_federation` parameter is set to "disabled".
fn parse_is_query_federation_disabled(params: &mut Option<Params>) -> Result<bool, ParseError> {
    if let Some(params) = params
        && let Some(value) = params.data.remove("query_federation")
    {
        match value {
            spicepod::param::ParamValue::String(s) if s == "enabled" => return Ok(false),
            spicepod::param::ParamValue::String(s) if s == "disabled" => return Ok(true),
            _ => {
                return Err(ParseError::InvalidAccelerationConfiguration {
                    detail: format!(
                        "Invalid 'query_federation' param value: {value:?}. Expected 'enabled' or 'disabled'."
                    ),
                });
            }
        }
    }
    Ok(false)
}

/// Parse the caching-mode entry TTL, accepted under either of its two names.
///
/// `caching_item_ttl` spells the suffix the runtime's other caches use
/// (`item_ttl` on the SQL-results, search-results and embeddings caches);
/// `caching_ttl` is the name this parameter shipped under. Both are read so
/// neither spelling is silently ignored, and setting both to different values
/// is an error rather than a coin flip.
fn parse_caching_ttl(params: &mut Option<Params>) -> Result<Option<Duration>, ParseError> {
    // Both are removed unconditionally: leaving one behind would let it read
    // as an unrecognised parameter later.
    let ttl = parse_duration_param(params, "caching_ttl")?;
    let item_ttl = parse_duration_param(params, "caching_item_ttl")?;

    match (ttl, item_ttl) {
        (Some(ttl), Some(item_ttl)) if ttl != item_ttl => {
            Err(ParseError::InvalidAccelerationConfiguration {
                detail: format!(
                    "Conflicting cache TTLs: 'caching_ttl' is {ttl:?} but 'caching_item_ttl' is {item_ttl:?}. They name the same setting — set only one."
                ),
            })
        }
        (Some(ttl), _) => Ok(Some(ttl)),
        (None, item_ttl) => Ok(item_ttl),
    }
}

/// Parse `caching_max_size`, the byte budget a caching accelerator's stored
/// rows may occupy, e.g. `512MiB`.
fn parse_caching_max_size(params: &mut Option<Params>) -> Result<Option<u64>, ParseError> {
    let Some(params) = params else {
        return Ok(None);
    };
    let Some(value) = params.data.remove("caching_max_size") else {
        return Ok(None);
    };
    match &value {
        // A plain YAML integer is a byte count, so `caching_max_size: 1048576`
        // means the same as `'1048576'` rather than being rejected.
        spicepod::param::ParamValue::Int(n) => {
            u64::try_from(*n)
                .map(Some)
                .map_err(|_| ParseError::InvalidAccelerationConfiguration {
                    detail: format!(
                        "Invalid 'caching_max_size' value: {n}. Expected a byte size such as '256MiB' or '1GB'."
                    ),
                })
        }
        spicepod::param::ParamValue::String(s) => {
            // Refuse an unparseable budget rather than falling back to
            // unbounded: this parameter is the bound, and a silent default
            // would leave an operator believing a limit they set is in force.
            let bytes = byte_unit::Byte::parse_str(s, true).map_err(|e| {
                ParseError::InvalidAccelerationConfiguration {
                    detail: format!(
                        "Invalid 'caching_max_size' value: '{s}' ({e}). Expected a byte size such as '256MiB' or '1GB'."
                    ),
                }
            })?;
            Ok(Some(bytes.as_u64()))
        }
        _ => Err(ParseError::InvalidAccelerationConfiguration {
            detail: format!(
                "Invalid 'caching_max_size' param value: {value:?}. Expected a byte size such as '256MiB' or '1GB'."
            ),
        }),
    }
}

/// Parse `caching_max_items`, the number of rows a caching accelerator may
/// keep.
fn parse_caching_max_items(params: &mut Option<Params>) -> Result<Option<u64>, ParseError> {
    let Some(params) = params else {
        return Ok(None);
    };
    let Some(value) = params.data.remove("caching_max_items") else {
        return Ok(None);
    };
    let detail = || {
        format!(
            "Invalid 'caching_max_items' param value: {value:?}. Expected a whole number of rows, such as '10000'."
        )
    };
    match &value {
        spicepod::param::ParamValue::String(s) => s
            .trim()
            .parse::<u64>()
            .map(Some)
            .map_err(|_| ParseError::InvalidAccelerationConfiguration { detail: detail() }),
        spicepod::param::ParamValue::Int(n) => u64::try_from(*n)
            .map(Some)
            .map_err(|_| ParseError::InvalidAccelerationConfiguration { detail: detail() }),
        _ => Err(ParseError::InvalidAccelerationConfiguration { detail: detail() }),
    }
}

/// Parse `caching_stale_while_revalidate_ttl` duration from params for caching mode.
fn parse_caching_stale_while_revalidate_ttl(
    params: &mut Option<Params>,
) -> Result<Option<Duration>, ParseError> {
    parse_duration_param(params, "caching_stale_while_revalidate_ttl")
}

/// Parse `caching_stale_if_error` from params for caching mode.
/// Valid values: "enabled", "disabled" (default)
fn parse_caching_stale_if_error(params: &mut Option<Params>) -> Result<StaleIfError, ParseError> {
    let Some(params) = params else {
        return Ok(StaleIfError::default());
    };
    let Some(value) = params.data.remove("caching_stale_if_error") else {
        return Ok(StaleIfError::default());
    };
    match value {
        spicepod::param::ParamValue::String(s) => match s.to_lowercase().as_str() {
            "enabled" => Ok(StaleIfError::Enabled),
            "disabled" => Ok(StaleIfError::Disabled),
            _ => Err(ParseError::InvalidAccelerationConfiguration {
                detail: format!(
                    "Invalid 'caching_stale_if_error' value: '{s}'. Expected 'enabled' or 'disabled'."
                ),
            }),
        },
        _ => Err(ParseError::InvalidAccelerationConfiguration {
            detail: format!(
                "Invalid 'caching_stale_if_error' param value: {value:?}. Expected 'enabled' or 'disabled'."
            ),
        }),
    }
}

/// Helper to parse a duration parameter from params.
fn parse_duration_param(
    params: &mut Option<Params>,
    param_name: &str,
) -> Result<Option<Duration>, ParseError> {
    let Some(params) = params else {
        return Ok(None);
    };
    let Some(value) = params.data.remove(param_name) else {
        return Ok(None);
    };
    match value {
        spicepod::param::ParamValue::String(s) => {
            fundu::parse_duration(&s).map(Some).map_err(|e| {
                ParseError::UnableToParseFieldAsDuration {
                    source: e,
                    field: param_name.into(),
                }
            })
        }
        _ => Err(ParseError::InvalidAccelerationConfiguration {
            detail: format!(
                "Invalid '{param_name}' param value: {value:?}. Expected a duration string."
            ),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use spicepod::param::ParamValue;
    use std::sync::Arc;

    /// The three connectors that override `DataConnector::resolve_refresh_mode`, plus
    /// the default. Asserted directly rather than only through a caller, because a
    /// caller that classifies a pod *and* checks itself against this table would agree
    /// with a wrong table.
    #[test]
    fn unset_refresh_mode_matches_the_connector_overrides() {
        assert_eq!(
            unset_refresh_mode_for_connector("debezium"),
            RefreshMode::Changes
        );
        assert_eq!(
            unset_refresh_mode_for_connector("cdc"),
            RefreshMode::Changes
        );
        assert_eq!(
            unset_refresh_mode_for_connector("sink"),
            RefreshMode::Disabled
        );
        // Every other connector keeps the trait default.
        for connector in ["postgres", "s3", "spice.ai", "mysql", ""] {
            assert_eq!(
                unset_refresh_mode_for_connector(connector),
                RefreshMode::Full,
                "connector '{connector}' should keep the `resolve_refresh_mode` default"
            );
        }
    }

    #[test]
    fn test_parse_federation_disabled_param() {
        let params_enabled = Params::from_string_map(HashMap::from([(
            "query_federation".to_string(),
            "enabled".to_string(),
        )]));
        let is_disabled =
            parse_is_query_federation_disabled(&mut Some(params_enabled)).expect("to parse");
        assert!(!is_disabled);

        let params_disabled = Params::from_string_map(HashMap::from([(
            "query_federation".to_string(),
            "disabled".to_string(),
        )]));
        let is_disabled =
            parse_is_query_federation_disabled(&mut Some(params_disabled)).expect("to parse");
        assert!(is_disabled);

        let params_invalid = Params::from_string_map(HashMap::from([(
            "query_federation".to_string(),
            "invalid".to_string(),
        )]));
        let result_invalid = parse_is_query_federation_disabled(&mut Some(params_invalid));
        result_invalid.expect_err("should error parsing query_federation param");

        let params_missing = Params::from_string_map(HashMap::new());
        let is_disabled =
            parse_is_query_federation_disabled(&mut Some(params_missing)).expect("to parse");
        assert!(!is_disabled);
    }

    #[test]
    fn test_parse_caching_stale_if_error() {
        // Test "enabled"
        let params_enabled = Params::from_string_map(HashMap::from([(
            "caching_stale_if_error".to_string(),
            "enabled".to_string(),
        )]));
        let result = parse_caching_stale_if_error(&mut Some(params_enabled)).expect("to parse");
        assert_eq!(result, StaleIfError::Enabled);

        // Test "disabled"
        let params_disabled = Params::from_string_map(HashMap::from([(
            "caching_stale_if_error".to_string(),
            "disabled".to_string(),
        )]));
        let result = parse_caching_stale_if_error(&mut Some(params_disabled)).expect("to parse");
        assert_eq!(result, StaleIfError::Disabled);

        // Test invalid value
        let params_invalid = Params::from_string_map(HashMap::from([(
            "caching_stale_if_error".to_string(),
            "invalid".to_string(),
        )]));
        parse_caching_stale_if_error(&mut Some(params_invalid)).expect_err("should error");

        // Test missing parameter (default)
        let result = parse_caching_stale_if_error(&mut None).expect("to parse");
        assert_eq!(result, StaleIfError::Disabled);
    }

    #[test]
    fn caching_max_size_accepts_byte_sizes_and_refuses_anything_else() {
        for (input, expected) in [
            ("1024", 1024_u64),
            ("256MiB", 256 * 1024 * 1024),
            ("1GB", 1_000_000_000),
        ] {
            let params = Params::from_string_map(HashMap::from([(
                "caching_max_size".to_string(),
                input.to_string(),
            )]));
            let parsed = parse_caching_max_size(&mut Some(params))
                .unwrap_or_else(|e| panic!("'{input}' should parse: {e}"));
            assert_eq!(parsed, Some(expected), "for '{input}'");
        }

        // An unparseable budget must fail the dataset rather than silently
        // leaving the cache unbounded — the parameter *is* the bound.
        let params = Params::from_string_map(HashMap::from([(
            "caching_max_size".to_string(),
            "lots".to_string(),
        )]));
        let err = parse_caching_max_size(&mut Some(params)).expect_err("should error");
        assert!(
            err.to_string().contains("caching_max_size"),
            "error must name the parameter: {err}"
        );

        // A YAML integer is a byte count, not a type error.
        let mut params = Params::from_string_map(HashMap::new());
        params
            .data
            .insert("caching_max_size".to_string(), ParamValue::Int(1_048_576));
        assert_eq!(
            parse_caching_max_size(&mut Some(params)).expect("to parse"),
            Some(1_048_576)
        );

        assert_eq!(parse_caching_max_size(&mut None).expect("to parse"), None);
    }

    #[test]
    fn caching_max_items_accepts_a_row_count_and_refuses_anything_else() {
        let params = Params::from_string_map(HashMap::from([(
            "caching_max_items".to_string(),
            "10000".to_string(),
        )]));
        assert_eq!(
            parse_caching_max_items(&mut Some(params)).expect("to parse"),
            Some(10_000)
        );

        for invalid in ["-1", "1.5", "many", ""] {
            let params = Params::from_string_map(HashMap::from([(
                "caching_max_items".to_string(),
                invalid.to_string(),
            )]));
            let err = parse_caching_max_items(&mut Some(params))
                .err()
                .unwrap_or_else(|| panic!("'{invalid}' must be refused, not silently ignored"));
            assert!(
                err.to_string().contains("caching_max_items"),
                "error must name the parameter: {err}"
            );
        }

        assert_eq!(parse_caching_max_items(&mut None).expect("to parse"), None);
    }

    #[test]
    fn caching_item_ttl_is_accepted_as_the_entry_ttl() {
        // `item_ttl` is the suffix the runtime's other caches use, so both
        // spellings must reach the same setting rather than one being ignored.
        let params = Params::from_string_map(HashMap::from([(
            "caching_item_ttl".to_string(),
            "45s".to_string(),
        )]));
        assert_eq!(
            parse_caching_ttl(&mut Some(params)).expect("to parse"),
            Some(Duration::from_secs(45))
        );

        let params = Params::from_string_map(HashMap::from([(
            "caching_ttl".to_string(),
            "45s".to_string(),
        )]));
        assert_eq!(
            parse_caching_ttl(&mut Some(params)).expect("to parse"),
            Some(Duration::from_secs(45))
        );
    }

    #[test]
    fn conflicting_ttl_spellings_are_an_error_not_a_coin_flip() {
        let params = Params::from_string_map(HashMap::from([
            ("caching_ttl".to_string(), "30s".to_string()),
            ("caching_item_ttl".to_string(), "10m".to_string()),
        ]));
        let err = parse_caching_ttl(&mut Some(params)).expect_err("should error");
        let msg = err.to_string();
        assert!(
            msg.contains("caching_ttl") && msg.contains("caching_item_ttl"),
            "error must name both spellings: {msg}"
        );

        // Agreeing values are fine.
        let params = Params::from_string_map(HashMap::from([
            ("caching_ttl".to_string(), "30s".to_string()),
            ("caching_item_ttl".to_string(), "30s".to_string()),
        ]));
        assert_eq!(
            parse_caching_ttl(&mut Some(params)).expect("to parse"),
            Some(Duration::from_secs(30))
        );
    }

    #[test]
    fn test_hash_index_param_is_ignored() {
        let acceleration = spicepod_acceleration::Acceleration {
            params: Some(Params::from_string_map(HashMap::from([(
                "hash_index".to_string(),
                "enabled".to_string(),
            )]))),
            ..Default::default()
        };

        let parsed = Acceleration::try_from(acceleration).expect("acceleration should parse");
        assert!(!parsed.params.contains_key("hash_index"));
        assert!(!parsed.is_hash_index_enabled());
    }

    #[test]
    fn test_storage_profile_is_parsed_from_spicepod_acceleration() {
        let acceleration = spicepod_acceleration::Acceleration {
            storage_profile: spicepod_acceleration::StorageProfile::Ebs,
            ..Default::default()
        };

        let parsed = Acceleration::try_from(acceleration).expect("acceleration should parse");
        assert_eq!(parsed.storage_profile, StorageProfile::Ebs);
    }

    #[test]
    fn test_maintained_aggregates_are_preserved_from_spicepod_acceleration() {
        let maintained = spicepod_acceleration::MaintainedAggregate {
            group_by: vec!["customer_id".to_string()],
            aggregates: vec![spicepod_acceleration::MaintainedAggregateExpr {
                function: spicepod_acceleration::MaintainedAggregateFunction::Count,
                column: None,
            }],
            filter_sql: None,
        };
        let acceleration = spicepod_acceleration::Acceleration {
            maintained_aggregates: spicepod_acceleration::MaintainedAggregates::new(
                spicepod_acceleration::MaintainAggregates::Disabled,
                vec![maintained.clone()],
            ),
            ..Default::default()
        };

        let parsed = Acceleration::try_from(acceleration).expect("acceleration should parse");
        assert_eq!(parsed.maintained_aggregates.as_slice(), &[maintained]);
        assert!(!parsed.maintained_aggregates.is_enabled());
    }

    fn empty_schema() -> SchemaRef {
        Arc::new(Schema::empty())
    }

    fn schema_with(columns: &[&str]) -> SchemaRef {
        Arc::new(Schema::new(
            columns
                .iter()
                .map(|c| Field::new(*c, DataType::Int32, false))
                .collect::<Vec<_>>(),
        ))
    }

    fn acceleration_with_primary_key(pk: &str) -> Acceleration {
        Acceleration {
            primary_key: Some(ColumnReference::try_from(pk).expect("valid column reference")),
            ..Acceleration::default()
        }
    }

    fn acceleration_with_index(index: &str) -> Acceleration {
        let mut indexes = HashMap::new();
        indexes.insert(
            ColumnReference::try_from(index).expect("valid column reference"),
            IndexType::Enabled,
        );
        Acceleration {
            indexes,
            ..Acceleration::default()
        }
    }

    #[test]
    fn empty_schema_with_primary_key_reports_empty_schema_not_missing_column() {
        let acceleration = acceleration_with_primary_key("marker_id");
        let err = acceleration
            .validate_primary_key(&empty_schema())
            .expect_err("empty schema with a primary key must fail");

        assert!(
            matches!(err, ParseError::AcceleratedSchemaEmpty { .. }),
            "expected AcceleratedSchemaEmpty, got: {err}"
        );
        let msg = err.to_string();
        assert!(
            msg.contains("no columns") && msg.contains("does not exist"),
            "message should point at the empty schema / missing table: {msg}"
        );
        assert!(
            !msg.contains("was not found in the schema"),
            "message should not be the misleading column-not-found error: {msg}"
        );
    }

    #[test]
    fn empty_schema_with_index_reports_empty_schema_not_missing_column() {
        let acceleration = acceleration_with_index("marker_id");
        let err = acceleration
            .validate_indexes(&empty_schema())
            .expect_err("empty schema with an index must fail");

        assert!(
            matches!(err, ParseError::AcceleratedSchemaEmpty { .. }),
            "expected AcceleratedSchemaEmpty, got: {err}"
        );
    }

    #[test]
    fn empty_schema_without_constraints_is_ok() {
        let acceleration = Acceleration::default();
        acceleration
            .validate_primary_key(&empty_schema())
            .expect("no primary key configured, so an empty schema is fine");
        acceleration
            .validate_indexes(&empty_schema())
            .expect("no indexes configured, so an empty schema is fine");
    }

    #[test]
    fn populated_schema_missing_primary_key_column_keeps_column_not_found_error() {
        let acceleration = acceleration_with_primary_key("marker_id");
        let err = acceleration
            .validate_primary_key(&schema_with(&["id", "value"]))
            .expect_err("missing primary key column must fail");

        assert!(
            matches!(err, ParseError::PrimaryKeyColumnNotFound { .. }),
            "expected PrimaryKeyColumnNotFound, got: {err}"
        );
        assert!(err.to_string().contains("was not found in the schema"));
    }

    #[test]
    fn populated_schema_with_primary_key_column_is_ok() {
        let acceleration = acceleration_with_primary_key("marker_id");
        acceleration
            .validate_primary_key(&schema_with(&["marker_id", "value"]))
            .expect("primary key column present, so validation passes");
    }

    /// The exact configuration that turns durable federated write-back on.
    fn durable_write_back_acceleration() -> Acceleration {
        let mut on_conflict = HashMap::new();
        on_conflict.insert(
            ColumnReference::try_from("id").expect("valid column reference"),
            OnConflictBehavior::Upsert(UpsertOptions::default()),
        );
        Acceleration {
            engine: Engine::Cayenne,
            write_mode: spicepod_acceleration::WriteMode::WriteBack,
            on_conflict,
            refresh_mode: Some(RefreshMode::Changes),
            ..Acceleration::default()
        }
    }

    #[test]
    fn durable_write_back_is_recognized() {
        assert!(durable_write_back_acceleration().resolves_to_durable_write_back());
    }

    /// Every conjunct must be load-bearing: if dropping one still reports
    /// durable write-back, the registration gate would fire on datasets that
    /// never mark keys (or, worse, stay silent on ones that do).
    #[test]
    fn every_condition_is_required_for_durable_write_back() {
        let not_cayenne = Acceleration {
            engine: Engine::DuckDB,
            ..durable_write_back_acceleration()
        };
        assert!(
            !not_cayenne.resolves_to_durable_write_back(),
            "only the Cayenne provider marks dirty keys for delivery"
        );

        let not_write_back = Acceleration {
            write_mode: spicepod_acceleration::WriteMode::WriteThrough,
            ..durable_write_back_acceleration()
        };
        assert!(
            !not_write_back.resolves_to_durable_write_back(),
            "write-through commits to the source directly, with no delivery worker"
        );

        let no_on_conflict = Acceleration {
            on_conflict: HashMap::new(),
            ..durable_write_back_acceleration()
        };
        assert!(
            !no_on_conflict.resolves_to_durable_write_back(),
            "without on_conflict there is no key to reconcile"
        );

        for refresh_mode in [
            RefreshMode::Disabled,
            RefreshMode::Full,
            RefreshMode::Append,
            RefreshMode::Caching,
            RefreshMode::Snapshot,
        ] {
            let not_changes = Acceleration {
                refresh_mode: Some(refresh_mode),
                ..durable_write_back_acceleration()
            };
            assert!(
                !not_changes.resolves_to_durable_write_back(),
                "{refresh_mode:?} is not CDC-fed, so no delivery worker runs"
            );
        }

        let unset_refresh_mode = Acceleration {
            refresh_mode: None,
            ..durable_write_back_acceleration()
        };
        assert!(
            !unset_refresh_mode.resolves_to_durable_write_back(),
            "an unset refresh_mode is not 'changes'"
        );
    }

    #[test]
    fn a_default_acceleration_is_not_durable_write_back() {
        assert!(
            !Acceleration::default().resolves_to_durable_write_back(),
            "the gate must not fire on ordinary datasets"
        );
    }
}
