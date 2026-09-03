/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use crate::{
    component::dataset::ReadyState,
    metric::Metrics,
    param::Params,
    partitioning::{PartitionedBy, deserialize_partition_by, serialize_partition_by},
};
#[cfg(feature = "schemars")]
use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize};
use std::{collections::HashMap, fmt::Display};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(rename_all = "lowercase")]
pub enum RefreshMode {
    Full,
    Append,
    Changes,
    Caching,
    /// Refresh exclusively by reloading newer snapshots from the configured
    /// snapshot location. The federated source is never queried for refreshes.
    /// Requires `snapshots` to be enabled and a snapshot-supporting engine.
    Snapshot,
}

/// Controls the write behavior for accelerated read-write datasets.
///
/// - `write_through` (default): Writes go to the federated source (e.g. Postgres)
///   synchronously. The user receives confirmation after a full ACID commit to the
///   source. The local accelerator is updated via the normal refresh mechanism
///   (e.g. WAL replication with `refresh_mode: changes`).
///
/// - `write_back`: Writes commit to the local accelerator and are carried to the
///   federated source afterwards by a delivery worker, so the source may lag.
///   Every write must be sent as a single `BEGIN; ...; COMMIT;` body: only the
///   transactional commit records the write for delivery, so a statement outside
///   a transaction is refused rather than accepted with weaker durability than a
///   caller would assume. `INSERT` and `UPDATE` only — `DELETE` is not supported.
///   To delete, first stop writing and wait for
///   `dataset_acceleration_write_back_pending_keys` to reach zero while write-back
///   is still enabled (the delivery worker is what drains it), then disable
///   write-back, delete at the source, and let the change stream carry it back.
///   Requires a
///   single-column `primary_key` to key each delivery on, and
///   `replication.enabled: true` as an explicit opt-in to the source lagging the
///   accelerator.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum WriteMode {
    #[default]
    WriteThrough,
    WriteBack,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(rename_all = "snake_case")]
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

/// Storage profile for file-backed accelerations.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum StorageProfile {
    /// Detect the storage profile from the acceleration path.
    #[default]
    Auto,
    /// Local SSD/NVMe-backed storage, such as EC2 instance store or Azure
    /// temporary/NVMe local storage.
    #[serde(alias = "ssd", alias = "nvme")]
    LocalSsd,
    /// Network-attached block storage, such as Amazon EBS or Azure Managed
    /// Disks.
    #[serde(alias = "azure_disk", alias = "managed_disk", alias = "network_disk")]
    Ebs,
    /// In-memory storage, such as a tmpfs or ramfs mount.
    #[serde(alias = "ram", alias = "ramdisk", alias = "ramfs", alias = "memory")]
    Tmpfs,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(rename_all = "lowercase")]
pub enum RefreshOnStartup {
    /// Always start a new refresh when Spice starts.
    Always,
    /// Only start a refresh if an existing acceleration is not available.
    #[default]
    Auto,
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
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum ZeroResultsAction {
    /// Return an empty result set. This is the default.
    #[default]
    ReturnEmpty,
    /// Fallback to querying the source table.
    UseSource,
}

impl Display for ZeroResultsAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ZeroResultsAction::ReturnEmpty => write!(f, "return_empty"),
            ZeroResultsAction::UseSource => write!(f, "use_source"),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Default)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(rename_all = "lowercase")]
pub enum IndexType {
    #[default]
    Enabled,
    Unique,
}

impl Display for IndexType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IndexType::Enabled => write!(f, "enabled"),
            IndexType::Unique => write!(f, "unique"),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Default)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum OnConflictBehavior {
    #[default]
    Drop,
    Upsert,
    UpsertDedup,
    UpsertDedupByRowId,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Default)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum SnapshotBehavior {
    /// Snapshots are disabled (default).
    #[default]
    Disabled,
    /// Enable both creating and bootstrapping from snapshots.
    Enabled,
    /// Only bootstrap from existing snapshots, don't attempt to create new ones.
    BootstrapOnly,
    /// Only create new snapshots.
    CreateOnly,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Default)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum SnapshotsResetExpiryOnLoad {
    #[default]
    Disabled,
    Enabled,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Default)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum SnapshotsCreationPolicy {
    Always,
    #[default]
    OnChange,
}

#[expect(clippy::trivially_copy_pass_by_ref)]
fn is_default_snapshot_behavior(b: &SnapshotBehavior) -> bool {
    *b == SnapshotBehavior::default()
}

#[expect(clippy::trivially_copy_pass_by_ref)]
fn is_default_snapshot_compaction(c: &SnapshotsCompaction) -> bool {
    *c == SnapshotsCompaction::default()
}

#[expect(clippy::trivially_copy_pass_by_ref)]
fn is_default_snapshots_reset_expiry_on_load(c: &SnapshotsResetExpiryOnLoad) -> bool {
    *c == SnapshotsResetExpiryOnLoad::default()
}

#[expect(clippy::trivially_copy_pass_by_ref)]
fn is_default_snapshots_creation_policy(c: &SnapshotsCreationPolicy) -> bool {
    *c == SnapshotsCreationPolicy::default()
}

#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotsTrigger {
    /// After each refresh is complete (default).
    RefreshComplete,
    // Periodically based on time interval
    TimeInterval,
    // Periodically based on stream batch processing
    StreamBatches,
}

fn deserialize_string_or_number<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum StringOrNumber {
        String(String),
        Number(serde_json::Number),
    }

    match Option::<StringOrNumber>::deserialize(deserializer)? {
        Some(StringOrNumber::String(s)) => Ok(Some(s)),
        Some(StringOrNumber::Number(n)) => Ok(Some(n.to_string())),
        None => Ok(None),
    }
}

#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default, Copy)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotsCompaction {
    #[default]
    Disabled,
    Enabled,
}

/// A Cayenne-maintained aggregate view declaration.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct MaintainedAggregate {
    /// Columns used as the `GROUP BY` key, in query output order.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub group_by: Vec<String>,

    /// Aggregate expressions maintained for each group, in query output order.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub aggregates: Vec<MaintainedAggregateExpr>,

    /// Optional SQL row predicate (a `WHERE` expression over the dataset's
    /// columns, e.g. `ol_delivery_d > '2007-01-02'`) selecting which rows
    /// contribute to the view. When set, an accelerator that supports it
    /// maintains the aggregate over only the matching rows and serves a query
    /// carrying the identical predicate from that maintained state — letting a
    /// filtered analytical query (the common dashboard shape) be answered from
    /// the incrementally-maintained view instead of a full re-scan.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter_sql: Option<String>,
}

/// One aggregate expression inside a maintained aggregate view.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct MaintainedAggregateExpr {
    pub function: MaintainedAggregateFunction,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub column: Option<String>,
}

/// Aggregate functions supported by Cayenne maintained aggregate views.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum MaintainedAggregateFunction {
    Count,
    Sum,
    Avg,
    /// `MIN(column)` over integer / temporal / decimal families (engine-side).
    /// Retraction-hard: requires a primary key so deletes can drop the extremum.
    Min,
    /// `MAX(column)` — mirror of [`Self::Min`].
    Max,
}

/// Controls whether configured maintained aggregates are materialized and
/// updated incrementally by accelerators that support them.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum MaintainAggregates {
    #[default]
    Disabled,
    Enabled,
}

impl MaintainAggregates {
    #[must_use]
    pub const fn is_enabled(self) -> bool {
        matches!(self, Self::Enabled)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct MaintainedAggregatesConfig {
    #[serde(default, alias = "enabled")]
    pub mode: MaintainAggregates,

    #[serde(
        default,
        alias = "aggregates",
        alias = "specs",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub views: Vec<MaintainedAggregate>,
}

impl Default for MaintainedAggregatesConfig {
    fn default() -> Self {
        Self {
            mode: MaintainAggregates::Disabled,
            views: Vec::new(),
        }
    }
}

/// Maintained aggregate configuration.
///
/// Accepts the original list form:
/// `maintained_aggregates: [{ group_by: ..., aggregates: ... }]`, or a policy
/// form with `mode` and `views` when the configured specs should be kept but
/// not maintained.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(untagged)]
pub enum MaintainedAggregates {
    List(Vec<MaintainedAggregate>),
    Config(MaintainedAggregatesConfig),
    Mode(MaintainAggregates),
}

impl Default for MaintainedAggregates {
    fn default() -> Self {
        Self::Mode(MaintainAggregates::Disabled)
    }
}

impl From<Vec<MaintainedAggregate>> for MaintainedAggregates {
    fn from(aggregates: Vec<MaintainedAggregate>) -> Self {
        Self::List(aggregates)
    }
}

impl<'a> IntoIterator for &'a MaintainedAggregates {
    type Item = &'a MaintainedAggregate;
    type IntoIter = std::slice::Iter<'a, MaintainedAggregate>;

    fn into_iter(self) -> Self::IntoIter {
        self.as_slice().iter()
    }
}

impl MaintainedAggregates {
    #[must_use]
    pub fn new(mode: MaintainAggregates, views: Vec<MaintainedAggregate>) -> Self {
        Self::Config(MaintainedAggregatesConfig { mode, views })
    }

    #[must_use]
    pub fn is_enabled(&self) -> bool {
        match self {
            Self::List(aggregates) => !aggregates.is_empty(),
            Self::Config(config) => config.mode.is_enabled(),
            Self::Mode(mode) => mode.is_enabled(),
        }
    }

    #[must_use]
    pub fn enabled_aggregates(&self) -> &[MaintainedAggregate] {
        if self.is_enabled() {
            self.as_slice()
        } else {
            &[]
        }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.as_slice().is_empty()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.as_slice().len()
    }

    pub fn iter(&self) -> std::slice::Iter<'_, MaintainedAggregate> {
        self.as_slice().iter()
    }

    #[must_use]
    pub fn is_default(&self) -> bool {
        match self {
            Self::List(aggregates) => aggregates.is_empty(),
            Self::Config(config) => !config.mode.is_enabled() && config.views.is_empty(),
            Self::Mode(mode) => !mode.is_enabled(),
        }
    }

    #[must_use]
    pub fn as_slice(&self) -> &[MaintainedAggregate] {
        match self {
            Self::List(aggregates) => aggregates,
            Self::Config(config) => &config.views,
            Self::Mode(_) => &[],
        }
    }
}

#[expect(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct Acceleration {
    #[serde(default = "default_true")]
    pub enabled: bool,

    #[serde(default)]
    pub mode: Mode,

    #[serde(default)]
    pub refresh_on_startup: RefreshOnStartup,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub engine: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refresh_mode: Option<RefreshMode>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refresh_check_interval: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refresh_cron: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refresh_sql: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refresh_data_window: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refresh_append_overlap: Option<String>,

    #[serde(default = "default_true")]
    pub refresh_retry_enabled: bool,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refresh_retry_max_attempts: Option<usize>,

    #[serde(default)]
    pub refresh_jitter_enabled: bool,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refresh_jitter_max: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<Params>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retention_period: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retention_sql: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retention_check_interval: Option<String>,

    #[serde(default, skip_serializing_if = "is_false")]
    pub retention_check_enabled: bool,

    #[serde(default)]
    pub on_zero_results: ZeroResultsAction,

    #[serde(default)]
    #[deprecated(
        since = "1.0.0-rc.1",
        note = "Use the dataset's or view's own `ready_state` instead."
    )]
    pub ready_state: Option<ReadyState>,

    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub indexes: HashMap<String, IndexType>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub primary_key: Option<String>,

    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub on_conflict: HashMap<String, OnConflictBehavior>,

    #[serde(default, skip_serializing_if = "is_default_maintained_aggregates")]
    pub maintained_aggregates: MaintainedAggregates,

    /// Controls write behavior for read-write accelerated datasets.
    /// Only applies when `access: read_write` and the dataset is accelerated.
    #[serde(default, skip_serializing_if = "is_default_write_mode")]
    pub write_mode: WriteMode,

    /// Storage profile for file-backed acceleration. `auto` detects the
    /// profile from the resolved acceleration path; use `local_ssd`/`ssd`/`nvme`
    /// `ebs`, or `tmpfs`/`ram`/`ramdisk`/`ramfs`/`memory` to override detection.
    #[serde(default, skip_serializing_if = "is_default_storage_profile")]
    pub storage_profile: StorageProfile,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metrics: Option<Metrics>,

    /// Partition expressions used to physically partition accelerated data.
    ///
    /// Each item accepts either:
    /// - a plain expression string, for example `"YEAR(created_at)"` or
    ///   `"bucket(100, user_id)"`; or
    /// - a single-entry mapping of a partition name to an expression, for
    ///   example `{ year: "YEAR(created_at)" }`.
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "serialize_partition_by",
        deserialize_with = "deserialize_partition_by"
    )]
    #[cfg_attr(
        feature = "schemars",
        schemars(with = "Vec<crate::partitioning::PartitionedBySchema>")
    )]
    pub partition_by: Vec<PartitionedBy>,

    /// Enables snapshots for this dataset, requires the top-level config `snapshots` to be defined.
    ///
    /// Options: `enabled` / `disabled` / `bootstrap_only` / `create_only`.
    ///
    /// `disabled` (default) will turn off snapshots for this dataset.
    /// `enabled` will enable both creating and bootstrapping from snapshots.
    /// `bootstrap_only` will only bootstrap on startup, it won't attempt to write new snapshots.
    /// `create_only` will only create snapshots, it won't attempt to bootstrap from one.
    #[serde(default, skip_serializing_if = "is_default_snapshot_behavior")]
    pub snapshots: SnapshotBehavior,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshots_trigger: Option<SnapshotsTrigger>,

    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_string_or_number"
    )]
    pub snapshots_trigger_threshold: Option<String>,

    #[serde(default, skip_serializing_if = "is_default_snapshot_compaction")]
    pub snapshots_compaction: SnapshotsCompaction,

    #[serde(
        default,
        skip_serializing_if = "is_default_snapshots_reset_expiry_on_load"
    )]
    pub snapshots_reset_expiry_on_load: SnapshotsResetExpiryOnLoad,

    #[serde(default, skip_serializing_if = "is_default_snapshots_creation_policy")]
    pub snapshots_creation_policy: SnapshotsCreationPolicy,
}

#[expect(clippy::trivially_copy_pass_by_ref)]
fn is_false(b: &bool) -> bool {
    !b
}

#[expect(clippy::trivially_copy_pass_by_ref)]
fn is_default_write_mode(mode: &WriteMode) -> bool {
    *mode == WriteMode::WriteThrough
}

#[expect(clippy::trivially_copy_pass_by_ref)]
fn is_default_storage_profile(storage_profile: &StorageProfile) -> bool {
    *storage_profile == StorageProfile::Auto
}

fn is_default_maintained_aggregates(maintained_aggregates: &MaintainedAggregates) -> bool {
    maintained_aggregates.is_default()
}

const fn default_true() -> bool {
    true
}

/// Fields an `enabled: false` block does not discard *because it is disabled*,
/// and so must not be named by a warning whose remedy is "remove
/// `enabled: false`": the switch itself, and `ready_state`.
///
/// `ready_state` is excluded for both components that carry an acceleration
/// block, for the same reason on each: `DatasetBuilder` and `ViewBuilder` both
/// read `acceleration.ready_state` out of the block and apply it whether or not
/// acceleration is enabled (deprecated, but honoured), so it is genuinely not
/// discarded by `enabled: false` on either component.
const CONSUMED_WHEN_DISABLED: [&str; 2] = ["enabled", "ready_state"];

impl Acceleration {
    /// The acceleration fields this block sets that the runtime will ignore
    /// because `enabled: false` turns the whole block off, in the order they
    /// should be reported.
    ///
    /// Empty for an enabled block, and for a disabled block that sets nothing
    /// else — `enabled: false` on its own is a deliberate, complete
    /// configuration.
    ///
    /// Derived from the serialized form rather than a hand-written field list,
    /// so a field added to this struct later is covered without anyone
    /// remembering to add it here. A field counts as set when it serializes to
    /// something other than what the same block carries at its default, which
    /// is what distinguishes `mode: memory` written out by hand — inert either
    /// way — from `mode: file`.
    ///
    /// Two fields are never reported, per [`CONSUMED_WHEN_DISABLED`]: `enabled`,
    /// which is the field doing the discarding, and `ready_state`, which no
    /// component discards *because of* `enabled: false`.
    #[must_use]
    pub fn fields_ignored_when_disabled(&self) -> Vec<String> {
        if self.enabled {
            return Vec::new();
        }

        let disabled_default = Self {
            enabled: false,
            ..Self::default()
        };
        let (Ok(serde_json::Value::Object(configured)), Ok(serde_json::Value::Object(unset))) = (
            serde_json::to_value(self),
            serde_json::to_value(&disabled_default),
        ) else {
            // Nothing here is worth failing a load over: this reports on a
            // configuration, it does not decide one.
            return Vec::new();
        };

        let mut ignored: Vec<String> = configured
            .into_iter()
            .filter(|(field, value)| {
                !CONSUMED_WHEN_DISABLED.contains(&field.as_str()) && unset.get(field) != Some(value)
            })
            .map(|(field, _)| field)
            .collect();
        ignored.sort();
        ignored
    }
}

impl Default for Acceleration {
    #[expect(deprecated)]
    fn default() -> Self {
        Self {
            enabled: true,
            mode: Mode::Memory,
            refresh_on_startup: RefreshOnStartup::default(),
            engine: None,
            refresh_mode: None,
            refresh_check_interval: None,
            refresh_cron: None,
            refresh_sql: None,
            refresh_data_window: None,
            refresh_append_overlap: None,
            refresh_retry_enabled: true,
            refresh_retry_max_attempts: None,
            refresh_jitter_enabled: false,
            refresh_jitter_max: None,
            params: None,
            retention_period: None,
            retention_sql: None,
            retention_check_interval: None,
            retention_check_enabled: false,
            on_zero_results: ZeroResultsAction::ReturnEmpty,
            ready_state: None,
            indexes: HashMap::default(),
            primary_key: None,
            on_conflict: HashMap::default(),
            maintained_aggregates: MaintainedAggregates::default(),
            write_mode: WriteMode::default(),
            storage_profile: StorageProfile::default(),
            metrics: None,
            partition_by: vec![],
            snapshots: SnapshotBehavior::Disabled,
            snapshots_trigger: None,
            snapshots_trigger_threshold: None,
            snapshots_compaction: SnapshotsCompaction::Disabled,
            snapshots_reset_expiry_on_load: SnapshotsResetExpiryOnLoad::Disabled,
            snapshots_creation_policy: SnapshotsCreationPolicy::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use yaml;

    fn acceleration_from_yaml(spec: &str) -> Acceleration {
        yaml::from_str(spec).expect("acceleration should deserialize")
    }

    #[test]
    fn a_disabled_acceleration_reports_the_settings_it_discards() {
        // The reported shape of #13514: everything below `enabled: false` is
        // read, accepted and then ignored, and the dataset serves federated
        // queries that look like a working cache.
        let acceleration = acceleration_from_yaml(
            r"
                enabled: false
                engine: duckdb
                mode: file
                refresh_mode: caching
                primary_key: '(request_query, request_path)'
                params:
                  duckdb_file: api_cache.db
                  caching_ttl: 1s
            ",
        );
        assert_eq!(
            acceleration.fields_ignored_when_disabled(),
            vec![
                "engine".to_string(),
                "mode".to_string(),
                "params".to_string(),
                "primary_key".to_string(),
                "refresh_mode".to_string(),
            ]
        );
    }

    #[test]
    fn a_disabled_acceleration_that_sets_nothing_else_discards_nothing() {
        // `enabled: false` alone is a complete, deliberate configuration and
        // must stay silent, or every dataset that turns acceleration off warns.
        let acceleration = acceleration_from_yaml("enabled: false");
        assert!(acceleration.fields_ignored_when_disabled().is_empty());
    }

    #[test]
    fn a_field_written_out_at_its_default_is_not_reported_as_discarded() {
        // `mode: memory` is what an omitted `mode` already means, so nothing is
        // lost by ignoring it and saying otherwise would be noise.
        let acceleration = acceleration_from_yaml(
            r"
                enabled: false
                mode: memory
            ",
        );
        assert!(acceleration.fields_ignored_when_disabled().is_empty());
    }

    #[test]
    fn an_enabled_acceleration_discards_nothing_however_it_is_configured() {
        // The whole block applies, so there is nothing to report — this is the
        // direction that would otherwise warn on every accelerated dataset.
        let acceleration = acceleration_from_yaml(
            r"
                engine: duckdb
                mode: file
                refresh_mode: full
            ",
        );
        assert!(acceleration.enabled, "enabled defaults to true");
        assert!(acceleration.fields_ignored_when_disabled().is_empty());
    }

    #[test]
    fn a_ready_state_inside_a_disabled_block_is_not_reported_as_discarded() {
        // `acceleration.ready_state` is deprecated, but the dataset still reads
        // it out of this block and applies it whether or not acceleration is
        // enabled — so it is the one setting here that a disabled block does not
        // discard, and saying otherwise would send the reader to fix something
        // that is working.
        let acceleration = acceleration_from_yaml(
            r"
                enabled: false
                ready_state: on_load
            ",
        );
        assert!(acceleration.fields_ignored_when_disabled().is_empty());
    }

    #[test]
    fn enabled_is_never_reported_as_one_of_the_discarded_fields() {
        // It is the field doing the discarding; naming it in its own list would
        // read as though turning acceleration off had itself been ignored.
        let acceleration = acceleration_from_yaml(
            r"
                enabled: false
                engine: duckdb
            ",
        );
        let ignored = acceleration.fields_ignored_when_disabled();
        assert!(
            !ignored.iter().any(|field| field == "enabled"),
            "{ignored:?}"
        );
    }

    #[test]
    fn a_field_added_to_the_block_later_is_covered_without_a_list_to_update() {
        // The guard against this helper going stale: it is derived from the
        // serialized form, so a field this test does not know about still shows
        // up. `retention_period` stands in for "whatever is added next".
        let acceleration = acceleration_from_yaml(
            r"
                enabled: false
                retention_period: 24h
            ",
        );
        assert_eq!(
            acceleration.fields_ignored_when_disabled(),
            vec!["retention_period".to_string()]
        );
    }

    #[test]
    fn test_deserialize_acceleration_on_conflict_string() {
        let yaml = r"
                on_conflict:
                  foo: upsert
            ";
        let acceleration: Acceleration =
            yaml::from_str(yaml).expect("Failed to parse Acceleration");
        assert_eq!(
            acceleration.on_conflict.get("foo"),
            Some(&OnConflictBehavior::Upsert)
        );
    }

    #[test]
    fn test_deserialize_acceleration_on_conflict_upsert_dedup() {
        let yaml = r"
                on_conflict:
                  foo: upsert_dedup
            ";
        let acceleration: Acceleration =
            yaml::from_str(yaml).expect("Failed to parse Acceleration");
        assert_eq!(
            acceleration.on_conflict.get("foo"),
            Some(&OnConflictBehavior::UpsertDedup)
        );
    }

    #[test]
    fn test_deserialize_acceleration_on_conflict_upsert_dedup_by_row_id() {
        let yaml = r"
                on_conflict:
                  foo: upsert_dedup_by_row_id
            ";
        let acceleration: Acceleration =
            yaml::from_str(yaml).expect("Failed to parse Acceleration");
        assert_eq!(
            acceleration.on_conflict.get("foo"),
            Some(&OnConflictBehavior::UpsertDedupByRowId)
        );
    }

    #[test]
    fn test_deserialize_acceleration_on_conflict_drop_string() {
        let yaml = r"
                on_conflict:
                  foo: drop
            ";
        let acceleration: Acceleration =
            yaml::from_str(yaml).expect("Failed to parse Acceleration");
        assert_eq!(
            acceleration.on_conflict.get("foo"),
            Some(&OnConflictBehavior::Drop)
        );
    }

    #[test]
    fn test_deserialize_maintained_aggregates() {
        let yaml = r"
                maintained_aggregates:
                  - group_by: [customer_id]
                    aggregates:
                      - function: count
                      - function: sum
                        column: amount
                      - function: avg
                        column: latency_ms
                      - function: min
                        column: amount
                      - function: max
                        column: amount
            ";
        let acceleration: Acceleration =
            yaml::from_str(yaml).expect("Failed to parse Acceleration");
        assert!(acceleration.maintained_aggregates.is_enabled());
        assert_eq!(acceleration.maintained_aggregates.len(), 1);
        let maintained = &acceleration.maintained_aggregates.as_slice()[0];
        assert_eq!(maintained.group_by, vec!["customer_id"]);
        assert_eq!(
            maintained.aggregates,
            vec![
                MaintainedAggregateExpr {
                    function: MaintainedAggregateFunction::Count,
                    column: None,
                },
                MaintainedAggregateExpr {
                    function: MaintainedAggregateFunction::Sum,
                    column: Some("amount".to_string()),
                },
                MaintainedAggregateExpr {
                    function: MaintainedAggregateFunction::Avg,
                    column: Some("latency_ms".to_string()),
                },
                MaintainedAggregateExpr {
                    function: MaintainedAggregateFunction::Min,
                    column: Some("amount".to_string()),
                },
                MaintainedAggregateExpr {
                    function: MaintainedAggregateFunction::Max,
                    column: Some("amount".to_string()),
                },
            ]
        );
    }

    #[test]
    fn test_deserialize_maintained_aggregate_filter() {
        let yaml = r"
                maintained_aggregates:
                  - group_by: [ol_number]
                    filter_sql: ol_delivery_d > '2007-01-02'
                    aggregates:
                      - function: sum
                        column: ol_amount
            ";
        let acceleration: Acceleration =
            yaml::from_str(yaml).expect("Failed to parse Acceleration");
        let maintained = &acceleration.maintained_aggregates.as_slice()[0];
        assert_eq!(
            maintained.filter_sql.as_deref(),
            Some("ol_delivery_d > '2007-01-02'"),
            "the filter predicate must round-trip from YAML"
        );

        // Absent filter_sql must default to None (backward compatible).
        let no_filter: Acceleration = yaml::from_str(
            "
                maintained_aggregates:
                  - group_by: [customer_id]
                    aggregates:
                      - function: count
            ",
        )
        .expect("Failed to parse Acceleration");
        assert_eq!(
            no_filter.maintained_aggregates.as_slice()[0].filter_sql,
            None
        );
    }

    #[test]
    fn test_maintained_aggregates_disabled_by_default() {
        let acceleration: Acceleration =
            yaml::from_str("{}").expect("Failed to parse Acceleration");

        assert!(!acceleration.maintained_aggregates.is_enabled());
        assert!(
            acceleration
                .maintained_aggregates
                .enabled_aggregates()
                .is_empty()
        );
    }

    #[test]
    fn test_deserialize_maintained_aggregates_config_defaults_disabled() {
        let yaml = concat!(
            "maintained_aggregates:\n",
            "  views:\n",
            "    - group_by: [customer_id]\n",
            "      aggregates:\n",
            "        - function: count\n",
        );
        let acceleration: Acceleration =
            yaml::from_str(yaml).expect("Failed to parse Acceleration");

        assert!(!acceleration.maintained_aggregates.is_enabled());
        assert_eq!(acceleration.maintained_aggregates.len(), 1);
        assert!(
            acceleration
                .maintained_aggregates
                .enabled_aggregates()
                .is_empty()
        );
    }

    #[test]
    fn test_deserialize_maintained_aggregates_enabled_config() {
        let yaml = concat!(
            "maintained_aggregates:\n",
            "  mode: enabled\n",
            "  views:\n",
            "    - group_by: [customer_id]\n",
            "      aggregates:\n",
            "        - function: count\n",
        );
        let acceleration: Acceleration =
            yaml::from_str(yaml).expect("Failed to parse Acceleration");

        assert!(acceleration.maintained_aggregates.is_enabled());
        assert_eq!(
            acceleration
                .maintained_aggregates
                .enabled_aggregates()
                .len(),
            1
        );
    }

    #[test]
    fn test_deserialize_maintained_aggregates_disabled_config() {
        let yaml = concat!(
            "maintained_aggregates:\n",
            "  mode: disabled\n",
            "  views:\n",
            "    - group_by: [customer_id]\n",
            "      aggregates:\n",
            "        - function: count\n",
        );
        let acceleration: Acceleration =
            yaml::from_str(yaml).expect("Failed to parse Acceleration");
        assert!(!acceleration.maintained_aggregates.is_enabled());
        assert_eq!(acceleration.maintained_aggregates.len(), 1);
    }

    #[test]
    fn test_deserialize_maintained_aggregates_disabled_scalar() {
        let acceleration: Acceleration = yaml::from_str("maintained_aggregates: disabled")
            .expect("Failed to parse Acceleration");
        assert!(!acceleration.maintained_aggregates.is_enabled());
        assert!(acceleration.maintained_aggregates.is_empty());
    }

    #[test]
    fn test_deserialize_mode_memory() {
        let yaml = "mode: memory";
        let accel: Acceleration = yaml::from_str(yaml).expect("should parse");
        assert_eq!(accel.mode, Mode::Memory);
    }

    #[test]
    fn test_deserialize_mode_file() {
        let yaml = "mode: file";
        let accel: Acceleration = yaml::from_str(yaml).expect("should parse");
        assert_eq!(accel.mode, Mode::File);
    }

    #[test]
    fn test_deserialize_mode_file_create() {
        let yaml = "mode: file_create";
        let accel: Acceleration = yaml::from_str(yaml).expect("should parse");
        assert_eq!(accel.mode, Mode::FileCreate);
    }

    #[test]
    fn test_deserialize_mode_file_update() {
        let yaml = "mode: file_update";
        let accel: Acceleration = yaml::from_str(yaml).expect("should parse");
        assert_eq!(accel.mode, Mode::FileUpdate);
    }

    #[test]
    fn test_mode_display_round_trip() {
        for mode in [Mode::Memory, Mode::File, Mode::FileCreate, Mode::FileUpdate] {
            let s = mode.to_string();
            let yaml = format!("mode: {s}");
            let accel: Acceleration =
                yaml::from_str(&yaml).unwrap_or_else(|_| panic!("should parse mode '{s}'"));
            assert_eq!(accel.mode, mode, "round-trip failed for mode '{s}'");
        }
    }

    #[test]
    fn test_deserialize_refresh_mode_snapshot() {
        let yaml = "refresh_mode: snapshot";
        let accel: Acceleration = yaml::from_str(yaml).expect("should parse");
        assert_eq!(accel.refresh_mode, Some(RefreshMode::Snapshot));
    }

    #[test]
    fn test_deserialize_all_refresh_modes() {
        for (yaml_value, expected) in [
            ("full", RefreshMode::Full),
            ("append", RefreshMode::Append),
            ("changes", RefreshMode::Changes),
            ("caching", RefreshMode::Caching),
            ("snapshot", RefreshMode::Snapshot),
        ] {
            let yaml = format!("refresh_mode: {yaml_value}");
            let accel: Acceleration = yaml::from_str(&yaml)
                .unwrap_or_else(|_| panic!("should parse refresh_mode '{yaml_value}'"));
            assert_eq!(
                accel.refresh_mode,
                Some(expected),
                "unexpected parse for '{yaml_value}'"
            );
        }
    }

    #[test]
    fn test_deserialize_all_storage_profiles() {
        for (yaml_value, expected) in [
            ("auto", StorageProfile::Auto),
            ("local_ssd", StorageProfile::LocalSsd),
            ("ssd", StorageProfile::LocalSsd),
            ("nvme", StorageProfile::LocalSsd),
            ("ebs", StorageProfile::Ebs),
            ("azure_disk", StorageProfile::Ebs),
            ("managed_disk", StorageProfile::Ebs),
            ("network_disk", StorageProfile::Ebs),
            ("tmpfs", StorageProfile::Tmpfs),
            ("ram", StorageProfile::Tmpfs),
            ("ramdisk", StorageProfile::Tmpfs),
            ("ramfs", StorageProfile::Tmpfs),
            ("memory", StorageProfile::Tmpfs),
        ] {
            let yaml = format!("storage_profile: {yaml_value}");
            let accel: Acceleration = yaml::from_str(&yaml)
                .unwrap_or_else(|_| panic!("should parse storage_profile '{yaml_value}'"));
            assert_eq!(
                accel.storage_profile, expected,
                "unexpected parse for '{yaml_value}'"
            );
        }
    }

    #[test]
    fn test_storage_display_round_trip() {
        for storage in [
            StorageProfile::Auto,
            StorageProfile::LocalSsd,
            StorageProfile::Ebs,
            StorageProfile::Tmpfs,
        ] {
            let s = storage.to_string();
            let yaml = format!("storage_profile: {s}");
            let accel: Acceleration = yaml::from_str(&yaml)
                .unwrap_or_else(|_| panic!("should parse storage_profile '{s}'"));
            assert_eq!(
                accel.storage_profile, storage,
                "round-trip failed for '{s}'"
            );
        }
    }
}
