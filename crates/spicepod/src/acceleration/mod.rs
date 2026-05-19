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
/// - `write_back`: Writes commit to the local accelerator first and return after
///   that accelerator commit completes. The same mutation is then forwarded to
///   the federated source asynchronously, so the source may lag and source
///   persistence failures are logged rather than returned to the caller. This
///   mode requires `replication.enabled: true` as an explicit opt-in to those
///   asynchronous source durability semantics.
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
    #[deprecated(since = "1.0.0-rc.1", note = "Use `dataset.ready_state` instead.")]
    pub ready_state: Option<ReadyState>,

    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub indexes: HashMap<String, IndexType>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub primary_key: Option<String>,

    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub on_conflict: HashMap<String, OnConflictBehavior>,

    /// Controls write behavior for read-write accelerated datasets.
    /// Only applies when `access: read_write` and the dataset is accelerated.
    #[serde(default, skip_serializing_if = "is_default_write_mode")]
    pub write_mode: WriteMode,

    /// Storage profile for file-backed acceleration. `auto` detects the
    /// profile from the resolved acceleration path; use `local_ssd`/`ssd`/`nvme`
    /// or `ebs` to override detection.
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

const fn default_true() -> bool {
    true
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
    fn test_deserialize_all_storage_modes() {
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
