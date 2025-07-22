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

#[cfg(feature = "schemars")]
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fmt::Display};

use crate::{component::dataset::ReadyState, metric::Metrics, param::Params};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(rename_all = "lowercase")]
pub enum RefreshMode {
    Full,
    Append,
    Changes,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    #[default]
    Memory,
    File,
}

impl Display for Mode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Mode::Memory => write!(f, "memory"),
            Mode::File => write!(f, "file"),
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
#[serde(rename_all = "lowercase")]
pub enum OnConflictBehavior {
    #[default]
    Drop,
    Upsert,
}

impl Display for OnConflictBehavior {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OnConflictBehavior::Drop => write!(f, "drop"),
            OnConflictBehavior::Upsert => write!(f, "upsert"),
        }
    }
}

#[allow(clippy::struct_excessive_bools)]
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

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metrics: Option<Metrics>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub partition_by: Vec<String>,
}

#[allow(clippy::trivially_copy_pass_by_ref)]
fn is_false(b: &bool) -> bool {
    !b
}

const fn default_true() -> bool {
    true
}

impl Default for Acceleration {
    #[allow(deprecated)]
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
            metrics: None,
            partition_by: vec![],
        }
    }
}
