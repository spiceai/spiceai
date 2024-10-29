/*
Copyright 2024 The Spice.ai OSS Authors

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

use std::collections::HashMap;

#[cfg(feature = "schemars")]
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{embeddings::ColumnEmbeddingConfig, params::Params, Nameable, WithDependsOn};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    #[default]
    Read,
    ReadWrite,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum TimeFormat {
    #[default]
    Timestamp,
    Timestamptz,
    UnixSeconds,
    UnixMillis,
    #[serde(rename = "ISO8601")]
    ISO8601,
}

impl std::fmt::Display for TimeFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct Dataset {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub from: String,

    pub name: String,

    pub description: Option<String>,

    #[serde(skip_serializing_if = "HashMap::is_empty")]
    #[serde(default)]
    pub metadata: HashMap<String, Value>,

    #[serde(default)]
    pub mode: Mode,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<Params>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub has_metadata_table: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replication: Option<replication::Replication>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time_column: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time_format: Option<TimeFormat>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acceleration: Option<acceleration::Acceleration>,

    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(rename = "embeddings", default)]
    pub embeddings: Vec<ColumnEmbeddingConfig>,

    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(rename = "dependsOn", default)]
    pub depends_on: Vec<String>,
}

impl Nameable for Dataset {
    fn name(&self) -> &str {
        &self.name
    }
}

impl Dataset {
    #[must_use]
    pub fn new(from: impl Into<String>, name: impl Into<String>) -> Self {
        Dataset {
            from: from.into(),
            name: name.into(),
            description: None,
            metadata: HashMap::default(),
            mode: Mode::default(),
            params: None,
            has_metadata_table: None,
            replication: None,
            time_column: None,
            time_format: None,
            acceleration: None,
            embeddings: Vec::default(),
            depends_on: Vec::default(),
        }
    }
}

impl WithDependsOn<Dataset> for Dataset {
    fn depends_on(&self, depends_on: &[String]) -> Dataset {
        Dataset {
            from: self.from.clone(),
            name: self.name.clone(),
            description: self.description.clone(),
            metadata: self.metadata.clone(),
            mode: self.mode.clone(),
            params: self.params.clone(),
            has_metadata_table: self.has_metadata_table,
            replication: self.replication.clone(),
            time_column: self.time_column.clone(),
            time_format: self.time_format.clone(),
            acceleration: self.acceleration.clone(),
            embeddings: self.embeddings.clone(),
            depends_on: depends_on.to_vec(),
        }
    }
}

pub mod acceleration {
    #[cfg(feature = "schemars")]
    use schemars::JsonSchema;
    use serde::{Deserialize, Serialize};
    use std::{collections::HashMap, fmt::Display};

    use crate::component::params::Params;

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

    /// Controls when the table is marked ready for queries.
    #[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Default)]
    #[cfg_attr(feature = "schemars", derive(JsonSchema))]
    #[serde(rename_all = "snake_case")]
    pub enum ReadyState {
        /// The table is ready once the initial load completes.
        #[default]
        OnLoad,
        /// The table is ready immediately on registration, with fallback to federated table for queries until the initial load completes.
        OnRegistration,
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

        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub engine: Option<String>,

        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub refresh_mode: Option<RefreshMode>,

        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub refresh_check_interval: Option<String>,

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
        pub retention_check_interval: Option<String>,

        #[serde(default, skip_serializing_if = "is_false")]
        pub retention_check_enabled: bool,

        #[serde(default)]
        pub on_zero_results: ZeroResultsAction,

        #[serde(default)]
        pub ready_state: ReadyState,

        #[serde(default, skip_serializing_if = "HashMap::is_empty")]
        pub indexes: HashMap<String, IndexType>,

        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub primary_key: Option<String>,

        #[serde(default, skip_serializing_if = "HashMap::is_empty")]
        pub on_conflict: HashMap<String, OnConflictBehavior>,
    }

    #[allow(clippy::trivially_copy_pass_by_ref)]
    fn is_false(b: &bool) -> bool {
        !b
    }

    const fn default_true() -> bool {
        true
    }

    impl Default for Acceleration {
        fn default() -> Self {
            Self {
                enabled: true,
                mode: Mode::Memory,
                engine: None,
                refresh_mode: None,
                refresh_check_interval: None,
                refresh_sql: None,
                refresh_data_window: None,
                refresh_append_overlap: None,
                refresh_retry_enabled: true,
                refresh_retry_max_attempts: None,
                refresh_jitter_enabled: false,
                refresh_jitter_max: None,
                params: None,
                retention_period: None,
                retention_check_interval: None,
                retention_check_enabled: false,
                on_zero_results: ZeroResultsAction::ReturnEmpty,
                ready_state: ReadyState::OnLoad,
                indexes: HashMap::default(),
                primary_key: None,
                on_conflict: HashMap::default(),
            }
        }
    }
}

pub mod replication {
    #[cfg(feature = "schemars")]
    use schemars::JsonSchema;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    #[cfg_attr(feature = "schemars", derive(JsonSchema))]
    pub struct Replication {
        #[serde(default)]
        pub enabled: bool,
    }
}
