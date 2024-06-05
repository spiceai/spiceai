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

use serde::{Deserialize, Serialize};

use super::{embeddings::ColumnEmbeddingConfig, params::Params, WithDependsOn};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    #[default]
    Read,
    ReadWrite,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "snake_case")]
pub enum TimeFormat {
    #[default]
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
pub struct Dataset {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub from: String,

    pub name: String,

    #[serde(default)]
    pub mode: Mode,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<Params>,

    #[serde(rename = "metadata", default, skip_serializing_if = "Option::is_none")]
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

impl Dataset {
    #[must_use]
    pub fn new(from: String, name: String) -> Self {
        Dataset {
            from,
            name,
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
    use serde::{Deserialize, Serialize};
    use std::fmt::Display;

    use crate::component::params::Params;

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
    #[serde(rename_all = "lowercase")]
    pub enum RefreshMode {
        #[default]
        Full,
        Append,
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
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

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    pub struct Acceleration {
        #[serde(default = "default_true")]
        pub enabled: bool,

        #[serde(default)]
        pub mode: Mode,

        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub engine: Option<String>,

        #[serde(default)]
        pub refresh_mode: RefreshMode,

        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub refresh_check_interval: Option<String>,

        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub refresh_sql: Option<String>,

        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub refresh_data_window: Option<String>,

        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub params: Option<Params>,

        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub engine_secret: Option<String>,

        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub retention_period: Option<String>,

        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub retention_check_interval: Option<String>,

        #[serde(default, skip_serializing_if = "is_false")]
        pub retention_check_enabled: bool,

        #[serde(default)]
        pub on_zero_results: ZeroResultsAction,
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
                refresh_mode: RefreshMode::Full,
                refresh_check_interval: None,
                refresh_sql: None,
                refresh_data_window: None,
                params: None,
                engine_secret: None,
                retention_period: None,
                retention_check_interval: None,
                retention_check_enabled: false,
                on_zero_results: ZeroResultsAction::ReturnEmpty,
            }
        }
    }
}

pub mod replication {
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    pub struct Replication {
        #[serde(default)]
        pub enabled: bool,
    }
}
