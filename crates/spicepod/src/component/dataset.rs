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

use std::{fs, time::Duration};

use serde::{Deserialize, Serialize};

use super::{params::Params, WithDependsOn};
use snafu::prelude::*;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to load SQL file {file}: {source}"))]
    UnableToLoadSqlFile {
        file: String,
        source: std::io::Error,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

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
    mode: Mode,

    /// Inline SQL that describes a view.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    sql: Option<String>,

    /// Reference to a SQL file that describes a view.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    sql_ref: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<Params>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replication: Option<replication::Replication>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time_column: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time_format: Option<TimeFormat>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acceleration: Option<acceleration::Acceleration>,

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
            sql: None,
            sql_ref: None,
            params: None,
            replication: None,
            time_column: None,
            time_format: None,
            acceleration: None,
            depends_on: Vec::default(),
        }
    }

    /// Returns the dataset source - the first part of the `from` field before the first `:`.
    ///
    /// # Examples
    ///
    /// ```
    /// use spicepod::component::dataset::Dataset;
    ///
    /// let dataset = Dataset::new("foo:bar".to_string(), "bar".to_string());
    ///
    /// assert_eq!(dataset.source(), "foo".to_string());
    /// ```
    ///
    /// ```
    /// use spicepod::component::dataset::Dataset;
    ///
    /// let dataset = Dataset::new("foo".to_string(), "bar".to_string());
    ///
    /// assert_eq!(dataset.source(), "spiceai");
    /// ```
    #[must_use]
    pub fn source(&self) -> String {
        let parts: Vec<&str> = self.from.splitn(2, ':').collect();
        if parts.len() > 1 {
            parts[0].to_string()
        } else {
            if self.from == "localhost" || self.from.is_empty() {
                return "localhost".to_string();
            }
            "spiceai".to_string()
        }
    }

    /// Returns the dataset path - the remainder of the `from` field after the first `:` or the whole string if no `:`.
    ///
    /// # Examples
    ///
    /// ```
    /// use spicepod::component::dataset::Dataset;
    ///
    /// let dataset = Dataset::new("foo:bar".to_string(), "bar".to_string());
    ///
    /// assert_eq!(dataset.path(), "bar".to_string());
    /// ```
    ///
    /// ```
    /// use spicepod::component::dataset::Dataset;
    ///
    /// let dataset = Dataset::new("foo".to_string(), "bar".to_string());
    ///
    /// assert_eq!(dataset.path(), "foo".to_string());
    /// ```
    #[must_use]
    pub fn path(&self) -> String {
        match self.from.find(':') {
            Some(index) => self.from[index + 1..].to_string(),
            None => self.from.clone(),
        }
    }

    #[must_use]
    pub fn acceleration_params(&self) -> Option<Params> {
        if let Some(acceleration) = &self.acceleration {
            return acceleration.params.clone();
        }

        None
    }

    #[must_use]
    pub fn engine_secret(&self) -> Option<String> {
        if let Some(acceleration) = &self.acceleration {
            return acceleration.engine_secret.clone();
        }

        None
    }

    #[must_use]
    pub fn refresh_check_interval(&self) -> Option<Duration> {
        if let Some(acceleration) = &self.acceleration {
            if let Some(refresh_check_interval) = &acceleration.refresh_check_interval {
                if let Ok(duration) = fundu::parse_duration(refresh_check_interval) {
                    return Some(duration);
                }
                tracing::warn!(
                    "Unable to parse refresh interval for dataset {}: {}",
                    self.name,
                    refresh_check_interval
                );
            }
        }

        None
    }

    pub fn retention_check_interval(&self) -> Option<Duration> {
        if let Some(acceleration) = &self.acceleration {
            if let Some(retention_check_interval) = &acceleration.retention_check_interval {
                if let Ok(duration) = fundu::parse_duration(retention_check_interval) {
                    return Some(duration);
                }
                tracing::warn!(
                    "Unable to parse retention check interval for dataset {}: {}",
                    self.name,
                    retention_check_interval
                );
            }
        }

        None
    }

    pub fn retention_period(&self) -> Option<Duration> {
        if let Some(acceleration) = &self.acceleration {
            if let Some(retention_period) = &acceleration.retention_period {
                if let Ok(duration) = fundu::parse_duration(retention_period) {
                    return Some(duration);
                }
                tracing::warn!(
                    "Unable to parse retention period for dataset {}: {}",
                    self.name,
                    retention_period
                );
            }
        }

        None
    }

    #[must_use]
    pub fn refresh_sql(&self) -> Option<String> {
        if let Some(acceleration) = &self.acceleration {
            return acceleration.refresh_sql.clone();
        }

        None
    }

    #[must_use]
    pub fn refresh_data_window(&self) -> Option<Duration> {
        if let Some(acceleration) = &self.acceleration {
            if let Some(refresh_data_window) = &acceleration.refresh_data_window {
                if let Ok(duration) = fundu::parse_duration(refresh_data_window) {
                    return Some(duration);
                }
                tracing::warn!(
                    "Unable to parse refresh period for dataset {}: {}",
                    self.name,
                    refresh_data_window
                );
            }
        }

        None
    }

    #[must_use]
    pub fn is_view(&self) -> bool {
        self.sql.is_some() || self.sql_ref.is_some()
    }

    #[must_use]
    pub fn view_sql(&self) -> Option<Result<String>> {
        if let Some(sql) = &self.sql {
            return Some(Ok(sql.clone()));
        }

        if let Some(sql_ref) = &self.sql_ref {
            return Some(Self::load_sql_ref(sql_ref));
        }

        None
    }

    fn load_sql_ref(sql_ref: &str) -> Result<String> {
        let sql =
            fs::read_to_string(sql_ref).context(UnableToLoadSqlFileSnafu { file: sql_ref })?;
        Ok(sql)
    }

    #[must_use]
    pub fn mode(&self) -> Mode {
        self.mode.clone()
    }

    #[must_use]
    pub fn is_accelerated(&self) -> bool {
        if let Some(acceleration) = &self.acceleration {
            return acceleration.enabled;
        }

        false
    }

    #[must_use]
    pub fn is_file_accelerated(&self) -> bool {
        if let Some(acceleration) = &self.acceleration {
            return acceleration.enabled && acceleration.mode == acceleration::Mode::File;
        }

        false
    }
}

impl WithDependsOn<Dataset> for Dataset {
    fn depends_on(&self, depends_on: &[String]) -> Dataset {
        Dataset {
            from: self.from.clone(),
            name: self.name.clone(),
            mode: self.mode.clone(),
            sql: self.sql.clone(),
            sql_ref: self.sql_ref.clone(),
            params: self.params.clone(),
            replication: self.replication.clone(),
            time_column: self.time_column.clone(),
            time_format: self.time_format.clone(),
            acceleration: self.acceleration.clone(),
            depends_on: depends_on.to_vec(),
        }
    }
}

pub mod acceleration {
    use serde::{Deserialize, Serialize};
    use std::{fmt::Display, sync::Arc};

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

        #[serde(default, skip_serializing_if = "is_default_mode")]
        pub mode: Mode,

        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub engine: Option<String>,

        #[serde(default, skip_serializing_if = "is_default_refresh_mode")]
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

        #[serde(default, skip_serializing_if = "is_default_zero_results_action")]
        pub on_zero_results: ZeroResultsAction,
    }

    #[allow(clippy::trivially_copy_pass_by_ref)]
    fn is_false(b: &bool) -> bool {
        !b
    }

    const fn default_true() -> bool {
        true
    }

    impl Acceleration {
        #[must_use]
        pub fn mode(&self) -> Mode {
            self.mode.clone()
        }

        #[must_use]
        pub fn engine(&self) -> Arc<str> {
            self.engine
                .as_ref()
                .map_or_else(|| "arrow", String::as_str)
                .into()
        }
    }

    fn is_default_zero_results_action(action: &ZeroResultsAction) -> bool {
        *action == ZeroResultsAction::default()
    }

    fn is_default_mode(mode: &Mode) -> bool {
        *mode == Mode::default()
    }

    fn is_default_refresh_mode(refresh_mode: &RefreshMode) -> bool {
        *refresh_mode == RefreshMode::default()
    }

    impl Default for Acceleration {
        fn default() -> Self {
            Acceleration {
                enabled: default_true(),
                mode: Mode::default(),
                engine: None,
                refresh_mode: RefreshMode::default(),
                refresh_check_interval: None,
                refresh_sql: None,
                refresh_data_window: None,
                params: None,
                engine_secret: None,
                retention_period: None,
                retention_check_interval: None,
                retention_check_enabled: false,
                on_zero_results: ZeroResultsAction::default(),
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
