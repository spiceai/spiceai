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

use data_components::util::column_reference;
use datafusion::sql::TableReference;
use snafu::prelude::*;
use spicepod::component::{
    dataset as spicepod_dataset, embeddings::ColumnEmbeddingConfig, params::Params,
};
use std::{collections::HashMap, time::Duration};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Column for index {index} not found in schema. Valid columns: {valid_columns}"
    ))]
    IndexColumnNotFound {
        index: String,
        valid_columns: String,
    },

    #[snafu(display("Unable to get table constraints: {source}"))]
    UnableToGetTableConstraints {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Unable to convert a SchemaRef to a DFSchema: {source}"))]
    UnableToConvertSchemaRefToDFSchema {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Only one `on_conflict` target can be specified, or all `on_conflict` targets must be specified and set to `drop`. {extra_detail}"))]
    OnConflictTargetMismatch { extra_detail: String },

    #[snafu(display("Error parsing column reference {column_ref}: {source}"))]
    UnableToParseColumnReference {
        column_ref: String,
        source: column_reference::Error,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum Mode {
    #[default]
    Read,
    ReadWrite,
}

impl From<spicepod_dataset::Mode> for Mode {
    fn from(mode: spicepod_dataset::Mode) -> Self {
        match mode {
            spicepod_dataset::Mode::Read => Mode::Read,
            spicepod_dataset::Mode::ReadWrite => Mode::ReadWrite,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum TimeFormat {
    #[default]
    UnixSeconds,
    UnixMillis,
    ISO8601,
}

impl From<spicepod_dataset::TimeFormat> for TimeFormat {
    fn from(time_format: spicepod_dataset::TimeFormat) -> Self {
        match time_format {
            spicepod_dataset::TimeFormat::UnixSeconds => TimeFormat::UnixSeconds,
            spicepod_dataset::TimeFormat::UnixMillis => TimeFormat::UnixMillis,
            spicepod_dataset::TimeFormat::ISO8601 => TimeFormat::ISO8601,
        }
    }
}

impl std::fmt::Display for TimeFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Dataset {
    pub from: String,
    pub name: TableReference,
    pub mode: Mode,
    pub params: HashMap<String, String>,
    pub has_metadata_table: bool,
    pub replication: Option<replication::Replication>,
    pub time_column: Option<String>,
    pub time_format: Option<TimeFormat>,
    pub acceleration: Option<acceleration::Acceleration>,
    pub embeddings: Vec<ColumnEmbeddingConfig>,
}

impl TryFrom<spicepod_dataset::Dataset> for Dataset {
    type Error = crate::Error;

    fn try_from(dataset: spicepod_dataset::Dataset) -> std::result::Result<Self, Self::Error> {
        let acceleration = dataset
            .acceleration
            .map(acceleration::Acceleration::try_from)
            .transpose()?;

        let table_reference = Dataset::parse_table_reference(&dataset.name)?;

        Ok(Dataset {
            from: dataset.from,
            name: table_reference,
            mode: Mode::from(dataset.mode),
            params: dataset
                .params
                .as_ref()
                .map(Params::as_string_map)
                .unwrap_or_default(),
            has_metadata_table: dataset
                .has_metadata_table
                .unwrap_or(Dataset::have_metadata_table_by_default()),
            replication: dataset.replication.map(replication::Replication::from),
            time_column: dataset.time_column,
            time_format: dataset.time_format.map(TimeFormat::from),
            embeddings: dataset.embeddings,
            acceleration,
        })
    }
}

impl Dataset {
    pub fn try_new(from: String, name: &str) -> std::result::Result<Self, crate::Error> {
        Ok(Dataset {
            from,
            name: Self::parse_table_reference(name)?,
            mode: Mode::default(),
            params: HashMap::default(),
            has_metadata_table: Self::have_metadata_table_by_default(),
            replication: None,
            time_column: None,
            time_format: None,
            acceleration: None,
            embeddings: Vec::default(),
        })
    }

    #[must_use]
    /// Returns whether the dataset should enable metadata by default.
    fn have_metadata_table_by_default() -> bool {
        false
    }

    pub(crate) fn parse_table_reference(
        name: &str,
    ) -> std::result::Result<TableReference, crate::Error> {
        match TableReference::parse_str(name) {
            table_ref @ (TableReference::Bare { .. } | TableReference::Partial { .. }) => {
                Ok(table_ref)
            }
            TableReference::Full { catalog, .. } => crate::DatasetNameIncludesCatalogSnafu {
                catalog,
                name: name.to_string(),
            }
            .fail(),
        }
    }

    /// Returns the dataset source - the first part of the `from` field before the first `:`.
    ///
    /// # Examples
    ///
    /// ```
    /// use runtime::component::dataset::Dataset;
    ///
    /// let dataset = Dataset::new("foo:bar".to_string(), "bar".to_string());
    ///
    /// assert_eq!(dataset.source(), "foo".to_string());
    /// ```
    ///
    /// ```
    /// use runtime::component::dataset::Dataset;
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
    /// use crate::component::dataset::Dataset;
    ///
    /// let dataset = Dataset::new("foo:bar".to_string(), "bar".to_string());
    ///
    /// assert_eq!(dataset.path(), "bar".to_string());
    /// ```
    ///
    /// ```
    /// use crate::component::dataset::Dataset;
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
    pub fn mode(&self) -> Mode {
        self.mode
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

pub mod acceleration;

pub mod replication {
    use spicepod::component::dataset::replication as spicepod_replication;

    #[derive(Debug, Clone, PartialEq, Default)]
    pub struct Replication {
        pub enabled: bool,
    }

    impl From<spicepod_replication::Replication> for Replication {
        fn from(replication: spicepod_replication::Replication) -> Self {
            Replication {
                enabled: replication.enabled,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use data_components::util::column_reference::ColumnReference;

    use super::acceleration::{Acceleration, IndexType};

    #[test]
    fn test_indexes_roundtrip() {
        let indexes_map = HashMap::from([
            ("foo".to_string(), IndexType::Enabled),
            ("bar".to_string(), IndexType::Unique),
        ]);

        let indexes_str = Acceleration::hashmap_to_option_string(&indexes_map);
        assert!(indexes_str == "foo:enabled;bar:unique" || indexes_str == "bar:unique;foo:enabled");
        let roundtrip_indexes_map: HashMap<String, IndexType> =
            data_components::util::hashmap_from_option_string(&indexes_str);

        let roundtrip_indexes_map = roundtrip_indexes_map
            .into_iter()
            .map(|(k, v)| (k, v.to_string()))
            .collect::<HashMap<String, String>>();

        let indexes_map = indexes_map
            .into_iter()
            .map(|(k, v)| (k, v.to_string()))
            .collect::<HashMap<String, String>>();

        assert_eq!(indexes_map, roundtrip_indexes_map);
    }

    #[test]
    fn test_compound_indexes_roundtrip() {
        let indexes_map = HashMap::from([
            ("(foo, bar)".to_string(), IndexType::Enabled),
            ("bar".to_string(), IndexType::Unique),
        ]);

        let indexes_str = Acceleration::hashmap_to_option_string(&indexes_map);
        assert!(
            indexes_str == "(foo, bar):enabled;bar:unique"
                || indexes_str == "bar:unique;(foo, bar):enabled"
        );
        let roundtrip_indexes_map: HashMap<String, IndexType> =
            data_components::util::hashmap_from_option_string(&indexes_str);

        let roundtrip_indexes_map = roundtrip_indexes_map
            .into_iter()
            .map(|(k, v)| (k, v.to_string()))
            .collect::<HashMap<String, String>>();

        let indexes_map = indexes_map
            .into_iter()
            .map(|(k, v)| (k, v.to_string()))
            .collect::<HashMap<String, String>>();

        assert_eq!(indexes_map, roundtrip_indexes_map);
    }

    #[test]
    fn test_get_index_columns() {
        let column_ref = ColumnReference::try_from("foo").expect("valid columns");
        assert_eq!(column_ref.iter().collect::<Vec<_>>(), vec!["foo"]);

        let column_ref = ColumnReference::try_from("(foo, bar)").expect("valid columns");
        assert_eq!(column_ref.iter().collect::<Vec<_>>(), vec!["bar", "foo"]);

        let column_ref = ColumnReference::try_from("(foo,bar)").expect("valid columns");
        assert_eq!(column_ref.iter().collect::<Vec<_>>(), vec!["bar", "foo"]);

        let err = ColumnReference::try_from("(foo,bar").expect_err("invalid columns");
        assert_eq!(
            err.to_string(),
            "The column reference \"(foo,bar\" is missing a closing parenthensis."
        );
    }
}
