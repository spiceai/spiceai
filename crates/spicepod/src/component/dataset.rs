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

use std::{collections::HashMap, fs, time::Duration};

use serde::{Deserialize, Serialize};

use super::WithDependsOn;
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    Read,
    ReadWrite,
    Append,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Dataset {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub from: String,

    pub name: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    mode: Option<Mode>,

    /// Inline SQL that describes a view.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    sql: Option<String>,

    /// Reference to a SQL file that describes a view.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    sql_ref: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<HashMap<String, String>>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replication: Option<replication::Replication>,

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
            mode: None,
            sql: None,
            sql_ref: None,
            params: Option::default(),
            replication: None,
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
    pub fn acceleration_params(&self) -> Option<HashMap<String, String>> {
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
    pub fn refresh_interval(&self) -> Option<Duration> {
        if let Some(acceleration) = &self.acceleration {
            if let Some(refresh_interval) = &acceleration.refresh_interval {
                if let Ok(duration) = fundu::parse_duration(refresh_interval) {
                    return Some(duration);
                }
                tracing::warn!(
                    "Unable to parse refresh interval for dataset {}: {}",
                    self.name,
                    refresh_interval
                );
            }
        }

        None
    }

    #[must_use]
    pub fn is_view(&self) -> bool {
        self.sql.is_some() || self.sql_ref.is_some()
    }

    pub fn view_sql(&self) -> Result<Option<String>> {
        if let Some(sql) = &self.sql {
            return Ok(Some(sql.clone()));
        }

        if let Some(sql_ref) = &self.sql_ref {
            let sql =
                fs::read_to_string(sql_ref).context(UnableToLoadSqlFileSnafu { file: sql_ref })?;
            return Ok(Some(sql));
        }

        Ok(None)
    }

    #[must_use]
    pub fn mode(&self) -> Mode {
        self.mode.clone().unwrap_or(Mode::Read)
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
            acceleration: self.acceleration.clone(),
            depends_on: depends_on.to_vec(),
        }
    }
}

pub mod acceleration {
    use std::fmt;

    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    #[serde(rename_all = "lowercase")]
    pub enum RefreshMode {
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

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
    #[serde(rename_all = "lowercase")]
    pub enum Engine {
        #[default]
        Arrow,
        #[cfg(feature = "duckdb")]
        DuckDB,
        #[cfg(feature = "postgres")]
        Postgres,
        #[cfg(feature = "sqlite")]
        Sqlite,
    }

    impl fmt::Display for Engine {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(
                f,
                "{}",
                match *self {
                    Engine::Arrow => "arrow",
                    #[cfg(feature = "duckdb")]
                    Engine::DuckDB => "duckdb",
                    #[cfg(feature = "postgres")]
                    Engine::Postgres => "postgres",
                    #[cfg(feature = "sqlite")]
                    Engine::Sqlite => "sqlite",
                }
            )
        }
    }
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    pub struct Acceleration {
        #[serde(default = "default_true")]
        pub enabled: bool,

        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub mode: Option<Mode>,

        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub engine: Option<Engine>,

        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub refresh_interval: Option<String>,

        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub refresh_mode: Option<RefreshMode>,

        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub retention: Option<String>,

        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub params: Option<HashMap<String, String>>,

        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub engine_secret: Option<String>,
    }

    const fn default_true() -> bool {
        true
    }

    impl Acceleration {
        #[must_use]
        pub fn mode(&self) -> Mode {
            self.mode.clone().unwrap_or_default()
        }

        #[must_use]
        pub fn engine(&self) -> Engine {
            self.engine.clone().unwrap_or_default()
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
