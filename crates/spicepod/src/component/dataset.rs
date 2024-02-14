use std::{collections::HashMap, time::Duration};

use serde::{Deserialize, Serialize};

use super::WithDependsOn;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Dataset {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub from: String,

    pub name: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sql: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<HashMap<String, String>>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acceleration: Option<acceleration::Acceleration>,

    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(rename = "dependsOn", default)]
    pub depends_on: Vec<String>,
}

impl Dataset {
    /// Returns the dataset source - the first part of the `from` field before the first `/`.
    ///
    /// # Examples
    ///
    /// ```
    /// use spicepod::component::dataset::Dataset;
    ///
    /// let dataset = Dataset {
    ///    from: "foo/bar".to_string(),
    ///    name: "bar".to_string(),
    ///    acceleration: None,
    ///    params: Default::default(),
    ///    depends_on: Default::default(),
    /// };
    ///
    /// assert_eq!(dataset.source(), "foo".to_string());
    /// ```
    ///
    /// ```
    /// use spicepod::component::dataset::Dataset;
    ///
    /// let dataset = Dataset {
    ///   from: "foo".to_string(),
    ///   name: "bar".to_string(),
    ///   acceleration: None,
    ///   params: Default::default(),
    ///   depends_on: Default::default(),
    /// };
    ///
    /// assert_eq!(dataset.source(), "foo".to_string());
    /// ```
    #[must_use]
    pub fn source(&self) -> String {
        self.from.split('/').next().unwrap_or_default().to_string()
    }

    #[must_use]
    pub fn path(&self) -> String {
        match self.from.find('/') {
            Some(index) => self.from[index + 1..].to_string(),
            None => String::new(),
        }
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
}

impl WithDependsOn<Dataset> for Dataset {
    fn depends_on(&self, depends_on: &[String]) -> Dataset {
        Dataset {
            from: self.from.clone(),
            name: self.name.clone(),
            sql: self.sql.clone(),
            params: self.params.clone(),
            acceleration: self.acceleration.clone(),
            depends_on: depends_on.to_vec(),
        }
    }
}

pub mod acceleration {
    use serde::{Deserialize, Serialize};

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
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct Acceleration {
        #[serde(default)]
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
