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

use std::collections::HashMap;

#[cfg(feature = "schemars")]
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{
    Nameable, WithDependsOn,
    dataset::{ReadyState, TimeFormat},
    is_default,
};
use crate::{
    acceleration::Acceleration, metadata::metadata_value_to_string, param::Params,
    semantic::Column, vector::VectorStore,
};
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct View {
    pub name: String,

    pub description: Option<String>,

    #[serde(skip_serializing_if = "HashMap::is_empty")]
    #[serde(default)]
    pub metadata: HashMap<String, Value>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub columns: Vec<Column>,

    /// Inline SQL that describes a view.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sql: Option<String>,

    /// Reference to a SQL file that describes a view.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sql_ref: Option<String>,

    /// Name of the column that carries the row's time value. Lets the
    /// acceleration / warm-tier data-window logic derive time-based
    /// properties (e.g. retention) for the view, mirroring datasets. For
    /// append data this should be at least creation-based; for
    /// mutating/change data it should be at least last-updated.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time_column: Option<String>,

    /// Encoding of `time_column`'s values (e.g. `timestamp`, `unix_seconds`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time_format: Option<TimeFormat>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acceleration: Option<Acceleration>,

    #[serde(default, skip_serializing_if = "is_default")]
    pub ready_state: ReadyState,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vectors: Option<VectorStore>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<Params>,

    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(rename = "dependsOn", default)]
    pub depends_on: Vec<String>,
}

impl Nameable for View {
    fn name(&self) -> &str {
        &self.name
    }
}

impl View {
    #[must_use]
    pub fn new(name: String) -> Self {
        Self {
            name,
            description: None,
            metadata: HashMap::default(),
            columns: vec![],
            sql: None,
            sql_ref: None,
            time_column: None,
            time_format: None,
            acceleration: None,
            ready_state: ReadyState::default(),
            depends_on: Vec::default(),
            vectors: None,
            params: None,
        }
    }

    #[must_use]
    pub fn with_acceleration(mut self, acceleration: Acceleration) -> Self {
        self.acceleration = Some(acceleration);
        self
    }

    #[must_use]
    pub fn with_sql(mut self, sql: impl Into<String>) -> Self {
        self.sql = Some(sql.into());
        self
    }

    #[must_use]
    pub fn metadata(&self) -> HashMap<String, String> {
        let mut metadata = HashMap::new();
        if let Some(d) = self.description.as_ref() {
            metadata.insert("description".to_string(), d.clone());
        }
        for (k, v) in &self.metadata {
            metadata.insert(k.clone(), metadata_value_to_string(v));
        }
        metadata
    }

    /// Find any primary keys explicitly defined in the [`View`]. Order of precedence:
    ///  1. Primary key defined in `.columns[].embeddings[].row_id`
    ///  2. Primary key defined in `.columns[].full_text_search[].row_id`
    #[must_use]
    pub fn primary_key_override(&self) -> Option<Vec<String>> {
        self.columns
            .iter()
            .find_map(|c| c.embeddings.iter().find_map(|e| e.row_ids.clone()))
            .or(self
                .columns
                .iter()
                .find_map(|c| c.full_text_search.as_ref().and_then(|f| f.row_ids.clone())))
    }
}

impl WithDependsOn<View> for View {
    fn depends_on(&self, depends_on: &[String]) -> View {
        Self {
            name: self.name.clone(),
            description: self.description.clone(),
            metadata: self.metadata.clone(),
            columns: self.columns.clone(),
            sql: self.sql.clone(),
            sql_ref: self.sql_ref.clone(),
            time_column: self.time_column.clone(),
            time_format: self.time_format.clone(),
            acceleration: self.acceleration.clone(),
            ready_state: self.ready_state,
            vectors: self.vectors.clone(),
            params: self.params.clone(),
            depends_on: depends_on.to_vec(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserializes_params_with_file_format() {
        let yaml = r"
name: my_view
sql: SELECT body FROM docs
params:
  file_format: md
";
        let view: View = yaml::from_str(yaml).expect("view should deserialize");
        let params = view.params.expect("params should be present");
        assert_eq!(
            params
                .as_string_map()
                .get("file_format")
                .map(String::as_str),
            Some("md")
        );
    }

    #[test]
    fn params_defaults_to_none_when_absent() {
        let yaml = r"
name: my_view
sql: SELECT body FROM docs
";
        let view: View = yaml::from_str(yaml).expect("view should deserialize");
        assert!(view.params.is_none());
    }

    #[test]
    fn deserializes_time_column_and_format() {
        // `time_format` uses the same case-insensitive parsing as datasets.
        let yaml = r"
name: my_view
sql: SELECT body, created_at FROM docs
time_column: created_at
time_format: ISO8601
";
        let view: View = yaml::from_str(yaml).expect("view should deserialize");
        assert_eq!(view.time_column.as_deref(), Some("created_at"));
        assert_eq!(view.time_format, Some(TimeFormat::ISO8601));
    }

    #[test]
    fn time_fields_default_to_none_when_absent() {
        let yaml = r"
name: my_view
sql: SELECT body FROM docs
";
        let view: View = yaml::from_str(yaml).expect("view should deserialize");
        assert!(view.time_column.is_none());
        assert!(view.time_format.is_none());
    }
}
