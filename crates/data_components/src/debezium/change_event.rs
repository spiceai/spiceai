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

use std::{
    collections::HashMap,
    fmt::{self, Display, Formatter},
};

use serde::{Deserialize, Serialize};

/// A representation of a Debezium Change Event Key.
#[derive(Serialize, Deserialize)]
pub struct ChangeEventKey {
    pub schema: Schema,
    pub payload: serde_json::Value,
}

impl ChangeEventKey {
    pub fn from_bytes(bytz: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(bytz)
    }

    /// Gets the primary key fields from the schema.
    ///
    /// # Example
    ///
    /// ```json
    /// {
    ///     "schema": {
    ///         "type": "struct",
    ///         "fields": [
    ///             {
    ///                 "type": "int32",
    ///                 "optional": false,
    ///                 "default": 0,
    ///                 "field": "id"
    ///             }
    ///         ],
    ///         "optional": false,
    ///         "name": "acceleration.public.customer_addresses2.Key"
    ///     },
    ///     "payload": {
    ///         "id": 4
    ///     }
    /// }
    /// ```
    #[must_use]
    pub fn get_primary_key(&self) -> Vec<String> {
        self.schema
            .fields
            .iter()
            .filter_map(|field| field.field.clone())
            .collect()
    }
}

/// A representation of a Debezium Change Event Value.
#[derive(Serialize, Deserialize)]
pub struct ChangeEvent {
    pub schema: Schema,
    pub payload: Payload,
}

impl ChangeEvent {
    pub fn from_bytes(bytz: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(bytz)
    }

    #[must_use]
    pub fn get_schema_fields(&self) -> Option<Vec<&Field>> {
        self.schema
            .fields
            .iter()
            .find(|field| field.field.as_ref().is_some_and(|field| field == "after"))
            .and_then(|field| {
                field
                    .fields
                    .as_ref()
                    .map(|fields| fields.as_slice().iter().collect())
            })
    }
}

#[derive(Serialize, Deserialize)]
pub enum Op {
    #[serde(rename = "c")]
    Create,
    #[serde(rename = "u")]
    Update,
    #[serde(rename = "d")]
    Delete,
    #[serde(rename = "r")]
    Read,
    #[serde(rename = "t")]
    Truncate,
    #[serde(rename = "m")]
    Message,
}

impl Display for Op {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            Op::Create => write!(f, "c"),
            Op::Update => write!(f, "u"),
            Op::Delete => write!(f, "d"),
            Op::Read => write!(f, "r"),
            Op::Truncate => write!(f, "t"),
            Op::Message => write!(f, "m"),
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct Payload {
    pub before: Option<serde_json::Value>,
    pub after: serde_json::Value,
    pub source: Source,
    pub op: Op,
    pub ts_ms: i64,
    pub transaction: Option<serde_json::Value>,
}

#[derive(Serialize, Deserialize)]
pub struct Source {
    pub version: String,
    pub connector: String,
    pub name: String,
    pub ts_ms: i64,
    pub snapshot: String,
    pub db: String,
    pub table: String,
}

#[derive(Serialize, Deserialize)]
pub struct Schema {
    #[serde(rename = "type")]
    pub schema_type: String,
    pub fields: Vec<Field>,
    pub optional: bool,
    pub name: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Field {
    #[serde(rename = "type")]
    pub field_type: String,
    pub fields: Option<Vec<Field>>,
    pub optional: bool,
    pub name: Option<String>,
    pub field: Option<String>,
    pub version: Option<i64>,
    pub parameters: Option<HashMap<String, String>>,
    pub items: Option<Box<Field>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_field(name: &str, field_type: &str) -> Field {
        Field {
            field_type: field_type.to_string(),
            fields: None,
            optional: true,
            name: None,
            field: Some(name.to_string()),
            version: None,
            parameters: None,
            items: None,
        }
    }

    #[test]
    fn test_field_equality_same_fields() {
        let f1 = make_field("id", "int32");
        let f2 = make_field("id", "int32");
        assert_eq!(f1, f2);
    }

    #[test]
    fn test_field_equality_different_name() {
        let f1 = make_field("id", "int32");
        let f2 = make_field("user_id", "int32");
        assert_ne!(f1, f2);
    }

    #[test]
    fn test_field_equality_different_type() {
        let f1 = make_field("id", "int32");
        let f2 = make_field("id", "int64");
        assert_ne!(f1, f2);
    }

    #[test]
    fn test_schema_evolution_detects_added_column() {
        let old_schema = vec![make_field("id", "int32"), make_field("name", "string")];
        let new_schema = vec![
            make_field("id", "int32"),
            make_field("name", "string"),
            make_field("resources", "string"),
        ];
        assert_ne!(old_schema, new_schema);
    }

    #[test]
    fn test_schema_evolution_detects_removed_column() {
        let old_schema = vec![
            make_field("id", "int32"),
            make_field("name", "string"),
            make_field("old_col", "string"),
        ];
        let new_schema = vec![make_field("id", "int32"), make_field("name", "string")];
        assert_ne!(old_schema, new_schema);
    }

    #[test]
    fn test_schema_evolution_same_schema() {
        let old_schema = vec![make_field("id", "int32"), make_field("name", "string")];
        let new_schema = vec![make_field("id", "int32"), make_field("name", "string")];
        assert_eq!(old_schema, new_schema);
    }

    #[test]
    fn test_get_schema_fields_extracts_after_fields() {
        let json = r#"{
            "schema": {
                "type": "struct",
                "fields": [
                    {
                        "type": "struct",
                        "fields": [
                            {"type": "int32", "optional": false, "field": "id"},
                            {"type": "string", "optional": true, "field": "name"}
                        ],
                        "optional": true,
                        "name": "test.Value",
                        "field": "before"
                    },
                    {
                        "type": "struct",
                        "fields": [
                            {"type": "int32", "optional": false, "field": "id"},
                            {"type": "string", "optional": true, "field": "name"}
                        ],
                        "optional": true,
                        "name": "test.Value",
                        "field": "after"
                    },
                    {"type": "struct", "fields": [{"type": "string", "optional": false, "field": "version"}], "optional": false, "name": "source", "field": "source"},
                    {"type": "string", "optional": false, "field": "op"},
                    {"type": "int64", "optional": false, "field": "ts_ms"}
                ],
                "optional": false,
                "name": "test.Envelope"
            },
            "payload": {
                "before": null,
                "after": {"id": 1, "name": "test"},
                "source": {"version": "2.0", "connector": "postgresql", "name": "test", "ts_ms": 1000, "snapshot": "false", "db": "testdb", "table": "users"},
                "op": "c",
                "ts_ms": 1000
            }
        }"#;

        let event: ChangeEvent = serde_json::from_str(json).expect("valid change event JSON");
        let fields = event
            .get_schema_fields()
            .expect("schema fields should be present");
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].field.as_deref(), Some("id"));
        assert_eq!(fields[1].field.as_deref(), Some("name"));
    }

    #[test]
    fn test_get_primary_key_from_key_schema() {
        let json = r#"{
            "schema": {
                "type": "struct",
                "fields": [
                    {"type": "int32", "optional": false, "field": "id"}
                ],
                "optional": false,
                "name": "test.Key"
            },
            "payload": {"id": 1}
        }"#;

        let key: ChangeEventKey = serde_json::from_str(json).expect("valid change event key JSON");
        let pks = key.get_primary_key();
        assert_eq!(pks, vec!["id".to_string()]);
    }
}
