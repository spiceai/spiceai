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

    fn make_field(field_type: &str, field_name: &str, optional: bool) -> Field {
        Field {
            field_type: field_type.to_string(),
            fields: None,
            optional,
            name: None,
            field: Some(field_name.to_string()),
            version: None,
            parameters: None,
            items: None,
        }
    }

    #[test]
    fn field_equality_same_fields() {
        let f1 = make_field("int32", "id", false);
        let f2 = make_field("int32", "id", false);
        assert_eq!(f1, f2);
    }

    #[test]
    fn field_inequality_different_type() {
        let f1 = make_field("int32", "id", false);
        let f2 = make_field("int64", "id", false);
        assert_ne!(f1, f2);
    }

    #[test]
    fn field_inequality_different_name() {
        let f1 = make_field("int32", "id", false);
        let f2 = make_field("int32", "name", false);
        assert_ne!(f1, f2);
    }

    #[test]
    fn field_inequality_different_optionality() {
        let f1 = make_field("int32", "id", false);
        let f2 = make_field("int32", "id", true);
        assert_ne!(f1, f2);
    }

    #[test]
    fn schema_evolution_column_added() {
        let old_fields = vec![
            make_field("int32", "id", false),
            make_field("string", "name", true),
        ];
        let new_fields = vec![
            make_field("int32", "id", false),
            make_field("string", "name", true),
            make_field("string", "email", true),
        ];
        assert_ne!(old_fields, new_fields);
    }

    #[test]
    fn schema_evolution_column_removed() {
        let old_fields = vec![
            make_field("int32", "id", false),
            make_field("string", "name", true),
            make_field("string", "obsolete", true),
        ];
        let new_fields = vec![
            make_field("int32", "id", false),
            make_field("string", "name", true),
        ];
        assert_ne!(old_fields, new_fields);
    }

    #[test]
    fn schema_evolution_column_type_changed() {
        let old_fields = vec![
            make_field("int32", "id", false),
            make_field("int32", "count", false),
        ];
        let new_fields = vec![
            make_field("int32", "id", false),
            make_field("int64", "count", false),
        ];
        assert_ne!(old_fields, new_fields);
    }

    #[test]
    fn schema_unchanged() {
        let fields1 = vec![
            make_field("int32", "id", false),
            make_field("string", "name", true),
        ];
        let fields2 = vec![
            make_field("int32", "id", false),
            make_field("string", "name", true),
        ];
        assert_eq!(fields1, fields2);
    }

    #[test]
    fn field_with_parameters_equality() {
        let mut params = HashMap::new();
        params.insert("connect.decimal.precision".to_string(), "38".to_string());
        params.insert("scale".to_string(), "9".to_string());

        let f1 = Field {
            field_type: "bytes".to_string(),
            fields: None,
            optional: true,
            name: Some("org.apache.kafka.connect.data.Decimal".to_string()),
            field: Some("amount".to_string()),
            version: Some(1),
            parameters: Some(params.clone()),
            items: None,
        };
        let f2 = Field {
            field_type: "bytes".to_string(),
            fields: None,
            optional: true,
            name: Some("org.apache.kafka.connect.data.Decimal".to_string()),
            field: Some("amount".to_string()),
            version: Some(1),
            parameters: Some(params),
            items: None,
        };
        assert_eq!(f1, f2);
    }

    #[test]
    fn field_with_different_parameters() {
        let mut params1 = HashMap::new();
        params1.insert("connect.decimal.precision".to_string(), "38".to_string());
        params1.insert("scale".to_string(), "9".to_string());

        let mut params2 = HashMap::new();
        params2.insert("connect.decimal.precision".to_string(), "38".to_string());
        params2.insert("scale".to_string(), "18".to_string());

        let f1 = Field {
            parameters: Some(params1),
            ..make_field("bytes", "amount", true)
        };
        let f2 = Field {
            parameters: Some(params2),
            ..make_field("bytes", "amount", true)
        };
        assert_ne!(f1, f2);
    }
}
