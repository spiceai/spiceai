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

#[derive(Clone, Serialize, Deserialize)]
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
