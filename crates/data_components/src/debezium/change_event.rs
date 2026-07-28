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
///
/// Supports both schema-embedded messages (`schemas.enable=true`) and
/// schemaless payloads (`schemas.enable=false`), where the root object is the
/// payload itself. When `schema` is absent, callers must supply an Arrow schema
/// (declared dataset columns) for row conversion.
#[derive(Serialize, Deserialize)]
pub struct ChangeEvent {
    #[serde(default)]
    pub schema: Option<Schema>,
    pub payload: Payload,
}

impl ChangeEvent {
    pub fn from_bytes(bytz: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(bytz)
    }

    /// Parse a JSON Debezium message that may be either a full
    /// `{schema, payload}` envelope or a schemaless payload object.
    ///
    /// A Debezium envelope carries `op` *inside* `payload`, whereas a schemaless
    /// payload carries `op` at the root. Treating the message as an envelope only
    /// when the root has no `op` avoids mis-detecting a schemaless event that
    /// happens to have a column literally named `payload`.
    pub fn from_json_value(value: serde_json::Value) -> Result<Self, serde_json::Error> {
        if value.get("op").is_none() && value.get("payload").is_some() {
            return serde_json::from_value(value);
        }
        let payload: Payload = serde_json::from_value(value)?;
        Ok(Self {
            schema: None,
            payload,
        })
    }

    #[must_use]
    pub fn get_schema_fields(&self) -> Option<Vec<&Field>> {
        self.schema.as_ref().and_then(|schema| {
            schema
                .fields
                .iter()
                .find(|field| field.field.as_ref().is_some_and(|field| field == "after"))
                .and_then(|field| {
                    field
                        .fields
                        .as_ref()
                        .map(|fields| fields.as_slice().iter().collect())
                })
        })
    }
}

#[derive(Clone, Copy, Serialize, Deserialize)]
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

impl Op {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Op::Create => "c",
            Op::Update => "u",
            Op::Delete => "d",
            Op::Read => "r",
            Op::Truncate => "t",
            Op::Message => "m",
        }
    }
}

impl Display for Op {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Serialize, Deserialize)]
pub struct Payload {
    pub before: Option<serde_json::Value>,
    /// Present for create/update/read; typically `null` for deletes.
    #[serde(default)]
    pub after: serde_json::Value,
    #[serde(default)]
    pub source: Source,
    pub op: Op,
    #[serde(default)]
    pub ts_ms: i64,
    pub transaction: Option<serde_json::Value>,
}

/// Debezium `source` block. Fields vary by connector (Postgres uses `db`+`table`,
/// SQL Server adds `schema`, Mongo uses collection names, etc.). Missing fields
/// default to empty so ingest stays source-agnostic.
#[derive(Default, Serialize, Deserialize)]
pub struct Source {
    #[serde(default)]
    pub version: String,
    #[serde(default)]
    pub connector: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub ts_ms: i64,
    #[serde(default)]
    pub snapshot: String,
    #[serde(default)]
    pub db: String,
    #[serde(default)]
    pub table: String,
}

#[derive(Serialize, Deserialize)]
pub struct Schema {
    #[serde(rename = "type")]
    pub schema_type: String,
    #[serde(default)]
    pub fields: Vec<Field>,
    #[serde(default)]
    pub optional: bool,
    #[serde(default)]
    pub name: String,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Field {
    #[serde(rename = "type")]
    pub field_type: String,
    pub fields: Option<Vec<Field>>,
    #[serde(default)]
    pub optional: bool,
    pub name: Option<String>,
    pub field: Option<String>,
    pub version: Option<i64>,
    pub parameters: Option<HashMap<String, String>>,
    pub items: Option<Box<Field>>,
}
