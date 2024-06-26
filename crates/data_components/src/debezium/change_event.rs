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

use std::{
    collections::HashMap,
    fmt::{self, Display, Formatter},
};

use serde::{Deserialize, Serialize};

/// A representation of a Debezium Change Event.
#[derive(Serialize, Deserialize)]
pub struct ChangeEvent {
    pub schema: Schema,
    pub payload: Payload,
}

impl ChangeEvent {
    pub fn from_bytes(bytz: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(bytz)
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
            Op::Create => write!(f, "create"),
            Op::Update => write!(f, "update"),
            Op::Delete => write!(f, "delete"),
            Op::Read => write!(f, "read"),
            Op::Truncate => write!(f, "truncate"),
            Op::Message => write!(f, "message"),
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
    pub sequence: String,
    pub schema: String,
    pub table: String,
    #[serde(rename = "txId")]
    pub tx_id: i64,
    pub lsn: i64,
    pub xmin: Option<serde_json::Value>,
}

#[derive(Serialize, Deserialize)]
pub struct Schema {
    #[serde(rename = "type")]
    pub schema_type: String,
    pub fields: Vec<Field>,
    pub optional: bool,
    pub name: String,
    pub version: i64,
}

#[derive(Serialize, Deserialize)]
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
