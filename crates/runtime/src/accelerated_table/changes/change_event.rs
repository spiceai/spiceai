use std::fmt::{self, Display, Formatter};

use crate::dataupdate::changes::ChangeEvent as DataUpdateChangeEvent;
use crate::dataupdate::changes::Op as DataUpdateOp;
use crate::dataupdate::changes::Source as DataUpdateSource;
use arrow::datatypes::SchemaRef;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct ChangeEvent {
    #[serde(skip)]
    table_schema: Option<SchemaRef>,
    pub schema: Schema,
    pub payload: Payload,
}

impl ChangeEvent {
    pub fn from_bytes(bytz: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(bytz)
    }

    pub fn set_schema(&mut self, schema: SchemaRef) {
        self.table_schema = Some(schema);
    }
}

impl From<ChangeEvent> for DataUpdateChangeEvent {
    fn from(mut event: ChangeEvent) -> Self {
        let Some(table_schema) = event.table_schema.take() else {
            panic!("Table schema not set");
        };

        let data = match event.payload.op {
            Op::Create | Op::Update | Op::Read => event.payload.after,
            Op::Delete => {
                let Some(before) = event.payload.before else {
                    todo!()
                };
                before
            }
            Op::Truncate => unimplemented!(),
            Op::Message | Op::Unknown => unimplemented!(),
        };

        DataUpdateChangeEvent {
            table_schema,
            op: event.payload.op.into(),
            ts_ms: event.payload.ts_ms,
            source: event.payload.source.into(),
            data,
        }
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
    #[serde(other)]
    Unknown,
}

impl From<Op> for DataUpdateOp {
    fn from(op: Op) -> Self {
        match op {
            Op::Create => DataUpdateOp::Create,
            Op::Update => DataUpdateOp::Update,
            Op::Delete => DataUpdateOp::Delete,
            Op::Read => DataUpdateOp::Read,
            Op::Truncate => DataUpdateOp::Truncate,
            Op::Message | Op::Unknown => unimplemented!(),
        }
    }
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
            Op::Unknown => write!(f, "unknown"),
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

impl From<Source> for DataUpdateSource {
    fn from(source: Source) -> Self {
        DataUpdateSource {
            version: Some(source.version),
            connector: Some(source.connector),
            name: Some(source.name),
            ts_ms: source.ts_ms,
            snapshot: Some(source.snapshot),
            db: source.db,
            schema: source.schema,
            table: source.table,
            tx_id: source.tx_id,
            lsn: source.lsn,
        }
    }
}

#[derive(Serialize, Deserialize)]
#[allow(clippy::struct_field_names)]
pub struct Schema {
    #[serde(rename = "type")]
    schema_type: String,
    fields: Vec<SchemaField>,
    optional: bool,
    name: String,
    version: i64,
}

#[derive(Serialize, Deserialize)]
pub struct SchemaField {
    #[serde(rename = "type")]
    field_type: String,
    fields: Option<Vec<FieldField>>,
    optional: bool,
    name: Option<String>,
    field: String,
    version: Option<i64>,
}

#[derive(Serialize, Deserialize)]
pub struct FieldField {
    #[serde(rename = "type")]
    field_type: Type,
    optional: bool,
    #[serde(rename = "default")]
    field_default: Option<Default>,
    field: String,
    name: Option<String>,
    version: Option<i64>,
    parameters: Option<Parameters>,
}

#[derive(Serialize, Deserialize)]
#[serde(untagged)]
pub enum Default {
    Integer(i64),
    String(String),
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Type {
    Int32,
    Int64,
    String,
}

#[derive(Serialize, Deserialize)]
pub struct Parameters {
    allowed: String,
}
