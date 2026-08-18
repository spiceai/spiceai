/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! Decode Confluent-wire-format and raw Avro Debezium payloads into
//! [`ChangeEvent`](super::change_event::ChangeEvent)s.
//!
//! Confluent wire format: magic byte `0`, 4-byte big-endian schema id, Avro body.
//! Schema is resolved from a Schema Registry (`GET {url}/schemas/ids/{id}`) or
//! supplied explicitly (header / dataset param) for raw Avro bodies.
//!
//! # Formats
//!
//! - **JSON** path is in [`super::decode`]
//! - **Avro** path is this module

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use apache_avro::{Schema as AvroSchema, from_avro_datum, types::Value as AvroValue};
use arrow::datatypes::SchemaRef;
use parking_lot::Mutex;
use snafu::prelude::*;

use super::{
    change_event::{ChangeEvent, Op, Payload, Source},
    decode::{self, Error as DecodeError, Result as DecodeResult},
};
use crate::cdc::ChangeBatch;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Avro body is empty"))]
    EmptyBody,

    #[snafu(display(
        "Avro CDC ingest requires a schema. Provide Confluent wire-format bytes with schema_registry_url, or pass an Avro schema via the X-Avro-Schema header / avro_schema param"
    ))]
    MissingSchema,

    #[snafu(display("Invalid Avro schema JSON: {source}"))]
    InvalidSchema { source: Box<apache_avro::Error> },

    #[snafu(display("Failed to decode Avro datum: {source}"))]
    DecodeDatum { source: Box<apache_avro::Error> },

    #[snafu(display("Failed to fetch schema id {schema_id} from registry {url}: {message}"))]
    RegistryFetch {
        url: String,
        schema_id: u32,
        message: String,
    },

    #[snafu(display("Failed to map Avro Debezium payload: {message}"))]
    MapPayload { message: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

type SchemaCacheKey = (String, u32);
type SchemaCache = HashMap<SchemaCacheKey, Arc<AvroSchema>>;

/// Process-wide cache of Schema Registry schemas by (`base_url`, id).
static SCHEMA_CACHE: std::sync::LazyLock<Mutex<SchemaCache>> =
    std::sync::LazyLock::new(|| Mutex::new(HashMap::new()));

/// Registry schema ids are stable and few per deployment; the cap only bounds
/// memory if a process fetches from many registries or churns schema ids.
const SCHEMA_CACHE_MAX: usize = 256;

/// Process-wide cache of explicitly-supplied schemas (dataset `avro_schema`
/// param or `X-Avro-Schema` header), keyed by the schema JSON text so the same
/// schema is parsed once, not on every request.
static EXPLICIT_SCHEMA_CACHE: std::sync::LazyLock<Mutex<HashMap<String, Arc<AvroSchema>>>> =
    std::sync::LazyLock::new(|| Mutex::new(HashMap::new()));

/// Real deployments use a handful of schemas; the cap only bounds memory if a
/// client streams many distinct `X-Avro-Schema` values.
const EXPLICIT_SCHEMA_CACHE_MAX: usize = 32;

/// Shared HTTP client for Schema Registry fetches (connection reuse). The
/// client-level timeouts below are best-effort; the hard bound is the
/// `tokio::time::timeout` wrapping the whole fetch in `fetch_schema`, so a
/// registry that hangs mid-connect or mid-body-read cannot stall ingest even if
/// the builder fell back to a client without configured timeouts.
static REGISTRY_CLIENT: std::sync::LazyLock<reqwest::Client> = std::sync::LazyLock::new(|| {
    reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap_or_default()
});

/// Hard wall-clock bound on a full Schema Registry fetch (connect + body read),
/// enforced independently of the HTTP client's own timeouts.
const REGISTRY_FETCH_TIMEOUT: Duration = Duration::from_secs(15);

/// Options for decoding an Avro CDC body.
#[derive(Debug, Clone, Default)]
pub struct AvroDecodeOptions {
    /// Confluent / Apicurio-compatible registry base URL (no trailing path).
    pub schema_registry_url: Option<String>,
    /// Explicit Avro schema JSON (raw Avro body, or override).
    pub avro_schema_json: Option<String>,
}

/// Decode one Avro-encoded Debezium record into change events.
///
/// Async because resolving a Confluent wire-format schema id may fetch from
/// the Schema Registry; cached schemas resolve without I/O.
pub async fn parse_avro_change_events(
    body: &[u8],
    options: &AvroDecodeOptions,
) -> Result<Vec<ChangeEvent>> {
    ensure!(!body.is_empty(), EmptyBodySnafu);

    // Prefer an explicit schema (raw Avro body). Confluent wire format is only
    // selected when a registry URL is configured — many raw Avro datums start
    // with `0` (e.g. union null branch) and would otherwise be misdetected.
    let (schema, datum) = if let Some(schema_json) = options.avro_schema_json.as_deref() {
        (parse_explicit_schema(schema_json)?, body)
    } else if options.schema_registry_url.is_some() && is_confluent_wire_format(body) {
        let (schema_id, datum) = split_confluent_wire(body)?;
        let registry = options
            .schema_registry_url
            .as_deref()
            .context(MissingSchemaSnafu)?;
        let schema = fetch_schema(registry, schema_id).await?;
        (schema, datum)
    } else {
        return MissingSchemaSnafu.fail();
    };

    let value =
        from_avro_datum(schema.as_ref(), &mut std::io::Cursor::new(datum), None).map_err(|e| {
            Error::DecodeDatum {
                source: Box::new(e),
            }
        })?;
    let event = avro_value_to_change_event(&value)?;
    Ok(vec![event])
}

/// Decode Avro body and convert to a [`ChangeBatch`].
pub async fn avro_body_to_change_batch(
    table_schema: &SchemaRef,
    primary_keys: &[String],
    body: &[u8],
    options: &AvroDecodeOptions,
) -> DecodeResult<ChangeBatch> {
    let events =
        parse_avro_change_events(body, options)
            .await
            .map_err(|e| DecodeError::Invalid {
                message: e.to_string(),
            })?;
    decode::change_events_to_batch(table_schema, primary_keys, &events)
}

/// Parse (or fetch from cache) an explicitly-supplied Avro schema JSON.
fn parse_explicit_schema(schema_json: &str) -> Result<Arc<AvroSchema>> {
    if let Some(schema) = EXPLICIT_SCHEMA_CACHE.lock().get(schema_json).cloned() {
        return Ok(schema);
    }
    let schema =
        Arc::new(
            AvroSchema::parse_str(schema_json).map_err(|e| Error::InvalidSchema {
                source: Box::new(e),
            })?,
        );
    let mut cache = EXPLICIT_SCHEMA_CACHE.lock();
    if cache.len() >= EXPLICIT_SCHEMA_CACHE_MAX {
        cache.clear();
    }
    cache.insert(schema_json.to_string(), Arc::clone(&schema));
    Ok(schema)
}

fn is_confluent_wire_format(body: &[u8]) -> bool {
    body.len() > 5 && body[0] == 0
}

fn split_confluent_wire(body: &[u8]) -> Result<(u32, &[u8])> {
    ensure!(body.len() > 5 && body[0] == 0, EmptyBodySnafu);
    let id = u32::from_be_bytes([body[1], body[2], body[3], body[4]]);
    Ok((id, &body[5..]))
}

async fn fetch_schema(registry_url: &str, schema_id: u32) -> Result<Arc<AvroSchema>> {
    let base = registry_url.trim_end_matches('/');
    let key = (base.to_string(), schema_id);
    if let Some(schema) = SCHEMA_CACHE.lock().get(&key).cloned() {
        return Ok(schema);
    }

    let url = format!("{base}/schemas/ids/{schema_id}");
    // Hard-bound the entire network operation (connect + send + body read) so a
    // registry that hangs at any stage cannot stall ingest, independent of
    // whether the client's own timeouts were configured.
    let fetch = async {
        let response = REGISTRY_CLIENT
            .get(&url)
            .header(
                "Accept",
                "application/vnd.schemaregistry.v1+json, application/json",
            )
            .send()
            .await?;
        let status = response.status();
        if !status.is_success() {
            return Ok(Err(format!("HTTP {status}")));
        }
        Ok::<_, reqwest::Error>(Ok(response.json::<serde_json::Value>().await?))
    };

    let body: serde_json::Value = match tokio::time::timeout(REGISTRY_FETCH_TIMEOUT, fetch).await {
        Ok(Ok(Ok(body))) => body,
        Ok(Ok(Err(message))) => {
            return RegistryFetchSnafu {
                url: base.to_string(),
                schema_id,
                message,
            }
            .fail();
        }
        Ok(Err(e)) => {
            return RegistryFetchSnafu {
                url: base.to_string(),
                schema_id,
                message: e.to_string(),
            }
            .fail();
        }
        Err(_) => {
            return RegistryFetchSnafu {
                url: base.to_string(),
                schema_id,
                message: format!("timed out after {}s", REGISTRY_FETCH_TIMEOUT.as_secs()),
            }
            .fail();
        }
    };
    let schema_str = body
        .get("schema")
        .and_then(|v| v.as_str())
        .context(RegistryFetchSnafu {
            url: base.to_string(),
            schema_id,
            message: "response missing 'schema' field".to_string(),
        })?;

    let schema = Arc::new(
        AvroSchema::parse_str(schema_str).map_err(|e| Error::InvalidSchema {
            source: Box::new(e),
        })?,
    );
    let mut cache = SCHEMA_CACHE.lock();
    if cache.len() >= SCHEMA_CACHE_MAX {
        cache.clear();
    }
    cache.insert(key, Arc::clone(&schema));
    Ok(schema)
}

fn avro_value_to_change_event(value: &AvroValue) -> Result<ChangeEvent> {
    let record = match value {
        AvroValue::Record(fields) => fields,
        AvroValue::Union(_, inner) => {
            return avro_value_to_change_event(inner);
        }
        other => {
            return MapPayloadSnafu {
                message: format!("expected Avro record at root, got {other:?}"),
            }
            .fail();
        }
    };

    // Full envelope `{ payload: {...} }` or payload-as-root.
    let fields: &[(String, AvroValue)] = if let Some((_, AvroValue::Record(payload))) =
        record.iter().find(|(k, _)| k == "payload")
    {
        payload.as_slice()
    } else {
        record.as_slice()
    };

    let field = |name: &str| -> Option<&AvroValue> {
        fields.iter().find(|(k, _)| k == name).map(|(_, v)| v)
    };

    let op = match field("op") {
        Some(AvroValue::String(s)) => parse_op(s)?,
        Some(AvroValue::Union(_, inner)) => match inner.as_ref() {
            AvroValue::String(s) => parse_op(s)?,
            other => {
                return MapPayloadSnafu {
                    message: format!("op must be a string, got {other:?}"),
                }
                .fail();
            }
        },
        other => {
            return MapPayloadSnafu {
                message: format!("missing or invalid op field: {other:?}"),
            }
            .fail();
        }
    };

    let before = field("before")
        .map(avro_to_json)
        .transpose()?
        .and_then(|v| if v.is_null() { None } else { Some(v) });

    let after = field("after")
        .map(avro_to_json)
        .transpose()?
        .unwrap_or(serde_json::Value::Null);

    let ts_ms = field("ts_ms").map(avro_to_i64).transpose()?.unwrap_or(0);

    let source = field("source")
        .map(avro_to_source)
        .transpose()?
        .unwrap_or_default();

    Ok(ChangeEvent {
        schema: None,
        payload: Payload {
            before,
            after,
            source,
            op,
            ts_ms,
            transaction: None,
        },
    })
}

fn parse_op(s: &str) -> Result<Op> {
    match s {
        "c" => Ok(Op::Create),
        "u" => Ok(Op::Update),
        "d" => Ok(Op::Delete),
        "r" => Ok(Op::Read),
        "t" => Ok(Op::Truncate),
        "m" => Ok(Op::Message),
        other => MapPayloadSnafu {
            message: format!("unknown Debezium op '{other}'"),
        }
        .fail(),
    }
}

fn avro_to_source(value: &AvroValue) -> Result<Source> {
    let json = avro_to_json(value)?;
    serde_json::from_value(json).map_err(|e| Error::MapPayload {
        message: format!("invalid source block: {e}"),
    })
}

fn avro_to_i64(value: &AvroValue) -> Result<i64> {
    match value {
        AvroValue::Long(v) => Ok(*v),
        AvroValue::Int(v) => Ok(i64::from(*v)),
        AvroValue::Union(_, inner) => avro_to_i64(inner),
        other => MapPayloadSnafu {
            message: format!("expected int/long, got {other:?}"),
        }
        .fail(),
    }
}

fn avro_to_json(value: &AvroValue) -> Result<serde_json::Value> {
    Ok(match value {
        AvroValue::Null => serde_json::Value::Null,
        AvroValue::Boolean(b) => serde_json::Value::Bool(*b),
        AvroValue::Int(i) => serde_json::json!(i),
        AvroValue::Long(i) => serde_json::json!(i),
        AvroValue::Float(f) => serde_json::json!(f),
        AvroValue::Double(f) => serde_json::json!(f),
        AvroValue::String(s) | AvroValue::Enum(_, s) => serde_json::Value::String(s.clone()),
        AvroValue::Bytes(b) | AvroValue::Fixed(_, b) => serde_json::Value::String(
            base64::Engine::encode(&base64::engine::general_purpose::STANDARD, b),
        ),
        AvroValue::Array(items) => {
            let mut arr = Vec::with_capacity(items.len());
            for item in items {
                arr.push(avro_to_json(item)?);
            }
            serde_json::Value::Array(arr)
        }
        AvroValue::Map(map) => {
            let mut obj = serde_json::Map::new();
            for (k, v) in map {
                obj.insert(k.clone(), avro_to_json(v)?);
            }
            serde_json::Value::Object(obj)
        }
        AvroValue::Record(fields) => {
            let mut obj = serde_json::Map::new();
            for (k, v) in fields {
                obj.insert(k.clone(), avro_to_json(v)?);
            }
            serde_json::Value::Object(obj)
        }
        AvroValue::Union(_, inner) => avro_to_json(inner)?,
        AvroValue::Decimal(d) => {
            // Represent as string to preserve precision through JSON→Arrow path.
            serde_json::Value::String(format!("{d:?}"))
        }
        AvroValue::Uuid(u) => serde_json::Value::String(u.to_string()),
        AvroValue::Date(d) => serde_json::json!(d),
        AvroValue::TimeMillis(t) => serde_json::json!(t),
        AvroValue::TimestampMillis(t) | AvroValue::LocalTimestampMillis(t) => {
            serde_json::json!(t)
        }
        AvroValue::TimeMicros(t)
        | AvroValue::TimestampMicros(t)
        | AvroValue::LocalTimestampMicros(t) => serde_json::json!(t),
        AvroValue::Duration(d) => serde_json::Value::String(format!("{d:?}")),
        other => {
            return MapPayloadSnafu {
                message: format!("unsupported Avro value type for JSON conversion: {other:?}"),
            }
            .fail();
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use apache_avro::{to_avro_datum, types::Record};

    #[tokio::test]
    async fn decodes_raw_avro_payload() {
        let schema = r#"{
            "type": "record",
            "name": "Envelope",
            "fields": [
                {"name": "before", "type": ["null", {
                    "type": "record", "name": "Value",
                    "fields": [
                        {"name": "id", "type": "long"},
                        {"name": "name", "type": "string"}
                    ]
                }], "default": null},
                {"name": "after", "type": ["null", "Value"], "default": null},
                {"name": "op", "type": "string"},
                {"name": "ts_ms", "type": "long", "default": 0}
            ]
        }"#;
        let avro_schema = AvroSchema::parse_str(schema).expect("schema");
        let mut record = Record::new(&avro_schema).expect("record");
        record.put(
            "after",
            AvroValue::Union(
                1,
                Box::new(AvroValue::Record(vec![
                    ("id".to_string(), AvroValue::Long(7)),
                    ("name".to_string(), AvroValue::String("n".into())),
                ])),
            ),
        );
        record.put("before", AvroValue::Union(0, Box::new(AvroValue::Null)));
        record.put("op", AvroValue::String("c".into()));
        record.put("ts_ms", AvroValue::Long(42));

        let datum = to_avro_datum(&avro_schema, record).expect("datum");
        let events = parse_avro_change_events(
            &datum,
            &AvroDecodeOptions {
                avro_schema_json: Some(schema.to_string()),
                ..Default::default()
            },
        )
        .await
        .expect("parse");
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0].payload.op, Op::Create));
        assert_eq!(events[0].payload.after["id"], 7);
        assert_eq!(events[0].payload.ts_ms, 42);
    }
}
