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

//! Decode Debezium change events from JSON (and Avro via [`super::avro`]) into
//! [`ChangeBatch`]es for the shared CDC apply path.
//!
//! Used by the push-ingest API (Debezium source plugins → Spice) and reusable
//! by the Kafka consumer path.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use snafu::prelude::*;

use crate::cdc::ChangeBatch;

use super::{
    arrow::changes::vector_to_change_batch,
    change_event::{ChangeEvent, Op},
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to parse Debezium JSON: {source}"))]
    JsonParse { source: serde_json::Error },

    #[snafu(display(
        "Empty CDC ingest body. Send one or more Debezium change events as a JSON object, a JSON array, or newline-delimited JSON"
    ))]
    EmptyBody,

    #[snafu(display("Failed to convert Debezium change events to Arrow: {source}"))]
    Convert { source: super::arrow::Error },

    #[snafu(display("{message}"))]
    Invalid { message: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Wire format for a CDC ingest request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CdcFormat {
    Json,
    Avro,
}

impl CdcFormat {
    /// Resolve format from an HTTP `Content-Type` header value.
    #[must_use]
    pub fn from_content_type(content_type: &str) -> Option<Self> {
        let ct = content_type
            .split(';')
            .next()
            .unwrap_or(content_type)
            .trim()
            .to_ascii_lowercase();
        match ct.as_str() {
            "application/json"
            | "application/vnd.apache.debezium.v1+json"
            | "application/vnd.debezium+json"
            | "application/x-ndjson"
            | "application/jsonl" => Some(Self::Json),
            "application/avro"
            | "application/vnd.apache.avro+binary"
            | "application/vnd.apache.debezium.v1+avro"
            | "application/vnd.debezium+avro"
            | "application/octet-stream" => Some(Self::Avro),
            _ => None,
        }
    }
}

/// Parse one or more Debezium JSON change events from a request body.
///
/// Accepts:
/// - a single `{schema?, payload}` or schemaless payload object
/// - a JSON array of those
/// - newline-delimited JSON (NDJSON)
///
/// Non-row ops (`m` message) are filtered out. Tombstones (empty body lines)
/// are skipped.
pub fn parse_json_change_events(body: &[u8]) -> Result<Vec<ChangeEvent>> {
    if body.iter().all(u8::is_ascii_whitespace) {
        return EmptyBodySnafu.fail();
    }

    // Prefer a single JSON value (object or array).
    if let Ok(value) = serde_json::from_slice::<serde_json::Value>(body) {
        return events_from_json_value(value);
    }

    // Fall back to NDJSON.
    let text = std::str::from_utf8(body).map_err(|e| Error::Invalid {
        message: format!("CDC JSON body is not valid UTF-8: {e}"),
    })?;
    let mut events = Vec::new();
    for (line_no, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let value: serde_json::Value =
            serde_json::from_str(line)
                .context(JsonParseSnafu)
                .map_err(|e| Error::Invalid {
                    message: format!("Invalid NDJSON on line {}: {e}", line_no + 1),
                })?;
        events.extend(events_from_json_value(value)?);
    }
    ensure!(!events.is_empty(), EmptyBodySnafu);
    Ok(filter_row_events(events))
}

fn events_from_json_value(value: serde_json::Value) -> Result<Vec<ChangeEvent>> {
    match value {
        serde_json::Value::Array(items) => {
            ensure!(!items.is_empty(), EmptyBodySnafu);
            let mut events = Vec::with_capacity(items.len());
            for item in items {
                events.push(ChangeEvent::from_json_value(item).context(JsonParseSnafu)?);
            }
            Ok(filter_row_events(events))
        }
        other => {
            let event = ChangeEvent::from_json_value(other).context(JsonParseSnafu)?;
            Ok(filter_row_events(vec![event]))
        }
    }
}

fn filter_row_events(events: Vec<ChangeEvent>) -> Vec<ChangeEvent> {
    events
        .into_iter()
        .filter(|e| !matches!(e.payload.op, Op::Message))
        .collect()
}

/// Convert parsed Debezium change events into a single [`ChangeBatch`].
pub fn change_events_to_batch(
    table_schema: &SchemaRef,
    primary_keys: &[String],
    events: &[ChangeEvent],
) -> Result<ChangeBatch> {
    ensure!(!events.is_empty(), EmptyBodySnafu);

    let refs: Vec<&ChangeEvent> = events.iter().collect();
    let source_commit_ts_ms = events
        .iter()
        .map(|e| {
            let source_ts = e.payload.source.ts_ms;
            if source_ts != 0 {
                source_ts
            } else {
                e.payload.ts_ms
            }
        })
        .max();

    let batch = vector_to_change_batch(table_schema, primary_keys, &refs, None)
        .context(ConvertSnafu)?
        .with_source_commit_ts_ms(source_commit_ts_ms);
    Ok(batch)
}

/// Convenience: parse JSON body and convert to a [`ChangeBatch`].
pub fn json_body_to_change_batch(
    table_schema: &SchemaRef,
    primary_keys: &[String],
    body: &[u8],
) -> Result<ChangeBatch> {
    let events = parse_json_change_events(body)?;
    change_events_to_batch(table_schema, primary_keys, &events)
}

/// Best-effort schema inference from embedded Debezium JSON schemas in events.
#[must_use]
pub fn infer_schema_from_events(events: &[ChangeEvent]) -> Option<SchemaRef> {
    for event in events {
        if let Some(fields) = event.get_schema_fields()
            && let Ok(schema) = super::arrow::convert_fields_to_arrow_schema(fields)
        {
            return Some(Arc::new(schema));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};

    fn table_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    #[test]
    fn parses_schemaless_create() {
        let body = br#"{"before":null,"after":{"id":1,"name":"a"},"source":{"connector":"postgresql","ts_ms":100},"op":"c","ts_ms":100}"#;
        let events = parse_json_change_events(body).expect("parse");
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0].payload.op, Op::Create));
        let batch =
            change_events_to_batch(&table_schema(), &["id".to_string()], &events).expect("batch");
        assert_eq!(batch.record.num_rows(), 1);
    }

    #[test]
    fn parses_array_and_filters_message_ops() {
        let body = br#"[
          {"before":null,"after":{"id":1,"name":"a"},"op":"c","ts_ms":1,"source":{}},
          {"before":null,"after":null,"op":"m","ts_ms":2,"source":{}},
          {"before":{"id":1,"name":"a"},"after":{"id":1,"name":"b"},"op":"u","ts_ms":3,"source":{}}
        ]"#;
        let events = parse_json_change_events(body).expect("parse");
        assert_eq!(events.len(), 2);
    }

    #[test]
    fn content_type_mapping() {
        assert_eq!(
            CdcFormat::from_content_type("application/json; charset=utf-8"),
            Some(CdcFormat::Json)
        );
        assert_eq!(
            CdcFormat::from_content_type("application/vnd.debezium+avro"),
            Some(CdcFormat::Avro)
        );
        assert_eq!(CdcFormat::from_content_type("text/plain"), None);
    }
}
