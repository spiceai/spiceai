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

#[cfg(test)]
mod tests {
    use super::{ChangeEvent, ChangeEventKey, Op, Payload};

    /// A full Debezium envelope (`schemas.enable=true`) for a create.
    fn envelope_json(op: &str, before: serde_json::Value, after: serde_json::Value) -> String {
        let mut envelope = serde_json::json!({
            "schema": {
                "type": "struct",
                "optional": false,
                "name": "acceleration.public.orders.Envelope",
                "fields": [
                    {
                        "type": "struct",
                        "optional": true,
                        "field": "before",
                        "fields": [
                            {"type": "int32", "optional": false, "field": "id"},
                            {"type": "string", "optional": true, "field": "name"}
                        ]
                    },
                    {
                        "type": "struct",
                        "optional": true,
                        "field": "after",
                        "fields": [
                            {"type": "int32", "optional": false, "field": "id"},
                            {"type": "string", "optional": true, "field": "name"}
                        ]
                    }
                ]
            },
            "payload": {
                "source": {
                    "version": "2.5.0",
                    "connector": "postgresql",
                    "name": "acceleration",
                    "ts_ms": 1_700_000_000_000_i64,
                    "snapshot": "false",
                    "db": "postgres",
                    "table": "orders"
                },
                "op": op,
                "ts_ms": 1_700_000_000_001_i64,
                "transaction": null
            }
        });
        envelope["payload"]["before"] = before;
        envelope["payload"]["after"] = after;
        envelope.to_string()
    }

    #[test]
    fn envelope_message_keeps_its_schema_and_payload() {
        let json = envelope_json("c", serde_json::Value::Null, serde_json::json!({"id": 1}));
        let event = ChangeEvent::from_bytes(json.as_bytes()).expect("envelope parses");

        assert!(event.schema.is_some(), "envelope carries a schema block");
        assert_eq!(event.payload.op.as_str(), "c");
        assert_eq!(event.payload.after, serde_json::json!({"id": 1}));
        assert!(event.payload.before.is_none());
        assert_eq!(event.payload.source.table, "orders");
        assert_eq!(event.payload.ts_ms, 1_700_000_000_001);
    }

    #[test]
    fn from_json_value_detects_an_envelope_by_the_absence_of_a_root_op() {
        let json = envelope_json(
            "u",
            serde_json::json!({"id": 1}),
            serde_json::json!({"id": 2}),
        );
        let value: serde_json::Value = serde_json::from_str(&json).expect("valid json");

        let event = ChangeEvent::from_json_value(value).expect("envelope parses");
        assert!(event.schema.is_some());
        assert_eq!(event.payload.op.as_str(), "u");
        assert_eq!(event.payload.after, serde_json::json!({"id": 2}));
    }

    #[test]
    fn from_json_value_reads_a_schemaless_message_from_the_root() {
        // `schemas.enable=false`: the payload *is* the message.
        let value = serde_json::json!({
            "before": null,
            "after": {"id": 7, "name": "row"},
            "op": "c",
            "ts_ms": 5
        });

        let event = ChangeEvent::from_json_value(value).expect("schemaless payload parses");
        assert!(
            event.schema.is_none(),
            "schemaless messages carry no schema"
        );
        assert_eq!(event.payload.op.as_str(), "c");
        assert_eq!(
            event.payload.after,
            serde_json::json!({"id": 7, "name": "row"})
        );
    }

    /// A schemaless row whose table happens to have a column named `payload`
    /// must not be mistaken for an envelope — the envelope branch would then
    /// read the *column value* as the change payload and silently ingest the
    /// wrong row. The root-level `op` is what settles it.
    #[test]
    fn a_schemaless_row_with_a_payload_column_is_not_read_as_an_envelope() {
        let value = serde_json::json!({
            "before": null,
            "after": {"id": 3, "payload": "user data"},
            "payload": "user data",
            "op": "c",
            "ts_ms": 0
        });

        let event = ChangeEvent::from_json_value(value).expect("schemaless payload parses");
        assert!(event.schema.is_none());
        assert_eq!(
            event.payload.after,
            serde_json::json!({"id": 3, "payload": "user data"}),
            "the row, not the `payload` column, is the change data"
        );
    }

    #[test]
    fn every_debezium_op_code_round_trips() {
        for (code, expected) in [
            ("c", Op::Create),
            ("u", Op::Update),
            ("d", Op::Delete),
            ("r", Op::Read),
            ("t", Op::Truncate),
            ("m", Op::Message),
        ] {
            let parsed: Op = serde_json::from_value(serde_json::Value::String(code.to_string()))
                .expect("known op code parses");
            assert_eq!(parsed.as_str(), code);
            assert_eq!(parsed.to_string(), code);
            assert_eq!(expected.as_str(), code);
        }
    }

    /// An op code Spice does not know must fail the parse outright. Mapping it
    /// onto a known op would apply the wrong mutation to the accelerator.
    #[test]
    fn an_unknown_op_code_is_rejected_rather_than_coerced() {
        let value = serde_json::json!({"before": null, "after": {}, "op": "z"});
        assert!(
            ChangeEvent::from_json_value(value).is_err(),
            "unknown op codes must not parse"
        );
    }

    /// Debezium omits `after` entirely on a delete. It has to default to JSON
    /// null rather than failing the parse, because the delete's row image lives
    /// in `before`.
    #[test]
    fn a_delete_without_an_after_field_defaults_after_to_null() {
        let value = serde_json::json!({
            "before": {"id": 9, "name": "gone"},
            "op": "d",
            "ts_ms": 1
        });

        let event = ChangeEvent::from_json_value(value).expect("delete parses");
        assert_eq!(event.payload.op.as_str(), "d");
        assert_eq!(event.payload.after, serde_json::Value::Null);
        assert_eq!(
            event.payload.before,
            Some(serde_json::json!({"id": 9, "name": "gone"}))
        );
    }

    /// The `source` block differs per connector, so every field defaults rather
    /// than failing the parse — ingest stays source-agnostic.
    #[test]
    fn a_missing_source_block_defaults_instead_of_failing() {
        let value = serde_json::json!({"before": null, "after": {"id": 1}, "op": "c"});

        let event = ChangeEvent::from_json_value(value).expect("payload without source parses");
        assert_eq!(event.payload.source.connector, "");
        assert_eq!(event.payload.source.db, "");
        assert_eq!(event.payload.source.table, "");
        assert_eq!(event.payload.source.ts_ms, 0);
        assert_eq!(event.payload.ts_ms, 0);
        assert!(event.payload.transaction.is_none());
    }

    #[test]
    fn get_schema_fields_reads_the_after_struct() {
        let json = envelope_json("c", serde_json::Value::Null, serde_json::json!({"id": 1}));
        let event = ChangeEvent::from_bytes(json.as_bytes()).expect("envelope parses");

        let fields = event.get_schema_fields().expect("after fields");
        let names: Vec<&str> = fields.iter().filter_map(|f| f.field.as_deref()).collect();
        assert_eq!(names, vec!["id", "name"]);
    }

    #[test]
    fn get_schema_fields_is_none_for_a_schemaless_event() {
        let value = serde_json::json!({"before": null, "after": {"id": 1}, "op": "c"});
        let event = ChangeEvent::from_json_value(value).expect("schemaless payload parses");

        assert!(
            event.get_schema_fields().is_none(),
            "a schemaless event has no field list; callers supply the Arrow schema"
        );
    }

    #[test]
    fn change_event_key_lists_the_primary_key_columns_in_schema_order() {
        let key_json = serde_json::json!({
            "schema": {
                "type": "struct",
                "optional": false,
                "name": "acceleration.public.orders.Key",
                "fields": [
                    {"type": "int32", "optional": false, "field": "tenant_id"},
                    {"type": "int32", "optional": false, "field": "id"}
                ]
            },
            "payload": {"tenant_id": 4, "id": 9}
        })
        .to_string();

        let key = ChangeEventKey::from_bytes(key_json.as_bytes()).expect("key parses");
        assert_eq!(key.get_primary_key(), vec!["tenant_id", "id"]);
    }

    #[test]
    fn a_key_schema_without_named_fields_yields_no_primary_key() {
        let key_json = serde_json::json!({
            "schema": {"type": "struct", "fields": [], "optional": false, "name": "k"},
            "payload": {}
        })
        .to_string();

        let key = ChangeEventKey::from_bytes(key_json.as_bytes()).expect("key parses");
        assert!(key.get_primary_key().is_empty());
    }

    #[test]
    fn a_payload_without_an_op_is_rejected() {
        // `op` has no default: a message that cannot say what it does must not
        // be silently treated as an insert.
        let parsed = serde_json::from_value::<Payload>(serde_json::json!({"after": {"id": 1}}));
        assert!(parsed.is_err(), "a payload with no `op` must not parse");
    }
}
