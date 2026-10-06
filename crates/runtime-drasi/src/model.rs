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

//! Drasi's two ingestion wire formats.
//!
//! They are not variants of one schema — they share no field names, and a body
//! in one format is rejected by the other's endpoint:
//!
//! | | HTTP source | platform (Redis) source |
//! |---|---|---|
//! | discriminator | `operation: "insert"` | `op: "i"` |
//! | element | `element.type: "node"` | `payload.after` / `payload.before` |
//! | prior state | never sent | required on delete |
//! | timestamp | `timestamp` (optional) | `payload.source.ts_ns` (required) |
//!
//! Both are modelled here rather than taken from a Drasi crate. `drasi-core`
//! carries the equivalent Rust types, but it is the continuous-query engine —
//! depending on it to obtain six struct definitions compiles a Cypher parser and
//! an `opentelemetry` release that conflicts with this workspace's tracing stack.
//! The wire contract is small and stable, so it is spelled out instead.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// A graph node: one row of a relational source.
///
/// Drasi's own relational sources emit only nodes — foreign keys are *not*
/// auto-converted to relations, and joins are expressed in the continuous
/// query's Cypher instead (`MATCH (o:orders), (c:customers) WHERE o.customer_id
/// = c.id`). This forwarder follows that convention, so it never emits relation
/// elements and the direction ambiguity in Drasi's relation producers does not
/// arise.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeElement {
    pub id: String,
    /// Shared across every node in a batch — see [`ElementMapping::labels`].
    ///
    /// [`ElementMapping::labels`]: crate::element::ElementMapping::labels
    pub labels: Arc<[String]>,
    pub properties: Map<String, Value>,
}

// ---------------------------------------------------------------------------
// HTTP source format
// ---------------------------------------------------------------------------

/// The `element` object of an HTTP-source change, tagged `"type": "node"`.
///
/// Serialized separately from [`NodeElement`] because only this format carries
/// the `type` discriminator.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct HttpNodeElement<'a> {
    #[serde(rename = "type")]
    pub element_type: &'static str,
    /// Flattened rather than mirrored field-by-field, so a column added to
    /// [`NodeElement`] cannot be silently dropped from the HTTP wire format.
    #[serde(flatten)]
    pub node: &'a NodeElement,
}

impl<'a> From<&'a NodeElement> for HttpNodeElement<'a> {
    fn from(node: &'a NodeElement) -> Self {
        Self {
            element_type: "node",
            node,
        }
    }
}

/// One change in the HTTP source format.
///
/// `update` is a **full-state replace**, not a delta: every property is sent
/// every time. There is no `before` in this format at all.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "operation", rename_all = "lowercase")]
pub enum HttpSourceChange<'a> {
    Insert {
        element: HttpNodeElement<'a>,
        /// **Nanoseconds** since the Unix epoch. Drasi divides by `1_000_000` to
        /// reach the millisecond `effective_from` it stores, so sending
        /// milliseconds here dates every element to 1970.
        #[serde(skip_serializing_if = "Option::is_none")]
        timestamp: Option<u64>,
    },
    Update {
        element: HttpNodeElement<'a>,
        #[serde(skip_serializing_if = "Option::is_none")]
        timestamp: Option<u64>,
    },
    Delete {
        id: &'a str,
        #[serde(skip_serializing_if = "Option::is_none")]
        labels: Option<&'a [String]>,
        #[serde(skip_serializing_if = "Option::is_none")]
        timestamp: Option<u64>,
    },
}

/// Body of `POST /sources/{id}/events/batch`.
///
/// An object wrapping the array, not a bare array — Drasi's own `http-format.tsp`
/// `TypeSpec` declares a bare array, but the handler deserializes this struct, so
/// a bare array is rejected.
#[derive(Debug, Clone, Serialize)]
pub struct BatchEventRequest<'a> {
    pub events: Vec<HttpSourceChange<'a>>,
}

/// Response body of both HTTP ingestion routes.
#[derive(Debug, Clone, Deserialize)]
pub struct EventResponse {
    #[serde(default)]
    pub success: bool,
    #[serde(default)]
    pub message: String,
    /// Present only when at least one event failed.
    ///
    /// Must be `default`ed: on full success the field is *omitted*, not null.
    /// This is the only signal distinguishing a partial failure from a complete
    /// one, because a partial failure still returns HTTP 200 with
    /// `"success": true`.
    #[serde(default)]
    pub error: Option<String>,
}

// ---------------------------------------------------------------------------
// Platform (Redis / CloudEvents) format
// ---------------------------------------------------------------------------

/// The `payload.source` object. `table` is the *element kind*, not a SQL table.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct PlatformSource<'a> {
    /// Source namespace. Drasi's own producer fills this from its source id.
    pub db: &'a str,
    /// Element kind — only `"node"` and `"rel"` are legal. Not the SQL table
    /// name, which travels in the element's `labels`.
    pub table: &'static str,
    /// **Nanoseconds** since the Unix epoch. Required; Drasi divides by
    /// `1_000_000` to reach the millisecond `effective_from`.
    pub ts_ns: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct PlatformPayload<'a> {
    /// Present for `i` and `u`. Drasi errors if absent for those ops.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub after: Option<&'a NodeElement>,
    /// Required for `d` — Drasi rejects a delete whose `before` is absent.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub before: Option<&'a NodeElement>,
    pub source: PlatformSource<'a>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct PlatformChange<'a> {
    /// `"i"` | `"u"` | `"d"`. Any other value is rejected.
    pub op: &'static str,
    pub payload: PlatformPayload<'a>,
}

/// The `CloudEvents` envelope `XADD`ed onto the Drasi platform source's stream.
#[derive(Debug, Clone, Serialize)]
pub struct CloudEventEnvelope<'a> {
    pub specversion: &'static str,
    #[serde(rename = "type")]
    pub event_type: &'static str,
    pub source: &'a str,
    pub id: String,
    pub time: String,
    pub datacontenttype: &'static str,
    /// Always an array, even for a single change.
    pub data: Vec<PlatformChange<'a>>,
}

impl<'a> CloudEventEnvelope<'a> {
    #[must_use]
    pub fn new(source: &'a str, id: String, time: String, data: Vec<PlatformChange<'a>>) -> Self {
        Self {
            specversion: "1.0",
            event_type: "drasi.change",
            source,
            id,
            time,
            datacontenttype: "application/json",
            data,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node() -> NodeElement {
        let mut properties = Map::new();
        properties.insert("user_id".to_string(), Value::from(12_345));
        properties.insert("username".to_string(), Value::from("john_doe"));
        properties.insert("deleted_at".to_string(), Value::Null);
        NodeElement {
            id: "public.users:12345".to_string(),
            labels: Arc::from(vec!["public.users".to_string()]),
            properties,
        }
    }

    #[test]
    fn http_insert_matches_the_documented_shape() {
        let node = node();
        let change = HttpSourceChange::Insert {
            element: (&node).into(),
            timestamp: Some(1_699_900_000_000_000_000),
        };

        let json = serde_json::to_value(&change).expect("serializes");
        assert_eq!(
            json,
            serde_json::json!({
                "operation": "insert",
                "element": {
                    "type": "node",
                    "id": "public.users:12345",
                    "labels": ["public.users"],
                    "properties": {
                        "user_id": 12345,
                        "username": "john_doe",
                        "deleted_at": null
                    }
                },
                "timestamp": 1_699_900_000_000_000_000u64
            })
        );
    }

    /// A delete carries identity only — no element object, no properties.
    #[test]
    fn http_delete_carries_identity_only() {
        let labels = vec!["public.users".to_string()];
        let change = HttpSourceChange::Delete {
            id: "public.users:12345",
            labels: Some(&labels),
            timestamp: Some(1_699_900_002_000_000_000),
        };

        assert_eq!(
            serde_json::to_value(&change).expect("serializes"),
            serde_json::json!({
                "operation": "delete",
                "id": "public.users:12345",
                "labels": ["public.users"],
                "timestamp": 1_699_900_002_000_000_000u64
            })
        );
    }

    /// The batch body wraps the array in an object; a bare array is rejected by
    /// the Drasi handler.
    #[test]
    fn batch_body_wraps_events_in_an_object() {
        let node = node();
        let body = BatchEventRequest {
            events: vec![HttpSourceChange::Insert {
                element: (&node).into(),
                timestamp: None,
            }],
        };

        let json = serde_json::to_value(&body).expect("serializes");
        assert!(json.get("events").is_some_and(Value::is_array));
        assert!(
            json["events"][0].get("timestamp").is_none(),
            "an absent timestamp is omitted, letting Drasi stamp its own"
        );
    }

    /// On full success Drasi omits `error` rather than sending null, so the
    /// field has to tolerate absence.
    #[test]
    fn event_response_tolerates_an_absent_error_field() {
        let ok: EventResponse =
            serde_json::from_str(r#"{"success":true,"message":"All 2 events processed"}"#)
                .expect("deserializes");
        assert!(ok.error.is_none());

        let partial: EventResponse = serde_json::from_str(
            r#"{"success":true,"message":"Processed 8 events successfully, 2 failed","error":"boom"}"#,
        )
        .expect("deserializes");
        assert!(
            partial.success,
            "a partial failure still reports success: true — only `error` distinguishes it"
        );
        assert_eq!(partial.error.as_deref(), Some("boom"));
    }

    #[test]
    fn platform_delete_sends_before_not_after() {
        let node = node();
        let change = PlatformChange {
            op: "d",
            payload: PlatformPayload {
                after: None,
                before: Some(&node),
                source: PlatformSource {
                    db: "spice-cdc",
                    table: "node",
                    ts_ns: 1_699_900_002_000_000_000,
                },
            },
        };

        let json = serde_json::to_value(&change).expect("serializes");
        assert_eq!(json["op"], "d");
        assert!(
            json["payload"].get("after").is_none(),
            "after must be omitted on delete"
        );
        assert_eq!(json["payload"]["before"]["id"], "public.users:12345");
        assert_eq!(
            json["payload"]["source"]["table"], "node",
            "source.table is the element kind, not the SQL table"
        );
    }

    #[test]
    fn cloud_event_envelope_wraps_changes_in_a_data_array() {
        let node = node();
        let envelope = CloudEventEnvelope::new(
            "spice-cdc",
            "evt-1".to_string(),
            "2026-01-01T00:00:00Z".to_string(),
            vec![PlatformChange {
                op: "i",
                payload: PlatformPayload {
                    after: Some(&node),
                    before: None,
                    source: PlatformSource {
                        db: "spice-cdc",
                        table: "node",
                        ts_ns: 1_699_900_000_000_000_000,
                    },
                },
            }],
        );

        let json = serde_json::to_value(&envelope).expect("serializes");
        assert_eq!(json["specversion"], "1.0");
        assert!(json["data"].is_array());
        assert_eq!(json["data"][0]["op"], "i");
        assert!(json["data"][0]["payload"].get("before").is_none());
    }
}
