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

//! Parse S3 Event Notifications as delivered to SQS.
//!
//! Supports the three bodies operators actually see on a queue:
//! direct S3 → SQS (`Records`), S3 → SNS → SQS (SNS envelope with a nested
//! `Message`), and EventBridge S3 events (`detail.bucket` / `detail.object`).
//! An S3 `s3:TestEvent` used when a notification is first attached has no
//! object and yields an empty list so the consumer can drop it.

use percent_encoding::percent_decode_str;
use serde_json::Value;
use url::Url;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectEventKind {
    Created,
    Removed,
    Other,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3ObjectEvent {
    pub event_name: String,
    pub kind: ObjectEventKind,
    pub bucket: String,
    pub key: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseError {
    InvalidJson { detail: String },
    Unrecognized { detail: String },
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidJson { detail } => {
                write!(f, "SQS message body is not JSON: {detail}")
            }
            Self::Unrecognized { detail } => {
                write!(
                    f,
                    "SQS message body is not an S3 event notification: {detail}"
                )
            }
        }
    }
}

impl std::error::Error for ParseError {}

/// Parse one SQS message body into zero or more object events.
///
/// # Errors
///
/// Returns [`ParseError::InvalidJson`] when the body is not JSON, and
/// [`ParseError::Unrecognized`] when it is JSON but not an S3 / SNS /
/// EventBridge notification (so a poison payload is visible rather than
/// silently treated as a test event).
pub fn parse_notification_body(body: &str) -> Result<Vec<S3ObjectEvent>, ParseError> {
    let value: Value = serde_json::from_str(body).map_err(|e| ParseError::InvalidJson {
        detail: e.to_string(),
    })?;
    parse_notification_value(&value)
}

fn parse_notification_value(value: &Value) -> Result<Vec<S3ObjectEvent>, ParseError> {
    if is_s3_test_event(value) {
        return Ok(Vec::new());
    }

    if let Some(message) = value.get("Message").and_then(Value::as_str) {
        let inner: Value = serde_json::from_str(message).map_err(|e| ParseError::InvalidJson {
            detail: format!("SNS Message is not JSON: {e}"),
        })?;
        return parse_notification_value(&inner);
    }

    if let Some(records) = value.get("Records").and_then(Value::as_array) {
        return records.iter().map(parse_s3_record).collect();
    }

    if value.get("detail").is_some() {
        return Ok(vec![parse_eventbridge_detail(value)?]);
    }

    Err(ParseError::Unrecognized {
        detail: "expected an S3 `Records` array, an SNS `Message`, or an EventBridge `detail`"
            .to_string(),
    })
}

fn is_s3_test_event(value: &Value) -> bool {
    value.get("Event").and_then(Value::as_str) == Some("s3:TestEvent")
        || value.get("eventName").and_then(Value::as_str) == Some("s3:TestEvent")
}

fn parse_s3_record(record: &Value) -> Result<S3ObjectEvent, ParseError> {
    let event_name = record
        .get("eventName")
        .and_then(Value::as_str)
        .unwrap_or("unknown")
        .to_string();
    let bucket = record
        .pointer("/s3/bucket/name")
        .and_then(Value::as_str)
        .ok_or_else(|| ParseError::Unrecognized {
            detail: "S3 record is missing `s3.bucket.name`".to_string(),
        })?
        .to_string();
    let encoded_key = record
        .pointer("/s3/object/key")
        .and_then(Value::as_str)
        .ok_or_else(|| ParseError::Unrecognized {
            detail: "S3 record is missing `s3.object.key`".to_string(),
        })?;

    Ok(S3ObjectEvent {
        kind: event_kind(&event_name),
        event_name,
        bucket,
        key: decode_s3_key(encoded_key),
    })
}

fn parse_eventbridge_detail(value: &Value) -> Result<S3ObjectEvent, ParseError> {
    let event_name = value
        .get("detail-type")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|detail_type| !detail_type.is_empty())
        .ok_or_else(|| ParseError::Unrecognized {
            detail: "EventBridge event is missing `detail-type`".to_string(),
        })?
        .to_string();
    let bucket = value
        .pointer("/detail/bucket/name")
        .and_then(Value::as_str)
        .ok_or_else(|| ParseError::Unrecognized {
            detail: "EventBridge detail is missing `detail.bucket.name`".to_string(),
        })?
        .to_string();
    // EventBridge delivers `detail.object.key` unencoded. Do not run it through
    // `decode_s3_key` (`+` → space, `%xx` decode) or the object path is wrong.
    let key = value
        .pointer("/detail/object/key")
        .and_then(Value::as_str)
        .ok_or_else(|| ParseError::Unrecognized {
            detail: "EventBridge detail is missing `detail.object.key`".to_string(),
        })?
        .to_string();

    Ok(S3ObjectEvent {
        kind: event_kind(&event_name),
        event_name,
        bucket,
        key,
    })
}

#[must_use]
pub fn event_kind(event_name: &str) -> ObjectEventKind {
    let name = event_name.to_ascii_lowercase();
    if name.contains("objectcreated") || name.contains("object created") {
        ObjectEventKind::Created
    } else if name.contains("objectremoved")
        || name.contains("object deleted")
        || name.contains("objectdeleted")
    {
        ObjectEventKind::Removed
    } else {
        ObjectEventKind::Other
    }
}

/// S3 encodes object keys in notifications (`%20` / `+` for space).
#[must_use]
pub fn decode_s3_key(encoded: &str) -> String {
    let plus_as_space = encoded.replace('+', " ");
    percent_decode_str(&plus_as_space)
        .decode_utf8_lossy()
        .into_owned()
}

/// Build an `s3://` URL for a decoded object key. `Url::set_path` percent-encodes
/// spaces and reserved characters so `Url::parse` in the listing connector accepts it.
///
/// # Errors
///
/// Returns a parse error when `bucket` is not a valid URL host.
pub fn s3_object_from(bucket: &str, key: &str) -> Result<String, url::ParseError> {
    let mut url = Url::parse(&format!("s3://{bucket}"))?;
    url.set_path(key);
    Ok(url.to_string())
}

/// Whether `event` belongs to this dataset's bucket and key prefix.
#[must_use]
pub fn matches_dataset(event: &S3ObjectEvent, bucket: &str, key_prefix: &str) -> bool {
    if event.bucket != bucket {
        return false;
    }
    if key_prefix.is_empty() {
        return true;
    }
    event.key.starts_with(key_prefix)
}

#[cfg(test)]
mod tests {
    use super::*;

    const S3_PUT_BODY: &str = r#"{
        "Records": [{
            "eventName": "ObjectCreated:Put",
            "s3": {
                "bucket": {"name": "my-bucket"},
                "object": {"key": "events/year%3D2026/data+file.parquet"}
            }
        }]
    }"#;

    const SNS_WRAPPED: &str = r#"{
        "Type": "Notification",
        "Message": "{\"Records\":[{\"eventName\":\"ObjectCreated:CompleteMultipartUpload\",\"s3\":{\"bucket\":{\"name\":\"my-bucket\"},\"object\":{\"key\":\"events/part.parquet\"}}}]}"
    }"#;

    const EVENTBRIDGE_CREATED: &str = r#"{
        "source": "aws.s3",
        "detail-type": "Object Created",
        "detail": {
            "bucket": {"name": "my-bucket"},
            "object": {"key": "events/eb.parquet"}
        }
    }"#;

    const EVENTBRIDGE_DELETED: &str = r#"{
        "source": "aws.s3",
        "detail-type": "Object Deleted",
        "detail": {
            "bucket": {"name": "my-bucket"},
            "object": {"key": "events/gone.parquet"}
        }
    }"#;

    const TEST_EVENT: &str = r#"{
        "Service": "Amazon S3",
        "Event": "s3:TestEvent",
        "Bucket": "my-bucket"
    }"#;

    #[test]
    fn parse_direct_s3_put_decodes_key() {
        let events = parse_notification_body(S3_PUT_BODY).expect("valid S3 notification");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].kind, ObjectEventKind::Created);
        assert_eq!(events[0].bucket, "my-bucket");
        assert_eq!(events[0].key, "events/year=2026/data file.parquet");
    }

    #[test]
    fn parse_sns_wrapped_notification() {
        let events = parse_notification_body(SNS_WRAPPED).expect("valid SNS notification");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].kind, ObjectEventKind::Created);
        assert_eq!(events[0].key, "events/part.parquet");
    }

    #[test]
    fn parse_eventbridge_created_and_deleted() {
        let created =
            parse_notification_body(EVENTBRIDGE_CREATED).expect("valid EventBridge create");
        assert_eq!(created[0].kind, ObjectEventKind::Created);
        assert_eq!(created[0].key, "events/eb.parquet");

        let deleted =
            parse_notification_body(EVENTBRIDGE_DELETED).expect("valid EventBridge delete");
        assert_eq!(deleted[0].kind, ObjectEventKind::Removed);
        assert_eq!(deleted[0].key, "events/gone.parquet");
    }

    #[test]
    fn parse_s3_test_event_is_empty() {
        let events = parse_notification_body(TEST_EVENT).expect("test event is valid");
        assert!(events.is_empty());
    }

    #[test]
    fn parse_rejects_unrecognized_json() {
        let err = parse_notification_body(r#"{"hello":"world"}"#)
            .expect_err("unrecognized JSON must fail closed");
        assert!(matches!(err, ParseError::Unrecognized { .. }));
    }

    #[test]
    fn parse_rejects_invalid_json() {
        let err = parse_notification_body("not-json").expect_err("invalid JSON must fail");
        assert!(matches!(err, ParseError::InvalidJson { .. }));
    }

    #[test]
    fn prefix_filter_matches_nested_keys_only() {
        let event = S3ObjectEvent {
            event_name: "ObjectCreated:Put".into(),
            kind: ObjectEventKind::Created,
            bucket: "my-bucket".into(),
            key: "events/2026/a.parquet".into(),
        };
        assert!(matches_dataset(&event, "my-bucket", "events/"));
        assert!(!matches_dataset(&event, "other-bucket", "events/"));
        assert!(!matches_dataset(&event, "my-bucket", "other/"));
        assert!(matches_dataset(&event, "my-bucket", ""));
        assert!(
            !matches_dataset(
                &S3ObjectEvent {
                    event_name: "ObjectCreated:Put".into(),
                    kind: ObjectEventKind::Created,
                    bucket: "my-bucket".into(),
                    key: "events".into(),
                },
                "my-bucket",
                "events/"
            ),
            "object key `events` is not under prefix `events/`"
        );
    }

    #[test]
    fn parse_eventbridge_rejects_missing_detail_type() {
        let err = parse_notification_body(
            r#"{
                "source": "aws.s3",
                "detail": {
                    "bucket": {"name": "my-bucket"},
                    "object": {"key": "events/x.parquet"}
                }
            }"#,
        )
        .expect_err("missing detail-type must fail closed");
        assert!(matches!(err, ParseError::Unrecognized { .. }));
        assert!(
            err.to_string().contains("detail-type"),
            "error must name the missing field, got: {err}"
        );
    }

    #[test]
    fn parse_eventbridge_preserves_literal_plus_in_key() {
        let events = parse_notification_body(
            r#"{
                "source": "aws.s3",
                "detail-type": "Object Created",
                "detail": {
                    "bucket": {"name": "my-bucket"},
                    "object": {"key": "events/data+file.parquet"}
                }
            }"#,
        )
        .expect("valid EventBridge notification");
        assert_eq!(events[0].key, "events/data+file.parquet");
    }

    #[test]
    fn object_removed_delete_is_removed() {
        assert_eq!(event_kind("ObjectRemoved:Delete"), ObjectEventKind::Removed);
        assert_eq!(
            event_kind("ObjectRemoved:DeleteMarkerCreated"),
            ObjectEventKind::Removed
        );
        assert_eq!(event_kind("s3:ObjectCreated:*"), ObjectEventKind::Created);
    }

    #[test]
    fn s3_object_from_encodes_spaces_and_preserves_key_path() {
        let uri = s3_object_from("my-bucket", "events/data file.parquet")
            .expect("bucket is a valid URL host");
        assert_eq!(uri, "s3://my-bucket/events/data%20file.parquet");
        assert_eq!(
            s3_object_from("my-bucket", "events/a.parquet").expect("valid"),
            "s3://my-bucket/events/a.parquet"
        );
    }
}
