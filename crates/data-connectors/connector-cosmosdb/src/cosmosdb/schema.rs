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

//! Schema inference for Cosmos DB documents.
//!
//! Cosmos DB `NoSQL` does not expose a static schema. The first `N` documents
//! returned from the configured query are sampled and handed to Arrow's
//! [`infer_json_schema_from_iterator`] to derive a best-effort Arrow schema.
//!
//! Cosmos always stamps every document with `_rid`, `_self`, `_etag`,
//! `_attachments`, and `_ts` system fields. These are stripped from the
//! sample set before inference to avoid polluting downstream tables with
//! metadata columns the user almost never wants to query.

use arrow::datatypes::{Schema, SchemaRef};
use arrow::json::reader::infer_json_schema_from_iterator;
use serde_json::Value;
use snafu::ResultExt;
use std::sync::Arc;

use super::{Error, SchemaInferenceSnafu};

/// System fields stamped on every Cosmos document. Stripped prior to schema
/// inference so they never become user-visible columns.
const COSMOS_SYSTEM_FIELDS: &[&str] = &["_rid", "_self", "_etag", "_attachments", "_ts"];

/// Strip Cosmos DB-internal system fields from a top-level JSON object. Any
/// non-object value is returned unchanged.
#[must_use]
pub fn strip_system_fields(value: Value) -> Value {
    match value {
        Value::Object(mut map) => {
            for field in COSMOS_SYSTEM_FIELDS {
                map.remove(*field);
            }
            Value::Object(map)
        }
        other => other,
    }
}

/// Infer an Arrow schema from a slice of sampled Cosmos documents. Callers
/// are expected to have already run the values through [`strip_system_fields`].
///
/// Returns an empty [`Schema`] when `samples` is empty — callers should map
/// that condition to a user-facing `EmptyContainer` error rather than letting
/// it propagate as a successful inference.
///
/// # Errors
/// Returns an error if Arrow's JSON schema inference fails.
pub fn infer_schema(samples: &[Value]) -> Result<SchemaRef, Error> {
    if samples.is_empty() {
        return Ok(Arc::new(Schema::empty()));
    }

    let schema = infer_json_schema_from_iterator(
        samples
            .iter()
            .map(Result::<_, arrow::error::ArrowError>::Ok),
    )
    .context(SchemaInferenceSnafu)?;

    Ok(Arc::new(schema))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;
    use serde_json::json;

    #[test]
    fn strip_system_fields_removes_all_known_system_fields() {
        let doc = json!({
            "_rid": "rid",
            "_self": "self",
            "_etag": "etag",
            "_attachments": "attachments",
            "_ts": 1234,
            "id": "doc1",
            "payload": "keep me",
        });
        let stripped = strip_system_fields(doc);
        let obj = stripped
            .as_object()
            .expect("result should be a JSON object");
        assert!(!obj.contains_key("_rid"));
        assert!(!obj.contains_key("_self"));
        assert!(!obj.contains_key("_etag"));
        assert!(!obj.contains_key("_attachments"));
        assert!(!obj.contains_key("_ts"));
        assert_eq!(obj.get("id").and_then(Value::as_str), Some("doc1"));
        assert_eq!(obj.get("payload").and_then(Value::as_str), Some("keep me"));
    }

    #[test]
    fn strip_system_fields_ignores_non_object_values() {
        assert_eq!(strip_system_fields(json!(null)), json!(null));
        assert_eq!(strip_system_fields(json!("string")), json!("string"));
        assert_eq!(strip_system_fields(json!(42)), json!(42));
        assert_eq!(strip_system_fields(json!([1, 2, 3])), json!([1, 2, 3]));
    }

    #[test]
    fn strip_system_fields_does_not_strip_nested_occurrences() {
        // System field stripping only applies at the top level — nested
        // objects (user-controlled payloads) are preserved unchanged.
        let doc = json!({
            "_rid": "top_rid",
            "nested": {"_rid": "nested_rid", "value": 1},
        });
        let stripped = strip_system_fields(doc);
        let obj = stripped
            .as_object()
            .expect("result should be a JSON object");
        assert!(!obj.contains_key("_rid"));
        let nested = obj
            .get("nested")
            .and_then(Value::as_object)
            .expect("nested field should be an object");
        assert_eq!(
            nested.get("_rid").and_then(Value::as_str),
            Some("nested_rid")
        );
    }

    #[test]
    fn infer_schema_returns_empty_schema_for_empty_sample() {
        let schema = infer_schema(&[]).expect("infer_schema on empty input should succeed");
        assert_eq!(schema.fields().len(), 0);
    }

    #[test]
    fn infer_schema_produces_fields_from_sample_documents() {
        let samples = vec![
            json!({"id": "1", "count": 10}),
            json!({"id": "2", "count": 20}),
        ];
        let schema = infer_schema(&samples).expect("infer_schema should succeed");
        let id_field = schema
            .field_with_name("id")
            .expect("schema should have 'id' field");
        let count_field = schema
            .field_with_name("count")
            .expect("schema should have 'count' field");
        assert_eq!(id_field.data_type(), &DataType::Utf8);
        assert!(matches!(
            count_field.data_type(),
            DataType::Int64 | DataType::Float64
        ));
    }

    #[test]
    fn infer_schema_unions_mixed_documents() {
        // Documents with different field sets should merge into a single
        // schema that contains the union of fields.
        let samples = vec![
            json!({"id": "1", "only_in_first": "x"}),
            json!({"id": "2", "only_in_second": 42}),
        ];
        let schema = infer_schema(&samples).expect("infer_schema should succeed");
        schema
            .field_with_name("id")
            .expect("schema should have 'id' field");
        schema
            .field_with_name("only_in_first")
            .expect("schema should have 'only_in_first' field");
        schema
            .field_with_name("only_in_second")
            .expect("schema should have 'only_in_second' field");
    }
}
