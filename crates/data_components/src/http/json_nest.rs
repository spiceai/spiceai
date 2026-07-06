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

//! JSON schema decomposition for the HTTP connector.
//!
//! Mirrors the `DynamoDB` connector's `JsonNesting` feature so that users
//! can declare a set of top-level static columns plus one catch-all JSON
//! column via the spicepod `columns:` syntax:
//!
//! ```yaml
//! datasets:
//!   - from: https://api.tvmaze.com/shows
//!     name: tvmaze_shows
//!     columns:
//!       - name: id
//!       - name: name
//!       - name: premiered
//!       - name: details
//!         metadata:
//!           json_object: "*"
//! ```
//!
//! Each JSON row returned by the HTTP endpoint is decomposed: every
//! declared static field is projected as a top-level `Utf8` column, and
//! all remaining keys are serialized into a JSON object string stored in
//! the catch-all column.
//!
//! In addition, declared columns whose names match one of the HTTP
//! connector's built-in metadata fields (`request_path`,
//! `request_query`, `request_body`, `request_headers`, `content`,
//! `response_status`, `response_headers`, `_fetched_at`) are *passed
//! through* from the HTTP request/response rather than being decomposed
//! from the JSON body. This lets queries reference both decomposed
//! columns and the original HTTP metadata (e.g. for direct fetches via
//! filter pushdown on `request_path`).

use crate::schema_projection::SchemaProjection;
use snafu::{ResultExt, Snafu};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to serialize catch-all JSON column: {source}"))]
    JsonSerialize { source: serde_json::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Configuration for decomposing a JSON response row into a set of
/// user-declared static columns plus a single catch-all JSON column.
///
/// Static fields are projected into their own top-level columns; every
/// other JSON key in the response row is gathered into a sorted JSON
/// object and stored (as a string) in the `json_field_name` column.
#[derive(Debug, Clone)]
pub struct HttpJsonNesting {
    /// Column order exactly as declared in the spicepod, used to build
    /// the table schema. Includes the catch-all column in its declared
    /// position.
    pub column_order: Vec<String>,
    /// Set of declared columns sourced from HTTP request/response
    /// metadata rather than from the JSON body. Names must match
    /// fields in [`HttpTableProvider::base_table_schema`].
    ///
    /// [`HttpTableProvider::base_table_schema`]: super::provider::HttpTableProvider::base_table_schema
    pub metadata_fields: HashSet<String>,
    /// Connector-agnostic projection (kept fields + catch-all) that performs the
    /// actual object decomposition via [`SchemaProjection::project_row`]. This
    /// is the shared core used by every nesting-capable connector; HTTP only
    /// adds the string-parsing, raw-text-fallback, and metadata-field handling
    /// around it. The static-field set and catch-all name are read back from it
    /// via [`Self::static_fields`] / [`Self::json_field_name`].
    projection: SchemaProjection,
}

impl HttpJsonNesting {
    /// Build a new nesting configuration from the declared column order,
    /// the name of the catch-all column, and the set of declared columns
    /// that should be sourced from HTTP metadata rather than the JSON
    /// body. The catch-all column name must appear in `column_order`.
    #[must_use]
    pub fn new(
        column_order: Vec<String>,
        json_field_name: String,
        metadata_fields: HashSet<String>,
    ) -> Self {
        let static_fields: Vec<String> = column_order
            .iter()
            .filter(|c| {
                c.as_str() != json_field_name.as_str() && !metadata_fields.contains(c.as_str())
            })
            .cloned()
            .collect();
        let projection = SchemaProjection::nesting(static_fields, json_field_name);
        Self {
            column_order,
            metadata_fields,
            projection,
        }
    }

    /// Declared static (kept-as-is) field names. Read from the shared
    /// projection so there is a single source of truth.
    #[must_use]
    pub fn static_fields(&self) -> &HashSet<String> {
        self.projection.static_fields()
    }

    /// Name of the catch-all JSON column. Read from the shared projection.
    #[must_use]
    pub fn json_field_name(&self) -> &str {
        self.projection.catch_all_name().unwrap_or_default()
    }
}

/// Decomposed representation of a single HTTP JSON response row.
///
/// Keys are column names. `None` values represent SQL `NULL`.
pub type DecomposedRow = HashMap<String, Option<String>>;

/// Decompose a single JSON row string according to the nesting
/// configuration. See [`HttpJsonNesting`] for the semantics.
///
/// Behavior for non-object JSON rows (arrays, primitives): the entire
/// row is placed into the catch-all column and every declared static
/// field resolves to `NULL`. This preserves data without silently
/// dropping values.
///
/// Behavior for rows that are *not valid JSON at all* (empty bodies,
/// HTML error pages, plain text): the row is preserved losslessly
/// rather than failing the query — every declared static field resolves
/// to `NULL`, and the catch-all column receives the raw row text encoded
/// as a JSON string (or `NULL` when the body is empty/whitespace-only).
/// This mirrors the non-nested code path ([`HttpExec::parse_content`]),
/// which already returns a non-JSON body as a single raw `content` row.
/// Hard-erroring here previously crashed any `SELECT` against an HTTP
/// dataset that declared `columns:` whenever the endpoint returned a
/// non-JSON body (e.g. fetching a base URL with no path) — see
/// <https://github.com/spiceai/spiceai/issues/11155>.
///
/// [`HttpExec::parse_content`]: super::provider::HttpExec
pub fn decompose_json_row(json_row: &str, nesting: &HttpJsonNesting) -> Result<DecomposedRow> {
    let mut out: DecomposedRow = HashMap::new();
    let Ok(mut value) = serde_json::from_str::<serde_json::Value>(json_row) else {
        // Not valid JSON: preserve the raw row instead of failing the
        // whole query. All declared static fields are NULL; the
        // catch-all keeps the raw text as a JSON string (NULL when the
        // body is empty/whitespace).
        for name in nesting.static_fields() {
            out.insert(name.clone(), None);
        }
        let catchall = if json_row.trim().is_empty() {
            None
        } else {
            Some(
                serde_json::to_string(&serde_json::Value::String(json_row.to_string()))
                    .context(JsonSerializeSnafu)?,
            )
        };
        out.insert(nesting.json_field_name().to_string(), catchall);
        return Ok(out);
    };

    // Body keys colliding with HTTP metadata names are dropped here: the
    // metadata column is populated from the actual HTTP request/response, not
    // from the body, and must not leak into the catch-all.
    if let serde_json::Value::Object(map) = &mut value {
        map.retain(|k, _| !nesting.metadata_fields.contains(k));
    }

    // Delegate the static/catch-all partition + sorted-JSON serialization to the
    // shared connector-agnostic core (`RowShape for serde_json::Value`).
    let projected = nesting.projection.project_row(value);

    let mut obj = match projected {
        serde_json::Value::Object(map) => map,
        // `project_row` always returns an object; this arm is unreachable.
        _ => serde_json::Map::new(),
    };

    // Materialize one `Option<String>` per declared column. Absent declared
    // fields (and an empty catch-all) become SQL NULL.
    let mut out: DecomposedRow = HashMap::with_capacity(nesting.static_fields().len() + 1);
    for name in nesting.static_fields() {
        let value = obj.remove(name).and_then(json_value_to_string);
        out.insert(name.clone(), value);
    }
    let catchall = obj
        .remove(nesting.json_field_name())
        .and_then(json_value_to_string);
    out.insert(nesting.json_field_name().to_string(), catchall);

    Ok(out)
}

/// Convert a JSON value to the string representation used for static
/// columns. JSON strings are emitted verbatim (no surrounding quotes);
/// objects and arrays are re-serialized to JSON text; `null` maps to
/// SQL `NULL`.
fn json_value_to_string(v: serde_json::Value) -> Option<String> {
    match v {
        serde_json::Value::Null => None,
        serde_json::Value::String(s) => Some(s),
        other => Some(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn nesting(cols: &[&str], json_field: &str) -> HttpJsonNesting {
        HttpJsonNesting::new(
            cols.iter().map(|s| (*s).to_string()).collect(),
            json_field.to_string(),
            HashSet::new(),
        )
    }

    fn nesting_with_meta(cols: &[&str], json_field: &str, meta: &[&str]) -> HttpJsonNesting {
        HttpJsonNesting::new(
            cols.iter().map(|s| (*s).to_string()).collect(),
            json_field.to_string(),
            meta.iter().map(|s| (*s).to_string()).collect(),
        )
    }

    #[test]
    fn decomposes_object_into_static_and_catchall() {
        let n = nesting(&["id", "title", "data"], "data");
        let row = json!({
            "id": "abc",
            "title": "hello",
            "description": "a value",
            "count": 42,
            "nested": {"x": 1, "y": [1, 2]}
        })
        .to_string();

        let d = decompose_json_row(&row, &n).expect("decompose");

        assert_eq!(d.get("id").expect("id").as_deref(), Some("abc"));
        assert_eq!(d.get("title").expect("title").as_deref(), Some("hello"));

        let catchall = d.get("data").expect("data").as_deref().expect("catchall");
        let parsed: serde_json::Value = serde_json::from_str(catchall).expect("parse catchall");
        assert_eq!(parsed["description"], "a value");
        assert_eq!(parsed["count"], 42);
        assert_eq!(parsed["nested"]["x"], 1);
    }

    #[test]
    fn missing_static_field_is_null() {
        let n = nesting(&["id", "title", "data"], "data");
        let row = json!({"id": "abc"}).to_string();
        let d = decompose_json_row(&row, &n).expect("decompose");
        assert_eq!(d.get("id").expect("id").as_deref(), Some("abc"));
        assert!(d.get("title").expect("title").is_none());
        // no extra keys => catch-all is NULL
        assert!(d.get("data").expect("data").is_none());
    }

    #[test]
    fn complex_static_field_serialized_as_json() {
        let n = nesting(&["payload", "data"], "data");
        let row = json!({"payload": {"a": 1}, "extra": "e"}).to_string();
        let d = decompose_json_row(&row, &n).expect("decompose");
        let payload = d.get("payload").expect("payload").as_deref().expect("val");
        let parsed: serde_json::Value = serde_json::from_str(payload).expect("parse");
        assert_eq!(parsed["a"], 1);
        assert!(d.get("data").expect("data").is_some());
    }

    #[test]
    fn non_object_row_goes_to_catchall() {
        let n = nesting(&["id", "data"], "data");
        let row = json!([1, 2, 3]).to_string();
        let d = decompose_json_row(&row, &n).expect("decompose");
        assert!(d.get("id").expect("id").is_none());
        assert_eq!(d.get("data").expect("data").as_deref(), Some("[1,2,3]"));
    }

    #[test]
    fn catchall_keys_are_sorted() {
        let n = nesting(&["id", "data"], "data");
        let row = json!({"id": "x", "zeta": 1, "alpha": 2, "mu": 3}).to_string();
        let d = decompose_json_row(&row, &n).expect("decompose");
        let catchall = d.get("data").expect("data").as_deref().expect("val");
        // BTreeMap ordering => keys alphabetical
        assert_eq!(catchall, r#"{"alpha":2,"mu":3,"zeta":1}"#);
    }

    #[test]
    fn non_json_row_is_preserved_in_catchall() {
        // Regression for https://github.com/spiceai/spiceai/issues/11155:
        // a non-JSON HTTP body (e.g. an HTML error page returned when the
        // base URL is fetched with no path) must not crash the query.
        let n = nesting(&["id", "title", "data"], "data");
        let row = "<!DOCTYPE html><html><body>not json</body></html>";
        let d = decompose_json_row(row, &n).expect("non-JSON row must not error");
        assert!(d.get("id").expect("id present").is_none());
        assert!(d.get("title").expect("title present").is_none());
        let catchall = d
            .get("data")
            .expect("data present")
            .as_deref()
            .expect("catchall not null");
        // The raw text is preserved as a JSON string, so the catch-all
        // column always contains valid JSON.
        let parsed: serde_json::Value =
            serde_json::from_str(catchall).expect("catchall is valid JSON");
        assert_eq!(parsed, serde_json::Value::String(row.to_string()));
    }

    #[test]
    fn empty_row_yields_all_null() {
        // An empty HTTP body (e.g. a 5xx with no content) decomposes to a
        // single all-NULL row rather than erroring.
        let n = nesting(&["id", "data"], "data");
        for row in ["", "   ", "\n\t "] {
            let d = decompose_json_row(row, &n).expect("empty row must not error");
            assert!(d.get("id").expect("id present").is_none());
            assert!(
                d.get("data").expect("data present").is_none(),
                "empty body => catch-all NULL, got {:?}",
                d.get("data")
            );
        }
    }

    #[test]
    fn malformed_json_is_preserved_not_dropped() {
        // Truncated/garbage JSON is preserved verbatim, not silently
        // discarded.
        let n = nesting(&["id", "data"], "data");
        let row = r#"{"id": "abc", "#; // truncated, invalid JSON
        let d = decompose_json_row(row, &n).expect("malformed JSON must not error");
        assert!(d.get("id").expect("id present").is_none());
        let catchall = d
            .get("data")
            .expect("data present")
            .as_deref()
            .expect("catchall not null");
        let parsed: serde_json::Value =
            serde_json::from_str(catchall).expect("catchall is valid JSON");
        assert_eq!(parsed, serde_json::Value::String(row.to_string()));
    }

    #[test]
    fn null_static_field_stays_null() {
        let n = nesting(&["id", "data"], "data");
        let row = json!({"id": null, "extra": 1}).to_string();
        let d = decompose_json_row(&row, &n).expect("decompose");
        assert!(d.get("id").expect("id").is_none());
    }

    #[test]
    fn metadata_field_is_not_extracted_from_body() {
        // `request_path` is declared as a metadata column. Even if the
        // body contains a `request_path` key, it must not appear in the
        // decomposed output (the metadata column is filled separately
        // by the batch builder) and must not leak into the catch-all.
        let n = nesting_with_meta(&["request_path", "id", "data"], "data", &["request_path"]);
        let row = json!({
            "request_path": "/should-be-ignored",
            "id": "abc",
            "extra": 1
        })
        .to_string();
        let d = decompose_json_row(&row, &n).expect("decompose");
        assert!(
            !d.contains_key("request_path"),
            "metadata field must not be populated from body"
        );
        assert_eq!(d.get("id").expect("id").as_deref(), Some("abc"));
        let catchall = d.get("data").expect("data").as_deref().expect("val");
        let parsed: serde_json::Value = serde_json::from_str(catchall).expect("parse");
        assert!(
            parsed.get("request_path").is_none(),
            "metadata field must not leak into catch-all"
        );
        assert_eq!(parsed["extra"], 1);
    }
}
