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

use snafu::{ResultExt, Snafu};
use std::collections::{BTreeMap, HashMap, HashSet};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to parse HTTP response row as JSON: {source}"))]
    JsonParse { source: serde_json::Error },

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
    /// the table schema. Includes `json_field_name` in its declared
    /// position.
    pub column_order: Vec<String>,
    /// Set of declared static field names (i.e. `column_order` minus
    /// `json_field_name`).
    pub static_fields: HashSet<String>,
    /// Name of the catch-all JSON column.
    pub json_field_name: String,
}

impl HttpJsonNesting {
    /// Build a new nesting configuration from the declared column order
    /// and the name of the catch-all column. The catch-all column name
    /// must appear in `column_order`.
    #[must_use]
    pub fn new(column_order: Vec<String>, json_field_name: String) -> Self {
        let static_fields: HashSet<String> = column_order
            .iter()
            .filter(|c| **c != json_field_name)
            .cloned()
            .collect();
        Self {
            column_order,
            static_fields,
            json_field_name,
        }
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
pub fn decompose_json_row(json_row: &str, nesting: &HttpJsonNesting) -> Result<DecomposedRow> {
    let value: serde_json::Value = serde_json::from_str(json_row).context(JsonParseSnafu)?;

    let mut out: DecomposedRow = HashMap::new();

    match value {
        serde_json::Value::Object(map) => {
            // Use BTreeMap so the serialized catch-all has deterministic,
            // sorted keys (matches DynamoDB's `json_nest` behavior).
            let mut catchall: BTreeMap<String, serde_json::Value> = BTreeMap::new();

            for (k, v) in map {
                if nesting.static_fields.contains(&k) {
                    out.insert(k, json_value_to_string(v));
                } else {
                    catchall.insert(k, v);
                }
            }

            // Any declared static field that was absent from the row
            // becomes explicit NULL rather than being missing from the
            // batch.
            for name in &nesting.static_fields {
                out.entry(name.clone()).or_insert(None);
            }

            let catchall_str = if catchall.is_empty() {
                None
            } else {
                Some(serde_json::to_string(&catchall).context(JsonSerializeSnafu)?)
            };
            out.insert(nesting.json_field_name.clone(), catchall_str);
        }
        other => {
            // Non-object row: preserve it in the catch-all column so no
            // data is lost. Static fields are NULL.
            for name in &nesting.static_fields {
                out.insert(name.clone(), None);
            }
            out.insert(
                nesting.json_field_name.clone(),
                Some(serde_json::to_string(&other).context(JsonSerializeSnafu)?),
            );
        }
    }

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
    fn null_static_field_stays_null() {
        let n = nesting(&["id", "data"], "data");
        let row = json!({"id": null, "extra": 1}).to_string();
        let d = decompose_json_row(&row, &n).expect("decompose");
        assert!(d.get("id").expect("id").is_none());
    }
}
