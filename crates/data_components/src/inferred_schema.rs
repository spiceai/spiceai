/*
Copyright 2026 The Spice.ai OSS Authors

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

//! Connector-agnostic contract for **extended schema inference**.
//!
//! Base column/type inference is always performed by every connector. When a
//! dataset opts into extended inference (`schema_inference: extended`), a connector
//! may additionally discover the source table's primary key, secondary indexes, and
//! sort/clustering order, and emit them as JSON in the Arrow schema metadata via
//! [`InferredSchema::to_metadata`]. The runtime reads them back with
//! [`InferredSchema::from_metadata`] and fills any acceleration settings the user
//! left unspecified.
//!
//! Keeping the (de)serialization in one place ensures the producer (connector) and
//! the consumer (runtime) always agree on the wire format.

use std::collections::HashMap;

use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::{
    INFERRED_INDEXES_METADATA_KEY, INFERRED_PRIMARY_KEY_METADATA_KEY,
    INFERRED_ROW_COUNT_METADATA_KEY, INFERRED_SORT_COLUMNS_METADATA_KEY,
    INFERRED_TABLE_BYTES_METADATA_KEY,
};

/// Every Arrow schema-metadata key written by [`InferredSchema::to_metadata`].
///
/// Centralized so the inference hints can be located — and removed from a query
/// scan's output schema — in one place; see [`strip_inferred_metadata`].
pub const INFERRED_METADATA_KEYS: [&str; 5] = [
    INFERRED_PRIMARY_KEY_METADATA_KEY,
    INFERRED_INDEXES_METADATA_KEY,
    INFERRED_SORT_COLUMNS_METADATA_KEY,
    INFERRED_ROW_COUNT_METADATA_KEY,
    INFERRED_TABLE_BYTES_METADATA_KEY,
];

/// Remove every extended-inference hint ([`INFERRED_METADATA_KEYS`]) from a
/// schema-metadata map, in place.
///
/// These hints stay useful on a table provider's *advertised* schema: the runtime
/// surfaces the inferred row-count / byte-size as table statistics (see
/// `MetadataEnrichedTableProvider`) and seeds the accelerator's tuning warm-start
/// from them. They must not, however, ride on a query scan's *output* schema.
/// Their values vary per table, and DataFusion builds a join's output schema by
/// merging its inputs' schema-level metadata in input order; when the
/// `join_selection` physical-optimizer rule swaps a hash join's build/probe
/// sides, that merge order flips and the surviving values change, so the rule's
/// output schema no longer equals its input schema and the schema-invariant
/// check fails. Strip them from the scan schema (only) to keep join schemas
/// stable while leaving the statistics/tuning consumers untouched.
pub fn strip_inferred_metadata(metadata: &mut HashMap<String, String>) {
    for key in INFERRED_METADATA_KEYS {
        metadata.remove(key);
    }
}

/// A secondary index inferred from the source table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InferredIndex {
    /// Index columns, in index order.
    pub columns: Vec<String>,
    /// Whether the index enforces uniqueness.
    pub unique: bool,
}

/// A sort/clustering column inferred from the source table, with direction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InferredSortColumn {
    /// Column name.
    pub column: String,
    /// Whether the column sorts descending. Defaults to ascending.
    #[serde(default)]
    pub desc: bool,
}

/// Deeper schema details inferred from a source table, used to fill unspecified
/// acceleration settings.
///
/// Every field is optional: a connector emits only what it could discover, and the
/// runtime only fills settings the user left unset.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct InferredSchema {
    /// Primary-key columns, in key order. Empty when none was inferred.
    pub primary_key: Vec<String>,
    /// Secondary indexes (excludes the primary key's own index).
    pub indexes: Vec<InferredIndex>,
    /// Sort/clustering columns, in sort order.
    pub sort_columns: Vec<InferredSortColumn>,
    /// Rough estimated row count from the source catalog (an estimate, not a
    /// precise count). `None` when not inferred.
    pub row_count: Option<u64>,
    /// Rough estimated table data byte size from the source catalog. `None` when
    /// not inferred.
    pub table_bytes: Option<u64>,
}

impl InferredSchema {
    /// Whether nothing was inferred.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.primary_key.is_empty()
            && self.indexes.is_empty()
            && self.sort_columns.is_empty()
            && self.row_count.is_none()
            && self.table_bytes.is_none()
    }

    /// Serialize the non-empty components into Arrow schema-metadata entries.
    ///
    /// Returns the entries to merge into the table provider's schema metadata.
    /// A component that fails to serialize is skipped with a warning rather than
    /// failing the whole table.
    #[must_use]
    pub fn to_metadata(&self) -> HashMap<String, String> {
        let mut metadata = HashMap::new();

        if !self.primary_key.is_empty() {
            insert_json(
                &mut metadata,
                INFERRED_PRIMARY_KEY_METADATA_KEY,
                &self.primary_key,
            );
        }
        if !self.indexes.is_empty() {
            insert_json(&mut metadata, INFERRED_INDEXES_METADATA_KEY, &self.indexes);
        }
        if !self.sort_columns.is_empty() {
            insert_json(
                &mut metadata,
                INFERRED_SORT_COLUMNS_METADATA_KEY,
                &self.sort_columns,
            );
        }
        if let Some(row_count) = self.row_count {
            metadata.insert(
                INFERRED_ROW_COUNT_METADATA_KEY.to_string(),
                row_count.to_string(),
            );
        }
        if let Some(table_bytes) = self.table_bytes {
            metadata.insert(
                INFERRED_TABLE_BYTES_METADATA_KEY.to_string(),
                table_bytes.to_string(),
            );
        }

        metadata
    }

    /// Parse inferred-schema components out of Arrow schema metadata.
    ///
    /// Missing keys yield empty collections; malformed values are skipped with a
    /// warning. Always succeeds.
    #[must_use]
    pub fn from_metadata(metadata: &HashMap<String, String>) -> Self {
        Self {
            primary_key: metadata
                .get(INFERRED_PRIMARY_KEY_METADATA_KEY)
                .and_then(|raw| parse_json_or_warn(raw, INFERRED_PRIMARY_KEY_METADATA_KEY))
                .unwrap_or_default(),
            indexes: metadata
                .get(INFERRED_INDEXES_METADATA_KEY)
                .and_then(|raw| parse_json_or_warn(raw, INFERRED_INDEXES_METADATA_KEY))
                .unwrap_or_default(),
            sort_columns: metadata
                .get(INFERRED_SORT_COLUMNS_METADATA_KEY)
                .and_then(|raw| parse_json_or_warn(raw, INFERRED_SORT_COLUMNS_METADATA_KEY))
                .unwrap_or_default(),
            row_count: metadata
                .get(INFERRED_ROW_COUNT_METADATA_KEY)
                .and_then(|raw| raw.parse().ok()),
            table_bytes: metadata
                .get(INFERRED_TABLE_BYTES_METADATA_KEY)
                .and_then(|raw| raw.parse().ok()),
        }
    }
}

fn insert_json<T: Serialize>(metadata: &mut HashMap<String, String>, key: &str, value: &T) {
    match serde_json::to_string(value) {
        Ok(json) => {
            metadata.insert(key.to_string(), json);
        }
        Err(error) => {
            tracing::warn!(
                key = %key,
                error = %error,
                "Failed to serialize inferred-schema metadata; omitting"
            );
        }
    }
}

fn parse_json_or_warn<T: DeserializeOwned>(raw: &str, key: &str) -> Option<T> {
    match serde_json::from_str(raw) {
        Ok(value) => Some(value),
        Err(error) => {
            tracing::warn!(
                key = %key,
                error = %error,
                "Failed to parse inferred-schema metadata; ignoring"
            );
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> InferredSchema {
        InferredSchema {
            primary_key: vec!["tenant_id".to_string(), "id".to_string()],
            indexes: vec![
                InferredIndex {
                    columns: vec!["email".to_string()],
                    unique: true,
                },
                InferredIndex {
                    columns: vec!["created_at".to_string(), "region".to_string()],
                    unique: false,
                },
            ],
            sort_columns: vec![
                InferredSortColumn {
                    column: "created_at".to_string(),
                    desc: true,
                },
                InferredSortColumn {
                    column: "id".to_string(),
                    desc: false,
                },
            ],
            row_count: Some(123_456),
            table_bytes: Some(7_890_123),
        }
    }

    #[test]
    fn round_trips_through_metadata() {
        let original = sample();
        let metadata = original.to_metadata();
        // All keys present.
        assert!(metadata.contains_key(INFERRED_PRIMARY_KEY_METADATA_KEY));
        assert!(metadata.contains_key(INFERRED_INDEXES_METADATA_KEY));
        assert!(metadata.contains_key(INFERRED_SORT_COLUMNS_METADATA_KEY));
        assert_eq!(
            metadata
                .get(INFERRED_ROW_COUNT_METADATA_KEY)
                .map(String::as_str),
            Some("123456")
        );
        assert_eq!(
            metadata
                .get(INFERRED_TABLE_BYTES_METADATA_KEY)
                .map(String::as_str),
            Some("7890123")
        );

        let parsed = InferredSchema::from_metadata(&metadata);
        assert_eq!(parsed, original);
    }

    #[test]
    fn empty_schema_emits_no_metadata() {
        let empty = InferredSchema::default();
        assert!(empty.is_empty());
        assert!(empty.to_metadata().is_empty());
    }

    #[test]
    fn missing_keys_yield_empty() {
        let parsed = InferredSchema::from_metadata(&HashMap::new());
        assert!(parsed.is_empty());
    }

    #[test]
    fn malformed_values_are_skipped() {
        let mut metadata = HashMap::new();
        metadata.insert(
            INFERRED_PRIMARY_KEY_METADATA_KEY.to_string(),
            "not json".to_string(),
        );
        metadata.insert(
            INFERRED_INDEXES_METADATA_KEY.to_string(),
            serde_json::to_string(&sample().indexes).expect("serialize"),
        );
        let parsed = InferredSchema::from_metadata(&metadata);
        // Bad PK ignored, good indexes preserved.
        assert!(parsed.primary_key.is_empty());
        assert_eq!(parsed.indexes, sample().indexes);
    }

    #[test]
    fn strip_inferred_metadata_removes_only_inferred_keys() {
        // Every key `to_metadata` can emit, plus unrelated schema-level metadata.
        let mut metadata = sample().to_metadata();
        metadata.insert("spice.accelerator".to_string(), "cayenne".to_string());
        metadata.insert("description".to_string(), "orders".to_string());

        strip_inferred_metadata(&mut metadata);

        for key in INFERRED_METADATA_KEYS {
            assert!(!metadata.contains_key(key), "{key} should be stripped");
        }
        // Unrelated schema-level metadata is preserved.
        assert_eq!(
            metadata.get("spice.accelerator").map(String::as_str),
            Some("cayenne")
        );
        assert_eq!(
            metadata.get("description").map(String::as_str),
            Some("orders")
        );
    }

    #[test]
    fn strip_inferred_metadata_is_a_noop_when_absent() {
        let mut metadata = HashMap::new();
        metadata.insert("spice.accelerator".to_string(), "cayenne".to_string());

        strip_inferred_metadata(&mut metadata);

        assert_eq!(metadata.len(), 1);
        assert_eq!(
            metadata.get("spice.accelerator").map(String::as_str),
            Some("cayenne")
        );
    }

    #[test]
    fn sort_column_defaults_to_ascending() {
        // `desc` omitted in the JSON should deserialize to false.
        let raw = r#"[{"column":"id"}]"#;
        let parsed: Vec<InferredSortColumn> = serde_json::from_str(raw).expect("parse");
        assert_eq!(
            parsed,
            vec![InferredSortColumn {
                column: "id".to_string(),
                desc: false,
            }]
        );
    }
}
