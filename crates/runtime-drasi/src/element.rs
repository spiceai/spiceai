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

//! Maps a CDC row onto a Drasi graph node.
//!
//! One row becomes one node: the primary key derives the element id, the
//! configured labels become the node labels, and every column becomes a
//! property.

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow_json::writer::{JsonArray, WriterBuilder};
use percent_encoding::{AsciiSet, utf8_percent_encode};
use serde_json::{Map, Value};

use crate::error::{
    EncodeChangeSnafu, Error, PrimaryKeyColumnMissingSnafu, PrimaryKeyMissingSnafu,
    PrimaryKeyValueNullSnafu, Result,
};
use crate::model::NodeElement;

/// A change operation Drasi can represent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeOp {
    Insert,
    Update,
    Delete,
}

impl ChangeOp {
    /// Maps a Debezium operation code onto a Drasi operation.
    ///
    /// A snapshot read (`r`) becomes an insert: it carries a row's initial state,
    /// which is exactly what Drasi needs to seed the graph.
    ///
    /// # Errors
    ///
    /// Returns the unmapped code for anything Drasi cannot express. Notably `t`
    /// (truncate) has no Drasi equivalent — it deletes every row without naming
    /// them, so silently dropping it would leave Drasi's graph holding rows the
    /// source no longer has.
    pub fn from_op_code(code: &str) -> std::result::Result<Self, &str> {
        match code {
            "c" | "r" => Ok(ChangeOp::Insert),
            "u" => Ok(ChangeOp::Update),
            "d" => Ok(ChangeOp::Delete),
            other => Err(other),
        }
    }

    /// The `op` string used by the platform (Redis) wire format.
    #[must_use]
    pub fn platform_code(self) -> &'static str {
        match self {
            ChangeOp::Insert => "i",
            ChangeOp::Update => "u",
            ChangeOp::Delete => "d",
        }
    }
}

/// How this dataset's rows are named and labelled in the Drasi graph.
#[derive(Debug, Clone)]
pub struct ElementMapping {
    /// Dataset name, for error messages only.
    pub dataset: String,
    /// Node labels. The first also prefixes the element id, following the
    /// `{table}:{key}` convention used by Drasi's own relational sources.
    ///
    /// Shared rather than owned per node: every row in a batch gets the same
    /// labels, so cloning the `Vec` per row would allocate once per row for a
    /// value that never varies.
    pub labels: Arc<[String]>,
}

impl ElementMapping {
    /// The element-id prefix: the first label, or the dataset name when no
    /// label is configured.
    fn id_prefix(&self) -> &str {
        self.labels.first().map_or(&self.dataset, String::as_str)
    }

    /// Builds a mapping from anything that can supply the labels.
    #[must_use]
    pub fn new(dataset: impl Into<String>, labels: impl Into<Arc<[String]>>) -> Self {
        Self {
            dataset: dataset.into(),
            labels: labels.into(),
        }
    }
}

/// Renders every row of `data` as a JSON object, with NULLs written explicitly.
///
/// Explicit nulls matter to Drasi: a null property is a first-class value there,
/// whereas an absent key drops the property entirely — which changes the result
/// of a continuous query testing `WHERE col IS NULL`.
///
/// # Errors
///
/// Returns an error if the batch cannot be rendered as one JSON object per row.
pub fn rows_to_json(dataset: &str, data: &RecordBatch) -> Result<Vec<Map<String, Value>>> {
    let mut writer = WriterBuilder::new()
        .with_explicit_nulls(true)
        .build::<_, JsonArray>(Vec::new());

    writer
        .write_batches(&[data])
        .map_err(|e| encode_failed(dataset, &e))?;
    writer.finish().map_err(|e| encode_failed(dataset, &e))?;

    // `from_slice`, not `from_reader`: the buffer is already in memory, and
    // `from_reader` would wrap it in serde_json's `IoRead`, which pulls the input
    // one byte at a time through an `io::Read` iterator instead of scanning the
    // slice directly.
    let rendered: Vec<Value> =
        serde_json::from_slice(&writer.into_inner()).map_err(|e| encode_failed(dataset, &e))?;

    rendered
        .into_iter()
        .map(|value| match value {
            Value::Object(map) => Ok(map),
            other => EncodeChangeSnafu {
                dataset,
                message: format!(
                    "expected a JSON object per row, found {}",
                    json_type_name(&other)
                ),
            }
            .fail(),
        })
        .collect()
}

fn encode_failed(dataset: &str, source: &dyn std::fmt::Display) -> Error {
    Error::EncodeChange {
        dataset: dataset.to_string(),
        message: source.to_string(),
    }
}

fn json_type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "a boolean",
        Value::Number(_) => "a number",
        Value::String(_) => "a string",
        Value::Array(_) => "an array",
        Value::Object(_) => "an object",
    }
}

/// The string form of a primary-key component.
///
/// A JSON string contributes its contents, not its quoted encoding, so the id
/// derived from a text key reads as the key itself.
fn key_component(dataset: &str, column: &str, value: Option<&Value>) -> Result<String> {
    match value {
        None => PrimaryKeyColumnMissingSnafu { dataset, column }.fail(),
        Some(Value::Null) => PrimaryKeyValueNullSnafu { dataset, column }.fail(),
        Some(Value::String(s)) => Ok(s.clone()),
        Some(other) => Ok(other.to_string()),
    }
}

/// The only characters that would make a composite key ambiguous: the `_`
/// separator, and the `%` that escaping it introduces.
///
/// Deliberately built from [`AsciiSet::EMPTY`] rather than `CONTROLS`, so
/// nothing else in a key value is rewritten and ids stay byte-identical to
/// Drasi's convention wherever that convention is unambiguous.
const KEY_COMPONENT: &AsciiSet = &AsciiSet::EMPTY.add(b'%').add(b'_');

/// Percent-encodes the characters that would make a composite key ambiguous.
///
/// Applied only when the key has more than one component — see [`element_id`].
fn escape_key_component(component: &str) -> String {
    utf8_percent_encode(component, KEY_COMPONENT).to_string()
}

/// Derives the Drasi element id for one row.
///
/// Follows the `{table}:{key}` convention of Drasi's own relational sources, so
/// a continuous query written against a Drasi `PostgreSQL` source matches rows
/// forwarded from here. The table prefix is not decoration: element ids are
/// unique per *source*, not per table, so two tables both keyed `1` would
/// otherwise collide on one id.
///
/// A composite key joins its components with `_`, again matching the
/// convention — but with `%` and `_` percent-encoded inside each component
/// first. Drasi's convention does not escape, which makes it ambiguous:
/// `("a_b", "c")` and `("a", "b_c")` are distinct rows that both render
/// `t:a_b_c`, silently merging two rows into one graph node. Encoding keeps the
/// join injective. It is a no-op unless a component actually contains one of
/// those characters, so ids stay byte-identical to the convention in every case
/// where the convention is unambiguous — including every single-column key,
/// which needs no separator and is therefore never escaped.
///
/// # Errors
///
/// Returns an error if the row carries no primary key, or if a key column is
/// absent or NULL — none of which can name a row stably.
pub fn element_id(
    mapping: &ElementMapping,
    primary_key_columns: &[&str],
    row: &Map<String, Value>,
) -> Result<String> {
    let prefix = mapping.id_prefix();

    match primary_key_columns {
        [] => PrimaryKeyMissingSnafu {
            dataset: &mapping.dataset,
        }
        .fail(),
        [only] => {
            let value = key_component(&mapping.dataset, only, row.get(*only))?;
            Ok(format!("{prefix}:{value}"))
        }
        columns => {
            let mut parts = Vec::with_capacity(columns.len());
            for column in columns {
                let value = key_component(&mapping.dataset, column, row.get(*column))?;
                parts.push(escape_key_component(&value));
            }
            Ok(format!("{prefix}:{}", parts.join("_")))
        }
    }
}

/// Builds the node for one row.
///
/// # Errors
///
/// Returns an error if no stable element id can be derived from the row's
/// primary key.
pub fn node_element(
    mapping: &ElementMapping,
    primary_key_columns: &[&str],
    row: Map<String, Value>,
) -> Result<NodeElement> {
    let id = element_id(mapping, primary_key_columns, &row)?;
    Ok(NodeElement {
        id,
        // Shared, not copied: a batch is one dataset, so every node in it
        // carries the same labels.
        labels: Arc::clone(&mapping.labels),
        properties: row,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn mapping() -> ElementMapping {
        ElementMapping::new("orders".to_string(), vec!["public.orders".to_string()])
    }

    fn row(pairs: &[(&str, Value)]) -> Map<String, Value> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), v.clone()))
            .collect()
    }

    #[test]
    fn snapshot_reads_seed_the_graph_as_inserts() {
        assert_eq!(ChangeOp::from_op_code("c"), Ok(ChangeOp::Insert));
        assert_eq!(ChangeOp::from_op_code("r"), Ok(ChangeOp::Insert));
        assert_eq!(ChangeOp::from_op_code("u"), Ok(ChangeOp::Update));
        assert_eq!(ChangeOp::from_op_code("d"), Ok(ChangeOp::Delete));
    }

    /// Truncate deletes every row without naming any, so it cannot be forwarded
    /// as a set of deletes. It must surface rather than be dropped.
    #[test]
    fn truncate_and_unknown_ops_have_no_mapping() {
        assert_eq!(ChangeOp::from_op_code("t"), Err("t"));
        assert_eq!(ChangeOp::from_op_code("m"), Err("m"));
    }

    #[test]
    fn single_column_key_matches_the_drasi_convention() {
        let id =
            element_id(&mapping(), &["id"], &row(&[("id", 12_345.into())])).expect("derives an id");
        assert_eq!(id, "public.orders:12345");
    }

    /// A text key contributes its contents, not its JSON quoting.
    #[test]
    fn string_key_is_unquoted() {
        let id = element_id(&mapping(), &["sku"], &row(&[("sku", "ABC-1".into())]))
            .expect("derives an id");
        assert_eq!(id, "public.orders:ABC-1");
    }

    /// A single-column key is never escaped, so an underscore in the value
    /// survives verbatim and stays byte-identical to Drasi's convention.
    #[test]
    fn single_column_key_is_never_escaped() {
        let id = element_id(&mapping(), &["sku"], &row(&[("sku", "a_b".into())]))
            .expect("derives an id");
        assert_eq!(id, "public.orders:a_b");
    }

    #[test]
    fn composite_key_joins_with_underscore() {
        let id = element_id(
            &mapping(),
            &["order_id", "line"],
            &row(&[("order_id", 1001.into()), ("line", 5.into())]),
        )
        .expect("derives an id");
        assert_eq!(
            id, "public.orders:1001_5",
            "matches the documented convention when no component needs escaping"
        );
    }

    /// The defect this escaping exists to close: without it, these two distinct
    /// rows both render `public.orders:a_b_c` and merge into one graph node.
    #[test]
    fn composite_key_components_containing_the_separator_stay_distinct() {
        let left = element_id(
            &mapping(),
            &["a", "b"],
            &row(&[("a", "a_b".into()), ("b", "c".into())]),
        )
        .expect("derives an id");
        let right = element_id(
            &mapping(),
            &["a", "b"],
            &row(&[("a", "a".into()), ("b", "b_c".into())]),
        )
        .expect("derives an id");

        assert_eq!(left, "public.orders:a%5Fb_c");
        assert_eq!(right, "public.orders:a_b%5Fc");
        assert_ne!(left, right);
    }

    /// Escaping `%` too keeps an already-escaped-looking value from colliding
    /// with a genuinely escaped one.
    #[test]
    fn composite_key_percent_is_escaped_so_encoding_stays_injective() {
        let literal_percent = element_id(
            &mapping(),
            &["a", "b"],
            &row(&[("a", "a%5Fb".into()), ("b", "c".into())]),
        )
        .expect("derives an id");
        let literal_underscore = element_id(
            &mapping(),
            &["a", "b"],
            &row(&[("a", "a_b".into()), ("b", "c".into())]),
        )
        .expect("derives an id");

        assert_eq!(literal_percent, "public.orders:a%255Fb_c");
        assert_eq!(literal_underscore, "public.orders:a%5Fb_c");
        assert_ne!(
            literal_percent, literal_underscore,
            "a literal '%5F' must not collide with an escaped '_'"
        );
    }

    /// A NULL key cannot name a row, and guessing one would silently merge
    /// every such row onto a single node.
    #[test]
    fn null_key_is_rejected() {
        let err = element_id(&mapping(), &["id"], &row(&[("id", Value::Null)]))
            .expect_err("a NULL key has no stable id");
        assert!(matches!(err, Error::PrimaryKeyValueNull { .. }));
    }

    #[test]
    fn absent_key_column_is_rejected() {
        let err = element_id(&mapping(), &["id"], &row(&[("other", 1.into())]))
            .expect_err("a missing key column has no stable id");
        assert!(matches!(err, Error::PrimaryKeyColumnMissing { .. }));
    }

    #[test]
    fn keyless_row_is_rejected() {
        let err = element_id(&mapping(), &[], &row(&[("id", 1.into())]))
            .expect_err("a row with no key has no stable id");
        assert!(matches!(err, Error::PrimaryKeyMissing { .. }));
    }

    #[test]
    fn id_prefix_falls_back_to_the_dataset_name_without_labels() {
        let mapping = ElementMapping::new("orders".to_string(), vec![]);
        let id = element_id(&mapping, &["id"], &row(&[("id", 7.into())])).expect("derives an id");
        assert_eq!(id, "orders:7");
    }

    /// NULL columns must reach Drasi as explicit nulls; dropping the key would
    /// change the result of a continuous query testing `IS NULL`.
    #[test]
    fn nulls_are_rendered_explicitly() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("note", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("hi"), None])),
            ],
        )
        .expect("valid batch");

        let rows = rows_to_json("orders", &batch).expect("renders rows");

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get("note"), Some(&Value::from("hi")));
        assert_eq!(
            rows[1].get("note"),
            Some(&Value::Null),
            "a NULL column must be present with a null value, not omitted"
        );
    }

    #[test]
    fn node_element_carries_labels_and_all_columns() {
        let node = node_element(
            &mapping(),
            &["id"],
            row(&[("id", 7.into()), ("total", 42.5.into())]),
        )
        .expect("builds a node");

        assert_eq!(node.id, "public.orders:7");
        assert_eq!(node.labels.as_ref(), ["public.orders".to_string()]);
        assert_eq!(node.properties.get("total"), Some(&Value::from(42.5)));
        assert_eq!(
            node.properties.get("id"),
            Some(&Value::from(7)),
            "the key column stays a property as well as naming the element"
        );
    }
}
