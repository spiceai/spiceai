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

//! The JSON key columns a data file stores: per row, the text of the value
//! under one top-level key of one JSON column ([`super::top_level_value`]), after
//! the columns the file holds for the table.
//!
//! A write chooses them from the head of its own rows before its first file
//! starts, since a file's schema is fixed when its writer opens it. A string
//! column of the table is a JSON column for the write when nearly all of the
//! head's non-NULL values are JSON objects; its stored keys are the top-level
//! keys found in at least half of the head's rows, the most frequent first, at
//! most [`MAX_KEYS_PER_COLUMN`] of them. Every file of the write stores the same
//! keys, and another write may choose others: a scan reads each file's stored
//! values where it has them and computes them from the JSON column where it
//! does not, so the choice changes what a query costs, never what it returns.
//!
//! The stored values are exact for every row, whatever the choice: each is
//! [`super::top_level_value`] of the row's document, from one walk of the
//! document for all of a column's keys.

use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;

use arrow::array::builder::{LargeStringBuilder, StringBuilder, StringViewBuilder};
use arrow::array::{Array, ArrayRef, AsArray, RecordBatch, RecordBatchOptions};
use arrow::datatypes::{DataType, Field, FieldRef, Schema, SchemaRef};
use datafusion::common::{Result, exec_err};
use futures::{Stream, StreamExt};

use super::{
    for_each_top_level_key, hidden_column_name, is_json_text_type, is_shreddable_key,
    top_level_value_ranges,
};

/// Rows at the head of a write that choose its stored keys.
pub(crate) const SAMPLE_ROWS: usize = 8192;

/// Most keys a file stores for one JSON column.
pub(crate) const MAX_KEYS_PER_COLUMN: usize = 16;

/// Share, in percent, of a column's non-NULL sampled values that must be JSON
/// objects for the column to be treated as JSON.
const MIN_OBJECT_PERCENT: usize = 90;

/// Share, in percent, of the sampled rows a key must be found in to be stored.
const MIN_KEY_PERCENT: usize = 50;

/// Longest key, in bytes, a file stores a column for.
const MAX_KEY_BYTES: usize = 256;

/// Distinct keys counted per column while sampling; keys first seen after this
/// many are not counted.
const MAX_COUNTED_KEYS: usize = 16_384;

/// The JSON key columns every file of one write stores, after the write's own
/// columns.
#[derive(Debug)]
pub struct StoredJsonKeys {
    columns: Vec<StoredColumn>,
    fields: Vec<FieldRef>,
}

/// One JSON column's stored keys.
#[derive(Debug)]
struct StoredColumn {
    /// Index of the JSON column in the write's schema.
    source: usize,
    /// The JSON column's type in the write's schema, which its stored key
    /// columns take.
    data_type: DataType,
    keys: Vec<String>,
}

/// How often each key was found while sampling one column.
#[derive(Default)]
struct KeyCounts {
    rows: usize,
    non_null: usize,
    objects: usize,
    /// Per key: the rows it was found in, and the last row that counted it, so a
    /// duplicate key counts once per row.
    keys: HashMap<Box<str>, (usize, usize)>,
}

impl KeyCounts {
    fn observe(&mut self, document: Option<&str>) {
        let row = self.rows;
        self.rows += 1;
        let Some(document) = document else {
            return;
        };
        self.non_null += 1;
        let keys = &mut self.keys;
        let is_object = for_each_top_level_key(document, |key| {
            if let Some((rows, last)) = keys.get_mut(key) {
                if *last != row {
                    *rows += 1;
                    *last = row;
                }
            } else if keys.len() < MAX_COUNTED_KEYS {
                keys.insert(key.into(), (1, row));
            }
        });
        if is_object {
            self.objects += 1;
        }
    }

    /// The keys to store, most frequent first: none unless the column is JSON.
    fn chosen(self, json_column: &str, taken: &Schema) -> Vec<String> {
        if self.non_null == 0 || self.objects * 100 < self.non_null * MIN_OBJECT_PERCENT {
            return Vec::new();
        }
        let rows = self.rows;
        let mut keys: Vec<(Box<str>, usize)> = self
            .keys
            .into_iter()
            .map(|(key, (found, _))| (key, found))
            .filter(|(key, found)| {
                found * 100 >= rows * MIN_KEY_PERCENT
                    && key.len() <= MAX_KEY_BYTES
                    && is_shreddable_key(key)
                    && taken
                        .field_with_name(&hidden_column_name(json_column, key))
                        .is_err()
            })
            .collect();
        keys.sort_unstable_by(|(a, a_found), (b, b_found)| b_found.cmp(a_found).then(a.cmp(b)));
        keys.truncate(MAX_KEYS_PER_COLUMN);
        keys.into_iter().map(|(key, _)| key.into_string()).collect()
    }
}

impl StoredJsonKeys {
    /// The columns of `write_schema` that may be JSON: string columns the table
    /// (`table_schema`) has.
    #[must_use]
    pub fn candidates(write_schema: &Schema, table_schema: &Schema) -> Vec<usize> {
        write_schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, field)| {
                is_json_text_type(field.data_type())
                    && table_schema
                        .field_with_name(field.name())
                        .is_ok_and(|table_field| is_json_text_type(table_field.data_type()))
            })
            .map(|(index, _)| index)
            .collect()
    }

    /// The keys a write of `write_schema` stores for its `candidates`, chosen from
    /// the first [`SAMPLE_ROWS`] rows of `head`, its first batches. `None` when it
    /// stores none.
    #[must_use]
    pub fn choose(
        write_schema: &Schema,
        candidates: &[usize],
        head: &[RecordBatch],
    ) -> Option<Self> {
        let mut columns = Vec::new();
        let mut fields = Vec::new();
        for &source in candidates {
            let field = write_schema.fields().get(source)?;
            let mut counts = KeyCounts::default();
            for batch in head {
                if counts.rows >= SAMPLE_ROWS {
                    break;
                }
                let Some(json) = batch.column_by_name(field.name()) else {
                    continue;
                };
                let take = (SAMPLE_ROWS - counts.rows).min(json.len());
                let json = json.slice(0, take);
                match json.data_type() {
                    DataType::Utf8 => json.as_string::<i32>().iter().for_each(|doc| counts.observe(doc)),
                    DataType::LargeUtf8 => json.as_string::<i64>().iter().for_each(|doc| counts.observe(doc)),
                    DataType::Utf8View => json.as_string_view().iter().for_each(|doc| counts.observe(doc)),
                    _ => {}
                }
            }
            let keys = counts.chosen(field.name(), write_schema);
            if keys.is_empty() {
                continue;
            }
            fields.extend(keys.iter().map(|key| {
                Arc::new(Field::new(
                    hidden_column_name(field.name(), key),
                    field.data_type().clone(),
                    true,
                )) as FieldRef
            }));
            columns.push(StoredColumn {
                source,
                data_type: field.data_type().clone(),
                keys,
            });
        }
        (!columns.is_empty()).then_some(Self { columns, fields })
    }

    /// `write_schema` followed by the stored key columns: the schema of each file.
    #[must_use]
    pub fn file_schema(&self, write_schema: &Schema) -> SchemaRef {
        let mut fields: Vec<FieldRef> = write_schema.fields().iter().cloned().collect();
        fields.extend(self.fields.iter().cloned());
        Arc::new(Schema::new_with_metadata(
            fields,
            write_schema.metadata().clone(),
        ))
    }

    /// The stored key columns' `(JSON column, key)` pairs, in file order.
    #[must_use]
    pub fn keys(&self, write_schema: &Schema) -> Vec<(String, String)> {
        self.columns
            .iter()
            .flat_map(|column| {
                let name = write_schema.field(column.source).name().clone();
                column
                    .keys
                    .iter()
                    .map(move |key| (name.clone(), key.clone()))
            })
            .collect()
    }

    /// `batch` (rows of the write's schema) followed by its stored key columns,
    /// under `file_schema` ([`Self::file_schema`]).
    ///
    /// # Errors
    ///
    /// When `batch` does not hold the write's JSON columns as strings, or the
    /// columns do not match `file_schema`.
    pub fn append(&self, batch: &RecordBatch, file_schema: &SchemaRef) -> Result<RecordBatch> {
        let rows = batch.num_rows();
        let mut columns = batch.columns().to_vec();
        for column in &self.columns {
            let Some(json) = batch.columns().get(column.source) else {
                return exec_err!(
                    "Cannot store JSON key columns: the written rows have no column at index {}",
                    column.source
                );
            };
            columns.extend(column.values(json.as_ref())?);
        }
        Ok(RecordBatch::try_new_with_options(
            Arc::clone(file_schema),
            columns,
            &RecordBatchOptions::new().with_row_count(Some(rows)),
        )?)
    }
}

/// Builds one stored key column in the JSON column's own string type, copying
/// each value out of its document.
enum ValueBuilder {
    Utf8(StringBuilder),
    LargeUtf8(LargeStringBuilder),
    Utf8View(StringViewBuilder),
}

impl ValueBuilder {
    fn new(data_type: &DataType, rows: usize) -> Option<Self> {
        match data_type {
            DataType::Utf8 => Some(Self::Utf8(StringBuilder::with_capacity(rows, 0))),
            DataType::LargeUtf8 => Some(Self::LargeUtf8(LargeStringBuilder::with_capacity(rows, 0))),
            DataType::Utf8View => Some(Self::Utf8View(StringViewBuilder::with_capacity(rows))),
            _ => None,
        }
    }

    fn append(&mut self, value: Option<&str>) {
        match self {
            Self::Utf8(builder) => builder.append_option(value),
            Self::LargeUtf8(builder) => builder.append_option(value),
            Self::Utf8View(builder) => builder.append_option(value),
        }
    }

    fn finish(self) -> ArrayRef {
        match self {
            Self::Utf8(mut builder) => Arc::new(builder.finish()),
            Self::LargeUtf8(mut builder) => Arc::new(builder.finish()),
            Self::Utf8View(mut builder) => Arc::new(builder.finish()),
        }
    }
}

impl StoredColumn {
    /// Per key, [`super::top_level_value`] of every document in `json`.
    fn values(&self, json: &dyn Array) -> Result<Vec<ArrayRef>> {
        let rows = json.len();
        let Some(mut builders) = self
            .keys
            .iter()
            .map(|_| ValueBuilder::new(json.data_type(), rows))
            .collect::<Option<Vec<_>>>()
        else {
            return exec_err!(
                "Cannot store JSON key columns of a column of type {}",
                json.data_type()
            );
        };
        let keys: Vec<&str> = self.keys.iter().map(String::as_str).collect();
        let mut found: Vec<Option<Range<usize>>> = vec![None; keys.len()];
        let mut store = |document: Option<&str>| {
            match document {
                Some(document) => {
                    top_level_value_ranges(document, &keys, &mut found);
                    for (builder, range) in builders.iter_mut().zip(&found) {
                        builder.append(range.clone().and_then(|range| document.get(range)));
                    }
                }
                None => builders.iter_mut().for_each(|builder| builder.append(None)),
            }
        };
        match json.data_type() {
            DataType::Utf8 => json.as_string::<i32>().iter().for_each(&mut store),
            DataType::LargeUtf8 => json.as_string::<i64>().iter().for_each(&mut store),
            DataType::Utf8View => json.as_string_view().iter().for_each(&mut store),
            other => {
                return exec_err!("Cannot store JSON key columns of a column of type {other}");
            }
        }
        // Each value is a substring of its document, so a column built in the
        // documents' own type always fits it; one in another type is cast, which
        // fails rather than overflows.
        builders
            .into_iter()
            .map(|builder| {
                let values = builder.finish();
                if values.data_type() == &self.data_type {
                    Ok(values)
                } else {
                    Ok(arrow::compute::cast(&values, &self.data_type)?)
                }
            })
            .collect()
    }
}

/// `batches` with each batch's stored key columns appended, computed off the
/// async runtime with up to `parallelism` batches in flight, in order.
pub(crate) fn append_to_stream(
    batches: impl Stream<Item = Result<RecordBatch>> + Send + 'static,
    keys: Arc<StoredJsonKeys>,
    file_schema: SchemaRef,
    parallelism: usize,
) -> impl Stream<Item = Result<RecordBatch>> + Send + 'static {
    batches
        .map(move |batch| {
            let keys = Arc::clone(&keys);
            let file_schema = Arc::clone(&file_schema);
            async move {
                let batch = batch?;
                tokio::task::spawn_blocking(move || keys.append(&batch, &file_schema))
                    .await
                    .map_err(|error| {
                        datafusion::error::DataFusionError::ExecutionJoin(Box::new(error))
                    })?
            }
        })
        .buffered(parallelism.max(1))
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::{Int64Array, StringArray, StringViewArray};

    use crate::json_shred::top_level_value;

    fn schema(extra: &[&str]) -> SchemaRef {
        let mut fields = vec![
            Field::new("id", DataType::Int64, false),
            Field::new("attributes", DataType::Utf8, true),
            Field::new("body", DataType::Utf8, true),
        ];
        fields.extend(extra.iter().map(|name| Field::new(*name, DataType::Utf8, true)));
        Arc::new(Schema::new(fields))
    }

    fn batch(schema: &SchemaRef, attributes: &[Option<String>], body: &[&str]) -> RecordBatch {
        let rows = attributes.len();
        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from_iter_values(0..i64::try_from(rows).expect("fits"))),
            Arc::new(attributes.iter().map(Option::as_deref).collect::<StringArray>()),
            Arc::new(body.iter().map(|b| Some(*b)).collect::<StringArray>()),
        ];
        for _ in 3..schema.fields().len() {
            columns.push(Arc::new(StringArray::from(vec![None::<&str>; rows])));
        }
        RecordBatch::try_new(Arc::clone(schema), columns).expect("batch")
    }

    /// The keys a write stores: found in at least half of the head's rows, the
    /// most frequent first, at most 16, never an integer-like key, never one
    /// whose column name a written column already has, and none for a column
    /// whose values are not nearly all JSON objects.
    #[test]
    fn a_write_stores_the_keys_found_in_half_its_rows() {
        let colliding = hidden_column_name("attributes", "taken");
        let schema = schema(&[&colliding]);
        // 100 rows: `always` in every object (written twice in some), `half` in
        // 50, `rare` in 49, an integer key and a key named like a written column
        // in every object, and with `k_keys`, 20 keys `k00`..`k19` in 94 down to
        // 75 rows. Five rows are not objects (95% of the non-NULL ones are), one
        // is NULL.
        let attributes = |k_keys: bool| -> Vec<Option<String>> {
            (0..100)
                .map(|row| {
                    if row == 0 {
                        return None;
                    }
                    if row < 6 {
                        return Some(format!("not an object {row}"));
                    }
                    let mut members = vec![
                        "\"always\": 1".to_string(),
                        "\"0\": \"index\"".to_string(),
                        "\"taken\": 2".to_string(),
                    ];
                    if row % 2 == 0 {
                        members.push("\"always\": \"again\"".to_string());
                    }
                    if row < 56 {
                        members.push("\"half\": [1]".to_string());
                    }
                    if row >= 51 {
                        members.push("\"rare\": {}".to_string());
                    }
                    for k in 0..20 {
                        if k_keys && row >= 6 + k {
                            members.push(format!("\"k{k:02}\": {k}"));
                        }
                    }
                    Some(format!("{{{}}}", members.join(", ")))
                })
                .collect()
        };
        let body: Vec<&str> = (0..100).map(|_| "plain text").collect();
        let candidates = StoredJsonKeys::candidates(&schema, &schema);
        assert_eq!(candidates, vec![1, 2, 3]);
        let chosen = |k_keys: bool| -> Vec<String> {
            let head = [batch(&schema, &attributes(k_keys), &body)];
            StoredJsonKeys::choose(&schema, &candidates, &head)
                .expect("keys")
                .keys(&schema)
                .into_iter()
                .map(|(column, key)| {
                    assert_eq!(column, "attributes", "only the JSON column stores keys");
                    key
                })
                .collect()
        };
        // `half` (50 of 100 rows) qualifies; `rare` (49) does not.
        assert_eq!(chosen(false), vec!["always".to_string(), "half".to_string()]);
        // 22 keys qualify; the 16 most frequent are stored, ties by name.
        let mut expected = vec!["always".to_string()];
        expected.extend((0..15).map(|k| format!("k{k:02}")));
        assert_eq!(chosen(true), expected);

        // A column whose values are mostly not objects stores nothing.
        let mostly_text: Vec<Option<String>> = (0..100)
            .map(|row| Some(if row < 89 { "{\"a\": 1}".to_string() } else { "text".to_string() }))
            .collect();
        assert!(
            StoredJsonKeys::choose(&schema, &candidates, &[batch(&schema, &mostly_text, &body)])
                .is_none(),
            "89% objects is not a JSON column"
        );
    }

    /// Each stored value is [`top_level_value`] of its row's document, NULL where
    /// it finds none, for every string type a JSON column can have.
    #[test]
    fn stored_values_are_each_row_s_top_level_value() {
        let documents = vec![
            Some(r#"{"k": "short", "v": 1}"#),
            Some(r#"{"v": [1, oops], "k": "after a malformed value"}"#),
            None,
            Some(r#"{"k": "a value longer than twelve bytes", "k": "dup"}"#),
            Some("[1, 2]"),
            Some(""),
            Some(r#"{"v": {"nested": true}}"#),
        ];
        let keys = ["k", "v"];
        for data_type in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
            let json: ArrayRef = match data_type {
                DataType::Utf8 => Arc::new(StringArray::from(documents.clone())),
                DataType::LargeUtf8 => {
                    Arc::new(arrow::array::LargeStringArray::from(documents.clone()))
                }
                _ => Arc::new(StringViewArray::from(documents.clone())),
            };
            let write_schema = Arc::new(Schema::new(vec![Field::new(
                "doc",
                data_type.clone(),
                true,
            )]));
            let stored = StoredJsonKeys {
                columns: vec![StoredColumn {
                    source: 0,
                    data_type: data_type.clone(),
                    keys: keys.iter().map(ToString::to_string).collect(),
                }],
                fields: keys
                    .iter()
                    .map(|key| {
                        Arc::new(Field::new(
                            hidden_column_name("doc", key),
                            data_type.clone(),
                            true,
                        )) as FieldRef
                    })
                    .collect(),
            };
            let file_schema = stored.file_schema(&write_schema);
            let batch = RecordBatch::try_new(write_schema, vec![json]).expect("batch");
            let written = stored.append(&batch, &file_schema).expect("append");
            assert_eq!(written.schema(), file_schema);
            for (position, key) in keys.iter().enumerate() {
                let values = arrow::compute::cast(written.column(1 + position), &DataType::Utf8)
                    .expect("cast");
                let values = values.as_string::<i32>();
                let got: Vec<Option<&str>> = values.iter().collect();
                let expected: Vec<Option<&str>> = documents
                    .iter()
                    .map(|doc| doc.and_then(|doc| top_level_value(doc, key)))
                    .collect();
                assert_eq!(got, expected, "{data_type}: key {key}");
            }
        }
    }
}
