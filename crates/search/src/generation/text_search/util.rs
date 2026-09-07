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
use snafu::ResultExt;
use std::sync::Arc;

use arrow::{
    array::{ArrayRef, RecordBatch, StringArray},
    datatypes::DataType,
    error::ArrowError,
};
use arrow_schema::{Field as ArrowField, Schema, SchemaRef};

/// Adds an additional [`StringArray`] column to a [`RecordBatch`] as a JSON-string representation
/// from a subset of the columns present.
pub fn with_json_subset_column(
    batch: &RecordBatch,
    subset_columns: &[String],
    new_column_name: &str,
) -> Result<RecordBatch, Box<dyn std::error::Error + Send + Sync>> {
    let mut subset_fields = Vec::with_capacity(subset_columns.len());
    let mut subset_arrays = Vec::with_capacity(subset_columns.len());
    for col_name in subset_columns {
        let idx = batch.schema().index_of(col_name.as_str()).boxed()?;
        subset_fields.push(batch.schema().field(idx).clone());
        subset_arrays.push(Arc::clone(batch.column(idx)));
    }

    let subset_schema: SchemaRef = Arc::new(Schema::new(subset_fields));
    let subset_batch = RecordBatch::try_new(Arc::clone(&subset_schema), subset_arrays).boxed()?;

    // Line-delimited writer emits one JSON object per row (NDJSON). Use the raw line
    // bytes as Utf8 values directly — no serde Map parse/re-serialize round-trip.
    let buf = Vec::new();
    let mut writer = arrow_json::LineDelimitedWriter::new(buf);
    writer.write_batches(&[&subset_batch]).boxed()?;
    writer.finish().boxed()?;
    let json_data = writer.into_inner();

    let json_str = std::str::from_utf8(&json_data).boxed()?;
    // `lines().filter(...)` is not ExactSizeIterator; `from_iter_values` requires sized.
    let lines: Vec<&str> = json_str.lines().filter(|line| !line.is_empty()).collect();
    let json_array: ArrayRef = Arc::new(StringArray::from(lines));

    let mut new_fields: Vec<_> = batch.schema().fields().iter().cloned().collect();
    new_fields.push(Arc::new(ArrowField::new(
        new_column_name,
        DataType::Utf8,
        false,
    )));
    let new_schema: SchemaRef = Arc::new(Schema::new(new_fields));

    let mut new_columns: Vec<ArrayRef> = batch.columns().to_vec();
    new_columns.push(json_array);

    RecordBatch::try_new(new_schema, new_columns).boxed()
}

/// Returns `batch` with every column named in `exclude` removed. Column order is otherwise
/// preserved.
pub fn without_columns(batch: &RecordBatch, exclude: &[String]) -> Result<RecordBatch, ArrowError> {
    let keep: Vec<usize> = (0..batch.num_columns())
        .filter(|&i| !exclude.iter().any(|e| e == batch.schema().field(i).name()))
        .collect();
    batch.project(&keep)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{Array, Int32Array, RecordBatch, StringArray},
        datatypes::{DataType, Field, Schema},
    };

    use super::with_json_subset_column;

    /// Locks the exact NDJSON string format written into tantivy as the composite
    /// primary-key unique field. Format drift breaks update/delete term matching
    /// against existing on-disk indexes.
    #[test]
    fn json_subset_column_utf8_int32_golden_format() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("seq", DataType::Int32, false),
            Field::new("content", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["a", "b"])),
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["apple", "banana"])),
            ],
        )
        .expect("test batch");

        let with_pk = with_json_subset_column(
            &batch,
            &["id".to_string(), "seq".to_string()],
            "__spice.unique_field",
        )
        .expect("json subset column");

        let col_idx = with_pk
            .schema()
            .index_of("__spice.unique_field")
            .expect("unique field present");
        let json_col = with_pk
            .column(col_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Utf8 unique field");

        let lines: Vec<&str> = (0..json_col.len()).map(|i| json_col.value(i)).collect();
        insta::assert_snapshot!("json_subset_column_utf8_int32", lines.join("\n"));
    }
}
