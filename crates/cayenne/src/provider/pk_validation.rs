/*
Copyright 2025-2026 The Spice.ai OSS Authors

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

//! The message every write and delete path reports when a primary key column holds a
//! null. A composite key is only actionable if the message says which of its columns
//! carried the null, so the message is built from the batch rather than written out at
//! each call site.

use arrow::record_batch::RecordBatch;

const FIX: &str = "Every primary key column must be non-null: populate it in the source data, or set `primary_key` to columns that are always present. For details, visit https://spiceai.org/docs/features/data-acceleration/constraints";

/// The `DataValidation` message for a batch whose primary key columns (`pk_indices`,
/// as positions in `batch`) are not all populated, naming the ones that hold a null.
pub(crate) fn null_primary_key_message(batch: &RecordBatch, pk_indices: &[usize]) -> String {
    let schema = batch.schema();
    let columns: Vec<String> = pk_indices
        .iter()
        .filter(|&&index| {
            batch
                .columns()
                .get(index)
                .is_some_and(|column| column.null_count() > 0)
        })
        .map(|&index| format!("'{}'", schema.field(index).name()))
        .collect();

    match columns.as_slice() {
        // The caller found a null the null counts no longer see (a batch swapped
        // underneath, an index outside the batch): still report the violation.
        [] => format!("Primary key values must be non-null. {FIX}"),
        [column] => format!("Primary key column {column} has null values. {FIX}"),
        _ => format!(
            "Primary key columns {} have null values. {FIX}",
            columns.join(", ")
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn batch(instance_ids: Vec<Option<&str>>, values: Vec<Option<i64>>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("time_unix_nano", DataType::Int64, false),
            Field::new("service.instance.id", DataType::Utf8, true),
            Field::new("value", DataType::Int64, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1_i64, 2])),
                Arc::new(StringArray::from(instance_ids)),
                Arc::new(Int64Array::from(values)),
            ],
        )
        .expect("batch")
    }

    #[test]
    fn names_the_null_column_of_a_composite_key() {
        let message =
            null_primary_key_message(&batch(vec![Some("a"), None], vec![None, None]), &[0, 1]);
        assert!(
            message.starts_with("Primary key column 'service.instance.id' has null values."),
            "the null column must be named: {message}"
        );
        assert!(
            message.contains("data-acceleration/constraints"),
            "the message must link the constraints docs: {message}"
        );
    }

    #[test]
    fn names_every_null_column() {
        let message =
            null_primary_key_message(&batch(vec![None, None], vec![Some(1), None]), &[1, 2]);
        assert!(
            message.starts_with(
                "Primary key columns 'service.instance.id', 'value' have null values."
            ),
            "both null columns must be named: {message}"
        );
    }

    #[test]
    fn reports_the_violation_without_a_column_when_none_is_null() {
        let message = null_primary_key_message(
            &batch(vec![Some("a"), Some("b")], vec![None, None]),
            &[0, 1],
        );
        assert!(
            message.starts_with("Primary key values must be non-null."),
            "the fallback keeps reporting the violation: {message}"
        );
    }
}
