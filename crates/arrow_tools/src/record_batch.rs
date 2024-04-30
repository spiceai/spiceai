/*
Copyright 2024 The Spice.ai OSS Authors

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

use std::sync::Arc;

use arrow::{
    array::{
        new_null_array, Array, ArrayRef, Int32Array, Int64Array, LargeStringArray, RecordBatch,
        StringArray, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampSecondArray,
    },
    datatypes::{DataType, Field, SchemaRef, TimeUnit},
};
use snafu::prelude::*;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error converting record batch: {}", source))]
    ConvertRecordBatch { source: arrow::error::ArrowError },

    #[snafu(display("Unable to convert array: {}", source))]
    ParseTimestampFromString { source: chrono::ParseError },

    #[snafu(display("Unable to convert array from {:?} to {:?}", from, to))]
    ConvertArrowArray { from: DataType, to: DataType },
}

/// Convert a given record batch into a new record batch with the given schema.
///
/// # Errors
///
/// This function will return an error if the record batch cannot be converted.
#[allow(clippy::needless_pass_by_value)]
pub fn convert_to(record_batch: RecordBatch, schema: SchemaRef) -> Result<RecordBatch> {
    let existing_schema = record_batch.schema();

    if schema.contains(&existing_schema) {
        return record_batch
            .with_schema(schema)
            .context(ConvertRecordBatchSnafu);
    }

    let num_rows = record_batch.num_rows();
    let mut cols = vec![];

    for field in schema.fields() {
        let mut array: ArrayRef = new_null_array(field.data_type(), num_rows);

        if let (Ok(existing_field), Some(column)) = (
            record_batch.schema().field_with_name(field.name()),
            record_batch.column_by_name(field.name()),
        ) {
            if field.contains(existing_field) {
                array = Arc::clone(column);
            } else {
                array = convert_column(Arc::clone(column), existing_field, field)?;
            }
        }

        cols.push(array);
    }

    RecordBatch::try_new(schema, cols).context(ConvertRecordBatchSnafu)
}

#[allow(clippy::needless_pass_by_value)]
fn convert_column(
    column: Arc<dyn Array>,
    existing_field: &Field,
    field: &Field,
) -> Result<Arc<dyn Array>> {
    let result: Arc<dyn Array> = match (existing_field.data_type(), field.data_type()) {
        (DataType::Int32, DataType::Int64) => {
            if let Some(array) = column.as_any().downcast_ref::<Int32Array>() {
                let converted: Vec<Option<i64>> =
                    array.into_iter().map(|f| f.map(i64::from)).collect();
                Arc::new(Int64Array::from(converted))
            } else {
                Arc::new(new_null_array(field.data_type(), column.len()))
            }
        }
        (DataType::Utf8, DataType::LargeUtf8) => {
            if let Some(array) = column.as_any().downcast_ref::<StringArray>() {
                let converted: Vec<Option<&str>> = array.into_iter().collect();
                Arc::new(LargeStringArray::from(converted))
            } else {
                Arc::new(new_null_array(field.data_type(), column.len()))
            }
        }
        (DataType::Utf8, DataType::Timestamp(unit, _)) => {
            convert_timestamp_column(column, unit, field)?
        }
        (_, _) => ConvertArrowArraySnafu {
            from: existing_field.data_type().clone(),
            to: field.data_type().clone(),
        }
        .fail()?,
    };

    Ok(result)
}

#[allow(clippy::needless_pass_by_value)]
fn convert_timestamp_column(
    column: Arc<dyn Array>,
    unit: &TimeUnit,
    field: &Field,
) -> Result<Arc<dyn Array>, Error> {
    let data_type = field.data_type().clone();
    Ok(
        if let Some(array) = column.as_any().downcast_ref::<StringArray>() {
            let converted: Vec<Option<i64>> = array
                .into_iter()
                .map(|f| -> Result<Option<i64>> {
                    if let Some(f) = f {
                        let value =
                            chrono::NaiveDateTime::parse_from_str(f, "%Y-%m-%d %H:%M:%S%.6f")
                                .context(ParseTimestampFromStringSnafu)?
                                .and_utc()
                                .timestamp_micros()
                                / (match unit {
                                    TimeUnit::Second => 1_000_000,
                                    TimeUnit::Millisecond => 1_000,
                                    TimeUnit::Microsecond => 1,
                                    TimeUnit::Nanosecond => ConvertArrowArraySnafu {
                                        from: DataType::Timestamp(unit.clone(), None),
                                        to: data_type.clone(),
                                    }
                                    .fail()?,
                                });
                        Ok(Some(value))
                    } else {
                        Ok(None)
                    }
                })
                .collect::<Result<_, _>>()?;
            match unit.clone() {
                TimeUnit::Second => Arc::new(TimestampSecondArray::from(converted)),
                TimeUnit::Millisecond => Arc::new(TimestampMillisecondArray::from(converted)),
                TimeUnit::Microsecond => Arc::new(TimestampMicrosecondArray::from(converted)),
                TimeUnit::Nanosecond => ConvertArrowArraySnafu {
                    from: DataType::Timestamp(unit.clone(), None),
                    to: data_type,
                }
                .fail()?,
            }
        } else {
            Arc::new(new_null_array(field.data_type(), column.len()))
        },
    )
}

#[cfg(test)]
mod test {
    use arrow::datatypes::Schema;

    use super::*;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, false),
            Field::new("c", DataType::Utf8, false),
        ]))
    }

    fn to_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::LargeUtf8, false),
            Field::new("c", DataType::Timestamp(TimeUnit::Microsecond, None), false),
        ]))
    }

    fn batch_input() -> RecordBatch {
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["foo", "bar", "baz"])),
                Arc::new(StringArray::from(vec![
                    "2024-01-13 03:18:09.000000",
                    "2024-01-13 03:18:09",
                    "2024-01-13 03:18:09.000",
                ])),
            ],
        )
        .expect("record batch should not panic")
    }

    #[test]
    fn test_string_to_timestamp_conversion() {
        let result = convert_to(batch_input(), to_schema()).expect("converted");
        assert_eq!(3, result.num_rows());
    }
}
