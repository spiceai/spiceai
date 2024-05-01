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

use arrow::{
    array::{
        new_null_array, Array, Int32Array, Int64Array, LargeStringArray, RecordBatch, StringArray,
        TimestampMicrosecondArray, TimestampMillisecondArray, TimestampSecondArray,
    },
    datatypes::{DataType, Field, SchemaRef, TimeUnit},
};
use snafu::prelude::*;
use std::{num::TryFromIntError, sync::Arc};

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error converting record batch: {}", source))]
    ConvertRecordBatch { source: arrow::error::ArrowError },

    #[snafu(display("Unable to convert array: {}", source))]
    ParseTimestampFromString { source: chrono::ParseError },

    #[snafu(display("Unable to convert {} array from {:?} to {:?}", name, from, to))]
    ConvertArrowArray {
        from: DataType,
        to: DataType,
        name: String,
    },

    #[snafu(display("Unable to try from int: {}", source))]
    ConvertInt { source: TryFromIntError },

    #[snafu(display("Unable to use null for field {field}"))]
    UnableToUseNullForField { field: String },

    #[snafu(display("Unable to downcast array {field} to {downcast_to}"))]
    UnableToDowncast { field: String, downcast_to: String },
}

/// Cast a given record batch into a new record batch with the given schema.
///
/// # Errors
///
/// This function will return an error if the record batch cannot be casted.
#[allow(clippy::needless_pass_by_value)]
pub fn try_cast_to(record_batch: RecordBatch, schema: SchemaRef) -> Result<RecordBatch> {
    let existing_schema = record_batch.schema();

    if schema.contains(&existing_schema) {
        return record_batch
            .with_schema(schema)
            .context(ConvertRecordBatchSnafu);
    }

    let cols = schema
        .fields()
        .into_iter()
        .map(|field| {
            if let (Ok(existing_field), Some(column)) = (
                record_batch.schema().field_with_name(field.name()),
                record_batch.column_by_name(field.name()),
            ) {
                if field.contains(existing_field) {
                    Ok(Arc::clone(column))
                } else {
                    try_cast_array(Arc::clone(column), existing_field, field)
                }
            } else if field.is_nullable() {
                Ok(new_null_array(field.data_type(), record_batch.num_rows()))
            } else {
                UnableToUseNullForFieldSnafu {
                    field: field.name(),
                }
                .fail()
            }
        })
        .collect::<Result<Vec<Arc<dyn Array>>>>()?;

    RecordBatch::try_new(schema, cols).context(ConvertRecordBatchSnafu)
}

#[allow(clippy::needless_pass_by_value)]
fn try_cast_array(
    array: Arc<dyn Array>,
    existing_field: &Field,
    field: &Field,
) -> Result<Arc<dyn Array>> {
    let result: Arc<dyn Array> = match (existing_field.data_type(), field.data_type()) {
        (DataType::Int32, DataType::Int64) => {
            if let Some(array) = array.as_any().downcast_ref::<Int32Array>() {
                Arc::new(Int64Array::from(
                    array
                        .into_iter()
                        .map(|f| f.map(i64::from))
                        .collect::<Vec<Option<i64>>>(),
                ))
            } else {
                UnableToDowncastSnafu {
                    field: field.name(),
                    downcast_to: "Int32Array".to_string(),
                }
                .fail()?
            }
        }
        (DataType::Int64, DataType::Int32) => {
            if let Some(array) = array.as_any().downcast_ref::<Int64Array>() {
                Arc::new(Int32Array::from(
                    array
                        .into_iter()
                        .map(|f| {
                            f.map_or_else(
                                || Ok(None),
                                |f| Ok(Some(i32::try_from(f).context(ConvertIntSnafu)?)),
                            )
                        })
                        .collect::<Result<Vec<Option<i32>>, _>>()?,
                ))
            } else {
                UnableToDowncastSnafu {
                    field: field.name(),
                    downcast_to: "Int64Array".to_string(),
                }
                .fail()?
            }
        }
        (DataType::Utf8, DataType::LargeUtf8) => {
            if let Some(array) = array.as_any().downcast_ref::<StringArray>() {
                Arc::new(LargeStringArray::from(
                    array.into_iter().collect::<Vec<Option<&str>>>(),
                ))
            } else {
                UnableToDowncastSnafu {
                    field: field.name(),
                    downcast_to: "StringArray".to_string(),
                }
                .fail()?
            }
        }
        (DataType::Utf8, DataType::Timestamp(unit, _)) => {
            try_cast_array_from_string_to_timestamp(array, unit, field)?
        }
        (_, _) => ConvertArrowArraySnafu {
            from: existing_field.data_type().clone(),
            to: field.data_type().clone(),
            name: field.name(),
        }
        .fail()?,
    };

    Ok(result)
}

#[allow(clippy::needless_pass_by_value)]
fn try_cast_array_from_string_to_timestamp(
    array: Arc<dyn Array>,
    unit: &TimeUnit,
    field: &Field,
) -> Result<Arc<dyn Array>> {
    let data_type = field.data_type().clone();
    Ok(
        if let Some(array) = array.as_any().downcast_ref::<StringArray>() {
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
                                        name: field.name(),
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
                    name: field.name(),
                }
                .fail()?,
            }
        } else {
            UnableToDowncastSnafu {
                field: field.name(),
                downcast_to: "StringArray".to_string(),
            }
            .fail()?
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
        let result = try_cast_to(batch_input(), to_schema()).expect("converted");
        assert_eq!(3, result.num_rows());
    }
}
