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

use crate::arrow::struct_builder::StructBuilder;

use super::change_event::Field as ChangeEventField;
use arrow::{
    array::{
        ArrayBuilder, BinaryBuilder, BooleanBuilder, Decimal128Builder, Float32Builder,
        Float64Builder, Int16Builder, Int32Builder, Int64Builder, ListBuilder, PrimitiveBuilder,
        RecordBatch, StringBuilder, StructArray, Time64MicrosecondBuilder,
        TimestampMicrosecondBuilder, TimestampMillisecondBuilder,
    },
    datatypes::{
        ArrowPrimitiveType, DataType, Date32Type, Field, Int16Type, Int32Type, Int64Type, Schema,
        Time64MicrosecondType, TimeUnit, TimestampMicrosecondType, TimestampMillisecondType,
    },
};
use base64::prelude::*;
use chrono::{DateTime, NaiveTime, Timelike, Utc};
use snafu::prelude::*;
use std::sync::Arc;

pub mod changes;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Missing the parameters map for org.apache.kafka.connect.data.Decimal"))]
    MissingParametersForDecimal,

    #[snafu(display("Missing the `connect.decimal.precision` parameter for org.apache.kafka.connect.data.Decimal"))]
    MissingPrecisionForDecimal,

    #[snafu(display("Unable to parse precision value for decimal: {source}"))]
    UnableToParsePrecision { source: std::num::ParseIntError },

    #[snafu(display("Unable to parse scale value for decimal: {source}"))]
    UnableToParseScale { source: std::num::ParseIntError },

    #[snafu(display("Missing the `scale` parameter for org.apache.kafka.connect.data.Decimal"))]
    MissingScaleForDecimal,

    #[snafu(display("Missing the `items` field for array"))]
    MissingItemsForArray,

    #[snafu(display("Missing the required `field` name"))]
    MissingFieldName,

    #[snafu(display("Missing the required field {field_name} in {value}"))]
    MissingFieldInValue {
        field_name: String,
        value: serde_json::Value,
    },

    #[snafu(display(
        "Missing field builder at index {data_struct_field_idx} in struct with schema {schema:?}"
    ))]
    MissingStructBuilder {
        data_struct_field_idx: usize,
        schema: Schema,
    },

    #[snafu(display("Unable to downcast ArrayBuilder"))]
    DowncastBuilder,

    #[snafu(display("Unable to decode base64 string: {source}"))]
    UnableToDecodeBase64 { source: base64::DecodeError },

    #[snafu(display("Decimal value is not 16 bytes. Got: {} bytes", value.len()))]
    Decimal128BytesNot16Bytes { value: Vec<u8> },

    #[snafu(display("Unable to convert value to i64"))]
    UnableToConvertToI64,

    #[snafu(display("Unable to convert value to f64"))]
    UnableToConvertToF64,

    #[snafu(display("Timestamp type ({unit:?},{time_zone:?}) not supported yet",))]
    TimestampNotSupported {
        unit: TimeUnit,
        time_zone: Option<String>,
    },

    #[snafu(display("Data type {data_type} not supported yet"))]
    DataTypeNotSupported { data_type: DataType },

    #[snafu(display("List field {data_type} not supported yet"))]
    ListDataTypeNotSupported { data_type: DataType },

    #[snafu(display("Debezium field type {field_type} not supported yet"))]
    DebeziumFieldNotSupported { field_type: String },

    #[snafu(display("Unable to parse timestamp: {source}"))]
    UnableToParseTimestamp { source: chrono::ParseError },

    #[snafu(display("A deletion change was received without a 'before' field."))]
    DeleteOpWithoutBeforeField,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub fn convert_fields_to_arrow_schema(fields: Vec<&ChangeEventField>) -> Result<Schema> {
    let arrow_fields = fields
        .into_iter()
        .map(convert_to_arrow_field)
        .collect::<Result<Vec<Field>>>()?;

    Ok(Schema::new(arrow_fields))
}

pub fn to_record_batch(values: Vec<serde_json::Value>, schema: &Schema) -> Result<RecordBatch> {
    Ok(to_struct_array(values, schema)?.into())
}

pub fn to_struct_array(values: Vec<serde_json::Value>, schema: &Schema) -> Result<StructArray> {
    let mut struct_builder = StructBuilder::from_fields(schema.fields().clone(), values.len());

    for value in values {
        append_value_to_struct_builder(value, &mut struct_builder)?;
    }

    Ok(struct_builder.finish())
}

pub fn append_value_to_struct_builder(
    value: serde_json::Value,
    builder: &mut StructBuilder,
) -> Result<()> {
    builder.append(true);

    for (idx, field) in builder.fields().iter().enumerate() {
        let Some(field_value) = value.get(field.name()) else {
            return MissingFieldInValueSnafu {
                field_name: field.name().to_string(),
                value,
            }
            .fail();
        };

        let field_builder = builder.field_builder_array(idx);

        append_field_value_to_builder(field_value, field, field_builder)?;
    }

    Ok(())
}

#[allow(clippy::cast_possible_truncation)]
#[allow(clippy::too_many_lines)]
fn append_field_value_to_builder(
    field_value: &serde_json::Value,
    field: &Arc<Field>,
    builder: &mut dyn ArrayBuilder,
) -> Result<()> {
    match field.data_type() {
        DataType::Utf8 => {
            let str_builder = downcast_builder::<StringBuilder>(builder)?;
            str_builder.append_option(field_value.as_str());
        }
        DataType::Int16 => {
            append_i64_to_builder::<i16, Int16Type>(field_value, builder)?;
        }
        DataType::Int32 => {
            append_i64_to_builder::<i32, Int32Type>(field_value, builder)?;
        }
        DataType::Int64 => {
            append_i64_to_builder::<i64, Int64Type>(field_value, builder)?;
        }
        DataType::Float32 => {
            let float_builder = downcast_builder::<Float32Builder>(builder)?;
            float_builder.append_option(field_value.as_f64().map(|f| f as f32));
        }
        DataType::Float64 => {
            let float_builder = downcast_builder::<Float64Builder>(builder)?;
            float_builder.append_option(field_value.as_f64());
        }
        DataType::Boolean => {
            let bool_builder = downcast_builder::<BooleanBuilder>(builder)?;
            bool_builder.append_option(field_value.as_bool());
        }
        DataType::Decimal128(_, _) => {
            let decimal_builder = downcast_builder::<Decimal128Builder>(builder)?;
            decimal_builder.append_option(
                field_value
                    .as_str()
                    .map(convert_string_to_decimal)
                    .transpose()?,
            );
        }
        DataType::Timestamp(unit, time_zone) => match (unit, time_zone) {
            (TimeUnit::Microsecond, None) => {
                append_i64_to_builder::<i64, TimestampMicrosecondType>(field_value, builder)?;
            }
            (TimeUnit::Millisecond, None) => {
                append_i64_to_builder::<i64, TimestampMillisecondType>(field_value, builder)?;
            }
            (TimeUnit::Microsecond, Some(_)) => {
                let tz_builder = downcast_builder::<TimestampMicrosecondBuilder>(builder)?;
                let time_micros = field_value
                    .as_str()
                    .map(|ts| {
                        // ts is in the format "2024-06-26T02:12:51.219026Z"
                        let parsed_timestamp: DateTime<Utc> =
                            ts.parse().context(UnableToParseTimestampSnafu)?;
                        Ok(parsed_timestamp.timestamp_micros())
                    })
                    .transpose()?;
                tz_builder.append_option(time_micros);
            }
            (TimeUnit::Millisecond, Some(_)) => {
                let tz_builder = downcast_builder::<TimestampMillisecondBuilder>(builder)?;
                let time_millis = field_value
                    .as_str()
                    .map(|ts| {
                        // ts is in the format "2024-06-26T02:12:51.219026Z"
                        let parsed_timestamp: DateTime<Utc> =
                            ts.parse().context(UnableToParseTimestampSnafu)?;
                        Ok(parsed_timestamp.timestamp_millis())
                    })
                    .transpose()?;
                tz_builder.append_option(time_millis);
            }
            _ => TimestampNotSupportedSnafu {
                unit: *unit,
                time_zone: time_zone.as_ref().map(|tz| tz.as_ref().to_string()),
            }
            .fail()?,
        },
        DataType::Time64(TimeUnit::Microsecond) => {
            if field_value.is_string() {
                let time_builder = downcast_builder::<Time64MicrosecondBuilder>(builder)?;
                let time_micros = field_value
                    .as_str()
                    .map(|ts| {
                        // ts is in the format "02:12:51.219026Z"
                        let parsed_time: NaiveTime = NaiveTime::parse_from_str(ts, "%H:%M:%S%.fZ")
                            .context(UnableToParseTimestampSnafu)?;
                        let microseconds: i64 = i64::from(parsed_time.num_seconds_from_midnight())
                            * 1_000_000
                            + i64::from(parsed_time.nanosecond() / 1_000);
                        Ok(microseconds)
                    })
                    .transpose()?;
                time_builder.append_option(time_micros);
            } else {
                append_i64_to_builder::<i64, Time64MicrosecondType>(field_value, builder)?;
            }
        }
        DataType::Date32 => {
            append_i64_to_builder::<i32, Date32Type>(field_value, builder)?;
        }
        DataType::List(field) => {
            let field_array: Option<&Vec<serde_json::Value>> = field_value.as_array();
            match field.data_type() {
                DataType::Utf8 => {
                    append_array_value_to_list_builder::<StringBuilder>(
                        field_array,
                        builder,
                        |str_builder, field_value| {
                            str_builder.append_option(field_value.as_str());
                        },
                    )?;
                }
                DataType::Boolean => {
                    append_array_value_to_list_builder::<BooleanBuilder>(
                        field_array,
                        builder,
                        |bool_builder, field_value| {
                            bool_builder.append_option(field_value.as_bool());
                        },
                    )?;
                }
                DataType::Int16 => {
                    append_array_value_to_list_builder::<Int16Builder>(
                        field_array,
                        builder,
                        |ts_builder, field_value| {
                            ts_builder.append_option(field_value.as_i64().map(|i| i as i16));
                        },
                    )?;
                }
                DataType::Int32 => {
                    append_array_value_to_list_builder::<Int32Builder>(
                        field_array,
                        builder,
                        |ts_builder, field_value| {
                            ts_builder.append_option(field_value.as_i64().map(|i| i as i32));
                        },
                    )?;
                }
                DataType::Int64 => {
                    append_array_value_to_list_builder::<Int64Builder>(
                        field_array,
                        builder,
                        |ts_builder, field_value| {
                            ts_builder.append_option(field_value.as_i64());
                        },
                    )?;
                }
                DataType::Float32 => {
                    append_array_value_to_list_builder::<Float32Builder>(
                        field_array,
                        builder,
                        |float_builder, field_value| {
                            float_builder.append_option(field_value.as_f64().map(|f| f as f32));
                        },
                    )?;
                }
                DataType::Float64 => {
                    append_array_value_to_list_builder::<Float64Builder>(
                        field_array,
                        builder,
                        |float_builder, field_value| {
                            float_builder.append_option(field_value.as_f64());
                        },
                    )?;
                }
                _ => {
                    ListDataTypeNotSupportedSnafu {
                        data_type: field.data_type().clone(),
                    }
                    .fail()?;
                }
            }
        }
        DataType::Binary => {
            let binary_builder = downcast_builder::<BinaryBuilder>(builder)?;
            let base64_decoded = field_value
                .as_str()
                .map(|v| BASE64_STANDARD.decode(v))
                .transpose()
                .context(UnableToDecodeBase64Snafu)?;
            binary_builder.append_option(base64_decoded);
        }
        _ => {
            DataTypeNotSupportedSnafu {
                data_type: field.data_type().clone(),
            }
            .fail()?;
        }
    }

    Ok(())
}

fn append_array_value_to_list_builder<T: ArrayBuilder>(
    field_array: Option<&Vec<serde_json::Value>>,
    builder: &mut dyn ArrayBuilder,
    append: impl Fn(&mut T, &serde_json::Value),
) -> Result<()> {
    let list_builder = downcast_builder::<ListBuilder<Box<dyn ArrayBuilder>>>(builder)?;
    let Some(field_array) = field_array else {
        list_builder.append_null();
        return Ok(());
    };

    let val_builder = downcast_builder::<T>(list_builder.values())?;

    for field_value in field_array {
        append(val_builder, field_value);
    }

    list_builder.append(true);

    Ok(())
}

fn append_i64_to_builder<CastTo, T: ArrowPrimitiveType<Native = CastTo>>(
    field_value: &serde_json::Value,
    builder: &mut dyn ArrayBuilder,
) -> Result<()>
where
    CastTo: TryFrom<i64> + Copy,
{
    let ts_builder = downcast_builder::<PrimitiveBuilder<T>>(builder)?;
    ts_builder.append_option(
        field_value
            .as_i64()
            .map(CastTo::try_from)
            .transpose()
            .map_err(|_| Error::UnableToConvertToI64)?,
    );
    Ok(())
}

pub(crate) fn downcast_builder<T: ArrayBuilder>(builder: &mut dyn ArrayBuilder) -> Result<&mut T> {
    let builder = builder
        .as_any_mut()
        .downcast_mut::<T>()
        .context(DowncastBuilderSnafu)?;
    Ok(builder)
}

fn convert_string_to_decimal(field_value: &str) -> Result<i128> {
    let mut decimal_bytes = BASE64_STANDARD
        .decode(field_value)
        .context(UnableToDecodeBase64Snafu)?;

    // Pad the bytes to 16 bytes, inserting 0s at the beginning
    while decimal_bytes.len() < 16 {
        decimal_bytes.insert(0, 0);
    }

    let decimal_slice: [u8; 16] = match decimal_bytes.try_into() {
        Ok(slice) => slice,
        Err(value) => {
            return Decimal128BytesNot16BytesSnafu { value }.fail();
        }
    };

    let decimal_i128 = i128::from_be_bytes(decimal_slice);

    Ok(decimal_i128)
}

fn convert_to_arrow_field(field: &ChangeEventField) -> Result<Field> {
    Ok(Field::new(
        field.field.as_deref().context(MissingFieldNameSnafu)?,
        convert_to_arrow_data_type(field)?,
        field.optional,
    ))
}

fn convert_to_arrow_data_type(field: &ChangeEventField) -> Result<DataType> {
    let data_type = match field.field_type.as_str() {
        "string" => match field.name.as_deref() {
            Some("io.debezium.time.ZonedTime") => DataType::Time64(TimeUnit::Microsecond),
            Some("io.debezium.time.ZonedTimestamp") => {
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
            }
            _ => DataType::Utf8,
        },
        "int16" => DataType::Int16,
        "int32" => match field.name.as_deref() {
            Some("io.debezium.time.Date") => DataType::Date32,
            Some("io.debezium.time.Time") => DataType::Time32(TimeUnit::Millisecond),
            Some("io.debezium.time.Timestamp") => DataType::Timestamp(TimeUnit::Millisecond, None),
            _ => DataType::Int32,
        },
        "int64" => match field.name.as_deref() {
            Some("io.debezium.time.MicroTime") => DataType::Time64(TimeUnit::Microsecond),
            Some("io.debezium.time.MicroTimestamp") => {
                DataType::Timestamp(TimeUnit::Microsecond, None)
            }
            _ => DataType::Int64,
        },
        "boolean" => DataType::Boolean,
        "float" => DataType::Float32,
        "double" => DataType::Float64,
        "bytes" => match field.name.as_deref() {
            Some("org.apache.kafka.connect.data.Decimal") => {
                let parameters = field
                    .parameters
                    .as_ref()
                    .context(MissingParametersForDecimalSnafu)?;

                let precision = parameters
                    .get("connect.decimal.precision")
                    .context(MissingPrecisionForDecimalSnafu)?
                    .parse::<u8>()
                    .context(UnableToParsePrecisionSnafu)?;
                let scale = parameters
                    .get("scale")
                    .context(MissingScaleForDecimalSnafu)?
                    .parse::<i8>()
                    .context(UnableToParseScaleSnafu)?;
                if precision <= 38 {
                    DataType::Decimal128(precision, scale)
                } else {
                    DataType::Decimal256(precision, scale)
                }
            }
            _ => DataType::Binary,
        },
        "array" => {
            let items = field.items.as_ref().context(MissingItemsForArraySnafu)?;
            let item_type = convert_to_arrow_data_type(items)?;
            DataType::List(Arc::new(Field::new("item", item_type, items.optional)))
        }
        _ => DebeziumFieldNotSupportedSnafu {
            field_type: field.field_type.clone(),
        }
        .fail()?,
    };

    Ok(data_type)
}
