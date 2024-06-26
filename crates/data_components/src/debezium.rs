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

use change_event::Field;
use snafu::prelude::*;
use std::sync::Arc;

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
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub mod change_event;

#[cfg(test)]
mod tests;

pub fn convert_fields_to_arrow_schema(fields: &[Field]) -> Result<arrow::datatypes::Schema> {
    let arrow_fields = fields
        .iter()
        .map(convert_to_arrow_field)
        .collect::<Result<Vec<arrow::datatypes::Field>>>()?;

    Ok(arrow::datatypes::Schema::new(arrow_fields))
}

fn convert_to_arrow_field(field: &Field) -> Result<arrow::datatypes::Field> {
    Ok(arrow::datatypes::Field::new(
        field.field.as_deref().context(MissingFieldNameSnafu)?,
        convert_to_arrow_data_type(field)?,
        field.optional,
    ))
}

fn convert_to_arrow_data_type(field: &Field) -> Result<arrow::datatypes::DataType> {
    let data_type = match field.field_type.as_str() {
        "string" => match field.name.as_deref() {
            Some("io.debezium.time.ZonedTime") => {
                arrow::datatypes::DataType::Time64(arrow::datatypes::TimeUnit::Microsecond)
            }
            Some("io.debezium.time.ZonedTimestamp") => arrow::datatypes::DataType::Timestamp(
                arrow::datatypes::TimeUnit::Microsecond,
                Some("UTC".into()),
            ),
            _ => arrow::datatypes::DataType::Utf8,
        },
        "int16" => arrow::datatypes::DataType::Int16,
        "int32" => match field.name.as_deref() {
            Some("io.debezium.time.Date") => arrow::datatypes::DataType::Date32,
            Some("io.debezium.time.Time") => {
                arrow::datatypes::DataType::Time32(arrow::datatypes::TimeUnit::Millisecond)
            }
            Some("io.debezium.time.Timestamp") => {
                arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None)
            }
            _ => arrow::datatypes::DataType::Int32,
        },
        "int64" => match field.name.as_deref() {
            Some("io.debezium.time.MicroTime") => {
                arrow::datatypes::DataType::Time64(arrow::datatypes::TimeUnit::Microsecond)
            }
            Some("io.debezium.time.MicroTimestamp") => {
                arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
            }
            _ => arrow::datatypes::DataType::Int64,
        },
        "boolean" => arrow::datatypes::DataType::Boolean,
        "float" => arrow::datatypes::DataType::Float32,
        "double" => arrow::datatypes::DataType::Float64,
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
                    arrow::datatypes::DataType::Decimal128(precision, scale)
                } else {
                    arrow::datatypes::DataType::Decimal256(precision, scale)
                }
            }
            _ => arrow::datatypes::DataType::Binary,
        },
        "array" => {
            let items = field.items.as_ref().context(MissingItemsForArraySnafu)?;
            let item_type = convert_to_arrow_data_type(items)?;
            arrow::datatypes::DataType::List(Arc::new(arrow::datatypes::Field::new(
                "item",
                item_type,
                items.optional,
            )))
        }
        _ => unimplemented!("{} not implemented", field.field_type),
    };

    Ok(data_type)
}
