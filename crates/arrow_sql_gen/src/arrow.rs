use arrow::{array::ArrayBuilder, datatypes::DataType};

pub fn map_data_type_to_array_builder_optional(
    data_type: Option<&DataType>,
) -> Option<Box<dyn ArrayBuilder>> {
    match data_type {
        Some(data_type) => Some(map_data_type_to_array_builder(data_type)),
        None => None,
    }
}

pub fn map_data_type_to_array_builder(data_type: &DataType) -> Box<dyn ArrayBuilder> {
    match data_type {
        DataType::Int16 => Box::new(arrow::array::Int16Builder::new()),
        DataType::Int32 => Box::new(arrow::array::Int32Builder::new()),
        DataType::Int64 => Box::new(arrow::array::Int64Builder::new()),
        DataType::Float32 => Box::new(arrow::array::Float32Builder::new()),
        DataType::Float64 => Box::new(arrow::array::Float64Builder::new()),
        DataType::Utf8 => Box::new(arrow::array::StringBuilder::new()),
        DataType::Boolean => Box::new(arrow::array::BooleanBuilder::new()),
        DataType::Decimal128(precision, scale) => Box::new(
            arrow::array::Decimal128Builder::new()
                .with_precision_and_scale(*precision, *scale)
                .unwrap_or_default(),
        ),
        DataType::Timestamp(time_unit, time_zone) => match time_unit {
            arrow::datatypes::TimeUnit::Microsecond => Box::new(
                arrow::array::TimestampMicrosecondBuilder::new()
                    .with_timezone_opt(time_zone.clone()),
            ),
            arrow::datatypes::TimeUnit::Second => Box::new(
                arrow::array::TimestampSecondBuilder::new().with_timezone_opt(time_zone.clone()),
            ),
            arrow::datatypes::TimeUnit::Millisecond => Box::new(
                arrow::array::TimestampMillisecondBuilder::new()
                    .with_timezone_opt(time_zone.clone()),
            ),
            arrow::datatypes::TimeUnit::Nanosecond => Box::new(
                arrow::array::TimestampNanosecondBuilder::new()
                    .with_timezone_opt(time_zone.clone()),
            ),
        },
        _ => unimplemented!("Unsupported data type {:?}", data_type),
    }
}
