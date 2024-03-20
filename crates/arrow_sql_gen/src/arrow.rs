use arrow::{
    array::{
        ArrayBuilder, BooleanBuilder, Decimal128Builder, Float32Builder, Float64Builder,
        Int16Builder, Int32Builder, Int64Builder, StringBuilder, TimestampMicrosecondBuilder,
        TimestampMillisecondBuilder, TimestampNanosecondBuilder, TimestampSecondBuilder,
    },
    datatypes::{DataType, TimeUnit},
};

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
        DataType::Int16 => Box::new(Int16Builder::new()),
        DataType::Int32 => Box::new(Int32Builder::new()),
        DataType::Int64 => Box::new(Int64Builder::new()),
        DataType::Float32 => Box::new(Float32Builder::new()),
        DataType::Float64 => Box::new(Float64Builder::new()),
        DataType::Utf8 => Box::new(StringBuilder::new()),
        DataType::Boolean => Box::new(BooleanBuilder::new()),
        DataType::Decimal128(precision, scale) => Box::new(
            Decimal128Builder::new()
                .with_precision_and_scale(*precision, *scale)
                .unwrap_or_default(),
        ),
        DataType::Timestamp(time_unit, time_zone) => match time_unit {
            TimeUnit::Microsecond => {
                Box::new(TimestampMicrosecondBuilder::new().with_timezone_opt(time_zone.clone()))
            }
            TimeUnit::Second => {
                Box::new(TimestampSecondBuilder::new().with_timezone_opt(time_zone.clone()))
            }
            TimeUnit::Millisecond => {
                Box::new(TimestampMillisecondBuilder::new().with_timezone_opt(time_zone.clone()))
            }
            TimeUnit::Nanosecond => {
                Box::new(TimestampNanosecondBuilder::new().with_timezone_opt(time_zone.clone()))
            }
        },
        // We can't recursively call map_data_type_to_array_builder here because downcasting will not work if the
        // values_builder is boxed.
        DataType::List(values_field) => match values_field.data_type() {
            DataType::Int16 => Box::new(arrow::array::ListBuilder::new(
                arrow::array::Int16Builder::new(),
            )),
            DataType::Int32 => Box::new(arrow::array::ListBuilder::new(
                arrow::array::Int32Builder::new(),
            )),
            DataType::Int64 => Box::new(arrow::array::ListBuilder::new(
                arrow::array::Int64Builder::new(),
            )),
            DataType::Float32 => Box::new(arrow::array::ListBuilder::new(
                arrow::array::Float32Builder::new(),
            )),
            DataType::Float64 => Box::new(arrow::array::ListBuilder::new(
                arrow::array::Float64Builder::new(),
            )),
            DataType::Utf8 => Box::new(arrow::array::ListBuilder::new(
                arrow::array::StringBuilder::new(),
            )),
            DataType::Boolean => Box::new(arrow::array::ListBuilder::new(
                arrow::array::BooleanBuilder::new(),
            )),
            _ => unimplemented!("Unsupported list value data type {:?}", data_type),
        },
        _ => unimplemented!("Unsupported data type {:?}", data_type),
    }
}
