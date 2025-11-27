use datafusion::arrow::{
    array::{
        types::Int8Type, ArrayBuilder, BinaryBuilder, BooleanBuilder, Date32Builder, Date64Builder,
        Decimal128Builder, Decimal256Builder, FixedSizeBinaryBuilder, FixedSizeListBuilder,
        Float32Builder, Float64Builder, Int16Builder, Int32Builder, Int64Builder, Int8Builder,
        IntervalMonthDayNanoBuilder, LargeBinaryBuilder, LargeStringBuilder, ListBuilder,
        NullBuilder, StringBuilder, StringDictionaryBuilder, StructBuilder,
        Time64NanosecondBuilder, TimestampMicrosecondBuilder, TimestampMillisecondBuilder,
        TimestampNanosecondBuilder, TimestampSecondBuilder, UInt16Builder, UInt32Builder,
        UInt64Builder, UInt8Builder,
    },
    datatypes::{DataType, TimeUnit, UInt16Type},
};

pub fn map_data_type_to_array_builder_optional(
    data_type: Option<&DataType>,
) -> Option<Box<dyn ArrayBuilder>> {
    match data_type {
        Some(data_type) => Some(map_data_type_to_array_builder(data_type)),
        None => None,
    }
}

#[allow(clippy::too_many_lines)]
pub fn map_data_type_to_array_builder(data_type: &DataType) -> Box<dyn ArrayBuilder> {
    match data_type {
        DataType::Int8 => Box::new(Int8Builder::new()),
        DataType::Int16 => Box::new(Int16Builder::new()),
        DataType::Int32 => Box::new(Int32Builder::new()),
        DataType::Int64 => Box::new(Int64Builder::new()),
        DataType::UInt8 => Box::new(UInt8Builder::new()),
        DataType::UInt16 => Box::new(UInt16Builder::new()),
        DataType::UInt32 => Box::new(UInt32Builder::new()),
        DataType::UInt64 => Box::new(UInt64Builder::new()),
        DataType::Float32 => Box::new(Float32Builder::new()),
        DataType::Float64 => Box::new(Float64Builder::new()),
        DataType::Utf8 => Box::new(StringBuilder::new()),
        DataType::LargeUtf8 => Box::new(LargeStringBuilder::new()),
        DataType::Boolean => Box::new(BooleanBuilder::new()),
        DataType::Binary => Box::new(BinaryBuilder::new()),
        DataType::LargeBinary => Box::new(LargeBinaryBuilder::new()),
        DataType::Interval(_) => Box::new(IntervalMonthDayNanoBuilder::new()),
        DataType::Decimal128(precision, scale) => Box::new(
            Decimal128Builder::new()
                .with_precision_and_scale(*precision, *scale)
                .unwrap_or_default(),
        ),
        DataType::Decimal256(precision, scale) => Box::new(
            Decimal256Builder::new()
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
        DataType::Dictionary(ref key_type, ref value_type) => match (&**key_type, &**value_type) {
            (DataType::Int8, DataType::Utf8) => {
                Box::new(StringDictionaryBuilder::<Int8Type>::new())
            }
            (DataType::UInt16, DataType::Utf8) => {
                Box::new(StringDictionaryBuilder::<UInt16Type>::new())
            }
            _ => unimplemented!("Unimplemented dictionary type"),
        },
        DataType::Date32 => Box::new(Date32Builder::new()),
        DataType::Date64 => Box::new(Date64Builder::new()),
        // For time format, always use nanosecond
        DataType::Time64(TimeUnit::Nanosecond) => Box::new(Time64NanosecondBuilder::new()),
        DataType::FixedSizeBinary(s) => Box::new(FixedSizeBinaryBuilder::new(*s)),
        // We can't recursively call map_data_type_to_array_builder here because downcasting will not work if the
        // values_builder is boxed.
        DataType::List(values_field) | DataType::LargeList(values_field) => {
            match values_field.data_type() {
                DataType::Int8 => Box::new(ListBuilder::new(Int8Builder::new())),
                DataType::Int16 => Box::new(ListBuilder::new(Int16Builder::new())),
                DataType::Int32 => Box::new(ListBuilder::new(Int32Builder::new())),
                DataType::Int64 => Box::new(ListBuilder::new(Int64Builder::new())),
                DataType::UInt32 => Box::new(ListBuilder::new(UInt32Builder::new())),
                DataType::Float32 => Box::new(ListBuilder::new(Float32Builder::new())),
                DataType::Float64 => Box::new(ListBuilder::new(Float64Builder::new())),
                DataType::Utf8 => Box::new(ListBuilder::new(StringBuilder::new())),
                DataType::Boolean => Box::new(ListBuilder::new(BooleanBuilder::new())),
                DataType::Binary => Box::new(ListBuilder::new(BinaryBuilder::new())),
                _ => unimplemented!("Unsupported list value data type {:?}", data_type),
            }
        }
        DataType::FixedSizeList(values_field, size) => match values_field.data_type() {
            DataType::Int8 => Box::new(FixedSizeListBuilder::new(
                Int8Builder::new(),
                size.to_owned(),
            )),
            DataType::Int16 => Box::new(FixedSizeListBuilder::new(
                Int16Builder::new(),
                size.to_owned(),
            )),
            DataType::Int32 => Box::new(FixedSizeListBuilder::new(
                Int32Builder::new(),
                size.to_owned(),
            )),
            DataType::Int64 => Box::new(FixedSizeListBuilder::new(
                Int64Builder::new(),
                size.to_owned(),
            )),
            DataType::UInt32 => Box::new(FixedSizeListBuilder::new(
                UInt32Builder::new(),
                size.to_owned(),
            )),
            DataType::Float32 => Box::new(FixedSizeListBuilder::new(
                Float32Builder::new(),
                size.to_owned(),
            )),
            DataType::Float64 => Box::new(FixedSizeListBuilder::new(
                Float64Builder::new(),
                size.to_owned(),
            )),
            DataType::Utf8 => Box::new(FixedSizeListBuilder::new(
                StringBuilder::new(),
                size.to_owned(),
            )),
            DataType::Boolean => Box::new(FixedSizeListBuilder::new(
                BooleanBuilder::new(),
                size.to_owned(),
            )),
            _ => unimplemented!("Unsupported list value data type {:?}", data_type),
        },
        DataType::Null => Box::new(NullBuilder::new()),
        DataType::Struct(fields) => {
            let mut field_builders = Vec::with_capacity(fields.len());
            for field in fields {
                field_builders.push(map_data_type_to_array_builder(field.data_type()));
            }
            Box::new(StructBuilder::new(fields.clone(), field_builders))
        }
        _ => unimplemented!("Unsupported data type {:?}", data_type),
    }
}
