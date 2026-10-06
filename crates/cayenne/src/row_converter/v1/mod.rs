/*
Copyright 2026 The Spice.ai OSS Authors

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

//! Version 1 of the row format — byte-identical to Apache Arrow `arrow-row` 58.3.0.
//!
//! [`build_codec`] is the only version-specific piece: it maps each supported primary-key
//! [`DataType`] to a [`ColumnCodec`]. A future version reuses this for unchanged types and only
//! overrides the entry for a type whose encoding it optimizes.

mod fixed;
mod variable;

use arrow::datatypes::{
    Date32Type, Date64Type, Decimal128Type, Decimal256Type, Float32Type, Float64Type, Int8Type,
    Int16Type, Int32Type, Int64Type, Time32MillisecondType, Time32SecondType,
    Time64MicrosecondType, Time64NanosecondType, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType, UInt8Type, UInt16Type,
    UInt32Type, UInt64Type,
};
use arrow_schema::{ArrowError, DataType, TimeUnit};

use self::fixed::{BooleanCodec, PrimitiveCodec};
use self::variable::{VarKind, VariableCodec};
use crate::row_converter::SortField;
use crate::row_converter::codec::ColumnCodec;

/// Build the V1 column codec for `field`, or an error if its type cannot be a Cayenne primary key.
pub(crate) fn build_codec(field: &SortField) -> Result<Box<dyn ColumnCodec>, ArrowError> {
    let opts = field.options;
    let data_type = &field.data_type;

    macro_rules! primitive {
        ($t:ty) => {
            Ok(Box::new(PrimitiveCodec::<$t>::new(data_type.clone(), opts))
                as Box<dyn ColumnCodec>)
        };
    }
    macro_rules! variable {
        ($kind:expr) => {
            Ok(Box::new(VariableCodec::new($kind, opts)) as Box<dyn ColumnCodec>)
        };
    }

    match data_type {
        DataType::Boolean => Ok(Box::new(BooleanCodec::new(opts)) as Box<dyn ColumnCodec>),

        DataType::Int8 => primitive!(Int8Type),
        DataType::Int16 => primitive!(Int16Type),
        DataType::Int32 => primitive!(Int32Type),
        DataType::Int64 => primitive!(Int64Type),
        DataType::UInt8 => primitive!(UInt8Type),
        DataType::UInt16 => primitive!(UInt16Type),
        DataType::UInt32 => primitive!(UInt32Type),
        DataType::UInt64 => primitive!(UInt64Type),
        DataType::Float32 => primitive!(Float32Type),
        DataType::Float64 => primitive!(Float64Type),

        DataType::Date32 => primitive!(Date32Type),
        DataType::Date64 => primitive!(Date64Type),
        DataType::Time32(TimeUnit::Second) => primitive!(Time32SecondType),
        DataType::Time32(TimeUnit::Millisecond) => primitive!(Time32MillisecondType),
        DataType::Time64(TimeUnit::Microsecond) => primitive!(Time64MicrosecondType),
        DataType::Time64(TimeUnit::Nanosecond) => primitive!(Time64NanosecondType),
        DataType::Timestamp(TimeUnit::Second, _) => primitive!(TimestampSecondType),
        DataType::Timestamp(TimeUnit::Millisecond, _) => primitive!(TimestampMillisecondType),
        DataType::Timestamp(TimeUnit::Microsecond, _) => primitive!(TimestampMicrosecondType),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => primitive!(TimestampNanosecondType),

        DataType::Decimal128(_, _) => primitive!(Decimal128Type),
        DataType::Decimal256(_, _) => primitive!(Decimal256Type),

        DataType::Binary => variable!(VarKind::Binary),
        DataType::LargeBinary => variable!(VarKind::LargeBinary),
        DataType::Utf8 => variable!(VarKind::Utf8),
        DataType::LargeUtf8 => variable!(VarKind::LargeUtf8),
        DataType::BinaryView => variable!(VarKind::BinaryView),
        DataType::Utf8View => variable!(VarKind::Utf8View),

        other => Err(ArrowError::NotYetImplemented(format!(
            "cayenne row_converter: unsupported primary key type: {other}"
        ))),
    }
}
