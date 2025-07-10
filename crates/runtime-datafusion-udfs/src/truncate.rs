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

use std::num::TryFromIntError;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, Decimal128Array, Decimal256Array, Int8Array, Int16Array, Int32Array, Int64Array,
    StringArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::compute::binary;
use arrow::compute::kernels::substring::{substring, substring_by_char};
use arrow::datatypes::{DataType, i256};
use datafusion::common::DataFusionError;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::scalar::ScalarValue;
use snafu::{ResultExt, Snafu, ensure};
use tract_core::num_traits::Num;

// Maximum truncation width or length, chosen to prevent overflow or excessive memory usage.
const MAX_TRUNCATE_WIDTH: i64 = i64::MAX / 2;

#[derive(Debug, Snafu)]
pub enum TruncateError {
    #[snafu(display(
        "Invalid width value: '{width}'. Must be a positive integer less than or equal to {MAX_TRUNCATE_WIDTH}"
    ))]
    InvalidWidthValue { width: i64 },

    #[snafu(display("Truncation width must be a positive Int64, got {width_datatype}"))]
    InvalidWidthDataType { width_datatype: DataType },

    #[snafu(display("Expected exactly two arguments, got {count}"))]
    InvalidArgumentCount { count: usize },

    #[snafu(display(
        "Second argument must be Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64, Decimal128, Decimal256, Utf8, or Binary, got {data_type}"
    ))]
    InvalidSecondArgType { data_type: DataType },

    #[snafu(display("Failed to cast the width argument: {source}"))]
    WidthCastingFailed { source: TryFromIntError },
}

impl From<TruncateError> for DataFusionError {
    fn from(val: TruncateError) -> Self {
        DataFusionError::External(val.to_string().into())
    }
}

#[derive(Debug)]
pub struct Truncate {
    signature: Signature,
}

impl Default for Truncate {
    fn default() -> Self {
        Self::new()
    }
}

impl Truncate {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl Truncate {
    fn validate_args(args: Vec<ColumnarValue>) -> Result<(i64, ColumnarValue), TruncateError> {
        if args.len() != 2 {
            return Err(TruncateError::InvalidArgumentCount { count: args.len() });
        }

        let count = args.len();
        let mut args = args.into_iter();
        match (args.next(), args.next()) {
            (Some(ColumnarValue::Scalar(ScalarValue::Int64(Some(width)))), Some(arg)) => {
                ensure!(
                    width > 0 && width <= MAX_TRUNCATE_WIDTH,
                    InvalidWidthValueSnafu { width }
                );
                Ok((width, arg))
            }
            (Some(width), Some(_)) => {
                let width_datatype = width.data_type();
                Err(TruncateError::InvalidWidthDataType { width_datatype })
            }
            _ => Err(TruncateError::InvalidArgumentCount { count }),
        }
    }
}

impl ScalarUDFImpl for Truncate {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "truncate"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType, DataFusionError> {
        let count = arg_types.len();
        ensure!(count == 2, InvalidArgumentCountSnafu { count });

        let width_datatype = arg_types[0].clone();

        ensure!(
            matches!(width_datatype, DataType::Int64),
            InvalidWidthDataTypeSnafu { width_datatype }
        );

        match &arg_types[1] {
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
            | DataType::Utf8
            | DataType::Binary => Ok(arg_types[1].clone()),
            _ => Err(TruncateError::InvalidSecondArgType {
                data_type: arg_types[1].clone(),
            }
            .into()),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue, DataFusionError> {
        let (width, arg) = Self::validate_args(args.args)?;

        tracing::trace!("Computing truncate with width: {width}");

        match arg {
            ColumnarValue::Scalar(scalar) => {
                let result = compute_truncate_scalar(scalar, width)?;
                Ok(ColumnarValue::Scalar(result))
            }
            ColumnarValue::Array(array) => {
                let result = compute_truncate_array(&array, width)?;
                Ok(ColumnarValue::Array(result))
            }
        }
    }
}

macro_rules! truncate_numeric_array {
    ($ARRAY:expr, $WIDTH:expr, $ARRAY_TYPE:ty, $SCALAR_TYPE:ty) => {{
        let casted_array = $ARRAY
            .as_any()
            .downcast_ref::<$ARRAY_TYPE>()
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "Failed to downcast to {}",
                    stringify!($ARRAY_TYPE)
                ))
            })?;
        let width: $SCALAR_TYPE = $WIDTH.try_into().context(WidthCastingFailedSnafu)?;
        let width_array = <$ARRAY_TYPE>::from_value(width, $ARRAY.len());
        let result: $ARRAY_TYPE =
            binary(casted_array, &width_array, |v, w| v - (((v % w) + w) % w))?;
        Ok(Arc::new(result) as ArrayRef)
    }};
}

fn compute_truncate_scalar(
    scalar: ScalarValue,
    width: i64,
) -> Result<ScalarValue, DataFusionError> {
    if scalar.is_null() {
        return Ok(scalar);
    }

    match scalar {
        ScalarValue::Int8(Some(v)) => Ok(ScalarValue::Int8(Some(truncate_numeric(v, width)?))),
        ScalarValue::Int16(Some(v)) => Ok(ScalarValue::Int16(Some(truncate_numeric(v, width)?))),
        ScalarValue::Int32(Some(v)) => Ok(ScalarValue::Int32(Some(truncate_numeric(v, width)?))),
        ScalarValue::Int64(Some(v)) => {
            let result = v - (((v % width) + width) % width);
            Ok(ScalarValue::Int64(Some(result)))
        }
        ScalarValue::UInt8(Some(v)) => Ok(ScalarValue::UInt8(Some(truncate_numeric(v, width)?))),
        ScalarValue::UInt16(Some(v)) => Ok(ScalarValue::UInt16(Some(truncate_numeric(v, width)?))),
        ScalarValue::UInt32(Some(v)) => Ok(ScalarValue::UInt32(Some(truncate_numeric(v, width)?))),
        ScalarValue::UInt64(Some(v)) => Ok(ScalarValue::UInt64(Some(truncate_numeric(v, width)?))),
        ScalarValue::Decimal128(Some(v), p, s) => {
            let width = i128::from(width);
            let result = v - (((v % width) + width) % width);
            Ok(ScalarValue::Decimal128(Some(result), p, s))
        }
        ScalarValue::Decimal256(Some(v), p, s) => {
            let width = i256::from_i128(i128::from(width));
            let result = v - (((v % width) + width) % width);
            Ok(ScalarValue::Decimal256(Some(result), p, s))
        }
        ScalarValue::Utf8(Some(mut v)) => {
            let new_len = usize::try_from(width).context(WidthCastingFailedSnafu)?;
            v.truncate(new_len);
            Ok(ScalarValue::Utf8(Some(v)))
        }
        ScalarValue::Binary(Some(v)) => {
            let n = usize::try_from(width).context(WidthCastingFailedSnafu)?;
            let truncated = v.iter().take(n).copied().collect::<Vec<u8>>();
            Ok(ScalarValue::Binary(Some(truncated)))
        }
        _ => Err(TruncateError::InvalidSecondArgType {
            data_type: scalar.data_type(),
        }
        .into()),
    }
}

fn truncate_numeric<V, W>(v: V, w: W) -> Result<V, TruncateError>
where
    V: Num + Copy + TryFrom<W, Error = TryFromIntError>,
{
    let w = V::try_from(w).context(WidthCastingFailedSnafu)?;
    Ok(v - (((v % w) + w) % w))
}

fn compute_truncate_array(array: &ArrayRef, width: i64) -> Result<ArrayRef, DataFusionError> {
    match array.data_type() {
        DataType::Int8 => truncate_numeric_array!(array, width, Int8Array, i8),
        DataType::Int16 => truncate_numeric_array!(array, width, Int16Array, i16),
        DataType::Int32 => truncate_numeric_array!(array, width, Int32Array, i32),
        DataType::Int64 => {
            let casted_array = array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                DataFusionError::Internal("Failed to downcast to Int64Array".into())
            })?;
            let width_array = Int64Array::from_value(width, array.len());
            let result: Int64Array =
                binary(casted_array, &width_array, |v, w| v - (((v % w) + w) % w))?;
            Ok(Arc::new(result) as ArrayRef)
        }
        DataType::UInt8 => truncate_numeric_array!(array, width, UInt8Array, u8),
        DataType::UInt16 => truncate_numeric_array!(array, width, UInt16Array, u16),
        DataType::UInt32 => truncate_numeric_array!(array, width, UInt32Array, u32),
        DataType::UInt64 => truncate_numeric_array!(array, width, UInt64Array, u64),
        DataType::Decimal128(_, _) => {
            let casted_array = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| {
                    DataFusionError::Internal("Failed to downcast to Decimal128Array".into())
                })?;
            let width = i128::from(width);
            let width_array = Decimal128Array::from_value(width, array.len());
            let result: Decimal128Array =
                binary(casted_array, &width_array, |v, w| v - (((v % w) + w) % w))?;
            Ok(Arc::new(result))
        }
        DataType::Decimal256(_, _) => {
            let casted_array = array
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .ok_or_else(|| {
                    DataFusionError::Internal("Failed to downcast to Decimal256Array".into())
                })?;
            let width = i256::from_i128(i128::from(width));
            let width_array = Decimal256Array::from_value(width, array.len());
            let result: Decimal256Array =
                binary(casted_array, &width_array, |v, w| v - (((v % w) + w) % w))?;
            Ok(Arc::new(result))
        }
        DataType::Binary => {
            let length = u64::try_from(width).context(WidthCastingFailedSnafu)?;
            let result = substring(&array, 0, Some(length))?;
            Ok(result)
        }
        DataType::Utf8 => {
            let casted_array = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal("Failed to downcast to StringArray".into())
                })?;
            let length = u64::try_from(width).context(WidthCastingFailedSnafu)?;
            let result = substring_by_char(casted_array, 0, Some(length))?;
            Ok(Arc::new(result))
        }
        _ => Err(TruncateError::InvalidSecondArgType {
            data_type: array.data_type().clone(),
        }
        .into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        Array as _, BinaryArray, Decimal128Array, Int8Array, Int16Array, Int32Array, Int64Array,
        StringArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
    };
    use datafusion::arrow::datatypes::DataType;

    #[test]
    fn test_truncate_int8_array() {
        let udf = Truncate::new();
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Int64(Some(10))),
                ColumnarValue::Array(Arc::new(Int8Array::from(vec![101, -1, 0]))),
            ],
            number_rows: 3,
            return_type: &DataType::Int8,
        };
        let result = udf.invoke_with_args(args).expect("invoke UDF");
        if let ColumnarValue::Array(array) = result {
            let int_array = array
                .as_any()
                .downcast_ref::<Int8Array>()
                .expect("downcast to Int8Array");
            assert_eq!(int_array.len(), 3);
            assert_eq!(int_array.value(0), 100, "Expected truncate(10, 101) = 100");
            assert_eq!(int_array.value(1), -10, "Expected truncate(10, -1) = -10");
            assert_eq!(int_array.value(2), 0, "Expected truncate(10, 0) = 0");
        } else {
            panic!("Expected Int8 array");
        }
    }

    #[test]
    fn test_truncate_int16_array() {
        let udf = Truncate::new();
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Int64(Some(100))),
                ColumnarValue::Array(Arc::new(Int16Array::from(vec![1234, -567, 99]))),
            ],
            number_rows: 3,
            return_type: &DataType::Int16,
        };
        let result = udf.invoke_with_args(args).expect("invoke UDF");
        if let ColumnarValue::Array(array) = result {
            let int_array = array
                .as_any()
                .downcast_ref::<Int16Array>()
                .expect("downcast to Int16Array");
            assert_eq!(int_array.len(), 3);
            assert_eq!(
                int_array.value(0),
                1200,
                "Expected truncate(100, 1234) = 1200"
            );
            assert_eq!(
                int_array.value(1),
                -600,
                "Expected truncate(100, -567) = -600"
            );
            assert_eq!(int_array.value(2), 0, "Expected truncate(100, 99) = 0");
        } else {
            panic!("Expected Int16 array");
        }
    }

    #[test]
    fn test_truncate_int32_array() {
        let udf = Truncate::new();
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Int64(Some(10))),
                ColumnarValue::Array(Arc::new(Int32Array::from(vec![101, -1, 0]))),
            ],
            number_rows: 3,
            return_type: &DataType::Int32,
        };
        let result = udf.invoke_with_args(args).expect("invoke UDF");
        if let ColumnarValue::Array(array) = result {
            let int_array = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("downcast to Int32Array");
            assert_eq!(int_array.len(), 3);
            assert_eq!(int_array.value(0), 100, "Expected truncate(10, 101) = 100");
            assert_eq!(int_array.value(1), -10, "Expected truncate(10, -1) = -10");
            assert_eq!(int_array.value(2), 0, "Expected truncate(10, 0) = 0");
        } else {
            panic!("Expected Int32 array");
        }
    }

    #[test]
    fn test_truncate_int64_array() {
        let udf = Truncate::new();
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Int64(Some(1000))),
                ColumnarValue::Array(Arc::new(Int64Array::from(vec![1234, -5678, 999]))),
            ],
            number_rows: 3,
            return_type: &DataType::Int64,
        };
        let result = udf.invoke_with_args(args).expect("invoke UDF");
        if let ColumnarValue::Array(array) = result {
            let int_array = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("downcast to Int64Array");
            assert_eq!(int_array.len(), 3);
            assert_eq!(
                int_array.value(0),
                1000,
                "Expected truncate(1000, 1234) = 1000"
            );
            assert_eq!(
                int_array.value(1),
                -6000,
                "Expected truncate(1000, -5678) = -6000"
            );
            assert_eq!(int_array.value(2), 0, "Expected truncate(1000, 999) = 0");
        } else {
            panic!("Expected Int64 array");
        }
    }

    #[test]
    fn test_truncate_uint8_array() {
        let udf = Truncate::new();
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Int64(Some(10))),
                ColumnarValue::Array(Arc::new(UInt8Array::from(vec![101, 1, 0]))),
            ],
            number_rows: 3,
            return_type: &DataType::UInt8,
        };
        let result = udf.invoke_with_args(args).expect("invoke UDF");
        if let ColumnarValue::Array(array) = result {
            let int_array = array
                .as_any()
                .downcast_ref::<UInt8Array>()
                .expect("downcast to UInt8Array");
            assert_eq!(int_array.len(), 3);
            assert_eq!(int_array.value(0), 100, "Expected truncate(10, 101) = 100");
            assert_eq!(int_array.value(1), 0, "Expected truncate(10, 1) = 0");
            assert_eq!(int_array.value(2), 0, "Expected truncate(10, 0) = 0");
        } else {
            panic!("Expected UInt8 array");
        }
    }

    #[test]
    fn test_truncate_uint16_array() {
        let udf = Truncate::new();
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Int64(Some(100))),
                ColumnarValue::Array(Arc::new(UInt16Array::from(vec![1234, 567, 99]))),
            ],
            number_rows: 3,
            return_type: &DataType::UInt16,
        };
        let result = udf.invoke_with_args(args).expect("invoke UDF");
        if let ColumnarValue::Array(array) = result {
            let int_array = array
                .as_any()
                .downcast_ref::<UInt16Array>()
                .expect("downcast to UInt16Array");
            assert_eq!(int_array.len(), 3);
            assert_eq!(
                int_array.value(0),
                1200,
                "Expected truncate(100, 1234) = 1200"
            );
            assert_eq!(int_array.value(1), 500, "Expected truncate(100, 567) = 500");
            assert_eq!(int_array.value(2), 0, "Expected truncate(100, 99) = 0");
        } else {
            panic!("Expected UInt16 array");
        }
    }

    #[test]
    fn test_truncate_uint32_array() {
        let udf = Truncate::new();
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Int64(Some(1000))),
                ColumnarValue::Array(Arc::new(UInt32Array::from(vec![1234, 5678, 999]))),
            ],
            number_rows: 3,
            return_type: &DataType::UInt32,
        };
        let result = udf.invoke_with_args(args).expect("invoke UDF");
        if let ColumnarValue::Array(array) = result {
            let int_array = array
                .as_any()
                .downcast_ref::<UInt32Array>()
                .expect("downcast to UInt32Array");
            assert_eq!(int_array.len(), 3);
            assert_eq!(
                int_array.value(0),
                1000,
                "Expected truncate(1000, 1234) = 1000"
            );
            assert_eq!(
                int_array.value(1),
                5000,
                "Expected truncate(1000, 5678) = 5000"
            );
            assert_eq!(int_array.value(2), 0, "Expected truncate(1000, 999) = 0");
        } else {
            panic!("Expected UInt32 array");
        }
    }

    #[test]
    fn test_truncate_uint64_array() {
        let udf = Truncate::new();
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Int64(Some(1000))),
                ColumnarValue::Array(Arc::new(UInt64Array::from(vec![1234, 5678, 999]))),
            ],
            number_rows: 3,
            return_type: &DataType::UInt64,
        };
        let result = udf.invoke_with_args(args).expect("invoke UDF");
        if let ColumnarValue::Array(array) = result {
            let int_array = array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("downcast to UInt64Array");
            assert_eq!(int_array.len(), 3);
            assert_eq!(
                int_array.value(0),
                1000,
                "Expected truncate(1000, 1234) = 1000"
            );
            assert_eq!(
                int_array.value(1),
                5000,
                "Expected truncate(1000, 5678) = 5000"
            );
            assert_eq!(int_array.value(2), 0, "Expected truncate(1000, 999) = 0");
        } else {
            panic!("Expected UInt64 array");
        }
    }

    #[test]
    fn test_truncate_decimal_array() {
        let udf = Truncate::new();
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Int64(Some(50))),
                ColumnarValue::Array(Arc::new(Decimal128Array::from_iter_values(vec![
                    1065i128, 1234i128,
                ]))),
            ],
            number_rows: 2,
            return_type: &DataType::Decimal128(10, 2),
        };
        let result = udf.invoke_with_args(args).expect("invoke UDF");
        if let ColumnarValue::Array(array) = result {
            let dec_array = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .expect("downcast to Decimal128Array");
            assert_eq!(dec_array.len(), 2);
            assert_eq!(
                dec_array.value(0),
                1050,
                "Expected truncate(50, 10.65) = 10.50"
            );
            assert_eq!(
                dec_array.value(1),
                1200,
                "Expected truncate(50, 12.34) = 12.00"
            );
        } else {
            panic!("Expected Decimal128 array");
        }
    }

    #[test]
    fn test_truncate_utf8_array() {
        let udf = Truncate::new();
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Int64(Some(3))),
                ColumnarValue::Array(Arc::new(StringArray::from(vec!["iceberg", "spark"]))),
            ],
            number_rows: 2,
            return_type: &DataType::Utf8,
        };
        let result = udf.invoke_with_args(args).expect("invoke UDF");
        if let ColumnarValue::Array(array) = result {
            let str_array = array
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("downcast to StringArray");
            assert_eq!(str_array.len(), 2);
            assert_eq!(
                str_array.value(0),
                "ice",
                "Expected truncate(3, 'iceberg') = 'ice'"
            );
            assert_eq!(
                str_array.value(1),
                "spa",
                "Expected truncate(3, 'spark') = 'spa'"
            );
        } else {
            panic!("Expected Utf8 array");
        }
    }

    #[test]
    fn test_truncate_binary_array() {
        let udf = Truncate::new();
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Int64(Some(3))),
                ColumnarValue::Array(Arc::new(BinaryArray::from_vec(vec![
                    &[1, 2, 3, 4, 5],
                    &[6, 7, 8],
                ]))),
            ],
            number_rows: 2,
            return_type: &DataType::Binary,
        };
        let result = udf.invoke_with_args(args).expect("invoke UDF");
        if let ColumnarValue::Array(array) = result {
            let bin_array = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .expect("downcast to BinaryArray");
            assert_eq!(bin_array.len(), 2);
            assert_eq!(
                bin_array.value(0),
                vec![1, 2, 3],
                "Expected truncate(3, [1,2,3,4,5]) = [1,2,3]"
            );
            assert_eq!(
                bin_array.value(1),
                vec![6, 7, 8],
                "Expected truncate(3, [6,7,8]) = [6,7,8]"
            );
        } else {
            panic!("Expected Binary array");
        }
    }

    #[test]
    fn test_truncate_null_array() {
        let udf = Truncate::new();
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Int64(Some(10))),
                ColumnarValue::Array(Arc::new(Int64Array::from(vec![None, Some(101), None]))),
            ],
            number_rows: 3,
            return_type: &DataType::Int64,
        };
        let result = udf.invoke_with_args(args).expect("invoke UDF");
        if let ColumnarValue::Array(array) = result {
            let int_array = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("downcast to Int64Array");
            assert_eq!(int_array.len(), 3);
            assert!(int_array.is_null(0), "Expected NULL at index 0");
            assert_eq!(int_array.value(1), 100, "Expected truncate(10, 101) = 100");
            assert!(int_array.is_null(2), "Expected NULL at index 2");
        } else {
            panic!("Expected Int64 array");
        }
    }

    #[test]
    fn test_truncate_invalid_type() {
        let udf = Truncate::new();
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Int64(Some(10))),
                ColumnarValue::Array(Arc::new(arrow::array::BooleanArray::from(vec![
                    true, false,
                ]))),
            ],
            number_rows: 2,
            return_type: &DataType::Int64,
        };
        let result = udf.invoke_with_args(args);
        assert!(
            result.is_err(),
            "Expected error for invalid second argument type"
        );
    }
}
