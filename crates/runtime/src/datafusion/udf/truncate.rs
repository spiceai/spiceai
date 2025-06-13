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

use std::sync::Arc;

use arrow::array::{
    ArrayRef, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
    UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::compute::binary;
use arrow::compute::kernels::substring::substring;
use arrow::datatypes::DataType;
use datafusion::common::DataFusionError;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::scalar::ScalarValue;
use snafu::{OptionExt, Snafu};

macro_rules! truncate_numeric_array {
    ($array:expr, $width:expr, $array_type:ty, $cast_type:ty, $output_type:ty) => {{
        let casted_array = $array
            .as_any()
            .downcast_ref::<$array_type>()
            .context(DowncastFailedSnafu)?;
        let width_array = <$array_type>::from_value($width as $cast_type, $array.len());
        let result: $array_type = binary(casted_array, &width_array, |v, w| {
            let v = i64::from(v);
            let w = i64::from(w);
            (v - (((v % w) + w) % w)) as $output_type
        })
        .map_err(|e| DataFusionError::ArrowError(e, None))?;
        Ok(Arc::new(<$array_type>::new(
            result.values().clone(),
            $array.nulls().cloned(),
        )))
    }};
}

/// Maximum truncation width or length, chosen to prevent overflow or excessive memory usage.
const MAX_TRUNCATE_WIDTH: i64 = i64::MAX / 2;

#[derive(Debug, Snafu)]
pub enum TruncateError {
    #[snafu(display(
        "Invalid width: {width}. Must be a positive integer less than or equal to {MAX_TRUNCATE_WIDTH}"
    ))]
    InvalidWidth { width: i64 },

    #[snafu(display("Expected exactly two arguments, got {count}"))]
    InvalidArgumentCount { count: usize },

    #[snafu(display("First argument must be a positive Int64, got {value:?}"))]
    InvalidFirstArgType { value: ColumnarValue },

    #[snafu(display(
        "Second argument must be Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64, Float32, Float64, Utf8, or Binary, got {value:?}"
    ))]
    InvalidSecondArgType { value: ColumnarValue },

    #[snafu(display("Failed to downcast array"))]
    DowncastFailed,
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
        if arg_types.len() != 2 {
            return Err(TruncateError::InvalidArgumentCount {
                count: arg_types.len(),
            }
            .into());
        }
        if !matches!(arg_types[0], DataType::Int64) {
            return Err(TruncateError::InvalidFirstArgType {
                value: ColumnarValue::Scalar(ScalarValue::try_from(&arg_types[0])?),
            }
            .into());
        }
        match &arg_types[1] {
            DataType::Int8 => Ok(DataType::Int8),
            DataType::Int16 => Ok(DataType::Int16),
            DataType::Int32 => Ok(DataType::Int32),
            DataType::Int64 => Ok(DataType::Int64),
            DataType::UInt8 => Ok(DataType::UInt8),
            DataType::UInt16 => Ok(DataType::UInt16),
            DataType::UInt32 => Ok(DataType::UInt32),
            DataType::UInt64 => Ok(DataType::UInt64),
            DataType::Float32 => Ok(DataType::Float32),
            DataType::Float64 => Ok(DataType::Float64),
            DataType::Utf8 => Ok(DataType::Utf8),
            DataType::Binary => Ok(DataType::Binary),
            _ => Err(TruncateError::InvalidSecondArgType {
                value: ColumnarValue::Scalar(ScalarValue::try_from(&arg_types[1])?),
            }
            .into()),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue, DataFusionError> {
        let args = args.args;
        if args.len() != 2 {
            tracing::debug!("Invalid argument count: {}", args.len());
            return Err(TruncateError::InvalidArgumentCount { count: args.len() }.into());
        }

        let width = match &args[0] {
            ColumnarValue::Scalar(ScalarValue::Int64(Some(w))) => {
                if *w <= 0 || *w > MAX_TRUNCATE_WIDTH {
                    return Err(TruncateError::InvalidWidth { width: *w }.into());
                }
                *w
            }
            arg => {
                return Err(TruncateError::InvalidFirstArgType { value: arg.clone() }.into());
            }
        };

        tracing::trace!("Computing truncate with width: {}", width);

        match &args[1] {
            ColumnarValue::Scalar(scalar) => {
                let result = compute_truncate_scalar(scalar, width)?;
                Ok(ColumnarValue::Scalar(result))
            }
            ColumnarValue::Array(array) => {
                let result = compute_truncate_array(array, width)?;
                Ok(ColumnarValue::Array(result))
            }
        }
    }
}

fn compute_truncate_scalar(
    scalar: &ScalarValue,
    width: i64,
) -> Result<ScalarValue, DataFusionError> {
    if scalar.is_null() {
        return match scalar {
            ScalarValue::Int8(_) => Ok(ScalarValue::Int8(None)),
            ScalarValue::Int16(_) => Ok(ScalarValue::Int16(None)),
            ScalarValue::Int32(_) => Ok(ScalarValue::Int32(None)),
            ScalarValue::Int64(_) => Ok(ScalarValue::Int64(None)),
            ScalarValue::UInt8(_) => Ok(ScalarValue::UInt8(None)),
            ScalarValue::UInt16(_) => Ok(ScalarValue::UInt16(None)),
            ScalarValue::UInt32(_) => Ok(ScalarValue::UInt32(None)),
            ScalarValue::UInt64(_) => Ok(ScalarValue::UInt64(None)),
            ScalarValue::Float32(_) => Ok(ScalarValue::Float32(None)),
            ScalarValue::Float64(_) => Ok(ScalarValue::Float64(None)),
            ScalarValue::Utf8(_) => Ok(ScalarValue::Utf8(None)),
            ScalarValue::Binary(_) => Ok(ScalarValue::Binary(None)),
            _ => Err(TruncateError::InvalidSecondArgType {
                value: ColumnarValue::Scalar(scalar.clone()),
            }
            .into()),
        };
    }

    match scalar {
        ScalarValue::Int8(Some(v)) => {
            let v = i64::from(*v);
            let result = v - (((v % width) + width) % width);
            Ok(ScalarValue::Int8(Some(result.try_into().map_err(
                |_| DataFusionError::Execution(format!("Value out of range for Int8: {}", result)),
            )?)))
        }
        ScalarValue::Int16(Some(v)) => {
            let v = i64::from(*v);
            let result = v - (((v % width) + width) % width);
            Ok(ScalarValue::Int16(Some(result.try_into().map_err(
                |_| DataFusionError::Execution(format!("Value out of range for Int16: {}", result)),
            )?)))
        }
        ScalarValue::Int32(Some(v)) => {
            let v = i64::from(*v);
            let result = v - (((v % width) + width) % width);
            Ok(ScalarValue::Int32(Some(result.try_into().map_err(
                |_| DataFusionError::Execution(format!("Value out of range for Int32: {}", result)),
            )?)))
        }
        ScalarValue::Int64(Some(v)) => {
            let result = v - (((v % width) + width) % width);
            Ok(ScalarValue::Int64(Some(result)))
        }
        ScalarValue::UInt8(Some(v)) => {
            let v = i64::from(*v);
            let result = v - (((v % width) + width) % width);
            Ok(ScalarValue::UInt8(Some(result.try_into().map_err(
                |_| DataFusionError::Execution(format!("Value out of range for UInt8: {}", result)),
            )?)))
        }
        ScalarValue::UInt16(Some(v)) => {
            let v = i64::from(*v);
            let result = v - (((v % width) + width) % width);
            Ok(ScalarValue::UInt16(Some(result.try_into().map_err(
                |_| {
                    DataFusionError::Execution(format!("Value out of range for UInt16: {}", result))
                },
            )?)))
        }
        ScalarValue::UInt32(Some(v)) => {
            let v = i64::from(*v);
            let result = v - (((v % width) + width) % width);
            Ok(ScalarValue::UInt32(Some(result.try_into().map_err(
                |_| {
                    DataFusionError::Execution(format!("Value out of range for UInt32: {}", result))
                },
            )?)))
        }
        ScalarValue::UInt64(Some(v)) => {
            let v = i64::try_from(*v).map_err(|_| {
                DataFusionError::Execution(format!("Value too large for Int64: {}", v))
            })?;
            let result = v - (((v % width) + width) % width);
            Ok(ScalarValue::UInt64(Some(result.try_into().map_err(
                |_| {
                    DataFusionError::Execution(format!("Value out of range for UInt64: {}", result))
                },
            )?)))
        }
        ScalarValue::Float32(Some(v)) => {
            let v = v.floor() as i64;
            let result = v - (((v % width) + width) % width);
            Ok(ScalarValue::Float32(Some(result as f32)))
        }
        ScalarValue::Float64(Some(v)) => {
            let v = v.floor() as i64;
            let result = v - (((v % width) + width) % width);
            Ok(ScalarValue::Float64(Some(result as f64)))
        }
        ScalarValue::Utf8(Some(v)) => Ok(ScalarValue::Utf8(Some(
            v.chars().take(width as usize).collect::<String>(),
        ))),
        ScalarValue::Binary(Some(v)) => {
            let truncated = v.iter().take(width as usize).copied().collect::<Vec<u8>>();
            Ok(ScalarValue::Binary(Some(truncated)))
        }
        _ => Err(TruncateError::InvalidSecondArgType {
            value: ColumnarValue::Scalar(scalar.clone()),
        }
        .into()),
    }
}

fn compute_truncate_array(array: &ArrayRef, width: i64) -> Result<ArrayRef, DataFusionError> {
    match array.data_type() {
        DataType::Int8 => truncate_numeric_array!(array, width, Int8Array, i8, i8),
        DataType::Int16 => truncate_numeric_array!(array, width, Int16Array, i16, i16),
        DataType::Int32 => truncate_numeric_array!(array, width, Int32Array, i32, i32),
        DataType::Int64 => truncate_numeric_array!(array, width, Int64Array, i64, i64),
        DataType::UInt8 => truncate_numeric_array!(array, width, UInt8Array, u8, u8),
        DataType::UInt16 => truncate_numeric_array!(array, width, UInt16Array, u16, u16),
        DataType::UInt32 => truncate_numeric_array!(array, width, UInt32Array, u32, u32),
        DataType::UInt64 => {
            let casted_array = array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .context(DowncastFailedSnafu)?;
            let width_array = UInt64Array::from_value(width as u64, array.len());
            let result: UInt64Array = binary(casted_array, &width_array, |v, w| {
                let v = i64::try_from(v).unwrap_or(i64::MAX);
                let w = i64::try_from(w).unwrap_or(i64::MAX);
                (v - (((v % w) + w) % w)) as u64
            })
            .map_err(|e| DataFusionError::ArrowError(e, None))?;
            Ok(Arc::new(UInt64Array::new(
                result.values().clone(),
                array.nulls().cloned(),
            )))
        }
        DataType::Float32 => {
            let casted_array = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .context(DowncastFailedSnafu)?;
            let width_array = Float32Array::from_value(width as f32, array.len());
            let result: Float32Array = binary(casted_array, &width_array, |v, w| {
                let v = v.floor() as i64;
                let w = w as i64;
                (v - (((v % w) + w) % w)) as f32
            })
            .map_err(|e| DataFusionError::ArrowError(e, None))?;
            Ok(Arc::new(Float32Array::new(
                result.values().clone(),
                array.nulls().cloned(),
            )))
        }
        DataType::Float64 => {
            let casted_array = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .context(DowncastFailedSnafu)?;
            let width_array = Float64Array::from_value(width as f64, array.len());
            let result: Float64Array = binary(casted_array, &width_array, |v, w| {
                let v = v.floor() as i64;
                let w = w as i64;
                (v - (((v % w) + w) % w)) as f64
            })
            .map_err(|e| DataFusionError::ArrowError(e, None))?;
            Ok(Arc::new(Float64Array::new(
                result.values().clone(),
                array.nulls().cloned(),
            )))
        }
        DataType::Utf8 | DataType::Binary => {
            let result = substring(array, 0, Some(width as u64))
                .map_err(|e| DataFusionError::ArrowError(e, None))?;
            Ok(Arc::new(result))
        }
        _ => Err(TruncateError::InvalidSecondArgType {
            value: ColumnarValue::Array(Arc::clone(array)),
        }
        .into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        Array as _, BinaryArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
        Int64Array, StringArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
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
                ColumnarValue::Scalar(ScalarValue::Int64(Some(1000))),
                ColumnarValue::Array(Arc::new(Int32Array::from(vec![1234, -5678, 999]))),
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
    fn test_truncate_float32_array() {
        let udf = Truncate::new();
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Int64(Some(10))),
                ColumnarValue::Array(Arc::new(Float32Array::from(vec![101.7, -1.2, 0.0]))),
            ],
            number_rows: 3,
            return_type: &DataType::Float32,
        };
        let result = udf.invoke_with_args(args).expect("invoke UDF");
        if let ColumnarValue::Array(array) = result {
            let float_array = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .expect("downcast to Float32Array");
            assert_eq!(float_array.len(), 3);
            assert_eq!(
                float_array.value(0),
                100.0,
                "Expected truncate(10, 101.7) = 100.0"
            );
            assert_eq!(
                float_array.value(1),
                -10.0,
                "Expected truncate(10, -1.2) = -10.0"
            );
            assert_eq!(
                float_array.value(2),
                0.0,
                "Expected truncate(10, 0.0) = 0.0"
            );
        } else {
            panic!("Expected Float32 array");
        }
    }

    #[test]
    fn test_truncate_float64_array() {
        let udf = Truncate::new();
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Int64(Some(100))),
                ColumnarValue::Array(Arc::new(Float64Array::from(vec![123.4, -56.78, 9.9]))),
            ],
            number_rows: 3,
            return_type: &DataType::Float64,
        };
        let result = udf.invoke_with_args(args).expect("invoke UDF");
        if let ColumnarValue::Array(array) = result {
            let float_array = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("downcast to Float64Array");
            assert_eq!(float_array.len(), 3);
            assert_eq!(
                float_array.value(0),
                100.0,
                "Expected truncate(100, 123.4) = 100.0"
            );
            assert_eq!(
                float_array.value(1),
                -100.0,
                "Expected truncate(100, -56.78) = -100.0"
            );
            assert_eq!(
                float_array.value(2),
                0.0,
                "Expected truncate(100, 9.9) = 0.0"
            );
        } else {
            panic!("Expected Float64 array");
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
            return_type: &DataType::Boolean,
        };
        let result = udf.invoke_with_args(args);
        assert!(
            result.is_err(),
            "Expected error for invalid second argument type"
        );
    }
}
