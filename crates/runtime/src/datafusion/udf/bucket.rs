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

use ahash::RandomState;
use arrow::array::ArrayRef;
use datafusion::arrow::array::{Array, Int32Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::DataFusionError;
use datafusion::common::hash_utils::create_hashes;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::scalar::ScalarValue;
use snafu::Snafu;

const NULL_BUCKET: i32 = -1;

/// Maximum number of buckets, chosen to support large-scale partitioning while preventing excessive memory usage.
const MAX_NUM_BUCKETS: i64 = 1_000_000;

#[derive(Debug, Snafu)]
pub enum BucketError {
    #[snafu(display(
        "Invalid number of buckets: {num_buckets}. Must be a positive integer less than {MAX_NUM_BUCKETS}."
    ))]
    InvalidNumBuckets { num_buckets: i64 },

    #[snafu(display("Expected exactly two arguments, got {count}"))]
    InvalidArgumentCount { count: usize },

    #[snafu(display("ScalarValue `{scalar:?}` is not supported"))]
    UnsupportedScalarValue { scalar: ScalarValue },

    #[snafu(display("DataFusion error: {source}"))]
    DataFusion { source: DataFusionError },

    #[snafu(display("First argument must be a positive Int64, got {value}"))]
    InvalidFirstArgType { value: ColumnarValue },
}

impl Into<DataFusionError> for BucketError {
    fn into(self) -> DataFusionError {
        DataFusionError::External(self.to_string().into())
    }
}

#[derive(Debug)]
pub struct Bucket {
    signature: Signature,
}

impl Bucket {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for Bucket {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "bucket"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType, DataFusionError> {
        if arg_types.len() != 2 {
            return Err(DataFusionError::Plan(format!(
                "Expected exactly two arguments, got {}",
                arg_types.len()
            )));
        }
        Ok(DataType::Int32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue, DataFusionError> {
        let args = args.args;
        let num_args = args.len();
        if num_args != 2 {
            tracing::debug!("Invalid argument count: {num_args}");
            return Err(BucketError::InvalidArgumentCount { count: args.len() }.into());
        }

        let num_buckets = match &args[0] {
            ColumnarValue::Scalar(ScalarValue::Int64(Some(n))) => {
                if *n <= 0 || *n > MAX_NUM_BUCKETS {
                    return Err(BucketError::InvalidNumBuckets { num_buckets: *n }.into());
                }
                *n
            }
            arg => {
                return Err(BucketError::InvalidFirstArgType { value: arg.clone() }.into());
            }
        };

        tracing::trace!("Computing bucket with num_buckets: {num_buckets}");

        match &args[1] {
            ColumnarValue::Scalar(scalar) => {
                let bucket = compute_bucket(&scalar, num_buckets)?;
                Ok(ColumnarValue::Scalar(ScalarValue::Int32(Some(bucket))))
            }
            ColumnarValue::Array(array) => {
                let buckets = compute_bucket_array(Arc::clone(array), num_buckets)?;
                Ok(ColumnarValue::Array(Arc::new(buckets)))
            }
        }
    }
}

fn compute_bucket(scalar: &ScalarValue, num_buckets: i64) -> Result<i32, DataFusionError> {
    if scalar.is_null() {
        return Ok(NULL_BUCKET);
    }
    let array = scalar.to_array()?;
    let mut hashes = vec![0; 1];
    let random_state = RandomState::new();
    create_hashes(&[array], &random_state, &mut hashes)?;
    Ok((hashes[0] % num_buckets as u64) as i32)
}

fn compute_bucket_array(array: ArrayRef, num_buckets: i64) -> Result<Int32Array, DataFusionError> {
    let mut hashes = vec![0; array.len()];
    let random_state = RandomState::new();
    let capacity = array.len();
    create_hashes(&[Arc::clone(&array)], &random_state, &mut hashes)?;

    let mut builder = Int32Array::builder(capacity);
    for i in 0..capacity {
        let bucket = if array.is_null(i) {
            NULL_BUCKET
        } else {
            (hashes[i] % num_buckets as u64) as i32
        };
        builder.append_value(bucket);
    }
    Ok(builder.finish())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;

    #[tokio::test]
    async fn test_bucket_scalar() {
        let udf = Bucket::new();
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Int64(Some(10))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("test".to_string()))),
            ],
            number_rows: 1,
            return_type: &DataType::Int32,
        };
        let result = udf.invoke_with_args(args).expect("invoke UDF");
        if let ColumnarValue::Scalar(ScalarValue::Int32(Some(bucket))) = result {
            assert!(bucket >= 0 && bucket < 10, "Bucket out of range: {bucket}",);
        } else {
            panic!("Expected Int32 scalar");
        }
    }

    #[tokio::test]
    async fn test_bucket_array() {
        let udf = Bucket::new();
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Int64(Some(5))),
                ColumnarValue::Array(Arc::new(StringArray::from(vec!["a", "b", "c"]))),
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
            for i in 0..3 {
                let bucket = int_array.value(i);
                assert!(bucket >= 0 && bucket < 5, "Bucket out of range: {bucket}",);
            }
        } else {
            panic!("Expected Int32 array");
        }
    }

    #[tokio::test]
    async fn test_invalid_num_buckets() {
        let udf = Bucket::new();
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Int64(Some(0))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("test".to_string()))),
            ],
            number_rows: 1,
            return_type: &DataType::Int32,
        };
        let result = udf.invoke_with_args(args);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_max_buckets() {
        let udf = Bucket::new();
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Int64(Some(MAX_NUM_BUCKETS + 1))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("test".to_string()))),
            ],
            number_rows: 1,
            return_type: &DataType::Int32,
        };
        let result = udf.invoke_with_args(args);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_null_input() {
        let udf = Bucket::new();
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Int64(Some(10))),
                ColumnarValue::Scalar(ScalarValue::Null),
            ],
            number_rows: 1,
            return_type: &DataType::Int32,
        };
        let result = udf.invoke_with_args(args).expect("invoke udf");
        if let ColumnarValue::Scalar(ScalarValue::Int32(Some(bucket))) = result {
            assert_eq!(bucket, NULL_BUCKET);
        } else {
            panic!("Expected Int32 scalar");
        }
    }

    #[tokio::test]
    async fn test_decimal_input() {
        let udf = Bucket::new();
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Int64(Some(10))),
                ColumnarValue::Scalar(ScalarValue::Decimal128(Some(12345), 10, 2)),
            ],
            number_rows: 1,
            return_type: &DataType::Int32,
        };
        let result = udf.invoke_with_args(args).expect("invoke udf");
        if let ColumnarValue::Scalar(ScalarValue::Int32(Some(bucket))) = result {
            assert!(bucket >= 0 && bucket < 10);
        } else {
            panic!("Expected Int32 scalar");
        }
    }

    #[tokio::test]
    async fn test_empty_array() {
        let udf = Bucket::new();
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Int64(Some(5))),
                ColumnarValue::Array(Arc::new(StringArray::from(Vec::<String>::new()))),
            ],
            number_rows: 0,
            return_type: &DataType::Int32,
        };
        let result = udf.invoke_with_args(args).expect("invoke udf");
        if let ColumnarValue::Array(array) = result {
            let int_array = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("downcast_ref");
            assert_eq!(int_array.len(), 0);
        } else {
            panic!("Expected empty Int32 array");
        }
    }
}
