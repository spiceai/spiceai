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
use std::sync::{Arc, LazyLock};

use ahash::RandomState;
use arrow::array::{ArrayRef, UInt64Array};
use arrow::compute::binary;
use datafusion::arrow::array::{Array, Int32Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::DataFusionError;
use datafusion::common::hash_utils::create_hashes;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::scalar::ScalarValue;
use snafu::{ResultExt as _, Snafu};

/// Maximum number of buckets, chosen to support large-scale partitioning while preventing excessive memory usage.
const MAX_NUM_BUCKETS: i64 = 1_000_000;

/// Static `RandomState` for deterministic hashing.
static RANDOM_STATE: LazyLock<RandomState> =
    LazyLock::new(|| RandomState::with_seeds(0x53, 0x50, 0x49, 0x43_45));

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

    #[snafu(display("Bucket value is larger than the storage type: {source}"))]
    BucketLargerThanType {
        #[snafu(source)]
        source: TryFromIntError,
    },
}

impl From<BucketError> for DataFusionError {
    fn from(val: BucketError) -> Self {
        DataFusionError::External(val.to_string().into())
    }
}

#[derive(Debug)]
pub struct Bucket {
    signature: Signature,
}

impl Default for Bucket {
    fn default() -> Self {
        Self::new()
    }
}

impl Bucket {
    #[must_use]
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

    fn name(&self) -> &'static str {
        "bucket"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType, DataFusionError> {
        if arg_types.len() != 2 {
            return Err(BucketError::InvalidArgumentCount {
                count: arg_types.len(),
            }
            .into());
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
                let bucket = compute_bucket(scalar, num_buckets)?;
                Ok(ColumnarValue::Scalar(bucket))
            }
            ColumnarValue::Array(array) => {
                let buckets = compute_bucket_array(array, num_buckets)?;
                Ok(ColumnarValue::Array(Arc::new(buckets)))
            }
        }
    }
}

fn compute_bucket(scalar: &ScalarValue, num_buckets: i64) -> Result<ScalarValue, DataFusionError> {
    if scalar.is_null() {
        return Ok(ScalarValue::Int32(None));
    }
    let array = scalar.to_array()?;
    let mut hashes = vec![0; 1];
    create_hashes(&[array], &RANDOM_STATE, &mut hashes)?;
    Ok(ScalarValue::Int32(Some(
        u64::try_from(num_buckets)
            .and_then(|n| i32::try_from(hashes[0] % n))
            .context(BucketLargerThanTypeSnafu)?,
    )))
}

#[allow(clippy::missing_panics_doc)]
fn compute_bucket_array(array: &ArrayRef, num_buckets: i64) -> Result<Int32Array, DataFusionError> {
    let num_buckets = i32::try_from(num_buckets).context(BucketLargerThanTypeSnafu)?;

    let mut hashes = vec![0u64; array.len()];
    create_hashes(&[Arc::clone(array)], &RANDOM_STATE, &mut hashes)?;

    let hash_array = UInt64Array::from(hashes);

    let bucket_array: Int32Array = binary(
        &hash_array,
        &Int32Array::from_value(num_buckets, array.len()),
        |hash, n| {
            const _: () = assert!(
                MAX_NUM_BUCKETS <= i32::MAX as i64,
                "MAX_NUM_BUCKETS exceeds i32::MAX"
            );
            #[allow(clippy::expect_used)]
            // SAFETY: unwrap is safe because we restrict MAX_NUM_BUCKETS at compile time
            u64::try_from(n)
                .and_then(|n| i32::try_from(hash % n))
                .expect("MAX_NUM_BUCKETS smaller than i32 positive maximum")
        },
    )?;

    let result = Int32Array::new(bucket_array.values().clone(), array.nulls().cloned());

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;
    use insta::assert_snapshot;

    #[test]
    fn test_bucket_scalar() {
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
        assert_snapshot!("bucket_scalar", result);
    }

    #[test]
    fn test_bucket_array() {
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
        assert_snapshot!("bucket_array", result);
    }

    #[test]
    fn test_bucket_determinism_scalar() {
        let udf = Bucket::new();

        // Run the UDF multiple times (10) to ensure determinism
        let results: Vec<_> = (0..10)
            .map(|i| {
                let args = ScalarFunctionArgs {
                    args: vec![
                        ColumnarValue::Scalar(ScalarValue::Int64(Some(10))),
                        ColumnarValue::Scalar(ScalarValue::Utf8(Some("test".to_string()))),
                    ],
                    number_rows: 1,
                    return_type: &DataType::Int32,
                };
                udf.invoke_with_args(args)
                    .unwrap_or_else(|_| panic!("invoke UDF {i}"))
            })
            .collect();

        // Verify all results are identical to the first
        if let ColumnarValue::Scalar(ScalarValue::Int32(Some(first_bucket))) = results[0] {
            for (i, result) in results.iter().enumerate().skip(1) {
                if let ColumnarValue::Scalar(ScalarValue::Int32(Some(bucket))) = result {
                    assert_eq!(
                        first_bucket, *bucket,
                        "Non-deterministic bucket for scalar at invocation {i}"
                    );
                } else {
                    panic!("Expected Int32 scalar at invocation {i}");
                }
            }
        } else {
            panic!("Expected Int32 scalar for first invocation");
        }
    }

    #[test]
    fn test_bucket_determinism_array() {
        let udf = Bucket::new();

        // Run the UDF multiple times (10) to ensure determinism
        let results: Vec<_> = (0..10)
            .map(|i| {
                let args = ScalarFunctionArgs {
                    args: vec![
                        ColumnarValue::Scalar(ScalarValue::Int64(Some(5))),
                        ColumnarValue::Array(Arc::new(StringArray::from(vec!["a", "b", "c"]))),
                    ],
                    number_rows: 3,
                    return_type: &DataType::Int32,
                };
                udf.invoke_with_args(args)
                    .unwrap_or_else(|_| panic!("invoke UDF {i}"))
            })
            .collect();

        // Verify all results are identical to the first
        if let ColumnarValue::Array(first_array) = &results[0] {
            let first_int_array = first_array
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("downcast to Int32Array for first invocation");
            assert_eq!(first_int_array.len(), 3);

            for (i, result) in results.iter().enumerate().skip(1) {
                if let ColumnarValue::Array(array) = result {
                    let int_array = array
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap_or_else(|| panic!("downcast to Int32Array for invocation {i}"));
                    assert_eq!(int_array.len(), 3);
                    for j in 0..3 {
                        let bucket = int_array.value(j);
                        let first_bucket = first_int_array.value(j);
                        assert_eq!(
                            first_bucket, bucket,
                            "Non-deterministic bucket at index {j} for invocation {i}: {first_bucket} != {bucket}"
                        );
                    }
                } else {
                    panic!("Expected Int32 array for invocation {i}");
                }
            }
        } else {
            panic!("Expected Int32 array for first invocation");
        }
    }

    #[test]
    fn test_invalid_num_buckets() {
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

    #[test]
    fn test_max_buckets() {
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

    #[test]
    fn test_null_input() {
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
        assert_snapshot!("null_input", result);
    }

    #[test]
    fn test_decimal_input() {
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
        assert_snapshot!("decimal_input", result);
    }

    #[test]
    fn test_empty_array() {
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
        assert_snapshot!("empty_array", result);
    }

    #[test]
    fn test_null_array_input() {
        let udf = Bucket::new();
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Int64(Some(5))),
                ColumnarValue::Array(Arc::new(StringArray::from(vec![
                    None::<String>,
                    Some("a".to_string()),
                    None::<String>,
                ]))),
            ],
            number_rows: 3,
            return_type: &DataType::Int32,
        };
        let result = udf.invoke_with_args(args).expect("invoke udf");
        assert_snapshot!("null_array_input", result);
    }
}
