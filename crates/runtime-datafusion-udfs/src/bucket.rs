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
use arrow::array::ArrayRef;
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

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct Bucket {
    signature: Signature,
}

impl Default for Bucket {
    fn default() -> Self {
        Self::new()
    }
}
pub static BUCKET_SCALAR_UDF_NAME: &str = "bucket";
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
        BUCKET_SCALAR_UDF_NAME
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
                // Avoid clone by formatting the error message directly
                let arg_type = match arg {
                    ColumnarValue::Scalar(sv) => format!("Scalar({sv:?})"),
                    ColumnarValue::Array(array) => format!("Array({})", array.data_type()),
                };
                return Err(DataFusionError::Plan(format!(
                    "First argument must be a positive Int64, got {arg_type}"
                )));
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

#[inline(always)]
#[allow(clippy::cast_possible_truncation)]
fn bucket_from_hash(hash: u64, num_buckets_u64: u64, bitmask: Option<u64>) -> i32 {
    let bucket_u64 = if let Some(mask) = bitmask {
        hash & mask
    } else {
        hash % num_buckets_u64
    };

    // SAFETY: bucket_u64 < num_buckets_u64 and num_buckets_u64 <= MAX_NUM_BUCKETS <= i32::MAX
    bucket_u64 as i32
}

fn compute_bucket(scalar: &ScalarValue, num_buckets: i64) -> Result<ScalarValue, DataFusionError> {
    const _: () = assert!(
        MAX_NUM_BUCKETS <= i32::MAX as i64,
        "MAX_NUM_BUCKETS exceeds i32::MAX"
    );

    if scalar.is_null() {
        return Ok(ScalarValue::Int32(None));
    }

    // Pre-compute to avoid error handling in hot path
    let num_buckets_u64 = u64::try_from(num_buckets).context(BucketLargerThanTypeSnafu)?;
    let bitmask = num_buckets_u64
        .is_power_of_two()
        .then(|| num_buckets_u64 - 1);

    let array = scalar.to_array()?;
    let mut hashes = vec![0; 1];
    create_hashes(&[array], &RANDOM_STATE, &mut hashes)?;

    let bucket = bucket_from_hash(hashes[0], num_buckets_u64, bitmask);
    Ok(ScalarValue::Int32(Some(bucket)))
}

/// Optimized bucket computation using SIMD-friendly direct iteration.
///
/// This implementation is optimized for SIMD auto-vectorization by:
/// 1. Using direct slice iteration instead of Arrow's `binary()` kernel (which uses closures)
/// 2. Structuring the loop to be auto-vectorizable by the compiler
/// 3. Avoiding unnecessary allocations and clones
/// 4. Using pre-allocated buffers for zero-copy operations
#[allow(clippy::missing_panics_doc)]
fn compute_bucket_array(array: &ArrayRef, num_buckets: i64) -> Result<Int32Array, DataFusionError> {
    let num_buckets_u64 = u64::try_from(num_buckets).context(BucketLargerThanTypeSnafu)?;
    let bitmask = num_buckets_u64
        .is_power_of_two()
        .then(|| num_buckets_u64 - 1);

    let len = array.len();

    // Create hashes - this is already optimized in DataFusion's hash_utils
    let mut hashes = vec![0u64; len];
    create_hashes(&[Arc::clone(array)], &RANDOM_STATE, &mut hashes)?;

    // SIMD-optimized modulo computation
    // The compiler can auto-vectorize this loop because:
    // 1. No complex closures or function pointers
    // 2. Simple arithmetic operations (modulo)
    // 3. Contiguous memory access pattern
    // 4. No branches in the hot loop
    let mut buckets = Vec::with_capacity(len);

    // Process in chunks for better cache locality and auto-vectorization
    // Rust/LLVM will auto-vectorize this with NEON (arm64) or AVX2/AVX-512 (x86_64)
    for &hash in &hashes {
        const _: () = assert!(
            MAX_NUM_BUCKETS <= i32::MAX as i64,
            "MAX_NUM_BUCKETS exceeds i32::MAX"
        );

        buckets.push(bucket_from_hash(hash, num_buckets_u64, bitmask));
    }

    // Zero-copy construction: transfer ownership of Vec to Arrow array
    // This avoids the clone() that was in the original implementation
    let result = Int32Array::new(buckets.into(), array.nulls().cloned());

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        Float32Array, Float64Array, Int32Array, Int64Array, StringArray, UInt32Array, UInt64Array,
    };
    use arrow_schema::Field;
    use datafusion::config::ConfigOptions;
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
            arg_fields: vec![],
            return_field: Arc::new(Field::new("ignored_name", DataType::Int32, false)),
            config_options: Arc::new(ConfigOptions::default()),
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
            arg_fields: vec![],
            return_field: Arc::new(Field::new("ignored_name", DataType::Int32, false)),
            config_options: Arc::new(ConfigOptions::default()),
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
                    arg_fields: vec![],
                    return_field: Arc::new(Field::new("ignored_name", DataType::Int32, false)),
                    config_options: Arc::new(ConfigOptions::default()),
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
                    arg_fields: vec![],
                    return_field: Arc::new(Field::new("ignored_name", DataType::Int32, false)),
                    config_options: Arc::new(ConfigOptions::default()),
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
            arg_fields: vec![],
            return_field: Arc::new(Field::new("ignored_name", DataType::Int32, false)),
            config_options: Arc::new(ConfigOptions::default()),
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
            arg_fields: vec![],
            return_field: Arc::new(Field::new("ignored_name", DataType::Int32, false)),
            config_options: Arc::new(ConfigOptions::default()),
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
            arg_fields: vec![],
            return_field: Arc::new(Field::new("ignored_name", DataType::Int32, false)),
            config_options: Arc::new(ConfigOptions::default()),
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
            arg_fields: vec![],
            return_field: Arc::new(Field::new("ignored_name", DataType::Int32, false)),
            config_options: Arc::new(ConfigOptions::default()),
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
            arg_fields: vec![],
            return_field: Arc::new(Field::new("ignored_name", DataType::Int32, false)),
            config_options: Arc::new(ConfigOptions::default()),
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
            arg_fields: vec![],
            return_field: Arc::new(Field::new("ignored_name", DataType::Int32, false)),
            config_options: Arc::new(ConfigOptions::default()),
        };
        let result = udf.invoke_with_args(args).expect("invoke udf");
        assert_snapshot!("null_array_input", result);
    }

    #[test]
    fn test_negative_num_buckets() {
        let udf = Bucket::new();
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Int64(Some(-5))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("test".to_string()))),
            ],
            number_rows: 1,
            arg_fields: vec![],
            return_field: Arc::new(Field::new("ignored_name", DataType::Int32, false)),
            config_options: Arc::new(ConfigOptions::default()),
        };
        let result = udf.invoke_with_args(args);
        assert!(result.is_err(), "Negative num_buckets should fail");
    }

    #[test]
    fn test_wrong_argument_count() {
        let udf = Bucket::new();
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(ScalarValue::Int64(Some(10)))],
            number_rows: 1,
            arg_fields: vec![],
            return_field: Arc::new(Field::new("ignored_name", DataType::Int32, false)),
            config_options: Arc::new(ConfigOptions::default()),
        };
        let result = udf.invoke_with_args(args);
        assert!(result.is_err(), "Wrong argument count should fail");
    }

    #[test]
    fn test_invalid_first_argument_type() {
        let udf = Bucket::new();
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("not_a_number".to_string()))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("test".to_string()))),
            ],
            number_rows: 1,
            arg_fields: vec![],
            return_field: Arc::new(Field::new("ignored_name", DataType::Int32, false)),
            config_options: Arc::new(ConfigOptions::default()),
        };
        let result = udf.invoke_with_args(args);
        assert!(result.is_err(), "Invalid first argument type should fail");
    }

    #[test]
    fn test_bucket_distribution_in_range() {
        let udf = Bucket::new();
        let num_buckets = 10;
        let test_values = vec!["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"];

        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Int64(Some(num_buckets))),
                ColumnarValue::Array(Arc::new(StringArray::from(test_values))),
            ],
            number_rows: 10,
            arg_fields: vec![],
            return_field: Arc::new(Field::new("ignored_name", DataType::Int32, false)),
            config_options: Arc::new(ConfigOptions::default()),
        };

        let result = udf.invoke_with_args(args).expect("invoke udf");
        if let ColumnarValue::Array(array) = result {
            let int_array = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("downcast to Int32Array");

            #[allow(clippy::cast_possible_truncation)]
            // num_buckets <= MAX_NUM_BUCKETS (1_000_000 < i32::MAX)
            for i in 0..int_array.len() {
                let bucket = int_array.value(i);
                assert!(
                    (0..num_buckets as i32).contains(&bucket),
                    "Bucket {bucket} should be in range [0, {num_buckets})"
                );
            }
        } else {
            panic!("Expected Int32 array");
        }
    }

    #[test]
    fn test_large_array() {
        // Test with 10,000 elements to verify SIMD optimization path
        let udf = Bucket::new();
        let large_values: Vec<String> = (0..10_000).map(|i| format!("value_{i}")).collect();

        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Int64(Some(100))),
                ColumnarValue::Array(Arc::new(StringArray::from(large_values))),
            ],
            number_rows: 10_000,
            arg_fields: vec![],
            return_field: Arc::new(Field::new("ignored_name", DataType::Int32, false)),
            config_options: Arc::new(ConfigOptions::default()),
        };

        let result = udf.invoke_with_args(args).expect("invoke udf");
        if let ColumnarValue::Array(array) = result {
            let int_array = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("downcast to Int32Array");

            assert_eq!(int_array.len(), 10_000);

            // Verify all buckets are in valid range
            for i in 0..int_array.len() {
                let bucket = int_array.value(i);
                assert!(
                    (0..100).contains(&bucket),
                    "Bucket {bucket} at index {i} should be in range [0, 100)"
                );
            }
        } else {
            panic!("Expected Int32 array");
        }
    }

    #[test]
    fn test_int32_array() {
        let udf = Bucket::new();
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Int64(Some(7))),
                ColumnarValue::Array(Arc::new(Int32Array::from(vec![1, 2, 3, 100, -50]))),
            ],
            number_rows: 5,
            arg_fields: vec![],
            return_field: Arc::new(Field::new("ignored_name", DataType::Int32, false)),
            config_options: Arc::new(ConfigOptions::default()),
        };

        let result = udf.invoke_with_args(args).expect("invoke udf");
        if let ColumnarValue::Array(array) = result {
            let int_array = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("downcast to Int32Array");

            assert_eq!(int_array.len(), 5);
            for i in 0..int_array.len() {
                let bucket = int_array.value(i);
                assert!(
                    (0..7).contains(&bucket),
                    "Bucket {bucket} should be in range [0, 7)"
                );
            }
        } else {
            panic!("Expected Int32 array");
        }
    }

    #[test]
    fn test_int64_array() {
        let udf = Bucket::new();
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Int64(Some(12))),
                ColumnarValue::Array(Arc::new(Int64Array::from(vec![
                    1,
                    1000,
                    i64::MAX,
                    i64::MIN,
                ]))),
            ],
            number_rows: 4,
            arg_fields: vec![],
            return_field: Arc::new(Field::new("ignored_name", DataType::Int32, false)),
            config_options: Arc::new(ConfigOptions::default()),
        };

        let result = udf.invoke_with_args(args).expect("invoke udf");
        if let ColumnarValue::Array(array) = result {
            let int_array = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("downcast to Int32Array");

            assert_eq!(int_array.len(), 4);
            for i in 0..int_array.len() {
                let bucket = int_array.value(i);
                assert!(
                    (0..12).contains(&bucket),
                    "Bucket {bucket} should be in range [0, 12)"
                );
            }
        } else {
            panic!("Expected Int32 array");
        }
    }

    #[test]
    fn test_uint32_array() {
        let udf = Bucket::new();
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Int64(Some(8))),
                ColumnarValue::Array(Arc::new(UInt32Array::from(vec![0, 100, u32::MAX]))),
            ],
            number_rows: 3,
            arg_fields: vec![],
            return_field: Arc::new(Field::new("ignored_name", DataType::Int32, false)),
            config_options: Arc::new(ConfigOptions::default()),
        };

        let result = udf.invoke_with_args(args).expect("invoke udf");
        if let ColumnarValue::Array(array) = result {
            let int_array = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("downcast to Int32Array");

            assert_eq!(int_array.len(), 3);
            for i in 0..int_array.len() {
                let bucket = int_array.value(i);
                assert!(
                    (0..8).contains(&bucket),
                    "Bucket {bucket} should be in range [0, 8)"
                );
            }
        } else {
            panic!("Expected Int32 array");
        }
    }

    #[test]
    fn test_uint64_array() {
        let udf = Bucket::new();
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Int64(Some(15))),
                ColumnarValue::Array(Arc::new(UInt64Array::from(vec![0, 1000, u64::MAX]))),
            ],
            number_rows: 3,
            arg_fields: vec![],
            return_field: Arc::new(Field::new("ignored_name", DataType::Int32, false)),
            config_options: Arc::new(ConfigOptions::default()),
        };

        let result = udf.invoke_with_args(args).expect("invoke udf");
        if let ColumnarValue::Array(array) = result {
            let int_array = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("downcast to Int32Array");

            assert_eq!(int_array.len(), 3);
            for i in 0..int_array.len() {
                let bucket = int_array.value(i);
                assert!(
                    (0..15).contains(&bucket),
                    "Bucket {bucket} should be in range [0, 15)"
                );
            }
        } else {
            panic!("Expected Int32 array");
        }
    }

    #[test]
    fn test_float32_array() {
        let udf = Bucket::new();
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Int64(Some(6))),
                ColumnarValue::Array(Arc::new(Float32Array::from(vec![1.5, -2.7, 0.0, 100.123]))),
            ],
            number_rows: 4,
            arg_fields: vec![],
            return_field: Arc::new(Field::new("ignored_name", DataType::Int32, false)),
            config_options: Arc::new(ConfigOptions::default()),
        };

        let result = udf.invoke_with_args(args).expect("invoke udf");
        if let ColumnarValue::Array(array) = result {
            let int_array = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("downcast to Int32Array");

            assert_eq!(int_array.len(), 4);
            for i in 0..int_array.len() {
                let bucket = int_array.value(i);
                assert!(
                    (0..6).contains(&bucket),
                    "Bucket {bucket} should be in range [0, 6)"
                );
            }
        } else {
            panic!("Expected Int32 array");
        }
    }

    #[test]
    fn test_float64_array() {
        let udf = Bucket::new();
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Int64(Some(9))),
                ColumnarValue::Array(Arc::new(Float64Array::from(vec![
                    1.5,
                    -2.7,
                    0.0,
                    f64::MAX,
                    f64::MIN,
                ]))),
            ],
            number_rows: 5,
            arg_fields: vec![],
            return_field: Arc::new(Field::new("ignored_name", DataType::Int32, false)),
            config_options: Arc::new(ConfigOptions::default()),
        };

        let result = udf.invoke_with_args(args).expect("invoke udf");
        if let ColumnarValue::Array(array) = result {
            let int_array = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("downcast to Int32Array");

            assert_eq!(int_array.len(), 5);
            for i in 0..int_array.len() {
                let bucket = int_array.value(i);
                assert!(
                    (0..9).contains(&bucket),
                    "Bucket {bucket} should be in range [0, 9)"
                );
            }
        } else {
            panic!("Expected Int32 array");
        }
    }
}
