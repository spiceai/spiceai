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
use std::sync::LazyLock;

use crate::vendored_hash::{RandomState, create_hashes};
use arrow::array::ArrayRef;
use datafusion::arrow::array::{Array, Int32Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::DataFusionError;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::scalar::ScalarValue;
use snafu::{ResultExt as _, Snafu};
use util::format_datafusion_error;

/// Maximum number of buckets, chosen to support large-scale partitioning while preventing excessive memory usage.
const MAX_NUM_BUCKETS: i64 = 1_000_000;

/// Compile-time assertion that `MAX_NUM_BUCKETS` does not exceed `i32::MAX`
const _: () = assert!(
    MAX_NUM_BUCKETS <= i32::MAX as i64,
    "MAX_NUM_BUCKETS exceeds i32::MAX"
);

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

    #[snafu(display("DataFusion error: {}", format_datafusion_error(source)))]
    DataFusion { source: DataFusionError },

    #[snafu(display(
        "Bucket function first argument must be a positive number, got {description}. Ensure the function is called like `bucket(num_buckets, column)`, for example `bucket(10, my_column)`."
    ))]
    InvalidFirstArgType { description: String },

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
runtime_udfs_api::register_spice_function!(BUCKET_SPICE_FUNCTION, BUCKET_SCALAR_UDF_NAME);
impl Bucket {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for Bucket {
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
        match &arg_types[0] {
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64 => Ok(arg_types[0].clone()),
            other => Err(DataFusionError::Plan(format!(
                "BUCKET UDF expects first argument to be an integer type, but got {other:?}"
            ))),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue, DataFusionError> {
        let args = args.args;
        let num_args = args.len();
        if num_args != 2 {
            tracing::debug!("Invalid argument count: {num_args}");
            return Err(BucketError::InvalidArgumentCount { count: args.len() }.into());
        }

        let (num_buckets, output_type) = match &args[0] {
            ColumnarValue::Scalar(scalar) => match scalar_to_i64(scalar) {
                Some(n) if n > 0 && n <= MAX_NUM_BUCKETS => (n, scalar.data_type()),
                Some(n) => return Err(BucketError::InvalidNumBuckets { num_buckets: n }.into()),
                None => {
                    return Err(BucketError::InvalidFirstArgType {
                        description: describe_columnar_value(&args[0]),
                    }
                    .into());
                }
            },
            arg @ ColumnarValue::Array(_) => {
                return Err(BucketError::InvalidFirstArgType {
                    description: describe_columnar_value(arg),
                }
                .into());
            }
        };

        tracing::trace!("Computing bucket with num_buckets: {num_buckets}");

        match &args[1] {
            ColumnarValue::Scalar(value) => {
                let bucket = compute_bucket(value, num_buckets, &output_type)?;
                Ok(ColumnarValue::Scalar(bucket))
            }
            ColumnarValue::Array(array) => {
                let buckets = compute_bucket_array(array, num_buckets, &output_type)?;
                Ok(ColumnarValue::Array(buckets))
            }
        }
    }
}

/// Wraps a bucket value (u64) into the appropriate `ScalarValue` for the given output type.
fn wrap_bucket(bucket: u64, output_type: &DataType) -> Result<ScalarValue, DataFusionError> {
    match output_type {
        DataType::Int8 => Ok(ScalarValue::Int8(Some(
            i8::try_from(bucket).context(BucketLargerThanTypeSnafu)?,
        ))),
        DataType::Int16 => Ok(ScalarValue::Int16(Some(
            i16::try_from(bucket).context(BucketLargerThanTypeSnafu)?,
        ))),
        DataType::Int64 => Ok(ScalarValue::Int64(Some(
            i64::try_from(bucket).context(BucketLargerThanTypeSnafu)?,
        ))),
        DataType::UInt8 => Ok(ScalarValue::UInt8(Some(
            u8::try_from(bucket).context(BucketLargerThanTypeSnafu)?,
        ))),
        DataType::UInt16 => Ok(ScalarValue::UInt16(Some(
            u16::try_from(bucket).context(BucketLargerThanTypeSnafu)?,
        ))),
        DataType::UInt32 => Ok(ScalarValue::UInt32(Some(
            u32::try_from(bucket).context(BucketLargerThanTypeSnafu)?,
        ))),
        DataType::UInt64 => Ok(ScalarValue::UInt64(Some(bucket))),
        // Also catches `DataType::Int32`.
        _ => Ok(ScalarValue::Int32(Some(
            i32::try_from(bucket).context(BucketLargerThanTypeSnafu)?,
        ))),
    }
}

/// Attempts to extract an `i64` value from any integer-typed `ScalarValue`.
fn scalar_to_i64(scalar: &ScalarValue) -> Option<i64> {
    match scalar {
        ScalarValue::Int8(Some(n)) => Some(i64::from(*n)),
        ScalarValue::Int16(Some(n)) => Some(i64::from(*n)),
        ScalarValue::Int32(Some(n)) => Some(i64::from(*n)),
        ScalarValue::Int64(Some(n)) => Some(*n),
        ScalarValue::UInt8(Some(n)) => Some(i64::from(*n)),
        ScalarValue::UInt16(Some(n)) => Some(i64::from(*n)),
        ScalarValue::UInt32(Some(n)) => Some(i64::from(*n)),
        ScalarValue::UInt64(Some(n)) => i64::try_from(*n).ok(),
        _ => None,
    }
}

/// Creates a human-readable description of a `ColumnarValue` for error messages.
/// Avoids printing array contents which can be very long and confusing.
fn describe_columnar_value(value: &ColumnarValue) -> String {
    match value {
        ColumnarValue::Array(array) => {
            format!("a column of type {}", array.data_type())
        }
        ColumnarValue::Scalar(scalar) => {
            format!("a scalar value {scalar}")
        }
    }
}

fn compute_bucket(
    scalar: &ScalarValue,
    num_buckets: i64,
    output_type: &DataType,
) -> Result<ScalarValue, DataFusionError> {
    if scalar.is_null() {
        return Ok(ScalarValue::try_from(output_type).context(DataFusionSnafu)?);
    }
    let array = scalar.to_array()?;
    let mut hashes = vec![0; 1];
    create_hashes(array.as_ref(), &RANDOM_STATE, &mut hashes)?;
    let bucket = u64::try_from(num_buckets)
        .map(|n| hashes[0] % n)
        .context(BucketLargerThanTypeSnafu)?;
    wrap_bucket(bucket, output_type)
}

fn compute_bucket_array(
    array: &ArrayRef,
    num_buckets: i64,
    output_type: &DataType,
) -> Result<ArrayRef, DataFusionError> {
    let num_buckets = i32::try_from(num_buckets).context(BucketLargerThanTypeSnafu)?;
    // Validated positive and <= MAX_NUM_BUCKETS (<= i32::MAX) by the caller.
    let num_buckets_u64 = u64::try_from(num_buckets).context(BucketLargerThanTypeSnafu)?;

    let mut hashes = vec![0u64; array.len()];
    create_hashes(array.as_ref(), &RANDOM_STATE, &mut hashes)?;

    // Compute bucket IDs directly from hashes — no intermediate Arrow arrays.
    // `hash % num_buckets` is always < num_buckets <= MAX_NUM_BUCKETS <= i32::MAX.
    let mut buckets = Vec::with_capacity(hashes.len());
    for hash in hashes {
        #[expect(
            clippy::cast_possible_truncation,
            reason = "modulo result is < num_buckets <= MAX_NUM_BUCKETS <= i32::MAX"
        )]
        let bucket = (hash % num_buckets_u64) as i32;
        buckets.push(bucket);
    }

    let result = Int32Array::new(buckets.into(), array.nulls().cloned());

    Ok(arrow::compute::cast(&result, output_type)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;
    use arrow_schema::Field;
    use datafusion::config::ConfigOptions;
    use insta::assert_snapshot;
    use std::sync::Arc;

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
        if let ColumnarValue::Scalar(ScalarValue::Int64(Some(first_bucket))) = results[0] {
            for (i, result) in results.iter().enumerate().skip(1) {
                if let ColumnarValue::Scalar(ScalarValue::Int64(Some(bucket))) = result {
                    assert_eq!(
                        first_bucket, *bucket,
                        "Non-deterministic bucket for scalar at invocation {i}"
                    );
                } else {
                    panic!("Expected Int64 scalar at invocation {i}");
                }
            }
        } else {
            panic!("Expected Int64 scalar for first invocation");
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
                .downcast_ref::<arrow::array::Int64Array>()
                .expect("downcast to Int64Array for first invocation");
            assert_eq!(first_int_array.len(), 3);

            for (i, result) in results.iter().enumerate().skip(1) {
                if let ColumnarValue::Array(array) = result {
                    let int_array = array
                        .as_any()
                        .downcast_ref::<arrow::array::Int64Array>()
                        .unwrap_or_else(|| panic!("downcast to Int64Array for invocation {i}"));
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
                    panic!("Expected Int64 array for invocation {i}");
                }
            }
        } else {
            panic!("Expected Int64 array for first invocation");
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
        result.expect_err("Should fail for invalid num_buckets");
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
        result.expect_err("Should fail for invalid num_buckets");
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
    fn test_first_arg_column_error_message() {
        // This test verifies the improved error message when the first argument
        // is a column (array) instead of a scalar Int64 literal.
        // See: https://github.com/spiceai/spiceai/issues/8238
        let udf = Bucket::new();
        let args = ScalarFunctionArgs {
            args: vec![
                // First argument is an array (column) instead of a scalar
                ColumnarValue::Array(Arc::new(arrow::array::Int64Array::from(vec![
                    0, 1, 2, 3, 4,
                ]))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(10))),
            ],
            number_rows: 5,
            arg_fields: vec![],
            return_field: Arc::new(Field::new("ignored_name", DataType::Int32, false)),
            config_options: Arc::new(ConfigOptions::default()),
        };
        let result = udf.invoke_with_args(args);
        let error = result.expect_err("Should fail when first argument is a column");
        let error_msg = error.to_string();

        // Verify the error message is helpful and doesn't dump array contents
        assert!(
            error_msg.contains("Bucket function first argument must be a positive number, got"),
            "Error message should indicate the first argument must be a literal: {error_msg}"
        );
        assert!(
            error_msg.contains("bucket(10, my_column)"),
            "Error message should provide a usage example: {error_msg}"
        );
        // Make sure we don't dump the array values
        assert!(
            !error_msg.contains("+---"),
            "Error message should not contain table formatting: {error_msg}"
        );
    }

    /// Golden-value guard that pins `bucket()`'s output for a representative
    /// value of every non-string Arrow type the partition transform hashes.
    ///
    /// `bucket(n, value) = hash(value) % n` is the partition transform behind
    /// `runtime-table-partition`: a value MUST map to the same bucket for the
    /// lifetime of a persisted dataset, or an equality-filter prune silently
    /// skips the partition that holds the matching rows — missing-row data loss
    /// (#11277). The hash is therefore part of the on-disk format, and
    /// `vendored_hash.rs` freezes it to `DataFusion` 53's `ahash`. But that module
    /// still delegates the actual hashing to the external `ahash` crate, pinned
    /// only as `^0.8` — and ahash gives **no** cross-version output guarantee, so
    /// a routine `cargo update` (or a build that enables ahash's AES path) can
    /// silently re-bucket every existing partitioned dataset.
    ///
    /// These goldens make that drift LOUD: any change here fails CI instead of
    /// silently corrupting pruning. The pre-existing snapshot tests only locked
    /// the string (`bucket_scalar`/`bucket_array`) and decimal paths; the integer,
    /// unsigned, boolean, float, and binary paths — the common partition keys,
    /// e.g. `bucket(50, org_id)` or `bucket(3, user_id)` — had no guard at all.
    ///
    /// A large modulus (`MAX_NUM_BUCKETS`) is used so the assertion captures the
    /// low 6 decimal digits of the hash, not just a handful of low bits. Edge
    /// values fill each integer width so every width's hashing path is distinct.
    ///
    /// If a value here ever needs to change, that is a breaking change to the
    /// on-disk partition format: regenerate the goldens **and** version/migrate
    /// the format — do not blindly update them to make CI green.
    #[test]
    fn test_bucket_hash_stability_golden_values() {
        // (type label, input value, expected `bucket(MAX_NUM_BUCKETS, value)`).
        let cases: [(&str, ScalarValue, i64); 14] = [
            ("Int8", ScalarValue::Int8(Some(-128)), 924_318),
            ("Int16", ScalarValue::Int16(Some(-12_345)), 180_632),
            ("Int32", ScalarValue::Int32(Some(-1_234_567)), 143_530),
            ("Int64", ScalarValue::Int64(Some(-123_456_789_012)), 397_203),
            ("UInt8", ScalarValue::UInt8(Some(255)), 670_181),
            ("UInt16", ScalarValue::UInt16(Some(65_535)), 162_628),
            ("UInt32", ScalarValue::UInt32(Some(4_000_000_000)), 663_368),
            (
                "UInt64",
                ScalarValue::UInt64(Some(18_000_000_000_000_000_000)),
                279_638,
            ),
            ("Boolean_true", ScalarValue::Boolean(Some(true)), 719_061),
            ("Boolean_false", ScalarValue::Boolean(Some(false)), 627_404),
            ("Float32", ScalarValue::Float32(Some(1.5)), 157_006),
            ("Float64", ScalarValue::Float64(Some(-2.5)), 749_390),
            (
                "Utf8",
                ScalarValue::Utf8(Some("user_42".to_string())),
                594_815,
            ),
            (
                "Binary",
                ScalarValue::Binary(Some(vec![0x00, 0x01, 0x02, 0xff])),
                682_122,
            ),
        ];

        for (label, value, expected) in cases {
            let bucket = compute_bucket(&value, MAX_NUM_BUCKETS, &DataType::Int64)
                .expect("compute_bucket should succeed for a supported type");
            let ScalarValue::Int64(Some(actual)) = bucket else {
                panic!("expected an Int64 bucket for {label}, got {bucket:?}");
            };
            assert_eq!(
                actual, expected,
                "bucket() output for {label} drifted from the frozen on-disk value \
                 {expected} to {actual}. The partition hash must not change (see \
                 #11277 and vendored_hash.rs); if this is an intentional, \
                 format-versioned migration, update the golden AND bump the format."
            );
        }

        // The vectorized array path must agree with the scalar path (it shares
        // `create_hashes`); lock it for a non-string type — the existing
        // `bucket_array` snapshot only covers strings. Element 0 reuses the
        // Int64 scalar value above and must match its golden (397_203).
        let arr: ArrayRef = std::sync::Arc::new(arrow::array::Int64Array::from(vec![
            -123_456_789_012_i64,
            0,
            9_999_999_999,
        ]));
        let bucketed = compute_bucket_array(&arr, MAX_NUM_BUCKETS, &DataType::Int64)
            .expect("compute_bucket_array should succeed for Int64");
        let bucketed = bucketed
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("bucket array should be Int64");
        assert_eq!(
            bucketed.values(),
            &[397_203_i64, 627_404, 929_657],
            "vectorized bucket() output for an Int64 column drifted from its frozen \
             on-disk values (see #11277)"
        );
    }

    /// The `bucket()` partition hash must never come from ahash's AES-accelerated
    /// hasher: `aes_hash::AHasher` and `fallback_hash::AHasher` produce different
    /// output from the same seed, so an AES build would silently re-bucket
    /// persisted partitioned datasets relative to the shipped fallback-hashed
    /// builds (#11277). `vendored_hash.rs` has a `compile_error!` that fails any
    /// `x86/x86_64` build with `target_feature = "aes"` active, so on such a build
    /// this crate does not compile and this test never runs. Where it *does* run,
    /// assert the invariant explicitly and re-lock one fallback golden to prove
    /// the active hasher is in fact the portable fallback.
    #[test]
    fn test_bucket_uses_portable_fallback_hash_not_aes() {
        let aes_hasher_active = cfg!(all(
            any(target_arch = "x86", target_arch = "x86_64"),
            target_feature = "aes",
            not(miri)
        ));
        assert!(
            !aes_hasher_active,
            "the compile_error! guard in vendored_hash.rs should have prevented \
             compiling this crate with ahash's AES hasher active (see #11277)"
        );

        // Same input/golden as `test_bucket_hash_stability_golden_values` (Utf8
        // "user_42"): proves the hasher actually driving bucket() in this build
        // is the portable fallback, not an output-incompatible variant.
        let bucket = compute_bucket(
            &ScalarValue::Utf8(Some("user_42".to_string())),
            MAX_NUM_BUCKETS,
            &DataType::Int64,
        )
        .expect("compute_bucket should succeed for Utf8");
        assert_eq!(
            bucket,
            ScalarValue::Int64(Some(594_815)),
            "bucket() must use the frozen portable fallback hash (#11277)"
        );
    }

    #[test]
    fn test_first_arg_wrong_scalar_type_error_message() {
        // Test error message when first argument is a scalar but wrong type
        let udf = Bucket::new();
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("not_a_number".to_string()))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(10))),
            ],
            number_rows: 1,
            arg_fields: vec![],
            return_field: Arc::new(Field::new("ignored_name", DataType::Int32, false)),
            config_options: Arc::new(ConfigOptions::default()),
        };
        let result = udf.invoke_with_args(args);
        let error = result.expect_err("Should fail when first argument is wrong scalar type");
        let error_msg = error.to_string();

        assert!(
            error_msg.contains("Bucket function first argument must be a positive number"),
            "Error message should indicate the first argument must be a literal: {error_msg}"
        );
        assert!(
            error_msg.contains("a scalar value"),
            "Error message should describe what was received: {error_msg}"
        );
    }
}
