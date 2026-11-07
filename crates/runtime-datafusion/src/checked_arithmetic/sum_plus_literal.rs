/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! `SUM(x + k)` for several literals `k` as one `SUM(x)` and one `COUNT(x)`.
//!
//! `DataFusion` rewrites `SUM(x + 1), SUM(x + 2), ...` (`ClickBench` Q29 has 90
//! of them) to `SUM(x) + k * COUNT(x)`, so common-subexpression elimination
//! leaves two aggregates however many literals there are. Its rewrite adds
//! and multiplies with wrapping kernels, which [`CheckedSum`](super::CheckedSum)
//! cannot use for an integer `x`. Narrowing `SUM(x)` to `Int64` before adding
//! `k * COUNT(x)` is not exact either: `SUM(x)` can leave `Int64` where
//! `SUM(x + k)` does not, and `k * COUNT(x)` can too.
//!
//! So an integer `x` is summed as `Decimal128(38, 0)`, which holds any total of
//! `Int64` values (fewer than 2^63 of them, each under 2^63 in magnitude, sum
//! to under 2^126 < 10^38), and [`CheckedSumPlusCount`] adds `k * COUNT(x)` in
//! `i128` and narrows once, with the error checked `SUM` gives. `SUM(x + k)` is
//! the sum of `x + k` over the rows where `x` is not NULL, which is
//! `SUM(x) + k * COUNT(x)`, and NULL where `SUM(x)` is.
//!
//! [`CheckedSumWiden`] does the widening rather than a `CAST`: a column name
//! leaves out casts, so `SUM(CAST(x AS ...))` and `COUNT(CAST(x AS ...))` would
//! take the names of the query's own `SUM(x)` and `COUNT(x)` (`ClickBench` Q29
//! starts with `SUM(x)`), and an aggregate with two outputs of one name does
//! not plan.

use std::sync::{Arc, LazyLock};

use arrow::array::{Array, AsArray, Int64Array};
use arrow::datatypes::{DataType, Decimal128Type, Field, FieldRef, Int64Type};
use datafusion::common::{Result, ScalarValue, exec_err, plan_err};
use datafusion::functions_aggregate::count::count;
use datafusion::logical_expr::expr::AggregateFunction;
use datafusion::logical_expr::{
    ColumnarValue, Expr, Operator, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl,
    Signature, Volatility, lit,
};

use super::is_integer_arith_type;
use super::sum::{checked_sum_state_type, int64_overflow};

/// Rewrite target for `SUM(x + k)`. Not a documented SQL API; the `__spice_`
/// prefix matches the checked operator kernels.
pub const CHECKED_SUM_PLUS_COUNT_UDF_NAME: &str = "__spice_checked_sum_plus_count";

runtime_udfs_api::register_spice_function!(
    CHECKED_SUM_PLUS_COUNT_SPICE_FUNCTION,
    CHECKED_SUM_PLUS_COUNT_UDF_NAME
);

/// Rewrite helper that widens the `x` of `SUM(x + k)`. Not a documented SQL API.
pub const CHECKED_SUM_WIDEN_UDF_NAME: &str = "__spice_checked_sum_widen";

runtime_udfs_api::register_spice_function!(
    CHECKED_SUM_WIDEN_SPICE_FUNCTION,
    CHECKED_SUM_WIDEN_UDF_NAME
);

#[must_use]
pub(crate) fn checked_sum_plus_count_udf() -> Arc<ScalarUDF> {
    static UDF: LazyLock<Arc<ScalarUDF>> =
        LazyLock::new(|| Arc::new(ScalarUDF::from(CheckedSumPlusCount::new())));
    Arc::clone(&UDF)
}

#[must_use]
pub(crate) fn checked_sum_widen_udf() -> Arc<ScalarUDF> {
    static UDF: LazyLock<Arc<ScalarUDF>> =
        LazyLock::new(|| Arc::new(ScalarUDF::from(CheckedSumWiden::new())));
    Arc::clone(&UDF)
}

/// `SUM(arg + k)` as `sum + k * count` over a wide `SUM(arg)` and `COUNT(arg)`,
/// for the checked `SUM` call `sum_of_arg_plus_k` and a signed integer `k`.
///
/// `None` for any operator but `+`, as the built-in rewrite. `k` is the
/// literal's value: after type coercion `arg` has the literal's type, so it is
/// a signed integer too.
pub(crate) fn sum_plus_literal(
    sum_of_arg_plus_k: &AggregateFunction,
    arg: &Expr,
    op: Operator,
    k: i64,
) -> Option<Expr> {
    if op != Operator::Plus {
        return None;
    }
    let widened = checked_sum_widen_udf().call(vec![arg.clone()]);
    let mut wide_sum = sum_of_arg_plus_k.clone();
    wide_sum.params.args = vec![widened.clone()];
    Some(checked_sum_plus_count_udf().call(vec![
        Expr::AggregateFunction(wide_sum),
        count(widened),
        lit(k),
    ]))
}

/// An integer as `Decimal128(38, 0)`, which holds every integer value exactly,
/// NULL where the integer is. See the module documentation for why this is a
/// function and not a `CAST`.
#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct CheckedSumWiden {
    signature: Signature,
}

impl CheckedSumWiden {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CheckedSumWiden {
    fn name(&self) -> &'static str {
        CHECKED_SUM_WIDEN_UDF_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(checked_sum_state_type())
    }

    /// The argument as written, casts included. The default name leaves casts
    /// out, so `SUM(x + 1)` and `SUM(CAST(x AS INT) + 1)` over a SMALLINT `x`
    /// would name their wide `SUM`s alike.
    fn schema_name(&self, args: &[Expr]) -> Result<String> {
        let args = args
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        Ok(format!("{}({args})", self.name()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [value] = args.arg_fields else {
            return plan_err!("{} expects one argument", self.name());
        };
        Ok(Arc::new(Field::new(
            self.name(),
            checked_sum_state_type(),
            value.is_nullable(),
        )))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        match arg_types {
            [value] if is_integer_arith_type(value) => Ok(vec![value.clone()]),
            _ => plan_err!(
                "{} expects one integer argument, got {arg_types:?}",
                self.name()
            ),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [value] = args.args.as_slice() else {
            return exec_err!("{} expects one argument", self.name());
        };
        if !is_integer_arith_type(&value.data_type()) {
            return exec_err!(
                "{} expects an integer, got {}",
                self.name(),
                value.data_type()
            );
        }
        value.cast_to(&checked_sum_state_type(), None)
    }
}

/// `sum + k * count` for a `Decimal128(38, 0)` `sum`, an `Int64` `count` and an
/// `Int64` `k`, as an `Int64`: NULL where `sum` is, and the checked `SUM`
/// overflow error where the total is not an `Int64`.
#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct CheckedSumPlusCount {
    signature: Signature,
}

impl CheckedSumPlusCount {
    fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![checked_sum_state_type(), DataType::Int64, DataType::Int64],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for CheckedSumPlusCount {
    fn name(&self) -> &'static str {
        CHECKED_SUM_PLUS_COUNT_UDF_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    /// Nullable where the `SUM` is, as the `SUM(x + k)` it replaces.
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [sum, _count, _k] = args.arg_fields else {
            return plan_err!("{} expects three arguments", self.name());
        };
        Ok(Arc::new(Field::new(
            self.name(),
            DataType::Int64,
            sum.is_nullable(),
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [sum, count, k] = args.args.as_slice() else {
            return exec_err!("{} expects three arguments", self.name());
        };
        let ColumnarValue::Scalar(ScalarValue::Int64(Some(k))) = k else {
            return exec_err!("{} expects a non-null BIGINT literal k", self.name());
        };
        let arrays = ColumnarValue::values_to_arrays(&[sum.clone(), count.clone()])?;
        let [sums, counts] = arrays.as_slice() else {
            return exec_err!("{} expects a sum and a count", self.name());
        };
        let (Some(sums), Some(counts)) = (
            sums.as_primitive_opt::<Decimal128Type>(),
            counts.as_primitive_opt::<Int64Type>(),
        ) else {
            return exec_err!(
                "{} expects a Decimal128(38, 0) sum and an Int64 count, got {} and {}",
                self.name(),
                sums.data_type(),
                counts.data_type()
            );
        };
        let totals = sums
            .iter()
            .zip(counts.iter())
            .map(|(sum, count)| match (sum, count) {
                (Some(sum), Some(count)) => sum_plus_k_times_count(sum, count, *k).map(Some),
                _ => Ok(None),
            })
            .collect::<Result<Int64Array>>()?;
        Ok(ColumnarValue::Array(Arc::new(totals)))
    }
}

/// `sum + k * count` exactly, narrowed to `Int64` once. A product or a total
/// that leaves `i128` is far outside `Int64`, so it is the same overflow.
fn sum_plus_k_times_count(sum: i128, count: i64, k: i64) -> Result<i64> {
    i128::from(k)
        .checked_mul(i128::from(count))
        .and_then(|scaled| sum.checked_add(scaled))
        .and_then(|total| i64::try_from(total).ok())
        .ok_or_else(int64_overflow)
}

#[cfg(test)]
mod tests {
    use super::sum_plus_k_times_count;

    /// The narrowing point is the total, not its parts: either part may leave
    /// `Int64` while the total does not.
    #[test]
    fn total_is_narrowed_once() {
        let max = i128::from(i64::MAX);
        // SUM(x) past i64::MAX, pulled back by a negative k.
        assert_eq!(
            sum_plus_k_times_count(max + 10, 5, -2).expect("total fits"),
            i64::MAX
        );
        // k * COUNT(x) past i64::MAX, pulled back by a negative SUM(x).
        assert_eq!(
            sum_plus_k_times_count(-(1_i128 << 55), 1 << 23, 1 << 40).expect("total fits"),
            i64::MAX - (1_i64 << 55) + 1
        );
        assert_eq!(
            sum_plus_k_times_count(i128::from(i64::MIN), 0, i64::MAX).expect("no rows of k"),
            i64::MIN
        );
        for (sum, count, k) in [
            (max, 1, 1),
            (i128::from(i64::MIN), 1, -1),
            (0, i64::MAX, i64::MAX),
        ] {
            let error = sum_plus_k_times_count(sum, count, k)
                .expect_err("total is not an Int64")
                .to_string();
            assert!(
                error.contains("Overflow in SUM of BIGINT"),
                "{sum} + {k} * {count}: {error}"
            );
        }
    }
}
