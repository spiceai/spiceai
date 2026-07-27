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

//! Whole-table aggregate folding from Vortex file statistics.
//!
//! `DataFusion`'s built-in `AggregateStatistics` physical rule already answers
//! `COUNT(*)`, `COUNT(col)`, `MIN`, and `MAX` from exact statistics (via each
//! aggregate UDF's `value_from_stats`). The built-in `sum`/`avg` UDFs do **not**
//! implement that hook, so `SUM`/`AVG` always fall through to a full scan even
//! when the answer is sitting in the Vortex footer.
//!
//! This module fills that gap. Given an [`AggregateExec`] with no `GROUP BY`
//! over an *unfiltered* Cayenne scan, it computes the one-row result directly
//! from the scan's [`Statistics`], covering the full metadata-answerable set:
//! `COUNT(*)`, `COUNT(col)`, `SUM`, `AVG`, `MIN`, `MAX`. Covering the whole set
//! (not just `SUM`/`AVG`) matters for mixed queries such as
//! `SELECT COUNT(*), SUM(v), MIN(v) FROM t`: `DataFusion`'s rule declines the
//! whole aggregate as soon as one expression (`SUM`) is unsupported, so the
//! entire node is left for this rule to fold.
//!
//! Soundness: every value is taken only when its [`Precision`] is `Exact`. The
//! caller is responsible for ensuring the underlying scan carries no pushed-down
//! filter (a filtered scan still reports whole-file sums/counts, which would be
//! wrong for the filtered query). Anything we cannot answer exactly returns
//! `None`, leaving the original scan+aggregate in place.

use arrow::array::RecordBatch;
use arrow_schema::Schema;
use arrow_schema::{DataType, FieldRef};
use datafusion::error::Result as DataFusionResult;
// `ExecutionPlan` is used as a trait (for `AggregateExec::schema`/`input`), not a
// path, so it reads as unused to a shallow linter — it is required to compile.
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion_common::{
    ColumnStatistics, DataFusionError, ScalarValue, Statistics, stats::Precision,
};
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::expressions::{CastExpr, Column, Literal};
use std::sync::Arc;

/// One metadata-answerable aggregate, paired with the input-column index it
/// reads (where applicable). Column indices reference the aggregate's *input*
/// schema, which is the schema the supplied [`Statistics`] must be aligned to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StatsAggKind {
    /// `COUNT(*)` / `COUNT(1)` — the table cardinality.
    CountStar,
    /// `COUNT(col)` — non-null count of a column.
    CountColumn(usize),
    /// `SUM(col)`.
    Sum(usize),
    /// `AVG(col)`.
    Avg(usize),
    /// `MIN(col)`.
    Min(usize),
    /// `MAX(col)`.
    Max(usize),
}

/// Parse an aggregate into the metadata-answerable kinds it requests, in output
/// order. Returns `None` if the aggregate has a `GROUP BY`, a `FILTER`, a
/// `LIMIT`, or any expression this rule cannot answer from statistics.
fn parse_stats_aggregates(aggregate: &AggregateExec) -> Option<Vec<StatsAggKind>> {
    if !matches!(
        aggregate.mode(),
        AggregateMode::Single | AggregateMode::SinglePartitioned | AggregateMode::Partial
    ) {
        return None;
    }

    // Whole-table only: no grouping, no FILTER clause, no LIMIT. For a global
    // aggregate (no `GROUP BY`) `group_expr().expr()` is empty and
    // `groups()` is empty (a `GROUP BY` would give one grouping set);
    // `has_grouping_set()` rejects `GROUPING SETS`/`ROLLUP`/`CUBE`.
    if !aggregate.group_expr().expr().is_empty()
        || aggregate.group_expr().has_grouping_set()
        || aggregate.limit_options().is_some()
        || aggregate.filter_expr().iter().any(Option::is_some)
    {
        return None;
    }

    let input_schema = aggregate.input().schema();
    let mut kinds = Vec::with_capacity(aggregate.aggr_expr().len());

    for aggregate_expr in aggregate.aggr_expr() {
        if aggregate_expr.is_distinct()
            || !aggregate_expr.order_bys().is_empty()
            || aggregate_expr.is_reversed()
        {
            return None;
        }

        let name = aggregate_expr.fun().name().to_ascii_lowercase();
        let expressions = aggregate_expr.expressions();

        let kind = match name.as_str() {
            "count" => match count_target(&expressions)? {
                CountTarget::Column(index) => StatsAggKind::CountColumn(index),
                CountTarget::AllRows => StatsAggKind::CountStar,
            },
            "sum" => StatsAggKind::Sum(single_column_index(&expressions, &input_schema)?),
            "avg" => StatsAggKind::Avg(single_column_index(&expressions, &input_schema)?),
            "min" => StatsAggKind::Min(single_column_index(&expressions, &input_schema)?),
            "max" => StatsAggKind::Max(single_column_index(&expressions, &input_schema)?),
            _ => return None,
        };

        // Bounds-check column references against the input schema so a later
        // out-of-range index access cannot panic.
        if let Some(index) = kind.column_index()
            && index >= input_schema.fields().len()
        {
            return None;
        }

        kinds.push(kind);
    }

    Some(kinds)
}

impl StatsAggKind {
    const fn column_index(self) -> Option<usize> {
        match self {
            Self::CountStar => None,
            Self::CountColumn(i) | Self::Sum(i) | Self::Avg(i) | Self::Min(i) | Self::Max(i) => {
                Some(i)
            }
        }
    }
}

/// What a `COUNT` aggregate counts, once its argument has been analyzed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CountTarget {
    /// `COUNT(*)` / `COUNT(<non-null lit>)` — the table cardinality.
    AllRows,
    /// `COUNT(col)` — the non-null count of the column at this input index.
    Column(usize),
}

/// `COUNT` argument analysis: `AllRows` for `COUNT(*)`/`COUNT(<non-null lit>)`,
/// `Column(index)` for `COUNT(col)`. Returns `None` (decline) for anything else
/// (multiple args, expressions, null literal).
fn count_target(expressions: &[Arc<dyn PhysicalExpr>]) -> Option<CountTarget> {
    match expressions {
        [] => Some(CountTarget::AllRows),
        [expr] => {
            if let Some(column) = expr.downcast_ref::<Column>() {
                Some(CountTarget::Column(column.index()))
            } else if let Some(literal) = expr.downcast_ref::<Literal>() {
                // COUNT(1) counts all rows; COUNT(NULL) counts none and must not
                // be folded as COUNT(*).
                (!literal.value().is_null()).then_some(CountTarget::AllRows)
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Extract the single input-column index for `SUM`/`AVG`/`MIN`/`MAX`, seeing
/// through a value-preserving numeric *widening* cast.
///
/// `DataFusion` coerces an integer argument before aggregating — e.g. `AVG(i64)`
/// becomes `avg(CAST(col AS Float64))` — so the physical input is a `CastExpr`
/// over a `Column`, not a bare `Column`. We read the aggregate from the
/// *original* column's footer stats (`sum`/`min`/`max`/`null_count`), so unwrapping is
/// sound exactly when the cast is a numeric widening: it preserves the column
/// sum (hence `AVG`), and being monotonic it preserves `MIN`/`MAX`. We then cast
/// the folded scalar to the aggregate's output type, matching the query.
fn single_column_index(
    expressions: &[Arc<dyn PhysicalExpr>],
    input_schema: &Schema,
) -> Option<usize> {
    let [expr] = expressions else {
        return None;
    };

    if let Some(column) = expr.downcast_ref::<Column>() {
        return Some(column.index());
    }

    let cast = expr.downcast_ref::<CastExpr>()?;
    let column = cast.expr().downcast_ref::<Column>()?;
    let source = input_schema.fields().get(column.index())?.data_type();
    is_numeric_widening_cast(source, cast.cast_type()).then_some(column.index())
}

/// Whether casting `source` to `target` is a value-preserving numeric widening:
/// integers widen to their family's 64-bit type or to `Float64`, floats widen to
/// `Float64`. These are the implicit coercions `DataFusion` inserts for `SUM`/`AVG`
/// and they preserve sum (and order, for `MIN`/`MAX`). Narrowing or lossy casts
/// (e.g. `Float64 -> Int32`, anything non-numeric) are rejected so we never fold
/// a stat that does not match the query's casted values.
fn is_numeric_widening_cast(source: &DataType, target: &DataType) -> bool {
    use DataType::{
        Float16, Float32, Float64, Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64,
    };
    matches!(
        (source, target),
        (Int8 | Int16 | Int32 | Int64, Int64 | Float64)
            | (UInt8 | UInt16 | UInt32 | UInt64, UInt64 | Float64)
            | (Float16 | Float32 | Float64, Float64)
    )
}

/// Build the one-row result batch for `output_aggregate` from `input_stats`, or
/// `None` if any requested aggregate cannot be answered exactly.
///
/// `query_aggregate` supplies the aggregate shape (function + column) and is the
/// `Partial` node under a `Final`/`FinalPartitioned`, or the `Single` node
/// itself. `output_aggregate` supplies the result schema (the `Final`/`Single`
/// node). `input_stats` must be the statistics of `query_aggregate.input()`.
///
/// # Errors
///
/// Returns an error only if building the Arrow batch from otherwise-valid
/// scalars fails.
pub(crate) fn stats_aggregate_batch(
    query_aggregate: &AggregateExec,
    output_aggregate: &AggregateExec,
    input_stats: &Statistics,
) -> DataFusionResult<Option<RecordBatch>> {
    let Some(kinds) = parse_stats_aggregates(query_aggregate) else {
        return Ok(None);
    };

    let schema = output_aggregate.schema();
    // No GROUP BY, so every output column is one folded aggregate.
    if schema.fields().len() != kinds.len() {
        return Ok(None);
    }

    let Precision::Exact(num_rows) = input_stats.num_rows else {
        return Ok(None);
    };

    let mut columns = Vec::with_capacity(kinds.len());
    for (field, kind) in schema.fields().iter().zip(&kinds) {
        let Some(scalar) = scalar_for_kind(*kind, num_rows, input_stats, field)? else {
            return Ok(None);
        };
        columns.push(scalar.to_array_of_size(1)?);
    }

    RecordBatch::try_new(schema, columns)
        .map(Some)
        .map_err(|source| DataFusionError::ArrowError(Box::new(source), None))
}

/// Compute the folded scalar for one aggregate, cast to the output field type.
/// `None` means "cannot answer exactly" — the caller bails.
fn scalar_for_kind(
    kind: StatsAggKind,
    num_rows: usize,
    stats: &Statistics,
    field: &FieldRef,
) -> DataFusionResult<Option<ScalarValue>> {
    let column = |index: usize| -> Option<&ColumnStatistics> { stats.column_statistics.get(index) };

    let scalar = match kind {
        StatsAggKind::CountStar => {
            let Ok(count) = i64::try_from(num_rows) else {
                return Ok(None);
            };
            Some(ScalarValue::Int64(Some(count)))
        }
        StatsAggKind::CountColumn(index) => {
            let Some(non_null) = non_null_count(num_rows, column(index)) else {
                return Ok(None);
            };
            let Ok(count) = i64::try_from(non_null) else {
                return Ok(None);
            };
            Some(ScalarValue::Int64(Some(count)))
        }
        StatsAggKind::Sum(index) => match column(index).map(|c| &c.sum_value) {
            Some(Precision::Exact(sum)) => Some(sum.clone()),
            _ => return Ok(None),
        },
        StatsAggKind::Min(index) => match column(index).map(|c| &c.min_value) {
            Some(Precision::Exact(min)) => Some(min.clone()),
            _ => return Ok(None),
        },
        StatsAggKind::Max(index) => match column(index).map(|c| &c.max_value) {
            Some(Precision::Exact(max)) => Some(max.clone()),
            _ => return Ok(None),
        },
        StatsAggKind::Avg(index) => {
            let Some(non_null) = non_null_count(num_rows, column(index)) else {
                return Ok(None);
            };
            let Some(Precision::Exact(sum)) = column(index).map(|c| c.sum_value.clone()) else {
                return Ok(None);
            };
            // AVG over zero non-null rows is SQL NULL.
            if non_null == 0 {
                return Ok(Some(cast_scalar(ScalarValue::Float64(None), field)?));
            }
            let Some(sum) = scalar_to_f64(&sum) else {
                return Ok(None);
            };
            let Some(count) = usize_to_exact_f64(non_null) else {
                return Ok(None);
            };
            Some(ScalarValue::Float64(Some(sum / count)))
        }
    };

    scalar.map(|scalar| cast_scalar(scalar, field)).transpose()
}

/// Non-null row count `num_rows - null_count`, only when the null count is
/// `Exact`. Returns `None` (decline) otherwise.
fn non_null_count(num_rows: usize, column: Option<&ColumnStatistics>) -> Option<usize> {
    match column.map(|c| c.null_count) {
        Some(Precision::Exact(nulls)) => num_rows.checked_sub(nulls),
        _ => None,
    }
}

/// Cast a folded scalar to the aggregate's output field type so the produced
/// batch matches the plan's schema exactly.
fn cast_scalar(scalar: ScalarValue, field: &FieldRef) -> DataFusionResult<ScalarValue> {
    if &scalar.data_type() == field.data_type() {
        return Ok(scalar);
    }
    scalar.cast_to(field.data_type())
}

/// Convert a numeric scalar to `f64` for AVG. Returns `None` for non-numeric or
/// null inputs (callers handle the null/empty case separately).
///
/// Declines exact integer sums whose magnitude exceeds 2^53: those cannot be
/// represented exactly in f64, so dividing them would make the AVG fold
/// silently inexact. This mirrors the denominator guard in `usize_to_exact_f64`
/// and keeps the fold sound, falling back to a real scan instead.
fn scalar_to_f64(scalar: &ScalarValue) -> Option<f64> {
    const MAX_EXACT_F64_INTEGER: u128 = 1_u128 << f64::MANTISSA_DIGITS;
    if let Some(magnitude) = integer_scalar_magnitude(scalar)
        && magnitude > MAX_EXACT_F64_INTEGER
    {
        return None;
    }
    match scalar.cast_to(&DataType::Float64) {
        Ok(ScalarValue::Float64(Some(value))) => Some(value),
        _ => None,
    }
}

/// Magnitude of an exact integer scalar, or `None` for non-integer (float /
/// decimal) scalars, which carry their own inherent precision and are not
/// subject to the exact-integer-range check. SUM widens integer columns to
/// `Int64`/`UInt64`, but the smaller variants are handled too for safety.
fn integer_scalar_magnitude(scalar: &ScalarValue) -> Option<u128> {
    let signed = match scalar {
        ScalarValue::Int8(Some(v)) => Some(i64::from(*v)),
        ScalarValue::Int16(Some(v)) => Some(i64::from(*v)),
        ScalarValue::Int32(Some(v)) => Some(i64::from(*v)),
        ScalarValue::Int64(Some(v)) => Some(*v),
        _ => None,
    };
    if let Some(v) = signed {
        return Some(i128::from(v).unsigned_abs());
    }
    match scalar {
        ScalarValue::UInt8(Some(v)) => Some(u128::from(*v)),
        ScalarValue::UInt16(Some(v)) => Some(u128::from(*v)),
        ScalarValue::UInt32(Some(v)) => Some(u128::from(*v)),
        ScalarValue::UInt64(Some(v)) => Some(u128::from(*v)),
        _ => None,
    }
}

/// Convert a count to `f64`, declining if it falls outside the range where every
/// integer is exactly representable (±2^53). Beyond that an AVG computed via f64
/// division could be silently inexact, so we fall back to a real scan.
fn usize_to_exact_f64(value: usize) -> Option<f64> {
    const MAX_EXACT_F64_INTEGER: u64 = 1_u64 << f64::MANTISSA_DIGITS;
    let value = u64::try_from(value).ok()?;
    if value > MAX_EXACT_F64_INTEGER {
        return None;
    }
    #[expect(
        clippy::cast_precision_loss,
        reason = "value is range-checked to <= 2^53 above, where u64 -> f64 is exact"
    )]
    Some(value as f64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::physical_expr::aggregate::AggregateExprBuilder;
    use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
    use datafusion_functions_aggregate::average::avg_udaf;
    use datafusion_functions_aggregate::count::count_udaf;
    use datafusion_functions_aggregate::min_max::{max_udaf, min_udaf};
    use datafusion_functions_aggregate::sum::sum_udaf;
    use datafusion_physical_expr::aggregate::AggregateFunctionExpr;
    use datafusion_physical_expr::expressions::{cast, col, lit};

    fn value_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            true,
        )]))
    }

    /// One-column value stats: `value` column with the given sum / `null_count` /
    /// min / max, over `num_rows` total rows.
    fn stats(
        num_rows: Precision<usize>,
        sum: Precision<ScalarValue>,
        null_count: Precision<usize>,
        min: Precision<ScalarValue>,
        max: Precision<ScalarValue>,
    ) -> Statistics {
        Statistics {
            num_rows,
            total_byte_size: Precision::Absent,
            column_statistics: vec![ColumnStatistics {
                null_count,
                min_value: min,
                max_value: max,
                sum_value: sum,
                distinct_count: Precision::Absent,
                byte_size: Precision::Absent,
            }],
        }
    }

    /// Stats for sum=6, count=3 (rows 1,2,3), min=1, max=3 — used by most tests.
    fn exact_stats() -> Statistics {
        stats(
            Precision::Exact(3),
            Precision::Exact(ScalarValue::Int64(Some(6))),
            Precision::Exact(0),
            Precision::Exact(ScalarValue::Int64(Some(1))),
            Precision::Exact(ScalarValue::Int64(Some(3))),
        )
    }

    /// Build a `Single`-mode whole-table aggregate over a `value: Int64` input.
    fn single_aggregate(aggrs: Vec<Arc<AggregateFunctionExpr>>) -> Arc<AggregateExec> {
        let schema = value_schema();
        let input = MemorySourceConfig::try_new_exec(&[vec![]], Arc::clone(&schema), None)
            .expect("input exec");
        let filters = vec![None; aggrs.len()];
        Arc::new(
            AggregateExec::try_new(
                AggregateMode::Single,
                PhysicalGroupBy::new_single(vec![]),
                aggrs,
                filters,
                input,
                schema,
            )
            .expect("aggregate exec"),
        )
    }

    fn agg(
        udaf: datafusion_expr::AggregateUDF,
        arg: Arc<dyn PhysicalExpr>,
        alias: &str,
    ) -> Arc<AggregateFunctionExpr> {
        Arc::new(
            AggregateExprBuilder::new(Arc::new(udaf), vec![arg])
                .schema(value_schema())
                .alias(alias.to_string())
                .build()
                .expect("aggregate expr"),
        )
    }

    fn value_col() -> Arc<dyn PhysicalExpr> {
        col("value", &value_schema()).expect("value column")
    }

    /// The single folded scalar from the rewrite of `exec` against `stats`.
    fn folded_scalar(exec: &AggregateExec, stats: &Statistics) -> Option<ScalarValue> {
        let batch = stats_aggregate_batch(exec, exec, stats).expect("no error")?;
        assert_eq!(batch.num_rows(), 1, "fold must produce exactly one row");
        Some(ScalarValue::try_from_array(batch.column(0), 0).expect("scalar"))
    }

    #[test]
    fn folds_sum_from_exact_stats() {
        let exec = single_aggregate(vec![agg(
            sum_udaf().as_ref().clone(),
            value_col(),
            "sum(value)",
        )]);
        assert_eq!(
            folded_scalar(&exec, &exact_stats()),
            Some(ScalarValue::Int64(Some(6)))
        );
    }

    #[test]
    fn folds_count_star() {
        let exec = single_aggregate(vec![agg(
            count_udaf().as_ref().clone(),
            lit(1_i64),
            "count(*)",
        )]);
        assert_eq!(
            folded_scalar(&exec, &exact_stats()),
            Some(ScalarValue::Int64(Some(3)))
        );
    }

    #[test]
    fn folds_count_column_as_num_rows_minus_nulls() {
        let stats = stats(
            Precision::Exact(5),
            Precision::Exact(ScalarValue::Int64(Some(6))),
            Precision::Exact(2), // 2 nulls -> COUNT(value) = 3
            Precision::Exact(ScalarValue::Int64(Some(1))),
            Precision::Exact(ScalarValue::Int64(Some(3))),
        );
        let exec = single_aggregate(vec![agg(
            count_udaf().as_ref().clone(),
            value_col(),
            "count(value)",
        )]);
        assert_eq!(
            folded_scalar(&exec, &stats),
            Some(ScalarValue::Int64(Some(3)))
        );
    }

    #[test]
    fn folds_min_and_max() {
        let min_exec = single_aggregate(vec![agg(
            min_udaf().as_ref().clone(),
            value_col(),
            "min(value)",
        )]);
        let max_exec = single_aggregate(vec![agg(
            max_udaf().as_ref().clone(),
            value_col(),
            "max(value)",
        )]);
        assert_eq!(
            folded_scalar(&min_exec, &exact_stats()),
            Some(ScalarValue::Int64(Some(1)))
        );
        assert_eq!(
            folded_scalar(&max_exec, &exact_stats()),
            Some(ScalarValue::Int64(Some(3)))
        );
    }

    #[test]
    fn folds_avg_over_widening_cast() {
        // DataFusion coerces AVG(Int64) to avg(CAST(value AS Float64)); the fold
        // must see through that cast and compute 6/3 = 2.0.
        let cast_arg = cast(value_col(), &value_schema(), DataType::Float64).expect("cast");
        let exec = single_aggregate(vec![agg(
            avg_udaf().as_ref().clone(),
            cast_arg,
            "avg(value)",
        )]);
        assert_eq!(
            folded_scalar(&exec, &exact_stats()),
            Some(ScalarValue::Float64(Some(2.0)))
        );
    }

    #[test]
    fn avg_over_zero_non_null_rows_is_null() {
        // All rows null: COUNT(value) = 0, so AVG is SQL NULL (not a divide-by-zero).
        let stats = stats(
            Precision::Exact(3),
            Precision::Exact(ScalarValue::Int64(Some(0))),
            Precision::Exact(3),
            Precision::Absent,
            Precision::Absent,
        );
        let cast_arg = cast(value_col(), &value_schema(), DataType::Float64).expect("cast");
        let exec = single_aggregate(vec![agg(
            avg_udaf().as_ref().clone(),
            cast_arg,
            "avg(value)",
        )]);
        assert_eq!(
            folded_scalar(&exec, &stats),
            Some(ScalarValue::Float64(None))
        );
    }

    #[test]
    fn does_not_fold_avg_with_oversized_integer_sum() {
        // An exact integer SUM beyond 2^53 cannot be represented exactly in f64,
        // so the AVG fold would be silently inexact -> decline and let it scan.
        let oversized = (1_i64 << f64::MANTISSA_DIGITS) + 1;
        let stats = stats(
            Precision::Exact(3),
            Precision::Exact(ScalarValue::Int64(Some(oversized))),
            Precision::Exact(0),
            Precision::Absent,
            Precision::Absent,
        );
        let cast_arg = cast(value_col(), &value_schema(), DataType::Float64).expect("cast");
        let exec = single_aggregate(vec![agg(
            avg_udaf().as_ref().clone(),
            cast_arg,
            "avg(value)",
        )]);
        assert!(
            stats_aggregate_batch(&exec, &exec, &stats)
                .expect("no error")
                .is_none(),
            "oversized integer sum must not fold AVG"
        );
    }

    #[test]
    fn does_not_fold_inexact_sum() {
        let stats = stats(
            Precision::Exact(3),
            Precision::Inexact(ScalarValue::Int64(Some(6))), // inexact -> must not fold
            Precision::Exact(0),
            Precision::Exact(ScalarValue::Int64(Some(1))),
            Precision::Exact(ScalarValue::Int64(Some(3))),
        );
        let exec = single_aggregate(vec![agg(
            sum_udaf().as_ref().clone(),
            value_col(),
            "sum(value)",
        )]);
        assert!(
            stats_aggregate_batch(&exec, &exec, &stats)
                .expect("no error")
                .is_none(),
            "inexact sum must not fold"
        );
    }

    #[test]
    fn does_not_fold_inexact_num_rows() {
        let stats = stats(
            Precision::Inexact(3), // inexact row count -> count/avg unsafe
            Precision::Exact(ScalarValue::Int64(Some(6))),
            Precision::Exact(0),
            Precision::Exact(ScalarValue::Int64(Some(1))),
            Precision::Exact(ScalarValue::Int64(Some(3))),
        );
        let exec = single_aggregate(vec![agg(
            count_udaf().as_ref().clone(),
            lit(1_i64),
            "count(*)",
        )]);
        assert!(
            stats_aggregate_batch(&exec, &exec, &stats)
                .expect("no error")
                .is_none(),
            "inexact num_rows must not fold COUNT(*)"
        );
    }

    #[test]
    fn does_not_fold_absent_sum() {
        // Absent sum (e.g. a file with deletes) -> SUM must not fold.
        let stats = stats(
            Precision::Exact(3),
            Precision::Absent,
            Precision::Exact(0),
            Precision::Exact(ScalarValue::Int64(Some(1))),
            Precision::Exact(ScalarValue::Int64(Some(3))),
        );
        let exec = single_aggregate(vec![agg(
            sum_udaf().as_ref().clone(),
            value_col(),
            "sum(value)",
        )]);
        assert!(
            stats_aggregate_batch(&exec, &exec, &stats)
                .expect("no error")
                .is_none(),
            "absent sum must not fold"
        );
    }

    #[test]
    fn folds_mixed_rollup() {
        // COUNT(*), SUM, MIN, MAX together — DataFusion's built-in rule declines
        // (SUM unsupported), so this rule must fold the whole node.
        let exec = single_aggregate(vec![
            agg(count_udaf().as_ref().clone(), lit(1_i64), "count(*)"),
            agg(sum_udaf().as_ref().clone(), value_col(), "sum(value)"),
            agg(min_udaf().as_ref().clone(), value_col(), "min(value)"),
            agg(max_udaf().as_ref().clone(), value_col(), "max(value)"),
        ]);
        let batch = stats_aggregate_batch(&exec, &exec, &exact_stats())
            .expect("no error")
            .expect("rollup should fold");
        let scalars: Vec<ScalarValue> = (0..batch.num_columns())
            .map(|i| ScalarValue::try_from_array(batch.column(i), 0).expect("scalar"))
            .collect();
        assert_eq!(
            scalars,
            vec![
                ScalarValue::Int64(Some(3)),
                ScalarValue::Int64(Some(6)),
                ScalarValue::Int64(Some(1)),
                ScalarValue::Int64(Some(3)),
            ]
        );
    }
}
