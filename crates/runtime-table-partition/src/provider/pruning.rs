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

use arrow_schema::{Field, Schema};

// Constants for bucket enumeration limits
const MAX_BUCKET_ENUMERATION_I32: i32 = 10_000;
const MAX_BUCKET_ENUMERATION_I64: i64 = 10_000;

// Constants for date_trunc granularity calculations
const NANOS_PER_SECOND: i64 = 1_000_000_000;
const NANOS_PER_MINUTE: i64 = 60 * NANOS_PER_SECOND;
const NANOS_PER_HOUR: i64 = 60 * NANOS_PER_MINUTE;
const NANOS_PER_DAY: i64 = 24 * NANOS_PER_HOUR;
const NANOS_PER_WEEK: i64 = 7 * NANOS_PER_DAY;
const NANOS_PER_MONTH: i64 = 30 * NANOS_PER_DAY; // Approximate
const NANOS_PER_YEAR: i64 = 365 * NANOS_PER_DAY; // Approximate
use datafusion::{
    common::{
        Column, ToDFSchema as _,
        tree_node::{Transformed, TreeNode as _},
    },
    config::ConfigOptions,
    error::DataFusionError,
    execution::context::ExecutionProps,
    logical_expr::{
        BinaryExpr, ColumnarValue, Operator, ScalarFunctionArgs, ScalarUDF,
        expr::{InList, ScalarFunction},
        interval_arithmetic::NullableInterval,
        simplify::SimplifyContext,
    },
    optimizer::simplify_expressions::ExprSimplifier,
    prelude::Expr,
    scalar::ScalarValue,
};

/// Collects conditions (equalities or inequalities) from nested expressions for a given operator.
fn collect_conditions(
    expr: &Expr,
    combining_op: Operator,
    condition_op: Operator,
) -> Option<(Column, Vec<ScalarValue>)> {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) if *op == combining_op => {
            let left_result = collect_conditions(left, combining_op, condition_op);
            let right_result = collect_conditions(right, combining_op, condition_op);
            match (left_result, right_result) {
                (Some((col_left, mut lits_left)), Some((col_right, lits_right)))
                    if col_left == col_right =>
                {
                    lits_left.extend(lits_right);
                    Some((col_left, lits_left))
                }
                _ => None,
            }
        }
        Expr::BinaryExpr(BinaryExpr { left, op, right }) if *op == condition_op => {
            match (left.as_ref(), right.as_ref()) {
                (Expr::Column(col), Expr::Literal(lit, _))
                | (Expr::Literal(lit, _), Expr::Column(col)) => {
                    Some((col.clone(), vec![lit.clone()]))
                }
                _ => None,
            }
        }
        _ => None,
    }
}

/// Transforms `partition_by` expression by replacing column with `filter_value` and evaluates it.
fn transform_and_evaluate(
    partition_by: &Expr,
    col: &Column,
    filter_value: &ScalarValue,
    schema: &Schema,
) -> Result<ScalarValue, DataFusionError> {
    let transformed_expr = partition_by
        .clone()
        .transform(|e| {
            Ok(match e {
                Expr::Column(expr_col) if expr_col.name() == col.name() => {
                    Transformed::yes(Expr::Literal(filter_value.clone(), None))
                }
                _ => Transformed::no(e),
            })
        })
        .map_err(|e| DataFusionError::Plan(format!("Failed to transform expression: {e}")))?
        .data;

    evaluate_expr(
        &transformed_expr,
        schema,
        vec![(
            Expr::Column(col.clone()),
            NullableInterval::from(filter_value.clone()),
        )],
    )
}

/// Evaluates an expression to a scalar value using `ExprSimplifier`, falling back to direct evaluation if needed.
fn evaluate_expr(
    expr: &Expr,
    schema: &Schema,
    guarantees: Vec<(Expr, NullableInterval)>,
) -> Result<ScalarValue, DataFusionError> {
    let dfschema = schema.clone().to_dfschema_ref()?;
    let props = ExecutionProps::new();
    let context = SimplifyContext::new(&props).with_schema(dfschema);
    let simplifier = ExprSimplifier::new(context).with_guarantees(guarantees.clone());

    let simplified_expr = simplifier.simplify(expr.clone())?;

    if let Expr::Literal(lit, _) = simplified_expr {
        return Ok(lit);
    }

    // Fallback to direct evaluation if simplification doesn't yield a literal
    // An example of this occurs in `test_prune_partition_case` because regex_match
    // function is used and the Simplifier cannot simplify to a literal
    match &simplified_expr {
        Expr::Literal(lit, _) => Ok(lit.clone()),
        Expr::ScalarFunction(ScalarFunction { func, args }) => {
            let args = args
                .iter()
                .map(|arg| evaluate_expr(arg, schema, guarantees.clone()))
                .collect::<Result<Vec<_>, _>>()?;
            call(func.as_ref(), args)
        }
        Expr::Case(case) => {
            for (when, then) in &case.when_then_expr {
                let condition = evaluate_expr(when, schema, guarantees.clone())?;
                if matches!(condition, ScalarValue::Boolean(Some(true))) {
                    return evaluate_expr(then, schema, guarantees);
                }
            }
            if let Some(else_expr) = &case.else_expr {
                evaluate_expr(else_expr, schema, guarantees)
            } else {
                Ok(ScalarValue::Null)
            }
        }
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            let left_val = evaluate_expr(left, schema, guarantees.clone())?;
            let right_val = evaluate_expr(right, schema, guarantees)?;
            match op {
                Operator::Plus => left_val.add(&right_val),
                Operator::Minus => left_val.sub(&right_val),
                Operator::Multiply => left_val.mul(&right_val),
                Operator::Divide => left_val.div(&right_val),
                Operator::Modulo => left_val.rem(&right_val),
                Operator::Gt => Ok(ScalarValue::Boolean(Some(
                    left_val.partial_cmp(&right_val) == Some(std::cmp::Ordering::Greater),
                ))),
                Operator::GtEq => Ok(ScalarValue::Boolean(Some(
                    left_val.partial_cmp(&right_val) != Some(std::cmp::Ordering::Less),
                ))),
                Operator::Lt => Ok(ScalarValue::Boolean(Some(
                    left_val.partial_cmp(&right_val) == Some(std::cmp::Ordering::Less),
                ))),
                Operator::LtEq => Ok(ScalarValue::Boolean(Some(
                    left_val.partial_cmp(&right_val) != Some(std::cmp::Ordering::Greater),
                ))),
                Operator::Eq => Ok(ScalarValue::Boolean(Some(
                    left_val.partial_cmp(&right_val) == Some(std::cmp::Ordering::Equal),
                ))),
                _ => Err(DataFusionError::Plan(
                    "Unsupported binary operator".to_string(),
                )),
            }
        }
        _ => Err(DataFusionError::Plan(
            "Unsupported expression type".to_string(),
        )),
    }
}

/// Evaluates if a filter expression excludes a partition value based on the partition-by expression.
#[allow(clippy::too_many_lines)]
pub(crate) fn prune_partition(
    filters: &[Expr],
    partition_by: &Expr,
    partition_value: &ScalarValue,
    schema: &Schema, // Added schema parameter
) -> Result<bool, DataFusionError> {
    let partition_by_columns = partition_by.column_refs();

    // Special handling for bucket expressions with inequalities
    // Bucket needs access to all filters to determine bounded ranges
    if let Expr::ScalarFunction(ScalarFunction { func, args }) = partition_by
        && func.name() == "bucket"
        && args.len() == 2
        && let (Expr::Literal(bucket_count, _), Expr::Column(col)) = (&args[0], &args[1])
    {
        // Check if we have inequality filters on this column
        let has_inequality = filters.iter().any(|f| {
            matches!(
                f,
                Expr::BinaryExpr(BinaryExpr {
                    op: Operator::Gt
                        | Operator::GtEq
                        | Operator::Lt
                        | Operator::LtEq
                        | Operator::NotEq,
                    ..
                }) | Expr::InList(InList { negated: true, .. })
            )
        });

        if has_inequality {
            return evaluate_bucket_inequality(
                filters,
                col,
                partition_by,
                partition_value,
                func,
                bucket_count,
            );
        }
    }

    for filter in filters {
        // Skip if the filter does not contain the columns in the partition_by Expr
        if filter
            .column_refs()
            .iter()
            .any(|col| !partition_by_columns.contains(col))
        {
            continue;
        }

        match filter {
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                match (left.as_ref(), op, right.as_ref()) {
                    // Direct partition expression match: e.g., bucket(10, user_id) = 0
                    (expr, Operator::Eq, Expr::Literal(lit, _)) if expr == partition_by => {
                        // Direct comparison: does the partition value match the filter literal?
                        if partition_value != lit {
                            return Ok(true); // Prune this partition
                        }
                    }
                    (Expr::Literal(lit, _), Operator::Eq, expr) if expr == partition_by => {
                        // Direct comparison: does the partition value match the filter literal?
                        if partition_value != lit {
                            return Ok(true); // Prune this partition
                        }
                    }
                    (Expr::Column(col), Operator::Eq, Expr::Literal(lit, _))
                    | (Expr::Literal(lit, _), Operator::Eq, Expr::Column(col)) => {
                        if !filter_or_udf_value_matches(
                            col,
                            partition_by,
                            partition_value,
                            lit,
                            schema,
                        )? {
                            return Ok(true);
                        }
                    }
                    (
                        Expr::Column(col),
                        op @ (Operator::Gt | Operator::GtEq | Operator::Lt | Operator::LtEq),
                        Expr::Literal(lit, _),
                    )
                    | (
                        Expr::Literal(lit, _),
                        op @ (Operator::Gt | Operator::GtEq | Operator::Lt | Operator::LtEq),
                        Expr::Column(col),
                    ) => {
                        if evaluate_inequality(
                            col,
                            *op,
                            lit,
                            partition_by,
                            partition_value,
                            schema,
                        )? {
                            return Ok(true);
                        }
                    }
                    _ => {
                        if let Some((col_name, literals)) =
                            collect_conditions(filter, Operator::Or, Operator::Eq)
                        {
                            let mut any_matches = false;
                            for lit in literals {
                                let is_match = filter_or_udf_value_matches(
                                    &col_name,
                                    partition_by,
                                    partition_value,
                                    &lit,
                                    schema,
                                )?;
                                any_matches |= is_match;
                            }
                            if !any_matches {
                                return Ok(true);
                            }
                        } else if let Some((col_name, literals)) =
                            collect_conditions(filter, Operator::And, Operator::NotEq)
                        {
                            for lit in literals {
                                let is_match = filter_or_udf_value_matches(
                                    &col_name,
                                    partition_by,
                                    partition_value,
                                    &lit,
                                    schema,
                                )?;
                                if is_match {
                                    return Ok(true);
                                }
                            }
                        }
                    }
                }
            }
            Expr::InList(InList {
                expr,
                list,
                negated,
            }) => {
                if let Expr::Column(col) = expr.as_ref() {
                    let mut any_matches = false;
                    for lit in list {
                        if let Expr::Literal(lit_val, _) = lit {
                            let is_match = filter_or_udf_value_matches(
                                col,
                                partition_by,
                                partition_value,
                                lit_val,
                                schema,
                            )?;
                            if is_match && *negated {
                                return Ok(true);
                            }
                            any_matches |= is_match;
                        }
                    }
                    if !any_matches && !negated {
                        return Ok(true);
                    }
                }
            }
            Expr::ScalarFunction(ScalarFunction { func, args }) => {
                let result =
                    evaluate_function_filter(func, args, partition_by, partition_value, schema)?;
                if !result {
                    return Ok(true);
                }
            }
            _ => {}
        }
    }

    Ok(false)
}

/// Evaluates if the `partition_by` expression with the column substituted by `filter_value` equals `partition_value`.
fn filter_or_udf_value_matches(
    col: &Column,
    partition_by: &Expr,
    partition_value: &ScalarValue,
    filter_value: &ScalarValue,
    schema: &Schema,
) -> Result<bool, DataFusionError> {
    let result = transform_and_evaluate(partition_by, col, filter_value, schema)?;
    Ok(&result == partition_value)
}

/// Evaluates inequality conditions to determine if they exclude the partition value.
#[allow(clippy::too_many_lines)]
fn evaluate_inequality(
    col: &Column,
    op: Operator,
    filter_value: &ScalarValue,
    partition_by: &Expr,
    partition_value: &ScalarValue,
    _schema: &Schema,
) -> Result<bool, DataFusionError> {
    // Check if partition_by is a simple column expression
    if let Expr::Column(partition_col) = partition_by
        && partition_col.name() == col.name()
    {
        // For simple column partitions, directly compare partition_value with filter_value
        // Return true if this partition should be pruned (filter doesn't match)
        let matches = match op {
            Operator::Gt => partition_value > filter_value,
            Operator::GtEq => partition_value >= filter_value,
            Operator::Lt => partition_value < filter_value,
            Operator::LtEq => partition_value <= filter_value,
            _ => return Err(DataFusionError::Plan("Unsupported operator".to_string())),
        };
        return Ok(!matches); // Prune if doesn't match
    }

    // For complex partition expressions, analyze specific function types
    if let Expr::ScalarFunction(ScalarFunction { func, args }) = partition_by {
        let func_name = func.name();

        // Handle truncate(step, col) - partition represents range [partition_value, partition_value + step)
        if func_name == "truncate"
            && args.len() == 2
            && let (Expr::Literal(step_lit, _), Expr::Column(partition_col)) = (&args[0], &args[1])
            && partition_col.name() == col.name()
        {
            return evaluate_truncate_inequality(step_lit, partition_value, filter_value, op);
        }

        // Handle date_trunc(granularity, col) - partition represents temporal range based on granularity
        if func_name == "date_trunc"
            && args.len() == 2
            && let (Expr::Literal(granularity_lit, _), Expr::Column(partition_col)) =
                (&args[0], &args[1])
            && partition_col.name() == col.name()
        {
            return evaluate_date_trunc_inequality(
                granularity_lit,
                partition_value,
                filter_value,
                op,
            );
        }
    }

    // Handle modulo expression: col % divisor
    if let Expr::BinaryExpr(BinaryExpr {
        left,
        op: Operator::Modulo,
        right,
    }) = partition_by
        && let Expr::Column(partition_col) = left.as_ref()
        && let Expr::Literal(divisor_lit, _) = right.as_ref()
        && partition_col.name() == col.name()
    {
        return evaluate_modulo_inequality(divisor_lit, partition_value, filter_value, op);
    }

    // Conservative approach for other complex expressions
    Ok(false)
}

/// Special handler for bucket inequality that needs access to all filters to determine bounded ranges.
/// This is called from `prune_partition` when we detect a bucket expression.
#[allow(clippy::too_many_lines)]
fn evaluate_bucket_inequality(
    filters: &[Expr],
    col: &Column,
    _partition_by: &Expr,
    partition_value: &ScalarValue,
    func: &Arc<ScalarUDF>,
    bucket_count: &ScalarValue,
) -> Result<bool, DataFusionError> {
    // Collect all inequality filters on this column to determine bounded range
    let mut lower_bound: Option<(ScalarValue, bool)> = None; // (value, inclusive)
    let mut upper_bound: Option<(ScalarValue, bool)> = None; // (value, inclusive)

    for filter in filters {
        if let Expr::BinaryExpr(BinaryExpr { left, op, right }) = filter {
            match (left.as_ref(), op, right.as_ref()) {
                (Expr::Column(filter_col), op, Expr::Literal(lit, _))
                | (Expr::Literal(lit, _), op, Expr::Column(filter_col))
                    if filter_col.name() == col.name() =>
                {
                    match op {
                        Operator::Gt => {
                            lower_bound = Some((lit.clone(), false));
                        }
                        Operator::GtEq => {
                            lower_bound = Some((lit.clone(), true));
                        }
                        Operator::Lt => {
                            upper_bound = Some((lit.clone(), false));
                        }
                        Operator::LtEq => {
                            upper_bound = Some((lit.clone(), true));
                        }
                        Operator::NotEq => {
                            // For bucket partitions with != don't prune any partitions
                            return Ok(false);
                        }
                        _ => {}
                    }
                }
                _ => {}
            }
        } else if let Expr::InList(InList { negated: true, .. }) = filter {
            // Handle negated IN list (NOT IN)
            // For bucket partitioning with NOT IN, don't prune any partitions
            return Ok(false);
        }
    }

    // If we have a bounded range (both lower and upper), enumerate values and check buckets
    if let (Some((lower, lower_inc)), Some((upper, upper_inc))) = (lower_bound, upper_bound) {
        // Only handle integer types for now (can extend to other types)
        match (&lower, &upper) {
            (ScalarValue::Int32(Some(l)), ScalarValue::Int32(Some(u))) => {
                #[allow(clippy::items_after_statements, clippy::cast_sign_loss)]
                {
                    let start = if lower_inc { *l } else { l + 1 };
                    let end = if upper_inc { *u } else { u - 1 };

                    // Early exit if range is invalid
                    if start > end {
                        return Ok(true); // Prune - empty range
                    }

                    // Limit enumeration to reasonable range to avoid performance issues
                    if end - start > MAX_BUCKET_ENUMERATION_I32 {
                        return Ok(false); // Conservative: don't prune for very large ranges
                    }

                    // Use vectorized approach: build array once
                    use datafusion::arrow::array::Int32Array;

                    let range_size = (end - start + 1) as usize;
                    let values: Int32Array = (start..=end).collect();

                    // Extract partition value for fast comparison (avoid repeated ScalarValue operations)
                    let target_bucket = if let ScalarValue::Int32(Some(pv)) = partition_value {
                        *pv
                    } else {
                        return Ok(false); // Conservative if partition value isn't Int32
                    };

                    // Extract bucket_count once to avoid cloning on every iteration
                    let bucket_count_value = bucket_count.clone();

                    // Batch process: check if any value hashes to target bucket
                    for i in 0..range_size {
                        let hashed = call(
                            func,
                            vec![
                                bucket_count_value.clone(),
                                ScalarValue::Int32(Some(values.value(i))),
                            ],
                        )?;
                        if let ScalarValue::Int32(Some(h)) = hashed
                            && h == target_bucket
                        {
                            return Ok(false); // Early exit - found matching bucket
                        }
                    }
                    return Ok(true); // Prune - no values in range hash to this partition
                }
            }
            (ScalarValue::Int64(Some(l)), ScalarValue::Int64(Some(u))) => {
                #[allow(
                    clippy::items_after_statements,
                    clippy::cast_sign_loss,
                    clippy::cast_possible_truncation
                )]
                {
                    let start = if lower_inc { *l } else { l + 1 };
                    let end = if upper_inc { *u } else { u - 1 };

                    // Early exit if range is invalid
                    if start > end {
                        return Ok(true); // Prune - empty range
                    }

                    if end - start > MAX_BUCKET_ENUMERATION_I64 {
                        return Ok(false);
                    }

                    // Use vectorized approach: build array once
                    use datafusion::arrow::array::Int64Array;

                    let range_size = (end - start + 1) as usize;
                    let values: Int64Array = (start..=end).collect();

                    // Extract partition value for fast comparison
                    let target_bucket = if let ScalarValue::Int32(Some(pv)) = partition_value {
                        *pv
                    } else {
                        return Ok(false); // Conservative if partition value isn't Int32
                    };

                    // Extract bucket_count once to avoid cloning on every iteration
                    let bucket_count_value = bucket_count.clone();

                    // Batch process with early exit
                    for i in 0..range_size {
                        let hashed = call(
                            func,
                            vec![
                                bucket_count_value.clone(),
                                ScalarValue::Int64(Some(values.value(i))),
                            ],
                        )?;
                        if let ScalarValue::Int32(Some(h)) = hashed
                            && h == target_bucket
                        {
                            return Ok(false); // Early exit - found matching bucket
                        }
                    }
                    return Ok(true);
                }
            }
            _ => {}
        }
    }

    // Conservative: if unbounded or unsupported type, don't prune
    Ok(false)
}

/// Evaluates inequality for modulo partitions using statistics-based pruning.
/// For col % divisor = `partition_value`, the values that map to this partition form
/// an arithmetic sequence: `partition_value`, `partition_value` + divisor, `partition_value` + 2*divisor, ...
/// We can prune if we know the filter range doesn't contain any values from this sequence.
fn evaluate_modulo_inequality(
    divisor: &ScalarValue,
    partition_value: &ScalarValue,
    filter_value: &ScalarValue,
    op: Operator,
) -> Result<bool, DataFusionError> {
    // Fast path for integer types with direct arithmetic
    match (divisor, partition_value, filter_value) {
        (
            ScalarValue::Int32(Some(d)),
            ScalarValue::Int32(Some(pv)),
            ScalarValue::Int32(Some(fv)),
        ) => {
            // Partition represents: pv, pv + d, pv + 2d, pv + 3d, ...
            // For negative values: ..., pv - 3d, pv - 2d, pv - d, pv, pv + d, ...

            // Check if any value in the arithmetic sequence satisfies the inequality
            let can_satisfy = match op {
                Operator::Gt => {
                    // col > fv: Need pv + k*d > fv for some integer k
                    // If pv > fv, satisfied immediately (k=0)
                    // Otherwise, need k > (fv - pv) / d, which means k >= ceil((fv - pv + 1) / d)
                    // Since sequence is infinite in positive direction, always satisfiable if d > 0
                    *pv > *fv || *d > 0
                }
                Operator::GtEq => {
                    // col >= fv: Need pv + k*d >= fv
                    *pv >= *fv || *d > 0
                }
                Operator::Lt => {
                    // col < fv: Need pv + k*d < fv
                    *pv < *fv || *d < 0
                }
                Operator::LtEq => {
                    // col <= fv: Need pv + k*d <= fv
                    *pv <= *fv || *d < 0
                }
                _ => return Err(DataFusionError::Plan("Unsupported operator".to_string())),
            };

            return Ok(!can_satisfy); // Prune if no value can satisfy
        }
        (
            ScalarValue::Int64(Some(d)),
            ScalarValue::Int64(Some(pv)),
            ScalarValue::Int64(Some(fv)),
        ) => {
            let can_satisfy = match op {
                Operator::Gt => *pv > *fv || *d > 0,
                Operator::GtEq => *pv >= *fv || *d > 0,
                Operator::Lt => *pv < *fv || *d < 0,
                Operator::LtEq => *pv <= *fv || *d < 0,
                _ => return Err(DataFusionError::Plan("Unsupported operator".to_string())),
            };

            return Ok(!can_satisfy);
        }
        _ => {}
    }

    // Conservative fallback for unsupported types
    Ok(false)
}

/// Evaluates inequality for truncate(step, col) partitions.
/// Partition value represents range [`partition_value`, `partition_value` + step).
fn evaluate_truncate_inequality(
    step: &ScalarValue,
    partition_value: &ScalarValue,
    filter_value: &ScalarValue,
    op: Operator,
) -> Result<bool, DataFusionError> {
    // Fast path for integer types - avoid ScalarValue arithmetic overhead
    match (partition_value, step, filter_value) {
        (
            ScalarValue::Int32(Some(pv)),
            ScalarValue::Int32(Some(s)),
            ScalarValue::Int32(Some(fv)),
        ) => {
            let partition_upper = pv + s;
            let overlaps = match op {
                Operator::Gt | Operator::GtEq => partition_upper > *fv,
                Operator::Lt => *pv < *fv,
                Operator::LtEq => *pv <= *fv,
                _ => return Err(DataFusionError::Plan("Unsupported operator".to_string())),
            };
            return Ok(!overlaps);
        }
        (
            ScalarValue::Int64(Some(pv)),
            ScalarValue::Int64(Some(s)),
            ScalarValue::Int64(Some(fv)),
        ) => {
            let partition_upper = pv + s;
            let overlaps = match op {
                Operator::Gt | Operator::GtEq => partition_upper > *fv,
                Operator::Lt => *pv < *fv,
                Operator::LtEq => *pv <= *fv,
                _ => return Err(DataFusionError::Plan("Unsupported operator".to_string())),
            };
            return Ok(!overlaps);
        }
        _ => {}
    }

    // Fallback to ScalarValue arithmetic for other types
    let partition_upper = partition_value.add(step)?;

    // Check if the partition range [partition_value, partition_upper) overlaps with the filter
    let overlaps = match op {
        Operator::Gt => {
            // col > filter_value: prune if partition_upper <= filter_value (all values in partition <= filter)
            &partition_upper > filter_value
        }
        Operator::GtEq => {
            // col >= filter_value: prune if partition_upper <= filter_value
            &partition_upper > filter_value
        }
        Operator::Lt => {
            // col < filter_value: prune if partition_value >= filter_value (all values in partition >= filter)
            partition_value < filter_value
        }
        Operator::LtEq => {
            // col <= filter_value: prune if partition_value > filter_value
            partition_value <= filter_value
        }
        _ => return Err(DataFusionError::Plan("Unsupported operator".to_string())),
    };

    Ok(!overlaps) // Prune if no overlap
}

/// Evaluates inequality for `date_trunc(granularity`, col) partitions.
/// Partition value represents the start of a temporal range based on granularity.
fn evaluate_date_trunc_inequality(
    granularity: &ScalarValue,
    partition_value: &ScalarValue,
    filter_value: &ScalarValue,
    op: Operator,
) -> Result<bool, DataFusionError> {
    let ScalarValue::Utf8(Some(gran)) = granularity else {
        return Ok(false); // Conservative: don't prune if granularity not a string
    };

    // Extract timestamp from partition_value
    let partition_ts = match partition_value {
        ScalarValue::TimestampNanosecond(Some(ts), _) => *ts,
        ScalarValue::TimestampMicrosecond(Some(ts), _) => ts * 1_000,
        ScalarValue::TimestampMillisecond(Some(ts), _) => ts * 1_000_000,
        ScalarValue::TimestampSecond(Some(ts), _) => ts * NANOS_PER_SECOND,
        _ => return Ok(false), // Conservative: not a timestamp
    };

    // Compute the upper bound based on granularity
    let nanos_in_granularity = match gran.as_str() {
        "second" => NANOS_PER_SECOND,
        "minute" => NANOS_PER_MINUTE,
        "hour" => NANOS_PER_HOUR,
        "day" => NANOS_PER_DAY,
        "week" => NANOS_PER_WEEK,
        "month" => NANOS_PER_MONTH,
        "year" => NANOS_PER_YEAR,
        _ => return Ok(false), // Unknown granularity, be conservative
    };

    let partition_upper_ts = partition_ts + nanos_in_granularity;

    // Create upper bound ScalarValue matching the partition_value type
    let partition_upper = match partition_value {
        ScalarValue::TimestampNanosecond(_, tz) => {
            ScalarValue::TimestampNanosecond(Some(partition_upper_ts), tz.clone())
        }
        ScalarValue::TimestampMicrosecond(_, tz) => {
            ScalarValue::TimestampMicrosecond(Some(partition_upper_ts / 1_000), tz.clone())
        }
        ScalarValue::TimestampMillisecond(_, tz) => {
            ScalarValue::TimestampMillisecond(Some(partition_upper_ts / 1_000_000), tz.clone())
        }
        ScalarValue::TimestampSecond(_, tz) => {
            ScalarValue::TimestampSecond(Some(partition_upper_ts / 1_000_000_000), tz.clone())
        }
        _ => return Ok(false),
    };

    // Check if the partition range [partition_value, partition_upper) overlaps with the filter
    let overlaps = match op {
        Operator::Gt => {
            // date > filter_value: prune if partition_upper <= filter_value
            &partition_upper > filter_value
        }
        Operator::GtEq => {
            // date >= filter_value: prune if partition_upper <= filter_value
            &partition_upper > filter_value
        }
        Operator::Lt => {
            // date < filter_value: prune if partition_value >= filter_value
            partition_value < filter_value
        }
        Operator::LtEq => {
            // date <= filter_value: prune if partition_value > filter_value
            partition_value <= filter_value
        }
        _ => return Err(DataFusionError::Plan("Unsupported operator".to_string())),
    };

    Ok(!overlaps) // Prune if no overlap
}

/// Evaluates a function-based filter (e.g., `date_trunc`, truncate).
fn evaluate_function_filter(
    func: &Arc<ScalarUDF>,
    args: &[Expr],
    partition_by: &Expr,
    partition_value: &ScalarValue,
    schema: &Schema,
) -> Result<bool, DataFusionError> {
    let evaluated_args = args
        .iter()
        .map(|arg| match arg {
            Expr::Literal(lit, _) => Ok(lit.clone()),
            Expr::Column(col) => transform_and_evaluate(partition_by, col, partition_value, schema),
            _ => Err(DataFusionError::Plan(
                "Unsupported argument type".to_string(),
            )),
        })
        .collect::<Result<Vec<_>, _>>()?;

    let result = call(func, evaluated_args)?;
    Ok(&result == partition_value)
}

fn call(f: &ScalarUDF, args: Vec<ScalarValue>) -> Result<ScalarValue, DataFusionError> {
    let arg_types = args.iter().map(ScalarValue::data_type).collect::<Vec<_>>();
    let return_type = f.return_type(&arg_types)?;
    let args = args.into_iter().map(ColumnarValue::Scalar).collect();

    let return_field = Arc::new(Field::new("ignored_name", return_type, false));

    let args = ScalarFunctionArgs {
        args,
        arg_fields: vec![],
        number_rows: 1,
        return_field,
        config_options: Arc::new(ConfigOptions::default()),
    };

    let ColumnarValue::Scalar(bucket_value) = f.invoke_with_args(args)? else {
        return Err(DataFusionError::Plan("Expected scalar value".to_string()));
    };

    Ok(bucket_value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, TimeUnit};
    use chrono::{NaiveDateTime, TimeZone as _, Utc};
    use datafusion::{
        functions::regex::regexp_match,
        prelude::{case, col, date_trunc, in_list, lit},
    };
    use runtime_datafusion_udfs::{bucket, truncate};
    use std::sync::Arc;

    macro_rules! assert_prune_partition {
        ($filters:expr, $partition_by:expr, $schema:expr, $scalar_variant:ident, [$(($val:expr, $should_prune:expr)),*]) => {
            $(
                let partition_value = ScalarValue::$scalar_variant(Some($val));
                assert_eq!(
                    prune_partition($filters, &$partition_by, &partition_value, &$schema)?,
                    $should_prune,
                    "partition_value = {partition_value:?}, should_prune = {}",
                    $should_prune,
                );
            )*
        };
    }

    fn timestamp_nanos(datetime: &str) -> i64 {
        let naive =
            NaiveDateTime::parse_from_str(datetime, "%Y-%m-%d %H:%M:%S").expect("datetime parse");

        // Assume UTC; convert NaiveDateTime to a DateTime<Utc>
        let datetime_utc = Utc.from_utc_datetime(&naive);

        datetime_utc
            .timestamp_nanos_opt()
            .expect("timestamp_nanos_opt is ok")
    }

    #[test]
    fn test_prune_partition_multiple_columns() -> Result<(), DataFusionError> {
        let schema = Schema::new(vec![
            Field::new("region", DataType::Utf8, false),
            Field::new("col2", DataType::Utf8, false),
        ]);
        let partition_by = col("region");
        let filters = &[col("col2").eq(partition_by.clone())];
        assert_prune_partition!(
            filters,
            &partition_by,
            schema,
            Utf8,
            [("us-east-1".into(), false)]
        );
        Ok(())
    }

    #[test]
    fn test_prune_partition_exact_match() -> Result<(), DataFusionError> {
        let schema = Schema::new(vec![Field::new("region", DataType::Utf8, false)]);
        let partition_by = col("region");
        let region = "us-east-2";
        let filters = &[col("region").eq(lit(region))];
        assert_prune_partition!(
            filters,
            &partition_by,
            schema,
            Utf8,
            [("us-east-2".into(), false), ("ap-northeast-2".into(), true)]
        );
        Ok(())
    }

    #[test]
    fn test_prune_partition_inlist() -> Result<(), DataFusionError> {
        let schema = Schema::new(vec![Field::new("account_id", DataType::Int32, false)]);
        let partition_by = col("account_id");
        let filters = &[in_list(
            partition_by.clone(),
            vec![lit(1), lit(2), lit(3)],
            false,
        )];
        assert_prune_partition!(
            filters,
            &partition_by,
            schema,
            Int32,
            [
                (1, false),
                (2, false),
                (3, false),
                (4, true),
                (5, true),
                (6, true)
            ]
        );
        Ok(())
    }

    #[test]
    fn test_prune_partition_not_inlist() -> Result<(), DataFusionError> {
        let schema = Schema::new(vec![Field::new("account_id", DataType::Int32, false)]);
        let partition_by = col("account_id");
        let filters = &[in_list(
            partition_by.clone(),
            vec![lit(1), lit(2), lit(3)],
            true,
        )];
        assert_prune_partition!(
            filters,
            &partition_by,
            schema,
            Int32,
            [
                (1, true),
                (2, true),
                (3, true),
                (4, false),
                (5, false),
                (6, false)
            ]
        );
        Ok(())
    }

    #[test]
    fn test_prune_partition_or_equalities_2_items() -> Result<(), DataFusionError> {
        let schema = Schema::new(vec![Field::new("account_id", DataType::Int32, false)]);
        let partition_by = col("account_id");
        let filter = col("account_id")
            .eq(lit(1))
            .or(col("account_id").eq(lit(2)));
        assert_prune_partition!(
            std::slice::from_ref(&filter),
            &partition_by,
            schema,
            Int32,
            [(1, false), (2, false), (3, true), (4, true)]
        );
        Ok(())
    }

    #[test]
    fn test_prune_partition_or_equalities_3_items() -> Result<(), DataFusionError> {
        let schema = Schema::new(vec![Field::new("account_id", DataType::Int32, false)]);
        let partition_by = col("account_id");
        let filter = col("account_id")
            .eq(lit(1))
            .or(col("account_id").eq(lit(2)))
            .or(col("account_id").eq(lit(3)));
        assert_prune_partition!(
            std::slice::from_ref(&filter),
            &partition_by,
            schema,
            Int32,
            [
                (1, false),
                (2, false),
                (3, false),
                (4, true),
                (5, true),
                (6, true)
            ]
        );
        Ok(())
    }

    #[test]
    fn test_prune_partition_and_inequalities_2_items() -> Result<(), DataFusionError> {
        let schema = Schema::new(vec![Field::new("account_id", DataType::Int32, false)]);
        let partition_by = col("account_id");
        let filter = col("account_id")
            .not_eq(lit(1))
            .and(col("account_id").not_eq(lit(2)));
        assert_prune_partition!(
            std::slice::from_ref(&filter),
            &partition_by,
            schema,
            Int32,
            [(1, true), (2, true), (3, false), (4, false)]
        );
        Ok(())
    }

    #[test]
    fn test_prune_partition_and_inequalities_3_items() -> Result<(), DataFusionError> {
        let schema = Schema::new(vec![Field::new("account_id", DataType::Int32, false)]);
        let partition_by = col("account_id");
        let filter = col("account_id")
            .not_eq(lit(1))
            .and(col("account_id").not_eq(lit(2)))
            .and(col("account_id").not_eq(lit(3)));
        assert_prune_partition!(
            std::slice::from_ref(&filter),
            &partition_by,
            schema,
            Int32,
            [
                (1, true),
                (2, true),
                (3, true),
                (4, false),
                (5, false),
                (6, false)
            ]
        );
        Ok(())
    }

    fn bucket_expr(args: Vec<Expr>) -> Expr {
        let func = Arc::new(ScalarUDF::new_from_impl(bucket::Bucket::new()));
        Expr::ScalarFunction(ScalarFunction { func, args })
    }

    #[test]
    fn test_prune_partition_hash_exact() -> Result<(), DataFusionError> {
        let schema = Schema::new(vec![Field::new("region", DataType::Utf8, false)]);
        let partition_by = bucket_expr(vec![lit(10i64), col("region")]);
        let region = "us-east-2";
        let filters = &[col("region").eq(lit(region))];
        let f = ScalarUDF::new_from_impl(bucket::Bucket::new());
        let ScalarValue::Int32(Some(us_east_2)) = call(
            &f,
            vec![
                ScalarValue::Int64(Some(10)),
                ScalarValue::Utf8(Some(region.into())),
            ],
        )?
        else {
            panic!("expected Int32");
        };
        let ScalarValue::Int32(Some(ap_northeast_2)) = call(
            &f,
            vec![
                ScalarValue::Int64(Some(10)),
                ScalarValue::Utf8(Some("ap-northeast-2".into())),
            ],
        )?
        else {
            panic!("expected Int32");
        };
        assert_prune_partition!(
            filters,
            &partition_by,
            schema,
            Int32,
            [(us_east_2, false), (ap_northeast_2, true)]
        );
        Ok(())
    }

    #[test]
    fn test_prune_partition_hash_inlist() -> Result<(), DataFusionError> {
        let schema = Schema::new(vec![Field::new("account_id", DataType::Int32, false)]);
        let partition_by = bucket_expr(vec![lit(10i64), col("account_id")]);
        let filters = &[in_list(
            col("account_id"),
            vec![lit(1), lit(2), lit(3)],
            false,
        )];
        let f = ScalarUDF::new_from_impl(bucket::Bucket::new());
        let hashed_values = (1..=6)
            .map(|i| {
                let ScalarValue::Int32(Some(val)) = call(
                    &f,
                    vec![ScalarValue::Int64(Some(10)), ScalarValue::Int32(Some(i))],
                )?
                else {
                    panic!("expected Int32");
                };
                Ok(val)
            })
            .collect::<Result<Vec<_>, DataFusionError>>()?;
        for (val, should_prune) in hashed_values.into_iter().zip((1..=6).map(|i| i > 3)) {
            let partition_value = ScalarValue::Int32(Some(val));
            assert_eq!(
                prune_partition(filters.as_slice(), &partition_by, &partition_value, &schema)?,
                should_prune,
                "partition_value = {partition_value:?}, should_prune = {should_prune}",
            );
        }
        Ok(())
    }

    #[test]
    fn test_prune_partition_bucket_with_not_inlist() -> Result<(), DataFusionError> {
        // Test Case: user_id NOT IN (10, 20, 30)
        // For bucket partitioning with NOT IN, no partitions should be pruned because:
        // - Each bucket can contain multiple values
        // - Even if a bucket contains one of the excluded values, it likely contains other values not in the list

        let schema = Schema::new(vec![Field::new("user_id", DataType::Int32, false)]);
        let partition_by = Expr::ScalarFunction(ScalarFunction {
            func: Arc::new(ScalarUDF::new_from_impl(bucket::Bucket::new())),
            args: vec![lit(10_i64), col("user_id")],
        });

        let filters_not_in = &[in_list(
            col("user_id"),
            vec![lit(10_i32), lit(20_i32), lit(30_i32)],
            true, // NOT IN
        )];

        for partition_value in 0..10_i32 {
            let pruned = prune_partition(
                &filters_not_in[..],
                &partition_by,
                &ScalarValue::Int32(Some(partition_value)),
                &schema,
            )?;
            assert!(
                !pruned,
                "Partition {partition_value} should not be pruned for user_id NOT IN (10, 20, 30) (bucket partitioning)",
            );
        }
        Ok(())
    }

    #[test]
    fn test_prune_partition_hash_and_inequalities_3_items() -> Result<(), DataFusionError> {
        let schema = Schema::new(vec![Field::new("account_id", DataType::Int32, false)]);
        let partition_by = bucket_expr(vec![lit(10i64), col("account_id")]);
        let filter = col("account_id")
            .not_eq(lit(1))
            .and(col("account_id").not_eq(lit(2)))
            .and(col("account_id").not_eq(lit(3)));
        let f = ScalarUDF::new_from_impl(bucket::Bucket::new());
        let hashed_values = (1..=6)
            .map(|i| {
                let ScalarValue::Int32(Some(val)) = call(
                    &f,
                    vec![ScalarValue::Int64(Some(10)), ScalarValue::Int32(Some(i))],
                )?
                else {
                    panic!("expected Int32");
                };
                Ok(val)
            })
            .collect::<Result<Vec<_>, DataFusionError>>()?;
        for (val, should_prune) in hashed_values.into_iter().zip((1..=6).map(|i| i <= 3)) {
            let partition_value = ScalarValue::Int32(Some(val));
            assert_eq!(
                prune_partition(
                    std::slice::from_ref(&filter),
                    &partition_by,
                    &partition_value,
                    &schema
                )?,
                should_prune,
                "partition_value = {partition_value:?}, should_prune = {should_prune}",
            );
        }
        Ok(())
    }

    #[test]
    fn test_prune_partition_region() -> Result<(), DataFusionError> {
        let schema = Schema::new(vec![Field::new("region", DataType::Utf8, false)]);
        let partition_by = col("region");
        let filters = &[col("region").eq(lit("us-east-2"))];
        assert_prune_partition!(
            filters,
            &partition_by,
            schema,
            Utf8,
            [("us-east-2".into(), false), ("ap-northeast-2".into(), true)]
        );
        Ok(())
    }

    #[test]
    fn test_prune_partition_greater_than() -> Result<(), DataFusionError> {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let partition_by = col("a").gt(lit(5));
        let filters = &[col("a").eq(lit(4))];
        assert_prune_partition!(
            filters,
            &partition_by,
            schema,
            Boolean,
            [(true, true), (false, false)]
        );
        Ok(())
    }

    #[test]
    fn test_prune_partition_modulo() -> Result<(), DataFusionError> {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let partition_by = col("a") % lit(10);
        let filters = &[col("a").eq(lit(12))];
        assert_prune_partition!(
            filters,
            &partition_by,
            schema,
            Int32,
            [(2, false), (3, true)]
        );
        Ok(())
    }

    #[test]
    fn test_prune_partition_case() -> Result<(), DataFusionError> {
        let schema = Schema::new(vec![Field::new("a", DataType::Utf8, false)]);
        let partition_by = case(Expr::ScalarFunction(ScalarFunction {
            func: regexp_match(),
            args: vec![col("a"), lit("^DATAFUSION(-cli)*")],
        }))
        .when(lit(true), lit("datafusion"))
        .otherwise(lit("other"))?;
        let filters = &[col("a").eq(lit("DATAFUSION-cli"))];
        assert_prune_partition!(
            filters,
            &partition_by,
            schema,
            Utf8,
            [("datafusion".into(), false), ("other".into(), true)]
        );
        Ok(())
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn test_prune_partition_date_trunc() -> Result<(), DataFusionError> {
        let schema = Schema::new(vec![Field::new(
            "date",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        )]);
        let filter_date =
            ScalarValue::TimestampNanosecond(Some(timestamp_nanos("2025-07-15 00:00:00")), None);
        let filters = &[col("date").eq(lit(filter_date.clone()))];

        // Test multiple granularities
        let granularities = vec!["year", "month", "day", "hour", "minute", "second"];
        for granularity in granularities {
            let partition_by = date_trunc(lit(granularity), col("date"));

            // Define partition values for each granularity
            let partition_values = match granularity {
                "year" => vec![
                    (
                        ScalarValue::TimestampNanosecond(
                            Some(timestamp_nanos("2025-01-01 00:00:00")),
                            None,
                        ),
                        false,
                    ),
                    (
                        ScalarValue::TimestampNanosecond(
                            Some(timestamp_nanos("2026-01-01 00:00:00")),
                            None,
                        ),
                        true,
                    ),
                ],
                "month" => vec![
                    (
                        ScalarValue::TimestampNanosecond(
                            Some(timestamp_nanos("2025-07-01 00:00:00")),
                            None,
                        ),
                        false,
                    ),
                    (
                        ScalarValue::TimestampNanosecond(
                            Some(timestamp_nanos("2025-08-01 00:00:00")),
                            None,
                        ),
                        true,
                    ),
                ],
                "day" => vec![
                    (
                        ScalarValue::TimestampNanosecond(
                            Some(timestamp_nanos("2025-07-15 00:00:00")),
                            None,
                        ),
                        false,
                    ),
                    (
                        ScalarValue::TimestampNanosecond(
                            Some(timestamp_nanos("2025-07-16 00:00:00")),
                            None,
                        ),
                        true,
                    ),
                ],
                "hour" => vec![
                    (
                        ScalarValue::TimestampNanosecond(
                            Some(timestamp_nanos("2025-07-15 00:00:00")),
                            None,
                        ),
                        false,
                    ),
                    (
                        ScalarValue::TimestampNanosecond(
                            Some(timestamp_nanos("2025-07-15 01:00:00")),
                            None,
                        ),
                        true,
                    ),
                ],
                "minute" => vec![
                    (
                        ScalarValue::TimestampNanosecond(
                            Some(timestamp_nanos("2025-07-15 00:00:00")),
                            None,
                        ),
                        false,
                    ),
                    (
                        ScalarValue::TimestampNanosecond(
                            Some(timestamp_nanos("2025-07-15 00:01:00")),
                            None,
                        ),
                        true,
                    ),
                ],
                "second" => vec![
                    (
                        ScalarValue::TimestampNanosecond(
                            Some(timestamp_nanos("2025-07-15 00:00:00")),
                            None,
                        ),
                        false,
                    ),
                    (
                        ScalarValue::TimestampNanosecond(
                            Some(timestamp_nanos("2025-07-15 00:00:01")),
                            None,
                        ),
                        true,
                    ),
                ],
                _ => vec![],
            };

            for (partition_value, should_prune) in partition_values {
                assert_eq!(
                    prune_partition(filters.as_slice(), &partition_by, &partition_value, &schema)?,
                    should_prune,
                    "granularity = {granularity}, partition_value = {partition_value:?}, should_prune = {should_prune}"
                );
            }
        }
        Ok(())
    }

    #[test]
    fn test_prune_partition_truncate() -> Result<(), DataFusionError> {
        let schema = Schema::new(vec![Field::new("sales_volume", DataType::Int64, false)]);
        let partition_by = Expr::ScalarFunction(ScalarFunction {
            func: Arc::new(ScalarUDF::new_from_impl(truncate::Truncate::new())),
            args: vec![lit(1000i64), col("sales_volume")],
        });
        let filters = &[col("sales_volume").eq(lit(1234i64))];
        assert_prune_partition!(
            filters,
            &partition_by,
            schema,
            Int64,
            [(1000, false), (2000, true)]
        );
        Ok(())
    }

    #[test]
    fn test_prune_partition_bucket() -> Result<(), DataFusionError> {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let partition_by = Expr::ScalarFunction(ScalarFunction {
            func: Arc::new(ScalarUDF::new_from_impl(bucket::Bucket::new())),
            args: vec![lit(10i64), col("a")],
        });
        let filters = &[in_list(col("a"), vec![lit(1), lit(2), lit(3)], false)];
        let f = ScalarUDF::new_from_impl(bucket::Bucket::new());
        let hashed_values = (1..=6)
            .map(|i| {
                let ScalarValue::Int32(Some(val)) = call(
                    &f,
                    vec![ScalarValue::Int64(Some(10)), ScalarValue::Int32(Some(i))],
                )?
                else {
                    panic!("expected Int32");
                };
                Ok(val)
            })
            .collect::<Result<Vec<_>, DataFusionError>>()?;
        for (val, should_prune) in hashed_values.into_iter().zip((1..=6).map(|i| i > 3)) {
            let partition_value = ScalarValue::Int32(Some(val));
            assert_eq!(
                prune_partition(&filters[..], &partition_by, &partition_value, &schema)?,
                should_prune,
                "partition_value = {partition_value:?}, should_prune = {should_prune}",
            );
        }
        Ok(())
    }

    #[test]
    fn test_prune_partition_modulo_with_inequality() -> Result<(), DataFusionError> {
        // Test that modulo partitions with inequality filters ARE correctly pruned
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let partition_by = col("a") % lit(10);

        // Filter: a > 25
        // Partition value 0 represents values like 0, 10, 20, 30, 40...
        // Values 30, 40, 50... satisfy a > 25, so partition 0 should NOT be pruned
        // Partition value 5 represents values like 5, 15, 25, 35, 45...
        // Values 35, 45, 55... satisfy a > 25, so partition 5 should NOT be pruned
        // Partition value 9 represents values like 9, 19, 29, 39...
        // Values 29, 39, 49... satisfy a > 25, so partition 9 should NOT be pruned
        let filters = &[col("a").gt(lit(25))];

        // All partitions have values > 25, so none should be pruned
        assert_prune_partition!(
            filters,
            &partition_by,
            schema,
            Int32,
            [(0, false), (5, false), (9, false)]
        );

        // Filter: a > 95
        // Partition 0: 0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100... -> 100+ satisfy
        // Partition 5: 5, 15, 25, 35, 45, 55, 65, 75, 85, 95, 105... -> 105+ satisfy
        // Partition 6: 6, 16, 26, 36, 46, 56, 66, 76, 86, 96, 106... -> 96+ satisfy
        let filters_95 = &[col("a").gt(lit(95))];
        assert_prune_partition!(
            filters_95,
            &partition_by,
            schema,
            Int32,
            [(0, false), (5, false), (6, false)]
        );
        Ok(())
    }

    #[test]
    fn test_prune_partition_bucket_with_inequality() -> Result<(), DataFusionError> {
        // Test that bucket partitions with inequality filters ARE correctly pruned
        let schema = Schema::new(vec![Field::new("user_id", DataType::Int32, false)]);
        let partition_by = bucket_expr(vec![lit(10i64), col("user_id")]);

        // Filter: user_id > 50 AND user_id <= 100
        // We need to determine which buckets contain ANY user_ids in range (51, 100]
        let filters = &[col("user_id").gt(lit(50)), col("user_id").lt_eq(lit(100))];

        let f = ScalarUDF::new_from_impl(bucket::Bucket::new());

        // Compute which buckets contain values in range (50, 100]
        let mut buckets_with_matches = std::collections::HashSet::new();
        for user_id in 51..=100 {
            let ScalarValue::Int32(Some(bucket)) = call(
                &f,
                vec![
                    ScalarValue::Int64(Some(10)),
                    ScalarValue::Int32(Some(user_id)),
                ],
            )?
            else {
                panic!("expected Int32");
            };
            buckets_with_matches.insert(bucket);
        }

        // Test that buckets without matches are pruned, those with matches are not
        for partition_value in 0..10 {
            let should_prune = !buckets_with_matches.contains(&partition_value);
            assert_eq!(
                prune_partition(
                    &filters[..],
                    &partition_by,
                    &ScalarValue::Int32(Some(partition_value)),
                    &schema
                )?,
                should_prune,
                "Partition {partition_value} should{}be pruned",
                if should_prune { " " } else { " not " }
            );
        }
        Ok(())
    }

    #[test]
    fn test_prune_partition_date_trunc_with_inequality() -> Result<(), DataFusionError> {
        // Test that date_trunc partitions with inequality filters ARE correctly pruned
        let schema = Schema::new(vec![Field::new(
            "date",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        )]);
        let partition_by = date_trunc(lit("day"), col("date"));

        // Filter: date > '2025-07-15 12:00:00'
        // Partition '2025-07-14 00:00:00' (all times on July 14) should be pruned - all times are <= filter
        // Partition '2025-07-15 00:00:00' should NOT be pruned - includes times after 12:00:00
        // Partition '2025-07-16 00:00:00' should NOT be pruned - all times are > filter
        let filter_timestamp =
            ScalarValue::TimestampNanosecond(Some(timestamp_nanos("2025-07-15 12:00:00")), None);
        let filters = &[col("date").gt(lit(filter_timestamp))];

        let test_cases = [
            (timestamp_nanos("2025-07-14 00:00:00"), true), // All times <= filter
            (timestamp_nanos("2025-07-15 00:00:00"), false), // Some times > filter
            (timestamp_nanos("2025-07-16 00:00:00"), false), // All times > filter
        ];
        for (ts, should_prune) in test_cases {
            let partition_value = ScalarValue::TimestampNanosecond(Some(ts), None);
            assert_eq!(
                prune_partition(&filters[..], &partition_by, &partition_value, &schema)?,
                should_prune,
                "partition_value = {partition_value:?}, should_prune = {should_prune}"
            );
        }

        // Filter: date >= '2025-07-15 00:00:00' AND date < '2025-07-16 00:00:00'
        // Only partition '2025-07-15 00:00:00' should NOT be pruned
        let filter_start =
            ScalarValue::TimestampNanosecond(Some(timestamp_nanos("2025-07-15 00:00:00")), None);
        let filter_end =
            ScalarValue::TimestampNanosecond(Some(timestamp_nanos("2025-07-16 00:00:00")), None);
        let filters_range = &[
            col("date").gt_eq(lit(filter_start)),
            col("date").lt(lit(filter_end)),
        ];

        let test_cases_range = [
            (timestamp_nanos("2025-07-14 00:00:00"), true), // All times < filter_start
            (timestamp_nanos("2025-07-15 00:00:00"), false), // Overlaps range
            (timestamp_nanos("2025-07-16 00:00:00"), true), // All times >= filter_end
        ];
        for (ts, should_prune) in test_cases_range {
            let partition_value = ScalarValue::TimestampNanosecond(Some(ts), None);
            assert_eq!(
                prune_partition(&filters_range[..], &partition_by, &partition_value, &schema)?,
                should_prune,
                "partition_value = {partition_value:?}, should_prune = {should_prune}"
            );
        }
        Ok(())
    }

    #[test]
    fn test_prune_partition_simple_column_with_multiple_inequalities() -> Result<(), DataFusionError>
    {
        // Test that simple column partitions correctly handle multiple inequality filters
        let schema = Schema::new(vec![Field::new("age", DataType::Int32, false)]);
        let partition_by = col("age");

        // Filter: age > 18 AND age < 65
        let filters = &[col("age").gt(lit(18)), col("age").lt(lit(65))];

        assert_prune_partition!(
            filters,
            &partition_by,
            schema,
            Int32,
            [
                (17, true),
                (18, true),
                (19, false),
                (30, false),
                (64, false),
                (65, true),
                (100, true)
            ]
        );
        Ok(())
    }

    #[test]
    fn test_prune_partition_truncate_with_inequality() -> Result<(), DataFusionError> {
        // Test that truncate partitions with inequality filters ARE correctly pruned
        let schema = Schema::new(vec![Field::new("sales_volume", DataType::Int64, false)]);
        let partition_by = Expr::ScalarFunction(ScalarFunction {
            func: Arc::new(ScalarUDF::new_from_impl(truncate::Truncate::new())),
            args: vec![lit(1000i64), col("sales_volume")],
        });

        // Filter: sales_volume > 1500
        // Partition 0 represents values 0-999, all < 1500, should be pruned
        // Partition 1000 represents values 1000-1999, some (1501-1999) satisfy the filter, should NOT be pruned
        // Partition 2000 represents values 2000-2999, all > 1500, should NOT be pruned
        let filters = &[col("sales_volume").gt(lit(1500i64))];

        assert_prune_partition!(
            filters,
            &partition_by,
            schema.clone(),
            Int64,
            [
                (0, true),     // All values 0-999 <= 1500
                (1000, false), // Some values 1501-1999 > 1500
                (2000, false)  // All values 2000-2999 > 1500
            ]
        );

        // Filter: sales_volume >= 2000 AND sales_volume < 3000
        // Only partition 2000 should NOT be pruned
        let filters_range = &[
            col("sales_volume").gt_eq(lit(2000i64)),
            col("sales_volume").lt(lit(3000i64)),
        ];

        assert_prune_partition!(
            filters_range,
            &partition_by,
            schema,
            Int64,
            [
                (0, true),     // All values 0-999 < 2000
                (1000, true),  // All values 1000-1999 < 2000
                (2000, false), // All values 2000-2999 in range
                (3000, true)   // All values 3000-3999 >= 3000
            ]
        );
        Ok(())
    }

    #[test]
    #[allow(clippy::similar_names)]
    fn test_prune_partition_modulo_all_operators() -> Result<(), DataFusionError> {
        // Test all inequality operators with modulo - should correctly prune based on value ranges
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let partition_by = col("a") % lit(10);

        // Test partition value 5 (represents 5, 15, 25, 35, 45, ...)
        // Test partition value 3 (represents 3, 13, 23, 33, 43, ...)

        // Greater than: a > 30
        // Partition 5: 35, 45, 55... satisfy -> should NOT prune
        // Partition 3: 33, 43, 53... satisfy -> should NOT prune
        let filters_gt = &[col("a").gt(lit(30))];
        assert!(!prune_partition(
            &filters_gt[..],
            &partition_by,
            &ScalarValue::Int32(Some(5)),
            &schema
        )?);
        assert!(!prune_partition(
            &filters_gt[..],
            &partition_by,
            &ScalarValue::Int32(Some(3)),
            &schema
        )?);

        // Greater than or equal: a >= 25
        // Partition 5: 25, 35, 45... satisfy -> should NOT prune
        // Partition 3: 33, 43, 53... satisfy -> should NOT prune
        #[allow(clippy::similar_names)]
        let filters_gte = &[col("a").gt_eq(lit(25))];
        assert!(!prune_partition(
            &filters_gte[..],
            &partition_by,
            &ScalarValue::Int32(Some(5)),
            &schema
        )?);
        assert!(!prune_partition(
            &filters_gte[..],
            &partition_by,
            &ScalarValue::Int32(Some(3)),
            &schema
        )?);

        // Less than: a < 20
        // Partition 5: 5, 15 satisfy -> should NOT prune
        // Partition 3: 3, 13 satisfy -> should NOT prune
        #[allow(clippy::similar_names)]
        let filters_lt = &[col("a").lt(lit(20))];
        assert!(!prune_partition(
            &filters_lt[..],
            &partition_by,
            &ScalarValue::Int32(Some(5)),
            &schema
        )?);
        assert!(!prune_partition(
            &filters_lt[..],
            &partition_by,
            &ScalarValue::Int32(Some(3)),
            &schema
        )?);

        // Less than or equal: a <= 15
        // Partition 5: 5, 15 satisfy -> should NOT prune
        // Partition 3: 3, 13 satisfy -> should NOT prune
        #[allow(clippy::similar_names)]
        let filters_lte = &[col("a").lt_eq(lit(15))];
        assert!(!prune_partition(
            &filters_lte[..],
            &partition_by,
            &ScalarValue::Int32(Some(5)),
            &schema
        )?);
        assert!(!prune_partition(
            &filters_lte[..],
            &partition_by,
            &ScalarValue::Int32(Some(3)),
            &schema
        )?);

        // Test case where some partitions should be pruned: a >= 10 AND a < 20
        // Partition 5: only 15 is in range [10, 20) -> should NOT prune
        // Partition 8: only 18 is in range [10, 20) -> should NOT prune
        // Partition 0: only 10 is in range [10, 20) -> should NOT prune
        // Partition 3: only 13 is in range [10, 20) -> should NOT prune
        let filters_range = &[col("a").gt_eq(lit(10)), col("a").lt(lit(20))];
        for partition_value in 0..10 {
            // All partitions have at least one value in [10, 20) (namely, 10+partition_value)
            assert!(
                !prune_partition(
                    &filters_range[..],
                    &partition_by,
                    &ScalarValue::Int32(Some(partition_value)),
                    &schema
                )?,
                "Partition {partition_value} should not be pruned for range [10, 20)"
            );
        }

        Ok(())
    }

    #[test]
    fn test_prune_partition_date_part() -> Result<(), DataFusionError> {
        use arrow_schema::DataType;
        use datafusion::functions::datetime::date_part;
        use datafusion::prelude::*;

        let schema = Schema::new(vec![Field::new("order_date", DataType::Date32, true)]);

        // Partition by date_part('month', order_date)
        let date_part_udf = date_part();
        let partition_by = Expr::ScalarFunction(ScalarFunction::new_udf(
            Arc::clone(&date_part_udf),
            vec![lit("month"), col("order_date")],
        ));

        // Filter: date_part('month', order_date) = 3
        let filter = Expr::ScalarFunction(ScalarFunction::new_udf(
            date_part_udf,
            vec![lit("month"), col("order_date")],
        ))
        .eq(lit(3_f64));

        // Partition 3 should NOT be pruned
        assert!(
            !prune_partition(
                std::slice::from_ref(&filter),
                &partition_by,
                &ScalarValue::Float64(Some(3.0)),
                &schema
            )?,
            "Partition 3 should not be pruned for date_part = 3"
        );

        // Partition 1 should be pruned
        assert!(
            prune_partition(
                std::slice::from_ref(&filter),
                &partition_by,
                &ScalarValue::Float64(Some(1.0)),
                &schema
            )?,
            "Partition 1 should be pruned for date_part = 3"
        );

        // Partition 5 should be pruned
        assert!(
            prune_partition(
                &[filter],
                &partition_by,
                &ScalarValue::Float64(Some(5.0)),
                &schema
            )?,
            "Partition 5 should be pruned for date_part = 3"
        );

        Ok(())
    }

    #[test]
    fn test_prune_partition_bucket_uuid_direct_filter() -> Result<(), DataFusionError> {
        use arrow_schema::DataType;
        use datafusion::prelude::*;

        let schema = Schema::new(vec![Field::new("user_id", DataType::Utf8, true)]);

        // Partition by bucket(5, user_id)
        let partition_by = Expr::ScalarFunction(ScalarFunction {
            func: Arc::new(ScalarUDF::new_from_impl(bucket::Bucket::new())),
            args: vec![lit(5_i64), col("user_id")],
        });

        // Test Case 1: Filter on the UUID column directly
        // WHERE user_id = 'some-uuid-value'
        //
        // EXPECTED BEHAVIOR: Evaluate bucket(5, 'uuid-value') to determine which partition
        // the UUID belongs to, then only scan that partition. This enables partition pruning!
        let uuid_value = "550e8400-e29b-41d4-a716-446655440000";
        let filter = col("user_id").eq(lit(uuid_value));

        // Compute which bucket this UUID hashes to
        let f = ScalarUDF::new_from_impl(bucket::Bucket::new());
        let ScalarValue::Int32(Some(expected_bucket)) = call(
            &f,
            vec![
                ScalarValue::Int64(Some(5)),
                ScalarValue::Utf8(Some(uuid_value.into())),
            ],
        )?
        else {
            panic!("expected Int32");
        };

        // Only the matching partition should NOT be pruned
        for partition_value in 0..5_i32 {
            let pruned = prune_partition(
                std::slice::from_ref(&filter),
                &partition_by,
                &ScalarValue::Int32(Some(partition_value)),
                &schema,
            )?;
            let should_prune = partition_value != expected_bucket;
            assert_eq!(
                pruned,
                should_prune,
                "Partition {} should{} be pruned (UUID hashes to bucket {})",
                partition_value,
                if should_prune { "" } else { " not" },
                expected_bucket
            );
        }

        Ok(())
    }

    #[test]
    fn test_prune_partition_bucket_uuid_bucket_filter() -> Result<(), DataFusionError> {
        use arrow_schema::DataType;
        use datafusion::prelude::*;

        let schema = Schema::new(vec![Field::new("user_id", DataType::Utf8, true)]);

        // Partition by bucket(5, user_id)
        let partition_by = Expr::ScalarFunction(ScalarFunction {
            func: Arc::new(ScalarUDF::new_from_impl(bucket::Bucket::new())),
            args: vec![lit(5_i64), col("user_id")],
        });

        // Test Case 2: Filter on the bucket expression itself
        // WHERE bucket(5, user_id) = 2
        // This SHOULD enable partition pruning
        let filter = Expr::ScalarFunction(ScalarFunction {
            func: Arc::new(ScalarUDF::new_from_impl(bucket::Bucket::new())),
            args: vec![lit(5_i64), col("user_id")],
        })
        .eq(lit(2_i64));

        // Partition 2 should NOT be pruned
        assert!(
            !prune_partition(
                std::slice::from_ref(&filter),
                &partition_by,
                &ScalarValue::Int64(Some(2)),
                &schema
            )?,
            "Partition 2 should not be pruned for bucket = 2"
        );

        // Partition 0 should be pruned
        assert!(
            prune_partition(
                std::slice::from_ref(&filter),
                &partition_by,
                &ScalarValue::Int64(Some(0)),
                &schema
            )?,
            "Partition 0 should be pruned for bucket = 2"
        );

        // Partition 4 should be pruned
        assert!(
            prune_partition(
                &[filter],
                &partition_by,
                &ScalarValue::Int64(Some(4)),
                &schema
            )?,
            "Partition 4 should be pruned for bucket = 2"
        );

        Ok(())
    }

    #[test]
    fn test_prune_partition_bucket_uuid_in_filter() -> Result<(), DataFusionError> {
        use arrow_schema::DataType;
        use datafusion::prelude::*;

        let schema = Schema::new(vec![Field::new("user_id", DataType::Utf8, true)]);

        // Partition by bucket(5, user_id)
        let partition_by = Expr::ScalarFunction(ScalarFunction {
            func: Arc::new(ScalarUDF::new_from_impl(bucket::Bucket::new())),
            args: vec![lit(5_i64), col("user_id")],
        });

        // Test Case 3: Filter with multiple UUID values
        // WHERE user_id IN ('uuid1', 'uuid2', 'uuid3')
        //
        // EXPECTED BEHAVIOR: Evaluate bucket(5, uuid) for each UUID to determine which
        // partitions contain these values, then only scan those partitions.
        let uuids = vec![
            "550e8400-e29b-41d4-a716-446655440000",
            "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
            "7c9e6679-7425-40de-944b-e07fc1f90ae7",
        ];
        let uuid_list: Vec<Expr> = uuids.iter().map(|u| lit(*u)).collect();
        let filter = Expr::InList(InList {
            expr: Box::new(col("user_id")),
            list: uuid_list,
            negated: false,
        });

        // Compute which buckets these UUIDs hash to
        let f = ScalarUDF::new_from_impl(bucket::Bucket::new());
        let mut expected_buckets = std::collections::HashSet::new();
        for uuid in &uuids {
            let ScalarValue::Int32(Some(bucket)) = call(
                &f,
                vec![
                    ScalarValue::Int64(Some(5)),
                    ScalarValue::Utf8(Some((*uuid).into())),
                ],
            )?
            else {
                panic!("expected Int32");
            };
            expected_buckets.insert(bucket);
        }

        // Only partitions containing the UUIDs should NOT be pruned
        for partition_value in 0..5_i32 {
            let pruned = prune_partition(
                std::slice::from_ref(&filter),
                &partition_by,
                &ScalarValue::Int32(Some(partition_value)),
                &schema,
            )?;
            let should_prune = !expected_buckets.contains(&partition_value);
            assert_eq!(
                pruned,
                should_prune,
                "Partition {} should{} be pruned (UUIDs hash to buckets {:?})",
                partition_value,
                if should_prune { "" } else { " not" },
                expected_buckets
            );
        }

        Ok(())
    }

    #[test]
    fn test_prune_partition_bucket_integer_direct_filter() -> Result<(), DataFusionError> {
        use arrow_schema::DataType;
        use datafusion::prelude::*;

        let schema = Schema::new(vec![Field::new("user_id", DataType::Int64, true)]);

        // Partition by bucket(10, user_id)
        let partition_by = Expr::ScalarFunction(ScalarFunction {
            func: Arc::new(ScalarUDF::new_from_impl(bucket::Bucket::new())),
            args: vec![lit(10_i64), col("user_id")],
        });

        // Test Case 4: Filter on integer column directly
        // WHERE user_id = 42
        //
        // EXPECTED BEHAVIOR: Evaluate bucket(10, 42) to determine which partition
        // contains this value, then only scan that partition.
        let filter = col("user_id").eq(lit(42_i64));

        // Compute which bucket 42 hashes to
        let f = ScalarUDF::new_from_impl(bucket::Bucket::new());
        let ScalarValue::Int32(Some(expected_bucket)) = call(
            &f,
            vec![ScalarValue::Int64(Some(10)), ScalarValue::Int64(Some(42))],
        )?
        else {
            panic!("expected Int32");
        };

        // Only the matching partition should NOT be pruned
        for partition_value in 0..10_i32 {
            let pruned = prune_partition(
                std::slice::from_ref(&filter),
                &partition_by,
                &ScalarValue::Int32(Some(partition_value)),
                &schema,
            )?;
            let should_prune = partition_value != expected_bucket;
            assert_eq!(
                pruned,
                should_prune,
                "Partition {} should{} be pruned (42 hashes to bucket {})",
                partition_value,
                if should_prune { "" } else { " not" },
                expected_bucket
            );
        }

        Ok(())
    }

    #[test]
    fn test_prune_partition_modulo_uses_arithmetic_not_hashing() -> Result<(), DataFusionError> {
        use arrow_schema::DataType;
        use datafusion::prelude::*;

        // Test that modulo partitioning uses arithmetic modulo (%), not hashing like bucket()
        // This is critical because modulo is deterministic and predictable:
        // - 12 % 10 = 2
        // - 22 % 10 = 2
        // - 32 % 10 = 2
        // whereas bucket(10, 12) could hash to any bucket 0-9

        let schema = Schema::new(vec![Field::new("user_id", DataType::Int32, false)]);
        let partition_by = col("user_id") % lit(10);

        // Test Case 1: Filter user_id = 17
        // 17 % 10 = 7, so only partition 7 should NOT be pruned
        let filter_17 = col("user_id").eq(lit(17_i32));
        for partition_value in 0..10_i32 {
            let pruned = prune_partition(
                std::slice::from_ref(&filter_17),
                &partition_by,
                &ScalarValue::Int32(Some(partition_value)),
                &schema,
            )?;
            let expected_partition = 17 % 10;
            let should_prune = partition_value != expected_partition;
            assert_eq!(
                pruned,
                should_prune,
                "Partition {} should{} be pruned (17 % 10 = {})",
                partition_value,
                if should_prune { "" } else { " not" },
                expected_partition
            );
        }

        // Test Case 2: Filter user_id = 42
        // 42 % 10 = 2, so only partition 2 should NOT be pruned
        let filter_42 = col("user_id").eq(lit(42_i32));
        for partition_value in 0..10_i32 {
            let pruned = prune_partition(
                std::slice::from_ref(&filter_42),
                &partition_by,
                &ScalarValue::Int32(Some(partition_value)),
                &schema,
            )?;
            let expected_partition = 42 % 10;
            let should_prune = partition_value != expected_partition;
            assert_eq!(
                pruned,
                should_prune,
                "Partition {} should{} be pruned (42 % 10 = {})",
                partition_value,
                if should_prune { "" } else { " not" },
                expected_partition
            );
        }

        // Test Case 3: IN list with multiple values
        // user_id IN (13, 23, 33) - all map to partition 3
        let filter_in = in_list(
            col("user_id"),
            vec![lit(13_i32), lit(23_i32), lit(33_i32)],
            false,
        );
        for partition_value in 0..10_i32 {
            let pruned = prune_partition(
                std::slice::from_ref(&filter_in),
                &partition_by,
                &ScalarValue::Int32(Some(partition_value)),
                &schema,
            )?;
            // All values (13, 23, 33) map to partition 3
            let should_prune = partition_value != 3;
            assert_eq!(
                pruned,
                should_prune,
                "Partition {} should{} be pruned (13, 23, 33 all % 10 = 3)",
                partition_value,
                if should_prune { "" } else { " not" }
            );
        }

        // Test Case 4: IN list with values mapping to different partitions
        // user_id IN (11, 22, 33) -> partitions 1, 2, 3
        let filter_in_multi = in_list(
            col("user_id"),
            vec![lit(11_i32), lit(22_i32), lit(33_i32)],
            false,
        );
        for partition_value in 0..10_i32 {
            let pruned = prune_partition(
                std::slice::from_ref(&filter_in_multi),
                &partition_by,
                &ScalarValue::Int32(Some(partition_value)),
                &schema,
            )?;
            // 11 % 10 = 1, 22 % 10 = 2, 33 % 10 = 3
            let should_prune = ![1, 2, 3].contains(&partition_value);
            assert_eq!(
                pruned,
                should_prune,
                "Partition {} should{} be pruned (11→1, 22→2, 33→3)",
                partition_value,
                if should_prune { "" } else { " not" }
            );
        }

        Ok(())
    }

    #[test]
    fn test_prune_partition_modulo_vs_bucket_different_results() -> Result<(), DataFusionError> {
        use arrow_schema::DataType;
        use datafusion::prelude::*;

        // Demonstrate that modulo and bucket produce DIFFERENT partition assignments
        // for the same values, proving they use different algorithms

        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);

        // Modulo partitioning: id % 10
        let partition_by_modulo = col("id") % lit(10);

        // Bucket partitioning: bucket(10, id)
        let partition_by_bucket = Expr::ScalarFunction(ScalarFunction {
            func: Arc::new(ScalarUDF::new_from_impl(bucket::Bucket::new())),
            args: vec![lit(10_i64), col("id")],
        });

        // For value 42:
        // - Modulo: 42 % 10 = 2 (deterministic arithmetic)
        // - Bucket: bucket(10, 42) = hash(42) % 10 (could be any value 0-9)

        let filter = col("id").eq(lit(42_i32));

        // Modulo should map to partition 2
        let modulo_partition = 42 % 10;
        assert_eq!(modulo_partition, 2, "42 % 10 should equal 2");

        assert!(
            !prune_partition(
                std::slice::from_ref(&filter),
                &partition_by_modulo,
                &ScalarValue::Int32(Some(2)),
                &schema
            )?,
            "Modulo partition 2 should not be pruned for value 42"
        );

        // Bucket maps to whatever the hash function returns
        let f = ScalarUDF::new_from_impl(bucket::Bucket::new());
        let ScalarValue::Int32(Some(bucket_partition)) = call(
            &f,
            vec![ScalarValue::Int64(Some(10)), ScalarValue::Int32(Some(42))],
        )?
        else {
            panic!("expected Int32");
        };

        assert!(
            !prune_partition(
                std::slice::from_ref(&filter),
                &partition_by_bucket,
                &ScalarValue::Int32(Some(bucket_partition)),
                &schema
            )?,
            "Bucket partition {bucket_partition} should not be pruned for value 42"
        );

        // Verify that modulo and bucket produce different results for at least some values
        // (they might coincidentally match for value 42, but shouldn't match for all values)
        let test_values = [0, 1, 7, 13, 42, 99, 100, 123];
        let mut differences_found = false;

        for &value in &test_values {
            let modulo_result = value % 10;
            let ScalarValue::Int32(Some(bucket_result)) = call(
                &f,
                vec![
                    ScalarValue::Int64(Some(10)),
                    ScalarValue::Int32(Some(value)),
                ],
            )?
            else {
                panic!("expected Int32");
            };

            if modulo_result != bucket_result {
                differences_found = true;
                break;
            }
        }

        assert!(
            differences_found,
            "Modulo and bucket should produce different partition assignments for at least some values"
        );

        Ok(())
    }

    #[test]
    fn test_transform_and_evaluate_bucket() -> Result<(), DataFusionError> {
        let schema = Schema::new(vec![Field::new("user_id", DataType::Int32, false)]);
        let partition_expr = bucket_expr(vec![lit(3i64), col("user_id")]);
        let col_ref = Column::from_name("user_id");

        // Test bucket(3, 221) should evaluate consistently
        let result = transform_and_evaluate(
            &partition_expr,
            &col_ref,
            &ScalarValue::Int32(Some(221)),
            &schema,
        )?;
        assert!(matches!(result, ScalarValue::Int32(_)));

        // Verify the same value always produces the same result
        let result2 = transform_and_evaluate(
            &partition_expr,
            &col_ref,
            &ScalarValue::Int32(Some(221)),
            &schema,
        )?;
        assert_eq!(result, result2, "bucket should be deterministic");

        // Test different values produce potentially different buckets
        let result_100 = transform_and_evaluate(
            &partition_expr,
            &col_ref,
            &ScalarValue::Int32(Some(100)),
            &schema,
        )?;
        let result_200 = transform_and_evaluate(
            &partition_expr,
            &col_ref,
            &ScalarValue::Int32(Some(200)),
            &schema,
        )?;

        // All results should be in range [0, 3)
        if let ScalarValue::Int32(Some(val)) = result_100 {
            assert!(
                (0..3).contains(&val),
                "bucket result should be in [0, 3), got {val}"
            );
        }
        if let ScalarValue::Int32(Some(val)) = result_200 {
            assert!(
                (0..3).contains(&val),
                "bucket result should be in [0, 3), got {val}"
            );
        }

        Ok(())
    }

    #[test]
    fn test_transform_and_evaluate_truncate() -> Result<(), DataFusionError> {
        let schema = Schema::new(vec![Field::new("sales", DataType::Int64, false)]);
        let func = Arc::new(ScalarUDF::new_from_impl(truncate::Truncate::new()));
        let partition_expr = Expr::ScalarFunction(ScalarFunction {
            func,
            args: vec![lit(1000i64), col("sales")],
        });
        let col_ref = Column::from_name("sales");

        // Test truncate(1000, 1500) = 1000
        let result = transform_and_evaluate(
            &partition_expr,
            &col_ref,
            &ScalarValue::Int64(Some(1500)),
            &schema,
        )?;
        assert_eq!(result, ScalarValue::Int64(Some(1000)));

        // Test truncate(1000, 2750) = 2000
        let result = transform_and_evaluate(
            &partition_expr,
            &col_ref,
            &ScalarValue::Int64(Some(2750)),
            &schema,
        )?;
        assert_eq!(result, ScalarValue::Int64(Some(2000)));

        // Test truncate(1000, 999) = 0
        let result = transform_and_evaluate(
            &partition_expr,
            &col_ref,
            &ScalarValue::Int64(Some(999)),
            &schema,
        )?;
        assert_eq!(result, ScalarValue::Int64(Some(0)));

        Ok(())
    }

    #[test]
    fn test_transform_and_evaluate_date_trunc() -> Result<(), DataFusionError> {
        let schema = Schema::new(vec![Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        )]);
        let partition_expr = date_trunc(lit("day"), col("timestamp"));
        let col_ref = Column::from_name("timestamp");

        // Test date_trunc to day boundary
        let input_time = timestamp_nanos("2025-01-15 14:30:45");
        let expected_time = timestamp_nanos("2025-01-15 00:00:00");

        let result = transform_and_evaluate(
            &partition_expr,
            &col_ref,
            &ScalarValue::TimestampNanosecond(Some(input_time), None),
            &schema,
        )?;
        assert_eq!(
            result,
            ScalarValue::TimestampNanosecond(Some(expected_time), None)
        );

        Ok(())
    }

    #[test]
    fn test_transform_and_evaluate_modulo() -> Result<(), DataFusionError> {
        let schema = Schema::new(vec![Field::new("value", DataType::Int32, false)]);
        let partition_expr = col("value") % lit(10i32);
        let col_ref = Column::from_name("value");

        // Test various modulo operations
        let test_cases = vec![
            (23, 3),  // 23 % 10 = 3
            (45, 5),  // 45 % 10 = 5
            (100, 0), // 100 % 10 = 0
            (7, 7),   // 7 % 10 = 7
        ];

        for (input, expected) in test_cases {
            let result = transform_and_evaluate(
                &partition_expr,
                &col_ref,
                &ScalarValue::Int32(Some(input)),
                &schema,
            )?;
            assert_eq!(
                result,
                ScalarValue::Int32(Some(expected)),
                "modulo({input}, 10) should equal {expected}"
            );
        }

        Ok(())
    }

    #[test]
    fn test_transform_and_evaluate_simple_column() -> Result<(), DataFusionError> {
        let schema = Schema::new(vec![Field::new("region", DataType::Utf8, false)]);
        let partition_expr = col("region");
        let col_ref = Column::from_name("region");

        // Simple column reference should just return the value
        let result = transform_and_evaluate(
            &partition_expr,
            &col_ref,
            &ScalarValue::Utf8(Some("us-east-1".to_string())),
            &schema,
        )?;
        assert_eq!(result, ScalarValue::Utf8(Some("us-east-1".to_string())));

        Ok(())
    }

    #[test]
    fn test_transform_and_evaluate_date_part() -> Result<(), DataFusionError> {
        use datafusion::functions::datetime::date_part;

        let schema = Schema::new(vec![Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        )]);

        // Use the correct function for date extraction
        let date_part_udf = date_part();
        let partition_expr = Expr::ScalarFunction(ScalarFunction::new_udf(
            Arc::clone(&date_part_udf),
            vec![lit("year"), col("timestamp")],
        ));
        let col_ref = Column::from_name("timestamp");

        // Test extracting year from timestamp
        let input_time = timestamp_nanos("2024-06-15 14:30:45");

        let result = transform_and_evaluate(
            &partition_expr,
            &col_ref,
            &ScalarValue::TimestampNanosecond(Some(input_time), None),
            &schema,
        )?;

        // date_part can return Int32 or Float64 depending on the extracted part
        // For year, it returns Int32
        assert_eq!(result, ScalarValue::Int32(Some(2024)));

        Ok(())
    }

    #[test]
    fn test_prune_partition_bucket_with_not_eq() -> Result<(), DataFusionError> {
        // Test Case: user_id != 42
        // For bucket partitioning with !=, no partitions should be pruned because:
        // - Each bucket can contain multiple values
        // - Even if a bucket contains 42, it likely contains other values that are != 42
        let schema = Schema::new(vec![Field::new("user_id", DataType::Int32, false)]);
        let partition_by = Expr::ScalarFunction(ScalarFunction {
            func: Arc::new(ScalarUDF::new_from_impl(bucket::Bucket::new())),
            args: vec![lit(10_i64), col("user_id")],
        });
        let filters_not_equal = &[col("user_id").not_eq(lit(42_i32))];

        for partition_value in 0..10_i32 {
            let pruned = prune_partition(
                &filters_not_equal[..],
                &partition_by,
                &ScalarValue::Int32(Some(partition_value)),
                &schema,
            )?;
            assert!(
                !pruned,
                "Partition {partition_value} should not be pruned for user_id != 42 (bucket partitioning)",
            );
        }

        Ok(())
    }
}
