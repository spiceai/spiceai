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

use chrono::{Datelike as _, TimeZone, Timelike as _, Utc};
use datafusion::{
    arrow::temporal_conversions::NANOSECONDS,
    common::tree_node::{Transformed, TreeNode as _},
    error::DataFusionError,
    logical_expr::{
        BinaryExpr, ColumnarValue, Operator, ScalarFunctionArgs, ScalarUDF,
        expr::{InList, ScalarFunction},
    },
    prelude::Expr,
    scalar::ScalarValue,
};

/// Collects equality conditions from nested OR expressions.
fn collect_or_equalities(expr: &Expr) -> Option<(String, Vec<ScalarValue>)> {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) if *op == Operator::Or => {
            let left_result = collect_or_equalities(left);
            let right_result = collect_or_equalities(right);
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
        Expr::BinaryExpr(BinaryExpr { left, op, right }) if *op == Operator::Eq => {
            match (left.as_ref(), right.as_ref()) {
                (Expr::Column(col), Expr::Literal(lit))
                | (Expr::Literal(lit), Expr::Column(col)) => {
                    Some((col.name().to_string(), vec![lit.clone()]))
                }
                _ => None,
            }
        }
        _ => None,
    }
}

/// Collects inequality conditions from nested AND expressions.
fn collect_and_inequalities(expr: &Expr) -> Option<(String, Vec<ScalarValue>)> {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) if *op == Operator::And => {
            let left_result = collect_and_inequalities(left);
            let right_result = collect_and_inequalities(right);
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
        Expr::BinaryExpr(BinaryExpr { left, op, right }) if *op == Operator::NotEq => {
            match (left.as_ref(), right.as_ref()) {
                (Expr::Column(col), Expr::Literal(lit))
                | (Expr::Literal(lit), Expr::Column(col)) => {
                    Some((col.name().to_string(), vec![lit.clone()]))
                }
                _ => None,
            }
        }
        _ => None,
    }
}

/// Evaluates if a filter expression excludes a partition value based on the partition-by expression.
pub(crate) fn prune_partition(
    filters: &[Expr],
    partition_by: &Expr,
    partition_value: &ScalarValue,
) -> Result<bool, DataFusionError> {
    let partition_by_columns = partition_by.column_refs();

    for filter in filters {
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
                    (Expr::Column(_), Operator::Eq, Expr::Literal(lit))
                    | (Expr::Literal(lit), Operator::Eq, Expr::Column(_)) => {
                        if !filter_or_udf_value_matches(left, partition_by, partition_value, lit)? {
                            return Ok(true);
                        }
                    }
                    (
                        Expr::Column(_),
                        op @ (Operator::Gt | Operator::GtEq | Operator::Lt | Operator::LtEq),
                        Expr::Literal(lit),
                    )
                    | (
                        Expr::Literal(lit),
                        op @ (Operator::Gt | Operator::GtEq | Operator::Lt | Operator::LtEq),
                        Expr::Column(_),
                    ) => {
                        if evaluate_inequality(
                            left,
                            *op,
                            right,
                            partition_by,
                            partition_value,
                            lit,
                        )? {
                            return Ok(true);
                        }
                    }
                    _ => {
                        if let Some((col_name, literals)) = collect_or_equalities(filter) {
                            let mut any_matches = false;
                            for lit in literals {
                                let is_match = filter_or_udf_value_matches(
                                    &Expr::Column(col_name.clone().into()),
                                    partition_by,
                                    partition_value,
                                    &lit,
                                )?;
                                any_matches |= is_match;
                            }
                            if !any_matches {
                                return Ok(true);
                            }
                        } else if let Some((col_name, literals)) = collect_and_inequalities(filter)
                        {
                            for lit in literals {
                                let is_match = filter_or_udf_value_matches(
                                    &Expr::Column(col_name.clone().into()),
                                    partition_by,
                                    partition_value,
                                    &lit,
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
                if let Expr::Column(_) = expr.as_ref() {
                    let mut any_matches = false;
                    for lit in list {
                        if let Expr::Literal(lit_val) = lit {
                            let is_match = filter_or_udf_value_matches(
                                expr,
                                partition_by,
                                partition_value,
                                lit_val,
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
                let result = evaluate_function_filter(func, args, partition_by, partition_value)?;
                if !result {
                    return Ok(true);
                }
            }
            _ => {}
        }
    }

    Ok(false)
}

/// Evaluates if the partition_by expression with the column substituted by filter_value equals partition_value.
fn filter_or_udf_value_matches(
    column: &Expr,
    partition_by: &Expr,
    partition_value: &ScalarValue,
    filter_value: &ScalarValue,
) -> Result<bool, DataFusionError> {
    let Expr::Column(col) = column else {
        return Err(DataFusionError::Plan("Expected column expression".into()));
    };

    // Replace column reference with filter value in partition_by expression
    let transformed_expr = partition_by
        .clone()
        .transform(|e| {
            Ok(match e {
                Expr::Column(expr_col) if expr_col == *col => {
                    Transformed::yes(Expr::Literal(filter_value.clone()))
                }
                _ => Transformed::no(e),
            })
        })
        .map_err(|e| DataFusionError::Plan(format!("Failed to transform expression: {e}")))?
        .data;

    let result = evaluate_expr(&transformed_expr)?;
    Ok(&result == partition_value)
}

/// Evaluates inequality conditions to determine if they exclude the partition value.
fn evaluate_inequality(
    left: &Expr,
    op: Operator,
    right: &Expr,
    partition_by: &Expr,
    partition_value: &ScalarValue,
    filter_value: &ScalarValue,
) -> Result<bool, DataFusionError> {
    let col = match (left, right) {
        (Expr::Column(c), _) => c,
        (_, Expr::Column(c)) => c,
        _ => {
            return Err(DataFusionError::Plan(
                "Expected column expression".to_string(),
            ));
        }
    };

    let transformed_expr = partition_by
        .clone()
        .transform(|e| {
            Ok(match e {
                Expr::Column(expr_col) if expr_col == *col => {
                    Transformed::yes(Expr::Literal(filter_value.clone()))
                }
                _ => Transformed::no(e),
            })
        })
        .map_err(|e| DataFusionError::Plan(format!("Failed to transform expression: {e}")))?
        .data;

    let result = evaluate_expr(&transformed_expr)?;
    let is_filter_satisfied = match (left, op, right) {
        (Expr::Column(_), Operator::Gt, Expr::Literal(lit)) => {
            filter_value.partial_cmp(lit) == Some(std::cmp::Ordering::Greater)
        }
        (Expr::Column(_), Operator::GtEq, Expr::Literal(lit)) => {
            filter_value.partial_cmp(lit) != Some(std::cmp::Ordering::Less)
        }
        (Expr::Column(_), Operator::Lt, Expr::Literal(lit)) => {
            filter_value.partial_cmp(lit) == Some(std::cmp::Ordering::Less)
        }
        (Expr::Column(_), Operator::LtEq, Expr::Literal(lit)) => {
            filter_value.partial_cmp(lit) != Some(std::cmp::Ordering::Greater)
        }
        (Expr::Literal(lit), Operator::Gt, Expr::Column(_)) => {
            lit.partial_cmp(filter_value) == Some(std::cmp::Ordering::Greater)
        }
        (Expr::Literal(lit), Operator::GtEq, Expr::Column(_)) => {
            lit.partial_cmp(filter_value) != Some(std::cmp::Ordering::Less)
        }
        (Expr::Literal(lit), Operator::Lt, Expr::Column(_)) => {
            lit.partial_cmp(filter_value) == Some(std::cmp::Ordering::Less)
        }
        (Expr::Literal(lit), Operator::LtEq, Expr::Column(_)) => {
            lit.partial_cmp(filter_value) != Some(std::cmp::Ordering::Greater)
        }
        _ => return Err(DataFusionError::Plan("Unsupported operator".to_string())),
    };

    Ok(is_filter_satisfied && &result != partition_value)
}

/// Evaluates a function-based filter (e.g., date_trunc, truncate).
fn evaluate_function_filter(
    func: &Arc<ScalarUDF>,
    args: &[Expr],
    partition_by: &Expr,
    partition_value: &ScalarValue,
) -> Result<bool, DataFusionError> {
    let evaluated_args = args
        .iter()
        .map(|arg| match arg {
            Expr::Literal(lit) => Ok(lit.clone()),
            Expr::Column(col) => {
                let transformed = partition_by
                    .clone()
                    .transform(|e| {
                        Ok(match e {
                            Expr::Column(expr_col) if expr_col == *col => {
                                Transformed::yes(Expr::Literal(partition_value.clone()))
                            }
                            _ => Transformed::no(e),
                        })
                    })
                    .map_err(|e| DataFusionError::Plan(format!("Failed to transform: {e}")))?
                    .data;
                evaluate_expr(&transformed)
            }
            _ => Err(DataFusionError::Plan(
                "Unsupported argument type".to_string(),
            )),
        })
        .collect::<Result<Vec<_>, _>>()?;

    let result = call(func, evaluated_args)?;
    Ok(&result == partition_value)
}

/// Evaluates an expression to a scalar value.
fn evaluate_expr(expr: &Expr) -> Result<ScalarValue, DataFusionError> {
    match expr {
        Expr::Literal(lit) => Ok(lit.clone()),
        Expr::ScalarFunction(ScalarFunction { func, args }) => {
            let args = args
                .iter()
                .map(|arg| evaluate_expr(arg))
                .collect::<Result<Vec<_>, _>>()?;
            if func.name() == "date_trunc" {
                if let [
                    ScalarValue::Utf8(Some(granularity)),
                    ScalarValue::TimestampNanosecond(Some(ts), _),
                ] = args.as_slice()
                {
                    if granularity == "month" {
                        let seconds = ts / NANOSECONDS;
                        let nanos = (ts % NANOSECONDS) as u32;
                        let dt = Utc.timestamp_opt(seconds, nanos).single().ok_or_else(|| {
                            DataFusionError::Plan(format!("Invalid timestamp: {} ns", ts))
                        })?;
                        let truncated = dt
                            .with_day(1)
                            .and_then(|d| d.with_hour(0))
                            .and_then(|d| d.with_minute(0))
                            .and_then(|d| d.with_second(0))
                            .and_then(|d| d.with_nanosecond(0))
                            .ok_or_else(|| {
                                DataFusionError::Plan(format!(
                                    "Failed to truncate timestamp: {} ns",
                                    ts
                                ))
                            })?;
                        Ok(ScalarValue::TimestampNanosecond(
                            Some(truncated.timestamp_nanos_opt().unwrap_or(0)),
                            None,
                        ))
                    } else {
                        Err(DataFusionError::Plan(format!(
                            "Unsupported date_trunc arguments: {:?}",
                            args
                        )))
                    }
                } else {
                    Err(DataFusionError::Plan(format!(
                        "Unsupported date_trunc arguments: {:?}",
                        args
                    )))
                }
            } else {
                call(func.as_ref(), args)
            }
        }
        Expr::Case(case) => {
            for (when, then) in &case.when_then_expr {
                let condition = evaluate_expr(when)?;
                if matches!(condition, ScalarValue::Boolean(Some(true))) {
                    return evaluate_expr(then);
                }
            }
            if let Some(else_expr) = &case.else_expr {
                evaluate_expr(else_expr)
            } else {
                Ok(ScalarValue::Null)
            }
        }
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            let left_val = evaluate_expr(left)?;
            let right_val = evaluate_expr(right)?;
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

fn call(f: &ScalarUDF, args: Vec<ScalarValue>) -> Result<ScalarValue, DataFusionError> {
    let arg_types = args.iter().map(ScalarValue::data_type).collect::<Vec<_>>();
    let return_type = &f.return_type(&arg_types)?;
    let args = args.into_iter().map(ColumnarValue::Scalar).collect();

    let args = ScalarFunctionArgs {
        args,
        number_rows: 1,
        return_type,
    };

    let ColumnarValue::Scalar(bucket_value) = f.invoke_with_args(args)? else {
        return Err(DataFusionError::Plan("Expected scalar value".to_string()));
    };

    Ok(bucket_value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::{
        functions::regex::regexp_match,
        prelude::{case, col, date_trunc, in_list, lit},
    };
    use runtime_datafusion_udfs::{bucket, truncate};
    use std::sync::Arc;

    macro_rules! assert_prune_partition {
        ($filters:expr, $partition_by:expr, $scalar_variant:ident, [$(($val:expr, $should_prune:expr)),*]) => {
            $(
                let partition_value = ScalarValue::$scalar_variant(Some($val));
                assert_eq!(
                    prune_partition($filters, &$partition_by, &partition_value)?,
                    $should_prune,
                    "partition_value = {partition_value:?}, should_prune = {}",
                    $should_prune,
                );
            )*
        };
    }

    #[test]
    fn test_prune_partition_multiple_columns() -> Result<(), DataFusionError> {
        let partition_by = col("region");
        let filters = &[col("col2").eq(partition_by.clone())];
        assert_prune_partition!(filters, &partition_by, Utf8, [("us-east-1".into(), false)]);
        Ok(())
    }

    #[test]
    fn test_prune_partition_exact_match() -> Result<(), DataFusionError> {
        let partition_by = col("region");
        let region = "us-east-2";
        let filters = &[col("region").eq(lit(region))];
        assert_prune_partition!(
            filters,
            &partition_by,
            Utf8,
            [("us-east-2".into(), false), ("ap-northeast-2".into(), true)]
        );
        Ok(())
    }

    #[test]
    fn test_prune_partition_inlist() -> Result<(), DataFusionError> {
        let partition_by = col("account_id");
        let filters = &[in_list(
            partition_by.clone(),
            vec![lit(1), lit(2), lit(3)],
            false,
        )];
        assert_prune_partition!(
            filters,
            &partition_by,
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
        let partition_by = col("account_id");
        let filters = &[in_list(
            partition_by.clone(),
            vec![lit(1), lit(2), lit(3)],
            true,
        )];
        assert_prune_partition!(
            filters,
            &partition_by,
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
        let partition_by = col("account_id");
        let filter = col("account_id")
            .eq(lit(1))
            .or(col("account_id").eq(lit(2)));
        assert_prune_partition!(
            &[filter.clone()],
            &partition_by,
            Int32,
            [(1, false), (2, false), (3, true), (4, true)]
        );
        Ok(())
    }

    #[test]
    fn test_prune_partition_or_equalities_3_items() -> Result<(), DataFusionError> {
        let partition_by = col("account_id");
        let filter = col("account_id")
            .eq(lit(1))
            .or(col("account_id").eq(lit(2)))
            .or(col("account_id").eq(lit(3)));
        assert_prune_partition!(
            &[filter.clone()],
            &partition_by,
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
        let partition_by = col("account_id");
        let filter = col("account_id")
            .not_eq(lit(1))
            .and(col("account_id").not_eq(lit(2)));
        assert_prune_partition!(
            &[filter.clone()],
            &partition_by,
            Int32,
            [(1, true), (2, true), (3, false), (4, false)]
        );
        Ok(())
    }

    #[test]
    fn test_prune_partition_and_inequalities_3_items() -> Result<(), DataFusionError> {
        let partition_by = col("account_id");
        let filter = col("account_id")
            .not_eq(lit(1))
            .and(col("account_id").not_eq(lit(2)))
            .and(col("account_id").not_eq(lit(3)));
        assert_prune_partition!(
            &[filter.clone()],
            &partition_by,
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
            Int32,
            [(us_east_2, false), (ap_northeast_2, true)]
        );
        Ok(())
    }

    #[test]
    fn test_prune_partition_hash_inlist() -> Result<(), DataFusionError> {
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
                prune_partition(filters, &partition_by, &partition_value)?,
                should_prune,
                "partition_value = {partition_value:?}, should_prune = {should_prune}",
            );
        }
        Ok(())
    }

    #[test]
    fn test_prune_partition_hash_not_inlist() -> Result<(), DataFusionError> {
        let partition_by = bucket_expr(vec![lit(10i64), col("account_id")]);
        let filters = &[in_list(
            col("account_id"),
            vec![lit(1), lit(2), lit(3)],
            true,
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
        for (val, should_prune) in hashed_values.into_iter().zip((1..=6).map(|i| i <= 3)) {
            let partition_value = ScalarValue::Int32(Some(val));
            assert_eq!(
                prune_partition(filters, &partition_by, &partition_value)?,
                should_prune,
                "partition_value = {partition_value:?}, should_prune = {should_prune}",
            );
        }
        Ok(())
    }

    #[test]
    fn test_prune_partition_hash_and_inequalities_3_items() -> Result<(), DataFusionError> {
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
                prune_partition(&[filter.clone()], &partition_by, &partition_value)?,
                should_prune,
                "partition_value = {partition_value:?}, should_prune = {should_prune}",
            );
        }
        Ok(())
    }

    #[test]
    fn test_prune_partition_region() -> Result<(), DataFusionError> {
        let partition_by = col("region");
        let filters = &[col("region").eq(lit("us-east-2"))];
        assert_prune_partition!(
            filters,
            &partition_by,
            Utf8,
            [("us-east-2".into(), false), ("ap-northeast-2".into(), true)]
        );
        Ok(())
    }

    #[test]
    fn test_prune_partition_greater_than() -> Result<(), DataFusionError> {
        let partition_by = col("a").gt(lit(5));
        let filters = &[col("a").eq(lit(4))];
        assert_prune_partition!(
            filters,
            &partition_by,
            Boolean,
            [(true, true), (false, false)]
        );
        Ok(())
    }

    #[test]
    fn test_prune_partition_modulo() -> Result<(), DataFusionError> {
        let partition_by = col("a") % lit(10);
        let filters = &[col("a").eq(lit(12))];
        assert_prune_partition!(filters, &partition_by, Int32, [(2, false), (3, true)]);
        Ok(())
    }

    #[test]
    fn test_prune_partition_case() -> Result<(), DataFusionError> {
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
            Utf8,
            [("datafusion".into(), false), ("other".into(), true)]
        );
        Ok(())
    }

    #[test]
    fn test_prune_partition_date_trunc() -> Result<(), DataFusionError> {
        let partition_by = date_trunc(lit("month"), col("date"));
        let filters = &[col("date").eq(lit(ScalarValue::TimestampNanosecond(
            Some(1752537600000000000), // 2025-07-15
            None,
        )))];
        let partition_value = ScalarValue::TimestampNanosecond(Some(1751328000000000000), None); // 2025-07-01
        assert_eq!(
            prune_partition(filters, &partition_by, &partition_value)?,
            false,
            "partition_value = {partition_value:?}, should_prune = false"
        );
        let partition_value = ScalarValue::TimestampNanosecond(Some(1754016000000000000), None); // 2025-08-01
        assert_eq!(
            prune_partition(filters, &partition_by, &partition_value)?,
            true,
            "partition_value = {partition_value:?}, should_prune = true"
        );
        Ok(())
    }

    #[test]
    fn test_prune_partition_truncate() -> Result<(), DataFusionError> {
        let partition_by = Expr::ScalarFunction(ScalarFunction {
            func: Arc::new(ScalarUDF::new_from_impl(truncate::Truncate::new())),
            args: vec![lit(1000i64), col("sales_volume")],
        });
        let filters = &[col("sales_volume").eq(lit(1234i64))];
        assert_prune_partition!(filters, &partition_by, Int64, [(1000, false), (2000, true)]);
        Ok(())
    }

    #[test]
    fn test_prune_partition_bucket() -> Result<(), DataFusionError> {
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
                prune_partition(&filters[..], &partition_by, &partition_value)?,
                should_prune,
                "partition_value = {partition_value:?}, should_prune = {should_prune}",
            );
        }
        Ok(())
    }
}
