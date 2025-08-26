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

use std::{cmp::Ordering, sync::Arc};

use arrow_schema::{Field, Schema};
use datafusion::{
    common::{
        Column, ToDFSchema as _,
        tree_node::{Transformed, TreeNode as _},
    },
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
fn evaluate_inequality(
    col: &Column,
    op: Operator,
    filter_value: &ScalarValue,
    partition_by: &Expr,
    partition_value: &ScalarValue,
    schema: &Schema,
) -> Result<bool, DataFusionError> {
    let result = transform_and_evaluate(partition_by, col, filter_value, schema)?;
    let is_filter_satisfied = match op {
        Operator::Gt => filter_value.partial_cmp(&result) == Some(Ordering::Greater),
        Operator::GtEq => filter_value.partial_cmp(&result) != Some(Ordering::Less),
        Operator::Lt => filter_value.partial_cmp(&result) == Some(Ordering::Less),
        Operator::LtEq => filter_value.partial_cmp(&result) != Some(Ordering::Greater),
        _ => return Err(DataFusionError::Plan("Unsupported operator".to_string())),
    };

    Ok(is_filter_satisfied && &result != partition_value)
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
            &[filter.clone()],
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
            &[filter.clone()],
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
            &[filter.clone()],
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
            &[filter.clone()],
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
    fn test_prune_partition_hash_not_inlist() -> Result<(), DataFusionError> {
        let schema = Schema::new(vec![Field::new("account_id", DataType::Int32, false)]);
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
                prune_partition(filters.as_slice(), &partition_by, &partition_value, &schema)?,
                should_prune,
                "partition_value = {partition_value:?}, should_prune = {should_prune}",
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
                prune_partition(&[filter.clone()], &partition_by, &partition_value, &schema)?,
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
}
