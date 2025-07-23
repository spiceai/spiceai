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

use datafusion::{
    common::tree_node::{Transformed, TreeNode as _},
    error::DataFusionError,
    logical_expr::{
        BinaryExpr, ColumnarValue, Operator, ScalarFunctionArgs, ScalarUDF,
        expr::{InList, ScalarFunction},
    },
    prelude::Expr,
    scalar::ScalarValue,
};

/// Collects equality conditions from nested OR expressions, ensuring they are on the same column.
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

/// Collects inequality conditions from nested AND expressions, ensuring they are on the same column.
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

/// Determine whether a partition should be pruned based on filters, `partition_by`, and `partition_value`.
pub(crate) fn prune_partition(
    filters: &[Expr],
    partition_by: &Expr,
    partition_value: &ScalarValue,
) -> Result<bool, DataFusionError> {
    let partition_by_columns = partition_by.column_refs();

    for filter in filters {
        // Skip filters with columns not in partition_by
        if filter
            .column_refs()
            .iter()
            .any(|col| !partition_by_columns.contains(col))
        {
            continue;
        }

        match filter {
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Operator::Eq,
                right,
            }) => {
                if let (Expr::Column(_), Expr::Literal(lit))
                | (Expr::Literal(lit), Expr::Column(_)) = (left.as_ref(), right.as_ref())
                {
                    if !filter_or_udf_value_matches(left, partition_by, partition_value, lit)? {
                        return Ok(true); // Prune if equality does not match
                    }
                }
            }
            Expr::BinaryExpr(_) => {
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
                } else if let Some((col_name, literals)) = collect_and_inequalities(filter) {
                    for lit in literals {
                        let is_match = filter_or_udf_value_matches(
                            &Expr::Column(col_name.clone().into()),
                            partition_by,
                            partition_value,
                            &lit,
                        )?;
                        if is_match {
                            return Ok(true); // Prune if match for NOT IN-like condition
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
                                return Ok(true); // prune if match in NOT IN
                            }
                            any_matches |= is_match;
                        }
                    }
                    if !any_matches && !negated {
                        return Ok(true);
                    }
                }
            }
            _ => {}
        }
    }

    Ok(false)
}

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

    let result = match transformed_expr {
        Expr::Literal(lit) => lit,
        Expr::ScalarFunction(ScalarFunction { func, args }) => {
            let args = args
                .into_iter()
                .map(|arg| match arg {
                    Expr::Literal(lit) => Ok(lit),
                    _ => Err(DataFusionError::Plan(
                        "Expected literal after transformation".into(),
                    )),
                })
                .collect::<Result<Vec<_>, _>>()?;
            call(func.as_ref(), args)?
        }
        _ => {
            return Err(DataFusionError::Plan(
                "Unexpected expression type after transformation".into(),
            ));
        }
    };

    Ok(&result == partition_value)
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
        return Err(DataFusionError::Plan("Expected scalar value".into()));
    };

    Ok(bucket_value)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::prelude::{col, in_list, lit};
    use runtime_datafusion_udfs::bucket;

    use super::*;

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
}
