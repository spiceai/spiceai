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

//! Shared primary key extraction logic for `DynamoDB` DML operations.

use crate::utils::scalar_to_attribute_value;
use arrow::array::Array;
use aws_sdk_dynamodb::types::AttributeValue;
use datafusion::common::ScalarValue;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::expr::InList;
use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
use std::collections::HashSet;

/// A single primary key: (`partition_key_value`, optional `sort_key_value`).
pub type PrimaryKey = (AttributeValue, Option<AttributeValue>);

/// A key that a filter allows. `None` means the filter does not restrict that key column.
type PartialKey = (Option<AttributeValue>, Option<AttributeValue>);

/// Extract the primary keys of the items that DML filter expressions select.
///
/// The filters are conjuncts: an item is selected only when its key satisfies every filter.
/// Each filter can combine, with `AND` and `OR`:
/// - `pk = <literal>`, `sk = <literal>`
/// - `pk IN (<literal>, ...)`, `sk IN (<literal>, ...)`
/// - `(pk, sk) = (<literal>, <literal>)`, `(pk, sk) IN ((<literal>, <literal>), ...)`
///
/// Every selected key must have a partition key value, and a sort key value when the table has a
/// sort key. Filters on other columns are rejected. Filters that contradict each other select no
/// key.
pub(crate) fn extract_primary_keys(
    filters: &[Expr],
    partition_key: &str,
    sort_key: Option<&str>,
    time_format: &str,
) -> Result<Vec<PrimaryKey>, DataFusionError> {
    let Some((first, rest)) = filters.split_first() else {
        return Err(DataFusionError::Plan(
            "DynamoDB DML requires a WHERE clause on the primary key".to_string(),
        ));
    };

    let mut allowed = keys_allowed_by(first, partition_key, sort_key, time_format)?;
    for filter in rest {
        let other = keys_allowed_by(filter, partition_key, sort_key, time_format)?;
        allowed = intersect(&allowed, &other);
    }

    let mut keys: Vec<PrimaryKey> = Vec::with_capacity(allowed.len());
    let mut seen = HashSet::with_capacity(allowed.len());
    for key in allowed {
        match key {
            (Some(pk_val), sk_val) if sort_key.is_none() || sk_val.is_some() => {
                if seen.insert((key_identity(&pk_val), sk_val.as_ref().map(key_identity))) {
                    keys.push((pk_val, sk_val));
                }
            }
            _ => {
                let key_columns = match sort_key {
                    Some(sk) => format!("partition key '{partition_key}' and sort key '{sk}'"),
                    None => format!("partition key '{partition_key}'"),
                };
                return Err(DataFusionError::Plan(format!(
                    "DynamoDB DML requires the WHERE clause to give a value for the {key_columns} of each item"
                )));
            }
        }
    }

    Ok(keys)
}

/// A hashable identity for a key value. Key attributes are strings, numbers or binary; any other
/// value gets its debug text.
fn key_identity(value: &AttributeValue) -> (char, Vec<u8>) {
    match value {
        AttributeValue::S(s) => ('S', s.as_bytes().to_vec()),
        AttributeValue::N(n) => ('N', n.as_bytes().to_vec()),
        AttributeValue::B(b) => ('B', b.as_ref().to_vec()),
        other => ('?', format!("{other:?}").into_bytes()),
    }
}

/// The keys `expr` allows: `OR` is the union of its sides, `AND` the intersection.
fn keys_allowed_by(
    expr: &Expr,
    partition_key: &str,
    sort_key: Option<&str>,
    time_format: &str,
) -> Result<Vec<PartialKey>, DataFusionError> {
    let full_keys = |keys: Vec<PrimaryKey>| {
        keys.into_iter()
            .map(|(pk, sk)| (Some(pk), sk))
            .collect::<Vec<_>>()
    };

    match expr {
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::Or,
            right,
        }) => {
            let mut keys = keys_allowed_by(left, partition_key, sort_key, time_format)?;
            keys.extend(keys_allowed_by(right, partition_key, sort_key, time_format)?);
            Ok(keys)
        }
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::And,
            right,
        }) => Ok(intersect(
            &keys_allowed_by(left, partition_key, sort_key, time_format)?,
            &keys_allowed_by(right, partition_key, sort_key, time_format)?,
        )),
        // Equality: pk = <literal>, sk = <literal> or (pk, sk) = (<literal>, <literal>)
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::Eq,
            right,
        }) => match (left.as_ref(), right.as_ref()) {
            (Expr::Column(col), Expr::Literal(scalar, _))
            | (Expr::Literal(scalar, _), Expr::Column(col)) => {
                let col = col.name();
                if col == partition_key {
                    Ok(vec![(Some(scalar_to_attribute_value(scalar, time_format)?), None)])
                } else if sort_key.is_some_and(|sk| sk == col) {
                    Ok(vec![(None, Some(scalar_to_attribute_value(scalar, time_format)?))])
                } else {
                    Err(DataFusionError::Plan(format!(
                        "DynamoDB DML only supports filters on primary key columns, got '{col}'"
                    )))
                }
            }
            (Expr::ScalarFunction(func), Expr::Literal(scalar, _))
            | (Expr::Literal(scalar, _), Expr::ScalarFunction(func)) => {
                let mut keys = Vec::new();
                extract_keys_from_struct_eq(
                    func,
                    scalar,
                    partition_key,
                    sort_key,
                    time_format,
                    &mut keys,
                )?;
                Ok(full_keys(keys))
            }
            _ => Err(DataFusionError::Plan(
                "DynamoDB DML only supports column = literal or struct = struct filters on primary key columns".to_string(),
            )),
        },
        // IN list: pk IN (v1, v2, v3), sk IN (v1, v2, v3) or (pk, sk) IN ((v1, v2), ...)
        Expr::InList(InList {
            expr: in_expr,
            list,
            negated,
        }) => {
            if *negated {
                return Err(DataFusionError::Plan(
                    "DynamoDB DML does not support NOT IN".to_string(),
                ));
            }
            if let Expr::Column(col) = in_expr.as_ref() {
                let col = col.name();
                let is_partition_key = col == partition_key;
                let is_sort_key = sort_key.is_some_and(|sk| sk == col);
                if !(is_partition_key || is_sort_key) {
                    return Err(DataFusionError::Plan(format!(
                        "DynamoDB DML only supports filters on primary key columns, got '{col}'"
                    )));
                }
                let mut keys = Vec::with_capacity(list.len());
                for item in list {
                    let Expr::Literal(scalar, _) = item else {
                        return Err(DataFusionError::Plan(format!(
                            "DynamoDB DML IN list values must be literals, got: {item}"
                        )));
                    };
                    // A NULL in an IN list matches no item.
                    if scalar.is_null() {
                        continue;
                    }
                    let attr = scalar_to_attribute_value(scalar, time_format)?;
                    keys.push(if is_partition_key {
                        (Some(attr), None)
                    } else {
                        (None, Some(attr))
                    });
                }
                return Ok(keys);
            }
            let mut keys = Vec::new();
            extract_keys_from_in_list(
                in_expr,
                list,
                partition_key,
                sort_key,
                time_format,
                &mut keys,
            )?;
            Ok(full_keys(keys))
        }
        _ => Err(DataFusionError::Plan(format!(
            "DynamoDB DML: unsupported filter expression: {expr}"
        ))),
    }
}

/// The keys both `left` and `right` allow.
fn intersect(left: &[PartialKey], right: &[PartialKey]) -> Vec<PartialKey> {
    fn agree(a: Option<&AttributeValue>, b: Option<&AttributeValue>) -> bool {
        a.zip(b).is_none_or(|(a, b)| a == b)
    }

    let mut keys = Vec::new();
    for (l_pk, l_sk) in left {
        for (r_pk, r_sk) in right {
            if agree(l_pk.as_ref(), r_pk.as_ref()) && agree(l_sk.as_ref(), r_sk.as_ref()) {
                keys.push((
                    l_pk.as_ref().or(r_pk.as_ref()).cloned(),
                    l_sk.as_ref().or(r_sk.as_ref()).cloned(),
                ));
            }
        }
    }
    keys
}

fn extract_keys_from_in_list(
    in_expr: &Expr,
    list: &[Expr],
    partition_key: &str,
    sort_key: Option<&str>,
    time_format: &str,
    keys: &mut Vec<PrimaryKey>,
) -> Result<(), DataFusionError> {
    match in_expr {
        Expr::ScalarFunction(func) => {
            let columns: Vec<&str> = func
                .args
                .iter()
                .map(|arg| match arg {
                    Expr::Column(col) => Ok(col.name()),
                    _ => Err(DataFusionError::Plan(
                        "DynamoDB DML: tuple IN list expr must contain only columns".to_string(),
                    )),
                })
                .collect::<Result<_, _>>()?;

            let pk_idx = columns
                .iter()
                .position(|&c| c == partition_key)
                .ok_or_else(|| {
                    DataFusionError::Plan(format!(
                        "DynamoDB DML: tuple IN list must include partition key '{partition_key}'"
                    ))
                })?;

            let sk_idx = sort_key
                .map(|sk| {
                    columns.iter().position(|&c| c == sk).ok_or_else(|| {
                        DataFusionError::Plan(
                            "DynamoDB DML: tuple IN list includes non-primary-key columns"
                                .to_string(),
                        )
                    })
                })
                .transpose()?;

            let expected_len = if sort_key.is_some() { 2 } else { 1 };
            if columns.len() != expected_len {
                return Err(DataFusionError::Plan(format!(
                    "DynamoDB DML: tuple IN list must contain only primary key columns, got {} columns",
                    columns.len()
                )));
            }

            for item in list {
                let (pk_attr, sk_attr) = match item {
                    Expr::Literal(ScalarValue::Struct(struct_array), _) => {
                        if struct_array.len() != 1 {
                            return Err(DataFusionError::Plan(
                                "DynamoDB DML: expected single-row struct in IN list".to_string(),
                            ));
                        }
                        let pk_scalar = ScalarValue::try_from_array(struct_array.column(pk_idx), 0)
                            .map_err(|e| {
                                DataFusionError::Plan(format!(
                                    "Failed to extract pk from struct: {e}"
                                ))
                            })?;
                        let pk_attr = scalar_to_attribute_value(&pk_scalar, time_format)?;
                        let sk_attr = sk_idx
                            .map(|idx| {
                                let sk_scalar =
                                    ScalarValue::try_from_array(struct_array.column(idx), 0)
                                        .map_err(|e| {
                                            DataFusionError::Plan(format!(
                                                "Failed to extract sk from struct: {e}"
                                            ))
                                        })?;
                                scalar_to_attribute_value(&sk_scalar, time_format)
                            })
                            .transpose()?;
                        (pk_attr, sk_attr)
                    }
                    Expr::ScalarFunction(func) => {
                        extract_key_pair_from_struct_func(func, pk_idx, sk_idx, time_format)?
                    }
                    _ => {
                        return Err(DataFusionError::Plan(format!(
                            "DynamoDB DML: tuple IN list values must be struct literals, got: {item}"
                        )));
                    }
                };
                keys.push((pk_attr, sk_attr));
            }
            Ok(())
        }
        _ => Err(DataFusionError::Plan(format!(
            "DynamoDB DML: unsupported IN list expression: {in_expr}"
        ))),
    }
}

fn extract_keys_from_struct_eq(
    func: &datafusion::logical_expr::expr::ScalarFunction,
    scalar: &ScalarValue,
    partition_key: &str,
    sort_key: Option<&str>,
    time_format: &str,
    keys: &mut Vec<PrimaryKey>,
) -> Result<(), DataFusionError> {
    let columns: Vec<&str> = func
        .args
        .iter()
        .map(|arg| match arg {
            Expr::Column(col) => Ok(col.name()),
            _ => Err(DataFusionError::Plan(
                "DynamoDB DML: struct equality expr must contain only columns".to_string(),
            )),
        })
        .collect::<Result<_, _>>()?;

    let pk_idx = columns
        .iter()
        .position(|&c| c == partition_key)
        .ok_or_else(|| {
            DataFusionError::Plan(format!(
                "DynamoDB DML: struct equality must include partition key '{partition_key}'"
            ))
        })?;

    let sk_idx = sort_key
        .map(|sk| {
            columns.iter().position(|&c| c == sk).ok_or_else(|| {
                DataFusionError::Plan(
                    "DynamoDB DML: struct equality includes non-primary-key columns".to_string(),
                )
            })
        })
        .transpose()?;

    let expected_len = if sort_key.is_some() { 2 } else { 1 };
    if columns.len() != expected_len {
        return Err(DataFusionError::Plan(format!(
            "DynamoDB DML: struct equality must contain only primary key columns, got {} columns",
            columns.len()
        )));
    }

    let ScalarValue::Struct(struct_array) = scalar else {
        return Err(DataFusionError::Plan(
            "DynamoDB DML: struct equality right-hand side must be a struct literal".to_string(),
        ));
    };

    if struct_array.len() != 1 {
        return Err(DataFusionError::Plan(
            "DynamoDB DML: expected single-row struct in equality".to_string(),
        ));
    }

    let pk_scalar = ScalarValue::try_from_array(struct_array.column(pk_idx), 0)
        .map_err(|e| DataFusionError::Plan(format!("Failed to extract pk from struct: {e}")))?;
    let pk_attr = scalar_to_attribute_value(&pk_scalar, time_format)?;

    let sk_attr = sk_idx
        .map(|idx| {
            let sk_scalar =
                ScalarValue::try_from_array(struct_array.column(idx), 0).map_err(|e| {
                    DataFusionError::Plan(format!("Failed to extract sk from struct: {e}"))
                })?;
            scalar_to_attribute_value(&sk_scalar, time_format)
        })
        .transpose()?;

    keys.push((pk_attr, sk_attr));
    Ok(())
}

pub(crate) fn extract_key_pair_from_struct_func(
    func: &datafusion::logical_expr::expr::ScalarFunction,
    pk_idx: usize,
    sk_idx: Option<usize>,
    time_format: &str,
) -> Result<PrimaryKey, DataFusionError> {
    let pk_arg = func.args.get(pk_idx).ok_or_else(|| {
        DataFusionError::Plan(format!(
            "DynamoDB DML: struct has no argument at index {pk_idx}"
        ))
    })?;
    let Expr::Literal(pk_scalar, _) = pk_arg else {
        return Err(DataFusionError::Plan(format!(
            "DynamoDB DML: struct argument at index {pk_idx} is not a literal: {pk_arg}"
        )));
    };
    let pk_attr = scalar_to_attribute_value(pk_scalar, time_format)?;

    let sk_attr = sk_idx
        .map(|idx| {
            let sk_arg = func.args.get(idx).ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "DynamoDB DML: struct has no argument at index {idx}"
                ))
            })?;
            let Expr::Literal(sk_scalar, _) = sk_arg else {
                return Err(DataFusionError::Plan(format!(
                    "DynamoDB DML: struct argument at index {idx} is not a literal: {sk_arg}"
                )));
            };
            scalar_to_attribute_value(sk_scalar, time_format)
        })
        .transpose()?;

    Ok((pk_attr, sk_attr))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::{col, lit};

    fn s(value: &str) -> AttributeValue {
        AttributeValue::S(value.to_string())
    }

    fn keys(filters: &[Expr], sort_key: Option<&str>) -> Result<Vec<PrimaryKey>, DataFusionError> {
        extract_primary_keys(filters, "pk", sort_key, "")
    }

    #[test]
    fn an_item_must_satisfy_every_filter() -> Result<(), DataFusionError> {
        let a_or_b = col("pk").in_list(vec![lit("a"), lit("b")], false);
        let b_or_c = col("pk").in_list(vec![lit("b"), lit("c")], false);

        assert_eq!(
            keys(&[a_or_b.clone(), col("pk").eq(lit("c"))], None)?,
            vec![]
        );
        assert_eq!(keys(&[a_or_b, b_or_c], None)?, vec![(s("b"), None)]);
        assert_eq!(
            keys(
                &[col("pk").in_list(vec![lit("a"), lit("b"), lit("c"), lit("a")], false)],
                None
            )?,
            vec![(s("a"), None), (s("b"), None), (s("c"), None)]
        );
        assert_eq!(
            keys(
                &[col("pk").in_list(
                    vec![lit("a"), lit(ScalarValue::Utf8(None)), lit("b"), lit("c")],
                    false
                )],
                None
            )?,
            vec![(s("a"), None), (s("b"), None), (s("c"), None)]
        );
        assert_eq!(
            keys(
                &[col("pk")
                    .eq(lit("a"))
                    .and(col("pk").eq(lit("b")))
                    .or(col("pk").eq(lit("c")))],
                None
            )?,
            vec![(s("c"), None)]
        );
        Ok(())
    }

    #[test]
    fn a_composite_key_can_come_from_separate_filters() -> Result<(), DataFusionError> {
        assert_eq!(
            keys(
                &[col("pk").eq(lit("a")), col("sk").eq(lit("1"))],
                Some("sk")
            )?,
            vec![(s("a"), Some(s("1")))]
        );
        assert_eq!(
            keys(
                &[
                    col("sk").eq(lit("1")),
                    col("pk").eq(lit("a")).or(col("pk").eq(lit("b")))
                ],
                Some("sk")
            )?,
            vec![(s("a"), Some(s("1"))), (s("b"), Some(s("1")))]
        );
        assert_eq!(
            keys(
                &[
                    col("pk").eq(lit("a")),
                    col("sk").in_list(vec![lit("1"), lit("2")], false)
                ],
                Some("sk")
            )?,
            vec![(s("a"), Some(s("1"))), (s("a"), Some(s("2")))]
        );
        Ok(())
    }

    #[test]
    fn a_filter_on_another_column_is_refused() {
        assert!(matches!(
            keys(&[col("pk").eq(lit("a")), col("score").eq(lit(f64::NAN))], None),
            Err(DataFusionError::Plan(message)) if message.contains("got 'score'")
        ));
    }

    #[test]
    fn every_item_needs_its_full_key() {
        let refused = |result: Result<Vec<PrimaryKey>, DataFusionError>| {
            matches!(result, Err(DataFusionError::Plan(message))
                if message.contains("partition key 'pk' and sort key 'sk'"))
        };

        assert!(refused(keys(&[col("pk").eq(lit("a"))], Some("sk"))));
        assert!(refused(keys(
            &[col("pk")
                .eq(lit("a"))
                .and(col("sk").eq(lit("1")))
                .or(col("pk").eq(lit("b")))],
            Some("sk")
        )));
    }
}
