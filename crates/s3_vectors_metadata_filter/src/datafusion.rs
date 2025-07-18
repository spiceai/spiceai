/*
Copyright 2025 The Spice.ai OSS Authors

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

use crate::error::{InvalidFilterSnafu, InvalidValueTypeSnafu, Result};
use crate::filter::{FieldOperation, FilterExpression, LogicalOperation, MetadataFilter, Operator};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::{Expr, Operator as DFOperator, binary_expr, col, lit};
use datafusion::prelude::{and, or};

use datafusion::scalar::ScalarValue;
use serde_json::Value;
use std::collections::HashMap;
use std::str::FromStr;

pub fn convert_to_datafusion_expr(filter: &MetadataFilter) -> Result<Expr> {
    match filter {
        MetadataFilter::Simple(map) => convert_simple_filter(map),
        MetadataFilter::Complex(expr) => convert_expression(expr),
    }
}

/// Checks that an arbitrary [`Expr`] can be successfully converted to a [`MetadataFilter`] and that all columns referenced are within `columns`.
#[must_use]
pub fn supports_filter_expr(columns: &[String], filter: &Expr) -> bool {
    match filter {
        Expr::BinaryExpr(binary) => supports_binary_expr(columns, binary),
        Expr::IsNull(expr) | Expr::IsNotNull(expr) => supports_column_ref(columns, expr),
        Expr::InList(in_list) => supports_in_list(columns, in_list),
        _ => false, // Unsupported expression type
    }
}

/// Converts `DataFusion` Expr filters to S3 Vectors API filter format.
///
/// This function converts `DataFusion` expressions to the `MetadataFilter` format
/// that S3 Vectors API expects (MongoDB-style query operators).
pub fn convert_datafusion_filters_to_s3_vectors(
    filters: &[Expr],
) -> DataFusionResult<Option<MetadataFilter>> {
    if filters.is_empty() {
        return Ok(None);
    }

    if filters.len() == 1 {
        // Single filter - convert directly
        convert_expr_to_filter(&filters[0])
    } else {
        // Multiple filters - combine with AND
        let mut and_filters = Vec::new();
        for filter in filters {
            if let Some(expr) = convert_expr_to_filter_expression(filter)? {
                and_filters.push(expr);
            }
        }

        if and_filters.is_empty() {
            return Ok(None);
        }

        if and_filters.len() == 1 {
            if let Some(filter_expr) = and_filters.pop() {
                return Ok(Some(MetadataFilter::Complex(filter_expr)));
            }
        }

        let logical_op = LogicalOperation {
            and: Some(and_filters),
            or: None,
        };
        Ok(Some(MetadataFilter::Complex(FilterExpression::Logical(
            logical_op,
        ))))
    }
}

/// Checks if a binary expression is supported
fn supports_binary_expr(columns: &[String], binary: &datafusion::logical_expr::BinaryExpr) -> bool {
    use datafusion::logical_expr::Operator;

    match binary.op {
        Operator::And | Operator::Or => {
            // Logical operators - check both sides
            supports_filter_expr(columns, &binary.left)
                && supports_filter_expr(columns, &binary.right)
        }
        Operator::Eq
        | Operator::NotEq
        | Operator::Lt
        | Operator::LtEq
        | Operator::Gt
        | Operator::GtEq => {
            // Comparison operators - check left is column and right is literal
            supports_column_ref(columns, &binary.left) && is_literal(&binary.right)
        }
        _ => false, // Unsupported operator
    }
}

/// Checks if an expression references a supported column
fn supports_column_ref(columns: &[String], expr: &Expr) -> bool {
    match expr {
        Expr::Column(col) => columns.contains(&col.name),
        _ => false,
    }
}

/// Checks if an `InList` expression is supported
fn supports_in_list(columns: &[String], in_list: &datafusion::logical_expr::expr::InList) -> bool {
    // Check that the field is a supported column
    if !supports_column_ref(columns, &in_list.expr) {
        return false;
    }

    // Check that all items in the list are literals
    in_list.list.iter().all(is_literal)
}

/// Checks if an expression is a literal value
fn is_literal(expr: &Expr) -> bool {
    matches!(expr, Expr::Literal(_))
}

/// Converts a single `DataFusion` Expr to a `MetadataFilter`
fn convert_expr_to_filter(expr: &Expr) -> DataFusionResult<Option<MetadataFilter>> {
    match convert_expr_to_filter_expression(expr)? {
        Some(filter_expr) => Ok(Some(MetadataFilter::Complex(filter_expr))),
        None => Ok(None),
    }
}

/// Converts a single `DataFusion` Expr to a `FilterExpression`
fn convert_expr_to_filter_expression(expr: &Expr) -> DataFusionResult<Option<FilterExpression>> {
    match expr {
        Expr::BinaryExpr(binary) => convert_binary_expr_to_filter_expression(binary),
        Expr::IsNull(is_null_expr) => convert_is_null_to_filter_expression(is_null_expr, true),
        Expr::IsNotNull(is_not_null_expr) => {
            convert_is_null_to_filter_expression(is_not_null_expr, false)
        }
        Expr::InList(in_list) => convert_in_list_to_filter_expression(in_list),
        _ => {
            tracing::warn!("Unsupported filter expression type: {:?}", expr);
            Ok(None)
        }
    }
}

/// Converts a binary expression to `FilterExpression` format
fn convert_binary_expr_to_filter_expression(
    binary: &datafusion::logical_expr::BinaryExpr,
) -> DataFusionResult<Option<FilterExpression>> {
    use datafusion::logical_expr::Operator;

    match binary.op {
        Operator::And => {
            // Handle AND of two expressions
            return convert_logical_and(&binary.left, &binary.right);
        }
        Operator::Or => {
            // Handle OR of two expressions
            return convert_logical_or(&binary.left, &binary.right);
        }
        _ => {}
    }

    let left_field = extract_field_name(&binary.left)?;
    let right_value = extract_literal_value(&binary.right)?;

    let operator_str = match binary.op {
        Operator::Eq => "$eq",
        Operator::NotEq => "$ne",
        Operator::Lt => "$lt",
        Operator::LtEq => "$lte",
        Operator::Gt => "$gt",
        Operator::GtEq => "$gte",
        _ => {
            tracing::warn!("Unsupported binary operator: {:?}", binary.op);
            return Ok(None);
        }
    };

    let mut operation_map = HashMap::new();
    operation_map.insert(operator_str.to_string(), right_value);

    let field_operation = FieldOperation::Operation(operation_map);

    let mut field_map = HashMap::new();
    field_map.insert(left_field, field_operation);

    Ok(Some(FilterExpression::Field(field_map)))
}

/// Converts logical AND to `FilterExpression` format
fn convert_logical_and(left: &Expr, right: &Expr) -> DataFusionResult<Option<FilterExpression>> {
    let left_expr = convert_expr_to_filter_expression(left)?;
    let right_expr = convert_expr_to_filter_expression(right)?;

    match (left_expr, right_expr) {
        (Some(left), Some(right)) => {
            let logical_op = LogicalOperation {
                and: Some(vec![left, right]),
                or: None,
            };
            Ok(Some(FilterExpression::Logical(logical_op)))
        }
        (Some(expr), None) | (None, Some(expr)) => Ok(Some(expr)),
        (None, None) => Ok(None),
    }
}

/// Converts logical OR to `FilterExpression` format
fn convert_logical_or(left: &Expr, right: &Expr) -> DataFusionResult<Option<FilterExpression>> {
    let left_expr = convert_expr_to_filter_expression(left)?;
    let right_expr = convert_expr_to_filter_expression(right)?;

    match (left_expr, right_expr) {
        (Some(left), Some(right)) => {
            let logical_op = LogicalOperation {
                and: None,
                or: Some(vec![left, right]),
            };
            Ok(Some(FilterExpression::Logical(logical_op)))
        }
        (Some(expr), None) | (None, Some(expr)) => Ok(Some(expr)),
        (None, None) => Ok(None),
    }
}

/// Converts IS NULL/IS NOT NULL to `FilterExpression` format
fn convert_is_null_to_filter_expression(
    expr: &Expr,
    is_null: bool,
) -> DataFusionResult<Option<FilterExpression>> {
    let field_name = extract_field_name(expr)?;

    let mut operation_map = HashMap::new();
    operation_map.insert("$exists".to_string(), serde_json::Value::Bool(!is_null));

    let field_operation = FieldOperation::Operation(operation_map);

    let mut field_map = HashMap::new();
    field_map.insert(field_name, field_operation);

    Ok(Some(FilterExpression::Field(field_map)))
}

/// Converts IN/NOT IN list to `FilterExpression` format
fn convert_in_list_to_filter_expression(
    in_list: &datafusion::logical_expr::expr::InList,
) -> DataFusionResult<Option<FilterExpression>> {
    let field_name = extract_field_name(&in_list.expr)?;

    let mut values = Vec::new();
    for item in &in_list.list {
        values.push(extract_literal_value(item)?);
    }

    let operator_str = if in_list.negated { "$nin" } else { "$in" };

    let mut operation_map = HashMap::new();
    operation_map.insert(operator_str.to_string(), serde_json::Value::Array(values));

    let field_operation = FieldOperation::Operation(operation_map);

    let mut field_map = HashMap::new();
    field_map.insert(field_name, field_operation);

    Ok(Some(FilterExpression::Field(field_map)))
}

/// Extracts field name from a `DataFusion` expression
fn extract_field_name(expr: &Expr) -> DataFusionResult<String> {
    match expr {
        Expr::Column(col) => Ok(col.name.clone()),
        _ => Err(DataFusionError::Plan(format!(
            "Expected column reference, got: {expr:?}"
        ))),
    }
}

/// Extracts literal value from a `DataFusion` expression
fn extract_literal_value(expr: &Expr) -> DataFusionResult<serde_json::Value> {
    match expr {
        Expr::Literal(scalar) => scalar_to_json_value(scalar),
        _ => Err(DataFusionError::Plan(format!(
            "Expected literal value, got: {expr:?}"
        ))),
    }
}

/// Converts `DataFusion` `ScalarValue` to `serde_json::Value`
fn scalar_to_json_value(
    scalar: &datafusion::scalar::ScalarValue,
) -> DataFusionResult<serde_json::Value> {
    use datafusion::scalar::ScalarValue;

    match scalar {
        ScalarValue::Boolean(Some(b)) => Ok(serde_json::Value::Bool(*b)),
        ScalarValue::Int8(Some(i)) => Ok(serde_json::Value::Number((*i).into())),
        ScalarValue::Int16(Some(i)) => Ok(serde_json::Value::Number((*i).into())),
        ScalarValue::Int32(Some(i)) => Ok(serde_json::Value::Number((*i).into())),
        ScalarValue::Int64(Some(i)) => Ok(serde_json::Value::Number((*i).into())),
        ScalarValue::UInt8(Some(i)) => Ok(serde_json::Value::Number((*i).into())),
        ScalarValue::UInt16(Some(i)) => Ok(serde_json::Value::Number((*i).into())),
        ScalarValue::UInt32(Some(i)) => Ok(serde_json::Value::Number((*i).into())),
        ScalarValue::UInt64(Some(i)) => Ok(serde_json::Value::Number((*i).into())),
        ScalarValue::Float32(Some(f)) => {
            if let Some(num) = serde_json::Number::from_f64(f64::from(*f)) {
                Ok(serde_json::Value::Number(num))
            } else {
                Err(DataFusionError::Plan(format!("Invalid float value: {f}")))
            }
        }
        ScalarValue::Float64(Some(f)) => {
            if let Some(num) = serde_json::Number::from_f64(*f) {
                Ok(serde_json::Value::Number(num))
            } else {
                Err(DataFusionError::Plan(format!("Invalid float value: {f}")))
            }
        }
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => {
            Ok(serde_json::Value::String(s.clone()))
        }
        ScalarValue::Boolean(None)
        | ScalarValue::Int8(None)
        | ScalarValue::Int16(None)
        | ScalarValue::Int32(None)
        | ScalarValue::Int64(None)
        | ScalarValue::UInt8(None)
        | ScalarValue::UInt16(None)
        | ScalarValue::UInt32(None)
        | ScalarValue::UInt64(None)
        | ScalarValue::Float32(None)
        | ScalarValue::Float64(None)
        | ScalarValue::Utf8(None)
        | ScalarValue::LargeUtf8(None) => Ok(serde_json::Value::Null),
        _ => Err(DataFusionError::Plan(format!(
            "Unsupported scalar type for filter: {scalar:?}"
        ))),
    }
}

fn convert_simple_filter(map: &HashMap<String, Value>) -> Result<Expr> {
    let mut expressions = Vec::new();

    for (field, value) in map {
        expressions.push(binary_expr(
            col(field),
            DFOperator::Eq,
            lit(json_value_to_scalar(value)?),
        ));
    }

    combine_with_and(expressions)
}

fn convert_expression(expr: &FilterExpression) -> Result<Expr> {
    match expr {
        FilterExpression::Field(map) => convert_field_expressions(map),
        FilterExpression::Logical(logical) => convert_logical_expression(logical),
    }
}

fn convert_field_expressions(map: &HashMap<String, FieldOperation>) -> Result<Expr> {
    let mut expressions = Vec::new();

    for (field, operation) in map {
        let field_exprs = convert_field_operation(field, operation)?;
        expressions.extend(field_exprs);
    }

    combine_with_and(expressions)
}

fn convert_field_operation(field: &str, operation: &FieldOperation) -> Result<Vec<Expr>> {
    match operation {
        FieldOperation::Direct(value) => Ok(vec![binary_expr(
            col(field),
            DFOperator::Eq,
            lit(json_value_to_scalar(value)?),
        )]),
        FieldOperation::Operation(ops) => {
            let mut expressions = Vec::new();

            for (op_str, value) in ops {
                expressions.push(convert_operator_expression(
                    field,
                    Operator::from_str(op_str.as_str())?,
                    value,
                )?);
            }

            Ok(expressions)
        }
    }
}

fn convert_operator_expression(field: &str, operator: Operator, value: &Value) -> Result<Expr> {
    match operator {
        Operator::Eq => Ok(binary_expr(
            col(field),
            DFOperator::Eq,
            lit(json_value_to_scalar(value)?),
        )),
        Operator::Ne => Ok(binary_expr(
            col(field),
            DFOperator::NotEq,
            lit(json_value_to_scalar(value)?),
        )),
        Operator::Gt => Ok(binary_expr(
            col(field),
            DFOperator::Gt,
            lit(json_value_to_scalar(value)?),
        )),
        Operator::Gte => Ok(binary_expr(
            col(field),
            DFOperator::GtEq,
            lit(json_value_to_scalar(value)?),
        )),
        Operator::Lt => Ok(binary_expr(
            col(field),
            DFOperator::Lt,
            lit(json_value_to_scalar(value)?),
        )),
        Operator::Lte => Ok(binary_expr(
            col(field),
            DFOperator::LtEq,
            lit(json_value_to_scalar(value)?),
        )),
        Operator::In => {
            let Value::Array(arr) = value else {
                return InvalidValueTypeSnafu {
                    operator: Operator::In,
                    expected: "array",
                    actual: value_type_name(value),
                }
                .fail();
            };
            let mut expressions = Vec::new();
            for item in arr {
                expressions.push(binary_expr(
                    col(field),
                    DFOperator::Eq,
                    lit(json_value_to_scalar(item)?),
                ));
            }
            combine_with_or(expressions)
        }
        Operator::Nin => {
            if let Value::Array(arr) = value {
                let mut expressions = Vec::new();
                for item in arr {
                    let scalar_value = json_value_to_scalar(item)?;
                    let expr = binary_expr(col(field), DFOperator::NotEq, lit(scalar_value));
                    expressions.push(expr);
                }
                combine_with_and(expressions)
            } else {
                InvalidValueTypeSnafu {
                    operator: Operator::Nin,
                    expected: "array",
                    actual: value_type_name(value),
                }
                .fail()
            }
        }
        Operator::Exists => {
            if let Value::Bool(exists) = value {
                if *exists {
                    // Field exists: field IS NOT NULL
                    Ok(col(field).is_not_null())
                } else {
                    // Field does not exist: field IS NULL
                    Ok(col(field).is_null())
                }
            } else {
                InvalidValueTypeSnafu {
                    operator: Operator::Exists,
                    expected: "boolean",
                    actual: value_type_name(value),
                }
                .fail()
            }
        }
    }
}

fn convert_logical_expression(logical: &LogicalOperation) -> Result<Expr> {
    let mut expressions = Vec::new();

    if let Some(and_filters) = &logical.and {
        let mut and_exprs = Vec::new();
        for filter in and_filters {
            and_exprs.push(convert_expression(filter)?);
        }
        if let Ok(and_expr) = combine_with_and(and_exprs) {
            expressions.push(and_expr);
        }
    }

    if let Some(or_filters) = &logical.or {
        let mut or_exprs = Vec::new();
        for filter in or_filters {
            or_exprs.push(convert_expression(filter)?);
        }
        if let Ok(or_expr) = combine_with_or(or_exprs) {
            expressions.push(or_expr);
        }
    }

    combine_with_and(expressions)
}

fn combine_with_and(expressions: Vec<Expr>) -> Result<Expr> {
    let mut iter = expressions.into_iter();
    let Some(mut expr) = iter.next() else {
        return InvalidFilterSnafu {
            message: "No expressions to combine".to_string(),
        }
        .fail();
    };
    for expr2 in iter {
        expr = and(expr, expr2);
    }
    Ok(expr)
}

fn combine_with_or(expressions: Vec<Expr>) -> Result<Expr> {
    let mut iter = expressions.into_iter();
    let Some(mut expr) = iter.next() else {
        return InvalidFilterSnafu {
            message: "No expressions to combine".to_string(),
        }
        .fail();
    };
    for expr2 in iter {
        expr = or(expr, expr2);
    }
    Ok(expr)
}

fn json_value_to_scalar(value: &Value) -> Result<ScalarValue> {
    match value {
        Value::String(s) => Ok(ScalarValue::Utf8(Some(s.clone()))),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(ScalarValue::Int64(Some(i)))
            } else if let Some(f) = n.as_f64() {
                Ok(ScalarValue::Float64(Some(f)))
            } else {
                InvalidFilterSnafu {
                    message: format!("Invalid number: {n}"),
                }
                .fail()
            }
        }
        Value::Bool(b) => Ok(ScalarValue::Boolean(Some(*b))),
        _ => InvalidFilterSnafu {
            message: format!("Unsupported value type: {value}"),
        }
        .fail(),
    }
}

fn value_type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::logical_expr::{BinaryExpr, Operator, col, lit};
    use datafusion::scalar::ScalarValue;
    use datafusion::sql::unparser::{Unparser, dialect::DefaultDialect};

    #[test]
    #[allow(clippy::too_many_lines)]
    fn test_valid_datafusion_expressions() {
        let columns = vec![
            "genre".to_string(),
            "year".to_string(),
            "rating".to_string(),
            "budget".to_string(),
            "optional_field".to_string(),
        ];

        let test_cases = vec![
            // Simple equality
            (
                Expr::BinaryExpr(BinaryExpr::new(
                    Box::new(col("genre")),
                    Operator::Eq,
                    Box::new(lit("documentary")),
                )),
                r#"{"genre":{"$eq":"documentary"}}"#,
            ),
            // Numeric comparisons
            (
                Expr::BinaryExpr(BinaryExpr::new(
                    Box::new(col("year")),
                    Operator::GtEq,
                    Box::new(lit(ScalarValue::Int64(Some(2020)))),
                )),
                r#"{"year":{"$gte":2020}}"#,
            ),
            (
                Expr::BinaryExpr(BinaryExpr::new(
                    Box::new(col("year")),
                    Operator::Lt,
                    Box::new(lit(ScalarValue::Int64(Some(2020)))),
                )),
                r#"{"year":{"$lt":2020}}"#,
            ),
            (
                Expr::BinaryExpr(BinaryExpr::new(
                    Box::new(col("rating")),
                    Operator::GtEq,
                    Box::new(lit(ScalarValue::Float64(Some(8.5)))),
                )),
                r#"{"rating":{"$gte":8.5}}"#,
            ),
            // Array operations
            (
                Expr::InList(datafusion::logical_expr::expr::InList::new(
                    Box::new(col("genre")),
                    vec![lit("horror"), lit("thriller")],
                    true, // negated ($nin)
                )),
                r#"{"genre":{"$nin":["horror","thriller"]}}"#,
            ),
            (
                Expr::InList(datafusion::logical_expr::expr::InList::new(
                    Box::new(col("genre")),
                    vec![lit("drama"), lit("thriller")],
                    false, // not negated ($in)
                )),
                r#"{"genre":{"$in":["drama","thriller"]}}"#,
            ),
            // Existence checks
            (
                Expr::IsNotNull(Box::new(col("genre"))),
                r#"{"genre":{"$exists":true}}"#,
            ),
            (
                Expr::IsNull(Box::new(col("optional_field"))),
                r#"{"optional_field":{"$exists":false}}"#,
            ),
            // Logical operations
            (
                Expr::BinaryExpr(BinaryExpr::new(
                    Box::new(Expr::BinaryExpr(BinaryExpr::new(
                        Box::new(col("genre")),
                        Operator::Eq,
                        Box::new(lit("drama")),
                    ))),
                    Operator::And,
                    Box::new(Expr::BinaryExpr(BinaryExpr::new(
                        Box::new(col("year")),
                        Operator::GtEq,
                        Box::new(lit(ScalarValue::Int64(Some(2020)))),
                    ))),
                )),
                r#"{"$and":[{"genre":{"$eq":"drama"}},{"year":{"$gte":2020}}]}"#,
            ),
            (
                Expr::BinaryExpr(BinaryExpr::new(
                    Box::new(Expr::BinaryExpr(BinaryExpr::new(
                        Box::new(col("genre")),
                        Operator::Eq,
                        Box::new(lit("drama")),
                    ))),
                    Operator::Or,
                    Box::new(Expr::BinaryExpr(BinaryExpr::new(
                        Box::new(col("year")),
                        Operator::GtEq,
                        Box::new(lit(ScalarValue::Int64(Some(2020)))),
                    ))),
                )),
                r#"{"$or":[{"genre":{"$eq":"drama"}},{"year":{"$gte":2020}}]}"#,
            ),
        ];

        for (expr, expected_json) in test_cases {
            assert!(supports_filter_expr(&columns, &expr));
            let result = convert_datafusion_filters_to_s3_vectors(&[expr])
                .expect("Failed to convert DataFusion filters to S3 Vectors filters");
            if let Some(filter) = result {
                assert!(filter.validate().is_ok());

                let json_result = filter.to_json().expect("Failed to convert filter to JSON");
                let parsed_value: serde_json::Value =
                    serde_json::from_str(&json_result).expect("Failed to parse JSON");
                let expected_value: serde_json::Value =
                    serde_json::from_str(expected_json).expect("Failed to parse expected JSON");

                assert_eq!(
                    parsed_value, expected_value,
                    "Expression conversion mismatch.\nActual: {json_result}\nExpected: {expected_json}"
                );
            }
        }
    }

    #[test]
    fn test_valid_expressions() {
        let valid_expressions = vec![
            (
                r#"{"genre": "documentary"}"#,
                "genre = Utf8(\"documentary\")",
                "(genre = 'documentary')",
            ),
            (
                r#"{"genre": {"$eq": "documentary"}}"#,
                "genre = Utf8(\"documentary\")",
                "(genre = 'documentary')",
            ),
            (
                r#"{"genre": {"$ne": "drama"}}"#,
                "genre != Utf8(\"drama\")",
                "(genre <> 'drama')",
            ),
            (
                r#"{"year": {"$gt": 2019}}"#,
                "year > Int64(2019)",
                r#"("year" > 2019)"#,
            ),
            (
                r#"{"year": {"$gte": 2020}}"#,
                "year >= Int64(2020)",
                r#"("year" >= 2020)"#,
            ),
            (
                r#"{"year": {"$lt": 2020}}"#,
                "year < Int64(2020)",
                r#"("year" < 2020)"#,
            ),
            (
                r#"{"year": {"$lte": 2020}}"#,
                "year <= Int64(2020)",
                r#"("year" <= 2020)"#,
            ),
            (
                r#"{"genre": {"$in": ["comedy", "documentary"]}}"#,
                "genre = Utf8(\"comedy\") OR genre = Utf8(\"documentary\")",
                "((genre = 'comedy') OR (genre = 'documentary'))",
            ),
            (
                r#"{"genre": {"$nin": ["horror", "thriller"]}}"#,
                "genre != Utf8(\"horror\") AND genre != Utf8(\"thriller\")",
                "((genre <> 'horror') AND (genre <> 'thriller'))",
            ),
            (
                r#"{"genre": {"$exists": true}}"#,
                "genre IS NOT NULL",
                r"genre IS NOT NULL",
            ),
            (
                r#"{"optional_field": {"$exists": false}}"#,
                "optional_field IS NULL",
                r"optional_field IS NULL",
            ),
            (
                r#"{"$and": [{"genre": {"$eq": "drama"}}, {"year": {"$gte": 2020}}]}"#,
                "genre = Utf8(\"drama\") AND year >= Int64(2020)",
                "((genre = 'drama') AND (\"year\" >= 2020))",
            ),
            (
                r#"{"$or": [{"genre": {"$eq": "drama"}}, {"year": {"$gte": 2020}}]}"#,
                "genre = Utf8(\"drama\") OR year >= Int64(2020)",
                "((genre = 'drama') OR (\"year\" >= 2020))",
            ),
            (
                r#"{
                "$and": [
                    {"genre": {"$in": ["drama", "thriller"]}},
                    {"$or": [
                        {"year": {"$gte": 2020}},
                        {"rating": {"$gte": 8.5}}
                    ]},
                    {"budget": {"$exists": true}}
                ]
            }"#,
                "(genre = Utf8(\"drama\") OR genre = Utf8(\"thriller\")) AND (year >= Int64(2020) OR rating >= Float64(8.5)) AND budget IS NOT NULL",
                "((((genre = 'drama') OR (genre = 'thriller')) AND ((\"year\" >= 2020) OR (rating >= 8.5))) AND budget IS NOT NULL)",
            ),
        ];

        let unparser = Unparser::new(&DefaultDialect {});
        for (filter_json, expected_detailed, expected_sql) in valid_expressions {
            let filter =
                MetadataFilter::from_json(filter_json).expect("Failed to parse filter JSON");
            let expr = convert_to_datafusion_expr(&filter)
                .expect("Failed to convert filter to DataFusion expression");
            assert_eq!(expr.to_string(), expected_detailed);
            assert_eq!(
                unparser
                    .expr_to_sql(&expr)
                    .expect("Failed to convert DataFusion expression to SQL")
                    .to_string(),
                expected_sql
            );
        }
    }

    #[test]
    fn test_multiple_conditions_same_field() {
        let filter_json = r#"{"price": {"$gte": 10, "$lte": 50}}"#;
        let filter = MetadataFilter::from_json(filter_json).expect("Failed to parse filter JSON");

        let expr = convert_to_datafusion_expr(&filter)
            .expect("Failed to convert filter to DataFusion expression");
        let expr_str = expr.to_string();
        // HashMap iteration order is not guaranteed, so we check both possible orders
        assert!(
            expr_str == "price >= Int64(10) AND price <= Int64(50)"
                || expr_str == "price <= Int64(50) AND price >= Int64(10)"
        );
    }

    #[test]
    fn test_multiple_fields_simple() {
        let filter_json = r#"{"genre": "documentary", "year": 2020}"#;
        let filter = MetadataFilter::from_json(filter_json).expect("Failed to parse filter JSON");

        let expr = convert_to_datafusion_expr(&filter)
            .expect("Failed to convert filter to DataFusion expression");
        // Note: HashMap iteration order is not guaranteed, so we check both possible orders
        let expr_str = expr.to_string();
        assert!(
            expr_str == "genre = Utf8(\"documentary\") AND year = Int64(2020)"
                || expr_str == "year = Int64(2020) AND genre = Utf8(\"documentary\")"
        );
    }
}
