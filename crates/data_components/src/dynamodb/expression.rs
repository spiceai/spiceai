use aws_sdk_dynamodb::types::AttributeValue;
use std::collections::HashMap;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::{Expr, Operator};

pub struct DynamoDbFilter {
    pub filter_expression: String,
    pub expression_attribute_names: HashMap<String, String>,
    pub expression_attribute_values: HashMap<String, AttributeValue>,
}

pub fn combine_exprs_with_and(exprs: &[Expr]) -> Option<Expr> {
    let mut iter = exprs.iter();
    let first = iter.next()?.clone();
    Some(iter.fold(first, |acc, e| acc.and(e.clone())))
}

pub fn expr_to_dynamodb_filter(expr: &Expr) -> Option<DynamoDbFilter> {
    let mut counter = 0;
    let mut names = HashMap::new();
    let mut values = HashMap::new();

    let filter_expr = build_filter_expression(expr, &mut counter, &mut names, &mut values)?;

    Some(DynamoDbFilter {
        filter_expression: filter_expr,
        expression_attribute_names: names,
        expression_attribute_values: values,
    })
}

fn build_filter_expression(
    expr: &Expr,
    counter: &mut i32,
    names: &mut HashMap<String, String>,
    values: &mut HashMap<String, AttributeValue>,
) -> Option<String> {
    match expr {
        Expr::BinaryExpr(binary) => match binary.op {
            Operator::And => {
                let l = build_filter_expression(&binary.left, counter, names, values)?;
                let r = build_filter_expression(&binary.right, counter, names, values)?;
                Some(format!("({} AND {})", l, r))
            }
            Operator::Or => {
                let l = build_filter_expression(&binary.left, counter, names, values)?;
                let r = build_filter_expression(&binary.right, counter, names, values)?;
                Some(format!("({} OR {})", l, r))
            }
            Operator::Eq => {
                build_comparison(&binary.left, &binary.right, "=", counter, names, values)
            }
            Operator::Gt => {
                build_comparison(&binary.left, &binary.right, ">", counter, names, values)
            }
            Operator::Lt => {
                build_comparison(&binary.left, &binary.right, "<", counter, names, values)
            }
            Operator::GtEq => {
                build_comparison(&binary.left, &binary.right, ">=", counter, names, values)
            }
            Operator::LtEq => {
                build_comparison(&binary.left, &binary.right, "<=", counter, names, values)
            }
            Operator::NotEq => {
                build_comparison(&binary.left, &binary.right, "<>", counter, names, values)
            }
            _ => {
                println!("Unsupported operator: {:?}", binary.op);
                None
            }
        },
        _ => {
            println!("Non-binary expr: {expr:?}");
            None
        }
    }
}

fn build_comparison(
    left: &Expr,
    right: &Expr,
    op: &str,
    counter: &mut i32,
    names: &mut HashMap<String, String>,
    values: &mut HashMap<String, AttributeValue>,
) -> Option<String> {
    let field = extract_column_name(left)?;
    let value = extract_literal_value(right)?;

    let name_placeholder = format!("#field{}", counter);
    let value_placeholder = format!(":val{}", counter);
    *counter += 1;

    names.insert(name_placeholder.clone(), field);
    values.insert(value_placeholder.clone(), convert_to_attribute_value(value)?);

    Some(format!("{} {} {}", name_placeholder, op, value_placeholder))
}

fn extract_column_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Column(col) => Some(col.name.clone()),
        _ => None,
    }
}

fn extract_literal_value(expr: &Expr) -> Option<Bson> {
    match expr {
        Expr::Literal(scalar, _) => match scalar {
            ScalarValue::Utf8(Some(s)) => Some(Bson::String(s.clone())),
            ScalarValue::Utf8(None) => Some(Bson::Null),
            ScalarValue::Int32(Some(i)) => Some(Bson::Int32(*i)),
            ScalarValue::Int32(None) => Some(Bson::Null),
            ScalarValue::Int64(Some(i)) => Some(Bson::Int64(*i)),
            ScalarValue::Int64(None) => Some(Bson::Null),
            ScalarValue::Float32(Some(f)) => Some(Bson::Double(*f as f64)),
            ScalarValue::Float32(None) => Some(Bson::Null),
            ScalarValue::Float64(Some(f)) => Some(Bson::Double(*f)),
            ScalarValue::Float64(None) => Some(Bson::Null),
            ScalarValue::Boolean(Some(b)) => Some(Bson::Boolean(*b)),
            ScalarValue::Boolean(None) => Some(Bson::Null),

            ScalarValue::UInt8(Some(i)) => Some(Bson::Int32(*i as i32)),
            ScalarValue::UInt16(Some(i)) => Some(Bson::Int32(*i as i32)),
            ScalarValue::UInt32(Some(i)) => Some(Bson::Int64(*i as i64)),
            ScalarValue::UInt64(Some(i)) => Some(Bson::Int64(*i as i64)),
            ScalarValue::Int8(Some(i)) => Some(Bson::Int32(*i as i32)),
            ScalarValue::Int16(Some(i)) => Some(Bson::Int32(*i as i32)),

            ScalarValue::UInt8(None)
            | ScalarValue::UInt16(None)
            | ScalarValue::UInt32(None)
            | ScalarValue::UInt64(None)
            | ScalarValue::Int8(None)
            | ScalarValue::Int16(None) => Some(Bson::Null),

            _ => None,
        },
        _ => None,
    }
}

// You'll need to implement this based on what extract_literal_value returns
fn convert_to_attribute_value(value: /* your value type */) -> Option<AttributeValue> {
    // Example conversions:
    // For strings: Some(AttributeValue::S(value.to_string()))
    // For numbers: Some(AttributeValue::N(value.to_string()))
    // For booleans: Some(AttributeValue::Bool(value))
    // For binary: Some(AttributeValue::B(Blob::new(value)))

    // Replace with actual conversion logic
    Some(AttributeValue::S(value.to_string()))
}