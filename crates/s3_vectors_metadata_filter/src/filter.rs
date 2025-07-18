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

use crate::{
    Error,
    error::{
        EmptyArraySnafu, InvalidFilterSnafu, InvalidValueTypeSnafu, JsonParsingSnafu, Result,
        UnsupportedOperatorSnafu,
    },
};
use aws_smithy_types::Document;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use snafu::ResultExt;
use std::{collections::HashMap, fmt, str::FromStr};

/// [`MetadataFilter`] is a data structure that defines expressible filters for S3 vector.
/// ## Filter Examples
/// ### Simple Equality (Implicit $eq)
/// ```json
/// {"genre": "documentary"}
/// ```
///
/// ### Explicit Operations
///
/// ```json
/// {"genre": {"$eq": "documentary"}}
/// {"genre": {"$ne": "drama"}}
/// ```
///
/// ### Numeric Comparisons
/// ```json
/// {"year": {"$gt": 2019}}
/// {"year": {"$gte": 2020}}
/// {"year": {"$lt": 2020}}
/// {"year": {"$lte": 2020}}
/// ```
///
/// ### Array Operations
/// ```json
/// {"genre": {"$in": ["comedy", "documentary"]}}
/// {"genre": {"$nin": ["horror", "thriller"]}}
/// ```
///
/// ### Existence Checks
/// ```json
/// {"genre": {"$exists": true}}
/// {"optional_field": {"$exists": false}}
/// ```
///
/// ### Logical Operations
/// ```json
/// {"$and": [{"genre": {"$eq": "drama"}}, {"year": {"$gte": 2020}}]}
/// {"$or": [{"genre": {"$eq": "drama"}}, {"year": {"$gte": 2020}}]}
/// ```
///
/// ### Multiple Conditions on Same Field
/// ```json
/// {"price": {"$gte": 10, "$lte": 50}}
/// ```
///
/// ### Complex Nested Filters
/// ```json
/// {
///     "$and": [
///         {"genre": {"$in": ["drama", "thriller"]}},
///         {"$or": [
///             {"year": {"$gte": 2020}},
///             {"rating": {"$gte": 8.5}}
///         ]},
///         {"budget": {"$exists": true}}
///     ]
/// }
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum MetadataFilter {
    /// Complex filter with operators.
    Complex(FilterExpression),
    /// Simple field-value equality (implicit $eq). `Simple` must be last enum member as all
    /// `Complex` values will serialize incorrectly into `Simple`.
    Simple(HashMap<String, Value>),
}

impl From<MetadataFilter> for Map<String, Value> {
    fn from(val: MetadataFilter) -> Self {
        match val {
            MetadataFilter::Simple(map) => map.into_iter().collect(),
            MetadataFilter::Complex(expr) => expr.into(),
        }
    }
}

impl From<MetadataFilter> for Document {
    fn from(val: MetadataFilter) -> Self {
        match val {
            MetadataFilter::Simple(map) => Document::Object(
                map.into_iter()
                    .map(|(k, v)| (k, json_value_to_document(v)))
                    .collect(),
            ),
            MetadataFilter::Complex(expr) => expr.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum FilterExpression {
    /// Logical operations (check first)
    Logical(LogicalOperation),
    /// Field-based operations
    Field(HashMap<String, FieldOperation>),
}

impl From<FilterExpression> for Map<String, Value> {
    fn from(val: FilterExpression) -> Self {
        match val {
            FilterExpression::Logical(logical) => logical.into(),
            FilterExpression::Field(field_map) => {
                field_map.into_iter().map(|(k, v)| (k, v.into())).collect()
            }
        }
    }
}

impl From<FilterExpression> for Document {
    fn from(val: FilterExpression) -> Self {
        match val {
            FilterExpression::Logical(logical) => logical.into(),
            FilterExpression::Field(field_map) => {
                let mut map = HashMap::new();
                for (k, v) in field_map {
                    map.insert(k, v.into());
                }
                Document::Object(map)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum FieldOperation {
    /// Direct value (implicit $eq)
    Direct(Value),
    /// Operation with operator
    Operation(HashMap<String, Value>),
}

impl From<FieldOperation> for Value {
    fn from(val: FieldOperation) -> Self {
        match val {
            FieldOperation::Direct(value) => value,
            FieldOperation::Operation(map) => {
                let mut result = Map::new();
                for (key, value) in map {
                    result.insert(key, value);
                }
                Value::Object(result)
            }
        }
    }
}

impl From<FieldOperation> for Document {
    fn from(value: FieldOperation) -> Self {
        match value {
            FieldOperation::Direct(value) => json_value_to_document(value),
            FieldOperation::Operation(map) => {
                let mut result = HashMap::new();
                for (key, value) in map {
                    result.insert(key, json_value_to_document(value));
                }
                Document::Object(result)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LogicalOperation {
    #[serde(rename = "$and", skip_serializing_if = "Option::is_none")]
    pub and: Option<Vec<FilterExpression>>,
    #[serde(rename = "$or", skip_serializing_if = "Option::is_none")]
    pub or: Option<Vec<FilterExpression>>,
}

impl From<LogicalOperation> for Map<String, Value> {
    fn from(val: LogicalOperation) -> Self {
        let mut result = Map::new();

        if let Some(and_filters) = val.and {
            let and_array: Vec<Value> = and_filters
                .into_iter()
                .map(|expr| Value::Object(expr.into()))
                .collect();
            result.insert("$and".to_string(), Value::Array(and_array));
        }

        if let Some(or_filters) = val.or {
            let or_array: Vec<Value> = or_filters
                .into_iter()
                .map(|expr| Value::Object(expr.into()))
                .collect();
            result.insert("$or".to_string(), Value::Array(or_array));
        }

        result
    }
}

impl From<LogicalOperation> for Document {
    fn from(val: LogicalOperation) -> Self {
        let mut result = HashMap::new();

        if let Some(and_filters) = val.and {
            let and_array: Vec<Document> = and_filters.into_iter().map(Into::into).collect();
            result.insert("$and".to_string(), Document::Array(and_array));
        }

        if let Some(or_filters) = val.or {
            let or_array: Vec<Document> = or_filters.into_iter().map(Into::into).collect();
            result.insert("$or".to_string(), Document::Array(or_array));
        }

        Document::Object(result)
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Operator {
    Eq,
    Ne,
    Gt,
    Gte,
    Lt,
    Lte,
    In,
    Nin,
    Exists,
}

impl FromStr for Operator {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "$eq" => Ok(Operator::Eq),
            "$ne" => Ok(Operator::Ne),
            "$gt" => Ok(Operator::Gt),
            "$gte" => Ok(Operator::Gte),
            "$lt" => Ok(Operator::Lt),
            "$lte" => Ok(Operator::Lte),
            "$in" => Ok(Operator::In),
            "$nin" => Ok(Operator::Nin),
            "$exists" => Ok(Operator::Exists),
            _ => UnsupportedOperatorSnafu { not_an_operator: s }.fail(),
        }
    }
}
impl Operator {
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Operator::Eq => "$eq",
            Operator::Ne => "$ne",
            Operator::Gt => "$gt",
            Operator::Gte => "$gte",
            Operator::Lt => "$lt",
            Operator::Lte => "$lte",
            Operator::In => "$in",
            Operator::Nin => "$nin",
            Operator::Exists => "$exists",
        }
    }
}

impl fmt::Display for MetadataFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MetadataFilter::Simple(map) => {
                write!(f, "{{")?;
                let mut first = true;
                for (key, value) in map {
                    if !first {
                        write!(f, ",")?;
                    }
                    first = false;
                    write!(f, "{}:{}", key, format_value(value))?;
                }
                write!(f, "}}")
            }
            MetadataFilter::Complex(expr) => write!(f, "{expr}"),
        }
    }
}

impl fmt::Display for FilterExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FilterExpression::Logical(logical) => write!(f, "{logical}"),
            FilterExpression::Field(field_map) => {
                write!(f, "{{")?;
                let mut first = true;
                for (key, op) in field_map {
                    if !first {
                        write!(f, ",")?;
                    }
                    first = false;
                    write!(f, "{key}:{op}")?;
                }
                write!(f, "}}")
            }
        }
    }
}

impl fmt::Display for FieldOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FieldOperation::Direct(value) => write!(f, "{}", format_value(value)),
            FieldOperation::Operation(map) => {
                write!(f, "{{")?;
                let mut first = true;
                for (op, value) in map {
                    if !first {
                        write!(f, ",")?;
                    }
                    first = false;
                    write!(f, "{}:{}", op, format_value(value))?;
                }
                write!(f, "}}")
            }
        }
    }
}

impl fmt::Display for LogicalOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{{")?;
        let mut first = true;

        if let Some(and_filters) = &self.and {
            write!(f, "$and:[")?;
            for (i, filter) in and_filters.iter().enumerate() {
                if i > 0 {
                    write!(f, ",")?;
                }
                write!(f, "{filter}")?;
            }
            write!(f, "]")?;
            first = false;
        }

        if let Some(or_filters) = &self.or {
            if !first {
                write!(f, ",")?;
            }
            write!(f, "$or:[")?;
            for (i, filter) in or_filters.iter().enumerate() {
                if i > 0 {
                    write!(f, ",")?;
                }
                write!(f, "{filter}")?;
            }
            write!(f, "]")?;
        }

        write!(f, "}}")
    }
}

fn format_value(value: &Value) -> String {
    match value {
        Value::String(s) => format!("\"{s}\""),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Array(arr) => {
            let items: Vec<String> = arr.iter().map(format_value).collect();
            format!("[{}]", items.join(","))
        }
        Value::Object(obj) => {
            let items: Vec<String> = obj
                .iter()
                .map(|(k, v)| format!("{k}:{}", format_value(v)))
                .collect();
            format!("{{{}}}", items.join(","))
        }
        Value::Null => "null".to_string(),
    }
}

impl MetadataFilter {
    pub fn from_json(json: &str) -> Result<Self> {
        let value: Value = serde_json::from_str(json).context(JsonParsingSnafu)?;
        Self::from_value(&value)
    }

    fn from_value(value: &Value) -> Result<Self> {
        match value {
            Value::Object(obj) => {
                // Check if this is a logical operation
                if obj.contains_key("$and") || obj.contains_key("$or") {
                    let logical = LogicalOperation::from_value(value)?;
                    return Ok(MetadataFilter::Complex(FilterExpression::Logical(logical)));
                }

                // Check if any field has complex operations
                let mut is_complex = false;
                for (_, field_value) in obj {
                    if let Value::Object(field_obj) = field_value {
                        // Check if any key starts with $
                        if field_obj.keys().any(|k| k.starts_with('$')) {
                            is_complex = true;
                            break;
                        }
                    }
                }

                if is_complex {
                    let mut field_map = HashMap::new();
                    for (field, field_value) in obj {
                        field_map.insert(field.clone(), FieldOperation::from(field_value));
                    }
                    Ok(MetadataFilter::Complex(FilterExpression::Field(field_map)))
                } else {
                    // Simple equality filter
                    let map: HashMap<String, Value> =
                        obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                    Ok(MetadataFilter::Simple(map))
                }
            }
            _ => InvalidFilterSnafu {
                message: "Filter must be a JSON object".to_string(),
            }
            .fail(),
        }
    }

    pub fn to_json(&self) -> Result<String> {
        serde_json::to_string(self).context(JsonParsingSnafu)
    }

    pub fn validate(&self) -> Result<()> {
        match self {
            MetadataFilter::Simple(map) => {
                for (field, value) in map {
                    validate_field_name(field)?;
                    validate_primitive_value(value)?;
                }
            }
            MetadataFilter::Complex(expr) => {
                validate_expression(expr)?;
            }
        }
        Ok(())
    }
}

fn validate_field_name(field: &str) -> Result<()> {
    if field.is_empty() {
        return InvalidFilterSnafu {
            message: "Field name cannot be empty".to_string(),
        }
        .fail();
    }
    if field.starts_with('$') {
        return InvalidFilterSnafu {
            message: format!("Field name cannot start with '$': {field}"),
        }
        .fail();
    }
    Ok(())
}

fn validate_primitive_value(value: &Value) -> Result<()> {
    match value {
        Value::String(_) | Value::Number(_) | Value::Bool(_) => Ok(()),
        Value::Array(arr) => {
            for item in arr {
                validate_primitive_value(item)?;
            }
            Ok(())
        }
        _ => InvalidFilterSnafu {
            message: format!("Invalid value type: {value}"),
        }
        .fail(),
    }
}

fn validate_expression(expr: &FilterExpression) -> Result<()> {
    match expr {
        FilterExpression::Field(map) => {
            for (field, op) in map {
                validate_field_name(field)?;
                validate_field_operation(op)?;
            }
        }
        FilterExpression::Logical(logical) => {
            validate_logical_operation(logical)?;
        }
    }
    Ok(())
}

fn validate_field_operation(op: &FieldOperation) -> Result<()> {
    match op {
        FieldOperation::Direct(value) => validate_primitive_value(value),
        FieldOperation::Operation(map) => {
            for (operator, value) in map {
                let op = Operator::from_str(operator)?;
                validate_operator_value(op, value)?;
            }
            Ok(())
        }
    }
}

fn validate_operator_value(operator: Operator, value: &Value) -> Result<()> {
    match operator {
        Operator::Eq | Operator::Ne => {
            if !matches!(value, Value::String(_) | Value::Number(_) | Value::Bool(_)) {
                return InvalidValueTypeSnafu {
                    operator,
                    expected: "string, number, or boolean",
                    actual: value_type_name(value),
                }
                .fail();
            }
        }
        Operator::Gt | Operator::Gte | Operator::Lt | Operator::Lte => {
            if !matches!(value, Value::Number(_)) {
                return InvalidValueTypeSnafu {
                    operator,
                    expected: "number",
                    actual: value_type_name(value),
                }
                .fail();
            }
        }
        Operator::In | Operator::Nin => {
            if let Value::Array(arr) = value {
                if arr.is_empty() {
                    return EmptyArraySnafu {
                        aggregate: operator.as_str(),
                    }
                    .fail();
                }
                for item in arr {
                    if !matches!(item, Value::String(_) | Value::Number(_) | Value::Bool(_)) {
                        return InvalidValueTypeSnafu {
                            operator,
                            expected: "array of primitives",
                            actual: value_type_name(item),
                        }
                        .fail();
                    }
                }
            } else {
                return InvalidValueTypeSnafu {
                    operator,
                    expected: "array",
                    actual: value_type_name(value),
                }
                .fail();
            }
        }
        Operator::Exists => {
            if !matches!(value, Value::Bool(_)) {
                return InvalidValueTypeSnafu {
                    operator,
                    expected: "boolean",
                    actual: value_type_name(value),
                }
                .fail();
            }
        }
    }
    Ok(())
}

fn validate_logical_operation(logical: &LogicalOperation) -> Result<()> {
    if let Some(and_filters) = &logical.and {
        if and_filters.is_empty() {
            return EmptyArraySnafu { aggregate: "$and" }.fail();
        }
        for filter in and_filters {
            validate_expression(filter)?;
        }
    }
    if let Some(or_filters) = &logical.or {
        if or_filters.is_empty() {
            return EmptyArraySnafu { aggregate: "$or" }.fail();
        }
        for filter in or_filters {
            validate_expression(filter)?;
        }
    }
    Ok(())
}

impl LogicalOperation {
    fn from_value(value: &Value) -> Result<Self> {
        if let Value::Object(obj) = value {
            let mut logical = LogicalOperation {
                and: None,
                or: None,
            };

            if let Some(and_value) = obj.get("$and") {
                if let Value::Array(arr) = and_value {
                    let mut filters = Vec::new();
                    for item in arr {
                        filters.push(FilterExpression::from_value(item)?);
                    }
                    logical.and = Some(filters);
                } else {
                    return InvalidFilterSnafu {
                        message: "$and must be an array".to_string(),
                    }
                    .fail();
                }
            }

            if let Some(or_value) = obj.get("$or") {
                if let Value::Array(arr) = or_value {
                    let mut filters = Vec::new();
                    for item in arr {
                        filters.push(FilterExpression::from_value(item)?);
                    }
                    logical.or = Some(filters);
                } else {
                    return InvalidFilterSnafu {
                        message: "$or must be an array".to_string(),
                    }
                    .fail();
                }
            }

            Ok(logical)
        } else {
            InvalidFilterSnafu {
                message: "Logical operation must be an object".to_string(),
            }
            .fail()
        }
    }
}

impl FilterExpression {
    fn from_value(value: &Value) -> Result<Self> {
        if let Value::Object(obj) = value {
            // Check if this is a logical operation
            if obj.contains_key("$and") || obj.contains_key("$or") {
                let logical = LogicalOperation::from_value(value)?;
                Ok(FilterExpression::Logical(logical))
            } else {
                // Field operations
                let mut field_map = HashMap::new();
                for (field, field_value) in obj {
                    field_map.insert(field.clone(), field_value.into());
                }
                Ok(FilterExpression::Field(field_map))
            }
        } else {
            InvalidFilterSnafu {
                message: "Filter expression must be an object".to_string(),
            }
            .fail()
        }
    }
}

impl From<&Value> for FieldOperation {
    fn from(value: &Value) -> Self {
        match value {
            Value::Object(obj) => {
                // Check if any key starts with $ (operator)
                if obj.keys().any(|k| k.starts_with('$')) {
                    let map: HashMap<String, Value> =
                        obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                    FieldOperation::Operation(map)
                } else {
                    // Direct value as object (treat as direct)
                    FieldOperation::Direct(value.clone())
                }
            }
            _ => FieldOperation::Direct(value.clone()),
        }
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

#[must_use]
pub fn document_to_json_map(document: Document) -> Map<String, Value> {
    match document {
        Document::Object(map) => map
            .into_iter()
            .map(|(k, v)| (k, document_to_json_value(v)))
            .collect(),
        _ => Map::new(),
    }
}

#[must_use]
pub fn document_to_json_value(document: Document) -> Value {
    match document {
        Document::Object(map) => Value::Object(
            map.into_iter()
                .map(|(k, v)| (k, document_to_json_value(v)))
                .collect(),
        ),
        Document::Array(arr) => Value::Array(arr.into_iter().map(document_to_json_value).collect()),
        Document::Number(num) => aws_number_to_json_number(num).map_or(Value::Null, Value::Number),
        Document::String(s) => Value::String(s),
        Document::Bool(b) => Value::Bool(b),
        Document::Null => Value::Null,
    }
}

#[must_use]
pub fn aws_number_to_json_number(num: aws_smithy_types::Number) -> Option<serde_json::Number> {
    match num {
        aws_smithy_types::Number::PosInt(pos_int) => Some(serde_json::Number::from(pos_int)),
        aws_smithy_types::Number::NegInt(neg_int) => Some(serde_json::Number::from(neg_int)),
        aws_smithy_types::Number::Float(float) => serde_json::Number::from_f64(float),
    }
}

#[must_use]
#[allow(clippy::needless_pass_by_value)]
pub fn json_number_to_aws_number(num: serde_json::Number) -> Option<aws_smithy_types::Number> {
    if num.is_i64() {
        let i = num.as_i64()?;
        if i >= 0 {
            #[allow(clippy::cast_sign_loss)]
            Some(aws_smithy_types::Number::PosInt(i as u64))
        } else {
            Some(aws_smithy_types::Number::NegInt(i))
        }
    } else if num.is_u64() {
        Some(aws_smithy_types::Number::PosInt(num.as_u64()?))
    } else if num.is_f64() {
        Some(aws_smithy_types::Number::Float(num.as_f64()?))
    } else {
        None
    }
}

#[must_use]
pub fn json_value_to_document(value: Value) -> Document {
    match value {
        Value::Object(map) => Document::Object(
            map.into_iter()
                .map(|(k, v)| (k, json_value_to_document(v)))
                .collect(),
        ),
        Value::Array(arr) => Document::Array(arr.into_iter().map(json_value_to_document).collect()),
        Value::Number(num) => {
            json_number_to_aws_number(num).map_or(Document::Null, Document::Number)
        }
        Value::String(s) => Document::String(s),
        Value::Bool(b) => Document::Bool(b),
        Value::Null => Document::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[allow(clippy::unwrap_used)]
    fn test_valid_expressions() {
        let valid_expressions = vec![
            r#"{"year": {"$gte": 2020}}"#,
            r#"{"year": {"$lt": 2020}}"#,
            r#"{"genre": {"$nin": ["horror", "thriller"]}}"#,
            r#"{"genre": {"$exists": true}}"#,
            r#"{"optional_field": {"$exists": false}}"#,
            r#"{"$or": [{"genre": {"$eq": "drama"}}, {"year": {"$gte": 2020}}]}"#,
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
            r#"{"$or": [{"genre": {"$eq": "drama"}}, {"year": {"$gte": 2020}}]}"#,
        ];
        for expr in valid_expressions {
            assert!(MetadataFilter::from_json(expr).unwrap().validate().is_ok());
        }
    }

    #[test]
    fn test_display_formatting() {
        // Test simple filter
        let simple_filter =
            MetadataFilter::from_json(r#"{"genre": "drama"}"#).expect("Failed to parse filter");
        assert_eq!(simple_filter.to_string(), r#"{genre:"drama"}"#);

        // Test complex filter with operator
        let complex_filter = MetadataFilter::from_json(r#"{"year": {"$gte": 2020}}"#)
            .expect("Failed to parse filter");
        assert_eq!(complex_filter.to_string(), r"{year:{$gte:2020}}");

        // Test array operation
        let array_filter = MetadataFilter::from_json(r#"{"genre": {"$in": ["drama", "comedy"]}}"#)
            .expect("Failed to parse filter");
        assert_eq!(
            array_filter.to_string(),
            r#"{genre:{$in:["drama","comedy"]}}"#
        );

        // Test logical operation
        let logical_filter = MetadataFilter::from_json(
            r#"{"$or": [{"genre": {"$eq": "drama"}}, {"year": {"$gte": 2020}}]}"#,
        )
        .expect("Failed to parse filter");
        assert_eq!(
            logical_filter.to_string(),
            r#"{$or:[{genre:{$eq:"drama"}},{year:{$gte:2020}}]}"#
        );
    }
}
