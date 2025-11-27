use arrow::datatypes::{DataType, Field, Fields, IntervalUnit, TimeUnit};
use arrow::error::ArrowError;
use serde_json::json;
use serde_json::Value;
use std::sync::Arc;

use crate::UnsupportedTypeAction;

#[derive(Debug, Clone)]
pub(crate) struct ParseContext {
    pub(crate) unsupported_type_action: UnsupportedTypeAction,
    pub(crate) type_details: Option<serde_json::Value>,
}

impl ParseContext {
    pub(crate) fn new() -> Self {
        Self {
            unsupported_type_action: UnsupportedTypeAction::Error,
            type_details: None,
        }
    }

    pub(crate) fn with_unsupported_type_action(
        mut self,
        unsupported_type_action: UnsupportedTypeAction,
    ) -> Self {
        self.unsupported_type_action = unsupported_type_action;
        self
    }

    pub(crate) fn with_type_details(mut self, type_details: serde_json::Value) -> Self {
        self.type_details = Some(type_details);
        self
    }
}

impl Default for ParseContext {
    fn default() -> Self {
        Self::new()
    }
}

pub(crate) fn pg_data_type_to_arrow_type(
    pg_type: &str,
    context: &ParseContext,
) -> Result<DataType, ArrowError> {
    let base_type = pg_type.split('(').next().unwrap_or(pg_type).trim();

    match base_type {
        "smallint" => Ok(DataType::Int16),
        "integer" | "int" | "int4" => Ok(DataType::Int32),
        "bigint" | "int8" | "money" => Ok(DataType::Int64),
        "oid" | "xid" | "regproc" => Ok(DataType::UInt32),
        "numeric" | "decimal" => {
            let (precision, scale) = parse_numeric_type(pg_type)?;
            Ok(DataType::Decimal128(precision, scale))
        }
        "real" | "float4" => Ok(DataType::Float32),
        "double precision" | "float8" => Ok(DataType::Float64),
        "\"char\"" => Ok(DataType::Int8),
        "character" | "char" | "character varying" | "varchar" | "text" | "bpchar" | "uuid"
        | "name" => Ok(DataType::Utf8),
        "bytea" => Ok(DataType::Binary),
        "date" => Ok(DataType::Date32),
        "time" | "time without time zone" => Ok(DataType::Time64(TimeUnit::Nanosecond)),
        "timestamp" | "timestamp without time zone" => {
            Ok(DataType::Timestamp(TimeUnit::Nanosecond, None))
        }
        "timestamp with time zone" | "timestamptz" => Ok(DataType::Timestamp(
            TimeUnit::Nanosecond,
            Some("UTC".into()),
        )),
        "interval" => Ok(DataType::Interval(IntervalUnit::MonthDayNano)),
        "boolean" => Ok(DataType::Boolean),
        "enum" => Ok(DataType::Dictionary(
            Box::new(DataType::Int8),
            Box::new(DataType::Utf8),
        )),
        "point" => Ok(DataType::FixedSizeList(
            Arc::new(Field::new("item", DataType::Float64, true)),
            2,
        )),
        "line" | "lseg" | "box" | "path" | "polygon" | "circle" => Ok(DataType::Binary),
        "inet" | "cidr" | "macaddr" => Ok(DataType::Utf8),
        "bit" | "bit varying" => Ok(DataType::Binary),
        "tsvector" | "tsquery" => Ok(DataType::LargeUtf8),
        "xml" | "json" => Ok(DataType::Utf8),
        // `Name` is a 64 bytes (varchar) / internal type for object names
        "\"Name\"" => Ok(DataType::Utf8),
        "aclitem" | "pg_node_tree" => Ok(DataType::Utf8),
        "array" => parse_array_type(context),
        "anyarray" => Ok(DataType::List(Arc::new(Field::new(
            "item",
            DataType::Binary,
            true,
        )))),
        "int4range" => Ok(DataType::Struct(Fields::from(vec![
            Field::new("lower", DataType::Int32, true),
            Field::new("upper", DataType::Int32, true),
        ]))),
        "composite" => parse_composite_type(context),
        "geometry" | "geography" => Ok(DataType::Binary),

        // `jsonb` is currently not supported, but if the user has set the `UnsupportedTypeAction` to `String` we'll return `Utf8`.
        "jsonb" if context.unsupported_type_action == UnsupportedTypeAction::String => {
            Ok(DataType::Utf8)
        }
        _ => Err(ArrowError::ParseError(format!(
            "Unsupported PostgreSQL type: {}",
            pg_type
        ))),
    }
}

fn parse_array_type(context: &ParseContext) -> Result<DataType, ArrowError> {
    let details = context
        .type_details
        .as_ref()
        .ok_or_else(|| ArrowError::ParseError("Missing type details for array type".to_string()))?;
    let details = details
        .as_object()
        .ok_or_else(|| ArrowError::ParseError("Invalid array type details format".to_string()))?;
    let element_type = details
        .get("element_type")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            ArrowError::ParseError("Missing or invalid element_type for array".to_string())
        })?;

    let inner_type = if element_type.ends_with("[]") {
        let inner_context = context.clone().with_type_details(json!({
            "type": "array",
            "element_type": element_type.trim_end_matches("[]"),
        }));
        parse_array_type(&inner_context)?
    } else {
        pg_data_type_to_arrow_type(element_type, context)?
    };

    Ok(DataType::List(Arc::new(Field::new(
        "item", inner_type, true,
    ))))
}

fn parse_composite_type(context: &ParseContext) -> Result<DataType, ArrowError> {
    let details = context.type_details.as_ref().ok_or_else(|| {
        ArrowError::ParseError("Missing type details for composite type".to_string())
    })?;
    let details = details.as_object().ok_or_else(|| {
        ArrowError::ParseError("Invalid composite type details format".to_string())
    })?;
    let attributes = details
        .get("attributes")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            ArrowError::ParseError("Missing or invalid attributes for composite type".to_string())
        })?;

    let fields: Result<Vec<Field>, ArrowError> = attributes
        .iter()
        .map(|attr| {
            let attr_obj = attr.as_object().ok_or_else(|| {
                ArrowError::ParseError("Invalid attribute format in composite type".to_string())
            })?;
            let name = attr_obj
                .get("name")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    ArrowError::ParseError(
                        "Missing or invalid name in composite type attribute".to_string(),
                    )
                })?;
            let attr_type = attr_obj
                .get("type")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    ArrowError::ParseError(
                        "Missing or invalid type in composite type attribute".to_string(),
                    )
                })?;
            let field_type = if attr_type == "composite" {
                let inner_context = context.clone().with_type_details(attr.clone());
                parse_composite_type(&inner_context)?
            } else {
                pg_data_type_to_arrow_type(attr_type, context)?
            };
            Ok(Field::new(name, field_type, true))
        })
        .collect();

    Ok(DataType::Struct(Fields::from(fields?)))
}

fn parse_numeric_type(pg_type: &str) -> Result<(u8, i8), ArrowError> {
    let type_str = pg_type
        .trim_start_matches("numeric")
        .trim_start_matches("decimal")
        .trim();

    if type_str.is_empty() || type_str == "()" {
        return Ok((38, 20)); // Default precision and scale if not specified
    }

    let parts: Vec<&str> = type_str
        .trim_start_matches('(')
        .trim_end_matches(')')
        .split(',')
        .collect();

    match parts.len() {
        1 => {
            let precision = parts[0]
                .trim()
                .parse::<u8>()
                .map_err(|_| ArrowError::ParseError("Invalid numeric precision".to_string()))?;
            Ok((precision, 0))
        }
        2 => {
            let precision = parts[0]
                .trim()
                .parse::<u8>()
                .map_err(|_| ArrowError::ParseError("Invalid numeric precision".to_string()))?;
            let scale = parts[1]
                .trim()
                .parse::<i8>()
                .map_err(|_| ArrowError::ParseError("Invalid numeric scale".to_string()))?;
            Ok((precision, scale))
        }
        _ => Err(ArrowError::ParseError(
            "Invalid numeric type format".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pg_data_type_to_arrow_type() {
        let context = ParseContext::new();
        // Test basic types
        assert_eq!(
            pg_data_type_to_arrow_type("smallint", &context).expect("Failed to convert smallint"),
            DataType::Int16
        );
        assert_eq!(
            pg_data_type_to_arrow_type("integer", &context).expect("Failed to convert integer"),
            DataType::Int32
        );
        assert_eq!(
            pg_data_type_to_arrow_type("bigint", &context).expect("Failed to convert bigint"),
            DataType::Int64
        );
        assert_eq!(
            pg_data_type_to_arrow_type("real", &context).expect("Failed to convert real"),
            DataType::Float32
        );
        assert_eq!(
            pg_data_type_to_arrow_type("double precision", &context)
                .expect("Failed to convert double precision"),
            DataType::Float64
        );
        assert_eq!(
            pg_data_type_to_arrow_type("boolean", &context).expect("Failed to convert boolean"),
            DataType::Boolean
        );
        assert_eq!(
            pg_data_type_to_arrow_type("\"char\"", &context)
                .expect("Failed to convert single character"),
            DataType::Int8
        );

        // Test string types
        assert_eq!(
            pg_data_type_to_arrow_type("character", &context).expect("Failed to convert character"),
            DataType::Utf8
        );
        assert_eq!(
            pg_data_type_to_arrow_type("character varying", &context)
                .expect("Failed to convert character varying"),
            DataType::Utf8
        );
        assert_eq!(
            pg_data_type_to_arrow_type("name", &context).expect("Failed to convert name"),
            DataType::Utf8
        );
        assert_eq!(
            pg_data_type_to_arrow_type("text", &context).expect("Failed to convert text"),
            DataType::Utf8
        );

        // Test date/time types
        assert_eq!(
            pg_data_type_to_arrow_type("date", &context).expect("Failed to convert date"),
            DataType::Date32
        );
        assert_eq!(
            pg_data_type_to_arrow_type("time without time zone", &context)
                .expect("Failed to convert time without time zone"),
            DataType::Time64(TimeUnit::Nanosecond)
        );
        assert_eq!(
            pg_data_type_to_arrow_type("timestamp without time zone", &context)
                .expect("Failed to convert timestamp without time zone"),
            DataType::Timestamp(TimeUnit::Nanosecond, None)
        );
        assert_eq!(
            pg_data_type_to_arrow_type("timestamp with time zone", &context)
                .expect("Failed to convert timestamp with time zone"),
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
        );
        assert_eq!(
            pg_data_type_to_arrow_type("interval", &context).expect("Failed to convert interval"),
            DataType::Interval(IntervalUnit::MonthDayNano)
        );

        // Test numeric types
        assert_eq!(
            pg_data_type_to_arrow_type("numeric", &context).expect("Failed to convert numeric"),
            DataType::Decimal128(38, 20)
        );
        assert_eq!(
            pg_data_type_to_arrow_type("numeric()", &context).expect("Failed to convert numeric()"),
            DataType::Decimal128(38, 20)
        );
        assert_eq!(
            pg_data_type_to_arrow_type("numeric(10,2)", &context)
                .expect("Failed to convert numeric(10,2)"),
            DataType::Decimal128(10, 2)
        );

        // Test array type
        let array_type_context = context.clone().with_type_details(json!({
            "type": "array",
            "element_type": "integer",
        }));
        assert_eq!(
            pg_data_type_to_arrow_type("array", &array_type_context)
                .expect("Failed to convert array"),
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true)))
        );

        // Test composite type
        let composite_type_context = context.clone().with_type_details(json!({
            "type": "composite",
            "attributes": [
                {"name": "x", "type": "integer"},
                {"name": "y", "type": "text"}
            ]
        }));
        assert_eq!(
            pg_data_type_to_arrow_type("composite", &composite_type_context)
                .expect("Failed to convert composite"),
            DataType::Struct(Fields::from(vec![
                Field::new("x", DataType::Int32, true),
                Field::new("y", DataType::Utf8, true)
            ]))
        );

        // Test unsupported type
        assert!(pg_data_type_to_arrow_type("unsupported_type", &context).is_err());
    }

    #[test]
    fn test_parse_numeric_type() {
        assert_eq!(
            parse_numeric_type("numeric").expect("Failed to parse numeric"),
            (38, 20)
        );
        assert_eq!(
            parse_numeric_type("numeric()").expect("Failed to parse numeric()"),
            (38, 20)
        );
        assert_eq!(
            parse_numeric_type("numeric(10)").expect("Failed to parse numeric(10)"),
            (10, 0)
        );
        assert_eq!(
            parse_numeric_type("numeric(10,2)").expect("Failed to parse numeric(10,2)"),
            (10, 2)
        );
        assert_eq!(
            parse_numeric_type("decimal").expect("Failed to parse decimal"),
            (38, 20)
        );
        assert_eq!(
            parse_numeric_type("decimal()").expect("Failed to parse decimal()"),
            (38, 20)
        );
        assert_eq!(
            parse_numeric_type("decimal(15)").expect("Failed to parse decimal(15)"),
            (15, 0)
        );
        assert_eq!(
            parse_numeric_type("decimal(15,5)").expect("Failed to parse decimal(15,5)"),
            (15, 5)
        );

        // Test invalid formats
        assert!(parse_numeric_type("numeric(invalid)").is_err());
        assert!(parse_numeric_type("numeric(10,2,3)").is_err());
        assert!(parse_numeric_type("numeric(,)").is_err());
    }

    #[test]
    fn test_pg_data_type_to_arrow_type_with_size() {
        let context = ParseContext::new();
        assert_eq!(
            pg_data_type_to_arrow_type("character(10)", &context)
                .expect("Failed to convert character(10)"),
            DataType::Utf8
        );
        assert_eq!(
            pg_data_type_to_arrow_type("character varying(255)", &context)
                .expect("Failed to convert character varying(255)"),
            DataType::Utf8
        );
        assert_eq!(
            pg_data_type_to_arrow_type("bit(8)", &context).expect("Failed to convert bit(8)"),
            DataType::Binary
        );
        assert_eq!(
            pg_data_type_to_arrow_type("bit varying(64)", &context)
                .expect("Failed to convert bit varying(64)"),
            DataType::Binary
        );
        assert_eq!(
            pg_data_type_to_arrow_type("numeric(10,2)", &context)
                .expect("Failed to convert numeric(10,2)"),
            DataType::Decimal128(10, 2)
        );
    }

    #[test]
    fn test_pg_data_type_to_arrow_type_extended() {
        let context = ParseContext::new();
        // Test additional numeric types
        assert_eq!(
            pg_data_type_to_arrow_type("numeric(38,10)", &context)
                .expect("Failed to convert numeric(38,10)"),
            DataType::Decimal128(38, 10)
        );
        assert_eq!(
            pg_data_type_to_arrow_type("decimal(5,0)", &context)
                .expect("Failed to convert decimal(5,0)"),
            DataType::Decimal128(5, 0)
        );

        // Test time types with precision
        assert_eq!(
            pg_data_type_to_arrow_type("time(6) without time zone", &context)
                .expect("Failed to convert time(6) without time zone"),
            DataType::Time64(TimeUnit::Nanosecond)
        );

        // Test array types
        let nested_array_type_details = context.clone().with_type_details(json!({
            "type": "array",
            "element_type": "integer[]",
        }));
        assert_eq!(
            pg_data_type_to_arrow_type("array", &nested_array_type_details)
                .expect("Failed to convert nested array"),
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                true
            )))
        );

        // Test enum type
        let enum_type_details = context.clone().with_type_details(json!({
            "type": "enum",
            "values": ["small", "medium", "large"]
        }));
        assert_eq!(
            pg_data_type_to_arrow_type("enum", &enum_type_details).expect("Failed to convert enum"),
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8))
        );

        // Test geometric types
        assert_eq!(
            pg_data_type_to_arrow_type("point", &context).expect("Failed to convert point"),
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float64, true)), 2)
        );
        assert_eq!(
            pg_data_type_to_arrow_type("line", &context).expect("Failed to convert line"),
            DataType::Binary
        );

        // Test network address types
        assert_eq!(
            pg_data_type_to_arrow_type("inet", &context).expect("Failed to convert inet"),
            DataType::Utf8
        );
        assert_eq!(
            pg_data_type_to_arrow_type("cidr", &context).expect("Failed to convert cidr"),
            DataType::Utf8
        );

        // Test range types
        assert_eq!(
            pg_data_type_to_arrow_type("int4range", &context).expect("Failed to convert int4range"),
            DataType::Struct(Fields::from(vec![
                Field::new("lower", DataType::Int32, true),
                Field::new("upper", DataType::Int32, true),
            ]))
        );

        // Test JSON types
        assert_eq!(
            pg_data_type_to_arrow_type("json", &context).expect("Failed to convert json"),
            DataType::Utf8
        );

        let jsonb_context = context
            .clone()
            .with_unsupported_type_action(UnsupportedTypeAction::String);
        assert_eq!(
            pg_data_type_to_arrow_type("jsonb", &jsonb_context).expect("Failed to convert jsonb"),
            DataType::Utf8
        );

        // Test UUID type
        assert_eq!(
            pg_data_type_to_arrow_type("uuid", &context).expect("Failed to convert uuid"),
            DataType::Utf8
        );

        // Test text search types
        assert_eq!(
            pg_data_type_to_arrow_type("tsvector", &context).expect("Failed to convert tsvector"),
            DataType::LargeUtf8
        );
        assert_eq!(
            pg_data_type_to_arrow_type("tsquery", &context).expect("Failed to convert tsquery"),
            DataType::LargeUtf8
        );

        // Test bpchar type
        assert_eq!(
            pg_data_type_to_arrow_type("bpchar", &context).expect("Failed to convert bpchar"),
            DataType::Utf8
        );

        // Test bpchar with length specification
        assert_eq!(
            pg_data_type_to_arrow_type("bpchar(10)", &context)
                .expect("Failed to convert bpchar(10)"),
            DataType::Utf8
        );
    }

    #[test]
    fn test_parse_array_type_extended() {
        let context = ParseContext::new();
        let single_dim_array = context.clone().with_type_details(json!({
            "type": "array",
            "element_type": "integer",
        }));
        assert_eq!(
            parse_array_type(&single_dim_array).expect("Failed to parse single dimension array"),
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true)))
        );

        let multi_dim_array = context.clone().with_type_details(json!({
            "type": "array",
            "element_type": "text[]",
        }));
        assert_eq!(
            parse_array_type(&multi_dim_array).expect("Failed to parse multi-dimension array"),
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true
            )))
        );

        let invalid_array = context.clone().with_type_details(json!({"type": "array"}));
        assert!(parse_array_type(&invalid_array).is_err());
    }

    #[test]
    fn test_parse_composite_type_extended() {
        let context = ParseContext::new();
        let simple_composite = context.clone().with_type_details(json!({
            "type": "composite",
            "attributes": [
                {"name": "id", "type": "integer"},
                {"name": "name", "type": "text"},
                {"name": "active", "type": "boolean"}
            ]
        }));
        assert_eq!(
            parse_composite_type(&simple_composite).expect("Failed to parse simple composite type"),
            DataType::Struct(Fields::from(vec![
                Field::new("id", DataType::Int32, true),
                Field::new("name", DataType::Utf8, true),
                Field::new("active", DataType::Boolean, true),
            ]))
        );

        let nested_composite = context.clone().with_type_details(json!({
            "type": "composite",
            "attributes": [
                {"name": "id", "type": "integer"},
                {"name": "details", "type": "composite", "attributes": [
                    {"name": "x", "type": "float8"},
                    {"name": "y", "type": "float8"}
                ]}
            ]
        }));
        assert_eq!(
            parse_composite_type(&nested_composite).expect("Failed to parse nested composite type"),
            DataType::Struct(Fields::from(vec![
                Field::new("id", DataType::Int32, true),
                Field::new(
                    "details",
                    DataType::Struct(Fields::from(vec![
                        Field::new("x", DataType::Float64, true),
                        Field::new("y", DataType::Float64, true),
                    ])),
                    true
                ),
            ]))
        );

        let invalid_composite = context.clone().with_type_details(json!({
            "type": "composite",
        }));
        assert!(parse_composite_type(&invalid_composite).is_err());
    }
}
