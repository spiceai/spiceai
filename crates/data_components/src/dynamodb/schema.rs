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
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use aws_sdk_dynamodb::types::AttributeValue;
use std::collections::HashMap;
use std::sync::Arc;

use super::{Error, Result};

pub fn infer_arrow_schema_from_items(
    items: &[HashMap<String, AttributeValue>],
) -> Result<SchemaRef> {
    if items.is_empty() {
        return Ok(Arc::new(Schema::empty()));
    }

    let mut field_types: HashMap<String, DataType> = HashMap::new();

    for item in items {
        analyze_item(item, &mut field_types);
    }

    let mut fields: Vec<Field> = field_types
        .into_iter()
        .map(|(name, data_type)| Field::new(name, data_type, true))
        .collect();

    // Sort fields by column name to make sure the schema is deterministic
    fields.sort_by(|a, b| a.name().cmp(b.name()));

    Ok(Arc::new(Schema::new(fields)))
}

fn analyze_item(
    item: &HashMap<String, AttributeValue>,
    field_types: &mut HashMap<String, DataType>,
) {
    for (key, value) in item {
        let inferred_type = infer_dynamodb_type(value);

        match field_types.get(key) {
            Some(existing_type) => {
                // Use the most general type
                let unified_type = unify_types(existing_type, &inferred_type);
                field_types.insert(key.clone(), unified_type);
            }
            None => {
                field_types.insert(key.clone(), inferred_type);
            }
        }
    }
}

fn infer_dynamodb_type(value: &AttributeValue) -> DataType {
    match value {
        AttributeValue::Bool(_) => DataType::Boolean,
        AttributeValue::S(s) => {
            // Try to detect temporal types
            if is_iso8601_timestamp(s) {
                DataType::Timestamp(TimeUnit::Millisecond, Some(Arc::from("UTC")))
            } else if is_date_yyyy_mm_dd(s) {
                DataType::Date32
            } else {
                DataType::Utf8
            }
        }
        AttributeValue::Ss(_) => DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
        AttributeValue::N(n) => {
            // Determine if it's an integer or float based on the string representation
            if n.contains('.') || n.contains('e') || n.contains('E') {
                DataType::Float64
            } else {
                DataType::Int64
            }
        }
        AttributeValue::Ns(numbers) => {
            // Determine the type based on the first number in the set
            let inner_type = if let Some(first) = numbers.first() {
                if first.contains('.') || first.contains('e') || first.contains('E') {
                    DataType::Float64
                } else {
                    DataType::Int64
                }
            } else {
                DataType::Int64 // Default to Int64 for empty sets
            };
            DataType::List(Arc::new(Field::new("item", inner_type, true)))
        }
        AttributeValue::B(_) => DataType::Binary,
        AttributeValue::Bs(_) => {
            DataType::List(Arc::new(Field::new("item", DataType::Binary, true)))
        }
        AttributeValue::L(_) => {
            // DynamoDB lists can be heterogeneous [1, "foo", true]
            // Arrow arrays must be homogeneous - use strings to preserve all data
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))
        }
        AttributeValue::M(_) => {
            // Represent nested maps as JSON strings
            // Consider recursive schema inference with unnest_depth in the future
            DataType::Utf8
        }
        AttributeValue::Null(_) => DataType::Null,
        _ => DataType::Utf8, // Fallback for any other AttributeValue variants
    }
}

fn unify_types(type1: &DataType, type2: &DataType) -> DataType {
    match (type1, type2) {
        (a, b) if a == b => a.clone(),
        (DataType::Null, other) | (other, DataType::Null) => other.clone(),

        // Numeric type promotion for scalar numbers
        (DataType::Int64, DataType::Float64) | (DataType::Float64, DataType::Int64) => {
            DataType::Float64
        }

        // Temporal type unification - if same temporal type, keep it
        (DataType::Timestamp(_, _), DataType::Timestamp(_, _)) => type1.clone(),
        (DataType::Date32, DataType::Date32) => DataType::Date32,
        (DataType::Date64, DataType::Date64) => DataType::Date64,

        // Mixed temporal types - fall back to string
        (DataType::Timestamp(_, _), DataType::Date32)
        | (DataType::Date32, DataType::Timestamp(_, _))
        | (DataType::Timestamp(_, _), DataType::Date64)
        | (DataType::Date64, DataType::Timestamp(_, _))
        | (DataType::Date32, DataType::Date64)
        | (DataType::Date64, DataType::Date32) => DataType::Utf8,

        // Temporal mixed with string - fall back to string
        (DataType::Timestamp(_, _), DataType::Utf8)
        | (DataType::Utf8, DataType::Timestamp(_, _))
        | (DataType::Date32, DataType::Utf8)
        | (DataType::Utf8, DataType::Date32)
        | (DataType::Date64, DataType::Utf8)
        | (DataType::Utf8, DataType::Date64) => DataType::Utf8,

        // Numeric type promotion for lists (e.g., Number Sets)
        (DataType::List(field1), DataType::List(field2)) => {
            let unified_inner = unify_types(field1.data_type(), field2.data_type());
            DataType::List(Arc::new(Field::new("item", unified_inner, true)))
        }

        // Otherwise use string as the most general type
        _ => DataType::Utf8,
    }
}

fn is_iso8601_timestamp(s: &str) -> bool {
    // Try parsing as ISO8601 timestamp
    // Handles formats like: 2023-08-31T12:34:56Z, 2023-08-31T12:34:56.123Z, 2023-08-31T12:34:56+00:00
    chrono::DateTime::parse_from_rfc3339(s).is_ok()
        || s.parse::<chrono::DateTime<chrono::Utc>>().is_ok()
}

fn is_date_yyyy_mm_dd(s: &str) -> bool {
    // Check for YYYY-MM-DD format (exactly 10 chars with 2 dashes)
    if s.len() == 10 && s.chars().filter(|c| *c == '-').count() == 2 {
        chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").is_ok()
    } else {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_dynamodb::types::AttributeValue;
    use datafusion::arrow::datatypes::DataType;
    use std::collections::HashMap;

    fn av_string(s: &str) -> AttributeValue {
        AttributeValue::S(s.to_string())
    }

    fn av_number(n: &str) -> AttributeValue {
        AttributeValue::N(n.to_string())
    }

    fn av_bool(b: bool) -> AttributeValue {
        AttributeValue::Bool(b)
    }

    #[test]
    fn test_empty_items() {
        let items: Vec<HashMap<String, AttributeValue>> = vec![];
        let schema = infer_arrow_schema_from_items(&items).unwrap();
        assert_eq!(schema.fields().len(), 0);
    }

    #[test]
    fn test_single_item_simple_types() {
        let mut item = HashMap::new();
        item.insert("name".to_string(), av_string("Alice"));
        item.insert("age".to_string(), av_number("30"));
        item.insert("height".to_string(), av_number("5.6"));
        item.insert("is_active".to_string(), av_bool(true));

        let items = vec![item];
        let schema = infer_arrow_schema_from_items(&items).unwrap();

        // Check field count
        assert_eq!(schema.fields().len(), 4);

        // Check each field type
        let field_map: HashMap<String, &DataType> = schema
            .fields()
            .iter()
            .map(|f| (f.name().clone(), f.data_type()))
            .collect();

        assert_eq!(field_map.get("name"), Some(&&DataType::Utf8));
        assert_eq!(field_map.get("age"), Some(&&DataType::Int64));
        assert_eq!(field_map.get("height"), Some(&&DataType::Float64));
        assert_eq!(field_map.get("is_active"), Some(&&DataType::Boolean));

        // Check all fields are nullable
        for field in schema.fields() {
            assert!(field.is_nullable());
        }
    }

    #[test]
    fn test_dynamodb_specific_types() {
        let mut item = HashMap::new();
        item.insert(
            "tags".to_string(),
            AttributeValue::Ss(vec!["tag1".to_string(), "tag2".to_string()]),
        );
        item.insert(
            "scores".to_string(),
            AttributeValue::Ns(vec!["10".to_string(), "20".to_string()]),
        );
        item.insert(
            "data".to_string(),
            AttributeValue::B(aws_sdk_dynamodb::primitives::Blob::new(vec![1, 2, 3])),
        );
        item.insert(
            "binary_set".to_string(),
            AttributeValue::Bs(vec![
                aws_sdk_dynamodb::primitives::Blob::new(vec![1, 2]),
                aws_sdk_dynamodb::primitives::Blob::new(vec![3, 4]),
            ]),
        );

        let items = vec![item];
        let schema = infer_arrow_schema_from_items(&items).unwrap();
        let field_map: HashMap<String, &DataType> = schema
            .fields()
            .iter()
            .map(|f| (f.name().clone(), f.data_type()))
            .collect();

        // String Set
        if let Some(DataType::List(field)) = field_map.get("tags") {
            assert_eq!(field.data_type(), &DataType::Utf8);
        } else {
            panic!("Expected List<Utf8> for tags");
        }

        // Number Set
        if let Some(DataType::List(field)) = field_map.get("scores") {
            assert_eq!(field.data_type(), &DataType::Int64);
        } else {
            panic!("Expected List<Int64> for scores");
        }

        // Binary
        assert_eq!(field_map.get("data"), Some(&&DataType::Binary));

        // Binary Set
        if let Some(DataType::List(field)) = field_map.get("binary_set") {
            assert_eq!(field.data_type(), &DataType::Binary);
        } else {
            panic!("Expected List<Binary> for binary_set");
        }
    }

    #[test]
    fn test_number_type_inference() {
        let mut item = HashMap::new();
        item.insert("integer".to_string(), av_number("42"));
        item.insert("float".to_string(), av_number("3.14"));
        item.insert("scientific".to_string(), av_number("1.5e10"));
        item.insert("negative_int".to_string(), av_number("-100"));
        item.insert("negative_float".to_string(), av_number("-2.5"));

        let items = vec![item];
        let schema = infer_arrow_schema_from_items(&items).unwrap();
        let field_map: HashMap<String, &DataType> = schema
            .fields()
            .iter()
            .map(|f| (f.name().clone(), f.data_type()))
            .collect();

        assert_eq!(field_map.get("integer"), Some(&&DataType::Int64));
        assert_eq!(field_map.get("float"), Some(&&DataType::Float64));
        assert_eq!(field_map.get("scientific"), Some(&&DataType::Float64));
        assert_eq!(field_map.get("negative_int"), Some(&&DataType::Int64));
        assert_eq!(field_map.get("negative_float"), Some(&&DataType::Float64));
    }

    #[test]
    fn test_number_set_type_inference() {
        let mut item1 = HashMap::new();
        item1.insert(
            "int_set".to_string(),
            AttributeValue::Ns(vec!["1".to_string(), "2".to_string()]),
        );

        let mut item2 = HashMap::new();
        item2.insert(
            "float_set".to_string(),
            AttributeValue::Ns(vec!["1.5".to_string(), "2.5".to_string()]),
        );

        let items = vec![item1, item2];
        let schema = infer_arrow_schema_from_items(&items).unwrap();
        let field_map: HashMap<String, &DataType> = schema
            .fields()
            .iter()
            .map(|f| (f.name().clone(), f.data_type()))
            .collect();

        if let Some(DataType::List(field)) = field_map.get("int_set") {
            assert_eq!(field.data_type(), &DataType::Int64);
        } else {
            panic!("Expected List<Int64> for int_set");
        }

        if let Some(DataType::List(field)) = field_map.get("float_set") {
            assert_eq!(field.data_type(), &DataType::Float64);
        } else {
            panic!("Expected List<Float64> for float_set");
        }
    }

    #[test]
    fn test_list_types() {
        let mut item = HashMap::new();
        item.insert("empty_list".to_string(), AttributeValue::L(vec![]));
        item.insert(
            "string_list".to_string(),
            AttributeValue::L(vec![av_string("a"), av_string("b"), av_string("c")]),
        );
        item.insert(
            "number_list".to_string(),
            AttributeValue::L(vec![av_number("1"), av_number("2"), av_number("3")]),
        );
        item.insert(
            "mixed_list".to_string(),
            AttributeValue::L(vec![av_string("text"), av_number("42"), av_bool(true)]),
        );

        let items = vec![item];
        let schema = infer_arrow_schema_from_items(&items).unwrap();
        let field_map: HashMap<String, &DataType> = schema
            .fields()
            .iter()
            .map(|f| (f.name().clone(), f.data_type()))
            .collect();

        // All lists should be List<Utf8> since DynamoDB lists are heterogeneous
        for list_name in &["empty_list", "string_list", "number_list", "mixed_list"] {
            if let Some(DataType::List(field)) = field_map.get(*list_name) {
                assert_eq!(
                    field.data_type(),
                    &DataType::Utf8,
                    "Expected List<Utf8> for {}",
                    list_name
                );
            } else {
                panic!("Expected List type for {}", list_name);
            }
        }
    }

    #[test]
    fn test_map_types() {
        let mut inner_map = HashMap::new();
        inner_map.insert("name".to_string(), av_string("Alice"));
        inner_map.insert("age".to_string(), av_number("30"));

        let mut item = HashMap::new();
        item.insert("user".to_string(), AttributeValue::M(inner_map));
        item.insert("metadata".to_string(), AttributeValue::M(HashMap::new()));

        let items = vec![item];
        let schema = infer_arrow_schema_from_items(&items).unwrap();
        let field_map: HashMap<String, &DataType> = schema
            .fields()
            .iter()
            .map(|f| (f.name().clone(), f.data_type()))
            .collect();

        // Maps should be treated as strings (JSON) by default
        assert_eq!(field_map.get("user"), Some(&&DataType::Utf8));
        assert_eq!(field_map.get("metadata"), Some(&&DataType::Utf8));
    }

    #[test]
    fn test_type_unification_numeric_promotion_int_to_float() {
        let mut item1 = HashMap::new();
        item1.insert("value".to_string(), av_number("10"));

        let mut item2 = HashMap::new();
        item2.insert("value".to_string(), av_number("3.14"));

        let items = vec![item1, item2];
        let schema = infer_arrow_schema_from_items(&items).unwrap();
        let field = schema.field_with_name("value").unwrap();
        assert_eq!(field.data_type(), &DataType::Float64);
    }

    #[test]
    fn test_type_unification_number_set_promotion() {
        let mut item1 = HashMap::new();
        item1.insert(
            "numbers".to_string(),
            AttributeValue::Ns(vec!["1".to_string(), "2".to_string()]),
        );

        let mut item2 = HashMap::new();
        item2.insert(
            "numbers".to_string(),
            AttributeValue::Ns(vec!["1.5".to_string(), "2.5".to_string()]),
        );

        let items = vec![item1, item2];
        let schema = infer_arrow_schema_from_items(&items).unwrap();
        let field = schema.field_with_name("numbers").unwrap();

        if let DataType::List(inner_field) = field.data_type() {
            assert_eq!(inner_field.data_type(), &DataType::Float64);
        } else {
            panic!("Expected List type");
        }
    }

    #[test]
    fn test_type_unification_to_string_fallback() {
        let mut item1 = HashMap::new();
        item1.insert("value".to_string(), av_number("10"));

        let mut item2 = HashMap::new();
        item2.insert("value".to_string(), av_string("text"));

        let items = vec![item1, item2];
        let schema = infer_arrow_schema_from_items(&items).unwrap();
        let field = schema.field_with_name("value").unwrap();
        assert_eq!(field.data_type(), &DataType::Utf8);
    }

    #[test]
    fn test_null_unification() {
        let mut item1 = HashMap::new();
        item1.insert("value".to_string(), AttributeValue::Null(true));

        let mut item2 = HashMap::new();
        item2.insert("value".to_string(), av_string("text"));

        let items = vec![item1, item2];
        let schema = infer_arrow_schema_from_items(&items).unwrap();
        let field = schema.field_with_name("value").unwrap();
        assert_eq!(field.data_type(), &DataType::Utf8);
    }

    #[test]
    fn test_only_null_values() {
        let mut item1 = HashMap::new();
        item1.insert("value".to_string(), AttributeValue::Null(true));

        let mut item2 = HashMap::new();
        item2.insert("value".to_string(), AttributeValue::Null(true));

        let items = vec![item1, item2];
        let schema = infer_arrow_schema_from_items(&items).unwrap();
        let field = schema.field_with_name("value").unwrap();
        assert_eq!(field.data_type(), &DataType::Null);
    }

    #[test]
    fn test_missing_fields_across_items() {
        let mut item1 = HashMap::new();
        item1.insert("name".to_string(), av_string("Alice"));
        item1.insert("age".to_string(), av_number("30"));

        let mut item2 = HashMap::new();
        item2.insert("name".to_string(), av_string("Bob"));
        item2.insert("city".to_string(), av_string("NYC"));

        let mut item3 = HashMap::new();
        item3.insert("age".to_string(), av_number("25"));
        item3.insert("country".to_string(), av_string("US"));

        let items = vec![item1, item2, item3];
        let schema = infer_arrow_schema_from_items(&items).unwrap();

        // Should have all unique fields
        assert_eq!(schema.fields().len(), 4);

        let field_names: std::collections::HashSet<&str> =
            schema.fields().iter().map(|f| f.name().as_str()).collect();

        assert!(field_names.contains("name"));
        assert!(field_names.contains("age"));
        assert!(field_names.contains("city"));
        assert!(field_names.contains("country"));

        // All fields should be nullable since they're missing in some items
        for field in schema.fields() {
            assert!(field.is_nullable());
        }
    }

    #[test]
    fn test_fields_are_sorted() {
        let mut item = HashMap::new();
        item.insert("zebra".to_string(), av_string("striped"));
        item.insert("apple".to_string(), av_string("red"));
        item.insert("monkey".to_string(), av_string("brown"));
        item.insert("banana".to_string(), av_string("yellow"));

        let items = vec![item];
        let schema = infer_arrow_schema_from_items(&items).unwrap();

        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(field_names, vec!["apple", "banana", "monkey", "zebra"]);
    }

    #[test]
    fn test_large_item_set() {
        let mut items = Vec::new();

        // Generate 100 items with varying schemas
        for i in 0..100 {
            let mut item = HashMap::new();
            item.insert("id".to_string(), av_number(&i.to_string()));
            item.insert("name".to_string(), av_string(&format!("user_{}", i)));

            // Add optional fields for some items
            if i % 2 == 0 {
                item.insert("age".to_string(), av_number(&(20 + i % 50).to_string()));
            }
            if i % 3 == 0 {
                item.insert("city".to_string(), av_string("NYC"));
            }
            if i % 5 == 0 {
                let score = (i as f64) / 10.0;
                item.insert("score".to_string(), av_number(&score.to_string()));
            }

            items.push(item);
        }

        let schema = infer_arrow_schema_from_items(&items).unwrap();

        // Should have all the fields
        let field_names: std::collections::HashSet<&str> =
            schema.fields().iter().map(|f| f.name().as_str()).collect();

        assert!(field_names.contains("id"));
        assert!(field_names.contains("name"));
        assert!(field_names.contains("age"));
        assert!(field_names.contains("city"));
        assert!(field_names.contains("score"));

        // All fields should be nullable
        for field in schema.fields() {
            assert!(field.is_nullable());
        }
    }

    #[test]
    fn test_iso8601_timestamp_detection() {
        let items = vec![
            HashMap::from([
                ("id".to_string(), av_string("1")),
                ("created_at".to_string(), av_string("2023-08-31T12:34:56Z")),
            ]),
            HashMap::from([
                ("id".to_string(), av_string("2")),
                (
                    "created_at".to_string(),
                    av_string("2024-01-15T08:22:11.123Z"),
                ),
            ]),
        ];

        let schema = infer_arrow_schema_from_items(&items).unwrap();
        let created_at_field = schema.field_with_name("created_at").unwrap();

        assert!(matches!(
            created_at_field.data_type(),
            DataType::Timestamp(TimeUnit::Millisecond, Some(_))
        ));
    }

    #[test]
    fn test_iso8601_timestamp_with_timezone() {
        let items = vec![
            HashMap::from([(
                "event_time".to_string(),
                av_string("2023-08-31T12:34:56+00:00"),
            )]),
            HashMap::from([(
                "event_time".to_string(),
                av_string("2024-01-15T08:22:11-05:00"),
            )]),
        ];

        let schema = infer_arrow_schema_from_items(&items).unwrap();
        let event_time_field = schema.field_with_name("event_time").unwrap();

        assert!(matches!(
            event_time_field.data_type(),
            DataType::Timestamp(TimeUnit::Millisecond, Some(_))
        ));
    }

    #[test]
    fn test_date_yyyy_mm_dd_detection() {
        let items = vec![
            HashMap::from([
                ("id".to_string(), av_string("1")),
                ("birth_date".to_string(), av_string("2024-01-15")),
            ]),
            HashMap::from([
                ("id".to_string(), av_string("2")),
                ("birth_date".to_string(), av_string("1990-05-22")),
            ]),
        ];

        let schema = infer_arrow_schema_from_items(&items).unwrap();
        let birth_date_field = schema.field_with_name("birth_date").unwrap();

        assert_eq!(birth_date_field.data_type(), &DataType::Date32);
    }

    #[test]
    fn test_mixed_timestamp_and_plain_string_falls_back_to_utf8() {
        let items = vec![
            HashMap::from([("value".to_string(), av_string("2023-08-31T12:34:56Z"))]),
            HashMap::from([("value".to_string(), av_string("not a timestamp"))]),
        ];

        let schema = infer_arrow_schema_from_items(&items).unwrap();
        let value_field = schema.field_with_name("value").unwrap();

        assert_eq!(value_field.data_type(), &DataType::Utf8);
    }

    #[test]
    fn test_mixed_date_and_plain_string_falls_back_to_utf8() {
        let items = vec![
            HashMap::from([("value".to_string(), av_string("2024-01-15"))]),
            HashMap::from([("value".to_string(), av_string("random text"))]),
        ];

        let schema = infer_arrow_schema_from_items(&items).unwrap();
        let value_field = schema.field_with_name("value").unwrap();

        assert_eq!(value_field.data_type(), &DataType::Utf8);
    }

    #[test]
    fn test_mixed_timestamp_and_date_falls_back_to_utf8() {
        let items = vec![
            HashMap::from([("value".to_string(), av_string("2023-08-31T12:34:56Z"))]),
            HashMap::from([("value".to_string(), av_string("2024-01-15"))]),
        ];

        let schema = infer_arrow_schema_from_items(&items).unwrap();
        let value_field = schema.field_with_name("value").unwrap();

        assert_eq!(value_field.data_type(), &DataType::Utf8);
    }

    #[test]
    fn test_invalid_date_format_stays_utf8() {
        let items = vec![
            HashMap::from([
                ("value".to_string(), av_string("01-15-2024")), // MM-DD-YYYY, not valid
            ]),
            HashMap::from([
                ("value".to_string(), av_string("2024/01/15")), // Wrong separator
            ]),
        ];

        let schema = infer_arrow_schema_from_items(&items).unwrap();
        let value_field = schema.field_with_name("value").unwrap();

        assert_eq!(value_field.data_type(), &DataType::Utf8);
    }

    #[test]
    fn test_invalid_timestamp_format_stays_utf8() {
        let items = vec![
            HashMap::from([
                ("value".to_string(), av_string("2023-08-31 12:34:56")), // Missing T
            ]),
            HashMap::from([
                ("value".to_string(), av_string("2023-13-31T12:34:56Z")), // Invalid month
            ]),
        ];

        let schema = infer_arrow_schema_from_items(&items).unwrap();
        let value_field = schema.field_with_name("value").unwrap();

        assert_eq!(value_field.data_type(), &DataType::Utf8);
    }

    #[test]
    fn test_multiple_temporal_columns() {
        let items = vec![
            HashMap::from([
                ("id".to_string(), av_string("1")),
                ("created_at".to_string(), av_string("2023-08-31T12:34:56Z")),
                ("birth_date".to_string(), av_string("1990-05-22")),
                ("name".to_string(), av_string("John Doe")),
            ]),
            HashMap::from([
                ("id".to_string(), av_string("2")),
                ("created_at".to_string(), av_string("2024-01-15T08:22:11Z")),
                ("birth_date".to_string(), av_string("1985-12-10")),
                ("name".to_string(), av_string("Jane Smith")),
            ]),
        ];

        let schema = infer_arrow_schema_from_items(&items).unwrap();

        let id_field = schema.field_with_name("id").unwrap();
        assert_eq!(id_field.data_type(), &DataType::Utf8);

        let created_at_field = schema.field_with_name("created_at").unwrap();
        assert!(matches!(
            created_at_field.data_type(),
            DataType::Timestamp(TimeUnit::Millisecond, Some(_))
        ));

        let birth_date_field = schema.field_with_name("birth_date").unwrap();
        assert_eq!(birth_date_field.data_type(), &DataType::Date32);

        let name_field = schema.field_with_name("name").unwrap();
        assert_eq!(name_field.data_type(), &DataType::Utf8);
    }

    #[test]
    fn test_consistent_timestamps_across_many_items() {
        let items: Vec<_> = (0..10)
            .map(|i| {
                HashMap::from([
                    ("id".to_string(), av_string(&i.to_string())),
                    (
                        "timestamp".to_string(),
                        av_string(&format!("2024-01-{:02}T10:00:00Z", i + 1)),
                    ),
                ])
            })
            .collect();

        let schema = infer_arrow_schema_from_items(&items).unwrap();
        let timestamp_field = schema.field_with_name("timestamp").unwrap();

        assert!(matches!(
            timestamp_field.data_type(),
            DataType::Timestamp(TimeUnit::Millisecond, Some(_))
        ));
    }

    #[test]
    fn test_empty_string_with_temporal_types() {
        let items = vec![
            HashMap::from([("value".to_string(), av_string("2024-01-15"))]),
            HashMap::from([("value".to_string(), av_string(""))]),
        ];

        let schema = infer_arrow_schema_from_items(&items).unwrap();
        let value_field = schema.field_with_name("value").unwrap();

        assert_eq!(value_field.data_type(), &DataType::Utf8);
    }

    #[test]
    fn test_null_value_with_temporal_types() {
        let items = vec![
            HashMap::from([
                ("id".to_string(), av_string("1")),
                ("timestamp".to_string(), av_string("2024-01-15T10:00:00Z")),
            ]),
            HashMap::from([
                ("id".to_string(), av_string("2")),
                ("timestamp".to_string(), AttributeValue::Null(true)),
            ]),
            HashMap::from([
                ("id".to_string(), av_string("3")),
                ("timestamp".to_string(), av_string("2024-01-16T10:00:00Z")),
            ]),
        ];

        let schema = infer_arrow_schema_from_items(&items).unwrap();
        let timestamp_field = schema.field_with_name("timestamp").unwrap();

        assert!(matches!(
            timestamp_field.data_type(),
            DataType::Timestamp(TimeUnit::Millisecond, Some(_))
        ));
    }
}
