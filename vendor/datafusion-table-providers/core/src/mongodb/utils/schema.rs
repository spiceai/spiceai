use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use mongodb::bson::{Bson, Document};
use std::collections::HashMap;
use std::sync::Arc;

use crate::mongodb::{Error, Result};
use chrono::{LocalResult, TimeZone, Timelike, Utc};

pub fn infer_arrow_schema_from_documents(
    docs: &[Document],
    tz: Option<&str>,
) -> Result<SchemaRef, Error> {
    if docs.is_empty() {
        return Ok(Arc::new(Schema::empty()));
    }

    let mut field_types: HashMap<String, DataType> = HashMap::new();

    for doc in docs {
        analyze_document(doc, &mut field_types, tz);
    }

    let mut fields: Vec<Field> = field_types
        .into_iter()
        .map(|(name, data_type)| Field::new(name, data_type, true))
        .collect();

    // Sort fields by column name to make sure the schema is deterministic
    fields.sort_by(|a, b| a.name().cmp(b.name()));

    Ok(Arc::new(Schema::new(fields)))
}

fn analyze_document(doc: &Document, field_types: &mut HashMap<String, DataType>, tz: Option<&str>) {
    for (key, value) in doc {
        let inferred_type = infer_bson_type(value, tz);

        match field_types.get(key) {
            Some(existing_type) => {
                // Ue the most general type
                let unified_type = unify_types(existing_type, &inferred_type);
                field_types.insert(key.clone(), unified_type);
            }
            None => {
                field_types.insert(key.clone(), inferred_type);
            }
        }
    }
}

fn infer_bson_type(value: &Bson, tz: Option<&str>) -> DataType {
    match value {
        Bson::Double(_) => DataType::Float64,
        Bson::String(_) => DataType::Utf8,
        Bson::Array(_) => {
            // MongoDB arrays can be heterogeneous [1, "foo", true]
            // Arrow arrays must be homogeneous - use strings to preserve all data
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))
        }
        Bson::Document(_) => {
            // Represent nested documents as JSON strings
            // Maybe consider to recursively infer nested schemas in the future
            DataType::Utf8
        }
        Bson::Boolean(_) => DataType::Boolean,
        Bson::Null => DataType::Null,
        Bson::RegularExpression(_) => DataType::Utf8,
        Bson::JavaScriptCode(_) => DataType::Utf8,
        Bson::JavaScriptCodeWithScope(_) => DataType::Utf8,
        Bson::Int32(_) => DataType::Int32,
        Bson::Int64(_) => DataType::Int64,
        Bson::Timestamp(_) => DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
        Bson::Binary(_) => DataType::Binary,
        Bson::ObjectId(_) => DataType::Utf8,
        Bson::DateTime(dt) => {
            let millis = dt.timestamp_millis();
            match Utc.timestamp_millis_opt(millis) {
                LocalResult::Single(chrono_dt) => {
                    let is_midnight = chrono_dt.time().num_seconds_from_midnight() == 0
                        && chrono_dt.time().nanosecond() == 0;
                    if is_midnight {
                        DataType::Date32
                    } else {
                        DataType::Timestamp(
                            TimeUnit::Millisecond,
                            Some(Arc::from(tz.unwrap_or("UTC"))),
                        )
                    }
                }
                _ => {
                    DataType::Timestamp(TimeUnit::Millisecond, Some(Arc::from(tz.unwrap_or("UTC"))))
                }
            }
        }
        Bson::Symbol(_) => DataType::Utf8,
        Bson::Decimal128(_) => DataType::Decimal128(18, 6),
        Bson::Undefined => DataType::Null,
        Bson::MaxKey => DataType::Utf8,
        Bson::MinKey => DataType::Utf8,
        Bson::DbPointer(_) => DataType::Utf8,
    }
}

fn unify_types(type1: &DataType, type2: &DataType) -> DataType {
    match (type1, type2) {
        (a, b) if a == b => a.clone(),
        (DataType::Null, other) | (other, DataType::Null) => other.clone(),

        // Numeric type promotion
        (DataType::Int32, DataType::Int64) | (DataType::Int64, DataType::Int32) => DataType::Int64,
        (DataType::Int32, DataType::Float64) | (DataType::Float64, DataType::Int32) => {
            DataType::Float64
        }
        (DataType::Int64, DataType::Float64) | (DataType::Float64, DataType::Int64) => {
            DataType::Float64
        }
        (DataType::Date32, DataType::Timestamp(tu, tz)) => DataType::Timestamp(*tu, tz.clone()),

        // Otherwise use string
        _ => DataType::Utf8,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, TimeUnit};
    use mongodb::bson::{doc, Bson, Document};
    use std::str::FromStr;

    #[test]
    fn test_empty_documents() {
        let docs: Vec<Document> = vec![];
        let schema = infer_arrow_schema_from_documents(&docs, None).unwrap();
        assert_eq!(schema.fields().len(), 0);
    }

    #[test]
    fn test_single_document_simple_types() {
        let doc = doc! {
            "name": "Alice",
            "age": 30_i32,
            "height": 5.6_f64,
            "is_active": true
        };
        let docs = vec![doc];

        let schema = infer_arrow_schema_from_documents(&docs, None).unwrap();

        // Check field count
        assert_eq!(schema.fields().len(), 4);

        // Check each field type (order may vary due to HashMap)
        let field_map: HashMap<String, &DataType> = schema
            .fields()
            .iter()
            .map(|f| (f.name().clone(), f.data_type()))
            .collect();

        assert_eq!(field_map.get("name"), Some(&&DataType::Utf8));
        assert_eq!(field_map.get("age"), Some(&&DataType::Int32));
        assert_eq!(field_map.get("height"), Some(&&DataType::Float64));
        assert_eq!(field_map.get("is_active"), Some(&&DataType::Boolean));

        // Check all fields are nullable
        for field in schema.fields() {
            assert!(field.is_nullable());
        }
    }

    #[test]
    fn test_mongodb_specific_types() {
        let doc = doc! {
            "id": mongodb::bson::oid::ObjectId::new(),
            "created_at": mongodb::bson::DateTime::now(),
            "timestamp": mongodb::bson::Timestamp { time: 1234567890, increment: 1 },
            "binary_data": mongodb::bson::Binary { subtype: mongodb::bson::spec::BinarySubtype::Generic, bytes: vec![1, 2, 3] },
            "decimal": mongodb::bson::Decimal128::from_str("123.456").unwrap(),
        };
        let docs = vec![doc];

        let schema = infer_arrow_schema_from_documents(&docs, None).unwrap();
        let field_map: HashMap<String, &DataType> = schema
            .fields()
            .iter()
            .map(|f| (f.name().clone(), f.data_type()))
            .collect();

        assert_eq!(field_map.get("id"), Some(&&DataType::Utf8)); // ObjectId as string
        assert_eq!(
            field_map.get("created_at"),
            Some(&&DataType::Timestamp(
                TimeUnit::Millisecond,
                Some("UTC".into())
            ))
        );
        assert_eq!(
            field_map.get("timestamp"),
            Some(&&DataType::Timestamp(TimeUnit::Millisecond, None))
        );
        assert_eq!(field_map.get("binary_data"), Some(&&DataType::Binary));
        assert_eq!(
            field_map.get("decimal"),
            Some(&&DataType::Decimal128(18, 6))
        );
    }

    #[test]
    fn test_mongodb_custom_timezone() {
        let doc = doc! {
            "created_at": mongodb::bson::DateTime::now(),
        };
        let docs = vec![doc];

        let schema = infer_arrow_schema_from_documents(&docs, Some("+02:00")).unwrap();
        let field_map: HashMap<String, &DataType> = schema
            .fields()
            .iter()
            .map(|f| (f.name().clone(), f.data_type()))
            .collect();

        assert_eq!(
            field_map.get("created_at"),
            Some(&&DataType::Timestamp(
                TimeUnit::Millisecond,
                Some("+02:00".into())
            ))
        );
    }

    #[test]
    fn test_date32_detection() {
        let doc = doc! {
            "created_date": mongodb::bson::DateTime::builder().year(2021).month(1).day(1).build().unwrap(),
        };
        let docs = vec![doc];

        let schema = infer_arrow_schema_from_documents(&docs, None).unwrap();
        let field_map: HashMap<String, &DataType> = schema
            .fields()
            .iter()
            .map(|f| (f.name().clone(), f.data_type()))
            .collect();

        assert_eq!(field_map.get("created_date"), Some(&&DataType::Date32));
    }

    #[test]
    fn test_array_types() {
        let doc = doc! {
            "empty_array": [],
            "string_array": ["a", "b", "c"],
            "number_array": [1_i32, 2_i32, 3_i32],
            "mixed_array": ["text", 42_i32, true], // Should infer from first non-null
            "null_array": [Bson::Null, Bson::Null, "finally_text"]
        };
        let docs = vec![doc];

        let schema = infer_arrow_schema_from_documents(&docs, None).unwrap();
        let field_map: HashMap<String, &DataType> = schema
            .fields()
            .iter()
            .map(|f| (f.name().clone(), f.data_type()))
            .collect();

        assert!(matches!(
            field_map.get("empty_array"),
            Some(DataType::List(_))
        ));

        if let Some(DataType::List(field)) = field_map.get("string_array") {
            assert_eq!(field.data_type(), &DataType::Utf8);
        } else {
            panic!("Expected List type for string_array");
        }

        if let Some(DataType::List(field)) = field_map.get("number_array") {
            assert_eq!(field.data_type(), &DataType::Utf8);
        } else {
            panic!("Expected List type for number_array");
        }

        if let Some(DataType::List(field)) = field_map.get("mixed_array") {
            assert_eq!(field.data_type(), &DataType::Utf8);
        } else {
            panic!("Expected List type for mixed_array");
        }

        if let Some(DataType::List(field)) = field_map.get("null_array") {
            assert_eq!(field.data_type(), &DataType::Utf8);
        } else {
            panic!("Expected List type for null_array");
        }
    }

    #[test]
    fn test_nested_document() {
        let doc = doc! {
            "user": {
                "name": "Alice",
                "age": 30_i32
            },
            "metadata": {}
        };
        let docs = vec![doc];

        let schema = infer_arrow_schema_from_documents(&docs, None).unwrap();
        let field_map: HashMap<String, &DataType> = schema
            .fields()
            .iter()
            .map(|f| (f.name().clone(), f.data_type()))
            .collect();

        // Nested documents should be treated as strings (JSON)
        assert_eq!(field_map.get("user"), Some(&&DataType::Utf8));
        assert_eq!(field_map.get("metadata"), Some(&&DataType::Utf8));
    }

    #[test]
    fn test_type_unification_numeric_promotion() {
        let docs = vec![
            doc! { "value": 10_i32 }, // Int32
            doc! { "value": 20_i64 }, // Int64 -> should promote to Int64
        ];

        let schema = infer_arrow_schema_from_documents(&docs, None).unwrap();
        let field = schema.field_with_name("value").unwrap();
        assert_eq!(field.data_type(), &DataType::Int64);
    }

    #[test]
    #[allow(clippy::approx_constant)]
    fn test_type_unification_to_float() {
        let docs = vec![
            doc! { "value": 10_i32 },   // Int32
            doc! { "value": 3.14_f64 }, // Float64 -> should promote to Float64
        ];

        let schema = infer_arrow_schema_from_documents(&docs, None).unwrap();
        let field = schema.field_with_name("value").unwrap();
        assert_eq!(field.data_type(), &DataType::Float64);
    }

    #[test]
    fn test_type_unification_to_string_fallback() {
        let docs = vec![
            doc! { "value": 10_i32 }, // Int32
            doc! { "value": "text" }, // String -> should fallback to String
        ];

        let schema = infer_arrow_schema_from_documents(&docs, None).unwrap();
        let field = schema.field_with_name("value").unwrap();
        assert_eq!(field.data_type(), &DataType::Utf8);
    }

    #[test]
    fn test_null_unification() {
        let docs = vec![
            doc! { "value": Bson::Null }, // Null
            doc! { "value": "text" },     // String -> should be String
        ];

        let schema = infer_arrow_schema_from_documents(&docs, None).unwrap();
        let field = schema.field_with_name("value").unwrap();
        assert_eq!(field.data_type(), &DataType::Utf8);
    }

    #[test]
    fn test_only_null_values() {
        let docs = vec![doc! { "value": Bson::Null }, doc! { "value": Bson::Null }];

        let schema = infer_arrow_schema_from_documents(&docs, None).unwrap();
        let field = schema.field_with_name("value").unwrap();
        assert_eq!(field.data_type(), &DataType::Null);
    }

    #[test]
    fn test_missing_fields_across_documents() {
        let docs = vec![
            doc! { "name": "Alice", "age": 30_i32 },
            doc! { "name": "Bob", "city": "NYC" },
            doc! { "age": 25_i32, "country": "US" },
        ];

        let schema = infer_arrow_schema_from_documents(&docs, None).unwrap();

        // Should have all unique fields
        assert_eq!(schema.fields().len(), 4);

        let field_names: std::collections::HashSet<&str> =
            schema.fields().iter().map(|f| f.name().as_str()).collect();

        assert!(field_names.contains("name"));
        assert!(field_names.contains("age"));
        assert!(field_names.contains("city"));
        assert!(field_names.contains("country"));

        // All fields should be nullable since they're missing in some docs
        for field in schema.fields() {
            assert!(field.is_nullable());
        }
    }

    #[test]
    fn test_fields_are_sorted() {
        let doc = doc! {
            "zebra": "striped",
            "apple": "red",
            "monkey": "brown",
            "banana": "yellow"
        };
        let docs = vec![doc];

        let schema = infer_arrow_schema_from_documents(&docs, None).unwrap();

        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(field_names, vec!["apple", "banana", "monkey", "zebra"]);
    }

    #[test]
    fn test_large_document_set() {
        let mut docs = Vec::new();

        // Generate 100 documents with varying schemas
        for i in 0..100 {
            let mut doc = Document::new();
            doc.insert("id", i);
            doc.insert("name", format!("user_{}", i));

            // Add optional fields for some documents
            if i % 2 == 0 {
                doc.insert("age", 20 + i % 50);
            }
            if i % 3 == 0 {
                doc.insert("city", "NYC");
            }
            if i % 5 == 0 {
                doc.insert("score", (i as f64) / 10.0);
            }

            docs.push(doc);
        }

        let schema = infer_arrow_schema_from_documents(&docs, None).unwrap();

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
}
