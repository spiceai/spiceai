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

//! Convert Elasticsearch index mappings to Arrow schemas.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use elasticsearch::FieldMapping;

/// Convert an Elasticsearch index mapping to an Arrow [`Schema`].
///
/// Flattens nested objects using dot-separated field names (e.g. `address.city`).
/// `dense_vector` fields become `FixedSizeList<Float32>`.
#[must_use]
#[expect(clippy::implicit_hasher)]
pub fn mapping_to_schema(properties: &HashMap<String, FieldMapping>) -> SchemaRef {
    let mut fields = Vec::new();
    collect_fields(properties, "", &mut fields);
    Arc::new(Schema::new(fields))
}

fn collect_fields(
    properties: &HashMap<String, FieldMapping>,
    prefix: &str,
    fields: &mut Vec<Field>,
) {
    let mut entries: Vec<_> = properties.iter().collect();
    entries.sort_by_key(|(name, _)| (*name).clone());

    for (name, mapping) in entries {
        let full_name = if prefix.is_empty() {
            name.clone()
        } else {
            format!("{prefix}.{name}")
        };

        // Recurse into nested objects.
        if let Some(sub_props) = &mapping.properties {
            if mapping.field_type.as_deref() == Some("nested") {
                // Nested objects are complex; for now treat as JSON string.
                fields.push(Field::new(&full_name, DataType::Utf8, true));
            } else {
                collect_fields(sub_props, &full_name, fields);
            }
            continue;
        }

        let data_type = es_type_to_arrow(mapping);
        fields.push(Field::new(&full_name, data_type, true));
    }
}

/// Map an Elasticsearch field type to an Arrow [`DataType`].
#[expect(clippy::match_same_arms)]
fn es_type_to_arrow(mapping: &FieldMapping) -> DataType {
    match mapping.field_type.as_deref() {
        Some("text" | "keyword" | "wildcard" | "constant_keyword" | "match_only_text") => {
            DataType::Utf8
        }
        Some("long") => DataType::Int64,
        // unsigned_long covers the full u64 range; Int64 would silently overflow values > i64::MAX.
        Some("unsigned_long") => DataType::UInt64,
        Some("integer") => DataType::Int32,
        Some("short") => DataType::Int16,
        Some("byte") => DataType::Int8,
        Some("double") => DataType::Float64,
        Some("float" | "half_float" | "scaled_float") => DataType::Float32,
        Some("boolean") => DataType::Boolean,
        Some("date" | "date_nanos") => DataType::Utf8, // Keep as string; ES dates are flexible.
        Some("binary") => DataType::Utf8, // ES binary fields are base64-encoded strings in JSON.
        Some("ip") => DataType::Utf8,
        Some("dense_vector") => {
            // dims is required for dense_vector in Elasticsearch. If missing or out of i32 range,
            // fall back to Utf8 rather than guessing a wrong dimension.
            if let Some(dims) = mapping
                .dims
                .and_then(|d| i32::try_from(d).ok())
                .filter(|&d| d > 0)
            {
                DataType::FixedSizeList(
                    Arc::new(Field::new("item", DataType::Float32, false)),
                    dims,
                )
            } else {
                DataType::Utf8 // Unknown dims: store raw JSON representation.
            }
        }
        Some("object") => DataType::Utf8, // Serialized JSON
        _ => DataType::Utf8,              // Fallback
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_mapping_conversion() {
        let mut properties = HashMap::new();
        properties.insert(
            "title".to_string(),
            FieldMapping {
                field_type: Some("text".to_string()),
                properties: None,
                dims: None,
                similarity: None,
            },
        );
        properties.insert(
            "count".to_string(),
            FieldMapping {
                field_type: Some("integer".to_string()),
                properties: None,
                dims: None,
                similarity: None,
            },
        );
        properties.insert(
            "embedding".to_string(),
            FieldMapping {
                field_type: Some("dense_vector".to_string()),
                properties: None,
                dims: Some(384),
                similarity: Some("cosine".to_string()),
            },
        );

        let schema = mapping_to_schema(&properties);
        assert_eq!(schema.fields().len(), 3);

        let count_field = schema.field_with_name("count").expect("count field");
        assert_eq!(count_field.data_type(), &DataType::Int32);

        let embed_field = schema
            .field_with_name("embedding")
            .expect("embedding field");
        assert!(matches!(
            embed_field.data_type(),
            DataType::FixedSizeList(_, 384)
        ));
    }

    #[test]
    fn test_unsigned_long_mapping() {
        let mut properties = HashMap::new();
        properties.insert(
            "big".to_string(),
            FieldMapping {
                field_type: Some("unsigned_long".to_string()),
                properties: None,
                dims: None,
                similarity: None,
            },
        );

        let schema = mapping_to_schema(&properties);
        let big = schema.field_with_name("big").expect("big field");
        assert_eq!(big.data_type(), &DataType::UInt64);
    }
}
