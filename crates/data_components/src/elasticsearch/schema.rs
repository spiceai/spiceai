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
use elasticsearch_datafusion_filter::{EsFilterSchema, EsMappingField};

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

/// Convert an Elasticsearch index mapping to an [`EsFilterSchema`], using the real per-field
/// `type` (and any `keyword`-typed multi-field sibling) rather than the derived Arrow schema —
/// so `keyword` fields and `text` fields with a `keyword` sibling are filterable, not just
/// numeric/boolean columns. Field names are flattened the same way as [`mapping_to_schema`] so
/// they line up with the Arrow schema the filters are expressed against.
#[must_use]
#[expect(clippy::implicit_hasher)]
pub fn mapping_to_filter_schema(properties: &HashMap<String, FieldMapping>) -> EsFilterSchema {
    let mut fields = HashMap::new();
    collect_mapping_fields(properties, "", &mut fields);
    EsFilterSchema::from_mapping(fields.iter().map(|(name, info)| (name.as_str(), info)))
}

fn collect_mapping_fields(
    properties: &HashMap<String, FieldMapping>,
    prefix: &str,
    fields: &mut HashMap<String, EsMappingField>,
) {
    for (name, mapping) in properties {
        let full_name = if prefix.is_empty() {
            name.clone()
        } else {
            format!("{prefix}.{name}")
        };

        // Recurse into nested objects, mirroring `collect_fields`; a `nested` object is treated
        // as an opaque JSON string by `mapping_to_schema` and is not filterable here either.
        if let Some(sub_props) = &mapping.properties {
            if mapping.field_type.as_deref() != Some("nested") {
                collect_mapping_fields(sub_props, &full_name, fields);
            }
            continue;
        }

        let Some(field_type) = &mapping.field_type else {
            continue;
        };
        let keyword_sibling = mapping.fields.as_ref().and_then(|subfields| {
            subfields.iter().find(|(_, sub_mapping)| {
                matches!(
                    sub_mapping.field_type.as_deref(),
                    Some("keyword" | "constant_keyword" | "wildcard")
                )
            })
        });
        let keyword_subfield = keyword_sibling.map(|(sub_name, _)| sub_name.clone());
        // Whichever field the pushdown will actually query for an exact-value predicate — the
        // `keyword` sibling for `text`, or the field itself for `keyword`/`wildcard`/
        // `constant_keyword` — carries the `ignore_above` that truncates it.
        let is_keyword_family = matches!(
            field_type.as_str(),
            "keyword" | "wildcard" | "constant_keyword"
        );
        let keyword_ignore_above = if is_keyword_family {
            mapping.ignore_above
        } else {
            keyword_sibling.and_then(|(_, sub_mapping)| sub_mapping.ignore_above)
        }
        .map(|n| n as usize);
        // A `null_value` on either the field itself or its keyword sibling makes an `exists`
        // pre-filter unsafe for IS [NOT] NULL (see `EsFilterSchema::from_mapping`).
        let has_null_value = mapping.null_value.is_some()
            || keyword_sibling.is_some_and(|(_, sub_mapping)| sub_mapping.null_value.is_some());
        // `index`/`doc_values` are read from whichever field the pushdown will actually query —
        // the keyword sibling for `text`, or the field itself otherwise — same target as
        // `keyword_ignore_above` above. Elasticsearch defaults both to `true` when absent.
        let (indexed, has_doc_values) = if is_keyword_family {
            (mapping.index, mapping.doc_values)
        } else if let Some((_, sub_mapping)) = keyword_sibling {
            (sub_mapping.index, sub_mapping.doc_values)
        } else {
            (mapping.index, mapping.doc_values)
        };
        fields.insert(
            full_name,
            EsMappingField {
                field_type: field_type.clone(),
                keyword_subfield,
                keyword_ignore_above,
                has_null_value,
                indexed: indexed.unwrap_or(true),
                has_doc_values: has_doc_values.unwrap_or(true),
            },
        );
    }
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
                ..Default::default()
            },
        );
        properties.insert(
            "count".to_string(),
            FieldMapping {
                field_type: Some("integer".to_string()),
                ..Default::default()
            },
        );
        properties.insert(
            "embedding".to_string(),
            FieldMapping {
                field_type: Some("dense_vector".to_string()),
                dims: Some(384),
                similarity: Some("cosine".to_string()),
                ..Default::default()
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
                ..Default::default()
            },
        );

        let schema = mapping_to_schema(&properties);
        let big = schema.field_with_name("big").expect("big field");
        assert_eq!(big.data_type(), &DataType::UInt64);
    }

    #[test]
    fn filter_schema_carries_keyword_ignore_above_and_null_value() {
        use datafusion::logical_expr::TableProviderFilterPushDown;
        use datafusion::prelude::{col, lit};
        use elasticsearch_datafusion_filter::classify_filter;

        let mut title_fields = HashMap::new();
        title_fields.insert(
            "keyword".to_string(),
            FieldMapping {
                field_type: Some("keyword".to_string()),
                ignore_above: Some(64),
                ..Default::default()
            },
        );
        let mut properties = HashMap::new();
        properties.insert(
            "title".to_string(),
            FieldMapping {
                field_type: Some("text".to_string()),
                fields: Some(title_fields),
                ..Default::default()
            },
        );
        properties.insert(
            "code".to_string(),
            FieldMapping {
                field_type: Some("keyword".to_string()),
                null_value: Some(serde_json::json!("UNKNOWN")),
                ..Default::default()
            },
        );

        let filter_schema = mapping_to_filter_schema(&properties);

        // A literal past the real `ignore_above: 64` must not be pushed as a superset — the
        // matching row could be entirely absent from the `.keyword` sub-field.
        let long_literal = "x".repeat(65);
        assert_eq!(
            classify_filter(&filter_schema, &col("title").eq(lit(long_literal))),
            TableProviderFilterPushDown::Unsupported
        );
        assert_eq!(
            classify_filter(&filter_schema, &col("title").eq(lit("short"))),
            TableProviderFilterPushDown::Inexact
        );

        // `null_value` makes `exists` untrustworthy for IS [NOT] NULL. Equality is unaffected by
        // `null_value`, but a mapping-derived field's cardinality is never confirmed scalar (see
        // `EsFilterSchema::is_confirmed_scalar`), so it is capped to `Inexact` regardless.
        assert_eq!(
            classify_filter(&filter_schema, &col("code").is_null()),
            TableProviderFilterPushDown::Unsupported
        );
        assert_eq!(
            classify_filter(&filter_schema, &col("code").eq(lit("open"))),
            TableProviderFilterPushDown::Inexact
        );
    }

    #[test]
    fn filter_schema_honors_index_and_doc_values() {
        use datafusion::logical_expr::TableProviderFilterPushDown;
        use datafusion::prelude::{col, lit};
        use elasticsearch_datafusion_filter::classify_filter;

        let mut properties = HashMap::new();
        properties.insert(
            "internal".to_string(),
            FieldMapping {
                field_type: Some("keyword".to_string()),
                index: Some(false),
                ..Default::default()
            },
        );
        properties.insert(
            "unsorted".to_string(),
            FieldMapping {
                field_type: Some("long".to_string()),
                doc_values: Some(false),
                ..Default::default()
            },
        );

        let filter_schema = mapping_to_filter_schema(&properties);

        // `index: false` means Elasticsearch cannot search the field at all.
        assert_eq!(
            classify_filter(&filter_schema, &col("internal").eq(lit("x"))),
            TableProviderFilterPushDown::Unsupported
        );
        // `doc_values: false` rules out a range clause, but not equality (`term` doesn't need
        // doc values).
        assert_eq!(
            classify_filter(&filter_schema, &col("unsorted").gt(lit(5_i64))),
            TableProviderFilterPushDown::Unsupported
        );
        assert_eq!(
            classify_filter(&filter_schema, &col("unsorted").eq(lit(5_i64))),
            TableProviderFilterPushDown::Inexact
        );
    }
}
