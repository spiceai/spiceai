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

use std::sync::Arc;

use super::{
    error::{IcebergResponseError, InternalServerErrorCode},
    namespace::{Namespace, NamespacePath},
};
use crate::datafusion::is_spice_internal_schema;
use crate::datafusion::request_context_extension::get_current_datafusion;
use arrow::datatypes::{DataType, Field, Fields, Schema as ArrowSchema};
use axum::{
    Json,
    extract::Path,
    http::status,
    response::{IntoResponse, Response},
};
use datafusion::sql::TableReference;
use iceberg::{
    arrow::arrow_schema_to_schema,
    spec::{PartitionSpec, Schema, SortOrder},
};
use runtime_request_context::{AsyncMarker, RequestContext};
use serde::{Serialize, Serializer};
use uuid::Uuid;

const PARQUET_FIELD_ID_META_KEY: &str = "PARQUET:field_id";
const MAX_SCHEMA_RECURSION_DEPTH: usize = 10;

/// Check if a table exists.
///
/// This endpoint returns a 200 OK response if the table exists, otherwise it returns a 404 Not Found response.
#[cfg_attr(feature = "openapi", utoipa::path(
    head,
    path = "/v1/iceberg/namespaces/{namespace}/tables/{table}",
    operation_id = "head_table",
    tag = "Iceberg",
    responses(
        (status = 200, description = "Table exists"),
        (status = 404, description = "Table does not exist")
    )
))]
pub(crate) async fn head(Path((namespace, table)): Path<(NamespacePath, String)>) -> Response {
    let context = RequestContext::current(AsyncMarker::new().await);
    let df = get_current_datafusion(&context);

    let namespace = Namespace::from(namespace);
    let Some(table_reference) = table_reference(&namespace, &table) else {
        return status::StatusCode::NOT_FOUND.into_response();
    };

    match df.get_table(&table_reference).await {
        Some(_) => status::StatusCode::OK.into_response(),
        None => status::StatusCode::NOT_FOUND.into_response(),
    }
}

#[derive(Debug, Serialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
struct LoadTableResponse {
    metadata: TableMetadata,
}

#[derive(Debug)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
enum TableFormatVersion {
    V2,
}

impl Serialize for TableFormatVersion {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            TableFormatVersion::V2 => serializer.serialize_u8(2),
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "kebab-case")]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
struct TableMetadata {
    format_version: TableFormatVersion,
    #[cfg_attr(feature = "openapi", schema(value_type=String, example="2b9da507-2c07-4bb3-9f0b-8df66a5e9e53"))]
    table_uuid: Uuid,
    location: String,

    /// Iceberg schemas, see `<https://apache.github.io/iceberg/spec/#schemas>`.
    #[cfg_attr(feature = "openapi", schema(value_type=Type::Object))]
    schemas: Vec<Schema>,

    // The following fields are part of the Iceberg Table Metadata V2 spec - but we don't do anything with them yet
    last_updated_ms: i64,
    last_column_id: u32,
    last_sequence_number: u64,
    current_schema_id: u32,
    #[cfg_attr(feature = "openapi", schema(value_type=Type::Object))]
    partition_specs: Vec<PartitionSpec>,
    default_spec_id: u32,
    last_partition_id: u32,
    #[cfg_attr(feature = "openapi", schema(value_type=Type::Object))]
    sort_orders: Vec<SortOrder>,
    default_sort_order_id: u32,
}

/// Get a table.
///
/// This endpoint returns the table if it exists, otherwise it returns a 404 Not Found response.
#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/v1/iceberg/namespaces/{namespace}/tables/{table}",
    operation_id = "get_table",
    tag = "Iceberg",
    params(
        ("namespace" = String, Path, description = "The namespace of the table."),
        ("table" = String, Path, description = "The name of the table.")
    ),
    responses(
        (status = 200, description = "Table exists", body = LoadTableResponse),
        (status = 404, description = "Table does not exist"),
        (status = 500, description = "An internal server error occurred while getting the table", content((
            IcebergResponseError = "application/json",
            example = json!({
                "error": {
                    "message": "Request failed. An internal server error occurred while getting the table.",
                    "r#type": "InternalServerError",
                    "code": 500
                }
            })
        )))
    )
))]
#[expect(clippy::cast_possible_truncation)]
pub(crate) async fn get(Path((namespace, table)): Path<(NamespacePath, String)>) -> Response {
    let context = RequestContext::current(AsyncMarker::new().await);
    let df = get_current_datafusion(&context);

    let namespace = Namespace::from(namespace);
    let Some(table_reference) = table_reference(&namespace, &table) else {
        return status::StatusCode::NOT_FOUND.into_response();
    };

    let Some(table) = df.get_table(&table_reference).await else {
        return status::StatusCode::NOT_FOUND.into_response();
    };

    let arrow_schema = table.schema();
    let arrow_schema = assign_field_ids(&arrow_schema);
    let iceberg_schema = match arrow_schema_to_schema(&arrow_schema) {
        Ok(schema) => schema,
        Err(e) => {
            tracing::debug!(
                "Error converting arrow schema to iceberg schema for {table_reference}: {e}"
            );
            return IcebergResponseError::internal(InternalServerErrorCode::InvalidSchema)
                .into_response();
        }
    };

    let last_updated_ms = chrono::Utc::now().timestamp_millis();

    let partition_specs = if let Ok(partition_spec) = PartitionSpec::builder(iceberg_schema.clone())
        .with_spec_id(0)
        .build()
    {
        vec![partition_spec]
    } else {
        vec![]
    };

    let metadata = TableMetadata {
        format_version: TableFormatVersion::V2,
        table_uuid: Uuid::new_v4(),
        location: format!("spice.ai/{table_reference}"),
        schemas: vec![iceberg_schema],
        last_column_id: arrow_schema.fields.len() as u32,
        last_updated_ms,
        last_sequence_number: 0,
        current_schema_id: 0,
        partition_specs,
        default_spec_id: 0,
        last_partition_id: 1000,
        sort_orders: vec![SortOrder::unsorted_order()],
        default_sort_order_id: 0,
    };

    let response = LoadTableResponse { metadata };

    (status::StatusCode::OK, Json(response)).into_response()
}

fn table_reference(namespace: &Namespace, table: &str) -> Option<TableReference> {
    if namespace.parts.len() != 2 {
        return None;
    }

    let catalog = namespace.parts[0].as_str();
    let schema = namespace.parts[1].as_str();

    if is_spice_internal_schema(catalog, schema) {
        return None;
    }

    Some(TableReference::full(catalog, schema, table))
}

struct DepthExceeded;

/// Iceberg requires field IDs to be set for all fields, including nested fields in Struct, List, and Map types.
/// The iceberg-rust crate expects them to be set in the `PARQUET:field_id` metadata key.
fn assign_field_ids(schema: &ArrowSchema) -> ArrowSchema {
    if let Ok(new_schema) = try_assign_field_ids(schema) {
        new_schema
    } else {
        tracing::warn!(
            "Schema recursion depth limit ({MAX_SCHEMA_RECURSION_DEPTH}) exceeded, returning original schema"
        );
        schema.clone()
    }
}

fn try_assign_field_ids(schema: &ArrowSchema) -> Result<ArrowSchema, DepthExceeded> {
    let mut counter: i32 = 0;
    let fields: Vec<Arc<Field>> = schema
        .fields
        .iter()
        .map(|f| Ok(Arc::new(assign_field_id_recursive(f, &mut counter, 0)?)))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(ArrowSchema::new(fields))
}

fn assign_field_id_recursive(
    field: &Field,
    counter: &mut i32,
    depth: usize,
) -> Result<Field, DepthExceeded> {
    if depth > MAX_SCHEMA_RECURSION_DEPTH {
        return Err(DepthExceeded);
    }

    let id = *counter;
    *counter += 1;

    let new_data_type = match field.data_type() {
        DataType::Struct(fields) => {
            let new_fields: Vec<Arc<Field>> = fields
                .iter()
                .map(|f| Ok(Arc::new(assign_field_id_recursive(f, counter, depth + 1)?)))
                .collect::<Result<Vec<_>, _>>()?;
            DataType::Struct(Fields::from(new_fields))
        }
        DataType::List(element_field) => DataType::List(Arc::new(assign_field_id_recursive(
            element_field,
            counter,
            depth + 1,
        )?)),
        DataType::LargeList(element_field) => DataType::LargeList(Arc::new(
            assign_field_id_recursive(element_field, counter, depth + 1)?,
        )),
        DataType::FixedSizeList(element_field, size) => DataType::FixedSizeList(
            Arc::new(assign_field_id_recursive(
                element_field,
                counter,
                depth + 1,
            )?),
            *size,
        ),
        DataType::Map(struct_field, keys_sorted) => DataType::Map(
            Arc::new(assign_field_id_recursive(struct_field, counter, depth + 1)?),
            *keys_sorted,
        ),
        other => other.clone(),
    };

    // Preserve existing metadata and add/update the field ID
    let mut metadata = field.metadata().clone();
    metadata.insert(PARQUET_FIELD_ID_META_KEY.to_string(), id.to_string());

    Ok(Field::new(field.name(), new_data_type, field.is_nullable()).with_metadata(metadata))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use iceberg::arrow::arrow_schema_to_schema;

    fn get_field_id(field: &Field) -> Option<i32> {
        field
            .metadata()
            .get(PARQUET_FIELD_ID_META_KEY)
            .and_then(|v| v.parse().ok())
    }

    fn create_nested_schema(depth: usize) -> ArrowSchema {
        let mut current_type = DataType::Int32;
        for i in (0..depth).rev() {
            current_type = DataType::Struct(Fields::from(vec![Field::new(
                format!("level_{i}"),
                current_type,
                false,
            )]));
        }
        ArrowSchema::new(vec![Field::new("root", current_type, false)])
    }

    #[test]
    fn test_assign_field_ids_primitive_fields() {
        let schema = ArrowSchema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, true),
            Field::new("c", DataType::Float64, false),
        ]);

        let result = assign_field_ids(&schema);

        assert_eq!(result.fields.len(), 3);
        assert_eq!(get_field_id(&result.fields[0]), Some(0));
        assert_eq!(get_field_id(&result.fields[1]), Some(1));
        assert_eq!(get_field_id(&result.fields[2]), Some(2));

        // Verify iceberg conversion succeeds
        arrow_schema_to_schema(&result).expect("Should convert to iceberg schema");
    }

    #[test]
    fn test_assign_field_ids_nested_struct() {
        let inner_fields = Fields::from(vec![
            Field::new("inner_a", DataType::Int32, false),
            Field::new("inner_b", DataType::Utf8, true),
        ]);
        let schema = ArrowSchema::new(vec![
            Field::new("outer", DataType::Struct(inner_fields), false),
            Field::new("other", DataType::Int64, false),
        ]);

        let result = assign_field_ids(&schema);

        // outer gets id 0, inner_a gets id 1, inner_b gets id 2, other gets id 3
        assert_eq!(get_field_id(&result.fields[0]), Some(0));
        if let DataType::Struct(inner) = result.fields[0].data_type() {
            assert_eq!(get_field_id(&inner[0]), Some(1));
            assert_eq!(get_field_id(&inner[1]), Some(2));
        } else {
            panic!("Expected struct type");
        }
        assert_eq!(get_field_id(&result.fields[1]), Some(3));

        // Verify iceberg conversion succeeds
        arrow_schema_to_schema(&result).expect("Should convert to iceberg schema");
    }

    #[test]
    fn test_assign_field_ids_list() {
        let schema = ArrowSchema::new(vec![
            Field::new(
                "list_col",
                DataType::List(Arc::new(Field::new("element", DataType::Int32, false))),
                true,
            ),
            Field::new("other", DataType::Utf8, false),
        ]);

        let result = assign_field_ids(&schema);

        // list_col gets id 0, element gets id 1, other gets id 2
        assert_eq!(get_field_id(&result.fields[0]), Some(0));
        if let DataType::List(element_field) = result.fields[0].data_type() {
            assert_eq!(get_field_id(element_field), Some(1));
        } else {
            panic!("Expected list type");
        }
        assert_eq!(get_field_id(&result.fields[1]), Some(2));

        // Verify iceberg conversion succeeds
        arrow_schema_to_schema(&result).expect("Should convert to iceberg schema");
    }

    #[test]
    fn test_assign_field_ids_map() {
        let map_field = Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Int32, true),
            ])),
            false,
        );
        let schema = ArrowSchema::new(vec![Field::new(
            "map_col",
            DataType::Map(Arc::new(map_field), false),
            true,
        )]);

        let result = assign_field_ids(&schema);

        // map_col gets id 0, entries (struct) gets id 1, key gets id 2, value gets id 3
        assert_eq!(get_field_id(&result.fields[0]), Some(0));
        if let DataType::Map(struct_field, _) = result.fields[0].data_type() {
            assert_eq!(get_field_id(struct_field), Some(1));
            if let DataType::Struct(kv_fields) = struct_field.data_type() {
                assert_eq!(get_field_id(&kv_fields[0]), Some(2));
                assert_eq!(get_field_id(&kv_fields[1]), Some(3));
            } else {
                panic!("Expected struct type inside map");
            }
        } else {
            panic!("Expected map type");
        }

        // Verify iceberg conversion succeeds
        arrow_schema_to_schema(&result).expect("Should convert to iceberg schema");
    }

    #[test]
    fn test_assign_field_ids_deeply_nested() {
        // List of structs containing lists
        let inner_list = Field::new(
            "inner_list",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, false))),
            true,
        );
        let struct_fields =
            Fields::from(vec![Field::new("name", DataType::Utf8, false), inner_list]);
        let schema = ArrowSchema::new(vec![Field::new(
            "outer_list",
            DataType::List(Arc::new(Field::new(
                "element",
                DataType::Struct(struct_fields),
                false,
            ))),
            true,
        )]);

        let result = assign_field_ids(&schema);

        // Verify all fields have IDs assigned and iceberg conversion succeeds
        arrow_schema_to_schema(&result).expect("Should convert deeply nested schema to iceberg");
    }

    #[test]
    fn test_assign_field_ids_large_list() {
        let schema = ArrowSchema::new(vec![Field::new(
            "large_list_col",
            DataType::LargeList(Arc::new(Field::new("element", DataType::Utf8, false))),
            true,
        )]);

        let result = assign_field_ids(&schema);

        assert_eq!(get_field_id(&result.fields[0]), Some(0));
        if let DataType::LargeList(element_field) = result.fields[0].data_type() {
            assert_eq!(get_field_id(element_field), Some(1));
        } else {
            panic!("Expected large list type");
        }

        // Verify iceberg conversion succeeds
        arrow_schema_to_schema(&result).expect("Should convert to iceberg schema");
    }

    #[test]
    fn test_assign_field_ids_fixed_size_list() {
        let schema = ArrowSchema::new(vec![Field::new(
            "fixed_list_col",
            DataType::FixedSizeList(
                Arc::new(Field::new("element", DataType::Float32, false)),
                10,
            ),
            true,
        )]);

        let result = assign_field_ids(&schema);

        assert_eq!(get_field_id(&result.fields[0]), Some(0));
        if let DataType::FixedSizeList(element_field, size) = result.fields[0].data_type() {
            assert_eq!(get_field_id(element_field), Some(1));
            assert_eq!(*size, 10);
        } else {
            panic!("Expected fixed size list type");
        }

        // Verify iceberg conversion succeeds
        arrow_schema_to_schema(&result).expect("Should convert to iceberg schema");
    }

    #[test]
    fn test_assign_field_ids_preserves_existing_metadata() {
        let mut existing_metadata = HashMap::new();
        existing_metadata.insert("custom_key".to_string(), "custom_value".to_string());
        existing_metadata.insert("another_key".to_string(), "another_value".to_string());

        let field_with_metadata =
            Field::new("a", DataType::Int32, false).with_metadata(existing_metadata);
        let schema = ArrowSchema::new(vec![field_with_metadata]);

        let result = assign_field_ids(&schema);

        let result_field = &result.fields[0];
        let metadata = result_field.metadata();

        // Verify field ID was added
        assert_eq!(get_field_id(result_field), Some(0));

        // Verify existing metadata was preserved
        assert_eq!(
            metadata.get("custom_key"),
            Some(&"custom_value".to_string())
        );
        assert_eq!(
            metadata.get("another_key"),
            Some(&"another_value".to_string())
        );

        // Verify we have all three keys
        assert_eq!(metadata.len(), 3);
    }

    #[test]
    fn test_assign_field_ids_preserves_nested_metadata() {
        let mut inner_metadata = HashMap::new();
        inner_metadata.insert("inner_key".to_string(), "inner_value".to_string());

        let inner_field = Field::new("inner", DataType::Utf8, false).with_metadata(inner_metadata);
        let schema = ArrowSchema::new(vec![Field::new(
            "outer",
            DataType::Struct(Fields::from(vec![inner_field])),
            false,
        )]);

        let result = assign_field_ids(&schema);

        if let DataType::Struct(inner_fields) = result.fields[0].data_type() {
            let inner_metadata = inner_fields[0].metadata();
            assert_eq!(get_field_id(&inner_fields[0]), Some(1));
            assert_eq!(
                inner_metadata.get("inner_key"),
                Some(&"inner_value".to_string())
            );
        } else {
            panic!("Expected struct type");
        }
    }

    #[test]
    fn test_assign_field_ids_at_max_depth() {
        // Depth of 10 should work (depth starts at 0, so levels 0-10 are allowed)
        let schema = create_nested_schema(10);
        let result = assign_field_ids(&schema);

        // Should have field IDs assigned (not return original schema)
        assert_eq!(get_field_id(&result.fields[0]), Some(0));

        // Verify we can traverse and find field IDs at each level
        let mut current_field = &result.fields[0];
        for expected_id in 0..10 {
            assert_eq!(
                get_field_id(current_field),
                Some(expected_id),
                "Field at depth {expected_id} should have ID {expected_id}"
            );
            if let DataType::Struct(fields) = current_field.data_type() {
                current_field = &fields[0];
            }
        }
    }

    #[test]
    fn test_assign_field_ids_exceeds_max_depth() {
        // Depth of 12 exceeds the limit of 10
        let schema = create_nested_schema(12);
        let result = assign_field_ids(&schema);

        // Should return original schema (no field IDs assigned)
        assert_eq!(
            get_field_id(&result.fields[0]),
            None,
            "Original schema should be returned when depth limit exceeded"
        );

        // Verify the schema structure is preserved (root -> level_0 -> level_1 -> ... -> level_11)
        assert_eq!(result.fields[0].name(), "root");
        let mut current_field = &result.fields[0];
        for i in 0..12 {
            if let DataType::Struct(fields) = current_field.data_type() {
                current_field = &fields[0];
                assert_eq!(current_field.name(), format!("level_{i}").as_str());
            }
        }
    }

    #[test]
    fn test_assign_field_ids_exactly_at_limit_boundary() {
        // Test depth = 11 (just over the limit of 10)
        let schema = create_nested_schema(11);
        let result = assign_field_ids(&schema);

        // Should return original schema
        assert_eq!(
            get_field_id(&result.fields[0]),
            None,
            "Original schema should be returned when depth is 11 (exceeds limit of 10)"
        );
    }

    #[test]
    fn test_assign_field_ids_nested_list_depth_limit() {
        // Create deeply nested lists that exceed the depth limit
        let mut current_type = DataType::Int32;
        for _ in 0..12 {
            current_type =
                DataType::List(Arc::new(Field::new("element", current_type.clone(), false)));
        }
        let schema = ArrowSchema::new(vec![Field::new("nested_lists", current_type, false)]);

        let result = assign_field_ids(&schema);

        // Should return original schema (no field IDs assigned)
        assert_eq!(
            get_field_id(&result.fields[0]),
            None,
            "Original schema should be returned for deeply nested lists"
        );
    }
}
