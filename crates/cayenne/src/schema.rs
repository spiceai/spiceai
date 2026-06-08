/*
Copyright 2026, Spice AI, Inc.

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

//! Arrow schema transformations for Vortex compatibility.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion_table_providers::UnsupportedTypeAction;

fn is_vortex_supported_type(data_type: &DataType) -> bool {
    !matches!(
        data_type,
        DataType::Interval(_) | DataType::Duration(_) | DataType::FixedSizeBinary(_)
    )
}

fn unsupported_field_detail(path: &str, data_type: &DataType) -> String {
    format!("'{path}' (type: {data_type:?})")
}

fn transform_field_for_vortex(
    field: &Field,
    path: &str,
    unsupported_type_action: UnsupportedTypeAction,
    top_level: bool,
    unsupported_fields: &mut Vec<String>,
) -> Option<Arc<Field>> {
    let data_type = transform_data_type_for_vortex(
        field.data_type(),
        path,
        unsupported_type_action,
        top_level,
        unsupported_fields,
    )?;
    Some(Arc::new(field.clone().with_data_type(data_type)))
}

fn transform_data_type_for_vortex(
    data_type: &DataType,
    path: &str,
    unsupported_type_action: UnsupportedTypeAction,
    top_level: bool,
    unsupported_fields: &mut Vec<String>,
) -> Option<DataType> {
    if matches!(data_type, DataType::Float16) {
        tracing::debug!("Converting Float16 field '{path}' to Float32 for Vortex compatibility");
        return Some(DataType::Float32);
    }

    if let DataType::Timestamp(unit, tz) = data_type
        && !matches!(unit, TimeUnit::Microsecond)
    {
        tracing::debug!(
            "Converting timestamp field '{path}' from {:?} to Microsecond for Vortex compatibility",
            unit
        );
        return Some(DataType::Timestamp(TimeUnit::Microsecond, tz.clone()));
    }

    if !is_vortex_supported_type(data_type) {
        return handle_unsupported_type(
            data_type,
            path,
            unsupported_type_action,
            top_level,
            unsupported_fields,
        );
    }

    match data_type {
        DataType::Dictionary(key_type, value_type) => {
            let value_type = transform_data_type_for_vortex(
                value_type,
                path,
                unsupported_type_action,
                false,
                unsupported_fields,
            )?;
            Some(DataType::Dictionary(key_type.clone(), Box::new(value_type)))
        }
        DataType::List(field) => Some(DataType::List(transform_nested_field(
            field,
            &format!("{path}[]"),
            unsupported_type_action,
            unsupported_fields,
        ))),
        DataType::LargeList(field) => Some(DataType::LargeList(transform_nested_field(
            field,
            &format!("{path}[]"),
            unsupported_type_action,
            unsupported_fields,
        ))),
        DataType::FixedSizeList(field, size) => Some(DataType::FixedSizeList(
            transform_nested_field(
                field,
                &format!("{path}[]"),
                unsupported_type_action,
                unsupported_fields,
            ),
            *size,
        )),
        DataType::ListView(field) => Some(DataType::ListView(transform_nested_field(
            field,
            &format!("{path}[]"),
            unsupported_type_action,
            unsupported_fields,
        ))),
        DataType::LargeListView(field) => Some(DataType::LargeListView(transform_nested_field(
            field,
            &format!("{path}[]"),
            unsupported_type_action,
            unsupported_fields,
        ))),
        DataType::Map(field, sorted) => Some(DataType::Map(
            transform_nested_field(field, path, unsupported_type_action, unsupported_fields),
            *sorted,
        )),
        DataType::Struct(fields) => {
            let fields: Vec<Field> = fields
                .iter()
                .map(|field| {
                    transform_nested_field(
                        field,
                        &format!("{path}.{}", field.name()),
                        unsupported_type_action,
                        unsupported_fields,
                    )
                    .as_ref()
                    .clone()
                })
                .collect();
            Some(DataType::Struct(fields.into()))
        }
        DataType::Union(fields, mode) => Some(DataType::Union(
            fields
                .iter()
                .map(|(type_id, field)| {
                    (
                        type_id,
                        transform_nested_field(
                            field,
                            &format!("{path}.{}", field.name()),
                            unsupported_type_action,
                            unsupported_fields,
                        ),
                    )
                })
                .collect(),
            *mode,
        )),
        DataType::RunEndEncoded(run_ends, values) => Some(DataType::RunEndEncoded(
            Arc::clone(run_ends),
            transform_nested_field(values, path, unsupported_type_action, unsupported_fields),
        )),
        _ => Some(data_type.clone()),
    }
}

fn transform_nested_field(
    field: &Arc<Field>,
    path: &str,
    unsupported_type_action: UnsupportedTypeAction,
    unsupported_fields: &mut Vec<String>,
) -> Arc<Field> {
    transform_field_for_vortex(
        field,
        path,
        unsupported_type_action,
        false,
        unsupported_fields,
    )
    .unwrap_or_else(|| Arc::clone(field))
}

fn handle_unsupported_type(
    data_type: &DataType,
    path: &str,
    unsupported_type_action: UnsupportedTypeAction,
    top_level: bool,
    unsupported_fields: &mut Vec<String>,
) -> Option<DataType> {
    match (top_level, unsupported_type_action) {
        (true, UnsupportedTypeAction::String) => {
            tracing::warn!(
                "Converting unsupported type {:?} for field '{}' to Utf8.",
                data_type,
                path
            );
            Some(DataType::Utf8)
        }
        (true, UnsupportedTypeAction::Error) => {
            unsupported_fields.push(unsupported_field_detail(path, data_type));
            Some(data_type.clone())
        }
        (true, UnsupportedTypeAction::Ignore) => {
            tracing::warn!(
                "Ignoring unsupported type {:?} for field '{}'",
                data_type,
                path
            );
            None
        }
        (_, UnsupportedTypeAction::Warn) => {
            tracing::warn!(
                "Including unsupported type {:?} for field '{}' - insertion may fail",
                data_type,
                path
            );
            Some(data_type.clone())
        }
        (false, _) => {
            unsupported_fields.push(unsupported_field_detail(path, data_type));
            Some(data_type.clone())
        }
    }
}

/// Transform an Arrow schema for Vortex compatibility.
///
/// Always applies:
/// - `Float16` → `Float32`
/// - Non-microsecond `Timestamp` → `Timestamp(Microsecond, tz)`
///
/// Truly unsupported types (`Interval`, `Duration`, `FixedSizeBinary`) are
/// handled according to `unsupported_type_action` at the top level. Nested
/// unsupported types error unless the action is `warn`, because schema-only
/// string conversion or field removal would not preserve nested data correctly.
///
/// # Errors
///
/// Returns an error when `unsupported_type_action` is [`UnsupportedTypeAction::Error`]
/// and the schema contains unsupported types.
pub fn transform_schema_for_vortex(
    schema: &Schema,
    unsupported_type_action: UnsupportedTypeAction,
) -> DFResult<Schema> {
    let mut unsupported_fields = Vec::new();
    let mut transformed_fields = Vec::new();

    for field in schema.fields() {
        if let Some(field) = transform_field_for_vortex(
            field,
            field.name(),
            unsupported_type_action,
            true,
            &mut unsupported_fields,
        ) {
            transformed_fields.push(field);
        }
    }

    if !unsupported_fields.is_empty() {
        return Err(DataFusionError::Execution(format!(
            "Unsupported data type(s) in schema: {}. By default, unsupported types cause an \
             error. To convert top-level unsupported columns to strings, set 'unsupported_type_action: string'; \
             nested unsupported types must be removed or rewritten to preserve data correctness.",
            unsupported_fields.join(", ")
        )));
    }

    Ok(Schema::new_with_metadata(
        transformed_fields,
        schema.metadata().clone(),
    ))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion_table_providers::UnsupportedTypeAction;

    use super::transform_schema_for_vortex;

    #[test]
    fn float16_converts_to_float32() {
        let schema = Schema::new(vec![Field::new("x", DataType::Float16, false)]);
        let out = transform_schema_for_vortex(&schema, UnsupportedTypeAction::Error)
            .expect("should succeed");
        assert_eq!(out.field(0).data_type(), &DataType::Float32);
    }

    #[test]
    fn non_microsecond_timestamp_converted() {
        let schema = Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        )]);
        let out = transform_schema_for_vortex(&schema, UnsupportedTypeAction::Error)
            .expect("should succeed");
        assert_eq!(
            out.field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
    }

    #[test]
    fn unsupported_type_errors_when_action_is_error() {
        let schema = Schema::new(vec![Field::new(
            "d",
            DataType::Duration(TimeUnit::Second),
            false,
        )]);
        transform_schema_for_vortex(&schema, UnsupportedTypeAction::Error)
            .expect_err("Duration should be unsupported");
    }

    #[test]
    fn field_metadata_preserved_on_type_conversion() {
        let mut field_metadata = HashMap::new();
        field_metadata.insert("logicalType".to_string(), "FIXED".to_string());
        field_metadata.insert("scale".to_string(), "2".to_string());

        let field = Field::new("x", DataType::Float16, false).with_metadata(field_metadata.clone());
        let mut schema_metadata = HashMap::new();
        schema_metadata.insert("source".to_string(), "snowflake".to_string());

        let schema = Schema::new_with_metadata(vec![field], schema_metadata.clone());
        let out = transform_schema_for_vortex(&schema, UnsupportedTypeAction::Error)
            .expect("should succeed");

        assert_eq!(out.field(0).data_type(), &DataType::Float32);
        assert_eq!(out.field(0).metadata(), &field_metadata);
        assert_eq!(out.metadata(), &schema_metadata);
    }

    #[test]
    fn field_metadata_preserved_on_timestamp_conversion() {
        let mut field_metadata = HashMap::new();
        field_metadata.insert("logicalType".to_string(), "TIMESTAMP_NTZ".to_string());

        let field = Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), false)
            .with_metadata(field_metadata.clone());
        let schema = Schema::new(vec![field]);
        let out = transform_schema_for_vortex(&schema, UnsupportedTypeAction::Error)
            .expect("should succeed");

        assert_eq!(
            out.field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(out.field(0).metadata(), &field_metadata);
    }

    #[test]
    fn field_metadata_preserved_on_unsupported_to_string() {
        let mut field_metadata = HashMap::new();
        field_metadata.insert("originalType".to_string(), "interval".to_string());

        let field = Field::new("d", DataType::Duration(TimeUnit::Second), false)
            .with_metadata(field_metadata.clone());
        let schema = Schema::new(vec![field]);
        let out = transform_schema_for_vortex(&schema, UnsupportedTypeAction::String)
            .expect("should succeed");

        assert_eq!(out.field(0).data_type(), &DataType::Utf8);
        assert_eq!(out.field(0).metadata(), &field_metadata);
    }

    #[test]
    fn nested_safe_types_are_transformed_recursively() {
        let mut nested_metadata = HashMap::new();
        nested_metadata.insert("logicalType".to_string(), "TIMESTAMP_NTZ".to_string());

        let schema = Schema::new(vec![Field::new(
            "payload",
            DataType::Struct(
                vec![
                    Field::new("score", DataType::Float16, true),
                    Field::new(
                        "events",
                        DataType::List(Arc::new(
                            Field::new(
                                "item",
                                DataType::Timestamp(TimeUnit::Nanosecond, None),
                                true,
                            )
                            .with_metadata(nested_metadata.clone()),
                        )),
                        true,
                    ),
                ]
                .into(),
            ),
            true,
        )]);

        let out = transform_schema_for_vortex(&schema, UnsupportedTypeAction::Error)
            .expect("nested supported conversions should succeed");
        let DataType::Struct(fields) = out.field(0).data_type() else {
            panic!("expected struct output");
        };
        assert_eq!(fields[0].data_type(), &DataType::Float32);
        let DataType::List(item) = fields[1].data_type() else {
            panic!("expected list output");
        };
        assert_eq!(
            item.data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(item.metadata(), &nested_metadata);
    }

    #[test]
    fn nested_unsupported_type_errors_with_path() {
        let schema = Schema::new(vec![Field::new(
            "payload",
            DataType::Struct(
                vec![Field::new(
                    "duration",
                    DataType::Duration(TimeUnit::Second),
                    true,
                )]
                .into(),
            ),
            true,
        )]);

        let err = transform_schema_for_vortex(&schema, UnsupportedTypeAction::String)
            .expect_err("nested schema-only string conversion would be unsafe");
        let message = err.to_string();
        assert!(
            message.contains("payload.duration"),
            "error should include nested field path, got: {message}"
        );
    }

    #[test]
    fn map_with_supported_children_is_preserved() {
        let schema = Schema::new(vec![Field::new(
            "headers",
            DataType::Map(
                Arc::new(Field::new_struct(
                    "entries",
                    vec![
                        Arc::new(Field::new("keys", DataType::Utf8, false)),
                        Arc::new(Field::new("values", DataType::Utf8, true)),
                    ],
                    false,
                )),
                false,
            ),
            true,
        )]);

        let out = transform_schema_for_vortex(&schema, UnsupportedTypeAction::Error)
            .expect("map with supported children should be preserved");
        assert_eq!(out, schema);
    }
}
