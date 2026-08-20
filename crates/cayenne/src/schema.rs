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

use arrow::datatypes::{DataType, Field, Schema};
use arrow_tools::type_rewrite::{Float16ToFloat32, TimestampToMicrosecond, TypeRewriteRules};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion_table_providers::UnsupportedTypeAction;

/// The type rewrites Cayenne applies when it creates a table, because Vortex cannot
/// represent the incoming type.
///
/// This is what [`transform_schema_for_vortex`] applies. It is deliberately narrower
/// than [`CAYENNE_TYPE_REWRITE_RULES`]: a rewrite belongs here only while the engine
/// still performs it.
static CAYENNE_CREATION_REWRITE_RULES: TypeRewriteRules = &[&Float16ToFloat32];

/// The stored types the acceleration write path must recognize as Cayenne's own, so it
/// can tell a type the engine produced from a schema that has genuinely drifted.
///
/// This is a superset of [`CAYENNE_CREATION_REWRITE_RULES`], because a table keeps the
/// types it was created with. [`TimestampToMicrosecond`] is here and *not* in the
/// creation rules: Cayenne once normalized every timestamp to microseconds, and a table
/// created then still stores microseconds even though a table created now keeps its
/// source's unit. Without it, an existing microsecond table fed by a nanosecond source
/// — every `PostgreSQL` `timestamptz`, which infers as `Timestamp(ns, "UTC")` — reads as
/// an incompatible schema change on the first batch after upgrade, which stops CDC
/// replication under `on_schema_change: fail` for a schema that never changed.
///
/// The list is accelerator-wide, so it cannot tell a microsecond column an older build
/// normalized from one whose source is itself microsecond. A table of the second kind
/// whose source later widens to nanoseconds reports the cast as the engine's own rather
/// than as the source schema change it is, so `on_schema_change` does not act on it —
/// the behavior every Cayenne table had while creation normalized unconditionally.
/// Telling the two apart needs per-table provenance, which the metastore does not
/// record today.
pub static CAYENNE_TYPE_REWRITE_RULES: TypeRewriteRules =
    &[&Float16ToFloat32, &TimestampToMicrosecond];

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
    // The always-applied rewrites. These are leaf rules (never a container type), so
    // consulting them before the nested walk below is what `apply_rules` would do too -
    // which is what lets `CAYENNE_CREATION_REWRITE_RULES` describe this step rather
    // than be a second copy of it.
    for rule in CAYENNE_CREATION_REWRITE_RULES {
        if let Some(rewritten) = rule.rewrite(data_type) {
            tracing::debug!(
                "Converting field '{path}' from {data_type:?} to {rewritten:?} for Vortex compatibility"
            );
            return Some(rewritten);
        }
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
            transform_nested_field(
                field,
                &format!("{path}.{}", field.name()),
                unsupported_type_action,
                unsupported_fields,
            ),
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
            transform_nested_field(
                values,
                &format!("{path}.{}", values.name()),
                unsupported_type_action,
                unsupported_fields,
            ),
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
        (true, UnsupportedTypeAction::Error) | (false, _) => {
            unsupported_fields.push(unsupported_field_detail(path, data_type));
            Some(data_type.clone())
        }
    }
}

/// Transform an Arrow schema for Vortex compatibility.
///
/// Always applies:
/// - `Float16` → `Float32`
///
/// `Timestamp` passes through with its time unit and timezone intact: Vortex
/// represents second, millisecond, microsecond and nanosecond timestamps, so a
/// table stores the precision its source reports.
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

    use arrow::datatypes::{DataType, Field, Schema, TimeUnit, UnionFields, UnionMode};
    use arrow_tools::type_rewrite::apply_rules;
    use datafusion_table_providers::UnsupportedTypeAction;

    use super::{CAYENNE_CREATION_REWRITE_RULES, transform_schema_for_vortex};

    #[test]
    fn creation_rewrite_rules_match_vortex_for_all_supported_type_families() {
        let list_item = Arc::new(Field::new(
            "item",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            true,
        ));
        let map_entries = Arc::new(Field::new_struct(
            "entries",
            vec![
                Arc::new(Field::new("key", DataType::Utf8, false)),
                Arc::new(Field::new("value", DataType::Float16, true)),
            ],
            false,
        ));
        let union_fields = UnionFields::try_new(
            vec![0, 1],
            vec![
                Arc::new(Field::new("text", DataType::Utf8, true)),
                Arc::new(Field::new(
                    "at",
                    DataType::Timestamp(TimeUnit::Second, None),
                    true,
                )),
            ],
        )
        .expect("valid union fields");
        let types = vec![
            DataType::Null,
            DataType::Boolean,
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
            DataType::Float16,
            DataType::Float32,
            DataType::Float64,
            DataType::Timestamp(TimeUnit::Second, None),
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            DataType::Timestamp(TimeUnit::Microsecond, None),
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            DataType::Date32,
            DataType::Date64,
            DataType::Time32(TimeUnit::Second),
            DataType::Time32(TimeUnit::Millisecond),
            DataType::Time64(TimeUnit::Microsecond),
            DataType::Time64(TimeUnit::Nanosecond),
            DataType::Binary,
            DataType::LargeBinary,
            DataType::BinaryView,
            DataType::Utf8,
            DataType::LargeUtf8,
            DataType::Utf8View,
            DataType::List(Arc::clone(&list_item)),
            DataType::ListView(Arc::clone(&list_item)),
            DataType::FixedSizeList(Arc::clone(&list_item), 2),
            DataType::LargeList(Arc::clone(&list_item)),
            DataType::LargeListView(Arc::clone(&list_item)),
            DataType::Struct(
                vec![
                    Field::new("score", DataType::Float16, true),
                    Field::new(
                        "at",
                        DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                        true,
                    ),
                ]
                .into(),
            ),
            DataType::Union(union_fields, UnionMode::Sparse),
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Float16)),
            DataType::Decimal32(5, 2),
            DataType::Decimal64(10, 2),
            DataType::Decimal128(20, 2),
            DataType::Decimal256(40, 2),
            DataType::Map(map_entries, false),
            DataType::RunEndEncoded(
                Arc::new(Field::new("run_ends", DataType::Int32, false)),
                Arc::new(Field::new(
                    "values",
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    true,
                )),
            ),
        ];
        let schema = Schema::new(
            types
                .into_iter()
                .enumerate()
                .map(|(index, data_type)| Field::new(format!("column_{index}"), data_type, true))
                .collect::<Vec<_>>(),
        );

        let transformed = transform_schema_for_vortex(&schema, UnsupportedTypeAction::Error)
            .expect("supported types should transform for Vortex");
        let creation_rules = apply_rules(&schema, CAYENNE_CREATION_REWRITE_RULES);

        assert_eq!(
            creation_rules, transformed,
            "creation rewrite rules must normalize every supported Cayenne type exactly as table creation does"
        );
    }

    #[test]
    fn float16_converts_to_float32() {
        let schema = Schema::new(vec![Field::new("x", DataType::Float16, false)]);
        let out = transform_schema_for_vortex(&schema, UnsupportedTypeAction::Error)
            .expect("should succeed");
        assert_eq!(out.field(0).data_type(), &DataType::Float32);
    }

    /// Vortex represents all four Arrow timestamp units, so a table stores the
    /// precision its source reports. Coercing to microseconds instead left a
    /// Postgres `timestamptz` (inferred as ns) permanently unable to match its
    /// own accelerated schema — regression test for
    /// <https://github.com/spiceai/spiceai/issues/13014>.
    #[test]
    fn timestamp_units_are_preserved() {
        for unit in [
            TimeUnit::Second,
            TimeUnit::Millisecond,
            TimeUnit::Microsecond,
            TimeUnit::Nanosecond,
        ] {
            for tz in [None, Some("UTC".into()), Some("+05:30".into())] {
                let data_type = DataType::Timestamp(unit, tz);
                let schema = Schema::new(vec![Field::new("ts", data_type.clone(), false)]);
                let out = transform_schema_for_vortex(&schema, UnsupportedTypeAction::Error)
                    .expect("should succeed");
                assert_eq!(
                    out.field(0).data_type(),
                    &data_type,
                    "timestamp unit and timezone must pass through unchanged"
                );
            }
        }
    }

    /// The compatibility rules must keep explaining a stored microsecond timestamp
    /// while creation preserves nanoseconds. Folding the two lists back together
    /// breaks one side or the other: creation would down-convert again (#13018), or
    /// an existing microsecond table would read as drifted and stop CDC (#13014).
    #[test]
    fn creation_preserves_units_while_the_compatibility_rules_still_explain_microseconds() {
        use arrow_tools::type_rewrite::rewrite_data_type;

        let ns = DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()));
        let us = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));

        let schema = Schema::new(vec![Field::new("ts", ns.clone(), false)]);
        let created = transform_schema_for_vortex(&schema, UnsupportedTypeAction::Error)
            .expect("should succeed");
        assert_eq!(
            created.field(0).data_type(),
            &ns,
            "a table created now stores the source's unit"
        );

        assert_eq!(
            rewrite_data_type(&ns, super::CAYENNE_TYPE_REWRITE_RULES),
            us,
            "the write path must still recognize the microseconds a pre-existing table stores"
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
    fn field_metadata_preserved_on_timestamp_passthrough() {
        let mut field_metadata = HashMap::new();
        field_metadata.insert("logicalType".to_string(), "TIMESTAMP_NTZ".to_string());

        let field = Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), false)
            .with_metadata(field_metadata.clone());
        let schema = Schema::new(vec![field]);
        let out = transform_schema_for_vortex(&schema, UnsupportedTypeAction::Error)
            .expect("should succeed");

        assert_eq!(
            out.field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Nanosecond, None)
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
            &DataType::Timestamp(TimeUnit::Nanosecond, None),
            "a nested timestamp keeps its unit, like a top-level one"
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

    #[test]
    fn map_nested_unsupported_type_errors_with_entries_path() {
        let schema = Schema::new(vec![Field::new(
            "headers",
            DataType::Map(
                Arc::new(Field::new_struct(
                    "entries",
                    vec![
                        Arc::new(Field::new("keys", DataType::Utf8, false)),
                        Arc::new(Field::new(
                            "values",
                            DataType::Duration(TimeUnit::Second),
                            true,
                        )),
                    ],
                    false,
                )),
                false,
            ),
            true,
        )]);

        let err = transform_schema_for_vortex(&schema, UnsupportedTypeAction::Error)
            .expect_err("nested map values duration should be unsupported");
        let message = err.to_string();
        assert!(
            message.contains("headers.entries.values"),
            "error should include map entries field path, got: {message}"
        );
    }

    #[test]
    fn run_end_encoded_nested_unsupported_type_errors_with_values_path() {
        let schema = Schema::new(vec![Field::new(
            "encoded",
            DataType::RunEndEncoded(
                Arc::new(Field::new("run_ends", DataType::Int32, false)),
                Arc::new(Field::new(
                    "values",
                    DataType::Struct(
                        vec![Field::new(
                            "duration",
                            DataType::Duration(TimeUnit::Second),
                            true,
                        )]
                        .into(),
                    ),
                    true,
                )),
            ),
            true,
        )]);

        let err = transform_schema_for_vortex(&schema, UnsupportedTypeAction::Error)
            .expect_err("nested run-end encoded values duration should be unsupported");
        let message = err.to_string();
        assert!(
            message.contains("encoded.values.duration"),
            "error should include run-end encoded values field path, got: {message}"
        );
    }
}
