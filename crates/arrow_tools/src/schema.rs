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

use std::{collections::HashMap, sync::Arc};

use arrow::array::RecordBatch;
use arrow_schema::{DataType, Field, Schema};
use datafusion::common::DFSchema;
use snafu::prelude::*;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Expected and actual number of fields in the query result don't match: expected {expected}, received {actual}"
    ))]
    SchemaMismatchNumFields { expected: usize, actual: usize },

    #[snafu(display(
        "Query returned an unexpected data type for column {name}: expected {expected}, received {actual}. Is the column data type supported by the data accelerator (https://spiceai.org/docs/reference/datatypes)?"
    ))]
    SchemaMismatchDataType {
        name: String,
        expected: String,
        actual: String,
    },

    #[snafu(display(
        "Query returned an unexpected field name at position {position}: expected '{expected}', received '{actual}'"
    ))]
    SchemaMismatchFieldName {
        position: usize,
        expected: String,
        actual: String,
    },

    #[snafu(display("Failed to get field data type"))]
    UnableToGetFieldDataType {},
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// Validates the fields between two Arrow schemas match, with a specific error about which field is mismatched.
///
/// # Errors
///
/// This function will return an error if the fields of the expected schema don't
/// match the fields of the actual schema.
pub fn verify_schema(
    expected: &arrow::datatypes::Fields,
    actual: &arrow::datatypes::Fields,
) -> Result<()> {
    if expected.len() != actual.len() {
        return SchemaMismatchNumFieldsSnafu {
            expected: expected.len(),
            actual: actual.len(),
        }
        .fail();
    }

    for idx in 0..expected.len() {
        let a = expected.get(idx).context(UnableToGetFieldDataTypeSnafu)?;
        let b = actual.get(idx).context(UnableToGetFieldDataTypeSnafu)?;

        // Verify field names match
        if a.name() != b.name() {
            return SchemaMismatchFieldNameSnafu {
                position: idx,
                expected: a.name(),
                actual: b.name(),
            }
            .fail();
        }

        let a_data_type = a.data_type();
        let b_data_type = b.data_type();

        // Parameterized queries will result in a schema mismatch because the
        // field type is unknown (and defaults to NULL) but once the query is
        // executed, a (likely) non-null value is produced
        if is_null_placeholder(a) || is_null_placeholder(b) {
            continue;
        }

        // Utf8View is semantically equivalent to Utf8 and LargeUtf8 — they differ
        // only in memory layout. Accept any combination of these string types so
        // that DoPut clients (ADBC, Flight) can send Utf8 data to Cayenne tables
        // that store columns as Utf8View.
        if is_utf8_family(a_data_type) && is_utf8_family(b_data_type) {
            continue;
        }

        if !DFSchema::datatype_is_semantically_equal(a_data_type, b_data_type) {
            return SchemaMismatchDataTypeSnafu {
                name: a.name(),
                expected: format!("{a_data_type}"),
                actual: format!("{b_data_type}"),
            }
            .fail();
        }
    }

    Ok(())
}

fn is_null_placeholder(field: &Arc<Field>) -> bool {
    let is_placeholder = field.name().starts_with('$') || field.name().starts_with('?');
    is_placeholder && field.data_type() == &DataType::Null
}

fn is_utf8_family(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

#[must_use]
pub fn expand_views_schema(schema: &Schema) -> Schema {
    let transformed_fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|field| {
            let new_type = match field.data_type() {
                DataType::Utf8View => DataType::LargeUtf8,
                DataType::BinaryView => DataType::LargeBinary,
                t => t.clone(),
            };
            if &new_type == field.data_type() {
                field.as_ref().clone()
            } else {
                field.as_ref().clone().with_data_type(new_type)
            }
        })
        .collect();

    Schema::new_with_metadata(transformed_fields, schema.metadata().clone())
}

/// Cast any columns whose type differs from the target schema (e.g. `Utf8View` → `LargeUtf8`).
///
/// Returns the batch unchanged if the column count differs from the target schema.
///
/// # Errors
///
/// Returns an [`arrow::error::ArrowError`] if casting a column to the target type fails.
pub fn cast_view_columns(
    batch: RecordBatch,
    target_schema: &Arc<Schema>,
) -> Result<RecordBatch, arrow::error::ArrowError> {
    if batch.num_columns() != target_schema.fields().len() {
        return Ok(batch);
    }

    if batch
        .schema()
        .fields()
        .iter()
        .zip(target_schema.fields().iter())
        .all(|(s, t)| s.data_type() == t.data_type())
    {
        return Ok(batch);
    }

    let columns = batch
        .columns()
        .iter()
        .zip(target_schema.fields().iter())
        .map(|(col, target_field)| {
            if col.data_type() == target_field.data_type() {
                Ok(Arc::clone(col))
            } else {
                arrow::compute::cast(col, target_field.data_type())
            }
        })
        .collect::<Result<Vec<_>, _>>()?;

    RecordBatch::try_new(Arc::clone(target_schema), columns)
}

/// Replaces Arrow `Dictionary`-encoded fields with the dictionary's value type.
///
/// Data accelerators such as `DuckDB` and `SQLite` do not natively support Arrow
/// Dictionary types.  By unpacking the dictionary encoding at the schema level
/// (and later casting the data via `arrow_cast::cast`), the downstream
/// accelerator receives only primitive types it can handle.
///
/// For example, `Dictionary(Int32, Utf8)` is normalised to `Utf8`.
#[must_use]
pub fn normalize_dictionary_types(schema: &Schema) -> Schema {
    let transformed_fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|field| {
            let new_type = normalize_dictionary_data_type(field.data_type());
            if &new_type == field.data_type() {
                field.as_ref().clone()
            } else {
                field.as_ref().clone().with_data_type(new_type)
            }
        })
        .collect();

    Schema::new_with_metadata(transformed_fields, schema.metadata().clone())
}

/// Returns `true` if `schema` contains any `Dictionary`-encoded fields,
/// including inside nested container types (List, Struct, Map, etc.).
#[must_use]
pub fn has_dictionary_types(schema: &Schema) -> bool {
    schema
        .fields()
        .iter()
        .any(|f| data_type_contains_dictionary(f.data_type()))
}

/// Recursively checks whether a `DataType` contains any `Dictionary` encoding.
fn data_type_contains_dictionary(data_type: &DataType) -> bool {
    match data_type {
        DataType::Dictionary(_, _) => true,
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _) => data_type_contains_dictionary(field.data_type()),
        DataType::Struct(fields) => fields
            .iter()
            .any(|f| data_type_contains_dictionary(f.data_type())),
        DataType::Union(fields, _) => fields
            .iter()
            .any(|(_, f)| data_type_contains_dictionary(f.data_type())),
        DataType::RunEndEncoded(_, values_field) => {
            data_type_contains_dictionary(values_field.data_type())
        }
        _ => false,
    }
}

/// Recursively replaces `Dictionary(_, value_type)` with `value_type`,
/// descending into nested container types (List, Struct, Map, etc.).
fn normalize_dictionary_data_type(data_type: &DataType) -> DataType {
    match data_type {
        DataType::Dictionary(_, value_type) => normalize_dictionary_data_type(value_type.as_ref()),
        DataType::List(field) => {
            let inner = normalize_dictionary_data_type(field.data_type());
            DataType::List(Arc::new(field.as_ref().clone().with_data_type(inner)))
        }
        DataType::LargeList(field) => {
            let inner = normalize_dictionary_data_type(field.data_type());
            DataType::LargeList(Arc::new(field.as_ref().clone().with_data_type(inner)))
        }
        DataType::FixedSizeList(field, size) => {
            let inner = normalize_dictionary_data_type(field.data_type());
            DataType::FixedSizeList(
                Arc::new(field.as_ref().clone().with_data_type(inner)),
                *size,
            )
        }
        DataType::ListView(field) => {
            let inner = normalize_dictionary_data_type(field.data_type());
            DataType::ListView(Arc::new(field.as_ref().clone().with_data_type(inner)))
        }
        DataType::LargeListView(field) => {
            let inner = normalize_dictionary_data_type(field.data_type());
            DataType::LargeListView(Arc::new(field.as_ref().clone().with_data_type(inner)))
        }
        DataType::Map(field, sorted) => {
            let inner = normalize_dictionary_data_type(field.data_type());
            DataType::Map(
                Arc::new(field.as_ref().clone().with_data_type(inner)),
                *sorted,
            )
        }
        DataType::Struct(fields) => {
            let new_fields: Vec<Field> = fields
                .iter()
                .map(|f| {
                    let inner = normalize_dictionary_data_type(f.data_type());
                    f.as_ref().clone().with_data_type(inner)
                })
                .collect();
            DataType::Struct(new_fields.into())
        }
        DataType::Union(fields, mode) => DataType::Union(
            fields
                .iter()
                .map(|(type_id, f)| {
                    let inner = normalize_dictionary_data_type(f.data_type());
                    (type_id, Arc::new(f.as_ref().clone().with_data_type(inner)))
                })
                .collect(),
            *mode,
        ),
        DataType::RunEndEncoded(run_ends_field, values_field) => {
            let inner = normalize_dictionary_data_type(values_field.data_type());
            DataType::RunEndEncoded(
                Arc::clone(run_ends_field),
                Arc::new(values_field.as_ref().clone().with_data_type(inner)),
            )
        }
        other => other.clone(),
    }
}

/// Returns `true` if casting from `from` to `to` is guaranteed to preserve
/// sort order (i.e. the cast is a monotonically non-decreasing function).
///
/// Covers:
/// - Identity (same type)
/// - Any temporal→temporal (Arrow stores UTC instants; timezone is metadata)
/// - Safe numeric widening (same-sign widening, float widening, int→float
///   where all source values are exactly representable)
#[must_use]
pub fn is_order_preserving_cast(from: &DataType, to: &DataType) -> bool {
    if from == to {
        return true;
    }
    if from.is_temporal() && to.is_temporal() {
        return true;
    }
    if from.is_numeric() && to.is_numeric() {
        return is_numeric_widening(from, to);
    }
    false
}

/// Same-signedness integer widening, float widening, and integer→float where
/// all values are exactly representable.
#[must_use]
pub fn is_numeric_widening(from: &DataType, to: &DataType) -> bool {
    matches!(
        (from, to),
        (
            DataType::Int8 | DataType::Int16 | DataType::Int32,
            DataType::Int64
        ) | (DataType::Int8 | DataType::Int16, DataType::Int32)
            | (DataType::Int8, DataType::Int16)
            | (
                DataType::UInt8 | DataType::UInt16 | DataType::UInt32,
                DataType::UInt64
            )
            | (DataType::UInt8 | DataType::UInt16, DataType::UInt32)
            | (DataType::UInt8, DataType::UInt16)
            | (
                DataType::Float32
                    | DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::UInt8
                    | DataType::UInt16
                    | DataType::UInt32,
                DataType::Float64
            )
            | (
                DataType::Int8 | DataType::Int16 | DataType::UInt8 | DataType::UInt16,
                DataType::Float32
            )
    )
}

pub fn set_computed_columns_meta<S: ::std::hash::BuildHasher>(
    schema: &mut Schema,
    computed_columns_meta: &HashMap<String, Vec<String>, S>,
) {
    for (base_column, computed_columns) in computed_columns_meta {
        set_computed_columns_meta_for_base_column(schema, base_column, computed_columns);
    }
}

pub fn set_computed_columns_meta_for_base_column(
    schema: &mut Schema,
    base_column: &str,
    computed_columns: &[String],
) {
    schema.metadata.insert(
        format!("{base_column}_computed_columns"),
        computed_columns.join(","),
    );
}

#[must_use]
pub fn schema_meta_get_computed_columns(
    schema: &Schema,
    base_column: &str,
) -> Option<Vec<Arc<Field>>> {
    let key = format!("{base_column}_computed_columns");

    if let Some(computed_columns_str) = schema.metadata.get(&key) {
        let computed_column_names: Vec<&str> = computed_columns_str.split(',').collect();

        Some(
            schema
                .fields()
                .iter()
                .filter(|field| computed_column_names.contains(&field.name().as_str()))
                .cloned()
                .collect(),
        )
    } else {
        None
    }
}

/// Returns a string describing the difference between two schemas, if any.
#[must_use]
pub fn schema_difference(expected: &Schema, actual: &Schema) -> Option<String> {
    let mut differences = Vec::new();

    // Check for missing columns in actual schema
    for field in expected.fields() {
        if !actual.fields().iter().any(|f| f.name() == field.name()) {
            differences.push(format!("The column `{}` is missing", field.name()));
        }
    }

    // Check for extra columns in actual schema
    for field in actual.fields() {
        if !expected.fields().iter().any(|f| f.name() == field.name()) {
            differences.push(format!("The column `{}` is unexpected", field.name()));
        }
    }

    // Check for type mismatches in common columns
    for expected_field in expected.fields() {
        if let Some(actual_field) = actual
            .fields()
            .iter()
            .find(|f| f.name() == expected_field.name())
            && !DFSchema::datatype_is_semantically_equal(
                expected_field.data_type(),
                actual_field.data_type(),
            )
        {
            differences.push(format!(
                "The type of `{column_name}` changed from `{expected_type}` to `{actual_type}`",
                column_name = expected_field.name(),
                expected_type = expected_field.data_type(),
                actual_type = actual_field.data_type()
            ));
        }
    }

    if differences.is_empty() {
        None
    } else {
        Some(differences.join(". "))
    }
}

/// Returns associated `DataFusion` SQL type name for provided Arrow `DataType`.
/// `<https://datafusion.apache.org/user-guide/sql/data_types.html>`
#[must_use]
pub fn to_source_native_type_name(data_type: &DataType) -> &'static str {
    // For non-complex types, `to_string()` can be used to return type information, but for consistency and control over naming,
    // explicit matching and type names are used.
    match data_type {
        DataType::Null => "NULL",
        DataType::Boolean => "BOOLEAN",
        DataType::Int8 => "TINYINT",
        DataType::Int16 => "SMALLINT",
        DataType::Int32 => "INTEGER",
        DataType::Int64 => "BIGINT",
        DataType::UInt8 => "TINYINT UNSIGNED",
        DataType::UInt16 => "SMALLINT UNSIGNED",
        DataType::UInt32 => "INTEGER UNSIGNED",
        DataType::UInt64 => "BIGINT UNSIGNED",
        // There is no direct mapping for Float16 in DataFusion SQL, so we use REAL.
        DataType::Float16 | DataType::Float32 => "REAL",
        DataType::Float64 => "DOUBLE",
        DataType::Decimal32(_, _)
        | DataType::Decimal64(_, _)
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _) => "DECIMAL",
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "VARCHAR",
        DataType::Timestamp(_, _) => "TIMESTAMP",
        DataType::Date32 | DataType::Date64 => "DATE",
        DataType::Time32(_) | DataType::Time64(_) => "TIME",
        DataType::Interval(_) => "INTERVAL",
        DataType::Binary
        | DataType::FixedSizeBinary(_)
        | DataType::LargeBinary
        | DataType::BinaryView => "BYTEA",
        DataType::List(_)
        | DataType::LargeList(_)
        | DataType::FixedSizeList(_, _)
        | DataType::LargeListView(_)
        | DataType::ListView(_) => "ARRAY",

        DataType::Struct(_) => "STRUCT",
        // The following types are not durectly supported in SQL queries by DataFusion,
        // Clients must treat them as text or use custom logic to handle them.
        // `<https://github.com/apache/datafusion/blob/85eebcd25dfbe8e2d2d75d85b8683de8be4851e8/datafusion/sql/src/planner.rs#L720>`
        DataType::Map(_, _) => "MAP",
        DataType::Duration(_) => "DURATION",
        DataType::Union(_, _) => "UNION",
        DataType::Dictionary(_, _) => "DICTIONARY",
        DataType::RunEndEncoded(_, _) => "RUNENDENCODED",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    fn create_test_schema_with_embeddings() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, true),
            Field::new(
                "name_embedding",
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::FixedSizeList(
                        Arc::new(Field::new("item", DataType::Float32, false)),
                        1536,
                    ),
                    false,
                ))),
                false,
            ),
            Field::new(
                "name_offset",
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::FixedSizeList(
                        Arc::new(Field::new("item", DataType::Int32, false)),
                        2,
                    ),
                    false,
                ))),
                false,
            ),
        ])
    }

    #[test]
    fn test_computed_columns_meta() {
        let mut schema = create_test_schema_with_embeddings();

        let mut computed_columns_meta = HashMap::new();
        computed_columns_meta.insert(
            "name".to_string(),
            vec!["name_embedding".to_string(), "name_offset".to_string()],
        );

        // Set metadata
        set_computed_columns_meta(&mut schema, &computed_columns_meta);

        // Retrieve computed columns metadata
        let computed_columns = schema_meta_get_computed_columns(&schema, "name")
            .expect("should return computed columns");

        assert_eq!(computed_columns.len(), 2);
        assert_eq!(computed_columns[0].name(), "name_embedding");
        assert_eq!(computed_columns[1].name(), "name_offset");
    }

    #[test]
    fn test_schema_difference() {
        let expected = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int32, true),
        ]);

        let actual = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("age", DataType::Utf8, true),
            Field::new("extra", DataType::Float64, false),
        ]);

        let diff = schema_difference(&expected, &actual);
        let Some(diff) = diff else {
            panic!("should return a string");
        };
        assert!(diff.contains("The column `name` is missing"));
        assert!(diff.contains("The column `extra` is unexpected"));
        assert!(diff.contains("The type of `age` changed from `Int32` to `Utf8`"));
    }

    #[test]
    fn test_schema_difference_nullability() {
        let expected = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]);

        let actual = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]);

        let diff = schema_difference(&expected, &actual);
        assert!(diff.is_none());
    }

    #[test]
    fn test_schema_difference_identical() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]);

        let diff = schema_difference(&schema, &schema);
        assert!(diff.is_none());
    }

    #[test]
    fn test_normalize_dictionary_types() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "status",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                true,
            ),
            Field::new("name", DataType::Utf8, false),
            Field::new(
                "category",
                DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::LargeUtf8)),
                false,
            ),
        ]);

        let normalized = normalize_dictionary_types(&schema);

        assert_eq!(normalized.field(0).data_type(), &DataType::Int64);
        assert_eq!(normalized.field(1).data_type(), &DataType::Utf8);
        assert!(normalized.field(1).is_nullable());
        assert_eq!(normalized.field(2).data_type(), &DataType::Utf8);
        assert_eq!(normalized.field(3).data_type(), &DataType::LargeUtf8);
        assert!(!normalized.field(3).is_nullable());
    }

    #[test]
    fn test_has_dictionary_types() {
        let no_dict_schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]);
        assert!(!has_dictionary_types(&no_dict_schema));

        let dict_schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "status",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                true,
            ),
        ]);
        assert!(has_dictionary_types(&dict_schema));
    }

    #[test]
    fn test_normalize_schema_without_dictionary_is_noop() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]);

        let normalized = normalize_dictionary_types(&schema);
        assert_eq!(schema, normalized);
    }

    #[test]
    fn test_normalize_preserves_metadata() {
        let mut metadata = HashMap::new();
        metadata.insert("key".to_string(), "value".to_string());

        let mut field_metadata = HashMap::new();
        field_metadata.insert("custom_key".to_string(), "custom_value".to_string());

        let schema = Schema::new_with_metadata(
            vec![
                Field::new(
                    "col",
                    DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                    false,
                )
                .with_metadata(field_metadata.clone()),
            ],
            metadata.clone(),
        );

        let normalized = normalize_dictionary_types(&schema);
        assert_eq!(normalized.metadata(), &metadata);
        assert_eq!(normalized.field(0).data_type(), &DataType::Utf8);
        assert_eq!(
            normalized.field(0).metadata(),
            &field_metadata,
            "field-level metadata must be preserved after normalization"
        );
    }

    #[test]
    fn test_normalize_nested_dictionary_in_list() {
        let schema = Schema::new(vec![Field::new(
            "tags",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                true,
            ))),
            false,
        )]);

        let normalized = normalize_dictionary_types(&schema);
        let expected = DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));
        assert_eq!(normalized.field(0).data_type(), &expected);
    }

    #[test]
    fn test_normalize_nested_dictionary_in_struct() {
        let schema = Schema::new(vec![Field::new(
            "record",
            DataType::Struct(
                vec![
                    Field::new("id", DataType::Int64, false),
                    Field::new(
                        "status",
                        DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                        true,
                    ),
                ]
                .into(),
            ),
            false,
        )]);

        let normalized = normalize_dictionary_types(&schema);
        let expected = DataType::Struct(
            vec![
                Field::new("id", DataType::Int64, false),
                Field::new("status", DataType::Utf8, true),
            ]
            .into(),
        );
        assert_eq!(normalized.field(0).data_type(), &expected);
    }

    #[test]
    fn test_has_dictionary_types_nested() {
        // Dictionary inside List
        let list_dict_schema = Schema::new(vec![Field::new(
            "tags",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                true,
            ))),
            false,
        )]);
        assert!(has_dictionary_types(&list_dict_schema));

        // Dictionary inside Struct
        let struct_dict_schema = Schema::new(vec![Field::new(
            "record",
            DataType::Struct(
                vec![Field::new(
                    "status",
                    DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                    true,
                )]
                .into(),
            ),
            false,
        )]);
        assert!(has_dictionary_types(&struct_dict_schema));

        // No dictionary in nested types
        let no_dict_nested = Schema::new(vec![Field::new(
            "tags",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            false,
        )]);
        assert!(!has_dictionary_types(&no_dict_nested));
    }

    #[test]
    fn test_normalize_nested_dictionary_in_union() {
        use arrow_schema::{UnionFields, UnionMode};

        let union_fields = UnionFields::try_new(
            vec![0, 1],
            vec![
                Field::new("int_val", DataType::Int32, false),
                Field::new(
                    "dict_val",
                    DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                    true,
                ),
            ],
        )
        .expect("union fields");
        let schema = Schema::new(vec![Field::new(
            "mixed",
            DataType::Union(union_fields, UnionMode::Dense),
            false,
        )]);

        let normalized = normalize_dictionary_types(&schema);
        let expected_union = UnionFields::try_new(
            vec![0, 1],
            vec![
                Field::new("int_val", DataType::Int32, false),
                Field::new("dict_val", DataType::Utf8, true),
            ],
        )
        .expect("expected union fields");
        assert_eq!(
            normalized.field(0).data_type(),
            &DataType::Union(expected_union, UnionMode::Dense)
        );
    }

    #[test]
    fn test_has_dictionary_types_in_union() {
        use arrow_schema::{UnionFields, UnionMode};

        let union_fields = UnionFields::try_new(
            vec![0, 1],
            vec![
                Field::new("int_val", DataType::Int32, false),
                Field::new(
                    "dict_val",
                    DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                    true,
                ),
            ],
        )
        .expect("union fields");
        let schema = Schema::new(vec![Field::new(
            "mixed",
            DataType::Union(union_fields, UnionMode::Dense),
            false,
        )]);
        assert!(has_dictionary_types(&schema));

        let no_dict_union = UnionFields::try_new(
            vec![0, 1],
            vec![
                Field::new("int_val", DataType::Int32, false),
                Field::new("str_val", DataType::Utf8, true),
            ],
        )
        .expect("union fields");
        let no_dict_schema = Schema::new(vec![Field::new(
            "mixed",
            DataType::Union(no_dict_union, UnionMode::Sparse),
            false,
        )]);
        assert!(!has_dictionary_types(&no_dict_schema));
    }

    #[test]
    fn test_normalize_nested_dictionary_in_list_view() {
        let schema = Schema::new(vec![Field::new(
            "tags",
            DataType::ListView(Arc::new(Field::new(
                "item",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                true,
            ))),
            false,
        )]);

        let normalized = normalize_dictionary_types(&schema);
        let expected = DataType::ListView(Arc::new(Field::new("item", DataType::Utf8, true)));
        assert_eq!(normalized.field(0).data_type(), &expected);
    }

    #[test]
    fn test_normalize_nested_dictionary_in_large_list_view() {
        let schema = Schema::new(vec![Field::new(
            "tags",
            DataType::LargeListView(Arc::new(Field::new(
                "item",
                DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::LargeUtf8)),
                true,
            ))),
            false,
        )]);

        let normalized = normalize_dictionary_types(&schema);
        let expected =
            DataType::LargeListView(Arc::new(Field::new("item", DataType::LargeUtf8, true)));
        assert_eq!(normalized.field(0).data_type(), &expected);
    }

    #[test]
    fn test_normalize_nested_dictionary_in_run_end_encoded() {
        let schema = Schema::new(vec![Field::new(
            "encoded",
            DataType::RunEndEncoded(
                Arc::new(Field::new("run_ends", DataType::Int32, false)),
                Arc::new(Field::new(
                    "values",
                    DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                    true,
                )),
            ),
            false,
        )]);

        let normalized = normalize_dictionary_types(&schema);
        let expected = DataType::RunEndEncoded(
            Arc::new(Field::new("run_ends", DataType::Int32, false)),
            Arc::new(Field::new("values", DataType::Utf8, true)),
        );
        assert_eq!(normalized.field(0).data_type(), &expected);
    }

    #[test]
    fn test_has_dictionary_types_in_list_view() {
        let dict_schema = Schema::new(vec![Field::new(
            "tags",
            DataType::ListView(Arc::new(Field::new(
                "item",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                true,
            ))),
            false,
        )]);
        assert!(has_dictionary_types(&dict_schema));

        let no_dict = Schema::new(vec![Field::new(
            "tags",
            DataType::ListView(Arc::new(Field::new("item", DataType::Utf8, true))),
            false,
        )]);
        assert!(!has_dictionary_types(&no_dict));
    }

    #[test]
    fn test_has_dictionary_types_in_run_end_encoded() {
        let dict_schema = Schema::new(vec![Field::new(
            "encoded",
            DataType::RunEndEncoded(
                Arc::new(Field::new("run_ends", DataType::Int32, false)),
                Arc::new(Field::new(
                    "values",
                    DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                    true,
                )),
            ),
            false,
        )]);
        assert!(has_dictionary_types(&dict_schema));

        let no_dict = Schema::new(vec![Field::new(
            "encoded",
            DataType::RunEndEncoded(
                Arc::new(Field::new("run_ends", DataType::Int32, false)),
                Arc::new(Field::new("values", DataType::Utf8, true)),
            ),
            false,
        )]);
        assert!(!has_dictionary_types(&no_dict));
    }

    #[test]
    fn test_expand_views_schema_preserves_metadata() {
        let field_metadata: HashMap<String, String> =
            [("logicalType".to_string(), "TEXT".to_string())]
                .into_iter()
                .collect();
        let schema_metadata: HashMap<String, String> =
            [("source".to_string(), "snowflake".to_string())]
                .into_iter()
                .collect();

        let schema = Schema::new_with_metadata(
            vec![
                Field::new("id", DataType::Int64, false),
                Field::new("name", DataType::Utf8View, false).with_metadata(field_metadata.clone()),
                Field::new("data", DataType::BinaryView, true),
                Field::new("unchanged", DataType::Float64, false)
                    .with_metadata(field_metadata.clone()),
            ],
            schema_metadata.clone(),
        );

        let expanded = expand_views_schema(&schema);

        assert_eq!(*expanded.field(1).data_type(), DataType::LargeUtf8);
        assert_eq!(expanded.field(1).metadata(), &field_metadata);
        assert_eq!(*expanded.field(2).data_type(), DataType::LargeBinary);
        assert_eq!(*expanded.field(3).data_type(), DataType::Float64);
        assert_eq!(expanded.field(3).metadata(), &field_metadata);
        assert_eq!(expanded.metadata(), &schema_metadata);
    }

    mod cast_view_columns_tests {
        use super::*;
        use arrow::array::{
            Array, BinaryViewArray, Int32Array, LargeBinaryArray, LargeStringArray, RecordBatch,
            StringArray, StringViewArray,
        };

        fn schema(fields: Vec<(&str, DataType)>) -> Arc<Schema> {
            Arc::new(Schema::new(
                fields
                    .into_iter()
                    .map(|(name, dt)| Field::new(name, dt, true))
                    .collect::<Vec<_>>(),
            ))
        }

        #[test]
        fn noop_when_types_already_match() {
            let src = schema(vec![("id", DataType::Int32)]);
            let batch = RecordBatch::try_new(
                Arc::clone(&src),
                vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
            )
            .expect("valid batch");

            let result = cast_view_columns(batch.clone(), &src).expect("cast should succeed");
            assert_eq!(result.schema(), batch.schema());
            assert_eq!(result.num_rows(), 3);
        }

        #[test]
        fn noop_when_column_count_differs() {
            let src = schema(vec![("id", DataType::Int32)]);
            let tgt = schema(vec![("id", DataType::Int32), ("name", DataType::LargeUtf8)]);
            let batch =
                RecordBatch::try_new(Arc::clone(&src), vec![Arc::new(Int32Array::from(vec![1]))])
                    .expect("valid batch");

            let result = cast_view_columns(batch.clone(), &tgt).expect("cast should succeed");
            assert_eq!(result.schema(), batch.schema());
        }

        #[test]
        fn utf8view_cast_to_large_utf8() {
            let src = schema(vec![("name", DataType::Utf8View)]);
            let tgt = schema(vec![("name", DataType::LargeUtf8)]);

            let batch = RecordBatch::try_new(
                Arc::clone(&src),
                vec![Arc::new(StringViewArray::from(vec!["hello", "world"]))],
            )
            .expect("valid batch");

            let result = cast_view_columns(batch, &tgt).expect("cast should succeed");
            assert_eq!(result.schema().field(0).data_type(), &DataType::LargeUtf8);

            let col = result
                .column(0)
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .expect("LargeStringArray");
            assert_eq!(col.value(0), "hello");
            assert_eq!(col.value(1), "world");
        }

        #[test]
        fn binaryview_cast_to_large_binary() {
            let src = schema(vec![("data", DataType::BinaryView)]);
            let tgt = schema(vec![("data", DataType::LargeBinary)]);

            let batch = RecordBatch::try_new(
                Arc::clone(&src),
                vec![Arc::new(BinaryViewArray::from(vec![
                    b"foo".as_ref(),
                    b"bar".as_ref(),
                ]))],
            )
            .expect("valid batch");

            let result = cast_view_columns(batch, &tgt).expect("cast should succeed");
            assert_eq!(result.schema().field(0).data_type(), &DataType::LargeBinary);

            let col = result
                .column(0)
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .expect("LargeBinaryArray");
            assert_eq!(col.value(0), b"foo");
            assert_eq!(col.value(1), b"bar");
        }

        #[test]
        fn mixed_batch_only_view_columns_cast() {
            let src = schema(vec![
                ("id", DataType::Int32),
                ("name", DataType::Utf8View),
                ("raw", DataType::BinaryView),
            ]);
            let tgt = schema(vec![
                ("id", DataType::Int32),
                ("name", DataType::LargeUtf8),
                ("raw", DataType::LargeBinary),
            ]);

            let batch = RecordBatch::try_new(
                Arc::clone(&src),
                vec![
                    Arc::new(Int32Array::from(vec![42])),
                    Arc::new(StringViewArray::from(vec!["alice"])),
                    Arc::new(BinaryViewArray::from(vec![b"bytes".as_ref()])),
                ],
            )
            .expect("valid batch");

            let result = cast_view_columns(batch, &tgt).expect("cast should succeed");
            assert_eq!(result.schema().field(0).data_type(), &DataType::Int32);
            assert_eq!(result.schema().field(1).data_type(), &DataType::LargeUtf8);
            assert_eq!(result.schema().field(2).data_type(), &DataType::LargeBinary);

            let ids = result
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("Int32Array");
            assert_eq!(ids.value(0), 42);

            let names = result
                .column(1)
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .expect("LargeStringArray");
            assert_eq!(names.value(0), "alice");
        }

        #[test]
        fn non_view_utf8_passthrough() {
            let src = schema(vec![("label", DataType::Utf8)]);
            let batch = RecordBatch::try_new(
                Arc::clone(&src),
                vec![Arc::new(StringArray::from(vec!["x"]))],
            )
            .expect("valid batch");

            let result = cast_view_columns(batch, &src).expect("cast should succeed");
            assert_eq!(result.schema().field(0).data_type(), &DataType::Utf8);
            assert_eq!(
                result
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("StringArray")
                    .value(0),
                "x"
            );
        }

        #[test]
        fn null_values_preserved_through_cast() {
            let src = schema(vec![("name", DataType::Utf8View)]);
            let tgt = schema(vec![("name", DataType::LargeUtf8)]);

            let arr: StringViewArray = vec![Some("hello"), None, Some("world")]
                .into_iter()
                .collect();
            let batch =
                RecordBatch::try_new(Arc::clone(&src), vec![Arc::new(arr)]).expect("valid batch");

            let result = cast_view_columns(batch, &tgt).expect("cast should succeed");
            let col = result
                .column(0)
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .expect("LargeStringArray");
            assert_eq!(col.value(0), "hello");
            assert!(col.is_null(1));
            assert_eq!(col.value(2), "world");
        }
    }

    mod monotonic_cast_tests {
        use super::super::{is_numeric_widening, is_order_preserving_cast};
        use arrow::datatypes::{DataType, TimeUnit};

        #[test]
        fn timestamp_ns_to_us_same_tz() {
            let from = DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()));
            let to = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
            assert!(is_order_preserving_cast(&from, &to));
        }

        #[test]
        fn timestamp_s_to_ns() {
            let from = DataType::Timestamp(TimeUnit::Second, Some("UTC".into()));
            let to = DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()));
            assert!(is_order_preserving_cast(&from, &to));
        }

        #[test]
        fn timestamp_ms_to_us() {
            let from = DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()));
            let to = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
            assert!(is_order_preserving_cast(&from, &to));
        }

        #[test]
        fn timestamp_different_tz_annotations() {
            let from = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
            let to = DataType::Timestamp(TimeUnit::Nanosecond, Some("America/New_York".into()));
            assert!(is_order_preserving_cast(&from, &to));
        }

        #[test]
        fn timestamp_tz_to_no_tz() {
            let from = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
            let to = DataType::Timestamp(TimeUnit::Nanosecond, None);
            assert!(is_order_preserving_cast(&from, &to));
        }

        #[test]
        fn date32_to_date64() {
            assert!(is_order_preserving_cast(
                &DataType::Date32,
                &DataType::Date64
            ));
        }

        #[test]
        fn date64_to_date32() {
            assert!(is_order_preserving_cast(
                &DataType::Date64,
                &DataType::Date32
            ));
        }

        #[test]
        fn int8_to_int16() {
            assert!(is_numeric_widening(&DataType::Int8, &DataType::Int16));
            assert!(is_order_preserving_cast(&DataType::Int8, &DataType::Int16));
        }

        #[test]
        fn int8_to_int32() {
            assert!(is_numeric_widening(&DataType::Int8, &DataType::Int32));
        }

        #[test]
        fn int8_to_int64() {
            assert!(is_numeric_widening(&DataType::Int8, &DataType::Int64));
        }

        #[test]
        fn int16_to_int32() {
            assert!(is_numeric_widening(&DataType::Int16, &DataType::Int32));
        }

        #[test]
        fn int16_to_int64() {
            assert!(is_numeric_widening(&DataType::Int16, &DataType::Int64));
        }

        #[test]
        fn int32_to_int64() {
            assert!(is_numeric_widening(&DataType::Int32, &DataType::Int64));
        }

        #[test]
        fn uint8_to_uint16() {
            assert!(is_numeric_widening(&DataType::UInt8, &DataType::UInt16));
        }

        #[test]
        fn uint8_to_uint32() {
            assert!(is_numeric_widening(&DataType::UInt8, &DataType::UInt32));
        }

        #[test]
        fn uint8_to_uint64() {
            assert!(is_numeric_widening(&DataType::UInt8, &DataType::UInt64));
        }

        #[test]
        fn uint16_to_uint32() {
            assert!(is_numeric_widening(&DataType::UInt16, &DataType::UInt32));
        }

        #[test]
        fn uint16_to_uint64() {
            assert!(is_numeric_widening(&DataType::UInt16, &DataType::UInt64));
        }

        #[test]
        fn uint32_to_uint64() {
            assert!(is_numeric_widening(&DataType::UInt32, &DataType::UInt64));
        }

        #[test]
        fn float32_to_float64() {
            assert!(is_numeric_widening(&DataType::Float32, &DataType::Float64));
        }

        #[test]
        fn int32_to_float64() {
            assert!(is_numeric_widening(&DataType::Int32, &DataType::Float64));
        }

        #[test]
        fn uint32_to_float64() {
            assert!(is_numeric_widening(&DataType::UInt32, &DataType::Float64));
        }

        #[test]
        fn int16_to_float32() {
            assert!(is_numeric_widening(&DataType::Int16, &DataType::Float32));
        }

        #[test]
        fn uint16_to_float32() {
            assert!(is_numeric_widening(&DataType::UInt16, &DataType::Float32));
        }

        #[test]
        fn int8_to_float32() {
            assert!(is_numeric_widening(&DataType::Int8, &DataType::Float32));
        }

        #[test]
        fn int8_to_float64() {
            assert!(is_numeric_widening(&DataType::Int8, &DataType::Float64));
        }

        #[test]
        fn int32_to_uint32_not_monotonic() {
            assert!(!is_numeric_widening(&DataType::Int32, &DataType::UInt32));
            assert!(!is_order_preserving_cast(
                &DataType::Int32,
                &DataType::UInt32
            ));
        }

        #[test]
        fn uint32_to_int32_not_monotonic() {
            assert!(!is_numeric_widening(&DataType::UInt32, &DataType::Int32));
        }

        #[test]
        fn int64_to_int32_not_widening() {
            assert!(!is_numeric_widening(&DataType::Int64, &DataType::Int32));
        }

        #[test]
        fn float64_to_float32_not_widening() {
            assert!(!is_numeric_widening(&DataType::Float64, &DataType::Float32));
        }

        #[test]
        fn int64_to_float64_not_widening() {
            assert!(!is_numeric_widening(&DataType::Int64, &DataType::Float64));
        }

        #[test]
        fn uint64_to_float64_not_widening() {
            assert!(!is_numeric_widening(&DataType::UInt64, &DataType::Float64));
        }

        #[test]
        fn int32_to_float32_not_widening() {
            assert!(!is_numeric_widening(&DataType::Int32, &DataType::Float32));
        }

        #[test]
        fn uint32_to_float32_not_widening() {
            assert!(!is_numeric_widening(&DataType::UInt32, &DataType::Float32));
        }

        #[test]
        fn utf8_to_int64_not_order_preserving() {
            assert!(!is_order_preserving_cast(&DataType::Utf8, &DataType::Int64));
        }

        #[test]
        fn int64_to_utf8_not_order_preserving() {
            assert!(!is_order_preserving_cast(&DataType::Int64, &DataType::Utf8));
        }

        #[test]
        fn utf8_to_utf8_same_type() {
            assert!(is_order_preserving_cast(&DataType::Utf8, &DataType::Utf8));
        }

        #[test]
        fn same_type_always_preserving() {
            assert!(is_order_preserving_cast(&DataType::Int64, &DataType::Int64));
            assert!(is_order_preserving_cast(
                &DataType::Float64,
                &DataType::Float64
            ));
            assert!(is_order_preserving_cast(
                &DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                &DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
            ));
        }
    }
}
