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

        let a_data_type = a.data_type();
        let b_data_type = b.data_type();

        // Parameterized queries will result in a schema mismatch because the
        // field type is unknown (and defaults to NULL) but once the query is
        // executed, a (likely) non-null value is produced
        if is_null_placeholder(a) || is_null_placeholder(b) {
            continue;
        }

        // We set the DataFusion option `df_config.options_mut().optimizer.expand_views_at_output = true`
        // to expand views at the output of a query. This means that a query that expects a
        // `Utf8View` will be expanded to a `LargeUtf8` in the result set.
        if a_data_type == &DataType::Utf8View && b_data_type == &DataType::LargeUtf8 {
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
            Field::new(field.name(), new_type, field.is_nullable())
        })
        .collect();

    Schema::new(transformed_fields)
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
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => "DECIMAL",
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
}
