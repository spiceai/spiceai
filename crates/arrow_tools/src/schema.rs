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
    #[snafu(display("Expected and actual number of fields in the query result don't match: expected {expected}, received {actual}"))]
    SchemaMismatchNumFields { expected: usize, actual: usize },

    #[snafu(display("Query returned an unexpected data type for column {name}: expected {expected}, received {actual}. Is the column data type supported by the data accelerator (https://spiceai.org/docs/reference/datatypes)?"))]
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
}
