/*
Copyright 2025 The Spice.ai OSS Authors

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

use arrow::array::{Array, RecordBatch, StructArray};
use arrow::datatypes::{DataType, Field, Fields, Schema};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

/// Represents a path to a field in the original nested schema
#[derive(Debug, Clone, PartialEq)]
#[expect(clippy::doc_link_with_quotes)]
pub struct FieldPath {
    /// The field names along the path (e.g., ["person", "name"])
    pub field_names: Vec<String>,
    /// The field indices along the path (e.g., [1, 0] for second field, first nested field)
    pub field_indices: Vec<usize>,
}

/// Maps a flattened field name back to its path in the original nested schema.
/// Returns `None` if the flattened field name doesn't exist in the original schema.
///
/// # Arguments
/// * `flattened_field_name` - The flattened field name (e.g., "person.name")
/// * `original_schema` - The original nested schema
/// * `separator` - The separator used for flattening (e.g., ".")
///
/// # Example
/// ```rust,no_run
/// use arrow::datatypes::{DataType, Field, Fields, Schema};
/// use dataformat_json::{map_flattened_to_original, FieldPath};
/// use std::sync::Arc;
///
/// let name_field = Field::new("name", DataType::Utf8, false);
/// let person_fields = Fields::from(vec![name_field]);
/// let person_field = Field::new("person", DataType::Struct(person_fields), false);
/// let schema = Schema::new(vec![person_field]);
///
/// let path = map_flattened_to_original("person.name", &schema, ".").unwrap();
/// assert_eq!(path.field_names, vec!["person", "name"]);
/// assert_eq!(path.field_indices, vec![0, 0]);
/// ```
#[must_use]
pub fn map_flattened_to_original(
    flattened_field_name: &str,
    original_schema: &Schema,
    separator: &str,
) -> Option<FieldPath> {
    let path_parts: Vec<&str> = flattened_field_name.split(separator).collect();
    if path_parts.is_empty() {
        return None;
    }

    let mut field_names = Vec::new();
    let mut field_indices = Vec::new();
    let mut current_fields = original_schema.fields();

    for part in path_parts {
        // Find the field with this name at the current level
        let field_index = current_fields.iter().position(|f| f.name() == part)?;
        let field = &current_fields[field_index];

        field_names.push(part.to_string());
        field_indices.push(field_index);

        // If this field is a struct, prepare to look into its nested fields
        if let DataType::Struct(nested_fields) = field.data_type() {
            current_fields = nested_fields;
        }
    }

    Some(FieldPath {
        field_names,
        field_indices,
    })
}

/// Flattens a schema by expanding nested struct fields into top-level fields with dot notation.
/// Adds metadata to each field to allow reconstruction of the original schema.
#[must_use]
pub fn unnest_struct_schema(schema: &Schema, separator: &str) -> Schema {
    let mut flattened_fields = Vec::new();
    let mut stack = Vec::new();

    // Initialize stack with top-level fields and empty prefixes
    for field in schema.fields().iter().rev() {
        stack.push((field, String::new()));
    }

    while let Some((field, prefix)) = stack.pop() {
        let field_name = if prefix.is_empty() {
            field.name().clone()
        } else {
            format!("{}{}{}", prefix, separator, field.name())
        };

        if let DataType::Struct(nested_fields) = field.data_type() {
            // Push nested fields onto stack with updated prefix
            for nested_field in nested_fields.iter().rev() {
                stack.push((nested_field, field_name.clone()));
            }
        } else {
            let flattened_field =
                Field::new(field_name, field.data_type().clone(), field.is_nullable());
            flattened_fields.push(flattened_field);
        }
    }

    Schema::new(flattened_fields)
}

/// Reconstructs the original nested schema from a flattened schema.
/// Uses the field names directly as paths and the provided separator.
///
/// # Arguments
/// * `flattened_schema` - The flattened schema
/// * `separator` - The separator used for flattening (e.g., ".")
#[must_use]
pub fn nest_struct_schema(flattened_schema: &Schema, separator: &str) -> Schema {
    // Build a tree structure from the flattened paths
    let mut tree = FieldTree::new();

    for (order, field) in flattened_schema.fields().iter().enumerate() {
        let original_path = field.name();
        let path_parts: Vec<&str> = original_path.split(separator).collect();
        tree.insert_with_order(&path_parts, field, order);
    }

    Schema::new(tree.build_fields())
}

/// Finds the column index in the nested schema that corresponds to a flattened field.
/// Returns `None` if the flattened field doesn't contain the required metadata or
/// if the root field is not found in the nested schema.
///
/// # Arguments
/// * `flattened_field` - A field from a flattened schema with metadata
/// * `nested_schema` - The original nested schema
///
/// # Example
/// ```rust,no_run
/// use arrow::datatypes::{DataType, Field, Fields, Schema};
/// use dataformat_json::{unnest_struct_schema, find_col_index};
///
/// let name_field = Field::new("name", DataType::Utf8, false);
/// let person_fields = Fields::from(vec![name_field]);
/// let person_field = Field::new("person", DataType::Struct(person_fields), false);
/// let id_field = Field::new("id", DataType::Int32, false);
///
/// let nested_schema = Schema::new(vec![id_field, person_field]);
/// let flattened_schema = unnest_struct_schema(&nested_schema, ".");
///
/// // Find which column in the nested schema the flattened "person.name" field belongs to
/// let person_name_field = flattened_schema.field(1); // "person.name"
/// let col_index = find_col_index(person_name_field, &nested_schema).unwrap();
/// assert_eq!(col_index, 1); // The "person" struct is at index 1
/// ```
#[must_use]
pub fn find_col_index(
    flattened_field: &Field,
    nested_schema: &Schema,
    separator: &str,
) -> Option<usize> {
    let original_path = flattened_field.name();

    let path_parts: Vec<&str> = original_path.split(separator).collect();
    let root_field_name = path_parts.first()?;

    nested_schema
        .fields()
        .iter()
        .position(|field| field.name() == root_field_name)
}

#[must_use]
pub fn flatten_json_obj(value: &Value, delimiter: &str) -> Value {
    let mut out = serde_json::Map::new();
    flatten_json(value, String::new(), delimiter, &mut out);
    Value::Object(out)
}

fn flatten_json(
    value: &Value,
    prefix: String,
    delimiter: &str,
    out: &mut serde_json::Map<String, Value>,
) {
    match value {
        Value::Object(map) => {
            for (k, v) in map {
                let new_prefix = if prefix.is_empty() {
                    k.clone()
                } else {
                    format!("{prefix}{delimiter}{k}")
                };
                flatten_json(v, new_prefix, delimiter, out);
            }
        }
        _ => {
            // For arrays and scalar values (null, boolean, number, string)
            out.insert(prefix, value.clone());
        }
    }
}

/// Projects a nested schema to only include top-level fields that are referenced
/// by any field in the filtered flattened schema. Returns the complete original fields,
/// not filtered versions of them.
///
/// # Arguments
/// * `filtered_flattened_schema` - A subset of flattened fields with metadata
/// * `original_nested_schema` - The original nested schema to project from
///
/// # Example
/// ```rust,no_run
/// use arrow::datatypes::{DataType, Field, Fields, Schema};
/// use dataformat_json::{unnest_struct_schema, project_nested_schema};
///
/// let name_field = Field::new("name", DataType::Utf8, false);
/// let age_field = Field::new("age", DataType::Int32, false);
/// let email_field = Field::new("email", DataType::Utf8, false);
/// let person_fields = Fields::from(vec![name_field, age_field, email_field]);
/// let person_field = Field::new("person", DataType::Struct(person_fields), false);
/// let id_field = Field::new("id", DataType::Int32, false);
/// let status_field = Field::new("status", DataType::Utf8, false);
///
/// let original_nested = Schema::new(vec![id_field, person_field, status_field]);
/// let full_flattened = unnest_struct_schema(&original_nested, ".");
///
/// // Create a subset: person.name and status
/// let subset_fields = vec![
///     full_flattened.field(1).clone(), // person.name
///     full_flattened.field(4).clone(), // status  
/// ];
/// let filtered_flattened = Schema::new(subset_fields);
///
/// let projected_nested = project_nested_schema(&filtered_flattened, &original_nested);
/// // Result: [person{name, age, email}, status] - complete original fields
/// ```
#[must_use]
pub fn project_nested_schema(
    filtered_flattened_schema: &Schema,
    original_nested_schema: &Schema,
    separator: &str,
) -> Schema {
    use std::collections::BTreeSet;

    // Collect all column indices that are referenced by the filtered fields
    let mut selected_column_indices = BTreeSet::new();

    for field in filtered_flattened_schema.fields() {
        if let Some(col_index) = find_col_index(field, original_nested_schema, separator) {
            selected_column_indices.insert(col_index);
        }
    }

    // Build the projected schema with the complete original fields
    let mut projected_fields = Vec::new();
    for &col_index in &selected_column_indices {
        let original_field = original_nested_schema.field(col_index);
        projected_fields.push(original_field.clone());
    }

    Schema::new(projected_fields)
}

/// Converts a flattened `RecordBatch` back to its original nested structure.
/// Returns `None` if the reconstruction fails.
#[must_use]
pub fn nest_struct(flattened_batch: &RecordBatch, separator: &str) -> Option<RecordBatch> {
    let original_schema = Arc::new(nest_struct_schema(&flattened_batch.schema(), separator));

    let mut tree = FieldTree::new();

    // Build tree with column references
    for (i, field) in flattened_batch.schema().fields().iter().enumerate() {
        let original_path = field.name();
        let path_parts: Vec<&str> = original_path.split(&separator).collect();
        tree.insert_with_column(&path_parts, field, flattened_batch.column(i), i);
    }

    let nested_columns = tree.build_columns();
    RecordBatch::try_new(original_schema, nested_columns).ok()
}

#[derive(Debug)]
struct FieldTree {
    name: String,
    field: Option<Field>,
    column: Option<Arc<dyn Array>>,
    order: usize,
    children: HashMap<String, FieldTree>,
}

impl FieldTree {
    fn new() -> Self {
        Self {
            name: String::new(),
            field: None,
            column: None,
            order: usize::MAX,
            children: HashMap::new(),
        }
    }

    fn insert_with_order(&mut self, path_parts: &[&str], field: &Field, order: usize) {
        if path_parts.is_empty() {
            return;
        }

        if path_parts.len() == 1 {
            // Leaf node
            self.children.insert(
                path_parts[0].to_string(),
                FieldTree {
                    name: path_parts[0].to_string(),
                    field: Some(Field::new(
                        path_parts[0].to_string(),
                        field.data_type().clone(),
                        field.is_nullable(),
                    )),
                    column: None,
                    order,
                    children: HashMap::new(),
                },
            );
        } else {
            // Internal node
            let child_name = path_parts[0].to_string();
            let child = self
                .children
                .entry(child_name.clone())
                .or_insert_with(|| FieldTree {
                    name: child_name,
                    field: None,
                    column: None,
                    order,
                    children: HashMap::new(),
                });
            child.insert_with_order(&path_parts[1..], field, order);
        }
    }

    fn insert_with_column(
        &mut self,
        path_parts: &[&str],
        field: &Field,
        column: &Arc<dyn Array>,
        order: usize,
    ) {
        if path_parts.is_empty() {
            return;
        }

        if path_parts.len() == 1 {
            // Leaf node
            self.children.insert(
                path_parts[0].to_string(),
                FieldTree {
                    name: path_parts[0].to_string(),
                    field: Some(Field::new(
                        path_parts[0].to_string(),
                        field.data_type().clone(),
                        field.is_nullable(),
                    )),
                    column: Some(Arc::clone(column)),
                    order,
                    children: HashMap::new(),
                },
            );
        } else {
            // Internal node
            let child_name = path_parts[0].to_string();
            let child = self
                .children
                .entry(child_name.clone())
                .or_insert_with(|| FieldTree {
                    name: child_name,
                    field: None,
                    column: None,
                    order,
                    children: HashMap::new(),
                });
            child.insert_with_column(&path_parts[1..], field, column, order);
        }
    }

    fn build_fields(&self) -> Vec<Field> {
        let mut fields = Vec::new();

        // Sort children by original order
        let mut sorted_children: Vec<_> = self.children.iter().collect();
        sorted_children.sort_by_key(|(_, child)| child.order);

        for (_, child) in sorted_children {
            if let Some(field) = child.build_field() {
                fields.push(field);
            }
        }

        fields
    }

    fn build_field(&self) -> Option<Field> {
        if let Some(field) = &self.field {
            // Leaf node
            Some(field.clone())
        } else if !self.children.is_empty() {
            // Internal node - build struct
            let nested_fields = self.build_fields();
            let fields = Fields::from(nested_fields);
            Some(Field::new(
                self.name.clone(),
                DataType::Struct(fields),
                false,
            ))
        } else {
            None
        }
    }

    fn build_columns(&self) -> Vec<Arc<dyn Array>> {
        let mut columns = Vec::new();

        // Sort children by original order
        let mut sorted_children: Vec<_> = self.children.iter().collect();
        sorted_children.sort_by_key(|(_, child)| child.order);

        for (_, child) in sorted_children {
            if let Some(column) = child.build_column() {
                columns.push(column);
            }
        }

        columns
    }

    fn build_column(&self) -> Option<Arc<dyn Array>> {
        if let Some(column) = &self.column {
            // Leaf node
            Some(Arc::clone(column))
        } else if !self.children.is_empty() {
            // Internal node - build struct array
            let nested_columns = self.build_columns();
            let nested_fields = self.build_fields();

            if nested_columns.len() == nested_fields.len() {
                let struct_columns: Vec<_> = nested_fields
                    .into_iter()
                    .zip(nested_columns)
                    .map(|(field, column)| (Arc::new(field), column))
                    .collect();

                Some(Arc::new(StructArray::from(struct_columns)))
            } else {
                None
            }
        } else {
            None
        }
    }
}

/// Extracts columns from a nested `RecordBatch` to match a filtered flattened schema.
/// Takes a `RecordBatch` with complete nested structures and returns a `RecordBatch`
/// with only the specific fields requested in the filtered flattened schema.
///
/// # Arguments
/// * `nested_batch` - A `RecordBatch` with nested structures (e.g., person{name, age, email}, status)
/// * `filtered_flattened_schema` - A schema with specific flattened fields (e.g., person.name, status)
///
/// # Example
/// ```rust,no_run
/// use arrow::datatypes::{DataType, Field, Fields, Schema};
/// use arrow::array::{Int32Array, StringArray, StructArray, RecordBatch};
/// use dataformat_json::{unnest_struct_schema, extract_flattened_from_nested};
/// use std::sync::Arc;
///
/// // Original nested RecordBatch with person{name, age, email}, status
/// // Filtered schema wants only person.name, status
/// // Result: RecordBatch with columns [person.name_data, status_data]
/// ```
#[must_use]
pub fn extract_flattened_from_nested(
    nested_batch: &RecordBatch,
    filtered_flattened_schema: &Schema,
    separator: &str,
) -> Option<RecordBatch> {
    let mut extracted_columns = Vec::new();

    for field in filtered_flattened_schema.fields() {
        let original_path = field.name();
        let path_parts: Vec<&str> = original_path.split(separator).collect();

        let column = extract_column_by_path(nested_batch, &path_parts)?;
        extracted_columns.push(column);
    }

    RecordBatch::try_new(
        Arc::new(filtered_flattened_schema.clone()),
        extracted_columns,
    )
    .ok()
}

fn extract_column_by_path(batch: &RecordBatch, path_parts: &[&str]) -> Option<Arc<dyn Array>> {
    if path_parts.is_empty() {
        return None;
    }

    // Find the root field in the batch
    let root_field_name = path_parts[0];
    let root_field_index = batch
        .schema()
        .fields()
        .iter()
        .position(|f| f.name() == root_field_name)?;

    let mut current_column = batch.column(root_field_index);
    let batch_schema = batch.schema();
    let mut current_field = batch_schema.field(root_field_index);

    // Navigate through the path
    for &field_name in &path_parts[1..] {
        // Current field should be a struct
        if let DataType::Struct(struct_fields) = current_field.data_type() {
            let struct_array = current_column.as_any().downcast_ref::<StructArray>()?;

            // Find the field in the struct
            let field_index = struct_fields.iter().position(|f| f.name() == field_name)?;

            current_column = struct_array.column(field_index);
            current_field = &struct_fields[field_index];
        } else {
            return None; // Can't navigate further, not a struct
        }
    }

    Some(Arc::clone(current_column))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::Fields;

    #[test]
    fn test_flatten_schema() {
        // Test flattening schema without creating data
        let name_field = Field::new("name", DataType::Utf8, false);
        let age_field = Field::new("age", DataType::Int32, true);
        let person_fields = Fields::from(vec![name_field, age_field]);
        let person_field = Field::new("person", DataType::Struct(person_fields), false);

        let id_field = Field::new("id", DataType::Int32, false);
        let status_field = Field::new("status", DataType::Utf8, true);

        let schema = Schema::new(vec![id_field, person_field, status_field]);
        let flattened_schema = unnest_struct_schema(&schema, ".");

        assert_eq!(flattened_schema.fields().len(), 4);
        assert_eq!(flattened_schema.field(0).name(), "id");
        assert_eq!(flattened_schema.field(0).data_type(), &DataType::Int32);
        assert!(!flattened_schema.field(0).is_nullable());

        assert_eq!(flattened_schema.field(1).name(), "person.name");
        assert_eq!(flattened_schema.field(1).data_type(), &DataType::Utf8);
        assert!(!flattened_schema.field(1).is_nullable());

        assert_eq!(flattened_schema.field(2).name(), "person.age");
        assert_eq!(flattened_schema.field(2).data_type(), &DataType::Int32);
        assert!(flattened_schema.field(2).is_nullable());

        assert_eq!(flattened_schema.field(3).name(), "status");
        assert_eq!(flattened_schema.field(3).data_type(), &DataType::Utf8);
        assert!(flattened_schema.field(3).is_nullable());
    }

    #[test]
    fn test_map_flattened_to_original_simple() {
        // Test mapping simple flattened field back to original
        let name_field = Field::new("name", DataType::Utf8, false);
        let age_field = Field::new("age", DataType::Int32, false);
        let person_fields = Fields::from(vec![name_field, age_field]);
        let person_field = Field::new("person", DataType::Struct(person_fields), false);
        let id_field = Field::new("id", DataType::Int32, false);

        let schema = Schema::new(vec![id_field, person_field]);

        // Test mapping "person.name"
        let path = map_flattened_to_original("person.name", &schema, ".").expect("path");
        assert_eq!(path.field_names, vec!["person", "name"]);
        assert_eq!(path.field_indices, vec![1, 0]); // person is index 1, name is index 0 within person

        // Test mapping "person.age"
        let path = map_flattened_to_original("person.age", &schema, ".").expect("path");
        assert_eq!(path.field_names, vec!["person", "age"]);
        assert_eq!(path.field_indices, vec![1, 1]); // person is index 1, age is index 1 within person

        // Test mapping top-level field "id"
        let path = map_flattened_to_original("id", &schema, ".").expect("path");
        assert_eq!(path.field_names, vec!["id"]);
        assert_eq!(path.field_indices, vec![0]); // id is index 0
    }

    #[test]
    fn test_map_flattened_to_original_deeply_nested() {
        // Test deeply nested structure: a.b.c
        let c_field = Field::new("c", DataType::Int32, false);
        let b_fields = Fields::from(vec![c_field]);
        let b_field = Field::new("b", DataType::Struct(b_fields), false);
        let a_fields = Fields::from(vec![b_field]);
        let a_field = Field::new("a", DataType::Struct(a_fields), false);

        let schema = Schema::new(vec![a_field]);

        let path = map_flattened_to_original("a.b.c", &schema, ".").expect("path");
        assert_eq!(path.field_names, vec!["a", "b", "c"]);
        assert_eq!(path.field_indices, vec![0, 0, 0]);
    }

    #[test]
    fn test_map_flattened_to_original_complex() {
        // Test the same complex structure used in column_schema_ordering_consistency test
        let a_field = Field::new("a", DataType::Int32, false);

        let e_field = Field::new("e", DataType::Int32, false);
        let f_field = Field::new("f", DataType::Int32, false);
        let d_fields = Fields::from(vec![e_field, f_field]);
        let d_field = Field::new("d", DataType::Struct(d_fields), false);

        let c_field = Field::new("c", DataType::Int32, false);
        let g_field = Field::new("g", DataType::Int32, false);
        let b_fields = Fields::from(vec![c_field, d_field, g_field]);
        let b_field = Field::new("b", DataType::Struct(b_fields), false);

        let h_field = Field::new("h", DataType::Int32, false);

        let schema = Schema::new(vec![a_field, b_field, h_field]);

        // Test various flattened paths
        let path = map_flattened_to_original("a", &schema, ".").expect("path");
        assert_eq!(path.field_names, vec!["a"]);
        assert_eq!(path.field_indices, vec![0]);

        let path = map_flattened_to_original("b.c", &schema, ".").expect("path");
        assert_eq!(path.field_names, vec!["b", "c"]);
        assert_eq!(path.field_indices, vec![1, 0]);

        let path = map_flattened_to_original("b.d.e", &schema, ".").expect("path");
        assert_eq!(path.field_names, vec!["b", "d", "e"]);
        assert_eq!(path.field_indices, vec![1, 1, 0]);

        let path = map_flattened_to_original("b.d.f", &schema, ".").expect("path");
        assert_eq!(path.field_names, vec!["b", "d", "f"]);
        assert_eq!(path.field_indices, vec![1, 1, 1]);

        let path = map_flattened_to_original("b.g", &schema, ".").expect("path");
        assert_eq!(path.field_names, vec!["b", "g"]);
        assert_eq!(path.field_indices, vec![1, 2]);

        let path = map_flattened_to_original("h", &schema, ".").expect("path");
        assert_eq!(path.field_names, vec!["h"]);
        assert_eq!(path.field_indices, vec![2]);
    }

    #[test]
    fn test_map_flattened_to_original_nonexistent() {
        // Test with fields that don't exist
        let name_field = Field::new("name", DataType::Utf8, false);
        let person_fields = Fields::from(vec![name_field]);
        let person_field = Field::new("person", DataType::Struct(person_fields), false);
        let schema = Schema::new(vec![person_field]);

        // Non-existent top-level field
        assert!(map_flattened_to_original("nonexistent", &schema, ".").is_none());

        // Non-existent nested field
        assert!(map_flattened_to_original("person.nonexistent", &schema, ".").is_none());

        // Field that exists but trying to access it as a struct
        assert!(map_flattened_to_original("person.name.something", &schema, ".").is_none());
    }

    #[test]
    fn test_map_flattened_to_original_custom_separator() {
        // Test with custom separator
        let name_field = Field::new("name", DataType::Utf8, false);
        let person_fields = Fields::from(vec![name_field]);
        let person_field = Field::new("person", DataType::Struct(person_fields), false);
        let schema = Schema::new(vec![person_field]);

        let path = map_flattened_to_original("person_name", &schema, "_").expect("path");
        assert_eq!(path.field_names, vec!["person", "name"]);
        assert_eq!(path.field_indices, vec![0, 0]);
    }

    #[test]
    fn test_map_flattened_to_original_empty_input() {
        let schema = Schema::new(vec![Field::new("test", DataType::Int32, false)]);

        // Empty string should return None
        assert!(map_flattened_to_original("", &schema, ".").is_none());
    }

    #[test]
    fn test_nest_struct_schema_simple() {
        // Create a simple flattened schema and reconstruct it
        let name_field = Field::new("name", DataType::Utf8, false);
        let age_field = Field::new("age", DataType::Int32, false);
        let person_fields = Fields::from(vec![name_field, age_field]);
        let person_field = Field::new("person", DataType::Struct(person_fields), false);
        let id_field = Field::new("id", DataType::Int32, false);

        let original_schema = Schema::new(vec![id_field, person_field]);
        let flattened_schema = unnest_struct_schema(&original_schema, "_");
        let reconstructed_schema = nest_struct_schema(&flattened_schema, "_");

        // Check that the reconstructed schema matches the original
        assert_eq!(reconstructed_schema.fields().len(), 2);

        // Check id field
        let id_field = reconstructed_schema.field(0);
        assert_eq!(id_field.name(), "id");
        assert_eq!(id_field.data_type(), &DataType::Int32);
        assert!(!id_field.is_nullable());

        // Check person field
        let person_field = reconstructed_schema.field(1);
        assert_eq!(person_field.name(), "person");
        if let DataType::Struct(nested_fields) = person_field.data_type() {
            assert_eq!(nested_fields.len(), 2);
            assert_eq!(nested_fields[0].name(), "name");
            assert_eq!(nested_fields[0].data_type(), &DataType::Utf8);
            assert_eq!(nested_fields[1].name(), "age");
            assert_eq!(nested_fields[1].data_type(), &DataType::Int32);
        } else {
            panic!("Person field should be a struct");
        }
    }

    #[test]
    fn test_nest_struct_schema_deeply_nested() {
        // Test deeply nested structure reconstruction
        let c_field = Field::new("c", DataType::Int32, false);
        let b_fields = Fields::from(vec![c_field]);
        let b_field = Field::new("b", DataType::Struct(b_fields), false);
        let a_fields = Fields::from(vec![b_field]);
        let a_field = Field::new("a", DataType::Struct(a_fields), false);

        let original_schema = Schema::new(vec![a_field]);
        let flattened_schema = unnest_struct_schema(&original_schema, ".");
        let reconstructed_schema = nest_struct_schema(&flattened_schema, ".");

        // Check that the reconstructed schema matches the original
        assert_eq!(reconstructed_schema.fields().len(), 1);

        let a_field = reconstructed_schema.field(0);
        assert_eq!(a_field.name(), "a");
        if let DataType::Struct(a_fields) = a_field.data_type() {
            assert_eq!(a_fields.len(), 1);
            let b_field = &a_fields[0];
            assert_eq!(b_field.name(), "b");
            if let DataType::Struct(b_fields) = b_field.data_type() {
                assert_eq!(b_fields.len(), 1);
                let c_field = &b_fields[0];
                assert_eq!(c_field.name(), "c");
                assert_eq!(c_field.data_type(), &DataType::Int32);
            } else {
                panic!("B field should be a struct");
            }
        } else {
            panic!("A field should be a struct");
        }
    }

    #[test]
    fn test_nest_struct_schema_mixed_fields() {
        // Test schema with both nested and non-nested fields
        let id_field = Field::new("id", DataType::Int32, false);
        let name_field = Field::new("name", DataType::Utf8, false);
        let age_field = Field::new("age", DataType::Int32, false);
        let person_fields = Fields::from(vec![name_field, age_field]);
        let person_field = Field::new("person", DataType::Struct(person_fields), false);
        let status_field = Field::new("status", DataType::Utf8, false);

        let original_schema = Schema::new(vec![id_field, person_field, status_field]);
        let flattened_schema = unnest_struct_schema(&original_schema, ".");
        let reconstructed_schema = nest_struct_schema(&flattened_schema, ".");

        // Check that the reconstructed schema matches the original
        assert_eq!(reconstructed_schema.fields().len(), 3);

        // Check field names and types
        assert_eq!(reconstructed_schema.field(0).name(), "id");
        assert_eq!(reconstructed_schema.field(0).data_type(), &DataType::Int32);

        assert_eq!(reconstructed_schema.field(1).name(), "person");
        if let DataType::Struct(person_fields) = reconstructed_schema.field(1).data_type() {
            assert_eq!(person_fields.len(), 2);
            assert_eq!(person_fields[0].name(), "name");
            assert_eq!(person_fields[1].name(), "age");
        } else {
            panic!("Person field should be a struct");
        }

        assert_eq!(reconstructed_schema.field(2).name(), "status");
        assert_eq!(reconstructed_schema.field(2).data_type(), &DataType::Utf8);
    }

    #[test]
    fn test_nest_struct_schema_without_metadata() {
        // Test that nesting works even without metadata by using field names directly
        let field = Field::new("person.name", DataType::Utf8, false);
        let schema = Schema::new(vec![field]);

        // This should work since we use field names directly
        let reconstructed = nest_struct_schema(&schema, ".");
        assert_eq!(reconstructed.fields().len(), 1);

        let person_field = reconstructed.field(0);
        assert_eq!(person_field.name(), "person");
        if let DataType::Struct(nested_fields) = person_field.data_type() {
            assert_eq!(nested_fields.len(), 1);
            assert_eq!(nested_fields[0].name(), "name");
            assert_eq!(nested_fields[0].data_type(), &DataType::Utf8);
        } else {
            panic!("Person field should be a struct");
        }
    }

    #[test]
    fn test_nest_struct_custom_separator() {
        // Test with custom separator
        let name_field = Field::new("name", DataType::Utf8, false);
        let person_fields = Fields::from(vec![name_field]);
        let person_field = Field::new("person", DataType::Struct(person_fields), false);

        let original_schema = Schema::new(vec![person_field]);
        let flattened_schema = unnest_struct_schema(&original_schema, "_");
        let reconstructed_schema = nest_struct_schema(&flattened_schema, "_");

        // Check that the reconstructed schema matches the original
        assert_eq!(reconstructed_schema.fields().len(), 1);
        let person_field = reconstructed_schema.field(0);
        assert_eq!(person_field.name(), "person");

        if let DataType::Struct(nested_fields) = person_field.data_type() {
            assert_eq!(nested_fields.len(), 1);
            assert_eq!(nested_fields[0].name(), "name");
        } else {
            panic!("Person field should be a struct");
        }
    }

    #[test]
    fn test_find_col_index_simple() {
        // Test finding column index for simple nested structure
        let name_field = Field::new("name", DataType::Utf8, false);
        let age_field = Field::new("age", DataType::Int32, false);
        let person_fields = Fields::from(vec![name_field, age_field]);
        let person_field = Field::new("person", DataType::Struct(person_fields), false);
        let id_field = Field::new("id", DataType::Int32, false);
        let status_field = Field::new("status", DataType::Utf8, false);

        let nested_schema = Schema::new(vec![id_field, person_field, status_field]);
        let flattened_schema = unnest_struct_schema(&nested_schema, ".");

        // Test id field (top-level) -> column 0
        let id_flattened = flattened_schema.field(0); // "id"
        let col_index =
            find_col_index(id_flattened, &nested_schema, ".").expect("Should find index");
        assert_eq!(col_index, 0);

        // Test person.name field -> column 1 (person struct)
        let person_name_flattened = flattened_schema.field(1); // "person.name"
        let col_index =
            find_col_index(person_name_flattened, &nested_schema, ".").expect("Should find index");
        assert_eq!(col_index, 1);

        // Test person.age field -> column 1 (person struct)
        let person_age_flattened = flattened_schema.field(2); // "person.age"
        let col_index =
            find_col_index(person_age_flattened, &nested_schema, ".").expect("Should find index");
        assert_eq!(col_index, 1);

        // Test status field (top-level) -> column 2
        let status_flattened = flattened_schema.field(3); // "status"
        let col_index =
            find_col_index(status_flattened, &nested_schema, ".").expect("Should find index");
        assert_eq!(col_index, 2);
    }

    #[test]
    fn test_find_col_index_deeply_nested() {
        // Test with deeply nested structure: a.b.c
        let c_field = Field::new("c", DataType::Int32, false);
        let b_fields = Fields::from(vec![c_field]);
        let b_field = Field::new("b", DataType::Struct(b_fields), false);
        let a_fields = Fields::from(vec![b_field]);
        let a_field = Field::new("a", DataType::Struct(a_fields), false);
        let x_field = Field::new("x", DataType::Utf8, false);

        let nested_schema = Schema::new(vec![x_field, a_field]);
        let flattened_schema = unnest_struct_schema(&nested_schema, ".");

        // Test x field -> column 0
        let x_flattened = flattened_schema.field(0); // "x"
        let col_index =
            find_col_index(x_flattened, &nested_schema, ".").expect("Should find index");
        assert_eq!(col_index, 0);

        // Test a.b.c field -> column 1 (a struct)
        let abc_flattened = flattened_schema.field(1); // "a.b.c"
        let col_index =
            find_col_index(abc_flattened, &nested_schema, ".").expect("Should find index");
        assert_eq!(col_index, 1);
    }

    #[test]
    #[expect(clippy::similar_names)]
    fn test_find_col_index_complex_structure() {
        // Test the complex structure from earlier tests
        let a_field = Field::new("a", DataType::Int32, false);

        let e_field = Field::new("e", DataType::Int32, false);
        let f_field = Field::new("f", DataType::Int32, false);
        let d_fields = Fields::from(vec![e_field, f_field]);
        let d_field = Field::new("d", DataType::Struct(d_fields), false);

        let c_field = Field::new("c", DataType::Int32, false);
        let g_field = Field::new("g", DataType::Int32, false);
        let b_fields = Fields::from(vec![c_field, d_field, g_field]);
        let b_field = Field::new("b", DataType::Struct(b_fields), false);

        let h_field = Field::new("h", DataType::Int32, false);

        let nested_schema = Schema::new(vec![a_field, b_field, h_field]);
        let flattened_schema = unnest_struct_schema(&nested_schema, ".");

        // Expected flattened order: ["a", "b.c", "b.d.e", "b.d.f", "b.g", "h"]
        // Test each flattened field
        let a_flattened = flattened_schema.field(0); // "a"
        assert_eq!(
            find_col_index(a_flattened, &nested_schema, ".").expect("Should find"),
            0
        );

        let bc_flattened = flattened_schema.field(1); // "b.c"
        assert_eq!(
            find_col_index(bc_flattened, &nested_schema, ".").expect("Should find"),
            1
        );

        let bde_flattened = flattened_schema.field(2); // "b.d.e"
        assert_eq!(
            find_col_index(bde_flattened, &nested_schema, ".").expect("Should find"),
            1
        );

        let bdf_flattened = flattened_schema.field(3); // "b.d.f"
        assert_eq!(
            find_col_index(bdf_flattened, &nested_schema, ".").expect("Should find"),
            1
        );

        let bg_flattened = flattened_schema.field(4); // "b.g"
        assert_eq!(
            find_col_index(bg_flattened, &nested_schema, ".").expect("Should find"),
            1
        );

        let h_flattened = flattened_schema.field(5); // "h"
        assert_eq!(
            find_col_index(h_flattened, &nested_schema, ".").expect("Should find"),
            2
        );
    }

    #[test]
    fn test_find_col_index_custom_separator() {
        // Test with custom separator
        let name_field = Field::new("name", DataType::Utf8, false);
        let person_fields = Fields::from(vec![name_field]);
        let person_field = Field::new("person", DataType::Struct(person_fields), false);
        let id_field = Field::new("id", DataType::Int32, false);

        let nested_schema = Schema::new(vec![id_field, person_field]);
        let flattened_schema = unnest_struct_schema(&nested_schema, "_");

        // Test person_name field -> column 1
        let person_name_flattened = flattened_schema.field(1); // "person_name"
        let col_index =
            find_col_index(person_name_flattened, &nested_schema, "_").expect("Should find index");
        assert_eq!(col_index, 1);
    }

    #[test]
    fn test_find_col_index_nonexistent_field() {
        // Test with field that doesn't exist in nested schema
        let name_field = Field::new("name", DataType::Utf8, false);
        let person_fields = Fields::from(vec![name_field]);
        let person_field = Field::new("person", DataType::Struct(person_fields), false);

        let nested_schema = Schema::new(vec![person_field]);
        let flattened_schema = unnest_struct_schema(&nested_schema, ".");

        // Get a valid flattened field
        let person_name_flattened = flattened_schema.field(0); // "person.name"

        // But try to find it in a different nested schema that doesn't have "person"
        let different_schema = Schema::new(vec![Field::new("other", DataType::Int32, false)]);

        // Should return None since "person" doesn't exist in different_schema
        assert!(find_col_index(person_name_flattened, &different_schema, ".").is_none());
    }

    #[test]
    fn test_find_col_index_multiple_fields_same_struct() {
        // Test that multiple fields from the same struct return the same column index
        let name_field = Field::new("name", DataType::Utf8, false);
        let age_field = Field::new("age", DataType::Int32, false);
        let email_field = Field::new("email", DataType::Utf8, false);
        let person_fields = Fields::from(vec![name_field, age_field, email_field]);
        let person_field = Field::new("person", DataType::Struct(person_fields), false);
        let id_field = Field::new("id", DataType::Int32, false);

        let nested_schema = Schema::new(vec![id_field, person_field]);
        let flattened_schema = unnest_struct_schema(&nested_schema, ".");

        // All person.* fields should map to column 1
        let person_name = flattened_schema.field(1); // "person.name"
        let person_age = flattened_schema.field(2); // "person.age"
        let person_email = flattened_schema.field(3); // "person.email"

        assert_eq!(
            find_col_index(person_name, &nested_schema, ".").expect("Should find"),
            1
        );
        assert_eq!(
            find_col_index(person_age, &nested_schema, ".").expect("Should find"),
            1
        );
        assert_eq!(
            find_col_index(person_email, &nested_schema, ".").expect("Should find"),
            1
        );
    }

    #[test]
    fn test_project_nested_schema_simple() {
        // Test simple projection with subset of fields
        let name_field = Field::new("name", DataType::Utf8, false);
        let age_field = Field::new("age", DataType::Int32, false);
        let person_fields = Fields::from(vec![name_field, age_field]);
        let person_field = Field::new("person", DataType::Struct(person_fields), false);
        let id_field = Field::new("id", DataType::Int32, false);
        let status_field = Field::new("status", DataType::Utf8, false);

        let original_nested = Schema::new(vec![id_field, person_field, status_field]);
        let full_flattened = unnest_struct_schema(&original_nested, ".");

        // Create subset: only id and status (skip person entirely)
        let subset_fields = vec![
            full_flattened.field(0).clone(), // id
            full_flattened.field(3).clone(), // status
        ];
        let filtered_flattened = Schema::new(subset_fields);

        let projected = project_nested_schema(&filtered_flattened, &original_nested, ".");

        // Should have only id and status fields
        assert_eq!(projected.fields().len(), 2);
        assert_eq!(projected.field(0).name(), "id");
        assert_eq!(projected.field(0).data_type(), &DataType::Int32);
        assert_eq!(projected.field(1).name(), "status");
        assert_eq!(projected.field(1).data_type(), &DataType::Utf8);
    }

    #[test]
    fn test_project_nested_schema_partial_struct() {
        // Test projection with partial struct fields - should return complete original struct
        let name_field = Field::new("name", DataType::Utf8, false);
        let age_field = Field::new("age", DataType::Int32, false);
        let email_field = Field::new("email", DataType::Utf8, false);
        let person_fields = Fields::from(vec![name_field, age_field, email_field]);
        let person_field = Field::new("person", DataType::Struct(person_fields), false);
        let id_field = Field::new("id", DataType::Int32, false);

        let original_nested = Schema::new(vec![id_field, person_field]);
        let full_flattened = unnest_struct_schema(&original_nested, ".");

        // Create subset: id, person.name, person.email (skip person.age)
        let subset_fields = vec![
            full_flattened.field(0).clone(), // id
            full_flattened.field(1).clone(), // person.name
            full_flattened.field(3).clone(), // person.email
        ];
        let filtered_flattened = Schema::new(subset_fields);

        let projected = project_nested_schema(&filtered_flattened, &original_nested, ".");

        // Should have id and complete person{name, age, email}
        assert_eq!(projected.fields().len(), 2);

        assert_eq!(projected.field(0).name(), "id");
        assert_eq!(projected.field(0).data_type(), &DataType::Int32);

        assert_eq!(projected.field(1).name(), "person");
        if let DataType::Struct(person_fields) = projected.field(1).data_type() {
            assert_eq!(person_fields.len(), 3);
            assert_eq!(person_fields[0].name(), "name");
            assert_eq!(person_fields[0].data_type(), &DataType::Utf8);
            assert_eq!(person_fields[1].name(), "age");
            assert_eq!(person_fields[1].data_type(), &DataType::Int32);
            assert_eq!(person_fields[2].name(), "email");
            assert_eq!(person_fields[2].data_type(), &DataType::Utf8);
        } else {
            panic!("Person field should be a struct");
        }
    }

    #[test]
    fn test_project_nested_schema_deeply_nested() {
        // Test projection with deeply nested structures - should return complete original struct
        let c_field = Field::new("c", DataType::Int32, false);
        let d_field = Field::new("d", DataType::Utf8, false);
        let b_fields = Fields::from(vec![c_field, d_field]);
        let b_field = Field::new("b", DataType::Struct(b_fields), false);
        let e_field = Field::new("e", DataType::Float64, false);
        let a_fields = Fields::from(vec![b_field, e_field]);
        let a_field = Field::new("a", DataType::Struct(a_fields), false);
        let x_field = Field::new("x", DataType::Int32, false);

        let original_nested = Schema::new(vec![x_field, a_field]);
        let full_flattened = unnest_struct_schema(&original_nested, ".");

        // Create subset: x, a.b.c (skip a.b.d and a.e)
        let subset_fields = vec![
            full_flattened.field(0).clone(), // x
            full_flattened.field(1).clone(), // a.b.c
        ];
        let filtered_flattened = Schema::new(subset_fields);

        let projected = project_nested_schema(&filtered_flattened, &original_nested, ".");

        // Should have x and complete a{b{c, d}, e}
        assert_eq!(projected.fields().len(), 2);

        assert_eq!(projected.field(0).name(), "x");
        assert_eq!(projected.field(0).data_type(), &DataType::Int32);

        assert_eq!(projected.field(1).name(), "a");
        if let DataType::Struct(a_fields) = projected.field(1).data_type() {
            assert_eq!(a_fields.len(), 2);
            assert_eq!(a_fields[0].name(), "b");
            if let DataType::Struct(b_fields) = a_fields[0].data_type() {
                assert_eq!(b_fields.len(), 2);
                assert_eq!(b_fields[0].name(), "c");
                assert_eq!(b_fields[0].data_type(), &DataType::Int32);
                assert_eq!(b_fields[1].name(), "d");
                assert_eq!(b_fields[1].data_type(), &DataType::Utf8);
            } else {
                panic!("B field should be a struct");
            }
            assert_eq!(a_fields[1].name(), "e");
            assert_eq!(a_fields[1].data_type(), &DataType::Float64);
        } else {
            panic!("A field should be a struct");
        }
    }

    #[test]
    fn test_project_nested_schema_complex() {
        // Test projection with the complex structure from earlier tests - should return complete original struct
        let a_field = Field::new("a", DataType::Int32, false);

        let e_field = Field::new("e", DataType::Int32, false);
        let f_field = Field::new("f", DataType::Int32, false);
        let d_fields = Fields::from(vec![e_field, f_field]);
        let d_field = Field::new("d", DataType::Struct(d_fields), false);

        let c_field = Field::new("c", DataType::Int32, false);
        let g_field = Field::new("g", DataType::Int32, false);
        let b_fields = Fields::from(vec![c_field, d_field, g_field]);
        let b_field = Field::new("b", DataType::Struct(b_fields), false);

        let h_field = Field::new("h", DataType::Int32, false);

        let original_nested = Schema::new(vec![a_field, b_field, h_field]);
        let full_flattened = unnest_struct_schema(&original_nested, ".");

        // Create subset: a, b.c, b.d.f, h (skip b.d.e and b.g)
        let subset_fields = vec![
            full_flattened.field(0).clone(), // a
            full_flattened.field(1).clone(), // b.c
            full_flattened.field(3).clone(), // b.d.f
            full_flattened.field(5).clone(), // h
        ];
        let filtered_flattened = Schema::new(subset_fields);

        let projected = project_nested_schema(&filtered_flattened, &original_nested, ".");

        // Should have a, complete b{c, d{e, f}, g}, h
        assert_eq!(projected.fields().len(), 3);

        assert_eq!(projected.field(0).name(), "a");
        assert_eq!(projected.field(0).data_type(), &DataType::Int32);

        assert_eq!(projected.field(1).name(), "b");
        if let DataType::Struct(b_fields) = projected.field(1).data_type() {
            assert_eq!(b_fields.len(), 3);
            // Fields should preserve original order: c, d, g (as in original schema)
            assert_eq!(b_fields[0].name(), "c");
            assert_eq!(b_fields[0].data_type(), &DataType::Int32);

            assert_eq!(b_fields[1].name(), "d");
            if let DataType::Struct(d_fields) = b_fields[1].data_type() {
                assert_eq!(d_fields.len(), 2);
                assert_eq!(d_fields[0].name(), "e");
                assert_eq!(d_fields[0].data_type(), &DataType::Int32);
                assert_eq!(d_fields[1].name(), "f");
                assert_eq!(d_fields[1].data_type(), &DataType::Int32);
            } else {
                panic!("D field should be a struct");
            }

            assert_eq!(b_fields[2].name(), "g");
            assert_eq!(b_fields[2].data_type(), &DataType::Int32);
        } else {
            panic!("B field should be a struct");
        }

        assert_eq!(projected.field(2).name(), "h");
        assert_eq!(projected.field(2).data_type(), &DataType::Int32);
    }

    #[test]
    fn test_project_nested_schema_empty() {
        // Test projection with no matching fields
        let name_field = Field::new("name", DataType::Utf8, false);
        let person_fields = Fields::from(vec![name_field]);
        let person_field = Field::new("person", DataType::Struct(person_fields), false);
        let id_field = Field::new("id", DataType::Int32, false);

        let original_nested = Schema::new(vec![id_field, person_field]);

        // Create empty filtered schema
        let filtered_flattened = Schema::new(Vec::<Field>::new());

        let projected = project_nested_schema(&filtered_flattened, &original_nested, ".");

        // Should be empty
        assert_eq!(projected.fields().len(), 0);
    }

    #[test]
    fn test_project_nested_schema_full() {
        // Test projection with all fields (should match original)
        let name_field = Field::new("name", DataType::Utf8, false);
        let age_field = Field::new("age", DataType::Int32, false);
        let person_fields = Fields::from(vec![name_field, age_field]);
        let person_field = Field::new("person", DataType::Struct(person_fields), false);
        let id_field = Field::new("id", DataType::Int32, false);

        let original_nested = Schema::new(vec![id_field, person_field]);
        let full_flattened = unnest_struct_schema(&original_nested, ".");

        let projected = project_nested_schema(&full_flattened, &original_nested, ".");

        // Should match the original structure
        assert_eq!(projected.fields().len(), 2);

        assert_eq!(projected.field(0).name(), "id");
        assert_eq!(projected.field(0).data_type(), &DataType::Int32);

        assert_eq!(projected.field(1).name(), "person");
        if let DataType::Struct(person_fields) = projected.field(1).data_type() {
            assert_eq!(person_fields.len(), 2);
            assert_eq!(person_fields[0].name(), "name");
            assert_eq!(person_fields[0].data_type(), &DataType::Utf8);
            assert_eq!(person_fields[1].name(), "age");
            assert_eq!(person_fields[1].data_type(), &DataType::Int32);
        } else {
            panic!("Person field should be a struct");
        }
    }

    #[test]
    fn test_project_nested_schema_custom_separator() {
        // Test projection with custom separator - should return complete original struct
        let name_field = Field::new("name", DataType::Utf8, false);
        let age_field = Field::new("age", DataType::Int32, false);
        let person_fields = Fields::from(vec![name_field, age_field]);
        let person_field = Field::new("person", DataType::Struct(person_fields), false);
        let id_field = Field::new("id", DataType::Int32, false);

        let original_nested = Schema::new(vec![id_field, person_field]);
        let full_flattened = unnest_struct_schema(&original_nested, "_");

        // Create subset: only id and person_name
        let subset_fields = vec![
            full_flattened.field(0).clone(), // id
            full_flattened.field(1).clone(), // person_name
        ];
        let filtered_flattened = Schema::new(subset_fields);

        let projected = project_nested_schema(&filtered_flattened, &original_nested, "_");

        // Should have id and complete person{name, age}
        assert_eq!(projected.fields().len(), 2);

        assert_eq!(projected.field(0).name(), "id");
        assert_eq!(projected.field(0).data_type(), &DataType::Int32);

        assert_eq!(projected.field(1).name(), "person");
        if let DataType::Struct(person_fields) = projected.field(1).data_type() {
            assert_eq!(person_fields.len(), 2);
            assert_eq!(person_fields[0].name(), "name");
            assert_eq!(person_fields[0].data_type(), &DataType::Utf8);
            assert_eq!(person_fields[1].name(), "age");
            assert_eq!(person_fields[1].data_type(), &DataType::Int32);
        } else {
            panic!("Person field should be a struct");
        }
    }

    #[test]
    fn test_project_nested_schema_fields_without_metadata() {
        // Test projection with fields that don't have metadata (should be ignored) - should return complete original struct
        let name_field = Field::new("name", DataType::Utf8, false);
        let person_fields = Fields::from(vec![name_field]);
        let person_field = Field::new("person", DataType::Struct(person_fields), false);
        let id_field = Field::new("id", DataType::Int32, false);

        let original_nested = Schema::new(vec![id_field, person_field]);
        let full_flattened = unnest_struct_schema(&original_nested, ".");

        // Create a mix of fields with and without metadata
        let field_without_metadata = Field::new("orphan", DataType::Float32, false);
        let subset_fields = vec![
            full_flattened.field(0).clone(), // id (with metadata)
            field_without_metadata,          // orphan (without metadata)
            full_flattened.field(1).clone(), // person.name (with metadata)
        ];
        let filtered_flattened = Schema::new(subset_fields);

        let projected = project_nested_schema(&filtered_flattened, &original_nested, ".");

        // Should only include fields that have metadata and exist in original
        assert_eq!(projected.fields().len(), 2);

        assert_eq!(projected.field(0).name(), "id");
        assert_eq!(projected.field(0).data_type(), &DataType::Int32);

        assert_eq!(projected.field(1).name(), "person");
        if let DataType::Struct(person_fields) = projected.field(1).data_type() {
            assert_eq!(person_fields.len(), 1);
            assert_eq!(person_fields[0].name(), "name");
            assert_eq!(person_fields[0].data_type(), &DataType::Utf8);
        } else {
            panic!("Person field should be a struct");
        }
    }

    #[test]
    fn test_project_nested_schema_preserves_original_ordering() {
        // Test that field ordering follows the original schema order
        let z_field = Field::new("z", DataType::Int32, false);
        let b_field = Field::new("b", DataType::Utf8, false);
        let a_field = Field::new("a", DataType::Float64, false);

        let original_nested = Schema::new(vec![z_field, b_field, a_field]);
        let full_flattened = unnest_struct_schema(&original_nested, ".");

        // Create subset in different order: a, z (skip b)
        let subset_fields = vec![
            full_flattened.field(2).clone(), // a
            full_flattened.field(0).clone(), // z
        ];
        let filtered_flattened = Schema::new(subset_fields);

        let projected = project_nested_schema(&filtered_flattened, &original_nested, ".");

        // Should preserve original schema ordering: z, a (not filtered a, z)
        assert_eq!(projected.fields().len(), 2);
        assert_eq!(projected.field(0).name(), "z");
        assert_eq!(projected.field(1).name(), "a");
    }

    #[test]
    fn test_extract_flattened_from_nested() {
        // Test extracting columns from a nested RecordBatch
        let name_field = Field::new("name", DataType::Utf8, false);
        let age_field = Field::new("age", DataType::Int32, false);
        let email_field = Field::new("email", DataType::Utf8, false);
        let person_fields = Fields::from(vec![name_field, age_field, email_field]);
        let person_field = Field::new("person", DataType::Struct(person_fields.clone()), false);
        let id_field = Field::new("id", DataType::Int32, false);
        let status_field = Field::new("status", DataType::Utf8, false);

        // Create a nested RecordBatch
        let id_array = Arc::new(Int32Array::from(vec![1, 2]));
        let name_array = Arc::new(StringArray::from(vec!["John", "Jane"]));
        let age_array = Arc::new(Int32Array::from(vec![30, 25]));
        let email_array = Arc::new(StringArray::from(vec![
            "john@example.com",
            "jane@example.com",
        ]));
        let status_array = Arc::new(StringArray::from(vec!["active", "inactive"]));

        let person_array = Arc::new(StructArray::from(vec![
            (Arc::clone(&person_fields[0]), name_array as Arc<dyn Array>),
            (Arc::clone(&person_fields[1]), age_array as Arc<dyn Array>),
            (Arc::clone(&person_fields[2]), email_array as Arc<dyn Array>),
        ]));

        let nested_schema = Arc::new(Schema::new(vec![
            id_field.clone(),
            person_field.clone(),
            status_field.clone(),
        ]));
        let nested_batch = RecordBatch::try_new(
            nested_schema,
            vec![
                id_array as Arc<dyn Array>,
                person_array as Arc<dyn Array>,
                status_array as Arc<dyn Array>,
            ],
        )
        .expect("Failed to create nested RecordBatch");

        // Create a filtered flattened schema with proper metadata
        let original_nested_schema = Schema::new(vec![id_field, person_field, status_field]);
        let full_flattened_schema = unnest_struct_schema(&original_nested_schema, ".");

        // Create subset: id, person.name, status
        let filtered_flattened_schema = Schema::new(vec![
            full_flattened_schema.field(0).clone(), // id
            full_flattened_schema.field(1).clone(), // person.name
            full_flattened_schema.field(4).clone(), // status
        ]);

        // Extract columns from the nested RecordBatch
        let extracted_batch =
            extract_flattened_from_nested(&nested_batch, &filtered_flattened_schema, ".")
                .expect("Should extract columns");

        // Check the extracted batch
        assert_eq!(extracted_batch.num_columns(), 3);
        assert_eq!(extracted_batch.schema().field(0).name(), "id");
        assert_eq!(extracted_batch.schema().field(1).name(), "person.name");
        assert_eq!(extracted_batch.schema().field(2).name(), "status");

        // Check data values
        let id_col = extracted_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("ID column should be Int32Array");
        assert_eq!(id_col.value(0), 1);
        assert_eq!(id_col.value(1), 2);

        let name_col = extracted_batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Person name should be StringArray");
        assert_eq!(name_col.value(0), "John");
        assert_eq!(name_col.value(1), "Jane");

        let status_col = extracted_batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Status should be StringArray");
        assert_eq!(status_col.value(0), "active");
        assert_eq!(status_col.value(1), "inactive");
    }

    #[test]
    fn test_flatten_json_obj_simple() {
        // Test basic flattening with a simple nested object
        let nested = serde_json::json!({
            "person": {
                "name": "John",
                "age": 30
            },
            "status": "active"
        });

        let flattened = flatten_json_obj(&nested, ".");

        // Expected: {"person.name": "John", "person.age": 30, "status": "active"}
        let expected = serde_json::json!({
            "person.name": "John",
            "person.age": 30,
            "status": "active"
        });

        assert_eq!(flattened, expected);
    }

    #[test]
    fn test_flatten_json_obj_deeply_nested() {
        // Test with deeply nested objects
        let nested = serde_json::json!({
            "user": {
                "details": {
                    "profile": {
                        "name": "Alice",
                        "contact": {
                            "email": "alice@example.com",
                            "phone": "555-1234"
                        }
                    }
                },
                "settings": {
                    "theme": "dark"
                }
            },
            "id": 123
        });

        let flattened = flatten_json_obj(&nested, ".");

        // Expected flattened structure
        let expected = serde_json::json!({
            "user.details.profile.name": "Alice",
            "user.details.profile.contact.email": "alice@example.com",
            "user.details.profile.contact.phone": "555-1234",
            "user.settings.theme": "dark",
            "id": 123
        });

        assert_eq!(flattened, expected);
    }

    #[test]
    fn test_flatten_json_obj_arrays() {
        // Test that arrays are preserved as values, not flattened
        let nested = serde_json::json!({
            "user": {
                "name": "Bob",
                "hobbies": ["reading", "hiking", "coding"],
                "scores": [85, 92, 78]
            },
            "tags": ["important", "urgent"]
        });

        let flattened = flatten_json_obj(&nested, ".");

        // Expected: arrays should be preserved as is
        let expected = serde_json::json!({
            "user.name": "Bob",
            "user.hobbies": ["reading", "hiking", "coding"],
            "user.scores": [85, 92, 78],
            "tags": ["important", "urgent"]
        });

        assert_eq!(flattened, expected);
    }

    #[test]
    fn test_flatten_json_obj_custom_delimiter() {
        // Test flattening with custom delimiter
        let nested = serde_json::json!({
            "person": {
                "name": "John",
                "age": 30
            },
            "status": "active"
        });

        let flattened = flatten_json_obj(&nested, "_");

        // Expected: {"person_name": "John", "person_age": 30, "status": "active"}
        let expected = serde_json::json!({
            "person_name": "John",
            "person_age": 30,
            "status": "active"
        });

        assert_eq!(flattened, expected);
    }

    #[test]
    fn test_flatten_json_obj_empty_objects() {
        // Test with empty objects
        let nested = serde_json::json!({
            "user": {
                "details": {}
            },
            "metadata": {}
        });

        let flattened = flatten_json_obj(&nested, ".");

        // Empty objects should result in no keys
        let expected = serde_json::json!({});

        assert_eq!(flattened, expected);
    }

    #[test]
    fn test_flatten_json_obj_null_values() {
        // Test with null values
        let nested = serde_json::json!({
            "user": {
                "name": "Charlie",
                "email": null,
                "details": {
                    "age": null
                }
            },
            "active": null
        });

        let flattened = flatten_json_obj(&nested, ".");

        // Null values should be preserved
        let expected = serde_json::json!({
            "user.name": "Charlie",
            "user.email": null,
            "user.details.age": null,
            "active": null
        });

        assert_eq!(flattened, expected);
    }

    #[test]
    fn test_flatten_json_obj_special_characters() {
        // Test with keys containing special characters
        let nested = serde_json::json!({
            "user-info": {
                "first.name": "John",
                "last_name": "Doe"
            },
            "meta.data": {
                "key-value": 123
            }
        });

        let flattened = flatten_json_obj(&nested, ":");

        // Special characters in keys should be preserved
        let expected = serde_json::json!({
            "user-info:first.name": "John",
            "user-info:last_name": "Doe",
            "meta.data:key-value": 123
        });

        assert_eq!(flattened, expected);
    }
}
