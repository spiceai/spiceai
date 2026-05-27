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

use arrow::datatypes::{Field, Schema, SchemaRef};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Merges `inferred` with `declared` when `declared` is present.
/// Returns `inferred` unchanged when `declared` is `None`.
#[must_use]
pub fn merge_inferred_with_declared(
    inferred: SchemaRef,
    declared: Option<&SchemaRef>,
) -> SchemaRef {
    match declared {
        Some(d) => merge_with_declared(&inferred, d),
        None => inferred,
    }
}

/// Merges an inferred Arrow schema with a declared schema.
///
/// Declared fields take precedence: where both schemas contain a field with the same
/// name, the declared type and nullability are used. Fields present only in the
/// inferred schema are kept unchanged. Fields present only in the declared schema
/// are appended after the inferred fields.
#[must_use]
pub fn merge_with_declared(inferred: &SchemaRef, declared: &SchemaRef) -> SchemaRef {
    let declared_by_name: HashMap<&str, &Field> = declared
        .fields()
        .iter()
        .map(|f| (f.name().as_str(), f.as_ref()))
        .collect();

    let inferred_names: HashSet<&str> = inferred
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();

    let mut merged: Vec<Field> = inferred
        .fields()
        .iter()
        .map(|f| {
            declared_by_name
                .get(f.name().as_str())
                .copied()
                .cloned()
                .unwrap_or_else(|| f.as_ref().clone())
        })
        .collect();

    for f in declared.fields() {
        if !inferred_names.contains(f.name().as_str()) {
            merged.push(f.as_ref().clone());
        }
    }

    Arc::new(Schema::new(merged))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;

    fn schema(fields: &[(&str, DataType, bool)]) -> SchemaRef {
        Arc::new(Schema::new(
            fields
                .iter()
                .map(|(name, dt, nullable)| Field::new(*name, dt.clone(), *nullable))
                .collect::<Vec<_>>(),
        ))
    }

    #[test]
    fn declared_type_overrides_inferred_for_matching_column() {
        let inferred = schema(&[("id", DataType::Int64, true)]);
        let declared = schema(&[("id", DataType::Utf8, false)]);

        let result = merge_with_declared(&inferred, &declared);

        assert_eq!(result.fields().len(), 1);
        let f = result.field(0);
        assert_eq!(f.name(), "id");
        assert_eq!(f.data_type(), &DataType::Utf8);
        assert!(!f.is_nullable());
    }

    #[test]
    fn inferred_only_fields_are_kept() {
        let inferred = schema(&[
            ("id", DataType::Int64, true),
            ("extra", DataType::Boolean, true),
        ]);
        let declared = schema(&[("id", DataType::Utf8, false)]);

        let result = merge_with_declared(&inferred, &declared);

        assert_eq!(result.fields().len(), 2);
        // "extra" is only in inferred — kept as-is
        let extra = result.field_with_name("extra").expect("extra field");
        assert_eq!(extra.data_type(), &DataType::Boolean);
    }

    #[test]
    fn declared_only_fields_are_appended_after_inferred() {
        let inferred = schema(&[("a", DataType::Int64, true)]);
        let declared = schema(&[("b", DataType::Utf8, false)]);

        let result = merge_with_declared(&inferred, &declared);

        assert_eq!(result.fields().len(), 2);
        assert_eq!(result.field(0).name(), "a"); // inferred first
        assert_eq!(result.field(1).name(), "b"); // declared-only appended
    }

    #[test]
    fn inferred_field_order_is_preserved() {
        let inferred = schema(&[
            ("c", DataType::Int64, true),
            ("a", DataType::Int64, true),
            ("b", DataType::Int64, true),
        ]);
        let declared = schema(&[("a", DataType::Utf8, false)]);

        let result = merge_with_declared(&inferred, &declared);

        let names: Vec<&str> = result.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["c", "a", "b"]);
    }

    #[test]
    fn declared_only_fields_appended_in_declared_order() {
        let inferred = schema(&[("x", DataType::Int64, true)]);
        let declared = schema(&[
            ("z", DataType::Utf8, false),
            ("y", DataType::Boolean, false),
        ]);

        let result = merge_with_declared(&inferred, &declared);

        let names: Vec<&str> = result.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["x", "z", "y"]);
    }

    #[test]
    fn empty_inferred_returns_declared_fields() {
        let inferred = schema(&[]);
        let declared = schema(&[
            ("id", DataType::Int64, false),
            ("name", DataType::Utf8, true),
        ]);

        let result = merge_with_declared(&inferred, &declared);

        assert_eq!(result.fields().len(), 2);
        assert_eq!(result.field(0).name(), "id");
        assert_eq!(result.field(1).name(), "name");
    }

    #[test]
    fn empty_declared_returns_inferred_unchanged() {
        let inferred = schema(&[("id", DataType::Int64, true)]);
        let declared = schema(&[]);

        let result = merge_with_declared(&inferred, &declared);

        assert_eq!(result.fields().len(), 1);
        assert_eq!(result.field(0).data_type(), &DataType::Int64);
    }

    #[test]
    fn both_empty_returns_empty_schema() {
        let result = merge_with_declared(&schema(&[]), &schema(&[]));
        assert_eq!(result.fields().len(), 0);
    }

    #[test]
    fn opt_none_returns_inferred_unchanged() {
        let inferred = schema(&[("id", DataType::Int64, true)]);
        let result = merge_inferred_with_declared(Arc::clone(&inferred), None);
        assert!(Arc::ptr_eq(&result, &inferred));
    }

    #[test]
    fn opt_some_delegates_to_merge_with_declared() {
        let inferred = schema(&[("id", DataType::Int64, true)]);
        let declared = schema(&[("id", DataType::Utf8, false)]);

        let result = merge_inferred_with_declared(inferred, Some(&declared));

        assert_eq!(result.field(0).data_type(), &DataType::Utf8);
    }
}
