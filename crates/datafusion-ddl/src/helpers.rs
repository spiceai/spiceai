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

//! Generic helpers shared across DDL integrations (Cayenne, Iceberg, etc.).

use std::collections::HashSet;
use std::sync::{RwLock, Weak};

use arrow::datatypes::Schema;
use datafusion::common::{Constraint, DFSchemaRef};
use datafusion::sql::sqlparser::ast::{CreateTable, CreateTableOptions, SqlOption};

/// Creates the shared output schema for DDL result nodes — a single `result: Utf8` column.
///
/// # Panics
///
/// Panics only if the fixed schema cannot be constructed, which is a compile-time invariant.
#[must_use]
pub fn ddl_output_schema() -> DFSchemaRef {
    use arrow::datatypes::{DataType, Field};
    DFSchemaRef::new(
        datafusion::common::DFSchema::try_from(Schema::new(vec![Field::new(
            "result",
            DataType::Utf8,
            false,
        )]))
        .unwrap_or_else(|e| unreachable!("fixed DDL output schema must be valid: {e}")),
    )
}

/// Split a possibly-qualified schema name `"catalog.schema"` into `(catalog, schema)`.
///
/// When the name contains no `.`, the `default_catalog` is used.
#[must_use]
pub fn parse_qualified_schema_name(name: &str, default_catalog: &str) -> (String, String) {
    match name.split_once('.') {
        Some((catalog, schema)) => (catalog.to_string(), schema.to_string()),
        None => (default_catalog.to_string(), name.to_string()),
    }
}

/// Extract primary key column names from `DataFusion` [`Constraints`] using the Arrow schema
/// to resolve column indices to names.
#[must_use]
pub fn extract_primary_key_columns(
    constraints: &datafusion::common::Constraints,
    arrow_schema: &Schema,
) -> Vec<String> {
    constraints
        .iter()
        .find_map(|c| {
            if let Constraint::PrimaryKey(indices) = c {
                Some(indices)
            } else {
                None
            }
        })
        .map(|indices| {
            let fields = arrow_schema.fields();
            indices
                .iter()
                .filter_map(|&idx| fields.get(idx).map(|f| f.name().clone()))
                .collect()
        })
        .unwrap_or_default()
}

/// Returns `true` if `catalog_name` is present in the DDL-enabled catalog set.
///
/// Gracefully returns `false` if the `Weak` reference has been dropped.
#[must_use]
pub fn is_ddl_enabled<S: ::std::hash::BuildHasher>(
    ddl_enabled_catalogs: &Weak<RwLock<HashSet<String, S>>>,
    catalog_name: &str,
) -> bool {
    ddl_enabled_catalogs
        .upgrade()
        .and_then(|catalogs| catalogs.read().ok().map(|set| set.contains(catalog_name)))
        .unwrap_or(false)
}

/// Returns `true` if the `CREATE TABLE` AST node contains extensions that Spice
/// intercepts before handing to `DataFusion`:
/// - A `PARTITION BY` clause, or
/// - `WITH (...)` options whose keys start with `acceleration.` or `dataset.`
#[must_use]
pub fn has_ddl_extensions(ct: &CreateTable) -> bool {
    if ct.partition_by.is_some() {
        return true;
    }
    if let CreateTableOptions::With(options) = &ct.table_options {
        return options.iter().any(|opt| {
            if let SqlOption::KeyValue { key, .. } = opt {
                key.value.starts_with("acceleration.") || key.value.starts_with("dataset.")
            } else {
                false
            }
        });
    }
    false
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field};
    use datafusion::common::{Constraint, Constraints};

    use super::*;

    fn test_schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ])
    }

    #[test]
    fn parse_qualified_schema_name_splits_catalog_and_schema() {
        let (cat, sch) = parse_qualified_schema_name("mycatalog.myschema", "spice");
        assert_eq!(cat, "mycatalog");
        assert_eq!(sch, "myschema");
    }

    #[test]
    fn parse_qualified_schema_name_uses_default_when_unqualified() {
        let (cat, sch) = parse_qualified_schema_name("myschema", "spice");
        assert_eq!(cat, "spice");
        assert_eq!(sch, "myschema");
    }

    #[test]
    fn extract_primary_key_single_column() {
        let schema = test_schema();
        let constraints = Constraints::new_unverified(vec![Constraint::PrimaryKey(vec![0])]);
        assert_eq!(
            extract_primary_key_columns(&constraints, &schema),
            vec!["id"]
        );
    }

    #[test]
    fn extract_primary_key_composite() {
        let schema = test_schema();
        let constraints = Constraints::new_unverified(vec![Constraint::PrimaryKey(vec![0, 1])]);
        assert_eq!(
            extract_primary_key_columns(&constraints, &schema),
            vec!["id", "name"]
        );
    }

    #[test]
    fn extract_primary_key_empty_when_no_constraints() {
        let schema = test_schema();
        let constraints = Constraints::new_unverified(vec![]);
        assert!(extract_primary_key_columns(&constraints, &schema).is_empty());
    }

    #[test]
    fn is_ddl_enabled_returns_false_for_dropped_weak() {
        let weak: Weak<RwLock<HashSet<String>>> = Weak::new();
        assert!(!is_ddl_enabled(&weak, "any"));
    }

    #[test]
    fn is_ddl_enabled_returns_true_when_present() {
        let set = Arc::new(RwLock::new(HashSet::from(["mycatalog".to_string()])));
        let weak = Arc::downgrade(&set);
        assert!(is_ddl_enabled(&weak, "mycatalog"));
        assert!(!is_ddl_enabled(&weak, "other"));
    }
}
