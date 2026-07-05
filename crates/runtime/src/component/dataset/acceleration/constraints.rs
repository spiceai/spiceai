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

use super::{Acceleration, IndexType};
use crate::component::dataset;
use arrow::datatypes::SchemaRef;
use datafusion::common::{Constraint, Constraints};
use std::{collections::HashMap, fmt::Display};

impl Acceleration {
    #[must_use]
    pub fn hashmap_to_option_string<K, V>(map: &HashMap<K, V>) -> String
    where
        K: Display,
        V: Display,
    {
        map.iter()
            .map(|(k, v)| format!("{k}:{v}"))
            .collect::<Vec<String>>()
            .join(";")
    }

    fn valid_columns(schema: &SchemaRef) -> String {
        schema
            .flattened_fields()
            .into_iter()
            .map(|f| f.name().clone())
            .collect::<Vec<_>>()
            .join(", ")
    }

    pub fn validate_indexes(&self, schema: &SchemaRef) -> dataset::Result<()> {
        if !self.indexes.is_empty() {
            Self::ensure_schema_populated(schema, "indexes")?;
        }
        for column in self.indexes.keys() {
            for index_column in column.iter() {
                if schema.field_with_name(index_column).is_err() {
                    return dataset::IndexColumnNotFoundSnafu {
                        index: index_column.to_string(),
                        valid_columns: Self::valid_columns(schema),
                    }
                    .fail();
                }
            }
        }

        Ok(())
    }

    pub fn validate_primary_key(&self, schema: &SchemaRef) -> dataset::Result<()> {
        if let Some(columns) = &self.primary_key {
            Self::ensure_schema_populated(schema, "a primary key")?;
            for column in columns.iter() {
                if schema.field_with_name(column).is_err() {
                    return dataset::PrimaryKeyColumnNotFoundSnafu {
                        invalid_column: column.to_string(),
                        valid_columns: Self::valid_columns(schema),
                    }
                    .fail();
                }
            }
        }

        Ok(())
    }

    /// A configured constraint (primary key or index) can only be validated against a schema
    /// that actually has columns. An empty schema means the source table could not be resolved
    /// (e.g. it does not exist), so report that root cause instead of a misleading
    /// "column was not found. Valid columns: " message listing no valid columns.
    fn ensure_schema_populated(schema: &SchemaRef, constraint: &str) -> dataset::Result<()> {
        if schema.fields().is_empty() {
            return dataset::AcceleratedSchemaEmptySnafu {
                constraint: constraint.to_string(),
            }
            .fail();
        }
        Ok(())
    }

    #[expect(clippy::needless_pass_by_value)]
    pub fn table_constraints(&self, schema: SchemaRef) -> dataset::Result<Option<Constraints>> {
        if self.indexes.is_empty() && self.primary_key.is_none() {
            tracing::trace!(
                "No indexes or primary key identified for accelerator table constraints",
            );
            return Ok(None);
        }

        tracing::trace!("Primary key definition: {:?}", self.primary_key);
        tracing::trace!("Indexes: {:?}", self.indexes);

        let mut table_constraints: Vec<Constraint> = Vec::new();

        for (column, index_type) in &self.indexes {
            match index_type {
                IndexType::Enabled => {}
                IndexType::Unique => {
                    let index_indices: Vec<usize> = column
                        .iter()
                        .filter_map(|c| schema.index_of(c).ok())
                        .collect();
                    let tc = Constraint::Unique(index_indices);

                    table_constraints.push(tc);
                }
            }
        }

        if let Some(primary_key) = &self.primary_key {
            let pk_indices: Vec<usize> = primary_key
                .iter()
                .filter_map(|c| schema.index_of(c).ok())
                .collect();
            let tc = Constraint::PrimaryKey(pk_indices);

            table_constraints.push(tc);
        }

        tracing::trace!("Table constraints: {table_constraints:?}");

        Ok(Some(Constraints::new_unverified(table_constraints)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::component::dataset::Error;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_table_providers::util::column_reference::ColumnReference;
    use std::sync::Arc;

    fn empty_schema() -> SchemaRef {
        Arc::new(Schema::empty())
    }

    fn schema_with(columns: &[&str]) -> SchemaRef {
        Arc::new(Schema::new(
            columns
                .iter()
                .map(|c| Field::new(*c, DataType::Int32, false))
                .collect::<Vec<_>>(),
        ))
    }

    fn acceleration_with_primary_key(pk: &str) -> Acceleration {
        Acceleration {
            primary_key: Some(ColumnReference::try_from(pk).expect("valid column reference")),
            ..Acceleration::default()
        }
    }

    fn acceleration_with_index(index: &str) -> Acceleration {
        let mut indexes = HashMap::new();
        indexes.insert(
            ColumnReference::try_from(index).expect("valid column reference"),
            IndexType::Enabled,
        );
        Acceleration {
            indexes,
            ..Acceleration::default()
        }
    }

    // Regression test for #10920: when the source table does not exist, the schema resolves to
    // zero columns. The primary-key validation must report the empty schema (missing table) as
    // the root cause, not a misleading "Primary key column '...' was not found. Valid columns: ".
    #[test]
    fn empty_schema_with_primary_key_reports_empty_schema_not_missing_column() {
        let acceleration = acceleration_with_primary_key("marker_id");
        let err = acceleration
            .validate_primary_key(&empty_schema())
            .expect_err("empty schema with a primary key must fail");

        assert!(
            matches!(err, Error::AcceleratedSchemaEmpty { .. }),
            "expected AcceleratedSchemaEmpty, got: {err}"
        );
        let msg = err.to_string();
        assert!(
            msg.contains("no columns") && msg.contains("does not exist"),
            "message should point at the empty schema / missing table: {msg}"
        );
        assert!(
            !msg.contains("was not found in the schema"),
            "message should not be the misleading column-not-found error: {msg}"
        );
    }

    #[test]
    fn empty_schema_with_index_reports_empty_schema_not_missing_column() {
        let acceleration = acceleration_with_index("marker_id");
        let err = acceleration
            .validate_indexes(&empty_schema())
            .expect_err("empty schema with an index must fail");

        assert!(
            matches!(err, Error::AcceleratedSchemaEmpty { .. }),
            "expected AcceleratedSchemaEmpty, got: {err}"
        );
    }

    // An empty schema is only an error when a constraint is actually configured against it.
    #[test]
    fn empty_schema_without_constraints_is_ok() {
        let acceleration = Acceleration::default();
        acceleration
            .validate_primary_key(&empty_schema())
            .expect("no primary key configured, so an empty schema is fine");
        acceleration
            .validate_indexes(&empty_schema())
            .expect("no indexes configured, so an empty schema is fine");
    }

    // A populated schema that is simply missing the configured column must keep the existing,
    // more specific "column was not found" error (behavior preserved).
    #[test]
    fn populated_schema_missing_primary_key_column_keeps_column_not_found_error() {
        let acceleration = acceleration_with_primary_key("marker_id");
        let err = acceleration
            .validate_primary_key(&schema_with(&["id", "value"]))
            .expect_err("missing primary key column must fail");

        assert!(
            matches!(err, Error::PrimaryKeyColumnNotFound { .. }),
            "expected PrimaryKeyColumnNotFound, got: {err}"
        );
        assert!(err.to_string().contains("was not found in the schema"));
    }

    #[test]
    fn populated_schema_with_primary_key_column_is_ok() {
        let acceleration = acceleration_with_primary_key("marker_id");
        acceleration
            .validate_primary_key(&schema_with(&["marker_id", "value"]))
            .expect("primary key column present, so validation passes");
    }
}
