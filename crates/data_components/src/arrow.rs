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

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::{
    catalog::{Session, TableProviderFactory},
    common::Constraint,
    datasource::TableProvider,
    error::{DataFusionError, Result as DataFusionResult},
    logical_expr::CreateExternalTable,
};
use datafusion_table_providers::util::on_conflict::OnConflict;
use std::sync::Arc;

use crate::delete::DeletionTableProviderAdapter;

use self::write::MemTable;

pub mod indexed;
pub mod struct_builder;
pub mod write;

pub use indexed::IndexedMemTable;

#[derive(Debug)]
pub struct ArrowFactory {}

impl ArrowFactory {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for ArrowFactory {
    fn default() -> Self {
        Self::new()
    }
}

/// Extracts primary key column names from constraints.
fn extract_primary_key_columns(
    constraints: &datafusion::common::Constraints,
    schema: &arrow::datatypes::Schema,
) -> Vec<String> {
    for constraint in constraints.iter() {
        if let Constraint::PrimaryKey(indices) = constraint {
            return indices
                .iter()
                .map(|&idx| schema.field(idx).name().clone())
                .collect();
        }
    }
    Vec::new()
}

#[async_trait]
impl TableProviderFactory for ArrowFactory {
    async fn create(
        &self,
        _state: &dyn Session,
        cmd: &CreateExternalTable,
    ) -> DataFusionResult<Arc<dyn TableProvider>> {
        let schema: SchemaRef = Arc::new(cmd.schema.as_arrow().clone());

        // Extract primary key columns for hash index
        let primary_key_columns = extract_primary_key_columns(&cmd.constraints, &schema);

        // Hash index is disabled by default. Must be explicitly enabled with hash_index=enabled.
        // When enabled, a primary_key must be specified.
        let enable_hash_index = cmd
            .options
            .get("hash_index")
            .is_some_and(|v| v.eq_ignore_ascii_case("enabled"));

        // If hash index is enabled, use IndexedMemTable (requires primary key)
        if enable_hash_index {
            if primary_key_columns.is_empty() {
                return Err(DataFusionError::Configuration(
                    "hash_index requires a primary_key to be specified".to_string(),
                ));
            }
            let indexed_table =
                IndexedMemTable::try_new(Arc::clone(&schema), vec![], primary_key_columns)?;

            // Apply constraints
            let indexed_table = indexed_table
                .try_with_constraints(cmd.constraints.clone())
                .await?;

            // Apply on_conflict if specified
            let indexed_table = if let Some(on_conflict_str) = cmd.options.get("on_conflict") {
                indexed_table.with_on_conflict(
                    OnConflict::try_from(on_conflict_str.as_str()).map_err(|e| {
                        DataFusionError::External(format!("Error parsing on_conflict: {e}").into())
                    })?,
                )
            } else {
                indexed_table
            };

            // Apply sort_columns if specified
            let indexed_table = if let Some(sort_cols_str) = cmd.options.get("sort_columns") {
                let sort_columns: Vec<String> = sort_cols_str
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();

                if sort_columns.is_empty() {
                    indexed_table
                } else {
                    indexed_table.with_sort_columns(sort_columns)
                }
            } else {
                indexed_table
            };

            let delete_adapter = DeletionTableProviderAdapter::new(Arc::new(indexed_table));
            return Ok(Arc::new(delete_adapter));
        }

        // Standard MemTable path (no primary key or hash index disabled)
        let mut mem_table = MemTable::try_new(schema, vec![])?
            .try_with_constraints(cmd.constraints.clone())
            .await?;

        // Only set on_conflict if explicitly provided in options
        // For primary key constraints, MemTable will use them directly without needing on_conflict
        if let Some(on_conflict_str) = cmd.options.get("on_conflict") {
            mem_table = mem_table.with_on_conflict(
                OnConflict::try_from(on_conflict_str.as_str()).map_err(|e| {
                    DataFusionError::External(format!("Error parsing on_conflict: {e}").into())
                })?,
            );
        }

        // Parse sort_columns if provided
        if let Some(sort_cols_str) = cmd.options.get("sort_columns") {
            let sort_columns: Vec<String> = sort_cols_str
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();

            if !sort_columns.is_empty() {
                mem_table = mem_table.with_sort_columns(sort_columns);
            }
        }

        let delete_adapter = DeletionTableProviderAdapter::new(Arc::new(mem_table));
        Ok(Arc::new(delete_adapter))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{Field, Schema};
    use datafusion::common::{Constraint, Constraints};
    use datafusion::execution::SessionStateBuilder;
    use datafusion::logical_expr::CreateExternalTable;
    use std::collections::HashMap;

    fn create_test_schema() -> Schema {
        Schema::new(vec![
            Field::new("id", arrow::datatypes::DataType::Int64, false),
            Field::new("name", arrow::datatypes::DataType::Utf8, false),
        ])
    }

    #[tokio::test]
    async fn test_factory_creates_indexed_memtable_with_hash_index_enabled() {
        let factory = ArrowFactory::new();
        let schema = create_test_schema();

        // Create command with primary key constraint and hash_index=enabled
        let constraints = Constraints::new_unverified(vec![Constraint::PrimaryKey(vec![0])]);
        let mut options = HashMap::new();
        options.insert("hash_index".to_string(), "enabled".to_string());

        let cmd = CreateExternalTable {
            schema: Arc::new(
                datafusion::common::DFSchema::try_from(schema).expect("schema conversion"),
            ),
            name: "test_table".into(),
            location: String::new(),
            file_type: String::new(),
            table_partition_cols: vec![],
            if_not_exists: false,
            temporary: false,
            definition: None,
            order_exprs: vec![],
            unbounded: false,
            options,
            constraints,
            column_defaults: HashMap::new(),
        };

        let state = SessionStateBuilder::new().build();
        let table = factory
            .create(&state, &cmd)
            .await
            .expect("failed to create table");

        // The table should be created with an indexed structure
        assert!(table.as_any().is::<DeletionTableProviderAdapter>());
    }

    #[tokio::test]
    async fn test_factory_creates_memtable_without_primary_key() {
        let factory = ArrowFactory::new();
        let schema = create_test_schema();

        let cmd = CreateExternalTable {
            schema: Arc::new(
                datafusion::common::DFSchema::try_from(schema).expect("schema conversion"),
            ),
            name: "test_table".into(),
            location: String::new(),
            file_type: String::new(),
            table_partition_cols: vec![],
            if_not_exists: false,
            temporary: false,
            definition: None,
            order_exprs: vec![],
            unbounded: false,
            options: HashMap::new(),
            constraints: Constraints::new_unverified(vec![]),
            column_defaults: HashMap::new(),
        };

        let state = SessionStateBuilder::new().build();
        let table = factory
            .create(&state, &cmd)
            .await
            .expect("failed to create table");

        // Without primary key, should still be a valid table
        assert!(table.as_any().is::<DeletionTableProviderAdapter>());
    }

    #[tokio::test]
    async fn test_factory_hash_index_enabled_requires_primary_key() {
        let factory = ArrowFactory::new();
        let schema = create_test_schema();

        // Create command with hash_index=enabled but NO primary key
        let constraints = Constraints::new_unverified(vec![]);
        let mut options = HashMap::new();
        options.insert("hash_index".to_string(), "enabled".to_string());

        let cmd = CreateExternalTable {
            schema: Arc::new(
                datafusion::common::DFSchema::try_from(schema).expect("schema conversion"),
            ),
            name: "test_table".into(),
            location: String::new(),
            file_type: String::new(),
            table_partition_cols: vec![],
            if_not_exists: false,
            temporary: false,
            definition: None,
            order_exprs: vec![],
            unbounded: false,
            options,
            constraints,
            column_defaults: HashMap::new(),
        };

        let state = SessionStateBuilder::new().build();
        let result = factory.create(&state, &cmd).await;

        // Should fail because hash_index=enabled requires a primary_key
        assert!(result.is_err());
        let err = result.expect_err("expected error");
        assert!(
            err.to_string().contains("primary_key"),
            "Error should mention primary_key requirement: {err}"
        );
    }

    #[tokio::test]
    async fn test_factory_hash_index_disabled_by_default() {
        let factory = ArrowFactory::new();
        let schema = create_test_schema();

        // Create command with primary key but no hash_index option (defaults to disabled)
        let constraints = Constraints::new_unverified(vec![Constraint::PrimaryKey(vec![0])]);

        let cmd = CreateExternalTable {
            schema: Arc::new(
                datafusion::common::DFSchema::try_from(schema).expect("schema conversion"),
            ),
            name: "test_table".into(),
            location: String::new(),
            file_type: String::new(),
            table_partition_cols: vec![],
            if_not_exists: false,
            temporary: false,
            definition: None,
            order_exprs: vec![],
            unbounded: false,
            options: HashMap::new(),
            constraints,
            column_defaults: HashMap::new(),
        };

        let state = SessionStateBuilder::new().build();
        let table = factory
            .create(&state, &cmd)
            .await
            .expect("failed to create table");

        // With hash_index not specified, should still create successfully (uses non-indexed table)
        assert!(table.as_any().is::<DeletionTableProviderAdapter>());
    }
}
