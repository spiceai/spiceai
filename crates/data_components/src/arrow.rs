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
use hash_index::HashIndexBuilder;
use std::sync::Arc;

use crate::delete::DeletionTableProviderAdapter;

use self::indexed::SecondaryIndex;
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

/// Represents an index type from the spicepod configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexType {
    /// A standard index that allows duplicates (not fully utilized yet with hash index).
    Enabled,
    /// A unique index that enforces uniqueness.
    Unique,
}

impl IndexType {
    fn from_str(s: &str) -> Self {
        if s.eq_ignore_ascii_case("unique") {
            Self::Unique
        } else {
            Self::Enabled
        }
    }
}

/// Parses the indexes option string into column names and their index types.
/// Format: "col1:enabled;col2:unique;(col3,col4):unique" (compound key with columns col3 and col4)
fn parse_indexes_option(
    indexes_str: &str,
    schema: &arrow::datatypes::Schema,
) -> DataFusionResult<Vec<(Vec<String>, IndexType)>> {
    let mut indexes = Vec::new();

    for entry in indexes_str.split(';') {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }

        let parts: Vec<&str> = entry.split(':').collect();
        let Some(col_part) = parts.first().map(|s| s.trim()) else {
            continue;
        };
        if col_part.is_empty() {
            continue;
        }

        // Parse column reference - may be compound like "(col1, col2)" or just "col1"
        let columns: Vec<String> = if col_part.starts_with('(') && col_part.ends_with(')') {
            // Compound key: "(col1, col2)"
            col_part[1..col_part.len() - 1]
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect()
        } else {
            vec![col_part.to_string()]
        };

        // Validate all columns exist in schema
        for col in &columns {
            if schema.field_with_name(col).is_err() {
                return Err(DataFusionError::Configuration(format!(
                    "Index column '{col}' not found in schema"
                )));
            }
        }

        let index_type = if parts.len() > 1 {
            IndexType::from_str(parts[1].trim())
        } else {
            IndexType::Enabled
        };

        indexes.push((columns, index_type));
    }

    Ok(indexes)
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
            let mut indexed_table =
                IndexedMemTable::try_new(Arc::clone(&schema), vec![], primary_key_columns)?;

            // Parse and create secondary indexes if specified
            if let Some(indexes_str) = cmd.options.get("indexes") {
                let indexes_config = parse_indexes_option(indexes_str, &schema)?;
                let mut secondary_indexes = Vec::new();

                for (columns, index_type) in indexes_config {
                    let is_unique = index_type == IndexType::Unique;
                    let index_name = columns.join("_");

                    // Warn about compound secondary indexes not being used for query optimization yet
                    if columns.len() > 1 {
                        tracing::warn!(
                            index_name = %index_name,
                            columns = ?columns,
                            "Compound secondary index created but will not be used for query optimization. Only single-column secondary indexes currently accelerate queries."
                        );
                    }

                    // Build hash index for secondary columns
                    // Note: For empty table, we create the index structure; it will be populated on insert
                    let partitions: Vec<Vec<arrow::array::RecordBatch>> = vec![];
                    let hash_index = HashIndexBuilder::new(columns.clone())
                        .allow_duplicates(!is_unique)
                        .build(&partitions)
                        .map_err(|e| {
                            DataFusionError::Execution(format!(
                                "Failed to build secondary index '{index_name}': {e}"
                            ))
                        })?;

                    secondary_indexes.push(SecondaryIndex::new(
                        index_name,
                        columns,
                        is_unique,
                        Arc::new(hash_index),
                    ));
                }

                indexed_table = indexed_table.with_secondary_indexes(secondary_indexes);
            }

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

    // =============================================================================
    // parse_indexes_option Unit Tests
    // =============================================================================

    fn create_schema_with_columns(columns: &[(&str, arrow::datatypes::DataType)]) -> Schema {
        Schema::new(
            columns
                .iter()
                .map(|(name, dt)| Field::new(*name, dt.clone(), false))
                .collect::<Vec<_>>(),
        )
    }

    #[test]
    fn test_parse_indexes_single_column_enabled() {
        let schema = create_schema_with_columns(&[
            ("col1", arrow::datatypes::DataType::Int64),
            ("col2", arrow::datatypes::DataType::Utf8),
        ]);

        let result = parse_indexes_option("col1:enabled", &schema).expect("parse failed");
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, vec!["col1".to_string()]);
        assert_eq!(result[0].1, IndexType::Enabled);
    }

    #[test]
    fn test_parse_indexes_single_column_unique() {
        let schema = create_schema_with_columns(&[
            ("col1", arrow::datatypes::DataType::Int64),
            ("col2", arrow::datatypes::DataType::Utf8),
        ]);

        let result = parse_indexes_option("col2:unique", &schema).expect("parse failed");
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, vec!["col2".to_string()]);
        assert_eq!(result[0].1, IndexType::Unique);
    }

    #[test]
    fn test_parse_indexes_compound_key_unique() {
        let schema = create_schema_with_columns(&[
            ("col1", arrow::datatypes::DataType::Int64),
            ("col2", arrow::datatypes::DataType::Utf8),
            ("col3", arrow::datatypes::DataType::Int32),
        ]);

        let result = parse_indexes_option("(col1,col2):unique", &schema).expect("parse failed");
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, vec!["col1".to_string(), "col2".to_string()]);
        assert_eq!(result[0].1, IndexType::Unique);
    }

    #[test]
    fn test_parse_indexes_multiple_indexes() {
        let schema = create_schema_with_columns(&[
            ("col1", arrow::datatypes::DataType::Int64),
            ("col2", arrow::datatypes::DataType::Utf8),
            ("col3", arrow::datatypes::DataType::Int32),
        ]);

        let result = parse_indexes_option("col1:unique;col2:enabled;(col2,col3):unique", &schema)
            .expect("parse failed");
        assert_eq!(result.len(), 3);

        assert_eq!(result[0].0, vec!["col1".to_string()]);
        assert_eq!(result[0].1, IndexType::Unique);

        assert_eq!(result[1].0, vec!["col2".to_string()]);
        assert_eq!(result[1].1, IndexType::Enabled);

        assert_eq!(result[2].0, vec!["col2".to_string(), "col3".to_string()]);
        assert_eq!(result[2].1, IndexType::Unique);
    }

    #[test]
    fn test_parse_indexes_empty_string() {
        let schema = create_schema_with_columns(&[("col1", arrow::datatypes::DataType::Int64)]);

        let result = parse_indexes_option("", &schema).expect("parse failed");
        assert!(result.is_empty());
    }

    #[test]
    fn test_parse_indexes_malformed_entries_skipped() {
        let schema = create_schema_with_columns(&[
            ("col1", arrow::datatypes::DataType::Int64),
            ("col2", arrow::datatypes::DataType::Utf8),
        ]);

        // Empty entries are skipped, only valid entry is parsed
        let result = parse_indexes_option(";;col1:unique;;", &schema).expect("parse failed");
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, vec!["col1".to_string()]);
    }

    #[test]
    fn test_parse_indexes_invalid_column_returns_error() {
        let schema = create_schema_with_columns(&[("col1", arrow::datatypes::DataType::Int64)]);

        let result = parse_indexes_option("nonexistent:unique", &schema);
        assert!(result.is_err());
        let err = result.expect_err("expected error");
        assert!(
            err.to_string().contains("nonexistent"),
            "Error should mention the invalid column name"
        );
    }

    #[test]
    fn test_parse_indexes_compound_key_with_invalid_column() {
        let schema = create_schema_with_columns(&[
            ("col1", arrow::datatypes::DataType::Int64),
            ("col2", arrow::datatypes::DataType::Utf8),
        ]);

        let result = parse_indexes_option("(col1,invalid):unique", &schema);
        let _ = result.expect_err("expected error for invalid column");
    }

    #[test]
    fn test_parse_indexes_default_type_is_enabled() {
        let schema = create_schema_with_columns(&[("col1", arrow::datatypes::DataType::Int64)]);

        // No type specified - should default to Enabled
        let result = parse_indexes_option("col1", &schema).expect("parse failed");
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].1, IndexType::Enabled);
    }

    #[test]
    fn test_parse_indexes_whitespace_handling() {
        let schema = create_schema_with_columns(&[
            ("col1", arrow::datatypes::DataType::Int64),
            ("col2", arrow::datatypes::DataType::Utf8),
        ]);

        // Whitespace around entries and values should be trimmed
        let result = parse_indexes_option("  col1 : unique ; ( col1 , col2 ) : enabled  ", &schema)
            .expect("parse failed");
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].0, vec!["col1".to_string()]);
        assert_eq!(result[0].1, IndexType::Unique);
        assert_eq!(result[1].0, vec!["col1".to_string(), "col2".to_string()]);
        assert_eq!(result[1].1, IndexType::Enabled);
    }

    #[test]
    fn test_parse_indexes_case_insensitive_type() {
        let schema = create_schema_with_columns(&[("col1", arrow::datatypes::DataType::Int64)]);

        let result1 = parse_indexes_option("col1:UNIQUE", &schema).expect("parse failed");
        assert_eq!(result1[0].1, IndexType::Unique);

        let result2 = parse_indexes_option("col1:Unique", &schema).expect("parse failed");
        assert_eq!(result2[0].1, IndexType::Unique);

        let result3 = parse_indexes_option("col1:ENABLED", &schema).expect("parse failed");
        assert_eq!(result3[0].1, IndexType::Enabled);
    }
}
