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
    datasource::TableProvider,
    error::{DataFusionError, Result as DataFusionResult},
    logical_expr::CreateExternalTable,
};
use datafusion_table_providers::util::on_conflict::OnConflict;
use std::sync::Arc;

use crate::delete::DeletionTableProviderAdapter;

use self::write::MemTable;

pub mod struct_builder;
pub mod write;

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

#[async_trait]
impl TableProviderFactory for ArrowFactory {
    async fn create(
        &self,
        _state: &dyn Session,
        cmd: &CreateExternalTable,
    ) -> DataFusionResult<Arc<dyn TableProvider>> {
        let schema: SchemaRef = Arc::new(cmd.schema.as_arrow().clone());
        let mut mem_table = MemTable::try_new(schema, vec![])?
            .try_with_constraints(cmd.constraints.clone())
            .await?;

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
