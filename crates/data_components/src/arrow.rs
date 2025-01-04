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
    error::Result as DataFusionResult,
    logical_expr::CreateExternalTable,
};
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
        let mem_table = MemTable::try_new(schema, vec![])?
            .try_with_constraints(cmd.constraints.clone())
            .await?;
        let delete_adapter = DeletionTableProviderAdapter::new(Arc::new(mem_table));
        Ok(Arc::new(delete_adapter))
    }
}
