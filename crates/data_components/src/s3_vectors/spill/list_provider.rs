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
use std::{
    any::Any,
    sync::{Arc, atomic::AtomicU8},
};

use crate::s3_vectors::{
    gather_and_limit_providers, list_provider::S3VectorsListTable, spill::all_spill_tables,
    vector_table::S3VectorsTable,
};

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::{
    catalog::{Session, TableProvider},
    common::Constraints,
    datasource::TableType,
    error::{DataFusionError, Result as DataFusionResult},
    logical_expr::TableProviderFilterPushDown,
    physical_plan::ExecutionPlan,
    prelude::Expr,
};
use snafu::ResultExt;
#[derive(Debug, Clone)]
pub struct S3VectorsSpillListTable {
    table: S3VectorsTable,
    spill_index: Arc<AtomicU8>, // probably don't need this.
}

impl S3VectorsSpillListTable {
    #[must_use]
    pub fn new(table: S3VectorsTable) -> Self {
        Self {
            table,
            spill_index: Arc::new(AtomicU8::new(0)),
        }
    }
}

#[async_trait]
impl TableProvider for S3VectorsSpillListTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.table.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn constraints(&self) -> Option<&Constraints> {
        Some(&self.table.constraints)
    }

    /// S3 vectors `ListVectors` API operation does not support filtering.
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        Ok(vec![
            TableProviderFilterPushDown::Unsupported;
            filters.len()
        ])
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let list_tables: Vec<Arc<dyn TableProvider>> =
            all_spill_tables(&self.table, &self.spill_index)
                .await
                .boxed()
                .map_err(DataFusionError::External)?
                .into_iter()
                .map(|table| Arc::new(S3VectorsListTable::new(table)) as Arc<dyn TableProvider>)
                .collect();

        gather_and_limit_providers(list_tables, state, projection, filters, limit).await
    }
}
