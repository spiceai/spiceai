/*
Copyright 2026 The Spice.ai OSS Authors

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

use std::{any::Any, sync::Arc};

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::{
    catalog::{Session, TableProvider},
    datasource::TableType,
    error::Result as DataFusionResult,
    execution::SendableRecordBatchStream,
    logical_expr::TableProviderFilterPushDown,
    physical_expr::{EquivalenceProperties, Partitioning},
    physical_plan::{
        ExecutionPlan, PlanProperties,
        execution_plan::{Boundedness, EmissionType},
    },
    prelude::Expr,
};
use llms::embeddings::Embed;

use super::{
    DuckDBVectorQueryContext, hnsw::DuckDBHnswOptions, query_exec::DuckDBVectorQueryExec,
    sql::duckdb_filter_pushdown,
};

#[derive(Debug)]
pub(super) struct DuckDBVectorQueryTable {
    pub(super) query_text: String,
    pub(super) embedded_column: String,
    pub(super) compute_query: Arc<dyn Embed>,
    pub(super) dims: i32,
    pub(super) schema: SchemaRef,
    pub(super) hnsw: DuckDBHnswOptions,
    pub(super) context: DuckDBVectorQueryContext,
}

#[async_trait]
impl TableProvider for DuckDBVectorQueryTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|filter| duckdb_filter_pushdown(&self.schema, filter))
            .collect())
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let projected_schema = project_schema(&self.schema, projection)?;
        let projected_columns = match projection {
            Some(projection) => projection
                .iter()
                .map(|idx| self.schema.field(*idx).name().clone())
                .collect(),
            None => self
                .schema
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect(),
        };

        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&projected_schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );

        Ok(Arc::new(DuckDBVectorQueryExec {
            projected_schema,
            projected_columns,
            filters: filters.to_vec(),
            limit,
            query_text: self.query_text.clone(),
            embedded_column: self.embedded_column.clone(),
            compute_query: Arc::clone(&self.compute_query),
            dims: self.dims,
            hnsw: self.hnsw.clone(),
            context: self.context.clone(),
            properties,
        }))
    }
}

fn project_schema(
    schema: &SchemaRef,
    projection: Option<&Vec<usize>>,
) -> DataFusionResult<SchemaRef> {
    match projection {
        Some(columns) => Ok(Arc::new(schema.project(columns)?)),
        None => Ok(Arc::clone(schema)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};

    #[test]
    fn project_schema_honors_empty_projection() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let projection = Vec::new();

        let projected = project_schema(&schema, Some(&projection)).expect("schema should project");

        assert!(projected.fields().is_empty());
    }
}
