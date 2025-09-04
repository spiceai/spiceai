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
use std::{any::Any, sync::Arc};

use crate::s3_vectors::{S3VectorIdentifier, vector_table::S3VectorsTable};

use super::{
    Error,
    index_query_provider::{S3_VECTOR_DISTANCE_NAME, S3VectorsQueryIndexTable},
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::{
    catalog::{Session, TableProvider},
    common::Constraints,
    datasource::TableType,
    error::{DataFusionError, Result as DataFusionResult},
    logical_expr::TableProviderFilterPushDown,
    physical_plan::{ExecutionPlan, empty::EmptyExec, limit::GlobalLimitExec, union::UnionExec},
    prelude::Expr,
};
use s3_vectors::ListIndexesInput;
use s3_vectors_metadata_filter;
use snafu::ResultExt;

/// An S3 Vector bucket that implements [`TableProvider`] as a `QueryVector` API operation for all indexes in the bucket.
#[derive(Debug)]
pub struct S3VectorsQueryPartitionedIndexTable {
    table: S3VectorsTable,
    query: Vec<f32>,
}

impl S3VectorsQueryPartitionedIndexTable {
    #[must_use]
    pub fn new(table: S3VectorsTable, query: Vec<f32>) -> Self {
        Self { table, query }
    }
}

#[async_trait]
impl TableProvider for S3VectorsQueryPartitionedIndexTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        let mut base_fields = self
            .table
            .schema
            .fields()
            .iter()
            .cloned()
            .collect::<Vec<_>>();

        base_fields.push(Arc::new(Field::new(
            S3_VECTOR_DISTANCE_NAME,
            DataType::Float64,
            false,
        )));

        Arc::new(Schema::new(base_fields))
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn constraints(&self) -> Option<&Constraints> {
        Some(&self.table.constraints)
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        // Filters can only possibly be pushed down for columns in underlying metadata (i.e. not derived columns like `S3_VECTOR_DISTANCE_NAME`).
        let columns: Vec<_> = self
            .table
            .schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .filter(|c| self.table.is_filterable_column(c.as_str()))
            .collect();

        Ok(filters
            .iter()
            .map(|f| {
                if s3_vectors_metadata_filter::supports_filter_expr(columns.as_slice(), f) {
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if limit == Some(0) {
            return Ok(Arc::new(EmptyExec::new(Arc::clone(&self.schema()))));
        }

        let (_, Some(bucket_name), Some(index_name)) =
            self.table.identifier.index_identifier_variables()
        else {
            return Err(DataFusionError::Execution(format!(
                "No bucket name or index name for bucket query"
            )));
        };

        let list_indexes_output = self
            .table
            .client
            .list_indexes(
                ListIndexesInput::builder()
                    .set_vector_bucket_name(Some(bucket_name.clone()))
                    .build()
                    .boxed()
                    .map_err(DataFusionError::External)?,
            )
            .await
            .map_err(|e| {
                DataFusionError::External(
                    Error::S3VectorListIndexesError {
                        source: e.into_service_error(),
                    }
                    .into(),
                )
            })?;

        let index_names: Vec<_> = list_indexes_output
            .indexes()
            .iter()
            .filter_map(|idx| {
                let name = idx.index_name().to_string();
                // Avoid `index_name.len() == name.len()` as this will be a non-partitioned index we don't expect.
                if name.starts_with(&index_name) && index_name.len() > name.len() {
                    Some(name)
                } else {
                    None
                }
            })
            .collect();

        if index_names.is_empty() {
            return Ok(Arc::new(EmptyExec::new(Arc::clone(&self.schema()))));
        }

        let mut index_plans: Vec<Arc<dyn ExecutionPlan>> = Vec::new();
        for index_name in index_names {
            let index_table_identifier = S3VectorIdentifier::Index {
                bucket_name: bucket_name.clone(),
                index_name,
            };

            let index_table = S3VectorsTable {
                client: Arc::clone(&self.table.client),
                identifier: index_table_identifier,
                schema: Arc::clone(&self.table.schema),
                constraints: self.table.constraints.clone(),
            };

            let query_table = S3VectorsQueryIndexTable::new(index_table, self.query.clone());

            let index_plan = query_table.scan(state, projection, filters, limit).await?;
            index_plans.push(index_plan);
        }

        let union_plan = Arc::new(UnionExec::new(index_plans));

        let limit_plan = Arc::new(GlobalLimitExec::new(union_plan, 0, limit));

        Ok(limit_plan)
    }
}
