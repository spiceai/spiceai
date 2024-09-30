/*
Copyright 2024 The Spice.ai OSS Authors

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
use async_trait::async_trait;
use snafu::ResultExt;

use crate::arrow::write::MemTable;
use arrow::{array::RecordBatch, datatypes::SchemaRef};
use datafusion::{
    catalog::Session,
    datasource::{TableProvider, TableType},
    error::DataFusionError,
    logical_expr::Expr,
    physical_plan::ExecutionPlan,
};
use std::{any::Any, sync::Arc};

use super::{client::GraphQLClient, ResultTransformSnafu};
use super::{client::GraphQLQuery, Result};

pub type TransformFn =
    fn(&RecordBatch) -> Result<RecordBatch, Box<dyn std::error::Error + Send + Sync>>;

pub struct GraphQLTableProviderBuilder {
    client: GraphQLClient,
    transform_fn: Option<TransformFn>,
}

impl GraphQLTableProviderBuilder {
    #[must_use]
    pub fn new(client: GraphQLClient) -> Self {
        Self {
            client,
            transform_fn: None,
        }
    }

    #[must_use]
    pub fn with_schema_transform(mut self, transform_fn: TransformFn) -> Self {
        self.transform_fn = Some(transform_fn);
        self
    }

    pub async fn build(self, query_string: &str) -> Result<GraphQLTableProvider> {
        let mut query = GraphQLQuery::try_from(query_string)?;

        if self.client.json_pointer.is_none() && query.json_pointer.is_none() {
            return Err(super::Error::NoJsonPointerFound {});
        }

        let result = self.client.execute(&mut query, None, None, None).await?;

        let table_schema = match (self.transform_fn, result.records.first()) {
            (Some(transform_fn), Some(record_batch)) => transform_fn(record_batch)
                .context(ResultTransformSnafu)?
                .schema(),
            _ => Arc::clone(&result.schema),
        };

        Ok(GraphQLTableProvider {
            client: self.client,
            base_query: query_string.to_string(),
            gql_schema: Arc::clone(&result.schema),
            table_schema,
            transform_fn: self.transform_fn,
        })
    }
}

pub struct GraphQLTableProvider {
    client: GraphQLClient,
    base_query: String,
    gql_schema: SchemaRef,
    table_schema: SchemaRef,
    transform_fn: Option<TransformFn>,
}

#[async_trait]
impl TableProvider for GraphQLTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.table_schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let mut query =
            GraphQLQuery::try_from(self.base_query.as_str()).expect("Should have a query");

        let mut res = self
            .client
            .execute_paginated(&mut query, Arc::clone(&self.gql_schema), limit)
            .await
            .boxed()
            .map_err(DataFusionError::External)?;

        if let Some(transform_fn) = &self.transform_fn {
            res = res
                .into_iter()
                .map(|inner_vec| {
                    inner_vec
                        .into_iter()
                        .map(|batch| transform_fn(&batch).map_err(DataFusionError::External))
                        .collect::<Result<Vec<_>, DataFusionError>>()
                })
                .collect::<Result<Vec<Vec<_>>, DataFusionError>>()?;
        }

        let table = MemTable::try_new(Arc::clone(&self.table_schema), res)?;

        table.scan(state, projection, filters, limit).await
    }
}
