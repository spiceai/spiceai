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
use arrow::{
    array::RecordBatch,
    datatypes::{Schema, SchemaRef},
};
use datafusion::{
    catalog::Session,
    datasource::{TableProvider, TableType},
    error::DataFusionError,
    logical_expr::Expr,
    physical_plan::ExecutionPlan,
};
use std::{any::Any, sync::Arc};

use super::Result;
use super::{client::GraphQLClient, ResultTransformSnafu};

pub type TransformFn = Arc<
    dyn Fn(&RecordBatch) -> Result<RecordBatch, Box<dyn std::error::Error + Send + Sync>>
        + Send
        + Sync,
>;

pub type TransformFnPointer =
    fn(&RecordBatch) -> Result<RecordBatch, Box<dyn std::error::Error + Send + Sync>>;

pub struct GraphQLTableProvider {
    client: GraphQLClient,
    gql_schema: SchemaRef,
    table_schema: SchemaRef,
    transform_fn: Option<TransformFnPointer>,
}

impl GraphQLTableProvider {
    pub fn new(client: GraphQLClient) -> Self {
        Self {
            client,
            gql_schema: Arc::new(Schema::empty()),
            table_schema: Arc::new(Schema::empty()),
            transform_fn: None,
        }
    }

    pub fn with_schema_transform(mut self, transform_fn: TransformFnPointer) -> Self {
        self.transform_fn = Some(transform_fn);
        self
    }

    pub async fn build(mut self) -> Result<Self> {
        let (res, gql_schema, _) = self.client.execute(None, None, None).await?;

        self.gql_schema = Arc::clone(&gql_schema);
        self.table_schema = match (self.transform_fn, res.first()) {
            (Some(transform_fn), Some(record_batch)) => transform_fn(record_batch)
                .context(ResultTransformSnafu)?
                .schema(),
            _ => Arc::clone(&gql_schema),
        };

        Ok(self)
    }
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
        let mut res = self
            .client
            .execute_paginated(Arc::clone(&self.gql_schema), limit)
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
