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
    logical_expr::{Expr, TableProviderFilterPushDown},
    physical_plan::ExecutionPlan,
};
use std::{any::Any, sync::Arc};

use super::{client::GraphQLClient, ResultTransformSnafu};
use super::{client::GraphQLQuery, Result};

pub type TransformFn =
    fn(&RecordBatch) -> Result<RecordBatch, Box<dyn std::error::Error + Send + Sync>>;

pub type FilterPushdownFn =
    fn(&Expr) -> Result<FilterPushdownResult, datafusion::error::DataFusionError>;

pub type ParameterInjectionFn =
    fn(&[FilterPushdownResult], &str) -> Result<Arc<str>, datafusion::error::DataFusionError>;

#[derive(Debug, Clone)]
pub struct FilterPushdownResult {
    pub filter_pushdown: TableProviderFilterPushDown,
    pub expr: Expr,
    pub parameter: Option<String>,
}

pub struct GraphQLTableProviderBuilder {
    client: GraphQLClient,
    transform_fn: Option<TransformFn>,
    filter_pushdown_fn: Option<FilterPushdownFn>,
    parameter_injection_fn: Option<ParameterInjectionFn>,
}

impl GraphQLTableProviderBuilder {
    #[must_use]
    pub fn new(client: GraphQLClient) -> Self {
        Self {
            client,
            transform_fn: None,
            filter_pushdown_fn: None,
            parameter_injection_fn: None,
        }
    }

    #[must_use]
    pub fn with_schema_transform(mut self, transform_fn: TransformFn) -> Self {
        self.transform_fn = Some(transform_fn);
        self
    }

    #[must_use]
    pub fn with_filter_pushdown(mut self, filter_pushdown_fn: FilterPushdownFn) -> Self {
        self.filter_pushdown_fn = Some(filter_pushdown_fn);
        self
    }

    #[must_use]
    pub fn with_parameter_injection(
        mut self,
        parameter_injection_fn: ParameterInjectionFn,
    ) -> Self {
        self.parameter_injection_fn = Some(parameter_injection_fn);
        self
    }

    pub async fn build(self, query_string: &str) -> Result<GraphQLTableProvider> {
        let mut query = GraphQLQuery::try_from(query_string)?;

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
            filter_pushdown_fn: self.filter_pushdown_fn,
            parameter_injection_fn: self.parameter_injection_fn,
        })
    }
}

pub struct GraphQLTableProvider {
    client: GraphQLClient,
    base_query: String,
    gql_schema: SchemaRef,
    table_schema: SchemaRef,
    transform_fn: Option<TransformFn>,
    filter_pushdown_fn: Option<FilterPushdownFn>,
    parameter_injection_fn: Option<ParameterInjectionFn>,
}

// remove duplicate parameters
fn compact_parameters(parameters: &[FilterPushdownResult]) -> Vec<FilterPushdownResult> {
    let mut compacted: Vec<FilterPushdownResult> = vec![];

    for parameter in parameters {
        if !compacted.iter_mut().any(|p| p.expr == parameter.expr) {
            compacted.push(parameter.clone());
        }
    }

    compacted
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

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>, datafusion::error::DataFusionError> {
        if let Some(filter_pushdown_fn) = &self.filter_pushdown_fn {
            filters
                .iter()
                .map(|f| filter_pushdown_fn(f).map(|r| r.filter_pushdown))
                .collect::<Result<Vec<_>, datafusion::error::DataFusionError>>()
        } else {
            Ok(vec![
                TableProviderFilterPushDown::Unsupported;
                filters.len()
            ])
        }
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        println!("provided len: {}", filters.len());
        let parameters = if let Some(filter_pushdown_fn) = self.filter_pushdown_fn {
            compact_parameters(
                &filters
                    .iter()
                    .map(filter_pushdown_fn)
                    .collect::<Result<Vec<_>, datafusion::error::DataFusionError>>()?,
            )
        } else {
            println!("defaulting to no pushdown");
            vec![]
        };

        println!("parameters: {parameters:?}");

        // let query = if let Some(injection_fn) = self.parameter_injection_fn {
        //     injection_fn(&parameters, &self.client.get_query())?
        // } else {
        //     self.client.get_query()
        // };

        // println!("query: {query}");

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
