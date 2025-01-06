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

use arrow::{array::RecordBatch, datatypes::SchemaRef};
use async_trait::async_trait;
use datafusion::{
    catalog::Session,
    datasource::{TableProvider, TableType},
    error::{DataFusionError, Result as DataFusionResult},
    execution::{SendableRecordBatchStream, TaskContext},
    logical_expr::{Expr, TableProviderFilterPushDown},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        expressions::Column, projection::ProjectionExec, stream::RecordBatchStreamAdapter,
        DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, Partitioning, PhysicalExpr,
        PlanProperties,
    },
};
use futures::StreamExt;
use snafu::ResultExt;
use std::{any::Any, fmt, sync::Arc};

use super::{client::GraphQLClient, ErrorChecker, GraphQLContext, ResultTransformSnafu};
use super::{client::GraphQLQuery, Result};

pub type TransformFn =
    fn(&RecordBatch) -> Result<RecordBatch, Box<dyn std::error::Error + Send + Sync>>;

pub struct GraphQLTableProviderBuilder {
    client: GraphQLClient,
    transform_fn: Option<TransformFn>,
    context: Option<Arc<dyn GraphQLContext>>,
}

impl GraphQLTableProviderBuilder {
    #[must_use]
    pub fn new(client: GraphQLClient) -> Self {
        Self {
            client,
            transform_fn: None,
            context: None,
        }
    }

    #[must_use]
    pub fn with_schema_transform(mut self, transform_fn: TransformFn) -> Self {
        self.transform_fn = Some(transform_fn);
        self
    }

    #[must_use]
    pub fn with_context(mut self, context: Arc<dyn GraphQLContext>) -> Self {
        self.context = Some(context);
        self
    }

    pub async fn build(self, query_string: &str) -> Result<GraphQLTableProvider> {
        let query_string: Arc<str> = Arc::from(query_string);
        let mut query = GraphQLQuery::try_from(Arc::clone(&query_string))?;

        if self.client.json_pointer.is_none() && query.json_pointer.is_none() {
            return Err(super::Error::NoJsonPointerFound {});
        }

        let result = self
            .client
            .execute(
                &mut query,
                None,
                None,
                None,
                self.context.clone().and_then(|o| o.error_checker()),
            )
            .await?;

        let table_schema = match (self.transform_fn, result.records.first()) {
            (Some(transform_fn), Some(record_batch)) => transform_fn(record_batch)
                .context(ResultTransformSnafu)?
                .schema(),
            _ => Arc::clone(&result.schema),
        };

        Ok(GraphQLTableProvider {
            client: Arc::new(self.client),
            base_query: query_string,
            gql_schema: Arc::clone(&result.schema),
            table_schema,
            transform_fn: self.transform_fn,
            context: self.context,
        })
    }
}

pub struct GraphQLTableProvider {
    client: Arc<GraphQLClient>,
    base_query: Arc<str>,
    gql_schema: SchemaRef,
    table_schema: SchemaRef,
    transform_fn: Option<TransformFn>,
    context: Option<Arc<dyn GraphQLContext>>,
}

impl std::fmt::Debug for GraphQLTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GraphQLTableProvider")
            .field("base_query", &self.base_query)
            .field("gql_schema", &self.gql_schema)
            .field("table_schema", &self.table_schema)
            .field("context", &self.context)
            .finish_non_exhaustive()
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

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>, datafusion::error::DataFusionError> {
        if let Some(context) = &self.context {
            filters
                .iter()
                .map(|f| context.filter_pushdown(f).map(|r| r.filter_pushdown))
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
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let mut query = GraphQLQuery::try_from(Arc::clone(&self.base_query))
            .map_err(|e| DataFusionError::Execution(format!("{e}")))?;

        let error_checker = if let Some(context) = &self.context {
            let parameters = filters
                .iter()
                .map(|f| context.filter_pushdown(f))
                .collect::<Result<Vec<_>, datafusion::error::DataFusionError>>()?;

            context.inject_parameters(&parameters, &mut query)?;

            context.error_checker()
        } else {
            None
        };

        let graphql_exec = Arc::new(GraphQLTableProviderExec::new(
            Arc::clone(&self.client),
            query,
            Arc::clone(&self.gql_schema),
            Arc::clone(&self.table_schema),
            limit,
            error_checker,
            self.transform_fn,
        ));

        if let Some(projection) = projection {
            let mut projection_expr = Vec::with_capacity(projection.len());
            for idx in projection {
                let col_name = self.table_schema.field(*idx).name();
                projection_expr.push((
                    Arc::new(Column::new(col_name, *idx)) as Arc<dyn PhysicalExpr>,
                    col_name.to_string(),
                ));
            }

            let projection_exec = ProjectionExec::try_new(projection_expr, graphql_exec)?;
            return Ok(Arc::new(projection_exec));
        }

        Ok(graphql_exec)
    }
}

pub struct GraphQLTableProviderExec {
    client: Arc<GraphQLClient>,
    query: GraphQLQuery,
    gql_schema: SchemaRef,
    table_schema: SchemaRef,
    limit: Option<usize>,
    error_checker: Option<ErrorChecker>,
    transform_fn: Option<TransformFn>,
    properties: PlanProperties,
}

impl GraphQLTableProviderExec {
    #[must_use]
    pub fn new(
        client: Arc<GraphQLClient>,
        query: GraphQLQuery,
        gql_schema: SchemaRef,
        table_schema: SchemaRef,
        limit: Option<usize>,
        error_checker: Option<ErrorChecker>,
        transform_fn: Option<TransformFn>,
    ) -> Self {
        Self {
            client,
            query,
            gql_schema,
            table_schema: Arc::clone(&table_schema),
            limit,
            error_checker,
            transform_fn,
            properties: PlanProperties::new(
                EquivalenceProperties::new(table_schema),
                Partitioning::UnknownPartitioning(1),
                ExecutionMode::Bounded,
            ),
        }
    }
}

impl std::fmt::Debug for GraphQLTableProviderExec {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "GraphQLTableProviderExec")
    }
}

impl DisplayAs for GraphQLTableProviderExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        write!(f, "GraphQLTableProviderExec")
    }
}

impl ExecutionPlan for GraphQLTableProviderExec {
    fn name(&self) -> &'static str {
        "GraphQLTableProviderExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.table_schema)
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let mut stream = Arc::clone(&self.client).execute_paginated(
            self.query.clone(),
            Arc::clone(&self.gql_schema),
            Arc::clone(&self.table_schema),
            self.limit,
            self.error_checker.clone(),
        );

        if let Some(transform_fn) = &self.transform_fn {
            let transform_fn = *transform_fn;
            let schema = stream.schema();
            let tx_stream = stream.map(move |batch| {
                batch.and_then(|b| transform_fn(&b).map_err(DataFusionError::External))
            });

            stream = Box::pin(RecordBatchStreamAdapter::new(schema, tx_stream));
        }

        Ok(stream)
    }
}
