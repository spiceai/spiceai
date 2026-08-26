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
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PhysicalExpr, PlanProperties,
        execution_plan::{Boundedness, EmissionType},
        expressions::Column,
        projection::ProjectionExec,
        stream::RecordBatchStreamAdapter,
    },
};
use futures::StreamExt;
use snafu::ResultExt;
use std::{fmt, sync::Arc};

use super::{ErrorChecker, GraphQLContext, ResultTransformSnafu, client::GraphQLClient};
use super::{Result, client::GraphQLQuery};

pub type TransformFn =
    fn(&RecordBatch) -> Result<RecordBatch, Box<dyn std::error::Error + Send + Sync>>;

fn derive_table_schema(
    gql_schema: &SchemaRef,
    transform_fn: Option<TransformFn>,
) -> Result<SchemaRef> {
    match transform_fn {
        Some(transform_fn) => {
            // `new_empty` rather than `try_new`, which cannot build a batch from a fieldless
            // schema — see `derive_table_schema_handles_a_fieldless_schema`.
            let empty_batch = RecordBatch::new_empty(Arc::clone(gql_schema));

            Ok(transform_fn(&empty_batch)
                .context(ResultTransformSnafu)?
                .schema())
        }
        None => Ok(Arc::clone(gql_schema)),
    }
}

fn apply_client_json_pointer(client: &GraphQLClient, query: &mut GraphQLQuery) {
    if let Some(json_pointer) = client.json_pointer.as_ref() {
        query.json_pointer = Some(Arc::clone(json_pointer));
    }
}

pub struct GraphQLTableProviderBuilder {
    client: GraphQLClient,
    transform_fn: Option<TransformFn>,
    context: Option<Arc<dyn GraphQLContext>>,
    health_check_query: Option<GraphQLQuery>,
}

impl GraphQLTableProviderBuilder {
    #[must_use]
    pub fn new(client: GraphQLClient) -> Self {
        Self {
            client,
            transform_fn: None,
            context: None,
            health_check_query: None,
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

    #[must_use]
    pub fn with_health_check_query(mut self, health_check_query: GraphQLQuery) -> Self {
        self.health_check_query = Some(health_check_query);
        self
    }

    pub async fn build(self, query_string: &str) -> Result<GraphQLTableProvider> {
        let query_string: Arc<str> = Arc::from(query_string);
        let mut query = GraphQLQuery::try_from(Arc::clone(&query_string))?;
        apply_client_json_pointer(&self.client, &mut query);

        if self.client.json_pointer.is_none() && query.json_pointer.is_none() {
            return Err(super::Error::NoJsonPointerFound {});
        }

        // Health check on GraphQL resource existence
        if let Some(health_check_query) = self.health_check_query {
            let _ = self
                .client
                .execute(
                    &health_check_query,
                    None,
                    None,
                    None,
                    self.context.clone().and_then(|o| o.error_checker()),
                    None,
                )
                .await?;
        }

        let result = self
            .client
            .execute(
                &query,
                None,
                None,
                None,
                self.context.clone().and_then(|o| o.error_checker()),
                None,
            )
            .await?;

        let gql_schema = Arc::clone(&result.schema);

        let table_schema = match (self.transform_fn, result.records.first()) {
            (Some(transform_fn), Some(record_batch)) => transform_fn(record_batch)
                .context(ResultTransformSnafu)?
                .schema(),
            _ => derive_table_schema(&gql_schema, self.transform_fn)?,
        };

        Ok(GraphQLTableProvider {
            client: Arc::new(self.client),
            base_query: query_string,
            gql_schema,
            table_schema,
            transform_fn: self.transform_fn,
            context: self.context,
        })
    }

    pub fn build_without_validation(self, query_string: &str) -> Result<GraphQLTableProvider> {
        let query_string: Arc<str> = Arc::from(query_string);
        let query = GraphQLQuery::try_from(Arc::clone(&query_string))?;

        if self.client.json_pointer.is_none() && query.json_pointer.is_none() {
            return Err(super::Error::NoJsonPointerFound {});
        }

        let gql_schema =
            self.client
                .configured_schema()
                .ok_or_else(|| super::Error::InternalError {
                    message: "GraphQL provider fallback requires a configured schema".to_string(),
                })?;
        let table_schema = derive_table_schema(&gql_schema, self.transform_fn)?;

        Ok(GraphQLTableProvider {
            client: Arc::new(self.client),
            base_query: query_string,
            gql_schema,
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

impl GraphQLTableProvider {
    #[must_use]
    pub fn client(&self) -> Arc<GraphQLClient> {
        Arc::clone(&self.client)
    }

    /// Attaches a context to an already-built provider, for tests that build
    /// without validation and then exercise a context-dependent scan decision.
    #[cfg(test)]
    #[must_use]
    pub fn with_context(mut self, context: Arc<dyn GraphQLContext>) -> Self {
        self.context = Some(context);
        self
    }
}

#[async_trait]
impl TableProvider for GraphQLTableProvider {
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

        let (error_checker, query_cost, supports_limit_pushdown) =
            if let Some(context) = &self.context {
                let parameters = filters
                    .iter()
                    .map(|f| context.filter_pushdown(f))
                    .collect::<Result<Vec<_>, datafusion::error::DataFusionError>>()?;

                context.inject_parameters(&parameters, &mut query)?;

                (
                    context.error_checker(),
                    context.query_cost(),
                    context.supports_limit_pushdown(),
                )
            } else {
                (None, None, true)
            };

        // A table whose rows do not map one-to-one onto the paginated connection
        // cannot bound its scan by a row limit — see `supports_limit_pushdown`.
        let limit = if supports_limit_pushdown { limit } else { None };

        apply_client_json_pointer(self.client.as_ref(), &mut query);

        let graphql_exec = Arc::new(
            GraphQLTableProviderExec::new(
                Arc::clone(&self.client),
                query,
                Arc::clone(&self.gql_schema),
                Arc::clone(&self.table_schema),
            )
            .with_limit(limit)
            .with_error_checker(error_checker)
            .with_transform_fn(self.transform_fn)
            .with_query_cost(query_cost),
        );

        if let Some(projection) = projection {
            let mut projection_expr = Vec::with_capacity(projection.len());
            for idx in projection {
                let col_name = self.table_schema.field(*idx).name();
                projection_expr.push((
                    Arc::new(Column::new(col_name, *idx)) as Arc<dyn PhysicalExpr>,
                    col_name.clone(),
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
    properties: Arc<PlanProperties>,
    query_cost: Option<u32>,
}

impl GraphQLTableProviderExec {
    #[must_use]
    pub fn new(
        client: Arc<GraphQLClient>,
        query: GraphQLQuery,
        gql_schema: SchemaRef,
        table_schema: SchemaRef,
    ) -> Self {
        Self {
            client,
            query,
            gql_schema,
            table_schema: Arc::clone(&table_schema),
            limit: None,
            error_checker: None,
            transform_fn: None,
            properties: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(table_schema),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            )),
            query_cost: None,
        }
    }

    #[must_use]
    pub fn with_limit(mut self, limit: Option<usize>) -> Self {
        self.limit = limit;
        self
    }

    #[must_use]
    pub fn with_error_checker(mut self, error_checker: Option<ErrorChecker>) -> Self {
        self.error_checker = error_checker;
        self
    }

    #[must_use]
    pub fn with_transform_fn(mut self, transform_fn: Option<TransformFn>) -> Self {
        self.transform_fn = transform_fn;
        self
    }

    #[must_use]
    pub fn with_query_cost(mut self, query_cost: Option<u32>) -> Self {
        self.query_cost = query_cost;
        self
    }
}

impl std::fmt::Debug for GraphQLTableProviderExec {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let limit_str = if let Some(limit) = self.limit {
            format!("limit=[{limit}]")
        } else {
            String::new()
        };
        write!(f, "GraphQLTableProviderExec {limit_str}")
    }
}

impl DisplayAs for GraphQLTableProviderExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        let limit_str = if let Some(limit) = self.limit {
            format!("limit=[{limit}]")
        } else {
            String::new()
        };
        write!(f, "GraphQLTableProviderExec {limit_str}")
    }
}

impl ExecutionPlan for GraphQLTableProviderExec {
    fn name(&self) -> &'static str {
        "GraphQLTableProviderExec"
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.table_schema)
    }

    fn properties(&self) -> &Arc<PlanProperties> {
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
            self.query_cost,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graphql::builder::GraphQLClientBuilder;
    use crate::graphql::client::UnnestBehavior;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::TableProvider;
    use url::Url;

    fn rename_first_column(
        batch: &RecordBatch,
    ) -> Result<RecordBatch, Box<dyn std::error::Error + Send + Sync>> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "renamed_id",
            batch.schema().field(0).data_type().clone(),
            true,
        )]));

        RecordBatch::try_new(schema, vec![Arc::clone(batch.column(0))]).map_err(Into::into)
    }

    #[expect(
        clippy::unnecessary_wraps,
        reason = "signature is fixed by TransformFn, which fallible transforms also implement"
    )]
    fn fixed_output_schema(
        _batch: &RecordBatch,
    ) -> Result<RecordBatch, Box<dyn std::error::Error + Send + Sync>> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "cast_id",
            DataType::Utf8,
            true,
        )]));

        Ok(RecordBatch::new_empty(schema))
    }

    /// Regression test for #13004: with nothing configured to fall back to, an empty first page
    /// leaves a fieldless schema, and building the transform's probe batch out of it used to fail
    /// with "must either specify a row count or at least one column" — failing the whole dataset.
    #[test]
    fn derive_table_schema_handles_a_fieldless_schema() {
        let empty: SchemaRef = Arc::new(Schema::empty());

        let derived = derive_table_schema(&empty, Some(fixed_output_schema))
            .expect("a fieldless schema is not a reason to fail the dataset");

        assert_eq!(derived.field(0).name(), "cast_id");
    }

    /// The probe batch a transform sees still carries every configured field, so a transform that
    /// inspects its input keeps working.
    #[test]
    fn derive_table_schema_passes_configured_fields_to_the_transform() {
        let schema: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));

        let derived = derive_table_schema(&schema, Some(rename_first_column))
            .expect("transform to run over the configured fields");

        assert_eq!(derived.field(0).name(), "renamed_id");
        assert_eq!(derived.field(0).data_type(), &DataType::Int64);
    }

    #[test]
    fn build_without_validation_uses_configured_schema() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, true)]));
        let client = GraphQLClientBuilder::new(
            Url::parse("https://example.com/graphql").expect("valid URL"),
            UnnestBehavior::Depth(0),
        )
        .with_json_pointer(Some("/data/view/nodes"))
        .with_schema(Some(Arc::clone(&schema)))
        .build(reqwest::Client::new())
        .expect("client to build");

        let provider = GraphQLTableProviderBuilder::new(client)
            .build_without_validation("query { view { nodes { id } } }")
            .expect("provider to build without validation");

        assert_eq!(TableProvider::schema(&provider), schema);
    }

    #[test]
    fn build_without_validation_derives_transformed_schema_without_data() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, true)]));
        let client = GraphQLClientBuilder::new(
            Url::parse("https://example.com/graphql").expect("valid URL"),
            UnnestBehavior::Depth(0),
        )
        .with_json_pointer(Some("/data/view/nodes"))
        .with_schema(Some(schema))
        .build(reqwest::Client::new())
        .expect("client to build");

        let provider = GraphQLTableProviderBuilder::new(client)
            .with_schema_transform(rename_first_column)
            .build_without_validation("query { view { nodes { id } } }")
            .expect("provider to build without validation");

        assert_eq!(
            TableProvider::schema(&provider).field(0).name(),
            "renamed_id"
        );
    }

    /// A context that declines limit pushdown, for the scan test below.
    #[derive(Debug)]
    struct NoLimitPushdown;

    impl GraphQLContext for NoLimitPushdown {
        fn supports_limit_pushdown(&self) -> bool {
            false
        }
    }

    /// A table whose rows do not map one-to-one onto the paginated connection
    /// must not have a SQL `LIMIT` pushed into it: pagination bounds the limit by
    /// the connection's page size, so `LIMIT 20` would fetch 20 *parents* and stop
    /// having emitted however many rows those carried — fewer than 20, silently.
    #[tokio::test]
    async fn scan_drops_the_limit_when_the_context_declines_pushdown() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, true)]));
        let build = || {
            let client = GraphQLClientBuilder::new(
                Url::parse("https://example.com/graphql").expect("valid URL"),
                UnnestBehavior::Depth(0),
            )
            .with_json_pointer(Some("/data/view/nodes"))
            .with_schema(Some(Arc::clone(&schema)))
            .build(reqwest::Client::new())
            .expect("client to build");

            GraphQLTableProviderBuilder::new(client)
                .build_without_validation("query { view { nodes { id } } }")
                .expect("provider to build without validation")
        };

        let ctx = datafusion::prelude::SessionContext::new();

        // Declining pushdown drops the limit …
        let declining = build().with_context(Arc::new(NoLimitPushdown));
        let plan = declining
            .scan(&ctx.state(), None, &[], Some(20))
            .await
            .expect("scan to plan");
        assert!(
            !format!("{plan:?}").contains("limit="),
            "a fan-out table must not bound its scan by a row limit, got: {plan:?}"
        );

        // … while the default keeps it, so no other connector changes behavior.
        let plan = build()
            .scan(&ctx.state(), None, &[], Some(20))
            .await
            .expect("scan to plan");
        assert!(
            format!("{plan:?}").contains("limit=[20]"),
            "the default must still push the limit down, got: {plan:?}"
        );
    }

    #[test]
    fn configured_json_pointer_overrides_inferred_query_pointer() {
        let client = GraphQLClientBuilder::new(
            Url::parse("https://example.com/graphql").expect("valid URL"),
            UnnestBehavior::Depth(0),
        )
        .with_json_pointer(Some("/data/view"))
        .build(reqwest::Client::new())
        .expect("client to build");

        let mut query = GraphQLQuery::try_from(Arc::<str>::from("query { view { nodes { id } } }"))
            .expect("query to parse");

        query.json_pointer = Some(Arc::from("/data/view/nodes"));

        apply_client_json_pointer(&client, &mut query);

        assert_eq!(query.json_pointer.as_deref(), Some("/data/view"));
    }
}
