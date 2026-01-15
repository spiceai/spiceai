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

use crate::component::dataset::Dataset;
use async_trait::async_trait;
use data_components::github::GithubRestClient;
use datafusion::{
    catalog::Session,
    common::Statistics,
    config::ConfigOptions,
    datasource::{TableProvider, TableType},
    error::DataFusionError,
    execution::{SendableRecordBatchStream, TaskContext},
    logical_expr::{Expr, TableProviderFilterPushDown},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, ExecutionPlan, Partitioning, PhysicalExpr, PlanProperties,
        execution_plan::{
            Boundedness, CardinalityEffect, EmissionType, InvariantLevel, check_default_invariants,
        },
        expressions::Column,
        filter_pushdown::{
            ChildPushdownResult, FilterDescription, FilterPushdownPhase, FilterPushdownPropagation,
        },
        projection::ProjectionExec,
        stream::RecordBatchStreamAdapter,
    },
};
use futures::{TryFutureExt, TryStreamExt};
use std::{any::Any, sync::Arc};

use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};

use super::ConnectorComponent;

#[derive(Debug)]
pub struct WorkflowsTableProvider {
    client: Arc<GithubRestClient>,
    owner: Arc<str>,
    repo: Arc<str>,
    schema: SchemaRef,
}

impl WorkflowsTableProvider {
    pub async fn new(
        client: GithubRestClient,
        owner: &str,
        repo: &str,
        dataset: &Dataset,
    ) -> crate::dataconnector::DataConnectorResult<Self> {
        let fields = vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("path", DataType::Utf8, false),
            Field::new("state", DataType::Utf8, false),
            Field::new(
                "created_at",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ),
            Field::new(
                "updated_at",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ),
            Field::new("badge_url", DataType::Utf8, false),
        ];

        let schema = Arc::new(Schema::new(fields));

        // Validate access by fetching a limited set of workflows
        let client = Arc::new(client);
        Arc::clone(&client)
            .fetch_workflows(owner.into(), repo.into(), Some(1))
            .await
            .map_err(|e| super::DataConnectorError::UnableToGetReadProvider {
                dataconnector: "github".to_string(),
                connector_component: ConnectorComponent::from(dataset),
                source: e,
            })?;

        Ok(Self {
            client,
            owner: owner.into(),
            repo: repo.into(),
            schema,
        })
    }
}

#[async_trait]
impl TableProvider for WorkflowsTableProvider {
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
    ) -> std::result::Result<Vec<TableProviderFilterPushDown>, DataFusionError> {
        // No filter pushdown support for workflows listing
        Ok(vec![
            TableProviderFilterPushDown::Unsupported;
            filters.len()
        ])
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let github_plan = Arc::new(WorkflowsExecutionPlan {
            owner: Arc::clone(&self.owner),
            repo: Arc::clone(&self.repo),
            limit,
            schema: self.schema(),
            client: Arc::clone(&self.client),
            properties: PlanProperties::new(
                EquivalenceProperties::new(Arc::clone(&self.schema)),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Final,
                Boundedness::Bounded,
            ),
        });

        if let Some(projection) = projection {
            let mut projection_expr = Vec::with_capacity(projection.len());
            for idx in projection {
                let col_name = self.schema.field(*idx).name();
                projection_expr.push((
                    Arc::new(Column::new(col_name, *idx)) as Arc<dyn PhysicalExpr>,
                    col_name.clone(),
                ));
            }

            let projection_exec = ProjectionExec::try_new(projection_expr, github_plan)?;
            return Ok(Arc::new(projection_exec));
        }

        Ok(github_plan)
    }
}

#[derive(Debug)]
struct WorkflowsExecutionPlan {
    owner: Arc<str>,
    repo: Arc<str>,
    limit: Option<usize>,
    schema: SchemaRef,
    client: Arc<GithubRestClient>,
    properties: PlanProperties,
}

impl DisplayAs for WorkflowsExecutionPlan {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(
            f,
            "GitHubWorkflowsExecutionPlan: {}/{} limit={:?}",
            self.owner, self.repo, self.limit
        )
    }
}

#[deny(clippy::missing_trait_methods)]
impl ExecutionPlan for WorkflowsExecutionPlan {
    fn name(&self) -> &'static str {
        "GitHubWorkflowsExecutionPlan"
    }

    fn static_name() -> &'static str
    where
        Self: Sized,
    {
        "GitHubWorkflowsExecutionPlan"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn check_invariants(&self, check: InvariantLevel) -> datafusion::error::Result<()> {
        check_default_invariants(self, check)
    }

    fn required_input_distribution(&self) -> Vec<datafusion::physical_plan::Distribution> {
        vec![]
    }

    fn required_input_ordering(
        &self,
    ) -> Vec<Option<datafusion::physical_expr::OrderingRequirements>> {
        vec![]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        // this plan has no children
        Ok(self)
    }

    fn reset_state(self: Arc<Self>) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn repartitioned(
        &self,
        _target_partitions: usize,
        _config: &datafusion::config::ConfigOptions,
    ) -> datafusion::error::Result<Option<Arc<dyn ExecutionPlan>>> {
        Ok(None)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        let owner = Arc::clone(&self.owner);
        let repo = Arc::clone(&self.repo);
        let limit = self.limit;
        let client = Arc::clone(&self.client);

        let stream = futures::stream::once(
            client
                .fetch_workflows(owner, repo, limit)
                .map_err(DataFusionError::External),
        )
        .try_flatten();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }

    fn metrics(&self) -> Option<datafusion::physical_plan::metrics::MetricsSet> {
        None
    }

    fn statistics(&self) -> datafusion::error::Result<Statistics> {
        Ok(Statistics::new_unknown(&self.schema()))
    }

    fn partition_statistics(
        &self,
        _partition: Option<usize>,
    ) -> datafusion::error::Result<Statistics> {
        Ok(Statistics::new_unknown(&self.schema()))
    }

    fn supports_limit_pushdown(&self) -> bool {
        true
    }

    fn with_fetch(&self, _limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        None
    }

    fn fetch(&self) -> Option<usize> {
        None
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Unknown // this plan has no inputs
    }

    fn try_swapping_with_projection(
        &self,
        _projection: &ProjectionExec,
    ) -> datafusion::error::Result<Option<Arc<dyn ExecutionPlan>>> {
        Ok(None)
    }

    fn gather_filters_for_pushdown(
        &self,
        _phase: FilterPushdownPhase,
        parent_filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> datafusion::error::Result<FilterDescription> {
        Ok(FilterDescription::all_unsupported(
            &parent_filters,
            &self.children(),
        ))
    }

    fn handle_child_pushdown_result(
        &self,
        _phase: FilterPushdownPhase,
        child_pushdown_result: ChildPushdownResult,
        _config: &ConfigOptions,
    ) -> datafusion::error::Result<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>> {
        Ok(FilterPushdownPropagation::if_all(child_pushdown_result))
    }

    fn with_new_state(&self, _state: Arc<dyn Any + Send + Sync>) -> Option<Arc<dyn ExecutionPlan>> {
        None
    }
}
