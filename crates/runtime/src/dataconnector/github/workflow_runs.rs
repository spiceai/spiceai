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
    common::{Column, Statistics},
    config::ConfigOptions,
    datasource::{TableProvider, TableType},
    error::DataFusionError,
    execution::{SendableRecordBatchStream, TaskContext},
    logical_expr::{Expr, Operator, TableProviderFilterPushDown},
    physical_expr::{self, EquivalenceProperties},
    physical_plan::{
        DisplayAs, ExecutionPlan, Partitioning, PhysicalExpr, PlanProperties,
        execution_plan::{
            Boundedness, CardinalityEffect, EmissionType, InvariantLevel, check_default_invariants,
        },
        filter_pushdown::{
            ChildPushdownResult, FilterDescription, FilterPushdownPhase, FilterPushdownPropagation,
        },
        projection::ProjectionExec,
        stream::RecordBatchStreamAdapter,
    },
    scalar::ScalarValue,
};
use futures::{TryFutureExt, TryStreamExt};
use std::{any::Any, collections::HashMap, sync::Arc};

use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};

use super::ConnectorComponent;

#[derive(Debug, Clone, Default)]
pub struct WorkflowRunFilters {
    pub branch: Option<String>,
    pub status: Option<String>,
    pub head_sha: Option<String>,
    pub created: Option<String>,
}

impl WorkflowRunFilters {
    fn from_filters(filters: &[Expr]) -> (Self, Vec<Expr>) {
        let mut workflow_filters = Self::default();
        let mut unsupported_filters = Vec::new();

        for filter in filters {
            if let Some((column, value, op)) = extract_filter_column_value_op(filter) {
                match (column.name.as_str(), op) {
                    ("head_branch", Operator::Eq) => {
                        if let Some(val) = scalar_to_string(&value)
                            && workflow_filters.branch.is_none()
                        {
                            workflow_filters.branch = Some(val);
                            continue;
                        }
                    }
                    ("status", Operator::Eq) => {
                        if let Some(val) = scalar_to_string(&value)
                            && workflow_filters.status.is_none()
                        {
                            workflow_filters.status = Some(val);
                            continue;
                        }
                    }
                    ("head_sha", Operator::Eq) => {
                        if let Some(val) = scalar_to_string(&value)
                            && workflow_filters.head_sha.is_none()
                        {
                            workflow_filters.head_sha = Some(val);
                            continue;
                        }
                    }
                    (
                        "run_started_at",
                        Operator::Gt | Operator::GtEq | Operator::Lt | Operator::LtEq,
                    ) => {
                        if let Some(val) = format_created_filter(&value, op)
                            && workflow_filters.created.is_none()
                        {
                            workflow_filters.created = Some(val);
                            continue;
                        }
                    }
                    _ => {}
                }
            }
            unsupported_filters.push(filter.clone());
        }

        (workflow_filters, unsupported_filters)
    }

    pub fn to_query_params(&self) -> HashMap<String, String> {
        let mut params = HashMap::new();

        if let Some(ref branch) = self.branch {
            params.insert("branch".to_string(), branch.clone());
        }
        if let Some(ref status) = self.status {
            params.insert("status".to_string(), status.clone());
        }
        if let Some(ref head_sha) = self.head_sha {
            params.insert("head_sha".to_string(), head_sha.clone());
        }
        if let Some(ref created) = self.created {
            params.insert("created".to_string(), created.clone());
        }

        params
    }
}

fn extract_filter_column_value_op(expr: &Expr) -> Option<(Column, ScalarValue, Operator)> {
    match expr {
        Expr::BinaryExpr(binary_expr) => {
            if let (Expr::Column(col), Expr::Literal(val, _)) =
                (&*binary_expr.left, &*binary_expr.right)
            {
                Some((col.clone(), val.clone(), binary_expr.op))
            } else if let (Expr::Literal(val, _), Expr::Column(col)) =
                (&*binary_expr.left, &*binary_expr.right)
            {
                // Reverse the operator for reversed comparisons
                let reversed_op = match binary_expr.op {
                    Operator::Gt => Operator::Lt,
                    Operator::GtEq => Operator::LtEq,
                    Operator::Lt => Operator::Gt,
                    Operator::LtEq => Operator::GtEq,
                    op => op,
                };
                Some((col.clone(), val.clone(), reversed_op))
            } else {
                None
            }
        }
        _ => None,
    }
}

fn scalar_to_string(scalar: &ScalarValue) -> Option<String> {
    match scalar {
        ScalarValue::Utf8(Some(s)) => Some(s.clone()),
        _ => None,
    }
}

fn format_created_filter(scalar: &ScalarValue, op: Operator) -> Option<String> {
    // Extract timestamp and convert to ISO 8601 format with operator
    let timestamp_ms = match scalar {
        ScalarValue::TimestampMillisecond(Some(ts), _) => *ts,
        ScalarValue::TimestampSecond(Some(ts), _) => ts * 1000,
        _ => return None,
    };

    // Convert to datetime string in ISO 8601 format
    let datetime = chrono::DateTime::from_timestamp_millis(timestamp_ms)?;
    let iso_string = datetime.format("%Y-%m-%dT%H:%M:%SZ").to_string();

    // GitHub uses comparison operators in the created parameter
    // Format: >2024-01-01, >=2024-01-01, <2024-01-01, <=2024-01-01
    let operator_str = match op {
        Operator::Gt => ">",
        Operator::GtEq => ">=",
        Operator::Lt => "<",
        Operator::LtEq => "<=",
        _ => return None,
    };

    Some(format!("{operator_str}{iso_string}"))
}

#[derive(Debug)]
pub struct WorkflowRunsTableProvider {
    client: Arc<GithubRestClient>,
    owner: Arc<str>,
    repo: Arc<str>,
    workflow_id: Arc<str>,
    schema: SchemaRef,
    fetch_logs: bool,
}

impl WorkflowRunsTableProvider {
    pub async fn new(
        client: GithubRestClient,
        owner: &str,
        repo: &str,
        workflow_id: &str,
        fetch_logs: bool,
        dataset: &Dataset,
    ) -> crate::dataconnector::DataConnectorResult<Self> {
        let mut fields = vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("head_branch", DataType::Utf8, true),
            Field::new("head_sha", DataType::Utf8, false),
            Field::new("run_number", DataType::Int64, false),
            Field::new("display_title", DataType::Utf8, false),
            Field::new("event", DataType::Utf8, false),
            Field::new("status", DataType::Utf8, true),
            Field::new("conclusion", DataType::Utf8, true),
            Field::new("workflow_id", DataType::Int64, false),
            Field::new(
                "run_started_at",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ),
            Field::new("jobs_url", DataType::Utf8, false),
        ];

        if fetch_logs {
            fields.push(Field::new(
                "logs",
                DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(
                            vec![
                                Field::new("keys", DataType::Utf8, false),
                                Field::new("values", DataType::Utf8, true),
                            ]
                            .into(),
                        ),
                        false,
                    )),
                    false,
                ),
                true,
            ));
        }

        let schema = Arc::new(Schema::new(fields));

        // Validate access by fetching a limited set of workflow runs
        let client = Arc::new(client);
        Arc::clone(&client)
            .fetch_workflow_runs(
                owner.into(),
                repo.into(),
                workflow_id.into(),
                None,
                Some(1),
                false,
            )
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
            workflow_id: workflow_id.into(),
            schema,
            fetch_logs,
        })
    }
}

#[async_trait]
impl TableProvider for WorkflowRunsTableProvider {
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
        let mut pushdown_support = Vec::new();

        for filter in filters {
            if let Some((column, _, op)) = extract_filter_column_value_op(filter) {
                let support = match (column.name.as_str(), op) {
                    ("head_branch" | "status" | "head_sha", Operator::Eq)
                    | (
                        "run_started_at",
                        Operator::Gt | Operator::GtEq | Operator::Lt | Operator::LtEq,
                    ) => TableProviderFilterPushDown::Inexact, // Inexact, because a user could specify multiple values but the REST API only supports single value filtering
                    _ => TableProviderFilterPushDown::Unsupported,
                };
                pushdown_support.push(support);
            } else {
                pushdown_support.push(TableProviderFilterPushDown::Unsupported);
            }
        }

        Ok(pushdown_support)
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let (workflow_filters, remaining_filters) = WorkflowRunFilters::from_filters(filters);

        let query_params = workflow_filters.to_query_params();

        tracing::debug!("Pushing down filters to GitHub API: {query_params:?}");
        tracing::debug!("Remaining filters after pushdown: {remaining_filters:?}");

        let github_plan = Arc::new(WorkflowRunsExecutionPlan {
            owner: Arc::clone(&self.owner),
            repo: Arc::clone(&self.repo),
            workflow_id: Arc::clone(&self.workflow_id),
            query_params: if query_params.is_empty() {
                None
            } else {
                Some(query_params)
            },
            limit,
            fetch_logs: self.fetch_logs,
            schema: Arc::clone(&self.schema),
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
                    Arc::new(physical_expr::expressions::Column::new(col_name, *idx))
                        as Arc<dyn PhysicalExpr>,
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
struct WorkflowRunsExecutionPlan {
    owner: Arc<str>,
    repo: Arc<str>,
    workflow_id: Arc<str>,
    query_params: Option<HashMap<String, String>>,
    limit: Option<usize>,
    fetch_logs: bool,
    schema: SchemaRef,
    client: Arc<GithubRestClient>,
    properties: PlanProperties,
}

impl DisplayAs for WorkflowRunsExecutionPlan {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(
            f,
            "GitHubWorkflowRunsExecutionPlan: {}/{} workflow_id={} fetch_logs={} limit={:?} query_params={:?}",
            self.owner, self.repo, self.workflow_id, self.fetch_logs, self.limit, self.query_params
        )
    }
}

#[deny(clippy::missing_trait_methods)]
impl ExecutionPlan for WorkflowRunsExecutionPlan {
    fn name(&self) -> &'static str {
        "GitHubWorkflowRunsExecutionPlan"
    }

    fn static_name() -> &'static str
    where
        Self: Sized,
    {
        "GitHubWorkflowRunsExecutionPlan"
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
        let workflow_id = Arc::clone(&self.workflow_id);
        let query_params = self.query_params.clone();
        let limit = self.limit;
        let fetch_logs = self.fetch_logs;
        let client = Arc::clone(&self.client);

        let stream = futures::stream::once(
            client
                .fetch_workflow_runs(owner, repo, workflow_id, query_params, limit, fetch_logs)
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
