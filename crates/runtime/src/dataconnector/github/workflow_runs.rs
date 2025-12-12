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

use crate::component::dataset::Dataset;
use async_trait::async_trait;
use data_components::github::GithubRestClient;
use datafusion::{
    catalog::Session,
    common::Column,
    datasource::{TableProvider, TableType},
    error::DataFusionError,
    logical_expr::{Expr, Operator, TableProviderFilterPushDown},
    physical_plan::ExecutionPlan,
    scalar::ScalarValue,
};
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
                        if let Some(val) = scalar_to_string(&value) {
                            workflow_filters.branch = Some(val);
                            continue;
                        }
                    }
                    ("status", Operator::Eq) => {
                        if let Some(val) = scalar_to_string(&value) {
                            workflow_filters.status = Some(val);
                            continue;
                        }
                    }
                    ("head_sha", Operator::Eq) => {
                        if let Some(val) = scalar_to_string(&value) {
                            workflow_filters.head_sha = Some(val);
                            continue;
                        }
                    }
                    (
                        "run_started_at",
                        Operator::Gt | Operator::GtEq | Operator::Lt | Operator::LtEq,
                    ) => {
                        if let Some(val) = format_created_filter(&value, &op) {
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

fn format_created_filter(scalar: &ScalarValue, op: &Operator) -> Option<String> {
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

    Some(format!("{}{}", operator_str, iso_string))
}

#[derive(Debug)]
pub struct WorkflowRunsTableProvider {
    client: GithubRestClient,
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
        client
            .fetch_workflow_runs(owner, repo, workflow_id, None, Some(1), false)
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
                    ("head_branch", Operator::Eq) => TableProviderFilterPushDown::Exact,
                    ("status", Operator::Eq) => TableProviderFilterPushDown::Exact,
                    ("head_sha", Operator::Eq) => TableProviderFilterPushDown::Exact,
                    (
                        "run_started_at",
                        Operator::Gt | Operator::GtEq | Operator::Lt | Operator::LtEq,
                    ) => TableProviderFilterPushDown::Exact,
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
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let (workflow_filters, remaining_filters) = WorkflowRunFilters::from_filters(filters);

        let query_params = workflow_filters.to_query_params();

        tracing::debug!("Pushing down filters to GitHub API: {:?}", query_params);

        let batches = self
            .client
            .fetch_workflow_runs(
                &self.owner,
                &self.repo,
                &self.workflow_id,
                Some(&query_params),
                limit,
                self.fetch_logs,
            )
            .await
            .map_err(|e| DataFusionError::External(e.into()))?;

        let table = data_components::arrow::write::MemTable::try_new(
            Arc::clone(&self.schema),
            vec![batches],
        )?;

        table
            .scan(state, projection, &remaining_filters, limit)
            .await
    }
}
