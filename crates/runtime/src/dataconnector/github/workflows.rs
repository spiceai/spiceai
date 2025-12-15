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
    datasource::{TableProvider, TableType},
    error::DataFusionError,
    logical_expr::{Expr, TableProviderFilterPushDown},
    physical_plan::ExecutionPlan,
};
use std::{any::Any, sync::Arc};

use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};

use super::ConnectorComponent;

#[derive(Debug)]
pub struct WorkflowsTableProvider {
    client: GithubRestClient,
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
        client
            .fetch_workflows(owner, repo, Some(1))
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
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let batches = self
            .client
            .fetch_workflows(&self.owner, &self.repo, limit)
            .await
            .map_err(DataFusionError::External)?;

        let table = data_components::arrow::write::MemTable::try_new(
            Arc::clone(&self.schema),
            vec![batches],
        )?;

        table.scan(state, projection, filters, limit).await
    }
}
