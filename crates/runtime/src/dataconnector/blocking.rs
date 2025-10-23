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

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;

use std::{any::Any, pin::Pin, sync::Arc};

use crate::component::dataset::{Dataset, acceleration::RefreshMode};
use datafusion::{
    catalog::Session,
    common::{Constraints, project_schema},
    datasource::{TableProvider, TableType},
    logical_expr::Expr,
    physical_plan::{ExecutionPlan, empty::EmptyExec},
};
use futures::Future;

use super::{ConnectorParams, DataConnector, DataConnectorFactory, ParameterSpec};

/// A no-op connector that blocks the Tokio runtime when used. For testing only.
#[derive(Debug, Clone)]
pub struct BlockingConnector {
    schema: SchemaRef,
}

impl BlockingConnector {
    #[must_use]
    pub fn new() -> Self {
        Self {
            schema: Arc::new(Schema::new(vec![Field::new(
                "placeholder",
                DataType::Utf8,
                false,
            )])),
        }
    }
}

#[derive(Default, Copy, Clone)]
pub struct BlockingConnectorFactory {}

impl BlockingConnectorFactory {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }

    #[must_use]
    pub fn new_arc() -> Arc<dyn DataConnectorFactory> {
        Arc::new(Self {}) as Arc<dyn DataConnectorFactory>
    }
}

impl DataConnectorFactory for BlockingConnectorFactory {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create(
        &self,
        _params: ConnectorParams,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move { Ok(Arc::new(BlockingConnector::new()) as Arc<dyn DataConnector>) })
    }

    fn prefix(&self) -> &'static str {
        "blocking"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        &[]
    }
}

#[async_trait]
impl DataConnector for BlockingConnector {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn resolve_refresh_mode(&self, refresh_mode: Option<RefreshMode>) -> RefreshMode {
        refresh_mode.unwrap_or(RefreshMode::Disabled)
    }

    async fn read_provider(
        &self,
        _dataset: &Dataset,
    ) -> super::DataConnectorResult<Arc<dyn TableProvider>> {
        Ok(Arc::new(self.clone()))
    }
}

#[async_trait]
impl TableProvider for BlockingConnector {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn constraints(&self) -> Option<&Constraints> {
        None
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        std::thread::sleep(std::time::Duration::from_secs(30));
        Ok(Arc::new(EmptyExec::new(project_schema(
            &self.schema,
            projection,
        )?)))
    }
}
