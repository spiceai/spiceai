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

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;

use std::{any::Any, pin::Pin, sync::Arc};

use crate::component::dataset::{acceleration::RefreshMode, Dataset};
use datafusion::{
    catalog::Session,
    datasource::{TableProvider, TableType},
    logical_expr::Expr,
    physical_plan::{empty::EmptyExec, ExecutionPlan},
};
use futures::Future;

use super::{DataConnector, DataConnectorFactory, ParameterSpec, Parameters};

/// A no-op connector that allows for Spice to act as a "sink" for data.
///
/// Configure an accelerator to store data - the sink connector itself does nothing.
#[derive(Debug, Clone)]
pub struct SinkConnector {
    schema: SchemaRef,
}

impl SinkConnector {
    #[must_use]
    pub fn new(schema: SchemaRef) -> Self {
        Self { schema }
    }
}

#[derive(Default, Copy, Clone)]
pub struct SinkConnectorFactory {}

impl SinkConnectorFactory {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }

    #[must_use]
    pub fn new_arc() -> Arc<dyn DataConnectorFactory> {
        Arc::new(Self {}) as Arc<dyn DataConnectorFactory>
    }
}

impl DataConnectorFactory for SinkConnectorFactory {
    fn create(
        &self,
        _params: Parameters,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            let schema = Schema::new(vec![Field::new("placeholder", DataType::Utf8, false)]);

            Ok(Arc::new(SinkConnector::new(Arc::new(schema))) as Arc<dyn DataConnector>)
        })
    }

    fn prefix(&self) -> &'static str {
        "sink"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        &[]
    }
}

#[async_trait]
impl DataConnector for SinkConnector {
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

    async fn read_write_provider(
        &self,
        _dataset: &Dataset,
    ) -> Option<super::DataConnectorResult<Arc<dyn TableProvider>>> {
        Some(Ok(Arc::new(self.clone())))
    }
}

#[async_trait]
impl TableProvider for SinkConnector {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(EmptyExec::new(Arc::clone(&self.schema))))
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        _input: Arc<dyn ExecutionPlan>,
        _overwrite: bool,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(EmptyExec::new(Arc::clone(&self.schema))))
    }
}
