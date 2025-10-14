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

use std::{any::Any, sync::Arc};

use super::DataConnector;
use crate::component::{ComponentInitialization, dataset::Dataset};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::{
    catalog::Session,
    common::{Constraints, project_schema},
    datasource::{TableProvider, TableType},
    logical_expr::Expr,
    physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, empty::EmptyExec},
};

#[derive(Clone)]
pub struct DeferredConnector {
    schema: SchemaRef,
    inner: Arc<dyn DataConnector>,
}

impl DeferredConnector {
    pub fn new(inner: Arc<dyn DataConnector>) -> Self {
        Self {
            inner,
            schema: Arc::new(Schema::new(vec![Field::new(
                "placeholder",
                DataType::Utf8,
                false,
            )])),
        }
    }

    #[must_use]
    pub fn source(&self) -> Arc<dyn DataConnector> {
        Arc::clone(&self.inner)
    }
}

#[async_trait]
impl DataConnector for DeferredConnector {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        _dataset: &Dataset,
    ) -> super::DataConnectorResult<Arc<dyn TableProvider>> {
        Ok(Arc::new(self.clone()))
    }

    fn initialization(&self) -> ComponentInitialization {
        ComponentInitialization::OnTrigger
    }
}

#[async_trait]
impl TableProvider for DeferredConnector {
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
        Ok(Arc::new(EmptyExec::new(project_schema(
            &self.schema,
            projection,
        )?)))
    }
}

impl std::fmt::Debug for DeferredConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "DeferredConnector")
    }
}

impl DisplayAs for DeferredConnector {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "DeferredConnector")
    }
}
