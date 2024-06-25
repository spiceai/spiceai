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

use std::{any::Any, sync::Arc};

use crate::kafka::KafkaConsumer;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::{
    datasource::{TableProvider, TableType},
    error::Result as DataFusionResult,
    execution::context::SessionState,
    logical_expr::Expr,
    physical_plan::{empty::EmptyExec, ExecutionPlan},
};

pub struct DebeziumKafka {
    schema: SchemaRef,
    _consumer: KafkaConsumer,
}

impl DebeziumKafka {
    #[must_use]
    pub fn new(schema: SchemaRef, consumer: KafkaConsumer) -> Self {
        Self {
            schema,
            _consumer: consumer,
        }
    }
}

#[async_trait]
impl TableProvider for DebeziumKafka {
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
        _state: &SessionState,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(EmptyExec::new(self.schema())))
    }
}
