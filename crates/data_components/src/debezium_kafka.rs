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

use crate::{
    debezium::{
        arrow::changes,
        change_event::{ChangeEvent, ChangeEventKey},
    },
    kafka::KafkaConsumer,
};
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::{
    common::{Constraints, DFSchema},
    datasource::{TableProvider, TableType},
    error::{DataFusionError, Result as DataFusionResult},
    execution::{context::SessionState, SendableRecordBatchStream, TaskContext},
    logical_expr::Expr,
    physical_expr::EquivalenceProperties,
    physical_plan::{
        empty::EmptyExec, stream::RecordBatchStreamAdapter, DisplayAs, DisplayFormatType,
        ExecutionMode, ExecutionPlan, Partitioning, PlanProperties,
    },
    sql::sqlparser::ast::{Ident, TableConstraint},
};
use futures::StreamExt;
use std::{any::Any, fmt, sync::Arc};

pub struct DebeziumKafka {
    schema: SchemaRef,
    primary_keys: Vec<String>,
    constraints: Option<Constraints>,
    consumer: &'static KafkaConsumer,
}

impl DebeziumKafka {
    #[must_use]
    pub fn new(schema: SchemaRef, primary_keys: Vec<String>, consumer: KafkaConsumer) -> Self {
        let Ok(df_schema) = DFSchema::try_from(Arc::clone(&schema)) else {
            unreachable!("DFSchema::try_from is infallible as of DataFusion 38")
        };
        let constraints = Constraints::new_from_table_constraints(
            &[TableConstraint::PrimaryKey {
                name: None,
                index_name: None,
                index_type: None,
                columns: primary_keys
                    .iter()
                    .map(|col| Ident::new(col.clone()))
                    .collect(),
                index_options: vec![],
                characteristics: None,
            }],
            &Arc::new(df_schema),
        )
        .ok();
        Self {
            schema,
            primary_keys,
            constraints,
            consumer: Box::leak(Box::new(consumer)),
        }
    }

    #[must_use]
    pub fn get_primary_keys(&self) -> &Vec<String> {
        &self.primary_keys
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

    fn constraints(&self) -> Option<&Constraints> {
        self.constraints.as_ref()
    }

    async fn scan(
        &self,
        _state: &SessionState,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(EmptyExec::new(Arc::clone(&self.schema))) as Arc<dyn ExecutionPlan>)
    }
}

#[derive(Clone)]
pub struct DebeziumKafkaExec {
    schema: SchemaRef,
    primary_keys: Vec<String>,
    consumer: &'static KafkaConsumer,
    properties: PlanProperties,
}

impl DebeziumKafkaExec {
    #[must_use]
    pub fn new(
        schema: SchemaRef,
        primary_keys: Vec<String>,
        consumer: &'static KafkaConsumer,
    ) -> Self {
        Self {
            schema: Arc::clone(&schema),
            primary_keys,
            consumer,
            properties: PlanProperties::new(
                EquivalenceProperties::new(schema),
                Partitioning::UnknownPartitioning(0),
                ExecutionMode::Unbounded,
            ),
        }
    }
}

impl std::fmt::Debug for DebeziumKafkaExec {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "DebeziumKafkaExec primary_keys={}",
            self.primary_keys.join(",")
        )
    }
}

impl DisplayAs for DebeziumKafkaExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "DebeziumKafkaExec primary_keys={}",
            self.primary_keys.join(",")
        )
    }
}

impl ExecutionPlan for DebeziumKafkaExec {
    fn name(&self) -> &'static str {
        "DebeziumKafkaExec"
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
        let consumer: &'static KafkaConsumer = self.consumer;
        let schema = Arc::clone(&self.schema());
        let primary_keys = self.primary_keys.clone();
        let stream = consumer
            .stream_json::<ChangeEventKey, ChangeEvent>()
            .filter_map(move |msg| {
                let schema = Arc::clone(&schema);
                let pk = primary_keys.clone();
                async move {
                    let Ok(msg) = msg else { return None };

                    let val = msg.value();
                    let rb = changes::to_record_batch(&schema, &pk, val)
                        .map_err(|e| DataFusionError::Execution(e.to_string()));

                    if let Err(e) = msg.mark_processed() {
                        return Some(Err(DataFusionError::Execution(format!(
                            "Unable to mark Kafka message as being processed: {e}",
                        ))));
                    };

                    Some(rb)
                }
            });

        let stream_adapter = RecordBatchStreamAdapter::new(self.schema(), stream);

        Ok(Box::pin(stream_adapter))
    }
}
