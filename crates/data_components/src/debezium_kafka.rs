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
    cdc::{self, ChangeEnvelope, ChangesStream},
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
    error::Result as DataFusionResult,
    execution::context::SessionState,
    logical_expr::Expr,
    physical_plan::{empty::EmptyExec, ExecutionPlan},
    sql::sqlparser::ast::{Ident, TableConstraint},
};
use futures::StreamExt;
use std::{any::Any, sync::Arc};

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

    #[must_use]
    pub fn stream_changes(&self) -> ChangesStream {
        let schema = Arc::clone(&self.schema);
        let primary_keys = self.primary_keys.clone();
        let stream = self
            .consumer
            .stream_json::<ChangeEventKey, ChangeEvent>()
            .map(move |msg| {
                let schema = Arc::clone(&schema);
                let pk = primary_keys.clone();
                let Ok(msg) = msg else {
                    return Err(cdc::StreamError::Kafka(
                        "Unable to read message".to_string(),
                    ));
                };

                let val = msg.value();
                changes::to_change_batch(&schema, &pk, val)
                    .map(|rb| ChangeEnvelope::new(Some(Box::new(msg)), rb))
                    .map_err(|e| cdc::StreamError::SerdeJsonError(e.to_string()))
            });

        Box::pin(stream)
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
