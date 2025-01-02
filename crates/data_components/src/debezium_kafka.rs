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
    catalog::Session,
    common::{Constraint, Constraints, DFSchema},
    datasource::{TableProvider, TableType},
    error::Result as DataFusionResult,
    logical_expr::Expr,
    physical_plan::{empty::EmptyExec, ExecutionPlan},
};
use futures::StreamExt;
use std::{any::Any, sync::Arc};

pub struct DebeziumKafka {
    schema: SchemaRef,
    primary_keys: Vec<String>,
    constraints: Option<Constraints>,
    consumer: &'static KafkaConsumer,
}

impl std::fmt::Debug for DebeziumKafka {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DebeziumKafka")
            .field("schema", &self.schema)
            .field("primary_keys", &self.primary_keys)
            .field("constraints", &self.constraints)
            .finish_non_exhaustive()
    }
}

impl DebeziumKafka {
    #[must_use]
    pub fn new(schema: SchemaRef, primary_keys: Vec<String>, consumer: KafkaConsumer) -> Self {
        let Ok(df_schema) = DFSchema::try_from(Arc::clone(&schema)) else {
            unreachable!("DFSchema::try_from is infallible as of DataFusion 38")
        };

        // Get the indices of primary key columns in the schema
        let pk_indices: Vec<usize> = primary_keys
            .iter()
            .filter_map(|pk| df_schema.index_of_column_by_name(None, pk))
            .collect();

        // Create constraints with the primary key indices
        let constraints = if pk_indices.is_empty() {
            None
        } else {
            Some(Constraints::new_unverified(vec![Constraint::PrimaryKey(
                pk_indices,
            )]))
        };

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
                    .map(|rb| ChangeEnvelope::new(Box::new(msg), rb))
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
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(EmptyExec::new(Arc::clone(&self.schema))) as Arc<dyn ExecutionPlan>)
    }
}
