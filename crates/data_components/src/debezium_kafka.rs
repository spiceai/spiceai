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

use std::sync::Arc;

use crate::kafka::{
    CatchUp, KafkaMessage, KafkaOffsetCommitHook, MessageBatchCommitter, changes_after_catch_up,
    rdkafka::error::KafkaError,
};
use crate::{
    cdc::{self, ChangeEnvelope, ChangesStream},
    debezium::{
        arrow::changes,
        change_event::{ChangeEvent, ChangeEventKey},
    },
    kafka::{Error, KafkaConsumer},
    schema_projection::SchemaProjection,
};
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::{
    catalog::Session,
    common::{Constraint, Constraints, DFSchema, project_schema},
    datasource::{TableProvider, TableType},
    error::Result as DataFusionResult,
    logical_expr::Expr,
    physical_plan::{ExecutionPlan, empty::EmptyExec},
};
use tokio::time::Duration;
use tokio_stream::StreamExt;

pub struct DebeziumKafka {
    schema: SchemaRef,
    primary_keys: Vec<String>,
    constraints: Option<Constraints>,
    consumer: &'static KafkaConsumer,
    batching: (usize, Duration),
    offset_commit_hook: Option<Arc<dyn KafkaOffsetCommitHook>>,
    /// JSON-nesting / declared-schema projection applied to each change row's
    /// `before`/`after` payload before Arrow conversion. `None` ⇒ passthrough.
    projection: Option<SchemaProjection>,
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
    pub fn new(
        schema: SchemaRef,
        primary_keys: Vec<String>,
        consumer: KafkaConsumer,
        batching: (usize, Duration),
        projection: Option<SchemaProjection>,
    ) -> Self {
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
            batching,
            offset_commit_hook: None,
            projection,
        }
    }

    #[must_use]
    pub fn with_offset_commit_hook(
        mut self,
        offset_commit_hook: Arc<dyn KafkaOffsetCommitHook>,
    ) -> Self {
        self.offset_commit_hook = Some(offset_commit_hook);
        self
    }

    #[must_use]
    pub fn get_primary_keys(&self) -> &Vec<String> {
        &self.primary_keys
    }

    #[must_use]
    pub fn stream_changes(&self) -> ChangesStream {
        let schema = Arc::clone(&self.schema);
        let primary_keys = self.primary_keys.clone();
        let consumer = self.consumer;
        let offset_commit_hook = self.offset_commit_hook.clone();
        let projection = self.projection.clone();
        let batching = self.batching;
        changes_after_catch_up(consumer, Arc::clone(&self.schema), move |mut catch_up| {
            let signal_schema = Arc::clone(&schema);
            let to_envelope =
                move |messages: &[KafkaMessage<'static, ChangeEventKey, ChangeEvent>],
                      catch_up: &mut CatchUp| {
                    let changes: Vec<_> = messages.iter().map(KafkaMessage::value).collect();

                    // Newest upstream commit timestamp in the batch, for the
                    // replication-lag signal: prefer the source DB commit time
                    // (`source.ts_ms`), falling back to the connector envelope time
                    // (`payload.ts_ms`) when the source time is absent (0).
                    let source_commit_ts_ms = changes
                        .iter()
                        .map(|change| {
                            let source_ts = change.payload.source.ts_ms;
                            if source_ts != 0 {
                                source_ts
                            } else {
                                change.payload.ts_ms
                            }
                        })
                        .max();

                    let rb = changes::vector_to_change_batch(
                        &schema,
                        &primary_keys,
                        &changes,
                        projection.as_ref(),
                    )
                    .map_err(|e| cdc::StreamError::SerdeJsonError(e.to_string()))?
                    .with_source_commit_ts_ms(source_commit_ts_ms);

                    let committer = MessageBatchCommitter::from_messages(consumer, messages)
                        .with_offset_commit_hook(offset_commit_hook.clone());

                    Ok(ChangeEnvelope::new(
                        Box::new(committer),
                        rb,
                        catch_up.mark_ready(),
                    ))
                };
            let batches = consumer
                .stream_json::<ChangeEventKey, ChangeEvent>()
                .chunks_timeout(batching.0, batching.1);
            futures::StreamExt::flat_map(batches, move |msgs| {
                if msgs.is_empty() {
                    return futures::stream::iter(vec![Err(cdc::StreamError::Kafka(
                        Error::EmptyBatch,
                    ))]);
                }

                let mut messages = Vec::with_capacity(msgs.len());
                let mut error = None;
                for msg in msgs {
                    match msg {
                        Ok(message) => {
                            catch_up.observe_message(message.partition(), message.offset());
                            messages.push(message);
                        }
                        Err(Error::UnableToReceiveMessage {
                            source: KafkaError::PartitionEOF(partition),
                        }) => catch_up.observe_partition_end(partition),
                        Err(e) => {
                            error.get_or_insert(e);
                        }
                    }
                }

                let mut items = Vec::with_capacity(2);
                // An error fails the whole batch.
                if let Some(e) = error {
                    items.push(Err(cdc::StreamError::Kafka(e)));
                } else if !messages.is_empty() {
                    items.push(to_envelope(&messages, &mut catch_up));
                }
                items.extend(catch_up.ready_signal(&signal_schema));
                futures::stream::iter(items)
            })
        })
    }
}

#[async_trait]
impl TableProvider for DebeziumKafka {
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
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(EmptyExec::new(project_schema(
            &self.schema,
            projection,
        )?)))
    }
}
