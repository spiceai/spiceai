/*
Copyright 2024-2026 The Spice.ai OSS Authors

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
    cdc::{self, ChangeBatch, ChangeEnvelope, ChangesStream},
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
    common::{Constraint, Constraints, DFSchema, TableReference, project_schema},
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
    dataset_name: String,
    runtime_status: Option<Arc<runtime_status::RuntimeStatus>>,
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
            dataset_name: String::new(),
            runtime_status: None,
        }
    }

    #[must_use]
    pub fn with_dataset_name(mut self, dataset_name: impl Into<String>) -> Self {
        self.dataset_name = dataset_name.into();
        self
    }

    #[must_use]
    pub fn with_runtime_status(
        mut self,
        runtime_status: Arc<runtime_status::RuntimeStatus>,
    ) -> Self {
        self.runtime_status = Some(runtime_status);
        self
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

    pub fn stream_changes(&self) -> ChangesStream {
        let schema = Arc::clone(&self.schema);
        let primary_keys = self.primary_keys.clone();
        let consumer = self.consumer;
        let offset_commit_hook = self.offset_commit_hook.clone();
        let projection = self.projection.clone();
        let batching = self.batching;
        let dataset_name = self.dataset_name.clone();
        let runtime_status = self.runtime_status.clone();
        runtime_metrics::acceleration::seed_cdc_decode_errors(&dataset_name);
        changes_after_catch_up(consumer, Arc::clone(&self.schema), move |mut catch_up| {
            let signal_schema = Arc::clone(&schema);
            let batches = consumer
                .stream_json::<ChangeEventKey, ChangeEvent>()
                .chunks_timeout(batching.0, batching.1);
            // The consumer's fetch position has moved past every message in a batch,
            // so each one is delivered here or never again. Receive errors go out
            // between the envelopes on either side; a decode failure skips that
            // event and keeps the rest of the chunk, in order.
            futures::StreamExt::flat_map(batches, move |msgs| {
                if msgs.is_empty() {
                    return futures::stream::iter(vec![Err(cdc::StreamError::Kafka(
                        Error::EmptyBatch,
                    ))]);
                }

                let mut items = Vec::new();
                let mut messages = Vec::new();
                let mut extra_offsets = Vec::new();
                for msg in msgs {
                    match msg {
                        Ok(message) => {
                            catch_up.observe_message(message.partition(), message.offset());
                            messages.push(message);
                        }
                        Err(Error::UnableToReceiveMessage {
                            source: KafkaError::PartitionEOF(partition),
                        }) => catch_up.observe_partition_end(partition),
                        Err(e) if e.is_change_decode_error() => {
                            if let Some((topic, partition, offset)) = e.kafka_location() {
                                catch_up.observe_message(partition, offset);
                                report_skipped_change_event(
                                    dataset_name.as_str(),
                                    topic,
                                    partition,
                                    offset,
                                    e.change_decode_kind().unwrap_or("json_parse"),
                                    runtime_status.as_ref(),
                                );
                                extra_offsets.push((topic.to_string(), partition, offset));
                            }
                        }
                        Err(e) => {
                            if !messages.is_empty() || !extra_offsets.is_empty() {
                                items.push(decode_debezium_chunk(
                                    DecodeChunk {
                                        dataset_name: dataset_name.as_str(),
                                        schema: &schema,
                                        primary_keys: &primary_keys,
                                        projection: projection.as_ref(),
                                        consumer,
                                        offset_commit_hook: offset_commit_hook.clone(),
                                        runtime_status: runtime_status.as_ref(),
                                    },
                                    &messages,
                                    &extra_offsets,
                                    &mut catch_up,
                                ));
                                messages.clear();
                                extra_offsets.clear();
                            }
                            items.push(Err(cdc::StreamError::Kafka(e)));
                        }
                    }
                }
                if !messages.is_empty() || !extra_offsets.is_empty() {
                    items.push(decode_debezium_chunk(
                        DecodeChunk {
                            dataset_name: dataset_name.as_str(),
                            schema: &schema,
                            primary_keys: &primary_keys,
                            projection: projection.as_ref(),
                            consumer,
                            offset_commit_hook: offset_commit_hook.clone(),
                            runtime_status: runtime_status.as_ref(),
                        },
                        &messages,
                        &extra_offsets,
                        &mut catch_up,
                    ));
                }
                items.extend(catch_up.ready_signal(&signal_schema));
                futures::stream::iter(items)
            })
        })
    }
}

struct DecodeChunk<'a> {
    dataset_name: &'a str,
    schema: &'a SchemaRef,
    primary_keys: &'a [String],
    projection: Option<&'a SchemaProjection>,
    consumer: &'static KafkaConsumer,
    offset_commit_hook: Option<Arc<dyn KafkaOffsetCommitHook>>,
    runtime_status: Option<&'a Arc<runtime_status::RuntimeStatus>>,
}

fn event_commit_ts_ms(change: &ChangeEvent) -> i64 {
    let source_ts = change.payload.source.ts_ms;
    if source_ts == 0 {
        change.payload.ts_ms
    } else {
        source_ts
    }
}

/// User-facing skip message: dataset, Kafka location, and error kind only.
#[must_use]
pub(crate) fn skipped_change_event_log(
    dataset: &str,
    topic: &str,
    partition: i32,
    offset: i64,
    kind: &str,
) -> String {
    format!(
        "Failed to decode a change event for dataset '{dataset}' from Kafka topic '{topic}' partition {partition} offset {offset}, so that event was skipped and the dataset is Error until a full refresh or restart. Cause: {kind}. See: https://spiceai.org/docs/components/data-connectors/debezium"
    )
}

fn report_skipped_change_event(
    dataset: &str,
    topic: &str,
    partition: i32,
    offset: i64,
    kind: &str,
    runtime_status: Option<&Arc<runtime_status::RuntimeStatus>>,
) {
    let message = skipped_change_event_log(dataset, topic, partition, offset, kind);
    tracing::error!("{message}");
    runtime_metrics::acceleration::record_cdc_decode_error(dataset);
    if let Some(status) = runtime_status {
        status.mark_changes_decode_degraded(&TableReference::parse_str(dataset), message);
    }
}

fn decode_one_change(
    schema: &SchemaRef,
    primary_keys: &[String],
    change: &ChangeEvent,
    projection: Option<&SchemaProjection>,
) -> Result<ChangeBatch, &'static str> {
    let source_commit_ts_ms = event_commit_ts_ms(change);
    changes::to_change_batch(schema, primary_keys, change, projection)
        .map(|batch| batch.with_source_commit_ts_ms(Some(source_commit_ts_ms)))
        .map_err(|e| e.kind())
}

fn concat_decoded_change_batches(
    schema: &SchemaRef,
    primary_keys: &[String],
    projection: Option<&SchemaProjection>,
    mut batches: Vec<ChangeBatch>,
) -> Result<ChangeBatch, cdc::StreamError> {
    match batches.len() {
        0 => changes::vector_to_change_batch(schema, primary_keys, &[], projection).map_err(|e| {
            cdc::StreamError::Connector {
                connector: "Debezium",
                source: Box::new(e),
            }
        }),
        1 => Ok(batches.swap_remove(0)),
        _ => {
            let record_schema = batches[0].record.schema();
            let combined =
                arrow::compute::concat_batches(&record_schema, batches.iter().map(|b| &b.record))
                    .map_err(|e| cdc::StreamError::Arrow(e.to_string()))?;
            let source_commit_ts_ms = batches
                .iter()
                .filter_map(ChangeBatch::source_commit_ts_ms)
                .max();
            ChangeBatch::try_new(combined)
                .map(|batch| batch.with_source_commit_ts_ms(source_commit_ts_ms))
                .map_err(|e| cdc::StreamError::External(e.to_string()))
        }
    }
}

fn decode_debezium_chunk(
    ctx: DecodeChunk<'_>,
    messages: &[KafkaMessage<'static, ChangeEventKey, ChangeEvent>],
    extra_offsets: &[(String, i32, i64)],
    catch_up: &mut CatchUp,
) -> Result<ChangeEnvelope, cdc::StreamError> {
    let mut decoded = Vec::with_capacity(messages.len());
    for message in messages {
        match decode_one_change(
            ctx.schema,
            ctx.primary_keys,
            message.value(),
            ctx.projection,
        ) {
            Ok(batch) => decoded.push(batch),
            Err(kind) => report_skipped_change_event(
                ctx.dataset_name,
                message.topic(),
                message.partition(),
                message.offset(),
                kind,
                ctx.runtime_status,
            ),
        }
    }

    let change_batch =
        concat_decoded_change_batches(ctx.schema, ctx.primary_keys, ctx.projection, decoded)?;
    let positions = messages
        .iter()
        .map(|message| {
            (
                message.topic().to_string(),
                message.partition(),
                message.offset(),
            )
        })
        .chain(extra_offsets.iter().cloned());
    let committer = MessageBatchCommitter::from_offsets(ctx.consumer, positions)
        .with_offset_commit_hook(ctx.offset_commit_hook);

    Ok(ChangeEnvelope::new(
        Box::new(committer),
        change_batch,
        catch_up.mark_ready(),
    ))
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

#[cfg(test)]
mod tests {
    use super::skipped_change_event_log;

    const SECRET: &str = "s3cr3t-token-value";

    #[test]
    fn skipped_change_event_log_names_location_and_kind_without_payload() {
        let log = skipped_change_event_log(
            "orders",
            "dbserver1.public.orders",
            0,
            42,
            "missing_required_field",
        );
        assert!(log.contains("dataset 'orders'"));
        assert!(log.contains("topic 'dbserver1.public.orders'"));
        assert!(log.contains("partition 0"));
        assert!(log.contains("offset 42"));
        assert!(log.contains("missing_required_field"));
        assert!(log.contains("https://spiceai.org/docs/components/data-connectors/debezium"));
        assert!(
            !log.contains(SECRET),
            "skip log must not include payload values, got: {log}"
        );
    }
}
