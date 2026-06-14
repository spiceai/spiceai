/*
Copyright 2026 The Spice.ai OSS Authors

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

use async_stream::try_stream;
use async_trait::async_trait;
use data_components::{
    cdc::{
        ChangeEnvelope, ChangesStream, CommitChange, CommitError, NoOpCommitter, StreamError,
        build_ready_signal_envelope, wrap_data_as_change_batch,
    },
    mongodb::stream::{
        change_events_to_change_batch, default_unnest_parameters, truncate_change_batch,
    },
};
use datafusion::{
    arrow::datatypes::SchemaRef, datasource::TableProvider,
    physical_plan::SendableRecordBatchStream, prelude::SessionContext,
};
use datafusion_table_providers::mongodb::connection_pool::MongoDBConnectionPool;
use futures::StreamExt as FuturesStreamExt;
use mongodb::{
    Collection,
    bson::Document,
    change_stream::{ChangeStream, event::ChangeStreamEvent, event::ResumeToken},
    options::FullDocumentType,
};
use runtime::{
    component::dataset::{
        Dataset,
        acceleration::{Acceleration, Engine, OnConflictBehavior},
    },
    dataaccelerator::spice_sys::{
        OpenOption,
        mongodb::{MongoCheckpointMetadata, MongoSys},
    },
    federated_table::FederatedTable,
    parameters::{ExposedParamLookup, Parameters},
};
use std::{sync::Arc, time::Duration};
use tokio_stream::StreamExt as TokioStreamExt;

const DEFAULT_CHANGE_STREAM_BATCH_MAX_SIZE: usize = 1_000;
const DEFAULT_CHANGE_STREAM_BATCH_SIZE: u32 = 1_000;
const DEFAULT_CHANGE_STREAM_BATCH_MAX_DURATION: Duration = Duration::from_secs(1);
const DEFAULT_CHANGE_STREAM_MAX_AWAIT_TIME: Duration = Duration::from_secs(1);

pub fn build_changes_stream(
    pool: Arc<MongoDBConnectionPool>,
    params: Parameters,
    dataset: Dataset,
    federated_table: Arc<FederatedTable>,
) -> ChangesStream {
    // `try_stream!` keeps MongoDB cursor polling, snapshot reads, and commit-aware
    // CDC yields in one backpressured stream; a spawned channel would risk buffering
    // checkpoints ahead of downstream accelerator commits.
    Box::pin(try_stream! {
        let table_provider = federated_table.table_provider().await;
        let schema = table_provider.schema();
        let primary_keys = resolve_primary_keys(&dataset.name, dataset.acceleration.as_ref(), &schema)?;
        let config = ChangeStreamConfig::from_params(&params)?;
        let invalid_token_behavior = ResumeTokenInvalidBehavior::from_params(&params)?;
        let collection_name = dataset.path().to_string();

        let connection = pool
            .connect()
            .await
            .map_err(|error| StreamError::External(format!(
                "Failed to connect to MongoDB Change Stream for dataset `{}` collection `{collection_name}`: {error}",
                dataset.name
            )))?;
        let collection = connection
            .client
            .database(&connection.db_name)
            .collection::<Document>(&collection_name);

        let mongo_sys = if dataset.is_file_accelerated() {
            initialize_mongo_sys(&dataset).await
        } else {
            tracing::info!(
                dataset = %dataset.name,
                collection = %collection_name,
                "MongoDB Change Stream dataset is not file-accelerated; resume token will not be persisted across restarts"
            );
            None
        };

        let current_schema_json = serialize_current_schema(&schema, &dataset.name);
        let persisted =
            persisted_checkpoint(mongo_sys.as_deref(), &dataset, current_schema_json.as_deref())
                .await;

        let live_change_stream = if let Some(metadata) = persisted {
            let resume_token = deserialize_resume_token(&metadata.resume_token_json)
                .map_err(|error| StreamError::External(format!(
                    "Failed to deserialize persisted MongoDB resume token for dataset `{}` collection `{collection_name}`: {error}. To recover, delete the dataset's row from `spice_sys_mongodb` or restart with `mongodb_resume_token_invalid_behavior: rebootstrap`.",
                    dataset.name
                )))?;

            match try_open_change_stream(&collection, &config, Some(resume_token)).await {
                Ok(stream) => {
                    tracing::info!(
                        dataset = %dataset.name,
                        collection = %collection_name,
                        "MongoDB Change Stream resumed from persisted resume token; skipping collection snapshot"
                    );

                    let ready = build_ready_signal_envelope(&schema)
                        .map_err(|error| StreamError::Arrow(error.to_string()))?;
                    yield ready;
                    Some(stream)
                }
                Err(error) if is_stale_resume_token_error(&error) => match invalid_token_behavior {
                    ResumeTokenInvalidBehavior::Error => Err(StreamError::External(format!(
                        "MongoDB Change Stream resume token for dataset `{}` collection `{collection_name}` is past the oplog retention window or otherwise invalid (driver code {}). Set `mongodb_resume_token_invalid_behavior: rebootstrap` to drop the persisted token and re-snapshot the collection. Source: {error}",
                        dataset.name,
                        resume_token_error_code(&error).map_or_else(|| "unknown".to_string(), |c| c.to_string()),
                    )))?,
                    ResumeTokenInvalidBehavior::Rebootstrap => {
                        tracing::warn!(
                            dataset = %dataset.name,
                            collection = %collection_name,
                            error = %error,
                            "MongoDB Change Stream resume token is stale; rebootstrap behavior enabled, falling back to cold bootstrap"
                        );
                        clear_persisted_token(mongo_sys.as_deref(), &dataset).await;
                        None
                    }
                },
                Err(error) => Err(StreamError::External(format!(
                    "Failed to start MongoDB Change Stream for dataset `{}` collection `{collection_name}` while resuming from persisted token: {error}",
                    dataset.name
                )))?,
            }
        } else {
            None
        };

        let live_change_stream = if let Some(stream) = live_change_stream {
            stream
        } else {
            let initial_change_stream = open_change_stream(
                &collection,
                &config,
                &dataset.name,
                &collection_name,
                None,
            )
            .await?;
            let resume_token = initial_change_stream.resume_token().ok_or_else(|| {
                StreamError::External(format!(
                    "Failed to start MongoDB Change Stream for dataset `{}` collection `{collection_name}`: initial stream did not return a resume token",
                    dataset.name
                ))
            })?;
            drop(initial_change_stream);

            tracing::info!(
                dataset = %dataset.name,
                collection = %collection_name,
                "MongoDB Change Stream started; bootstrapping accelerator from collection snapshot"
            );

            let truncate = truncate_change_batch(&schema)
                .map_err(StreamError::MongoDB)?;
            yield ChangeEnvelope::new(Box::new(NoOpCommitter), truncate, false);

            let mut snapshot_stream = snapshot_stream(table_provider).await?;
            while let Some(batch) = FuturesStreamExt::next(&mut snapshot_stream).await {
                let batch = batch.map_err(|error| StreamError::Arrow(error.to_string()))?;
                if batch.num_rows() == 0 {
                    continue;
                }

                let change_batch = wrap_data_as_change_batch(&schema, &batch)
                    .map_err(|error| StreamError::Arrow(error.to_string()))?;
                yield ChangeEnvelope::new(Box::new(NoOpCommitter), change_batch, false);
            }

            // Commit the captured resume token piggy-backed on the ready signal envelope.
            // The committer fires after the downstream has fully persisted the empty
            // ready batch, which is the natural barrier between "bootstrap" and "live"
            // phases. A crash any time before this commit leaves the sidecar empty, so
            // the next start will re-bootstrap.
            let initial_token_json = serialize_resume_token(&resume_token)
                .map_err(|error| StreamError::External(format!(
                    "Failed to serialize MongoDB resume token for dataset `{}` collection `{collection_name}`: {error}",
                    dataset.name
                )))?;
            let ready = build_ready_signal_envelope(&schema)
                .map_err(|error| StreamError::Arrow(error.to_string()))?;
            let (_, batch, is_ready) = ready.into_parts();
            let committer: Box<dyn CommitChange + Send + Sync> = match mongo_sys.as_ref() {
                Some(sys) => Box::new(MongoResumeTokenCommitter::new(
                    Arc::clone(sys),
                    initial_token_json,
                    None,
                    current_schema_json.clone(),
                )),
                None => Box::new(NoOpCommitter),
            };
            yield ChangeEnvelope::from_parts(committer, batch, is_ready);

            tracing::info!(
                dataset = %dataset.name,
                collection = %collection_name,
                "MongoDB collection snapshot complete; resuming Change Stream events from captured token"
            );

            open_change_stream(
                &collection,
                &config,
                &dataset.name,
                &collection_name,
                Some(resume_token),
            )
            .await?
        };

        let unnest_parameters = default_unnest_parameters(config.unnest_depth);
        let event_batches = live_change_stream.chunks_timeout(
            config.batch_max_size,
            config.batch_max_duration,
        );
        tokio::pin!(event_batches);

        while let Some(batch) = TokioStreamExt::next(&mut event_batches).await {
            if batch.is_empty() {
                continue;
            }

            let events = collect_change_events(batch, &dataset)?;

            let tail_token = events.last().map(|event| event.id.clone());
            let tail_cluster_time = events
                .last()
                .and_then(|event| event.cluster_time)
                .map(|ts| i64::from(ts.time));

            if let Some(change_batch) = change_events_to_change_batch(
                events,
                &schema,
                &primary_keys,
                &unnest_parameters,
            )
            .map_err(StreamError::MongoDB)? {
                // MongoDB change-stream cluster time is whole seconds (BSON
                // Timestamp), so the replication-lag signal here has ~1s
                // granularity — fine for a multi-second tuner.
                let change_batch = change_batch
                    .with_source_commit_ts_ms(tail_cluster_time.map(|s| s.saturating_mul(1000)));
                let committer = build_batch_committer(
                    mongo_sys.as_ref(),
                    tail_token,
                    tail_cluster_time,
                    current_schema_json.as_deref(),
                    &dataset.name,
                );
                yield ChangeEnvelope::new(committer, change_batch, false);
            }
        }
    })
}

async fn initialize_mongo_sys(dataset: &Dataset) -> Option<Arc<MongoSys>> {
    match MongoSys::try_new(dataset, OpenOption::CreateIfNotExists).await {
        Ok(sys) => Some(Arc::new(sys)),
        Err(error) => {
            tracing::error!(
                dataset = %dataset.name,
                error = %error,
                "Failed to initialize MongoDB resume-token sidecar; resume token will not be persisted across restarts"
            );
            None
        }
    }
}

async fn persisted_checkpoint(
    mongo_sys: Option<&MongoSys>,
    dataset: &Dataset,
    current_schema_json: Option<&str>,
) -> Option<MongoCheckpointMetadata> {
    let sys = mongo_sys?;
    let metadata = sys.get().await?;

    // Warn (don't fail) on schema drift between runs. The connector schema is
    // inferred from sampled documents and may legitimately evolve; treating
    // drift as a hard error here would surprise operators. Followup work can
    // make this behavior configurable.
    if let (Some(persisted_schema_json), Some(current_schema_json)) =
        (metadata.schema_json.as_deref(), current_schema_json)
        && persisted_schema_json != current_schema_json
    {
        tracing::warn!(
            dataset = %dataset.name,
            "MongoDB Change Stream resume detected schema drift between runs; continuing with the current schema. If new fields fail to populate, restart with `mongodb_resume_token_invalid_behavior: rebootstrap` to re-snapshot."
        );
    }

    Some(metadata)
}

async fn clear_persisted_token(mongo_sys: Option<&MongoSys>, dataset: &Dataset) {
    if let Some(sys) = mongo_sys
        && let Err(error) = sys.delete().await
    {
        tracing::warn!(
            dataset = %dataset.name,
            error = %error,
            "Failed to clear stale MongoDB resume token; the subsequent bootstrap will overwrite it"
        );
    }
}

fn serialize_current_schema(
    schema: &SchemaRef,
    dataset_name: &datafusion::sql::TableReference,
) -> Option<String> {
    match MongoSys::serialize_schema(schema) {
        Ok(json) => Some(json),
        Err(error) => {
            tracing::warn!(
                dataset = %dataset_name,
                error = %error,
                "Failed to serialize MongoDB dataset schema for the resume-token sidecar; schema drift detection will be disabled for this run"
            );
            None
        }
    }
}

fn serialize_resume_token(token: &ResumeToken) -> Result<String, StreamError> {
    serde_json::to_string(token).map_err(|error| {
        StreamError::SerdeJsonError(format!("failed to serialize resume token: {error}"))
    })
}

fn deserialize_resume_token(token_json: &str) -> Result<ResumeToken, StreamError> {
    serde_json::from_str(token_json).map_err(|error| {
        StreamError::SerdeJsonError(format!("failed to deserialize resume token: {error}"))
    })
}

fn resume_token_error_code(error: &mongodb::error::Error) -> Option<i32> {
    match error.kind.as_ref() {
        mongodb::error::ErrorKind::Command(cmd) => Some(cmd.code),
        _ => None,
    }
}

fn build_batch_committer(
    mongo_sys: Option<&Arc<MongoSys>>,
    tail_token: Option<ResumeToken>,
    tail_cluster_time: Option<i64>,
    schema_json: Option<&str>,
    dataset_name: &datafusion::sql::TableReference,
) -> Box<dyn CommitChange + Send + Sync> {
    let Some(sys) = mongo_sys else {
        return Box::new(NoOpCommitter);
    };

    let Some(token) = tail_token else {
        return Box::new(NoOpCommitter);
    };

    match serialize_resume_token(&token) {
        Ok(token_json) => Box::new(MongoResumeTokenCommitter::new(
            Arc::clone(sys),
            token_json,
            tail_cluster_time,
            schema_json.map(str::to_string),
        )),
        Err(error) => {
            tracing::warn!(
                dataset = %dataset_name,
                error = %error,
                "Failed to serialize MongoDB resume token for batch checkpoint; falling back to NoOpCommitter for this batch"
            );
            Box::new(NoOpCommitter)
        }
    }
}

/// Behavior when the persisted resume token cannot be honored by the source
/// (e.g. the oplog window has rolled past the token's position).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum ResumeTokenInvalidBehavior {
    /// Surface a clear error so the operator can decide. Recommended default
    /// because a re-snapshot of a large collection should be opt-in.
    #[default]
    Error,
    /// Drop the persisted token and fall through to the cold-bootstrap path,
    /// re-snapshotting the collection.
    Rebootstrap,
}

impl ResumeTokenInvalidBehavior {
    fn from_params(params: &Parameters) -> Result<Self, StreamError> {
        match optional_string(params, "mongodb_resume_token_invalid_behavior").as_deref() {
            None => Ok(Self::default()),
            Some(value) => match value.trim().to_ascii_lowercase().as_str() {
                "error" => Ok(Self::Error),
                "rebootstrap" => Ok(Self::Rebootstrap),
                other => Err(invalid_parameter_error(
                    params,
                    "mongodb_resume_token_invalid_behavior",
                    format!("must be 'error' or 'rebootstrap', got {other:?}"),
                )),
            },
        }
    }
}

pub(crate) struct MongoResumeTokenCommitter {
    mongo_sys: Arc<MongoSys>,
    resume_token_json: String,
    cluster_time_ts: Option<i64>,
    schema_json: Option<String>,
}

impl MongoResumeTokenCommitter {
    fn new(
        mongo_sys: Arc<MongoSys>,
        resume_token_json: String,
        cluster_time_ts: Option<i64>,
        schema_json: Option<String>,
    ) -> Self {
        Self {
            mongo_sys,
            resume_token_json,
            cluster_time_ts,
            schema_json,
        }
    }
}

#[async_trait]
impl CommitChange for MongoResumeTokenCommitter {
    async fn commit(&self) -> Result<(), CommitError> {
        self.mongo_sys
            .upsert(&MongoCheckpointMetadata {
                resume_token_json: self.resume_token_json.clone(),
                cluster_time_ts: self.cluster_time_ts,
                schema_json: self.schema_json.clone(),
                updated_at: None,
            })
            .await
            .map_err(|error| CommitError::UnableToCommitChange {
                source: Box::new(error),
            })
    }
}

async fn try_open_change_stream(
    collection: &Collection<Document>,
    config: &ChangeStreamConfig,
    resume_token: Option<ResumeToken>,
) -> mongodb::error::Result<ChangeStream<ChangeStreamEvent<Document>>> {
    let mut watch = collection
        .watch()
        .full_document(FullDocumentType::UpdateLookup)
        .max_await_time(config.max_await_time)
        .batch_size(config.server_batch_size);

    if let Some(resume_token) = resume_token {
        watch = watch.resume_after(resume_token);
    }

    watch.await
}

async fn open_change_stream(
    collection: &Collection<Document>,
    config: &ChangeStreamConfig,
    dataset_name: &datafusion::sql::TableReference,
    collection_name: &str,
    resume_token: Option<ResumeToken>,
) -> Result<ChangeStream<ChangeStreamEvent<Document>>, StreamError> {
    try_open_change_stream(collection, config, resume_token)
        .await
        .map_err(|error| {
            StreamError::External(format!(
                "Failed to start MongoDB Change Stream for dataset `{dataset_name}` collection `{collection_name}`: {error}"
            ))
        })
}

/// Returns `true` if the driver error indicates the resume token is past the
/// oplog retention window (`ChangeStreamHistoryLost`, code 286) or the cursor
/// is otherwise unresumable (`ChangeStreamFatalError`, code 280).
fn is_stale_resume_token_error(error: &mongodb::error::Error) -> bool {
    matches!(
        error.kind.as_ref(),
        mongodb::error::ErrorKind::Command(cmd) if matches!(cmd.code, 286 | 280)
    )
}

async fn snapshot_stream(
    table_provider: Arc<dyn TableProvider>,
) -> Result<SendableRecordBatchStream, data_components::cdc::StreamError> {
    let ctx = SessionContext::new();
    let df = ctx
        .read_table(table_provider)
        .map_err(|error| data_components::cdc::StreamError::Arrow(error.to_string()))?;
    df.execute_stream()
        .await
        .map_err(|error| data_components::cdc::StreamError::Arrow(error.to_string()))
}

fn collect_change_events(
    batch: Vec<mongodb::error::Result<ChangeStreamEvent<Document>>>,
    dataset: &Dataset,
) -> Result<Vec<ChangeStreamEvent<Document>>, data_components::cdc::StreamError> {
    batch
        .into_iter()
        .collect::<mongodb::error::Result<Vec<_>>>()
        .map_err(|error| {
            data_components::cdc::StreamError::External(format!(
                "Failed to read MongoDB Change Stream event for dataset `{}`: {error}",
                dataset.name
            ))
        })
}

fn resolve_primary_keys(
    dataset_name: &datafusion::sql::TableReference,
    acceleration: Option<&Acceleration>,
    schema: &SchemaRef,
) -> Result<Vec<String>, data_components::cdc::StreamError> {
    let acceleration = acceleration.ok_or_else(|| {
        data_components::cdc::StreamError::External(format!(
            "mongodb change streams for dataset `{dataset_name}` require acceleration to be enabled"
        ))
    })?;

    let engine = acceleration.engine.to_unpartitioned();
    if matches!(engine, Engine::Arrow) {
        return Err(data_components::cdc::StreamError::External(format!(
            "mongodb change streams for dataset `{dataset_name}` require an accelerator engine with upsert support. Use duckdb, sqlite, postgres, turso, or cayenne instead of `{engine}`."
        )));
    }

    let primary_key = acceleration.primary_key.as_ref().ok_or_else(|| {
        data_components::cdc::StreamError::External(format!(
            "mongodb change streams for dataset `{dataset_name}` require `acceleration.primary_key` so UPDATE and DELETE events can be applied correctly. For most collections, set `primary_key: _id`."
        ))
    })?;

    let primary_keys = primary_key
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    if primary_keys.as_slice() != ["_id"] {
        return Err(data_components::cdc::StreamError::External(format!(
            "mongodb change streams for dataset `{dataset_name}` require `acceleration.primary_key: _id` because MongoDB delete events only include the document key"
        )));
    }

    if !matches!(
        acceleration.on_conflict.get(primary_key),
        Some(OnConflictBehavior::Upsert(_))
    ) {
        return Err(data_components::cdc::StreamError::External(format!(
            "mongodb change streams for dataset `{dataset_name}` require `acceleration.on_conflict` keyed on `primary_key` with `upsert` behavior so UPDATE events replace existing rows. Add: `on_conflict: {{ _id: upsert }}`."
        )));
    }

    for key in &primary_keys {
        if schema.field_with_name(key).is_err() {
            return Err(data_components::cdc::StreamError::External(format!(
                "mongodb change streams for dataset `{dataset_name}` require primary key column `{key}` to exist in the inferred MongoDB schema. For ObjectId-backed collections, use `_id` and ensure schema inference includes it."
            )));
        }
    }

    Ok(primary_keys)
}

#[derive(Debug)]
struct ChangeStreamConfig {
    batch_max_size: usize,
    batch_max_duration: Duration,
    max_await_time: Duration,
    server_batch_size: u32,
    unnest_depth: usize,
}

impl ChangeStreamConfig {
    fn from_params(params: &Parameters) -> Result<Self, data_components::cdc::StreamError> {
        Ok(Self {
            batch_max_size: optional_positive_usize(
                params,
                "change_stream_batch_max_size",
                DEFAULT_CHANGE_STREAM_BATCH_MAX_SIZE,
            )?,
            batch_max_duration: optional_positive_duration(
                params,
                "change_stream_batch_max_duration",
                DEFAULT_CHANGE_STREAM_BATCH_MAX_DURATION,
            )?,
            max_await_time: optional_positive_duration(
                params,
                "change_stream_max_await_time",
                DEFAULT_CHANGE_STREAM_MAX_AWAIT_TIME,
            )?,
            server_batch_size: optional_positive_u32(
                params,
                "change_stream_batch_size",
                DEFAULT_CHANGE_STREAM_BATCH_SIZE,
            )?,
            unnest_depth: optional_usize(params, "unnest_depth")?.unwrap_or(0),
        })
    }
}

fn optional_string(params: &Parameters, name: &str) -> Option<String> {
    match params.get(name).expose() {
        ExposedParamLookup::Present(value) => Some(value.to_string()),
        ExposedParamLookup::Absent(_) => None,
    }
}

fn optional_usize(
    params: &Parameters,
    name: &str,
) -> Result<Option<usize>, data_components::cdc::StreamError> {
    let Some(value) = optional_string(params, name) else {
        return Ok(None);
    };

    value.trim().parse::<usize>().map(Some).map_err(|error| {
        invalid_parameter_error(
            params,
            name,
            format!("must be a non-negative integer, got {value:?}: {error}"),
        )
    })
}

fn optional_positive_usize(
    params: &Parameters,
    name: &str,
    default: usize,
) -> Result<usize, data_components::cdc::StreamError> {
    let Some(value) = optional_usize(params, name)? else {
        return Ok(default);
    };

    if value == 0 {
        return Err(invalid_parameter_error(
            params,
            name,
            "must be greater than 0".to_string(),
        ));
    }

    Ok(value)
}

fn optional_positive_u32(
    params: &Parameters,
    name: &str,
    default: u32,
) -> Result<u32, data_components::cdc::StreamError> {
    let Some(value) = optional_usize(params, name)? else {
        return Ok(default);
    };

    if value == 0 {
        return Err(invalid_parameter_error(
            params,
            name,
            "must be greater than 0".to_string(),
        ));
    }

    u32::try_from(value).map_err(|error| {
        invalid_parameter_error(
            params,
            name,
            format!("must fit in an unsigned 32-bit integer, got {value}: {error}"),
        )
    })
}

fn optional_positive_duration(
    params: &Parameters,
    name: &str,
    default: Duration,
) -> Result<Duration, data_components::cdc::StreamError> {
    let Some(value) = optional_string(params, name) else {
        return Ok(default);
    };

    let duration = fundu::parse_duration(&value).map_err(|error| {
        invalid_parameter_error(
            params,
            name,
            format!("must be a duration, got {value:?}: {error}"),
        )
    })?;

    if duration.is_zero() {
        return Err(invalid_parameter_error(
            params,
            name,
            "must be greater than 0".to_string(),
        ));
    }

    Ok(duration)
}

fn invalid_parameter_error(
    params: &Parameters,
    name: &str,
    message: impl std::fmt::Display,
) -> data_components::cdc::StreamError {
    let user_param = params.user_param(name);
    data_components::cdc::StreamError::External(format!(
        "mongodb change streams parameter `{user_param}` {message}"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion_table_providers::util::{
        column_reference::ColumnReference, constraints::UpsertOptions,
    };
    use runtime::component::dataset::acceleration::{Acceleration, RefreshMode};
    use secrecy::SecretString;
    use std::collections::HashMap;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("_id", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    fn schema_without_id() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, true)]))
    }

    fn params(values: &[(&str, &str)]) -> Parameters {
        Parameters::new(
            values
                .iter()
                .map(|(name, value)| (name.to_string(), SecretString::from(value.to_string())))
                .collect(),
            "mongodb",
            crate::PARAMETERS,
        )
    }

    #[test]
    fn validates_primary_key_and_upsert() {
        let primary_key = ColumnReference::new(vec!["_id".to_string()]);
        let mut on_conflict = HashMap::new();
        on_conflict.insert(
            primary_key.clone(),
            OnConflictBehavior::Upsert(UpsertOptions::default()),
        );
        let acceleration = Acceleration {
            enabled: true,
            engine: Engine::DuckDB,
            refresh_mode: Some(RefreshMode::Changes),
            primary_key: Some(primary_key),
            on_conflict,
            ..Default::default()
        };

        let dataset_name = datafusion::sql::TableReference::bare("users");
        let keys = resolve_primary_keys(&dataset_name, Some(&acceleration), &schema())
            .expect("valid CDC config");
        assert_eq!(keys, vec!["_id".to_string()]);
    }

    #[test]
    fn rejects_missing_acceleration() {
        let dataset_name = datafusion::sql::TableReference::bare("users");
        let error = resolve_primary_keys(&dataset_name, None, &schema())
            .expect_err("missing acceleration should fail");

        assert!(error.to_string().contains("require acceleration"));
    }

    #[test]
    fn rejects_arrow_acceleration() {
        let primary_key = ColumnReference::new(vec!["_id".to_string()]);
        let mut on_conflict = HashMap::new();
        on_conflict.insert(
            primary_key.clone(),
            OnConflictBehavior::Upsert(UpsertOptions::default()),
        );
        let acceleration = Acceleration {
            enabled: true,
            engine: Engine::Arrow,
            refresh_mode: Some(RefreshMode::Changes),
            primary_key: Some(primary_key),
            on_conflict,
            ..Default::default()
        };

        let dataset_name = datafusion::sql::TableReference::bare("users");
        let error = resolve_primary_keys(&dataset_name, Some(&acceleration), &schema())
            .expect_err("arrow acceleration should fail");

        assert!(error.to_string().contains("upsert support"));
    }

    #[test]
    fn rejects_missing_primary_key() {
        let acceleration = Acceleration {
            enabled: true,
            engine: Engine::DuckDB,
            refresh_mode: Some(RefreshMode::Changes),
            ..Default::default()
        };

        let dataset_name = datafusion::sql::TableReference::bare("users");
        let error = resolve_primary_keys(&dataset_name, Some(&acceleration), &schema())
            .expect_err("missing primary key should fail");

        assert!(error.to_string().contains("primary_key"));
    }

    #[test]
    fn rejects_missing_upsert() {
        let acceleration = Acceleration {
            enabled: true,
            engine: Engine::DuckDB,
            refresh_mode: Some(RefreshMode::Changes),
            primary_key: Some(ColumnReference::new(vec!["_id".to_string()])),
            ..Default::default()
        };

        let dataset_name = datafusion::sql::TableReference::bare("users");
        let error = resolve_primary_keys(&dataset_name, Some(&acceleration), &schema())
            .expect_err("missing upsert should fail");
        assert!(error.to_string().contains("on_conflict"));
    }

    #[test]
    fn rejects_non_id_primary_key() {
        let primary_key = ColumnReference::new(vec!["name".to_string()]);
        let mut on_conflict = HashMap::new();
        on_conflict.insert(
            primary_key.clone(),
            OnConflictBehavior::Upsert(UpsertOptions::default()),
        );
        let acceleration = Acceleration {
            enabled: true,
            engine: Engine::DuckDB,
            refresh_mode: Some(RefreshMode::Changes),
            primary_key: Some(primary_key),
            on_conflict,
            ..Default::default()
        };

        let dataset_name = datafusion::sql::TableReference::bare("users");
        let error = resolve_primary_keys(&dataset_name, Some(&acceleration), &schema())
            .expect_err("non-_id primary key should fail");
        assert!(error.to_string().contains("primary_key: _id"));
    }

    #[test]
    fn rejects_composite_primary_key_before_missing_upsert() {
        let acceleration = Acceleration {
            enabled: true,
            engine: Engine::DuckDB,
            refresh_mode: Some(RefreshMode::Changes),
            primary_key: Some(ColumnReference::new(vec![
                "_id".to_string(),
                "other".to_string(),
            ])),
            ..Default::default()
        };

        let dataset_name = datafusion::sql::TableReference::bare("users");
        let error = resolve_primary_keys(&dataset_name, Some(&acceleration), &schema())
            .expect_err("composite primary key should fail before on_conflict hint");

        assert!(error.to_string().contains("primary_key: _id"));
    }

    #[test]
    fn rejects_missing_primary_key_column_in_schema() {
        let primary_key = ColumnReference::new(vec!["_id".to_string()]);
        let mut on_conflict = HashMap::new();
        on_conflict.insert(
            primary_key.clone(),
            OnConflictBehavior::Upsert(UpsertOptions::default()),
        );
        let acceleration = Acceleration {
            enabled: true,
            engine: Engine::DuckDB,
            refresh_mode: Some(RefreshMode::Changes),
            primary_key: Some(primary_key),
            on_conflict,
            ..Default::default()
        };

        let dataset_name = datafusion::sql::TableReference::bare("users");
        let error = resolve_primary_keys(&dataset_name, Some(&acceleration), &schema_without_id())
            .expect_err("missing _id column should fail");

        assert!(error.to_string().contains("primary key column `_id`"));
    }

    #[test]
    fn parses_default_change_stream_config() {
        let config = ChangeStreamConfig::from_params(&params(&[]))
            .expect("default change stream config should parse");

        assert_eq!(config.batch_max_size, DEFAULT_CHANGE_STREAM_BATCH_MAX_SIZE);
        assert_eq!(
            config.batch_max_duration,
            DEFAULT_CHANGE_STREAM_BATCH_MAX_DURATION
        );
        assert_eq!(config.max_await_time, DEFAULT_CHANGE_STREAM_MAX_AWAIT_TIME);
        assert_eq!(config.server_batch_size, DEFAULT_CHANGE_STREAM_BATCH_SIZE);
        assert_eq!(config.unnest_depth, 0);
    }

    #[test]
    fn parses_unnest_depth_param() {
        let config = ChangeStreamConfig::from_params(&params(&[("unnest_depth", "2")]))
            .expect("unnest depth should parse");

        assert_eq!(config.unnest_depth, 2);
    }

    #[test]
    fn rejects_invalid_batch_size_param() {
        let error = ChangeStreamConfig::from_params(&params(&[(
            "change_stream_batch_size",
            "not-a-number",
        )]))
        .expect_err("invalid batch size should fail");

        assert!(error.to_string().contains("change_stream_batch_size"));
    }

    #[test]
    fn rejects_zero_batch_max_size_param() {
        let error =
            ChangeStreamConfig::from_params(&params(&[("change_stream_batch_max_size", "0")]))
                .expect_err("zero batch max size should fail");

        assert!(error.to_string().contains("change_stream_batch_max_size"));
    }

    #[test]
    fn rejects_batch_size_over_u32_max() {
        let error =
            ChangeStreamConfig::from_params(&params(&[("change_stream_batch_size", "4294967296")]))
                .expect_err("batch size larger than u32 should fail");

        assert!(error.to_string().contains("unsigned 32-bit integer"));
    }

    #[test]
    fn rejects_zero_duration_param() {
        let error =
            ChangeStreamConfig::from_params(&params(&[("change_stream_batch_max_duration", "0s")]))
                .expect_err("zero duration should fail");

        assert!(
            error
                .to_string()
                .contains("change_stream_batch_max_duration")
        );
    }
}
