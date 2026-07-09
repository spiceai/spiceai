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
        ChangeEnvelope, ChangesStream, CommitChange, CommitError, DEFAULT_READY_LAG,
        InitialSnapshotMode, InvalidCheckpointBehavior, NoOpCommitter, StreamError,
        build_heartbeat_envelope, source_commit_within_ready_lag, wrap_data_as_change_batch,
    },
    mongodb::stream::{
        change_events_to_change_batch, default_unnest_parameters, nullable_clone,
        truncate_change_batch,
    },
};
use datafusion::{
    arrow::datatypes::SchemaRef, datasource::TableProvider,
    physical_plan::SendableRecordBatchStream, prelude::SessionContext,
};
use datafusion_table_providers::mongodb::connection_pool::MongoDBConnectionPool;
use futures::StreamExt as FuturesStreamExt;
use mongodb::{
    ClientSession, Collection,
    bson::Document,
    change_stream::{
        ChangeStream, event::ChangeStreamEvent, event::ResumeToken, session::SessionChangeStream,
    },
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
    dataconnector::schema_projection::{ProjectionPolicy, parse_schema_projection},
    federated_table::FederatedTable,
    parameters::{ExposedParamLookup, Parameters},
};
use std::{sync::Arc, time::Duration};

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
        // JSON-nesting projection, matching the scan path. `_id` is MongoDB's
        // only primary key and must stay a declared column (never folded into
        // the catch-all). `schema` is already the projected (exposed) schema.
        let projection = parse_schema_projection(
            &dataset,
            &ProjectionPolicy::new("mongodb").with_required_columns(vec!["_id".to_string()]),
        )
        .map_err(|e| StreamError::External(e.to_string()))?;
        let config = ChangeStreamConfig::from_params(&params)?;
        let invalid_token_behavior = invalid_checkpoint_behavior_from_params(&params)?;
        let ready_lag = ready_lag_from_params(&params)?;
        let snapshot_mode = snapshot_mode_from_params(&params)?;
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

        // `initial_snapshot: enabled` re-snapshots on every start: drop any
        // persisted resume token so the cold-bootstrap (snapshot) path below
        // runs unconditionally.
        let persisted = if snapshot_mode == InitialSnapshotMode::Enabled {
            if persisted.is_some() {
                clear_persisted_token(mongo_sys.as_deref(), &dataset).await;
            }
            None
        } else {
            persisted
        };

        // CDC readiness is purely lag-based (see the live loop): neither the
        // resume nor the bootstrap path emits a one-shot "ready" signal anymore.
        // Each live batch and each idle heartbeat instead carries a readiness
        // verdict computed from how far the newest applied change's cluster time
        // trails now, so the dataset is marked ready only once the stream has
        // caught up to the source head — never merely because bootstrap finished.
        //
        // The resume path returns the live session stream directly; `None` falls
        // through to cold bootstrap below.
        let resumed: Option<(SessionChangeStream<ChangeStreamEvent<Document>>, ClientSession)> =
            if let Some(metadata) = persisted {
                let resume_token = deserialize_resume_token(&metadata.resume_token_json)
                    .map_err(|error| StreamError::External(format!(
                        "Failed to deserialize persisted MongoDB resume token for dataset `{}` collection `{collection_name}`: {error}. To recover, delete the dataset's row from `spice_sys_mongodb` or restart with `mongodb_replication_invalid_checkpoint_behavior: restart`.",
                        dataset.name
                    )))?;

                // An explicit MongoDB session lets the live loop read the
                // source-attested `operationTime` gossiped on each getMore reply —
                // the cluster time the stream has provably scanned the oplog up to.
                // That timestamp (never a local now()) drives lag-based readiness
                // and the idle heartbeat.
                let mut session = start_change_stream_session(
                    connection.client.as_ref(),
                    &dataset.name,
                    &collection_name,
                )
                .await?;

                match try_open_session_change_stream(&collection, &config, &mut session, Some(resume_token)).await {
                    Ok(stream) => {
                        tracing::info!(
                            dataset = %dataset.name,
                            collection = %collection_name,
                            "MongoDB Change Stream resumed from persisted resume token; skipping collection snapshot"
                        );
                        Some((stream, session))
                    }
                    Err(error) if is_stale_resume_token_error(&error) => match invalid_token_behavior {
                        InvalidCheckpointBehavior::Error => Err(StreamError::External(format!(
                            "MongoDB Change Stream resume token for dataset `{}` collection `{collection_name}` is past the oplog retention window or otherwise invalid (driver code {}). Set `mongodb_replication_invalid_checkpoint_behavior: restart` to drop the persisted token and re-snapshot the collection. Source: {error}",
                            dataset.name,
                            resume_token_error_code(&error).map_or_else(|| "unknown".to_string(), |c| c.to_string()),
                        )))?,
                        InvalidCheckpointBehavior::Restart => {
                            tracing::warn!(
                                dataset = %dataset.name,
                                collection = %collection_name,
                                error = %error,
                                "MongoDB Change Stream resume token is stale; `restart` behavior enabled, falling back to cold bootstrap"
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

        let (mut live_change_stream, mut session) = if let Some(live) = resumed {
            live
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

            if snapshot_mode == InitialSnapshotMode::Disabled {
                // `initial_snapshot: disabled`: begin streaming change events
                // from the captured point without copying existing documents
                // (no truncate, no snapshot scan).
                tracing::info!(
                    dataset = %dataset.name,
                    collection = %collection_name,
                    "MongoDB Change Stream started; `initial_snapshot: disabled` — streaming changes from the current point without a collection snapshot"
                );
            } else {
                tracing::info!(
                    dataset = %dataset.name,
                    collection = %collection_name,
                    "MongoDB Change Stream started; bootstrapping accelerator from collection snapshot"
                );

                let truncate = truncate_change_batch(&schema)
                    .map_err(StreamError::MongoDB)?;
                yield ChangeEnvelope::new(Box::new(NoOpCommitter), truncate, false);

                // Use the same nullable schema that CDC event batches use (via nullable_clone
                // in change_events_to_change_batch), so snapshot and live-stream batches can
                // be coalesced without an Arrow schema mismatch on non-null fields like _id.
                let snapshot_schema = nullable_clone(&schema);
                let mut snapshot_stream = snapshot_stream(table_provider).await?;
                while let Some(batch) = FuturesStreamExt::next(&mut snapshot_stream).await {
                    let batch = batch.map_err(|error| StreamError::Arrow(error.to_string()))?;
                    if batch.num_rows() == 0 {
                        continue;
                    }

                    let batch = batch
                        .with_schema(Arc::clone(&snapshot_schema))
                        .map_err(|error| StreamError::Arrow(error.to_string()))?;
                    let change_batch = wrap_data_as_change_batch(&snapshot_schema, &batch)
                        .map_err(|error| StreamError::Arrow(error.to_string()))?;
                    yield ChangeEnvelope::new(Box::new(NoOpCommitter), change_batch, false);
                }
            }

            // Persist the captured resume token at the bootstrap→live barrier so a
            // restart resumes here instead of re-snapshotting. This is a zero-row,
            // NOT-ready envelope: readiness is lag-based and comes from the live
            // loop below, never from finishing bootstrap. The committer fires only
            // after the downstream has persisted this empty batch — the natural
            // barrier between the "bootstrap" and "live" phases. A crash before this
            // commit leaves the sidecar empty, so the next start re-bootstraps.
            let initial_token_json = serialize_resume_token(&resume_token)
                .map_err(|error| StreamError::External(format!(
                    "Failed to serialize MongoDB resume token for dataset `{}` collection `{collection_name}`: {error}",
                    dataset.name
                )))?;
            let barrier = build_heartbeat_envelope(&schema, None, false)
                .map_err(|error| StreamError::Arrow(error.to_string()))?;
            let (_, batch, _) = barrier.into_parts();
            let committer: Box<dyn CommitChange + Send + Sync> = match mongo_sys.as_ref() {
                Some(sys) => Box::new(MongoResumeTokenCommitter::new(
                    Arc::clone(sys),
                    initial_token_json,
                    None,
                    current_schema_json.clone(),
                    dataset.name.to_string(),
                )),
                None => Box::new(NoOpCommitter),
            };
            yield ChangeEnvelope::from_parts(committer, batch, false);

            tracing::info!(
                dataset = %dataset.name,
                collection = %collection_name,
                "MongoDB Change Stream bootstrap complete; resuming events from the captured resume token"
            );

            // Session created after the (possibly long) snapshot so an idle
            // MongoDB logical session cannot time out mid-bootstrap.
            let mut session = start_change_stream_session(
                connection.client.as_ref(),
                &dataset.name,
                &collection_name,
            )
            .await?;
            let stream = open_session_change_stream(
                &collection,
                &config,
                &mut session,
                &dataset.name,
                &collection_name,
                Some(resume_token),
            )
            .await?;
            (stream, session)
        };

        let unnest_parameters = default_unnest_parameters(config.unnest_depth);

        // Manual batching over the session change stream. `next_if_any` issues at
        // most one getMore per call and returns `Ok(None)` on an empty (idle)
        // getMore; the session then carries the source-attested `operationTime`
        // that getMore gossiped — the cluster time the stream has provably scanned
        // the oplog up to. A caught-up live batch or that idle timestamp (never a
        // local now()) drives lag-based readiness, so a quiet-but-caught-up source
        // still becomes ready while a draining backlog stays not-ready. Idle polls
        // are paced by `change_stream_max_await_time`, so this never busy-loops.
        let mut pending: Vec<ChangeStreamEvent<Document>> =
            Vec::with_capacity(config.batch_max_size);
        let mut batch_started_at: Option<std::time::Instant> = None;

        'live: loop {
            let next = live_change_stream
                .next_if_any(&mut session)
                .await
                .map_err(|error| StreamError::External(format!(
                    "Failed to read MongoDB Change Stream event for dataset `{}` collection `{collection_name}`: {error}",
                    dataset.name
                )))?;

            let flush = match next {
                Some(event) => {
                    pending.push(event);
                    let started = *batch_started_at.get_or_insert_with(std::time::Instant::now);
                    pending.len() >= config.batch_max_size
                        || started.elapsed() >= config.batch_max_duration
                }
                None => {
                    if !pending.is_empty() {
                        // An idle getMore closes the in-flight batch.
                        true
                    } else if !live_change_stream.is_alive() {
                        tracing::info!(
                            dataset = %dataset.name,
                            collection = %collection_name,
                            "MongoDB Change Stream ended (collection dropped or invalidated); completing"
                        );
                        break 'live;
                    } else {
                        // Idle and caught up: emit a zero-row heartbeat stamped with
                        // the getMore's gossiped cluster time so lag-based readiness
                        // stays live on a quiet source. With no gossiped time yet,
                        // emit nothing rather than fabricate a local now().
                        if let Some(op_time) = session.operation_time() {
                            // MongoDB cluster time is whole seconds (BSON Timestamp)
                            // → Unix-epoch milliseconds, matching the live path.
                            let cluster_time_ms = i64::from(op_time.time).saturating_mul(1000);
                            let is_ready =
                                source_commit_within_ready_lag(Some(cluster_time_ms), ready_lag);
                            let heartbeat =
                                build_heartbeat_envelope(&schema, Some(cluster_time_ms), is_ready)
                                    .map_err(|error| StreamError::Arrow(error.to_string()))?;
                            // Log the idle heartbeat so lag-based readiness can be
                            // verified from the logs (target spice_cdc::heartbeat).
                            let heartbeat_lag_ms = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .ok()
                                .and_then(|d| i64::try_from(d.as_millis()).ok())
                                .map(|now| now.saturating_sub(cluster_time_ms));
                            tracing::info!(
                                target: "spice_cdc::heartbeat",
                                connector = "mongodb",
                                dataset = %dataset.name,
                                source_commit_ts_ms = cluster_time_ms,
                                is_dataset_ready = is_ready,
                                lag_ms = ?heartbeat_lag_ms,
                                "CDC idle heartbeat emitted"
                            );
                            yield heartbeat;
                        }
                        false
                    }
                }
            };

            if !flush {
                continue;
            }

            let events = std::mem::take(&mut pending);
            batch_started_at = None;

            let tail_token = events.last().map(|event| event.id.clone());
            // MongoDB change-stream cluster time is whole seconds (BSON Timestamp),
            // so the replication-lag signal here has ~1s granularity — fine for a
            // multi-second ready lag.
            let tail_cluster_time = events
                .last()
                .and_then(|event| event.cluster_time)
                .map(|ts| i64::from(ts.time));

            if let Some(change_batch) = change_events_to_change_batch(
                events,
                &schema,
                &primary_keys,
                &unnest_parameters,
                projection.as_ref(),
            )
            .map_err(StreamError::MongoDB)?
            {
                let source_commit_ts_ms = tail_cluster_time.map(|s| s.saturating_mul(1000));
                let change_batch = change_batch.with_source_commit_ts_ms(source_commit_ts_ms);
                let is_ready = source_commit_within_ready_lag(source_commit_ts_ms, ready_lag);
                let committer = build_batch_committer(
                    mongo_sys.as_ref(),
                    tail_token,
                    tail_cluster_time,
                    current_schema_json.as_deref(),
                    &dataset.name,
                );
                yield ChangeEnvelope::new(committer, change_batch, is_ready);
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
            "MongoDB Change Stream resume detected schema drift between runs; continuing with the current schema. If new fields fail to populate, restart with `mongodb_replication_invalid_checkpoint_behavior: restart` to re-snapshot."
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
            dataset_name.to_string(),
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

/// Resolve the initial-snapshot mode from `mongodb_replication_initial_snapshot`
/// (`auto|enabled|disabled`); defaults to [`InitialSnapshotMode::Auto`].
fn snapshot_mode_from_params(params: &Parameters) -> Result<InitialSnapshotMode, StreamError> {
    match optional_string(params, "mongodb_replication_initial_snapshot") {
        None => Ok(InitialSnapshotMode::default()),
        Some(value) if value.trim().is_empty() => Ok(InitialSnapshotMode::default()),
        Some(value) => InitialSnapshotMode::from_canonical(&value).ok_or_else(|| {
            invalid_parameter_error(
                params,
                "mongodb_replication_initial_snapshot",
                format!(
                    "must be 'auto', 'enabled', or 'disabled', got {:?}",
                    value.trim()
                ),
            )
        }),
    }
}

/// Resolve the lag-based readiness threshold from `mongodb_replication_ready_lag`
/// (a duration); defaults to [`DEFAULT_READY_LAG`]. A `refresh_mode: changes`
/// dataset is marked ready once its replication lag falls below this.
fn ready_lag_from_params(params: &Parameters) -> Result<Duration, StreamError> {
    optional_positive_duration(params, "mongodb_replication_ready_lag", DEFAULT_READY_LAG)
}

/// Resolve the invalid-checkpoint behavior, preferring the canonical
/// `mongodb_replication_invalid_checkpoint_behavior` (`error|restart`) and
/// falling back to the deprecated `mongodb_resume_token_invalid_behavior`
/// (`error|rebootstrap`); defaults to [`InvalidCheckpointBehavior::Error`].
fn invalid_checkpoint_behavior_from_params(
    params: &Parameters,
) -> Result<InvalidCheckpointBehavior, StreamError> {
    if let Some(value) = optional_string(params, "mongodb_replication_invalid_checkpoint_behavior")
    {
        let trimmed = value.trim();
        if !trimmed.is_empty() {
            return InvalidCheckpointBehavior::from_canonical(trimmed).ok_or_else(|| {
                invalid_parameter_error(
                    params,
                    "mongodb_replication_invalid_checkpoint_behavior",
                    format!("must be 'error' or 'restart', got {trimmed:?}"),
                )
            });
        }
    }
    match optional_string(params, "mongodb_resume_token_invalid_behavior").as_deref() {
        None => Ok(InvalidCheckpointBehavior::default()),
        Some(value) if value.trim().is_empty() => Ok(InvalidCheckpointBehavior::default()),
        Some(value) => match value.trim().to_ascii_lowercase().as_str() {
            "error" => Ok(InvalidCheckpointBehavior::Error),
            "rebootstrap" => Ok(InvalidCheckpointBehavior::Restart),
            other => Err(invalid_parameter_error(
                params,
                "mongodb_resume_token_invalid_behavior",
                format!("must be 'error' or 'rebootstrap', got {other:?}"),
            )),
        },
    }
}

pub(crate) struct MongoResumeTokenCommitter {
    mongo_sys: Arc<MongoSys>,
    resume_token_json: String,
    cluster_time_ts: Option<i64>,
    schema_json: Option<String>,
    /// Dataset name, for the committer-progress log line.
    dataset: String,
}

impl MongoResumeTokenCommitter {
    fn new(
        mongo_sys: Arc<MongoSys>,
        resume_token_json: String,
        cluster_time_ts: Option<i64>,
        schema_json: Option<String>,
        dataset: String,
    ) -> Self {
        Self {
            mongo_sys,
            resume_token_json,
            cluster_time_ts,
            schema_json,
            dataset,
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
            })?;
        // MongoDB cluster time is whole seconds; convert to ms for lag reporting.
        data_components::cdc::log_committer_progress(
            "mongodb",
            &self.dataset,
            &self.resume_token_json,
            self.cluster_time_ts.map(|s| s.saturating_mul(1000)),
        );
        Ok(())
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

/// Open a session-bound live change stream. Mirrors [`try_open_change_stream`]
/// but binds an explicit [`ClientSession`] so the live loop can read the
/// source-attested `operationTime` gossiped on each getMore (the cluster time
/// the stream has provably scanned the oplog up to), which drives lag-based
/// readiness and the idle heartbeat.
async fn try_open_session_change_stream(
    collection: &Collection<Document>,
    config: &ChangeStreamConfig,
    session: &mut ClientSession,
    resume_token: Option<ResumeToken>,
) -> mongodb::error::Result<SessionChangeStream<ChangeStreamEvent<Document>>> {
    let mut watch = collection
        .watch()
        .full_document(FullDocumentType::UpdateLookup)
        .max_await_time(config.max_await_time)
        .batch_size(config.server_batch_size);

    if let Some(resume_token) = resume_token {
        watch = watch.resume_after(resume_token);
    }

    watch.session(session).await
}

async fn open_session_change_stream(
    collection: &Collection<Document>,
    config: &ChangeStreamConfig,
    session: &mut ClientSession,
    dataset_name: &datafusion::sql::TableReference,
    collection_name: &str,
    resume_token: Option<ResumeToken>,
) -> Result<SessionChangeStream<ChangeStreamEvent<Document>>, StreamError> {
    try_open_session_change_stream(collection, config, session, resume_token)
        .await
        .map_err(|error| {
            StreamError::External(format!(
                "Failed to start MongoDB Change Stream for dataset `{dataset_name}` collection `{collection_name}`: {error}"
            ))
        })
}

/// Start an explicit `MongoDB` client session for the live change stream. The
/// session carries the gossiped `operationTime` used for lag-based readiness.
async fn start_change_stream_session(
    client: &mongodb::Client,
    dataset_name: &datafusion::sql::TableReference,
    collection_name: &str,
) -> Result<ClientSession, StreamError> {
    client.start_session().await.map_err(|error| {
        StreamError::External(format!(
            "Failed to start a MongoDB session for Change Stream readiness on dataset `{dataset_name}` collection `{collection_name}`: {error}"
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
    fn snapshot_mode_parses_canonical_values() {
        assert_eq!(
            snapshot_mode_from_params(&params(&[])).expect("default parses"),
            InitialSnapshotMode::Auto
        );
        for (raw, expected) in [
            ("auto", InitialSnapshotMode::Auto),
            ("ENABLED", InitialSnapshotMode::Enabled),
            ("disabled", InitialSnapshotMode::Disabled),
        ] {
            assert_eq!(
                snapshot_mode_from_params(&params(&[(
                    "mongodb_replication_initial_snapshot",
                    raw
                )]))
                .expect("valid value parses"),
                expected,
                "raw: {raw}"
            );
        }
        let err = snapshot_mode_from_params(&params(&[(
            "mongodb_replication_initial_snapshot",
            "sometimes",
        )]))
        .expect_err("typo must error");
        assert!(
            format!("{err}").contains("mongodb_replication_initial_snapshot"),
            "got: {err}"
        );
    }

    #[test]
    fn invalid_checkpoint_behavior_prefers_canonical_over_deprecated_alias() {
        // Default when neither key is set.
        assert_eq!(
            invalid_checkpoint_behavior_from_params(&params(&[])).expect("default parses"),
            InvalidCheckpointBehavior::Error
        );
        // Canonical key + value.
        assert_eq!(
            invalid_checkpoint_behavior_from_params(&params(&[(
                "mongodb_replication_invalid_checkpoint_behavior",
                "restart"
            )]))
            .expect("canonical parses"),
            InvalidCheckpointBehavior::Restart
        );
        // Deprecated alias still honored.
        assert_eq!(
            invalid_checkpoint_behavior_from_params(&params(&[(
                "mongodb_resume_token_invalid_behavior",
                "rebootstrap"
            )]))
            .expect("deprecated alias parses"),
            InvalidCheckpointBehavior::Restart
        );
        // Canonical wins when both are set.
        assert_eq!(
            invalid_checkpoint_behavior_from_params(&params(&[
                ("mongodb_replication_invalid_checkpoint_behavior", "error"),
                ("mongodb_resume_token_invalid_behavior", "rebootstrap"),
            ]))
            .expect("both set parses"),
            InvalidCheckpointBehavior::Error
        );
        // Canonical key rejects the deprecated value vocabulary.
        let err = invalid_checkpoint_behavior_from_params(&params(&[(
            "mongodb_replication_invalid_checkpoint_behavior",
            "rebootstrap",
        )]))
        .expect_err("deprecated value on canonical key must error");
        assert!(
            format!("{err}").contains("'error' or 'restart'"),
            "got: {err}"
        );
    }

    #[test]
    fn ready_lag_defaults_and_parses() {
        assert_eq!(
            ready_lag_from_params(&params(&[])).expect("default parses"),
            DEFAULT_READY_LAG
        );
        assert_eq!(
            ready_lag_from_params(&params(&[("mongodb_replication_ready_lag", "5s")]))
                .expect("valid duration parses"),
            Duration::from_secs(5)
        );
        let err = ready_lag_from_params(&params(&[("mongodb_replication_ready_lag", "nope")]))
            .expect_err("invalid duration must error");
        assert!(
            format!("{err}").contains("mongodb_replication_ready_lag"),
            "got: {err}"
        );
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
