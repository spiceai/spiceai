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
use data_components::{
    cdc::{
        ChangeEnvelope, ChangesStream, NoOpCommitter, build_ready_signal_envelope,
        wrap_data_as_change_batch,
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
    Box::pin(try_stream! {
        let table_provider = federated_table.table_provider().await;
        let schema = table_provider.schema();
        let primary_keys = resolve_primary_keys(&dataset.name, dataset.acceleration.as_ref(), &schema)?;
        let config = ChangeStreamConfig::from_params(&params)?;
        let collection_name = dataset.path().to_string();

        let connection = pool
            .connect()
            .await
            .map_err(|error| data_components::cdc::StreamError::External(format!(
                "Failed to connect to MongoDB Change Stream for dataset `{}` collection `{collection_name}`: {error}",
                dataset.name
            )))?;
        let collection = connection
            .client
            .database(&connection.db_name)
            .collection::<Document>(&collection_name);

        let initial_change_stream = open_change_stream(
            &collection,
            &config,
            &dataset.name,
            &collection_name,
            None,
        )
        .await?;
        let resume_token = initial_change_stream.resume_token().ok_or_else(|| {
            data_components::cdc::StreamError::External(format!(
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
            .map_err(data_components::cdc::StreamError::MongoDB)?;
        yield ChangeEnvelope::new(Box::new(NoOpCommitter), truncate, false);

        let mut snapshot_stream = snapshot_stream(table_provider).await?;
        while let Some(batch) = FuturesStreamExt::next(&mut snapshot_stream).await {
            let batch = batch.map_err(|error| data_components::cdc::StreamError::Arrow(error.to_string()))?;
            if batch.num_rows() == 0 {
                continue;
            }

            let change_batch = wrap_data_as_change_batch(&schema, &batch)
                .map_err(|error| data_components::cdc::StreamError::Arrow(error.to_string()))?;
            yield ChangeEnvelope::new(Box::new(NoOpCommitter), change_batch, false);
        }

        let ready = build_ready_signal_envelope(&schema)
            .map_err(|error| data_components::cdc::StreamError::Arrow(error.to_string()))?;
        yield ready;

        tracing::info!(
            dataset = %dataset.name,
            collection = %collection_name,
            "MongoDB collection snapshot complete; resuming Change Stream events"
        );

        let change_stream = open_change_stream(
            &collection,
            &config,
            &dataset.name,
            &collection_name,
            Some(resume_token),
        )
        .await?;

        let unnest_parameters = default_unnest_parameters(config.unnest_depth);
        let event_batches = change_stream.chunks_timeout(
            config.batch_max_size,
            config.batch_max_duration,
        );
        tokio::pin!(event_batches);

        while let Some(batch) = TokioStreamExt::next(&mut event_batches).await {
            if batch.is_empty() {
                continue;
            }

            let events = collect_change_events(batch, &dataset)?;
            if let Some(change_batch) = change_events_to_change_batch(
                events,
                &schema,
                &primary_keys,
                &unnest_parameters,
            )
            .map_err(data_components::cdc::StreamError::MongoDB)? {
                yield ChangeEnvelope::new(Box::new(NoOpCommitter), change_batch, true);
            }
        }
    })
}

async fn open_change_stream(
    collection: &Collection<Document>,
    config: &ChangeStreamConfig,
    dataset_name: &datafusion::sql::TableReference,
    collection_name: &str,
    resume_token: Option<ResumeToken>,
) -> Result<ChangeStream<ChangeStreamEvent<Document>>, data_components::cdc::StreamError> {
    let mut watch = collection
        .watch()
        .full_document(FullDocumentType::UpdateLookup)
        .max_await_time(config.max_await_time)
        .batch_size(config.server_batch_size);

    if let Some(resume_token) = resume_token {
        watch = watch.resume_after(resume_token);
    }

    watch.await.map_err(|error| {
        data_components::cdc::StreamError::External(format!(
            "Failed to start MongoDB Change Stream for dataset `{dataset_name}` collection `{collection_name}`: {error}"
        ))
    })
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

    if !matches!(
        acceleration.on_conflict.get(primary_key),
        Some(OnConflictBehavior::Upsert(_))
    ) {
        let pk_hint = primary_key
            .iter()
            .next()
            .map_or_else(|| "_id".to_string(), ToString::to_string);
        return Err(data_components::cdc::StreamError::External(format!(
            "mongodb change streams for dataset `{dataset_name}` require `acceleration.on_conflict` keyed on `primary_key` with `upsert` behavior so UPDATE events replace existing rows. Add: `on_conflict: {{ {pk_hint}: upsert }}`."
        )));
    }

    let primary_keys = primary_key
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    if primary_keys.as_slice() != ["_id"] {
        return Err(data_components::cdc::StreamError::External(format!(
            "mongodb change streams for dataset `{dataset_name}` require `acceleration.primary_key: _id` because MongoDB delete events only include the document key"
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
