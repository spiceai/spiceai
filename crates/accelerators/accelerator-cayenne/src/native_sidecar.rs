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

//! The sidecar stores of a dataset under `cayenne_metastore: object_native`.
//!
//! The runtime's bookkeeping for a dataset (a CDC connector's applied position, the
//! dataset schema checkpoint) is committed through the same head log as the
//! dataset's metadata, as keyed values in the catalog. A write is durable before
//! the caller moves on, as with the `SQLite` sidecar: on its own it is one commit,
//! and inside a CDC checkpoint it joins the checkpoint's record, so a source
//! position commits with the data it covers.

use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use cayenne::{MetadataCatalog, metastore::object_native::ObjectNativeCatalog};
use runtime_acceleration::{
    dataset_checkpoint::{DatasetCheckpointer, deserialize_schema, serialize_schema},
    sidecar::{AcceleratorSidecar, unsupported_sidecar},
    snapshot::SnapshotBehavior,
};
use runtime_checkpoint_api::{
    BlobCheckpoint, BlobCheckpointStore, CheckpointError,
    debezium::{DebeziumCheckpoint, DebeziumCheckpointStore},
    kafka::{KafkaCheckpoint, KafkaCheckpointStore, KafkaOffset},
    mongodb::{MongoCheckpointMetadata, MongoCheckpointStore},
    mysql_binlog::{MySqlBinlogCheckpoint, MySqlBinlogStore},
};
use serde_json::{Value, json};

const ENGINE: &str = "cayenne object_native";
const DATASET_CHECKPOINT: &str = "spice_sys_dataset_checkpoint";

fn store_error(source: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> CheckpointError {
    CheckpointError::Store {
        source: source.into(),
    }
}

/// The keyed-value view of the native catalog both stores write through.
#[derive(Clone)]
struct Values {
    catalog: Arc<dyn MetadataCatalog>,
    key: String,
}

impl Values {
    fn native(&self) -> Result<&ObjectNativeCatalog, CheckpointError> {
        self.catalog
            .as_any()
            .downcast_ref::<ObjectNativeCatalog>()
            .ok_or_else(|| store_error("the dataset's catalog is not object-native"))
    }

    async fn get(&self) -> Result<Option<Value>, CheckpointError> {
        match self
            .native()?
            .sidecar_value(&self.key)
            .await
            .map_err(store_error)?
        {
            Some(bytes) => Ok(Some(serde_json::from_slice(&bytes).map_err(store_error)?)),
            None => Ok(None),
        }
    }

    /// The value and the per-partition offsets, as one commit.
    async fn set_with_offsets(
        &self,
        value: &Value,
        offsets: &[KafkaOffset],
    ) -> Result<(), CheckpointError> {
        let bytes = serde_json::to_vec(value).map_err(store_error)?;
        self.native()?
            .set_sidecar_value_and_counters(&self.key, Some(Some(bytes)), &offset_counters(offsets))
            .await
            .map_err(store_error)
    }

    async fn advance_offsets(&self, offsets: &[KafkaOffset]) -> Result<(), CheckpointError> {
        self.native()?
            .advance_sidecar_counters(&self.key, &offset_counters(offsets))
            .await
            .map_err(store_error)
    }

    async fn offsets(&self) -> Result<Vec<KafkaOffset>, CheckpointError> {
        self.native()?
            .sidecar_counters(&self.key)
            .await
            .map_err(store_error)?
            .into_iter()
            .map(|(name, offset)| {
                let (topic, partition) = name
                    .rsplit_once('/')
                    .ok_or_else(|| store_error(format!("offset name '{name}' has no partition")))?;
                Ok(KafkaOffset {
                    topic: topic.to_string(),
                    partition: partition.parse().map_err(store_error)?,
                    offset,
                })
            })
            .collect()
    }

    async fn set(&self, value: Option<&Value>) -> Result<(), CheckpointError> {
        let bytes = value
            .map(serde_json::to_vec)
            .transpose()
            .map_err(store_error)?;
        self.native()?
            .set_sidecar_value(&self.key, bytes)
            .await
            .map_err(store_error)
    }
}

/// Offsets are kept as counters named `<topic>/<partition>`; a Kafka topic name
/// cannot contain `/`. The catalog keeps the higher of two writes, as the SQL
/// stores' `MAX()` does.
fn offset_counters(offsets: &[KafkaOffset]) -> Vec<(String, i64)> {
    offsets
        .iter()
        .map(|o| (format!("{}/{}", o.topic, o.partition), o.offset))
        .collect()
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_secs())
}

fn time_of(value: &Value, field: &str) -> Option<SystemTime> {
    value
        .get(field)
        .and_then(Value::as_u64)
        .and_then(|secs| UNIX_EPOCH.checked_add(Duration::from_secs(secs)))
}

/// One dataset's sidecar in the native catalog.
pub(crate) struct NativeSidecar {
    catalog: Arc<dyn MetadataCatalog>,
    dataset_name: String,
}

impl NativeSidecar {
    pub(crate) fn new(catalog: Arc<dyn MetadataCatalog>, dataset_name: String) -> Self {
        Self {
            catalog,
            dataset_name,
        }
    }

    fn values(&self, table: &str) -> Values {
        Values {
            catalog: Arc::clone(&self.catalog),
            key: format!("{table}/{}", self.dataset_name),
        }
    }
}

#[async_trait]
impl AcceleratorSidecar for NativeSidecar {
    fn blob_checkpoint_store(
        &self,
        table_name: &'static str,
    ) -> Result<Arc<dyn BlobCheckpointStore>, CheckpointError> {
        Ok(Arc::new(NativeBlobStore(self.values(table_name))))
    }

    fn kafka_checkpoint_store(&self) -> Result<Arc<dyn KafkaCheckpointStore>, CheckpointError> {
        Ok(Arc::new(NativeKafkaStore(self.values("spice_sys_kafka"))))
    }

    fn debezium_checkpoint_store(
        &self,
    ) -> Result<Arc<dyn DebeziumCheckpointStore>, CheckpointError> {
        Ok(Arc::new(NativeDebeziumStore(
            self.values("spice_sys_debezium"),
        )))
    }

    fn mysql_binlog_store(&self) -> Result<Arc<dyn MySqlBinlogStore>, CheckpointError> {
        Ok(Arc::new(NativeMySqlBinlogStore(
            self.values("spice_sys_mysql_binlog"),
        )))
    }

    fn mongo_checkpoint_store(&self) -> Result<Arc<dyn MongoCheckpointStore>, CheckpointError> {
        Ok(Arc::new(NativeMongoStore(self.values("spice_sys_mongodb"))))
    }

    async fn dataset_checkpointer(
        &self,
        _snapshot_behavior: SnapshotBehavior,
    ) -> Result<Arc<dyn DatasetCheckpointer>, CheckpointError> {
        // Snapshots are not supported under the native catalog, so the behavior is
        // not consulted.
        Ok(Arc::new(NativeDatasetCheckpointer(
            self.values(DATASET_CHECKPOINT),
        )))
    }

    async fn update_caching_engine_fetched_at(&self) -> Result<(), CheckpointError> {
        Err(unsupported_sidecar(ENGINE, "caching-engine"))
    }
}

/// `{ "data": <payload>, "updated_at": <unix seconds> }`.
struct NativeBlobStore(Values);

#[async_trait]
impl BlobCheckpointStore for NativeBlobStore {
    async fn get(&self) -> Result<Option<BlobCheckpoint>, CheckpointError> {
        let Some(value) = self.0.get().await? else {
            return Ok(None);
        };
        let data = value
            .get("data")
            .and_then(Value::as_str)
            .ok_or_else(|| store_error("blob checkpoint has no data"))?
            .to_string();
        Ok(Some(BlobCheckpoint {
            data,
            updated_at: time_of(&value, "updated_at"),
        }))
    }

    async fn upsert(&self, data: &str) -> Result<(), CheckpointError> {
        self.0
            .set(Some(&json!({ "data": data, "updated_at": now_secs() })))
            .await
    }

    async fn delete(&self) -> Result<(), CheckpointError> {
        self.0.set(None).await
    }
}

fn text(value: &Value, field: &str) -> Result<String, CheckpointError> {
    value
        .get(field)
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .ok_or_else(|| store_error(format!("checkpoint has no '{field}'")))
}

/// `{ "consumer_group_id", "topic", "schema_json" }`, plus one counter per
/// `(topic, partition)` offset.
struct NativeKafkaStore(Values);

#[async_trait]
impl KafkaCheckpointStore for NativeKafkaStore {
    async fn get(&self) -> Result<Option<KafkaCheckpoint>, CheckpointError> {
        let Some(value) = self.0.get().await? else {
            return Ok(None);
        };
        Ok(Some(KafkaCheckpoint {
            consumer_group_id: text(&value, "consumer_group_id")?,
            topic: text(&value, "topic")?,
            schema_json: text(&value, "schema_json")?,
            offsets: self.0.offsets().await?,
        }))
    }

    async fn upsert(&self, checkpoint: &KafkaCheckpoint) -> Result<(), CheckpointError> {
        self.0
            .set_with_offsets(
                &json!({
                    "consumer_group_id": checkpoint.consumer_group_id,
                    "topic": checkpoint.topic,
                    "schema_json": checkpoint.schema_json,
                }),
                &checkpoint.offsets,
            )
            .await
    }

    async fn upsert_offsets(&self, offsets: &[KafkaOffset]) -> Result<(), CheckpointError> {
        self.0.advance_offsets(offsets).await
    }
}

/// `{ "consumer_group_id", "topic", "primary_keys", "schema_fields_json" }`, plus
/// one counter per `(topic, partition)` offset.
struct NativeDebeziumStore(Values);

#[async_trait]
impl DebeziumCheckpointStore for NativeDebeziumStore {
    async fn get(&self) -> Result<Option<DebeziumCheckpoint>, CheckpointError> {
        let Some(value) = self.0.get().await? else {
            return Ok(None);
        };
        let primary_keys = value
            .get("primary_keys")
            .cloned()
            .map(serde_json::from_value)
            .transpose()
            .map_err(store_error)?
            .ok_or_else(|| store_error("checkpoint has no 'primary_keys'"))?;
        Ok(Some(DebeziumCheckpoint {
            consumer_group_id: text(&value, "consumer_group_id")?,
            topic: text(&value, "topic")?,
            primary_keys,
            schema_fields_json: text(&value, "schema_fields_json")?,
            offsets: self.0.offsets().await?,
        }))
    }

    async fn upsert(&self, checkpoint: &DebeziumCheckpoint) -> Result<(), CheckpointError> {
        self.0
            .set_with_offsets(
                &json!({
                    "consumer_group_id": checkpoint.consumer_group_id,
                    "topic": checkpoint.topic,
                    "primary_keys": checkpoint.primary_keys,
                    "schema_fields_json": checkpoint.schema_fields_json,
                }),
                &checkpoint.offsets,
            )
            .await
    }

    async fn upsert_offsets(&self, offsets: &[KafkaOffset]) -> Result<(), CheckpointError> {
        self.0.advance_offsets(offsets).await
    }
}

/// [`MongoCheckpointMetadata`] as JSON, with the store's write time.
struct NativeMongoStore(Values);

#[async_trait]
impl MongoCheckpointStore for NativeMongoStore {
    async fn get(&self) -> Option<MongoCheckpointMetadata> {
        // An unreadable token is reported as absent, as the trait specifies.
        let value = self.0.get().await.ok()??;
        Some(MongoCheckpointMetadata {
            resume_token_json: value.get("resume_token_json")?.as_str()?.to_string(),
            cluster_time_ts: value.get("cluster_time_ts").and_then(Value::as_i64),
            schema_json: value
                .get("schema_json")
                .and_then(Value::as_str)
                .map(ToString::to_string),
            updated_at: time_of(&value, "updated_at"),
        })
    }

    async fn upsert(&self, metadata: &MongoCheckpointMetadata) -> Result<(), CheckpointError> {
        // Saving the token already held commits nothing, so a committer that
        // recorded its token with the data does not commit it again.
        if let Ok(Some(current)) = self.0.get().await
            && current.get("resume_token_json").and_then(Value::as_str)
                == Some(metadata.resume_token_json.as_str())
            && current.get("cluster_time_ts").and_then(Value::as_i64) == metadata.cluster_time_ts
            && current.get("schema_json").and_then(Value::as_str) == metadata.schema_json.as_deref()
        {
            return Ok(());
        }
        self.0
            .set(Some(&json!({
                "resume_token_json": metadata.resume_token_json,
                "cluster_time_ts": metadata.cluster_time_ts,
                "schema_json": metadata.schema_json,
                "updated_at": now_secs(),
            })))
            .await
    }

    async fn delete(&self) -> Result<(), CheckpointError> {
        self.0.set(None).await
    }
}

/// The `SQLite` `spice_sys_mysql_binlog` row's columns, as one JSON object.
struct NativeMySqlBinlogStore(Values);

#[async_trait]
impl MySqlBinlogStore for NativeMySqlBinlogStore {
    async fn get(&self) -> Option<MySqlBinlogCheckpoint> {
        // An unreadable position is reported as absent; see the trait.
        let value = self.0.get().await.ok()??;
        let text = |field: &str| {
            value
                .get(field)
                .and_then(Value::as_str)
                .map(ToString::to_string)
        };
        Some(MySqlBinlogCheckpoint {
            binlog_file: text("binlog_file")?,
            binlog_pos: value.get("binlog_pos").and_then(Value::as_u64)?,
            schema_json: text("schema_json"),
            gtid_executed: text("gtid_executed"),
            cursor_type: text("cursor_type"),
            updated_at: time_of(&value, "updated_at"),
        })
    }

    async fn upsert(&self, checkpoint: &MySqlBinlogCheckpoint) -> Result<(), CheckpointError> {
        self.0
            .set(Some(&json!({
                "binlog_file": checkpoint.binlog_file,
                "binlog_pos": checkpoint.binlog_pos,
                "schema_json": checkpoint.schema_json,
                "gtid_executed": checkpoint.gtid_executed,
                "cursor_type": checkpoint.cursor_type,
                "updated_at": now_secs(),
            })))
            .await
    }

    async fn delete(&self) -> Result<(), CheckpointError> {
        self.0.set(None).await
    }
}

/// `{ "schema_json", "refresh_sql", "updated_at" }`, as the `SQLite` checkpoint row.
struct NativeDatasetCheckpointer(Values);

#[async_trait]
impl DatasetCheckpointer for NativeDatasetCheckpointer {
    async fn exists(&self) -> bool {
        matches!(self.0.get().await, Ok(Some(_)))
    }

    async fn checkpoint(
        &self,
        schema: &SchemaRef,
        refresh_sql: Option<&str>,
    ) -> runtime_acceleration::dataset_checkpoint::Result<()> {
        let schema_json = serialize_schema(schema)?;
        self.0
            .set(Some(&json!({
                "schema_json": schema_json,
                "refresh_sql": refresh_sql,
                "updated_at": now_secs(),
            })))
            .await
            .map_err(Into::into)
    }

    async fn get_schema(
        &self,
    ) -> runtime_acceleration::dataset_checkpoint::Result<Option<SchemaRef>> {
        let Some(value) = self.0.get().await? else {
            return Ok(None);
        };
        let json = value
            .get("schema_json")
            .and_then(Value::as_str)
            .ok_or_else(|| store_error("dataset checkpoint has no schema"))?;
        Ok(Some(deserialize_schema(json)?))
    }

    async fn last_checkpoint_time(
        &self,
    ) -> runtime_acceleration::dataset_checkpoint::Result<Option<SystemTime>> {
        Ok(self
            .0
            .get()
            .await?
            .and_then(|value| time_of(&value, "updated_at")))
    }

    async fn get_refresh_sql(
        &self,
    ) -> runtime_acceleration::dataset_checkpoint::Result<Option<String>> {
        Ok(self.0.get().await?.and_then(|value| {
            value
                .get("refresh_sql")
                .and_then(Value::as_str)
                .map(ToString::to_string)
        }))
    }

    async fn set_schema(
        &self,
        schema: &SchemaRef,
    ) -> runtime_acceleration::dataset_checkpoint::Result<()> {
        // An absent checkpoint stays absent, so the refresh clock is not reset.
        let Some(mut value) = self.0.get().await? else {
            return Ok(());
        };
        value["schema_json"] = Value::String(serialize_schema(schema)?);
        self.0.set(Some(&value)).await.map_err(Into::into)
    }

    async fn delete(&self) -> runtime_acceleration::dataset_checkpoint::Result<()> {
        self.0.set(None).await.map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_table_providers::sql::db_connection_pool::{
        JoinPushDown, Mode, sqlitepool::SqliteConnectionPool,
    };
    use object_store::{memory::InMemory, path::Path};
    use runtime_checkpoint_sqlite::{
        SqliteDebeziumCheckpointStore, SqliteKafkaCheckpointStore, SqliteMongoCheckpointStore,
        SqliteMySqlBinlogStore,
    };

    async fn sidecars() -> (NativeSidecar, Arc<SqliteConnectionPool>, tempfile::TempDir) {
        let catalog = ObjectNativeCatalog::create(Arc::new(InMemory::new()), Path::from("catalog"))
            .await
            .expect("native catalog");
        let directory = tempfile::tempdir().expect("tempdir");
        let pool = SqliteConnectionPool::new(
            &directory.path().join("sidecar.db").to_string_lossy(),
            Mode::File,
            JoinPushDown::Disallow,
            Vec::new(),
            Duration::from_secs(5),
        )
        .await
        .expect("sqlite pool");
        (
            NativeSidecar::new(Arc::new(catalog), "orders".to_string()),
            Arc::new(pool),
            directory,
        )
    }

    fn offset(topic: &str, partition: i32, offset: i64) -> KafkaOffset {
        KafkaOffset {
            topic: topic.to_string(),
            partition,
            offset,
        }
    }

    fn sorted(mut offsets: Vec<KafkaOffset>) -> Vec<(String, i32, i64)> {
        offsets.sort_by(|a, b| (&a.topic, a.partition).cmp(&(&b.topic, b.partition)));
        offsets
            .into_iter()
            .map(|o| (o.topic, o.partition, o.offset))
            .collect()
    }

    /// The same Kafka calls through the `SQLite` store and the native one give
    /// the same answers: offsets keep the higher value, and a regressing write
    /// does not move one back.
    #[tokio::test]
    async fn kafka_offsets_match_the_sqlite_store() {
        let (native, pool, _directory) = sidecars().await;
        let sqlite = SqliteKafkaCheckpointStore::new(pool, "orders".to_string());
        let native = native.kafka_checkpoint_store().expect("native kafka store");
        let stores: [&dyn KafkaCheckpointStore; 2] = [&sqlite, native.as_ref()];
        let checkpoint = KafkaCheckpoint {
            consumer_group_id: "group".to_string(),
            topic: "orders".to_string(),
            schema_json: "{}".to_string(),
            offsets: vec![offset("orders", 0, 10), offset("orders", 1, 5)],
        };
        let mut answers = Vec::new();
        for store in stores {
            assert!(store.get().await.expect("get").is_none());
            store.upsert(&checkpoint).await.expect("upsert");
            store
                .upsert_offsets(&[
                    offset("orders", 0, 7),
                    offset("orders", 1, 9),
                    offset("orders", 2, 1),
                ])
                .await
                .expect("upsert offsets");
            let got = store.get().await.expect("get").expect("checkpoint");
            answers.push((
                got.consumer_group_id,
                got.topic,
                got.schema_json,
                sorted(got.offsets),
            ));
        }
        assert_eq!(answers[0], answers[1]);
        assert_eq!(
            answers[1].3,
            [
                ("orders".to_string(), 0, 10),
                ("orders".to_string(), 1, 9),
                ("orders".to_string(), 2, 1)
            ]
        );
    }

    #[tokio::test]
    async fn debezium_checkpoint_matches_the_sqlite_store() {
        let (native, pool, _directory) = sidecars().await;
        let sqlite = SqliteDebeziumCheckpointStore::new(pool, "orders".to_string());
        let native = native
            .debezium_checkpoint_store()
            .expect("native debezium store");
        let stores: [&dyn DebeziumCheckpointStore; 2] = [&sqlite, native.as_ref()];
        let mut answers = Vec::new();
        for store in stores {
            assert!(store.get().await.expect("get").is_none());
            store
                .upsert(&DebeziumCheckpoint {
                    consumer_group_id: "group".to_string(),
                    topic: "server.public.orders".to_string(),
                    primary_keys: vec!["o_id".to_string(), "o_w_id".to_string()],
                    schema_fields_json: "[]".to_string(),
                    offsets: vec![offset("server.public.orders", 0, 42)],
                })
                .await
                .expect("upsert");
            store
                .upsert_offsets(&[offset("server.public.orders", 0, 41)])
                .await
                .expect("upsert offsets");
            let got = store.get().await.expect("get").expect("checkpoint");
            answers.push((
                got.consumer_group_id,
                got.topic,
                got.primary_keys,
                got.schema_fields_json,
                sorted(got.offsets),
            ));
        }
        assert_eq!(answers[0], answers[1]);
        assert_eq!(answers[1].4, [("server.public.orders".to_string(), 0, 42)]);
    }

    #[tokio::test]
    async fn mongo_and_mysql_positions_match_the_sqlite_stores() {
        let (native, pool, _directory) = sidecars().await;
        let sqlite_mongo = SqliteMongoCheckpointStore::new(Arc::clone(&pool), "orders".to_string());
        let native_mongo = native.mongo_checkpoint_store().expect("native mongo store");
        let mut tokens = Vec::new();
        for store in [
            &sqlite_mongo as &dyn MongoCheckpointStore,
            native_mongo.as_ref(),
        ] {
            assert!(store.get().await.is_none());
            store
                .upsert(&MongoCheckpointMetadata {
                    resume_token_json: r#"{"_data":"8263"}"#.to_string(),
                    cluster_time_ts: Some(1_793_400_000),
                    schema_json: Some("{}".to_string()),
                    updated_at: None,
                })
                .await
                .expect("upsert");
            let got = store.get().await.expect("token");
            tokens.push((got.resume_token_json, got.cluster_time_ts, got.schema_json));
            store.delete().await.expect("delete");
            assert!(store.get().await.is_none());
        }
        assert_eq!(tokens[0], tokens[1]);

        let sqlite_mysql = SqliteMySqlBinlogStore::new(pool, "orders".to_string());
        let native_mysql = native.mysql_binlog_store().expect("native mysql store");
        let mut positions = Vec::new();
        for store in [
            &sqlite_mysql as &dyn MySqlBinlogStore,
            native_mysql.as_ref(),
        ] {
            assert!(store.get().await.is_none());
            store
                .upsert(&MySqlBinlogCheckpoint {
                    binlog_file: "binlog.000042".to_string(),
                    binlog_pos: 4_096,
                    schema_json: Some("{}".to_string()),
                    gtid_executed: Some("3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5".to_string()),
                    cursor_type: Some("gtid".to_string()),
                    updated_at: None,
                })
                .await
                .expect("upsert");
            let got = store.get().await.expect("position");
            positions.push((
                got.binlog_file,
                got.binlog_pos,
                got.schema_json,
                got.gtid_executed,
                got.cursor_type,
            ));
            store.delete().await.expect("delete");
            assert!(store.get().await.is_none());
        }
        assert_eq!(positions[0], positions[1]);
    }
}
