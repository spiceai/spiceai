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
    debezium::DebeziumCheckpointStore,
    kafka::KafkaCheckpointStore,
    mongodb::MongoCheckpointStore,
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
        Err(unsupported_sidecar(ENGINE, "kafka"))
    }

    fn debezium_checkpoint_store(
        &self,
    ) -> Result<Arc<dyn DebeziumCheckpointStore>, CheckpointError> {
        Err(unsupported_sidecar(ENGINE, "debezium"))
    }

    fn mysql_binlog_store(&self) -> Result<Arc<dyn MySqlBinlogStore>, CheckpointError> {
        Ok(Arc::new(NativeMySqlBinlogStore(
            self.values("spice_sys_mysql_binlog"),
        )))
    }

    fn mongo_checkpoint_store(&self) -> Result<Arc<dyn MongoCheckpointStore>, CheckpointError> {
        Err(unsupported_sidecar(ENGINE, "mongodb"))
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
