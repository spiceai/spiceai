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

use datafusion_table_providers::sql::db_connection_pool::{
    dbconnection::sqliteconn::SqliteConnection, sqlitepool::SqliteConnectionPool,
};

use super::{Error, KAFKA_TABLE_NAME, KafkaSys, Result};
use crate::dataconnector::kafka::KafkaMetadata;
use data_components::kafka::KafkaOffset;

impl KafkaSys {
    pub(super) async fn upsert_sqlite(
        &self,
        pool: &SqliteConnectionPool,
        metadata: &KafkaMetadata,
    ) -> Result<()> {
        let schema_json = Self::serialize_schema(&metadata.schema)?;
        let offsets_json = Self::serialize_offsets(&metadata.offsets)?;
        let dataset_name = self.dataset_name.clone();
        let consumer_group_id = metadata.consumer_group_id.clone();
        let topic = metadata.topic.clone();

        let conn_sync = pool.connect_sync();
        let Some(conn) = conn_sync.as_any().downcast_ref::<SqliteConnection>() else {
            return Err(Error::DowncastFailed {
                target: "SqliteConnection",
            });
        };

        conn.conn
            .call(move |conn| {
                ensure_kafka_table(conn)?;

                let upsert = format!(
                    "INSERT INTO {KAFKA_TABLE_NAME} (dataset_name, consumer_group_id, topic, schema_json, offsets_json, created_at, updated_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                     ON CONFLICT (dataset_name) DO UPDATE SET
                        consumer_group_id = ?2,
                        topic = ?3,
                        schema_json = ?4,
                        offsets_json = ?5,
                        updated_at = CURRENT_TIMESTAMP"
                );
                conn.execute(
                    &upsert,
                    [dataset_name, consumer_group_id, topic, schema_json, offsets_json],
                )?;

                Ok::<(), rusqlite::Error>(())
            })
            .await
            .map_err(Error::external)?;

        self.mark_schema_ensured();
        Ok(())
    }

    pub(super) async fn get_sqlite(&self, pool: &SqliteConnectionPool) -> Option<KafkaMetadata> {
        let dataset_name = self.dataset_name.clone();

        let conn_sync = pool.connect_sync();
        let conn = conn_sync.as_any().downcast_ref::<SqliteConnection>()?;

        let metadata = conn
            .conn
            .call(move |conn| {
                ensure_kafka_table(conn)?;

                let query = format!(
                    "SELECT consumer_group_id, topic, schema_json, offsets_json FROM {KAFKA_TABLE_NAME} WHERE dataset_name = ?"
                );
                let mut stmt = conn.prepare(&query)?;
                let mut rows = stmt.query([dataset_name])?;

                if let Some(row) = rows.next()? {
                    let consumer_group_id: String = row.get(0)?;
                    let topic: String = row.get(1)?;
                    let schema_json: String = row.get(2)?;
                    let offsets_json: Option<String> = row.get(3)?;

                    Ok(KafkaMetadata {
                        consumer_group_id,
                        topic,
                        schema: KafkaSys::deserialize_schema(&schema_json)
                            .map_err(|err| {
                                tracing::warn!("Failed to deserialize Kafka schema from SQLite: {err}");
                                rusqlite::Error::InvalidQuery
                            })?,
                        offsets: KafkaSys::deserialize_offsets(offsets_json.as_deref())
                            .map_err(|err| {
                                tracing::warn!("Failed to deserialize Kafka offsets from SQLite: {err}");
                                rusqlite::Error::InvalidQuery
                            })?,
                    })
                } else {
                    Err(rusqlite::Error::QueryReturnedNoRows)
                }
            })
            .await
            .ok()?;

        self.mark_schema_ensured();
        Some(metadata)
    }

    pub(super) async fn upsert_offsets_sqlite(
        &self,
        pool: &SqliteConnectionPool,
        offsets: &[KafkaOffset],
    ) -> Result<()> {
        let dataset_name = self.dataset_name.clone();
        let offsets_json = Self::serialize_offsets(offsets)?;
        let schema_needs_ensure = self.schema_needs_ensure();

        let conn_sync = pool.connect_sync();
        let Some(conn) = conn_sync.as_any().downcast_ref::<SqliteConnection>() else {
            return Err(Error::DowncastFailed {
                target: "SqliteConnection",
            });
        };

        conn.conn
            .call(move |conn| {
                if schema_needs_ensure {
                    ensure_kafka_table(conn)?;
                }
                let update = format!(
                    "UPDATE {KAFKA_TABLE_NAME} SET offsets_json = ?1, updated_at = CURRENT_TIMESTAMP WHERE dataset_name = ?2"
                );
                let changed = conn.execute(&update, [offsets_json, dataset_name])?;
                if changed == 0 {
                    return Err(rusqlite::Error::QueryReturnedNoRows);
                }
                Ok::<(), rusqlite::Error>(())
            })
            .await
            .map_err(Error::external)?;

        if schema_needs_ensure {
            self.mark_schema_ensured();
        }

        Ok(())
    }
}

fn ensure_kafka_table(conn: &rusqlite::Connection) -> rusqlite::Result<()> {
    let create_table = format!(
        "CREATE TABLE IF NOT EXISTS {KAFKA_TABLE_NAME} (
            dataset_name TEXT PRIMARY KEY,
            consumer_group_id TEXT,
            topic TEXT,
            schema_json TEXT,
            offsets_json TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )"
    );
    conn.execute(&create_table, [])?;

    let table_info = format!("PRAGMA table_info({KAFKA_TABLE_NAME})");
    let mut stmt = conn.prepare(&table_info)?;
    let columns = stmt.query_map([], |row| row.get::<_, String>(1))?;
    let mut has_offsets_json = false;
    for column in columns {
        if column? == "offsets_json" {
            has_offsets_json = true;
            break;
        }
    }

    if !has_offsets_json {
        let add_offsets = format!("ALTER TABLE {KAFKA_TABLE_NAME} ADD COLUMN offsets_json TEXT");
        conn.execute(&add_offsets, [])?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        builder::RuntimeBuilder,
        component::dataset::{
            Dataset,
            acceleration::{Acceleration, Engine, Mode},
            builder::DatasetBuilder,
        },
        dataaccelerator::spice_sys::OpenOption,
    };
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;
    use tempfile::TempDir;

    async fn create_test_dataset(ds_name: &str) -> (Dataset, TempDir) {
        let app = app::AppBuilder::new("test").build();
        let runtime = RuntimeBuilder::new().build().await;

        let mut dataset = DatasetBuilder::try_new("spice.ai".to_string(), ds_name)
            .expect("to create dataset builder")
            .with_app(Arc::new(app))
            .with_runtime(Arc::new(runtime))
            .build()
            .expect("to create dataset");

        let temp_dir = TempDir::new().expect("to create temp dir");
        let db_path = temp_dir
            .path()
            .join(format!("kafka_sqlite_test_{ds_name}.db"));
        dataset.acceleration = Some(Acceleration {
            engine: Engine::Sqlite,
            mode: Mode::File,
            params: [(
                "sqlite_file".to_string(),
                db_path.to_string_lossy().to_string(),
            )]
            .into_iter()
            .collect(),
            ..Default::default()
        });

        (dataset, temp_dir)
    }

    fn create_test_metadata() -> KafkaMetadata {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        KafkaMetadata {
            consumer_group_id: "test-group-123".to_string(),
            topic: "test-topic".to_string(),
            schema,
            offsets: vec![KafkaOffset {
                topic: "test-topic".to_string(),
                partition: 0,
                offset: 42,
            }],
        }
    }

    #[tokio::test]
    async fn test_sqlite_roundtrip() {
        let (ds, _temp_dir) = create_test_dataset("test_sqlite_roundtrip").await;
        let kafka_sys = KafkaSys::try_new(&ds, OpenOption::CreateIfNotExists)
            .await
            .expect("to create KafkaSys");

        let test_metadata = create_test_metadata();

        kafka_sys
            .upsert(&test_metadata)
            .await
            .expect("to upsert metadata");
        let retrieved = kafka_sys.get().await.expect("to retrieve metadata");

        assert_eq!(retrieved.consumer_group_id, test_metadata.consumer_group_id);
        assert_eq!(retrieved.topic, test_metadata.topic);
        assert_eq!(retrieved.schema, test_metadata.schema);
        assert_eq!(retrieved.offsets, test_metadata.offsets);
    }

    #[tokio::test]
    async fn test_sqlite_metadata_overwrite() {
        let (ds, _temp_dir) = create_test_dataset("test_sqlite_metadata_overwrite").await;
        let kafka_sys = KafkaSys::try_new(&ds, OpenOption::CreateIfNotExists)
            .await
            .expect("to create KafkaSys");
        let mut test_metadata = create_test_metadata();

        kafka_sys
            .upsert(&test_metadata)
            .await
            .expect("to upsert metadata");

        test_metadata.consumer_group_id = "updated-group-456".to_string();
        test_metadata.topic = "updated-topic".to_string();
        kafka_sys
            .upsert(&test_metadata)
            .await
            .expect("to overwrite metadata");

        let retrieved = kafka_sys.get().await.expect("to retrieve metadata");
        assert_eq!(retrieved.consumer_group_id, "updated-group-456");
        assert_eq!(retrieved.topic, "updated-topic");
        assert_eq!(retrieved.schema, test_metadata.schema);
        assert_eq!(retrieved.offsets, test_metadata.offsets);
    }

    #[tokio::test]
    async fn test_sqlite_offsets_update() {
        let (ds, _temp_dir) = create_test_dataset("test_sqlite_offsets_update").await;
        let kafka_sys = KafkaSys::try_new(&ds, OpenOption::CreateIfNotExists)
            .await
            .expect("to create KafkaSys");
        let test_metadata = create_test_metadata();

        kafka_sys
            .upsert(&test_metadata)
            .await
            .expect("to upsert metadata");

        let offsets = vec![KafkaOffset {
            topic: "test-topic".to_string(),
            partition: 1,
            offset: 99,
        }];
        kafka_sys
            .upsert_offsets(&offsets)
            .await
            .expect("to upsert offsets");

        let retrieved = kafka_sys.get().await.expect("to retrieve metadata");
        assert_eq!(retrieved.offsets, offsets);
    }

    #[tokio::test]
    async fn test_sqlite_get_nonexistent() {
        let (ds, _temp_dir) = create_test_dataset("test_sqlite_get_nonexistent").await;
        let kafka_sys = KafkaSys::try_new(&ds, OpenOption::CreateIfNotExists)
            .await
            .expect("to create KafkaSys");

        let result = kafka_sys.get().await;
        assert!(
            result.is_none(),
            "Should return None for nonexistent dataset"
        );
    }
}
