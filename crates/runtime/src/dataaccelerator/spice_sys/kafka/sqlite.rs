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
use rusqlite::OptionalExtension;

use super::super::offsets::{self, sort_offsets};
use super::{Error, KAFKA_OFFSETS_TABLE_NAME, KAFKA_TABLE_NAME, KafkaSys, Result};
use crate::dataconnector::kafka::KafkaMetadata;
use data_components::kafka::KafkaOffset;

impl KafkaSys {
    pub(super) async fn upsert_sqlite(
        &self,
        pool: &SqliteConnectionPool,
        metadata: &KafkaMetadata,
    ) -> Result<()> {
        let schema_json = Self::serialize_schema(&metadata.schema)?;
        let dataset_name = self.dataset_name.clone();
        let consumer_group_id = metadata.consumer_group_id.clone();
        let topic = metadata.topic.clone();
        let seed_offsets = metadata.offsets.clone();

        let conn_sync = pool.connect_sync();
        let Some(conn) = conn_sync.as_any().downcast_ref::<SqliteConnection>() else {
            return Err(Error::DowncastFailed {
                target: "SqliteConnection",
            });
        };

        conn.conn
            .call(move |conn| {
                ensure_kafka_tables(conn)?;

                let tx = conn.transaction()?;
                let upsert = format!(
                    "INSERT INTO {KAFKA_TABLE_NAME} (dataset_name, consumer_group_id, topic, schema_json, created_at, updated_at)
                     VALUES (?1, ?2, ?3, ?4, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                     ON CONFLICT (dataset_name) DO UPDATE SET
                        consumer_group_id = ?2,
                        topic = ?3,
                        schema_json = ?4,
                        updated_at = CURRENT_TIMESTAMP"
                );
                tx.execute(
                    &upsert,
                    rusqlite::params![dataset_name, consumer_group_id, topic, schema_json],
                )?;
                upsert_offsets_into(&tx, &dataset_name, &seed_offsets)?;
                tx.commit()?;
                Ok::<(), rusqlite::Error>(())
            })
            .await
            .map_err(Error::external)?;

        self.schema_ensured.mark_ensured();
        Ok(())
    }

    pub(super) async fn get_sqlite(
        &self,
        pool: &SqliteConnectionPool,
    ) -> Result<Option<KafkaMetadata>> {
        let dataset_name = self.dataset_name.clone();
        let schema_needs_ensure = self.schema_needs_ensure();

        let conn_sync = pool.connect_sync();
        let Some(conn) = conn_sync.as_any().downcast_ref::<SqliteConnection>() else {
            return Err(Error::DowncastFailed {
                target: "SqliteConnection",
            });
        };

        type MetadataRow = (String, String, String);
        let result = conn
            .conn
            .call(move |conn| {
                if schema_needs_ensure {
                    ensure_kafka_tables(conn)?;
                }

                let metadata_query = format!(
                    "SELECT consumer_group_id, topic, schema_json FROM {KAFKA_TABLE_NAME} WHERE dataset_name = ?1"
                );
                let metadata: Option<MetadataRow> = conn
                    .query_row(&metadata_query, [&dataset_name], |row| {
                        Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                    })
                    .optional()?;

                let Some((consumer_group_id, topic, schema_json)) = metadata else {
                    return Ok::<Option<(String, String, String, Vec<KafkaOffset>)>, rusqlite::Error>(None);
                };

                let offsets = load_offsets(conn, &dataset_name)?;
                Ok(Some((consumer_group_id, topic, schema_json, offsets)))
            })
            .await
            .map_err(Error::external)?;

        if schema_needs_ensure {
            self.mark_schema_ensured();
        }

        let Some((consumer_group_id, topic, schema_json, offsets)) = result else {
            return Ok(None);
        };

        Ok(Some(KafkaMetadata {
            consumer_group_id,
            topic,
            schema: KafkaSys::deserialize_schema(&schema_json)?,
            offsets,
        }))
    }

    pub(super) async fn upsert_offsets_sqlite(
        &self,
        pool: &SqliteConnectionPool,
        offsets: &[KafkaOffset],
    ) -> Result<()> {
        let dataset_name = self.dataset_name.clone();
        let new_offsets = offsets.to_vec();
        let warn_dataset = self.dataset_name.clone();
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
                    ensure_kafka_tables(conn)?;
                }

                // Diagnostic-only: surface a warn log when an offset regresses.
                // The SQL MAX() in upsert_offsets_into is the source of truth.
                if let Ok(prior) = load_offsets(conn, &dataset_name) {
                    let _ = offsets::merge_offsets(&warn_dataset, prior, &new_offsets);
                }

                let tx = conn.transaction()?;
                upsert_offsets_into(&tx, &dataset_name, &new_offsets)?;
                tx.commit()?;
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

fn ensure_kafka_tables(conn: &rusqlite::Connection) -> rusqlite::Result<()> {
    let create_metadata = format!(
        "CREATE TABLE IF NOT EXISTS {KAFKA_TABLE_NAME} (
            dataset_name TEXT PRIMARY KEY,
            consumer_group_id TEXT,
            topic TEXT,
            schema_json TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )"
    );
    conn.execute(&create_metadata, [])?;

    let create_offsets = format!(
        "CREATE TABLE IF NOT EXISTS {KAFKA_OFFSETS_TABLE_NAME} (
            dataset_name TEXT NOT NULL,
            topic TEXT NOT NULL,
            partition_id INTEGER NOT NULL,
            partition_offset BIGINT NOT NULL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (dataset_name, topic, partition_id)
        )"
    );
    conn.execute(&create_offsets, [])?;
    Ok(())
}

fn upsert_offsets_into(
    tx: &rusqlite::Transaction<'_>,
    dataset_name: &str,
    offsets: &[KafkaOffset],
) -> rusqlite::Result<()> {
    if offsets.is_empty() {
        return Ok(());
    }
    let stmt_sql = format!(
        "INSERT INTO {KAFKA_OFFSETS_TABLE_NAME}
            (dataset_name, topic, partition_id, partition_offset, updated_at)
         VALUES (?1, ?2, ?3, ?4, CURRENT_TIMESTAMP)
         ON CONFLICT (dataset_name, topic, partition_id) DO UPDATE SET
            partition_offset = MAX(excluded.partition_offset, partition_offset),
            updated_at = CURRENT_TIMESTAMP"
    );
    let mut stmt = tx.prepare(&stmt_sql)?;
    for offset in offsets {
        stmt.execute(rusqlite::params![
            dataset_name,
            offset.topic,
            offset.partition,
            offset.offset,
        ])?;
    }
    Ok(())
}

fn load_offsets(
    conn: &rusqlite::Connection,
    dataset_name: &str,
) -> rusqlite::Result<Vec<KafkaOffset>> {
    let query = format!(
        "SELECT topic, partition_id, partition_offset FROM {KAFKA_OFFSETS_TABLE_NAME} WHERE dataset_name = ?1"
    );
    let mut stmt = conn.prepare(&query)?;
    let rows = stmt.query_map([dataset_name], |row| {
        Ok(KafkaOffset {
            topic: row.get(0)?,
            partition: row.get(1)?,
            offset: row.get(2)?,
        })
    })?;
    let mut out: Vec<KafkaOffset> = rows.collect::<rusqlite::Result<_>>()?;
    sort_offsets(&mut out);
    Ok(out)
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
        let retrieved = kafka_sys
            .get()
            .await
            .expect("to retrieve metadata")
            .expect("metadata to exist");

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

        let retrieved = kafka_sys
            .get()
            .await
            .expect("to retrieve metadata")
            .expect("metadata to exist");
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

        let retrieved = kafka_sys
            .get()
            .await
            .expect("to retrieve metadata")
            .expect("metadata to exist");
        let mut expected_offsets = test_metadata.offsets.clone();
        expected_offsets.extend(offsets);
        assert_eq!(retrieved.offsets, expected_offsets);
    }

    #[tokio::test]
    async fn test_sqlite_get_nonexistent() {
        let (ds, _temp_dir) = create_test_dataset("test_sqlite_get_nonexistent").await;
        let kafka_sys = KafkaSys::try_new(&ds, OpenOption::CreateIfNotExists)
            .await
            .expect("to create KafkaSys");

        let result = kafka_sys.get().await;
        assert!(
            result.expect("to get empty metadata").is_none(),
            "Should return None for nonexistent dataset"
        );
    }

    /// Regression for finding #2: `upsert_offsets` used to fail with "Kafka
    /// sidecar metadata for dataset X does not exist" when no metadata row
    /// existed yet. With per-partition storage the offsets always land.
    #[tokio::test]
    async fn test_sqlite_offsets_update_succeeds_without_metadata_row() {
        let (ds, _temp_dir) = create_test_dataset("test_sqlite_offsets_no_metadata").await;
        let kafka_sys = KafkaSys::try_new(&ds, OpenOption::CreateIfNotExists)
            .await
            .expect("to create KafkaSys");

        let offsets = vec![KafkaOffset {
            topic: "test-topic".to_string(),
            partition: 0,
            offset: 42,
        }];
        kafka_sys
            .upsert_offsets(&offsets)
            .await
            .expect("upsert_offsets should succeed without a prior metadata row");

        // `get()` returns None because the metadata row is missing, but the
        // offset is durably persisted; a later `upsert(metadata)` will
        // surface it.
        let result = kafka_sys.get().await.expect("to query metadata");
        assert!(result.is_none(), "metadata row was never written");
    }

    /// Regression for finding #1 part a: concurrent `upsert_offsets` over
    /// disjoint partitions must keep every writer's data.
    #[tokio::test]
    async fn test_sqlite_concurrent_upserts_do_not_lose_partitions() {
        let (ds, _temp_dir) = create_test_dataset("test_sqlite_concurrent_upserts").await;
        let kafka_sys = Arc::new(
            KafkaSys::try_new(&ds, OpenOption::CreateIfNotExists)
                .await
                .expect("to create KafkaSys"),
        );

        let num_tasks = 8_i32;
        let partitions_per_task = 8_i32;

        let mut handles = Vec::new();
        for task_idx in 0..num_tasks {
            let kafka_sys = Arc::clone(&kafka_sys);
            handles.push(tokio::spawn(async move {
                let offsets: Vec<KafkaOffset> = (0..partitions_per_task)
                    .map(|partition_index| KafkaOffset {
                        topic: "concurrent-topic".to_string(),
                        partition: task_idx * partitions_per_task + partition_index,
                        offset: 100 + i64::from(partition_index),
                    })
                    .collect();
                kafka_sys
                    .upsert_offsets(&offsets)
                    .await
                    .expect("concurrent upsert_offsets should succeed");
            }));
        }
        for h in handles {
            h.await.expect("task join");
        }

        kafka_sys
            .upsert(&create_test_metadata())
            .await
            .expect("to upsert metadata after concurrent offset writes");

        let retrieved = kafka_sys
            .get()
            .await
            .expect("to retrieve metadata")
            .expect("metadata to exist");

        let expected_count = usize::try_from(num_tasks * partitions_per_task)
            .expect("test offset count should fit in usize");
        let concurrent_count = retrieved
            .offsets
            .iter()
            .filter(|o| o.topic == "concurrent-topic")
            .count();
        assert_eq!(
            concurrent_count, expected_count,
            "all per-partition offsets must land after concurrent writes"
        );
    }

    /// Regression for finding #1 part b: when two writers race on the same
    /// (topic, partition), the storage layer must keep the highest offset.
    #[tokio::test]
    async fn test_sqlite_concurrent_same_partition_keeps_max() {
        let (ds, _temp_dir) = create_test_dataset("test_sqlite_concurrent_same_partition").await;
        let kafka_sys = Arc::new(
            KafkaSys::try_new(&ds, OpenOption::CreateIfNotExists)
                .await
                .expect("to create KafkaSys"),
        );
        kafka_sys
            .upsert(&create_test_metadata())
            .await
            .expect("seed metadata");

        let mut handles = Vec::new();
        for off in [10_i64, 50, 30, 100, 70, 5] {
            let kafka_sys = Arc::clone(&kafka_sys);
            handles.push(tokio::spawn(async move {
                kafka_sys
                    .upsert_offsets(&[KafkaOffset {
                        topic: "test-topic".to_string(),
                        partition: 0,
                        offset: off,
                    }])
                    .await
                    .expect("upsert");
            }));
        }
        for h in handles {
            h.await.expect("task join");
        }

        let retrieved = kafka_sys
            .get()
            .await
            .expect("to retrieve")
            .expect("to exist");
        let p0 = retrieved
            .offsets
            .iter()
            .find(|o| o.partition == 0 && o.topic == "test-topic")
            .expect("partition 0 present");
        assert_eq!(p0.offset, 100, "must keep the highest concurrent offset");
    }

    /// Regression for finding #5: a backward offset must NOT overwrite a
    /// higher stored offset.
    #[tokio::test]
    async fn test_sqlite_backward_offset_does_not_regress() {
        let (ds, _temp_dir) = create_test_dataset("test_sqlite_backward_offset").await;
        let kafka_sys = KafkaSys::try_new(&ds, OpenOption::CreateIfNotExists)
            .await
            .expect("to create KafkaSys");
        kafka_sys
            .upsert(&create_test_metadata())
            .await
            .expect("seed metadata");

        kafka_sys
            .upsert_offsets(&[KafkaOffset {
                topic: "test-topic".to_string(),
                partition: 0,
                offset: 500,
            }])
            .await
            .expect("forward upsert");
        kafka_sys
            .upsert_offsets(&[KafkaOffset {
                topic: "test-topic".to_string(),
                partition: 0,
                offset: 100,
            }])
            .await
            .expect("backward upsert");

        let retrieved = kafka_sys.get().await.expect("retrieve").expect("exist");
        let p0 = retrieved
            .offsets
            .iter()
            .find(|o| o.partition == 0)
            .expect("partition 0 present");
        assert_eq!(p0.offset, 500, "backward offset must not overwrite");
    }
}
