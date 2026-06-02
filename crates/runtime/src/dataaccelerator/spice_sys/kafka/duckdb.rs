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

use super::super::offsets::{self, sort_offsets};
use super::{Error, KAFKA_OFFSETS_TABLE_NAME, KAFKA_TABLE_NAME, KafkaMetadata, KafkaSys, Result};
use data_components::kafka::KafkaOffset;
use datafusion_table_providers::sql::db_connection_pool::duckdbpool::DuckDbConnectionPool;
use spiceai_duckdb as duckdb;
use std::sync::Arc;

impl KafkaSys {
    pub(super) fn upsert_duckdb(
        &self,
        pool: &Arc<DuckDbConnectionPool>,
        metadata: &KafkaMetadata,
    ) -> Result<()> {
        let mut db_conn = Arc::clone(pool).connect_sync().map_err(Error::external)?;
        let duckdb_conn = datafusion_table_providers::duckdb::DuckDB::duckdb_conn(&mut db_conn)
            .map_err(Error::external)?
            .get_underlying_conn_mut();

        ensure_kafka_tables(duckdb_conn)?;
        self.schema_ensured.mark_ensured();

        let schema_json = Self::serialize_schema(&metadata.schema)?;

        let tx = duckdb_conn.transaction().map_err(Error::external)?;
        let upsert = format!(
            "INSERT INTO {KAFKA_TABLE_NAME} (dataset_name, consumer_group_id, topic, schema_json, created_at, updated_at)
             VALUES (?, ?, ?, ?, now(), now())
             ON CONFLICT (dataset_name) DO UPDATE SET
                consumer_group_id = excluded.consumer_group_id,
                topic = excluded.topic,
                schema_json = excluded.schema_json,
                updated_at = now()"
        );
        tx.execute(
            &upsert,
            duckdb::params![
                self.dataset_name,
                metadata.consumer_group_id,
                metadata.topic,
                schema_json,
            ],
        )
        .map_err(Error::external)?;
        upsert_offsets_tx(&tx, &self.dataset_name, &metadata.offsets)?;
        tx.commit().map_err(Error::external)?;

        Ok(())
    }

    pub(super) fn get_duckdb(
        &self,
        pool: &Arc<DuckDbConnectionPool>,
    ) -> Result<Option<KafkaMetadata>> {
        let mut db_conn = Arc::clone(pool).connect_sync().map_err(Error::external)?;
        let duckdb_conn = datafusion_table_providers::duckdb::DuckDB::duckdb_conn(&mut db_conn)
            .map_err(Error::external)?
            .get_underlying_conn_mut();

        if self.schema_needs_ensure() {
            ensure_kafka_tables(duckdb_conn)?;
            self.mark_schema_ensured();
        }

        let query = format!(
            "SELECT consumer_group_id, topic, schema_json FROM {KAFKA_TABLE_NAME} WHERE dataset_name = ?"
        );
        let mut stmt = duckdb_conn.prepare(&query).map_err(Error::external)?;
        let mut rows = stmt.query([&self.dataset_name]).map_err(Error::external)?;

        let Some(row) = rows.next().map_err(Error::external)? else {
            return Ok(None);
        };

        let consumer_group_id: String = row.get(0).map_err(Error::external)?;
        let topic: String = row.get(1).map_err(Error::external)?;
        let schema_json: String = row.get(2).map_err(Error::external)?;
        drop(rows);
        drop(stmt);

        let offsets = load_offsets(duckdb_conn, &self.dataset_name)?;

        Ok(Some(KafkaMetadata {
            consumer_group_id,
            topic,
            schema: KafkaSys::deserialize_schema(&schema_json)?,
            offsets,
        }))
    }

    pub(super) fn upsert_offsets_duckdb(
        &self,
        pool: &Arc<DuckDbConnectionPool>,
        offsets: &[KafkaOffset],
    ) -> Result<()> {
        let mut db_conn = Arc::clone(pool).connect_sync().map_err(Error::external)?;
        let duckdb_conn = datafusion_table_providers::duckdb::DuckDB::duckdb_conn(&mut db_conn)
            .map_err(Error::external)?
            .get_underlying_conn_mut();

        if self.schema_needs_ensure() {
            ensure_kafka_tables(duckdb_conn)?;
            self.mark_schema_ensured();
        }

        // Diagnostic-only: surface a warn log when an offset regresses.
        if let Ok(prior) = load_offsets(duckdb_conn, &self.dataset_name) {
            let _ = offsets::merge_offsets(&self.dataset_name, prior, offsets);
        }

        let tx = duckdb_conn.transaction().map_err(Error::external)?;
        upsert_offsets_tx(&tx, &self.dataset_name, offsets)?;
        tx.commit().map_err(Error::external)?;
        Ok(())
    }
}

fn ensure_kafka_tables(conn: &mut duckdb::Connection) -> Result<()> {
    let create_metadata = format!(
        "CREATE TABLE IF NOT EXISTS {KAFKA_TABLE_NAME} (
            dataset_name TEXT PRIMARY KEY,
            consumer_group_id TEXT,
            topic TEXT,
            schema_json TEXT,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        )"
    );
    conn.execute(&create_metadata, [])
        .map_err(Error::external)?;

    let create_offsets = format!(
        "CREATE TABLE IF NOT EXISTS {KAFKA_OFFSETS_TABLE_NAME} (
            dataset_name TEXT NOT NULL,
            topic TEXT NOT NULL,
            partition_id INTEGER NOT NULL,
            partition_offset BIGINT NOT NULL,
            updated_at TIMESTAMP,
            PRIMARY KEY (dataset_name, topic, partition_id)
        )"
    );
    conn.execute(&create_offsets, []).map_err(Error::external)?;
    Ok(())
}

fn upsert_offsets_tx(
    tx: &duckdb::Transaction<'_>,
    dataset_name: &str,
    offsets: &[KafkaOffset],
) -> Result<()> {
    if offsets.is_empty() {
        return Ok(());
    }
    let stmt_sql = format!(
        "INSERT INTO {KAFKA_OFFSETS_TABLE_NAME}
            (dataset_name, topic, partition_id, partition_offset, updated_at)
         VALUES (?, ?, ?, ?, now())
         ON CONFLICT (dataset_name, topic, partition_id) DO UPDATE SET
            partition_offset = GREATEST(excluded.partition_offset, partition_offset),
            updated_at = now()"
    );
    let mut stmt = tx.prepare(&stmt_sql).map_err(Error::external)?;
    for offset in offsets {
        stmt.execute(duckdb::params![
            dataset_name,
            offset.topic,
            offset.partition,
            offset.offset,
        ])
        .map_err(Error::external)?;
    }
    Ok(())
}

fn load_offsets(conn: &duckdb::Connection, dataset_name: &str) -> Result<Vec<KafkaOffset>> {
    let query = format!(
        "SELECT topic, partition_id, partition_offset FROM {KAFKA_OFFSETS_TABLE_NAME} WHERE dataset_name = ?"
    );
    let mut stmt = conn.prepare(&query).map_err(Error::external)?;
    let rows = stmt
        .query_map([dataset_name], |row| {
            Ok(KafkaOffset {
                topic: row.get(0)?,
                partition: row.get(1)?,
                offset: row.get(2)?,
            })
        })
        .map_err(Error::external)?;
    let mut out: Vec<KafkaOffset> = rows
        .collect::<duckdb::Result<_>>()
        .map_err(Error::external)?;
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
        let db_path = temp_dir.path().join("kafka_test.db");

        dataset.acceleration = Some(Acceleration {
            engine: Engine::DuckDB,
            mode: Mode::File,
            params: [(
                "duckdb_file".to_string(),
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
    async fn test_duckdb_roundtrip() {
        let (ds, _temp_dir) = create_test_dataset("test_duckdb_roundtrip").await;
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
    async fn test_duckdb_metadata_overwrite() {
        let (ds, _temp_dir) = create_test_dataset("test_duckdb_metadata_overwrite").await;
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
    async fn test_duckdb_offsets_update() {
        let (ds, _temp_dir) = create_test_dataset("test_duckdb_offsets_update").await;
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
    async fn test_duckdb_get_nonexistent() {
        let (ds, _temp_dir) = create_test_dataset("test_duckdb_get_nonexistent").await;
        let kafka_sys = KafkaSys::try_new(&ds, OpenOption::CreateIfNotExists)
            .await
            .expect("to create KafkaSys");

        let result = kafka_sys.get().await;
        assert!(
            result.expect("to get empty metadata").is_none(),
            "Should return None for nonexistent dataset"
        );
    }

    /// Regression for finding #2: `upsert_offsets` must succeed even when no
    /// metadata row exists.
    #[tokio::test]
    async fn test_duckdb_offsets_update_succeeds_without_metadata_row() {
        let (ds, _temp_dir) = create_test_dataset("test_duckdb_offsets_no_metadata").await;
        let kafka_sys = KafkaSys::try_new(&ds, OpenOption::CreateIfNotExists)
            .await
            .expect("to create KafkaSys");

        kafka_sys
            .upsert_offsets(&[KafkaOffset {
                topic: "test-topic".to_string(),
                partition: 0,
                offset: 42,
            }])
            .await
            .expect("upsert_offsets should succeed without metadata row");
    }

    /// Regression for finding #1 part b: per-partition INSERT ON CONFLICT
    /// keeps the highest offset when racing concurrent writers on the same
    /// partition.
    #[tokio::test]
    async fn test_duckdb_concurrent_same_partition_keeps_max() {
        let (ds, _temp_dir) = create_test_dataset("test_duckdb_concurrent_same_partition").await;
        let kafka_sys = Arc::new(
            KafkaSys::try_new(&ds, OpenOption::CreateIfNotExists)
                .await
                .expect("to create KafkaSys"),
        );
        kafka_sys
            .upsert(&create_test_metadata())
            .await
            .expect("seed");

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
            h.await.expect("join");
        }

        let retrieved = kafka_sys.get().await.expect("retrieve").expect("exist");
        let p0 = retrieved
            .offsets
            .iter()
            .find(|o| o.partition == 0 && o.topic == "test-topic")
            .expect("partition 0 present");
        assert_eq!(p0.offset, 100);
    }
}
