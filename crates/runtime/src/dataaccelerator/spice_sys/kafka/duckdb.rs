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

use super::{Error, KAFKA_TABLE_NAME, KafkaMetadata, KafkaSys, Result};
use data_components::kafka::KafkaOffset;
use datafusion_table_providers::sql::db_connection_pool::duckdbpool::DuckDbConnectionPool;
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

        ensure_kafka_table(duckdb_conn)?;

        let schema_json = Self::serialize_schema(&metadata.schema)?;
        let offsets_json = Self::serialize_offsets(&metadata.offsets)?;

        let upsert = format!(
            "INSERT INTO {KAFKA_TABLE_NAME} (dataset_name, consumer_group_id, topic, schema_json, offsets_json, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, now(), now())
             ON CONFLICT (dataset_name) DO UPDATE SET
                consumer_group_id = excluded.consumer_group_id,
                topic = excluded.topic,
                schema_json = excluded.schema_json,
                offsets_json = excluded.offsets_json,
                updated_at = now()"
        );

        duckdb_conn
            .execute(
                &upsert,
                [
                    &self.dataset_name,
                    &metadata.consumer_group_id,
                    &metadata.topic,
                    &schema_json,
                    &offsets_json,
                ],
            )
            .map_err(Error::external)?;

        Ok(())
    }

    pub(super) fn get_duckdb(&self, pool: &Arc<DuckDbConnectionPool>) -> Option<KafkaMetadata> {
        let mut db_conn = Arc::clone(pool).connect_sync().ok()?;
        let duckdb_conn = datafusion_table_providers::duckdb::DuckDB::duckdb_conn(&mut db_conn)
            .ok()?
            .get_underlying_conn_mut();

        ensure_kafka_table(duckdb_conn).ok()?;

        let query = format!(
            "SELECT consumer_group_id, topic, schema_json, offsets_json FROM {KAFKA_TABLE_NAME} WHERE dataset_name = ?"
        );
        let mut stmt = duckdb_conn.prepare(&query).ok()?;
        let mut rows = stmt.query([&self.dataset_name]).ok()?;

        if let Some(row) = rows.next().ok()? {
            let consumer_group_id: String = row.get(0).ok()?;
            let topic: String = row.get(1).ok()?;
            let schema_json: String = row.get(2).ok()?;
            let offsets_json: Option<String> = row.get(3).ok()?;

            Some(KafkaMetadata {
                consumer_group_id,
                topic,
                schema: KafkaSys::deserialize_schema(&schema_json).ok()?,
                offsets: KafkaSys::deserialize_offsets(offsets_json.as_deref()).ok()?,
            })
        } else {
            None
        }
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

        ensure_kafka_table(duckdb_conn)?;

        let offsets_json = Self::serialize_offsets(offsets)?;
        let update = format!(
            "UPDATE {KAFKA_TABLE_NAME} SET offsets_json = ?, updated_at = now() WHERE dataset_name = ?"
        );
        let changed = duckdb_conn
            .execute(&update, [&offsets_json, &self.dataset_name])
            .map_err(Error::external)?;

        if changed == 0 {
            return Err(Error::external(format!(
                "Kafka sidecar metadata for dataset {} does not exist",
                self.dataset_name
            )));
        }

        Ok(())
    }
}

fn ensure_kafka_table(conn: &mut duckdb::Connection) -> Result<()> {
    let create_table = format!(
        "CREATE TABLE IF NOT EXISTS {KAFKA_TABLE_NAME} (
            dataset_name TEXT PRIMARY KEY,
            consumer_group_id TEXT,
            topic TEXT,
            schema_json TEXT,
            offsets_json TEXT,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        )"
    );
    conn.execute(&create_table, []).map_err(Error::external)?;

    let add_offsets =
        format!("ALTER TABLE {KAFKA_TABLE_NAME} ADD COLUMN IF NOT EXISTS offsets_json TEXT");
    conn.execute(&add_offsets, []).map_err(Error::external)?;

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

        // Use a unique temp directory for each test to avoid parallel test interference
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
        let retrieved = kafka_sys.get().await.expect("to retrieve metadata");

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

        let retrieved = kafka_sys.get().await.expect("to retrieve metadata");
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

        let retrieved = kafka_sys.get().await.expect("to retrieve metadata");
        assert_eq!(retrieved.offsets, offsets);
    }

    #[tokio::test]
    async fn test_duckdb_get_nonexistent() {
        let (ds, _temp_dir) = create_test_dataset("test_duckdb_get_nonexistent").await;
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
