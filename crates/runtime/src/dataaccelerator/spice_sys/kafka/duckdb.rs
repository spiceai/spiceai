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

        let create_table = format!(
            "CREATE TABLE IF NOT EXISTS {KAFKA_TABLE_NAME} (
                dataset_name TEXT PRIMARY KEY,
                consumer_group_id TEXT,
                topic TEXT,
                schema_json TEXT,
                created_at TIMESTAMP,
                updated_at TIMESTAMP
            )"
        );
        duckdb_conn
            .execute(&create_table, [])
            .map_err(Error::external)?;

        let schema_json = Self::serialize_schema(&metadata.schema)?;

        let upsert = format!(
            "INSERT INTO {KAFKA_TABLE_NAME} (dataset_name, consumer_group_id, topic, schema_json, created_at, updated_at)
             VALUES (?, ?, ?, ?, now(), now())
             ON CONFLICT (dataset_name) DO UPDATE SET
                consumer_group_id = excluded.consumer_group_id,
                topic = excluded.topic,
                schema_json = excluded.schema_json,
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

        let query = format!(
            "SELECT consumer_group_id, topic, schema_json FROM {KAFKA_TABLE_NAME} WHERE dataset_name = ?"
        );
        let mut stmt = duckdb_conn.prepare(&query).ok()?;
        let mut rows = stmt.query([&self.dataset_name]).ok()?;

        if let Some(row) = rows.next().ok()? {
            let consumer_group_id: String = row.get(0).ok()?;
            let topic: String = row.get(1).ok()?;
            let schema_json: String = row.get(2).ok()?;

            Some(KafkaMetadata {
                consumer_group_id,
                topic,
                schema: KafkaSys::deserialize_schema(&schema_json).ok()?,
            })
        } else {
            None
        }
    }
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

    async fn create_test_dataset(ds_name: &str) -> Dataset {
        let app = app::AppBuilder::new("test").build();
        let runtime = RuntimeBuilder::new().build().await;

        let mut dataset = DatasetBuilder::try_new("spice.ai".to_string(), ds_name)
            .expect("to create dataset builder")
            .with_app(Arc::new(app))
            .with_runtime(Arc::new(runtime))
            .build()
            .expect("to create dataset");

        dataset.acceleration = Some(Acceleration {
            engine: Engine::DuckDB,
            mode: Mode::File,
            params: [(
                "duckdb_file".to_string(),
                ".spice/data/kafka_duckdb_test.db".to_string(),
            )]
            .into_iter()
            .collect(),
            ..Default::default()
        });

        dataset
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
        }
    }

    #[tokio::test]
    async fn test_duckdb_roundtrip() {
        let ds = create_test_dataset("test_duckdb_roundtrip").await;
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
    }

    #[tokio::test]
    async fn test_duckdb_metadata_overwrite() {
        let ds = create_test_dataset("test_duckdb_metadata_overwrite").await;
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
    }

    #[tokio::test]
    async fn test_duckdb_get_nonexistent() {
        let ds = create_test_dataset("test_duckdb_get_nonexistent").await;
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
