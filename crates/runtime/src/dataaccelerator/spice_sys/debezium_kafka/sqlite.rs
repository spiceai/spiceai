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

use super::{DebeziumKafkaMetadata, DebeziumKafkaSys, Result, DEBEZIUM_KAFKA_TABLE_NAME};
use data_components::debezium::change_event;
use datafusion_table_providers::sql::db_connection_pool::{
    dbconnection::sqliteconn::SqliteConnection, sqlitepool::SqliteConnectionPool,
};

impl DebeziumKafkaSys {
    pub(super) async fn upsert_sqlite(
        &self,
        pool: &SqliteConnectionPool,
        metadata: &DebeziumKafkaMetadata,
    ) -> Result<()> {
        let conn_sync = pool.connect_sync();
        let Some(conn) = conn_sync.as_any().downcast_ref::<SqliteConnection>() else {
            return Err("Failed to downcast to SqliteConnection".into());
        };
        let dataset_name = self.dataset_name.clone();
        let consumer_group_id = metadata.consumer_group_id.clone();
        let topic = metadata.topic.clone();
        let primary_keys =
            serde_json::to_string(&metadata.primary_keys).map_err(|e| e.to_string())?;
        let schema_fields =
            serde_json::to_string(&metadata.schema_fields).map_err(|e| e.to_string())?;

        conn.conn
            .call(move |conn| {
                let create_table = format!(
                    "CREATE TABLE IF NOT EXISTS {DEBEZIUM_KAFKA_TABLE_NAME} (
                    dataset_name TEXT PRIMARY KEY,
                    consumer_group_id TEXT,
                    topic TEXT,
                    primary_keys TEXT,
                    schema_fields TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )"
                );
                conn.execute(&create_table, [])?;

                let upsert = format!(
                    "INSERT INTO {DEBEZIUM_KAFKA_TABLE_NAME}
                 (dataset_name, consumer_group_id, topic, primary_keys, schema_fields, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, CURRENT_TIMESTAMP)
                 ON CONFLICT (dataset_name) DO UPDATE SET
                    consumer_group_id = ?2,
                    topic = ?3,
                    primary_keys = ?4,
                    schema_fields = ?5,
                    updated_at = CURRENT_TIMESTAMP"
                );

                conn.execute(
                    &upsert,
                    [
                        dataset_name,
                        consumer_group_id,
                        topic,
                        primary_keys,
                        schema_fields,
                    ],
                )?;

                Ok(())
            })
            .await
            .map_err(|e| e.to_string().into())
    }

    pub(super) async fn get_sqlite(
        &self,
        pool: &SqliteConnectionPool,
    ) -> Option<DebeziumKafkaMetadata> {
        let dataset_name = self.dataset_name.clone();
        let conn_sync = pool.connect_sync();
        let conn = conn_sync.as_any().downcast_ref::<SqliteConnection>()?;
        conn.conn
            .call(move |conn| {
            let query = format!(
                "SELECT consumer_group_id, topic, primary_keys, schema_fields FROM {DEBEZIUM_KAFKA_TABLE_NAME} WHERE dataset_name = ?"
            );
            let mut stmt = conn.prepare(&query)?;
            let mut rows = stmt.query([dataset_name])?;

            if let Some(row) = rows.next()? {
                let consumer_group_id: String = row.get(0)?;
                let topic: String = row.get(1)?;
                let primary_keys: String = row.get(2)?;
                let schema_fields: String = row.get(3)?;

                let primary_keys: Vec<String> = serde_json::from_str(&primary_keys).map_err(|e| tokio_rusqlite::Error::Other(Box::new(e)))?;
                let schema_fields: Vec<change_event::Field> = serde_json::from_str(&schema_fields).map_err(|e| tokio_rusqlite::Error::Other(Box::new(e)))?;

                Ok(DebeziumKafkaMetadata {
                    consumer_group_id,
                    topic,
                    primary_keys,
                    schema_fields,
                })
            } else {
                Err(tokio_rusqlite::Error::Other("No row found".into()))
            }
        })
        .await
        .ok()
    }
}
