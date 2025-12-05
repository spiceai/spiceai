/*
Copyright 2025 The Spice.ai OSS Authors

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

use super::{DYNAMODB_STREAMS_TABLE_NAME, DynamoDBCheckpointMetadata, DynamoDBSys, Error, Result};
use datafusion_table_providers::sql::db_connection_pool::{
    dbconnection::sqliteconn::SqliteConnection, sqlitepool::SqliteConnectionPool,
};

impl DynamoDBSys {
    pub(super) async fn upsert_sqlite(
        &self,
        pool: &SqliteConnectionPool,
        metadata: &DynamoDBCheckpointMetadata,
    ) -> Result<()> {
        let dataset_name = self.dataset_name.clone();
        let checkpoint_data = metadata.checkpoint_data.clone();

        let conn_sync = pool.connect_sync();
        let Some(conn) = conn_sync.as_any().downcast_ref::<SqliteConnection>() else {
            return Err(Error::DowncastFailed {
                target: "SqliteConnection",
            });
        };

        conn.conn
            .call(
                move |conn: &mut rusqlite::Connection| -> Result<(), rusqlite::Error> {
                    let create_table = format!(
                        "CREATE TABLE IF NOT EXISTS {DYNAMODB_STREAMS_TABLE_NAME} (
                    dataset_name TEXT PRIMARY KEY,
                    checkpoint_data TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )"
                    );
                    conn.execute(&create_table, [])?;

                    let upsert = format!(
                        "INSERT INTO {DYNAMODB_STREAMS_TABLE_NAME}
                 (dataset_name, checkpoint_data, updated_at)
                 VALUES (?1, ?2, CURRENT_TIMESTAMP)
                 ON CONFLICT (dataset_name) DO UPDATE SET
                    checkpoint_data = ?2,
                    updated_at = CURRENT_TIMESTAMP"
                    );

                    conn.execute(&upsert, [dataset_name, checkpoint_data])?;

                    Ok::<(), rusqlite::Error>(())
                },
            )
            .await
            .map_err(Error::external)
    }

    pub(super) async fn get_sqlite(
        &self,
        pool: &SqliteConnectionPool,
    ) -> Option<DynamoDBCheckpointMetadata> {
        let dataset_name = self.dataset_name.clone();

        let conn_sync = pool.connect_sync();
        let conn = conn_sync.as_any().downcast_ref::<SqliteConnection>()?;

        conn.conn
            .call(move |conn: &mut rusqlite::Connection| -> Result<DynamoDBCheckpointMetadata, rusqlite::Error> {
                let query = format!(
                    "SELECT checkpoint_data FROM {DYNAMODB_STREAMS_TABLE_NAME} WHERE dataset_name = ?"
                );
                let mut stmt = conn.prepare(&query)?;
                let mut rows = stmt.query([dataset_name])?;

                if let Some(row) = rows.next()? {
                    let checkpoint_data: String = row.get(0)?;

                    Ok(DynamoDBCheckpointMetadata { checkpoint_data })
                } else {
                    Err(rusqlite::Error::QueryReturnedNoRows)
                }
            })
            .await
            .ok()
    }
}
