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
use datafusion_table_providers::sql::db_connection_pool::postgrespool::PostgresConnectionPool;

impl DynamoDBSys {
    pub(super) async fn upsert_postgres(
        &self,
        pool: &PostgresConnectionPool,
        metadata: &DynamoDBCheckpointMetadata,
    ) -> Result<()> {
        let conn = pool.connect_direct().await.map_err(Error::external)?;

        let create_table = format!(
            "CREATE TABLE IF NOT EXISTS {DYNAMODB_STREAMS_TABLE_NAME} (
                dataset_name TEXT PRIMARY KEY,
                checkpoint_data TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )"
        );
        conn.conn
            .execute(&create_table, &[])
            .await
            .map_err(Error::external)?;

        let upsert = format!(
            "INSERT INTO {DYNAMODB_STREAMS_TABLE_NAME}
             (dataset_name, checkpoint_data, updated_at)
             VALUES ($1, $2, CURRENT_TIMESTAMP)
             ON CONFLICT (dataset_name) DO UPDATE SET
                checkpoint_data = EXCLUDED.checkpoint_data,
                updated_at = CURRENT_TIMESTAMP"
        );

        conn.conn
            .execute(&upsert, &[&self.dataset_name, &metadata.checkpoint_data])
            .await
            .map_err(Error::external)?;

        Ok(())
    }

    pub(super) async fn get_postgres(
        &self,
        pool: &PostgresConnectionPool,
    ) -> Option<DynamoDBCheckpointMetadata> {
        let conn = pool.connect_direct().await.ok()?;
        let query = format!(
            "SELECT checkpoint_data FROM {DYNAMODB_STREAMS_TABLE_NAME} WHERE dataset_name = $1"
        );
        let stmt = conn.conn.prepare(&query).await.ok()?;
        let row = conn
            .conn
            .query_opt(&stmt, &[&self.dataset_name])
            .await
            .ok()??;

        let checkpoint_data: String = row.get(0);

        Some(DynamoDBCheckpointMetadata { checkpoint_data })
    }
}
