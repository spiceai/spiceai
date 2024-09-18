/*
Copyright 2024 The Spice.ai OSS Authors

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

use datafusion_table_providers::sql::db_connection_pool::postgrespool::PostgresConnectionPool;

use super::{DatasetCheckpoint, Result, CHECKPOINT_TABLE_NAME};

impl DatasetCheckpoint {
    pub(super) async fn init_postgres(pool: &PostgresConnectionPool) -> Result<()> {
        let conn = pool.connect_direct().await.map_err(|e| e.to_string())?;

        let create_table = format!(
            "CREATE TABLE IF NOT EXISTS {CHECKPOINT_TABLE_NAME} (
                dataset_name TEXT PRIMARY KEY,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )"
        );
        conn.conn
            .execute(&create_table, &[])
            .await
            .map_err(|e| e.to_string())?;

        Ok(())
    }

    pub(super) async fn exists_postgres(&self, pool: &PostgresConnectionPool) -> Result<bool> {
        let conn = pool.connect_direct().await.map_err(|e| e.to_string())?;
        let query =
            format!("SELECT 1 FROM {CHECKPOINT_TABLE_NAME} WHERE dataset_name = $1 LIMIT 1");
        let stmt = conn.conn.prepare(&query).await.map_err(|e| e.to_string())?;
        let row = conn
            .conn
            .query_opt(&stmt, &[&self.dataset_name])
            .await
            .map_err(|e| e.to_string())?;
        Ok(row.is_some())
    }

    pub(super) async fn checkpoint_postgres(&self, pool: &PostgresConnectionPool) -> Result<()> {
        let conn = pool.connect_direct().await.map_err(|e| e.to_string())?;

        let upsert = format!(
            "INSERT INTO {CHECKPOINT_TABLE_NAME} (dataset_name, updated_at)
             VALUES ($1, CURRENT_TIMESTAMP)
             ON CONFLICT (dataset_name) DO UPDATE SET updated_at = CURRENT_TIMESTAMP"
        );
        conn.conn
            .execute(&upsert, &[&self.dataset_name])
            .await
            .map_err(|e| e.to_string())?;

        Ok(())
    }
}
