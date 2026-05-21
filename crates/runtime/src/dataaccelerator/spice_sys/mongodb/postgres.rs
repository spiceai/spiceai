/*
Copyright 2026 The Spice.ai OSS Authors

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

use super::{Error, MONGODB_TABLE_NAME, MongoCheckpointMetadata, MongoSys, Result};

impl MongoSys {
    pub(super) async fn upsert_postgres(
        &self,
        pool: &PostgresConnectionPool,
        metadata: &MongoCheckpointMetadata,
    ) -> Result<()> {
        let conn = pool.connect_direct().await.map_err(Error::external)?;

        let create_table = format!(
            "CREATE TABLE IF NOT EXISTS {MONGODB_TABLE_NAME} (
                dataset_name TEXT PRIMARY KEY,
                resume_token_json TEXT NOT NULL,
                cluster_time_ts BIGINT,
                schema_json TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )"
        );
        conn.conn
            .execute(&create_table, &[])
            .await
            .map_err(Error::external)?;

        let upsert = format!(
            "INSERT INTO {MONGODB_TABLE_NAME}
             (dataset_name, resume_token_json, cluster_time_ts, schema_json, updated_at)
             VALUES ($1, $2, $3, $4, CURRENT_TIMESTAMP)
             ON CONFLICT (dataset_name) DO UPDATE SET
                resume_token_json = EXCLUDED.resume_token_json,
                cluster_time_ts = EXCLUDED.cluster_time_ts,
                schema_json = EXCLUDED.schema_json,
                updated_at = CURRENT_TIMESTAMP"
        );

        conn.conn
            .execute(
                &upsert,
                &[
                    &self.dataset_name,
                    &metadata.resume_token_json,
                    &metadata.cluster_time_ts,
                    &metadata.schema_json,
                ],
            )
            .await
            .map_err(Error::external)?;

        Ok(())
    }

    pub(super) async fn get_postgres(
        &self,
        pool: &PostgresConnectionPool,
    ) -> Option<MongoCheckpointMetadata> {
        let conn = pool.connect_direct().await.ok()?;
        let query = format!(
            "SELECT resume_token_json, cluster_time_ts, schema_json, EXTRACT(EPOCH FROM updated_at) FROM {MONGODB_TABLE_NAME} WHERE dataset_name = $1"
        );
        let stmt = conn.conn.prepare(&query).await.ok()?;
        let row = conn
            .conn
            .query_opt(&stmt, &[&self.dataset_name])
            .await
            .ok()??;

        let resume_token_json: String = row.get(0);
        let cluster_time_ts: Option<i64> = row.get(1);
        let schema_json: Option<String> = row.get(2);
        let updated_at_epoch: Option<f64> = row.get(3);
        let updated_at = updated_at_epoch.and_then(|epoch| {
            std::time::UNIX_EPOCH.checked_add(std::time::Duration::from_secs_f64(epoch))
        });

        Some(MongoCheckpointMetadata {
            resume_token_json,
            cluster_time_ts,
            schema_json,
            updated_at,
        })
    }

    pub(super) async fn delete_postgres(&self, pool: &PostgresConnectionPool) -> Result<()> {
        let conn = pool.connect_direct().await.map_err(Error::external)?;
        let delete = format!("DELETE FROM {MONGODB_TABLE_NAME} WHERE dataset_name = $1");
        conn.conn
            .execute(&delete, &[&self.dataset_name])
            .await
            .map_err(Error::external)?;

        Ok(())
    }
}
