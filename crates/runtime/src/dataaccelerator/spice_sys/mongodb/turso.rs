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

use std::sync::Arc;

use super::{Error, MONGODB_TABLE_NAME, MongoCheckpointMetadata, MongoSys, Result};
use crate::dataaccelerator::turso::TursoConnectionPool;

impl MongoSys {
    pub(super) async fn upsert_turso(
        &self,
        pool: &Arc<TursoConnectionPool>,
        metadata: &MongoCheckpointMetadata,
    ) -> Result<()> {
        let dataset_name = self.dataset_name.clone();
        let resume_token_json = metadata.resume_token_json.clone();
        let cluster_time_ts = metadata.cluster_time_ts;
        let schema_json = metadata.schema_json.clone();

        let conn = pool.connect().await.map_err(Error::external)?;

        let create_table = format!(
            "CREATE TABLE IF NOT EXISTS {MONGODB_TABLE_NAME} (
                dataset_name TEXT PRIMARY KEY,
                resume_token_json TEXT NOT NULL,
                cluster_time_ts INTEGER,
                schema_json TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )"
        );
        conn.execute(&create_table, ())
            .await
            .map_err(Error::external)?;

        let upsert = format!(
            "INSERT INTO {MONGODB_TABLE_NAME}
             (dataset_name, resume_token_json, cluster_time_ts, schema_json, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
             ON CONFLICT (dataset_name) DO UPDATE SET
                resume_token_json = ?2,
                cluster_time_ts = ?3,
                schema_json = ?4,
                updated_at = CURRENT_TIMESTAMP"
        );

        conn.execute(
            &upsert,
            turso::params![
                dataset_name,
                resume_token_json,
                cluster_time_ts,
                schema_json,
            ],
        )
        .await
        .map_err(Error::external)?;

        Ok(())
    }

    pub(super) async fn get_turso(
        &self,
        pool: &Arc<TursoConnectionPool>,
    ) -> Option<MongoCheckpointMetadata> {
        let dataset_name = self.dataset_name.clone();
        let conn = pool.connect().await.ok()?;
        let query = format!(
            "SELECT resume_token_json, cluster_time_ts, schema_json, strftime('%s', updated_at) FROM {MONGODB_TABLE_NAME} WHERE dataset_name = ?"
        );

        let mut rows = conn
            .query(&query, turso::params![dataset_name])
            .await
            .ok()?;
        let row = rows.next().await.ok()??;

        let resume_token_json = row.get::<String>(0).ok()?;
        let cluster_time_ts: Option<i64> = row.get::<i64>(1).ok();
        let schema_json: Option<String> = row.get::<String>(2).ok();
        let updated_at_epoch: Option<i64> = row.get::<i64>(3).ok();
        let updated_at = updated_at_epoch.and_then(|epoch| {
            u64::try_from(epoch)
                .ok()
                .and_then(|e| std::time::UNIX_EPOCH.checked_add(std::time::Duration::from_secs(e)))
        });

        Some(MongoCheckpointMetadata {
            resume_token_json,
            cluster_time_ts,
            schema_json,
            updated_at,
        })
    }

    pub(super) async fn delete_turso(&self, pool: &Arc<TursoConnectionPool>) -> Result<()> {
        let dataset_name = self.dataset_name.clone();
        let conn = pool.connect().await.map_err(Error::external)?;
        let delete = format!("DELETE FROM {MONGODB_TABLE_NAME} WHERE dataset_name = ?1");
        conn.execute(&delete, turso::params![dataset_name])
            .await
            .map_err(Error::external)?;

        Ok(())
    }
}
