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

use std::sync::Arc;

use super::{DYNAMODB_STREAMS_TABLE_NAME, DynamoDBCheckpointMetadata, DynamoDBSys, Error, Result};
use crate::dataaccelerator::turso::TursoConnectionPool;

impl DynamoDBSys {
    pub(super) async fn upsert_turso(
        &self,
        pool: &Arc<TursoConnectionPool>,
        metadata: &DynamoDBCheckpointMetadata,
    ) -> Result<()> {
        let dataset_name = self.dataset_name.clone();
        let checkpoint_data = metadata.checkpoint_data.clone();

        let conn = pool.connect().await.map_err(Error::external)?;

        let create_table = format!(
            "CREATE TABLE IF NOT EXISTS {DYNAMODB_STREAMS_TABLE_NAME} (
                dataset_name TEXT PRIMARY KEY,
                checkpoint_data TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )"
        );
        conn.execute(&create_table, ())
            .await
            .map_err(Error::external)?;

        let upsert = format!(
            "INSERT INTO {DYNAMODB_STREAMS_TABLE_NAME}
             (dataset_name, checkpoint_data, updated_at)
             VALUES (?1, ?2, CURRENT_TIMESTAMP)
             ON CONFLICT (dataset_name) DO UPDATE SET
                checkpoint_data = ?2,
                updated_at = CURRENT_TIMESTAMP"
        );

        conn.execute(&upsert, turso::params![dataset_name, checkpoint_data])
            .await
            .map_err(Error::external)?;

        Ok(())
    }

    pub(super) async fn get_turso(
        &self,
        pool: &Arc<TursoConnectionPool>,
    ) -> Option<DynamoDBCheckpointMetadata> {
        let dataset_name = self.dataset_name.clone();
        let conn = pool.connect().await.ok()?;
        let query = format!(
            "SELECT checkpoint_data FROM {DYNAMODB_STREAMS_TABLE_NAME} WHERE dataset_name = ?"
        );

        let mut rows = conn
            .query(&query, turso::params![dataset_name])
            .await
            .ok()?;
        let row = rows.next().await.ok()??;

        let checkpoint_data = row.get::<String>(0).ok()?;

        Some(DynamoDBCheckpointMetadata { checkpoint_data })
    }
}
