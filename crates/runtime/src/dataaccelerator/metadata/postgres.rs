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

use std::sync::Arc;

use data_components::util::secrets::to_secret_map;
use db_connection_pool::postgrespool::PostgresConnectionPool;

use super::AcceleratedMetadataProvider;
use super::{METADATA_DATASET_COLUMN, METADATA_METADATA_COLUMN, METADATA_TABLE_NAME};
use crate::component::dataset::Dataset;

pub struct AcceleratedMetadataPostgres {
    pool: PostgresConnectionPool,
}

impl AcceleratedMetadataPostgres {
    pub async fn try_new(
        dataset: &Dataset,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let Some(acceleration) = &dataset.acceleration else {
            return Err("Dataset is not accelerated.".into());
        };

        let secret_map = Arc::new(to_secret_map(acceleration.params.clone()));

        let pool = PostgresConnectionPool::new(secret_map)
            .await
            .map_err(|e| e.to_string())?;

        Ok(Self { pool })
    }
}

#[async_trait::async_trait]
impl AcceleratedMetadataProvider for AcceleratedMetadataPostgres {
    async fn get_metadata(&self, dataset: &str) -> Option<String> {
        let query = format!(
            "SELECT {METADATA_METADATA_COLUMN} FROM {METADATA_TABLE_NAME} WHERE {METADATA_DATASET_COLUMN} = $1",
        );

        let conn = self.pool.connect_direct().await.ok()?;

        let stmt = conn.conn.prepare(&query).await.ok()?;
        let row = conn.conn.query_one(&stmt, &[&dataset]).await.ok()?;

        row.get(0)
    }

    async fn set_metadata(
        &self,
        dataset: &str,
        metadata: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let conn = self.pool.connect_direct().await?;
        let create_if_not_exists = format!(
            "CREATE TABLE IF NOT EXISTS {METADATA_TABLE_NAME} (
                {METADATA_DATASET_COLUMN} TEXT PRIMARY KEY,
                {METADATA_METADATA_COLUMN} TEXT
            );",
        );

        conn.conn.execute(&create_if_not_exists, &[]).await?;

        let query = format!(
            "INSERT INTO {METADATA_TABLE_NAME} ({METADATA_DATASET_COLUMN}, {METADATA_METADATA_COLUMN}) VALUES ($1, $2) ON CONFLICT ({METADATA_DATASET_COLUMN}) DO UPDATE SET {METADATA_METADATA_COLUMN} = $2",
        );

        conn.conn.execute(&query, &[&dataset, &metadata]).await?;

        Ok(())
    }
}
