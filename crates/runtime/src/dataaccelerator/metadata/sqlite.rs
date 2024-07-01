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

use super::{METADATA_DATASET_COLUMN, METADATA_METADATA_COLUMN, METADATA_TABLE_NAME};
use crate::{
    component::dataset::{acceleration::Engine, Dataset},
    dataaccelerator::{get_accelerator_engine, sqlite::SqliteAccelerator},
};
use std::path::Path;
use tokio_rusqlite::Connection;

use super::AcceleratedMetadataProvider;

pub struct AcceleratedMetadataSqlite {
    conn: Connection,
}

impl AcceleratedMetadataSqlite {
    pub async fn try_new(
        dataset: &Dataset,
        create_if_file_not_exists: bool,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let accelerator = get_accelerator_engine(Engine::Sqlite)
            .await
            .ok_or("Sqlite accelerator engine not available")?;
        let sqlite_accelerator = accelerator
            .as_any()
            .downcast_ref::<SqliteAccelerator>()
            .ok_or("Accelerator is not a SqliteAccelerator")?;

        let sqlite_file = sqlite_accelerator
            .sqlite_file_path(dataset)
            .ok_or("Acceleration mode is not file-based.")?;
        if !create_if_file_not_exists && !Path::new(&sqlite_file).exists() {
            return Err("Sqlite file does not exist.".into());
        }

        let conn = Connection::open(sqlite_file).await.map_err(Box::new)?;

        Ok(Self { conn })
    }
}

#[async_trait::async_trait]
impl AcceleratedMetadataProvider for AcceleratedMetadataSqlite {
    async fn get_metadata(&self, dataset: &str) -> Option<String> {
        let dataset = dataset.to_string();
        self.conn.call(move |conn| {
            let query = format!(
                "SELECT {METADATA_METADATA_COLUMN} FROM {METADATA_TABLE_NAME} WHERE {METADATA_DATASET_COLUMN} = ?",
            );
            let mut stmt = conn.prepare(&query)?;

            let mut rows = stmt.query([dataset])?;

            let metadata: Option<String> = if let Some(row) = rows.next()? {
                Some(row.get(0)?)
            } else {
                None
            };

            Ok(metadata)
        }).await.ok().flatten()
    }

    async fn set_metadata(
        &self,
        dataset: &str,
        metadata: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let dataset = dataset.to_string();
        let metadata = metadata.to_string();

        self.conn.call(move |conn| {
            let create_if_not_exists = format!(
                "CREATE TABLE IF NOT EXISTS {METADATA_TABLE_NAME} (
                    {METADATA_DATASET_COLUMN} TEXT PRIMARY KEY,
                    {METADATA_METADATA_COLUMN} TEXT
                );",
            );

            conn.execute(&create_if_not_exists, [])?;

            let query = format!(
                "INSERT INTO {METADATA_TABLE_NAME} ({METADATA_DATASET_COLUMN}, {METADATA_METADATA_COLUMN}) VALUES (?1, ?2) ON CONFLICT ({METADATA_DATASET_COLUMN}) DO UPDATE SET {METADATA_METADATA_COLUMN} = ?2",
            );

            conn.execute(&query, [dataset, metadata])?;

            Ok(())
        }).await.map_err(Into::into)
    }
}
