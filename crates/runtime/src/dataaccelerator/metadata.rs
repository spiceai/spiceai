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

//! Store and retrieve Spice metadata in durable accelerator tables
//!
//! The metadata table will be a new table `__spice_metadata` with two columns:
//! - `dataset` (PRIMARY KEY, TEXT): The dataset the metadata entry corresponds to
//! - `metadata` (TEXT): The metadata entry in JSON format

use std::{path::Path, sync::Arc};

use arrow::datatypes::SchemaRef;
use data_components::duckdb::DuckDB;
use duckdb::AccessMode;
use serde::de::DeserializeOwned;

use crate::component::dataset::{acceleration::Engine, Dataset};
use crate::dataaccelerator::DuckDBAccelerator;
use db_connection_pool::{dbconnection::SyncDbConnection, duckdbpool::DuckDbConnectionPool};

use super::get_accelerator_engine;

const METADATA_TABLE_NAME: &str = "spice_sys_metadata";
const METADATA_DATASET_COLUMN: &str = "dataset";
const METADATA_METADATA_COLUMN: &str = "metadata";

pub struct AcceleratedMetadata {
    metadata_provider: Box<dyn AcceleratedMetadataProvider>,
    dataset_name: String,
}

impl AcceleratedMetadata {
    pub async fn new(dataset: &Dataset) -> Option<Self> {
        let metadata_provider = get_metadata_provider(dataset, false).await.ok()?;

        Some(Self {
            metadata_provider,
            dataset_name: dataset.name.to_string(),
        })
    }

    pub async fn new_create_if_not_exists(
        dataset: &Dataset,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let metadata_provider = get_metadata_provider(dataset, true).await?;

        Ok(Self {
            metadata_provider,
            dataset_name: dataset.name.to_string(),
        })
    }

    #[must_use]
    pub fn get_metadata<T: DeserializeOwned>(&self) -> Option<T> {
        self.metadata_provider
            .get_metadata(&self.dataset_name)
            .map(|metadata| serde_json::from_str(&metadata))?
            .ok()
    }

    pub fn set_metadata<T: serde::Serialize>(
        &self,
        metadata: &T,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let metadata = serde_json::to_string(metadata).map_err(|e| e.to_string())?;
        self.metadata_provider
            .set_metadata(&self.dataset_name, &metadata)
    }

    pub fn get_schema(&self) -> Result<SchemaRef, Box<dyn std::error::Error + Send + Sync>> {
        self.metadata_provider.get_schema(&self.dataset_name)
    }
}

pub trait AcceleratedMetadataProvider {
    fn get_metadata(&self, dataset: &str) -> Option<String>;
    fn set_metadata(
        &self,
        dataset: &str,
        metadata: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
    fn get_schema(
        &self,
        dataset: &str,
    ) -> Result<SchemaRef, Box<dyn std::error::Error + Send + Sync>>;
}

async fn get_metadata_provider(
    dataset: &Dataset,
    create_if_file_not_exists: bool,
) -> Result<Box<dyn AcceleratedMetadataProvider>, Box<dyn std::error::Error + Send + Sync>> {
    let acceleration = dataset
        .acceleration
        .as_ref()
        .ok_or("Dataset acceleration not enabled")?;
    match acceleration.engine {
        Engine::DuckDB => Ok(Box::new(
            AcceleratedMetadataDuckDB::try_new(dataset, create_if_file_not_exists).await?,
        )),
        Engine::Sqlite => todo!(),
        Engine::PostgreSQL => todo!(),
        Engine::Arrow => Err("Arrow acceleration not supported for metadata".into()),
    }
}

pub struct AcceleratedMetadataDuckDB {
    pool: Arc<DuckDbConnectionPool>,
}

impl AcceleratedMetadataDuckDB {
    async fn try_new(
        dataset: &Dataset,
        create_if_file_not_exists: bool,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let accelerator = get_accelerator_engine(Engine::DuckDB)
            .await
            .ok_or("DuckDB accelerator engine not available")?;
        let duckdb_accelerator = accelerator
            .as_any()
            .downcast_ref::<DuckDBAccelerator>()
            .ok_or("Accelerator is not a DuckDBAccelerator")?;

        let duckdb_file = duckdb_accelerator
            .duckdb_file_path(dataset)
            .ok_or("Acceleration mode is not file-based.")?;
        if !create_if_file_not_exists && !Path::new(&duckdb_file).exists() {
            return Err("DuckDB file does not exist.".into());
        }

        let pool = DuckDbConnectionPool::new_file(&duckdb_file, &AccessMode::ReadWrite)
            .map_err(|e| e.to_string())?;

        Ok(Self {
            pool: Arc::new(pool),
        })
    }
}

impl AcceleratedMetadataProvider for AcceleratedMetadataDuckDB {
    fn get_metadata(&self, dataset: &str) -> Option<String> {
        let mut db_conn = Arc::clone(&self.pool).connect_sync().ok()?;
        let duckdb_conn = DuckDB::duckdb_conn(&mut db_conn)
            .ok()?
            .get_underlying_conn_mut();
        let query = format!(
            "SELECT {METADATA_METADATA_COLUMN} FROM {METADATA_TABLE_NAME} WHERE {METADATA_DATASET_COLUMN} = ?",
        );
        let mut stmt = duckdb_conn.prepare(&query).ok()?;
        let mut rows = stmt.query([dataset]).ok()?;

        let metadata: Option<String> = if let Some(row) = rows.next().ok()? {
            Some(row.get(0).ok()?)
        } else {
            None
        };

        metadata
    }

    fn set_metadata(
        &self,
        dataset: &str,
        metadata: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut db_conn = Arc::clone(&self.pool)
            .connect_sync()
            .map_err(|e| e.to_string())?;
        let duckdb_conn = DuckDB::duckdb_conn(&mut db_conn)
            .map_err(|e| e.to_string())?
            .get_underlying_conn_mut();

        let create_if_not_exists = format!(
            "CREATE TABLE IF NOT EXISTS {METADATA_TABLE_NAME} (
                {METADATA_DATASET_COLUMN} TEXT PRIMARY KEY,
                {METADATA_METADATA_COLUMN} TEXT
            );",
        );

        duckdb_conn
            .execute(&create_if_not_exists, [])
            .map_err(|e| e.to_string())?;

        let query = format!(
            "INSERT INTO {METADATA_TABLE_NAME} ({METADATA_DATASET_COLUMN}, {METADATA_METADATA_COLUMN}) VALUES (?, ?) ON CONFLICT ({METADATA_DATASET_COLUMN}) DO UPDATE SET {METADATA_METADATA_COLUMN} = ?",
        );

        duckdb_conn
            .execute(&query, [dataset, metadata, metadata])
            .map_err(|e| e.to_string())?;

        Ok(())
    }

    fn get_schema(
        &self,
        dataset: &str,
    ) -> Result<SchemaRef, Box<dyn std::error::Error + Send + Sync>> {
        let mut db_conn = Arc::clone(&self.pool)
            .connect_sync()
            .map_err(|e| e.to_string())?;
        let duckdb_conn = DuckDB::duckdb_conn(&mut db_conn).map_err(|e| e.to_string())?;

        let query = format!(r#"SELECT * FROM "{dataset}" LIMIT 0"#);

        let stream = duckdb_conn
            .query_arrow(&query, &[])
            .map_err(|e| e.to_string())?;

        Ok(stream.schema())
    }
}

pub struct AcceleratedMetadataSqlite {}

pub struct AcceleratedMetadataPostgres {}
