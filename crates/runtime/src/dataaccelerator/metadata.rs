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
//! The metadata table will be a new table `spice_sys_metadata` with three columns:
//! - `dataset` (PRIMARY KEY, TEXT): The dataset the metadata entry corresponds to
//! - `key` (PRIMARY KEY, TEXT): The key of the metadata entry, allowing multiple metadata values to be associated with a dataset
//! - `metadata` (TEXT): The metadata entry in JSON/string serialized format

use serde::de::DeserializeOwned;

use crate::component::dataset::{acceleration::Engine, Dataset};

pub const METADATA_TABLE_NAME: &str = "spice_sys_metadata";
pub const METADATA_DATASET_COLUMN: &str = "dataset";
pub const METADATA_METADATA_COLUMN: &str = "metadata";
pub const METADATA_KEY_COLUMN: &str = "key";

// Well-known keys
pub const DEBEZIUM_KAFKA_METADATA_KEY: &str = "debezium_kafka_metadata";
pub const INITIAL_REFRESH_COMPLETE_KEY: &str = "initial_refresh_complete";

#[cfg(feature = "duckdb")]
mod duckdb;
#[cfg(feature = "postgres")]
mod postgres;
#[cfg(feature = "sqlite")]
mod sqlite;

pub struct AcceleratedMetadata {
    metadata_provider: Box<dyn AcceleratedMetadataProvider + Send + Sync>,
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
    pub async fn get_metadata<T: DeserializeOwned>(&self, key: &str) -> Option<T> {
        self.metadata_provider
            .get_metadata(&self.dataset_name, key)
            .await
            .map(|metadata| serde_json::from_str(&metadata))?
            .ok()
    }

    pub async fn set_metadata<T: serde::Serialize>(
        &self,
        key: &str,
        metadata: &T,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let metadata = serde_json::to_string(metadata).map_err(|e| e.to_string())?;
        self.metadata_provider
            .set_metadata(&self.dataset_name, key, &metadata)
            .await
    }
}

#[async_trait::async_trait]
pub trait AcceleratedMetadataProvider {
    async fn get_metadata(&self, dataset: &str, key: &str) -> Option<String>;
    async fn set_metadata(
        &self,
        dataset: &str,
        key: &str,
        metadata: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}

async fn get_metadata_provider(
    dataset: &Dataset,
    create_if_file_not_exists: bool,
) -> Result<
    Box<dyn AcceleratedMetadataProvider + Send + Sync>,
    Box<dyn std::error::Error + Send + Sync>,
> {
    let acceleration = dataset
        .acceleration
        .as_ref()
        .ok_or("Dataset acceleration not enabled")?;
    match acceleration.engine {
        #[cfg(feature = "duckdb")]
        Engine::DuckDB => Ok(Box::new(
            crate::dataaccelerator::metadata::duckdb::AcceleratedMetadataDuckDB::try_new(
                dataset,
                create_if_file_not_exists,
            )
            .await?,
        )),
        #[cfg(not(feature = "duckdb"))]
        Engine::DuckDB => Err("Spice wasn't built with DuckDB support enabled".into()),
        #[cfg(feature = "sqlite")]
        Engine::Sqlite => Ok(Box::new(
            crate::dataaccelerator::metadata::sqlite::AcceleratedMetadataSqlite::try_new(
                dataset,
                create_if_file_not_exists,
            )
            .await?,
        )),
        #[cfg(not(feature = "sqlite"))]
        Engine::Sqlite => Err("Spice wasn't built with Sqlite support enabled".into()),
        #[cfg(feature = "postgres")]
        Engine::PostgreSQL => Ok(Box::new(
            crate::dataaccelerator::metadata::postgres::AcceleratedMetadataPostgres::try_new(
                dataset,
            )
            .await?,
        )),
        #[cfg(not(feature = "postgres"))]
        Engine::PostgreSQL => Err("Spice wasn't built with PostgreSQL support enabled".into()),
        Engine::Arrow => Err("Arrow acceleration not supported for metadata".into()),
    }
}
