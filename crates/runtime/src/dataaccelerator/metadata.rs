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
//! The metadata table will be a new table `spice_sys_metadata` with two columns:
//! - `dataset` (PRIMARY KEY, TEXT): The dataset the metadata entry corresponds to
//! - `metadata` (TEXT): The metadata entry in JSON format

use serde::de::DeserializeOwned;

use crate::component::dataset::{acceleration::Engine, Dataset};

pub const METADATA_TABLE_NAME: &str = "spice_sys_metadata";
pub const METADATA_DATASET_COLUMN: &str = "dataset";
pub const METADATA_METADATA_COLUMN: &str = "metadata";

#[cfg(feature = "duckdb")]
mod duckdb;
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
    pub async fn get_metadata<T: DeserializeOwned>(&self) -> Option<T> {
        self.metadata_provider
            .get_metadata(&self.dataset_name)
            .await
            .map(|metadata| serde_json::from_str(&metadata))?
            .ok()
    }

    pub async fn set_metadata<T: serde::Serialize>(
        &self,
        metadata: &T,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let metadata = serde_json::to_string(metadata).map_err(|e| e.to_string())?;
        self.metadata_provider
            .set_metadata(&self.dataset_name, &metadata)
            .await
    }
}

#[async_trait::async_trait]
pub trait AcceleratedMetadataProvider {
    async fn get_metadata(&self, dataset: &str) -> Option<String>;
    async fn set_metadata(
        &self,
        dataset: &str,
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
        Engine::DuckDB => Err("Spice wasn't build with DuckDB support enabled".into()),
        #[cfg(feature = "sqlite")]
        Engine::Sqlite => Ok(Box::new(
            crate::dataaccelerator::metadata::sqlite::AcceleratedMetadataSqlite::try_new(
                dataset,
                create_if_file_not_exists,
            )
            .await?,
        )),
        #[cfg(not(feature = "sqlite"))]
        Engine::Sqlite => Err("Spice wasn't build with Sqlite support enabled".into()),
        Engine::PostgreSQL => todo!(),
        Engine::Arrow => Err("Arrow acceleration not supported for metadata".into()),
    }
}
