/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

//! CREATE TABLE `spice_sys_dynamodb_streams` (
//!     `dataset_name` TEXT PRIMARY KEY,
//!     `checkpoint_data` TEXT,
//!     `created_at` TIMESTAMP DEFAULT `CURRENT_TIMESTAMP`,
//!     `updated_at` TIMESTAMP DEFAULT `CURRENT_TIMESTAMP` ON UPDATE `CURRENT_TIMESTAMP`,
//! );

use super::{AccelerationConnection, Error, Result, acceleration_connection};
use crate::{
    component::dataset::Dataset,
    dataaccelerator::spice_sys::OpenOption,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

const DYNAMODB_STREAMS_TABLE_NAME: &str = "spice_sys_dynamodb_streams";

#[cfg(feature = "duckdb")]
mod duckdb;

/// Serializable checkpoint metadata for DynamoDB Streams
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DynamoDBCheckpointMetadata {
    pub checkpoint_data: String, // JSON-encoded GlobalCheckpoint
}

pub struct DynamoDBSys {
    dataset_name: String,
    acceleration_connection: AccelerationConnection,
}

impl DynamoDBSys {
    pub async fn try_new(dataset: &Dataset, open_option: OpenOption) -> Result<Self> {
        Ok(Self {
            dataset_name: dataset.name.to_string(),
            acceleration_connection: acceleration_connection(dataset, open_option).await?,
        })
    }

    pub(crate) async fn get(&self) -> Option<DynamoDBCheckpointMetadata> {
        match &self.acceleration_connection {
            #[cfg(feature = "duckdb")]
            AccelerationConnection::DuckDB(pool) => self.get_duckdb(pool),
            #[cfg(not(feature = "duckdb"))]
            _ => None,
        }
    }

    pub(crate) async fn upsert(&self, metadata: &DynamoDBCheckpointMetadata) -> Result<()> {
        match &self.acceleration_connection {
            #[cfg(feature = "duckdb")]
            AccelerationConnection::DuckDB(pool) => self.upsert_duckdb(pool, metadata),
            #[cfg(not(feature = "duckdb"))]
            _ => Err(Error::NoAccelerationConnection),
        }
    }
}