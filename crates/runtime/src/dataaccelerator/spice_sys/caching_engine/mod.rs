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

use super::{AccelerationConnection, Error, Result, acceleration_connection};
use crate::{component::dataset::Dataset, dataaccelerator::spice_sys::OpenOption};

#[cfg(feature = "duckdb")]
mod duckdb;

pub struct CachingEngineSys {
    #[cfg_attr(not(feature = "duckdb"), expect(dead_code))]
    dataset_name: String,
    #[cfg_attr(not(feature = "duckdb"), expect(dead_code))]
    acceleration_connection: AccelerationConnection,
}

impl CachingEngineSys {
    pub async fn try_new(dataset: &Dataset, open_option: OpenOption) -> Result<Self> {
        Ok(Self {
            dataset_name: dataset.name.to_string(),
            acceleration_connection: acceleration_connection(dataset, open_option).await?,
        })
    }

    pub fn update_fetched_at(&self) -> Result<()> {
        #[cfg(feature = "duckdb")]
        {
            match &self.acceleration_connection {
                AccelerationConnection::DuckDB(pool) => self.update_fetched_at_duckdb(pool),
                #[cfg(feature = "postgres-accel")]
                AccelerationConnection::Postgres(_) => Err(Error::NoAccelerationConnection),
                #[cfg(feature = "sqlite")]
                AccelerationConnection::SQLite(_) => Err(Error::NoAccelerationConnection),
                #[cfg(feature = "turso")]
                AccelerationConnection::Turso(_) => Err(Error::NoAccelerationConnection),
                #[cfg(all(not(windows), feature = "sqlite"))]
                AccelerationConnection::Cayenne(_) => Err(Error::NoAccelerationConnection),
            }
        }

        #[cfg(not(feature = "duckdb"))]
        {
            Err(Error::NoAccelerationConnection)
        }
    }
}
