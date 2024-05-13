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

#![allow(clippy::missing_errors_doc)]
use std::{error::Error, sync::Arc};

use async_trait::async_trait;
use datafusion::{datasource::TableProvider, sql::TableReference};

pub mod arrow;
#[cfg(feature = "databricks")]
pub mod databricks_delta;
#[cfg(feature = "databricks")]
pub mod databricks_spark;

#[cfg(feature = "clickhouse")]
pub mod clickhouse;
#[cfg(feature = "databricks")]
pub mod deltatable;
#[cfg(feature = "duckdb")]
pub mod duckdb;
pub mod flight;
#[cfg(feature = "flightsql")]
pub mod flightsql;
#[cfg(feature = "mysql")]
pub mod mysql;
#[cfg(feature = "odbc")]
pub mod odbc;
#[cfg(feature = "postgres")]
pub mod postgres;
#[cfg(feature = "spark_connect")]
pub mod spark_connect;
#[cfg(feature = "sqlite")]
pub mod sqlite;

#[cfg(feature = "snowflake")]
pub mod snowflake;

pub mod delete;
pub mod util;

#[async_trait]
pub trait Read: Send + Sync {
    async fn table_provider(
        &self,
        table_reference: TableReference,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn Error + Send + Sync>>;
}

#[async_trait]
pub trait ReadWrite: Send + Sync {
    async fn table_provider(
        &self,
        table_reference: TableReference,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn Error + Send + Sync>>;
}

/// Similar to the `Read` trait above, but the `TableProvider.scan()` method returns ExecutionPlans that are unbounded (i.e. streaming).
#[async_trait]
pub trait Stream: Send + Sync {
    async fn table_provider(
        &self,
        table_reference: TableReference,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn Error + Send + Sync>>;
}
