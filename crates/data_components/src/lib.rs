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

#![allow(clippy::missing_errors_doc)]
use std::{error::Error, sync::Arc};

use async_trait::async_trait;
use datafusion::{catalog::CatalogProvider, datasource::TableProvider, sql::TableReference};

pub mod arrow;
#[cfg(feature = "clickhouse")]
pub mod clickhouse;
#[cfg(feature = "databricks")]
pub mod databricks;
#[cfg(feature = "debezium")]
pub mod debezium;
#[cfg(feature = "debezium")]
pub mod debezium_kafka;
#[cfg(feature = "delta_lake")]
pub mod delta_lake;
#[cfg(feature = "duckdb")]
pub mod duckdb;
#[cfg(feature = "dynamodb")]
pub mod dynamodb;
pub mod flight;
#[cfg(feature = "flightsql")]
pub mod flightsql;
pub mod iceberg;
#[cfg(any(feature = "debezium", feature = "kafka"))]
pub mod kafka;
#[cfg(feature = "mongodb")]
pub mod mongodb;
#[cfg(feature = "mssql")]
pub mod mssql;
#[cfg(feature = "mysql")]
pub mod mysql;
#[cfg(feature = "odbc")]
pub mod odbc;
#[cfg(feature = "oracle")]
pub mod oracle;
#[cfg(feature = "postgres")]
pub mod postgres;
pub mod refresh_skip;
pub mod s3_single_file_cached;
#[cfg(feature = "s3_vectors")]
pub mod s3_vectors;
#[cfg(feature = "scylladb")]
pub mod scylladb;

#[cfg(feature = "sharepoint")]
pub mod sharepoint;
#[cfg(feature = "snowflake")]
pub mod snowflake;
#[cfg(feature = "spark_connect")]
pub mod spark_connect;
pub mod spice_cloud;
#[cfg(feature = "sqlite")]
pub mod sqlite;
#[cfg(feature = "turso")]
pub mod turso;
pub mod unity_catalog;

pub mod git;
pub mod github;
pub mod key_filter;
pub mod rate_limit;

pub mod cdc;
pub mod delete;
pub mod graphql;
pub mod http;
#[cfg(feature = "imap")]
pub mod imap;
pub mod index_maintenance;
pub mod object;
pub mod poly;

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

#[async_trait]
pub trait RefreshableCatalogProvider: CatalogProvider {
    async fn refresh(&self) -> Result<(), Box<dyn Error + Send + Sync>>;
}
