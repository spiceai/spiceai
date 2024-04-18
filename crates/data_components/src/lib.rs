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
use datafusion::{
    common::OwnedTableReference, datasource::TableProvider, error::DataFusionError,
    execution::context::SessionState, logical_expr::Expr, physical_plan::ExecutionPlan,
};
use duckdb::write::DuckDBTableWriter;
use postgres::write::PostgresTableWriter;
use sqlite::write::SqliteTableWriter;

use crate::arrow::write::MemTable;

pub mod arrow;
#[cfg(feature = "databricks")]
pub mod databricks_delta;
#[cfg(feature = "databricks")]
pub mod databricks_spark;

#[cfg(feature = "databricks")]
pub mod deltatable;
#[cfg(feature = "duckdb")]
pub mod duckdb;
pub mod flight;
#[cfg(feature = "flightsql")]
pub mod flightsql;
#[cfg(feature = "mysql")]
pub mod mysql;
#[cfg(feature = "postgres")]
pub mod postgres;
#[cfg(feature = "databricks")]
pub mod spark_connect;
#[cfg(feature = "sqlite")]
pub mod sqlite;

pub mod delete;
pub mod util;

#[async_trait]
pub trait Read: Send + Sync {
    async fn table_provider(
        &self,
        table_reference: OwnedTableReference,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn Error + Send + Sync>>;
}

#[async_trait]
pub trait ReadWrite: Send + Sync {
    async fn table_provider(
        &self,
        table_reference: OwnedTableReference,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn Error + Send + Sync>>;
}

/// Similar to the `Read` trait above, but the `TableProvider.scan()` method returns ExecutionPlans that are unbounded (i.e. streaming).
#[async_trait]
pub trait Stream: Send + Sync {
    async fn table_provider(
        &self,
        table_reference: OwnedTableReference,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn Error + Send + Sync>>;
}

#[async_trait]
pub trait DeleteTableProvider: TableProvider {
    async fn delete_from(
        &self,
        _state: &SessionState,
        _filters: &[Expr],
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Plan("Not implemented".to_string()))
    }
}

// There is no good way to allow inter trait casting yet as TableProvider is not controlled
pub fn cast_to_deleteable<'a>(
    from: &'a dyn TableProvider,
) -> Option<&'a (dyn DeleteTableProvider + 'a)> {
    if let Some(p) = from.as_any().downcast_ref::<MemTable>() {
        return Some(p);
    }

    if let Some(p) = from.as_any().downcast_ref::<PostgresTableWriter>() {
        return Some(p);
    }

    if let Some(p) = from.as_any().downcast_ref::<DuckDBTableWriter>() {
        return Some(p);
    }

    if let Some(p) = from.as_any().downcast_ref::<SqliteTableWriter>() {
        return Some(p);
    }

    None
}
