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
use std::{any::Any, borrow::Cow, collections::HashMap, error::Error, sync::Arc};

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::{
    catalog::{CatalogProvider, Session},
    common::{Constraints, Statistics},
    datasource::{TableProvider, TableType},
    error::Result as DataFusionResult,
    logical_expr::{LogicalPlan, TableProviderFilterPushDown, dml::InsertOp},
    physical_plan::ExecutionPlan,
    prelude::Expr,
    sql::TableReference,
};

/// Schema-level metadata key for foreign key relationships.
///
/// The value is a JSON array of objects, each describing one foreign key constraint:
/// ```json
/// [
///   {
///     "columns": ["customer_id"],
///     "referenced_schema": "public",
///     "referenced_table": "customers",
///     "referenced_columns": ["id"]
///   }
/// ]
/// ```
pub const FOREIGN_KEYS_METADATA_KEY: &str = "foreign_keys";

pub mod arrow;
#[cfg(feature = "clickhouse")]
pub mod clickhouse;
#[cfg(feature = "cosmosdb")]
pub mod cosmosdb;
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
#[cfg(feature = "duckdb")]
pub mod ducklake;
#[cfg(feature = "dynamodb")]
pub mod dynamodb;
#[cfg(feature = "elasticsearch")]
pub mod elasticsearch;
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
#[cfg(feature = "postgres")]
pub mod postgres_replication;
pub mod refresh_skip;
pub mod resilient_http;
pub mod s3_single_file_cached;
#[cfg(feature = "s3_vectors")]
pub mod s3_vectors;
pub mod schema_discovery;
#[cfg(feature = "scylladb")]
pub mod scylladb;
pub mod sql_expr;

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
pub mod update;

/// A [`TableProvider`] wrapper that merges additional metadata into the Arrow schema.
///
/// All trait methods delegate to the inner provider except [`schema()`](TableProvider::schema),
/// which returns the original schema with `extra_metadata` merged in.
pub struct MetadataEnrichedTableProvider {
    inner: Arc<dyn TableProvider>,
    schema: SchemaRef,
}

impl MetadataEnrichedTableProvider {
    /// Wrap `inner`, merging `extra_metadata` into its schema-level metadata.
    #[must_use]
    pub fn new(inner: Arc<dyn TableProvider>, extra_metadata: HashMap<String, String>) -> Self {
        let base = inner.schema();
        let mut metadata = base.metadata().clone();
        metadata.extend(extra_metadata);
        let schema = Arc::new(base.as_ref().clone().with_metadata(metadata));
        Self { inner, schema }
    }
}

impl std::fmt::Debug for MetadataEnrichedTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetadataEnrichedTableProvider")
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl TableProvider for MetadataEnrichedTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn constraints(&self) -> Option<&Constraints> {
        self.inner.constraints()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    fn get_logical_plan(&self) -> Option<Cow<'_, LogicalPlan>> {
        self.inner.get_logical_plan()
    }

    fn get_column_default(&self, column: &str) -> Option<&Expr> {
        self.inner.get_column_default(column)
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        self.inner.supports_filters_pushdown(filters)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        self.inner.scan(state, projection, filters, limit).await
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        overwrite: InsertOp,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        self.inner.insert_into(state, input, overwrite).await
    }

    async fn delete_from(
        &self,
        state: &dyn Session,
        filters: Vec<Expr>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        self.inner.delete_from(state, filters).await
    }

    async fn update(
        &self,
        state: &dyn Session,
        assignments: Vec<(String, Expr)>,
        filters: Vec<Expr>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        self.inner.update(state, assignments, filters).await
    }

    fn statistics(&self) -> Option<Statistics> {
        self.inner.statistics()
    }
}

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
