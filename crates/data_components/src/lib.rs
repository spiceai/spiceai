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
use std::{
    any::Any, borrow::Cow, collections::HashMap, error::Error, hash::BuildHasher, sync::Arc,
};

use async_trait::async_trait;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
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
use datafusion_federation::FederatedTableProviderAdaptor;

/// Schema-level metadata key for foreign key relationships.
///
/// The value is a JSON array of objects, each describing one foreign key constraint:
/// ```json
/// [
///   {
///     "columns": ["customer_id"],
///     "foreign_table": "catalog.public.customers",
///     "foreign_columns": ["id"]
///   }
/// ]
/// ```
pub const FOREIGN_KEYS_METADATA_KEY: &str = "foreign_keys";

/// Canonical Arrow metadata key for user-facing table and column descriptions.
pub const DESCRIPTION_METADATA_KEY: &str = "description";

/// Canonical Arrow field metadata key for the source-native column type.
pub const SOURCE_TYPE_METADATA_KEY: &str = "source_type";

/// Canonical Arrow field metadata key marking source partition columns.
pub const PARTITION_METADATA_KEY: &str = "partition";

/// Canonical Arrow field metadata key marking source clustering columns.
///
/// Values are one-based ordinals when the source reports clustering order.
pub const CLUSTERING_METADATA_KEY: &str = "clustering";

/// Canonical Arrow schema metadata key for a source-native clustering expression.
pub const CLUSTERING_KEY_METADATA_KEY: &str = "clustering_key";

/// Metadata to merge into fields, keyed by field name.
pub type FieldMetadata = HashMap<String, HashMap<String, String>>;

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
#[cfg(feature = "federation")]
pub mod federation;
pub mod flight;
#[cfg(feature = "flightsql")]
pub mod flightsql;
pub mod function_support;
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
#[cfg(feature = "snowflake")]
pub(crate) mod source_arrow_compat;
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
pub mod pk_filter_expr;
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
    ///
    /// Keys in `extra_metadata` will overwrite any pre-existing schema metadata with the same key.
    #[must_use]
    pub fn new<S>(inner: Arc<dyn TableProvider>, extra_metadata: HashMap<String, String, S>) -> Self
    where
        S: BuildHasher,
    {
        Self::new_with_field_metadata(inner, extra_metadata, &FieldMetadata::new())
    }

    /// Wrap `inner`, merging schema-level metadata and per-field metadata into its schema.
    ///
    /// Keys in `extra_metadata` overwrite pre-existing schema metadata with the same key. Keys in
    /// `field_metadata` overwrite pre-existing field metadata for matching field names.
    #[must_use]
    pub fn new_with_field_metadata<S>(
        inner: Arc<dyn TableProvider>,
        extra_metadata: HashMap<String, String, S>,
        field_metadata: &FieldMetadata,
    ) -> Self
    where
        S: BuildHasher,
    {
        let base = inner.schema();
        let mut metadata = base.metadata().clone();
        metadata.extend(extra_metadata);

        let fields = base
            .fields()
            .iter()
            .map(|field| {
                if let Some(extra) = field_metadata.get(field.name().as_str()) {
                    let mut metadata = field.metadata().clone();
                    metadata.extend(
                        extra
                            .iter()
                            .map(|(key, value)| (key.clone(), value.clone())),
                    );
                    Arc::new(field.as_ref().clone().with_metadata(metadata))
                } else {
                    Arc::clone(field)
                }
            })
            .collect::<Vec<_>>();

        let schema = Arc::new(Schema::new_with_metadata(fields, metadata));
        Self { inner, schema }
    }

    #[must_use]
    pub fn get_inner_ref(&self) -> &Arc<dyn TableProvider> {
        &self.inner
    }
}

/// Wrap `provider` with schema metadata while preserving federation pushdown when possible.
///
/// `datafusion-federation` discovers federated tables by downcasting to
/// [`FederatedTableProviderAdaptor`]. If metadata enrichment is placed outside that adaptor, the
/// federated table is hidden from the analyzer and pushdown is lost. When the provider is already a
/// federated adaptor with a fallback provider, keep the adaptor as the outer provider and enrich the
/// fallback provider instead.
#[must_use]
pub fn metadata_enriched_table_provider<S>(
    provider: Arc<dyn TableProvider>,
    extra_metadata: HashMap<String, String, S>,
    field_metadata: FieldMetadata,
) -> Arc<dyn TableProvider>
where
    S: BuildHasher,
{
    if extra_metadata.is_empty() && field_metadata.is_empty() {
        return provider;
    }

    if let Some(adaptor) = provider
        .as_any()
        .downcast_ref::<FederatedTableProviderAdaptor>()
    {
        let Some(table_provider) = &adaptor.table_provider else {
            return Arc::clone(&provider);
        };

        let enriched_provider = metadata_enriched_table_provider(
            Arc::clone(table_provider),
            extra_metadata,
            field_metadata,
        );

        return Arc::new(FederatedTableProviderAdaptor::new_with_provider(
            Arc::clone(&adaptor.source),
            enriched_provider,
        ));
    }

    Arc::new(MetadataEnrichedTableProvider::new_with_field_metadata(
        provider,
        extra_metadata,
        &field_metadata,
    ))
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

    async fn truncate(&self, state: &dyn Session) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        self.inner.truncate(state).await
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

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field};
    use datafusion::error::DataFusionError;
    use datafusion::logical_expr::TableSource;
    use datafusion::optimizer::optimizer::Optimizer;
    use datafusion_federation::{
        FederatedTableProviderAdaptor, FederatedTableSource, FederationProvider,
    };

    #[derive(Debug)]
    struct TestFederationProvider;

    impl FederationProvider for TestFederationProvider {
        fn name(&self) -> &'static str {
            "test"
        }

        fn compute_context(&self) -> Option<String> {
            Some("test-context".to_string())
        }

        fn optimizer(&self) -> Option<Arc<Optimizer>> {
            None
        }
    }

    #[derive(Debug)]
    struct TestFederatedSource {
        schema: SchemaRef,
    }

    impl TableSource for TestFederatedSource {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }
    }

    impl FederatedTableSource for TestFederatedSource {
        fn federation_provider(&self) -> Arc<dyn FederationProvider> {
            Arc::new(TestFederationProvider)
        }
    }

    #[derive(Debug)]
    struct TestTableProvider {
        schema: SchemaRef,
    }

    #[async_trait]
    impl TableProvider for TestTableProvider {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }

        fn table_type(&self) -> TableType {
            TableType::Base
        }

        async fn scan(
            &self,
            _state: &dyn Session,
            _projection: Option<&Vec<usize>>,
            _filters: &[Expr],
            _limit: Option<usize>,
        ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
            Err(DataFusionError::NotImplemented("scan".to_string()))
        }
    }

    #[test]
    fn metadata_enrichment_preserves_federated_table_provider_adaptor() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let source: Arc<dyn FederatedTableSource> = Arc::new(TestFederatedSource {
            schema: Arc::clone(&schema),
        });
        let fallback: Arc<dyn TableProvider> = Arc::new(TestTableProvider { schema });
        let provider: Arc<dyn TableProvider> = Arc::new(
            FederatedTableProviderAdaptor::new_with_provider(source, fallback),
        );

        let field_metadata = HashMap::from([(
            "id".to_string(),
            HashMap::from([("source_type".to_string(), "BIGINT".to_string())]),
        )]);
        let enriched = metadata_enriched_table_provider(
            provider,
            HashMap::from([("description".to_string(), "orders".to_string())]),
            field_metadata,
        );

        let adaptor = enriched
            .as_any()
            .downcast_ref::<FederatedTableProviderAdaptor>()
            .expect("metadata enrichment should preserve the federated adaptor");
        let schema = adaptor.schema();
        assert_eq!(
            schema.metadata().get("description").map(String::as_str),
            Some("orders")
        );
        assert_eq!(
            schema
                .field_with_name("id")
                .expect("id field should exist")
                .metadata()
                .get("source_type")
                .map(String::as_str),
            Some("BIGINT")
        );
    }
}
